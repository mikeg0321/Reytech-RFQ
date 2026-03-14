"""
routes_v1.py — MCP-ready /api/v1/ endpoints for external AI agents.
Stable contract: these routes will not change shape without version bump.
"""
from flask import request, jsonify
from src.api.shared import bp, auth_required, api_response
import logging
import time as _time

_BOOT_TIME = _time.time()

log = logging.getLogger("reytech")


@bp.route("/api/v1/rfq/<rfq_id>")
@auth_required
def api_v1_get_rfq(rfq_id):
    """Get a single RFQ with line items.
    Returns: api_response({...rfq fields...}) or 404.
    Auth: X-API-Key or Basic Auth.
    """
    try:
        from src.core.dal import get_rfq, get_line_items
        rfq = get_rfq(rfq_id)
        if not rfq:
            return api_response(error="RFQ not found", status=404)
        rfq["line_items"] = get_line_items(rfq_id, "rfq")
        return api_response(rfq)
    except Exception as e:
        log.error("v1/rfq/%s error: %s", rfq_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/<rfq_id>/price", methods=["POST"])
@auth_required
def api_v1_price_rfq(rfq_id):
    """Trigger pricing on an RFQ. Enqueues as async task.
    Accepts: {"force": bool} (optional)
    Returns: api_response({"status": "queued", "rfq_id": id})
    Auth: X-API-Key or Basic Auth.
    """
    try:
        from src.core.dal import get_rfq
        rfq = get_rfq(rfq_id)
        if not rfq:
            return api_response(error="RFQ not found", status=404)
        data = request.get_json(force=True, silent=True) or {}
        force = data.get("force", False)
        try:
            from src.core.task_queue import enqueue
            task_id = enqueue("price_rfq", {"rfq_id": rfq_id, "force": force},
                              actor="api_v1")
            return api_response({"status": "queued", "rfq_id": rfq_id, "task_id": task_id})
        except Exception:
            # Fallback: task_queue not initialized yet, return accepted
            return api_response({"status": "accepted", "rfq_id": rfq_id})
    except Exception as e:
        log.error("v1/rfq/%s/price error: %s", rfq_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/pipeline")
@auth_required
def api_v1_pipeline():
    """Current queue depths and agent status for external orchestrators.
    Returns: api_response({rfqs: {...}, pcs: {...}, orders: {...}, agents: {...}})
    Auth: X-API-Key or Basic Auth.
    """
    try:
        from src.core.dal import list_rfqs, list_pcs, list_orders
        from collections import Counter

        # RFQ status counts
        rfqs = list_rfqs(limit=10000)
        rfq_counts = dict(Counter(r.get("status", "unknown") for r in rfqs))

        # PC status counts
        pcs = list_pcs(limit=10000)
        pc_counts = dict(Counter(p.get("status", "unknown") for p in pcs))

        # Order status counts
        orders = list_orders(limit=10000)
        order_counts = dict(Counter(o.get("status", "unknown") for o in orders))

        # Agent status from scheduler
        agents = {}
        try:
            from src.core.scheduler import get_all_jobs
            for job in get_all_jobs():
                agents[job["name"]] = {
                    "status": job["status"],
                    "last_run": job.get("last_run"),
                    "error_count": job.get("error_count", 0),
                }
        except Exception:
            pass

        return api_response({
            "rfqs": {"total": len(rfqs), "by_status": rfq_counts},
            "pcs": {"total": len(pcs), "by_status": pc_counts},
            "orders": {"total": len(orders), "by_status": order_counts},
            "agents": agents,
        })
    except Exception as e:
        log.error("v1/pipeline error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/rfq/new")
@auth_required
def rfq_new_form():
    """Manual RFQ creation form — fallback when email poller is down."""
    from src.api.render import render_page
    return render_page("rfq_new.html", active_page="Home")


@bp.route("/api/v1/rfq/create", methods=["POST"])
@auth_required
def api_v1_create_rfq():
    """Create an RFQ via DAL. Accepts JSON or form data.
    Returns: api_response({id, status}) with 201 on success.
    """
    import uuid
    try:
        data = request.get_json(force=True, silent=True) or {}
        # Also accept form data for HTML form submission
        if not data:
            data = {k: request.form.get(k, "") for k in request.form}

        # Parse line items from form or JSON
        items = data.get("items", [])
        if not items and request.form:
            # Build items from repeating form fields
            idx = 0
            while f"items[{idx}][description]" in request.form:
                item = {
                    "description": request.form.get(f"items[{idx}][description]", ""),
                    "qty": float(request.form.get(f"items[{idx}][qty]", 0) or 0),
                    "uom": request.form.get(f"items[{idx}][uom]", "EA"),
                    "unit_price": float(request.form.get(f"items[{idx}][unit_price]", 0) or 0),
                }
                if item["description"]:
                    items.append(item)
                idx += 1

        rfq_id = uuid.uuid4().hex[:8]
        from datetime import datetime
        rfq = {
            "id": rfq_id,
            "solicitation_number": data.get("solicitation_number", ""),
            "agency": data.get("agency", ""),
            "institution": data.get("institution", data.get("agency", "")),
            "requestor_name": data.get("requestor_name", ""),
            "requestor_email": data.get("requestor_email", ""),
            "rfq_number": data.get("solicitation_number", ""),
            "received_at": datetime.now().isoformat(),
            "status": "new",
            "source": "manual",
            "notes": data.get("notes", ""),
            "items": items,
        }

        from src.core.dal import save_rfq
        save_rfq(rfq, actor="manual_form")

        # If form submission, redirect to RFQ detail page
        if request.form:
            from flask import redirect
            return redirect(f"/rfq/{rfq_id}")

        return api_response({"id": rfq_id, "status": "new"}, status=201)
    except Exception as e:
        log.error("v1/rfq/create error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/webhook/rfq-created", methods=["POST"])
@auth_required
def api_v1_webhook_rfq_created():
    """Manually fire the rfq.created webhook (for testing)."""
    data = request.get_json(force=True, silent=True) or {}
    try:
        from src.core.webhooks import fire_webhook
        fire_webhook("rfq.created", data)
        return api_response({"dispatched": True})
    except Exception as e:
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/webhook/order-updated", methods=["POST"])
@auth_required
def api_v1_webhook_order_updated():
    """Manually fire the order.updated webhook (for testing)."""
    data = request.get_json(force=True, silent=True) or {}
    try:
        from src.core.webhooks import fire_webhook
        fire_webhook("order.updated", data)
        return api_response({"dispatched": True})
    except Exception as e:
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/webhook/test", methods=["POST"])
@auth_required
def api_v1_webhook_test():
    """Fire a test webhook to verify n8n connectivity."""
    try:
        from src.core.webhooks import fire_webhook
        fire_webhook("test", {
            "message": "Test webhook from Reytech RFQ",
            "timestamp": __import__("datetime").datetime.now().isoformat(),
        })
        return api_response({"dispatched": True, "message": "Test webhook fired"})
    except Exception as e:
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/notify/test-sms", methods=["POST"])
@auth_required
def api_v1_test_sms():
    """Send a test SMS to NOTIFY_PHONE."""
    try:
        from src.agents.notify_agent import notify_new_rfq_sms
        notify_new_rfq_sms({
            "id": "TEST", "solicitation_number": "TEST-SMS",
            "agency": "Test Agency", "items": [{"d": "test"}],
            "due_date": "2026-12-31",
        })
        return api_response({"sent": True})
    except Exception as e:
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/pc/<pc_id>/item/<path:item_number>/history")
@auth_required
def api_v1_pc_item_history(pc_id, item_number):
    """Price history for a specific line item on a PC.
    Returns last 5 price records matching by part number or description.
    """
    try:
        from src.core.dal import get_pc, get_price_history_for_item
        pc = get_pc(pc_id)
        if not pc:
            return api_response(error="PC not found", status=404)

        # Find the item in the PC to get description for fallback matching
        description = ""
        items = pc.get("items", [])
        if isinstance(items, list):
            for item in items:
                pn = item.get("item_number", "") or item.get("part_number", "")
                if pn == item_number:
                    description = item.get("description", "")
                    break

        history = get_price_history_for_item(
            part_number=item_number, description=description, limit=5)
        return api_response(history)
    except Exception as e:
        log.error("v1/pc/%s/item/%s/history error: %s", pc_id, item_number, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/health")
@auth_required
def api_v1_health():
    """Full system health for external orchestrators.
    Returns: version, uptime, DB state, queue depths, agent status.
    Auth: X-API-Key or Basic Auth.
    """
    import sqlite3
    try:
        # Version (git sha)
        version = "unknown"
        try:
            import subprocess
            version = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL, timeout=2
            ).decode().strip()
        except Exception:
            pass

        # Uptime
        uptime = int(_time.time() - _BOOT_TIME)

        # DB status
        db_info = {"status": "error", "tables": 0, "row_counts": {}}
        try:
            from src.core.db import DB_PATH
            conn = sqlite3.connect(DB_PATH, timeout=5)
            conn.row_factory = sqlite3.Row
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            db_info["status"] = "ok"
            db_info["tables"] = len(tables)
            for t in ("rfqs", "price_checks", "orders", "quotes", "contacts"):
                try:
                    cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    db_info["row_counts"][t] = cnt
                except Exception:
                    db_info["row_counts"][t] = -1
            conn.close()
        except Exception as e:
            db_info["status"] = f"error: {str(e)[:100]}"

        # Queue depths from DAL
        queues = {"rfqs_new": 0, "pcs_new": 0, "orders_active": 0}
        try:
            from src.core.dal import list_rfqs, list_pcs, list_orders
            queues["rfqs_new"] = len(list_rfqs(status="new"))
            queues["pcs_new"] = len(list_pcs(status="parsed"))
            queues["orders_active"] = len(list_orders(status="new")) + len(list_orders(status="active"))
        except Exception:
            pass

        # Agent status from scheduler
        agents = {}
        try:
            from src.core.scheduler import get_all_jobs
            for job in get_all_jobs():
                name = job["name"]
                agents[name] = {
                    "last_run": job.get("last_run"),
                    "status": "ok" if job["status"] == "running" else (
                        "error" if job["status"] in ("error", "dead") else "never"
                    ),
                }
                if name == "email-poller":
                    agents[name]["emails_processed_24h"] = job.get("run_count", 0)
        except Exception:
            pass

        # QB health
        try:
            from src.agents.quickbooks_agent import get_qb_health
            agents["quickbooks"] = get_qb_health()
        except Exception:
            agents["quickbooks"] = {"status": "unavailable", "error": "Module not loaded"}

        # SCPRS harvest status
        harvest = {}
        try:
            from src.core.db import DB_PATH as _hp
            import sqlite3 as _sq
            _hc = _sq.connect(_hp, timeout=5)
            harvest["po_master_count"] = _hc.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
            harvest["vendor_intel_count"] = _hc.execute("SELECT COUNT(*) FROM vendor_intel").fetchone()[0]
            harvest["buyer_intel_count"] = _hc.execute("SELECT COUNT(*) FROM buyer_intel").fetchone()[0]
            harvest["won_quotes_kb_count"] = _hc.execute("SELECT COUNT(*) FROM won_quotes_kb").fetchone()[0]
            harvest["competitors_count"] = _hc.execute("SELECT COUNT(*) FROM competitors").fetchone()[0]
            _hc.close()
            # Last harvest timestamp from log file
            import os as _os
            _log_path = _os.path.join(_os.environ.get("DATA_DIR", "data"), "scprs_harvest.log")
            if _os.path.exists(_log_path):
                harvest["last_harvest"] = datetime.fromtimestamp(
                    _os.path.getmtime(_log_path)).isoformat()
        except Exception:
            pass

        # Connector status
        connector_status = {}
        try:
            from src.core.pull_orchestrator import PullOrchestrator
            connector_status = PullOrchestrator().get_status()
        except Exception:
            pass

        return api_response({
            "version": version,
            "uptime_seconds": uptime,
            "db": db_info,
            "queues": queues,
            "agents": agents,
            "scprs_harvest": harvest,
            "connectors": connector_status,
        })
    except Exception as e:
        log.error("v1/health error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/audit/<entity_type>/<entity_id>")
@auth_required
def api_v1_audit_trail(entity_type, entity_id):
    """Get audit trail for an entity. Returns last 20 records."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_trail WHERE item_description=? AND rfq_id=? "
                "ORDER BY created_at DESC LIMIT 20",
                (entity_type, entity_id)).fetchall()
            return api_response([dict(r) for r in rows])
    except Exception as e:
        log.error("v1/audit error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/snapshots/<entity_type>/<entity_id>")
@auth_required
def api_v1_snapshots(entity_type, entity_id):
    """Get snapshots for an entity. Returns last 10."""
    try:
        from src.core.snapshots import list_snapshots
        snaps = list_snapshots(agent_name="dal", limit=50)
        # Filter to this entity
        filtered = [s for s in snaps if s.get("run_id") == entity_id
                    and s.get("entity_type") == entity_type][:10]
        return api_response(filtered)
    except Exception as e:
        log.error("v1/snapshots error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rollback/<int:snapshot_id>", methods=["POST"])
@auth_required
def api_v1_rollback(snapshot_id):
    """Restore an entity from a snapshot."""
    try:
        from src.core.snapshots import restore_snapshot
        from src.core.dal import save_rfq, save_pc, save_order
        result = restore_snapshot(snapshot_id)
        if not result.get("ok"):
            return api_response(error=result.get("error", "Restore failed"), status=404)
        # Write restored data back via DAL
        entity_type = result.get("entity")
        data = result.get("data")
        if entity_type == "rfq" and isinstance(data, dict):
            save_rfq(data, actor="rollback")
        elif entity_type == "price_check" and isinstance(data, dict):
            save_pc(data, actor="rollback")
        elif entity_type == "order" and isinstance(data, dict):
            save_order(data, actor="rollback")
        return api_response({"restored": True, "entity_type": entity_type,
                             "entity_id": result.get("row_count")})
    except Exception as e:
        log.error("v1/rollback error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Connector Management Endpoints ──────────────────────────────────────────

@bp.route("/api/v1/connectors")
@auth_required
def api_v1_connectors():
    """List all connectors with status, health, record counts."""
    try:
        from src.core.pull_orchestrator import PullOrchestrator
        status = PullOrchestrator().get_status()
        return api_response(status)
    except Exception as e:
        log.error("v1/connectors error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/connectors/<connector_id>/run", methods=["POST"])
@auth_required
def api_v1_run_connector(connector_id):
    """Trigger a connector pull. Returns immediately with queued status."""
    try:
        from src.core.task_queue import enqueue
        task_id = enqueue("run_connector", {"connector_id": connector_id},
                          actor="api_v1")
        return api_response({"queued": True, "connector_id": connector_id,
                             "task_id": task_id})
    except Exception as e:
        log.error("v1/connectors/%s/run error: %s", connector_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/connectors/<connector_id>/health")
@auth_required
def api_v1_connector_health(connector_id):
    """Health check a connector without pulling data."""
    try:
        from src.core.connector_registry import get_connector
        meta = get_connector(connector_id)
        if not meta:
            return api_response(error="Connector not found", status=404)
        if not meta.get("connector_class"):
            return api_response({"status": "scaffolded",
                                 "message": "No connector class — registry only"})
        import importlib
        parts = meta["connector_class"].rsplit(".", 1)
        mod = importlib.import_module(parts[0])
        connector = getattr(mod, parts[1])()
        health = connector.health_check()
        return api_response(health)
    except Exception as e:
        log.error("v1/connectors/%s/health error: %s", connector_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/agencies")
@auth_required
def api_v1_agencies():
    """List agencies from registry. Query: ?state=CA&limit=100"""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        state = request.args.get("state", "")
        limit = int(request.args.get("limit", 100))
        if state:
            rows = conn.execute(
                "SELECT * FROM agency_registry WHERE state=? AND active=1 LIMIT ?",
                (state, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM agency_registry WHERE active=1 LIMIT ?",
                (limit,)).fetchall()
        conn.close()
        return api_response([dict(r) for r in rows])
    except Exception as e:
        log.error("v1/agencies error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)
