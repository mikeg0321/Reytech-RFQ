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

        rfq["line_items"] = items  # alias — generate endpoint reads line_items
        rfq["_original_items"] = [dict(i) for i in items]  # snapshot for validation

        from src.core.dal import save_rfq
        save_rfq(rfq, actor="manual_form")

        # Also write to JSON + dashboard cache so generate/autosave can find it
        try:
            from src.api.dashboard import load_rfqs, save_rfqs
            rfqs = load_rfqs()
            rfqs[rfq_id] = rfq
            save_rfqs(rfqs)
        except Exception as _e:
            log.debug("RFQ JSON dual-write: %s", _e)

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


_health_cache = {"data": None, "ts": 0}

@bp.route("/api/v1/health")
@auth_required
def api_v1_health():
    """Full system health for external orchestrators.
    Returns: version, uptime, DB state, queue depths, agent status.
    Auth: X-API-Key or Basic Auth.
    """
    import time as _time
    global _health_cache
    if _health_cache["data"] and (_time.time() - _health_cache["ts"]) < 60:
        return jsonify(_health_cache["data"])
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

        # Compliance status
        compliance = {}
        try:
            from src.core.dal import check_compliance_alerts
            alerts = check_compliance_alerts("reytech")
            compliance = {
                "alerts": len(alerts),
                "critical": len([a for a in alerts if a["severity"] == "critical"]),
                "warning": len([a for a in alerts if a["severity"] == "warning"]),
                "health": "critical" if any(a["severity"] == "critical" for a in alerts) else "ok"
            }
        except Exception:
            pass

        _result = {
            "version": version,
            "uptime_seconds": uptime,
            "db": db_info,
            "queues": queues,
            "agents": agents,
            "scprs_harvest": harvest,
            "connectors": connector_status,
            "compliance": compliance,
        }
        _health_cache["data"] = _result
        _health_cache["ts"] = _time.time()
        return api_response(_result)
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


@bp.route("/api/v1/connectors/<connector_id>/run", methods=["GET", "POST"])
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
        limit = min(int(request.args.get("limit", 100)), 200)
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


# ── Harvest Trigger Endpoints (GET for browser use) ──────────────────────────

@bp.route("/api/v1/harvest/ca")
@auth_required
def api_v1_harvest_ca():
    """Trigger CA SCPRS harvest. GET for easy browser trigger."""
    try:
        import threading
        from src.core.pull_orchestrator import PullOrchestrator
        def _run():
            try:
                PullOrchestrator().run_connector("ca_scprs")
            except Exception as e:
                log.error("CA harvest background: %s", e)
        threading.Thread(target=_run, daemon=True, name="harvest-ca").start()
        return api_response({"message": "CA harvest started", "connector": "ca_scprs"})
    except Exception as e:
        log.error("v1/harvest/ca error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/federal")
@auth_required
def api_v1_harvest_federal():
    """Trigger federal USASpending harvest. GET for easy browser trigger."""
    try:
        import threading
        from src.core.pull_orchestrator import PullOrchestrator
        def _run():
            try:
                PullOrchestrator().run_connector("federal_usaspending")
            except Exception as e:
                log.error("Federal harvest background: %s", e)
        threading.Thread(target=_run, daemon=True, name="harvest-federal").start()
        return api_response({"message": "Federal harvest started", "connector": "federal_usaspending"})
    except Exception as e:
        log.error("v1/harvest/federal error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/status")
@auth_required
def api_v1_harvest_status():
    """Current connector status."""
    try:
        from src.core.pull_orchestrator import PullOrchestrator
        return api_response(PullOrchestrator().get_status())
    except Exception as e:
        log.error("v1/harvest/status error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Admin: DB Repair ────────────────────────────────────────────────────────

@bp.route("/api/v1/admin/db-repair")
@auth_required
def api_v1_db_repair():
    """Check DB integrity and repair if corrupted.
    If corrupt: rebuilds DB from scratch, runs migrations, re-seeds.
    If healthy: returns status ok.
    Auth: X-API-Key or Basic Auth required.
    """
    import sqlite3, os, shutil
    from src.core.db import DB_PATH, init_db
    from src.core.paths import DATA_DIR

    report = {
        "db_path": DB_PATH,
        "data_dir": DATA_DIR,
        "using_volume": DATA_DIR != os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data"),
        "steps": [],
    }

    # Step 1: Integrity check
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        report["integrity"] = result
        report["db_size_kb"] = db_size // 1024
        conn.close()

        if result == "ok":
            report["status"] = "healthy"
            report["steps"].append("Integrity check: OK")
            # Still run migrations in case tables are missing
            try:
                from src.core.migrations import run_migrations
                mig = run_migrations()
                report["steps"].append(f"Migrations: v{mig.get('version', '?')}, {mig.get('applied', 0)} applied")
            except Exception as e:
                report["steps"].append(f"Migrations: {e}")
            # Seed agency registry
            try:
                from src.core.ca_agencies import seed_agency_registry
                from src.core.db import get_db
                with get_db() as sc:
                    seed_agency_registry(sc)
                report["steps"].append("Agency registry seeded")
            except Exception as e:
                report["steps"].append(f"Agency seed: {e}")
            # Check table counts
            try:
                conn2 = sqlite3.connect(DB_PATH, timeout=5)
                for t in ["connectors", "agency_registry", "scprs_po_master",
                           "won_quotes_kb", "vendor_intel", "buyer_intel"]:
                    try:
                        cnt = conn2.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
                        report["steps"].append(f"{t}: {cnt} rows")
                    except Exception:
                        report["steps"].append(f"{t}: MISSING")
                conn2.close()
            except Exception:
                pass
            return api_response(report)

    except Exception as e:
        report["integrity"] = f"FAILED: {e}"
        report["steps"].append(f"Integrity check failed: {e}")

    # Step 2: DB is corrupt — rebuild
    report["status"] = "rebuilding"
    report["steps"].append("DB corrupt — starting rebuild")

    # Backup corrupt file
    try:
        if os.path.exists(DB_PATH):
            backup = DB_PATH + ".corrupt." + __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")
            shutil.copy2(DB_PATH, backup)
            report["steps"].append(f"Corrupt DB backed up to {backup}")
            os.remove(DB_PATH)
            # Also remove WAL/SHM
            for ext in ["-wal", "-shm", "-journal"]:
                p = DB_PATH + ext
                if os.path.exists(p):
                    os.remove(p)
            report["steps"].append("Corrupt DB + WAL/SHM removed")
    except Exception as e:
        report["steps"].append(f"Backup failed: {e}")
        return api_response(report, status=500)

    # Step 3: Rebuild
    try:
        init_db()
        report["steps"].append("init_db: schema created")
    except Exception as e:
        report["steps"].append(f"init_db failed: {e}")
        return api_response(report, status=500)

    try:
        from src.core.migrations import run_migrations
        mig = run_migrations()
        report["steps"].append(f"Migrations: v{mig.get('version', '?')}")
    except Exception as e:
        report["steps"].append(f"Migrations failed: {e}")

    try:
        from src.core.ca_agencies import seed_agency_registry
        from src.core.db import get_db
        with get_db() as sc:
            seed_agency_registry(sc)
        report["steps"].append("Agency registry seeded")
    except Exception as e:
        report["steps"].append(f"Agency seed failed: {e}")

    report["steps"].append("Rebuild complete — run /api/v1/harvest/ca to repopulate")
    return api_response(report)


@bp.route("/api/v1/admin/db-info")
@auth_required
def api_v1_db_info():
    """Quick DB diagnostic — path, size, table counts, volume status."""
    import sqlite3, os
    from src.core.db import DB_PATH
    from src.core.paths import DATA_DIR, _USING_VOLUME

    info = {
        "db_path": DB_PATH,
        "data_dir": DATA_DIR,
        "using_volume": _USING_VOLUME,
        "db_exists": os.path.exists(DB_PATH),
        "db_size_kb": os.path.getsize(DB_PATH) // 1024 if os.path.exists(DB_PATH) else 0,
    }

    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()]
        info["table_count"] = len(tables)
        # Key table row counts
        info["tables"] = {}
        for t in ["scprs_po_master", "won_quotes_kb", "connectors",
                   "agency_registry", "vendor_intel", "buyer_intel",
                   "rfqs", "price_checks", "quotes", "orders"]:
            try:
                info["tables"][t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
            except Exception:
                info["tables"][t] = "MISSING"
        # Schema version
        try:
            info["schema_version"] = conn.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
        except Exception:
            info["schema_version"] = "unknown"
        conn.close()
    except Exception as e:
        info["error"] = str(e)

    return api_response(info)



@bp.route("/api/v1/harvest/rebuild-intel")
@auth_required
def api_v1_rebuild_intel():
    """Rebuild intelligence tables from scprs_po_master data. Synchronous."""
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "scripts"))
        from run_scprs_harvest import (
            build_vendor_intel, build_buyer_intel, build_competitors,
            build_won_quotes_kb, build_scprs_awards, get_conn
        )
        conn = get_conn()
        results = {}
        results["vendor_intel"] = build_vendor_intel(conn)
        results["buyer_intel"] = build_buyer_intel(conn)
        results["competitors"] = build_competitors(conn)
        results["won_quotes_kb"] = build_won_quotes_kb(conn)
        results["scprs_awards"] = build_scprs_awards(conn)
        conn.close()
        return api_response(results)
    except Exception as e:
        log.error("rebuild-intel error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/tenant/profile")
@auth_required
def api_v1_tenant_profile():
    """Get tenant profile."""
    try:
        from src.core.dal import get_tenant_profile
        profile = get_tenant_profile("reytech")
        return api_response(profile)
    except Exception as e:
        log.error("v1/tenant/profile error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/tenant/profile", methods=["POST"])
@auth_required
def api_v1_update_tenant_profile():
    """Update tenant profile fields."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        from src.core.dal import update_tenant_profile
        update_tenant_profile("reytech", data)
        return api_response({"updated": True})
    except Exception as e:
        log.error("v1/tenant/profile update error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/tenant/compliance")
@auth_required
def api_v1_tenant_compliance():
    """Get compliance alerts."""
    try:
        from src.core.dal import check_compliance_alerts
        alerts = check_compliance_alerts("reytech")
        critical = len([a for a in alerts if a["severity"] == "critical"])
        warning = len([a for a in alerts if a["severity"] == "warning"])
        return api_response({
            "alerts": alerts,
            "critical_count": critical,
            "warning_count": warning,
            "compliance_health": "critical" if critical > 0 else "warning" if warning > 0 else "ok"
        })
    except Exception as e:
        log.error("v1/tenant/compliance error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Harvest Operations ──────────────────────────────────────────────────────

@bp.route("/api/v1/harvest/reprocess")
@auth_required
def api_v1_harvest_reprocess():
    """Rebuild intelligence tables from scprs_po_master. Synchronous."""
    try:
        import sys, os, sqlite3
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "scripts"))
        from run_scprs_harvest import (
            build_vendor_intel, build_buyer_intel, build_competitors,
            build_won_quotes_kb, build_scprs_awards, get_conn
        )
        conn = get_conn()
        build_vendor_intel(conn)
        build_buyer_intel(conn)
        build_competitors(conn)
        build_won_quotes_kb(conn)
        build_scprs_awards(conn)
        # Get counts
        counts = {}
        for t in ["won_quotes_kb", "vendor_intel", "buyer_intel", "competitors", "scprs_awards"]:
            try:
                counts[t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
            except Exception:
                counts[t] = 0
        conn.close()
        return api_response({"reprocessed": True, "table_counts": counts})
    except Exception as e:
        log.error("v1/harvest/reprocess error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/vendor-search")
@auth_required
def api_v1_harvest_vendor_search():
    """Run vendor name search across active connectors. Synchronous."""
    try:
        from src.core.pull_orchestrator import PullOrchestrator
        from src.core.dal import get_tenant_vendor_names
        names = get_tenant_vendor_names()
        result = PullOrchestrator().run_vendor_search(vendor_names=names)
        return api_response({"vendor_search_complete": True, "result": result})
    except Exception as e:
        log.error("v1/harvest/vendor-search error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/diagnose")
@auth_required
def api_v1_harvest_diagnose():
    """Diagnostic: row counts + sample rows from harvest tables."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        diag = {"counts": {}, "samples": {}, "harvest_progress": dict(_harvest_progress)}
        for t in ["scprs_po_master", "scprs_po_lines", "won_quotes_kb",
                   "vendor_intel", "buyer_intel", "competitors", "scprs_awards"]:
            try:
                diag["counts"][t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
            except Exception:
                diag["counts"][t] = "MISSING"
            try:
                row = conn.execute(f"SELECT * FROM [{t}] LIMIT 1").fetchone()
                if row:
                    d = dict(row)
                    # Truncate long values
                    for k, v in d.items():
                        if isinstance(v, str) and len(v) > 200:
                            d[k] = v[:200] + "..."
                    diag["samples"][t] = d
            except Exception:
                pass
        # Reytech wins
        try:
            r = conn.execute(
                "SELECT COUNT(*), SUM(grand_total) FROM scprs_po_master WHERE LOWER(supplier) LIKE '%reytech%'"
            ).fetchone()
            diag["reytech"] = {"wins": r[0], "value": r[1]}
        except Exception:
            pass
        conn.close()
        return api_response(diag)
    except Exception as e:
        log.error("v1/harvest/diagnose error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/deduplicate")
@auth_required
def api_v1_harvest_deduplicate():
    """Remove duplicate POs from scprs_po_master. Synchronous.
    Groups by (supplier, dept_name, grand_total, start_date, search_term).
    Tightened key includes search_term to avoid deleting recurring real orders.
    Only rows from the same scrape session (same search_term) are true dupes.
    """
    try:
        import sqlite3
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row

        before = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]

        # Find duplicate groups — tighter key includes search_term
        dupes = conn.execute("""
            SELECT supplier, dept_name, grand_total, start_date, search_term,
                   COUNT(*) as cnt, MIN(id) as keep_id,
                   GROUP_CONCAT(id) as all_ids
            FROM scprs_po_master
            GROUP BY supplier, dept_name, grand_total, start_date, search_term
            HAVING cnt > 1
        """).fetchall()

        deleted = 0
        for d in dupes:
            keep_id = d["keep_id"]
            all_ids = [int(x) for x in d["all_ids"].split(",")]
            delete_ids = [x for x in all_ids if x != keep_id]
            if delete_ids:
                placeholders = ",".join("?" * len(delete_ids))
                conn.execute(
                    f"DELETE FROM scprs_po_master WHERE id IN ({placeholders})",
                    delete_ids)
                deleted += len(delete_ids)

        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]

        # Reytech after dedup
        r = conn.execute(
            "SELECT COUNT(*), SUM(grand_total) FROM scprs_po_master WHERE LOWER(supplier) LIKE '%reytech%'"
        ).fetchone()
        conn.close()

        return api_response({
            "before": before,
            "deleted": deleted,
            "after": after,
            "duplicate_groups": len(dupes),
            "reytech_after": {"count": r[0], "value": r[1]},
        })
    except Exception as e:
        log.error("v1/harvest/deduplicate error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


_harvest_progress = {"running": False}


@bp.route("/api/v1/harvest/keywords")
@auth_required
def api_v1_harvest_keywords():
    """Run keyword-based harvest that fetches PO line item detail. Background thread."""
    if _harvest_progress.get("running"):
        return api_response({
            "already_running": True,
            "progress": _harvest_progress,
        })
    try:
        import threading
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        def _run():
            try:
                from src.core.harvest_keywords import HIGH_PRIORITY_KEYWORDS
                from src.agents.connectors.ca_scprs import CASCPRSConnector
                from src.core.pull_orchestrator import _store_results, _get_conn
                from datetime import datetime, timedelta

                _harvest_progress.update(running=True, keywords_total=len(HIGH_PRIORITY_KEYWORDS),
                    keywords_done=0, current_keyword="authenticating", lines_found_so_far=0)

                connector = CASCPRSConnector()
                if not connector.authenticate():
                    log.error("Keyword harvest: SCPRS auth failed")
                    _harvest_progress.update(running=False, current_keyword="auth_failed")
                    return

                from_dt = datetime.now() - timedelta(days=730)
                total_stored = 0
                total_lines = 0

                for i, kw in enumerate(HIGH_PRIORITY_KEYWORDS):
                    _harvest_progress.update(current_keyword=kw, keywords_done=i)
                    try:
                        # 45s timeout per keyword
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(
                                connector.search_by_keyword,
                                kw, from_dt, True, 15)  # max_detail=15
                            results = future.result(timeout=45)
                        if results:
                            conn = _get_conn()
                            stored = _store_results(conn, results, "ca_scprs")
                            lines = sum(len(r.get("line_items", [])) for r in results)
                            total_lines += lines
                            total_stored += stored
                            conn.close()
                            _harvest_progress["lines_found_so_far"] = total_lines
                            log.info("Keyword '%s': %d POs, %d lines stored", kw, stored, lines)
                        import time; time.sleep(3)
                    except FuturesTimeout:
                        log.warning("Keyword '%s' timed out (45s) — skipping", kw)
                    except Exception as e:
                        log.error("Keyword harvest '%s' failed: %s", kw, e)

                log.info("Keyword harvest complete: %d POs, %d line items",
                         total_stored, total_lines)

                # Rebuild intelligence tables
                try:
                    import sys, os
                    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
                        os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "scripts"))
                    from run_scprs_harvest import (
                        build_won_quotes_kb, build_vendor_intel, get_conn as harvest_conn)
                    hconn = harvest_conn()
                    build_won_quotes_kb(hconn)
                    build_vendor_intel(hconn)
                    hconn.close()
                    log.info("Intelligence tables rebuilt after keyword harvest")
                except Exception as e:
                    log.warning("Intel rebuild after keywords: %s", e)

                _harvest_progress.update(keywords_done=len(HIGH_PRIORITY_KEYWORDS),
                    current_keyword="complete", running=False)
            except Exception as e:
                log.error("Keyword harvest background: %s", e, exc_info=True)
                _harvest_progress.update(running=False, current_keyword=f"error: {str(e)[:50]}")

        threading.Thread(target=_run, daemon=True, name="harvest-keywords").start()
        from src.core.harvest_keywords import HIGH_PRIORITY_KEYWORDS
        return api_response({
            "started": True,
            "keywords": len(HIGH_PRIORITY_KEYWORDS),
            "estimated_pos": "200-1000",
            "note": "Fetching PO detail pages for line items. Monitor via /api/v1/harvest/diagnose"
        })
    except Exception as e:
        log.error("v1/harvest/keywords error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/keywords-sync")
@auth_required
def api_v1_harvest_keywords_sync():
    """Run keyword harvest SYNCHRONOUSLY (first 3 keywords, 5 detail max, 30s timeout each)."""
    try:
        from src.core.harvest_keywords import HIGH_PRIORITY_KEYWORDS
        from src.agents.connectors.ca_scprs import CASCPRSConnector
        from src.core.pull_orchestrator import _store_results, _get_conn
        from datetime import datetime, timedelta
        import time as _t
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        connector = CASCPRSConnector()
        if not connector.authenticate():
            return api_response(error="SCPRS auth failed", status=500)

        from_dt = datetime.now() - timedelta(days=730)
        results_summary = {}
        total_lines = 0

        for kw in HIGH_PRIORITY_KEYWORDS[:3]:
            t0 = _t.time()
            try:
                # 30s timeout per keyword using thread executor
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        connector.search_by_keyword,
                        kw, from_dt, True, 5)  # fetch_detail=True, max_detail=5
                    results = future.result(timeout=30)
                conn = _get_conn()
                stored = _store_results(conn, results, "ca_scprs")
                lines = sum(len(r.get("line_items", [])) for r in results)
                total_lines += lines
                conn.close()
                results_summary[kw] = {
                    "pos": len(results), "stored": stored,
                    "lines": lines, "seconds": round(_t.time() - t0, 1)
                }
            except FuturesTimeout:
                results_summary[kw] = {
                    "pos": 0, "stored": 0, "lines": 0,
                    "seconds": round(_t.time() - t0, 1), "error": "timeout (30s)"
                }
                log.warning("Keyword '%s' timed out after 30s — skipping", kw)
            except Exception as e:
                results_summary[kw] = {"error": str(e)[:100]}
                log.warning("Keyword '%s' failed: %s", kw, e)
            _t.sleep(2)

        return api_response({
            "keywords_run": len(results_summary),
            "results": results_summary,
            "total_line_items": total_lines,
        })
    except Exception as e:
        log.error("v1/harvest/keywords-sync error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/debug-buyer")
@auth_required
def api_v1_debug_buyer():
    """Debug buyer_intel data. Query: ?email=buyer@agency.gov"""
    try:
        import sqlite3, json
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        email = request.args.get("email", "")
        if not email:
            # Pick the top buyer by spend
            r = conn.execute("SELECT buyer_email FROM buyer_intel ORDER BY total_spend DESC LIMIT 1").fetchone()
            email = r[0] if r else ""
        result = {"email": email}
        # POs for this buyer
        pos = conn.execute("SELECT id, po_number, grand_total, institution FROM scprs_po_master WHERE buyer_email=?", (email,)).fetchall()
        result["po_count"] = len(pos)
        result["po_sample"] = [dict(p) for p in pos[:5]]
        # Lines for those POs
        if pos:
            po_ids = [p["id"] for p in pos]
            ph = ",".join("?" * len(po_ids))
            lines = conn.execute(f"SELECT po_id, description, unit_price FROM scprs_po_lines WHERE po_id IN ({ph}) LIMIT 10", po_ids).fetchall()
            result["line_count"] = len(lines)
            result["line_sample"] = [dict(l) for l in lines]
        else:
            result["line_count"] = 0
        # Current buyer_intel record
        bi = conn.execute("SELECT * FROM buyer_intel WHERE buyer_email=?", (email,)).fetchone()
        if bi:
            d = dict(bi)
            d["items_purchased"] = json.loads(d.get("items_purchased", "[]") or "[]")
            result["buyer_intel"] = d
        # Global stats
        result["global"] = {
            "total_buyers": conn.execute("SELECT COUNT(*) FROM buyer_intel").fetchone()[0],
            "with_items": conn.execute("SELECT COUNT(*) FROM buyer_intel WHERE items_purchased != '[]' AND items_purchased IS NOT NULL AND items_purchased != ''").fetchone()[0],
            "with_name": conn.execute("SELECT COUNT(*) FROM buyer_intel WHERE buyer_name IS NOT NULL AND buyer_name != ''").fetchone()[0],
        }
        conn.close()
        return api_response(result)
    except Exception as e:
        log.error("debug-buyer error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


import threading as _threading
_backfill_lock = _threading.Lock()
_backfill_state = {"running": False}  # mutable container — avoids nested global


@bp.route("/api/v1/harvest/debug-detail")
@auth_required
def api_v1_harvest_debug_detail():
    """Minimal standalone test: fresh session, search, click, log everything."""
    try:
        from src.agents.scprs_lookup import (
            FiscalSession, SCPRS_SEARCH_URL, SCPRS_DETAIL_URL,
            ALL_SEARCH_FIELDS, SEARCH_BUTTON, FIELD_SUPPLIER_NAME,
            FIELD_PO_NUM
        )
        from bs4 import BeautifulSoup
        import re as _re

        info = {"steps": []}

        # Step 1: Fresh session
        fs = FiscalSession()
        ok = fs.init_session()
        info["steps"].append({"step": "init", "ok": ok, "icsid": (fs.icsid or "")[:12]})
        if not ok:
            return api_response(info)

        # Step 2: Search (supplier_name=reytech)
        results = fs.search(supplier_name="reytech", from_date="01/01/2022")
        info["steps"].append({
            "step": "search",
            "results": len(results or []),
            "state_num": fs._last_state_num,
            "last_html_size": len(fs._last_html or ""),
        })
        if not results:
            return api_response(info)

        # Step 3: Modal click on row 0
        current_html = fs._last_html
        click_action = "ZZ_SCPR_RSLT_VW$hmodal$0"
        sv = {}
        for fld in ALL_SEARCH_FIELDS:
            m = _re.search(rf"name='{_re.escape(fld)}'[^>]*value=\"([^\"]*)\"", current_html)
            sv[fld] = m.group(1) if m else ""
        fd = fs._build_form_data(current_html, click_action, sv)
        modal_r = fs.session.post(SCPRS_SEARCH_URL, data=fd, timeout=20)
        modal_html = modal_r.text

        # Extract PO numbers from modal
        po_nums = _re.findall(r'4500\d{6}', modal_html)

        # Find all clickable links in modal
        modal_soup = BeautifulSoup(modal_html, "html.parser")
        all_links = [a.get("id", "") for a in modal_soup.find_all("a") if a.get("id")]
        scpr_links = [lid for lid in all_links if "SCPR" in lid]

        info["steps"].append({
            "step": "modal_click",
            "action": click_action,
            "status": modal_r.status_code,
            "size": len(modal_html),
            "has_PDL_DVW": "ZZ_SCPR_PDL_DVW" in modal_html,
            "has_SBP_WRK": "ZZ_SCPR_SBP_WRK" in modal_html,
            "po_numbers": po_nums[:5],
            "scpr_links": scpr_links[:20],
            "all_link_count": len(all_links),
        })

        # Step 4: Try clicking every unique SCPR link from the modal
        # to find which one leads to the detail page
        tried = set()
        for link_id in scpr_links[:5]:
            if link_id in tried or link_id == click_action:
                continue
            tried.add(link_id)

            # Update state from modal response
            new_icsid = fs._extract_icsid(modal_html)
            if new_icsid:
                fs.icsid = new_icsid
            sv2 = {}
            for fld in ALL_SEARCH_FIELDS:
                m = _re.search(rf"name='{_re.escape(fld)}'[^>]*value=\"([^\"]*)\"", modal_html)
                sv2[fld] = m.group(1) if m else ""
            fd2 = fs._build_form_data(modal_html, link_id, sv2)
            try:
                r2 = fs.session.post(SCPRS_SEARCH_URL, data=fd2, timeout=20)
                info["steps"].append({
                    "step": f"click_{link_id}",
                    "status": r2.status_code,
                    "size": len(r2.text),
                    "has_PDL_DVW": "ZZ_SCPR_PDL_DVW" in r2.text,
                    "has_SBP_WRK": "ZZ_SCPR_SBP_WRK" in r2.text,
                    "preview": r2.text[:200].replace("\n", " "),
                })
                if "ZZ_SCPR_PDL_DVW" in r2.text:
                    info["FOUND_DETAIL"] = link_id
                    break
            except Exception as e:
                info["steps"].append({"step": f"click_{link_id}", "error": str(e)})

        # Step 5: Also try searching by PO number directly
        if po_nums:
            fs2 = FiscalSession()
            fs2.init_session()
            page2 = fs2._load_page(2)
            fs2.icsid = fs2._extract_icsid(page2) or fs2.icsid
            sv3 = {f: "" for f in ALL_SEARCH_FIELDS}
            sv3[FIELD_PO_NUM] = po_nums[0]
            fd3 = fs2._build_form_data(page2, SEARCH_BUTTON, sv3)
            r3 = fs2.session.post(SCPRS_SEARCH_URL, data=fd3, timeout=20)
            info["steps"].append({
                "step": f"po_search_{po_nums[0]}",
                "status": r3.status_code,
                "size": len(r3.text),
                "has_PDL_DVW": "ZZ_SCPR_PDL_DVW" in r3.text,
                "has_SBP_WRK": "ZZ_SCPR_SBP_WRK" in r3.text,
                "preview": r3.text[:200].replace("\n", " "),
            })

        # Step 6: Test SCPRS2 via get_po_detail
        if po_nums:
            import requests as _requests
            s2 = _requests.Session()
            s2.headers.update({"User-Agent": fs.session.headers.get("User-Agent", "")})
            # Init SCPRS2 session
            s2_init = s2.get(SCPRS_DETAIL_URL.split("?")[0] + "?&", timeout=20, allow_redirects=True)
            info["steps"].append({
                "step": "scprs2_init",
                "status": s2_init.status_code,
                "size": len(s2_init.text),
                "has_ICSID": "ICSID" in s2_init.text,
                "has_form": "ZZ_SCPRS" in s2_init.text,
            })
            # Now try get_po_detail with that PO
            detail = fs.get_po_detail(po_nums[0], s2=s2)
            info["steps"].append({
                "step": f"get_po_detail_{po_nums[0]}",
                "got_result": detail is not None,
                "line_count": len(detail.get("line_items", [])) if detail else 0,
                "header": detail.get("header", {}) if detail else {},
            })
            if detail and detail.get("line_items"):
                info["FOUND_DETAIL"] = "get_po_detail"

        return api_response(info)
    except Exception as e:
        import traceback
        log.error("debug-detail error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/harvest/debug-modal")
@auth_required
def api_v1_harvest_debug_modal():
    """Analyze modal HTML to find where detail data actually lives."""
    try:
        from src.agents.scprs_lookup import (
            FiscalSession, SCPRS_SEARCH_URL, SCPRS_DETAIL_URL,
            ALL_SEARCH_FIELDS, SEARCH_BUTTON, FIELD_SUPPLIER_NAME
        )
        from bs4 import BeautifulSoup
        import re as _re

        info = {"approaches": []}

        # Fresh session + search
        fs = FiscalSession()
        fs.init_session()
        results = fs.search(supplier_name="reytech", from_date="01/01/2024")
        if not results:
            return api_response({"error": "no search results"})
        info["search_results"] = len(results)

        # Modal click
        current_html = fs._last_html
        click_action = "ZZ_SCPR_RSLT_VW$hmodal$0"
        sv = {}
        for fld in ALL_SEARCH_FIELDS:
            m = _re.search(rf"name='{_re.escape(fld)}'[^>]*value=\"([^\"]*)\"", current_html)
            sv[fld] = m.group(1) if m else ""
        fd = fs._build_form_data(current_html, click_action, sv)
        modal_r = fs.session.post(SCPRS_SEARCH_URL, data=fd, timeout=20)
        modal_html = modal_r.text

        # ANALYSIS 1: Find ALL unique ZZ_ element ID prefixes
        soup = BeautifulSoup(modal_html, "html.parser")
        all_ids = [el.get("id", "") for el in soup.find_all(id=True)]
        zz_prefixes = set()
        for eid in all_ids:
            if eid.startswith("ZZ_"):
                base = _re.sub(r'\$\d+$', '', eid)
                zz_prefixes.add(base)
        info["zz_prefixes"] = sorted(zz_prefixes)
        info["total_elements_with_id"] = len(all_ids)

        # ANALYSIS 2: Find all dollar amounts and their parent elements
        dollar_pattern = _re.compile(r'\$[\d,]+\.\d{2}')
        dollar_elements = []
        for el in soup.find_all(string=dollar_pattern):
            parent = el.parent
            dollar_elements.append({
                "text": el.strip()[:100],
                "parent_id": parent.get("id", ""),
                "parent_tag": parent.name,
                "grandparent_id": parent.parent.get("id", "") if parent.parent else "",
            })
        info["dollar_elements"] = dollar_elements[:30]

        # ANALYSIS 3: Find elements with qty/unit/price/line/item in ID
        detail_fields = []
        for el in soup.find_all(id=_re.compile(r'(?i)(PRICE|QTY|QUANTITY|UNIT|LINE|ITEM|DESCR)')):
            eid = el.get("id", "")
            text = el.get_text(strip=True)[:100]
            if text and text != "\xa0":
                detail_fields.append({"id": eid, "text": text})
        info["detail_fields"] = detail_fields[:50]

        # ANALYSIS 4: Find links with detail/transfer/modal keywords
        all_links = []
        for a in soup.find_all("a"):
            aid = a.get("id", "")
            href = a.get("href", "")[:200]
            onclick = a.get("onclick", "")[:200]
            text = a.get_text(strip=True)[:50]
            if any(kw in (aid + href + onclick).upper() for kw in
                   ["DETAIL", "TRANSFER", "MODAL", "SCPRS2", "PDDTL", "VIEW", "EXPAND"]):
                all_links.append({"id": aid, "href": href, "onclick": onclick, "text": text})
        info["navigation_links"] = all_links[:20]

        # ANALYSIS 5: Find all iframes
        iframes = [{"src": f.get("src", ""), "id": f.get("id", "")}
                   for f in soup.find_all("iframe")]
        info["iframes"] = iframes

        # ANALYSIS 6: Try SCPRS2 with PO as URL search key
        po_nums = _re.findall(r'4500\d{6}', modal_html)
        info["po_numbers"] = po_nums[:5]
        if po_nums:
            import requests as _requests
            for url_pattern in [
                f"{SCPRS_DETAIL_URL}&CRDMEM_ACCT_NBR={po_nums[0]}",
                f"{SCPRS_DETAIL_URL}&ZZ_SCPRS_SP_WRK_CRDMEM_ACCT_NBR={po_nums[0]}",
                SCPRS_DETAIL_URL,
            ]:
                s2 = _requests.Session()
                s2.headers.update({"User-Agent": fs.session.headers.get("User-Agent", "")})
                r2 = s2.get(url_pattern, timeout=20, allow_redirects=True)
                info["approaches"].append({
                    "url_tail": url_pattern.split("GBL")[1] if "GBL" in url_pattern else url_pattern[-80:],
                    "status": r2.status_code,
                    "size": len(r2.text),
                    "has_PDL": "ZZ_SCPR_PDL_DVW" in r2.text,
                    "has_SBP": "ZZ_SCPR_SBP_WRK" in r2.text,
                    "has_lines": bool(_re.search(r'UNIT_PRICE|LINE_TOTAL', r2.text)),
                })

            # ANALYSIS 7: Try with SCPRS1 cookies forwarded to SCPRS2
            s3 = _requests.Session()
            s3.headers.update(fs.session.headers)
            s3.cookies.update(fs.session.cookies)
            r3 = s3.get(SCPRS_DETAIL_URL, timeout=20, allow_redirects=True)
            info["approaches"].append({
                "url_tail": "SCPRS2_with_SCPRS1_cookies",
                "status": r3.status_code,
                "size": len(r3.text),
                "has_PDL": "ZZ_SCPR_PDL_DVW" in r3.text,
                "has_SBP": "ZZ_SCPR_SBP_WRK" in r3.text,
                "has_lines": bool(_re.search(r'UNIT_PRICE|LINE_TOTAL', r3.text)),
            })

        return api_response(info)
    except Exception as e:
        import traceback
        log.error("debug-modal error: %s", e, exc_info=True)
        return api_response(error=f"{e}\n{traceback.format_exc()}", status=500)


@bp.route("/api/v1/harvest/debug-modal2")
@auth_required
def api_v1_harvest_debug_modal2():
    """Find how PeopleSoft actually loads detail data."""
    try:
        from src.agents.scprs_lookup import (
            FiscalSession, SCPRS_SEARCH_URL, SCPRS_BASE,
            ALL_SEARCH_FIELDS, SEARCH_BUTTON, FIELD_PO_NUM
        )
        from bs4 import BeautifulSoup
        import re as _re
        import requests as _requests

        info = {"tests": []}

        # Fresh session + search
        fs = FiscalSession()
        fs.init_session()
        results = fs.search(supplier_name="reytech", from_date="01/01/2024")
        if not results:
            return api_response({"error": "no results"})
        info["result_count"] = len(results)

        current_html = fs._last_html

        # TEST 1: Modal click — search response JS for URLs
        click_action = "ZZ_SCPR_RSLT_VW$hmodal$0"
        sv = {}
        for fld in ALL_SEARCH_FIELDS:
            m = _re.search(rf"name='{_re.escape(fld)}'[^>]*value=\"([^\"]*)\"", current_html)
            sv[fld] = m.group(1) if m else ""
        fd = fs._build_form_data(current_html, click_action, sv)
        modal_r = fs.session.post(SCPRS_SEARCH_URL, data=fd, timeout=20)
        modal_html = modal_r.text

        # Find ALL URLs in the response
        all_urls = set()
        for match in _re.findall(r'(?:href|src|action|url)\s*[=:]\s*["\']([^"\']{10,})["\']', modal_html, _re.IGNORECASE):
            if "fiscal" in match.lower() or "SCPR" in match or "psc/" in match or "psfpd" in match:
                all_urls.add(match[:200])
        for match in _re.findall(r'["\']((https?://[^"\']+|/psc/[^"\']+))["\']', modal_html):
            url = match[0] if isinstance(match, tuple) else match
            if len(url) > 10:
                all_urls.add(url[:200])
        for match in _re.findall(r'(?:Url|URL|url|target|Target)\s*[=:]\s*["\']([^"\']+)["\']', modal_html):
            all_urls.add(match[:200])
        info["urls_in_modal"] = sorted(all_urls)

        # Find JS clues about detail navigation
        js_clues = []
        for pattern in [r'SCPRS2[^"\']{0,100}', r'PDDTL[^"\']{0,100}',
                        r'strModal[^;]{0,200}', r'ModalTarget[^;]{0,200}',
                        r'ICModal[^;]{0,200}', r'win1[^;]{0,200}',
                        r'secondary[^;]{0,200}']:
            for m2 in _re.findall(pattern, modal_html, _re.IGNORECASE):
                js_clues.append(m2[:150])
        info["js_clues"] = js_clues[:30]

        # Hidden inputs with modal/transfer/redirect keywords
        soup = BeautifulSoup(modal_html, "html.parser")
        hidden_modal = {}
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name", "")
            val = inp.get("value", "")
            if any(kw in name.upper() for kw in ["MODAL", "TARGET", "TRANSFER", "REDIRECT", "COMP", "PAGE", "MENU"]):
                hidden_modal[name] = val[:200]
        info["hidden_modal_inputs"] = hidden_modal

        po_nums = _re.findall(r'4500\d{6}', modal_html)
        info["po_numbers"] = po_nums[:5]

        # TEST 2: Non-modal row click (ZZ_SCPR_RSLT_VW$0 without $hmodal$)
        # Re-search first to reset session state
        fs2 = FiscalSession()
        fs2.init_session()
        results2 = fs2.search(supplier_name="reytech", from_date="01/01/2024")
        if results2:
            html2 = fs2._last_html
            sv2 = {}
            for fld in ALL_SEARCH_FIELDS:
                m2 = _re.search(rf"name='{_re.escape(fld)}'[^>]*value=\"([^\"]*)\"", html2)
                sv2[fld] = m2.group(1) if m2 else ""
            fd2 = fs2._build_form_data(html2, "ZZ_SCPR_RSLT_VW$0", sv2)
            bare_r = fs2.session.post(SCPRS_SEARCH_URL, data=fd2, timeout=20)
            info["tests"].append({
                "test": "bare_click_$0",
                "status": bare_r.status_code,
                "size": len(bare_r.text),
                "has_PDL": "ZZ_SCPR_PDL_DVW" in bare_r.text,
                "has_SBP": "ZZ_SCPR_SBP_WRK" in bare_r.text,
                "has_SCPRS2": "ZZ_SCPRS2" in bare_r.text,
                "has_PDDTL": "PDDTL" in bare_r.text,
                "preview": bare_r.text[:300].replace("\n", " "),
            })

        # TEST 3: SCPRS2 on same server (psfpd1 not psfpd1_1)
        scprs2_same = f"{SCPRS_BASE}/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS2_CMP.GBL?Page=ZZ_SCPRS_PDDTL_PG&Action=U"
        r3 = fs.session.get(scprs2_same, timeout=20, allow_redirects=True)
        info["tests"].append({
            "test": "scprs2_psfpd1_GET",
            "status": r3.status_code,
            "size": len(r3.text),
            "has_PDL": "ZZ_SCPR_PDL_DVW" in r3.text,
            "has_SBP": "ZZ_SCPR_SBP_WRK" in r3.text,
            "has_ICSID": "ICSID" in r3.text,
            "preview": r3.text[:300].replace("\n", " "),
        })

        # TEST 4: PO search on SCPRS1 (same component, same server)
        if po_nums:
            fs3 = FiscalSession()
            fs3.init_session()
            po_results = fs3.search(description=po_nums[0])
            info["tests"].append({
                "test": f"scprs1_po_search_{po_nums[0]}",
                "result_count": len(po_results or []),
            })
            # Also try with PO number field
            page3 = fs3._load_page(2)
            fs3.icsid = fs3._extract_icsid(page3) or fs3.icsid
            sv3 = {f: "" for f in ALL_SEARCH_FIELDS}
            sv3[FIELD_PO_NUM] = po_nums[0]
            fd3 = fs3._build_form_data(page3, SEARCH_BUTTON, sv3)
            r4 = fs3.session.post(SCPRS_SEARCH_URL, data=fd3, timeout=20)
            info["tests"].append({
                "test": f"scprs1_po_field_{po_nums[0]}",
                "status": r4.status_code,
                "size": len(r4.text),
                "has_PDL": "ZZ_SCPR_PDL_DVW" in r4.text,
                "has_results": "1 to" in r4.text,
                "preview": r4.text[:300].replace("\n", " "),
            })

        return api_response(info)
    except Exception as e:
        import traceback
        log.error("debug-modal2 error: %s", e, exc_info=True)
        return api_response(error=f"{e}\n{traceback.format_exc()}", status=500)


@bp.route("/api/v1/harvest/debug-click")
@auth_required
def api_v1_harvest_debug_click():
    """Analyze what the non-modal $0 click actually returns."""
    try:
        from src.agents.scprs_lookup import (
            FiscalSession, SCPRS_SEARCH_URL, ALL_SEARCH_FIELDS
        )
        from bs4 import BeautifulSoup
        import re as _re

        info = {}

        fs = FiscalSession()
        fs.init_session()
        results = fs.search(supplier_name="reytech", from_date="01/01/2024")
        if not results:
            return api_response({"error": "no results"})
        info["search_results"] = len(results)

        current_html = fs._last_html

        # Non-modal click: $0 (not $hmodal$0)
        sv = {}
        for fld in ALL_SEARCH_FIELDS:
            m = _re.search(rf"name='{_re.escape(fld)}'[^>]*value=\"([^\"]*)\"", current_html)
            sv[fld] = m.group(1) if m else ""
        fd = fs._build_form_data(current_html, "ZZ_SCPR_RSLT_VW$0", sv)
        r = fs.session.post(SCPRS_SEARCH_URL, data=fd, timeout=30)
        html = r.text
        info["click_size"] = len(html)
        info["status"] = r.status_code

        soup = BeautifulSoup(html, "html.parser")

        # 1. ALL unique ZZ_ prefixes
        zz_prefixes = set()
        for el in soup.find_all(id=_re.compile(r'^ZZ_')):
            base = _re.sub(r'\$\d+$', '', el.get("id", ""))
            zz_prefixes.add(base)
        info["zz_prefixes"] = sorted(zz_prefixes)

        # 2. ALL dollar amounts with parent IDs
        dollar_elements = []
        for el in soup.find_all(string=_re.compile(r'\$[\d,]+\.\d{2}')):
            p = el.parent
            gp = p.parent if p.parent else p
            dollar_elements.append({
                "text": el.strip()[:80],
                "parent_id": p.get("id", "")[:80],
                "gp_id": gp.get("id", "")[:80],
            })
        info["dollar_count"] = len(dollar_elements)
        info["dollar_elements"] = dollar_elements[:20]

        # 3. Fields with price/qty/unit/line keywords
        detail_data = []
        for el in soup.find_all(id=_re.compile(r'(?i)(PRICE|QTY|QUANTITY|UNIT|LINE|ITEM_ID|UOM|DESCR254)')):
            eid = el.get("id", "")
            text = el.get_text(strip=True)[:120]
            if text and text != "\xa0":
                detail_data.append({"id": eid, "text": text})
        info["detail_data"] = detail_data[:40]

        # 4. Page title
        title = _re.search(r'<title>([^<]*)</title>', html)
        info["title"] = title.group(1)[:100] if title else "?"

        # 5. PO numbers found
        po_nums = _re.findall(r'4500\d{6}', html)
        info["po_numbers"] = list(dict.fromkeys(po_nums))[:10]

        # 6. Any elements with BUYER, SUPPLIER, STATUS
        header_data = []
        for el in soup.find_all(id=_re.compile(r'(?i)(BUYER|SUPPLIER|STATUS|AWARDED|MERCH|FREIGHT|PHONE|EMAIL|START_DATE|END_DATE)')):
            eid = el.get("id", "")
            text = el.get_text(strip=True)[:120]
            if text and text != "\xa0":
                header_data.append({"id": eid, "text": text})
        info["header_data"] = header_data[:30]

        # 7. Forms and their actions
        forms = [{"id": f.get("id", ""), "name": f.get("name", ""),
                  "action": f.get("action", "")[:200]}
                 for f in soup.find_all("form")]
        info["forms"] = forms

        # 8. Non-ZZ unique ID prefixes
        other_prefixes = set()
        for el in soup.find_all(id=True):
            eid = el.get("id", "")
            if not eid.startswith("ZZ_") and not eid.startswith("win0"):
                base = _re.sub(r'[\$_]\d+$', '', eid)
                if len(base) > 3:
                    other_prefixes.add(base)
        info["other_prefixes_sample"] = sorted(other_prefixes)[:30]

        return api_response(info)
    except Exception as e:
        import traceback
        log.error("debug-click: %s", e, exc_info=True)
        return api_response(error=f"{e}\n{traceback.format_exc()}", status=500)


@bp.route("/api/v1/harvest/browser-screenshot")
@auth_required
def api_v1_browser_screenshot():
    """Serve browser screenshots."""
    import os
    from flask import send_file
    name = request.args.get("name", "scprs_click")
    path = f"/data/{name}.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png")
    shots = [f for f in os.listdir("/data") if f.endswith(".png")]
    return api_response({"available": shots, "requested": name}, status=404)


@bp.route("/api/v1/harvest/browser-screenshots")
@auth_required
def api_v1_browser_screenshots():
    """List all available browser screenshots."""
    import os
    shots = sorted([f for f in os.listdir("/data") if f.startswith("scprs_") and f.endswith(".png")])
    base_url = request.host_url.rstrip("/")
    return api_response({
        "screenshots": [
            {"name": f.replace(".png", ""), "url": f"{base_url}/api/v1/harvest/browser-screenshot?name={f.replace('.png', '')}"}
            for f in shots
        ]
    })


@bp.route("/api/v1/harvest/browser-test")
@auth_required
def api_v1_harvest_browser_test():
    """Test Playwright-based SCPRS detail scraping."""
    try:
        from src.agents.scprs_browser import scrape_details
        results = scrape_details(
            supplier_name="reytech",
            from_date="",
            max_rows=200
        )

        # Store in won_quotes KB
        ingested = 0
        try:
            from src.knowledge.won_quotes_db import ingest_scprs_result
            for r in results:
                header = r.get("header", {})
                for line in r.get("line_items", []):
                    up = line.get("unit_price_num")
                    if up and up > 0:
                        try:
                            ingest_scprs_result(
                                po_number=header.get("po_number", ""),
                                item_number=line.get("item_id", ""),
                                description=line.get("description", ""),
                                unit_price=up,
                                quantity=line.get("quantity_num", 1) or 1,
                                supplier=header.get("supplier", ""),
                                department=header.get("dept_name", ""),
                                award_date=header.get("start_date", ""),
                                source="scprs_browser",
                            )
                            ingested += 1
                        except Exception:
                            pass
        except ImportError:
            pass

        return api_response({
            "count": len(results),
            "ingested": ingested,
            "results": [
                {
                    "po": r.get("header", {}).get("po_number"),
                    "lines": len(r.get("line_items", [])),
                    "buyer": r.get("header", {}).get("buyer_name"),
                    "items": [
                        {
                            "desc": li.get("description", "")[:60],
                            "price": li.get("unit_price"),
                            "qty": li.get("quantity"),
                        }
                        for li in r.get("line_items", [])[:5]
                    ],
                }
                for r in results
            ],
        })
    except Exception as e:
        import traceback
        return api_response(
            error=f"{e}\n{traceback.format_exc()}",
            status=500
        )


@bp.route("/api/v1/harvest/fiscal-scrape-now")
@auth_required
def api_v1_fiscal_scrape_now():
    """Manually trigger full FI$Cal exhaustive scrape."""
    import threading as _th
    from src.agents.scprs_browser import _run_exhaustive_scrape
    t = _th.Thread(target=_run_exhaustive_scrape, daemon=True, name="fiscal-manual")
    t.start()
    return api_response({"status": "started", "message": "Full FI$Cal scrape running. Check logs."})


@bp.route("/api/v1/harvest/fiscal-scrape-status")
@auth_required
def api_v1_fiscal_scrape_status():
    """Check scrape progress across all data layers."""
    import os
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.row_factory = sqlite3.Row
        po_count = db.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
        line_count = db.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
        try:
            catalog_count = db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
        except Exception:
            catalog_count = 0
        reytech_pos = db.execute(
            "SELECT COUNT(*) FROM scprs_po_master WHERE UPPER(supplier) LIKE '%REYTECH%'"
        ).fetchone()[0]
        top_suppliers = db.execute("""
            SELECT supplier, COUNT(*) as cnt FROM scprs_po_master
            GROUP BY supplier ORDER BY cnt DESC LIMIT 20
        """).fetchall()
        top_depts = db.execute("""
            SELECT dept_name, COUNT(*) as cnt FROM scprs_po_master
            GROUP BY dept_name ORDER BY cnt DESC LIMIT 20
        """).fetchall()
        latest = db.execute("""
            SELECT po_number, supplier, grand_total, start_date
            FROM scprs_po_master ORDER BY rowid DESC LIMIT 10
        """).fetchall()
        db.close()
    except Exception as e:
        return api_response({"error": str(e)})

    po_screenshots = 0
    po_htmls = 0
    try:
        records_dir = "/data/po_records"
        if os.path.exists(records_dir):
            files = os.listdir(records_dir)
            po_screenshots = len([f for f in files if f.endswith(".png")])
            po_htmls = len([f for f in files if f.endswith(".html")])
    except Exception:
        pass

    return api_response({
        "layer1_raw_fiscal": {"total_pos": po_count, "total_line_items": line_count, "reytech_pos": reytech_pos},
        "layer3_catalog": {"unique_items": catalog_count},
        "po_records": {"screenshots": po_screenshots, "html_backups": po_htmls},
        "top_suppliers": [{"supplier": s[0], "count": s[1]} for s in top_suppliers],
        "top_departments": [{"dept": d[0], "count": d[1]} for d in top_depts],
        "latest_scraped": [{"po": l[0], "supplier": l[1], "total": l[2], "date": l[3]} for l in latest],
    })


@bp.route("/api/v1/harvest/po-screenshot/<po_number>")
@auth_required
def api_v1_po_screenshot(po_number):
    """Serve stored PO screenshot."""
    import os
    from flask import send_file
    path = f"/data/po_records/{po_number}.png"
    if os.path.exists(path):
        return send_file(path, mimetype="image/png")
    return api_response(error=f"No screenshot for {po_number}", status=404)


@bp.route("/api/v1/harvest/populate-catalog")
@auth_required
def api_v1_populate_catalog():
    """One-time: populate scprs_catalog from existing PO line data."""
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=30)

        po_lines_count = db.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]

        if po_lines_count == 0:
            try:
                from src.knowledge.won_quotes_db import get_db as get_wq_db
                wq = get_wq_db()
                rows = wq.execute("""
                    SELECT description, unit_price, quantity, supplier,
                           department, po_number, award_date, item_number
                    FROM won_quotes WHERE unit_price > 0
                """).fetchall()
                for r in rows:
                    desc = (r[0] or "")[:500]
                    price = r[1] or 0
                    if price <= 0 or not desc:
                        continue
                    try:
                        db.execute("""
                            INSERT INTO scprs_catalog
                            (description, unspsc, last_unit_price, last_quantity,
                             last_uom, last_supplier, last_department,
                             last_po_number, last_date, times_seen, updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?,1,datetime('now'))
                            ON CONFLICT(description) DO UPDATE SET
                                times_seen = scprs_catalog.times_seen + 1,
                                updated_at = datetime('now')
                        """, (desc, "", price, r[2] or 1, "", r[3] or "", r[4] or "", r[5] or "", r[6] or ""))
                    except Exception:
                        pass
                db.commit()
                catalog_count = db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
                db.close()
                return api_response({
                    "source": "won_quotes_db",
                    "source_rows": len(rows),
                    "catalog_items": catalog_count
                })
            except Exception as e:
                db.close()
                return api_response({"error": f"won_quotes fallback failed: {e}"})

        rows = db.execute("""
            SELECT l.description, l.unspsc, l.unit_price, l.quantity,
                   l.uom, m.supplier, m.dept_name, l.po_number, m.start_date
            FROM scprs_po_lines l
            JOIN scprs_po_master m ON l.po_number = m.po_number
            WHERE l.description != '' AND l.unit_price != ''
        """).fetchall()

        inserted = 0
        for r in rows:
            desc = (r[0] or "")[:500]
            try:
                price = float(str(r[2]).replace("$", "").replace(",", "").strip())
            except (ValueError, TypeError):
                continue
            if price <= 0 or not desc:
                continue
            try:
                qty = float(str(r[3]).replace(",", "").strip()) if r[3] else 1
            except (ValueError, TypeError):
                qty = 1
            try:
                db.execute("""
                    INSERT INTO scprs_catalog
                    (description, unspsc, last_unit_price, last_quantity,
                     last_uom, last_supplier, last_department,
                     last_po_number, last_date, times_seen, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,1,datetime('now'))
                    ON CONFLICT(description) DO UPDATE SET
                        last_unit_price = CASE WHEN excluded.last_date > scprs_catalog.last_date
                            THEN excluded.last_unit_price ELSE scprs_catalog.last_unit_price END,
                        last_supplier = CASE WHEN excluded.last_date > scprs_catalog.last_date
                            THEN excluded.last_supplier ELSE scprs_catalog.last_supplier END,
                        times_seen = scprs_catalog.times_seen + 1,
                        updated_at = datetime('now')
                """, (desc, r[1] or "", price, qty, r[4] or "", r[5] or "", r[6] or "", r[7] or "", r[8] or ""))
                inserted += 1
            except Exception:
                pass
        db.commit()
        catalog_count = db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
        db.close()
        return api_response({
            "source": "scprs_po_lines",
            "source_rows": len(rows),
            "catalog_items_populated": inserted,
            "catalog_total": catalog_count
        })
    except Exception as e:
        import traceback
        return api_response(error=f"{e}\n{traceback.format_exc()}", status=500)


@bp.route("/api/v1/buyers/refresh")
@auth_required
def api_v1_buyers_refresh():
    """Rebuild all buyer profiles from FI$Cal data."""
    from src.agents.buyer_intelligence import refresh_buyer_profiles
    count = refresh_buyer_profiles()
    return api_response({"buyers_updated": count})


@bp.route("/api/v1/buyers/prospects")
@auth_required
def api_v1_buyers_prospects():
    """Get ranked prospect list for outreach."""
    from src.agents.buyer_intelligence import get_top_prospects
    limit = min(int(request.args.get("limit", "50")), 200)
    min_score = float(request.args.get("min_score", "20"))
    new_only = request.args.get("new_only", "false").lower() == "true"
    prospects = get_top_prospects(limit=limit, min_score=min_score, exclude_customers=new_only)
    return api_response({"prospects": prospects, "count": len(prospects)})


@bp.route("/api/v1/buyers/profile/<path:email>")
@auth_required
def api_v1_buyer_profile(email):
    """Get full buyer profile with history and Reytech overlap."""
    from src.agents.buyer_intelligence import get_buyer_profile
    profile = get_buyer_profile(email)
    if not profile:
        return api_response(error=f"Buyer {email} not found", status=404)
    return api_response(profile)


@bp.route("/api/v1/buyers/search")
@auth_required
def api_v1_buyers_search():
    """Search buyers by name, department, or category."""
    import sqlite3
    from src.core.db import DB_PATH
    q = request.args.get("q", "")
    if not q:
        return api_response(error="No query", status=400)
    db = sqlite3.connect(DB_PATH, timeout=10)
    rows = db.execute("""
        SELECT buyer_email, buyer_name, department,
               total_pos, total_spend, prospect_score,
               relationship_status, top_categories
        FROM scprs_buyers
        WHERE LOWER(buyer_name) LIKE ?
           OR LOWER(department) LIKE ?
           OR LOWER(top_categories) LIKE ?
           OR LOWER(buyer_email) LIKE ?
        ORDER BY prospect_score DESC LIMIT 25
    """, (f"%{q.lower()}%",) * 4).fetchall()
    db.close()
    return api_response({
        "query": q,
        "results": [{
            "email": r[0], "name": r[1], "department": r[2],
            "total_pos": r[3], "total_spend": r[4],
            "prospect_score": r[5], "status": r[6], "categories": r[7],
        } for r in rows],
    })


@bp.route("/api/v1/harvest/fire-all-now")
@auth_required
def api_v1_fire_all_now():
    """Trigger entire pipeline immediately."""
    import threading as _th

    def _run_full_pipeline():
        import time as _time
        _log = log

        _log.info("FULL PIPELINE — STARTING NOW")

        # Step 1: Populate catalog
        _log.info("Step 1: Populating catalog...")
        try:
            import sqlite3
            from src.core.db import DB_PATH
            _db = sqlite3.connect(DB_PATH, timeout=30)
            try:
                from src.knowledge.won_quotes_db import get_db as _gwq
                _wq = _gwq()
                _rows = _wq.execute(
                    "SELECT description, unit_price, quantity, supplier, "
                    "department, po_number, award_date FROM won_quotes WHERE unit_price > 0"
                ).fetchall()
                for _r in _rows:
                    _desc = (_r[0] or "")[:500]
                    if not _desc or not _r[1] or _r[1] <= 0:
                        continue
                    try:
                        _db.execute(
                            "INSERT OR IGNORE INTO scprs_catalog "
                            "(description, last_unit_price, last_quantity, "
                            "last_supplier, last_department, last_po_number, "
                            "last_date, times_seen, updated_at) "
                            "VALUES (?,?,?,?,?,?,?,1,datetime('now'))",
                            (_desc, _r[1], _r[2] or 1, _r[3] or "", _r[4] or "", _r[5] or "", _r[6] or ""))
                    except Exception:
                        pass
                _db.commit()
                _log.info("Step 1 done: catalog has %d items",
                          _db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0])
            except Exception as _e:
                _log.warning("Step 1 catalog: %s", _e)
            _db.close()
        except Exception as _e:
            _log.error("Step 1 failed: %s", _e)

        # Step 2: Exhaustive FI$Cal scrape
        _log.info("Step 2: FI$Cal exhaustive scrape starting...")
        try:
            from src.agents.scprs_browser import _run_exhaustive_scrape
            _run_exhaustive_scrape()
        except Exception as _e:
            _log.error("Step 2 scrape failed: %s", _e)

        # Step 3: Enrich catalog identifiers
        _log.info("Step 3: Enriching catalog identifiers...")
        try:
            from src.agents.item_enricher import enrich_catalog
            enrich_catalog()
        except Exception as _e:
            _log.error("Step 3 enrich failed: %s", _e)

        # Step 4: Refresh buyer profiles
        _log.info("Step 4: Refreshing buyer profiles...")
        try:
            from src.agents.buyer_intelligence import refresh_buyer_profiles
            refresh_buyer_profiles()
        except Exception as _e:
            _log.error("Step 4 buyers failed: %s", _e)

        # Step 5: Reprocess quotes
        _log.info("Step 5: Reprocessing quotes...")
        try:
            from src.agents.quote_reprocessor import reprocess_all_quotes
            reprocess_all_quotes()
        except Exception as _e:
            _log.error("Step 5 reprocess failed: %s", _e)

        # Step 6: System audit
        _log.info("Step 6: Running system audit...")
        try:
            from src.agents.system_auditor import run_full_audit
            run_full_audit()
        except Exception as _e:
            _log.error("Step 6 audit failed: %s", _e)

        _log.info("FULL PIPELINE — COMPLETE")

    t = _th.Thread(target=_run_full_pipeline, daemon=True, name="fire-all-now")
    t.start()

    return api_response({
        "status": "started",
        "pipeline": [
            "Step 1: Populate catalog from won_quotes",
            "Step 2: Exhaustive FI$Cal scrape (all POs since 2019)",
            "Step 3: Enrich catalog with MFG#/UPC/ASIN",
            "Step 4: Refresh buyer profiles",
            "Step 5: Reprocess pending + validate sent quotes",
            "Step 6: System audit",
        ],
        "message": "Full pipeline running. Check /api/v1/harvest/fiscal-scrape-status for progress."
    })


@bp.route("/api/v1/quotes/underpriced")
@auth_required
def api_v1_quotes_underpriced():
    """Sent quotes that were underpriced vs market."""
    try:
        from src.agents.quote_reprocessor import get_underpriced_report
        report = get_underpriced_report()
        total_left = sum(r.get("gap_total", 0) for r in report if r.get("gap_total", 0) > 0)
        return api_response({
            "underpriced_items": report,
            "count": len(report),
            "total_margin_left_on_table": round(total_left, 2),
        })
    except Exception as e:
        return api_response({"underpriced_items": [], "count": 0, "error": str(e)})


@bp.route("/api/v1/system/validate-data")
@auth_required
def api_v1_validate_data():
    """Run data validation checks across all layers."""
    from src.agents.data_validator import validate_all
    report = validate_all()
    return api_response(report)


@bp.route("/api/v1/usage/track", methods=["POST"])
@auth_required
def api_v1_usage_track():
    """Receive usage tracking beacons from frontend."""
    try:
        from src.core.usage_tracker import track_action
        data = request.get_json(silent=True) or {}
        track_action(page=data.get("page", ""), action=data.get("action", ""),
                     detail=data.get("type", "click"))
    except Exception:
        pass
    return "", 204


@bp.route("/api/v1/usage/stats")
@auth_required
def api_v1_usage_stats():
    """Get usage analytics."""
    from src.core.usage_tracker import get_usage_stats
    days = int(request.args.get("days", "30"))
    return api_response(get_usage_stats(days))


@bp.route("/api/v1/usage/dead-pages")
@auth_required
def api_v1_usage_dead_pages():
    """Find pages with zero usage."""
    from src.core.usage_tracker import get_dead_pages
    days = int(request.args.get("days", "30"))
    return api_response(get_dead_pages(days))


@bp.route("/api/v1/pricing/lookup")
@auth_required
def api_v1_pricing_lookup():
    """Unified pricing lookup — THE endpoint for all pricing."""
    from src.core.pricing_oracle_v2 import get_pricing
    q = request.args.get("q", "")
    qty = float(request.args.get("qty", "1"))
    cost_str = request.args.get("cost")
    cost = float(cost_str) if cost_str else None
    item_num = request.args.get("item_number", "")
    if not q:
        return api_response(error="No query", status=400)
    result = get_pricing(q, quantity=qty, cost=cost, item_number=item_num)
    try:
        from src.core.usage_tracker import track_feature
        track_feature("pricing_lookup", q[:60])
    except Exception:
        pass
    return api_response(result)


@bp.route("/api/v1/pricing/confirm-item", methods=["POST"])
@auth_required
def api_v1_pricing_confirm_item():
    """Confirm an item mapping — persists forever."""
    from src.core.pricing_oracle_v2 import confirm_item_mapping
    data = request.get_json(force=True, silent=True) or {}
    confirm_item_mapping(
        original_description=data.get("original", ""),
        canonical_description=data.get("canonical", data.get("original", "")),
        item_number=data.get("item_number", ""),
        mfg_number=data.get("mfg_number", ""),
        product_url=data.get("product_url", ""),
        supplier=data.get("supplier", ""),
        cost=data.get("cost"),
    )
    return api_response({"status": "confirmed"})


@bp.route("/api/v1/pricing/lock-cost", methods=["POST"])
@auth_required
def api_v1_pricing_lock_cost():
    """Lock a supplier cost with expiry."""
    from src.core.pricing_oracle_v2 import lock_cost
    data = request.get_json(force=True, silent=True) or {}
    lock_cost(
        description=data.get("description", ""),
        cost=float(data.get("cost", 0)),
        supplier=data.get("supplier", ""),
        source=data.get("source", "manual"),
        expires_days=int(data.get("expires_days", 30)),
        item_number=data.get("item_number", ""),
    )
    return api_response({"status": "locked"})


@bp.route("/api/v1/pricing/expiring-costs")
@auth_required
def api_v1_pricing_expiring_costs():
    from src.core.pricing_oracle_v2 import get_expiring_costs
    days = int(request.args.get("days", "7"))
    return api_response({"expiring": get_expiring_costs(days)})


@bp.route("/api/v1/pricing/speed-stats")
@auth_required
def api_v1_pricing_speed_stats():
    from src.core.pricing_oracle_v2 import get_speed_stats
    return api_response(get_speed_stats())


@bp.route("/api/v1/pricing/backfill-memory")
@auth_required
def api_v1_pricing_backfill_memory():
    """Backfill item memory from all existing PCs, quotes, RFQs."""
    from src.core.pricing_oracle_v2 import backfill_item_memory
    result = backfill_item_memory()
    return api_response(result)


@bp.route("/api/v1/pricing/cross-sell")
@auth_required
def api_v1_pricing_cross_sell():
    from src.core.pricing_oracle_v2 import _get_cross_sell
    import sqlite3
    from src.core.db import DB_PATH
    q = request.args.get("q", "")
    if not q:
        return api_response(error="No query", status=400)
    db = sqlite3.connect(DB_PATH, timeout=10)
    result = _get_cross_sell(db, q)
    db.close()
    return api_response({"suggestions": result})


@bp.route("/api/v1/recovery/restore-from-db-backup")
@auth_required
def api_v1_recovery_from_db():
    """Restore rfqs + price_checks from the SQLite DB backup file."""
    import json as _json
    import os as _os
    import shutil as _shutil
    import sqlite3 as _sqlite3

    results = {"steps": [], "restored_rfqs": 0, "restored_pcs": 0}

    # Find backup DB
    backup_path = None
    for d in ["/data", "/data/backups"]:
        if not _os.path.exists(d):
            continue
        for f in sorted(_os.listdir(d), reverse=True):
            if "20260315" in f and f.endswith(".db"):
                backup_path = _os.path.join(d, f)
                break
        if backup_path:
            break

    if not backup_path or not _os.path.exists(backup_path):
        # List all .db files to help find it
        all_dbs = []
        for d in ["/data", "/data/backups"]:
            if _os.path.exists(d):
                all_dbs.extend([f for f in _os.listdir(d) if f.endswith(".db")])
        return api_response({"error": "Backup DB not found", "available_dbs": all_dbs})

    results["backup_file"] = backup_path
    results["steps"].append(f"Found backup: {backup_path}")

    try:
        bak = _sqlite3.connect(backup_path, timeout=10)

        # Check rfqs in backup
        try:
            rfq_rows = bak.execute("SELECT id, items, status FROM rfqs").fetchall()
            results["backup_rfqs"] = len(rfq_rows)
            rfq_details = []
            for r in rfq_rows:
                items = _json.loads(r[1] or "[]") if r[1] else []
                rfq_details.append({"id": r[0], "items": len(items), "status": r[2]})
            results["rfq_details"] = rfq_details
        except Exception as e:
            results["steps"].append(f"rfqs table: {e}")
            rfq_rows = []

        # Check price_checks in backup
        try:
            pc_rows = bak.execute("SELECT id, pc_data, items, status FROM price_checks").fetchall()
            results["backup_pcs"] = len(pc_rows)
            pc_details = []
            for r in pc_rows:
                pc_data = _json.loads(r[1] or "{}") if r[1] else {}
                items_col = _json.loads(r[2] or "[]") if r[2] else []
                items = pc_data.get("items", items_col)
                if isinstance(items, str):
                    items = _json.loads(items)
                pc_details.append({"id": r[0], "items": len(items), "status": r[3]})
            results["pc_details"] = pc_details
        except Exception as e:
            results["steps"].append(f"price_checks table: {e}")
            pc_rows = []

        bak.close()

        # Restore to live DB
        live = _sqlite3.connect("/data/reytech.db", timeout=30)
        restored_rfqs = 0
        for r in rfq_rows:
            items = _json.loads(r[1] or "[]") if r[1] else []
            if items:
                try:
                    live.execute("UPDATE rfqs SET items=?, updated_at=datetime('now') WHERE id=?",
                                 (_json.dumps(items, default=str), r[0]))
                    restored_rfqs += 1
                except Exception:
                    pass
        live.commit()
        results["restored_rfqs_to_sqlite"] = restored_rfqs
        results["steps"].append(f"Restored {restored_rfqs} RFQs to SQLite")

        restored_pcs = 0
        for r in pc_rows:
            pc_data = _json.loads(r[1] or "{}") if r[1] else {}
            items_col = _json.loads(r[2] or "[]") if r[2] else []
            if pc_data.get("items") or items_col:
                try:
                    live.execute("UPDATE price_checks SET pc_data=?, items=?, updated_at=datetime('now') WHERE id=?",
                                 (_json.dumps(pc_data, default=str), _json.dumps(items_col, default=str), r[0]))
                    restored_pcs += 1
                except Exception:
                    pass
        live.commit()
        live.close()
        results["restored_pcs_to_sqlite"] = restored_pcs
        results["steps"].append(f"Restored {restored_pcs} PCs to SQLite")

        # Restore rfqs.json
        rfqs_path = "/data/rfqs.json"
        try:
            with open(rfqs_path) as f:
                current = _json.load(f)
        except Exception:
            current = {}

        bak = _sqlite3.connect(backup_path, timeout=10)
        bak.row_factory = _sqlite3.Row
        rows = bak.execute("SELECT * FROM rfqs").fetchall()
        json_restored = 0
        for row in rows:
            rid = row["id"]
            items = _json.loads(row["items"] or "[]") if row["items"] else []
            if rid in current:
                cur_items = current[rid].get("line_items", [])
                if not cur_items and items:
                    current[rid]["line_items"] = items
                    json_restored += 1
            else:
                current[rid] = {"line_items": items, "status": row["status"] or "draft",
                                "solicitation_number": row["rfq_number"] or "",
                                "created_at": row["received_at"] or ""}
                json_restored += 1

        # Backup current and write
        if _os.path.exists(rfqs_path):
            _shutil.copy2(rfqs_path, rfqs_path + ".pre_restore")
        with open(rfqs_path, "w") as f:
            _json.dump(current, f, indent=2, default=str)
        bak.close()
        results["json_rfqs_restored"] = json_restored
        results["steps"].append(f"rfqs.json: {json_restored} RFQs restored, {len(current)} total")

        # Restore price_checks.json
        pc_path = "/data/price_checks.json"
        try:
            with open(pc_path) as f:
                current_pcs = _json.load(f)
        except Exception:
            current_pcs = {}

        bak = _sqlite3.connect(backup_path, timeout=10)
        bak.row_factory = _sqlite3.Row
        rows = bak.execute("SELECT * FROM price_checks").fetchall()
        pc_json_restored = 0
        for row in rows:
            pcid = row["id"]
            pc_data = _json.loads(row["pc_data"] or "{}") if row["pc_data"] else {}
            items = _json.loads(row["items"] or "[]") if row["items"] else []
            if not pc_data.get("items"):
                pc_data["items"] = items

            if pcid in current_pcs:
                cpd = current_pcs[pcid].get("pc_data", current_pcs[pcid])
                if isinstance(cpd, str):
                    try:
                        cpd = _json.loads(cpd)
                    except Exception:
                        cpd = {}
                if not cpd.get("items") and pc_data.get("items"):
                    current_pcs[pcid]["pc_data"] = pc_data
                    pc_json_restored += 1
            else:
                current_pcs[pcid] = {"pc_data": pc_data, "items": items,
                                     "status": row["status"] or "parsed"}
                pc_json_restored += 1

        if _os.path.exists(pc_path):
            _shutil.copy2(pc_path, pc_path + ".pre_restore")
        with open(pc_path, "w") as f:
            _json.dump(current_pcs, f, indent=2, default=str)
        bak.close()
        results["json_pcs_restored"] = pc_json_restored
        results["steps"].append(f"price_checks.json: {pc_json_restored} PCs restored, {len(current_pcs)} total")

        results["steps"].append("RECOVERY COMPLETE — reload app to verify")

    except Exception as e:
        import traceback
        results["error"] = str(e)
        results["traceback"] = traceback.format_exc()

    return api_response(results)


@bp.route("/api/v1/recovery/restore-from-drive")
@auth_required
def api_v1_recovery_restore():
    """Restore rfqs.json and price_checks.json from latest Google Drive backup."""
    import json as _json
    import os as _os
    results = {"steps": [], "restored_rfqs": 0, "restored_pcs": 0}

    try:
        from src.core.gdrive import is_configured, list_files, download_file, get_folder_path

        if not is_configured():
            return api_response({"error": "Drive not configured"})

        results["steps"].append("Drive configured")

        # Find backup folders
        backups_root = get_folder_path(category="Backups")
        folders = list_files(backups_root)
        sorted_folders = sorted(folders, key=lambda x: x.get("name", ""), reverse=True)
        results["backup_folders"] = [f["name"] for f in sorted_folders[:5]]

        if not sorted_folders:
            return api_response({"error": "No backup folders found"})

        latest = sorted_folders[0]
        results["restoring_from"] = latest["name"]
        backup_files = list_files(latest["id"])
        results["backup_files"] = [f["name"] for f in backup_files]

        # Restore rfqs.json
        for bf in backup_files:
            if bf["name"] == "rfqs.json":
                content = download_file(bf["id"])
                data = _json.loads(content)
                total_items = sum(len(r.get("line_items", [])) for r in data.values())
                results["steps"].append(f"Downloaded rfqs.json: {len(data)} RFQs, {total_items} items")

                if total_items > 0:
                    # Import directly to SQLite (single source of truth)
                    try:
                        from src.api.dashboard import _save_single_rfq
                        for rid, r in data.items():
                            _save_single_rfq(rid, r)
                    except Exception as _re:
                        results["steps"].append(f"SQLite import error: {_re}")
                    results["restored_rfqs"] = len(data)
                    results["restored_rfq_items"] = total_items
                    results["steps"].append(f"RESTORED {len(data)} RFQs to SQLite, {total_items} items")
                else:
                    results["steps"].append("WARNING: backup rfqs.json has 0 items")

        # Restore price_checks.json
        for bf in backup_files:
            if bf["name"] == "price_checks.json":
                content = download_file(bf["id"])
                data = _json.loads(content)
                total_items = 0
                for pc in data.values():
                    items = pc.get("items", [])
                    if not items:
                        pd = pc.get("pc_data", {})
                        if isinstance(pd, str):
                            pd = _json.loads(pd)
                        items = pd.get("items", [])
                    total_items += len(items)
                results["steps"].append(f"Downloaded price_checks.json: {len(data)} PCs, {total_items} items")

                if total_items > 0:
                    # Import directly to SQLite (single source of truth)
                    try:
                        from src.api.dashboard import _save_single_pc
                        for pcid, pc in data.items():
                            _save_single_pc(pcid, pc)
                    except Exception as _pe:
                        results["steps"].append(f"SQLite import error: {_pe}")
                    results["restored_pcs"] = len(data)
                    results["restored_pc_items"] = total_items
                    results["steps"].append(f"RESTORED {len(data)} PCs to SQLite, {total_items} items")

        results["steps"].append("DONE — reload the app to pick up restored data")
    except Exception as e:
        import traceback
        results["error"] = str(e)
        results["traceback"] = traceback.format_exc()

    return api_response(results)


@bp.route("/api/v1/debug/rfq-raw/<rid>")
@auth_required
def api_v1_debug_rfq_raw(rid):
    """Show raw RFQ data to debug missing items."""
    import json as _json
    import os as _os
    result = {"rid": rid}
    try:
        rfq_path = _os.path.join(_os.environ.get("DATA_DIR", "/data"), "rfqs.json")
        with open(rfq_path) as f:
            rfqs = _json.load(f)
        r = rfqs.get(rid)
        if not r:
            return api_response({"error": "RFQ not found", "available": list(rfqs.keys())})
        result["keys"] = list(r.keys())
        result["line_items_count"] = len(r.get("line_items", []))
        result["items_count"] = len(r.get("items", []))
        result["status"] = r.get("status")
        # Show first item from each key
        if r.get("line_items"):
            result["first_line_item"] = r["line_items"][0] if r["line_items"] else None
        if r.get("items"):
            result["first_item"] = r["items"][0] if r["items"] else None
        # Check for items_detail or other keys
        for k in r.keys():
            if "item" in k.lower() and isinstance(r[k], list) and r[k]:
                result[f"found_in_{k}"] = len(r[k])
    except Exception as e:
        result["error"] = str(e)
    return api_response(result)


@bp.route("/api/v1/debug/rfq-status")
@auth_required
def api_v1_debug_rfq_status():
    """Check all RFQs for empty items and attempt recovery from DB."""
    import json as _json
    import os as _os
    results = {"rfqs": [], "recovered": 0}
    try:
        rfq_path = _os.path.join(_os.environ.get("DATA_DIR", "/data"), "rfqs.json")
        if _os.path.exists(rfq_path):
            with open(rfq_path) as f:
                rfqs = _json.load(f)
            for rid, r in rfqs.items():
                items = r.get("line_items", [])
                results["rfqs"].append({
                    "id": rid, "items": len(items),
                    "status": r.get("status", "?"),
                    "solicitation": r.get("solicitation_number", ""),
                })
    except Exception as e:
        results["error"] = str(e)
    return api_response(results)


@bp.route("/api/v1/quotes/reprocess")
@auth_required
def api_v1_quotes_reprocess():
    """Re-enrich all pending quotes + validate sent quotes with fresh market data."""
    import threading as _th
    from src.agents.quote_reprocessor import reprocess_all_quotes
    t = _th.Thread(target=reprocess_all_quotes, daemon=True, name="quote-reprocess")
    t.start()
    return api_response({"status": "started", "message": "Reprocessing all quotes. Check logs."})


@bp.route("/api/v1/quotes/price-alerts")
@auth_required
def api_v1_price_alerts():
    """View underpricing alerts from last validation."""
    import os as _os
    import json as _json
    path = "/data/price_alerts.json"
    if not _os.path.exists(path):
        return api_response({"alerts": [], "message": "No alerts yet. Run /api/v1/quotes/reprocess first."})
    with open(path, "r") as f:
        return api_response(_json.load(f))


@bp.route("/api/v1/outreach/preview/<path:email>")
@auth_required
def api_v1_outreach_preview(email):
    """Preview outreach email for a specific prospect."""
    from src.agents.outreach_agent import generate_outreach_email
    strategy = request.args.get("strategy", "A")
    result = generate_outreach_email(email, strategy=strategy)
    return api_response(result)


@bp.route("/api/v1/outreach/batch")
@auth_required
def api_v1_outreach_batch():
    """Generate batch outreach for top prospects with A/B variants."""
    from src.agents.outreach_agent import generate_batch_outreach
    limit = min(int(request.args.get("limit", "20")), 200)
    min_score = float(request.args.get("min_score", "30"))
    batch = generate_batch_outreach(limit=limit, min_score=min_score)
    return api_response({"emails": batch, "count": len(batch)})


@bp.route("/api/v1/catalog/enrich")
@auth_required
def api_v1_catalog_enrich():
    """Run catalog enrichment — parse identifiers from all items."""
    from src.agents.item_enricher import enrich_catalog
    count = enrich_catalog()
    return api_response({"items_enriched": count})


@bp.route("/api/v1/catalog/item-lookup")
@auth_required
def api_v1_catalog_item_lookup():
    """Look up item — parse identifiers, generate search URLs."""
    from src.agents.item_enricher import search_product_url, parse_identifiers
    from src.agents.quote_intelligence import search_catalog
    q = request.args.get("q", "")
    if not q:
        return api_response(error="No query", status=400)
    matches = search_catalog(q, limit=5)
    parsed = parse_identifiers(q)
    urls = search_product_url(q)
    stored_ids = None
    if matches:
        import sqlite3
        from src.core.db import DB_PATH
        _db = sqlite3.connect(DB_PATH, timeout=10)
        best = matches[0]["description"]
        stored = _db.execute("""
            SELECT mfg_number, mfg_name, upc, asin, nsn, sku,
                   product_url, product_url_verified, identifiers_json,
                   enriched_description, enrichment_status
            FROM scprs_catalog WHERE description = ?
        """, (best,)).fetchone()
        _db.close()
        if stored:
            import json as _json
            stored_ids = {
                "mfg_number": stored[0], "mfg_name": stored[1],
                "upc": stored[2], "asin": stored[3], "nsn": stored[4],
                "sku": stored[5], "product_url": stored[6],
                "url_verified": bool(stored[7]),
                "all_identifiers": _json.loads(stored[8]) if stored[8] else {},
                "enriched_description": stored[9], "status": stored[10],
            }
    return api_response({
        "query": q, "parsed_identifiers": parsed,
        "search_urls": urls["search_urls"],
        "catalog_matches": matches, "stored_identifiers": stored_ids,
    })


@bp.route("/api/v1/catalog/set-url", methods=["POST"])
@auth_required
def api_v1_catalog_set_url():
    """User confirms a product URL for a catalog item."""
    from src.agents.item_enricher import set_product_url
    data = request.get_json(force=True, silent=True) or {}
    description = data.get("description", "")
    url = data.get("url", "")
    if not description or not url:
        return api_response(error="Need description and url", status=400)
    set_product_url(description, url, verified=True)
    return api_response({"status": "saved", "description": description[:60], "url": url})


@bp.route("/api/v1/catalog/enrichment-stats")
@auth_required
def api_v1_catalog_enrichment_stats():
    """Show catalog enrichment statistics."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    total = db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
    by_status = db.execute("SELECT enrichment_status, COUNT(*) FROM scprs_catalog GROUP BY enrichment_status").fetchall()
    with_url = db.execute("SELECT COUNT(*) FROM scprs_catalog WHERE product_url != ''").fetchone()[0]
    verified = db.execute("SELECT COUNT(*) FROM scprs_catalog WHERE product_url_verified = 1").fetchone()[0]
    with_mfg = db.execute("SELECT COUNT(*) FROM scprs_catalog WHERE mfg_number != ''").fetchone()[0]
    with_upc = db.execute("SELECT COUNT(*) FROM scprs_catalog WHERE upc != ''").fetchone()[0]
    with_asin = db.execute("SELECT COUNT(*) FROM scprs_catalog WHERE asin != ''").fetchone()[0]
    needs_url = db.execute("""
        SELECT description, mfg_number, mfg_name, last_unit_price, times_seen
        FROM scprs_catalog WHERE product_url = '' AND enrichment_status = 'enriched'
        ORDER BY times_seen DESC LIMIT 20
    """).fetchall()
    db.close()
    return api_response({
        "total_items": total,
        "by_status": {s[0] or "unknown": s[1] for s in by_status},
        "with_product_url": with_url, "urls_verified": verified,
        "with_mfg_number": with_mfg, "with_upc": with_upc, "with_asin": with_asin,
        "needs_url_review": [{"description": r[0][:80], "mfg_number": r[1],
                              "mfg_name": r[2], "price": r[3], "times_seen": r[4]} for r in needs_url],
    })


@bp.route("/api/v1/system/audit-now")
@auth_required
def api_v1_audit_now():
    """Trigger system audit immediately."""
    import threading as _th
    from src.agents.system_auditor import run_full_audit
    t = _th.Thread(target=run_full_audit, daemon=True, name="audit-manual")
    t.start()
    return api_response({"status": "started", "message": "Audit running. Check /api/v1/system/audit-report in a few minutes."})


@bp.route("/api/v1/system/audit-report")
@auth_required
def api_v1_audit_report():
    """View the latest audit report."""
    import os as _os
    import json as _json
    json_path = "/data/system_audit.json"
    if not _os.path.exists(json_path):
        return api_response(error="No audit report yet. Hit /api/v1/system/audit-now first.", status=404)
    with open(json_path, "r") as f:
        report = _json.load(f)
    report["summary"] = {
        "enhancements": len(report.get("enhancements", [])),
        "critical": len(report.get("critical", [])),
        "duplicates": len(report.get("duplicates", [])),
        "data_issues": len(report.get("data_issues", [])),
        "ui_issues": len(report.get("ui_issues", [])),
        "missing_integrations": len(report.get("missing_integrations", [])),
        "markdown_url": "/api/v1/system/audit-report-md",
    }
    return api_response(report)


@bp.route("/api/v1/system/audit-report-md")
@auth_required
def api_v1_audit_report_md():
    """View audit report as readable markdown."""
    from flask import Response
    import os as _os
    md_path = "/data/system_audit.md"
    if not _os.path.exists(md_path):
        return api_response(error="No audit report yet", status=404)
    with open(md_path, "r") as f:
        content = f.read()
    return Response(content, mimetype="text/plain")


@bp.route("/api/v1/quote/intelligence", methods=["POST"])
@auth_required
def api_v1_quote_intelligence():
    """Match RFQ items against catalog and suggest pricing."""
    from src.agents.quote_intelligence import match_rfq_items
    data = request.get_json(force=True, silent=True) or {}
    items = data.get("items", [])
    if not items:
        return api_response(error="No items provided", status=400)
    results = match_rfq_items(items)
    total_with_pricing = sum(1 for r in results if r.get("suggested_price"))
    return api_response({
        "matches": results,
        "items_total": len(results),
        "items_with_pricing": total_with_pricing,
    })


@bp.route("/api/v1/quote/catalog-search")
@auth_required
def api_v1_catalog_search():
    """Search the catalog for items matching a query."""
    from src.agents.quote_intelligence import search_catalog, get_competitor_prices
    q = request.args.get("q", "")
    limit = min(int(request.args.get("limit", "10")), 200)
    if not q:
        return api_response(error="No query provided", status=400)
    catalog = search_catalog(q, limit=limit)
    competitors = get_competitor_prices(q, limit=20)
    return api_response({
        "query": q,
        "catalog_matches": catalog,
        "all_supplier_prices": competitors,
    })


@bp.route("/api/v1/quote/enrich", methods=["POST"])
@auth_required
def api_v1_quote_enrich():
    """Full quote enrichment — takes RFQ data, returns draft with pricing."""
    from src.agents.quote_intelligence import enrich_quote_draft
    data = request.get_json(force=True, silent=True) or {}
    result = enrich_quote_draft(data)
    return api_response(result)


@bp.route("/api/v1/harvest/backfill-details")
@auth_required
def api_v1_backfill_details():
    """Backfill detail pages for POs that have no line items. Background thread."""
    if _backfill_state["running"]:
        return api_response({"started": False, "reason": "backfill already running"})

    try:
        import sqlite3
        from src.core.db import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=10)
        # Count POs needing backfill
        need_backfill = conn.execute("""
            SELECT COUNT(*) FROM scprs_po_master pm
            WHERE NOT EXISTS (
                SELECT 1 FROM scprs_po_lines pl WHERE pl.po_id = pm.id
            )
        """).fetchone()[0]
        conn.close()

        def _run():
            _backfill_state["running"] = True
            log.info("Backfill thread STARTED")
            try:
                from src.agents.scprs_lookup import FiscalSession
                from src.core.pull_orchestrator import _get_conn
                import time as _t

                conn = _get_conn()

                # Get PO numbers that already have lines (skip these)
                have_lines = set()
                for row in conn.execute("SELECT DISTINCT po_number FROM scprs_po_lines").fetchall():
                    have_lines.add(row["po_number"])
                log.info("Backfill: %d POs already have lines in DB", len(have_lines))

                # One session, one empty search — returns all Reytech POs
                session = FiscalSession()
                if not session.init_session():
                    log.error("Backfill: session init FAILED")
                    conn.close()
                    return

                results = session.search(supplier_name="reytech", from_date="01/01/2022")
                total_rows = len(results or [])
                log.info("Backfill: search returned %d rows", total_rows)

                if not results:
                    log.error("Backfill: 0 results from search — nothing to do")
                    conn.close()
                    return

                filled = 0
                lines_inserted = 0
                skipped = 0

                for row_idx in range(total_rows):
                    r = results[row_idx]
                    # Skip POs that already have lines
                    r_po = r.get("po_number", "")
                    if r_po and r_po in have_lines:
                        skipped += 1
                        continue

                    try:
                        # Click this row to get detail page
                        if not r.get("_results_html") or r.get("_row_index") is None:
                            log.warning("Backfill row %d: no _results_html, skipping", row_idx)
                            continue

                        detail = session.get_detail(
                            r["_results_html"], r["_row_index"],
                            r.get("_click_action"))

                        line_count = len(detail.get("line_items", [])) if detail else 0

                        if detail and detail.get("line_items"):
                            # Get real PO number from detail header
                            header = detail.get("header", {})
                            real_po = header.get("po_number") or detail.get("po_number") or r_po

                            # Find matching po_master row by PO number or search_term
                            master = conn.execute(
                                "SELECT id FROM scprs_po_master WHERE po_number = ? LIMIT 1",
                                (real_po,)).fetchone()
                            if not master and r_po:
                                master = conn.execute(
                                    "SELECT id FROM scprs_po_master WHERE po_number = ? LIMIT 1",
                                    (r_po,)).fetchone()
                            if not master:
                                # Try matching by grand total + supplier
                                gt = r.get("grand_total_num")
                                sup = r.get("supplier_name", "")
                                if gt and sup:
                                    master = conn.execute(
                                        "SELECT id FROM scprs_po_master WHERE grand_total = ? AND supplier = ? LIMIT 1",
                                        (gt, sup)).fetchone()

                            if master:
                                po_id = master["id"]
                                store_po = real_po or r_po
                                for idx, item in enumerate(detail["line_items"]):
                                    if not isinstance(item, dict):
                                        continue
                                    conn.execute("""
                                        INSERT OR IGNORE INTO scprs_po_lines
                                        (po_id, po_number, line_num, description,
                                         unit_price, quantity, line_total, category)
                                        VALUES (?,?,?,?,?,?,?,?)
                                    """, (po_id, store_po, idx + 1,
                                          (item.get("description", "") or "")[:500],
                                          item.get("unit_price_num") or item.get("unit_price", 0) or 0,
                                          item.get("quantity_num") or item.get("quantity", 0) or 0,
                                          item.get("line_total", 0) or 0,
                                          "other"))
                                    lines_inserted += 1
                                # Update buyer info + acq_method + real PO number
                                buyer = header.get("buyer_name") or detail.get("buyer_name")
                                email = header.get("buyer_email") or detail.get("buyer_email")
                                acq = header.get("acq_method") or detail.get("acq_method")
                                updates = []
                                params = []
                                if buyer:
                                    updates.append("buyer_name=?"); params.append(buyer)
                                if email:
                                    updates.append("buyer_email=?"); params.append(email)
                                if acq:
                                    updates.append("acq_method=?"); params.append(acq)
                                if real_po and real_po != r_po:
                                    updates.append("po_number=?"); params.append(real_po)
                                if updates:
                                    params.append(po_id)
                                    conn.execute(
                                        f"UPDATE scprs_po_master SET {', '.join(updates)} WHERE id=?",
                                        params)
                                filled += 1
                                conn.commit()
                                log.info("Backfill row %d: PO=%s %d lines, buyer=%s",
                                         row_idx, store_po, line_count, buyer or "?")
                            else:
                                log.warning("Backfill row %d: PO=%s not found in po_master", row_idx, real_po)
                        else:
                            log.info("Backfill row %d: PO=%s 0 lines from detail", row_idx, r_po)

                        # Re-search to reset session back to results page for next click
                        results = session.search(supplier_name="reytech", from_date="01/01/2022")
                        if not results or len(results) != total_rows:
                            log.warning("Backfill: re-search returned %d rows (expected %d)",
                                        len(results or []), total_rows)
                            if not results:
                                break

                        _t.sleep(1)
                    except Exception as e:
                        log.error("Backfill row %d FAILED: %s", row_idx, e, exc_info=True)
                        # Try to recover session for next row
                        try:
                            results = session.search(supplier_name="reytech", from_date="01/01/2022")
                        except Exception:
                            log.error("Backfill: session recovery failed, stopping")
                            break

                    if (row_idx + 1) % 10 == 0:
                        log.info("Backfill: %d/%d rows, %d filled, %d lines, %d skipped",
                                 row_idx + 1, total_rows, filled, lines_inserted, skipped)

                conn.close()
                log.info("Backfill complete: %d filled, %d lines inserted, %d skipped (already had lines)",
                         filled, lines_inserted, skipped)
            except Exception as e:
                log.error("Backfill error: %s", e, exc_info=True)
            finally:
                _backfill_state["running"] = False

        log.info("Backfill: pre-check count=%d, launching thread", need_backfill)
        _threading.Thread(target=_run, daemon=True, name="backfill-details").start()
        return api_response({
            "started": True,
            "pos_to_backfill": need_backfill,
            "note": "Fetching detail pages for POs without line items"
        })
    except Exception as e:
        log.error("backfill-details error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── RFQ Metadata Backfill ─────────────────────────────────────────────────

@bp.route("/api/v1/rfq/backfill-metadata")
@auth_required
def api_v1_rfq_backfill_metadata():
    """Backfill solicitation numbers and due dates for existing RFQs.
    ?dry_run=1 — show what would be recovered without saving
    """
    try:
        dry = request.args.get("dry_run", "0") == "1"
        result = backfill_rfq_metadata(dry_run=dry)
        return api_response(result)
    except Exception as e:
        log.error("backfill-metadata error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/imap-backfill")
@auth_required
def api_v1_rfq_imap_backfill():
    """Re-fetch original emails from IMAP to recover metadata and PDFs.
    ?dry_run=1 — show what would be recovered without saving
    """
    try:
        dry = request.args.get("dry_run", "0") == "1"
        result = imap_backfill_rfq_metadata(dry_run=dry)
        return api_response(result)
    except Exception as e:
        log.error("imap-backfill error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/diagnose-files")
@auth_required
def api_v1_rfq_diagnose_files():
    """Show what PDFs are stored in rfq_files for each RFQ."""
    try:
        from src.core.db import get_db
        with get_db() as db:
            rows = db.execute("""
                SELECT rfq_id, filename, file_type, category, file_size, created_at
                FROM rfq_files ORDER BY rfq_id, created_at
            """).fetchall()
        files_by_rfq = {}
        for r in rows:
            rid = r[0]
            files_by_rfq.setdefault(rid, []).append({
                "filename": r[1], "file_type": r[2], "category": r[3],
                "size": r[4], "created_at": r[5],
            })
        # Also show RFQs with zero files
        rfqs = load_rfqs()
        diag = []
        for rid in rfqs:
            diag.append({
                "rfq_id": rid,
                "status": rfqs[rid].get("status", ""),
                "files": files_by_rfq.get(rid, []),
                "file_count": len(files_by_rfq.get(rid, [])),
            })
        total_files = sum(len(v) for v in files_by_rfq.values())
        orphan_rfq_ids = [rid for rid in files_by_rfq if rid not in rfqs]
        return api_response({
            "rfqs": diag,
            "total_files": total_files,
            "rfqs_with_files": len([d for d in diag if d["file_count"] > 0]),
            "rfqs_without_files": len([d for d in diag if d["file_count"] == 0]),
            "orphan_file_rfq_ids": orphan_rfq_ids,
        })
    except Exception as e:
        log.error("diagnose-files error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/diagnose-fields")
@auth_required
def api_v1_rfq_diagnose_fields():
    """Show what metadata fields each RFQ has — for debugging.
    ?source=json  → read raw JSON file only
    ?source=db    → read raw SQLite only
    ?source=merged (default) → load_rfqs() merged view
    ?id=xxx       → show ALL fields for a single RFQ
    """
    import json as _json
    source = request.args.get("source", "merged")
    single_id = request.args.get("id", "")
    try:
        if source == "json":
            # Raw JSON file — no DAL, no merge
            import os as _os
            try:
                from src.core.paths import DATA_DIR as _DATA_DIR
            except Exception:
                _DATA_DIR = _os.environ.get("DATA_DIR", _os.path.join(_os.path.dirname(__file__), "..", "..", "data"))
            _rfq_path = _os.path.join(_DATA_DIR, "rfqs.json")
            exists = _os.path.exists(_rfq_path)
            size = _os.path.getsize(_rfq_path) if exists else 0
            try:
                with open(_rfq_path) as f:
                    rfqs = _json.load(f)
            except Exception as _e:
                return api_response({"error": f"JSON read failed: {_e}", "path": _rfq_path,
                                     "exists": exists, "size": size, "data_dir": _DATA_DIR})
            label = f"json ({_rfq_path}, {size}b, {len(rfqs)} entries)"
        elif source == "db":
            # Raw SQLite — no JSON merge
            from src.core.dal import list_rfqs as _dal_list
            rows = _dal_list(limit=10000)
            rfqs = {r["id"]: r for r in rows} if rows else {}
            label = "sqlite"
        else:
            rfqs = load_rfqs()
            label = "merged"

        if single_id and single_id in rfqs:
            r = rfqs[single_id]
            # Show all keys
            return api_response({"source": label, "id": single_id,
                                 "keys": sorted(r.keys()),
                                 "data": {k: (str(v)[:200] if isinstance(v, (str, list, dict)) else v)
                                          for k, v in r.items()}})

        diag = []
        for rid, r in rfqs.items():
            diag.append({
                "id": rid,
                "solicitation_number": r.get("solicitation_number", ""),
                "rfq_number": r.get("rfq_number", ""),
                "due_date": r.get("due_date", ""),
                "status": r.get("status", ""),
                "email_subject": (r.get("email_subject", "") or "")[:120],
                "has_body_text": bool(r.get("body_text", "")),
                "has_parse_note": bool(r.get("parse_note", "")),
                "requestor_email": r.get("requestor_email", ""),
                "form_type": r.get("form_type", ""),
                "keys": sorted(r.keys()),
            })
        return api_response({"source": label, "rfqs": diag, "count": len(diag)})
    except Exception as e:
        log.error("diagnose-fields error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Due Date Reminders ────────────────────────────────────────────────────

@bp.route("/api/v1/reminders/check-now")
@auth_required
def api_v1_reminders_check_now():
    """Manually trigger due date reminder check."""
    try:
        from src.agents.due_date_reminder import check_due_dates
        alerts = check_due_dates()
        return api_response({"alerts": alerts, "count": len(alerts)})
    except Exception as e:
        log.error("reminders/check-now error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── System Safety: Snapshots / Restore / Data Health ──────────────────

@bp.route("/api/v1/system/snapshots")
@auth_required
def api_v1_system_snapshots():
    """List data file snapshots."""
    try:
        from src.core.data_guard import list_snapshots
        filename = request.args.get("file", "")
        return api_response({"snapshots": list_snapshots(filename)})
    except Exception as e:
        log.error("system/snapshots error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/system/restore", methods=["POST"])
@auth_required
def api_v1_system_restore():
    """Restore a data file from a snapshot."""
    try:
        from src.core.data_guard import restore_snapshot
        data = request.get_json(force=True, silent=True) or {}
        snapshot = data.get("snapshot", "")
        target = data.get("target", "")
        allowed = ["rfqs.json", "price_checks.json", "orders.json"]
        if os.path.basename(target) not in allowed:
            return api_response(error=f"Only: {allowed}", status=403)
        try:
            from src.core.paths import DATA_DIR as _DATA_DIR
        except Exception:
            _DATA_DIR = os.environ.get("DATA_DIR", "/data")
        target_path = os.path.join(_DATA_DIR, target)
        return api_response(restore_snapshot(snapshot, target_path))
    except Exception as e:
        log.error("system/restore error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/system/data-health")
@auth_required
def api_v1_system_data_health():
    """Check data file integrity — item counts and snapshot status."""
    import json as _json
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    health = {"ok": True, "checks": []}
    for fname, item_key in [("rfqs.json", "line_items"),
                             ("price_checks.json", "items")]:
        try:
            with open(os.path.join(_DATA_DIR, fname)) as f:
                data = _json.load(f)
            total = len(data)
            total_items = 0
            for v in data.values():
                if not isinstance(v, dict):
                    continue
                if item_key == "items":
                    pd = v.get("pc_data", v)
                    if isinstance(pd, str):
                        try:
                            pd = _json.loads(pd)
                        except Exception:
                            pd = {}
                    total_items += len(
                        pd.get("items", v.get("items", []))
                        if isinstance(pd, dict) else []
                    )
                else:
                    total_items += len(v.get("line_items", v.get("items", [])))
            health["checks"].append({
                "file": fname, "records": total,
                "items": total_items, "ok": total_items > 0 or total == 0,
            })
            if total > 0 and total_items == 0:
                health["ok"] = False
        except Exception as e:
            health["checks"].append({"file": fname, "error": str(e), "ok": False})
            health["ok"] = False
    try:
        from src.core.data_guard import list_snapshots
        health["snapshots"] = len(list_snapshots())
    except Exception:
        health["snapshots"] = 0
    return api_response(health)


# ── Workflow Safeguards: Validation / Checklist / Linker / Sent Tracker ──

@bp.route("/api/v1/rfq/<rid>/validate")
@auth_required
def api_v1_rfq_validate(rid):
    """Validate RFQ readiness to generate a quote."""
    try:
        from src.core.quote_validator import validate_ready_to_generate
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            return api_response(error="RFQ not found", status=404)
        return api_response(validate_ready_to_generate(r))
    except Exception as e:
        log.error("rfq validate error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/<rid>/checklist")
@auth_required
def api_v1_rfq_checklist(rid):
    """Get completion checklist for an RFQ."""
    try:
        from src.core.quote_validator import get_completion_checklist
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            return api_response(error="RFQ not found", status=404)
        return api_response(get_completion_checklist(r))
    except Exception as e:
        log.error("rfq checklist error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/<rid>/link-pc", methods=["POST"])
@auth_required
def api_v1_rfq_link_pc(rid):
    """Link an RFQ to a PC and import pricing."""
    try:
        from src.core.pc_rfq_linker import auto_link_rfq_to_pc
        from src.api.dashboard import _load_price_checks
        data = request.get_json(force=True, silent=True) or {}
        pc_id = data.get("pc_id", "")

        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            return api_response(error="RFQ not found", status=404)

        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        if not pc:
            return api_response(error="PC not found", status=404)

        count = auto_link_rfq_to_pc(r, pc_id, pc)
        save_rfqs(rfqs)

        return api_response({
            "linked": True,
            "pc_id": pc_id,
            "items_imported": count,
        })
    except Exception as e:
        log.error("rfq link-pc error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/quotes/sent-tracker")
@auth_required
def api_v1_quotes_sent_tracker():
    """Get all sent quotes with follow-up status."""
    try:
        from src.agents.post_send_pipeline import get_sent_quotes_dashboard
        quotes = get_sent_quotes_dashboard()
        overdue = [q for q in quotes if q["urgency"] == "overdue"]
        follow_up_due = [q for q in quotes if q["urgency"] == "follow_up_due"]
        return api_response({
            "quotes": quotes,
            "total": len(quotes),
            "overdue": len(overdue),
            "follow_up_due": len(follow_up_due),
        })
    except Exception as e:
        log.error("sent-tracker error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/system/recover-pcs")
@auth_required
def api_v1_system_recover_pcs():
    """Recover PCs from SQLite into price_checks.json. Merges all sources, keeps version with most items."""
    import json as _json
    try:
        from src.core.db import get_db
        from src.core.data_guard import safe_save_json
        try:
            from src.core.paths import DATA_DIR as _DATA_DIR
        except Exception:
            _DATA_DIR = os.environ.get("DATA_DIR", "/data")

        pc_json_path = os.path.join(_DATA_DIR, "price_checks.json")

        # Load current JSON
        current = {}
        try:
            with open(pc_json_path) as f:
                current = _json.load(f)
        except Exception:
            pass

        # Load from live SQLite
        all_sources = {}
        with get_db() as conn:
            rows = conn.execute("SELECT * FROM price_checks").fetchall()
            for r in rows:
                d = dict(r)
                pcid = d["id"]
                pc_data = d.get("pc_data", "{}")
                if isinstance(pc_data, str):
                    try:
                        pc_data = _json.loads(pc_data)
                    except Exception:
                        pc_data = {}
                if isinstance(pc_data, dict):
                    for k, v in d.items():
                        if k != "pc_data" and v and not pc_data.get(k):
                            pc_data[k] = v
                items = pc_data.get("items", []) if isinstance(pc_data, dict) else []
                if not items:
                    items_col = d.get("items", "[]")
                    if isinstance(items_col, str):
                        try:
                            items = _json.loads(items_col)
                        except Exception:
                            items = []
                    if items and isinstance(pc_data, dict):
                        pc_data["items"] = items
                all_sources[pcid] = {"data": pc_data, "items": len(items), "source": "sqlite"}

        # Merge with JSON (prefer version with more items)
        for pcid, pc in current.items():
            if not isinstance(pc, dict):
                continue
            pd = pc.get("pc_data", pc)
            if isinstance(pd, str):
                try:
                    pd = _json.loads(pd)
                except Exception:
                    pd = {}
            items = pd.get("items", pc.get("items", [])) if isinstance(pd, dict) else []
            item_count = len(items) if isinstance(items, list) else 0
            if pcid not in all_sources or item_count > all_sources[pcid]["items"]:
                all_sources[pcid] = {"data": pc, "items": item_count, "source": "json"}

        # Build restored dict
        restored = {}
        details = []
        for pcid, info in all_sources.items():
            restored[pcid] = info["data"]
            details.append({"id": pcid, "items": info["items"], "source": info["source"]})

        # Save with data guard
        safe_save_json(pc_json_path, restored, reason="manual_recovery")

        # Also update SQLite
        with get_db() as conn:
            for pcid, pc in restored.items():
                if not isinstance(pc, dict):
                    continue
                items = pc.get("items", [])
                if isinstance(items, str):
                    try:
                        items = _json.loads(items)
                    except Exception:
                        items = []
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO price_checks
                        (id, created_at, requestor, agency, institution, items,
                         pc_number, status, pc_data)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (pcid, pc.get("created_at", ""), pc.get("requestor", ""),
                          pc.get("agency", ""), pc.get("institution", ""),
                          _json.dumps(items, default=str),
                          pc.get("pc_number", ""), pc.get("status", "parsed"),
                          _json.dumps(pc, default=str)))
                except Exception:
                    pass

        total_items = sum(info["items"] for info in all_sources.values())
        return api_response({
            "recovered": len(restored),
            "total_items": total_items,
            "details": details,
        })
    except Exception as e:
        log.error("recover-pcs error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/system/emergency-cleanup")
@auth_required
def api_v1_system_emergency_cleanup():
    """Delete bloated snapshot files and restore from smallest good snapshot."""
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    import json as _json

    snap_dir = os.path.join(_DATA_DIR, "snapshots")
    if not os.path.exists(snap_dir):
        return api_response({"error": "No snapshots dir"})

    # Find and delete bloated files (>1MB), keep small ones
    deleted = []
    kept = []
    freed = 0
    for f in os.listdir(snap_dir):
        fpath = os.path.join(snap_dir, f)
        size = os.path.getsize(fpath)
        if size > 1_000_000:  # >1MB is bloated
            os.remove(fpath)
            deleted.append({"file": f, "size": size})
            freed += size
        else:
            kept.append({"file": f, "size": size})

    # Now restore price_checks.json from the smallest valid snapshot
    best_snap = None
    best_size = 0
    for snap in kept:
        if "price_checks" in snap["file"] and snap["size"] > 100:
            fpath = os.path.join(snap_dir, snap["file"])
            try:
                with open(fpath) as sf:
                    data = _json.load(sf)
                if isinstance(data, dict) and len(data) > 0:
                    if best_snap is None or snap["size"] > best_size:
                        best_snap = snap["file"]
                        best_size = snap["size"]
            except Exception:
                pass

    restored_from = None
    if best_snap:
        src_path = os.path.join(snap_dir, best_snap)
        dst_path = os.path.join(_DATA_DIR, "price_checks.json")
        import shutil
        shutil.copy2(src_path, dst_path)
        restored_from = best_snap

    return api_response({
        "deleted": len(deleted),
        "freed_mb": round(freed / 1_000_000, 1),
        "kept": kept,
        "restored_from": restored_from,
    })


@bp.route("/api/v1/system/boot-health")
@auth_required
def api_v1_system_boot_health():
    """Run full health check — same as boot but on demand."""
    try:
        from src.core.data_guard import boot_health_check
        return api_response(boot_health_check())
    except Exception as e:
        log.error("boot-health error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/system/disk-usage")
@auth_required
def api_v1_system_disk_usage():
    """Show disk usage breakdown and clean up old files."""
    import subprocess
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")

    clean = request.args.get("clean", "0") == "1"
    result = {"data_dir": _DATA_DIR, "usage": [], "cleaned": []}

    # Disk usage per directory
    try:
        for entry in sorted(os.listdir(_DATA_DIR)):
            path = os.path.join(_DATA_DIR, entry)
            if os.path.isfile(path):
                result["usage"].append({"name": entry, "size_mb": round(os.path.getsize(path) / 1_000_000, 1)})
            elif os.path.isdir(path):
                total = 0
                count = 0
                for root, dirs, files in os.walk(path):
                    for f in files:
                        fp = os.path.join(root, f)
                        try:
                            total += os.path.getsize(fp)
                            count += 1
                        except OSError:
                            pass
                result["usage"].append({"name": entry + "/", "size_mb": round(total / 1_000_000, 1), "files": count})
        result["usage"].sort(key=lambda x: x["size_mb"], reverse=True)
        result["total_mb"] = round(sum(u["size_mb"] for u in result["usage"]), 1)
    except Exception as e:
        result["error"] = str(e)

    # Cleanup if requested
    if clean:
        import time as _time
        now = _time.time()
        cutoff_30d = now - (30 * 86400)
        cutoff_7d = now - (7 * 86400)

        # 1. PO records: delete ALL .html files (PNGs are enough), PNGs >30 days
        po_dir = os.path.join(_DATA_DIR, "po_records")
        if os.path.isdir(po_dir):
            for f in os.listdir(po_dir):
                fp = os.path.join(po_dir, f)
                try:
                    size = os.path.getsize(fp)
                    if f.endswith(".html"):
                        os.remove(fp)
                        result["cleaned"].append({"file": f"po_records/{f}", "size_mb": round(size / 1_000_000, 2)})
                    elif f.endswith(".png") and os.path.getmtime(fp) < cutoff_30d:
                        os.remove(fp)
                        result["cleaned"].append({"file": f"po_records/{f}", "size_mb": round(size / 1_000_000, 2)})
                except OSError:
                    pass

        # 2. Snapshots >7 days
        snap_dir = os.path.join(_DATA_DIR, "snapshots")
        if os.path.isdir(snap_dir):
            for f in os.listdir(snap_dir):
                fp = os.path.join(snap_dir, f)
                try:
                    if os.path.getmtime(fp) < cutoff_7d:
                        size = os.path.getsize(fp)
                        os.remove(fp)
                        result["cleaned"].append({"file": f"snapshots/{f}", "size_mb": round(size / 1_000_000, 2)})
                except OSError:
                    pass

        # 3. Uploads >30 days
        upload_dir = os.path.join(_DATA_DIR, "uploads")
        if os.path.isdir(upload_dir):
            for root, dirs, files in os.walk(upload_dir):
                for f in files:
                    fp = os.path.join(root, f)
                    try:
                        if os.path.getmtime(fp) < cutoff_30d:
                            size = os.path.getsize(fp)
                            os.remove(fp)
                            relpath = os.path.relpath(fp, _DATA_DIR)
                            result["cleaned"].append({"file": relpath, "size_mb": round(size / 1_000_000, 2)})
                    except OSError:
                        pass

        # 4. Delete corrupt DB backup
        corrupt = os.path.join(_DATA_DIR, "reytech.db.corrupt.20260314_205443")
        if os.path.exists(corrupt):
            size = os.path.getsize(corrupt)
            os.remove(corrupt)
            result["cleaned"].append({"file": "reytech.db.corrupt.20260314_205443", "size_mb": round(size / 1_000_000, 1)})

        # 5. Rebuild price_checks.json if bloated (>5MB means recursive nesting)
        pc_path = os.path.join(_DATA_DIR, "price_checks.json")
        if os.path.exists(pc_path) and os.path.getsize(pc_path) > 5_000_000:
            try:
                import json as _jclean
                from src.core.db import get_db
                with get_db() as conn:
                    rows = conn.execute("SELECT id, pc_data FROM price_checks WHERE pc_data IS NOT NULL").fetchall()
                rebuilt = {}
                for r in rows:
                    try:
                        pd = _jclean.loads(r[1]) if isinstance(r[1], str) else r[1]
                        if isinstance(pd, dict):
                            rebuilt[r[0]] = {k: v for k, v in pd.items() if k != "pc_data"}
                    except Exception:
                        pass
                if rebuilt:
                    old_size = os.path.getsize(pc_path)
                    from src.core.data_guard import safe_save_json
                    safe_save_json(pc_path, rebuilt, reason="disk_cleanup_rebuild")
                    new_size = os.path.getsize(pc_path)
                    result["cleaned"].append({
                        "file": "price_checks.json (rebuilt from SQLite)",
                        "size_mb": round((old_size - new_size) / 1_000_000, 1)
                    })
            except Exception as _e:
                result["cleaned"].append({"file": "price_checks.json rebuild FAILED", "error": str(_e)})

        # 6. Delete stale scprs screenshots in /data root
        for f in os.listdir(_DATA_DIR):
            if f.startswith("scprs_") and f.endswith(".png"):
                fp = os.path.join(_DATA_DIR, f)
                try:
                    size = os.path.getsize(fp)
                    os.remove(fp)
                    result["cleaned"].append({"file": f, "size_mb": round(size / 1_000_000, 2)})
                except OSError:
                    pass

        result["freed_mb"] = round(sum(c.get("size_mb", 0) for c in result["cleaned"]), 1)

    return api_response(result)


@bp.route("/api/v1/locations/search")
@auth_required
def api_v1_locations_search():
    """Search state facility locations for autocomplete. ?q=keyword"""
    q = (request.args.get("q", "") or "").strip().lower()
    if len(q) < 2:
        return api_response({"results": []})
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT display_name, agency, address, city, state, zip
                FROM customers
                WHERE display_name != ''
                AND (LOWER(display_name) LIKE ? OR LOWER(agency) LIKE ? OR LOWER(city) LIKE ?)
                ORDER BY display_name
                LIMIT 15
            """, (f"%{q}%", f"%{q}%", f"%{q}%")).fetchall()
        results = []
        for r in rows:
            addr_parts = [p for p in [r[2], r[3], r[4], r[5]] if p]
            results.append({
                "name": r[0],
                "agency": r[1] or "",
                "address": ", ".join(addr_parts),
                "display": f"{r[0]} ({r[1]})" if r[1] else r[0],
            })
        return api_response({"results": results})
    except Exception as e:
        log.error("locations/search error: %s", e, exc_info=True)
        return api_response({"results": []})


@bp.route("/api/v1/rfq/backfill-all-fields")
@auth_required
def api_v1_rfq_backfill_all_fields():
    """Backfill empty fields on all RFQs from buyer SCPRS history + email text."""
    import json as _json
    try:
        from src.core.db import get_db
        rfqs = load_rfqs()
        updated = 0
        details = []

        for rid, r in rfqs.items():
            changed = False
            entry = {"id": rid, "filled": []}
            email = (r.get("requestor_email") or "").strip().lower()

            # Source 1: Buyer SCPRS PO history for delivery/institution
            if email and (not r.get("delivery_location") or not r.get("institution")):
                try:
                    with get_db() as conn:
                        # Most common ship-to for this buyer
                        hist = conn.execute("""
                            SELECT ship_to_address, dept_name, COUNT(*) as cnt
                            FROM scprs_po_master
                            WHERE buyer_email = ?
                            AND ship_to_address != ''
                            GROUP BY ship_to_address
                            ORDER BY cnt DESC LIMIT 1
                        """, (email,)).fetchone()
                        if hist:
                            if not r.get("delivery_location") and hist[0]:
                                r["delivery_location"] = hist[0]
                                entry["filled"].append(f"delivery_location={hist[0][:40]}")
                                changed = True
                            if not r.get("institution") and hist[1]:
                                r["institution"] = hist[1]
                                entry["filled"].append(f"institution={hist[1][:40]}")
                                changed = True
                except Exception:
                    pass

            # Source 2: Buyer SCPRS history for requestor name
            if email and not r.get("requestor_name"):
                try:
                    with get_db() as conn:
                        buyer = conn.execute("""
                            SELECT buyer_name FROM scprs_po_master
                            WHERE buyer_email = ? AND buyer_name != ''
                            LIMIT 1
                        """, (email,)).fetchone()
                        if buyer and buyer[0]:
                            r["requestor_name"] = buyer[0]
                            entry["filled"].append(f"requestor_name={buyer[0]}")
                            changed = True
                except Exception:
                    pass

            # Source 3: Email text for due date
            if not r.get("due_date") or r.get("due_date") == "TBD":
                combined = f"{r.get('email_subject', '')} {r.get('body_text', '')}"
                if combined.strip():
                    from src.api.dashboard import _extract_due_date
                    due = _extract_due_date(combined)
                    if due:
                        r["due_date"] = due
                        entry["filled"].append(f"due_date={due}")
                        changed = True

            # Source 4: Email text for solicitation
            if not r.get("solicitation_number") or r.get("solicitation_number") == "unknown":
                combined = f"{r.get('email_subject', '')} {r.get('body_text', '')}"
                if combined.strip():
                    from src.api.dashboard import _extract_solicitation
                    sol = _extract_solicitation(combined)
                    if sol:
                        r["solicitation_number"] = sol
                        entry["filled"].append(f"solicitation_number={sol}")
                        changed = True

            if changed:
                updated += 1
                details.append(entry)

        if updated:
            save_rfqs(rfqs)

        return api_response({
            "updated": updated,
            "total_rfqs": len(rfqs),
            "details": details,
        })
    except Exception as e:
        log.error("backfill-all-fields error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/system/parse-gaps")
@auth_required
def api_v1_parse_gaps():
    """Report fields most often filled manually — parser improvement targets."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            gaps = conn.execute("""
                SELECT field_name, COUNT(*) as count,
                       GROUP_CONCAT(DISTINCT agency) as agencies
                FROM parse_gaps
                GROUP BY field_name
                ORDER BY count DESC
            """).fetchall()
        return api_response({
            "gaps": [{"field": r[0], "count": r[1], "agencies": r[2]}
                    for r in gaps],
            "total": sum(r[1] for r in gaps),
            "recommendation": "Fields most often filled manually should be prioritized for parser improvement"
        })
    except Exception as e:
        log.error("parse-gaps error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Intelligence Cleanup ──────────────────────────────────────────────

@bp.route("/api/v1/rfq/clean-intelligence")
@auth_required
def api_v1_clean_intelligence():
    """Strip garbage catalog matches from all RFQs."""
    _NON_PRODUCT = ["amendment", "name change", "legal name",
                    "has changed their", "fiscal year", "fy "]
    rfqs = load_rfqs()
    cleaned = 0
    for rid, r in rfqs.items():
        for item in r.get("line_items", r.get("items", [])):
            intel = item.get("intelligence", {})
            if not isinstance(intel, dict):
                continue
            matches = intel.get("catalog_matches", [])
            before = len(matches)
            matches = [m for m in matches if m.get("relevance_score", m.get("match_confidence", 0)) >= 0.4]
            matches = [m for m in matches if not any(
                s in (m.get("description", "") or "").lower() for s in _NON_PRODUCT)]
            matches = [m for m in matches if
                       (m.get("normalized_unit_price", 0) or 0) < 50000]
            intel["catalog_matches"] = matches[:3]
            if len(matches) < before:
                cleaned += 1
    if cleaned:
        save_rfqs(rfqs)
    return api_response({"cleaned_items": cleaned})


# ── Agency Intelligence ───────────────────────────────────────────────

@bp.route("/api/v1/agency/buyer-form-profile")
@auth_required
def api_v1_agency_buyer_form_profile():
    """Get learned form preferences for a buyer email."""
    email = request.args.get("email", "")
    if not email:
        return api_response(error="Pass ?email=buyer@agency.gov", status=400)
    try:
        from src.core.agency_config import get_buyer_form_preferences, match_agency
        prefs = get_buyer_form_preferences(email)
        # Also show what the pattern matcher would pick
        dummy = {"requestor_email": email}
        matched_key, matched_cfg = match_agency(dummy)
        return api_response({
            "email": email,
            "learned": prefs,
            "pattern_match": {"agency": matched_key, "name": matched_cfg.get("name", ""),
                            "required": matched_cfg.get("required_forms", [])},
        })
    except Exception as e:
        log.error("buyer-profile error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Master Template Management ────────────────────────────────────────

@bp.route("/api/v1/system/templates")
@auth_required
def api_v1_system_templates():
    """List master form templates on the volume."""
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    tmpl_dir = os.path.join(_DATA_DIR, "templates")
    if not os.path.exists(tmpl_dir):
        return api_response({"templates": [], "path": tmpl_dir})
    templates = []
    for f in sorted(os.listdir(tmpl_dir)):
        fp = os.path.join(tmpl_dir, f)
        if os.path.isfile(fp):
            templates.append({
                "filename": f,
                "size_kb": round(os.path.getsize(fp) / 1000, 1),
                "modified": os.path.getmtime(fp),
            })
    return api_response({"templates": templates, "path": tmpl_dir})


@bp.route("/api/v1/system/templates/upload", methods=["POST"])
@auth_required
def api_v1_system_templates_upload():
    """Upload form templates. Supports:
    - Single PDF: saved as-is (or with save_as name)
    - Multi-file: multiple PDFs uploaded at once
    - Bid package split: ?split=1 auto-detects and splits combined PDF into individual forms
    """
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    tmpl_dir = os.path.join(_DATA_DIR, "templates")
    os.makedirs(tmpl_dir, exist_ok=True)

    split_mode = request.args.get("split", "0") == "1" or request.form.get("split", "0") == "1"
    results = []

    # Accept multiple files
    files = request.files.getlist("template") or request.files.getlist("templates")
    if not files or (len(files) == 1 and not files[0].filename):
        # Try single file field
        f = request.files.get("template")
        if f and f.filename:
            files = [f]
        else:
            return api_response(error="No files uploaded", status=400)

    for f in files:
        filename = (f.filename or "").strip()
        if not filename.lower().endswith(".pdf"):
            results.append({"file": filename, "error": "not a PDF"})
            continue

        # Save to temp first
        import tempfile as _tf
        tmp = _tf.NamedTemporaryFile(suffix=".pdf", delete=False)
        f.save(tmp.name)
        tmp.close()

        if split_mode:
            # Auto-detect and split forms from combined PDF
            split_results = _split_bid_package(tmp.name, tmpl_dir)
            results.extend(split_results)
            os.remove(tmp.name)
        else:
            # Save as single template
            save_as = request.form.get("save_as", "").strip() or filename
            if not save_as.lower().endswith(".pdf"):
                save_as += ".pdf"
            import shutil
            dest = os.path.join(tmpl_dir, save_as)
            shutil.move(tmp.name, dest)
            results.append({"file": save_as, "size_kb": round(os.path.getsize(dest) / 1000, 1)})
            log.info("Template uploaded: %s", save_as)

    return api_response({"uploaded": results, "count": len(results)})


@bp.route("/api/v1/system/templates/<path:filename>", methods=["DELETE"])
@auth_required
def api_v1_system_templates_delete(filename):
    """Delete a master form template."""
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    tmpl_dir = os.path.join(_DATA_DIR, "templates")
    safe_name = os.path.basename(filename)
    fp = os.path.join(tmpl_dir, safe_name)
    if not os.path.isfile(fp):
        return api_response(error=f"Template not found: {safe_name}", status=404)
    os.remove(fp)
    log.info("Template deleted: %s", safe_name)
    return api_response({"deleted": safe_name})


@bp.route("/api/v1/system/templates/<path:filename>/rename", methods=["POST"])
@auth_required
def api_v1_system_templates_rename(filename):
    """Rename a master form template."""
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    tmpl_dir = os.path.join(_DATA_DIR, "templates")
    import shutil
    old_name = os.path.basename(filename)
    body = request.get_json(silent=True) or {}
    new_name = os.path.basename(body.get("new_name", "").strip())
    if not new_name:
        return api_response(error="new_name is required", status=400)
    if not new_name.lower().endswith(".pdf"):
        new_name += ".pdf"
    src = os.path.join(tmpl_dir, old_name)
    dst = os.path.join(tmpl_dir, new_name)
    if not os.path.isfile(src):
        return api_response(error=f"Template not found: {old_name}", status=404)
    if os.path.exists(dst):
        return api_response(error=f"A template named '{new_name}' already exists", status=409)
    shutil.move(src, dst)
    log.info("Template renamed: %s -> %s", old_name, new_name)
    return api_response({"renamed": {"from": old_name, "to": new_name}})


@bp.route("/api/v1/system/templates/<path:filename>/download")
@auth_required
def api_v1_system_templates_download(filename):
    """Download a master form template."""
    try:
        from src.core.paths import DATA_DIR as _DATA_DIR
    except Exception:
        _DATA_DIR = os.environ.get("DATA_DIR", "/data")
    tmpl_dir = os.path.join(_DATA_DIR, "templates")
    safe_name = os.path.basename(filename)
    from flask import send_from_directory
    return send_from_directory(tmpl_dir, safe_name, as_attachment=True)


# ── Source Material Preview ───────────────────────────────────────────

@bp.route("/api/v1/rfq/<rid>/source-material")
@auth_required
def api_v1_rfq_source_material(rid):
    """Return original email + attachments for an RFQ."""
    return _get_source_material("rfq", rid)

@bp.route("/api/v1/pc/<pcid>/source-material")
@auth_required
def api_v1_pc_source_material(pcid):
    """Return original email + attachments for a PC."""
    return _get_source_material("pc", pcid)

def _get_source_material(entity_type, entity_id):
    import json as _json
    result = {"email_subject": "", "email_from": "", "email_date": "",
              "email_body": "", "attachments": [], "extracted_fields": {},
              "linked_pc": None, "linked_rfq": None}
    try:
        if entity_type == "rfq":
            rfqs = load_rfqs()
            r = rfqs.get(entity_id)
            if not r:
                return api_response(error="Not found", status=404)
            result["email_subject"] = r.get("email_subject", "")
            result["email_from"] = r.get("requestor_email", r.get("email_sender", ""))
            result["email_date"] = r.get("created_at", r.get("received_at", ""))
            result["email_body"] = (r.get("body_text", "") or "")[:3000]
            result["extracted_fields"] = {
                "Solicitation #": r.get("solicitation_number", ""),
                "Buyer Name": r.get("requestor_name", ""),
                "Buyer Email": r.get("requestor_email", ""),
                "Due Date": r.get("due_date", ""),
                "Delivery": r.get("delivery_location", r.get("ship_to", "")),
                "Agency": r.get("agency_name", r.get("agency", "")),
                "Form Type": r.get("form_type", ""),
                "Items": len(r.get("line_items", r.get("items", []))),
            }
            if r.get("linked_pc_id"):
                result["linked_pc"] = {"id": r["linked_pc_id"],
                                       "number": r.get("linked_pc_number", "")}
        elif entity_type == "pc":
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            pc = pcs.get(entity_id)
            if not pc:
                return api_response(error="Not found", status=404)
            result["email_subject"] = pc.get("email_subject", "")
            result["email_from"] = pc.get("requestor_email", pc.get("requestor", ""))
            result["email_date"] = pc.get("created_at", "")
            result["email_body"] = (pc.get("body_text", "") or "")[:3000]
            pd = pc.get("pc_data", pc)
            if isinstance(pd, str):
                try: pd = _json.loads(pd)
                except (ValueError, TypeError): pd = pc
            result["extracted_fields"] = {
                "PC Number": pc.get("pc_number", ""),
                "Requestor": pc.get("requestor", ""),
                "Institution": pc.get("institution", ""),
                "Due Date": pc.get("due_date", ""),
                "Ship To": pc.get("ship_to", ""),
                "Items": len(pc.get("items", [])),
            }
            if pc.get("linked_rfq_id"):
                result["linked_rfq"] = {"id": pc["linked_rfq_id"],
                                        "number": pc.get("linked_rfq_number", "")}
        # Attachments from rfq_files table
        try:
            from src.api.dashboard import list_rfq_files
            files = list_rfq_files(entity_id)
            for f in files:
                result["attachments"].append({
                    "filename": f.get("filename", ""),
                    "category": f.get("category", ""),
                    "size_kb": round(f.get("file_size", 0) / 1024, 1) if f.get("file_size") else 0,
                    "download_url": f"/rfq/{entity_id}/file/{f['id']}" if f.get("id") else "",
                })
        except Exception:
            pass
        return api_response(result)
    except Exception as e:
        log.error("source-material error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)

@bp.route("/api/v1/rfq/<rid>/mark-reviewed", methods=["POST"])
@auth_required
def api_v1_rfq_mark_reviewed(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return api_response(error="Not found", status=404)
    from datetime import datetime
    r["reviewed_at"] = datetime.now().isoformat()
    r["needs_review"] = False
    save_rfqs(rfqs)
    return api_response({"ok": True})

@bp.route("/api/v1/pc/<pcid>/mark-reviewed", methods=["POST"])
@auth_required
def api_v1_pc_mark_reviewed(pcid):
    from src.api.dashboard import _load_price_checks, _save_price_checks
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return api_response(error="Not found", status=404)
    from datetime import datetime
    pc["reviewed"] = True
    pc["reviewed_at"] = datetime.now().isoformat()
    _save_price_checks(pcs)
    return api_response({"ok": True})


# ── Quote Integrity ──────────────────────────────────────────────────

@bp.route("/api/v1/quotes/cleanup-ghosts", methods=["GET", "POST"])
@auth_required
def api_v1_quotes_cleanup_ghosts():
    """Mark empty quote shells as VOID. Numbers preserved in ledger."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            ghosts = conn.execute("""
                SELECT quote_number, total, items_count, pdf_path, status
                FROM quotes
                WHERE (items_count IS NULL OR items_count = 0)
                AND (pdf_path IS NULL OR pdf_path = '')
                AND status NOT IN ('void', 'cancelled')
            """).fetchall()
            voided = []
            for g in ghosts:
                qn = g[0]
                total = g[1] or 0
                if total == 0:
                    conn.execute("""
                        UPDATE quotes SET status='void',
                        notes = COALESCE(notes,'') || ' [VOIDED: empty shell]'
                        WHERE quote_number = ?
                    """, (qn,))
                    try:
                        conn.execute("""
                            INSERT OR REPLACE INTO quote_number_ledger
                            (quote_number, assigned_at, status, voided_at, void_reason)
                            VALUES (?, COALESCE(
                                (SELECT assigned_at FROM quote_number_ledger WHERE quote_number=?),
                                datetime('now')),
                                'void', datetime('now'), 'empty shell')
                        """, (qn, qn))
                    except Exception:
                        pass
                    voided.append(qn)
        return api_response({"voided": voided, "count": len(voided)})
    except Exception as e:
        log.error("cleanup-ghosts: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/quotes/backfill-items", methods=["GET", "POST"])
@auth_required
def api_v1_quotes_backfill_items():
    """Fill items_detail on quotes from their source PC/RFQ."""
    import json as _json
    try:
        from src.core.db import get_db
        from src.api.dashboard import _load_price_checks, _get_pc_items
        pcs = _load_price_checks()
        rfqs = load_rfqs()
        fixed = 0
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, source_pc_id, source_rfq_id, requestor, institution
                FROM quotes
                WHERE (items_count IS NULL OR items_count = 0)
                AND status NOT IN ('void', 'cancelled')
            """).fetchall()
            for r in rows:
                qn, pc_id, rfq_id = r[0], r[1], r[2]
                items = []
                if pc_id and pc_id in pcs:
                    items = _get_pc_items(pcs[pc_id])
                if not items and rfq_id and rfq_id in rfqs:
                    items = rfqs[rfq_id].get("line_items", rfqs[rfq_id].get("items", []))
                if items:
                    details = [{
                        "description": it.get("description", it.get("desc", "")),
                        "qty": it.get("qty", it.get("quantity", 1)),
                        "unit_price": it.get("bid_price", it.get("price_per_unit", 0)),
                        "vendor_cost": it.get("supplier_cost", it.get("cost", 0)),
                    } for it in items]
                    conn.execute("""
                        UPDATE quotes SET items_detail=?, items_count=?,
                        items_text=? WHERE quote_number=?
                    """, (_json.dumps(details, default=str), len(details),
                          "; ".join(d["description"][:50] for d in details[:10]), qn))
                    fixed += 1
        return api_response({"fixed": fixed, "checked": len(rows)})
    except Exception as e:
        log.error("backfill-items: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Email Recovery ────────────────────────────────────────────────────

@bp.route("/api/v1/email/missed")
@auth_required
def api_v1_email_missed():
    """Find buyer/forward emails processed but never created a record."""
    try:
        from src.api.dashboard import POLL_STATUS
        poller = POLL_STATUS.get("_poller_instance")
        if not poller or not hasattr(poller, "audit_missed_emails"):
            return api_response(error="Poller not available — try after next poll cycle")
        days = int(request.args.get("days", 7))
        missed = poller.audit_missed_emails(days=days)
        return api_response({"missed": missed, "count": len(missed)})
    except Exception as e:
        log.error("email/missed: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/email/reprocess/<uid>", methods=["GET", "POST"])
@auth_required
def api_v1_email_reprocess(uid):
    """Remove a UID from ALL processed lists (sales@ + mike@) — will reprocess next cycle."""
    cleared = []
    try:
        from src.api.dashboard import POLL_STATUS
        # Clear from sales@ poller
        poller = POLL_STATUS.get("_poller_instance")
        if poller and hasattr(poller, "reprocess_uid"):
            poller.reprocess_uid(uid)
            cleared.append("sales@")
        # Clear from mike@ processed file
        try:
            _mike_path = os.path.join(os.environ.get("DATA_DIR", "/data"), "processed_emails_mike.json")
            if os.path.exists(_mike_path):
                import json as _jm
                with open(_mike_path) as _f:
                    _mike_uids = _jm.load(_f)
                if uid in _mike_uids:
                    _mike_uids.remove(uid)
                    with open(_mike_path, "w") as _f:
                        _jm.dump(_mike_uids, _f)
                    cleared.append("mike@")
        except Exception:
            pass
        # Clear from SQLite (both inboxes)
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM processed_emails WHERE uid=?", (uid,))
            cleared.append("sqlite")
        except Exception:
            pass
        return api_response({"ok": True, "uid": uid, "cleared_from": cleared})
    except Exception as e:
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/email/reprocess-all-missed", methods=["GET", "POST"])
@auth_required
def api_v1_email_reprocess_all():
    """Find and reprocess ALL missed buyer/forward emails."""
    try:
        from src.api.dashboard import POLL_STATUS
        poller = POLL_STATUS.get("_poller_instance")
        if not poller or not hasattr(poller, "audit_missed_emails"):
            return api_response(error="Poller not available")
        missed = poller.audit_missed_emails(days=int(request.args.get("days", 7)))
        recovered = []
        for m in missed:
            uid = m.get("uid", "")
            if uid:
                poller.reprocess_uid(uid)
                recovered.append({"uid": uid, "sender": m.get("sender_email", ""), "subject": m.get("subject", "")})
        return api_response({
            "recovered": len(recovered), "details": recovered,
            "message": f"Removed {len(recovered)} UIDs. They will be picked up next poll cycle."
        })
    except Exception as e:
        log.error("reprocess-all: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/email/recover-stuck", methods=["GET", "POST"])
@auth_required
def api_v1_recover_stuck():
    """Find ALL emails processed but never created a record. Covers:
    - Forwards from our domain (CalVet via mike@)
    - Direct buyer emails before PC detection existed (Katrina)
    - Emails misclassified as CS inquiries
    - Any .ca.gov email swallowed silently
    Removes UIDs from _processed so current detection logic can reclassify.
    """
    try:
        from src.api.dashboard import POLL_STATUS, _load_price_checks
        import imaplib
        import email as _email_mod
        from datetime import datetime as _dt, timedelta as _td

        poller = POLL_STATUS.get("_poller_instance")
        if not poller:
            return api_response(error="Poller not running — wait for first poll cycle")

        addr = os.environ.get("GMAIL_ADDRESS", "")
        pwd = os.environ.get("GMAIL_PASSWORD", "")
        if not addr or not pwd:
            return api_response(error="Email credentials not configured")

        # Get UIDs that already created records — DON'T touch these
        created_uids = set()
        try:
            pcs = _load_price_checks()
            rfqs = load_rfqs()
            for pc in pcs.values():
                u = pc.get("email_uid", "")
                if u:
                    created_uids.add(str(u))
            for r in rfqs.values():
                u = r.get("email_uid", "")
                if u:
                    created_uids.add(str(u))
        except Exception:
            pass

        days = int(request.args.get("days", 30))
        buyer_domains = [".ca.gov", "cdcr", "calvet", "cdph", "cchcs", "dsh",
                        "calfire", "caltrans", "chp", "dgs", "edd", "dca"]
        our_domains = ["reytechinc.com", "reytech.com"]
        noise_senders = ["no-reply@", "noreply@", "mailer-daemon", "postmaster",
                        "notifications@", "alerts@", "support@"]

        recovered = []
        skipped = []

        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(addr, pwd)
        imap.select("INBOX", readonly=True)

        since = (_dt.now() - _td(days=days)).strftime("%d-%b-%Y")
        _, data = imap.uid("search", None, f"(SINCE {since})")
        all_uids = data[0].split() if data[0] else []

        for uid_bytes in all_uids:
            uid_str = uid_bytes.decode()

            # Only look at UIDs that ARE in processed (stuck ones)
            if uid_str not in poller._processed:
                continue
            # Skip if it already created a record
            if uid_str in created_uids:
                continue

            try:
                _, msg_data = imap.uid("fetch", uid_bytes, "(BODY.PEEK[HEADER])")
                if not msg_data or not msg_data[0]:
                    continue
                header = _email_mod.message_from_bytes(msg_data[0][1])
                from_hdr = header.get("From", "")
                subj = header.get("Subject", "") or ""
                sender = ""
                _em = __import__("re").search(r'[\w.+-]+@[\w.-]+', from_hdr)
                if _em:
                    sender = _em.group(0).lower()

                # Skip noise
                if any(n in sender for n in noise_senders):
                    continue

                # Is this a buyer email or our forward?
                is_buyer = any(d in sender for d in buyer_domains)
                is_self = any(sender.endswith(f"@{d}") for d in our_domains)
                is_forward = any(subj.lower().strip().startswith(p) for p in ["fwd:", "fw:"])

                # Recover if: buyer email OR our forward with fwd: subject
                should_recover = False
                reason = ""
                if is_buyer:
                    should_recover = True
                    reason = "buyer_email"
                elif is_self and is_forward:
                    should_recover = True
                    reason = "self_forward"

                if should_recover:
                    poller.reprocess_uid(uid_str)
                    # Also clear from mike@ processed file
                    try:
                        import json as _jrm
                        _mike_path = os.path.join(
                            os.environ.get("DATA_DIR", "/data"), "processed_emails_mike.json")
                        if os.path.exists(_mike_path):
                            with open(_mike_path) as _f:
                                _muids = _jrm.load(_f)
                            if uid_str in _muids:
                                _muids.remove(uid_str)
                                with open(_mike_path, "w") as _f:
                                    _jrm.dump(_muids, _f)
                    except Exception:
                        pass

                    recovered.append({
                        "uid": uid_str,
                        "sender": sender,
                        "subject": subj[:100],
                        "reason": reason,
                    })
                    log.info("RECOVERED stuck: uid=%s sender=%s reason=%s subj=%s",
                            uid_str, sender, reason, subj[:60])
                else:
                    skipped.append({"uid": uid_str, "sender": sender, "subject": subj[:60]})
            except Exception:
                continue

        imap.close()
        imap.logout()
        return api_response({
            "recovered": len(recovered),
            "skipped": len(skipped),
            "details": recovered,
            "skipped_details": skipped[:20],
            "next_step": "Hit Check Now or wait for next poll cycle"
        })
    except Exception as e:
        log.error("recover-stuck: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# Keep old URL as alias
@bp.route("/api/v1/email/recover-forwards", methods=["GET", "POST"])
@auth_required
def api_v1_recover_forwards():
    """Alias for recover-stuck (broader recovery)."""
    return api_v1_recover_stuck()


@bp.route("/api/v1/email/diagnose/<uid>")
@auth_required
def api_v1_email_diagnose(uid):
    """Trace exactly what the poller would do with a specific email UID."""
    try:
        import imaplib
        import email as _em
        addr = os.environ.get("GMAIL_ADDRESS", "")
        pwd = os.environ.get("GMAIL_PASSWORD", "")
        if not addr or not pwd:
            return api_response(error="No email credentials")
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(addr, pwd)
        # Try sales@ first
        imap.select("INBOX")
        _, data = imap.uid("fetch", uid.encode(), "(BODY.PEEK[])")
        if not data or not data[0] or data[0] == b"":
            # Try searching by UID
            _, search = imap.uid("search", None, "ALL")
            all_uids = search[0].split() if search[0] else []
            found = uid.encode() in all_uids
            imap.logout()
            return api_response({"error": f"UID {uid} not found in sales@ INBOX", "total_uids": len(all_uids), "uid_in_list": found})

        msg = _em.message_from_bytes(data[0][1])
        subj = msg.get("Subject", "")
        from_hdr = msg.get("From", "")
        # Analyze structure
        parts = []
        pdf_count = 0
        nested_pdf_count = 0
        for part in msg.walk():
            ct = part.get_content_type()
            fn = part.get_filename() or ""
            parts.append({"type": ct, "filename": fn[:60]})
            if fn.lower().endswith(".pdf"):
                pdf_count += 1
            if ct == "message/rfc822":
                payload = part.get_payload()
                inners = payload if isinstance(payload, list) else ([payload] if hasattr(payload, 'walk') else [])
                for inner in inners:
                    if hasattr(inner, 'walk'):
                        for ip in inner.walk():
                            ipfn = ip.get_filename() or ""
                            if ipfn.lower().endswith(".pdf"):
                                nested_pdf_count += 1
                                parts.append({"type": "NESTED_PDF", "filename": ipfn[:60]})
        # Check forward signals
        subj_lower = subj.lower().strip()
        is_fwd = any(subj_lower.startswith(p) for p in ["fwd:", "fw:"])
        body = ""
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try: body = part.get_payload(decode=True).decode(errors="replace")
                except Exception: pass
                break
        has_fwd_body = any(m in body.lower() for m in ["forwarded message", "begin forwarded", "---------- forwarded"])
        has_rfc822 = any(p.get_content_type() == "message/rfc822" for p in msg.walk())
        signals = sum([is_fwd, has_fwd_body, bool(pdf_count), bool(nested_pdf_count), has_rfc822])

        # Check processed status
        from src.api.dashboard import POLL_STATUS
        poller = POLL_STATUS.get("_poller_instance")
        in_processed = uid in poller._processed if poller else "unknown"

        imap.logout()
        return api_response({
            "uid": uid,
            "subject": subj[:120],
            "from": from_hdr[:80],
            "parts": parts[:20],
            "top_level_pdfs": pdf_count,
            "nested_pdfs": nested_pdf_count,
            "forward_signals": {
                "fwd_subject": is_fwd,
                "fwd_body": has_fwd_body,
                "has_rfc822": has_rfc822,
                "top_pdfs": pdf_count,
                "nested_pdfs": nested_pdf_count,
                "total_signals": signals,
                "would_pass": signals >= 2,
            },
            "in_processed": in_processed,
            "body_preview": body[:500],
        })
    except Exception as e:
        log.error("email diagnose: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/email/force-process/<uid>", methods=["GET", "POST"])
@auth_required
def api_v1_email_force_process(uid):
    """Bypass poller — fetch email by UID, extract attachments (including ZIP), create RFQ/PC directly."""
    try:
        import imaplib
        import email as _em
        import zipfile
        import io
        import re as _re
        import uuid as _uuid
        from datetime import datetime as _dt

        addr = os.environ.get("GMAIL_ADDRESS", "")
        pwd = os.environ.get("GMAIL_PASSWORD", "")
        if not addr or not pwd:
            return api_response(error="No email credentials")

        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(addr, pwd)
        imap.select("INBOX")
        _, data = imap.uid("fetch", uid.encode(), "(BODY.PEEK[])")
        if not data or not data[0]:
            imap.logout()
            return api_response(error=f"UID {uid} not found")

        msg = _em.message_from_bytes(data[0][1])
        subj = msg.get("Subject", "")
        from_hdr = msg.get("From", "")
        sender_email = _re.search(r'[\w.+-]+@[\w.-]+', from_hdr)
        sender_email = sender_email.group(0).lower() if sender_email else ""

        # Extract forwarded sender
        body = ""
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    body = part.get_payload(decode=True).decode(errors="replace")
                except Exception:
                    pass
                break
        orig_sender = ""
        _fm = _re.search(r'From:.*?([\w.+-]+@[\w.-]+)', body, _re.IGNORECASE)
        if _fm:
            _addr = _fm.group(1).lower()
            if not any(_addr.endswith(f"@{d}") for d in ["reytechinc.com", "reytech.com"]):
                orig_sender = _addr

        # Extract ALL attachments — PDFs + PDFs from ZIPs
        try:
            from src.core.paths import UPLOAD_DIR
        except Exception:
            UPLOAD_DIR = os.path.join(os.environ.get("DATA_DIR", "/data"), "uploads")
        save_dir = os.path.join(UPLOAD_DIR, f"force_{uid}_{_dt.now().strftime('%H%M%S')}")
        os.makedirs(save_dir, exist_ok=True)

        pdfs = []
        for part in msg.walk():
            fn = part.get_filename()
            if not fn:
                continue
            fn_lower = fn.lower()
            if fn_lower.endswith(".pdf"):
                safe = _re.sub(r'[^\w\-_. ()]+', '_', fn)
                path = os.path.join(save_dir, safe)
                payload = part.get_payload(decode=True)
                if payload:
                    with open(path, "wb") as f:
                        f.write(payload)
                    pdfs.append({"path": path, "filename": safe, "type": "unknown"})
            elif fn_lower.endswith(".zip"):
                payload = part.get_payload(decode=True)
                if payload:
                    try:
                        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                            for zn in zf.namelist():
                                if zn.lower().endswith(".pdf") and not zn.startswith("__MACOSX"):
                                    safe = _re.sub(r'[^\w\-_. ()]+', '_', os.path.basename(zn))
                                    path = os.path.join(save_dir, safe)
                                    with open(path, "wb") as f:
                                        f.write(zf.read(zn))
                                    pdfs.append({"path": path, "filename": safe, "type": "unknown"})
                    except Exception as _ze:
                        log.warning("ZIP extract: %s", _ze)

        if not pdfs:
            imap.logout()
            return api_response({"error": "No PDFs found (even after ZIP extraction)", "parts": [
                {"filename": p.get_filename() or "", "type": p.get_content_type()} for p in msg.walk() if p.get_filename()
            ]})

        # Identify form types
        from src.forms.rfq_parser import identify_attachments
        id_map = identify_attachments([p["path"] for p in pdfs])
        for p in pdfs:
            for ftype, fpath in id_map.items():
                if fpath == p["path"]:
                    p["type"] = ftype

        # Build RFQ email info dict
        rfq_id = _dt.now().strftime("%Y%m%d_%H%M%S") + "_" + uid[:4]
        rfq_email = {
            "id": rfq_id,
            "email_uid": uid,
            "message_id": msg.get("Message-ID", ""),
            "subject": subj,
            "sender": from_hdr,
            "sender_email": orig_sender or sender_email,
            "body_text": body[:3000],
            "attachments": pdfs,
            "solicitation_hint": "",
        }

        # Extract solicitation from body
        _sol_m = _re.search(r'Requisition\s*\[?(\d+)\]?', body)
        if _sol_m:
            rfq_email["solicitation_hint"] = _sol_m.group(1)

        # Process through normal pipeline
        from src.api.dashboard import process_rfq_email
        result = process_rfq_email(rfq_email)

        imap.logout()
        return api_response({
            "processed": True,
            "rfq_id": rfq_id,
            "subject": subj[:100],
            "original_sender": orig_sender,
            "pdfs_found": len(pdfs),
            "pdf_files": [p["filename"] for p in pdfs],
            "form_types": {p["filename"]: p["type"] for p in pdfs},
            "result": "created" if result else "routed_to_pc_or_skipped",
        })
    except Exception as e:
        log.error("force-process: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/data/fix-buyer-names", methods=["GET", "POST"])
@auth_required
def api_v1_fix_buyer_names():
    """Fix garbage buyer names on existing PCs and RFQs using email sender."""
    try:
        from src.core.contracts import resolve_buyer_name, _is_real_name
        from src.api.dashboard import _load_price_checks, _save_price_checks
        fixed_pcs = 0
        fixed_rfqs = 0
        details = []
        pcs = _load_price_checks()
        for pcid, pc in pcs.items():
            current = pc.get("requestor", "")
            email = pc.get("requestor_email", pc.get("email", ""))
            if current and "@" in current and not email:
                email = current
                pc["requestor_email"] = current
                current = ""
            if (current and not _is_real_name(current) and email) or (not current and email):
                new_name = resolve_buyer_name(current, "", email)
                if new_name and new_name != current:
                    pc["requestor"] = new_name
                    pc["_original_parsed_requestor"] = current
                    details.append({"type": "pc", "id": pcid[:20], "old": current, "new": new_name})
                    fixed_pcs += 1
        if fixed_pcs:
            _save_price_checks(pcs)
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            current = r.get("requestor_name", "")
            email = r.get("requestor_email", "")
            # If name field contains an email address, use it as email source
            if current and "@" in current and not email:
                email = current
                r["requestor_email"] = current
                current = ""
            if (current and not _is_real_name(current) and email) or (not current and email):
                new_name = resolve_buyer_name(current, "", email)
                if new_name and new_name != current:
                    r["requestor_name"] = new_name
                    r["_original_parsed_requestor"] = current
                    details.append({"type": "rfq", "id": rid[:20], "old": current, "new": new_name})
                    fixed_rfqs += 1
        if fixed_rfqs:
            save_rfqs(rfqs)
        return api_response({"fixed_pcs": fixed_pcs, "fixed_rfqs": fixed_rfqs, "details": details})
    except Exception as e:
        log.error("fix-buyer-names: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── Data Integrity ────────────────────────────────────────────────────

@bp.route("/api/v1/quotes/fix-orphans", methods=["GET", "POST"])
@auth_required
def api_v1_fix_orphan_quotes():
    """Link orphan quotes (no source_pc_id/source_rfq_id) to matching PCs/RFQs."""
    try:
        from src.core.db import get_db
        from src.api.dashboard import _load_price_checks
        from src.core.contracts import safe_match
        import json as _json
        pcs = _load_price_checks()
        rfqs = load_rfqs()
        fixed = []
        with get_db() as conn:
            orphans = conn.execute("""
                SELECT quote_number, institution, requestor, rfq_number, contact_email
                FROM quotes
                WHERE (source_pc_id IS NULL OR source_pc_id = '')
                AND (source_rfq_id IS NULL OR source_rfq_id = '')
                AND status NOT IN ('void', 'cancelled')
            """).fetchall()
            for o in orphans:
                qn, inst, req, rfq_num, email = o[0], o[1] or "", o[2] or "", o[3] or "", o[4] or ""
                linked_to = None
                link_type = None
                # Try RFQ by solicitation number
                if rfq_num:
                    for rid, r in rfqs.items():
                        if safe_match(rfq_num, r.get("solicitation_number", "")):
                            linked_to = rid
                            link_type = "source_rfq_id"
                            break
                # Try PC by institution or requestor
                if not linked_to:
                    for pcid, pc in pcs.items():
                        if email and safe_match(email, pc.get("requestor_email", "") or pc.get("requestor", "")):
                            linked_to = pcid
                            link_type = "source_pc_id"
                            break
                        if safe_match(inst, pc.get("institution", "")):
                            linked_to = pcid
                            link_type = "source_pc_id"
                            break
                if linked_to:
                    conn.execute(f"UPDATE quotes SET {link_type}=? WHERE quote_number=?",
                                (linked_to, qn))
                    fixed.append({"quote": qn, "linked_to": linked_to, "via": link_type})
        return api_response({"fixed": fixed, "orphans_checked": len(orphans)})
    except Exception as e:
        log.error("fix-orphans: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/data/contract-violations")
@auth_required
def api_v1_contract_violations():
    """View recent contract violations."""
    try:
        from src.core.contracts import get_blocked_saves
        return api_response({"violations": get_blocked_saves(100), "count": len(get_blocked_saves(100))})
    except Exception as e:
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/data/integrity-check")
@auth_required
def api_v1_integrity_check():
    """Full data integrity audit — checks every record against its contract."""
    try:
        from src.core.contracts import validate_quote, validate_pc, validate_rfq
        from src.core.db import get_db
        issues = []
        with get_db() as conn:
            quotes = conn.execute("""
                SELECT quote_number, total, items_count, source_pc_id, source_rfq_id,
                       status, institution FROM quotes WHERE status != 'void'
            """).fetchall()
        for q in quotes:
            qd = {"quote_number": q[0], "total": q[1], "items_count": q[2],
                  "source_pc_id": q[3], "source_rfq_id": q[4], "status": q[5],
                  "institution": q[6]}
            valid, v = validate_quote(qd)
            if not valid:
                issues.append({"type": "quote", "id": q[0], "violations": v})
        try:
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            for pcid, pc in pcs.items():
                valid, v = validate_pc(pc, pcid)
                if not valid:
                    issues.append({"type": "pc", "id": pcid[:30], "violations": v})
        except Exception:
            pass
        try:
            rfqs = load_rfqs()
            for rid, r in rfqs.items():
                valid, v = validate_rfq(r, rid)
                if not valid:
                    issues.append({"type": "rfq", "id": rid[:30], "violations": v})
        except Exception:
            pass
        return api_response({
            "total_issues": len(issues), "issues": issues[:100],
            "summary": {
                "quotes": len([i for i in issues if i["type"] == "quote"]),
                "pcs": len([i for i in issues if i["type"] == "pc"]),
                "rfqs": len([i for i in issues if i["type"] == "rfq"]),
            }
        })
    except Exception as e:
        log.error("integrity-check: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


# ── DGS Form Auto-Downloader ─────────────────────────────────────────

@bp.route("/api/v1/forms/status")
@auth_required
def api_v1_forms_status():
    """Status of all registered DGS form templates."""
    try:
        from src.agents.form_updater import get_form_status, FORM_REGISTRY
        status = get_form_status()
        on_disk = sum(1 for s in status if s["on_disk"])
        return api_response({
            "forms": status,
            "total_registered": len(FORM_REGISTRY),
            "on_disk": on_disk,
            "missing": len(FORM_REGISTRY) - on_disk,
        })
    except Exception as e:
        log.error("forms/status error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/forms/update", methods=["POST"])
@auth_required
def api_v1_forms_update():
    """Download/update all registered forms from DGS."""
    try:
        from src.agents.form_updater import update_all_forms
        data = request.get_json(force=True, silent=True) or {}
        result = update_all_forms(force=data.get("force", False))
        return api_response(result)
    except Exception as e:
        log.error("forms/update error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/forms/update/<form_id>", methods=["POST"])
@auth_required
def api_v1_forms_update_single(form_id):
    """Download a specific form by ID."""
    try:
        from src.agents.form_updater import download_form, FORM_REGISTRY
        if form_id not in FORM_REGISTRY:
            return api_response(error=f"Unknown form: {form_id}", status=404)
        result = download_form(form_id, FORM_REGISTRY[form_id], force=True)
        return api_response(result)
    except Exception as e:
        log.error("forms/update/%s error: %s", form_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


def _split_bid_package(pdf_path, output_dir):
    """Split a combined bid package PDF into individual form templates."""
    from pypdf import PdfReader, PdfWriter
    results = []

    # Form detection markers: form_name -> (keywords in page text, output filename)
    FORM_MARKERS = {
        "calrecycle_74": {
            "keywords": ["CALRECYCLE", "RECYCLED-CONTENT", "RECYCLED CONTENT", "POSTCONSUMER"],
            "filename": "calrecycle_74_blank.pdf",
        },
        "obs_1600": {
            "keywords": ["OBS 1600", "AGRICULTURAL", "FOOD PRODUCT CERTIFICATION"],
            "filename": "obs_1600_blank.pdf",
        },
        "cuf": {
            "keywords": ["COMMERCIALLY USEFUL FUNCTION", "CV 012", "CV-012"],
            "filename": "cv012_cuf_blank.pdf",
        },
        "dvbe_843": {
            "keywords": ["DVBE DECLARATIONS", "DGS PD 843", "PD 843"],
            "filename": "dvbe_843_blank.pdf",
        },
        "darfur": {
            "keywords": ["DARFUR CONTRACTING", "DARFUR ACT"],
            "filename": "darfur_blank.pdf",
        },
        "bidder_decl": {
            "keywords": ["BIDDER DECLARATION", "GS/OAS 09"],
            "filename": "bidder_declaration_blank.pdf",
        },
        "drug_free": {
            "keywords": ["DRUG-FREE WORKPLACE", "DRUG FREE WORKPLACE"],
            "filename": "drug_free_blank.pdf",
        },
        "genai_708": {
            "keywords": ["GENAI", "GEN AI", "ARTIFICIAL INTELLIGENCE", "708"],
            "filename": "genai_708_blank.pdf",
        },
        "std_204": {
            "keywords": ["STD 204", "PAYEE DATA"],
            "filename": "std204_blank.pdf",
        },
    }

    try:
        reader = PdfReader(pdf_path)
        # Tag each page with its form type
        page_forms = []
        for i, page in enumerate(reader.pages):
            text = (page.extract_text() or "").upper()
            form_type = None
            for ftype, info in FORM_MARKERS.items():
                if any(kw.upper() in text for kw in info["keywords"]):
                    form_type = ftype
                    break
            page_forms.append(form_type)

        # Group consecutive pages of same form type
        current_form = None
        current_pages = []
        groups = []
        for i, ftype in enumerate(page_forms):
            if ftype and ftype != current_form:
                if current_form and current_pages:
                    groups.append((current_form, current_pages))
                current_form = ftype
                current_pages = [i]
            elif ftype == current_form:
                current_pages.append(i)
            else:
                # Unidentified page — attach to current form if within 1 page
                if current_form and current_pages and (i - current_pages[-1]) <= 1:
                    current_pages.append(i)
                else:
                    if current_form and current_pages:
                        groups.append((current_form, current_pages))
                    current_form = None
                    current_pages = []
        if current_form and current_pages:
            groups.append((current_form, current_pages))

        # Write each form group as a separate PDF
        for ftype, pages in groups:
            info = FORM_MARKERS[ftype]
            writer = PdfWriter()
            for p in pages:
                writer.add_page(reader.pages[p])
            out_path = os.path.join(output_dir, info["filename"])
            with open(out_path, "wb") as of:
                writer.write(of)
            results.append({
                "file": info["filename"],
                "form": ftype,
                "pages": [p + 1 for p in pages],
                "size_kb": round(os.path.getsize(out_path) / 1000, 1),
            })
            log.info("Split form: %s (%d pages) -> %s", ftype, len(pages), info["filename"])

        # Also save the full package as-is
        import shutil
        full_dest = os.path.join(output_dir, "cdcr_bid_package_template.pdf")
        shutil.copy2(pdf_path, full_dest)
        results.append({
            "file": "cdcr_bid_package_template.pdf",
            "form": "full_package",
            "pages": list(range(1, len(reader.pages) + 1)),
            "size_kb": round(os.path.getsize(full_dest) / 1000, 1),
        })

    except Exception as e:
        results.append({"error": f"Split failed: {e}"})
        log.error("Bid package split: %s", e, exc_info=True)

    return results
