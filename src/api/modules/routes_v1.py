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

        return api_response({
            "version": version,
            "uptime_seconds": uptime,
            "db": db_info,
            "queues": queues,
            "agents": agents,
            "scprs_harvest": harvest,
            "connectors": connector_status,
            "compliance": compliance,
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
def api_v1_harvest_status():
    """Current connector status. No auth — read only."""
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
_backfill_running = False


@bp.route("/api/v1/harvest/backfill-details")
@auth_required
def api_v1_backfill_details():
    """Backfill detail pages for POs that have no line items. Background thread."""
    global _backfill_running
    if _backfill_running:
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
            global _backfill_running
            _backfill_running = True
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
                _backfill_running = False

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
