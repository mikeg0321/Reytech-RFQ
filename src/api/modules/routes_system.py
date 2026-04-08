"""
System & Admin API Routes — extracted from dashboard.py for reduced blast radius.

These routes handle: scheduler status, backups, health checks, migrations,
circuit breakers, data integrity, pipeline health, and system diagnostics.
None reference dashboard.py internal state (load_rfqs, _load_price_checks, etc.).
"""

import os
import logging
from datetime import datetime, timezone
from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route

log = logging.getLogger("reytech.system")


# ── Scheduler & Backup ───────────────────────────────────────────────────────

@bp.route("/api/scheduler/status")
@auth_required
@safe_route
def scheduler_status():
    """Returns health status of all background jobs."""
    from src.core.scheduler import get_all_jobs, backup_health
    jobs = get_all_jobs()
    bh = backup_health()
    dead = [j for j in jobs if j["status"] == "dead"]
    return jsonify({
        "ok": True,
        "jobs": jobs,
        "total": len(jobs),
        "dead_count": len(dead),
        "dead_jobs": [j["name"] for j in dead],
        "backup_health": bh,
    })


@bp.route("/api/admin/backups")
@auth_required
@safe_route
def admin_backups():
    """List available database backups."""
    from src.core.scheduler import list_backups, backup_health
    return jsonify({
        "ok": True,
        "backups": list_backups(),
        "health": backup_health(),
    })


@bp.route("/api/admin/backup-now", methods=["GET", "POST"])
@auth_required
@safe_route
def admin_backup_now():
    """Trigger an immediate database backup."""
    from src.core.scheduler import run_backup
    result = run_backup()
    return jsonify(result)


# ── Circuit Breakers ─────────────────────────────────────────────────────────

@bp.route("/api/system/circuits")
@auth_required
@safe_route
def api_circuit_breaker_status():
    """Returns status of all circuit breakers for external API monitoring."""
    from src.core.circuit_breaker import all_status
    circuits = all_status()
    open_count = sum(1 for c in circuits if c["state"] == "open")
    return jsonify({"ok": True, "circuits": circuits, "total": len(circuits), "open_count": open_count})


# ── System Health ────────────────────────────────────────────────────────────

@bp.route("/api/system/health")
@auth_required
@safe_route
def system_health():
    health = {"status": "ok", "checks": {}}
    try:
        from src.core.db import get_db, DB_PATH
        with get_db() as conn:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
            health["checks"]["database"] = {
                "ok": True, "tables": tables,
                "size_mb": round(db_size / 1048576, 1)
            }
    except Exception as e:
        health["checks"]["database"] = {"ok": False, "error": str(e)}
        health["status"] = "degraded"

    try:
        from src.core.scheduler import get_scheduler_status
        sched = get_scheduler_status()
        dead = sched.get("dead_count", 0)
        health["checks"]["scheduler"] = {
            "ok": dead == 0, "jobs": sched.get("job_count", 0),
            "dead_jobs": dead
        }
        if dead > 0:
            health["status"] = "degraded"
    except Exception as e:
        health["checks"]["scheduler"] = {"ok": False, "error": str(e)}

    try:
        from src.core.scheduler import backup_health
        bh = backup_health()
        health["checks"]["backups"] = bh
        if not bh.get("ok"):
            health["status"] = "degraded"
    except Exception as e:
        health["checks"]["backups"] = {"ok": False, "error": str(e)}

    try:
        from src.core.migrations import get_migration_status
        ms = get_migration_status()
        health["checks"]["schema"] = {
            "ok": ms.get("up_to_date", False),
            "version": ms.get("current_version", 0),
            "pending": len(ms.get("pending", []))
        }
    except Exception as e:
        health["checks"]["schema"] = {"ok": False, "error": str(e)}

    return jsonify(health)


# ── Migrations ───────────────────────────────────────────────────────────────

@bp.route("/api/system/migrations")
@auth_required
@safe_route
def migration_status():
    """Schema migration status and history."""
    from src.core.migrations import get_migration_status
    return jsonify(get_migration_status())


@bp.route("/api/system/migrations/run", methods=["POST"])
@auth_required
@safe_route
def run_migrations_api():
    """Apply pending schema migrations."""
    from src.core.migrations import run_migrations
    result = run_migrations()
    return jsonify(result)


# ── Data Integrity & Pipeline ────────────────────────────────────────────────

@bp.route("/api/system/integrity")
@auth_required
@safe_route
def data_integrity():
    """Run data integrity checks across all tables."""
    from src.core.data_integrity import run_integrity_checks
    return jsonify(run_integrity_checks())


@bp.route("/api/system/pdf-versions")
@auth_required
@safe_route
def pdf_template_versions():
    """Current PDF template versions and generation stats."""
    from src.forms.pdf_versioning import get_version_info, TEMPLATE_VERSIONS
    info = get_version_info()
    return jsonify({"ok": True, "templates": info,
                    "registry": {k: v["history"] for k, v in TEMPLATE_VERSIONS.items()}})


@bp.route("/api/system/trace/<doc_id>")
@auth_required
@safe_route
def trace_document_api(doc_id):
    """Trace a document through the full RFQ->Quote->Order pipeline."""
    from src.core.data_tracer import trace_document
    doc_type = request.args.get("type", "auto")
    return jsonify(trace_document(doc_id, doc_type=doc_type))


@bp.route("/api/system/pipeline")
@auth_required
@safe_route
def pipeline_stats():
    """Pipeline overview: counts and conversion rates across all stages."""
    from src.core.data_tracer import get_pipeline_stats
    return jsonify(get_pipeline_stats())


@bp.route("/api/system/pipeline-health")
@auth_required
@safe_route
def api_pipeline_health():
    """Self-diagnosis: checks every stage of the data pipeline for issues."""
    from src.core.db import get_db
    issues = []
    stats = {}
    try:
        with get_db() as conn:
            stats["scprs_po_lines"] = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
            stats["scprs_po_master"] = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
            if stats["scprs_po_lines"] == 0:
                issues.append({"level": "critical", "area": "SCPRS", "msg": "No SCPRS PO data — run harvest first"})

            stats["won_quotes"] = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
            if stats["won_quotes"] == 0 and stats["scprs_po_lines"] > 0:
                issues.append({"level": "critical", "area": "won_quotes", "msg": f"SCPRS has {stats['scprs_po_lines']} lines but won_quotes KB is empty — sync needed"})

            stats["product_catalog"] = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
            if stats["product_catalog"] == 0:
                issues.append({"level": "warning", "area": "catalog", "msg": "Product catalog empty — enrichment matches will fail"})

            stats["price_checks"] = conn.execute("SELECT COUNT(*) FROM price_checks").fetchone()[0]
            try:
                stuck_pcs = conn.execute(
                    "SELECT COUNT(*) FROM price_checks WHERE status IN ('new','parsed') AND created_at < datetime('now', '-7 days')"
                ).fetchone()[0]
                if stuck_pcs > 0:
                    issues.append({"level": "warning", "area": "price_checks", "msg": f"{stuck_pcs} PCs stuck in new/parsed for >7 days"})
            except Exception:
                pass

            try:
                pending_old = conn.execute(
                    "SELECT COUNT(*) FROM quotes WHERE is_test=0 AND status='pending' AND created_date < date('now', '-7 days')"
                ).fetchone()[0]
                if pending_old > 0:
                    issues.append({"level": "warning", "area": "quotes", "msg": f"{pending_old} quotes stuck as pending for >7 days — never sent?"})
                sent_no_followup = conn.execute(
                    "SELECT COUNT(*) FROM quotes WHERE is_test=0 AND status='sent' AND follow_up_count=0 AND sent_at < datetime('now', '-7 days')"
                ).fetchone()[0]
                if sent_no_followup > 0:
                    issues.append({"level": "info", "area": "quotes", "msg": f"{sent_no_followup} sent quotes with 0 follow-ups after 7+ days"})
            except Exception:
                pass

            try:
                overdue_pulls = conn.execute("""
                    SELECT agency_key, last_pull, pull_interval_hours,
                           ROUND((JULIANDAY('now') - JULIANDAY(last_pull)) * 24, 1) as hours_since
                    FROM scprs_pull_schedule
                    WHERE last_pull IS NOT NULL
                      AND (JULIANDAY('now') - JULIANDAY(last_pull)) * 24 > pull_interval_hours * 2
                """).fetchall()
                for r in overdue_pulls:
                    issues.append({"level": "warning", "area": "SCPRS", "msg": f"{r[0]} pull overdue ({r[3]}h since last, expected every {r[2]}h)"})
            except Exception:
                pass

            try:
                from src.core.circuit_breaker import all_status
                for cb in all_status():
                    if cb["state"] == "open":
                        issues.append({"level": "critical", "area": "circuits", "msg": f"{cb['name']} circuit OPEN ({cb['failure_count']} failures)"})
            except Exception:
                pass

            stats["issues_count"] = len(issues)
            stats["critical"] = sum(1 for i in issues if i["level"] == "critical")
            stats["warnings"] = sum(1 for i in issues if i["level"] == "warning")

    except Exception as e:
        issues.append({"level": "critical", "area": "database", "msg": str(e)})

    health = "healthy" if not issues else ("degraded" if all(i["level"] != "critical" for i in issues) else "unhealthy")
    return jsonify({"ok": True, "health": health, "issues": issues, "stats": stats})


# ── Preflight & QA ───────────────────────────────────────────────────────────

@bp.route("/api/system/qa")
@auth_required
@safe_route
def qa_dashboard():
    """QA dashboard — combined health, integrity, pipeline, and test status."""
    result = {"ok": True, "checked_at": datetime.now().isoformat(), "sections": {}}

    try:
        from src.core.db import get_db, DB_PATH
        with get_db() as conn:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        result["sections"]["database"] = {
            "ok": True, "tables": tables,
            "size_mb": round(db_size / 1048576, 1)
        }
    except Exception as e:
        result["sections"]["database"] = {"ok": False, "error": str(e)}
        result["ok"] = False

    try:
        from src.core.data_integrity import run_integrity_checks
        ic = run_integrity_checks()
        result["sections"]["integrity"] = {
            "ok": ic["ok"], "passed": ic["passed"], "failed": ic["failed"],
            "details": [c for c in ic["checks"] if not c["ok"]]
        }
        if not ic["ok"]:
            result["ok"] = False
    except Exception as e:
        result["sections"]["integrity"] = {"ok": False, "error": str(e)}

    try:
        from src.core.data_tracer import get_pipeline_stats
        ps = get_pipeline_stats()
        result["sections"]["pipeline"] = ps
    except Exception as e:
        result["sections"]["pipeline"] = {"ok": False, "error": str(e)}

    try:
        from src.core.migrations import get_migration_status
        ms = get_migration_status()
        result["sections"]["schema"] = {
            "ok": ms.get("up_to_date", False),
            "version": ms.get("current_version"),
            "pending": len(ms.get("pending", []))
        }
    except Exception as e:
        result["sections"]["schema"] = {"ok": False, "error": str(e)}

    try:
        from flask import current_app
        rules = list(current_app.url_map.iter_rules())
        result["sections"]["routes"] = {"ok": len(rules) > 500, "count": len(rules)}
    except Exception as e:
        result["sections"]["routes"] = {"ok": False, "error": str(e)}

    try:
        from src.forms.pdf_versioning import get_version_info
        result["sections"]["pdf_templates"] = get_version_info()
    except Exception as e:
        result["sections"]["pdf_templates"] = {"error": str(e)}

    return jsonify(result)


@bp.route("/api/system/preflight")
@auth_required
@safe_route
def system_preflight():
    """Combined pre-flight check: health + integrity + schema."""
    result = {"status": "ok", "checks": {}}

    try:
        from src.core.db import get_db, DB_PATH
        with get_db() as conn:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        result["checks"]["database"] = {"ok": True, "tables": tables,
                                         "size_mb": round(db_size / 1048576, 1)}
    except Exception as e:
        result["checks"]["database"] = {"ok": False, "error": str(e)}
        result["status"] = "degraded"

    try:
        from src.core.migrations import get_migration_status
        ms = get_migration_status()
        result["checks"]["schema"] = {
            "ok": ms.get("up_to_date", False),
            "version": ms.get("current_version"),
            "pending": len(ms.get("pending", []))
        }
    except Exception as e:
        result["checks"]["schema"] = {"ok": False, "error": str(e)}

    try:
        from src.core.data_integrity import run_integrity_checks
        ic = run_integrity_checks()
        result["checks"]["integrity"] = {
            "ok": ic["ok"], "passed": ic["passed"],
            "failed": ic["failed"]
        }
        if not ic["ok"]:
            result["status"] = "degraded"
    except Exception as e:
        result["checks"]["integrity"] = {"ok": False, "error": str(e)}

    try:
        from flask import current_app
        rules = list(current_app.url_map.iter_rules())
        result["checks"]["routes"] = {"ok": len(rules) > 500, "count": len(rules)}
    except Exception as e:
        result["checks"]["routes"] = {"ok": False, "error": str(e)}

    return jsonify(result)


# ── Version & Route Map ──────────────────────────────────────────────────────

@bp.route("/ver")
def public_version():
    """Public — returns deployed git commit. No auth needed."""
    import subprocess as _sp
    try:
        commit = _sp.check_output(["git", "rev-parse", "--short", "HEAD"],
                                   stderr=_sp.DEVNULL).decode().strip()
    except Exception:
        commit = "unknown"
    return jsonify({"commit": commit})


@bp.route("/api/system/routes")
@auth_required
@safe_route
def api_route_map():
    """Auto-generated API documentation — all routes with methods."""
    from flask import current_app
    routes = []
    for rule in sorted(current_app.url_map.iter_rules(), key=lambda r: r.rule):
        if rule.rule.startswith("/static"):
            continue
        methods = sorted(rule.methods - {"HEAD", "OPTIONS"})
        routes.append({
            "path": rule.rule,
            "methods": methods,
            "endpoint": rule.endpoint,
        })
    return jsonify({
        "total": len(routes),
        "api_routes": [r for r in routes if r["path"].startswith("/api/")],
        "page_routes": [r for r in routes if not r["path"].startswith("/api/")],
    })


# ── SCPRS Sync ───────────────────────────────────────────────────────────────

@bp.route("/api/system/resync-scprs", methods=["POST", "GET"])
@auth_required
@safe_route
def api_resync_scprs():
    """Drop all won_quotes and re-sync from SCPRS with corrected per-unit prices."""
    from src.core.db import get_db
    with get_db() as conn:
        old_count = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
        conn.execute("DELETE FROM won_quotes")
        log.info("Cleared %d old won_quotes for clean re-sync", old_count)
    from src.knowledge.won_quotes_db import sync_from_scprs_tables
    result = sync_from_scprs_tables()
    with get_db() as conn:
        new_count = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
    result["cleared"] = old_count
    result["won_quotes_total"] = new_count
    return jsonify({"ok": True, **result})


@bp.route("/api/system/sync-scprs", methods=["POST", "GET"])
@auth_required
@safe_route
def api_sync_scprs():
    """Force sync SCPRS harvest data -> won_quotes KB."""
    from src.core.db import get_db
    with get_db() as conn:
        total_lines = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
        eligible = conn.execute("SELECT COUNT(*) FROM scprs_po_lines WHERE unit_price > 0 AND description != ''").fetchone()[0]
        zero_price = conn.execute("SELECT COUNT(*) FROM scprs_po_lines WHERE unit_price = 0 OR unit_price IS NULL").fetchone()[0]
        no_master = conn.execute("""
            SELECT COUNT(*) FROM scprs_po_lines l
            LEFT JOIN scprs_po_master p ON l.po_id = p.id
            WHERE p.id IS NULL
        """).fetchone()[0]

    from src.knowledge.won_quotes_db import sync_from_scprs_tables
    result = sync_from_scprs_tables()

    with get_db() as conn:
        wq = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
    result["won_quotes_total"] = wq
    result["scprs_po_lines_total"] = total_lines
    result["eligible_lines"] = eligible
    result["zero_price_lines"] = zero_price
    result["orphan_lines"] = no_master
    result["coverage_pct"] = round(wq / max(eligible, 1) * 100, 1)
    return jsonify({"ok": True, **result})
