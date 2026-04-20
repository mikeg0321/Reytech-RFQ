"""Quoting Health Dashboard — single-page observability for the full
quoting pipeline from email ingest → classification → PC/RFQ creation
→ linking → pricing → quote.

Reads directly from the canonical tables:
    price_checks      — PC records with agency/items/status
    quotes            — outbound quotes with cost/margin
    utilization_events — feature-use events recorded by time_feature /
                         record_feature_use (60-day window)
    feature_flags     — runtime flag state (ingest.classifier_v2_enabled
                        in particular)

Surfaces the things that matter right after the unified-ingest flag
goes live:
    1. Is the flag actually on?
    2. Is the v2 classifier getting invoked?
    3. Is it crashing? (ingest.classify_crashed feature presence)
    4. Confidence distribution — are we landing in high-confidence or
       tripping through low-confidence review?
    5. Ingest → PC/RFQ → link → quote funnel for the last 24h and 7d
    6. PC→RFQ link success rate (linked vs unlinked new RFQs)
    7. Top errored features (ok=0) as a fast triage list

Every query is defensive: tables may not exist in a fresh checkout,
utilization rows may be empty, feature flags may be missing. The
dashboard never raises — missing data shows as "--".
"""
from flask import request
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech.health")

from src.api.render import render_page
from src.core.db import get_db
from src.core.flags import get_flag, list_flags

from datetime import datetime, timedelta
import json


def _since(days: int) -> str:
    return (datetime.now() - timedelta(days=days)).isoformat()


def _safe_fetchall(sql: str, params=()):
    """Defensive read — any DB error returns an empty list."""
    try:
        with get_db() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) if hasattr(r, "keys") else r for r in rows]
    except Exception as e:
        log.debug("health query failed: %s | sql=%s", e, sql[:120])
        return []


def _safe_fetchone(sql: str, params=()):
    try:
        with get_db() as conn:
            row = conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return dict(row) if hasattr(row, "keys") else row
    except Exception as e:
        log.debug("health query failed: %s | sql=%s", e, sql[:120])
        return None


# ── Dashboard data builders ──────────────────────────────────────────────

def _build_flag_card():
    """Current state of the critical classifier_v2 flag + all runtime flags."""
    flags = list_flags() or []
    classifier_v2 = get_flag("ingest.classifier_v2_enabled", False)
    return {
        "classifier_v2_on": bool(classifier_v2),
        "flags": flags,
        "flag_count": len(flags),
    }


def _build_classifier_activity(days: int):
    """How often has the v2 ingest pipeline been invoked in the window?
    Splits success vs crash so we know whether the flag flip is landing
    cleanly or whether we need to roll back."""
    since = _since(days)
    happy = _safe_fetchone(
        """SELECT COUNT(*) AS n,
                  AVG(duration_ms) AS avg_ms,
                  SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS errors
             FROM utilization_events
            WHERE feature = 'ingest.process_buyer_request'
              AND created_at >= ?""",
        (since,),
    ) or {"n": 0, "avg_ms": 0, "errors": 0}
    crashed = _safe_fetchone(
        """SELECT COUNT(*) AS n FROM utilization_events
            WHERE feature = 'ingest.classify_crashed'
              AND created_at >= ?""",
        (since,),
    ) or {"n": 0}
    total = int(happy.get("n") or 0)
    errs = int(happy.get("errors") or 0) + int(crashed.get("n") or 0)
    return {
        "invocations": total,
        "crashes": int(crashed.get("n") or 0),
        "errors": errs,
        "error_rate": (errs / total) if total else None,
        "avg_ms": round(float(happy.get("avg_ms") or 0), 1),
    }


def _build_confidence_distribution(days: int):
    """Bucket the confidence scores recorded on the classifier timer
    into low / mid / high. Reads `context` JSON column from utilization
    events so we don't need a second table."""
    since = _since(days)
    rows = _safe_fetchall(
        """SELECT context FROM utilization_events
            WHERE feature = 'ingest.process_buyer_request'
              AND created_at >= ?
              AND ok = 1""",
        (since,),
    )
    low = mid = high = 0
    shapes: dict = {}
    agencies: dict = {}
    for r in rows:
        try:
            ctx = json.loads(r.get("context") or "{}")
        except Exception as _e:
            log.debug("suppressed: %s", _e)
            continue
        c = ctx.get("confidence")
        try:
            c = float(c) if c is not None else None
        except Exception as _e:
            log.debug("suppressed: %s", _e)
            c = None
        if c is None:
            continue
        if c < 0.60:
            low += 1
        elif c < 0.85:
            mid += 1
        else:
            high += 1
        s = ctx.get("shape") or "unknown"
        shapes[s] = shapes.get(s, 0) + 1
        a = ctx.get("agency") or "other"
        agencies[a] = agencies.get(a, 0) + 1
    top_shapes = sorted(shapes.items(), key=lambda x: -x[1])[:6]
    top_agencies = sorted(agencies.items(), key=lambda x: -x[1])[:6]
    total = low + mid + high
    return {
        "low": low,
        "mid": mid,
        "high": high,
        "total": total,
        "low_pct": round(100 * low / total, 1) if total else 0,
        "mid_pct": round(100 * mid / total, 1) if total else 0,
        "high_pct": round(100 * high / total, 1) if total else 0,
        "top_shapes": top_shapes,
        "top_agencies": top_agencies,
    }


def _build_funnel(days: int):
    """Ingest → PC → RFQ → Quote counts for the window, plus the
    PC→RFQ link rate from utilization events."""
    since = _since(days)

    pc_n = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM price_checks WHERE created_at >= ?",
        (since,),
    ) or {"n": 0}
    quote_n = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM quotes WHERE created_at >= ?",
        (since,),
    ) or {"n": 0}
    quote_won = _safe_fetchone(
        "SELECT COUNT(*) AS n, SUM(COALESCE(total,0)) AS won_total "
        "FROM quotes WHERE created_at >= ? AND status = 'won'",
        (since,),
    ) or {"n": 0, "won_total": 0}
    quote_sent = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM quotes WHERE created_at >= ? "
        "AND (sent_at IS NOT NULL AND sent_at != '')",
        (since,),
    ) or {"n": 0}

    # Ingest events give us the true top-of-funnel
    ingest_n = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM utilization_events "
        "WHERE feature = 'ingest.process_buyer_request' AND created_at >= ?",
        (since,),
    ) or {"n": 0}

    return {
        "ingested": int(ingest_n.get("n") or 0),
        "pcs": int(pc_n.get("n") or 0),
        "quotes": int(quote_n.get("n") or 0),
        "quotes_sent": int(quote_sent.get("n") or 0),
        "quotes_won": int(quote_won.get("n") or 0),
        "won_total": round(float(quote_won.get("won_total") or 0), 2),
    }


def _build_margin(days: int):
    """Average margin on costed quotes in the window."""
    since = _since(days)
    row = _safe_fetchone(
        """SELECT COUNT(*) AS n,
                  AVG(CASE WHEN margin_pct > 0 THEN margin_pct END) AS avg_margin,
                  AVG(CASE WHEN total > 0 THEN total END) AS avg_total,
                  SUM(COALESCE(gross_profit,0)) AS gp
             FROM quotes
            WHERE created_at >= ? AND is_test = 0 AND items_costed > 0""",
        (since,),
    ) or {}
    return {
        "costed": int(row.get("n") or 0),
        "avg_margin": round(float(row.get("avg_margin") or 0), 1),
        "avg_total": round(float(row.get("avg_total") or 0), 2),
        "gross_profit": round(float(row.get("gp") or 0), 2),
    }


def _build_top_errors(days: int, limit: int = 10):
    since = _since(days)
    rows = _safe_fetchall(
        """SELECT feature, COUNT(*) AS uses,
                  SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS errors,
                  AVG(duration_ms) AS avg_ms
             FROM utilization_events
            WHERE created_at >= ?
            GROUP BY feature
           HAVING errors > 0
            ORDER BY errors DESC
            LIMIT ?""",
        (since, limit),
    )
    for r in rows:
        uses = r.get("uses") or 0
        errs = r.get("errors") or 0
        r["error_rate"] = round(100 * errs / uses, 1) if uses else 0
        r["avg_ms"] = round(float(r.get("avg_ms") or 0), 1)
    return rows


def _build_recent_crashes(limit: int = 10):
    """Latest rows of the ingest.classify_crashed feature. The context
    column carries error type + attachment names + sender so triage
    doesn't need a second query."""
    rows = _safe_fetchall(
        """SELECT created_at, context FROM utilization_events
            WHERE feature = 'ingest.classify_crashed'
            ORDER BY created_at DESC
            LIMIT ?""",
        (limit,),
    )
    out = []
    for r in rows:
        try:
            ctx = json.loads(r.get("context") or "{}")
        except Exception as _e:
            log.debug("suppressed: %s", _e)
            ctx = {}
        out.append({
            "created_at": r.get("created_at", ""),
            "error_type": ctx.get("error_type", ""),
            "error": (ctx.get("error") or "")[:160],
            "sender": ctx.get("sender", ""),
            "file_count": ctx.get("file_count", 0),
            "attachment_names": ctx.get("attachment_names", []),
        })
    return out


# ── Route ───────────────────────────────────────────────────────────────

@bp.route("/health/quoting")
@auth_required
def quoting_health_page():
    """Single-page observability for the quoting pipeline. Read-only,
    no side effects — safe to leave open as a dashboard."""
    try:
        days = int(request.args.get("days", "7"))
    except (TypeError, ValueError):
        days = 7
    days = max(1, min(90, days))

    data = {
        "days": days,
        "flag_card": _build_flag_card(),
        "classifier_1d": _build_classifier_activity(1),
        "classifier_window": _build_classifier_activity(days),
        "confidence": _build_confidence_distribution(days),
        "funnel_1d": _build_funnel(1),
        "funnel_window": _build_funnel(days),
        "margin": _build_margin(days),
        "top_errors": _build_top_errors(days),
        "recent_crashes": _build_recent_crashes(),
        "db_health": _build_db_health(),
        "catalog_health": _build_catalog_health(),
    }
    return render_page("quoting_health.html", active_page="Health", **data)


@bp.route("/api/feature-status")
@auth_required
def feature_status_json():
    """Currently-degraded features as recorded by the silent-skip rollout
    (PRs #181-#188). Backs the dashboard banner — operators see "Claude
    amazon lookup: degraded since 14m ago (37 hits)" without waiting for
    the next quote run.

    Stale rows (>14 days) are pruned on read so the banner doesn't carry
    forgotten transient hits forever.
    """
    try:
        from src.core import feature_status
        days = request.args.get("prune_days", "14")
        try:
            prune = max(1, min(90, int(days)))
        except (TypeError, ValueError):
            prune = 14
        rows = feature_status.current_status(prune_older_than_days=prune)
        return {
            "ok": True,
            "count": len(rows),
            "by_severity": {
                "blocker": sum(1 for r in rows if r["severity"] == "blocker"),
                "warning": sum(1 for r in rows if r["severity"] == "warning"),
                "info":    sum(1 for r in rows if r["severity"] == "info"),
            },
            "rows": rows,
        }
    except Exception as e:
        log.warning("feature_status read failed: %s", e)
        # Observability failures must not break the dashboard.
        return {"ok": False, "error": str(e), "count": 0,
                "by_severity": {}, "rows": []}, 200


@bp.route("/api/health/catalog")
@auth_required
def catalog_health_json():
    """Catalog subsystem status — index enforcement + enrichment error
    count. Keyed separately from /api/health/quoting so an ops script
    can alert on just the catalog surface without pulling the whole
    quoting payload.
    """
    try:
        data = _build_catalog_health()
        return {"ok": True, **data}
    except Exception as e:
        log.warning("/api/health/catalog failed: %s", e)
        return {"ok": False, "error": str(e)}, 200


@bp.route("/api/health/quoting")
@auth_required
def quoting_health_json():
    """JSON variant of the same data — for scripts + external monitors."""
    try:
        days = int(request.args.get("days", "7"))
    except (TypeError, ValueError):
        days = 7
    days = max(1, min(90, days))
    return {
        "ok": True,
        "days": days,
        "flag_card": _build_flag_card(),
        "classifier_1d": _build_classifier_activity(1),
        "classifier_window": _build_classifier_activity(days),
        "confidence": _build_confidence_distribution(days),
        "funnel_1d": _build_funnel(1),
        "funnel_window": _build_funnel(days),
        "margin": _build_margin(days),
        "top_errors": _build_top_errors(days),
        "recent_crashes": _build_recent_crashes(),
        "db_health": _build_db_health(),
        "catalog_health": _build_catalog_health(),
    }


def _build_catalog_health():
    """Catalog-subsystem health: UPC + UNIQUE(name) index presence and a
    count of enrichment errors in the last 24h. Backs both the JSON
    endpoint and the dashboard tile.
    """
    result = {
        "upc_column": False,
        "unique_name_index": False,
        "upc_index": False,
        "enrichment_errors_24h": 0,
        "recent_enrichment_errors": [],
    }
    try:
        from src.agents.product_catalog import _get_conn
        conn = _get_conn()
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(product_catalog)").fetchall()}
            result["upc_column"] = "upc" in cols
            idxs = {r[1] for r in conn.execute("PRAGMA index_list(product_catalog)").fetchall()}
            result["unique_name_index"] = "idx_catalog_name_unique" in idxs
            result["upc_index"] = "idx_catalog_upc" in idxs
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM catalog_enrichment_errors "
                    "WHERE created_at >= datetime('now', '-1 day')"
                ).fetchone()
                result["enrichment_errors_24h"] = int(row[0]) if row else 0
                recent = conn.execute(
                    "SELECT product_id, column, error, created_at "
                    "FROM catalog_enrichment_errors "
                    "ORDER BY id DESC LIMIT 10"
                ).fetchall()
                result["recent_enrichment_errors"] = [
                    {"product_id": r[0], "column": r[1],
                     "error": (r[2] or "")[:200], "created_at": r[3]}
                    for r in recent
                ]
            except Exception:
                pass  # table may not exist on a stale DB before init runs
        finally:
            conn.close()
    except Exception as e:
        log.warning("_build_catalog_health failed: %s", e)
    return result


def _build_db_health():
    """DB size + backup status for the health dashboard."""
    import os as _dbh_os
    data_dir = _dbh_os.environ.get("DATA_DIR", _dbh_os.path.join(
        _dbh_os.path.dirname(_dbh_os.path.dirname(_dbh_os.path.dirname(_dbh_os.path.abspath(__file__)))), "data"))
    db_path = _dbh_os.path.join(data_dir, "reytech.db")
    result = {"db_size_mb": 0, "status": "unknown", "backup_count": 0}
    try:
        if _dbh_os.path.exists(db_path):
            size_mb = round(_dbh_os.path.getsize(db_path) / 1024 / 1024, 1)
            result["db_size_mb"] = size_mb
            result["status"] = "ok" if size_mb < 500 else "warning" if size_mb < 1000 else "critical"
        backup_dir = _dbh_os.path.join(data_dir, "backups")
        if _dbh_os.path.isdir(backup_dir):
            result["backup_count"] = len([f for f in _dbh_os.listdir(backup_dir) if f.endswith((".db", ".db.gz"))])
    except Exception:
        pass
    return result
