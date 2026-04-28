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
from src.core.security import rate_limit, _log_audit_internal

from datetime import datetime, timedelta
import json
import threading

# SY-3: single-flight lock around /api/admin/trim-rfq-files. VACUUM holds
# an exclusive write lock on the whole DB and on a 525 MB file can stall
# writes for tens of seconds. A double-click would queue a second VACUUM
# behind the first; returning 409 immediately is safer than stacking.
_TRIM_RFQ_FILES_LOCK = threading.Lock()


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
    PC→RFQ link rate from utilization events.

    AN-P0: every price_checks / quotes query must filter out is_test rows
    — the prod health dashboard is what ops stares at to decide whether
    classifier_v2 is landing cleanly. Test quotes (is_test=1) inflate the
    funnel by 5-30% and hide real classifier crashes under a pile of
    synthetic success. Same class as CR-5 (deadlines missing is_test on
    the RFQ loop) and IN-3/IN-4 (debug endpoints seeding ghost data).
    """
    since = _since(days)

    # `is_test IS NULL OR is_test = 0` — keeps legacy rows (NULL) while
    # excluding any row explicitly tagged as a test fixture.
    pc_n = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM price_checks "
        "WHERE created_at >= ? AND (is_test IS NULL OR is_test = 0)",
        (since,),
    ) or {"n": 0}
    quote_n = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM quotes "
        "WHERE created_at >= ? AND (is_test IS NULL OR is_test = 0)",
        (since,),
    ) or {"n": 0}
    quote_won = _safe_fetchone(
        "SELECT COUNT(*) AS n, SUM(COALESCE(total,0)) AS won_total "
        "FROM quotes WHERE created_at >= ? AND status = 'won' "
        "AND (is_test IS NULL OR is_test = 0)",
        (since,),
    ) or {"n": 0, "won_total": 0}
    quote_sent = _safe_fetchone(
        "SELECT COUNT(*) AS n FROM quotes WHERE created_at >= ? "
        "AND (sent_at IS NOT NULL AND sent_at != '') "
        "AND (is_test IS NULL OR is_test = 0)",
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


def _build_oracle_calibration_card():
    """Is the Oracle feedback loop actually firing?

    Aggregates `oracle_calibration` across all category×agency rows so an
    operator can tell at a glance whether the 4 runtime trigger paths
    (PC/RFQ mark-won/lost, order-ship, SCPRS loss-poll) are landing real
    signal. Staleness fires after 14 days with no update — matches the
    cadence the weekly report narrator expects.

    Context: before the 2026-04-20 PRs (#277-#280), prod showed 0 wins /
    47 losses and the weekly narrator mislabelled it "aggressive markup
    reduction." The card surfaces the same honest state: if loss_on_price
    still dominates after the loop is wired, that's a supplier-cost /
    market-reality signal, not a narrator bug.
    """
    row = _safe_fetchone(
        """SELECT COUNT(*) AS rows_n,
                  COALESCE(SUM(sample_size), 0) AS samples,
                  COALESCE(SUM(win_count), 0) AS wins,
                  COALESCE(SUM(loss_on_price), 0) AS losses_price,
                  COALESCE(SUM(loss_on_other), 0) AS losses_other,
                  MAX(last_updated) AS last_updated,
                  COUNT(DISTINCT agency) AS agencies_n
             FROM oracle_calibration"""
    ) or {}

    rows_n = int(row.get("rows_n") or 0)
    wins = int(row.get("wins") or 0)
    losses_price = int(row.get("losses_price") or 0)
    losses_other = int(row.get("losses_other") or 0)
    losses_total = losses_price + losses_other
    samples = int(row.get("samples") or 0)
    last_updated = row.get("last_updated") or ""

    days_since_update = None
    is_stale = None
    if last_updated:
        try:
            dt = datetime.fromisoformat(last_updated[:19])
            days_since_update = (datetime.now() - dt).days
            is_stale = days_since_update > 14
        except (ValueError, TypeError):
            pass

    win_rate_pct = None
    if (wins + losses_total) > 0:
        win_rate_pct = round(100 * wins / (wins + losses_total), 1)

    if rows_n == 0:
        status = "no_data"
    elif is_stale:
        status = "stale"
    elif wins == 0 and losses_total > 0:
        status = "losses_only"
    else:
        status = "healthy"

    return {
        "rows": rows_n,
        "agencies": int(row.get("agencies_n") or 0),
        "samples": samples,
        "wins": wins,
        "losses_price": losses_price,
        "losses_other": losses_other,
        "losses_total": losses_total,
        "win_rate_pct": win_rate_pct,
        "last_updated": last_updated,
        "days_since_update": days_since_update,
        "is_stale": is_stale,
        "status": status,
    }


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


# ── Aggregated gate ─────────────────────────────────────────────────────

# BUILD-4: Single-field status so external monitors (Railway cron,
# uptime robot, Mike's own dashboard) can alert on "calibration went
# stale" without parsing every card. Ordered from best to worst: the
# overall gate is the worst of its inputs.
_GATE_SEVERITY = {
    "healthy": 0,
    "no_data": 1,
    "losses_only": 1,
    "stale": 2,
    "degraded": 3,
}


def _build_health_gate(oracle_calibration):
    """Aggregate card statuses into a single gate status.

    Right now the only input is oracle_calibration.status — the feedback
    loop is the most consequential degradation mode (a 14-day-stale
    calibration means every recommendation is pricing off old data).
    Structured so additional inputs (db_health, classifier_1d, etc.)
    can be folded in without changing the gate's contract.
    """
    reasons = []
    severity = 0
    worst_status = "healthy"

    oc_status = (oracle_calibration or {}).get("status", "healthy")
    oc_sev = _GATE_SEVERITY.get(oc_status, 0)
    if oc_sev > 0:
        reasons.append({
            "source": "oracle_calibration",
            "status": oc_status,
            "days_since_update": (oracle_calibration or {}).get("days_since_update"),
        })
    if oc_sev > severity:
        severity = oc_sev
        worst_status = oc_status

    return {
        "status": worst_status,
        "severity": severity,
        "healthy": severity == 0,
        "reasons": reasons,
    }


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

    oracle_cal = _build_oracle_calibration_card()
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
        "oracle_calibration": oracle_cal,
        "pc_rfq_link": _build_pc_rfq_link_health(),
        "email_poll": _build_email_poll_card(),
        "gmail_send": _build_gmail_send_card(),
        "recent_quotes_cost_source": _build_recent_quotes_cost_source_card(),
        "time_to_send_kpi": _build_time_to_send_kpi_card(),
        "gate": _build_health_gate(oracle_cal),
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
    oracle_cal = _build_oracle_calibration_card()
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
        "oracle_calibration": oracle_cal,
        "registry_health": _build_registry_health(),
        "cert_health": _build_cert_health(),
        "bid_memory_health": _build_bid_memory_health(),
        "email_poll": _build_email_poll_card(),
        "gmail_send": _build_gmail_send_card(),
        "recent_quotes_cost_source": _build_recent_quotes_cost_source_card(),
        "time_to_send_kpi": _build_time_to_send_kpi_card(),
        "gate": _build_health_gate(oracle_cal),
    }


def _build_bid_memory_health():
    """V2-PR-6: bid_memory maintenance counter for /health/quoting.

    Returns total + by-outcome counts. Schema-tolerant.
    """
    result = {"ok": True, "total": 0, "by_outcome": {}}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT outcome, COUNT(*) AS n
                FROM bid_memory
                WHERE is_test = 0
                GROUP BY outcome
            """).fetchall()
            for r in rows:
                outcome = r["outcome"] if hasattr(r, "__getitem__") else r[0]
                n = r["n"] if hasattr(r, "__getitem__") else r[1]
                result["by_outcome"][outcome or "unknown"] = n
                result["total"] += n
    except Exception as e:
        log.debug("bid_memory_health suppressed: %s", e)
        result["ok"] = False
        result["error"] = str(e)
    return result


def _build_cert_health():
    """V2-PR-4: Reytech cert expiry visibility for /health/quoting.

    Returns total + by-status counts so Mike can see at a glance whether
    any active cert is past expiry or in the 60-day renewal window.
    Schema-tolerant — fresh DB without migration 26 returns ok=True with
    empty counts.
    """
    result = {"ok": True, "total": 0, "expired": 0, "expiring_soon": 0,
              "by_type": {}}
    try:
        from datetime import date
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT cert_type, expires_at FROM reytech_certifications
                WHERE is_active = 1 AND is_test = 0
            """).fetchall()
            today = date.today()
            for r in rows:
                ct = r["cert_type"] if hasattr(r, "__getitem__") else r[0]
                ex_raw = r["expires_at"] if hasattr(r, "__getitem__") else r[1]
                state = "unknown"
                if ex_raw:
                    try:
                        ex = date.fromisoformat(ex_raw[:10])
                        days = (ex - today).days
                        if days < 0:
                            state = "expired"
                            result["expired"] += 1
                        elif days <= 60:
                            state = "expiring_soon"
                            result["expiring_soon"] += 1
                        else:
                            state = "ok"
                    except (ValueError, TypeError):
                        pass
                result["by_type"][ct] = state
                result["total"] += 1
    except Exception as e:
        log.debug("cert_health suppressed: %s", e)
        result["ok"] = False
        result["error"] = str(e)
    return result


def _build_registry_health():
    """V2-PR-2: operator-visibility into agency_vendor_registry maintenance.

    Returns registry_rows_by_status — so Mike can tell at a glance
    whether the signal is actually being populated. Missing table (not
    yet migrated) returns ok=True with empty counts, no crash.
    """
    result = {"ok": True, "total": 0, "by_status": {}}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT status, COUNT(*) AS n
                FROM agency_vendor_registry
                WHERE is_test = 0
                GROUP BY status
            """).fetchall()
            for r in rows:
                status = r["status"] if hasattr(r, "__getitem__") else r[0]
                n = r["n"] if hasattr(r, "__getitem__") else r[1]
                result["by_status"][status] = n
                result["total"] += n
    except Exception as e:
        log.debug("registry_health suppressed: %s", e)
        result["ok"] = False
        result["error"] = str(e)
    return result


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
    from src.core.paths import DATA_DIR as data_dir
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


def _format_lag(seconds):
    """Render an integer-second lag as 'Ns ago' / 'Nm ago' / 'Nh ago' /
    'Nd ago'. Returns '—' for None so the template never renders 'None'.
    Used by `_build_email_poll_card`; broken out so tests can pin the
    exact thresholds."""
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def _build_email_poll_card(poll_status=None):
    """Email poller lag — front-of-funnel KPI signal (Plan §4.3).

    If polling is stalled the operator never sees the next RFQ, so
    time-to-send blows out and the §4.1 KPI degrades. This card surfaces
    the same `POLL_STATUS` dict that `/api/poll-now` returns, but in a
    passive read so the dashboard can render without triggering a fresh
    poll cycle.

    Status semantics (matched against the ~60s default poll cadence):
      • `error`    — last cycle returned an exception (red)
      • `paused`   — operator-paused via /api/email/pause (amber)
      • `stale`    — no successful check in >15 minutes (red)
      • `warn`     — no successful check in 5–15 minutes (amber)
      • `healthy`  — last check within 5 minutes (green)
      • `unknown`  — POLL_STATUS unavailable or never populated (grey)

    `poll_status` is injectable for tests. At runtime callers pass nothing
    and the fallback imports it from `src.api.dashboard` (the exec()-into-
    dashboard module-loading pattern means it lives there at runtime).
    """
    if poll_status is None:
        try:
            from src.api import dashboard as _dash
            poll_status = getattr(_dash, "POLL_STATUS", None)
        except Exception as e:
            log.debug("POLL_STATUS lookup failed: %s", e)
            poll_status = None
    if not isinstance(poll_status, dict):
        return {
            "status": "unknown", "running": None, "paused": None,
            "last_check_at": "", "lag_seconds": None, "lag_human": "—",
            "error": "", "emails_found_lifetime": 0,
        }

    last_check = (poll_status.get("last_check") or "").strip()
    paused = bool(poll_status.get("paused"))
    running = bool(poll_status.get("running"))
    error = (poll_status.get("error") or "")
    emails_found = int(poll_status.get("emails_found") or 0)

    lag_seconds = None
    if last_check:
        try:
            # POLL_STATUS["last_check"] is now written by `_pst_now_iso()` which
            # produces a TZ-aware ISO ("…-07:00"). Server runs UTC on Railway,
            # so a naive comparison (fromisoformat(last_check[:19]) → naive PST
            # vs datetime.now() → naive UTC) over-reports lag by 7-8 hours.
            # Parse the full ISO so the TZ is preserved, then compare in UTC.
            from datetime import timezone as _tz
            dt = datetime.fromisoformat(last_check)
            now = datetime.now(_tz.utc) if dt.tzinfo else datetime.now()
            lag_seconds = max(0, int((now - dt).total_seconds()))
        except (ValueError, TypeError):
            pass

    # Status priority: error > paused > stale > warn > healthy > unknown.
    # `error` wins even if poll later succeeded once — operator should
    # see the recent failure surfaced. Cleared on next clean cycle.
    if error:
        status = "error"
    elif paused:
        status = "paused"
    elif lag_seconds is None:
        status = "unknown"
    elif lag_seconds > 900:
        status = "stale"
    elif lag_seconds > 300:
        status = "warn"
    else:
        status = "healthy"

    return {
        "status": status,
        "running": running,
        "paused": paused,
        "last_check_at": last_check,
        "lag_seconds": lag_seconds,
        "lag_human": _format_lag(lag_seconds),
        "error": (error or "")[:200],
        "emails_found_lifetime": emails_found,
    }


def _build_gmail_send_card():
    """Gmail outbound-send health — companion to the inbound poll card
    (Plan §4.3 sub-2).

    The poll card surfaces "are we receiving RFQs"; this card surfaces
    "are we delivering quotes". Both gaps degrade the §4.1 KPI in
    different directions. If OAuth lapses, quota trips, or the SMTP
    fallback misconfigures, sends start failing silently — drafts pile
    up in /outbox while no buyer ever sees a quote, and the operator
    has no signal until a buyer pings them.

    Reads `email_outbox` directly. Status semantics:
      • `error`    — ≥1 send failed in last 24h (`failed` /
                     `permanently_failed`) — red
      • `stale`    — last successful send >7d ago — red, since Reytech's
                     baseline cadence is multi-quote/day during business
                     weeks
      • `warn`     — last successful send 24h-7d ago — amber, quiet but
                     not broken
      • `healthy`  — sent within 24h with zero failures in 24h — green
      • `unknown`  — `email_outbox` unreadable / never had a sent row —
                     grey (fresh-boot or schema-missing scenario)

    The 24h failure check uses `created_at` (when the row was queued)
    rather than `sent_at` (which only fills on success), so a recent
    failure is visible whether the row was ever resent or not.
    """
    result = {
        "status": "unknown",
        "last_send_at": "",
        "lag_seconds": None,
        "lag_human": "—",
        "sent_24h": 0,
        "sent_7d": 0,
        "failed_24h": 0,
        "pending_drafts": 0,
        "last_error": "",
    }
    try:
        with get_db() as conn:
            agg = conn.execute("""
                SELECT
                    MAX(CASE WHEN status='sent' THEN sent_at END) AS last_sent_at,
                    SUM(CASE WHEN status='sent'
                              AND sent_at IS NOT NULL AND sent_at != ''
                              AND datetime(sent_at) >= datetime('now','-1 day')
                             THEN 1 ELSE 0 END) AS sent_24h,
                    SUM(CASE WHEN status='sent'
                              AND sent_at IS NOT NULL AND sent_at != ''
                              AND datetime(sent_at) >= datetime('now','-7 days')
                             THEN 1 ELSE 0 END) AS sent_7d,
                    SUM(CASE WHEN status IN ('failed','permanently_failed')
                              AND created_at IS NOT NULL AND created_at != ''
                              AND datetime(created_at) >= datetime('now','-1 day')
                             THEN 1 ELSE 0 END) AS failed_24h,
                    SUM(CASE WHEN status IN ('draft','cs_draft','outreach_draft',
                                             'follow_up_draft','queued','approved')
                             THEN 1 ELSE 0 END) AS pending_drafts
                FROM email_outbox
            """).fetchone()
    except Exception as e:
        log.debug("gmail_send_card aggregate read failed: %s", e)
        return result
    if not agg:
        return result

    last_sent = (agg["last_sent_at"] or "").strip()
    result["last_send_at"] = last_sent
    result["sent_24h"] = int(agg["sent_24h"] or 0)
    result["sent_7d"] = int(agg["sent_7d"] or 0)
    result["failed_24h"] = int(agg["failed_24h"] or 0)
    result["pending_drafts"] = int(agg["pending_drafts"] or 0)

    if last_sent:
        try:
            from datetime import timezone as _tz
            dt = datetime.fromisoformat(last_sent)
            now = datetime.now(_tz.utc) if dt.tzinfo else datetime.now()
            result["lag_seconds"] = max(0, int((now - dt).total_seconds()))
            result["lag_human"] = _format_lag(result["lag_seconds"])
        except (ValueError, TypeError):
            pass

    if result["failed_24h"] > 0:
        try:
            with get_db() as conn:
                row = conn.execute("""
                    SELECT last_error FROM email_outbox
                    WHERE status IN ('failed','permanently_failed')
                      AND created_at IS NOT NULL AND created_at != ''
                      AND datetime(created_at) >= datetime('now','-1 day')
                    ORDER BY created_at DESC LIMIT 1
                """).fetchone()
            if row and row["last_error"]:
                result["last_error"] = str(row["last_error"])[:200]
        except Exception as e:
            log.debug("gmail_send_card last_error read failed: %s", e)

    if result["failed_24h"] > 0:
        result["status"] = "error"
    elif result["lag_seconds"] is None:
        result["status"] = "unknown"
    elif result["lag_seconds"] > 7 * 86400:
        result["status"] = "stale"
    elif result["lag_seconds"] > 86400:
        result["status"] = "warn"
    else:
        result["status"] = "healthy"

    return result


def _build_time_to_send_kpi_card():
    """Headline §4.1 KPI on /health/quoting: time-to-send distribution.

    The plan's KPI is "1 quote sent in <90 seconds." PR #608 wired the
    `operator_quote_sent` telemetry table; PR #622 closed the gap that
    kept the table empty for two RFQ-send paths. This card renders the
    median, p95, count, and %-under-90s for two windows (24h + 7d) so
    the operator can see whether the KPI is being met today AND
    whether the trend is healthy.

    Status semantics (priority error > warn > healthy > unknown):
      • `healthy`  — under_90_pct >= 60% in 7d window AND any rows in 24h
      • `warn`     — under_90_pct 30-60% (KPI degrading)
      • `error`    — under_90_pct < 30% in 7d window (KPI broken)
      • `unknown`  — no rows in 7d window (no signal yet)

    Reads via `operator_kpi.get_kpi_stats` so the same logic that the
    /analytics page uses drives this card. No duplication.
    """
    result = {
        "status": "unknown",
        "kpi_target_pct": 60,  # we want >= 60% of quotes sent <90s
        "window_24h": {"count": 0, "median_seconds": None,
                       "p95_seconds": None, "under_90_pct": None,
                       "under_90_count": 0},
        "window_7d":  {"count": 0, "median_seconds": None,
                       "p95_seconds": None, "under_90_pct": None,
                       "under_90_count": 0},
    }
    try:
        from src.core.operator_kpi import get_kpi_stats
        s24 = get_kpi_stats(window_days=1)
        s7  = get_kpi_stats(window_days=7)
    except Exception as e:
        log.debug("time_to_send_kpi: get_kpi_stats failed: %s", e)
        return result

    if not (s24 or {}).get("ok") or not (s7 or {}).get("ok"):
        return result

    for src, dst_key in ((s24, "window_24h"), (s7, "window_7d")):
        result[dst_key] = {
            "count":          int(src.get("count") or 0),
            "median_seconds": src.get("median_seconds"),
            "p95_seconds":    src.get("p95_seconds"),
            "under_90_pct":   src.get("under_90_pct"),
            "under_90_count": int(src.get("under_90_count") or 0),
        }

    pct_7d = result["window_7d"]["under_90_pct"]
    if result["window_7d"]["count"] == 0:
        result["status"] = "unknown"
    elif pct_7d is None:
        result["status"] = "unknown"
    elif pct_7d < 30:
        result["status"] = "error"
    elif pct_7d < 60:
        result["status"] = "warn"
    else:
        result["status"] = "healthy"

    return result


# Cost-source categories surfaced on the recent-quotes card. The keys are the
# canonical buckets the pricing pipeline writes; everything else falls into
# "unknown" so a new value never silently disappears from the UI.
_COST_SOURCE_BUCKETS = {
    "operator":         "operator",
    "catalog":          "catalog",
    "catalog_confirmed":"catalog",
    "amazon":           "amazon",
    "amazon_scrape":    "amazon",
    "scprs":            "scprs",
    "scprs_scrape":     "scprs",
    "needs_lookup":     "needs_lookup",
    "legacy_unknown":   "needs_lookup",
}


def _bucket_cost_source(raw):
    """Map a raw `pricing.cost_source` string onto one of the 5 canonical
    buckets used by the chips card. Unknown values land in 'unknown' so a
    new pipeline value (e.g. a new scraper) shows up as a grey chip rather
    than silently disappearing."""
    if not raw:
        return "needs_lookup"
    return _COST_SOURCE_BUCKETS.get(str(raw).lower().strip(), "unknown")


def _build_recent_quotes_cost_source_card(limit: int = 5):
    """Last-N quotes with a per-quote cost_source mix (Plan §4.3 sub-3).

    Pricing pipeline health = which path produced each item's cost.
    Healthy mix is operator/catalog dominant — those are the rows that
    fund the flywheel and stay accurate. Amazon/SCPRS dominance means
    we're quoting from reference ceilings, not real costs. needs_lookup
    means we shipped a quote with a gap.

    The card joins `operator_quote_sent` (single source of truth for
    "which quotes did the operator actually send") to the source PC/RFQ
    items[].pricing.cost_source so the operator sees the same signal at
    a glance — without having to open each PC.

    Returns:
      {
        "ok": True/False,
        "quotes": [
          {
            "quote_id", "quote_type", "sent_at", "lag_human",
            "item_count", "agency_key", "quote_total",
            "chips": {operator, catalog, amazon, scprs, needs_lookup, unknown},
            "missing_source": True if items couldn't be loaded,
          }, ...
        ],
        "totals": {bucket: total_count_across_all_quotes},
      }
    """
    result = {"ok": True, "quotes": [], "totals": {
        "operator": 0, "catalog": 0, "amazon": 0,
        "scprs": 0, "needs_lookup": 0, "unknown": 0,
    }}
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_id, quote_type, sent_at, item_count,
                       agency_key, quote_total
                FROM operator_quote_sent
                ORDER BY sent_at DESC
                LIMIT ?
            """, (int(limit),)).fetchall()
    except Exception as e:
        log.debug("recent_quotes_cost_source aggregate read failed: %s", e)
        return {"ok": False, "error": str(e), "quotes": [], "totals": result["totals"]}

    if not rows:
        return result

    # Avoid importing dal at module load (circular-ish via routes registration).
    from src.core.dal import get_pc, get_rfq

    for r in rows:
        quote_id = r["quote_id"]
        quote_type = (r["quote_type"] or "pc").lower()
        sent_at = (r["sent_at"] or "").strip()

        chips = {"operator": 0, "catalog": 0, "amazon": 0,
                 "scprs": 0, "needs_lookup": 0, "unknown": 0}
        missing_source = False
        try:
            src = get_pc(quote_id) if quote_type == "pc" else get_rfq(quote_id)
        except Exception as e:
            log.debug("recent_quotes_cost_source: load %s/%s failed: %s",
                      quote_type, quote_id, e)
            src = None
        if not isinstance(src, dict):
            missing_source = True
        else:
            items = src.get("items") or []
            if not isinstance(items, list):
                items = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                pricing = item.get("pricing") or {}
                if not isinstance(pricing, dict):
                    pricing = {}
                bucket = _bucket_cost_source(pricing.get("cost_source"))
                chips[bucket] = chips.get(bucket, 0) + 1
                result["totals"][bucket] = result["totals"].get(bucket, 0) + 1

        # Lag against now (TZ-aware fix from PR #617).
        lag_seconds = None
        lag_human = "—"
        if sent_at:
            try:
                from datetime import timezone as _tz
                dt = datetime.fromisoformat(sent_at)
                now = datetime.now(_tz.utc) if dt.tzinfo else datetime.now()
                lag_seconds = max(0, int((now - dt).total_seconds()))
                lag_human = _format_lag(lag_seconds)
            except (ValueError, TypeError):
                pass

        result["quotes"].append({
            "quote_id": quote_id,
            "quote_type": quote_type,
            "sent_at": sent_at,
            "lag_seconds": lag_seconds,
            "lag_human": lag_human,
            "item_count": int(r["item_count"] or 0),
            "agency_key": r["agency_key"] or "",
            "quote_total": float(r["quote_total"] or 0),
            "chips": chips,
            "missing_source": missing_source,
        })

    return result


@bp.route("/api/health/db-bloat")
@auth_required
def db_bloat_json():
    """Per-table row counts + approximate bytes. Diagnostic for sizing
    the 500MB+ reytech.db bloat: which tables grew unboundedly, and
    which rows are candidates for trim.

    Uses the dbstat virtual table if compiled in (SQLite ≥ 3.36 with
    SQLITE_ENABLE_DBSTAT_VTAB). Falls back to a count+avg_row_estimate
    which is rough but ranks tables correctly."""
    import os as _os
    from src.core.paths import DATA_DIR as data_dir

    db_path = _os.path.join(data_dir, "reytech.db")
    result = {
        "ok": True,
        "db_size_mb": 0,
        "page_size": 0,
        "dbstat_available": False,
        "tables": [],
    }

    if not _os.path.exists(db_path):
        result["ok"] = False
        result["error"] = "reytech.db not found"
        return result

    result["db_size_mb"] = round(_os.path.getsize(db_path) / 1024 / 1024, 2)

    try:
        with get_db() as conn:
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            result["page_size"] = page_size

            # List all user tables.
            rows = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name"
            ).fetchall()
            names = [r[0] for r in rows]

            # Try dbstat — gives actual on-disk bytes per table.
            dbstat_sizes = {}
            try:
                ds_rows = conn.execute(
                    "SELECT name, SUM(pgsize) AS bytes, SUM(payload) AS payload "
                    "FROM dbstat GROUP BY name"
                ).fetchall()
                for n, b, p in ds_rows:
                    dbstat_sizes[n] = {"bytes": b or 0, "payload": p or 0}
                result["dbstat_available"] = True
            except Exception as _e:
                log.debug("dbstat unavailable: %s", _e)

            out = []
            for t in names:
                try:
                    cnt = conn.execute(
                        f"SELECT COUNT(*) FROM {t}"
                    ).fetchone()[0]
                except Exception:
                    cnt = -1
                entry = {"table": t, "row_count": cnt}
                if t in dbstat_sizes:
                    entry["bytes"] = dbstat_sizes[t]["bytes"]
                    entry["mb"] = round(
                        dbstat_sizes[t]["bytes"] / 1024 / 1024, 2
                    )
                    entry["payload_bytes"] = dbstat_sizes[t]["payload"]
                out.append(entry)

            # Rank by mb if available, otherwise by row_count.
            if result["dbstat_available"]:
                out.sort(key=lambda r: r.get("mb", 0), reverse=True)
            else:
                out.sort(key=lambda r: r["row_count"], reverse=True)
            result["tables"] = out

            # rfq_files deep-dive — this table dominates DB size (PDF blobs).
            # Break it down by (category, file_type) and surface orphan files
            # pointing at missing/dismissed RFQs so we can pick a trim policy
            # without blindly deleting. Each diagnostic is independently
            # guarded so a single missing table doesn't suppress the others.
            result["rfq_files_breakdown"] = []
            result["rfq_files_orphans"] = {"count": 0, "mb": 0}
            result["rfq_files_dead_parents"] = {"count": 0, "mb": 0}
            result["rfq_files_size_histogram"] = {
                "lt_100kb": 0, "100kb_1mb": 0, "1mb_5mb": 0,
                "gt_5mb": 0, "biggest_bytes": 0,
            }
            try:
                rows = conn.execute("""
                    SELECT category, file_type,
                           COUNT(*) AS n,
                           SUM(file_size) AS bytes,
                           MIN(created_at) AS oldest,
                           MAX(created_at) AS newest
                    FROM rfq_files GROUP BY category, file_type
                    ORDER BY bytes DESC
                """).fetchall()
                result["rfq_files_breakdown"] = [
                    {
                        "category": r[0] or "(null)",
                        "file_type": r[1] or "(null)",
                        "count": r[2],
                        "mb": round((r[3] or 0) / 1024 / 1024, 2),
                        "oldest": r[4],
                        "newest": r[5],
                    }
                    for r in rows
                ]
            except Exception as _e:
                log.debug("rfq_files breakdown skipped: %s", _e)
            try:
                # Orphan detection: files whose rfq_id has no matching row
                # in rfqs AND no matching row in price_checks.
                orphans = conn.execute("""
                    SELECT COUNT(*) AS n, COALESCE(SUM(file_size), 0) AS bytes
                    FROM rfq_files rf
                    WHERE NOT EXISTS (SELECT 1 FROM rfqs r WHERE r.id = rf.rfq_id)
                      AND NOT EXISTS (SELECT 1 FROM price_checks p WHERE p.id = rf.rfq_id)
                """).fetchone()
                result["rfq_files_orphans"] = {
                    "count": orphans[0] or 0,
                    "mb": round((orphans[1] or 0) / 1024 / 1024, 2),
                }
            except Exception as _e:
                log.debug("rfq_files orphans skipped: %s", _e)
            try:
                # Dead-parent files: rfq_id points at an RFQ/PC whose status
                # is terminal. Retention-trim candidates — no longer
                # reachable through the UI but still cost storage.
                dead = conn.execute("""
                    SELECT COUNT(*) AS n, COALESCE(SUM(rf.file_size), 0) AS bytes
                    FROM rfq_files rf
                    LEFT JOIN rfqs r ON r.id = rf.rfq_id
                    LEFT JOIN price_checks p ON p.id = rf.rfq_id
                    WHERE COALESCE(r.status, p.status) IN
                          ('dismissed','archived','lost','cancelled','deleted','expired')
                """).fetchone()
                result["rfq_files_dead_parents"] = {
                    "count": dead[0] or 0,
                    "mb": round((dead[1] or 0) / 1024 / 1024, 2),
                }
            except Exception as _e:
                log.debug("rfq_files dead_parents skipped: %s", _e)
            try:
                hist = conn.execute("""
                    SELECT
                      SUM(CASE WHEN file_size < 100000 THEN 1 ELSE 0 END) AS lt100k,
                      SUM(CASE WHEN file_size BETWEEN 100000 AND 1000000 THEN 1 ELSE 0 END) AS k100_1m,
                      SUM(CASE WHEN file_size BETWEEN 1000000 AND 5000000 THEN 1 ELSE 0 END) AS m1_5,
                      SUM(CASE WHEN file_size > 5000000 THEN 1 ELSE 0 END) AS gt5m,
                      MAX(file_size) AS biggest
                    FROM rfq_files
                """).fetchone()
                result["rfq_files_size_histogram"] = {
                    "lt_100kb": hist[0] or 0,
                    "100kb_1mb": hist[1] or 0,
                    "1mb_5mb": hist[2] or 0,
                    "gt_5mb": hist[3] or 0,
                    "biggest_bytes": hist[4] or 0,
                }
            except Exception as _e:
                log.debug("rfq_files size_histogram skipped: %s", _e)

    except Exception as e:
        log.warning("/api/health/db-bloat failed: %s", e)
        result["ok"] = False
        result["error"] = str(e)

    return result


# ─────────────────────────────────────────────────────────────────────
# rfq_files trim — destructive endpoint behind dry_run + confirm=YES
# ─────────────────────────────────────────────────────────────────────

_TRIM_ORPHANS_SQL = """
    SELECT rf.id, rf.rfq_id, rf.filename, rf.file_size, rf.category,
           rf.file_type, rf.created_at
      FROM rfq_files rf
     WHERE NOT EXISTS (SELECT 1 FROM rfqs r WHERE r.id = rf.rfq_id)
       AND NOT EXISTS (SELECT 1 FROM price_checks p WHERE p.id = rf.rfq_id)
"""

_TRIM_DEAD_PARENTS_SQL = """
    SELECT rf.id, rf.rfq_id, rf.filename, rf.file_size, rf.category,
           rf.file_type, rf.created_at
      FROM rfq_files rf
      LEFT JOIN rfqs r ON r.id = rf.rfq_id
      LEFT JOIN price_checks p ON p.id = rf.rfq_id
     WHERE COALESCE(r.status, p.status) IN
           ('dismissed','archived','lost','cancelled','deleted','expired')
"""


@bp.route("/api/admin/trim-rfq-files", methods=["POST"])
@auth_required
@rate_limit("heavy")
def trim_rfq_files():
    """Reclaim space by deleting unreachable rfq_files rows.

    Modes:
        orphans       — rfq_id absent from both rfqs and price_checks
        dead_parents  — parent status in dismissed/archived/lost/cancelled/
                        deleted/expired
        both          — union of the above (de-duplicated by file id)

    Query params:
        mode     — one of the above (default: orphans)
        dry_run  — "1" (default) reports what *would* be deleted; "0"
                   performs the delete. dry_run=0 requires confirm=YES.
        confirm  — must equal "YES" to actually delete (case-sensitive).
        vacuum   — "1" (default when dry_run=0) runs VACUUM after the
                   delete to release pages back to the OS; "0" skips it.

    Returns:
        {
          "ok": bool,
          "mode": "orphans"|"dead_parents"|"both",
          "dry_run": bool,
          "matched": {"count": int, "mb": float},
          "deleted": {"count": int, "mb": float},   # 0/0 on dry_run
          "vacuum":  {"ran": bool, "before_mb": float,
                      "after_mb": float, "reclaimed_mb": float},
          "sample": [ up to 10 matched rows for eyeballing ]
        }
    """
    mode = (request.args.get("mode") or "orphans").strip().lower()
    if mode not in ("orphans", "dead_parents", "both"):
        return {"ok": False, "error": f"invalid mode: {mode}"}, 400

    dry_run = (request.args.get("dry_run", "1") != "0")
    confirm = request.args.get("confirm", "")
    if not dry_run and confirm != "YES":
        return {
            "ok": False,
            "error": "destructive op requires confirm=YES",
            "hint": "re-issue with &confirm=YES after reviewing dry-run",
        }, 400

    want_vacuum = (request.args.get("vacuum", "1") != "0") and not dry_run

    # SY-3: only one trim-rfq-files execution in flight at a time. Second
    # concurrent call gets 409 instead of queueing behind the VACUUM lock.
    # Dry runs are cheap and don't need the single-flight guard.
    if not dry_run and not _TRIM_RFQ_FILES_LOCK.acquire(blocking=False):
        return {
            "ok": False,
            "error": "trim-rfq-files already running",
            "hint": "wait for the first call to finish (VACUUM can take 30-60s on a large DB)",
        }, 409

    import os as _os
    from src.core.paths import DATA_DIR as data_dir
    db_path = _os.path.join(data_dir, "reytech.db")
    size_before = (_os.path.getsize(db_path) / 1024 / 1024) \
        if _os.path.exists(db_path) else 0

    # SY-3: audit row at start — tie together the before/after snapshots
    # and capture the caller IP/UA for any destructive run.
    if not dry_run:
        _log_audit_internal(
            "trim_rfq_files_start",
            f"mode={mode} vacuum={want_vacuum} size_before_mb={size_before:.2f}",
            metadata={"mode": mode, "vacuum": want_vacuum,
                      "size_before_mb": round(size_before, 2)},
        )

    out = {
        "ok": True,
        "mode": mode,
        "dry_run": dry_run,
        "matched": {"count": 0, "mb": 0.0},
        "deleted": {"count": 0, "mb": 0.0},
        "vacuum": {"ran": False, "before_mb": round(size_before, 2),
                   "after_mb": round(size_before, 2), "reclaimed_mb": 0.0},
        "sample": [],
    }

    try:
        # Gather ids, dedupe across modes.
        matched_rows = {}  # id -> row tuple
        try:
            with get_db() as conn:
                if mode in ("orphans", "both"):
                    for r in conn.execute(_TRIM_ORPHANS_SQL).fetchall():
                        matched_rows[r[0]] = r
                if mode in ("dead_parents", "both"):
                    for r in conn.execute(_TRIM_DEAD_PARENTS_SQL).fetchall():
                        matched_rows.setdefault(r[0], r)

                total_bytes = sum((r[3] or 0) for r in matched_rows.values())
                out["matched"]["count"] = len(matched_rows)
                out["matched"]["mb"] = round(total_bytes / 1024 / 1024, 2)
                out["sample"] = [
                    {
                        "id": r[0], "rfq_id": r[1], "filename": r[2],
                        "bytes": r[3], "category": r[4], "file_type": r[5],
                        "created_at": r[6],
                    }
                    for r in list(matched_rows.values())[:10]
                ]

                if not dry_run and matched_rows:
                    ids = list(matched_rows.keys())
                    # Chunk the DELETE so SQLite's 999-param limit doesn't bite.
                    CHUNK = 500
                    deleted = 0
                    for i in range(0, len(ids), CHUNK):
                        chunk = ids[i:i + CHUNK]
                        placeholders = ",".join("?" * len(chunk))
                        cur = conn.execute(
                            f"DELETE FROM rfq_files WHERE id IN ({placeholders})",
                            chunk,
                        )
                        deleted += cur.rowcount or 0
                    conn.commit()
                    out["deleted"]["count"] = deleted
                    out["deleted"]["mb"] = round(total_bytes / 1024 / 1024, 2)
                    log.warning(
                        "rfq_files trim (mode=%s): deleted %d rows, ~%.2f MB",
                        mode, deleted, total_bytes / 1024 / 1024,
                    )
        except Exception as e:
            log.warning("trim-rfq-files failed: %s", e, exc_info=True)
            if not dry_run:
                _log_audit_internal(
                    "trim_rfq_files_error",
                    f"mode={mode} error={e}",
                    metadata={"mode": mode, "error": str(e)[:500]},
                )
            return {"ok": False, "error": str(e), "partial": out}, 500

        if want_vacuum and out["deleted"]["count"] > 0:
            try:
                # VACUUM must run outside a transaction and without other
                # writers. Use a dedicated connection that auto-commits.
                import sqlite3
                vc = sqlite3.connect(db_path, timeout=60, isolation_level=None)
                vc.execute("VACUUM")
                vc.close()
                size_after = _os.path.getsize(db_path) / 1024 / 1024
                out["vacuum"]["ran"] = True
                out["vacuum"]["after_mb"] = round(size_after, 2)
                out["vacuum"]["reclaimed_mb"] = round(size_before - size_after, 2)
                log.warning(
                    "rfq_files trim VACUUM: %.2f MB → %.2f MB (reclaimed %.2f MB)",
                    size_before, size_after, size_before - size_after,
                )
            except Exception as e:
                log.warning("VACUUM after trim failed: %s", e)
                out["vacuum"]["error"] = str(e)

        # SY-3: audit row at end — records what actually shipped so ops can
        # reconcile "who ran this, when, and how much did it reclaim".
        if not dry_run:
            _log_audit_internal(
                "trim_rfq_files_done",
                f"mode={mode} deleted={out['deleted']['count']} "
                f"reclaimed_mb={out['vacuum']['reclaimed_mb']}",
                metadata={
                    "mode": mode,
                    "deleted_count": out["deleted"]["count"],
                    "deleted_mb": out["deleted"]["mb"],
                    "vacuum_ran": out["vacuum"]["ran"],
                    "reclaimed_mb": out["vacuum"]["reclaimed_mb"],
                    "after_mb": out["vacuum"]["after_mb"],
                },
            )

        return out
    finally:
        if not dry_run:
            try:
                _TRIM_RFQ_FILES_LOCK.release()
            except RuntimeError:
                pass


@bp.route("/api/health/quote-errors")
@auth_required
def quote_errors_json():
    """Failed quote/package generations in the last `hours` (default 24).

    Backed by `utilization_events`: every `Trace(...).fail(...)` call in
    the quoting routes persists a row with `feature='trace.<workflow>'`
    and `ok=0`. This endpoint filters those rows to the quote-relevant
    workflows so ops can see what's crashing without tailing logs.

    Query params:
        hours  — lookback window (default 24, clamped [1, 720])
        limit  — max rows returned (default 200, clamped [1, 1000])

    Surfaces:
        - summary: {total_attempts, failures, failure_rate}
        - by_workflow: failure count per workflow (rfq_package, etc.)
        - recent: last N failures with trace_id / rfq_id / pc_id / error
    """
    try:
        hours = int(request.args.get("hours", 24))
    except (TypeError, ValueError):
        hours = 24
    hours = max(1, min(hours, 720))

    try:
        limit = int(request.args.get("limit", 200))
    except (TypeError, ValueError):
        limit = 200
    limit = max(1, min(limit, 1000))

    since = (datetime.now() - timedelta(hours=hours)).isoformat()

    # Only workflows that represent quote/package generation. Other
    # traced flows (classifier, email pipeline) live elsewhere in the
    # dashboard and would pollute the signal here.
    quote_prefixes = (
        "trace.rfq_package",
        "trace.quote_generation",
        "trace.pc_quote",
        "trace.rfq_quote",
    )
    like_clauses = " OR ".join(["feature LIKE ?"] * len(quote_prefixes))
    like_params = [f"{p}%" for p in quote_prefixes]

    totals_sql = (
        f"SELECT COUNT(*) AS n, "
        f"SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS fails "
        f"FROM utilization_events "
        f"WHERE created_at >= ? AND ({like_clauses})"
    )
    by_wf_sql = (
        f"SELECT feature, "
        f"COUNT(*) AS attempts, "
        f"SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) AS failures, "
        f"AVG(duration_ms) AS avg_ms "
        f"FROM utilization_events "
        f"WHERE created_at >= ? AND ({like_clauses}) "
        f"GROUP BY feature ORDER BY failures DESC, attempts DESC"
    )
    recent_sql = (
        f"SELECT feature, context, duration_ms, created_at "
        f"FROM utilization_events "
        f"WHERE created_at >= ? AND ok=0 AND ({like_clauses}) "
        f"ORDER BY created_at DESC LIMIT ?"
    )

    params_base = [since, *like_params]

    tot_rows = _safe_fetchall(totals_sql, params_base)
    tot = tot_rows[0] if tot_rows else {"n": 0, "fails": 0}
    total = int(tot.get("n") or 0)
    fails = int(tot.get("fails") or 0)

    by_wf = _safe_fetchall(by_wf_sql, params_base)
    for row in by_wf:
        row["avg_ms"] = round(float(row.get("avg_ms") or 0), 1)

    recent_rows = _safe_fetchall(recent_sql, params_base + [limit])
    recent = []
    for r in recent_rows:
        ctx = {}
        try:
            ctx = json.loads(r.get("context") or "{}")
        except (TypeError, ValueError):
            ctx = {}
        recent.append({
            "feature": r.get("feature", ""),
            "created_at": r.get("created_at", ""),
            "duration_ms": r.get("duration_ms", 0),
            "trace_id": ctx.get("trace_id", ""),
            "rfq_id": ctx.get("rfq_id", ""),
            "pc_id": ctx.get("pc_id", ""),
            "error": ctx.get("error", ""),
            "status": ctx.get("status", ""),
        })

    return {
        "ok": True,
        "hours": hours,
        "summary": {
            "total_attempts": total,
            "failures": fails,
            "failure_rate": round(fails / total, 3) if total else 0.0,
        },
        "by_workflow": by_wf,
        "recent": recent,
    }


@bp.route("/api/health/profiles")
@auth_required
def profiles_json():
    """Registered form profile inventory + manifest drift status.

    Ops uses this to confirm that the running app's profile set matches the
    registry.yml manifest — a mismatch means a deploy bundled a profile YAML
    change without regenerating the manifest, which can mask a broken
    fingerprint from the pre-fill gate.

    Surfaces:
        profile_count: number of profiles loaded in-process.
        drift: per-profile (live vs manifest) fingerprint/field_count mismatches.
        profiles: list of {id, form_type, fill_mode, fingerprint, field_count,
                           blank_exists}.
    """
    import os as _os
    try:
        from src.forms.profile_registry import load_manifest, load_profiles
    except Exception as e:
        log.warning("profiles endpoint failed to import registry: %s", e)
        return {"ok": False, "error": f"registry_unavailable: {e}"}, 500

    profiles = load_profiles()
    manifest = load_manifest()

    drift = []
    for pid, p in sorted(profiles.items()):
        entry = manifest.get(pid)
        if not entry:
            drift.append({
                "profile_id": pid,
                "reason": "missing_from_manifest",
                "live_fingerprint": p.fingerprint,
                "manifest_fingerprint": None,
            })
            continue
        if p.fingerprint != entry.get("fingerprint", ""):
            drift.append({
                "profile_id": pid,
                "reason": "fingerprint_mismatch",
                "live_fingerprint": p.fingerprint,
                "manifest_fingerprint": entry.get("fingerprint", ""),
            })
        if len(p.fields) != entry.get("field_count"):
            drift.append({
                "profile_id": pid,
                "reason": "field_count_mismatch",
                "live_field_count": len(p.fields),
                "manifest_field_count": entry.get("field_count"),
            })
    # Manifest entries with no corresponding live profile.
    for mid in sorted(set(manifest.keys()) - set(profiles.keys())):
        drift.append({
            "profile_id": mid,
            "reason": "missing_from_runtime",
            "live_fingerprint": None,
            "manifest_fingerprint": manifest[mid].get("fingerprint", ""),
        })

    out_profiles = []
    for pid in sorted(profiles.keys()):
        p = profiles[pid]
        blank_exists = bool(p.blank_pdf) and _os.path.exists(p.blank_pdf)
        out_profiles.append({
            "id": p.id,
            "form_type": p.form_type,
            "fill_mode": p.fill_mode,
            "blank_pdf": p.blank_pdf or "",
            "blank_exists": blank_exists,
            "fingerprint": p.fingerprint or "",
            "field_count": len(p.fields),
        })

    return {
        "ok": True,
        "profile_count": len(profiles),
        "manifest_count": len(manifest),
        "drift": drift,
        "profiles": out_profiles,
    }


def _build_pc_rfq_link_health():
    """Builder for the CCHCS PC→RFQ handoff observability panel.

    Shared by `/api/health/pc-rfq-link` (JSON for ops scripts) and the
    tile on `/health/quoting` (operator-facing dashboard). Pure read,
    no side effects — safe to call on every page render.

    Returns the same shape as the JSON endpoint. Fields:
        links_24h: pc_rfq_linked activity events in last 24h.
        unlinks_24h: pc_rfq_unlinked activity events in last 24h. A
                     high unlink/link ratio flags a misfiring auto-
                     suggestion heuristic — ops watches this, not a
                     noisy per-event alert.
        reprices_24h: drifted lines repriced by oracle in last 24h
                      (summed from activity metadata — avoids re-scanning
                      RFQ items).
        skipped_no_price_24h: drifted lines where the oracle had no data
                              (operator follow-up required).
        cchcs_linked_total: CCHCS RFQs currently carrying `linked_pc_id`.
        cchcs_unlinked_total: CCHCS RFQs that could still be linked.
        unresolved_qty_drift: line_items across all RFQs where
                              qty_changed=True AND no reprice has landed.
                              The "needs manual pricing" backlog.
        recent_links: last 5 pc_rfq_linked entries (newest first).
    """
    out = {
        "links_24h": 0,
        "unlinks_24h": 0,
        "reprices_24h": 0,
        "skipped_no_price_24h": 0,
        "cchcs_linked_total": 0,
        "cchcs_unlinked_total": 0,
        "unresolved_qty_drift": 0,
        "recent_links": [],
    }

    cutoff = (datetime.now() - timedelta(days=1)).isoformat()

    # Activity-log scan: authoritative for "did the handoff actually run".
    try:
        from src.api.data_layer import _load_crm_activity
        activity = _load_crm_activity() or []
        link_events = [a for a in activity
                       if a.get("event_type") == "pc_rfq_linked"]
        recent_24h = [a for a in link_events
                      if (a.get("timestamp") or "") >= cutoff]
        out["links_24h"] = len(recent_24h)
        for a in recent_24h:
            meta = a.get("metadata") or {}
            rep = meta.get("reprice") or {}
            if isinstance(rep, dict):
                out["reprices_24h"] += int(rep.get("repriced") or 0)
                out["skipped_no_price_24h"] += int(rep.get("skipped_no_price") or 0)
        # Unlink churn: links later reversed signal bad auto-suggestions.
        # A high unlink/link ratio is the clearest ops signal that the
        # suggestion heuristic is misfiring.
        unlink_events_24h = [a for a in activity
                             if a.get("event_type") == "pc_rfq_unlinked"
                             and (a.get("timestamp") or "") >= cutoff]
        out["unlinks_24h"] = len(unlink_events_24h)
        # Last 5, newest first
        for a in sorted(link_events,
                        key=lambda x: x.get("timestamp") or "",
                        reverse=True)[:5]:
            out["recent_links"].append({
                "ref_id": a.get("ref_id", ""),
                "description": a.get("description", ""),
                "timestamp": a.get("timestamp", ""),
            })
    except Exception as e:
        log.debug("pc_rfq_link_health: activity scan failed: %s", e)

    # RFQ-level scan: link coverage + qty-drift backlog.
    try:
        from src.api.data_layer import load_rfqs
        rfqs = load_rfqs() or {}
        for _rid, r in rfqs.items():
            if not isinstance(r, dict):
                continue
            agency = (r.get("agency") or "").lower()
            inst = (r.get("institution") or r.get("department") or "").lower()
            is_cchcs = ("cchcs" in agency or "cchcs" in inst
                        or "correctional health" in inst)
            if is_cchcs:
                if r.get("linked_pc_id"):
                    out["cchcs_linked_total"] += 1
                else:
                    out["cchcs_unlinked_total"] += 1
            items = r.get("line_items") or r.get("items") or []
            for it in items:
                if not isinstance(it, dict):
                    continue
                if it.get("qty_changed") and it.get("repriced_reason") != "qty_change":
                    out["unresolved_qty_drift"] += 1
    except Exception as e:
        log.debug("pc_rfq_link_health: rfq scan failed: %s", e)

    return out


@bp.route("/api/health/pc-rfq-link")
@auth_required
def pc_rfq_link_health_json():
    """CCHCS PC→RFQ handoff observability. Ops reads this to confirm the
    one-engine chain is actually flowing in prod.

    Defensive: missing data returns zeros, not 500s. Ops dashboards
    should be able to hit this every minute without ever alerting on
    transient stat shape issues.
    """
    return {"ok": True, **_build_pc_rfq_link_health()}
