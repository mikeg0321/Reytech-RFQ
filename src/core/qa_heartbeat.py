"""QA/QC heartbeat — data freshness + pipeline-health checks.

Built 2026-05-13 after prod forensics surfaced silent data degradation
that nothing was watching:

  - `scprs_awards` table was 60 days stale (last write 2026-03-14)
  - `scheduler_heartbeats` table was empty (no jobs reporting)
  - 52% of April PCs were marked `duplicate` with NO audit trail
  - WR collapsed from 26% (Aug 2025) to 4.8% (2026 YTD) with no alarm

Per Mike: "maintain heartbeat to keep QA/QC ongoing." This module
runs the 7 checks listed in CHECKS below, writes each result to
`qa_heartbeat`, and surfaces fail/warn states for an admin route + an
optional alert hook.

Each check is a pure function: takes a DB connection + a context
dict, returns (status, value, threshold, message). No side effects
inside the check — writes happen in `run_all_checks`. Easy to extend:
add a new (name, fn) tuple to CHECKS and write a test pinning the
expected behavior.

Run modes:
  - `run_all_checks()` — one-shot, returns dict of results
  - `run_and_persist()` — runs + writes to qa_heartbeat
  - `latest_status()` — pulls the most recent row per check_name
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable

log = logging.getLogger("qa_heartbeat")


# ─── Thresholds ──────────────────────────────────────────────────────────────


# Conservative defaults. Override via env for tuning without redeploy.
def _t(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


# Hours since last record before a check goes warn / fail.
SCPRS_AWARDS_WARN_H = _t("QA_SCPRS_AWARDS_WARN_H", 72)      # 3 days
SCPRS_AWARDS_FAIL_H = _t("QA_SCPRS_AWARDS_FAIL_H", 168)     # 7 days
SCPRS_LINES_WARN_H  = _t("QA_SCPRS_LINES_WARN_H", 36)
SCPRS_LINES_FAIL_H  = _t("QA_SCPRS_LINES_FAIL_H", 72)
EMAIL_WARN_H        = _t("QA_EMAIL_WARN_H", 4)
EMAIL_FAIL_H        = _t("QA_EMAIL_FAIL_H", 12)
HB_JOBS_WARN_MIN    = _t("QA_HB_JOBS_WARN", 3)              # min distinct jobs heartbeating in last 6h
HB_JOBS_FAIL_MIN    = _t("QA_HB_JOBS_FAIL", 1)

DEDUP_RATE_WARN_PCT = _t("QA_DEDUP_RATE_WARN_PCT", 30)      # >30% duplicate-rate
DEDUP_RATE_FAIL_PCT = _t("QA_DEDUP_RATE_FAIL_PCT", 50)
SENT_RATE_WARN_PCT  = _t("QA_SENT_RATE_WARN_PCT", 20)       # <20% sent-rate
SENT_RATE_FAIL_PCT  = _t("QA_SENT_RATE_FAIL_PCT", 10)
ORACLE_AUDIT_WARN_PCT = _t("QA_AUDIT_RATE_WARN_PCT", 60)
ORACLE_AUDIT_FAIL_PCT = _t("QA_AUDIT_RATE_FAIL_PCT", 30)


# ─── Check helpers ───────────────────────────────────────────────────────────


def _hours_since(iso_or_dt) -> float | None:
    if not iso_or_dt:
        return None
    try:
        if isinstance(iso_or_dt, str):
            # Strip timezone for simple comparison
            s = iso_or_dt.rstrip("Z").split("+")[0].split(".")[0]
            t = datetime.fromisoformat(s)
        else:
            t = iso_or_dt
        return (datetime.utcnow() - t).total_seconds() / 3600
    except (TypeError, ValueError):
        return None


def _ladder(hours: float | None, warn: float, fail: float):
    """Map an age-in-hours to a (status, message) tuple."""
    if hours is None:
        return "fail", "no timestamp found — data may not exist"
    if hours >= fail:
        return "fail", f"{hours:.1f}h stale (≥ {fail}h fail threshold)"
    if hours >= warn:
        return "warn", f"{hours:.1f}h stale (≥ {warn}h warn threshold)"
    return "pass", f"{hours:.1f}h fresh"


# ─── Individual checks ───────────────────────────────────────────────────────


def check_scprs_awards_freshness(conn, ctx=None) -> dict:
    """Last `created_at` in scprs_awards. The 2026-03-14 freeze is what
    surfaced this whole audit. If this goes fail, the normalization
    pipeline that bridges po_lines → awards is dead."""
    try:
        row = conn.execute(
            "SELECT MAX(created_at) AS latest, COUNT(*) AS n FROM scprs_awards"
        ).fetchone()
        latest = row["latest"] if row else None
        n = row["n"] if row else 0
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": f"warn ≥{SCPRS_AWARDS_WARN_H}h",
                "message": "query failed — table may not exist"}
    h = _hours_since(latest)
    status, msg = _ladder(h, SCPRS_AWARDS_WARN_H, SCPRS_AWARDS_FAIL_H)
    return {
        "status": status,
        "value": {"latest_created_at": latest, "row_count": n,
                  "hours_since": round(h, 1) if h is not None else None},
        "threshold": f"warn ≥{SCPRS_AWARDS_WARN_H}h, fail ≥{SCPRS_AWARDS_FAIL_H}h",
        "message": msg,
    }


def check_scprs_po_lines_freshness(conn, ctx=None) -> dict:
    """scprs_po_lines is fed by the browser scrape. If this fails, the
    upstream data source is dead — no rollup, no chip, no signal."""
    # po_lines has no created_at column; use the po_id sequence + a
    # join to scprs_awards.created_at if available, otherwise the
    # presence of any 2026 rows.
    try:
        row = conn.execute("SELECT COUNT(*) AS n FROM scprs_po_lines").fetchone()
        total = row["n"] if row else 0
        # Proxy freshness: count of rows for any PO awarded in last 7 days
        recent = conn.execute(
            "SELECT COUNT(*) AS n FROM scprs_po_lines pl "
            "JOIN scprs_awards a ON a.po_number = pl.po_number "
            "WHERE a.created_at >= datetime('now', '-7 days')"
        ).fetchone()
        recent_n = recent["n"] if recent else 0
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": f"≥1 recent line in 7d",
                "message": "query failed"}
    if total == 0:
        return {"status": "fail", "value": {"total": 0, "recent_7d": 0},
                "threshold": "≥1 recent line",
                "message": "scprs_po_lines empty — scrape pipeline never ran"}
    if recent_n == 0:
        return {"status": "warn", "value": {"total": total, "recent_7d": 0},
                "threshold": "≥1 line linked to award in last 7d",
                "message": "no new po_lines linked to recent awards"}
    return {"status": "pass", "value": {"total": total, "recent_7d": recent_n},
            "threshold": "≥1 recent line in 7d", "message": "ok"}


def check_email_polling_freshness(conn, ctx=None) -> dict:
    """processed_emails should land within 2-4h during business hours.
    Skips the fail rung outside biz hours so off-hours pauses don't
    page anyone."""
    try:
        row = conn.execute(
            "SELECT MAX(processed_at) AS latest, COUNT(*) AS n FROM processed_emails"
        ).fetchone()
        latest = row["latest"] if row else None
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": "—", "message": "query failed"}
    h = _hours_since(latest)
    # Soften thresholds outside 6am-8pm PT
    now_h = datetime.utcnow().hour - 7  # rough PT shift
    in_biz = 6 <= (now_h % 24) <= 20
    warn = EMAIL_WARN_H if in_biz else EMAIL_WARN_H * 3
    fail = EMAIL_FAIL_H if in_biz else EMAIL_FAIL_H * 3
    status, msg = _ladder(h, warn, fail)
    return {
        "status": status,
        "value": {"latest_processed_at": latest,
                  "hours_since": round(h, 1) if h is not None else None,
                  "biz_hours": in_biz},
        "threshold": f"biz: warn ≥{warn}h, fail ≥{fail}h",
        "message": msg,
    }


def check_scheduler_heartbeats(conn, ctx=None) -> dict:
    """The scheduler_heartbeats table should have multiple jobs
    checking in. Empty = scheduler dead. The forensics found this
    table EMPTY on prod."""
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT job_name) AS n_jobs, MAX(last_heartbeat) AS latest "
            "FROM scheduler_heartbeats "
            "WHERE last_heartbeat >= datetime('now', '-6 hours')"
        ).fetchone()
        n_jobs = row["n_jobs"] if row else 0
        latest = row["latest"] if row else None
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": "—", "message": "query failed"}
    if n_jobs < HB_JOBS_FAIL_MIN:
        status, msg = "fail", f"only {n_jobs} jobs heartbeating (need ≥{HB_JOBS_FAIL_MIN})"
    elif n_jobs < HB_JOBS_WARN_MIN:
        status, msg = "warn", f"{n_jobs} jobs heartbeating (warn at <{HB_JOBS_WARN_MIN})"
    else:
        status, msg = "pass", f"{n_jobs} distinct jobs heartbeating in last 6h"
    return {
        "status": status,
        "value": {"jobs_in_last_6h": n_jobs, "latest_heartbeat": latest},
        "threshold": f"warn <{HB_JOBS_WARN_MIN}, fail <{HB_JOBS_FAIL_MIN}",
        "message": msg,
    }


def check_pc_dedup_rate(conn, ctx=None) -> dict:
    """% of PCs created in last 7d that got status='duplicate'. April
    2026 was 52%. If the auto-classifier is over-firing, we're losing
    real bid opportunities."""
    try:
        row = conn.execute(
            "SELECT "
            "  SUM(CASE WHEN status='duplicate' THEN 1 ELSE 0 END) AS dupes, "
            "  COUNT(*) AS total "
            "FROM price_checks "
            "WHERE created_at >= datetime('now', '-7 days')"
        ).fetchone()
        dupes = row["dupes"] or 0
        total = row["total"] or 0
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": "—", "message": "query failed"}
    if total == 0:
        return {"status": "warn", "value": {"dupes": 0, "total": 0},
                "threshold": "—",
                "message": "no PCs created in last 7d — volume concern"}
    pct = round(100.0 * dupes / total, 1)
    if pct >= DEDUP_RATE_FAIL_PCT:
        status, msg = "fail", f"{pct}% dedup-rate (fail ≥{DEDUP_RATE_FAIL_PCT}%) — classifier over-firing"
    elif pct >= DEDUP_RATE_WARN_PCT:
        status, msg = "warn", f"{pct}% dedup-rate (warn ≥{DEDUP_RATE_WARN_PCT}%)"
    else:
        status, msg = "pass", f"{pct}% dedup-rate"
    return {
        "status": status,
        "value": {"duplicate_count": dupes, "total_pcs_7d": total,
                  "duplicate_pct": pct},
        "threshold": f"warn ≥{DEDUP_RATE_WARN_PCT}%, fail ≥{DEDUP_RATE_FAIL_PCT}%",
        "message": msg,
    }


def check_pc_sent_rate(conn, ctx=None) -> dict:
    """% of PCs created in last 7d that reach status='sent'. Funnel
    health proxy. Currently April was 15% (15 sent of 100 created)."""
    try:
        row = conn.execute(
            "SELECT "
            "  SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent, "
            "  COUNT(*) AS total "
            "FROM price_checks "
            "WHERE created_at >= datetime('now', '-7 days')"
        ).fetchone()
        sent = row["sent"] or 0
        total = row["total"] or 0
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": "—", "message": "query failed"}
    if total == 0:
        return {"status": "warn", "value": {"sent": 0, "total": 0},
                "threshold": "—", "message": "no PCs in 7d — see volume check"}
    pct = round(100.0 * sent / total, 1)
    if pct <= SENT_RATE_FAIL_PCT:
        status, msg = "fail", f"{pct}% sent-rate (fail ≤{SENT_RATE_FAIL_PCT}%) — funnel stalled"
    elif pct <= SENT_RATE_WARN_PCT:
        status, msg = "warn", f"{pct}% sent-rate (warn ≤{SENT_RATE_WARN_PCT}%)"
    else:
        status, msg = "pass", f"{pct}% sent-rate"
    return {
        "status": status,
        "value": {"sent_count": sent, "total_pcs_7d": total, "sent_pct": pct},
        "threshold": f"warn ≤{SENT_RATE_WARN_PCT}%, fail ≤{SENT_RATE_FAIL_PCT}%",
        "message": msg,
    }


def check_oracle_audit_attach_rate(conn, ctx=None) -> dict:
    """% of priced PC items in last 7d that have `oracle_audit` on them.
    PR-G/H/I substrate depends on this being attached at enrichment
    time. If it falls, the chip + drift + shadow surfaces all go dark."""
    try:
        rows = conn.execute(
            "SELECT items FROM price_checks "
            "WHERE created_at >= datetime('now', '-7 days') "
            "AND status NOT IN ('duplicate','dismissed','reclassified')"
        ).fetchall()
    except Exception as e:
        return {"status": "fail", "value": {"err": str(e)[:120]},
                "threshold": "—", "message": "query failed"}
    total_priced_items = 0
    with_audit = 0
    for r in rows:
        try:
            items = json.loads(r["items"]) if r["items"] else []
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            unit_price = (it.get("unit_price")
                          or (it.get("pricing") or {}).get("unit_price"))
            if not unit_price:
                continue
            total_priced_items += 1
            if (it.get("oracle_audit") or {}):
                with_audit += 1
    if total_priced_items == 0:
        return {"status": "warn",
                "value": {"priced_items_7d": 0, "with_audit": 0},
                "threshold": "—",
                "message": "no priced items in last 7d"}
    pct = round(100.0 * with_audit / total_priced_items, 1)
    if pct <= ORACLE_AUDIT_FAIL_PCT:
        status, msg = "fail", f"{pct}% oracle_audit attach (fail ≤{ORACLE_AUDIT_FAIL_PCT}%) — substrate dark"
    elif pct <= ORACLE_AUDIT_WARN_PCT:
        status, msg = "warn", f"{pct}% oracle_audit attach (warn ≤{ORACLE_AUDIT_WARN_PCT}%)"
    else:
        status, msg = "pass", f"{pct}% items have oracle_audit"
    return {
        "status": status,
        "value": {"priced_items_7d": total_priced_items,
                  "with_audit": with_audit, "attach_pct": pct},
        "threshold": f"warn ≤{ORACLE_AUDIT_WARN_PCT}%, fail ≤{ORACLE_AUDIT_FAIL_PCT}%",
        "message": msg,
    }


# Registry — extend by appending. Each entry: (name, fn)
CHECKS: list[tuple[str, Callable]] = [
    ("scprs_awards_freshness",   check_scprs_awards_freshness),
    ("scprs_po_lines_freshness", check_scprs_po_lines_freshness),
    ("email_polling_freshness",  check_email_polling_freshness),
    ("scheduler_heartbeats",     check_scheduler_heartbeats),
    ("pc_dedup_rate",            check_pc_dedup_rate),
    ("pc_sent_rate",             check_pc_sent_rate),
    ("oracle_audit_attach_rate", check_oracle_audit_attach_rate),
]


# ─── Runners ─────────────────────────────────────────────────────────────────


def run_all_checks(conn=None) -> dict:
    """Run every registered check. Returns {check_name: result_dict}.
    Each check failure is isolated — one broken check doesn't kill
    the suite."""
    own_conn = conn is None
    if own_conn:
        from src.core.db import get_db
        ctx_mgr = get_db()
        conn = ctx_mgr.__enter__()
    try:
        results: dict = {}
        for name, fn in CHECKS:
            try:
                results[name] = fn(conn, ctx=None)
            except Exception as e:
                log.warning("QA check %s raised: %s", name, e)
                results[name] = {
                    "status": "fail",
                    "value": {"exception": str(e)[:200]},
                    "threshold": "—",
                    "message": f"check raised: {e}",
                }
        return results
    finally:
        if own_conn:
            try:
                ctx_mgr.__exit__(None, None, None)
            except Exception as _e:
                log.debug("QA conn close: %s", _e)


def run_and_persist() -> dict:
    """Run all checks + write each result to qa_heartbeat. Returns the
    same dict as run_all_checks plus a `cycle_id` field."""
    cycle_id = uuid.uuid4().hex[:12]
    ran_at = datetime.utcnow().isoformat()
    results = run_all_checks()
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = []
            for name, r in results.items():
                rows.append((
                    ran_at, name, r.get("status", "fail"),
                    json.dumps(r.get("value") or {}, default=str),
                    r.get("threshold") or "",
                    (r.get("message") or "")[:500],
                    cycle_id,
                ))
            conn.executemany(
                "INSERT INTO qa_heartbeat "
                "(ran_at, check_name, status, value_json, threshold, "
                " message, cycle_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            # Bound the table — keep last 30d of heartbeats.
            conn.execute(
                "DELETE FROM qa_heartbeat "
                "WHERE ran_at < datetime('now', '-30 days')"
            )
        log.info(
            "qa_heartbeat: cycle=%s pass=%d warn=%d fail=%d",
            cycle_id,
            sum(1 for r in results.values() if r.get("status") == "pass"),
            sum(1 for r in results.values() if r.get("status") == "warn"),
            sum(1 for r in results.values() if r.get("status") == "fail"),
        )
    except Exception as e:
        log.warning("qa_heartbeat persist failed: %s", e)
    return {"cycle_id": cycle_id, "ran_at": ran_at, "results": results}


def latest_status() -> dict:
    """Pull the most recent row per check_name. Used by the
    /admin/qa surface."""
    out: dict = {}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT q.* FROM qa_heartbeat q
                JOIN (
                    SELECT check_name, MAX(ran_at) AS latest
                      FROM qa_heartbeat
                     GROUP BY check_name
                ) m ON m.check_name = q.check_name AND m.latest = q.ran_at
            """).fetchall()
            for r in rows:
                try:
                    val = json.loads(r["value_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    val = {}
                out[r["check_name"]] = {
                    "status": r["status"], "ran_at": r["ran_at"],
                    "value": val, "threshold": r["threshold"],
                    "message": r["message"], "cycle_id": r["cycle_id"],
                }
    except Exception as e:
        log.warning("qa_heartbeat latest_status failed: %s", e)
    return out
