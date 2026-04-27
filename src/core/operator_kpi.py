"""Operator KPI telemetry — measure time-to-send for the <90s KPI.

Plan §4.1 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-27). Until now we had no
way to MEASURE the "1 quote sent in <90 seconds" KPI — quote send fired
without a timer. This module logs one row per Mark Sent click into the
`operator_quote_sent` table (migration 34) and exposes simple analytics
queries.

Usage from a send route:

    from src.core.operator_kpi import log_quote_sent
    log_quote_sent(
        quote_id=pcid,
        quote_type="pc",
        started_at=pc.get("created_at"),
        item_count=len(pc.get("items", [])),
        agency_key=agency_key,
        quote_total=pc.get("total", 0),
    )

The function is best-effort: any error is swallowed (with a warning log)
so a logging failure never blocks the actual send.
"""

import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger("operator_kpi")


def log_quote_sent(
    quote_id: str,
    quote_type: str = "pc",
    started_at: Optional[str] = None,
    item_count: int = 0,
    agency_key: str = "",
    quote_total: float = 0.0,
) -> dict:
    """Insert one row into operator_quote_sent. Computes
    time_to_send_seconds at ingest. Best-effort — never raises.

    Returns: {ok, time_to_send_seconds, sent_at} on success or
             {ok: False, error: ...} on failure.
    """
    if not quote_id:
        return {"ok": False, "error": "quote_id required"}

    sent_at = datetime.now()
    seconds: Optional[int] = None
    if started_at:
        try:
            # ISO 8601 with optional timezone — fromisoformat is lenient
            # in 3.11+. Strip 'Z' for older Python compat.
            s = started_at.rstrip("Z")
            t0 = datetime.fromisoformat(s)
            delta = (sent_at - t0).total_seconds()
            seconds = max(0, int(delta))
        except (TypeError, ValueError):
            seconds = None

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO operator_quote_sent
                (quote_id, quote_type, sent_at, started_at,
                 time_to_send_seconds, item_count, agency_key, quote_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(quote_id), str(quote_type or "pc"), sent_at.isoformat(),
                started_at, seconds, int(item_count or 0),
                str(agency_key or ""), float(quote_total or 0),
            ))
        log.info(
            "KPI: %s/%s sent in %ss (%d items, agency=%s)",
            quote_type, quote_id, seconds if seconds is not None else "?",
            item_count, agency_key,
        )
        return {"ok": True, "time_to_send_seconds": seconds,
                "sent_at": sent_at.isoformat()}
    except Exception as e:
        log.warning("operator_kpi log_quote_sent failed: %s", e)
        return {"ok": False, "error": str(e)}


def get_kpi_stats(window_days: int = 7,
                  one_item_only: bool = False,
                  agency_key: Optional[str] = None) -> dict:
    """Aggregate stats for analytics dashboard.

    Returns:
        {
          ok,
          window_days, count, median_seconds, p95_seconds,
          under_90_count, under_90_pct,
          per_agency: [{agency_key, count, median_seconds, under_90_pct}, ...],
        }
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            sql = """
                SELECT time_to_send_seconds, agency_key
                FROM operator_quote_sent
                WHERE sent_at >= datetime('now', ?)
                  AND time_to_send_seconds IS NOT NULL
            """
            params: list = [f"-{int(window_days)} days"]
            if one_item_only:
                sql += " AND item_count = 1"
            if agency_key:
                sql += " AND agency_key = ?"
                params.append(agency_key)
            rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        return {"ok": False, "error": str(e)}

    secs = sorted([r["time_to_send_seconds"] for r in rows
                   if r["time_to_send_seconds"] is not None])
    n = len(secs)
    if not n:
        return {
            "ok": True, "window_days": window_days, "count": 0,
            "median_seconds": None, "p95_seconds": None,
            "under_90_count": 0, "under_90_pct": None,
            "per_agency": [],
        }

    median = secs[n // 2]
    p95 = secs[min(n - 1, int(n * 0.95))]
    under_90 = sum(1 for s in secs if s <= 90)

    # Per-agency rollup
    by_agency: dict = {}
    for r in rows:
        ag = r["agency_key"] or "(unknown)"
        b = by_agency.setdefault(ag, [])
        if r["time_to_send_seconds"] is not None:
            b.append(r["time_to_send_seconds"])
    per_agency = []
    for ag, vs in by_agency.items():
        vs_sorted = sorted(vs)
        m = len(vs_sorted)
        if not m:
            continue
        per_agency.append({
            "agency_key": ag,
            "count": m,
            "median_seconds": vs_sorted[m // 2],
            "under_90_pct": round(100.0 * sum(1 for v in vs if v <= 90) / m, 1),
        })
    per_agency.sort(key=lambda d: -d["count"])

    return {
        "ok": True, "window_days": window_days, "count": n,
        "median_seconds": median, "p95_seconds": p95,
        "under_90_count": under_90,
        "under_90_pct": round(100.0 * under_90 / n, 1),
        "per_agency": per_agency,
    }
