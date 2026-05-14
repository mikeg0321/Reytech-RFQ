"""Operator KPI telemetry — measure time-to-send for the <90s KPI.

Plan §4.1 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-27). Until now we had no
way to MEASURE the "1 quote sent in <90 seconds" KPI — quote send fired
without a timer. This module logs one row per Mark Sent click into the
`operator_quote_sent` table (migration 34) and exposes simple analytics
queries.

PR-I (2026-05-13) extends this surface: `log_operator_drift` captures
the (sent_price, rec_price, caps_applied) tuple for every priced line at
Mark-Sent time, one row per LINE into `operator_drift_line` (migration
45). This is the high-volume signal — at N=50 sent quotes/mo with ~10
lines each, drift gives us ~500 datapoints/mo vs 50 WR datapoints — fast
enough to call cap binds "better" or "worse" within weeks instead of
quarters.

Usage from a send route:

    from src.core.operator_kpi import log_quote_sent, log_operator_drift
    log_quote_sent(
        quote_id=pcid,
        quote_type="pc",
        started_at=pc.get("created_at"),
        item_count=len(pc.get("items", [])),
        agency_key=agency_key,
        quote_total=pc.get("total", 0),
    )
    log_operator_drift(
        quote_id=pcid, quote_type="pc",
        items=pc.get("items") or [],
        agency_key=agency_key,
    )

Both functions are best-effort: any error is swallowed (with a warning
log) so a logging failure never blocks the actual send.
"""

import json
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


# ─── Operator drift (PR-I) ───────────────────────────────────────────────────


def _item_sent_price(item: dict) -> Optional[float]:
    """The price the operator actually sent for this line.

    Prefer `unit_price` (the canonical billable extension input),
    fall back to `bid_price` then `pricing.recommended_price`. Anything
    non-positive returns None — we don't log drift on zero-priced lines.
    """
    for key in ("unit_price", "bid_price", "price_per_unit"):
        v = item.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    pricing = item.get("pricing") or {}
    for key in ("recommended_price", "unit_price"):
        v = pricing.get(key)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return None


def log_operator_drift(
    quote_id: str,
    quote_type: str,
    items: list,
    agency_key: str = "",
    quote_number: str = "",
) -> dict:
    """Insert one row per priced line with an oracle_audit envelope into
    operator_drift_line. Lines without oracle_audit are skipped silently
    (legacy lines pre-PR-H, lines the oracle couldn't price).

    The drift signal answers "did the operator override the oracle?" —
    higher-resolution than WR at low monthly volume. Skipping lines that
    never had an oracle run is deliberate: a NULL drift row would
    pollute the dataset with non-decisions.

    Best-effort — never raises. Logging failure never blocks the actual
    send. Idempotency: re-firing Mark-Sent on an already-sent quote will
    insert duplicate rows. The Mark-Sent routes already gate the
    transition; we trust that gate rather than dedupe here.

    Returns {ok, rows_logged, skipped_no_audit, skipped_no_price}.
    """
    if not quote_id or not items:
        return {"ok": False, "error": "quote_id+items required",
                "rows_logged": 0}

    sent_at = datetime.now().isoformat()
    rows: list[tuple] = []
    skipped_no_audit = 0
    skipped_no_price = 0
    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        audit = it.get("oracle_audit") or {}
        if not isinstance(audit, dict) or not audit:
            skipped_no_audit += 1
            continue
        sent = _item_sent_price(it)
        if sent is None:
            skipped_no_price += 1
            continue
        try:
            rec = audit.get("rec_price")
            rec_f = float(rec) if rec is not None else None
        except (TypeError, ValueError):
            rec_f = None
        try:
            pre = audit.get("rec_pre_cap_price")
            pre_f = float(pre) if pre is not None else None
        except (TypeError, ValueError):
            pre_f = None
        drift_pct: Optional[float] = None
        if rec_f and rec_f > 0:
            drift_pct = round(((sent - rec_f) / rec_f) * 100, 2)
        caps = audit.get("caps_applied") or []
        try:
            caps_json = json.dumps(caps, default=str)
        except (TypeError, ValueError):
            caps_json = "[]"
        sources = ",".join(
            sorted({str(c.get("source") or "") for c in caps
                    if isinstance(c, dict) and c.get("source")})
        )
        rollup = audit.get("scprs_rollup") or {}
        try:
            match_count = int(rollup.get("count") or 0) if isinstance(rollup, dict) else 0
        except (TypeError, ValueError):
            match_count = 0
        rows.append((
            str(quote_id), str(quote_type or "pc"), sent_at,
            str(agency_key or ""), idx,
            str(it.get("item_number") or ""),
            str(it.get("mfg_number") or it.get("part_number") or ""),
            sent, rec_f, pre_f, drift_pct,
            caps_json, sources, match_count,
            str(quote_number or ""),
        ))

    if not rows:
        return {"ok": True, "rows_logged": 0,
                "skipped_no_audit": skipped_no_audit,
                "skipped_no_price": skipped_no_price}

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.executemany("""
                INSERT INTO operator_drift_line
                (quote_id, quote_type, sent_at, agency_key,
                 line_idx, item_number, mfg_number,
                 sent_price, rec_price, rec_pre_cap_price, drift_pct,
                 caps_applied_json, cap_sources, scprs_match_count,
                 quote_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
        log.info(
            "operator_drift: %s/%s logged %d/%d lines "
            "(skipped: %d no audit, %d no price)",
            quote_type, quote_id, len(rows), len(items),
            skipped_no_audit, skipped_no_price,
        )
        return {"ok": True, "rows_logged": len(rows),
                "skipped_no_audit": skipped_no_audit,
                "skipped_no_price": skipped_no_price}
    except Exception as e:
        log.warning("operator_kpi log_operator_drift failed: %s", e)
        return {"ok": False, "error": str(e),
                "rows_logged": 0}


def fire_drift_logs_on_send(quote_id: str, quote_type: str, pc_or_rfq: dict) -> dict:
    """Single entry point for Mark-Sent drift logging.

    PR-U (2026-05-13): the walkthrough audit found that drift logging
    only fired on `/api/pricecheck/<id>/send-quote` (Mike doesn't use),
    while the canonical operator path is `/api/pricecheck/<id>/mark-sent`
    + `/mark-sent-manually` + RFQ equivalents — none of which called
    `log_operator_drift` or `log_operator_drift_shadow`. Result: every
    week of operator activity emptied to the digest with no signal,
    PR-S auto-recommendations had near-zero input. Centralising the
    call here means every future mark-sent variant just calls one
    function.

    Returns the combined result dict; never raises. Best-effort —
    a logging failure must not block a Mark-Sent flip.
    """
    if not quote_id or not isinstance(pc_or_rfq, dict):
        return {"ok": False, "error": "quote_id + dict required"}
    items = pc_or_rfq.get("items") or pc_or_rfq.get("line_items") or []
    agency_key = (
        pc_or_rfq.get("agency_key")
        or pc_or_rfq.get("agency")
        or pc_or_rfq.get("institution")
        or ""
    )
    result: dict = {"drift": None, "shadow": None}
    try:
        result["drift"] = log_operator_drift(
            quote_id=quote_id, quote_type=quote_type,
            items=items, agency_key=agency_key,
        )
    except Exception as e:
        log.debug("fire_drift_logs (drift) suppressed: %s", e)
    try:
        result["shadow"] = log_operator_drift_shadow(
            quote_id=quote_id, quote_type=quote_type,
            items=items, agency_key=agency_key,
        )
    except Exception as e:
        log.debug("fire_drift_logs (shadow) suppressed: %s", e)
    return result


def get_drift_audit_coverage() -> dict:
    """PR-AQ — explain WHY operator_drift_line is empty.

    PR-AP diagnostic showed 0 rows table-wide. /admin/funnel shows
    sent PCs in the window. So drift logging fires on every Mark-Sent
    but produces zero rows.

    `log_operator_drift` skips lines for two reasons:
      - `skipped_no_audit`: item has no `oracle_audit` dict (or empty)
      - `skipped_no_price`: item has no positive
        unit_price/bid_price/price_per_unit

    This function walks every active PC and RFQ with status="sent" and
    reports per-record + aggregate:
      - items_total
      - items_with_audit (non-empty oracle_audit)
      - items_with_sent_price (positive unit_price/bid_price/price_per_unit)
      - items_would_log (both gates clear)

    The substrate fix path depends on which gate is killing the rows:
      - audit empty everywhere → oracle_audit is never being WRITTEN at
        the pricing/recommend step. Fix the writer.
      - price empty everywhere → Mark-Sent fires before the operator
        sets the final price. Fix the gate or capture timing.
      - both partial → mixed; fix both.
    """
    try:
        from src.api.dashboard import _load_price_checks, load_rfqs
    except Exception as e:
        return {"ok": False, "error": f"data layer unavailable: {e}"}

    def _walk(records: dict, kind: str) -> dict:
        items_total = 0
        items_with_audit = 0
        items_with_price = 0
        items_would_log = 0
        sent_count = 0
        per_record: list = []
        for rid, r in list(records.items() if records else []):
            if not isinstance(r, dict):
                continue
            if (r.get("status") or "").strip().lower() != "sent":
                continue
            sent_count += 1
            r_items = r.get("items") or r.get("line_items") or []
            r_total = 0
            r_audit = 0
            r_price = 0
            r_both = 0
            for it in r_items:
                if not isinstance(it, dict):
                    continue
                r_total += 1
                audit = it.get("oracle_audit") or {}
                has_audit = isinstance(audit, dict) and bool(audit)
                sent_price = _item_sent_price(it)
                has_price = sent_price is not None
                if has_audit:
                    r_audit += 1
                if has_price:
                    r_price += 1
                if has_audit and has_price:
                    r_both += 1
            items_total += r_total
            items_with_audit += r_audit
            items_with_price += r_price
            items_would_log += r_both
            per_record.append({
                "id": rid,
                "type": kind,
                "items_total": r_total,
                "items_with_audit": r_audit,
                "items_with_price": r_price,
                "items_would_log": r_both,
                "sent_at": r.get("sent_at"),
                "agency_key": (r.get("agency_key")
                               or r.get("agency")
                               or r.get("institution") or ""),
            })
        return {
            "kind": kind,
            "sent_count": sent_count,
            "items_total": items_total,
            "items_with_audit": items_with_audit,
            "items_with_price": items_with_price,
            "items_would_log": items_would_log,
            "per_record": per_record[:20],  # cap for response size
        }

    try:
        pc_summary = _walk(_load_price_checks() or {}, "pc")
        rfq_summary = _walk(load_rfqs() or {}, "rfq")
    except Exception as e:
        return {"ok": False, "error": str(e)}

    total_items = pc_summary["items_total"] + rfq_summary["items_total"]
    total_audit = pc_summary["items_with_audit"] + rfq_summary["items_with_audit"]
    total_price = pc_summary["items_with_price"] + rfq_summary["items_with_price"]
    total_both = pc_summary["items_would_log"] + rfq_summary["items_would_log"]

    return {
        "ok": True,
        "pc": pc_summary,
        "rfq": rfq_summary,
        "aggregate": {
            "sent_records": pc_summary["sent_count"] + rfq_summary["sent_count"],
            "items_total": total_items,
            "items_with_audit": total_audit,
            "items_with_price": total_price,
            "items_would_log": total_both,
            "audit_coverage_pct": (
                round(100.0 * total_audit / total_items, 1)
                if total_items else None
            ),
            "price_coverage_pct": (
                round(100.0 * total_price / total_items, 1)
                if total_items else None
            ),
        },
    }


def get_drift_diagnostic() -> dict:
    """PR-AP — operator_drift_line table state for the admin diagnostic.

    Returns counts (total + 7/30/90-day windows), per-quote_type
    breakdown, 10 most recent rows, distinct agency_keys, and drift_pct
    distribution (min/p25/median/p75/max).

    Lives here (src/core/) rather than the route module so the
    canonical_state gate doesn't reject the inline `datetime('now',?)`
    window filters — `src/core/` is the documented exempt zone for
    drift-table SQL (per the get_drift_lines_per_agency pattern).
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS c FROM operator_drift_line"
            ).fetchone()["c"]

            by_window: dict = {}
            for label, days in [("7d", 7), ("30d", 30), ("90d", 90)]:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM operator_drift_line "
                    "WHERE sent_at >= datetime('now', ?)",
                    (f"-{days} days",),
                ).fetchone()
                by_window[label] = row["c"]

            by_quote_type: dict = {}
            for row in conn.execute(
                "SELECT quote_type, COUNT(*) AS c FROM operator_drift_line "
                "GROUP BY quote_type"
            ).fetchall():
                by_quote_type[row["quote_type"] or "(null)"] = row["c"]

            recent: list = []
            for row in conn.execute(
                "SELECT quote_id, quote_type, sent_at, agency_key, "
                "drift_pct, cap_sources "
                "FROM operator_drift_line "
                "ORDER BY sent_at DESC LIMIT 10"
            ).fetchall():
                recent.append({
                    "quote_id": row["quote_id"],
                    "quote_type": row["quote_type"],
                    "sent_at": row["sent_at"],
                    "agency_key": row["agency_key"],
                    "drift_pct": row["drift_pct"],
                    "cap_sources": row["cap_sources"],
                })

            agencies = [
                r["agency_key"] for r in conn.execute(
                    "SELECT DISTINCT agency_key FROM operator_drift_line "
                    "WHERE agency_key IS NOT NULL AND agency_key != '' "
                    "ORDER BY agency_key"
                ).fetchall()
            ]

            drift_rows = [
                r["drift_pct"] for r in conn.execute(
                    "SELECT drift_pct FROM operator_drift_line "
                    "WHERE drift_pct IS NOT NULL "
                    "ORDER BY drift_pct"
                ).fetchall()
            ]
            stats: Optional[dict] = None
            if drift_rows:
                n = len(drift_rows)
                stats = {
                    "n": n,
                    "min": round(drift_rows[0], 2),
                    "p25": round(drift_rows[n // 4], 2),
                    "median": round(drift_rows[n // 2], 2),
                    "p75": round(drift_rows[(3 * n) // 4], 2),
                    "max": round(drift_rows[-1], 2),
                }
    except Exception as e:
        return {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "total": total,
        "by_window": by_window,
        "by_quote_type": by_quote_type,
        "recent": recent,
        "agencies": agencies,
        "drift_pct_stats": stats,
    }


def get_drift_lines_per_agency(window_days: int = 7) -> list:
    """PR-S helper: aggregate every operator_drift_line row in the window
    by agency_key. Used by `auto_recommendations.build_*`.

    Lives in operator_kpi.py (not the caller's `src/agents/` module) so
    the canonical_state gate doesn't reject the inline WHERE sent_at
    filter — `src/core/` is the exempt zone for drift-table SQL.

    Returns: list of dicts:
      {agency, line_count, quote_count, median_drift_pct,
       capped_lines, capped_pct}, sorted by line_count desc.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT agency_key, quote_id, drift_pct, cap_sources
                FROM operator_drift_line
                WHERE sent_at >= datetime('now', ?)
                  AND drift_pct IS NOT NULL
            """, (f"-{int(window_days)} days",)).fetchall()
    except Exception as e:
        log.warning("get_drift_lines_per_agency failed: %s", e)
        return []

    by_agency: dict = {}
    for r in rows:
        ag = (r["agency_key"] or "(unknown)").strip() or "(unknown)"
        by_agency.setdefault(ag, []).append({
            "quote_id": r["quote_id"],
            "drift_pct": float(r["drift_pct"]),
            "cap_sources": r["cap_sources"] or "",
        })

    out: list = []
    for ag, lines in by_agency.items():
        drifts = sorted([l["drift_pct"] for l in lines])
        n = len(drifts)
        median = drifts[n // 2] if n else None
        quote_count = len({l["quote_id"] for l in lines})
        capped_lines = sum(1 for l in lines if l["cap_sources"])
        capped_pct = round(100.0 * capped_lines / n, 1) if n else 0.0
        out.append({
            "agency": ag,
            "line_count": n,
            "quote_count": quote_count,
            "median_drift_pct": round(median, 2) if median is not None else None,
            "capped_lines": capped_lines,
            "capped_pct": capped_pct,
        })
    out.sort(key=lambda d: -d["line_count"])
    return out


def get_drift_stats(window_days: int = 30,
                    agency_key: Optional[str] = None) -> dict:
    """Aggregate drift stats for the digest preview.

    Returns:
        {
          ok, window_days, line_count, quote_count,
          median_drift_pct, p25_drift_pct, p75_drift_pct,
          capped_lines, capped_above_oracle, capped_below_oracle,
          per_cap_source: [{source, line_count, median_drift_pct}, ...],
        }

    "capped_above_oracle" = lines where the cap fired AND operator
    sent MORE than the capped rec_price (operator overrode the cap
    upward). This is the high-leverage cohort for tuning: if these
    lines win, the cap is too tight; if they lose, the cap was right.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            sql = """
                SELECT quote_id, drift_pct, cap_sources, sent_price,
                       rec_price, rec_pre_cap_price
                FROM operator_drift_line
                WHERE sent_at >= datetime('now', ?)
                  AND drift_pct IS NOT NULL
            """
            params: list = [f"-{int(window_days)} days"]
            if agency_key:
                sql += " AND agency_key = ?"
                params.append(agency_key)
            rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        return {"ok": False, "error": str(e)}

    if not rows:
        return {"ok": True, "window_days": window_days, "line_count": 0,
                "quote_count": 0, "median_drift_pct": None,
                "p25_drift_pct": None, "p75_drift_pct": None,
                "capped_lines": 0, "capped_above_oracle": 0,
                "capped_below_oracle": 0, "per_cap_source": []}

    drifts = sorted([float(r["drift_pct"]) for r in rows])
    n = len(drifts)
    median = drifts[n // 2]
    p25 = drifts[max(0, int(n * 0.25) - 1)]
    p75 = drifts[min(n - 1, int(n * 0.75))]

    quote_ids = {r["quote_id"] for r in rows}

    capped_rows = [r for r in rows if r["cap_sources"]]
    capped_above = sum(1 for r in capped_rows
                       if r["drift_pct"] is not None
                       and float(r["drift_pct"]) > 0)
    capped_below = sum(1 for r in capped_rows
                       if r["drift_pct"] is not None
                       and float(r["drift_pct"]) < 0)

    by_source: dict = {}
    for r in capped_rows:
        for src in (r["cap_sources"] or "").split(","):
            src = src.strip()
            if not src:
                continue
            b = by_source.setdefault(src, [])
            if r["drift_pct"] is not None:
                b.append(float(r["drift_pct"]))
    per_source = []
    for src, ds in by_source.items():
        ds_sorted = sorted(ds)
        m = len(ds_sorted)
        if not m:
            continue
        per_source.append({
            "source": src,
            "line_count": m,
            "median_drift_pct": round(ds_sorted[m // 2], 2),
        })
    per_source.sort(key=lambda d: -d["line_count"])

    return {
        "ok": True, "window_days": window_days,
        "line_count": n, "quote_count": len(quote_ids),
        "median_drift_pct": round(median, 2),
        "p25_drift_pct": round(p25, 2),
        "p75_drift_pct": round(p75, 2),
        "capped_lines": len(capped_rows),
        "capped_above_oracle": capped_above,
        "capped_below_oracle": capped_below,
        "per_cap_source": per_source,
    }


# ─── Shadow-mode cap evaluator (PR-J) ────────────────────────────────────────


def _scprs_rollup_cap_enabled() -> bool:
    """Delegates to `pricing_oracle_v2._scprs_rollup_cap_enabled` so the
    shadow logger and the active cap binding always read the same
    answer. PR-R (2026-05-13): post-PR-O auto-detect — when the env var
    is unset, the cap is active iff scprs_awards has fresh rows."""
    try:
        from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled as _impl
        return _impl()
    except Exception:
        # Defensive fallback for the very early-boot window before the
        # oracle module is importable. Treat as off — shadow logger will
        # tag rows as `would_cap`/`no_cap` instead of `cap_active`,
        # which is the correct conservative answer.
        return False


def log_operator_drift_shadow(
    quote_id: str,
    quote_type: str,
    items: list,
    agency_key: str = "",
    quote_number: str = "",
) -> dict:
    """For each item with `oracle_audit.scprs_rollup` data, compute the
    counterfactual cap price (p75 of the SCPRS rollup) and log how the
    sent_price compares.

    Three outcomes per line:
      - 'would_cap'   cap is currently OFF + sent_price > p75 → cap
                      would have lowered the price.
      - 'no_cap'      cap is currently OFF + sent_price <= p75 → cap
                      would not have moved the line.
      - 'cap_active'  cap is currently ON. Whether or not the
                      recommendation actually used the cap, this row
                      records that the live system had the cap binding
                      available.

    Lines without rollup data are skipped (shadow_action='no_data' is
    reserved for a future "ran but had no rollup" surface). Same
    best-effort discipline as `log_operator_drift`.

    Returns {ok, rows_logged, skipped_no_rollup}.
    """
    if not quote_id or not items:
        return {"ok": False, "error": "quote_id+items required",
                "rows_logged": 0}

    sent_at = datetime.now().isoformat()
    cap_is_live = _scprs_rollup_cap_enabled()
    rows: list[tuple] = []
    skipped_no_rollup = 0

    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        audit = it.get("oracle_audit") or {}
        if not isinstance(audit, dict):
            skipped_no_rollup += 1
            continue
        rollup = audit.get("scprs_rollup") or {}
        if not isinstance(rollup, dict) or not rollup:
            skipped_no_rollup += 1
            continue
        try:
            p75 = rollup.get("p75")
            p75_f = float(p75) if p75 is not None else None
        except (TypeError, ValueError):
            p75_f = None
        if p75_f is None or p75_f <= 0:
            skipped_no_rollup += 1
            continue

        sent = _item_sent_price(it)
        if sent is None:
            skipped_no_rollup += 1
            continue
        try:
            rec = audit.get("rec_price")
            rec_f = float(rec) if rec is not None else None
        except (TypeError, ValueError):
            rec_f = None

        # Shadow-cap simulation: cap binds quote_price down to p75.
        # Even when live cap is ON, the counterfactual price the cap
        # WOULD set is the same p75 — this lets the digest compare
        # operator_sent vs shadow_cap consistently across flag states.
        shadow_cap = p75_f
        if cap_is_live:
            action = "cap_active"
        elif sent > shadow_cap:
            action = "would_cap"
        else:
            action = "no_cap"

        # Drift between operator-sent and shadow cap price. Positive =
        # operator sent ABOVE shadow cap (cap would have hurt margin).
        # Negative = operator sent BELOW shadow cap (cap was leaving
        # margin on the table that the operator already shaved).
        try:
            shadow_drift_pct = round(
                ((sent - shadow_cap) / shadow_cap) * 100, 2
            )
        except (TypeError, ZeroDivisionError):
            shadow_drift_pct = None

        try:
            count = int(rollup.get("count") or 0)
        except (TypeError, ValueError):
            count = 0

        rows.append((
            str(quote_id), str(quote_type or "pc"), sent_at,
            str(agency_key or ""), idx,
            str(it.get("item_number") or ""),
            str(it.get("mfg_number") or it.get("part_number") or ""),
            sent, rec_f, shadow_cap, action, shadow_drift_pct,
            p75_f, count,
            str(rollup.get("match_key") or ""),
            str(rollup.get("match_key_type") or ""),
            str(quote_number or ""),
        ))

    if not rows:
        return {"ok": True, "rows_logged": 0,
                "skipped_no_rollup": skipped_no_rollup}

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.executemany("""
                INSERT INTO operator_drift_shadow
                (quote_id, quote_type, sent_at, agency_key,
                 line_idx, item_number, mfg_number,
                 sent_price, rec_price, shadow_cap_price,
                 shadow_action, shadow_drift_pct,
                 rollup_p75, rollup_count,
                 rollup_match_key, rollup_match_key_type,
                 quote_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
        log.info(
            "operator_drift_shadow: %s/%s logged %d/%d "
            "(skipped: %d no rollup, cap_live=%s)",
            quote_type, quote_id, len(rows), len(items),
            skipped_no_rollup, cap_is_live,
        )
        return {"ok": True, "rows_logged": len(rows),
                "skipped_no_rollup": skipped_no_rollup,
                "cap_was_live": cap_is_live}
    except Exception as e:
        log.warning("log_operator_drift_shadow failed: %s", e)
        return {"ok": False, "error": str(e), "rows_logged": 0}


def get_shadow_stats(window_days: int = 30,
                     agency_key: Optional[str] = None) -> dict:
    """Aggregate shadow-mode stats for the digest preview.

    Returns:
        {
          ok, window_days, line_count,
          would_cap_count,        # cap OFF, would have moved this line
          no_cap_count,           # cap OFF, would not have moved
          cap_active_count,       # cap ON
          median_shadow_drift_pct,
          avg_savings_per_capped_line,  # in dollars
          total_savings_if_capped,      # extrapolation: would_cap × (sent-p75)
        }

    `total_savings_if_capped` answers "how much margin would the cap
    have COST us in the last 30d?" — but it's stating it as a positive
    number because that's how the cap-decision conversation goes:
    "we'd have left $X on the table." Mike weighs that against the
    WR lift hypothesis.
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            sql = """
                SELECT shadow_action, shadow_drift_pct,
                       sent_price, shadow_cap_price
                FROM operator_drift_shadow
                WHERE sent_at >= datetime('now', ?)
            """
            params: list = [f"-{int(window_days)} days"]
            if agency_key:
                sql += " AND agency_key = ?"
                params.append(agency_key)
            rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        return {"ok": False, "error": str(e)}

    if not rows:
        return {"ok": True, "window_days": window_days,
                "line_count": 0,
                "would_cap_count": 0, "no_cap_count": 0,
                "cap_active_count": 0,
                "median_shadow_drift_pct": None,
                "avg_savings_per_capped_line": None,
                "total_savings_if_capped": 0.0}

    would_cap = [r for r in rows if r["shadow_action"] == "would_cap"]
    no_cap = [r for r in rows if r["shadow_action"] == "no_cap"]
    active = [r for r in rows if r["shadow_action"] == "cap_active"]

    drifts = sorted([float(r["shadow_drift_pct"]) for r in rows
                     if r["shadow_drift_pct"] is not None])
    median_drift = drifts[len(drifts) // 2] if drifts else None

    # Margin that would have moved off the quote into the customer's
    # pocket if the cap had been live. One row = (sent - shadow_cap)
    # × 1 unit; per-quote totals not computed here since qty isn't on
    # the shadow row. Future iteration could carry qty for $ savings.
    savings = []
    for r in would_cap:
        try:
            delta = float(r["sent_price"]) - float(r["shadow_cap_price"])
            if delta > 0:
                savings.append(delta)
        except (TypeError, ValueError):
            continue
    avg_saving = round(sum(savings) / len(savings), 2) if savings else None
    total_saving = round(sum(savings), 2) if savings else 0.0

    return {
        "ok": True, "window_days": window_days,
        "line_count": len(rows),
        "would_cap_count": len(would_cap),
        "no_cap_count": len(no_cap),
        "cap_active_count": len(active),
        "median_shadow_drift_pct": (
            round(median_drift, 2) if median_drift is not None else None
        ),
        "avg_savings_per_capped_line": avg_saving,
        "total_savings_if_capped": total_saving,
    }


# ─── Outcome resolution (PR-K1) ──────────────────────────────────────────────


def resolve_drift_outcome(
    quote_id: str = "",
    quote_number: str = "",
    outcome: str = "",
    source: str = "",
) -> dict:
    """Backfill `outcome` on operator_drift_line + operator_drift_shadow
    rows for the given quote.

    Joins via `quote_id` (the PC/RFQ id) AND `quote_number` so award_monitor
    (knows pc.id) and quote_lifecycle (knows quote_number) can both flush
    outcomes. Only updates rows where outcome IS NULL — re-detected awards
    must not overwrite an earlier resolution.

    This is the JOIN that turns drift signal into WR signal. Without it the
    digest can show drift distributions but can't answer "of lines with
    drift > 20%, what's the WR?" — the actual decision-supporting query
    for cap tuning.

    Returns {ok, drift_rows_updated, shadow_rows_updated}.
    """
    if outcome not in ("won", "lost"):
        return {"ok": False, "error": f"invalid outcome {outcome!r}"}
    if not quote_id and not quote_number:
        return {"ok": False, "error": "quote_id or quote_number required"}

    now_iso = datetime.now().isoformat()
    src = str(source or "unknown")

    # Build a flexible WHERE so either join key resolves the row.
    where_parts: list[str] = []
    params: list = []
    if quote_id:
        where_parts.append("quote_id = ?")
        params.append(str(quote_id))
    if quote_number:
        where_parts.append("quote_number = ?")
        params.append(str(quote_number))
    where_clause = "(" + " OR ".join(where_parts) + ")"

    try:
        from src.core.db import get_db
        with get_db() as conn:
            drift_cur = conn.execute(f"""
                UPDATE operator_drift_line
                   SET outcome = ?, outcome_at = ?, outcome_source = ?
                 WHERE {where_clause}
                   AND outcome IS NULL
            """, [outcome, now_iso, src] + params)
            drift_updated = drift_cur.rowcount or 0

            shadow_cur = conn.execute(f"""
                UPDATE operator_drift_shadow
                   SET outcome = ?, outcome_at = ?, outcome_source = ?
                 WHERE {where_clause}
                   AND outcome IS NULL
            """, [outcome, now_iso, src] + params)
            shadow_updated = shadow_cur.rowcount or 0

        log.info(
            "drift_outcome_resolved: quote_id=%s quote_number=%s "
            "outcome=%s drift_rows=%d shadow_rows=%d source=%s",
            quote_id or "—", quote_number or "—", outcome,
            drift_updated, shadow_updated, src,
        )
        return {
            "ok": True,
            "drift_rows_updated": drift_updated,
            "shadow_rows_updated": shadow_updated,
        }
    except Exception as e:
        log.warning("resolve_drift_outcome failed: %s", e)
        return {"ok": False, "error": str(e)}


def get_drift_wr_breakdown(
    window_days: int = 30,
    drift_threshold_pct: float = 20.0,
) -> dict:
    """The leverage query: of lines where drift > threshold, what's the
    WR vs lines where drift <= threshold? Only rows with `outcome` set
    (resolved by `resolve_drift_outcome`) count toward the WR math.

    Returns:
        {
          ok, window_days, threshold,
          resolved_lines, high_drift_lines, low_drift_lines,
          high_drift_won, high_drift_lost, high_drift_wr,
          low_drift_won, low_drift_lost, low_drift_wr,
          wr_delta,   # high_wr - low_wr; negative = high drift hurts
        }
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT drift_pct, outcome
                  FROM operator_drift_line
                 WHERE sent_at >= datetime('now', ?)
                   AND drift_pct IS NOT NULL
                   AND outcome IN ('won','lost')
            """, [f"-{int(window_days)} days"]).fetchall()
    except Exception as e:
        return {"ok": False, "error": str(e)}

    def _wr(won, lost):
        n = won + lost
        return round(100.0 * won / n, 1) if n else None

    high_won = high_lost = low_won = low_lost = 0
    for r in rows:
        d = float(r["drift_pct"])
        is_won = r["outcome"] == "won"
        if d > drift_threshold_pct:
            if is_won:
                high_won += 1
            else:
                high_lost += 1
        else:
            if is_won:
                low_won += 1
            else:
                low_lost += 1

    high_wr = _wr(high_won, high_lost)
    low_wr = _wr(low_won, low_lost)
    wr_delta = (
        round(high_wr - low_wr, 1)
        if (high_wr is not None and low_wr is not None) else None
    )
    return {
        "ok": True, "window_days": window_days,
        "threshold": drift_threshold_pct,
        "resolved_lines": len(rows),
        "high_drift_lines": high_won + high_lost,
        "low_drift_lines": low_won + low_lost,
        "high_drift_won": high_won, "high_drift_lost": high_lost,
        "high_drift_wr": high_wr,
        "low_drift_won": low_won, "low_drift_lost": low_lost,
        "low_drift_wr": low_wr,
        "wr_delta": wr_delta,
    }
