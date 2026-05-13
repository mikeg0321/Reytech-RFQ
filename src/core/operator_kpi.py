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
                 caps_applied_json, cap_sources, scprs_match_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                 rollup_match_key, rollup_match_key_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
