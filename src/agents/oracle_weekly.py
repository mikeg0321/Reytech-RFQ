"""Oracle Weekly Email — surfaces what calibration is learning.

Mike's request 2026-04-27: now that 488 calibrations are loaded
(PR #600 backfill) + Phase 4.7 swap-link is firing on prod, build
the weekly email that shows what the engine is actually learning.

This replaces the 3 stale-data digest emails (deadline, drafts, order)
that were disabled in PR #604. THIS is the email of value because it
reads from data sources that ARE connected and current:

  - quotes table: 7d won/lost outcomes
  - oracle_calibration: per-category × agency markups
  - intel_acceptance_log: swap-link accept/reject telemetry
  - category_intel rollup: live danger/win bucket signal

Sent manually first via POST /api/oracle/weekly-email (dry_run option),
then promoted to a Monday-morning scheduler in a follow-up PR once
the content is verified useful.

Default OFF until manual verification confirms the content.
"""

import logging
from datetime import datetime, timedelta

log = logging.getLogger("oracle_weekly")


def _week_window(week_end: datetime | None = None) -> tuple[str, str]:
    """Return (start_iso, end_iso) for the 7d window ending at week_end
    (default = now). Both ISO 8601, end-exclusive."""
    end = week_end or datetime.now()
    start = end - timedelta(days=7)
    return start.isoformat(), end.isoformat()


def _prev_week_window(week_end: datetime | None = None) -> tuple[str, str]:
    end = week_end or datetime.now()
    return ((end - timedelta(days=14)).isoformat(),
            (end - timedelta(days=7)).isoformat())


def _count_outcomes_in_window(conn, start_iso: str, end_iso: str) -> dict:
    """Count won/lost quotes in the window. Reads from quotes.updated_at."""
    rows = conn.execute("""
        SELECT status, COUNT(*) as n
        FROM quotes
        WHERE is_test = 0
          AND status IN ('won', 'lost')
          AND COALESCE(updated_at, sent_at, '') >= ?
          AND COALESCE(updated_at, sent_at, '') < ?
        GROUP BY status
    """, (start_iso, end_iso)).fetchall()
    counts = {"won": 0, "lost": 0}
    for r in rows:
        counts[r["status"]] = r["n"]
    decided = counts["won"] + counts["lost"]
    counts["decided"] = decided
    counts["win_rate_pct"] = (round(100.0 * counts["won"] / decided, 1)
                              if decided else None)
    return counts


def _live_buckets() -> dict:
    """Pull current danger + win buckets via the live category-intel
    aggregator. Reads quotes table directly — no agency filter (overall
    cross-agency signal). Returns {danger: [...], win: [...]}.
    """
    try:
        from src.api.modules.routes_oracle_category_intel import _aggregate_category
        from src.core.intel_categories import iter_categories
    except Exception as e:
        log.debug("live_buckets import: %s", e)
        return {"danger": [], "win": []}

    danger = []
    win = []
    seen = set()
    try:
        # iter_categories returns ALL category labels; we probe one
        # description per category to get its bucket stats.
        for cat_id, label in iter_categories():
            if cat_id in seen:
                continue
            seen.add(cat_id)
            # Use the category's label as the seed description so the
            # classifier returns this same cat_id.
            try:
                result = _aggregate_category(label)
            except Exception:
                continue
            quotes = result.get("quotes", 0)
            if quotes < 5:  # MIN_QUOTES_FOR_DANGER floor
                continue
            row = {
                "category": cat_id,
                "label": label,
                "quotes": quotes,
                "wins": result.get("wins", 0),
                "win_rate_pct": result.get("win_rate_pct"),
            }
            if result.get("danger"):
                danger.append(row)
            elif (result.get("win_rate_pct") or 0) >= 50:
                win.append(row)
    except Exception as e:
        log.warning("live_buckets walk failed: %s", e)

    danger.sort(key=lambda d: d.get("win_rate_pct") or 0)
    win.sort(key=lambda d: -(d.get("win_rate_pct") or 0))
    return {"danger": danger, "win": win[:5]}


def _swap_link_telemetry(conn, start_iso: str, end_iso: str) -> dict:
    """Pull intel_acceptance_log activity for the window."""
    try:
        rows = conn.execute("""
            SELECT category, accepted, COUNT(*) as n
            FROM intel_acceptance_log
            WHERE recorded_at >= ? AND recorded_at < ?
            GROUP BY category, accepted
        """, (start_iso, end_iso)).fetchall()
    except Exception as e:
        log.debug("swap_link_telemetry: %s", e)
        return {"offered": 0, "accepted": 0, "rejected": 0,
                "accept_rate_pct": None, "by_category": []}

    offered = accepted = rejected = 0
    by_cat: dict = {}
    for r in rows:
        cat = r["category"] or "uncategorized"
        n = r["n"]
        offered += n
        if r["accepted"]:
            accepted += n
        else:
            rejected += n
        b = by_cat.setdefault(cat, {"accepted": 0, "rejected": 0})
        if r["accepted"]:
            b["accepted"] += n
        else:
            b["rejected"] += n

    rate = round(100.0 * accepted / offered, 1) if offered else None
    # Surface the most-rejected category (best learning signal)
    cat_rows = [
        {"category": c, "accepted": v["accepted"], "rejected": v["rejected"],
         "total": v["accepted"] + v["rejected"]}
        for c, v in by_cat.items()
    ]
    cat_rows.sort(key=lambda d: -d["rejected"])
    return {"offered": offered, "accepted": accepted, "rejected": rejected,
            "accept_rate_pct": rate, "by_category": cat_rows[:5]}


def _calibration_activity(conn, start_iso: str, end_iso: str) -> dict:
    """Calibration rows updated this week — what learned what."""
    try:
        rows = conn.execute("""
            SELECT category, agency, sample_size, win_count, loss_on_price,
                   loss_on_other, avg_winning_margin, last_updated
            FROM oracle_calibration
            WHERE last_updated >= ? AND last_updated < ?
            ORDER BY last_updated DESC
            LIMIT 20
        """, (start_iso, end_iso)).fetchall()
    except Exception as e:
        log.debug("calibration_activity: %s", e)
        return {"count": 0, "rows": []}
    return {
        "count": len(rows),
        "rows": [dict(r) for r in rows],
    }


def build_weekly_report(week_end: datetime | None = None) -> dict:
    """Aggregate the 7d signal across all four data sources.

    Returns a dict with `subject`, `body`, plus all sub-reports for testing.
    """
    end = week_end or datetime.now()
    start_iso, end_iso = _week_window(end)
    prev_start, prev_end = _prev_week_window(end)

    from src.core.db import get_db
    with get_db() as conn:
        this_week = _count_outcomes_in_window(conn, start_iso, end_iso)
        last_week = _count_outcomes_in_window(conn, prev_start, prev_end)
        swap = _swap_link_telemetry(conn, start_iso, end_iso)
        calib = _calibration_activity(conn, start_iso, end_iso)
    buckets = _live_buckets()

    # Week-over-week delta
    wow_delta = None
    if (this_week["win_rate_pct"] is not None
            and last_week["win_rate_pct"] is not None):
        wow_delta = round(this_week["win_rate_pct"] - last_week["win_rate_pct"], 1)

    range_label = f"{end - timedelta(days=7):%b %d} – {end:%b %d}"
    win_rate_str = (f"{this_week['win_rate_pct']}%"
                    if this_week["win_rate_pct"] is not None else "n/a")
    delta_str = ""
    if wow_delta is not None:
        sign = "+" if wow_delta >= 0 else ""
        delta_str = f" ({sign}{wow_delta}pp vs prev week)"

    subject = f"🧠 Oracle Weekly — {range_label} — win rate {win_rate_str}"

    # ─────── Body ───────
    lines = []
    lines.append(f"Oracle Weekly — {range_label}")
    lines.append("=" * 60)
    lines.append("")
    lines.append("THIS WEEK")
    lines.append(f"  Wins: {this_week['won']} | Losses: {this_week['lost']} "
                 f"| Win rate: {win_rate_str}{delta_str}")
    lines.append("")

    lines.append("CALIBRATION ACTIVITY")
    if calib["count"] == 0:
        lines.append("  No calibration writes this week.")
    else:
        lines.append(f"  {calib['count']} category × agency cells updated.")
        for r in calib["rows"][:5]:
            cat = r.get("category", "?")
            agency = r.get("agency", "") or "(any)"
            margin = r.get("avg_winning_margin", 25)
            wins = r.get("win_count", 0)
            samp = r.get("sample_size", 0)
            lines.append(f"    • {cat} × {agency}: "
                         f"{margin:.1f}% margin (W{wins}/N{samp})")
    lines.append("")

    lines.append("LIVE DANGER BUCKETS (engine suggests damped markup)")
    if not buckets["danger"]:
        lines.append("  None active — no category below 15% on n≥5.")
    else:
        for b in buckets["danger"]:
            lines.append(f"  🔴 {b['label']}: {b['win_rate_pct']}% "
                         f"({b['wins']}/{b['quotes']} won)")
    lines.append("")

    lines.append("LIVE WIN BUCKETS (confident territory)")
    if not buckets["win"]:
        lines.append("  None active — no category at/above 50% on n≥5.")
    else:
        for b in buckets["win"]:
            lines.append(f"  🟢 {b['label']}: {b['win_rate_pct']}% "
                         f"({b['wins']}/{b['quotes']} won)")
    lines.append("")

    lines.append("SWAP-LINK TELEMETRY (Phase 4.7 Flavor B)")
    if swap["offered"] == 0:
        lines.append("  No suggestions offered this week.")
    else:
        rate = swap["accept_rate_pct"]
        lines.append(f"  Offered: {swap['offered']} | Accepted: "
                     f"{swap['accepted']} | Rejected: {swap['rejected']} "
                     f"| Accept rate: {rate}%")
        if swap["by_category"]:
            top = swap["by_category"][0]
            if top["rejected"] > 0:
                lines.append(f"  Most rejected: {top['category']} "
                             f"({top['rejected']}/{top['total']} declined) "
                             f"— review damping factor.")
    lines.append("")

    lines.append("WEEK-OVER-WEEK")
    last_rate = (f"{last_week['win_rate_pct']}%"
                 if last_week["win_rate_pct"] is not None else "n/a")
    lines.append(f"  This week win rate: {win_rate_str}")
    lines.append(f"  Last week win rate: {last_rate}")
    lines.append("")

    lines.append("— Reytech Oracle (run `/api/oracle/weekly-email` to refresh)")

    body = "\n".join(lines)

    return {
        "ok": True,
        "subject": subject,
        "body": body,
        "this_week": this_week,
        "last_week": last_week,
        "wow_delta_pp": wow_delta,
        "buckets": buckets,
        "swap_link": swap,
        "calibration": calib,
    }


def send_weekly_email(week_end: datetime | None = None,
                     dry_run: bool = False,
                     to_override: str | None = None) -> dict:
    """Build the report and send it via gmail_api. Returns status dict."""
    report = build_weekly_report(week_end=week_end)
    if not report.get("ok"):
        return {"ok": False, "error": "build failed",
                "details": report.get("error")}

    if dry_run:
        return {"ok": True, "dry_run": True, "subject": report["subject"],
                "body": report["body"]}

    import os
    notify_email = (to_override or os.environ.get("NOTIFY_EMAIL", "")
                    or os.environ.get("GMAIL_ADDRESS", ""))
    if not notify_email:
        return {"ok": False, "error": "NOTIFY_EMAIL or GMAIL_ADDRESS not set"}

    try:
        from src.core import gmail_api
        if not gmail_api.is_configured():
            return {"ok": False, "error": "gmail_api not configured"}
        service = gmail_api.get_send_service()
        gmail_api.send_message(
            service,
            to=notify_email,
            subject=report["subject"],
            body_plain=report["body"],
        )
    except Exception as e:
        log.error("oracle_weekly send failed: %s", e, exc_info=True)
        return {"ok": False, "error": str(e)}

    log.info("Oracle weekly sent to %s — subject: %s",
             notify_email, report["subject"])
    return {"ok": True, "to": notify_email, "subject": report["subject"]}
