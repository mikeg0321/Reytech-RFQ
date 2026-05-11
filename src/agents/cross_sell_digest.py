"""Cross-sell weekly digest — email Mike the top prospects + recommendations.

Mike P0 2026-05-11 needle-mover #2 Phase 2b. PR #901 shipped the intel
module + API; this module wires it to delivery so Mike gets the digest
via email weekly (Monday 8am PT) WITHOUT having to open the app.

Mirrors `oracle_weekly_report.py` pattern: scheduler thread + heartbeat
+ notify_agent.send_alert(channels=["email"]). Cooldown key prevents
double-sends on scheduler restart within the hour window.

Phase 2c-1 (2026-05-11): pivoted framing from "cold outreach prospects"
to "distribution list candidates". Mike's feedback on first digest was
that buyers already-served showed up + 4-month-old data isn't actionable
as cold-email signal; the substrate action is procurement-portal vendor
registration. Intel module now filters known Reytech customers; digest
column header + intro paragraph reflect the new verb.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

log = logging.getLogger(__name__)

_scheduler_started = False


def _fmt_money(amount: float | int | None) -> str:
    if amount is None:
        return "$0"
    try:
        return f"${float(amount):,.0f}"
    except (TypeError, ValueError):
        return "$0"


def _fmt_buyer_label(prospect: dict) -> str:
    name = (prospect.get("buyer_name") or "").strip()
    email = (prospect.get("buyer_email") or "").strip()
    if name and email and name != email:
        return f"{name} &lt;{email}&gt;"
    return name or email or "(unknown buyer)"


_FRESHNESS_BADGES = {
    "fresh":   ("#3fb950", "FRESH"),   # ≤30d — green, active buying cycle
    "warm":    ("#4f8cff", "warm"),    # ≤90d — blue, recent
    "stale":   ("#d29922", "stale"),   # ≤180d — amber, registration still valid
    "old":     ("#8b949e", "old"),     # ≤365d — gray, historical
    "unknown": ("#8b949e", "?"),
}


def _fmt_freshness_badge(tier: str | None) -> str:
    color, label = _FRESHNESS_BADGES.get(tier or "unknown", _FRESHNESS_BADGES["unknown"])
    return (
        f"<span style='display:inline-block;padding:2px 7px;border-radius:10px;"
        f"background:{color};color:#fff;font-size:11px;font-weight:600'>{label}</span>"
    )


def build_digest_body(window_days: int = 90, top_n: int = 10) -> dict:
    """Build the digest payload (text + html) for the configured window.

    Returns:
      {
        "ok": True,
        "plain": str,            # plain-text fallback
        "html": str,             # HTML body
        "prospect_count": int,
        "headline_count": int,   # number of recommendation bullets
        "period_days": int,
      }
    """
    from src.agents.cross_sell_intel import (
        get_prospects, get_top_items_by_spend, get_general_recommendations,
    )
    prospects = get_prospects(top_n=top_n, days_back=window_days)
    items = get_top_items_by_spend(top_n=10, days_back=window_days)
    recs = get_general_recommendations(days_back=window_days)

    # ── Plain-text fallback ────────────────────────────────────────────
    lines = [
        f"Cross-sell distribution-list candidates — last {window_days} days",
        f"{len(prospects)} buyer(s) not yet on Reytech's customer list · "
        f"{len(items)} categories surfaced",
        "",
        "Goal: get Reytech onto each buyer's agency procurement distribution",
        "list so future solicitations route to you. Cold outreach on old POs",
        "won't outperform being on the standing vendor list.",
        "",
        "── Recommendations ──",
    ]
    for b in (recs.get("bullets") or []):
        lines.append(f"  • {b.get('headline', '')}")
        lines.append(f"      → {b.get('action', '')}")
    lines.append("")
    lines.append("── Distribution-list candidates ──")
    for i, p in enumerate(prospects, start=1):
        comp = ", ".join(c for c in (p.get("competitors") or [])[:2] if c) or "?"
        sku = ", ".join(s for s in (p.get("skus") or [])[:3] if s) or "?"
        days = p.get("days_since_last_po")
        days_str = f"{days}d ago" if days is not None else "unknown date"
        fresh = (p.get("freshness") or "unknown").upper()
        lines.append(
            f"  {i:2}. [{fresh}] {_fmt_money(p.get('competitor_spend'))} — "
            f"{p.get('buyer_name') or p.get('buyer_email')} "
            f"({sku} from {comp}, {days_str})"
        )
    plain = "\n".join(lines)

    # ── HTML body ──────────────────────────────────────────────────────
    html_parts = [
        "<html><body style='font-family:Arial,Helvetica,sans-serif;color:#1a1a1a'>",
        "<h2 style='margin:0 0 6px;color:#4f8cff'>Distribution-list candidates"
        " — weekly digest</h2>",
        f"<div style='color:#666;font-size:13px;margin-bottom:6px'>"
        f"Last {window_days} days · "
        f"{len(prospects)} buyer(s) NOT yet a Reytech customer · "
        f"{len(items)} category(ies)</div>",
        "<div style='background:#f6f8fa;border-left:3px solid #4f8cff;"
        "padding:8px 12px;margin-bottom:14px;color:#444;font-size:12px;"
        "line-height:1.5'>"
        "<b>Goal:</b> get Reytech onto each agency's vendor distribution "
        "list so future solicitations route to you. Cold outreach on POs "
        "this old won't outperform being on the standing list. The "
        "<span style='color:#3fb950;font-weight:600'>FRESH</span> tag "
        "marks buyers active in the last 30 days — priority registration "
        "targets."
        "</div>",
    ]

    # Recommendations
    if recs.get("bullets"):
        html_parts.append("<h3 style='color:#1a1a1a;margin:18px 0 8px'>Recommendations</h3>")
        html_parts.append("<ul style='padding-left:18px;line-height:1.5'>")
        for b in recs["bullets"]:
            html_parts.append(
                f"<li><b>{b.get('headline', '')}</b><br>"
                f"<span style='color:#555'>→ {b.get('action', '')}</span></li>"
            )
        html_parts.append("</ul>")

    # Distribution-list candidates table
    if prospects:
        html_parts.append(
            "<h3 style='color:#1a1a1a;margin:18px 0 8px'>"
            "Distribution-list candidates</h3>"
        )
        html_parts.append(
            "<table style='border-collapse:collapse;width:100%;font-size:13px'>"
            "<tr style='background:#f0f4ff'>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>#</th>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>Freshness</th>"
            "<th style='text-align:right;padding:6px 8px;border-bottom:2px solid #4f8cff'>Spend</th>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>Buyer</th>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>Agency</th>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>SKUs</th>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>Competitor(s)</th>"
            "<th style='text-align:right;padding:6px 8px;border-bottom:2px solid #4f8cff'>Last buy</th>"
            "</tr>"
        )
        for i, p in enumerate(prospects, start=1):
            comp = ", ".join(c for c in (p.get("competitors") or [])[:2] if c) or "?"
            sku = ", ".join(s for s in (p.get("skus") or [])[:3] if s) or "?"
            agency = (p.get("dept_name") or "").strip() or "?"
            days = p.get("days_since_last_po")
            days_str = f"{days}d" if days is not None else "?"
            html_parts.append(
                f"<tr><td style='padding:6px 8px'>{i}</td>"
                f"<td style='padding:6px 8px'>{_fmt_freshness_badge(p.get('freshness'))}</td>"
                f"<td style='padding:6px 8px;text-align:right;font-family:monospace;font-weight:600;color:#3fb950'>"
                f"{_fmt_money(p.get('competitor_spend'))}</td>"
                f"<td style='padding:6px 8px'>{_fmt_buyer_label(p)}</td>"
                f"<td style='padding:6px 8px;color:#444;font-size:12px'>{agency}</td>"
                f"<td style='padding:6px 8px;font-family:monospace;font-size:12px'>{sku}</td>"
                f"<td style='padding:6px 8px;color:#888'>{comp}</td>"
                f"<td style='padding:6px 8px;text-align:right;font-family:monospace;color:#888'>{days_str}</td>"
                "</tr>"
            )
        html_parts.append("</table>")

    # Top categories rollup
    if items:
        html_parts.append("<h3 style='color:#1a1a1a;margin:18px 0 8px'>Top categories</h3>")
        html_parts.append(
            "<table style='border-collapse:collapse;width:100%;font-size:13px'>"
            "<tr style='background:#f0f4ff'>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>Category</th>"
            "<th style='text-align:left;padding:6px 8px;border-bottom:2px solid #4f8cff'>SKU</th>"
            "<th style='text-align:right;padding:6px 8px;border-bottom:2px solid #4f8cff'>Competitor spend</th>"
            "<th style='text-align:right;padding:6px 8px;border-bottom:2px solid #4f8cff'>Distinct buyers</th>"
            "</tr>"
        )
        for it in items:
            html_parts.append(
                "<tr>"
                f"<td style='padding:6px 8px'>{it.get('category', '')}</td>"
                f"<td style='padding:6px 8px;font-family:monospace'>{it.get('reytech_sku') or '-'}</td>"
                f"<td style='padding:6px 8px;text-align:right;font-family:monospace;font-weight:600;color:#3fb950'>"
                f"{_fmt_money(it.get('competitor_spend'))}</td>"
                f"<td style='padding:6px 8px;text-align:right;font-family:monospace'>"
                f"{it.get('distinct_buyers', 0)}</td>"
                "</tr>"
            )
        html_parts.append("</table>")

    html_parts.append(
        "<div style='margin-top:18px;color:#888;font-size:11px'>"
        "Open the app for full prospect list + per-prospect outreach drafts."
        "</div></body></html>"
    )
    html = "\n".join(html_parts)

    return {
        "ok": True,
        "plain": plain,
        "html": html,
        "prospect_count": len(prospects),
        "headline_count": len(recs.get("bullets") or []),
        "period_days": window_days,
    }


def send_weekly_digest(window_days: int = 90, top_n: int = 10) -> dict:
    """Build and send the cross-sell weekly digest email.

    Idempotent within the cooldown window (one send per cooldown_key
    per N hours, enforced by notify_agent). Safe to call from the
    scheduler or manually for testing.
    """
    from src.core.scheduler import heartbeat
    try:
        digest = build_digest_body(window_days=window_days, top_n=top_n)
    except Exception as e:
        log.error("Cross-sell digest build failed: %s", e, exc_info=True)
        try:
            heartbeat("cross-sell-weekly-digest", success=False, error=str(e))
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return {"ok": False, "error": f"build failed: {e}"}

    if digest["prospect_count"] == 0:
        log.info("Cross-sell digest: 0 prospects this period; skipping send")
        try:
            heartbeat("cross-sell-weekly-digest", success=True)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return {"ok": True, "skipped": "no_prospects"}

    try:
        from src.agents.notify_agent import send_alert
        result = send_alert(
            event_type="cross_sell_weekly",
            title=(
                f"Distribution-list candidates ({digest['prospect_count']} "
                f"new buyer(s))"
            ),
            body=digest["plain"],
            urgency="info",
            channels=["email"],
            context={"html_body": digest["html"]},
            cooldown_key="cross_sell_weekly",
            run_async=False,
        )
        try:
            heartbeat("cross-sell-weekly-digest", success=bool(result.get("ok")))
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        log.info("Cross-sell digest sent: %d prospects, send=%s",
                 digest["prospect_count"], result.get("ok"))
        return {"ok": bool(result.get("ok")), "digest": digest, "email": result}
    except Exception as e:
        log.error("Cross-sell digest send failed: %s", e, exc_info=True)
        try:
            heartbeat("cross-sell-weekly-digest", success=False, error=str(e)[:200])
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return {"ok": False, "error": f"send failed: {e}"}


def start_weekly_digest_scheduler():
    """Register + start the cross-sell weekly digest scheduler thread.

    Cadence matches oracle_weekly_report: Monday 8am PT (15 UTC). The
    loop wakes every 30 min to check the wall clock and avoid
    re-triggering within the hour.

    Call once at app boot. Re-calls are no-op (idempotent via
    `_scheduler_started` guard).
    """
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    try:
        from src.core.scheduler import register_job
        # 1-day heartbeat — watchdog detects thread death within 24h.
        register_job("cross-sell-weekly-digest", 86400)
    except Exception as e:
        log.warning("cross_sell_digest: register_job failed (continuing): %s", e)

    def _loop():
        # Wait 90s after boot before first check (gives DB + agents time
        # to settle and avoids competing with other startup tasks).
        time.sleep(90)
        last_send_hour = None
        while True:
            try:
                from src.core.scheduler import heartbeat
                now = datetime.now()
                # Monday = weekday 0. 8am PT = 15 UTC most of the year;
                # accept 15 or 16 UTC to handle DST transitions cleanly.
                is_send_window = (
                    now.weekday() == 0 and now.hour in (15, 16)
                )
                send_key = (now.year, now.month, now.day, now.hour)
                if is_send_window and last_send_hour != send_key:
                    log.info("cross_sell_digest: send window hit, dispatching")
                    send_weekly_digest()
                    last_send_hour = send_key
                else:
                    heartbeat("cross-sell-weekly-digest", success=True)
                time.sleep(1800)  # check every 30 min
            except Exception as e:
                log.error("cross_sell_digest loop error: %s", e, exc_info=True)
                try:
                    from src.core.scheduler import heartbeat as _hb
                    _hb("cross-sell-weekly-digest", success=False, error=str(e)[:200])
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
                time.sleep(3600)

    t = threading.Thread(target=_loop, daemon=True, name="cross-sell-weekly-digest")
    t.start()
    log.info(
        "Cross-sell weekly digest scheduler started "
        "(Mondays 8am PT, heartbeat every 30min)"
    )
