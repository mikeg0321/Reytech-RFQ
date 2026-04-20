"""
Oracle V3 Weekly Report — Calibration Health + Win/Loss Intelligence

Three responsibilities:
1. Retroactive seeder: process historical wins/losses into oracle_calibration
2. Weekly email report: summarize calibration changes, wins, losses
3. Scheduled runner: fire weekly via scheduler + heartbeat
"""
import logging
import json
from datetime import datetime, timedelta

log = logging.getLogger("reytech.oracle_report")

_REPORT_INTERVAL_SEC = 7 * 24 * 3600  # 1 week
_scheduler_started = False


def _calibration_why(*, wins: int, losses_total: int, win_rate: int) -> str:
    """Human-readable explanation for an oracle_calibration row.

    The old copy said 'Aggressive reduction — too many losses' whenever
    win_rate < 40 — including rows where 0 wins had been *captured* vs.
    actually *lost*. On 2026-04-20 prod had 3 calibration rows totaling
    47 losses and 0 wins; every one of them was displayed as 'too many
    losses' even though the real story was: no win-capture pipeline
    exists, so the data is structurally loss-biased.

    Returns phrasing that separates "loss-heavy signal" (we competed,
    we lost) from "loss-only signal" (we have no way to record wins).
    """
    if wins == 0 and losses_total == 0:
        return "No outcome data yet — calibration neutral"
    if wins == 0 and losses_total > 0:
        return "⚠ Loss-only data — wins not being captured"
    if win_rate >= 80:
        return "Strong — room for margin"
    if win_rate >= 60:
        return "Healthy — holding steady"
    if win_rate >= 40:
        return "Compressing markup to win more"
    return "Aggressive reduction — too many losses"


# ═══════════════════════════════════════════════════════════════════════════════
# 1. RETROACTIVE SEEDER — process historical outcomes into calibration table
# ═══════════════════════════════════════════════════════════════════════════════

def seed_calibration_from_history():
    """One-time seeder: reads all historical wins/losses and calibrates Oracle V3.
    Safe to run multiple times — calibration is idempotent (EMA converges)."""
    from src.core.db import get_db
    from src.core.pricing_oracle_v2 import calibrate_from_outcome

    stats = {"wins_processed": 0, "losses_processed": 0, "errors": 0}

    try:
        with get_db() as conn:
            # Process WON quotes
            won_rows = conn.execute("""
                SELECT quote_number, status, items_detail,
                       institution, agency, created_at
                FROM quotes WHERE status='won' AND is_test=0
                ORDER BY created_at ASC
            """).fetchall()

            for row in won_rows:
                try:
                    items_json = row[2]
                    if not items_json:
                        continue
                    items = json.loads(items_json) if isinstance(items_json, str) else items_json
                    if not isinstance(items, list) or not items:
                        continue
                    agency = row[3] or row[4] or ""
                    calibrate_from_outcome(items, "won", agency=agency)
                    stats["wins_processed"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    log.debug("Seed win error for %s: %s", row[0], e)

            # Process LOST quotes (from competitor_intel)
            # Try quote items first, fall back to competitor_intel.items_detail or item_summary
            loss_rows = conn.execute("""
                SELECT ci.quote_number, ci.agency, ci.competitor_price,
                       ci.loss_reason_class, q.items_detail,
                       ci.items_detail, ci.item_summary
                FROM competitor_intel ci
                LEFT JOIN quotes q ON q.quote_number = ci.quote_number
                WHERE ci.outcome='lost'
                ORDER BY ci.found_at ASC
            """).fetchall()

            for row in loss_rows:
                try:
                    # Try quote items, then CI items_detail, then build from summary
                    items_json = row[4] or row[5]
                    items = None
                    if items_json:
                        try:
                            items = json.loads(items_json) if isinstance(items_json, str) else items_json
                        except (json.JSONDecodeError, TypeError):
                            items = None
                    if not isinstance(items, list) or not items:
                        # Build minimal item from summary or just use a placeholder
                        summary = row[6] or ""
                        items = [{"description": summary[:200] if summary else "general item", "pricing": {}}]
                    agency = row[1] or ""
                    reason = "price" if row[3] in ("price_higher", "margin_too_high") else "other"
                    calibrate_from_outcome(items, "lost", agency=agency, loss_reason=reason)
                    stats["losses_processed"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    log.debug("Seed loss error for %s: %s", row[0], e)

    except Exception as e:
        log.error("Calibration seeder failed: %s", e, exc_info=True)
        stats["error"] = str(e)

    log.info("Oracle V3 calibration seeded: %s", stats)
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# 2. WEEKLY REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def generate_weekly_report():
    """Build the weekly Oracle intelligence report."""
    from src.core.db import get_db

    now = datetime.now()
    week_ago = (now - timedelta(days=7)).isoformat()
    report = {
        "period_start": week_ago[:10],
        "period_end": now.strftime("%Y-%m-%d"),
        "generated_at": now.isoformat(),
    }

    try:
        with get_db() as conn:
            # Recent wins
            wins = conn.execute("""
                SELECT quote_number, institution, total, po_number, created_at
                FROM quotes WHERE status='won' AND is_test=0
                AND created_at > ? ORDER BY created_at DESC
            """, (week_ago,)).fetchall()
            report["wins"] = [{
                "quote": r[0], "agency": r[1], "total": r[2],
                "po": r[3], "date": (r[4] or "")[:10]
            } for r in wins]
            report["win_count"] = len(wins)
            report["win_revenue"] = sum(r[2] or 0 for r in wins)

            # Recent losses
            losses = conn.execute("""
                SELECT quote_number, competitor_name, competitor_price,
                       our_price, price_delta_pct, agency, loss_reason_class, found_at
                FROM competitor_intel WHERE outcome='lost'
                AND found_at > ? ORDER BY found_at DESC
            """, (week_ago,)).fetchall()
            report["losses"] = [{
                "quote": r[0], "competitor": r[1], "their_price": r[2],
                "our_price": r[3], "delta_pct": r[4], "agency": r[5],
                "reason": r[6], "date": (r[7] or "")[:10]
            } for r in losses]
            report["loss_count"] = len(losses)

            # Calibration table state
            try:
                cal_rows = conn.execute("""
                    SELECT category, agency, sample_size, win_count,
                           loss_on_price, loss_on_other, avg_winning_margin,
                           recommended_max_markup, last_updated
                    FROM oracle_calibration
                    ORDER BY sample_size DESC
                """).fetchall()
                report["calibrations"] = [{
                    "category": r[0], "agency": r[1], "samples": r[2],
                    "wins": r[3], "losses_price": r[4], "losses_other": r[5],
                    "losses_total": (r[4] or 0) + (r[5] or 0),
                    "win_rate": round(r[3] / r[2] * 100) if r[2] > 0 else 0,
                    "avg_win_margin": round(r[6], 1),
                    "rec_max_markup": round(r[7], 1),
                    "last_updated": (r[8] or "")[:10],
                } for r in cal_rows]
            except Exception:
                report["calibrations"] = []

            # Winning prices stats (all time)
            try:
                wp_stats = conn.execute("""
                    SELECT COUNT(*), COUNT(DISTINCT fingerprint),
                           AVG(margin_pct), MIN(recorded_at), MAX(recorded_at)
                    FROM winning_prices
                """).fetchone()
                report["winning_prices_total"] = wp_stats[0] or 0
                report["winning_prices_unique"] = wp_stats[1] or 0
                report["avg_margin_all_time"] = round(wp_stats[2] or 0, 1)
            except Exception:
                report["winning_prices_total"] = 0

            # V4: Recent supplier research action items (cost reduction leads)
            try:
                v4_types = ("contact_mfg_rep", "sign_up_wholesale", "negotiate_volume",
                            "alternative_supplier", "direct_from_mfg")
                placeholders = ",".join("?" for _ in v4_types)
                supplier_leads = conn.execute(f"""
                    SELECT action_type, description, priority, source_quote, created_at
                    FROM action_items
                    WHERE action_type IN ({placeholders})
                    AND created_at > ? AND status='pending'
                    ORDER BY priority DESC, created_at DESC
                """, (*v4_types, week_ago)).fetchall()
                report["supplier_leads"] = [{
                    "type": r[0], "description": r[1], "priority": r[2],
                    "quote": r[3], "date": (r[4] or "")[:10],
                } for r in supplier_leads]
            except Exception:
                report["supplier_leads"] = []

            # Also pull pending action items of all types for the "do this" section
            try:
                pending = conn.execute("""
                    SELECT action_type, description, priority, source_quote, created_at
                    FROM action_items
                    WHERE status='pending'
                    ORDER BY
                        CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                        created_at DESC
                    LIMIT 15
                """).fetchall()
                report["pending_actions"] = [{
                    "type": r[0], "description": r[1], "priority": r[2],
                    "quote": r[3], "date": (r[4] or "")[:10],
                } for r in pending]
            except Exception:
                report["pending_actions"] = []

    except Exception as e:
        log.error("Weekly report generation failed: %s", e, exc_info=True)
        report["error"] = str(e)

    return report


def format_report_email(report):
    """Format the report as HTML email body."""
    wins = report.get("wins", [])
    losses = report.get("losses", [])
    cals = report.get("calibrations", [])

    html = f"""
<div style="font-family:system-ui,sans-serif;max-width:700px;margin:0 auto;color:#e6edf3;background:#0d1117;padding:20px;border-radius:8px">
<h2 style="color:#58a6ff;margin-top:0">Oracle V3 Weekly Intelligence</h2>
<p style="color:#8b949e">{report['period_start']} to {report['period_end']}</p>

<div style="display:flex;gap:20px;margin:16px 0">
  <div style="flex:1;background:#23863622;padding:12px;border-radius:8px;border:1px solid #23863655">
    <div style="font-size:24px;font-weight:700;color:#3fb950">{report.get('win_count', 0)}</div>
    <div style="color:#8b949e;font-size:13px">Wins</div>
    <div style="color:#3fb950;font-size:14px;font-weight:600">${report.get('win_revenue', 0):,.2f}</div>
  </div>
  <div style="flex:1;background:#da363322;padding:12px;border-radius:8px;border:1px solid #da363355">
    <div style="font-size:24px;font-weight:700;color:#f85149">{report.get('loss_count', 0)}</div>
    <div style="color:#8b949e;font-size:13px">Losses</div>
  </div>
  <div style="flex:1;background:#1f6feb22;padding:12px;border-radius:8px;border:1px solid #1f6feb55">
    <div style="font-size:24px;font-weight:700;color:#58a6ff">{report.get('winning_prices_total', 0)}</div>
    <div style="color:#8b949e;font-size:13px">Data Points</div>
    <div style="color:#8b949e;font-size:12px">{report.get('avg_margin_all_time', 0)}% avg margin</div>
  </div>
</div>
"""

    # Wins detail
    if wins:
        html += '<h3 style="color:#3fb950;margin-top:20px">Wins This Week</h3>'
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        html += '<tr style="color:#8b949e;border-bottom:1px solid #30363d"><th style="text-align:left;padding:4px">Quote</th><th>Agency</th><th style="text-align:right">Revenue</th><th>PO</th></tr>'
        for w in wins[:10]:
            html += f'<tr style="border-bottom:1px solid #21262d"><td style="padding:4px;color:#e6edf3">{w["quote"]}</td><td style="color:#8b949e">{w["agency"]}</td><td style="text-align:right;color:#3fb950;font-weight:600">${w["total"]:,.2f}</td><td style="color:#8b949e">{w["po"] or "—"}</td></tr>'
        html += '</table>'

    # Losses detail
    if losses:
        html += '<h3 style="color:#f85149;margin-top:20px">Losses This Week</h3>'
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        html += '<tr style="color:#8b949e;border-bottom:1px solid #30363d"><th style="text-align:left;padding:4px">Quote</th><th>Competitor</th><th style="text-align:right">Delta</th><th>Reason</th></tr>'
        for l in losses[:10]:
            delta = f"+{l['delta_pct']:.1f}%" if l.get("delta_pct") else "—"
            html += f'<tr style="border-bottom:1px solid #21262d"><td style="padding:4px;color:#e6edf3">{l["quote"]}</td><td style="color:#f0883e">{l["competitor"]}</td><td style="text-align:right;color:#f85149">{delta}</td><td style="color:#8b949e;font-size:12px">{l.get("reason","")}</td></tr>'
        html += '</table>'

    # Calibration state
    if cals:
        html += '<h3 style="color:#d29922;margin-top:20px">Oracle Calibration State</h3>'
        html += '<p style="color:#8b949e;font-size:12px">How the algorithm is adjusting based on outcomes:</p>'
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        html += '<tr style="color:#8b949e;border-bottom:1px solid #30363d"><th style="text-align:left;padding:4px">Category</th><th>Samples</th><th>Win Rate</th><th>Avg Win Margin</th><th>Max Markup</th><th>Why</th></tr>'
        for c in cals:
            wr_color = "#3fb950" if c["win_rate"] >= 70 else ("#d29922" if c["win_rate"] >= 40 else "#f85149")
            why = _calibration_why(
                wins=c.get("wins", 0),
                losses_total=c.get("losses_total", c.get("losses_price", 0)),
                win_rate=c["win_rate"],
            )
            html += f'<tr style="border-bottom:1px solid #21262d"><td style="padding:4px;color:#e6edf3;text-transform:capitalize">{c["category"]}</td><td style="color:#8b949e">{c["samples"]}</td><td style="color:{wr_color};font-weight:600">{c["win_rate"]}%</td><td style="color:#8b949e">{c["avg_win_margin"]}%</td><td style="color:#58a6ff;font-weight:600">{c["rec_max_markup"]}%</td><td style="color:#8b949e;font-size:11px">{why}</td></tr>'
        html += '</table>'

    if not wins and not losses:
        html += '<p style="color:#8b949e;margin-top:16px">No win/loss activity this week. Oracle calibration unchanged.</p>'

    # V4: Supplier Research Leads
    supplier_leads = report.get("supplier_leads", [])
    if supplier_leads:
        html += '<h3 style="color:#f0883e;margin-top:20px">🔍 Do This for Better Pricing</h3>'
        html += '<p style="color:#8b949e;font-size:12px;margin-bottom:8px">AI-researched supplier leads from recent losses. Act on these to lower your cost basis:</p>'
        _type_labels = {
            "contact_mfg_rep": "📞 Contact MFG Rep",
            "sign_up_wholesale": "🏪 Sign Up Wholesale",
            "negotiate_volume": "📦 Negotiate Volume",
            "alternative_supplier": "🔄 Alt Supplier",
            "direct_from_mfg": "🏭 Direct from MFG",
        }
        for lead in supplier_leads[:10]:
            prio_color = "#3fb950" if lead["priority"] == "high" else ("#d29922" if lead["priority"] == "medium" else "#8b949e")
            type_label = _type_labels.get(lead["type"], lead["type"])
            html += f'<div style="padding:8px 12px;margin-bottom:4px;background:#21262d;border-radius:6px;border-left:3px solid {prio_color}">'
            html += f'<div style="font-size:11px;color:{prio_color};font-weight:600;text-transform:uppercase;margin-bottom:2px">{type_label} · {lead["priority"]}</div>'
            html += f'<div style="color:#e6edf3;font-size:13px">{lead["description"]}</div>'
            if lead.get("quote"):
                html += f'<div style="color:#484f58;font-size:11px;margin-top:2px">From: {lead["quote"]} · {lead["date"]}</div>'
            html += '</div>'

    # Pending Action Items (all types)
    pending = report.get("pending_actions", [])
    if pending:
        html += '<h3 style="color:#a78bfa;margin-top:20px">📋 Pending Action Items</h3>'
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        html += '<tr style="color:#8b949e;border-bottom:1px solid #30363d"><th style="text-align:left;padding:4px">Priority</th><th style="text-align:left">Action</th><th>Quote</th></tr>'
        for a in pending[:15]:
            prio_color = "#f85149" if a["priority"] == "high" else ("#d29922" if a["priority"] == "medium" else "#8b949e")
            desc_short = a["description"][:120] + ("..." if len(a["description"]) > 120 else "")
            html += f'<tr style="border-bottom:1px solid #21262d"><td style="padding:4px;color:{prio_color};font-weight:600;text-transform:uppercase;font-size:11px">{a["priority"]}</td><td style="padding:4px;color:#c9d1d9;font-size:12px">{desc_short}</td><td style="padding:4px;color:#8b949e;font-size:11px">{a.get("quote","")}</td></tr>'
        html += '</table>'

    html += f"""
<div style="margin-top:20px;padding-top:12px;border-top:1px solid #30363d;color:#484f58;font-size:11px">
Oracle V4 Self-Calibrating Pricing + AI Supplier Discovery — Generated {report['generated_at'][:16]}<br>
Learning rate: alpha=0.15 | Markup floor: 15% | Ceiling: 50% | Min samples: 5
</div>
</div>
"""
    return html


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SCHEDULED RUNNER + HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

def run_weekly_report():
    """Generate and send the weekly Oracle report. Called by scheduler."""
    from src.core.scheduler import heartbeat

    try:
        report = generate_weekly_report()
        html_body = format_report_email(report)

        plain = (
            f"Oracle V4 Weekly: {report['period_start']} to {report['period_end']}\n"
            f"Wins: {report.get('win_count', 0)} (${report.get('win_revenue', 0):,.2f})\n"
            f"Losses: {report.get('loss_count', 0)}\n"
            f"Supplier leads: {len(report.get('supplier_leads', []))}\n"
            f"Data points: {report.get('winning_prices_total', 0)}\n"
            f"Calibrations active: {len(report.get('calibrations', []))}"
        )

        from src.agents.notify_agent import send_alert
        result = send_alert(
            event_type="oracle_weekly",
            title=f"Oracle Weekly: {report.get('win_count', 0)}W / {report.get('loss_count', 0)}L",
            body=plain,
            urgency="info",
            channels=["email"],
            context={"html_body": html_body},
            run_async=False,
        )

        # Record last successful send for the forcing function
        _record_send_status(True, report)

        if not result.get("ok"):
            # Email failed to send — fire backup alert via dashboard bell
            log.error("Oracle weekly email FAILED: %s", result.get("error", "unknown"))
            send_alert(
                event_type="oracle_weekly_failed",
                title="Oracle weekly email failed to send",
                body=f"Email send returned: {result}. Check GMAIL_ADDRESS/GMAIL_PASSWORD env vars.",
                urgency="warning",
                channels=["bell"],
                run_async=False,
            )
            heartbeat("oracle-weekly-report", success=False, error="Email send failed")
            return {"ok": False, "error": "Email send failed", "report": report}

        heartbeat("oracle-weekly-report", success=True)
        log.info("Oracle weekly report sent: %s wins, %s losses",
                 report.get("win_count", 0), report.get("loss_count", 0))
        return {"ok": True, "report": report, "email": result}

    except Exception as e:
        log.error("Oracle weekly report failed: %s", e, exc_info=True)
        _record_send_status(False, error=str(e))
        heartbeat("oracle-weekly-report", success=False, error=str(e))
        # Fire backup alert
        try:
            from src.agents.notify_agent import send_alert
            send_alert(
                event_type="oracle_weekly_failed",
                title="ORACLE WEEKLY REPORT CRASHED",
                body=f"Error: {e}. The Oracle feedback loop is broken. Investigate immediately.",
                urgency="urgent",
                channels=["bell"],
                run_async=False,
            )
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        return {"ok": False, "error": str(e)}


def _record_send_status(success, report=None, error=None):
    """Record weekly report send status to DB for the forcing function."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS oracle_report_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sent_at TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    win_count INTEGER DEFAULT 0,
                    loss_count INTEGER DEFAULT 0,
                    supplier_leads INTEGER DEFAULT 0,
                    calibrations INTEGER DEFAULT 0,
                    error TEXT DEFAULT ''
                )
            """)
            conn.execute("""
                INSERT INTO oracle_report_log (sent_at, success, win_count, loss_count,
                    supplier_leads, calibrations, error)
                VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)
            """, (
                1 if success else 0,
                report.get("win_count", 0) if report else 0,
                report.get("loss_count", 0) if report else 0,
                len(report.get("supplier_leads", [])) if report else 0,
                len(report.get("calibrations", [])) if report else 0,
                error or "",
            ))
    except Exception as e:
        log.debug("Report log error: %s", e)


def check_report_health():
    """Forcing function: verify the weekly report is actually running.
    Called by the scheduler watchdog. If last successful send was >9 days ago,
    fires an urgent alert."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            try:
                row = conn.execute("""
                    SELECT sent_at, success FROM oracle_report_log
                    WHERE success=1 ORDER BY sent_at DESC LIMIT 1
                """).fetchone()
            except Exception:
                row = None

            if not row:
                # No successful send ever — might be first week
                return

            last_sent = row[0]
            try:
                last_dt = datetime.fromisoformat(last_sent)
                days_since = (datetime.now() - last_dt).days
            except Exception:
                days_since = 99

            if days_since > 9:
                # Missed a weekly send — fire urgent alert
                log.error("FORCING FUNCTION: Oracle weekly report overdue by %d days!", days_since)
                from src.agents.notify_agent import send_alert
                send_alert(
                    event_type="oracle_weekly_overdue",
                    title=f"Oracle weekly report OVERDUE ({days_since} days)",
                    body=(
                        f"Last successful Oracle weekly report was {days_since} days ago ({last_sent}). "
                        f"The feedback loop may be broken. Check:\n"
                        f"1. Is the oracle-weekly-report thread alive?\n"
                        f"2. Are GMAIL_ADDRESS/GMAIL_PASSWORD env vars set?\n"
                        f"3. Manual trigger: POST /api/oracle/weekly-report"
                    ),
                    urgency="urgent",
                    channels=["email", "bell"],
                    cooldown_key="oracle_overdue",
                    run_async=False,
                )
    except Exception as e:
        log.debug("Report health check error: %s", e)


def start_weekly_reporter():
    """Start the weekly report scheduler. Called once at app boot."""
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    import threading
    from src.core.scheduler import register_job

    # Register with a 1-day heartbeat interval so watchdog detects death within 24h
    register_job("oracle-weekly-report", 86400)

    def _loop():
        import time
        # Wait 60s after boot before first check
        time.sleep(60)
        # Seed calibration on first boot if table is empty
        try:
            from src.core.db import get_db
            with get_db() as conn:
                try:
                    count = conn.execute("SELECT COUNT(*) FROM oracle_calibration").fetchone()[0]
                except Exception:
                    count = 0
            if count == 0:
                log.info("Oracle V3: seeding calibration from historical data...")
                stats = seed_calibration_from_history()
                log.info("Oracle V3 seeded: %s", stats)
        except Exception as e:
            log.warning("Oracle V3 seed failed: %s", e)

        while True:
            try:
                from src.core.scheduler import heartbeat
                now = datetime.now()

                # Monday 8am PST (15 UTC) — send the weekly report
                if now.weekday() == 0 and now.hour == 15:
                    run_weekly_report()
                    time.sleep(3600)  # Don't re-trigger for an hour
                else:
                    # Heartbeat every cycle so watchdog knows thread is alive
                    heartbeat("oracle-weekly-report", success=True)

                    # Thursday — mid-week forcing function check
                    if now.weekday() == 3 and now.hour == 15:
                        check_report_health()

                    time.sleep(1800)  # Check every 30 minutes
            except Exception as e:
                log.error("Oracle weekly loop error: %s", e)
                try:
                    from src.core.scheduler import heartbeat as _hb
                    _hb("oracle-weekly-report", success=False, error=str(e))
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
                time.sleep(3600)

    t = threading.Thread(target=_loop, daemon=True, name="oracle-weekly-report")
    t.start()
    log.info("Oracle V4 weekly reporter started (Mondays 8am PST, health check Thursdays)")
