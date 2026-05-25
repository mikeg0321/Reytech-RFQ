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
            # IN-13: A quote may have multiple competitor_intel rows (each
            # competitor that beat us). Without dedupe the same quote gets
            # calibrated N times and losses get inflated Nx in oracle
            # calibration stats. We dedupe by quote_number in Python —
            # cleaner than SQL DISTINCT because we also need to pick one
            # winning row per group, and ORDER BY found_at ASC below means
            # the first row seen per quote is the earliest reported loss.
            loss_rows = conn.execute("""
                SELECT ci.quote_number, ci.agency, ci.competitor_price,
                       ci.loss_reason_class, q.items_detail,
                       ci.items_detail, ci.item_summary
                FROM competitor_intel ci
                LEFT JOIN quotes q ON q.quote_number = ci.quote_number
                WHERE ci.outcome='lost'
                ORDER BY ci.found_at ASC
            """).fetchall()

            _seen_quote_numbers = set()
            for row in loss_rows:
                _qn = row[0]
                if _qn in _seen_quote_numbers:
                    continue
                _seen_quote_numbers.add(_qn)
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
                SELECT id, quote_number, competitor_name, competitor_price,
                       our_price, price_delta_pct, agency, loss_reason_class, found_at
                FROM competitor_intel WHERE outcome='lost'
                AND found_at > ? ORDER BY found_at DESC
            """, (week_ago,)).fetchall()
            report["losses"] = [{
                "id": r[0], "quote": r[1], "competitor": r[2], "their_price": r[3],
                "our_price": r[4], "delta_pct": r[5], "agency": r[6],
                "reason": r[7], "date": (r[8] or "")[:10]
            } for r in losses]
            report["loss_count"] = len(losses)

            # 2026-05-13 (PR-D): per-line loss breakdown. PR-C started
            # persisting line-level deltas to competitor_intel_lines so
            # the digest can show "lost item 4 by 18%, item 7 by 2%,
            # item 9 we were cheaper" instead of just one PO-level row.
            # Attach `lines: [...]` to each loss dict when child rows
            # exist. Losses without child rows (legacy data or awards
            # without `lines` info) stay as-is — back-compat preserved.
            try:
                for L in report["losses"]:
                    if not L.get("id"):
                        L["lines"] = []
                        continue
                    line_rows = conn.execute(
                        """SELECT line_num, scprs_description, scprs_unit_price,
                                  scprs_quantity, scprs_mfg, our_item_idx,
                                  our_unit_price, price_delta_pct, matched_by
                           FROM competitor_intel_lines
                           WHERE competitor_intel_id = ?
                           ORDER BY ABS(COALESCE(price_delta_pct, 0)) DESC,
                                    line_num""",
                        (L["id"],),
                    ).fetchall()
                    L["lines"] = [{
                        "line_num": lr[0],
                        "desc": (lr[1] or "")[:80],
                        "their_price": lr[2],
                        "qty": lr[3],
                        "their_mfg": lr[4],
                        "our_idx": lr[5],
                        "our_price": lr[6],
                        "delta_pct": lr[7],
                        "matched_by": lr[8] or "none",
                    } for lr in line_rows]
            except Exception as _e:
                log.debug("per-line digest enrichment skipped: %s", _e)
                for L in report["losses"]:
                    L.setdefault("lines", [])

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

            # Calibration samples stats (all-time, sourced from
            # oracle_calibration — the table calibrate_from_outcome
            # writes to on EVERY win AND loss). This is the "Data
            # Points" KPI on the email card.
            #
            # 2026-05-25: the old KPI read winning_prices.COUNT(*).
            # winning_prices is populated ONLY on the win path
            # (orders.creation → record_winning_prices), so the card
            # displayed 0 even when the calibration table had 300+
            # samples from real losses — a contradiction visible on
            # the 2026-05-18→05-25 weekly that prompted this fix.
            # The two substrate tables are kept separate; the report
            # surfaces the canonical one for the KPI label "Data
            # Points" and keeps winning_prices_total as a secondary
            # field for downstream consumers / tests.
            try:
                cal_stats = conn.execute("""
                    SELECT COALESCE(SUM(sample_size), 0),
                           COALESCE(SUM(win_count), 0),
                           COALESCE(AVG(avg_winning_margin), 0)
                    FROM oracle_calibration
                """).fetchone()
                report["calibration_samples_total"] = int(cal_stats[0] or 0)
                report["calibration_wins_total"] = int(cal_stats[1] or 0)
                report["avg_margin_all_time"] = round(cal_stats[2] or 0, 1)
            except Exception:
                report["calibration_samples_total"] = 0
                report["calibration_wins_total"] = 0
                report["avg_margin_all_time"] = 0

            # Winning prices — secondary metric, kept for downstream
            # consumers (tests, future dashboards). NOT the headline KPI.
            try:
                wp_stats = conn.execute("""
                    SELECT COUNT(*), COUNT(DISTINCT fingerprint)
                    FROM winning_prices
                """).fetchone()
                report["winning_prices_total"] = wp_stats[0] or 0
                report["winning_prices_unique"] = wp_stats[1] or 0
            except Exception:
                report["winning_prices_total"] = 0
                report["winning_prices_unique"] = 0

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
    <div style="font-size:24px;font-weight:700;color:#58a6ff">{report.get('calibration_samples_total', report.get('winning_prices_total', 0))}</div>
    <div style="color:#8b949e;font-size:13px">Calibration Samples</div>
    <div style="color:#8b949e;font-size:12px">{report.get('avg_margin_all_time', 0)}% avg win margin</div>
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
            # 2026-05-13 (PR-D): per-line breakdown — surface the SCPRS
            # line-level deltas under each loss. This is the actionable
            # form of the digest: instead of just "we lost the PO," Mike
            # sees which specific markups cost the bundle. Sorted by
            # |delta| desc so the worst offenders surface first. Only
            # render if PR-C's child rows are present; otherwise skip
            # (legacy losses without per-line data stay one-row).
            per_lines = l.get("lines") or []
            if per_lines:
                html += (
                    '<tr><td colspan="4" style="padding:0 4px 8px 16px;'
                    'background:#0d1117">'
                    '<table style="width:100%;border-collapse:collapse;'
                    'font-size:11px;margin:4px 0">'
                    '<tr style="color:#484f58">'
                    '<th style="text-align:left;padding:3px 6px;font-weight:600">Line</th>'
                    '<th style="text-align:left;padding:3px 6px;font-weight:600">SCPRS Item</th>'
                    '<th style="text-align:right;padding:3px 6px;font-weight:600">Their $</th>'
                    '<th style="text-align:right;padding:3px 6px;font-weight:600">Our $</th>'
                    '<th style="text-align:right;padding:3px 6px;font-weight:600">Δ%</th>'
                    '<th style="text-align:left;padding:3px 6px;font-weight:600">Match</th>'
                    '</tr>'
                )
                for pl in per_lines[:15]:
                    # Color the delta — red if competitor was cheaper
                    # (we lost on this line), green if we were cheaper
                    # (we lost the bundle despite winning this line),
                    # grey if no comparison possible.
                    dpct = pl.get("delta_pct")
                    if dpct is None:
                        d_str = "—"
                        d_color = "#484f58"
                    elif dpct < -2:
                        d_str = f"{dpct:+.1f}%"
                        d_color = "#f85149"   # competitor cheaper
                    elif dpct > 2:
                        d_str = f"{dpct:+.1f}%"
                        d_color = "#3fb950"   # we were cheaper
                    else:
                        d_str = f"{dpct:+.1f}%"
                        d_color = "#d29922"   # essentially tied
                    their_p = f"${pl['their_price']:,.2f}" if pl.get("their_price") else "—"
                    our_p = f"${pl['our_price']:,.2f}" if pl.get("our_price") else "—"
                    match_color = {
                        "mfg_exact": "#3fb950",
                        "desc_tokens": "#d29922",
                        "none": "#484f58",
                    }.get(pl.get("matched_by", "none"), "#484f58")
                    desc_short = (pl.get("desc") or "")[:60]
                    html += (
                        f'<tr style="color:#8b949e">'
                        f'<td style="padding:2px 6px">{pl.get("line_num", "")}</td>'
                        f'<td style="padding:2px 6px;color:#c9d1d9">{desc_short}</td>'
                        f'<td style="text-align:right;padding:2px 6px;color:#f0883e">{their_p}</td>'
                        f'<td style="text-align:right;padding:2px 6px;color:#58a6ff">{our_p}</td>'
                        f'<td style="text-align:right;padding:2px 6px;color:{d_color};font-weight:600">{d_str}</td>'
                        f'<td style="padding:2px 6px;color:{match_color};font-size:10px">{pl.get("matched_by", "none")}</td>'
                        f'</tr>'
                    )
                if len(per_lines) > 15:
                    html += (
                        f'<tr><td colspan="6" style="padding:3px 6px;'
                        f'color:#484f58;font-style:italic">'
                        f'… {len(per_lines) - 15} more lines</td></tr>'
                    )
                html += '</table></td></tr>'
        html += '</table>'

    # Calibration state
    if cals:
        html += '<h3 style="color:#d29922;margin-top:20px">Oracle Calibration State</h3>'
        html += '<p style="color:#8b949e;font-size:12px">How the algorithm is adjusting based on outcomes:</p>'
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px">'
        html += '<tr style="color:#8b949e;border-bottom:1px solid #30363d"><th style="text-align:left;padding:4px">Category</th><th>Samples</th><th>Win Rate</th><th>Avg Win Margin</th><th>Max Markup</th><th>Why</th></tr>'
        for c in cals:
            wr_color = "#3fb950" if c["win_rate"] >= 70 else ("#d29922" if c["win_rate"] >= 40 else "#f85149")
            # IN-20: losses_price fallback was dead — report["calibrations"]
            # builder at ~line 181 always sets losses_total explicitly, so the
            # c.get("losses_price", 0) branch can never fire. Removing the
            # fallback lets a real schema change surface loudly instead of
            # silently masking a rename regression.
            why = _calibration_why(
                wins=c.get("wins", 0),
                losses_total=c.get("losses_total", 0),
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
# 2b. TELEGRAM REPORT FORMATTER (MarkdownV2)
# ═══════════════════════════════════════════════════════════════════════════════

_MD2_RESERVED = r"_*[]()~`>#+-=|{}.!"


def _md2(text) -> str:
    """Escape every MarkdownV2-reserved char in arbitrary user content.
    Used OUTSIDE pre/code blocks where Telegram requires every reserved
    char to carry a backslash.
    """
    if text is None:
        return ""
    s = str(text)
    out = []
    for ch in s:
        if ch in _MD2_RESERVED:
            out.append("\\")
        out.append(ch)
    return "".join(out)


def _pre_pad(s, width, align="left") -> str:
    """Width-pad inside a pre block for monospace column alignment.
    Truncates with a trailing space-buffer so columns never collide."""
    s = "" if s is None else str(s)
    if len(s) > width:
        s = s[: max(0, width - 1)] + "…"
    if align == "right":
        return s.rjust(width)
    return s.ljust(width)


def format_telegram_report(report: dict) -> str:
    """Build a MarkdownV2 Telegram payload for the oracle weekly digest.

    Telegram doesn't render HTML, so the layout leans on:
      - Bold + emoji headers for section structure
      - Monospace pre-blocks for tabular data (calibration / wins / losses)
      - Strategic line dividers instead of CSS borders
      - Tables capped at ~30 columns so they don't wrap on a phone

    Returns the FULL message body (header + KPI strip + sections + footer).
    The caller passes this via context["telegram_body"] and _send_telegram
    sends it AS-IS — no further escaping. Every interpolated value here is
    pre-escaped via _md2() (outside pre blocks) or sanitized (inside).
    """
    period_start = _md2(report.get("period_start", ""))
    period_end = _md2(report.get("period_end", ""))

    win_count = report.get("win_count", 0) or 0
    win_rev = report.get("win_revenue", 0) or 0
    loss_count = report.get("loss_count", 0) or 0
    samples = report.get(
        "calibration_samples_total", report.get("winning_prices_total", 0)
    ) or 0
    margin = report.get("avg_margin_all_time", 0) or 0
    supplier_leads = report.get("supplier_leads", []) or []
    pending = report.get("pending_actions", []) or []

    rev_str = f"${win_rev:,.0f}"
    margin_str = f"{margin}% avg margin"

    DIVIDER = "━━━━━━━━━━━━━━━━━━━"

    lines = []

    # ── Header ───────────────────────────────────────────────────────
    lines.append("📊 *Oracle Weekly Intelligence*")
    lines.append(f"_{period_start} → {period_end}_")
    lines.append("")

    # ── KPI strip ────────────────────────────────────────────────────
    lines.append(f"🏆 *{_md2(win_count)}* Wins · `{_md2(rev_str)}`")
    lines.append(f"❌ *{_md2(loss_count)}* Losses")
    lines.append(
        f"📈 *{_md2(f'{samples:,}')}* Calibration Samples · `{_md2(margin_str)}`"
    )
    lines.append("")

    # ── Wins ─────────────────────────────────────────────────────────
    wins = report.get("wins", []) or []
    if wins:
        lines.append(DIVIDER)
        lines.append("🏆 *Wins This Week*")
        lines.append("```")
        lines.append(
            f"{_pre_pad('Quote', 11)} {_pre_pad('Agency', 8)} "
            f"{_pre_pad('Revenue', 9, 'right')}"
        )
        for w in wins[:10]:
            q = w.get("quote", "") or ""
            a = w.get("agency", "") or ""
            t = w.get("total", 0) or 0
            lines.append(
                f"{_pre_pad(q, 11)} {_pre_pad(a, 8)} "
                f"{_pre_pad(f'${t:,.0f}', 9, 'right')}"
            )
        if len(wins) > 10:
            lines.append(f"… +{len(wins) - 10} more")
        lines.append("```")
        lines.append("")

    # ── Losses ───────────────────────────────────────────────────────
    losses = report.get("losses", []) or []
    if losses:
        lines.append(DIVIDER)
        lines.append("❌ *Losses This Week*")
        lines.append("```")
        lines.append(
            f"{_pre_pad('Quote', 10)} {_pre_pad('Competitor', 12)} "
            f"{_pre_pad('Δ%', 6, 'right')}"
        )
        for L in losses[:10]:
            q = L.get("quote", "") or ""
            c = L.get("competitor", "") or ""
            d = L.get("delta_pct")
            d_str = f"{d:+.1f}%" if d is not None else "—"
            lines.append(
                f"{_pre_pad(q, 10)} {_pre_pad(c, 12)} "
                f"{_pre_pad(d_str, 6, 'right')}"
            )
        if len(losses) > 10:
            lines.append(f"… +{len(losses) - 10} more")
        lines.append("```")
        lines.append("")

    # ── Calibration State ────────────────────────────────────────────
    cals = report.get("calibrations", []) or []
    if cals:
        lines.append(DIVIDER)
        lines.append("🎯 *Calibration State*")
        lines.append("_How the algorithm is adjusting based on outcomes_")
        lines.append("```")
        lines.append(
            f"{_pre_pad('Category', 13)} {_pre_pad('Sam', 4, 'right')} "
            f"{_pre_pad('Win%', 4, 'right')} {_pre_pad('Markup', 7, 'right')}"
        )
        for c in cals[:8]:
            cat = (c.get("category", "") or "").replace("_", " ")
            s = c.get("samples", 0) or 0
            wr = c.get("win_rate", 0) or 0
            mk = c.get("rec_max_markup", 0) or 0
            lines.append(
                f"{_pre_pad(cat, 13)} {_pre_pad(s, 4, 'right')} "
                f"{_pre_pad(f'{wr}%', 4, 'right')} "
                f"{_pre_pad(f'{mk:.1f}%', 7, 'right')}"
            )
        if len(cals) > 8:
            lines.append(f"… +{len(cals) - 8} more rows")
        lines.append("```")
        lines.append("")

    # ── Quiet-week footer ────────────────────────────────────────────
    if not wins and not losses:
        lines.append(DIVIDER)
        lines.append("_No win/loss activity this week\\._")
        lines.append("_Oracle calibration unchanged\\._")
        lines.append("")

    # ── Pending action items (top 5) ─────────────────────────────────
    if pending:
        lines.append(DIVIDER)
        lines.append("📋 *Pending Actions*")
        for a in pending[:5]:
            prio_emoji = {
                "high": "🔴", "medium": "🟡", "low": "⚪",
            }.get(a.get("priority", ""), "•")
            desc = (a.get("description", "") or "")[:80]
            lines.append(f"{prio_emoji} {_md2(desc)}")
        if len(pending) > 5:
            lines.append(f"_… \\+{len(pending) - 5} more in dashboard_")
        lines.append("")

    # ── Supplier leads ───────────────────────────────────────────────
    if supplier_leads:
        lines.append(DIVIDER)
        lines.append("🔍 *Supplier Leads* \\(cost reduction\\)")
        for lead in supplier_leads[:3]:
            desc = (lead.get("description", "") or "")[:90]
            lines.append(f"• {_md2(desc)}")
        if len(supplier_leads) > 3:
            lines.append(f"_… \\+{len(supplier_leads) - 3} more_")
        lines.append("")

    # ── Footer ───────────────────────────────────────────────────────
    gen = (report.get("generated_at", "") or "")[:16]
    lines.append(f"_Generated {_md2(gen)} · Oracle V4_")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SCHEDULED RUNNER + HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

def run_weekly_report():
    """Generate and send the weekly Oracle report. Called by scheduler."""
    from src.core.scheduler import heartbeat

    try:
        report = generate_weekly_report()
        html_body = format_report_email(report)
        telegram_body = format_telegram_report(report)

        plain = (
            f"Oracle V4 Weekly: {report['period_start']} to {report['period_end']}\n"
            f"Wins: {report.get('win_count', 0)} (${report.get('win_revenue', 0):,.2f})\n"
            f"Losses: {report.get('loss_count', 0)}\n"
            f"Supplier leads: {len(report.get('supplier_leads', []))}\n"
            f"Calibration samples: {report.get('calibration_samples_total', 0)}\n"
            f"Calibrations active: {len(report.get('calibrations', []))}"
        )

        from src.agents.notify_agent import send_alert
        # 2026-05-25: dropped explicit channels=["email"] so this falls
        # back to CHANNEL_MAP["oracle_weekly"] = ["telegram", "bell"].
        # The empty-inbox transition: status digests land in Telegram,
        # operator-actionable events (PO/CS draft/RFQ) keep email/SMS.
        # telegram_body short-circuits _send_telegram's escape path —
        # see format_telegram_report() docstring for the layout shape.
        result = send_alert(
            event_type="oracle_weekly",
            title=f"Oracle Weekly: {report.get('win_count', 0)}W / {report.get('loss_count', 0)}L",
            body=plain,
            urgency="info",
            context={
                "html_body": html_body,
                "telegram_body": telegram_body,
            },
            cooldown_key="oracle_weekly",  # IN-12: dedupe weekly sends on retry
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
                cooldown_key="oracle_weekly_send_failed",  # IN-12: suppress repeat bells on retry loop
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
                cooldown_key="oracle_weekly_crash",  # IN-12: crash loop must not spam bell
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
    fires an urgent alert.

    IN-8 fix: previously returned silently when no successful send row
    existed ("might be first week"). That fails open on the two cases
    that most need alerting — a fresh DB after a migration glitch, and
    a prod box that has literally never sent a report. Both silently
    hide a broken feedback loop. Now: if the table has no successful
    send ever *and* the schema has been around long enough that we'd
    expect one, alert with a distinct "never sent" event."""
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
                # No successful send ever recorded. Alert — but distinguish
                # from the "overdue" path so ops know this is a "never
                # ran" condition, not a "missed a cycle" condition.
                log.error(
                    "FORCING FUNCTION: Oracle weekly report has NEVER sent "
                    "successfully (fresh DB, migration glitch, or thread "
                    "has been broken since first boot)."
                )
                from src.agents.notify_agent import send_alert
                send_alert(
                    event_type="oracle_weekly_never_sent",
                    title="Oracle weekly report has NEVER sent successfully",
                    body=(
                        "oracle_report_log shows zero successful sends. "
                        "This usually means:\n"
                        "1. The schema was just migrated (verify with "
                        "SELECT COUNT(*) FROM oracle_report_log).\n"
                        "2. The oracle-weekly-report thread never started.\n"
                        "3. Every attempted send has failed.\n"
                        "Check the Oracle weekly thread health and run a "
                        "manual trigger: POST /api/oracle/weekly-report."
                    ),
                    urgency="urgent",
                    channels=["email", "bell"],
                    cooldown_key="oracle_never_sent",
                    run_async=False,
                )
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
                # IN-14: threshold-aware cooldown so a stuck job re-escalates as it
                # gets worse. Bucket by week (9-13d, 14-20d, 21-27d, ...) — each
                # bucket is a fresh cooldown key so the alert fires again when
                # the situation degrades, not once and then silent forever.
                bucket = max(1, days_since // 7)
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
                    cooldown_key=f"oracle_overdue_{bucket}w",
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
