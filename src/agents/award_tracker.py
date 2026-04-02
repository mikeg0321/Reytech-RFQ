"""
SCPRS Award Tracker — Automated PO Award Monitoring
=====================================================
Polls SCPRS 3x/day to detect when competitors win POs we quoted.
Starts checking 2 days after a quote is marked "sent".

Flow:
  1. Find sent quotes ≥ 2 days old that haven't been checked recently
  2. Search SCPRS for matching POs by description keywords
  3. If competitor PO found → line-by-line price analysis
  4. Generate loss report (why we lost, by how much, per item)
  5. Send report via SMS + email + notification bell
  6. Record all competitor prices into pricing intelligence
  7. Auto-close quote as "lost" with detailed notes

Schedule: Every 8 hours (3x/day) via background daemon thread
"""

import json
import logging
import os
import threading
import time
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger("award_tracker")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

DB_PATH = os.path.join(DATA_DIR, "reytech.db")

# ── Configuration ─────────────────────────────────────────────────────────────

POLL_INTERVAL_SEC = 8 * 60 * 60      # 8 hours = 3x/day
MIN_DAYS_AFTER_SENT = 2              # Start checking 2 days after sent
RECHECK_INTERVAL_HOURS = 8           # Don't re-check same quote within 8h
MAX_SCPRS_SEARCHES_PER_RUN = 15      # Rate-limit SCPRS requests
MATCH_CONFIDENCE_THRESHOLD = 0.55    # Min confidence to consider a match
HIGH_CONFIDENCE_THRESHOLD = 0.80     # Auto-close only at this confidence
SEARCH_WINDOW_DAYS = 120             # How far back to search SCPRS

_scheduler_started = False
_last_run = None
_last_result = None


# ── Database Setup ────────────────────────────────────────────────────────────

def _db():
    conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_tables():
    """Create tracking tables if they don't exist."""
    conn = _db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quote_po_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            matched_at TEXT NOT NULL,
            quote_id TEXT,
            quote_number TEXT NOT NULL,
            po_number TEXT NOT NULL,
            scprs_supplier TEXT,
            scprs_total REAL DEFAULT 0,
            our_total REAL DEFAULT 0,
            match_confidence REAL DEFAULT 0,
            outcome TEXT,
            match_method TEXT,
            auto_closed INTEGER DEFAULT 0,
            loss_report TEXT,
            line_analysis TEXT,
            UNIQUE(quote_number, po_number)
        );

        CREATE TABLE IF NOT EXISTS award_tracker_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT NOT NULL,
            quote_number TEXT NOT NULL,
            scprs_searched INTEGER DEFAULT 0,
            matches_found INTEGER DEFAULT 0,
            outcome TEXT,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_atl_quote ON award_tracker_log(quote_number);
        CREATE INDEX IF NOT EXISTS idx_atl_checked ON award_tracker_log(checked_at);
        CREATE INDEX IF NOT EXISTS idx_qpm_quote ON quote_po_matches(quote_number);
    """)
    conn.commit()
    conn.close()


# ── Core Monitor Logic ────────────────────────────────────────────────────────

def run_award_check(force: bool = False) -> dict:
    """
    Main entry point. Scans sent quotes ≥ 2 days old, checks SCPRS for awards.

    Args:
        force: If True, skip recheck interval and check all eligible quotes.

    Returns:
        dict with results summary, actions taken, and any loss reports.
    """
    global _last_run, _last_result
    _ensure_tables()
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    _last_run = now_iso

    conn = _db()

    # ── Find eligible quotes: status='sent', sent 2+ days ago ─────────────
    cutoff_date = (now - timedelta(days=MIN_DAYS_AFTER_SENT)).isoformat()

    sent_quotes = conn.execute("""
        SELECT id, quote_number, agency, institution, total, line_items,
               items_text, sent_at, created_at, contact_email, contact_name,
               source_pc_id, 'quote' as record_type
        FROM quotes
        WHERE is_test = 0
          AND status = 'sent'
          AND total > 0
          AND (
              (sent_at IS NOT NULL AND sent_at != '' AND sent_at <= ?)
              OR
              (sent_at IS NULL OR sent_at = '') AND created_at <= ?
          )
    """, (cutoff_date, cutoff_date)).fetchall()

    # ── Also find eligible RFQs via sent_quote_tracker ───────────────────
    sent_rfqs = []
    try:
        sent_rfqs = conn.execute("""
            SELECT r.id, r.rfq_number as quote_number, r.agency, r.institution,
                   s.total_value as total, r.items as line_items,
                   '' as items_text, s.sent_at, r.received_at as created_at,
                   r.requestor_email as contact_email,
                   r.requestor_name as contact_name,
                   '' as source_pc_id, 'rfq' as record_type
            FROM rfqs r
            JOIN sent_quote_tracker s ON s.id = r.id
            WHERE s.status = 'sent'
              AND r.status IN ('sent', 'pending_award', 'generated')
              AND s.total_value > 0
              AND s.sent_at <= ?
        """, (cutoff_date,)).fetchall()
        if sent_rfqs:
            log.info("SCHEDULE: Found %d sent RFQs to monitor (in addition to %d quotes)",
                     len(sent_rfqs), len(sent_quotes))
    except Exception as e:
        log.debug("RFQ query for award tracking: %s", e)

    # Combine quotes + RFQs into unified list
    sent_quotes = list(sent_quotes) + list(sent_rfqs)

    if not sent_quotes:
        conn.close()
        result = {"ok": True, "message": "No sent quotes/RFQs ready for award check",
                  "eligible": 0, "checked": 0, "matches": 0, "losses": 0}
        _last_result = result
        return result

    # ── Cross-queue dedup: avoid double-monitoring same solicitation ──────
    # If a PC and an RFQ exist for the same solicitation, only check the
    # most recently sent one (avoid duplicate SCPRS queries).
    dedup_count = 0
    try:
        sol_map = {}  # solicitation -> list of records
        for q in sent_quotes:
            q = dict(q)
            # Try to extract solicitation from various fields
            sol = ""
            try:
                items_raw = q.get("line_items") or "[]"
                if isinstance(items_raw, str):
                    parsed = json.loads(items_raw)
                    if parsed and isinstance(parsed, list):
                        sol = parsed[0].get("solicitation", "") if isinstance(parsed[0], dict) else ""
            except Exception:
                pass
            if not sol:
                sol = q.get("quote_number", "")

            if sol and len(sol) > 3:
                sol_map.setdefault(sol, []).append(q)

        # For each solicitation with multiple records, keep only the most recent
        deduped = []
        seen_sols = set()
        for sol, records in sol_map.items():
            if len(records) > 1:
                # Sort by sent_at descending, keep the most recent
                records.sort(key=lambda r: r.get("sent_at") or r.get("created_at", ""), reverse=True)
                deduped.append(records[0])
                dedup_count += len(records) - 1
                log.info("DEDUP: Solicitation '%s' has %d records — keeping %s, skipping %d",
                         sol[:20], len(records), records[0].get("quote_number", "?"),
                         len(records) - 1)
                seen_sols.add(sol)
            else:
                deduped.append(records[0])
                seen_sols.add(sol)

        # Add any records that didn't have a solicitation (shouldn't be deduped)
        for q in sent_quotes:
            q = dict(q)
            qn = q.get("quote_number", "")
            if not any(d.get("quote_number") == qn for d in deduped):
                deduped.append(q)

        if dedup_count > 0:
            log.info("DEDUP: Removed %d duplicate records across PC/RFQ/quote queues", dedup_count)
        sent_quotes = deduped
    except Exception as de:
        log.debug("Cross-queue dedup: %s", de)
        sent_quotes = [dict(q) for q in sent_quotes]

    # ── Adaptive schedule: filter quotes by phase ──────────────────────────
    to_check = []
    expired_count = 0

    for q in sent_quotes:
        q = dict(q)
        quote_num = q["quote_number"]
        sent_at_str = q.get("sent_at") or q.get("created_at", "")

        # Parse sent_at for schedule calculation
        sent_at_dt = None
        try:
            sent_at_dt = datetime.fromisoformat(sent_at_str[:19])
        except Exception:
            pass

        if not force and sent_at_dt:
            try:
                from src.core.scprs_schedule import should_check_record, get_check_phase

                # Check if this record has expired (45+ days)
                phase = get_check_phase(sent_at_dt)
                if phase == "expired":
                    # Auto-expire this quote
                    total_days = (now.replace(tzinfo=None) - sent_at_dt).days
                    try:
                        conn.execute("""
                            UPDATE quotes SET status='expired',
                                status_notes=?, closed_by_agent='award_tracker',
                                updated_at=?
                            WHERE quote_number=? AND status='sent'
                        """, (f"No award found after {total_days} days", now_iso, quote_num))
                        log.info("SCHEDULE: %s EXPIRED — %d days since sent, no award found",
                                 quote_num, total_days)
                    except Exception as e:
                        log.debug("Expire quote: %s", e)
                    expired_count += 1
                    continue

                # Get last check info for this quote
                last_check_row = conn.execute("""
                    SELECT checked_at, notes FROM award_tracker_log
                    WHERE quote_number = ? ORDER BY checked_at DESC LIMIT 1
                """, (quote_num,)).fetchone()

                last_checked_dt = None
                last_window = ""
                if last_check_row:
                    try:
                        last_checked_dt = datetime.fromisoformat(
                            last_check_row["checked_at"][:19])
                    except Exception:
                        pass
                    # Extract window from notes if stored
                    notes_str = last_check_row["notes"] or ""
                    if "window" in notes_str:
                        # Try to extract window label
                        import re
                        wm = re.search(r"window (\d{2}:\d{2})", notes_str)
                        if wm:
                            last_window = wm.group(1)

                should, reason = should_check_record(
                    sent_at=sent_at_dt,
                    last_checked=last_checked_dt,
                    last_checked_window=last_window,
                )

                if not should:
                    log.debug("SCHEDULE: SKIP %s — %s", quote_num, reason)
                    continue
                else:
                    log.info("SCHEDULE: CHECK %s — %s", quote_num, reason)

            except ImportError:
                # Fallback to old fixed interval if scprs_schedule not available
                recheck_cutoff = (now - timedelta(hours=RECHECK_INTERVAL_HOURS)).isoformat()
                last_check = conn.execute("""
                    SELECT checked_at FROM award_tracker_log
                    WHERE quote_number = ? ORDER BY checked_at DESC LIMIT 1
                """, (quote_num,)).fetchone()
                if last_check and last_check["checked_at"] > recheck_cutoff:
                    continue

        to_check.append(q)

    if not to_check:
        conn.close()
        result = {"ok": True, "message": "All eligible quotes checked recently",
                  "eligible": len(sent_quotes), "checked": 0, "matches": 0,
                  "losses": 0, "expired": expired_count}
        _last_result = result
        return result

    # ── Initialize SCPRS session ──────────────────────────────────────────
    try:
        from src.agents.scprs_lookup import FiscalSession
        session = FiscalSession()
        if not session.init_session():
            conn.close()
            result = {"ok": False, "error": "SCPRS session unavailable",
                      "eligible": len(to_check)}
            _last_result = result
            return result
    except Exception as e:
        conn.close()
        result = {"ok": False, "error": f"SCPRS init failed: {e}"}
        _last_result = result
        return result

    # ── Check each quote ──────────────────────────────────────────────────
    total_checked = 0
    total_matches = 0
    total_losses = 0
    total_prices_recorded = 0
    loss_reports = []
    searches_used = 0

    for q in to_check:
        if searches_used >= MAX_SCPRS_SEARCHES_PER_RUN:
            log.info("SCPRS rate limit reached (%d searches), stopping", searches_used)
            break

        quote_num = q["quote_number"]
        institution = q.get("institution", "") or ""
        agency = q.get("agency", "") or ""
        our_total = q.get("total", 0) or 0

        # Parse line items from JSON
        our_items = []
        try:
            our_items = json.loads(q.get("line_items") or "[]")
        except Exception:
            pass

        # Extract search keywords from items
        keywords = _extract_search_keywords(our_items, q.get("items_text", ""))

        log.info("Award check: %s (agency=%s, inst=%s, $%.2f, %d items, %d keywords)",
                 quote_num, agency, institution, our_total, len(our_items), len(keywords))

        quote_matches = []
        for keyword in keywords[:3]:  # Max 3 searches per quote
            if searches_used >= MAX_SCPRS_SEARCHES_PER_RUN:
                break
            try:
                # Search SCPRS for this keyword
                from_date = q.get("sent_at") or q.get("created_at", "")
                if from_date:
                    # Convert ISO to MM/DD/YYYY
                    try:
                        dt = datetime.fromisoformat(from_date[:19])
                        scprs_from = dt.strftime("%m/%d/%Y")
                    except Exception:
                        scprs_from = (now - timedelta(days=SEARCH_WINDOW_DAYS)).strftime("%m/%d/%Y")
                else:
                    scprs_from = (now - timedelta(days=SEARCH_WINDOW_DAYS)).strftime("%m/%d/%Y")

                results = session.search(description=keyword, from_date=scprs_from)
                searches_used += 1
                time.sleep(1.2)  # Rate limit

                for po in results:
                    confidence, reasons = _match_quote_to_po(q, po, keyword)
                    if confidence >= MATCH_CONFIDENCE_THRESHOLD:
                        # Get line item details
                        detail = None
                        try:
                            detail = session.get_detail(
                                po.get("_results_html", ""),
                                po.get("_row_index", 0),
                                po.get("_click_action")
                            )
                            searches_used += 1
                            time.sleep(1.0)
                        except Exception as e:
                            log.debug("Detail fetch failed: %s", e)

                        po_data = {**po}
                        if detail:
                            po_data.update(detail)

                        quote_matches.append({
                            "po": po_data,
                            "confidence": confidence,
                            "reasons": reasons,
                        })

            except Exception as e:
                log.warning("SCPRS search '%s' for %s: %s", keyword, quote_num, e)

        # ── Evaluate matches ──────────────────────────────────────────────
        best_match = None
        if quote_matches:
            # Take highest confidence match
            best_match = max(quote_matches, key=lambda m: m["confidence"])

        outcome = "no_match"
        notes = f"Searched {len(keywords[:3])} keywords, {len(quote_matches)} potential matches"

        if best_match:
            total_matches += 1
            po = best_match["po"]
            supplier = po.get("supplier_name", po.get("supplier", "Unknown"))
            scprs_total = po.get("grand_total_num", 0) or 0
            po_number = po.get("po_number", "")

            # Determine if we won or lost
            if "reytech" in supplier.lower() or "rey tech" in supplier.lower():
                outcome = "we_won"
                notes = f"Reytech won PO {po_number} at ${scprs_total:,.2f}"
                log.info("✅ %s: WE WON — PO %s", quote_num, po_number)

                # Cross-queue sync: mark quote as won + update linked PC
                try:
                    conn.execute("""
                        UPDATE quotes SET status='won',
                            status_notes=?, closed_by_agent='award_tracker',
                            updated_at=?
                        WHERE quote_number=? AND status='sent'
                    """, (notes, now_iso, quote_num))
                    # Also sync any linked PC via source_pc_id
                    pc_id = q.get("source_pc_id", "")
                    if pc_id:
                        _sync_linked_pc(pc_id, "won", notes)
                except Exception as we:
                    log.debug("Win sync: %s", we)
            else:
                outcome = "lost_to_competitor"
                total_losses += 1

                # ── Line-by-line price analysis ───────────────────────────
                po_lines = po.get("line_items", [])
                analysis = _analyze_loss(q, our_items, po, po_lines, supplier, scprs_total)

                # ── Record competitor prices ──────────────────────────────
                prices_recorded = _record_competitor_prices(
                    conn, po_lines, po, agency, quote_num, our_items
                )
                total_prices_recorded += prices_recorded

                # ── Store match record ────────────────────────────────────
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO quote_po_matches
                        (matched_at, quote_id, quote_number, po_number,
                         scprs_supplier, scprs_total, our_total, match_confidence,
                         outcome, match_method, auto_closed, loss_report, line_analysis)
                        VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)
                    """, (now_iso, q["id"], quote_num, po_number,
                          supplier, scprs_total, our_total,
                          best_match["confidence"], outcome,
                          ", ".join(best_match["reasons"]),
                          analysis["report"],
                          json.dumps(analysis["line_comparison"], default=str)))
                except Exception as e:
                    log.error("Failed to store match: %s", e)

                # ── Log enhanced competitor intel ─────────────────────────
                try:
                    loss_class = analysis.get("loss_reason_class", "price_higher")
                    mth_items = analysis.get("margin_too_high_items", [])
                    avg_cost = 0
                    avg_margin = 0
                    cost_items = [c for c in analysis["line_comparison"]
                                  if c.get("matched") and (c.get("our_cost", 0) or 0) > 0]
                    if cost_items:
                        avg_cost = sum(c["our_cost"] for c in cost_items) / len(cost_items)
                        margins = [c.get("our_margin_pct", 0) for c in cost_items if c.get("our_margin_pct")]
                        avg_margin = sum(margins) / len(margins) if margins else 0

                    conn.execute("""
                        INSERT OR IGNORE INTO competitor_intel
                        (found_at, pc_id, quote_number, our_price, competitor_name,
                         competitor_price, price_delta, price_delta_pct, po_number,
                         agency, institution, item_summary, solicitation, outcome, notes,
                         loss_reason_class, our_cost, our_margin_pct, margin_too_high,
                         items_detail)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        now_iso, q.get("source_pc_id", ""), quote_num, our_total,
                        supplier, scprs_total,
                        round(our_total - scprs_total, 2),
                        round(analysis["pct_diff"], 1),
                        po_number, agency, institution,
                        ", ".join((c.get("our_description", "")[:40])
                                  for c in analysis["line_comparison"][:5]),
                        "", "lost",
                        f"{loss_class}: {analysis['summary'][:150]}",
                        loss_class, round(avg_cost, 2), round(avg_margin, 1),
                        1 if mth_items else 0,
                        json.dumps(analysis["line_comparison"], default=str),
                    ))
                    log.info("COMPETITOR_INTEL: %s loss_class=%s margin_too_high=%d",
                             quote_num, loss_class, len(mth_items))
                except Exception as e:
                    log.debug("Enhanced competitor intel: %s", e)

                # ── Auto-close quote as lost ──────────────────────────────
                # Guard: Only auto-close at HIGH confidence (0.80+)
                # At lower confidence, flag for manual review instead
                match_confidence = best_match["confidence"]
                loss_class = analysis.get("loss_reason_class", "price_higher")
                should_auto_close = match_confidence >= HIGH_CONFIDENCE_THRESHOLD

                # Extra guard: if we were cheaper and confidence < 0.70,
                # this might be a false positive — don't auto-close
                if loss_class == "relationship_incumbent" and match_confidence < 0.70:
                    should_auto_close = False
                    log.warning("FALSE_POSITIVE_GUARD: %s — we were cheaper but low confidence "
                                "(%.2f < 0.70), NOT auto-closing", quote_num, match_confidence)

                loss_note = (
                    f"SCPRS: {supplier} won PO {po_number} at ${scprs_total:,.2f} "
                    f"(we quoted ${our_total:,.2f}). "
                    f"Delta: ${our_total - scprs_total:+,.2f} "
                    f"({analysis['pct_diff']:+.1f}%) "
                    f"[{loss_class}, confidence: {match_confidence:.0%}]"
                )
                if should_auto_close:
                    try:
                        conn.execute("""
                            UPDATE quotes SET status='lost',
                                status_notes=?, close_reason=?, closed_by_agent='award_tracker',
                                updated_at=?
                            WHERE quote_number=? AND status='sent'
                        """, (loss_note, f"SCPRS: Lost to {supplier}", now_iso, quote_num))
                        log.info("AUTO_CLOSE: %s — confidence %.0f%% >= %.0f%% threshold",
                                 quote_num, match_confidence * 100, HIGH_CONFIDENCE_THRESHOLD * 100)
                        # Cross-queue sync: also close linked PC
                        pc_id = q.get("source_pc_id", "")
                        if pc_id:
                            _sync_linked_pc(pc_id, "lost", loss_note[:200])
                    except Exception as e:
                        log.error("Failed to close quote %s: %s", quote_num, e)
                else:
                    log.info("MANUAL_REVIEW: %s — confidence %.0f%% < %.0f%% threshold, "
                             "flagging for review (not auto-closing)",
                             quote_num, match_confidence * 100, HIGH_CONFIDENCE_THRESHOLD * 100)

                # ── Log CRM activity ──────────────────────────────────────
                try:
                    from src.api.dashboard import _log_crm_activity
                    _log_crm_activity(
                        quote_num, "quote_lost",
                        f"Lost to {supplier} — PO {po_number} awarded at "
                        f"${scprs_total:,.2f} (our quote: ${our_total:,.2f}). "
                        f"{analysis['summary']}",
                        actor="award_tracker",
                        metadata={
                            "po_number": po_number,
                            "winner": supplier,
                            "winner_total": scprs_total,
                            "our_total": our_total,
                            "price_delta": round(our_total - scprs_total, 2),
                            "pct_diff": round(analysis["pct_diff"], 1),
                            "items_compared": analysis["items_compared"],
                            "prices_recorded": prices_recorded,
                            "loss_reason_class": analysis.get("loss_reason_class", ""),
                            "margin_too_high_count": analysis.get("margin_too_high_count", 0),
                        }
                    )
                except Exception as e:
                    log.debug("CRM activity: %s", e)

                # ── Update quotes_log.json to match DB ────────────────────
                try:
                    _sync_quote_loss_to_json(quote_num, loss_note)
                except Exception as e:
                    log.debug("JSON sync: %s", e)

                # ── Build loss report ─────────────────────────────────────
                loss_reports.append({
                    "quote_number": quote_num,
                    "agency": agency,
                    "institution": institution,
                    "our_total": our_total,
                    "winner": supplier,
                    "winner_total": scprs_total,
                    "po_number": po_number,
                    "analysis": analysis,
                    "prices_recorded": prices_recorded,
                })

                # ── Pricing feedback loop: update competitive intelligence ──
                try:
                    from src.agents.pricing_feedback import update_competitive_intelligence
                    update_competitive_intelligence(
                        analysis=analysis,
                        quote=q,
                        po=best_match["po"],
                    )
                except Exception as pfe:
                    log.debug("Pricing feedback: %s", pfe)

                notes = (
                    f"Lost to {supplier} — PO {po_number} ${scprs_total:,.2f} "
                    f"(delta: ${our_total - scprs_total:+,.2f})"
                )
                log.info("❌ %s: LOST to %s — PO %s $%.2f vs our $%.2f",
                         quote_num, supplier, po_number, scprs_total, our_total)

        # ── Update recommendation_audit with outcome ────────────────────
        if outcome in ("we_won", "lost_to_competitor"):
            try:
                from src.core.db import get_db
                with get_db() as ra_conn:
                    pc_id = q.get("source_pc_id", "") or q.get("pc_id", "")
                    quote_num_ra = q.get("quote_number", "")
                    ra_outcome = "won" if outcome == "we_won" else "lost"
                    competitor_price = scprs_total if outcome == "lost_to_competitor" else 0
                    ra_conn.execute("""
                        UPDATE recommendation_audit
                        SET outcome=?, outcome_price=?, updated_at=datetime('now')
                        WHERE (pc_id=? OR quote_number=?) AND outcome='pending'
                    """, (ra_outcome, competitor_price, pc_id, quote_num_ra))
            except Exception:
                pass

        # ── Log check attempt (include SCPRS window for adaptive schedule) ──
        try:
            from src.core.scprs_schedule import current_scprs_window, get_check_phase
            window_label = current_scprs_window() or "manual"
            phase_label = get_check_phase(
                datetime.fromisoformat((q.get("sent_at") or q.get("created_at", ""))[:19])
            ) if q.get("sent_at") or q.get("created_at") else "unknown"
            notes += f" | window {window_label} | phase {phase_label}"
        except Exception:
            pass

        conn.execute("""
            INSERT INTO award_tracker_log
            (checked_at, quote_number, scprs_searched, matches_found, outcome, notes)
            VALUES (?,?,?,?,?,?)
        """, (now_iso, quote_num, searches_used, len(quote_matches), outcome, notes))

        total_checked += 1

    conn.commit()
    conn.close()

    # ── Send loss reports ─────────────────────────────────────────────────
    if loss_reports:
        _send_loss_reports(loss_reports)

    # ── Detect and notify on emerging patterns ───────────────────────────
    patterns_detected = 0
    if loss_reports:
        try:
            from src.agents.pricing_feedback import detect_margin_patterns
            patterns = detect_margin_patterns(days=90)
            critical_patterns = [p for p in patterns if p.get("severity") == "critical"]
            if critical_patterns:
                patterns_detected = len(critical_patterns)
                try:
                    from src.agents.notify_agent import send_alert
                    pattern_summary = "; ".join(
                        p["description"][:80] for p in critical_patterns[:3]
                    )
                    send_alert(
                        event_type="loss_pattern_detected",
                        title=f"Competitive Pattern Alert: {len(critical_patterns)} critical pattern(s)",
                        body=f"Critical competitive patterns detected:\n{pattern_summary}\n\n"
                             f"Review at /api/intel/loss-patterns",
                        urgency="warning",
                        context={"pattern_count": len(critical_patterns)},
                        channels=["email", "bell"],
                        cooldown_key=f"loss_patterns:{datetime.now().strftime('%Y%m%d')}",
                    )
                    log.info("PATTERN_NOTIFY: Sent alert for %d critical patterns",
                             len(critical_patterns))
                except Exception as ne:
                    log.debug("Pattern notification: %s", ne)
        except Exception as pe:
            log.debug("Pattern detection: %s", pe)

    result = {
        "ok": True,
        "eligible": len(sent_quotes),
        "checked": total_checked,
        "matches": total_matches,
        "losses": total_losses,
        "prices_recorded": total_prices_recorded,
        "scprs_searches": searches_used,
        "loss_reports": len(loss_reports),
        "reports": loss_reports,
        "patterns_detected": patterns_detected,
        "timestamp": now_iso,
    }
    _last_result = result
    _heartbeat(success=True)
    return result


# ── Search Keyword Extraction ─────────────────────────────────────────────────

def _extract_search_keywords(items: list, items_text: str = "") -> list:
    """Extract the best SCPRS search terms from quote line items."""
    keywords = set()
    for item in items:
        desc = (item.get("description") or item.get("name") or "").strip()
        if not desc:
            continue

        # Use manufacturer part numbers first (most specific)
        mfg = item.get("manufacturer_part", "") or item.get("mfg_number", "")
        sku = item.get("sku", "") or item.get("part_number", "")
        if mfg and len(mfg) > 3:
            keywords.add(mfg)
        elif sku and len(sku) > 3 and not sku.startswith("B0"):
            keywords.add(sku)

        # Extract key product words (first 3-4 meaningful words)
        stop_words = {"the", "and", "for", "with", "per", "each", "box", "case",
                      "pkg", "pack", "ea", "by", "of", "in", "or", "to", "a", "an"}
        words = [w for w in desc.split() if w.lower() not in stop_words and len(w) > 2]
        if words:
            # Use first 3 words as a phrase
            keywords.add(" ".join(words[:3]))
            # Also try manufacturer name + first product word
            mfg_name = item.get("manufacturer", "")
            if mfg_name and words:
                keywords.add(f"{mfg_name} {words[0]}")

    # Fallback: use items_text
    if not keywords and items_text:
        text = items_text.lower()
        for term in ["gloves", "briefs", "restraint", "surgical", "wound",
                      "sanitizer", "mask", "gown", "syringe", "bandage",
                      "gauze", "adapter", "catheter", "dressing"]:
            if term in text:
                keywords.add(term)

    return list(keywords)[:5]


# ── Quote-to-PO Matching ─────────────────────────────────────────────────────

def _match_quote_to_po(quote: dict, po: dict, search_term: str) -> tuple:
    """
    Calculate confidence that a SCPRS PO matches our quote.
    Returns (confidence: float 0-1, reasons: list[str]).
    """
    score = 0.0
    reasons = []

    agency = (quote.get("agency", "") or "").upper()
    institution = (quote.get("institution", "") or "").upper()
    dept_name = (po.get("dept", "") or "").upper()
    our_total = quote.get("total", 0) or 0
    scprs_total = po.get("grand_total_num", 0) or 0

    # Agency/department match (CDCR → corrections, CalVet → veterans, etc.)
    try:
        from src.agents.scprs_intelligence_engine import AGENCY_REGISTRY
        for ag_key, reg in AGENCY_REGISTRY.items():
            # Match if: registry key matches quote agency, OR
            # any dept_name_pattern matches quote agency, OR
            # quote agency appears in registry key
            patterns = reg.get("dept_name_patterns", [])
            key_match = (ag_key.upper() in agency or agency in ag_key.upper())
            pattern_match_agency = any(p.upper() in agency for p in patterns if len(p) > 2)

            if key_match or pattern_match_agency:
                # Now check if the SCPRS dept matches this registry entry
                if any(p in dept_name for p in patterns):
                    score += 0.35
                    reasons.append("agency_match")
                    break
    except Exception:
        # Fallback: simple text match
        if agency and agency in dept_name:
            score += 0.3
            reasons.append("agency_text_match")

    # Institution name match
    if institution and len(institution) > 3:
        if institution in dept_name:
            score += 0.25
            reasons.append("institution_exact")
        else:
            # Partial match — significant words
            inst_words = [w for w in institution.split() if len(w) > 3]
            matched = sum(1 for w in inst_words if w in dept_name)
            if matched > 0 and inst_words:
                score += 0.15 * (matched / len(inst_words))
                reasons.append("institution_partial")

    # Amount proximity
    if our_total > 0 and scprs_total > 0:
        ratio = min(our_total, scprs_total) / max(our_total, scprs_total)
        if ratio >= 0.85:
            score += 0.25
            reasons.append(f"amount_close({ratio:.0%})")
        elif ratio >= 0.60:
            score += 0.15
            reasons.append(f"amount_similar({ratio:.0%})")

    # Item description overlap
    first_item = (po.get("first_item", "") or "").upper()
    if first_item:
        for item in json.loads(quote.get("line_items") or "[]"):
            desc = (item.get("description") or item.get("name") or "").upper()
            if desc and first_item:
                # Check for common words
                desc_words = set(w for w in desc.split() if len(w) > 3)
                po_words = set(w for w in first_item.split() if len(w) > 3)
                overlap = desc_words & po_words
                if len(overlap) >= 2:
                    score += 0.2
                    reasons.append(f"items_overlap({','.join(list(overlap)[:3])})")
                    break
                elif len(overlap) >= 1:
                    score += 0.1
                    reasons.append(f"items_partial({','.join(overlap)})")
                    break

    return (min(score, 1.0), reasons)


# ── Loss Classification ──────────────────────────────────────────────────────

def _classify_loss_reason(pct_diff: float, line_comparison: list,
                          margin_too_high_items: list) -> str:
    """Classify WHY we lost into one of 4 categories.

    Returns one of:
      'margin_too_high'       — our cost was lower than their sell but we marked up too much
      'relationship_incumbent' — we were cheaper overall but still lost
      'cost_too_high'         — our cost basis exceeds their sell price (can't compete)
      'price_higher'          — we were simply more expensive (general case)
    """
    # Priority 1: Margin too high is the most actionable insight
    if margin_too_high_items:
        return "margin_too_high"

    # Priority 2: We were cheaper and still lost — relationship/incumbent
    if pct_diff < -2:
        return "relationship_incumbent"

    # Priority 3: Check if cost basis is the problem
    cost_items = [c for c in line_comparison
                  if c.get("matched") and (c.get("our_cost", 0) or 0) > 0]
    if cost_items:
        high_cost_count = sum(
            1 for c in cost_items
            if c["our_cost"] > (c.get("winner_unit_price", 0) or 0)
        )
        if high_cost_count > len(cost_items) / 2:
            return "cost_too_high"

    # Default: general pricing loss
    return "price_higher"


# ── Loss Analysis ─────────────────────────────────────────────────────────────

def _analyze_loss(quote: dict, our_items: list, po: dict, po_lines: list,
                  winner: str, winner_total: float) -> dict:
    """
    Line-by-line price analysis: compare our quote to the winning PO.
    Returns a structured analysis with report text and per-item comparison.
    """
    our_total = quote.get("total", 0) or 0
    delta = our_total - winner_total
    pct_diff = (delta / winner_total * 100) if winner_total > 0 else 0

    line_comparison = []
    items_compared = 0
    total_item_delta = 0

    # Try to match our items to PO line items
    for our_item in our_items:
        our_desc = (our_item.get("description") or our_item.get("name") or "").strip()
        our_price = our_item.get("unit_price") or our_item.get("our_price") or our_item.get("price", 0)
        our_qty = our_item.get("qty") or our_item.get("quantity", 0)
        our_ext = round((our_price or 0) * (our_qty or 0), 2)
        our_cost = our_item.get("cost") or our_item.get("supplier_price", 0)
        our_margin = our_item.get("margin_pct", 0)

        if not our_desc:
            continue

        # Try to find matching PO line
        best_po_match = None
        best_score = 0

        for po_line in po_lines:
            po_desc = (po_line.get("description") or "").strip()
            if not po_desc:
                continue
            # Score: word overlap
            our_words = set(w.upper() for w in our_desc.split() if len(w) > 2)
            po_words = set(w.upper() for w in po_desc.split() if len(w) > 2)
            if not our_words or not po_words:
                continue
            overlap = len(our_words & po_words)
            score = overlap / max(len(our_words), 1)
            if score > best_score and score >= 0.3:
                best_score = score
                best_po_match = po_line

        comp = {
            "our_description": our_desc[:80],
            "our_unit_price": our_price,
            "our_qty": our_qty,
            "our_extended": our_ext,
            "our_cost": our_cost,
            "our_margin_pct": our_margin,
        }

        if best_po_match:
            items_compared += 1
            their_price = best_po_match.get("unit_price", 0) or 0
            their_qty = best_po_match.get("quantity", 0) or 0
            their_ext = round(their_price * their_qty, 2)
            item_delta = (our_price or 0) - their_price
            item_pct = (item_delta / their_price * 100) if their_price > 0 else 0
            total_item_delta += item_delta * (our_qty or 1)

            comp.update({
                "winner_description": (best_po_match.get("description") or "")[:80],
                "winner_unit_price": their_price,
                "winner_qty": their_qty,
                "winner_extended": their_ext,
                "unit_price_delta": round(item_delta, 2),
                "unit_price_pct_diff": round(item_pct, 1),
                "matched": True,
            })
        else:
            comp.update({"matched": False, "winner_description": "(no match found)"})

        line_comparison.append(comp)

    # ── Build loss reasons ────────────────────────────────────────────────
    loss_reasons = []
    if pct_diff > 0:
        loss_reasons.append(f"We were {pct_diff:.1f}% higher overall (${delta:+,.2f})")
    else:
        loss_reasons.append(
            f"We were {abs(pct_diff):.1f}% LOWER but still lost "
            f"— possible relationship/incumbent advantage"
        )

    overpriced_items = [c for c in line_comparison
                        if c.get("matched") and (c.get("unit_price_pct_diff", 0) or 0) > 5]
    if overpriced_items:
        worst = max(overpriced_items, key=lambda c: c.get("unit_price_pct_diff", 0))
        loss_reasons.append(
            f"Worst item: '{worst['our_description'][:40]}' — "
            f"our ${worst['our_unit_price']:.2f} vs their ${worst['winner_unit_price']:.2f} "
            f"({worst['unit_price_pct_diff']:+.1f}%)"
        )

    underpriced_items = [c for c in line_comparison
                         if c.get("matched") and (c.get("unit_price_pct_diff", 0) or 0) < -5]
    if underpriced_items:
        loss_reasons.append(
            f"{len(underpriced_items)} item(s) we were CHEAPER on — "
            f"margin opportunity if we bid again"
        )

    # Check if our margins were too thin or too fat
    our_margins = [c.get("our_margin_pct", 0) for c in line_comparison if c.get("our_margin_pct")]
    if our_margins:
        avg_margin = sum(our_margins) / len(our_margins)
        if avg_margin > 30:
            loss_reasons.append(f"Avg margin was {avg_margin:.0f}% — may have room to be more competitive")
        elif avg_margin < 10 and pct_diff > 0:
            loss_reasons.append(f"Avg margin only {avg_margin:.0f}% and still lost — cost basis may be too high")

    # ── Margin Too High Detection ────────────────────────────────────────
    # CRITICAL: If our COST was lower than competitor's SELL price, but our
    # BID was higher, we had the cost advantage but priced ourselves out.
    margin_too_high_items = []
    for comp in line_comparison:
        if not comp.get("matched"):
            continue
        c_our_cost = comp.get("our_cost", 0) or 0
        c_their_sell = comp.get("winner_unit_price", 0) or 0
        c_our_sell = comp.get("our_unit_price", 0) or 0

        if c_our_cost > 0 and c_their_sell > 0 and c_our_sell > 0:
            if c_our_cost < c_their_sell and c_our_sell > c_their_sell:
                # We had lower cost but bid higher — margin too high
                could_have_bid = round(c_their_sell * 0.98, 2)  # 2% under competitor
                possible_margin = ((could_have_bid - c_our_cost) / could_have_bid * 100) if could_have_bid > 0 else 0
                actual_margin = ((c_our_sell - c_our_cost) / c_our_sell * 100) if c_our_sell > 0 else 0
                margin_too_high_items.append({
                    "description": comp["our_description"],
                    "our_cost": c_our_cost,
                    "our_sell": c_our_sell,
                    "their_sell": c_their_sell,
                    "actual_margin_pct": round(actual_margin, 1),
                    "could_have_bid": could_have_bid,
                    "possible_margin_pct": round(possible_margin, 1),
                    "wasted_advantage": round(c_our_sell - c_their_sell, 2),
                })
                log.info("MARGIN_ANALYSIS: '%s' — cost $%.2f < their sell $%.2f but we bid $%.2f. "
                         "Could have bid $%.2f at %.1f%% margin.",
                         comp["our_description"][:40], c_our_cost, c_their_sell,
                         c_our_sell, could_have_bid, possible_margin)

    if margin_too_high_items:
        loss_reasons.insert(0,
            f"MARGIN TOO HIGH on {len(margin_too_high_items)} item(s) — "
            f"had LOWER cost than competitor's sell price but bid higher. "
            f"Cost advantage wasted!"
        )
        for mth in margin_too_high_items[:2]:
            loss_reasons.append(
                f"  → '{mth['description'][:35]}': cost ${mth['our_cost']:.2f}, "
                f"bid ${mth['our_sell']:.2f}, they sold at ${mth['their_sell']:.2f}. "
                f"Could have bid ${mth['could_have_bid']:.2f} ({mth['possible_margin_pct']:.0f}% margin)"
            )

    # ── Classify loss reason ─────────────────────────────────────────────
    loss_reason_class = _classify_loss_reason(
        pct_diff, line_comparison, margin_too_high_items
    )
    log.info("LOSS_CLASS: %s — quote %s (%s)",
             loss_reason_class, quote.get("quote_number", "?"),
             "; ".join(loss_reasons[:2]))

    summary = "; ".join(loss_reasons[:3])

    # ── Build report text ─────────────────────────────────────────────────
    report_lines = [
        f"📊 LOSS ANALYSIS: {quote.get('quote_number', '?')}",
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Agency: {quote.get('agency', '?')} — {quote.get('institution', '?')}",
        f"Winner: {winner}",
        f"PO: {po.get('po_number', '?')}",
        f"",
        f"💰 TOTALS",
        f"   Our quote:  ${our_total:>10,.2f}",
        f"   Winning PO: ${winner_total:>10,.2f}",
        f"   Delta:      ${delta:>+10,.2f} ({pct_diff:+.1f}%)",
        f"",
    ]

    if line_comparison and items_compared > 0:
        report_lines.append(f"📋 LINE-BY-LINE ({items_compared} items matched)")
        for comp in line_comparison:
            if comp.get("matched"):
                report_lines.append(
                    f"   {comp['our_description'][:45]}"
                )
                report_lines.append(
                    f"      Us: ${comp['our_unit_price']:,.2f} × {comp['our_qty']}  |  "
                    f"Them: ${comp['winner_unit_price']:,.2f} × {comp['winner_qty']}  "
                    f"({comp['unit_price_pct_diff']:+.1f}%)"
                )
            else:
                report_lines.append(
                    f"   {comp['our_description'][:45]}"
                )
                report_lines.append(
                    f"      Us: ${comp['our_unit_price']:,.2f} × {comp['our_qty']}  |  No match in PO"
                )
        report_lines.append("")

    report_lines.append("🔍 WHY WE LOST")
    for reason in loss_reasons:
        report_lines.append(f"   • {reason}")

    # ── Margin Too High section in report ────────────────────────────────
    if margin_too_high_items:
        report_lines.append("")
        report_lines.append(f"⚠️  MARGIN TOO HIGH ({len(margin_too_high_items)} items)")
        report_lines.append("   You had a LOWER cost than the competitor's sell price")
        report_lines.append("   but your bid was higher — cost advantage wasted!")
        for mth in margin_too_high_items:
            report_lines.append(
                f"   {mth['description'][:45]}"
            )
            report_lines.append(
                f"      Your cost:  ${mth['our_cost']:>8,.2f}"
            )
            report_lines.append(
                f"      Your bid:   ${mth['our_sell']:>8,.2f}  (margin: {mth['actual_margin_pct']:.0f}%)"
            )
            report_lines.append(
                f"      Their sell: ${mth['their_sell']:>8,.2f}"
            )
            report_lines.append(
                f"      Could bid:  ${mth['could_have_bid']:>8,.2f}  (margin: {mth['possible_margin_pct']:.0f}%)"
            )

    report_lines.append("")
    report_lines.append(f"🏷️  LOSS CLASSIFICATION: {loss_reason_class.upper().replace('_', ' ')}")

    report_lines.append("")
    report_lines.append("📈 ACTION TAKEN")
    report_lines.append(f"   • Quote auto-closed as 'lost'")
    report_lines.append(f"   • {items_compared} competitor prices recorded to pricing intelligence")
    report_lines.append(f"   • Product catalog updated with competitor pricing")
    if margin_too_high_items:
        report_lines.append(f"   • {len(margin_too_high_items)} item(s) flagged for margin review")

    report = "\n".join(report_lines)

    return {
        "report": report,
        "summary": summary,
        "line_comparison": line_comparison,
        "items_compared": items_compared,
        "pct_diff": pct_diff,
        "delta": delta,
        "loss_reasons": loss_reasons,
        "loss_reason_class": loss_reason_class,
        "overpriced_count": len(overpriced_items),
        "underpriced_count": len(underpriced_items),
        "margin_too_high_items": margin_too_high_items,
        "margin_too_high_count": len(margin_too_high_items),
    }


# ── Pricing Intelligence Recording ───────────────────────────────────────────

def _record_competitor_prices(conn, po_lines: list, po: dict,
                              agency: str, quote_number: str,
                              our_items: list) -> int:
    """
    Record every competitor line item price into:
      1. price_history — raw price record
      2. product_catalog — update competitor fields
      3. catalog_price_history — timestamped price snapshot
      4. won_quotes KB — for future quote intelligence

    Returns count of prices recorded.
    """
    recorded = 0
    now = datetime.now(timezone.utc).isoformat()
    supplier = po.get("supplier_name", po.get("supplier", ""))
    po_number = po.get("po_number", "")

    for line in po_lines:
        desc = (line.get("description") or "").strip()
        unit_price = line.get("unit_price", 0) or 0
        quantity = line.get("quantity", 0) or 0
        item_id = line.get("item_id", "")

        if not desc or not unit_price or unit_price <= 0:
            continue

        # 1. price_history
        try:
            conn.execute("""
                INSERT INTO price_history
                (found_at, description, part_number, manufacturer, quantity, unit_price,
                 source, source_url, agency, quote_number, price_check_id, notes)
                VALUES (?,?,?,?,?,?,'scprs_award_track','',?,?,?,?)
            """, (now, desc, item_id, supplier, quantity, unit_price,
                  agency, quote_number, "",
                  f"Lost to {supplier} — PO {po_number}"))
        except Exception as e:
            log.debug("price_history insert: %s", e)

        # 2. product_catalog — update competitor_low_price fields
        try:
            # Find matching catalog product
            matches = conn.execute("""
                SELECT id, name, competitor_low_price, sell_price, cost
                FROM product_catalog
                WHERE LOWER(name) LIKE ? OR LOWER(description) LIKE ?
                LIMIT 3
            """, (f"%{desc[:30].lower()}%", f"%{desc[:30].lower()}%")).fetchall()

            for cat_match in matches:
                existing_competitor = cat_match["competitor_low_price"] or 999999
                if unit_price < existing_competitor:
                    conn.execute("""
                        UPDATE product_catalog SET
                            competitor_low_price = ?,
                            competitor_source = ?,
                            competitor_date = ?,
                            scprs_last_price = ?,
                            scprs_last_date = ?,
                            scprs_agency = ?,
                            times_lost = COALESCE(times_lost, 0) + 1,
                            updated_at = ?
                        WHERE id = ?
                    """, (unit_price, f"{supplier} via SCPRS PO {po_number}", now,
                          unit_price, now, agency, now, cat_match["id"]))
                else:
                    # Still update times_lost and scprs data
                    conn.execute("""
                        UPDATE product_catalog SET
                            scprs_last_price = ?,
                            scprs_last_date = ?,
                            scprs_agency = ?,
                            times_lost = COALESCE(times_lost, 0) + 1,
                            updated_at = ?
                        WHERE id = ?
                    """, (unit_price, now, agency, now, cat_match["id"]))

                # 3. catalog_price_history
                try:
                    conn.execute("""
                        INSERT INTO catalog_price_history
                        (product_id, price_type, price, quantity, source,
                         agency, institution, quote_number, recorded_at)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (cat_match["id"], "competitor_scprs", unit_price, quantity,
                          f"{supplier} — PO {po_number}",
                          agency, po.get("dept", ""), quote_number, now))
                except Exception as e:
                    log.debug("catalog_price_history: %s", e)
        except Exception as e:
            log.debug("catalog update: %s", e)

        # 4. won_quotes KB
        try:
            from src.knowledge.won_quotes_db import ingest_scprs_result
            ingest_scprs_result(
                po_number=po_number,
                item_number=item_id,
                description=desc,
                unit_price=unit_price,
                quantity=quantity,
                supplier=supplier,
                department=agency,
                award_date=po.get("start_date", ""),
                source="scprs_award_track",
            )
        except Exception as e:
            log.debug("won_quotes ingest: %s", e)

        recorded += 1

    return recorded


# ── Loss Report Delivery ─────────────────────────────────────────────────────

def _send_loss_reports(reports: list):
    """Send loss analysis reports via SMS, email, and notification bell."""
    if not reports:
        return

    # Build combined report for SMS/email
    has_margin_too_high = any(
        r.get("analysis", {}).get("loss_reason_class") == "margin_too_high"
        for r in reports
    )
    sms_lines = [f"🔴 {len(reports)} QUOTE{'S' if len(reports) > 1 else ''} LOST TO COMPETITORS\n"]
    for r in reports:
        analysis = r.get("analysis", {})
        loss_class = analysis.get("loss_reason_class", "unknown")
        sms_lines.append(
            f"• {r['quote_number']} ({r['institution']}) — "
            f"Lost to {r['winner']} at ${r['winner_total']:,.2f} "
            f"(we quoted ${r['our_total']:,.2f}, {analysis.get('pct_diff', 0):+.1f}%)"
        )
        # Add loss classification context
        if loss_class == "margin_too_high":
            mth_count = analysis.get("margin_too_high_count", 0)
            sms_lines.append(f"  ⚠️ MARGIN TOO HIGH on {mth_count} item(s) — had cost advantage but priced out!")
        elif loss_class == "cost_too_high":
            sms_lines.append(f"  → Cost basis too high (COGS problem, not just pricing)")
        elif loss_class == "relationship_incumbent":
            sms_lines.append(f"  → We were CHEAPER — likely incumbent/relationship advantage")
        elif analysis.get("loss_reasons"):
            sms_lines.append(f"  → {analysis['loss_reasons'][0]}")

    sms_lines.append(f"\n{sum(r['prices_recorded'] for r in reports)} competitor prices added to intel")
    combined_sms = "\n".join(sms_lines)

    # Full report for email
    email_body = "SCPRS Award Tracker — Loss Analysis Report\n"
    email_body += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
    email_body += "=" * 50 + "\n\n"
    for r in reports:
        email_body += r.get("analysis", {}).get("report", "") + "\n\n"
        email_body += "=" * 50 + "\n\n"

    # ── Send via notify_agent ─────────────────────────────────────────────
    try:
        from src.agents.notify_agent import send_alert

        # Use urgent margin_too_high event type if any report has that classification
        event_type = "award_loss_margin_too_high" if has_margin_too_high else "award_loss_detected"
        urgency = "urgent" if has_margin_too_high else "deal"
        title_prefix = "⚠️ MARGIN TOO HIGH" if has_margin_too_high else "🔴"

        loss_classes = list(set(
            r.get("analysis", {}).get("loss_reason_class", "unknown") for r in reports
        ))

        send_alert(
            event_type=event_type,
            title=f"{title_prefix} {len(reports)} Quote{'s' if len(reports)>1 else ''} Lost — Award Tracker",
            body=combined_sms,
            urgency=urgency,
            context={
                "quotes_lost": len(reports),
                "total_value": sum(r["our_total"] for r in reports),
                "winners": list(set(r["winner"] for r in reports)),
                "loss_classes": loss_classes,
                "margin_too_high_count": sum(
                    r.get("analysis", {}).get("margin_too_high_count", 0) for r in reports
                ),
            },
            channels=["sms", "email", "bell"],
            cooldown_key=f"award_loss:{datetime.now().strftime('%Y%m%d')}",
        )
    except Exception as e:
        log.error("Failed to send loss report: %s", e)

    # ── Fire webhook ──────────────────────────────────────────────────────
    try:
        from src.core.webhooks import fire_event
        for r in reports:
            fire_event("quote_lost", {
                "quote_number": r["quote_number"],
                "agency": r["agency"],
                "institution": r["institution"],
                "our_total": f"${r['our_total']:,.2f}",
                "winner": r["winner"],
                "winner_total": f"${r['winner_total']:,.2f}",
                "po_number": r["po_number"],
                "pct_diff": f"{r['analysis']['pct_diff']:+.1f}%",
                "source": "award_tracker",
            })
    except Exception as e:
        log.debug("Webhook: %s", e)

    # ── Store full report in notification for in-app viewing ──────────────
    try:
        from src.core.db import get_db
        with get_db() as nconn:
            for r in reports:
                nconn.execute("""
                    INSERT INTO notifications
                    (event_type, title, body, urgency, context_json, is_read, created_at)
                    VALUES (?,?,?,?,?,0,?)
                """, (
                    "award_loss",
                    f"❌ Lost {r['quote_number']} to {r['winner']}",
                    r["analysis"]["report"],
                    "urgent",
                    json.dumps({
                        "quote_number": r["quote_number"],
                        "winner": r["winner"],
                        "po_number": r["po_number"],
                    }),
                    datetime.now(timezone.utc).isoformat(),
                ))
    except Exception as e:
        log.debug("Notification store: %s", e)

    log.info("Loss reports sent: %d quotes, %d total prices recorded",
             len(reports), sum(r["prices_recorded"] for r in reports))


# ── JSON Sync ─────────────────────────────────────────────────────────────────

def _sync_quote_loss_to_json(quote_number: str, loss_note: str):
    """Update quotes_log.json to reflect the loss (keep JSON in sync with DB)."""
    path = os.path.join(DATA_DIR, "quotes_log.json")
    try:
        with open(path) as f:
            quotes = json.load(f)
        for q in quotes:
            if q.get("quote_number") == quote_number:
                q["status"] = "lost"
                q["status_notes"] = loss_note
                q["closed_by_agent"] = "award_tracker"
                q["updated_at"] = datetime.now(timezone.utc).isoformat()
                break
        with open(path, "w") as f:
            json.dump(quotes, f, indent=2, default=str)
    except Exception as e:
        log.debug("JSON sync failed: %s", e)


# ── Cross-Queue PC Sync ──────────────────────────────────────────────────────

def _sync_linked_pc(pc_id: str, status: str, notes: str):
    """When a quote wins or loses, sync the linked PC record too.

    Updates price_checks.json so the PC doesn't get re-checked by award_monitor.
    """
    pc_path = os.path.join(DATA_DIR, "price_checks.json")
    try:
        with open(pc_path) as f:
            pcs = json.load(f)
        if pc_id in pcs:
            pcs[pc_id]["status"] = status
            pcs[pc_id]["award_status"] = status
            pcs[pc_id]["closed_at"] = datetime.now(timezone.utc).isoformat()
            pcs[pc_id]["closed_reason"] = notes[:200]
            pcs[pc_id]["closed_by"] = "award_tracker_cross_sync"
            with open(pc_path, "w") as f:
                json.dump(pcs, f, indent=2, default=str)
            log.info("CROSS_SYNC: PC %s synced to status=%s (from linked quote)", pc_id, status)
    except Exception as e:
        log.debug("PC cross-sync: %s", e)


# ── Scheduler ─────────────────────────────────────────────────────────────────

def _heartbeat(success: bool = True, error: str = ""):
    try:
        from src.core.scheduler import heartbeat
        heartbeat("award-tracker", success=success, error=error[:200] if error else "")
    except Exception:
        pass


def start_award_tracker(interval_seconds: int = POLL_INTERVAL_SEC):
    """Start the background thread that polls SCPRS aligned to update windows.

    Schedule:
      - Wakes up aligned to SCPRS update times (7am, noon, 5pm PT by default)
      - Per-record adaptive phases:
        Phase 1 (biz days 1-4): check once per day
        Phase 2 (biz days 5-45): check 3x per day at SCPRS windows
        Phase 3 (day 45+): expire, stop checking
    """
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    # Register with central scheduler
    try:
        from src.core.scheduler import register_job
        register_job("award-tracker", interval_seconds)
    except Exception:
        pass

    def _loop():
        time.sleep(120)  # Wait for app boot + other agents to start
        _cycle = 0
        while True:
            try:
                from src.core.scprs_schedule import (
                    is_scprs_check_time, seconds_until_next_window,
                    current_scprs_window, SCPRS_UPDATE_TIMES_PT,
                    record_searches,
                )

                window = current_scprs_window()
                if is_scprs_check_time():
                    log.info("SCHEDULE: SCPRS window %s — starting award check (cycle %d)",
                             window, _cycle)
                    result = run_award_check()
                    log.info(
                        "SCHEDULE: Award check complete — checked=%d, matches=%d, "
                        "losses=%d, prices=%d, scprs_searches=%d",
                        result.get("checked", 0), result.get("matches", 0),
                        result.get("losses", 0), result.get("prices_recorded", 0),
                        result.get("scprs_searches", 0),
                    )
                    # Track searches for rate limiting
                    record_searches(result.get("scprs_searches", 0))

                    # Also run award monitor PC check (unified — delegates to same pipeline)
                    try:
                        from src.agents.award_monitor import run_award_check as monitor_check
                        monitor_check()
                    except Exception as me:
                        log.debug("Award monitor PC check: %s", me)
                else:
                    log.debug("SCHEDULE: Not in SCPRS window (cycle %d), sleeping", _cycle)

                # Sleep until next SCPRS window
                sleep_sec = seconds_until_next_window()
                log.debug("SCHEDULE: Next window in %d seconds (%.1f hours), "
                          "times=%s",
                          sleep_sec, sleep_sec / 3600,
                          [t.strftime("%H:%M") for t in SCPRS_UPDATE_TIMES_PT])

            except Exception as e:
                log.error("Award tracker error: %s", e, exc_info=True)
                _heartbeat(success=False, error=str(e))
                sleep_sec = 1800  # 30 min on error

            _cycle += 1
            time.sleep(sleep_sec)

    t = threading.Thread(target=_loop, daemon=True, name="award-tracker")
    t.start()
    try:
        from src.core.scprs_schedule import SCPRS_UPDATE_TIMES_PT, DAILY_CHECK_PHASE_DAYS, EXPIRY_DAYS
        log.info("Award tracker started (SCPRS-aligned: %s PT, "
                 "daily phase=%d biz days, expiry=%d days)",
                 [t.strftime("%H:%M") for t in SCPRS_UPDATE_TIMES_PT],
                 DAILY_CHECK_PHASE_DAYS, EXPIRY_DAYS)
    except Exception:
        log.info("Award tracker started (SCPRS-aligned schedule)")


# ── Status / Manual API ──────────────────────────────────────────────────────

def get_status() -> dict:
    """Return current tracker status for API/dashboard."""
    _ensure_tables()
    conn = _db()

    # Eligible quotes count
    cutoff = (datetime.now(timezone.utc) - timedelta(days=MIN_DAYS_AFTER_SENT)).isoformat()
    eligible = conn.execute("""
        SELECT COUNT(*) FROM quotes
        WHERE is_test=0 AND status='sent' AND total > 0
          AND ((sent_at IS NOT NULL AND sent_at != '' AND sent_at <= ?)
               OR (sent_at IS NULL OR sent_at = '') AND created_at <= ?)
    """, (cutoff, cutoff)).fetchone()[0]

    # Recent checks
    recent = conn.execute("""
        SELECT checked_at, quote_number, outcome, notes
        FROM award_tracker_log ORDER BY checked_at DESC LIMIT 10
    """).fetchall()

    # Total matches/losses
    total_losses = conn.execute(
        "SELECT COUNT(*) FROM quote_po_matches WHERE outcome='lost_to_competitor'"
    ).fetchone()[0]
    total_wins = conn.execute(
        "SELECT COUNT(*) FROM quote_po_matches WHERE outcome='we_won'"
    ).fetchone()[0]

    conn.close()

    return {
        "ok": True,
        "scheduler_running": _scheduler_started,
        "poll_interval_hours": POLL_INTERVAL_SEC / 3600,
        "min_days_after_sent": MIN_DAYS_AFTER_SENT,
        "eligible_quotes": eligible,
        "last_run": _last_run,
        "last_result": _last_result,
        "total_losses_detected": total_losses,
        "total_wins_detected": total_wins,
        "recent_checks": [dict(r) for r in recent],
    }
