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
               source_pc_id
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

    if not sent_quotes:
        conn.close()
        result = {"ok": True, "message": "No sent quotes ready for award check",
                  "eligible": 0, "checked": 0, "matches": 0, "losses": 0}
        _last_result = result
        return result

    # ── Filter out recently-checked quotes ────────────────────────────────
    recheck_cutoff = (now - timedelta(hours=RECHECK_INTERVAL_HOURS)).isoformat()
    to_check = []

    for q in sent_quotes:
        if not force:
            last_check = conn.execute("""
                SELECT checked_at FROM award_tracker_log
                WHERE quote_number = ? ORDER BY checked_at DESC LIMIT 1
            """, (q["quote_number"],)).fetchone()
            if last_check and last_check["checked_at"] > recheck_cutoff:
                continue  # Checked recently, skip
        to_check.append(dict(q))

    if not to_check:
        conn.close()
        result = {"ok": True, "message": "All eligible quotes checked recently",
                  "eligible": len(sent_quotes), "checked": 0, "matches": 0, "losses": 0}
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

                # ── Auto-close quote as lost ──────────────────────────────
                loss_note = (
                    f"SCPRS: {supplier} won PO {po_number} at ${scprs_total:,.2f} "
                    f"(we quoted ${our_total:,.2f}). "
                    f"Delta: ${our_total - scprs_total:+,.2f} "
                    f"({analysis['pct_diff']:+.1f}%)"
                )
                try:
                    conn.execute("""
                        UPDATE quotes SET status='lost',
                            status_notes=?, close_reason=?, closed_by_agent='award_tracker',
                            updated_at=?
                        WHERE quote_number=? AND status='sent'
                    """, (loss_note, f"SCPRS: Lost to {supplier}", now_iso, quote_num))
                except Exception as e:
                    log.error("Failed to close quote %s: %s", quote_num, e)

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

                notes = (
                    f"Lost to {supplier} — PO {po_number} ${scprs_total:,.2f} "
                    f"(delta: ${our_total - scprs_total:+,.2f})"
                )
                log.info("❌ %s: LOST to %s — PO %s $%.2f vs our $%.2f",
                         quote_num, supplier, po_number, scprs_total, our_total)

        # ── Log check attempt ─────────────────────────────────────────────
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

    report_lines.append("")
    report_lines.append("📈 ACTION TAKEN")
    report_lines.append(f"   • Quote auto-closed as 'lost'")
    report_lines.append(f"   • {items_compared} competitor prices recorded to pricing intelligence")
    report_lines.append(f"   • Product catalog updated with competitor pricing")

    report = "\n".join(report_lines)

    return {
        "report": report,
        "summary": summary,
        "line_comparison": line_comparison,
        "items_compared": items_compared,
        "pct_diff": pct_diff,
        "delta": delta,
        "loss_reasons": loss_reasons,
        "overpriced_count": len(overpriced_items),
        "underpriced_count": len(underpriced_items),
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
    sms_lines = [f"🔴 {len(reports)} QUOTE{'S' if len(reports) > 1 else ''} LOST TO COMPETITORS\n"]
    for r in reports:
        analysis = r.get("analysis", {})
        sms_lines.append(
            f"• {r['quote_number']} ({r['institution']}) — "
            f"Lost to {r['winner']} at ${r['winner_total']:,.2f} "
            f"(we quoted ${r['our_total']:,.2f}, {analysis.get('pct_diff', 0):+.1f}%)"
        )
        if analysis.get("loss_reasons"):
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
        send_alert(
            event_type="award_loss_detected",
            title=f"🔴 {len(reports)} Quote{'s' if len(reports)>1 else ''} Lost — SCPRS Award Tracker",
            body=combined_sms,
            urgency="deal",
            context={
                "quotes_lost": len(reports),
                "total_value": sum(r["our_total"] for r in reports),
                "winners": list(set(r["winner"] for r in reports)),
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


# ── Scheduler ─────────────────────────────────────────────────────────────────

def _heartbeat(success: bool = True, error: str = ""):
    try:
        from src.core.scheduler import heartbeat
        heartbeat("award-tracker", success=success, error=error[:200] if error else "")
    except Exception:
        pass


def start_award_tracker(interval_seconds: int = POLL_INTERVAL_SEC):
    """Start the background thread that polls SCPRS for award results."""
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
                log.info("Award tracker: starting scan")
                result = run_award_check()
                log.info(
                    "Award tracker: checked=%d, matches=%d, losses=%d, prices=%d",
                    result.get("checked", 0), result.get("matches", 0),
                    result.get("losses", 0), result.get("prices_recorded", 0),
                )
                # Also run award monitor check (merged — was separate thread)
                try:
                    from src.agents.award_monitor import run_award_check as monitor_check
                    monitor_check()
                except Exception as me:
                    log.debug("Award monitor check in tracker: %s", me)
            except Exception as e:
                log.error("Award tracker error: %s", e)
                _heartbeat(success=False, error=str(e))
            _cycle += 1
            time.sleep(interval_seconds)

    t = threading.Thread(target=_loop, daemon=True, name="award-tracker")
    t.start()
    log.info("Award tracker started (polls every %ds = %.1f hrs, checks quotes %dd+ after sent)",
             interval_seconds, interval_seconds / 3600, MIN_DAYS_AFTER_SENT)


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
