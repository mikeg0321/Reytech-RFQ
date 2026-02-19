"""
scprs_intelligence_engine.py — Full Agency SCPRS Intelligence + Automation
Phase 32 | Version 1.0.0

THREE SYSTEMS IN ONE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SYSTEM 1 — FULL AGENCY PULL
  Pulls SCPRS purchase data for ALL 8 agencies on a schedule:
  CCHCS (daily), CalVet (2-day), DSH/CalFire/CDPH (3-day),
  CalTrans/CHP/DGS (weekly)
  Every product category. Every line item. Stored to SQLite.

SYSTEM 2 — PO AWARD MONITOR
  For every open Reytech quote:
    → Search SCPRS for matching POs at same institution/agency
    → If PO found and our quote wasn't the winner → auto close-lost
    → Pull the winning vendor's price → store to price_history
    → Notify Mike with why we lost (price, vendor, amount)

SYSTEM 3 — PRICE INTELLIGENCE LOOP
  After every SCPRS pull:
    → Extract unit prices for all items we sell
    → Update price_history with agency-specific market rates
    → Price check tool reads this for next quote → competitive pricing
    → Growth agent reads gap data → recommends what to start selling
"""

import os
import re
import json
import time
import logging
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

log = logging.getLogger("scprs_intel")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    def get_db():
        return sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"))


# ── Agency Registry ───────────────────────────────────────────────────────────
AGENCY_REGISTRY = {
    "CCHCS": {
        "full_name": "CA Correctional Health Care Services / CDCR",
        "dept_codes": ["5225", "4700"],
        "dept_name_patterns": ["CCHCS", "CORRECTIONAL HEALTH", "CDCR",
                               "CA STATE PRISON", "CALIFORNIA INSTITUTION",
                               "PELICAN BAY", "MULE CREEK", "KERN VALLEY",
                               "SALINAS VALLEY", "FOLSOM", "SAN QUENTIN",
                               "HIGH DESERT", "PLEASANT VALLEY", "IRONWOOD",
                               "AVENAL", "CHUCKAWALLA", "CENTINELA"],
        "priority": "P0", "pull_interval_hours": 24,
        "what_they_buy": ["nitrile gloves", "adult briefs", "chux", "N95",
                          "wound care", "sharps", "restraints", "hand sanitizer"],
    },
    "CalVet": {
        "full_name": "CA Dept of Veterans Affairs — Veterans Homes",
        "dept_codes": ["7700"],
        "dept_name_patterns": ["VETERANS AFFAIRS", "CALVET", "VETERANS HOME",
                               "DVA", "VETERANS HOMES"],
        "priority": "P0", "pull_interval_hours": 48,
        "what_they_buy": ["adult briefs", "incontinence", "wound care",
                          "compression", "nitrile gloves", "activity supplies"],
    },
    "DSH": {
        "full_name": "Dept of State Hospitals",
        "dept_codes": ["4440"],
        "dept_name_patterns": ["STATE HOSPITALS", "DSH", "DEPARTMENT OF STATE HOSPITAL",
                               "ATASCADERO", "COALINGA", "METROPOLITAN", "NAPA",
                               "PATTON", "PORTERVILLE"],
        "priority": "P1", "pull_interval_hours": 72,
        "what_they_buy": ["nitrile gloves", "scrubs", "restraints", "personal care"],
    },
    "CalFire": {
        "full_name": "CA Dept of Forestry and Fire Protection",
        "dept_codes": ["3540"],
        "dept_name_patterns": ["FORESTRY", "CALFIRE", "CAL FIRE", "FIRE PROTECTION",
                               "CDFF", "FIRE STATION"],
        "priority": "P1", "pull_interval_hours": 72,
        "what_they_buy": ["N95 respirators", "hi-vis vests", "first aid kits",
                          "work gloves", "safety glasses"],
    },
    "CDPH": {
        "full_name": "CA Dept of Public Health",
        "dept_codes": ["4260"],
        "dept_name_patterns": ["PUBLIC HEALTH", "CDPH", "DEPARTMENT OF PUBLIC HEALTH"],
        "priority": "P1", "pull_interval_hours": 72,
        "what_they_buy": ["nitrile gloves", "N95", "Tyvek coveralls",
                          "surgical masks", "sanitizer", "PPE"],
    },
    "CalTrans": {
        "full_name": "CA Dept of Transportation",
        "dept_codes": ["2660"],
        "dept_name_patterns": ["TRANSPORTATION", "CALTRANS", "DOT"],
        "priority": "P1", "pull_interval_hours": 168,
        "what_they_buy": ["hi-vis vests", "hard hats", "safety glasses",
                          "work gloves", "first aid kits"],
    },
    "CHP": {
        "full_name": "CA Highway Patrol",
        "dept_codes": ["2720"],
        "dept_name_patterns": ["HIGHWAY PATROL", "CHP"],
        "priority": "P2", "pull_interval_hours": 168,
        "what_they_buy": ["black nitrile gloves", "trauma kits", "tourniquets",
                          "first aid", "N95"],
    },
    "DGS": {
        "full_name": "Dept of General Services",
        "dept_codes": ["1760"],
        "dept_name_patterns": ["GENERAL SERVICES", "DGS"],
        "priority": "P2", "pull_interval_hours": 168,
        "what_they_buy": ["office supplies", "janitorial", "safety equipment"],
    },
}

# Product terms → what Reytech sells (True) or could sell (False)
PRODUCT_SEARCH_PLAN = [
    # (term, category, reytech_sells, priority)
    ("nitrile gloves",      "exam_gloves",      True,  "P0"),
    ("nitrile exam",        "exam_gloves",      True,  "P0"),
    ("vinyl gloves",        "exam_gloves",      False, "P0"),
    ("latex gloves",        "exam_gloves",      False, "P0"),
    ("adult brief",         "incontinence",     True,  "P0"),
    ("incontinence pad",    "incontinence",     True,  "P0"),
    ("underpads",           "incontinence",     True,  "P0"),
    ("chux",                "incontinence",     True,  "P0"),
    ("N95",                 "respiratory",      True,  "P0"),
    ("respirator",          "respiratory",      True,  "P0"),
    ("surgical mask",       "respiratory",      False, "P0"),
    ("face mask",           "respiratory",      False, "P0"),
    ("first aid kit",       "first_aid",        True,  "P0"),
    ("tourniquet",          "trauma",           True,  "P0"),
    ("hi-vis vest",         "safety",           True,  "P0"),
    ("safety vest",         "safety",           True,  "P0"),
    ("wound care",          "wound_care",       False, "P1"),
    ("gauze",               "wound_care",       False, "P1"),
    ("ABD pad",             "wound_care",       False, "P1"),
    ("wound dressing",      "wound_care",       False, "P1"),
    ("sharps container",    "sharps",           False, "P1"),
    ("hand sanitizer",      "hand_hygiene",     False, "P1"),
    ("restraint",           "restraints",       False, "P1"),
    ("patient restraint",   "restraints",       False, "P1"),
    ("hard hat",            "safety",           False, "P1"),
    ("safety glasses",      "safety",           False, "P1"),
    ("work gloves",         "gloves_safety",    False, "P1"),
    ("black nitrile",       "exam_gloves_le",   True,  "P1"),
    ("trash bag",           "janitorial",       False, "P2"),
    ("paper towel",         "janitorial",       False, "P2"),
    ("disinfectant",        "janitorial",       False, "P2"),
    ("exam table paper",    "clinical",         False, "P2"),
    ("gown",                "clinical",         False, "P2"),
    ("catheter",            "clinical",         False, "P2"),
    ("IV bag",              "clinical",         False, "P2"),
    ("blood pressure",      "clinical",         False, "P2"),
    ("thermometer",         "clinical",         False, "P2"),
    ("syringe",             "pharmacy",         False, "P2"),
    ("compression",         "wound_care",       False, "P2"),
    ("activity supplies",   "recreational",     False, "P2"),
    ("recreation",          "recreational",     False, "P2"),
    ("toner",               "office",           False, "P2"),
    ("copy paper",          "office",           False, "P2"),
]


# ── DB helpers ────────────────────────────────────────────────────────────────
def _db():
    conn = get_db()
    conn.row_factory = sqlite3.Row
    return conn


def _is_target_agency(dept_code: str, dept_name: str, agency_key: str) -> bool:
    """Check if a SCPRS result matches the target agency."""
    reg = AGENCY_REGISTRY.get(agency_key, {})
    if dept_code in reg.get("dept_codes", []):
        return True
    dn = (dept_name or "").upper()
    return any(p in dn for p in reg.get("dept_name_patterns", []))


# ── SYSTEM 1: Full Agency SCPRS Pull ─────────────────────────────────────────

def pull_agency(agency_key: str, search_terms: list = None,
                days_back: int = 365, notify_fn=None) -> dict:
    """
    Pull SCPRS purchase data for one agency.
    Stores all matching POs + line items to DB.
    Updates price_history with real market prices.
    """
    try:
        from src.agents.scprs_lookup import FiscalSession
    except ImportError:
        return {"ok": False, "error": "scprs_lookup not available"}

    if agency_key not in AGENCY_REGISTRY:
        return {"ok": False, "error": f"Unknown agency: {agency_key}"}

    if search_terms is None:
        search_terms = [(t, c, s, p) for t, c, s, p in PRODUCT_SEARCH_PLAN]

    conn = _db()
    t_start = time.time()
    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    to_date = datetime.now().strftime("%m/%d/%Y")
    now = datetime.now(timezone.utc).isoformat()

    total_pos = 0
    total_lines = 0
    price_updates = 0
    errors = []

    try:
        session = FiscalSession()
        if not session.init_session():
            return {"ok": False, "error": "SCPRS session init failed"}
    except Exception as e:
        return {"ok": False, "error": f"Session: {e}"}

    log.info(f"SCPRS pull starting: {agency_key} ({len(search_terms)} terms)")
    if notify_fn:
        notify_fn("bell", f"SCPRS pull started: {agency_key}", "info")

    for term, category, we_sell, priority in search_terms:
        try:
            results = session.search(description=term, from_date=from_date, to_date=to_date)
            agency_results = [r for r in results
                              if _is_target_agency(r.get("dept_code",""),
                                                   r.get("dept_name",""), agency_key)]

            for po in agency_results:
                # Get line-item detail
                try:
                    if po.get("_results_html") and po.get("_row_index") is not None:
                        detail = session.get_detail(
                            po.get("_results_html", ""),
                            po.get("_row_index", 0),
                            po.get("_results_action")
                        )
                        if detail:
                            po.update(detail)
                except Exception as e:
                    log.debug(f"Detail error PO {po.get('po_number')}: {e}")

                stored = _store_po(conn, po, agency_key, term, category)
                if stored["is_new"]:
                    total_pos += 1
                total_lines += stored["lines_added"]

                # Feed real prices into price_history
                for line in po.get("line_items", []):
                    if line.get("unit_price") and line["unit_price"] > 0:
                        price_updates += _update_price_history(
                            conn, line, po, agency_key
                        )

            time.sleep(1.2)  # Rate limit

        except Exception as e:
            log.error(f"Pull '{term}' for {agency_key}: {e}")
            errors.append(f"{term}: {str(e)[:60]}")

    # Update pull schedule
    next_pull = (datetime.now() + timedelta(
        hours=AGENCY_REGISTRY[agency_key]["pull_interval_hours"]
    )).isoformat()
    conn.execute("""
        UPDATE scprs_pull_schedule SET last_pull=?, next_pull=? WHERE agency_key=?
    """, (now, next_pull, agency_key))

    # Log the pull
    duration = round(time.time() - t_start, 1)
    conn.execute("""
        INSERT INTO scprs_pull_log
        (pulled_at, search_term, dept_filter, results_found, lines_parsed, new_pos, duration_sec)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (now, f"FULL:{agency_key}", agency_key, total_lines, total_lines, total_pos, duration))
    conn.commit()
    conn.close()

    result = {
        "ok": True, "agency": agency_key,
        "new_pos": total_pos, "new_lines": total_lines,
        "price_updates": price_updates, "duration_sec": duration,
        "errors": errors[:5],
    }
    log.info(f"SCPRS pull complete: {agency_key} — {total_pos} POs, {total_lines} lines, {price_updates} prices, {duration}s")
    if notify_fn:
        notify_fn("bell",
                  f"✅ {agency_key} SCPRS pull: {total_pos} POs, {total_lines} lines, {price_updates} prices updated",
                  "success")
    return result


def _store_po(conn, po: dict, agency_key: str, search_term: str, category: str) -> dict:
    """Upsert PO + lines to DB. Returns {is_new, lines_added}."""
    from src.agents.cchcs_intel_puller import _classify_line
    now = datetime.now(timezone.utc).isoformat()
    po_num = po.get("po_number", "")
    if not po_num:
        return {"is_new": False, "lines_added": 0}

    existing = conn.execute(
        "SELECT id FROM scprs_po_master WHERE po_number=?", (po_num,)
    ).fetchone()

    if existing:
        po_id = existing["id"]
        is_new = False
    else:
        cur = conn.execute("""
            INSERT INTO scprs_po_master
            (pulled_at, po_number, dept_code, dept_name, institution, agency_key,
             supplier, supplier_id, status, start_date, end_date, acq_type,
             acq_method, merch_amount, grand_total, buyer_name, buyer_email,
             buyer_phone, search_term)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (now, po_num, po.get("dept_code",""), po.get("dept_name",""),
              po.get("institution", po.get("dept_name","")), agency_key,
              po.get("supplier",""), po.get("supplier_id",""),
              po.get("status",""), po.get("start_date",""), po.get("end_date",""),
              po.get("acq_type",""), po.get("acq_method",""),
              po.get("merch_amount"), po.get("grand_total"),
              po.get("buyer_name",""), po.get("buyer_email",""),
              po.get("buyer_phone",""), search_term))
        po_id = cur.lastrowid
        is_new = True

    lines_added = 0
    for i, line in enumerate(po.get("line_items", [])):
        existing_line = conn.execute(
            "SELECT id FROM scprs_po_lines WHERE po_id=? AND line_num=?",
            (po_id, i)
        ).fetchone()
        if existing_line:
            continue
        cls = _classify_line(line.get("description",""))
        conn.execute("""
            INSERT INTO scprs_po_lines
            (po_id, po_number, line_num, item_id, description, unspsc, uom,
             quantity, unit_price, line_total, line_status, category,
             reytech_sells, reytech_sku, opportunity_flag)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (po_id, po_num, i, line.get("item_id",""), line.get("description",""),
              line.get("unspsc",""), line.get("uom",""),
              line.get("quantity"), line.get("unit_price"), line.get("line_total"),
              line.get("line_status",""), cls["category"],
              1 if cls["reytech_sells"] else 0, cls["reytech_sku"],
              cls["opportunity_flag"]))
        lines_added += 1

    return {"is_new": is_new, "lines_added": lines_added}


def _update_price_history(conn, line: dict, po: dict, agency_key: str) -> int:
    """Push real SCPRS price into price_history for future quote intelligence."""
    desc = (line.get("description") or "").strip()
    unit_price = line.get("unit_price")
    if not desc or not unit_price or unit_price <= 0:
        return 0
    conn.execute("""
        INSERT INTO price_history
        (found_at, description, part_number, manufacturer, quantity, unit_price,
         source, source_url, agency, quote_number, price_check_id, notes)
        VALUES (?,?,?,?,?,?,'scprs_live','',?,?,?,'SCPRS live PO price')
    """, (datetime.now(timezone.utc).isoformat(), desc,
          line.get("item_id",""), po.get("supplier",""),
          line.get("quantity"), unit_price, agency_key,
          po.get("po_number",""), ""))
    return 1


# ── SYSTEM 2: PO Award Monitor ────────────────────────────────────────────────

def run_po_award_monitor(notify_fn=None) -> dict:
    """
    For every open Reytech quote → search SCPRS for matching awarded PO.
    If found and someone else won → auto close-lost, record competitor price.
    Returns summary of actions taken.
    """
    conn = _db()

    # Get open quotes
    open_quotes = conn.execute("""
        SELECT id, quote_number, agency, institution, total, items_text, created_at
        FROM quotes
        WHERE status IN ('sent','pending','open') AND is_test=0
    """).fetchall()

    if not open_quotes:
        conn.close()
        return {"ok": True, "message": "No open quotes to monitor", "checked": 0}

    try:
        from src.agents.scprs_lookup import FiscalSession
        session = FiscalSession()
        if not session.init_session():
            conn.close()
            return {"ok": False, "error": "SCPRS session unavailable"}
    except Exception as e:
        conn.close()
        return {"ok": False, "error": str(e)}

    now = datetime.now(timezone.utc).isoformat()
    closed_lost = 0
    matched = 0
    actions = []

    for quote in open_quotes:
        q = dict(quote)
        quote_id = q["id"]
        quote_num = q["quote_number"]
        institution = q.get("institution","")
        agency = q.get("agency","")
        our_total = q.get("total", 0)

        # Extract key items from quote
        items_text = q.get("items_text","")
        search_terms = _extract_quote_keywords(items_text)

        # Search SCPRS for each key term at this institution
        created_date = q.get("created_at","")[:10] if q.get("created_at") else ""

        for term in search_terms[:3]:  # Check top 3 keywords max
            try:
                results = session.search(
                    description=term,
                    from_date=created_date or (datetime.now() - timedelta(days=90)).strftime("%m/%d/%Y")
                )

                for po in results:
                    # Does this PO match our quote? (same agency + similar items)
                    confidence, reason = _calculate_match_confidence(q, po, term)

                    if confidence >= 0.6:
                        matched += 1
                        supplier = po.get("supplier","Unknown")
                        scprs_total = po.get("grand_total", 0)
                        outcome = _determine_outcome(q, po)

                        # Store match record
                        conn.execute("""
                            INSERT OR IGNORE INTO quote_po_matches
                            (matched_at, quote_id, quote_number, po_number,
                             scprs_supplier, scprs_total, our_total, match_confidence,
                             outcome, match_method)
                            VALUES (?,?,?,?,?,?,?,?,?,?)
                        """, (now, quote_id, quote_num, po.get("po_number",""),
                              supplier, scprs_total, our_total, confidence,
                              outcome, reason))

                        if outcome == "lost_to_competitor":
                            # Auto close-lost
                            conn.execute("""
                                UPDATE quotes SET status='lost',
                                status_notes=?, updated_at=?
                                WHERE id=?
                            """, (
                                f"Auto-closed: SCPRS shows {supplier} won PO {po.get('po_number','')} "
                                f"at ${scprs_total:,.2f} (our quote: ${our_total:,.2f}). "
                                f"Price delta: ${(our_total - scprs_total):,.2f}",
                                now, quote_id
                            ))
                            conn.execute("""
                                UPDATE quote_po_matches SET auto_closed=1 WHERE quote_id=? AND po_number=?
                            """, (quote_id, po.get("po_number","")))
                            closed_lost += 1

                            loss_msg = (
                                f"Quote {quote_num} AUTO-CLOSED LOST — "
                                f"{supplier} won at ${scprs_total:,.2f} "
                                f"(we quoted ${our_total:,.2f}, "
                                f"{'over by' if our_total > scprs_total else 'under by'} "
                                f"${abs(our_total - scprs_total):,.2f})"
                            )
                            log.info(loss_msg)
                            if notify_fn:
                                notify_fn("bell", loss_msg,
                                         "warn" if our_total > scprs_total else "info")
                            actions.append({
                                "quote": quote_num, "outcome": "closed_lost",
                                "winner": supplier, "winner_price": scprs_total,
                                "our_price": our_total, "po": po.get("po_number","")
                            })

                            # Record competitor price in price_history
                            for line in po.get("line_items", []):
                                if line.get("unit_price"):
                                    _update_price_history(conn, line, po, agency)

                time.sleep(0.8)
            except Exception as e:
                log.debug(f"Monitor '{term}' for {quote_num}: {e}")

    conn.commit()
    conn.close()
    return {
        "ok": True, "quotes_checked": len(open_quotes),
        "matches_found": matched, "auto_closed_lost": closed_lost,
        "actions": actions
    }


def _extract_quote_keywords(items_text: str) -> list:
    """Pull key product terms from a quote's items text."""
    keywords = []
    text = (items_text or "").lower()
    for term, *_ in PRODUCT_SEARCH_PLAN:
        if any(w in text for w in term.lower().split()):
            keywords.append(term)
    return keywords or ["medical supplies"]


def _calculate_match_confidence(quote: dict, po: dict, term: str) -> tuple:
    """Calculate how likely a SCPRS PO matches our quote. Returns (float, reason)."""
    score = 0.0
    reason = []
    agency = (quote.get("agency","")).upper()
    institution = (quote.get("institution","")).upper()
    dept_name = (po.get("dept_name","")).upper()
    dept_code = po.get("dept_code","")

    # Agency match
    for ag_key, reg in AGENCY_REGISTRY.items():
        if ag_key.upper() in agency or agency in ag_key.upper():
            if dept_code in reg["dept_codes"] or any(p in dept_name for p in reg["dept_name_patterns"]):
                score += 0.4
                reason.append("agency_match")
                break

    # Institution name overlap
    if institution and institution in dept_name:
        score += 0.3
        reason.append("institution_match")
    elif institution and any(w in dept_name for w in institution.split() if len(w) > 3):
        score += 0.15
        reason.append("institution_partial")

    # Amount proximity (within 30%)
    our_total = quote.get("total", 0) or 0
    scprs_total = po.get("grand_total", 0) or 0
    if our_total > 0 and scprs_total > 0:
        ratio = min(our_total, scprs_total) / max(our_total, scprs_total)
        if ratio >= 0.9:
            score += 0.3
            reason.append("amount_close")
        elif ratio >= 0.7:
            score += 0.15
            reason.append("amount_approx")

    return min(score, 1.0), "+".join(reason) or "term_match"


def _determine_outcome(quote: dict, po: dict) -> str:
    """Determine if we won, lost, or it's unclear."""
    supplier = (po.get("supplier","")).lower()
    # Check if the supplier is Reytech
    if "reytech" in supplier or "rey tech" in supplier:
        return "we_won"
    # Otherwise it's a loss if there's reasonable confidence
    return "lost_to_competitor"


# ── SYSTEM 3: Growth Gap Analysis ────────────────────────────────────────────

def get_growth_intelligence() -> dict:
    """
    Read SCPRS DB data and generate actionable growth recommendations.
    This feeds the Growth Agent and Manager Agent briefs.
    """
    conn = _db()

    # Total gap spend by agency
    by_agency = conn.execute("""
        SELECT p.agency_key,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_spend,
               SUM(CASE WHEN l.reytech_sells=0 THEN l.line_total ELSE 0 END) as gap_spend,
               SUM(CASE WHEN l.reytech_sells=1 THEN l.line_total ELSE 0 END) as win_back_spend,
               COUNT(DISTINCT p.supplier) as supplier_count
        FROM scprs_po_master p
        LEFT JOIN scprs_po_lines l ON l.po_id=p.id
        WHERE p.agency_key IS NOT NULL
        GROUP BY p.agency_key
        ORDER BY total_spend DESC
    """).fetchall()

    # Top gaps across all agencies
    top_gaps = conn.execute("""
        SELECT l.description, l.category,
               COUNT(DISTINCT p.agency_key) as agency_count,
               COUNT(*) as order_count,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_price,
               GROUP_CONCAT(DISTINCT p.agency_key) as agencies
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE l.reytech_sells=0 AND l.line_total > 0
          AND l.opportunity_flag='GAP_ITEM'
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC
        LIMIT 20
    """).fetchall()

    # Win-back opportunities
    win_back = conn.execute("""
        SELECT l.description, l.category, l.reytech_sku,
               COUNT(DISTINCT p.agency_key) as agency_count,
               COUNT(*) as order_count,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_price,
               GROUP_CONCAT(DISTINCT p.supplier) as incumbent_vendors,
               GROUP_CONCAT(DISTINCT p.agency_key) as agencies
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE l.reytech_sells=1 AND l.line_total > 0
          AND l.opportunity_flag='WIN_BACK'
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC
        LIMIT 20
    """).fetchall()

    # Competitor analysis
    competitors = conn.execute("""
        SELECT p.supplier,
               COUNT(DISTINCT p.agency_key) as agencies_served,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_value,
               GROUP_CONCAT(DISTINCT p.agency_key) as agencies,
               GROUP_CONCAT(DISTINCT l.category) as categories
        FROM scprs_po_master p
        JOIN scprs_po_lines l ON l.po_id=p.id
        WHERE p.supplier != ''
        GROUP BY LOWER(p.supplier)
        HAVING total_value > 1000
        ORDER BY total_value DESC
        LIMIT 15
    """).fetchall()

    # Recently lost quotes
    recent_losses = conn.execute("""
        SELECT q.quote_number, q.agency, q.institution, q.total,
               m.scprs_supplier, m.scprs_total, m.match_confidence
        FROM quotes q
        JOIN quote_po_matches m ON m.quote_id=q.id
        WHERE q.status='lost' AND m.auto_closed=1
        ORDER BY q.updated_at DESC LIMIT 10
    """).fetchall()

    # Pull status
    schedule = conn.execute("""
        SELECT agency_key, last_pull, next_pull, pull_interval_hours
        FROM scprs_pull_schedule ORDER BY priority ASC
    """).fetchall()

    conn.close()

    # Build recommendations
    recommendations = _generate_recommendations(
        by_agency=[dict(r) for r in by_agency],
        top_gaps=[dict(r) for r in top_gaps],
        win_back=[dict(r) for r in win_back],
        recent_losses=[dict(r) for r in recent_losses],
    )

    return {
        "by_agency": [dict(r) for r in by_agency],
        "top_gaps": [dict(r) for r in top_gaps],
        "win_back": [dict(r) for r in win_back],
        "competitors": [dict(r) for r in competitors],
        "recent_losses": [dict(r) for r in recent_losses],
        "pull_schedule": [dict(r) for r in schedule],
        "recommendations": recommendations,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _generate_recommendations(by_agency, top_gaps, win_back, recent_losses) -> list:
    """
    Generate ranked, actionable recommendations from intelligence data.
    Returns list of {priority, action, why, estimated_value, how}.
    """
    recs = []

    # Gap items → immediate product expansion recs
    for gap in top_gaps[:5]:
        spend = gap.get("total_spend") or 0
        if spend < 1000:
            continue
        cat = (gap.get("category") or "other").replace("_"," ")
        agencies = (gap.get("agencies") or "").split(",")
        recs.append({
            "priority": "P0" if spend > 50000 else "P1",
            "type": "add_product",
            "action": f"Start selling {gap.get('description','')[:40]}",
            "why": f"CCHCS/agencies spent ${spend:,.0f} on this item ({gap.get('order_count',0)} orders across {len(agencies)} agencies). You don't sell it yet.",
            "estimated_annual_value": round(spend * 1.2),
            "how": f"Source from Cardinal Health or McKesson. Category: {cat}. Avg price paid: ${gap.get('avg_price') or 0:.2f}.",
            "agencies": agencies,
        })

    # Win-back items → displace incumbent
    for wb in win_back[:5]:
        spend = wb.get("total_spend") or 0
        if spend < 500:
            continue
        vendors = (wb.get("incumbent_vendors") or "Unknown").split(",")[:2]
        agencies = (wb.get("agencies") or "").split(",")
        recs.append({
            "priority": "P0",
            "type": "win_back",
            "action": f"Displace {vendors[0]} on {wb.get('description','')[:35]}",
            "why": f"You already sell {wb.get('reytech_sku','this item')} but agencies paid ${spend:,.0f} to {', '.join(vendors)}. You can take this.",
            "estimated_annual_value": round(spend),
            "how": f"Quote your existing SKU {wb.get('reytech_sku','')} to {', '.join(agencies)}. Beat their price by 5-10%. They already know they need it.",
            "agencies": agencies,
        })

    # Recent losses → pricing lessons
    for loss in recent_losses[:3]:
        our_price = loss.get("total") or 0
        their_price = loss.get("scprs_total") or 0
        if not our_price or not their_price:
            continue
        delta = our_price - their_price
        if delta > 0:
            recs.append({
                "priority": "P0",
                "type": "pricing",
                "action": f"Reprice for {loss.get('agency','')} — lost {loss.get('quote_number','')} by ${delta:,.2f}",
                "why": f"{loss.get('scprs_supplier','Competitor')} won at ${their_price:,.2f}. We quoted ${our_price:,.2f}. We were {(delta/their_price*100):.0f}% too high.",
                "estimated_annual_value": round(their_price),
                "how": f"Pull current cost from vendor, reduce margin to 18-22% for this agency. SCPRS price is your ceiling.",
                "agencies": [loss.get("agency","")],
            })

    # Agency expansion recs
    for ag in by_agency:
        gap = ag.get("gap_spend") or 0
        if gap > 10000:
            recs.append({
                "priority": "P1",
                "type": "expand_agency",
                "action": f"Expand product line at {ag.get('agency_key','')}",
                "why": f"${gap:,.0f} in spend at {ag.get('agency_key','')} going to competitors. Only {ag.get('supplier_count',0)} vendors serving them — room to enter.",
                "estimated_annual_value": round(gap * 0.3),
                "how": f"Use SCPRS gap data to quote exactly what they buy. Add missing products. Pitch SB/DVBE advantage.",
                "agencies": [ag.get("agency_key","")],
            })

    # Sort by estimated value
    recs.sort(key=lambda r: (-({'P0': 3, 'P1': 2, 'P2': 1}.get(r['priority'],0) * 1e9
                                + (r.get('estimated_annual_value') or 0))))
    return recs[:12]


# ── Background scheduler ──────────────────────────────────────────────────────

_engine_thread = None
_engine_status = {"running": False, "current_agency": None, "last_results": {}}

def pull_all_agencies_background(notify_fn=None, priority_filter="P0") -> dict:
    """Kick off full pull for all agencies in background thread."""
    global _engine_thread, _engine_status
    if _engine_status["running"]:
        return {"ok": False, "message": "Pull already running", "current": _engine_status["current_agency"]}

    def _run():
        _engine_status["running"] = True
        agencies_to_pull = [
            k for k, v in AGENCY_REGISTRY.items()
            if priority_filter == "all" or v["priority"] <= priority_filter
        ]
        for agency_key in agencies_to_pull:
            _engine_status["current_agency"] = agency_key
            log.info(f"Starting pull: {agency_key}")
            result = pull_agency(agency_key, notify_fn=notify_fn)
            _engine_status["last_results"][agency_key] = result
        # After pull, run PO award monitor
        _engine_status["current_agency"] = "PO Monitor"
        try:
            monitor_result = run_po_award_monitor(notify_fn=notify_fn)
            _engine_status["last_results"]["po_monitor"] = monitor_result
        except Exception as e:
            log.error(f"PO monitor: {e}")
        _engine_status["running"] = False
        _engine_status["current_agency"] = None
        if notify_fn:
            notify_fn("bell", "✅ Full SCPRS intelligence pull complete — check /intel/growth for insights", "success")

    _engine_thread = threading.Thread(target=_run, daemon=True)
    _engine_thread.start()
    return {"ok": True, "message": f"Full SCPRS pull started ({priority_filter})",
            "agencies": list(AGENCY_REGISTRY.keys())}


def run_scheduled_pulls(notify_fn=None):
    """Called by scheduler — pull agencies due for refresh."""
    conn = _db()
    now = datetime.now(timezone.utc)
    due = conn.execute("""
        SELECT agency_key FROM scprs_pull_schedule
        WHERE enabled=1
          AND (next_pull IS NULL OR next_pull < ?)
        ORDER BY priority ASC
    """, (now.isoformat(),)).fetchall()
    conn.close()
    for row in due:
        pull_agency(row["agency_key"], notify_fn=notify_fn)


def get_engine_status() -> dict:
    conn = _db()
    counts = conn.execute("""
        SELECT agency_key, COUNT(DISTINCT po_number) as pos,
               MAX(pulled_at) as last_pull
        FROM scprs_po_master GROUP BY agency_key
    """).fetchall()
    schedule = conn.execute("""
        SELECT agency_key, last_pull, next_pull, pull_interval_hours
        FROM scprs_pull_schedule
    """).fetchall()
    total_lines = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
    total_gaps = conn.execute(
        "SELECT COUNT(*) FROM scprs_po_lines WHERE opportunity_flag='GAP_ITEM'"
    ).fetchone()[0]
    auto_closed = conn.execute(
        "SELECT COUNT(*) FROM quote_po_matches WHERE auto_closed=1"
    ).fetchone()[0]
    conn.close()
    return {
        "running": _engine_status["running"],
        "current_agency": _engine_status["current_agency"],
        "by_agency": [dict(r) for r in counts],
        "schedule": [dict(r) for r in schedule],
        "total_line_items": total_lines,
        "total_gap_items": total_gaps,
        "quotes_auto_closed": auto_closed,
        "last_results": _engine_status["last_results"],
    }
