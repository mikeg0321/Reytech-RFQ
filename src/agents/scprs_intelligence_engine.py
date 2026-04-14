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
    from src.core.db import DB_PATH as _DB_PATH
except ImportError:
    _DB_PATH = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data", "reytech.db")

def _db():
    """Return a plain SQLite connection to reytech.db (not the context-manager get_db)."""
    conn = sqlite3.connect(_DB_PATH, timeout=30, check_same_thread=False); conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


# ── Agency Registry ───────────────────────────────────────────────────────────
AGENCY_REGISTRY = {
    "CCHCS": {
        "full_name": "CA Correctional Health Care Services / CDCR",
        "dept_codes": ["5225", "4700"],
        "dept_name_patterns": ["CCHCS", "CORRECTIONAL HEALTH", "CDCR",
                               "CORRECTIONS & REHAB", "CORRECTIONS",
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
                               "DVA", "VETERANS HOMES", "DEPT OF VETERANS"],
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

# ── DVBE / CA certification context ──────────────────────────────────────────
# Reytech is DVBE + SB certified. CA agencies have a legal 3% DVBE spend mandate.
# This means even when an incumbent has lower prices, an agency may PREFER a DVBE
# supplier to satisfy their mandatory quota. This is a structural advantage Mike has.
KNOWN_NON_DVBE_INCUMBENTS = [
    "cardinal health", "mckesson", "medline industries", "medline",
    "grainger", "w.w. grainger", "amazon", "amazon business", "staples",
    "henry schein", "bound tree", "waxie sanitary", "concordance",
    "owens & minor", "ims health", "mckesson medical", "cardinal",
    "patterson", "fisher scientific", "vwr", "thermo fisher",
    "fastenal", "home depot", "office depot", "office max",
    "sysco", "us foods", "performance food",
]

# CA DVBE partner programs — large primes who NEED DVBE subs to win state contracts
DVBE_PARTNER_TARGETS = [
    "cardinal health", "mckesson", "grainger", "medline",
    "henry schein", "bound tree", "concordance",
]

# Search everything — no pre-filtering by product type.
# Mike is willing to source anything. Classification happens post-pull.
# Four layers: (1) blank=all POs, (2) broad category nouns, 
#              (3) competitor names, (4) specific product families
# Format: (search_term, category_hint, reytech_sells_now, priority)
# reytech_sells_now is just a hint — False means "needs sourcing" not "skip"
PRODUCT_SEARCH_PLAN = [
    # ── LAYER 1: Blank search = ALL POs for the department ────────────────
    # The most powerful search. Empty description + dept filter = everything.
    ("",                "all_pos",             None,  "P0"),

    # ── LAYER 2: Competitor name searches (reveals full picture) ──────────
    ("cardinal",        "competitor",          None,  "P0"),
    ("mckesson",        "competitor",          None,  "P0"),
    ("medline",         "competitor",          None,  "P0"),
    ("grainger",        "competitor",          None,  "P0"),
    ("bound tree",      "competitor",          None,  "P0"),
    ("henry schein",    "competitor",          None,  "P1"),
    ("concordance",     "competitor",          None,  "P1"),
    ("waxie",           "competitor",          None,  "P1"),
    ("amazon",          "competitor",          None,  "P1"),
    ("staples",         "competitor",          None,  "P1"),
    ("patterson",       "competitor",          None,  "P1"),

    # ── LAYER 3: Broad product-family nouns (catches everything) ──────────
    ("glove",           "gloves",              True,  "P0"),
    ("mask",            "respiratory",         True,  "P0"),
    ("brief",           "incontinence",        True,  "P0"),
    ("pad",             "absorbent",           True,  "P0"),
    ("supply",          "general_supplies",    None,  "P0"),
    ("medical",         "medical",             None,  "P0"),
    ("safety",          "safety",              True,  "P0"),
    ("vest",            "safety",              True,  "P0"),
    ("N95",             "respiratory",         True,  "P0"),
    ("respirator",      "respiratory",         True,  "P0"),
    ("first aid",       "first_aid",           True,  "P0"),
    ("tourniquet",      "trauma",              True,  "P0"),
    ("bag",             "bags_general",        None,  "P0"),
    ("soap",            "hygiene",             None,  "P0"),
    ("paper",           "paper_products",      None,  "P0"),
    ("towel",           "paper_products",      None,  "P1"),
    ("clean",           "janitorial",          None,  "P1"),
    ("sanitizer",       "hygiene",             None,  "P1"),
    ("gown",            "clinical",            None,  "P1"),
    ("dressing",        "wound_care",          None,  "P1"),
    ("gauze",           "wound_care",          None,  "P1"),
    ("bandage",         "wound_care",          None,  "P1"),
    ("sharps",          "sharps",              None,  "P1"),
    ("catheter",        "clinical",            None,  "P1"),
    ("restraint",       "restraints",          None,  "P1"),

    # ── LAYER 4: Broader non-medical categories ────────────────────────────
    ("uniform",         "apparel",             None,  "P1"),
    ("clothing",        "apparel",             None,  "P1"),
    ("equipment",       "equipment",           None,  "P1"),
    ("tool",            "maintenance",         None,  "P1"),
    ("furniture",       "furniture",           None,  "P1"),
    ("office",          "office",              None,  "P1"),
    ("toner",           "office",              None,  "P1"),
    ("food",            "food_service",        None,  "P2"),
    ("beverage",        "food_service",        None,  "P2"),
    ("chemical",        "chemicals",           None,  "P2"),
    ("battery",         "electronics",         None,  "P2"),
    ("flashlight",      "safety",              None,  "P2"),
    ("signage",         "signage",             None,  "P2"),
    ("print",           "print_services",      None,  "P2"),
    ("software",        "it",                  None,  "P2"),
    ("computer",        "it",                  None,  "P2"),
    ("phone",           "it",                  None,  "P2"),
    ("vehicle",         "fleet",               None,  "P2"),
    ("tire",            "fleet",               None,  "P2"),
    ("fuel",            "fleet",               None,  "P2"),
    ("janitorial",      "janitorial",          None,  "P1"),
    ("disinfect",       "janitorial",          None,  "P1"),
    ("hand",            "hygiene",             None,  "P1"),
    ("wipe",            "hygiene",             None,  "P1"),
    ("liner",           "janitorial",          None,  "P1"),
    ("health",          "medical",             None,  "P1"),
    ("clinical",        "medical",             None,  "P1"),
    ("patient",         "clinical",            None,  "P1"),
    ("protective",      "ppe",                 None,  "P1"),
    ("ppe",             "ppe",                 None,  "P1"),
    ("personal protective", "ppe",             None,  "P1"),
    ("hard hat",        "safety",              None,  "P1"),
    ("eyewear",         "safety",              None,  "P1"),
    ("glasses",         "safety",              None,  "P1"),
    ("boot",            "safety",              None,  "P2"),
    ("shoe",            "safety",              None,  "P2"),
    ("activity",        "recreational",        None,  "P2"),
    ("recreation",      "recreational",        None,  "P2"),
    ("game",            "recreational",        None,  "P2"),
    ("craft",           "recreational",        None,  "P2"),
    ("art",             "recreational",        None,  "P2"),
    ("maintenance",     "maintenance",         None,  "P2"),
    ("repair",          "maintenance",         None,  "P2"),
    ("janitorial",      "janitorial",          None,  "P1"),
]


def _is_target_agency(dept_code: str, dept_name: str, agency_key: str) -> bool:
    """Check if a SCPRS result belongs to the target agency."""
    if agency_key not in AGENCY_REGISTRY:
        return False
    reg = AGENCY_REGISTRY[agency_key]
    # Match by department code
    if dept_code and dept_code in reg.get("dept_codes", []):
        return True
    # Match by department name patterns
    dept_upper = (dept_name or "").upper()
    for pattern in reg.get("dept_name_patterns", []):
        if pattern.upper() in dept_upper:
            return True
    return False


def pull_agency(agency_key: str, search_terms: list = None,
                days_back: int = 365, notify_fn=None,
                from_date_override: str = "", to_date_override: str = "") -> dict:
    """
    Pull SCPRS purchase data for one agency.
    Stores all matching POs + line items to DB.
    Updates price_history with real market prices.

    from_date_override/to_date_override: MM/DD/YYYY format, bypasses days_back.
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
    from_date = from_date_override or (datetime.now() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    to_date = to_date_override or datetime.now().strftime("%m/%d/%Y")
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
                              if _is_target_agency(r.get("dept_code", ""),
                                                   r.get("dept", r.get("dept_name", "")),
                                                   agency_key)]

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
            conn.commit()  # Release DB lock between search terms

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
    import hashlib
    now = datetime.now(timezone.utc).isoformat()
    po_num = po.get("po_number", "")

    # Generate synthetic ID if no PO number (common in SCPRS list view)
    if not po_num:
        key_str = f"{po.get('dept','')}-{po.get('supplier_name','')}-{po.get('grand_total','')}-{po.get('start_date','')}"
        po_num = "SCPRS-" + hashlib.md5(key_str.encode()).hexdigest()[:12].upper()
        po["po_number"] = po_num

    # Parse grand_total to number if string
    def _safe_float(val):
        if val is None: return None
        if isinstance(val, (int, float)): return float(val)
        try: return float(str(val).replace("$","").replace(",","").strip())
        except Exception: return None

    grand_total = _safe_float(po.get("grand_total_num")) or _safe_float(po.get("grand_total"))

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
        """, (now, po_num,
              po.get("dept_code", ""),
              po.get("dept", po.get("dept_name", "")),
              po.get("institution", po.get("dept", po.get("dept_name", ""))),
              agency_key,
              po.get("supplier_name", po.get("supplier", "")),
              po.get("supplier_id", ""),
              po.get("status", ""),
              po.get("start_date", po.get("start_date_parsed", "")),
              po.get("end_date", ""),
              po.get("acq_type", ""),
              po.get("acq_method", ""),
              po.get("merch_amount"),
              grand_total,
              po.get("buyer_name", ""),
              po.get("buyer_email", ""),
              po.get("buyer_phone", ""),
              search_term))
        po_id = cur.lastrowid
        is_new = True

    lines_added = 0
    line_items = po.get("line_items", [])

    # If no detail items, create one from the list-view first_item field
    if not line_items and po.get("first_item"):
        line_items = [{
            "description": po["first_item"],
            "unit_price": grand_total,
            "quantity": 1,
            "line_total": grand_total,
        }]

    for i, line in enumerate(line_items):
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
          line.get("item_id",""), po.get("supplier_name", po.get("supplier","")),
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

    # Competitor analysis — full picture including DVBE angle
    competitors = conn.execute("""
        SELECT p.supplier,
               COUNT(DISTINCT p.agency_key) as agencies_served,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_value,
               GROUP_CONCAT(DISTINCT p.agency_key) as agencies,
               GROUP_CONCAT(DISTINCT l.category) as categories,
               GROUP_CONCAT(DISTINCT p.buyer_email) as buyer_emails
        FROM scprs_po_master p
        LEFT JOIN scprs_po_lines l ON l.po_id=p.id
        WHERE p.supplier != ''
        GROUP BY LOWER(p.supplier)
        HAVING total_value > 1000
        ORDER BY total_value DESC
        LIMIT 25
    """).fetchall()
    
    # Enrich competitors with DVBE context
    competitors_enriched = []
    for c in competitors:
        d = dict(c)
        name = (d.get("supplier") or "").lower()
        d["is_dvbe"] = not any(inc in name for inc in KNOWN_NON_DVBE_INCUMBENTS)
        d["dvbe_displace_opportunity"] = any(inc in name for inc in KNOWN_NON_DVBE_INCUMBENTS)
        d["partner_candidate"] = any(p in name for p in DVBE_PARTNER_TARGETS)
        competitors_enriched.append(d)
    competitors = competitors_enriched

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
        competitors=competitors_enriched,
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


def _generate_recommendations(by_agency, top_gaps, win_back, recent_losses, competitors=None) -> list:
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

    competitors = competitors or []
    # DVBE partnership and competitive analysis
    # For every non-DVBE incumbent found: add targeted DVBE recommendation
    incumbents_seen = set()
    for comp in competitors[:10]:
        name = (comp.get("supplier_name") or "").lower()
        val = comp.get("total_po_value") or 0
        if val < 5000:
            continue
        is_known_non_dvbe = any(inc in name for inc in KNOWN_NON_DVBE_INCUMBENTS)
        is_partner_candidate = any(p in name for p in DVBE_PARTNER_TARGETS)
        base_name = name.title()[:30]
        if is_known_non_dvbe and base_name not in incumbents_seen:
            incumbents_seen.add(base_name)
            ags = (comp.get("agencies") or comp.get("agency_list") or "").split(",")
            # Option A: Displace them directly (DVBE mandate)
            recs.append({
                "priority": "P0",
                "type": "dvbe_displace",
                "action": f"Quote against {base_name} using your DVBE cert",
                "why": (f"{base_name} holds ${val:,.0f} in CA government contracts but is NOT DVBE certified. "
                        f"CA law requires agencies to allocate 3% of spend to DVBEs. "
                        f"Agencies are under pressure to hit that quota — your DVBE cert is a structural advantage regardless of price."),
                "estimated_annual_value": round(val * 0.15),  # conservative 15% capture
                "how": (f"Contact the buyers currently ordering from {base_name}. "
                        f"Lead with DVBE angle: 'We can help you hit your DVBE spend mandate for {', '.join(ags[:2])} '."
                        f" Price to within 10-15% — the DVBE credit often closes the gap."),
                "agencies": ags,
                "dvbe_angle": True,
            })
            if is_partner_candidate:
                # Option B: Partner with them as DVBE sub
                recs.append({
                    "priority": "P1",
                    "type": "dvbe_partner",
                    "action": f"Approach {base_name} as their DVBE subcontractor",
                    "why": (f"{base_name} is a large distributor winning CA state contracts but needs DVBE subs "
                            f"to qualify for certain bids and satisfy agency DVBE requirements. "
                            f"You become their CA government DVBE partner — they bring volume, you bring the cert."),
                    "estimated_annual_value": round(val * 0.08),
                    "how": (f"Call {base_name}'s CA government sales team. Say: "
                            f"'We're a DVBE distributor in CA and we'd like to explore a teaming agreement on your state agency bids.' "
                            f"They likely have active contracts where they need DVBE spend credits."),
                    "agencies": ags,
                    "dvbe_angle": True,
                    "partner_model": True,
                })

    # Add "unknown item" catch-all sourcing recommendation
    # If there are items in DB we haven't classified as selling or gap
    unknown_spend = sum(
        (g.get("total_spend") or 0) for g in top_gaps
        if g.get("category") in ("all_pos", "general_supplies", "equipment", "competitor", None)
    )
    if unknown_spend > 10000:
        recs.append({
            "priority": "P1",
            "type": "source_anything",
            "action": f"Source ${unknown_spend:,.0f} in uncategorized spend",
            "why": (f"SCPRS data shows ${unknown_spend:,.0f} in spend on items you haven't started selling yet. "
                    f"You don't need existing vendor relationships — you can source anything through "
                    f"Grainger, Amazon Business, or direct manufacturer outreach to get competitive."),
            "estimated_annual_value": round(unknown_spend * 0.2),
            "how": (f"Pull the top items from the gaps tab. For each: (1) search Grainger/Amazon for cost, "
                    f"(2) check if you can price within 15% of SCPRS, (3) add to your catalog. "
                    f"Start with the highest-spend items and work down."),
            "agencies": [],
        })

    # Sort by estimated value
    recs.sort(key=lambda r: (-({'P0': 3, 'P1': 2, 'P2': 1}.get(r['priority'],0) * 1e9
                                + (r.get('estimated_annual_value') or 0))))
    return recs[:20]  # Return top 20 not 12


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

        # ── Rescore leads with fresh SCPRS data ──────────────────────
        try:
            from src.agents.lead_nurture_agent import rescore_all_leads
            rescore = rescore_all_leads()
            log.info("Post-pull lead rescore: %s", rescore)
            _engine_status["last_results"]["lead_rescore"] = rescore
        except Exception as _re:
            log.debug("Post-pull lead rescore failed: %s", _re)
        # ── End lead rescore ──────────────────────────────────────────

        # ── Rescore vendors with fresh price/order data ───────────────
        try:
            from src.agents.vendor_intelligence import score_all_vendors
            vscore = score_all_vendors()
            log.info("Post-pull vendor rescore: %s", vscore)
            _engine_status["last_results"]["vendor_rescore"] = vscore
        except Exception as _ve:
            log.debug("Post-pull vendor rescore failed: %s", _ve)
        # ── End vendor rescore ────────────────────────────────────────

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
    try:
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
    except Exception as e:
        return {
            "running": _engine_status.get("running", False),
            "current_agency": _engine_status.get("current_agency", ""),
            "error": str(e),
            "by_agency": [],
            "schedule": [],
            "total_line_items": 0,
            "total_gap_items": 0,
            "quotes_auto_closed": 0,
            "last_results": _engine_status.get("last_results", {}),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SYSTEM 4: Historical Backfill + Monthly Full Pull
# ═══════════════════════════════════════════════════════════════════════════════

def backfill_historical(year: int = 2025, notify_fn=None, force: bool = False) -> dict:
    """
    Pull ALL SCPRS data for an entire year across all agencies.
    This is permanent baseline data — never deleted, used across the app.

    For 2025: pulls Jan 1, 2025 → Dec 31, 2025
    Runs in background thread. Each agency × each search term.
    """
    global _engine_thread, _engine_status
    if _engine_status["running"] and not force:
        return {"ok": False, "message": "Pull already running", "current": _engine_status["current_agency"]}

    from_date = f"01/01/{year}"
    to_date = f"12/31/{year}"

    def _run():
        _engine_status["running"] = True
        total_pos = 0
        total_lines = 0
        results = {}

        try:
            for agency_key in AGENCY_REGISTRY:
                _engine_status["current_agency"] = f"BACKFILL-{year}: {agency_key}"
                log.info(f"Historical backfill {year}: {agency_key}")
                if notify_fn:
                    try:
                        notify_fn("bell", f"📥 Backfill {year}: pulling {agency_key}...", "info")
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)

                try:
                    result = pull_agency(
                        agency_key,
                        from_date_override=from_date,
                        to_date_override=to_date,
                        notify_fn=notify_fn,
                    )
                    results[agency_key] = result
                    total_pos += result.get("new_pos", 0)
                    total_lines += result.get("new_lines", 0)
                    log.info(f"Backfill {year} {agency_key}: {result.get('new_pos',0)} POs, {result.get('new_lines',0)} lines")
                except Exception as e:
                    log.error(f"Backfill {year} {agency_key} FAILED: {e}")
                    import traceback
                    log.error(traceback.format_exc())
                    results[agency_key] = {"ok": False, "error": str(e)}

        except Exception as e:
            log.error(f"Backfill {year} outer loop FAILED: {e}")
            import traceback
            log.error(traceback.format_exc())
        finally:
            _engine_status["running"] = False
            _engine_status["current_agency"] = None

        _engine_status["last_results"]["backfill"] = {
            "year": year, "total_pos": total_pos,
            "total_lines": total_lines, "agencies": results,
        }

        if notify_fn:
            try:
                notify_fn("bell",
                           f"✅ Historical backfill {year} complete: {total_pos} POs, {total_lines} line items",
                           "deal")
            except Exception as _e:
                log.debug("suppressed: %s", _e)

    _engine_thread = threading.Thread(target=_run, daemon=True,
                                       name=f"backfill-{year}")
    _engine_thread.start()
    return {"ok": True, "message": f"Backfill {year} started for all {len(AGENCY_REGISTRY)} agencies",
            "from": from_date, "to": to_date}


def run_monthly_full_pull(notify_fn=None) -> dict:
    """
    Full pull for current month across all agencies.
    Called monthly (1st of month) to permanently capture that month's data.
    """
    now = datetime.now()
    # Pull last 45 days to ensure overlap (catches late-posted POs)
    return pull_all_agencies_background(
        notify_fn=notify_fn,
        priority_filter="all",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SYSTEM 5: Competitor Intelligence
# ═══════════════════════════════════════════════════════════════════════════════

def get_competitor_intelligence(agency_filter: str = "", limit: int = 50) -> dict:
    """
    Who are your competitors? What are they selling? To whom? For how much?
    What contract vehicles do they use?

    Returns structured competitor analysis from SCPRS PO data.
    """
    conn = _db()

    # ── Top competitors by total spend ──
    agency_clause = "AND p.agency_key = ?" if agency_filter else ""
    params = (agency_filter,) if agency_filter else ()

    competitors = conn.execute("""
        SELECT p.supplier,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_spend,
               COUNT(DISTINCT p.institution) as institutions_served,
               COUNT(DISTINCT p.agency_key) as agencies_served,
               GROUP_CONCAT(DISTINCT p.agency_key) as agencies,
               GROUP_CONCAT(DISTINCT p.acq_type) as contract_vehicles,
               MIN(p.start_date) as first_po,
               MAX(p.start_date) as last_po,
               AVG(p.grand_total) as avg_po_value
        FROM scprs_po_master p
        WHERE p.supplier IS NOT NULL AND p.supplier != ''
        " + agency_clause + "
        GROUP BY LOWER(p.supplier)
        ORDER BY total_spend DESC
        LIMIT ?
    """, (*params, limit)).fetchall()

    competitor_list = []
    for c in competitors:
        d = dict(c)
        name = (d["supplier"] or "").lower()
        d["is_dvbe"] = not any(inc in name for inc in KNOWN_NON_DVBE_INCUMBENTS)
        d["dvbe_displace_target"] = any(inc in name for inc in KNOWN_NON_DVBE_INCUMBENTS)
        d["partner_candidate"] = any(p in name for p in DVBE_PARTNER_TARGETS)

        # Get their top product categories
        cats = conn.execute("""
            SELECT l.category, COUNT(*) as cnt, SUM(l.line_total) as cat_spend
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id = p.id
            WHERE LOWER(p.supplier) = ?
            " + agency_clause + "
            GROUP BY l.category ORDER BY cat_spend DESC LIMIT 5
        """, (name, *params)).fetchall()
        d["top_categories"] = [dict(r) for r in cats]

        # Items where Reytech could compete
        reytech_items = conn.execute("""
            SELECT l.description, l.unit_price, l.quantity, l.line_total
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id = p.id
            WHERE LOWER(p.supplier) = ? AND l.reytech_sells = 1
            " + agency_clause + "
            ORDER BY l.line_total DESC LIMIT 10
        """, (name, *params)).fetchall()
        d["reytech_overlap_items"] = [dict(r) for r in reytech_items]
        d["reytech_overlap_value"] = sum(r["line_total"] or 0 for r in reytech_items)

        competitor_list.append(d)

    # ── Contract vehicle breakdown ──
    vehicles = conn.execute("""
        SELECT p.acq_type as vehicle,
               p.acq_method as method,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_spend,
               COUNT(DISTINCT p.supplier) as supplier_count,
               COUNT(DISTINCT p.agency_key) as agency_count,
               GROUP_CONCAT(DISTINCT p.supplier) as top_suppliers
        FROM scprs_po_master p
        WHERE p.acq_type IS NOT NULL AND p.acq_type != ''
        " + agency_clause + "
        GROUP BY p.acq_type, p.acq_method
        ORDER BY total_spend DESC
    """, params).fetchall()

    # ── Institution-level spending ──
    institutions = conn.execute("""
        SELECT p.institution, p.agency_key,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_spend,
               COUNT(DISTINCT p.supplier) as supplier_count,
               GROUP_CONCAT(DISTINCT p.supplier) as top_suppliers,
               MAX(p.start_date) as last_po
        FROM scprs_po_master p
        WHERE p.institution IS NOT NULL AND p.institution != ''
        " + agency_clause + "
        GROUP BY p.institution
        ORDER BY total_spend DESC
        LIMIT 30
    """, params).fetchall()

    # ── Growth opportunities: where competitors win and Reytech can displace ──
    dvbe_opportunities = conn.execute("""
        SELECT p.supplier, p.institution, p.agency_key,
               SUM(p.grand_total) as total_spend,
               COUNT(DISTINCT p.po_number) as po_count,
               p.acq_type as vehicle
        FROM scprs_po_master p
        WHERE LOWER(p.supplier) IN ({','.join('?' for _ in KNOWN_NON_DVBE_INCUMBENTS)})
        " + agency_clause + "
        GROUP BY LOWER(p.supplier), p.institution
        ORDER BY total_spend DESC
        LIMIT 30
    """, (*[s.lower() for s in KNOWN_NON_DVBE_INCUMBENTS], *params)).fetchall()

    # ── Summary stats ──
    stats = conn.execute("""
        SELECT COUNT(DISTINCT po_number) as total_pos,
               COUNT(DISTINCT supplier) as total_suppliers,
               SUM(grand_total) as total_spend,
               COUNT(DISTINCT institution) as total_institutions,
               COUNT(DISTINCT agency_key) as total_agencies,
               MIN(start_date) as earliest_po,
               MAX(start_date) as latest_po
        FROM scprs_po_master
        {"WHERE agency_key = ?" if agency_filter else ""}
    """, params).fetchone()

    conn.close()

    return {
        "ok": True,
        "stats": dict(stats) if stats else {},
        "competitors": competitor_list,
        "contract_vehicles": [dict(v) for v in vehicles],
        "institutions": [dict(i) for i in institutions],
        "dvbe_opportunities": [dict(d) for d in dvbe_opportunities],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def search_scprs_data(query: str, search_type: str = "all",
                       agency: str = "", limit: int = 50) -> dict:
    """
    Dedicated SCPRS search. Search POs, suppliers, items, buyers.

    search_type: all | supplier | item | buyer | po | institution
    """
    conn = _db()
    q = f"%{query.lower()}%"
    results = {"ok": True, "query": query, "results": []}

    if search_type in ("all", "supplier"):
        rows = conn.execute("""
            SELECT LOWER(supplier) as supplier, COUNT(DISTINCT po_number) as pos,
                   SUM(grand_total) as spend, COUNT(DISTINCT institution) as insts,
                   GROUP_CONCAT(DISTINCT agency_key) as agencies,
                   GROUP_CONCAT(DISTINCT acq_type) as vehicles
            FROM scprs_po_master
            WHERE LOWER(supplier) LIKE ?
            GROUP BY LOWER(supplier)
            ORDER BY spend DESC LIMIT ?
        """, (q, limit)).fetchall()
        for r in rows:
            results["results"].append({
                "type": "supplier", "icon": "🏢",
                "name": r["supplier"],
                "detail": f"{r['pos']} POs · ${r['spend'] or 0:,.0f} · {r['insts']} institutions",
                "agencies": r["agencies"],
                "vehicles": r["vehicles"],
            })

    if search_type in ("all", "item"):
        rows = conn.execute("""
            SELECT l.description, AVG(l.unit_price) as avg_price,
                   SUM(l.quantity) as total_qty, SUM(l.line_total) as total_spend,
                   COUNT(DISTINCT p.supplier) as supplier_count,
                   COUNT(DISTINCT p.po_number) as po_count,
                   l.category
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id = p.id
            WHERE LOWER(l.description) LIKE ?
            GROUP BY LOWER(SUBSTR(l.description, 1, 50))
            ORDER BY total_spend DESC LIMIT ?
        """, (q, limit)).fetchall()
        for r in rows:
            results["results"].append({
                "type": "item", "icon": "📦",
                "name": r["description"],
                "detail": f"Avg ${r['avg_price'] or 0:,.2f} · {r['po_count']} POs · {r['supplier_count']} suppliers · ${r['total_spend'] or 0:,.0f} total",
                "category": r["category"],
            })

    if search_type in ("all", "buyer"):
        rows = conn.execute("""
            SELECT buyer_name, buyer_email, institution, agency_key,
                   COUNT(DISTINCT po_number) as pos,
                   SUM(grand_total) as spend
            FROM scprs_po_master
            WHERE (LOWER(buyer_name) LIKE ? OR LOWER(buyer_email) LIKE ?)
              AND buyer_email IS NOT NULL AND buyer_email != ''
            GROUP BY LOWER(buyer_email)
            ORDER BY spend DESC LIMIT ?
        """, (q, q, limit)).fetchall()
        for r in rows:
            results["results"].append({
                "type": "buyer", "icon": "👤",
                "name": f"{r['buyer_name']} <{r['buyer_email']}>",
                "detail": f"{r['institution']} · {r['agency_key']} · {r['pos']} POs · ${r['spend'] or 0:,.0f}",
            })

    if search_type in ("all", "institution"):
        rows = conn.execute("""
            SELECT institution, agency_key,
                   COUNT(DISTINCT po_number) as pos,
                   SUM(grand_total) as spend,
                   COUNT(DISTINCT supplier) as suppliers
            FROM scprs_po_master
            WHERE LOWER(institution) LIKE ? OR LOWER(dept_name) LIKE ?
            GROUP BY institution
            ORDER BY spend DESC LIMIT ?
        """, (q, q, limit)).fetchall()
        for r in rows:
            results["results"].append({
                "type": "institution", "icon": "🏛",
                "name": r["institution"],
                "detail": f"{r['agency_key']} · {r['pos']} POs · {r['suppliers']} suppliers · ${r['spend'] or 0:,.0f}",
            })

    if search_type in ("all", "po"):
        rows = conn.execute("""
            SELECT po_number, supplier, institution, agency_key,
                   grand_total, start_date, acq_type, status
            FROM scprs_po_master
            WHERE po_number LIKE ?
            ORDER BY start_date DESC LIMIT ?
        """, (q, limit)).fetchall()
        for r in rows:
            results["results"].append({
                "type": "po", "icon": "📋",
                "name": r["po_number"],
                "detail": f"{r['supplier']} → {r['institution']} · ${r['grand_total'] or 0:,.0f} · {r['acq_type']} · {r['start_date']}",
            })

    results["total"] = len(results["results"])
    conn.close()
    return results
