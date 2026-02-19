"""
scprs_universal_pull.py — Universal SCPRS Intelligence Engine
Phase 32 | Version 1.0

ONE engine that does everything:
  1. Pulls ALL CA agencies from SCPRS (not just CCHCS)
  2. Stores every PO / line item to SQLite
  3. Matches open Reytech quotes against SCPRS awards — marks closed_lost
  4. Extracts price intelligence for every item
  5. Identifies gap products and competitive threats by agency
  6. Feeds Growth agent and Manager agent with actionable signals

Design: Run Monday + Wednesday at 7am PST (existing scheduler).
        Also triggerable manually via API.
"""

import os
import re
import json
import time
import logging
import sqlite3
import hashlib
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger("scprs_universal")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    def get_db():
        return sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"))

# ── ALL CA Agencies to Monitor ────────────────────────────────────────────────
# dept_code: (friendly_name, priority, Reytech_active)
ALL_AGENCIES = {
    "5225": ("CDCR / Corrections",            "P0", True),
    "4700": ("CCHCS / Correctional Health",   "P0", True),
    "7800": ("CalVet",                         "P0", True),
    "4440": ("DSH / State Hospitals",          "P0", True),
    "3840": ("CalFire",                        "P0", True),
    "4265": ("CDPH / Public Health",           "P0", True),
    "2660": ("CalTrans",                       "P0", True),
    "2720": ("CHP",                            "P0", True),
    "1760": ("DGS",                            "P1", False),
    "6440": ("CalRecycle",                     "P2", False),
    "3100": ("Water Resources",                "P2", False),
    "0250": ("Governor's Office",              "P2", False),
    "5180": ("Social Services",                "P1", False),
    "4150": ("Mental Health",                  "P1", False),
    "7120": ("Veterans Affairs (federal)",     "P1", True),
}

# ── Product search terms (consolidated from cchcs_intel_puller) ───────────────
UNIVERSAL_SEARCH_TERMS = [
    # (term, category, priority)
    ("nitrile gloves",       "exam_gloves",     "P0"),
    ("nitrile exam gloves",  "exam_gloves",     "P0"),
    ("vinyl gloves",         "exam_gloves",     "P0"),
    ("adult briefs",         "incontinence",    "P0"),
    ("incontinence brief",   "incontinence",    "P0"),
    ("chux",                 "incontinence",    "P0"),
    ("underpads",            "incontinence",    "P0"),
    ("N95",                  "respiratory",     "P0"),
    ("respirator",           "respiratory",     "P0"),
    ("surgical mask",        "respiratory",     "P1"),
    ("wound care",           "wound_care",      "P1"),
    ("gauze",                "wound_care",      "P1"),
    ("ABD pad",              "wound_care",      "P1"),
    ("bandage",              "wound_care",      "P1"),
    ("sharps container",     "sharps",          "P1"),
    ("hand sanitizer",       "hand_hygiene",    "P1"),
    ("patient restraint",    "restraints",      "P1"),
    ("first aid kit",        "first_aid",       "P1"),
    ("tourniquet",           "trauma",          "P1"),
    ("hi-vis vest",          "safety",          "P1"),
    ("hard hat",             "safety",          "P1"),
    ("safety glasses",       "safety",          "P1"),
    ("trash bag",            "janitorial",      "P1"),
    ("paper towel",          "janitorial",      "P2"),
    ("disinfectant",         "janitorial",      "P2"),
    ("floor cleaner",        "janitorial",      "P2"),
    ("gown",                 "clinical",        "P2"),
    ("scrubs",               "clinical",        "P2"),
    ("exam table",           "clinical",        "P2"),
    ("thermometer",          "clinical",        "P2"),
    ("blood pressure",       "clinical",        "P2"),
    ("stethoscope",          "clinical",        "P2"),
    ("pulse oximeter",       "clinical",        "P2"),
    ("tongue depressor",     "clinical",        "P2"),
    ("catheter",             "clinical",        "P2"),
    ("syringe",              "pharmacy",        "P2"),
    ("insulin",              "pharmacy",        "P2"),
    ("IV bag",               "clinical",        "P2"),
    ("oxygen",               "respiratory",     "P2"),
    ("colostomy",            "clinical",        "P2"),
    ("compression stocking", "wound_care",      "P2"),
    ("recreational supply",  "recreation",      "P2"),
    ("activity kit",         "recreation",      "P2"),
]

# ── What Reytech sells (for gap classification) ───────────────────────────────
REYTECH_PRODUCTS = {
    "nitrile": True, "exam gloves": True, "vinyl gloves": False,
    "adult brief": True, "incontinence": True, "chux": True, "underpads": True,
    "n95": True, "respirator": True,
    "first aid kit": True, "tourniquet": True,
    "hi-vis": True, "hard hat": True, "safety glasses": True,
    "wound care": False, "gauze": False, "abd pad": False,
    "sharps": False, "hand sanitizer": False, "restraint": False,
    "trash bag": False, "disinfectant": False, "gown": False,
    "recreational": False, "activity": False,
}

def _sells(description: str) -> bool:
    d = (description or "").lower()
    return any(k in d for k, v in REYTECH_PRODUCTS.items() if v)

def _opportunity_flag(description: str) -> str:
    d = (description or "").lower()
    for k, sells in REYTECH_PRODUCTS.items():
        if k in d:
            return "WIN_BACK" if sells else "GAP_ITEM"
    return None

def _dept_name_to_agency(dept_name: str) -> Optional[str]:
    dn = (dept_name or "").upper()
    for code, (name, pri, active) in ALL_AGENCIES.items():
        if any(part in dn for part in name.upper().split(" / ")):
            return code
    return None

def _ensure_schema():
    conn = get_db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS scprs_po_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pulled_at TEXT, po_number TEXT UNIQUE,
        dept_code TEXT, dept_name TEXT, institution TEXT,
        supplier TEXT, supplier_id TEXT, status TEXT,
        start_date TEXT, end_date TEXT,
        acq_type TEXT, acq_method TEXT,
        merch_amount REAL, grand_total REAL,
        buyer_name TEXT, buyer_email TEXT, buyer_phone TEXT,
        search_term TEXT, agency_code TEXT
    );
    CREATE TABLE IF NOT EXISTS scprs_po_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        po_id INTEGER REFERENCES scprs_po_master(id),
        po_number TEXT, line_num INTEGER,
        item_id TEXT, description TEXT, unspsc TEXT,
        uom TEXT, quantity REAL, unit_price REAL,
        line_total REAL, line_status TEXT,
        category TEXT, reytech_sells INTEGER DEFAULT 0,
        reytech_sku TEXT, opportunity_flag TEXT
    );
    CREATE TABLE IF NOT EXISTS scprs_pull_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pulled_at TEXT, search_term TEXT, dept_filter TEXT,
        results_found INTEGER DEFAULT 0, lines_parsed INTEGER DEFAULT 0,
        new_pos INTEGER DEFAULT 0, error TEXT, duration_sec REAL
    );
    CREATE TABLE IF NOT EXISTS cchcs_supplier_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_name TEXT, supplier_id TEXT,
        total_po_value REAL DEFAULT 0, po_count INTEGER DEFAULT 0,
        categories TEXT, agency_codes TEXT,
        first_seen TEXT, last_seen TEXT,
        is_competitor INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_po_dept ON scprs_po_master(dept_code);
    CREATE INDEX IF NOT EXISTS idx_po_supplier ON scprs_po_master(supplier);
    CREATE INDEX IF NOT EXISTS idx_lines_desc ON scprs_po_lines(description);
    CREATE INDEX IF NOT EXISTS idx_lines_opp ON scprs_po_lines(opportunity_flag);
    """)
    conn.commit()
    conn.close()

# ── Quote Auto-Close Logic ────────────────────────────────────────────────────

def check_quotes_against_scprs() -> dict:
    """
    For every open Reytech quote, check SCPRS to see if the agency awarded
    a PO for the same items to a different vendor. If so → close_lost + record why.

    Logic:
    1. Get all quotes in status 'sent' or 'pending'
    2. For each quote, search SCPRS for:
       - Same agency (dept_code) + overlapping date range
       - Same or similar item descriptions
    3. If SCPRS shows a PO awarded to vendor != Reytech for matching items → closed_lost
    4. Record the winning vendor, their price, and the gap in price_history
    5. If no matching PO found, quote stays open (maybe we won, maybe still pending)
    """
    conn = get_db()
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).isoformat()

    open_quotes = conn.execute("""
        SELECT id, quote_number, agency, institution, status,
               total, created_at, items_text, items_detail
        FROM quotes
        WHERE status IN ('sent', 'pending', 'submitted')
          AND is_test = 0
          AND created_at > date('now', '-180 days')
    """).fetchall()

    auto_closed = 0
    checked = 0
    results = []

    for q in open_quotes:
        checked += 1
        q_dict = dict(q)
        agency = q_dict.get("agency", "")
        items_text = q_dict.get("items_text", "")

        if not agency or not items_text:
            continue

        # Find the dept_code for this agency
        dept_code = None
        for code, (name, *_) in ALL_AGENCIES.items():
            if agency.upper() in name.upper() or name.upper() in agency.upper():
                dept_code = code
                break

        if not dept_code:
            continue

        # Search SCPRS for matching POs from same agency, same items
        # Check if SCPRS has a newer PO for similar items from a different vendor
        matching_pos = conn.execute("""
            SELECT p.po_number, p.supplier, p.grand_total, p.start_date, p.buyer_email,
                   l.description, l.unit_price, l.quantity
            FROM scprs_po_master p
            JOIN scprs_po_lines l ON l.po_id = p.id
            WHERE p.dept_code = ?
              AND p.start_date >= ?
              AND p.supplier NOT LIKE '%Reytech%'
              AND p.supplier NOT LIKE '%Rey Tech%'
              AND (
                  """ + " OR ".join([
                      f"LOWER(l.description) LIKE '%{term.lower()}%'"
                      for term in (items_text or "").split(" | ")[:3]
                      if len(term) > 4
                  ] or ["1=0"]) + """
              )
            ORDER BY p.start_date DESC LIMIT 5
        """, (dept_code, q_dict.get("created_at", "")[:10])).fetchall()

        if matching_pos:
            winner = dict(matching_pos[0])
            # Auto-close the quote
            conn.execute("""
                UPDATE quotes SET status='closed_lost',
                    status_notes=?, updated_at=?
                WHERE id=?
            """, (
                f"SCPRS: {winner['supplier']} awarded PO {winner['po_number']} "
                f"on {winner['start_date']} — ${winner['grand_total']:,.0f}. "
                f"Their price: ${winner['unit_price'] or 0:.2f}/{winner['description'][:40]}",
                now, q_dict["id"]
            ))

            # Record price intelligence
            for po_row in matching_pos[:3]:
                po = dict(po_row)
                if po.get("unit_price") and po.get("description"):
                    conn.execute("""
                        INSERT OR IGNORE INTO price_history
                        (found_at, description, unit_price, quantity, source,
                         agency, quote_number, notes)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (now, po["description"], po["unit_price"],
                          po.get("quantity"), "scprs_award",
                          agency, q_dict["quote_number"],
                          f"Won by {po['supplier']} — PO {po['po_number']}"))

            auto_closed += 1
            results.append({
                "quote": q_dict["quote_number"],
                "agency": agency,
                "won_by": winner["supplier"],
                "their_po": winner["po_number"],
                "their_price": winner.get("unit_price"),
                "status": "auto_closed_lost"
            })

    conn.commit()
    conn.close()

    return {
        "quotes_checked": checked,
        "auto_closed": auto_closed,
        "details": results,
        "run_at": now,
    }


# ── Universal Pull ─────────────────────────────────────────────────────────────

_pull_lock = threading.Lock()
_pull_status = {"running": False, "progress": "", "last_result": None}

def run_universal_pull(priority: str = "P0") -> dict:
    """
    Full SCPRS pull for all target agencies.
    priority: "P0" = critical items only | "P1" = + clinical | "all" = everything
    """
    _ensure_schema()

    try:
        from src.agents.scprs_lookup import FiscalSession
    except ImportError:
        return {"ok": False, "error": "scprs_lookup unavailable"}

    conn = get_db()
    t_start = time.time()
    total_new_pos = 0
    total_new_lines = 0
    total_gaps = 0
    errors = []
    now = datetime.now(timezone.utc).isoformat()

    terms = [(t, c, p) for t, c, p in UNIVERSAL_SEARCH_TERMS
             if priority == "all" or p <= priority]

    _pull_status["progress"] = f"Initializing SCPRS session..."

    try:
        session = FiscalSession()
        if not session.init_session():
            return {"ok": False, "error": "SCPRS session failed — check Railway connectivity to fiscal.ca.gov"}
    except Exception as e:
        return {"ok": False, "error": f"Session error: {e}"}

    from_date = (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y")

    for i, (term, category, term_priority) in enumerate(terms):
        _pull_status["progress"] = f"[{i+1}/{len(terms)}] Searching: {term}"
        t0 = time.time()

        try:
            results = session.search(description=term, from_date=from_date)
            new_pos = 0
            new_lines = 0

            for po in results:
                dept_code = po.get("dept_code", "")
                dept_name = po.get("dept_name", "")

                # Resolve agency code
                agency_code = dept_code if dept_code in ALL_AGENCIES else _dept_name_to_agency(dept_name)

                # Get line item detail for all relevant agencies
                if agency_code or any(d in (dept_name or "").upper()
                                       for d in ["CDCR","CCHCS","CALVET","CALFIRE","CDPH","CHP","CALTRANS","DSH"]):
                    try:
                        detail = session.get_detail(
                            po.get("_results_html", ""),
                            po.get("_row_index", 0),
                            po.get("_results_action")
                        )
                        if detail:
                            po.update(detail)
                    except Exception:
                        pass

                po_num = po.get("po_number", "")
                if not po_num:
                    continue

                # Upsert PO master
                exists = conn.execute("SELECT id FROM scprs_po_master WHERE po_number=?", (po_num,)).fetchone()
                if exists:
                    po_id = exists[0]
                else:
                    cur = conn.execute("""
                        INSERT INTO scprs_po_master
                        (pulled_at, po_number, dept_code, dept_name, institution,
                         supplier, supplier_id, status, start_date, end_date,
                         acq_type, acq_method, merch_amount, grand_total,
                         buyer_name, buyer_email, buyer_phone, search_term, agency_code)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (now, po_num, dept_code, dept_name,
                          po.get("institution", dept_name),
                          po.get("supplier",""), po.get("supplier_id",""),
                          po.get("status",""), po.get("start_date",""),
                          po.get("end_date",""), po.get("acq_type",""),
                          po.get("acq_method",""), po.get("merch_amount"),
                          po.get("grand_total"), po.get("buyer_name",""),
                          po.get("buyer_email",""), po.get("buyer_phone",""),
                          term, agency_code or dept_code))
                    po_id = cur.lastrowid
                    new_pos += 1

                    # Track supplier
                    supplier = po.get("supplier", "")
                    if supplier:
                        _upsert_supplier(conn, supplier, po.get("supplier_id",""),
                                         po.get("grand_total", 0), category,
                                         agency_code or dept_code)

                # Store line items
                for j, line in enumerate(po.get("line_items", [])):
                    desc = line.get("description", "")
                    if not desc:
                        continue
                    already = conn.execute(
                        "SELECT id FROM scprs_po_lines WHERE po_id=? AND line_num=?",
                        (po_id, j)
                    ).fetchone()
                    if already:
                        continue
                    sells = _sells(desc)
                    opp = _opportunity_flag(desc)
                    conn.execute("""
                        INSERT INTO scprs_po_lines
                        (po_id, po_number, line_num, item_id, description, unspsc,
                         uom, quantity, unit_price, line_total, line_status,
                         category, reytech_sells, opportunity_flag)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (po_id, po_num, j,
                          line.get("item_id",""), desc,
                          line.get("unspsc",""), line.get("uom",""),
                          line.get("quantity"), line.get("unit_price"),
                          line.get("line_total"), line.get("line_status",""),
                          category, 1 if sells else 0, opp))
                    new_lines += 1
                    if opp == "GAP_ITEM":
                        total_gaps += 1

                    # Feed price intelligence for items we sell
                    if sells and line.get("unit_price") and po.get("supplier"):
                        conn.execute("""
                            INSERT OR IGNORE INTO price_history
                            (found_at, description, unit_price, quantity, source,
                             agency, notes)
                            VALUES (?,?,?,?,?,?,?)
                        """, (now, desc, line["unit_price"], line.get("quantity"),
                               "scprs_market", dept_name or agency_code,
                               f"Paid by {dept_name} to {po.get('supplier','')}"))

            total_new_pos += new_pos
            total_new_lines += new_lines

            conn.execute("""
                INSERT INTO scprs_pull_log
                (pulled_at, search_term, dept_filter, results_found,
                 lines_parsed, new_pos, duration_sec)
                VALUES (?,?,?,?,?,?,?)
            """, (now, term, "all_agencies", len(results),
                  new_lines, new_pos, round(time.time()-t0, 2)))
            conn.commit()

        except Exception as e:
            log.error(f"Pull '{term}': {e}")
            errors.append(f"{term}: {str(e)[:80]}")

        time.sleep(1.2)

    # After pull — auto-check for lost quotes
    close_result = check_quotes_against_scprs()

    conn.close()

    result = {
        "ok": True,
        "terms_searched": len(terms),
        "new_pos": total_new_pos,
        "new_lines": total_new_lines,
        "gap_items": total_gaps,
        "quotes_auto_closed": close_result.get("auto_closed", 0),
        "duration_sec": round(time.time() - t_start, 1),
        "errors": errors,
        "run_at": now,
    }
    _pull_status["last_result"] = result
    _pull_status["progress"] = f"Done — {total_new_pos} new POs, {total_new_lines} lines"
    return result


def _upsert_supplier(conn, supplier, supplier_id, total, category, agency_code):
    now = datetime.now(timezone.utc).isoformat()
    existing = conn.execute(
        "SELECT id, categories, agency_codes, total_po_value FROM cchcs_supplier_map WHERE supplier_name=?",
        (supplier,)
    ).fetchone()
    known_comps = ["cardinal","mckesson","medline","grainger","bound tree","waxie",
                   "henry schein","owens","concordance","ims health","amazon","staples"]
    is_comp = any(c in (supplier or "").lower() for c in known_comps)

    if existing:
        cats = set(json.loads(existing[1] or "[]"))
        codes = set(json.loads(existing[2] or "[]"))
        cats.add(category)
        if agency_code: codes.add(agency_code)
        conn.execute(
            "UPDATE cchcs_supplier_map SET total_po_value=total_po_value+?, "
            "po_count=po_count+1, categories=?, agency_codes=?, last_seen=? WHERE id=?",
            (total or 0, json.dumps(sorted(cats)), json.dumps(sorted(codes)), now, existing[0])
        )
    else:
        conn.execute(
            "INSERT INTO cchcs_supplier_map "
            "(supplier_name, supplier_id, total_po_value, po_count, categories, "
            "agency_codes, first_seen, last_seen, is_competitor) VALUES (?,?,?,1,?,?,?,?,?)",
            (supplier, supplier_id, total or 0, json.dumps([category]),
             json.dumps([agency_code] if agency_code else []), now, now, 1 if is_comp else 0)
        )


def pull_background(priority: str = "P0") -> dict:
    global _pull_status
    if _pull_status.get("running"):
        return {"ok": False, "error": "Pull already running",
                "progress": _pull_status.get("progress","")}
    def _run():
        _pull_status["running"] = True
        _pull_status["start_time"] = datetime.now(timezone.utc).isoformat()
        try:
            r = run_universal_pull(priority=priority)
            _pull_status["last_result"] = r
        except Exception as e:
            _pull_status["last_result"] = {"ok": False, "error": str(e)}
        finally:
            _pull_status["running"] = False
            _pull_status["progress"] = _pull_status.get("progress","")
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": f"Universal SCPRS pull started (priority={priority})",
            "terms": len([t for t,c,p in UNIVERSAL_SEARCH_TERMS if priority=="all" or p<=priority])}


def get_pull_status() -> dict:
    conn = get_db()
    po_count  = conn.execute("SELECT COUNT(*) FROM scprs_po_master").fetchone()[0]
    line_count = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
    agency_count = conn.execute("SELECT COUNT(DISTINCT dept_code) FROM scprs_po_master").fetchone()[0]
    last = conn.execute(
        "SELECT pulled_at, search_term, new_pos FROM scprs_pull_log ORDER BY pulled_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return {
        "pos_stored": po_count, "lines_stored": line_count,
        "agencies_seen": agency_count,
        "last_pull": dict(zip(["pulled_at","search_term","new_pos"], last)) if last else None,
        "running": _pull_status.get("running", False),
        "progress": _pull_status.get("progress", ""),
        "last_result": _pull_status.get("last_result"),
    }


def get_universal_intelligence(agency_code: str = None) -> dict:
    """Full cross-agency intelligence summary."""
    _ensure_schema()
    conn = get_db()
    conn.row_factory = sqlite3.Row

    where = f"AND p.agency_code='{agency_code}'" if agency_code else ""

    # Totals
    totals = conn.execute(f"""
        SELECT COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_spend,
               COUNT(DISTINCT p.supplier) as supplier_count,
               COUNT(DISTINCT p.dept_code) as agency_count
        FROM scprs_po_master p WHERE 1=1 {where}
    """).fetchone()

    # By agency
    by_agency = conn.execute(f"""
        SELECT p.dept_name, p.dept_code, p.agency_code,
               COUNT(DISTINCT p.po_number) as po_count,
               SUM(p.grand_total) as total_spend,
               SUM(CASE WHEN l.reytech_sells=1 THEN l.line_total ELSE 0 END) as we_sell_spend,
               SUM(CASE WHEN l.reytech_sells=0 THEN l.line_total ELSE 0 END) as gap_spend
        FROM scprs_po_master p
        JOIN scprs_po_lines l ON l.po_id=p.id
        WHERE 1=1 {where}
        GROUP BY p.dept_code
        ORDER BY total_spend DESC
    """).fetchall()

    # Gap items — products they buy we don't sell
    gaps = conn.execute(f"""
        SELECT l.description, l.category,
               COUNT(*) as times_ordered,
               COUNT(DISTINCT p.dept_code) as agencies_buying,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_price
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE l.opportunity_flag='GAP_ITEM' AND l.line_total > 0 {where}
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC LIMIT 30
    """).fetchall()

    # Win-back — products they buy that we sell (from competitors)
    win_back = conn.execute(f"""
        SELECT l.description, l.category,
               p.supplier as incumbent_vendor,
               COUNT(*) as times_ordered,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as their_price
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE l.opportunity_flag='WIN_BACK' AND l.line_total > 0 {where}
        GROUP BY LOWER(l.description), p.supplier
        ORDER BY total_spend DESC LIMIT 25
    """).fetchall()

    # Competitors by agency
    competitors = conn.execute("""
        SELECT supplier_name, total_po_value, po_count,
               categories, agency_codes, is_competitor
        FROM cchcs_supplier_map
        WHERE is_competitor=1
        ORDER BY total_po_value DESC LIMIT 15
    """).fetchall()

    # Auto-closed quotes
    auto_closed = conn.execute("""
        SELECT quote_number, agency, status_notes, updated_at
        FROM quotes WHERE status='closed_lost'
          AND status_notes LIKE 'SCPRS:%'
        ORDER BY updated_at DESC LIMIT 10
    """).fetchall()

    conn.close()

    return {
        "totals": dict(totals) if totals else {},
        "by_agency": [dict(r) for r in by_agency],
        "gap_items": [dict(r) for r in gaps],
        "win_back": [dict(r) for r in win_back],
        "competitors": [dict(r) for r in competitors],
        "auto_closed_quotes": [dict(r) for r in auto_closed],
        "summary": {
            "total_market_spend": (dict(totals).get("total_spend") or 0) if totals else 0,
            "gap_opportunity": sum(r["total_spend"] or 0 for r in gaps),
            "win_back_opportunity": sum(r["total_spend"] or 0 for r in win_back),
            "agencies_tracked": (dict(totals).get("agency_count") or 0) if totals else 0,
        }
    }
