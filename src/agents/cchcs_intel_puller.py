"""
cchcs_intel_puller.py — CCHCS Purchasing Intelligence Engine
Phase 32 | Version 1.0.0

Pulls live SCPRS purchase order data for CDCR/CCHCS departments,
stores line-item detail in SQLite, and generates opportunity analysis:
- What is CCHCS buying that Reytech isn't selling them?
- Who are the incumbent vendors (competitors)?
- What are the actual unit prices paid?
- Which facilities are buying what?
- What's the estimated total spend by category?

Runs on Railway (no proxy restriction). Public SCPRS data, no auth needed.
SCPRS is CA government transparency law — all POs are public record.
"""

import os
import re
import json
import time
import logging
import hashlib
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger("cchcs_intel")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    def get_db():
        return sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"))

# ── CCHCS Department Codes in FI$Cal ─────────────────────────────────────────
# These are the official SCPRS business unit codes for CDCR/CCHCS departments
CCHCS_DEPT_CODES = [
    "5225",   # CDCR - Corrections
    "4700",   # CCHCS - Health Care
]

CCHCS_DEPT_NAMES = [
    "CORRECTIONAL HEALTH",
    "CDCR",
    "CCHCS",
    "CORRECTIONS AND REHABILITATION",
    "CA STATE PRISON",
    "CALIFORNIA INSTITUTION",
    "PELICAN BAY",
    "FOLSOM STATE",
    "SAN QUENTIN",
    "MULE CREEK",
    "KERN VALLEY",
    "HIGH DESERT",
    "SALINAS VALLEY",
    "PLEASANT VALLEY",
]

# ── Products Reytech sells (for gap analysis) ─────────────────────────────────
REYTECH_CATALOG = {
    "nitrile gloves": {"sku": "NITRILE-M", "category": "exam_gloves", "sells": True},
    "nitrile exam gloves": {"sku": "NITRILE-M", "category": "exam_gloves", "sells": True},
    "latex gloves": {"sku": None, "category": "exam_gloves", "sells": False},
    "vinyl gloves": {"sku": None, "category": "exam_gloves", "sells": False},
    "adult briefs": {"sku": "BRIEFS-M", "category": "incontinence", "sells": True},
    "incontinence briefs": {"sku": "BRIEFS-M", "category": "incontinence", "sells": True},
    "chux": {"sku": "CHUX-23", "category": "incontinence", "sells": True},
    "underpads": {"sku": "CHUX-23", "category": "incontinence", "sells": True},
    "n95": {"sku": "N95-3M8210", "category": "respiratory", "sells": True},
    "respirator": {"sku": "N95-3M8210", "category": "respiratory", "sells": True},
    "surgical mask": {"sku": None, "category": "respiratory", "sells": False},
    "face mask": {"sku": None, "category": "respiratory", "sells": False},
    "gauze": {"sku": None, "category": "wound_care", "sells": False},
    "wound dressing": {"sku": None, "category": "wound_care", "sells": False},
    "abd pad": {"sku": None, "category": "wound_care", "sells": False},
    "bandage": {"sku": None, "category": "wound_care", "sells": False},
    "sharps container": {"sku": None, "category": "sharps", "sells": False},
    "needle disposal": {"sku": None, "category": "sharps", "sells": False},
    "hand sanitizer": {"sku": None, "category": "hand_hygiene", "sells": False},
    "restraint": {"sku": None, "category": "restraints", "sells": False},
    "trash bag": {"sku": None, "category": "janitorial", "sells": False},
    "paper towel": {"sku": None, "category": "janitorial", "sells": False},
    "disinfectant": {"sku": None, "category": "janitorial", "sells": False},
    "first aid kit": {"sku": "FAK-ANSI-B", "category": "first_aid", "sells": True},
    "tourniquet": {"sku": "CAT-GEN7", "category": "trauma", "sells": True},
    "hi-vis vest": {"sku": "HIVIS-ANSI2", "category": "safety", "sells": True},
}

# ── SCPRS Search Terms → Categories ──────────────────────────────────────────
SEARCH_PLAN = [
    # (search_term, category, priority)
    ("nitrile gloves",      "exam_gloves",      "P0"),
    ("nitrile exam",        "exam_gloves",      "P0"),
    ("vinyl gloves",        "exam_gloves",      "P0"),
    ("adult brief",         "incontinence",     "P0"),
    ("incontinence",        "incontinence",     "P0"),
    ("underpads",           "incontinence",     "P0"),
    ("chux",                "incontinence",     "P0"),
    ("N95",                 "respiratory",      "P0"),
    ("respirator",          "respiratory",      "P0"),
    ("surgical mask",       "respiratory",      "P1"),
    ("wound care",          "wound_care",       "P1"),
    ("gauze",               "wound_care",       "P1"),
    ("ABD pad",             "wound_care",       "P1"),
    ("sharps container",    "sharps",           "P1"),
    ("hand sanitizer",      "hand_hygiene",     "P1"),
    ("restraint",           "restraints",       "P1"),
    ("patient restraint",   "restraints",       "P1"),
    ("trash bag",           "janitorial",       "P2"),
    ("disinfectant",        "janitorial",       "P2"),
    ("paper towel",         "janitorial",       "P2"),
    ("first aid",           "first_aid",        "P1"),
    ("tourniquet",          "trauma",           "P1"),
    ("exam table paper",    "clinical",         "P2"),
    ("tongue depressor",    "clinical",         "P2"),
    ("blood pressure cuff", "clinical",         "P2"),
    ("gown",                "clinical",         "P2"),
    ("scrub",               "clinical",         "P2"),
    ("thermometer",         "clinical",         "P2"),
    ("otoscope",            "clinical",         "P2"),
    ("stethoscope",         "clinical",         "P2"),
    ("oxygen",              "respiratory",      "P2"),
    ("pulse oximeter",      "clinical",         "P2"),
    ("lancet",              "clinical",         "P2"),
    ("glucose",             "clinical",         "P2"),
    ("insulin",             "pharmacy",         "P2"),
    ("syringe",             "pharmacy",         "P2"),
    ("IV bag",              "clinical",         "P2"),
    ("catheter",            "clinical",         "P2"),
    ("colostomy",           "clinical",         "P2"),
    ("compression",         "wound_care",       "P2"),
]


def _get_db():
    """Get a raw SQLite connection (caller must close or use in try/finally)."""
    import sqlite3 as _sql3
    db_path = os.path.join(DATA_DIR, "reytech.db")
    conn = _sql3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = _sql3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_tables():
    """Ensure SCPRS tables exist (idempotent)."""
    conn = _get_db()
    try:
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
        CREATE TABLE IF NOT EXISTS cchcs_supplier_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name TEXT, supplier_id TEXT,
            total_po_value REAL DEFAULT 0, po_count INTEGER DEFAULT 0,
            categories TEXT, agency_codes TEXT,
            first_seen TEXT, last_seen TEXT,
            is_competitor INTEGER DEFAULT 0,
            reytech_can_compete INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS scprs_pull_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pulled_at TEXT, search_term TEXT, dept_filter TEXT,
            results_found INTEGER DEFAULT 0, lines_parsed INTEGER DEFAULT 0,
            new_pos INTEGER DEFAULT 0, error TEXT, duration_sec REAL
        );
        """)
        conn.commit()
    finally:
        conn.close()

try:
    _ensure_tables()
except Exception:
    pass


def _classify_line(description: str) -> dict:
    """Classify a PO line item: category, Reytech sells?, opportunity."""
    desc_lower = (description or "").lower()
    for keyword, data in REYTECH_CATALOG.items():
        if keyword in desc_lower:
            return {
                "category": data["category"],
                "reytech_sells": data["sells"],
                "reytech_sku": data.get("sku"),
                "opportunity_flag": "WIN_BACK" if data["sells"] else "GAP_ITEM",
            }
    return {"category": "other", "reytech_sells": False,
            "reytech_sku": None, "opportunity_flag": None}


def _is_cchcs_dept(dept_code: str, dept_name: str) -> bool:
    """Check if a result is from CDCR/CCHCS."""
    if dept_code in CCHCS_DEPT_CODES:
        return True
    dn = (dept_name or "").upper()
    return any(d in dn for d in CCHCS_DEPT_NAMES)


def _upsert_supplier(conn, supplier: str, supplier_id: str,
                     po_total: float, category: str):
    """Track competitor/supplier spend in cchcs_supplier_map."""
    now = datetime.now(timezone.utc).isoformat()
    existing = conn.execute(
        "SELECT id, total_po_value, po_count, categories FROM cchcs_supplier_map "
        "WHERE supplier_name=?", (supplier,)
    ).fetchone()

    if existing:
        cats = set(json.loads(existing["categories"] or "[]"))
        cats.add(category)
        conn.execute(
            "UPDATE cchcs_supplier_map SET total_po_value=total_po_value+?, "
            "po_count=po_count+1, categories=?, last_seen=? WHERE id=?",
            (po_total or 0, json.dumps(sorted(cats)), now, existing["id"])
        )
    else:
        # Is it a known competitor we could displace?
        known_competitors = ["cardinal", "mckesson", "medline", "grainger",
                             "bound tree", "waxie", "amazon", "staples",
                             "ims health", "henry schein", "owens", "concordance"]
        is_comp = any(c in (supplier or "").lower() for c in known_competitors)
        conn.execute(
            "INSERT INTO cchcs_supplier_map "
            "(supplier_name, supplier_id, total_po_value, po_count, categories, "
            "first_seen, last_seen, is_competitor, reytech_can_compete) "
            "VALUES (?,?,?,1,?,?,?,?,?)",
            (supplier, supplier_id, po_total or 0, json.dumps([category]),
             now, now, 1 if is_comp else 0, 1 if is_comp else 0)
        )


def store_pos(pos: list, search_term: str) -> dict:
    """
    Store parsed PO results into SQLite.
    Returns: {new_pos, new_lines, gap_items, win_back_items}
    """
    conn = _get_db()
    now = datetime.now(timezone.utc).isoformat()
    new_pos = 0
    new_lines = 0
    gap_items = 0
    win_back = 0

    for po in pos:
        po_num = po.get("po_number", "")
        dept_code = po.get("dept_code", "")
        dept_name = po.get("dept_name", "")

        # Only store CCHCS/CDCR POs
        if not _is_cchcs_dept(dept_code, dept_name):
            continue

        # Check if already stored
        existing = conn.execute(
            "SELECT id FROM scprs_po_master WHERE po_number=?", (po_num,)
        ).fetchone()

        if existing:
            po_id = existing["id"]
        else:
            cur = conn.execute("""
                INSERT INTO scprs_po_master
                (pulled_at, po_number, dept_code, dept_name, institution,
                 supplier, supplier_id, status, start_date, end_date,
                 acq_type, acq_method, merch_amount, grand_total,
                 buyer_name, buyer_email, buyer_phone, search_term)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, po_num, dept_code, dept_name,
                  po.get("institution", dept_name),
                  po.get("supplier", ""), po.get("supplier_id", ""),
                  po.get("status", ""), po.get("start_date", ""),
                  po.get("end_date", ""), po.get("acq_type", ""),
                  po.get("acq_method", ""), po.get("merch_amount"),
                  po.get("grand_total"), po.get("buyer_name", ""),
                  po.get("buyer_email", ""), po.get("buyer_phone", ""),
                  search_term))
            po_id = cur.lastrowid
            new_pos += 1

            # Track supplier
            _upsert_supplier(conn, po.get("supplier", ""),
                             po.get("supplier_id", ""),
                             po.get("grand_total", 0), search_term)

        # Store line items
        for i, line in enumerate(po.get("line_items", [])):
            desc = line.get("description", "")
            classification = _classify_line(desc)

            # Skip if already stored for this PO
            already = conn.execute(
                "SELECT id FROM scprs_po_lines WHERE po_id=? AND line_num=?",
                (po_id, i)
            ).fetchone()
            if already:
                continue

            conn.execute("""
                INSERT INTO scprs_po_lines
                (po_id, po_number, line_num, item_id, description, unspsc,
                 uom, quantity, unit_price, line_total, line_status,
                 category, reytech_sells, reytech_sku, opportunity_flag)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (po_id, po_num, i,
                  line.get("item_id", ""), desc,
                  line.get("unspsc", ""), line.get("uom", ""),
                  line.get("quantity"), line.get("unit_price"),
                  line.get("line_total"), line.get("line_status", ""),
                  classification["category"],
                  1 if classification["reytech_sells"] else 0,
                  classification["reytech_sku"],
                  classification["opportunity_flag"]))
            new_lines += 1

            if classification["opportunity_flag"] == "GAP_ITEM":
                gap_items += 1
            elif classification["opportunity_flag"] == "WIN_BACK":
                win_back += 1

    conn.commit()
    conn.close()
    return {"new_pos": new_pos, "new_lines": new_lines,
            "gap_items": gap_items, "win_back_items": win_back}


def run_cchcs_pull(priority: str = "P0", max_terms: int = None) -> dict:
    """
    Execute a full CCHCS purchasing intelligence pull.
    Searches SCPRS for each term in SEARCH_PLAN filtered by priority.
    Stores all CDCR/CCHCS results to DB.
    Returns summary dict.
    """
    try:
        from src.agents.scprs_lookup import FiscalSession
    except ImportError:
        return {"ok": False, "error": "scprs_lookup not available"}

    conn = _get_db()
    pull_start = time.time()
    terms_to_search = [(t, c, p) for t, c, p in SEARCH_PLAN
                       if priority == "all" or p <= priority]
    if max_terms:
        terms_to_search = terms_to_search[:max_terms]

    total_pos = 0
    total_lines = 0
    total_gaps = 0
    errors = []

    try:
        session = FiscalSession()
        if not session.init_session():
            return {"ok": False, "error": "SCPRS session init failed — check Railway connectivity"}
    except Exception as e:
        return {"ok": False, "error": f"Session error: {e}"}

    for search_term, category, term_priority in terms_to_search:
        try:
            log.info(f"CCHCS intel pull: '{search_term}' [{category}]")
            t0 = time.time()

            # Search SCPRS — last 12 months
            from_date = (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y")
            results = session.search(
                description=search_term,
                from_date=from_date,
            )

            # Filter to CCHCS only and get details
            cchcs_results = []
            for po in results:
                dept_code = po.get("dept_code", "")
                dept_name = po.get("dept_name", "")
                if _is_cchcs_dept(dept_code, dept_name):
                    # Get line item detail
                    try:
                        detail = session.get_detail(
                            po.get("_results_html", ""),
                            po.get("_row_index", 0),
                            po.get("_results_action")
                        )
                        if detail:
                            po.update(detail)
                    except Exception as e:
                        log.warning(f"Detail fetch failed for PO {po.get('po_number')}: {e}")
                    cchcs_results.append(po)

            # Store to DB
            stored = store_pos(cchcs_results, search_term)
            total_pos += stored["new_pos"]
            total_lines += stored["new_lines"]
            total_gaps += stored["gap_items"]

            # Log pull
            conn.execute("""
                INSERT INTO scprs_pull_log
                (pulled_at, search_term, dept_filter, results_found, lines_parsed,
                 new_pos, duration_sec)
                VALUES (?, ?, 'CCHCS/CDCR', ?, ?, ?, ?)
            """, (datetime.now(timezone.utc).isoformat(), search_term,
                  len(cchcs_results), stored["new_lines"], stored["new_pos"],
                  round(time.time() - t0, 2)))
            conn.commit()

            time.sleep(1.5)  # Respectful rate limit

        except Exception as e:
            log.error(f"CCHCS pull '{search_term}': {e}")
            errors.append(f"{search_term}: {e}")

    conn.close()
    duration = round(time.time() - pull_start, 1)

    return {
        "ok": True,
        "terms_searched": len(terms_to_search),
        "new_pos_stored": total_pos,
        "new_lines_stored": total_lines,
        "gap_items_found": total_gaps,
        "errors": errors,
        "duration_sec": duration,
    }


def get_cchcs_intelligence() -> dict:
    """
    Return full CCHCS purchasing intelligence analysis from stored DB data.
    """
    conn = _get_db()

    # Total spend captured
    totals = conn.execute("""
        SELECT COUNT(DISTINCT po_number) as po_count,
               SUM(grand_total) as total_spend,
               COUNT(DISTINCT supplier) as supplier_count,
               MIN(start_date) as earliest_po,
               MAX(start_date) as latest_po
        FROM scprs_po_master
        WHERE dept_code IN ('5225','4700')
           OR dept_name LIKE '%CDCR%' OR dept_name LIKE '%CCHCS%'
           OR dept_name LIKE '%CORRECTIONAL%'
    """).fetchone()

    # Spend by category
    by_category = conn.execute("""
        SELECT l.category,
               COUNT(DISTINCT l.po_id) as po_count,
               COUNT(*) as line_count,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_unit_price,
               SUM(CASE WHEN l.reytech_sells=1 THEN l.line_total ELSE 0 END) as reytech_sells_spend,
               SUM(CASE WHEN l.reytech_sells=0 THEN l.line_total ELSE 0 END) as gap_spend
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE (p.dept_code IN ('5225','4700')
           OR p.dept_name LIKE '%CDCR%' OR p.dept_name LIKE '%CCHCS%'
           OR p.dept_name LIKE '%CORRECTIONAL%')
          AND l.category != 'other'
        GROUP BY l.category
        ORDER BY total_spend DESC
    """).fetchall()

    # Top items by spend
    top_items = conn.execute("""
        SELECT l.description,
               l.category,
               l.reytech_sells,
               l.opportunity_flag,
               COUNT(*) as times_purchased,
               SUM(l.quantity) as total_qty,
               AVG(l.unit_price) as avg_unit_price,
               SUM(l.line_total) as total_spend
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE (p.dept_code IN ('5225','4700')
           OR p.dept_name LIKE '%CDCR%' OR p.dept_name LIKE '%CCHCS%'
           OR p.dept_name LIKE '%CORRECTIONAL%')
          AND l.line_total > 0
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC
        LIMIT 50
    """).fetchall()

    # GAP items — what they buy that we DON'T sell
    gap_items = conn.execute("""
        SELECT l.description, l.category,
               COUNT(*) as times_purchased,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_price
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE l.reytech_sells=0
          AND l.opportunity_flag='GAP_ITEM'
          AND (p.dept_code IN ('5225','4700')
           OR p.dept_name LIKE '%CDCR%' OR p.dept_name LIKE '%CCHCS%'
           OR p.dept_name LIKE '%CORRECTIONAL%')
          AND l.line_total > 0
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC
        LIMIT 30
    """).fetchall()

    # WIN_BACK items — things they buy that we DO sell
    win_back = conn.execute("""
        SELECT l.description, l.category, l.reytech_sku,
               p.supplier,
               COUNT(*) as times_purchased,
               SUM(l.line_total) as total_spend,
               AVG(l.unit_price) as avg_price
        FROM scprs_po_lines l
        JOIN scprs_po_master p ON l.po_id=p.id
        WHERE l.reytech_sells=1
          AND l.opportunity_flag='WIN_BACK'
          AND (p.dept_code IN ('5225','4700')
           OR p.dept_name LIKE '%CDCR%' OR p.dept_name LIKE '%CCHCS%'
           OR p.dept_name LIKE '%CORRECTIONAL%')
          AND l.line_total > 0
        GROUP BY LOWER(l.description)
        ORDER BY total_spend DESC
        LIMIT 20
    """).fetchall()

    # Top suppliers (competitors)
    suppliers = conn.execute("""
        SELECT supplier_name, total_po_value, po_count,
               categories, is_competitor, reytech_can_compete
        FROM cchcs_supplier_map
        ORDER BY total_po_value DESC
        LIMIT 20
    """).fetchall()

    # Facility breakdown
    facilities = conn.execute("""
        SELECT institution, dept_name,
               COUNT(DISTINCT po_number) as po_count,
               SUM(grand_total) as total_spend,
               COUNT(DISTINCT supplier) as supplier_count
        FROM scprs_po_master
        WHERE dept_code IN ('5225','4700')
           OR dept_name LIKE '%CDCR%' OR dept_name LIKE '%CCHCS%'
           OR dept_name LIKE '%CORRECTIONAL%'
        GROUP BY institution
        ORDER BY total_spend DESC
    """).fetchall()

    # Pull history
    pull_log = conn.execute("""
        SELECT pulled_at, search_term, results_found, new_pos, duration_sec
        FROM scprs_pull_log
        ORDER BY pulled_at DESC LIMIT 20
    """).fetchall()

    conn.close()

    total_gap_spend = sum(r["gap_spend"] or 0 for r in by_category)
    total_win_back_spend = sum(r["reytech_sells_spend"] or 0 for r in by_category)

    return {
        "totals": dict(totals) if totals else {},
        "by_category": [dict(r) for r in by_category],
        "top_items": [dict(r) for r in top_items],
        "gap_items": [dict(r) for r in gap_items],
        "win_back_items": [dict(r) for r in win_back],
        "suppliers": [dict(r) for r in suppliers],
        "facilities": [dict(r) for r in facilities],
        "pull_log": [dict(r) for r in pull_log],
        "summary": {
            "total_po_value_captured": dict(totals).get("total_spend", 0) if totals else 0,
            "gap_spend_not_selling": total_gap_spend,
            "win_back_spend": total_win_back_spend,
            "data_freshness": dict(pull_log[0]).get("pulled_at") if pull_log else None,
            "pos_in_db": dict(totals).get("po_count", 0) if totals else 0,
        }
    }


def get_pull_status() -> dict:
    """Quick status for /api/cchcs/intel/status."""
    conn = _get_db()
    po_count = conn.execute(
        "SELECT COUNT(*) FROM scprs_po_master"
    ).fetchone()[0]
    line_count = conn.execute(
        "SELECT COUNT(*) FROM scprs_po_lines"
    ).fetchone()[0]
    last_pull = conn.execute(
        "SELECT pulled_at, search_term FROM scprs_pull_log ORDER BY pulled_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return {
        "pos_stored": po_count,
        "lines_stored": line_count,
        "last_pull": dict(last_pull) if last_pull else None,
        "ready": po_count > 0,
    }


# ── Background puller ─────────────────────────────────────────────────────────
_pull_thread = None
_pull_status = {"running": False, "last_result": None}

def pull_in_background(priority: str = "P0"):
    global _pull_thread, _pull_status
    if _pull_status["running"]:
        return {"ok": False, "error": "Pull already running"}
    def _run():
        _pull_status["running"] = True
        try:
            result = run_cchcs_pull(priority=priority)
            _pull_status["last_result"] = result
        except Exception as e:
            _pull_status["last_result"] = {"ok": False, "error": str(e)}
        finally:
            _pull_status["running"] = False
    _pull_thread = threading.Thread(target=_run, daemon=True)
    _pull_thread.start()
    return {"ok": True, "message": f"CCHCS intel pull started (priority={priority})"}
