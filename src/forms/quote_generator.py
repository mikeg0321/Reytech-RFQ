


"""
Reytech Quote PDF Generator v2
================================
Pixel-perfect professional quotes matching QuoteWerks output.
Colors, fonts, positions extracted from actual R26Q14 PDFs.

Features:
  - Exact #C3C3E0 lavender fill, #46468D / #004080 borders
  - Logo upload support (PNG/JPG at data/reytech_logo.*)
  - Agency-specific layouts (CCHCS, CDCR, CalVet, DGS)
  - Dynamic row heights (1-line items and 6-line items)
  - Sequential quote numbering R{YY}Q{seq}, annual reset Jan 1
  - Searchable quotes database (quotes_log.json)
  - Multi-page with header repeat
"""

import os
import re
import json
import logging
import glob
from datetime import datetime, timedelta
from typing import Optional

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import Color, HexColor
from reportlab.lib.utils import simpleSplit, ImageReader
from reportlab.pdfgen import canvas

log = logging.getLogger("quote_gen")

try:
    from src.forms.reytech_filler_v4 import _normalize_item
except ImportError:
    def _normalize_item(item):
        """Fallback normalizer."""
        n = dict(item)
        n["description"] = (item.get("description") or item.get("desc") or "").strip()
        qty = item.get("qty") or item.get("quantity") or 0
        try: n["qty"] = float(str(qty).replace(",", ""))
        except (ValueError, TypeError): n["qty"] = 0
        price = item.get("price_per_unit") or item.get("bid_price") or item.get("unit_price") or 0
        try: n["price_per_unit"] = float(str(price).replace("$", "").replace(",", ""))
        except (ValueError, TypeError): n["price_per_unit"] = 0
        cost = item.get("supplier_cost") or item.get("cost") or 0
        try: n["supplier_cost"] = float(str(cost).replace("$", "").replace(",", ""))
        except (ValueError, TypeError): n["supplier_cost"] = 0
        n["part_number"] = str(item.get("part_number") or item.get("item_number") or "")
        n["uom"] = str(item.get("uom") or item.get("UOM") or "EA")
        return n

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")

# ═══════════════════════════════════════════════════════════════════════════════
# EXACT COLORS — extracted from QuoteWerks R26Q14 via pdfplumber
# ═══════════════════════════════════════════════════════════════════════════════
FILL    = Color(0.765, 0.765, 0.882)   # #C3C3E0  lavender header fill
LBL_BD  = Color(0.278, 0.278, 0.553)   # #46468D  label cell border
VAL_BD  = Color(0.0, 0.251, 0.502)     # #004080  value cell border (navy)
TBL_BD  = Color(0.278, 0.278, 0.553)   # #46468D  table grid borders
BLACK   = HexColor("#000000")
WHITE   = HexColor("#FFFFFF")
GRAY    = HexColor("#555555")
NAVY    = HexColor("#1a2744")           # brand accent for text logo
ALT_ROW = Color(0.96, 0.96, 0.98)      # subtle alternate row

# ═══════════════════════════════════════════════════════════════════════════════
# COMPANY INFO
# ═══════════════════════════════════════════════════════════════════════════════
REYTECH = {
    "name":     "Reytech Inc.",
    "line1":    "30 Carnoustie Way",
    "line2":    "Trabuco Canyon, CA 92679",
    "contact":  "Michael Guadan",
    "title":    "Owner",
    "phone":    "949-229-1575",
    "email":    "sales@reytechinc.com",
    "permit":   "245652416-00001",
    "sb_mb":    "2002605",
    "dvbe":     "2002605",
}

# ═══════════════════════════════════════════════════════════════════════════════
# AGENCY CONFIGS — each has different quoting requirements
# ═══════════════════════════════════════════════════════════════════════════════
AGENCY_CONFIGS = {
    "CCHCS": {
        "full_name": "California Correctional Health Care Services",
        "show_bill_to": True,
        "show_permit": True,
        "bill_to_name": "Dept. of Corrections and Rehabilitation",
        "bill_to_lines": ["Attn: Accounts Payable", "P.O. BOX 187021", "Sacramento, CA 95818-7021", "APA.Invoices@cdcr.ca.gov"],
        "default_tax": 0.0725,
        "default_terms": "Net 45",
    },
    "CDCR": {
        "full_name": "Dept. of Corrections and Rehabilitation",
        "show_bill_to": True,
        "show_permit": True,
        "bill_to_name": "Dept. of Corrections and Rehabilitation",
        "bill_to_lines": ["P.O. Box 187021", "Sacramento, CA 95818-7021", "United States"],
        "default_tax": 0.0725,
        "default_terms": "Net 45",
    },
    "CalVet": {
        "full_name": "California Department of Veterans Affairs",
        "show_bill_to": True,
        "show_permit": True,
        "bill_to_name": "California Department of Veterans Affairs",
        "bill_to_lines": ["APinvoices@calvet.ca.gov", "1227 \"O\" Street, Room 403", "Sacramento, CA 95814", "United States"],
        "default_tax": 0.0725,
        "default_terms": "Net 45",
    },
    "DGS": {
        "full_name": "Department of General Services",
        "show_bill_to": True,
        "show_permit": True,
        "bill_to_name": "Department of General Services",
        "bill_to_lines": ["707 Third Street", "West Sacramento, CA 95605", "United States"],
        "default_tax": 0.0725,
        "default_terms": "Net 45",
    },
    "DEFAULT": {
        "full_name": "",
        "show_bill_to": True,
        "show_permit": True,
        "bill_to_name": "",
        "bill_to_lines": [],
        "default_tax": 0.0725,
        "default_terms": "Net 45",
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# FACILITY DATABASE — maps abbreviations/names to parent agency + full address
# Used for To: (parent agency) and Ship To: (facility + address)
# ═══════════════════════════════════════════════════════════════════════════════

FACILITY_DB = {
    # CDCR facilities
    "CIW":  {"name": "CIW - California Institution for Women", "parent": "CCHCS", "parent_full": "California Correctional Health Care Services", "address": ["16756 Chino-Corona Road", "Corona, CA 92880"]},
    "CIM":  {"name": "CIM - California Institution for Men", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["14901 S Central Ave", "Chino, CA 91710"]},
    "CSP-SAC": {"name": "CSP Sacramento - New Folsom", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["300 Prison Road", "Represa, CA 95671"]},
    "CSP-COR": {"name": "CSP Corcoran", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["4001 King Ave", "Corcoran, CA 93212"]},
    "CSP-LAC": {"name": "CSP Los Angeles County", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["44750 60th St West", "Lancaster, CA 93536"]},
    "CSP-SOL": {"name": "CSP Solano", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["2100 Peabody Road", "Vacaville, CA 95687"]},
    "SATF": {"name": "SATF - Substance Abuse Treatment Facility", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["900 Quebec Ave", "Corcoran, CA 93212"]},
    "CHCF": {"name": "CHCF - California Health Care Facility", "parent": "CCHCS", "parent_full": "California Correctional Health Care Services", "address": ["23370 Road 22", "Stockton, CA 95215"]},
    "PVSP": {"name": "PVSP - Pleasant Valley State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["24863 W Jayne Ave", "Coalinga, CA 93210"]},
    "KVSP": {"name": "KVSP - Kern Valley State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["3000 W Cecil Ave", "Delano, CA 93215"]},
    "NKSP": {"name": "NKSP - North Kern State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["2737 W Cecil Ave", "Delano, CA 93215"]},
    "MCSP": {"name": "MCSP - Mule Creek State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["4001 Hwy 104", "Ione, CA 95640"]},
    "WSP":  {"name": "WSP - Wasco State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["701 Scofield Ave", "Wasco, CA 93280"]},
    "SCC":  {"name": "SCC - Sierra Conservation Center", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["5100 O'Byrnes Ferry Road", "Jamestown, CA 95327"]},
    "CMC":  {"name": "CMC - California Men's Colony", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["Hwy 1", "San Luis Obispo, CA 93409"]},
    "CTF":  {"name": "CTF - Correctional Training Facility", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["Hwy 101 North", "Soledad, CA 93960"]},
    "CCWF": {"name": "CCWF - Central California Women's Facility", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["23370 Road 22", "Chowchilla, CA 93610"]},
    "VSP":  {"name": "VSP - Valley State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["21633 Avenue 24", "Chowchilla, CA 93610"]},
    "SVSP": {"name": "SVSP - Salinas Valley State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["31625 Hwy 101", "Soledad, CA 93960"]},
    "PBSP": {"name": "PBSP - Pelican Bay State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["5905 Lake Earl Dr", "Crescent City, CA 95531"]},
    "CRC":  {"name": "CRC - California Rehabilitation Center", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["5th Street & Western Ave", "Norco, CA 92860"]},
    "CCI":  {"name": "CCI - California Correctional Institution", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["24900 Hwy 202", "Tehachapi, CA 93561"]},
    "ASP":  {"name": "ASP - Avenal State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["1 Kings Way", "Avenal, CA 93204"]},
    "HDSP": {"name": "HDSP - High Desert State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["475-750 Rice Canyon Rd", "Susanville, CA 96127"]},
    "ISP":  {"name": "ISP - Ironwood State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["19005 Wiley's Well Rd", "Blythe, CA 92225"]},
    "FSP":  {"name": "FSP - Folsom State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["300 Prison Road", "Represa, CA 95671"]},
    "RJD":  {"name": "RJD - Richard J. Donovan Correctional Facility", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["480 Alta Road", "San Diego, CA 92179"]},
    "CAL":  {"name": "CAL - Calipatria State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["7018 Blair Rd", "Calipatria, CA 92233"]},
    "CEN":  {"name": "CEN - Centinela State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["2302 Brown Rd", "Imperial, CA 92251"]},
    "SQ":   {"name": "SQ - San Quentin State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["Main Street", "San Quentin, CA 94964"]},
    "SQSP": {"name": "SQ - San Quentin State Prison", "parent": "CDCR", "parent_full": "Dept. of Corrections and Rehabilitation", "address": ["Main Street", "San Quentin, CA 94964"]},
    # CalVet facilities
    "CALVETHOME-YV": {"name": "Veterans Home of California - Yountville", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["190 California Dr", "Yountville, CA 94599"]},
    "CALVETHOME-BF": {"name": "Veterans Home of California - Barstow", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["100 E Veterans Pkwy", "Barstow, CA 92311"]},
    "CALVETHOME-CV": {"name": "Veterans Home of California - Chula Vista", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["700 E Naples Ct", "Chula Vista, CA 91911"]},
    "CALVETHOME-LA": {"name": "Veterans Home of California - West Los Angeles", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["11500 Nimitz Ave Bldg 209", "Los Angeles, CA 90049"]},
    "CALVETHOME-FR": {"name": "Veterans Home of California - Fresno", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["2811 W California Ave", "Fresno, CA 93706"]},
    "CALVETHOME-RD": {"name": "Veterans Home of California - Redding", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["3400 Knighton Rd", "Redding, CA 96002"]},
    "CALVETHOME-MV": {"name": "Veterans Home of California - Moosehaven", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["11 Moosehaven Blvd", "Moosehaven, CA 95380"]},
    "CALVETHOME-VM": {"name": "Veterans Home of California - Ventura", "parent": "CalVet", "parent_full": "California Department of Veterans Affairs", "address": ["10900 Telephone Rd", "Ventura, CA 93004"]},
}

# ── Build reverse zip→facility lookup ─────────────────────────────────────────
# Each zip maps to a list of facility keys (most are unique, some CDCR share zips)
ZIP_TO_FACILITY = {}
for _fk, _fv in FACILITY_DB.items():
    _addr_str = " ".join(_fv.get("address", []))
    _zip_matches = re.findall(r'\b(\d{5})\b', _addr_str)
    if _zip_matches:
        _zip = _zip_matches[-1]  # Last 5-digit number is the zip
        ZIP_TO_FACILITY.setdefault(_zip, []).append(_fk)


def _lookup_facility_by_zip(text: str) -> tuple:
    """Scan text for zip codes and match to facilities.
    Returns (facility_dict, ambiguous_list) where ambiguous_list has >1 if zip is shared."""
    if not text:
        return None, []
    found_zips = re.findall(r'\b(\d{5})\b', text)
    for z in found_zips:
        if z in ZIP_TO_FACILITY:
            keys = ZIP_TO_FACILITY[z]
            fac = FACILITY_DB.get(keys[0])
            if fac:
                return fac, keys
    return None, []


def _lookup_facility(text: str) -> dict | None:
    """Look up a CDCR/CalVet facility from free text (delivery location, ship_to, institution).
    Returns FACILITY_DB entry or None."""
    if not text:
        return None
    upper = text.upper().strip()
    # Direct abbreviation match (e.g. "CIW", "CSP-SAC")
    for key in FACILITY_DB:
        if upper.startswith(key + " ") or upper.startswith(key + "-") or upper.startswith(key + ",") or upper == key:
            return FACILITY_DB[key]
    # Name fragment match (e.g. "California Institution for Women")
    for key, fac in FACILITY_DB.items():
        fname = fac["name"].upper()
        # Check if the facility's descriptive name appears in the text
        # e.g. "California Institution for Women" in "CIW - California Institution for Women, 16756..."
        desc_part = fname.split(" - ", 1)[1] if " - " in fname else fname
        if desc_part and len(desc_part) > 5 and desc_part in upper:
            return fac
    # City-based fallback for known prison cities
    _CITY_MAP = {
        "CHINO": "CIM", "CORONA": "CIW", "CORCORAN": "CSP-COR", "LANCASTER": "CSP-LAC",
        "VACAVILLE": "CSP-SOL", "STOCKTON": "CHCF", "COALINGA": "PVSP", "DELANO": "KVSP",
        "IONE": "MCSP", "WASCO": "WSP", "CHOWCHILLA": "CCWF", "SOLEDAD": "CTF",
        "CRESCENT CITY": "PBSP", "NORCO": "CRC", "TEHACHAPI": "CCI", "AVENAL": "ASP",
        "SUSANVILLE": "HDSP", "BLYTHE": "ISP", "REPRESA": "FSP", "SAN QUENTIN": "SQ",
        "CALIPATRIA": "CAL", "IMPERIAL": "CEN", "JAMESTOWN": "SCC",
        "SAN LUIS OBISPO": "CMC", "YOUNTVILLE": "CALVETHOME-YV", "BARSTOW": "CALVETHOME-BF",
        "REDDING": "CALVETHOME-RD", "WEST LOS ANGELES": "CALVETHOME-LA",
        "FRESNO": "CALVETHOME-FR", "CHULA VISTA": "CALVETHOME-CV",
    }
    for city, fac_key in _CITY_MAP.items():
        if city in upper:
            return FACILITY_DB.get(fac_key)
    return None


def _parse_address_parts(raw: str) -> tuple:
    """Parse a raw address string into (name, [address_lines]).
    Splits on newlines first, then commas. First part is name, rest is address.
    If it looks like the first part IS an address (has a number), treats the whole thing as address."""
    if not raw:
        return "", []
    lines = [l.strip() for l in raw.replace("\\r\\n", "\\n").replace("\r\n", "\n").split("\n") if l.strip()]
    if len(lines) > 1:
        # Multi-line: first line is name, rest is address
        # But check if first line looks like a street address (starts with number)
        if lines[0] and lines[0][0].isdigit():
            return "", lines  # All address, no name
        return lines[0], lines[1:]
    # Single line: split on commas
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) > 1:
        # First part is name, rest is address
        if parts[0] and parts[0][0].isdigit():
            return "", parts
        return parts[0], parts[1:]
    return raw.strip(), []

def _load_counter():
    """Load counter from SQLite — the single source of truth."""
    try:
        from src.core.db import get_setting
        year_val = get_setting("quote_counter_year", datetime.now().year)
        seq_val = get_setting("quote_counter_seq", get_setting("quote_counter", 16))
        return {"year": int(year_val), "seq": int(seq_val)}
    except Exception:
        return {}

def _save_counter(data):
    """Save counter to SQLite (primary) and JSON (backup)."""
    try:
        from src.core.db import set_setting
        set_setting("quote_counter_year", data.get("year", datetime.now().year))
        set_setting("quote_counter_seq", data.get("seq", 16))
        set_setting("quote_counter", data.get("seq", 16))  # legacy key compat
    except Exception as _e:
        log.warning("Counter SQLite save failed: %s", _e)
    # Also write JSON as belt-and-suspenders backup
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "quote_counter.json")
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

def set_quote_counter(seq: int, year: int = None):
    """Manually set the quote counter (e.g., to sync with QuoteWerks)."""
    if year is None:
        year = datetime.now().year
    _save_counter({"year": year, "seq": seq})
    # Also update the guardrail so auto-increment knows this is the trusted value
    try:
        from src.core.db import set_setting
        set_setting("quote_counter_last_good", str(seq))
    except Exception:
        pass
    log.info("Quote counter set to seq=%d year=%d → next will be R%sQ%d",
             seq, year, str(year)[-2:], seq + 1)

def _should_reset_counter(stored_year: int) -> bool:
    """Reset at 12:00:01 AM on Jan 1 of a new year only."""
    now = datetime.now()
    return stored_year != now.year

def _next_quote_number() -> str:
    """R{YY}Q{seq} — sequential, collision-safe.

    Uses a single DB connection with a short exclusive lock.
    No nested connections — that was causing 2+ minute DB lock cascades.
    """
    import sqlite3
    from src.core.paths import DATA_DIR as _DD
    import os as _os

    _db_path = _os.path.join(_DD, "reytech.db") if _os.path.exists(_os.path.join(_DD, "reytech.db")) else _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))), "data", "reytech.db")

    year = datetime.now().year
    yy = str(year)[-2:]
    prefix = f"R{yy}Q"

    conn = sqlite3.connect(_db_path, timeout=5)  # Short timeout — fail fast, don't block
    try:
        conn.execute("BEGIN IMMEDIATE")

        # Read counter directly — no nested get_setting() calls
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='quote_counter_seq'"
        ).fetchone()
        stored_seq = int(row[0]) if row else 0

        # Year check
        yr_row = conn.execute(
            "SELECT value FROM app_settings WHERE key='quote_counter_year'"
        ).fetchone()
        stored_year = int(yr_row[0]) if yr_row else year
        if stored_year != year:
            log.info("New year — resetting quote counter from %d to 1", stored_seq)
            stored_seq = 0

        next_seq = stored_seq + 1

        # +5 jump guardrail: prevent counter from jumping wildly
        lg_row = conn.execute(
            "SELECT value FROM app_settings WHERE key='quote_counter_last_good'"
        ).fetchone()
        last_good = int(lg_row[0]) if lg_row else next_seq
        if next_seq - last_good > 5:
            log.warning("Quote counter jump blocked: seq=%d but last_good=%d — capping at %d",
                        next_seq, last_good, last_good + 1)
            next_seq = last_good + 1

        # Write counter directly — no nested set_setting() calls
        now = datetime.now().isoformat()
        for key, val in [("quote_counter_seq", next_seq), ("quote_counter", next_seq),
                         ("quote_counter_year", year), ("quote_counter_last_good", next_seq)]:
            conn.execute("""
                INSERT INTO app_settings (key, value, updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """, (key, str(val), now))

        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error("_next_quote_number error: %s", e)
        # Fallback: use timestamp
        return f"R{yy}Q{datetime.now().strftime('%H%M%S')}"
    finally:
        conn.close()

    new_number = f"{prefix}{next_seq}"
    log.info("Quote number: %s (seq=%d)", new_number, next_seq)
    return new_number

def _rollback_quote_number(quote_number: str):
    """Roll back the counter after a failed generate so the number isn't wasted."""
    try:
        prefix_len = 4  # "R26Q"
        seq = int(quote_number[prefix_len:])
        year = int("20" + quote_number[1:3])
        # Only roll back if the counter is still at this seq (no one else incremented)
        data = _load_counter()
        if data.get("seq") == seq:
            data["seq"] = seq - 1
            _save_counter(data)
            log.info("Quote number %s rolled back — counter now at seq=%d", quote_number, seq - 1)
        else:
            log.warning("Quote number %s rollback skipped — counter already at %d", quote_number, data.get("seq"))
    except Exception as e:
        log.warning("Quote number rollback failed: %s", e)


def peek_next_quote_number() -> str:
    """Preview what the next number would be without consuming it."""
    data = _load_counter()
    year = datetime.now().year
    yy = str(year)[-2:]
    if _should_reset_counter(data.get("year", 0)):
        return f"R{yy}Q1"
    return f"R{yy}Q{data.get('seq', 0) + 1}"

# ═══════════════════════════════════════════════════════════════════════════════
# QUOTES DATABASE — searchable log with Win/Loss tracking
# ═══════════════════════════════════════════════════════════════════════════════

VALID_STATUSES = ("pending", "won", "lost", "draft", "sent", "expired")

def get_all_quotes(include_test: bool = False) -> list:
    """Return all quotes. By default excludes test/QA quotes.
    Reads from quotes_log.json first, falls back to SQLite if JSON is empty/stale."""
    path = os.path.join(DATA_DIR, "quotes_log.json")
    try:
        with open(path) as f:
            quotes = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        quotes = []

    # ── Fallback: if JSON is empty or all totals are 0, use SQLite ──
    non_empty = [q for q in quotes if q.get("total", 0) > 0]
    if len(quotes) == 0 or (len(quotes) > 2 and len(non_empty) == 0):
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute("""
                    SELECT quote_number, status, total, agency, institution,
                           po_number, contact_name, contact_email, subtotal, tax,
                           created_at, updated_at, is_test, source, sent_at,
                           line_items, ship_to_name, ship_to_address
                    FROM quotes ORDER BY created_at DESC
                """).fetchall()
                if rows:
                    quotes = []
                    for r in rows:
                        items = []
                        try:
                            items = json.loads(r["line_items"] or "[]")
                        except Exception:
                            pass
                        ship_addr = []
                        try:
                            ship_addr = json.loads(r["ship_to_address"] or "[]")
                        except Exception:
                            if r["ship_to_address"]:
                                ship_addr = [r["ship_to_address"]]
                        quotes.append({
                            "quote_number": r["quote_number"],
                            "status": r["status"] or "pending",
                            "total": r["total"] or 0,
                            "subtotal": r["subtotal"] or 0,
                            "tax": r["tax"] or 0,
                            "agency": r["agency"] or "",
                            "institution": r["institution"] or "",
                            "po_number": r["po_number"] or "",
                            "contact_name": r["contact_name"] or "",
                            "contact_email": r["contact_email"] or "",
                            "created_at": r["created_at"] or "",
                            "updated_at": r["updated_at"] or "",
                            "is_test": bool(r["is_test"]),
                            "source": r["source"] or "",
                            "sent_at": r["sent_at"] or "",
                            "items_detail": items,
                            "ship_to_name": r["ship_to_name"] or "",
                            "ship_to_address": ship_addr,
                        })
                    # Sync back to JSON for other consumers
                    _save_all_quotes(quotes)
        except Exception:
            pass
    # ── End SQLite fallback ──

    if include_test:
        return quotes
    # Filter out test quotes — TEST-/QA- prefixed numbers or is_test flag
    return [q for q in quotes if not (
        q.get("is_test") or
        str(q.get("quote_number", "")).startswith("TEST-") or
        str(q.get("quote_number", "")).startswith("QA-") or
        str(q.get("source_pc_id", "")).startswith("test_")
    )]

def _save_all_quotes(quotes: list):
    path = os.path.join(DATA_DIR, "quotes_log.json")
    os.makedirs(DATA_DIR, exist_ok=True)
    if len(quotes) > 2000:
        quotes = quotes[-2000:]
    with open(path, "w") as f:
        json.dump(quotes, f, indent=2, default=str)

def search_quotes(query: str = "", agency: str = "", status: str = "",
                  limit: int = 50) -> list:
    """Search quotes — full-text across all fields including items, part numbers, ship_to."""
    quotes = get_all_quotes()
    q = query.lower()
    results = []
    now = datetime.now()
    for qt in reversed(quotes):
        # Auto-expire: if pending and older than 45 days, mark expired
        if qt.get("status", "pending") == "pending":
            try:
                created = qt.get("created_at") or qt.get("date", "")
                if created:
                    if "T" in str(created):
                        created_dt = datetime.fromisoformat(str(created).replace("Z", "+00:00")).replace(tzinfo=None)
                    else:
                        created_dt = datetime.strptime(str(created), "%b %d, %Y")
                    if (now - created_dt).days > 45:
                        qt["status"] = "expired"
            except Exception:
                pass

        if agency and qt.get("agency", "").lower() != agency.lower():
            continue
        if status and qt.get("status", "pending").lower() != status.lower():
            continue
        if q:
            # Build searchable text from ALL fields — requestor, contact, notes included
            parts = [
                qt.get("quote_number", ""),
                qt.get("institution", ""),
                qt.get("rfq_number", ""),
                qt.get("agency", ""),
                qt.get("po_number", ""),
                qt.get("status_notes", ""),
                qt.get("items_text", ""),
                qt.get("ship_to_name", ""),
                " ".join(qt.get("ship_to_address", [])) if isinstance(qt.get("ship_to_address"), list) else str(qt.get("ship_to_address", "")),
                qt.get("requestor", ""),           # ← was missing
                qt.get("contact_name", ""),         # ← was missing
                qt.get("requestor_name", ""),       # ← was missing
                qt.get("email", ""),                # ← was missing
                qt.get("requestor_email", ""),      # ← was missing
                qt.get("phone", ""),                # ← was missing
                qt.get("notes", ""),                # ← was missing
                qt.get("source", ""),               # ← was missing
                str(qt.get("total", "")),
            ]
            # Add item descriptions and part numbers from items_detail
            for item in qt.get("items_detail", []):
                parts.append(str(item.get("description", "")))
                parts.append(str(item.get("part_number", "")))
            searchable = " ".join(parts).lower()
            if q not in searchable:
                continue
        results.append(qt)
        if len(results) >= limit:
            break
    return results

def update_quote_status(quote_number: str, status: str, po_number: str = "",
                         notes: str = "", actor: str = "user") -> bool:
    """Mark a quote as won, lost, or pending. Records status_history. Returns True if found.
    Business rule: 'won' requires a PO number (from formal PO email)."""
    if status not in VALID_STATUSES:
        return False
    # Enforce: won requires PO (only formal PO email = won)
    if status == "won" and not (po_number or "").strip() and actor == "user":
        log.warning("Blocked won status for %s — no PO number (actor=%s)", quote_number, actor)
        return False
    quotes = get_all_quotes()
    found = False
    now = datetime.now().isoformat()
    for qt in quotes:
        if qt.get("quote_number") == quote_number:
            qt["status"] = status
            qt["status_updated"] = now
            qt["updated_at"] = now
            if po_number:
                qt["po_number"] = po_number
            if notes:
                qt["status_notes"] = notes
            # Auto-set sent_at when marking as sent
            if status == "sent" and not qt.get("sent_at"):
                qt["sent_at"] = now
            # Append to status_history (create if missing for legacy records)
            history = qt.get("status_history", [])
            entry = {"status": status, "timestamp": now, "actor": actor}
            if po_number:
                entry["po_number"] = po_number
            if notes:
                entry["notes"] = notes
            history.append(entry)
            qt["status_history"] = history
            found = True
            break
    if found:
        _save_all_quotes(quotes)
        # Sync to DB
        try:
            from src.core.db import get_db
            with get_db() as conn:
                updates = {"status": status, "updated_at": now}
                if po_number:
                    updates["po_number"] = po_number
                if notes:
                    updates["status_notes"] = notes
                if status == "sent":
                    updates["sent_at"] = now
                set_clause = ", ".join(f"{k}=?" for k in updates.keys())
                conn.execute(
                    f"UPDATE quotes SET {set_clause} WHERE quote_number=?",
                    (*updates.values(), quote_number)
                )
        except Exception as _e:
            log.debug("DB sync for quote status: %s", _e)
        log.info("Quote %s marked as %s%s", quote_number, status.upper(),
                 f" (PO: {po_number})" if po_number else "")
    return found

def get_quote_stats() -> dict:
    """Win/loss statistics for the quotes database."""
    quotes = get_all_quotes()
    stats = {"total": len(quotes), "won": 0, "lost": 0, "pending": 0,
             "won_total": 0.0, "lost_total": 0.0, "win_rate": 0.0}
    decided = 0
    for qt in quotes:
        s = qt.get("status", "pending")
        stats[s] = stats.get(s, 0) + 1
        if s == "won":
            stats["won_total"] += qt.get("total", 0)
            decided += 1
        elif s == "lost":
            stats["lost_total"] += qt.get("total", 0)
            decided += 1
    if decided > 0:
        stats["win_rate"] = round(stats["won"] / decided * 100, 1)
    return stats

def _log_quote(result: dict):
    quotes = get_all_quotes()
    now = datetime.now().isoformat()
    qn = result.get("quote_number")
    
    # Check if this quote number already exists (regeneration)
    existing_idx = None
    if qn:
        for i, q in enumerate(quotes):
            if q.get("quote_number") == qn:
                existing_idx = i
                break
    
    # Determine if this is a test quote — never let test data touch real records
    is_test = bool(
        result.get("is_test") or
        (qn and (str(qn).startswith("TEST-") or str(qn).startswith("QA-"))) or
        result.get("source_pc_id", "").startswith("test_")
    )

    entry = {
        "quote_number":  qn,
        "date":          result.get("date"),
        "agency":        result.get("agency"),
        "institution":   result.get("institution", ""),
        "rfq_number":    result.get("rfq_number", ""),
        "total":         result.get("total", 0),
        "subtotal":      result.get("subtotal", 0),
        "tax":           result.get("tax", 0),
        "items_count":   result.get("items_count", 0),
        "items_text":    result.get("items_text", ""),
        "items_detail":  result.get("items_detail", []),
        "pdf_path":      result.get("path", ""),
        "source_pc_id":  result.get("source_pc_id", ""),
        "source_rfq_id": result.get("source_rfq_id", ""),
        "ship_to_name":  result.get("ship_to_name", ""),
        "ship_to_address": result.get("ship_to_address", []),
        "requestor":     result.get("requestor") or result.get("contact_name", ""),
        "contact_name":  result.get("contact_name") or result.get("requestor", ""),
        "email":         result.get("email") or result.get("requestor_email", ""),
        "phone":         result.get("phone") or result.get("contact_phone", ""),
        "source":        result.get("source", ""),
        "is_test":       is_test,
    }

    # Contract enforcement (Law 28) — block empty shells
    try:
        from src.core.contracts import validate_quote, log_blocked_save
        is_valid, violations = validate_quote(entry, strict=True)
        if not is_valid:
            log_blocked_save("quote", qn, violations, "_log_quote")
            log.warning("Quote %s NOT saved — contract: %s", qn, violations)
            return
    except ImportError:
        pass

    # TEST GUARD: test quotes never write to SQLite or appear in real data
    if is_test:
        log.info("Test quote %s logged with is_test=True — excluded from real records", qn)
        existing = [q for q in get_all_quotes() if q.get("quote_number") != qn]
        entry["status"] = "pending"
        entry["created_at"] = datetime.now().isoformat()
        entry["status_history"] = []
        existing.append(entry)
        _save_all_quotes(existing)
        return  # Do NOT write test quotes to SQLite
    
    if existing_idx is not None:
        # UPDATE existing — preserve status, history, and created_at
        old = quotes[existing_idx]
        entry["status"] = old.get("status", "pending")
        entry["created_at"] = old.get("created_at", now)
        entry["status_history"] = old.get("status_history", [])
        entry["po_number"] = old.get("po_number", "")
        entry["regenerated_at"] = now
        entry["regeneration_count"] = old.get("regeneration_count", 0) + 1
        quotes[existing_idx] = entry
        log.info("Quote %s regenerated (update #%d)", qn, entry["regeneration_count"])
    else:
        # NEW quote
        entry["status"] = "pending"
        entry["created_at"] = now
        entry["status_history"] = [
            {"status": "pending", "timestamp": now, "actor": "system"}
        ]
        quotes.append(entry)
    
    _save_all_quotes(quotes)

    # ── Also persist to SQLite (survives Railway redeploys) ──
    try:
        from src.core.db import upsert_quote, record_price
        upsert_quote(entry)
        # Record every line item price into price_history
        for item in entry.get("items_detail", []):
            price = item.get("unit_price") or item.get("price_each") or item.get("our_price")
            desc = item.get("description", "")
            if price and price > 0 and desc:
                record_price(
                    description=desc,
                    unit_price=float(price),
                    source="quote",
                    part_number=item.get("part_number", "") or item.get("item_number", ""),
                    manufacturer=item.get("manufacturer", ""),
                    quantity=float(item.get("qty", 1) or 1),
                    agency=result.get("agency", ""),
                    quote_number=qn or "",
                )
    except Exception as _db_err:
        log.debug("DB write skipped: %s", _db_err)

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_agency(data: dict) -> str:
    """Detect state agency from ALL available data — institution, ship_to, email, addresses."""
    # Cast widest possible net across all fields
    text = " ".join(str(v) for v in [
        data.get("institution", ""), data.get("department", ""),
        data.get("bill_to", ""), data.get("bill_to_name", ""),
        data.get("ship_to", ""), data.get("ship_to_name", ""),
        " ".join(data.get("ship_to_address", [])) if isinstance(data.get("ship_to_address"), list) else data.get("ship_to_address", ""),
        " ".join(data.get("to_address", [])) if isinstance(data.get("to_address"), list) else data.get("to_address", ""),
        data.get("requestor", ""), data.get("requestor_name", ""),
        data.get("requestor_email", ""), data.get("email", ""),
        data.get("delivery_location", ""),
    ]).upper()

    # ── Email domain matching (most reliable) ──
    email_text = " ".join(str(v) for v in [
        data.get("requestor_email", ""), data.get("email", ""),
    ]).upper()
    if "CDCR.CA.GOV" in email_text:   return "CDCR"
    if "CCHCS.CA.GOV" in email_text:  return "CCHCS"
    if "CALVET.CA.GOV" in email_text: return "CalVet"
    if "DGS.CA.GOV" in email_text:    return "DGS"
    if "DSH.CA.GOV" in email_text:    return "DSH"

    # ── Direct agency name matches ──
    if "CCHCS" in text or "HEALTH CARE" in text or "CALIFORNIA HEALTH" in text: return "CCHCS"
    if "CALVET" in text or "VETERAN" in text or "VETERANS HOME" in text:        return "CalVet"
    if "DGS" in text or "GENERAL SERVICE" in text:                               return "DGS"
    if "DSH" in text or "STATE HOSPITAL" in text or "DEPT OF STATE HOSP" in text: return "DSH"
    if "CDCR" in text or "CORRECTION" in text or "DEPT OF CORRECTIONS" in text:  return "CDCR"

    # ── CDCR prison abbreviations ──
    _CDCR_PREFIXES = (
        "CSP", "CIM", "CIW", "SCC", "CMC", "SATF", "CHCF", "PVSP", "KVSP",
        "LAC", "MCSP", "NKSP", "SAC", "WSP", "SOL", "FSP", "HDSP", "ISP",
        "CTF", "DVI", "RJD", "CAL", "CEN", "ASP", "CCWF", "VSP", "SVSP",
        "PBSP", "CRC", "CCI", "SQ", "SQSP",
    )
    for prefix in _CDCR_PREFIXES:
        if text.startswith(prefix + "-") or text.startswith(prefix + " ") or text == prefix:
            return "CDCR"
        if f" {prefix}-" in text or f" {prefix} " in text or f"- {prefix}" in text:
            return "CDCR"

    # ── CDCR location keywords (prison names + known cities) ──
    _CDCR_KEYWORDS = (
        "STATE PRISON", "CONSERVATION CENTER", "INSTITUTION FOR",
        "FOLSOM", "PELICAN BAY", "SAN QUENTIN", "CORCORAN",
        "IRONWOOD", "CHUCKAWALLA", "WASCO", "SOLEDAD", "TEHACHAPI",
        "AVENAL", "BLYTHE", "SUSANVILLE", "CRESCENT CITY",
        "REPRESA", "DELANO", "COALINGA", "VACAVILLE", "CHINO",
        "LANCASTER", "NORCO", "SOLANO", "MULE CREEK",
        "NORTH KERN", "KERN VALLEY", "VALLEY STATE", "CENTINELA",
        "RICHARD J DONOVAN", "PLEASANT VALLEY", "HIGH DESERT",
        "CALIFORNIA MEN", "CALIFORNIA WOMEN",
    )
    for kw in _CDCR_KEYWORDS:
        if kw in text:
            return "CDCR"

    # ── DSH hospital locations ──
    _DSH_KEYWORDS = ("ATASCADERO", "COALINGA STATE HOSP", "METROPOLITAN STATE",
                     "NAPA STATE", "PATTON STATE")
    for kw in _DSH_KEYWORDS:
        if kw in text:
            return "DSH"

    # ── CalVet home locations ──
    _CALVET_KEYWORDS = ("VETERANS HOME", "VET HOME", "YOUNTVILLE",
                        "BARSTOW VET", "CHULA VISTA VET", "FRESNO VET",
                        "LANCASTER VET", "REDDING VET", "WEST LOS ANGELES VET")
    for kw in _CALVET_KEYWORDS:
        if kw in text:
            return "CalVet"

    return "DEFAULT"

def _find_logo() -> Optional[str]:
    """Find logo: reytech_logo.{png,jpg,jpeg,gif} in project root, data dir, or forms dir."""
    from src.core.paths import PROJECT_ROOT
    for d in [PROJECT_ROOT, DATA_DIR, os.path.dirname(__file__), "/app/data", "/app", "/data"]:
        for ext in ("png", "jpg", "jpeg", "gif"):
            for name in ("reytech_logo", "logo"):
                p = os.path.join(d, f"{name}.{ext}")
                if os.path.exists(p):
                    return p
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PDF GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_quote(
    quote_data: dict,
    output_path: str,
    agency: str = None,
    quote_number: str = None,
    tax_rate: float = None,
    include_tax: bool = True,
    shipping: float = 0.0,
    terms: str = None,
    expiry_days: int = 45,
    notes: str = None,
    revision: int = None,
) -> dict:
    """
    Generate a professional Reytech quote PDF.

    quote_data keys:
        institution, ship_to_name, ship_to_address[], rfq_number,
        bill_to_name?, bill_to_address[]?,
        line_items: [{line_number, part_number, qty, uom, description, unit_price}]
    """
    # ── Setup ──────────────────────────────────────────────────────────────────
    if not agency:
        agency = _detect_agency(quote_data)
    cfg = AGENCY_CONFIGS.get(agency, AGENCY_CONFIGS["DEFAULT"])

    _allocated_number = False
    if not quote_number:
        quote_number = _next_quote_number()
        _allocated_number = True  # track so we can roll back on failure

    log.info("Generating quote %s for %s (agency=%s, %d items)",
             quote_number, quote_data.get("institution", "?")[:40],
             agency, len(quote_data.get("line_items", [])))

    today = datetime.now()
    quote_date  = today.strftime("%b %d, %Y")
    expiry_date = (today + timedelta(days=expiry_days)).strftime("%b %d, %Y")

    rate      = tax_rate if tax_rate is not None else cfg["default_tax"]
    pay_terms = terms or cfg["default_terms"]

    to_name   = quote_data.get("institution", "")
    ship_name = quote_data.get("ship_to_name", to_name)
    ship_addr = quote_data.get("ship_to_address", [])
    if isinstance(ship_addr, str): ship_addr = [ship_addr]
    to_addr   = quote_data.get("to_address", ship_addr)
    if isinstance(to_addr, str): to_addr = [to_addr]

    show_bill    = cfg["show_bill_to"]
    bill_name    = quote_data.get("bill_to_name", cfg.get("bill_to_name", ""))
    bill_lines   = quote_data.get("bill_to_address", cfg.get("bill_to_lines", []))
    if not bill_name: show_bill = False

    rfq_num = quote_data.get("rfq_number", quote_data.get("solicitation_number", ""))
    items   = quote_data.get("line_items", [])

    # ── Page constants ─────────────────────────────────────────────────────────
    # Matches QuoteWerks: page=612x792, margins L=18 R=594
    W, H  = letter
    ML    = 18       # left margin
    MR    = 594      # right edge
    UW    = MR - ML  # 576 usable
    TXT_X = 53       # company info text indent (from extraction)

    try:
        c = canvas.Canvas(output_path, pagesize=letter)
    except Exception as _ce:
        if _allocated_number:
            _rollback_quote_number(quote_number)
        raise
    c.setTitle(f"Reytech Quote {quote_number}")
    c.setAuthor("Reytech Inc.")

    # pdfplumber y = from top; reportlab y = from bottom
    def Y(top_y):
        return H - top_y

    # ── Helper: bordered box ──────────────────────────────────────────────────
    def box(x, yt, w, h, fill=False, border_color=TBL_BD):
        rl_y = Y(yt) - h
        if fill:
            c.setFillColor(FILL)
            c.rect(x, rl_y, w, h, fill=1, stroke=0)
        c.setStrokeColor(border_color)
        c.setLineWidth(0.5)
        c.rect(x, rl_y, w, h, fill=0, stroke=1)
        return rl_y  # bottom of box in rl coords

    def _fmt_qty(q):
        """Format qty: 20.0 → '20', 1.5 → '1.5'"""
        return str(int(q)) if float(q) == int(float(q)) else str(q)

    def _sanitize(s):
        """Replace unicode chars that Helvetica renders as squares."""
        if not s:
            return ""
        s = str(s)
        # Smart quotes → straight quotes
        s = s.replace("\u2018", "'").replace("\u2019", "'")   # '' → '
        s = s.replace("\u201c", '"').replace("\u201d", '"')   # "" → "
        # Dashes
        s = s.replace("\u2013", "-").replace("\u2014", "-")   # – — → -
        # Other common unicode
        s = s.replace("\u2026", "...").replace("\u00a0", " ") # … and nbsp
        s = s.replace("\u2022", "-")                          # bullet
        s = s.replace("\u00b7", "-")                          # middle dot
        return s

    def text(x, yt, txt, font="Helvetica", size=9, color=BLACK, align="left"):
        c.setFont(font, size)
        c.setFillColor(color)
        s = _sanitize(txt)
        rl_y = Y(yt)
        if align == "right":
            c.drawRightString(x, rl_y, s)
        elif align == "center":
            c.drawCentredString(x, rl_y, s)
        else:
            c.drawString(x, rl_y, s)

    # ══════════════════════════════════════════════════════════════════════════
    # PAGE 1 HEADER  (all y values are "from top of page")
    # ══════════════════════════════════════════════════════════════════════════

    TOP = 50   # start content near top (saves ~100pt vs original QuoteWerks layout)

    # -- "QUOTE" title -- right-aligned
    text(MR, TOP, "QUOTE", "Helvetica-Bold", 22, BLACK, "right")

    # -- Horizontal rule
    c.setStrokeColor(LBL_BD)
    c.setLineWidth(1.5)
    c.line(ML, Y(TOP + 2), MR, Y(TOP + 2))

    # -- QUOTE # / DATE boxes -- right column (tight to header)
    qbox_y = TOP + 6
    box(396, qbox_y, 67, 20, fill=True, border_color=LBL_BD)
    text(400, qbox_y + 15, "QUOTE #", "Helvetica-Bold", 10)
    box(463, qbox_y, 131, 20, fill=False, border_color=VAL_BD)
    _qn_display = f"{quote_number} Rev {revision}" if revision else quote_number
    text(MR - 6, qbox_y + 15, _qn_display, "Helvetica-Bold", 11 if revision else 12, BLACK, "right")

    box(396, qbox_y + 21, 67, 20, fill=True, border_color=LBL_BD)
    text(400, qbox_y + 36, "DATE", "Helvetica-Bold", 10)
    box(463, qbox_y + 21, 131, 20, fill=False, border_color=VAL_BD)
    text(MR - 6, qbox_y + 36, quote_date, "Helvetica-Bold", 10, BLACK, "right")

    qbox_bottom = qbox_y + 42   # bottom of DATE box, used for Bill To spacing

    # -- Reytech logo + company info (left column)
    logo_path = _find_logo()
    logo_y = TOP + 5
    logo_text_x = ML + 34
    if logo_path:
        try:
            img = ImageReader(logo_path)
            iw, ih = img.getSize()
            max_w, max_h = 130, 30
            scale = min(max_w / iw, max_h / ih)
            dw, dh = iw * scale, ih * scale
            c.drawImage(logo_path, ML + 8, Y(logo_y) - dh, width=dw, height=dh,
                        preserveAspectRatio=True, mask='auto')
            logo_text_x = ML + 8 + dw + 6
        except Exception as e:
            log.warning(f"Logo load failed: {e}")
            logo_path = None

    if not logo_path:
        p = c.beginPath()
        bx, by = ML + 8, Y(logo_y + 12)
        p.moveTo(bx, by); p.lineTo(bx + 10, by + 14)
        p.lineTo(bx + 20, by); p.lineTo(bx + 10, by - 3); p.close()
        c.setFillColor(NAVY)
        c.drawPath(p, fill=1, stroke=0)
        logo_text_x = ML + 34
        c.setFont("Helvetica-Bold", 13)
        c.setFillColor(NAVY)
        c.drawString(logo_text_x, Y(logo_y + 14), "Reytech Inc.")
        c.setFont("Helvetica-Oblique", 7.5)
        c.setFillColor(HexColor("#4f8cff"))
        c.drawString(logo_text_x, Y(logo_y + 25), "CA Certified Small Business (SB) & DVBE")

    # Company details -- compact 10pt line spacing
    c.setFillColor(BLACK)
    c.setFont("Helvetica", 9)
    info_y = logo_y + 36
    info_lines = [
        REYTECH["line1"],
        REYTECH["line2"],
        f"{REYTECH['contact']}, {REYTECH['title']}",
        REYTECH["phone"],
        REYTECH["email"],
        "www.reytechinc.com",
    ]
    if cfg["show_permit"]:
        info_lines.append(f"CA Sellers Permit: {REYTECH['permit']}")
    for itxt in info_lines:
        text(TXT_X, info_y, itxt, "Helvetica", 9)
        info_y += 10

    # -- Bill To (right column, only for CDCR/CalVet)
    BILL_X = 396
    ADDR_LBL_X = ML + 10   # label "To:" indented slightly from margin
    ADDR_VAL_X = ML + 10   # address content same x, line below label
    bill_bottom_y = info_y
    if show_bill:
        bill_y = qbox_bottom + 12   # comfortable gap below DATE box
        text(BILL_X, bill_y, "Bill to:", "Helvetica-Bold", 10)
        by = bill_y + 15            # 15pt gap: label to first content line
        text(BILL_X, by, bill_name, "Helvetica", 9)
        by += 12
        for bl in bill_lines:
            text(BILL_X, by, bl, "Helvetica", 9)
            by += 11
        bill_bottom_y = max(bill_bottom_y, by)

    # -- To: / Ship to Location:
    addr_y = max(info_y, bill_bottom_y) + 6

    # Left column: "To:" label, then name + address below
    text(ADDR_LBL_X, addr_y, "To:", "Helvetica-Bold", 10)
    ay = addr_y + 15                 # 15pt gap below label
    text(ADDR_VAL_X, ay, to_name, "Helvetica", 10)
    ay += 12
    for line in to_addr:
        text(ADDR_VAL_X, ay, line, "Helvetica", 10)
        ay += 11
    if to_addr and "united states" not in " ".join(to_addr).lower():
        text(ADDR_VAL_X, ay, "United States", "Helvetica", 10)
        ay += 11

    # Right column: "Ship to Location:" label, then facility + address below
    text(BILL_X, addr_y, "Ship to Location:", "Helvetica-Bold", 10)
    sy = addr_y + 15                 # 15pt gap below label
    text(BILL_X, sy, ship_name, "Helvetica", 10)
    sy += 12
    for line in ship_addr:
        text(BILL_X, sy, line, "Helvetica", 10)
        sy += 11
    if ship_addr and "united states" not in " ".join(ship_addr).lower():
        text(BILL_X, sy, "United States", "Helvetica", 10)
        sy += 11


    # ── Salesperson / RFQ / Terms / Expiry bar ────────────────────────────────
    # 4-column bar, each column is a filled+bordered cell with header+value
    bar_y = max(ay, sy) + 2  # tight gap after addresses
    bar_h = 28
    col_positions = [
        (ML,      143),  # Salesperson
        (ML+144,  143),  # RFQ Number
        (ML+288,  143),  # Terms
        (ML+431,  145),  # Expiration Date
    ]
    headers_vals = [
        ("Salesperson",    "Mike Guadan"),
        ("RFQ Number",     str(rfq_num)),
        ("Terms",          pay_terms),
        ("Expiration Date", expiry_date),
    ]

    for (cx, cw), (hdr, val) in zip(col_positions, headers_vals):
        # Filled background
        box(cx, bar_y, cw, bar_h, fill=True, border_color=TBL_BD)
        # Header text (top of cell)
        text(cx + 6, bar_y + 12, hdr, "Helvetica-Bold", 10)
        # Value text (bottom of cell)
        text(cx + 6, bar_y + 25, val, "Helvetica", 10)

    # ══════════════════════════════════════════════════════════════════════════
    # LINE ITEMS TABLE
    # ══════════════════════════════════════════════════════════════════════════

    # Column definitions: (x_offset_from_ML, width)
    # Extracted from QuoteWerks: LINE#=18-66, PART=66-154, QTY=154-187,
    # UOM=187-243, DESC=243-446, UPRICE=446-515, TPRICE=515-594
    COLS = [
        ("LINE #",      ML,       48),   # 18 → 66
        ("MFG. PART #", ML + 48,  88),   # 66 → 154
        ("QTY",         ML + 136, 33),   # 154 → 187
        ("UOM",         ML + 169, 56),   # 187 → 243
        ("DESCRIPTION", ML + 225, 203),  # 243 → 446
        ("UNIT PRICE",  ML + 428, 69),   # 446 → 515
        ("TOTAL PRICE", ML + 497, 79),   # 515 → 594
    ]

    table_top_y = bar_y + bar_h + 4  # compact gap before line items
    hdr_h = 22

    def _draw_table_header(ty):
        """Draw column headers at top-origin y. Returns top-origin y of first data row."""
        for name, cx, cw in COLS:
            # Filled header cell
            rl_y = Y(ty) - hdr_h
            c.setFillColor(FILL)
            c.rect(cx, rl_y, cw, hdr_h, fill=1, stroke=0)
            c.setStrokeColor(TBL_BD)
            c.setLineWidth(0.5)
            c.rect(cx, rl_y, cw, hdr_h, fill=0, stroke=1)
            # Header text
            c.setFillColor(BLACK)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(cx + 4, rl_y + 7, name)
        return ty + hdr_h

    cur_y = _draw_table_header(table_top_y)  # top-origin cursor
    subtotal = 0.0
    page_num = 1
    total_pages = 1  # will fix up if multi-page

    for idx, _raw_item in enumerate(items):
        item = _normalize_item(_raw_item)
        qty    = item["qty"] or 1
        uprice = item["price_per_unit"]
        if not uprice:
            uprice = _raw_item.get("unit_price") or _raw_item.get("our_price") or _raw_item.get("recommended_price") or 0
            try: uprice = float(uprice)
            except (TypeError, ValueError): uprice = 0.0
        tprice = round(uprice * qty, 2)
        subtotal += tprice

        desc = item["description"]
        part = item["part_number"]

        # ── Dynamic row height based on description wrapping ──────────────────
        desc_col_w = 203 - 8  # desc column width minus padding
        desc_lines = simpleSplit(desc, "Helvetica", 8.5, desc_col_w)
        # At least 1 line; each line ~10pt; plus 8pt padding
        row_h = max(20, len(desc_lines) * 10 + 8)

        # ── Page break if needed (leave 100pt for totals) ─────────────────────
        if Y(cur_y) - row_h < 100:
            # Footer on current page (use placeholder for total pages)
            c.setFillColor(GRAY)
            c.setFont("Helvetica", 8)
            c.drawRightString(MR, 20, f"Page {page_num}")
            # Continuation header on new page
            c.showPage()
            page_num += 1
            total_pages += 1
            # Mini header on continuation pages
            c.setFont("Helvetica-Bold", 10)
            c.setFillColor(NAVY)
            c.drawString(ML, Y(25), f"Reytech Quote {quote_number}")
            c.setFont("Helvetica", 8)
            c.setFillColor(GRAY)
            c.drawRightString(MR, Y(25), f"(continued)")
            cur_y = 40  # start near top of new page
            cur_y = _draw_table_header(cur_y)

        # ── Row background (alternate) ────────────────────────────────────────
        rl_row_y = Y(cur_y) - row_h
        if idx % 2 == 1:
            c.setFillColor(ALT_ROW)
            c.rect(ML, rl_row_y, UW, row_h, fill=1, stroke=0)

        # ── Cell borders ──────────────────────────────────────────────────────
        c.setStrokeColor(TBL_BD)
        c.setLineWidth(0.3)
        for _, cx, cw in COLS:
            c.rect(cx, rl_row_y, cw, row_h, fill=0, stroke=1)

        # ── Cell content ──────────────────────────────────────────────────────
        c.setFillColor(BLACK)
        text_baseline = rl_row_y + row_h - 12  # top line of text

        # LINE #
        c.setFont("Helvetica", 9)
        c.drawString(COLS[0][1] + 8, text_baseline,
                     str(item.get("line_number", idx + 1)))

        # MFG. PART #
        c.setFont("Helvetica", 8)
        part_col_w = COLS[1][2] - 8  # column width minus padding
        # Truncate to fit column — at 8pt, ~5pt per char → ~16 chars
        if c.stringWidth(part, "Helvetica", 8) > part_col_w:
            # Try smaller font first
            c.setFont("Helvetica", 6.5)
            if c.stringWidth(part, "Helvetica", 6.5) > part_col_w:
                # Still too long — truncate
                while len(part) > 3 and c.stringWidth(part, "Helvetica", 6.5) > part_col_w:
                    part = part[:-1]
        c.drawString(COLS[1][1] + 4, text_baseline, part)

        # QTY (right-aligned in cell)
        c.setFont("Helvetica", 9)
        qty_cx, qty_cw = COLS[2][1], COLS[2][2]
        c.drawRightString(qty_cx + qty_cw - 8, text_baseline, _fmt_qty(qty))

        # UOM
        c.drawString(COLS[3][1] + 4, text_baseline,
                     str(item.get("uom", "EA")).upper())

        # DESCRIPTION (multi-line, dynamic height)
        c.setFont("Helvetica", 8.5)
        dy = text_baseline
        for dline in desc_lines:
            c.drawString(COLS[4][1] + 4, dy, dline)
            dy -= 10

        # UNIT PRICE (right-aligned)
        c.setFont("Helvetica", 9)
        up_cx, up_cw = COLS[5][1], COLS[5][2]
        c.drawRightString(up_cx + up_cw - 6, text_baseline, f"${uprice:,.2f}")

        # TOTAL PRICE (right-aligned)
        tp_cx, tp_cw = COLS[6][1], COLS[6][2]
        c.drawRightString(tp_cx + tp_cw - 6, text_baseline, f"${tprice:,.2f}")

        cur_y += row_h

    # ══════════════════════════════════════════════════════════════════════════
    # TOTALS SECTION
    # ══════════════════════════════════════════════════════════════════════════
    tax     = round(subtotal * rate, 2) if include_tax else 0.0
    total   = round(subtotal + tax + shipping, 2)

    # Totals are right-aligned under UNIT PRICE + TOTAL PRICE columns
    # From extraction: labels at x=429→514 (w=85), values at x=514→594 (w=80)
    lbl_x  = 429
    lbl_w  = 85
    val_x  = 514
    val_w  = 80
    tot_h  = 19

    _tax_label = f"TAX ({rate*100:.2f}%)" if include_tax else "TAX"
    totals_data = [
        ("SUBTOTAL",  f"${subtotal:,.2f}",  False),
        (_tax_label,  f"${tax:,.2f}",        False),
        ("SHIPPING",  "$0.00",               False),
        ("TOTAL",     f"${total:,.2f}",      True),
    ]

    ty = cur_y + 4  # gap below last row (top-origin)

    for label, val, is_total in totals_data:
        rl_y = Y(ty) - tot_h

        # Page break safety
        if rl_y < 30:
            c.setFillColor(GRAY)
            c.setFont("Helvetica", 8)
            c.drawRightString(MR, 20, f"Page {page_num}")
            c.showPage()
            page_num += 1
            total_pages += 1
            rl_y = H - 60

        # Label cell (always filled)
        c.setFillColor(FILL)
        c.rect(lbl_x, rl_y, lbl_w, tot_h, fill=1, stroke=0)
        lw = 1.0 if is_total else 0.5
        c.setStrokeColor(TBL_BD)
        c.setLineWidth(lw)
        c.rect(lbl_x, rl_y, lbl_w, tot_h, fill=0, stroke=1)

        # Value cell
        c.setFillColor(WHITE)
        c.rect(val_x, rl_y, val_w, tot_h, fill=1, stroke=0)
        c.setStrokeColor(TBL_BD)
        c.setLineWidth(lw)
        c.rect(val_x, rl_y, val_w, tot_h, fill=0, stroke=1)

        # Text
        c.setFillColor(BLACK)
        fsz = 11 if is_total else 10
        c.setFont("Helvetica-Bold", fsz)
        c.drawRightString(lbl_x + lbl_w - 6, rl_y + 5, label)
        c.setFont("Helvetica-Bold" if is_total else "Helvetica", fsz)
        c.drawRightString(val_x + val_w - 6, rl_y + 5, val)

        ty += tot_h + 1

    # ── Quote Notes (printed below totals if present) ────────────────────────
    if notes and notes.strip():
        import textwrap as _textwrap
        # rl_y is the bottom of the last totals row (bottom-origin ReportLab coords)
        # Notes go directly below it — no coordinate conversion needed
        notes_label_y = rl_y - 20
        if notes_label_y < 50:  # near bottom — start a new page
            c.setFont("Helvetica", 8)
            c.setFillColor(GRAY)
            c.drawRightString(MR, 20, f"Page {page_num}")
            c.showPage()
            page_num += 1
            total_pages += 1
            notes_label_y = H - 60
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(GRAY)
        c.drawString(ML + 4, notes_label_y, "NOTES:")
        note_lines = []
        for para in notes.strip().split("\n"):
            wrapped = _textwrap.wrap(para.strip(), width=112)
            note_lines.extend(wrapped if wrapped else [""])
        nl_y = notes_label_y - 12
        c.setFont("Helvetica", 8)
        for nl in note_lines[:10]:
            if nl_y < 35:
                break
            c.setFillColor(GRAY)
            c.drawString(ML + 4, nl_y, nl)
            nl_y -= 10

    # ── Footer ────────────────────────────────────────────────────────────────
    c.setFillColor(GRAY)
    c.setFont("Helvetica", 8)
    if total_pages > 1:
        c.drawRightString(MR, 20, f"Page {page_num} of {total_pages}")
    else:
        c.drawRightString(MR, 20, f"Quote {quote_number}")

    try:
        c.save()
    except Exception as _se:
        if _allocated_number:
            _rollback_quote_number(quote_number)
        log.error("Quote %s PDF save failed — number rolled back: %s", quote_number, _se)
        raise

    result = {
        "ok": True,
        "path": output_path,
        "quote_number": quote_number,
        "agency": agency,
        "institution": to_name,
        "rfq_number": rfq_num,
        "subtotal": subtotal,
        "tax": tax,
        "tax_rate": rate,
        "shipping": shipping,
        "total": total,
        "items_count": len(items),
        "date": quote_date,
        "expiry": expiry_date,
        "ship_to_name": ship_name,
        "ship_to_address": ship_addr,
        "items_text": " | ".join(
            str(it.get("description", ""))[:80] for it in items
        ),
        "items_detail": [
            {
                "description": str(it.get("description", ""))[:120],
                "part_number": str(it.get("part_number", "")),
                "qty": it.get("qty", 0),
                "unit_price": it.get("unit_price", 0),
                "asin": it.get("asin", ""),
                "supplier_url": it.get("supplier_url", ""),
                "supplier": it.get("supplier", ""),
                "cost": it.get("cost", 0),
            }
            for it in items
        ],
        # Bidirectional linking — trace to source document
        "source_pc_id": quote_data.get("source_pc_id", ""),
        "source_rfq_id": quote_data.get("source_rfq_id", ""),
    }
    _log_quote(result)
    log.info("Quote %s generated: $%.2f total, %d items → %s",
             quote_number, result["total"], result["items_count"], output_path)
    # Stamp template version for audit trail
    try:
        from src.forms.pdf_versioning import stamp_pdf_metadata
        stamp_pdf_metadata("quote", quote_number,
                           {"generator": "quote_generator", "file_path": output_path})
    except Exception:
        pass
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE WRAPPERS
# ═══════════════════════════════════════════════════════════════════════════════

def generate_quote_from_pc(pc: dict, output_path: str, **kwargs) -> dict:
    """Generate Reytech quote from a Price Check record."""
    header = pc.get("parsed", {}).get("header", {})
    items = pc.get("items", [])

    # To: and Ship To: should show the same institution + address
    institution = header.get("institution", pc.get("institution", ""))
    ship_to_raw = pc.get("ship_to", "") or ""
    delivery = pc.get("delivery_location", "") or ""

    # If user explicitly entered a delivery address (>10 chars), trust it over facility DB
    _user_set_delivery = bool(delivery and delivery.strip() and len(delivery.strip()) > 10)

    # ── Facility lookup: try all available address sources ──
    facility = (_lookup_facility(delivery) or
                _lookup_facility(ship_to_raw) or
                _lookup_facility(institution))

    if _user_set_delivery:
        # User-entered delivery address takes priority — parse it directly
        ship_name, ship_addr = _parse_address_parts(delivery)
        if not ship_name:
            ship_name = institution
        to_name = institution
        to_addr = list(ship_addr)
        # Still use facility for agency detection
        if facility and "agency" not in kwargs:
            _parent_map = {"CDCR": "CDCR", "CCHCS": "CCHCS", "CalVet": "CalVet", "DGS": "DGS", "DSH": "DSH"}
            if facility["parent"] in _parent_map:
                kwargs["agency"] = _parent_map[facility["parent"]]
    elif facility:
        ship_name = facility["name"]
        ship_addr = list(facility["address"])
        to_name = institution or facility["parent_full"]
        to_addr = list(facility["address"])
        if "agency" not in kwargs:
            _parent_map = {"CDCR": "CDCR", "CCHCS": "CCHCS", "CalVet": "CalVet", "DGS": "DGS", "DSH": "DSH"}
            if facility["parent"] in _parent_map:
                kwargs["agency"] = _parent_map[facility["parent"]]
    else:
        # Manual parsing
        source = delivery or ship_to_raw
        if source:
            ship_name, ship_addr = _parse_address_parts(source)
        else:
            ship_name, ship_addr = institution, []
        if not ship_name:
            ship_name = institution
        to_name = institution
        to_addr = list(ship_addr)

    data = {
        "institution": to_name,
        "to_address": to_addr,
        "ship_to_name": ship_name,
        "ship_to_address": ship_addr,
        "rfq_number": pc.get("pc_number", ""),
        "source_pc_id": pc.get("id", ""),
        "line_items": [],
    }

    for item in items:
        if item.get("no_bid"):
            continue
        pricing = item.get("pricing", {})
        up = pricing.get("recommended_price") or pricing.get("amazon_price") or 0

        # Pull ASIN from Amazon lookup for MFG PART # column
        asin = pricing.get("amazon_asin", "")
        part_num = asin if asin else item.get("part_number", "")

        # Build description with ASIN reference
        desc = item.get("description", "")
        if asin and f"ASIN" not in desc:
            desc = f"{desc}\nRef ASIN: {asin}"

        data["line_items"].append({
            "line_number": item.get("item_number", ""),
            "part_number": part_num,
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "EA"),
            "description": desc,
            "unit_price": up,
            # Profit tracking — first-class fields take precedence over pricing dict
            "vendor_cost": item.get("vendor_cost") or pricing.get("unit_cost") or pricing.get("amazon_price") or 0,
            "markup_pct":  item.get("markup_pct")  or pricing.get("markup_pct") or 25,
        })

    # ── Tax: always include, look up rate from ship-to facility ───────────
    if "include_tax" not in kwargs:
        kwargs["include_tax"] = True
    if "tax_rate" not in kwargs:
        try:
            from src.core.tax_rates import get_rate_for_facility, lookup_tax_rate
            if facility:
                tax_info = get_rate_for_facility(facility)
            elif ship_addr:
                _addr_str = " ".join(ship_addr) if isinstance(ship_addr, list) else str(ship_addr)
                _re = re  # already imported at top
                _zm = _re.search(r'\b(\d{5})\b', _addr_str)
                tax_info = lookup_tax_rate(
                    address=ship_addr[0] if ship_addr else "",
                    city=ship_name,
                    zip_code=_zm.group(1) if _zm else "",
                )
            else:
                tax_info = lookup_tax_rate()
            kwargs["tax_rate"] = tax_info["rate"]
            log.info("Tax rate for PC %s: %.4f (%s, source=%s)",
                     pc.get("pc_number", "?"), tax_info["rate"],
                     tax_info.get("jurisdiction", "?"), tax_info.get("source", "?"))
        except Exception as _te:
            log.warning("Tax rate lookup failed for PC: %s — using CA base 7.25%%", _te)
            kwargs["tax_rate"] = 0.0725

    kwargs.setdefault("shipping", 0.0)
    if "notes" not in kwargs and pc.get("quote_notes"):
        kwargs["notes"] = pc["quote_notes"]

    return generate_quote(data, output_path, **kwargs)


def generate_quote_from_rfq(rfq: dict, output_path: str, **kwargs) -> dict:
    """Generate Reytech quote from an RFQ record."""
    institution = rfq.get("agency_name", "") or rfq.get("department", rfq.get("requestor_name", ""))
    delivery = rfq.get("delivery_location", "") or ""
    ship_to_raw = rfq.get("ship_to", "") or ""
    ship_to_name_raw = rfq.get("ship_to_name", "") or ""
    institution_name = rfq.get("institution_name", "") or ""

    # ── Facility lookup: try all available address sources ──
    facility = (_lookup_facility(delivery) or 
                _lookup_facility(ship_to_raw) or 
                _lookup_facility(ship_to_name_raw) or
                _lookup_facility(institution_name) or
                _lookup_facility(institution))

    # ── If still blank: find zip code → match facility ──
    if not facility:
        # Gather ALL text we have: RFQ fields + email body + email_log + PDF content
        _rid = rfq.get("id", "")
        _all_text = f"{delivery} {ship_to_raw} {ship_to_name_raw} {institution_name}"
        _all_text += " " + rfq.get("body_text", "")
        _all_text += " " + rfq.get("email_subject", "")
        
        # Pull email body from DB
        if _rid:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _row = conn.execute(
                        "SELECT full_body FROM email_log WHERE rfq_id = ? ORDER BY id DESC LIMIT 1",
                        (_rid,)
                    ).fetchone()
                    if _row and _row["full_body"]:
                        _all_text += " " + _row["full_body"]
            except Exception:
                pass
        
        # Pull text from stored PDFs
        if _rid:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    _pdfs = conn.execute(
                        "SELECT data, filename FROM rfq_files WHERE rfq_id = ? AND category IN ('attachment','template') LIMIT 5",
                        (_rid,)
                    ).fetchall()
                for _pr in (_pdfs or []):
                    if _pr["data"] and (_pr["filename"] or "").lower().endswith(".pdf"):
                        try:
                            from pypdf import PdfReader
                            import io
                            for _pg in PdfReader(io.BytesIO(_pr["data"])).pages[:3]:
                                _all_text += " " + (_pg.extract_text() or "")
                        except Exception:
                            pass
            except Exception:
                pass
        
        # Match zip → facility
        facility, _amb = _lookup_facility_by_zip(_all_text)
        if facility:
            log.info("Facility matched by zip: %s", facility["name"])

    # ── Set To / Ship To / Bill To ──
    # RULE: "To" and "Ship To" are ALWAYS the same — the individual child facility.
    #       "Bill To" is ALWAYS the parent agency's master billing address.
    #
    # Priority for ship-to: RFQ delivery_location (from 703B) > facility lookup > raw fields
    # Bill-to: always from AGENCY_CONFIGS based on parent agency
    
    ship_name = ""
    ship_addr = []
    
    # 1. Try RFQ's parsed delivery_location first (703B has exact address)
    if delivery and delivery.strip():
        _parsed_name, _parsed_addr = _parse_address_parts(delivery)
        if _parsed_name or _parsed_addr:
            ship_name = _parsed_name or ""
            ship_addr = _parsed_addr or []

    # 2. Facility lookup as fallback
    if not ship_name and facility:
        ship_name = facility["name"]
        ship_addr = list(facility["address"])
    
    # 3. Raw fields as last resort
    if not ship_name:
        if ship_to_raw:
            ship_name, ship_addr = _parse_address_parts(ship_to_raw)
        elif ship_to_name_raw:
            ship_name, ship_addr = _parse_address_parts(ship_to_name_raw)
        if not ship_name:
            ship_name = institution or institution_name or ""
    
    # To = same as Ship To (child facility)
    to_name = ship_name
    to_addr = list(ship_addr)

    # Agency detection for bill-to
    if facility:
        _parent_agency_map = {"CDCR": "CDCR", "CCHCS": "CCHCS", "CalVet": "CalVet", "DGS": "DGS", "DSH": "DSH"}
        _agency_key = _parent_agency_map.get(facility["parent"], "")
        if _agency_key and "agency" not in kwargs:
            kwargs["agency"] = _agency_key

    log.info("Quote addresses: To/ShipTo=%s %s", ship_name, ship_addr)

    data = {
        "institution": to_name,
        "to_address": to_addr,
        "ship_to_name": ship_name,
        "ship_to_address": ship_addr,
        "rfq_number": rfq.get("solicitation_number", ""),
        "source_rfq_id": rfq.get("id", ""),
        "requestor_email": rfq.get("requestor_email", ""),
        "line_items": [],
    }

    for idx_q, _raw_q in enumerate(rfq.get("line_items", [])):
        item = _normalize_item(_raw_q)
        up = item["price_per_unit"] or _raw_q.get("our_price") or 0
        pn = item.get("item_number", item.get("part_number", ""))
        
        # Don't use pure numeric values as MFG PART # — those are form row numbers
        # Real part numbers have letters+digits (e.g. "EQX7044", "SHINY-S-852", "491861315")
        # Pure sequential digits 1-99 are line item numbers, not MFG part numbers
        if pn and pn.strip().isdigit() and int(pn.strip()) < 100:
            pn = ""
        
        asin = item.get("asin", "")
        supplier_url = item.get("supplier_url", "") or item.get("item_link", "")
        
        # Auto-generate Amazon URL from ASIN/B0 part number
        if not supplier_url and asin:
            supplier_url = f"https://www.amazon.com/dp/{asin}"
        elif not supplier_url and pn and pn.startswith("B0"):
            supplier_url = f"https://www.amazon.com/dp/{pn}"
        
        # Use description_raw if available (full untruncated text from parser)
        # Fall back to description (cleaned/shortened version)
        _desc = item.get("description_raw") or item.get("description", "")
        _desc = _desc.replace('\ufffd', '').replace('■', '').replace('□', '')
        _desc = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', _desc)
        _desc = re.sub(r'\s+', ' ', _desc).strip()
        
        data["line_items"].append({
            "line_number": item.get("line_number") or (idx_q + 1),
            "part_number": pn,
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "EA"),
            "description": _desc,
            "unit_price": up,
            "asin": asin,
            "supplier_url": supplier_url,
            "supplier": item.get("item_supplier", "") or ("Amazon" if asin or (pn and pn.startswith("B0")) else ""),
            "cost": item.get("supplier_cost", 0),
        })

    # Enrich items without intelligence
    try:
        from src.agents.quote_intelligence import enrich_extracted_items
        _unenriched = [i for i in data["line_items"] if not i.get("intelligence")]
        if _unenriched:
            _enriched = enrich_extracted_items(_unenriched)
            _emap = {e.get("original_description", ""): e.get("intelligence")
                     for e in _enriched if e.get("intelligence")}
            for _item in data["line_items"]:
                _d = _item.get("description", "")
                if _d in _emap and not _item.get("intelligence"):
                    _item["intelligence"] = _emap[_d]
    except Exception:
        pass

    # Pass agency explicitly if known from RFQ — OVERRIDES facility-derived agency
    # (CCHCS facilities are inside CDCR prisons, but CCHCS bills separately)
    _agency_map = {"calvet": "CalVet", "cchcs": "CCHCS", "dsh": "DSH", "dgs": "DGS", "cdcr": "CDCR"}
    _rfq_agency = rfq.get("agency", "")
    if _rfq_agency in _agency_map:
        _resolved_agency = _agency_map[_rfq_agency]
        if kwargs.get("agency") != _resolved_agency:
            log.info("RFQ agency %s overrides facility-derived %s", _resolved_agency, kwargs.get("agency", "none"))
        kwargs["agency"] = _resolved_agency
        # Bill-to comes from AGENCY_CONFIGS — handled by generate_quote()
        # Do NOT override data["institution"] or data["to_address"] — those are the child facility

    # ── Tax: always include, look up rate from ship-to facility ───────────
    # Uses CDTFA API with fallback to hardcoded rates per zip code
    if "include_tax" not in kwargs:
        kwargs["include_tax"] = True
    if "tax_rate" not in kwargs:
        try:
            from src.core.tax_rates import get_rate_for_facility, lookup_tax_rate
            if facility:
                tax_info = get_rate_for_facility(facility)
            elif ship_addr:
                # Parse zip from address lines
                _addr_str = " ".join(ship_addr) if isinstance(ship_addr, list) else str(ship_addr)
                _re = re  # already imported at top
                _zm = _re.search(r'\b(\d{5})\b', _addr_str)
                tax_info = lookup_tax_rate(
                    address=ship_addr[0] if ship_addr else "",
                    city=ship_name,
                    zip_code=_zm.group(1) if _zm else "",
                )
            else:
                tax_info = lookup_tax_rate()
            kwargs["tax_rate"] = tax_info["rate"]
            log.info("Tax rate for %s: %.4f (%s, source=%s)",
                     ship_name or "unknown", tax_info["rate"],
                     tax_info.get("jurisdiction", "?"), tax_info.get("source", "?"))
        except Exception as _te:
            log.warning("Tax rate lookup failed: %s — using CA base 7.25%%", _te)
            kwargs["tax_rate"] = 0.0725

    # No shipping line — shipping is baked into item cost/margin
    kwargs.setdefault("shipping", 0.0)
    if "notes" not in kwargs and rfq.get("quote_notes"):
        kwargs["notes"] = rfq["quote_notes"]
    if "revision" not in kwargs and rfq.get("quote_revision"):
        kwargs["revision"] = rfq["quote_revision"]

    return generate_quote(data, output_path, **kwargs)


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test = {
        "institution": "SCC - Sierra Conservation Center",
        "ship_to_name": "SCC - Sierra Conservation Center",
        "ship_to_address": ["5100 O'Byrnes Ferry Road", "Jamestown, CA 95327"],
        "rfq_number": "10838043",
        "line_items": [
            {"line_number": 1, "part_number": "6500-001-430", "qty": 2, "uom": "SET",
             "description": "X-RESTRAINT PACKAGE by Stryker Medical\nNew OEM Original Outright\nOEM#: 6500001430",
             "unit_price": 454.40},
            {"line_number": 2, "part_number": "6250-001-125", "qty": 2, "uom": "EACH",
             "description": "RESTRAINT STRAP, CHEST, GREEN, FOR USE WITH: FOR MODEL 6250/6251/6252 STAIR-PRO® STAIR CHAIR by Stryker Medical\nNew OEM Original Outright OEM#: 6250001125",
             "unit_price": 69.12},
            {"line_number": 3, "part_number": "6250-001-126", "qty": 2, "uom": "EACH",
             "description": "RESTRAINT STRAP, CHEST, BLACK, FOR USE WITH: FOR MODEL 6250/6251/6252 STAIR-PRO® STAIR CHAIR by Stryker Medical\nOEM#: 6250001126",
             "unit_price": 69.12},
        ],
    }

    os.makedirs("/tmp/quotes", exist_ok=True)

    r1 = generate_quote(test, "/tmp/quotes/CDCR.pdf", agency="CDCR",
                         quote_number="R26Q14", include_tax=True)
    print(f"CDCR:  ${r1['total']:,.2f}  items={r1['items_count']}  → {r1['path']}")

    r2 = generate_quote(test, "/tmp/quotes/CCHCS.pdf", agency="CCHCS",
                         quote_number="R26Q14", include_tax=True)
    print(f"CCHCS: ${r2['total']:,.2f}  items={r2['items_count']}  → {r2['path']}")

    # Test with a long-description item to verify dynamic heights
    test_long = {
        "institution": "CalVet - Barstow Veterans Home",
        "ship_to_name": "CalVet - Barstow Veterans Home",
        "ship_to_address": ["100 East Veterans Parkway", "Barstow, CA 92311"],
        "rfq_number": "CVH-2026-001",
        "line_items": [
            {"line_number": 1, "part_number": "ABC-123", "qty": 5, "uom": "EACH",
             "description": "SHORT ITEM", "unit_price": 10.00},
            {"line_number": 2, "part_number": "DEF-456-LNG", "qty": 1, "uom": "SET",
             "description": "LONG DESCRIPTION ITEM: This is a medical device with extensive specifications including multiple sub-components, replacement parts, calibration tools, mounting hardware, instruction manual, quick-start guide, warranty card, and carrying case. Compatible with models A100, B200, C300, D400, E500. Requires annual maintenance per manufacturer guidelines. Includes 2-year limited warranty covering defects in materials and workmanship.",
             "unit_price": 2499.99},
            {"line_number": 3, "part_number": "GHI-789", "qty": 100, "uom": "BOX",
             "description": "Nitrile gloves, powder-free, blue, medium", "unit_price": 8.50},
        ],
    }

    r3 = generate_quote(test_long, "/tmp/quotes/CalVet_long.pdf", agency="CalVet",
                         include_tax=True)
    print(f"CalVet: ${r3['total']:,.2f}  items={r3['items_count']}  → {r3['path']}")
    print(f"\nQuotes log: {len(get_all_quotes())} entries")
    print(f"Next quote would be: {peek_next_quote_number()}")
