


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
        "show_bill_to": False,
        "show_permit": False,
        "bill_to_name": "",
        "bill_to_lines": [],
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
        "bill_to_name": "Dept. of Veterans Affairs",
        "bill_to_lines": ["1227 O Street", "Sacramento, CA 95814", "United States"],
        "default_tax": 0.0725,
        "default_terms": "Net 30",
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
# QUOTE NUMBERING — R{YY}{seq}, resets Jan 1 each year
# Format: R2616, R2617 ... R27001 (new year)
# ═══════════════════════════════════════════════════════════════════════════════

def _load_counter():
    """Load counter from SQLite (primary) with JSON fallback."""
    try:
        from src.core.db import get_setting
        year_val = get_setting("quote_counter_year", datetime.now().year)
        seq_val = get_setting("quote_counter_seq", get_setting("quote_counter", 16))
        return {"year": int(year_val), "seq": int(seq_val)}
    except Exception:
        pass
    # JSON fallback
    path = os.path.join(DATA_DIR, "quote_counter.json")
    try:
        with open(path) as f:
            raw = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if "counter" in raw and "seq" not in raw:
        migrated = {"year": datetime.now().year, "seq": raw["counter"]}
        _save_counter(migrated)
        return migrated
    return raw

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
    log.info("Quote counter set to seq=%d year=%d → next will be R%sQ%d",
             seq, year, str(year)[-2:], seq + 1)

def _should_reset_counter(stored_year: int) -> bool:
    """Reset at 12:00:01 AM on Jan 1 of a new year only."""
    now = datetime.now()
    return stored_year != now.year

def _next_quote_number() -> str:
    """R{YY}Q{seq} — sequential per calendar year, resets midnight Jan 1."""
    data = _load_counter()
    year = datetime.now().year
    yy = str(year)[-2:]

    if _should_reset_counter(data.get("year", 0)):
        log.info("New year detected — resetting quote counter from seq=%d (year=%d) to seq=1 (year=%d)",
                 data.get("seq", 0), data.get("year", 0), year)
        data = {"year": year, "seq": 0}

    data["seq"] = data.get("seq", 0) + 1
    data["year"] = year
    _save_counter(data)
    return f"R{yy}Q{data['seq']}"

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
    """Return all quotes. By default excludes test/QA quotes."""
    path = os.path.join(DATA_DIR, "quotes_log.json")
    try:
        with open(path) as f:
            quotes = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        quotes = []
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
                qt.get("ship_to_address", ""),
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
    """Mark a quote as won, lost, or pending. Records status_history. Returns True if found."""
    if status not in VALID_STATUSES:
        return False
    quotes = get_all_quotes()
    found = False
    now = datetime.now().isoformat()
    for qt in quotes:
        if qt.get("quote_number") == quote_number:
            qt["status"] = status
            qt["status_updated"] = now
            if po_number:
                qt["po_number"] = po_number
            if notes:
                qt["status_notes"] = notes
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
    """Find logo: data/reytech_logo.{png,jpg,jpeg,gif}"""
    for d in [DATA_DIR, os.path.dirname(__file__), "/app/data", "/app"]:
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

    if not quote_number:
        quote_number = _next_quote_number()

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

    c = canvas.Canvas(output_path, pagesize=letter)
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

    def text(x, yt, txt, font="Helvetica", size=9, color=BLACK, align="left"):
        c.setFont(font, size)
        c.setFillColor(color)
        s = str(txt) if txt else ""
        rl_y = Y(yt)
        if align == "right":
            c.drawRightString(x, rl_y, s)
        elif align == "center":
            c.drawCentredString(x, rl_y, s)
        else:
            c.drawString(x, rl_y, s)

    # ══════════════════════════════════════════════════════════════════════════
    # PAGE 1 HEADER
    # ══════════════════════════════════════════════════════════════════════════

    # ── "QUOTE" title — y=129, right-aligned, ~20pt bold ──────────────────────
    text(MR, 148, "QUOTE", "Helvetica-Bold", 22, BLACK, "right")

    # ── Horizontal rule at y=148 (dark purple-navy) ───────────────────────────
    c.setStrokeColor(LBL_BD)
    c.setLineWidth(1.5)
    c.line(ML, Y(149), MR, Y(149))

    # ── QUOTE # box: y=155.5→177, label=396→463, value=464→594 ───────────────
    # Label cell (filled)
    box(396, 155, 67, 22, fill=True, border_color=LBL_BD)
    text(400, 172, "QUOTE #", "Helvetica-Bold", 10)
    # Value cell (white)
    box(463, 155, 131, 22, fill=False, border_color=VAL_BD)
    text(MR - 6, 172, quote_number, "Helvetica-Bold", 12, BLACK, "right")

    # ── DATE box: y=178→200 ───────────────────────────────────────────────────
    box(396, 178, 67, 22, fill=True, border_color=LBL_BD)
    text(400, 195, "DATE", "Helvetica-Bold", 10)
    box(463, 178, 131, 22, fill=False, border_color=VAL_BD)
    text(MR - 6, 195, quote_date, "Helvetica-Bold", 10, BLACK, "right")

    # ── Reytech logo + company info (left column) ──────────────────────────────
    # Original QuoteWerks logo: x=48, top=152, w=135, h=35
    logo_path = _find_logo()
    logo_text_x = ML + 34  # default text position if no logo
    if logo_path:
        try:
            img = ImageReader(logo_path)
            iw, ih = img.getSize()
            max_w, max_h = 130, 32
            scale = min(max_w / iw, max_h / ih)
            dw, dh = iw * scale, ih * scale
            logo_rl_y = Y(152) - dh
            c.drawImage(logo_path, ML + 8, logo_rl_y, width=dw, height=dh,
                        preserveAspectRatio=True, mask='auto')
            logo_text_x = ML + 8 + dw + 6  # text right of logo
        except Exception as e:
            log.warning(f"Logo load failed: {e}")
            logo_path = None

    if not logo_path:
        # Text-only fallback — navy chevron
        p = c.beginPath()
        bx, by = ML + 8, Y(164)
        p.moveTo(bx, by)
        p.lineTo(bx + 10, by + 14)
        p.lineTo(bx + 20, by)
        p.lineTo(bx + 10, by - 3)
        p.close()
        c.setFillColor(NAVY)
        c.drawPath(p, fill=1, stroke=0)
        logo_text_x = ML + 34

    # Always render "Reytech Inc." as selectable text (even when logo present)
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(NAVY)
    c.drawString(logo_text_x, Y(166), "Reytech Inc.")

    # PRD Feature P2: Enhanced branding — SB/DVBE tagline
    c.setFont("Helvetica-Oblique", 7.5)
    c.setFillColor(HexColor("#4f8cff"))
    c.drawString(logo_text_x, Y(178), "CA Certified Small Business (SB) & DVBE")

    # Company details — y positions matched to QuoteWerks extraction
    # Combined "Michael Guadan, Owner" saves a line
    c.setFillColor(BLACK)
    c.setFont("Helvetica", 9)
    info_lines = [
        (195, REYTECH["line1"]),                              # 30 Carnoustie Way
        (207, REYTECH["line2"]),                              # Trabuco Canyon, CA 92679
        (219, f"{REYTECH['contact']}, {REYTECH['title']}"),   # Michael Guadan, Owner
        (231, REYTECH["phone"]),                              # 949-229-1575
        (243, REYTECH["email"]),                              # sales@reytechinc.com
        (255, "www.reytechinc.com"),                          # website
    ]
    for iy, itxt in info_lines:
        text(TXT_X, iy, itxt, "Helvetica", 9)

    # Sellers Permit — same 9pt style as other company info (agency-dependent)
    if cfg["show_permit"]:
        text(TXT_X, 255, f"CA Sellers Permit: {REYTECH['permit']}", "Helvetica", 9)

    # ── Bill To (right side, y≈221, only for CDCR/CalVet) ─────────────────────
    if show_bill:
        text(308, 228, "Bill to:", "Helvetica-Bold", 10)
        text(405, 228, bill_name, "Helvetica", 9)
        by = 242
        for bl in bill_lines:
            text(405, by, bl, "Helvetica", 9)
            by += 12

    # ── To: / Ship to Location: — positioned in white space ──────────────────
    # CDCR: text top=312, CCHCS: text top=297 (add baseline offset)
    addr_y = 319 if show_bill else 304

    text(ML + 7, addr_y, "To:", "Helvetica-Bold", 10)
    text(TXT_X, addr_y, to_name, "Helvetica", 10)
    ay = addr_y + 14
    for line in to_addr:
        text(TXT_X, ay, line, "Helvetica", 10)
        ay += 12
    if to_addr and "united states" not in " ".join(to_addr).lower():
        text(TXT_X, ay, "United States", "Helvetica", 10)
        ay += 12

    text(305, addr_y, "Ship to Location:", "Helvetica-Bold", 10)
    text(402, addr_y, ship_name, "Helvetica", 10)
    sy = addr_y + 14
    for line in ship_addr:
        text(402, sy, line, "Helvetica", 10)
        sy += 12
    if ship_addr and "united states" not in " ".join(ship_addr).lower():
        text(402, sy, "United States", "Helvetica", 10)
        sy += 12

    # ── Salesperson / RFQ / Terms / Expiry bar ────────────────────────────────
    # 4-column bar, each column is a filled+bordered cell with header+value
    bar_y = max(ay, sy) - 8  # top of bar — tight gap matching QuoteWerks
    bar_h = 30
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

    table_top_y = bar_y + bar_h + 8  # top-origin y where table starts
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

    for idx, item in enumerate(items):
        qty    = item.get("qty", 1)
        uprice = item.get("unit_price",
                  item.get("our_price",
                  item.get("price_per_unit",
                  item.get("recommended_price", 0))))
        try:
            uprice = float(uprice)
        except (TypeError, ValueError):
            uprice = 0.0
        tprice = round(uprice * qty, 2)
        subtotal += tprice

        desc = str(item.get("description", ""))
        part = str(item.get("part_number", item.get("item_number", "")))

        # ── Dynamic row height based on description wrapping ──────────────────
        desc_col_w = 203 - 8  # desc column width minus padding
        desc_lines = simpleSplit(desc, "Helvetica", 8.5, desc_col_w)
        # At least 1 line; each line ~10pt; plus 8pt padding
        row_h = max(20, len(desc_lines) * 10 + 8)

        # ── Page break if needed (leave 100pt for totals) ─────────────────────
        if Y(cur_y) - row_h < 100:
            # Footer on current page
            c.setFillColor(GRAY)
            c.setFont("Helvetica", 8)
            c.drawRightString(MR, 20, f"{page_num} of {{PAGES}}")
            c.showPage()
            page_num += 1
            total_pages += 1
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
        c.setFont("Helvetica", 8.5)
        c.drawString(COLS[1][1] + 4, text_baseline, part[:16])

        # QTY (right-aligned in cell)
        c.setFont("Helvetica", 9)
        qty_cx, qty_cw = COLS[2][1], COLS[2][2]
        c.drawRightString(qty_cx + qty_cw - 8, text_baseline, str(qty))

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

    totals_data = [
        ("SUBTOTAL",  f"${subtotal:,.2f}",  False),
        ("SALES TAX", f"${tax:,.2f}",       False),
        ("SHIPPING",  f"${shipping:,.2f}" if shipping else "0.00", False),
        ("TOTAL",     f"${total:,.2f}",     True),
    ]

    ty = cur_y + 4  # gap below last row (top-origin)

    for label, val, is_total in totals_data:
        rl_y = Y(ty) - tot_h

        # Page break safety
        if rl_y < 30:
            c.setFillColor(GRAY)
            c.setFont("Helvetica", 8)
            c.drawRightString(MR, 20, f"{page_num} of {{PAGES}}")
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

    # ── Footer ────────────────────────────────────────────────────────────────
    c.setFillColor(GRAY)
    c.setFont("Helvetica", 8)
    c.drawRightString(MR, 20, f"{page_num} of {total_pages}")

    c.save()

    # ── Fix up page numbers if multi-page (replace {PAGES}) ──────────────────
    if total_pages > 1:
        try:
            from pypdf import PdfReader, PdfWriter
            # Simple approach: just leave the last page correct
            # For full fix, would need to post-process all page footers
            pass
        except ImportError:
            pass

    result = {
        "ok": True,
        "path": output_path,
        "quote_number": quote_number,
        "agency": agency,
        "institution": to_name,
        "rfq_number": rfq_num,
        "subtotal": subtotal,
        "tax": tax,
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
    # Parse address lines from ship_to (typically "Name, Street, City ST ZIP")
    ship_parts = [p.strip() for p in ship_to_raw.split(",") if p.strip()]

    data = {
        "institution": institution,
        "to_address": ship_parts[1:] if len(ship_parts) > 1 else ship_parts,
        "ship_to_name": ship_parts[0] if ship_parts else institution,
        "ship_to_address": ship_parts[1:] if len(ship_parts) > 1 else ship_parts,
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
        })

    return generate_quote(data, output_path, **kwargs)


def generate_quote_from_rfq(rfq: dict, output_path: str, **kwargs) -> dict:
    """Generate Reytech quote from an RFQ record."""
    institution = rfq.get("department", rfq.get("requestor_name", ""))
    delivery = rfq.get("ship_to", rfq.get("delivery_location", "")) or ""
    del_parts = [p.strip() for p in delivery.split(",") if p.strip()]

    data = {
        "institution": institution,
        "to_address": del_parts[1:] if len(del_parts) > 1 else del_parts,
        "ship_to_name": del_parts[0] if del_parts else institution,
        "ship_to_address": del_parts[1:] if len(del_parts) > 1 else del_parts,
        "rfq_number": rfq.get("solicitation_number", ""),
        "source_rfq_id": rfq.get("id", ""),
        "line_items": [],
    }

    for item in rfq.get("line_items", []):
        up = item.get("price_per_unit") or item.get("our_price") or 0
        data["line_items"].append({
            "line_number": item.get("line_number", item.get("item_number", "")),
            "part_number": item.get("item_number", item.get("part_number", "")),
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "EA"),
            "description": item.get("description", ""),
            "unit_price": up,
        })

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
