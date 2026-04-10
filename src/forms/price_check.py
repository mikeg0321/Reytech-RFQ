


"""
price_check.py — Price Check Processor for Reytech RFQ Automation
Phase 6 | Version: 6.2

Parses AMS 704 Price Check PDFs (fillable or scanned), looks up prices
via SerpApi/Amazon and SCPRS, runs through Pricing Oracle, and fills
in the completed form.

Workflow:
  1. Parse AMS 704 PDF → extract header + line items
  2. Search Amazon (SerpApi) for each item
  3. Run through Pricing Oracle for recommended pricing
  4. Fill supplier info + unit prices + extensions + totals
  5. Output completed PDF

Dependencies: pypdf (already installed)
"""

# ═══════════════════════════════════════════════════════════════
# DATA MODEL — PC Item Dict
# ═══════════════════════════════════════════════════════════════
# Each item in pc["items"] has this structure:
#
# CORE FIELDS (always present):
#   description     str    Item description text
#   qty             int    Quantity ordered (ALWAYS coerce with int(float(raw)))
#   uom             str    Unit of measure ("EA", "PACK", "CASE", etc.)
#   qty_per_uom     int    Items per unit (default 1)
#   row_index       int    1-based row number for PDF field mapping
#   no_bid          bool   True if item is marked no-bid
#   is_substitute   bool   True if offering a substitute product
#
# PRICING FIELDS (dual storage — write to BOTH on save):
#   unit_price      float  Selling price → also in pricing["recommended_price"]
#   vendor_cost     float  Our cost      → also in pricing["unit_cost"]
#   markup_pct      float  Markup %      → also in pricing["markup_pct"]
#
# PRICING SUB-DICT (item["pricing"]):
#   recommended_price  float  = unit_price (canonical sell price)
#   unit_cost          float  = vendor_cost (canonical cost)
#   markup_pct         float  = markup_pct
#   amazon_price       float  Price from Amazon lookup
#   scprs_price        float  Price from state contract lookup
#   amazon_url         str    Amazon product URL
#   scprs_confidence   float  0-1 confidence of SCPRS match
#   web_source         str    Source name from web search
#   web_url            str    URL from web search
#
# IDENTITY FIELDS:
#   mfg_number      str    Manufacturer part number / MFG#
#   item_link       str    Supplier product URL
#   item_supplier   str    Detected supplier name from URL
#   item_number     str    Sequential item number on the form
#
# METADATA:
#   notes           str    User notes for this item
#   description_raw str    Original uncleaned description (set on first display)
#   confidence      dict   Grading info {grade: "A"/"B"/"C"/"F"}
#   profit_unit     float  Per-unit profit (price - cost)
#   profit_total    float  Total profit (profit_unit × qty)
#   margin_pct      float  Margin percentage
#
# ROW_FIELDS MAPPING (for PDF generation):
#   "PRICE PER UNITRow{n}" → unit_price
#   "EXTENSIONRow{n}"      → unit_price × qty
#   Row numbers are 1-based, sequential across pages
#   Page 1: rows 1-8, Page 2: rows 9-19, Page 3: rows 20-27
# ═══════════════════════════════════════════════════════════════

import json
import os
import re
import logging
from datetime import datetime, timedelta, timezone


def _pst_now() -> datetime:
    """Current time in US/Pacific (handles PST/PDT automatically)."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Los_Angeles")).replace(tzinfo=None)
from typing import Optional
from copy import deepcopy

try:
    from pypdf import PdfReader, PdfWriter
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    from src.agents.product_research import research_product, quick_lookup
    HAS_RESEARCH = True
except ImportError:
    try:
        from src.agents.product_research import research_product, quick_lookup
        HAS_RESEARCH = True
    except ImportError:
        HAS_RESEARCH = False

try:
    from src.knowledge.pricing_oracle import recommend_price
    HAS_ORACLE = True
except ImportError:
    try:
        from src.knowledge.pricing_oracle import recommend_price
        HAS_ORACLE = True
    except ImportError:
        HAS_ORACLE = False

try:
    from src.knowledge.won_quotes_db import find_similar_items
    HAS_WON_QUOTES = True
except ImportError:
    try:
        from src.knowledge.won_quotes_db import find_similar_items
        HAS_WON_QUOTES = True
    except ImportError:
        HAS_WON_QUOTES = False

log = logging.getLogger("pricecheck")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")


def clean_description(raw: str) -> str:
    """
    Extract the product name from a verbose procurement description.
    
    '3/4"x3"; Engraved two line name tag, black/white, Arial, 18 font size, magnetic; rounded corners'
    → 'Engraved two line name tag, black/white'
    
    Strips: dimensions, font specs, manufacturing details, trailing specs after semicolons.
    """
    if not raw:
        return raw
    
    text = raw.strip()
    
    # Remove leading dimensions like '3/4"x3"' or '8.5x11' or '2"x3"' 
    text = re.sub(r'^[\d/]+["\']?\s*[xX×]\s*[\d/]+["\']?\s*[;:,.]?\s*', '', text)
    
    # Split on semicolons — first clause is usually the product name
    parts = text.split(';')
    text = parts[0].strip()
    
    # Remove font/typography specs
    text = re.sub(r',?\s*(Arial|Helvetica|Times|Courier|font\s*size|[0-9]+\s*pt|[0-9]+\s*font)\b[^,;]*', '', text, flags=re.IGNORECASE)
    
    # Remove material/finish specs that come after the noun
    text = re.sub(r',?\s*(magnetic|rounded corners?|matte|glossy|laminated|self.?adhesive)\b[^,;]*', '', text, flags=re.IGNORECASE)
    
    # Clean up trailing commas, spaces
    text = re.sub(r'[,\s]+$', '', text)
    text = re.sub(r'^[,\s]+', '', text)
    
    return text.strip() if text.strip() else raw.strip()

# ─── Reytech Company Info (fills supplier section) ──────────────────────────
# Loaded from reytech_config.json — single source of truth for company data.

def _build_reytech_info():
    try:
        from src.forms.reytech_filler_v4 import load_config
        co = load_config().get("company", {})
        addr = co.get("address", "30 Carnoustie Way Trabuco Canyon CA 92679")
        # Add commas for form display: "Street, City, ST ZIP"
        if addr and ", " not in addr:
            m = re.match(r'^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$', addr)
            if m:
                addr = f"{m.group(1)}, {m.group(2)} {m.group(3)}"
        return {
            "company_name": co.get("name", "Reytech Inc."),
            "representative": co.get("owner", "Michael Guadan"),
            "address": addr,
            "phone": co.get("phone", "949-229-1575"),
            "email": co.get("email", "sales@reytechinc.com"),
            "sb_mb": co.get("cert_number", "2002605"),
            "dvbe": co.get("cert_number", "2002605"),
            "discount": "Included",
            "delivery": "5-7 business days",
        }
    except Exception:
        return {
            "company_name": "Reytech Inc.",
            "representative": "Michael Guadan",
            "address": "30 Carnoustie Way, Trabuco Canyon, CA 92679",
            "phone": "949-229-1575",
            "email": "sales@reytechinc.com",
            "sb_mb": "2002605",
            "dvbe": "2002605",
            "discount": "Included",
            "delivery": "5-7 business days",
        }

REYTECH_INFO = _build_reytech_info()


# ─── AMS 704 Field Name Patterns ────────────────────────────────────────────

# Row fields follow pattern: "FIELD NAMERow{N}" where N = 1-8 per page
# Canonical source: ams704_helpers.ROW_FIELD_TEMPLATES_704A
from src.forms.ams704_helpers import ROW_FIELD_TEMPLATES_704A as ROW_FIELDS

HEADER_FIELDS = {
    "price_check_number": "Text1",
    "due_date": "Text2",
    "due_time": "Time",
    "am_pst": "Check Box2",
    "pm_pst": "Check Box7",
    "requestor": "Requestor",
    "institution": "Institution or HQ Program",
    "zip_code": "Delivery Zip Code",
    "phone": "Phone Number",
    "date_of_request": "Date of Request",
    "ship_to": "Ship to",
}

SUPPLIER_FIELDS = {
    "company_name": "COMPANY NAME",
    "representative": "COMPANY REPRESENTATIVE print name",
    "delivery_aro": "Delivery Date and Time ARO",
    "address": "Address",
    "discount": "Discount Offered",
    "sb_mb": "Certified SBMB",
    "dvbe": "Certified DVBE",
    "phone": "Phone Number_2",
    "email": "EMail Address",
    "expires": "Date Price Check Expires",
}

TOTAL_FIELDS = {
    "subtotal": "fill_70",
    "freight": "fill_71",
    "tax": "fill_72",
    "total": "fill_73",
    "notes": "Supplier andor Requestor Notes",
    "fob_prepaid": "Check Box4",
    "fob_ppadd": "Check Box8",
    "fob_collect": "Check Box10",
}

MAX_ROWS_PER_PAGE = 8


# ── Part Number Extraction ────────────────────────────────────────────────
# The 704 "ITEM" field is just a sequential row number (1, 2, 3).
# Real part/MFG/reference numbers appear in:
#   1. SUBSTITUTED ITEM column ("Include manufacturer, part number, and/or reference number")
#   2. Embedded in the DESCRIPTION field (e.g. "MFG#: ABC-123" or "Item #12345")
#   3. Sometimes the ITEM field itself has a real part number (alphanumeric, not just digits)

_PN_PATTERNS = [
    # Explicit labeled patterns
    re.compile(r'(?:MFG|Mfg)[\s.#:]*\s*([A-Z0-9][A-Z0-9\-\.\/]{2,25})', re.IGNORECASE),
    re.compile(r'(?:Part|P/N|PN)[\s.#:]*\s*([A-Z0-9][A-Z0-9\-\.\/]{2,25})', re.IGNORECASE),
    re.compile(r'(?:Item|Catalog|Cat)[\s.#:]+\s*([A-Z0-9][A-Z0-9\-\.\/]{3,25})', re.IGNORECASE),
    re.compile(r'(?:SKU|Model|MDL)[\s.#:]*\s*([A-Z0-9][A-Z0-9\-\.\/]{2,25})', re.IGNORECASE),
    re.compile(r'(?:Ref|Reference)[\s.#:]*\s*([A-Z0-9][A-Z0-9\-\.\/]{2,25})', re.IGNORECASE),
    # Dash-separated codes: ABC-12345, 5110-00-079-3230 (NSN format)
    re.compile(r'\b(\d{4,5}[\-]\d{2,5}[\-]\d{2,5}[\-]\d{2,5})\b'),
    re.compile(r'\b([A-Z]{1,4}[\-][A-Z0-9\-]{3,20})\b'),
    re.compile(r'\b(\d{4,5}[\-]\d{2,5}[\-]?\d{0,5})\b'),
    # Alphanumeric codes: ABC1234, AB-12.34
    re.compile(r'\b([A-Z][A-Z0-9]{2,}[\-\.][A-Z0-9]{1,10})\b'),
    re.compile(r'\b([A-Z]{2,4}\d{3,8})\b'),
    # Single letter + digits: W12919, W9235 (S&S Worldwide format)
    re.compile(r'\b([A-Z]\d{4,6})\b'),
    # Trailing code after " - ": "JUMBO JACKS - W14100" → W14100
    re.compile(r'[\-–]\s*([A-Z0-9][A-Z0-9\-]{2,15})\s*$'),
    # Pure numeric codes at end: "EASY PACK - 16753" → 16753 (5+ digits)
    re.compile(r'[\-–]\s*(\d{5,8})\s*$'),
]

_PN_SKIP = {
    'ea', 'each', 'box', 'bx', 'case', 'cs', 'pk', 'pack', 'bag',
    'roll', 'rl', 'dz', 'dozen', 'pr', 'pair', 'set',
    'n/a', 'na', 'none', 'tbd', 'see', 'per', 'uom', 'row',
}


def _extract_part_number(text: str) -> str:
    """Extract a MFG/part/reference number from text. Returns best candidate or ''."""
    if not text or not text.strip():
        return ""
    text = text.strip()
    for pat in _PN_PATTERNS:
        m = pat.search(text)
        if m:
            candidate = m.group(1).strip().rstrip('.')
            if candidate.lower() in _PN_SKIP or len(candidate) < 3:
                continue
            has_letter = any(c.isalpha() for c in candidate)
            has_digit = any(c.isdigit() for c in candidate)
            has_dash = '-' in candidate
            if (has_letter and has_digit) or (has_dash and has_digit and len(candidate) >= 5) or (has_digit and len(candidate) >= 5 and not has_letter):
                return candidate
    return ""


def _is_sequential_number(val: str) -> bool:
    """Check if value is just a sequential row number (1-50), not a real part number."""
    v = val.strip()
    if not v:
        return True
    try:
        return 0 < int(float(v)) <= 50
    except (ValueError, TypeError):
        return False


def extract_item_numbers(item: dict) -> str:
    """
    Extract the best MFG/part/reference number for a line item.
    Checks: substituted field → description → item_number field.
    Returns the number or empty string.
    """
    # 1. Check substituted item field (most likely source on 704s)
    sub = (item.get("substituted") or "").strip()
    if sub:
        pn = _extract_part_number(sub)
        if pn:
            return pn
        # If the whole substituted field looks like a part number itself
        if len(sub) >= 3 and not sub.lower().startswith(("see ", "per ", "n/a")):
            clean = sub.strip().split('\n')[0].strip()
            if len(clean) <= 30 and any(c.isdigit() for c in clean):
                return clean

    # 2. Check description for embedded part numbers
    desc = (item.get("description_raw") or item.get("description") or "").strip()
    if desc:
        pn = _extract_part_number(desc)
        if pn:
            return pn

    # 3. Check item_number field if it's not just a sequential row number
    item_num = (item.get("item_number") or "").strip()
    if item_num and not _is_sequential_number(item_num):
        return item_num

    return ""


# ─── Parse-time sanitizer ─────────────────────────────────────────────────────

def _sanitize_parsed_items(items: list) -> list:
    """Normalize types on freshly-parsed items to prevent downstream crashes.
    Ensures qty is int, uom is str, description is str, row_index is int."""
    for item in items:
        # qty: must be int >= 1
        raw_qty = item.get("qty", 1)
        try:
            item["qty"] = max(1, int(float(raw_qty))) if raw_qty else 1
        except (ValueError, TypeError):
            item["qty"] = 1
        # uom: must be str, uppercased
        item["uom"] = str(item.get("uom") or "EA").strip().upper()
        # description: must be str, capped at 5000
        desc = item.get("description") or ""
        item["description"] = str(desc)[:5000]
        # row_index: must be int >= 1
        try:
            item["row_index"] = max(1, int(item.get("row_index", 1)))
        except (ValueError, TypeError):
            item["row_index"] = 1
        # qty_per_uom: must be int >= 1
        try:
            item["qty_per_uom"] = max(1, int(item.get("qty_per_uom", 1)))
        except (ValueError, TypeError):
            item["qty_per_uom"] = 1
        # Ensure pricing dict exists
        if "pricing" not in item:
            item["pricing"] = {}
    return items


# ─── Junk item filter ────────────────────────────────────────────────────────

def _filter_junk_items(items: list) -> list:
    """Remove parsed items that are clearly not real line items.
    Government PDFs contain legal text, definitions, instructions,
    and boilerplate that parsers mistakenly extract as items."""

    # Phrases that indicate legal/instruction text, not product descriptions
    JUNK_PHRASES = [
        "i certify under penalty",
        "penalty of perjury",
        "laws of the state",
        "postconsumer recycled",
        "recycled-content material",
        "product category refers to",
        "categories listed below",
        "does not belong in any",
        "enter n/a",
        "common n/a",
        "reportable purchase",
        "instructions last page",
        "see instructions",
        "data entry notes",
        "enter the model",
        "complete this field",
        "qty per uom",
        "substituted item",
        "item description, noun first",
        "include manufacturer",
        "used for informational purposes",
        "it is not an order",
        "enter grand total",
        "front page",
        "fob destination",
        "fob origin",
        "freight prepaid",
        "freight collect",
        "merchandise subtotal",
        "total price",
        "supplier and/or requestor",
        "price check worksheet",
        "ams 704",
        "state of california",
        "correctional health care",
        "non-negotiable",
        "payment terms",
        "signature and date",
        "company representative",
        "certified sb/mb",
        "certified dvbe",
        "delivery date and time",
        "discount offered",
        "price check expires",
        "this document is used for",
        "all or none",
        "(rev 1/2019)",
        "supplier information",
        "company name",
        "bid submission",
        "terms and conditions",
        "pursuant to",
        "in accordance with",
        "hereby certify",
        "authorized signature",
        # CalRecycle 74 form content
        "reused or refurbished products",
        "minimum content requirement",
        "pcc 12209",
        "percent postconsumer",
        "percent by weight",
        "postconsumer material",
        "calrecycle",
        "recycled content certification",
        "recycled-content certification",
        "product categories, enter",
        "product category",
        "commercially useful function",
        # Other government form boilerplate
        "bidder declaration",
        "iran contracting act",
        "darfur contracting act",
        "disabled veteran business",
        "nondiscrimination clause",
        "small business preference",
        "drug-free workplace",
        "conflict of interest",
        "general provisions",
        "instructions to bidders",
        "contractor certification",
    ]

    filtered = []
    removed = 0
    for item in items:
        desc = (item.get("description") or "").strip().lower()
        # Strip leading punctuation/whitespace (parsed fragments often start with ", ")
        desc = re.sub(r'^[\s,;:\.\-]+', '', desc).strip()

        # Skip items with no description
        if not desc or len(desc) < 3:
            removed += 1
            continue

        # Skip items where description matches junk phrases
        is_junk = False
        for phrase in JUNK_PHRASES:
            if phrase in desc:
                is_junk = True
                break

        if is_junk:
            removed += 1
            continue

        # Skip items that are just reference numbers like "(2), (3) and (b)(1)"
        # Pattern: mostly parentheses, commas, numbers, "and", "or"
        stripped = re.sub(r'[\(\)\,\.\d\s]', '', desc)
        stripped = stripped.replace('and', '').replace('or', '').strip()
        if len(stripped) < 4 and len(desc) > 3:
            removed += 1
            continue

        # Skip items where description is clearly a sentence/paragraph (>150 chars)
        # with no product-like content (no numbers, no brand names)
        if len(desc) > 150:
            has_number = bool(re.search(r'\d{3,}', desc))  # 3+ digit number (UPC, part#)
            has_dash_number = bool(re.search(r'\w+-\w+', desc))  # part-number pattern
            if not has_number and not has_dash_number:
                removed += 1
                continue

        # Skip items that start with a quote and look like definitions
        if desc.startswith('"') or desc.startswith("'") or desc.startswith('\u201c'):
            if len(desc) > 80:
                removed += 1
                continue

        filtered.append(item)

    if removed:
        log.info("Junk filter: removed %d/%d items, kept %d", removed, len(items), len(filtered))

    return filtered


# ─── Parse items from email body text ─────────────────────────────────────────

def parse_items_from_email_body(body_text: str) -> dict:
    """Parse line items from email body text when no 704 PDF is attached.

    Handles tabular data like CalVet RFQs:
    LINE NO.  QTY/UNIT  U OF M  PART #  DESCRIPTION
    1         20        CS      MCK-123 BANDAGE ELASTIC 6"
    """
    if not body_text or len(body_text) < 50:
        return {"line_items": [], "header": {}}

    lines = body_text.split("\n")
    header = {}

    # ── Extract header info ──
    for line in lines:
        m = re.search(r'request(?:or|er)[:\s]+([A-Z][A-Za-z\s]+?)(?:\s+\d|$)', line)
        if m and not header.get("requestor"):
            header["requestor"] = m.group(1).strip()

        m = re.search(r'due\s*(?:date|day)?[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})', line, re.IGNORECASE)
        if m and not header.get("due_date"):
            header["due_date"] = m.group(1)

        for pat in [r"department[:\s]+(.+?)(?:\s{2,}|$)",
                    r"veteran'?s?\s+(?:home|affairs)\s*[-\u2013\u2014]?\s*(\w+)",
                    r"(cal\s*vet\b.+?)(?:\s{2,}|$)"]:
            m = re.search(pat, line, re.IGNORECASE)
            if m and not header.get("institution"):
                header["institution"] = m.group(1).strip()

        m = re.search(r'(?:requisition|solicitation|rfq)\s*#?\s*(\d+)', line, re.IGNORECASE)
        if m and not header.get("solicitation_number"):
            header["solicitation_number"] = m.group(1).strip()

        m = re.search(r'deliver(?:y|ed)\s+to\s+(.+?)(?:\.\s|$)', line, re.IGNORECASE)
        if m and not header.get("ship_to"):
            header["ship_to"] = m.group(1).strip()

    # ── Extract line items ──
    # Pattern: LINE_NO  QTY  UOM  PART#  DESCRIPTION
    item_pat = re.compile(
        r'^\s*(\d{1,3})\s+'
        r'(\d{1,6})\s+'
        r'([A-Z]{1,5})\s+'
        r'([\w\-\.#]+(?:\s[\w\-\.#]+)?)\s+'
        r'(.+?)\s*$',
        re.IGNORECASE
    )
    # Simpler: LINE_NO  QTY  DESCRIPTION (no UOM/part#)
    simple_pat = re.compile(r'^\s*(\d{1,3})\s+(\d{1,6})\s+(.{10,})\s*$')

    items = []
    skip_headers = {"LINE NO", "QTY/UNIT", "PART #", "DESCRIPTION", "ITEM #", "U OF M"}

    for line in lines:
        line = line.strip()
        if not line or len(line) < 5:
            continue
        if any(h in line.upper() for h in skip_headers):
            continue

        m = item_pat.match(line)
        if m:
            items.append({
                "item_number": m.group(1),
                "qty": int(m.group(2)),
                "uom": m.group(3).upper(),
                "mfg_number": m.group(4).strip(),
                "description": m.group(5).strip(),
                "row_index": int(m.group(1)),
                "pricing": {},
                "source": "email_body",
            })
            continue

        if items:
            m = simple_pat.match(line)
            if m:
                items.append({
                    "item_number": m.group(1),
                    "qty": int(m.group(2)),
                    "uom": "EA",
                    "mfg_number": "",
                    "description": m.group(3).strip(),
                    "row_index": int(m.group(1)),
                    "pricing": {},
                    "source": "email_body",
                })

    if items:
        items = _filter_junk_items(_sanitize_parsed_items(items))

    log.info("parse_email_body: found %d items, header=%s", len(items),
             {k: v for k, v in header.items() if v})
    return {"line_items": items, "header": header}


# ─── Pre-generation validation ────────────────────────────────────────────────

def validate_against_source(priced_items: list, source_items: list) -> dict:
    """Compare priced items against original parsed items to catch mismatches.

    Returns dict with ok, warnings, errors, missing_items, qty_mismatches.
    """
    result = {
        "ok": True,
        "warnings": [],
        "errors": [],
        "missing_items": [],
        "qty_mismatches": [],
    }

    if not source_items:
        result["warnings"].append("No source items to compare against")
        return result

    def _normalize(s):
        if not s:
            return ""
        s = s.lower().strip()
        s = re.sub(r'[^a-z0-9\s]', '', s)
        return re.sub(r'\s+', ' ', s)

    def _similarity(a, b):
        wa = set(_normalize(a).split())
        wb = set(_normalize(b).split())
        if not wa or not wb:
            return 0
        return len(wa & wb) / max(len(wa), len(wb))

    matched_source = set()

    for pi, priced in enumerate(priced_items):
        if priced.get("no_bid"):
            continue
        p_desc = priced.get("description", "")
        p_qty = priced.get("qty", 1)
        p_mfg = _normalize(priced.get("mfg_number", ""))

        best_idx, best_score = -1, 0
        for si, source in enumerate(source_items):
            if si in matched_source:
                continue
            s_desc = source.get("description", "")
            s_mfg = _normalize(source.get("mfg_number", ""))
            score = _similarity(p_desc, s_desc)
            if p_mfg and s_mfg and p_mfg == s_mfg:
                score = max(score, 0.8)
            if score > best_score:
                best_score = score
                best_idx = si

        if best_idx >= 0 and best_score >= 0.4:
            matched_source.add(best_idx)
            s_item = source_items[best_idx]
            s_qty = s_item.get("qty", 1)
            if s_qty and p_qty and s_qty != p_qty:
                result["qty_mismatches"].append({
                    "item": pi + 1,
                    "desc": p_desc[:50],
                    "priced_qty": p_qty,
                    "source_qty": s_qty,
                })
                result["warnings"].append(
                    f"Item #{pi+1} qty mismatch: priced={p_qty}, source={s_qty} ({p_desc[:40]})"
                )

    # Check for source items not matched
    for si, source in enumerate(source_items):
        if si not in matched_source:
            s_desc = source.get("description", "")
            if s_desc and len(s_desc) > 3:
                result["missing_items"].append({
                    "source_idx": si + 1,
                    "desc": s_desc[:50],
                })
                result["warnings"].append(
                    f"Source item #{si+1} not found in priced items: {s_desc[:40]}"
                )

    if result["qty_mismatches"] or len(result["missing_items"]) > len(source_items) * 0.3:
        result["ok"] = False
        result["errors"].append(
            f"{len(result['qty_mismatches'])} qty mismatches, "
            f"{len(result['missing_items'])} missing items"
        )

    log.info("validate_against_source: ok=%s, warnings=%d, mismatches=%d, missing=%d",
             result["ok"], len(result["warnings"]),
             len(result["qty_mismatches"]), len(result["missing_items"]))
    return result


# ─── Source validation ─────────────────────────────────────────────────────

def validate_source_email(rfq_data: dict) -> dict:
    """Cross-reference original email against RFQ system data."""
    result = {"ok": True, "warnings": [], "errors": [], "checks": []}
    email_body = rfq_data.get("body_text", "") or rfq_data.get("email_body", "") or ""
    email_subject = rfq_data.get("email_subject", "") or ""
    all_text = f"{email_subject} {email_body}"
    if not all_text or len(all_text) < 20:
        result["warnings"].append("No email body available for cross-reference")
        return result

    items = rfq_data.get("line_items", rfq_data.get("items", []))

    # Item count
    item_numbers = re.findall(r'(?:^|\n)\s*(\d{1,3})\s+\d', all_text)
    if item_numbers:
        max_item = max(int(n) for n in item_numbers)
        if max_item != len(items):
            result["warnings"].append(f"Email has ~{max_item} items, system has {len(items)}")
        else:
            result["checks"].append(f"Item count matches: {len(items)}")

    # Due date
    system_due = rfq_data.get("due_date", "")
    for pat in [r'(?:due|deadline)[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})',
                r'(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:08:00|8:00|AM|PM)']:
        m = re.search(pat, all_text, re.IGNORECASE)
        if m:
            email_due = m.group(1)
            if system_due and email_due not in system_due and system_due not in email_due:
                result["warnings"].append(f"Due date mismatch — email: {email_due}, system: {system_due}")
            else:
                result["checks"].append(f"Due date matches: {system_due or email_due}")
            break

    # Solicitation number
    system_sol = rfq_data.get("solicitation_number", "") or rfq_data.get("rfq_number", "")
    m = re.search(r'(?:solicitation|requisition|rfq)\s*#?\s*(\d+)', all_text, re.IGNORECASE)
    if m:
        email_sol = m.group(1)
        if system_sol and email_sol != system_sol:
            result["errors"].append(f"Solicitation # mismatch — email: {email_sol}, system: {system_sol}")
            result["ok"] = False
        else:
            result["checks"].append(f"Solicitation # matches: {system_sol or email_sol}")

    # Delivery address
    system_delivery = rfq_data.get("delivery_location", "") or rfq_data.get("ship_to", "")
    m = re.search(r'deliver(?:y|ed)\s+to\s+(.+?)(?:\.|$)', all_text, re.IGNORECASE)
    if m:
        email_addr = m.group(1).strip()[:100]
        if system_delivery:
            overlap = len(set(email_addr.lower().split()) & set(system_delivery.lower().split()))
            if overlap < 2 and len(email_addr.split()) > 2:
                result["warnings"].append(f"Delivery may differ — email: '{email_addr[:50]}', system: '{system_delivery[:50]}'")
            else:
                result["checks"].append("Delivery address matches")

    total = len(result["errors"]) + len(result["warnings"])
    result["summary"] = f"✅ Passed ({len(result['checks'])} checks)" if total == 0 else f"⚠️ {total} issue(s)"
    return result


# ─── Field audit ──────────────────────────────────────────────────────────────

def audit_generated_form(pdf_path: str, form_id: str, expected_values: dict = None) -> dict:
    """Read a generated PDF and verify its fields match expectations."""
    result = {"ok": True, "checks": [], "warnings": [], "errors": [], "fields": {}}

    if not os.path.exists(pdf_path):
        result["errors"].append(f"File not found: {pdf_path}")
        result["ok"] = False
        return result

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        fields = reader.get_fields() or {}
        result["page_count"] = len(reader.pages)
        result["field_count"] = len(fields)

        for fname, fdata in fields.items():
            val = str(fdata.get("/V", "")).strip() if isinstance(fdata, dict) else ""
            if val and val != "/Off":
                result["fields"][fname] = val

        filled = len(result["fields"])
        result["checks"].append(f"{filled}/{len(fields)} fields filled")

        if form_id == "bidder_decl":
            _check_field(result, "Solicitaion #", "Solicitation number")
            _check_filled(result, "page", "Page number")
        elif form_id == "darfur_act":
            _check_field(result, "CompanyVendor Name", "Company name")
            _check_field(result, "Federal ID Number", "FEIN")
            _check_filled(result, "Date of signature", "Signature date")
        elif form_id == "704b":
            _check_field(result, "COMPANY NAME", "Company name")
            buyer_fields = ["DEPARTMENT", "PHONEEMAIL", "REQUESTOR", "SOLICITATION #"]
            for bf in buyer_fields:
                if bf in result["fields"] and expected_values:
                    if expected_values.get("company_name", "").lower() in result["fields"].get(bf, "").lower():
                        result["errors"].append(f"BUYER FIELD OVERWRITTEN: {bf}")
                        result["ok"] = False
        elif form_id == "quote":
            text = ""
            for page in reader.pages[:3]:
                text += page.extract_text() or ""
            if "$" not in text:
                result["warnings"].append("Quote may not contain prices")
            else:
                result["checks"].append("Quote contains pricing")
        elif form_id == "sellers_permit":
            result["checks"].append("Static copy — no fields to verify")
        else:
            if filled == 0:
                result["errors"].append(f"No fields filled in {form_id}")
                result["ok"] = False
            elif filled < 3:
                result["warnings"].append(f"Only {filled} fields filled")
            else:
                result["checks"].append(f"{filled} fields filled")
    except Exception as e:
        result["errors"].append(f"Failed to read PDF: {e}")
        result["ok"] = False

    return result


def _check_field(result, field_name, label):
    val = result["fields"].get(field_name, "")
    if val:
        result["checks"].append(f"{label}: {val[:40]}")
    else:
        result["warnings"].append(f"{label} is empty")


def _check_filled(result, field_name, label):
    val = result["fields"].get(field_name, "")
    if val:
        result["checks"].append(f"{label}: filled")
    else:
        result["warnings"].append(f"{label} is empty")


# ─── Parse Quality Validation ────────────────────────────────────────────────

def validate_parse_quality(pdf_path: str, parsed_items: list) -> dict:
    """Validate parsed items against the source PDF to catch parse errors.

    Uses pdfplumber to count data rows in tables and compares against parsed
    item count. Also checks for doubled descriptions, empty fields, etc.

    Returns:
        {"score": 0-100, "grade": "A/B/C/F", "expected_items": N,
         "parsed_items": N, "warnings": [...]}
    """
    result = {
        "score": 100,
        "grade": "A",
        "expected_items": 0,
        "parsed_items": len(parsed_items),
        "warnings": [],
    }

    if not parsed_items:
        result["score"] = 0
        result["grade"] = "F"
        result["warnings"].append("No items parsed from PDF")
        return result

    # ── Count expected data rows from the PDF tables ──
    pdf_data_rows = 0
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if not row:
                            continue
                        # A data row has a numeric value in one of the first 3 cells
                        # (item#, qty, or qty_per_uom) AND text in a later cell (description)
                        cells = [str(c or "").strip() for c in row]
                        has_number = any(
                            c.isdigit() and 0 < int(c) <= 999
                            for c in cells[:4] if c
                        )
                        has_text = any(
                            len(c) > 10 and not c.upper().startswith(("ITEM", "QTY", "UNIT OF", "PRICE", "EXTENSION", "SUBSTITUT"))
                            for c in cells[3:] if c
                        )
                        if has_number and has_text:
                            pdf_data_rows += 1
    except ImportError:
        log.debug("validate_parse_quality: pdfplumber not available")
    except Exception as e:
        log.debug("validate_parse_quality: table count failed: %s", e)

    result["expected_items"] = pdf_data_rows
    penalty = 0

    # ── Item count mismatch ──
    if pdf_data_rows > 0:
        diff = abs(pdf_data_rows - len(parsed_items))
        if diff == 0:
            pass  # perfect
        elif diff <= 2:
            penalty += 15
            result["warnings"].append(
                f"Item count mismatch: PDF has ~{pdf_data_rows} rows, parsed {len(parsed_items)}"
            )
        else:
            penalty += 30 + min(diff * 5, 30)
            result["warnings"].append(
                f"Major item count mismatch: PDF has ~{pdf_data_rows} rows, parsed {len(parsed_items)}"
            )

    # ── Check each parsed item for quality issues ──
    for i, item in enumerate(parsed_items):
        desc = (item.get("description") or "").strip()
        raw = (item.get("description_raw") or desc).strip()
        label = f"Item {item.get('item_number', i+1)}"

        # Doubled description: same phrase (5+ words) appears twice
        if desc and len(desc) > 20:
            words = desc.split()
            if len(words) >= 10:
                half = len(words) // 2
                first_half = " ".join(words[:half]).lower()
                second_half = " ".join(words[half:]).lower()
                # Check if the two halves are very similar (>70% overlap)
                w1 = set(first_half.split())
                w2 = set(second_half.split())
                if w1 and w2:
                    overlap = len(w1 & w2) / max(len(w1), len(w2))
                    if overlap > 0.7:
                        penalty += 10
                        result["warnings"].append(f"{label}: possible doubled description")

        # Very short description
        if len(desc) < 5:
            penalty += 5
            result["warnings"].append(f"{label}: very short description ({len(desc)} chars)")

        # Missing qty
        if not item.get("qty") or item.get("qty", 0) <= 0:
            penalty += 5
            result["warnings"].append(f"{label}: missing or zero quantity")

    # ── Compute final score and grade ──
    result["score"] = max(0, 100 - penalty)
    if result["score"] >= 90:
        result["grade"] = "A"
    elif result["score"] >= 70:
        result["grade"] = "B"
    elif result["score"] >= 50:
        result["grade"] = "C"
    else:
        result["grade"] = "F"

    log.info("validate_parse_quality: score=%d grade=%s expected=%d parsed=%d warnings=%d",
             result["score"], result["grade"], pdf_data_rows, len(parsed_items),
             len(result["warnings"]))
    return result


# ─── Parse AMS 704 ──────────────────────────────────────────────────────────

def parse_ams704(pdf_path: str) -> dict:
    """
    Parse an AMS 704 Price Check PDF and extract all data.

    Returns:
        {
            "header": {field: value, ...},
            "line_items": [
                {"item_number": str, "qty": int, "uom": str, "qty_per_uom": int,
                 "description": str, "row_index": int},
                ...
            ],
            "existing_prices": {row_index: float, ...},
            "ship_to": str,
            "source_pdf": str,
            "field_count": int,
            "parse_method": "fillable" | "ocr",
        }
    """
    if not HAS_PYPDF:
        return {"error": "pypdf not available"}

    result = {
        "header": {},
        "line_items": [],
        "existing_prices": {},
        "ship_to": "",
        "source_pdf": pdf_path,
        "field_count": 0,
        "parse_method": "fillable",
    }

    if not os.path.exists(pdf_path):
        result["error"] = f"File not found: {os.path.basename(pdf_path)}"
        return result

    try:
        reader = PdfReader(pdf_path)
    except Exception as e:
        log.warning("parse_ams704: corrupt/unreadable PDF %s: %s", os.path.basename(pdf_path), e)
        result["error"] = f"Cannot read PDF: {e}"
        return result

    try:
        fields = reader.get_fields()
    except Exception as e:
        log.warning("parse_ams704: failed to extract fields from %s: %s", os.path.basename(pdf_path), e)
        result["parse_method"] = "ocr"
        return _parse_ams704_ocr(pdf_path, result)

    if not fields:
        # Try OCR fallback
        result["parse_method"] = "ocr"
        return _parse_ams704_ocr(pdf_path, result)

    result["field_count"] = len(fields)

    # Log all field names for debugging (helps identify naming patterns)
    log.info("parse_ams704 %s: %d fields found", os.path.basename(pdf_path), len(fields))
    field_names = sorted(fields.keys())
    for fn in field_names[:40]:
        fval = fields[fn].get("/V", "") if isinstance(fields[fn], dict) else ""
        if fval:
            log.info("  field '%s' = '%s'", fn, str(fval)[:60])

    # Extract header
    for key, field_name in HEADER_FIELDS.items():
        field = fields.get(field_name, {})
        val = field.get("/V", "") if isinstance(field, dict) else ""
        if val:
            val = str(val).strip()
            # Strip checkbox values
            if val in ("/Yes", "/Off"):
                val = val == "/Yes"
        result["header"][key] = val

    result["ship_to"] = str(fields.get("Ship to", {}).get("/V", "")).strip()

    # Extract line items — check rows 1-24 to support multi-page 704s
    # (Standard 704 has 8 rows per page, up to 3 pages = 24 items)
    max_row_check = 50
    
    # Build a field lookup that handles naming variants
    # Some 704s use "SUBSTITUTED ITEM..." while others use "REPLACEMENT..." etc.
    field_map_cache = {}
    def _find_field(pattern_key, row_n):
        """Find the best matching field for a given key/row combo."""
        cache_key = f"{pattern_key}_{row_n}"
        if cache_key in field_map_cache:
            return field_map_cache[cache_key]
        # Try exact match first
        exact = ROW_FIELDS[pattern_key].format(n=row_n)
        if exact in fields:
            field_map_cache[cache_key] = exact
            return exact
        # Fuzzy match: look for fields containing key words + row number
        key_words = {
            "substituted": ["substitut", "replacement", "alternate"],
            "description": ["description", "item desc"],
            "qty": ["qty", "quantity"],
            "uom": ["uom", "unit of measure", "unit measure"],
            "unit_price": ["price per", "unit price"],
            "extension": ["extension", "ext"],
            "item_number": ["item row", "item #"],
            "qty_per_uom": ["qty per"],
        }
        search_terms = key_words.get(pattern_key, [pattern_key])
        # Exclusion patterns: prevent "QTY PER UOM" matching as "QTY" etc.
        exclude_words = {
            "qty": ["per uom", "qty per", "per unit"],
            "uom": ["qty per uom", "qty per"],
        }
        excludes = exclude_words.get(pattern_key, [])
        row_str = str(row_n)
        for fname in fields:
            fl = fname.lower()
            if row_str in fname and any(t in fl for t in search_terms):
                if any(ex in fl for ex in excludes):
                    continue  # Skip: this field belongs to a different column
                field_map_cache[cache_key] = fname
                log.debug("Fuzzy field match: '%s' row %d → '%s'", pattern_key, row_n, fname)
                return fname
        field_map_cache[cache_key] = exact  # Fall back to exact even if not found
        return exact

    # Build list of (row_num, suffix) pairs to check.
    # Standard: Row1..Row50 (page 1)
    # Multi-page: Row1_2..Row8_2 (page 2), Row1_3..Row8_3 (page 3), etc.
    row_checks = [(r, "") for r in range(1, max_row_check + 1)]
    # Detect multi-page _2, _3 suffixed fields (e.g., QTYRow1_2 for page 2)
    for fname in fields:
        for suffix in ("_2", "_3", "_4"):
            if suffix in fname and "Row" in fname:
                # Add page 2/3/4 rows if not already present
                for r in range(1, 9):  # 8 rows per page
                    pair = (r, suffix)
                    if pair not in row_checks:
                        row_checks.append(pair)
                break

    _logical_row = 0
    for row_num, row_suffix in row_checks:
        row_data = {}
        has_data = False

        for key, pattern in ROW_FIELDS.items():
            # Build field name with optional page suffix
            field_name = _find_field(key, row_num)
            if row_suffix:
                field_name = field_name + row_suffix
            field = fields.get(field_name, {})
            val = field.get("/V", "") if isinstance(field, dict) else ""
            val = str(val).strip() if val else ""
            row_data[key] = val
            if val and key in ("description", "qty"):
                has_data = True

        if has_data and row_data.get("description"):
            # Parse qty
            qty = 1
            try:
                qty = int(float(row_data.get("qty", "1") or "1"))
            except (ValueError, TypeError):
                qty = 1

            # Detect continuation rows: has description but NO qty AND NO item#
            # These are multi-line descriptions that spilled into the next row's fields
            # Also detect rows with auto-filled sequential item numbers but no qty
            raw_qty = (row_data.get("qty") or "").strip()
            raw_item_num = (row_data.get("item_number") or "").strip()
            # Treat "0" as effectively empty — some forms auto-fill 0 for blank qty
            qty_is_empty = (not raw_qty or raw_qty == "0")
            is_continuation = (qty_is_empty and not raw_item_num and result["line_items"])
            # Also treat as continuation: no real qty AND item# is just a sequential number
            # (some forms auto-fill item# 2,3,4... even for empty rows)
            if not is_continuation and result["line_items"] and qty_is_empty:
                if not raw_item_num or _is_sequential_number(raw_item_num):
                    is_continuation = True
            # Also treat as continuation: parsed qty is 0 or 1 (default) while
            # the previous item has a real qty > 1 — likely a wrapped description
            if not is_continuation and result["line_items"] and qty <= 1:
                prev_qty = result["line_items"][-1].get("qty", 1)
                if prev_qty > 1 and (_is_sequential_number(raw_item_num) or not raw_item_num):
                    if _is_supplementary_desc(row_data["description"]):
                        is_continuation = True
            # Final fallback: if description is clearly supplementary info (pack size,
            # part number, etc.) it's NEVER a real line item — always merge regardless
            # of qty/item# values (some forms auto-fill these from the previous row)
            if not is_continuation and result["line_items"]:
                if _is_supplementary_desc(row_data["description"]):
                    is_continuation = True
                    log.info("  row %d forced continuation: supplementary desc '%s' (qty=%s item#=%s)",
                             row_num, row_data["description"][:40], raw_qty, raw_item_num)

            if is_continuation:
                prev = result["line_items"][-1]
                prev["description"] = clean_description(
                    prev.get("description_raw", prev["description"]) + " " + row_data["description"]
                )
                prev["description_raw"] = (prev.get("description_raw", "") + " " + row_data["description"]).strip()
                real_pn = extract_item_numbers(prev)
                if real_pn:
                    prev["mfg_number"] = real_pn
                log.info("  row %d merged as continuation into row %d: '%s'",
                         row_num, prev["row_index"], row_data["description"][:40])
                continue

            qty_per_uom = 1
            try:
                qty_per_uom = int(float(row_data.get("qty_per_uom", "1") or "1"))
            except (ValueError, TypeError):
                qty_per_uom = 1

            _logical_row += 1
            # Preserve buyer's original item number from the form field
            _buyer_item_num = (row_data.get("item_number") or "").strip()
            item = {
                "item_number": _buyer_item_num or str(_logical_row),
                "qty": qty,
                "uom": (row_data.get("uom", "ea") or "ea").upper(),
                "qty_per_uom": qty_per_uom,
                "description": clean_description(row_data["description"]),
                "description_raw": row_data["description"],
                "substituted": row_data.get("substituted", ""),
                "row_index": _logical_row,
            }

            # Extract real MFG/part number from substituted field, description, etc.
            real_pn = extract_item_numbers(item)
            if real_pn:
                item["mfg_number"] = real_pn
            
            log.info("  parsed row %d: desc='%s' mfg='%s' sub='%s' qty=%d uom=%s",
                     row_num, item["description"][:40], item.get("mfg_number",""),
                     (item.get("substituted",""))[:40], qty, item["uom"])

            result["line_items"].append(item)

            # Check for existing price
            if row_data.get("unit_price"):
                try:
                    price = float(row_data["unit_price"].replace("$", "").replace(",", ""))
                    result["existing_prices"][row_num] = price
                except (ValueError, TypeError):
                    pass

    log.info("parse_ams704: %d items found across rows 1-%d", len(result["line_items"]), max_row_check)
    
    # Post-processing: merge items that look like continuation rows the parser missed
    result["line_items"] = _merge_continuation_items(result["line_items"])

    # ── CRITICAL: If fillable fields existed but yielded 0 items, fall through
    # to text regex + vision chain. This happens with DocuSign-flattened forms
    # that have field definitions but empty values. ──
    if not result["line_items"]:
        log.info("parse_ams704: fillable fields found (%d) but 0 items extracted — trying text/vision fallback",
                 result["field_count"])
        return _parse_ams704_ocr(pdf_path, result)

    # Filter out junk items (legal text, instructions, boilerplate)
    result["line_items"] = _filter_junk_items(_sanitize_parsed_items(result["line_items"]))

    # Sort by buyer's item number to restore logical order.
    # Multi-page 704s have unsuffixed rows (Row1-Row11) parsed first, then
    # _2 suffix rows (Row1_2-Row8_2) parsed after. This can put buyer items
    # out of order (e.g., 11-18, 27-29, 19-26). Sort by numeric item_number.
    def _sort_key(item):
        try:
            return int(float(item.get("item_number", "9999")))
        except (ValueError, TypeError):
            return 9999
    result["line_items"].sort(key=_sort_key)

    # Re-index: only renumber items that had NO buyer-provided item number.
    # If the buyer wrote 11, 12, 13... preserve those exactly.
    # Only re-index when all item_numbers are sequential from 1 (auto-filled by form)
    # and merges removed rows, causing gaps like 1, 3, 5.
    _all_nums = [item.get("item_number", "") for item in result["line_items"]]
    _is_auto_sequential = all(
        n == str(i + 1) for i, n in enumerate(_all_nums) if n
    ) if _all_nums else True
    if _is_auto_sequential:
        # Auto-filled sequential numbers — safe to re-index after merges
        for i, item in enumerate(result["line_items"]):
            item["item_number"] = str(i + 1)
            item["row_index"] = i + 1
    else:
        # Buyer provided custom line numbers — preserve them, only update row_index
        for i, item in enumerate(result["line_items"]):
            item["row_index"] = i + 1

    # Parse quality validation
    result["parse_quality"] = validate_parse_quality(pdf_path, result["line_items"])

    return result


def _is_supplementary_desc(desc: str) -> bool:
    """Check if a description looks like pack/case info or a part number, not a real product."""
    d = desc.strip().upper()
    if not d:
        return True
    # Pack/case/quantity info: "100PCS PER PACK", "12/CASE", "50 PER BOX", "100 CT"
    if re.match(r'^\d+\s*(PCS?|PIECES?|CT|COUNT)\s*(PER|/)\s*', d, re.I):
        return True
    # Ratio formats: "12/CASE", "50/BOX", "24/PKG", "6/PACK"
    if re.match(r'^\d+\s*/\s*(CASE|BOX|PKG|PACK|BAG|CARTON|EACH|EA|BX|CS|PK)', d, re.I):
        return True
    # "PACK OF 100", "BOX OF 12", "CASE OF 24"
    if re.match(r'^(PACK|BOX|CASE|BAG|CARTON|PKG)\s*(OF|:)\s*\d+', d, re.I):
        return True
    # "N PER CASE/BOX/PACK/etc" (no leading PCS)
    if re.match(r'^\d+\s+PER\s+(PACK|CASE|BOX|BAG|CARTON|PKG|EACH|EA)', d, re.I):
        return True
    # Item/part number references: "ITEM#HT-126", "MFG#ABC123", "P/N: XYZ"
    if re.match(r'^(ITEM\s*#|MFG\s*#|P/?N\s*[:# ]|PART\s*#|REF\s*#|CAT\s*#|SKU\s*#|NDC\s*#|UPC\s*#|NSN\s*#)', d, re.I):
        return True
    # Very short descriptions that are just codes: "HT-126/SONGFIR"
    if len(d) < 25 and '/' in d and any(c.isdigit() for c in d) and ' ' not in d.strip():
        return True
    return False


def _merge_continuation_items(items: list) -> list:
    """
    Post-processing pass to merge false line items that are really
    continuation descriptions from the previous item.
    
    Heuristic: if an item has default qty (1), default UOM (EA),
    and its description looks like supplementary info (pack size,
    part number, etc.), merge into the previous real item.
    """
    if len(items) <= 1:
        return items
    
    merged = [items[0]]
    for item in items[1:]:
        prev = merged[-1]
        desc = (item.get("description") or "").strip()
        raw_desc = (item.get("description_raw") or desc).strip()
        
        # ── GUARD: Never merge items that have their own line number ──
        # If the form gave this item a distinct item_number, it's a real line item.
        # Only exception: descriptions that are clearly supplementary (pack info, MFG#).
        item_num = (item.get("item_number") or "").strip()
        has_own_line_num = bool(item_num and item_num.isdigit() and int(item_num) > 0)

        # Candidate for merging if:
        # 1. Default qty (0 or 1) and default/missing UOM — no real data was parsed
        # 2. OR description is clearly supplementary (pack info, part number)
        is_default_qty = (item.get("qty", 1) in (0, 1))
        is_default_uom = (item.get("uom", "EA").upper() in ("EA", ""))
        is_supplement = _is_supplementary_desc(raw_desc)

        should_merge = False
        if is_supplement and not has_own_line_num:
            should_merge = True
        elif has_own_line_num:
            # Item has its own line number — never merge, period.
            should_merge = False
        elif is_default_qty and is_default_uom and prev.get("qty", 1) > 1:
            # Previous item has a real quantity but this one is default — likely continuation
            should_merge = True
        elif item.get("qty", 1) == 0:
            # Zero-qty items are never real line items — always merge
            should_merge = True
        elif is_default_qty and is_default_uom:
            # Default qty + default UOM and no own line number: possible continuation.
            # Check if desc is just more detail text (not a distinct product)
            desc_up = desc.upper()
            has_product_word = any(w in desc_up for w in [
                "GLOVE", "MASK", "BRIEF", "WIPE", "GOWN", "SYRINGE", "BANDAGE",
                "GAUZE", "TAPE", "SOAP", "TOWEL", "PAPER", "BAG", "LINER",
                "SANITIZER", "CATHETER", "NEEDLE", "BOOT", "VEST", "HELMET",
                "GAME", "STACKS", "DOMINO", "CHESS", "JACK", "PUZZLE", "ART",
                "PENCIL", "CRAYON", "PAINT", "CANVAS", "POSTER", "TOTE",
                "PACK", "KIT", "SET", "BOX", "TUB", "STORAGE",
            ])
            if not has_product_word and len(desc) < 40:
                should_merge = True
        elif item.get("qty") == prev.get("qty") and item.get("uom") == prev.get("uom"):
            # Same qty AND same UOM as previous item — could be auto-filled from above row,
            # but also could be two distinct items with same qty. Only merge if description
            # is clearly supplementary (pack info, part number) — NOT just because it's short.
            if is_supplement:
                should_merge = True
        
        if should_merge:
            # Merge description into previous item
            prev_raw = prev.get("description_raw", prev.get("description", ""))
            prev["description_raw"] = (prev_raw + " " + raw_desc).strip()
            prev["description"] = clean_description(prev["description_raw"])
            # Re-extract part number with fuller description
            real_pn = extract_item_numbers(prev)
            if real_pn:
                prev["mfg_number"] = real_pn
            log.info("  post-merge: '%s' into item %s", desc[:40], prev.get("item_number", "?"))
        else:
            merged.append(item)
    
    if len(merged) != len(items):
        log.info("  post-merge: %d items → %d items", len(items), len(merged))
    
    return merged


def _parse_ams704_ocr(pdf_path: str, result: dict) -> dict:
    """
    Fallback: parse non-fillable (DocuSign-flattened) AMS 704 via text extraction.
    
    Strategy:
    1. Try pdfplumber table extraction first (works on some scanned 704s)
    2. Fall back to regex-based text parsing (handles DocuSign flattened forms)
    3. Use Claude's document content as last resort (context window has clean text)
    """
    # Try pdfplumber first
    pdfplumber_worked = False
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                if page_num == 0:
                    _extract_header_from_text(text, result)
                tables = page.extract_tables()
                for table in tables:
                    _extract_items_from_table(table, result, page_num)
                if result["line_items"]:
                    pdfplumber_worked = True
    except ImportError:
        pass
    except Exception as e:
        log.debug("pdfplumber attempt: %s", e)

    if pdfplumber_worked and len(result["line_items"]) >= 3:
        result["line_items"] = _merge_continuation_items(result["line_items"])
        # Filter out junk items (legal text, instructions, boilerplate)
        result["line_items"] = _filter_junk_items(_sanitize_parsed_items(result["line_items"]))
        result["parse_quality"] = validate_parse_quality(pdf_path, result["line_items"])
        return result

    # pdfplumber failed — use regex text parser on pypdf output
    log.info("pdfplumber found %d items — falling back to text parser for %s",
             len(result["line_items"]), os.path.basename(pdf_path))
    result["line_items"] = []  # Reset
    result["parse_method"] = "text_regex"

    try:
        reader = PdfReader(pdf_path)
        all_text = ""
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            all_text += text + "\n"
            if page_num == 0:
                _extract_header_from_text(text, result)

        _extract_items_from_text(all_text, result)
    except Exception as e:
        result["error"] = f"Text parse error: {e}"
        log.error("Text parse error for %s: %s", pdf_path, e, exc_info=True)

    result["line_items"] = _merge_continuation_items(result["line_items"])

    # ── Vision fallback: use when text parser clearly missed items ──
    text_item_count = len(result["line_items"])
    
    # Detect how many items the form SHOULD have by finding highest item number in text
    try:
        all_text_for_count = ""
        for page in PdfReader(pdf_path).pages:
            all_text_for_count += (page.extract_text() or "") + "\n"
        # Look for item numbers in the ITEM # column (standalone numbers 1-50)
        item_nums_found = set()
        for m in re.finditer(r'(?:^|\n)\s*(\d{1,2})\s*(?:\n|$)', all_text_for_count):
            n = int(m.group(1))
            if 1 <= n <= 50:
                item_nums_found.add(n)
        max_item_num = max(item_nums_found) if item_nums_found else 0
        page_count = len(PdfReader(pdf_path).pages)
        expected_min = max(max_item_num, page_count * 6, 3)
    except Exception:
        expected_min = 8
        max_item_num = 0

    missing_items = expected_min - text_item_count
    if missing_items > 0:
        log.info("Text parser got %d items but form has ~%d (max item#=%d, pages=%s) — trying vision",
                 text_item_count, expected_min, max_item_num, 
                 page_count if 'page_count' in dir() else '?')
        try:
            from src.forms.vision_parser import parse_with_vision, is_available
            if is_available():
                vision_result = parse_with_vision(pdf_path)
                if vision_result and len(vision_result.get("line_items", [])) > text_item_count:
                    log.info("Vision parser got %d items (vs %d from text) — using vision result",
                             len(vision_result["line_items"]), text_item_count)
                    # Filter out junk items (legal text, instructions, boilerplate)
                    vision_result["line_items"] = _filter_junk_items(_sanitize_parsed_items(vision_result.get("line_items", [])))
                    vision_result["parse_quality"] = validate_parse_quality(pdf_path, vision_result["line_items"])
                    return vision_result
                else:
                    log.info("Vision parser got %d items — keeping text result (%d items)",
                             len(vision_result.get("line_items", [])) if vision_result else 0,
                             text_item_count)
            else:
                log.debug("Vision parser not available (no API key) — using text result")
        except Exception as _ve:
            log.debug("Vision fallback: %s", _ve)

    # Filter out junk items (legal text, instructions, boilerplate)
    result["line_items"] = _filter_junk_items(_sanitize_parsed_items(result["line_items"]))
    result["parse_quality"] = validate_parse_quality(pdf_path, result["line_items"])
    return result


def parse_multi_pc(pdf_path: str) -> list:
    """Parse a combined DocuSign AMS 704 PDF containing multiple price checks.
    Detects PC boundaries by looking for new header blocks.
    Returns list of parsed PC dicts with page_start/page_end."""
    import re as _re

    if not os.path.exists(pdf_path):
        return []

    results = []
    try:
        try:
            import pdfplumber
        except ImportError:
            log.warning("parse_multi_pc: pdfplumber not available, falling back to single parse")
            result = parse_ams704(pdf_path)
            result["page_start"] = 0
            result["page_end"] = 0
            return [result]

        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            log.info("parse_multi_pc: %d pages in %s", total_pages, os.path.basename(pdf_path))

            page_texts = []
            for page in pdf.pages:
                page_texts.append(page.extract_text() or "")

            BOUNDARY_PATTERNS = [
                _re.compile(r'Institution or HQ Program', _re.IGNORECASE),
                _re.compile(r'PRICE\s+CHECK\s+#', _re.IGNORECASE),
                _re.compile(r'AMS\s*704', _re.IGNORECASE),
            ]

            boundary_pages = []
            for i, text in enumerate(page_texts):
                hit_count = sum(1 for pat in BOUNDARY_PATTERNS if pat.search(text))
                if hit_count >= 2:
                    boundary_pages.append(i)

            if not boundary_pages:
                log.info("parse_multi_pc: no boundaries found, treating as single PC")
                result = parse_ams704(pdf_path)
                result["page_start"] = 0
                result["page_end"] = total_pages - 1
                return [result]

            log.info("parse_multi_pc: found %d PC boundaries at pages %s",
                     len(boundary_pages), boundary_pages)

            # ── Trim purchase justification pages from section ends ──
            # If the page immediately before a boundary is a "PURCHASE JUSTIFICATION"
            # page (not an AMS 704 page), shrink the previous section's range.
            _PURCHASE_JUST = _re.compile(r'PURCHASE\s+JUSTIFICATION', _re.IGNORECASE)
            _trimmed_pages = set()
            for bi in range(1, len(boundary_pages)):
                prev_page = boundary_pages[bi] - 1
                if prev_page > boundary_pages[bi - 1] and _PURCHASE_JUST.search(page_texts[prev_page]):
                    _trimmed_pages.add(prev_page)
                    log.info("parse_multi_pc: page %d is purchase justification — trimmed from section %d",
                             prev_page, bi - 1)

            for section_idx, start_page in enumerate(boundary_pages):
                end_page = boundary_pages[section_idx + 1] - 1 if section_idx + 1 < len(boundary_pages) else total_pages - 1
                # Shrink end if the last page(s) are purchase justifications for the next section
                while end_page in _trimmed_pages and end_page > start_page:
                    end_page -= 1
                section_text = "\n".join(page_texts[start_page:end_page + 1])

                header = {}

                # ── Parse header from the line AFTER the column headers ──
                # DocuSign 704 text comes as:
                #   "Requestor Institution or HQ Program Delivery Zip Code Phone Number Date of Request"
                #   "Magana- CIW ML EOP 92880 909 597-1771 3/24/2026"
                # The data line has: requestor, institution, zip, phone, date — all concatenated.
                _hdr_m = _re.search(
                    r'Requestor\s+Institution.*?Date of Request\s*\n\s*(.+)',
                    section_text, _re.IGNORECASE)
                if _hdr_m:
                    _data_line = _hdr_m.group(1).strip()
                    # Extract zip code (5 digits) — anchor for splitting
                    _zip_m = _re.search(r'\b(\d{5})\b', _data_line)
                    if _zip_m:
                        header["zip_code"] = _zip_m.group(1)
                        _before_zip = _data_line[:_zip_m.start()].strip()
                        _after_zip = _data_line[_zip_m.end():].strip()
                        # Before zip: "Requestor Institution" — split on double-space or known patterns
                        _parts = _re.split(r'\s{2,}', _before_zip)
                        if len(_parts) >= 2:
                            header["requestor"] = _parts[0].strip()
                            header["institution"] = " ".join(_parts[1:]).strip()
                        elif _before_zip:
                            # Try splitting at transition from name to institution code
                            _inst_m = _re.search(r'(.*?)\s+((?:CIW|CHCF|CMF|CSP|CCWF|SATF|MCSP|HDSP|KVSP|RJD|SCC|LAC|SVSP|CTF|COR|SOL|SAC|WSP|ISP|CIM|CAL|DVI|SQ|NKSP|FSP|CCI|PBSP|VSP|ASP|CMC)\b.+)', _before_zip)
                            if _inst_m:
                                header["requestor"] = _inst_m.group(1).strip()
                                header["institution"] = _inst_m.group(2).strip()
                            else:
                                header["institution"] = _before_zip
                        # After zip: phone and date
                        _phone_m = _re.search(r'(\d{3}[\s\-]?\d{3}[\s\-]?\d{4}(?:\s*(?:x|ext)\.?\s*\d+)?)', _after_zip)
                        if _phone_m:
                            header["phone"] = _phone_m.group(1).strip()
                        _date_m = _re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', _after_zip)
                        if _date_m:
                            header["due_date"] = _date_m.group(1).strip()
                else:
                    # Fallback: try individual field patterns
                    inst_m = _re.search(r'Institution or HQ Program\s*[\n:]+\s*([^\n]+)', section_text, _re.IGNORECASE)
                    if inst_m:
                        header["institution"] = inst_m.group(1).strip()
                    req_m = _re.search(r'Requestor\s*[\n:]+\s*([^\n]+)', section_text, _re.IGNORECASE)
                    if req_m:
                        header["requestor"] = req_m.group(1).strip()
                    zip_m = _re.search(r'(?:Zip|Delivery Zip)[:\s]+(\d{5})', section_text, _re.IGNORECASE)
                    if zip_m:
                        header["zip_code"] = zip_m.group(1)

                pc_num_m = _re.search(r'PRICE\s+CHECK\s*#\s*([A-Z0-9\-]+)', section_text, _re.IGNORECASE)
                if pc_num_m:
                    header["price_check_number"] = pc_num_m.group(1).strip()

                if "due_date" not in header:
                    due_m = _re.search(r'Date of Request\s*\n.*?(\d{1,2}/\d{1,2}/\d{2,4})', section_text)
                    if not due_m:
                        due_m = _re.search(r'(\d{1,2}/\d{1,2}/\d{4})', section_text)
                    if due_m:
                        header["due_date"] = due_m.group(1).strip()

                section_result = {
                    "header": header, "line_items": [], "existing_prices": {},
                    "ship_to": "", "source_pdf": pdf_path, "field_count": 0,
                    "parse_method": "multi_pc_text",
                    "page_start": start_page, "page_end": end_page,
                    "section_index": section_idx, "total_sections": len(boundary_pages),
                }

                ITEM_ROW = _re.compile(
                    r'^\s*(\d{1,2})\s+(\d+)\s+(EA|BX|CS|PK|PKG|PCK|PACK|BAG|SET|DZ|PR|GL|LB|OZ|CASE|EACH|CTN|RL|BT|TB|JR|CT|CA)\s+'
                    r'(?:(\d+)\s+)?(.+?)(?:\s+\$[\d,]+\.\d{2}\s+\$[\d,]+\.\d{2})?\s*$',
                    _re.IGNORECASE | _re.MULTILINE)

                items = []
                for m in ITEM_ROW.finditer(section_text):
                    desc = m.group(5).strip()
                    if any(h in desc.upper() for h in ["ITEM DESCRIPTION", "INCLUDE MANUFACTURER", "NOUN FIRST"]):
                        continue
                    items.append({
                        "item_number": int(m.group(1)), "row_index": int(m.group(1)),
                        "qty": int(m.group(2)), "uom": m.group(3).upper(),
                        "qty_per_uom": int(m.group(4)) if m.group(4) else 1,
                        "description": desc[:200], "part_number": "", "pricing": {},
                    })

                section_result["line_items"] = items
                log.info("parse_multi_pc: section %d (%s, pages %d-%d): %d items",
                         section_idx, header.get("institution", "?"), start_page, end_page, len(items))
                results.append(section_result)

            # ── Compute non-PC pages (purchase justifications, blank overflow, etc.) ──
            all_pc_pages = set()
            for s in results:
                all_pc_pages.update(range(s["page_start"], s["page_end"] + 1))
            non_pc_pages = sorted(set(range(total_pages)) - all_pc_pages)
            for s in results:
                s["non_pc_pages"] = non_pc_pages
            if non_pc_pages:
                log.info("parse_multi_pc: non-PC pages (purchase justifications etc.): %s", non_pc_pages)

    except Exception as e:
        log.error("parse_multi_pc %s: %s", os.path.basename(pdf_path), e, exc_info=True)
        result = parse_ams704(pdf_path)
        result["page_start"] = 0
        result["page_end"] = 0
        return [result]

    return results


def _extract_items_from_text(text: str, result: dict):
    """
    Extract line items from raw pypdf text of a flattened AMS 704.
    
    Handles multiple text patterns produced by DocuSign-flattened forms:
    Pattern A (clean): "10 each 1 Coast Emerald Burst Bodywash - 816559012292"
    Pattern B (garbled): "Suave Deodorant 2.6 Oz ... - 784922807236112each"
    Pattern C (page 3): "6 each wet n wild MegaSlicks Lip Gloss..."
    """
    items = []
    item_number = len(result.get("line_items", [])) + 1
    
    lines = text.split("\n")
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 10:
            continue
        
        # Skip header/footer lines
        skip_phrases = [
            "item description", "unit of measure", "substituted item",
            "price per unit", "extension", "merchandise subtotal",
            "total price", "fob destination", "ship to", "supplier",
            "company name", "certified sb", "instructions", "data entry",
            "page", "docusign", "state of california", "ams 704",
            "price check worksheet", "california correction", "see instructions",
            "payment terms", "note:", "date price check", "enter gran",
            "this document is used", "requestor:", "item description",
            "include manufacturer", "complete this field",
        ]
        line_lower = line.lower()
        if any(sp in line_lower for sp in skip_phrases):
            continue
        
        # Skip pure number sequences (item number columns extracted separately)
        if re.match(r'^\d{1,2}$', line.strip()):
            continue
        
        # Pattern A: "QTY UOM QTY_PER_UOM DESCRIPTION - UPC"
        # Most common on pages 2+ of DocuSign 704s
        m = re.match(
            r'^(\d{1,3})\s+(each|pack|set|box|case|dozen|pair)\s+(\d{1,3})\s+(.+)',
            line, re.IGNORECASE
        )
        if m:
            qty = int(m.group(1))
            uom = m.group(2).lower()
            qty_per_uom = int(m.group(3))
            desc = m.group(4).strip()
            # Extract UPC from end of description
            upc = ""
            upc_m = re.search(r'[-–]\s*(\d{6,15})\s*$', desc)
            if upc_m:
                upc = upc_m.group(1)
                desc = desc[:upc_m.start()].strip().rstrip('-–').strip()
            items.append({
                "item_number": str(item_number),
                "qty": qty,
                "uom": uom,
                "qty_per_uom": qty_per_uom,
                "description": desc,
                "part_number": upc,
                "row_index": item_number - 1,
            })
            item_number += 1
            continue
        
        # Pattern B: "QTY UOM DESCRIPTION - UPC" (no qty_per_uom)
        m = re.match(
            r'^(\d{1,3})\s+(each|pack|set|box|case|dozen|pair)\s+(.+)',
            line, re.IGNORECASE
        )
        if m:
            qty = int(m.group(1))
            uom = m.group(2).lower()
            desc = m.group(3).strip()
            upc = ""
            upc_m = re.search(r'[-–]\s*(\d{6,15})\s*$', desc)
            if upc_m:
                upc = upc_m.group(1)
                desc = desc[:upc_m.start()].strip().rstrip('-–').strip()
            items.append({
                "item_number": str(item_number),
                "qty": qty,
                "uom": uom,
                "qty_per_uom": 1,
                "description": desc,
                "part_number": upc,
                "row_index": item_number - 1,
            })
            item_number += 1
            continue
        
        # Pattern C: "DESCRIPTION - UPC+QTY+QTY_PER_UOM" (garbled, DocuSign page 1)
        # e.g. "Suave Deodorant ... - 784922807236112" → UPC=784922807236, qty_per_uom=1, qty=12
        # e.g. "VO5 Conditioner ... - 816559019857each 110" → UPC=816559019857
        # Standard UPCs are 12-13 digits; trailing digits after that are qty data
        m = re.match(
            r'^(.{10,}?)\s*[-–]\s*(\d{12,})\s*(each|pack|set|box|case)?\s*(\d{1,3})?\s*(each|pack|set|box|case)?\s*(\d{1,3})?\s*$',
            line, re.IGNORECASE
        )
        if m:
            desc = m.group(1).strip()
            digit_blob = m.group(2)
            uom_mid = (m.group(3) or m.group(5) or "each").lower()
            extra1 = int(m.group(4)) if m.group(4) else 0
            extra2 = int(m.group(6)) if m.group(6) else 0
            
            # Separate UPC (12-13 digits) from trailing qty data
            if len(digit_blob) > 13:
                upc = digit_blob[:12]
                trailing = digit_blob[12:]
                # Trailing digits are qty_per_uom + qty (e.g. "112" = 1, 12)
                if len(trailing) >= 2:
                    qty_per_uom = int(trailing[0])
                    qty = int(trailing[1:]) if trailing[1:] else 1
                elif len(trailing) == 1:
                    qty = int(trailing)
                    qty_per_uom = 1
                else:
                    qty = 1
                    qty_per_uom = 1
            elif len(digit_blob) == 13:
                # Could be 13-digit EAN or 12-digit UPC + 1 digit qty
                upc = digit_blob[:12]
                qty = int(digit_blob[12]) if digit_blob[12] != '0' else 1
                qty_per_uom = 1
            else:
                upc = digit_blob
                qty = 1
                qty_per_uom = 1
            
            # Handle explicit numbers after UOM
            qty_from_blob = (len(digit_blob) > 12)
            if extra1 and extra2:
                qty_per_uom = min(extra1, extra2)
                qty = max(extra1, extra2)
            elif extra1 and qty_from_blob:
                # qty already set from blob trailing digits — extra1 is qty_per_uom
                qty_per_uom = extra1
            elif extra1 and not qty_from_blob:
                # No qty from blob — extra1 might be jammed qpu+qty
                # "each 110" → qpu=1, qty=10 | "each 1" → qpu=1
                s = str(extra1)
                if len(s) >= 3 and int(s[0]) <= 3 and int(s[1:]) <= 150:
                    qty_per_uom = int(s[0])
                    qty = int(s[1:])
                elif len(s) == 2 and int(s[0]) <= 3:
                    qty_per_uom = int(s[0])
                    qty = int(s[1])
                elif extra1 <= 3:
                    qty_per_uom = extra1
                else:
                    qty = extra1
            
            items.append({
                "item_number": str(item_number),
                "qty": qty,
                "uom": uom_mid,
                "qty_per_uom": qty_per_uom,
                "description": desc,
                "part_number": upc,
                "row_index": item_number - 1,
            })
            item_number += 1
            continue
        
        # Pattern D: "DESCRIPTION - UPC" alone (qty on separate fragmented line)
        m = re.match(r'^(.{15,}?)\s*[-–]\s*(\d{6,15})\s*$', line)
        if m:
            desc = m.group(1).strip()
            upc = m.group(2)
            # Trim UPC to 12-13 digits if longer (trailing qty jammed on)
            if len(upc) > 13:
                real_upc = upc[:12]
                trailing = upc[12:]
                qty = int(trailing) if trailing.isdigit() and int(trailing) < 200 else 1
                upc = real_upc
            else:
                qty = 1
            items.append({
                "item_number": str(item_number),
                "qty": qty,
                "uom": "each",
                "qty_per_uom": 1,
                "description": desc,
                "part_number": upc,
                "row_index": item_number - 1,
            })
            item_number += 1
            continue
        
        # Pattern E: "UOM QTY_PER_UOM DESCRIPTION - UPC" (garbled, UOM comes first)
        # e.g. "each Zest Cocoa Butter... - 081655901245210"
        # e.g. "pack 2 Garnier Fructis... - 6030845557656"
        m = re.match(
            r'^(each|pack|set|box|case)\s+(\d{1,3}\s+)?(.+?)\s*[-–]\s*(\d{10,})\s*(\d{1,3})?\s*$',
            line, re.IGNORECASE
        )
        if m and len(m.group(3)) > 5:
            uom = m.group(1).lower()
            qty_per_uom = int(m.group(2).strip()) if m.group(2) else 1
            desc = m.group(3).strip()
            digit_blob = m.group(4)
            extra_qty = int(m.group(5)) if m.group(5) else 0
            
            # Separate UPC from trailing qty
            if len(digit_blob) > 13:
                upc = digit_blob[:12]
                trailing = digit_blob[12:]
                qty = int(trailing) if trailing.isdigit() and int(trailing) < 200 else 1
            else:
                upc = digit_blob[:12] if len(digit_blob) >= 12 else digit_blob
                qty = extra_qty or 1
            
            items.append({
                "item_number": str(item_number),
                "qty": qty,
                "uom": uom,
                "qty_per_uom": qty_per_uom,
                "description": desc,
                "part_number": upc,
                "row_index": item_number - 1,
            })
            item_number += 1
            continue
    
    # Post-process pass 1: handle split-line items
    # Pattern: "QTY UOM QTY_PER_UOM" on one line, description on next
    # e.g. "2 pack 144\nPaper Mate Arrowhead Pink Cap Erasers..."
    merged_items = []
    pending_header = None  # {"qty": int, "uom": str, "qty_per_uom": int}
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        # Check for "QTY UOM QTY_PER_UOM" standalone (no description)
        m = re.match(r'^(\d{1,3})\s+(each|pack|set|box|case)\s+(\d{1,4})\s*$', line, re.IGNORECASE)
        if m:
            pending_header = {
                "qty": int(m.group(1)),
                "uom": m.group(2).lower(),
                "qty_per_uom": int(m.group(3)),
            }
            continue
        
        # If we have a pending header, merge with next descriptive line
        if pending_header:
            # This line should be the description
            desc = line.strip()
            if len(desc) > 5 and not re.match(r'^\d{1,3}$', desc):
                # Extract UPC if present
                upc = ""
                upc_m = re.search(r'[-–]\s*(\d{6,15})\s*$', desc)
                if not upc_m:
                    # Try UPC in parens: "(73015)"
                    upc_m = re.search(r'\((\d{4,15})\)\s*$', desc)
                if upc_m:
                    upc = upc_m.group(1)
                    desc = desc[:upc_m.start()].strip().rstrip('-–').strip()
                
                items.append({
                    "item_number": str(item_number),
                    "qty": pending_header["qty"],
                    "uom": pending_header["uom"],
                    "qty_per_uom": pending_header["qty_per_uom"],
                    "description": desc,
                    "part_number": upc,
                    "row_index": item_number - 1,
                })
                item_number += 1
            pending_header = None
            continue
        pending_header = None
    
    # Post-process pass 2: handle "QTY_PER_UOM DESC...trailing QTY UOM" pattern
    # e.g. "120 Flexible Safety Pens...59574 pack"
    for i, line in enumerate(lines):
        line = line.strip()
        m = re.match(
            r'^(\d{2,4})\s+(.{10,}?)\s*(\d{1,3})\s+(each|pack|set|box|case)\s*$',
            line, re.IGNORECASE
        )
        if m:
            qpu = int(m.group(1))
            desc = m.group(2).strip()
            qty = int(m.group(3))
            uom = m.group(4).lower()
            # Only use if qty_per_uom > qty (120 > 4) and not already captured
            if qpu > qty and not any(desc[:20] in it.get("description", "") for it in items):
                # Extract UPC from end of desc
                upc = ""
                upc_m = re.search(r'[-–]\s*(\d{4,15})\s*$', desc)
                if upc_m:
                    upc = upc_m.group(1)
                    desc = desc[:upc_m.start()].strip().rstrip('-–').strip()
                items.append({
                    "item_number": str(item_number),
                    "qty": qty,
                    "uom": uom,
                    "qty_per_uom": qpu,
                    "description": desc,
                    "part_number": upc,
                    "row_index": item_number - 1,
                })
                item_number += 1
    
    # Post-process pass 3: fix items where description IS a number (broken split)
    # e.g. item with desc="144" should be removed (it was merged above)
    items = [it for it in items if not re.match(r'^\d{1,4}$', it.get("description", "").strip())]
    
    # Re-number items sequentially
    for i, it in enumerate(items):
        it["item_number"] = str(i + 1)
        it["row_index"] = i
    
    result["line_items"].extend(items)
    log.info("Text parser extracted %d items from %s", len(items),
             result.get("source_pdf", "?"))


def _extract_header_from_text(text: str, result: dict):
    """Extract header fields from raw text of AMS 704."""
    lines = text.split("\n")
    full_text = text
    
    # Requestor — look for name after "Requestor" label
    for i, line in enumerate(lines):
        if 'requestor' in line.lower() and 'notes' not in line.lower():
            # Name is usually on the NEXT line
            if i + 1 < len(lines):
                name = lines[i + 1].strip()
                if name and len(name) > 2 and not any(k in name.lower() for k in [
                    "institution", "delivery", "phone", "date", "company",
                    "megan", "notes", "item"
                ]):
                    result["header"]["requestor"] = name
                elif "megan" in name.lower() or re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+', name):
                    result["header"]["requestor"] = name
            # Also check same line: "Requestor\nMegan Smith" 
            m = re.search(r'(?:Requestor)\s*\n?\s*([A-Z][a-z]+\s+[A-Z][a-z]+)', text)
            if m:
                result["header"]["requestor"] = m.group(1).strip()
            break
    
    # Try direct pattern: "Requestor" followed by name on same/next line
    m = re.search(r'Requestor\s*\n\s*(\w[\w\s]{2,30}?)(?:\n|lnstitution|Institution)', text)
    if m:
        result["header"]["requestor"] = m.group(1).strip()

    # Institution — look for program name after "Institution or HQ Program"
    m = re.search(r'(?:Institution|lnstitution).*?(?:Program|program)\s*\n?\s*([A-Z][\w\.\-]+)', text)
    if m:
        result["header"]["institution"] = m.group(1).strip()
    
    # Delivery zip code
    m = re.search(r'(?:Delivery|Oelivery)\s*Zip\s*Code\s*\n?\s*(\d{5})', text)
    if m:
        result["header"]["delivery_zip"] = m.group(1)
        result["ship_to"] = m.group(1)
    
    # Phone number
    m = re.search(r'Phone\s*Number\s*\n?\s*([\d\-\(\)\s\.ext]+)', text)
    if m:
        result["header"]["phone"] = m.group(1).strip()
    
    # Date of request
    m = re.search(r'(?:Date of Request|Oate of Request)\s*\n?\s*([\d\-/\.]+)', text)
    if m:
        result["header"]["date_of_request"] = m.group(1).strip()
    
    # Due date
    m = re.search(r'(?:DUE DATE|Due Date).*?Date:\s*([\d/\-\.]+)', text, re.DOTALL)
    if m:
        result["header"]["due_date"] = m.group(1).strip()
    
    # Price Check # (often empty on forms sent for pricing)
    m = re.search(r'PRICE\s*CHECK\s*#\s*[:\s]*(\S+)', text)
    if m and m.group(1) not in ("Payment", "DUE", "Date"):
        result["header"]["price_check_number"] = m.group(1).strip()


def _extract_items_from_table(table: list, result: dict, page_num: int):
    """Extract line items from a pdfplumber table."""
    if not table or len(table) < 2:
        return

    # Find header row
    header_row = None
    for i, row in enumerate(table):
        row_text = ' '.join(str(c or '') for c in row).lower()
        if 'item' in row_text and ('description' in row_text or 'qty' in row_text):
            header_row = i
            break

    if header_row is None:
        return

    # Map columns
    headers = table[header_row]
    col_map = {}
    for j, h in enumerate(headers):
        h_text = str(h or "").lower()
        if "item" in h_text and "#" in h_text:
            col_map["item_number"] = j
        elif "qty" == h_text.strip() or h_text.startswith("qty"):
            if "per" in h_text:
                col_map["qty_per_uom"] = j
            else:
                col_map["qty"] = j
        elif "uom" in h_text or "measure" in h_text:
            col_map["uom"] = j
        elif "description" in h_text:
            col_map["description"] = j
        elif "substitut" in h_text or ("part" in h_text and "number" in h_text):
            col_map["substituted"] = j
        elif "price" in h_text and "unit" in h_text:
            col_map["unit_price"] = j
        elif "extension" in h_text:
            col_map["extension"] = j

    # Extract data rows
    for i in range(header_row + 1, len(table)):
        row = table[i]
        if not row:
            continue

        desc = str(row[col_map["description"]] or "") if "description" in col_map else ""
        if not desc.strip():
            continue

        # Check if this row has an item number or qty — if not, it's a
        # continuation of the previous item's multi-line description
        item_num_val = str(row[col_map.get("item_number", 0)] or "").strip() if "item_number" in col_map else ""
        qty_raw = str(row[col_map.get("qty", "")] or "").strip() if "qty" in col_map else ""
        uom_raw = str(row[col_map.get("uom", "")] or "").strip() if "uom" in col_map else ""

        has_item_number = bool(item_num_val) and item_num_val not in ("None", "0")
        has_qty = bool(qty_raw)

        # Treat sequential item numbers (1-50) as auto-filled, not real data
        if has_item_number and _is_sequential_number(item_num_val):
            has_item_number = False

        # Continuation row: no real item# AND no qty — merge into previous item
        is_continuation = (not has_item_number and not has_qty and result["line_items"])
        # Also merge if description is clearly supplementary (pack info, part number)
        # regardless of qty/item# (some forms auto-fill these from previous row)
        if not is_continuation and result["line_items"] and _is_supplementary_desc(desc.strip()):
            is_continuation = True
        if is_continuation:
            prev = result["line_items"][-1]
            prev_raw = prev.get("description_raw", prev["description"])
            prev["description"] = clean_description(prev_raw + " " + desc.strip())
            prev["description_raw"] = (prev_raw + " " + desc.strip()).strip()
            # Re-extract MFG/part number with the fuller description
            real_pn = extract_item_numbers(prev)
            if real_pn:
                prev["mfg_number"] = real_pn
            log.debug("  continuation row merged into item %s: '%s'",
                       prev.get("item_number", "?"), desc.strip()[:40])
            continue

        qty_str = qty_raw or "1"
        try:
            qty = int(float(qty_str))
        except Exception:
            qty = 1

        row_num = len(result["line_items"]) + 1 + (page_num * MAX_ROWS_PER_PAGE)

        # Get substituted item text if column exists
        sub_text = ""
        if "substituted" in col_map and col_map["substituted"] < len(row):
            sub_text = str(row[col_map["substituted"]] or "").strip()

        # Get qty_per_uom if column exists
        _qpu = 1
        if "qty_per_uom" in col_map and col_map["qty_per_uom"] < len(row):
            try:
                _qpu = max(1, int(float(str(row[col_map["qty_per_uom"]] or "1").replace(",", ""))))
            except (ValueError, TypeError):
                _qpu = 1

        item = {
            "item_number": item_num_val or str(row_num),
            "qty": qty,
            "uom": (uom_raw or "ea").upper(),
            "qty_per_uom": _qpu,
            "description": clean_description(desc.strip()),
            "description_raw": desc.strip(),
            "substituted": sub_text,
            "row_index": row_num,
        }

        # Extract real MFG/part number
        real_pn = extract_item_numbers(item)
        if real_pn:
            item["mfg_number"] = real_pn

        result["line_items"].append(item)


# ─── Price Lookup ────────────────────────────────────────────────────────────

def lookup_prices(parsed_pc: dict) -> dict:
    """
    Look up prices for all line items in a parsed Price Check.
    Uses Amazon (SerpApi) and SCPRS Won Quotes KB.

    Returns updated parsed_pc with pricing added to each item.
    """
    items = parsed_pc.get("line_items", [])
    results = []

    for item in items:
        desc = item["description"]
        pricing = {
            "amazon_price": None,
            "amazon_title": "",
            "amazon_url": "",
            "amazon_asin": "",
            "scprs_price": None,
            "recommended_price": None,
            "price_source": None,
        }

        # 1. Search Amazon
        if HAS_RESEARCH:
            try:
                research = research_product(description=desc)
                if research.get("found"):
                    pricing["amazon_price"] = research["price"]
                    pricing["amazon_title"] = research.get("title", "")
                    pricing["amazon_url"] = research.get("url", "")
                    pricing["amazon_asin"] = research.get("asin", "")
                    pricing["mfg_number"] = research.get("mfg_number", "")
                    pricing["manufacturer"] = research.get("manufacturer", "")
                    pricing["price_source"] = "amazon"
            except Exception as e:
                log.error(f"Amazon lookup error for '{desc[:50]}': {e}")

        # 2. Check SCPRS Won Quotes
        if HAS_WON_QUOTES:
            try:
                matches = find_similar_items(
                    item_number=item.get("item_number", ""),
                    description=desc,
                )
                if matches:
                    best = matches[0]
                    quote = best.get("quote", best)
                    pricing["scprs_price"] = quote.get("unit_price")
            except Exception as e:
                log.error(f"SCPRS lookup error for '{desc[:50]}': {e}")

        # 3. Run Pricing Oracle
        supplier_cost = pricing["amazon_price"]
        if HAS_ORACLE and supplier_cost:
            try:
                rec = recommend_price(
                    supplier_cost=supplier_cost,
                    scprs_matches=[{"unit_price": pricing["scprs_price"]}] if pricing["scprs_price"] else [],
                    item_category="general",
                )
                if rec and rec.get("recommended"):
                    pricing["recommended_price"] = rec["recommended"]["price"]
            except Exception as e:
                log.error(f"Oracle error for '{desc[:50]}': {e}")

        # Fallback: if oracle didn't run, use cost + 25% markup
        if not pricing["recommended_price"] and supplier_cost:
            pricing["recommended_price"] = round(supplier_cost * 1.25, 2)

        item["pricing"] = pricing
        results.append(item)

        # ── Persist every price found to SQLite price_history ──
        try:
            from src.core.db import record_price
            pc_id = parsed_pc.get("pc_id", "") or parsed_pc.get("id", "")
            agency = parsed_pc.get("agency", "") or parsed_pc.get("institution", "")
            if pricing.get("amazon_price"):
                record_price(
                    description=desc,
                    unit_price=pricing["amazon_price"],
                    source="amazon",
                    part_number=item.get("item_number","") or pricing.get("mfg_number",""),
                    manufacturer=pricing.get("manufacturer",""),
                    source_url=pricing.get("amazon_url",""),
                    source_id=pricing.get("amazon_asin",""),
                    agency=agency,
                    price_check_id=pc_id,
                )
            if pricing.get("scprs_price"):
                record_price(
                    description=desc,
                    unit_price=pricing["scprs_price"],
                    source="scprs",
                    part_number=item.get("item_number",""),
                    agency=agency,
                    price_check_id=pc_id,
                )
            if pricing.get("recommended_price"):
                record_price(
                    description=desc,
                    unit_price=pricing["recommended_price"],
                    source="recommended",
                    part_number=item.get("item_number",""),
                    agency=agency,
                    price_check_id=pc_id,
                    notes=f"markup from {pricing.get('price_source','unknown')}",
                )
        except Exception:
            pass

    parsed_pc["line_items"] = results
    return parsed_pc


# ─── Fill AMS 704 PDF ────────────────────────────────────────────────────────


def _detect_page_layout(pdf_fields: dict, source_pdf: str = None):
    """Detect the AMS 704 page layout: how many rows on page 1, page 2 suffix rows,
    and page 2 unsuffixed continuation rows.

    Returns (pg1_rows, pg2_suffix_rows, pg2_extra_rows) where:
      pg1_rows: unsuffixed row fields physically on page 1 (usually 8)
      pg2_suffix_rows: _2 suffix row fields on page 2 (usually 8)
      pg2_extra_rows: unsuffixed row fields physically on page 2 (e.g. Row9-11 → 3)

    The form_capacity = pg1_rows + pg2_suffix_rows + pg2_extra_rows (usually 19).
    """
    pg1_rows = 0
    pg2_extra_rows = 0
    pg2_suffix_rows = 0

    # Count _2 suffix rows from field names (always reliable)
    for fname in pdf_fields:
        m = re.search(r'QTYRow(\d+)_2$', fname)
        if m:
            pg2_suffix_rows = max(pg2_suffix_rows, int(m.group(1)))

    # If we have the source PDF, check physical page of each unsuffixed annotation
    if source_pdf:
        try:
            reader = PdfReader(source_pdf)
            pg1_unsuf = set()
            pg2_unsuf = set()
            for pg_idx, page in enumerate(reader.pages[:2]):
                for annot_ref in (page.get("/Annots") or []):
                    try:
                        obj = annot_ref.get_object()
                        name = str(obj.get("/T", ""))
                        m = re.search(r'QTYRow(\d+)$', name)
                        if m:
                            row_n = int(m.group(1))
                            if pg_idx == 0:
                                pg1_unsuf.add(row_n)
                            else:
                                pg2_unsuf.add(row_n)
                    except Exception:
                        pass
            if pg1_unsuf:
                pg1_rows = max(pg1_unsuf)
                pg2_extra_rows = len(pg2_unsuf)
                log.info("_detect_page_layout: pg1=%d unsuffixed on page 1, pg2_suffix=%d, "
                         "pg2_extra=%d unsuffixed on page 2 (rows %s)",
                         pg1_rows, pg2_suffix_rows, pg2_extra_rows, sorted(pg2_unsuf))
                return pg1_rows, pg2_suffix_rows, pg2_extra_rows
        except Exception as e:
            log.debug("_detect_page_layout: annotation scan failed: %s", e)

    # Fallback: count all unsuffixed field names, assume all on page 1
    max_unsuffixed = 0
    for fname in pdf_fields:
        m = re.search(r'QTYRow(\d+)$', fname)
        if m:
            max_unsuffixed = max(max_unsuffixed, int(m.group(1)))
    pg1_rows = max_unsuffixed or 8
    log.info("_detect_page_layout: pg1=%d (fallback name scan), pg2_suffix=%d, pg2_extra=0",
             pg1_rows, pg2_suffix_rows)
    return pg1_rows, pg2_suffix_rows, 0


def fill_ams704(
    source_pdf: str,
    parsed_pc: dict,
    output_pdf: str,
    company_info: dict = None,
    price_tier: str = "recommended",  # "recommended", "aggressive", "safe"
    tax_rate: float = 0.0,  # e.g. 0.0775 for 7.75%
    custom_notes: str = "",  # Editable supplier notes
    delivery_option: str = "",  # Override delivery time
    original_mode: bool = False,  # True = only fill company info + pricing, leave buyer fields
    keep_all_pages: bool = False,  # True = don't trim unused pages (DOCX-converted sources)
) -> dict:
    """
    Fill in the AMS 704 form with supplier info and pricing.

    Args:
        source_pdf: Path to original AMS 704 PDF
        parsed_pc: Parsed price check data with pricing
        output_pdf: Path for filled output PDF
        company_info: Override REYTECH_INFO
        price_tier: Which price tier to use
        tax_rate: Tax rate (0 if tax exempt)

    Returns:
        {"ok": bool, "output": str, "summary": {...}}
    """
    if not HAS_PYPDF:
        return {"ok": False, "error": "pypdf not available"}

    info = company_info or REYTECH_INFO
    items = parsed_pc.get("line_items", [])

    # Override delivery if specified
    if delivery_option:
        info = dict(info)  # don't mutate original
        info["delivery"] = delivery_option

    # Build field values
    field_values = []

    # Supplier info
    supplier_mappings = [
        ("COMPANY NAME", info.get("company_name", "")),
        ("COMPANY REPRESENTATIVE print name", info.get("representative", "")),
        ("Address", info.get("address", "")),
        ("Phone Number_2", info.get("phone", "")),
        ("EMail Address", info.get("email", "")),
        ("Certified SBMB", info.get("sb_mb", "")),
        ("Certified DVBE", info.get("dvbe", "")),
        ("Delivery Date and Time ARO", info.get("delivery", "5-7 business days")),
        ("Discount Offered", info.get("discount", "Included")),
        ("Date Price Check Expires", _expiry_date()),
    ]

    for field_id, value in supplier_mappings:
        if value:
            field_values.append({
                "field_id": field_id,
                "page": 1,
                "value": value,
            })

    # Ship To: use institution/HQ program from the PC header
    _institution = (parsed_pc.get("header", {}).get("institution", "") or "").strip()
    _ship_to = (parsed_pc.get("ship_to", "") or "").strip()
    _zip = (parsed_pc.get("header", {}).get("zip_code", "") or "").strip()
    ship_to_value = _ship_to or _institution
    if ship_to_value and _zip and _zip not in ship_to_value:
        ship_to_value = f"{ship_to_value}, {_zip}"
    # Ship to: only write if template field is empty (buyer may have pre-filled)
    if ship_to_value:
        _existing_ship = ""
        try:
            from pypdf import PdfReader as _ShipPR
            _ship_check = _ShipPR(source_pdf)
            _ship_fields = _ship_check.get_fields() or {}
            _existing_ship = str((_ship_fields.get("Ship to") or {}).get("/V", "")).strip()
        except Exception:
            pass
        if not _existing_ship:
            field_values.append({
                "field_id": "Ship to",
                "page": 1,
                "value": ship_to_value,
            })
        else:
            log.info("fill_ams704: Ship to already has value '%s' — not overwriting", _existing_ship[:40])

    # ── Template introspection via TemplateProfile (single read, cached) ──
    from src.forms.template_registry import get_profile
    _profile = get_profile(source_pdf)
    _pdf_fields = {fn: None for fn in _profile.field_names}  # compat dict for legacy refs
    if _profile.field_names:
        log.info("fill_ams704: %d PDF fields found (via TemplateProfile). Checkbox fields: %s",
                 len(_profile.field_names),
                 [fn for fn in sorted(_profile.field_names)
                  if any(k in fn.lower() for k in ("check", "fob", "box", "dest", "freight"))][:10])

    # FOB Destination, Freight Prepaid checkbox — from TemplateProfile detection
    _fob_names_to_check = set(_profile.fob_prepaid_fields)
    # Also add static fallbacks that may not match the dynamic pattern
    for _static_fob in ("FOB Destination Freight Prepaid",
                        "FOB Destination  Freight Prepaid",
                        "FOBDestinationFreightPrepaid"):
        if _profile.has_field(_static_fob):
            _fob_names_to_check.add(_static_fob)
    for _fob_name in _fob_names_to_check:
        field_values.append({
            "field_id": _fob_name,
            "page": 1,
            "value": "/Yes",
        })
    # For flat PDFs (no form fields), also add the overlay checkbox name
    # so _fill_pdf_text_overlay draws the X mark
    if not _fob_names_to_check:
        field_values.append({
            "field_id": "Check Box4",
            "page": 1,
            "value": "/Yes",
        })

    # Line items with pricing
    subtotal = 0.0
    items_priced = 0

    # ── Fill strategy via FillStrategy enum (replaces original_mode boolean) ──
    from src.forms.ams704_helpers import FillStrategy
    _form_is_prefilled = _profile.is_prefilled
    # Also check legacy field names that TemplateProfile's QTYRow scan may miss
    if not _form_is_prefilled:
        for _check_field in ["Qty_1", "QTY_1", "qty_1", "fill_5"]:
            _pv = _profile.get_field_value(_check_field)
            if _pv and _pv not in ("0", "/Off"):
                _form_is_prefilled = True
                log.info("fill_ams704: pre-filled detected via legacy field '%s'", _check_field)
                break

    _strategy = FillStrategy.for_pc(
        is_prefilled=_form_is_prefilled,
        is_docx_source=keep_all_pages,
    )
    # Backward compat: sync original_mode with strategy for downstream code
    if _form_is_prefilled and not original_mode:
        original_mode = True
        log.info("fill_ams704: auto-switched to original_mode (pre-filled template detected)")
    log.info("fill_ams704: strategy=%s (original_mode=%s, keep_all_pages=%s)",
             _strategy.value, original_mode, keep_all_pages)

    # ── Fill buyer header fields when template is blank (not pre-filled) ──
    # DOCX-sourced PCs use the blank template, so we must write requestor/institution/etc.
    if not _form_is_prefilled:
        _hdr = parsed_pc.get("header", {})
        # Also check top-level parsed_pc keys (upload handler copies header→top-level)
        def _h(key, *alts):
            v = _hdr.get(key, "")
            if not v:
                for a in alts:
                    v = _hdr.get(a, "") or parsed_pc.get(a, "")
                    if v:
                        break
            if not v:
                v = parsed_pc.get(key, "")
            return str(v).strip() if v else ""
        _header_mappings = [
            ("Requestor", _h("requestor")),
            ("Institution or HQ Program", _h("institution")),
            ("Delivery Zip Code", _h("zip_code", "delivery_zip")),
            ("Phone Number", _h("phone", "phone_number")),
            ("Date of Request", _h("date_of_request")),
            ("PRICE CHECK", _h("pc_number") or parsed_pc.get("pc_number", "")),
            ("Text2", _h("due_date")),
        ]
        for _hf_id, _hf_val in _header_mappings:
            if _hf_val:
                field_values.append({"field_id": _hf_id, "page": 1, "value": str(_hf_val)})
        _hdr_filled = sum(1 for _, v in _header_mappings if v)
        if _hdr_filled:
            log.info("fill_ams704: filled %d buyer header fields (blank template)", _hdr_filled)

    seq = 0  # sequential line item counter
    _skipped_no_row = 0
    _skipped_no_price = 0
    max_row = 50  # Support up to 50 items across multiple pages (8+11+8+11+8...)

    # Pre-compute which rows are occupied by items (for description overflow)
    occupied_rows = set()
    for _idx, _item in enumerate(items):
        _r = _item.get("row_index") or (_idx + 1)
        if 1 <= _r <= max_row:
            occupied_rows.add(_r)
    overflow_rows = set()  # Track rows used for description overflow

    # Layout detection via TemplateProfile (replaces inline _detect_page_layout)
    _has_suffix_fields = _profile.has_suffix_fields
    _pg1_rows = _profile.pg1_row_count
    _pg2_rows = len(_profile.pg2_rows_suffixed)
    _pg2_extra = len(_profile.pg2_rows_plain)
    _form_capacity = _profile.row_capacity
    log.info("fill_ams704: layout pg1=%d, pg2_suffix=%d, pg2_extra=%d, capacity=%d, has_suffix=%s (via TemplateProfile)",
             _pg1_rows, _pg2_rows, _pg2_extra, _form_capacity, _has_suffix_fields)

    for item_idx, item in enumerate(items):
        row = item.get("row_index") or (item_idx + 1)  # default to 1-based position
        # Map items to correct form fields based on detected page layout
        # Page 1: items 1.._pg1_rows → unsuffixed Row1..Row{_pg1_rows}
        # Page 2 top: items _pg1_rows+1.._pg1_rows+_pg2_rows → _2 suffix Row1_2..Row{_pg2_rows}_2
        # Page 2 bottom: items _pg1_rows+_pg2_rows+1..+_pg2_extra → unsuffixed Row{_pg1_rows+1}..
        # Beyond: overflow pages (no form fields)
        _field_suffix = ""
        _page_num = 1
        if _has_suffix_fields and row > _pg1_rows:
            _beyond_pg1 = row - _pg1_rows  # 1-based offset past page 1
            if _beyond_pg1 <= _pg2_rows:
                # Page 2 — _2 suffix fields (Row1_2 through Row{_pg2_rows}_2)
                _field_suffix = "_2"
                _page_num = 2
                row = _beyond_pg1
            elif _beyond_pg1 <= _pg2_rows + _pg2_extra:
                # Page 2 — unsuffixed continuation rows (Row9, Row10, Row11)
                _field_suffix = ""
                _page_num = 2
                row = _pg1_rows + (_beyond_pg1 - _pg2_rows)  # maps to Row9, Row10, Row11
            else:
                # Beyond all form fields — handled by _append_overflow_pages()
                continue
        elif row > _pg1_rows and not _has_suffix_fields:
            # No suffix fields — use sequential numbering (Row9, Row10, etc.)
            row = item_idx + 1
        if row < 1 or row > max_row:
            _skipped_no_row += 1
            log.debug("fill_ams704 SKIP item (bad row_index=%s): desc='%s'",
                       row, (item.get("description") or "")[:40])
            continue

        pricing = item.get("pricing", {})
        seq += 1
        _raw_qty = item.get("qty", 1)
        try:
            qty = int(float(_raw_qty)) if _raw_qty else 1
        except (ValueError, TypeError):
            qty = 1

        # ── Resolve price via strategy (V3 shared helper) ──
        from src.forms.ams704_helpers import resolve_pc_price, enrich_pc_description, build_pc_substitute_text, split_description
        unit_price = resolve_pc_price(item, _strategy)

        # ── ORIGINAL MODE: only fill pricing fields, leave buyer fields untouched ──
        if original_mode:
            if unit_price and unit_price > 0:
                extension = round(unit_price * qty, 2)
                if extension > 0:
                    subtotal += extension
                    items_priced += 1
                    price_field = ROW_FIELDS["unit_price"].format(n=row) + _field_suffix
                    ext_field = ROW_FIELDS["extension"].format(n=row) + _field_suffix
                    field_values.append({"field_id": price_field, "page": _page_num, "value": f"{unit_price:,.2f}"})
                    field_values.append({"field_id": ext_field, "page": _page_num, "value": f"{extension:,.2f}"})
                    log.info("fill_ams704 ORIGINAL row=%d idx=%d: price=%.2f qty=%d ext=%.2f",
                             row, item_idx, unit_price, qty, extension)
                else:
                    _skipped_no_price += 1
            else:
                _skipped_no_price += 1

            # Always write qty/uom — user may have edited these
            qty_field = ROW_FIELDS["qty"].format(n=row) + _field_suffix
            field_values.append({"field_id": qty_field, "page": _page_num, "value": str(qty)})
            uom_val = str(item.get("uom") or "EA").strip().upper()
            uom_field = ROW_FIELDS["uom"].format(n=row) + _field_suffix
            field_values.append({"field_id": uom_field, "page": _page_num, "value": uom_val})
            _qpu = item.get("qty_per_uom", 1)
            try:
                _qpu = int(float(_qpu)) if _qpu else 1
            except (ValueError, TypeError):
                _qpu = 1
            qpu_field = ROW_FIELDS["qty_per_uom"].format(n=row) + _field_suffix
            field_values.append({"field_id": qpu_field, "page": _page_num, "value": str(_qpu) if _qpu > 1 else ""})

            continue

        # ── NORMAL MODE: write all fields ──

        # Item number (sequential)
        item_num_field = ROW_FIELDS["item_number"].format(n=row) + _field_suffix
        field_values.append({"field_id": item_num_field, "page": _page_num, "value": str(seq)})

        # QTY
        qty_field = ROW_FIELDS["qty"].format(n=row) + _field_suffix
        field_values.append({"field_id": qty_field, "page": _page_num, "value": str(qty)})

        # Description (enriched via V3 helper)
        desc_final = enrich_pc_description(item, clean_fn=clean_description)
        if desc_final:
            desc_field = ROW_FIELDS["description"].format(n=row) + _field_suffix
            # Description overflow into next empty row
            part1, part2 = split_description(desc_final)
            if part2:
                _global_row = (row + _pg1_rows) if _field_suffix == "_2" else row
                _global_next = _global_row + 1
                if _global_next <= max_row and _global_next not in occupied_rows and _global_next not in overflow_rows:
                    _ovf_suffix = _field_suffix
                    _ovf_row = row + 1
                    _ovf_page = _page_num
                    if _field_suffix == "" and _ovf_row > _pg1_rows and _has_suffix_fields:
                        _ovf_suffix = "_2"
                        _ovf_row = 1
                        _ovf_page = 2
                    elif _field_suffix == "_2" and _ovf_row > _pg2_rows:
                        _ovf_suffix = None
                    if _ovf_suffix is not None:
                        overflow_field = ROW_FIELDS["description"].format(n=_ovf_row) + _ovf_suffix
                        field_values.append({"field_id": overflow_field, "page": _ovf_page, "value": part2})
                        overflow_rows.add(_global_next)
                        desc_final = part1
                        log.info("fill_ams704 row=%d: desc overflow into global row %d (%d chars)",
                                 _global_row, _global_next, len(part2))
                else:
                    desc_final = part1  # Can't overflow, truncate
            field_values.append({"field_id": desc_field, "page": _page_num, "value": desc_final})

        # UOM
        uom_val = (item.get("uom") or "EA").upper()
        uom_field = ROW_FIELDS["uom"].format(n=row) + _field_suffix
        field_values.append({"field_id": uom_field, "page": _page_num, "value": uom_val})

        # ── Price and Extension (only if we have a price) ──
        if not unit_price:
            _skipped_no_price += 1
            log.info("fill_ams704 row=%d: desc WRITTEN, but NO PRICE (desc='%s')",
                     row, (desc_final or "")[:40])
            # Still write substituted item
            sub_field = ROW_FIELDS["substituted"].format(n=row) + _field_suffix
            sub_text = build_pc_substitute_text(item, clean_description(item.get("description", "")))
            if sub_text:
                field_values.append({"field_id": sub_field, "page": _page_num, "value": sub_text})
            continue

        _qpu = item.get("qty_per_uom", 1)
        try:
            _qpu = int(float(_qpu)) if _qpu else 1
        except (ValueError, TypeError):
            _qpu = 1
        if _qpu > 1:
            qpu_field = ROW_FIELDS["qty_per_uom"].format(n=row) + _field_suffix
            field_values.append({"field_id": qpu_field, "page": _page_num, "value": str(_qpu)})
        extension = round(unit_price * qty, 2)
        subtotal += extension
        items_priced += 1
        log.info("fill_ams704 WRITE row=%d: desc='%s' price=%.2f qty=%d ext=%.2f",
                 row, (desc_final or "")[:40], unit_price, qty, extension)

        price_field = ROW_FIELDS["unit_price"].format(n=row) + _field_suffix
        ext_field = ROW_FIELDS["extension"].format(n=row) + _field_suffix
        field_values.append({"field_id": price_field, "page": _page_num, "value": f"{unit_price:,.2f}"})
        field_values.append({"field_id": ext_field, "page": _page_num, "value": f"{extension:,.2f}"})

        # Substituted item (via V3 helper)
        sub_field = ROW_FIELDS["substituted"].format(n=row) + _field_suffix
        sub_text = build_pc_substitute_text(item, clean_description(item.get("description", "")))
        if sub_text:
            field_values.append({"field_id": sub_field, "page": _page_num, "value": sub_text})
        elif not item.get("is_substitute"):
            field_values.append({"field_id": sub_field, "page": _page_num, "value": " "})

    # Clear unused rows to prevent ghost data from previous fills
    # Only clear rows that actually exist as form fields:
    #   Page 1: rows 1.._pg1_rows (unsuffixed)
    #   Page 2: rows 1.._pg2_rows (with _2 suffix)
    if not original_mode:
        filled_rows = occupied_rows | overflow_rows
        _cleared = 0

        # Page 1 rows (unsuffixed)
        for empty_row in range(1, _pg1_rows + 1):
            if empty_row in filled_rows:
                continue
            for key, pattern in ROW_FIELDS.items():
                field_values.append({
                    "field_id": pattern.format(n=empty_row),
                    "page": 1,
                    "value": " ",
                })
            _cleared += 1

        # Page 2 rows (_2 suffix) — only if template has page 2 fields
        if _has_suffix_fields:
            for empty_row in range(1, _pg2_rows + 1):
                _global_row = _pg1_rows + empty_row
                if _global_row in filled_rows:
                    continue
                for key, pattern in ROW_FIELDS.items():
                    field_values.append({
                        "field_id": pattern.format(n=empty_row) + "_2",
                        "page": 2,
                        "value": " ",
                    })
                _cleared += 1

        # Page 2 extra unsuffixed rows (Row9-Row11 on page 2)
        if _pg2_extra > 0:
            for i in range(1, _pg2_extra + 1):
                _unsuf_row = _pg1_rows + i  # Row9, Row10, Row11
                _global_row = _pg1_rows + _pg2_rows + i
                if _global_row in filled_rows:
                    continue
                for key, pattern in ROW_FIELDS.items():
                    field_values.append({
                        "field_id": pattern.format(n=_unsuf_row),
                        "page": 2,
                        "value": " ",
                    })
                _cleared += 1

        log.info("fill_ams704: cleared %d unused rows (filled: %d items + %d overflow)",
                 _cleared, len(occupied_rows), len(overflow_rows))
    else:
        log.info("fill_ams704 ORIGINAL MODE: skipped row clearing (preserving buyer fields)")

    # Totals
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)

    field_values.append({"field_id": "fill_70", "page": 1, "value": f"{subtotal:,.2f}"})
    field_values.append({"field_id": "fill_71", "page": 1, "value": "0.00"})  # Freight
    field_values.append({"field_id": "fill_72", "page": 1, "value": f"{tax:,.2f}"})
    field_values.append({"field_id": "fill_73", "page": 1, "value": f"{total:,.2f}"})

    # Always fill SUPPLIER NAME (shared field, appears on all pages)
    if "SUPPLIER NAME" in _pdf_fields:
        field_values.append({"field_id": "SUPPLIER NAME", "page": 1, "value": info.get("company_name", "Reytech Inc.")})

    # Determine how many pages actually have items
    _max_item_row = max((it.get("row_index") or (i + 1) for i, it in enumerate(items)), default=0)
    if _max_item_row <= 0:
        _pages_with_items = 1
    elif _pg1_rows > 0 and _max_item_row <= _pg1_rows:
        _pages_with_items = 1
    elif _form_capacity > 0 and _max_item_row <= _form_capacity:
        _pages_with_items = 2  # pg2 suffix rows + pg2 extra unsuffixed all fit on page 2
    else:
        # Pages 3+: overflow items beyond form capacity
        _overflow = _max_item_row - _form_capacity
        _rows_per_overflow_page = _pg2_rows if _pg2_rows > 0 else 8  # safe default
        _pages_with_items = 2 + ((_overflow - 1) // _rows_per_overflow_page) + 1
    _pdf_total_pages = _profile.page_count or 1

    # When keep_all_pages=True (DOCX-converted sources), treat all source pages as having content.
    # The converted PDF preserves the buyer's layout — don't trim any pages.
    if keep_all_pages and _pdf_total_pages > _pages_with_items:
        _pages_with_items = _pdf_total_pages
        log.info("fill_ams704: keep_all_pages=True — treating all %d pages as having content", _pdf_total_pages)

    # Page numbering — set on pages with items, BLANK on empty pages
    # Track whether page-specific fields exist (if not, need overlay for page 2+)
    _page_fields_are_shared = "Page" in _pdf_fields and "Page_2" not in _pdf_fields
    for pg in range(1, _pdf_total_pages + 1):
        suffix = "" if pg == 1 else f"_{pg}"
        page_field = f"Page{suffix}"
        of_field = f"of{suffix}"
        if pg <= _pages_with_items:
            # Page has items — set correct numbering
            if page_field in _pdf_fields:
                field_values.append({"field_id": page_field, "page": pg, "value": str(pg)})
            if of_field in _pdf_fields:
                field_values.append({"field_id": of_field, "page": pg, "value": str(_pages_with_items)})
        else:
            # Empty page — blank out any pre-filled page numbers and supplier
            if page_field in _pdf_fields:
                field_values.append({"field_id": page_field, "page": pg, "value": " "})
            if of_field in _pdf_fields:
                field_values.append({"field_id": of_field, "page": pg, "value": " "})
            # Blank out supplier name on empty continuation pages
            sup_field = f"SUPPLIER NAME{suffix}"
            if sup_field in _pdf_fields:
                field_values.append({"field_id": sup_field, "page": pg, "value": " "})
    # For shared Page/of fields: form fill sets page 1 value, overlay corrects page 2+
    if _page_fields_are_shared and _pages_with_items >= 2:
        log.info("fill_ams704: Page/of are shared fields — will overlay correct numbers on page 2+")

    # Multi-page: grand total on page 2 ONLY if page 2 has items
    if _pages_with_items >= 2 and _has_suffix_fields and "EXTENSIONENTER GRAND TOTAL ON FRONT PAGE" in _pdf_fields:
        field_values.append({"field_id": "EXTENSIONENTER GRAND TOTAL ON FRONT PAGE",
                             "page": 2, "value": f"{total:,.2f}"})

    # Notes — user-editable, no default
    if not original_mode:
        field_values.append({"field_id": "Supplier andor Requestor Notes", "page": 1, "value": custom_notes or ""})

    # Write field_values.json and use fill script
    fv_path = os.path.join(DATA_DIR, "pc_field_values.json")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(fv_path, "w") as f:
        json.dump(field_values, f, indent=2)

    # Trim source template to needed pages for form fill (max 2 — pages 3+ added by overflow)
    _fill_pages = min(_pages_with_items, 2)  # Form fields only on pages 1-2
    _fill_source = source_pdf
    _trimmed_tmp = None
    if _fill_pages < _pdf_total_pages:
        try:
            import tempfile as _tmpmod
            from pypdf import PdfReader as _TrimR, PdfWriter as _TrimW
            _tr = _TrimR(source_pdf)
            if len(_tr.pages) > _fill_pages:
                _tw = _TrimW()
                for _tpi in range(_fill_pages):
                    _tw.add_page(_tr.pages[_tpi])
                _trimmed_tmp = _tmpmod.NamedTemporaryFile(suffix=".pdf", delete=False)
                _tw.write(_trimmed_tmp)
                _trimmed_tmp.close()
                _fill_source = _trimmed_tmp.name
                log.info("fill_ams704: Trimmed source from %d to %d pages for form fill",
                         len(_tr.pages), _fill_pages)
        except Exception as _trim_e:
            log.debug("Source trimming failed (non-fatal): %s", _trim_e)

    # Fill the PDF (form fields handle pages 1-2)
    try:
        _fill_pdf_fields(_fill_source, field_values, output_pdf)
    except Exception as e:
        return {"ok": False, "error": f"PDF fill error: {e}"}
    finally:
        # Clean up temp trimmed source
        if _trimmed_tmp:
            try:
                os.unlink(_trimmed_tmp.name)
            except Exception:
                pass

    # Fix shared Page/of fields: overlay correct numbers on page 2+
    if _page_fields_are_shared and _pages_with_items >= 2 and os.path.exists(output_pdf):
        try:
            _fix_shared_page_numbers(output_pdf, source_pdf, _pages_with_items)
        except Exception as _pne:
            log.warning("fill_ams704: page number overlay failed: %s", _pne)

    # Pages 3+: append overflow pages with ALL content drawn by reportlab
    # Form fields only exist on pages 1-2. Pages 3+ need full overlay.
    if _max_item_row > _form_capacity and os.path.exists(output_pdf):
        try:
            _append_overflow_pages(
                output_pdf=output_pdf,
                items=items,
                field_values=field_values,
                source_pdf=source_pdf,
                form_capacity=_form_capacity,
                pg2_rows=_pg2_rows,
                pages_with_items=_pages_with_items,
                subtotal=subtotal,
                tax=tax,
                total=total,
                company_name=info.get("company_name", "Reytech Inc."),
            )
        except Exception as _oe:
            log.warning("fill_ams704: overflow page append failed (pages 3+ may be missing): %s", _oe, exc_info=True)

    log.info("fill_ams704 COMPLETE%s: %d/%d items priced, %d skipped(no row), %d skipped(no price), "
             "subtotal=$%.2f, %d field_values written to %s",
             " [ORIGINAL MODE]" if original_mode else "",
             items_priced, len(items), _skipped_no_row, _skipped_no_price,
             subtotal, len(field_values), os.path.basename(output_pdf))

    return {
        "ok": True,
        "output": output_pdf,
        "summary": {
            "items_total": len(items),
            "items_priced": items_priced,
            "subtotal": subtotal,
            "tax": tax,
            "total": total,
            "price_tier": price_tier,
        },
    }


def _fix_shared_page_numbers(output_pdf: str, source_pdf: str, pages_with_items: int):
    """Overlay correct page numbers on pages 2+ when Page/of are shared fields.

    Shared PDF form fields show the same value on all pages. The form fill sets
    Page="1" (correct for page 1), but page 2+ inherits "1" too. This function
    detects the Page/of field positions from the source PDF and draws the correct
    numbers via reportlab overlay on each continuation page.
    """
    import io
    from reportlab.pdfgen import canvas as rl_canvas

    reader = PdfReader(output_pdf)
    if len(reader.pages) < 2:
        return

    writer = PdfWriter()
    writer.append(reader)

    # Find Page and of field rects from source PDF page 2 (continuation page)
    src_reader = PdfReader(source_pdf)
    page_rect = None
    of_rect = None
    for annot_ref in (src_reader.pages[min(1, len(src_reader.pages) - 1)].get("/Annots") or []):
        try:
            obj = annot_ref.get_object()
            name = str(obj.get("/T", ""))
            rect = obj.get("/Rect")
            if not rect:
                continue
            r = [float(x) for x in rect]
            if name == "Page":
                page_rect = r
            elif name == "of":
                of_rect = r
        except Exception:
            pass

    if not page_rect and not of_rect:
        log.debug("_fix_shared_page_numbers: no Page/of rects found on page 2")
        return

    from pypdf.generic import NameObject, ArrayObject

    modified = False
    for pg_idx in range(1, len(writer.pages)):  # skip page 0 (correct already)
        pg_num = pg_idx + 1
        if pg_num > pages_with_items:
            break

        page = writer.pages[pg_idx]
        pw = float(page.mediabox.width)
        ph = float(page.mediabox.height)

        # Remove Page/of annotations from this page so the shared field "1"
        # doesn't render. The overlay will draw the correct number instead.
        annots = page.get("/Annots")
        if annots:
            cleaned = []
            for annot_ref in annots:
                try:
                    annot = annot_ref.get_object()
                    name = str(annot.get("/T", ""))
                    if name in ("Page", "of"):
                        continue  # strip this annotation
                    cleaned.append(annot_ref)
                except Exception:
                    cleaned.append(annot_ref)
            page[NameObject("/Annots")] = ArrayObject(cleaned)

        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(pw, ph))

        if page_rect:
            x1, y1, x2, y2 = page_rect
            c.setFillColorRGB(0, 0, 0)
            c.setFont("Helvetica", 12)
            c.drawRightString(x2 - 2, y1 + 2, str(pg_num))

        if of_rect:
            x1, y1, x2, y2 = of_rect
            c.setFillColorRGB(0, 0, 0)
            c.setFont("Helvetica", 12)
            c.drawRightString(x2 - 2, y1 + 2, str(pages_with_items))

        c.save()
        buf.seek(0)

        overlay = PdfReader(buf)
        if overlay.pages:
            page.merge_page(overlay.pages[0])
            modified = True

    if modified:
        with open(output_pdf, "wb") as f:
            writer.write(f)
        log.info("_fix_shared_page_numbers: overlaid correct page numbers on pages 2-%d", pages_with_items)


def _detect_row_y_positions(source_pdf: str, page_idx: int, suffix: str = ""):
    """Extract item row Y positions from PDF field /Rect annotations on a given page.

    Returns list of (y_bot, y_top) tuples sorted top-to-bottom, plus
    (price_x1, price_x2) and (ext_x1, ext_x2) column ranges.
    Falls back to empty list if no fields found.
    """
    rows = []
    price_x = None
    ext_x = None
    col_x = {}
    _suffix_pat = re.escape(suffix) if suffix else ""
    try:
        reader = PdfReader(source_pdf)
        if page_idx >= len(reader.pages):
            return rows, price_x, ext_x, col_x
        page = reader.pages[page_idx]
        for annot_ref in (page.get("/Annots") or []):
            try:
                obj = annot_ref.get_object()
                name = str(obj.get("/T", ""))
                rect = obj.get("/Rect")
                if not rect:
                    continue
                r = [float(x) for x in rect]

                # Match PRICE PER UNITRow{n}{suffix}
                if "PRICE PER UNIT" in name and "Row" in name:
                    m = re.search(r'Row(\d+)' + _suffix_pat + r'$', name)
                    if m:
                        rows.append((int(m.group(1)), r[1], r[3]))
                        if price_x is None:
                            price_x = (r[0], r[2])

                # Match EXTENSIONRow{n}{suffix}
                if "EXTENSION" in name and "Row" in name and "GRAND" not in name:
                    m = re.search(r'Row(\d+)' + _suffix_pat + r'$', name)
                    if m and ext_x is None:
                        ext_x = (r[0], r[2])

                # Collect column X positions from Row1 fields
                _row1_suffix = f"Row1{suffix}"
                if name.endswith(_row1_suffix):
                    if name.startswith("ITEM ") and "DESCRIPTION" not in name:
                        col_x["item"] = (r[0], r[2])
                    elif name.startswith("QTY") and "PER" not in name:
                        col_x["qty"] = (r[0], r[2])
                    elif "UNIT OF MEASURE" in name:
                        col_x["uom"] = (r[0], r[2])
                    elif "QTY PER" in name:
                        col_x["qpu"] = (r[0], r[2])
                    elif "DESCRIPTION" in name:
                        col_x["desc"] = (r[0], r[2])
            except Exception:
                continue
    except Exception as e:
        log.debug("_detect_row_y_positions: %s", e)

    # Sort by row number (ascending), which gives us top-to-bottom in PDF coords
    rows.sort(key=lambda r: r[0])
    result = [(y_bot, y_top) for _, y_bot, y_top in rows]
    return result, price_x, ext_x, col_x


def _append_overflow_pages(
    output_pdf: str,
    items: list,
    field_values: list,
    source_pdf: str,
    form_capacity: int,
    pg2_rows: int,
    pages_with_items: int,
    subtotal: float,
    tax: float,
    total: float,
    company_name: str = "Reytech Inc.",
):
    """Append pages 3+ to a filled PDF for items beyond form field capacity.

    Pages 1-2 are filled via form fields. Pages 3+ have no form fields,
    so we create new pages using the continuation page as background and
    draw ALL content via reportlab canvas overlay.
    """
    import io
    import copy
    from reportlab.pdfgen import canvas as rl_canvas

    # Detect row positions from page 2 of source (continuation page layout)
    cont_rows, price_x, ext_x, col_x = _detect_row_y_positions(source_pdf, 1, "_2")
    if not cont_rows:
        log.warning("_append_overflow_pages: no row positions detected on page 2 — cannot create overflow pages")
        return

    # Fallback column positions
    col_x.setdefault("item", (32.2, 62.3))
    col_x.setdefault("qty", (64.1, 98.3))
    col_x.setdefault("uom", (100.1, 152.3))
    col_x.setdefault("qpu", (154.1, 197.3))
    col_x.setdefault("desc", (199.1, 580.0))
    if not price_x:
        price_x = (637.0, 686.0)
    if not ext_x:
        ext_x = (691.0, 754.0)

    log.info("_append_overflow_pages: cont_rows=%d, price_x=%s, ext_x=%s",
             len(cont_rows), price_x, ext_x)

    # Collect overflow items (beyond form capacity)
    overflow_items = []
    for idx, item in enumerate(items):
        row_idx = item.get("row_index") or (idx + 1)
        if row_idx > form_capacity:
            overflow_items.append((row_idx, item))
    if not overflow_items:
        return

    # Read the filled output and source template
    reader_out = PdfReader(output_pdf)
    writer = PdfWriter()
    for p in reader_out.pages:
        writer.add_page(p)

    reader_tmpl = PdfReader(source_pdf)
    # Use last page (continuation) as template background
    cont_tmpl_idx = min(1, len(reader_tmpl.pages) - 1)
    cont_page_tmpl = reader_tmpl.pages[cont_tmpl_idx]
    pw = float(cont_page_tmpl.mediabox.width)
    ph = float(cont_page_tmpl.mediabox.height)

    items_per_page = min(len(cont_rows), pg2_rows)
    page_num = 3  # Overflow starts at page 3

    for chunk_start in range(0, len(overflow_items), items_per_page):
        chunk = overflow_items[chunk_start:chunk_start + items_per_page]

        # Create reportlab overlay
        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(pw, ph))

        # Supplier name
        c.setFont("Helvetica", 12)
        c.drawString(340, 530, company_name)

        # Page number — detect position from cont page annotations or use fallback
        c.setFont("Helvetica", 12)
        c.drawString(690, 555, str(page_num))
        c.drawString(735, 555, str(pages_with_items))

        # Draw each item
        for slot_idx, (row_idx, item) in enumerate(chunk):
            if slot_idx >= len(cont_rows):
                break
            y_bot, y_top = cont_rows[slot_idx]
            y_mid = y_bot + (y_top - y_bot) * 0.3  # text baseline

            p = item.get("pricing") or {}
            desc = item.get("description", "")
            qty = str(item.get("qty", 1))
            uom = (item.get("uom") or "EA").upper()
            qpu = str(item.get("qty_per_uom", 1))
            price = item.get("unit_price") or p.get("recommended_price") or 0
            try:
                price = float(price)
            except (ValueError, TypeError):
                price = 0
            ext = round(price * float(item.get("qty", 1)), 2) if price else 0

            # Item number
            c.setFont("Helvetica", 10)
            x1, x2 = col_x["item"]
            c.drawRightString(x2 - 2, y_mid, str(row_idx))

            # QTY
            x1, x2 = col_x["qty"]
            c.drawRightString(x2 - 2, y_mid, qty)

            # UOM
            x1, x2 = col_x["uom"]
            c.drawString(x1 + 2, y_mid, uom)

            # QPU
            x1, x2 = col_x["qpu"]
            c.drawRightString(x2 - 2, y_mid, qpu)

            # Description (auto-fit font)
            x1, x2 = col_x["desc"]
            fs = 9
            c.setFont("Helvetica", fs)
            desc_trunc = desc[:100]
            while c.stringWidth(desc_trunc, "Helvetica", fs) > (x2 - x1 - 4) and fs > 5:
                fs -= 0.5
                c.setFont("Helvetica", fs)
            c.drawString(x1 + 2, y_mid, desc_trunc)

            # Price
            if price > 0:
                px1, px2 = price_x
                c.setFont("Helvetica", 9)
                c.drawRightString(px2 - 2, y_mid, f"{price:,.2f}")

            # Extension
            if ext > 0:
                ex1, ex2 = ext_x
                c.setFont("Helvetica", 9)
                c.drawRightString(ex2 - 2, y_mid, f"{ext:,.2f}")

        # Grand total on the LAST overflow page
        is_last_page = (chunk_start + items_per_page >= len(overflow_items))
        if is_last_page:
            ex1, ex2 = ext_x
            c.setFont("Helvetica", 10)
            c.drawRightString(ex2 - 2, 107, f"{total:,.2f}")

        c.save()
        buf.seek(0)

        # Create new page from continuation template + overlay
        new_page = copy.deepcopy(cont_page_tmpl)
        # Strip form field annotations to prevent shared field corruption
        if "/Annots" in new_page:
            del new_page["/Annots"]

        overlay_reader = PdfReader(buf)
        if overlay_reader.pages:
            new_page.merge_page(overlay_reader.pages[0])

        writer.add_page(new_page)
        page_num += 1

    # Write final multi-page PDF
    with open(output_pdf, "wb") as f:
        writer.write(f)

    log.info("_append_overflow_pages: appended %d overflow page(s) for %d items beyond form capacity",
             page_num - 3, len(overflow_items))


def merge_bundle_pdfs(
    source_pdf: str,
    pc_outputs: list,
    non_pc_pages: list,
    output_pdf: str,
) -> dict:
    """Merge individually-generated PC PDFs back into one combined response PDF,
    preserving original page ordering and including non-PC pages (purchase justifications).

    Args:
        source_pdf: Path to original combined source PDF
        pc_outputs: List of {"page_start": int, "page_end": int, "output_pdf": str}
                    sorted by page_start. Each output_pdf contains only that PC's pages.
        non_pc_pages: List of int page indices for non-PC pages (0-indexed)
        output_pdf: Output path for merged bundle PDF
    Returns:
        {"ok": bool, "output": str, "page_count": int} or {"ok": False, "error": "..."}
    """
    if not HAS_PYPDF:
        return {"ok": False, "error": "pypdf not available"}

    if not os.path.exists(source_pdf):
        return {"ok": False, "error": f"Source PDF not found: {source_pdf}"}

    try:
        from pypdf import PdfReader, PdfWriter

        source_reader = PdfReader(source_pdf)
        total_pages = len(source_reader.pages)
        non_pc_set = set(non_pc_pages or [])

        # Build a map: page_index → which PC output covers it
        page_to_pc = {}
        for pc_out in pc_outputs:
            ps = int(pc_out["page_start"])
            pe = int(pc_out["page_end"])
            out_path = pc_out["output_pdf"]
            if not os.path.exists(out_path):
                log.warning("merge_bundle_pdfs: PC output missing: %s", out_path)
                continue
            for pi in range(ps, pe + 1):
                page_to_pc[pi] = {
                    "output_pdf": out_path,
                    "offset": pi - ps,  # page index within the generated PDF
                }

        # Pre-load all generated PC PDF readers
        _readers = {}
        for pc_out in pc_outputs:
            out_path = pc_out["output_pdf"]
            if out_path not in _readers and os.path.exists(out_path):
                _readers[out_path] = PdfReader(out_path)

        writer = PdfWriter()
        pages_written = 0

        for page_idx in range(total_pages):
            if page_idx in non_pc_set:
                # Non-PC page (purchase justification) — copy from original source
                writer.add_page(source_reader.pages[page_idx])
                pages_written += 1
            elif page_idx in page_to_pc:
                # PC page — use the generated/filled version
                pc_info = page_to_pc[page_idx]
                reader = _readers.get(pc_info["output_pdf"])
                if reader and pc_info["offset"] < len(reader.pages):
                    writer.add_page(reader.pages[pc_info["offset"]])
                    pages_written += 1
                else:
                    # Fallback: use original page if generated page missing
                    log.warning("merge_bundle_pdfs: page %d — generated page missing, using original", page_idx)
                    writer.add_page(source_reader.pages[page_idx])
                    pages_written += 1
            else:
                # Page not claimed by any PC and not in non_pc_pages
                # (e.g., blank overflow pages between PCs) — include from original
                writer.add_page(source_reader.pages[page_idx])
                pages_written += 1

        os.makedirs(os.path.dirname(output_pdf) or ".", exist_ok=True)
        with open(output_pdf, "wb") as f:
            writer.write(f)

        log.info("merge_bundle_pdfs: merged %d pages → %s (%d PC sections, %d non-PC pages)",
                 pages_written, os.path.basename(output_pdf), len(pc_outputs), len(non_pc_set))

        return {"ok": True, "output": output_pdf, "page_count": pages_written}

    except Exception as e:
        log.error("merge_bundle_pdfs failed: %s", e, exc_info=True)
        return {"ok": False, "error": f"PDF merge error: {e}"}


def _detect_ams704_overlay_positions(source_pdf):
    """Use pdfplumber to detect item table layout for AMS 704 overlay.

    Scans each page for:
    - Item table rows (Y boundaries for each item row)
    - PRICE PER UNIT and EXTENSION column X boundaries
    - Supplier info cell positions (page 1)
    - Totals area (page 1)

    All returned coordinates are in reportlab space (origin bottom-left).
    Returns list of per-page dicts, or None if pdfplumber unavailable.
    """
    try:
        import pdfplumber
    except ImportError:
        log.warning("pdfplumber not available — cannot auto-detect overlay positions")
        return None

    try:
        pdf = pdfplumber.open(source_pdf)
    except Exception as e:
        log.warning("pdfplumber failed to open %s: %s", source_pdf, e)
        return None

    pages = []
    for pg_idx, page in enumerate(pdf.pages):
        ph = float(page.height)
        pw = float(page.width)

        def _to_rl_y(plumber_y):
            return ph - plumber_y

        info = {
            "item_rows": [],      # (y_bot, y_top) for price sub-row only
            "desc_tops": [],      # rl y_top of full cell (for QTY/UOM positioning)
            "orig_values": [],    # buyer's original {qty, uom, qpu} per row
            "price_x": None,
            "ext_x": None,
            "supplier_cells": {},
            "totals_cells": {},
            "notes_area": None,
            "fob_area": None,
            "ship_to_area": None,
            "supplier_name_cont": None,
            "pw": pw, "ph": ph,
        }

        words = page.extract_words(keep_blank_chars=False, x_tolerance=3, y_tolerance=3)
        edges = page.edges

        # ── Find EXTENSION header (unique, always in rightmost area) ──
        ext_header = None
        for w in words:
            if w["text"].upper().strip() == "EXTENSION" and w["x0"] > pw * 0.6:
                ext_header = w
                break

        if not ext_header:
            log.info("OVERLAY detect pg%d: no EXTENSION header found", pg_idx)
            pages.append(None)
            continue

        # ── Find PRICE column header near EXTENSION but to its left ──
        # "PRICE PER UNIT" may be split as "PRICE"/"PER"/"UNIT" or even
        # "PRIC"/"E"/"PER"/"UNIT" in DOCX-converted PDFs.
        # Strategy: look for "PRIC" or "PRICE" near EXTENSION, with wider Y tolerance.
        price_header = None
        for w in words:
            wt = w["text"].upper().strip()
            if wt in ("PRICE", "PRIC") and w["x0"] > pw * 0.6:
                # Must be near EXTENSION Y (within 80pt for DOCX layouts) and to its left
                if abs(w["top"] - ext_header["top"]) < 80 and w["x0"] < ext_header["x0"]:
                    price_header = w
                    break

        if not price_header:
            log.info("OVERLAY detect pg%d: no PRICE header found near EXTENSION", pg_idx)
            pages.append(None)
            continue

        # ── Column X boundaries ──
        # Best source: horizontal rects that form the PRICE/EXTENSION column headers.
        # These give exact cell boundaries. Fallback to v-edges, then text estimation.
        p_left = p_right = e_right = None

        # Method 1: Find horizontal rects that form PRICE/EXTENSION cell boundaries.
        # These are thin horizontal rects (h<5) with width 40-80pt in the right area.
        # PRICE rects have width ~50pt, EXTENSION rects have width ~68pt, separated
        # by a small gap (the border line between columns).
        # Filter to rects near the column header Y to avoid picking up totals/other areas.
        _header_y = ext_header["top"]
        col_rects = [r for r in page.rects
                     if r["x0"] > pw * 0.75 and r["width"] > 30 and r["width"] < 100
                     and r["height"] < 5
                     and abs(r["top"] - _header_y) < 40]
        if col_rects:
            # Group by x0 (rects with same x0 belong to same column)
            x0_groups = {}
            for r in col_rects:
                key = round(r["x0"], 0)
                if key not in x0_groups:
                    x0_groups[key] = r
            unique_x0s = sorted(x0_groups.keys())
            if len(unique_x0s) >= 2:
                price_rect = x0_groups[unique_x0s[0]]  # leftmost = PRICE
                ext_rect = x0_groups[unique_x0s[1]]    # next = EXTENSION
                p_left = price_rect["x0"]
                p_right = price_rect["x1"]
                e_right = ext_rect["x1"]
                log.info("OVERLAY detect pg%d: columns from rects: PRICE=(%.1f,%.1f) EXT=(%.1f,%.1f)",
                         pg_idx, p_left, p_right, ext_rect["x0"], e_right)

        # Method 2: vertical edges
        if p_left is None:
            v_edges_x = sorted(set(
                round(e["x0"], 0) for e in edges
                if abs(e["x0"] - e["x1"]) < 2 and (e["bottom"] - e["top"]) > 30
            ))
            if v_edges_x:
                price_v_left = [v for v in v_edges_x if v <= price_header["x0"] + 5]
                price_v_right = [v for v in v_edges_x if abs(v - ext_header["x0"]) < 15]
                p_left = max(price_v_left) if price_v_left else None
                p_right = min(price_v_right) if price_v_right else None
                ext_right_v = [v for v in v_edges_x if v > ext_header["x1"] + 5]
                e_right = min(ext_right_v) if ext_right_v else None

        # Method 3: text estimation (least accurate)
        if p_left is None:
            p_left = price_header["x0"] - 12
        if p_right is None:
            p_right = ext_header["x0"] - 8
        if e_right is None:
            e_right = ext_header["x1"] + 15

        # Inset 3pt from actual borders to avoid masking border lines
        info["price_x"] = (p_left + 3, p_right - 3)
        info["ext_x"] = (p_right + 3, e_right - 3)

        log.info("OVERLAY detect pg%d: price_x=(%.1f,%.1f) ext_x=(%.1f,%.1f)",
                 pg_idx, info["price_x"][0], info["price_x"][1],
                 info["ext_x"][0], info["ext_x"][1])

        # ── Find all horizontal lines ──
        all_h_lines = sorted(set(
            round(e["top"], 1) for e in edges
            if abs(e["top"] - e["bottom"]) < 2
            and e["x1"] - e["x0"] > pw * 0.3
        ))

        # Group consecutive h-lines within 3pt into single boundaries
        grouped_h = []
        for y in all_h_lines:
            if not grouped_h or y - grouped_h[-1] > 3:
                grouped_h.append(y)
            else:
                grouped_h[-1] = y  # keep the lower line of the pair

        # ── Find item numbers in the leftmost column ──
        # Only accept numbers below the column headers (ext_header bottom + margin)
        _item_y_min = ext_header["bottom"] + 3
        item_positions = []  # [(item_num, pdfplumber_y_top), ...]
        for w in words:
            t = w["text"].strip()
            if (t.isdigit() and 1 <= int(t) <= 50
                    and w["x0"] < pw * 0.07
                    and w["top"] > _item_y_min):  # must be below column headers
                item_positions.append((int(t), w["top"]))
        item_positions.sort(key=lambda x: x[1])  # sort by Y position

        if not item_positions:
            log.info("OVERLAY detect pg%d: no item numbers found", pg_idx)
            pages.append(None)
            continue

        # ── Map each item to its cell boundaries ──
        # For each item, cell_top = h-line just above item text
        # Cell_bottom = cell_top of next item, or table end for last item
        item_cell_tops = []
        for _inum, _iy in item_positions:
            candidates = [h for h in grouped_h if h <= _iy + 2]
            cell_top = max(candidates) if candidates else _iy - 5
            item_cell_tops.append(cell_top)

        # Find table bottom: last grouped_h line after all items
        last_item_y = item_positions[-1][1]
        table_bottom_candidates = [h for h in grouped_h if h > last_item_y + 20]
        # Take the second line after the last item (first is sub-row divider, second is cell bottom)
        if len(table_bottom_candidates) >= 2:
            table_bottom = table_bottom_candidates[1]
        elif table_bottom_candidates:
            table_bottom = table_bottom_candidates[0]
        else:
            table_bottom = last_item_y + 50

        # Build item row boundaries — use only the PRICE sub-row (lower half)
        # Each item cell has upper (description) and lower (price) sub-rows
        # separated by an h-line. We return only the price sub-row so that
        # prices are vertically centered in the correct ~22pt band, not the
        # full ~44pt cell.
        for i in range(len(item_cell_tops)):
            cell_top_pl = item_cell_tops[i]
            if i + 1 < len(item_cell_tops):
                cell_bot_pl = item_cell_tops[i + 1]
            else:
                cell_bot_pl = table_bottom

            # Find h-line sub-divider(s) within this cell
            sub_dividers = [h for h in grouped_h
                            if cell_top_pl + 5 < h < cell_bot_pl - 5]
            if sub_dividers:
                # Last sub-divider = top of price sub-row
                price_top_pl = max(sub_dividers)
            else:
                # No sub-divider found — use bottom half as fallback
                price_top_pl = (cell_top_pl + cell_bot_pl) / 2

            rl_y_top = _to_rl_y(price_top_pl) - 1
            rl_y_bot = _to_rl_y(cell_bot_pl) + 1
            rl_desc_top = _to_rl_y(cell_top_pl) - 1  # full cell top for QTY
            if rl_y_top > rl_y_bot:
                info["item_rows"].append((rl_y_bot, rl_y_top))
                info["desc_tops"].append(rl_desc_top)

                # Extract buyer's original QTY/UOM/QPU text from description sub-row
                # These sit on the same Y-line as the item number (within 4pt)
                _item_y = item_positions[i][1]
                _orig = {}
                for _w in words:
                    if abs(_w["top"] - _item_y) > 4:
                        continue
                    _wx0 = _w["x0"]
                    _wt = _w["text"].strip()
                    # QTY column: numeric only, ~x 63-103
                    if pw * 0.08 <= _wx0 < pw * 0.13 and _wt.isdigit():
                        _orig["qty"] = _wt
                    # UOM column: short alpha text (EA, Pck, Pkg, etc.), ~x 103-158
                    elif pw * 0.13 <= _wx0 < pw * 0.20 and len(_wt) <= 5 and not _wt.isdigit():
                        _orig["uom"] = _wt
                    # QPU column: numeric only, ~x 158-214
                    elif pw * 0.20 <= _wx0 < pw * 0.27 and _wt.replace(".", "").isdigit():
                        _orig["qpu"] = _wt
                info["orig_values"].append(_orig)

        log.info("OVERLAY detect pg%d: %d items → rows: %s",
                 pg_idx, len(info["item_rows"]),
                 [(f"{yb:.0f}-{yt:.0f}") for yb, yt in info["item_rows"]])

        # ── Supplier info detection (page 1 only) ──
        # For converted DOCXs without vertical edges in the supplier section,
        # supplier fields use scaled hardcoded positions. Detection here is
        # best-effort — the overlay function falls back to hardcoded if empty.
        if pg_idx == 0:
            # Find "COMPANY NAME" label in supplier section
            for w in words:
                if w["text"].upper().strip() == "COMPANY" and w["x0"] < pw * 0.25:
                    nearby = [w2 for w2 in words
                              if abs(w2["top"] - w["top"]) < 5
                              and w2["x0"] > w["x1"] - 5]
                    if any("NAME" in w2["text"].upper() for w2 in nearby):
                        # Found COMPANY NAME label. Supplier data cells are below it.
                        # Find h-lines that form the supplier section grid
                        # Use w["top"] (not bottom) and wider range for DOCX layouts
                        sup_area_y = w["top"]
                        sup_h = sorted([h for h in grouped_h
                                        if sup_area_y - 15 <= h <= sup_area_y + 120])
                        if len(sup_h) >= 4:
                            # 3 rows of supplier info between consecutive h-lines
                            for ri in range(min(3, len(sup_h) - 1)):
                                r_top_pl = sup_h[ri]
                                r_bot_pl = sup_h[ri + 1]
                                # Row 1: COMPANY NAME | REPRESENTATIVE | DELIVERY
                                # Row 2: Address | Signature | Discount
                                # Row 3: SB/MB | DVBE | Phone | Email | Expires
                                rl_top = _to_rl_y(r_top_pl) - 1
                                rl_bot = _to_rl_y(r_bot_pl) + 1
                                if ri == 0:
                                    # Split into 3 cells using proportional widths
                                    w1 = pw * 0.35
                                    w2 = pw * 0.76
                                    info["supplier_cells"]["COMPANY NAME"] = (
                                        pw * 0.04, rl_bot, w1, rl_top)
                                    info["supplier_cells"]["COMPANY REPRESENTATIVE print name"] = (
                                        w1 + 2, rl_bot, w2, rl_top)
                                    info["supplier_cells"]["Delivery Date and Time ARO"] = (
                                        w2 + 2, rl_bot, pw * 0.96, rl_top)
                                elif ri == 1:
                                    info["supplier_cells"]["Address"] = (
                                        pw * 0.04, rl_bot, pw * 0.35, rl_top)
                                    info["supplier_cells"]["Discount Offered"] = (
                                        pw * 0.76 + 2, rl_bot, pw * 0.96, rl_top)
                                elif ri == 2:
                                    # 5 cells in row 3
                                    xs = [pw*0.04, pw*0.20, pw*0.35, pw*0.56, pw*0.76, pw*0.96]
                                    fields_r3 = ["Certified SBMB", "Certified DVBE",
                                                 "Phone Number_2", "EMail Address",
                                                 "Date Price Check Expires"]
                                    for fi, fname in enumerate(fields_r3):
                                        info["supplier_cells"][fname] = (
                                            xs[fi], rl_bot, xs[fi + 1] - 2, rl_top)
                        break

            # Totals: look for "$" signs in the bottom-right quadrant
            dollar_words = [w for w in words
                            if w["text"].strip() == "$"
                            and w["x0"] > pw * 0.85
                            and w["top"] > ph * 0.65]
            dollar_words.sort(key=lambda w: w["top"])
            if len(dollar_words) >= 4:
                total_ids = ["fill_70", "fill_71", "fill_72", "fill_73"]
                for ti, (dw, fid) in enumerate(zip(dollar_words[:4], total_ids)):
                    row_h = sorted([h for h in grouped_h
                                    if abs(h - dw["top"]) < 25 or abs(h - dw["bottom"]) < 25])
                    if len(row_h) >= 2:
                        rl_top = _to_rl_y(row_h[0]) - 1
                        rl_bot = _to_rl_y(row_h[-1]) + 1
                    else:
                        rl_top = _to_rl_y(dw["top"]) + 2
                        rl_bot = _to_rl_y(dw["bottom"]) - 2
                    info["totals_cells"][fid] = (
                        dw["x0"] + 8, rl_bot, pw - 10, rl_top)

            # Fallback: position totals using "$" sign positions as anchors
            if len(info["totals_cells"]) < 4:
                _dollar_signs = sorted(
                    [w for w in words if w["text"].strip() == "$"
                     and w["x0"] > pw * 0.85 and w["top"] > ph * 0.7],
                    key=lambda w: w["top"])
                if _dollar_signs:
                    # Value column: right of "$" to rightmost vertical line
                    _left_x = max(d["x1"] for d in _dollar_signs) + 2
                    _v_right = sorted([v for v in set(
                        round(e["x0"], 0) for e in edges
                        if abs(e["x0"] - e["x1"]) < 2 and (e["bottom"] - e["top"]) > 30
                    ) if v > pw * 0.9], reverse=True)
                    _right_x = _v_right[0] - 2 if _v_right else pw - 35
                    # Use h-lines to bound the totals area, then divide by 4
                    # The totals area spans from the last thin-row-skip to the last h-line
                    _totals_area_h = sorted([h for h in grouped_h if h > ph * 0.8])
                    if len(_totals_area_h) >= 2:
                        # Skip thin leading border, use the wider span
                        while len(_totals_area_h) >= 2 and (_totals_area_h[1] - _totals_area_h[0]) < 12:
                            _totals_area_h.pop(0)
                        _area_top = _totals_area_h[0] if _totals_area_h else _dollar_signs[0]["top"] - 5
                        _area_bot = _totals_area_h[-1] if _totals_area_h else _dollar_signs[0]["top"] + 55
                    else:
                        _area_top = _dollar_signs[0]["top"] - 5
                        _area_bot = _area_top + 55
                    _row_h = (_area_bot - _area_top) / 4
                    total_ids = ["fill_70", "fill_71", "fill_72", "fill_73"]
                    for ti in range(4):
                        _row_y = _area_top + (ti * _row_h)
                        rl_top = _to_rl_y(_row_y) + 1
                        rl_bot = _to_rl_y(_row_y + _row_h) + 1
                        info["totals_cells"][total_ids[ti]] = (
                            _left_x, rl_bot, _right_x, rl_top)
                    log.info("OVERLAY detect pg%d: totals: area=%.1f-%.1f row_h=%.1f cells=%s",
                             pg_idx, _area_top, _area_bot, _row_h,
                             [(f"{v[1]:.0f}-{v[3]:.0f}") for v in info["totals_cells"].values()])

            # FOB checkbox — find the checkbox character (\uf06f) or small rect
            # near the first "FOB" text in the footer area
            _fob_checkbox = None
            for w in words:
                if w["text"] == "\uf06f" and w["top"] > ph * 0.7 and w["x0"] < pw * 0.4:
                    _fob_checkbox = w
                    break
            if _fob_checkbox:
                # Checkbox character found — draw X centered on it
                _cb_cx = (_fob_checkbox["x0"] + _fob_checkbox["x1"]) / 2
                _cb_cy = (_fob_checkbox["top"] + _fob_checkbox["bottom"]) / 2
                _cb_sz = 6  # half-size of the X mark
                info["fob_area"] = (
                    _cb_cx - _cb_sz, _to_rl_y(_cb_cy + _cb_sz),
                    _cb_cx + _cb_sz, _to_rl_y(_cb_cy - _cb_sz))
            else:
                # Fallback: position relative to first "FOB" word
                for w in words:
                    if w["text"].upper().strip() == "FOB" and w["x0"] > pw * 0.2 and w["top"] > ph * 0.7:
                        fob_cy = (w["top"] + w["bottom"]) / 2
                        info["fob_area"] = (
                            w["x0"] - 18, _to_rl_y(fob_cy + 7),
                            w["x0"] - 3,  _to_rl_y(fob_cy - 7))
                        break

            # Ship to
            for w in words:
                if w["text"].strip().lower() == "ship" and w["x0"] > pw * 0.25:
                    # Check if next word is "to"
                    nearby_to = [w2 for w2 in words
                                 if w2["text"].strip().lower().rstrip(":") == "to"
                                 and abs(w2["top"] - w["top"]) < 5
                                 and w2["x0"] > w["x1"] - 5]
                    if nearby_to:
                        to_w = nearby_to[0]
                        info["ship_to_area"] = (
                            to_w["x1"] + 5, _to_rl_y(w["bottom"]) - 1,
                            pw * 0.7,        _to_rl_y(w["top"]) + 1)
                    break

            # Notes area
            for w in words:
                if w["text"].upper().strip() == "SUPPLIER" and w["top"] > ph * 0.65:
                    nearby_notes = [w2 for w2 in words
                                    if abs(w2["top"] - w["top"]) < 5
                                    and "NOTES" in w2["text"].upper()]
                    if nearby_notes:
                        info["notes_area"] = (
                            w["x0"], _to_rl_y(w["top"] + 60),
                            pw * 0.35, _to_rl_y(w["bottom"] + 5))
                    break

        else:
            # ── Continuation page: supplier name area ──
            for w in words:
                if w["text"].upper().strip() == "SUPPLIER" and w["top"] < ph * 0.15:
                    nearby_name = [w2 for w2 in words
                                   if abs(w2["top"] - w["top"]) < 5
                                   and "NAME" in w2["text"].upper()]
                    if nearby_name:
                        name_w = nearby_name[0]
                        info["supplier_name_cont"] = (
                            name_w["x1"] + 10, _to_rl_y(name_w["bottom"]) - 1,
                            pw - 20, _to_rl_y(name_w["top"]) + 1)
                    break

        pages.append(info)

    pdf.close()
    return pages


def _fill_pdf_text_overlay(source_pdf: str, field_values: list, output_pdf: str):
    """
    Fallback for flat/DocuSign PDFs with no fillable form fields.
    Uses pdfplumber to auto-detect item table positions when possible.
    Falls back to hardcoded coordinates from CCHCS DocuSign AMS 704.
    ONLY draws: supplier info + pricing + totals. Never touches item descriptions.
    """
    import io
    import re as _re
    from reportlab.pdfgen import canvas as rl_canvas

    reader = PdfReader(source_pdf)
    writer = PdfWriter()
    writer.append(reader)

    fv_map = {fv["field_id"]: fv.get("value", "") for fv in field_values}

    # ── Try pdfplumber auto-detection first ──
    detected = _detect_ams704_overlay_positions(source_pdf)
    _using_detected = detected is not None and any(d is not None for d in detected)
    if _using_detected:
        log.info("OVERLAY: using pdfplumber-detected layout (%d pages detected)", len(detected))
    else:
        log.info("OVERLAY: using hardcoded DocuSign layout (detection unavailable or failed)")

    # ═══ HARDCODED FALLBACK from CCHCS DocuSign AMS 704 (792×612 landscape) ═══
    _HC_SUPPLIER = {
        "COMPANY NAME":                       (33.1, 421.3, 278.3, 441.4),
        "COMPANY REPRESENTATIVE print name":  (280.1, 421.3, 602.4, 441.4),
        "Delivery Date and Time ARO":         (604.3, 421.3, 754.9, 441.4),
        "Address":                            (33.1, 389.9, 278.3, 412.1),
        "Discount Offered":                   (604.3, 389.9, 754.9, 412.1),
        "Certified SBMB":                     (33.0, 362.8, 156.8, 380.8),
        "Certified DVBE":                     (158.4, 362.8, 278.4, 380.8),
        "Phone Number_2":                     (280.0, 362.8, 445.0, 380.8),
        "EMail Address":                      (446.5, 362.8, 602.5, 380.8),
        "Date Price Check Expires":           (604.2, 362.8, 755.0, 380.8),
        "Ship to":                            (278.0, 101.0, 530.0, 114.0),
    }
    _HC_LABELED = {"Certified SBMB", "Certified DVBE", "Phone Number_2",
                   "EMail Address", "Date Price Check Expires"}
    _HC_NOTES = ("Supplier andor Requestor Notes", 32.6, 41.9, 237.2, 118.9)
    _HC_TOTALS = {
        "fill_70": (696.0, 141.0, 758.0, 159.0),
        "fill_71": (694.9, 117.5, 758.0, 138.0),
        "fill_72": (685.0, 93.6,  758.0, 115.5),
        "fill_73": (695.5, 69.0,  758.0, 92.0),
    }
    _HC_CHECKBOX = ("Check Box4", 241.0, 127.5, 256.0, 139.0)
    _HC_PRICE_X = (637.0, 686.0)
    _HC_EXT_X = (691.0, 754.0)
    _HC_QTY_X = (70.0, 95.0)
    _HC_UOM_X = (107.0, 148.0)
    _HC_QPU_X = (158.0, 192.0)
    _HC_PG1_ROWS = [(292.0, 311.5), (237.5, 257.5), (192.5, 212.5)]
    _HC_PG2_ROWS = [(457.5, 484.0), (399.0, 425.5), (340.5, 367.0), (288.7, 308.5), (235.0, 263.5)]
    _HC_PG2_SUPPLIER = (330.0, 523.0, 760.0, 550.0)

    _PAD = 4

    def _cell(c, x1, y1, x2, y2, text, fs=9, mask_top_pct=1.0):
        if not text or not text.strip():
            return
        text = text.strip()
        w, h = x2 - x1, y2 - y1
        if w <= 0 or h <= 0:
            return
        c.saveState()
        if mask_top_pct < 1.0:
            c.setFont("Helvetica", fs)
            c.setFillColorRGB(0, 0, 0)
            while c.stringWidth(text, "Helvetica", fs) > w - 8 and fs > 5.5:
                fs -= 0.5
                c.setFont("Helvetica", fs)
            c.drawString(x1 + 4, y1 + 1, text)
        else:
            p = c.beginPath()
            p.rect(x1 + _PAD, y1 + _PAD, w - _PAD * 2, h - _PAD * 2)
            c.clipPath(p, stroke=0)
            c.setFillColorRGB(1, 1, 1)
            c.rect(x1 + _PAD, y1 + _PAD, w - _PAD * 2, h - _PAD * 2, fill=1, stroke=0)
            fs = min(fs, h * 0.75)
            c.setFont("Helvetica", fs)
            c.setFillColorRGB(0, 0, 0)
            while c.stringWidth(text, "Helvetica", fs) > w - _PAD * 2 - 4 and fs > 4.5:
                fs -= 0.5
                c.setFont("Helvetica", fs)
            c.drawString(x1 + _PAD + 1, y1 + (h - fs) / 2, text)
        c.restoreState()

    def _cell_right(c, x1, y1, x2, y2, text, fs=9, mask=True):
        """Draw RIGHT-ALIGNED text. mask=False skips white background (for empty cells)."""
        if not text or not text.strip():
            return
        text = text.strip()
        w, h = x2 - x1, y2 - y1
        if w <= 0 or h <= 0:
            return
        c.saveState()
        p = c.beginPath()
        p.rect(x1 + _PAD, y1 + _PAD, w - _PAD * 2, h - _PAD * 2)
        c.clipPath(p, stroke=0)
        if mask:
            c.setFillColorRGB(1, 1, 1)
            c.rect(x1 + _PAD, y1 + _PAD, w - _PAD * 2, h - _PAD * 2, fill=1, stroke=0)
        fs = min(fs, h * 0.75)
        c.setFont("Helvetica", fs)
        c.setFillColorRGB(0, 0, 0)
        while c.stringWidth(text, "Helvetica", fs) > w - _PAD * 2 - 4 and fs > 4.5:
            fs -= 0.5
            c.setFont("Helvetica", fs)
        text_w = c.stringWidth(text, "Helvetica", fs)
        c.drawString(x2 - _PAD - text_w - 1, y1 + (h - fs) / 2, text)
        c.restoreState()

    def _multiline(c, x1, y1, x2, y2, text, fs=8):
        if not text or not text.strip():
            return
        w, h = x2 - x1, y2 - y1
        c.saveState()
        p = c.beginPath()
        p.rect(x1 + _PAD, y1 + _PAD, w - _PAD * 2, h - _PAD * 2)
        c.clipPath(p, stroke=0)
        c.setFillColorRGB(1, 1, 1)
        c.rect(x1 + _PAD, y1 + _PAD, w - _PAD * 2, h - _PAD * 2, fill=1, stroke=0)
        fs = min(fs, h * 0.6)
        c.setFont("Helvetica", fs)
        c.setFillColorRGB(0, 0, 0)
        for i, line in enumerate(text.strip().split("\n")[:5]):
            ly = y2 - _PAD - (fs + 2) - (i * (fs + 1.5))
            if ly < y1 + _PAD:
                break
            t = line.strip()
            while c.stringWidth(t, "Helvetica", fs) > w - _PAD * 2 - 4 and len(t) > 3:
                t = t[:-1]
            c.drawString(x1 + _PAD + 1, ly, t)
        c.restoreState()

    # Find highest priced row to skip empty trailing pages
    max_row = 0
    for fv in field_values:
        m = _re.search(r'Row(\d+)', fv["field_id"])
        if m and fv.get("value", "").strip():
            max_row = max(max_row, int(m.group(1)))

    num_pages = len(reader.pages)
    current_row = 1

    # Pre-compute per-page row counts for detected layout
    _detected_rows_per_page = []
    if _using_detected:
        for d in detected:
            _detected_rows_per_page.append(len(d["item_rows"]) if d else 0)

    log.info("OVERLAY: %d pages, max_row=%d, %d field_values, detected=%s",
             num_pages, max_row, len(field_values), _using_detected)

    for pg_idx in range(num_pages):
        page = reader.pages[pg_idx]
        mb = page.mediabox
        pw, ph = float(mb.width), float(mb.height)

        is_pg1 = (pg_idx == 0)

        # ── Determine layout source for this page ──
        pg_detected = (detected[pg_idx] if _using_detected and pg_idx < len(detected) else None)

        if pg_detected:
            # Dynamic layout from pdfplumber
            rows = pg_detected["item_rows"]
            price_x = pg_detected["price_x"]
            ext_x = pg_detected["ext_x"]
        else:
            # Hardcoded fallback with scaling
            _sx = pw / 792.0
            _sy = ph / 612.0
            rows = [(_sy * yb, _sy * yt) for yb, yt in (_HC_PG1_ROWS if is_pg1 else _HC_PG2_ROWS)]
            price_x = (_sx * _HC_PRICE_X[0], _sx * _HC_PRICE_X[1])
            ext_x = (_sx * _HC_EXT_X[0], _sx * _HC_EXT_X[1])

        # Skip if all rows on this page are beyond our data
        page_first_row = current_row
        if page_first_row > max_row and pg_idx > 0:
            log.info("OVERLAY pg%d: skip (rows start at %d, max=%d)", pg_idx, page_first_row, max_row)
            current_row += len(rows)
            continue

        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(pw, ph))
        drew = False

        # ── Coordinate helper for hardcoded fallback ──
        _sx = pw / 792.0
        _sy = ph / 612.0
        # QTY/UOM/QPU columns — always use scaled hardcoded (consistent across 704 forms)
        qty_x = (_sx * _HC_QTY_X[0], _sx * _HC_QTY_X[1])
        uom_x = (_sx * _HC_UOM_X[0], _sx * _HC_UOM_X[1])
        qpu_x = (_sx * _HC_QPU_X[0], _sx * _HC_QPU_X[1])
        def _sc(x1, y1, x2, y2):
            return (x1 * _sx, y1 * _sy, x2 * _sx, y2 * _sy)

        # ── SUPPLIER INFO (page 1 only) ──
        if is_pg1:
            if pg_detected and pg_detected.get("supplier_cells"):
                # Use detected positions — offset to BOTTOM HALF of cell
                # to avoid covering the label text at the top of each cell
                for fname, (cx1, cy1, cx2, cy2) in pg_detected["supplier_cells"].items():
                    val = fv_map.get(fname, "")
                    if val:
                        cell_h = cy2 - cy1
                        # Draw value in bottom 55% of cell, no white mask (preserves label)
                        _cell(c, cx1, cy1, cx2, cy1 + cell_h * 0.55, val, fs=9,
                              mask_top_pct=0.0)
                        drew = True
                # For detected layouts, do NOT fall back to hardcoded supplier positions.
                # The DOCX layout is different — hardcoded coords cause misalignment.
                # Ship-to uses detected ship_to_area if available.
                if pg_detected.get("ship_to_area"):
                    _st_val = fv_map.get("Ship to", "")
                    if _st_val:
                        _cell(c, *pg_detected["ship_to_area"], _st_val, fs=8, mask_top_pct=0.0)
                        drew = True
            else:
                # All hardcoded
                for fname, (x1, y1, x2, y2) in _HC_SUPPLIER.items():
                    val = fv_map.get(fname, "")
                    if val:
                        mtp = 0.65 if fname in _HC_LABELED else 1.0
                        _cell(c, *_sc(x1, y1, x2, y2), val, fs=10, mask_top_pct=mtp)
                        drew = True

            # Notes
            nf = _HC_NOTES[0]
            nval = fv_map.get(nf, "")
            if nval:
                if pg_detected and pg_detected.get("notes_area"):
                    _multiline(c, *pg_detected["notes_area"], nval, fs=8)
                else:
                    _multiline(c, *_sc(_HC_NOTES[1], _HC_NOTES[2], _HC_NOTES[3], _HC_NOTES[4]), nval, fs=8)
                drew = True

            # FOB Checkbox
            cf = _HC_CHECKBOX[0]
            if fv_map.get(cf) in ("/Yes", "Yes", True, "True"):
                if pg_detected and pg_detected.get("fob_area"):
                    cx1, cy1, cx2, cy2 = pg_detected["fob_area"]
                else:
                    cx1, cy1, cx2, cy2 = _sc(_HC_CHECKBOX[1], _HC_CHECKBOX[2],
                                             _HC_CHECKBOX[3], _HC_CHECKBOX[4])
                c.saveState()
                c.setFillColorRGB(1, 1, 1)
                c.rect(cx1 + 2, cy1 + 2, cx2 - cx1 - 4, cy2 - cy1 - 4, fill=1, stroke=0)
                c.setStrokeColorRGB(0, 0, 0)
                c.setLineWidth(1.5)
                _pad = 3
                c.line(cx1 + _pad, cy1 + _pad, cx2 - _pad, cy2 - _pad)
                c.line(cx1 + _pad, cy2 - _pad, cx2 - _pad, cy1 + _pad)
                c.restoreState()
                drew = True

            # Totals — skip mask for detected layouts (empty cells in DOCX forms)
            _totals_mask = not bool(pg_detected)
            if pg_detected and pg_detected.get("totals_cells"):
                for fname, (tx1, ty1, tx2, ty2) in pg_detected["totals_cells"].items():
                    val = fv_map.get(fname, "")
                    if val:
                        # Draw right-aligned with minimal padding for detected cells
                        _th = ty2 - ty1
                        _tfs = min(9, _th * 0.8)
                        c.saveState()
                        c.setFont("Helvetica-Bold", _tfs)
                        c.setFillColorRGB(0, 0, 0)
                        _tw = c.stringWidth(val, "Helvetica-Bold", _tfs)
                        c.drawString(tx2 - _tw - 2, ty1 + (_th - _tfs) / 2, val)
                        c.restoreState()
                        drew = True
                # For detected layouts, do NOT fall back to hardcoded totals.
            else:
                for fname, (x1, y1, x2, y2) in _HC_TOTALS.items():
                    val = fv_map.get(fname, "")
                    if val:
                        _cell_right(c, *_sc(x1, y1, x2, y2), val, fs=10)
                        drew = True

        # ── CONTINUATION HEADER (supplier name on page 2+) ──
        _page_has_items = any(
            fv_map.get(ROW_FIELDS["unit_price"].format(n=rn), "").strip()
            for rn in range(current_row, current_row + len(rows))
        ) if not is_pg1 else True
        if not is_pg1 and _page_has_items:
            company = fv_map.get("COMPANY NAME", "")
            if company:
                if pg_detected and pg_detected.get("supplier_name_cont"):
                    _cell(c, *pg_detected["supplier_name_cont"], company, fs=12)
                else:
                    sp = _sc(_HC_PG2_SUPPLIER[0], _HC_PG2_SUPPLIER[1],
                             _HC_PG2_SUPPLIER[2], _HC_PG2_SUPPLIER[3])
                    _cell(c, *sp, company, fs=12)
                drew = True

        # ── ROW PRICING: PRICE PER UNIT + EXTENSION columns ──
        # For detected layouts (DOCX-converted flat PDFs), cells are empty — skip white mask
        # to avoid visible rectangle artifacts. For hardcoded (DocuSign), mask existing content.
        _need_mask = not bool(pg_detected)
        _orig_values = pg_detected.get("orig_values", []) if pg_detected else []
        _desc_tops = pg_detected.get("desc_tops", []) if pg_detected else []
        for slot_idx, (y_bot, y_top) in enumerate(rows):
            rn = current_row + slot_idx
            # QTY/UOM/QPU positioning:
            # - Hardcoded layouts: always draw (no buyer text baked in)
            # - Detected layouts: only draw if app value DIFFERS from buyer's
            #   original (e.g. buyer asked for an edit). Draw in the description
            #   sub-row (y_top → desc_top) to properly cover buyer's text.
            if pg_detected:
                # For detected layouts (DOCX-converted), SKIP QTY/UOM/QPU overlay entirely.
                # The buyer's original form already has these values printed. Overlaying
                # causes double-text artifacts even when values match (detection is imperfect).
                pass
            else:
                _row_h = y_top - y_bot
                _qty_top = y_top + _row_h  # full row height above price line
                # QTY
                qf = ROW_FIELDS["qty"].format(n=rn)
                qv = fv_map.get(qf, "")
                if qv and qv.strip():
                    _cell(c, qty_x[0], y_top, qty_x[1], _qty_top, qv, fs=9)
                    drew = True
                # UOM
                uf = ROW_FIELDS["uom"].format(n=rn)
                uv = fv_map.get(uf, "")
                if uv and uv.strip():
                    _cell(c, uom_x[0], y_top, uom_x[1], _qty_top, uv, fs=8)
                    drew = True
                # QTY PER UOM
                qpf = ROW_FIELDS["qty_per_uom"].format(n=rn)
                qpv = fv_map.get(qpf, "")
                if qpv and qpv.strip():
                    _cell(c, qpu_x[0], y_top, qpu_x[1], _qty_top, qpv, fs=9)
                    drew = True
            # Price
            pf = ROW_FIELDS["unit_price"].format(n=rn)
            pv = fv_map.get(pf, "")
            if pv and pv.strip():
                _cell_right(c, price_x[0], y_bot, price_x[1], y_top, pv, fs=9, mask=_need_mask)
                drew = True
            # Extension
            ef = ROW_FIELDS["extension"].format(n=rn)
            ev = fv_map.get(ef, "")
            if ev and ev.strip():
                _cell_right(c, ext_x[0], y_bot, ext_x[1], y_top, ev, fs=9, mask=_need_mask)
                drew = True

        log.info("OVERLAY pg%d: %s rows=%d-%d (%d slots) drew=%s detected=%s",
                 pg_idx, "pg1" if is_pg1 else "cont",
                 current_row, current_row + len(rows) - 1, len(rows),
                 drew, pg_detected is not None)
        current_row += len(rows)

        c.save()
        buf.seek(0)
        if drew:
            overlay = PdfReader(buf)
            if overlay.pages:
                writer.pages[pg_idx].merge_page(overlay.pages[0])

    # ── Remove pages that had no priced items drawn ──
    pages_with_items = set()
    check_row = 1
    for pg_idx in range(num_pages):
        if _using_detected and pg_idx < len(detected) and detected[pg_idx]:
            rows_on_page = len(detected[pg_idx]["item_rows"])
        else:
            rows_on_page = len(_HC_PG1_ROWS) if pg_idx == 0 else len(_HC_PG2_ROWS)
        page_first = check_row
        has_items = False
        for rn in range(page_first, page_first + rows_on_page):
            pf = ROW_FIELDS["unit_price"].format(n=rn)
            if fv_map.get(pf, "").strip():
                has_items = True
                break
        if has_items or pg_idx == 0:
            pages_with_items.add(pg_idx)
        check_row += rows_on_page

    if len(pages_with_items) < len(writer.pages):
        trimmed_writer = PdfWriter()
        for pg_idx in range(len(writer.pages)):
            if pg_idx in pages_with_items:
                trimmed_writer.add_page(writer.pages[pg_idx])
        log.info("OVERLAY: trimmed output from %d to %d pages (kept: %s)",
                 len(writer.pages), len(trimmed_writer.pages), sorted(pages_with_items))
        writer = trimmed_writer

    # Pass detected sig position for DOCX layouts (Row 2 middle cell = Signature and Date)
    _detected_sig = None
    if _using_detected and detected and detected[0]:
        _d0 = detected[0]
        # The "Address" cell is Row 2 left. "Discount Offered" is Row 2 right.
        # Signature is in between — use Address right edge to Discount left edge,
        # at the same Y range as Address.
        _addr = _d0.get("supplier_cells", {}).get("Address")
        _disc = _d0.get("supplier_cells", {}).get("Discount Offered")
        if _addr and _disc:
            _detected_sig = (_addr[2] + 2, _addr[1], _disc[0] - 2, _addr[3])
            log.info("OVERLAY: detected sig rect from supplier cells: (%.0f, %.0f, %.0f, %.0f)",
                     *_detected_sig)
    _add_signature_to_pdf(writer, source_pdf_path=source_pdf, sig_rect_override=_detected_sig)
    with open(output_pdf, "wb") as f:
        writer.write(f)
    log.info("Filled AMS 704 (OVERLAY) to %s — %d pages", output_pdf, len(writer.pages))


def _fill_pdf_fields(source_pdf: str, field_values: list, output_pdf: str):
    """
    Fill PDF form fields with auto-fit font sizing.

    Strategy:
    1. Try native form-field fill (works for fillable PDFs with /AcroForm)
    2. If PDF is flat/scanned (no /AcroForm), fall back to reportlab text overlay
    """
    reader = PdfReader(source_pdf)
    writer = PdfWriter()
    writer.append(reader)

    from pypdf.generic import NameObject, TextStringObject, ArrayObject, DictionaryObject, NumberObject

    has_acroform = "/AcroForm" in writer._root_object

    if not has_acroform:
        # Method 1: Reader has fields that didn't transfer to writer
        reader_fields = reader.get_fields()
        if reader_fields:
            try:
                root = reader.trailer["/Root"]
                if hasattr(root, 'get_object'):
                    root = root.get_object()
                acroform = root.get("/AcroForm")
                if acroform:
                    if hasattr(acroform, 'get_object'):
                        acroform = acroform.get_object()
                    writer._root_object[NameObject("/AcroForm")] = acroform
                    has_acroform = True
                    log.info("_fill_pdf_fields: recovered /AcroForm from reader (%d fields)", len(reader_fields))
            except Exception as e:
                log.warning("_fill_pdf_fields: AcroForm copy failed: %s", e)

        # Method 2: Build AcroForm from page Widget annotations
        if not has_acroform:
            all_widgets = []
            for page in writer.pages:
                for annot_ref in (page.get("/Annots") or []):
                    try:
                        annot = annot_ref.get_object()
                        if str(annot.get("/Subtype", "")) == "/Widget":
                            all_widgets.append(annot_ref)
                    except Exception:
                        pass
            if all_widgets:
                writer._root_object[NameObject("/AcroForm")] = DictionaryObject({
                    NameObject("/Fields"): ArrayObject(all_widgets),
                })
                has_acroform = True
                log.info("_fill_pdf_fields: built /AcroForm from %d Widget annotations", len(all_widgets))

    if not has_acroform:
        log.warning("_fill_pdf_fields: No /AcroForm and no Widget annotations — using text overlay for %s",
                     os.path.basename(source_pdf))
        _fill_pdf_text_overlay(source_pdf, field_values, output_pdf)
        return

    # ── Check if AcroForm has any writable text fields ──
    # DocuSign PDFs have /AcroForm but only contain a Sig field — no text fields.
    # Trying to fill them natively produces blank output. Detect and use overlay.
    _writable_text_fields = 0
    try:
        _rf = reader.get_fields() or {}
        for _fn, _fobj in _rf.items():
            _ft = str((_fobj or {}).get("/FT", ""))
            if _ft in ("/Tx", "/Ch"):
                _writable_text_fields += 1
        if _writable_text_fields == 0:
            for _page in reader.pages:
                for _annot_ref in (_page.get("/Annots") or []):
                    try:
                        _annot = _annot_ref.get_object()
                        if (str(_annot.get("/Subtype", "")) == "/Widget" and
                                str(_annot.get("/FT", "")) == "/Tx"):
                            _writable_text_fields += 1
                    except Exception:
                        pass
    except Exception:
        pass

    if _writable_text_fields == 0:
        log.info("_fill_pdf_fields: AcroForm has 0 writable text fields (DocuSign/flat) — forcing overlay for %s",
                  os.path.basename(source_pdf))
        _fill_pdf_text_overlay(source_pdf, field_values, output_pdf)
        return

    # ── Native form-field fill path ──

    # Strip non-form annotations
    for page in writer.pages:
        annots = page.get("/Annots")
        if not annots:
            continue
        cleaned = []
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                subtype = str(annot.get("/Subtype", ""))
                if subtype == "/Widget":
                    cleaned.append(annot_ref)
                else:
                    log.debug(f"Stripping annotation: {subtype} T={annot.get('/T','')}")
            except Exception:
                cleaned.append(annot_ref)
        page[NameObject("/Annots")] = ArrayObject(cleaned)

    checkbox_fields = {}
    text_values = {}
    for fv in field_values:
        if fv.get("value") in ("/Yes", "/Off", "Yes", "Off"):
            checkbox_fields[fv["field_id"]] = fv["value"].replace("/", "")
        else:
            text_values[fv["field_id"]] = fv["value"]

    field_widths = {}
    for page in reader.pages:
        for annot_ref in (page.get("/Annots") or []):
            try:
                annot = annot_ref.get_object()
                name = str(annot.get("/T", ""))
                rect = annot.get("/Rect", [0, 0, 0, 0])
                w = float(rect[2]) - float(rect[0])
                field_widths[name] = w
            except Exception:
                pass

    def calc_font_size(text: str, field_width: float, max_size: float = 12.0, min_size: float = 6.0) -> float:
        if not text or field_width <= 0:
            return max_size
        is_numeric = all(c in '0123456789.,$' for c in text.strip())
        char_factor = 0.50 if is_numeric else 0.52
        padding = 4
        usable = field_width - padding
        ideal = usable / (len(text) * char_factor)
        return max(min_size, min(max_size, ideal))

    for page in writer.pages:
        for annot_ref in (page.get("/Annots") or []):
            try:
                annot = annot_ref.get_object()
                name = str(annot.get("/T", ""))
                if name in text_values and text_values[name]:
                    val = text_values[name]
                    width = field_widths.get(name, 200)
                    font_size = calc_font_size(val, width)
                    da_str = f"/Helv {font_size:.1f} Tf 0 g"
                    annot[NameObject("/DA")] = TextStringObject(da_str)
                    # Right-align price, extension, and total fields
                    if any(k in name for k in ("PRICE PER UNIT", "EXTENSION", "fill_7")):
                        annot[NameObject("/Q")] = NumberObject(2)
            except Exception:
                pass

    for page_num in range(len(writer.pages)):
        writer.update_page_form_field_values(
            writer.pages[page_num],
            text_values,
            auto_regenerate=True,
        )

    # ── Check checkboxes: scan page annotations AND /Kids of parent fields ──
    def _set_checkbox(annot_obj, desired):
        """Set a checkbox widget to desired state ('Yes' or 'Off')."""
        ap = annot_obj.get("/AP", {})
        if hasattr(ap, 'get_object'):
            ap = ap.get_object()
        ap_n = ap.get("/N", {}) if isinstance(ap, dict) else {}
        if hasattr(ap_n, 'get_object'):
            ap_n = ap_n.get_object()
        available = [str(k) for k in ap_n.keys() if str(k) != "/Off"] if isinstance(ap_n, dict) else []
        if desired in ("Yes", "On", "1") and available:
            on_state = available[0].lstrip("/")
            annot_obj[NameObject("/V")] = NameObject(f"/{on_state}")
            annot_obj[NameObject("/AS")] = NameObject(f"/{on_state}")
            return True
        elif desired in ("Yes", "On", "1"):
            annot_obj[NameObject("/V")] = NameObject(f"/{desired}")
            annot_obj[NameObject("/AS")] = NameObject(f"/{desired}")
            return True
        return False

    _checked_fields = set()
    # Method 1: scan page /Annots directly (works for simple checkbox widgets)
    for page in writer.pages:
        for annot_ref in (page.get("/Annots") or []):
            try:
                annot = annot_ref.get_object()
                name = str(annot.get("/T", ""))
                if name in checkbox_fields:
                    if _set_checkbox(annot, checkbox_fields[name]):
                        _checked_fields.add(name)
            except Exception:
                pass

    # Method 2: traverse AcroForm /Fields for parent fields with /Kids
    # DocuSign PDFs use parent/child structure where the parent has /T and /FT=/Btn
    # but the actual widget with /AP is in /Kids, not in page /Annots.
    _unchecked = set(checkbox_fields.keys()) - _checked_fields
    if _unchecked:
        try:
            acroform = writer._root_object.get("/AcroForm")
            if acroform:
                if hasattr(acroform, 'get_object'):
                    acroform = acroform.get_object()
                for field_ref in (acroform.get("/Fields") or []):
                    try:
                        field = field_ref.get_object()
                        fname = str(field.get("/T", ""))
                        if fname not in _unchecked:
                            continue
                        ft = str(field.get("/FT", ""))
                        if ft != "/Btn":
                            continue
                        # Check /Kids for child widgets
                        kids = field.get("/Kids", [])
                        for kid_ref in kids:
                            kid = kid_ref.get_object()
                            if _set_checkbox(kid, checkbox_fields[fname]):
                                _checked_fields.add(fname)
                                log.info("_fill_pdf_fields: checked '%s' via /Kids widget", fname)
                                break
                    except Exception:
                        pass
        except Exception as e:
            log.debug("_fill_pdf_fields: AcroForm /Kids checkbox scan failed: %s", e)

    if _unchecked - _checked_fields:
        log.warning("_fill_pdf_fields: unchecked checkbox fields (no widget found): %s",
                     _unchecked - _checked_fields)

    _add_signature_to_pdf(writer, source_pdf_path=source_pdf)

    with open(output_pdf, "wb") as f:
        writer.write(f)

    # Post-fill verification: read back and log unmatched fields
    try:
        from pypdf import PdfReader as _VR
        _vfields = _VR(output_pdf).get_fields() or {}
        _actual_keys = set(_vfields.keys())
        _intended_keys = set(text_values.keys())
        _unmatched = _intended_keys - _actual_keys
        if _unmatched:
            log.warning("_fill_pdf_fields: %d/%d intended fields not found in output: %s",
                        len(_unmatched), len(_intended_keys), sorted(_unmatched)[:10])
    except Exception:
        pass  # verification is best-effort

    log.info(f"Filled AMS 704 saved to {output_pdf}")


def _detect_sig_field_rect(source_pdf_or_writer):
    """Detect the Signature field /Rect from the PDF.
    Returns (left, bottom, right, top) or None.
    Works with both PdfReader and PdfWriter objects."""
    try:
        pages = source_pdf_or_writer.pages if hasattr(source_pdf_or_writer, 'pages') else []
        if not pages:
            return None
        page = pages[0]
        for annot_ref in (page.get("/Annots") or []):
            try:
                annot = annot_ref.get_object()
                name = str(annot.get("/T", ""))
                # Match any signature-like field in the supplier row
                if name.lower() in ("signature1", "signature", "sig") or (
                        annot.get("/FT") == "/Sig" and "envelope" not in name.lower()):
                    rect = annot.get("/Rect")
                    if rect:
                        r = [float(x) for x in rect]
                        # Sanity: field should be >100pt wide and in the middle band
                        if r[2] - r[0] > 100 and r[3] - r[1] > 10:
                            log.info("_detect_sig_field_rect: found '%s' Rect=%s", name, r)
                            return tuple(r)
            except Exception:
                pass
    except Exception as e:
        log.debug("_detect_sig_field_rect: %s", e)
    return None


def _add_signature_to_pdf(writer, source_pdf_path=None, sig_rect_override=None):
    """Overlay signature image and date onto the Signature field.

    Uses dynamic field detection when possible, falls back to reference
    coordinates from AMS 704 Rev 1/2019 landscape (792×612).
    White-masks the entire signature cell first to clear any baked-in
    label text (e.g. 'Signature and Date') from DocuSign-flattened PDFs.
    """
    import io

    # ── Detect actual field boundaries ──
    sig_rect = sig_rect_override  # From detected DOCX supplier cells
    if not sig_rect:
        sig_rect = _detect_sig_field_rect(writer)
    if not sig_rect and source_pdf_path:
        try:
            sig_rect = _detect_sig_field_rect(PdfReader(source_pdf_path))
        except Exception:
            pass

    # Fallback: reference coords from AMS 704 Rev 1/2019
    # Signature field: Rect=[279.144, 388.486, 602.284, 412.005]
    if not sig_rect:
        sig_rect = (279.144, 388.486, 602.284, 412.005)

    field_left, field_bot, field_right, field_top = sig_rect
    field_w = field_right - field_left
    field_h = field_top - field_bot

    # Scale-aware: detect actual page size
    page_width, page_height = 792.0, 612.0
    try:
        mb = writer.pages[0].mediabox
        page_width, page_height = float(mb.width), float(mb.height)
    except Exception:
        pass

    # Scale factors (reference coords are for 792×612)
    sx = page_width / 792.0
    sy = page_height / 612.0

    # Apply scaling to field rect (skip if override provided — already absolute)
    if sig_rect_override:
        fl, fb, fr, ft = field_left, field_bot, field_right, field_top
    else:
        fl = field_left * sx
        fb = field_bot * sy
        fr = field_right * sx
        ft = field_top * sy
    fw = fr - fl
    fh = ft - fb

    # Layout: signature image takes left 60%, date in right 20%
    sig_w = min(fw * 0.60, 220)
    sig_h = fh - 6  # fit WITHIN cell (leave 3pt padding top + bottom)
    date_x = fr - fw * 0.22  # right quarter
    date_y = fb + (fh - 9) / 2  # vertically centered for ~9pt font

    # Find signature image
    sig_paths = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "signature_transparent.png"),
        "/app/signature_transparent.png",
        os.path.join(DATA_DIR, "signature_transparent.png"),
    ]
    sig_path = None
    for p in sig_paths:
        if os.path.exists(p):
            sig_path = p
            break

    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.utils import ImageReader

        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(page_width, page_height))

        # ── Clip + white-mask the signature cell ──
        # clipPath prevents ANY drawing from touching cell borders.
        # White fill clears baked-in "Signature and Date" label text.
        _SP = 4  # inset from cell edge to preserve border lines
        c.saveState()
        clip = c.beginPath()
        clip.rect(fl + _SP, fb + _SP, fw - _SP * 2, fh - _SP * 2)
        c.clipPath(clip, stroke=0)
        c.setFillColorRGB(1, 1, 1)
        c.rect(fl + _SP, fb + _SP, fw - _SP * 2, fh - _SP * 2, fill=1, stroke=0)

        # ── Draw signature image (left portion of cell) ──
        if sig_path:
            try:
                img = ImageReader(sig_path)
                c.drawImage(img, fl + _SP + 1, fb + _SP + 1, width=sig_w, height=sig_h,
                           mask='auto', preserveAspectRatio=True, anchor='sw')
            except Exception as e:
                log.warning("Could not draw signature image: %s", e)

        # ── Draw date (right portion of cell) ──
        today = f"{_pst_now().month}/{_pst_now().day}/{_pst_now().year}"
        date_fs = min(9, (fh - _SP * 2) * 0.6)
        c.setFont("Helvetica", date_fs)
        c.setFillColorRGB(0, 0, 0)
        c.drawString(date_x, date_y, today)
        c.restoreState()

        c.save()
        buf.seek(0)

        # Merge overlay onto first page
        overlay_reader = PdfReader(buf)
        overlay_page = overlay_reader.pages[0]
        writer.pages[0].merge_page(overlay_page)

        log.info("_add_signature_to_pdf: field=(%d,%d,%d,%d) page=%dx%d sig_w=%.0f date_x=%.0f",
                 field_left, field_bot, field_right, field_top, page_width, page_height, sig_w, date_x)

    except ImportError:
        log.warning("reportlab not available, setting Signature1 as text")
        today = f"{_pst_now().month}/{_pst_now().day}/{_pst_now().year}"
        text_values = {"Signature1": f"Michael Guadan  {today}"}
        writer.update_page_form_field_values(writer.pages[0], text_values, auto_regenerate=False)
    except Exception as e:
        log.error("Signature overlay error: %s", e)


def _expiry_date() -> str:
    """Generate an expiry date 45 days from now (PST)."""
    exp = _pst_now() + timedelta(days=45)
    return f"{exp.month}/{exp.day}/{exp.year}"


# ─── Full Pipeline ───────────────────────────────────────────────────────────

def process_price_check(
    pdf_path: str,
    output_dir: str = None,
    company_info: dict = None,
    tax_rate: float = 0.0,
) -> dict:
    """
    Full pipeline: Parse → Lookup → Price → Fill → Output.

    Args:
        pdf_path: Path to AMS 704 PDF
        output_dir: Where to save filled PDF (default: data/)
        company_info: Override company info
        tax_rate: Tax rate

    Returns:
        {
            "ok": bool,
            "parsed": {...},
            "output_pdf": str,
            "summary": {...},
        }
    """
    if not output_dir:
        output_dir = DATA_DIR
    os.makedirs(output_dir, exist_ok=True)

    # 1. Parse
    log.info(f"Parsing AMS 704: {pdf_path}")
    parsed = parse_ams704(pdf_path)
    if parsed.get("error"):
        return {"ok": False, "error": parsed["error"], "parsed": parsed}

    if not parsed["line_items"]:
        return {"ok": False, "error": "No line items found in PDF", "parsed": parsed}

    log.info(f"Found {len(parsed['line_items'])} line items")

    # 2. Lookup prices
    log.info("Looking up prices...")
    parsed = lookup_prices(parsed)

    # 3. Generate output filename
    pc_num = parsed["header"].get("price_check_number", "unknown")
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    output_filename = f"PC_{safe_name}_Reytech_.pdf"
    output_path = os.path.join(output_dir, output_filename)

    # 4. Fill the form
    log.info(f"Filling AMS 704 → {output_path}")
    fill_result = fill_ams704(
        source_pdf=pdf_path,
        parsed_pc=parsed,
        output_pdf=output_path,
        company_info=company_info,
        tax_rate=tax_rate,
    )

    return {
        "ok": fill_result.get("ok", False),
        "parsed": parsed,
        "output_pdf": output_path if fill_result.get("ok") else None,
        "summary": fill_result.get("summary", {}),
        "error": fill_result.get("error"),
    }


# ─── Test ────────────────────────────────────────────────────────────────────

def test_parse(pdf_path: str) -> dict:
    """Test parsing an AMS 704 PDF without doing price lookups."""
    parsed = parse_ams704(pdf_path)
    return {
        "header": parsed.get("header", {}),
        "line_items": [
            {
                "item": i.get("item_number"),
                "qty": i.get("qty"),
                "uom": i.get("uom"),
                "description": i.get("description", "")[:80],
            }
            for i in parsed.get("line_items", [])
        ],
        "ship_to": parsed.get("ship_to"),
        "field_count": parsed.get("field_count"),
        "parse_method": parsed.get("parse_method"),
    }
