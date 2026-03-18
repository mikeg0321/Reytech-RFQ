


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
from datetime import datetime
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

REYTECH_INFO = {
    "company_name": "Reytech Inc.",
    "representative": "Michael Guadan",
    "address": "30 Carnoustie Way, Trabuco Canyon, CA 92679",
    "phone": "949-229-1575",
    "email": "sales@reytechinc.com",
    "sb_mb": "2002605",
    "dvbe": "2002605",
    "discount": "Included",
    "delivery": "5-7 business days",  # Default; dashboard offers dropdown
}


# ─── AMS 704 Field Name Patterns ────────────────────────────────────────────

# Row fields follow pattern: "FIELD NAMERow{N}" where N = 1-8 per page
ROW_FIELDS = {
    "item_number": "ITEM Row{n}",
    "qty": "QTYRow{n}",
    "uom": "UNIT OF MEASURE UOMRow{n}",
    "qty_per_uom": "QTY PER UOMRow{n}",
    "description": "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow{n}",
    "substituted": "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow{n}",
    "unit_price": "PRICE PER UNITRow{n}",
    "extension": "EXTENSIONRow{n}",
}

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
            if (has_letter and has_digit) or (has_dash and has_digit and len(candidate) >= 5):
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
    ]

    filtered = []
    removed = 0
    for item in items:
        desc = (item.get("description") or "").strip().lower()

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

    reader = PdfReader(pdf_path)
    fields = reader.get_fields()

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

    for row_num in range(1, max_row_check + 1):
        row_data = {}
        has_data = False

        for key, pattern in ROW_FIELDS.items():
            field_name = _find_field(key, row_num)
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

            item = {
                "item_number": row_data.get("item_number", str(row_num)),
                "qty": qty,
                "uom": (row_data.get("uom", "ea") or "ea").upper(),
                "qty_per_uom": qty_per_uom,
                "description": clean_description(row_data["description"]),
                "description_raw": row_data["description"],
                "substituted": row_data.get("substituted", ""),
                "row_index": row_num,
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
        
        # Candidate for merging if:
        # 1. Default qty (0 or 1) and default/missing UOM — no real data was parsed
        # 2. OR description is clearly supplementary (pack info, part number)
        is_default_qty = (item.get("qty", 1) in (0, 1))
        is_default_uom = (item.get("uom", "EA").upper() in ("EA", ""))
        is_supplement = _is_supplementary_desc(raw_desc)
        
        should_merge = False
        if is_supplement:
            should_merge = True
        elif is_default_qty and is_default_uom and prev.get("qty", 1) > 1:
            # Previous item has a real quantity but this one is default — likely continuation
            should_merge = True
        elif item.get("qty", 1) == 0:
            # Zero-qty items are never real line items — always merge
            should_merge = True
        elif is_default_qty and is_default_uom:
            # Default qty + default UOM: check if desc is just more detail text
            # (not a distinct product). Short descs without product keywords = merge
            desc_up = desc.upper()
            has_product_word = any(w in desc_up for w in [
                "GLOVE", "MASK", "BRIEF", "WIPE", "GOWN", "SYRINGE", "BANDAGE",
                "GAUZE", "TAPE", "SOAP", "TOWEL", "PAPER", "BAG", "LINER",
                "SANITIZER", "CATHETER", "NEEDLE", "BOOT", "VEST", "HELMET",
            ])
            if not has_product_word and len(desc) < 60:
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
    return result


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
            prev["description"] = (prev["description"] + " " + desc.strip()).strip()
            prev["description_raw"] = prev.get("description_raw", prev["description"])
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

        item = {
            "item_number": item_num_val or str(row_num),
            "qty": qty,
            "uom": (uom_raw or "ea").upper(),
            "qty_per_uom": 1,
            "description": desc.strip(),
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

    # FOB Destination, Freight Prepaid checkbox
    field_values.append({
        "field_id": "Check Box4",
        "page": 1,
        "value": "/Yes",
    })

    # Line items with pricing
    subtotal = 0.0
    items_priced = 0

    # ── Detect if template is pre-filled by buyer ──
    _form_is_prefilled = False
    try:
        from pypdf import PdfReader as _PrePR
        _pre_check = _PrePR(source_pdf)
        _pre_fields = _pre_check.get_fields() or {}
        for _check_field in ["Qty_1", "QTY_1", "qty_1", "fill_5"]:
            _pv = str((_pre_fields.get(_check_field) or {}).get("/V", "")).strip()
            if _pv and _pv not in ("", "0"):
                _form_is_prefilled = True
                break
        if _form_is_prefilled:
            log.info("fill_ams704: template is pre-filled — switching to pricing-only mode")
    except Exception:
        pass

    if _form_is_prefilled and not original_mode:
        original_mode = True
        log.info("fill_ams704: auto-switched to original_mode (pre-filled template detected)")

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

    for item_idx, item in enumerate(items):
        row = item.get("row_index") or (item_idx + 1)  # default to 1-based position
        # Ensure row is sequential if row_index resets per page (1-8 instead of 9-16, 17-24)
        if row <= 8 and item_idx >= 8:
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

        # ── ORIGINAL MODE: only fill pricing fields, leave buyer fields untouched ──
        if original_mode:
            unit_price = item.get("unit_price") or pricing.get("recommended_price")
            if not unit_price:
                unit_price = pricing.get("amazon_price")
            if unit_price:
                try:
                    unit_price = float(unit_price)
                except (ValueError, TypeError):
                    unit_price = 0
            if unit_price and unit_price > 0:
                try:
                    extension = round(float(unit_price) * int(qty), 2)
                except (ValueError, TypeError):
                    extension = 0
                if extension > 0:
                    subtotal += extension
                    items_priced += 1
                    price_field = ROW_FIELDS["unit_price"].format(n=row)
                    ext_field = ROW_FIELDS["extension"].format(n=row)
                    field_values.append({"field_id": price_field, "page": 1, "value": f"{unit_price:,.2f}"})
                    field_values.append({"field_id": ext_field, "page": 1, "value": f"{extension:,.2f}"})
                    log.info("fill_ams704 ORIGINAL row=%d idx=%d: price=%.2f qty=%d ext=%.2f",
                             row, item_idx, unit_price, qty, extension)
                else:
                    _skipped_no_price += 1
                    log.warning("fill_ams704 ORIGINAL row=%d idx=%d: extension=0 (price=%.2f qty=%s)",
                                row, item_idx, unit_price, item.get("qty"))
            else:
                _skipped_no_price += 1
            continue  # Skip description, item#, qty, uom, substituted — buyer's fields stay as-is

        # ── NORMAL MODE: write all fields ──

        # ── ALWAYS WRITE: Item#, Qty, Description, UOM ──
        # These fields appear on every 704 regardless of pricing status

        # ITEM NUMBER field: sequential line numbers (1, 2, 3...)
        item_num_field = ROW_FIELDS["item_number"].format(n=row)
        field_values.append({
            "field_id": item_num_field,
            "page": 1,
            "value": str(seq),
        })

        # QTY field
        qty_field = ROW_FIELDS["qty"].format(n=row)
        field_values.append({
            "field_id": qty_field,
            "page": 1,
            "value": str(qty),
        })

        # DESCRIPTION field: always write to PDF
        # Priority: user-edited description > original parsed text
        desc_user = (item.get("description") or "").strip()
        desc_raw = (item.get("description_raw") or "").strip()
        desc_source = desc_user or desc_raw
        desc_clean = clean_description(desc_source) if desc_source else ""
        desc_final = desc_clean or desc_source
        if desc_final:
            desc_field = ROW_FIELDS["description"].format(n=row)
            # Always append MFG# to PDF description so purchasing sees the part number
            mfg_num = (item.get("mfg_number") or pricing.get("mfg_number") 
                       or pricing.get("manufacturer_part") or "")
            if mfg_num and mfg_num.lower() not in desc_final.lower():
                desc_final = f"{desc_final}\nMFG#: {mfg_num}"
            # For substitutes, also add ASIN if no MFG#
            if item.get("is_substitute") and not mfg_num:
                asin = pricing.get("amazon_asin", "")
                if asin and asin not in desc_final:
                    desc_final = f"{desc_final}\nASIN: {asin}"
            item_notes = (item.get("notes") or "").strip()
            if item_notes:
                desc_final = f"{desc_final}\nNote: {item_notes}"
            # Description overflow into next empty row
            DESC_CHAR_LIMIT = 140
            if len(desc_final) > DESC_CHAR_LIMIT:
                next_row = row + 1
                if next_row <= max_row and next_row not in occupied_rows and next_row not in overflow_rows:
                    split_at = DESC_CHAR_LIMIT
                    for break_char in ['\n', ', ', ' ']:
                        pos = desc_final.rfind(break_char, 0, split_at + 10)
                        if pos > split_at - 40:
                            split_at = pos + len(break_char)
                            break
                    part1 = desc_final[:split_at].rstrip()
                    part2 = desc_final[split_at:].lstrip()
                    if part2:
                        overflow_field = ROW_FIELDS["description"].format(n=next_row)
                        field_values.append({
                            "field_id": overflow_field,
                            "page": 1,
                            "value": part2,
                        })
                        overflow_rows.add(next_row)
                        desc_final = part1
                        log.info("fill_ams704 row=%d: desc overflow into row %d (%d chars)", row, next_row, len(part2))
            field_values.append({
                "field_id": desc_field,
                "page": 1,
                "value": desc_final,
            })

        # UOM (uppercase)
        uom_val = (item.get("uom") or "EA").upper()
        uom_field = ROW_FIELDS["uom"].format(n=row)
        field_values.append({
            "field_id": uom_field,
            "page": 1,
            "value": uom_val,
        })

        # ── CONDITIONALLY WRITE: Price and Extension (only if we have a price) ──
        unit_price = item.get("unit_price") or pricing.get("recommended_price")
        if not unit_price:
            unit_price = pricing.get("amazon_price")
        if not unit_price:
            _skipped_no_price += 1
            log.info("fill_ams704 row=%d: desc WRITTEN, but NO PRICE (desc='%s')",
                     row, (desc_final or "")[:40])
            # Still write substituted item if applicable, but skip price fields
            sub_field = ROW_FIELDS["substituted"].format(n=row)
            if item.get("is_substitute"):
                sub_text = desc_clean or item.get("description", "")
                _sub_mfg = (item.get("mfg_number") or pricing.get("mfg_number")
                             or pricing.get("manufacturer_part") or "")
                if _sub_mfg:
                    sub_text = f"{_sub_mfg} — {sub_text}" if sub_text else _sub_mfg
                field_values.append({"field_id": sub_field, "page": 1, "value": sub_text[:120]})
            continue  # Skip price/extension fields

        qty_per_uom = item.get("qty_per_uom", 1)
        extension = round(unit_price * qty, 2)
        subtotal += extension
        items_priced += 1
        log.info("fill_ams704 WRITE row=%d: desc='%s' price=%.2f qty=%d ext=%.2f",
                 row, (desc_final or "")[:40], unit_price, qty, extension)

        # Fill price and extension
        price_field = ROW_FIELDS["unit_price"].format(n=row)
        ext_field = ROW_FIELDS["extension"].format(n=row)

        field_values.append({
            "field_id": price_field,
            "page": 1,
            "value": f"{unit_price:,.2f}",
        })
        field_values.append({
            "field_id": ext_field,
            "page": 1,
            "value": f"{extension:,.2f}",
        })

        # Fill SUBSTITUTED ITEM column: only when quoting a replacement/substitute item
        # (controlled by the "Sub?" checkbox on the pricecheck detail page)
        sub_field = ROW_FIELDS["substituted"].format(n=row)
        if item.get("is_substitute"):
            # Use the description of what we're actually quoting (the substitute)
            sub_text = desc_clean or item.get("description", "")
            # Prepend MFG# if available and not already in description
            _sub_mfg = (item.get("mfg_number") or pricing.get("mfg_number")
                         or pricing.get("manufacturer_part") or "")
            if _sub_mfg and _sub_mfg.lower() not in sub_text.lower():
                sub_text = f"MFG#: {_sub_mfg}\n{sub_text}"
            sub_text = sub_text.strip()[:120]
            if sub_text:
                field_values.append({
                    "field_id": sub_field,
                    "page": 1,
                    "value": sub_text,
                })
        else:
            # Clear any pre-filled substituted text from original 704A
            # Use space (not empty string) to force pypdf to overwrite the field
            field_values.append({
                "field_id": sub_field,
                "page": 1,
                "value": " ",
            })

    # Clear unused rows to prevent ghost data from previous fills
    if not original_mode:
        filled_rows = occupied_rows | overflow_rows

        for empty_row in range(1, max_row + 1):
            if empty_row in filled_rows:
                continue
            # Blank out all fields for this row
            for key, pattern in ROW_FIELDS.items():
                field_values.append({
                    "field_id": pattern.format(n=empty_row),
                    "page": 1,
                    "value": " ",  # Space forces pypdf to overwrite (empty string may not)
                })

        log.info("fill_ams704: cleared %d unused rows (filled: %d items + %d overflow)",
                 max_row - len(filled_rows), len(occupied_rows), len(overflow_rows))
    else:
        log.info("fill_ams704 ORIGINAL MODE: skipped row clearing (preserving buyer fields)")

    # Totals
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)

    field_values.append({"field_id": "fill_70", "page": 1, "value": f"{subtotal:,.2f}"})
    field_values.append({"field_id": "fill_71", "page": 1, "value": "0.00"})  # Freight
    field_values.append({"field_id": "fill_72", "page": 1, "value": f"{tax:,.2f}"})
    field_values.append({"field_id": "fill_73", "page": 1, "value": f"{total:,.2f}"})

    # Notes — user-editable, no default
    if not original_mode:
        field_values.append({"field_id": "Supplier andor Requestor Notes", "page": 1, "value": custom_notes or ""})

    # Write field_values.json and use fill script
    fv_path = os.path.join(DATA_DIR, "pc_field_values.json")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(fv_path, "w") as f:
        json.dump(field_values, f, indent=2)

    # Fill the PDF
    try:
        _fill_pdf_fields(source_pdf, field_values, output_pdf)
    except Exception as e:
        return {"ok": False, "error": f"PDF fill error: {e}"}

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


def _fill_pdf_text_overlay(source_pdf: str, field_values: list, output_pdf: str):
    """
    Fallback for flat/DocuSign PDFs with no fillable form fields.
    Uses exact coordinates from AMS 704 Rev 1/2019 template.
    ONLY draws: supplier info + pricing + totals. Never touches item descriptions.
    Page type by index: 0,2,4=page-1-format  1,3,5=continuation.
    """
    import io
    import re as _re
    from reportlab.pdfgen import canvas as rl_canvas

    reader = PdfReader(source_pdf)
    writer = PdfWriter()
    writer.append(reader)

    fv_map = {fv["field_id"]: fv.get("value", "") for fv in field_values}

    # ═══ COORDINATES from PC_Karaoke_Reytech.pdf (real AMS 704 template) ═══
    SUPPLIER_FIELDS = {
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
        "Ship to":                            (277.6, 74.2, 526.1, 89.2),
    }
    NOTES_FIELD = ("Supplier andor Requestor Notes", 32.6, 41.9, 237.2, 118.9)
    TOTALS = {
        "fill_70": (696.0, 113.8, 760.0, 134.0),
        "fill_71": (694.9, 89.9, 760.0, 112.3),
        "fill_72": (695.4, 67.4, 760.0, 88.6),
        "fill_73": (695.5, 41.9, 760.0, 66.0),
    }
    CHECKBOX = ("Check Box4", 244.5, 95.1, 257.6, 106.7)

    # Price + Extension column X ranges (tight — only these two columns)
    PRICE_X = (637.0, 686.0)   # inset 2pt from annotation edges to avoid border overlap
    EXT_X = (691.0, 754.0)     # inset 2pt from annotation edges to avoid border overlap

    # Page-1-format: 8 rows starting at y=300
    PG1_ROWS = [(300.1 - i * 22.5, 321.2 - i * 22.5) for i in range(8)]
    # Continuation-format: 11 rows starting at y=459
    PG2_ROWS = [(459.0 - i * 22.5, 480.0 - i * 22.5) for i in range(11)]
    # Continuation header: SUPPLIER NAME area
    PG2_SUPPLIER = (330.0, 523.0, 760.0, 550.0)

    def _cell(c, x1, y1, x2, y2, text, fs=9):
        """Draw text in a tight white-masked cell."""
        if not text or not text.strip():
            return
        text = text.strip()
        w, h = x2 - x1, y2 - y1
        if w <= 0 or h <= 0:
            return
        c.saveState()
        c.setFillColorRGB(1, 1, 1)
        c.rect(x1 + 2, y1 + 1, w - 4, h - 2, fill=1, stroke=0)
        c.restoreState()
        fs = min(fs, h * 0.75)
        c.setFont("Helvetica", fs)
        while c.stringWidth(text, "Helvetica", fs) > w - 2 and fs > 4.5:
            fs -= 0.5
            c.setFont("Helvetica", fs)
        c.drawString(x1 + 1, y1 + (h - fs) / 2, text)

    def _cell_right(c, x1, y1, x2, y2, text, fs=9):
        """Draw RIGHT-ALIGNED text in a tight white-masked cell. For prices/currency."""
        if not text or not text.strip():
            return
        text = text.strip()
        w, h = x2 - x1, y2 - y1
        if w <= 0 or h <= 0:
            return
        c.saveState()
        c.setFillColorRGB(1, 1, 1)
        c.rect(x1 + 2, y1 + 1, w - 4, h - 2, fill=1, stroke=0)
        c.restoreState()
        fs = min(fs, h * 0.75)
        c.setFont("Helvetica", fs)
        while c.stringWidth(text, "Helvetica", fs) > w - 4 and fs > 4.5:
            fs -= 0.5
            c.setFont("Helvetica", fs)
        text_w = c.stringWidth(text, "Helvetica", fs)
        c.drawString(x2 - text_w - 2, y1 + (h - fs) / 2, text)

    def _multiline(c, x1, y1, x2, y2, text, fs=8):
        """Draw multi-line text with white mask."""
        if not text or not text.strip():
            return
        w, h = x2 - x1, y2 - y1
        c.saveState()
        c.setFillColorRGB(1, 1, 1)
        c.rect(x1 + 2, y1 + 1, w - 4, h - 2, fill=1, stroke=0)
        c.restoreState()
        fs = min(fs, h * 0.6)
        c.setFont("Helvetica", fs)
        for i, line in enumerate(text.strip().split("\n")[:5]):
            ly = y2 - (fs + 2) - (i * (fs + 1.5))
            if ly < y1:
                break
            t = line.strip()
            while c.stringWidth(t, "Helvetica", fs) > w - 2 and len(t) > 3:
                t = t[:-1]
            c.drawString(x1 + 1, ly, t)

    # Find highest priced row to skip empty trailing pages
    max_row = 0
    for fv in field_values:
        m = _re.search(r'Row(\d+)', fv["field_id"])
        if m and fv.get("value", "").strip():
            max_row = max(max_row, int(m.group(1)))

    num_pages = len(reader.pages)
    current_row = 1

    log.info("OVERLAY: %d pages, max_row=%d, %d field_values", num_pages, max_row, len(field_values))

    for pg_idx in range(num_pages):
        page = reader.pages[pg_idx]
        mb = page.mediabox
        pw, ph = float(mb.width), float(mb.height)

        # Page type: 0,2,4=page-1-format  1,3,5=continuation
        is_pg1 = (pg_idx % 2 == 0)
        rows = PG1_ROWS if is_pg1 else PG2_ROWS

        # Skip if all rows on this page are beyond our data
        page_first_row = current_row
        if page_first_row > max_row and pg_idx > 0:
            log.info("OVERLAY pg%d: skip (rows start at %d, max=%d)", pg_idx, page_first_row, max_row)
            current_row += len(rows)
            continue

        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(pw, ph))
        drew = False

        # ── SUPPLIER INFO (page-1-format only) ──
        if is_pg1:
            for fname, (x1, y1, x2, y2) in SUPPLIER_FIELDS.items():
                val = fv_map.get(fname, "")
                if val:
                    _cell(c, x1, y1, x2, y2, val, fs=10)
                    drew = True
            # Notes
            nf, nx1, ny1, nx2, ny2 = NOTES_FIELD
            nval = fv_map.get(nf, "")
            if nval:
                _multiline(c, nx1, ny1, nx2, ny2, nval, fs=8)
                drew = True
            # Checkbox
            cf, cx1, cy1, cx2, cy2 = CHECKBOX
            if fv_map.get(cf) in ("/Yes", "Yes", True, "True"):
                c.saveState()
                c.setFillColorRGB(1, 1, 1)
                c.rect(cx1 + 1, cy1 + 1, cx2 - cx1 - 2, cy2 - cy1 - 2, fill=1, stroke=0)
                c.restoreState()
                c.setFont("ZapfDingbats", 10)
                c.drawString(cx1 + 1, cy1 + 1, "4")
                drew = True
            # Totals (first page only)
            if pg_idx == 0:
                for fname, (x1, y1, x2, y2) in TOTALS.items():
                    val = fv_map.get(fname, "")
                    if val:
                        _cell_right(c, x1, y1, x2, y2, val, fs=10)
                        drew = True

        # ── CONTINUATION HEADER (mask + fill SUPPLIER NAME) ──
        if not is_pg1:
            company = fv_map.get("COMPANY NAME", "")
            if company:
                _cell(c, PG2_SUPPLIER[0], PG2_SUPPLIER[1], PG2_SUPPLIER[2], PG2_SUPPLIER[3],
                      company, fs=12)
                drew = True

        # ── ROW PRICING: only PRICE PER UNIT + EXTENSION columns ──
        for slot_idx, (y_bot, y_top) in enumerate(rows):
            rn = current_row + slot_idx
            # Price
            pf = ROW_FIELDS["unit_price"].format(n=rn)
            pv = fv_map.get(pf, "")
            if pv and pv.strip():
                _cell_right(c, PRICE_X[0], y_bot, PRICE_X[1], y_top, pv, fs=9)
                drew = True
            # Extension
            ef = ROW_FIELDS["extension"].format(n=rn)
            ev = fv_map.get(ef, "")
            if ev and ev.strip():
                _cell_right(c, EXT_X[0], y_bot, EXT_X[1], y_top, ev, fs=9)
                drew = True

        log.info("OVERLAY pg%d: %s rows=%d-%d drew=%s",
                 pg_idx, "pg1" if is_pg1 else "cont",
                 current_row, current_row + len(rows) - 1, drew)
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
    for pg_idx in range(len(reader.pages)):
        is_pg1 = (pg_idx % 2 == 0)
        rows_on_page = 8 if is_pg1 else 11
        page_first = check_row
        page_last = check_row + rows_on_page - 1
        has_items = False
        for rn in range(page_first, page_last + 1):
            pf = ROW_FIELDS["unit_price"].format(n=rn)
            if fv_map.get(pf, "").strip():
                has_items = True
                break
        if has_items or pg_idx == 0:  # always keep page 1
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

    _add_signature_to_pdf(writer)
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

    from pypdf.generic import NameObject, TextStringObject, ArrayObject, DictionaryObject

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
            except Exception:
                pass

    for page_num in range(len(writer.pages)):
        writer.update_page_form_field_values(
            writer.pages[page_num],
            text_values,
            auto_regenerate=True,
        )

    for page in writer.pages:
        for annot_ref in (page.get("/Annots") or []):
            try:
                annot = annot_ref.get_object()
                name = str(annot.get("/T", ""))
                if name in checkbox_fields:
                    state = checkbox_fields[name]
                    annot[NameObject("/V")] = NameObject(f"/{state}")
                    annot[NameObject("/AS")] = NameObject(f"/{state}")
            except Exception:
                pass

    _add_signature_to_pdf(writer)

    with open(output_pdf, "wb") as f:
        writer.write(f)

    log.info(f"Filled AMS 704 saved to {output_pdf}")


def _add_signature_to_pdf(writer):
    """Overlay signature image and date onto the Signature field."""
    import io

    # Signature field: Rect=[279.144, 388.486, 602.284, 412.005] on landscape page (792x612)
    # But PDF coords have y=0 at bottom, so we need to flip for reportlab
    SIG_LEFT = 279.0
    SIG_BOTTOM = 386.0  # slight adjustment for visual centering
    SIG_WIDTH = 160.0   # signature image width
    SIG_HEIGHT = 28.0   # signature image height

    # Date goes to the right of signature
    DATE_X = 470.0
    DATE_Y = 392.0

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
        from reportlab.lib.pagesizes import landscape, letter
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.utils import ImageReader

        # Create overlay page matching AMS 704 dimensions (landscape letter)
        page_width, page_height = 792.0, 612.0
        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(page_width, page_height))

        # Draw signature image if available
        if sig_path:
            try:
                img = ImageReader(sig_path)
                c.drawImage(img, SIG_LEFT, SIG_BOTTOM, width=SIG_WIDTH, height=SIG_HEIGHT,
                           mask='auto', preserveAspectRatio=True, anchor='sw')
            except Exception as e:
                log.warning(f"Could not draw signature image: {e}")

        # Draw today's date
        today = datetime.now().strftime("%-m/%-d/%Y")
        c.setFont("Helvetica", 10)
        c.drawString(DATE_X, DATE_Y, today)

        c.save()
        buf.seek(0)

        # Merge overlay onto first page
        overlay_reader = PdfReader(buf)
        overlay_page = overlay_reader.pages[0]
        writer.pages[0].merge_page(overlay_page)

    except ImportError:
        # reportlab not available — just set text field
        log.warning("reportlab not available, setting Signature1 as text")
        today = datetime.now().strftime("%-m/%-d/%Y")
        text_values = {"Signature1": f"Michael Guadan  {today}"}
        writer.update_page_form_field_values(writer.pages[0], text_values, auto_regenerate=False)
    except Exception as e:
        log.error(f"Signature overlay error: {e}")


def _expiry_date() -> str:
    """Generate an expiry date 45 days from now."""
    from datetime import timedelta
    exp = datetime.now() + timedelta(days=45)
    return exp.strftime("%-m/%-d/%Y")


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
