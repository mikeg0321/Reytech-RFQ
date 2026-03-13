


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
    max_row_check = 24
    
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
            
            # Override with explicit numbers if present
            if extra1 and extra2:
                qty = max(extra1, extra2)
                qty_per_uom = min(extra1, extra2)
            elif extra1:
                # "110" after "each" = qty_per_uom(1) + qty(10) jammed together
                s = str(extra1)
                if len(s) >= 2 and int(s[0]) <= 3 and int(s[1:]) <= 150:
                    qty_per_uom = int(s[0])
                    qty = int(s[1:])
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
    
    # Post-process: try to fix items with missing qty from nearby number fragments
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

    # FOB Destination, Freight Prepaid checkbox
    field_values.append({
        "field_id": "Check Box4",
        "page": 1,
        "value": "/Yes",
    })

    # Line items with pricing
    subtotal = 0.0
    items_priced = 0

    seq = 0  # sequential line item counter
    _skipped_no_row = 0
    _skipped_no_price = 0
    max_row = 24  # Support up to 3 pages (8 rows each)
    for item_idx, item in enumerate(items):
        row = item.get("row_index") or (item_idx + 1)  # default to 1-based position
        if row < 1 or row > max_row:
            _skipped_no_row += 1
            log.debug("fill_ams704 SKIP item (bad row_index=%s): desc='%s'",
                       row, (item.get("description") or "")[:40])
            continue

        pricing = item.get("pricing", {})
        seq += 1
        qty = item.get("qty", 1)

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
    filled_rows = set()
    for item_idx, item in enumerate(items):
        r = item.get("row_index") or (item_idx + 1)
        if 1 <= r <= max_row:
            filled_rows.add(r)
    
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
    
    log.info("fill_ams704: cleared %d unused rows (filled: %s)", 
             max_row - len(filled_rows), sorted(filled_rows))

    # Totals
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)

    field_values.append({"field_id": "fill_70", "page": 1, "value": f"{subtotal:,.2f}"})
    field_values.append({"field_id": "fill_71", "page": 1, "value": "0.00"})  # Freight
    field_values.append({"field_id": "fill_72", "page": 1, "value": f"{tax:,.2f}"})
    field_values.append({"field_id": "fill_73", "page": 1, "value": f"{total:,.2f}"})

    # Notes — user-editable, no default
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

    log.info("fill_ams704 COMPLETE: %d/%d items priced, %d skipped(no row), %d skipped(no price), "
             "subtotal=$%.2f, %d field_values written to %s",
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


def _fill_pdf_fields(source_pdf: str, field_values: list, output_pdf: str):
    """
    Fill PDF form fields with auto-fit font sizing.
    
    Strategy:
    1. Read all field rects from source PDF
    2. Calculate optimal font size per field based on content vs width
    3. Set /DA with correct font size on each annotation
    4. Fill values with auto_regenerate=True so pypdf builds appearance streams
    """
    reader = PdfReader(source_pdf)
    writer = PdfWriter()
    writer.append(reader)

    from pypdf.generic import NameObject, TextStringObject, ArrayObject

    # Step 0: Strip non-form annotations (stamps, popups, freetext placed by requestors)
    for page in writer.pages:
        annots = page.get("/Annots")
        if not annots:
            continue
        cleaned = []
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                subtype = str(annot.get("/Subtype", ""))
                # Keep only form widgets, remove stamps/popups/freetext/etc.
                if subtype == "/Widget":
                    cleaned.append(annot_ref)
                else:
                    log.debug(f"Stripping annotation: {subtype} T={annot.get('/T','')}")
            except Exception:
                cleaned.append(annot_ref)  # Keep if we can't inspect
        page[NameObject("/Annots")] = ArrayObject(cleaned)

    # Separate checkbox values from text values
    checkbox_fields = {}
    text_values = {}
    for fv in field_values:
        if fv.get("value") in ("/Yes", "/Off", "Yes", "Off"):
            checkbox_fields[fv["field_id"]] = fv["value"].replace("/", "")
        else:
            text_values[fv["field_id"]] = fv["value"]

    # Step 1: Build field width map from source annotations
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

    # Step 2: Calculate optimal font size for each text value
    def calc_font_size(text: str, field_width: float, max_size: float = 12.0, min_size: float = 6.0) -> float:
        """Calculate largest font that fits text in field width."""
        if not text or field_width <= 0:
            return max_size
        # Helvetica average char width ≈ 0.52 × font_size (mixed case)
        # Narrower for digits: ≈ 0.50 × font_size
        is_numeric = all(c in '0123456789.,$' for c in text.strip())
        char_factor = 0.50 if is_numeric else 0.52
        padding = 4  # 2pt each side
        usable = field_width - padding
        ideal = usable / (len(text) * char_factor)
        return max(min_size, min(max_size, ideal))

    # Step 3: Set /DA with calculated font size on writer's annotations BEFORE filling
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

    # Step 4: Fill values with auto_regenerate=True (builds appearance streams using our /DA)
    for page_num in range(len(writer.pages)):
        writer.update_page_form_field_values(
            writer.pages[page_num],
            text_values,
            auto_regenerate=True,
        )

    # Step 5: Set checkbox states
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

    # Step 6: Add signature image + date
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
