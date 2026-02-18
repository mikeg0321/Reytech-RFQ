


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
        from product_research import research_product, quick_lookup
        HAS_RESEARCH = True
    except ImportError:
        HAS_RESEARCH = False

try:
    from src.knowledge.pricing_oracle import recommend_price
    HAS_ORACLE = True
except ImportError:
    try:
        from pricing_oracle import recommend_price
        HAS_ORACLE = True
    except ImportError:
        HAS_ORACLE = False

try:
    from src.knowledge.won_quotes_db import find_similar_items
    HAS_WON_QUOTES = True
except ImportError:
    try:
        from won_quotes_db import find_similar_items
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

    # Extract line items (check rows 1-8 per page, could have multiple pages)
    for row_num in range(1, MAX_ROWS_PER_PAGE + 1):
        row_data = {}
        has_data = False

        for key, pattern in ROW_FIELDS.items():
            field_name = pattern.format(n=row_num)
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
                "row_index": row_num,
            }
            result["line_items"].append(item)

            # Check for existing price
            if row_data.get("unit_price"):
                try:
                    price = float(row_data["unit_price"].replace("$", "").replace(",", ""))
                    result["existing_prices"][row_num] = price
                except (ValueError, TypeError):
                    pass

    return result


def _parse_ams704_ocr(pdf_path: str, result: dict) -> dict:
    """
    Fallback: parse non-fillable AMS 704 via text extraction.
    Uses pdfplumber for better table extraction.
    """
    try:
        import pdfplumber
    except ImportError:
        result["error"] = "pdfplumber not available for OCR fallback"
        return result

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text() or ""

                # Extract header info from text
                if page_num == 0:
                    _extract_header_from_text(text, result)

                # Extract tables for line items
                tables = page.extract_tables()
                for table in tables:
                    _extract_items_from_table(table, result, page_num)

    except Exception as e:
        result["error"] = f"OCR parse error: {e}"
        log.error(f"OCR parse error: {e}", exc_info=True)

    return result


def _extract_header_from_text(text: str, result: dict):
    """Extract header fields from raw text."""
    lines = text.split("\n")
    for line in lines:
        line_lower = line.lower()

        # Price Check #
        m = re.search(r'price\s+check\s*#?\s*(.+?)(?:due|$)', line, re.I)
        if m:
            result["header"]["price_check_number"] = m.group(1).strip()

        # Requestor
        m = re.search(r'requestor\s+(.+?)(?:institution|$)', line, re.I)
        if m:
            result["header"]["requestor"] = m.group(1).strip()

        # Institution
        m = re.search(r'institution.*?program\s+(.+?)(?:delivery|$)', line, re.I)
        if m:
            result["header"]["institution"] = m.group(1).strip()

        # Ship to
        m = re.search(r'ship\s+to:?\s*(.+?)(?:\(|$)', line, re.I)
        if m:
            result["ship_to"] = m.group(1).strip()

        # Due date
        m = re.search(r'date:?\s*(\d{1,2}/\d{1,2}/\d{2,4})', line, re.I)
        if m and "due_date" not in result["header"]:
            result["header"]["due_date"] = m.group(1)


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

        qty_str = str(row[col_map.get("qty", 1)] or "1")
        try:
            qty = int(float(qty_str))
        except Exception:
            qty = 1

        row_num = len(result["line_items"]) + 1 + (page_num * MAX_ROWS_PER_PAGE)

        item = {
            "item_number": str(row[col_map.get("item_number", 0)] or row_num),
            "qty": qty,
            "uom": str(row[col_map.get("uom", "")] or "ea").upper(),
            "qty_per_uom": 1,
            "description": desc.strip(),
            "row_index": row_num,
        }
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

    for item in items:
        row = item.get("row_index", 0)
        if row < 1 or row > MAX_ROWS_PER_PAGE:
            continue

        pricing = item.get("pricing", {})

        # Select price based on tier
        unit_price = pricing.get("recommended_price")
        if not unit_price:
            unit_price = pricing.get("amazon_price")
        if not unit_price:
            continue  # Can't price this item

        qty = item.get("qty", 1)
        qty_per_uom = item.get("qty_per_uom", 1)
        extension = round(unit_price * qty, 2)
        subtotal += extension
        items_priced += 1

        # Overwrite description with cleaned version (strips font specs, dimensions, etc.)
        desc_raw = item.get("description_raw") or item.get("description", "")
        desc_clean = clean_description(desc_raw) if desc_raw else ""
        if desc_clean:
            desc_field = ROW_FIELDS["description"].format(n=row)
            # Issue 1: Append MFG/ASIN if available
            mfg_num = (pricing.get("mfg_number") or pricing.get("manufacturer_part") 
                       or item.get("mfg_number") or "")
            asin = pricing.get("amazon_asin", "")
            if mfg_num and mfg_num.lower() not in desc_clean.lower():
                desc_clean = f"{desc_clean}\nMFG#: {mfg_num}"
            elif asin and asin not in desc_clean:
                desc_clean = f"{desc_clean}\nASIN: {asin}"
            field_values.append({
                "field_id": desc_field,
                "page": 1,
                "value": desc_clean,
            })

        # Issue 3: Write UOM (uppercase) to PDF
        uom_val = (item.get("uom") or "EA").upper()
        uom_field = ROW_FIELDS["uom"].format(n=row)
        field_values.append({
            "field_id": uom_field,
            "page": 1,
            "value": uom_val,
        })

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

        # Add substituted item info if we found an Amazon match
        if pricing.get("amazon_title"):
            sub_field = ROW_FIELDS["substituted"].format(n=row)
            sub_text = pricing["amazon_title"][:80]
            field_values.append({
                "field_id": sub_field,
                "page": 1,
                "value": sub_text,
            })

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
