"""
generic_rfq_parser.py — Parse line items from unstructured RFQ PDFs.

For agencies that DON'T use AMS 704/703 forms (e.g. Cal Vet, DGS, CalFire).
Extracts tabular line items, solicitation info, and buyer details from
free-form PDFs using text extraction + pattern matching.

LEARNING: Successful parses are stored in agency_parse_profiles table
so the system gets smarter over time.
"""

import re
import os
import json
import logging
import sqlite3
from datetime import datetime

log = logging.getLogger("generic_rfq_parser")

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None


# ═══════════════════════════════════════════════════════════════════════════════
# Agency Detection
# ═══════════════════════════════════════════════════════════════════════════════

AGENCY_SIGNATURES = {
    "calvet": {
        "name": "CalVet",
        "full_name": "California Department of Veterans Affairs",
        "domains": ["calvet.ca.gov"],
        "keywords": ["calvet", "veterans affairs", "veterans home", "cal vet",
                     "department of veterans", "dvbe", "yountville", "barstow",
                     "chula vista", "fresno", "lancaster", "ventura",
                     "west los angeles", "redding"],
        "form_type": "generic_rfq",  # No 704 forms
        "quote_type": "formal",      # Needs full Reytech quote
    },
    "calfire": {
        "name": "CAL FIRE",
        "full_name": "California Department of Forestry and Fire Protection",
        "domains": ["fire.ca.gov"],
        "keywords": ["cal fire", "calfire", "forestry", "fire protection"],
        "form_type": "generic_rfq",
        "quote_type": "formal",
    },
    "dgs": {
        "name": "DGS",
        "full_name": "Department of General Services",
        "domains": ["dgs.ca.gov"],
        "keywords": ["general services", "dgs", "procurement division"],
        "form_type": "generic_rfq",
        "quote_type": "formal",
    },
    "cchcs": {
        "name": "CCHCS",
        "full_name": "California Correctional Health Care Services",
        "domains": ["cchcs.ca.gov", "cdcr.ca.gov"],
        "keywords": ["cchcs", "cdcr", "correctional", "prison", "state prison"],
        "form_type": "ams_704",  # Uses 704 forms
        "quote_type": "704b_fill",
    },
    "dsh": {
        "name": "DSH",
        "full_name": "Department of State Hospitals",
        "domains": ["dsh.ca.gov"],
        "keywords": ["state hospital", "dsh", "napa state", "patton state",
                     "atascadero", "coalinga", "metropolitan"],
        "form_type": "ams_704",
        "quote_type": "704b_fill",
    },
}


def detect_agency(subject="", body="", sender_email="", pdf_text=""):
    """Detect which CA agency this RFQ is from."""
    combined = f"{subject} {body} {sender_email} {pdf_text}".lower()

    for key, sig in AGENCY_SIGNATURES.items():
        # Check sender domain
        for domain in sig["domains"]:
            if domain in sender_email.lower():
                return key, sig

        # Check keywords
        matches = sum(1 for kw in sig["keywords"] if kw in combined)
        if matches >= 2:
            return key, sig

    return "unknown", {"name": "Unknown", "form_type": "generic_rfq", "quote_type": "formal"}


# ═══════════════════════════════════════════════════════════════════════════════
# Generic PDF Text Extraction
# ═══════════════════════════════════════════════════════════════════════════════

def extract_pdf_text(pdf_path):
    """Extract all text from a PDF, page by page."""
    if not PdfReader or not os.path.exists(pdf_path):
        return ""
    try:
        reader = PdfReader(pdf_path)
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n--- PAGE BREAK ---\n\n".join(pages)
    except Exception as e:
        log.warning("PDF text extraction failed for %s: %s", pdf_path, e)
        return ""


# ═══════════════════════════════════════════════════════════════════════════════
# Line Item Extraction — Heuristic Patterns
# ═══════════════════════════════════════════════════════════════════════════════

def parse_line_items_from_text(text):
    """Extract line items from unstructured PDF text using multiple strategies."""
    items = []

    # Strategy 1: Tabular rows with line number, qty, description, price
    # Pattern: "1  2  EA  Widget description here  $123.45"
    tabular = re.findall(
        r'^\s*(\d{1,3})\s+'           # Line number
        r'(\d{1,6})\s+'               # Quantity
        r'([A-Z]{1,6})\s+'            # UOM (EA, BX, CS, PK, etc.)
        r'(.{10,200}?)\s+'            # Description (lazy)
        r'\$([\d,]+\.\d{2})',          # Price (MUST have $ and .XX)
        text, re.MULTILINE
    )
    for match in tabular:
        items.append({
            "line_number": int(match[0]),
            "qty": int(match[1]),
            "uom": match[2].strip(),
            "description": match[3].strip(),
            "unit_price": _parse_price(match[4]),
            "parse_method": "tabular",
        })

    if items:
        return _deduplicate_items(items)

    # Strategy 1b: Tabular rows WITHOUT price (just line#, qty, uom, description)
    tabular_no_price = re.findall(
        r'^\s*(\d{1,3})\s+'           # Line number
        r'(\d{1,6})\s+'               # Quantity
        r'([A-Z]{1,6})\s+'            # UOM
        r'(.{10,200}?)$',             # Description (to end of line)
        text, re.MULTILINE
    )
    for match in tabular_no_price:
        desc = match[3].strip()
        price = _extract_price_from_text(desc)
        items.append({
            "line_number": int(match[0]),
            "qty": int(match[1]),
            "uom": match[2].strip(),
            "description": _clean_description(desc) if price else desc,
            "unit_price": price,
            "parse_method": "tabular_no_price",
        })

    if items:
        return _deduplicate_items(items)

    # Strategy 2: "Item X:" or numbered list patterns
    # "1. Description of product\n   Qty: 5  UOM: EA"
    numbered = re.findall(
        r'(?:Item\s*#?\s*|Line\s*#?\s*)?(\d{1,3})[.):]\s*'  # Item/Line number
        r'(.{10,200}?)(?:\n|$)',                               # Description
        text, re.MULTILINE
    )
    if numbered:
        lines_list = text.split('\n')
        for num, desc in numbered:
            # Look ahead: join next 2 lines to capture Qty/UOM on separate lines
            desc_idx = None
            for li, line in enumerate(lines_list):
                if desc.strip() and desc.strip()[:20] in line:
                    desc_idx = li
                    break
            extended = desc
            if desc_idx is not None:
                lookahead = lines_list[desc_idx+1:desc_idx+3]
                extended = desc + " " + " ".join(l.strip() for l in lookahead)

            qty, uom = _extract_qty_from_text(extended)
            price = _extract_price_from_text(extended)
            if desc.strip() and not _is_header_text(desc):
                items.append({
                    "line_number": int(num),
                    "qty": qty or 1,
                    "uom": uom,
                    "description": _clean_description(desc),
                    "unit_price": price,
                    "parse_method": "numbered_list",
                })

    if items:
        return _deduplicate_items(items)

    # Strategy 3: Look for product description blocks with part numbers
    # "MFG# 12345  Description of product  Qty: 5"
    product_blocks = re.findall(
        r'(?:MFG|MPN|P/N|Part|Item|Model|Cat|Catalog)\s*#?\s*:?\s*'
        r'([A-Z0-9][\w\-./]{3,30})\s+'    # Part/MFG number
        r'(.{10,200})',                     # Description
        text, re.IGNORECASE | re.MULTILINE
    )
    for i, (part, desc) in enumerate(product_blocks):
        qty, uom = _extract_qty_from_text(desc)
        price = _extract_price_from_text(desc)
        items.append({
            "line_number": i + 1,
            "qty": qty or 1,
            "uom": uom,
            "item_number": part.strip(),
            "description": _clean_description(desc),
            "unit_price": price,
            "parse_method": "product_block",
        })

    if items:
        return _deduplicate_items(items)

    # Strategy 4: Lines that look like product descriptions
    # Fallback — any line with enough substance
    lines = text.split('\n')
    candidate_items = []
    for line in lines:
        line = line.strip()
        if len(line) < 15 or len(line) > 300:
            continue
        if _is_header_text(line):
            continue
        # Must contain at least one number (qty or price)
        if not re.search(r'\d', line):
            continue
        # Should not be all caps header
        if line == line.upper() and len(line) < 50:
            continue
        qty, uom = _extract_qty_from_text(line)
        price = _extract_price_from_text(line)
        if qty > 0 or price > 0:
            candidate_items.append({
                "line_number": len(candidate_items) + 1,
                "qty": qty or 1,
                "uom": uom,
                "description": _clean_description(line),
                "unit_price": price,
                "parse_method": "line_scan",
            })

    return _deduplicate_items(candidate_items)


def _extract_qty_from_text(text):
    """Extract quantity and UOM from text."""
    uom_map = {"ea": "EA", "each": "EA", "bx": "BX", "box": "BX", "pk": "PK",
               "pack": "PK", "cs": "CS", "case": "CS", "set": "SET", "pr": "PR",
               "pair": "PR", "dz": "DZ", "dozen": "DZ", "bg": "BG", "bag": "BG",
               "rl": "RL", "roll": "RL", "bt": "BT", "bottle": "BT", "ct": "CT"}
    _uom_words = "EA|EACH|BX|BOX|PK|PACK|CS|CASE|SET|PR|PAIR|DZ|DOZEN|BG|BAG|RL|ROLL|BT|BOTTLE|CT"

    # Pattern: "Qty: 5 EA" or "Quantity: 5 PR" (qty label + number + optional UOM)
    qty_match = re.search(
        r'(?:qty|quantity|quan)\s*:?\s*(\d{1,5})\s*(' + _uom_words + r')?\b',
        text, re.IGNORECASE
    )
    if qty_match:
        qty = int(qty_match.group(1))
        if qty_match.group(2):
            u = qty_match.group(2).lower()
            return qty, uom_map.get(u, u.upper()[:4])
        # Check for separate UOM: label
        uom_match = re.search(r'(?:uom|unit)\s*:?\s*(\w+)', text, re.IGNORECASE)
        if uom_match:
            u = uom_match.group(1).lower()
            return qty, uom_map.get(u, u.upper()[:4])
        return qty, "EA"

    # Pattern: "5 EA" or "10 EACH" or "3 BX"
    qty_uom = re.search(r'(\d{1,5})\s+(' + _uom_words + r')\b', text, re.IGNORECASE)
    if qty_uom:
        qty = int(qty_uom.group(1))
        u = qty_uom.group(2).lower()
        return qty, uom_map.get(u, u.upper()[:4])

    return 0, "EA"


def _extract_price_from_text(text):
    """Extract price from text."""
    # Pattern: $123.45 or $1,234.56
    price_match = re.search(r'\$\s*([\d,]+\.?\d{0,2})', text)
    if price_match:
        return _parse_price(price_match.group(1))
    return 0


def _parse_price(s):
    """Parse price string to float."""
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _clean_description(text):
    """Clean up a description string."""
    # Remove price, qty, uom from description
    text = re.sub(r'\$[\d,.]+', '', text)
    text = re.sub(r'\b(?:qty|quantity)\s*:?\s*\d+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\b\d+\s*(?:EA|EACH|BX|BOX|PK|CS)\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _is_header_text(text):
    """Check if text is a header/footer/boilerplate."""
    lower = text.lower().strip()
    headers = [
        "page ", "total", "subtotal", "grand total", "terms", "conditions",
        "signature", "date:", "vendor", "ship to", "bill to", "attn:",
        "phone:", "fax:", "email:", "address:", "confidential",
        "request for quotation", "request for quote", "rfq", "purchase order",
        "all rights reserved", "copyright", "www.", "http",
    ]
    return any(lower.startswith(h) or lower == h for h in headers)


def _deduplicate_items(items):
    """Remove duplicate items based on description similarity."""
    if not items:
        return items
    seen = set()
    unique = []
    for item in items:
        key = item.get("description", "").lower()[:40]
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


# ═══════════════════════════════════════════════════════════════════════════════
# Solicitation Info Extraction
# ═══════════════════════════════════════════════════════════════════════════════

def extract_solicitation_info(text, subject="", sender=""):
    """Extract solicitation number, due date, buyer info from PDF text."""
    info = {
        "solicitation_number": "",
        "due_date": "",
        "requestor_name": "",
        "requestor_email": "",
        "institution": "",
        "ship_to": "",
        "delivery_days": "",
    }

    # Solicitation number
    sol_patterns = [
        r'(?:solicitation|rfq|bid)\s*(?:#|no\.?|number)?\s*:?\s*(\d{6,10})',
        r'(?:rfq|sol)\s*#?\s*(\d{6,10})',
        r'#\s*(\d{7,10})',
    ]
    for pat in sol_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            info["solicitation_number"] = m.group(1)
            break

    # Also check subject line
    if not info["solicitation_number"]:
        m = re.search(r'(?:sol|rfq|#)\s*(\d{6,10})', subject, re.IGNORECASE)
        if m:
            info["solicitation_number"] = m.group(1)

    # Due date
    date_patterns = [
        r'(?:due|deadline|closing|respond by|submit by|response due)\s*(?:date)?\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?:due|deadline)\s*:?\s*(\w+ \d{1,2},?\s*\d{4})',
    ]
    for pat in date_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            info["due_date"] = m.group(1).strip()
            break

    # Email addresses
    emails = re.findall(r'[\w.+-]+@[\w.-]+\.(?:ca\.gov|gov|com|org)', text)
    if emails:
        # Prefer .ca.gov emails
        gov_emails = [e for e in emails if ".ca.gov" in e.lower()]
        info["requestor_email"] = (gov_emails[0] if gov_emails else emails[0]).lower()

    # Name near "contact" or "buyer" or "requestor"
    name_match = re.search(
        r'(?:contact|buyer|requestor|attn|attention)\s*:?\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,3})',
        text
    )
    if name_match:
        info["requestor_name"] = name_match.group(1).strip()

    # Institution / Facility
    facility_patterns = [
        r'(?:facility|institution|location|ship to|deliver to)\s*:?\s*(.{5,80}?)(?:\n|$)',
        r'(?:veterans home|state hospital|conservation camp|correctional facility)\s*[-—]?\s*(.{3,40})',
    ]
    for pat in facility_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            info["institution"] = m.group(1).strip()
            break

    # Delivery days
    del_match = re.search(r'(?:deliver|delivery|ard|ship)\s*(?:within|by)?\s*:?\s*(\d{1,3})\s*(?:days|calendar)', text, re.IGNORECASE)
    if del_match:
        info["delivery_days"] = del_match.group(1)

    return info


# ═══════════════════════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def parse_generic_rfq(pdf_paths, subject="", sender_email="", body=""):
    """
    Parse line items from non-704 RFQ PDFs.
    Returns dict compatible with the standard RFQ data structure.

    Args:
        pdf_paths: list of PDF file paths
        subject: email subject
        sender_email: sender email address
        body: email body text
    """
    all_text = ""
    all_items = []
    parse_details = []

    for pdf_path in pdf_paths:
        if not os.path.exists(pdf_path):
            continue

        text = extract_pdf_text(pdf_path)
        if not text:
            parse_details.append({"file": os.path.basename(pdf_path), "status": "no_text"})
            continue

        all_text += f"\n\n{text}"

        items = parse_line_items_from_text(text)
        if items:
            parse_details.append({
                "file": os.path.basename(pdf_path),
                "status": "parsed",
                "items_found": len(items),
                "method": items[0].get("parse_method", "unknown") if items else "",
            })
            all_items.extend(items)
        else:
            parse_details.append({
                "file": os.path.basename(pdf_path),
                "status": "no_items",
                "text_length": len(text),
            })

    # Detect agency
    agency_key, agency_info = detect_agency(subject, body, sender_email, all_text)

    # Extract solicitation info
    sol_info = extract_solicitation_info(all_text, subject, sender_email)

    # Ensure items have all required fields
    for i, item in enumerate(all_items):
        item.setdefault("line_number", i + 1)
        item.setdefault("qty", 1)
        item.setdefault("uom", "EA")
        item.setdefault("description", "")
        item.setdefault("item_number", "")
        item.setdefault("unit_price", 0)
        item.setdefault("supplier_cost", 0)
        item.setdefault("scprs_last_price", None)
        item.setdefault("source_type", "general")
        item.setdefault("price_per_unit", item.get("unit_price", 0))

    result = {
        "agency": agency_key,
        "agency_name": agency_info.get("name", "Unknown"),
        "form_type": agency_info.get("form_type", "generic_rfq"),
        "quote_type": agency_info.get("quote_type", "formal"),
        "line_items": all_items,
        "parse_details": parse_details,
        "parsed_at": datetime.now().isoformat(),
        **sol_info,
    }

    # Save parse profile for learning
    _save_parse_profile(agency_key, result, pdf_paths)

    log.info("Generic RFQ parsed: agency=%s, %d items from %d PDFs, sol=%s",
             agency_key, len(all_items), len(pdf_paths), sol_info.get("solicitation_number", "?"))

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Learning: Agency Parse Profiles
# ═══════════════════════════════════════════════════════════════════════════════

def _save_parse_profile(agency_key, result, pdf_paths):
    """Store successful parse patterns for agency learning."""
    if not result.get("line_items"):
        return

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS agency_parse_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency_key TEXT,
                agency_name TEXT,
                solicitation_number TEXT,
                items_found INTEGER,
                parse_methods TEXT,
                pdf_filenames TEXT,
                sample_descriptions TEXT,
                created_at TEXT
            )""")
            methods = list(set(i.get("parse_method", "") for i in result["line_items"]))
            sample_descs = [i.get("description", "")[:80] for i in result["line_items"][:3]]
            conn.execute(
                """INSERT INTO agency_parse_profiles
                (agency_key, agency_name, solicitation_number, items_found,
                 parse_methods, pdf_filenames, sample_descriptions, created_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (agency_key, result.get("agency_name", ""),
                 result.get("solicitation_number", ""),
                 len(result["line_items"]),
                 json.dumps(methods),
                 json.dumps([os.path.basename(p) for p in pdf_paths]),
                 json.dumps(sample_descs),
                 datetime.now().isoformat())
            )
    except Exception as e:
        log.debug("Failed to save parse profile: %s", e)


def get_agency_parse_history(agency_key, limit=10):
    """Get past successful parses for an agency to show patterns."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM agency_parse_profiles
                WHERE agency_key = ? ORDER BY created_at DESC LIMIT ?""",
                (agency_key, limit)
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []
