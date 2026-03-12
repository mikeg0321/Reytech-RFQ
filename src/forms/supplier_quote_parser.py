"""
Supplier Quote PDF Parser
Extracts line items + prices from vendor quotes (Echelon, Cardinal, McKesson, etc.)

Strategy:
1. Extract all text from PDF
2. Find price-bearing lines (contain $ amounts)
3. Parse qty, description, unit price, extended price
4. Return structured items for matching to RFQ line items
"""

import re
import os
import logging
from typing import List, Dict, Optional

log = logging.getLogger("supplier_quote")

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False


def parse_supplier_quote(pdf_path: str) -> dict:
    """
    Parse a supplier quote PDF and extract line items with prices.
    
    Returns:
        {
            "ok": True/False,
            "supplier": "Echelon Distribution" (detected or "Unknown"),
            "quote_number": "Q-12345",
            "quote_date": "03/11/2026",
            "items": [
                {"description": str, "qty": int, "uom": str,
                 "unit_price": float, "extended": float,
                 "part_number": str, "line_number": int},
            ],
            "raw_text": str (first 2000 chars for debugging),
        }
    """
    if not HAS_PYPDF:
        return {"ok": False, "error": "pypdf not available"}
    if not os.path.exists(pdf_path):
        return {"ok": False, "error": f"File not found: {pdf_path}"}

    try:
        reader = PdfReader(pdf_path)
    except Exception as e:
        return {"ok": False, "error": f"Cannot read PDF: {e}"}

    # Extract all text
    full_text = ""
    for page in reader.pages:
        full_text += (page.extract_text() or "") + "\n"

    if len(full_text.strip()) < 20:
        return {"ok": False, "error": "PDF has no extractable text (scanned image?)"}

    # Detect supplier
    supplier = _detect_supplier(full_text)
    
    # Extract quote metadata
    quote_number = _extract_quote_number(full_text)
    quote_date = _extract_date(full_text)

    # Parse line items
    items = _extract_line_items(full_text)

    if not items:
        # Fallback: try page-by-page with different strategies
        for page in reader.pages:
            page_text = page.extract_text() or ""
            page_items = _extract_line_items(page_text)
            items.extend(page_items)

    # Deduplicate (same description + price)
    seen = set()
    unique_items = []
    for it in items:
        key = (it["description"][:50].lower(), it["unit_price"])
        if key not in seen:
            seen.add(key)
            it["line_number"] = len(unique_items) + 1
            unique_items.append(it)

    return {
        "ok": True,
        "supplier": supplier,
        "quote_number": quote_number,
        "quote_date": quote_date,
        "items": unique_items,
        "raw_text": full_text[:2000],
        "total_pages": len(reader.pages),
    }


def _detect_supplier(text: str) -> str:
    """Detect supplier name from quote text."""
    t = text.lower()
    suppliers = {
        "Echelon Distribution": ["echelon distribution", "echelon dist"],
        "Cardinal Health": ["cardinal health"],
        "McKesson": ["mckesson"],
        "Medline": ["medline"],
        "Henry Schein": ["henry schein"],
        "Concordance Healthcare": ["concordance"],
        "Bound Tree Medical": ["bound tree"],
        "Grainger": ["grainger"],
    }
    for name, patterns in suppliers.items():
        if any(p in t for p in patterns):
            return name
    return "Unknown"


def _extract_quote_number(text: str) -> str:
    """Find quote/reference number."""
    patterns = [
        r'(?:quote|quotation|ref|reference)\s*#?\s*:?\s*([A-Z0-9][\w\-]{3,20})',
        r'(?:proposal|estimate)\s*#?\s*:?\s*([A-Z0-9][\w\-]{3,20})',
        r'#\s*([A-Z]{1,4}[\-]?\d{4,10})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _extract_date(text: str) -> str:
    """Find quote date."""
    patterns = [
        r'(?:date|dated)\s*:?\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})',
        r'(\d{1,2}[/\-]\d{1,2}[/\-]20\d{2})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _parse_price(s: str) -> Optional[float]:
    """Extract a price from a string like '$12.34' or '12.34'."""
    if not s:
        return None
    s = s.replace(",", "").replace("$", "").strip()
    try:
        val = float(s)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def _extract_line_items(text: str) -> List[Dict]:
    """
    Extract line items from quote text. Uses multiple strategies.
    
    Strategy 1: Find lines with dollar amounts and parse surrounding context
    Strategy 2: Find tabular patterns (qty, desc, price columns)
    """
    items = []
    lines = text.split("\n")

    # ── Strategy 1: Dollar-amount lines ──
    # Look for lines containing prices and build items from context
    price_pattern = re.compile(r'\$?\s*(\d{1,6}\.\d{2})\b')
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line or len(line) < 5:
            continue

        prices = price_pattern.findall(line)
        if not prices:
            continue

        # Skip header/total lines
        line_lower = line.lower()
        if any(skip in line_lower for skip in [
            "total", "subtotal", "tax", "shipping", "freight", "discount",
            "balance", "amount due", "payment", "terms", "net ",
            "page ", "date:", "phone:", "fax:", "email:",
        ]):
            continue

        # Parse the line for qty, description, prices
        item = _parse_item_line(line, prices)
        if item and item.get("unit_price", 0) > 0:
            items.append(item)

    # ── Strategy 2: Structured table rows ──
    # If strategy 1 found nothing, try detecting table structure
    if not items:
        items = _parse_table_structure(lines)

    return items


def _parse_item_line(line: str, prices: list) -> Optional[Dict]:
    """
    Parse a single line that contains price(s) into an item dict.
    
    Common formats:
      "2 EA  Widget Blue 3x5  $12.50  $25.00"
      "1  Surgical Mask N95  45.00"
      "25-1156  Training BLS Provider  2  EA  $125.00  $250.00"
    """
    # Remove dollar signs for easier parsing
    clean = line.replace("$", "").strip()
    
    # Try to extract part number (leading alphanumeric code)
    part_match = re.match(r'^([A-Z0-9][\w\-]{2,15})\s+', clean, re.IGNORECASE)
    part_number = ""
    if part_match:
        candidate = part_match.group(1)
        # Verify it looks like a part# (has letters AND digits, or is a known format)
        if (any(c.isalpha() for c in candidate) and any(c.isdigit() for c in candidate)) or \
           re.match(r'^\d{2,5}-\d{2,5}$', candidate):
            part_number = candidate

    # Extract qty — look for a standalone number followed by UOM
    qty = 1
    uom = "EA"
    qty_match = re.search(r'\b(\d{1,5})\s*(EA|BX|CS|PK|DZ|BG|CT|RL|PR|SET|KT|CA)\b', clean, re.IGNORECASE)
    if qty_match:
        qty = int(qty_match.group(1))
        uom = qty_match.group(2).upper()
    else:
        # Try leading number
        lead_qty = re.match(r'^(\d{1,4})\s+', clean)
        if lead_qty and not part_number:
            qty = int(lead_qty.group(1))

    # Get prices — last price is likely extended, second-to-last is unit
    price_vals = [float(p) for p in prices if float(p) > 0]
    
    if len(price_vals) >= 2:
        unit_price = price_vals[-2]
        extended = price_vals[-1]
        # Sanity: if extended ≈ unit * qty, we got it right
        if qty > 1 and abs(extended - unit_price * qty) < 0.02:
            pass  # Confirmed
        elif qty == 1 and extended > unit_price:
            # Maybe qty is embedded and we missed it
            if unit_price > 0:
                implied_qty = round(extended / unit_price)
                if abs(extended - unit_price * implied_qty) < 0.02 and implied_qty > 0:
                    qty = implied_qty
    elif len(price_vals) == 1:
        unit_price = price_vals[0]
        extended = unit_price * qty
    else:
        return None

    # Extract description — strip prices, qty, part#, UOM
    desc = line
    # Remove dollar amounts
    desc = re.sub(r'\$?\s*\d{1,6}\.\d{2}', '', desc)
    # Remove part number from start
    if part_number:
        desc = re.sub(re.escape(part_number), '', desc, count=1)
    # Remove qty + UOM
    desc = re.sub(r'\b\d{1,5}\s*(EA|BX|CS|PK|DZ|BG|CT|RL|PR|SET|KT|CA)\b', '', desc, flags=re.IGNORECASE)
    # Remove leading standalone number (qty)
    desc = re.sub(r'^\d{1,4}\s+', '', desc.strip())
    # Clean up
    desc = re.sub(r'\s{2,}', ' ', desc).strip(" ,-–/|")

    if not desc or len(desc) < 3:
        return None

    return {
        "description": desc,
        "qty": qty,
        "uom": uom,
        "unit_price": round(unit_price, 2),
        "extended": round(extended, 2),
        "part_number": part_number,
    }


def _parse_table_structure(lines: list) -> List[Dict]:
    """Fallback: detect table header and parse rows."""
    items = []
    header_idx = None
    col_positions = {}

    # Find header row
    for i, line in enumerate(lines):
        ll = line.lower()
        # Look for rows with multiple column headers
        col_count = sum(1 for kw in ["qty", "description", "price", "amount", "ext", "uom", "unit"]
                       if kw in ll)
        if col_count >= 2:
            header_idx = i
            # Rough column detection by keyword position
            for kw in ["qty", "quantity"]:
                pos = ll.find(kw)
                if pos >= 0:
                    col_positions["qty"] = pos
            for kw in ["description", "item description", "product"]:
                pos = ll.find(kw)
                if pos >= 0:
                    col_positions["desc"] = pos
            for kw in ["unit price", "price", "unit"]:
                pos = ll.find(kw)
                if pos >= 0:
                    col_positions["price"] = pos
            break

    if header_idx is None:
        return items

    # Parse rows after header
    for line in lines[header_idx + 1:]:
        line = line.strip()
        if not line or len(line) < 5:
            continue
        # Stop at totals
        if any(t in line.lower() for t in ["total", "subtotal", "tax", "shipping"]):
            break

        prices = re.findall(r'\$?\s*(\d{1,6}\.\d{2})', line)
        if prices:
            item = _parse_item_line(line, prices)
            if item and item.get("unit_price", 0) > 0:
                items.append(item)

    return items


def match_quote_to_rfq(quote_items: List[Dict], rfq_items: List[Dict]) -> List[Dict]:
    """
    Match parsed supplier quote items to existing RFQ line items.
    
    Returns list of match results:
    [{"rfq_idx": int, "quote_idx": int, "confidence": float, "cost": float}, ...]
    """
    results = []
    used_rfq = set()

    for qi, q_item in enumerate(quote_items):
        q_desc = (q_item.get("description") or "").lower()
        q_pn = (q_item.get("part_number") or "").lower()
        q_words = set(re.findall(r'\w{3,}', q_desc))

        best_match = None
        best_score = 0

        for ri, r_item in enumerate(rfq_items):
            if ri in used_rfq:
                continue

            r_desc = (r_item.get("description") or "").lower()
            r_pn = (r_item.get("item_number") or r_item.get("part_number") or "").lower()
            r_words = set(re.findall(r'\w{3,}', r_desc))

            score = 0

            # Exact part number match — very strong
            if q_pn and r_pn and (q_pn == r_pn or q_pn in r_pn or r_pn in q_pn):
                score += 0.6

            # Word overlap
            if q_words and r_words:
                overlap = q_words & r_words
                union = q_words | r_words
                jaccard = len(overlap) / len(union) if union else 0
                score += jaccard * 0.4

            # Substring match bonus
            if len(q_desc) > 5 and len(r_desc) > 5:
                if q_desc in r_desc or r_desc in q_desc:
                    score += 0.2

            # Qty match bonus
            if q_item.get("qty") == r_item.get("qty") and q_item["qty"] > 0:
                score += 0.05

            if score > best_score and score >= 0.25:
                best_score = score
                best_match = ri

        results.append({
            "quote_idx": qi,
            "quote_desc": q_item.get("description", "")[:60],
            "quote_pn": q_item.get("part_number", ""),
            "unit_price": q_item.get("unit_price", 0),
            "qty": q_item.get("qty", 1),
            "rfq_idx": best_match,
            "confidence": round(best_score, 2),
            "matched": best_match is not None,
        })
        if best_match is not None:
            used_rfq.add(best_match)

    return results
