"""
Supplier Quote PDF Parser
Primary: Echelon Distribution quotes
Also handles: Cardinal, McKesson, Medline, generic vendor quotes

Echelon format (pypdf text extraction):
  Text runs together — line items appear as:
  "{qty}  ${unit_price}  ${total}{line_num} {unit}{uom_factor} {item#} {description}..."
  
  We use regex to find the repeating pattern of qty+prices+line# blocks.
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
    """Parse a supplier quote PDF and extract line items with prices."""
    if not HAS_PYPDF:
        return {"ok": False, "error": "pypdf not available"}
    if not os.path.exists(pdf_path):
        return {"ok": False, "error": f"File not found: {pdf_path}"}

    try:
        reader = PdfReader(pdf_path)
    except Exception as e:
        return {"ok": False, "error": f"Cannot read PDF: {e}"}

    full_text = ""
    for page in reader.pages:
        full_text += (page.extract_text() or "") + "\n"

    if len(full_text.strip()) < 20:
        return {"ok": False, "error": "PDF has no extractable text (scanned image?)"}

    supplier = _detect_supplier(full_text)
    quote_number = _extract_quote_number(full_text)
    quote_date = _extract_date(full_text)

    # Try Echelon-specific parser first
    items = []
    if "echelon" in full_text.lower():
        items = _parse_echelon(full_text)
        log.info("Echelon parser: %d items found", len(items))

    # Fallback: generic
    if not items:
        items = _parse_generic(full_text)
        log.info("Generic parser: %d items found", len(items))

    # Deduplicate
    seen = set()
    unique = []
    for it in items:
        key = (it.get("item_number", "").lower(), round(it["unit_price"], 2))
        if key not in seen:
            seen.add(key)
            it["line_number"] = len(unique) + 1
            unique.append(it)

    return {
        "ok": True,
        "supplier": supplier,
        "quote_number": quote_number,
        "quote_date": quote_date,
        "items": unique,
        "raw_text": full_text[:2000],
        "total_pages": len(reader.pages),
    }


def _detect_supplier(text: str) -> str:
    t = text.lower()
    suppliers = {
        "Echelon Distribution": ["echelon distribution", "echelon dist", "echelondistribution"],
        "Cardinal Health": ["cardinal health"],
        "McKesson": ["mckesson medical"],
        "Medline": ["medline"],
        "Henry Schein": ["henry schein"],
        "Concordance Healthcare": ["concordance"],
        "Bound Tree Medical": ["bound tree"],
    }
    for name, patterns in suppliers.items():
        if any(p in t for p in patterns):
            return name
    return "Unknown"


def _extract_quote_number(text: str) -> str:
    m = re.search(r'(ECHQ\d{5,10})', text)
    if m:
        return m.group(1)
    for pat in [r'(?:quote|quotation|ref)\s*#?\s*:?\s*([A-Z0-9][\w\-]{3,20})']:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _extract_date(text: str) -> str:
    m = re.search(r'(?:Estimate\s*Date|Date)\s*(\w+\s+\d{1,2},?\s*\d{4})', text)
    if m:
        return m.group(1).strip()
    m = re.search(r'(\d{1,2}[/\-]\d{1,2}[/\-]20\d{2})', text)
    if m:
        return m.group(1).strip()
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
# Echelon-Specific Parser
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_echelon(text: str) -> List[Dict]:
    """
    Parse Echelon Distribution quote.
    
    pypdf extracts Echelon's table as run-together text. Each item block:
      "40.00  $87.41  $3,496.401 BX10EA/BX 449317 Collagen Dressing..."
      
    The line number is glued to the total: "$3,496.401" = total $3,496.40 + line 1
    """
    items = []

    # Find all qty+price blocks
    # Pattern: qty  $unit_price  $total{line_num} {unit}
    block_pattern = re.compile(
        r'(\d+\.?\d*)\s+'           # qty (e.g. "40.00")
        r'\$([\d,]+\.\d{2})\s+'     # unit price
        r'\$([\d,]+\.\d{2})'        # total (line# glued after)
        r'(\d{1,2})\s+'             # line number
        r'([A-Z]{2})'               # unit (BX, CS, EA)
    )

    matches = list(block_pattern.finditer(text))
    if not matches:
        log.warning("Echelon parser: no qty+price blocks found")
        return items

    for i, m in enumerate(matches):
        qty = int(float(m.group(1)))
        unit_price = float(m.group(2).replace(",", ""))
        total = float(m.group(3).replace(",", ""))
        line_num = int(m.group(4))
        unit = m.group(5)

        # Text after unit until next block or end
        start = m.end()
        if i + 1 < len(matches):
            end = matches[i + 1].start()
        else:
            end_markers = [em.start() + start for em in
                          re.finditer(r'(?:SubTotal|Sales Tax|Shipping|Total\s+\$|CALVET|RFQ DUE)', text[start:])]
            end = (start + min(end_markers)) if end_markers else min(start + 500, len(text))

        remainder = text[start:end].strip()

        # Parse UOM factor (e.g. "10EA/BX", "50EA/CS", "144/BX 24BX/CS")
        uom_match = re.match(r'([\d/]+\w+(?:/\w+)?(?:\s+\d+\w+/\w+)?)\s+', remainder)
        uom_factor = ""
        if uom_match:
            uom_factor = uom_match.group(1)
            remainder = remainder[uom_match.end():].strip()

        # Item number: first alphanumeric token
        # Handle glued text like "MDS098001ZHydrogen" — split at transition from
        # code chars (digits/uppercase) to description word (lowercase after uppercase)
        item_match = re.match(r'([A-Z0-9][\w\-]{2,15})', remainder, re.IGNORECASE)
        item_number = ""
        if item_match:
            candidate = item_match.group(1)
            if any(c.isdigit() for c in candidate):
                # Check for glued description: "MDS098001ZHydrogen"
                # Split where digit/uppercase transitions to lowercase word
                split_m = re.match(r'^([A-Z0-9][\dA-Z\-]*[0-9A-Z])([A-Z][a-z])', candidate)
                if split_m:
                    item_number = split_m.group(1)
                    # Put the description start back (everything after item# in candidate)
                    remainder = candidate[len(item_number):] + remainder[item_match.end():]
                else:
                    item_number = candidate
                    remainder = remainder[item_match.end():].strip()

        # Description: clean up remainder
        desc = remainder
        # Strip McKesson/Manufacturer references FIRST (before word boundary fixes)
        desc = re.sub(r'McKesson\s*#?\s*\d*\w*', '', desc)
        desc = re.sub(r'Manufacturer\s*#?\s*\w+', '', desc)
        desc = re.sub(r'#[A-Z0-9][\w\-]*', '', desc, flags=re.IGNORECASE)
        # Remove ALL-CAPS product code lines (e.g. "DRESSING, PROMOGRAN MATRIX WND")
        desc = re.sub(r'[A-Z]{3,}(?:,\s*[A-Z\d\s/"()\-\.]+){1,}', '', desc)
        # Remove UOM/pack info in parens or standalone
        desc = re.sub(r'\(\d+/[A-Z]{2,3}(?:\s+\d+[A-Z]{2,3}/[A-Z]{2,3})?\)', '', desc, flags=re.IGNORECASE)
        desc = re.sub(r'\d+/[A-Z]{2,3}\s*$', '', desc, flags=re.IGNORECASE)
        # NOW insert spaces at word boundaries where text is glued
        # But be careful: only split lowercase→uppercase transitions
        desc = re.sub(r'([a-z])([A-Z][a-z])', r'\1 \2', desc)
        # Strip leftover # chars and trailing manufacturer numbers
        desc = re.sub(r'#\s*\d*\s*$', '', desc)
        desc = re.sub(r'\s*#\s+', ' ', desc)
        desc = re.sub(r'#$', '', desc)
        # Strip trailing UOM info (e.g. "2500/CS", "50/CS")
        desc = re.sub(r'\s*\d+/[A-Z]{2,3}\s*$', '', desc, flags=re.IGNORECASE)
        # Collapse whitespace
        desc = re.sub(r'\s+', ' ', desc).strip(" ,;-#")

        # If description is now too short, use original remainder cleaned minimally
        if len(desc) < 5 and remainder:
            desc = re.sub(r'McKesson\s*#?\s*\w+', '', remainder)
            desc = re.sub(r'Manufacturer\s*#?\s*\w+', '', desc)
            desc = re.sub(r'([a-z])([A-Z][a-z])', r'\1 \2', desc)
            desc = re.sub(r'\s+', ' ', desc).strip()[:100]

        items.append({
            "line_number": line_num,
            "qty": qty,
            "uom": unit,
            "uom_factor": uom_factor,
            "item_number": item_number,
            "description": desc,
            "unit_price": round(unit_price, 2),
            "extended": round(total, 2),
            "part_number": item_number,
        })

    log.info("Echelon parser: extracted %d items", len(items))
    return items


# ═══════════════════════════════════════════════════════════════════════════════
# Generic Parser (fallback)
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_generic(text: str) -> List[Dict]:
    """Fallback parser for non-Echelon quotes."""
    items = []
    lines = text.split("\n")
    price_pattern = re.compile(r'\$?\s*(\d{1,6}\.\d{2})\b')

    for line in lines:
        line = line.strip()
        if not line or len(line) < 5:
            continue
        prices = price_pattern.findall(line)
        if not prices:
            continue
        if any(skip in line.lower() for skip in [
            "total", "subtotal", "tax", "shipping", "freight",
            "discount", "balance", "amount due", "payment",
        ]):
            continue
        item = _parse_generic_line(line, prices)
        if item and item.get("unit_price", 0) > 0:
            items.append(item)
    return items


def _parse_generic_line(line: str, prices: list) -> Optional[Dict]:
    clean = line.replace("$", "").strip()
    part_match = re.match(r'^([A-Z0-9][\w\-]{2,15})\s+', clean, re.IGNORECASE)
    part_number = ""
    if part_match:
        c = part_match.group(1)
        if any(ch.isalpha() for ch in c) and any(ch.isdigit() for ch in c):
            part_number = c

    qty = 1
    uom = "EA"
    qty_match = re.search(r'\b(\d{1,5})\s*(EA|BX|CS|PK|DZ|CT)\b', clean, re.IGNORECASE)
    if qty_match:
        qty = int(qty_match.group(1))
        uom = qty_match.group(2).upper()

    price_vals = [float(p) for p in prices if float(p) > 0]
    if len(price_vals) >= 2:
        unit_price, extended = price_vals[-2], price_vals[-1]
    elif len(price_vals) == 1:
        unit_price = price_vals[0]
        extended = unit_price * qty
    else:
        return None

    desc = re.sub(r'\$?\s*\d{1,6}\.\d{2}', '', line)
    if part_number:
        desc = desc.replace(part_number, '', 1)
    desc = re.sub(r'\b\d{1,5}\s*(EA|BX|CS|PK)\b', '', desc, flags=re.IGNORECASE)
    desc = re.sub(r'\s{2,}', ' ', desc).strip(" ,-/|")
    if not desc or len(desc) < 3:
        return None

    return {
        "description": desc, "qty": qty, "uom": uom,
        "unit_price": round(unit_price, 2), "extended": round(extended, 2),
        "part_number": part_number,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Matching Engine
# ═══════════════════════════════════════════════════════════════════════════════

def match_quote_to_rfq(quote_items: List[Dict], rfq_items: List[Dict]) -> List[Dict]:
    """Match parsed supplier quote items to existing RFQ line items."""
    results = []
    used_rfq = set()

    for qi, q_item in enumerate(quote_items):
        q_desc = (q_item.get("description") or "").lower()
        q_pn = (q_item.get("item_number") or "").lower().strip()
        q_words = set(re.findall(r'\w{3,}', q_desc))

        best_match = None
        best_score = 0

        for ri, r_item in enumerate(rfq_items):
            if ri in used_rfq:
                continue
            r_desc = (r_item.get("description") or "").lower()
            r_pn = (r_item.get("item_number") or r_item.get("part_number") or "").lower().strip()
            r_words = set(re.findall(r'\w{3,}', r_desc))

            score = 0
            if q_pn and r_pn and (q_pn == r_pn or q_pn in r_pn or r_pn in q_pn):
                score += 0.7
            if q_words and r_words:
                overlap = q_words & r_words
                union = q_words | r_words
                score += (len(overlap) / len(union) if union else 0) * 0.3
            if len(q_desc) > 5 and len(r_desc) > 5:
                if q_desc[:20] in r_desc or r_desc[:20] in q_desc:
                    score += 0.15

            if score > best_score and score >= 0.2:
                best_score = score
                best_match = ri

        results.append({
            "quote_idx": qi,
            "quote_desc": q_item.get("description", "")[:80],
            "quote_pn": q_item.get("item_number", ""),
            "unit_price": q_item.get("unit_price", 0),
            "qty": q_item.get("qty", 1),
            "uom": q_item.get("uom", "EA"),
            "rfq_idx": best_match,
            "confidence": round(best_score, 2),
            "matched": best_match is not None,
        })
        if best_match is not None:
            used_rfq.add(best_match)

    return results
