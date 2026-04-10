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
import json
import logging
from typing import List, Dict, Optional

log = logging.getLogger("supplier_quote")

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False


def parse_supplier_quote(pdf_path: str) -> dict:
    """Parse a supplier quote PDF and extract line items with prices.
    
    Pipeline: regex parser → vision upgrade (if API available)
    Vision catches items regex misses and provides cleaner descriptions.
    """
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

    # ── Vision upgrade: get cleaner descriptions + catch missed items ──
    vision_items = _parse_supplier_quote_vision(pdf_path)
    if vision_items:
        items = _merge_regex_and_vision(items, vision_items)

    # Deduplicate
    seen = set()
    unique = []
    for it in items:
        key = (it.get("item_number", "").lower(), round(it["unit_price"], 2))
        if key not in seen:
            seen.add(key)
            it["line_number"] = len(unique) + 1
            unique.append(it)

    # Enrich with pricing intelligence
    try:
        from src.agents.quote_intelligence import enrich_extracted_items
        for _it in unique:
            if _it.get("unit_price") and not _it.get("cost"):
                _it["cost"] = _it["unit_price"]
        enriched = enrich_extracted_items(unique)
        for _i, _e in enumerate(enriched):
            if _i < len(unique) and _e.get("intelligence"):
                unique[_i]["intelligence"] = _e["intelligence"]
    except Exception:
        pass

    return {
        "ok": True,
        "supplier": supplier,
        "quote_number": quote_number,
        "quote_date": quote_date,
        "items": unique,
        "raw_text": full_text[:2000],
        "total_pages": len(reader.pages),
    }


def _parse_supplier_quote_vision(pdf_path: str) -> Optional[List[Dict]]:
    """Use Claude vision to extract supplier quote items.
    Returns list of items or None if unavailable."""
    try:
        from src.forms.vision_parser import is_available, _pdf_pages_to_base64, _call_vision_api
        if not is_available():
            return None
    except ImportError:
        return None

    import requests as _req
    from src.forms.vision_parser import ANTHROPIC_API_KEY
    import base64 as _b64

    # Try native PDF input first (smaller payload, better text extraction)
    # Falls back to PNG conversion if PDF is too large
    content = []
    _used_native_pdf = False
    try:
        _pdf_size = os.path.getsize(pdf_path)
        if _pdf_size < 5_000_000:  # Claude PDF limit ~5MB
            with open(pdf_path, "rb") as _f:
                _pdf_bytes = _f.read()
            content.append({
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": _b64.standard_b64encode(_pdf_bytes).decode("ascii"),
                },
            })
            _used_native_pdf = True
            log.info("Supplier quote: using native PDF input (%dKB)", _pdf_size // 1024)
    except Exception as _e:
        log.debug("Native PDF input failed, falling back to images: %s", _e)

    if not _used_native_pdf:
        page_images = _pdf_pages_to_base64(pdf_path, dpi=200, max_pages=5)
        if not page_images:
            return None
        for pg in page_images:
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/png", "data": pg["base64"]}
            })
    content.append({
        "type": "text",
        "text": ("Extract ALL line items from this supplier quote. "
                 "Return ONLY a JSON array (no other text), each item:\n"
                 '{"item_number":"449317","qty":40,"uom":"BX","uom_factor":"10EA/BX",'
                 '"description":"Collagen Dressing 3M Promogran Matrix 4 Square Inch Hexagon Sterile '
                 'DRESSING, PROMOGRAN MATRIX WND(10/BX) McKesson # 449317 Manufacturer # PG004",'
                 '"unit_price":87.41,"extended":3496.40}\n\n'
                 "Rules:\n"
                 "- Include the FULL description with ALL McKesson #, Manufacturer #, and product codes\n"
                 "- item_number = the Item# column value\n"
                 "- Keep pack size info in uom_factor (e.g. '10EA/BX', '50EA/CS')\n"
                 "- Return raw JSON array, no markdown fences")
    })

    try:
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "system": [{"type": "text", "text": "You are a precise data extractor. Return ONLY valid JSON arrays. No explanation.", "cache_control": {"type": "ephemeral"}}],
                "messages": [{"role": "user", "content": content}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["content"][0]["text"].strip()
        if text.startswith("```"):
            text = re.sub(r'^```\w*\n?', '', text)
            text = re.sub(r'\n?```$', '', text)

        vision_items = json.loads(text)
        if isinstance(vision_items, list):
            log.info("Vision supplier parser: %d items extracted", len(vision_items))
            return vision_items
    except Exception as e:
        log.debug("Vision supplier parse failed: %s", e)

    return None


def _merge_regex_and_vision(regex_items: List[Dict], vision_items: List[Dict]) -> List[Dict]:
    """Merge regex-parsed items with vision-parsed items.
    
    Strategy: regex is primary (has McKesson/Manufacturer refs for matching).
    Vision fills gaps: missing items, missing UOM factors, richer display name.
    NEVER replace regex descriptions — they contain MFG cross-references.
    """
    merged = []
    used_vision = set()

    for ri in regex_items:
        r_pn = (ri.get("item_number") or "").lower()
        r_price = ri.get("unit_price", 0)
        best_vision = None

        for vi_idx, vi in enumerate(vision_items):
            if vi_idx in used_vision:
                continue
            v_pn = (vi.get("item_number") or "").lower()
            v_price = vi.get("unit_price", 0)

            if (r_pn and v_pn and (r_pn == v_pn or r_pn in v_pn or v_pn in r_pn)):
                best_vision = (vi_idx, vi)
                break
            if r_price > 0 and v_price > 0 and abs(r_price - v_price) < 0.01:
                best_vision = (vi_idx, vi)
                break

        if best_vision:
            vi_idx, vi = best_vision
            used_vision.add(vi_idx)
            # Only fill MISSING fields from vision — never overwrite regex data
            if vi.get("uom_factor") and not ri.get("uom_factor"):
                ri["uom_factor"] = vi["uom_factor"]
            # Store vision description separately for display purposes
            v_desc = vi.get("description", "")
            if v_desc and len(v_desc) > 10:
                ri["vision_description"] = v_desc

        merged.append(ri)

    # Add vision-only items (regex missed them entirely)
    for vi_idx, vi in enumerate(vision_items):
        if vi_idx not in used_vision and vi.get("unit_price", 0) > 0:
            merged.append({
                "item_number": vi.get("item_number", ""),
                "qty": vi.get("qty", 1),
                "uom": vi.get("uom", "EA"),
                "uom_factor": vi.get("uom_factor", ""),
                "description": vi.get("description", ""),
                "unit_price": vi.get("unit_price", 0),
                "extended": vi.get("extended", 0),
                "part_number": vi.get("item_number", ""),
                "_source": "vision_only",
            })
            log.info("Vision added item regex missed: %s $%.2f",
                     vi.get("item_number", "?"), vi.get("unit_price", 0))

    return merged


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

        # Description: clean up remainder but KEEP manufacturer/McKesson references
        desc = remainder
        # Strip footer garbage that leaks into last item
        desc = re.sub(r'(?:SubTotal|Sub Total|Sales Tax|Shipping|Total)\s*\$[\d,.]+', '', desc)
        desc = re.sub(r'(?:CALVET|RFQ)\s*(?:DUE|RFQ)\s*[\d/]+', '', desc)
        desc = re.sub(r'\d+\s+of\s+\d+\s*$', '', desc)
        # Remove ALL-CAPS product code lines (e.g. "DRESSING, PROMOGRAN MATRIX WND")
        # but KEEP "McKesson # 449317" and "Manufacturer # PG004"
        desc = re.sub(r'(?<!\w)[A-Z]{3,}(?:,\s*[A-Z\d\s/"()\-\.]+){1,}(?!\s*#)', '', desc)
        # NOW insert spaces at word boundaries where text is glued
        desc = re.sub(r'([a-z])([A-Z][a-z])', r'\1 \2', desc)
        # Strip trailing UOM info (e.g. "2500/CS", "50/CS") but not from middle
        desc = re.sub(r'\s*\d+/[A-Z]{2,3}\s*$', '', desc, flags=re.IGNORECASE)
        # Collapse whitespace
        desc = re.sub(r'\s+', ' ', desc).strip(" ,;-")

        # If description is now too short, use original remainder cleaned minimally
        if len(desc) < 5 and remainder:
            desc = re.sub(r'([a-z])([A-Z][a-z])', r'\1 \2', remainder)
            desc = re.sub(r'(?:SubTotal|Sales Tax|Shipping|Total)\s*\$[\d,.]+', '', desc)
            desc = re.sub(r'\s+', ' ', desc).strip()[:200]

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
    """Match parsed supplier quote items to existing RFQ line items.
    
    Multi-signal matching:
    1. Exact part number match (strongest)
    2. Manufacturer/MFG number extraction from descriptions
    3. Numeric suffix matching (strip vendor prefixes: SQU022767 → 022767)
    4. Description keyword overlap (Jaccard similarity)
    5. Qty + UOM confirmation bonus
    """
    results = []
    used_rfq = set()

    for qi, q_item in enumerate(quote_items):
        q_desc = (q_item.get("description") or "").lower()
        q_pn = (q_item.get("item_number") or "").lower().strip()
        q_qty = q_item.get("qty", 0)
        q_uom = (q_item.get("uom") or "").lower().strip()
        q_words = set(re.findall(r'\w{3,}', q_desc))

        # Extract ALL part numbers from the supplier quote description
        # Echelon format: "McKesson # 449317", "Manufacturer # PG004"
        q_all_pns = _extract_all_part_numbers(q_desc, q_pn)

        best_match = None
        best_score = 0

        for ri, r_item in enumerate(rfq_items):
            if ri in used_rfq:
                continue
            r_desc = (r_item.get("description") or "").lower()
            r_pn = (r_item.get("item_number") or r_item.get("part_number") or "").lower().strip()
            r_qty = r_item.get("qty", 0)
            r_uom = (r_item.get("uom") or "").lower().strip()
            r_words = set(re.findall(r'\w{3,}', r_desc))

            # Extract part numbers from RFQ description too
            # RFQ format: "... # EQX7044", "... # J-JPG004Z"
            r_all_pns = _extract_all_part_numbers(r_desc, r_pn)

            score = 0

            # ── Signal 1: Exact part number match (0.8) ──
            if q_pn and r_pn and (q_pn == r_pn or q_pn in r_pn or r_pn in q_pn):
                score += 0.8

            # ── Signal 2: Cross-reference ALL extracted part numbers (0.7) ──
            if not score and q_all_pns and r_all_pns:
                for qp in q_all_pns:
                    for rp in r_all_pns:
                        if qp == rp:
                            score += 0.75
                            break
                        # Substring: "022767" in "squ022767" or "pg004" in "jpg004z"
                        if len(qp) >= 4 and len(rp) >= 4:
                            if qp in rp or rp in qp:
                                score += 0.7
                                break
                    if score > 0:
                        break

            # ── Signal 3: Numeric suffix match (0.6) ──
            # Strip all letter prefixes and compare: "SQU022767" → "022767"
            if not score:
                q_nums = _extract_numeric_suffixes(q_all_pns)
                r_nums = _extract_numeric_suffixes(r_all_pns)
                for qn in q_nums:
                    for rn in r_nums:
                        if qn == rn and len(qn) >= 4:
                            score += 0.65
                            break
                        if len(qn) >= 4 and len(rn) >= 4 and (qn in rn or rn in qn):
                            score += 0.55
                            break
                    if score > 0:
                        break

            # ── Signal 4: Description word overlap (0.3 max) ──
            if q_words and r_words:
                overlap = q_words & r_words
                union = q_words | r_words
                jaccard = len(overlap) / len(union) if union else 0
                score += jaccard * 0.3

            # ── Signal 5: Description prefix match (0.15) ──
            if len(q_desc) > 5 and len(r_desc) > 5:
                if q_desc[:20] in r_desc or r_desc[:20] in q_desc:
                    score += 0.15

            # ── Signal 6: Qty + UOM confirmation bonus (0.1) ──
            if q_qty and r_qty and q_qty == r_qty:
                score += 0.05
            if q_uom and r_uom and (q_uom == r_uom or q_uom[:2] == r_uom[:2]):
                score += 0.05

            if score > best_score and score >= 0.2:
                best_score = score
                best_match = ri

        results.append({
            "quote_idx": qi,
            "quote_desc": q_item.get("description", ""),
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


def _extract_all_part_numbers(description: str, primary_pn: str = "") -> List[str]:
    """Extract all part/manufacturer/McKesson numbers from a description.
    
    Handles:
    - "McKesson # 449317" → "449317"
    - "Manufacturer # PG004" → "pg004"
    - "# EQX7044" → "eqx7044"
    - "# J-JPG004Z" → "j-jpg004z", "jpg004z"
    - "#MDS098001Z" → "mds098001z"
    """
    pns = set()
    desc = description.lower()

    # Add primary part number
    if primary_pn:
        pn = primary_pn.lower().strip()
        pns.add(pn)
        # Also add without common prefixes
        stripped = re.sub(r'^[a-z]{1,3}[-]?', '', pn)
        if stripped and len(stripped) >= 3 and stripped != pn:
            pns.add(stripped)

    # Extract "# XXXXX" patterns
    for m in re.finditer(r'#\s*([A-Za-z0-9\-]{3,20})', desc):
        val = m.group(1).lower().strip('-')
        pns.add(val)
        # Strip letter prefixes: "j-jpg004z" → "jpg004z", "squ022767" → "022767"
        stripped = re.sub(r'^[a-z]{1,3}[-]?', '', val)
        if stripped and len(stripped) >= 3 and stripped != val:
            pns.add(stripped)

    # Extract "McKesson # XXX" and "Manufacturer # XXX" explicitly
    for m in re.finditer(r'(?:mckesson|manufacturer|mfg|mfr)\s*#?\s*:?\s*([A-Za-z0-9\-]{3,20})', desc):
        val = m.group(1).lower().strip('-')
        pns.add(val)

    # Extract standalone part-number-like strings at end: "- 816559012292"
    for m in re.finditer(r'[-–]\s*([A-Za-z0-9]{5,20})\s*$', desc):
        pns.add(m.group(1).lower())

    return list(pns)


def _extract_numeric_suffixes(part_numbers: List[str]) -> List[str]:
    """Extract pure numeric suffixes from part numbers.
    "squ022767" → "022767", "j-jpg004z" → "004", "eqx7044" → "7044"
    """
    nums = set()
    for pn in part_numbers:
        # Find longest numeric run
        for m in re.finditer(r'\d{3,}', pn):
            nums.add(m.group(0))
    return list(nums)
