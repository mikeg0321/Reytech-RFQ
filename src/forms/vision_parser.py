"""
Vision-based PDF parser for AMS 704 Price Check forms.

Uses Claude's vision capabilities to extract structured data from
PDF pages rendered as images. Handles ALL forms regardless of:
- DocuSign flattened fields
- Scanned/photographed forms
- Non-standard layouts
- Garbled OCR text

Falls back gracefully when API key is not available.
"""

import os
import re
import json
import base64
import logging
import tempfile
from typing import Optional

log = logging.getLogger("reytech.vision_parser")

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from src.core.secrets import get_agent_key
    ANTHROPIC_API_KEY = get_agent_key("item_identifier")
except Exception:
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


# ═══════════════════════════════════════════════════════════════════════
# PDF → Image Conversion
# ═══════════════════════════════════════════════════════════════════════

def _pdf_pages_to_base64(pdf_path: str, dpi: int = 200, max_pages: int = 10) -> list:
    """Convert PDF pages to base64-encoded PNG images.
    Returns list of {"base64": str, "page": int}."""
    results = []
    try:
        from pdf2image import convert_from_path
        images = convert_from_path(pdf_path, dpi=dpi, first_page=1,
                                    last_page=max_pages, fmt="png")
        for i, img in enumerate(images):
            # Resize if too large (Claude has token limits for images)
            max_dim = 1568  # Claude's recommended max
            if img.width > max_dim or img.height > max_dim:
                ratio = min(max_dim / img.width, max_dim / img.height)
                new_size = (int(img.width * ratio), int(img.height * ratio))
                img = img.resize(new_size)

            # Convert to base64
            import io
            buf = io.BytesIO()
            img.save(buf, format="PNG", optimize=True)
            b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            results.append({"base64": b64, "page": i + 1})
    except Exception as e:
        log.error("PDF to image conversion failed: %s", e)
    return results


# ═══════════════════════════════════════════════════════════════════════
# Vision API Call
# ═══════════════════════════════════════════════════════════════════════

_VISION_SYSTEM = """You are a precise data extractor for California state government procurement forms (AMS 704 Price Check Worksheet).

Extract ALL data from the form image(s) and return ONLY valid JSON with no other text.

The JSON structure must be:
{
  "header": {
    "price_check_number": "",
    "requestor": "",
    "institution": "",
    "delivery_zip": "",
    "phone": "",
    "date_of_request": "",
    "due_date": ""
  },
  "items": [
    {
      "item_number": "1",
      "qty": 12,
      "uom": "each",
      "qty_per_uom": 1,
      "description": "Suave Deodorant 2.6 Oz. Sweet Pea & Violet Invis. Solid",
      "part_number": "784922807236"
    }
  ]
}

Rules:
- Extract EVERY line item visible in the table, even if partially obscured
- The part_number should be the UPC/barcode number (typically 12-13 digits at the end of the description)
- Strip the UPC from the description — put it in part_number only
- uom = unit of measure (each, pack, set, box, case, etc.)
- qty_per_uom = number of items per unit (e.g. "2 Pack" means qty_per_uom=2)
- For multi-page forms, combine all items into one list with correct sequential item numbers
- Return ONLY the JSON object, no markdown, no explanation"""


def _call_vision_api(page_images: list) -> Optional[dict]:
    """Send page images to Claude API and get structured extraction.
    Returns parsed JSON dict or None."""
    if not ANTHROPIC_API_KEY:
        log.debug("Vision parser: no API key")
        return None
    if not HAS_REQUESTS:
        log.debug("Vision parser: requests module not available")
        return None

    # Build content array with all page images
    content = []
    for pg in page_images:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": pg["base64"],
            }
        })
    content.append({
        "type": "text",
        "text": ("Extract ALL header fields and ALL line items from this AMS 704 "
                 "Price Check form. Return ONLY JSON, no markdown fences.")
    })

    try:
        resp = _requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "system": _VISION_SYSTEM,
                "messages": [{"role": "user", "content": content}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["content"][0]["text"].strip()

        # Clean up JSON response
        if text.startswith("```"):
            text = re.sub(r'^```\w*\n?', '', text)
            text = re.sub(r'\n?```$', '', text)

        result = json.loads(text)
        log.info("Vision API extracted: %d items, header=%s",
                 len(result.get("items", [])),
                 list(result.get("header", {}).keys()))
        return result

    except json.JSONDecodeError as e:
        log.error("Vision API returned non-JSON: %s — raw: %s", e, text[:200] if text else "empty")
        return None
    except Exception as e:
        log.error("Vision API call failed: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════
# Main Parser
# ═══════════════════════════════════════════════════════════════════════

def parse_with_vision(pdf_path: str) -> Optional[dict]:
    """
    Parse an AMS 704 PDF using Claude's vision capabilities.

    Returns dict in same format as parse_ams704:
    {
        "header": {...},
        "line_items": [...],
        "existing_prices": {},
        "ship_to": str,
        "source_pdf": str,
        "parse_method": "vision",
    }
    Or None if vision parsing is unavailable/fails.
    """
    if not ANTHROPIC_API_KEY:
        return None

    # Convert PDF to images
    page_images = _pdf_pages_to_base64(pdf_path, dpi=200)
    if not page_images:
        log.warning("Vision parser: no images generated from %s", pdf_path)
        return None

    # Filter out instruction/blank pages (last page of 704 is usually instructions)
    # Keep all pages that might have items (up to 10)
    log.info("Vision parser: %d page images from %s", len(page_images), os.path.basename(pdf_path))

    # Call API
    vision_result = _call_vision_api(page_images)
    if not vision_result:
        return None

    # Convert to standard parse_ams704 format
    header = vision_result.get("header", {})
    raw_items = vision_result.get("items", [])

    line_items = []
    for i, item in enumerate(raw_items):
        line_items.append({
            "item_number": str(item.get("item_number", i + 1)),
            "qty": int(item.get("qty", 1)),
            "uom": str(item.get("uom", "each")).lower(),
            "qty_per_uom": int(item.get("qty_per_uom", 1)),
            "description": str(item.get("description", "")),
            "part_number": str(item.get("part_number", "")),
            "row_index": i,
        })

    result = {
        "header": {
            "price_check_number": header.get("price_check_number", ""),
            "requestor": header.get("requestor", ""),
            "institution": header.get("institution", ""),
            "delivery_zip": header.get("delivery_zip", ""),
            "phone": header.get("phone", ""),
            "date_of_request": header.get("date_of_request", ""),
            "due_date": header.get("due_date", ""),
        },
        "line_items": line_items,
        "existing_prices": {},
        "ship_to": header.get("delivery_zip", ""),
        "source_pdf": pdf_path,
        "field_count": 0,
        "parse_method": "vision",
    }

    log.info("Vision parser complete: %d items from %s",
             len(line_items), os.path.basename(pdf_path))
    return result


def is_available() -> bool:
    """Check if vision parsing is available (API key + dependencies)."""
    return bool(ANTHROPIC_API_KEY) and HAS_REQUESTS
