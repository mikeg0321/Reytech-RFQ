"""
Vision-based document parser for procurement forms and item lists.

Uses Claude's vision capabilities to extract structured data from
PDF pages, images, screenshots, and photographs. Handles:
- AMS 704 Price Check forms
- Any procurement list (typed, handwritten, screenshot)
- DocuSign flattened fields
- Scanned/photographed documents
- Non-standard layouts

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

def _get_api_key() -> str:
    """Get Anthropic API key, trying secrets manager first then env var."""
    try:
        from src.core.secrets import get_agent_key
        return get_agent_key("item_identifier") or ""
    except Exception:
        return os.environ.get("ANTHROPIC_API_KEY", "")

ANTHROPIC_API_KEY = _get_api_key()


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


def _image_file_to_base64(image_path: str) -> Optional[dict]:
    """Load an image file (PNG, JPG, etc.) as base64 for the vision API.
    Returns {"base64": str, "media_type": str} or None."""
    ext = os.path.splitext(image_path)[1].lower()
    media_map = {
        ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/png",
        ".tiff": "image/png", ".tif": "image/png",
    }
    media_type = media_map.get(ext)
    if not media_type:
        log.warning("Unsupported image format: %s", ext)
        return None
    try:
        # For BMP/TIFF, convert to PNG via PIL
        if ext in (".bmp", ".tiff", ".tif"):
            from PIL import Image
            import io
            img = Image.open(image_path)
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            return {"base64": b64, "media_type": "image/png"}
        else:
            with open(image_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")
            return {"base64": b64, "media_type": media_type}
    except Exception as e:
        log.error("Failed to load image %s: %s", image_path, e)
        return None


# ═══════════════════════════════════════════════════════════════════════
# Vision API Call
# ═══════════════════════════════════════════════════════════════════════

_VISION_SYSTEM = """You are a precise data extractor for procurement documents — item lists, price check forms, quote requests, screenshots, and handwritten lists.

FIRST: Count how many line items you see in the document. State the count to yourself before extracting.

Extract ALL data and return ONLY valid JSON with no other text.

JSON structure:
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
      "description": "Item description here",
      "part_number": "manufacturer or UPC number",
      "item_link": "full URL if visible (https://...)"
    }
  ]
}

Rules:
- Extract EVERY line item — count them first, then extract that exact count
- Works for ANY document layout: tables, numbered lists, handwritten lists, screenshots
- part_number = UPC, manufacturer part number, SKU, or catalog number (if visible)
- item_link = any full URL visible alongside the item (http:// or https://). Extract the COMPLETE URL — do not truncate.
- Strip the part number from the description — put it in part_number only
- uom = unit of measure (each, pack, set, box, case, etc.)
- qty_per_uom = number of items per unit (e.g. "2 Pack" means qty_per_uom=2)
- If qty is not specified, default to 1
- For multi-page documents, combine all items into one list
- Return ONLY the JSON object, no markdown fences, no explanation"""


_VISION_SYSTEM_URLS = """You are a precise data extractor for screenshots containing product URLs and descriptions — browser tabs, spreadsheets, emails, web pages, or any image with supplier links.

FIRST: Count how many distinct products/items you see. State the count to yourself before extracting.

Your PRIMARY goal is to extract URLs. Look for URLs in:
- Browser address bars
- Hyperlink text (blue/underlined text)
- Visible URL strings in table cells, spreadsheet columns, or text
- Partial URLs — reconstruct the full URL if possible

Extract ALL data and return ONLY valid JSON with no other text.

JSON structure:
{
  "items": [
    {
      "description": "Product description or title",
      "item_link": "https://full-url-here.com/product/...",
      "qty": 1,
      "uom": "each",
      "part_number": "manufacturer or SKU number if visible"
    }
  ]
}

Rules:
- Extract EVERY item with a URL — count them first, then extract that exact count
- item_link = the COMPLETE URL. Do NOT truncate. Include full path and query parameters.
- If a URL is partially visible (cut off by screenshot edge), extract what you can see
- description = the product name/description associated with that URL
- part_number = any visible SKU, MFG#, part number, or catalog number
- qty defaults to 1 if not visible
- uom defaults to "each" if not visible
- If you see items WITHOUT URLs, still extract them (leave item_link empty)
- Return ONLY the JSON object, no markdown fences, no explanation"""


def _call_vision_api(page_images: list, system_prompt: str = None) -> Optional[dict]:
    """Send page images to Claude API and get structured extraction.
    Returns parsed JSON dict or None."""
    api_key = _get_api_key()  # re-read live — env var may be set after module load
    if not api_key:
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
                "media_type": pg.get("media_type", "image/png"),
                "data": pg["base64"],
            }
        })
    content.append({
        "type": "text",
        "text": ("Extract ALL header fields and ALL line items from this document. "
                 "Count items first, then extract that exact count. Return ONLY JSON, no markdown fences.")
    })

    try:
        resp = _requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "system": system_prompt or _VISION_SYSTEM,
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

def parse_with_vision(file_path: str, mode: str = "standard") -> Optional[dict]:
    """
    Parse a PDF or image using Claude's vision capabilities.

    Accepts: PDF files, PNG, JPG, JPEG, GIF, WEBP, BMP, TIFF.

    Args:
        file_path: Path to the file to parse.
        mode: "standard" (default) for procurement docs,
              "screenshot_urls" for screenshots with supplier URLs.

    Returns dict:
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
    if not _get_api_key():
        return None

    ext = os.path.splitext(file_path)[1].lower()
    is_image = ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif")

    if is_image:
        # Load image directly — don't try pdf2image
        img_data = _image_file_to_base64(file_path)
        if not img_data:
            log.warning("Vision parser: failed to load image %s", file_path)
            return None
        page_images = [img_data]
        log.info("Vision parser: loaded image %s (%s)", os.path.basename(file_path), img_data["media_type"])
    else:
        # PDF: convert to images
        page_images = _pdf_pages_to_base64(file_path, dpi=200)
        if not page_images:
            log.warning("Vision parser: no images generated from %s", file_path)
            return None
        log.info("Vision parser: %d page images from %s", len(page_images), os.path.basename(file_path))

    # Call API
    sys_prompt = _VISION_SYSTEM_URLS if mode == "screenshot_urls" else None
    vision_result = _call_vision_api(page_images, system_prompt=sys_prompt)
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
            "item_link": str(item.get("item_link", "")),
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
        "source_pdf": file_path,
        "field_count": 0,
        "parse_method": "vision",
    }

    log.info("Vision parser complete: %d items from %s",
             len(line_items), os.path.basename(file_path))
    return result


def is_available() -> bool:
    """Check if vision parsing is available (API key + dependencies)."""
    return bool(_get_api_key()) and HAS_REQUESTS
