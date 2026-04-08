"""
Product Validator — LLM-powered product identification and match verification.

Uses xAI Grok API (with built-in web search) to:
1. Validate whether a matched product is correct
2. Find the correct product when matching fails
3. Return actionable data: product name, URL, price, ASIN

Called by the enrichment pipeline for items with confidence < 0.75
after all identifier-based and fuzzy matching steps have run.
"""

import os
import json
import logging
import time

log = logging.getLogger("product_validator")

XAI_API_URL = "https://api.x.ai/v1/chat/completions"
XAI_MODEL = "grok-3-mini"  # fast, cheap, good at product lookup
_MAX_RETRIES = 2
_TIMEOUT = 20  # seconds per API call

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def _get_api_key() -> str:
    """Get xAI API key from environment."""
    return os.environ.get("XAI_API_KEY", "")


def validate_product(
    description: str,
    upc: str = "",
    mfg_number: str = "",
    qty: int = 1,
    uom: str = "EA",
    qty_per_uom: int = 1,
    best_match_title: str = "",
    best_match_price: float = 0,
    best_match_confidence: float = 0,
    best_match_source: str = "",
) -> dict:
    """
    Validate or find the correct product using Grok with web search.

    Returns:
        {
            "ok": True/False,
            "is_correct_match": True/False,
            "product_name": str,
            "url": str (prefer Amazon),
            "price": float,
            "asin": str,
            "supplier": str,
            "confidence": float (0-1),
            "reasoning": str,
            "tokens_used": int,
        }
    """
    if not HAS_REQUESTS:
        return {"ok": False, "error": "requests not available"}

    api_key = _get_api_key()
    if not api_key:
        return {"ok": False, "error": "XAI_API_KEY not set"}

    # Build context for Grok
    item_info = f"Description: {description}"
    if upc:
        item_info += f"\nUPC/Barcode: {upc}"
    if mfg_number:
        item_info += f"\nMFG#/Part#: {mfg_number}"
    item_info += f"\nQty: {qty} {uom}"
    if qty_per_uom > 1:
        item_info += f" ({qty_per_uom} per {uom})"

    match_info = ""
    if best_match_title:
        match_info = (
            f"\n\nCurrent best match (confidence {best_match_confidence:.0%}):\n"
            f"  Title: {best_match_title}\n"
            f"  Price: ${best_match_price:.2f}\n"
            f"  Source: {best_match_source}"
        )

    prompt = f"""You are a product identification specialist for a California government procurement company.

A buyer submitted a Price Check (AMS 704 form) requesting a quote for this item:

{item_info}{match_info}

Your task:
1. Search the web to find this EXACT product
2. Verify if the current match (if any) is the correct product
3. Find the best purchase URL (PREFER Amazon.com for ease of ordering)
4. Return the current retail/list price (NOT sale/coupon price)

IMPORTANT:
- The UPC/barcode is the most reliable identifier — search by it first
- "S&S Worldwide" items are often available on Amazon under different listings
- Return the PACK price matching the buyer's UOM, not per-unit price
- If the item is a pack of {qty_per_uom}, return the price for 1 pack of {qty_per_uom}

Respond in this exact JSON format (no markdown, no code blocks):
{{"is_correct_match": true/false, "product_name": "exact product name", "url": "https://www.amazon.com/dp/ASIN or best URL", "price": 0.00, "asin": "B0XXXXXXXX or empty", "supplier": "Amazon or supplier name", "confidence": 0.0-1.0, "reasoning": "brief explanation"}}"""

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": XAI_MODEL,
        "messages": [
            {"role": "system", "content": "You are a product research assistant. Always respond with valid JSON only. No markdown formatting."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,  # low temperature for factual accuracy
        "max_tokens": 500,
    }

    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.post(
                XAI_API_URL,
                headers=headers,
                json=payload,
                timeout=_TIMEOUT,
            )
            if resp.status_code == 429:
                # Rate limited — wait and retry
                time.sleep(2 * (attempt + 1))
                continue
            if resp.status_code != 200:
                log.warning("Grok API %d: %s", resp.status_code, resp.text[:200])
                return {"ok": False, "error": f"API {resp.status_code}"}

            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            tokens = data.get("usage", {}).get("total_tokens", 0)

            # Parse JSON from response — handle markdown code blocks
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[-1]
                if content.endswith("```"):
                    content = content[:-3]
                content = content.strip()

            try:
                result = json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from mixed content
                import re
                json_match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    log.warning("Grok response not JSON: %s", content[:200])
                    return {"ok": False, "error": "Response not JSON", "raw": content[:200]}

            # Normalize and validate
            result["ok"] = True
            result["tokens_used"] = tokens
            result["price"] = float(result.get("price") or 0)
            result["confidence"] = float(result.get("confidence") or 0)
            result["is_correct_match"] = bool(result.get("is_correct_match"))
            result.setdefault("product_name", "")
            result.setdefault("url", "")
            result.setdefault("asin", "")
            result.setdefault("supplier", "")
            result.setdefault("reasoning", "")

            # Extract ASIN from URL if not provided
            if not result["asin"] and "amazon.com" in result.get("url", ""):
                import re
                asin_m = re.search(r'/dp/([A-Z0-9]{10})', result["url"])
                if asin_m:
                    result["asin"] = asin_m.group(1)

            log.info("Grok validated: '%s' → %s (conf=%.0f%%, $%.2f, %d tokens)",
                     description[:40],
                     "CORRECT" if result["is_correct_match"] else result["product_name"][:40],
                     result["confidence"] * 100,
                     result["price"],
                     tokens)

            return result

        except requests.exceptions.Timeout:
            log.warning("Grok API timeout (attempt %d/%d)", attempt + 1, _MAX_RETRIES)
            continue
        except Exception as e:
            log.error("Grok API error: %s", e)
            return {"ok": False, "error": str(e)}

    return {"ok": False, "error": "Max retries exceeded"}


def validate_batch(items: list, max_calls: int = 5) -> list:
    """
    Validate multiple items. Only processes items that need validation
    (low confidence or no match). Returns list of results aligned by index.

    items: [{description, upc, mfg_number, qty, uom, qty_per_uom,
             best_match_title, best_match_price, best_match_confidence,
             best_match_source, idx}]
    """
    results = []
    calls_made = 0

    for item in items:
        if calls_made >= max_calls:
            results.append({"ok": False, "error": "Rate limit reached", "idx": item.get("idx")})
            continue

        result = validate_product(
            description=item.get("description", ""),
            upc=item.get("upc", ""),
            mfg_number=item.get("mfg_number", ""),
            qty=item.get("qty", 1),
            uom=item.get("uom", "EA"),
            qty_per_uom=item.get("qty_per_uom", 1),
            best_match_title=item.get("best_match_title", ""),
            best_match_price=item.get("best_match_price", 0),
            best_match_confidence=item.get("best_match_confidence", 0),
            best_match_source=item.get("best_match_source", ""),
        )
        result["idx"] = item.get("idx")
        results.append(result)
        calls_made += 1

        # Rate limit between calls
        if calls_made < max_calls:
            time.sleep(0.5)

    log.info("Grok batch validation: %d items, %d calls made", len(items), calls_made)
    return results
