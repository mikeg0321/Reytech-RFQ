"""
unspsc_classifier.py — UNSPSC code classification + Country of Origin detection.

Uses Claude Haiku to classify product descriptions into UNSPSC codes and
identify likely country of origin. Includes TAA compliance checking.

Phase 1 (V1): Claude-based classification with cached UNSPSC segments.
Phase 2 (V2): FAISS vector search for offline classification.
"""
import json
import logging
import os
import time

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

log = logging.getLogger("reytech.unspsc_classifier")

_MODEL = "claude-haiku-4-5-20251001"
_API_TIMEOUT = 10  # seconds — fail fast, never block pipeline
_MAX_BATCH = 10    # items per API call

# ═══════════════════════════════════════════════════════════════════════════
# TAA COMPLIANCE
# ═══════════════════════════════════════════════════════════════════════════

TAA_NON_COMPLIANT_COUNTRIES = {
    "china", "india", "russia", "iran", "north korea", "syria",
    "sudan", "cuba", "venezuela", "pakistan", "belarus", "myanmar",
    "afghanistan", "iraq", "libya", "somalia", "yemen",
}


def check_taa_compliance(country: str) -> int:
    """Return 1 (compliant), 0 (non-compliant), -1 (unknown)."""
    if not country or not country.strip():
        return -1
    normalized = country.strip().lower()
    if normalized in TAA_NON_COMPLIANT_COUNTRIES:
        return 0
    return 1


# ═══════════════════════════════════════════════════════════════════════════
# UNSPSC SEGMENTS (top-level + key families for prompt context)
# ═══════════════════════════════════════════════════════════════════════════

_UNSPSC_SEGMENTS = """
UNSPSC Code Segments (2-digit) and Key Families (4-digit):

10 - Live Plant and Animal Material
11 - Mineral and Textile and Inedible Plant and Animal Materials
12 - Chemicals including Bio Chemicals and Gas Materials
13 - Resin and Rosin and Rubber and Foam and Film and Elastomeric Materials
14 - Paper Materials and Products
15 - Fuels and Fuel Additives and Lubricants
20 - Mining and Well Drilling Machinery and Accessories
21 - Farming and Fishing and Forestry and Wildlife Machinery
22 - Building and Construction Machinery
23 - Industrial Manufacturing and Processing Machinery
24 - Material Handling and Conditioning and Storage Machinery
25 - Commercial and Military and Private Vehicles and their Accessories
26 - Power Generation and Distribution Machinery
27 - Tools and General Machinery
30 - Structures and Building and Construction Components
31 - Manufacturing Components and Supplies
32 - Electronic Components and Supplies
39 - Lighting Fixtures and Accessories
40 - Distribution and Conditioning Systems and Equipment
41 - Laboratory and Measuring and Observing and Testing Equipment
42 - Medical Equipment and Accessories and Supplies
  4211 - Medical instruments; 4213 - Medical supplies; 4214 - Patient care;
  4219 - Orthopedic; 4221 - Mortuary; 4222 - Patient transport;
  4229 - Respiratory therapy; 4223 - Medical furniture
43 - Information Technology Broadcasting and Telecommunications
  4320 - IT components; 4321 - Computers; 4322 - Storage; 4323 - Software
44 - Office Equipment and Accessories and Supplies
  4411 - Office machines; 4412 - Office supplies
45 - Printing and Photographic and Audio and Visual Equipment
46 - Defense and Law Enforcement and Security Equipment
  4618 - Personal safety/PPE; 4617 - Security systems
47 - Cleaning Equipment and Supplies
  4713 - Cleaning supplies; 4712 - Janitorial carts
48 - Service Industry Machinery and Equipment and Supplies
49 - Sports and Recreational Equipment and Supplies
50 - Food Beverage and Tobacco Products
  5011 - Meat/poultry; 5013 - Dairy; 5016 - Produce; 5020 - Beverages
51 - Drugs and Pharmaceutical Products
  5110 - Anti-infective; 5112 - Cardiovascular; 5114 - Respiratory
52 - Domestic Appliances and Supplies
53 - Apparel and Luggage and Personal Care Products
  5310 - Clothing; 5313 - Footwear
54 - Timepieces and Jewelry and Gemstone Products
55 - Published Products
56 - Furniture and Furnishings
  5610 - Accommodation furniture; 5612 - Institutional furniture
60 - Musical Instruments and Games and Toys and Arts and Crafts
70 - Farming and Fishing and Forestry and Wildlife Contracting Services
71 - Mining and Oil and Gas Services
72 - Building and Facility Construction and Maintenance Services
73 - Industrial Production and Manufacturing Services
76 - Industrial Cleaning Services
77 - Environmental Services
78 - Transportation and Storage and Mail Services
80 - Management and Business Professionals and Administrative Services
81 - Engineering and Research and Technology Based Services
82 - Editorial and Design and Graphic and Fine Art Services
83 - Public Utilities and Public Sector Related Services
84 - Financial and Insurance Services
85 - Healthcare Services
86 - Education and Training Services
90 - Travel and Food and Lodging and Entertainment Services
91 - Personal and Domestic Services
92 - National Defense and Public Order and Security and Safety Services
93 - Politics and Civic Affairs Services
94 - Organizations and Clubs
95 - Land and Buildings and Structures and Thoroughfares
"""

# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFICATION PROMPT
# ═══════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = f"""You are a product classification expert. Given product descriptions, return:
1. The most specific UNSPSC code (preferably 8 digits, minimum 4 digits)
2. The UNSPSC description for that code
3. The most likely country of origin/manufacture

{_UNSPSC_SEGMENTS}

Return ONLY valid JSON array. Each element:
{{"unspsc_code": "42131500", "unspsc_description": "Examination gloves", "country_of_origin": "Malaysia"}}

Rules:
- Use the most specific UNSPSC code you can determine (8-digit preferred, 4-digit minimum)
- For medical supplies (gloves, gowns, masks, etc.), use segment 42
- For janitorial/cleaning, use segment 47
- For office supplies, use segment 44
- For PPE/safety equipment, use segment 46
- country_of_origin: use the most common manufacturing country for that product type
- If manufacturer is known, use their primary manufacturing country
- If truly unknown, return empty string for country_of_origin
- Never guess wildly — return empty string if unsure"""


# ═══════════════════════════════════════════════════════════════════════════
# API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _get_api_key() -> str:
    for var in ("AGENT_PRICING_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var, "")
        if key:
            return key
    return ""


def _call_claude(user_msg: str) -> str:
    """Call Claude Haiku, return raw text response. Empty string on failure."""
    api_key = _get_api_key()
    if not api_key or not HAS_REQUESTS:
        log.warning("UNSPSC classifier: no API key or requests library")
        return ""

    try:
        request_body = {
            "model": _MODEL,
            "max_tokens": 2048,
            "system": [{"type": "text", "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": user_msg[:8000]}],
        }
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers, json=request_body, timeout=_API_TIMEOUT,
        )

        if resp.status_code == 429:
            log.debug("UNSPSC classifier: 429 rate limited")
            return ""

        if resp.status_code != 200:
            log.debug("UNSPSC classifier: API error %d", resp.status_code)
            return ""

        data = resp.json()
        full_text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                full_text += block.get("text", "")
        return full_text.strip()

    except requests.exceptions.Timeout:
        log.debug("UNSPSC classifier: timeout after %ds", _API_TIMEOUT)
        return ""
    except Exception as e:
        log.error("UNSPSC classifier API error: %s", e, exc_info=True)
        return ""


def _parse_response(text: str, expected_count: int) -> list:
    """Parse Claude JSON response into list of classification dicts."""
    if not text:
        return [_empty_result()] * expected_count

    try:
        # Find JSON array in response
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            results = json.loads(text[start:end])
            if isinstance(results, list):
                # Pad or trim to expected count
                while len(results) < expected_count:
                    results.append(_empty_result())
                return results[:expected_count]
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("UNSPSC classifier: JSON parse error: %s", e)

    return [_empty_result()] * expected_count


def _empty_result() -> dict:
    return {
        "unspsc_code": "",
        "unspsc_description": "",
        "country_of_origin": "",
        "taa_compliant": -1,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════

def classify_item(description: str, manufacturer: str = "") -> dict:
    """Classify a single product description.

    Returns: {"unspsc_code", "unspsc_description", "country_of_origin", "taa_compliant"}
    """
    if not description or not description.strip():
        return _empty_result()

    prompt = f"Classify this product:\nDescription: {description}"
    if manufacturer:
        prompt += f"\nManufacturer: {manufacturer}"

    text = _call_claude(prompt)
    results = _parse_response(text, 1)
    result = results[0]

    # Apply TAA check
    result["taa_compliant"] = check_taa_compliance(result.get("country_of_origin", ""))

    log.info("Classified item: %s → UNSPSC=%s, Origin=%s, TAA=%d",
             description[:60], result.get("unspsc_code", ""),
             result.get("country_of_origin", ""), result.get("taa_compliant", -1))
    return result


def classify_batch(items: list, batch_size: int = _MAX_BATCH) -> list:
    """Classify a list of items in batches.

    Args:
        items: list of dicts with at least 'description' key
        batch_size: max items per API call (default 10)

    Returns: list of classification dicts (same length as items)
    """
    if not items:
        return []

    all_results = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        prompt_lines = []
        for j, item in enumerate(batch, 1):
            desc = item.get("description", "") or item.get("name", "") or ""
            mfg = item.get("manufacturer", "") or ""
            line = f"{j}. {desc}"
            if mfg:
                line += f" (Manufacturer: {mfg})"
            prompt_lines.append(line)

        prompt = "Classify these products:\n" + "\n".join(prompt_lines)

        text = _call_claude(prompt)
        batch_results = _parse_response(text, len(batch))

        # Apply TAA check to each result
        for result in batch_results:
            result["taa_compliant"] = check_taa_compliance(
                result.get("country_of_origin", "")
            )

        all_results.extend(batch_results)

        log.info("UNSPSC batch %d-%d: classified %d items",
                 i + 1, i + len(batch), len(batch_results))

    return all_results


def update_catalog_item(product_id: int, unspsc_code: str, unspsc_desc: str,
                        country: str, taa: int = -1):
    """Write classification back to the products table."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                """UPDATE products SET unspsc_code=?, unspsc_description=?,
                   country_of_origin=?, taa_compliant=?, updated_at=datetime('now')
                   WHERE id=?""",
                (unspsc_code, unspsc_desc, country, taa, product_id)
            )
        log.info("Updated catalog item %d: UNSPSC=%s, COO=%s", product_id, unspsc_code, country)
    except Exception as e:
        log.error("Failed to update catalog item %d: %s", product_id, e)


def batch_retag_catalog(limit: int = 50) -> dict:
    """Batch retro-tag catalog items missing UNSPSC codes."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, name, description, manufacturer FROM products WHERE (unspsc_code IS NULL OR unspsc_code = '') LIMIT ?",
                (limit,)
            ).fetchall()
        if not rows:
            return {"tagged": 0, "remaining": 0, "errors": 0}
        items = [{"description": r["name"] or r["description"] or "", "manufacturer": r["manufacturer"] or "", "_id": r["id"]} for r in rows]
        results = classify_batch(items)
        tagged, errors = 0, 0
        for item, result in zip(items, results):
            if result.get("unspsc_code"):
                try:
                    update_catalog_item(item["_id"], result["unspsc_code"], result.get("unspsc_description", ""), result.get("country_of_origin", ""), result.get("taa_compliant", -1))
                    tagged += 1
                except Exception:
                    errors += 1
            else:
                errors += 1
        remaining = 0
        try:
            with get_db() as conn:
                remaining = conn.execute("SELECT COUNT(*) FROM products WHERE unspsc_code IS NULL OR unspsc_code = ''").fetchone()[0]
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        log.info("UNSPSC batch retag: tagged=%d, errors=%d, remaining=%d", tagged, errors, remaining)
        return {"tagged": tagged, "remaining": remaining, "errors": errors}
    except Exception as e:
        log.error("Batch retag error: %s", e, exc_info=True)
        return {"tagged": 0, "remaining": -1, "errors": 1}
