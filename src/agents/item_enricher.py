"""
Item Enricher
Parses product identifiers from FI$Cal descriptions.
Structures catalog entries with MFG#, UPC, ASIN, SKU, URLs.
"""
import re
import logging
import json
import urllib.parse

log = logging.getLogger("reytech.item_enricher")


def parse_identifiers(description):
    """Extract all product identifiers from a FI$Cal description."""
    desc = description or ""
    desc_upper = desc.upper()

    result = {"mfg_numbers": [], "upc_codes": [], "asins": [], "nsns": [],
              "skus": [], "item_numbers": [], "ref_codes": [], "raw_identifiers": []}

    # ASIN (Amazon): B followed by 9 alphanumeric
    for m in re.finditer(r'\b(B0[0-9A-Z]{8})\b', desc_upper):
        result["asins"].append(m.group(1))
    for m in re.finditer(r'ASIN[:\s]+([A-Z0-9]{10})', desc_upper):
        if m.group(1) not in result["asins"]:
            result["asins"].append(m.group(1))

    # UPC: 12-13 digits after #
    for m in re.finditer(r'#\s*(\d{12,13})\b', desc):
        result["upc_codes"].append(m.group(1))
    for m in re.finditer(r'\b(\d{12,13})\b', desc):
        if m.group(1).startswith("0") and m.group(1) not in result["upc_codes"]:
            result["upc_codes"].append(m.group(1))

    # NSN: XXXX-XX-XXX-XXXX
    for m in re.finditer(r'\b(\d{4}-\d{2}-\d{3,4}-\d{4})\b', desc):
        result["nsns"].append(m.group(1))

    # MFR# / MFG# / MPN#
    for m in re.finditer(r'(?:MFR|MFG|MPN|MDL|MODEL)\s*#?\s*:?\s*([A-Z0-9][\w\-\.]{2,20})', desc_upper):
        val = m.group(1).strip().rstrip(".,;")
        if len(val) >= 3 and val not in result["mfg_numbers"]:
            result["mfg_numbers"].append(val)

    # Item #
    for m in re.finditer(r'(?:ITEM|ITM)\s*#?\s*:?\s*([A-Z0-9][\w\-\.\/]{2,25})', desc_upper):
        val = m.group(1).strip().rstrip(".,;)")
        if len(val) >= 3 and val not in result["item_numbers"]:
            result["item_numbers"].append(val)

    # REF:
    for m in re.finditer(r'REF[:\s]+([A-Z0-9][\w\-\.]{2,20})', desc_upper):
        val = m.group(1).strip()
        if val not in result["ref_codes"]:
            result["ref_codes"].append(val)

    # SKU
    for m in re.finditer(r'(?:SKU|SKID)\s*:?\s*([A-Z0-9][\w\-]{3,25})', desc_upper):
        val = m.group(1).strip()
        if val not in result["skus"]:
            result["skus"].append(val)

    # Standalone # identifiers
    for m in re.finditer(r'#\s*([A-Z0-9][\w\-\.]{2,20})', desc_upper):
        val = m.group(1).strip().rstrip(".,;)")
        if val.isdigit() and len(val) >= 12:
            continue
        if val.isdigit() and len(val) <= 3:
            continue
        if val not in result["raw_identifiers"]:
            result["raw_identifiers"].append(val)

    # Uline S-XXXXX
    for m in re.finditer(r'\b(S-\d{4,6})\b', desc_upper):
        if m.group(1) not in result["skus"]:
            result["skus"].append(m.group(1))

    # Primary MFG number
    primary_mfg = ""
    if result["mfg_numbers"]:
        primary_mfg = result["mfg_numbers"][0]
    elif result["item_numbers"]:
        primary_mfg = result["item_numbers"][0]
    elif result["raw_identifiers"]:
        primary_mfg = result["raw_identifiers"][0]

    mfg_name = _extract_manufacturer(desc)
    enriched = _build_enriched_description(desc, result, mfg_name)

    search_query = primary_mfg if primary_mfg else _extract_search_terms(desc)
    search_url = f"https://www.google.com/search?q={urllib.parse.quote_plus(search_query)}&tbm=shop" if search_query else ""

    amazon_url = ""
    if result["asins"]:
        amazon_url = f"https://www.amazon.com/dp/{result['asins'][0]}"
    elif primary_mfg:
        amazon_url = f"https://www.amazon.com/s?k={urllib.parse.quote_plus(primary_mfg)}"

    return {
        "identifiers": result,
        "primary_mfg_number": primary_mfg,
        "primary_upc": result["upc_codes"][0] if result["upc_codes"] else "",
        "primary_asin": result["asins"][0] if result["asins"] else "",
        "primary_nsn": result["nsns"][0] if result["nsns"] else "",
        "mfg_name": mfg_name,
        "enriched_description": enriched,
        "search_url": search_url,
        "amazon_url": amazon_url,
        "identifier_count": sum(len(v) for v in result.values()),
    }


_KNOWN_MFGS = {
    "MEDLINE": "Medline", "MCKESSON": "McKesson", "DYNAREX": "Dynarex",
    "RUBBERMAID": "Rubbermaid", "3M": "3M", "INVACARE": "Invacare",
    "CROCS": "Crocs", "CLOROX": "Clorox", "STERIS": "Steris",
    "RESMED": "ResMed", "ULINE": "Uline", "STRYKER": "Stryker",
    "BAXTER": "Baxter", "MICROFLEX": "Microflex/Ansell",
    "TRONEX": "Tronex", "RICHARDSON": "Richardson Products",
    "JOERNS": "Joerns Healthcare", "BD ": "Becton Dickinson",
    "TEGADERM": "3M Tegaderm", "KINSMAN": "Kinsman",
}


def _extract_manufacturer(description):
    desc_upper = (description or "").upper()
    for keyword, mfg_name in _KNOWN_MFGS.items():
        if keyword in desc_upper:
            return mfg_name
    return ""


def _build_enriched_description(original, identifiers, mfg_name):
    clean = re.sub(r'\s+', ' ', original).strip()
    clean = re.sub(r'\xa0', ' ', clean)
    extras = []
    if mfg_name:
        extras.append(f"MFG: {mfg_name}")
    if identifiers["mfg_numbers"]:
        extras.append(f"MFG#: {identifiers['mfg_numbers'][0]}")
    if identifiers["upc_codes"]:
        extras.append(f"UPC: {identifiers['upc_codes'][0]}")
    if identifiers["asins"]:
        extras.append(f"ASIN: {identifiers['asins'][0]}")
    if extras:
        return clean + " | " + " | ".join(extras)
    return clean


def _extract_search_terms(description):
    clean = re.sub(r'[#\$\(\)\[\]]', ' ', description)
    clean = re.sub(r'\b\d{12,}\b', '', clean)
    clean = re.sub(r'\b\d+/\w{2,4}\b', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    parts = re.split(r'[,;]|\bItem\b|\bMFR\b|\bRef\b', clean, flags=re.IGNORECASE)
    return parts[0].strip()[:80] if parts else clean[:80]


def enrich_catalog():
    """Run identifier enrichment on all catalog items."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=30)

    log.info("Enriching catalog with product identifiers...")

    rows = db.execute("""
        SELECT description FROM scprs_catalog
        WHERE enrichment_status = 'raw' OR enrichment_status = '' OR enrichment_status IS NULL
        LIMIT 5000
    """).fetchall()

    log.info("Enriching %d catalog items", len(rows))
    enriched_count = 0

    for row in rows:
        desc = row[0]
        if not desc:
            continue
        try:
            parsed = parse_identifiers(desc)
            db.execute("""
                UPDATE scprs_catalog SET
                    mfg_number = COALESCE(NULLIF(?, ''), mfg_number),
                    mfg_name = COALESCE(NULLIF(?, ''), mfg_name),
                    upc = COALESCE(NULLIF(?, ''), upc),
                    asin = COALESCE(NULLIF(?, ''), asin),
                    nsn = COALESCE(NULLIF(?, ''), nsn),
                    sku = COALESCE(NULLIF(?, ''), sku),
                    identifiers_json = ?,
                    enriched_description = ?,
                    enrichment_status = ?,
                    updated_at = datetime('now')
                WHERE description = ?
            """, (
                parsed["primary_mfg_number"], parsed["mfg_name"],
                parsed["primary_upc"], parsed["primary_asin"],
                parsed["primary_nsn"],
                parsed["identifiers"]["skus"][0] if parsed["identifiers"]["skus"] else "",
                json.dumps(parsed["identifiers"], default=str),
                parsed["enriched_description"],
                "enriched" if parsed["identifier_count"] > 0 else "no_identifiers",
                desc,
            ))
            enriched_count += 1
        except Exception as e:
            log.warning("Enrich '%s' failed: %s", desc[:40], str(e)[:40])

    db.commit()
    db.close()
    log.info("Catalog enrichment complete: %d items processed", enriched_count)
    return enriched_count


def set_product_url(description, url, verified=True):
    """User confirms a product URL — persist permanently."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)

    asin = ""
    asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
    if asin_match:
        asin = asin_match.group(1)

    db.execute("""
        UPDATE scprs_catalog SET
            product_url = ?, product_url_verified = ?,
            asin = COALESCE(NULLIF(?, ''), asin),
            enrichment_status = 'verified', updated_at = datetime('now')
        WHERE description = ?
    """, (url, 1 if verified else 0, asin, description))
    db.commit()
    db.close()
    log.info("Product URL set: %s -> %s", description[:40], url[:60])


def search_product_url(description):
    """Generate search URLs to help user find the product."""
    parsed = parse_identifiers(description)
    primary_id = parsed["primary_mfg_number"]
    mfg = parsed["mfg_name"]

    urls = {}
    if parsed["primary_asin"]:
        urls["amazon_direct"] = f"https://www.amazon.com/dp/{parsed['primary_asin']}"
    if primary_id:
        urls["amazon_search"] = f"https://www.amazon.com/s?k={urllib.parse.quote_plus(primary_id)}"

    search_term = f"{mfg} {primary_id}" if mfg and primary_id else (primary_id or _extract_search_terms(description))
    urls["google_shopping"] = f"https://www.google.com/search?q={urllib.parse.quote_plus(search_term)}&tbm=shop"
    urls["google"] = f"https://www.google.com/search?q={urllib.parse.quote_plus(search_term)}"

    if parsed["primary_upc"]:
        urls["upc_lookup"] = f"https://www.barcodelookup.com/{parsed['primary_upc']}"
    if any(s.startswith("S-") for s in parsed["identifiers"]["skus"]):
        uline_sku = next(s for s in parsed["identifiers"]["skus"] if s.startswith("S-"))
        urls["uline"] = f"https://www.uline.com/{uline_sku}"

    return {"search_urls": urls, "parsed_identifiers": parsed, "search_term": search_term}
