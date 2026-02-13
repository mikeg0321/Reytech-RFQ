"""
CDTFA Tax Rate Agent
Automates California "Find a Sales and Use Tax Rate" lookup.
https://maps.cdtfa.ca.gov/ → https://services.maps.cdtfa.ca.gov/api/taxrate/

Primary: REST API (GetRateByAddress)
Fallback: Local cache → CA base rate 7.25%

API requires: address (no PO Boxes), city, zip (all required)
Returns: rate, jurisdiction, city, county, tac, confidence
"""
import os, json, re, logging
from datetime import datetime

log = logging.getLogger("cdtfa_tax")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

CA_BASE_RATE = 0.0725  # statewide minimum
CACHE_FILE = os.path.join(DATA_DIR, "tax_rates_cache.json")
API_BASE = "https://services.maps.cdtfa.ca.gov/api/taxrate"


# ═══════════════════════════════════════════════════════════════
# Address Parsing Helpers
# ═══════════════════════════════════════════════════════════════

def extract_zip(text):
    """Pull 5-digit zip from any text (address line, full address, etc.)."""
    lines = text if isinstance(text, list) else [text]
    for line in lines:
        m = re.search(r'\b(\d{5})(?:-\d{4})?\b', str(line))
        if m: return m.group(1)
    return ""


def extract_city(text):
    """Extract city from 'City, ST ZIP' or 'City, ST' pattern."""
    lines = text if isinstance(text, list) else [text]
    for line in lines:
        line = str(line).strip()
        # Match "Jamestown, CA 95327" → "Jamestown"
        m = re.match(r'^([A-Za-z\s\.]+),\s*[A-Z]{2}\s+\d{5}', line)
        if m: return m.group(1).strip()
        m = re.match(r'^([A-Za-z\s\.]+),\s*[A-Z]{2}\b', line)
        if m: return m.group(1).strip()
    return ""


def extract_street(text):
    """Extract street address (first line that looks like a street)."""
    if isinstance(text, list):
        for line in text:
            line = line.strip()
            # Skip lines that are city/state/zip, "United States", or PO Boxes
            if re.match(r'^[A-Z][a-z]+.*,\s*[A-Z]{2}\s+\d{5}', line): continue
            if "United States" in line: continue
            if re.match(r'^P\.?O\.?\s+Box', line, re.I): continue
            if re.search(r'\d+\s+\w', line):  # has a number + word = street
                return line
        return text[0] if text else ""
    return str(text).strip()


def is_po_box(address):
    """CDTFA API rejects PO Box addresses."""
    return bool(re.match(r'^P\.?O\.?\s+Box', str(address), re.I))


def parse_ship_to(ship_to_name, ship_to_address):
    """
    Parse ship-to into API-ready components.
    Returns: {street, city, zip, raw_input}
    """
    all_text = [ship_to_name] + (ship_to_address or [])
    zip_code = extract_zip(all_text)
    city = extract_city(all_text)
    street = extract_street(ship_to_address or [])

    # If street looks like a PO Box, try to use ship_to_name or another line
    if is_po_box(street):
        # Try other lines
        for line in (ship_to_address or []):
            if not is_po_box(line) and re.search(r'\d+\s+\w', line):
                street = line; break
        else:
            street = ""  # No usable street; will use generic fallback

    return {
        "street": street,
        "city": city,
        "zip": zip_code,
        "raw_input": " | ".join(all_text),
    }


# ═══════════════════════════════════════════════════════════════
# Local Cache (offline fallback)
# ═══════════════════════════════════════════════════════════════

def _load_cache():
    try:
        with open(CACHE_FILE) as f: return json.load(f)
    except: return {}

def _save_cache(data):
    with open(CACHE_FILE, "w") as f: json.dump(data, f, indent=2)

def cache_rate(key, result):
    """Store a CDTFA result keyed by 'street|city|zip'."""
    cache = _load_cache()
    cache[key] = {
        "result": result,
        "cached_at": datetime.now().isoformat(),
    }
    _save_cache(cache)

def get_cached_rate(key, max_age_days=30):
    """Retrieve cached rate if fresh enough."""
    cache = _load_cache()
    entry = cache.get(key)
    if not entry: return None
    try:
        cached_at = datetime.fromisoformat(entry["cached_at"])
        if (datetime.now() - cached_at).days <= max_age_days:
            return entry["result"]
    except:
        pass
    return None


# ═══════════════════════════════════════════════════════════════
# CDTFA REST API Calls
# ═══════════════════════════════════════════════════════════════

def _call_api_by_address(street, city, zip_code):
    """
    Call CDTFA GetRateByAddress API.
    All three params required. No PO Boxes.
    Returns parsed result dict or None.
    """
    import requests

    params = {
        "address": street.strip(),
        "city": city.strip(),
        "zip": zip_code,
    }

    url = f"{API_BASE}/GetRateByAddress"
    log.info(f"CDTFA API call: {url} params={params}")

    try:
        r = requests.get(url, params=params, timeout=15)
        log.info(f"CDTFA response: {r.status_code}")

        if r.status_code == 200:
            data = r.json()

            # Check for errors
            if "errors" in data and data["errors"]:
                log.warning(f"CDTFA errors: {data['errors']}")
                return {"error": data["errors"], "source": "cdtfa_api_error"}

            tax_info = data.get("taxRateInfo", [])
            if not tax_info:
                log.warning("CDTFA returned empty taxRateInfo")
                return None

            # Take first result (highest confidence)
            ti = tax_info[0]
            geo = data.get("geocodeInfo", {})

            result = {
                "rate": ti["rate"],
                "jurisdiction": ti.get("jurisdiction", ""),
                "city": ti.get("city", ""),
                "county": ti.get("county", ""),
                "tac": ti.get("tac", ""),
                "confidence": geo.get("confidence", ""),
                "calc_method": geo.get("calcMethod", ""),
                "formatted_address": geo.get("formattedAddress", ""),
                "buffer_distance": geo.get("bufferDistance", 50),
                "source": "cdtfa_api",
                "multiple_rates": len(tax_info) > 1,
            }

            # If multiple rates returned (near boundary), note them
            if len(tax_info) > 1:
                result["all_rates"] = [
                    {"rate": t["rate"], "jurisdiction": t.get("jurisdiction","")}
                    for t in tax_info
                ]
                log.info(f"CDTFA returned {len(tax_info)} rates (boundary): {result['all_rates']}")

            return result

        elif r.status_code == 400:
            try:
                return {"error": r.json().get("errors", []), "source": "cdtfa_api_error"}
            except:
                return {"error": str(r.text), "source": "cdtfa_api_error"}
        else:
            log.warning(f"CDTFA API HTTP {r.status_code}")
            return None

    except requests.exceptions.Timeout:
        log.warning("CDTFA API timeout")
        return None
    except requests.exceptions.ConnectionError as e:
        log.warning(f"CDTFA API connection error: {e}")
        return None
    except Exception as e:
        log.warning(f"CDTFA API unexpected error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# Main Tax Rate Agent
# ═══════════════════════════════════════════════════════════════

def get_tax_rate(ship_to_name="", ship_to_address=None, street=None, city=None, zip_code=None):
    """
    Automated tax rate agent. Resolves CA sales tax by ship-to location.

    Priority:
    1. Direct params (street, city, zip_code) if provided
    2. Parse from ship_to_name + ship_to_address
    3. CDTFA API call with parsed address
    4. Retry with generic street if original fails
    5. Local cache fallback
    6. CA base rate 7.25%

    Returns: {
        rate: float,        # e.g. 0.0875
        rate_pct: str,      # e.g. "8.750%"
        jurisdiction: str,  # e.g. "SACRAMENTO"
        city: str, county: str,
        confidence: str,    # High/Medium/Low
        source: str,        # cdtfa_api / cache / default
        formatted_address: str,
    }
    """
    # Step 1: Resolve address components
    if street and city and zip_code:
        parsed = {"street": street, "city": city, "zip": zip_code}
    else:
        parsed = parse_ship_to(ship_to_name, ship_to_address or [])

    zip_code = parsed["zip"]
    city = parsed["city"]
    street = parsed["street"]
    cache_key = f"{street}|{city}|{zip_code}"

    if not zip_code:
        log.warning(f"No zip code found in: {parsed}")
        return _default_result("No zip code in ship-to address")

    # Step 2: Check local cache first (fast path)
    cached = get_cached_rate(cache_key)
    if cached:
        cached["source"] = "cache"
        log.info(f"Cache hit for {cache_key}: {cached['rate']}")
        return cached

    # Step 3: Try CDTFA API with parsed street
    result = None
    if street and not is_po_box(street):
        result = _call_api_by_address(street, city, zip_code)

    # Step 4: If that failed (no street, PO Box, or API error), try generic street
    if result is None or "error" in result:
        if city and zip_code:
            log.info(f"Retrying with generic address for {city}, {zip_code}")
            result = _call_api_by_address("1 Main St", city, zip_code)

    # Step 5: If still no result, try zip-only with "1 Main St"
    if result is None or "error" in result:
        if zip_code:
            # Try without city (use a placeholder)
            log.info(f"Last resort: generic address with zip {zip_code}")
            # We don't know the city for this zip, but CDTFA requires it.
            # Check cache for any entry with this zip
            all_cache = _load_cache()
            for k, v in all_cache.items():
                if k.endswith(f"|{zip_code}"):
                    r = v.get("result", {})
                    r["source"] = "cache_zip_match"
                    return r

    # Step 6: Process result
    if result and "error" not in result and result.get("rate"):
        result["rate_pct"] = f"{result['rate']*100:.3f}%"
        cache_rate(cache_key, result)
        return result

    # Step 7: Default fallback
    return _default_result(f"CDTFA lookup failed for {cache_key}")


def _default_result(reason=""):
    return {
        "rate": CA_BASE_RATE,
        "rate_pct": f"{CA_BASE_RATE*100:.3f}%",
        "jurisdiction": "CALIFORNIA (BASE)",
        "city": "", "county": "",
        "confidence": "N/A",
        "source": "default",
        "formatted_address": "",
        "note": reason or "Using CA base rate",
    }


# ═══════════════════════════════════════════════════════════════
# Bulk / Batch Lookup (for pre-seeding known locations)
# ═══════════════════════════════════════════════════════════════

def seed_known_locations():
    """
    Pre-lookup tax rates for all known CRM ship-to addresses.
    Run this periodically to keep cache warm.
    Returns list of results.
    """
    # Import CRM contacts
    try:
        from quote_generator import load_contacts
        contacts = load_contacts()
    except:
        contacts = []

    results = []
    for c in contacts:
        if c.get("type") != "ship_to": continue
        name = c.get("name", "")
        addr = c.get("address_lines", [])
        log.info(f"Seeding tax rate for: {name}")
        r = get_tax_rate(ship_to_name=name, ship_to_address=addr)
        results.append({
            "contact": name,
            "rate": r.get("rate"),
            "jurisdiction": r.get("jurisdiction", ""),
            "source": r.get("source", ""),
        })
        log.info(f"  → {r.get('rate')} ({r.get('source')})")

    return results


# ═══════════════════════════════════════════════════════════════
# Dashboard API Helpers
# ═══════════════════════════════════════════════════════════════

def tax_rate_for_rfq(rfq_data):
    """
    Given parsed RFQ data, resolve the tax rate for ship-to location.
    Returns full tax result dict.
    """
    addr = rfq_data.get("delivery_address", "")
    parts = [p.strip() for p in addr.split(",")]
    name = parts[0] if parts else ""
    lines = [", ".join(parts[1:])] if len(parts) > 1 else []
    return get_tax_rate(ship_to_name=name, ship_to_address=lines)


def validate_tax_rate(zip_code, expected_rate):
    """
    Validate an expected rate against CDTFA for a zip code.
    Returns: {valid: bool, cdtfa_rate: float, expected: float, difference: float}
    """
    result = get_tax_rate(zip_code=zip_code, street="1 Main St", city="")
    cdtfa_rate = result.get("rate", CA_BASE_RATE)
    diff = abs(cdtfa_rate - expected_rate)
    return {
        "valid": diff < 0.001,  # within 0.1% tolerance
        "cdtfa_rate": cdtfa_rate,
        "expected": expected_rate,
        "difference": diff,
        "jurisdiction": result.get("jurisdiction", ""),
        "source": result.get("source", ""),
    }


# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test address parsing
    print("=== Address Parsing Tests ===")
    tests = [
        (["5100 O'Byrnes Ferry Road", "Jamestown, CA 95327", "United States"],
         {"zip": "95327", "city": "Jamestown"}),
        (["P.O. Box 187021", "Sacramento, CA 95818-7021", "United States"],
         {"zip": "95818", "city": "Sacramento"}),
        (["260 California Drive", "Yountville, CA 94599", "United States"],
         {"zip": "94599", "city": "Yountville"}),
        (["100 E. Veterans Parkway", "Barstow, CA 92311", "United States"],
         {"zip": "92311", "city": "Barstow"}),
    ]
    for addr, expected in tests:
        z = extract_zip(addr); ci = extract_city(addr)
        st = extract_street(addr)
        ok = z == expected["zip"] and ci == expected["city"]
        print(f"  {'✓' if ok else '✗'} {addr[0][:30]:30s} → zip={z} city={ci} street={st}")

    # Test API call (will work on Railway, may fail in sandbox)
    print("\n=== CDTFA API Test ===")
    r = get_tax_rate(
        ship_to_name="SCC - Sierra Conservation Center",
        ship_to_address=["5100 O'Byrnes Ferry Road", "Jamestown, CA 95327", "United States"]
    )
    print(f"  Rate: {r.get('rate')} ({r.get('rate_pct','')})")
    print(f"  Jurisdiction: {r.get('jurisdiction','')}")
    print(f"  Source: {r.get('source','')}")
    print(f"  Confidence: {r.get('confidence','')}")
    if r.get("note"): print(f"  Note: {r['note']}")
