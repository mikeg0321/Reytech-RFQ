"""
California Sales & Use Tax Rate Lookup
───────────────────────────────────────
Uses CDTFA REST API for real-time rates, with hardcoded fallbacks
for known facility zip codes.

API: https://services.maps.cdtfa.ca.gov/api/taxrate/GetRateByAddress
Ref: https://maps.cdtfa.ca.gov/

All quotes MUST include tax. Shipping is baked into item cost/margin.
"""

import logging
import json
import os
import time
from datetime import datetime, timedelta

log = logging.getLogger("reytech.tax")

# ── Circuit breaker for CDTFA API ────────────────────────────────────────────
_cdtfa_failures = 0
_cdtfa_circuit_open_until = 0

# ── Cache: avoid hitting CDTFA API repeatedly ─────────────────────────────────
_rate_cache = {}  # zip -> (rate, fetched_at)
CACHE_TTL = timedelta(hours=24)

# ── Data dir for persistent cache ─────────────────────────────────────────────
try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))

# ── Hardcoded fallback rates by zip (updated Feb 2026) ────────────────────────
# Source: https://cdtfa.ca.gov/taxes-and-fees/sales-use-tax-rates.htm
# These are used when CDTFA API is unreachable
FALLBACK_RATES = {
    # CalVet facilities
    "96002": 0.0825,   # Redding (Shasta County)
    "94599": 0.0800,   # Yountville (Napa County)
    "92311": 0.0775,   # Barstow (San Bernardino County)
    "91911": 0.0875,   # Chula Vista (San Diego County)
    "93706": 0.0863,   # Fresno
    "90049": 0.1025,   # West Los Angeles (LA County)
    "95380": 0.0788,   # Moosehaven (Stanislaus County)
    "93004": 0.0725,   # Ventura

    # CDCR/CCHCS common facilities
    "95814": 0.0875,   # Sacramento (HQ)
    "95818": 0.0875,   # Sacramento (CDCR billing)
    "91710": 0.1000,   # Chino (CIM)
    "92880": 0.0875,   # Corona (CIW)
    "93212": 0.0775,   # Corcoran
    "93536": 0.1025,   # Lancaster
    "95696": 0.0813,   # Vacaville (CSP-SOL)
    "95202": 0.0925,   # Stockton (CHCF)
    "93210": 0.0725,   # Coalinga
    "93215": 0.0775,   # Delano
    "95640": 0.0775,   # Ione (MCSP)
    "93280": 0.0775,   # Wasco
    "93610": 0.0863,   # Chowchilla (CCWF/VSPW)
    "93960": 0.0925,   # Soledad (CTF)
    "95531": 0.0850,   # Crescent City (PBSP)
    "92860": 0.0875,   # Norco (CRC)
    "93561": 0.0775,   # Tehachapi (CCI)
    "93204": 0.0725,   # Avenal (ASP)
    "96130": 0.0725,   # Susanville (HDSP)
    "92226": 0.0775,   # Blythe (ISP)
    "95671": 0.0863,   # Represa (FSP)
    "94964": 0.0925,   # San Quentin
    "92233": 0.0775,   # Calipatria
    "92243": 0.0775,   # Imperial (CEN)
    "95327": 0.0775,   # Jamestown (SCC)
    "93409": 0.0775,   # San Luis Obispo (CMC)

    # DGS / DSH
    "94203": 0.0875,   # Sacramento (DGS)
    "93560": 0.0775,   # Ridgecrest

    # Reytech (origin, used when ship-to unknown)
    "92679": 0.0775,   # Trabuco Canyon (Orange County base rate)
}

# CA statewide minimum
CA_BASE_RATE = 0.0725


def lookup_tax_rate(address: str = "", city: str = "", zip_code: str = "",
                    facility_key: str = "") -> dict:
    """
    Look up California sales tax rate for a shipping address.

    Returns: {
        "rate": float,           # e.g. 0.0875
        "rate_pct": str,         # e.g. "8.75%"
        "jurisdiction": str,     # e.g. "SACRAMENTO"
        "source": str,           # "cdtfa_api" | "cache" | "fallback" | "default"
        "city": str,
        "county": str,
    }
    """
    # Normalize zip
    zip5 = ""
    if zip_code:
        zip5 = zip_code.strip().replace("-", "")[:5]
    elif address:
        import re
        zm = re.search(r'\b(\d{5})\b', address)
        if zm:
            zip5 = zm.group(1)

    # Check memory cache
    if zip5 and zip5 in _rate_cache:
        cached_rate, cached_at = _rate_cache[zip5]
        if datetime.now() - cached_at < CACHE_TTL:
            return {**cached_rate, "source": "cache"}

    # Try CDTFA API
    api_result = _call_cdtfa_api(address, city, zip5)
    if api_result:
        _rate_cache[zip5] = (api_result, datetime.now())
        _save_rate_cache(zip5, api_result)
        return {**api_result, "source": "cdtfa_api"}

    # Fallback: hardcoded rates by zip
    if zip5 and zip5 in FALLBACK_RATES:
        rate = FALLBACK_RATES[zip5]
        result = {
            "rate": rate,
            "rate_pct": f"{rate * 100:.2f}%",
            "jurisdiction": city.upper() if city else zip5,
            "city": city,
            "county": "",
            "source": "fallback",
        }
        _rate_cache[zip5] = (result, datetime.now())
        return result

    # Try persistent cache (from previous API calls)
    persisted = _load_rate_cache(zip5)
    if persisted:
        _rate_cache[zip5] = (persisted, datetime.now())
        return {**persisted, "source": "persisted_cache"}

    # Default: CA statewide base rate
    log.warning("Tax rate: no rate found for zip=%s, using CA base %.4f", zip5, CA_BASE_RATE)
    return {
        "rate": CA_BASE_RATE,
        "rate_pct": f"{CA_BASE_RATE * 100:.2f}%",
        "jurisdiction": "CALIFORNIA (DEFAULT)",
        "city": city,
        "county": "",
        "source": "default",
    }


def get_rate_for_facility(facility: dict) -> dict:
    """Look up tax rate from a FACILITY_DB entry (has address list)."""
    if not facility:
        return lookup_tax_rate()

    addr_lines = facility.get("address", [])
    name = facility.get("name", "")

    # Extract city and zip from address lines
    city = ""
    zip_code = ""
    address = ""
    import re

    if addr_lines:
        address = addr_lines[0] if addr_lines else ""
        # Last line usually has "City, CA ZIPCODE"
        last = addr_lines[-1] if len(addr_lines) > 0 else ""
        csz = re.match(r'([^,]+),\s*CA\.?\s*(\d{5})', last)
        if csz:
            city = csz.group(1).strip()
            zip_code = csz.group(2)

    return lookup_tax_rate(address=address, city=city, zip_code=zip_code)


def _call_cdtfa_api(address: str, city: str, zip_code: str) -> dict:
    """Call CDTFA REST API for tax rate. Returns dict or None on failure."""
    global _cdtfa_failures, _cdtfa_circuit_open_until

    if not zip_code and not address:
        return None

    # Circuit breaker check — skip API if too many recent failures
    if _cdtfa_failures >= 5 and time.time() < _cdtfa_circuit_open_until:
        log.warning("CDTFA circuit breaker open — skipping API call (will retry after %ds)",
                     int(_cdtfa_circuit_open_until - time.time()))
        return None  # fall through to fallback logic

    import urllib.request
    import urllib.parse
    import urllib.error

    params = {}
    if address:
        params["address"] = address
    if city:
        params["city"] = city
    if zip_code:
        params["zip"] = zip_code

    url = "https://services.maps.cdtfa.ca.gov/api/taxrate/GetRateByAddress?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())

        rates = data.get("taxRateInfo", [])
        if not rates:
            log.warning("CDTFA API returned no rates for zip=%s", zip_code)
            return None

        # Use first (highest confidence) rate
        best = rates[0]
        rate = best.get("rate", CA_BASE_RATE)
        result = {
            "rate": rate,
            "rate_pct": f"{rate * 100:.2f}%",
            "jurisdiction": best.get("jurisdiction", ""),
            "city": best.get("city", city),
            "county": best.get("county", ""),
        }
        log.info("CDTFA API: zip=%s → rate=%.4f jurisdiction=%s",
                 zip_code, rate, result["jurisdiction"])
        # API success — reset circuit breaker
        _cdtfa_failures = 0
        return result

    except urllib.error.URLError as e:
        log.warning("CDTFA API unreachable: %s", e)
        _cdtfa_failures += 1
        if _cdtfa_failures >= 5:
            _cdtfa_circuit_open_until = time.time() + 3600
            log.error("CDTFA circuit breaker OPENED after %d failures — fallback rates for 1 hour", _cdtfa_failures)
        return None
    except Exception as e:
        log.warning("CDTFA API error: %s", e)
        _cdtfa_failures += 1
        if _cdtfa_failures >= 5:
            _cdtfa_circuit_open_until = time.time() + 3600
            log.error("CDTFA circuit breaker OPENED after %d failures — fallback rates for 1 hour", _cdtfa_failures)
        return None


# ── Persistent cache (JSON file) ──────────────────────────────────────────────

def _cache_path():
    return os.path.join(DATA_DIR, "tax_rate_cache.json")


def _save_rate_cache(zip5: str, rate_data: dict):
    """Append/update rate in persistent cache file."""
    try:
        cache = {}
        if os.path.exists(_cache_path()):
            with open(_cache_path()) as f:
                cache = json.load(f)
        cache[zip5] = {
            **rate_data,
            "cached_at": datetime.now().isoformat(),
        }
        with open(_cache_path(), "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        log.debug("Tax cache save error: %s", e)


def _load_rate_cache(zip5: str) -> dict:
    """Load rate from persistent cache file."""
    if not zip5:
        return None
    try:
        if os.path.exists(_cache_path()):
            with open(_cache_path()) as f:
                cache = json.load(f)
            entry = cache.get(zip5)
            if entry:
                # Check if not too stale (7 days for persisted)
                cached_at = entry.get("cached_at", "")
                if cached_at:
                    age = datetime.now() - datetime.fromisoformat(cached_at)
                    if age < timedelta(days=7):
                        return entry
    except Exception as e:
        log.debug("Tax cache load error: %s", e)
    return None
