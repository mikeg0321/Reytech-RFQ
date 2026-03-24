"""
Institution Name Resolver — Canonical name mapping for CA state agencies.

Normalizes the dozens of ways buyers refer to the same facility:
  "CSP-SAC" → "California State Prison, Sacramento"
  "VHC Yountville" → "Veterans Home of California, Yountville"
  "Patton" → "DSH — Patton State Hospital"

Used at:
  - Extraction time (parsers) to normalize before storage
  - Match time (pc_rfq_linker) to compare institutions accurately
  - Display time (templates) for clean, consistent names
"""

import re
import logging

log = logging.getLogger("reytech.institution_resolver")

# ═══════════════════════════════════════════════════════════════════════════════
# CDCR / CCHCS Facilities
# ═══════════════════════════════════════════════════════════════════════════════

_CDCR_FACILITIES = {
    # Abbreviation → (Full Name, City)
    "ASP":   ("Avenal State Prison", "Avenal"),
    "CAL":   ("California State Prison, Calipatria", "Calipatria"),
    "CCC":   ("California Correctional Center", "Susanville"),
    "CCWF":  ("Central California Women's Facility", "Chowchilla"),
    "CEN":   ("Centinela State Prison", "Imperial"),
    "CHCF":  ("California Health Care Facility", "Stockton"),
    "CIM":   ("California Institution for Men", "Chino"),
    "CIW":   ("California Institution for Women", "Corona"),
    "CMC":   ("California Men's Colony", "San Luis Obispo"),
    "CMF":   ("California Medical Facility", "Vacaville"),
    "COR":   ("California State Prison, Corcoran", "Corcoran"),
    "CRC":   ("California Rehabilitation Center", "Norco"),
    "CTF":   ("Correctional Training Facility", "Soledad"),
    "CVSP":  ("Chuckawalla Valley State Prison", "Blythe"),
    "DVI":   ("Deuel Vocational Institution", "Tracy"),
    "FSP":   ("Folsom State Prison", "Represa"),
    "HDSP":  ("High Desert State Prison", "Susanville"),
    "ISP":   ("Ironwood State Prison", "Blythe"),
    "KVSP":  ("Kern Valley State Prison", "Delano"),
    "LAC":   ("California State Prison, Los Angeles County", "Lancaster"),
    "MCSP":  ("Mule Creek State Prison", "Ione"),
    "NKSP":  ("North Kern State Prison", "Delano"),
    "PBSP":  ("Pelican Bay State Prison", "Crescent City"),
    "PVSP":  ("Pleasant Valley State Prison", "Coalinga"),
    "RJD":   ("Richard J. Donovan Correctional Facility", "San Diego"),
    "SAC":   ("California State Prison, Sacramento", "Sacramento"),
    "SCC":   ("Sierra Conservation Center", "Jamestown"),
    "SOL":   ("California State Prison, Solano", "Vacaville"),
    "SQ":    ("San Quentin State Prison", "San Quentin"),
    "SATF":  ("Substance Abuse Treatment Facility", "Corcoran"),
    "SVSP":  ("Salinas Valley State Prison", "Soledad"),
    "VSP":   ("Valley State Prison", "Chowchilla"),
    "WSP":   ("Wasco State Prison", "Wasco"),
    "CSP":   ("California State Prison", ""),  # generic — needs location suffix
}

# City → CDCR abbreviation (reverse lookup for facility name matching)
_CDCR_CITIES = {}
for _abbr, (_name, _city) in _CDCR_FACILITIES.items():
    if _city:
        _CDCR_CITIES[_city.lower()] = _abbr
    # Also index key words from facility name
    for _word in _name.lower().split():
        if len(_word) >= 5 and _word not in ("state", "prison", "california", "facility", "center", "valley"):
            _CDCR_CITIES[_word] = _abbr

# ═══════════════════════════════════════════════════════════════════════════════
# CalVet Facilities
# ═══════════════════════════════════════════════════════════════════════════════

_CALVET_FACILITIES = {
    "yountville":   "Veterans Home of California, Yountville",
    "barstow":      "Veterans Home of California, Barstow",
    "chula vista":  "Veterans Home of California, Chula Vista",
    "fresno":       "Veterans Home of California, Fresno",
    "lancaster":    "Veterans Home of California, Lancaster",
    "ventura":      "Veterans Home of California, Ventura",
    "west la":      "Veterans Home of California, West Los Angeles",
    "west los angeles": "Veterans Home of California, West Los Angeles",
    "wla":          "Veterans Home of California, West Los Angeles",
    "redding":      "Veterans Home of California, Redding",
}

# ═══════════════════════════════════════════════════════════════════════════════
# DSH Facilities
# ═══════════════════════════════════════════════════════════════════════════════

_DSH_FACILITIES = {
    "atascadero":   "DSH — Atascadero State Hospital",
    "coalinga":     "DSH — Coalinga State Hospital",
    "metropolitan": "DSH — Metropolitan State Hospital",
    "napa":         "DSH — Napa State Hospital",
    "patton":       "DSH — Patton State Hospital",
}

# ═══════════════════════════════════════════════════════════════════════════════
# Agency Aliases (abbreviation → canonical display name)
# ═══════════════════════════════════════════════════════════════════════════════

_AGENCY_ALIASES = {
    "cdcr": "CDCR",
    "cchcs": "CCHCS / CDCR",
    "calvet": "CalVet",
    "cal vet": "CalVet",
    "cva": "CalVet",
    "dva": "CalVet",
    "veterans affairs": "CalVet",
    "department of veterans affairs": "CalVet",
    "dgs": "DGS",
    "general services": "DGS",
    "department of general services": "DGS",
    "dsh": "DSH",
    "state hospitals": "DSH",
    "department of state hospitals": "DSH",
    "calfire": "CAL FIRE",
    "cal fire": "CAL FIRE",
    "forestry": "CAL FIRE",
    "fire protection": "CAL FIRE",
    "calrecycle": "CalRecycle",
    "cal recycle": "CalRecycle",
    "hhsa": "HHSA",
    "dfpi": "DFPI",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def resolve(raw_name: str) -> dict:
    """Resolve a raw institution/agency name to canonical form.

    Returns:
        {
            "canonical": str,      # Normalized display name
            "agency": str,         # Agency key (calvet, cchcs, dsh, dgs, etc.)
            "facility_code": str,  # Abbreviation if known (CSP, SAC, etc.)
            "original": str,       # Input as-is
        }
    """
    if not raw_name or not raw_name.strip():
        return {"canonical": "", "agency": "", "facility_code": "", "original": ""}

    original = raw_name.strip()
    text = _normalize_text(original)

    # 1. Try CDCR facility abbreviation match (CSP-SAC, CIM, CHCF, etc.)
    cdcr_match = _match_cdcr(text)
    if cdcr_match:
        return {**cdcr_match, "original": original}

    # 2. Try CalVet facility match
    calvet_match = _match_calvet(text)
    if calvet_match:
        return {**calvet_match, "original": original}

    # 3. Try DSH facility match
    dsh_match = _match_dsh(text)
    if dsh_match:
        return {**dsh_match, "original": original}

    # 4. Try agency alias
    alias_match = _match_alias(text)
    if alias_match:
        return {**alias_match, "original": original}

    # 5. No match — return cleaned version of original
    return {"canonical": original, "agency": "", "facility_code": "", "original": original}


def normalize(raw_name: str) -> str:
    """Convenience: resolve and return just the canonical name."""
    return resolve(raw_name).get("canonical", raw_name or "")


def same_institution(name_a: str, name_b: str) -> bool:
    """Compare two institution names after normalization.

    Returns True if they resolve to the same canonical name,
    or if one is a substring of the other after normalization.
    """
    if not name_a or not name_b:
        return False

    ra = resolve(name_a)
    rb = resolve(name_b)

    # Same canonical name
    ca = ra["canonical"].lower()
    cb = rb["canonical"].lower()
    if ca and cb and ca == cb:
        return True

    # Same agency + facility code
    if ra["facility_code"] and ra["facility_code"] == rb["facility_code"]:
        return True

    # Same agency (if no facility code to distinguish)
    if ra["agency"] and ra["agency"] == rb["agency"]:
        # Both are the same general agency — consider a match
        # unless both have different facility codes
        if not ra["facility_code"] and not rb["facility_code"]:
            return True

    # Substring match on canonical names
    if len(ca) >= 5 and len(cb) >= 5 and (ca in cb or cb in ca):
        return True

    return False


# ═══════════════════════════════════════════════════════════════════════════════
# Internal Matching
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_text(text: str) -> str:
    """Lowercase, strip punctuation, normalize whitespace."""
    text = text.lower().strip()
    text = re.sub(r'[—–\-_,.:;/\\]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def _match_cdcr(text: str) -> dict:
    """Match CDCR/CCHCS facilities by abbreviation or name keywords."""
    # Check for "CSP-SAC" or "CSP SAC" pattern
    m = re.match(r'^(csp)\s+(\w+)', text)
    if m:
        suffix = m.group(2).upper()
        # Try suffix as abbreviation
        if suffix in _CDCR_FACILITIES:
            name, city = _CDCR_FACILITIES[suffix]
            return {"canonical": name, "agency": "cchcs", "facility_code": suffix}
        # Try suffix as city
        if suffix.lower() in _CDCR_CITIES:
            code = _CDCR_CITIES[suffix.lower()]
            name, city = _CDCR_FACILITIES[code]
            return {"canonical": name, "agency": "cchcs", "facility_code": code}
        # Generic CSP with location
        return {"canonical": f"California State Prison, {suffix.title()}", "agency": "cchcs", "facility_code": "CSP"}

    # Check for exact abbreviation
    words = text.split()
    if words:
        first = words[0].upper()
        if first in _CDCR_FACILITIES and first != "CSP":
            name, city = _CDCR_FACILITIES[first]
            return {"canonical": name, "agency": "cchcs", "facility_code": first}

    # Check for CDCR/CCHCS keyword + city/facility
    if any(kw in text for kw in ("cdcr", "cchcs", "corrections", "correctional", "prison", "state prison")):
        # Try to find facility by city name
        for city, code in _CDCR_CITIES.items():
            if city in text:
                name, _ = _CDCR_FACILITIES[code]
                return {"canonical": name, "agency": "cchcs", "facility_code": code}
        # Generic CDCR
        return {"canonical": "CDCR", "agency": "cchcs", "facility_code": ""}

    # Check for facility city names without CDCR prefix
    for city, code in _CDCR_CITIES.items():
        if len(city) >= 5 and text == city:
            name, _ = _CDCR_FACILITIES[code]
            return {"canonical": name, "agency": "cchcs", "facility_code": code}

    return None


def _match_calvet(text: str) -> dict:
    """Match CalVet facilities."""
    if any(kw in text for kw in ("calvet", "cal vet", "cva", "veterans home", "veterans affairs", "vhc")):
        # Try to find specific facility
        for loc, full_name in _CALVET_FACILITIES.items():
            if loc in text:
                return {"canonical": full_name, "agency": "calvet", "facility_code": f"VHC-{loc.title().replace(' ', '')}"}
        return {"canonical": "CalVet", "agency": "calvet", "facility_code": ""}

    # Check for facility location without CalVet prefix (less confident)
    for loc, full_name in _CALVET_FACILITIES.items():
        if len(loc) >= 5 and text == loc:
            return {"canonical": full_name, "agency": "calvet", "facility_code": f"VHC-{loc.title().replace(' ', '')}"}

    return None


def _match_dsh(text: str) -> dict:
    """Match DSH facilities."""
    if any(kw in text for kw in ("dsh", "state hospital", "department of state hospitals")):
        for loc, full_name in _DSH_FACILITIES.items():
            if loc in text:
                return {"canonical": full_name, "agency": "dsh", "facility_code": f"DSH-{loc.title()}"}
        return {"canonical": "DSH", "agency": "dsh", "facility_code": ""}

    return None


def _match_alias(text: str) -> dict:
    """Match simple agency aliases."""
    for alias, canonical in _AGENCY_ALIASES.items():
        if text == alias or text.startswith(alias + " "):
            return {"canonical": canonical, "agency": canonical.lower().split()[0], "facility_code": ""}
    return None
