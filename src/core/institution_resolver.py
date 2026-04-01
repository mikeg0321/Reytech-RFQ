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
# Address / ZIP → Facility Mapping (for ship-to address resolution)
# ═══════════════════════════════════════════════════════════════════════════════

_ADDRESS_FACILITIES = {
    # CalVet facilities
    "91911": ("Veterans Home of California, Chula Vista", "calvet", "VHC-ChulaVista"),
    "92311": ("Veterans Home of California, Barstow", "calvet", "VHC-Barstow"),
    "94599": ("Veterans Home of California, Yountville", "calvet", "VHC-Yountville"),
    "93721": ("Veterans Home of California, Fresno", "calvet", "VHC-Fresno"),
    "93534": ("Veterans Home of California, Lancaster", "calvet", "VHC-Lancaster"),
    "93003": ("Veterans Home of California, Ventura", "calvet", "VHC-Ventura"),
    "90073": ("Veterans Home of California, West Los Angeles", "calvet", "VHC-WLA"),
    "96001": ("Veterans Home of California, Redding", "calvet", "VHC-Redding"),
    # CDCR facilities by ZIP
    "92179": ("Richard J. Donovan Correctional Facility", "cchcs", "RJD"),
    "91710": ("California Institution for Men", "cchcs", "CIM"),
    "92880": ("California Institution for Women", "cchcs", "CIW"),
    "93409": ("California Men's Colony", "cchcs", "CMC"),
    "95696": ("California Medical Facility", "cchcs", "CMF"),
    "95763": ("Folsom State Prison", "cchcs", "FSP"),
    "94964": ("San Quentin State Prison", "cchcs", "SQ"),
    "95202": ("California Health Care Facility", "cchcs", "CHCF"),
    # DSH facilities by ZIP
    "93423": ("DSH — Atascadero State Hospital", "dsh", "ASH"),
    "93210": ("DSH — Coalinga State Hospital", "dsh", "CSH"),
    "90660": ("DSH — Metropolitan State Hospital", "dsh", "MSH"),
    "94558": ("DSH — Napa State Hospital", "dsh", "NSH"),
    "92369": ("DSH — Patton State Hospital", "dsh", "PSH"),
}

# Street address keywords → facility (when ZIP alone is ambiguous)
_ADDRESS_KEYWORDS = {
    "naples": ("Veterans Home of California, Chula Vista", "calvet", "VHC-ChulaVista"),
    "alta rd": ("Richard J. Donovan Correctional Facility", "cchcs", "RJD"),
    "donovan": ("Richard J. Donovan Correctional Facility", "cchcs", "RJD"),
    "carnoustie": ("Reytech Inc.", "", ""),  # Our own address
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

    # 5. Try address/zip-based resolution (ship-to addresses)
    addr_match = _match_address(text)
    if addr_match:
        return {**addr_match, "original": original}

    # 6. No match — return cleaned version of original
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

    # Check for exact abbreviation (with optional unit/program suffix like "ML EOP")
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


def _match_address(text: str) -> dict:
    """Match ship-to addresses by ZIP code or street keywords."""
    import re
    # Extract ZIP code — look for 5 digits near end (after state abbreviation)
    zip_match = re.search(r'(?:CA|california)\s+(\d{5})\b', text, re.IGNORECASE)
    if not zip_match:
        # Fallback: last 5-digit number in the string
        all_zips = re.findall(r'\b(\d{5})\b', text)
        zip_match = type('M', (), {'group': lambda s, n: all_zips[-1]})() if all_zips else None
    if zip_match:
        z = zip_match.group(1)
        if z in _ADDRESS_FACILITIES:
            name, agency, code = _ADDRESS_FACILITIES[z]
            return {"canonical": name, "agency": agency, "facility_code": code}

    # Check street address keywords
    text_lower = text.lower()
    for keyword, (name, agency, code) in _ADDRESS_KEYWORDS.items():
        if keyword in text_lower:
            return {"canonical": name, "agency": agency, "facility_code": code}

    return None


# ── Backward-compatibility aliases ──────────────────────────────────────────
resolve_institution = resolve
canonical_name = normalize
