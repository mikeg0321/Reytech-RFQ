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

## Substrate status — 2026-05-27 collapse (LAW 2 deletion commit)

This module is now a THIN FACADE over `src/core/facility_registry.py`.
The 5 heuristic data tables previously duplicated here —
`_CDCR_FACILITIES`, `_CALVET_FACILITIES`, `_DSH_FACILITIES`,
`_ADDRESS_FACILITIES`, `_ADDRESS_KEYWORDS` — have been DELETED. All
facility lookups now go through `facility_registry.resolve()`, which
is the single source of truth.

Why: the 2026-05-27 facility audit found 6 drift defects where the
heuristic tables here disagreed with the canonical registry:
  - FSP zip 95763 (wrong) vs 95671 (canonical, Represa)
  - DSH-Atascadero zip 93423 vs 93422 (canonical)
  - DSH-Metropolitan zip 90660 vs 90650 (canonical, Norwalk)
  - VHC-Lancaster zip 93534 vs 93536 (canonical)
  - `"prison road"` keyword UNCONDITIONALLY mapped to CSP-SAC, but
    300 Prison Rd is FSP (Old Folsom) and 100 Prison Rd is CSP-SAC
    (New Folsom). The keyword guess shipped wrong addresses.
  - bare `"lancaster"` CalVet match collided with CSP-LAC.

The legacy heuristic-table fallbacks would silently guess on
ambiguous input. The canonical registry refuses to guess — bare
"Folsom" / "Lancaster" / shared zips return None so the caller can
prompt the operator or fall through. This is a STRICT SUPERSET of
the prior CORRECT behavior — the only behavior lost is the wrong-
answer silent guesses, which were defects.

What's PRESERVED in this module:
  - The public dict-shape API (`resolve()` returns
    `{canonical, agency, facility_code, original, source}`)
  - `_match_alias` — agency-alias map (CDCR, CalVet, DSH, etc.)
  - `_match_email_domain` — email-domain → agency mapping
  - `_GARBAGE_NAMES` — form-label words that aren't institutions
  - `_AGENCY_ALIASES`, `_EMAIL_DOMAINS` (no facility addresses here)
  - The 3-input fallback chain (raw → ship_to → email)

Callers should prefer the `quote_contract` facades
(`canonical_name` / `same_institution` / `classify_agency` /
`ship_to_for_text`) — they go through this module which goes
through `facility_registry`. Direct `institution_resolver` imports
are bounded by `tests/test_classify_agency_facade.py`.
"""

import logging
import re

from src.core import facility_registry

log = logging.getLogger("reytech.institution_resolver")

# ═══════════════════════════════════════════════════════════════════════════════
# Email Domain → Agency Mapping
# ═══════════════════════════════════════════════════════════════════════════════

_EMAIL_DOMAINS = {
    "cdcr.ca.gov": ("CDCR", "cchcs"),
    "cchcs.ca.gov": ("CCHCS / CDCR", "cchcs"),
    "calvet.ca.gov": ("CalVet", "calvet"),
    "dsh.ca.gov": ("DSH", "dsh"),
    "dgs.ca.gov": ("DGS", "dgs"),
    "calpia.ca.gov": ("CALPIA", "calpia"),
    "fire.ca.gov": ("CAL FIRE", "calfire"),
    "cdfa.ca.gov": ("CDFA", "cdfa"),
    "chp.ca.gov": ("CHP", "chp"),
    "calrecycle.ca.gov": ("CalRecycle", "calrecycle"),
    "dtsc.ca.gov": ("DTSC", "dtsc"),
    "parks.ca.gov": ("State Parks", "parks"),
    "dhcs.ca.gov": ("DHCS", "dhcs"),
}

# Form label words that are NOT real institution names — triggers fallback
_GARBAGE_NAMES = {
    "delivery", "ship", "ship to", "address", "location", "n/a", "na", "tbd",
    "none", "unknown", "other", "see below", "see above", "same", "same as above",
    "zip code", "delivery zip", "delivery zip code", "phone", "date",
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
# Legacy facility_code mapping — for backward-compat dict shape
# ═══════════════════════════════════════════════════════════════════════════════
#
# The registry uses canonical codes like "CSP-SAC", "CALVETHOME-LA",
# "DSH-Patton". The legacy `institution_resolver.resolve()` dict shape
# carried shorter codes like "SAC", "VHC-WLA", "DSH-Patton". This map
# translates registry → legacy on OUTPUT only. No facility data
# duplicated — just a code-to-code rename.

# Legacy canonical-name overrides. The registry stores some CDCR
# CSP-prefixed facilities with the short form ("CSP Corcoran", "CSP
# Los Angeles County", "CSP Solano") because that's how they're
# commonly identified. The legacy `_CDCR_FACILITIES` table carried
# the long form ("California State Prison, Corcoran") and callers/
# tests pin the long form. Map registry → legacy on OUTPUT only.
# CSP-SAC's registry canonical_name is already the long form
# ("California State Prison, Sacramento") per the audit W canonical-
# name fix, so it doesn't need an override here.
_REGISTRY_TO_LEGACY_CANONICAL_NAME = {
    "CSP-COR": "California State Prison, Corcoran",
    "CSP-LAC": "California State Prison, Los Angeles County",
    "CSP-SOL": "California State Prison, Solano",
}


_REGISTRY_TO_LEGACY_CODE = {
    # CDCR — registry "CSP-XXX" → legacy "XXX"
    "CSP-SAC": "SAC",
    "CSP-COR": "COR",
    "CSP-LAC": "LAC",
    "CSP-SOL": "SOL",
    # CalVet — registry "CALVETHOME-XX" → legacy "VHC-Xxxxxx"
    "CALVETHOME-YV": "VHC-Yountville",
    "CALVETHOME-BF": "VHC-Barstow",
    "CALVETHOME-CV": "VHC-ChulaVista",
    "CALVETHOME-LA": "VHC-WestLosAngeles",
    "CALVETHOME-FR": "VHC-Fresno",
    "CALVETHOME-RD": "VHC-Redding",
    "CALVETHOME-VM": "VHC-Ventura",
    "CALVETHOME-LC": "VHC-Lancaster",
    # DSH codes already match registry shape (DSH-Atascadero, etc.)
    # CDCR non-CSP codes (CIM, CIW, FSP, ...) pass through unchanged.
}


# Parent-agency → resolver-style lowercase agency key. CDCR-parent
# facilities map to "cchcs" because the legacy resolver classified
# every CDCR prison under the CCHCS healthcare-procurement bucket.
# (Distinct from `parent_agency` on the registry record, which is the
# dept that OWNS the facility — see test_facility_registry.py
# :test_ciw_parent_agency_is_cdcr for the registry-side contract.)
_PARENT_AGENCY_TO_LEGACY_KEY = {
    "CDCR": "cchcs",
    "CCHCS": "cchcs",
    "CalVet": "calvet",
    "DSH": "dsh",
    "DGS": "dgs",
}


def _strip_code_prefix(name: str, code: str) -> str:
    """Strip leading "CODE - " from a canonical_name, matching the
    legacy shape. Registry has both "CSP-SAC" (no prefix in name) and
    "CIW" (canonical_name has "CIW - " prefix). Old `_match_cdcr`
    always returned just the descriptive name."""
    prefix = f"{code} - "
    if name.startswith(prefix):
        return name[len(prefix):]
    return name


def _record_to_legacy_dict(rec) -> dict:
    """Map a `facility_registry.FacilityRecord` to the legacy
    institution_resolver dict shape. Returns None for None input."""
    if rec is None:
        return None
    agency = _PARENT_AGENCY_TO_LEGACY_KEY.get(
        rec.parent_agency, rec.parent_agency.lower()
    )
    code = _REGISTRY_TO_LEGACY_CODE.get(rec.code, rec.code)
    name = _REGISTRY_TO_LEGACY_CANONICAL_NAME.get(
        rec.code, _strip_code_prefix(rec.canonical_name, rec.code)
    )
    return {
        "canonical": name,
        "agency": agency,
        "facility_code": code,
    }


def _match_email_domain(email: str) -> dict:
    """Resolve institution from email domain (e.g., @cdcr.ca.gov → CDCR)."""
    if not email or "@" not in email:
        return None
    domain = email.strip().lower().split("@")[-1]
    if domain in _EMAIL_DOMAINS:
        display, agency = _EMAIL_DOMAINS[domain]
        return {"canonical": display, "agency": agency, "facility_code": ""}
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def resolve(raw_name: str, email: str = "", ship_to: str = "") -> dict:
    """Resolve a raw institution/agency name to canonical form.

    Uses a fallback chain: raw_name → ship_to address → email domain.
    If raw_name is a form label (e.g., "Delivery"), falls through to
    ship_to and email for resolution.

    Args:
        raw_name: Institution name from PDF/form.
        email: Requestor email (optional, e.g., "jane@cdcr.ca.gov").
        ship_to: Ship-to address (optional, e.g., "16756 Chino-Corona Rd...").

    Returns:
        {
            "canonical": str,      # Normalized display name
            "agency": str,         # Agency key (calvet, cchcs, dsh, dgs, etc.)
            "facility_code": str,  # Abbreviation if known (CSP, SAC, etc.)
            "original": str,       # Input as-is
            "source": str,         # How it was resolved (name/ship_to/email)
        }
    """
    _empty = {"canonical": "", "agency": "", "facility_code": "",
              "original": "", "source": ""}
    if not raw_name or not raw_name.strip():
        # No name at all — try fallbacks directly
        if ship_to:
            addr_match = _match_address(ship_to)
            if addr_match:
                return {**addr_match, "original": "", "source": "ship_to"}
        if email:
            email_match = _match_email_domain(email)
            if email_match:
                return {**email_match, "original": "", "source": "email"}
        return _empty

    original = raw_name.strip()
    text = _normalize_text(original)

    # 1. Try CDCR facility abbreviation match (CSP-SAC, CIM, CHCF, etc.)
    cdcr_match = _match_cdcr(text)
    if cdcr_match:
        return {**cdcr_match, "original": original, "source": "name"}

    # 2. Try CalVet facility match
    calvet_match = _match_calvet(text)
    if calvet_match:
        return {**calvet_match, "original": original, "source": "name"}

    # 3. Try DSH facility match
    dsh_match = _match_dsh(text)
    if dsh_match:
        return {**dsh_match, "original": original, "source": "name"}

    # 4. Try agency alias
    alias_match = _match_alias(text)
    if alias_match:
        return {**alias_match, "original": original, "source": "name"}

    # 5. Try address/zip-based resolution on the raw name itself
    addr_match = _match_address(text)
    if addr_match:
        return {**addr_match, "original": original, "source": "name"}

    # 6. raw_name didn't resolve — check if it's a garbage/form label word
    _is_garbage = text.lower().strip() in _GARBAGE_NAMES

    # 7. Try ship-to address (always, but especially if name is garbage)
    if ship_to:
        ship_match = _match_address(ship_to)
        if ship_match:
            return {**ship_match, "original": original, "source": "ship_to"}

    # 8. Try email domain
    if email:
        email_match = _match_email_domain(email)
        if email_match:
            return {**email_match, "original": original, "source": "email"}

    # 9. No match — return cleaned version of original
    return {"canonical": original, "agency": "", "facility_code": "",
            "original": original, "source": ""}


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
# Internal Matching — facades over facility_registry
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize_text(text: str) -> str:
    """Lowercase, strip punctuation, normalize whitespace."""
    text = text.lower().strip()
    text = re.sub(r'[—–\-_,.:;/\\]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def _registry_resolve(text: str):
    """Thin wrapper around `facility_registry.resolve()` that also
    tolerates inputs already passed through `_normalize_text()` (which
    strips hyphens, so "csp-sac" arrives as "csp sac"). Tries both the
    normalized form and a hyphenated reconstruction so common code-
    pattern aliases ("csp-sac", "csp-cor") still hit the alias index.
    """
    if not text:
        return None
    # Direct attempt with the input as-given.
    rec = facility_registry.resolve(text)
    if rec is not None:
        return rec
    # Some legacy callers pass `_normalize_text(...)` output where
    # "CSP-SAC" became "csp sac". Registry alias index has both
    # "csp sac" AND "csp-sac" for CSP-SAC, so the direct attempt
    # usually wins. This fallback covers the few cases where the
    # text contains the hyphenated form spelled out as separate
    # tokens that should re-join (e.g. raw-name "CSP - SAC").
    if " " in text:
        joined = text.replace(" ", "-")
        rec = facility_registry.resolve(joined)
        if rec is not None:
            return rec
    return None


def _match_cdcr(text: str) -> dict:
    """Match CDCR/CCHCS facilities via the canonical facility_registry.

    Facade over `facility_registry.resolve()` — replaces the deleted
    `_CDCR_FACILITIES` heuristic table. Preserves the legacy keyword-
    fallback behavior: when text contains "cdcr"/"cchcs"/"corrections"/
    "prison"/"state prison" but no specific facility resolves, returns
    a generic CDCR record so callers see SOMETHING is CDCR-side.

    Agency-context narrowing: when the CDCR/CCHCS keyword is present,
    a follow-on city token that the registry refuses to resolve on its
    own (e.g. bare "lancaster" is ambiguous between CSP-LAC and
    VHC-Lancaster; bare "sac" isn't a registry alias) is re-tried
    against CDCR-parent facilities only. The keyword is the
    disambiguator — without it, the registry's None is preserved.
    """
    # 1. Try canonical registry resolution.
    rec = _registry_resolve(text)
    if rec is not None and rec.parent_agency in ("CDCR", "CCHCS"):
        return _record_to_legacy_dict(rec)

    # 1b. Bare CDCR short-code (e.g. "sac" → CSP-SAC). The registry
    # stores these as "CSP-XXX" so bare 3-letter input misses the
    # alias index. Preserved per legacy behavior: bare CDCR short
    # codes are unambiguous CDCR identifiers (no non-CDCR facility
    # uses these tokens).
    stripped = text.strip()
    if " " not in stripped and stripped in _CDCR_BARE_ABBREVS:
        code = _CDCR_BARE_ABBREVS[stripped]
        narrowed = facility_registry.FACILITIES_BY_CODE.get(code)
        if narrowed is not None and narrowed.parent_agency in ("CDCR", "CCHCS"):
            return _record_to_legacy_dict(narrowed)

    # 2. Keyword fallback: text mentions CDCR/CCHCS but the registry
    # couldn't pin a facility.
    has_cdcr_kw = any(
        kw in text for kw in (
            "cdcr", "cchcs", "corrections", "correctional",
            "prison", "state prison",
        )
    )
    if has_cdcr_kw:
        # 2a. Agency-context narrow: scan text against CDCR-parent
        # facility aliases the registry refused on bare input. The
        # keyword narrows the universe — "cdcr lancaster" picks
        # CSP-LAC (not VHC-Lancaster); "cchcs sac" picks CSP-SAC.
        narrowed = _scan_for_agency_facility(text, parent_agencies=("CDCR", "CCHCS"))
        if narrowed is not None:
            return _record_to_legacy_dict(narrowed)

        # "csp" with no facility suffix → generic California State Prison
        if re.search(r"\bcsp\b", text):
            return {
                "canonical": "California State Prison",
                "agency": "cchcs",
                "facility_code": "CSP",
            }
        # Generic CDCR
        return {"canonical": "CDCR", "agency": "cchcs", "facility_code": ""}

    return None


# Legacy 3-letter CDCR abbreviations the registry intentionally
# doesn't carry as aliases (it stores them as "CSP-XXX" instead). On
# bare input these are unambiguous CDCR identifiers — no non-CDCR
# facility uses these tokens — so they resolve directly. Used both
# for bare single-token input ("SAC" → CSP-SAC) and for CDCR-keyword
# context ("CCHCS SAC" → CSP-SAC).
_CDCR_BARE_ABBREVS = {
    "sac": "CSP-SAC",
    "cor": "CSP-COR",
    "lac": "CSP-LAC",
    "sol": "CSP-SOL",
}

# City tokens the registry refuses on bare input (they collide with
# non-CDCR facilities), but CDCR keyword present narrows them in.
# DO NOT add to this map without verifying the city is unambiguous
# WITH a CDCR keyword present. "lancaster" alone is ambiguous between
# CSP-LAC and VHC-Lancaster; "cdcr lancaster" is unambiguously CSP-LAC.
_CDCR_NARROW_CITIES = {
    "lancaster": "CSP-LAC",
}


def _scan_for_agency_facility(text: str, parent_agencies):
    """Whole-word scan for CDCR-only alias tokens the registry refuses
    on bare input. Returns the FacilityRecord or None. Only used when
    the caller has already established an agency context (keyword match).
    Scans both the bare abbrev set ("sac" / "cor" / etc.) and the
    city-narrow set ("lancaster" — only valid with CDCR keyword).
    """
    for token_map in (_CDCR_BARE_ABBREVS, _CDCR_NARROW_CITIES):
        for tok, code in token_map.items():
            if re.search(r"\b" + re.escape(tok) + r"\b", text):
                rec = facility_registry.FACILITIES_BY_CODE.get(code)
                if rec is not None and rec.parent_agency in parent_agencies:
                    return rec
    return None


def _match_calvet(text: str) -> dict:
    """Match CalVet facilities via the canonical facility_registry.

    Facade over `facility_registry.resolve()` — replaces the deleted
    `_CALVET_FACILITIES` heuristic table. Preserves legacy keyword
    fallback (generic CalVet when text mentions calvet/veterans but
    no specific facility resolves). The audit's `"lancaster"` ambiguity
    is now correctly handled by the registry returning None (CSP-LAC
    and VHC-Lancaster share city + zip 93536); callers fall through.
    """
    rec = _registry_resolve(text)
    if rec is not None and rec.parent_agency == "CalVet":
        return _record_to_legacy_dict(rec)

    has_calvet_kw = any(
        kw in text for kw in (
            "calvet", "cal vet", "cva", "veterans home",
            "veterans affairs", "vhc",
        )
    )
    if has_calvet_kw:
        return {"canonical": "CalVet", "agency": "calvet", "facility_code": ""}

    return None


def _match_dsh(text: str) -> dict:
    """Match DSH facilities via the canonical facility_registry.

    Facade over `facility_registry.resolve()` — replaces the deleted
    `_DSH_FACILITIES` heuristic table. The registry's bare alias for
    "atascadero" / "patton" continues to resolve those uniquely; the
    DSH-HQ catch-all carries the agency-only fallback aliases ("dsh",
    "department of state hospitals", etc.).
    """
    rec = _registry_resolve(text)
    if rec is not None and rec.parent_agency == "DSH":
        return _record_to_legacy_dict(rec)

    has_dsh_kw = any(
        kw in text for kw in (
            "dsh", "state hospital", "department of state hospitals",
        )
    )
    if has_dsh_kw:
        return {"canonical": "DSH", "agency": "dsh", "facility_code": ""}

    return None


def _match_alias(text: str) -> dict:
    """Match simple agency aliases (CDCR, CalVet, DSH, etc.).

    PRESERVED in the 2026-05-27 collapse — the `_AGENCY_ALIASES` map
    is agency-level (not facility-level) and doesn't carry any address
    data, so it stays here rather than moving to facility_registry.
    """
    for alias, canonical in _AGENCY_ALIASES.items():
        if text == alias or text.startswith(alias + " "):
            return {
                "canonical": canonical,
                "agency": canonical.lower().split()[0],
                "facility_code": "",
            }
    return None


def _match_address(text: str) -> dict:
    """Match ship-to addresses via the canonical facility_registry.

    Facade over `facility_registry.resolve()` — replaces the deleted
    `_ADDRESS_FACILITIES` (zip table) and `_ADDRESS_KEYWORDS` (street
    keyword table) heuristic tables.

    The deleted heuristics had two correctness bugs the facade now
    closes:
      - `"prison road"` UNCONDITIONALLY mapped to CSP-SAC, but
        300 Prison Rd is FSP. Now returns None on ambiguous zip 95671;
        a caller with full street text resolves correctly through
        `facility_registry.resolve()`.
      - `"lancaster"` (via `_ADDRESS_KEYWORDS`) collided with CSP-LAC.
        Registry resolves bare "lancaster" to None (ambiguous).
    """
    rec = _registry_resolve(text)
    if rec is None:
        return None
    return _record_to_legacy_dict(rec)


# ── Backward-compatibility aliases ──────────────────────────────────────────
resolve_institution = resolve
canonical_name = normalize
