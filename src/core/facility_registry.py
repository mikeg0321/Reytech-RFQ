"""Bundle-1 PR-1a: canonical facility registry.

Source: audit item W in the 2026-04-22 session audit. Mike's exact
quote on RFQ 10840486:

    "Email contract says CSP - Sac clearly and the formal quote says
     Folsom State prison.....more evidence when I say email contract
     and forms are source of truth. P0 because its a blocker unless
     you want live editing of forms"

Root cause: `quote_generator.py:FACILITY_DB` had both `CSP-SAC` and
`FSP` listed at `300 Prison Road, Represa, CA 95671` — they're
different prisons. CSP-SAC (New Folsom) is at 100 Prison Road; FSP
(Old Folsom) is at 300 Prison Road. The resolver mapped the buyer's
"CA State Prison Sacramento" to CSP-SAC, but since both codes
pointed at the same wrong address, the generated quote went out
with "FSP / 300 Prison Road" stamped on the ship-to.

### What this module is
One canonical source of truth for CA government facilities. Every
future write or read — quote generation, tax lookup, institution
resolver, analytics — should eventually consume from here instead
of each module carrying its own fragment of the address table.

### What this module is NOT
Not yet a DB table. Keeping the canonical data as a Python dict
for the first ship (PR-1a) means:
  - Zero migration risk
  - The P0 wrong-prison fix goes live on the next deploy
  - Follow-on PRs (1b resolver, 1c tax, 1d quote-gen) can consume
    from here without any schema-migration coordination

A later PR can migrate to a `facility_registry` SQLite table if
admin editability / multi-tenant justifies it. The public API
(`resolve`, `get`, `all_facilities`) is designed so callers don't
care whether the storage is in-memory or DB.

### Audit W data fix
- **CSP-SAC** (CA State Prison - Sacramento aka New Folsom):
  `100 Prison Road, Represa, CA 95671` (was: `300 Prison Road`).
- **FSP** (Folsom State Prison aka Old Folsom): `300 Prison Road,
  Represa, CA 95671` (unchanged).
Both share the 95671 zip — CDTFA tax rates are identical, so the
bug was mostly silent for TAX purposes, but the SHIP-TO address on
the generated quote PDF was demonstrably wrong.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("reytech.facility_registry")


@dataclass(frozen=True)
class FacilityRecord:
    """One canonical CA facility entry.

    Fields:
      code — short identifier (CSP-SAC, FSP, CIW, ...). Used as the
        primary key everywhere. Never derived from free-text input.
      canonical_name — human-readable name as it should appear on
        quotes, invoices, analytics. Picked ONE representation per
        facility so "CSP Sacramento" and "CSP-SAC" don't show up as
        two separate facility rows in aggregates.
      address_line1 / address_line2 — street + city/state/zip. Split
        so callers can render them on their own lines without having
        to parse a joined string.
      zip — 5-digit zip for CDTFA tax lookup.
      parent_agency — the umbrella agency code (CDCR, CCHCS, CalVet).
      aliases — every known spelling of this facility that should
        resolve to this record. Case-insensitive. Includes full
        names, common misspellings, and historical references.
    """
    code: str
    canonical_name: str
    address_line1: str
    address_line2: str
    zip: str
    parent_agency: str
    parent_agency_full: str
    aliases: Tuple[str, ...] = field(default_factory=tuple)
    # Optional canonical tax rate for this facility's address. Set ONLY when
    # an operator has manually verified the rate at https://maps.cdtfa.ca.gov/
    # and the rate differs from what CDTFA's address-lookup API returns
    # (typically because the city carries a district add-on the API misses).
    # When set, `tax_resolver.resolve_tax()` returns this rate with
    # source="facility_registry" + validated=True and skips CDTFA entirely.
    # When None, behavior is unchanged: CDTFA + cache + base fallback.
    tax_rate: Optional[float] = None
    tax_jurisdiction: str = ""

    def address(self) -> List[str]:
        """Return the 2-line address as a list of strings, safe to
        splat into quote PDF renderers that expect `address` lists."""
        return [self.address_line1, self.address_line2]

    def full_name_and_code(self) -> str:
        """Display form for UI: `CSP-SAC — CA State Prison Sacramento`."""
        return f"{self.code} — {self.canonical_name}"


# ── Canonical facility data ───────────────────────────────────────────
# Seed ordering: CDCR prisons first (alphabetical by code), then CCHCS-
# tagged sites, then CalVet homes, then catch-all DEFAULT for any
# unrecognized institution so callers can always return a record.
#
# Audit W fix: CSP-SAC address corrected from "300 Prison Road" to
# "100 Prison Road". FSP remains at 300 Prison Road. Both in Represa
# (95671) — the shared zip is NOT a bug, the shared street was.

_SEED: Tuple[FacilityRecord, ...] = (
    # ═══ CDCR prisons ═══
    FacilityRecord(
        code="ASP", canonical_name="ASP - Avenal State Prison",
        address_line1="1 Kings Way", address_line2="Avenal, CA 93204",
        zip="93204", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("avenal state prison", "avenal"),
    ),
    FacilityRecord(
        code="CAL", canonical_name="CAL - Calipatria State Prison",
        address_line1="7018 Blair Rd", address_line2="Calipatria, CA 92233",
        zip="92233", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("calipatria state prison", "calipatria"),
    ),
    FacilityRecord(
        code="CCI", canonical_name="CCI - California Correctional Institution",
        address_line1="24900 Hwy 202", address_line2="Tehachapi, CA 93561",
        zip="93561", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("california correctional institution", "tehachapi"),
    ),
    FacilityRecord(
        code="CCWF", canonical_name="CCWF - Central California Women's Facility",
        address_line1="23370 Road 22", address_line2="Chowchilla, CA 93610",
        zip="93610", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("central california women's facility", "chowchilla women"),
    ),
    FacilityRecord(
        code="CEN", canonical_name="CEN - Centinela State Prison",
        address_line1="2302 Brown Rd", address_line2="Imperial, CA 92251",
        zip="92251", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("centinela state prison",),
    ),
    FacilityRecord(
        code="CHCF", canonical_name="CHCF - California Health Care Facility",
        address_line1="23370 Road 22", address_line2="Stockton, CA 95215",
        zip="95215", parent_agency="CCHCS",
        parent_agency_full="California Correctional Health Care Services",
        aliases=("california health care facility", "stockton hcf"),
    ),
    FacilityRecord(
        code="CIM", canonical_name="CIM - California Institution for Men",
        address_line1="14901 S Central Ave", address_line2="Chino, CA 91710",
        zip="91710", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("california institution for men", "chino men"),
    ),
    FacilityRecord(
        code="CIW", canonical_name="CIW - California Institution for Women",
        address_line1="16756 Chino-Corona Road", address_line2="Corona, CA 92880",
        zip="92880", parent_agency="CCHCS",
        parent_agency_full="California Correctional Health Care Services",
        aliases=("california institution for women", "corona women"),
    ),
    FacilityRecord(
        code="CMC", canonical_name="CMC - California Men's Colony",
        address_line1="Hwy 1", address_line2="San Luis Obispo, CA 93409",
        zip="93409", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("california men's colony", "san luis obispo prison"),
    ),
    FacilityRecord(
        code="CRC", canonical_name="CRC - California Rehabilitation Center",
        address_line1="5th Street & Western Ave", address_line2="Norco, CA 92860",
        zip="92860", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("california rehabilitation center", "norco prison"),
    ),
    # ─── CSP (Correctional State Prison) sites ───
    # Audit W fix is here: CSP-SAC = 100 Prison Road (New Folsom),
    # FSP = 300 Prison Road (Old Folsom). Both in Represa 95671.
    FacilityRecord(
        code="CSP-COR", canonical_name="CSP Corcoran",
        address_line1="4001 King Ave", address_line2="Corcoran, CA 93212",
        zip="93212", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("csp corcoran", "corcoran state prison"),
    ),
    FacilityRecord(
        code="CSP-LAC", canonical_name="CSP Los Angeles County",
        address_line1="44750 60th St West", address_line2="Lancaster, CA 93536",
        zip="93536", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("csp lancaster", "los angeles county state prison"),
    ),
    FacilityRecord(
        code="CSP-SAC", canonical_name="CSP Sacramento - New Folsom",
        address_line1="100 Prison Road",           # Audit W fix (was 300)
        address_line2="Represa, CA 95671",
        zip="95671", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=(
            "csp sacramento", "csp-sac", "csp sac",
            "california state prison sacramento",
            "ca state prison sacramento",
            "new folsom", "new folsom prison", "new folsom state prison",
            "csp-sacramento", "sacramento state prison",
        ),
    ),
    FacilityRecord(
        code="CSP-SOL", canonical_name="CSP Solano",
        address_line1="2100 Peabody Road", address_line2="Vacaville, CA 95687",
        zip="95687", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("csp solano", "solano state prison"),
    ),
    FacilityRecord(
        code="CTF", canonical_name="CTF - Correctional Training Facility",
        address_line1="Hwy 101 North", address_line2="Soledad, CA 93960",
        zip="93960", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("correctional training facility", "soledad ctf"),
    ),
    # FSP = Old Folsom State Prison at 300 Prison Road
    FacilityRecord(
        code="FSP", canonical_name="FSP - Folsom State Prison",
        address_line1="300 Prison Road",
        address_line2="Represa, CA 95671",
        zip="95671", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=(
            "folsom state prison", "old folsom", "old folsom state prison",
            "folsom prison",
        ),
    ),
    FacilityRecord(
        code="HDSP", canonical_name="HDSP - High Desert State Prison",
        address_line1="475-750 Rice Canyon Rd", address_line2="Susanville, CA 96127",
        zip="96127", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("high desert state prison", "susanville"),
    ),
    FacilityRecord(
        code="ISP", canonical_name="ISP - Ironwood State Prison",
        address_line1="19005 Wiley's Well Rd", address_line2="Blythe, CA 92225",
        zip="92225", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("ironwood state prison", "blythe prison"),
    ),
    FacilityRecord(
        code="KVSP", canonical_name="KVSP - Kern Valley State Prison",
        address_line1="3000 W Cecil Ave", address_line2="Delano, CA 93215",
        zip="93215", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("kern valley state prison", "delano kern"),
    ),
    FacilityRecord(
        code="MCSP", canonical_name="MCSP - Mule Creek State Prison",
        address_line1="4001 Hwy 104", address_line2="Ione, CA 95640",
        zip="95640", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("mule creek state prison", "ione prison"),
    ),
    FacilityRecord(
        code="NKSP", canonical_name="NKSP - North Kern State Prison",
        address_line1="2737 W Cecil Ave", address_line2="Delano, CA 93215",
        zip="93215", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("north kern state prison", "delano north"),
    ),
    FacilityRecord(
        code="PBSP", canonical_name="PBSP - Pelican Bay State Prison",
        address_line1="5905 Lake Earl Dr", address_line2="Crescent City, CA 95531",
        zip="95531", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("pelican bay state prison", "crescent city prison"),
    ),
    FacilityRecord(
        code="PVSP", canonical_name="PVSP - Pleasant Valley State Prison",
        address_line1="24863 W Jayne Ave", address_line2="Coalinga, CA 93210",
        zip="93210", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("pleasant valley state prison", "coalinga"),
    ),
    FacilityRecord(
        code="RJD", canonical_name="RJD - Richard J. Donovan Correctional Facility",
        address_line1="480 Alta Road", address_line2="San Diego, CA 92179",
        zip="92179", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("richard j. donovan", "donovan correctional", "san diego prison"),
    ),
    FacilityRecord(
        code="SATF", canonical_name="SATF - Substance Abuse Treatment Facility",
        address_line1="900 Quebec Ave", address_line2="Corcoran, CA 93212",
        zip="93212", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("substance abuse treatment facility", "corcoran satf"),
    ),
    FacilityRecord(
        code="SCC", canonical_name="SCC - Sierra Conservation Center",
        address_line1="5100 O'Byrnes Ferry Road", address_line2="Jamestown, CA 95327",
        zip="95327", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("sierra conservation center", "jamestown"),
    ),
    FacilityRecord(
        code="SQ", canonical_name="SQ - San Quentin State Prison",
        address_line1="Main Street", address_line2="San Quentin, CA 94964",
        zip="94964", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("san quentin state prison", "san quentin", "sqsp"),
    ),
    FacilityRecord(
        code="SVSP", canonical_name="SVSP - Salinas Valley State Prison",
        address_line1="31625 Hwy 101", address_line2="Soledad, CA 93960",
        zip="93960", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("salinas valley state prison", "soledad svsp"),
    ),
    FacilityRecord(
        code="VSP", canonical_name="VSP - Valley State Prison",
        address_line1="21633 Avenue 24", address_line2="Chowchilla, CA 93610",
        zip="93610", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("valley state prison", "chowchilla vsp"),
    ),
    FacilityRecord(
        code="WSP", canonical_name="WSP - Wasco State Prison",
        address_line1="701 Scofield Ave", address_line2="Wasco, CA 93280",
        zip="93280", parent_agency="CDCR",
        parent_agency_full="Dept. of Corrections and Rehabilitation",
        aliases=("wasco state prison", "wasco"),
    ),
    # ═══ CalVet Veterans Homes ═══
    FacilityRecord(
        code="CALVETHOME-YV",
        canonical_name="Veterans Home of California - Yountville",
        address_line1="190 California Dr", address_line2="Yountville, CA 94599",
        zip="94599", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        # Bare "yountville" added as a unique-city alias — the only
        # facility in our registry in Yountville. Lets a buyer email
        # like "California Department of Veterans Affairs - Yountville
        # Division" resolve cleanly via substring match without
        # requiring the multi-word alias to span " - " punctuation.
        aliases=("veterans home yountville", "yountville veterans", "yountville"),
    ),
    FacilityRecord(
        code="CALVETHOME-BF",
        canonical_name="Veterans Home of California - Barstow",
        address_line1="100 E Veterans Pkwy", address_line2="Barstow, CA 92311",
        zip="92311", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        # Bare "barstow" alias: incident 2026-04-24 — Mike's PC
        # f81c4e9b had ship-to text "California Department of Veterans
        # Affairs - Barstow Division, Skilled Nursing Unit" which the
        # 2-word "barstow veterans" alias couldn't match (" - " breaks
        # contiguity). Bare "barstow" is the unique-city identifier;
        # the only Barstow facility in our registry is this one.
        aliases=("veterans home barstow", "barstow veterans", "barstow"),
        # Manual override: CDTFA address-lookup API returns 7.250% (CA base)
        # for 92311 because the Barstow district add-on isn't in its zip table.
        # Verified at https://maps.cdtfa.ca.gov/ on 2026-04-23 — actual combined
        # rate for 100 Veterans Pkwy, Barstow, CA 92311 is 8.750% (BARSTOW
        # jurisdiction, San Bernardino County).
        tax_rate=0.0875, tax_jurisdiction="BARSTOW",
    ),
    FacilityRecord(
        code="CALVETHOME-CV",
        canonical_name="Veterans Home of California - Chula Vista",
        address_line1="700 E Naples Ct", address_line2="Chula Vista, CA 91911",
        zip="91911", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        aliases=("veterans home chula vista", "chula vista veterans",
                 "chula vista"),
    ),
    FacilityRecord(
        code="CALVETHOME-LA",
        canonical_name="Veterans Home of California - West Los Angeles",
        address_line1="11500 Nimitz Ave Bldg 209",
        address_line2="Los Angeles, CA 90049",
        zip="90049", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        # Bare "los angeles" intentionally omitted — too broad, would
        # match many non-veteran contexts. "west los angeles" + "west la"
        # are precise enough.
        aliases=("veterans home west los angeles", "west la veterans",
                 "west los angeles"),
    ),
    FacilityRecord(
        code="CALVETHOME-FR",
        canonical_name="Veterans Home of California - Fresno",
        address_line1="2811 W California Ave", address_line2="Fresno, CA 93706",
        zip="93706", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        # Bare "fresno" included — only Fresno facility in our registry.
        # Reytech's CA-only scope means "Fresno" in a buyer ship-to
        # reliably means this facility.
        aliases=("veterans home fresno", "fresno veterans", "fresno"),
    ),
    FacilityRecord(
        code="CALVETHOME-RD",
        canonical_name="Veterans Home of California - Redding",
        address_line1="3400 Knighton Rd", address_line2="Redding, CA 96002",
        zip="96002", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        aliases=("veterans home redding", "redding veterans", "redding"),
    ),
    FacilityRecord(
        code="CALVETHOME-VM",
        canonical_name="Veterans Home of California - Ventura",
        address_line1="10900 Telephone Rd", address_line2="Ventura, CA 93004",
        zip="93004", parent_agency="CalVet",
        parent_agency_full="California Department of Veterans Affairs",
        aliases=("veterans home ventura", "ventura veterans", "ventura"),
    ),
)


# Public lookup maps, built once at import time.
FACILITIES_BY_CODE: Dict[str, FacilityRecord] = {
    f.code: f for f in _SEED
}
_ALIAS_INDEX: Dict[str, FacilityRecord] = {}
for _f in _SEED:
    _ALIAS_INDEX[_f.code.lower()] = _f
    _ALIAS_INDEX[_f.canonical_name.lower()] = _f
    for _a in _f.aliases:
        _ALIAS_INDEX[_a.lower()] = _f

# Zip index. A zip with multiple facilities returns None from
# `resolve_by_zip` — callers must disambiguate on city/name.
_ZIP_INDEX: Dict[str, List[FacilityRecord]] = {}
for _f in _SEED:
    _ZIP_INDEX.setdefault(_f.zip, []).append(_f)

# Tokens that by themselves could refer to more than one facility —
# the resolver must return `ambiguous_substring` for these, even when
# they don't appear in any specific alias list. Mike's example from
# audit W: bare "Folsom" could mean CSP-SAC (New Folsom) or FSP
# (Old Folsom), so never guess a single code from just "Folsom".
_AMBIGUOUS_TOKENS: Dict[str, Tuple[FacilityRecord, ...]] = {
    "folsom": (FACILITIES_BY_CODE["CSP-SAC"], FACILITIES_BY_CODE["FSP"]),
}


# ── Public API ───────────────────────────────────────────────────────

def get(code: str) -> Optional[FacilityRecord]:
    """Exact lookup by canonical code. Returns None if code is unknown."""
    if not code:
        return None
    return FACILITIES_BY_CODE.get(code.strip().upper())


def all_facilities() -> List[FacilityRecord]:
    """Enumerate every canonical facility in registry order (seed
    insertion order)."""
    return list(_SEED)


def _substring_candidates(text_lower: str) -> set:
    """Enumerate candidate facilities for a free-text input.

    Two-pass: SPECIFIC aliases win over ambiguous short tokens. If
    `text_lower` includes "ca state prison sacramento, ..., folsom",
    the specific alias "ca state prison sacramento" resolves to
    {CSP-SAC} and we stop — even though bare "folsom" would also
    pull in FSP via `_AMBIGUOUS_TOKENS`. The ambiguous-token list
    is the fallback for inputs that don't name any specific facility.

    Whole-token matching uses `\\b` so "cal" doesn't accidentally
    match inside "calvet".
    """
    # Pass 1: specific aliases / codes / canonical names
    specific: set = set()
    for key, fac in _ALIAS_INDEX.items():
        if re.search(r"\b" + re.escape(key) + r"\b", text_lower):
            specific.add(fac)
    if specific:
        return specific

    # Pass 2 (fallback): ambiguous short tokens like "folsom"
    ambiguous: set = set()
    for tok, facs in _AMBIGUOUS_TOKENS.items():
        if re.search(r"\b" + re.escape(tok) + r"\b", text_lower):
            for f in facs:
                ambiguous.add(f)
    return ambiguous


def _substring_match(text_lower: str) -> Optional[FacilityRecord]:
    """Return the UNIQUE facility from substring candidates, or None
    on zero matches OR multiple candidate matches. Ambiguity must
    never silently resolve to a single guess."""
    candidates = _substring_candidates(text_lower)
    if len(candidates) == 1:
        return next(iter(candidates))
    return None


def resolve(text: str) -> Optional[FacilityRecord]:
    """Resolve free-text to a canonical facility record.

    Priority order (per audit item W fix direction):
      1. Exact alias / canonical name / code match
      2. Substring match — but ONLY if unambiguous (single candidate)
      3. Zip match — but ONLY if zip maps to exactly one facility
      4. None → caller must prompt operator or abort

    Never silently guesses when input is ambiguous. Raw "Folsom" or
    "Folsom State Prison" could be CSP-SAC (New Folsom) or FSP (Old
    Folsom) — returns None, not a random pick.
    """
    if not text:
        return None
    s = text.strip()
    if not s:
        return None

    # 1. Exact alias / code / canonical-name match (case-insensitive)
    exact = _ALIAS_INDEX.get(s.lower())
    if exact:
        return exact

    # 2. Unique whole-word substring match
    sub = _substring_match(s.lower())
    if sub:
        return sub

    # 3. Zip match — only when unambiguous. Two-zip facilities (e.g.
    # Corcoran CSP-COR + SATF both in 93212) return None so the
    # caller doesn't ship-to the wrong one.
    zip_matches = re.findall(r"\b(\d{5})\b", s)
    for z in zip_matches:
        entries = _ZIP_INDEX.get(z) or []
        if len(entries) == 1:
            return entries[0]

    return None


# ── Agency-key → canonical-facility map ───────────────────────────────
# Some `agency_key` values from `agency_config.py` are facility-specific
# (e.g. `calvet_barstow` always means the Veterans Home of California -
# Barstow facility). When the converter / agency_config has already
# resolved an RFQ to one of these keys, the quote generator MUST use
# this mapping as the authoritative ship-to source — text-based
# `resolve(...)` over stale buyer fields was the 2026-04-24 root cause
# of the f81c4e9b → Calipatria mis-render. PR #501 collapsed
# `quote_generator.FACILITY_DB` onto this registry; this map is the
# Fix-B follow-up that flips the priority order from text-first to
# agency-key-first inside `generate_quote_from_rfq`.
#
# Keep this map narrow — only facility-specific keys belong here.
# Generic agency keys with multiple facilities (cdcr, calvet, cchcs,
# dgs, calfire, other) need text resolution to pick the child facility.
AGENCY_KEY_TO_FACILITY_CODE: Dict[str, str] = {
    "calvet_barstow": "CALVETHOME-BF",
    # Future: add facility-specific keys here as they're introduced.
    # E.g. each calvet_* facility variant, each dsh_* hospital variant.
}


def resolve_by_agency_key(agency_key: str) -> Optional[FacilityRecord]:
    """Resolve an `agency_key` to its canonical FacilityRecord, when
    the key is facility-specific. Returns None for generic agency keys
    (cdcr/calvet/cchcs/dgs/calfire/other) — those need text resolution
    to pick the child facility, since multiple facilities share the
    parent agency.

    Used by `quote_generator.generate_quote_from_rfq` to honour the
    converter-resolved canonical agency_key BEFORE text-based
    facility lookup. Per `feedback_canonical_not_verbatim` — when a
    canonical id is present, do not let buyer free-text override it.
    """
    if not agency_key:
        return None
    code = AGENCY_KEY_TO_FACILITY_CODE.get(agency_key.strip().lower())
    if not code:
        return None
    return FACILITIES_BY_CODE.get(code)


def resolve_with_reason(text: str) -> Tuple[Optional[FacilityRecord], str]:
    """Debug-friendly variant: returns (record, reason_slug).

    Reason slugs:
      - `exact` — code / alias / canonical match hit
      - `substring_unique` — unambiguous substring
      - `zip_unique` — zip matched exactly one facility
      - `ambiguous_substring` — multiple facilities match text
      - `ambiguous_zip` — zip maps to multiple facilities
      - `no_match` — nothing in registry matches
      - `empty_input` — caller passed empty string

    Useful for telemetry + the operator-disambiguate UI in PR-1b.
    """
    if not text or not text.strip():
        return None, "empty_input"
    s = text.strip()

    if s.lower() in _ALIAS_INDEX:
        return _ALIAS_INDEX[s.lower()], "exact"

    # Substring — check for ambiguity vs uniqueness
    candidates = _substring_candidates(s.lower())
    if len(candidates) == 1:
        return next(iter(candidates)), "substring_unique"
    if len(candidates) > 1:
        return None, "ambiguous_substring"

    # Zip
    zip_matches = re.findall(r"\b(\d{5})\b", s)
    for z in zip_matches:
        entries = _ZIP_INDEX.get(z) or []
        if len(entries) == 1:
            return entries[0], "zip_unique"
        if len(entries) > 1:
            return None, "ambiguous_zip"

    return None, "no_match"
