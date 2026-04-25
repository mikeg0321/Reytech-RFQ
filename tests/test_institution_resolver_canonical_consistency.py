"""Cross-source consistency ratchet — institution_resolver vs facility_registry.

## The rule

Every facility code in `institution_resolver._FACILITY_ADDRESSES` MUST map (via
the translation table below) to a canonical `FacilityRecord` in
`facility_registry`, and the two sources MUST agree on city + zip.

## Why this exists

The 2026-04-22 audit (item W) exposed that quote_generator carried its own
duplicate FACILITY_DB that diverged from `facility_registry`. PR #501 collapsed
that duplicate. PR #503/#507/#516 enforced the architectural ratchet that
prevents new renderers from importing the canonical resolvers directly.

But `institution_resolver` ITSELF still owns 5 separate dicts (`_CDCR_FACILITIES`,
`_CALVET_FACILITIES`, `_DSH_FACILITIES`, `_FACILITY_ADDRESSES`, `_AGENCY_ALIASES`)
that pre-date the canonical registry. Until those are folded into
`facility_registry` (long-running cleanup), this test is the guard rail that
prevents the two sources from drifting apart in subtle ways:

- A new institution_resolver address landing in a different zip than canonical
  → quote PDF address line wrong but tax lookup uses canonical → silent bug.
- A new canonical facility added without a matching institution_resolver entry
  → ship-to lookup falls back to empty string → operator types it manually
  → free-text leak past canonical layer.

## What this test does NOT enforce

- Street address text equality. "100 E Veterans Pkwy" vs "100 East Veterans
  Parkway" are the same street; we don't want this test to fail on
  abbreviation style. Zip + city is the unambiguous identity.
- Field-by-field migration. The long-term goal is to delete
  `_FACILITY_ADDRESSES` once all callers route through canonical; this test
  is the safety net during the migration, not the migration itself.
"""
from __future__ import annotations

import re

import pytest

from src.core.facility_registry import FACILITIES_BY_CODE


# institution_resolver code → canonical facility_registry code.
# Identity entries (e.g. ASP→ASP, DSH-Patton→DSH-Patton) are still listed
# explicitly so the table doubles as the complete migration map and any
# omission shows up as a clear test failure rather than a silent drop-through.
CODE_TRANSLATION = {
    # CDCR — most codes match canonical 1:1
    "ASP": "ASP", "CAL": "CAL", "CCC": "CCC", "CCI": "CCI",
    "CCWF": "CCWF", "CEN": "CEN", "CHCF": "CHCF", "CIM": "CIM",
    "CIW": "CIW", "CMC": "CMC", "CMF": "CMF", "CRC": "CRC", "CTF": "CTF",
    "CVSP": "CVSP", "DVI": "DVI", "FSP": "FSP", "HDSP": "HDSP",
    "ISP": "ISP", "KVSP": "KVSP", "MCSP": "MCSP", "NKSP": "NKSP",
    "PBSP": "PBSP", "PVSP": "PVSP", "RJD": "RJD", "SATF": "SATF",
    "SCC": "SCC", "SQ": "SQ", "SVSP": "SVSP", "VSP": "VSP",
    "WSP": "WSP",
    # CDCR — institution_resolver uses short codes, canonical uses CSP- prefix
    "COR": "CSP-COR",
    "LAC": "CSP-LAC",
    "SAC": "CSP-SAC",
    "SOL": "CSP-SOL",
    # CalVet — institution_resolver uses VHC-City, canonical uses CALVETHOME-XX
    "VHC-Yountville":  "CALVETHOME-YV",
    "VHC-Barstow":     "CALVETHOME-BF",
    "VHC-ChulaVista":  "CALVETHOME-CV",
    "VHC-Fresno":      "CALVETHOME-FR",
    "VHC-Lancaster":   "CALVETHOME-LC",
    "VHC-Ventura":     "CALVETHOME-VM",
    "VHC-WLA":         "CALVETHOME-LA",
    "VHC-Redding":     "CALVETHOME-RD",
    # DSH — codes are identical
    "DSH-Atascadero":   "DSH-Atascadero",
    "DSH-Coalinga":     "DSH-Coalinga",
    "DSH-Metropolitan": "DSH-Metropolitan",
    "DSH-Napa":         "DSH-Napa",
    "DSH-Patton":       "DSH-Patton",
}


def _extract_city_zip(address: str) -> tuple:
    """Pull (city, zip) from a free-form address string.

    Accepts both styles seen in the codebase:
      - "100 E Veterans Pkwy, Barstow, CA 92311"
      - "San Quentin State Prison, San Quentin, CA 94964"
      - "100 E Veterans Pkwy" (line1) + "Barstow, CA 92311" (line2)

    Returns (city_lower, zip_5digit) or ("", "") on parse failure. The city is
    lowercased + stripped to make the comparison resilient to "Norwalk" vs
    "norwalk" etc. without weakening the identity check.
    """
    if not address:
        return ("", "")
    # Find the trailing "City, CA NNNNN" anchor.
    m = re.search(r"([A-Za-z][A-Za-z .'\-]+),\s*CA\s+(\d{5})\b", address)
    if not m:
        return ("", "")
    city = m.group(1).strip().lower()
    zip_code = m.group(2)
    return (city, zip_code)


def _canonical_city_zip(record) -> tuple:
    """Same extraction, run on the canonical FacilityRecord's address_line2."""
    return _extract_city_zip(record.address_line2)


# ── Tests ────────────────────────────────────────────────────────────


def test_every_institution_resolver_address_has_canonical_translation():
    """Every facility_code that institution_resolver knows must have an
    entry in `CODE_TRANSLATION` that points at a real canonical record.

    If a future PR adds a new code to `_FACILITY_ADDRESSES`, this test fires
    until the new code is either translated or added to the canonical seed.
    """
    from src.core.institution_resolver import _FACILITY_ADDRESSES
    untranslated = []
    for code in _FACILITY_ADDRESSES.keys():
        if code not in CODE_TRANSLATION:
            untranslated.append(code)
    assert not untranslated, (
        "institution_resolver._FACILITY_ADDRESSES has codes with no "
        "translation to canonical facility_registry. Either add the new "
        "code to `CODE_TRANSLATION` in this file (and a matching "
        "FacilityRecord to facility_registry._SEED), or delete the "
        "institution_resolver entry. Codes: " + repr(untranslated)
    )


def test_every_translation_target_exists_in_canonical():
    """`CODE_TRANSLATION` values are PROMISES that those canonical codes
    exist. If the canonical seed gets edited and a code disappears,
    this test surfaces the broken promise."""
    missing = []
    for src_code, canonical_code in CODE_TRANSLATION.items():
        if canonical_code not in FACILITIES_BY_CODE:
            missing.append((src_code, canonical_code))
    assert not missing, (
        "Translation table points at canonical codes that don't exist "
        "in facility_registry._SEED. Either add the FacilityRecord or "
        "remove the translation entry. Pairs: " + repr(missing)
    )


def test_translated_addresses_agree_on_city_and_zip():
    """The substantive consistency check: for every translated code, the
    (city, zip) extracted from institution_resolver's address must match
    the (city, zip) on the canonical FacilityRecord.

    Street-text differences ("Pkwy" vs "Parkway", "Rd" vs "Road") are
    tolerated — we'd rather not have flake on abbreviation style. But
    city + zip identity is unambiguous and catches the divergence pattern
    that caused the Calipatria-vs-Barstow regression.
    """
    from src.core.institution_resolver import _FACILITY_ADDRESSES
    mismatches = []
    for src_code, address in _FACILITY_ADDRESSES.items():
        canonical_code = CODE_TRANSLATION.get(src_code)
        if not canonical_code:
            continue  # caught by translation-existence test above
        canonical = FACILITIES_BY_CODE.get(canonical_code)
        if canonical is None:
            continue  # caught by translation-target test above
        ir_city, ir_zip = _extract_city_zip(address)
        ca_city, ca_zip = _canonical_city_zip(canonical)
        if ir_zip and ca_zip and ir_zip != ca_zip:
            mismatches.append(
                f"  {src_code} → {canonical_code}: zip {ir_zip!r} (IR) vs "
                f"{ca_zip!r} (canonical)"
            )
            continue
        if ir_city and ca_city and ir_city != ca_city:
            mismatches.append(
                f"  {src_code} → {canonical_code}: city {ir_city!r} (IR) "
                f"vs {ca_city!r} (canonical)"
            )
    assert not mismatches, (
        "institution_resolver._FACILITY_ADDRESSES disagrees with "
        "facility_registry on city/zip — pick one source of truth and "
        "fix the other. Mismatches:\n" + "\n".join(mismatches)
    )


def test_no_canonical_facility_orphaned_from_institution_resolver():
    """The reverse check: every canonical facility should be reachable
    via institution_resolver too, otherwise ship-to lookups for it fall
    through to empty string. New canonical facilities MUST be added to
    institution_resolver._FACILITY_ADDRESSES at the same time, until the
    long-term consolidation deletes _FACILITY_ADDRESSES entirely.
    """
    from src.core.institution_resolver import _FACILITY_ADDRESSES
    reverse = {v: k for k, v in CODE_TRANSLATION.items()}
    orphans = []
    for canonical_code in FACILITIES_BY_CODE.keys():
        ir_code = reverse.get(canonical_code)
        if ir_code is None or ir_code not in _FACILITY_ADDRESSES:
            orphans.append(canonical_code)
    assert not orphans, (
        "Canonical facility/facilities have no matching entry in "
        "institution_resolver._FACILITY_ADDRESSES — ship-to lookup "
        "via the resolver will return empty string for these. Add the "
        "address there too (and to CODE_TRANSLATION in this file). "
        "Orphans: " + repr(sorted(orphans))
    )


def test_translation_table_has_no_stale_entries():
    """If institution_resolver removes a code, the translation entry
    becomes dead weight. Surface it so the dead row gets pruned in the
    same PR rather than living on as confusing scaffolding."""
    from src.core.institution_resolver import _FACILITY_ADDRESSES
    dead = []
    for src_code in CODE_TRANSLATION.keys():
        if src_code not in _FACILITY_ADDRESSES:
            dead.append(src_code)
    assert not dead, (
        "CODE_TRANSLATION has rows for institution_resolver codes that "
        "no longer exist. Delete them: " + repr(dead)
    )
