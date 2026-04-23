"""Bundle-1 PR-1a: canonical facility registry.

Audit item W (2026-04-22): FSP and CSP-SAC both carried the same
address in `quote_generator.py:FACILITY_DB`. They're different
prisons — CSP-SAC at 100 Prison Road, FSP at 300 Prison Road,
both in Represa. The quote for RFQ 10840486 shipped out with the
wrong one.

This module is the canonical source of truth so the resolver, tax
lookup, and quote generator all read from the same place.

### Regression guards
- CSP-SAC and FSP MUST have different addresses (the actual P0 fix)
- Resolver honors Mike's priority: exact > substring > zip > None
- Raw "Folsom" returns `ambiguous_substring`, not a silent guess
- "CA State Prison Sacramento" + "Folsom" co-occurring still picks
  CSP-SAC (specific alias wins over ambiguous token fallback)
- Empty / unknown input returns None + a telemetry-friendly reason
"""
from __future__ import annotations

import pytest

from src.core.facility_registry import (
    FACILITIES_BY_CODE,
    FacilityRecord,
    all_facilities,
    get,
    resolve,
    resolve_with_reason,
)


class TestAuditWAddressFix:
    """The P0 data fix: CSP-SAC and FSP must have different
    addresses. This test is the canary — it fires if anyone
    reverts the seed data to the buggy shared-address state."""

    def test_csp_sac_has_100_prison_road(self):
        csp = get("CSP-SAC")
        assert csp is not None
        assert csp.address_line1 == "100 Prison Road", (
            f"CSP-SAC line1 must be '100 Prison Road' (audit W fix), "
            f"got {csp.address_line1!r}"
        )
        assert csp.address_line2 == "Represa, CA 95671"
        assert csp.zip == "95671"

    def test_fsp_has_300_prison_road(self):
        fsp = get("FSP")
        assert fsp is not None
        assert fsp.address_line1 == "300 Prison Road", (
            f"FSP line1 must be '300 Prison Road', got {fsp.address_line1!r}"
        )
        assert fsp.address_line2 == "Represa, CA 95671"
        assert fsp.zip == "95671"

    def test_csp_sac_and_fsp_have_different_addresses(self):
        """The actual regression guard. If a future seed-data edit
        makes these match, this test breaks and tells the author
        why."""
        csp = get("CSP-SAC")
        fsp = get("FSP")
        assert csp.address_line1 != fsp.address_line1, (
            "CSP-SAC (New Folsom, 100 Prison Road) and FSP (Old "
            "Folsom, 300 Prison Road) are different prisons — their "
            "street addresses must not match. See audit W in "
            "project_2026_04_22_session_audit.md."
        )

    def test_shared_zip_is_expected(self):
        """Both prisons share the 95671 zip code (both Represa). That
        sharing is NOT a bug; only the street was wrong. Locking it
        in so a later 'fix' doesn't split the zips artificially."""
        assert get("CSP-SAC").zip == get("FSP").zip == "95671"


class TestExactLookup:
    def test_get_by_code(self):
        assert get("CIW").code == "CIW"
        assert get("CSP-SAC").code == "CSP-SAC"
        assert get("WSP").code == "WSP"

    def test_get_case_insensitive(self):
        assert get("ciw").code == "CIW"
        assert get("csp-sac").code == "CSP-SAC"

    def test_get_unknown_returns_none(self):
        assert get("NONEXISTENT") is None
        assert get("") is None
        assert get(None) is None


class TestResolvePriority:
    """The resolver's priority order per audit W fix direction:
    full name > code > zip+city > substring. Inputs that can't
    pick a unique record return None, not a silent guess."""

    def test_full_facility_name_resolves_to_code(self):
        r = resolve("CA State Prison Sacramento")
        assert r is not None and r.code == "CSP-SAC"

    def test_csp_sac_with_address_resolves(self):
        """Mike's exact RFQ 10840486 delivery string."""
        r = resolve(
            "CA State Prison Sacramento, 100 Prison Road, Folsom CA 95671"
        )
        assert r is not None
        assert r.code == "CSP-SAC", (
            f"Buyer's explicit delivery string for CSP-SAC resolved "
            f"to {r.code!r} — specific alias 'ca state prison "
            f"sacramento' must win over the ambiguous 'folsom' token"
        )

    def test_old_folsom_resolves_to_fsp(self):
        r = resolve("Folsom State Prison")
        assert r is not None and r.code == "FSP"

    def test_bare_folsom_is_ambiguous(self):
        """Audit W explicit requirement: raw 'Folsom' alone should
        NOT resolve — it's ambiguous between CSP-SAC (New Folsom)
        and FSP (Old Folsom). Operator must disambiguate."""
        r, reason = resolve_with_reason("Folsom")
        assert r is None
        assert reason == "ambiguous_substring"

    def test_wsp_facility_led_address_resolves(self):
        """Audit X follow-up: WSP facility-led addresses (like the
        one in RFQ a3056be1) resolve cleanly."""
        r = resolve(
            "WSP - Wasco State Prison, 701 Scofield Avenue, Wasco, CA 93280"
        )
        assert r is not None and r.code == "WSP"

    def test_unique_zip_resolves(self):
        """94964 belongs only to SQ — zip-only input should resolve."""
        r, reason = resolve_with_reason("something at 94964")
        # May hit substring_unique if "san quentin" slipped through;
        # but the zip path should pick SQ either way.
        assert r is not None and r.code == "SQ"

    def test_shared_zip_does_not_silently_pick(self):
        """95671 belongs to both CSP-SAC and FSP. Bare '95671' with
        no disambiguating text must return None, not a random pick."""
        r, reason = resolve_with_reason("delivery to 95671")
        assert r is None
        assert reason == "ambiguous_zip"

    def test_empty_input_returns_empty_reason(self):
        r, reason = resolve_with_reason("")
        assert r is None
        assert reason == "empty_input"

    def test_no_match_returns_no_match_reason(self):
        r, reason = resolve_with_reason("Some completely unrelated text")
        assert r is None
        assert reason == "no_match"


class TestRegistryCompleteness:
    """The registry must cover every facility the old FACILITY_DB
    in quote_generator.py carried, so downstream consumers that
    migrate from FACILITY_DB to facility_registry don't lose rows."""

    def test_all_cdcr_prisons_present(self):
        required_cdcr = (
            "CIM", "CIW", "CSP-SAC", "CSP-COR", "CSP-LAC", "CSP-SOL",
            "FSP", "SATF", "PVSP", "KVSP", "NKSP", "MCSP", "WSP",
            "SCC", "CMC", "CTF", "CCWF", "VSP", "SVSP", "PBSP",
            "CRC", "CCI", "ASP", "HDSP", "ISP", "RJD", "CAL", "CEN",
            "SQ",
        )
        present = set(FACILITIES_BY_CODE.keys())
        missing = [c for c in required_cdcr if c not in present]
        assert not missing, f"missing CDCR codes: {missing}"

    def test_all_calvet_homes_present(self):
        required_calvet = (
            "CALVETHOME-YV", "CALVETHOME-BF", "CALVETHOME-CV",
            "CALVETHOME-LA", "CALVETHOME-FR", "CALVETHOME-RD",
            "CALVETHOME-VM",
        )
        for code in required_calvet:
            assert code in FACILITIES_BY_CODE, (
                f"{code} missing from facility_registry"
            )

    def test_every_record_has_required_fields(self):
        for code, fac in FACILITIES_BY_CODE.items():
            assert fac.code, f"{code} has blank code"
            assert fac.canonical_name, f"{code} has blank canonical_name"
            assert fac.address_line1, f"{code} has blank address_line1"
            assert fac.address_line2, f"{code} has blank address_line2"
            assert fac.zip, f"{code} has blank zip"
            assert fac.parent_agency, f"{code} has blank parent_agency"
            assert fac.parent_agency_full, (
                f"{code} has blank parent_agency_full"
            )

    def test_every_zip_is_five_digits(self):
        for code, fac in FACILITIES_BY_CODE.items():
            assert len(fac.zip) == 5 and fac.zip.isdigit(), (
                f"{code} has malformed zip: {fac.zip!r}"
            )
