"""Tests for agency_config.py — agency matching, required forms, fallback behavior.

This module caused a production incident when logging was added without
import logging, crashing match_agency() and falling to wrong agency.
"""
import pytest


def _import_agency_config():
    """Import agency_config, handling path issues."""
    import sys, os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    from src.core.agency_config import (
        match_agency, load_agency_configs, extract_required_forms_from_text,
        DEFAULT_AGENCY_CONFIGS, AVAILABLE_FORMS, FORM_TEXT_PATTERNS,
    )
    return match_agency, load_agency_configs, extract_required_forms_from_text, DEFAULT_AGENCY_CONFIGS


# ── Agency Matching ───────────────────────────────────────────────────────────

class TestMatchAgency:
    """match_agency() returns (key, config) based on RFQ data."""

    def test_cchcs_by_institution_falls_to_other(self):
        # Post-§0 Job #1 (2026-05-27): DEFAULT_AGENCY_CONFIGS["cchcs"] DELETED.
        # CCHCS routes through the Spine — the legacy match_agency() path
        # no longer resolves CCHCS-class inputs to a CCHCS-named entry.
        # It falls through to the documented "other" fallback.
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"institution": "California Health Care Facility"})
        assert key == "other"

    def test_cdcr_by_agency_falls_to_other(self):
        # Post-deletion: CDCR no longer routes through legacy cchcs config.
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"agency": "CDCR"})
        assert key == "other"

    def test_calvet_by_email(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"requestor_email": "buyer@calvet.ca.gov"})
        assert key == "calvet"

    def test_calvet_barstow_specific(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"institution": "Barstow Veterans Home"})
        assert key == "calvet_barstow"
        assert "barstow_cuf" in cfg["required_forms"]

    def test_prison_keywords_fall_to_other(self):
        # Post-deletion: prison-domain keywords no longer resolve to a
        # CCHCS-named entry via legacy match_agency. "STATE PRISON" is
        # still a CDCR parent strong-pattern, but with no CCHCS child
        # config left in DEFAULT_AGENCY_CONFIGS the parent-default
        # branch falls through to "other".
        match, _, _, _ = _import_agency_config()
        for keyword in ["CIM", "STATE PRISON", "CORRECTIONAL"]:
            key, cfg = match({"institution": keyword})
            assert key == "other", f"Expected 'other' fallback for keyword {keyword!r}, got {key!r}"

    def test_unknown_falls_to_other(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"agency": "Unknown Agency XYZ"})
        assert key == "other"

    def test_empty_rfq_data_no_crash(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({})
        assert key == "other"
        assert isinstance(cfg, dict)

    def test_none_values_no_crash(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"agency": None, "institution": None, "requestor_email": None})
        assert key == "other"

    def test_matched_by_field_present(self):
        # Use a surviving agency (calvet) since CCHCS no longer matches.
        # Mechanism under test: a successful match copies the cfg and stamps
        # `matched_by`. The "other" fallback path is bare and does NOT stamp
        # the field — only pattern/domain/buyer-history matches do.
        match, _, _, _ = _import_agency_config()
        _, cfg = match({"agency": "CALVET"})
        assert "matched_by" in cfg

    def test_case_insensitive_matching(self):
        # Same input in different cases must produce the same key. CCHCS
        # no longer resolves to a CCHCS-named entry, but both casings still
        # land on the same "other" fallback — preserving the property.
        match, _, _, _ = _import_agency_config()
        key1, _ = match({"agency": "cchcs"})
        key2, _ = match({"agency": "CCHCS"})
        assert key1 == key2 == "other"


# ── Required Forms ────────────────────────────────────────────────────────────

class TestRequiredForms:
    """Each agency config must have required_forms that are valid form IDs."""

    def test_all_configs_have_required_forms(self):
        _, load, _, defaults = _import_agency_config()
        configs = load()
        for key, cfg in configs.items():
            assert "required_forms" in cfg, f"Agency '{key}' missing required_forms"
            assert isinstance(cfg["required_forms"], list), f"Agency '{key}' required_forms is not a list"

    # NOTE: test_cchcs_package_is_minimal DELETED per §0 Job #1 acceptance
    # 2026-05-27. The CCHCS legacy form-list contract moved to the Spine;
    # this test pinned a now-deleted DEFAULT_AGENCY_CONFIGS["cchcs"] entry.

    def test_calvet_has_no_bidpkg(self):
        """CalVet uses individual compliance forms, not a bid package."""
        _, load, _, _ = _import_agency_config()
        cfg = load()["calvet"]
        forms = cfg["required_forms"]
        assert "bidpkg" not in forms
        assert "quote" in forms

    def test_form_ids_are_valid(self):
        """All form IDs in configs must be in AVAILABLE_FORMS."""
        from src.core.agency_config import AVAILABLE_FORMS
        _, load, _, _ = _import_agency_config()
        valid_ids = {f["id"] for f in AVAILABLE_FORMS}
        configs = load()
        for key, cfg in configs.items():
            for fid in cfg.get("required_forms", []) + cfg.get("optional_forms", []):
                assert fid in valid_ids, f"Agency '{key}' has unknown form ID '{fid}'"


# ── Form Detection from Text ─────────────────────────────────────────────────

class TestDetectRequiredForms:
    """detect_required_forms() scans text for form keywords."""

    def test_detects_std204(self):
        _, _, extract, _ = _import_agency_config()
        result = extract("Please include STD 204 Payee Data Record")
        assert "std204" in result["forms"]

    def test_detects_multiple_forms(self):
        _, _, extract, _ = _import_agency_config()
        result = extract("Submit STD 204, DVBE 843, and Darfur Act certification")
        assert "std204" in result["forms"]
        assert "dvbe843" in result["forms"]
        assert "darfur_act" in result["forms"]

    def test_empty_text_returns_empty(self):
        _, _, extract, _ = _import_agency_config()
        result = extract("")
        assert len(result["forms"]) == 0

    def test_case_insensitive_detection(self):
        _, _, extract, _ = _import_agency_config()
        result = extract("please include calrecycle form")
        assert "calrecycle74" in result["forms"]


# ── Edge Cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_load_configs_returns_dict(self):
        _, load, _, _ = _import_agency_config()
        configs = load()
        assert isinstance(configs, dict)
        assert len(configs) >= 3  # at least cchcs, calvet, other

    def test_other_config_exists(self):
        _, load, _, _ = _import_agency_config()
        configs = load()
        assert "other" in configs
        assert "required_forms" in configs["other"]


# ── CDCR ↔ DSH PARENT/CHILD HIERARCHY (2026-05-25) ──────────────────────────
#
# Mike's 2026-05-25 directive: CCHCS and DSH are sibling child agencies under
# a CDCR-family parent. They share buyers + facility locations (Coalinga has
# both PVSP/CCHCS AND Coalinga State Hospital/DSH) but each carries its OWN
# addresses, processes, forms, bill-to.
#
# Pre-fix bug: PVSP (Pleasant Valley State Prison, CCHCS) in COALINGA, CA
# matched DSH because the legacy resolver iterated [calvet_barstow, dsh]
# BEFORE checking cchcs, and DSH's match_patterns included "COALINGA". The
# Fill Plan rendered "DSH — State Hospitals" with NO PROFILE for CCHCS Bid
# Package — a structural send blocker for every CCHCS quote shipping to PVSP.
#
# The substrate fix (PARENT_AGENCIES registry + OVERLAP_PATTERNS + scoped
# child matching): patterns ambiguous across parents (COALINGA) never fire
# without a parent signal.

class TestCdcrDshHierarchy:
    """Two-tier resolver: parent → scoped child. No overlap pattern fires
    without parent context."""

    def test_pvsp_coalinga_does_not_match_dsh(self):
        """THE bug. PVSP in Coalinga, CCHCS buyer at cdcr.ca.gov.
        Pre-fix returned 'dsh' because 'COALINGA' is in DSH's patterns
        and DSH was prioritized over cchcs in the legacy loop.

        Post-§0 Job #1 (2026-05-27): DEFAULT_AGENCY_CONFIGS["cchcs"] is DELETED.
        The anti-regression invariant the COALINGA-overlap fix delivered
        STILL HOLDS — a CDCR-domain sender must NOT be mis-routed to DSH.
        The new fallback is 'other' (legacy substrate no longer routes
        CCHCS; the Spine does)."""
        match, _, _, _ = _import_agency_config()
        key, cfg = match({
            "agency": "cchcs",
            "requestor_email": "Mohammad.Chechi@cdcr.ca.gov",
            "institution": "cchcs",
            "ship_to": "Pleasant Valley State Prison, 24863 West Jayne Avenue, Coalinga, CA 93210",
            "solicitation_number": "10846357",
        })
        assert key != "dsh", (
            f"PVSP/Coalinga CDCR-domain quote MUST NOT route to DSH; got {key!r}. "
            "If this fails, the COALINGA-as-DSH overlap regressed."
        )
        assert key == "other"

    def test_bare_coalinga_without_parent_falls_to_other(self):
        """No parent signal + only the ambiguous 'COALINGA' token in text
        must not fire DSH. Returns 'other' instead — operator-visible
        prompt to clarify which org."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "institution": "unclear facility",
            "ship_to": "somewhere in Coalinga",
        })
        assert key == "other", (
            f"Bare 'Coalinga' without parent should be ambiguous → other; got {key!r}"
        )

    def test_dsh_coalinga_with_dsh_domain_resolves_dsh(self):
        """Coalinga State Hospital with an @dsh.ca.gov sender DOES resolve
        DSH — the parent signal lifts the overlap."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "requestor_email": "foo@dsh.ca.gov",
            "institution": "Coalinga State Hospital",
            "ship_to": "Coalinga, CA",
        })
        assert key == "dsh"

    def test_dsh_atascadero_still_resolves(self):
        """Atascadero is DSH-only (no CCHCS facility there). Parent
        detection via 'STATE HOSPITAL' or 'DSH' should still pick dsh."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "agency": "DSH",
            "institution": "Atascadero State Hospital",
            "ship_to": "Atascadero, CA",
        })
        assert key == "dsh"

    def test_chcf_stockton_falls_to_other_not_dsh(self):
        """CHCF Stockton is a CCHCS facility. Post-§0 Job #1: no legacy
        CCHCS config, so resolution falls to 'other'. Must NOT mis-route
        to DSH — the cross-parent invariant is preserved."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "requestor_email": "Marc.Argarin@cdcr.ca.gov",
            "ship_to": "CHCF - California Health Care Facility, 7707 Austin Road, Stockton, CA 95215",
            "solicitation_number": "10843811",
        })
        assert key != "dsh"
        assert key == "other"

    def test_vsp_chowchilla_falls_to_other_not_dsh(self):
        """VSP Chowchilla is a CCHCS facility. Post-§0 Job #1: falls to
        'other'; must NOT mis-route to DSH."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "requestor_email": "Marc.Argarin@cdcr.ca.gov",
            "ship_to": "VSP - Valley State Prison, 21633 Avenue 24, Chowchilla, CA 93610",
            "solicitation_number": "10847776",
        })
        assert key != "dsh"
        assert key == "other"

    def test_parent_registry_shape(self):
        """PARENT_AGENCIES exports the expected parent ids and shapes."""
        from src.core.agency_config import PARENT_AGENCIES, OVERLAP_PATTERNS
        assert set(PARENT_AGENCIES) >= {"CDCR", "DSH", "CALVET", "DGS", "CALFIRE"}
        for parent_id, info in PARENT_AGENCIES.items():
            assert "strong_patterns" in info
            assert "domains" in info
            assert "children" in info
            assert isinstance(info["children"], list) and info["children"]
        assert "COALINGA" in OVERLAP_PATTERNS

    def test_detect_parent_helper(self):
        """The _detect_parent helper is the substrate primitive — pure
        function on (search_text, email_domain)."""
        from src.core.agency_config import _detect_parent
        assert _detect_parent("PLEASANT VALLEY STATE PRISON COALINGA", "cdcr.ca.gov") == "CDCR"
        assert _detect_parent("ATASCADERO STATE HOSPITAL", None) == "DSH"
        assert _detect_parent("DEPARTMENT OF VETERANS AFFAIRS", None) == "CALVET"
        assert _detect_parent("just coalinga, nothing else", None) is None
        assert _detect_parent("", None) is None
