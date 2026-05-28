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

    def test_cchcs_by_institution(self):
        # Restored 2026-05-27 22:10Z (hot-fix): DEFAULT_AGENCY_CONFIGS["cchcs"]
        # was reinstated to fix the Fill Plan substrate split-brain caused by
        # PR #1157's deletion. Legacy Regenerate path still calls match_agency.
        # Input uses CHCF abbreviation — that pattern is in the restored
        # minimal match-pattern set.
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"institution": "CHCF — California Health Care Facility"})
        assert key == "cchcs"
        assert "703b" in cfg["required_forms"] or "703c" in cfg["required_forms"]

    def test_cdcr_by_agency(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"agency": "CDCR"})
        assert key == "cchcs"  # CDCR maps to cchcs config

    def test_calvet_by_email(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"requestor_email": "buyer@calvet.ca.gov"})
        assert key == "calvet"

    def test_calvet_barstow_specific(self):
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"institution": "Barstow Veterans Home"})
        assert key == "calvet_barstow"
        assert "barstow_cuf" in cfg["required_forms"]

    def test_prison_keywords(self):
        match, _, _, _ = _import_agency_config()
        # Exact match_patterns from the restored cchcs config (uppercased).
        # Restored 2026-05-27 with a minimal pattern set: CCHCS / CDCR /
        # CORRECTIONS / CALIFORNIA CORRECTIONAL / STATE PRISON / CSP- /
        # CSP_ / CHCF / DUFFEY / FOLSOM / SAN QUENTIN / PELICAN BAY /
        # CORCORAN / AVENAL / CCHCS.CA.GOV / CDCR.CA.GOV.
        for keyword in ["STATE PRISON", "CORRECTIONS", "CALIFORNIA CORRECTIONAL"]:
            key, cfg = match({"institution": keyword})
            assert key == "cchcs", f"Failed for keyword: {keyword}"

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
        match, _, _, _ = _import_agency_config()
        _, cfg = match({"agency": "CCHCS"})
        assert "matched_by" in cfg

    def test_case_insensitive_matching(self):
        match, _, _, _ = _import_agency_config()
        key1, _ = match({"agency": "cchcs"})
        key2, _ = match({"agency": "CCHCS"})
        # search_text is .upper(), so both should match
        assert key1 == key2 == "cchcs"


# ── Required Forms ────────────────────────────────────────────────────────────

class TestRequiredForms:
    """Each agency config must have required_forms that are valid form IDs."""

    def test_all_configs_have_required_forms(self):
        _, load, _, defaults = _import_agency_config()
        configs = load()
        for key, cfg in configs.items():
            assert "required_forms" in cfg, f"Agency '{key}' missing required_forms"
            assert isinstance(cfg["required_forms"], list), f"Agency '{key}' required_forms is not a list"

    def test_cchcs_package_is_minimal(self):
        """CCHCS = 703B/C + 704B + bid package + quote ONLY.
        DVBE 843, seller's permit, CalRecycle are INSIDE the bid package."""
        _, load, _, _ = _import_agency_config()
        cfg = load()["cchcs"]
        forms = cfg["required_forms"]
        # Must have the core forms
        assert "bidpkg" in forms
        assert "quote" in forms
        # 703b or 703c (buyer provides template)
        assert "703b" in forms or "703c" in forms
        # These should NOT be standalone — they're inside bidpkg
        assert "dvbe843" not in forms, "DVBE 843 should be inside bid package, not standalone"
        assert "sellers_permit" not in forms, "Sellers permit should be inside bid package"

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

    def test_pvsp_coalinga_cchcs_does_not_match_dsh(self):
        """THE bug. PVSP in Coalinga, CCHCS buyer at cdcr.ca.gov.
        Pre-fix returned 'dsh' because 'COALINGA' is in DSH's patterns
        and DSH was prioritized over cchcs in the legacy loop. Now must
        return 'cchcs' because the cdcr.ca.gov email domain pins parent=CDCR."""
        match, _, _, _ = _import_agency_config()
        key, cfg = match({
            "agency": "cchcs",
            "requestor_email": "Mohammad.Chechi@cdcr.ca.gov",
            "institution": "cchcs",
            "ship_to": "Pleasant Valley State Prison, 24863 West Jayne Avenue, Coalinga, CA 93210",
            "solicitation_number": "10846357",
        })
        assert key == "cchcs", (
            f"Expected cchcs for PVSP/Coalinga CCHCS quote, got {key!r}. "
            "If this fails again, the COALINGA-as-DSH overlap regressed."
        )

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

    def test_chcf_stockton_cchcs(self):
        """CHCF Stockton is CCHCS. Stockton isn't a DSH city — no overlap."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "requestor_email": "Marc.Argarin@cdcr.ca.gov",
            "ship_to": "CHCF - California Health Care Facility, 7707 Austin Road, Stockton, CA 95215",
            "solicitation_number": "10843811",
        })
        assert key == "cchcs"

    def test_vsp_chowchilla_cchcs(self):
        """VSP Chowchilla is CCHCS. Chowchilla isn't a DSH city."""
        match, _, _, _ = _import_agency_config()
        key, _ = match({
            "requestor_email": "Marc.Argarin@cdcr.ca.gov",
            "ship_to": "VSP - Valley State Prison, 21633 Avenue 24, Chowchilla, CA 93610",
            "solicitation_number": "10847776",
        })
        assert key == "cchcs"

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


# ── Hot-fix regression pin (2026-05-27 PR-CCHCS-HOTFIX) ─────────────────────

def test_cchcs_legacy_regenerate_path_resolves_not_other():
    """PR #1157 hot-fix regression pin: the legacy /rfq/<id>/generate route
    still calls match_agency; CCHCS RFQs must resolve to 'cchcs' (not fall to
    'other'). Delete this test only when the legacy Regenerate route is
    repointed at Spine or deleted (Job #1 architectural follow-up).
    Incident: Duffey rfq_0124647e walked 2026-05-27 produced a 2-file
    OtherUnknown package instead of the 4-file CCHCS package."""
    from src.core.agency_config import match_agency
    key, cfg = match_agency({"agency": "CCHCS", "institution": "CSP-SAC"})
    assert key == "cchcs", f"Expected cchcs, got {key}"
    assert set(cfg["required_forms"]) >= {"703b", "704b", "bidpkg", "quote"}
    # Email-domain fallback
    key2, _ = match_agency({"requestor_email": "buyer@cdcr.ca.gov"})
    assert key2 == "cchcs", f"Expected cchcs via email domain, got {key2}"


# ── Substrate singleness — collapsed duplicates (2026-05-27 PR #1165 follow-up) ─

class TestSubstrateSingleness:
    """Pin against the re-emergence of duplicate substrate seams.

    Class lesson: when two files own the same concept (AVAILABLE_FORMS,
    DEFAULT_AGENCY_CONFIGS, match_agency), a change to one silently bypasses
    the other. Each assertion below catches a specific re-divergence.

    Incident this guards against: 2026-05-27 Coleman sol# 10842771 — 703A was
    added to canonical AVAILABLE_FORMS by PR #1163 but a duplicate copy in
    routes_analytics.py was missed, so /settings/packages UI didn't render
    703A and the cchcs agency_package_configs DB row was seeded without 703a
    in required_forms. The render dispatcher computed `_req_forms` without
    any 703 entry. The bug class is the duplicate's existence, not its
    contents.
    """

    def test_routes_analytics_available_forms_is_canonical(self):
        """routes_analytics.AVAILABLE_FORMS must BE the canonical object,
        not a copy. `is` identity (not `==`) is the right check — a copy
        can drift; the same object cannot."""
        from src.core.agency_config import AVAILABLE_FORMS as canonical
        from src.api.modules.routes_analytics import AVAILABLE_FORMS as routes_copy
        assert routes_copy is canonical, (
            "routes_analytics.AVAILABLE_FORMS must be the canonical list "
            "from src.core.agency_config. If you see this fail, someone "
            "redeclared the list — collapse it back to a single import."
        )

    def test_canonical_available_forms_has_source_field(self):
        """Every entry must carry a `source` value so /settings/packages
        renders the right icon (📧 / 📄 / ⚡ / 📎) per form. The render
        template at agency_packages.html:63 reads `form.source`."""
        from src.core.agency_config import AVAILABLE_FORMS
        valid_sources = {"email", "template", "generated", "static"}
        for f in AVAILABLE_FORMS:
            assert "source" in f, f"Form {f['id']} missing 'source' field"
            assert f["source"] in valid_sources, (
                f"Form {f['id']} has unknown source {f['source']!r}; "
                f"must be one of {valid_sources}"
            )

    def test_canonical_available_forms_contains_703_trio(self):
        """703A/703B/703C must all be in canonical AVAILABLE_FORMS. Pinned
        2026-05-27 after Coleman sol# 10842771 walked the substrate gap."""
        from src.core.agency_config import AVAILABLE_FORMS
        ids = {f["id"] for f in AVAILABLE_FORMS}
        for required in ("703a", "703b", "703c"):
            assert required in ids, (
                f"{required!r} missing from AVAILABLE_FORMS — every new "
                f"703 revision lives here, not in a route module."
            )
