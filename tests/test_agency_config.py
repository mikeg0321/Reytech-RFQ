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
        match, _, _, _ = _import_agency_config()
        key, cfg = match({"institution": "California Health Care Facility"})
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
        # These are exact match_patterns from cchcs config (uppercased)
        for keyword in ["CIM", "STATE PRISON", "CORRECTIONAL"]:
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
