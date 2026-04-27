"""Phase 1.6 PR1: per-buyer form profile lookup.

Covers the resolution mechanism without requiring real buyer YAMLs
(those come in PR2). Builds in-memory FormProfile objects with
synthetic fingerprints and asserts which one match_profile_for_agency
returns.
"""

import os
import tempfile
from unittest.mock import patch

import pytest

from src.forms.profile_registry import (
    FormProfile,
    FieldMapping,
    _agency_matches,
    _normalize_agency_token,
    match_profile_for_agency,
)


def _profile(pid: str, fp: str, agency_match: list[str] | None = None) -> FormProfile:
    """Build a minimal in-memory profile."""
    return FormProfile(
        id=pid,
        form_type="703b",
        blank_pdf="",
        fill_mode="acroform",
        fingerprint=fp,
        agency_match=list(agency_match or []),
    )


@pytest.fixture
def fake_pdf(tmp_path):
    """A real on-disk PDF whose fingerprint we control via patching."""
    p = tmp_path / "fake.pdf"
    p.write_bytes(b"%PDF-1.4\n%mock\n")
    return str(p)


class TestNormalizeAgencyToken:
    def test_lowercases_and_collapses_separators(self):
        assert _normalize_agency_token("CDCR Folsom") == "cdcr_folsom"
        assert _normalize_agency_token("CDCR-Folsom") == "cdcr_folsom"
        assert _normalize_agency_token("cdcr_folsom") == "cdcr_folsom"
        assert _normalize_agency_token("California Institution For Women") == \
            "california_institution_for_women"

    def test_handles_none_and_empty(self):
        assert _normalize_agency_token("") == ""
        assert _normalize_agency_token(None) == ""

    def test_strips_leading_trailing_underscores(self):
        assert _normalize_agency_token("  CDCR  ") == "cdcr"
        assert _normalize_agency_token("---CDCR---") == "cdcr"


class TestAgencyMatches:
    def test_empty_tokens_never_match(self):
        assert _agency_matches([], "CDCR Folsom") is False

    def test_empty_agency_never_matches(self):
        assert _agency_matches(["cdcr"], "") is False

    def test_exact_token_match(self):
        assert _agency_matches(["cdcr_folsom"], "CDCR Folsom") is True

    def test_token_substring_in_agency(self):
        # 'cdcr' token matches 'cdcr_folsom' agency
        assert _agency_matches(["cdcr"], "CDCR Folsom") is True

    def test_agency_substring_in_token(self):
        # Reverse: agency 'folsom' matches token 'cdcr_folsom'
        assert _agency_matches(["cdcr_folsom"], "Folsom") is True

    def test_no_match_when_unrelated(self):
        assert _agency_matches(["cdcr"], "Veterans Home Barstow") is False


class TestMatchProfileForAgency:
    def test_no_fingerprint_match_returns_none(self, fake_pdf):
        profiles = {"a": _profile("a", "fp_xyz")}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_unknown"):
            result = match_profile_for_agency(fake_pdf, profiles, "")
        assert result is None

    def test_only_generic_returned_when_no_specific_exists(self, fake_pdf):
        std = _profile("703b_std", "fp_match")
        profiles = {"703b_std": std}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = match_profile_for_agency(fake_pdf, profiles, "CDCR Folsom")
        assert result is std

    def test_specific_wins_over_generic(self, fake_pdf):
        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = match_profile_for_agency(fake_pdf, profiles, "CDCR Folsom")
        assert result is spec

    def test_falls_back_to_generic_when_specific_doesnt_match_agency(self, fake_pdf):
        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            # Different buyer — should fall back to standard
            result = match_profile_for_agency(fake_pdf, profiles, "Veterans Home Barstow")
        assert result is std

    def test_longest_token_wins_on_ties(self, fake_pdf):
        # Both 'cdcr' (parent) and 'cdcr_folsom' (specific) match the
        # input agency 'CDCR Folsom'. Specific should win.
        parent = _profile("703b_cdcr_parent", "fp_match",
                          agency_match=["cdcr"])
        specific = _profile("703b_cdcr_folsom", "fp_match",
                            agency_match=["cdcr_folsom"])
        profiles = {"parent": parent, "specific": specific}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = match_profile_for_agency(fake_pdf, profiles, "CDCR Folsom")
        assert result is specific

    def test_empty_agency_string_returns_generic(self, fake_pdf):
        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = match_profile_for_agency(fake_pdf, profiles, "")
        assert result is std

    def test_specific_only_no_generic_returns_specific_as_fallback(self, fake_pdf):
        # If a buyer-specific is the ONLY profile for this fingerprint
        # and the agency doesn't match, still return it rather than None
        # — the alternative (returning None) breaks the fill flow.
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_cdcr_folsom": spec}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = match_profile_for_agency(fake_pdf, profiles, "Veterans Home Barstow")
        assert result is spec

    def test_multiple_tokens_per_profile(self, fake_pdf):
        # A single profile can apply to several facilities
        shared = _profile("704b_vet_homes", "fp_match",
                          agency_match=["veterans_home_barstow",
                                        "veterans_home_yountville"])
        std = _profile("704b_std", "fp_match")
        profiles = {"std": std, "shared": shared}
        with patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            r1 = match_profile_for_agency(fake_pdf, profiles, "Veterans Home Barstow")
            r2 = match_profile_for_agency(fake_pdf, profiles, "Veterans Home Yountville")
            r3 = match_profile_for_agency(fake_pdf, profiles, "Veterans Home Fresno")
        assert r1 is shared
        assert r2 is shared
        assert r3 is std  # falls back


class TestYamlLoadsAgencyMatch:
    def test_top_level_agency_match_loads(self, tmp_path):
        from src.forms.profile_registry import load_profile
        yaml_text = """
id: 703b_cdcr_folsom
form_type: 703b
fill_mode: acroform
agency_match:
  - CDCR Folsom
fields: {}
"""
        path = tmp_path / "test.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        profile = load_profile(str(path))
        assert profile.agency_match == ["cdcr_folsom"]

    def test_meta_nested_agency_match_loads(self, tmp_path):
        from src.forms.profile_registry import load_profile
        yaml_text = """
id: 703b_cdcr_folsom
form_type: 703b
fill_mode: acroform
meta:
  agency_match:
    - CDCR Folsom
    - cdcr_lac
fields: {}
"""
        path = tmp_path / "test.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        profile = load_profile(str(path))
        assert profile.agency_match == ["cdcr_folsom", "cdcr_lac"]

    def test_string_agency_match_coerces_to_list(self, tmp_path):
        from src.forms.profile_registry import load_profile
        yaml_text = """
id: 703b_one
form_type: 703b
fill_mode: acroform
agency_match: cdcr_folsom
fields: {}
"""
        path = tmp_path / "test.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        profile = load_profile(str(path))
        assert profile.agency_match == ["cdcr_folsom"]

    def test_missing_agency_match_is_empty_list(self, tmp_path):
        from src.forms.profile_registry import load_profile
        yaml_text = """
id: 703b_std
form_type: 703b
fill_mode: acroform
fields: {}
"""
        path = tmp_path / "test.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        profile = load_profile(str(path))
        assert profile.agency_match == []
