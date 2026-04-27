"""Phase 1.6 PR2: pick_profile() routes through match_profile_for_agency.

Confirms the dispatcher in src/core/quote_engine.py now passes the
quote's agency context into the resolver so buyer-specific profiles
win over generic. Backwards-compatible — when no buyer-specific
exists, the generic standard still resolves identically.
"""

from unittest.mock import patch

import pytest

from src.core.quote_engine import pick_profile
from src.forms.profile_registry import FormProfile


def _quote_with_agency(agency: str = "", institution: str = ""):
    """Build a minimal Quote-shaped mock for pick_profile."""
    class _Header:
        agency_key = agency
        institution_key = institution
    class _Provenance:
        parsed_from_files = []
    class _Quote:
        header = _Header()
        provenance = _Provenance()
        doc_type = "pc"
    return _Quote()


def _profile(pid: str, fp: str = "fp123",
             agency_match: list[str] | None = None) -> FormProfile:
    return FormProfile(
        id=pid,
        form_type="703b",
        blank_pdf="",
        fill_mode="acroform",
        fingerprint=fp,
        agency_match=list(agency_match or []),
    )


class TestPickProfileAgencyAwareWiring:
    def test_buyer_specific_wins_when_agency_matches(self, tmp_path):
        # Real on-disk file so candidate_pdfs filter passes
        pdf = tmp_path / "buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}

        quote = _quote_with_agency(agency="CDCR Folsom")
        with patch("src.core.quote_engine.get_profiles",
                   return_value=profiles), \
             patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = pick_profile(quote, pdf_hint=str(pdf))
        assert result is spec

    def test_falls_back_to_generic_when_no_buyer_match(self, tmp_path):
        pdf = tmp_path / "buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}

        quote = _quote_with_agency(agency="Veterans Home Barstow")
        with patch("src.core.quote_engine.get_profiles",
                   return_value=profiles), \
             patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = pick_profile(quote, pdf_hint=str(pdf))
        # Different agency → falls back to standard
        assert result is std

    def test_uses_institution_key_when_agency_key_blank(self, tmp_path):
        # Some quote shapes only populate institution_key — make sure
        # the resolver still finds the buyer-specific match
        pdf = tmp_path / "buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}

        quote = _quote_with_agency(agency="", institution="CDCR Folsom")
        with patch("src.core.quote_engine.get_profiles",
                   return_value=profiles), \
             patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = pick_profile(quote, pdf_hint=str(pdf))
        assert result is spec

    def test_no_agency_falls_through_to_generic(self, tmp_path):
        pdf = tmp_path / "buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        std = _profile("703b_std", "fp_match")
        spec = _profile("703b_cdcr_folsom", "fp_match",
                        agency_match=["cdcr_folsom"])
        profiles = {"703b_std": std, "703b_cdcr_folsom": spec}

        quote = _quote_with_agency(agency="", institution="")
        with patch("src.core.quote_engine.get_profiles",
                   return_value=profiles), \
             patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = pick_profile(quote, pdf_hint=str(pdf))
        # No agency → can't pick buyer-specific → standard
        assert result is std

    def test_existing_world_with_no_buyer_profiles_unchanged(self, tmp_path):
        # The pre-PR2 behavior: only generic profiles loaded. Should
        # still resolve identically.
        pdf = tmp_path / "buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        std = _profile("703b_std", "fp_match")
        profiles = {"703b_std": std}

        quote = _quote_with_agency(agency="CDCR Folsom")
        with patch("src.core.quote_engine.get_profiles",
                   return_value=profiles), \
             patch("src.forms.profile_registry._compute_fingerprint",
                   return_value="fp_match"):
            result = pick_profile(quote, pdf_hint=str(pdf))
        assert result is std
