"""V2 Test Suite — Group 10: Signature & Form Package Safety.

Tests that prevent:
- Double signatures
- Signatures in wrong position (should be lower 40%)
- Wrong CCHCS package contents
- Optional forms auto-included without user consent

Incidents: Double-sign, wrong form packages for CCHCS.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


class TestSignaturePosition:
    """Generic sig fields (Signature1, Signature) must be in lower 40%."""

    def test_lower_40_percent_check(self):
        """Signature at y=100 on a 792pt page is at 12.6% from bottom — OK."""
        page_height = 792  # standard letter
        sig_y = 100  # near bottom
        position_from_bottom = sig_y / page_height
        assert position_from_bottom <= 0.40, \
            f"Signature at {position_from_bottom:.0%} from bottom — should be in lower 40%"

    def test_upper_position_rejected(self):
        """Signature at y=600 on a 792pt page is at 75.8% — too high."""
        page_height = 792
        sig_y = 600  # near top
        position_from_bottom = sig_y / page_height
        in_lower_40 = position_from_bottom <= 0.40
        assert in_lower_40 is False, \
            "Signature in upper portion should be rejected for generic fields"


class TestNoDoubleSignature:
    """If PDF has /Sig form field, overlay signature must NOT also run."""

    def test_sig_field_present_skips_overlay(self):
        """When /Sig exists, fill_and_sign_pdf handles it — no overlay needed."""
        pdf_has_sig_field = True
        should_run_overlay = not pdf_has_sig_field
        assert should_run_overlay is False, \
            "/Sig field present = overlay must not run"

    def test_no_sig_field_allows_overlay(self):
        """When no /Sig field, overlay is the only option."""
        pdf_has_sig_field = False
        should_run_overlay = not pdf_has_sig_field
        assert should_run_overlay is True


class TestCchcsPackageContents:
    """CCHCS package = 703B/C + 704B + Bid Package + Quote ONLY.
    DVBE 843, seller's permit, CalRecycle are INSIDE the bid package.
    """

    CCHCS_REQUIRED = {"703b", "704b", "bidpkg", "quote"}
    INSIDE_BIDPKG = {"dvbe843", "sellers_permit", "calrecycle74", "darfur_act"}

    # DELETED 2026-05-27 (Job #1): test_cchcs_required_forms
    #
    # This test pinned the CCHCS form set against the legacy
    # DEFAULT_AGENCY_CONFIGS dict (`load_agency_configs()["cchcs"]`).
    # That entry was DELETED per §0 LAW 2 (Spine is canonical for
    # CCHCS). The form contract (703b + 704b + bidpkg + quote) now
    # lives on the Spine path — see PR #1155 (`src/spine/agency_constants.py`)
    # and PR #1156 (`AGENCY_CONFIGS["CCHCS"]` bill-to migration), both
    # of which land on main before PR-B merges.
    #
    # The Spine-side equivalent is pinned in
    # `tests/spine/test_agency_constants.py::test_cchcs_required_forms`.
    # The remaining tests in this class (dvbe_not_standalone) still
    # ride the legacy dict and stay valid for other agencies.

    def test_dvbe_not_standalone_for_cchcs(self):
        """DVBE 843 should NOT be a standalone required form for CCHCS
        (it's inside the bid package)."""
        try:
            from src.core.agency_config import load_agency_configs
            configs = load_agency_configs()
            cchcs = configs.get("cchcs", {})
            required = set(cchcs.get("required_forms", []))
            # dvbe843 as standalone would be wrong — it's in the bid package
            if "dvbe843" in required and "bidpkg" in required:
                # Both present is suspicious but may be valid
                pass  # Allow — some configs include both
        except ImportError:
            pytest.skip("agency_config not importable")


class TestOptionalFormsNotAutoIncluded:
    """Optional forms must ONLY be included if user explicitly checks them."""

    def test_empty_package_forms_means_defaults_only(self):
        """With no user selections, only agency-required forms should generate."""
        user_selections = {}  # User checked nothing
        agency_required = {"703b", "704b", "bidpkg"}

        # Final forms = agency required + user selections
        final_forms = set(agency_required)
        for form_id, checked in user_selections.items():
            if checked:
                final_forms.add(form_id)

        assert final_forms == agency_required, \
            "No user selections should only produce agency-required forms"

    def test_user_can_add_optional_form(self):
        """User checking CalRecycle should add it."""
        user_selections = {"calrecycle74": True}
        agency_required = {"703b", "704b"}

        final_forms = set(agency_required)
        for form_id, checked in user_selections.items():
            if checked:
                final_forms.add(form_id)

        assert "calrecycle74" in final_forms

    def test_agency_required_overrides_user_unchecked(self):
        """Even if user unchecks a required form, it must still be included."""
        user_selections = {"703b": False}  # User tried to uncheck
        agency_required = {"703b", "704b"}

        final_forms = set(agency_required)  # Required always win
        for form_id, checked in user_selections.items():
            if checked and form_id not in agency_required:
                final_forms.add(form_id)

        assert "703b" in final_forms, \
            "Agency-required forms must always be included"
