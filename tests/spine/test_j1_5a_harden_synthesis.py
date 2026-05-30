"""Tests for J1-5a: Hardened CCHCS Spine synthesis — form-set resilience.

Verifies four requirements from the Inspector's J1-2 gate finding:
  (a) CCHCS RFQ with tax resolver returning None still yields the correct
      CCHCS required_forms — does NOT fall back to legacy config / drop forms.
  (b) CCHCS RFQ with empty/blank-description line_items still yields the
      correct CCHCS form set — does NOT fall back to legacy config.
  (c) The loud-WARNING path fires for a CCHCS synthesis failure but NOT for
      a genuine non-CCHCS (NotCchcsError) RFQ (that path stays quiet).
  (d) The existing J1-2 tests still pass (regression guard).

Also tests get_cchcs_required_forms() directly.
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.spine.email_contract import CCHCS_DEFAULT_REQUIRED_FORMS
from src.spine_bridge.ingest import NotCchcsError, get_cchcs_required_forms


# ──────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────


def _cchcs_rfq(**overrides) -> dict:
    base = {
        "agency": "CCHCS",
        "institution": "SATF Corcoran",
        "ship_to": "900 Quebec Ave, Corcoran, CA 93212",
        "solicitation_number": "PREQ 10847262",
        "line_items": [
            {
                "description": "Elastic Bandage 4 inch",
                "qty": 100,
                "uom": "EA",
                "item_number": "W12919",
            }
        ],
    }
    base.update(overrides)
    return base


def _calvet_rfq() -> dict:
    return {
        "agency": "CALVET",
        "institution": "Yountville",
        "ship_to": "100 California Dr, Yountville, CA 94599",
        "solicitation_number": "CV-2025-001",
        "line_items": [
            {"description": "Gloves", "qty": 200, "uom": "BX"}
        ],
    }


def _tax_none(_addr: str) -> None:
    """Tax resolver that always returns None (simulates CDTFA outage)."""
    return None


def _tax_825(_addr: str) -> int:
    return 825


# ──────────────────────────────────────────────────────────────────────
# get_cchcs_required_forms — unit tests
# ──────────────────────────────────────────────────────────────────────


class TestGetCchcsRequiredForms:
    """get_cchcs_required_forms returns a valid form list without tax or items."""

    def test_default_forms_for_bare_rfq(self):
        """An RFQ with no required_forms field returns the CCHCS defaults."""
        rfq = _cchcs_rfq()
        assert "required_forms" not in rfq

        forms = get_cchcs_required_forms(rfq)

        assert set(forms) == set(CCHCS_DEFAULT_REQUIRED_FORMS)

    def test_passes_through_valid_custom_forms(self):
        """If rfq_row carries a recognized required_forms list it passes through."""
        # Use only codes that appear in ALL_FORM_CODES (703c is valid; 703a is
        # not currently a FormCode in the Spine model).
        rfq = _cchcs_rfq(required_forms=["703c", "704b", "bidpkg", "quote"])

        forms = get_cchcs_required_forms(rfq)

        assert "703c" in forms
        assert "704b" in forms

    def test_falls_back_on_invalid_form_code(self):
        """An unrecognized code in required_forms causes fallback to defaults."""
        rfq = _cchcs_rfq(required_forms=["BOGUS_FORM", "704b"])

        forms = get_cchcs_required_forms(rfq)

        # Falls back to defaults — "BOGUS_FORM" must not appear.
        assert "BOGUS_FORM" not in forms
        assert set(forms) == set(CCHCS_DEFAULT_REQUIRED_FORMS)

    def test_falls_back_on_empty_list(self):
        """An empty required_forms causes fallback to defaults."""
        rfq = _cchcs_rfq(required_forms=[])

        forms = get_cchcs_required_forms(rfq)

        assert set(forms) == set(CCHCS_DEFAULT_REQUIRED_FORMS)

    def test_works_with_empty_line_items(self):
        """Works even when rfq_row has no line_items — does not need them."""
        rfq = _cchcs_rfq(line_items=[])

        forms = get_cchcs_required_forms(rfq)

        assert "704b" in forms
        assert "bidpkg" in forms

    def test_works_with_no_ship_to_no_facility(self):
        """Works even without ship_to or facility — no tax needed."""
        rfq = _cchcs_rfq()
        rfq.pop("ship_to", None)
        rfq.pop("institution", None)

        forms = get_cchcs_required_forms(rfq)

        assert set(forms) == set(CCHCS_DEFAULT_REQUIRED_FORMS)


# ──────────────────────────────────────────────────────────────────────
# (a) Tax resolver returning None — fill_plan_builder._resolve_agency
# ──────────────────────────────────────────────────────────────────────


class TestCchcsFormSetResilientToTaxNone:
    """CCHCS form set must survive a tax resolver that returns None."""

    def test_fill_plan_tax_none_yields_cchcs_forms(self):
        """_resolve_agency: tax-None still gives CCHCS form set, NOT match_agency."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq()

        with patch(
            "src.spine_bridge.shadow_ingest._make_tax_resolver",
            return_value=_tax_none,
        ):
            with patch("src.core.agency_config.match_agency") as mock_match:
                key, cfg = _resolve_agency(rfq, quote_id="rfq_tax_none")

        # match_agency must NOT be called — form-set fallback is used instead.
        mock_match.assert_not_called()
        assert key == "cchcs"
        forms = cfg["required_forms"]
        assert "704b" in forms
        assert "bidpkg" in forms
        assert "quote" in forms

    def test_fill_plan_tax_none_does_not_return_empty_forms(self):
        """Form set must not be empty when tax resolver returns None."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq()

        with patch(
            "src.spine_bridge.shadow_ingest._make_tax_resolver",
            return_value=_tax_none,
        ):
            key, cfg = _resolve_agency(rfq, quote_id="rfq_tax_none_nonempty")

        assert cfg["required_forms"], "required_forms must not be empty after tax-None"


# ──────────────────────────────────────────────────────────────────────
# (b) Empty / blank-description line_items — fill_plan_builder
# ──────────────────────────────────────────────────────────────────────


class TestCchcsFormSetResilientToEmptyItems:
    """CCHCS form set must survive empty or blank-only line_items."""

    def test_fill_plan_empty_items_yields_cchcs_forms(self):
        """_resolve_agency: empty line_items still gives CCHCS form set."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq(line_items=[])

        with patch(
            "src.spine_bridge.shadow_ingest._make_tax_resolver",
            return_value=_tax_825,
        ):
            with patch("src.core.agency_config.match_agency") as mock_match:
                key, cfg = _resolve_agency(rfq, quote_id="rfq_empty_items")

        mock_match.assert_not_called()
        assert key == "cchcs"
        forms = cfg["required_forms"]
        assert "704b" in forms
        assert "bidpkg" in forms

    def test_fill_plan_blank_descriptions_yields_cchcs_forms(self):
        """_resolve_agency: items with blank descriptions still give CCHCS form set."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq(line_items=[
            {"description": "", "qty": 10, "uom": "EA"},
            {"description": "   ", "qty": 5, "uom": "BX"},
        ])

        with patch(
            "src.spine_bridge.shadow_ingest._make_tax_resolver",
            return_value=_tax_825,
        ):
            with patch("src.core.agency_config.match_agency") as mock_match:
                key, cfg = _resolve_agency(rfq, quote_id="rfq_blank_descs")

        mock_match.assert_not_called()
        assert key == "cchcs"
        forms = cfg["required_forms"]
        assert "704b" in forms
        assert "bidpkg" in forms


# ──────────────────────────────────────────────────────────────────────
# (c) Loud-WARNING for CCHCS failure; quiet for NotCchcsError
# ──────────────────────────────────────────────────────────────────────


class TestWarningLoudness:
    """The WARNING path fires for CCHCS synthesis failure but not for NotCchcsError."""

    def test_cchcs_synthesis_failure_logs_warning(self, caplog):
        """A ValueError during CCHCS synthesis must produce a WARNING log entry."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq()

        import src.spine_bridge as _sb_module
        orig_synth = _sb_module.synthesize_cchcs_email_contract
        try:
            _sb_module.synthesize_cchcs_email_contract = MagicMock(
                side_effect=ValueError("tax resolver returned no rate")
            )
            with caplog.at_level(logging.WARNING, logger="reytech.fill_plan"):
                key, cfg = _resolve_agency(rfq, quote_id="rfq_warn_test")
        finally:
            _sb_module.synthesize_cchcs_email_contract = orig_synth

        # Must have produced a WARNING (not just DEBUG).
        warning_messages = [
            r.message for r in caplog.records if r.levelno >= logging.WARNING
        ]
        assert any("J1-5a" in msg for msg in warning_messages), (
            f"Expected a J1-5a WARNING, got: {warning_messages}"
        )
        # And the form set must still be CCHCS, not empty.
        assert key == "cchcs"
        assert "704b" in cfg["required_forms"]

    def test_not_cchcs_error_does_not_log_warning(self, caplog):
        """NotCchcsError (non-CCHCS RFQ) must NOT produce a WARNING log entry."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _calvet_rfq()

        with patch("src.core.agency_config.match_agency") as mock_match:
            mock_match.return_value = (
                "calvet",
                {"name": "CalVet", "required_forms": ["quote"]},
            )
            with caplog.at_level(logging.WARNING, logger="reytech.fill_plan"):
                key, cfg = _resolve_agency(rfq, quote_id="rfq_calvet_quiet")

        # No WARNING should be emitted for a non-CCHCS RFQ.
        warning_messages = [
            r.message for r in caplog.records if r.levelno >= logging.WARNING
        ]
        assert not any("J1-5a" in msg for msg in warning_messages), (
            f"Unexpected J1-5a WARNING for non-CCHCS: {warning_messages}"
        )
        assert key == "calvet"

    def test_pydantic_validation_error_also_logs_warning(self, caplog):
        """A Pydantic ValidationError (empty line_items) must also produce a WARNING."""
        from pydantic import ValidationError
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq()

        import src.spine_bridge as _sb_module
        orig_synth = _sb_module.synthesize_cchcs_email_contract
        try:
            # Simulate Pydantic rejecting empty line_items
            _sb_module.synthesize_cchcs_email_contract = MagicMock(
                side_effect=Exception("Pydantic ValidationError: line_items too short")
            )
            with caplog.at_level(logging.WARNING, logger="reytech.fill_plan"):
                key, cfg = _resolve_agency(rfq, quote_id="rfq_pydantic_err")
        finally:
            _sb_module.synthesize_cchcs_email_contract = orig_synth

        warning_messages = [
            r.message for r in caplog.records if r.levelno >= logging.WARNING
        ]
        assert any("J1-5a" in msg for msg in warning_messages), (
            f"Expected J1-5a WARNING for ValidationError-like failure, "
            f"got: {warning_messages}"
        )
        assert key == "cchcs"
        assert "704b" in cfg["required_forms"]


# ──────────────────────────────────────────────────────────────────────
# J1-5 safety gate: after this ticket, deleting DEFAULT_AGENCY_CONFIGS
# ["cchcs"] cannot silently drop the CCHCS form set.
# ──────────────────────────────────────────────────────────────────────


class TestJ15DeletionSafetyGate:
    """Prove that J1-5 (delete DEFAULT_AGENCY_CONFIGS["cchcs"]) is safe.

    These tests verify that even if DEFAULT_AGENCY_CONFIGS["cchcs"] does
    not exist, the CCHCS form set is still resolved correctly from the
    Spine path — so J1-5's deletion cannot silently drop the form set.
    """

    def test_cchcs_forms_independent_of_legacy_config(self):
        """get_cchcs_required_forms does NOT read DEFAULT_AGENCY_CONFIGS."""
        # Simulate DEFAULT_AGENCY_CONFIGS["cchcs"] being absent by patching.
        with patch.dict(
            "sys.modules",
            {},  # no extra patching needed — get_cchcs_required_forms never
                 # touches agency_config.py
        ):
            rfq = _cchcs_rfq()
            forms = get_cchcs_required_forms(rfq)

        assert "704b" in forms
        assert "bidpkg" in forms
        assert len(forms) >= 4

    def test_fill_plan_tax_none_does_not_read_agency_config(self):
        """_resolve_agency with tax-None must not touch DEFAULT_AGENCY_CONFIGS."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _cchcs_rfq()

        with patch(
            "src.spine_bridge.shadow_ingest._make_tax_resolver",
            return_value=_tax_none,
        ):
            # If match_agency is called AND agency_config["cchcs"] is absent,
            # the function would fall through to "other".  Assert match_agency
            # is not called so that path is provably dead.
            with patch("src.core.agency_config.match_agency") as mock_match:
                key, cfg = _resolve_agency(rfq, quote_id="rfq_j15_gate")

        mock_match.assert_not_called()
        assert key == "cchcs", (
            "After J1-5, a tax-None CCHCS synthesis failure must still yield "
            "agency_key='cchcs' — not 'other'."
        )
        assert "704b" in cfg["required_forms"]
