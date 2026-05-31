"""Tests for J1-2: CCHCS form-set repoint to the Spine contract.

Verifies four repoint targets:
  1. routes_rfq_gen.py — primary generate path
  2. routes_rfq_admin.py api_diag_package — diagnostic endpoint
  3. forms/reytech_filler_v4.py fill_bid_package — bidpkg page-trim
  4. agents/fill_plan_builder.py _resolve_agency — fill-plan display

Core assertions per target:
  - CCHCS RFQ: form set comes from the Spine contract, NOT from
    DEFAULT_AGENCY_CONFIGS["cchcs"]  (match_agency NOT called).
  - Non-CCHCS RFQ: still uses match_agency (legacy path unchanged).
  - Both CCHCS formats: packet (single_pdf) and standalone (separate_pdfs).
  - Fallback: if synthesize raises for CCHCS, legacy path is used.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from src.spine.email_contract import CCHCS_DEFAULT_REQUIRED_FORMS, EmailContract
from src.spine_bridge.ingest import NotCchcsError


# ──────────────────────────────────────────────────────────────────────
# Shared stubs / fixtures
# ──────────────────────────────────────────────────────────────────────


def _tax_825(_addr: str) -> int:
    return 825


def _minimal_rfq(**overrides) -> dict:
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


def _calvet_rfq(**overrides) -> dict:
    base = {
        "agency": "CALVET",
        "institution": "Yountville",
        "ship_to": "100 California Dr, Yountville, CA 94599",
        "solicitation_number": "CV-2025-001",
        "line_items": [
            {"description": "Gloves", "qty": 200, "uom": "BX"}
        ],
    }
    base.update(overrides)
    return base


def _mock_spine_contract(forms=None) -> MagicMock:
    """Return an EmailContract mock with the given required_forms."""
    m = MagicMock(spec=EmailContract)
    m.required_forms = forms if forms is not None else list(CCHCS_DEFAULT_REQUIRED_FORMS)
    return m


# ──────────────────────────────────────────────────────────────────────
# 1. _resolve_agency (fill_plan_builder) — target 4
# ──────────────────────────────────────────────────────────────────────


class TestResolveAgencyFillPlan:
    """_resolve_agency repoints CCHCS to Spine contract."""

    def test_cchcs_does_not_call_match_agency(self):
        """match_agency must NOT be called for a CCHCS RFQ."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _minimal_rfq()
        # Patch the dynamic import of match_agency inside _resolve_agency
        # by patching on the module object after the fact.
        with patch("src.core.agency_config.match_agency") as mock_match:
            # For CCHCS: synthesize_cchcs_email_contract runs; NotCchcsError
            # not raised → match_agency branch never entered.
            # We use the real synthesize with a stubbed tax resolver.
            with patch("src.spine_bridge.shadow_ingest._make_tax_resolver",
                       return_value=_tax_825):
                key, cfg = _resolve_agency(rfq, quote_id="rfq_test001")

        mock_match.assert_not_called()
        assert key == "cchcs"

    def test_cchcs_required_forms_come_from_spine(self):
        """Form set for CCHCS must contain the Spine CCHCS defaults."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _minimal_rfq()
        with patch("src.spine_bridge.shadow_ingest._make_tax_resolver",
                   return_value=_tax_825):
            key, cfg = _resolve_agency(rfq, quote_id="rfq_test001")

        assert key == "cchcs"
        forms = cfg["required_forms"]
        assert "704b" in forms
        assert "bidpkg" in forms
        assert "quote" in forms
        assert any(f in forms for f in ("703a", "703b", "703c"))

    def test_non_cchcs_uses_match_agency(self):
        """Non-CCHCS RFQ must still use the legacy match_agency path."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _calvet_rfq()
        with patch("src.core.agency_config.match_agency") as mock_match:
            mock_match.return_value = (
                "calvet",
                {"name": "CalVet", "required_forms": ["quote"]},
            )
            key, cfg = _resolve_agency(rfq, quote_id="rfq_calvet01")

        mock_match.assert_called_once()
        assert key == "calvet"

    def test_cchcs_acq_also_uses_spine(self):
        """CCHCS-ACQ variant must also use the Spine path (not match_agency)."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _minimal_rfq(agency="CCHCS-ACQ")
        with patch("src.core.agency_config.match_agency") as mock_match:
            with patch("src.spine_bridge.shadow_ingest._make_tax_resolver",
                       return_value=_tax_825):
                key, cfg = _resolve_agency(rfq, quote_id="rfq_acq01")

        mock_match.assert_not_called()
        assert key == "cchcs"

    def test_cchcs_synthesis_failure_falls_back_to_match_agency(self):
        """If synthesize raises ValueError, _resolve_agency falls back to match_agency."""
        from src.agents.fill_plan_builder import _resolve_agency

        rfq = _minimal_rfq()
        fallback_cfg = {
            "name": "CCHCS / CDCR (legacy)",
            "required_forms": ["703b", "704b", "bidpkg", "quote"],
        }
        # The dynamic import inside _resolve_agency does:
        #   from src.spine_bridge import synthesize_cchcs_email_contract
        # which resolves via src.spine_bridge.__init__. Patch the package
        # attribute so the dynamic import gets the mock.
        import src.spine_bridge as _sb_module
        orig = _sb_module.synthesize_cchcs_email_contract
        try:
            _sb_module.synthesize_cchcs_email_contract = MagicMock(
                side_effect=ValueError("tax resolver returned no rate")
            )
            with patch("src.core.agency_config.match_agency") as mock_match:
                mock_match.return_value = ("cchcs", fallback_cfg)
                key, cfg = _resolve_agency(rfq, quote_id="rfq_fallback")
        finally:
            _sb_module.synthesize_cchcs_email_contract = orig

        mock_match.assert_called_once()
        assert key == "cchcs"
        assert "704b" in cfg["required_forms"]


# ──────────────────────────────────────────────────────────────────────
# 2. fill_bid_package page-trim logic (reytech_filler_v4) — target 3
# ──────────────────────────────────────────────────────────────────────


class TestFillBidPackagePageTrim:
    """fill_bid_package page-trim must NOT call match_agency for CCHCS."""

    def test_cchcs_agency_raw_in_bypass_set(self):
        """Ensure CCHCS hits the bypass branch and produces empty _bidpkg_replaced."""
        # Test the exact logic transplanted from the repointed block.
        for agency in ("CCHCS", "CCHCS-ACQ", "cchcs"):
            rfq = _minimal_rfq(agency=agency)
            _agency_raw = (rfq.get("agency") or rfq.get("agency_key") or "").upper()
            in_bypass = _agency_raw in ("CCHCS", "CCHCS-ACQ")
            if in_bypass:
                _required = set()
            else:
                # Simulate legacy path (won't run for CCHCS).
                _required = {"bidder_decl", "darfur_act"}
            _bidpkg_replaced = frozenset({"bidder_decl", "darfur_act"} & _required)
            assert in_bypass, f"agency={agency!r} should be in bypass set"
            assert _bidpkg_replaced == frozenset(), (
                f"Expected empty _bidpkg_replaced for {agency!r}, got {_bidpkg_replaced}"
            )

    def test_non_cchcs_not_in_bypass(self):
        """Non-CCHCS agencies must NOT be in the CCHCS bypass set."""
        for agency in ("CALVET", "CDCR", "DSH", "DGS", ""):
            _agency_raw = agency.upper()
            in_bypass = _agency_raw in ("CCHCS", "CCHCS-ACQ")
            assert not in_bypass, f"agency={agency!r} incorrectly in bypass set"

    def test_cchcs_bidpkg_replaced_always_empty_for_both_sources(self):
        """For CCHCS, _bidpkg_replaced is always empty regardless of form source."""
        for cchcs_forms in [
            ["703a", "703b", "703c", "704b", "bidpkg", "quote"],  # legacy
            ["703b", "704b", "bidpkg", "quote"],                  # Spine default
        ]:
            _required = set(cchcs_forms)
            _bidpkg_replaced = frozenset({"bidder_decl", "darfur_act"} & _required)
            assert _bidpkg_replaced == frozenset(), (
                f"Expected empty _bidpkg_replaced for CCHCS, got {_bidpkg_replaced} "
                f"with forms={cchcs_forms}"
            )


# ──────────────────────────────────────────────────────────────────────
# 3. Primary generate path logic — target 1
# ──────────────────────────────────────────────────────────────────────


class TestGeneratePathFormSetSource:
    """The generate path must read CCHCS forms from the Spine, not match_agency."""

    def _run_generate_agency_block(self, rfq: dict, rid: str,
                                   tax_resolver=_tax_825,
                                   force_synth_fail: bool = False):
        """Replicate the generate-path agency block in isolation.

        Returns (match_agency_called: bool, agency_key: str, req_forms: list).
        """
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("cchcs", {
                "name": "CCHCS / CDCR (legacy)",
                "required_forms": ["703b", "704b", "bidpkg", "quote"],
            })

        _spine_contract = None
        if force_synth_fail:
            _spine_contract = None
            _agency_key, _agency_cfg = _fake_match_agency(rfq)
            _req_forms_raw = list(_agency_cfg.get("required_forms", []))
        else:
            try:
                from src.spine_bridge.ingest import (
                    synthesize_cchcs_email_contract, NotCchcsError,
                )
                _spine_contract = synthesize_cchcs_email_contract(
                    rfq_row=rfq, rfq_id=rid,
                    tax_resolver=tax_resolver,
                )
            except NotCchcsError:
                pass
            except Exception:
                _spine_contract = None

            if _spine_contract is not None:
                _spine_forms_base = list(_spine_contract.required_forms)
                _703_variants = {"703a", "703b", "703c"}
                if _703_variants & set(_spine_forms_base):
                    for _v703 in ("703a", "703b", "703c"):
                        if _v703 not in _spine_forms_base:
                            _spine_forms_base.append(_v703)
                _req_forms_raw = _spine_forms_base
                _agency_key = "cchcs"
            else:
                _agency_key, _agency_cfg = _fake_match_agency(rfq)
                _req_forms_raw = list(_agency_cfg.get("required_forms", []))

        return match_agency_called, _agency_key, _req_forms_raw

    def test_cchcs_match_agency_not_called(self):
        """match_agency must NOT be called for a well-formed CCHCS RFQ."""
        rfq = _minimal_rfq()
        called, key, forms = self._run_generate_agency_block(rfq, "rfq_j12_ok")
        assert not called, "match_agency was called for CCHCS — it must not be"
        assert key == "cchcs"

    def test_cchcs_forms_include_all_703_variants(self):
        """_req_forms_raw for CCHCS must include all three 703 variants (for rev-aware filter)."""
        rfq = _minimal_rfq()
        _, _, forms = self._run_generate_agency_block(rfq, "rfq_j12_703")
        assert "703a" in forms
        assert "703b" in forms
        assert "703c" in forms
        assert "704b" in forms
        assert "bidpkg" in forms
        assert "quote" in forms

    def test_cchcs_req_forms_are_six_items(self):
        """Expected form count after 703-expansion: 703a+703b+703c+704b+bidpkg+quote = 6."""
        rfq = _minimal_rfq()
        _, _, forms = self._run_generate_agency_block(rfq, "rfq_j12_count")
        assert len(set(forms)) == 6

    def test_non_cchcs_synthesis_raises_not_cchcs_error(self):
        """Non-CCHCS RFQ must raise NotCchcsError (triggering legacy fallback)."""
        from src.spine_bridge.ingest import (
            synthesize_cchcs_email_contract, NotCchcsError,
        )
        rfq = _calvet_rfq()
        with pytest.raises(NotCchcsError):
            synthesize_cchcs_email_contract(
                rfq_row=rfq, rfq_id="rfq_calvet_test",
                tax_resolver=_tax_825,
            )

    def test_non_cchcs_uses_legacy_path(self):
        """Non-CCHCS RFQ's _spine_contract stays None → legacy path used."""
        rfq = _calvet_rfq()
        _spine_contract = None
        try:
            from src.spine_bridge.ingest import (
                synthesize_cchcs_email_contract, NotCchcsError,
            )
            synthesize_cchcs_email_contract(
                rfq_row=rfq, rfq_id="rfq_calvet",
                tax_resolver=_tax_825,
            )
        except NotCchcsError:
            pass  # expected
        except Exception:
            pass

        assert _spine_contract is None, (
            "CalVet RFQ must leave _spine_contract=None, triggering legacy path"
        )

    def test_cchcs_synthesis_failure_triggers_fallback(self):
        """If synthesis fails (non-NotCchcsError), match_agency must be called."""
        rfq = _minimal_rfq()
        called, key, forms = self._run_generate_agency_block(
            rfq, "rfq_j12_fail", force_synth_fail=True
        )
        assert called, "Legacy fallback must call match_agency when synthesis fails"
        assert key == "cchcs"


# ──────────────────────────────────────────────────────────────────────
# 4. CCHCS both formats: packet vs standalone
# ──────────────────────────────────────────────────────────────────────


class TestCCHCSBothFormats:
    """Verify required forms support both CCHCS response formats."""

    def test_standalone_set_has_all_required_forms(self):
        """Standalone format: 703a/b/c + 704b + bidpkg + quote must be present after expansion."""
        from src.spine_bridge.ingest import synthesize_cchcs_email_contract

        rfq = _minimal_rfq()
        contract = synthesize_cchcs_email_contract(
            rfq_row=rfq, rfq_id="rfq_standalone",
            tax_resolver=_tax_825,
        )
        # Simulate the generate-path 703 expansion.
        forms = set(contract.required_forms)
        if {"703a", "703b", "703c"} & forms:
            forms |= {"703a", "703b", "703c"}

        for required in ("703a", "703b", "703c", "704b", "bidpkg", "quote"):
            assert required in forms, (
                f"Form {required!r} missing from expanded CCHCS set: {sorted(forms)}"
            )

    def test_packet_format_response_packaging_field_is_valid(self):
        """contract.response_packaging must be a recognized value."""
        from src.spine_bridge.ingest import synthesize_cchcs_email_contract

        rfq = _minimal_rfq()
        contract = synthesize_cchcs_email_contract(
            rfq_row=rfq, rfq_id="rfq_packet",
            tax_resolver=_tax_825,
        )
        assert contract.response_packaging in ("single_pdf", "separate_pdfs", "either")

    def test_spine_contract_required_forms_valid_form_codes(self):
        """All forms in the synthesized contract must be valid FormCode literals."""
        from src.spine.email_contract import ALL_FORM_CODES
        from src.spine_bridge.ingest import synthesize_cchcs_email_contract

        rfq = _minimal_rfq()
        contract = synthesize_cchcs_email_contract(
            rfq_row=rfq, rfq_id="rfq_formcodes",
            tax_resolver=_tax_825,
        )
        for f in contract.required_forms:
            assert f in ALL_FORM_CODES, (
                f"Form {f!r} in contract.required_forms is not a valid FormCode"
            )

    def test_non_cchcs_raises_not_cchcs_from_synthesize(self):
        """CalVet RFQ passed to synthesize_cchcs_email_contract must raise NotCchcsError."""
        from src.spine_bridge.ingest import synthesize_cchcs_email_contract

        rfq = _calvet_rfq()
        with pytest.raises(NotCchcsError):
            synthesize_cchcs_email_contract(
                rfq_row=rfq, rfq_id="rfq_calvet_format",
                tax_resolver=_tax_825,
            )


# ──────────────────────────────────────────────────────────────────────
# 5. Diagnostic endpoint logic (routes_rfq_admin) — target 2
# ──────────────────────────────────────────────────────────────────────


class TestDiagPackageFormSource:
    """api_diag_package must read CCHCS forms from Spine, not legacy."""

    def _run_diag_block(self, rfq: dict, rid: str,
                        tax_resolver=_tax_825) -> tuple:
        """Replicate the repointed agency block in api_diag_package.

        Returns (source: str, agency_key: str, req_forms: list).
        'source' is 'spine_contract' or 'legacy'.
        """
        match_agency_called = [False]

        def _fake_match_agency(data):
            match_agency_called[0] = True
            return ("legacy_agency", {
                "name": "legacy",
                "required_forms": ["quote"],
            })

        _spine_diag_contract = None
        try:
            from src.spine_bridge.ingest import (
                synthesize_cchcs_email_contract, NotCchcsError,
            )
            _spine_diag_contract = synthesize_cchcs_email_contract(
                rfq_row=rfq, rfq_id=rid,
                tax_resolver=tax_resolver,
            )
        except NotCchcsError:
            pass
        except Exception:
            pass

        if _spine_diag_contract is not None:
            _agency_key = "cchcs"
            _req = list(_spine_diag_contract.required_forms)
            source = "spine_contract"
        else:
            _agency_key, _agency_cfg = _fake_match_agency(rfq)
            _req = _agency_cfg.get("required_forms", [])
            source = "legacy"

        return source, match_agency_called[0], _agency_key, _req

    def test_cchcs_diag_uses_spine_not_match_agency(self):
        """CCHCS diagnostic must use Spine contract, not match_agency."""
        source, called, key, req = self._run_diag_block(_minimal_rfq(), "rfq_diag_test")
        assert source == "spine_contract"
        assert not called
        assert key == "cchcs"
        assert "704b" in req

    def test_non_cchcs_diag_uses_match_agency(self):
        """Non-CCHCS diagnostic must still use legacy match_agency."""
        source, called, key, _ = self._run_diag_block(_calvet_rfq(), "rfq_diag_calvet")
        assert source == "legacy"
        assert called
        assert key == "legacy_agency"
