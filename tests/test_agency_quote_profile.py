"""
Tests for the 2026-04-19 CalVet quote profile fallback (Batch B).

Background: pricing_oracle_v2 only surfaced an `institution_profile` block
when institution_pricing_profile had >= _CAL_MIN_SAMPLES (5) real won
quotes. CCHCS had enough history; CalVet, DSH, and every smaller agency
had a silently blank panel.

This batch adds resolve_agency_profile(agency) — a static fallback that
returns agency_config-derived defaults (markup target, sensitivity guess,
payment/shipping terms) tagged with source='agency_config_default' so
the UI can label it appropriately. Wired into pricing_oracle_v2 so the
fallback fires automatically whenever the DB profile is missing/thin.
"""
from __future__ import annotations

import pytest

from src.core.agency_quote_profile import resolve_agency_profile


class TestResolveAgencyProfile:
    def test_calvet_returns_profile_with_defaults(self):
        p = resolve_agency_profile("calvet")
        assert p is not None
        assert p["institution"] == "calvet"
        assert p["source"] == "agency_config_default"
        assert p["avg_winning_markup"] == 25.0
        assert p["payment_terms"] == "Net 30"
        assert p["total_quotes"] == 0
        assert p["win_rate"] is None

    def test_cchcs_resolved_via_direct_key(self):
        p = resolve_agency_profile("cchcs")
        assert p is not None
        assert p["institution"] == "cchcs"
        assert p["payment_terms"] == "Net 45"

    def test_unknown_agency_returns_none(self):
        # An agency not in agency_config and not aliased
        assert resolve_agency_profile("ACME_CORPORATION_DOES_NOT_EXIST_42") is None

    def test_empty_agency_returns_none(self):
        assert resolve_agency_profile("") is None
        assert resolve_agency_profile(None) is None

    def test_sensitivity_inferred_from_markup(self):
        from src.core.agency_quote_profile import _infer_sensitivity
        assert _infer_sensitivity(15) == "high"   # price-sensitive
        assert _infer_sensitivity(25) == "normal"
        assert _infer_sensitivity(35) == "low"    # premium-tolerant


class TestOracleUsesFallback:
    """End-to-end: pricing_oracle_v2.get_pricing should surface a profile
    block for CalVet even with zero historical quotes."""

    def test_calvet_oracle_surfaces_fallback_profile(self):
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(
            description="Multipurpose copy paper, 8.5x11, 500 sheets",
            quantity=10,
            cost=4.50,
            department="calvet",
        )
        # institution_profile lives inside result["recommendation"] in
        # the current oracle output shape (the top-level result dict
        # only carries cost/market/strategies/etc; profile rides with rec)
        prof = (result.get("recommendation") or {}).get("institution_profile")
        assert prof is not None, "CalVet oracle must surface a fallback profile"
        # Source label tells UI it's static, not historical
        assert prof.get("source") == "agency_config_default"
        assert prof.get("institution") == "calvet"

    def test_unknown_agency_no_profile_block(self):
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(
            description="copy paper",
            quantity=1,
            cost=4.50,
            department="MARS_COLONY_DOES_NOT_EXIST",
        )
        rec = result.get("recommendation") or {}
        prof = rec.get("institution_profile")
        # Truly unknown agency must NOT get a fake profile
        assert prof is None or prof.get("source") != "agency_config_default"
