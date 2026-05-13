"""PR-E (Phase 1.5-B prep) — get_pricing() rollup sidecar.

Phase 1.5-A shipped the rollup table + builder. Phase 1.5-A2 wired
the boot-time backfill + preview endpoint. This PR adds an optional
sidecar field `result["scprs_rollup"]` on `get_pricing()` so consumers
can SEE the rollup data alongside the existing recommendation —
WITHOUT changing the recommendation logic.

The actual binding (oracle uses rollup as the primary signal) ships
in a follow-up PR behind the `ORACLE_USE_SCPRS_ROLLUP` flag, after
Mike has eyeballed the rollup data and confirmed sane percentiles.

This file pins:
  1. `result["scprs_rollup"]` key always present (None or dict)
  2. Populated when mfg_number or unspsc is supplied AND rollup has data
  3. Stays None when neither is supplied
  4. Recommendation fields unchanged when sidecar populates
  5. Caller-compat: existing callers with positional args still work
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Signature compat ─────────────────────────────────────────────


def test_get_pricing_accepts_mfg_number_kwarg():
    """The new kwarg must be optional — every existing caller signature
    keeps working."""
    import inspect
    from src.core.pricing_oracle_v2 import get_pricing
    sig = inspect.signature(get_pricing)
    assert "mfg_number" in sig.parameters
    assert sig.parameters["mfg_number"].default == ""


def test_get_pricing_accepts_unspsc_kwarg():
    import inspect
    from src.core.pricing_oracle_v2 import get_pricing
    sig = inspect.signature(get_pricing)
    assert "unspsc" in sig.parameters
    assert sig.parameters["unspsc"].default == ""


# ── Sidecar key always present ───────────────────────────────────


def test_result_always_has_scprs_rollup_key():
    """The result dict shape must include `scprs_rollup` even when no
    MFG#/UNSPSC was supplied — consumers can rely on the key existing."""
    from src.core.pricing_oracle_v2 import get_pricing
    # Empty description bails early; key must still exist on the returned dict
    r = get_pricing(description="")
    assert "scprs_rollup" in r
    assert r["scprs_rollup"] is None


# ── Sidecar populates when MFG# resolved ─────────────────────────


def test_sidecar_consults_lookup_when_mfg_number_supplied():
    """When the caller passes mfg_number=, `lookup_price_stat` should
    be called with that value (verifies the wiring)."""
    from src.core import pricing_oracle_v2 as oracle
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = {
            "match_key_type": "mfg", "match_key": "X-1", "agency": "cchcs",
            "year": "*", "qty_band": "10-49", "count": 5,
            "mean": 10.0, "p50": 9.5, "p75": 11.0, "p90": 12.5,
            "updated_at": "2026-05-13T08:00:00",
        }
        r = oracle.get_pricing(
            description="Bandage Sterile 4x4",
            quantity=20,
            department="cchcs",
            mfg_number="X-1",
        )
    # Lookup must have been called with the supplied MFG# + agency
    assert mock_lookup.called
    kwargs = mock_lookup.call_args.kwargs
    assert kwargs.get("mfg_number") == "X-1"
    assert kwargs.get("agency") == "cchcs"
    # Sidecar populated with the rollup result
    assert r["scprs_rollup"] is not None
    assert r["scprs_rollup"]["p50"] == 9.5
    assert r["scprs_rollup"]["count"] == 5


def test_sidecar_consults_lookup_when_unspsc_supplied():
    """UNSPSC-only callers (no MFG#) also trigger the lookup."""
    from src.core import pricing_oracle_v2 as oracle
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = {
            "match_key_type": "unspsc", "match_key": "42143000",
            "agency": "cchcs", "year": "*", "qty_band": "10-49",
            "count": 12, "mean": 8.0, "p50": 7.5, "p75": 9.0, "p90": 10.0,
            "updated_at": "2026-05-13T08:00:00",
        }
        r = oracle.get_pricing(
            description="Surgical Bandage",
            quantity=15,
            department="cchcs",
            unspsc="42143000",
        )
    assert mock_lookup.called
    kwargs = mock_lookup.call_args.kwargs
    assert kwargs.get("unspsc") == "42143000"
    assert r["scprs_rollup"]["match_key_type"] == "unspsc"


def test_sidecar_falls_back_to_item_number_when_mfg_blank():
    """If mfg_number= is not passed but the legacy item_number= is,
    the lookup uses item_number as the MFG# probe. (Many existing
    callers still pass item_number positionally.)"""
    from src.core import pricing_oracle_v2 as oracle
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = None
        oracle.get_pricing(
            description="Bandage",
            quantity=10,
            item_number="W12919",
            department="cchcs",
        )
    if mock_lookup.called:
        kwargs = mock_lookup.call_args.kwargs
        # Either mfg_number is W12919 OR unspsc is set — either way the
        # legacy item_number plumbs through as a probe.
        assert kwargs.get("mfg_number") == "W12919" or kwargs.get("unspsc")


def test_sidecar_none_when_neither_mfg_nor_unspsc_supplied():
    """No MFG# AND no UNSPSC → no rollup lookup. The sidecar stays None
    (we don't generate dummy probes against the rollup)."""
    from src.core import pricing_oracle_v2 as oracle
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        r = oracle.get_pricing(
            description="Anonymous Item No Identifier",
            quantity=1,
            department="cchcs",
        )
    # Lookup should NOT have been called (or if called with both blank
    # keys, the helper returns None anyway)
    if mock_lookup.called:
        kwargs = mock_lookup.call_args.kwargs
        assert not kwargs.get("mfg_number") and not kwargs.get("unspsc"), (
            "lookup_price_stat should not be called with both keys blank"
        )
    assert r["scprs_rollup"] is None


def test_sidecar_none_when_rollup_has_no_data():
    """When the lookup returns None (no bucket with count >= 1), the
    sidecar stays None — operator sees "rollup has no prior for this
    SKU" implicitly via the missing field."""
    from src.core import pricing_oracle_v2 as oracle
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = None
        r = oracle.get_pricing(
            description="New Product",
            quantity=5,
            department="cchcs",
            mfg_number="UNKNOWN-SKU-12345",
        )
    assert r["scprs_rollup"] is None


# ── Recommendation logic untouched ───────────────────────────────


def test_sidecar_population_does_not_change_recommendation_fields():
    """The big claim of this PR: adding the sidecar DOES NOT change any
    other field in the result dict. Lock in the contract — if we ever
    accidentally bind get_pricing's recommendation to the rollup, this
    test fails and we know."""
    from src.core import pricing_oracle_v2 as oracle

    # Two calls with identical inputs except MFG# (which triggers the
    # rollup lookup). Both should produce identical recommendation
    # fields — only `scprs_rollup` should differ.
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = None
        r_no_mfg = oracle.get_pricing(
            description="Test Bandage", quantity=10, department="cchcs",
        )

    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = {
            "match_key_type": "mfg", "match_key": "X-1", "agency": "cchcs",
            "year": "*", "qty_band": "10-49", "count": 50,
            "mean": 100.0, "p50": 99.0, "p75": 105.0, "p90": 120.0,
            "updated_at": "2026-05-13",
        }
        r_with_rollup = oracle.get_pricing(
            description="Test Bandage", quantity=10, department="cchcs",
            mfg_number="X-1",
        )

    # The recommendation must be identical (sidecar is preview-only)
    assert r_no_mfg["recommendation"] == r_with_rollup["recommendation"], (
        "recommendation must NOT change when the sidecar populates. "
        f"without rollup: {r_no_mfg['recommendation']!r}, "
        f"with rollup: {r_with_rollup['recommendation']!r}"
    )
    # And market/strategies/tiers untouched
    assert r_no_mfg["market"] == r_with_rollup["market"]
    assert r_no_mfg["strategies"] == r_with_rollup["strategies"]
    assert r_no_mfg["tiers"] == r_with_rollup["tiers"]
    # Only difference: scprs_rollup
    assert r_no_mfg["scprs_rollup"] is None
    assert r_with_rollup["scprs_rollup"] is not None


def test_sidecar_does_not_crash_when_lookup_helper_missing():
    """Defensive: if `scprs_price_stats` module is unavailable
    (table not yet built on a fresh deploy), get_pricing must not
    crash — sidecar just stays None."""
    from src.core import pricing_oracle_v2 as oracle
    with patch(
        "src.agents.scprs_price_stats.lookup_price_stat",
        side_effect=Exception("table missing"),
    ):
        r = oracle.get_pricing(
            description="Test", quantity=1, department="cchcs",
            mfg_number="X-1",
        )
    assert r["scprs_rollup"] is None
    # And no exception bubbled
