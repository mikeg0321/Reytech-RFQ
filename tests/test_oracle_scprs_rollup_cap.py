"""PR-F — flagged SCPRS rollup cap on get_pricing.

Phase 1.5-B real bind. When `ORACLE_USE_SCPRS_ROLLUP=1` AND the rollup
has count >= MIN_SAMPLES (default 5), the recommendation's
`quote_price` gets capped at the rollup's p75 (configurable). The cap
is monotonic — it can only lower, never raise.

Semantic justification: *"75% of comparable POs awarded at $X or less,
so we shouldn't bid above that."* This is the safest possible
binding — no behavior change when the flag is off; cap-only when on.

This file pins:
  1. Flag default = OFF (no behavior change in prod until Mike opts in)
  2. Flag aliases (1, true, yes, on)
  3. Min-samples gate (sparse rollups don't cap)
  4. Cap is monotonic (rec at/below cap stays as-is)
  5. Markup_pct re-derived against capped price
  6. caps_applied audit record present
  7. quote_price_pre_cap captures original
  8. Env-configurable percentile (p50 / p75 / p90)
  9. Defense: missing percentile / bad data / no rec → no-op
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _build_result(quote_price=100.0, rollup_data=None):
    """Stage a get_pricing-shaped result dict for cap testing."""
    return {
        "description": "test",
        "quantity": 10,
        "recommendation": {
            "quote_price": quote_price,
            "markup_pct": 50.0,
        },
        "scprs_rollup": rollup_data,
    }


# ── Flag gating ──────────────────────────────────────────────────


def test_cap_flag_default_off(monkeypatch):
    """Default behavior: flag not set → cap NEVER fires, no matter
    what the rollup says. This is the prod-safety invariant."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    # Cap did NOT fire — quote_price unchanged, no caps_applied
    assert result["recommendation"]["quote_price"] == 100.0
    assert "quote_price_pre_cap" not in result["recommendation"]
    assert "caps_applied" not in result["recommendation"]


def test_cap_flag_truthy_aliases_enable(monkeypatch):
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    for v in ("1", "true", "yes", "on", "TRUE", "Yes"):
        monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", v)
        assert _scprs_rollup_cap_enabled(), f"{v!r} should enable the cap"


def test_cap_flag_falsey_aliases_disable(monkeypatch):
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_enabled
    for v in ("0", "false", "no", "off", "", "  ", "maybe"):
        monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", v)
        assert not _scprs_rollup_cap_enabled(), f"{v!r} must NOT enable"


# ── Min-samples gate ─────────────────────────────────────────────


def test_cap_does_not_fire_when_samples_below_threshold(monkeypatch):
    """A rollup with count=3 (below default 5) is too thin to act on —
    cap must NOT fire even with the flag on."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 3, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    assert result["recommendation"]["quote_price"] == 100.0
    assert "caps_applied" not in result["recommendation"]


def test_cap_min_samples_env_override(monkeypatch):
    """Operator can tighten/loosen the threshold via env."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    monkeypatch.setenv("ORACLE_SCPRS_ROLLUP_MIN_SAMPLES", "2")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 3, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    # With MIN_SAMPLES=2, count=3 is enough — cap fires
    assert result["recommendation"]["quote_price"] == 60.0
    assert result["recommendation"]["quote_price_pre_cap"] == 100.0


# ── Cap monotonicity (can only lower) ────────────────────────────


def test_cap_lowers_quote_price_when_above_p75(monkeypatch):
    """The main path: rec is $100, rollup p75 is $60, count=50, flag on
    → quote_price drops to $60."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    assert result["recommendation"]["quote_price"] == 60.0
    assert result["recommendation"]["quote_price_pre_cap"] == 100.0


def test_cap_leaves_lower_quote_price_alone(monkeypatch):
    """Rec is $40, rollup p75 is $60 → cap does NOT fire (would RAISE).
    Monotonicity invariant: cap is one-way."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=40.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=30.0)
    assert result["recommendation"]["quote_price"] == 40.0
    assert "caps_applied" not in result["recommendation"]
    assert "quote_price_pre_cap" not in result["recommendation"]


def test_cap_no_op_when_quote_price_equals_cap(monkeypatch):
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=60.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=40.0)
    assert result["recommendation"]["quote_price"] == 60.0
    assert "caps_applied" not in result["recommendation"]


# ── Markup re-derivation ─────────────────────────────────────────


def test_cap_recalculates_markup_pct_when_fires(monkeypatch):
    """When cap fires, markup_pct must reflect the NEW price vs cost.
    Old: cost=50, price=100, markup=100%. New: cost=50, price=60,
    markup=20%."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    result["recommendation"]["markup_pct"] = 100.0   # stale
    _apply_scprs_rollup_cap(result, cost=50.0)
    assert result["recommendation"]["markup_pct"] == 20.0


def test_cap_leaves_markup_alone_when_cost_missing(monkeypatch):
    """No cost → can't re-derive markup. Leave it alone rather than
    invent a number."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    result["recommendation"]["markup_pct"] = 100.0
    _apply_scprs_rollup_cap(result, cost=None)
    # Price did get capped, but markup left alone
    assert result["recommendation"]["quote_price"] == 60.0
    assert result["recommendation"]["markup_pct"] == 100.0


# ── caps_applied audit record ────────────────────────────────────


def test_cap_audit_record_populated(monkeypatch):
    """caps_applied list must include source, percentile, prices,
    match_key, and sample_count for traceability."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "16-N8MMPA",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    caps = result["recommendation"].get("caps_applied", [])
    assert len(caps) == 1
    cap = caps[0]
    assert cap["source"] == "scprs_rollup"
    assert cap["percentile"] == "p75"
    assert cap["cap_price"] == 60.0
    assert cap["pre_cap_price"] == 100.0
    assert cap["match_key"] == "16-N8MMPA"
    assert cap["match_key_type"] == "mfg"
    assert cap["sample_count"] == 50


# ── Percentile env override ──────────────────────────────────────


def test_cap_percentile_env_override_p50(monkeypatch):
    """Tighter cap: p50 instead of p75. Useful when Mike wants to
    aggressively beat market median."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    monkeypatch.setenv("ORACLE_SCPRS_ROLLUP_PERCENTILE", "p50")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p50": 45.0, "p75": 60.0, "p90": 80.0,
                     "match_key": "X-1", "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=30.0)
    assert result["recommendation"]["quote_price"] == 45.0
    assert result["recommendation"]["caps_applied"][0]["percentile"] == "p50"


def test_cap_percentile_env_override_p90(monkeypatch):
    """Looser cap: p90. Don't bid above what 90% of awards landed at."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    monkeypatch.setenv("ORACLE_SCPRS_ROLLUP_PERCENTILE", "p90")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p50": 45.0, "p75": 60.0, "p90": 80.0,
                     "match_key": "X-1", "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=30.0)
    assert result["recommendation"]["quote_price"] == 80.0


def test_cap_percentile_invalid_falls_back_to_p75(monkeypatch):
    from src.core.pricing_oracle_v2 import _scprs_rollup_cap_percentile
    monkeypatch.setenv("ORACLE_SCPRS_ROLLUP_PERCENTILE", "garbage")
    assert _scprs_rollup_cap_percentile() == "p75"


# ── Defensive edges ──────────────────────────────────────────────


def test_cap_no_op_when_rollup_is_none(monkeypatch):
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(quote_price=100.0, rollup_data=None)
    _apply_scprs_rollup_cap(result, cost=50.0)
    assert result["recommendation"]["quote_price"] == 100.0
    assert "caps_applied" not in result["recommendation"]


def test_cap_no_op_when_percentile_missing(monkeypatch):
    """Rollup with count=50 but no p75 value → can't cap on a None."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=100.0,
        rollup_data={"count": 50, "p75": None, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    assert result["recommendation"]["quote_price"] == 100.0


def test_cap_no_op_when_no_quote_price(monkeypatch):
    """If the recommendation has no quote_price (oracle bailed earlier),
    there's nothing to cap."""
    from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    result = _build_result(
        quote_price=None,
        rollup_data={"count": 50, "p75": 60.0, "match_key": "X-1",
                     "match_key_type": "mfg"},
    )
    _apply_scprs_rollup_cap(result, cost=50.0)
    assert result["recommendation"]["quote_price"] is None


# ── End-to-end through get_pricing ───────────────────────────────


def test_get_pricing_caps_when_flag_on_and_rollup_dense(monkeypatch):
    """Integration: get_pricing's recommendation.quote_price drops to
    p75 when the full pipeline runs with the flag on."""
    from src.core import pricing_oracle_v2 as oracle
    monkeypatch.setenv("ORACLE_USE_SCPRS_ROLLUP", "1")
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = {
            "match_key_type": "mfg", "match_key": "X-1", "agency": "cchcs",
            "year": "*", "qty_band": "10-49", "count": 50,
            "mean": 50.0, "p50": 45.0, "p75": 60.0, "p90": 80.0,
            "updated_at": "2026-05-13",
        }
        # Pre-stage: get_pricing's recommendation comes back with a
        # high quote_price from the ad-hoc path; the cap should pull
        # it down. We patch _calculate_recommendation to control the
        # pre-cap value deterministically.
        with patch.object(oracle, "_calculate_recommendation") as mock_rec:
            mock_rec.return_value = {
                "quote_price": 100.0,
                "markup_pct": 100.0,
                "strategies": [],
                "tiers": [],
            }
            r = oracle.get_pricing(
                description="Bandage",
                quantity=20,
                cost=50.0,
                mfg_number="X-1",
                department="cchcs",
            )
    assert r["recommendation"]["quote_price"] == 60.0
    assert r["recommendation"]["quote_price_pre_cap"] == 100.0
    assert r["recommendation"]["markup_pct"] == 20.0   # (60-50)/50*100


def test_get_pricing_does_not_cap_when_flag_off(monkeypatch):
    """Default behavior: get_pricing's recommendation is unchanged when
    the flag is off, even if the rollup data would have capped it."""
    from src.core import pricing_oracle_v2 as oracle
    monkeypatch.delenv("ORACLE_USE_SCPRS_ROLLUP", raising=False)
    with patch("src.agents.scprs_price_stats.lookup_price_stat") as mock_lookup:
        mock_lookup.return_value = {
            "match_key_type": "mfg", "match_key": "X-1", "agency": "cchcs",
            "year": "*", "qty_band": "10-49", "count": 50,
            "mean": 50.0, "p50": 45.0, "p75": 60.0, "p90": 80.0,
            "updated_at": "2026-05-13",
        }
        with patch.object(oracle, "_calculate_recommendation") as mock_rec:
            mock_rec.return_value = {
                "quote_price": 100.0,
                "markup_pct": 100.0,
                "strategies": [],
                "tiers": [],
            }
            r = oracle.get_pricing(
                description="Bandage",
                quantity=20,
                cost=50.0,
                mfg_number="X-1",
                department="cchcs",
            )
    assert r["recommendation"]["quote_price"] == 100.0
    assert "caps_applied" not in r["recommendation"]
