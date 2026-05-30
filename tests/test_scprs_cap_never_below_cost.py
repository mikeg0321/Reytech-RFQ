"""Regression guard: the SCPRS rollup cap must never price below cost.

Follow-up to the 2026-05-29 contamination incident. SCPRS p75 is what the
STATE paid OTHER vendors and can sit below Reytech's own cost. The cap lowered
the quote straight to p75, committing a guaranteed loss — rfq_fca653f6 L3:
a $97.95-cost line capped to $45.11. The cap now floors at cost.
"""
from __future__ import annotations

from src.core.pricing_oracle_v2 import _apply_scprs_rollup_cap


def _result(quote_price, p75, cost, count=10):
    return {
        "recommendation": {"quote_price": quote_price, "rationale": ""},
        "cost": {"locked_cost": cost} if cost else {},
        "scprs_rollup": {
            "count": count, "p50": p75, "p75": p75, "p90": p75,
            "match_key": "MFG-X", "match_key_type": "mfg_number",
        },
    }


def test_cap_floors_at_cost_when_p75_below_cost():
    """p75 below cost: cap floors at cost (break-even), never below it."""
    result = _result(quote_price=132.23, p75=45.11, cost=97.95)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 97.95, "must floor at cost, not p75"
    assert rec["quote_price_pre_cap"] == 132.23
    cap = rec["caps_applied"][0]
    assert cap["floored_at_cost"] is True
    assert cap["p75"] == 45.11
    assert "floored at cost" in rec["rationale"]


def test_cap_still_lowers_to_p75_when_p75_above_cost():
    """Normal case unchanged: p75 above cost → cap to p75."""
    result = _result(quote_price=100.0, p75=70.0, cost=50.0)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 70.0
    assert rec["caps_applied"][0]["floored_at_cost"] is False


def test_cap_is_noop_when_floor_would_raise_price():
    """If cost (the floor) is at/above the current price, a cap would RAISE
    the bid — forbidden. Leave the price untouched."""
    result = _result(quote_price=100.0, p75=45.0, cost=120.0)
    _apply_scprs_rollup_cap(result)
    rec = result["recommendation"]
    assert rec["quote_price"] == 100.0, "cap must never raise the bid"
    assert "quote_price_pre_cap" not in rec
    assert not rec.get("caps_applied")
