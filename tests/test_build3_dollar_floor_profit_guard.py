"""BUILD-3 P0 regression guards — dollar-floor profit guard.

Context: `_calculate_recommendation` has a percent-based floor at
`cost * 1.15`. On cheap items that produces pennies of gross profit —
a $0.80 swab at 15% markup = $0.12/unit × 2 qty = $0.24 total GP for
the quote line. Less than the cost of a round-trip email. We must
never recommend a line that isn't worth quoting.

BUILD-3 added `_apply_dollar_floor(result, cost, qty)` and calls it
before both recommendation return sites (the win-anchor early return
and the final return). It bumps the price up to clear a minimum
gross-profit floor in absolute dollars (default $3, overridable via
`get_flag("oracle.min_gross_profit_dollars")`).

These guards lock: constant default, no-op conditions (no cost,
no qty, no price, thick margin), bump conditions (skinny margin),
idempotency, feature-flag gating, both return sites wired.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
ORACLE_PATH = ROOT / "src" / "core" / "pricing_oracle_v2.py"


# ── Pure-helper tests ────────────────────────────────────────────────────────

def test_default_floor_constant():
    from src.core.pricing_oracle_v2 import _DOLLAR_FLOOR_DEFAULT
    assert _DOLLAR_FLOOR_DEFAULT == 3.0, (
        "BUILD-3: default min gross profit floor must be $3.00 — matches "
        "the branch name and the audit's justification. Changing the "
        "default is a policy change, not a refactor."
    )


def test_noop_when_cost_zero():
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 1.00, "markup_pct": 15, "rationale": "x"}
    _apply_dollar_floor(result, cost=0, qty=5)
    assert result["quote_price"] == 1.00
    assert "dollar_floor_applied" not in result


def test_noop_when_cost_none():
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 1.00, "markup_pct": 15, "rationale": "x"}
    _apply_dollar_floor(result, cost=None, qty=5)
    assert result["quote_price"] == 1.00
    assert "dollar_floor_applied" not in result


def test_noop_when_qty_zero():
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 1.00, "markup_pct": 15, "rationale": "x"}
    _apply_dollar_floor(result, cost=0.50, qty=0)
    assert result["quote_price"] == 1.00
    assert "dollar_floor_applied" not in result


def test_noop_when_quote_price_none():
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": None, "markup_pct": None, "rationale": "nodata"}
    _apply_dollar_floor(result, cost=10.0, qty=3)
    assert result["quote_price"] is None
    assert "dollar_floor_applied" not in result


def test_noop_when_already_above_floor():
    """A $10 cost × 50% markup on qty=5 = $25 GP. That clears a $3 floor
    by a mile, so the helper must not touch the price."""
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 15.00, "markup_pct": 50, "rationale": "fat"}
    _apply_dollar_floor(result, cost=10.0, qty=5)
    assert result["quote_price"] == 15.00
    assert "dollar_floor_applied" not in result


def test_bumps_skinny_margin_cheap_item():
    """$0.80 cost at 15% markup × qty=2 = $0.24 GP. Floor is $3. Helper
    must bump to cost + $3/qty = $0.80 + $1.50 = $2.30."""
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 0.92, "markup_pct": 15,
              "rationale": "15% floor"}
    _apply_dollar_floor(result, cost=0.80, qty=2)
    assert result["quote_price"] == pytest.approx(2.30)
    # New markup = (2.30 - 0.80) / 0.80 = 187.5%
    assert result["markup_pct"] == pytest.approx(187.5)
    assert "dollar_floor_applied" in result
    ap = result["dollar_floor_applied"]
    assert ap["original_price"] == pytest.approx(0.92)
    assert ap["floor_price"] == pytest.approx(2.30)
    assert ap["min_gross_profit"] == pytest.approx(3.00)
    assert ap["original_gp_total"] == pytest.approx(0.24)
    assert ap["qty"] == 2
    # rationale must be extended, not replaced
    assert "15% floor" in result["rationale"]
    assert "Bumped to" in result["rationale"]


def test_bumps_right_at_boundary():
    """cost=1.00, qty=1, quote_price=3.99 → GP=$2.99 (below $3 floor).
    Must bump to exactly $4.00."""
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 3.99, "markup_pct": 299, "rationale": "x"}
    _apply_dollar_floor(result, cost=1.00, qty=1)
    assert result["quote_price"] == pytest.approx(4.00)


def test_idempotent():
    """Running the guard twice must leave the same result — the second
    pass sees a price that already clears the floor and no-ops."""
    from src.core.pricing_oracle_v2 import _apply_dollar_floor
    result = {"quote_price": 0.92, "markup_pct": 15,
              "rationale": "15% floor"}
    _apply_dollar_floor(result, cost=0.80, qty=2)
    first = dict(result["dollar_floor_applied"])
    price_after_first = result["quote_price"]
    # Second pass
    _apply_dollar_floor(result, cost=0.80, qty=2)
    assert result["quote_price"] == price_after_first
    # dollar_floor_applied block is unchanged (no re-bump)
    assert result["dollar_floor_applied"] == first


def test_feature_flag_can_disable(monkeypatch):
    """Ops can kill the guard at runtime via get_flag('oracle.dollar_floor')
    without a code deploy. Regression: a hard-coded bypass that can't be
    toggled."""
    from src.core import flags as flag_mod
    from src.core import pricing_oracle_v2 as oracle

    def _fake_get_flag(key, default=None):
        if key == "oracle.dollar_floor":
            return False
        return default

    monkeypatch.setattr(flag_mod, "get_flag", _fake_get_flag)
    # Re-import path: helper imports get_flag locally, so patching the
    # module attribute is sufficient.
    result = {"quote_price": 0.92, "markup_pct": 15, "rationale": "x"}
    oracle._apply_dollar_floor(result, cost=0.80, qty=2)
    assert result["quote_price"] == pytest.approx(0.92)
    assert "dollar_floor_applied" not in result


def test_feature_flag_can_override_floor_amount(monkeypatch):
    """Ops can raise the floor (e.g., $10) via
    get_flag('oracle.min_gross_profit_dollars'). A line at $3 GP must
    then bump to clear $10."""
    from src.core import flags as flag_mod
    from src.core import pricing_oracle_v2 as oracle

    def _fake_get_flag(key, default=None):
        if key == "oracle.dollar_floor":
            return True
        if key == "oracle.min_gross_profit_dollars":
            return 10.0
        return default

    monkeypatch.setattr(flag_mod, "get_flag", _fake_get_flag)
    # cost=1, qty=1, quote=$4 → GP=$3 < $10, must bump to $11
    result = {"quote_price": 4.00, "markup_pct": 300, "rationale": "x"}
    oracle._apply_dollar_floor(result, cost=1.00, qty=1)
    assert result["quote_price"] == pytest.approx(11.00)
    assert result["dollar_floor_applied"]["min_gross_profit"] == pytest.approx(10.0)


def test_zero_or_negative_floor_disables(monkeypatch):
    """If an operator sets the floor to 0 (or negative via misconfig),
    the helper must no-op instead of rounding to a strange price."""
    from src.core import flags as flag_mod
    from src.core import pricing_oracle_v2 as oracle

    def _fake_get_flag(key, default=None):
        if key == "oracle.dollar_floor":
            return True
        if key == "oracle.min_gross_profit_dollars":
            return 0.0
        return default

    monkeypatch.setattr(flag_mod, "get_flag", _fake_get_flag)
    result = {"quote_price": 0.92, "markup_pct": 15, "rationale": "x"}
    oracle._apply_dollar_floor(result, cost=0.80, qty=2)
    assert result["quote_price"] == pytest.approx(0.92)
    assert "dollar_floor_applied" not in result


# ── Source-level guards — both return sites must be wired ────────────────────

def test_helper_called_at_win_anchor_return():
    """Regression: a refactor that removes the win-anchor call site would
    leave $0.24-GP lines shipping through the 'we won this before'
    branch. That's the branch that bypasses *everything* else, so it
    MUST have the guard."""
    src = ORACLE_PATH.read_text(encoding="utf-8")
    # Find the win-anchor block — ends with `Won Nx at ...` rationale.
    m = re.search(
        r"Won \{win_times\}x at.*?return result",
        src, re.DOTALL,
    )
    assert m, "BUILD-3: win-anchor return block not found — structure changed"
    block = m.group(0)
    assert "_apply_dollar_floor(result, cost, qty)" in block, (
        "BUILD-3: win-anchor branch must call _apply_dollar_floor before "
        "its early return — otherwise the guard is bypassed on every "
        "previously-won item"
    )


def test_helper_called_at_final_return():
    """The final return is the has_cost+has_market / has_cost-only /
    has_market-only path. All three MUST be guarded."""
    src = ORACLE_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"No data\. Manual research needed.*?return result",
        src, re.DOTALL,
    )
    assert m, "BUILD-3: final return block not found — structure changed"
    block = m.group(0)
    assert "_apply_dollar_floor(result, cost, qty)" in block, (
        "BUILD-3: final return must call _apply_dollar_floor — covers the "
        "has_cost+has_market and has_cost-only recommendation branches"
    )


def test_both_call_sites_exist():
    """Exactly two call sites — if a future refactor adds a third return
    inside _calculate_recommendation, this test fires and the author
    knows to wire it."""
    src = ORACLE_PATH.read_text(encoding="utf-8")
    calls = re.findall(r"_apply_dollar_floor\(", src)
    # helper definition + 2 call sites = 3 occurrences
    assert len(calls) == 3, (
        f"BUILD-3: expected 3 _apply_dollar_floor references (1 def + 2 "
        f"call sites), found {len(calls)}. If you added a new return "
        f"branch in _calculate_recommendation, it MUST call the guard."
    )
