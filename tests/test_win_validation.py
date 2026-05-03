"""Tests for src/core/win_validation.compute_win_warnings.

Covers the three warning families from the 2026-05-03 validate-to-win
audit (Gaps #1, #2, #5). All warnings are soft — these tests pin that
the helper NEVER mutates the input and NEVER throws on missing fields.
"""
from __future__ import annotations

import pytest

from src.core.win_validation import (
    COST_VS_LAST_WON_RATIO,
    LINE_MIN_MARGIN_USD,
    LINE_MIN_MARKUP_PCT,
    QUOTE_MIN_MARGIN_PCT,
    compute_win_warnings,
)


def _item(line_no=1, cost=10.0, price=15.0, qty=1, **extra):
    return {
        "line_number": line_no,
        "description": extra.get("description", "Test Widget"),
        "part_number": extra.get("part_number", "PN-1"),
        "supplier_cost": cost,
        "unit_price": price,
        "quantity": qty,
        **{k: v for k, v in extra.items() if k not in ("description", "part_number")},
    }


# ─── Gap #2: line-level margin floor ────────────────────────────────────


class TestLineLowMargin:

    def test_high_margin_no_warning(self):
        rfq = {"line_items": [_item(cost=10.0, price=20.0)]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "line_low_margin" for w in ws)

    def test_below_markup_pct_floor_orange(self):
        # 10 → 11 = 10% markup, below 15% floor
        rfq = {"line_items": [_item(cost=10.0, price=11.0, qty=10)]}
        ws = compute_win_warnings(rfq)
        line = [w for w in ws if w["code"] == "line_low_margin"]
        assert len(line) == 1
        assert line[0]["level"] == "orange"
        assert line[0]["line_no"] == 1
        assert "markup" in line[0]["message"]
        assert line[0]["meta"]["markup_pct"] == pytest.approx(10.0)

    def test_below_absolute_margin_floor(self):
        # 50 → 51 = 2% markup AND $1 absolute margin (both below)
        rfq = {"line_items": [_item(cost=50.0, price=51.0)]}
        ws = compute_win_warnings(rfq)
        line = [w for w in ws if w["code"] == "line_low_margin"]
        assert len(line) == 1
        assert "margin" in line[0]["message"]

    def test_no_bid_skipped(self):
        rfq = {"line_items": [_item(cost=10.0, price=10.0, no_bid=True)]}
        ws = compute_win_warnings(rfq)
        assert not ws

    def test_zero_cost_skipped_silently(self):
        # Zero cost is the validator's hard-gate — win_validation should
        # not double-warn (operator already sees that error).
        rfq = {"line_items": [_item(cost=0, price=15.0)]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "line_low_margin" for w in ws)

    def test_line_with_qty_zero_still_evaluated(self):
        # Line-level warning is per-unit, not per-line-total. Qty=0
        # means operator hasn't entered qty yet, but margin
        # math still works on the unit cost vs unit price.
        rfq = {"line_items": [_item(cost=10.0, price=10.5, qty=0)]}
        ws = compute_win_warnings(rfq)
        assert any(w["code"] == "line_low_margin" for w in ws)


# ─── Gap #5: quote-level margin floor ──────────────────────────────────


class TestQuoteLowMargin:

    def test_total_margin_above_floor_no_warning(self):
        # 100 cost → 130 revenue = 30% markup, above 22%
        rfq = {"line_items": [_item(cost=100.0, price=130.0, qty=1)]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "quote_low_margin" for w in ws)

    def test_total_margin_below_floor_orange(self):
        # 100 cost → 115 revenue = 15% markup, below 22% — orange
        rfq = {"line_items": [_item(cost=100.0, price=115.0, qty=1)]}
        ws = compute_win_warnings(rfq)
        quote = [w for w in ws if w["code"] == "quote_low_margin"]
        assert len(quote) == 1
        assert quote[0]["level"] == "orange"
        assert quote[0]["line_no"] is None
        assert quote[0]["meta"]["total_markup_pct"] == pytest.approx(15.0)

    def test_quote_aggregates_qty_correctly(self):
        # 2 items, qty=10 each — totals must respect qty.
        rfq = {"line_items": [
            _item(line_no=1, cost=10.0, price=11.0, qty=10),  # 100 cost, 110 rev
            _item(line_no=2, cost=20.0, price=22.0, qty=10),  # 200 cost, 220 rev
        ]}
        # total: 300 cost, 330 rev → 10% markup → below 22%
        ws = compute_win_warnings(rfq)
        quote = [w for w in ws if w["code"] == "quote_low_margin"]
        assert len(quote) == 1
        assert quote[0]["meta"]["total_cost"] == pytest.approx(300.0)
        assert quote[0]["meta"]["total_revenue"] == pytest.approx(330.0)

    def test_no_priced_items_no_quote_warning(self):
        # If no items have both cost and price, skip the quote-level rollup
        # silently — operator hasn't priced anything yet.
        rfq = {"line_items": [_item(cost=10.0, price=0)]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "quote_low_margin" for w in ws)


# ─── Gap #1: cost vs last-won (caller pre-resolves) ─────────────────────


class TestCostAboveLastWon:
    """Caller (route) does the DB lookup and stuffs `last_won_price` +
    `last_won_quote` onto each item. This module only checks the
    threshold."""

    def test_no_last_won_field_no_warning(self):
        rfq = {"line_items": [_item(cost=100.0, price=130.0)]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "cost_above_last_won" for w in ws)

    def test_zero_last_won_price_skipped(self):
        rfq = {"line_items": [_item(cost=100.0, last_won_price=0)]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "cost_above_last_won" for w in ws)

    def test_cost_within_threshold_no_warning(self):
        # last_won = $50; current cost $60 = 1.2× — under 1.5× threshold
        rfq = {"line_items": [_item(
            cost=60.0, price=80.0,
            last_won_price=50.0, last_won_quote="Q-OLD",
        )]}
        ws = compute_win_warnings(rfq)
        assert not any(w["code"] == "cost_above_last_won" for w in ws)

    def test_cost_above_threshold_red_warning(self):
        # last_won = $50; current cost $100 = 2× — above 1.5×
        rfq = {"line_items": [_item(
            cost=100.0, price=130.0,
            last_won_price=50.0, last_won_quote="Q-OLD-123",
        )]}
        ws = compute_win_warnings(rfq)
        red = [w for w in ws if w["code"] == "cost_above_last_won"]
        assert len(red) == 1
        assert red[0]["level"] == "red"
        assert red[0]["meta"]["ratio"] == pytest.approx(2.0)
        assert red[0]["meta"]["last_won_quote"] == "Q-OLD-123"

    def test_string_last_won_price_coerced(self):
        rfq = {"line_items": [_item(
            cost=100.0, last_won_price="$50.00",
            last_won_quote="Q-X",
        )]}
        ws = compute_win_warnings(rfq)
        assert any(w["code"] == "cost_above_last_won" for w in ws)


# ─── Robustness ────────────────────────────────────────────────────────


class TestRobustness:

    def test_empty_input_returns_empty_list(self):
        assert compute_win_warnings({}) == []
        assert compute_win_warnings({"line_items": []}) == []

    def test_non_list_items_returns_empty(self):
        assert compute_win_warnings({"line_items": "not a list"}) == []

    def test_non_dict_item_skipped(self):
        rfq = {"line_items": [None, "string", 42, _item(cost=10, price=15)]}
        ws = compute_win_warnings(rfq)
        # Non-dict items skipped silently; valid item processed.
        assert isinstance(ws, list)

    def test_does_not_mutate_input(self):
        rfq = {"line_items": [_item(cost=10.0, price=11.0, qty=1)]}
        before = str(rfq)
        compute_win_warnings(rfq)
        assert str(rfq) == before

    def test_string_currency_values_coerced(self):
        rfq = {"line_items": [{
            "line_number": 1,
            "supplier_cost": "$100.00",
            "unit_price": "$110",
            "quantity": "1",
            "description": "x",
        }]}
        ws = compute_win_warnings(rfq)
        # Should evaluate to 10% markup → low margin
        assert any(w["code"] == "line_low_margin" for w in ws)

    def test_thresholds_exposed_for_callers(self):
        # Constants are part of the public API so UI templates can
        # reference them in tooltip copy without re-declaring magic numbers.
        assert COST_VS_LAST_WON_RATIO == 1.5
        assert LINE_MIN_MARKUP_PCT == 15.0
        assert LINE_MIN_MARGIN_USD == 2.0
        assert QUOTE_MIN_MARGIN_PCT == 22.0
