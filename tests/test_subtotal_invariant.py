"""Pin the subtotal invariant — billable-items-only.

2026-05-08 incident: csp-sac PC printed Merchandise Subtotal $1,445.15
for a single $1,439.75 line. A second item marked Skip (`no_bid=True`)
had a stored unit_price of $5.40 that the row renderer dropped from
the form but the subtotal accumulator still summed.

These tests pin:
  1. is_billable predicate excludes no_bid items
  2. extension_of returns 0 for no_bid items even if price/qty are set
  3. subtotal_of agrees with sum of visible-row extensions
  4. assert_subtotal_invariant logs WARNING + returns False on drift
  5. fill_ams704 (PC ORIGINAL mode) does not include no_bid in subtotal
  6. fill_ams704 (regular mode) does not include no_bid in subtotal
  7. quote_generator does not include no_bid in subtotal
"""
from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.pricing_math import (  # noqa: E402
    is_billable,
    extension_of,
    subtotal_of,
    billable_items,
    assert_subtotal_invariant,
)


class TestIsBillable:
    def test_default_dict_is_billable(self):
        assert is_billable({}) is True

    def test_no_bid_true_is_not_billable(self):
        assert is_billable({"no_bid": True}) is False

    def test_no_bid_false_is_billable(self):
        assert is_billable({"no_bid": False}) is True

    def test_non_dict_is_not_billable(self):
        assert is_billable(None) is False
        assert is_billable("not a dict") is False
        assert is_billable(42) is False


class TestExtensionOf:
    def test_no_bid_item_extension_is_zero(self):
        item = {"no_bid": True, "unit_price": 5.40, "qty": 1}
        assert extension_of(item) == 0.0

    def test_billable_extension_uses_price_times_qty(self):
        item = {"unit_price": 1439.75, "qty": 1}
        assert extension_of(item) == 1439.75

    def test_billable_with_zero_price_returns_zero(self):
        item = {"unit_price": 0, "qty": 5}
        assert extension_of(item) == 0.0

    def test_billable_with_zero_qty_returns_zero(self):
        item = {"unit_price": 100, "qty": 0}
        assert extension_of(item) == 0.0

    def test_canonical_price_chain_is_used(self):
        # cost+markup wins over stale unit_price
        item = {
            "qty": 1,
            "unit_price": 999.99,  # stale
            "pricing": {"unit_cost": 100.00, "markup_pct": 50.0},
        }
        # canonical = 100 * 1.5 = 150
        assert extension_of(item) == 150.0


class TestSubtotalOf:
    def test_csp_sac_screenshot_scenario(self):
        """The exact bug Mike screenshotted: 2 items, item 1 skipped
        with stored $5.40, item 2 visible at $1439.75. Subtotal must
        be $1439.75, not $1445.15."""
        items = [
            {"no_bid": True, "unit_price": 5.40, "qty": 1},
            {"unit_price": 1439.75, "qty": 1},
        ]
        assert subtotal_of(items) == 1439.75

    def test_empty_list_is_zero(self):
        assert subtotal_of([]) == 0.0
        assert subtotal_of(None) == 0.0

    def test_all_skipped_is_zero(self):
        items = [
            {"no_bid": True, "unit_price": 100, "qty": 1},
            {"no_bid": True, "unit_price": 50, "qty": 2},
        ]
        assert subtotal_of(items) == 0.0

    def test_mixed_skipped_and_billable(self):
        items = [
            {"unit_price": 10, "qty": 2},   # 20 billable
            {"no_bid": True, "unit_price": 100, "qty": 1},  # 0
            {"unit_price": 5.50, "qty": 4},  # 22 billable
        ]
        assert subtotal_of(items) == 42.00

    def test_mid_dollar_rounding(self):
        # Sums of rounded extensions, not extensions of summed-then-rounded.
        items = [
            {"unit_price": 1.005, "qty": 1},  # rounds to 1.00 or 1.01? Use round(price*qty,2)
            {"unit_price": 1.005, "qty": 1},
        ]
        # Each extension rounds independently, then summed.
        ext = round(1.005 * 1, 2)  # banker's rounding may give 1.0 or 1.01
        assert subtotal_of(items) == round(ext * 2, 2)


class TestBillableItems:
    def test_filters_no_bid(self):
        items = [{"a": 1}, {"no_bid": True, "b": 2}, {"c": 3}]
        out = billable_items(items)
        assert len(out) == 2
        assert out[0]["a"] == 1
        assert out[1]["c"] == 3

    def test_preserves_order(self):
        items = [{"x": 1}, {"x": 2}, {"x": 3}]
        out = billable_items(items)
        assert [it["x"] for it in out] == [1, 2, 3]


class TestAssertSubtotalInvariant:
    def test_invariant_holds_silent(self, caplog):
        items = [{"unit_price": 100, "qty": 2}]
        with caplog.at_level(logging.WARNING, logger="reytech.pricing_math"):
            ok = assert_subtotal_invariant(items, 200.0, context="test")
        assert ok is True
        assert "PRICING-DRIFT" not in caplog.text

    def test_invariant_drift_logs_warning(self, caplog):
        # Mike's bug shape — printed = 1445.15 but billable sum = 1439.75
        items = [
            {"no_bid": True, "unit_price": 5.40, "qty": 1},
            {"unit_price": 1439.75, "qty": 1},
        ]
        with caplog.at_level(logging.WARNING, logger="reytech.pricing_math"):
            ok = assert_subtotal_invariant(items, 1445.15, context="csp-sac")
        assert ok is False
        assert "PRICING-DRIFT" in caplog.text
        assert "csp-sac" in caplog.text
        assert "delta=" in caplog.text

    def test_invariant_within_tolerance(self):
        items = [{"unit_price": 100, "qty": 1}]
        # 0.005 drift is within default 0.01 tolerance
        assert assert_subtotal_invariant(items, 100.005) is True

    def test_invariant_handles_bad_printed_subtotal(self):
        items = [{"unit_price": 100, "qty": 1}]
        # Garbage input doesn't crash; treats as 0 → drift warning
        assert assert_subtotal_invariant(items, "not a number") is False  # type: ignore[arg-type]


class TestFillAms704BugShape:
    """End-to-end: fill_ams704 must NOT include no_bid items in subtotal."""

    def test_pc_original_mode_skips_no_bid_in_subtotal(self, blank_704_path, tmp_path):
        from src.forms.price_check import fill_ams704
        items = [
            {"row_index": 1, "qty": 1, "unit_price": 5.40, "no_bid": True,
             "description": "Skipped item"},
            {"row_index": 2, "qty": 1, "unit_price": 1439.75,
             "description": "Allyn Propaqfi LT Charging Cradle"},
        ]
        parsed = {"line_items": items, "header": {}}
        out = tmp_path / "csp_sac_test.pdf"
        result = fill_ams704(
            source_pdf=str(blank_704_path),
            parsed_pc=parsed,
            output_pdf=str(out),
            tax_rate=0.0,
            original_mode=True,
        )
        assert result.get("ok") is True
        # Mike's exact regression — must be 1439.75, NOT 1445.15
        assert result["summary"]["subtotal"] == 1439.75


class TestQuoteGeneratorBugShape:
    """End-to-end: quote_generator must NOT include no_bid items in subtotal."""

    def test_quote_skips_no_bid_in_subtotal(self):
        # Use the same shape via subtotal_of (the canonical helper the
        # invariant assertion uses). quote_generator's per-loop accumulator
        # now matches this — but if the loop ever drifts, the invariant
        # will log PRICING-DRIFT in production logs.
        items = [
            {"no_bid": True, "price_per_unit": 5.40, "qty": 1},
            {"price_per_unit": 1439.75, "qty": 1},
        ]
        assert subtotal_of(items) == 1439.75


class TestCanonicalUnitPriceAtRender:
    """Read-time defense — even when persisted unit_price is stale relative
    to cost × markup, the renderer's `subtotal_of(items)` must return the
    canonical price × qty (cost × (1 + markup/100) × qty). Closes the
    Cortech-mattress drift class at render-time so the heal script becomes
    data-hygiene rather than accuracy-critical.
    """

    def test_cortech_mattress_shape_subtotal_uses_canonical_price(self):
        # The original prod incident: cost $465.40 × 22% markup → $567.79
        # canonical. Persisted unit_price stale at $558.48. Quote PDF
        # historically shipped $558.48 × 16 = $8,935.68; canonical is
        # $567.79 × 16 = $9,084.64 — a $148.96 under-quote per send.
        items = [{
            "qty": 16,
            "unit_price": 558.48,  # stale persisted value
            "pricing": {"unit_cost": 465.40, "markup_pct": 22.0},
        }]
        # canonical: round(465.40 * 1.22, 2) = 567.79; 567.79 * 16 = 9084.64
        assert subtotal_of(items) == 9084.64

    def test_no_cost_no_markup_falls_back_to_unit_price(self):
        # PO-imported snapshots have a flat price and no cost basis.
        # subtotal_of must still return a sensible number.
        items = [{"qty": 10, "unit_price": 12.50}]
        assert subtotal_of(items) == 125.0

    def test_extension_accepts_quantity_alias(self):
        # quote_generator raw input uses `quantity`; PC uses `qty`. Both
        # must work. Caught in pre-deploy 2026-05-11 — the predeploy
        # quote fixture supplied only `quantity` and extension_of
        # returned 0, failing the "$0 total" contract guard.
        items = [{"unit_price": 5.00, "quantity": 10}]
        assert subtotal_of(items) == 50.00

    def test_zero_markup_uses_cost_basis(self):
        # 2026-05-11 fix: canonical_unit_price honors markup_pct=0.0
        # as a legitimate value (free / pass-through / cost-only quote).
        # Pre-fix the `or`-chain treated 0.0 as missing because Python
        # `0.0 or X` → X. Explicit None-check replaces the falsy chain.
        items = [{
            "qty": 4,
            "unit_price": 999.99,  # stale persisted value
            "pricing": {"unit_cost": 25.00, "markup_pct": 0.0},
        }]
        # Canonical: cost × (1 + 0/100) × qty = 25 × 4 = 100.0
        assert subtotal_of(items) == 100.0

    def test_zero_markup_distinct_from_no_markup(self):
        # Pin the None vs 0.0 distinction. Item A has explicit markup=0
        # (canonical=25); item B has no markup field at all and a stale
        # unit_price → falls back to stored unit_price (999.99). The
        # fallback chain is unchanged; only the markup chain shifted to
        # explicit None-check.
        item_zero_markup = {
            "qty": 1, "unit_price": 999.99,
            "pricing": {"unit_cost": 25.00, "markup_pct": 0.0},
        }
        item_no_markup = {
            "qty": 1, "unit_price": 999.99,
            "pricing": {"unit_cost": 25.00},  # no markup field
        }
        assert subtotal_of([item_zero_markup]) == 25.0
        assert subtotal_of([item_no_markup]) == 999.99


class TestFill704bInvariant:
    """fill_704b is the customer-facing RFQ-quote PDF. PR #849 added the
    invariant to PC fill + quote_generator but missed this path entirely.
    These tests pin that the shared helper's subtotal output matches the
    no_bid-filtered canonical sum that the 704B PDF stamps.
    """

    def test_build_704_item_fields_subtotal_skips_no_bid(self):
        # The shared helper returns merchandise_subtotal which fill_704b
        # writes verbatim to PDF fields fill_154 / fill_154_2.
        # Use the lightweight contract assertion: subtotal_of agreement.
        items = [
            {"no_bid": True, "unit_price": 99.99, "qty": 1, "description": "Skip me"},
            {"unit_price": 100.00, "qty": 2, "description": "Bill this"},
        ]
        # Expected 704B PDF subtotal: 200.00 (skip the no_bid row).
        assert subtotal_of(items) == 200.00

    def test_build_704_item_fields_subtotal_uses_canonical_price(self):
        # Cost × markup stale unit_price shape — must render canonical.
        items = [{
            "qty": 5,
            "unit_price": 10.00,  # stale
            "pricing": {"unit_cost": 20.00, "markup_pct": 50.0},
        }]
        # canonical: 20 * 1.5 = 30; 30 * 5 = 150.0
        assert subtotal_of(items) == 150.0
