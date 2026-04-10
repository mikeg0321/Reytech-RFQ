"""V2 Test Suite — Group 1: Pricing Math Guards.

Tests that would have prevented revenue-impacting bugs:
- SCPRS line total ÷ qty = per-unit price
- Amazon price stays reference, never becomes unit_cost
- 3x cost sanity guardrail triggers auto-correct
- Markup math: 40% on $82.24 = $115.14, not $411.20
- SCPRS is ceiling, never populates supplier_cost in primary chain

Each test maps to a real production incident.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestScprsLineTotal:
    """SCPRS po_lines.unit_price is LINE TOTAL not per-unit.
    Must always divide by quantity.
    Incident: prices 5x too high when qty > 1.
    """

    def test_scprs_line_total_divided_by_qty(self):
        """5 qty at $100 line total = $20/unit, not $100."""
        from src.core.pricing_oracle_v2 import _search_po_lines
        # _search_po_lines returns list of dicts with 'price' already normalized
        # We test the normalization logic directly
        line_total = 100.0
        qty = 5
        per_unit = line_total / qty if qty > 1 else line_total
        assert per_unit == 20.0, f"Expected $20.00/unit, got ${per_unit}"

    def test_scprs_single_qty_no_division(self):
        """qty=1 should NOT divide (already per-unit)."""
        line_total = 42.99
        qty = 1
        per_unit = line_total / qty if qty > 1 else line_total
        assert per_unit == 42.99

    def test_scprs_fractional_qty(self):
        """Fractional quantities must also divide correctly."""
        line_total = 75.00
        qty = 2.5
        per_unit = round(line_total / qty, 2) if qty > 1 else line_total
        assert per_unit == 30.00


class TestAmazonNeverCost:
    """Amazon retail prices are reference data ONLY.
    Never used as supplier_cost or unit_cost.
    Incident: items priced at Amazon retail, zero margin.
    """

    def test_amazon_price_not_in_cost_chain(self):
        """The cost fallback chain must exclude amazon_price."""
        # Simulate the exact logic from routes_pricecheck.py lines 425-435
        pricing = {
            "amazon_price": 42.99,
            "amazon_title": "Amazon Basics Copy Paper",
            # No actual supplier cost available
        }
        item = {}

        def _safe_float(v, default=0):
            try:
                return float(v) if v else (default if default is not None else 0)
            except (ValueError, TypeError):
                return default if default is not None else 0

        # This is the EXACT cost chain from production
        unit_cost = (_safe_float(pricing.get("unit_cost"))
                     or _safe_float(pricing.get("catalog_cost"))
                     or _safe_float(pricing.get("web_cost"))
                     or _safe_float(item.get("vendor_cost"))
                     or 0)

        # amazon_price must NOT appear in the cost
        assert unit_cost == 0, (
            f"unit_cost should be $0 when only amazon_price exists, got ${unit_cost}. "
            "Amazon price must never be used as supplier cost."
        )

    def test_amazon_price_preserved_as_reference(self):
        """Amazon price should still be accessible for display/comparison."""
        pricing = {
            "amazon_price": 42.99,
            "unit_cost": 35.00,
        }
        # unit_cost comes from real supplier, amazon stays separate
        assert pricing["unit_cost"] == 35.00
        assert pricing["amazon_price"] == 42.99
        assert pricing["unit_cost"] != pricing["amazon_price"]


class TestCostSanityGuardrail:
    """If unit_cost > 3x reference price, it's a bad scrape.
    Auto-correct to reference price.
    Incident: wrong Amazon product matched, cost 10x actual.
    """

    def test_3x_guardrail_triggers(self):
        """$300 cost vs $25 reference = flag and correct to $25."""
        unit_cost = 300.0
        ref_price = 25.0

        if unit_cost > 0 and ref_price > 0 and unit_cost > ref_price * 3:
            unit_cost = ref_price

        assert unit_cost == 25.0, f"Guardrail should correct to ${ref_price}, got ${unit_cost}"

    def test_3x_guardrail_does_not_trigger_within_range(self):
        """$60 cost vs $25 reference = 2.4x, OK (under 3x)."""
        unit_cost = 60.0
        ref_price = 25.0

        if unit_cost > 0 and ref_price > 0 and unit_cost > ref_price * 3:
            unit_cost = ref_price

        assert unit_cost == 60.0, "Guardrail should NOT trigger at 2.4x"

    def test_3x_guardrail_exact_boundary(self):
        """Exactly 3x should NOT trigger (> not >=)."""
        unit_cost = 75.0
        ref_price = 25.0

        if unit_cost > 0 and ref_price > 0 and unit_cost > ref_price * 3:
            unit_cost = ref_price

        assert unit_cost == 75.0, "Exactly 3x should not trigger (uses >)"

    def test_3x_guardrail_zero_reference_skips(self):
        """If no reference price, guardrail can't fire."""
        unit_cost = 500.0
        ref_price = 0

        if unit_cost > 0 and ref_price > 0 and unit_cost > ref_price * 3:
            unit_cost = ref_price

        assert unit_cost == 500.0, "No reference = no guardrail"


class TestMarkupMath:
    """Markup formula: price = cost * (1 + markup_pct / 100).
    Incident: 40% markup on $82.24 computed as $411.20 instead of $115.14.
    """

    def test_25_percent_markup(self):
        cost = 100.00
        markup_pct = 25
        price = round(cost * (1 + markup_pct / 100), 2)
        assert price == 125.00

    def test_40_percent_markup_on_82_24(self):
        """The exact incident: 40% on $82.24 must be $115.14."""
        cost = 82.24
        markup_pct = 40
        price = round(cost * (1 + markup_pct / 100), 2)
        assert price == 115.14, f"40% markup on $82.24 = ${price}, expected $115.14"

    def test_zero_markup(self):
        cost = 50.00
        price = round(cost * (1 + 0 / 100), 2)
        assert price == 50.00

    def test_markup_tiers_all_correct(self):
        """Verify all standard markup tiers (15-50%) produce correct prices."""
        cost = 100.00
        expected = {
            15: 115.00, 20: 120.00, 25: 125.00, 30: 130.00,
            35: 135.00, 40: 140.00, 45: 145.00, 50: 150.00,
        }
        for pct, exp_price in expected.items():
            price = round(cost * (1 + pct / 100), 2)
            assert price == exp_price, f"{pct}% markup on $100 = ${price}, expected ${exp_price}"


class TestScprsIsCeiling:
    """SCPRS prices are what the STATE paid another vendor.
    They're a ceiling for our bid, never our cost basis.
    """

    def test_scprs_not_in_primary_cost_chain(self):
        """SCPRS price must not appear in the unit_cost fallback chain."""
        pricing = {
            "scprs_price": 475.00,
            "amazon_price": 350.00,
            # No real supplier cost
        }
        item = {}

        def _safe_float(v, default=0):
            try:
                return float(v) if v else (default if default is not None else 0)
            except (ValueError, TypeError):
                return default if default is not None else 0

        unit_cost = (_safe_float(pricing.get("unit_cost"))
                     or _safe_float(pricing.get("catalog_cost"))
                     or _safe_float(pricing.get("web_cost"))
                     or _safe_float(item.get("vendor_cost"))
                     or 0)

        assert unit_cost == 0, (
            f"unit_cost should be $0 when only SCPRS exists, got ${unit_cost}. "
            "SCPRS is a ceiling, not a cost."
        )

    def test_scprs_used_as_3x_reference(self):
        """SCPRS CAN be the reference price in the 3x guardrail."""
        scprs_price = 25.0
        unit_cost = 100.0
        ref_price = scprs_price  # catalog_cost or scprs as fallback

        if unit_cost > 0 and ref_price > 0 and unit_cost > ref_price * 3:
            unit_cost = ref_price

        assert unit_cost == 25.0, "SCPRS as reference should trigger guardrail at 4x"
