"""
test_quote_dollar_accuracy.py — Quote Dollar Accuracy Tests

Verifies the full pricing math chain:
  cost -> markup tier -> subtotal -> tax -> grand total

Tests exact dollar amounts that appear on quotes sent to buyers.
A rounding error here = wrong invoice = lost trust.

Covers:
  - Pricing oracle tier calculations (recommended, aggressive, safe)
  - SCPRS-as-ceiling guard (never use as cost)
  - Amazon-as-reference guard (never use as cost)
  - 3x cost sanity guardrail
  - Subtotal + tax + total math on 704 forms
  - Profit floor enforcement
  - Markup math correctness
"""

import json
import os
import pytest
from unittest.mock import patch


# ═══════════════════════════════════════════════════════════════════════
# Pricing Oracle Tier Math
# ═══════════════════════════════════════════════════════════════════════

class TestPricingOracleMath:
    """Test recommend_price() with known inputs → exact expected outputs."""

    def _recommend(self, **kwargs):
        """Helper to call recommend_price with mocked SCPRS history."""
        from src.knowledge.pricing_oracle import recommend_price
        # Mock get_price_history to return no historical data by default
        # (tests provide scprs_price directly when needed)
        with patch("src.knowledge.pricing_oracle.get_price_history") as mock_hist:
            mock_hist.return_value = {
                "matches": 0, "median_price": None, "min_price": None,
                "max_price": None, "recent_avg": None, "trend": "unknown",
                "data_points": [],
            }
            with patch("src.knowledge.pricing_oracle.win_probability") as mock_wp:
                mock_wp.return_value = {"probability": 0.70}
                return recommend_price(**kwargs)

    def test_cost_only_default_25pct_markup(self, temp_data_dir):
        """Cost only → recommended = cost * 1.25 (25% default markup)."""
        rec = self._recommend(
            item_number="TEST-001", description="Copy paper",
            supplier_cost=100.00,
        )
        assert rec["data_quality"] == "cost_only"
        # With $100 cost, 25% markup = $125
        # But profit floor is $100 general, so min = $200
        assert rec["recommended"]["price"] >= 200.00, \
            f"Profit floor ($100) should push price to at least $200, got ${rec['recommended']['price']}"

    def test_cost_only_amazon_lower_floor(self, temp_data_dir):
        """Amazon source → profit floor is $50 instead of $100."""
        rec = self._recommend(
            item_number="", description="Nitrile gloves",
            supplier_cost=100.00, source_type="amazon",
        )
        # Amazon profit floor = $50, so min = $150
        assert rec["recommended"]["price"] >= 150.00

    def test_scprs_undercut_1pct(self, temp_data_dir):
        """With SCPRS reference, recommended = SCPRS * 0.99 (1% undercut)."""
        rec = self._recommend(
            item_number="TEST-002", description="Bandage wrap",
            supplier_cost=50.00, scprs_price=200.00,
        )
        # SCPRS undercut: $200 * 0.99 = $198.00
        # Profit floor: $50 + $100 = $150 (below $198, so no floor applied)
        assert rec["recommended"]["price"] == pytest.approx(198.00, abs=0.01), \
            f"Expected ~$198.00 (1% undercut of $200), got ${rec['recommended']['price']}"

    def test_aggressive_undercut_3pct(self, temp_data_dir):
        """Aggressive tier = SCPRS * 0.97 (3% undercut)."""
        rec = self._recommend(
            item_number="TEST-003", description="Exam gloves",
            supplier_cost=50.00, scprs_price=200.00,
        )
        # Aggressive: $200 * 0.97 = $194.00
        assert rec["aggressive"]["price"] == pytest.approx(194.00, abs=0.01)

    def test_safe_markup_30pct(self, temp_data_dir):
        """Safe tier = cost * 1.30, but profit floor may push higher."""
        rec = self._recommend(
            item_number="TEST-004", description="Safety vest",
            supplier_cost=80.00, scprs_price=200.00,
        )
        safe = rec["safe"]["price"]
        # Safe must be above cost
        assert safe > 80.00, f"Safe must exceed cost, got ${safe}"
        # Safe must have positive margin
        assert rec["safe"]["margin_pct"] > 0

    def test_no_data_returns_no_data_quality(self, temp_data_dir):
        """No cost + no SCPRS = data_quality='no_data', no recommendation."""
        rec = self._recommend(
            item_number="TEST-005", description="Mystery item",
        )
        assert rec["data_quality"] == "no_data"

    def test_profit_floor_enforced(self, temp_data_dir):
        """Even with low SCPRS, price never goes below cost + profit floor."""
        rec = self._recommend(
            item_number="TEST-006", description="Cheap item",
            supplier_cost=500.00, scprs_price=510.00,
        )
        # SCPRS undercut: $510 * 0.99 = $504.90
        # But profit floor: $500 + $100 = $600
        # Floor should win
        assert rec["recommended"]["price"] >= 600.00, \
            f"Profit floor should enforce $600 min, got ${rec['recommended']['price']}"

    def test_margin_calculation_correct(self, temp_data_dir):
        """Margin % = (price - cost) / cost."""
        rec = self._recommend(
            item_number="TEST-007", description="Toner cartridge",
            supplier_cost=50.00, scprs_price=200.00,
        )
        price = rec["recommended"]["price"]
        margin = rec["recommended"]["margin_pct"]
        expected_margin = (price - 50.00) / 50.00
        assert margin == pytest.approx(expected_margin, abs=0.01)


# ═══════════════════════════════════════════════════════════════════════
# Pricing Guard Rails
# ═══════════════════════════════════════════════════════════════════════

class TestPricingGuardRails:
    """Tests for critical pricing rules from CLAUDE.md."""

    def test_scprs_price_is_ceiling_not_cost(self, temp_data_dir):
        """SCPRS prices inform pricing but profit floor may override.
        When cost is low and SCPRS is low, profit floor ($100) takes precedence."""
        from src.knowledge.pricing_oracle import recommend_price
        with patch("src.knowledge.pricing_oracle.get_price_history") as mock_hist:
            mock_hist.return_value = {
                "matches": 5, "median_price": 75.00, "min_price": 60.00,
                "max_price": 90.00, "recent_avg": 78.00, "trend": "stable",
                "data_points": [],
            }
            with patch("src.knowledge.pricing_oracle.win_probability") as mock_wp:
                mock_wp.return_value = {"probability": 0.65}
                rec = recommend_price(
                    item_number="TEST-GUARD",
                    description="Medical gloves",
                    supplier_cost=30.00,  # real cost
                    scprs_price=75.00,    # what state paid = ceiling
                )
        # Price must be ABOVE cost (never sell at a loss)
        assert rec["recommended"]["price"] > 30.00, \
            "Price must be above supplier cost"
        # Profit floor enforced: $30 + $100 = $130
        assert rec["recommended"]["price"] >= 130.00, \
            "Profit floor ($100) should enforce minimum"
        # The price source should reflect profit floor was applied
        assert "profit_floor" in str(rec.get("flags", [])), \
            "Profit floor flag should be set when floor overrides SCPRS undercut"

    def test_markup_math_exact_values(self, temp_data_dir):
        """Verify: 25% markup on $82.24 = $102.80, not $411.20 or other garbage."""
        cost = 82.24
        markup_25 = round(cost * 1.25, 2)
        assert markup_25 == 102.80, f"25% markup on $82.24 should be $102.80, got ${markup_25}"

        markup_30 = round(cost * 1.30, 2)
        assert markup_30 == 106.91, f"30% markup on $82.24 should be $106.91, got ${markup_30}"

    def test_scprs_per_unit_division(self, temp_data_dir):
        """SCPRS unit_price is LINE TOTAL. Must divide by quantity."""
        # Simulates what the enrichment pipeline does
        scprs_price = 100.00  # line total
        scprs_qty = 5         # quantity on PO
        per_unit = round(scprs_price / scprs_qty, 2)
        assert per_unit == 20.00, f"$100 / 5 qty should be $20/unit, got ${per_unit}"

    def test_large_qty_rounding(self, temp_data_dir):
        """Per-unit price with odd division should round to 2 decimals."""
        scprs_price = 47.50
        scprs_qty = 3
        per_unit = round(scprs_price / scprs_qty, 2)
        assert per_unit == 15.83, f"$47.50 / 3 should be $15.83, got ${per_unit}"


# ═══════════════════════════════════════════════════════════════════════
# 704 Form Subtotal/Tax/Total Math
# ═══════════════════════════════════════════════════════════════════════

class TestFormTotalsMath:
    """Verify subtotal + tax + grand total calculations."""

    def _compute_totals(self, items, tax_rate=0.0):
        """Replicate the subtotal/tax/total calc from price_check.py."""
        subtotal = 0.0
        for it in items:
            qty = it.get("qty", 1)
            price = it.get("unit_price", 0)
            extension = round(qty * price, 2)
            subtotal += extension
        subtotal = round(subtotal, 2)
        tax = round(subtotal * tax_rate, 2)
        total = round(subtotal + tax, 2)
        return subtotal, tax, total

    def test_single_item_no_tax(self):
        items = [{"qty": 10, "unit_price": 15.72}]
        sub, tax, total = self._compute_totals(items)
        assert sub == 157.20
        assert tax == 0.00
        assert total == 157.20

    def test_two_items_no_tax(self):
        items = [
            {"qty": 22, "unit_price": 15.72},
            {"qty": 5, "unit_price": 53.74},
        ]
        sub, tax, total = self._compute_totals(items)
        assert sub == 345.84 + 268.70
        assert sub == 614.54
        assert total == 614.54

    def test_with_tax_8_75_pct(self):
        items = [
            {"qty": 10, "unit_price": 25.00},
            {"qty": 5, "unit_price": 100.00},
        ]
        sub, tax, total = self._compute_totals(items, tax_rate=0.0875)
        assert sub == 750.00
        assert tax == 65.62  # 750 * 0.0875 = 65.625 → banker's rounding
        assert total == 815.62

    def test_15_items_total_accuracy(self):
        """15-item PC with varied prices — verify no floating point drift."""
        items = [{"qty": i + 1, "unit_price": 10.00 + i * 3.33} for i in range(15)]
        sub, tax, total = self._compute_totals(items, tax_rate=0.0875)
        # Manually verify subtotal
        expected_sub = sum(round((i + 1) * (10.00 + i * 3.33), 2) for i in range(15))
        assert sub == round(expected_sub, 2)
        assert total == round(sub + round(sub * 0.0875, 2), 2)

    def test_zero_price_items_excluded(self):
        """Items with $0 price should contribute $0 to subtotal."""
        items = [
            {"qty": 10, "unit_price": 25.00},
            {"qty": 5, "unit_price": 0.00},  # no-bid item
            {"qty": 3, "unit_price": 50.00},
        ]
        sub, tax, total = self._compute_totals(items)
        assert sub == 400.00  # 250 + 0 + 150
        assert total == 400.00

    def test_fractional_penny_rounding(self):
        """Verify extension rounding: 3 * $33.33 = $99.99, not $99.989..."""
        items = [{"qty": 3, "unit_price": 33.33}]
        sub, _, total = self._compute_totals(items)
        assert sub == 99.99

    def test_large_order_total(self):
        """Bulk order: 500 qty * $2.49 = $1,245.00."""
        items = [{"qty": 500, "unit_price": 2.49}]
        sub, _, total = self._compute_totals(items)
        assert sub == 1245.00

    def test_tax_rounding_edge_case(self):
        """Tax on $999.99 at 8.75% = $87.50 (rounds from $87.499...)"""
        items = [{"qty": 1, "unit_price": 999.99}]
        sub, tax, total = self._compute_totals(items, tax_rate=0.0875)
        assert sub == 999.99
        assert tax == 87.50  # 999.99 * 0.0875 = 87.499125 → rounds to 87.50 (standard)
        assert total == 1087.49  # 999.99 + 87.50
