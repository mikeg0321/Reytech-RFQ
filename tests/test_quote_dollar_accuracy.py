"""V3 Test Suite — Quote Dollar Accuracy End-to-End.

Tests the complete money flow:
  item costs → markup → extensions → subtotal → tax → grand total → PDF fields

A rounding error in any module produces wrong quotes sent to buyers.
These tests verify exact dollar amounts across the full chain.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

# Patch _expiry_date for Windows
import src.forms.price_check as _pc_mod
from datetime import datetime, timedelta
def _expiry_date_win():
    exp = datetime.now() + timedelta(days=45)
    return f"{exp.month}/{exp.day}/{exp.year}"
_pc_mod._expiry_date = _expiry_date_win

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "templates")
BLANK_704 = os.path.join(FIXTURES_DIR, "ams_704_blank.pdf")
if not os.path.exists(BLANK_704):
    BLANK_704 = os.path.join(TEMPLATE_DIR, "ams_704_blank.pdf")


def _skip_if_no_template():
    if not os.path.exists(BLANK_704):
        pytest.skip("ams_704_blank.pdf not available")


# ═══════════════════════════════════════════════════════════════════════════
# Unit: Extension (line total) rounding
# ═══════════════════════════════════════════════════════════════════════════

class TestExtensionRounding:
    """extension = round(unit_price * qty, 2) — verify no penny drift."""

    def test_simple_extension(self):
        assert round(10.00 * 5, 2) == 50.00

    def test_fractional_extension(self):
        """$8.49 * 3 = $25.47 exactly."""
        assert round(8.49 * 3, 2) == 25.47

    def test_penny_boundary(self):
        """$33.33 * 3 = $99.99, not $100.00."""
        assert round(33.33 * 3, 2) == 99.99

    def test_large_qty(self):
        """$0.05 * 1000 = $50.00."""
        assert round(0.05 * 1000, 2) == 50.00

    def test_known_problem_case(self):
        """$82.24 * 1 = $82.24 (the markup incident item)."""
        assert round(82.24 * 1, 2) == 82.24

    def test_float_precision_edge(self):
        """$19.99 * 7 = $139.93, not $139.92999..."""
        result = round(19.99 * 7, 2)
        assert result == 139.93, f"Expected 139.93, got {result}"


# ═══════════════════════════════════════════════════════════════════════════
# Unit: Markup formula
# ═══════════════════════════════════════════════════════════════════════════

class TestMarkupFormula:
    """price = round(cost * (1 + markup_pct / 100), 2)"""

    def test_25_pct_on_82_24(self):
        assert round(82.24 * (1 + 25/100), 2) == 102.80

    def test_40_pct_on_82_24(self):
        """The exact incident: 40% on $82.24 must be $115.14."""
        assert round(82.24 * (1 + 40/100), 2) == 115.14

    def test_15_pct_on_1_00(self):
        assert round(1.00 * (1 + 15/100), 2) == 1.15

    def test_50_pct_on_99_99(self):
        """99.99 * 1.5 = 149.985 → rounds to 149.98 (banker's rounding)."""
        assert round(99.99 * (1 + 50/100), 2) == 149.98


# ═══════════════════════════════════════════════════════════════════════════
# Unit: Tax calculation
# ═══════════════════════════════════════════════════════════════════════════

class TestTaxCalculation:
    """tax = round(subtotal * tax_rate, 2); total = round(subtotal + tax, 2)"""

    def test_775_tax_on_1000(self):
        subtotal = 1000.00
        tax = round(subtotal * 0.0775, 2)
        total = round(subtotal + tax, 2)
        assert tax == 77.50
        assert total == 1077.50

    def test_775_tax_on_pennies(self):
        """$123.45 * 7.75% = $9.57 (rounded)."""
        subtotal = 123.45
        tax = round(subtotal * 0.0775, 2)
        assert tax == 9.57

    def test_zero_tax(self):
        subtotal = 500.00
        tax = round(subtotal * 0.0, 2)
        total = round(subtotal + tax, 2)
        assert tax == 0.0
        assert total == 500.00

    def test_tax_on_odd_subtotal(self):
        """$847.23 * 8% = $67.78."""
        subtotal = 847.23
        tax = round(subtotal * 0.08, 2)
        assert tax == 67.78
        total = round(subtotal + tax, 2)
        assert total == 915.01


# ═══════════════════════════════════════════════════════════════════════════
# Unit: Subtotal accumulation from extensions
# ═══════════════════════════════════════════════════════════════════════════

class TestSubtotalAccumulation:
    """Subtotal = sum of individually-rounded extensions."""

    def test_5_item_subtotal(self):
        """Verify exact subtotal for a realistic 5-item quote."""
        items = [
            {"unit_price": 15.72, "qty": 22},   # ext = 345.84
            {"unit_price": 53.74, "qty": 5},    # ext = 268.70
            {"unit_price": 10.61, "qty": 10},   # ext = 106.10
            {"unit_price": 454.40, "qty": 1},   # ext = 454.40
            {"unit_price": 8.49, "qty": 100},   # ext = 849.00
        ]
        subtotal = 0.0
        for item in items:
            ext = round(item["unit_price"] * item["qty"], 2)
            subtotal += ext

        expected = 345.84 + 268.70 + 106.10 + 454.40 + 849.00
        assert round(subtotal, 2) == round(expected, 2) == 2024.04

    def test_accumulation_matches_sum_of_rounded(self):
        """Sum of rounded extensions must equal independently calculated sum."""
        prices = [12.58, 42.99, 8.49, 350.00, 69.12]
        qtys = [22, 5, 10, 1, 2]

        # Method 1: accumulate rounded extensions (how fill_ams704 does it)
        subtotal_accum = 0.0
        for p, q in zip(prices, qtys):
            subtotal_accum += round(p * q, 2)

        # Method 2: compute each extension independently and sum
        extensions = [round(p * q, 2) for p, q in zip(prices, qtys)]
        subtotal_sum = sum(extensions)

        assert round(subtotal_accum, 2) == round(subtotal_sum, 2)


# ═══════════════════════════════════════════════════════════════════════════
# Integration: Full PDF dollar verification
# ═══════════════════════════════════════════════════════════════════════════

class TestPdfDollarAccuracy:
    """End-to-end: generate a 704 PDF and verify every dollar field."""

    def test_5_item_pdf_totals(self):
        """Generate 5-item 704 and verify subtotal/tax/total in PDF fields."""
        _skip_if_no_template()
        from src.forms.price_check import fill_ams704

        items = [
            {"row_index": 1, "description": "Name tags", "qty": 22,
             "uom": "EA", "unit_price": 15.72, "pricing": {"recommended_price": 15.72}},
            {"row_index": 2, "description": "Copy paper", "qty": 5,
             "uom": "BOX", "unit_price": 53.74, "pricing": {"recommended_price": 53.74}},
            {"row_index": 3, "description": "Nitrile gloves", "qty": 10,
             "uom": "EA", "unit_price": 10.61, "pricing": {"recommended_price": 10.61}},
            {"row_index": 4, "description": "X-Restraint", "qty": 1,
             "uom": "SET", "unit_price": 454.40, "pricing": {"recommended_price": 454.40}},
            {"row_index": 5, "description": "Bandages", "qty": 100,
             "uom": "EA", "unit_price": 8.49, "pricing": {"recommended_price": 8.49}},
        ]

        # Expected: compute ourselves
        expected_extensions = [
            round(15.72 * 22, 2),   # 345.84
            round(53.74 * 5, 2),    # 268.70
            round(10.61 * 10, 2),   # 106.10
            round(454.40 * 1, 2),   # 454.40
            round(8.49 * 100, 2),   # 849.00
        ]
        expected_subtotal = sum(expected_extensions)  # 2024.04

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            result = fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={
                    "line_items": items,
                    "header": {"institution": "CSP-Sacramento"},
                    "ship_to": "CSP-Sacramento",
                },
                output_pdf=output,
                price_tier="recommended",
            )
            assert result is not None
            assert result.get("ok") or result.get("summary")

            # Read field values
            fv_path = os.path.join(_pc_mod.DATA_DIR, "pc_field_values.json")
            with open(fv_path) as f:
                fv_list = json.load(f)
            fv = {e["field_id"]: e["value"] for e in fv_list} if isinstance(fv_list, list) else fv_list

            # Verify subtotal (fill_70)
            pdf_subtotal = fv.get("fill_70", "0")
            assert pdf_subtotal.replace(",", "") == f"{expected_subtotal:.2f}", \
                f"PDF subtotal {pdf_subtotal} != expected {expected_subtotal:.2f}"

            # Verify tax = 0 (tax not enabled)
            pdf_tax = fv.get("fill_72", "0")
            assert pdf_tax.replace(",", "") == "0.00", \
                f"PDF tax should be 0.00, got {pdf_tax}"

            # Verify grand total = subtotal (no tax)
            pdf_total = fv.get("fill_73", "0")
            assert pdf_total.replace(",", "") == f"{expected_subtotal:.2f}", \
                f"PDF total {pdf_total} != expected {expected_subtotal:.2f}"

            # Verify individual extensions
            ext1 = fv.get("EXTENSIONRow1", "")
            if ext1:
                assert ext1.replace(",", "") == "345.84", \
                    f"Row1 extension {ext1} != 345.84"

        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_single_item_exact_dollars(self):
        """Single item: $82.24 * 1 qty = $82.24 subtotal."""
        _skip_if_no_template()
        from src.forms.price_check import fill_ams704

        items = [{"row_index": 1, "description": "Medical widget",
                  "qty": 1, "uom": "EA", "unit_price": 82.24,
                  "pricing": {"recommended_price": 82.24}}]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            result = fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={"line_items": items, "header": {"institution": "Test"},
                           "ship_to": "Test"},
                output_pdf=output, price_tier="recommended",
            )
            fv_path = os.path.join(_pc_mod.DATA_DIR, "pc_field_values.json")
            with open(fv_path) as f:
                fv_list = json.load(f)
            fv = {e["field_id"]: e["value"] for e in fv_list} if isinstance(fv_list, list) else fv_list

            assert fv.get("fill_70", "").replace(",", "") == "82.24", \
                f"Subtotal should be 82.24, got {fv.get('fill_70')}"
            assert fv.get("fill_73", "").replace(",", "") == "82.24", \
                f"Total should be 82.24, got {fv.get('fill_73')}"
        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_format_string_comma_separator(self):
        """Large amounts must have comma separators: 1234.56 → '1,234.56'."""
        value = 12345.67
        formatted = f"{value:,.2f}"
        assert formatted == "12,345.67"

    def test_format_string_no_extra_decimals(self):
        """Values must always be exactly 2 decimal places."""
        for value in [100.0, 0.1, 99.999, 1.005]:
            formatted = f"{round(value, 2):,.2f}"
            parts = formatted.split(".")
            assert len(parts) == 2
            assert len(parts[1]) == 2, f"{value} → {formatted} has wrong decimal places"
