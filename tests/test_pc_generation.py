"""Integration tests for Price Check PDF generation.

Tests the fill_ams704() pipeline with the real blank 704 template.
Verifies field values in generated PDFs across boundary conditions.

These tests use the temp_data_dir fixture (autouse) for full DB isolation
and the real PDF template from tests/fixtures/.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "templates")
BLANK_704 = os.path.join(FIXTURES_DIR, "ams_704_blank.pdf")

if not os.path.exists(BLANK_704):
    BLANK_704 = os.path.join(TEMPLATE_DIR, "ams_704_blank.pdf")

# Patch _expiry_date for Windows (%-m is Unix-only strftime)
import src.forms.price_check as _pc_mod
from datetime import datetime, timedelta
def _expiry_date_win():
    exp = datetime.now() + timedelta(days=45)
    return f"{exp.month}/{exp.day}/{exp.year}"
_pc_mod._expiry_date = _expiry_date_win

from src.forms.price_check import fill_ams704


def _skip_if_no_template():
    if not os.path.exists(BLANK_704):
        pytest.skip("ams_704_blank.pdf not available")


def _make_items(count):
    """Create N test line items with sequential pricing."""
    items = []
    for i in range(1, count + 1):
        items.append({
            "row_index": i,
            "description": f"Test Item #{i} - Widget for testing purposes",
            "qty": 2,
            "uom": "EA",
            "qty_per_uom": 1,
            "unit_price": 10.00 + i,
            "supplier_cost": 8.00 + i,
            "pricing": {"recommended_price": 10.00 + i},
        })
    return items


def _fill_and_get_fields(item_count, price_tier="recommended"):
    """Run fill_ams704 and return (result_dict, field_values, page_count)."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        output = tmp.name
    try:
        result = fill_ams704(
            source_pdf=BLANK_704,
            parsed_pc={
                "line_items": _make_items(item_count),
                "header": {"institution": "CSP-Sacramento"},
                "ship_to": "CSP-Sacramento, 100 Prison Rd, Represa, CA 95671",
            },
            output_pdf=output,
            price_tier=price_tier,
        )
        # Read field values written by fill_ams704
        # Returns a list of {field_id, page, value} dicts
        fv_path = os.path.join(_pc_mod.DATA_DIR, "pc_field_values.json")
        field_values_list = []
        if os.path.exists(fv_path):
            with open(fv_path) as f:
                field_values_list = json.load(f)
        # Convert to flat dict for easier assertions
        field_values = {}
        if isinstance(field_values_list, list):
            for entry in field_values_list:
                field_values[entry["field_id"]] = entry.get("value", "")
        elif isinstance(field_values_list, dict):
            field_values = field_values_list

        from pypdf import PdfReader
        page_count = len(PdfReader(output).pages) if os.path.exists(output) else 0
        return result, field_values, page_count
    finally:
        if os.path.exists(output):
            os.unlink(output)


# ═══════════════════════════════════════════════════════════════════════════
# Single page (1-8 items)
# ═══════════════════════════════════════════════════════════════════════════

class TestSinglePageGeneration:

    def test_one_item(self):
        """Minimum: 1 item should produce a valid PDF."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(1)
        assert result is not None
        assert pages >= 1
        # Supplier name should be set
        assert "SUPPLIER NAME" in fv or any("SUPPLIER" in k for k in fv)

    def test_eight_items_fills_page1(self):
        """8 items should fill page 1 completely."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(8)
        assert result is not None
        # Should have pricing for all 8 rows
        price_fields = [k for k in fv if "PRICE PER UNIT" in k and not k.endswith("_2")]
        assert len(price_fields) >= 8, f"Expected 8 price fields, got {len(price_fields)}: {price_fields}"

    def test_prices_are_correct(self):
        """Verify actual price values match input."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(3)
        # Item 1 price should be 11.00 (10.00 + 1)
        row1_price = fv.get("PRICE PER UNITRow1", "")
        if row1_price:
            assert "11" in str(row1_price), f"Row1 price expected ~11.00, got {row1_price}"


# ═══════════════════════════════════════════════════════════════════════════
# Two pages (9-19 items) — uses _2 suffix fields
# ═══════════════════════════════════════════════════════════════════════════

class TestTwoPageGeneration:

    def test_nine_items_spills_to_page2(self):
        """9 items should use page 2 (_2 suffix fields)."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(9)
        assert pages >= 2
        # Should have at least one _2 suffix field
        suffix_fields = [k for k in fv if k.endswith("_2")]
        assert len(suffix_fields) > 0, "Expected _2 suffix fields for 9 items"

    def test_sixteen_items_fills_both_pages(self):
        """16 items should fill both pages of form fields."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(16)
        assert result is not None
        assert pages >= 2

    def test_nineteen_items_max_form_capacity(self):
        """19 items = max form field capacity (11 unsuffixed + 8 suffixed)."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(19)
        assert result is not None


# ═══════════════════════════════════════════════════════════════════════════
# Overflow (20+ items) — uses reportlab canvas
# ═══════════════════════════════════════════════════════════════════════════

class TestOverflowGeneration:

    def test_twenty_items_triggers_overflow(self):
        """20 items exceeds form capacity — overflow pages generated."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(20)
        assert result is not None
        assert pages >= 3, f"Expected 3+ pages for 20 items, got {pages}"

    def test_thirty_items_multiple_overflow(self):
        """30 items — stress test for multiple overflow pages."""
        _skip_if_no_template()
        result, fv, pages = _fill_and_get_fields(30)
        assert result is not None
        assert pages >= 3


# ═══════════════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════════════

class TestEdgeCases:

    def test_zero_price_items(self):
        """Items with $0 price should not crash generation."""
        _skip_if_no_template()
        items = _make_items(3)
        items[1]["unit_price"] = 0
        items[1]["pricing"]["recommended_price"] = 0
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            result = fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={
                    "line_items": items,
                    "header": {"institution": "Test"},
                    "ship_to": "Test",
                },
                output_pdf=output,
                price_tier="recommended",
            )
            assert result is not None
        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_long_descriptions(self):
        """Items with very long descriptions should not crash."""
        _skip_if_no_template()
        items = _make_items(2)
        items[0]["description"] = "A" * 300 + " - Very long test description"
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            result = fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={
                    "line_items": items,
                    "header": {"institution": "Test"},
                    "ship_to": "Test",
                },
                output_pdf=output,
                price_tier="recommended",
            )
            assert result is not None
        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_special_characters_in_description(self):
        """Unicode and special chars should not crash generation."""
        _skip_if_no_template()
        items = _make_items(1)
        items[0]["description"] = "Adhesive — 3M™ bandage (size: 4\"x4\") #123"
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            result = fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={
                    "line_items": items,
                    "header": {"institution": "Test"},
                    "ship_to": "Test",
                },
                output_pdf=output,
                price_tier="recommended",
            )
            assert result is not None
        finally:
            if os.path.exists(output):
                os.unlink(output)


# ═══════════════════════════════════════════════════════════════════════════
# Required vendor fields — Form QA gates
# ═══════════════════════════════════════════════════════════════════════════

class TestRequiredVendorFields:
    """Vendor identification fields that Form QA (`form_qa.py`) treats as
    required. Missing any of these triggers `Form QA FAIL — Missing: <field>`
    on every PC generation. Regression for 2026-04-23 incident: PC f81c4e9b
    failed QA on PERSON PROVIDING QUOTE because fill_ams704's supplier_mappings
    list omitted that key (only COMPANY REPRESENTATIVE was being written)."""

    def test_company_name_filled(self):
        _skip_if_no_template()
        _result, fv, _pages = _fill_and_get_fields(1)
        assert fv.get("COMPANY NAME"), "COMPANY NAME must be set on every PC"

    def test_company_representative_filled(self):
        _skip_if_no_template()
        _result, fv, _pages = _fill_and_get_fields(1)
        assert fv.get("COMPANY REPRESENTATIVE print name"), \
            "COMPANY REPRESENTATIVE print name must be set on every PC"

    def test_person_providing_quote_filled(self):
        """The bug — PERSON PROVIDING QUOTE was missing from the mapping
        for ~every PC generation, tripping Form QA on every save+gen."""
        _skip_if_no_template()
        _result, fv, _pages = _fill_and_get_fields(1)
        assert fv.get("PERSON PROVIDING QUOTE"), \
            "PERSON PROVIDING QUOTE must be set (Form QA required field)"

    def test_person_providing_quote_matches_representative(self):
        """Both fields refer to the canonical Reytech contact — they must
        carry the same value so the form reads consistently."""
        _skip_if_no_template()
        _result, fv, _pages = _fill_and_get_fields(1)
        assert fv["PERSON PROVIDING QUOTE"] == fv["COMPANY REPRESENTATIVE print name"]
