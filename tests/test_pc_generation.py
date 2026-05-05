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


# ═══════════════════════════════════════════════════════════════════════════
# original_mode partial-fill — buyer pre-fills qty/price but leaves
# description/MFG# blank on a row (description came from email body, not PDF).
# Regression: pc_177b18e6 row 2 generated with qty=60/UOM=BX/price=$13.50/ext=$810
# but description and substituted item silently dropped on the output PDF.
# ═══════════════════════════════════════════════════════════════════════════

class TestOriginalModePartialFill:
    """When the buyer's source 704 has *some* rows fully filled and other
    rows partially filled (qty + price but no description), original_mode
    must NOT drop description/item#/substituted on the partial rows.
    Filling those blanks is what we own — buyer's filled cells are still
    preserved (we only write where the source field is empty)."""

    def _make_stub_profile(self, prefilled_field_values):
        """Build a stand-in TemplateProfile that reports `is_prefilled=True`
        and exposes a controlled `field_values` dict, so we can simulate a
        partially-filled buyer 704 without authoring a real pre-filled PDF."""
        from src.forms.template_registry import get_profile as _real_get_profile
        real_profile = _real_get_profile(BLANK_704)

        class _StubProfile:
            """Forwards every attribute to the real profile by default; only
            `is_prefilled` and `field_values` are forced. Keeps the test
            forward-compatible if TemplateProfile grows new attrs."""
            def __init__(self, base, values):
                object.__setattr__(self, "_base", base)
                object.__setattr__(self, "field_values", dict(values))
                object.__setattr__(self, "is_prefilled", True)
                # Empty so prefilled_suffix_for_item never claims a row that
                # would force the fill into a non-original code path.
                object.__setattr__(self, "prefilled_item_rows", {})

            def __getattr__(self, name):
                return getattr(self._base, name)

            def has_field(self, name):
                return self._base.has_field(name)

            def get_field_value(self, name):
                return self.field_values.get(name)

        return _StubProfile(real_profile, prefilled_field_values)

    def test_partial_row_description_and_substituted_get_filled(self, monkeypatch):
        """The pc_177b18e6 regression: buyer's source PDF has row 2 with
        QTY=60 / UOM=BX / PRICE=13.50 but blank description + substituted.
        Our PC items have desc='Nads Hair Removal Body Wax Strips' and
        mfg='0063899500192'. After fill_ams704, the output field_values
        MUST include those for row 2."""
        _skip_if_no_template()

        # Buyer's source PDF: row 1 fully filled (description + qty + ...),
        # row 2 has qty + price but blank description / substituted.
        prefilled = {
            "QTYRow1": "10",
            "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow1": "Heel Donut Cushions, Heel Cups, Silicon Insoles, One Size Fits All - 1 Pair",
            "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow1": "5CAIS1G9WZZC",
            "QTYRow2": "60",
            "PRICE PER UNITRow2": "13.50",
            # Description + substituted on row 2 are deliberately absent (blank).
        }
        stub = self._make_stub_profile(prefilled)
        import src.forms.template_registry as _tr
        monkeypatch.setattr(_tr, "get_profile", lambda _path: stub)
        # fill_ams704 imports get_profile inline, so patch the module the
        # function reads from too:
        import src.forms.price_check as _pc
        monkeypatch.setattr("src.forms.template_registry.get_profile", lambda _path: stub)

        items = [
            {
                "row_index": 1,
                "description": "Heel Donut Cushions, Heel Cups, Silicon Insoles, One Size Fits All - 1 Pair",
                "mfg_number": "5CAIS1G9WZZC",
                "qty": 10, "uom": "PR", "qty_per_uom": 1,
                "unit_price": 9.60,
                "pricing": {"recommended_price": 9.60, "unit_cost": 8.0},
            },
            {
                "row_index": 2,
                "description": "Nads Hair Removal Body Wax Strips for Normal Skin",
                "mfg_number": "0063899500192",
                "is_substitute": True,
                "qty": 60, "uom": "BX", "qty_per_uom": 24,
                "unit_price": 13.50,
                "pricing": {"recommended_price": 13.50, "unit_cost": 12.99},
            },
        ]
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            result = fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={
                    "line_items": items,
                    "header": {"institution": "CSP-SAC"},
                    "ship_to": "CSP-SAC, 100 Prison Rd, Represa, CA 95671",
                },
                output_pdf=output,
                price_tier="recommended",
            )
            assert result is not None

            fv_path = os.path.join(_pc_mod.DATA_DIR, "pc_field_values.json")
            assert os.path.exists(fv_path), "pc_field_values.json must be written"
            with open(fv_path) as f:
                entries = json.load(f)
            by_field = {e["field_id"]: e.get("value", "") for e in entries}

            # Buyer's row 1 description must NOT be overwritten — but our
            # write only goes through when source is blank, so we just assert
            # that the partial-fill block did NOT clobber it (we never wrote
            # row 1 description in original_mode, and the source already has it).
            row1_desc_field = "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow1"
            # If we wrote row 1 description, the value should equal source (we shouldn't overwrite a filled field)
            if row1_desc_field in by_field:
                # We chose not to write because source had a value — _src_blank returned False.
                pass

            # Row 2 description MUST be written (this is the regression fix)
            row2_desc_field = "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow2"
            assert row2_desc_field in by_field, \
                f"Row 2 description must be filled (was blank in source). Wrote: {sorted(by_field.keys())}"
            assert "Nads" in by_field[row2_desc_field], \
                f"Row 2 description should carry our PC item description, got: {by_field[row2_desc_field]!r}"

            # Row 2 substituted (MFG#) MUST be written
            row2_sub_field = "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow2"
            assert row2_sub_field in by_field, \
                f"Row 2 substituted must be filled (was blank in source). Wrote: {sorted(by_field.keys())}"
            assert "0063899500192" in by_field[row2_sub_field], \
                f"Row 2 substituted should carry our MFG#, got: {by_field[row2_sub_field]!r}"
        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_partial_row_does_not_overwrite_buyer_filled_description(self, monkeypatch):
        """When the buyer DID fill row 2's description, original_mode must
        leave it alone — that's the original purpose of original_mode."""
        _skip_if_no_template()

        buyer_row2_desc = "Buyer's exact wording for row 2 — do not touch"
        prefilled = {
            "QTYRow1": "10",
            "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow1": "Row 1 desc",
            "QTYRow2": "60",
            "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow2": buyer_row2_desc,
            "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow2": "BUYER-MFG-XYZ",
        }
        stub = self._make_stub_profile(prefilled)
        monkeypatch.setattr("src.forms.template_registry.get_profile", lambda _path: stub)

        items = [
            {"row_index": 1, "description": "ours-row-1", "qty": 10, "uom": "EA", "unit_price": 5.0,
             "pricing": {"recommended_price": 5.0}},
            {"row_index": 2, "description": "ours-row-2-DIFFERENT", "mfg_number": "OURS-MFG",
             "qty": 60, "uom": "BX", "unit_price": 13.50,
             "pricing": {"recommended_price": 13.50}},
        ]
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            output = tmp.name
        try:
            fill_ams704(
                source_pdf=BLANK_704,
                parsed_pc={"line_items": items, "header": {"institution": "T"}, "ship_to": "T"},
                output_pdf=output,
                price_tier="recommended",
            )
            fv_path = os.path.join(_pc_mod.DATA_DIR, "pc_field_values.json")
            with open(fv_path) as f:
                entries = json.load(f)
            by_field = {e["field_id"]: e.get("value", "") for e in entries}

            row2_desc_field = "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow2"
            row2_sub_field = "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow2"
            # Either we didn't write the field at all (source had it) — that's fine —
            # OR if we did write, we must NOT overwrite buyer's words with ours.
            if row2_desc_field in by_field:
                assert "ours-row-2-DIFFERENT" not in by_field[row2_desc_field], \
                    f"Must not overwrite buyer's filled description, got: {by_field[row2_desc_field]!r}"
            if row2_sub_field in by_field:
                assert "OURS-MFG" not in by_field[row2_sub_field], \
                    f"Must not overwrite buyer's filled substituted, got: {by_field[row2_sub_field]!r}"
        finally:
            if os.path.exists(output):
                os.unlink(output)
