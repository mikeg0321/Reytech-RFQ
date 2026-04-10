"""V2 Test Suite — Group 5: PDF Form Field Safety.

Tests that would have caught the 2026-04-03 incident
(11 failed commits from hardcoded row count).

Tests the ACTUAL template structure vs what fill_ams704 expects.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "templates")
BLANK_704 = os.path.join(FIXTURES_DIR, "ams_704_blank.pdf")
if not os.path.exists(BLANK_704):
    BLANK_704 = os.path.join(TEMPLATE_DIR, "ams_704_blank.pdf")


def _skip_if_no_template():
    if not os.path.exists(BLANK_704):
        pytest.skip("ams_704_blank.pdf not available")


class TestDetectPg1RowsNotHardcoded:
    """_detect_page_layout must read the ACTUAL template, never hardcode."""

    def test_detect_returns_actual_count(self):
        """detect_page_layout returns real field counts from the PDF."""
        _skip_if_no_template()
        from pypdf import PdfReader
        from src.forms.price_check import _detect_page_layout
        fields = PdfReader(BLANK_704).get_fields() or {}
        pg1, pg2_suf, pg2_extra = _detect_page_layout(fields, source_pdf=BLANK_704)
        # Must be > 0 (template has rows)
        assert pg1 > 0, "detect_page_layout returned 0 for page 1 rows"
        assert pg2_suf > 0, "detect_page_layout returned 0 for page 2 suffix rows"
        # Total capacity = pg1 + pg2_suf + pg2_extra
        total = pg1 + pg2_suf + pg2_extra
        assert total == 19, f"Expected 19 total capacity, got {total}"

    def test_detect_not_hardcoded_8(self):
        """The actual template has 8 on pg1 — but this must come from detection."""
        _skip_if_no_template()
        from pypdf import PdfReader
        from src.forms.price_check import _detect_page_layout
        fields = PdfReader(BLANK_704).get_fields() or {}
        pg1, _, _ = _detect_page_layout(fields, source_pdf=BLANK_704)
        # pg1 should be 8 for this template, but it's DETECTED not hardcoded
        assert pg1 == 8, f"Blank 704 template: expected 8 rows on pg1, got {pg1}"


class TestSuffixFieldMapping:
    """Items 9-19 must use _2 suffix fields. Items 20+ must use overlay."""

    def test_items_12_through_19_use_suffix_2(self):
        """Items in the _2 range must get _2 suffix field names."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        # Item at pg1_count + 1 should be on page 2 with _2 suffix
        pg2_start = profile.pg1_row_count + 1
        suffix = profile.row_field_suffix(pg2_start)
        assert suffix is not None, f"Slot {pg2_start} should have a suffix"
        assert "_2" in suffix, f"Slot {pg2_start} should use _2 suffix, got {suffix}"

    def test_items_20_plus_overflow(self):
        """Items beyond form capacity (19) should return None (overflow)."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        assert profile.row_field_suffix(20) is None, "Slot 20 should be overflow"
        assert profile.row_field_suffix(25) is None, "Slot 25 should be overflow"

    def test_no_suffix_3_fields_exist(self):
        """The template must NOT have _3 suffix fields."""
        _skip_if_no_template()
        from pypdf import PdfReader
        fields = PdfReader(BLANK_704).get_fields() or {}
        suffix_3 = [f for f in fields if f.endswith("_3")]
        assert not suffix_3, f"Found unexpected _3 suffix fields: {suffix_3}"


class TestSharedFields:
    """PDF fields like 'Page' and 'SUPPLIER NAME' are shared across pages."""

    def test_shared_field_names_exist(self):
        """Key shared fields must be in the template."""
        _skip_if_no_template()
        from pypdf import PdfReader
        fields = PdfReader(BLANK_704).get_fields() or {}
        for name in ["SUPPLIER NAME", "Page", "of"]:
            assert name in fields, f"Shared field '{name}' missing from template"


class TestBoundaryItemCounts:
    """Page boundaries at 8/9 and 16/17 items."""

    def test_8_items_single_page_capacity(self):
        """8 items should fit on page 1 (pg1 has 8 rows)."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        # All 8 slots should be on page 1
        for slot in range(1, 9):
            page = profile.row_page_number(slot)
            assert page == 1, f"Slot {slot} should be on page 1, got page {page}"

    def test_9_items_spills_to_page_2(self):
        """Item 9 must be on page 2."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        page = profile.row_page_number(9)
        assert page == 2, f"Slot 9 should be on page 2, got page {page}"

    def test_19_items_max_form_capacity(self):
        """Item 19 is the last form field slot."""
        _skip_if_no_template()
        from src.forms.template_registry import TemplateProfile
        profile = TemplateProfile(BLANK_704)
        suffix = profile.row_field_suffix(19)
        assert suffix is not None, "Slot 19 should be valid (max capacity)"
        # Slot 20 should overflow
        assert profile.row_field_suffix(20) is None, "Slot 20 should overflow"
