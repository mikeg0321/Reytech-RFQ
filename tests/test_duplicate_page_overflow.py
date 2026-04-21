"""Duplicate-page overflow implementation — PR #317.

PR #316 parsed the `overflow:` block + added a guard that refused to fill
when items exceeded capacity without an implemented strategy. This PR lifts
the capability gap: when a profile declares `overflow.mode: duplicate_page`,
the fill engine clones the source row-page as many times as needed, renaming
each row-field widget with `_{page}` suffix, so items past the base capacity
land in distinct, independently addressable PDF form fields.

These tests use the real `cchcs_it_rfq_blank.pdf` fixture (18 pages, row
widgets on PDF page 5, 10 rows per page) to prove the end-to-end behavior.
"""
from __future__ import annotations

import io
from decimal import Decimal

import pytest
from pypdf import PdfReader

from src.core.quote_model import Quote, LineItem, QuoteHeader, DocType
from src.forms.fill_engine import fill
from src.forms.profile_registry import load_profiles


@pytest.fixture
def cchcs_it_rfq_profile():
    profiles = load_profiles()
    p = profiles.get("cchcs_it_rfq_reytech_standard")
    if p is None:
        pytest.skip("cchcs_it_rfq_reytech_standard profile not in registry")
    return p


def _quote(n_items: int) -> Quote:
    return Quote(
        doc_type=DocType.PC,
        header=QuoteHeader(),
        line_items=[
            LineItem(
                line_no=i + 1,
                description=f"ITEM {i + 1} DESC",
                qty=Decimal("1"),
                unit_cost=Decimal("10.00"),
                markup_pct=Decimal("0"),
            )
            for i in range(n_items)
        ],
    )


class TestYamlSourcePage:
    """Sanity: cchcs_it_rfq YAML advertises source_page=5, matching the
    actual PDF layout where row widgets live on page 5."""

    def test_source_page_is_five(self, cchcs_it_rfq_profile):
        assert cchcs_it_rfq_profile.overflow.get("source_page") == 5

    def test_blank_pdf_row_widgets_live_on_page_five(self, cchcs_it_rfq_profile):
        r = PdfReader(cchcs_it_rfq_profile.blank_pdf)
        page5_annots = r.pages[4].get("/Annots")
        if hasattr(page5_annots, "get_object"):
            page5_annots = page5_annots.get_object()
        names = set()
        for ref in list(page5_annots or []):
            ao = ref.get_object()
            names.add(str(ao.get("/T", "")))
        # Row-field widgets should be present on page 5
        assert "Item Description1" in names
        assert "Qty1" in names
        assert "Item Description10" in names


class TestFillAtBaseCapacity:
    """At or under the 10-row base capacity, no page duplication happens."""

    def test_ten_items_does_not_duplicate(self, cchcs_it_rfq_profile):
        result = fill(_quote(10), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        # Blank is 18 pages; no duplication expected at capacity.
        assert len(r.pages) == 18

    def test_ten_items_fills_base_fields_only(self, cchcs_it_rfq_profile):
        result = fill(_quote(10), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        fields = r.get_fields() or {}
        assert "ITEM 1 DESC" in str(fields.get("Item Description1", {}).get("/V", ""))
        assert "ITEM 10 DESC" in str(fields.get("Item Description10", {}).get("/V", ""))
        # No _2-suffixed field should exist at capacity
        assert "Item Description1_2" not in fields


class TestFillAtFifteenItems:
    """11-20 items triggers one duplicate row-page."""

    def test_adds_one_page(self, cchcs_it_rfq_profile):
        result = fill(_quote(15), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        assert len(r.pages) == 19

    def test_items_one_through_ten_on_base_page(self, cchcs_it_rfq_profile):
        result = fill(_quote(15), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        fields = r.get_fields() or {}
        for n in range(1, 11):
            v = str(fields.get(f"Item Description{n}", {}).get("/V", ""))
            assert f"ITEM {n} DESC" in v, f"row {n} missing on base page: {v!r}"

    def test_items_eleven_through_fifteen_on_duplicate_page(self, cchcs_it_rfq_profile):
        result = fill(_quote(15), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        fields = r.get_fields() or {}
        # Item 11 → row 1 of duplicate page → suffix _2
        assert "ITEM 11 DESC" in str(fields.get("Item Description1_2", {}).get("/V", ""))
        assert "ITEM 15 DESC" in str(fields.get("Item Description5_2", {}).get("/V", ""))
        # Rows 6-10 of the duplicate page exist but aren't populated
        assert fields.get("Item Description10_2") is not None


class TestFillAtTwentyFiveItems:
    """21-30 items triggers two duplicate row-pages."""

    def test_adds_two_pages(self, cchcs_it_rfq_profile):
        result = fill(_quote(25), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        assert len(r.pages) == 20

    def test_suffix_three_fields_populated(self, cchcs_it_rfq_profile):
        result = fill(_quote(25), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        fields = r.get_fields() or {}
        # Item 21 → row 1 of second duplicate → suffix _3
        assert "ITEM 21 DESC" in str(fields.get("Item Description1_3", {}).get("/V", ""))
        assert "ITEM 25 DESC" in str(fields.get("Item Description5_3", {}).get("/V", ""))

    def test_all_twenty_five_items_land_distinct_fields(self, cchcs_it_rfq_profile):
        result = fill(_quote(25), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        fields = r.get_fields() or {}

        def _field_name(item_no: int) -> str:
            if item_no <= 10:
                return f"Item Description{item_no}"
            page_ordinal = 2 + (item_no - 11) // 10  # items 11-20 → 2, 21-30 → 3
            row_on_page = ((item_no - 1) % 10) + 1
            return f"Item Description{row_on_page}_{page_ordinal}"

        for i in range(1, 26):
            name = _field_name(i)
            v = str(fields.get(name, {}).get("/V", ""))
            assert f"ITEM {i} DESC" in v, (
                f"item {i} expected in {name!r} but got {v!r}"
            )


class TestDuplicatedFieldsIndependent:
    """Duplicated row fields must hold distinct values — proves the clone
    gave each widget its own /V, not a shared reference."""

    def test_row1_and_row1_suffix_two_differ(self, cchcs_it_rfq_profile):
        result = fill(_quote(11), cchcs_it_rfq_profile)
        r = PdfReader(io.BytesIO(result))
        fields = r.get_fields() or {}
        v1 = str(fields.get("Item Description1", {}).get("/V", ""))
        v1_2 = str(fields.get("Item Description1_2", {}).get("/V", ""))
        assert v1 != v1_2
        assert "ITEM 1 DESC" in v1
        assert "ITEM 11 DESC" in v1_2
