"""Audit CC (2026-04-23): CalRecycle 074 boundary regression guard.

Mike's 2026-04-23 directive:

    "cal recycle pulls from item list, be prepared for multiple pages
     if 30+ items. Never silently drop items."

`fill_calrecycle_standalone` in `reytech_filler_v4.py` ALREADY
implements the overflow logic — page 1 holds 6 items, overflow pages
each hold 6 more, and a reference table page is appended at the end.
Lines 2977-3025 of reytech_filler_v4.py handle batch=6 splitting + pypdf
merge.

But the behavior wasn't boundary-tested. This file adds the
regression guards per the audit memo's explicit "1 / capacity /
capacity+1 / 2×capacity" contract — boundary testing discipline
that kept 704 fills honest (see `test_multipage_704.py` for the
template).

### Contract
- **Capacity per page:** 6 items (hardcoded in
  `_calrecycle_overlay_items(output_path, items[:6])` +
  `batch = remaining[batch_start:batch_start + 6]`).
- **Page count formula:** `ceil(N/6)` filled pages + 1 reference
  table page at the end. Page 1 is the filled first page.
  For N=0..6: 2 pages total (filled + ref). For N=7..12: 3 pages.
  Etc.
- **Silent drop:** zero items ever dropped. The filler iterates
  `range(0, len(remaining), 6)` so every item ends up in exactly
  one overflow batch.
"""
from __future__ import annotations

import os

import pytest

# Skip this suite entirely when the blank template fixture is
# missing (e.g., on a barebones clone without the /data/templates
# directory populated).
_TEMPLATE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "templates",
)
_TEMPLATE = os.path.join(_TEMPLATE_DIR, "calrecycle_74_blank.pdf")

pytestmark = pytest.mark.skipif(
    not os.path.exists(_TEMPLATE),
    reason="data/templates/calrecycle_74_blank.pdf not available",
)


REYTECH_CFG = {
    "company": {
        "name": "Reytech Inc.",
        "address": "30 Carnoustie, Trabuco Canyon, CA 92679",
        "phone": "(949) 555-0100",
        "owner": "Michael Guadan",
        "title": "Owner",
        "email": "sales@reytechinc.com",
    },
}


def _mk_items(n):
    """Generate n distinct-looking items so the test can tell
    whether any were dropped."""
    return [
        {
            "line_number": str(i),
            "item_number": f"SKU-{i:04d}",
            "description": f"Test item {i} with enough text",
            "qty": 1,
            "uom": "EA",
            "price_per_unit": 10.0,
        }
        for i in range(1, n + 1)
    ]


def _mk_rfq(items):
    return {
        "solicitation_number": f"AUDIT-CC-{len(items):03d}",
        "line_items": items,
        "sign_date": "2026-04-23",
    }


def _page_count(path):
    from pypdf import PdfReader
    return len(PdfReader(path).pages)


@pytest.fixture
def output_dir(tmp_path):
    return str(tmp_path)


# ── Boundary tests ───────────────────────────────────────────────

class TestCalRecycleBoundaries:
    """Per the audit CC directive, boundary cases at 1 / capacity /
    capacity+1 / 2×capacity and beyond must all generate a usable
    PDF with zero dropped items."""

    def test_one_item_single_page_plus_ref(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_1.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(_mk_items(1)), REYTECH_CFG, out,
        )
        # 1 filled page + 1 reference table page = 2 total
        assert _page_count(out) == 2

    def test_six_items_single_page_plus_ref(self, output_dir):
        """Capacity exactly — still one filled page + one ref page."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_6.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(_mk_items(6)), REYTECH_CFG, out,
        )
        assert _page_count(out) == 2

    def test_seven_items_triggers_overflow(self, output_dir):
        """capacity+1 — must add exactly one overflow page."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_7.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(_mk_items(7)), REYTECH_CFG, out,
        )
        # page 1 (items 1-6) + overflow page (item 7) + ref = 3
        assert _page_count(out) == 3

    def test_twelve_items_two_filled_pages(self, output_dir):
        """2×capacity — exactly two filled pages + reference table."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_12.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(_mk_items(12)), REYTECH_CFG, out,
        )
        # page 1 (1-6) + overflow (7-12) + ref = 3
        assert _page_count(out) == 3

    def test_thirty_items_five_overflow_pages(self, output_dir):
        """Mike's explicit example — 30 items. Expected layout:
        page 1 (1-6) + 4 overflow pages (7-12, 13-18, 19-24, 25-30)
        + reference table = 6 pages."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_30.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(_mk_items(30)), REYTECH_CFG, out,
        )
        assert _page_count(out) == 6

    def test_thirty_one_items_six_overflow_pages(self, output_dir):
        """2×capacity+1 beyond Mike's call — 31 items forces a 5th
        overflow page to hold the singleton (item 31)."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_31.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(_mk_items(31)), REYTECH_CFG, out,
        )
        # page 1 + 5 overflow pages + ref = 7
        assert _page_count(out) == 7

    def test_zero_items_still_renders(self, output_dir):
        """Empty item list still produces a valid PDF (edge case —
        agency template rendered with 'All Items' placeholder)."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        out = os.path.join(output_dir, "cr_0.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq([]), REYTECH_CFG, out,
        )
        # 2 pages — 1 filled (with 'All Items' placeholder) + ref
        assert _page_count(out) == 2


class TestNoSilentDrop:
    """Audit CC explicitly demands 'Never silently drop items'.
    These tests extract text from each page and verify every item
    the caller supplied appears on at least one page."""

    def _extract_all_text(self, path):
        from pypdf import PdfReader
        reader = PdfReader(path)
        return "\n".join(
            (p.extract_text() or "") for p in reader.pages
        )

    def test_30_items_all_sku_numbers_survive(self, output_dir):
        """30 items — every SKU-NNNN identifier must appear in the
        rendered PDF text. If any are missing, the filler silently
        dropped them."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        items = _mk_items(30)
        out = os.path.join(output_dir, "cr_30_no_drop.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(items), REYTECH_CFG, out,
        )
        text = self._extract_all_text(out)
        # Note: item_number is rendered but may be truncated to ~10
        # chars. Check for the distinctive numeric portion.
        missing = []
        for it in items:
            sku = it["item_number"]
            # Check for the short form (filler truncates at 10 chars)
            short = sku[:10]
            if short not in text:
                missing.append(sku)
        assert not missing, (
            f"silent drop: items not in rendered PDF text: "
            f"{missing[:5]}... total={len(missing)}/30"
        )

    def test_7_items_boundary_item_rendered(self, output_dir):
        """The 7th item is the first overflow-page item — most likely
        to be dropped by an off-by-one. Lock it in."""
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        items = _mk_items(7)
        out = os.path.join(output_dir, "cr_7_boundary.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(items), REYTECH_CFG, out,
        )
        text = self._extract_all_text(out)
        assert "SKU-0007" in text, "the 7th item (first overflow) dropped"
        assert "SKU-0001" in text, "item 1 dropped"


class TestReferenceTablePreserved:
    """The reference table on page 2 of the blank template is the
    SABRC product-category code legend. Every filled package must
    include it at the END so the reviewing agency can cross-reference
    the codes. A broken pypdf merge could easily drop this page."""

    def test_30_item_package_still_has_reference_table(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        from pypdf import PdfReader
        items = _mk_items(30)
        out = os.path.join(output_dir, "cr_30_ref.pdf")
        fill_calrecycle_standalone(
            _TEMPLATE, _mk_rfq(items), REYTECH_CFG, out,
        )
        reader = PdfReader(out)
        # Reference table is the LAST page (appended after all
        # filled + overflow pages).
        last_page_text = reader.pages[-1].extract_text() or ""
        # The reference table contains SABRC category text — look
        # for common tokens. 'SABRC' is the most unique identifier.
        assert (
            "SABRC" in last_page_text
            or "Product Category" in last_page_text
            or "Recycled Content" in last_page_text
        ), (
            "reference table page missing from merged output — "
            f"last page text sample: {last_page_text[:200]!r}"
        )
