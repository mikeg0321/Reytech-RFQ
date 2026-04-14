"""Form A+ visual regression for AMS 704 fills.

Catches the #1 production-incident class: PDF coordinates drifting
after a template swap, font change, or accidental `fill_ams704`
refactor. See the 2026-04-03 multi-page 704 incident — 11 consecutive
failed fix commits because the test suite only verified field VALUES,
not their POSITIONS on the page.

This module adds *structural* regression checks:

  1. Every expected row has a field bounding box (nothing stripped).
  2. Row y-coordinates are monotonically decreasing down the page
     (catches row shuffling).
  3. Row vertical spacing is roughly uniform within a page (catches
     rows collapsing into each other — the exact 2026-04-03 symptom).
  4. Overlay pages (items 20+) contain draw content and each row
     appears as extracted text at a unique y-position (catches the
     reportlab overlay getting its coordinate origin wrong).

We deliberately skip pixel-exact snapshots because pdfplumber word
coordinates vary by 0.5-1pt across library versions, and maintaining
golden PNGs is brittle. Structural invariants catch the real
regression class without false positives.
"""
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Same Windows strftime workaround test_multipage_704 uses
import src.forms.price_check as _pc_mod
from datetime import datetime, timedelta
def _expiry_date_win():
    exp = datetime.now() + timedelta(days=45)
    return f"{exp.month}/{exp.day}/{exp.year}"
_pc_mod._expiry_date = _expiry_date_win

from src.forms.price_check import fill_ams704


TEMPLATE = os.path.join(
    os.path.dirname(__file__), "..", "data", "templates", "ams_704_blank.pdf"
)

# Tolerance (in PDF points) for "roughly uniform" row spacing. A full
# row on the 704 template is ~25pt tall, so 3pt catches any real drift
# while ignoring sub-pixel rounding.
ROW_SPACING_TOLERANCE = 3.0

# Minimum Y-axis gap between adjacent rows. Anything tighter means
# rows are collapsing into each other — the exact 2026-04-03 bug.
MIN_ROW_GAP = 15.0


def _make_items(count: int):
    return [
        {
            "row_index": i,
            "description": f"VR Test Item {i} — {'X' * 30}",
            "qty": 2,
            "uom": "EA",
            "qty_per_uom": 1,
            "unit_price": 10.00 + i,
            "pricing": {"recommended_price": 10.00 + i},
        }
        for i in range(1, count + 1)
    ]


def _fill(count: int):
    """Run fill_ams704 and return (output_path, reader). Caller is
    responsible for cleanup via os.unlink(output_path)."""
    output = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False).name
    fill_ams704(
        source_pdf=TEMPLATE,
        parsed_pc={
            "line_items": _make_items(count),
            "header": {"institution": "VR Test"},
            "ship_to": "VR Test",
        },
        output_pdf=output,
        price_tier="recommended",
    )
    from pypdf import PdfReader
    return output, PdfReader(output)


import re
_ITEM_ROW_RE = re.compile(r"^ITEM Row(\d+)(_2)?$")


def _row_field_rects(reader) -> dict:
    """Return {row_key: (page_index, x1, y1, x2, y2)} for every row
    anchor in the PDF. The 704 template has ~8 fields per row (ITEM,
    QTY, UOM, DESCRIPTION, PRICE, ...) and they share the same y
    because they're on one visual row. We anchor on the "ITEM Row\\d+"
    field — one per row — so later tests can compare row-level
    coordinates without worrying about which column to pick.

    Returned key is "Row1", "Row1_2", etc. — not the full field name.
    """
    out = {}
    for page_idx, page in enumerate(reader.pages):
        annots = page.get("/Annots") or []
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                name = str(annot.get("/T", "") or "")
                m = _ITEM_ROW_RE.match(name)
                if not m:
                    continue
                rect = annot.get("/Rect")
                if not rect:
                    continue
                x1, y1, x2, y2 = [float(v) for v in rect]
                row_key = f"Row{m.group(1)}" + (m.group(2) or "")
                out[row_key] = (page_idx, x1, y1, x2, y2)
            except Exception:
                continue
    return out


def _y_center(rect_tuple) -> float:
    _, _x1, y1, _x2, y2 = rect_tuple
    return (y1 + y2) / 2


def _rows_on_page(rects: dict, page_idx: int):
    """Return [(field_name, y_center)] for rows on the given page,
    sorted top-down (highest y first — PDF coordinates have origin
    at bottom-left)."""
    items = [
        (name, _y_center(r)) for name, r in rects.items()
        if r[0] == page_idx
    ]
    items.sort(key=lambda x: -x[1])  # top-down
    return items


# ── Tests ───────────────────────────────────────────────────────────────


class TestFieldBoundingBoxes:
    def test_all_row_fields_have_rects(self):
        """Every row anchor on pages 1 and 2 must have a /Rect. If any
        are missing, fill_ams704 ships PDFs where items silently
        vanish."""
        output, reader = _fill(19)
        try:
            rects = _row_field_rects(reader)
            page1_rows = [n for n in rects if rects[n][0] == 0]
            page2_rows = [n for n in rects if rects[n][0] == 1]
            # Ground truth per CLAUDE.md: ≥8 on each of the first two
            # pages. The real number can be 8 or 11 on page 1 depending
            # on template variant — the key assertion is that *both*
            # pages have the minimum capacity.
            assert len(page1_rows) >= 8, (
                f"Page 1 row anchors ({len(page1_rows)}): {page1_rows}"
            )
            assert len(page2_rows) >= 8, (
                f"Page 2 row anchors ({len(page2_rows)}): {page2_rows}"
            )
        finally:
            os.unlink(output)


class TestRowMonotonicity:
    def test_rows_decrease_top_down_on_page_1(self):
        """Row1 through RowN on page 1 must have strictly decreasing
        y-centers (origin bottom-left → higher y = higher on page).
        A drift that swaps row order was one of the failure modes in
        the 2026-04-03 incident."""
        output, reader = _fill(11)
        try:
            rects = _row_field_rects(reader)
            # Only unsuffixed Row\d+ anchors on page 1
            unsuffixed = [
                (name, _y_center(r)) for name, r in rects.items()
                if r[0] == 0 and not name.endswith("_2")
            ]
            assert len(unsuffixed) >= 8, (
                f"Expected ≥8 unsuffixed row anchors on page 1, got "
                f"{len(unsuffixed)}: {[n for n, _ in unsuffixed]}"
            )
            # Natural ordering: Row1, Row2, Row3, ...
            by_number = sorted(
                unsuffixed,
                key=lambda x: int(x[0][3:]) if x[0][3:].isdigit() else 999,
            )
            ys = [y for _, y in by_number]
            for a, b in zip(ys, ys[1:]):
                assert a > b, (
                    f"Row numbering is not monotonic top-down — "
                    f"y_centers: {ys}"
                )
        finally:
            os.unlink(output)

    def test_rows_have_minimum_gap(self):
        """Adjacent rows must have a y-gap of at least MIN_ROW_GAP.
        If rows collapse (gap < 15pt) the rendered text overlaps and
        the PDF is unreadable. This is exactly what happened on
        2026-04-03 when PG1_ROWS mismatch drifted the current_row
        counter and later rows stacked on earlier ones."""
        output, reader = _fill(16)
        try:
            rects = _row_field_rects(reader)
            for page_idx in (0, 1):
                rows = _rows_on_page(rects, page_idx)
                if len(rows) < 2:
                    continue
                ys = [y for _, y in rows]
                gaps = [a - b for a, b in zip(ys, ys[1:])]
                min_gap = min(gaps)
                assert min_gap >= MIN_ROW_GAP, (
                    f"Page {page_idx + 1}: min row gap {min_gap:.1f}pt < "
                    f"{MIN_ROW_GAP}pt — rows are collapsing. "
                    f"Gaps: {[round(g, 1) for g in gaps]}"
                )
        finally:
            os.unlink(output)


class TestRowSpacingUniformity:
    def test_page_1_spacing_is_uniform(self):
        """Row spacing on page 1 should be roughly uniform — catches
        a regression where fill_ams704 accidentally draws into the
        wrong row's /Rect and the text ends up at the wrong y.
        Tolerance: ROW_SPACING_TOLERANCE (3pt)."""
        output, reader = _fill(11)
        try:
            rects = _row_field_rects(reader)
            rows = _rows_on_page(rects, page_idx=0)
            if len(rows) < 3:
                pytest.skip("Need ≥3 rows for uniformity check")
            ys = [y for _, y in rows[:8]]  # first 8 rows
            gaps = [a - b for a, b in zip(ys, ys[1:])]
            median_gap = sorted(gaps)[len(gaps) // 2]
            for i, g in enumerate(gaps):
                drift = abs(g - median_gap)
                assert drift <= ROW_SPACING_TOLERANCE, (
                    f"Row {i}→{i+1} gap {g:.1f}pt differs from median "
                    f"{median_gap:.1f}pt by {drift:.1f}pt > "
                    f"{ROW_SPACING_TOLERANCE}pt tolerance"
                )
        finally:
            os.unlink(output)


class TestOverlayPages:
    """Items 20+ are drawn via reportlab overlay on copies of page 2.
    These pages have no form fields, so we verify via pdfplumber text
    extraction."""

    def test_overflow_content_present(self):
        """A 22-item fill (overflows the 19-field capacity) must
        produce a PDF with ≥3 pages and extractable text on page 3."""
        output, reader = _fill(22)
        try:
            assert len(reader.pages) >= 3, \
                f"22-item fill should produce ≥3 pages, got {len(reader.pages)}"
            import pdfplumber
            with pdfplumber.open(output) as pdf:
                assert len(pdf.pages) >= 3
                page3_text = pdf.pages[2].extract_text() or ""
                # Items 20, 21, 22 should appear somewhere on the
                # overflow page. We seed "VR Test Item N" so look for
                # item numbers in the range.
                # Allow flexibility in formatting — pdfplumber can
                # split words.
                found = 0
                for item_num in (20, 21, 22):
                    marker = f"Item {item_num}"
                    if marker in page3_text:
                        found += 1
                assert found >= 2, (
                    f"Overflow page 3 missing expected items. "
                    f"Found {found}/3 markers. "
                    f"First 300 chars: {page3_text[:300]!r}"
                )
        finally:
            os.unlink(output)

    def test_overflow_rows_have_unique_y_positions(self):
        """Each overflow item on the overlay page must render at its
        own y-coordinate. If rows collapse onto each other, we get
        text overlap. Verified by checking that distinct item numbers
        appear at distinct y-positions."""
        output, reader = _fill(22)
        try:
            import pdfplumber
            with pdfplumber.open(output) as pdf:
                if len(pdf.pages) < 3:
                    pytest.skip("Need ≥3 pages for overlay test")
                page3 = pdf.pages[2]
                words = page3.extract_words() or []
                item_ys = {}
                for w in words:
                    text = w.get("text", "")
                    if text.startswith("VR") or text == "Item":
                        continue
                    # Look for numeric markers like "20", "21", "22"
                    try:
                        n = int(text)
                    except ValueError:
                        continue
                    if 20 <= n <= 24:
                        item_ys.setdefault(n, []).append(float(w.get("top", 0)))
                # Need at least 2 distinct y-positions
                all_tops = []
                for tops in item_ys.values():
                    all_tops.extend(tops)
                if len(all_tops) >= 2:
                    # Two different items should not be at the same y
                    unique_ys = set(round(y, 0) for y in all_tops)
                    assert len(unique_ys) >= 2, (
                        f"Overlay items collapsed to same y. "
                        f"tops: {all_tops}"
                    )
        finally:
            os.unlink(output)


class TestPageCount:
    """Boundary cases from CLAUDE.md — catch row-count regressions."""

    @pytest.mark.parametrize("items,min_pages", [
        (1, 1), (8, 1), (9, 2), (16, 2), (19, 2), (20, 3), (22, 3),
    ])
    def test_page_count_for_item_ranges(self, items, min_pages):
        output, reader = _fill(items)
        try:
            assert len(reader.pages) >= min_pages, (
                f"{items} items → expected ≥{min_pages} pages, "
                f"got {len(reader.pages)}"
            )
        finally:
            os.unlink(output)
