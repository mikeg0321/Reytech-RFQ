"""Regression guard for the row_index off-by-one bug in _parse_ams704_ocr
that caused pc_e922bd5c to show duplicate 'line item 7' on the SCU Group
Tx Materials PDF.

Root cause: the OCR fallback path was re-numbering items with
`it["row_index"] = i` (0-indexed) while everywhere else in the parser
uses `i + 1` (1-indexed). The downstream sanitizer clamps
`max(1, row_index)`, so items 1 and 2 both collapsed onto row_index=1,
and every subsequent row shifted wrong.

These tests pin the correct 1-indexed behavior so the bug can't
silently return.
"""
import os

import pytest


FIX_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "unified_ingest",
)
SCU_FILLED = os.path.join(FIX_DIR, "pc_pdf_scu_reytech_filled.pdf")
SCU_BLANK = os.path.join(FIX_DIR, "pc_pdf_scu_blank.pdf")


@pytest.mark.skipif(
    not os.path.exists(SCU_FILLED),
    reason="SCU filled fixture missing",
)
class TestScuRowIndexUnique:
    def test_parses_all_26_items(self):
        from src.forms.price_check import parse_ams704
        r = parse_ams704(SCU_FILLED)
        items = r.get("line_items", [])
        assert len(items) == 26, (
            f"SCU PDF should parse 26 items, got {len(items)}"
        )

    def test_row_indices_are_unique(self):
        """No two items may share a row_index. The off-by-one bug
        at line 2070 caused items 1 and 2 to both land at row_index=1
        after the `max(1, ...)` sanitizer clamp."""
        from src.forms.price_check import parse_ams704
        r = parse_ams704(SCU_FILLED)
        items = r.get("line_items", [])
        row_indices = [i["row_index"] for i in items]
        duplicates = [x for x in set(row_indices) if row_indices.count(x) > 1]
        assert not duplicates, (
            f"duplicate row_index values: {duplicates} — "
            f"the SCU line-item-7 bug has returned"
        )

    def test_row_indices_are_sequential_1_indexed(self):
        from src.forms.price_check import parse_ams704
        r = parse_ams704(SCU_FILLED)
        items = r.get("line_items", [])
        row_indices = sorted(i["row_index"] for i in items)
        assert row_indices == list(range(1, len(items) + 1)), (
            f"row_indices must be 1-indexed and sequential, got {row_indices}"
        )

    def test_row_index_matches_item_number(self):
        """For auto-sequential buyer forms, item_number and row_index
        must agree. If they diverge, the UI will render items out of
        order vs what the buyer saw on their PDF."""
        from src.forms.price_check import parse_ams704
        r = parse_ams704(SCU_FILLED)
        items = r.get("line_items", [])
        for i, item in enumerate(items):
            expected = i + 1
            assert item["row_index"] == expected, (
                f"item {i+1}: row_index={item['row_index']} expected {expected}"
            )
            assert item["item_number"] == str(expected), (
                f"item {i+1}: item_number={item['item_number']!r} expected {str(expected)!r}"
            )
