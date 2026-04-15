"""Regression guard for the 704B field-naming convention parse failure
that caused RFQ 20260413_215152_19d88d (CCHCS RFQ 10837703) to parse
zero items on 2026-04-15.

Root cause: `parse_ams704` only knew the 704A (Price Check) field name
templates — "ITEM DESCRIPTION NOUN FIRST ... Row1", etc. The CCHCS
"704B Acquisition Quote Worksheet" uses a different naming scheme with
"ITEM DESCRIPTION PRODUCT SPECIFICATIONRow1" and "ITEM NUMBERRow1". The
fuzzy-fallback matcher used `str(row_n) in fname`, which mis-targets
(row 1 matches Row10, Row11, Row1_2…) and returned nothing usable, so
the pipeline reported ok=True with items_parsed=0.

Second bug surfaced by the same PDF: the post-parse sort key forced
`int(item_number)`, then ordered items ascending. On 704B forms the
ITEM NUMBER column holds MFG#/SKU codes ("W14105", "4056", "24354534"),
so sorting as ints scrambled the buyer's row order.

Both fixes live in `parse_ams704`: detect the 704B convention and swap
to a local template map, and skip the numeric sort when item_numbers
don't look like sequential small integers.
"""
import os
import tempfile
from unittest.mock import patch, MagicMock


def _fake_704b_fields():
    """A minimal field dict matching the CCHCS 704B Acquisition Quote
    Worksheet field naming (ITEM DESCRIPTION PRODUCT SPECIFICATIONRowN,
    ITEM NUMBERRowN, QTYRowN, UOMRowN)."""
    rows = [
        ("W14105",   "GAME, SEQUENCE",                "1", "BOX"),
        ("W13879",   "GAME, BIG BOGGLE",              "1", "BOX"),
        ("24354534", "POUCHES, TO LAMINATE, LTR.",    "1", "PACK"),
        ("379465",   "MARKER, DRY ERASE, ASST.",      "1", "PACK"),
        ("4056",     "DVD GAME, DEAL OR NO DEAL",     "1", "BOX"),
    ]
    out = {}
    for i, (itemno, desc, qty, uom) in enumerate(rows, start=1):
        out[f"ITEM NUMBERRow{i}"] = {"/V": itemno}
        out[f"ITEM DESCRIPTION PRODUCT SPECIFICATIONRow{i}"] = {"/V": desc}
        out[f"QTYRow{i}"] = {"/V": qty}
        out[f"UOMRow{i}"] = {"/V": uom}
    return out, rows


class _StubReader:
    def __init__(self, fields):
        self._fields = fields

    def get_fields(self):
        return self._fields


def _run_parse_with_stub(stub_fields):
    """Invoke parse_ams704 with PdfReader swapped for a stub."""
    tf = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tf.write(b"%PDF-1.4\n%stub\n")
    tf.close()
    try:
        with patch("src.forms.price_check.PdfReader",
                   return_value=_StubReader(stub_fields)):
            from src.forms.price_check import parse_ams704
            return parse_ams704(tf.name)
    finally:
        try:
            os.unlink(tf.name)
        except OSError:
            pass


def test_704b_convention_parses_all_items():
    fields, rows = _fake_704b_fields()
    result = _run_parse_with_stub(fields)
    items = result.get("line_items", [])
    assert len(items) == len(rows), (
        f"expected {len(rows)} items, got {len(items)} "
        f"(704B convention detection regressed)"
    )


def test_704b_convention_preserves_row_order():
    """On 704B the ITEM NUMBER column is MFG#/SKU, not sequential — the
    parser must return items in the buyer's Row1→RowN order, not sorted
    by ITEM NUMBER converted to int."""
    fields, rows = _fake_704b_fields()
    result = _run_parse_with_stub(fields)
    items = result.get("line_items", [])
    expected_item_nums = [r[0] for r in rows]
    got = [it.get("item_number") for it in items]
    assert got == expected_item_nums, (
        f"row order scrambled — expected {expected_item_nums}, got {got}. "
        "The post-parse numeric sort must be skipped when item_numbers "
        "are SKU codes."
    )


def test_704b_convention_captures_qty_and_uom():
    fields, rows = _fake_704b_fields()
    result = _run_parse_with_stub(fields)
    items = result.get("line_items", [])
    for it, (_, _, qty, uom) in zip(items, rows):
        assert it.get("qty") == int(qty), (
            f"qty lost for {it.get('item_number')}: {it.get('qty')!r} != {qty}"
        )
        assert it.get("uom", "").upper() == uom, (
            f"uom lost for {it.get('item_number')}: "
            f"{it.get('uom')!r} != {uom!r}"
        )


def test_704a_convention_still_sorts_sequential_item_numbers():
    """Guard rail: the multi-page 704A re-sort must still fire when the
    buyer's ITEM NUMBER column holds sequential small integers."""
    fields = {}
    # Intentionally add items out of order: 3, 1, 2
    rows_out_of_order = [
        ("3", "THIRD ITEM"),
        ("1", "FIRST ITEM"),
        ("2", "SECOND ITEM"),
    ]
    for i, (itemno, desc) in enumerate(rows_out_of_order, start=1):
        fields[f"ITEM Row{i}"] = {"/V": itemno}
        fields[(
            "ITEM DESCRIPTION NOUN FIRST Include manufacturer part "
            f"number andor reference numberRow{i}"
        )] = {"/V": desc}
        fields[f"QTYRow{i}"] = {"/V": "1"}
        fields[f"UNIT OF MEASURE UOMRow{i}"] = {"/V": "EA"}

    result = _run_parse_with_stub(fields)
    items = result.get("line_items", [])
    assert [it.get("item_number") for it in items] == ["1", "2", "3"], (
        "sequential small-int item_numbers must still be sorted — "
        "the multi-page 704A fix should not have regressed"
    )
