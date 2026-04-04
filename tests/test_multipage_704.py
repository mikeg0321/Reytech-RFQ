"""Test multi-page AMS 704 PDF generation.

Tests the fill_ams704() function with varying item counts to verify
correct field mapping, page calculations, and overflow handling.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Patch _expiry_date for Windows (%-m is Unix-only strftime)
import src.forms.price_check as _pc_mod
from datetime import datetime, timedelta
def _expiry_date_win():
    exp = datetime.now() + timedelta(days=45)
    return f"{exp.month}/{exp.day}/{exp.year}"
_pc_mod._expiry_date = _expiry_date_win

from src.forms.price_check import fill_ams704, _detect_pg1_rows, ROW_FIELDS


# ── Helpers ──

TEMPLATE = os.path.join(os.path.dirname(__file__), "..", "data", "templates", "ams_704_blank.pdf")


def _make_items(count: int) -> list:
    """Generate mock line items."""
    items = []
    for i in range(1, count + 1):
        items.append({
            "row_index": i,
            "description": f"Test Item #{i} -Sample Description for testing purposes",
            "qty": 2,
            "uom": "EA",
            "qty_per_uom": 1,
            "unit_price": 10.00 + i,
            "pricing": {"recommended_price": 10.00 + i},
        })
    return items


def _make_parsed_pc(count: int) -> dict:
    return {
        "line_items": _make_items(count),
        "header": {"institution": "Test Institution"},
        "ship_to": "Test Location",
    }


def _fill_and_inspect(item_count: int, label: str):
    """Fill a 704 with N items and return the field_values JSON for inspection."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        output = tmp.name
    try:
        result = fill_ams704(
            source_pdf=TEMPLATE,
            parsed_pc=_make_parsed_pc(item_count),
            output_pdf=output,
            price_tier="recommended",
        )
        # Read the field_values that were written
        fv_path = os.path.join(os.path.dirname(__file__), "..", "data", "pc_field_values.json")
        with open(fv_path) as f:
            field_values = json.load(f)

        # Also check output PDF page count
        from pypdf import PdfReader
        if os.path.exists(output):
            reader = PdfReader(output)
            pdf_pages = len(reader.pages)
        else:
            pdf_pages = 0

        return result, field_values, pdf_pages
    finally:
        if os.path.exists(output):
            os.unlink(output)


# ── Tests ──

def test_detect_pg1_rows():
    """Verify _detect_pg1_rows reads the correct count from the blank template."""
    from pypdf import PdfReader
    reader = PdfReader(TEMPLATE)
    fields = reader.get_fields() or {}
    pg1_rows = _detect_pg1_rows(fields)
    assert pg1_rows == 11, f"Expected 11 rows on page 1, got {pg1_rows}"
    print(f"  PASS: _detect_pg1_rows = {pg1_rows}")


def test_5_items():
    """5 items = 1 page. All fields unsuffixed."""
    result, fv, pages = _fill_and_inspect(5, "5 items")
    assert result["ok"], f"Fill failed: {result}"
    assert pages == 1, f"Expected 1 page, got {pages}"

    # Check field names are unsuffixed
    fv_map = {f["field_id"]: f["value"] for f in fv}
    assert "QTYRow5" in fv_map, "Missing QTYRow5 (unsuffixed)"
    assert "QTYRow5_2" not in fv_map or fv_map.get("QTYRow5_2", "").strip() in ("", " "), \
        "QTYRow5_2 should not have real data"
    print(f"  PASS: 5 items ->{pages} page(s), all unsuffixed")


def test_11_items():
    """11 items = exactly fills page 1. No page 2 needed."""
    result, fv, pages = _fill_and_inspect(11, "11 items")
    assert result["ok"], f"Fill failed: {result}"
    assert pages == 1, f"Expected 1 page, got {pages}"

    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Row 11 should be unsuffixed
    assert "QTYRow11" in fv_map, "Missing QTYRow11 (unsuffixed)"
    # No _2 suffix data fields should have real values
    qty_2_fields = [k for k in fv_map if k.startswith("QTY") and "_2" in k and fv_map[k].strip() not in ("", " ")]
    assert len(qty_2_fields) == 0, f"Unexpected _2 data fields: {qty_2_fields}"
    print(f"  PASS: 11 items ->{pages} page(s), page 1 full, no _2 data")


def test_12_items():
    """12 items = page 1 full + 1 item on page 2."""
    result, fv, pages = _fill_and_inspect(12, "12 items")
    assert result["ok"], f"Fill failed: {result}"
    assert pages == 2, f"Expected 2 pages, got {pages}"

    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 12 ->Row1_2
    assert "QTYRow1_2" in fv_map, "Missing QTYRow1_2 for item 12"
    assert fv_map["QTYRow1_2"].strip() == "2", f"QTYRow1_2 value wrong: {fv_map.get('QTYRow1_2')}"
    # Item 11 ->Row11 (unsuffixed)
    assert "QTYRow11" in fv_map, "Missing QTYRow11 for item 11"
    print(f"  PASS: 12 items ->{pages} page(s), item 12 ->Row1_2")


def test_19_items():
    """19 items = page 1 (11) + page 2 (8) -max form capacity."""
    result, fv, pages = _fill_and_inspect(19, "19 items")
    assert result["ok"], f"Fill failed: {result}"
    assert pages == 2, f"Expected 2 pages, got {pages}"

    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 19 ->Row8_2 (last slot on page 2)
    assert "QTYRow8_2" in fv_map, "Missing QTYRow8_2 for item 19"
    assert fv_map["QTYRow8_2"].strip() == "2", f"QTYRow8_2 value wrong: {fv_map.get('QTYRow8_2')}"
    # Item 11 ->Row11 (unsuffixed)
    assert "QTYRow11" in fv_map, "Missing QTYRow11"
    # Item 12 ->Row1_2
    assert "QTYRow1_2" in fv_map, "Missing QTYRow1_2"
    print(f"  PASS: 19 items ->{pages} page(s), items fill both pages exactly")


def test_25_items():
    """25 items = page 1 (11) + page 2 (8) + overflow page 3 (6 items)."""
    result, fv, pages = _fill_and_inspect(25, "25 items")
    assert result["ok"], f"Fill failed: {result}"
    # Should be 3 pages: 2 from form fill + 1 overflow
    assert pages == 3, f"Expected 3 pages, got {pages}"

    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Items 1-11 unsuffixed, 12-19 _2 suffix
    assert "QTYRow11" in fv_map, "Missing QTYRow11"
    assert "QTYRow8_2" in fv_map, "Missing QTYRow8_2"
    # Items 20-25 should NOT have form field entries (handled by overflow)
    assert "QTYRow1_3" not in fv_map, "Should not have _3 suffix fields"
    print(f"  PASS: 25 items ->{pages} page(s), overflow page created")


def test_price_fields_have_suffix():
    """Verify that price/extension fields for page 2 items use _2 suffix."""
    result, fv, pages = _fill_and_inspect(15, "15 items price suffix")
    assert result["ok"]

    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 15 ->Row4_2 (15 - 11 = 4th on page 2)
    price_key = "PRICE PER UNITRow4_2"
    ext_key = "EXTENSIONRow4_2"
    assert price_key in fv_map, f"Missing {price_key}"
    assert ext_key in fv_map, f"Missing {ext_key}"
    assert fv_map[price_key].strip() != "", f"{price_key} is empty"
    print(f"  PASS: item 15 price ->{price_key} = {fv_map[price_key]}")


def test_all_fields_have_suffix_for_page2():
    """Every field type (item#, qty, uom, desc, price, ext, sub) must use _2 for page 2 items."""
    result, fv, pages = _fill_and_inspect(14, "14 items all fields suffix")
    assert result["ok"]

    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 14 ->Row3_2 (14 - 11 = 3rd on page 2)
    expected_fields = [
        ("ITEM Row3_2", "item_number"),
        ("QTYRow3_2", "qty"),
        ("UNIT OF MEASURE UOMRow3_2", "uom"),
        ("PRICE PER UNITRow3_2", "unit_price"),
        ("EXTENSIONRow3_2", "extension"),
    ]
    for field_name, label in expected_fields:
        assert field_name in fv_map, f"Missing {field_name} ({label}) for item 14"
        assert fv_map[field_name].strip() not in ("", " "), \
            f"{field_name} ({label}) is blank: '{fv_map[field_name]}'"
    print(f"  PASS: all field types for item 14 have _2 suffix")


def test_summary_totals():
    """Verify subtotal calculation is correct."""
    result, fv, pages = _fill_and_inspect(5, "5 items totals")
    assert result["ok"]
    # Items: price = 11, 12, 13, 14, 15; qty = 2 each
    # Extensions: 22, 24, 26, 28, 30 = 130
    expected_sub = sum((10.0 + i) * 2 for i in range(1, 6))
    assert result["summary"]["subtotal"] == expected_sub, \
        f"Subtotal {result['summary']['subtotal']} != expected {expected_sub}"
    print(f"  PASS: subtotal = ${expected_sub:.2f}")


# ── Runner ──

if __name__ == "__main__":
    if not os.path.exists(TEMPLATE):
        print(f"ERROR: Template not found at {TEMPLATE}")
        sys.exit(1)

    tests = [
        test_detect_pg1_rows,
        test_5_items,
        test_11_items,
        test_12_items,
        test_19_items,
        test_25_items,
        test_price_fields_have_suffix,
        test_all_fields_have_suffix_for_page2,
        test_summary_totals,
    ]

    passed = 0
    failed = 0
    for t in tests:
        try:
            print(f"\n{t.__name__}:")
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)}")
    sys.exit(0 if failed == 0 else 1)
