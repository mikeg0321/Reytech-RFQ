"""Test multi-page AMS 704 PDF generation.

Tests the fill_ams704() function with varying item counts to verify
correct field mapping, page calculations, and overflow handling.

Template layout (ams_704_blank.pdf):
  Page 1: 8 unsuffixed rows (Row1-Row8)
  Page 2: 8 _2 suffix rows (Row1_2-Row8_2) + 3 unsuffixed (Row9-Row11)
  Total form capacity: 19 items
  Items 20+: overflow pages via reportlab
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

from src.forms.price_check import fill_ams704, _detect_page_layout, ROW_FIELDS


TEMPLATE = os.path.join(os.path.dirname(__file__), "..", "data", "templates", "ams_704_blank.pdf")


def _make_items(count):
    items = []
    for i in range(1, count + 1):
        items.append({
            "row_index": i,
            "description": f"Test Item #{i} - Sample Description for testing",
            "qty": 2,
            "uom": "EA",
            "qty_per_uom": 1,
            "unit_price": 10.00 + i,
            "pricing": {"recommended_price": 10.00 + i},
        })
    return items


def _fill_and_inspect(item_count, label):
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        output = tmp.name
    try:
        result = fill_ams704(
            source_pdf=TEMPLATE,
            parsed_pc={"line_items": _make_items(item_count), "header": {"institution": "Test"}, "ship_to": "Test"},
            output_pdf=output,
            price_tier="recommended",
        )
        fv_path = os.path.join(os.path.dirname(__file__), "..", "data", "pc_field_values.json")
        with open(fv_path) as f:
            field_values = json.load(f)
        from pypdf import PdfReader
        pdf_pages = len(PdfReader(output).pages) if os.path.exists(output) else 0
        return result, field_values, pdf_pages
    finally:
        if os.path.exists(output):
            os.unlink(output)


# ---- Tests ----

def test_detect_page_layout():
    """Verify layout detection: 8 on pg1, 8 _2 suffix, 3 extra unsuffixed on pg2."""
    from pypdf import PdfReader
    fields = PdfReader(TEMPLATE).get_fields() or {}
    pg1, pg2_suf, pg2_extra = _detect_page_layout(fields, source_pdf=TEMPLATE)
    assert pg1 == 8, f"Expected 8 rows on page 1, got {pg1}"
    assert pg2_suf == 8, f"Expected 8 _2 suffix rows, got {pg2_suf}"
    assert pg2_extra == 3, f"Expected 3 extra unsuffixed on page 2, got {pg2_extra}"
    print(f"  PASS: layout pg1={pg1}, pg2_suf={pg2_suf}, pg2_extra={pg2_extra}, capacity={pg1+pg2_suf+pg2_extra}")


def test_5_items():
    """5 items -> 1 page, all unsuffixed."""
    result, fv, pages = _fill_and_inspect(5, "5 items")
    assert result["ok"]
    assert pages == 1, f"Expected 1 page, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    assert "QTYRow5" in fv_map, "Missing QTYRow5"
    print(f"  PASS: 5 items -> {pages} page(s)")


def test_8_items():
    """8 items = exactly fills page 1."""
    result, fv, pages = _fill_and_inspect(8, "8 items")
    assert result["ok"]
    assert pages == 1, f"Expected 1 page, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    assert "QTYRow8" in fv_map, "Missing QTYRow8"
    print(f"  PASS: 8 items -> {pages} page(s), page 1 full")


def test_9_items():
    """9 items = page 1 (8) + 1 item on page 2 (Row1_2)."""
    result, fv, pages = _fill_and_inspect(9, "9 items")
    assert result["ok"]
    assert pages == 2, f"Expected 2 pages, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 9 -> Row1_2
    assert "QTYRow1_2" in fv_map, "Missing QTYRow1_2 for item 9"
    assert fv_map["QTYRow1_2"].strip() == "2", f"QTYRow1_2 wrong: {fv_map.get('QTYRow1_2')}"
    print(f"  PASS: 9 items -> {pages} page(s), item 9 -> Row1_2")


def test_16_items():
    """16 items = page 1 (8) + page 2 _2 suffix (8). Row1_2 through Row8_2."""
    result, fv, pages = _fill_and_inspect(16, "16 items")
    assert result["ok"]
    assert pages == 2, f"Expected 2 pages, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    assert "QTYRow8_2" in fv_map, "Missing QTYRow8_2 for item 16"
    print(f"  PASS: 16 items -> {pages} page(s)")


def test_17_items():
    """17 items = page 1 (8) + page 2 _2 suffix (8) + 1 extra unsuffixed (Row9)."""
    result, fv, pages = _fill_and_inspect(17, "17 items")
    assert result["ok"]
    assert pages == 2, f"Expected 2 pages, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 17 -> Row9 (unsuffixed, on page 2)
    assert "QTYRow9" in fv_map, "Missing QTYRow9 for item 17"
    assert fv_map["QTYRow9"].strip() == "2", f"QTYRow9 wrong: {fv_map.get('QTYRow9')}"
    print(f"  PASS: 17 items -> {pages} page(s), item 17 -> Row9 (pg2 extra)")


def test_19_items():
    """19 items = max form capacity (8 + 8 + 3). All on 2 pages."""
    result, fv, pages = _fill_and_inspect(19, "19 items")
    assert result["ok"]
    assert pages == 2, f"Expected 2 pages, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 19 -> Row11 (unsuffixed, last slot on page 2)
    assert "QTYRow11" in fv_map, "Missing QTYRow11 for item 19"
    assert fv_map["QTYRow11"].strip() == "2", f"QTYRow11 wrong: {fv_map.get('QTYRow11')}"
    # Item 16 -> Row8_2
    assert "QTYRow8_2" in fv_map, "Missing QTYRow8_2"
    # Item 9 -> Row1_2
    assert "QTYRow1_2" in fv_map, "Missing QTYRow1_2"
    print(f"  PASS: 19 items -> {pages} page(s), max capacity")


def test_25_items():
    """25 items = 19 form + 6 overflow. 3 pages."""
    result, fv, pages = _fill_and_inspect(25, "25 items")
    assert result["ok"]
    # 2 pages from form fill + 1 overflow page
    assert pages == 3, f"Expected 3 pages, got {pages}"
    fv_map = {f["field_id"]: f["value"] for f in fv}
    assert "QTYRow11" in fv_map, "Missing QTYRow11"
    assert "QTYRow8_2" in fv_map, "Missing QTYRow8_2"
    assert "QTYRow1_3" not in fv_map, "Should not have _3 suffix fields"
    print(f"  PASS: 25 items -> {pages} page(s), overflow created")


def test_all_fields_have_suffix():
    """All field types for page 2 _2 suffix items have correct suffix."""
    result, fv, pages = _fill_and_inspect(14, "14 items all fields")
    assert result["ok"]
    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 14 -> Row6_2 (14 - 8 = 6th on page 2 _2 section)
    for field_name, label in [
        ("ITEM Row6_2", "item_number"),
        ("QTYRow6_2", "qty"),
        ("UNIT OF MEASURE UOMRow6_2", "uom"),
        ("PRICE PER UNITRow6_2", "unit_price"),
        ("EXTENSIONRow6_2", "extension"),
    ]:
        assert field_name in fv_map, f"Missing {field_name} ({label}) for item 14"
        assert fv_map[field_name].strip() not in ("", " "), f"{field_name} is blank"
    print(f"  PASS: all field types for item 14 have _2 suffix")


def test_pg2_extra_fields():
    """Item 17 (first pg2_extra) maps to unsuffixed Row9, not _2 suffix."""
    result, fv, pages = _fill_and_inspect(18, "18 items pg2 extra")
    assert result["ok"]
    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 17 -> Row9 (unsuffixed), Item 18 -> Row10 (unsuffixed)
    assert "QTYRow9" in fv_map, "Missing QTYRow9 for item 17"
    assert "QTYRow10" in fv_map, "Missing QTYRow10 for item 18"
    # These should NOT have _2 suffix
    assert "QTYRow9_2" not in fv_map or fv_map.get("QTYRow9_2", "").strip() in ("", " "), \
        "QTYRow9_2 should not have data (Row9 is unsuffixed)"
    print(f"  PASS: items 17-18 -> Row9, Row10 (unsuffixed on page 2)")


def test_summary_totals():
    """Verify subtotal calculation."""
    result, fv, pages = _fill_and_inspect(5, "5 items totals")
    assert result["ok"]
    expected_sub = sum((10.0 + i) * 2 for i in range(1, 6))
    assert result["summary"]["subtotal"] == expected_sub, \
        f"Subtotal {result['summary']['subtotal']} != {expected_sub}"
    print(f"  PASS: subtotal = ${expected_sub:.2f}")


if __name__ == "__main__":
    if not os.path.exists(TEMPLATE):
        print(f"ERROR: Template not found at {TEMPLATE}")
        sys.exit(1)

    tests = [
        test_detect_page_layout,
        test_5_items,
        test_8_items,
        test_9_items,
        test_16_items,
        test_17_items,
        test_19_items,
        test_25_items,
        test_all_fields_have_suffix,
        test_pg2_extra_fields,
        test_summary_totals,
    ]

    passed = failed = 0
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
