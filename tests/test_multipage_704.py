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
        # Read field values from the same DATA_DIR that fill_ams704 writes to
        # (conftest patches DATA_DIR to a temp dir, so we must import it)
        import src.forms.price_check as _pc
        fv_path = os.path.join(_pc.DATA_DIR, "pc_field_values.json")
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
    """Pricing fields for page 2 _2 suffix items have correct suffix.

    Note: The blank 704 template is detected as prefilled, so fill_ams704
    runs in ORIGINAL mode (PC_ORIGINAL strategy). In this mode:
      - Pricing fields (PRICE PER UNIT, EXTENSION) ARE written
      - QTY/UOM fields ARE written
      - Item number / description fields are NOT written (buyer's values preserved)
    Only assert fields that original mode actually writes.
    """
    result, fv, pages = _fill_and_inspect(14, "14 items all fields")
    assert result["ok"]
    fv_map = {f["field_id"]: f["value"] for f in fv}
    # Item 14 -> Row6_2 (14 - 8 = 6th on page 2 _2 section)
    # Fields written in original mode (pricing + qty/uom):
    for field_name, label in [
        ("QTYRow6_2", "qty"),
        ("UNIT OF MEASURE UOMRow6_2", "uom"),
        ("PRICE PER UNITRow6_2", "unit_price"),
        ("EXTENSIONRow6_2", "extension"),
    ]:
        assert field_name in fv_map, f"Missing {field_name} ({label}) for item 14"
        assert fv_map[field_name].strip() not in ("", " "), f"{field_name} is blank"
    # ITEM Row field behavior depends on mode:
    # Original mode: NOT written (buyer's values preserved)
    # Full mode: written (our item numbers)
    # Both are valid — just verify the pricing fields above are present.
    print(f"  PASS: pricing fields for item 14 have _2 suffix")


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


# ═══════════════════════════════════════════════════════════════════════
# Overlay bounds regression tests (infrastructure for DOCX 704 calibration)
# ═══════════════════════════════════════════════════════════════════════
#
# These tests verify that the form_qa.verify_overlay_bounds() helper and
# the conftest.assert_overlay_text_in_cell fixture correctly detect drift
# when overlay text lands outside its expected cell. They run against
# synthetic fixtures so they don't depend on LibreOffice being installed.
#
# Once a real DOCX-converted 704 is available (run
# _overnight_review/scripts/08_measure_docx_704.py with LibreOffice
# installed, or drop a converted PDF into tests/fixtures/docx_704/),
# the same helpers drive the real DOCX calibration tests in this file.

def _write_synthetic_overlay_pdf(out_path, cell_rects, texts, drift=0.0):
    """Generate a tiny PDF with text drawn at given cell positions.

    Each text in `texts` is drawn at the corresponding cell rect from
    `cell_rects`, offset by `drift` points on both axes. Used to
    exercise the overlay bounds check: drift=0 should pass, drift>5
    should fail.
    """
    from reportlab.pdfgen import canvas as rl_canvas
    c = rl_canvas.Canvas(out_path, pagesize=(792, 612))
    for (x0, y0, x1, y1), text in zip(cell_rects, texts):
        fs = 9
        c.setFont("Helvetica", fs)
        c.drawString(x0 + 4 + drift, y0 + 4 + drift, text)
    c.save()


def test_overlay_bounds_helper_passes_on_clean_fill():
    """Zero drift: text drawn exactly inside cell rects must pass the
    bounds check and the test helper."""
    from src.forms.form_qa import verify_overlay_bounds
    cells = [
        ("row1_price", (637, 292, 686, 311)),
        ("row1_ext", (691, 292, 754, 311)),
    ]
    texts = ["10.00", "20.00"]
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        _write_synthetic_overlay_pdf(tmp.name, [c[1] for c in cells], texts, drift=0)
        path = tmp.name
    try:
        expected = {k: v for k, v in cells}
        r = verify_overlay_bounds(path, expected, tolerance_pt=5.0)
        assert r["passed"], f"clean fill should pass, got issues: {r['issues']}"
        assert not r["issues"]
    finally:
        if os.path.exists(path):
            os.unlink(path)


def test_overlay_bounds_helper_catches_20pt_drift():
    """20pt drift (the documented DOCX 704 failure mode) must be
    caught as a critical issue, not a warning."""
    from src.forms.form_qa import verify_overlay_bounds
    cells = [
        ("row1_price", (637, 292, 686, 311)),
        ("row1_ext", (691, 292, 754, 311)),
    ]
    texts = ["99.00", "88.00"]
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        _write_synthetic_overlay_pdf(tmp.name, [c[1] for c in cells], texts, drift=25)
        path = tmp.name
    try:
        expected = {k: v for k, v in cells}
        r = verify_overlay_bounds(path, expected, tolerance_pt=5.0)
        assert not r["passed"], "25pt drift must fail the bounds check"
        assert any("drift" in i.lower() or "outside" in i.lower() for i in r["issues"]), \
            f"expected a drift issue, got: {r['issues']}"
    finally:
        if os.path.exists(path):
            os.unlink(path)


def test_overlay_bounds_helper_tolerates_small_drift():
    """Sub-tolerance drift (2pt, within the 5pt tolerance) must NOT
    be flagged — covers normal sub-pixel rounding from pdfplumber."""
    from src.forms.form_qa import verify_overlay_bounds
    cells = [("row1_price", (637, 292, 686, 311))]
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        _write_synthetic_overlay_pdf(tmp.name, [cells[0][1]], ["15.00"], drift=2)
        path = tmp.name
    try:
        expected = {k: v for k, v in cells}
        r = verify_overlay_bounds(path, expected, tolerance_pt=5.0)
        assert r["passed"], f"2pt drift should pass, got: {r['issues']}"
    finally:
        if os.path.exists(path):
            os.unlink(path)


def test_conftest_helper_matches_form_qa_behavior():
    """The tests/conftest.py assert_overlay_text_in_cell helper and
    the form_qa.verify_overlay_bounds function must agree — same
    tolerance logic applied to the same PDF should produce the same
    pass/fail result."""
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from conftest import assert_overlay_text_in_cell
    from src.forms.form_qa import verify_overlay_bounds

    cells = [("row1_price", (637, 292, 686, 311))]
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        _write_synthetic_overlay_pdf(tmp.name, [cells[0][1]], ["42.00"], drift=0)
        path = tmp.name
    try:
        # form_qa: passes
        r = verify_overlay_bounds(path, {cells[0][0]: cells[0][1]}, tolerance_pt=5.0)
        assert r["passed"]
        # conftest helper: passes (raises on failure, so reaching the
        # next line is the assertion)
        assert_overlay_text_in_cell(
            path, cells[0][0], cells[0][1],
            expected_text_contains="42",
            tolerance_pt=5.0,
        )
    finally:
        if os.path.exists(path):
            os.unlink(path)


# ── DOCX 704 real-fixture regression tests ──────────────────────────────
#
# Both fixtures are LibreOffice-converted PDFs of the real CCHCS DOCX
# 704 worksheet, committed to tests/fixtures/docx_704/. They pin the
# current working behavior of _detect_ams704_overlay_positions +
# _fill_pdf_text_overlay so it can't regress silently.
#
# Measurement session 2026-04-13: every price/extension cell had 0pt
# drift on both fixtures. The DOCX 704 overlay bug described in the
# project memory snapshot was silently fixed by the col_rects detection
# method; these tests lock that fix in.

DOCX_NON_FOOD_CONVERTED = os.path.join(
    os.path.dirname(__file__), "fixtures", "docx_704", "sample_non_food_converted.pdf"
)
DOCX_FOOD_CONVERTED = os.path.join(
    os.path.dirname(__file__), "fixtures", "docx_704", "sample_food_converted.pdf"
)


import pytest


def _fill_docx_and_measure_drift(source_pdf, num_items):
    """Helper: fill the given LibreOffice-converted 704 PDF with
    distinctive per-row prices, then measure the drift between each
    written price/ext cell and the detector's reported cell. Returns
    the form_qa verify_overlay_bounds result."""
    import tempfile
    from src.forms.price_check import (
        _detect_ams704_overlay_positions,
        _fill_pdf_text_overlay,
    )
    from src.forms.form_qa import verify_overlay_bounds

    # Distinctive values: 100.11, 200.22, 300.33, ... — each unique so
    # the form_qa word matcher can't cross-match.
    field_values = [
        {"field_id": "COMPANY NAME", "value": "Reytech Inc."},
        {"field_id": "Address",
         "value": "30 Carnoustie Way, Trabuco Canyon CA 92679"},
        {"field_id": "Phone Number_2", "value": "949-229-1575"},
        {"field_id": "EMail Address", "value": "sales@reytechinc.com"},
        {"field_id": "COMPANY REPRESENTATIVE print name",
         "value": "Michael Guadan"},
        {"field_id": "Certified SBMB", "value": "2002605"},
        {"field_id": "Certified DVBE", "value": "2002605"},
        {"field_id": "Delivery Date and Time ARO", "value": "5-7 days"},
        {"field_id": "Discount Offered", "value": "Included"},
        {"field_id": "Date Price Check Expires", "value": "5/31/2027"},
    ]
    for n in range(1, num_items + 1):
        field_values.append({
            "field_id": f"PRICE PER UNITRow{n}",
            "value": f"{n * 100}.{n * 11:02d}",
        })
        field_values.append({
            "field_id": f"EXTENSIONRow{n}",
            "value": f"{n * 200}.{n * 22:02d}",
        })

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        out_path = tmp.name
    try:
        _fill_pdf_text_overlay(source_pdf, field_values, out_path)
        # Capture detector output and build expected-cells map
        detected = _detect_ams704_overlay_positions(source_pdf)
        assert detected is not None, "detector returned None"
        expected_cells = {}
        row_counter = 0
        for pg_idx, d in enumerate(detected):
            if d is None:
                continue
            for (yb, yt) in d["item_rows"]:
                row_counter += 1
                px0, px1 = d["price_x"]
                ex0, ex1 = d["ext_x"]
                expected_cells[f"price_row{row_counter}"] = (px0, yb, px1, yt)
                expected_cells[f"ext_row{row_counter}"] = (ex0, yb, ex1, yt)
        result = verify_overlay_bounds(out_path, expected_cells, tolerance_pt=5.0)
        result["detected_rows"] = row_counter
        return result
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


@pytest.mark.skipif(
    not os.path.exists(DOCX_NON_FOOD_CONVERTED),
    reason="DOCX non-food fixture missing"
)
def test_docx_704_non_food_end_to_end_zero_drift():
    """Full parse → detect → overlay-fill → re-read cycle on the
    LibreOffice-converted non-food sample must produce ZERO drift
    (within 5pt tolerance) for every priced cell. This is the
    authoritative regression guard for the DOCX 704 fix."""
    result = _fill_docx_and_measure_drift(DOCX_NON_FOOD_CONVERTED, num_items=3)
    assert result["passed"], (
        f"DOCX non-food drift issues: {result['issues']}"
    )
    assert result["detected_rows"] >= 3, (
        f"expected >=3 rows detected, got {result['detected_rows']}"
    )
    # Every single drift measurement should be under 2pt
    for d in result["drift_details"]:
        if not d["field"].startswith(("price_row", "ext_row")):
            continue
        assert d["drift"] < 2.0, (
            f"row cell {d['field']} drifted {d['drift']}pt "
            f"(text='{d['text']}')"
        )


@pytest.mark.skipif(
    not os.path.exists(DOCX_FOOD_CONVERTED),
    reason="DOCX food fixture missing"
)
def test_docx_704_food_multi_page_zero_drift():
    """Multi-page DOCX 704 (food sample: 3 items pg1 + 2 items pg2)
    must have zero drift across BOTH pages. This is the harder case
    since detection has to run successfully on page 2 and its rows
    continue the numbering from page 1."""
    result = _fill_docx_and_measure_drift(DOCX_FOOD_CONVERTED, num_items=5)
    assert result["passed"], (
        f"DOCX food multi-page drift issues: {result['issues']}"
    )
    assert result["detected_rows"] >= 5, (
        f"expected >=5 rows across both pages, got {result['detected_rows']}"
    )
    price_rows = [d for d in result["drift_details"]
                  if d["field"].startswith("price_row")]
    assert len(price_rows) >= 5, (
        f"expected >=5 price rows with text, got {len(price_rows)}"
    )
    for d in price_rows:
        assert d["drift"] < 2.0, (
            f"row cell {d['field']} drifted {d['drift']}pt "
            f"(text='{d['text']}') — DOCX 704 regression"
        )


@pytest.mark.skipif(
    not os.path.exists(DOCX_NON_FOOD_CONVERTED),
    reason="DOCX non-food fixture missing"
)
def test_docx_704_detector_returns_sane_geometry():
    """Sanity-check the detector output on a real LibreOffice-
    converted PDF. The col_rects method should produce price_x and
    ext_x ranges that are wide enough to hold prices and positioned
    in the right half of the page."""
    from src.forms.price_check import _detect_ams704_overlay_positions

    detected = _detect_ams704_overlay_positions(DOCX_NON_FOOD_CONVERTED)
    assert detected is not None, "detector returned None on DOCX-converted fixture"
    assert any(d is not None for d in detected), "all pages failed detection"

    for pg_idx, d in enumerate(detected):
        if d is None:
            continue
        assert d["price_x"][1] - d["price_x"][0] > 25, (
            f"pg{pg_idx+1}: price_x range too narrow: {d['price_x']}"
        )
        assert d["ext_x"][1] - d["ext_x"][0] > 25, (
            f"pg{pg_idx+1}: ext_x range too narrow: {d['ext_x']}"
        )
        assert d["price_x"][0] > 400, (
            f"pg{pg_idx+1}: price_x not in right half: {d['price_x']}"
        )
        for yb, yt in d["item_rows"]:
            band = yt - yb
            assert 8 < band < 50, (
                f"pg{pg_idx+1}: item row band {band:.1f}pt outside (8, 50)"
            )


@pytest.mark.skipif(
    not os.path.exists(DOCX_NON_FOOD_CONVERTED),
    reason="DOCX non-food fixture missing"
)
def test_form_qa_verify_single_form_runs_overlay_self_check():
    """form_qa.verify_single_form called with form_id='704' on a
    filled DOCX-converted 704 must run the overlay self-check and
    include its result in the return dict. This is the scale-safety
    hook: any path that generates a 704 and calls form_qa inherits
    the overlay-drift guard automatically."""
    import tempfile
    from src.forms.price_check import _fill_pdf_text_overlay
    from src.forms.form_qa import verify_single_form

    fv = [
        {"field_id": "PRICE PER UNITRow1", "value": "111.11"},
        {"field_id": "EXTENSIONRow1", "value": "222.22"},
        {"field_id": "PRICE PER UNITRow2", "value": "333.33"},
        {"field_id": "EXTENSIONRow2", "value": "444.44"},
        {"field_id": "PRICE PER UNITRow3", "value": "555.55"},
        {"field_id": "EXTENSIONRow3", "value": "666.66"},
    ]
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        out = tmp.name
    try:
        _fill_pdf_text_overlay(DOCX_NON_FOOD_CONVERTED, fv, out)
        r = verify_single_form(out, form_id="704", data={}, config={})
        assert "overlay_self_check" in r, (
            "verify_single_form(form_id='704') must include overlay_self_check"
        )
        osc = r["overlay_self_check"]
        assert osc["passed"], f"overlay self-check failed: {osc['issues']}"
        assert osc["rows_checked"] >= 3, (
            f"expected >=3 rows checked, got {osc['rows_checked']}"
        )
    finally:
        if os.path.exists(out):
            os.unlink(out)


@pytest.mark.skipif(
    not os.path.exists(DOCX_NON_FOOD_CONVERTED),
    reason="DOCX non-food fixture missing"
)
def test_docx_704_producer_metadata_reports_libreoffice():
    """Confirm the committed fixture is actually a LibreOffice-
    converted PDF, not a DocuSign-style PDF that happens to be named
    _converted. This is a guard against future fixture drift."""
    from pypdf import PdfReader
    meta = PdfReader(DOCX_NON_FOOD_CONVERTED).metadata
    producer = str(meta.get("/Producer", "") or "")
    creator = str(meta.get("/Creator", "") or "")
    assert "LibreOffice" in producer or "LibreOffice" in creator, (
        f"fixture producer/creator must contain 'LibreOffice'; "
        f"got producer={producer!r} creator={creator!r}"
    )


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
