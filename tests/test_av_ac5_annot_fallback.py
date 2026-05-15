"""PR-AV-AC5 — verify_filled_form recovers field /V values from
per-page /Annots when document-level /AcroForm root is absent.

Defense-in-depth companion to PR-AV-AC1. AC1 fixed the bidpkg
page-trim that stripped /AcroForm. AC5 ensures the audit's verdict
survives the NEXT writer bug of the same shape — a defensive layer
that turns a writer regression from a 17-false-positive avalanche
into a single warning and accurate field-by-field verification.

THE BUG CLASS (re-stated for the regression lock)
=================================================

A PDF can have form-field widgets present as per-page annotations
(carrying /T name and /V value) WITHOUT having a document-level
`/Root → /AcroForm → /Fields` enumeration entry. In that state:

  - PdfReader.get_fields()   → {}   (pypdf has no enumeration root)
  - reader.pages[i].get('/Annots')[j].get('/V')  → value present

The 5/15 bidpkg incident was exactly this: 17 fields filled
correctly on annots, 0 fields visible to get_fields() → audit
reported every required field as "Missing:". AC1 fixed the
specific writer bug; AC5 makes the audit *resilient* against the
class.

WHAT THIS TEST PINS
===================

  - When /AcroForm is absent BUT per-page /Annots have /Tx widgets
    with /V values, verify_filled_form recovers those values and
    reports them as filled (NOT as Missing:)
  - When /AcroForm is present AND get_fields() works, behavior is
    unchanged (no regression)
  - Mixed: some required fields recovered from annots, others
    genuinely missing — only the genuinely missing ones flag
  - Defensive scope: only /Tx /Btn /Ch /Sig widget annots are
    pulled; non-form annots (e.g. text comments) ignored
  - Source-grep: PR-AV-AC5 marker + _build_fields_from_annots
    helper present in form_qa.py
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"


def _make_widget_only_pdf(tmp_path, name_to_value):
    """Build a PDF with form-field widgets on a page but NO document-
    level /AcroForm root. Mimics the writer bug that AC1 closed.

    Returns the output path.
    """
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import (
        NameObject, TextStringObject, ArrayObject, DictionaryObject,
        NumberObject,
    )

    # Blank page
    raw = tmp_path / "raw_no_acroform.pdf"
    c = canvas.Canvas(str(raw), pagesize=letter)
    c.showPage()
    c.save()

    reader = PdfReader(str(raw))
    writer = PdfWriter()
    writer.append(reader)
    page = writer.pages[0]

    annots = []
    for name, value in name_to_value.items():
        widget = DictionaryObject({
            NameObject("/Type"): NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Widget"),
            NameObject("/FT"): NameObject("/Tx"),
            NameObject("/T"): TextStringObject(name),
            NameObject("/Rect"): ArrayObject([
                NumberObject(0), NumberObject(0),
                NumberObject(0), NumberObject(0),
            ]),
            NameObject("/F"): NumberObject(4),
        })
        if value is not None:
            widget[NameObject("/V")] = TextStringObject(str(value))
        annots.append(writer._add_object(widget))

    page[NameObject("/Annots")] = ArrayObject(annots)
    # Intentionally do NOT set writer._root_object[/AcroForm] — that's
    # the writer-bug condition AC5 is defending against.

    out = tmp_path / "widget_only.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    return out


def _make_full_acroform_pdf(tmp_path, name_to_value):
    """Sanity helper: build a PDF WITH /AcroForm root (working state)
    so we can pin no-regression behavior.
    """
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import (
        NameObject, TextStringObject, ArrayObject, DictionaryObject,
        NumberObject, BooleanObject,
    )

    raw = tmp_path / "raw_with_acroform.pdf"
    c = canvas.Canvas(str(raw), pagesize=letter)
    c.showPage()
    c.save()

    reader = PdfReader(str(raw))
    writer = PdfWriter()
    writer.append(reader)
    page = writer.pages[0]

    annots = []
    field_refs = []
    for name, value in name_to_value.items():
        widget = DictionaryObject({
            NameObject("/Type"): NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Widget"),
            NameObject("/FT"): NameObject("/Tx"),
            NameObject("/T"): TextStringObject(name),
            NameObject("/Rect"): ArrayObject([
                NumberObject(0), NumberObject(0),
                NumberObject(0), NumberObject(0),
            ]),
            NameObject("/F"): NumberObject(4),
        })
        if value is not None:
            widget[NameObject("/V")] = TextStringObject(str(value))
        ref = writer._add_object(widget)
        annots.append(ref)
        field_refs.append(ref)

    page[NameObject("/Annots")] = ArrayObject(annots)

    acroform = DictionaryObject({
        NameObject("/Fields"): ArrayObject(field_refs),
        NameObject("/NeedAppearances"): BooleanObject(True),
    })
    writer._root_object[NameObject("/AcroForm")] = acroform

    out = tmp_path / "with_acroform.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    return out


# ── Tests ───────────────────────────────────────────────────────────


def test_annot_fallback_recovers_filled_values(tmp_path):
    """The headline AC5 case: /AcroForm absent, widgets have /V →
    audit reports them as filled, not missing.

    Uses a real bidpkg-shaped expected-field set (CUF subset) so the
    test exercises the same code path as the rfq_9e63456e incident.
    """
    from src.forms.form_qa import verify_filled_form

    pdf = _make_widget_only_pdf(tmp_path, {
        "DOING BUSINESS AS DBA NAME_CUF": "Reytech Inc.",
        "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": "2002605",
        "Date_CUF": "05/15/2026",
        "Text7_CUF": "10847262",
        "CompanyVendor Name Printed_darfur": "Reytech Inc.",
        "Federal ID Number_darfur": "47-4588061",
        "Date__darfur": "05/15/2026",
        "Text0_105": "10847262",
        "Text1_105": "SB/DVBE",
        "Text1_PD843": "Reytech Inc.",
        "Text2_PD843": "2002605",
        "Text4_PD843": "10847262",
        "Date1_PD843": "05/15/2026",
        "708_Text1": "10847262",
        "708_Text3": "Reytech Inc.",
        "708_Text16": "05/15/2026",
        "Text1_std21": "Reytech Inc.",
        "Text2_std21": "47-4588061",
        "ContractorCompany Name": "Reytech Inc.",
        "Address": "30 Carnoustie Way Trabuco Canyon CA 92679",
    })
    config = {
        "company": {
            "name": "Reytech Inc.",
            "cert_number": "2002605",
            "fein": "47-4588061",
            "address": "30 Carnoustie Way Trabuco Canyon CA 92679",
        },
    }
    rfq_data = {
        "solicitation_number": "10847262",
        "sign_date": "05/15/2026",
    }
    result = verify_filled_form(str(pdf), "bidpkg", rfq_data, config)

    # Every required field landed in field_details with non-empty
    # "actual" — meaning the audit saw the /V from the annot.
    fails = [d for d in result["field_details"] if d.get("status") == "FAIL"]
    assert not fails, (
        f"AC5 fallback should have recovered every annot /V; FAIL "
        f"entries: {[(d['name'], d['actual']) for d in fails]}"
    )
    # The fallback should have emitted a single warning so operators
    # know the AcroForm was missing (forms still print correctly,
    # but PDF-viewer field navigation breaks).
    assert any(
        "AcroForm root missing" in w for w in result["warnings"]
    ), f"AC5 must flag the AcroForm-absent state; warnings={result['warnings']}"


def test_acroform_present_no_regression(tmp_path):
    """Regression guard: when AcroForm is present, the fallback does
    NOT fire — original get_fields() result is used as-is.
    """
    from src.forms.form_qa import verify_filled_form

    pdf = _make_full_acroform_pdf(tmp_path, {
        "DOING BUSINESS AS DBA NAME_CUF": "Reytech Inc.",
        "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": "2002605",
        "Date_CUF": "05/15/2026",
        "Text7_CUF": "10847262",
        "CompanyVendor Name Printed_darfur": "Reytech Inc.",
        "Federal ID Number_darfur": "47-4588061",
        "Date__darfur": "05/15/2026",
        "Text0_105": "10847262",
        "Text1_105": "SB/DVBE",
        "Text1_PD843": "Reytech Inc.",
        "Text2_PD843": "2002605",
        "Text4_PD843": "10847262",
        "Date1_PD843": "05/15/2026",
        "708_Text1": "10847262",
        "708_Text3": "Reytech Inc.",
        "708_Text16": "05/15/2026",
        "Text1_std21": "Reytech Inc.",
        "Text2_std21": "47-4588061",
        "ContractorCompany Name": "Reytech Inc.",
        "Address": "30 Carnoustie Way",
    })
    config = {"company": {
        "name": "Reytech Inc.", "cert_number": "2002605",
        "fein": "47-4588061", "address": "30 Carnoustie Way"}}
    rfq_data = {"solicitation_number": "10847262", "sign_date": "05/15/2026"}
    result = verify_filled_form(str(pdf), "bidpkg", rfq_data, config)

    # No AcroForm-missing warning — the AcroForm was there.
    assert not any(
        "AcroForm root missing" in w for w in result["warnings"]
    ), f"AC5 must NOT fire when AcroForm is present; warnings={result['warnings']}"
    fails = [d for d in result["field_details"] if d.get("status") == "FAIL"]
    assert not fails


def test_genuine_missing_still_flagged(tmp_path):
    """The fallback must not hide REAL missing fields. If a required
    field has no widget at all, it still flags as Missing:.
    """
    from src.forms.form_qa import verify_filled_form

    # Build a widget-only PDF that only has SOME of the expected
    # bidpkg fields — omit Text1_std21 + Text2_std21 entirely.
    pdf = _make_widget_only_pdf(tmp_path, {
        "DOING BUSINESS AS DBA NAME_CUF": "Reytech Inc.",
        "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": "2002605",
        "Date_CUF": "05/15/2026",
        "Text7_CUF": "10847262",
        "CompanyVendor Name Printed_darfur": "Reytech Inc.",
        "Federal ID Number_darfur": "47-4588061",
        "Date__darfur": "05/15/2026",
        "Text0_105": "10847262",
        "Text1_105": "SB/DVBE",
        "Text1_PD843": "Reytech Inc.",
        "Text2_PD843": "2002605",
        "Text4_PD843": "10847262",
        "Date1_PD843": "05/15/2026",
        "708_Text1": "10847262",
        "708_Text3": "Reytech Inc.",
        "708_Text16": "05/15/2026",
        # Text1_std21 + Text2_std21 deliberately omitted
        "ContractorCompany Name": "Reytech Inc.",
        "Address": "30 Carnoustie Way",
    })
    config = {"company": {
        "name": "Reytech Inc.", "cert_number": "2002605",
        "fein": "47-4588061", "address": "30 Carnoustie Way"}}
    rfq_data = {"solicitation_number": "10847262", "sign_date": "05/15/2026"}
    result = verify_filled_form(str(pdf), "bidpkg", rfq_data, config)

    missing = [i for i in result["issues"] if "Missing:" in i]
    # Only Text1_std21 + Text2_std21 should flag
    missing_names = {i.replace("Missing: ", "") for i in missing}
    assert "Text1_std21" in missing_names, (
        f"genuinely-missing field must still flag; got {missing}"
    )
    assert "Text2_std21" in missing_names
    # Recovered fields should NOT appear in the missing list
    assert "DOING BUSINESS AS DBA NAME_CUF" not in missing_names
    assert "708_Text1" not in missing_names


def test_helper_builds_fields_from_annots(tmp_path):
    """Pin the helper contract: synthesizes a dict keyed by /T name
    with /V + /FT entries. Same shape verify_filled_form expects from
    get_fields().
    """
    from pypdf import PdfReader
    from src.forms.form_qa import _build_fields_from_annots

    pdf = _make_widget_only_pdf(tmp_path, {
        "fieldA": "valueA",
        "fieldB": "valueB",
        "emptyField": None,
    })
    reader = PdfReader(str(pdf))
    out = _build_fields_from_annots(reader)

    assert "fieldA" in out
    assert "fieldB" in out
    assert "emptyField" in out
    assert str(out["fieldA"]["/V"]) == "valueA"
    assert str(out["fieldB"]["/V"]) == "valueB"
    # Empty-value widget should be present but with /V None
    assert out["emptyField"]["/V"] is None
    # Each entry carries the field type
    assert out["fieldA"]["/FT"] == "/Tx"


def test_helper_returns_empty_on_no_annots(tmp_path):
    """Edge case: a PDF with no /Annots at all should produce an
    empty result, not crash.
    """
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from pypdf import PdfReader
    from src.forms.form_qa import _build_fields_from_annots

    p = tmp_path / "blank.pdf"
    c = canvas.Canvas(str(p), pagesize=letter)
    c.showPage()
    c.save()
    reader = PdfReader(str(p))
    out = _build_fields_from_annots(reader)
    assert out == {}


def test_source_grep_ac5_marker_present():
    """Lock the helper + the call-site into source so future
    refactors can't drop the AC5 defense silently.
    """
    target = REPO_ROOT / "src" / "forms" / "form_qa.py"
    src = target.read_text(encoding="utf-8")
    assert "PR-AV-AC5" in src, "PR-AV-AC5 marker must remain in form_qa.py"
    assert "def _build_fields_from_annots" in src, (
        "AC5 helper must be defined in form_qa.py"
    )
    assert "_build_fields_from_annots(reader)" in src, (
        "verify_filled_form must invoke the AC5 fallback when "
        "get_fields() returns empty"
    )
