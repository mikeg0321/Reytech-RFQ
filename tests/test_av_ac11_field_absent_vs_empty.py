"""PR-AV-AC11 — verify_filled_form distinguishes "field absent from
PDF" from "field present but /V empty".

CONTEXT

Bid-package PDFs are buyer-supplied composites. Different agency
buyers ship different template variants — some carry the AMS 708
(GenAI Disclosure) page, some don't; some carry OBS 1600 (food
items), some don't; some carry STD 105 Bidder Declaration in the
bid package, others have it as a separate attachment.

The `FORM_FIELD_REGISTRY["bidpkg"].required_fields` map lists field
names that COULD appear in a CCHCS bid package: CUF fields, Darfur
fields, GSPD-05-105 fields, DVBE PD843 fields, AMS 708 fields,
STD 21 fields, CalRecycle 74 fields. Not every buyer's template
carries ALL of these.

Prior behavior: when a registry-listed field was absent from the
PDF (e.g., 708_Text1 because the buyer's bidpkg had no 708 page),
verify_filled_form treated it the same as "field present but
/V empty" — flagged "Missing: 708_Text1" as a critical issue,
failed QA. This was a false positive: Reytech can't fill a field
that isn't in the source template, AND the agency_config.required
_forms for this buyer didn't list 708 anyway.

Surfaced on rfq_9e63456e (5/15 PREQ 10847262 audit). After AC1-AC10
closed the rest of the QA-banner false-positive class, this was
the LAST remaining noise — 4 critical issues all stemming from
708_Text* fields being absent from prod's buyer bidpkg variant.

THE FIX

In `verify_filled_form`, when `actual_field is None` (PDF doesn't
contain the field widget at all), mark the detail row as N/A and
skip — don't add to issues, don't fail QA. Empty-but-present
fields (`/V == ""`) still fail (operator/fill bug).

Mirror the same logic for checkbox_fields and date_fields so the
"Missing date: <field>" duplicates don't double-count when the
section is absent entirely.

WHAT THIS TEST PINS
===================

  - PDF with required field absent → field_details row with
    status="N/A", NOT in issues list, passed remains True
  - PDF with required field present but /V empty → field_details
    row with status="FAIL", "Missing: <field>" in issues, passed
    is False
  - PDF with required field present + /V filled → field_details
    row with status="PASS"
  - date_fields and checkbox_fields apply the same N/A semantics
    when absent
  - Source-grep: PR-AV-AC11 marker present + the `is None`
    sentinel check is in all three loops (required, checkbox,
    date)
"""
from __future__ import annotations

from pathlib import Path

import pytest
from pypdf import PdfReader, PdfWriter
from pypdf.generic import (
    NameObject, TextStringObject, ArrayObject, DictionaryObject,
    NumberObject, BooleanObject,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_pdf(tmp_path, name_to_value):
    """Build a PDF with /AcroForm + given form-field widgets and
    return the path. None-valued entries get no /V (i.e. present
    but empty).
    """
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter

    raw = tmp_path / "raw.pdf"
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

    out = tmp_path / "bidpkg.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    return str(out)


def _verify(pdf, rfq_data=None, config=None):
    from src.forms.form_qa import verify_filled_form
    return verify_filled_form(
        pdf, "bidpkg",
        rfq_data or {"solicitation_number": "10847262", "sign_date": "05/15/2026"},
        config or {"company": {
            "name": "Reytech Inc.", "cert_number": "2002605",
            "fein": "47-4588061", "address": "30 Carnoustie Way",
        }},
    )


# ── Tests ───────────────────────────────────────────────────────────


def test_absent_708_field_marks_na_not_fail(tmp_path):
    """The 5/15 rfq_9e63456e scenario: buyer bidpkg has CUF +
    Darfur + 105 + PD843 + std21 + CalRecycle but NO 708 page.
    708_Text* fields are absent → must mark N/A, NOT critical.
    """
    pdf = _make_pdf(tmp_path, {
        # All non-708 required fields PRESENT and FILLED
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
        "Text1_std21": "Reytech Inc.",
        "Text2_std21": "47-4588061",
        "ContractorCompany Name": "Reytech Inc.",
        "Address": "30 Carnoustie Way",
        # 708_Text1 / 708_Text3 / 708_Text16 deliberately ABSENT
    })
    result = _verify(pdf)
    missing_708 = [i for i in result["issues"] if "708_" in i]
    assert not missing_708, (
        f"708_* fields absent from PDF must NOT be reported as "
        f"Missing; got {missing_708}"
    )
    # N/A status rows should appear in field_details
    na_708 = [d for d in result["field_details"]
              if "708_" in d["name"] and d.get("status") == "N/A"]
    assert len(na_708) == 3, (
        f"708_Text1 / 708_Text3 / 708_Text16 should each have "
        f"status='N/A' in field_details; got {na_708}"
    )
    # No 'Missing date: 708_Text16' duplicate either
    assert not any("Missing date: 708_Text16" in i for i in result["issues"])


def test_present_but_empty_field_still_fails(tmp_path):
    """Defense: a field that IS in the PDF but has /V empty must
    still fail as before. This is the operator/fill-bug case AC11
    must not silently swallow.
    """
    pdf = _make_pdf(tmp_path, {
        # 708 fields PRESENT but unfilled
        "708_Text1": None,
        "708_Text3": None,
        "708_Text16": None,
        # Everything else missing (default missing). Just check 708
        # status — we want to confirm the empty-not-absent path
        # still fails.
    })
    result = _verify(pdf)
    missing_708 = [i for i in result["issues"] if "Missing: 708_" in i]
    assert len(missing_708) == 3, (
        f"empty 708_* fields must still fail; got {missing_708}"
    )
    assert result["passed"] is False


def test_present_and_filled_passes(tmp_path):
    """Defense: a fully-filled field still passes."""
    pdf = _make_pdf(tmp_path, {
        "DOING BUSINESS AS DBA NAME_CUF": "Reytech Inc.",
        "OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY_CUF": "2002605",
    })
    result = _verify(pdf)
    cuf_pass = [d for d in result["field_details"]
                if d["name"] == "DOING BUSINESS AS DBA NAME_CUF"]
    assert cuf_pass and cuf_pass[0]["status"] == "PASS"


def test_absent_date_field_marks_na_not_critical(tmp_path):
    """The 'Missing date: 708_Text16' duplicate on rfq_9e63456e: the
    date_fields loop ALSO flagged 708_Text16 as missing-date, on top
    of the required_fields-loop flagging it as Missing:. Both stemmed
    from the same absence. AC11 silences both.
    """
    pdf = _make_pdf(tmp_path, {
        "Text1_PD843": "Reytech Inc.",
        # 708_Text16 absent → also a date_fields entry
    })
    result = _verify(pdf)
    missing_date_708 = [i for i in result["issues"]
                       if "Missing date: 708_Text16" in i]
    assert not missing_date_708, (
        f"absent 708_Text16 must not double-flag in date_fields; "
        f"got {missing_date_708}"
    )


def test_source_grep_ac11_markers_in_all_three_loops():
    src = (REPO_ROOT / "src" / "forms" / "form_qa.py").read_text(encoding="utf-8")
    assert "PR-AV-AC11" in src, "PR-AV-AC11 marker must remain in form_qa.py"
    # The `actual_field is None` check must appear in three places:
    # required_fields, checkbox_fields, date_fields loops.
    n_is_none = src.count("actual_field is None")
    assert n_is_none >= 3, (
        f"expected `actual_field is None` check in 3 loops "
        f"(required, checkbox, date); found {n_is_none}"
    )
