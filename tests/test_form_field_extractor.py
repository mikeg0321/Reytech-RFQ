"""PR-AV1 — form-field extractor substrate.

Pins the failure class from rfq_9e63456e + rfq_efbdef4a (2026-05-14):
both RFQs landed with wrong scalars because the body-regex
extractor reads PDF template *labels* ("Solicitation Number:",
"Due Date:") but never reads the buyer-typed AcroForm field VALUES
below them. Result: default 5/18 due date, "PREQ 10847262" sol#
that came from the email subject not the form, etc.

The extractor reads AcroForm field values via pypdf and:
  1. Normalizes field names case/punctuation-insensitively
  2. Maps name aliases to canonical contract scalars
  3. Normalizes sol# by stripping subject-prefix tokens
     (PREQ/REQ/RFQ/etc.)
  4. Normalizes date fields to ISO YYYY-MM-DD

Tests pin the normalizers (don't require live PDF parsing — that
would couple tests to pypdf install + test PDF fixtures). The
end-to-end PDF extraction is tested via a synthetic AcroForm PDF
when pypdf is available.
"""
from __future__ import annotations

import pytest


def test_normalize_sol_num_strips_preq_prefix():
    from src.agents.form_field_extractor import _normalize_sol_num
    assert _normalize_sol_num("PREQ 10847262") == "10847262"
    assert _normalize_sol_num("PREQ#10847262") == "10847262"
    assert _normalize_sol_num("preq 10847262") == "10847262"
    assert _normalize_sol_num("PREQ: 10847262") == "10847262"


def test_normalize_sol_num_strips_other_prefixes():
    from src.agents.form_field_extractor import _normalize_sol_num
    assert _normalize_sol_num("RFQ # 25CB021") == "25CB021"
    assert _normalize_sol_num("BID-12345") == "12345"
    assert _normalize_sol_num("SOL 99999") == "99999"
    assert _normalize_sol_num("QUOTE: ABC-001") == "ABC-001"


def test_normalize_sol_num_preserves_already_clean():
    from src.agents.form_field_extractor import _normalize_sol_num
    assert _normalize_sol_num("10847262") == "10847262"
    assert _normalize_sol_num("25CB021") == "25CB021"
    assert _normalize_sol_num("R26Q44") == "R26Q44"


def test_normalize_sol_num_handles_empty():
    from src.agents.form_field_extractor import _normalize_sol_num
    assert _normalize_sol_num("") == ""
    assert _normalize_sol_num("   ") == ""


def test_normalize_sol_num_rejects_digit_less_values():
    """Substrate invariant (2026-05-26 / Coleman 10842771): a real
    sol# always contains at least one digit. Digit-less form-field
    values are template defaults / category labels, not buyer input.
    Returning "" lets `attachment_contract_parser` fall back to the
    email-derived sol# instead of overwriting `10842771` with
    `NON-IT`. The previous behavior leaked 'PREQ' / 'NON-IT' through.
    """
    from src.agents.form_field_extractor import _normalize_sol_num
    # Form-template / category labels — REJECT
    assert _normalize_sol_num("NON-IT") == ""
    assert _normalize_sol_num("IT-GOODS") == ""
    assert _normalize_sol_num("GOODS") == ""
    # Just "PREQ" alone (the prefix only, no digits) — also REJECT
    assert _normalize_sol_num("PREQ") == ""
    # Real digit-bearing values still pass
    assert _normalize_sol_num("10842771") == "10842771"
    assert _normalize_sol_num("PREQ 10842771") == "10842771"
    assert _normalize_sol_num("25CB021") == "25CB021"


def test_normalize_date_mdy_slash():
    """05/15/2026 → 2026-05-15 (the actual 25CB021 / 10847262 date)."""
    from src.agents.form_field_extractor import _normalize_date
    assert _normalize_date("05/15/2026") == "2026-05-15"
    assert _normalize_date("5/15/2026") == "2026-05-15"
    assert _normalize_date("5/5/26") == "2026-05-05"


def test_normalize_date_mdy_dash():
    from src.agents.form_field_extractor import _normalize_date
    assert _normalize_date("05-15-2026") == "2026-05-15"
    assert _normalize_date("5-15-26") == "2026-05-15"


def test_normalize_date_iso_passthrough():
    from src.agents.form_field_extractor import _normalize_date
    assert _normalize_date("2026-05-15") == "2026-05-15"
    assert _normalize_date("2026-5-15") == "2026-05-15"


def test_normalize_date_long_form():
    from src.agents.form_field_extractor import _normalize_date
    assert _normalize_date("May 15, 2026") == "2026-05-15"
    assert _normalize_date("May 15 2026") == "2026-05-15"


def test_normalize_date_empty_and_invalid():
    from src.agents.form_field_extractor import _normalize_date
    assert _normalize_date("") == ""
    assert _normalize_date("not a date") == ""
    assert _normalize_date("13/45/9999") == ""  # invalid month/day


def test_normalize_name_collapses_punctuation():
    from src.agents.form_field_extractor import _normalize_name
    assert _normalize_name("Solicitation_Number") == "solicitation number"
    assert _normalize_name("SOLICITATION-NUMBER") == "solicitation number"
    assert _normalize_name("Solicitation.Number") == "solicitation number"
    assert _normalize_name("Solicitation  Number") == "solicitation number"


def test_name_matches_sol_num_aliases():
    from src.agents.form_field_extractor import _normalize_name, _name_matches, _SOL_NUM_ALIASES
    for raw in [
        "Solicitation Number", "SolicitationNumber", "Sol_Number",
        "RFQ Number", "rfq_number", "Bid Number", "Quote Number",
    ]:
        n = _normalize_name(raw)
        assert _name_matches(n, _SOL_NUM_ALIASES), f"failed to match: {raw!r}"


def test_name_matches_due_date_aliases():
    from src.agents.form_field_extractor import _normalize_name, _name_matches, _DUE_DATE_ALIASES
    for raw in [
        "Due Date", "DueDate", "Date_Due", "Response Due", "Closing Date",
        "Submission Deadline", "Deadline",
    ]:
        n = _normalize_name(raw)
        assert _name_matches(n, _DUE_DATE_ALIASES), f"failed: {raw!r}"


def test_form_field_values_merge_keeps_existing():
    """Merge fills empty slots but doesn't overwrite non-empty ones —
    cover-sheet attachment processes FIRST, so its values stay."""
    from src.agents.form_field_extractor import FormFieldValues
    a = FormFieldValues(
        solicitation_number="10847262",
        due_date="2026-05-15",
    )
    b = FormFieldValues(
        solicitation_number="WRONG",
        due_date="2026-05-18",
        ship_to="900 Quebec Ave, Corcoran CA 93212",
    )
    a.merge(b)
    assert a.solicitation_number == "10847262"  # not overwritten
    assert a.due_date == "2026-05-15"           # not overwritten
    assert a.ship_to == "900 Quebec Ave, Corcoran CA 93212"  # filled


def test_form_field_values_has_values():
    from src.agents.form_field_extractor import FormFieldValues
    assert FormFieldValues().has_values is False
    assert FormFieldValues(solicitation_number="X").has_values is True
    assert FormFieldValues(due_date="2026-05-15").has_values is True


# ── End-to-end PDF extraction (synthetic AcroForm) ─────────────────


def _make_pdf_with_form_fields(fields: dict) -> bytes:
    """Build a minimal PDF with an AcroForm carrying the given fields.

    Uses reportlab + pypdf to compose. Returns bytes. Skips if
    reportlab/pypdf missing."""
    pytest.importorskip("pypdf")
    try:
        from reportlab.pdfgen import canvas
        from io import BytesIO
        from pypdf import PdfReader, PdfWriter
        from pypdf.generic import (
            DictionaryObject, NameObject, TextStringObject, ArrayObject,
            NumberObject, IndirectObject,
        )
    except ImportError:
        pytest.skip("reportlab not available for synthetic PDF")

    # Build a one-page PDF
    buf = BytesIO()
    c = canvas.Canvas(buf)
    c.drawString(72, 720, "Synthetic test PDF")
    # Add form fields via reportlab's acroForm
    form = c.acroForm
    y = 700
    for name, val in fields.items():
        form.textfield(
            name=name, value=str(val),
            x=72, y=y, width=200, height=18, borderWidth=0,
            forceBorder=False,
        )
        y -= 24
    c.save()
    buf.seek(0)
    return buf.read()


def test_e2e_extract_due_date_from_acroform():
    pytest.importorskip("pypdf")
    pdf_bytes = _make_pdf_with_form_fields({
        "Solicitation Number": "10847262",
        "Due Date": "05/15/2026",
    })
    from src.agents.form_field_extractor import extract_from_pdf_bytes
    ff = extract_from_pdf_bytes(pdf_bytes, source_label="test.pdf")
    assert ff is not None, "extractor returned None on synthetic PDF"
    assert ff.solicitation_number == "10847262"
    assert ff.due_date == "2026-05-15"


def test_e2e_strips_preq_prefix_from_form_field():
    """If the buyer's form field value carries a 'PREQ ' prefix
    (unlikely but possible — and the bug we're closing), strip it."""
    pytest.importorskip("pypdf")
    pdf_bytes = _make_pdf_with_form_fields({
        "Solicitation Number": "PREQ 10847262",
    })
    from src.agents.form_field_extractor import extract_from_pdf_bytes
    ff = extract_from_pdf_bytes(pdf_bytes, source_label="test.pdf")
    assert ff is not None
    assert ff.solicitation_number == "10847262"


def test_e2e_no_acroform_returns_none():
    """A plain PDF with no form fields returns None (not an empty
    FormFieldValues — callers can short-circuit)."""
    pytest.importorskip("pypdf")
    try:
        from reportlab.pdfgen import canvas
        from io import BytesIO
    except ImportError:
        pytest.skip("reportlab not available")
    buf = BytesIO()
    c = canvas.Canvas(buf)
    c.drawString(72, 720, "No form fields here")
    c.save()
    buf.seek(0)
    from src.agents.form_field_extractor import extract_from_pdf_bytes
    ff = extract_from_pdf_bytes(buf.read(), source_label="no-form.pdf")
    assert ff is None


def test_e2e_invalid_bytes_returns_none():
    from src.agents.form_field_extractor import extract_from_pdf_bytes
    assert extract_from_pdf_bytes(b"not a pdf", source_label="x") is None
    assert extract_from_pdf_bytes(b"", source_label="x") is None
