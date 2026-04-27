"""Phase 1.6 PR3b: attachment_contract_parser tests.

Asserts that PDF attachments contribute to the merged RFQRequirements
and that fill_plan_builder picks up attachment-derived required forms.
"""

import os
import tempfile
from unittest.mock import patch

import pytest

from src.agents.attachment_contract_parser import (
    _is_pdf,
    _looks_like_cover_sheet,
    _merge_into,
    merge_with_email_contract,
    parse_attachments_for_requirements,
    COVER_SHEET_TOKENS,
)
from src.agents.requirement_extractor import RFQRequirements


# ─── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
def mock_pdf_text():
    """Pretend pdfplumber returned this text for a 'bid instructions' PDF."""
    return """
    REQUEST FOR QUOTATION
    Bid Instructions

    Required forms to submit with your quote:
    - STD 204 Payee Data Record
    - STD 843 DVBE Declaration
    - DARFUR Contracting Act Certification
    - GSPD-05-105 Bidder Declaration
    - CalRecycle 74 Postconsumer Recycled-Content
    - CV 012 Commercially Useful Function (CUF)

    Quote Due: 5/15/2026 by 2:00 PM PST
    Solicitation #: RFQ-2026-CDCR-001
    """


# ─── Helper assertions ────────────────────────────────────────────────────

class TestIsPdf:
    def test_extension_match(self):
        assert _is_pdf({"filename": "test.pdf"}) is True
        assert _is_pdf({"filename": "TEST.PDF"}) is True

    def test_file_type_match(self):
        assert _is_pdf({"filename": "x", "file_type": "pdf"}) is True
        assert _is_pdf({"filename": "x", "file_type": "application/pdf"}) is True

    def test_non_pdf(self):
        assert _is_pdf({"filename": "test.docx"}) is False
        assert _is_pdf({"filename": "test.xlsx"}) is False


class TestCoverSheetDetection:
    def test_known_tokens_match(self):
        for fname in [
            "Bid_Instructions.pdf",
            "RFQ Cover Sheet.pdf",
            "Solicitation_Document.pdf",
            "BID PACKAGE & FORMS (Under 100k) - 10844466.pdf",
        ]:
            assert _looks_like_cover_sheet({"filename": fname}), \
                f"should match: {fname}"

    def test_non_cover_sheet(self):
        for fname in [
            "703B Quote Worksheet.pdf",
            "STD 204.pdf",
            "DARFUR_signed.pdf",
        ]:
            assert not _looks_like_cover_sheet({"filename": fname}), \
                f"should not match: {fname}"


class TestMergeInto:
    def test_lists_union(self):
        a = RFQRequirements(forms_required=["std204"], special_instructions=["A"])
        b = RFQRequirements(forms_required=["dvbe843", "std204"],
                            special_instructions=["B"])
        _merge_into(a, b)
        assert sorted(a.forms_required) == ["dvbe843", "std204"]
        assert a.special_instructions == ["A", "B"]

    def test_scalars_prefer_target(self):
        a = RFQRequirements(due_date="2026-05-15", solicitation_number="A1")
        b = RFQRequirements(due_date="2026-06-01", solicitation_number="B2")
        _merge_into(a, b)
        # Target wins on existing values
        assert a.due_date == "2026-05-15"
        assert a.solicitation_number == "A1"

    def test_scalars_fill_empty(self):
        a = RFQRequirements()  # all empty
        b = RFQRequirements(due_date="2026-06-01", solicitation_number="B2")
        _merge_into(a, b)
        # Empty target absorbs source
        assert a.due_date == "2026-06-01"
        assert a.solicitation_number == "B2"

    def test_food_items_or(self):
        a = RFQRequirements(food_items_present=False)
        b = RFQRequirements(food_items_present=True)
        _merge_into(a, b)
        assert a.food_items_present is True


class TestParseAttachmentsForRequirements:
    def test_returns_empty_when_no_attachments(self):
        r = parse_attachments_for_requirements([])
        assert isinstance(r, RFQRequirements)
        assert r.forms_required == []

    def test_skips_non_pdf(self):
        atts = [{"filename": "notes.docx", "file_path": "/nope"}]
        r = parse_attachments_for_requirements(atts)
        assert r.forms_required == []

    def test_parses_pdf_text_extracts_forms(self, tmp_path, mock_pdf_text):
        # Build a real (tiny) PDF on disk so pdfplumber can open it
        pdf_path = tmp_path / "Bid_Instructions.pdf"
        _write_simple_pdf(pdf_path, mock_pdf_text)
        atts = [{"filename": "Bid_Instructions.pdf",
                 "file_path": str(pdf_path)}]
        r = parse_attachments_for_requirements(atts)
        # Regex extractor should find at least the labeled forms
        assert "std204" in r.forms_required
        assert "dvbe843" in r.forms_required
        assert "darfur_act" in r.forms_required
        # Cover-sheet treated as authoritative — extraction_method labels it
        assert r.extraction_method == "attachment_regex"

    def test_extracts_due_date_from_attachment(self, tmp_path, mock_pdf_text):
        pdf_path = tmp_path / "Bid_Instructions.pdf"
        _write_simple_pdf(pdf_path, mock_pdf_text)
        atts = [{"filename": "Bid_Instructions.pdf",
                 "file_path": str(pdf_path)}]
        r = parse_attachments_for_requirements(atts)
        assert r.due_date == "2026-05-15"


class TestMergeWithEmailContract:
    def test_email_wins_on_due_date_conflict(self):
        email = RFQRequirements(due_date="2026-05-01",
                                forms_required=["std204"])
        attach = RFQRequirements(due_date="2026-06-01",
                                 forms_required=["dvbe843"],
                                 extraction_method="attachment_regex")
        merge_with_email_contract(email, attach)
        # Email's due_date preserved
        assert email.due_date == "2026-05-01"
        # Forms unioned
        assert "std204" in email.forms_required
        assert "dvbe843" in email.forms_required

    def test_attachment_fills_empty_email_fields(self):
        email = RFQRequirements()  # email had nothing
        attach = RFQRequirements(due_date="2026-06-01",
                                 solicitation_number="ABC-123",
                                 forms_required=["dvbe843"])
        merge_with_email_contract(email, attach)
        assert email.due_date == "2026-06-01"
        assert email.solicitation_number == "ABC-123"
        assert "dvbe843" in email.forms_required

    def test_extraction_method_label_appends(self):
        email = RFQRequirements(extraction_method="claude",
                                forms_required=["std204"])
        attach = RFQRequirements(forms_required=["dvbe843"])
        merge_with_email_contract(email, attach)
        assert email.extraction_method == "claude+attachment"


class TestFillPlanBuilderIntegration:
    """The fill-plan now picks up attachment-derived requirements."""

    def test_attachment_required_forms_added_to_plan(self, tmp_path, mock_pdf_text):
        from src.agents.fill_plan_builder import build_fill_plan

        pdf_path = tmp_path / "Bid_Instructions.pdf"
        _write_simple_pdf(pdf_path, mock_pdf_text)

        import json
        quote = {
            "id": "PC-1",
            "agency": "CDCR Folsom",
            "institution": "CDCR Folsom",
            "requirements_json": json.dumps({}),  # email had no requirements
            "source_file": str(pdf_path),  # PC source_file path is the attached PDF
        }

        attached = [{"filename": "Bid_Instructions.pdf",
                     "file_path": str(pdf_path)}]
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=attached):
            plan = build_fill_plan("PC-1", "pc", quote_data=quote)

        ids = [it.form_id for it in plan.items]
        assert "std204" in ids
        assert "dvbe843" in ids
        # Source labelled correctly
        assert plan.contract_source in ("attachment", "email+attachment")
        # Attribution: at least one form should carry attachment_contract
        attach_attributed = [it for it in plan.items
                             if "attachment_contract" in it.required_by]
        assert attach_attributed, "no items attributed to attachment_contract"


# ─── Test helpers ─────────────────────────────────────────────────────────

def _write_simple_pdf(path, text: str) -> None:
    """Write a minimal PDF whose extract_text() returns roughly `text`.

    Uses reportlab if available; otherwise falls back to a stub PDF
    that pdfplumber can open but won't extract meaningful text from
    (in which case the test will skip).
    """
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        c = canvas.Canvas(str(path), pagesize=letter)
        textobj = c.beginText(50, 750)
        textobj.setFont("Helvetica", 9)
        for line in text.splitlines():
            textobj.textLine(line)
        c.drawText(textobj)
        c.save()
    except ImportError:
        # No reportlab — skip these tests
        pytest.skip("reportlab not available for PDF generation")
