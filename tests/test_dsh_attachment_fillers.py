"""Tests for the DSH Attachment A/B/C overlay fillers.

The DSH packet attachments are flat (zero AcroForm fields) so the fillers
draw with reportlab and merge onto the buyer's source PDF. These tests pin:

  - Each filler returns a valid 1-page PDF (BytesIO that pypdf can re-open)
  - The overlaid values land on the page (extractable via pdfplumber)
  - The Attachment B totals math is right (subtotal = sum(qty * unit))
  - Source-PDF integrity is preserved (DSH-printed text still present)
"""
from __future__ import annotations

import io
import os

import pdfplumber
import pytest
from pypdf import PdfReader

from src.forms.dsh_attachment_fillers import (
    fill_dsh_attachment_a,
    fill_dsh_attachment_b,
    fill_dsh_attachment_c,
)

_FIX_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fixtures", "dsh",
)
_ATT_A = os.path.join(_FIX_DIR, "dsh_25CB020_attachA_bidder.pdf")
_ATT_B = os.path.join(_FIX_DIR, "dsh_25CB020_attachB_pricing.pdf")
_ATT_C = os.path.join(_FIX_DIR, "dsh_25CB020_attachC_forms.pdf")


@pytest.fixture
def reytech_info():
    """Canonical Reytech identity — mirrors src/forms/reytech_config.json
    but kept inline so the test doesn't depend on file I/O."""
    return {
        "company": {
            "name": "Reytech Inc.",
            "street": "30 Carnoustie Way",
            "city": "Trabuco Canyon",
            "state": "CA",
            "zip": "92679",
            "owner": "Michael Guadan",
            "title": "Owner",
            "phone": "949-229-1575",
            "email": "sales@reytechinc.com",
            "fein": "47-4588061",
            "sellers_permit": "245652416 - 00001",
            "cert_number": "2002605",
        }
    }


@pytest.fixture
def parsed_25cb020():
    """A representative parsed DSH 25CB020 RFQ with priced line items."""
    return {
        "header": {"solicitation_number": "25CB020"},
        "sol_expires": "03/30/2026",
        "lead_time": "5-7 business days",
        "warranty": "Per manufacturer",
        "dvbe_pct": "100%",
        "items": [
            {"qty": 50,  "unit_price": 25.50},  # XX-LARGE gloves
            {"qty": 300, "unit_price": 24.10},  # X-LARGE
            {"qty": 300, "unit_price": 24.10},  # LARGE
            {"qty": 300, "unit_price": 24.10},  # MEDIUM
            {"qty": 50,  "unit_price": 18.75},  # CARE SOFT large
            {"qty": 50,  "unit_price": 18.75},  # CARE SOFT medium
            {"qty": 50,  "unit_price": 18.75},  # CARE SOFT small
        ],
    }


def _open(buf: io.BytesIO) -> PdfReader:
    buf.seek(0)
    return PdfReader(buf)


def _all_text(buf: io.BytesIO) -> str:
    buf.seek(0)
    chunks = []
    with pdfplumber.open(buf) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return "\n".join(chunks)


@pytest.mark.skipif(
    not os.path.exists(_ATT_A),
    reason="DSH 25CB020 packet fixtures not present",
)
class TestAttachmentA:

    def test_returns_valid_pdf(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_a(reytech_info, parsed_25cb020, src_pdf=_ATT_A)
        assert out is not None, "filler returned None"
        reader = _open(out)
        assert len(reader.pages) >= 1

    def test_overlays_vendor_identity(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_a(reytech_info, parsed_25cb020, src_pdf=_ATT_A)
        text = _all_text(out)
        assert "Reytech Inc." in text
        assert "Michael Guadan" in text
        assert "sales@reytechinc.com" in text
        assert "30 Carnoustie Way" in text
        assert "Trabuco Canyon" in text

    def test_preserves_source_pdf_text(self, reytech_info, parsed_25cb020):
        """Overlay must not erase DSH's pre-printed labels."""
        out = fill_dsh_attachment_a(reytech_info, parsed_25cb020, src_pdf=_ATT_A)
        text = _all_text(out)
        assert "BIDDER'S INFORMATION" in text
        assert "DEPARTMENT OF STATE HOSPITALS" in text
        assert "ATTACHMENT A" in text


@pytest.mark.skipif(
    not os.path.exists(_ATT_B),
    reason="DSH 25CB020 packet fixtures not present",
)
class TestAttachmentB:

    def test_returns_valid_pdf(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_b(reytech_info, parsed_25cb020, src_pdf=_ATT_B)
        assert out is not None
        reader = _open(out)
        assert len(reader.pages) >= 1

    def test_overlays_vendor_name_and_total(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_b(reytech_info, parsed_25cb020, src_pdf=_ATT_B)
        text = _all_text(out)
        assert "Reytech Inc." in text
        # Subtotal = 50*25.50 + 300*24.10*3 + 50*18.75*3 = 1275 + 21690 + 2812.50 = 25777.50
        assert "$25,777.50" in text or "25,777.50" in text

    def test_per_row_unit_prices_appear(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_b(reytech_info, parsed_25cb020, src_pdf=_ATT_B)
        text = _all_text(out)
        import re
        # pdfplumber may interleave overlay glyphs with adjacent source text
        # (e.g. extension "$1,275.00" ends up next to source "$ -" → space-
        # separated digits). Match digits with optional whitespace between.
        assert "25.50" in text
        assert "24.10" in text
        assert "18.75" in text
        # Per-line extension totals (qty * unit) — allow inter-glyph whitespace.
        assert re.search(r"1\s*,\s*2\s*7\s*5", text)   # 50 * 25.50
        assert re.search(r"7\s*,\s*2\s*3\s*0", text)   # 300 * 24.10
        assert re.search(r"9\s*3\s*7\s*\.\s*5", text)  # 50 * 18.75

    def test_handles_no_items(self, reytech_info):
        """An empty parsed RFQ should not crash — vendor name still drawn,
        totals show $0.00 so the operator can see the form is wired."""
        out = fill_dsh_attachment_b(reytech_info, {"items": []}, src_pdf=_ATT_B)
        assert out is not None
        text = _all_text(out)
        assert "Reytech Inc." in text
        assert "$0.00" in text


@pytest.mark.skipif(
    not os.path.exists(_ATT_C),
    reason="DSH 25CB020 packet fixtures not present",
)
class TestAttachmentC:

    def test_returns_valid_pdf(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_c(reytech_info, parsed_25cb020, src_pdf=_ATT_C)
        assert out is not None

    def test_overlays_vendor_name(self, reytech_info, parsed_25cb020):
        out = fill_dsh_attachment_c(reytech_info, parsed_25cb020, src_pdf=_ATT_C)
        text = _all_text(out)
        assert "Reytech Inc." in text
        # DSH labels still present
        assert "ATTACHMENT C" in text
        assert "REQUIRED FORMS" in text
