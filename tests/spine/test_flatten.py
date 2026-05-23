"""PDF flatten — bake form widgets into static page content.

Job #1 PR-5: the flatten primitive used at the send / preview-of-send
boundary so the bytes leaving Reytech can't be edited at the buyer's
end. No password — recipient opens the PDF normally; the form fields
are just gone, their values baked into the page.
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest
from pypdf import PdfReader

from src.spine.flatten import flatten_pdf_bytes, flatten_pdf_file

_REPO_ROOT = Path(__file__).resolve().parents[2]
_T704B = _REPO_ROOT / "tests/fixtures/704b_blank.pdf"
_T703B = _REPO_ROOT / "tests/fixtures/703b_blank.pdf"


_needs_fixtures = pytest.mark.skipif(
    not (_T704B.is_file() and _T703B.is_file()),
    reason="form template fixtures missing",
)


def _field_count(data: bytes) -> int:
    return len(PdfReader(io.BytesIO(data)).get_fields() or {})


# ── pure flatten — bytes in, bytes out ───────────────────────────────


@_needs_fixtures
def test_flatten_drops_every_form_field():
    """The 704B blank ships 362 form fields. After flatten: zero."""
    data = _T704B.read_bytes()
    assert _field_count(data) > 0
    flat = flatten_pdf_bytes(data)
    assert _field_count(flat) == 0
    # Still a valid PDF.
    assert flat[:5] == b"%PDF-"


@_needs_fixtures
def test_flatten_preserves_page_count():
    """Flattening must not lose pages."""
    data = _T703B.read_bytes()
    pages_before = len(PdfReader(io.BytesIO(data)).pages)
    flat = flatten_pdf_bytes(data)
    pages_after = len(PdfReader(io.BytesIO(flat)).pages)
    assert pages_after == pages_before


def test_flatten_passes_through_non_pdf_unchanged():
    """Best-effort: empty / non-PDF bytes return as-is, no crash."""
    assert flatten_pdf_bytes(b"") == b""
    junk = b"not a pdf at all"
    assert flatten_pdf_bytes(junk) == junk


def test_flatten_passes_through_corrupt_pdf_unchanged():
    """A bytes blob with the %PDF- header but corrupt body returns the
    input unchanged (best-effort — never raises, never partial output)."""
    corrupt = b"%PDF-1.4\nthis is not real PDF content"
    out = flatten_pdf_bytes(corrupt)
    assert out == corrupt  # unchanged on failure


# ── file API ──────────────────────────────────────────────────────────


@_needs_fixtures
def test_flatten_pdf_file_writes_flat_output(tmp_path):
    out = tmp_path / "flat.pdf"
    flatten_pdf_file(str(_T704B), str(out))
    assert out.is_file()
    assert _field_count(out.read_bytes()) == 0


# ── round-trip from a real rendered form (PR-3 adapter output) ────────


@_needs_fixtures
def test_flatten_rendered_704b_keeps_values_visible(tmp_path):
    """After flatten the values must still be VISIBLE (as page content)
    even though they're no longer in form fields. Verifies fitz.bake
    converted /AP appearances into page content streams correctly.
    """
    # Render a real 704B via the forms_render adapter, then flatten.
    from datetime import datetime, timezone

    from src.spine.email_contract import ContractLineItem, EmailContract
    from src.spine.forms_render import render_cchcs_forms_via_legacy
    from src.spine.model import LineItem, Quote

    quote = Quote(
        quote_id="Q-flatten", agency="CCHCS", facility="SAC",
        solicitation_number="10848901",
        line_items=[LineItem(
            line_no=1, description="Test Item", mfg_number="X-1",
            qty=5, uom="EA",
            cost_cents=8000,
            cost_source_url="https://example.com/x",
            cost_validated_at=datetime.now(timezone.utc),
            unit_price_cents=12500,
        )],
        tax_rate_bps=775,
    )
    contract = EmailContract(
        contract_id="contract_Q-flatten_1", rfq_id="Q-flatten",
        agency="CCHCS", facility="SAC",
        solicitation_number="10848901",
        buyer_name="Grace Pfost", buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        line_items=[
            ContractLineItem(line_no=1, description="Test Item", qty=5, uom="EA"),
        ],
        attachment_refs=[
            "tests/fixtures/703b_blank.pdf",
            "tests/fixtures/704b_blank.pdf",
            "tests/fixtures/cchcs_bidpkg_blank.pdf",
        ],
    )
    res = render_cchcs_forms_via_legacy(
        quote, contract, output_dir=str(tmp_path), strict=False)
    assert res["ok"], res["error"]
    pdf_bytes = (res["forms"].get("704b") or {}).get("pdf_bytes") or b""
    assert pdf_bytes[:5] == b"%PDF-"
    assert _field_count(pdf_bytes) > 0

    flat = flatten_pdf_bytes(pdf_bytes)
    assert _field_count(flat) == 0
    # The sol# was filled in the 704B header — after flatten it should
    # still be visible in the page's text content (baked from /AP).
    import fitz

    doc = fitz.open(stream=flat, filetype="pdf")
    try:
        text_all = "".join(pg.get_text() for pg in doc)
    finally:
        doc.close()
    assert "10848901" in text_all
