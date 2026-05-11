"""OCR vision fallback for image-only RFQ PDFs.

Mike P0 2026-05-11 (rfq_8efe9fae): an image-only RFQ PDF was extracted as
0 items by the text-based parser, causing downstream fallback to email-body
regex which then misfired (Fresno zip code 93706 stored as item qty).

Fix: when PASS-2 text extraction yields zero items AND the PDF appears
image-only (no_text status or text_length < 200), fall through to Claude
Vision OCR. Items get parse_method="vision_ocr" so downstream UI can
surface a badge indicating OCR provenance.

These tests pin the gating logic for the vision fallback — verifying we
trigger narrowly (truly image-only / sparse-text PDFs) and not on
parser-miss cases (long text but regex didn't match anything).
"""
from __future__ import annotations

from unittest.mock import patch

from src.forms.generic_rfq_parser import (
    _vision_fallback_candidates,
    _vision_fallback_extract,
)


# ─── _vision_fallback_candidates gating ──────────────────────────────────


def test_no_text_status_becomes_vision_candidate():
    """A PDF that hit `no_text` (text extraction returned empty — classic
    image-only signal) is a vision candidate."""
    parse_details = [
        {"file": "scan.pdf", "status": "no_text"},
    ]
    pdf_paths = ["/data/uploads/abc/scan.pdf"]
    cands = _vision_fallback_candidates(parse_details, pdf_paths)
    assert cands == ["/data/uploads/abc/scan.pdf"]


def test_no_items_with_short_text_becomes_vision_candidate():
    """`no_items` with text_length < 200 means OCR extracted token noise
    but no real content — vision is the right next step."""
    parse_details = [
        {"file": "short.pdf", "status": "no_items", "text_length": 150},
    ]
    pdf_paths = ["/data/uploads/abc/short.pdf"]
    cands = _vision_fallback_candidates(parse_details, pdf_paths)
    assert cands == ["/data/uploads/abc/short.pdf"]


def test_no_items_with_long_text_is_NOT_vision_candidate():
    """`no_items` with plenty of text means the PDF was readable but our
    regex patterns missed — that's a parser-miss not an OCR case. Don't
    burn a vision call on it."""
    parse_details = [
        {"file": "long.pdf", "status": "no_items", "text_length": 50000},
    ]
    pdf_paths = ["/data/uploads/abc/long.pdf"]
    cands = _vision_fallback_candidates(parse_details, pdf_paths)
    assert cands == [], "long-text no_items must not trigger vision"


def test_parsed_status_is_NOT_vision_candidate():
    """A PDF that already yielded items doesn't need vision."""
    parse_details = [
        {"file": "good.pdf", "status": "parsed", "items_found": 5},
    ]
    pdf_paths = ["/data/uploads/abc/good.pdf"]
    assert _vision_fallback_candidates(parse_details, pdf_paths) == []


def test_skipped_boilerplate_is_NOT_vision_candidate():
    """Boilerplate PDFs (Bidder Declaration, etc.) are skipped by filename
    earlier in PASS 2; they must NEVER consume a vision call."""
    parse_details = [
        {"file": "Bidder_Declaration.pdf", "status": "skipped_boilerplate"},
    ]
    pdf_paths = ["/data/uploads/abc/Bidder_Declaration.pdf"]
    assert _vision_fallback_candidates(parse_details, pdf_paths) == []


def test_skipped_boilerplate_content_is_NOT_vision_candidate():
    """Same exclusion for content-detected boilerplate."""
    parse_details = [
        {"file": "PD_843.pdf", "status": "skipped_boilerplate_content"},
    ]
    pdf_paths = ["/data/uploads/abc/PD_843.pdf"]
    assert _vision_fallback_candidates(parse_details, pdf_paths) == []


def test_multi_pdf_mix_only_image_pdfs_become_candidates():
    """Real-world: 4-PDF RFQ packet — 1 scanned items PDF + 3 boilerplate
    forms. Only the scanned items PDF should land in the candidate list."""
    parse_details = [
        {"file": "items.pdf", "status": "no_text"},
        {"file": "DVBE_843.pdf", "status": "skipped_boilerplate"},
        {"file": "Darfur_Cert.pdf", "status": "skipped_boilerplate"},
        {"file": "Terms.pdf", "status": "skipped_boilerplate_content"},
    ]
    pdf_paths = [
        "/data/uploads/x/items.pdf",
        "/data/uploads/x/DVBE_843.pdf",
        "/data/uploads/x/Darfur_Cert.pdf",
        "/data/uploads/x/Terms.pdf",
    ]
    cands = _vision_fallback_candidates(parse_details, pdf_paths)
    assert cands == ["/data/uploads/x/items.pdf"]


# ─── _vision_fallback_extract behavior ───────────────────────────────────


def test_vision_extract_returns_empty_when_unavailable():
    """When vision API key/deps aren't available, return [] (caller sees
    parse-failed-but-no-vision-tried instead of crashing)."""
    with patch("src.forms.vision_parser.is_available", return_value=False):
        result = _vision_fallback_extract("/data/uploads/x/scan.pdf")
    assert result == []


def test_vision_extract_returns_empty_when_quota_exhausted():
    """Daily $ cap gates the vision call. When exhausted, return [] so we
    don't blow the budget; downstream sees parse-failed badge."""
    with patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.core.anthropic_quota.check_quota", return_value=True):
        result = _vision_fallback_extract("/data/uploads/x/scan.pdf")
    assert result == []


def test_vision_extract_canonicalizes_items_with_parse_method_tag():
    """When vision succeeds, items must be tagged `parse_method='vision_ocr'`
    so downstream UI can surface a 'OCR' badge and operators know to verify."""
    fake_vision_result = {
        "line_items": [
            {"line_number": 1, "qty": 5, "uom": "ea",
             "description": "Penlight White Light Disposable",
             "item_number": "161574", "unit_price": 10.59},
            {"line_number": 2, "qty": 12, "uom": "CS",
             "description": "Sterile Gauze 4x4",
             "item_number": "MCK-099", "unit_price": 25.0},
        ],
    }
    with patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.core.anthropic_quota.check_quota", return_value=False), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value=fake_vision_result):
        items = _vision_fallback_extract("/data/uploads/x/scan.pdf")
    assert len(items) == 2
    assert all(it["parse_method"] == "vision_ocr" for it in items)
    assert items[0]["description"] == "Penlight White Light Disposable"
    assert items[0]["item_number"] == "161574"
    assert items[0]["qty"] == 5
    # UOM uppercased to match the codebase's canonical tokens
    assert items[0]["uom"] == "EA"
    assert items[1]["uom"] == "CS"


def test_vision_extract_handles_missing_fields():
    """Vision sometimes returns items with missing fields. Canonicalization
    must provide safe defaults rather than NPE the caller."""
    fake_vision_result = {
        "line_items": [
            # Missing qty, uom, unit_price — vision had partial extraction
            {"description": "Some Item", "item_number": "PN-X"},
        ],
    }
    with patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.core.anthropic_quota.check_quota", return_value=False), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value=fake_vision_result):
        items = _vision_fallback_extract("/data/uploads/x/scan.pdf")
    assert len(items) == 1
    item = items[0]
    # Safe defaults
    assert item["qty"] == 1
    assert item["uom"] == "EA"
    assert item["unit_price"] == 0
    assert item["description"] == "Some Item"
    assert item["item_number"] == "PN-X"


def test_vision_extract_falls_back_to_part_number():
    """Vision schema uses `part_number` while our text parser uses
    `item_number`. Canonicalization must accept either."""
    fake_vision_result = {
        "line_items": [
            {"description": "X", "part_number": "PN-FOO", "qty": 1, "uom": "EA"},
        ],
    }
    with patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.core.anthropic_quota.check_quota", return_value=False), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value=fake_vision_result):
        items = _vision_fallback_extract("/data/uploads/x/scan.pdf")
    assert items[0]["item_number"] == "PN-FOO"


def test_vision_extract_returns_empty_on_api_failure():
    """When the vision API itself returns None / errors, fail clean."""
    with patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.core.anthropic_quota.check_quota", return_value=False), \
         patch("src.forms.vision_parser.parse_with_vision", return_value=None):
        items = _vision_fallback_extract("/data/uploads/x/scan.pdf")
    assert items == []
