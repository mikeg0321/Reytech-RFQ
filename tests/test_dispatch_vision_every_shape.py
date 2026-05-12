"""Vision verification runs on EVERY parser shape — Mike directive 2026-05-11.

PR #908 added Vision verification only to the SHAPE_GENERIC_RFQ_PDF
branch of `ingest_pipeline._dispatch_parser`. The 3-shape cross-test
on rfq_8efe9fae (CalVet) + pc_5728f934 (CCHCS) + pc_93edc64e (CSP-SAC)
revealed that the AMS 704 path ALSO drops items that Vision sees —
same failure class as the generic-RFQ-PDF path, just on a different
ingest shape.

PR-A (2026-05-11): Mike pushed Vision-primary after the rfq_0ebe242f
phantom-item incident: "Vision is verification right? ... relying on
regex could just be creating bad data... garbage in, garbage out... I
don't want any of that."

Architecture: for PDF shapes, Vision is the TRUTH source for items;
the base parser is now a SANITY-CHECK signal consulted only to flag
disagreements for operator review. The same `_vision_primary_extract`
helper is called from every PDF branch; per-shape branches only fill
`base_items` / `base_header` and the count for the disagreement gate.

Shapes covered:
  - SHAPE_GENERIC_RFQ_PDF
  - SHAPE_PC_704_PDF_*
  - SHAPE_CCHCS_IT_RFQ
  - SHAPE_CCHCS_PACKET

Each test mocks the base parser AND `_vision_primary_extract` to assert
the post-PR behavior: Vision items always win on PDFs; the base
parser's header is preserved; large count disagreements set the
`_needs_review` header flag.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from src.core.ingest_pipeline import _dispatch_parser
from src.core.request_classifier import (
    SHAPE_CCHCS_PACKET,
    SHAPE_CCHCS_IT_RFQ,
    SHAPE_PC_704_PDF_FILLABLE,
    SHAPE_PC_704_PDF_DOCUSIGN,
    SHAPE_PC_704_DOCX,
    SHAPE_GENERIC_RFQ_PDF,
    SHAPE_GENERIC_RFQ_DOCX,
    SHAPE_GENERIC_RFQ_XLSX,
)


def _classification(shape: str):
    """Stand-in classification with the minimal surface _dispatch_parser
    actually reads."""
    c = MagicMock()
    c.shape = shape
    return c


# ─── SHAPE_GENERIC_RFQ_PDF — Vision-primary on PDFs ───────────────────────


def test_generic_rfq_pdf_vision_is_primary(tmp_path):
    f = tmp_path / "calvet.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}, {"description": "b"}]
    visionx = [{"description": f"v{i}"} for i in range(15)]
    with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
               return_value={"items": base, "header": {"institution": "CalVet"}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_GENERIC_RFQ_PDF))
    assert err is None
    # Vision is primary: its 15 items beat the base parser's 2.
    assert len(items) == 15
    # Base parser's header value wins on agency-keyed fields.
    assert header.get("institution") == "CalVet"
    # Large disagreement triggers the review flag.
    assert header.get("_needs_review") is True


# ─── SHAPE_PC_704_PDF — Vision-primary on AMS 704 path ────────────────────


def test_ams_704_pdf_vision_primary(tmp_path):
    """CCHCS PC pc_5728f934: ams_704 parser returned 8 items, Vision
    found 10. With Vision-primary, 10 items ship regardless of how the
    base count compares."""
    f = tmp_path / "704.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": f"i{i}"} for i in range(8)]
    visionx = [{"description": f"v{i}"} for i in range(10)]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {"requestor": "Chechi"}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    assert err is None
    assert len(items) == 10
    assert header.get("requestor") == "Chechi"


def test_ams_704_vision_wins_even_when_base_higher(tmp_path):
    """Architectural pin: Mike's directive — Vision is the truth source.
    When the base parser reports MORE items than Vision (could be
    phantom matches from regex on parenthetical text), Vision still
    wins. The disagreement gate flags it for operator review."""
    f = tmp_path / "704.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": f"i{i}"} for i in range(8)]  # potentially phantoms
    visionx = [{"description": "v0"}]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {"requestor": "X"}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    assert err is None
    # Vision is primary — its 1 item wins over base's 8.
    assert len(items) == 1
    # Disagreement > threshold flags for review.
    assert header.get("_needs_review") is True


def test_ams_704_docusign_path_vision_primary(tmp_path):
    """DocuSign 704 PDFs are a separate shape but the same parser/Vision
    pipeline applies — pin it explicitly."""
    f = tmp_path / "docusign_704.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "i1"}]
    visionx = [{"description": f"v{i}"} for i in range(6)]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_DOCUSIGN))
    assert err is None
    assert len(items) == 6


def test_ams_704_docx_skips_vision_no_op(tmp_path):
    """DOCX path: Vision can't read non-PDF formats, so base parser items
    pass through with a `vision_unsupported_format` warning."""
    f = tmp_path / "704.docx"
    f.write_bytes(b"PK stub")
    base = [{"description": "i1"}, {"description": "i2"}]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract") as mock_vis:
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_DOCX))
    # Vision was NOT invoked on the DOCX path.
    mock_vis.assert_not_called()
    assert err is None
    assert len(items) == 2
    # Warning emitted so the operator knows Vision couldn't verify.
    warnings = header.get("_ingest_warnings") or []
    assert any(w.get("kind") == "vision_unsupported_format" for w in warnings)


# ─── SHAPE_CCHCS_IT_RFQ — Vision-primary ───────────────────────────────────


def test_cchcs_it_rfq_vision_primary(tmp_path):
    """CCHCS LPA IT-Goods RFQs route through the generic parser; Vision
    primary applies on top."""
    f = tmp_path / "lpa.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}]
    visionx = [{"description": f"v{i}"} for i in range(5)]
    with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
               return_value={"items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_CCHCS_IT_RFQ))
    assert err is None
    assert len(items) == 5


# ─── SHAPE_CCHCS_PACKET — Vision-primary ──────────────────────────────────


def test_cchcs_packet_vision_primary(tmp_path):
    f = tmp_path / "cchcs.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}, {"description": "b"}]
    visionx = [{"description": f"v{i}"} for i in range(20)]
    with patch("src.forms.cchcs_packet_parser.parse_cchcs_packet",
               return_value={"ok": True, "line_items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_CCHCS_PACKET))
    assert err is None
    assert len(items) == 20


# ─── Error-recovery: base parser fails, Vision recovers ──────────────────


def test_base_parser_error_with_vision_recovery(tmp_path):
    """If the base parser errors but Vision returns items, we surface
    Vision's items (better than propagating a parse failure with 0
    items). Bid-window is the cost we're optimizing for."""
    f = tmp_path / "bad.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    visionx = [{"description": f"v{i}"} for i in range(3)]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"error": "504 timeout"}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    # Vision-recovered the items; error path no longer fires when we have items.
    assert err is None
    assert len(items) == 3


def test_base_parser_error_without_vision_propagates(tmp_path):
    """When the base parser errors AND Vision can't recover, the error
    is propagated so the operator sees a useful message instead of a
    silent 0-items record."""
    f = tmp_path / "bad.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    with patch("src.forms.price_check.parse_ams704",
               return_value={"error": "504 timeout"}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=None):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    assert err is not None
    assert "504 timeout" in err
    assert items == []


# ─── Vision unavailable: base parser is the fallback ─────────────────────


def test_vision_unavailable_falls_back_to_base(tmp_path):
    """When Vision returns None (no API key, quota, crash), the base
    parser items still ship — with a `vision_skipped` warning so the
    operator knows verification didn't run."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}, {"description": "b"}]
    with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
               return_value={"items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_primary_extract",
               return_value=None):
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_GENERIC_RFQ_PDF))
    assert err is None
    assert len(items) == 2
    warnings = header.get("_ingest_warnings") or []
    assert any(w.get("kind") == "vision_skipped" for w in warnings)


# ─── Unknown shape — no parser at all ────────────────────────────────────


def test_unknown_shape_returns_error(tmp_path):
    f = tmp_path / "x.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    items, _h, err = _dispatch_parser(str(f), _classification("UNKNOWN_SHAPE"))
    assert err is not None
    assert "no parser for shape" in err
    assert items == []
