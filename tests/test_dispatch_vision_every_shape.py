"""Vision verification runs on EVERY parser shape — Mike directive 2026-05-11.

PR #908 added Vision verification only to the SHAPE_GENERIC_RFQ_PDF
branch of `ingest_pipeline._dispatch_parser`. The 3-shape cross-test
on rfq_8efe9fae (CalVet) + pc_5728f934 (CCHCS) + pc_93edc64e (CSP-SAC)
revealed that the AMS 704 path ALSO drops items that Vision sees —
same failure class as the generic-RFQ-PDF path, just on a different
ingest shape.

Mike: "ship vision, should be a part of every parse".

This module pins that the refactored `_dispatch_parser` wraps EVERY
shape with Vision verification + uses Vision's items when it finds
more. The same `_vision_verify_items_if_pdf` helper from PR #908 is
called; the per-shape branch only fills `base_items` / `base_header`.

Shapes covered:
  - SHAPE_GENERIC_RFQ_PDF   (was already Vision-verified)
  - SHAPE_PC_704_PDF_*      (NEW — close the second gap)
  - SHAPE_CCHCS_IT_RFQ      (NEW)
  - SHAPE_CCHCS_PACKET      (NEW)

Each test mocks the base parser AND the Vision helper to assert the
post-PR behavior: when Vision returns more items, Vision wins; the
base parser's header is preserved either way.
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


# ─── SHAPE_GENERIC_RFQ_PDF — already Vision-verified pre-refactor ─────────


def test_generic_rfq_pdf_vision_replaces_base_when_higher(tmp_path):
    f = tmp_path / "calvet.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}, {"description": "b"}]
    visionx = [{"description": f"v{i}"} for i in range(15)]
    with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
               return_value={"items": base, "header": {"institution": "CalVet"}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=visionx):
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_GENERIC_RFQ_PDF))
    assert err is None
    assert len(items) == 15
    # Header from base parser is preserved
    assert header == {"institution": "CalVet"}


# ─── SHAPE_PC_704_PDF — NEW Vision coverage (closes the CCHCS gap) ────────


def test_ams_704_pdf_vision_replaces_base_when_higher(tmp_path):
    """CCHCS PC pc_5728f934: ams_704 parser returned 8 items, Vision
    found 10. The new pipeline wraps the AMS 704 path with Vision
    verification so the higher count wins."""
    f = tmp_path / "704.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": f"i{i}"} for i in range(8)]
    visionx = [{"description": f"v{i}"} for i in range(10)]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {"requestor": "Chechi"}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=visionx):
        items, header, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    assert err is None
    assert len(items) == 10
    assert header == {"requestor": "Chechi"}


def test_ams_704_pdf_keeps_base_when_vision_lower(tmp_path):
    """When the 704 parser already captured everything, Vision MUST
    NOT regress the count. Gate logic: vision > base → swap; else keep."""
    f = tmp_path / "704.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": f"i{i}"} for i in range(8)]
    visionx = [{"description": "v0"}]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {"requestor": "X"}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    assert err is None
    assert len(items) == 8


def test_ams_704_docusign_path_gets_vision_too(tmp_path):
    """DocuSign 704 PDFs are a separate shape but the same parser/Vision
    pipeline applies — pin it explicitly."""
    f = tmp_path / "docusign_704.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "i1"}]
    visionx = [{"description": f"v{i}"} for i in range(6)]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_DOCUSIGN))
    assert err is None
    assert len(items) == 6


def test_ams_704_docx_skips_vision_no_op(tmp_path):
    """DOCX path: `_vision_verify_items_if_pdf` early-returns None on
    non-PDF, so the base 704 parser's items pass through unchanged."""
    f = tmp_path / "704.docx"
    f.write_bytes(b"PK stub")
    base = [{"description": "i1"}, {"description": "i2"}]
    with patch("src.forms.price_check.parse_ams704",
               return_value={"line_items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=None):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_DOCX))
    assert err is None
    assert len(items) == 2


# ─── SHAPE_CCHCS_IT_RFQ — NEW Vision coverage ─────────────────────────────


def test_cchcs_it_rfq_vision_replaces_when_higher(tmp_path):
    """CCHCS LPA IT-Goods RFQs route through the generic parser today;
    Vision verification applies on top."""
    f = tmp_path / "lpa.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}]
    visionx = [{"description": f"v{i}"} for i in range(5)]
    with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
               return_value={"items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=visionx):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_CCHCS_IT_RFQ))
    assert err is None
    assert len(items) == 5


# ─── SHAPE_CCHCS_PACKET — NEW Vision coverage ─────────────────────────────


def test_cchcs_packet_vision_replaces_when_higher(tmp_path):
    f = tmp_path / "cchcs.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base = [{"description": "a"}, {"description": "b"}]
    visionx = [{"description": f"v{i}"} for i in range(20)]
    with patch("src.forms.cchcs_packet_parser.parse_cchcs_packet",
               return_value={"ok": True, "line_items": base, "header": {}}), \
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
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
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
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
         patch("src.core.ingest_pipeline._vision_verify_items_if_pdf",
               return_value=None):
        items, _h, err = _dispatch_parser(
            str(f), _classification(SHAPE_PC_704_PDF_FILLABLE))
    assert err is not None
    assert "504 timeout" in err
    assert items == []


# ─── Unknown shape — no parser at all ────────────────────────────────────


def test_unknown_shape_returns_error(tmp_path):
    f = tmp_path / "x.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    items, _h, err = _dispatch_parser(str(f), _classification("UNKNOWN_SHAPE"))
    assert err is not None
    assert "no parser for shape" in err
    assert items == []
