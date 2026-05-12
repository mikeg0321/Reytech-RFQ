"""Mike P000 2026-05-11 — ingest_pipeline Vision verification on generic-RFQ-PDF.

Mike missed the CalVet Keith Alsing 2026-05-11 bid window because the
buyer's RFQ landed with 5 items when the original had 15-16. Root cause:
`ingest_pipeline._parse_files` for SHAPE_GENERIC_RFQ_PDF called
`parse_generic_rfq()` (regex-based) and blindly trusted the output.
Vision AI was never invoked. The regex parser dropped page 2+ on
the multi-page CalVet PDF; the pipeline shipped 5 items downstream.

Fix: a Vision verification pass runs alongside the regex parser on
suspect cases (multi-page PDF, 0 items, or <3 items/page). If Vision
returns more items, those replace the regex output. The regex header
is preserved (it has agency-specific learning).

These tests pin:
  - The suspicion-gate triggers on multi-page PDFs even when regex
    found some items
  - Vision-found count > regex-found count -> Vision wins
  - Vision-found count <= regex-found count -> regex stays (no upgrade)
  - Single-page PDFs with >=3 items/page skip the Vision call (cost)
  - Errors in the Vision pass don't crash ingestion
"""
from __future__ import annotations

import os
from unittest.mock import patch, MagicMock

import pytest

from src.core.ingest_pipeline import _vision_verify_items_if_pdf


# ─── Gate behavior: when does Vision verification run? ────────────────────


def test_vision_skipped_on_non_pdf():
    """A .xlsx path should not invoke Vision (xlsx has its own path)."""
    with patch("src.core.ingest_pipeline._pdf_page_count") as pc:
        out = _vision_verify_items_if_pdf("/tmp/file.xlsx", [{"a": 1}], {})
    assert out is None
    pc.assert_not_called()


def test_vision_runs_when_pdf_is_multipage_even_if_items_found(tmp_path):
    """The CalVet failure class: regex finds 5 items on a multi-page PDF.
    Vision MUST run to catch the dropped page-2 items. The gate must
    fire even when regex returned a non-zero count."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base_items = [{"description": f"item {i}"} for i in range(5)]

    mock_vision_result = {"line_items": [{"description": f"item {i}"} for i in range(15)]}
    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=3), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value=mock_vision_result) as mock_vis:
        out = _vision_verify_items_if_pdf(str(f), base_items, {})

    mock_vis.assert_called_once()
    assert out is not None
    assert len(out) == 15


def test_vision_runs_when_regex_found_zero_items(tmp_path):
    """Zero-items is a full-miss signal — Vision must always try."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")

    mock_vision_result = {"line_items": [{"description": "found one"}]}
    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=1), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value=mock_vision_result) as mock_vis:
        out = _vision_verify_items_if_pdf(str(f), [], {})

    mock_vis.assert_called_once()
    assert out is not None
    assert len(out) == 1


def test_vision_skipped_when_single_page_with_dense_items(tmp_path):
    """Cost gate: a 1-page PDF with 4+ items already extracted
    (4 items / 1 page = 4 items/page > 3 threshold) skips Vision.
    Sparse-doc single-page PDFs ARE NOT the failure class."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base_items = [{"description": f"item {i}"} for i in range(4)]

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=1), \
         patch("src.forms.vision_parser.parse_with_vision") as mock_vis:
        out = _vision_verify_items_if_pdf(str(f), base_items, {})

    mock_vis.assert_not_called()
    assert out is None


def test_vision_runs_when_items_per_page_below_threshold(tmp_path):
    """A 2-page PDF with only 2 items extracted = 1 item/page < 3
    threshold — suspect, run Vision."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base_items = [{"description": "item 1"}, {"description": "item 2"}]

    mock_vision_result = {"line_items": [{"description": f"i{i}"} for i in range(10)]}
    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=2), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value=mock_vision_result) as mock_vis:
        out = _vision_verify_items_if_pdf(str(f), base_items, {})

    mock_vis.assert_called_once()
    assert out is not None


# ─── Upgrade logic: when does Vision's output replace regex's? ────────────


def test_vision_wins_when_it_finds_more_items(tmp_path):
    """The whole point: when Vision finds MORE items than regex,
    Vision's result is returned. Caller compares len() and swaps."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base_items = [{"description": "a"}, {"description": "b"}]
    vision_items = [{"description": f"item {i}"} for i in range(15)]

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=3), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value={"line_items": vision_items}):
        out = _vision_verify_items_if_pdf(str(f), base_items, {})

    assert out is not None
    assert len(out) == 15


def test_vision_returns_items_for_caller_to_compare(tmp_path):
    """Helper returns vision_items as long as Vision found ANY items
    on a suspect doc. Caller (in _parse_files) does the len() comparison
    and decides whether to swap. This keeps the helper composable."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")
    base_items = [{"description": "a"}]
    vision_items = [{"description": "b"}]

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=2), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value={"line_items": vision_items}):
        out = _vision_verify_items_if_pdf(str(f), base_items, {})

    # Helper returns vision_items even when count equal/lower — caller
    # decides. (Note: caller in _parse_files uses `> len(base)` so
    # equal counts keep base.)
    assert out == vision_items


def test_vision_unavailable_returns_none(tmp_path):
    """When Vision AI isn't configured (no API key), helper returns
    None and the regex output is the only signal we have."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=3), \
         patch("src.forms.vision_parser.is_available", return_value=False):
        out = _vision_verify_items_if_pdf(str(f), [], {})

    assert out is None


def test_vision_crash_is_swallowed_returns_none(tmp_path):
    """A Vision call that throws (API timeout, JSON parse failure, etc.)
    must not crash ingestion. Helper returns None; regex output stays."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=3), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               side_effect=RuntimeError("vision API timeout")):
        out = _vision_verify_items_if_pdf(str(f), [], {})

    assert out is None


def test_vision_empty_items_returns_none(tmp_path):
    """Vision ran but found nothing (e.g., the doc isn't really an
    item list). Helper returns None — caller keeps regex output."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=3), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value={"line_items": []}):
        out = _vision_verify_items_if_pdf(str(f), [], {})

    assert out is None


def test_vision_handles_items_key_alias(tmp_path):
    """Vision parser returns under `line_items` but historically some
    callers used `items` — helper accepts either."""
    f = tmp_path / "rfq.pdf"
    f.write_bytes(b"%PDF-1.4 stub")

    with patch("src.core.ingest_pipeline._pdf_page_count", return_value=3), \
         patch("src.forms.vision_parser.is_available", return_value=True), \
         patch("src.forms.vision_parser.parse_with_vision",
               return_value={"items": [{"description": "x"}]}):
        out = _vision_verify_items_if_pdf(str(f), [], {})

    assert out == [{"description": "x"}]


# ─── _pdf_page_count helper ──────────────────────────────────────────────


def test_pdf_page_count_returns_zero_on_unreadable_file(tmp_path):
    """A non-PDF or corrupt file returns 0 (treated as 'unknown' by
    caller — does not block Vision verification)."""
    from src.core.ingest_pipeline import _pdf_page_count
    f = tmp_path / "garbage.pdf"
    f.write_bytes(b"not really a pdf")
    assert _pdf_page_count(str(f)) == 0


def test_pdf_page_count_returns_zero_on_missing_file():
    from src.core.ingest_pipeline import _pdf_page_count
    assert _pdf_page_count("/tmp/does-not-exist.pdf") == 0
