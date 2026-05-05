"""Cascade fallback to attachment filename for PC/RFQ name (Surface #17).

Mike's screenshot 2026-05-04: queue showed two PCs with `AUTO_db670ad9` and
`AUTO_43afa525` while other rows for the same buyer correctly displayed
the attachment filename like `AMS 704 - Heel Donut - 04.29.26`.

Per feedback_global_fix_not_one_off + feedback_app_is_source_of_truth:
the attachment filename IS source-of-truth for the title when the buyer
left the embedded PRICE CHECK field blank (per
project_ams704_ingest_drift_2026_05_03 surface #1, this is intentional
buyer behavior — not a parsing bug). The cascade adds the filename as a
4th step before the `AUTO_<short_id>` last resort.
"""
from __future__ import annotations

import pytest

from src.core.ingest_pipeline import _attachment_filename_title


class TestStripsAttachmentBoilerplate:
    def test_strips_pdf_extension(self):
        assert _attachment_filename_title("/data/AMS 704 - Heel Donut - 04.29.26.pdf") == "Heel Donut - 04.29.26"

    def test_strips_ams_704_prefix(self):
        assert _attachment_filename_title("AMS 704 - Equipment - 05.01.26.pdf") == "Equipment - 05.01.26"

    def test_strips_quote_prefix(self):
        assert _attachment_filename_title("Quote - Welch Allyn pole.pdf") == "Welch Allyn pole"

    def test_strips_rfq_prefix(self):
        assert _attachment_filename_title("RFQ - Bandages.pdf") == "Bandages"

    def test_strips_price_check_prefix(self):
        assert _attachment_filename_title("Price Check - Cleaning Supplies.pdf") == "Cleaning Supplies"

    def test_handles_docx(self):
        assert _attachment_filename_title("AMS 704 - Office Furniture.docx") == "Office Furniture"

    def test_case_insensitive_prefix(self):
        # The original Heel Donut case had spaces around the dash that varied
        assert _attachment_filename_title("ams 704 - LowerCase test.pdf") == "LowerCase test"


class TestRejectsUnusableTitles:
    def test_returns_empty_for_empty_input(self):
        assert _attachment_filename_title("") == ""
        assert _attachment_filename_title(None) == ""  # type: ignore[arg-type]

    def test_returns_empty_for_too_short(self):
        # "AB.pdf" → strip ext → "AB" (2 chars, below 3-char threshold)
        assert _attachment_filename_title("AB.pdf") == ""

    def test_rejects_uuid_shaped_titles(self):
        """A pure-hex filename is worse than AUTO_<8hex> as a display title —
        AUTO_ is at least signaling 'system-generated'. Reject so the cascade
        falls through to AUTO_<short_id>."""
        assert _attachment_filename_title("/tmp/db670ad94caea3f1.pdf") == ""
        assert _attachment_filename_title("abc123def456.pdf") == ""

    def test_returns_empty_when_only_boilerplate(self):
        # If filename is JUST the boilerplate, strip leaves nothing.
        # "AMS 704.pdf" → "AMS 704" → no boilerplate match (no separator) →
        # returns "AMS 704" as-is. That's fine since "AMS 704" alone IS a
        # usable display title (better than AUTO_<hex>). Verify the no-match
        # path doesn't accidentally return empty.
        assert _attachment_filename_title("AMS 704.pdf") == "AMS 704"


class TestSourceWiring:
    """Source-level guards that the cascade actually uses the helper."""

    def test_helper_is_exported(self):
        """The helper must be importable from ingest_pipeline."""
        from src.core.ingest_pipeline import _attachment_filename_title
        assert callable(_attachment_filename_title)

    def test_both_pc_and_rfq_cascades_include_attachment_title(self):
        """Source-level: capture each cascade by tracking paren depth so
        `header.get("pc_number", "")` (which contains a literal `)`) doesn't
        prematurely close the block."""
        from pathlib import Path
        src_lines = Path("src/core/ingest_pipeline.py").read_text(
            encoding="utf-8"
        ).splitlines()

        def _capture_cascade(opener: str) -> str:
            block, inside, depth = [], False, 0
            for line in src_lines:
                if opener in line:
                    inside = True
                if inside:
                    block.append(line)
                    depth += line.count("(") - line.count(")")
                    if depth <= 0:
                        break
            return "\n".join(block)

        for record_kind, opener in [
            ("pc", 'record["pc_number"] = ('),
            ("rfq", 'record["rfq_number"] = ('),
        ]:
            block = _capture_cascade(opener)
            assert block, f"{record_kind} cascade block not found"
            assert "_attachment_title" in block, (
                f"{record_kind.upper()} cascade missing _attachment_title "
                f"fallback. Surface #17 — per feedback_global_fix_not_one_off "
                f"both PC and RFQ must share the cascade."
            )
            attach_idx = block.find("_attachment_title")
            auto_idx = block.find("AUTO_")
            assert 0 <= attach_idx < auto_idx, (
                f"{record_kind}: _attachment_title must come BEFORE AUTO_ "
                f"in the or-cascade — otherwise the filename never wins."
            )
