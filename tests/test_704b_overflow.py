"""Regression tests for the 704B chunked-refill overflow path.

Coleman 10842771 / rfq_5a55f1b5 (2026-05-28) shipped a 704B with
items 16-21 silently dropped (15-row template, 21-item quote). Mike's
fix-direction: "duplicate an empty form and refill" — i.e. when items
exceed the template's row capacity, chunk the items into capacity-sized
groups, fill the empty template once per chunk, flatten each filled
copy (so re-using row field names across chunks doesn't collide via
PDF shared-AcroForm semantics), and concatenate the chunks into a
multi-page output.

These tests pin:
1. 21-item input → multi-page output (not silently truncated)
2. Output page count == ceil(items / capacity) × pages-per-chunk
3. Item descriptions from the overflow chunk(s) appear in the output
"""
from __future__ import annotations

import io
import math
import os
import sys

import pytest
from pypdf import PdfReader

# Make sure we can import src.* without a flake.
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)


_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
_704B_BLANK = os.path.join(_FIXTURES_DIR, "704b_blank.pdf")


def _has_fixture() -> bool:
    return os.path.exists(_704B_BLANK)


pytestmark = pytest.mark.skipif(
    not _has_fixture(),
    reason="tests/fixtures/704b_blank.pdf not checked in",
)


def _build_items(n: int) -> list[dict]:
    """Build N synthetic line items in the legacy raw-dict shape that
    fill_704b consumes. Distinct descriptions per item so we can assert
    overflow-chunk items appear in the final output."""
    return [
        {
            "line_number": i,
            "description": f"Overflow Test Item #{i:02d}",
            "qty": i + 1,
            "uom": "EA",
            "unit_price": 100.00 + i,
            "supplier_cost": 80.00 + i,
            "mfg_number": f"TEST-MFG-{i:03d}",
            "item_number": f"TEST-MFG-{i:03d}",
        }
        for i in range(1, n + 1)
    ]


def _minimal_config(tmp_path) -> dict:
    """A minimal `config` dict satisfying the keys fill_704b reads."""
    return {
        "company": {
            "name": "Reytech Inc.",
            "owner": "Mike G.",
            "address": "16756 Chino-Corona Road, Corona, CA 92880",
            "phone": "(949) 555-0100",
            "email": "sales@reytechinc.com",
            "fein": "00-0000000",
            "sellers_permit": "TEST-PERMIT",
            "title": "Owner",
            "cert_number": "TEST-CERT",
            "cert_expiration": "2099-12-31",
        }
    }


# ─────────────────────────────────────────────────────────────────────


class TestColeman704BOverflowChunkedRefill:
    """Mike's directive 2026-05-28: "investigate overflow, just duplicate
    an empty form and refill, i can put tags on the line items." Pin the
    chunked-refill mechanic at the level of byte output."""

    def _overflow_count(self):
        """Compute an item count that forces overflow on the fixture
        template — fixture capacity + 6 (mirrors Coleman's 15+6=21
        ratio against the prod buyer template)."""
        from src.forms.template_registry import get_profile
        profile = get_profile(_704B_BLANK)
        capacity = (
            len(profile.pg1_rows or [])
            + len(profile.pg2_rows_suffixed or [])
            + len(profile.pg2_rows_plain or [])
        )
        return capacity, capacity + 6  # 6-item overflow chunk

    def test_overflow_produces_multi_chunk_output(self, tmp_path):
        """Items > template capacity must produce > 1 chunk — the
        pre-fix silent-drop is the regression this pins."""
        from src.forms.reytech_filler_v4 import fill_704b

        capacity, total_items = self._overflow_count()
        out_path = os.path.join(tmp_path, "10842771_704B_Reytech.pdf")
        rfq_data = {
            "line_items": _build_items(total_items),
            "solicitation_number": "10842771",
            "sign_date": "2026-05-28",
        }
        fill_704b(_704B_BLANK, rfq_data, _minimal_config(tmp_path), out_path)

        # Output file exists + has multiple chunks.
        assert os.path.exists(out_path), (
            "fill_704b did not write the output file"
        )
        reader = PdfReader(out_path)
        page_count = len(reader.pages)

        expected_chunks = math.ceil(total_items / capacity)
        single_pdf_page_count = len(PdfReader(_704B_BLANK).pages)
        expected_min_pages = expected_chunks * single_pdf_page_count

        assert page_count >= expected_min_pages, (
            f"Expected >= {expected_min_pages} pages "
            f"({expected_chunks} chunks × {single_pdf_page_count} pages/chunk) "
            f"for {total_items} items at capacity={capacity}, "
            f"got {page_count}. Overflow path likely silently dropped chunks."
        )

    def test_overflow_chunk_items_appear_in_output_text(self, tmp_path):
        """Items past the first chunk must be present in the output —
        proves they weren't silently dropped, AND the overflow chunk
        actually rendered (the exact Coleman 10842771 regression)."""
        from src.forms.reytech_filler_v4 import fill_704b

        capacity, total_items = self._overflow_count()
        out_path = os.path.join(tmp_path, "10842771_704B_overflow.pdf")
        rfq_data = {
            "line_items": _build_items(total_items),
            "solicitation_number": "10842771",
            "sign_date": "2026-05-28",
        }
        fill_704b(_704B_BLANK, rfq_data, _minimal_config(tmp_path), out_path)

        # Concatenate all-page text. Items past `capacity` (which live
        # on overflow chunks) MUST appear in the rendered bytes.
        reader = PdfReader(out_path)
        all_text = ""
        for page in reader.pages:
            try:
                all_text += page.extract_text() or ""
            except Exception:
                pass

        # Sample three overflow-chunk descriptions (first, middle, last).
        overflow_first = capacity + 1
        overflow_last = total_items
        overflow_middle = (overflow_first + overflow_last) // 2
        for n in (overflow_first, overflow_middle, overflow_last):
            marker = f"Overflow Test Item #{n:02d}"
            assert marker in all_text, (
                f"overflow item #{n} ({marker!r}) not found in output PDF. "
                "Chunked refill is silently dropping items — the exact "
                "Coleman 10842771 regression. Got text sample: "
                f"{all_text[:300]!r}..."
            )

    def test_15_items_or_fewer_uses_single_fill_path(self, tmp_path):
        """At-or-under capacity, the existing single-fill path must
        remain unchanged. No overflow path entered, no flatten applied
        — output retains form fields for the Inspector to read."""
        from src.forms.reytech_filler_v4 import fill_704b
        from src.forms.template_registry import get_profile

        profile = get_profile(_704B_BLANK)
        capacity = (
            len(profile.pg1_rows or [])
            + len(profile.pg2_rows_suffixed or [])
            + len(profile.pg2_rows_plain or [])
        )
        target_count = min(capacity, 5)  # well within capacity

        out_path = os.path.join(tmp_path, "small_704b.pdf")
        rfq_data = {
            "line_items": _build_items(target_count),
            "solicitation_number": "10842771",
            "sign_date": "2026-05-28",
        }
        fill_704b(_704B_BLANK, rfq_data, _minimal_config(tmp_path), out_path)

        assert os.path.exists(out_path)
        reader = PdfReader(out_path)
        # The single-fill output should keep form fields (NOT flattened).
        # The Inspector gate + verify_704b_computations rely on this.
        fields = reader.get_fields() or {}
        assert len(fields) > 0, (
            "single-fill output should retain form fields — "
            "the overflow path's flatten step must NOT be applied "
            "when items fit on one chunk."
        )
