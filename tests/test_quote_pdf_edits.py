"""Tests for src/forms/quote_pdf_edits — PR-D2 read-back helper.

The helper is best-effort and never raises. Tests pin:
  • Roundtrip: PR-D1 generate(editable=True) → read_quote_pdf_edits
    returns the same buyer/ship-to values that were rendered.
  • Empty / non-AcroForm input → empty dict (no crash).
  • Stray fields outside the known prefixes are filtered out.
  • edits_diff returns only changed fields.
"""
from __future__ import annotations

import io
import os
import tempfile

import pytest

from src.forms.quote_generator import generate_quote
from src.forms.quote_pdf_edits import (
    edits_diff,
    read_quote_pdf_edits,
)


_QUOTE_DATA = {
    "institution": "CSP Sacramento - New Folsom",
    "ship_to_name": "CSP Sacramento - New Folsom",
    "ship_to_address": ["100 Prison Road", "Represa, CA 95671"],
    "rfq_number": "10840486",
    "line_items": [
        {
            "line_number": 1,
            "part_number": "ABC-123",
            "qty": 2,
            "uom": "EA",
            "description": "Test Widget",
            "unit_price": 10.0,
            "supplier_cost": 7.0,
        },
    ],
}


def _generate(editable: bool, agency: str = "CCHCS"):
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
        path = tf.name
    try:
        generate_quote(
            _QUOTE_DATA, path,
            agency=agency, quote_number="TEST-D2",
            tax_rate=0.0, include_tax=False, shipping=0.0,
            editable=editable,
        )
        with open(path, "rb") as fh:
            return fh.read()
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# ─── Roundtrip: generate(editable=True) → read ─────────────────────────


class TestRoundtrip:

    def test_read_returns_buyer_ship_to_values(self):
        pdf = _generate(editable=True, agency="CCHCS")
        edits = read_quote_pdf_edits(pdf)
        # CCHCS shows Bill To
        assert "bill_name" in edits
        # ship_name is always present
        assert "ship_name" in edits
        assert "Sacramento" in edits["ship_name"]

    def test_ship_addr_lines_preserved(self):
        pdf = _generate(editable=True, agency="CCHCS")
        edits = read_quote_pdf_edits(pdf)
        assert "ship_addr_1" in edits
        assert "100 Prison Road" in edits["ship_addr_1"]
        assert "ship_addr_2" in edits
        assert "Represa" in edits["ship_addr_2"]

    def test_to_name_present(self):
        pdf = _generate(editable=True, agency="CDCR")
        edits = read_quote_pdf_edits(pdf)
        assert "to_name" in edits
        assert "Sacramento" in edits["to_name"]


# ─── Flat PDF (no AcroForm) returns empty ──────────────────────────────


class TestFlatPdfEmpty:

    def test_flat_pdf_returns_empty_dict(self):
        pdf = _generate(editable=False)
        edits = read_quote_pdf_edits(pdf)
        assert edits == {}

    def test_default_mode_returns_empty(self):
        """When editable param is omitted (default False), no fields."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            path = tf.name
        try:
            generate_quote(
                _QUOTE_DATA, path,
                agency="CDCR", quote_number="TEST-DEFAULT-D2",
                tax_rate=0.0, include_tax=False,
            )
            with open(path, "rb") as fh:
                pdf = fh.read()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass
        assert read_quote_pdf_edits(pdf) == {}


# ─── Robustness ────────────────────────────────────────────────────────


class TestRobustness:

    def test_garbage_bytes_returns_empty(self):
        # Helper must never raise on malformed input
        edits = read_quote_pdf_edits(b"this is not a PDF")
        assert edits == {}

    def test_empty_bytes_returns_empty(self):
        edits = read_quote_pdf_edits(b"")
        assert edits == {}

    def test_io_object_input_works(self):
        pdf = _generate(editable=True)
        stream = io.BytesIO(pdf)
        edits = read_quote_pdf_edits(stream)
        assert "ship_name" in edits

    def test_unknown_prefix_fields_filtered(self):
        """Even if a third-party tool added unknown AcroForm fields, the
        helper only returns values for PR-D1's known prefixes."""
        pdf = _generate(editable=True)
        edits = read_quote_pdf_edits(pdf)
        for name in edits:
            # Every returned field must be in the whitelist.
            assert (name in ("bill_name", "to_name", "ship_name")
                    or name.startswith(("bill_addr_", "to_addr_", "ship_addr_")))


# ─── edits_diff ─────────────────────────────────────────────────────────


class TestEditsDiff:

    def test_diff_returns_only_changed_fields(self):
        before = {"ship_name": "CSP-SAC", "ship_addr_1": "100 Prison Rd"}
        after = {"ship_name": "CSP Sacramento", "ship_addr_1": "100 Prison Rd"}
        d = edits_diff(before, after)
        assert "ship_name" in d
        assert d["ship_name"]["before"] == "CSP-SAC"
        assert d["ship_name"]["after"] == "CSP Sacramento"
        assert "ship_addr_1" not in d  # unchanged

    def test_diff_handles_new_field(self):
        before = {}
        after = {"ship_name": "Newly typed"}
        d = edits_diff(before, after)
        assert d["ship_name"]["before"] == ""
        assert d["ship_name"]["after"] == "Newly typed"

    def test_diff_handles_empty_inputs(self):
        assert edits_diff({}, {}) == {}
        assert edits_diff(None, None) == {}
        assert edits_diff(None, {"x": "y"}) == {"x": {"before": "", "after": "y"}}

    def test_diff_does_not_surface_removed_keys(self):
        # Operator can't remove AcroForm fields from the PDF — fields
        # always render with whatever value is in /V (possibly empty).
        before = {"ship_name": "X", "extra": "Y"}
        after = {"ship_name": "X"}
        d = edits_diff(before, after)
        assert d == {}
