"""Tests for src/forms/quote_pdf_flatten — PR-D3 flatten + DB-sync helpers.

Pins the contract that PR-D4 (the route wiring) will rely on:
  • flatten_quote_pdf(editable_bytes) → (flat_bytes, edits_dict)
  • flat_bytes has no /AcroForm dict
  • edits_dict captures the operator's AcroForm field values BEFORE flatten
  • diff_to_quote_fields(edits) → canonical Quote DB column updates
"""
from __future__ import annotations

import io
import os
import tempfile

import pytest

from src.forms.quote_generator import generate_quote
from src.forms.quote_pdf_flatten import (
    diff_to_quote_fields,
    flatten_quote_pdf,
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


def _generate(editable: bool, agency: str = "CCHCS") -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
        path = tf.name
    try:
        generate_quote(
            _QUOTE_DATA, path,
            agency=agency, quote_number="TEST-D3",
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


def _has_acroform(pdf_bytes: bytes) -> bool:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "/AcroForm" in reader.trailer["/Root"]


# ─── flatten_quote_pdf ─────────────────────────────────────────────────


class TestFlattenQuotePdf:

    def test_editable_pdf_loses_acroform_after_flatten(self):
        editable = _generate(editable=True)
        assert _has_acroform(editable), "precondition: editable input must have /AcroForm"
        flat, _edits = flatten_quote_pdf(editable)
        assert flat.startswith(b"%PDF-")
        assert not _has_acroform(flat), \
            "flatten_quote_pdf must remove /AcroForm so buyer can't edit"

    def test_flatten_returns_edits_dict(self):
        editable = _generate(editable=True, agency="CCHCS")
        _flat, edits = flatten_quote_pdf(editable)
        # ship_name + bill_name are CCHCS standard
        assert "ship_name" in edits
        assert "bill_name" in edits

    def test_flat_input_passes_through(self):
        flat = _generate(editable=False)
        assert not _has_acroform(flat)
        out, edits = flatten_quote_pdf(flat)
        # No fields to read, no flattening to do
        assert edits == {}
        # Output should still be a valid PDF
        assert out.startswith(b"%PDF-")

    def test_empty_input_returns_empty(self):
        out, edits = flatten_quote_pdf(b"")
        assert out == b""
        assert edits == {}

    def test_garbage_input_does_not_raise(self):
        # Helper is best-effort. Returns the input + empty edits dict.
        out, edits = flatten_quote_pdf(b"not a pdf")
        # Either returns the input or empty bytes — never raises
        assert isinstance(out, (bytes, bytearray))
        assert edits == {}


# ─── diff_to_quote_fields ──────────────────────────────────────────────


class TestDiffToQuoteFields:

    def test_empty_edits_yields_empty(self):
        assert diff_to_quote_fields({}) == {}
        assert diff_to_quote_fields(None) == {}

    def test_ship_block_collected(self):
        edits = {
            "ship_name": "CSP Sacramento - New Folsom",
            "ship_addr_1": "100 Prison Road",
            "ship_addr_2": "Represa, CA 95671",
        }
        out = diff_to_quote_fields(edits)
        assert out["ship_to_name"] == "CSP Sacramento - New Folsom"
        assert out["ship_to_address"] == ["100 Prison Road", "Represa, CA 95671"]

    def test_bill_block_collected(self):
        edits = {
            "bill_name": "CCHCS Accounting",
            "bill_addr_1": "PO Box 588500",
            "bill_addr_2": "Elk Grove, CA 95758",
        }
        out = diff_to_quote_fields(edits)
        assert out["bill_to_name"] == "CCHCS Accounting"
        assert out["bill_to_address"] == ["PO Box 588500", "Elk Grove, CA 95758"]

    def test_to_block_collected(self):
        edits = {
            "to_name": "Jane Buyer",
            "to_addr_1": "Procurement Office",
        }
        out = diff_to_quote_fields(edits)
        assert out["contact_name"] == "Jane Buyer"
        assert out["contact_address"] == ["Procurement Office"]

    def test_partial_blocks_dont_create_keys(self):
        # Only ship_name set — no ship_addr lines
        edits = {"ship_name": "X"}
        out = diff_to_quote_fields(edits)
        assert out == {"ship_to_name": "X"}
        assert "ship_to_address" not in out

    def test_blank_values_dropped(self):
        edits = {"ship_name": "  ", "ship_addr_1": ""}
        out = diff_to_quote_fields(edits)
        assert out == {}

    def test_address_collection_stops_at_first_gap(self):
        # If addr_2 is empty, addr_3 isn't read (avoids gaps)
        edits = {
            "ship_addr_1": "Line 1",
            "ship_addr_2": "",
            "ship_addr_3": "Line 3 — should not appear",
        }
        out = diff_to_quote_fields(edits)
        assert out["ship_to_address"] == ["Line 1"]

    def test_address_collection_caps_at_12(self):
        # Sanity guard: even if 99 numbered fields are present, helper stops at 12
        edits = {f"ship_addr_{i}": f"L{i}" for i in range(1, 100)}
        out = diff_to_quote_fields(edits)
        assert len(out["ship_to_address"]) == 12


# ─── End-to-end roundtrip: generate → flatten → diff_to_quote_fields ──


class TestEndToEndRoundtrip:

    def test_generate_then_flatten_yields_canonical_db_dict(self):
        editable = _generate(editable=True, agency="CCHCS")
        _flat, edits = flatten_quote_pdf(editable)
        canonical = diff_to_quote_fields(edits)
        # The original quote_data had ship_to_name = "CSP Sacramento - New Folsom"
        # — generator may rewrite via facility lookup but it should still be a
        # CSP/Folsom variant, NOT empty.
        assert "ship_to_name" in canonical
        assert canonical["ship_to_name"]
