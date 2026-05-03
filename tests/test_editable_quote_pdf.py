"""Tests for PR-D1: editable AcroForm working copy of the Reytech quote PDF.

Locked scope per project_editable_quote_pdf_2026_05_03.md:
  • editable=False (default) — flat PDF, NO /AcroForm dict, behavior unchanged
  • editable=True — buyer/ship-to block becomes AcroForm text fields
  • Letterhead, quote_number, dates, line items remain flat in PR-D1

Field names that must exist when editable=True (stable across pages
so PR-D2 read-back can find them):
  bill_name, bill_addr_<N>          — Bill To block (when shown)
  to_name, to_addr_<N>               — To: block (always shown)
  ship_name, ship_addr_<N>           — Ship To Location block
"""
from __future__ import annotations

import io
import os
import tempfile

import pytest

from src.forms.quote_generator import generate_quote


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


def _generate(**overrides):
    """Generate a quote PDF and return bytes."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
        path = tf.name
    try:
        generate_quote(
            _QUOTE_DATA,
            path,
            agency=overrides.pop("agency", "CDCR"),
            quote_number=overrides.pop("quote_number", "TEST-D1"),
            tax_rate=overrides.pop("tax_rate", 0.0),
            include_tax=False,
            shipping=0.0,
            **overrides,
        )
        with open(path, "rb") as fh:
            return fh.read()
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def _read_acroform_field_names(pdf_bytes: bytes) -> list[str]:
    """Return the list of AcroForm /T (field name) values in a PDF."""
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    return sorted(fields.keys())


def _has_acroform_dict(pdf_bytes: bytes) -> bool:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    root = reader.trailer["/Root"]
    return "/AcroForm" in root


# ─── editable=False behavior (default, must stay backward-compatible) ──


class TestFlatModeUnchanged:

    def test_flat_pdf_renders_without_error(self):
        pdf = _generate(editable=False)
        # PDFs start with %PDF-
        assert pdf.startswith(b"%PDF-")

    def test_flat_pdf_has_no_acroform(self):
        pdf = _generate(editable=False)
        assert not _has_acroform_dict(pdf), \
            "editable=False must not emit /AcroForm dict"

    def test_default_mode_is_flat(self):
        """editable defaults to False — backward compat with all callers."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            path = tf.name
        try:
            # Don't pass editable — relies on the default
            generate_quote(
                _QUOTE_DATA, path,
                agency="CDCR", quote_number="TEST-DEFAULT",
                tax_rate=0.0, include_tax=False,
            )
            with open(path, "rb") as fh:
                pdf = fh.read()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass
        assert not _has_acroform_dict(pdf)


# ─── editable=True emits AcroForm fields ──


class TestEditableModeFields:

    def test_editable_pdf_has_acroform(self):
        pdf = _generate(editable=True)
        assert _has_acroform_dict(pdf), \
            "editable=True must emit /AcroForm dict"

    def test_editable_pdf_has_buyer_block_fields(self):
        pdf = _generate(editable=True, agency="CCHCS")
        names = _read_acroform_field_names(pdf)
        # CCHCS: Bill To shown
        assert "bill_name" in names
        # ship_name always shown
        assert "ship_name" in names
        # at least one ship_addr line
        assert any(n.startswith("ship_addr_") for n in names)

    def test_editable_pdf_has_to_name_field(self):
        pdf = _generate(editable=True, agency="CDCR")
        names = _read_acroform_field_names(pdf)
        # The "To:" block (left col) renders to_name
        assert "to_name" in names

    def test_editable_pdf_field_names_are_stable_across_calls(self):
        """Same quote data → same field names. Required for PR-D2 read-back."""
        pdf1 = _generate(editable=True)
        pdf2 = _generate(editable=True)
        assert _read_acroform_field_names(pdf1) == _read_acroform_field_names(pdf2)

    def test_editable_pdf_renders_without_error(self):
        pdf = _generate(editable=True)
        assert pdf.startswith(b"%PDF-")

    def test_editable_field_value_matches_input(self):
        """Generated AcroForm field for ship_name must contain the input value."""
        from pypdf import PdfReader
        pdf = _generate(editable=True)
        reader = PdfReader(io.BytesIO(pdf))
        fields = reader.get_fields() or {}
        ship_name_field = fields.get("ship_name")
        assert ship_name_field is not None
        # field value lives at /V key
        v = ship_name_field.get("/V") or ship_name_field.get("value")
        assert v
        assert "Sacramento" in str(v)


# ─── PR-D1 hard-line: line items + quote_number stay flat ──


class TestNonEditableFieldsStayFlat:

    def test_quote_number_not_in_acroform(self):
        """quote_number is locked (counter-collision risk) per A3-lock-qno."""
        pdf = _generate(editable=True)
        names = _read_acroform_field_names(pdf)
        assert "quote_number" not in names

    def test_line_items_not_in_acroform_in_d1(self):
        """Line item fields land in PR-D1.5/D2; D1 only ships buyer/ship-to."""
        pdf = _generate(editable=True)
        names = _read_acroform_field_names(pdf)
        assert not any(n.startswith("item_") for n in names), \
            f"unexpected item_ fields in PR-D1: {[n for n in names if n.startswith('item_')]}"
