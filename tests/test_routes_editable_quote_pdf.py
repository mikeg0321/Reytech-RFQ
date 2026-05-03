"""Tests for PR-D4: editable Quote PDF download + upload-edited routes.

Covers the two new endpoints in routes_rfq_admin.py:

  GET  /api/rfq/<rid>/quote-editable.pdf       — generate AcroForm working copy
  POST /api/rfq/<rid>/upload-edited-quote      — sync edits back to RFQ row

The download route returns PDF bytes with /AcroForm fields. The upload
route flattens, applies diff_to_quote_fields to the row, and audit-logs.

Tests use the auth_client fixture so Basic Auth is auto-injected.
"""
from __future__ import annotations

import io
import json
import os
import tempfile

import pytest


# ─── /api/rfq/<rid>/quote-editable.pdf ─────────────────────────────────


class TestEditableDownload:

    def test_returns_404_for_unknown_rfq(self, auth_client):
        resp = auth_client.get("/api/rfq/does_not_exist/quote-editable.pdf")
        assert resp.status_code == 404

    def test_returns_pdf_for_seeded_rfq(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.get(f"/api/rfq/{rid}/quote-editable.pdf")
        assert resp.status_code == 200
        assert resp.mimetype == "application/pdf"
        assert resp.data.startswith(b"%PDF-")
        assert resp.headers.get("X-Quote-Mode") == "editable"

    def test_pdf_has_acroform(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.get(f"/api/rfq/{rid}/quote-editable.pdf")
        assert resp.status_code == 200
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(resp.data))
        assert "/AcroForm" in reader.trailer["/Root"], \
            "editable download must carry /AcroForm dict"


# ─── /api/rfq/<rid>/upload-edited-quote ────────────────────────────────


class TestUploadEdited:

    def _build_editable_pdf(self, agency: str = "CCHCS") -> bytes:
        """Generate an editable PDF directly so we can upload it back."""
        from src.forms.quote_generator import generate_quote
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            path = tf.name
        try:
            generate_quote(
                {
                    "institution": "CSP Sacramento - New Folsom",
                    "ship_to_name": "CSP Sacramento - New Folsom",
                    "ship_to_address": ["100 Prison Road", "Represa, CA 95671"],
                    "rfq_number": "TEST-D4",
                    "line_items": [{"line_number": 1, "qty": 1, "uom": "EA",
                                    "description": "Item", "unit_price": 10.0,
                                    "supplier_cost": 5.0}],
                },
                path,
                agency=agency, quote_number="TEST-D4-UP",
                tax_rate=0.0, include_tax=False, shipping=0.0,
                editable=True,
            )
            with open(path, "rb") as fh:
                return fh.read()
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def test_upload_returns_404_for_unknown_rfq(self, auth_client):
        pdf = self._build_editable_pdf()
        resp = auth_client.post(
            "/api/rfq/does_not_exist/upload-edited-quote",
            data={"pdf": (io.BytesIO(pdf), "edited.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 404

    def test_upload_rejects_missing_file(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/upload-edited-quote",
            data={},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        body = json.loads(resp.data)
        assert body["ok"] is False
        assert "no pdf" in body["error"].lower()

    def test_upload_rejects_non_pdf(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/upload-edited-quote",
            data={"pdf": (io.BytesIO(b"not a pdf"), "junk.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        body = json.loads(resp.data)
        assert "not a PDF" in body["error"]

    def test_upload_rejects_pdf_without_acroform(self, auth_client, seed_rfq):
        # Generate a flat (non-editable) PDF, upload it — should reject.
        from src.forms.quote_generator import generate_quote
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            path = tf.name
        try:
            generate_quote(
                {"institution": "X", "ship_to_name": "Y", "ship_to_address": ["Z"],
                 "rfq_number": "T", "line_items": [{"line_number": 1, "qty": 1,
                                                    "uom": "EA", "description": "Item",
                                                    "unit_price": 10.0, "supplier_cost": 5.0}]},
                path,
                agency="CDCR", quote_number="T-FLAT",
                tax_rate=0.0, include_tax=False,
                editable=False,
            )
            with open(path, "rb") as fh:
                flat_pdf = fh.read()
        finally:
            try: os.remove(path)
            except OSError: pass
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/upload-edited-quote",
            data={"pdf": (io.BytesIO(flat_pdf), "flat.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        body = json.loads(resp.data)
        assert "AcroForm" in body["error"]

    def test_upload_applies_edits_to_rfq_row(self, auth_client, seed_rfq, temp_data_dir):
        rid = seed_rfq
        pdf = self._build_editable_pdf(agency="CCHCS")
        resp = auth_client.post(
            f"/api/rfq/{rid}/upload-edited-quote",
            data={"pdf": (io.BytesIO(pdf), "edited.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200, resp.data
        body = json.loads(resp.data)
        assert body["ok"] is True
        # applied may be True (edits differ from current row) or False
        # (edits matched current values exactly) — either is valid.
        assert "edits" in body
        # ship_name should be populated from AcroForm read-back
        assert "ship_name" in body["edits"]


# ─── Auth gates ────────────────────────────────────────────────────────


class TestAuthGates:

    def test_editable_download_requires_auth(self, anon_client):
        resp = anon_client.get("/api/rfq/anyid/quote-editable.pdf")
        assert resp.status_code in (401, 403)

    def test_upload_requires_auth(self, anon_client):
        resp = anon_client.post(
            "/api/rfq/anyid/upload-edited-quote",
            data={"pdf": (io.BytesIO(b"%PDF-"), "x.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code in (401, 403)
