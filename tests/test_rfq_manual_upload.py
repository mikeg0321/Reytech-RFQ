"""Tests for the +RFQ manual upload page + API.

DSH ships RFQs through Proofpoint secure mail (`securemail.dsh.ca.gov`),
which Gmail can't poll. The manual-upload endpoint lets the operator drop
the packet PDFs + paste the email body into one screen and run the same
classifier_v2 pipeline the inbox poller uses.

These tests pin:
  - The page route renders and contains the dropzone + email field
  - The API rejects non-PDFs / empty submits with a 400
  - A real DSH 25CB020 packet upload runs through process_buyer_request,
    detects the `dsh` agency from the PDF text, and returns a usable
    IngestResult (ok or — at minimum — classification populated).
  - The header "+ RFQ" button is present on every page (base.html nav).
"""
from __future__ import annotations

import io
import os

import pytest


_DSH_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fixtures", "dsh",
)
_COVER = os.path.join(_DSH_DIR, "dsh_25CB020_cover.pdf")
_ATT_A = os.path.join(_DSH_DIR, "dsh_25CB020_attachA_bidder.pdf")
_ATT_B = os.path.join(_DSH_DIR, "dsh_25CB020_attachB_pricing.pdf")
_ATT_C = os.path.join(_DSH_DIR, "dsh_25CB020_attachC_forms.pdf")

_DSH_EMAIL = """\
Hello,
The Department of State Hospitals - Atascadero would like to request a quote
from your company, which will be awarded to the lowest cost vendor. Please
ensure requested documents are returned. All quotes must have the approximate
lead time under attachment A - Bidder's Information completed for quote to be
considered.

Deadline for submission is 3/30/2026 at 1:30 pm

Thank you,
Cornell Butuza, RN
Central Medical Services / Central Supply
Department of State Hospital - Atascadero
"""


class TestPageRender:

    def test_upload_page_renders(self, auth_client):
        resp = auth_client.get("/rfq/upload-manual")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "RFQ Manual Upload" in body or "+ RFQ" in body
        assert 'id="rfqFiles"' in body, "file input must be on the page"
        assert 'id="emailBody"' in body, "email-body textarea must be on the page"
        assert "/api/rfq/upload-manual" in body, (
            "page must POST to the manual-upload API"
        )

    def test_add_rfq_button_in_global_header(self, auth_client):
        """The '+ RFQ' button needs to be reachable from any page so DSH
        packets can be ingested mid-workflow without nav hunting."""
        resp = auth_client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert 'id="add-rfq-btn"' in body
        assert "/rfq/upload-manual" in body


class TestApiValidation:

    def test_no_files_returns_400(self, auth_client):
        resp = auth_client.post("/api/rfq/upload-manual",
                                data={}, content_type="multipart/form-data")
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["ok"] is False
        assert "PDF" in body["error"] or "file" in body["error"].lower()

    def test_non_pdf_rejected(self, auth_client):
        data = {"files": (io.BytesIO(b"not a pdf"), "evil.exe")}
        resp = auth_client.post("/api/rfq/upload-manual", data=data,
                                content_type="multipart/form-data")
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["ok"] is False
        assert "PDF" in body["error"]


@pytest.mark.skipif(
    not os.path.exists(_ATT_B),
    reason="DSH 25CB020 fixtures not present — copy them to tests/fixtures/dsh/",
)
class TestEndToEndDshPacket:

    def test_upload_packet_runs_pipeline(self, auth_client, temp_data_dir):
        """Drop the full DSH packet + email body. The pipeline should:
          - accept all 4 PDFs
          - return an IngestResult shape (ok flag, classification, record_id)
          - identify DSH agency from the PDF text patterns
        """
        from unittest.mock import patch
        attachments = []
        for path in (_COVER, _ATT_A, _ATT_B, _ATT_C):
            with open(path, "rb") as fh:
                attachments.append(
                    (io.BytesIO(fh.read()), os.path.basename(path))
                )
        data = {
            "files": attachments,
            "email_subject": "FW: Please find attached quote request 25CB020",
            "email_sender": "Corneliu.Butuza@dsh.ca.gov",
            "email_body": _DSH_EMAIL,
        }
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ):
            resp = auth_client.post("/api/rfq/upload-manual", data=data,
                                    content_type="multipart/form-data")
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        # Result shape comes from IngestResult.to_dict()
        for key in ("ok", "record_type", "record_id", "classification",
                    "items_parsed", "errors", "warnings"):
            assert key in body, f"missing IngestResult key: {key}"
        # Classification must mark this as a DSH request — the cover PDF +
        # email body both carry the DSH/ATASCADERO match patterns.
        cls = body.get("classification") or {}
        agency = (cls.get("agency") or "").lower()
        # The classifier may report the agency as "dsh" directly, or carry
        # it inside the broader fields — assert it's recognisable as DSH.
        haystack = (
            agency
            + " " + (cls.get("agency_name") or "").lower()
            + " " + " ".join(str(v).lower() for v in cls.values()
                             if isinstance(v, str))
        )
        assert "dsh" in haystack or "atascadero" in haystack, (
            f"classifier did not detect DSH/Atascadero from packet+email; "
            f"classification={cls}"
        )
