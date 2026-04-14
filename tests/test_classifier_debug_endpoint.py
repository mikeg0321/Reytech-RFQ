"""Tests for /api/admin/classifier/classify debug endpoint."""
import io
import json
import os

import pytest


FIX_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "unified_ingest",
)
CCHCS_PACKET = os.path.join(FIX_DIR, "cchcs_packet_preq.pdf")
PC_DOCX = os.path.join(FIX_DIR, "pc_docx_food.docx")


class TestClassifierDebugEndpoint:
    def test_upload_cchcs_packet_returns_shape(self, client):
        if not os.path.exists(CCHCS_PACKET):
            pytest.skip("cchcs packet fixture missing")
        with open(CCHCS_PACKET, "rb") as f:
            data = {
                "file": (f, "packet.pdf"),
                "email_subject": "PREQ10843276",
                "email_sender": "buyer@cdcr.ca.gov",
            }
            r = client.post(
                "/api/admin/classifier/classify",
                data=data,
                content_type="multipart/form-data",
            )
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["classification"]["shape"] == "cchcs_packet"
        assert d["classification"]["agency"] == "cchcs"

    def test_upload_docx_704(self, client):
        if not os.path.exists(PC_DOCX):
            pytest.skip("docx fixture missing")
        with open(PC_DOCX, "rb") as f:
            r = client.post(
                "/api/admin/classifier/classify",
                data={"file": (f, "pc.docx")},
                content_type="multipart/form-data",
            )
        assert r.status_code == 200
        d = r.get_json()
        assert d["classification"]["shape"] == "pc_704_docx"
        assert d["classification"]["is_quote_only"] is True

    def test_no_file_with_email_only(self, client):
        r = client.post(
            "/api/admin/classifier/classify",
            data={
                "email_body": "Need pricing on VHC-WLA medical supplies",
                "email_sender": "buyer@calvet.ca.gov",
                "email_subject": "Quote request",
            },
            content_type="multipart/form-data",
        )
        assert r.status_code == 200
        d = r.get_json()
        assert d["classification"]["shape"] == "email_only"
        assert d["classification"]["agency"] == "calvet"

    def test_endpoint_does_not_persist_anything(self, client, temp_data_dir):
        """Debug endpoint is read-only — must NOT create any PC or RFQ."""
        if not os.path.exists(CCHCS_PACKET):
            pytest.skip("cchcs packet fixture missing")

        from src.api.dashboard import _load_price_checks, load_rfqs
        pcs_before = len(_load_price_checks())
        rfqs_before = len(load_rfqs())

        with open(CCHCS_PACKET, "rb") as f:
            client.post(
                "/api/admin/classifier/classify",
                data={"file": (f, "p.pdf")},
                content_type="multipart/form-data",
            )

        pcs_after = len(_load_price_checks())
        rfqs_after = len(load_rfqs())
        assert pcs_before == pcs_after, "debug endpoint must not create PCs"
        assert rfqs_before == rfqs_after, "debug endpoint must not create RFQs"

    def test_auth_required(self, anon_client):
        r = anon_client.post(
            "/api/admin/classifier/classify",
            data={"email_body": "test"},
            content_type="multipart/form-data",
        )
        assert r.status_code in (401, 403)
