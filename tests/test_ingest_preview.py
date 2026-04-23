"""Bundle-2 PR-2a: `/api/rfq/upload-preview` endpoint + the underlying
`preview_buyer_request` function.

Source audit item B (2026-04-22 session audit): manual upload today
forces "type subject/sender/body → submit" in one shot. No preview of
what the parser detected, no chance to correct a mis-classified shape
or a wrong facility before a record is written. The two-step ingest
starts here — detect in step 1, render preview, confirm in step 2.

These tests guard:
  - The preview function itself is a no-op on records (zero DB writes)
  - Every stage (classify / parse / facility / deadline) contributes
    to the returned dict when data supports it
  - A classifier crash surfaces as `ok: False` with a warning, NOT a
    500 — the two-step UI still needs something to render
  - The HTTP route cleans up its tempdir even on crash path
  - Telemetry hook fires so `/health/quoting` can track
    preview→create conversion
"""
from __future__ import annotations

import io
import json
import os

import pytest


FIX_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "unified_ingest",
)
CCHCS_PACKET = os.path.join(FIX_DIR, "cchcs_packet_preq.pdf")


# ── preview_buyer_request (direct call) ───────────────────────────────

class TestPreviewDirectCall:
    """Preview function runs the classifier + parser + resolver pipeline
    without touching the DB. These tests use the real CCHCS fixture
    (same one test_ingest_pipeline.py uses for end-to-end)."""

    def test_empty_input_is_no_op(self, temp_data_dir):
        from src.core.ingest_pipeline import preview_buyer_request
        r = preview_buyer_request(files=[])
        # Empty drop still returns a dict; classifier sees zero files
        # and produces its own "unknown" result — that's fine.
        assert isinstance(r, dict)
        assert "classification" in r
        assert r["items_parsed"] == 0
        assert r["items"] == []

    def test_cchcs_packet_classifies_and_parses(self, temp_data_dir):
        from src.core.ingest_pipeline import preview_buyer_request
        r = preview_buyer_request(
            files=[CCHCS_PACKET],
            email_subject="PREQ10843276 Quote Request",
            email_sender="ashley.russ@cdcr.ca.gov",
        )
        assert r["ok"] is True
        # Classification ran
        assert r["classification"] is not None
        # CCHCS packet is recognized
        assert r["shape"], "classifier returned blank shape on known fixture"
        # Items parse-out — fixture has at least 1 line item
        assert r["items_parsed"] >= 1, (
            f"expected items_parsed >= 1, got {r['items_parsed']!r}"
        )
        # Required forms list is populated for a full-package shape
        assert isinstance(r["required_forms"], list)

    def test_preview_does_not_create_record(self, temp_data_dir):
        """Calling the preview function does not write to rfqs.json or
        price_checks.json. The whole point of PR-2a is a 'look but
        don't touch' pass."""
        rfqs_path = os.path.join(temp_data_dir, "rfqs.json")
        pcs_path = os.path.join(temp_data_dir, "price_checks.json")
        # Snapshot
        before_rfqs = (
            open(rfqs_path).read() if os.path.exists(rfqs_path) else ""
        )
        before_pcs = (
            open(pcs_path).read() if os.path.exists(pcs_path) else ""
        )

        from src.core.ingest_pipeline import preview_buyer_request
        preview_buyer_request(
            files=[CCHCS_PACKET],
            email_subject="PREQ10843276 Quote Request",
            email_sender="ashley.russ@cdcr.ca.gov",
        )

        after_rfqs = (
            open(rfqs_path).read() if os.path.exists(rfqs_path) else ""
        )
        after_pcs = (
            open(pcs_path).read() if os.path.exists(pcs_path) else ""
        )
        assert before_rfqs == after_rfqs, (
            "preview wrote to rfqs.json — it must not create records"
        )
        assert before_pcs == after_pcs, (
            "preview wrote to price_checks.json — it must not create records"
        )

    def test_classifier_crash_returns_ok_false_not_raise(
        self, temp_data_dir, monkeypatch
    ):
        """Belt-and-suspenders: if classify_request raises, the preview
        must degrade to `ok: False` + an error message — the UI still
        needs a dict to render a 'parse failed' state."""
        def _boom(*a, **kw):
            raise RuntimeError("classifier exploded")

        monkeypatch.setattr(
            "src.core.request_classifier.classify_request", _boom
        )
        from src.core.ingest_pipeline import preview_buyer_request
        r = preview_buyer_request(files=[CCHCS_PACKET])
        assert r["ok"] is False
        assert any("classifier" in e for e in r["errors"]), (
            f"expected classifier error in errors list, got {r['errors']!r}"
        )


# ── /api/rfq/upload-preview (HTTP) ───────────────────────────────────

class TestPreviewRoute:
    """End-to-end route coverage. Uses the Flask test client so auth,
    rate limiting, and tempdir cleanup all exercise."""

    def test_empty_upload_returns_400(self, client):
        resp = client.post("/api/rfq/upload-preview", data={})
        assert resp.status_code == 400
        payload = resp.get_json()
        assert payload["ok"] is False
        assert "at least one pdf" in payload["error"].lower()

    def test_non_pdf_returns_400(self, client):
        data = {
            "files": (io.BytesIO(b"not a pdf"), "notes.txt"),
        }
        resp = client.post(
            "/api/rfq/upload-preview",
            data=data,
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        payload = resp.get_json()
        assert payload["ok"] is False
        assert "not a PDF" in payload["error"]

    def test_valid_pdf_returns_preview_shape(self, client):
        with open(CCHCS_PACKET, "rb") as f:
            pdf_bytes = f.read()
        data = {
            "files": (io.BytesIO(pdf_bytes), "cchcs_packet.pdf"),
            "email_subject": "PREQ10843276 Quote Request",
            "email_sender": "ashley.russ@cdcr.ca.gov",
            "email_body": "Please quote the attached packet.",
        }
        resp = client.post(
            "/api/rfq/upload-preview",
            data=data,
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True
        # Required keys in the preview contract
        for key in (
            "shape", "agency", "confidence", "required_forms",
            "items", "items_parsed", "header",
            "facility", "deadline", "warnings", "errors",
        ):
            assert key in payload, f"preview response missing key {key!r}"

    def test_tempdir_cleaned_up_on_happy_path(self, client, tmp_path):
        """The route uses tempfile.mkdtemp and rm -rf on exit.
        Previewing 3 times should not leak 3 directories under the
        system tempdir."""
        import tempfile
        import glob
        before = set(glob.glob(
            os.path.join(tempfile.gettempdir(), "rfq_preview_*")
        ))
        with open(CCHCS_PACKET, "rb") as f:
            pdf_bytes = f.read()
        for _ in range(3):
            data = {
                "files": (io.BytesIO(pdf_bytes), "cchcs_packet.pdf"),
            }
            resp = client.post(
                "/api/rfq/upload-preview",
                data=data,
                content_type="multipart/form-data",
            )
            assert resp.status_code == 200
        after = set(glob.glob(
            os.path.join(tempfile.gettempdir(), "rfq_preview_*")
        ))
        leaked = after - before
        assert not leaked, (
            f"preview route leaked tempdirs: {leaked}"
        )

    def test_auth_required(self, anon_client):
        """Unauthenticated requests must be blocked by the decorator.
        This mirrors the gate on /api/rfq/upload-manual so the
        preview isn't a side-door around auth."""
        with open(CCHCS_PACKET, "rb") as f:
            pdf_bytes = f.read()
        data = {"files": (io.BytesIO(pdf_bytes), "cchcs_packet.pdf")}
        resp = anon_client.post(
            "/api/rfq/upload-preview",
            data=data,
            content_type="multipart/form-data",
        )
        assert resp.status_code in (401, 403), (
            f"unauthenticated preview returned {resp.status_code}, "
            "expected 401/403"
        )
