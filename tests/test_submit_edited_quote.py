"""Tests for Item Z (release valve): operator-edited Quote PDF upload.

Ships the `/api/rfq/<rid>/submit-edited-quote` endpoint that lets the
operator replace the auto-generated Quote PDF with a hand-edited copy.
Used when a field-level bug (wrong ship-to, tax, institution) would
otherwise block submission while a code fix is in flight.

Covered:
  - POST validation (404, empty/invalid PDF, 15MB cap, post-send lock)
  - Success path: file persisted at <SOL>_Quote_Reytech_EDITED.pdf,
    record["edited_quote"] populated, audit log appended
  - Archive on re-edit (prior upload moved to _prev/)
  - DELETE clears the flag (resume auto-gen)
  - Audit log format — fields we rollup to drive resolver priority
"""
from __future__ import annotations

import io
import json
import os

import pytest


@pytest.fixture
def patched_output_dir(temp_data_dir, monkeypatch):
    out = os.path.join(temp_data_dir, "output")
    os.makedirs(out, exist_ok=True)
    import importlib
    gen_mod = importlib.import_module("src.api.modules.routes_rfq_gen")
    monkeypatch.setattr(gen_mod, "OUTPUT_DIR", out, raising=True)
    return out


@pytest.fixture
def patched_data_dir(temp_data_dir, monkeypatch):
    """Redirect audit log writes to the per-test temp dir."""
    import importlib
    gen_mod = importlib.import_module("src.api.modules.routes_rfq_gen")
    monkeypatch.setattr(gen_mod, "DATA_DIR", temp_data_dir, raising=True)
    return temp_data_dir


def _minimal_pdf_bytes() -> bytes:
    from pypdf import PdfWriter
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _read_rfqs(app):
    with app.app_context():
        from src.api.data_layer import load_rfqs
        return load_rfqs()


class TestSubmitEditedQuoteValidation:

    def test_missing_rfq_returns_404(self, auth_client):
        resp = auth_client.post(
            "/api/rfq/does-not-exist/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "edit.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 404

    def test_no_file_returns_400(self, auth_client, seed_rfq):
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        assert "No file" in resp.get_json()["error"]

    def test_non_pdf_extension_rejected(self, auth_client, seed_rfq):
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(b"not a pdf"), "edit.txt")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        assert "PDF" in resp.get_json()["error"]

    def test_empty_file_rejected(self, auth_client, seed_rfq):
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(b""), "edit.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400

    def test_truncated_pdf_rejected(self, auth_client, seed_rfq):
        """<100 bytes is treated as truncated — pypdf can't validate
        meaningful structure below that threshold."""
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(b"x" * 50), "edit.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400

    def test_invalid_pdf_content_rejected(self, auth_client, seed_rfq):
        """Valid extension + >100 bytes but not actually a PDF."""
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(b"x" * 500), "edit.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        assert "valid PDF" in resp.get_json()["error"]

    def test_over_10mb_rejected(self, auth_client, seed_rfq):
        """Z8 per Mike 2026-04-23: 10 MB cap, HTTP 413. Operator compresses
        in Acrobat before re-upload."""
        oversized = _minimal_pdf_bytes() + b"\x00" * (11 * 1024 * 1024)
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(oversized), "big.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 413
        assert "10 MB" in resp.get_json()["error"]


class TestPostSendLock:
    """Z3 default: once a quote is sent, edits require explicit status revert."""

    def test_edit_refused_when_status_sent(
        self, auth_client, app, seed_rfq, patched_output_dir, patched_data_dir
    ):
        # Flip the record to sent
        with app.app_context():
            from src.api.data_layer import load_rfqs, _save_single_rfq
            rfqs = load_rfqs()
            r = rfqs[seed_rfq]
            r["status"] = "sent"
            _save_single_rfq(seed_rfq, r)

        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "edit.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 409
        body = resp.get_json()
        assert body["ok"] is False
        assert body["status"] == "sent"
        assert "revert" in body["error"].lower()


class TestSubmitEditedQuoteSuccess:

    def test_upload_persists_pdf_at_edited_path(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        pdf = _minimal_pdf_bytes()
        resp = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(pdf), "corrected.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["filename"].endswith("_Quote_Reytech_EDITED.pdf")
        assert body["bytes"] == len(pdf)
        assert body["was_re_edit"] is False

        # File exists on disk at the expected path
        r = _read_rfqs(app)[seed_rfq]
        sol = r.get("solicitation_number", "RFQ")
        expected = os.path.join(patched_output_dir, sol, f"{sol}_Quote_Reytech_EDITED.pdf")
        assert os.path.exists(expected), f"File missing at {expected}"
        assert os.path.getsize(expected) == len(pdf)

    def test_upload_sets_edited_quote_flag(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "edit.pdf")},
            content_type="multipart/form-data",
        )
        r = _read_rfqs(app)[seed_rfq]
        flag = r.get("edited_quote")
        assert flag, "edited_quote flag must be set"
        assert flag.get("original_filename") == "edit.pdf"
        assert flag.get("pages") >= 1
        assert flag.get("uploaded_at")
        assert flag.get("path", "").endswith("_Quote_Reytech_EDITED.pdf")

    def test_re_edit_archives_previous(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        """Second upload moves the first to _prev/ and marks was_re_edit=True."""
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "v1.pdf")},
            content_type="multipart/form-data",
        )
        resp2 = auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "v2.pdf")},
            content_type="multipart/form-data",
        )
        assert resp2.status_code == 200
        body = resp2.get_json()
        assert body["was_re_edit"] is True
        assert body["archived_prev"] is True

        r = _read_rfqs(app)[seed_rfq]
        sol = r.get("solicitation_number", "RFQ")
        prev_dir = os.path.join(patched_output_dir, sol, "_prev")
        assert os.path.isdir(prev_dir)
        archived_files = os.listdir(prev_dir)
        assert any("EDITED" in f for f in archived_files), (
            f"Expected archived edited-quote file in _prev, got {archived_files}"
        )

    def test_upload_appends_to_output_files(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "edit.pdf")},
            content_type="multipart/form-data",
        )
        r = _read_rfqs(app)[seed_rfq]
        assert any(
            "Quote_Reytech_EDITED.pdf" in f for f in r.get("output_files", [])
        ), f"output_files should include edited quote. Got: {r.get('output_files')}"


class TestAuditLog:

    def test_audit_log_appends_on_upload(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        """Every edit is written to quote_edit_audit.jsonl for the 30-day
        rollup that feeds resolver/filler priority tuning."""
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "edit.pdf")},
            content_type="multipart/form-data",
        )
        audit_path = os.path.join(patched_data_dir, "quote_edit_audit.jsonl")
        assert os.path.exists(audit_path), f"audit log missing at {audit_path}"
        with open(audit_path) as f:
            lines = f.readlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["event"] == "quote_edit_uploaded"
        assert entry["rid"] == seed_rfq
        assert entry["original_filename"] == "edit.pdf"
        assert entry["was_re_edit"] is False
        assert "record_snapshot" in entry
        # Snapshot captures the fields we want to measure drift on
        snap = entry["record_snapshot"]
        for field in ("ship_to_name", "tax_rate", "institution", "agency", "status"):
            assert field in snap, f"audit snapshot missing {field}"

    def test_audit_log_marks_re_edit(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "v1.pdf")},
            content_type="multipart/form-data",
        )
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "v2.pdf")},
            content_type="multipart/form-data",
        )
        audit_path = os.path.join(patched_data_dir, "quote_edit_audit.jsonl")
        with open(audit_path) as f:
            lines = f.readlines()
        assert len(lines) == 2
        second = json.loads(lines[1])
        assert second["was_re_edit"] is True


class TestDeleteClearsFlag:

    def test_delete_clears_edited_quote_flag(
        self, auth_client, seed_rfq, app, patched_output_dir, patched_data_dir
    ):
        auth_client.post(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
            data={"file": (io.BytesIO(_minimal_pdf_bytes()), "edit.pdf")},
            content_type="multipart/form-data",
        )
        # Confirm flag set
        r = _read_rfqs(app)[seed_rfq]
        assert r.get("edited_quote")

        # DELETE clears it
        resp = auth_client.delete(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
        )
        assert resp.status_code == 200
        assert resp.get_json()["cleared"] is True

        r2 = _read_rfqs(app)[seed_rfq]
        assert r2.get("edited_quote") is None

    def test_delete_on_missing_flag_returns_cleared_false(
        self, auth_client, seed_rfq, app
    ):
        resp = auth_client.delete(
            f"/api/rfq/{seed_rfq}/submit-edited-quote",
        )
        assert resp.status_code == 200
        assert resp.get_json()["cleared"] is False
