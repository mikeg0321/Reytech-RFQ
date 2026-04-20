"""Tests for the unified Contract Builder upload route + classifier.

Covers:
  - Classifier behavior (filename → slot, image → email, fingerprint fallback)
  - POST /api/rfq/<rid>/contract-upload fans files into the right slots
  - Email screenshot gets stamped filename + r["email_screenshot"] flag
  - 704B-fingerprinted PDF lands in r["templates"]["704b"]
  - Unknown PDFs go to attachments, not templates
  - 404 / empty-upload guards

Shape mirrors tests/test_manual_submit_704b.py — same temp-dir patch trick
because the route imports UPLOAD_DIR at module load time.
"""
from __future__ import annotations

import io
import os

import pytest


@pytest.fixture
def patched_upload_dir(temp_data_dir, monkeypatch):
    """Point routes_rfq_gen.UPLOAD_DIR at the per-test temp dir. The
    conftest patches dashboard.UPLOAD_DIR, but routes_rfq_gen captured
    its own reference via `from src.core.paths import UPLOAD_DIR` at
    import time, so we patch that reference directly."""
    up = os.path.join(temp_data_dir, "uploads")
    os.makedirs(up, exist_ok=True)
    import importlib
    gen_mod = importlib.import_module("src.api.modules.routes_rfq_gen")
    monkeypatch.setattr(gen_mod, "UPLOAD_DIR", up, raising=True)
    return up


def _blank_pdf_bytes(pages: int = 1) -> bytes:
    """Smallest PDF pypdf will accept."""
    from pypdf import PdfWriter
    w = PdfWriter()
    for _ in range(pages):
        w.add_blank_page(width=612, height=792)
    buf = io.BytesIO()
    w.write(buf)
    return buf.getvalue()


def _pdf_with_fields(field_names: list[str]) -> bytes:
    """Build a 1-page PDF containing an AcroForm with the given field names.
    Used to exercise fingerprint classification when the filename is
    ambiguous."""
    import io
    from pypdf import PdfWriter
    from pypdf.generic import (
        DictionaryObject, NameObject, TextStringObject, ArrayObject,
        NumberObject, BooleanObject,
    )
    w = PdfWriter()
    w.add_blank_page(width=612, height=792)
    page = w.pages[0]
    annots = ArrayObject()
    for i, name in enumerate(field_names):
        field = DictionaryObject()
        field[NameObject("/T")] = TextStringObject(name)
        field[NameObject("/FT")] = NameObject("/Tx")
        field[NameObject("/V")] = TextStringObject("")
        field[NameObject("/Rect")] = ArrayObject(
            [NumberObject(72), NumberObject(700 - i * 20),
             NumberObject(200), NumberObject(720 - i * 20)])
        field[NameObject("/Type")] = NameObject("/Annot")
        field[NameObject("/Subtype")] = NameObject("/Widget")
        field[NameObject("/P")] = page.indirect_reference
        ref = w._add_object(field)
        annots.append(ref)
    page[NameObject("/Annots")] = annots
    # Attach AcroForm dict to the document root so pypdf's get_fields() finds it.
    acro = DictionaryObject()
    acro[NameObject("/Fields")] = ArrayObject(
        [page[NameObject("/Annots")][i] for i in range(len(field_names))])
    acro[NameObject("/NeedAppearances")] = BooleanObject(True)
    w._root_object[NameObject("/AcroForm")] = acro
    buf = io.BytesIO()
    w.write(buf)
    return buf.getvalue()


def _png_bytes() -> bytes:
    """Tiny valid 1x1 PNG."""
    return (
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
        b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xff\xff?\x00\x05\xfe\x02\xfe\xdc\xccY\xe7"
        b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )


# ─────────────────────────────────────────────────────────────────
# Classifier unit tests
# ─────────────────────────────────────────────────────────────────

class TestClassifier:

    def test_png_routes_to_email_screenshot(self):
        from src.forms.form_classifier import classify
        d = classify("buyer_email.png", _png_bytes())
        assert d["kind"] == "email_screenshot"
        assert d["category"] == "email_screenshot"

    def test_jpg_routes_to_email_screenshot(self):
        from src.forms.form_classifier import classify
        d = classify("Screenshot 2026-04-20.jpg", b"\xff\xd8\xff\xe0fake")
        assert d["kind"] == "email_screenshot"

    def test_704b_filename_routes_to_template(self):
        from src.forms.form_classifier import classify
        d = classify(
            "AMS_704B_-_CCHCS_Acquisition_Quote_Worksheet.pdf",
            _blank_pdf_bytes(),
        )
        assert d["kind"] == "template"
        assert d["slot"] == "704b"

    def test_703b_filename_routes_to_template(self):
        from src.forms.form_classifier import classify
        d = classify("703B_RFQ_Informal.pdf", _blank_pdf_bytes())
        assert d["kind"] == "template"
        assert d["slot"] == "703b"

    def test_unknown_pdf_routes_to_attachment(self):
        from src.forms.form_classifier import classify
        d = classify("some_contract_addendum.pdf", _blank_pdf_bytes())
        assert d["kind"] == "attachment"
        assert d["slot"] is None

    def test_non_pdf_non_image_routes_to_attachment(self):
        from src.forms.form_classifier import classify
        d = classify("terms.txt", b"plain text content")
        assert d["kind"] == "attachment"

    def test_fingerprint_catches_703b_without_filename_hint(self):
        """A 703B whose filename was stripped still gets classified via
        form-field prefix."""
        from src.forms.form_classifier import classify
        pdf = _pdf_with_fields(["703B_Business Name", "703B_Address",
                                 "703B_Signature"])
        d = classify("attachment1.pdf", pdf)
        assert d["kind"] == "template"
        assert d["slot"] == "703b"


# ─────────────────────────────────────────────────────────────────
# Route integration tests
# ─────────────────────────────────────────────────────────────────

class TestContractUploadRoute:

    def test_missing_rfq_returns_404(self, auth_client):
        resp = auth_client.post(
            "/api/rfq/does-not-exist/contract-upload",
            data={"files": (io.BytesIO(_blank_pdf_bytes()), "x.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 404

    def test_no_files_returns_400(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400
        assert resp.get_json()["ok"] is False

    def test_704b_lands_in_templates_slot(self, auth_client, seed_rfq,
                                          patched_upload_dir, app):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={"files": (io.BytesIO(_blank_pdf_bytes()),
                             "AMS_704B_Worksheet.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["ok"] is True
        assert body["counts"]["template"] == 1
        assert "704b" in body["templates"]
        # Persisted on the RFQ record
        with app.app_context():
            from src.api.data_layer import load_rfqs
            r = load_rfqs()[rid]
        assert "704b" in (r.get("templates") or {})
        assert os.path.exists(r["templates"]["704b"])

    def test_image_lands_in_email_screenshot_slot(self, auth_client, seed_rfq,
                                                   patched_upload_dir, app):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={"files": (io.BytesIO(_png_bytes()), "inbox_screenshot.png")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["counts"]["email_screenshot"] == 1
        with app.app_context():
            from src.api.data_layer import load_rfqs
            r = load_rfqs()[rid]
        es = r.get("email_screenshot") or {}
        assert es.get("original_filename") == "inbox_screenshot.png"
        assert es.get("path") and os.path.exists(es["path"])

    def test_unknown_pdf_lands_in_attachments(self, auth_client, seed_rfq,
                                               patched_upload_dir, app):
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={"files": (io.BytesIO(_blank_pdf_bytes()),
                             "random_memo.pdf")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["counts"]["attachment"] == 1
        assert body["counts"]["template"] == 0
        with app.app_context():
            from src.api.data_layer import load_rfqs
            r = load_rfqs()[rid]
        atts = r.get("attachments") or []
        assert len(atts) == 1
        assert atts[0]["filename"].endswith(".pdf")

    def test_mixed_upload_routes_each_file(self, auth_client, seed_rfq,
                                            patched_upload_dir, app):
        """One call with an image + a 704B + junk → three distinct slots."""
        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={
                "files": [
                    (io.BytesIO(_png_bytes()), "email.png"),
                    (io.BytesIO(_blank_pdf_bytes()), "AMS_704B.pdf"),
                    (io.BytesIO(_blank_pdf_bytes()), "random.pdf"),
                ]
            },
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["counts"]["email_screenshot"] == 1
        assert body["counts"]["template"] == 1
        assert body["counts"]["attachment"] == 1


# ─────────────────────────────────────────────────────────────────
# Auto-extract-on-upload regression (R26Q36, 2026-04-20)
#
# Before: email screenshot landed in r["email_screenshot"] and
# extraction never ran — operator had to click "Re-extract".
# After: vision OCR + requirement_extractor run in a background
# thread right after the save, persisting body_text/subject/
# requirements_json.
# ─────────────────────────────────────────────────────────────────

class TestAutoExtractOnUpload:

    def _patch_async_to_sync(self, monkeypatch):
        """Replace threading.Thread with a sync stub for the duration of
        the test so the auto-extract helper completes before we inspect
        state. We patch by setting the `threading` attribute on the
        helper's module directly (resolved via importlib because
        src.api.modules has no __init__.py at import time)."""
        import importlib
        gen_mod = importlib.import_module("src.api.modules.routes_rfq_gen")

        class _SyncThread:
            def __init__(self, target=None, name=None, daemon=None, **_k):
                self._target = target
            def start(self):
                if self._target:
                    self._target()

        import threading as _real_threading
        # Replace just the Thread class, not the whole module
        monkeypatch.setattr(_real_threading, "Thread", _SyncThread)

    def test_email_screenshot_triggers_auto_extract(
        self, auth_client, seed_rfq, patched_upload_dir, app, monkeypatch,
    ):
        """Uploading an email screenshot should populate body_text +
        requirements_json without a second click."""
        self._patch_async_to_sync(monkeypatch)

        class _FakeReq:
            has_requirements = True
            extraction_method = "claude"
            confidence = 0.9
            forms_required = ["703b", "704b"]
            due_date = "2026-05-01"
            def to_dict(self):
                return {
                    "forms_required": self.forms_required,
                    "due_date": self.due_date,
                    "extraction_method": self.extraction_method,
                }

        def _fake_ocr(path):
            return {
                "subject": "RFQ R26Q36 — Gauze pricing needed by Friday",
                "sender_name": "Lucy Buyer",
                "sender_email": "lucy@cchcs.ca.gov",
                "body_text": "Please quote the attached 704B. Forms 703B and 704B required. Due 2026-05-01.",
                "solicitation_number": "R26Q0036",
            }

        import src.forms.vision_parser as vp
        import src.agents.requirement_extractor as re_mod
        monkeypatch.setattr(vp, "extract_email_from_screenshot", _fake_ocr)
        monkeypatch.setattr(re_mod, "extract_requirements",
                            lambda body, subj, atts: _FakeReq())

        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={"files": (io.BytesIO(_png_bytes()), "buyer_email.png")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["counts"]["email_screenshot"] == 1
        assert body["results"][0]["auto_extract"] == "queued"

        with app.app_context():
            from src.api.data_layer import load_rfqs
            r = load_rfqs()[rid]

        # OCR fields persisted
        assert "Please quote" in (r.get("body_text") or "")
        assert "R26Q36" in (r.get("email_subject") or "")
        assert (r.get("from_email") or "") == "lucy@cchcs.ca.gov"

        # Requirements persisted as JSON string
        import json as _json
        req_json = _json.loads(r.get("requirements_json") or "{}")
        assert "703b" in req_json.get("forms_required", [])
        assert "704b" in req_json.get("forms_required", [])
        # seed_rfq ships a concrete due_date; helper must NOT overwrite it,
        # only fill in when the RFQ has TBD / empty / None.
        assert r.get("due_date") == "2026-03-15"

    def test_auto_extract_noop_when_ocr_fails(
        self, auth_client, seed_rfq, patched_upload_dir, app, monkeypatch,
    ):
        """If OCR returns None (no API key, bad image) we log and move on —
        the upload must still succeed and the file must still be saved."""
        self._patch_async_to_sync(monkeypatch)

        import src.forms.vision_parser as vp
        monkeypatch.setattr(vp, "extract_email_from_screenshot",
                            lambda path: None)

        rid = seed_rfq
        resp = auth_client.post(
            f"/api/rfq/{rid}/contract-upload",
            data={"files": (io.BytesIO(_png_bytes()), "buyer_email.png")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["counts"]["email_screenshot"] == 1

        with app.app_context():
            from src.api.data_layer import load_rfqs
            r = load_rfqs()[rid]
        # body_text not clobbered, screenshot still saved
        assert "email_screenshot" in r
        assert os.path.exists(r["email_screenshot"]["path"])
