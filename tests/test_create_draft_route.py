"""Integration tests for the draft-preview Send arc routes (PR-B3):

- POST /api/rfq/<id>/create-draft  → creates Gmail draft, double-sig
  pre-flight, returns Gmail web URL.
- POST /api/rfq/<id>/discard-draft → deletes Gmail draft + clears pointer.
- POST /api/rfq/<id>/send-quote    → 410 Gone (deprecated direct-send).
- POST /rfq/<id>/send-email        → 302 to /review-package (deprecated
  direct-send via HTML form).
"""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest


def _seed_rfq_with_pdf(tmp_path, rid="rfq_b3"):
    """Build a sample RFQ + write a fake PDF into output dir so attachments resolve."""
    sol = "B3-DEMO-001"
    outdir = tmp_path / "output" / sol
    outdir.mkdir(parents=True, exist_ok=True)
    pdf = outdir / "704b.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%dummy\n%%EOF")
    rfq = {
        "id": rid,
        "requestor_name": "Keith Alsing",
        "requestor_email": "keith@calvet.ca.gov",
        "agency": "CalVet",
        "department": "CalVet - Yountville",
        "solicitation_number": sol,
        "rfq_number": sol,
        "email_subject": "Bid request",
        "email_message_id": "<msg-1@x.gov>",
        "email_thread_id": "thr_abc",
        "status": "priced",
        "items": [{"item_number":"1","qty":1,"description":"x"}],
        "line_items": [{"item_number":"1","qty":1,"description":"x"}],
        "output_files": ["704b.pdf"],
    }
    return rfq, str(pdf)


class TestCreateDraft:

    def test_returns_400_when_no_attachments(self, auth_client, seed_rfq):
        with patch("src.core.gmail_api.is_configured", return_value=True):
            r = auth_client.post(f"/api/rfq/{seed_rfq}/create-draft")
        # Sample RFQ has no generated PDFs, so attachments resolve empty
        assert r.status_code == 400
        assert "package" in r.get_json()["error"].lower()

    def test_returns_400_when_gmail_unconfigured(self, auth_client, seed_rfq):
        with patch("src.core.gmail_api.is_configured", return_value=False):
            r = auth_client.post(f"/api/rfq/{seed_rfq}/create-draft")
        assert r.status_code == 400

    def test_returns_404_for_unknown_rfq(self, auth_client):
        r = auth_client.post("/api/rfq/no-such-rfq/create-draft")
        assert r.status_code == 404

    def test_blocks_on_double_sig_issues(self, auth_client, seed_rfq, temp_data_dir):
        """When the pre-flight scanner finds issues, return 422 with details."""
        rid = seed_rfq
        # Make resolve_attachments return something so we get past the empty check
        fake_atts = ["/tmp/fake.pdf"]
        fake_issues = [{
            "form_id": "704b", "filename": "fake.pdf", "page": 1,
            "kind": "acroform_plus_overlay",
            "detail": "/Sig field present AND overlay name drawn in lower band",
        }]
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.api.draft_builder.resolve_attachments",
                   return_value=fake_atts), \
             patch("src.forms.double_sig_scanner.scan_package_for_double_sigs",
                   return_value=fake_issues):
            r = auth_client.post(f"/api/rfq/{rid}/create-draft")
        assert r.status_code == 422
        body = r.get_json()
        assert body["ok"] is False
        assert body["double_sig_issues"] == fake_issues

    def test_creates_draft_when_clean(self, auth_client, seed_rfq):
        """Happy path — pre-flight clean → save_draft called → response wired."""
        rid = seed_rfq
        # Patch Gmail + the attachment lookup so we don't need real files
        fake_atts = ["/tmp/fake.pdf"]
        fake_save = MagicMock(return_value={
            "id": "draft_abc",
            "message": {"id": "msg_xyz", "threadId": "thr_abc",
                        "labelIds": ["INBOX"]},
        })
        # The labels list call (for agency labels) — return empty so we skip
        labels_resp = {"labels": []}
        fake_service = MagicMock()
        fake_service.users().labels().list().execute.return_value = labels_resp
        # drafts().delete is called for prior cleanup (none on first run)
        fake_service.users().drafts().delete().execute.return_value = {}

        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_send_service",
                   return_value=fake_service), \
             patch("src.core.gmail_api.save_draft", new=fake_save), \
             patch("src.api.draft_builder.resolve_attachments",
                   return_value=fake_atts), \
             patch("src.forms.double_sig_scanner.scan_package_for_double_sigs",
                   return_value=[]):
            r = auth_client.post(f"/api/rfq/{rid}/create-draft")
        assert r.status_code == 200, r.get_data(as_text=True)
        body = r.get_json()
        assert body["ok"] is True
        assert body["draft_id"] == "draft_abc"
        assert body["gmail_draft_url"] == "https://mail.google.com/mail/u/0/#drafts/msg_xyz"
        assert "fake.pdf" in body["attachments"]
        assert fake_save.called


class TestDiscardDraft:

    def test_clears_pointer_when_no_draft(self, auth_client, seed_rfq):
        # Sample RFQ has no draft id — the call should still 200 cleanly
        r = auth_client.post(f"/api/rfq/{seed_rfq}/discard-draft")
        assert r.status_code == 200
        assert r.get_json()["ok"] is True


class TestDeprecatedDirectSend:

    def test_send_quote_returns_410(self, auth_client, seed_rfq):
        # Hard-block direct-send. 410 Gone is the explicit signal.
        r = auth_client.post(f"/api/rfq/{seed_rfq}/send-quote",
                             data=json.dumps({"to": "x@y.gov"}),
                             content_type="application/json")
        assert r.status_code == 410
        body = r.get_json()
        assert body["deprecated"] is True
        assert "review-package" in body["redirect"]

    def test_send_email_redirects_to_review_package(self, auth_client, seed_rfq):
        # HTML-form variant — redirect rather than 410 so the browser lands
        # somewhere usable instead of seeing a raw error page.
        r = auth_client.post(f"/rfq/{seed_rfq}/send-email",
                             data={"to": "x@y.gov", "subject": "s", "body": "b"})
        assert r.status_code == 302
        assert "review-package" in (r.headers.get("Location") or "")
