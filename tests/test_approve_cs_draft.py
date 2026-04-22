"""Tests for /api/email/approve-cs (api_approve_cs_draft).

OB-1 regression: the original implementation wrote to an undefined variable
`outbox_path` after SMTP send succeeded, which raised NameError. The outer
try/except swallowed it and returned `ok: False`, so the operator clicked
"Send Reply" again and the SMTP send fired a second time. 261 stalled CS
drafts were observed in prod on 2026-04-21.

Critical invariants these tests pin:
  1. Once SMTP send succeeds, the route MUST NOT return ok=False under any
     downstream failure mode — that's what triggered the duplicate-send.
  2. A draft already in status='sent' returns 409, never re-sends.
  3. Post-send persist failure returns ok=True with a `warning` field so the
     operator sees something went wrong without triggering a retry.
  4. The literal identifier `outbox_path` must never re-appear in the
     function body — it was the NameError source.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


ROOT = Path(__file__).resolve().parents[1]
ROUTES_CRM = ROOT / "src" / "api" / "modules" / "routes_crm.py"


# ─── Static regression guards ────────────────────────────────────────────────


class TestRegressionStatic:
    def test_outbox_path_identifier_never_reappears(self):
        """OB-1 root cause was `with open(outbox_path, "w")` where outbox_path
        was never defined. This guard makes the NameError un-reintroducible."""
        import re
        src = ROUTES_CRM.read_text(encoding="utf-8")
        # Scope to the approve function body only so we don't false-flag
        # other functions that may have locally-defined variables like
        # `outbox_path_local`.
        start = src.find("def api_approve_cs_draft")
        assert start != -1
        end = src.find("\n@bp.route", start + 1)
        body = src[start:end] if end != -1 else src[start:]
        # Word-boundary match: `outbox_path` but NOT `outbox_path_local`.
        assert re.search(r"\boutbox_path\b", body) is None, (
            "OB-1 regression: `outbox_path` is an undefined identifier that "
            "would raise NameError after SMTP send succeeds, causing "
            "duplicate-send on operator retry."
        )

    def test_uses_dal_update_outbox_status(self):
        """Approve path must persist via the DAL, not a raw file write."""
        src = ROUTES_CRM.read_text(encoding="utf-8")
        assert "update_outbox_status" in src

    def test_no_json_dump_to_outbox_file(self):
        """No raw JSON persistence should remain in the approve path."""
        src = ROUTES_CRM.read_text(encoding="utf-8")
        # Scope: just the approve function body
        start = src.find("def api_approve_cs_draft")
        assert start != -1
        end = src.find("\n@bp.route", start + 1)
        body = src[start:end] if end != -1 else src[start:]
        assert "json.dump" not in body


# ─── Live behavior via Flask test client ─────────────────────────────────────


@pytest.fixture
def _patched_approve(monkeypatch):
    """Stub the three collaborators so we can exercise the route in isolation.

    Returns a tuple (sender_mock, update_status_mock, set_outbox) where:
      - sender_mock is the EmailSender instance mock (inspect .send calls)
      - update_status_mock is the update_outbox_status mock
      - set_outbox(list) replaces the fake get_outbox return value
    """
    outbox_store = {"rows": []}

    def fake_get_outbox(*a, **kw):
        return list(outbox_store["rows"])

    sender_instance = MagicMock()

    class _FakeEmailSender:
        def __init__(self, cfg):
            pass

        def send(self, payload):
            return sender_instance.send(payload)

    update_status_mock = MagicMock(return_value=True)

    monkeypatch.setattr("src.core.dal.get_outbox", fake_get_outbox)
    monkeypatch.setattr(
        "src.core.dal.update_outbox_status", update_status_mock
    )
    monkeypatch.setattr(
        "src.agents.email_poller.EmailSender", _FakeEmailSender
    )
    # notify_agent.log_email_event is best-effort — neuter it so noise in
    # that collaborator can't mask a real issue in the tested code path.
    monkeypatch.setattr(
        "src.agents.notify_agent.log_email_event",
        lambda **kw: None,
    )
    # The approve route refuses to send if GMAIL_ADDRESS / GMAIL_PASSWORD
    # are missing — set them so tests exercise the send/persist path.
    monkeypatch.setenv("GMAIL_ADDRESS", "tester@example.com")
    monkeypatch.setenv("GMAIL_PASSWORD", "app-password")

    def _set_outbox(rows):
        outbox_store["rows"] = rows

    return sender_instance, update_status_mock, _set_outbox


class TestApproveCSDraftRoute:
    def test_success_marks_sent_via_dal(self, auth_client, _patched_approve):
        sender, update_status, set_outbox = _patched_approve
        set_outbox([{
            "id": "draft-ok-1",
            "to": "buyer@example.gov",
            "subject": "Re: Quote",
            "body": "thanks, here is the quote",
            "status": "cs_draft",
            "intent": "reply",
        }])

        resp = auth_client.post(
            "/api/email/approve-cs",
            json={"draft_id": "draft-ok-1"},
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True
        assert payload["sent_to"] == "buyer@example.gov"

        sender.send.assert_called_once()
        update_status.assert_called_once()
        args, kwargs = update_status.call_args
        assert args[0] == "draft-ok-1"
        assert args[1] == "sent"
        assert "sent_at" in kwargs

    def test_missing_draft_returns_404(self, auth_client, _patched_approve):
        sender, update_status, set_outbox = _patched_approve
        set_outbox([])

        resp = auth_client.post(
            "/api/email/approve-cs",
            json={"draft_id": "does-not-exist"},
        )
        assert resp.status_code == 404
        assert resp.get_json()["ok"] is False
        sender.send.assert_not_called()
        update_status.assert_not_called()

    def test_already_sent_returns_409_no_resend(
        self, auth_client, _patched_approve
    ):
        """OB-1 defense-in-depth: even if the operator clicks twice, the
        second call must never re-send."""
        sender, update_status, set_outbox = _patched_approve
        set_outbox([{
            "id": "draft-already-sent",
            "to": "buyer@example.gov",
            "subject": "Re: Quote",
            "body": "body",
            "status": "sent",
            "intent": "reply",
        }])

        resp = auth_client.post(
            "/api/email/approve-cs",
            json={"draft_id": "draft-already-sent"},
        )
        assert resp.status_code == 409
        payload = resp.get_json()
        assert payload["ok"] is False
        assert payload.get("already_sent") is True
        sender.send.assert_not_called()
        update_status.assert_not_called()

    def test_send_failure_does_not_persist(
        self, auth_client, _patched_approve
    ):
        """If SMTP raises, the draft is NOT marked sent — safe to retry."""
        sender, update_status, set_outbox = _patched_approve
        set_outbox([{
            "id": "draft-send-fails",
            "to": "buyer@example.gov",
            "subject": "s",
            "body": "b",
            "status": "cs_draft",
            "intent": "reply",
        }])
        sender.send.side_effect = RuntimeError("SMTP 550 mailbox unavailable")

        resp = auth_client.post(
            "/api/email/approve-cs",
            json={"draft_id": "draft-send-fails"},
        )
        assert resp.status_code == 200  # Flask default for jsonify w/o explicit code
        payload = resp.get_json()
        assert payload["ok"] is False
        assert "SMTP 550" in payload["error"]
        update_status.assert_not_called()

    def test_persist_failure_returns_ok_true_with_warning(
        self, auth_client, _patched_approve
    ):
        """THE OB-1 INVARIANT: send succeeded → never return ok=False, even
        if the DB write fails. Otherwise the operator retries and we
        duplicate-send."""
        sender, update_status, set_outbox = _patched_approve
        set_outbox([{
            "id": "draft-persist-fails",
            "to": "buyer@example.gov",
            "subject": "s",
            "body": "b",
            "status": "cs_draft",
            "intent": "reply",
        }])
        update_status.return_value = False

        resp = auth_client.post(
            "/api/email/approve-cs",
            json={"draft_id": "draft-persist-fails"},
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True, (
            "OB-1 REGRESSION: post-send persist failure must not return "
            "ok=False — that's what caused 261 stalled drafts and the "
            "duplicate-send risk."
        )
        assert "warning" in payload
        sender.send.assert_called_once()  # send did happen

    def test_persist_raises_returns_ok_true_with_warning(
        self, auth_client, _patched_approve
    ):
        """Same invariant, exception path."""
        sender, update_status, set_outbox = _patched_approve
        set_outbox([{
            "id": "draft-persist-raises",
            "to": "buyer@example.gov",
            "subject": "s",
            "body": "b",
            "status": "cs_draft",
            "intent": "reply",
        }])
        update_status.side_effect = RuntimeError("db locked")

        resp = auth_client.post(
            "/api/email/approve-cs",
            json={"draft_id": "draft-persist-raises"},
        )
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["ok"] is True
        assert "warning" in payload
        assert "db locked" in payload["warning"]

    def test_empty_draft_id_returns_error_before_touching_anything(
        self, auth_client, _patched_approve
    ):
        sender, update_status, _ = _patched_approve
        resp = auth_client.post("/api/email/approve-cs", json={})
        payload = resp.get_json()
        assert payload["ok"] is False
        assert "draft_id" in payload["error"]
        sender.send.assert_not_called()
        update_status.assert_not_called()
