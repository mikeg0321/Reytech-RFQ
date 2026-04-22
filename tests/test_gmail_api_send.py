"""Tests for Gmail API send infrastructure in src/core/gmail_api.py.

All Gmail API calls are mocked — no real SMTP, no real Gmail API network
traffic. These tests must be safe to run in any environment.
"""
import base64
import os
import tempfile
from email import message_from_bytes
from unittest.mock import MagicMock

import pytest

from src.core import gmail_api


# ─── _build_mime_message ────────────────────────────────────────────────


def test_plain_body_only_builds_simple_mime_text():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Quote Q-0321",
        body_plain="Please see the attached quote.",
    )
    raw = msg.as_bytes()
    parsed = message_from_bytes(raw)
    assert parsed["To"] == "buyer@example.gov"
    assert parsed["Subject"] == "Quote Q-0321"
    assert parsed.get_content_type() == "text/plain"
    assert "Please see the attached quote." in parsed.get_payload(decode=True).decode()


def test_html_body_produces_multipart_alternative():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="RFQ reply",
        body_plain="plain version",
        body_html="<p>html version</p>",
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert parsed.get_content_type() == "multipart/alternative"
    subtypes = [p.get_content_type() for p in parsed.walk() if not p.is_multipart()]
    assert "text/plain" in subtypes
    assert "text/html" in subtypes


def test_attachment_produces_multipart_mixed():
    with tempfile.NamedTemporaryFile(
        suffix=".pdf", delete=False, mode="wb"
    ) as f:
        f.write(b"%PDF-1.4 fake quote pdf")
        path = f.name
    try:
        msg = gmail_api._build_mime_message(
            to="buyer@example.gov",
            subject="Quote with attachment",
            body_plain="See attached.",
            attachments=[path],
        )
        parsed = message_from_bytes(msg.as_bytes())
        assert parsed.get_content_type() == "multipart/mixed"
        filenames = [
            p.get_filename()
            for p in parsed.walk()
            if p.get_filename()
        ]
        assert os.path.basename(path) in filenames
    finally:
        os.unlink(path)


def test_missing_attachment_file_is_skipped_with_warning():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Quote",
        body_plain="body",
        attachments=["/nonexistent/path/quote.pdf"],
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert parsed.get_content_type() == "text/plain"


def test_cc_appears_in_header_bcc_does_not():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Quote",
        body_plain="body",
        cc="cc@example.gov, cc2@example.gov",
        bcc="hidden@reytechinc.com",
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert "cc@example.gov" in parsed["Cc"]
    assert "cc2@example.gov" in parsed["Cc"]
    assert parsed.get("Bcc") is None
    assert "hidden@reytechinc.com" not in msg.as_string()


def test_cc_accepts_list_or_csv_string():
    msg_list = gmail_api._build_mime_message(
        to="x@y.gov", subject="s", body_plain="b",
        cc=["a@b.gov", "c@d.gov"],
    )
    msg_csv = gmail_api._build_mime_message(
        to="x@y.gov", subject="s", body_plain="b",
        cc="a@b.gov, c@d.gov",
    )
    assert message_from_bytes(msg_list.as_bytes())["Cc"] == \
           message_from_bytes(msg_csv.as_bytes())["Cc"]


def test_threading_headers_are_set():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Re: Quote Q-0321",
        body_plain="reply body",
        in_reply_to="<CAF=ABC@mail.gmail.com>",
        references="<CAF=AAA@mail.gmail.com> <CAF=ABC@mail.gmail.com>",
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert parsed["In-Reply-To"] == "<CAF=ABC@mail.gmail.com>"
    assert "<CAF=ABC@mail.gmail.com>" in parsed["References"]


def test_references_defaults_to_in_reply_to_when_not_given():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Re: Quote",
        body_plain="reply body",
        in_reply_to="<only-id@mail.gmail.com>",
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert parsed["References"] == "<only-id@mail.gmail.com>"


def test_from_name_and_from_addr_build_from_header():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Quote",
        body_plain="body",
        from_name="Michael Guadan",
        from_addr="sales@reytechinc.com",
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert parsed["From"] == "Michael Guadan <sales@reytechinc.com>"


def test_extra_headers_are_applied():
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Quote",
        body_plain="body",
        extra_headers={"X-Reytech-Quote-Id": "Q-0321"},
    )
    parsed = message_from_bytes(msg.as_bytes())
    assert parsed["X-Reytech-Quote-Id"] == "Q-0321"


def test_empty_to_raises():
    with pytest.raises(ValueError, match="'to' recipient"):
        gmail_api._build_mime_message(
            to="", subject="s", body_plain="b",
        )


def test_empty_subject_raises():
    with pytest.raises(ValueError, match="subject"):
        gmail_api._build_mime_message(
            to="x@y.gov", subject="", body_plain="b",
        )


def test_empty_body_raises():
    with pytest.raises(ValueError, match="body_plain or body_html"):
        gmail_api._build_mime_message(
            to="x@y.gov", subject="s",
        )


def test_caller_supplied_body_is_sent_verbatim_no_signature_added():
    """Enforces CLAUDE.md 'Gmail Handles Signatures' — send_message must not
    append any hardcoded Reytech signature. Only what the caller passes goes
    out; Gmail auto-sig does the rest."""
    body = "Hi, please see the attached quote.\n\nThanks."
    msg = gmail_api._build_mime_message(
        to="buyer@example.gov",
        subject="Quote",
        body_plain=body,
    )
    parsed = message_from_bytes(msg.as_bytes())
    decoded_body = parsed.get_payload(decode=True).decode()
    assert "Best regards" not in decoded_body
    assert "Reytech Inc." not in decoded_body
    assert "30 Carnoustie" not in decoded_body
    assert "SB/DVBE Cert" not in decoded_body
    assert "949-229-1575" not in decoded_body
    assert body == decoded_body


# ─── send_message ───────────────────────────────────────────────────────


def _fake_service(response=None):
    """Build a MagicMock shaped like a googleapiclient Gmail service."""
    service = MagicMock()
    send_call = service.users.return_value.messages.return_value.send
    send_call.return_value.execute.return_value = response or {
        "id": "18f0abc",
        "threadId": "18f0abc",
        "labelIds": ["SENT"],
    }
    return service, send_call


def test_send_message_calls_users_messages_send_with_raw_body():
    service, send_call = _fake_service()
    result = gmail_api.send_message(
        service,
        to="buyer@example.gov",
        subject="Quote Q-0321",
        body_plain="please see attached",
    )
    assert result == {"id": "18f0abc", "threadId": "18f0abc", "labelIds": ["SENT"]}

    send_call.assert_called_once()
    kwargs = send_call.call_args.kwargs
    assert kwargs["userId"] == "me"
    assert "raw" in kwargs["body"]

    decoded = base64.urlsafe_b64decode(kwargs["body"]["raw"])
    parsed = message_from_bytes(decoded)
    assert parsed["To"] == "buyer@example.gov"
    assert parsed["Subject"] == "Quote Q-0321"
    assert "please see attached" in parsed.get_payload(decode=True).decode()


def test_send_message_threads_via_threadId_when_provided():
    service, send_call = _fake_service(
        response={"id": "msg2", "threadId": "thread-1"},
    )
    gmail_api.send_message(
        service,
        to="buyer@example.gov",
        subject="Re: Quote",
        body_plain="reply",
        thread_id="thread-1",
    )
    body = send_call.call_args.kwargs["body"]
    assert body["threadId"] == "thread-1"


def test_send_message_omits_threadId_when_not_provided():
    service, send_call = _fake_service()
    gmail_api.send_message(
        service,
        to="buyer@example.gov",
        subject="New Quote",
        body_plain="body",
    )
    body = send_call.call_args.kwargs["body"]
    assert "threadId" not in body


def test_send_message_propagates_api_errors_not_swallowed():
    """'Never Die Silently' — callers must know when a send failed."""
    service = MagicMock()
    send_call = service.users.return_value.messages.return_value.send
    send_call.return_value.execute.side_effect = RuntimeError("403 insufficient scope")

    with pytest.raises(RuntimeError, match="insufficient scope"):
        gmail_api.send_message(
            service,
            to="buyer@example.gov",
            subject="s",
            body_plain="b",
        )


def test_send_message_rejects_empty_to_before_calling_api():
    service, send_call = _fake_service()
    with pytest.raises(ValueError):
        gmail_api.send_message(
            service, to="", subject="s", body_plain="b",
        )
    send_call.assert_not_called()


def test_send_message_with_attachment_encodes_into_raw_body():
    with tempfile.NamedTemporaryFile(
        suffix=".pdf", delete=False, mode="wb"
    ) as f:
        f.write(b"%PDF-1.4 fake")
        path = f.name
    try:
        service, send_call = _fake_service()
        gmail_api.send_message(
            service,
            to="buyer@example.gov",
            subject="Quote with pdf",
            body_plain="see attached",
            attachments=[path],
        )
        raw = base64.urlsafe_b64decode(
            send_call.call_args.kwargs["body"]["raw"]
        )
        parsed = message_from_bytes(raw)
        assert parsed.get_content_type() == "multipart/mixed"
        filenames = [
            p.get_filename() for p in parsed.walk() if p.get_filename()
        ]
        assert os.path.basename(path) in filenames
    finally:
        os.unlink(path)


# ─── get_send_service ───────────────────────────────────────────────────


def test_get_send_service_raises_when_not_configured(monkeypatch):
    monkeypatch.setattr(gmail_api, "GMAIL_OAUTH_CLIENT_ID", "")
    monkeypatch.setattr(gmail_api, "GMAIL_OAUTH_CLIENT_SECRET", "")
    monkeypatch.setattr(gmail_api, "GMAIL_OAUTH_REFRESH_TOKEN", "")
    with pytest.raises(RuntimeError, match="not configured"):
        gmail_api.get_send_service()


# ─── scope configuration ────────────────────────────────────────────────


def test_gmail_send_scope_is_registered():
    """Without gmail.send in SCOPES the OAuth setup script won't request it,
    and every send_message call would 403 forever."""
    assert any("gmail.send" in s for s in gmail_api.SCOPES)
