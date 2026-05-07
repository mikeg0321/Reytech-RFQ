"""Pin transient-error retry on `gmail_api.get_message_metadata` (2026-05-07).

This is the same failure class as B-3 (PR #823): IncompleteRead and
[SSL] record layer failures during transit, before the server signal
ever returns. PR #823 wrapped `list_message_ids` and `get_raw_message`.
This pins the third surface — `get_message_metadata`, used by the
email_poller, observed_send detector, registration_gap_detector, and
two admin endpoints. A transient skip here = a missed inbound RFQ.

The retry is read-only and idempotent; no risk of duplicate side
effects. Send paths (send_message / save_draft) deliberately stay
NON-retried — replaying a write at the transport layer can
double-send.
"""
from __future__ import annotations

from unittest.mock import MagicMock


def test_get_message_metadata_retries_transient(monkeypatch):
    """A single IncompleteRead during `.execute()` retries and the
    second attempt succeeds, so the caller still gets headers."""
    from src.core import gmail_api
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    call_count = {"n": 0}

    def execute():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise OSError("IncompleteRead while reading from server")
        return {
            "threadId": "thread_X",
            "payload": {"headers": [
                {"name": "Subject", "value": "Re: Quote"},
                {"name": "From", "value": "buyer@example.com"},
                {"name": "Date", "value": "Wed, 7 May 2026 10:00:00 -0700"},
                {"name": "Message-ID", "value": "<msg_001>"},
                {"name": "To", "value": "sales@reytechinc.com"},
            ]},
        }

    request = MagicMock()
    request.execute = execute
    service = MagicMock()
    service.users().messages().get.return_value = request

    meta = gmail_api.get_message_metadata(service, "msg_001")
    assert call_count["n"] == 2  # one transient retry, then success
    assert meta["subject"] == "Re: Quote"
    assert meta["from"] == "buyer@example.com"
    assert meta["thread_id"] == "thread_X"
    assert meta["gmail_id"] == "msg_001"


def test_get_message_metadata_returns_stub_on_permanent_failure(
        monkeypatch):
    """Pre-existing behavior preserved: if the call ultimately fails
    (non-transient OR exceeds retry budget), we log + return a stub
    {gmail_id: ...} dict instead of raising."""
    from src.core import gmail_api
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    request = MagicMock()
    request.execute = MagicMock(side_effect=ValueError("403 Forbidden"))
    service = MagicMock()
    service.users().messages().get.return_value = request

    meta = gmail_api.get_message_metadata(service, "msg_999")
    assert meta == {"gmail_id": "msg_999"}
    # Non-transient = single attempt only
    assert request.execute.call_count == 1
