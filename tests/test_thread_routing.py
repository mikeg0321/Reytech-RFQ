"""Contract tests for buyer-reply thread routing (PR-E).

Pins `_route_buyer_reply_to_existing` — the helper that takes an
incoming buyer-reply email matched on Gmail thread_id and folds it
into the existing primary record instead of letting the legacy
create path spawn a spurious sibling.

Key contracts:

  * Append `gmail_message_id` to the existing record's
    `gmail_message_ids` list (idempotent).
  * Append a structured entry to the record's `buyer_replies` list
    capturing from / subject / body excerpt / message_id /
    attachment filenames.
  * For each attachment, write a new `rfq_files` row with
    `category='buyer_reply'` and the same `gmail_message_id`.
  * Append an audit-log entry tagged `buyer-reply-routed`.
  * Refuse to route into an already-dismissed (thread-duplicate)
    record — that would compound dismissals.
  * Refuse to route when the matched record vanished (test/race).
  * Returns True on success, False on refuse/fail.

Q5 doctrine: behavior is gated by feature flag
`email_poller.thread_routing_enabled`. Default True, but operators
can flip OFF without a deploy if anything goes wrong.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest


@pytest.fixture
def helper():
    """Import the helper. Tests below patch every I/O function the
    helper calls (load_rfqs, save_rfqs, _load_price_checks,
    _save_price_checks, save_rfq_file) so no real disk state is touched."""
    from src.api import dashboard
    return dashboard._route_buyer_reply_to_existing


def _make_email(*, gmail_message_id="msg_001", thread_id="thread_X",
                sender="buyer@example.com", subject="Re: Quote please",
                body="My follow-up question on the quote",
                attachments=None):
    return {
        "gmail_message_id": gmail_message_id,
        "gmail_thread_id": thread_id,
        "email_uid": gmail_message_id,
        "sender_email": sender,
        "subject": subject,
        "body": body,
        "attachments": attachments or [],
    }


# ─── Routing into an RFQ ───────────────────────────────────────────────


def test_routes_to_existing_rfq_appends_message_id(helper):
    from src.api import dashboard
    rfqs = {
        "rfq_alpha": {
            "id": "rfq_alpha",
            "status": "draft",
            "email_thread_id": "thread_X",
            "gmail_message_ids": ["msg_orig"],
            "buyer_replies": [],
        }
    }
    saved = {}
    with patch.object(dashboard, "load_rfqs", return_value=rfqs), \
         patch.object(dashboard, "save_rfqs",
                      side_effect=lambda d: saved.update({"v": d})), \
         patch.object(dashboard, "save_rfq_file") as mock_save_file:
        ok = helper("rfq_alpha", "rfq",
                    _make_email(gmail_message_id="msg_reply_1"))

    assert ok is True
    out = saved["v"]["rfq_alpha"]
    assert out["gmail_message_ids"] == ["msg_orig", "msg_reply_1"]
    assert len(out["buyer_replies"]) == 1
    assert out["buyer_replies"][0]["gmail_message_id"] == "msg_reply_1"
    assert out["buyer_replies"][0]["from"] == "buyer@example.com"
    assert out["audit_log"][-1]["action"] == "buyer-reply-routed"
    # No attachments → save_rfq_file not called
    assert mock_save_file.call_count == 0


def test_routes_attachments_with_buyer_reply_category(helper):
    from src.api import dashboard
    rfqs = {
        "rfq_alpha": {
            "id": "rfq_alpha",
            "status": "draft",
            "gmail_message_ids": [],
        }
    }
    email = _make_email(
        gmail_message_id="msg_reply_2",
        attachments=[
            {"filename": "amendment.pdf",
             "content_type": "application/pdf",
             "content": b"%PDF-1.4 fake"},
            {"filename": "addendum.docx",
             "content_type": "application/vnd.docx",
             "content": b"docx-bytes"},
        ],
    )
    with patch.object(dashboard, "load_rfqs", return_value=rfqs), \
         patch.object(dashboard, "save_rfqs"), \
         patch.object(dashboard, "save_rfq_file") as mock_save_file:
        ok = helper("rfq_alpha", "rfq", email)

    assert ok is True
    assert mock_save_file.call_count == 2
    for call in mock_save_file.call_args_list:
        kwargs = call.kwargs
        assert kwargs["category"] == "buyer_reply"
        assert kwargs["gmail_message_id"] == "msg_reply_2"
        assert kwargs["uploaded_by"] == "email_poller.thread_routing"


def test_routing_idempotent_on_message_id(helper):
    """Calling twice with the same message_id should NOT duplicate the
    list entry. (Two new buyer_replies entries are fine — those represent
    two events; the message-graph dedup is the load-bearing one.)"""
    from src.api import dashboard
    rfqs = {
        "rfq_alpha": {
            "id": "rfq_alpha",
            "gmail_message_ids": ["msg_reply_3"],
        }
    }
    saved = {}
    with patch.object(dashboard, "load_rfqs",
                      return_value=rfqs), \
         patch.object(dashboard, "save_rfqs",
                      side_effect=lambda d: saved.update({"v": d})), \
         patch.object(dashboard, "save_rfq_file"):
        ok = helper("rfq_alpha", "rfq",
                    _make_email(gmail_message_id="msg_reply_3"))

    assert ok is True
    # Length still 1 — already in list
    assert saved["v"]["rfq_alpha"]["gmail_message_ids"] == ["msg_reply_3"]


# ─── Routing into a PC ─────────────────────────────────────────────────


def test_routes_to_existing_pc(helper):
    from src.api import dashboard
    pcs = {
        "pc_beta": {
            "id": "pc_beta",
            "status": "draft",
            "gmail_message_ids": [],
        }
    }
    saved = {}
    with patch.object(dashboard, "_load_price_checks",
                      return_value=pcs), \
         patch.object(dashboard, "_save_price_checks",
                      side_effect=lambda d: saved.update({"v": d})), \
         patch.object(dashboard, "save_rfq_file"):
        ok = helper("pc_beta", "pc",
                    _make_email(gmail_message_id="msg_pcreply"))

    assert ok is True
    assert saved["v"]["pc_beta"]["gmail_message_ids"] == ["msg_pcreply"]


# ─── Refusal cases ─────────────────────────────────────────────────────


def test_refuses_to_route_into_dismissed_record(helper):
    """If the matched record is itself a thread-duplicate of another,
    routing here would compound dismissals. Bail and let the dedup
    logger fall through to the legacy create path (which the existing
    sol-number dedup still has a chance of catching)."""
    from src.api import dashboard
    rfqs = {
        "rfq_dismissed": {
            "id": "rfq_dismissed",
            "gmail_thread_duplicate_of": "rfq_primary",
        }
    }
    with patch.object(dashboard, "load_rfqs", return_value=rfqs), \
         patch.object(dashboard, "save_rfqs") as mock_save, \
         patch.object(dashboard, "save_rfq_file") as mock_save_file:
        ok = helper("rfq_dismissed", "rfq", _make_email())

    assert ok is False
    mock_save.assert_not_called()
    mock_save_file.assert_not_called()


def test_refuses_to_route_when_record_vanished(helper):
    from src.api import dashboard
    with patch.object(dashboard, "load_rfqs", return_value={}), \
         patch.object(dashboard, "save_rfqs") as mock_save:
        ok = helper("rfq_missing", "rfq", _make_email())

    assert ok is False
    mock_save.assert_not_called()


def test_returns_false_on_unexpected_exception(helper):
    """save_rfqs raising should be swallowed — the helper never
    propagates a save failure into the email-poller (which would
    block the legacy fallback path too)."""
    from src.api import dashboard
    rfqs = {"rfq_alpha": {"id": "rfq_alpha", "gmail_message_ids": []}}
    with patch.object(dashboard, "load_rfqs", return_value=rfqs), \
         patch.object(dashboard, "save_rfqs",
                      side_effect=RuntimeError("disk full")), \
         patch.object(dashboard, "save_rfq_file"):
        ok = helper("rfq_alpha", "rfq", _make_email())

    assert ok is False
