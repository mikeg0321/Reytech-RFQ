"""Contract tests for the buyer-reply diff extraction endpoint
(PR-F2 of post-quote queue item 24, 2026-05-07).

Pins:
  * Endpoint requires auth.
  * 404 when RFQ not found.
  * 404 when buyer_reply index out of range.
  * Helper's skip-reason flows through to the response.
  * Helper's diff dict flows through; reply_meta surfaces from/subject/at.
  * Path traversal in rid blocked.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture
def seed_rfq_with_replies(monkeypatch):
    """Patch load_rfqs to return a record with two buyer replies."""
    rfqs = {
        "rfq_alpha": {
            "id": "rfq_alpha",
            "status": "generated",
            "line_items": [
                {"description": "Widget A", "qty": 10, "unit_price": 5.00,
                 "line_number": 1},
                {"description": "Widget B", "qty": 5, "unit_price": 12.00,
                 "line_number": 2},
            ],
            "buyer_replies": [
                {
                    "at": "2026-05-07T10:00:00",
                    "from": "buyer@example.com",
                    "subject": "Re: Quote",
                    "body_excerpt": "Can you do widget A at $4.50?",
                    "gmail_message_id": "msg_001",
                    "attachments": [],
                },
                {
                    "at": "2026-05-07T11:00:00",
                    "from": "buyer@example.com",
                    "subject": "Re: Quote — qty change",
                    "body_excerpt": "Bump widget B qty to 8.",
                    "gmail_message_id": "msg_002",
                    "attachments": [],
                },
            ],
        }
    }
    monkeypatch.setattr("src.api.data_layer.load_rfqs", lambda: rfqs)
    return rfqs


def test_endpoint_requires_auth(anon_client, seed_rfq_with_replies):
    r = anon_client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r.status_code in (401, 302, 403)


def test_endpoint_returns_404_for_missing_rfq(client, monkeypatch):
    monkeypatch.setattr("src.api.data_layer.load_rfqs", lambda: {})
    r = client.post("/api/rfq/rfq_missing/buyer-replies/0/extract-diff")
    assert r.status_code == 404
    assert r.get_json()["error"] == "RFQ not found"


def test_endpoint_returns_404_for_out_of_range_index(
        client, seed_rfq_with_replies):
    r = client.post("/api/rfq/rfq_alpha/buyer-replies/5/extract-diff")
    assert r.status_code == 404
    assert "out of range" in r.get_json()["error"]


def test_endpoint_blocks_path_traversal_in_rid(client):
    r = client.post("/api/rfq/..%2Fetc/buyer-replies/0/extract-diff")
    # Either flask's strict_slashes routes 404 OR our _validate_rid 400.
    assert r.status_code in (400, 404)


def test_endpoint_passes_skipped_reason_through(
        client, seed_rfq_with_replies, monkeypatch):
    """When the helper skips (e.g. no API key), the reason flows to the
    response so the operator UI surfaces it instead of a generic error."""
    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff",
        lambda text, items, **kw: ({}, "ANTHROPIC_API_KEY not set"),
    )
    r = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["skipped_reason"] == "ANTHROPIC_API_KEY not set"


def test_endpoint_returns_diff_with_reply_meta(
        client, seed_rfq_with_replies, monkeypatch):
    fake_diff = {
        "price_changes": [{"line_no": 1, "requested_unit_price": 4.50,
                           "rationale": "buyer asked for $4.50"}],
        "qty_changes": [],
        "items_added": [],
        "items_removed": [],
        "notes": [],
        "_empty": False,
    }
    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff",
        lambda text, items, **kw: (fake_diff, None),
    )
    r = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["skipped_reason"] is None
    assert body["diff"]["price_changes"][0]["line_no"] == 1
    assert body["reply_meta"]["from"] == "buyer@example.com"
    assert body["reply_meta"]["subject"] == "Re: Quote"
    assert body["reply_meta"]["gmail_message_id"] == "msg_001"


def test_endpoint_passes_current_items_to_helper(
        client, seed_rfq_with_replies, monkeypatch):
    """The helper must receive the record's actual line_items so the
    LLM can resolve `line 1` references back to descriptions."""
    captured = {}

    def fake_extract(text, items, **kw):
        captured["text"] = text
        captured["items"] = items
        captured["totals"] = kw.get("current_totals")
        return ({"price_changes": [], "qty_changes": [], "items_added": [],
                 "items_removed": [], "notes": [], "_empty": True}, None)

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)
    r = client.post("/api/rfq/rfq_alpha/buyer-replies/1/extract-diff")
    assert r.status_code == 200
    assert captured["text"] == "Bump widget B qty to 8."
    assert len(captured["items"]) == 2
    assert captured["items"][1]["description"] == "Widget B"
    assert captured["items"][1]["qty"] == 5
