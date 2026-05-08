"""Pin the buyer-reply diff cache (Tier 3b, audit 2026-05-07).

Closes the audit-named regression: every "Extract changes" click on
`/api/rfq/<rid>/buyer-replies/<idx>/extract-diff` used to call
Claude, regardless of whether the operator had already extracted
the diff. Operator clicking 100x = 100 paid Claude calls.

This module pins:
  1. First call calls Claude, persists the diff onto the reply row.
  2. Second call with same inputs returns the cached diff and
     does NOT call Claude again. Response includes `cached: true`.
  3. `?force=1` query param bypasses the cache.
  4. Editing the reply body invalidates the cache (signature change).
  5. Editing the items list invalidates the cache (signature change).
  6. Skip reasons are also cached — a "no API key set" outcome
     doesn't hammer the (still-down) endpoint 100x in a row.
  7. The signature helper itself is stable for identical inputs and
     varies for any meaningful input change.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest


# ─── Signature helper (pure, no fixture needed) ──────────────────────

def test_signature_stable_for_identical_inputs():
    from src.agents.buyer_reply_diff import compute_diff_cache_signature
    s1 = compute_diff_cache_signature(
        "Buyer asks for a discount.",
        [{"description": "Widget", "qty": 10, "unit_price": 5.0}])
    s2 = compute_diff_cache_signature(
        "Buyer asks for a discount.",
        [{"description": "Widget", "qty": 10, "unit_price": 5.0}])
    assert s1 == s2
    # SHA-256 hex == 64 chars.
    assert len(s1) == 64


def test_signature_changes_when_body_changes():
    from src.agents.buyer_reply_diff import compute_diff_cache_signature
    items = [{"description": "Widget", "qty": 10, "unit_price": 5.0}]
    s1 = compute_diff_cache_signature("body A", items)
    s2 = compute_diff_cache_signature("body B", items)
    assert s1 != s2


def test_signature_changes_when_items_change():
    from src.agents.buyer_reply_diff import compute_diff_cache_signature
    body = "Buyer asks for a discount."
    s1 = compute_diff_cache_signature(
        body, [{"description": "Widget", "qty": 10, "unit_price": 5.0}])
    s2 = compute_diff_cache_signature(
        body, [{"description": "Widget", "qty": 10, "unit_price": 6.0}])
    assert s1 != s2


def test_signature_stable_under_irrelevant_field_changes():
    """Adding fields beyond what `_normalize_items` keeps must NOT
    invalidate the cache — the LLM never saw them."""
    from src.agents.buyer_reply_diff import compute_diff_cache_signature
    body = "x"
    items_minimal = [{"description": "W", "qty": 1, "unit_price": 1.0}]
    items_padded = [{"description": "W", "qty": 1, "unit_price": 1.0,
                     "category": "tools", "supplier": "ACME"}]
    assert (compute_diff_cache_signature(body, items_minimal)
            == compute_diff_cache_signature(body, items_padded))


# ─── Endpoint cache tests ────────────────────────────────────────────

@pytest.fixture
def seed_rfq_with_replies(monkeypatch):
    """Mutable rfq-store fixture so cache writes can be observed by
    re-reading via `load_rfqs()`."""
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
            ],
        }
    }
    # Mutable: cache writes go through `_save_single_rfq` which we
    # stub here to mutate this dict directly.
    monkeypatch.setattr("src.api.data_layer.load_rfqs", lambda: rfqs)

    def fake_save(rid, r, raise_on_error=False):
        rfqs[rid] = r
        return True

    monkeypatch.setattr("src.api.data_layer._save_single_rfq", fake_save)
    return rfqs


def test_first_call_calls_claude_and_persists_diff(
        client, seed_rfq_with_replies, monkeypatch):
    fake_diff = {
        "price_changes": [{"line_no": 1, "requested_unit_price": 4.50,
                           "rationale": "buyer asked for $4.50"}],
        "qty_changes": [], "items_added": [],
        "items_removed": [], "notes": [], "_empty": False,
    }
    call_count = {"n": 0}

    def fake_extract(text, items, **kw):
        call_count["n"] += 1
        return (fake_diff, None)

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)

    r = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["cached"] is False
    assert body["diff"]["price_changes"][0]["line_no"] == 1
    assert call_count["n"] == 1

    # Persisted on the reply row.
    reply = seed_rfq_with_replies["rfq_alpha"]["buyer_replies"][0]
    assert reply["_extracted_diff"] == fake_diff
    assert reply["_extracted_diff_signature"]
    assert reply["_extracted_diff_at"]
    assert reply["_extracted_diff_skipped_reason"] is None


def test_second_call_serves_from_cache_without_claude(
        client, seed_rfq_with_replies, monkeypatch):
    fake_diff = {
        "price_changes": [{"line_no": 1, "requested_unit_price": 4.50,
                           "rationale": "x"}],
        "qty_changes": [], "items_added": [],
        "items_removed": [], "notes": [], "_empty": False,
    }
    call_count = {"n": 0}

    def fake_extract(text, items, **kw):
        call_count["n"] += 1
        return (fake_diff, None)

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)

    # First call seeds the cache.
    client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert call_count["n"] == 1

    # Second call must hit cache.
    r2 = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r2.status_code == 200
    body = r2.get_json()
    assert body["cached"] is True
    assert body["diff"]["price_changes"][0]["line_no"] == 1
    assert call_count["n"] == 1, "second call must NOT re-invoke Claude"


def test_force_query_param_bypasses_cache(
        client, seed_rfq_with_replies, monkeypatch):
    fake_diff = {"price_changes": [], "qty_changes": [],
                 "items_added": [], "items_removed": [],
                 "notes": [], "_empty": True}
    call_count = {"n": 0}

    def fake_extract(text, items, **kw):
        call_count["n"] += 1
        return (fake_diff, None)

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)

    client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert call_count["n"] == 1

    r2 = client.post(
        "/api/rfq/rfq_alpha/buyer-replies/0/extract-diff?force=1")
    body = r2.get_json()
    assert body["cached"] is False
    assert call_count["n"] == 2, "force=1 must re-invoke Claude"


def test_body_change_invalidates_cache(
        client, seed_rfq_with_replies, monkeypatch):
    fake_diff = {"price_changes": [], "qty_changes": [],
                 "items_added": [], "items_removed": [],
                 "notes": [], "_empty": True}
    call_count = {"n": 0}

    def fake_extract(text, items, **kw):
        call_count["n"] += 1
        return (fake_diff, None)

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)

    client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert call_count["n"] == 1

    # Mutate the reply body — e.g. operator manually edited the
    # excerpt or the body_excerpt was re-extracted from the raw
    # message. Cache key changes → re-extract.
    rfq = seed_rfq_with_replies["rfq_alpha"]
    rfq["buyer_replies"][0]["body_excerpt"] = "Buyer changed mind: $3 only."

    r2 = client.post(
        "/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    body = r2.get_json()
    assert body["cached"] is False
    assert call_count["n"] == 2


def test_items_change_invalidates_cache(
        client, seed_rfq_with_replies, monkeypatch):
    fake_diff = {"price_changes": [], "qty_changes": [],
                 "items_added": [], "items_removed": [],
                 "notes": [], "_empty": True}
    call_count = {"n": 0}

    def fake_extract(text, items, **kw):
        call_count["n"] += 1
        return (fake_diff, None)

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)

    client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert call_count["n"] == 1

    # Operator changed unit price on widget A — cache must invalidate.
    rfq = seed_rfq_with_replies["rfq_alpha"]
    rfq["line_items"][0]["unit_price"] = 4.75

    r2 = client.post(
        "/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    body = r2.get_json()
    assert body["cached"] is False
    assert call_count["n"] == 2


def test_skipped_reason_is_also_cached(
        client, seed_rfq_with_replies, monkeypatch):
    """A skip outcome (e.g. ANTHROPIC_API_KEY not set) must cache
    too. Otherwise the operator hammering the button while the env
    is misconfigured re-invokes the helper 100x on each click —
    cheap (no Claude call), but pointless work."""
    call_count = {"n": 0}

    def fake_extract(text, items, **kw):
        call_count["n"] += 1
        return ({}, "ANTHROPIC_API_KEY not set")

    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff", fake_extract)

    r1 = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r1.get_json()["skipped_reason"] == "ANTHROPIC_API_KEY not set"
    assert call_count["n"] == 1

    r2 = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    body = r2.get_json()
    assert body["cached"] is True
    assert body["skipped_reason"] == "ANTHROPIC_API_KEY not set"
    assert call_count["n"] == 1


def test_save_failure_is_logged_but_does_not_fail_request(
        client, seed_rfq_with_replies, monkeypatch):
    """If `_save_single_rfq` raises (DB lock, disk full), the
    operator must still get the diff — cache-write is best-effort."""
    fake_diff = {"price_changes": [], "qty_changes": [],
                 "items_added": [], "items_removed": [],
                 "notes": [], "_empty": True}
    monkeypatch.setattr(
        "src.agents.buyer_reply_diff.extract_quote_diff",
        lambda text, items, **kw: (fake_diff, None))

    def boom(*a, **kw):
        raise RuntimeError("disk full")
    monkeypatch.setattr("src.api.data_layer._save_single_rfq", boom)

    r = client.post("/api/rfq/rfq_alpha/buyer-replies/0/extract-diff")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["cached"] is False
