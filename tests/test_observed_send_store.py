"""Contract tests for observed_sends persistence + admin endpoints
(PR-G2 of post-quote queue item 23).

Pins the upsert / list / confirm / reject lifecycle. Each test creates
its own in-memory DB schema so it doesn't depend on the full app boot.

Doctrine pinned here:
  * Re-running the scanner doesn't double-insert (UNIQUE on
    gmail_message_id; existing pending rows get UPDATE, decided rows
    get skipped).
  * confirm() appends gmail_message_id to the matched record's
    gmail_message_ids list (mirrors PR-E forward path).
  * reject() leaves the row in the table — Reytech Law 22.
  * Idempotent confirm/reject of a row already in the same status =
    {"ok": True, "no_change": True}.
  * Cannot confirm a rejected obs (and vice versa) — operator must
    explicitly transition through reset (future PR).
"""
from __future__ import annotations

import importlib
import sqlite3
import sys
from unittest.mock import patch

import pytest


@pytest.fixture
def store():
    if "src.agents.observed_send_store" in sys.modules:
        del sys.modules["src.agents.observed_send_store"]
    return importlib.import_module("src.agents.observed_send_store")


@pytest.fixture
def conn(tmp_path):
    """Fresh DB with the observed_sends schema applied."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    # Replay just migration 41 — the rest aren't needed for store tests.
    from src.core.migrations import MIGRATIONS
    for num, _name, sql in MIGRATIONS:
        if num == 41:
            db.executescript(sql)
    yield db
    db.close()


def _make_detection(matches):
    return {
        "ok": True,
        "since_days": 7,
        "scanned": len(matches),
        "matches": matches,
        "unmatched": [],
        "skipped_non_quote": 0,
    }


def _match(**kw):
    base = {
        "gmail_message_id": "msg_001",
        "thread_id": "thread_x",
        "subject": "Reytech Quote R26Q40",
        "to": "buyer@example.com",
        "date": "Tue, 06 May 2026 15:30:00 -0700",
        "matched_record_id": "rfq_alpha",
        "matched_record_kind": "rfq",
        "match_signal": "quote_number",
        "match_value": "R26Q40",
        "confidence": 0.95,
        "already_attached": False,
    }
    base.update(kw)
    return base


# ─── upsert_from_detection ────────────────────────────────────────────


def test_upsert_inserts_new_pending_rows(store, conn):
    result = store.upsert_from_detection(
        _make_detection([_match()]), conn=conn)
    conn.commit()

    assert result["ok"]
    assert result["inserted"] == 1
    assert result["updated"] == 0
    assert result["rows"][0]["status"] == "pending"


def test_upsert_idempotent_on_pending_row(store, conn):
    """Second scan of same message updates fields but stays pending."""
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()

    # Same message, different match details
    new_match = _match(subject="Reytech Quote R26Q40 (resend)",
                       confidence=0.92)
    result = store.upsert_from_detection(
        _make_detection([new_match]), conn=conn)
    conn.commit()

    assert result["inserted"] == 0
    assert result["updated"] == 1
    rows = store.list_observed_sends(conn=conn)
    assert len(rows) == 1
    assert rows[0]["subject"] == "Reytech Quote R26Q40 (resend)"
    assert rows[0]["status"] == "pending"


def test_upsert_skips_already_decided(store, conn):
    """If operator already confirmed the obs, re-scanning shouldn't
    drop it back to pending."""
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]

    # Mark confirmed directly (skip the helper for setup speed)
    conn.execute(
        "UPDATE observed_sends SET status='confirmed' WHERE id=?",
        (obs_id,))
    conn.commit()

    result = store.upsert_from_detection(
        _make_detection([_match()]), conn=conn)
    conn.commit()

    assert result["skipped_already_decided"] == 1
    assert result["inserted"] == 0
    assert result["updated"] == 0
    rows = store.list_observed_sends(conn=conn)
    assert rows[0]["status"] == "confirmed"  # unchanged


def test_upsert_skips_matches_without_message_id(store, conn):
    bad = _match(gmail_message_id="")
    result = store.upsert_from_detection(
        _make_detection([bad]), conn=conn)
    conn.commit()
    assert result["inserted"] == 0


# ─── list_observed_sends ──────────────────────────────────────────────


def test_list_filters_by_status(store, conn):
    store.upsert_from_detection(
        _make_detection([
            _match(gmail_message_id="msg_a"),
            _match(gmail_message_id="msg_b"),
            _match(gmail_message_id="msg_c"),
        ]), conn=conn)
    conn.execute("UPDATE observed_sends SET status='confirmed' "
                 "WHERE gmail_message_id='msg_b'")
    conn.commit()

    pending = store.list_observed_sends(status="pending", conn=conn)
    assert {r["gmail_message_id"] for r in pending} == {"msg_a", "msg_c"}

    confirmed = store.list_observed_sends(status="confirmed", conn=conn)
    assert [r["gmail_message_id"] for r in confirmed] == ["msg_b"]

    all_rows = store.list_observed_sends(conn=conn)
    assert len(all_rows) == 3


def test_list_orders_newest_first(store, conn):
    """Newer created_at sorts to top — operator UI defaults to most-
    recent first."""
    import time
    store.upsert_from_detection(
        _make_detection([_match(gmail_message_id="msg_old")]), conn=conn)
    conn.commit()
    time.sleep(1.05)  # ensure created_at differs at second resolution
    store.upsert_from_detection(
        _make_detection([_match(gmail_message_id="msg_new")]), conn=conn)
    conn.commit()

    rows = store.list_observed_sends(conn=conn)
    assert rows[0]["gmail_message_id"] == "msg_new"


# ─── confirm ──────────────────────────────────────────────────────────


def test_confirm_sets_status_and_calls_record_attach(store, conn):
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]

    with patch.object(store, "_append_to_record_message_ids",
                      return_value=True) as mock_attach:
        result = store.confirm(obs_id, by="mike",
                               notes="confirmed via test", conn=conn)
        conn.commit()

    assert result["ok"]
    assert result["row"]["status"] == "confirmed"
    assert result["row"]["decided_by"] == "mike"
    assert result["row"]["notes"] == "confirmed via test"
    assert result["attached_to_record"] is True
    mock_attach.assert_called_once_with(
        "rfq_alpha", "rfq", "msg_001")


def test_confirm_idempotent_on_already_confirmed(store, conn):
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]
    conn.execute(
        "UPDATE observed_sends SET status='confirmed' WHERE id=?",
        (obs_id,))
    conn.commit()

    result = store.confirm(obs_id, conn=conn)
    assert result["ok"]
    assert result.get("no_change") is True


def test_confirm_refuses_rejected(store, conn):
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]
    conn.execute(
        "UPDATE observed_sends SET status='rejected' WHERE id=?",
        (obs_id,))
    conn.commit()

    result = store.confirm(obs_id, conn=conn)
    assert not result["ok"]
    assert "rejected" in result["error"]


def test_confirm_returns_error_for_missing_id(store, conn):
    result = store.confirm(99999, conn=conn)
    assert not result["ok"]
    assert "not found" in result["error"]


# ─── reject ───────────────────────────────────────────────────────────


def test_reject_sets_status_and_keeps_row(store, conn):
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]

    result = store.reject(obs_id, by="mike", reason="not Reytech",
                          conn=conn)
    conn.commit()

    assert result["ok"]
    assert result["row"]["status"] == "rejected"
    assert result["row"]["notes"] == "not Reytech"

    # Row still in table (Reytech Law 22)
    cnt = conn.execute(
        "SELECT COUNT(*) FROM observed_sends WHERE id=?",
        (obs_id,)).fetchone()[0]
    assert cnt == 1


def test_reject_refuses_confirmed(store, conn):
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]
    conn.execute(
        "UPDATE observed_sends SET status='confirmed' WHERE id=?",
        (obs_id,))
    conn.commit()

    result = store.reject(obs_id, conn=conn)
    assert not result["ok"]
    assert "confirmed" in result["error"]


def test_reject_idempotent(store, conn):
    store.upsert_from_detection(_make_detection([_match()]), conn=conn)
    conn.commit()
    obs_id = conn.execute(
        "SELECT id FROM observed_sends LIMIT 1").fetchone()["id"]
    store.reject(obs_id, conn=conn)
    conn.commit()
    result = store.reject(obs_id, conn=conn)
    assert result["ok"]
    assert result.get("no_change") is True
