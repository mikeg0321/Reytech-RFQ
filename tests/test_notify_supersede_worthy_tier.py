"""Pin: WORTHY-tier liveness alerts supersede their prior Telegram card.

PR-A (back-window audit Item 6 / 2026-05-26): without supersede, the
daily liveness sweep accreted stale duplicate cards in Mike's chat —
five "Gmail inbound silent 96h" cards spanning a week, each one a
separate alarm that already had a successor. Fix:
`_telegram_post` calls `_supersede_prior_telegrams` after a successful
send for events in `_SUPERSEDING_EVENT_TYPES`; the helper queries
prior un-deleted telegram_messages rows for the same event_type and
deletes them via Telegram's deleteMessage API.

These tests pin:
  1. The 9 expected WORTHY events are in the superseding set
  2. Same event firing twice → second supersedes first via Telegram API
     + DB deleted_at stamp
  3. Different event_types do NOT supersede each other
  4. Non-superseding events (e.g. oracle_weekly, app_down) → no-op
  5. Telegram "message to delete not found" past 48h wall → still
     stamps deleted_at (we don't retry forever)
  6. Other delete errors → delete_error column, deleted_at stays NULL
  7. _telegram_post wires _supersede after _record_telegram_send
     (order matters — record first so the new row exists and the
     supersede query's `message_id != ?` filter targets only OLDER cards)
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest


# ─── _SUPERSEDING_EVENT_TYPES set ─────────────────────────────────────


def test_superseding_set_contains_worthy_liveness_events():
    from src.agents.notify_agent import _SUPERSEDING_EVENT_TYPES
    expected = {
        "award_tracker_idle",
        "external_service_disconnected",
        "scprs_pull_failed_persistent",
        "gmail_oauth_expired",
        "twilio_unreachable",
        "oracle_weekly_failed",
        "oracle_weekly_never_sent",
        "oracle_weekly_overdue",
        "oracle_weekly_crash",
    }
    assert expected.issubset(_SUPERSEDING_EVENT_TYPES)


def test_superseding_set_excludes_distinct_content_events():
    """Each oracle_weekly digest is distinct content (different week's
    report). Each loss_pattern_detected MAY be distinct. Each
    app_down occurrence is investigative-worthy. None should supersede."""
    from src.agents.notify_agent import _SUPERSEDING_EVENT_TYPES
    assert "oracle_weekly" not in _SUPERSEDING_EVENT_TYPES
    assert "loss_pattern_detected" not in _SUPERSEDING_EVENT_TYPES
    assert "app_down" not in _SUPERSEDING_EVENT_TYPES
    assert "ingest_broken" not in _SUPERSEDING_EVENT_TYPES


# ─── _supersede_prior_telegrams primary path ──────────────────────────


def _seed_telegram_messages(conn, rows):
    """Helper: insert (message_id, chat_id, event_type, sent_at) tuples."""
    for r in rows:
        conn.execute(
            "INSERT OR REPLACE INTO telegram_messages "
            "(message_id, chat_id, event_type, title, sent_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (r["message_id"], r["chat_id"], r["event_type"],
             r.get("title", ""), r.get("sent_at",
             datetime.now(timezone.utc).isoformat())),
        )


def test_supersede_deletes_prior_card_for_same_event(monkeypatch):
    """Two cards for the same WORTHY event → second call supersedes the
    first. Pinned: Telegram deleteMessage called once with prior id;
    DB row stamped deleted_at."""
    from src.core.db import get_db
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")

    with get_db() as conn:
        _seed_telegram_messages(conn, [
            {"message_id": 100, "chat_id": "12345",
             "event_type": "gmail_oauth_expired"},
            {"message_id": 200, "chat_id": "12345",
             "event_type": "gmail_oauth_expired"},
        ])

    delete_calls = []

    def fake_delete(message_id):
        delete_calls.append(int(message_id))
        return {"ok": True}

    monkeypatch.setattr(na, "_telegram_delete_message", fake_delete)

    n = na._supersede_prior_telegrams(
        "gmail_oauth_expired", current_message_id=200,
    )

    # Only the prior (100) was superseded; the current (200) was NOT
    # touched because the helper's `message_id != ?` filter excludes it.
    assert n == 1
    assert delete_calls == [100]

    with get_db() as conn:
        r100 = conn.execute(
            "SELECT deleted_at FROM telegram_messages WHERE message_id=100"
        ).fetchone()
        r200 = conn.execute(
            "SELECT deleted_at FROM telegram_messages WHERE message_id=200"
        ).fetchone()
    assert r100[0] is not None, "prior card must have deleted_at stamped"
    assert r200[0] is None, "current card must NOT be marked deleted"


def test_supersede_skips_different_event_types(monkeypatch):
    """A scprs_pull_failed_persistent card must NOT be superseded by
    a later gmail_oauth_expired card."""
    from src.core.db import get_db
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")

    with get_db() as conn:
        _seed_telegram_messages(conn, [
            {"message_id": 300, "chat_id": "12345",
             "event_type": "scprs_pull_failed_persistent"},
            {"message_id": 400, "chat_id": "12345",
             "event_type": "gmail_oauth_expired"},
        ])

    delete_calls = []
    monkeypatch.setattr(
        na, "_telegram_delete_message",
        lambda mid: delete_calls.append(mid) or {"ok": True},
    )

    n = na._supersede_prior_telegrams(
        "gmail_oauth_expired", current_message_id=400,
    )
    assert n == 0
    assert delete_calls == []  # no delete fired


def test_supersede_skips_already_deleted_rows(monkeypatch):
    """A row with deleted_at IS NOT NULL is excluded from the query —
    no double-delete attempts."""
    from src.core.db import get_db
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        _seed_telegram_messages(conn, [
            {"message_id": 500, "chat_id": "12345",
             "event_type": "twilio_unreachable"},
        ])
        conn.execute(
            "UPDATE telegram_messages SET deleted_at = ? "
            "WHERE message_id = 500",
            (now,),
        )
        _seed_telegram_messages(conn, [
            {"message_id": 600, "chat_id": "12345",
             "event_type": "twilio_unreachable"},
        ])

    delete_calls = []
    monkeypatch.setattr(
        na, "_telegram_delete_message",
        lambda mid: delete_calls.append(mid) or {"ok": True},
    )
    n = na._supersede_prior_telegrams(
        "twilio_unreachable", current_message_id=600,
    )
    assert n == 0
    assert delete_calls == []


def test_supersede_noop_for_non_superseding_event(monkeypatch):
    """`oracle_weekly` is NOT in the set — function should early-return
    without touching the DB or calling Telegram."""
    import src.agents.notify_agent as na

    delete_calls = []
    monkeypatch.setattr(
        na, "_telegram_delete_message",
        lambda mid: delete_calls.append(mid) or {"ok": True},
    )

    n = na._supersede_prior_telegrams(
        "oracle_weekly", current_message_id=999,
    )
    assert n == 0
    assert delete_calls == []


def test_supersede_treats_not_found_as_success(monkeypatch):
    """Telegram's 48h delete wall returns 'message to delete not
    found'. Treat as success: stamp deleted_at so we stop retrying."""
    from src.core.db import get_db
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")

    with get_db() as conn:
        _seed_telegram_messages(conn, [
            {"message_id": 700, "chat_id": "12345",
             "event_type": "award_tracker_idle"},
            {"message_id": 800, "chat_id": "12345",
             "event_type": "award_tracker_idle"},
        ])

    def fake_delete(message_id):
        return {"ok": False, "error": "Bad Request: message to delete not found"}

    monkeypatch.setattr(na, "_telegram_delete_message", fake_delete)

    n = na._supersede_prior_telegrams(
        "award_tracker_idle", current_message_id=800,
    )
    # 0 successful supersedes (the API call failed), BUT the DB row
    # for 700 gets deleted_at stamped because "not found" means it's
    # already gone from chat.
    assert n == 0
    with get_db() as conn:
        row = conn.execute(
            "SELECT deleted_at, delete_error FROM telegram_messages "
            "WHERE message_id=700"
        ).fetchone()
    assert row[0] is not None, "not-found prior card must stamp deleted_at"
    assert "not found" in row[1].lower()


def test_supersede_records_other_errors_without_stamping_deleted(monkeypatch):
    """Generic Telegram API errors (rate limit, network failure) leave
    deleted_at NULL so the next supersede attempt retries; the error
    is captured in delete_error for debuggability."""
    from src.core.db import get_db
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")

    with get_db() as conn:
        _seed_telegram_messages(conn, [
            {"message_id": 900, "chat_id": "12345",
             "event_type": "external_service_disconnected"},
            {"message_id": 901, "chat_id": "12345",
             "event_type": "external_service_disconnected"},
        ])

    def fake_delete(message_id):
        return {"ok": False, "error": "429 Too Many Requests"}

    monkeypatch.setattr(na, "_telegram_delete_message", fake_delete)
    na._supersede_prior_telegrams(
        "external_service_disconnected", current_message_id=901,
    )
    with get_db() as conn:
        row = conn.execute(
            "SELECT deleted_at, delete_error FROM telegram_messages "
            "WHERE message_id=900"
        ).fetchone()
    assert row[0] is None, "transient errors must NOT mark deleted_at"
    assert "429" in row[1]


# ─── _telegram_post wires supersede after successful send ─────────────


def test_telegram_post_fires_supersede_after_send(monkeypatch):
    """End-to-end: _telegram_post sends, records, then supersedes —
    in that order. The order matters: record FIRST so the new row
    exists; the supersede query's `message_id != ?` filter targets
    only OLDER cards."""
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setattr(na, "TELEGRAM_BOT_TOKEN", "T")

    call_order = []

    class _Resp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok":true,"result":{"message_id":1234}}'

    def fake_urlopen(req, timeout=None):
        return _Resp()

    def fake_record(message_id, event_type, title):
        call_order.append(("record", message_id, event_type))

    def fake_supersede(event_type, current_message_id):
        call_order.append(("supersede", event_type, current_message_id))
        return 0

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr(na, "_record_telegram_send", fake_record)
    monkeypatch.setattr(na, "_supersede_prior_telegrams", fake_supersede)

    result = na._telegram_post(
        text="hello", event_type="gmail_oauth_expired", title="test",
    )
    assert result["ok"] is True
    assert result["message_id"] == 1234
    # Order: record first, supersede second
    assert [c[0] for c in call_order] == ["record", "supersede"]
    assert call_order[0] == ("record", 1234, "gmail_oauth_expired")
    assert call_order[1] == ("supersede", "gmail_oauth_expired", 1234)


def test_telegram_post_skips_supersede_on_send_failure(monkeypatch):
    """If Telegram rejects the send, NO supersede fires — we don't
    want to delete the prior card and leave Mike with no card at all
    when the new send broke."""
    import src.agents.notify_agent as na
    monkeypatch.setattr(na, "TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setattr(na, "TELEGRAM_BOT_TOKEN", "T")

    class _Resp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok":false,"description":"chat not found"}'

    supersede_calls = []
    monkeypatch.setattr("urllib.request.urlopen",
                        lambda *a, **kw: _Resp())
    monkeypatch.setattr(
        na, "_supersede_prior_telegrams",
        lambda et, current_message_id: supersede_calls.append(et) or 0,
    )

    result = na._telegram_post(
        text="t", event_type="gmail_oauth_expired", title="x",
    )
    assert result["ok"] is False
    assert supersede_calls == [], "supersede must NOT fire on send failure"
