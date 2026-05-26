"""Pin: /api/notifications/mark-event-read + mark_event_type_read helper.

PR Phase B (back-window audit 2026-05-26 follow-on): Mike's bell archive
hit 99+ unread after the Coleman NON-IT misparse fired 1,313
`deadline_critical` events for one bid. PR #1102 superseded the
Telegram side; this PR is the bell-side counterpart — one click clears
one event_type class without touching the rest of the archive.

Tests pin:
  1. Endpoint marks only matching event_type rows
  2. Endpoint leaves OTHER event_type rows untouched (substrate-singleness)
  3. Endpoint rejects missing/blank event_type with 400
  4. Helper is idempotent — second call returns updated=0
  5. Helper handles unknown event_type without error (updated=0)
  6. Already-read rows are NOT re-touched (write stays small)
"""
from __future__ import annotations

from datetime import datetime


def _seed_notification(conn, event_type, is_read=0, title="t", body="b",
                       urgency="info", created_at=None):
    conn.execute(
        "INSERT INTO notifications "
        "(created_at, event_type, urgency, title, body, is_read) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (created_at or datetime.utcnow().isoformat(),
         event_type, urgency, title, body, is_read),
    )


def _purge_notifications(conn):
    conn.execute("DELETE FROM notifications")


def _unread_count(conn, event_type):
    row = conn.execute(
        "SELECT COUNT(*) FROM notifications "
        "WHERE event_type=? AND is_read=0",
        (event_type,),
    ).fetchone()
    return row[0] if row else 0


# ─── Endpoint ────────────────────────────────────────────────────────


def test_mark_event_read_clears_only_matching_event_type(auth_client):
    """The whole point: 5 deadline_critical rows clear, 3 cs_draft_ready
    stay unread."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)
        for _ in range(5):
            _seed_notification(conn, "deadline_critical")
        for _ in range(3):
            _seed_notification(conn, "cs_draft_ready")

    r = auth_client.post(
        "/api/notifications/mark-event-read",
        json={"event_type": "deadline_critical"},
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["updated"] == 5
    assert data["event_type"] == "deadline_critical"

    with get_db() as conn:
        assert _unread_count(conn, "deadline_critical") == 0
        # Substrate-singleness check — unrelated event_type untouched.
        assert _unread_count(conn, "cs_draft_ready") == 3


def test_mark_event_read_rejects_missing_event_type(auth_client):
    r = auth_client.post(
        "/api/notifications/mark-event-read",
        json={},
    )
    assert r.status_code == 400
    data = r.get_json()
    assert data["ok"] is False
    assert "event_type" in data["error"]


def test_mark_event_read_rejects_blank_event_type(auth_client):
    r = auth_client.post(
        "/api/notifications/mark-event-read",
        json={"event_type": "   "},
    )
    assert r.status_code == 400


def test_mark_event_read_handles_unknown_event_type(auth_client):
    """An event_type that doesn't exist returns ok=True updated=0
    — endpoint is idempotent + safe to call on a stale UI."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)

    r = auth_client.post(
        "/api/notifications/mark-event-read",
        json={"event_type": "never_emitted_xyz"},
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["updated"] == 0


# ─── Helper ──────────────────────────────────────────────────────────


def test_mark_event_type_read_helper_is_idempotent():
    """Second call on the same event_type returns updated=0 — no double-
    write. Rules out the "click button twice = double-count" bug class."""
    from src.core.db import get_db
    from src.agents.notify_agent import mark_event_type_read
    with get_db() as conn:
        _purge_notifications(conn)
        for _ in range(4):
            _seed_notification(conn, "rfq_arrived")

    r1 = mark_event_type_read("rfq_arrived")
    assert r1["ok"] is True
    assert r1["updated"] == 4

    r2 = mark_event_type_read("rfq_arrived")
    assert r2["ok"] is True
    assert r2["updated"] == 0


def test_mark_event_type_read_helper_skips_already_read():
    """Pre-read rows are not re-touched — keeps the UPDATE small even
    when run on a noisy event_type after the initial cleanup."""
    from src.core.db import get_db
    from src.agents.notify_agent import mark_event_type_read
    with get_db() as conn:
        _purge_notifications(conn)
        for _ in range(2):
            _seed_notification(conn, "po_received", is_read=1)
        for _ in range(3):
            _seed_notification(conn, "po_received", is_read=0)

    r = mark_event_type_read("po_received")
    assert r["ok"] is True
    assert r["updated"] == 3

    with get_db() as conn:
        # All 5 now read; nothing unread of this type.
        assert _unread_count(conn, "po_received") == 0


def test_mark_event_type_read_helper_rejects_blank():
    from src.agents.notify_agent import mark_event_type_read
    r = mark_event_type_read("")
    assert r["ok"] is False
    assert r["updated"] == 0

    r2 = mark_event_type_read(None)
    assert r2["ok"] is False
    assert r2["updated"] == 0
