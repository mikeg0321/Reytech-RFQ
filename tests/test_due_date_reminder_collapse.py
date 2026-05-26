"""Pin: due_date_reminder is collapsed onto deadline_critical at the
emitter + historical rows are migrated.

Chrome MCP audit 2026-05-26 anomaly #4: two daemons fired the same
conceptual event (bid deadline approaching) under two event_types —
`due_date_reminder` (src/agents/due_date_reminder.py:131, ~21/30d)
and `deadline_critical` (notify_agent.start_deadline_watcher).
Substrate-singleness — collapse onto the canonical name.
"""
from __future__ import annotations


def test_due_date_reminder_emit_site_uses_canonical_event_type():
    """The emitter writes event_type='deadline_critical' (not the
    legacy 'due_date_reminder'). Anchored on the file source so a
    future revert is caught."""
    from pathlib import Path
    src = Path(
        __file__,
    ).parent.parent / "src" / "agents" / "due_date_reminder.py"
    content = src.read_text(encoding="utf-8")
    # Must NOT emit the legacy event_type.
    assert 'send_alert("due_date_reminder"' not in content, (
        "due_date_reminder emit site still uses legacy event_type — "
        "should be 'deadline_critical' for substrate-singleness."
    )
    # Must emit the canonical event_type.
    assert 'send_alert(\n                "deadline_critical"' in content \
        or 'send_alert("deadline_critical"' in content, (
        "due_date_reminder emit site missing the canonical "
        "'deadline_critical' event_type."
    )


def test_due_date_reminder_emit_site_has_per_bid_cooldown():
    """Pre-fix the emit site passed no entity_id, so the default
    cooldown_key collided to 'due_date_reminder:' across all bids —
    explaining the suspiciously low 21 events / 30d (everything but
    the first dedup'd). Post-fix must pass per-bid cooldown_key."""
    from pathlib import Path
    src = Path(
        __file__,
    ).parent.parent / "src" / "agents" / "due_date_reminder.py"
    content = src.read_text(encoding="utf-8")
    assert 'cooldown_key=f"deadline_critical:' in content, (
        "due_date_reminder emit site lost per-bid cooldown_key — "
        "would dedupe ALL bids to one global key."
    )
    assert "cooldown_seconds=3600" in content, (
        "due_date_reminder emit site lost explicit 1h cooldown — "
        "would fall back to the 15-min default."
    )


# ─── Historical migration ────────────────────────────────────────────


def _seed(conn, event_type, urgency="urgent", title="x"):
    from datetime import datetime
    conn.execute(
        "INSERT INTO notifications "
        "(created_at, event_type, urgency, title) "
        "VALUES (?, ?, ?, ?)",
        (datetime.utcnow().isoformat(), event_type, urgency, title),
    )


def _count(conn, event_type):
    row = conn.execute(
        "SELECT COUNT(*) FROM notifications WHERE event_type=?",
        (event_type,),
    ).fetchone()
    return row[0] if row else 0


def test_migration_rewrites_pre_collapse_rows():
    from src.core.db import (
        get_db,
        _migrate_due_date_reminder_event_type,
    )
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN ('due_date_reminder','deadline_critical')"
        )
        for _ in range(4):
            _seed(conn, "due_date_reminder")
        for _ in range(2):
            _seed(conn, "deadline_critical")

    rewritten = _migrate_due_date_reminder_event_type()
    assert rewritten == 4

    with get_db() as conn:
        assert _count(conn, "due_date_reminder") == 0
        assert _count(conn, "deadline_critical") == 6


def test_migration_is_idempotent():
    from src.core.db import (
        get_db,
        _migrate_due_date_reminder_event_type,
    )
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN ('due_date_reminder','deadline_critical')"
        )
        for _ in range(2):
            _seed(conn, "due_date_reminder")

    assert _migrate_due_date_reminder_event_type() == 2
    assert _migrate_due_date_reminder_event_type() == 0
    assert _migrate_due_date_reminder_event_type() == 0


def test_migration_does_not_touch_unrelated_rows():
    from src.core.db import (
        get_db,
        _migrate_due_date_reminder_event_type,
    )
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN "
            "('due_date_reminder','deadline_critical','rfq_arrived',"
            "'cs_draft_ready')"
        )
        _seed(conn, "due_date_reminder")
        _seed(conn, "rfq_arrived")
        _seed(conn, "cs_draft_ready")

    _migrate_due_date_reminder_event_type()

    with get_db() as conn:
        assert _count(conn, "deadline_critical") == 1
        assert _count(conn, "rfq_arrived") == 1
        assert _count(conn, "cs_draft_ready") == 1
