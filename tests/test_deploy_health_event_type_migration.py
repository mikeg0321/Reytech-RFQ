"""Pin: pre-PR-#1079 `deploy_health` notification rows are rewritten
to the canonical `deploy_health_failed` event_type on boot.

Chrome MCP audit 2026-05-26 anomaly #5: PR #1079 (2026-05-25) renamed
the startup-checks emitter from event_type='deploy_health' (urgency
'urgent') to event_type='deploy_health_failed' (urgency 'warning')
to align with the new silent-default routing. The emitter rename
was correct but the pre-rename rows in the notifications table were
left behind, surfacing as a duplicate card on /notifications with
the wrong urgency tier. This migration cleans them up — idempotent,
one-shot, runs in init_db_deferred.
"""
from __future__ import annotations


def _seed(conn, event_type, urgency, title="x"):
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


def test_migration_rewrites_pre_rename_rows():
    from src.core.db import get_db, _migrate_deploy_health_event_type
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN ('deploy_health','deploy_health_failed')"
        )
        for _ in range(5):
            _seed(conn, "deploy_health", "urgent")
        for _ in range(2):
            _seed(conn, "deploy_health_failed", "warning")

    rewritten = _migrate_deploy_health_event_type()
    assert rewritten == 5

    with get_db() as conn:
        # All 7 should now be on the canonical event_type.
        assert _count(conn, "deploy_health") == 0
        assert _count(conn, "deploy_health_failed") == 7
        # Urgency on the rewritten ones is now warning.
        rows = conn.execute(
            "SELECT urgency, COUNT(*) FROM notifications "
            "WHERE event_type='deploy_health_failed' "
            "GROUP BY urgency"
        ).fetchall()
        urgencies = {r[0]: r[1] for r in rows}
        assert urgencies.get("warning") == 7, urgencies
        assert "urgent" not in urgencies


def test_migration_is_idempotent():
    from src.core.db import get_db, _migrate_deploy_health_event_type
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN ('deploy_health','deploy_health_failed')"
        )
        for _ in range(3):
            _seed(conn, "deploy_health", "urgent")

    # First run: 3 rewrites.
    assert _migrate_deploy_health_event_type() == 3
    # Second run: nothing left to do.
    assert _migrate_deploy_health_event_type() == 0
    # Third run, same.
    assert _migrate_deploy_health_event_type() == 0


def test_migration_does_not_touch_unrelated_rows():
    from src.core.db import get_db, _migrate_deploy_health_event_type
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN "
            "('deploy_health','deploy_health_failed','server_error',"
            "'rfq_arrived')"
        )
        _seed(conn, "deploy_health", "urgent")
        _seed(conn, "server_error", "warning")
        _seed(conn, "rfq_arrived", "urgent")

    _migrate_deploy_health_event_type()

    with get_db() as conn:
        # Substrate-singleness check: only deploy_health was touched.
        assert _count(conn, "deploy_health_failed") == 1
        assert _count(conn, "server_error") == 1
        assert _count(conn, "rfq_arrived") == 1
        # And the urgency on unrelated rows wasn't clobbered.
        row = conn.execute(
            "SELECT urgency FROM notifications "
            "WHERE event_type='rfq_arrived'"
        ).fetchone()
        assert row[0] == "urgent"
