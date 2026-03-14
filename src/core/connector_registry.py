"""
connector_registry.py — Connector lifecycle management.

All connector metadata lives in the 'connectors' DB table.
No connector config lives in Python code.
"""
import logging
import sqlite3
from datetime import datetime, timezone, timedelta

log = logging.getLogger("reytech.connectors")


def _get_conn():
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=30); conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def get_active_connectors() -> list:
    """Return all connectors with status='active'."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM connectors WHERE status='active' ORDER BY priority"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_connector(connector_id: str) -> dict | None:
    """Return a single connector by ID."""
    conn = _get_conn()
    row = conn.execute("SELECT * FROM connectors WHERE id=?", (connector_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_due_connectors() -> list:
    """Return active connectors that are overdue for a pull."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT * FROM connectors
        WHERE status = 'active'
          AND (last_pulled_at IS NULL
               OR datetime(last_pulled_at, '+' || pull_frequency_hours || ' hours') < datetime('now'))
        ORDER BY priority ASC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_connector_after_pull(connector_id: str, health_grade: str,
                                 record_count: int) -> None:
    """Update connector metadata after a pull completes."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE connectors SET last_pulled_at=?, last_health_grade=?,
            record_count=? WHERE id=?
    """, (now, health_grade, record_count, connector_id))
    conn.commit()
    conn.close()


def get_all_connectors() -> list:
    """Return all connectors (active + scaffolded)."""
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM connectors ORDER BY priority").fetchall()
    conn.close()
    return [dict(r) for r in rows]
