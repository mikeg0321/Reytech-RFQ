"""
snapshots.py — Pre-run snapshots for destructive agent operations.

Before an agent modifies data, take a snapshot. If something goes wrong,
restore from the snapshot.

Usage:
    from src.core.snapshots import create_snapshot, restore_snapshot, list_snapshots

    snap_id = create_snapshot("scprs_pull", "price_checks", data_dict)
    # ... agent does its work ...
    # If bad: restore_snapshot(snap_id)
"""
import json
import logging
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger("reytech.snapshots")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    agent_name  TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    run_id      TEXT,
    data_json   TEXT NOT NULL,
    row_count   INTEGER DEFAULT 0,
    restored    INTEGER DEFAULT 0,
    restored_at TEXT,
    notes       TEXT
);
CREATE INDEX IF NOT EXISTS idx_snap_agent ON agent_snapshots(agent_name);
"""


def _get_db():
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=30); conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_snapshots():
    """Create snapshots table if not exists."""
    conn = _get_db()
    conn.executescript(_SCHEMA)
    conn.close()


def create_snapshot(agent_name: str, entity_type: str, data: dict | list,
                    run_id: str = None, notes: str = "") -> int:
    """Save a snapshot before a destructive operation. Returns snapshot ID."""
    data_json = json.dumps(data, default=str)
    row_count = len(data) if isinstance(data, (list, dict)) else 0
    conn = _get_db()
    try:
        cur = conn.execute(
            "INSERT INTO agent_snapshots (agent_name, entity_type, run_id, data_json, row_count, notes) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (agent_name, entity_type, run_id, data_json, row_count, notes))
        conn.commit()
        snap_id = cur.lastrowid
        log.info("Snapshot created: id=%d agent=%s entity=%s rows=%d",
                 snap_id, agent_name, entity_type, row_count)
        return snap_id
    finally:
        conn.close()


def get_snapshot(snap_id: int) -> dict | None:
    """Retrieve a snapshot by ID."""
    conn = _get_db()
    try:
        row = conn.execute("SELECT * FROM agent_snapshots WHERE id=?", (snap_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["data"] = json.loads(result.pop("data_json"))
        return result
    finally:
        conn.close()


def restore_snapshot(snap_id: int) -> dict:
    """Load snapshot data and mark as restored. Returns the snapshot data.
    Caller is responsible for writing the data back to the appropriate store."""
    conn = _get_db()
    try:
        row = conn.execute("SELECT * FROM agent_snapshots WHERE id=?", (snap_id,)).fetchone()
        if not row:
            return {"ok": False, "error": "Snapshot not found"}
        conn.execute(
            "UPDATE agent_snapshots SET restored=1, restored_at=datetime('now') WHERE id=?",
            (snap_id,))
        conn.commit()
        data = json.loads(row["data_json"])
        log.warning("Snapshot restored: id=%d agent=%s entity=%s",
                    snap_id, row["agent_name"], row["entity_type"])
        return {"ok": True, "data": data, "agent": row["agent_name"],
                "entity": row["entity_type"], "row_count": row["row_count"]}
    finally:
        conn.close()


def list_snapshots(agent_name: str = None, limit: int = 20) -> list:
    """List recent snapshots, optionally filtered by agent."""
    conn = _get_db()
    try:
        if agent_name:
            rows = conn.execute(
                "SELECT id, created_at, agent_name, entity_type, run_id, row_count, restored, notes "
                "FROM agent_snapshots WHERE agent_name=? ORDER BY id DESC LIMIT ?",
                (agent_name, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, created_at, agent_name, entity_type, run_id, row_count, restored, notes "
                "FROM agent_snapshots ORDER BY id DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def cleanup(days: int = 30):
    """Remove old snapshots. Keeps restored ones longer."""
    conn = _get_db()
    try:
        conn.execute(
            "DELETE FROM agent_snapshots WHERE restored=0 "
            "AND created_at < datetime('now', ?)", (f"-{days} days",))
        conn.execute(
            "DELETE FROM agent_snapshots WHERE restored=1 "
            "AND created_at < datetime('now', ?)", (f"-{days * 3} days",))
        conn.commit()
    finally:
        conn.close()
