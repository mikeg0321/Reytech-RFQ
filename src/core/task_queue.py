"""
task_queue.py — Lightweight SQLite-backed task queue.

Enables web process to enqueue work, worker process to dequeue and execute.
No Redis or external dependencies needed.

Usage:
    from src.core.task_queue import enqueue, dequeue, complete, fail

    # Producer (web route)
    task_id = enqueue("run_scprs_pull", {"agency": "CDCR"}, actor="user")

    # Consumer (worker loop)
    task = dequeue()
    if task:
        try:
            result = do_work(task)
            complete(task["id"], result)
        except Exception as e:
            fail(task["id"], str(e))
"""
import json
import logging
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger("reytech.taskqueue")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    task_type   TEXT NOT NULL,
    payload     TEXT DEFAULT '{}',
    status      TEXT DEFAULT 'pending',
    priority    INTEGER DEFAULT 0,
    actor       TEXT DEFAULT 'system',
    started_at  TEXT,
    finished_at TEXT,
    result      TEXT,
    error       TEXT,
    retries     INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3
);
CREATE INDEX IF NOT EXISTS idx_tq_status ON task_queue(status);
CREATE INDEX IF NOT EXISTS idx_tq_type ON task_queue(task_type);
"""


def _get_db():
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=30); conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_task_queue():
    """Create task_queue table if not exists."""
    conn = _get_db()
    conn.executescript(_SCHEMA)
    conn.close()


def enqueue(task_type: str, payload: dict = None, actor: str = "system",
            priority: int = 0, max_retries: int = 3) -> int:
    """Add a task to the queue. Returns task ID."""
    conn = _get_db()
    try:
        cur = conn.execute(
            "INSERT INTO task_queue (task_type, payload, actor, priority, max_retries) "
            "VALUES (?, ?, ?, ?, ?)",
            (task_type, json.dumps(payload or {}), actor, priority, max_retries))
        conn.commit()
        task_id = cur.lastrowid
        log.info("Task enqueued: id=%d type=%s actor=%s", task_id, task_type, actor)
        return task_id
    finally:
        conn.close()


def dequeue(task_types: list = None) -> dict | None:
    """Claim the next pending task. Returns task dict or None."""
    conn = _get_db()
    try:
        where = "WHERE status = 'pending'"
        params = []
        if task_types:
            placeholders = ",".join("?" * len(task_types))
            where += f" AND task_type IN ({placeholders})"
            params.extend(task_types)
        row = conn.execute(
            f"SELECT * FROM task_queue {where} ORDER BY priority DESC, id ASC LIMIT 1",
            params).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE task_queue SET status='running', started_at=datetime('now') WHERE id=?",
            (row["id"],))
        conn.commit()
        return dict(row)
    finally:
        conn.close()


def complete(task_id: int, result: dict = None):
    """Mark a task as completed."""
    conn = _get_db()
    try:
        conn.execute(
            "UPDATE task_queue SET status='completed', finished_at=datetime('now'), result=? WHERE id=?",
            (json.dumps(result or {}), task_id))
        conn.commit()
    finally:
        conn.close()


def fail(task_id: int, error: str):
    """Mark a task as failed. Retries if under max_retries."""
    conn = _get_db()
    try:
        row = conn.execute("SELECT retries, max_retries FROM task_queue WHERE id=?",
                           (task_id,)).fetchone()
        if row and row["retries"] < row["max_retries"]:
            conn.execute(
                "UPDATE task_queue SET status='pending', retries=retries+1, error=? WHERE id=?",
                (error, task_id))
        else:
            conn.execute(
                "UPDATE task_queue SET status='failed', finished_at=datetime('now'), error=? WHERE id=?",
                (error, task_id))
        conn.commit()
        log.warning("Task %d failed: %s", task_id, error[:200])
    finally:
        conn.close()


def reset_stale_running(max_age_minutes: int = 30) -> int:
    """On boot: reset tasks stuck in 'running' (from pre-deploy crash) back to 'pending'.

    Returns count of tasks reset.
    """
    conn = _get_db()
    try:
        cur = conn.execute(
            "UPDATE task_queue SET status='pending', error='reset on boot' "
            "WHERE status='running' AND started_at < datetime('now', ?)",
            (f"-{max_age_minutes} minutes",))
        conn.commit()
        count = cur.rowcount
        if count:
            log.info("Reset %d stale 'running' tasks to 'pending' on boot", count)
        return count
    finally:
        conn.close()


def get_queue_stats() -> dict:
    """Get queue statistics."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM task_queue GROUP BY status").fetchall()
        stats = {r["status"]: r["cnt"] for r in rows}
        stats["total"] = sum(stats.values())
        return stats
    finally:
        conn.close()


def cleanup(days: int = 7):
    """Remove completed/failed tasks older than N days."""
    conn = _get_db()
    try:
        conn.execute(
            "DELETE FROM task_queue WHERE status IN ('completed','failed') "
            "AND finished_at < datetime('now', ?)",
            (f"-{days} days",))
        conn.commit()
    finally:
        conn.close()
