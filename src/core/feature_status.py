"""Persistent feature-status table — the dashboard banner backend.

PRs #181-#187 added drainable skip ledgers to five modules. Each ledger
fills with SkipReason events and is drained by its caller (orchestrator,
scheduler, route). But each drain only covers one call site at one moment.
An operator opening the dashboard 14 minutes after the last quote ran has
no idea which features are currently degraded — by the time they look,
every ledger is empty.

This module persists drained skips into a SQLite table keyed by
(name, where, severity). Each occurrence bumps a counter and updates
`last_seen`. The dashboard banner reads `current_status()` to show
"Claude amazon lookup: degraded since 14m ago (37 hits)" without having
to wait for the next pipeline run.

Public surface:

    record_skips(skips: list[SkipReason]) -> None
        Persist a batch of drained skips. Same-key occurrences dedupe.

    current_status(prune_older_than_days: int = 14) -> list[dict]
        Read all currently-active feature degradations, prune stale ones
        on the same call so an old transient hit doesn't haunt the banner
        forever. Ordered most-severe-first, then most-recent-first.

The table is auto-created on first call so consumers don't need to wire
a migration.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.core.dependency_check import Severity, SkipReason

# ── Path resolution ───────────────────────────────────────────────────────────
# Reuse the volume-aware DATA_DIR so prod writes survive redeploys. Tests
# patch _DB_PATH_OVERRIDE for isolation.
_DB_PATH_OVERRIDE: Optional[str] = None
_table_init_lock = threading.Lock()
_table_initialized = False


def _db_path() -> str:
    if _DB_PATH_OVERRIDE:
        return _DB_PATH_OVERRIDE
    # Source of truth is src.core.db.DB_PATH — same string on prod volume,
    # same string on local dev. Avoids drift if DATA_DIR resolution changes.
    from src.core.db import DB_PATH
    return DB_PATH


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create the table on first use. Uses (name, where, severity) as the
    natural key — same skip from the same site at the same severity is
    one row. Different `where` for the same `name` (e.g. claude_amazon_lookup
    vs claude_product_lookup both missing ANTHROPIC_API_KEY) gets two rows
    so operators can see exactly which surfaces are affected."""
    global _table_initialized
    if _table_initialized and not _DB_PATH_OVERRIDE:
        return
    with _table_init_lock:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feature_status (
                name        TEXT NOT NULL,
                where_      TEXT NOT NULL,
                severity    TEXT NOT NULL,
                reason      TEXT NOT NULL,
                count       INTEGER NOT NULL DEFAULT 1,
                first_seen  TEXT NOT NULL,
                last_seen   TEXT NOT NULL,
                PRIMARY KEY (name, where_, severity)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_feature_status_last_seen
            ON feature_status (last_seen DESC)
        """)
        conn.commit()
        _table_initialized = True


def record_skips(skips: list[SkipReason]) -> None:
    """Persist a batch of drained skips. Empty input is a no-op so callers
    can drain unconditionally without checking length."""
    if not skips:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        _ensure_table(conn)
        for s in skips:
            sev = s.severity.value
            # UPSERT: insert if new, otherwise bump count + update last_seen + reason.
            conn.execute("""
                INSERT INTO feature_status
                    (name, where_, severity, reason, count, first_seen, last_seen)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT (name, where_, severity) DO UPDATE SET
                    count = count + 1,
                    reason = excluded.reason,
                    last_seen = excluded.last_seen
            """, (s.name, s.where, sev, s.reason, now, now))
        conn.commit()


# Severity ordering for display: most-severe-first so the banner draws
# the operator's eye to the worst outage.
_SEVERITY_ORDER = {"blocker": 0, "warning": 1, "info": 2}


def current_status(prune_older_than_days: int = 14) -> list[dict]:
    """Return all currently-active feature degradations.

    Prunes entries whose `last_seen` is older than `prune_older_than_days`
    on the same call so a one-off transient hit from two weeks ago doesn't
    haunt the banner forever.

    Ordered: most-severe-first, then most-recent-first (operators triage
    the worst recent thing first).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=prune_older_than_days)).isoformat()
    with _connect() as conn:
        _ensure_table(conn)
        # Prune stale rows on read.
        conn.execute("DELETE FROM feature_status WHERE last_seen < ?", (cutoff,))
        conn.commit()
        rows = conn.execute("""
            SELECT name, where_ AS "where", severity, reason, count,
                   first_seen, last_seen
            FROM feature_status
            ORDER BY last_seen DESC
        """).fetchall()
    out = [dict(r) for r in rows]
    # Stable sort by severity (Python sorted is stable, so within-severity
    # order remains last_seen DESC from the SQL).
    out.sort(key=lambda r: _SEVERITY_ORDER.get(r["severity"], 99))
    return out


# ── Test seam ────────────────────────────────────────────────────────────────
# Tests need to backdate a row's last_seen to verify pruning. Real callers
# must never call this — pruning is a read-time concern, not a write-time one.
def _set_last_seen_for_test(*, name: str, where: str, severity: str,
                             last_seen: str) -> None:
    with _connect() as conn:
        _ensure_table(conn)
        conn.execute(
            "UPDATE feature_status SET last_seen=? "
            "WHERE name=? AND where_=? AND severity=?",
            (last_seen, name, where, severity),
        )
        conn.commit()
