"""The Spine — persistence layer.

ONE table (spine_quotes). ONE writer (write_quote). Append-only event
log. No joins to priced_carts or rfq_files. The Spine's correctness
does not depend on legacy table correctness.

The architectural test `test_one_writer.py` enforces that the only
function calling `.execute("INSERT ... spine_quotes ...")` or
`.execute("UPDATE spine_quotes ...")` is `_persist_state` in this
module. Adding a second writer fails the build.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from src.spine.model import (
    Quote,
    SpineValidationError,
    _COMPUTED_FIELD_NAMES,
)

# ──────────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS spine_quotes (
    quote_id    TEXT PRIMARY KEY,
    state_json  TEXT NOT NULL,
    event_log   TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spine_quotes_updated_at
    ON spine_quotes(updated_at DESC);
"""

# Module-level write lock — Python-side serializer for the single
# writer. The DB write itself is atomic via INSERT OR REPLACE; this
# lock prevents lost-update races on the read-modify-write of the
# event_log.
_WRITE_LOCK = threading.Lock()


# ──────────────────────────────────────────────────────────────────────
# Connection management — stays inside the Spine.
# ──────────────────────────────────────────────────────────────────────


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path) -> None:
    """Idempotent schema setup."""
    with _connect(db_path) as conn:
        conn.executescript(SCHEMA)


# ──────────────────────────────────────────────────────────────────────
# Read path — pure, no mutation.
# ──────────────────────────────────────────────────────────────────────


def read_quote(db_path: str | Path, quote_id: str) -> Quote | None:
    """Load and validate a Quote from the DB. Returns None if absent.

    Validation runs on every load — a malformed row raises immediately
    instead of propagating to a renderer.
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT state_json FROM spine_quotes WHERE quote_id = ?",
            (quote_id,),
        ).fetchone()
    if row is None:
        return None
    state = json.loads(row["state_json"])
    return Quote.model_validate(state)


def read_event_log(db_path: str | Path, quote_id: str) -> list[dict]:
    """Return the append-only event log for a quote (oldest-first)."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT event_log FROM spine_quotes WHERE quote_id = ?",
            (quote_id,),
        ).fetchone()
    if row is None:
        return []
    return json.loads(row["event_log"])


def iter_quote_ids(db_path: str | Path) -> Iterator[str]:
    """Iterate all Spine quote IDs in updated_at DESC order."""
    with _connect(db_path) as conn:
        for row in conn.execute(
            "SELECT quote_id FROM spine_quotes ORDER BY updated_at DESC"
        ):
            yield row["quote_id"]


# ──────────────────────────────────────────────────────────────────────
# Write path — THE ONLY WRITER. Tested by test_one_writer.py.
# ──────────────────────────────────────────────────────────────────────


def write_quote(
    db_path: str | Path,
    quote: Quote,
    *,
    actor: str,
    note: str | None = None,
) -> Quote:
    """Persist `quote` atomically. Appends to the event log.

    Args:
        db_path: SQLite database path.
        quote:   The full Quote state to persist. Must already be
                 validated (`Quote.model_validate(...)` ran).
        actor:   Who made the change. "operator", "spine_ingest",
                 "system", or a specific username. Required for audit.
        note:    Optional human-readable note for the event log entry.

    Returns:
        The persisted Quote (same instance, with updated_at refreshed).

    Raises:
        SpineValidationError: if the requested write would corrupt the
            event log (e.g., trying to overwrite a sent quote without
            first transitioning it).
    """
    if not actor or not actor.strip():
        raise SpineValidationError("write_quote requires non-empty actor.")

    now_iso = datetime.now(timezone.utc).isoformat()
    quote = quote.model_copy(update={"updated_at": datetime.now(timezone.utc)})

    new_event = {
        "timestamp": now_iso,
        "actor": actor.strip(),
        "status": quote.status.value,
        "note": note,
        "state": _quote_to_persisted_dict(quote),
    }

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            prior = conn.execute(
                "SELECT state_json, event_log, created_at FROM spine_quotes "
                "WHERE quote_id = ?",
                (quote.quote_id,),
            ).fetchone()

            if prior is None:
                events = [new_event]
                created_at = now_iso
            else:
                prior_quote = Quote.model_validate(json.loads(prior["state_json"]))
                if prior_quote.status.value == "sent":
                    raise SpineValidationError(
                        f"quote_id={quote.quote_id!r} is already sent — terminal. "
                        "Sent quotes are immutable in v1."
                    )
                events = json.loads(prior["event_log"])
                events.append(new_event)
                created_at = prior["created_at"]

            _persist_state(
                conn,
                quote_id=quote.quote_id,
                state_json=json.dumps(_quote_to_persisted_dict(quote)),
                event_log=json.dumps(events),
                created_at=created_at,
                updated_at=now_iso,
            )

    return quote


def _persist_state(
    conn: sqlite3.Connection,
    *,
    quote_id: str,
    state_json: str,
    event_log: str,
    created_at: str,
    updated_at: str,
) -> None:
    """THE ONLY function in src/spine/ that writes to spine_quotes.

    Guarded by test_one_writer.py. Adding another writer to
    spine_quotes anywhere in src/spine/ fails the build.
    """
    conn.execute(
        "INSERT OR REPLACE INTO spine_quotes "
        "(quote_id, state_json, event_log, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (quote_id, state_json, event_log, created_at, updated_at),
    )


# ──────────────────────────────────────────────────────────────────────
# Serialization — model ↔ persisted dict.
# ──────────────────────────────────────────────────────────────────────


def _quote_to_persisted_dict(quote: Quote) -> dict:
    """Serialize a Quote for storage. Delegates to Quote.to_persisted_dict().

    Single source of truth lives on the model itself; this is the
    DB-layer indirection so future schema changes (e.g., wrapping in
    a "v": 1 envelope) happen in one place.
    """
    return quote.to_persisted_dict()
