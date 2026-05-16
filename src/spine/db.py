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

import hashlib
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

-- ──────────────────────────────────────────────────────────────────
-- Immutable Quote PDF snapshots.
-- ──────────────────────────────────────────────────────────────────
-- A snapshot is the durable, byte-identical record of what was shown
-- to the operator at approval time. Each row is the materialization
-- of one Finalize-and-Snapshot click:
--   - pdf_bytes         : the rendered PDF, byte-identical to what the
--                         operator approved and to what gets emailed.
--   - sha256            : hex digest of pdf_bytes for integrity checks
--                         against transit corruption.
--   - state_json        : Quote.to_persisted_dict() at snapshot time.
--                         The matching gate (in quote_pdf.py) has
--                         already verified pdf_bytes is consistent
--                         with this state before the row is written.
--   - actor / note      : same audit fields as the event log.
-- This table is APPEND-ONLY. There is no UPDATE path. There is no
-- DELETE path. The "void and replace" pattern from Stripe is the
-- precedent — corrections create a new snapshot, never modify a
-- prior one.
CREATE TABLE IF NOT EXISTS spine_quote_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    quote_id    TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    pdf_bytes   BLOB NOT NULL,
    state_json  TEXT NOT NULL,
    actor       TEXT NOT NULL,
    note        TEXT,
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spine_snapshots_quote_id
    ON spine_quote_snapshots(quote_id, created_at DESC);
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
# Snapshot path — THE ONLY WRITER for spine_quote_snapshots.
# ──────────────────────────────────────────────────────────────────────


def write_snapshot(
    db_path: str | Path,
    quote: Quote,
    *,
    actor: str,
    note: str | None = None,
) -> dict:
    """Render `quote` to PDF, run the matching gate, persist the bytes.

    The act of writing a snapshot is the act of approving "these exact
    bytes are what we will deliver to the agency." Three things happen
    atomically (Python-side lock + SQLite atomicity):

    1. The Quote is rendered via render_quote_pdf, which runs the
       SpineRenderMismatchError gate. If the renderer ever produces
       bytes that disagree with the model, no snapshot is written.
    2. SHA-256 of pdf_bytes is computed for integrity-on-send checks.
    3. (snapshot_id, quote_id, sha256, pdf_bytes, state_json, actor,
       note, created_at) is INSERTed into spine_quote_snapshots.

    The status precondition (quote.status in {FINALIZED, SENT}) is
    enforced at the HTTP layer, not here — internal callers
    (re-render, audit replay) may want snapshots of earlier states
    for forensic comparison without satisfying the operator-flow gate.

    Returns:
        dict with snapshot_id, sha256, created_at, byte_len.

    Raises:
        SpineRenderMismatchError: render disagreed with the model.
        SpineValidationError: actor was empty.
    """
    from src.spine.quote_pdf import render_quote_pdf

    if not actor or not actor.strip():
        raise SpineValidationError("write_snapshot requires non-empty actor.")

    pdf_bytes = render_quote_pdf(quote)  # raises SpineRenderMismatchError on lie
    sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    now_iso = datetime.now(timezone.utc).isoformat()
    state_dict = _quote_to_persisted_dict(quote)
    # Exclude updated_at from the identity hash — it changes every save
    # but does not represent a different operator-approved state. The
    # snapshot's job is to commit to a specific Quote SHAPE; updated_at
    # is a timestamp on that shape, not a part of it.
    identity_dict = {k: v for k, v in state_dict.items() if k != "updated_at"}
    state_json = json.dumps(state_dict, sort_keys=True)
    state_identity = hashlib.sha256(
        json.dumps(identity_dict, sort_keys=True).encode("utf-8")
    ).hexdigest()

    # Deterministic snapshot_id derived from the STATE's identity hash,
    # not the PDF bytes — ReportLab embeds /CreationDate and /ModDate
    # in every render, so byte hashes differ across calls even for an
    # unchanged Quote. The state identity is the operator's intent;
    # the PDF bytes are a deterministic-modulo-timestamps render of
    # that intent. Idempotency lives on intent, not bytes.
    snapshot_id = f"snap_{quote.quote_id}_{state_identity[:12]}"

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            existing = conn.execute(
                "SELECT 1 FROM spine_quote_snapshots WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if existing:
                # Same bytes, same state → no-op. Caller treats it as
                # success. This is intentional: clicking Snapshot
                # twice on an unchanged state should not duplicate
                # rows in the audit chain.
                row = conn.execute(
                    "SELECT snapshot_id, sha256, created_at, LENGTH(pdf_bytes) AS byte_len "
                    "FROM spine_quote_snapshots WHERE snapshot_id = ?",
                    (snapshot_id,),
                ).fetchone()
                return dict(row)
            _persist_snapshot(
                conn,
                snapshot_id=snapshot_id,
                quote_id=quote.quote_id,
                sha256=sha256,
                pdf_bytes=pdf_bytes,
                state_json=state_json,
                actor=actor.strip(),
                note=note,
                created_at=now_iso,
            )

    return {
        "snapshot_id": snapshot_id,
        "sha256": sha256,
        "created_at": now_iso,
        "byte_len": len(pdf_bytes),
    }


def _persist_snapshot(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
    quote_id: str,
    sha256: str,
    pdf_bytes: bytes,
    state_json: str,
    actor: str,
    note: str | None,
    created_at: str,
) -> None:
    """THE ONLY function in src/spine/ that writes spine_quote_snapshots.

    Mirror of _persist_state for the snapshot table. Adding any other
    INSERT/UPDATE/DELETE against spine_quote_snapshots inside src/spine/
    is a substrate regression and must fail the architecture-contract
    test (test_one_writer_for_spine_snapshots).
    """
    conn.execute(
        "INSERT INTO spine_quote_snapshots "
        "(snapshot_id, quote_id, sha256, pdf_bytes, state_json, "
        " actor, note, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (snapshot_id, quote_id, sha256, pdf_bytes, state_json,
         actor, note, created_at),
    )


def read_snapshot(db_path: str | Path, snapshot_id: str) -> dict | None:
    """Load a snapshot by ID. Returns dict with bytes + metadata, or None."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT snapshot_id, quote_id, sha256, pdf_bytes, state_json, "
            "       actor, note, created_at "
            "FROM spine_quote_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def iter_snapshots(db_path: str | Path, quote_id: str) -> list[dict]:
    """Return all snapshots for a quote, newest-first. Excludes bytes."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT snapshot_id, quote_id, sha256, state_json, "
            "       actor, note, created_at, LENGTH(pdf_bytes) AS byte_len "
            "FROM spine_quote_snapshots WHERE quote_id = ? "
            "ORDER BY created_at DESC",
            (quote_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def latest_snapshot(db_path: str | Path, quote_id: str) -> dict | None:
    """Return the most recent snapshot for a quote, or None.

    Used by routes_spine to enforce the finalized→sent precondition:
    a quote may only transition to sent if its current state matches
    the state captured in the latest snapshot.
    """
    rows = iter_snapshots(db_path, quote_id)
    return rows[0] if rows else None


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
