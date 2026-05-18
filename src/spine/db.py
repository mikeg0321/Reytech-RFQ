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

-- ──────────────────────────────────────────────────────────────────
-- EmailContracts — the ingestion master record.
-- ──────────────────────────────────────────────────────────────────
-- Per Mike 2026-05-16: the email contract is the ground truth that
-- every Quote and rendered PDF is compared against. One row per
-- ingestion event. Append-only — corrections (rebid, revised RFQ)
-- create a NEW contract row with the same source_thread_id, never
-- modify the prior. The contract is the BUYER'S statement; we
-- preserve it byte-for-byte so audit + replay both work.
CREATE TABLE IF NOT EXISTS spine_email_contracts (
    contract_id     TEXT PRIMARY KEY,
    rfq_id          TEXT,                  -- nullable: contract may pre-date its quote
    pc_id           TEXT,
    source_email_id TEXT,
    source_thread_id TEXT,
    contract_json   TEXT NOT NULL,         -- EmailContract.model_dump(mode='json')
    sha256          TEXT NOT NULL,
    ingested_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spine_contracts_rfq_id
    ON spine_email_contracts(rfq_id);
CREATE INDEX IF NOT EXISTS idx_spine_contracts_thread
    ON spine_email_contracts(source_thread_id, ingested_at DESC);

-- ──────────────────────────────────────────────────────────────────
-- IngestRejections — every email considered emits a row.
-- ──────────────────────────────────────────────────────────────────
-- Closes the "missed-bid silent-drop" class (Russ-class miss). When
-- the ingest pipeline refuses an inbound email, the rejection is
-- durably recorded so the operator can audit what was dropped and
-- the Telegram missed-bid watcher (queued) can escalate aging
-- rejections. Append-only; one writer (`write_ingest_rejection`).
CREATE TABLE IF NOT EXISTS spine_ingest_rejections (
    rejection_id     TEXT PRIMARY KEY,
    source_email_id  TEXT,
    source_thread_id TEXT,
    sender_email     TEXT,
    subject          TEXT,
    reason_code      TEXT NOT NULL,
    reason_detail    TEXT,
    raw_excerpt      TEXT,
    received_at      TEXT,
    rejected_at      TEXT NOT NULL,
    parser_version   TEXT
);

CREATE INDEX IF NOT EXISTS idx_spine_rejections_rejected_at
    ON spine_ingest_rejections(rejected_at DESC);
CREATE INDEX IF NOT EXISTS idx_spine_rejections_reason
    ON spine_ingest_rejections(reason_code);
CREATE INDEX IF NOT EXISTS idx_spine_rejections_thread
    ON spine_ingest_rejections(source_thread_id);

-- ──────────────────────────────────────────────────────────────────
-- Sequential counters — the substrate primitive for R26PCXXXX /
-- R26R#### / R26Q#### buyer-facing numbers.
-- ──────────────────────────────────────────────────────────────────
-- One row per named counter ("pc_2026", "rfq_2026", "quote_2026", ...).
-- next_value() reads-modifies-writes under _WRITE_LOCK and refuses to
-- jump more than +5 in a single call (mirrors the quote_counter rule
-- from CLAUDE.md: "Max jump = 5. Counter blocked if it tries to jump
-- more than 5 from last known value.").
--
-- The current_value column stores the LAST value handed out. The next
-- call returns current_value + 1. Counters are year-namespaced by the
-- caller (e.g., "pc_2026" rolls to "pc_2027" on Jan 1) so a year
-- rollover doesn't require resetting the integer — it just selects a
-- different row.
CREATE TABLE IF NOT EXISTS spine_counters (
    counter_name  TEXT PRIMARY KEY,
    current_value INTEGER NOT NULL,
    last_set_at   TEXT NOT NULL,
    last_actor    TEXT
);

-- ──────────────────────────────────────────────────────────────────
-- Quote ↔ Quote links — typically PC predecessor ← RFQ rebid.
-- ──────────────────────────────────────────────────────────────────
-- Closes Mike's 5/17 directive: "PC goes out, RFQ comes in, should be
-- auto priced". This table records the relationship so the editor can
-- surface "this RFQ matches your prior PC R26PC####" and the auto-price
-- substrate (queued PR) can copy validated prior costs forward.
--
-- Directional: from_quote_id is the NEW quote (typically the RFQ);
-- to_quote_id is the PRIOR quote (typically the PC). A single from
-- may have multiple links (manual + auto, primary + alternate); the
-- top-confidence row is the canonical link.
--
-- Append-only. Operators don't DELETE prior judgments — they add new
-- ones. A bad auto-link is overridden by a higher-confidence
-- "operator_manual" link, not by deleting the auto row. Audit chain
-- stays intact.
--
-- evidence_json carries the structured reason the matcher decided:
--   { "mfg_overlap_ratio": 0.75, "desc_jaccard_mean": 0.62,
--     "same_facility": true, "same_solicitation_number": false,
--     "matched_line_pairs": [[1, 1], [2, 3], [4, 4]] }
-- Future matchers may add fields; reader is tolerant.
CREATE TABLE IF NOT EXISTS spine_quote_links (
    link_id        TEXT PRIMARY KEY,
    from_quote_id  TEXT NOT NULL,
    to_quote_id    TEXT NOT NULL,
    match_method   TEXT NOT NULL,
    confidence     REAL NOT NULL,
    evidence_json  TEXT NOT NULL,
    linked_at      TEXT NOT NULL,
    actor          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spine_links_from
    ON spine_quote_links(from_quote_id, confidence DESC);
CREATE INDEX IF NOT EXISTS idx_spine_links_to
    ON spine_quote_links(to_quote_id, confidence DESC);

-- ──────────────────────────────────────────────────────────────────
-- Product catalog — buyer-supplied + validated product data.
-- ──────────────────────────────────────────────────────────────────
-- Closes Mike's 5/17 directive: "this data should be cataloged in
-- the table". Every spine ingest with a non-empty mfg_number emits
-- a catalog observation. Over time the catalog becomes the durable
-- record of what buyers actually ask for + what we've priced before.
--
-- One row per normalized MFG#. Repeated observations of the same
-- MFG# update last_seen_at, increment seen_count, and union the
-- descriptions/uoms/unspsc lists. Cost data updates last_priced_at
-- + last_priced_cents + last_priced_quote_id when a Quote line
-- carrying that MFG# has cost_cents > 0 (substrate input for the
-- stale-cost recheck signal — task #22).
--
-- source_url + photo_url + photo_path are nullable enrichment
-- columns (task #23). The catalog substrate exposes the columns;
-- the actual fetcher is a separate background worker that walks
-- the catalog and fills them in.
CREATE TABLE IF NOT EXISTS spine_catalog (
    catalog_id            TEXT PRIMARY KEY,
    mfg_number            TEXT NOT NULL,
    canonical_description TEXT NOT NULL,
    descriptions_json     TEXT NOT NULL,
    uoms_seen_json        TEXT NOT NULL,
    unspsc_codes_json     TEXT NOT NULL,
    seen_count            INTEGER NOT NULL DEFAULT 1,
    first_seen_at         TEXT NOT NULL,
    last_seen_at          TEXT NOT NULL,
    last_seen_quote_id    TEXT,
    -- Stale-cost recheck signal (task #22)
    last_priced_at        TEXT,
    last_priced_quote_id  TEXT,
    last_priced_cents     INTEGER,
    -- Enrichment columns (task #23) — written by a background fetcher.
    source_url            TEXT,
    source_url_checked_at TEXT,
    photo_url             TEXT,
    photo_path            TEXT,
    enrichment_status     TEXT
);

CREATE INDEX IF NOT EXISTS idx_spine_catalog_mfg
    ON spine_catalog(mfg_number);
CREATE INDEX IF NOT EXISTS idx_spine_catalog_last_seen
    ON spine_catalog(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_spine_catalog_last_priced
    ON spine_catalog(last_priced_at DESC);
CREATE INDEX IF NOT EXISTS idx_spine_catalog_enrichment
    ON spine_catalog(enrichment_status, last_seen_at DESC);
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

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            prior = conn.execute(
                "SELECT state_json, event_log, created_at FROM spine_quotes "
                "WHERE quote_id = ?",
                (quote.quote_id,),
            ).fetchone()

            # First-write sequential number assignment. On the very
            # first persist of a quote_id, pull the next R{yy}Q####
            # integer from spine_counters and stamp the model so every
            # subsequent read renders the same buyer-facing identifier.
            # We do this INSIDE the _WRITE_LOCK + connection so the
            # counter increment and the spine_quotes INSERT are atomic
            # from the caller's point of view: two parallel first-writes
            # can't both win the same seq, and a counter increment
            # can never be left without a corresponding quote row.
            if prior is None and quote.quote_seq is None:
                year = datetime.now(timezone.utc).year
                row = conn.execute(
                    "SELECT current_value FROM spine_counters "
                    "WHERE counter_name = ?",
                    (f"quote_{year}",),
                ).fetchone()
                seq = (int(row["current_value"]) if row is not None else 0) + 1
                _persist_counter(
                    conn,
                    counter_name=f"quote_{year}",
                    current_value=seq,
                    last_set_at=now_iso,
                    last_actor=actor.strip(),
                )
                quote = quote.model_copy(update={
                    "quote_seq": seq,
                    "quote_year": year,
                })

            new_event = {
                "timestamp": now_iso,
                "actor": actor.strip(),
                "status": quote.status.value,
                "note": note,
                "state": _quote_to_persisted_dict(quote),
            }

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

                # IDENTITY IMMUTABILITY GUARD — closes the 5/18 regression
                # where the editor JS Save dropped quote_seq from the
                # round-trip dict, the POST handler validated a new Quote
                # with quote_seq=None, write_quote silently persisted the
                # null, and R26Q40 → pc_e96e0408 on every subsequent
                # render. quote_seq + quote_year + quote_id are IDENTITY
                # fields: stamped once, immutable forever.
                #
                # Behavior:
                #   - prior stamped, new=None → PRESERVE from prior (the
                #     common case — editor Save that didn't echo seq).
                #   - prior stamped, new=different → REJECT (mutation
                #     attempt; identity changes corrupt the audit chain
                #     because every snapshot/render refers to display_number).
                #   - prior unstamped, new=None → fall through to the
                #     first-write block above (but `prior is not None`
                #     here, so this is dead — see the `prior is None`
                #     guard at the top).
                if prior_quote.quote_seq is not None:
                    if quote.quote_seq is None:
                        # Editor JS round-trip without quote_seq —
                        # preserve identity from disk.
                        quote = quote.model_copy(update={
                            "quote_seq": prior_quote.quote_seq,
                            "quote_year": prior_quote.quote_year,
                        })
                    elif (
                        quote.quote_seq != prior_quote.quote_seq
                        or quote.quote_year != prior_quote.quote_year
                    ):
                        raise SpineValidationError(
                            f"quote_id={quote.quote_id!r}: identity is "
                            f"immutable. prior stamped "
                            f"quote_seq={prior_quote.quote_seq} "
                            f"quote_year={prior_quote.quote_year}; "
                            f"write attempted "
                            f"quote_seq={quote.quote_seq} "
                            f"quote_year={quote.quote_year}. "
                            "Identity (quote_seq, quote_year) is "
                            "stamped once and is the buyer-facing "
                            "reference for every snapshot, render, "
                            "and send. Mutation is rejected. If the "
                            "operator needs to re-number, use the "
                            "admin counter/backfill endpoints on a "
                            "rolled-back row, not an in-place edit. "
                            "5/18 regression class — substrate gate."
                        )
                    # else: client echoed back the same stamped value —
                    # OK, no-op preservation.
                # Re-snapshot the event payload with the (possibly
                # re-stamped) quote so the event-log state matches what
                # we're about to persist.
                new_event["state"] = _quote_to_persisted_dict(quote)
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
# EmailContract path — THE ONLY WRITER for spine_email_contracts.
# ──────────────────────────────────────────────────────────────────────


def write_email_contract(db_path: str | Path, contract) -> dict:
    """Persist an EmailContract. Append-only.

    Per Mike 2026-05-16: the contract is the BUYER'S statement and
    is captured once at ingest. Re-issuing the same contract_id
    raises — corrections must come as NEW contract rows with a new
    contract_id (typically sharing source_thread_id for rebid
    grouping).

    Returns metadata dict (sha256, ingested_at, byte_len).
    """
    from src.spine.email_contract import EmailContract

    if not isinstance(contract, EmailContract):
        raise SpineValidationError(
            f"write_email_contract expects EmailContract; got "
            f"{type(contract).__name__}"
        )

    state_json = json.dumps(contract.model_dump(mode="json"), sort_keys=True)
    sha = hashlib.sha256(state_json.encode("utf-8")).hexdigest()
    now_iso = datetime.now(timezone.utc).isoformat()

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            existing = conn.execute(
                "SELECT 1 FROM spine_email_contracts WHERE contract_id = ?",
                (contract.contract_id,),
            ).fetchone()
            if existing:
                raise SpineValidationError(
                    f"contract_id={contract.contract_id!r} already exists. "
                    "Contracts are append-only; create a new contract_id "
                    "for rebids (preserve source_thread_id to group)."
                )
            _persist_contract(
                conn,
                contract_id=contract.contract_id,
                rfq_id=contract.rfq_id,
                pc_id=contract.pc_id,
                source_email_id=contract.source_email_id,
                source_thread_id=contract.source_thread_id,
                contract_json=state_json,
                sha256=sha,
                ingested_at=now_iso,
            )

    return {
        "contract_id": contract.contract_id,
        "sha256": sha,
        "ingested_at": now_iso,
        "byte_len": len(state_json),
    }


def _persist_contract(
    conn: sqlite3.Connection,
    *,
    contract_id: str,
    rfq_id: str | None,
    pc_id: str | None,
    source_email_id: str | None,
    source_thread_id: str | None,
    contract_json: str,
    sha256: str,
    ingested_at: str,
) -> None:
    """THE ONLY function in src/spine/ that writes spine_email_contracts.

    Mirror of _persist_state / _persist_snapshot. Adding any other
    INSERT/UPDATE/DELETE against spine_email_contracts inside src/spine/
    is a substrate regression.
    """
    conn.execute(
        "INSERT INTO spine_email_contracts "
        "(contract_id, rfq_id, pc_id, source_email_id, source_thread_id, "
        " contract_json, sha256, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (contract_id, rfq_id, pc_id, source_email_id, source_thread_id,
         contract_json, sha256, ingested_at),
    )


def read_email_contract(db_path: str | Path, contract_id: str):
    """Load and validate an EmailContract from the DB. Returns None
    if absent. The contract is fully Pydantic-validated on every
    read — same property as Quote."""
    from src.spine.email_contract import EmailContract

    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT contract_json FROM spine_email_contracts WHERE contract_id = ?",
            (contract_id,),
        ).fetchone()
    if row is None:
        return None
    state = json.loads(row["contract_json"])
    return EmailContract.model_validate(state)


def find_contract_for_quote(db_path: str | Path, quote_id: str):
    """Find the EmailContract that drove ingest of `quote_id`. Returns
    the EmailContract, or None if the quote was ingested before the
    contract substrate existed (legacy data).
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT contract_id FROM spine_email_contracts "
            "WHERE rfq_id = ? ORDER BY ingested_at DESC LIMIT 1",
            (quote_id,),
        ).fetchone()
    if row is None:
        return None
    return read_email_contract(db_path, row["contract_id"])


# ──────────────────────────────────────────────────────────────────────
# IngestRejection path — THE ONLY WRITER for spine_ingest_rejections.
# ──────────────────────────────────────────────────────────────────────


def write_ingest_rejection(db_path: str | Path, rejection) -> dict:
    """Persist an IngestRejection. Append-only.

    Closes the "missed-bid silent-drop" class: every email the parser
    considered and refused emits exactly one row here. Re-issuing the
    same rejection_id is rejected (substrate invariant: rejection is a
    durable historical fact; if the same email is reconsidered later
    and a new judgment is rendered, that's a NEW rejection_id).

    Returns metadata dict (rejection_id, rejected_at).
    """
    from src.spine.ingest_rejection import IngestRejection

    if not isinstance(rejection, IngestRejection):
        raise SpineValidationError(
            f"write_ingest_rejection expects IngestRejection; got "
            f"{type(rejection).__name__}"
        )

    rejected_at_iso = rejection.rejected_at.astimezone(timezone.utc).isoformat()
    received_at_iso = (
        rejection.received_at.astimezone(timezone.utc).isoformat()
        if rejection.received_at is not None else None
    )

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            existing = conn.execute(
                "SELECT 1 FROM spine_ingest_rejections WHERE rejection_id = ?",
                (rejection.rejection_id,),
            ).fetchone()
            if existing:
                raise SpineValidationError(
                    f"rejection_id={rejection.rejection_id!r} already exists. "
                    "Rejections are append-only; emit a new rejection_id "
                    "for any new judgment on the same email."
                )
            _persist_rejection(
                conn,
                rejection_id=rejection.rejection_id,
                source_email_id=rejection.source_email_id,
                source_thread_id=rejection.source_thread_id,
                sender_email=rejection.sender_email,
                subject=rejection.subject,
                reason_code=rejection.reason_code,
                reason_detail=rejection.reason_detail,
                raw_excerpt=rejection.raw_excerpt,
                received_at=received_at_iso,
                rejected_at=rejected_at_iso,
                parser_version=rejection.parser_version,
            )

    return {
        "rejection_id": rejection.rejection_id,
        "rejected_at": rejected_at_iso,
        "reason_code": rejection.reason_code,
    }


def _persist_rejection(
    conn: sqlite3.Connection,
    *,
    rejection_id: str,
    source_email_id: str | None,
    source_thread_id: str | None,
    sender_email: str | None,
    subject: str | None,
    reason_code: str,
    reason_detail: str | None,
    raw_excerpt: str | None,
    received_at: str | None,
    rejected_at: str,
    parser_version: str,
) -> None:
    """THE ONLY function in src/spine/ that writes spine_ingest_rejections.

    Mirror of _persist_contract. Adding any other INSERT/UPDATE/DELETE
    against spine_ingest_rejections inside src/spine/ is a substrate
    regression (caught by test_one_writer.py).
    """
    conn.execute(
        "INSERT INTO spine_ingest_rejections "
        "(rejection_id, source_email_id, source_thread_id, sender_email, "
        " subject, reason_code, reason_detail, raw_excerpt, "
        " received_at, rejected_at, parser_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (rejection_id, source_email_id, source_thread_id, sender_email,
         subject, reason_code, reason_detail, raw_excerpt,
         received_at, rejected_at, parser_version),
    )


def latest_rejections(
    db_path: str | Path,
    *,
    limit: int = 50,
    reason_code: str | None = None,
) -> list[dict]:
    """Return recent rejections newest-first. Optional reason_code filter.

    Read-side surface for the /queue/rejected route and the
    Telegram missed-bid watcher (queued). Returns plain dicts —
    rehydration into IngestRejection is the caller's choice.
    """
    if not isinstance(limit, int) or limit < 1 or limit > 1000:
        raise SpineValidationError(
            f"limit must be 1..1000; got {limit!r}"
        )
    sql = (
        "SELECT rejection_id, source_email_id, source_thread_id, "
        "       sender_email, subject, reason_code, reason_detail, "
        "       raw_excerpt, received_at, rejected_at, parser_version "
        "FROM spine_ingest_rejections "
    )
    params: tuple = ()
    if reason_code is not None:
        sql += "WHERE reason_code = ? "
        params = (reason_code,)
    sql += "ORDER BY rejected_at DESC LIMIT ?"
    params = params + (limit,)

    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


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


# ──────────────────────────────────────────────────────────────────────
# Sequential counters — THE ONLY WRITER for spine_counters.
# ──────────────────────────────────────────────────────────────────────

COUNTER_MAX_JUMP = 5


def next_value(
    db_path: str | Path,
    counter_name: str,
    *,
    actor: str,
) -> int:
    """Atomically increment `counter_name` and return the new value.

    First call for a never-seen counter returns 1. Subsequent calls
    return prior + 1. The read-modify-write happens under _WRITE_LOCK
    so concurrent callers receive distinct sequential values.

    Args:
        db_path:      SQLite database path.
        counter_name: Stable key — e.g., "pc_2026", "rfq_2026",
                      "quote_2026". Year-namespacing is the caller's
                      responsibility; this layer treats the name as
                      an opaque string.
        actor:        Who triggered the increment ("spine_ingest",
                      "operator", a username). Recorded for audit.

    Returns:
        The newly-assigned integer (>= 1).
    """
    if not counter_name or not counter_name.strip():
        raise SpineValidationError("next_value requires non-empty counter_name.")
    if not actor or not actor.strip():
        raise SpineValidationError("next_value requires non-empty actor.")

    now_iso = datetime.now(timezone.utc).isoformat()

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT current_value FROM spine_counters WHERE counter_name = ?",
                (counter_name.strip(),),
            ).fetchone()
            prior = int(row["current_value"]) if row is not None else 0
            new_value = prior + 1
            _persist_counter(
                conn,
                counter_name=counter_name.strip(),
                current_value=new_value,
                last_set_at=now_iso,
                last_actor=actor.strip(),
            )

    return new_value


def get_counter(db_path: str | Path, counter_name: str) -> int | None:
    """Return the current value of a counter, or None if never set.

    Pure read; no mutation, no lock. Callers that care about a
    "what would the next value be?" preview can use this + 1, but
    must not act on the preview — only `next_value` is atomic.
    """
    if not counter_name or not counter_name.strip():
        raise SpineValidationError("get_counter requires non-empty counter_name.")
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT current_value FROM spine_counters WHERE counter_name = ?",
            (counter_name.strip(),),
        ).fetchone()
    if row is None:
        return None
    return int(row["current_value"])


def set_counter(
    db_path: str | Path,
    counter_name: str,
    value: int,
    *,
    actor: str,
) -> None:
    """Manually set a counter to `value`. Operator escape hatch.

    Mirrors the CLAUDE.md quote_counter rule: a manual set is
    authoritative, but the writer refuses jumps greater than
    COUNTER_MAX_JUMP (=5) above the current value to catch
    fat-finger errors that would burn an entire numbering block.
    Setting BELOW the current value is allowed (back-correction
    of a bad increment) and is recorded in last_actor for audit.

    Args:
        db_path:      SQLite path.
        counter_name: Counter key.
        value:        New current_value. Must be >= 0.
        actor:        Who is forcing the value. Required for audit.

    Raises:
        SpineValidationError: value < 0, jump > 5, or empty actor.
    """
    if not counter_name or not counter_name.strip():
        raise SpineValidationError("set_counter requires non-empty counter_name.")
    if not actor or not actor.strip():
        raise SpineValidationError("set_counter requires non-empty actor.")
    if not isinstance(value, int) or value < 0:
        raise SpineValidationError(
            f"set_counter value must be a non-negative int; got {value!r}"
        )

    now_iso = datetime.now(timezone.utc).isoformat()

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT current_value FROM spine_counters WHERE counter_name = ?",
                (counter_name.strip(),),
            ).fetchone()
            prior = int(row["current_value"]) if row is not None else 0
            if value > prior + COUNTER_MAX_JUMP:
                raise SpineValidationError(
                    f"set_counter refused: {counter_name!r} jump "
                    f"{prior} -> {value} exceeds max_jump={COUNTER_MAX_JUMP}. "
                    "Apply smaller increments or audit the request."
                )
            _persist_counter(
                conn,
                counter_name=counter_name.strip(),
                current_value=value,
                last_set_at=now_iso,
                last_actor=actor.strip(),
            )


def _persist_counter(
    conn: sqlite3.Connection,
    *,
    counter_name: str,
    current_value: int,
    last_set_at: str,
    last_actor: str | None,
) -> None:
    """THE ONLY function in src/spine/ that writes spine_counters.

    Mirror of _persist_state / _persist_snapshot / _persist_contract /
    _persist_rejection. Adding any other INSERT/UPDATE/DELETE against
    spine_counters inside src/spine/ is a substrate regression caught
    by test_one_writer.py.
    """
    conn.execute(
        "INSERT OR REPLACE INTO spine_counters "
        "(counter_name, current_value, last_set_at, last_actor) "
        "VALUES (?, ?, ?, ?)",
        (counter_name, current_value, last_set_at, last_actor),
    )


# ──────────────────────────────────────────────────────────────────────
# Quote links — THE ONLY WRITER for spine_quote_links.
# ──────────────────────────────────────────────────────────────────────

# Confidence values are bounded [0.0, 1.0]. Operator-asserted manual
# links use AUTO_LINK_OPERATOR_CONFIDENCE; the matcher sets its own
# value derived from the evidence. The boundary itself is enforced at
# the writer.
LINK_CONFIDENCE_MIN = 0.0
LINK_CONFIDENCE_MAX = 1.0
AUTO_LINK_OPERATOR_CONFIDENCE = 1.0


def write_quote_link(
    db_path: str | Path,
    *,
    from_quote_id: str,
    to_quote_id: str,
    match_method: str,
    confidence: float,
    evidence: dict | None = None,
    actor: str,
) -> dict:
    """Persist a directional link between two quotes. Append-only.

    A link records "from_quote_id has a relationship to to_quote_id of
    the given match_method with the given confidence". The canonical
    use is PC predecessor (`to`) ← RFQ rebid (`from`). Operators may
    add multiple links per `from` (primary + alternates); reads sort
    by confidence DESC.

    Args:
        db_path:        SQLite path.
        from_quote_id:  Quote.quote_id of the newer record (e.g., RFQ).
        to_quote_id:    Quote.quote_id of the prior record (e.g., PC).
                        MUST NOT equal from_quote_id (self-link refused).
        match_method:   Short tag — "auto_mfg_desc", "operator_manual",
                        "auto_solicitation_match", etc. Free-form so
                        new matchers can declare their kind.
        confidence:     0.0..1.0. The matcher's confidence; 1.0 reserved
                        for operator-asserted ground truth.
        evidence:       Structured JSON-serializable dict explaining the
                        decision. Stored verbatim for audit.
        actor:          Who wrote the link. "spine_auto_linker",
                        "operator:<user>", etc.

    Returns:
        dict with link_id, linked_at.

    Raises:
        SpineValidationError on invalid inputs (self-link, bad
        confidence, empty actor/method, missing quote_ids).
    """
    if not from_quote_id or not from_quote_id.strip():
        raise SpineValidationError("write_quote_link requires from_quote_id.")
    if not to_quote_id or not to_quote_id.strip():
        raise SpineValidationError("write_quote_link requires to_quote_id.")
    if from_quote_id.strip() == to_quote_id.strip():
        raise SpineValidationError(
            f"write_quote_link refused: self-link "
            f"({from_quote_id!r} → itself) is meaningless."
        )
    if not match_method or not match_method.strip():
        raise SpineValidationError("write_quote_link requires match_method.")
    if not actor or not actor.strip():
        raise SpineValidationError("write_quote_link requires actor.")
    if not isinstance(confidence, (int, float)):
        raise SpineValidationError(
            f"write_quote_link confidence must be number; got {type(confidence).__name__}"
        )
    if confidence < LINK_CONFIDENCE_MIN or confidence > LINK_CONFIDENCE_MAX:
        raise SpineValidationError(
            f"write_quote_link confidence must be in "
            f"[{LINK_CONFIDENCE_MIN}, {LINK_CONFIDENCE_MAX}]; got {confidence}"
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    evidence_dict = evidence if evidence is not None else {}
    evidence_str = json.dumps(evidence_dict, sort_keys=True)

    # Deterministic link_id derived from (from, to, method) so the same
    # matcher running twice on the same pair doesn't duplicate. Operator
    # re-confirmation of an existing auto-link no-ops.
    payload = f"{from_quote_id.strip()}|{to_quote_id.strip()}|{match_method.strip()}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    link_id = f"link_{digest}"

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            existing = conn.execute(
                "SELECT linked_at FROM spine_quote_links WHERE link_id = ?",
                (link_id,),
            ).fetchone()
            if existing:
                return {"link_id": link_id, "linked_at": existing["linked_at"], "duplicate": True}
            _persist_link(
                conn,
                link_id=link_id,
                from_quote_id=from_quote_id.strip(),
                to_quote_id=to_quote_id.strip(),
                match_method=match_method.strip(),
                confidence=float(confidence),
                evidence_json=evidence_str,
                linked_at=now_iso,
                actor=actor.strip(),
            )

    return {"link_id": link_id, "linked_at": now_iso, "duplicate": False}


def _persist_link(
    conn: sqlite3.Connection,
    *,
    link_id: str,
    from_quote_id: str,
    to_quote_id: str,
    match_method: str,
    confidence: float,
    evidence_json: str,
    linked_at: str,
    actor: str,
) -> None:
    """THE ONLY function in src/spine/ that writes spine_quote_links.

    Mirror of _persist_state / _persist_snapshot / _persist_contract /
    _persist_rejection / _persist_counter. Substrate convention.
    """
    conn.execute(
        "INSERT INTO spine_quote_links "
        "(link_id, from_quote_id, to_quote_id, match_method, confidence, "
        " evidence_json, linked_at, actor) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (link_id, from_quote_id, to_quote_id, match_method, confidence,
         evidence_json, linked_at, actor),
    )


def find_links_from(db_path: str | Path, from_quote_id: str) -> list[dict]:
    """Return all links originating from `from_quote_id`, highest
    confidence first. Plain dicts — caller decides what to do with
    them. Empty list if no links.

    Used by editor surfaces ("this RFQ links to PC R26PC####") and by
    the auto-price substrate (queued PR — pull validated costs from
    the top-confidence linked PC)."""
    if not from_quote_id or not from_quote_id.strip():
        raise SpineValidationError("find_links_from requires from_quote_id.")
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT link_id, from_quote_id, to_quote_id, match_method, "
            "       confidence, evidence_json, linked_at, actor "
            "FROM spine_quote_links WHERE from_quote_id = ? "
            "ORDER BY confidence DESC, linked_at DESC",
            (from_quote_id.strip(),),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        try:
            d["evidence"] = json.loads(d["evidence_json"])
        except Exception:
            d["evidence"] = {}
        out.append(d)
    return out


def find_links_to(db_path: str | Path, to_quote_id: str) -> list[dict]:
    """Return all links pointing TO `to_quote_id`. Mirror of
    find_links_from for the reverse direction — "which RFQs reference
    this PC?". Same sort + shape."""
    if not to_quote_id or not to_quote_id.strip():
        raise SpineValidationError("find_links_to requires to_quote_id.")
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT link_id, from_quote_id, to_quote_id, match_method, "
            "       confidence, evidence_json, linked_at, actor "
            "FROM spine_quote_links WHERE to_quote_id = ? "
            "ORDER BY confidence DESC, linked_at DESC",
            (to_quote_id.strip(),),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        try:
            d["evidence"] = json.loads(d["evidence_json"])
        except Exception:
            d["evidence"] = {}
        out.append(d)
    return out
