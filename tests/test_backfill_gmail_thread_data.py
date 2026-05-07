"""Contract tests for the dual-column Gmail backfill in
scripts/backfill_email_thread_id.py.

Original PR-B1 (2026-05-01) backfilled `email_thread_id`. PR-C of the
thread-aware-ingest arc (2026-05-07, this PR) extended the same script
to ALSO seed `gmail_message_ids = [email_uid]` on historical records,
because PR #808 added the column with default '[]' and forward-only
ingest population — leaving every pre-#808 record stuck at empty list.

Contracts pinned here:

  * A record with email_thread_id set BUT empty gmail_message_ids is
    still scanned (mid-state from PR-B1 → #808 transition).
  * A record with both already filled is skipped.
  * Apply writes BOTH columns, idempotently.
  * Dry-run writes nothing.
  * Per-record report flags `filled_thread_id` / `filled_message_ids`
    so operators can see which fields each row got.
  * `_apply_one` never overwrites an existing thread_id or message_ids
    list (defensive — even if scan logic is wrong, the writer is safe).
"""
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module")
def mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "backfill_email_thread_id" in sys.modules:
        del sys.modules["backfill_email_thread_id"]
    return importlib.import_module("backfill_email_thread_id")


def _seed_db(db_path: str) -> None:
    """Seed a DB with both new columns present (post-#808 schema)."""
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY,
            email_thread_id TEXT DEFAULT '',
            gmail_message_ids TEXT DEFAULT '[]',
            data_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            email_thread_id TEXT DEFAULT '',
            gmail_message_ids TEXT DEFAULT '[]',
            data_json TEXT,
            updated_at TEXT
        );
    """)
    conn.commit()
    conn.close()


def _insert(conn: sqlite3.Connection, *, kind: str, rid: str,
            email_uid: str = "", thread_id: str = "",
            message_ids: list | None = None) -> None:
    """Insert a record with the given column values; data_json blob mirrors."""
    table = "price_checks" if kind == "pc" else "rfqs"
    blob = {"email_uid": email_uid}
    if thread_id:
        blob["email_thread_id"] = thread_id
    if message_ids is not None:
        blob["gmail_message_ids"] = message_ids
    conn.execute(
        f"INSERT INTO {table} (id, email_thread_id, gmail_message_ids, "
        f"data_json) VALUES (?, ?, ?, ?)",
        (rid, thread_id, json.dumps(message_ids or []),
         json.dumps(blob)),
    )


def _read(conn: sqlite3.Connection, *, kind: str, rid: str) -> sqlite3.Row:
    table = "price_checks" if kind == "pc" else "rfqs"
    conn.row_factory = sqlite3.Row
    return conn.execute(
        f"SELECT * FROM {table} WHERE id=?", (rid,)
    ).fetchone()


# ─── _scan ──────────────────────────────────────────────────────────────


def test_scan_picks_up_record_missing_both_columns(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="GMAIL-MSG-1")
    conn.commit()

    candidates = mod._scan(conn, "rfq")
    conn.close()

    assert len(candidates) == 1
    assert candidates[0]["needs_thread_id"] is True
    assert candidates[0]["needs_message_ids"] is True


def test_scan_picks_up_record_with_thread_but_empty_message_ids(tmp_path, mod):
    """Mid-state: PR-B1 already backfilled thread_id, but #808's column
    wasn't backfilled. The scan must catch this row."""
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1",
            email_uid="GMAIL-MSG-1",
            thread_id="THREAD-X",
            message_ids=[])
    conn.commit()

    candidates = mod._scan(conn, "rfq")
    conn.close()

    assert len(candidates) == 1
    assert candidates[0]["needs_thread_id"] is False
    assert candidates[0]["needs_message_ids"] is True


def test_scan_skips_fully_populated_record(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1",
            email_uid="GMAIL-MSG-1",
            thread_id="THREAD-X",
            message_ids=["GMAIL-MSG-1"])
    conn.commit()

    assert mod._scan(conn, "rfq") == []
    conn.close()


def test_scan_skips_record_with_no_lookup_keys(tmp_path, mod):
    """No email_uid AND no rfc822 message-id → cannot resolve → skip."""
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="")
    conn.commit()

    assert mod._scan(conn, "rfq") == []
    conn.close()


# ─── _apply_one ─────────────────────────────────────────────────────────


def test_apply_writes_both_columns(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="GMAIL-MSG-1")
    conn.commit()

    candidates = mod._scan(conn, "rfq")
    mod._apply_one(conn, candidates[0],
                   thread_id="THREAD-X", resolved_gmail_id="GMAIL-MSG-1")
    conn.commit()

    row = _read(conn, kind="rfq", rid="r1")
    assert row["email_thread_id"] == "THREAD-X"
    assert json.loads(row["gmail_message_ids"]) == ["GMAIL-MSG-1"]
    blob = json.loads(row["data_json"])
    assert blob["email_thread_id"] == "THREAD-X"
    assert blob["gmail_message_ids"] == ["GMAIL-MSG-1"]
    assert "email_thread_id_backfilled_at" in blob
    assert "gmail_message_ids_backfilled_at" in blob
    conn.close()


def test_apply_writes_only_message_ids_when_thread_already_set(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1",
            email_uid="GMAIL-MSG-1",
            thread_id="EXISTING-THREAD",
            message_ids=[])
    conn.commit()

    candidates = mod._scan(conn, "rfq")
    mod._apply_one(conn, candidates[0],
                   thread_id="THREAD-Y", resolved_gmail_id="GMAIL-MSG-1")
    conn.commit()

    row = _read(conn, kind="rfq", rid="r1")
    # thread_id NOT overwritten — needs_thread_id was False
    assert row["email_thread_id"] == "EXISTING-THREAD"
    # message_ids written
    assert json.loads(row["gmail_message_ids"]) == ["GMAIL-MSG-1"]
    conn.close()


def test_apply_idempotent_second_run_skips(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="GMAIL-MSG-1")
    conn.commit()

    # First pass
    candidates = mod._scan(conn, "rfq")
    mod._apply_one(conn, candidates[0],
                   thread_id="THREAD-X", resolved_gmail_id="GMAIL-MSG-1")
    conn.commit()

    # Second pass — scan should now return empty
    assert mod._scan(conn, "rfq") == []
    conn.close()


# ─── run() integration ──────────────────────────────────────────────────


def test_run_dry_run_writes_nothing(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="GMAIL-MSG-1")
    conn.commit()
    conn.close()

    fake_meta = MagicMock(return_value={"thread_id": "THREAD-X"})
    fake_service = MagicMock()
    with patch("src.core.gmail_api.is_configured", return_value=True), \
         patch("src.core.gmail_api.get_service", return_value=fake_service), \
         patch("src.core.gmail_api.get_message_metadata", fake_meta):
        result = mod.run(db, apply=False)

    assert result["ok"]
    assert result["mode"] == "dry-run"
    assert result["flipped"] == 1
    assert result["message_ids_filled"] == 1
    # Verify nothing was actually written
    conn = sqlite3.connect(db)
    row = _read(conn, kind="rfq", rid="r1")
    assert row["email_thread_id"] == ""
    assert json.loads(row["gmail_message_ids"]) == []
    conn.close()


def test_run_apply_writes_both_columns(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="GMAIL-MSG-1")
    _insert(conn, kind="pc", rid="p1", email_uid="GMAIL-MSG-2")
    conn.commit()
    conn.close()

    def fake_meta(_service, gmail_id):
        return {"thread_id": f"THREAD-{gmail_id[-1]}"}

    fake_service = MagicMock()
    with patch("src.core.gmail_api.is_configured", return_value=True), \
         patch("src.core.gmail_api.get_service", return_value=fake_service), \
         patch("src.core.gmail_api.get_message_metadata", side_effect=fake_meta):
        result = mod.run(db, apply=True)

    assert result["ok"]
    assert result["mode"] == "apply"
    assert result["flipped"] == 2
    assert result["message_ids_filled"] == 2

    conn = sqlite3.connect(db)
    r1 = _read(conn, kind="rfq", rid="r1")
    assert r1["email_thread_id"] == "THREAD-1"
    assert json.loads(r1["gmail_message_ids"]) == ["GMAIL-MSG-1"]
    p1 = _read(conn, kind="pc", rid="p1")
    assert p1["email_thread_id"] == "THREAD-2"
    assert json.loads(p1["gmail_message_ids"]) == ["GMAIL-MSG-2"]
    conn.close()


def test_run_only_filter_pc(tmp_path, mod):
    """--only pc must skip the rfqs table entirely."""
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="rfq", rid="r1", email_uid="GMAIL-MSG-1")
    _insert(conn, kind="pc", rid="p1", email_uid="GMAIL-MSG-2")
    conn.commit()
    conn.close()

    fake_service = MagicMock()
    with patch("src.core.gmail_api.is_configured", return_value=True), \
         patch("src.core.gmail_api.get_service", return_value=fake_service), \
         patch("src.core.gmail_api.get_message_metadata",
               return_value={"thread_id": "T"}):
        result = mod.run(db, apply=True, only="pc")

    assert result["ok"]
    assert result["total_found"] == 1
    conn = sqlite3.connect(db)
    # rfq untouched
    r1 = _read(conn, kind="rfq", rid="r1")
    assert r1["email_thread_id"] == ""
    # pc filled
    p1 = _read(conn, kind="pc", rid="p1")
    assert p1["email_thread_id"] == "T"
    conn.close()


def test_run_max_caps_records(tmp_path, mod):
    db = str(tmp_path / "test.db")
    _seed_db(db)
    conn = sqlite3.connect(db)
    for i in range(5):
        _insert(conn, kind="rfq", rid=f"r{i}", email_uid=f"GMAIL-{i}")
    conn.commit()
    conn.close()

    fake_service = MagicMock()
    with patch("src.core.gmail_api.is_configured", return_value=True), \
         patch("src.core.gmail_api.get_service", return_value=fake_service), \
         patch("src.core.gmail_api.get_message_metadata",
               return_value={"thread_id": "T"}):
        result = mod.run(db, apply=True, max_records=3)

    assert result["ok"]
    assert result["total_found"] == 5
    assert result["capped_at"] == 3
    assert len(result["records"]) == 3
