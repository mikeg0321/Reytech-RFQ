"""Contract tests for thread-duplicate dismissal substrate (PR-D).

Pins the four pieces of PR-D in lockstep:

  1. `is_thread_duplicate(record)` — Python predicate matches the
     gmail_thread_duplicate_of column on either the row or the blob.
  2. `is_active_queue(record)` — returns False when the record is a
     thread duplicate, even if status is otherwise "active".
  3. `SQL_ACTIVE_QUEUE_FRAGMENT` — has the
     `gmail_thread_duplicate_of = ''` clause.
  4. Migration 40 — recreated views exclude rows where the column is
     non-empty (and still exclude is_test + dismissed states).
  5. `scripts/dismiss_thread_duplicates.py` — sets the column +
     audit-log entry; idempotent; never runs without `--apply`.

Reytech Law 22 doctrine: dismissal preserves the record (data + items
+ attachments) so audit / training / future-replay can use it. This
suite verifies the dismissed row is still readable from the base table,
just absent from the queue view.
"""
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys

import pytest


@pytest.fixture(scope="module")
def script_mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "dismiss_thread_duplicates" in sys.modules:
        del sys.modules["dismiss_thread_duplicates"]
    return importlib.import_module("dismiss_thread_duplicates")


# ─── Python predicate ────────────────────────────────────────────────


def test_is_thread_duplicate_true_when_column_set():
    from src.core.canonical_state import is_thread_duplicate
    assert is_thread_duplicate({"gmail_thread_duplicate_of": "rfq_a5b09b56"})


def test_is_thread_duplicate_false_when_empty():
    from src.core.canonical_state import is_thread_duplicate
    assert not is_thread_duplicate({"gmail_thread_duplicate_of": ""})
    assert not is_thread_duplicate({})
    assert not is_thread_duplicate({"gmail_thread_duplicate_of": None})


def test_is_thread_duplicate_strips_whitespace_only_values():
    from src.core.canonical_state import is_thread_duplicate
    assert not is_thread_duplicate({"gmail_thread_duplicate_of": "   "})


def test_is_active_queue_excludes_thread_duplicates():
    from src.core.canonical_state import is_active_queue
    record = {
        "status": "draft",  # otherwise active
        "is_test": False,
        "gmail_thread_duplicate_of": "rfq_parent",
    }
    assert not is_active_queue(record)


def test_is_active_queue_unaffected_when_dup_of_empty():
    from src.core.canonical_state import is_active_queue
    record = {
        "status": "draft",
        "is_test": False,
        "gmail_thread_duplicate_of": "",
    }
    assert is_active_queue(record)


# ─── SQL fragment ────────────────────────────────────────────────────


def test_active_queue_sql_fragment_excludes_duplicates():
    from src.core.canonical_state import SQL_ACTIVE_QUEUE_FRAGMENT
    assert "gmail_thread_duplicate_of" in SQL_ACTIVE_QUEUE_FRAGMENT
    assert "= ''" in SQL_ACTIVE_QUEUE_FRAGMENT


def test_active_queue_sql_clause_runs_against_real_db(tmp_path):
    """Belt-and-braces: build a DB with the post-#808 schema, insert
    one active + one dismissed-by-thread row, run the clause, expect
    only the active row."""
    from src.core.canonical_state import active_queue_sql_clause
    db = str(tmp_path / "t.db")
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            status TEXT,
            is_test INTEGER DEFAULT 0,
            gmail_thread_duplicate_of TEXT DEFAULT ''
        )
    """)
    conn.execute("INSERT INTO rfqs VALUES ('a', 'draft', 0, '')")
    conn.execute("INSERT INTO rfqs VALUES ('b', 'draft', 0, 'rfq_parent')")
    conn.commit()

    clause, params = active_queue_sql_clause()
    rows = conn.execute(
        f"SELECT id FROM rfqs WHERE {clause}", params
    ).fetchall()
    conn.close()

    assert {r[0] for r in rows} == {"a"}


# ─── Migration 40 ────────────────────────────────────────────────────


def _seed_post_808_schema(db_path: str) -> None:
    """Build a DB with the columns + views needed for migration 40
    to recreate, then run migrations 39 and 40 to install the new view.
    """
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            status TEXT DEFAULT 'draft',
            data_json TEXT DEFAULT '{}',
            gmail_thread_duplicate_of TEXT DEFAULT '',
            updated_at TEXT
        );
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY,
            status TEXT DEFAULT 'draft',
            data_json TEXT DEFAULT '{}',
            gmail_thread_duplicate_of TEXT DEFAULT '',
            updated_at TEXT
        );
    """)
    # Replay just migrations 39 and 40 — the rest aren't needed for
    # this isolated view-shape test.
    from src.core.migrations import MIGRATIONS
    for num, _name, sql in MIGRATIONS:
        if num in (39, 40):
            conn.executescript(sql)
    conn.commit()
    conn.close()


def test_migration_40_recreates_view_excluding_thread_duplicates(tmp_path):
    db = str(tmp_path / "t.db")
    _seed_post_808_schema(db)

    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO rfqs (id, status, gmail_thread_duplicate_of) "
                 "VALUES ('a', 'draft', '')")
    conn.execute("INSERT INTO rfqs (id, status, gmail_thread_duplicate_of) "
                 "VALUES ('b', 'draft', 'rfq_parent')")
    conn.execute("INSERT INTO rfqs (id, status, gmail_thread_duplicate_of) "
                 "VALUES ('c', 'sent', '')")  # excluded by status
    conn.commit()

    rows = conn.execute("SELECT id FROM v_active_queue_rfqs").fetchall()
    assert {r[0] for r in rows} == {"a"}

    # Same shape on PCs.
    conn.execute("INSERT INTO price_checks "
                 "(id, status, gmail_thread_duplicate_of) "
                 "VALUES ('p1', 'draft', '')")
    conn.execute("INSERT INTO price_checks "
                 "(id, status, gmail_thread_duplicate_of) "
                 "VALUES ('p2', 'draft', 'rfq_parent')")
    conn.commit()
    rows = conn.execute("SELECT id FROM v_active_queue_pcs").fetchall()
    assert {r[0] for r in rows} == {"p1"}

    # And the dismissed row is still readable from the base table —
    # Reytech Law 22 says "never delete buyer-source data".
    rows = conn.execute("SELECT id FROM rfqs").fetchall()
    assert {r[0] for r in rows} == {"a", "b", "c"}
    conn.close()


# ─── dismiss_thread_duplicates.py CLI ────────────────────────────────


def _seed_dismissal_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            data_json TEXT DEFAULT '{}',
            gmail_thread_duplicate_of TEXT DEFAULT '',
            updated_at TEXT
        );
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY,
            data_json TEXT DEFAULT '{}',
            gmail_thread_duplicate_of TEXT DEFAULT '',
            updated_at TEXT
        );
    """)
    conn.commit()
    conn.close()


def _insert(conn, *, kind, rid):
    table = "price_checks" if kind == "pc" else "rfqs"
    conn.execute(f"INSERT INTO {table} (id, data_json) VALUES (?, ?)",
                 (rid, "{}"))


def _read(conn, *, kind, rid):
    table = "price_checks" if kind == "pc" else "rfqs"
    conn.row_factory = sqlite3.Row
    return conn.execute(
        f"SELECT * FROM {table} WHERE id=?", (rid,)
    ).fetchone()


def test_dismiss_dry_run_writes_nothing(tmp_path, script_mod):
    db = str(tmp_path / "t.db")
    _seed_dismissal_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="pc", rid="pc_dup")
    _insert(conn, kind="rfq", rid="rfq_parent")
    conn.commit()
    conn.close()

    pairs_csv = str(tmp_path / "pairs.csv")
    with open(pairs_csv, "w", encoding="utf-8") as f:
        f.write("pc,pc_dup,rfq_parent,test reason\n")

    result = script_mod.run(db, apply=False, pairs_csv=pairs_csv)
    assert result["ok"]
    assert result["mode"] == "dry-run"
    assert result["records"][0]["status"] == "dismissed"

    conn = sqlite3.connect(db)
    row = _read(conn, kind="pc", rid="pc_dup")
    # Dry-run rolled back — column still empty
    assert (row["gmail_thread_duplicate_of"] or "") == ""
    conn.close()


def test_dismiss_apply_sets_column_and_audit(tmp_path, script_mod):
    db = str(tmp_path / "t.db")
    _seed_dismissal_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="pc", rid="pc_dup")
    _insert(conn, kind="rfq", rid="rfq_parent")
    conn.commit()
    conn.close()

    pairs_csv = str(tmp_path / "pairs.csv")
    with open(pairs_csv, "w", encoding="utf-8") as f:
        f.write("pc,pc_dup,rfq_parent,buyer reply mid-quote\n")

    result = script_mod.run(db, apply=True, pairs_csv=pairs_csv)
    assert result["ok"]
    assert result["records"][0]["status"] == "dismissed"

    conn = sqlite3.connect(db)
    row = _read(conn, kind="pc", rid="pc_dup")
    assert row["gmail_thread_duplicate_of"] == "rfq_parent"
    blob = json.loads(row["data_json"])
    assert blob["gmail_thread_duplicate_of"] == "rfq_parent"
    assert blob["gmail_thread_duplicate_reason"] == "buyer reply mid-quote"
    assert "gmail_thread_duplicated_at" in blob
    audit = blob.get("audit_log", [])
    assert len(audit) == 1
    assert audit[0]["action"] == "thread-duplicate-dismiss"
    assert audit[0]["parent_id"] == "rfq_parent"
    conn.close()


def test_dismiss_idempotent_second_run_skips(tmp_path, script_mod):
    db = str(tmp_path / "t.db")
    _seed_dismissal_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="pc", rid="pc_dup")
    _insert(conn, kind="rfq", rid="rfq_parent")
    conn.commit()
    conn.close()

    pairs_csv = str(tmp_path / "pairs.csv")
    with open(pairs_csv, "w", encoding="utf-8") as f:
        f.write("pc,pc_dup,rfq_parent,reason\n")

    # First run dismisses
    script_mod.run(db, apply=True, pairs_csv=pairs_csv)
    # Second run should report already-dismissed
    result = script_mod.run(db, apply=True, pairs_csv=pairs_csv)
    assert result["records"][0]["status"] == "already-dismissed"


def test_dismiss_reports_missing_duplicate(tmp_path, script_mod):
    db = str(tmp_path / "t.db")
    _seed_dismissal_db(db)
    # No records inserted

    pairs_csv = str(tmp_path / "pairs.csv")
    with open(pairs_csv, "w", encoding="utf-8") as f:
        f.write("pc,pc_does_not_exist,rfq_parent,reason\n")

    result = script_mod.run(db, apply=True, pairs_csv=pairs_csv)
    assert result["records"][0]["status"] == "duplicate-not-found"


def test_dismiss_reports_missing_parent(tmp_path, script_mod):
    db = str(tmp_path / "t.db")
    _seed_dismissal_db(db)
    conn = sqlite3.connect(db)
    _insert(conn, kind="pc", rid="pc_dup")
    # parent missing
    conn.commit()
    conn.close()

    pairs_csv = str(tmp_path / "pairs.csv")
    with open(pairs_csv, "w", encoding="utf-8") as f:
        f.write("pc,pc_dup,rfq_does_not_exist,reason\n")

    result = script_mod.run(db, apply=True, pairs_csv=pairs_csv)
    assert result["records"][0]["status"] == "parent-not-found"


def test_dismiss_missing_db_returns_error(tmp_path, script_mod):
    bogus = str(tmp_path / "missing.db")
    result = script_mod.run(bogus, apply=False)
    assert not result["ok"]
    assert "DB not found" in result["error"]
