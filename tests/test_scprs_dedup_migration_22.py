"""
Regression test for migration 22 (scprs_po_lines dedup + UNIQUE INDEX).

Why this exists:
  scprs_universal_pull.run_universal_pull was issuing
    INSERT OR REPLACE INTO scprs_po_lines ... VALUES (...)
  against a table that had only `id` as PRIMARY KEY. With no UNIQUE on
  (po_id, line_num), every re-pull duplicated the line items. Over the
  7-week window when the dashboard was silently broken, the DB
  accumulated unknown numbers of duplicate rows, inflating gap_spend /
  win_back_spend / by_agency totals.

  Migration 22 deduplicates existing rows (keeping the most recent) and
  adds the UNIQUE INDEX so future re-pulls actually upsert.

  These tests prove:
    1. The migration removes duplicates, keeping the highest id per
       (po_id, line_num).
    2. The UNIQUE INDEX prevents new duplicates from being inserted
       (INSERT OR REPLACE behaves as upsert; raw INSERT raises).
    3. The migration is idempotent — second run is a no-op.
    4. The migration is safe on a fresh DB with no scprs_po_lines table.
    5. _ensure_schema on a fresh DB also installs the UNIQUE INDEX
       (so new installs don't have to wait for migration 22 to fire).
"""
import os
import sqlite3

import pytest


def _make_db_with_dupes(db_path):
    """Create scprs_po_lines (without UNIQUE) and seed dupes."""
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER, po_number TEXT, line_num INTEGER,
            description TEXT, unit_price REAL, line_total REAL,
            reytech_sells INTEGER DEFAULT 0, opportunity_flag TEXT
        );
    """)
    # PO 1 line 0: 3 dupes (ids 1,2,3) — keep id=3
    # PO 1 line 1: 2 dupes (ids 4,5) — keep id=5
    # PO 2 line 0: 1 row (id 6) — keep
    # PO 2 line 1: 4 dupes (ids 7,8,9,10) — keep id=10
    rows = [
        (1, "PO-1", 0, "gloves",   8.00, 400.0, 1, "WIN_BACK"),
        (1, "PO-1", 0, "gloves",   8.00, 400.0, 1, "WIN_BACK"),
        (1, "PO-1", 0, "gloves",   8.00, 400.0, 1, "WIN_BACK"),
        (1, "PO-1", 1, "wipes",    5.00,  50.0, 0, "GAP_ITEM"),
        (1, "PO-1", 1, "wipes",    5.00,  50.0, 0, "GAP_ITEM"),
        (2, "PO-2", 0, "masks",    6.00, 600.0, 1, "WIN_BACK"),
        (2, "PO-2", 1, "abd pads", 0.40,   8.0, 0, "GAP_ITEM"),
        (2, "PO-2", 1, "abd pads", 0.40,   8.0, 0, "GAP_ITEM"),
        (2, "PO-2", 1, "abd pads", 0.40,   8.0, 0, "GAP_ITEM"),
        (2, "PO-2", 1, "abd pads", 0.40,   8.0, 0, "GAP_ITEM"),
    ]
    conn.executemany(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description, "
        "unit_price, line_total, reytech_sells, opportunity_flag) "
        "VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn


def test_migration_22_dedups_and_keeps_highest_id(tmp_path):
    db_path = str(tmp_path / "scprs_iso_a" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_db_with_dupes(db_path)
    assert conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0] == 10

    from src.core.migrations import _run_migration_22
    _run_migration_22(conn)
    conn.commit()

    # 4 distinct (po_id, line_num) combos remain.
    surviving = conn.execute(
        "SELECT id, po_id, line_num FROM scprs_po_lines ORDER BY id"
    ).fetchall()
    assert len(surviving) == 4
    # Highest id per group kept.
    by_key = {(po_id, line_num): _id for (_id, po_id, line_num) in surviving}
    assert by_key == {(1, 0): 3, (1, 1): 5, (2, 0): 6, (2, 1): 10}
    conn.close()


def test_migration_22_installs_unique_index_blocking_new_dupes(tmp_path):
    db_path = str(tmp_path / "scprs_iso_b" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_db_with_dupes(db_path)
    from src.core.migrations import _run_migration_22
    _run_migration_22(conn)
    conn.commit()

    # Raw INSERT of a duplicate (po_id, line_num) MUST raise — proves
    # the UNIQUE INDEX is enforced.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description, "
            "unit_price, line_total, reytech_sells, opportunity_flag) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (1, "PO-1", 0, "gloves-redux", 9.00, 450.0, 1, "WIN_BACK"),
        )

    # INSERT OR REPLACE upserts cleanly — proves the post-fix
    # run_universal_pull behavior actually upserts now.
    conn.execute(
        "INSERT OR REPLACE INTO scprs_po_lines (po_id, po_number, line_num, description, "
        "unit_price, line_total, reytech_sells, opportunity_flag) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (1, "PO-1", 0, "gloves-v2", 9.50, 475.0, 1, "WIN_BACK"),
    )
    conn.commit()
    rows_for_po1_line0 = conn.execute(
        "SELECT description, unit_price FROM scprs_po_lines "
        "WHERE po_id=1 AND line_num=0"
    ).fetchall()
    assert len(rows_for_po1_line0) == 1
    assert rows_for_po1_line0[0] == ("gloves-v2", 9.50)
    conn.close()


def test_migration_22_is_idempotent(tmp_path):
    db_path = str(tmp_path / "scprs_iso_c" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_db_with_dupes(db_path)
    from src.core.migrations import _run_migration_22
    _run_migration_22(conn)
    conn.commit()
    after_first = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
    _run_migration_22(conn)
    conn.commit()
    after_second = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
    assert after_first == after_second == 4
    conn.close()


def test_migration_22_handles_partially_cleaned_db(tmp_path):
    """
    Simulates: someone manually deduped some rows by hand before migration
    22 runs. Migration must still finish cleanly — neither double-delete
    nor leave stragglers.
    """
    db_path = str(tmp_path / "scprs_iso_f" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_db_with_dupes(db_path)
    # Manually pre-clean ONE of the dup groups (PO 1 line 0): delete ids 1,2
    # so only id=3 remains. Other dup groups still have dupes.
    conn.execute("DELETE FROM scprs_po_lines WHERE id IN (1, 2)")
    conn.commit()
    pre = conn.execute("SELECT COUNT(*) FROM scprs_po_lines").fetchone()[0]
    assert pre == 8  # 10 original - 2 manual deletes

    from src.core.migrations import _run_migration_22
    _run_migration_22(conn)
    conn.commit()

    # Same end state as the full-dirty case: 4 rows, one per (po_id, line_num).
    surviving = conn.execute(
        "SELECT id, po_id, line_num FROM scprs_po_lines ORDER BY id"
    ).fetchall()
    assert len(surviving) == 4
    by_key = {(po_id, line_num): _id for (_id, po_id, line_num) in surviving}
    assert by_key == {(1, 0): 3, (1, 1): 5, (2, 0): 6, (2, 1): 10}
    conn.close()


def test_migration_22_safe_on_fresh_db_without_scprs_table(tmp_path):
    """No scprs_po_lines table yet → migration must no-op, not raise."""
    db_path = str(tmp_path / "scprs_iso_d" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    from src.core.migrations import _run_migration_22
    _run_migration_22(conn)  # Must not raise.
    # Confirm no table was accidentally created.
    has = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scprs_po_lines'"
    ).fetchone()
    assert has is None
    conn.close()


def test_ensure_schema_on_fresh_db_installs_unique_index(tmp_path, monkeypatch):
    """Fresh installs get the UNIQUE INDEX without waiting for migration 22."""
    data_dir = tmp_path / "scprs_iso_e"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()

    conn = sqlite3.connect(str(data_dir / "reytech.db"))
    # The UNIQUE INDEX exists.
    idx = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND name='uq_scprs_po_lines_po_linenum'"
    ).fetchone()
    assert idx is not None
    # And it actually blocks dupes.
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description) "
        "VALUES (1, 'PO-A', 0, 'first')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description) "
            "VALUES (1, 'PO-A', 0, 'second')"
        )
    conn.close()
