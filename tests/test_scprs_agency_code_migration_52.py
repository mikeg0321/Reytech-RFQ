"""
Regression test for migration 52 (scprs_po_master.agency_code).

Why this exists:
  `agency_code TEXT` is listed in scprs_universal_pull._ensure_schema()'s
  CREATE TABLE statement, but that uses CREATE TABLE IF NOT EXISTS. Any prod
  DB whose scprs_po_master was created BEFORE agency_code was added to that
  CREATE never got the column. get_universal_intelligence()'s by_agency query
  then selects p.agency_code and raises:

      OperationalError: no such column: p.agency_code

  which the /intel/scprs handler catches and surfaces as the red error banner,
  zeroing every KPI (POs Captured / Market Spend / Gap Items / Win-Back /
  Auto-Closed) even though the DB has records. (Found by the 2026-05-28 Chrome
  bug-sweep; same column-drift class that migration 23 fixed for is_test.)

  Migration 52 idempotently ALTERs scprs_po_master to add agency_code and
  backfills it from dept_code (the writer stores `agency_code or dept_code`).

  These tests prove:
    1. The migration adds agency_code on a legacy table that lacks it.
    2. The migration backfills agency_code from dept_code where blank.
    3. The by_agency-style query (the one that crashed prod) succeeds after.
    4. The migration is idempotent — second run is a no-op, no error.
    5. On a DB that already has agency_code, existing values are NOT clobbered.
    6. The migration is safe on a fresh DB with no scprs_po_master table.
"""
import os
import sqlite3

import pytest


def _make_legacy_master(db_path):
    """Create scprs_po_master WITHOUT agency_code (the pre-drift shape)."""
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pulled_at TEXT, po_number TEXT UNIQUE,
            dept_code TEXT, dept_name TEXT, institution TEXT,
            supplier TEXT, status TEXT,
            merch_amount REAL, grand_total REAL
        );
    """)
    rows = [
        ("PO-1", "CDCR", "CDCR HQ", "Henry Schein", 1000.0),
        ("PO-2", "CCHCS", "CCHCS Pharmacy", "McKesson", 2500.0),
        ("PO-3", "CDCR", "CDCR HQ", "Medline", 500.0),
    ]
    conn.executemany(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, supplier, grand_total) "
        "VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn


def _columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_migration_52_adds_and_backfills_agency_code(tmp_path):
    db_path = str(tmp_path / "iso_a" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_legacy_master(db_path)
    assert "agency_code" not in _columns(conn, "scprs_po_master")

    from src.core.migrations import _run_migration_52
    _run_migration_52(conn)
    conn.commit()

    assert "agency_code" in _columns(conn, "scprs_po_master")
    # Backfilled from dept_code.
    got = dict(conn.execute(
        "SELECT po_number, agency_code FROM scprs_po_master ORDER BY po_number"
    ).fetchall())
    assert got == {"PO-1": "CDCR", "PO-2": "CCHCS", "PO-3": "CDCR"}
    conn.close()


def test_by_agency_query_succeeds_after_migration(tmp_path):
    """Reproduces the exact prod crash, then proves the migration heals it."""
    db_path = str(tmp_path / "iso_b" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_legacy_master(db_path)

    by_agency_sql = (
        "SELECT p.agency_code, COUNT(*) AS pos, SUM(p.grand_total) AS spend "
        "FROM scprs_po_master p GROUP BY p.agency_code"
    )
    # Pre-migration: the column does not exist → the exact prod error.
    with pytest.raises(sqlite3.OperationalError, match="no such column: p.agency_code"):
        conn.execute(by_agency_sql).fetchall()

    from src.core.migrations import _run_migration_52
    _run_migration_52(conn)
    conn.commit()

    # Post-migration: the query runs and aggregates by agency.
    result = {r[0]: (r[1], r[2]) for r in conn.execute(by_agency_sql).fetchall()}
    assert result == {"CDCR": (2, 1500.0), "CCHCS": (1, 2500.0)}
    conn.close()


def test_migration_52_is_idempotent(tmp_path):
    db_path = str(tmp_path / "iso_c" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _make_legacy_master(db_path)
    from src.core.migrations import _run_migration_52
    _run_migration_52(conn)
    conn.commit()
    _run_migration_52(conn)  # second run must not raise
    conn.commit()
    # Column present exactly once (PRAGMA returns a set; just assert present).
    assert "agency_code" in _columns(conn, "scprs_po_master")
    conn.close()


def test_migration_52_does_not_clobber_existing_agency_code(tmp_path):
    """A DB created from the current CREATE already has agency_code populated;
    the migration must not overwrite a real value with dept_code."""
    db_path = str(tmp_path / "iso_d" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT UNIQUE, dept_code TEXT, grand_total REAL,
            agency_code TEXT
        );
    """)
    conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, grand_total, agency_code) "
        "VALUES ('PO-9', 'RAW_DEPT', 100.0, 'REAL_AGENCY')"
    )
    conn.commit()

    from src.core.migrations import _run_migration_52
    _run_migration_52(conn)
    conn.commit()

    # Existing non-blank agency_code preserved (NOT overwritten by dept_code).
    assert conn.execute(
        "SELECT agency_code FROM scprs_po_master WHERE po_number='PO-9'"
    ).fetchone()[0] == "REAL_AGENCY"
    conn.close()


def test_migration_52_safe_on_fresh_db_without_table(tmp_path):
    """No scprs_po_master table yet → migration must no-op, not raise."""
    db_path = str(tmp_path / "iso_e" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    from src.core.migrations import _run_migration_52
    _run_migration_52(conn)  # must not raise
    has = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scprs_po_master'"
    ).fetchone()
    assert has is None
    conn.close()
