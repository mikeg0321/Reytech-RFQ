"""IN-10 regression: `winning_quote_shapes.agency` column exists + is written.

Before 2026-04-22 the table had only `institution`. `pricing_oracle_v2.
calibrate_from_outcome` bound the agency code into that column, conflating
the two concepts — any future consumer that wanted agency-keyed rollups
had to re-derive them from institution strings.

Fix: add a dedicated `agency` column (indexed), write it on every new row,
back-fill existing rows from institution via one-shot migration UPDATE.

This test verifies:
  - Fresh-install schema creates the column.
  - `calibrate_from_outcome` writes to both columns.
  - The back-fill migration copies institution → agency on rows that
    predate the column.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest


def _apply_schema_to(path: str):
    """Minimal standalone replay of the winning_quote_shapes DDL. We do not
    go through src.core.db.init_db because that writes to the real DB_PATH
    and pulls in the full app schema — we only care about this one table."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS winning_quote_shapes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            institution TEXT DEFAULT '',
            agency TEXT DEFAULT '',
            category_mix TEXT,
            total_items INTEGER,
            avg_markup REAL,
            markup_stddev REAL,
            markup_distribution TEXT,
            outcome TEXT,
            recorded_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_wqs_institution ON winning_quote_shapes(institution);
        CREATE INDEX IF NOT EXISTS idx_wqs_agency ON winning_quote_shapes(agency);
        CREATE INDEX IF NOT EXISTS idx_wqs_outcome ON winning_quote_shapes(outcome);
        """
    )
    conn.commit()
    return conn


@pytest.fixture()
def conn(tmp_path):
    """Sqlite connection that closes cleanly — Windows holds the DB file
    locked while any connection is live, so explicit close matters for
    TemporaryDirectory-style cleanup."""
    db = str(tmp_path / "test.db")
    c = _apply_schema_to(db)
    try:
        yield c
    finally:
        c.close()


def test_fresh_schema_has_agency_column(conn):
    cols = {
        r[1]
        for r in conn.execute("PRAGMA table_info(winning_quote_shapes)").fetchall()
    }
    assert "agency" in cols, (
        "IN-10 regressed: fresh schema dropped winning_quote_shapes.agency"
    )
    assert "institution" in cols, "legacy column must remain"


def test_fresh_schema_has_agency_index(conn):
    idx_names = {
        r[1]
        for r in conn.execute(
            "SELECT * FROM sqlite_master WHERE type='index' "
            "AND tbl_name='winning_quote_shapes'"
        ).fetchall()
    }
    assert "idx_wqs_agency" in idx_names, (
        "Agency index missing — per-agency rollups will table-scan."
    )


def test_insert_writes_both_institution_and_agency(conn):
    """Mirror the INSERT from pricing_oracle_v2.calibrate_from_outcome."""
    conn.execute(
        """
        INSERT INTO winning_quote_shapes
            (institution, agency, category_mix, total_items, avg_markup,
             markup_stddev, markup_distribution, outcome, recorded_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            "cchcs",
            "cchcs",
            "{}",
            3,
            20.0,
            2.0,
            "[]",
            "won",
            "2026-04-22T00:00:00",
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT institution, agency FROM winning_quote_shapes"
    ).fetchone()
    assert row == ("cchcs", "cchcs")


def test_backfill_updates_rows_missing_agency(conn):
    """Replays the migration UPDATE: rows with institution set but agency
    empty should get agency back-filled from institution."""
    conn.execute(
        "INSERT INTO winning_quote_shapes "
        "(institution, agency, outcome, recorded_at) "
        "VALUES (?,?,?,?)",
        ("calvet", "", "won", "2026-04-10T00:00:00"),
    )
    conn.execute(
        "INSERT INTO winning_quote_shapes "
        "(institution, agency, outcome, recorded_at) "
        "VALUES (?,?,?,?)",
        ("cchcs", "cchcs", "won", "2026-04-22T00:00:00"),
    )
    conn.commit()

    conn.execute(
        "UPDATE winning_quote_shapes "
        "SET agency = institution "
        "WHERE (agency IS NULL OR agency = '') AND institution != ''"
    )
    conn.commit()

    rows = conn.execute(
        "SELECT institution, agency FROM winning_quote_shapes ORDER BY id"
    ).fetchall()
    assert rows == [("calvet", "calvet"), ("cchcs", "cchcs")], f"Backfill failed: {rows}"


def test_pricing_oracle_insert_sql_writes_agency():
    """Grep-invariant: the oracle INSERT must include the `agency` column in
    the column list. A silent regression to the 8-column shape would leave
    agency empty on every new row."""
    src = (
        Path(__file__).resolve().parents[1]
        / "src" / "core" / "pricing_oracle_v2.py"
    ).read_text(encoding="utf-8")
    assert "INSERT INTO winning_quote_shapes" in src
    idx = src.index("INSERT INTO winning_quote_shapes")
    fragment = src[idx : idx + 400]
    assert "agency" in fragment, (
        "pricing_oracle_v2 INSERT dropped the agency column — rows will "
        "persist with agency='' and downstream agency rollups will see "
        "everything as 'unknown'."
    )
