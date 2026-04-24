"""Regression: SCPRS MFG# backfill extracts identifiers from descriptions
and writes them to the per-table column the oracle searches.

The oracle's MFG# partition (PR #487) only fires when ≥2 historical
SCPRS rows share the quote item's MFG#. Until this backfill runs,
every historical row has NULL/empty in `item_number` /
`mfg_number` / `item_id` / `part_number`, so the partition's OR
clause matches nothing — even when Mike's PC carries a clean MFG#
like `WL085P`.

This regression locks in:
- Each table's correct target column
- Idempotency (re-run leaves already-set rows alone)
- dry_run flag doesn't write
- Real description samples from prod yield expected MFG#s
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def temp_db(tmp_path):
    """Build a SQLite DB with the four SCPRS-style tables the backfill
    targets. Uses the same column names as the production schema so
    the backfill module's queries hit the same shape."""
    db_path = str(tmp_path / "test_backfill.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE won_quotes (
            id TEXT PRIMARY KEY,
            description TEXT,
            item_number TEXT,
            unit_price REAL,
            quantity REAL,
            supplier TEXT,
            department TEXT,
            award_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE scprs_catalog (
            description TEXT PRIMARY KEY,
            mfg_number TEXT,
            last_unit_price REAL,
            last_quantity REAL,
            times_seen INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT,
            description TEXT,
            item_id TEXT,
            unit_price REAL,
            quantity REAL,
            uom TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE winning_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT,
            part_number TEXT,
            sell_price REAL,
            qty REAL,
            supplier TEXT
        )
    """)
    conn.commit()
    conn.close()

    with patch("src.core.db.DB_PATH", db_path):
        yield db_path


def _insert(db_path, table, rows):
    conn = sqlite3.connect(db_path)
    cols = list(rows[0].keys())
    placeholders = ",".join("?" * len(cols))
    col_list = ",".join(cols)
    for r in rows:
        conn.execute(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
                     [r[c] for c in cols])
    conn.commit()
    conn.close()


def _read_col(db_path, table, col, where="1=1"):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(f"SELECT {col} FROM {table} WHERE {where}").fetchall()
    conn.close()
    return [r[0] for r in rows]


# ── Per-table targeting ────────────────────────────────────────────


def test_won_quotes_writes_to_item_number(temp_db):
    _insert(temp_db, "won_quotes", [
        {"id": "wq1",
         # Trailing-dash format — matches `_PN_PATTERNS` rule 12.
         # This is the most common shape SCPRS descriptions take when
         # they DO carry an identifier (e.g., "JUMBO JACKS - W14100").
         "description": "Stanley RoamAlert Wrist Strap - W14100",
         "item_number": None, "unit_price": 45.0, "quantity": 1,
         "supplier": "Reytech", "department": "5225",
         "award_date": "2025-08-01"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    out = backfill_mfg_numbers()
    assert out["ok"] is True
    stats = out["stats"]["won_quotes"]
    assert stats["extracted"] == 1
    assert stats["written"] == 1
    vals = _read_col(temp_db, "won_quotes", "item_number")
    assert vals == ["W14100"]


def test_scprs_catalog_writes_to_mfg_number(temp_db):
    _insert(temp_db, "scprs_catalog", [
        # Trailing 5-8 digit code after dash — `_PN_PATTERNS` rule 13.
        # Matches Sunrise Medical / generic-medical SKU shape.
        {"description": "Sedeo Pro Armrest Pad - 163353",
         "mfg_number": "", "last_unit_price": 67.5,
         "last_quantity": 1, "times_seen": 3},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    backfill_mfg_numbers()
    vals = _read_col(temp_db, "scprs_catalog", "mfg_number")
    assert vals == ["163353"]


def test_scprs_po_lines_writes_to_item_id(temp_db):
    _insert(temp_db, "scprs_po_lines", [
        # "Item: <code>" labeled format — `_PN_PATTERNS` rule 3.
        {"po_number": "PO-1",
         "description": "PNEUMATIC WHEELS Item: WC-2280 ALUMINUM",
         "item_id": None, "unit_price": 31.5, "quantity": 10,
         "uom": "EA"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    backfill_mfg_numbers()
    vals = _read_col(temp_db, "scprs_po_lines", "item_id")
    assert vals[0] is not None and len(vals[0]) >= 3


def test_winning_prices_writes_to_part_number(temp_db):
    _insert(temp_db, "winning_prices", [
        {"description": "Supregear Cane Tip - W14100 Heavy Duty",
         "part_number": "", "sell_price": 17.5, "qty": 1,
         "supplier": "Reytech"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    backfill_mfg_numbers()
    vals = _read_col(temp_db, "winning_prices", "part_number")
    assert vals == ["W14100"]


# ── Idempotency ────────────────────────────────────────────────────


def test_already_set_rows_are_not_overwritten(temp_db):
    """Rows where the target column is already populated must be left
    alone — the backfill is "fill missing", never "rewrite all"."""
    _insert(temp_db, "won_quotes", [
        {"id": "wq1",
         "description": "Different Brand DIFFERENT-MFG-X1",
         "item_number": "ALREADY-SET",  # pre-populated
         "unit_price": 10.0, "quantity": 1, "supplier": "x",
         "department": "x", "award_date": "2025-01-01"},
        {"id": "wq2",
         "description": "Stanley New Item - W14100",
         "item_number": None,  # empty — should be filled
         "unit_price": 20.0, "quantity": 1, "supplier": "x",
         "department": "x", "award_date": "2025-01-01"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    out = backfill_mfg_numbers()
    stats = out["stats"]["won_quotes"]
    assert stats["scanned"] == 1, "Pre-populated row must not be scanned"
    assert stats["written"] == 1
    vals = sorted(_read_col(temp_db, "won_quotes", "item_number"))
    assert vals == ["ALREADY-SET", "W14100"]


def test_re_run_is_safe(temp_db):
    """Calling twice should be a no-op the second time."""
    _insert(temp_db, "won_quotes", [
        {"id": "wq1",
         "description": "Stanley Wrist Strap - W14100",
         "item_number": None, "unit_price": 10.0, "quantity": 1,
         "supplier": "x", "department": "x", "award_date": "2025-01-01"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    out1 = backfill_mfg_numbers()
    assert out1["stats"]["won_quotes"]["written"] == 1
    out2 = backfill_mfg_numbers()
    assert out2["stats"]["won_quotes"]["scanned"] == 0
    assert out2["stats"]["won_quotes"]["written"] == 0


# ── dry_run + safety ───────────────────────────────────────────────


def test_dry_run_does_not_write(temp_db):
    _insert(temp_db, "won_quotes", [
        {"id": "wq1",
         "description": "Stanley Wrist Strap - W14100",
         "item_number": None, "unit_price": 10.0, "quantity": 1,
         "supplier": "x", "department": "x", "award_date": "2025-01-01"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    out = backfill_mfg_numbers(dry_run=True)
    stats = out["stats"]["won_quotes"]
    assert stats["extracted"] == 1
    # In dry_run the "written" stat mirrors extracted so the operator
    # sees what would happen, but the DB stays unchanged.
    assert stats["written"] == 1
    vals = _read_col(temp_db, "won_quotes", "item_number")
    assert vals == [None], f"Dry run wrote to DB: {vals}"


def test_descriptions_with_no_extractable_mfg_are_left_alone(temp_db):
    """The Stanley/Sedeo wins have MFG#s; "PNEUMATIC WHEELS" alone
    does NOT have a clean part number — backfill should leave it
    NULL, not write a junk extraction."""
    _insert(temp_db, "won_quotes", [
        {"id": "wq1",
         "description": "PNEUMATIC WHEELS LARGE",  # no part #
         "item_number": None, "unit_price": 10.0, "quantity": 1,
         "supplier": "x", "department": "x", "award_date": "2025-01-01"},
    ])
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    out = backfill_mfg_numbers()
    stats = out["stats"]["won_quotes"]
    assert stats["scanned"] == 1
    assert stats["extracted"] == 0
    assert stats["written"] == 0
    assert _read_col(temp_db, "won_quotes", "item_number") == [None]


def test_limit_per_table_caps_scan(temp_db):
    """Spot-check mode on prod: limit_per_table=N should stop after
    N scanned rows."""
    rows = [
        {"id": f"wq{i}",
         "description": f"Stanley Wrist Strap - W{14000 + i}",
         "item_number": None, "unit_price": 10.0, "quantity": 1,
         "supplier": "x", "department": "x", "award_date": "2025-01-01"}
        for i in range(10)
    ]
    _insert(temp_db, "won_quotes", rows)
    from src.core.scprs_mfg_backfill import backfill_mfg_numbers
    out = backfill_mfg_numbers(limit_per_table=3)
    assert out["stats"]["won_quotes"]["scanned"] == 3


def test_missing_table_does_not_crash(tmp_path):
    """Some installs may lack winning_prices / scprs_catalog. The
    backfill should skip cleanly and continue to other tables."""
    db_path = str(tmp_path / "partial.db")
    conn = sqlite3.connect(db_path)
    # Only create won_quotes — others missing.
    conn.execute("""
        CREATE TABLE won_quotes (
            id TEXT PRIMARY KEY, description TEXT, item_number TEXT,
            unit_price REAL, quantity REAL, supplier TEXT,
            department TEXT, award_date TEXT
        )
    """)
    conn.commit()
    conn.close()
    with patch("src.core.db.DB_PATH", db_path):
        from src.core.scprs_mfg_backfill import backfill_mfg_numbers
        out = backfill_mfg_numbers()
    # Each missing table is reported but doesn't kill the run.
    assert "won_quotes" in out["stats"]
    assert "scprs_catalog" in out["stats"]
    assert "scprs_po_lines" in out["stats"]
