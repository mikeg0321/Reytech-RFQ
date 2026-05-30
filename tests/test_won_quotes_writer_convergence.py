"""won_quotes writer-convergence guards (2026-05-29 bloat fix).

The Won Quotes KB (`won_quotes` table) had 90,886 rows against ~6.3k real
SCPRS lines. Root causes the source-of-truth audit found:
  1. Dual id scheme — `sync_from_scprs_tables` wrote `wq_scprs_{line_id}`
     while every other writer used `generate_record_id(...)`. ON CONFLICT
     only dedups within a scheme, so a synced line + the same line seen via
     a lookup became two rows.
  2. Column-shift — sync trusted upstream `scprs_po_lines.category`, which
     carried award DATES, landing dates in `won_quotes.category`.
  3. No validation — blank descriptions and corrupt prices ($2B–$9B) were
     written, poisoning avg/max stats and the pricing oracle.

These tests pin all three forward-only guards. They do NOT touch existing
prod rows (that backfill is a separate LAW-4 step).
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def wq(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import src.core.paths as paths
    importlib.reload(paths)
    import src.core.db as core_db
    importlib.reload(core_db)
    import src.knowledge.won_quotes_db as _wq
    importlib.reload(_wq)
    core_db.init_db()
    return _wq


# ── Guard 1: single canonical id scheme ───────────────────────────────

def test_sync_uses_canonical_id_not_line_id_scheme(wq):
    """sync_from_scprs_tables must key rows by generate_record_id so a
    synced line collides with the same line seen via ingest_scprs_result.
    """
    conn = wq._get_db_conn()
    conn.executescript("""
        CREATE TABLE scprs_po_master (id INTEGER PRIMARY KEY, supplier TEXT,
            agency_key TEXT, start_date TEXT);
        CREATE TABLE scprs_po_lines (id INTEGER PRIMARY KEY, po_id INTEGER,
            po_number TEXT, item_id TEXT, description TEXT, unit_price REAL,
            quantity REAL, category TEXT);
        INSERT INTO scprs_po_master VALUES (1,'ACME','CCHCS','2026-01-01');
        INSERT INTO scprs_po_lines VALUES
            (1, 1, 'PO-1', 'IT-9', 'Nitrile Gloves Large', 100.0, 10, '01/24/2025');
    """)
    conn.commit()
    conn.close()

    stats = wq.sync_from_scprs_tables()
    assert stats["synced"] == 1

    expected_id = wq.generate_record_id("PO-1", "IT-9", "Nitrile Gloves Large")
    conn = wq._get_db_conn()
    ids = [r[0] for r in conn.execute("SELECT id FROM won_quotes").fetchall()]
    conn.close()
    assert ids == [expected_id], f"sync must use canonical id, got {ids}"
    assert not any(str(i).startswith("wq_scprs_") for i in ids)


def test_sync_then_ingest_same_line_is_one_row(wq):
    """The structural bloat fix: sync + per-lookup ingest of the SAME line
    must collapse to ONE row, not two."""
    conn = wq._get_db_conn()
    conn.executescript("""
        CREATE TABLE scprs_po_master (id INTEGER PRIMARY KEY, supplier TEXT,
            agency_key TEXT, start_date TEXT);
        CREATE TABLE scprs_po_lines (id INTEGER PRIMARY KEY, po_id INTEGER,
            po_number TEXT, item_id TEXT, description TEXT, unit_price REAL,
            quantity REAL, category TEXT);
        INSERT INTO scprs_po_master VALUES (1,'ACME','CCHCS','2026-01-01');
        INSERT INTO scprs_po_lines VALUES
            (1, 1, 'PO-1', 'IT-9', 'Nitrile Gloves Large', 100.0, 10, '');
    """)
    conn.commit()
    conn.close()

    wq.sync_from_scprs_tables()
    # Same logical line arrives via a per-lookup ingest (per-unit price).
    wq.ingest_scprs_result(
        po_number="PO-1", item_number="IT-9",
        description="Nitrile Gloves Large", unit_price=10.0, quantity=10,
    )
    conn = wq._get_db_conn()
    n = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
    conn.close()
    assert n == 1, f"sync+ingest of one line must be 1 row, got {n}"


# ── Guard 2: category is derived, never the upstream date column ───────

def test_sync_ignores_upstream_category_date_contamination(wq):
    """scprs_po_lines.category held award dates; sync must derive category
    from the description instead so no date lands in won_quotes.category."""
    conn = wq._get_db_conn()
    conn.executescript("""
        CREATE TABLE scprs_po_master (id INTEGER PRIMARY KEY, supplier TEXT,
            agency_key TEXT, start_date TEXT);
        CREATE TABLE scprs_po_lines (id INTEGER PRIMARY KEY, po_id INTEGER,
            po_number TEXT, item_id TEXT, description TEXT, unit_price REAL,
            quantity REAL, category TEXT);
        INSERT INTO scprs_po_master VALUES (1,'ACME','CCHCS','2026-01-01');
        INSERT INTO scprs_po_lines VALUES
            (1, 1, 'PO-1', 'IT-9', 'Surgical Gauze Pads', 5.0, 1, '12/16/2025');
    """)
    conn.commit()
    conn.close()

    wq.sync_from_scprs_tables()
    conn = wq._get_db_conn()
    cat = conn.execute("SELECT category FROM won_quotes").fetchone()[0]
    conn.close()
    assert "/" not in cat, f"category must not be a date, got {cat!r}"
    assert cat == wq.classify_category("Surgical Gauze Pads")


# ── Guard 3: validation rejects junk + corrupt prices ─────────────────

def test_is_valid_won_quote_rejects_junk():
    import src.knowledge.won_quotes_db as wq
    assert wq.is_valid_won_quote("Real Item", 12.50) is True
    assert wq.is_valid_won_quote("", 12.50) is False          # blank desc
    assert wq.is_valid_won_quote("   ", 12.50) is False        # whitespace desc
    assert wq.is_valid_won_quote("Item", 0) is False           # zero price
    assert wq.is_valid_won_quote("Item", -5) is False          # negative
    assert wq.is_valid_won_quote("Item", 9_398_433_450.0) is False  # the $9.4B row
    assert wq.is_valid_won_quote("Item", None) is False        # missing


def test_ingest_skips_invalid_and_writes_nothing(wq):
    wq.ingest_scprs_result(po_number="PO-X", item_number="I1",
                           description="", unit_price=10.0)          # blank desc
    wq.ingest_scprs_result(po_number="PO-X", item_number="I2",
                           description="Good Item", unit_price=2e9)  # corrupt price
    conn = wq._get_db_conn()
    n = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
    conn.close()
    assert n == 0, f"invalid rows must not be written, got {n}"


def test_ingest_writes_valid_row(wq):
    rec = wq.ingest_scprs_result(po_number="PO-Y", item_number="I3",
                                 description="Exam Gloves", unit_price=8.25,
                                 quantity=4)
    assert rec.get("id")
    conn = wq._get_db_conn()
    row = conn.execute(
        "SELECT unit_price, total, category FROM won_quotes WHERE id=?",
        (rec["id"],)).fetchone()
    conn.close()
    assert row[0] == 8.25
    assert row[1] == 33.0
    assert "/" not in row[2]
