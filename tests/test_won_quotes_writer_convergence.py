"""won_quotes writer-convergence guards (2026-05-29 bloat fix).

The Won Quotes KB (`won_quotes` table) had 90,886 rows against ~6.3k real
SCPRS lines, with avg unit_price $790k and a max of $9.4B. The 2026-05-29
source-of-truth audit found three writer bugs (forward-only fixes here; the
existing-row backfill is a separate LAW-4 step):

  1. Dual id scheme — `sync_from_scprs_tables` wrote `wq_scprs_{line_id}`
     while every other writer used `generate_record_id(...)`. ON CONFLICT /
     INSERT OR IGNORE only dedup WITHIN a scheme, so a synced line and the
     same line seen via a lookup became two rows.
  2. Column-shift — sync trusted upstream `scprs_po_lines.category`, which
     carried award DATES, landing dates in `won_quotes.category`.
  3. No validation — blank descriptions and corrupt prices ($2B–$9B) were
     written, poisoning avg/max stats and the pricing oracle.

Isolation: these tests do NOT call init_db (it builds the whole app schema
and is fragile on a non-pristine DB). They create ONLY the tables the
writers touch, in a per-test temp DB whose path is forced onto the module's
DATA_DIR. ingest_scprs_result's cross-posts to price_history / scprs_catalog
are best-effort (try/except in the source) so their absence here is fine.
"""
from __future__ import annotations

import importlib
import os
import sqlite3

import pytest


@pytest.fixture
def wq(tmp_path, monkeypatch):
    """won_quotes_db module bound to an isolated temp DB (no init_db)."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import src.core.paths as paths
    importlib.reload(paths)
    import src.knowledge.won_quotes_db as _wq
    importlib.reload(_wq)
    # Belt-and-suspenders: force the module's data location onto tmp_path so
    # every _get_db_conn() in this test hits the isolated DB regardless of
    # import/reload ordering quirks.
    _wq.DATA_DIR = str(tmp_path)
    _wq.WON_QUOTES_FILE = os.path.join(str(tmp_path), "won_quotes.json")
    _wq._ensure_won_quotes_table()
    return _wq


def _seed_scprs_line(wq, *, po="PO-1", item="IT-9",
                     desc="Nitrile Gloves Large", line_total=100.0,
                     qty=10, category=""):
    """Create the minimal scprs source tables + one line in the SAME temp DB
    the module writes to, then close the connection before sync runs."""
    conn = sqlite3.connect(os.path.join(wq.DATA_DIR, "reytech.db"), timeout=30)
    try:
        conn.executescript("""
            DROP TABLE IF EXISTS scprs_po_master;
            DROP TABLE IF EXISTS scprs_po_lines;
            CREATE TABLE scprs_po_master (id INTEGER PRIMARY KEY, supplier TEXT,
                agency_key TEXT, start_date TEXT);
            CREATE TABLE scprs_po_lines (id INTEGER PRIMARY KEY, po_id INTEGER,
                po_number TEXT, item_id TEXT, description TEXT, unit_price REAL,
                quantity REAL, category TEXT);
            INSERT INTO scprs_po_master VALUES (1,'ACME','CCHCS','2026-01-01');
        """)
        conn.execute(
            "INSERT INTO scprs_po_lines VALUES (1,1,?,?,?,?,?,?)",
            (po, item, desc, line_total, qty, category),
        )
        conn.commit()
    finally:
        conn.close()


def _count(wq):
    conn = sqlite3.connect(os.path.join(wq.DATA_DIR, "reytech.db"), timeout=30)
    try:
        return conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
    finally:
        conn.close()


def _rows(wq):
    conn = sqlite3.connect(os.path.join(wq.DATA_DIR, "reytech.db"), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM won_quotes")]
    finally:
        conn.close()


# ── Guard 1: single canonical id scheme ───────────────────────────────

def test_sync_uses_canonical_id_not_line_id_scheme(wq):
    """sync_from_scprs_tables must key rows by generate_record_id so a
    synced line collides with the same line seen via ingest_scprs_result."""
    _seed_scprs_line(wq, po="PO-1", item="IT-9", desc="Nitrile Gloves Large")
    stats = wq.sync_from_scprs_tables()
    assert stats["synced"] == 1

    expected_id = wq.generate_record_id("PO-1", "IT-9", "Nitrile Gloves Large")
    ids = [r["id"] for r in _rows(wq)]
    assert ids == [expected_id], f"sync must use canonical id, got {ids}"
    assert not any(str(i).startswith("wq_scprs_") for i in ids)


def test_sync_then_ingest_same_line_is_one_row(wq):
    """The structural bloat fix: sync + per-lookup ingest of the SAME line
    must collapse to ONE row, not two."""
    _seed_scprs_line(wq, po="PO-1", item="IT-9",
                     desc="Nitrile Gloves Large", line_total=100.0, qty=10)
    wq.sync_from_scprs_tables()
    wq.ingest_scprs_result(
        po_number="PO-1", item_number="IT-9",
        description="Nitrile Gloves Large", unit_price=10.0, quantity=10,
    )
    assert _count(wq) == 1


# ── Guard 2: category is derived, never the upstream date column ───────

def test_sync_ignores_upstream_category_date_contamination(wq):
    """scprs_po_lines.category held award dates; sync must derive category
    from the description instead so no date lands in won_quotes.category."""
    _seed_scprs_line(wq, desc="Surgical Gauze Pads", line_total=5.0, qty=1,
                     category="12/16/2025")
    wq.sync_from_scprs_tables()
    cat = _rows(wq)[0]["category"]
    assert "/" not in cat, f"category must not be a date, got {cat!r}"
    assert cat == wq.classify_category("Surgical Gauze Pads")


# ── Guard 3: validation rejects junk + corrupt prices ─────────────────

def test_is_valid_won_quote_rejects_junk():
    import src.knowledge.won_quotes_db as wq
    assert wq.is_valid_won_quote("Real Item", 12.50) is True
    assert wq.is_valid_won_quote("", 12.50) is False             # blank desc
    assert wq.is_valid_won_quote("   ", 12.50) is False           # whitespace
    assert wq.is_valid_won_quote("Item", 0) is False             # zero price
    assert wq.is_valid_won_quote("Item", -5) is False            # negative
    assert wq.is_valid_won_quote("Item", 9_398_433_450.0) is False  # the $9.4B row
    assert wq.is_valid_won_quote("Item", None) is False          # missing


def test_ingest_skips_invalid_and_writes_nothing(wq):
    wq.ingest_scprs_result(po_number="PO-X", item_number="I1",
                           description="", unit_price=10.0)          # blank desc
    wq.ingest_scprs_result(po_number="PO-X", item_number="I2",
                           description="Good Item", unit_price=2e9)  # corrupt price
    assert _count(wq) == 0


def test_ingest_writes_valid_row(wq):
    rec = wq.ingest_scprs_result(po_number="PO-Y", item_number="I3",
                                 description="Exam Gloves", unit_price=8.25,
                                 quantity=4)
    assert rec.get("id")
    rows = _rows(wq)
    assert len(rows) == 1
    assert rows[0]["unit_price"] == 8.25
    assert rows[0]["total"] == 33.0
    assert "/" not in rows[0]["category"]
