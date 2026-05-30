"""ISSUE-3 (2026-05-29 audit) — existing-row backfill for won_quotes.

PR #1228 converged the WRITERS so new rows are clean. These tests cover the
separate LAW-4 repair of the 90,886 already-frozen rows: dedupe onto the
canonical id, drop corrupt rows, fix dates parked in the category column —
and prove the repair is idempotent (CLAUDE.md §5: fix the data, not just the
code).
"""
from __future__ import annotations

import sqlite3

import pytest

from src.knowledge import won_quotes_db as wq


@pytest.fixture
def wq_db(tmp_path, monkeypatch):
    """Point won_quotes_db at an isolated temp reytech.db with the canonical
    table created. Yields a helper to insert raw rows."""
    monkeypatch.setattr(wq, "DATA_DIR", str(tmp_path))
    wq._ensure_won_quotes_table()

    def insert(**row):
        cols = ["id", "po_number", "item_number", "description",
                "normalized_description", "tokens", "category", "supplier",
                "department", "unit_price", "quantity", "total", "award_date",
                "source", "confidence", "ingested_at", "updated_at"]
        defaults = {c: None for c in cols}
        defaults.update(row)
        conn = sqlite3.connect(str(tmp_path / "reytech.db"))
        conn.execute(
            f"INSERT OR REPLACE INTO won_quotes ({','.join(cols)}) "
            f"VALUES ({','.join('?' for _ in cols)})",
            tuple(defaults[c] for c in cols),
        )
        conn.commit()
        conn.close()

    return insert


def _count(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "reytech.db"))
    n = conn.execute("SELECT COUNT(*) FROM won_quotes").fetchone()[0]
    conn.close()
    return n


def test_dual_id_dupes_collapse_to_canonical(wq_db, tmp_path):
    """The same logical line written under the old `wq_scprs_*` scheme AND
    the canonical id collapses to ONE row keyed by the canonical id."""
    cid = wq.generate_record_id("PO123", "1", "Nitrile Gloves Large")
    # Canonical-id row (clean writer)
    wq_db(id=cid, po_number="PO123", item_number="1",
          description="Nitrile Gloves Large", unit_price=10.0, category="janitorial",
          updated_at="2026-05-29T10:00:00")
    # Old dual-scheme row for the SAME line (the bloat source)
    wq_db(id="wq_scprs_99999", po_number="PO123", item_number="1",
          description="Nitrile Gloves Large", unit_price=10.0, category="01/02/2025",
          updated_at="2026-05-28T10:00:00")
    assert _count(tmp_path) == 2

    plan = wq.repair_existing_rows(dry_run=False)
    assert plan["rows_before"] == 2
    assert plan["rows_after"] == 1
    assert _count(tmp_path) == 1

    conn = sqlite3.connect(str(tmp_path / "reytech.db"))
    rid, cat = conn.execute("SELECT id, category FROM won_quotes").fetchone()
    conn.close()
    assert rid == cid, "survivor must be keyed by the canonical id"
    # newest valid member (the canonical one, category 'janitorial') survives
    assert cat == "janitorial"


def test_corrupt_rows_dropped(wq_db, tmp_path):
    """Blank-desc and $9.4B-price rows have no valid cluster member → dropped."""
    wq_db(id="wq_scprs_1", po_number="PO1", item_number="1",
          description="", unit_price=5.0)                       # blank desc
    wq_db(id="wq_scprs_2", po_number="PO2", item_number="2",
          description="Steel Bolt", unit_price=9_398_433_450.0)  # corrupt price
    # one good row to ensure repair keeps the legit data
    good = wq.generate_record_id("PO3", "3", "Copy Paper")
    wq_db(id=good, po_number="PO3", item_number="3",
          description="Copy Paper", unit_price=4.25)

    wq.repair_existing_rows(dry_run=False)
    conn = sqlite3.connect(str(tmp_path / "reytech.db"))
    ids = [r[0] for r in conn.execute("SELECT id FROM won_quotes").fetchall()]
    conn.close()
    assert ids == [good]


def test_date_in_category_repaired(wq_db, tmp_path):
    """A valid row whose category holds an award DATE gets a derived category,
    not deleted."""
    cid = wq.generate_record_id("PO5", "1", "Surgical Mask Box")
    wq_db(id=cid, po_number="PO5", item_number="1",
          description="Surgical Mask Box", unit_price=12.0, category="12/16/2025")
    wq.repair_existing_rows(dry_run=False)
    conn = sqlite3.connect(str(tmp_path / "reytech.db"))
    cat = conn.execute("SELECT category FROM won_quotes").fetchone()[0]
    conn.close()
    assert cat == "medical_equipment"  # derived from 'mask'/'surgical'


def test_repair_is_idempotent(wq_db, tmp_path):
    """A second real run after a clean repair deletes nothing."""
    cid = wq.generate_record_id("PO9", "1", "Trash Bags")
    wq_db(id=cid, po_number="PO9", item_number="1",
          description="Trash Bags", unit_price=8.0, category="janitorial")
    wq_db(id="wq_scprs_7", po_number="PO9", item_number="1",
          description="Trash Bags", unit_price=8.0, category="03/03/2025")

    first = wq.repair_existing_rows(dry_run=False)
    assert first["rows_deleted"] == 1
    second = wq.repair_existing_rows(dry_run=False)
    assert second["rows_deleted"] == 0
    assert second["rows_before"] == second["rows_after"] == 1


def test_dry_run_writes_nothing(wq_db, tmp_path):
    """dry_run=True returns the plan but leaves every row in place."""
    cid = wq.generate_record_id("PO9", "1", "Trash Bags")
    wq_db(id=cid, po_number="PO9", item_number="1",
          description="Trash Bags", unit_price=8.0, category="janitorial")
    wq_db(id="wq_scprs_7", po_number="PO9", item_number="1",
          description="Trash Bags", unit_price=8.0)
    plan = wq.repair_existing_rows(dry_run=True)
    assert plan["dry_run"] is True
    assert plan["rows_deleted"] == 1
    assert _count(tmp_path) == 2  # nothing actually deleted


def test_diagnose_bloat_is_readonly(wq_db, tmp_path):
    """diagnose_bloat reports the inflation without mutating the table."""
    cid = wq.generate_record_id("PO9", "1", "Trash Bags")
    wq_db(id=cid, po_number="PO9", item_number="1",
          description="Trash Bags", unit_price=8.0)
    wq_db(id="wq_scprs_7", po_number="PO9", item_number="1",
          description="Trash Bags", unit_price=8.0)
    wq_db(id="wq_scprs_8", po_number="PO8", item_number="2",
          description="", unit_price=1.0)  # corrupt blank
    diag = wq.diagnose_bloat()
    assert diag["total_rows"] == 3
    assert diag["distinct_canonical_keys"] == 2
    assert diag["duplicate_rows"] == 1
    assert diag["corrupt_blank_desc_rows"] == 1
    assert _count(tmp_path) == 3  # read-only
