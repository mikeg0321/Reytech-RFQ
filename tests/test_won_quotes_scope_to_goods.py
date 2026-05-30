"""Scope the won_quotes KB to GOODS (2026-05-29 acq_type analysis).

won_quotes is a commodity pricing/intel KB, but the SCPRS sync ingested ALL
award types — Services, Subvention (grants), Interagency Agreements, Leases,
Encumbrance — which a goods reseller can never bid on (they dominated the KB
at avg $56k-$220k). These tests pin that the purge removes only positively
non-product rows and KEEPS Goods, Telecom, and unmatched (NULL acq_type) rows.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.knowledge import won_quotes_db as wq


@pytest.fixture
def scoped_db(tmp_path, monkeypatch):
    monkeypatch.setattr(wq, "DATA_DIR", str(tmp_path))
    wq._ensure_won_quotes_table()
    db = str(tmp_path / "reytech.db")
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT UNIQUE, acq_type TEXT
        )
    """)
    # (po_number, acq_type)  → won_quotes row keyed by po_number
    pos = [
        ("PO-GOODS",   "NON-IT Goods"),
        ("PO-ITGOODS", "IT Goods"),
        ("PO-TEL",     "Telecom"),
        ("PO-SVC",     "NON-IT Services_Personal Services"),
        ("PO-GRANT",   "NON-IT Services_Subvention and Local Assistance"),
        ("PO-IA",      "NON-IT Services_Interagency Agreements"),
        ("PO-ENC",     "Encumbrance Only"),
        ("PO-NULL",    None),          # unmatched acq_type → keep
    ]
    for po, at in pos:
        conn.execute("INSERT INTO scprs_po_master (po_number, acq_type) VALUES (?,?)", (po, at))
    cols = ("id", "po_number", "description", "unit_price", "quantity", "total", "source")
    for po, _ in pos:
        conn.execute(
            f"INSERT INTO won_quotes ({','.join(cols)}) VALUES (?,?,?,?,?,?,?)",
            (wq.generate_record_id(po, "1", po), po, f"item for {po}", 100.0, 1, 100.0, "scprs_sync"),
        )
    # An extra row whose PO is NOT in master at all (truly unmatched) → keep
    conn.execute(
        f"INSERT INTO won_quotes ({','.join(cols)}) VALUES (?,?,?,?,?,?,?)",
        (wq.generate_record_id("PO-ORPHAN", "1", "x"), "PO-ORPHAN", "orphan goods", 50.0, 1, 50.0, "scprs_browser_won"),
    )
    conn.commit()
    conn.close()
    return db


def _pos(db):
    conn = sqlite3.connect(db)
    out = sorted(r[0] for r in conn.execute("SELECT po_number FROM won_quotes").fetchall())
    conn.close()
    return out


def test_dry_run_counts_without_deleting(scoped_db):
    plan = wq.repair_noncommodity_scope(dry_run=True)
    assert plan["rows_before"] == 9
    # SVC, GRANT, IA, ENC = 4 non-product
    assert plan["rows_to_purge"] == 4
    assert plan["rows_after"] == 5
    assert len(_pos(scoped_db)) == 9  # nothing deleted


def test_real_purge_keeps_goods_telecom_unmatched(scoped_db):
    wq.repair_noncommodity_scope(dry_run=False)
    kept = _pos(scoped_db)
    assert kept == ["PO-GOODS", "PO-ITGOODS", "PO-NULL", "PO-ORPHAN", "PO-TEL"]
    # the four non-product POs are gone
    for gone in ("PO-SVC", "PO-GRANT", "PO-IA", "PO-ENC"):
        assert gone not in kept


def test_purge_is_idempotent(scoped_db):
    first = wq.repair_noncommodity_scope(dry_run=False)
    assert first["rows_to_purge"] == 4
    second = wq.repair_noncommodity_scope(dry_run=False)
    assert second["rows_to_purge"] == 0
    assert second["rows_before"] == second["rows_after"] == 5
