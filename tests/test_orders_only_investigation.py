"""Tests for the orders-only investigation surface (PR-3 follow-up to
the SCPRS reconciler).

Two parts:
  (a) `_classify_orders_only_po` — bucket each orders.po_number that
      isn't covered by SCPRS into one of: sentinel / rfq_as_po /
      bare_numeric_unknown / looks_canonical / unknown.
  (b) `/api/admin/scprs-orders-only-sentinel-cleanup` — flips
      is_test=1 on classified-as-sentinel rows. Idempotent.

Live prod reading 2026-04-29 had 8 orders-only rows: 1 sentinel
('TEST'), 3 rfq_as_po (RFQ882023, RFQ Gowns, R23O20), 3
bare_numeric_unknown (10820146, 10820523, 10819149 — likely
NKSP/CMF/CIW requisition numbers misparsed), 1 looks_canonical
(8955-0000050349 — recent CalVet, SCPRS not yet indexed).
"""
from __future__ import annotations

import pytest


@pytest.mark.parametrize("po,expected", [
    # Sentinel values
    ("TEST",      "sentinel"),
    ("test",      "sentinel"),
    ("N/A",       "sentinel"),
    ("NA",        "sentinel"),
    ("TBD",       "sentinel"),
    ("PENDING",   "sentinel"),
    ("?",         "sentinel"),
    ("X",         "sentinel"),
    ("none",      "sentinel"),

    # RFQ-as-PO operator placeholders
    ("RFQ882023",  "rfq_as_po"),
    ("RFQ Gowns",  "rfq_as_po"),
    ("R23O20",     "rfq_as_po"),  # Reytech quote number pattern
    ("R26Q42",     "rfq_as_po"),
    ("RFQ 12345",  "rfq_as_po"),

    # Already-canonical (recent PO not yet in SCPRS)
    ("8955-0000050349",  "looks_canonical"),
    ("8955-0000076737",  "looks_canonical"),
    ("4440-0000063878",  "looks_canonical"),
    ("4500752793",       "looks_canonical"),
    ("4500737702",       "looks_canonical"),

    # Bare numeric — likely requisition# (NKSP / CMF / CIW)
    ("10820146",  "bare_numeric_unknown"),
    ("10820523",  "bare_numeric_unknown"),
    ("10819149",  "bare_numeric_unknown"),
    ("12345678",  "bare_numeric_unknown"),

    # Edge cases
    ("",                 "unknown"),
    ("  ",               "unknown"),
    ("ABC123",           "unknown"),
    # Very short numerics shouldn't classify as bare_numeric_unknown
    ("123",              "unknown"),
    ("123456",           "unknown"),
    # Whitespace tolerated
    (" TEST ",           "sentinel"),
    (" 8955-0000050349 ", "looks_canonical"),
])
def test_classify_orders_only_po(po, expected):
    from src.api.modules.routes_health import _classify_orders_only_po
    assert _classify_orders_only_po(po) == expected


# ── Card annotates orders_only samples with classification ────────────


def _seed_wins(conn, rows):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scprs_reytech_wins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT NOT NULL,
            business_unit TEXT,
            dept_name TEXT,
            associated_po TEXT,
            start_date TEXT,
            end_date TEXT,
            grand_total REAL,
            items_json TEXT,
            imported_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_scprs_reytech_wins_po "
        "ON scprs_reytech_wins(po_number)"
    )
    conn.execute("DELETE FROM scprs_reytech_wins")
    for r in rows:
        conn.execute("""
            INSERT INTO scprs_reytech_wins
            (po_number, business_unit, dept_name, grand_total, items_json)
            VALUES (?,?,?,?,?)
        """, (r["po_number"], r.get("business_unit", ""),
              r.get("dept_name", ""),
              float(r.get("grand_total", 0)), "[]"))


def _seed_orders(conn, rows):
    try:
        conn.execute("DELETE FROM orders")
    except Exception:
        pass
    for r in rows:
        conn.execute("""
            INSERT INTO orders
              (id, quote_number, po_number, agency, institution,
               total, status, items, created_at, updated_at, is_test)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (r["id"], r.get("quote_number", "Q1"),
              r["po_number"], r.get("agency", "CDCR"),
              r.get("institution", ""),
              float(r.get("total", 100)), "open", "[]",
              "2026-04-28T10:00:00", "2026-04-28T10:00:00",
              int(r.get("is_test", 0))))


def test_card_annotates_orders_only_with_classification(auth_client):
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [
            {"id": "o1", "po_number": "TEST"},
            {"id": "o2", "po_number": "RFQ882023"},
            {"id": "o3", "po_number": "10820146"},
            {"id": "o4", "po_number": "8955-0000050349"},
        ])
        c.commit()

    out = _build_scprs_reconcile_card()
    classes = {s["po_number"]: s["classification"]
               for s in out["samples"]["orders_only"]}
    assert classes["TEST"] == "sentinel"
    assert classes["RFQ882023"] == "rfq_as_po"
    assert classes["10820146"] == "bare_numeric_unknown"
    assert classes["8955-0000050349"] == "looks_canonical"


def test_card_includes_orders_only_by_class_summary(auth_client):
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [
            {"id": "o1", "po_number": "TEST"},
            {"id": "o2", "po_number": "RFQ882023"},
            {"id": "o3", "po_number": "RFQ Gowns"},
            {"id": "o4", "po_number": "10820146"},
        ])
        c.commit()
    out = _build_scprs_reconcile_card()
    summary = out.get("orders_only_by_class", {})
    assert summary.get("sentinel") == 1
    assert summary.get("rfq_as_po") == 2
    assert summary.get("bare_numeric_unknown") == 1


# ── Sentinel cleanup endpoint ─────────────────────────────────────────


def test_sentinel_cleanup_dry_run_lists_without_writing(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "TEST"},
            {"id": "o2", "po_number": "8955-0000076737"},  # legit
        ])
        c.commit()
    resp = auth_client.post(
        "/api/admin/scprs-orders-only-sentinel-cleanup?dry_run=1"
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["dry_run"] is True
    assert data["candidates"] == 1
    assert data["rows_updated"] == 0
    assert data["samples"][0]["po_number"] == "TEST"
    # Verify DB untouched
    with get_db() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["is_test"] == 0


def test_sentinel_cleanup_apply_flips_is_test(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "TEST"},
            {"id": "o2", "po_number": "N/A"},
            {"id": "o3", "po_number": "RFQ Gowns"},  # NOT a sentinel
        ])
        c.commit()
    resp = auth_client.post(
        "/api/admin/scprs-orders-only-sentinel-cleanup"
    )
    data = resp.get_json()
    assert data["candidates"] == 2
    assert data["rows_updated"] == 2
    with get_db() as c:
        rows = {r["id"]: r["is_test"] for r in c.execute(
            "SELECT id, is_test FROM orders ORDER BY id"
        ).fetchall()}
    assert rows["o1"] == 1
    assert rows["o2"] == 1
    assert rows["o3"] == 0  # untouched


def test_sentinel_cleanup_idempotent(auth_client):
    """After flipping, the rows drop out of the WHERE is_test=0 filter,
    so a second run finds nothing."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "TEST"},
        ])
        c.commit()
    auth_client.post("/api/admin/scprs-orders-only-sentinel-cleanup")
    resp = auth_client.post("/api/admin/scprs-orders-only-sentinel-cleanup")
    data = resp.get_json()
    assert data["candidates"] == 0
    assert data["rows_updated"] == 0


def test_sentinel_cleanup_does_not_touch_canonical_pos(auth_client):
    """Defensive: a row with `8955-0000050349` (canonical CalVet PO,
    looks_canonical class) must NEVER be flipped to is_test."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [
            {"id": "o1", "po_number": "8955-0000050349"},
            {"id": "o2", "po_number": "10820146"},  # bare_numeric
        ])
        c.commit()
    resp = auth_client.post(
        "/api/admin/scprs-orders-only-sentinel-cleanup?dry_run=1"
    )
    data = resp.get_json()
    assert data["candidates"] == 0
    samples = [s["po_number"] for s in data["samples"]]
    assert "8955-0000050349" not in samples
    assert "10820146" not in samples


def test_sentinel_cleanup_returns_zero_when_no_orders(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_orders(c, [])
        c.commit()
    resp = auth_client.post(
        "/api/admin/scprs-orders-only-sentinel-cleanup?dry_run=1"
    )
    data = resp.get_json()
    assert data["candidates"] == 0
