"""Tests for /api/admin/scprs-format-drift-fix.

The endpoint rewrites orders.po_number from bare to canonical form
when the SCPRS reconciler detects format_drift. SCPRS = ground truth
(Mike confirmed 2026-04-28); the parse-bug-stripped DB rows lose.

Idempotent: re-running after a successful fix returns 0 candidates.
Per-row isolation: a constraint violation on one row doesn't kill
the rest of the batch.
"""
from __future__ import annotations

import pytest


def _seed_wins(conn, rows):
    """Create scprs_reytech_wins + insert rows. Schema matches
    scripts/import_scprs_reytech_wins._ensure_table."""
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
        """, (r["po_number"], r["business_unit"],
              r.get("dept_name", ""), float(r.get("grand_total", 0)), "[]"))


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


def test_dry_run_lists_candidates_without_writing(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 24710.99},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000057329"},
        ])
        c.commit()

    resp = auth_client.post(
        "/api/admin/scprs-format-drift-fix?dry_run=1"
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["dry_run"] is True
    assert data["candidates"] == 1
    assert data["rows_updated"] == 0
    assert data["updates"][0]["from_po"] == "0000057329"
    assert data["updates"][0]["to_po"] == "8955-0000057329"

    # Verify DB is untouched
    with get_db() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["po_number"] == "0000057329"


def test_apply_updates_orders_po_number(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 24710.99},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000057329"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["dry_run"] is False
    assert data["candidates"] == 1
    assert data["rows_updated"] == 1

    with get_db() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["po_number"] == "8955-0000057329"


def test_idempotent_second_apply_finds_zero(auth_client):
    """After a successful fix, the bare po_number is gone, so the
    drift detector finds nothing on a second run."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 24710.99},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000057329"},
        ])
        c.commit()

    auth_client.post("/api/admin/scprs-format-drift-fix")
    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["candidates"] == 0
    assert data["rows_updated"] == 0


def test_handles_dsh_drift(auth_client):
    """DSH parity check — `4440-` prefix path should also fix."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000051233", "business_unit": "4440",
             "dept_name": "Department of State Hospitals",
             "grand_total": 3444.92},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000051233"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    data = resp.get_json()
    assert data["rows_updated"] == 1
    with get_db() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["po_number"] == "4440-0000051233"


def test_skips_cchcs_no_drift(auth_client):
    """CCHCS PO already has 4500 baked in (BU=5225 → po_doc as-is).
    No drift should be detected even if both rows match exactly."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "4500752793", "business_unit": "5225",
             "dept_name": "Corrections & Rehab", "grand_total": 952},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "4500752793"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix?dry_run=1")
    data = resp.get_json()
    assert data["candidates"] == 0


def test_does_not_touch_test_orders(auth_client):
    """is_test=1 rows must be left alone even if they superficially
    look like a drift candidate."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 100},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000057329", "is_test": 1},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    data = resp.get_json()
    assert data["candidates"] == 0   # is_test row excluded from order_pos
    with get_db() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id='o1'"
        ).fetchone()
    assert row["po_number"] == "0000057329"   # untouched


def test_constraint_violation_isolated_per_row(auth_client):
    """If a UNIQUE-constraint conflict occurs on one drift row (the
    canonical form already exists with the SAME quote_number), we
    surface it in failures[] and the batch continues with other rows."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 24710},
            {"po_number": "0000051233", "business_unit": "4440",
             "dept_name": "Department of State Hospitals",
             "grand_total": 3444},
        ])
        _seed_orders(c, [
            # Drift candidate — bare CalVet
            {"id": "o1", "po_number": "0000057329", "quote_number": "Q1"},
            # Drift candidate — bare DSH
            {"id": "o2", "po_number": "0000051233", "quote_number": "Q2"},
            # Already-canonical CalVet — constraint blocks updating o1
            # to "8955-0000057329" if quote_number also matches Q1
            {"id": "o3", "po_number": "8955-0000057329",
             "quote_number": "Q1"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    data = resp.get_json()
    # Both candidates seen
    assert data["candidates"] == 2
    # DSH update succeeded
    assert data["rows_updated"] >= 1
    # CalVet either succeeded or failed — but the batch didn't crash
    assert "rows_failed" in data
    # DSH definitely landed
    with get_db() as c:
        dsh = c.execute(
            "SELECT po_number FROM orders WHERE id='o2'"
        ).fetchone()
    assert dsh["po_number"] == "4440-0000051233"


def test_returns_zero_when_scprs_table_empty(auth_client):
    """No SCPRS data → no canonical to compare → 0 candidates."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000057329"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix?dry_run=1")
    data = resp.get_json()
    assert data["candidates"] == 0
    assert data["dry_run"] is True


def test_does_not_touch_orders_already_canonical(auth_client):
    """orders rows already storing the canonical form must NOT be
    re-updated — the drift detector requires `bare in order_pos`,
    which a canonical entry doesn't satisfy."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 100},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "8955-0000057329"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    data = resp.get_json()
    assert data["candidates"] == 0
    assert data["rows_updated"] == 0


def test_updates_all_orders_sharing_bare_po(auth_client):
    """Multi-quote PO: if 2 orders rows share the bare po_number
    (different quote_numbers — legit per po_aggregate), BOTH should
    be updated to canonical in one shot."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000057329", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 100},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000057329", "quote_number": "Q1"},
            {"id": "o2", "po_number": "0000057329", "quote_number": "Q2"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-format-drift-fix")
    data = resp.get_json()
    assert data["candidates"] == 1   # one drift PO
    assert data["rows_updated"] == 2 # touched both quote_number rows
    with get_db() as c:
        rows = c.execute(
            "SELECT po_number FROM orders ORDER BY id"
        ).fetchall()
    assert all(r["po_number"] == "8955-0000057329" for r in rows)
