"""Tests for /api/admin/scprs-only-stub-generate.

The endpoint creates orders rows for every SCPRS-known canonical PO
that has no corresponding orders entry. Each stub is flagged with
status='stub' so operator surfaces can prompt review/enrichment.

Idempotent via id=`STUB-{canonical_po}` PRIMARY KEY + INSERT OR IGNORE.
Re-running after a successful generate skips already-stubbed rows.
"""
from __future__ import annotations

import json

import pytest


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
            (po_number, business_unit, dept_name, start_date,
             grand_total, items_json, imported_at)
            VALUES (?,?,?,?,?,?,?)
        """, (r["po_number"], r["business_unit"],
              r.get("dept_name", ""), r.get("start_date", ""),
              float(r.get("grand_total", 0)),
              r.get("items_json", "[]"),
              r.get("imported_at", "2026-04-29 00:00:00")))


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
              r["po_number"], r.get("agency", ""),
              r.get("institution", ""),
              float(r.get("total", 0)),
              r.get("status", "open"), "[]",
              "2026-04-28T10:00:00", "2026-04-28T10:00:00",
              int(r.get("is_test", 0))))


# ── BU → agency label ─────────────────────────────────────────────────


@pytest.mark.parametrize("bu,expected", [
    ("8955",       "CalVet"),
    ("'8955",      "CalVet"),
    ("4440",       "DSH"),
    ("'4440",      "DSH"),
    ("5225",       "CCHCS"),
    ("'5225",      "CCHCS"),
    ("",           ""),
    ("XXXX",       ""),
    (None,         ""),
])
def test_bu_to_agency_label(bu, expected):
    from src.api.modules.routes_intel_ops import _scprs_bu_to_agency_label
    assert _scprs_bu_to_agency_label(bu) == expected


# ── Endpoint behavior ─────────────────────────────────────────────────


def test_dry_run_lists_candidates_without_inserting(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Dept of Veterans Affairs",
             "start_date": "2026-03-30", "grand_total": 87609.27,
             "items_json": '[{"line_num":"1","description":"GAUZE"}]'},
        ])
        _seed_orders(c, [])
        c.commit()

    resp = auth_client.post(
        "/api/admin/scprs-only-stub-generate?dry_run=1"
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["dry_run"] is True
    assert data["candidates"] == 1
    assert data["rows_inserted"] == 0
    assert data["inserts"][0]["stub_id"] == "STUB-8955-0000076737"
    assert data["inserts"][0]["canonical"] == "8955-0000076737"
    assert data["inserts"][0]["agency"] == "CalVet"
    assert data["inserts"][0]["total"] == 87609.27

    # DB untouched
    with get_db() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    assert n["n"] == 0


def test_apply_inserts_stub_with_correct_fields(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Dept of Veterans Affairs",
             "start_date": "2026-03-30", "grand_total": 87609.27,
             "items_json": '[{"line_num":"1","description":"GAUZE"}]'},
        ])
        _seed_orders(c, [])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    assert data["rows_inserted"] == 1
    assert data["candidates"] == 1

    with get_db() as c:
        row = c.execute(
            "SELECT * FROM orders WHERE id = 'STUB-8955-0000076737'"
        ).fetchone()
    assert row is not None
    assert row["po_number"] == "8955-0000076737"
    assert row["agency"] == "CalVet"
    assert row["status"] == "stub"
    assert row["total"] == 87609.27
    assert row["po_date"] == "2026-03-30"
    assert row["is_test"] == 0
    items = json.loads(row["items"])
    assert items[0]["description"] == "GAUZE"
    assert "auto-stub from SCPRS" in row["notes"]


def test_idempotent_re_apply_skips_already_stubbed(auth_client):
    """Re-running generates the same stub_id — INSERT OR IGNORE
    leaves the original row + the second apply reports
    rows_skipped_already_stubbed."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Dept of Veterans Affairs",
             "grand_total": 87609.27},
        ])
        _seed_orders(c, [])
        c.commit()

    auth_client.post("/api/admin/scprs-only-stub-generate")
    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    # Second pass: candidates=0 because the stub now exists in
    # existing_pos via canonical match
    assert data["candidates"] == 0
    assert data["rows_inserted"] == 0


def test_skips_pos_already_in_orders_canonical(auth_client):
    """If orders.po_number already has the canonical form, no stub
    is generated (it's an exact_match, not scprs_only)."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Dept of Veterans Affairs",
             "grand_total": 87609.27},
        ])
        _seed_orders(c, [
            {"id": "real-001", "po_number": "8955-0000076737",
             "total": 87609.27},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    assert data["candidates"] == 0


def test_skips_pos_already_in_orders_bare(auth_client):
    """If orders.po_number has the bare form (format_drift case),
    no stub is generated — that's a different cleanup (PR #642)."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Dept of Veterans Affairs",
             "grand_total": 100},
        ])
        _seed_orders(c, [
            {"id": "real-001", "po_number": "0000076737"},
        ])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    assert data["candidates"] == 0


def test_handles_dsh(auth_client):
    """DSH parity — `4440-` prefix assembled, agency label `DSH`."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000063878", "business_unit": "4440",
             "dept_name": "Department of State Hospitals",
             "grand_total": 4389.98, "start_date": "2025-12-12"},
        ])
        _seed_orders(c, [])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    assert data["rows_inserted"] == 1
    with get_db() as c:
        row = c.execute(
            "SELECT * FROM orders WHERE id = 'STUB-4440-0000063878'"
        ).fetchone()
    assert row["po_number"] == "4440-0000063878"
    assert row["agency"] == "DSH"


def test_handles_cchcs_no_dash(auth_client):
    """CCHCS — BU 5225 maps to po_doc as-is (4500... prefix already
    in the PO number itself)."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "4500752793", "business_unit": "5225",
             "dept_name": "Dept of Corrections & Rehab",
             "grand_total": 952.65},
        ])
        _seed_orders(c, [])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    assert data["rows_inserted"] == 1
    with get_db() as c:
        row = c.execute(
            "SELECT * FROM orders WHERE id = 'STUB-4500752793'"
        ).fetchone()
    assert row["po_number"] == "4500752793"
    assert row["agency"] == "CCHCS"


def test_multi_canonical_batch(auth_client):
    """Three POs across all three agencies → three stubs in one shot."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "VA", "grand_total": 100},
            {"po_number": "0000063878", "business_unit": "4440",
             "dept_name": "DSH", "grand_total": 200},
            {"po_number": "4500752793", "business_unit": "5225",
             "dept_name": "CDCR", "grand_total": 300},
        ])
        _seed_orders(c, [])
        c.commit()

    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    data = resp.get_json()
    assert data["rows_inserted"] == 3
    with get_db() as c:
        rows = {r["id"]: r["agency"] for r in c.execute(
            "SELECT id, agency FROM orders WHERE id LIKE 'STUB-%'"
        ).fetchall()}
    assert rows["STUB-8955-0000076737"] == "CalVet"
    assert rows["STUB-4440-0000063878"] == "DSH"
    assert rows["STUB-4500752793"] == "CCHCS"


def test_zero_when_no_scprs_data(auth_client):
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [])
        c.commit()
    resp = auth_client.post(
        "/api/admin/scprs-only-stub-generate?dry_run=1"
    )
    data = resp.get_json()
    assert data["candidates"] == 0


def test_stub_carries_items_json_verbatim(auth_client):
    """SCPRS items_json (line_num/item_id/description/unspsc) is
    stored AS-IS in orders.items. Operator enriches qty/unit_price
    on review (not in this PR's scope)."""
    from src.core.db import get_db
    items = '[{"line_num":"1","item_id":"","description":"BANDAGE","unspsc":"42311500"}]'
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "items_json": items, "grand_total": 100},
        ])
        _seed_orders(c, [])
        c.commit()
    resp = auth_client.post("/api/admin/scprs-only-stub-generate")
    assert resp.get_json()["rows_inserted"] == 1
    with get_db() as c:
        row = c.execute(
            "SELECT items FROM orders WHERE id = 'STUB-8955-0000076737'"
        ).fetchone()
    parsed = json.loads(row["items"])
    assert parsed[0]["unspsc"] == "42311500"
    assert parsed[0]["description"] == "BANDAGE"


def test_reconcile_flips_to_exact_match_after_stub(auth_client):
    """After a stub lands, the reconciler counts that PO as
    exact_match (canonical PO is now in orders), draining the
    scprs_only bucket. Cause-and-effect proof."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "VA", "grand_total": 100},
        ])
        _seed_orders(c, [])
        c.commit()

    pre = _build_scprs_reconcile_card()
    assert pre["scprs_only"] == 1
    assert pre["exact_match"] == 0

    auth_client.post("/api/admin/scprs-only-stub-generate")

    post = _build_scprs_reconcile_card()
    assert post["scprs_only"] == 0
    assert post["exact_match"] == 1
