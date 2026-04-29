"""Tests for the SCPRS ↔ orders reconciliation card on /health/quoting.

The card joins `scprs_reytech_wins` (state's record of awarded POs,
populated from the Detail Information XLS export) against the
operational `orders` table. Four buckets:
  exact_match    — same canonical PO string in both
  format_drift   — same canonical, but orders has the bare/altered form
  scprs_only     — SCPRS knows it, orders doesn't
  orders_only    — orders has it, SCPRS doesn't

Status thresholds:
  unknown — wins_count = 0 (no SCPRS imports yet)
  error   — any scprs_only or orders_only > 0
  warn    — format_drift > 0 (parse-bug stragglers, fixable)
  healthy — all wins matched, no drift
"""
from __future__ import annotations

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
            (po_number, business_unit, dept_name, start_date, grand_total,
             items_json)
            VALUES (?,?,?,?,?,?)
        """, (r["po_number"], r.get("business_unit", ""),
              r.get("dept_name", ""), r.get("start_date", ""),
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
        """, (r.get("id"), r.get("quote_number", "Q1"),
              r["po_number"], r.get("agency", "CDCR"),
              r.get("institution", "Inst"),
              float(r.get("total", 100)), "open", "[]",
              "2026-04-28T10:00:00", "2026-04-28T10:00:00",
              int(r.get("is_test", 0))))


def test_card_unknown_when_no_scprs_imports(auth_client):
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["status"] == "unknown"
    assert out["wins_count"] == 0


def test_card_healthy_when_all_match(auth_client):
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 87609},
            {"po_number": "4500752793", "business_unit": "5225",
             "dept_name": "Corrections & Rehab", "grand_total": 952},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "8955-0000076737",
             "quote_number": "Q1", "total": 87609},
            {"id": "o2", "po_number": "4500752793",
             "quote_number": "Q2", "total": 952},
        ])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["status"] == "healthy"
    assert out["exact_match"] == 2
    assert out["format_drift"] == 0
    assert out["scprs_only"] == 0
    assert out["orders_only"] == 0


def test_card_warn_on_format_drift(auth_client):
    """orders has bare CalVet PO `0000076737`, SCPRS canonical is
    `8955-0000076737` → format_drift bucket. Status: warn."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 87609},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "0000076737",  # bare, no prefix
             "quote_number": "Q1", "total": 87609},
        ])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["status"] == "warn"
    assert out["format_drift"] == 1
    assert out["exact_match"] == 0
    assert out["scprs_only"] == 0
    drift = out["samples"]["format_drift"][0]
    assert drift["canonical"] == "8955-0000076737"
    assert drift["stored_po"] == "0000076737"


def test_card_error_on_scprs_only(auth_client):
    """SCPRS has it, orders doesn't → scprs_only. Status: error."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000076737", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 87609},
        ])
        _seed_orders(c, [])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["status"] == "error"
    assert out["scprs_only"] == 1
    assert out["samples"]["scprs_only"][0]["canonical"] == "8955-0000076737"


def test_card_error_on_orders_only(auth_client):
    """orders has it, SCPRS doesn't → orders_only. Status: error.
    Common case: very recent PO not yet indexed by state, OR truly
    orphan (RFQ-in-po, parse bug we never closed)."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [
            {"id": "o1", "po_number": "RFQ882023",
             "quote_number": "Q1", "total": 181000,
             "institution": "Mystery"},
        ])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["status"] == "error"
    assert out["orders_only"] == 1
    assert out["samples"]["orders_only"][0]["po_number"] == "RFQ882023"


def test_card_excludes_test_orders(auth_client):
    """is_test=1 rows must not pollute the reconciler."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [
            {"id": "o1", "po_number": "TEST123", "is_test": 1},
        ])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["orders_with_po"] == 0
    assert out["orders_only"] == 0


def test_card_samples_capped_at_20(auth_client):
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": f"000007{i:04d}", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 1000 + i}
            for i in range(30)
        ])
        _seed_orders(c, [])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["scprs_only"] == 30
    assert len(out["samples"]["scprs_only"]) == 20


def test_card_samples_sorted_by_total_desc(auth_client):
    """Biggest $ first — operator chases the most expensive gaps."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000000001", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 100},
            {"po_number": "0000000002", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 5000},
            {"po_number": "0000000003", "business_unit": "8955",
             "dept_name": "Veterans Affairs", "grand_total": 750},
        ])
        _seed_orders(c, [])
        c.commit()
    out = _build_scprs_reconcile_card()
    samples = out["samples"]["scprs_only"]
    assert [s["total"] for s in samples] == [5000, 750, 100]


def test_card_includes_watcher_status(auth_client):
    """Card surfaces the auto-ingest watcher's running state inline."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert "watcher" in out
    assert "running" in out["watcher"]


def test_endpoint_scprs_reconcile_returns_card(auth_client):
    """JSON endpoint /api/admin/scprs-reconcile == card data."""
    from src.core.db import get_db
    with get_db() as c:
        _seed_wins(c, [])
        _seed_orders(c, [])
        c.commit()
    resp = auth_client.get("/api/admin/scprs-reconcile")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "exact_match" in data
    assert "format_drift" in data
    assert "scprs_only" in data
    assert "orders_only" in data


def test_card_handles_dsh_canonical(auth_client):
    """DSH — same logic as CalVet, dashed prefix `4440-`."""
    from src.core.db import get_db
    from src.api.modules.routes_health import _build_scprs_reconcile_card
    with get_db() as c:
        _seed_wins(c, [
            {"po_number": "0000063878", "business_unit": "4440",
             "dept_name": "Department of State Hospitals",
             "grand_total": 4389},
        ])
        _seed_orders(c, [
            {"id": "o1", "po_number": "4440-0000063878",
             "quote_number": "Q1", "total": 4389},
        ])
        c.commit()
    out = _build_scprs_reconcile_card()
    assert out["exact_match"] == 1
    assert out["status"] == "healthy"
