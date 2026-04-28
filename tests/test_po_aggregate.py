"""Tests for `po_aggregate` SQL view + the /health/quoting card it feeds.

The drift card's prior "DUP POs" counter conflated two distinct
populations:
  1. Operator typos: same po_number entered on two unrelated quotes
     by mistake. Real bugs.
  2. Multi-quote POs: one buyer PO legitimately spans multiple
     awarded quotes. Real-world data — verified on prod where PO
     `0000053217` covers 7 quotes.

These tests lock the view + the card so the operator can distinguish
the two without shelling into prod SQLite. The view is derived from
`orders` so there's no sync to drift; deleting test data and re-
running just changes the result, no migration repair needed.

The view is NAMED `po_aggregate` — a separate `purchase_orders`
table already exists in routes_order_tracking.py for PO email
tracking. They are unrelated; do not conflate.
"""
from __future__ import annotations

from datetime import datetime

import pytest


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_order(conn, *, order_id, quote_number="", po_number="",
                total=100.0, agency="CDCR", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, "",
          total, "open", "[]", when, when, is_test))


def _build_card():
    from src.api.modules.routes_health import _build_po_aggregate_card
    return _build_po_aggregate_card()


# ── View existence + basic shape ────────────────────────────────────────


def test_po_aggregate_view_exists():
    """View must exist after init_db. Without it the card stays empty
    silently — which would hide the divergence we built it to see."""
    with _conn() as c:
        rows = c.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='view' AND name='po_aggregate'"
        ).fetchall()
    assert len(rows) == 1


def test_po_aggregate_columns_match_card_expectations():
    with _conn() as c:
        cols = c.execute(
            "SELECT name FROM pragma_table_info('po_aggregate')"
        ).fetchall()
        names = {row["name"] for row in cols}
    for k in ("po_number", "quote_count", "total_amount",
              "agency", "is_test"):
        assert k in names


# ── View correctness ────────────────────────────────────────────────────


def test_view_aggregates_one_row_per_distinct_po_number():
    """Two orders rows sharing one po_number = one view row,
    quote_count=2, total_amount=sum of children."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="o1", quote_number="Q1",
                    po_number="PO-MULTI", total=100.0)
        _seed_order(c, order_id="o2", quote_number="Q2",
                    po_number="PO-MULTI", total=200.0)
        _seed_order(c, order_id="o3", quote_number="Q3",
                    po_number="PO-SOLO", total=50.0)
        c.commit()
        rows = c.execute(
            "SELECT po_number, quote_count, total_amount FROM po_aggregate "
            "ORDER BY po_number"
        ).fetchall()
    assert len(rows) == 2
    multi = next(r for r in rows if r["po_number"] == "PO-MULTI")
    solo = next(r for r in rows if r["po_number"] == "PO-SOLO")
    assert multi["quote_count"] == 2
    assert float(multi["total_amount"]) == 300.0
    assert solo["quote_count"] == 1
    assert float(solo["total_amount"]) == 50.0


def test_view_skips_blank_and_null_po_numbers():
    """Orders with empty/NULL po_number aren't a PO yet. They're a
    different signal — already counted by orders_drift.orders_no_po."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="")
        _seed_order(c, order_id="o2", quote_number="Q2", po_number="PO-1")
        c.commit()
        n = c.execute(
            "SELECT COUNT(*) AS n FROM po_aggregate"
        ).fetchone()["n"]
    assert n == 1


def test_view_reflects_orders_changes_immediately():
    """Views read live — no sync to maintain. Updating a child orders
    row immediately changes the aggregate, which is the whole reason
    we used a view rather than a parent table."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="o1", quote_number="Q1",
                    po_number="PO-LIVE", total=100.0)
        c.commit()
        before = c.execute(
            "SELECT total_amount FROM po_aggregate WHERE po_number = ?",
            ("PO-LIVE",)
        ).fetchone()
        assert float(before["total_amount"]) == 100.0
        c.execute(
            "UPDATE orders SET total = 999 WHERE id = ?", ("o1",)
        )
        c.commit()
        after = c.execute(
            "SELECT total_amount FROM po_aggregate WHERE po_number = ?",
            ("PO-LIVE",)
        ).fetchone()
    assert float(after["total_amount"]) == 999.0


def test_view_propagates_is_test_from_orders():
    """A PO whose orders are all is_test=1 must show is_test=1 in the
    view too — otherwise the card excludes it incorrectly."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="o1", quote_number="T1",
                    po_number="PO-TEST", is_test=1)
        c.commit()
        row = c.execute(
            "SELECT is_test FROM po_aggregate WHERE po_number = ?",
            ("PO-TEST",)
        ).fetchone()
    assert int(row["is_test"]) == 1


# ── Card builder ────────────────────────────────────────────────────────


def test_card_unknown_when_view_empty():
    with _conn() as c:
        _wipe(c)
    out = _build_card()
    assert out["status"] == "unknown"
    assert out["total_pos"] == 0
    assert out["multi_quote_pos"] == 0
    assert out["biggest_pos"] == []


def test_card_distinguishes_single_and_multi_quote_pos():
    """The whole point of this card: distinguish legitimate
    multi-quote POs from operator typos so the operator doesn't
    panic at the orders-drift "DUP POs" counter."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="SINGLE-1")
        _seed_order(c, order_id="o2", quote_number="Q2", po_number="SINGLE-2")
        _seed_order(c, order_id="o3", quote_number="Q3", po_number="MULTI-1")
        _seed_order(c, order_id="o4", quote_number="Q4", po_number="MULTI-1")
        _seed_order(c, order_id="o5", quote_number="Q5", po_number="MULTI-1")
        c.commit()
    out = _build_card()
    assert out["total_pos"] == 3
    assert out["single_quote_pos"] == 2
    assert out["multi_quote_pos"] == 1
    assert out["max_quote_count"] == 3
    assert out["status"] == "healthy"


def test_card_biggest_pos_lists_only_multi_quote():
    """Single-quote POs aren't interesting; the drawer surfaces the
    multi-quote ones because they're the operator's actionable view."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="o1", quote_number="Q1", po_number="SOLO")
        _seed_order(c, order_id="o2", quote_number="Q2", po_number="MULTI-A")
        _seed_order(c, order_id="o3", quote_number="Q3", po_number="MULTI-A")
        c.commit()
    out = _build_card()
    assert len(out["biggest_pos"]) == 1
    assert out["biggest_pos"][0]["po_number"] == "MULTI-A"
    assert out["biggest_pos"][0]["quote_count"] == 2


def test_card_excludes_test_pos():
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="oT", quote_number="QT",
                    po_number="TESTPO", is_test=1)
        _seed_order(c, order_id="oR", quote_number="QR",
                    po_number="REALPO", is_test=0)
        c.commit()
    out = _build_card()
    assert out["total_pos"] == 1


# ── /health/quoting integration ─────────────────────────────────────────


def test_health_quoting_json_includes_po_aggregate(auth_client):
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "po_aggregate" in data
    po = data["po_aggregate"]
    for k in ("status", "total_pos", "single_quote_pos",
              "multi_quote_pos", "max_quote_count", "biggest_pos"):
        assert k in po


def test_health_quoting_html_renders_po_aggregate_card(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "PO aggregate" in body
    assert "MULTI-QUOTE" in body or "MULTI" in body
