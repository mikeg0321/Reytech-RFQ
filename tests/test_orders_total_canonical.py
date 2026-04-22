"""O-28 regression: header `orders.total` can drift from the sum of line
extendeds because writes to `order_line_items` (qty, unit_price) don't
cascade to `orders.total` in every code path. On prod this surfaced as a
$460.92 gap between `/api/orders/kpi` (read header) and
`/api/order/<oid>/margins` (compute from lines).

Fix: make `get_order()` and `list_orders()` canonicalize `order["total"]`
from line extendeds when normalized lines exist. Reads are definitionally
consistent across every endpoint; no write-path changes, no backfill.

Tests here seed a header/line drift directly via SQL and assert the DAL
rewrites `total` on the way out.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime


def _seed_drifted_order(temp_data_dir, oid, header_total, lines):
    """Seed an order whose header total intentionally disagrees with the
    sum of its line extendeds. `lines` is a list of (qty, unit_price)."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()

    conn.execute(
        """INSERT OR REPLACE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, f"Q-{oid}", f"PO-{oid}", "cchcs", "CCHCS", header_total, "new",
         "Test Buyer", "buyer@cchcs.ca.gov", now, now, ""),
    )
    conn.execute("DELETE FROM order_line_items WHERE order_id=?", (oid,))
    for i, (qty, price) in enumerate(lines, start=1):
        conn.execute(
            """INSERT INTO order_line_items
               (order_id, line_number, description, qty_ordered, unit_price,
                unit_cost, extended_price, extended_cost,
                sourcing_status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (oid, i, f"Item {i}", qty, price, 0,
             round(qty * price, 2), 0, "pending", now, now),
        )
    conn.commit()
    conn.close()


def _seed_order_without_normalized_lines(temp_data_dir, oid, header_total):
    """Seed an order with ONLY the header row — no rows in order_line_items.
    Simulates a legacy order that predates normalized storage."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            items, buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, f"Q-{oid}", f"PO-{oid}", "cchcs", "CCHCS", header_total, "new",
         "[]", "Test Buyer", "buyer@cchcs.ca.gov", now, now, ""),
    )
    conn.execute("DELETE FROM order_line_items WHERE order_id=?", (oid,))
    conn.commit()
    conn.close()


def test_get_order_recomputes_total_from_lines(temp_data_dir):
    """get_order() returns line-sum as canonical total, not the drifted header."""
    from src.core.order_dal import get_order

    _seed_drifted_order(temp_data_dir, "ORD-O28-A",
                        header_total=6408.24,
                        lines=[(1, 1000.0), (3, 1500.0), (2, 223.66)])

    order = get_order("ORD-O28-A")
    assert order is not None
    # 1*1000 + 3*1500 + 2*223.66 = 5947.32
    assert order["total"] == 5947.32, (
        f"O-28 regression: get_order returned total={order['total']!r}, "
        f"expected 5947.32 (line-sum). Header was 6408.24 (drifted).")


def test_list_orders_recomputes_total_from_lines(temp_data_dir):
    """list_orders() applies the same canonicalization as get_order()."""
    from src.core.order_dal import load_orders_dict

    _seed_drifted_order(temp_data_dir, "ORD-O28-B",
                        header_total=9999.99,
                        lines=[(5, 20.0), (10, 5.0)])

    orders = load_orders_dict()
    assert "ORD-O28-B" in orders
    # 5*20 + 10*5 = 150
    assert orders["ORD-O28-B"]["total"] == 150.0


def test_empty_lines_preserves_header_total(temp_data_dir):
    """When no normalized lines exist, header total is preserved — there's
    nothing authoritative to recompute from."""
    from src.core.order_dal import get_order

    _seed_order_without_normalized_lines(temp_data_dir, "ORD-O28-C",
                                         header_total=250.0)

    order = get_order("ORD-O28-C")
    assert order is not None
    assert order["total"] == 250.0, (
        "get_order must preserve header total when no normalized lines exist.")


def test_kpi_and_margins_agree_after_canonicalization(auth_client, temp_data_dir):
    """The prod drift ($460.92 between /api/orders/kpi total_revenue and
    /api/order/<oid>/margins total_revenue) must be zero after O-28."""
    _seed_drifted_order(temp_data_dir, "ORD-O28-KPI",
                        header_total=1000.0,   # drifted header
                        lines=[(1, 100.0), (2, 50.0), (4, 25.0)])
    # line-sum = 100 + 100 + 100 = 300

    margins_resp = auth_client.get("/api/order/ORD-O28-KPI/margins")
    assert margins_resp.status_code == 200
    margins = margins_resp.get_json()
    assert margins.get("ok") is True, margins
    assert margins["total_revenue"] == 300.0

    kpi_resp = auth_client.get("/api/orders/kpi")
    assert kpi_resp.status_code == 200
    kpi = kpi_resp.get_json()
    # Find our order's contribution — kpi aggregates across all orders, so
    # compare the totals against the seeded order if it's the only one
    # included in the test DB. Use top_agencies CCHCS bucket as a proxy.
    cchcs_bucket = next((a for a in kpi.get("top_agencies", [])
                         if a.get("agency", "").lower() == "cchcs"), None)
    assert cchcs_bucket is not None, (
        f"kpi missing cchcs agency bucket: {kpi.get('top_agencies')!r}")
    # cchcs bucket's value must reflect the CANONICAL 300, not the drifted 1000.
    assert cchcs_bucket["value"] == 300.0, (
        f"O-28 regression: /api/orders/kpi cchcs bucket still reads drifted "
        f"header (${cchcs_bucket['value']}). After canonicalization it must "
        f"equal the margins-endpoint line-sum ($300).")
