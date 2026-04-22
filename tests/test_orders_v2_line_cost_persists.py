"""O-1 / O-11 regression: /api/order/<oid>/line/<lid>/cost must persist
unit_cost to the normalized order_line_items table.

Bug captured in project_orders_module_audit_2026_04_21:

    api_order_line_cost writes `it["cost"] = float(cost)` into the in-memory
    line dict, NOT `it["unit_cost"]`. Then calls `_save_orders(orders)`
    which rewrites every order via `save_line_items_batch`. That batch
    reads `item.get("unit_cost", item.get("cost", 0))` — since the loaded
    dict already has an `unit_cost` key (set by `_row_to_line_item`), the
    NEW cost value we just wrote to `it["cost"]` is ignored.

    Result: POST /api/order/.../cost returns {ok:true}, margin_pct looks
    right in the response, but `order_line_items.unit_cost` never changes.
    The margins panel on /order/<oid> continues to show 0.0% COST $0.00.

The fix is for the route to persist via the DAL (`update_line_status` or
equivalent) so the write lands on the authoritative column.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime


def _seed_order_with_line(temp_data_dir, oid="ORD-PO-O1-TEST", line_number=1,
                           unit_price=100.0, old_unit_cost=40.0):
    """Seed an order + one normalized line item directly via SQL."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()

    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, "Q-O1-1", "PO-O1-1", "cchcs", "CCHCS", unit_price, "new",
         "Test Buyer", "buyer@cchcs.ca.gov", now, now, ""),
    )
    cur = conn.execute(
        """INSERT INTO order_line_items
           (order_id, line_number, description, qty_ordered, unit_price,
            unit_cost, extended_price, extended_cost,
            sourcing_status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, line_number, "Widget", 1, unit_price, old_unit_cost,
         unit_price, old_unit_cost, "pending", now, now),
    )
    db_id = cur.lastrowid
    conn.commit()
    conn.close()
    return oid, f"L{line_number:03d}", db_id


def _read_unit_cost(temp_data_dir, oid, line_number=1):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT unit_cost FROM order_line_items WHERE order_id=? AND line_number=?",
        (oid, line_number),
    ).fetchone()
    conn.close()
    return row[0] if row else None


def test_line_cost_persists_to_unit_cost_column(auth_client, temp_data_dir):
    """Regression for O-1/O-11: POST /api/order/<oid>/line/<lid>/cost must
    write the new cost to `order_line_items.unit_cost`, not just to the
    in-memory `cost` alias that gets lost on the next read."""
    oid, lid, _db_id = _seed_order_with_line(
        temp_data_dir, unit_price=100.0, old_unit_cost=40.0)

    resp = auth_client.post(
        f"/api/order/{oid}/line/{lid}/cost",
        json={"cost": 55.25},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True, body

    persisted = _read_unit_cost(temp_data_dir, oid)
    assert persisted == 55.25, (
        f"O-1 regression: POST /line/{lid}/cost did not persist to "
        f"order_line_items.unit_cost — read back {persisted!r}. "
        f"The route writes `it['cost']` but save_line_items_batch reads "
        f"`item.get('unit_cost', item.get('cost', 0))` — so the OLD "
        f"unit_cost wins on round-trip."
    )


def test_line_cost_returns_correct_margin_pct(auth_client, temp_data_dir):
    """The /cost endpoint's response must reflect the margin computed from
    the NEW cost (sanity check — fails today if the endpoint is totally
    broken, passes today if only the persistence side is broken)."""
    oid, lid, _db_id = _seed_order_with_line(
        temp_data_dir, unit_price=100.0, old_unit_cost=40.0)

    resp = auth_client.post(
        f"/api/order/{oid}/line/{lid}/cost",
        json={"cost": 75.0},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    # margin_pct = (100 - 75) / 100 * 100 = 25.0
    assert body.get("margin_pct") == 25.0, body
