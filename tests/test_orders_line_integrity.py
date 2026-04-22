"""Pin the supplier-alias-shadow fix in api_order_update_line.

save_line_items_batch reads `supplier_name` (the normalized column) before
the `supplier` alias. If the route only writes the alias, the batch save
re-reads the stale `supplier_name` from the existing row and the user's
edit silently reverts. Fix: mirror supplier → supplier_name on write.

Companion coverage for the other audit-2026-04-21 line-integrity fixes
lives in:
- tests/test_orders_v2_line_cost_persists.py (O-1 line_cost → unit_cost)
- tests/test_orders_v2_line_edits_persist.py (update_line, bulk, invoice)
- tests/test_orders_p1_data_integrity.py   (O-12/O-13/O-15)
"""
from datetime import datetime


def _seed_order_with_line(order_id="ORD-SUP-01", supplier_name="OldVendor"):
    from src.core.db import get_db
    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO orders (id, po_number, status, total, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?)",
            (order_id, "PO-" + order_id[-4:], "new", 100.0, now, now),
        )
        conn.execute(
            "INSERT INTO order_line_items "
            "(order_id, line_number, description, qty_ordered, unit_price, unit_cost, "
            " supplier_name, sourcing_status, extended_price, extended_cost, "
            " created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (order_id, 1, "Widget-X", 2, 50.0, 0.0,
             supplier_name, "pending", 100.0, 0.0, now, now),
        )


def _line_row(order_id, line_number=1):
    from src.core.db import get_db
    with get_db() as conn:
        return dict(conn.execute(
            "SELECT * FROM order_line_items WHERE order_id=? AND line_number=?",
            (order_id, line_number),
        ).fetchone())


def test_supplier_edit_lands_in_supplier_name_column(auth_client, temp_data_dir):
    oid = "ORD-SUP-01"
    _seed_order_with_line(oid, supplier_name="OldVendor")

    resp = auth_client.post(f"/api/order/{oid}/line/L001",
                            json={"supplier": "NewVendor"})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    row = _line_row(oid)
    assert row["supplier_name"] == "NewVendor", (
        "supplier edit must propagate to supplier_name — save_line_items_batch "
        "reads supplier_name first, so a stale column value would otherwise "
        "shadow the user's edit on the next save."
    )
