"""Orders V2 phase 4 — verify data_json blob read/write has stopped.

Regression guard for the final step of the Orders V2 migration. After
this PR lands, any save via dal.save_order or order_dal's save path
must NOT populate orders.data_json. The column itself still exists
for a 48h monitoring window; it will be dropped in a follow-up PR.
"""
import json
import sqlite3
import pytest


def _read_data_json(order_id: str):
    """Direct SQL read of the raw data_json column — bypasses the DAL
    so the test sees whatever (if anything) was actually written."""
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=10)
    row = conn.execute(
        "SELECT data_json FROM orders WHERE id = ?", (order_id,)
    ).fetchone()
    conn.close()
    return row[0] if row else None


class TestSaveOrderStopsDataJsonWrite:
    def test_canonical_save_order_does_not_write_blob(self, temp_data_dir):
        """Lock the V2 phase 4 invariant: orders.data_json is never written.

        Originally tested core.dal.save_order, which was deleted 2026-04-30
        (V1 DAL audit drift #1). The canonical writer in order_dal.save_order
        is now the only path; this test confirms it also doesn't touch the
        blob, so the column-drop follow-up stays safe."""
        from src.core.order_dal import save_order
        save_order(
            "ord_test_1",
            {
                "id": "ord_test_1",
                "order_id": "ord_test_1",
                "quote_number": "R26Q999",
                "agency": "cchcs",
                "po_number": "PO123",
                "total": 500.0,
                "items": [{"description": "Test Item", "qty": 5, "price": 100}],
                "line_items": [{"description": "Test Item", "qty": 5, "price": 100}],
                "status": "new",
                "created_at": "2026-04-14T12:00:00",
            },
            actor="test",
        )
        blob = _read_data_json("ord_test_1")
        assert blob is None or blob == "", \
            f"data_json was written: {blob[:120] if blob else ''}"

    def test_dal_get_order_ignores_stale_blob(self, temp_data_dir):
        """Even if a row exists with a stale data_json value (from
        before phase 4), the reader must return the items column, not
        the blob. Simulate the legacy state by writing the blob
        directly via SQL."""
        from src.core.db import DB_PATH
        from src.core.dal import get_order

        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            """INSERT INTO orders
               (id, quote_number, agency, po_number, total, items, status,
                created_at, updated_at, data_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("ord_stale", "R26Q888", "cchcs", "PO888", 300.0,
             json.dumps([{"description": "Fresh", "qty": 1, "price": 300}]),
             "new", "2026-04-14T12:00:00", "2026-04-14T12:00:00",
             json.dumps({"items": [{"description": "STALE", "qty": 99, "price": 999}]})),
        )
        conn.commit()
        conn.close()

        order = get_order("ord_stale")
        assert order is not None
        items = order.get("items") or []
        assert len(items) == 1
        assert items[0]["description"] == "Fresh"
        assert items[0]["qty"] == 1
        # The stale blob must not leak into the returned dict.
        assert "STALE" not in json.dumps(order)

    def test_dal_list_orders_ignores_stale_blob(self, temp_data_dir):
        from src.core.db import DB_PATH
        from src.core.dal import list_orders

        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            """INSERT INTO orders
               (id, quote_number, agency, po_number, total, items, status,
                created_at, updated_at, data_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("ord_list_stale", "R26Q777", "calvet", "PO777", 200.0,
             json.dumps([{"description": "Fresh List", "qty": 2, "price": 100}]),
             "new", "2026-04-14T12:00:00", "2026-04-14T12:00:00",
             json.dumps({"items": [{"description": "STALE-LIST", "qty": 99}]})),
        )
        conn.commit()
        conn.close()

        orders = list_orders()
        target = next((o for o in orders if o.get("id") == "ord_list_stale"), None)
        assert target is not None
        items = target.get("items") or []
        assert items[0]["description"] == "Fresh List"
        assert "STALE-LIST" not in json.dumps(target)
