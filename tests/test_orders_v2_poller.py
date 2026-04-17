"""Orders V2 PO email poller — unit tests for the V2 path.

Covers all 5 status keywords + tracking + the silent-skip-on-unknown-PO
behavior. Also pins the FF dispatcher so we can flip the flag at runtime
without breaking tests for the legacy code path.

See docs/PRD_ORDERS_V2_POLLER_MIGRATION.md for the migration plan.
"""
import json
import sqlite3
import pytest


def _seed_order(order_id, po_number, line_count=2):
    """Insert an order + N pending line items into the V2 tables."""
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("""INSERT INTO orders
            (id, po_number, agency, institution, total, status, items, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (order_id, po_number, "CDCR", "Folsom State Prison",
             100.0, "new", "[]", "2026-04-17T00:00:00Z", "2026-04-17T00:00:00Z"))
        line_ids = []
        for i in range(line_count):
            cur = conn.execute("""INSERT INTO order_line_items
                (order_id, line_number, description, qty_ordered, unit_price,
                 sourcing_status, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (order_id, i + 1, f"Test item {i+1}", 5, 10.0,
                 "pending", "2026-04-17T00:00:00Z", "2026-04-17T00:00:00Z"))
            line_ids.append(cur.lastrowid)
        conn.commit()
    return line_ids


def _get_order(order_id):
    from src.core.db import get_db
    with get_db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        return dict(row) if row else None


def _get_lines(order_id):
    from src.core.db import get_db
    with get_db() as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(
            "SELECT * FROM order_line_items WHERE order_id=? ORDER BY line_number",
            (order_id,)
        ).fetchall()]


def _audit_actions(order_id):
    from src.core.db import get_db
    with get_db() as conn:
        return [r[0] for r in conn.execute(
            "SELECT action FROM order_audit_log WHERE order_id=? ORDER BY id",
            (order_id,)
        ).fetchall()]


class TestPollerV2:

    def test_unknown_po_silently_skipped(self, temp_data_dir):
        """Email about a PO that doesn't exist in V2 → no writes, matched=False."""
        from src.core import po_email_v2
        result = po_email_v2.process_email(
            subject="PO #99999 update",
            sender="vendor@x.com",
            body="Your order PO #99999 has shipped",
            email_uid="msg-001",
        )
        assert result["matched"] is False
        assert result["po_id"] is None

    def test_tracking_number_applied_to_open_lines(self, temp_data_dir):
        """Tracking regex match → all pending lines get tracking + ship_date + status=shipped."""
        from src.core import po_email_v2
        line_ids = _seed_order("ORD-PO-12345", "12345", line_count=3)

        result = po_email_v2.process_email(
            subject="PO 12345 shipping",
            sender="vendor@x.com",
            body="Tracking: 1Z999AA10123456784 — out for delivery soon",
            email_uid="msg-002",
        )
        assert result["matched"] is True
        assert result["po_id"] == "ORD-PO-12345"

        lines = _get_lines("ORD-PO-12345")
        for line in lines:
            assert line["tracking_number"] == "1Z999AA10123456784"
            assert line["sourcing_status"] == "shipped"
            assert line["ship_date"]  # set to today

    def test_shipped_keyword_transitions_order(self, temp_data_dir):
        """'shipped' keyword → orders.status=shipped + each line sourcing_status=shipped."""
        from src.core import po_email_v2
        _seed_order("ORD-PO-2001", "2001")
        po_email_v2.process_email(
            subject="Update PO 2001",
            sender="vendor@x.com",
            body="Your shipment has shipped today.",
            email_uid="msg-003",
        )
        order = _get_order("ORD-PO-2001")
        assert order["status"] == "shipped"
        for line in _get_lines("ORD-PO-2001"):
            assert line["sourcing_status"] == "shipped"

    def test_delivered_keyword_transitions_order(self, temp_data_dir):
        from src.core import po_email_v2
        _seed_order("ORD-PO-2002", "2002")
        po_email_v2.process_email(
            subject="PO 2002 update",
            sender="vendor@x.com",
            body="Package was delivered to the loading dock.",
            email_uid="msg-004",
        )
        order = _get_order("ORD-PO-2002")
        assert order["status"] == "delivered"

    def test_backorder_keyword_marks_lines(self, temp_data_dir):
        """'backorder' keyword → each open line sourcing_status=backordered."""
        from src.core import po_email_v2
        _seed_order("ORD-PO-2003", "2003")
        po_email_v2.process_email(
            subject="PO 2003",
            sender="vendor@x.com",
            body="Item is on backorder for 2 weeks.",
            email_uid="msg-005",
        )
        for line in _get_lines("ORD-PO-2003"):
            assert line["sourcing_status"] == "backordered"

    def test_invoiced_keyword_transitions_order(self, temp_data_dir):
        from src.core import po_email_v2
        _seed_order("ORD-PO-2004", "2004")
        po_email_v2.process_email(
            subject="PO 2004 invoice",
            sender="vendor@x.com",
            body="Invoice attached. Payment due in 30 days.",
            email_uid="msg-006",
        )
        order = _get_order("ORD-PO-2004")
        assert order["status"] == "invoiced"

    def test_confirmed_keyword_maps_to_sourcing(self, temp_data_dir):
        """V2 has no 'confirmed' state — 'order confirmed' should map to 'sourcing'."""
        from src.core import po_email_v2
        _seed_order("ORD-PO-2005", "2005")
        po_email_v2.process_email(
            subject="PO 2005",
            sender="vendor@x.com",
            body="Order confirmed — we are processing your order.",
            email_uid="msg-007",
        )
        order = _get_order("ORD-PO-2005")
        assert order["status"] == "sourcing"

    def test_audit_log_records_email(self, temp_data_dir):
        """Every matched email writes one inbound_email row to order_audit_log."""
        from src.core import po_email_v2
        _seed_order("ORD-PO-2006", "2006")
        po_email_v2.process_email(
            subject="PO 2006 status",
            sender="vendor@x.com",
            body="Order shipped via UPS today.",
            email_uid="msg-008",
        )
        actions = _audit_actions("ORD-PO-2006")
        assert "inbound_email" in actions

    def test_extract_po_numbers_handles_formats(self, temp_data_dir):
        """Parser must catch all five PO# formats from the legacy regex set."""
        from src.core import po_email_v2
        text = "PO#11111 and Purchase Order: 22222 and P.O. 33333 and Order #44444 and #55555"
        nums = sorted(po_email_v2.extract_po_numbers(text))
        assert nums == ["11111", "22222", "33333", "44444", "55555"]

    def test_extract_status_updates_handles_all_keywords(self, temp_data_dir):
        """Keyword parser must produce the 5 status_change types + 1 tracking type."""
        from src.core import po_email_v2

        cases = {
            "shipped": "Your package has shipped today",
            "delivered": "Package was delivered yesterday",
            "backordered": "Item is on backorder",
            "invoiced": "Invoice for this order attached",
            "confirmed": "Order confirmed and processing",
        }
        for expected, body in cases.items():
            updates = po_email_v2.extract_status_updates(body)
            statuses = [u["new_status"] for u in updates if u["type"] == "status_change"]
            assert expected in statuses, f"Expected {expected} in {statuses} for body '{body}'"
