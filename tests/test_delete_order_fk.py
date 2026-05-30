"""Regression (ISSUE-11 sweep, 2026-05-30): delete_order must actually persist.

The fk_order_audit_log_order trigger aborts an order_audit_log insert whose
order_id is no longer in `orders`. delete_order used to delete the order and
THEN audit-log it → the audit insert aborted → the whole transaction rolled
back → the order survived while delete_order returned False (swallowed by the
route, which reported ok:true). Auditing before the delete fixes it.
"""

from src.core.order_dal import save_order, get_order, delete_order


def test_delete_order_actually_persists(temp_data_dir):
    oid = "ORD-DELTEST-1"
    save_order(oid, {
        "quote_number": "R26Q999", "po_number": "TEST-DEL-1", "total": 100.0,
        "agency": "CDCR", "institution": "CSP-Sacramento",
        "line_items": [{"description": "widget", "qty": 1, "unit_price": 100.0}],
    }, actor="test")
    assert get_order(oid) is not None, "order should exist after save"

    assert delete_order(oid, actor="test", reason="regression") is True
    # The bug: order survived here (FK-trigger rollback). Must be gone now.
    assert get_order(oid) is None, "order must be gone after delete_order"


def test_delete_missing_order_is_safe(temp_data_dir):
    # deleting a non-existent order shouldn't raise (audit row may persist)
    assert delete_order("ORD-DOES-NOT-EXIST", actor="test") in (True, False)
