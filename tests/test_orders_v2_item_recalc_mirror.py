"""Orders V2 dual-write — line-item recalc must mirror to V2.

Before this guard: user updates a PO line item status via
`POST /api/po/<po_id>/update-item`. The handler recalculates the
rollup status on `purchase_orders` but NEVER mirrored to the V2
`orders` table. Margin/analytics views stayed stale until the next
app restart ran the boot merge migration. For a solo operator doing
2-4 quotes per session, that gap was felt — edits didn't show up in
the V2 margin dashboard mid-session.

Contract:

  1. `_recalculate_po_status` returns the computed `new_status` so
     callers can forward it to `_mirror_status_to_orders_v2` without
     a second SELECT round-trip.

  2. `POST /api/po/<po_id>/update-item` calls the mirror after the
     legacy write completes. Mirror failures MUST NOT fail the
     user-facing request — the legacy row is already written.

  3. The mirror keeps Orders V2 `orders.status` in sync with
     `purchase_orders.status` after a line-item rollup, so margin
     views don't lag user edits.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime

import pytest


def _ot_module():
    """Grab the exec'd routes_order_tracking module from sys.modules.
    Direct import fails — the module relies on globals (safe_page, bp, ...)
    injected by dashboard's loader. Once dashboard has loaded it, the module
    is cached under this key."""
    return sys.modules["src.api.modules.routes_order_tracking"]


@pytest.fixture(autouse=True)
def _init_po_schema(app, temp_data_dir):
    """The PO tracking schema is created on module load against whatever DB
    was active at that moment. Re-init against the patched test DB so the
    purchase_orders/po_line_items tables exist for these tests."""
    _ot_module()._init_po_tracking_db()
    yield


def _seed_po_with_line_item(temp_data_dir, po_number="PO-TEST-RECALC-1"):
    """Seed a PO in both legacy (purchase_orders + po_line_items) AND
    V2 (orders + order_line_items) tables so the mirror has somewhere
    to write. Mirrors what `create_po` + `_mirror_po_to_orders_v2` do
    together, without needing to hit the Flask route (which would
    require auth, a linked RFQ, etc.)."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    po_id = f"po_{uuid.uuid4().hex[:8]}"
    now = datetime.now().isoformat()
    oid = f"ORD-PO-{po_number}"

    conn.execute(
        """INSERT INTO purchase_orders
           (id, po_number, vendor_name, buyer_name, buyer_email, institution,
            order_date, total_amount, status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (po_id, po_number, "Acme", "Buyer", "b@x.com", "CDCR",
         now, 100.0, "received", now, now))
    line_item_id_row = conn.execute(
        """INSERT INTO po_line_items
           (po_id, line_number, description, qty_ordered, unit_price,
            extended_price, status, updated_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (po_id, 1, "Test item", 1, 100.0, 100.0, "pending", now))
    line_item_id = line_item_id_row.lastrowid

    # Mirror into V2 (what `_mirror_po_to_orders_v2` does after create_po)
    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, "", po_number, "", "CDCR", 100.0, "received",
         "Buyer", "b@x.com", now, now, ""))
    conn.execute(
        """INSERT INTO order_line_items
           (order_id, line_number, description, qty_ordered, unit_price,
            extended_price, sourcing_status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (oid, 1, "Test item", 1, 100.0, 100.0, "pending", now, now))
    conn.commit()
    conn.close()
    return po_id, line_item_id, oid


def test_recalculate_returns_new_status(temp_data_dir):
    """Contract: `_recalculate_po_status` returns the status it wrote
    so callers can mirror without re-querying."""
    po_id, line_item_id, _oid = _seed_po_with_line_item(
        temp_data_dir, po_number="PO-RECALC-RET-1")

    # Flip the single line to 'delivered' so the rollup computes 'delivered'
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    conn.execute(
        "UPDATE po_line_items SET status='delivered', updated_at=? WHERE id=?",
        (now, line_item_id))
    conn.commit()

    from src.api.modules.routes_order_tracking import _recalculate_po_status
    result = _recalculate_po_status(conn, po_id)
    conn.commit()
    conn.close()

    assert result == "delivered", (
        f"_recalculate_po_status must return the computed status so the "
        f"caller can mirror to V2, got {result!r}"
    )


def test_recalculate_returns_none_for_unknown_po(temp_data_dir):
    """If the PO has no line items, recalc must return None — the
    caller should skip the mirror rather than write a stale status."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    from src.api.modules.routes_order_tracking import _recalculate_po_status
    result = _recalculate_po_status(conn, "po_nonexistent")
    conn.close()
    assert result is None, (
        f"recalc on empty po must return None, got {result!r}"
    )


def test_update_item_mirrors_recalc_to_orders_v2(auth_client, temp_data_dir):
    """End-to-end: user hits POST /api/po/<po_id>/update-item and flips
    the only line item to 'delivered'. After the request returns, the
    V2 `orders.status` must also be 'delivered' — no restart required."""
    po_id, line_item_id, oid = _seed_po_with_line_item(
        temp_data_dir, po_number="PO-RECALC-E2E-1")

    resp = auth_client.post(
        f"/api/po/{po_id}/update-item",
        json={"item_id": line_item_id, "status": "delivered"})
    assert resp.status_code == 200, resp.get_data(as_text=True)

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    legacy = conn.execute(
        "SELECT status FROM purchase_orders WHERE id = ?", (po_id,)
    ).fetchone()
    v2 = conn.execute(
        "SELECT status FROM orders WHERE id = ?", (oid,)
    ).fetchone()
    conn.close()

    assert legacy["status"] == "delivered", (
        f"Legacy purchase_orders.status should recalculate to 'delivered', "
        f"got {legacy['status']!r}"
    )
    assert v2 is not None, (
        "V2 orders row must exist (seeded up front); if this fails, the "
        "seed helper or the mirror pattern regressed"
    )
    assert v2["status"] == "delivered", (
        f"V2 orders.status must mirror the recalc to 'delivered' "
        f"(without waiting for next boot migration), got {v2['status']!r}. "
        f"This is the whole point of dual-write — margin/analytics views "
        f"lag user edits when this mirror is missing."
    )


def test_update_item_mirror_failure_does_not_fail_request(
        auth_client, temp_data_dir, monkeypatch):
    """If the V2 mirror raises (e.g. schema drift, lock contention),
    the user's legacy PO update MUST still succeed — the mirror is
    best-effort. Simulate by monkeypatching the mirror to always raise."""
    po_id, line_item_id, _oid = _seed_po_with_line_item(
        temp_data_dir, po_number="PO-RECALC-FAIL-1")

    rot = _ot_module()
    def _boom(*a, **kw):
        raise RuntimeError("simulated mirror crash")
    monkeypatch.setattr(rot, "_mirror_status_to_orders_v2", _boom)

    resp = auth_client.post(
        f"/api/po/{po_id}/update-item",
        json={"item_id": line_item_id, "status": "delivered"})
    assert resp.status_code == 200, (
        f"Mirror failure must NOT fail the legacy write "
        f"(got {resp.status_code}: {resp.get_data(as_text=True)})"
    )

    # Legacy write still happened
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    legacy = conn.execute(
        "SELECT status FROM purchase_orders WHERE id = ?", (po_id,)
    ).fetchone()
    conn.close()
    assert legacy[0] == "delivered"
