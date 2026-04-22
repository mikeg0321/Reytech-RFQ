"""PR E / Orders audit P1s — regression tests for O-12, O-13, O-15.

- O-12: POST /api/order/<oid>/line/<lid> must accept {field: "qty"} /
  {field: "unit_price"} and recompute `extended` and the order total.
  Before this fix the whitelist was ("sourcing_status", …, "unit_cost",
  "cost", "asin", "part_number") — qty/unit_price were silently dropped.
- O-13: Mutation endpoints (add-line, bulk, bulk-tracking, invoice,
  link-quote, delete) must write to order_audit_log so the /order/<oid>
  timeline shows the event. Before this fix only the /line/<lid> PATCH
  emitted timeline rows — everything else was invisible to audit.
- O-15: `cost` is no longer a write field on /line/<lid> — only
  `unit_cost`. Legacy templates still read `it.cost` via the
  `_row_to_line_item` alias, so the route mirrors unit_cost → cost in
  the blob on write, but does NOT accept `cost` as input.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime


def _seed_order_with_line(temp_data_dir, oid="ORD-PR-E-TEST", line_number=1,
                          qty=2, unit_price=50.0, unit_cost=20.0):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, "Q-PRE-1", "PO-PRE-1", "cchcs", "CCHCS",
         qty * unit_price, "new",
         "Test Buyer", "buyer@cchcs.ca.gov", now, now, ""),
    )
    conn.execute(
        """INSERT INTO order_line_items
           (order_id, line_number, description, qty_ordered, unit_price,
            unit_cost, extended_price, extended_cost,
            sourcing_status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, line_number, "Widget", qty, unit_price, unit_cost,
         qty * unit_price, qty * unit_cost, "pending", now, now),
    )
    conn.commit()
    conn.close()
    return oid, f"L{line_number:03d}"


def _read_line(temp_data_dir, oid, line_number=1):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT qty_ordered, unit_price, unit_cost, extended_price "
        "FROM order_line_items WHERE order_id=? AND line_number=?",
        (oid, line_number),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _read_order_total(temp_data_dir, oid):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT total FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    return row[0] if row else None


def _read_audit_actions(temp_data_dir, oid):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT action FROM order_audit_log WHERE order_id=? ORDER BY id",
        (oid,),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# O-12: qty + unit_price writable; extended + order total recomputed
# ═══════════════════════════════════════════════════════════════════════

def test_o12_qty_edit_persists_and_recomputes_extended(auth_client, temp_data_dir):
    oid, lid = _seed_order_with_line(temp_data_dir, qty=2, unit_price=50.0)
    resp = auth_client.post(
        f"/api/order/{oid}/line/{lid}",
        json={"field": "qty", "value": 5},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.get_json().get("ok") is True

    row = _read_line(temp_data_dir, oid)
    assert row["qty_ordered"] == 5, f"qty not persisted: {row!r}"
    assert row["extended_price"] == 250.0, (
        f"extended not recomputed: {row!r} — expected 5×50=$250")


def test_o12_unit_price_edit_persists_and_recomputes_extended(auth_client, temp_data_dir):
    oid, lid = _seed_order_with_line(temp_data_dir, qty=2, unit_price=50.0)
    resp = auth_client.post(
        f"/api/order/{oid}/line/{lid}",
        json={"field": "unit_price", "value": 75.50},
    )
    assert resp.status_code == 200
    assert resp.get_json().get("ok") is True

    row = _read_line(temp_data_dir, oid)
    assert row["unit_price"] == 75.50, f"unit_price not persisted: {row!r}"
    assert row["extended_price"] == 151.0, (
        f"extended not recomputed: {row!r} — expected 2×75.50=$151")


def test_o12_order_total_rolls_up_from_line_edit(auth_client, temp_data_dir):
    oid, lid = _seed_order_with_line(temp_data_dir, qty=2, unit_price=50.0)
    # Pre: 2 × $50 = $100
    assert _read_order_total(temp_data_dir, oid) == 100.0

    auth_client.post(
        f"/api/order/{oid}/line/{lid}",
        json={"field": "qty", "value": 10},
    )
    # Post: 10 × $50 = $500
    assert _read_order_total(temp_data_dir, oid) == 500.0


# ═══════════════════════════════════════════════════════════════════════
# O-15: unit_cost canonical; bare `cost` field is not a writer
# ═══════════════════════════════════════════════════════════════════════

def test_o15_update_line_whitelist_source_pins_unit_cost_only():
    """Grep-invariant: routes_orders_full.py api_order_update_line whitelist
    must include 'unit_cost' and must NOT treat bare 'cost' as writable.

    The bidirectional cost↔unit_cost sync block has been deleted — `cost`
    is a read-only legacy alias populated from unit_cost by
    `_row_to_line_item`."""
    path = "src/api/modules/routes_orders_full.py"
    src = open(path, encoding="utf-8").read()
    # Find the api_order_update_line body
    anchor = "def api_order_update_line"
    i = src.find(anchor)
    assert i >= 0, "api_order_update_line not found"
    body = src[i:i + 4000]
    assert '"unit_cost"' in body, "whitelist missing unit_cost"
    assert '"qty"' in body, "whitelist missing qty (O-12)"
    assert '"unit_price"' in body, "whitelist missing unit_price (O-12)"
    # O-15: no bidirectional sync — the old `elif field == "cost":` branch
    # must be gone.
    assert 'elif field == "cost"' not in body, (
        "O-15 regression: cost↔unit_cost bidirectional sync still present. "
        "Only unit_cost is canonical; cost is a read alias only.")


# ═══════════════════════════════════════════════════════════════════════
# O-13: mutation endpoints write to order_audit_log
# ═══════════════════════════════════════════════════════════════════════

def test_o13_add_line_emits_timeline_event(auth_client, temp_data_dir):
    oid, _lid = _seed_order_with_line(
        temp_data_dir, oid="ORD-O13-ADD", qty=1, unit_price=10.0)
    resp = auth_client.post(
        f"/api/order/{oid}/add-line",
        json={"description": "Added widget", "qty": 3, "unit_price": 12.0},
    )
    assert resp.status_code == 200
    actions = _read_audit_actions(temp_data_dir, oid)
    assert "line_added" in actions, (
        f"O-13 regression: /add-line did not emit timeline event — "
        f"got {actions!r}")


def test_o13_bulk_update_emits_timeline_event(auth_client, temp_data_dir):
    oid, _lid = _seed_order_with_line(temp_data_dir, oid="ORD-O13-BULK")
    resp = auth_client.post(
        f"/api/order/{oid}/bulk",
        json={"sourcing_status": "ordered"},
    )
    assert resp.status_code == 200
    actions = _read_audit_actions(temp_data_dir, oid)
    assert "bulk_line_update" in actions, (
        f"O-13 regression: /bulk did not emit timeline event — "
        f"got {actions!r}")


def test_o13_bulk_tracking_emits_timeline_event(auth_client, temp_data_dir):
    oid, _lid = _seed_order_with_line(temp_data_dir, oid="ORD-O13-TRK")
    resp = auth_client.post(
        f"/api/order/{oid}/bulk-tracking",
        json={"tracking": "1Z999AA10123456784", "carrier": "UPS"},
    )
    assert resp.status_code == 200
    actions = _read_audit_actions(temp_data_dir, oid)
    assert "bulk_tracking" in actions, (
        f"O-13 regression: /bulk-tracking did not emit timeline event — "
        f"got {actions!r}")


def test_o13_invoice_emits_timeline_event(auth_client, temp_data_dir):
    oid, _lid = _seed_order_with_line(temp_data_dir, oid="ORD-O13-INV")
    resp = auth_client.post(
        f"/api/order/{oid}/invoice",
        json={"type": "full", "invoice_number": "INV-123"},
    )
    assert resp.status_code == 200
    actions = _read_audit_actions(temp_data_dir, oid)
    assert "invoice_full" in actions, (
        f"O-13 regression: /invoice did not emit timeline event — "
        f"got {actions!r}")


def test_o13_delete_emits_timeline_event(auth_client, temp_data_dir):
    oid, _lid = _seed_order_with_line(temp_data_dir, oid="ORD-O13-DEL")
    resp = auth_client.post(
        f"/api/order/{oid}/delete",
        json={"reason": "duplicate"},
    )
    assert resp.status_code == 200
    actions = _read_audit_actions(temp_data_dir, oid)
    assert "order_deleted" in actions, (
        f"O-13 regression: /delete did not emit timeline event — "
        f"got {actions!r}")
