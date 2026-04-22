"""O-1 audit regression sweep: every line-edit route on Orders must persist
to the authoritative V2 `order_line_items` table, not just the in-memory dict.

Tests one endpoint at a time with a round-trip assertion: POST, then read
back from the SQLite normalized table to confirm the write reached disk.

Covers the 8 endpoints identified in project_orders_module_audit_2026_04_21.md
O-1 (import-po is covered by existing test_order_lifecycle.py):
  - /api/order/<oid>/add-line       (414)
  - /api/order/<oid>/line/<lid>     (583)  update-line
  - /api/order/<oid>/line/<lid>/cost (3305)
  - /api/order/<oid>/bulk           (675)  bulk-update
  - /api/order/<oid>/bulk-tracking  (698)
  - /api/order/<oid>/invoice        (727) — full + partial paths
  - /api/order/<oid>/clone          (3444)

The round-trip reads SQLite directly (bypassing the DAL) so no false greens
from hydration. Outcome: 7/8 endpoints already persist correctly via
_save_single_order → save_line_items_batch; only the cost-key asymmetry in
api_order_line_cost (PR #325) and the status_history KeyError in
api_order_invoice (O-7) were real data-loss bugs.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime


def _seed_order(temp_data_dir, oid="ORD-PO-O1-SWEEP", n_lines=2,
                unit_price=100.0, old_unit_cost=40.0):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()

    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, "Q-O1-SWEEP", oid.replace("ORD-PO-", ""), "cchcs", "CCHCS",
         unit_price * n_lines, "new", "Test Buyer",
         "buyer@cchcs.ca.gov", now, now, ""),
    )
    line_ids = []
    for i in range(1, n_lines + 1):
        cur = conn.execute(
            """INSERT INTO order_line_items
               (order_id, line_number, description, qty_ordered, unit_price,
                unit_cost, extended_price, extended_cost,
                sourcing_status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (oid, i, f"Widget {i}", 1, unit_price, old_unit_cost,
             unit_price, old_unit_cost, "pending", now, now),
        )
        line_ids.append((f"L{i:03d}", cur.lastrowid))
    conn.commit()
    conn.close()
    return oid, line_ids


def _read_line(temp_data_dir, oid, line_number=1):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM order_line_items WHERE order_id=? AND line_number=?",
        (oid, line_number),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _count_lines(temp_data_dir, oid):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    n = conn.execute(
        "SELECT COUNT(*) FROM order_line_items WHERE order_id=?", (oid,)
    ).fetchone()[0]
    conn.close()
    return n


def _read_order(temp_data_dir, oid):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ── /add-line ──────────────────────────────────────────────────────────

def test_add_line_persists_new_row(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-ADDLINE", n_lines=1)
    before = _count_lines(temp_data_dir, oid)

    resp = auth_client.post(
        f"/api/order/{oid}/add-line",
        json={"description": "New widget", "qty": 3, "unit_price": 50.0,
              "part_number": "WG-42", "supplier": "Acme"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True, body

    after = _count_lines(temp_data_dir, oid)
    assert after == before + 1, f"O-1: add-line did not persist (before={before}, after={after})"

    new_row = _read_line(temp_data_dir, oid, line_number=2)
    assert new_row is not None, "O-1: new line row missing from order_line_items"
    assert new_row["description"] == "New widget"
    assert new_row["qty_ordered"] == 3
    assert new_row["unit_price"] == 50.0


# ── /line/<lid> (update-line) ──────────────────────────────────────────

def test_update_line_cost_persists_to_column(auth_client, temp_data_dir):
    """update-line with field=unit_cost must reach order_line_items.unit_cost."""
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-UPDLINE-COST")

    resp = auth_client.post(
        f"/api/order/{oid}/line/L001",
        json={"field": "unit_cost", "value": 67.89},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    row = _read_line(temp_data_dir, oid)
    assert row["unit_cost"] == 67.89, (
        f"O-1: update-line unit_cost did not persist — got {row['unit_cost']!r}"
    )


def test_update_line_sourcing_status_persists(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-UPDLINE-STATUS")

    resp = auth_client.post(
        f"/api/order/{oid}/line/L001",
        json={"field": "sourcing_status", "value": "ordered"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    row = _read_line(temp_data_dir, oid)
    assert row["sourcing_status"] == "ordered", (
        f"O-1: update-line sourcing_status did not persist — got {row['sourcing_status']!r}"
    )


def test_update_line_tracking_persists(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-UPDLINE-TRK")

    resp = auth_client.post(
        f"/api/order/{oid}/line/L001",
        json={"field": "tracking_number", "value": "1Z999AA10123456784"},
    )
    assert resp.status_code == 200

    row = _read_line(temp_data_dir, oid)
    assert row["tracking_number"] == "1Z999AA10123456784"


# ── /line/<lid>/cost ───────────────────────────────────────────────────

def test_line_cost_endpoint_persists_unit_cost(auth_client, temp_data_dir):
    """PR #325 regression — belt and suspenders alongside test_orders_v2_line_cost_persists.py."""
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-LINECOST",
                         unit_price=200.0, old_unit_cost=80.0)

    resp = auth_client.post(
        f"/api/order/{oid}/line/L001/cost",
        json={"cost": 123.45},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True

    row = _read_line(temp_data_dir, oid)
    assert row["unit_cost"] == 123.45, (
        f"O-1/O-11: /line/<lid>/cost did not persist — got {row['unit_cost']!r}"
    )


# ── /bulk (bulk-update) ────────────────────────────────────────────────

def test_bulk_update_sourcing_status_persists_all_lines(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-BULK-SS", n_lines=3)

    resp = auth_client.post(
        f"/api/order/{oid}/bulk",
        json={"sourcing_status": "ordered"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    for i in (1, 2, 3):
        row = _read_line(temp_data_dir, oid, line_number=i)
        assert row["sourcing_status"] == "ordered", (
            f"O-1: bulk-update did not persist line {i} — got {row['sourcing_status']!r}"
        )


# ── /bulk-tracking ─────────────────────────────────────────────────────

def test_bulk_tracking_persists_tracking_and_status(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-BULK-TRK", n_lines=2)

    resp = auth_client.post(
        f"/api/order/{oid}/bulk-tracking",
        json={"tracking": "TRACK-O1-TEST", "carrier": "UPS"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True
    assert body.get("updated") == 2

    for i in (1, 2):
        row = _read_line(temp_data_dir, oid, line_number=i)
        assert row["tracking_number"] == "TRACK-O1-TEST", (
            f"O-1: bulk-tracking tracking_number not persisted on line {i}"
        )
        assert row["carrier"] == "UPS"
        assert row["sourcing_status"] == "shipped"
        assert row["ship_date"], "ship_date should be stamped"


# ── /invoice ───────────────────────────────────────────────────────────

def test_invoice_full_persists_invoice_fields_on_all_lines(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-INV-FULL", n_lines=2)

    resp = auth_client.post(
        f"/api/order/{oid}/invoice",
        json={"type": "full", "invoice_number": "INV-9001"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True, body

    for i in (1, 2):
        row = _read_line(temp_data_dir, oid, line_number=i)
        assert row["invoice_status"] == "invoiced", (
            f"O-1/O-7: invoice full did not persist invoice_status on line {i}"
        )
        assert row["invoice_number"] == "INV-9001"


def test_invoice_partial_persists_delivered_lines_only(auth_client, temp_data_dir):
    """O-7 + partial-invoice branch: only delivered lines should flip to invoiced."""
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-INV-PARTIAL", n_lines=3)
    # Flip line 1 to delivered, leave line 2 as shipped, line 3 as pending
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE order_line_items SET sourcing_status=? WHERE order_id=? AND line_number=1",
                 ("delivered", oid))
    conn.execute("UPDATE order_line_items SET sourcing_status=? WHERE order_id=? AND line_number=2",
                 ("shipped", oid))
    conn.commit()
    conn.close()

    resp = auth_client.post(
        f"/api/order/{oid}/invoice",
        json={"type": "partial", "invoice_number": "INV-PART-1"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True, body

    row_delivered = _read_line(temp_data_dir, oid, line_number=1)
    row_shipped = _read_line(temp_data_dir, oid, line_number=2)
    row_pending = _read_line(temp_data_dir, oid, line_number=3)

    assert row_delivered["invoice_status"] == "invoiced", (
        "O-1/O-7 partial: delivered line did not flip to invoiced"
    )
    assert row_delivered["invoice_number"] == "INV-PART-1"
    assert row_shipped["invoice_status"] == "partial", (
        "partial: shipped line should be marked 'partial', not invoiced"
    )
    assert row_pending["invoice_status"] == "pending", (
        "partial: pending line should not be touched"
    )


# ── /clone ─────────────────────────────────────────────────────────────

def test_clone_creates_new_order_with_lines(auth_client, temp_data_dir):
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-CLONESRC", n_lines=2,
                         unit_price=75.0, old_unit_cost=30.0)

    resp = auth_client.post(
        f"/api/order/{oid}/clone",
        json={"po_number": "CLONE-9001"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True, body

    new_oid = "ORD-PO-CLONE-9001"
    new_order = _read_order(temp_data_dir, new_oid)
    assert new_order is not None, (
        f"O-1: clone did not persist new order {new_oid} to orders table"
    )

    n_lines = _count_lines(temp_data_dir, new_oid)
    assert n_lines == 2, (
        f"O-1: clone persisted {n_lines} lines, expected 2"
    )

    # Verify line contents copied correctly — not just row count
    new_line = _read_line(temp_data_dir, new_oid, line_number=1)
    assert new_line["description"] == "Widget 1", (
        f"O-1 clone: description not copied — got {new_line['description']!r}"
    )
    assert new_line["unit_price"] == 75.0, (
        f"O-1 clone: unit_price not copied — got {new_line['unit_price']!r}"
    )
    # Statuses must be reset per clone semantics
    assert new_line["sourcing_status"] == "pending"
    assert new_line["tracking_number"] == ""
    assert new_line["invoice_status"] == "pending"


# ── RE-AUDIT-3: supplier-lookup alias drift (2026-04-22) ────────────────

def test_supplier_lookup_mirrors_supplier_name_on_auto_populate(
    auth_client, temp_data_dir, monkeypatch
):
    """Supplier auto-lookup must write BOTH `supplier` and `supplier_name`.

    Bug: api_order_lookup_suppliers wrote only it["supplier"]="Amazon". When
    get_order hydrated the line, supplier_name from DB was already set → the
    next save_line_items_batch round-trip read supplier_name first, silently
    discarding the lookup. The row in order_line_items kept its old supplier_name.

    Fix: mirror supplier_name alongside supplier on auto-populate.
    """
    # Seed with unit_cost=0 so the auto-populate branch runs (route skips
    # when a cost already exists), then pre-set an old supplier_name so the
    # bug would manifest as supplier_name staying at "OldSupplier" post-lookup.
    oid, _ = _seed_order(temp_data_dir, oid="ORD-PO-SUPP-LOOKUP",
                         old_unit_cost=0.0)
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE order_line_items SET supplier_name=?, part_number=? WHERE order_id=?",
        ("OldSupplier", "B0TESTASIN", oid),
    )
    conn.commit()
    conn.close()

    # Stub research_product so the test is hermetic — no Amazon/SerpApi call.
    import src.agents.product_research as pr
    monkeypatch.setattr(
        pr, "research_product",
        lambda item_number=None, description=None: {
            "found": True, "price": 19.99, "title": "Stub Amazon",
            "url": "https://amazon.com/dp/B0TESTASIN", "asin": "B0TESTASIN",
        },
    )

    resp = auth_client.post(f"/api/order/{oid}/lookup-suppliers", json={})
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("ok") is True, body

    row = _read_line(temp_data_dir, oid, line_number=1)
    assert row["supplier_name"] == "Amazon", (
        f"RE-AUDIT-3: supplier_name not mirrored — got {row['supplier_name']!r}"
    )
    assert row["unit_cost"] == 19.99
    assert row["supplier_url"].endswith("B0TESTASIN")
