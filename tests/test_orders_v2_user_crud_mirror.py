"""Tests for Orders V2 user-CRUD mirror (Batch F1).

POST /api/po/create and POST /api/po/<id>/update-status both keep writing
to the legacy purchase_orders table (unchanged behavior) AND now eagerly
mirror to orders + order_line_items so the V2 read paths don't go stale
between deploys.

Failures of the mirror MUST NOT break the legacy write — that's the
guarantee these tests pin down so we can flip read paths to V2 with
confidence.
"""
import sqlite3
from datetime import datetime

import pytest


def _conn(temp_data_dir):
    import os
    db = sqlite3.connect(os.path.join(temp_data_dir, "reytech.db"))
    db.row_factory = sqlite3.Row
    return db


def _ot_module():
    """Grab the exec'd routes_order_tracking module from sys.modules.

    Direct `import src.api.modules.routes_order_tracking` fails — the file
    relies on globals (`safe_page`, `bp`, …) injected by dashboard's loader.
    Once dashboard.py has loaded the route module, it's cached at this key.
    """
    import sys
    return sys.modules["src.api.modules.routes_order_tracking"]


@pytest.fixture(autouse=True)
def _init_po_schema(app, temp_data_dir):
    """The PO tracking schema is created on module load against whatever DB
    was active at that moment. Re-init against the patched test DB so the
    purchase_orders/po_line_items/po_status_history tables exist for these
    tests."""
    _ot_module()._init_po_tracking_db()
    yield


class TestCreateMirror:

    def test_create_writes_to_both_tables(self, auth_client, temp_data_dir):
        resp = auth_client.post("/api/po/create", json={
            "po_number": "PO-V2MIR-001",
            "vendor_name": "Reytech Inc.",
            "buyer_name": "Jane Buyer",
            "buyer_email": "jane@example.gov",
            "institution": "CCHCS",
            "quote_number": "Q-2026-0099",
        })
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

        db = _conn(temp_data_dir)
        legacy = db.execute(
            "SELECT po_number, status, buyer_name FROM purchase_orders WHERE po_number=?",
            ("PO-V2MIR-001",)
        ).fetchone()
        assert legacy is not None
        assert legacy["po_number"] == "PO-V2MIR-001"

        v2 = db.execute(
            "SELECT id, po_number, status, buyer_name, institution, quote_number "
            "FROM orders WHERE po_number=?",
            ("PO-V2MIR-001",)
        ).fetchone()
        assert v2 is not None, "V2 mirror should write into orders table"
        assert v2["id"] == "ORD-PO-PO-V2MIR-001"
        assert v2["status"] == "received"
        assert v2["buyer_name"] == "Jane Buyer"
        assert v2["institution"] == "CCHCS"
        assert v2["quote_number"] == "Q-2026-0099"
        db.close()

    def test_create_with_rfq_propagates_line_items(self, auth_client, temp_data_dir):
        # Seed an RFQ so create_po pulls line items from it
        from src.core.db import get_db, init_db
        init_db()
        # Save RFQ via the JSON store (load_rfqs reads from price_checks/rfqs.json)
        import os, json
        rfq_path = os.path.join(temp_data_dir, "rfqs.json")
        rfq = {
            "id": "rfq_lim_001",
            "requestor_name": "John Buyer",
            "requestor_email": "john@example.gov",
            "delivery_location": "CDCR",
            "reytech_quote_number": "Q-LIM-001",
            "line_items": [
                {"description": "Widget A", "item_number": "WA-1", "qty": 5,
                 "price_per_unit": 10.00, "uom": "EA"},
                {"description": "Widget B", "item_number": "WB-2", "qty": 2,
                 "price_per_unit": 25.50, "uom": "EA"},
            ],
        }
        with open(rfq_path, "w") as f:
            json.dump({rfq["id"]: rfq}, f)

        resp = auth_client.post("/api/po/create", json={
            "po_number": "PO-LIM-001", "rfq_id": "rfq_lim_001",
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["items"] == 2

        db = _conn(temp_data_dir)
        # V2 line items should be exploded into order_line_items
        v2_lines = db.execute(
            "SELECT line_number, description, qty_ordered, unit_price, extended_price "
            "FROM order_line_items WHERE order_id=? ORDER BY line_number",
            ("ORD-PO-PO-LIM-001",)
        ).fetchall()
        assert len(v2_lines) == 2
        assert v2_lines[0]["description"] == "Widget A"
        assert v2_lines[0]["qty_ordered"] == 5
        assert v2_lines[0]["unit_price"] == 10.00
        assert v2_lines[0]["extended_price"] == 50.00
        assert v2_lines[1]["description"] == "Widget B"
        assert v2_lines[1]["extended_price"] == 51.00
        # Total on the order itself should equal sum of extended prices
        v2_order = db.execute(
            "SELECT total FROM orders WHERE id=?", ("ORD-PO-PO-LIM-001",)
        ).fetchone()
        assert abs(v2_order["total"] - 101.00) < 0.01
        db.close()

    def test_mirror_failure_does_not_break_legacy_write(
        self, auth_client, temp_data_dir, monkeypatch
    ):
        """If V2 mirror raises, the legacy PO must still land — best-effort only."""
        _ot = _ot_module()

        def _boom(*a, **kw):
            raise RuntimeError("simulated V2 outage")

        monkeypatch.setattr(_ot, "_mirror_po_to_orders_v2", _boom)

        resp = auth_client.post("/api/po/create", json={"po_number": "PO-RESILIENT-1"})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

        db = _conn(temp_data_dir)
        legacy = db.execute(
            "SELECT po_number FROM purchase_orders WHERE po_number=?",
            ("PO-RESILIENT-1",)
        ).fetchone()
        assert legacy is not None, "legacy write must survive a V2 mirror failure"
        db.close()

    def test_idempotent_when_v2_row_already_exists(
        self, auth_client, temp_data_dir
    ):
        """Boot migration may have already created ORD-PO-<num>; mirror must not duplicate."""
        from src.core.db import get_db
        # Pre-seed an Orders V2 row exactly as the boot migration would
        with get_db() as conn:
            conn.execute(
                "INSERT INTO orders (id, po_number, status, agency, total, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                ("ORD-PO-PO-DUP-001", "PO-DUP-001", "new", "", 0,
                 datetime.now().isoformat(), datetime.now().isoformat())
            )

        resp = auth_client.post("/api/po/create", json={"po_number": "PO-DUP-001"})
        assert resp.status_code == 200

        db = _conn(temp_data_dir)
        rows = db.execute(
            "SELECT id FROM orders WHERE po_number=?", ("PO-DUP-001",)
        ).fetchall()
        assert len(rows) == 1, "must not create duplicate orders row"
        db.close()


class TestStatusMirror:

    def test_update_status_propagates_to_v2(self, auth_client, temp_data_dir):
        # Create the PO first (which writes to both tables)
        auth_client.post("/api/po/create", json={"po_number": "PO-STAT-001"})
        # Find the legacy po_id
        db = _conn(temp_data_dir)
        po_id = db.execute(
            "SELECT id FROM purchase_orders WHERE po_number=?", ("PO-STAT-001",)
        ).fetchone()["id"]
        db.close()

        # Update status via the user CRUD endpoint
        resp = auth_client.post(f"/api/po/{po_id}/update-status",
                                json={"status": "shipped"})
        assert resp.status_code == 200

        db = _conn(temp_data_dir)
        legacy_status = db.execute(
            "SELECT status FROM purchase_orders WHERE id=?", (po_id,)
        ).fetchone()["status"]
        v2_status = db.execute(
            "SELECT status FROM orders WHERE po_number=?", ("PO-STAT-001",)
        ).fetchone()["status"]
        db.close()

        assert legacy_status == "shipped"
        assert v2_status == "shipped", "V2 row must mirror the new status"

    def test_status_mirror_failure_does_not_break_legacy(
        self, auth_client, temp_data_dir, monkeypatch
    ):
        auth_client.post("/api/po/create", json={"po_number": "PO-STAT-RES"})
        db = _conn(temp_data_dir)
        po_id = db.execute(
            "SELECT id FROM purchase_orders WHERE po_number=?", ("PO-STAT-RES",)
        ).fetchone()["id"]
        db.close()

        _ot = _ot_module()
        monkeypatch.setattr(_ot, "_mirror_status_to_orders_v2",
                            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))

        resp = auth_client.post(f"/api/po/{po_id}/update-status",
                                json={"status": "delivered"})
        assert resp.status_code == 200

        db = _conn(temp_data_dir)
        legacy_status = db.execute(
            "SELECT status FROM purchase_orders WHERE id=?", (po_id,)
        ).fetchone()["status"]
        db.close()
        assert legacy_status == "delivered"


class TestParityEndpoint:

    def test_parity_endpoint_reports_zero_drift_after_mirrored_create(
        self, auth_client, temp_data_dir
    ):
        # Create a couple of POs through the new mirrored path
        auth_client.post("/api/po/create", json={"po_number": "PO-PAR-1"})
        auth_client.post("/api/po/create", json={"po_number": "PO-PAR-2"})

        resp = auth_client.get("/api/po/migration-parity")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["legacy_count"] >= 2
        assert body["v2_count"] >= 2
        assert body["unmirrored"] == 0, (
            "no legacy PO should be missing from V2 — mirror is supposed to be eager"
        )
        assert body["parity"] is True

    def test_parity_detects_unmirrored_legacy_row(self, auth_client, temp_data_dir):
        # Insert a raw legacy PO that bypasses the mirrored endpoint
        from src.core.db import get_db
        now = datetime.now().isoformat()
        with get_db() as conn:
            conn.execute(
                "INSERT INTO purchase_orders "
                "(id, po_number, status, total_amount, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?)",
                ("po_raw_1", "PO-RAW-9999", "received", 0, now, now)
            )
        resp = auth_client.get("/api/po/migration-parity")
        body = resp.get_json()
        assert body["unmirrored"] >= 1
        assert body["parity"] is False
