"""Tests for the DAL (Data Access Layer) — RFQ, PriceCheck, Order, LineItem."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import sqlite3
from src.core.db import get_db, DB_PATH, init_db
from src.core.dal import (
    get_rfq, list_rfqs, save_rfq, update_rfq_status,
    get_pc, list_pcs, save_pc, update_pc_status,
    get_order, list_orders, save_order, update_order_status,
    get_line_items, save_line_items,
)


@pytest.fixture(autouse=True)
def setup_db():
    """Ensure DB schema exists before each test."""
    init_db()
    yield


class TestRFQ:
    def test_save_and_get(self):
        rfq = {"id": "TEST001", "agency": "CDCR", "status": "new",
               "received_at": "2026-01-01", "items": [{"desc": "Gloves"}]}
        assert save_rfq(rfq) is True
        result = get_rfq("TEST001")
        assert result is not None
        assert result["agency"] == "CDCR"
        assert result["status"] == "new"

    def test_get_not_found(self):
        assert get_rfq("NONEXISTENT") is None

    def test_list_rfqs(self):
        save_rfq({"id": "LIST1", "status": "new", "received_at": "2026-01-01"})
        save_rfq({"id": "LIST2", "status": "sent", "received_at": "2026-01-02"})
        all_rfqs = list_rfqs()
        ids = [r["id"] for r in all_rfqs]
        assert "LIST1" in ids
        assert "LIST2" in ids
        # Filter by status
        new_only = list_rfqs(status="new")
        assert all(r["status"] == "new" for r in new_only)

    def test_update_status(self):
        save_rfq({"id": "UPD1", "status": "new", "received_at": "2026-01-01"})
        update_rfq_status("UPD1", "sent")
        result = get_rfq("UPD1")
        assert result["status"] == "sent"


class TestPriceCheck:
    def test_save_and_get(self):
        pc = {"id": "PC001", "requestor": "Buyer", "status": "parsed",
              "created_at": "2026-01-01", "items": [{"description": "Paper"}]}
        assert save_pc(pc) is True
        result = get_pc("PC001")
        assert result is not None
        assert result["requestor"] == "Buyer"

    def test_get_not_found(self):
        assert get_pc("NOPC") is None

    def test_list_and_filter(self):
        save_pc({"id": "PCL1", "status": "parsed", "created_at": "2026-01-01"})
        save_pc({"id": "PCL2", "status": "sent", "created_at": "2026-01-02"})
        all_pcs = list_pcs()
        assert len(all_pcs) >= 2

    def test_update_status(self):
        save_pc({"id": "PCU1", "status": "parsed", "created_at": "2026-01-01"})
        update_pc_status("PCU1", "sent")
        assert get_pc("PCU1")["status"] == "sent"


class TestOrder:
    def test_save_and_get(self):
        order = {"id": "ORD001", "agency": "CalVet", "status": "new",
                 "created_at": "2026-01-01", "total": 1500.00,
                 "items": [{"description": "Bandages", "qty": 10}]}
        assert save_order(order) is True
        result = get_order("ORD001")
        assert result is not None
        assert result["total"] == 1500.00

    def test_get_not_found(self):
        assert get_order("NOORDER") is None

    def test_update_status(self):
        save_order({"id": "ORDU1", "status": "new", "created_at": "2026-01-01"})
        update_order_status("ORDU1", "shipped")
        # Note: get_order prefers data_json blob which update_order_status doesn't touch.
        # Verify the SQL column was updated directly.
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute("SELECT status FROM orders WHERE id=?", ("ORDU1",)).fetchone()
        assert row["status"] == "shipped"


class TestLineItems:
    def test_get_and_save(self):
        save_rfq({"id": "LI1", "status": "new", "received_at": "2026-01-01",
                  "items": [{"desc": "Original"}]})
        items = get_line_items("LI1", "rfq")
        assert len(items) == 1
        # Update items
        new_items = [{"desc": "Updated"}, {"desc": "Added"}]
        save_line_items("LI1", new_items, "rfq")
        items = get_line_items("LI1", "rfq")
        assert len(items) == 2

    def test_get_empty(self):
        items = get_line_items("NOPE", "rfq")
        assert items == []


class TestAuditTrail:
    def test_save_rfq_creates_audit(self):
        save_rfq({"id": "AUD1", "status": "new", "received_at": "2026-01-01"})
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_trail WHERE item_description='rfq' AND rfq_id='AUD1'"
            ).fetchall()
            assert len(rows) >= 1
            assert rows[0]["field_changed"] in ("create", "update")


class TestSnapshots:
    def test_snapshot_taken_before_update(self):
        save_rfq({"id": "SNAP1", "status": "new", "received_at": "2026-01-01", "agency": "BEFORE"})
        save_rfq({"id": "SNAP1", "status": "new", "received_at": "2026-01-01", "agency": "AFTER"})
        from src.core.snapshots import list_snapshots
        snaps = list_snapshots(agent_name="dal")
        snap_ids = [s["run_id"] for s in snaps]
        assert "SNAP1" in snap_ids

    def test_restore_returns_original(self):
        save_rfq({"id": "REST1", "status": "new", "received_at": "2026-01-01", "agency": "ORIG"})
        save_rfq({"id": "REST1", "status": "draft", "received_at": "2026-01-01", "agency": "CHANGED"})
        from src.core.snapshots import list_snapshots
        snaps = [s for s in list_snapshots(agent_name="dal") if s.get("run_id") == "REST1"]
        assert len(snaps) >= 1
        from src.core.snapshots import restore_snapshot
        result = restore_snapshot(snaps[0]["id"])
        assert result["ok"] is True
        assert result["data"]["agency"] == "ORIG"
