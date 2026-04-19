"""Carrier auto-status tests (Batch F2).

Attaching a tracking number to an order line — whether on the V2
order_line_items path (`order_dal.update_line_status`) or the legacy
`/api/po/<id>/update-item` route — should auto-promote the line to
``shipped`` and stamp ``carrier`` + ``ship_date`` without requiring
the operator to also click a status button.

Promotion is one-way: a tracking number must NEVER downgrade a line
that was already manually marked ``delivered``.
"""
import sqlite3
import uuid
from datetime import datetime

import pytest


def _conn(temp_data_dir):
    import os
    db = sqlite3.connect(os.path.join(temp_data_dir, "reytech.db"))
    db.row_factory = sqlite3.Row
    return db


def _ot_module():
    import sys
    return sys.modules["src.api.modules.routes_order_tracking"]


@pytest.fixture(autouse=True)
def _init_po_schema(app, temp_data_dir):
    _ot_module()._init_po_tracking_db()
    yield


def _seed_v2_line(temp_data_dir, order_id, line_number=1, status="pending",
                  tracking="", carrier="", ship_date=""):
    """Insert an orders + order_line_items row pair the V2 DAL can update."""
    from src.core.db import get_db
    now = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO orders (id, po_number, status, total, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?)",
            (order_id, "PO-AUTO-" + order_id[-4:], "new", 0, now, now)
        )
        cur = conn.execute(
            "INSERT INTO order_line_items "
            "(order_id, line_number, description, qty_ordered, unit_price, "
            " sourcing_status, carrier, tracking_number, ship_date, "
            " created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (order_id, line_number, "Widget", 1, 10.00,
             status, carrier, tracking, ship_date, now, now)
        )
        return cur.lastrowid


class TestAutoPromoteHelper:
    """Pure-function semantics — no DB."""

    def test_promotes_pending_to_shipped(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        new_status, carrier = auto_promote_status_for_tracking(
            "pending", "1Z999AA10123456784"
        )
        assert new_status == "shipped"
        assert carrier == "UPS"

    def test_promotes_ordered_to_shipped(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        new_status, _ = auto_promote_status_for_tracking("ordered", "1Z999AA10123456784")
        assert new_status == "shipped"

    def test_promotes_empty_status_to_shipped(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        new_status, _ = auto_promote_status_for_tracking(None, "TBA000000000001")
        assert new_status == "shipped"

    def test_does_not_downgrade_delivered(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        new_status, carrier = auto_promote_status_for_tracking(
            "delivered", "1Z999AA10123456784"
        )
        assert new_status is None, "delivered must never get downgraded by tracking"
        assert carrier == "UPS", "carrier should still be reported for backfill"

    def test_does_not_change_already_shipped(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        new_status, _ = auto_promote_status_for_tracking("shipped", "1Z999AA10123456784")
        assert new_status is None

    def test_no_tracking_no_change(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        new_status, carrier = auto_promote_status_for_tracking("pending", "")
        assert new_status is None
        assert carrier is None

    def test_existing_carrier_not_re_detected(self):
        from src.core.carrier_tracking import auto_promote_status_for_tracking
        # When operator already entered a carrier, trust them — don't override
        # via shape detection (which can mis-attribute 12-digit codes).
        _, carrier = auto_promote_status_for_tracking(
            "pending", "123456789012", current_carrier="DHL"
        )
        assert carrier == "DHL"


class TestV2DALAutoPromote:

    def test_setting_tracking_promotes_to_shipped(self, app, temp_data_dir):
        """update_line_status(field='tracking_number') auto-bumps sourcing_status."""
        from src.core import order_dal
        order_id = "ord_auto_v2_a"
        line_db_id = _seed_v2_line(temp_data_dir, order_id, status="pending")

        ok = order_dal.update_line_status(
            order_id, line_db_id, "tracking_number", "1Z999AA10123456784"
        )
        assert ok

        db = _conn(temp_data_dir)
        row = db.execute(
            "SELECT sourcing_status, tracking_number, carrier, ship_date "
            "FROM order_line_items WHERE id=?", (line_db_id,)
        ).fetchone()
        db.close()
        assert row["tracking_number"] == "1Z999AA10123456784"
        assert row["sourcing_status"] == "shipped", "tracking should auto-promote"
        assert row["carrier"] == "UPS"
        assert row["ship_date"], "ship_date should be stamped on auto-promotion"

    def test_delivered_line_keeps_status_when_tracking_added(self, app, temp_data_dir):
        from src.core import order_dal
        order_id = "ord_auto_v2_b"
        line_db_id = _seed_v2_line(
            temp_data_dir, order_id, status="delivered", ship_date="2026-04-01"
        )

        ok = order_dal.update_line_status(
            order_id, line_db_id, "tracking_number", "1Z999AA10123456784"
        )
        assert ok

        db = _conn(temp_data_dir)
        row = db.execute(
            "SELECT sourcing_status, ship_date FROM order_line_items WHERE id=?",
            (line_db_id,)
        ).fetchone()
        db.close()
        assert row["sourcing_status"] == "delivered"
        assert row["ship_date"] == "2026-04-01", "ship_date must not be overwritten"

    def test_audit_log_records_auto_promotion(self, app, temp_data_dir):
        from src.core import order_dal
        order_id = "ord_auto_v2_audit"
        line_db_id = _seed_v2_line(temp_data_dir, order_id, status="pending")

        order_dal.update_line_status(
            order_id, line_db_id, "tracking_number", "1Z999AA10123456784"
        )

        db = _conn(temp_data_dir)
        rows = db.execute(
            "SELECT action, field, new_value, actor FROM order_audit_log "
            "WHERE order_id=? ORDER BY id", (order_id,)
        ).fetchall()
        db.close()
        # We expect the line_update entry plus the auto_promote entry
        actions = [(r["action"], r["field"], r["actor"]) for r in rows]
        assert ("auto_promote", "sourcing_status", "carrier_tracking_auto") in actions

    def test_existing_carrier_not_overwritten(self, app, temp_data_dir):
        """Operator-entered carrier must not be replaced by shape detection."""
        from src.core import order_dal
        order_id = "ord_auto_v2_c"
        line_db_id = _seed_v2_line(
            temp_data_dir, order_id, status="pending", carrier="DHL"
        )

        order_dal.update_line_status(
            order_id, line_db_id, "tracking_number", "1Z999AA10123456784"
        )

        db = _conn(temp_data_dir)
        row = db.execute(
            "SELECT carrier FROM order_line_items WHERE id=?", (line_db_id,)
        ).fetchone()
        db.close()
        assert row["carrier"] == "DHL"


class TestLegacyPOAutoPromote:
    """The /api/po/<id>/update-item route is what the operator clicks.
    Sending a tracking number with no explicit status must work."""

    def _seed_po(self, temp_data_dir, po_number="PO-AUTO-LEG-1"):
        from src.core.db import get_db
        now = datetime.now().isoformat()
        po_id = "po_" + po_number.replace("-", "_").lower()
        with get_db() as conn:
            conn.execute(
                "INSERT INTO purchase_orders (id, po_number, status, total_amount, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?)",
                (po_id, po_number, "received", 0, now, now)
            )
            cur = conn.execute(
                "INSERT INTO po_line_items (po_id, line_number, description, "
                "qty_ordered, unit_price, status, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (po_id, 1, "Widget", 1, 10.00, "pending", now)
            )
            return po_id, cur.lastrowid

    def test_tracking_only_auto_promotes(self, auth_client, temp_data_dir):
        po_id, line_id = self._seed_po(temp_data_dir, "PO-AUTO-LEG-A")

        resp = auth_client.post(
            f"/api/po/{po_id}/update-item",
            json={"item_id": line_id, "tracking_number": "1Z999AA10123456784"}
        )
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

        db = _conn(temp_data_dir)
        row = db.execute(
            "SELECT status, tracking_number, carrier, ship_date "
            "FROM po_line_items WHERE id=?", (line_id,)
        ).fetchone()
        db.close()
        assert row["status"] == "shipped"
        assert row["tracking_number"] == "1Z999AA10123456784"
        assert row["carrier"] == "UPS"
        assert row["ship_date"]

    def test_explicit_status_still_honored(self, auth_client, temp_data_dir):
        """Operator passing status=delivered + tracking should land on delivered."""
        po_id, line_id = self._seed_po(temp_data_dir, "PO-AUTO-LEG-B")

        resp = auth_client.post(
            f"/api/po/{po_id}/update-item",
            json={"item_id": line_id, "status": "delivered",
                  "tracking_number": "1Z999AA10123456784"}
        )
        assert resp.status_code == 200

        db = _conn(temp_data_dir)
        row = db.execute(
            "SELECT status FROM po_line_items WHERE id=?", (line_id,)
        ).fetchone()
        db.close()
        assert row["status"] == "delivered"

    def test_no_status_no_tracking_returns_400(self, auth_client, temp_data_dir):
        po_id, line_id = self._seed_po(temp_data_dir, "PO-AUTO-LEG-C")
        resp = auth_client.post(
            f"/api/po/{po_id}/update-item", json={"item_id": line_id}
        )
        assert resp.status_code == 400
