"""Orders V2 dual-write — legacy email poller rollup must mirror to V2.

When `ingest.po_email_v2` is OFF (rollback path), the legacy poller
`_process_po_email_legacy` updates purchase_orders on inbound status
emails but used to leave Orders V2 stale. After PR #306 shipped the
user-CRUD mirror, this closes the last remaining poller-side gap so
the FF is safely reversible without re-introducing split-brain.

Contract:

  1. After `_process_po_email_legacy` matches a PO and `_recalculate_po_status`
     produces a final rollup, the caller MUST forward that rollup to
     `_mirror_status_to_orders_v2`.

  2. The mirror is best-effort. A failure MUST NOT undo the legacy writes
     the poller already made (po_emails row, po_line_items updates,
     purchase_orders.status).

  3. When no PO matches (zero hits), no mirror call should fire — there's
     nothing to mirror, and an unsolicited call with a non-existent
     po_number would log a spurious warning.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import uuid
from datetime import datetime

import pytest


def _ot_module():
    return sys.modules["src.api.modules.routes_order_tracking"]


@pytest.fixture(autouse=True)
def _init_po_schema(app, temp_data_dir):
    _ot_module()._init_po_tracking_db()
    yield


def _seed_po(temp_data_dir, po_number):
    """Seed legacy + V2 rows the way `create_po` + boot migration do
    together. Leaves a single pending line item so recalc has something
    to roll up."""
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
         now, 100.0, "pending", now, now))
    conn.execute(
        """INSERT INTO po_line_items
           (po_id, line_number, description, qty_ordered, unit_price,
            extended_price, status, updated_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (po_id, 1, "Test item", 1, 100.0, 100.0, "pending", now))
    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, "", po_number, "", "CDCR", 100.0, "pending",
         "Buyer", "b@x.com", now, now, ""))
    conn.execute(
        """INSERT INTO order_line_items
           (order_id, line_number, description, qty_ordered, unit_price,
            extended_price, sourcing_status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (oid, 1, "Test item", 1, 100.0, 100.0, "pending", now, now))
    conn.commit()
    conn.close()
    return po_id, oid


def test_legacy_poller_mirrors_rollup_to_orders_v2(temp_data_dir):
    """`delivered` keyword in the email body → legacy poller flips
    purchase_orders.status → recalc rolls up → V2 orders.status follows
    without waiting for next boot migration.

    Note: `_extract_po_numbers` matches numeric PO numbers only
    (e.g. `PO#12345`), so the seed uses a numeric po_number even though
    most test fixtures use `PO-TEST-...` prefixes for readability."""
    po_number = "88880001"  # numeric — matches _extract_po_numbers
    _seed_po(temp_data_dir, po_number)

    rot = _ot_module()
    result = rot._process_po_email_legacy(
        subject=f"Shipment update PO#{po_number}",
        sender="carrier@example.com",
        body=f"Your order PO#{po_number} has been delivered — signed for at dock.",
        email_uid="uid-poll-mirror-1",
    )
    assert result["matched"] is True, "poller should have matched the seeded PO"

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    legacy = conn.execute(
        "SELECT status FROM purchase_orders WHERE po_number = ?", (po_number,)
    ).fetchone()
    v2 = conn.execute(
        "SELECT status FROM orders WHERE po_number = ?", (po_number,)
    ).fetchone()
    conn.close()

    assert legacy["status"] == "delivered", (
        f"legacy purchase_orders.status must update to 'delivered', "
        f"got {legacy['status']!r}"
    )
    assert v2 is not None, "V2 orders row must exist (seeded)"
    assert v2["status"] == "delivered", (
        f"V2 orders.status must mirror the rollup (not stay 'pending'); "
        f"got {v2['status']!r}. This is the whole point of the poller "
        f"rollup mirror — margin views lag the poller without it."
    )


def test_legacy_poller_mirror_failure_preserves_legacy_write(
        temp_data_dir, monkeypatch):
    """If the V2 mirror raises (simulated via monkeypatch), the legacy
    purchase_orders.status update the poller already made MUST stand."""
    po_number = "88880002"  # numeric — see note in first test
    _seed_po(temp_data_dir, po_number)

    rot = _ot_module()

    def _boom(*a, **kw):
        raise RuntimeError("simulated V2 mirror crash")
    monkeypatch.setattr(rot, "_mirror_status_to_orders_v2", _boom)

    result = rot._process_po_email_legacy(
        subject=f"delivery for PO#{po_number}",
        sender="carrier@example.com",
        body=f"PO#{po_number} delivered to the facility.",
        email_uid="uid-poll-mirror-fail-1",
    )
    assert result["matched"] is True

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    legacy_status = conn.execute(
        "SELECT status FROM purchase_orders WHERE po_number = ?", (po_number,)
    ).fetchone()[0]
    conn.close()
    assert legacy_status == "delivered", (
        f"mirror crash must NOT undo the legacy poller write "
        f"(purchase_orders.status={legacy_status!r})"
    )


def test_legacy_poller_no_match_skips_mirror(temp_data_dir, monkeypatch):
    """If no PO matches, the mirror MUST NOT be called — there's no
    rollup to propagate, and an unsolicited call would log noise."""
    rot = _ot_module()
    calls = []

    def _spy(*a, **kw):
        calls.append((a, kw))
        return True
    monkeypatch.setattr(rot, "_mirror_status_to_orders_v2", _spy)

    result = rot._process_po_email_legacy(
        subject="nothing relevant",
        sender="spam@example.com",
        body="this email references PO-NONEXISTENT-999 which we don't track",
        email_uid="uid-poll-mirror-nomatch",
    )
    assert result["matched"] is False
    assert calls == [], (
        f"mirror should not fire when no PO matched; got {calls}"
    )
