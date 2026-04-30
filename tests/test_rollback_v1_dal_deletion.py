"""Regression tests for the V1 DAL deletion (2026-04-30 audit drift #1).

The legacy `core.dal.save_order` / `save_rfq` / `save_pc` were deleted because:
  1. They wrote a 12-13 col subset of each table — silently dropping fields
     like buyer_email, total_cost, margin_pct, ship_to_address, body_text,
     solicitation_number, due_date.
  2. `save_order` skipped the PR #664 ensure_quote_won_for_order hook, so
     rolling back a paid order left its paired quote stuck in 'open' status.

Only kept-alive caller was `routes_v1.py:api_v1_rollback`. This file locks:
  - The legacy functions are gone (import fails as expected).
  - The rollback path now uses canonical writers AND fires the PR #664 hook.
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes", "rfqs", "price_checks"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


# ── V1 DAL functions are gone ─────────────────────────────────────────


def test_legacy_save_rfq_is_deleted():
    """core.dal.save_rfq must not be importable. Anything still trying to
    use it surfaces as an ImportError instead of silently writing 12 of 22
    columns and dropping the rest."""
    with pytest.raises(ImportError):
        from src.core.dal import save_rfq  # noqa: F401


def test_legacy_save_pc_is_deleted():
    with pytest.raises(ImportError):
        from src.core.dal import save_pc  # noqa: F401


def test_legacy_save_order_is_deleted():
    with pytest.raises(ImportError):
        from src.core.dal import save_order  # noqa: F401


# ── Rollback path: order write must fire PR #664 hook ─────────────────


def test_rollback_order_flips_paired_quote_to_won():
    """The footgun the audit named: rolling back a paid order via the
    legacy V1 DAL silently skipped the ensure_quote_won_for_order hook,
    leaving the quote stuck in 'open' status. After this PR, the rollback
    path uses order_dal.save_order which fires the hook."""
    # Seed: a quote in 'open' state + an order pointing to it
    with _conn() as c:
        _wipe(c)
        when = datetime.now().isoformat()
        c.execute(
            """INSERT INTO quotes (quote_number, agency, institution, status,
                total, subtotal, tax, contact_name, contact_email,
                created_at, updated_at, is_test, line_items)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("ROLL-1", "CDCR", "CSP-Sac", "open", 500.0, 460.0, 40.0,
             "Buyer", "b@x.gov", when, when, 0, "[]"),
        )
        c.commit()

    # Simulate the rollback path's order write through the canonical writer
    from src.core.order_dal import save_order
    save_order("ORD-ROLLBACK-1", {
        "id": "ORD-ROLLBACK-1",
        "quote_number": "ROLL-1",
        "po_number": "PO-ROLLBACK",
        "agency": "CDCR",
        "total": 500.0,
        "status": "shipped",
        "buyer_email": "b@x.gov",
        "total_cost": 380.0,
        "margin_pct": 24.0,
        "ship_to_address": [],
    }, actor="rollback")

    # The hook fired — quote status flipped open -> won
    with _conn() as c:
        row = c.execute(
            "SELECT status FROM quotes WHERE quote_number=?", ("ROLL-1",)
        ).fetchone()
        assert row is not None
        assert row["status"] == "won", (
            f"rollback order write must flip paired quote to 'won' via "
            f"PR #664 hook; got {row['status']!r}"
        )


def test_rollback_writes_full_column_shape():
    """The legacy save_order wrote 13 cols; the canonical writer writes 20.
    Lock that buyer_email / total_cost / margin_pct land on the row so a
    rollback restore actually restores those fields."""
    from src.core.order_dal import save_order
    with _conn() as c:
        _wipe(c)
        c.commit()

    save_order("ORD-FULL-COLS", {
        "id": "ORD-FULL-COLS",
        "quote_number": "",
        "po_number": "PO-FULL",
        "agency": "calvet",
        "total": 1234.56,
        "status": "shipped",
        "buyer_email": "alice@calvet.ca.gov",
        "buyer_name": "Alice A",
        "ship_to": "VHC-Yountville",
        "total_cost": 950.00,
        "margin_pct": 23.0,
        "fulfillment_type": "dropship",
    }, actor="rollback")

    with _conn() as c:
        row = c.execute(
            """SELECT buyer_email, buyer_name, ship_to, total_cost, margin_pct,
                      fulfillment_type FROM orders WHERE id=?""",
            ("ORD-FULL-COLS",),
        ).fetchone()
        assert row is not None
        assert row["buyer_email"] == "alice@calvet.ca.gov"
        assert row["buyer_name"] == "Alice A"
        assert row["ship_to"] == "VHC-Yountville"
        assert float(row["total_cost"]) == 950.00
        assert float(row["margin_pct"]) == 23.0
        assert row["fulfillment_type"] == "dropship"
