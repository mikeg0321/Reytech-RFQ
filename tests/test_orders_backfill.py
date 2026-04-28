"""Tests for `src/core/orders_backfill.py:ensure_order_for_won_quote`.

Closes the gap PR #629's orders-drift card surfaced on prod (102/102
won quotes had NO matching `orders` row). Three background workers
(award_tracker, email_poller, revenue_engine) update
`quotes.status='won'` directly without creating an `orders` row;
this helper exists for them to call after the UPDATE.

Tests lock the idempotency contract — calling the helper on a quote
that already has an order must NOT write a duplicate row, since the
revenue_engine sync calls it on every periodic run. Without
idempotency, every sync would either fail (if a UNIQUE constraint
landed) or duplicate-write (if not).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest


def _fn():
    from src.core.orders_backfill import ensure_order_for_won_quote
    return ensure_order_for_won_quote


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_quote(conn, *, quote_number, status="won", total=300.0,
                items=None, po_number="", contact_email="b@x.gov",
                contact_name="Buyer", agency="CDCR", is_test=0):
    when = datetime.now().isoformat()
    items = items if items is not None else []
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, institution, status, total, subtotal,
           tax, contact_name, contact_email, created_at, updated_at,
           sent_at, is_test, line_items, po_number, ship_to_name)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, agency, "Test Inst", status, total,
          total * 0.92, total * 0.08, contact_name, contact_email,
          when, when, when, is_test, json.dumps(items), po_number,
          "Ship To"))


def _count_orders(conn, quote_number):
    r = conn.execute(
        "SELECT COUNT(*) AS n FROM orders WHERE quote_number = ?",
        (quote_number,)
    ).fetchone()
    return int(r["n"] if r else 0)


# ── Happy path: creates the orders row ─────────────────────────────────


def test_creates_order_when_won_quote_has_none():
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="WIN-1",
                    items=[{"description": "Item A", "qty": 2,
                            "pricing": {"unit_price": 50.0}}])
        c.commit()

    out = fn("WIN-1", po_number="PO-12345", actor="test")
    assert out["ok"] is True
    assert out["created"] is True
    assert out["order_id"] == "ORD-WIN-1"

    with _conn() as c:
        assert _count_orders(c, "WIN-1") == 1
        row = c.execute(
            "SELECT po_number, total, agency FROM orders WHERE quote_number = ?",
            ("WIN-1",)
        ).fetchone()
        assert row["po_number"] == "PO-12345"
        assert float(row["total"]) == 300.0
        assert row["agency"] == "CDCR"


def test_passes_through_quote_metadata_to_order():
    """contact_email / agency / institution / total flow through to
    the orders row so /orders renders correctly without back-joining
    the quote."""
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="META-1", total=999.99,
                    contact_email="alice@cdcr.ca.gov",
                    contact_name="Alice A", agency="CDCR")
        c.commit()
    out = fn("META-1")
    assert out["ok"] is True

    with _conn() as c:
        row = c.execute("""
            SELECT buyer_email, buyer_name, agency, total
            FROM orders WHERE quote_number = ?
        """, ("META-1",)).fetchone()
        assert row["buyer_email"] == "alice@cdcr.ca.gov"
        assert row["buyer_name"] == "Alice A"
        assert row["agency"] == "CDCR"
        assert float(row["total"]) == 999.99


# ── Idempotency: critical contract ─────────────────────────────────────


def test_does_not_create_duplicate_when_order_already_exists():
    """The SHARP edge: revenue_engine fires every sync. Without
    idempotency, every sync attempts to insert a duplicate."""
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="IDEM-1")
        c.commit()
    fn("IDEM-1", po_number="PO-1")
    fn("IDEM-1", po_number="PO-1")
    fn("IDEM-1", po_number="PO-1")
    with _conn() as c:
        assert _count_orders(c, "IDEM-1") == 1


def test_returns_existing_order_id_with_created_false():
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="EXIST-1")
        c.commit()
    first = fn("EXIST-1")
    second = fn("EXIST-1")
    assert first["created"] is True
    assert second["created"] is False
    assert second["order_id"] == first["order_id"]


# ── Edge cases ──────────────────────────────────────────────────────────


def test_returns_error_when_quote_number_blank():
    fn = _fn()
    out = fn("")
    assert out["ok"] is False
    assert "required" in out["error"].lower()
    assert out["created"] is False


def test_returns_error_when_quote_does_not_exist():
    fn = _fn()
    with _conn() as c:
        _wipe(c)
    out = fn("DOES-NOT-EXIST")
    assert out["ok"] is False
    assert "not found" in out["error"].lower()


def test_works_when_quote_has_no_line_items():
    """Some legacy quotes have empty line_items but valid total —
    helper still creates the order (downstream code handles empty items)."""
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="EMPTY-1", items=[])
        c.commit()
    out = fn("EMPTY-1")
    assert out["ok"] is True
    assert out["created"] is True


def test_handles_malformed_line_items_json():
    """If line_items column is corrupt, helper still produces an order
    (with empty line_items) rather than crashing."""
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="BAD-1")
        c.execute(
            "UPDATE quotes SET line_items = ? WHERE quote_number = ?",
            ("not-json", "BAD-1")
        )
        c.commit()
    out = fn("BAD-1")
    assert out["ok"] is True
    assert out["created"] is True


def test_test_flag_propagates_from_quote_to_order():
    """is_test=1 on the quote must mark the order is_test=1 too —
    otherwise test-quote-conversion pollutes headline /orders revenue."""
    fn = _fn()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="TQ-1", is_test=1)
        c.commit()
    fn("TQ-1")
    with _conn() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE quote_number = ?",
            ("TQ-1",)
        ).fetchone()
        assert row["is_test"] == 1


def test_safe_when_db_query_raises(monkeypatch):
    """If the DB connection fails entirely, helper returns ok=False
    without exposing the exception to the caller (we don't want a
    background worker crashing because of an SQLite hiccup). Patches
    src.core.db.get_db at the source since orders_backfill imports
    it lazily inside the function body."""
    import src.core.db as _db

    class _Boom:
        def __enter__(self): raise RuntimeError("simulated DB failure")
        def __exit__(self, *a): return False

    monkeypatch.setattr(_db, "get_db", lambda: _Boom())
    fn = _fn()
    out = fn("ANYTHING")
    assert out["ok"] is False
    assert "simulated" in out["error"].lower()


# ── Integration: drift-card recovery ────────────────────────────────────


def test_drift_card_flips_from_error_to_healthy_after_helper_runs():
    """Lock the cause-and-effect: orders-drift card was the
    diagnostic; this helper is the cure. Card should go error →
    healthy when ensure_order_for_won_quote runs over orphan wins."""
    fn = _fn()
    from src.api.modules.routes_health import _build_orders_drift_card

    with _conn() as c:
        _wipe(c)
        # Three orphan wins (no orders row) → 100% drift → error.
        for i in range(3):
            _seed_quote(c, quote_number=f"DRIFT-{i}")
        c.commit()
    pre = _build_orders_drift_card()
    assert pre["status"] == "error"
    assert pre["drift_pct"] == 100.0
    assert pre["won_quotes_no_order"] == 3

    # Run the backfill helper on each.
    for i in range(3):
        fn(f"DRIFT-{i}")

    post = _build_orders_drift_card()
    assert post["status"] == "healthy"
    assert post["drift_pct"] == 0.0
    assert post["won_quotes_no_order"] == 0
