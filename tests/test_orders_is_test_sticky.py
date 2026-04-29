"""Tests for PR-6 — `orders.is_test` must be sticky on save_order().

Background: PR #643 cleaned up the prod TEST sentinel row by flipping
is_test=1. After PR #644 deployed, the row reappeared with is_test=0.
Root cause: save_order's ON CONFLICT clause unconditionally set
`is_test = excluded.is_test`, so any worker re-saving the order
without explicitly carrying the flag flipped it back to 0.

The architectural fix: once an order is is_test=1, no subsequent
save_order() call can flip it to 0. Implemented via
`is_test = MAX(orders.is_test, excluded.is_test)` in the ON CONFLICT
clause.

Per Mike's `feedback_fix_at_architecture_layer` memory: this is the
ingestion-layer fix, not a one-off cleanup script.
"""
from __future__ import annotations


def _seed_order(conn, order_id, **kw):
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, kw.get("quote_number", "Q1"),
          kw.get("po_number", "TEST"),
          kw.get("agency", ""),
          kw.get("institution", ""),
          float(kw.get("total", 0)), kw.get("status", "new"),
          "[]", "2026-04-28T00:00:00", "2026-04-28T00:00:00",
          int(kw.get("is_test", 0))))


def test_save_order_does_not_clear_is_test_flag(auth_client):
    """A row that's already is_test=1 must stay is_test=1 even if
    save_order is called again with no is_test field."""
    from src.core.db import get_db
    from src.core.order_dal import save_order
    with get_db() as c:
        _seed_order(c, "ord-test-1", is_test=1, po_number="TEST")
        c.commit()

    # Caller doesn't pass is_test — defaults to 0. Old behavior would
    # flip the row to is_test=0; new behavior keeps it at 1.
    save_order("ord-test-1", {
        "quote_number": "Q1",
        "po_number": "TEST",
        "agency": "",
        "institution": "",
        "total": 0,
        "status": "new",
        # NOTE: no is_test key — that's the bug we're fixing.
    })

    with get_db() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE id=?",
            ("ord-test-1",),
        ).fetchone()
    assert row["is_test"] == 1, (
        "is_test was reset to 0 by save_order — sticky guard regressed"
    )


def test_save_order_can_still_promote_zero_to_one(auth_client):
    """Forward direction still works: a row at is_test=0 can be
    promoted to is_test=1 by passing it explicitly."""
    from src.core.db import get_db
    from src.core.order_dal import save_order
    with get_db() as c:
        _seed_order(c, "ord-promote-1", is_test=0, po_number="REAL-PO")
        c.commit()

    save_order("ord-promote-1", {
        "quote_number": "Q1",
        "po_number": "REAL-PO",
        "agency": "",
        "institution": "",
        "total": 0,
        "status": "new",
        "is_test": 1,
    })

    with get_db() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE id=?",
            ("ord-promote-1",),
        ).fetchone()
    assert row["is_test"] == 1


def test_first_save_writes_is_test_zero_when_not_supplied(auth_client):
    """Insert path (no existing row) defaults is_test=0 unmodified.
    Sticky logic only kicks in on conflict."""
    from src.core.db import get_db
    from src.core.order_dal import save_order
    with get_db() as c:
        try:
            c.execute("DELETE FROM orders WHERE id='ord-fresh'")
        except Exception:
            pass
        c.commit()

    save_order("ord-fresh", {
        "quote_number": "Q-fresh",
        "po_number": "8955-0000044935",
        "agency": "",
        "institution": "",
        "total": 100,
        "status": "new",
    })

    with get_db() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE id=?",
            ("ord-fresh",),
        ).fetchone()
    assert row["is_test"] == 0


def test_save_order_idempotent_on_flagged_row(auth_client):
    """Calling save_order twice on a flagged row keeps it flagged
    both times — the sticky guard is idempotent."""
    from src.core.db import get_db
    from src.core.order_dal import save_order
    with get_db() as c:
        _seed_order(c, "ord-test-2", is_test=1)
        c.commit()

    payload = {
        "quote_number": "Q1",
        "po_number": "TEST",
        "agency": "",
        "institution": "",
        "total": 0,
        "status": "new",
    }
    save_order("ord-test-2", payload)
    save_order("ord-test-2", payload)
    save_order("ord-test-2", payload)

    with get_db() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE id=?",
            ("ord-test-2",),
        ).fetchone()
    assert row["is_test"] == 1


def test_save_order_inherits_is_test_from_quote_link(auth_client):
    """Existing BUILD-10 behavior preserved: when the order doesn't
    carry is_test but its quote_number row in `quotes` is is_test=1,
    save_order picks up the flag from the linked quote."""
    from src.core.db import get_db
    from src.core.order_dal import save_order
    qn = "Q-linked-test"
    with get_db() as c:
        # Make sure a quote row with is_test=1 exists for the link.
        try:
            c.execute("DELETE FROM quotes WHERE quote_number=?", (qn,))
            c.execute("DELETE FROM orders WHERE id='ord-link-1'")
        except Exception:
            pass
        c.execute("""
            INSERT INTO quotes (quote_number, total, status, is_test,
                                created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (qn, 100, "won", 1,
              "2026-04-28T00:00:00", "2026-04-28T00:00:00"))
        c.commit()

    save_order("ord-link-1", {
        "quote_number": qn,
        "po_number": "8955-0000044935",
        "agency": "",
        "institution": "",
        "total": 100,
        "status": "new",
        # No is_test — should inherit from the quote.
    })

    with get_db() as c:
        row = c.execute(
            "SELECT is_test FROM orders WHERE id=?",
            ("ord-link-1",),
        ).fetchone()
    assert row["is_test"] == 1
