"""Tests for `clean_po_number` + the orders backfill that NULL-s out
sentinel po_numbers.

Surfaced 2026-04-28 via PR #632's po_aggregate card: the literal
string `"N/A"` was entered as po_number on 6 distinct orders ($219k
total), pretending to be a multi-quote PO. These tests:

  1. Lock the canonical sentinel set so every write path treats
     `"N/A"`, `"TBD"`, etc. as "no PO" rather than a real PO.
  2. Lock the boot-time backfill that retro-clears existing prod
     rows. Without backfill, the live N/A rows would persist on
     prod; without sanitization, they'd come back the next time an
     operator typed N/A.
  3. Lock the cause-and-effect: po_aggregate "MULTI-QUOTE" count
     drops by 1 once the N/A row is cleared (it was 1 of the 28).
"""
from __future__ import annotations

from datetime import datetime

import pytest


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


def _seed_order(conn, *, order_id, quote_number="", po_number="",
                total=100.0, agency="CDCR", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, "",
          total, "open", "[]", when, when, is_test))


# ── clean_po_number sentinel rules ──────────────────────────────────────


@pytest.mark.parametrize("raw,expected", [
    ("PO-12345",   "PO-12345"),     # real PO passes through
    ("0000053217", "0000053217"),   # numeric PO from prod
    ("  PO-1  ",   "PO-1"),         # whitespace stripped
    ("",           ""),
    (None,         ""),
    ("N/A",        ""),
    ("n/a",        ""),
    ("NA",         ""),
    ("na",         ""),
    ("TBD",        ""),
    ("tbd",        ""),
    ("TBA",        ""),
    ("PENDING",    ""),
    ("pending",    ""),
    ("NONE",       ""),
    ("None",       ""),
    ("NULL",       ""),
    ("?",          ""),
    ("??",         ""),
    ("???",        ""),
    ("-",          ""),
    ("--",         ""),
    ("X",          ""),
    ("xx",         ""),
    ("XXX",        ""),
    ("UNKNOWN",    ""),
    ("unknown",    ""),
    ("  N/A  ",    ""),             # whitespace + sentinel
])
def test_clean_po_number_handles_sentinels(raw, expected):
    from src.core.order_dal import clean_po_number
    assert clean_po_number(raw) == expected


def test_clean_po_number_does_not_strip_legit_pos_with_letters():
    """Real PO numbers like 'PO-N123' contain N but aren't sentinels.
    The check is on the FULL trimmed/uppered value, not a substring."""
    from src.core.order_dal import clean_po_number
    assert clean_po_number("PO-N123") == "PO-N123"
    assert clean_po_number("NA-5678") == "NA-5678"     # not exactly 'NA'
    assert clean_po_number("X-1") == "X-1"             # not exactly 'X'


# ── save_order integration ──────────────────────────────────────────────


def test_save_order_strips_na_sentinel():
    """The canonical write path must apply clean_po_number — every
    other path eventually delegates here."""
    from src.core.order_dal import save_order
    when = datetime.now().isoformat()
    save_order("ord-1", {
        "quote_number": "Q1", "po_number": "N/A",
        "agency": "CDCR", "total": 100.0,
        "created_at": when,
    })
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = ?", ("ord-1",)
        ).fetchone()
    assert row["po_number"] == ""


def test_save_order_preserves_real_po():
    from src.core.order_dal import save_order
    when = datetime.now().isoformat()
    save_order("ord-real", {
        "quote_number": "Q1", "po_number": "PO-9999",
        "agency": "CDCR", "total": 100.0,
        "created_at": when,
    })
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = ?", ("ord-real",)
        ).fetchone()
    assert row["po_number"] == "PO-9999"


# ── Boot-time backfill ──────────────────────────────────────────────────


def test_migrate_columns_clears_existing_sentinel_pos():
    """Existing rows on prod with po_number='N/A' must get cleared
    on next boot. Without this, the 6 known N/A rows persist
    forever — the sanitization only catches future writes."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="dirty-1", po_number="N/A")
        _seed_order(c, order_id="dirty-2", po_number="TBD")
        _seed_order(c, order_id="dirty-3", po_number="?")
        _seed_order(c, order_id="dirty-4", po_number="  N/A ")
        _seed_order(c, order_id="clean", po_number="PO-OK")
        c.commit()

    _migrate_columns()

    with _conn() as c:
        rows = c.execute(
            "SELECT id, po_number FROM orders ORDER BY id"
        ).fetchall()
    by_id = {r["id"]: r["po_number"] for r in rows}
    assert by_id["dirty-1"] == ""
    assert by_id["dirty-2"] == ""
    assert by_id["dirty-3"] == ""
    assert by_id["dirty-4"] == ""
    assert by_id["clean"] == "PO-OK"


def test_backfill_is_idempotent():
    """Backfill runs on every boot. Re-running on already-clean rows
    must be a no-op (don't trigger spurious updated_at bumps)."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ord-1", po_number="N/A")
        c.commit()
    _migrate_columns()
    _migrate_columns()
    _migrate_columns()
    with _conn() as c:
        n = c.execute("""
            SELECT COUNT(*) AS n FROM orders
            WHERE po_number = 'N/A'
        """).fetchone()["n"]
    assert n == 0


# ── Cause-and-effect: po_aggregate count drops ─────────────────────────


def test_po_aggregate_multi_quote_count_drops_after_backfill():
    """Lock the diagnostic→cure pair: po_aggregate showed 28
    multi-quote POs before this PR; one of those was the N/A
    sentinel covering 6 quotes. After backfill, that group
    disappears entirely and the legitimate-multi count drops by 1."""
    from src.core.db import _migrate_columns
    from src.api.modules.routes_health import _build_po_aggregate_card

    with _conn() as c:
        _wipe(c)
        # 6 orders sharing po_number='N/A' (the N/A bug)
        for i in range(6):
            _seed_order(c, order_id=f"na-{i}", quote_number=f"NQ{i}",
                        po_number="N/A")
        # 1 legitimate multi-quote PO (3 orders share it)
        for i in range(3):
            _seed_order(c, order_id=f"legit-{i}",
                        quote_number=f"LQ{i}", po_number="PO-LEGIT")
        # 1 single-quote PO
        _seed_order(c, order_id="single", quote_number="SQ",
                    po_number="PO-SINGLE")
        c.commit()

    pre = _build_po_aggregate_card()
    # Before backfill: 3 PO groups (N/A, PO-LEGIT, PO-SINGLE)
    # 2 multi-quote (N/A=6 quotes, PO-LEGIT=3 quotes), 1 single
    assert pre["total_pos"] == 3
    assert pre["multi_quote_pos"] == 2
    # The N/A group is in biggest_pos with quote_count=6
    na_entry = next((p for p in pre["biggest_pos"] if p["po_number"] == "N/A"), None)
    assert na_entry is not None
    assert na_entry["quote_count"] == 6

    _migrate_columns()

    post = _build_po_aggregate_card()
    # After: only PO-LEGIT (multi) and PO-SINGLE (single) remain
    assert post["total_pos"] == 2
    assert post["multi_quote_pos"] == 1
    assert post["single_quote_pos"] == 1
    # N/A no longer present
    assert not any(p["po_number"] == "N/A" for p in post["biggest_pos"])
    assert not any(p["po_number"] == "" for p in post["biggest_pos"])


# ── Drift card semantics ────────────────────────────────────────────────


def test_orders_drift_orders_no_po_picks_up_cleared_rows():
    """After backfill, the 6 N/A rows show up in the drift card's
    orders_no_po counter (since they're now empty po_number with
    open status). That's the correct surface — they're missing a
    PO, not a duplicate of one."""
    from src.core.db import _migrate_columns
    from src.api.modules.routes_health import _build_orders_drift_card

    with _conn() as c:
        _wipe(c)
        for i in range(6):
            _seed_order(c, order_id=f"na-{i}", quote_number=f"NQ{i}",
                        po_number="N/A")
        c.commit()

    pre = _build_orders_drift_card()
    # Before: 1 duplicate po_number group ('N/A' on 6 rows)
    assert pre["duplicate_po_numbers"] == 1
    assert pre["orders_no_po"] == 0

    _migrate_columns()

    post = _build_orders_drift_card()
    # After: dup count drops, orders-no-po picks them up
    assert post["duplicate_po_numbers"] == 0
    assert post["orders_no_po"] == 6
