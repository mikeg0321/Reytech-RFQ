"""Tests for the DSH PO prefix backfill (PR #637).

Sibling of the CalVet backfill in PR #636. Same pattern: parse
path strips `4440-` prefix off DSH POs, leaving them as bare
numeric tails (`0000050349` for the Atascadero row visible on
prod). Backfill prepends `4440-` to bare-numeric po_numbers on
State Hospital institutions.

These tests lock:
  - DSH (State Hospital) bare-numeric rows get `4440-` prepended
  - Already-prefixed rows untouched
  - CalVet (Veterans Home) rows untouched (avoid stomping the
    sibling backfill's domain)
  - CDCR / non-State-Hospital rows untouched
  - Idempotent across repeated runs
  - Cause-and-effect: po_prefix card DSH count grows after backfill
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
    try:
        conn.execute("DROP INDEX IF EXISTS idx_orders_po_quote")
    except Exception:
        pass
    conn.commit()


def _seed_order(conn, *, order_id, po_number="", quote_number="",
                institution="", agency="", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, institution,
          100.0, "open", "[]", when, when, is_test))


def test_dsh_backfill_prepends_4440_to_state_hospital_bare_numeric():
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        # 4 DSH facilities — bare numeric POs, the bug
        _seed_order(c, order_id="dsh_atas", po_number="0000050349",
                    quote_number="QA",
                    institution="Atascadero State Hospital")
        _seed_order(c, order_id="dsh_napa", po_number="1234567",
                    quote_number="QN",
                    institution="Napa State Hospital")
        _seed_order(c, order_id="dsh_patt", po_number="9876543",
                    quote_number="QP",
                    institution="Patton State Hospital")
        _seed_order(c, order_id="dsh_metr", po_number="5555555",
                    quote_number="QM",
                    institution="Metropolitan State Hospital")
        # Already-prefixed DSH row — must NOT double-prefix
        _seed_order(c, order_id="dsh_ok", po_number="4440-1234567",
                    quote_number="QO",
                    institution="Coalinga State Hospital")
        # CalVet row (Veterans Home) — must NOT touch (sibling backfill's domain)
        _seed_order(c, order_id="cv1", po_number="0000067018",
                    quote_number="QV",
                    institution="Veterans Home of California - Barstow")
        # CDCR (State Prison) row — must NOT touch
        _seed_order(c, order_id="cdcr1", po_number="0000099999",
                    quote_number="QC",
                    institution="North Kern State Prison")
        c.commit()

    _migrate_columns()

    with _conn() as c:
        rows = c.execute(
            "SELECT id, po_number FROM orders ORDER BY id"
        ).fetchall()
    by_id = {r["id"]: r["po_number"] for r in rows}

    # DSH backfilled
    assert by_id["dsh_atas"] == "4440-0000050349"
    assert by_id["dsh_napa"] == "4440-1234567"
    assert by_id["dsh_patt"] == "4440-9876543"
    assert by_id["dsh_metr"] == "4440-5555555"
    # Already-prefixed DSH untouched
    assert by_id["dsh_ok"] == "4440-1234567"
    # CalVet picked up by SIBLING backfill (different prefix), not this one
    assert by_id["cv1"] == "8955-0000067018"
    # CDCR untouched (not State Hospital, not Veterans Home)
    assert by_id["cdcr1"] == "0000099999"


def test_dsh_backfill_is_idempotent():
    """Re-running on already-prefixed rows must NOT match."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="dsh1", po_number="1234567",
                    quote_number="Q1",
                    institution="Atascadero State Hospital")
        c.commit()
    _migrate_columns()
    _migrate_columns()
    _migrate_columns()
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = 'dsh1'"
        ).fetchone()
    # Single prefix only — not 4440-4440-…
    assert row["po_number"] == "4440-1234567"


def test_dsh_backfill_skips_already_4500_prefixed():
    """A CCHCS-prefixed PO (`4500NNNN`) on a State-Hospital-named
    institution (unlikely but theoretically possible — operator
    miscategorized) must NOT get `4440-` prepended on top."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="weird", po_number="4500999999",
                    quote_number="QW",
                    institution="Some State Hospital")
        c.commit()
    _migrate_columns()
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = 'weird'"
        ).fetchone()
    # Untouched — 4500 prefix excluded by the backfill clause
    assert row["po_number"] == "4500999999"


def test_dsh_backfill_skips_long_pos_outside_canonical_length():
    """DSH canonical tail is 7-9 digits. Bare numerics longer than
    12 digits aren't DSH POs — likely some other identifier
    (transaction ID, etc.). Conservative range avoids those."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="long", po_number="123456789012345",
                    quote_number="QL",
                    institution="Atascadero State Hospital")
        c.commit()
    _migrate_columns()
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = 'long'"
        ).fetchone()
    # Untouched — outside the 7-12 length window
    assert row["po_number"] == "123456789012345"


def test_po_prefix_card_dsh_count_grows_after_backfill():
    """End-to-end: DSH bucket on po_prefix card flips from 0 to N
    after backfill runs. The diagnostic→cure pair locked together."""
    from src.core.db import _migrate_columns
    from src.api.modules.routes_health import _build_po_prefix_card

    with _conn() as c:
        _wipe(c)
        for i, fac in enumerate([
            "Atascadero State Hospital",
            "Napa State Hospital",
            "Patton State Hospital",
        ]):
            _seed_order(c, order_id=f"dsh{i}",
                        po_number=f"000005{i:04d}",
                        quote_number=f"Q{i}",
                        institution=fac)
        c.commit()

    pre = _build_po_prefix_card()
    assert pre["by_prefix"]["DSH"] == 0
    assert pre["unidentified"] == 3

    _migrate_columns()

    post = _build_po_prefix_card()
    assert post["by_prefix"]["DSH"] == 3
    assert post["unidentified"] == 0
