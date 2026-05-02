"""Tests for canonical revenue helper + the three migrated callers.

PR-2 cuts three competing definitions of "YTD revenue" down to one:

  1. `update_revenue_tracker` was taking max() over four unfiltered
     sources — orders had no year filter, so all-time orders sums
     dominated and the home page showed lifetime revenue as YTD.
  2. `get_goal_progress` used FISCAL_YEAR_START='2025-07-01' (CA
     fiscal year), even though Mike confirmed calendar year.
  3. `get_revenue_ytd` filtered orders by created_at + paid/invoiced/
     delivered status — close, but a separate code path means it can
     and did drift from the other two.

After this PR, all three return the same number sourced from
`v_revenue_year_2026`. The legacy values are still computed for one
deploy cycle (Scientist-style) and any disagreement is logged.

These tests lock that contract.
"""
from __future__ import annotations

from datetime import datetime

import pytest


# ─── Helpers ─────────────────────────────────────────────────────────────


def _conn():
    from src.core.db import get_db
    return get_db()


@pytest.fixture(autouse=True)
def _ensure_views(temp_data_dir):
    """Run migrations so v_revenue_year_2026 (and friends) exist.

    The temp_data_dir fixture seeds the schema from db.SCHEMA but
    does NOT trigger migrations.py — those normally run lazily via
    /api/v1/* or the routes_system endpoint. For tests that depend
    on a view created by a migration, we have to ask explicitly.
    """
    from src.core.migrations import run_migrations
    try:
        run_migrations()
    except Exception:
        # If migration fails (e.g. partial schema), let the test
        # surface its own clearer error rather than masking it here.
        pass
    # `orders.payment_amount` is referenced by get_revenue_ytd's
    # legacy query but is added to prod schema out-of-band (not in
    # db.SCHEMA or migrations.py). Add it here so the legacy query
    # doesn't crash before the dual-emit log line fires.
    with _conn() as c:
        for col, coltype in [("payment_amount", "REAL"),
                             ("payment_date", "TEXT"),
                             ("invoice_number", "TEXT"),
                             ("invoice_date", "TEXT")]:
            try:
                c.execute(f"ALTER TABLE orders ADD COLUMN {col} {coltype}")
            except Exception:
                pass
        c.commit()
    yield


def _wipe(conn):
    """Clear every table the canonical view + legacy paths read.

    The canonical view UNIONs orders + revenue_log, so both must be
    empty for the test to start from a known floor. quotes is wiped
    too because the legacy `update_revenue_tracker` queries it.
    """
    for tbl in ("orders", "revenue_log", "quotes"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_order(conn, *, order_id, total, created_at, status="paid",
                po_number=None, quote_number=None,
                agency="CDCR", is_test=0):
    """Seed an order with auto-unique po/quote numbers (orders has a
    UNIQUE constraint on (po_number, quote_number) when both non-empty)."""
    if po_number is None:
        po_number = f"PO-{order_id}"
    if quote_number is None:
        quote_number = f"Q-{order_id}"
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, "",
          total, status, "[]", created_at, created_at, is_test))


def _seed_revenue_log(conn, *, log_id, amount, logged_at,
                      po_number=None, quote_number=None,
                      description="test seed",
                      agency="CDCR", is_test=0):
    if po_number is None:
        po_number = f"PO-{log_id}"
    if quote_number is None:
        quote_number = f"Q-{log_id}"
    conn.execute("""
        INSERT INTO revenue_log
          (id, logged_at, amount, description, source,
           quote_number, po_number, agency, is_test)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (log_id, logged_at, amount, description, "test",
          quote_number, po_number, agency, is_test))


# ─── canonical_year_revenue_total ────────────────────────────────────────


def test_canonical_revenue_empty_db_returns_zero(temp_data_dir):
    from src.core.order_dal import canonical_year_revenue_total
    with _conn() as c:
        _wipe(c)
    assert canonical_year_revenue_total(2026) == 0.0


def test_canonical_revenue_sums_orders_in_year(temp_data_dir):
    """Orders dated within calendar 2026 contribute their total."""
    from src.core.order_dal import canonical_year_revenue_total
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ord-a", total=1000.0,
                    created_at="2026-03-15T12:00:00")
        _seed_order(c, order_id="ord-b", total=2500.0,
                    created_at="2026-08-20T12:00:00")
        c.commit()
    assert canonical_year_revenue_total(2026) == 3500.0


def test_canonical_revenue_excludes_other_years(temp_data_dir):
    """Orders dated outside 2026 (either side of the boundary) are
    excluded — the disease being treated is YTD bleeding prior
    years."""
    from src.core.order_dal import canonical_year_revenue_total
    with _conn() as c:
        _wipe(c)
        # 2025: excluded
        _seed_order(c, order_id="ord-2025", total=99999.0,
                    created_at="2025-12-31T23:59:59")
        # 2026: included
        _seed_order(c, order_id="ord-2026", total=100.0,
                    created_at="2026-06-15T12:00:00")
        # 2027: excluded
        _seed_order(c, order_id="ord-2027", total=99999.0,
                    created_at="2027-01-01T00:00:01")
        c.commit()
    assert canonical_year_revenue_total(2026) == 100.0


def test_canonical_revenue_excludes_test_rows(temp_data_dir):
    """is_test=1 orders are not real revenue."""
    from src.core.order_dal import canonical_year_revenue_total
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ord-real", total=500.0,
                    created_at="2026-04-01T00:00:00", is_test=0)
        _seed_order(c, order_id="ord-test", total=99999.0,
                    created_at="2026-04-01T00:00:00", is_test=1)
        c.commit()
    assert canonical_year_revenue_total(2026) == 500.0


def test_canonical_revenue_unions_revenue_log(temp_data_dir):
    """The view is `orders UNION ALL revenue_log` — both sources are
    summed. (Pre-canonical, sales_intel.update_revenue_tracker took
    max() rather than sum, masking double-writes; canonical view
    delegates the dedup question to the writers, not the readers.)"""
    from src.core.order_dal import canonical_year_revenue_total
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ord-1", total=300.0,
                    created_at="2026-02-01T00:00:00")
        _seed_revenue_log(c, log_id="rev-1", amount=200.0,
                          logged_at="2026-02-01T00:00:00")
        c.commit()
    # Sum of both sources, not max.
    assert canonical_year_revenue_total(2026) == 500.0


def test_canonical_revenue_revenue_log_year_filter(temp_data_dir):
    """revenue_log rows outside 2026 are excluded just like orders."""
    from src.core.order_dal import canonical_year_revenue_total
    with _conn() as c:
        _wipe(c)
        # Old fiscal-year row (the buggy 2025-07-01 cutoff would include this).
        _seed_revenue_log(c, log_id="rev-2025",
                          amount=99999.0,
                          logged_at="2025-07-15T00:00:00")
        # In-year row.
        _seed_revenue_log(c, log_id="rev-2026", amount=100.0,
                          logged_at="2026-06-15T00:00:00")
        c.commit()
    assert canonical_year_revenue_total(2026) == 100.0


def test_canonical_revenue_unsupported_year_returns_zero(temp_data_dir):
    """Only v_revenue_year_2026 exists today. Asking for 2027 returns
    0 + a warning log (not an exception) so callers degrade gracefully
    until the new-year migration ships."""
    from src.core.order_dal import canonical_year_revenue_total
    assert canonical_year_revenue_total(2027) == 0.0
    assert canonical_year_revenue_total(2024) == 0.0


# ─── Migration agreement: all three callers see the same number ─────────


def test_get_revenue_ytd_uses_canonical_for_headline(temp_data_dir):
    """`get_revenue_ytd().ytd.revenue` must equal canonical_year_revenue_total."""
    from src.core.order_dal import canonical_year_revenue_total, get_revenue_ytd
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ord-a", total=1234.56,
                    created_at="2026-05-01T00:00:00", status="paid")
        _seed_order(c, order_id="ord-b", total=765.44,
                    created_at="2026-06-01T00:00:00", status="invoiced")
        c.commit()
    canonical = canonical_year_revenue_total(2026)
    out = get_revenue_ytd()
    assert out["ok"] is True
    assert out["ytd"]["revenue"] == round(canonical, 2)
    # PR-6 (#696): the dual-emit `revenue_legacy` field was removed
    # once canonical numbers settled.
    assert "revenue_legacy" not in out["ytd"]


def test_get_goal_progress_uses_canonical_ytd(temp_data_dir):
    """get_goal_progress.ytd_revenue must equal canonical helper."""
    from src.core.order_dal import canonical_year_revenue_total
    from src.agents.revenue_engine import get_goal_progress
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ord-x", total=5000.0,
                    created_at="2026-04-15T00:00:00")
        c.commit()
    canonical = canonical_year_revenue_total(2026)
    out = get_goal_progress()
    assert out["ok"] is True
    assert out["ytd_revenue"] == round(canonical, 2)
    # PR-6 (#696): the dual-emit `ytd_revenue_legacy` field was removed
    # once canonical numbers settled.
    assert "ytd_revenue_legacy" not in out


def test_update_revenue_tracker_closed_revenue_is_canonical(temp_data_dir):
    """The home-page closed_revenue field must be the canonical sum,
    not the legacy max() over unfiltered sources.

    Regression guard for the prod symptom Mike reported 2026-05-02:
    home page rendered $1,816,696 revenue YTD because update_revenue_tracker
    took max(db_revenue, orders_revenue, ...) and orders_revenue had
    no year filter — so the max was lifetime orders, not 2026 orders.
    """
    from src.core.order_dal import canonical_year_revenue_total
    from src.agents.sales_intel import update_revenue_tracker
    with _conn() as c:
        _wipe(c)
        # An old all-time order that the legacy max() would surface
        # as "YTD revenue" — the disease this PR cures.
        _seed_order(c, order_id="ord-old", total=1_000_000.0,
                    created_at="2024-01-15T00:00:00",
                    po_number="0000040001",
                    quote_number="R24Q001")
        # The actual in-year revenue.
        _seed_order(c, order_id="ord-new", total=12_345.0,
                    created_at="2026-04-15T00:00:00",
                    po_number="0000050001",
                    quote_number="R26Q001")
        c.commit()
    canonical = canonical_year_revenue_total(2026)
    # Sanity: the canonical helper saw only the 2026 order.
    assert canonical == 12_345.0

    out = update_revenue_tracker()
    assert out["ok"] is True
    assert out["closed_revenue"] == round(canonical, 2)
    # PR-6 (#696): the dual-emit `closed_revenue_legacy` field was removed
    # once canonical numbers settled.
    assert "closed_revenue_legacy" not in out


# ─── PR-6 (#696): dual-emit machinery removed ────────────────────────────
#
# Earlier in this arc we kept a legacy reading alongside the canonical
# one and logged the diff (Scientist pattern). Once the canonical numbers
# stabilized on prod, the legacy paths became dead weight — they could
# only ever be wrong relative to the new source of truth. PR-6 deletes
# them. The historical regression remains covered by
# `test_update_revenue_tracker_closed_revenue_is_canonical` above.
