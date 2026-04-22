"""BUILD-10 P1 regression guard — is_test leak in orders + revenue_log.

Prior to BUILD-10, the `orders` and `revenue_log` tables had no `is_test`
column, so every test quote that converted to an order (either manually
or via the auto-create path in `quote_lifecycle._auto_create_order`) got
counted into headline BI revenue. The two analytics aggregates at
`routes_analytics.py:4188` (SUM orders.total) and `:4196`
(SUM revenue_log.amount) had no way to filter test rows out, so
`won_revenue_orders` and `revenue_logged` were both inflated whenever the
test sandbox ran against the prod DB or a seeded test quote slipped the
net.

This test locks the schema + source-level guards:

  1. `orders` and `revenue_log` tables gain `is_test INTEGER DEFAULT 0`
     (verified via ALTER TABLE migration list in `src/core/db.py`).
  2. `routes_analytics.py:4188/4196` aggregates filter `AND is_test=0`.
  3. All INSERT sites in `dal.py`, `order_dal.py`, `quote_lifecycle.py`,
     `revenue_engine.py` explicitly write an `is_test` value — either
     mirrored from the linked quote, or hard-coded 0 when the source
     query already filters test rows out.
"""
from __future__ import annotations

import re
from pathlib import Path


def _read(p: str) -> str:
    return Path(p).read_text(encoding="utf-8")


# ─── Schema + migration guards ─────────────────────────────────────────

def test_db_schema_declares_is_test_on_orders_and_revenue_log():
    """CREATE TABLE definitions must declare is_test for fresh installs."""
    src = _read("src/core/db.py")

    # orders table
    orders_match = re.search(
        r"CREATE TABLE IF NOT EXISTS orders\s*\((.*?)\);",
        src, re.DOTALL,
    )
    assert orders_match, "orders CREATE TABLE not found"
    assert "is_test" in orders_match.group(1), (
        "BUILD-10: orders CREATE TABLE must declare is_test column"
    )

    # revenue_log table
    rl_match = re.search(
        r"CREATE TABLE IF NOT EXISTS revenue_log\s*\((.*?)\);",
        src, re.DOTALL,
    )
    assert rl_match, "revenue_log CREATE TABLE not found"
    assert "is_test" in rl_match.group(1), (
        "BUILD-10: revenue_log CREATE TABLE must declare is_test column"
    )


def test_migration_list_adds_is_test_to_both_tables():
    """The ALTER TABLE migration list must add is_test to both tables so
    existing prod DBs get the column on next boot."""
    src = _read("src/core/db.py")
    # Look for the tuple entries in the migrations list
    assert re.search(
        r'\("orders",\s*"is_test",\s*"INTEGER DEFAULT 0"\)',
        src,
    ), "BUILD-10: migration list must add orders.is_test"
    assert re.search(
        r'\("revenue_log",\s*"is_test",\s*"INTEGER DEFAULT 0"\)',
        src,
    ), "BUILD-10: migration list must add revenue_log.is_test"


# ─── Analytics aggregate filter guards ─────────────────────────────────

def test_bi_aggregates_filter_is_test():
    """routes_analytics._build_bi_data's two revenue SUMs must filter
    is_test=0 — without this, test orders/revenue_log rows inflate the
    headline won_revenue metric on /analytics."""
    src = _read("src/api/modules/routes_analytics.py")

    # SUM(total) FROM orders — must contain is_test=0
    orders_sum = re.search(
        r'"SELECT COALESCE\(SUM\(total\),0\) FROM orders WHERE([^"]*)"',
        src,
    )
    assert orders_sum, "BUILD-10: orders revenue aggregate not found"
    assert "is_test=0" in orders_sum.group(1), (
        "BUILD-10: orders SUM aggregate must filter is_test=0 — "
        "test orders would otherwise inflate won_revenue_orders"
    )

    # SUM(amount) FROM revenue_log — must contain is_test=0
    rl_sum = re.search(
        r'"SELECT COALESCE\(SUM\(amount\),0\) FROM revenue_log WHERE([^"]*)"',
        src,
    )
    assert rl_sum, "BUILD-10: revenue_log aggregate not found"
    assert "is_test=0" in rl_sum.group(1), (
        "BUILD-10: revenue_log SUM aggregate must filter is_test=0"
    )


# ─── INSERT site guards ────────────────────────────────────────────────

def test_dal_save_order_writes_is_test():
    """src/core/dal.py save_order INSERT must include is_test column and
    resolve the value from the linked quote when not passed directly."""
    src = _read("src/core/dal.py")
    # Find the INSERT INTO orders statement
    insert_match = re.search(
        r"INSERT INTO orders \([^)]*\)\s*VALUES[^)]*\)",
        src, re.DOTALL,
    )
    assert insert_match, "BUILD-10: orders INSERT in dal.py not found"
    assert "is_test" in insert_match.group(0), (
        "BUILD-10: dal.save_order INSERT must include is_test column"
    )
    # Must resolve from quote row when not present on the input dict
    assert re.search(
        r"SELECT is_test FROM quotes WHERE quote_number",
        src,
    ), (
        "BUILD-10: dal.save_order must resolve is_test from the linked "
        "quote row when the input dict doesn't carry the flag"
    )


def test_order_dal_save_order_writes_is_test():
    """src/core/order_dal.py save_order INSERT must include is_test."""
    src = _read("src/core/order_dal.py")
    # order_dal's INSERT covers many more columns than dal.py's — match
    # with DOTALL and check for is_test in the column list.
    insert_match = re.search(
        r"INSERT INTO orders\s*\(([^)]*)\)",
        src, re.DOTALL,
    )
    assert insert_match, "BUILD-10: orders INSERT in order_dal.py not found"
    assert "is_test" in insert_match.group(1), (
        "BUILD-10: order_dal.save_order INSERT must include is_test column"
    )
    assert re.search(
        r"SELECT is_test FROM quotes WHERE quote_number",
        src,
    ), (
        "BUILD-10: order_dal.save_order must resolve is_test from the "
        "linked quote row"
    )


def test_quote_lifecycle_auto_create_order_propagates_is_test():
    """_auto_create_order in quote_lifecycle.py must SELECT is_test from
    the source quote and write it into BOTH the derived order row AND
    the derived revenue_log row."""
    src = _read("src/agents/quote_lifecycle.py")
    # Source SELECT must include is_test
    sel_match = re.search(
        r"SELECT agency, institution, total[^;]*?FROM quotes WHERE quote_number",
        src, re.DOTALL,
    )
    assert sel_match, "BUILD-10: quote_lifecycle SELECT from quotes not found"
    assert "is_test" in sel_match.group(0), (
        "BUILD-10: quote_lifecycle _auto_create_order must SELECT is_test "
        "from the source quote so derived rows inherit the flag"
    )
    # Both INSERTs must carry is_test in the column list
    orders_ins = re.search(
        r"INSERT OR IGNORE INTO orders\s*\(([^)]*)\)",
        src, re.DOTALL,
    )
    assert orders_ins and "is_test" in orders_ins.group(1), (
        "BUILD-10: quote_lifecycle orders INSERT must include is_test"
    )
    revlog_ins = re.search(
        r"INSERT INTO revenue_log\s*\(([^)]*)\)",
        src, re.DOTALL,
    )
    assert revlog_ins and "is_test" in revlog_ins.group(1), (
        "BUILD-10: quote_lifecycle revenue_log INSERT must include is_test"
    )


def test_revenue_engine_filters_and_propagates_is_test():
    """revenue_engine.sync_revenue must (a) filter is_test=0 when reading
    orders, and (b) include is_test in BOTH revenue_log INSERTs."""
    src = _read("src/agents/revenue_engine.py")
    # Orders SELECT must filter is_test
    assert re.search(
        r"FROM orders\s+WHERE\s+total\s*>\s*0\s+AND\s+is_test\s*=\s*0",
        src, re.DOTALL,
    ), (
        "BUILD-10: revenue_engine orders SELECT must filter is_test=0 so "
        "test orders don't create revenue_log rows"
    )
    # Count INSERT INTO revenue_log statements — must be 3 (source 1, 2, 3)
    # and the first 2 must name is_test in the column list.
    inserts = re.findall(
        r"INSERT INTO revenue_log\s*\(([^)]*)\)",
        src, re.DOTALL,
    )
    assert len(inserts) >= 2, (
        f"BUILD-10: revenue_engine expected >=2 revenue_log INSERTs, "
        f"found {len(inserts)}"
    )
    # Sources 1 + 2 (order-based + manual-win) must propagate is_test
    assert "is_test" in inserts[0], (
        "BUILD-10: revenue_engine source-1 (orders) revenue_log INSERT "
        "must propagate is_test from the order row"
    )
    assert "is_test" in inserts[1], (
        "BUILD-10: revenue_engine source-2 (won quotes) revenue_log "
        "INSERT must explicitly set is_test (source query filters "
        "is_test=0, so the value is always 0 here — but locking it in "
        "the column list protects against future reorders)"
    )
