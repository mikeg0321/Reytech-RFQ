"""CC-3 regression guard: CCHCS packet gate must fail closed when the
packet would total $0.

Audited 2026-04-22. Original `_check_line_item_pricing` `continue`d past
qty=0 rows without counting them, so a packet where every row had qty=0
(or an empty line_items list) produced zero issues -> `passed=True`
-> ok=True downstream, even though the delivered PDF totals $0.

Three scenarios the gate MUST now block:
  1. parsed.line_items is empty / missing
  2. every line item has qty=0 (nothing to price)
  3. at least one row has qty>0 but none ended up with a unit_price
"""
from __future__ import annotations

from src.forms.cchcs_packet_gate import _check_line_item_pricing


def test_empty_line_items_fails_closed():
    """Empty line_items list must yield a blocking issue, not silent pass."""
    res = _check_line_item_pricing({"line_items": []}, None)
    assert res["issues"], (
        "CC-3 regression: empty line_items must produce an issue. The gate "
        "is silently passing zero-row packets."
    )
    assert any("CC-3 fail-closed" in i for i in res["issues"])


def test_missing_line_items_key_fails_closed():
    """Parsed dict without a line_items key must also fail closed."""
    res = _check_line_item_pricing({}, None)
    assert res["issues"]
    assert any("CC-3 fail-closed" in i for i in res["issues"])


def test_all_qty_zero_fails_closed():
    """If every row has qty=0, packet totals $0 — gate must block."""
    parsed = {"line_items": [
        {"row_index": 1, "qty": 0, "unit_price": 0},
        {"row_index": 2, "qty": 0, "unit_price": 0},
        {"row_index": 3, "qty": 0, "unit_price": 0},
    ]}
    res = _check_line_item_pricing(parsed, None)
    assert res["issues"], (
        "CC-3 regression: all-qty-zero packet must produce an issue. The "
        "gate's `if qty <= 0: continue` used to silently skip every row."
    )
    assert any("all 3 line items have qty=0" in i for i in res["issues"])
    assert any("CC-3 fail-closed" in i for i in res["issues"])


def test_qty_positive_but_no_price_fails_closed():
    """Rows with qty>0 but no price must each emit their own per-row issue
    (this was already covered) PLUS a summary fail-closed issue."""
    parsed = {"line_items": [
        {"row_index": 1, "qty": 5, "unit_price": 0},
        {"row_index": 2, "qty": 3, "unit_price": 0},
    ]}
    res = _check_line_item_pricing(parsed, None)
    # per-row issues for each unpriced row
    assert any("row 1: no price set" in i for i in res["issues"])
    assert any("row 2: no price set" in i for i in res["issues"])
    # plus the aggregate fail-closed summary
    assert any(
        "0 of 2 line items with qty>0 received a unit price" in i
        for i in res["issues"]
    )


def test_valid_priced_rows_still_pass():
    """Sanity: real packets with priced rows must NOT hit the fail-closed
    guard — we only block $0 totals."""
    parsed = {"line_items": [
        {"row_index": 1, "qty": 5, "unit_price": 12.50,
         "pricing": {"unit_cost": 10.00}},
        {"row_index": 2, "qty": 3, "unit_price": 22.00,
         "pricing": {"unit_cost": 18.00}},
    ]}
    res = _check_line_item_pricing(parsed, None)
    # no issues on healthy packet
    assert not res["issues"], (
        f"Unexpected issues on a valid priced packet: {res['issues']}"
    )


def test_mixed_qty_some_priced_passes():
    """A packet with a mix of qty=0 skip rows and properly priced rows
    must pass (qty=0 rows are buyer line-deletions, expected)."""
    parsed = {"line_items": [
        {"row_index": 1, "qty": 0, "unit_price": 0},  # buyer skipped
        {"row_index": 2, "qty": 10, "unit_price": 5.50,
         "pricing": {"unit_cost": 4.00}},
    ]}
    res = _check_line_item_pricing(parsed, None)
    assert not res["issues"], (
        f"Mixed-qty packet with 1 priced row must pass: {res['issues']}"
    )
