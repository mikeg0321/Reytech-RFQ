"""Regression: profit-floor blocker is suppressed when any active item
lacks a sell price.

Incident 2026-05-01 (Mike's screenshot): a PC with one $25,500 item that
had cost set but no sell price showed `Total profit $-25500.00 is below
$75.00 floor`. The math came from the fallback path in `_check_profit`:
when the per-item summary doesn't exist, it iterates items and computes
`(price - cost) * qty`. With `price = 0` and `cost = 25500`, that yields
a fictional $-25500 "loss" on top of the (correct) "Cost exists but no
sell price set" blocker.

The fix: gate the profit-floor check on having all active items priced.
The per-item + aggregate "no price" blockers already cover the actionable
state; the floor message only makes sense once the operator has set
prices on every active line.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agents.pc_qa_agent import _check_profit, BLOCKER, CAT_PROFIT


def _profit_blockers(issues):
    return [i for i in issues if i.get("category") == CAT_PROFIT]


def test_no_profit_blocker_when_unpriced_item_has_cost():
    """The exact shape from Mike's screenshot: 1 active item with
    unit_cost=25500 and no sell price. Profit floor MUST stay quiet."""
    pc = {}
    items = [{
        "qty": 1,
        "pricing": {"unit_cost": 25500.0},  # cost set, no price
    }]
    issues = _check_profit(pc, items)
    assert _profit_blockers(issues) == [], (
        "profit-floor blocker fired against unpriced item — should be suppressed "
        "until the operator sets the sell price"
    )


def test_no_profit_blocker_when_partially_priced():
    """Mixed bag: one item priced & profitable, one item with cost but no
    price. Until both are priced, the aggregate profit number is
    meaningless — suppress the floor blocker."""
    pc = {}
    items = [
        # `bid_price` lives at item level (PC: unit_price; RFQ: bid_price)
        {"qty": 5, "bid_price": 50.0, "pricing": {"unit_cost": 10.0}},
        {"qty": 1, "pricing": {"unit_cost": 25500.0}},  # unpriced
    ]
    issues = _check_profit(pc, items)
    assert _profit_blockers(issues) == [], (
        "profit-floor must stay quiet while any active item lacks a price"
    )


def test_profit_blocker_fires_when_fully_priced_and_below_floor():
    """Once every active item has a price, the floor check resumes —
    locking in that this fix doesn't disable the check entirely."""
    pc = {}
    items = [{
        "qty": 1,
        "bid_price": 110.0,
        "pricing": {"unit_cost": 100.0},
    }]
    # gross_profit = (110 - 100) * 1 = $10  → below the $75 floor
    issues = _check_profit(pc, items)
    blockers = _profit_blockers(issues)
    assert len(blockers) == 1
    assert blockers[0]["severity"] == BLOCKER
    assert "below $75.00 floor" in blockers[0]["message"]


def test_profit_blocker_silent_when_fully_priced_and_above_floor():
    pc = {}
    items = [{
        "qty": 10,
        "bid_price": 80.0,
        "pricing": {"unit_cost": 50.0},
    }]
    # gross_profit = (80 - 50) * 10 = $300  → above the $75 floor
    issues = _check_profit(pc, items)
    assert _profit_blockers(issues) == [], (
        "above-floor profit must not trigger any blocker"
    )


def test_no_bid_lines_dont_count_as_unpriced():
    """A line marked no_bid is intentionally dropped from the quote and
    must not block the profit floor on the rest of the items."""
    pc = {}
    items = [
        {"qty": 1, "pricing": {"unit_cost": 999.0}, "no_bid": True},  # excluded
        {"qty": 10, "bid_price": 80.0, "pricing": {"unit_cost": 50.0}},  # priced
    ]
    issues = _check_profit(pc, items)
    # No-bid line is skipped both by the unpriced gate and the profit calc
    # → only the priced line drives the floor check, which passes ($300).
    assert _profit_blockers(issues) == []


def test_negative_profit_value_not_surfaced_via_unpriced_path():
    """Belt-and-suspenders: even if some downstream calc accidentally
    feeds a profit_summary with a fake negative profit, the unpriced gate
    should still suppress the blocker. This proves the gate runs BEFORE
    we trust profit_summary values."""
    pc = {"profit_summary": {"gross_profit": -25500.0}}
    items = [{
        "qty": 1,
        "pricing": {"unit_cost": 25500.0},  # unpriced
    }]
    issues = _check_profit(pc, items)
    assert _profit_blockers(issues) == [], (
        "unpriced gate must run before reading profit_summary so a "
        "stale/derived negative number can't leak through"
    )
