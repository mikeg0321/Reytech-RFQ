"""IN-2 / IN-11 regression guard: SCPRS per-unit normalization.

Prior to 2026-04-21 _search_won_quotes / _search_scprs_catalog / _search_po_lines
divided price by qty unconditionally whenever qty > 1. That corrupts rows
where the stored unit_price was ALREADY per-unit (e.g., qty=3 × $5 ea would
become $1.67). The sibling _search_winning_prices already carried the right
guard (`p > qty * 2`); this test locks that guard into a shared helper used
by every search site.
"""
from __future__ import annotations

import pytest

from src.core.pricing_oracle_v2 import _scprs_per_unit


@pytest.mark.parametrize(
    "price, qty, expected",
    [
        # obvious line totals: big price, small qty → divide
        (100.0, 5, 20.0),      # 5 units × $20 ea stored as $100
        (240.0, 12, 20.0),     # 12 × $20 ea
        (50.0, 10, 5.0),       # 10 × $5 ea
        # already per-unit: p <= qty * 2 → leave alone
        (5.0, 3, 5.0),         # 3 × $5 ea stored per-unit — DO NOT divide
        (1.50, 100, 1.50),     # 100 × $1.50 ea — DO NOT divide
        (2.0, 1, 2.0),         # qty=1 trivial
        # edge: p == qty*2 → leave alone (guard is strict >)
        (10.0, 5, 10.0),
        # zero / missing
        (0, 10, 0),
        (None, 5, None),
    ],
)
def test_scprs_per_unit(price, qty, expected):
    assert _scprs_per_unit(price, qty) == expected


def test_scprs_per_unit_bad_inputs_return_original():
    # Non-numeric qty should not crash — return original price
    assert _scprs_per_unit(100, "not-a-number") == 100


def test_scprs_per_unit_qty_zero_treated_as_one():
    # qty=0 shouldn't divide by zero; treat as qty=1 (no normalization)
    assert _scprs_per_unit(50.0, 0) == 50.0
