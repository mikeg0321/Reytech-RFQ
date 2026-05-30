"""Cross-vector cost-sanity invariant for the Pricing Oracle.

The oracle's recommended `quote_price` is set in THREE independent branches —
the market/comp-avg path, the win-history anchor, and the SCPRS-rollup cap.
The 2026-05-29/30 incident fixed them ONE AT A TIME (#1229, #1235, #1237)
because each branch emitted quote_price without the others' guard (see lesson
`feedback_guard_every_sibling_branch`).

This test encodes the unifying invariant for the two branches that live in
`_calculate_recommendation` (market + win-anchor): given a contaminated
signal, a KNOWN cost, and no strong basis, the recommendation must stay
cost-relative sane — never an absurd markup, never (much) below cost. If a
FUTURE branch is added to `_calculate_recommendation` that emits quote_price
without the guard, this fails the build instead of waiting for a 4th incident.

(The third emitter — the SCPRS-rollup cap — has its own never-below-cost
guard in test_scprs_cap_never_below_cost.py.)
"""
from __future__ import annotations

import pytest

from src.core import pricing_oracle_v2 as oracle

# Mirror the module's guard bound so this tracks the constant if it changes.
SANE_MAX = oracle.MARKET_CEILING_SANITY_MAX_MARKUP_PCT


@pytest.mark.parametrize("label,cost,market,win_history", [
    # Market branch: comp_avg falls back to a contaminated weighted_avg
    # (cross-category catalog rows polluted the average).
    ("contaminated_market", 2.00,
     {"data_points": 5, "competitor_avg": None, "weighted_avg": 70.03}, None),
    # Win-anchor branch: a remembered win 7x the current cost — a mis-matched
    # item identity in item-memory, not a real signal.
    ("contaminated_win_anchor", 8.99,
     {"data_points": 0}, {"last_sell_price": 61.91, "times_confirmed": 5}),
])
def test_recommendation_stays_cost_sane(label, cost, market, win_history):
    rec = oracle._calculate_recommendation(
        cost=cost, market=market, quantity=10,
        category="office", agency="CCHCS", _db=None, win_history=win_history,
    )
    qp = rec.get("quote_price")
    assert qp is not None, label
    implied = (qp - cost) / cost * 100
    assert implied <= SANE_MAX, (
        f"{label}: ${qp} = {implied:.0f}% over cost ${cost} (> {SANE_MAX}% bound)")
    assert qp >= cost * 0.9, f"{label}: ${qp} is below cost ${cost} (a loss)"


def test_legitimate_signal_is_not_suppressed():
    """Guard the guards: a normal, in-bounds win is still honored — the
    sanity caps must not produce false positives on real history."""
    rec = oracle._calculate_recommendation(
        cost=8.99, market={"data_points": 0}, quantity=4,
        category="office", agency="CCHCS", _db=None,
        win_history={"last_sell_price": 11.50, "times_confirmed": 5},  # ~28%
    )
    assert rec.get("quote_price") == 11.50
