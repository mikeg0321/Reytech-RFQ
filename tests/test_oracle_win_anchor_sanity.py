"""Regression guard for the win-history anchor contamination.

Follow-up to the 2026-05-29 cross-category incident. After that fix, repricing
rfq_fca653f6 through the (now-fixed) oracle STILL returned a bad bid for L7:
an $8.99 "JOURNAL, TEEN GRATITUDE" came back at $61.91 (589% markup). Root
cause was a SECOND contamination vector — the win-history anchor in
`_calculate_recommendation` returns the remembered win price directly and
bypasses the MARKET_CEILING_SANITY backstop (which only guards the has_market
branch). A win price 7x the current cost is a mis-matched item identity in
item-memory, not a real signal. WIN_ANCHOR_SANITY_MAX_MARKUP_PCT now rejects it.
"""
from __future__ import annotations

from src.core import pricing_oracle_v2


def test_win_anchor_rejected_when_markup_absurd():
    """A remembered win 7x the current cost must NOT anchor the quote."""
    win_history = {"last_sell_price": 61.91, "times_confirmed": 5}
    rec = pricing_oracle_v2._calculate_recommendation(
        cost=8.99, market={"data_points": 0}, quantity=4,
        category="office", agency="CCHCS", _db=None,
        win_history=win_history,
    )
    qp = rec.get("quote_price")
    assert qp is not None
    # Must fall through to cost-plus, NOT the $61.91 win.
    assert qp < 20.0, f"win anchor should have been rejected, got ${qp}"
    assert "win_anchor_rejected" in rec
    assert rec.get("win_anchor") is None  # key is initialized to None; never populated
    assert rec["win_anchor_rejected"]["price"] == 61.91


def test_sane_win_anchor_still_honored():
    """A normal win price (within the sanity bound) must still anchor."""
    win_history = {"last_sell_price": 11.50, "times_confirmed": 5}  # ~28% over cost
    rec = pricing_oracle_v2._calculate_recommendation(
        cost=8.99, market={"data_points": 0}, quantity=4,
        category="office", agency="CCHCS", _db=None,
        win_history=win_history,
    )
    qp = rec.get("quote_price")
    assert qp == 11.50, f"sane win anchor should be honored, got ${qp}"
    assert rec.get("win_anchor", {}).get("price") == 11.50
    assert "win_anchor_rejected" not in rec
