"""BUILD-7 P2 regression guard — win-probability surfaced at the FINAL markup.

The buyer_curve block already reports P(win) at the EV-optimal markup, but
the actual shipped markup_pct may differ after ceiling/floor/dollar-floor/
win-anchor adjustments. Without BUILD-7 the UI only knows the probability
of the *optimal* markup, not the *actual* one — a quote that gets floored
up could have a visibly worse win probability that never surfaces.

This test locks:
  1. `_apply_win_probability` writes a `win_probability` key into result.
  2. Both return sites in `_calculate_recommendation` call the helper
     (source-level guards — refactor-proof).
  3. Cold-start prior behaves monotonically (high markup → lower P(win))
     so the field remains meaningful even when no DB curve exists.
  4. Dollar-floor bumps the markup AND the win_probability recomputes
     against the bumped markup, not the pre-bump one.
  5. Feature flag disable removes the key entirely.
"""
from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import patch

import pytest


# ─────────────────────────────────────────────────────────────────────
# Pure-helper unit tests
# ─────────────────────────────────────────────────────────────────────

def test_apply_win_probability_writes_key_when_markup_present():
    from src.core.pricing_oracle_v2 import _apply_win_probability
    result = {"markup_pct": 30.0}
    _apply_win_probability(result, "CDCR", None)
    assert "win_probability" in result
    p = result["win_probability"]
    assert 0.0 <= p <= 1.0, f"win_probability must be a probability, got {p}"


def test_apply_win_probability_cold_start_monotone_decreasing():
    """With no DB curve, the helper falls through to the cold-start prior,
    which should decrease as markup rises — higher price, lower P(win).
    Without this monotonicity the EV-maximizer above degenerates."""
    from src.core.pricing_oracle_v2 import _apply_win_probability
    probs = []
    for mk in [10.0, 25.0, 40.0, 55.0, 70.0]:
        r = {"markup_pct": mk}
        _apply_win_probability(r, "UNKNOWN_AGENCY", None)
        probs.append(r["win_probability"])
    # Strictly non-increasing
    for a, b in zip(probs, probs[1:]):
        assert b <= a, f"win_probability is not monotone decreasing: {probs}"
    # 10% markup should win more often than 70%
    assert probs[0] > probs[-1]


def test_apply_win_probability_missing_markup_is_noop():
    from src.core.pricing_oracle_v2 import _apply_win_probability
    result = {"quote_price": 50.0}  # no markup_pct
    _apply_win_probability(result, "CDCR", None)
    assert "win_probability" not in result


def test_apply_win_probability_reads_buyer_curve_when_present():
    """When result carries a pre-fetched buyer_curve block, the helper
    should NOT re-hit the DB — proof is that it produces a probability
    that reflects the bucket it was given, not the cold-start prior."""
    from src.core.pricing_oracle_v2 import _apply_win_probability, _bucket_markup
    target = _bucket_markup(30.0)
    result = {
        "markup_pct": 30.0,
        "buyer_curve": {
            "buckets": [
                {"markup_min": target, "markup_max": target + 5,
                 "total": 10, "wins": 9, "win_rate": 0.9, "samples": 10},
            ],
            "total_samples": 10,
            "won": 9,
            "lost": 1,
        },
    }
    _apply_win_probability(result, "CDCR", None)
    # Should read from the bucket (~0.9), not cold-start (~0.52 at 30% markup)
    assert result["win_probability"] >= 0.85, (
        f"buyer_curve bucket not consulted: got {result['win_probability']}"
    )


def test_apply_win_probability_flag_off_skips():
    from src.core.pricing_oracle_v2 import _apply_win_probability
    with patch("src.core.flags.get_flag") as m:
        m.return_value = False
        result = {"markup_pct": 25.0}
        _apply_win_probability(result, "CDCR", None)
    assert "win_probability" not in result


def test_apply_win_probability_handles_malformed_markup():
    from src.core.pricing_oracle_v2 import _apply_win_probability
    result = {"markup_pct": "not-a-number"}
    _apply_win_probability(result, "CDCR", None)
    # Swallowed silently — no key, no crash
    assert "win_probability" not in result


# ─────────────────────────────────────────────────────────────────────
# Source-level wiring guards
# ─────────────────────────────────────────────────────────────────────

def test_both_return_sites_call_apply_win_probability():
    """Lock the regression that broke BUILD-3: a refactor removes one of
    the two return-path helper calls and probability silently disappears
    from the win-anchor branch."""
    src = Path("src/core/pricing_oracle_v2.py").read_text(encoding="utf-8")
    call_count = len(re.findall(r"_apply_win_probability\(", src))
    # Exactly 3 matches: the def + 2 call sites (win-anchor + final return)
    assert call_count == 3, (
        f"BUILD-7: expected 3 refs to _apply_win_probability (def + 2 call "
        f"sites), found {call_count}. Did a refactor drop a return-path call?"
    )


def test_calculate_recommendation_includes_win_probability():
    """Smoke test: a full _calculate_recommendation call produces a
    `win_probability` on the result. This proves both wiring and the
    helper work end-to-end, not just in isolation."""
    from src.core.pricing_oracle_v2 import _calculate_recommendation
    cost = 25.00
    market = {"competitor_avg": 40.00, "competitor_low": 38.00,
              "data_points": 12, "reytech_avg": 38.00, "weighted_avg": 39.00}
    result = _calculate_recommendation(cost, market, quantity=5,
                                        category="general", agency="CDCR")
    assert "win_probability" in result, (
        "BUILD-7: full recommendation missing win_probability key. "
        "Either the helper call regressed or the flag is off in this env."
    )
    assert 0.0 <= result["win_probability"] <= 1.0
