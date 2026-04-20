"""Regression tests for the Oracle calibration 'Why' classifier.

Background (2026-04-20): Prod oracle_calibration had 47 losses, 0 wins.
The old classifier labelled every row 'Aggressive reduction — too many
losses' whenever win_rate < 40%, which hid the real story: wins weren't
being captured at all. Every quote priced that week was constrained by
a 15% max-markup floor derived from data that was loss-only by design
(only SCPRS-matched losses ever got written).

Tests lock in the honest copy:
  - wins=0, losses=0  → neutral ('No outcome data yet ...')
  - wins=0, losses>0  → flagged ('⚠ Loss-only data — wins not being
                         captured') — NOT 'too many losses'
  - wins>0            → existing Strong / Healthy / Compressing /
                         Aggressive thresholds
"""
from __future__ import annotations

import pytest

from src.agents.oracle_weekly_report import _calibration_why


class TestCalibrationWhyNoData:
    def test_zero_wins_zero_losses_is_neutral(self):
        msg = _calibration_why(wins=0, losses_total=0, win_rate=0)
        assert "No outcome data yet" in msg
        assert "too many losses" not in msg.lower()


class TestCalibrationWhyLossOnlyData:
    """The bug this PR fixes: loss-only data was being narrated as
    'too many losses', as if we'd competed and lost. Real story: we
    have zero way to record wins."""

    def test_zero_wins_many_losses_flags_pipeline_gap(self):
        msg = _calibration_why(wins=0, losses_total=47, win_rate=0)
        assert "wins not being captured" in msg.lower() or "loss-only" in msg.lower()
        assert "too many losses" not in msg.lower(), (
            "Loss-only data must not be labelled 'too many losses' — "
            "it's a signal-capture gap, not a losing streak"
        )

    def test_one_loss_no_wins_still_flags_gap(self):
        msg = _calibration_why(wins=0, losses_total=1, win_rate=0)
        assert "loss-only" in msg.lower() or "not being captured" in msg.lower()


class TestCalibrationWhyWithWins:
    """Existing thresholds unchanged for rows where we have actual wins
    vs. losses to compare. Regression-lock them."""

    @pytest.mark.parametrize("win_rate,expected", [
        (95, "Strong"),
        (80, "Strong"),
        (75, "Healthy"),
        (60, "Healthy"),
        (50, "Compressing"),
        (40, "Compressing"),
        (25, "Aggressive"),
        (10, "Aggressive"),
    ])
    def test_existing_thresholds(self, win_rate, expected):
        # wins and losses both positive so the gap-detector doesn't fire
        msg = _calibration_why(wins=5, losses_total=5, win_rate=win_rate)
        assert expected in msg


class TestCalibrationWhyEdgeCases:
    def test_all_wins_no_losses_still_reads_as_strong(self):
        """If we somehow get a wins-only run (inverse of the bug), it
        shouldn't regress to loss-phrasing — it's just a great week."""
        msg = _calibration_why(wins=10, losses_total=0, win_rate=100)
        assert "Strong" in msg
        assert "too many losses" not in msg.lower()
