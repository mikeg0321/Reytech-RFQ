"""ISSUE-32 regression: cost-aware unit-mismatch guard for "% above SCPRS" alerts.

An item whose COST already exceeds ~3x the SCPRS line price is a pack-vs-each
unit mismatch (SCPRS is per-EACH; our quote is a PACK/CASE), NOT overpricing.
The raw price/SCPRS ratio ("2081% above SCPRS $16") is a false alarm that would
push the operator to slash a correctly-priced bundle. All three "% above SCPRS"
builders must consult the shared predicate.
"""
from pathlib import Path

import pytest

from src.core.pricing_math import unit_mismatch_vs_scprs


@pytest.mark.parametrize(
    "cost,scprs,expected",
    [
        (317.26, 16.00, True),   # the confirmed live case
        (100.0, 16.0, True),     # 6.25x
        (49.0, 16.0, True),      # ~3.06x -> just over
        (48.0, 16.0, False),     # exactly 3x -> not over (strict >)
        (20.0, 16.0, False),     # normal markup, real signal
        (16.0, 16.0, False),
        (0.0, 16.0, False),      # no cost known -> can't claim mismatch
        (317.26, 0.0, False),    # no scprs -> nothing to compare
        (None, None, False),
        ("bad", 16.0, False),    # non-numeric -> safe False
    ],
)
def test_unit_mismatch_predicate(cost, scprs, expected):
    assert unit_mismatch_vs_scprs(cost, scprs) is expected


def test_factor_is_tunable():
    assert unit_mismatch_vs_scprs(50, 16, factor=4.0) is False  # 3.1x < 4x
    assert unit_mismatch_vs_scprs(70, 16, factor=4.0) is True   # 4.4x > 4x


def test_all_three_alert_sites_use_shared_guard():
    """Anti-drift: every '% above SCPRS' builder routes through the shared
    predicate so they cannot diverge again."""
    for rel in (
        "src/api/modules/routes_growth_intel.py",
        "src/api/modules/routes_rfq_admin.py",
        "src/api/modules/routes_rfq.py",
    ):
        src = Path(rel).read_text(encoding="utf-8")
        assert "unit_mismatch_vs_scprs" in src, f"{rel}: missing shared guard import/call"
