"""PR-AV6 (AV-10) — identical-row pricing guard.

Closes the failure class observed on rfq_9e63456e (PREQ 10847262)
where retry-auto-price stamped `scprs_last_price = $12,406.11`
across all 7 items — a SCPRS line-total misuse. Downstream
`recommended_price = cost * 1.25` amplified that bogus value into
quoted prices.

Tests pin:
  1. detect_uniform_suspicious_pricing on the actual rfq_9e63456e
     pattern (7 items, all scprs_last_price=$12406.11)
  2. Below-min-items batches don't fire (1-item and 2-item batches
     can legitimately share prices)
  3. Mixed values don't fire (a real diversified quote)
  4. Tolerance is tight enough to distinguish $100 from $100.05 but
     loose enough to ignore floating-point rounding (0.011 = 1.1¢)
  5. revert_*: cleared values replace the suspicious field with
     None — operator-typed cost fields untouched
  6. Empty / malformed / mostly-zero inputs are safe
"""
from __future__ import annotations

import pytest

from src.core.pricing_math import (
    detect_uniform_suspicious_pricing,
    revert_uniform_suspicious_pricing,
)


# ───────────────────────── detector tests ──────────────────────────


def test_rfq_9e63456e_actual_failure_pattern():
    """The exact bug from today: 7 items, all SCPRS = $12,406.11."""
    items = [
        {"part": "RDT2060AP", "qty": 30,   "scprs_last_price": 12406.11},
        {"part": "7074",      "qty": 50,   "scprs_last_price": 12406.11},
        {"part": "7075",      "qty": 50,   "scprs_last_price": 12406.11},
        {"part": "7076",      "qty": 25,   "scprs_last_price": 12406.11},
        {"part": "6121W",     "qty": 15,   "scprs_last_price": 12406.11},
        {"part": "2555",      "qty": 1000, "scprs_last_price": 12406.11},
        {"part": "224BWPX",   "qty": 6,    "scprs_last_price": 12406.11},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is True
    assert r["count"] == 7
    assert r["common_value"] == 12406.11
    assert r["indices"] == [0, 1, 2, 3, 4, 5, 6]


def test_diversified_real_quote_does_not_fire():
    """A real quote with diverse SCPRS prices is NOT flagged."""
    items = [
        {"scprs_last_price": 45.99},
        {"scprs_last_price": 89.50},
        {"scprs_last_price": 145.00},
        {"scprs_last_price": 22.10},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False


def test_single_item_batch_does_not_fire():
    items = [{"scprs_last_price": 100.00}]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False


def test_two_item_batch_does_not_fire():
    """Two products legitimately share prices sometimes (size-variant
    SKUs). Below the 3-item floor → don't fire."""
    items = [
        {"scprs_last_price": 50.00},
        {"scprs_last_price": 50.00},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False


def test_three_item_batch_fires():
    """3 items with identical prices crosses the threshold."""
    items = [
        {"scprs_last_price": 50.00},
        {"scprs_last_price": 50.00},
        {"scprs_last_price": 50.00},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is True
    assert r["count"] == 3


def test_partial_cluster_does_not_fire():
    """If only SOME items share a price, don't fire — the bug is
    'ALL non-zero values identical'. A real quote where 2 items
    share an SCPRS value but 3 others differ should NOT be flagged."""
    items = [
        {"scprs_last_price": 100.00},
        {"scprs_last_price": 100.00},
        {"scprs_last_price": 100.00},
        {"scprs_last_price": 75.50},     # differs
        {"scprs_last_price": 22.30},     # differs
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False


def test_floating_point_noise_within_tolerance():
    """3 items priced as 100.001 / 100.005 / 100.000 (sub-cent
    noise) should still classify as identical — tolerance 1.1¢."""
    items = [
        {"scprs_last_price": 100.001},
        {"scprs_last_price": 100.005},
        {"scprs_last_price": 100.000},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is True


def test_cent_difference_breaks_uniformity():
    """A $0.05 difference is enough to NOT cluster (real product
    pricing easily varies by cents)."""
    items = [
        {"scprs_last_price": 100.00},
        {"scprs_last_price": 100.05},
        {"scprs_last_price": 100.10},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False


def test_zero_and_missing_values_skipped():
    """Items with missing or zero values in the field don't count
    toward the cluster. 3 zero-priced items + 2 real diverse should
    NOT fire."""
    items = [
        {"scprs_last_price": 0},
        {"scprs_last_price": None},
        {},
        {"scprs_last_price": 45.99},
        {"scprs_last_price": 89.50},
    ]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False


def test_empty_input_safe():
    assert detect_uniform_suspicious_pricing([], "scprs_last_price")["suspicious"] is False
    assert detect_uniform_suspicious_pricing(None, "scprs_last_price")["suspicious"] is False


def test_malformed_item_safe():
    """Non-dict entries are skipped gracefully."""
    items = [None, "garbage", {"scprs_last_price": 100}, {"scprs_last_price": 100},
             {"scprs_last_price": 100}]
    r = detect_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is True
    assert r["count"] == 3


def test_arbitrary_field_supported():
    """The guard works on any pricing-shaped field — amazon_price,
    recommended_price, unit_cost — not just scprs_last_price."""
    items = [
        {"amazon_price": 99.99},
        {"amazon_price": 99.99},
        {"amazon_price": 99.99},
    ]
    r = detect_uniform_suspicious_pricing(items, "amazon_price")
    assert r["suspicious"] is True


# ───────────────────────── revert tests ────────────────────────────


def test_revert_clears_suspicious_field_only():
    """When suspicious is True, the field gets cleared to None on
    EVERY matching item — but other cost fields are untouched."""
    items = [
        {"part": "A", "scprs_last_price": 12406.11, "unit_cost": 45.99},
        {"part": "B", "scprs_last_price": 12406.11, "unit_cost": 90.00},
        {"part": "C", "scprs_last_price": 12406.11, "unit_cost": 130.00},
    ]
    r = revert_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is True
    # scprs_last_price cleared
    for it in items:
        assert it["scprs_last_price"] is None
    # unit_cost untouched
    assert items[0]["unit_cost"] == 45.99
    assert items[1]["unit_cost"] == 90.00
    assert items[2]["unit_cost"] == 130.00


def test_revert_no_op_when_not_suspicious():
    """A real quote stays unchanged when revert runs."""
    items = [
        {"scprs_last_price": 45.99},
        {"scprs_last_price": 89.50},
        {"scprs_last_price": 145.00},
    ]
    snapshot = [dict(it) for it in items]
    r = revert_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["suspicious"] is False
    for original, current in zip(snapshot, items):
        assert original == current


def test_revert_returns_detector_result():
    """revert returns the same shape detect returns so callers can
    log + report."""
    items = [{"scprs_last_price": 100} for _ in range(4)]
    r = revert_uniform_suspicious_pricing(items, "scprs_last_price")
    assert r["count"] == 4
    assert r["common_value"] == 100
    assert r["indices"] == [0, 1, 2, 3]


# ───────────────────────── integration ─────────────────────────────


def test_route_imports_helper():
    """Smoke: the new helper is reachable from src.core.pricing_math
    where the route imports it. Catches accidental renames /
    relocations that would silently disable the guard."""
    from src.core.pricing_math import (
        detect_uniform_suspicious_pricing,
        revert_uniform_suspicious_pricing,
    )
    assert callable(detect_uniform_suspicious_pricing)
    assert callable(revert_uniform_suspicious_pricing)


def test_route_wires_guard_pinned():
    """Verify the RFQ retry-auto-price route source contains the
    AV-10 wiring. A future refactor that removes it fails this."""
    with open("src/api/modules/routes_analytics.py", encoding="utf-8") as f:
        src = f.read()
    assert "AV-10" in src
    assert "revert_uniform_suspicious_pricing" in src
    assert "scprs_last_price" in src
    # The post-compute guard runs after step 3 in the route
    assert "recommended_price" in src
