"""Regression: qty-band weighting + sample drill-down in
`pricing_oracle_v2._analyze_market_prices`.

Mike's 2026-04-23 incident: a Stanley RoamAlert RFQ at qty=1 was
showing "SCPRS Avg = $113" while our cost was $400. The comp set
included state-buyer rows at qty=200 (volume contracts) that were
weighted equally with single-unit reference rows, dragging the
average down to a number we could never match.

Defenses being tested:

1. **Qty-band downweighting.** When `target_quantity` is provided and
   a row's own quantity differs by more than 10x in either direction,
   weight is multiplied by 0.2 and the row is tagged
   `qty_band_match=False`. The row stays visible (operator can still
   see it as reference) but stops dominating the weighted average.

2. **Sample drill-down.** The aggregator returns a `samples` list
   (top 10 contributing rows by weight) so the UI can show which
   SCPRS rows actually produced the SCPRS Avg. Each sample carries
   po_number, supplier, date, quantity, unit_price, weight, and the
   `qty_band_match` flag.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.core.pricing_oracle_v2 import _analyze_market_prices


def _row(price, quantity=1, source="scprs_po_lines", po_number="",
         supplier="", date="2025-06-15", description="Stanley RoamAlert wrist strap"):
    return {
        "price": price,
        "quantity": quantity,
        "source": source,
        "po_number": po_number,
        "supplier": supplier,
        "date": date,
        "description": description,
        "department": "5225",
        "uom": "EA",
        "is_reytech": False,
    }


def test_qty_band_downweights_far_off_rows():
    """A row at qty=500 should be downweighted vs target qty=2."""
    rows = [
        _row(100.00, quantity=500, po_number="P-VOLUME"),  # bulk, off-tier
        _row(220.00, quantity=2,   po_number="P-MATCHED-A"),
        _row(225.00, quantity=3,   po_number="P-MATCHED-B"),
    ]
    out = _analyze_market_prices(rows, request_qty=2, target_quantity=2)
    assert out["qty_band_downweighted"] == 1
    # weighted_avg should sit close to the in-band rows ($220-225), not
    # to the bulk $100 — at most a small pull from the residual ×0.2 weight.
    assert out["weighted_avg"] is not None
    assert out["weighted_avg"] > 180, (
        f"weighted_avg {out['weighted_avg']} got dragged by the bulk row"
    )


def test_qty_band_inactive_when_target_quantity_omitted():
    """No target_quantity → no qty-band downweighting (back-compat)."""
    rows = [
        _row(100.00, quantity=500, po_number="P-VOLUME"),
        _row(220.00, quantity=2,   po_number="P-MATCHED"),
    ]
    out = _analyze_market_prices(rows, request_qty=2)
    assert out["qty_band_downweighted"] == 0
    # All samples should have qty_band_match=None when target absent.
    for s in out["samples"]:
        assert s["qty_band_match"] is None


def test_samples_carry_po_number_and_metadata():
    """Each sample row must surface po_number, supplier, date, qty,
    unit_price, weight, and source — UI uses these for the drill-down."""
    rows = [
        _row(150.0, quantity=10, po_number="P-SAC-001",
             supplier="Acme Medical", source="scprs_po_lines",
             date="2025-08-01"),
        _row(180.0, quantity=15, po_number="",
             supplier="Reytech Inc", source="winning_prices",
             date="2025-09-12"),
    ]
    out = _analyze_market_prices(rows, request_qty=10, target_quantity=10)
    samples = out["samples"]
    assert len(samples) == 2
    by_po = {s["po_number"]: s for s in samples}
    assert "P-SAC-001" in by_po
    sac = by_po["P-SAC-001"]
    assert sac["supplier"] == "Acme Medical"
    assert sac["source"] == "scprs_po_lines"
    assert sac["date"] == "2025-08-01"
    assert sac["quantity"] == 10
    assert sac["unit_price"] == pytest.approx(150.0)
    assert sac["weight"] > 0


def test_samples_sorted_by_weight_descending():
    """The drill-down must show the highest-weight rows first so the
    operator sees the data points actually driving the average."""
    rows = [
        _row(100.0, quantity=2, po_number="P-OLD",   date="2018-01-01"),
        _row(200.0, quantity=2, po_number="P-FRESH", date="2025-12-01"),
        _row(150.0, quantity=2, po_number="P-MID",   date="2022-06-01"),
    ]
    out = _analyze_market_prices(rows, request_qty=2, target_quantity=2)
    samples = out["samples"]
    weights = [s["weight"] for s in samples]
    assert weights == sorted(weights, reverse=True), (
        f"Samples not sorted by weight: {weights}"
    )
    # Fresh row should be first.
    assert samples[0]["po_number"] == "P-FRESH"


def test_samples_capped_at_ten():
    rows = [_row(100 + i, quantity=2, po_number=f"P-{i}") for i in range(25)]
    out = _analyze_market_prices(rows, request_qty=2, target_quantity=2)
    assert len(out["samples"]) == 10


def test_qty_band_match_tag_distinguishes_in_vs_off_tier():
    rows = [
        _row(100.00, quantity=500, po_number="P-BULK"),    # off-tier
        _row(220.00, quantity=2,   po_number="P-MATCHED"), # in-tier
        _row(50.00,  quantity=1,   po_number="P-EVEN-MATCHED"),  # in-tier (1×)
    ]
    out = _analyze_market_prices(rows, request_qty=2, target_quantity=2)
    by_po = {s["po_number"]: s for s in out["samples"]}
    assert by_po["P-BULK"]["qty_band_match"] is False
    assert by_po["P-MATCHED"]["qty_band_match"] is True
    assert by_po["P-EVEN-MATCHED"]["qty_band_match"] is True


def test_returns_empty_samples_on_no_data():
    out = _analyze_market_prices([], request_qty=1)
    assert out["samples"] == []
    assert out["data_points"] == 0
