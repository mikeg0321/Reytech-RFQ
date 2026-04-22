"""BUILD-8 P2 regression guard — competitor breakdown scoped per agency.

Before BUILD-8 `_get_competitor_breakdown` aggregated suppliers GLOBALLY
across every agency's historical prices. A CDCR quote showed the same
top-8 competitor list as a CalVet quote for the same item, even though
the competitor composition is wildly different by department.

This test locks:
  1. Per-agency slicing: suppliers with zero data for the requested
     agency are DROPPED when the per-agency slice has >=3 competitors.
  2. Per-agency avg/low/high are computed ONLY from that agency's prices.
  3. Sparse fallback: <3 per-agency competitors triggers the global view
     (so callers aren't left empty).
  4. Each row carries a `scope` field ("per_agency" or "global").
  5. Agency-key normalization: "CDCR", "cdcr", " CDCR " all equate.
  6. `_search_won_quotes` populates supplier + department on every row
     (pre-BUILD-8 it only used them internally — the competitor
     breakdown never saw the richest data source at all).
  7. `get_pricing` wires `department` into the breakdown call.
"""
from __future__ import annotations

import re
from pathlib import Path


# ─── Helpers ─────────────────────────────────────────────────────────

def _mp(supplier, price, department="", is_reytech=False, desc="widget", qty=1):
    """Build a market_prices row as the search functions produce them."""
    return {
        "supplier": supplier,
        "price": price,
        "department": department,
        "description": desc,
        "quantity": qty,
        "is_reytech": is_reytech,
        "source": "test",
    }


# ─── Per-agency scoping ──────────────────────────────────────────────

def test_per_agency_slice_drops_off_agency_suppliers():
    """When the requested agency has >=3 suppliers, off-agency suppliers
    must be excluded — they're not competing in this market."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown
    prices = [
        _mp("ACME", 10.0, department="CDCR"),
        _mp("ACME", 11.0, department="CDCR"),
        _mp("BOLT", 12.0, department="CDCR"),
        _mp("CORK", 13.0, department="CDCR"),
        # Off-agency suppliers — should be dropped from a CDCR leaderboard:
        _mp("ONLY_CALVET", 5.0, department="CalVet"),
        _mp("ONLY_DGS", 6.0, department="DGS"),
    ]
    out = _get_competitor_breakdown(prices, agency="CDCR")
    names = {r["supplier"] for r in out}
    assert "ONLY_CALVET" not in names
    assert "ONLY_DGS" not in names
    assert {"ACME", "BOLT", "CORK"}.issubset(names)
    for r in out:
        assert r["scope"] == "per_agency", (
            f"dense slice should mark rows per_agency, got {r['scope']}"
        )


def test_per_agency_avg_uses_only_that_agencys_prices():
    """Supplier ACME has $10/$11 in CDCR but $100 in CalVet. A CDCR
    breakdown must report $10.50 avg — not a mixed $40.33."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown
    prices = [
        _mp("ACME", 10.0, department="CDCR"),
        _mp("ACME", 11.0, department="CDCR"),
        _mp("ACME", 100.0, department="CalVet"),  # off-agency, must not leak
        _mp("BOLT", 12.0, department="CDCR"),
        _mp("CORK", 13.0, department="CDCR"),
    ]
    out = _get_competitor_breakdown(prices, agency="CDCR")
    acme = next(r for r in out if r["supplier"] == "ACME")
    assert acme["avg_price"] == 10.5, (
        f"BUILD-8: per-agency avg leaked cross-agency prices — got {acme['avg_price']}, "
        f"expected 10.5 (only CDCR rows should count)"
    )
    assert acme["data_points"] == 2
    assert acme["high"] == 11.0


def test_sparse_slice_falls_back_to_global():
    """When fewer than 3 suppliers compete in the requested agency, the
    global view is returned instead — a thin slice would hide context."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown
    prices = [
        # Only 2 CDCR competitors — below the 3-threshold
        _mp("ACME", 10.0, department="CDCR"),
        _mp("BOLT", 11.0, department="CDCR"),
        # Global shows 4 — fallback should expose these
        _mp("CORK", 12.0, department="CalVet"),
        _mp("DUNE", 13.0, department="DGS"),
    ]
    out = _get_competitor_breakdown(prices, agency="CDCR")
    assert len(out) >= 3, (
        "Fallback should return the richer global view when per-agency "
        "slice is too thin"
    )
    for r in out:
        assert r["scope"] == "global", (
            f"Fallback rows must be tagged scope=global, got {r['scope']}"
        )


def test_no_agency_equals_global():
    """Empty agency string skips slicing entirely — same as pre-BUILD-8."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown
    prices = [
        _mp("ACME", 10.0, department="CDCR"),
        _mp("BOLT", 12.0, department="CalVet"),
        _mp("CORK", 15.0, department="DGS"),
    ]
    out = _get_competitor_breakdown(prices, agency="")
    names = {r["supplier"] for r in out}
    assert names == {"ACME", "BOLT", "CORK"}
    for r in out:
        assert r["scope"] == "global"


def test_agency_key_normalization():
    """Agency keys must match regardless of case/whitespace — a row stored
    as 'cdcr' must line up with a request for 'CDCR'."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown, _norm_agency_key
    assert _norm_agency_key("CDCR") == _norm_agency_key("cdcr")
    assert _norm_agency_key("  CDCR  ") == _norm_agency_key("CDCR")
    assert _norm_agency_key("") == ""
    assert _norm_agency_key(None) == ""

    prices = [
        _mp("ACME", 10.0, department="cdcr"),
        _mp("BOLT", 11.0, department=" CDCR "),
        _mp("CORK", 12.0, department="CDCR"),
    ]
    out = _get_competitor_breakdown(prices, agency="CDCR")
    assert len(out) == 3, (
        f"Case/whitespace variants must match — got {len(out)} rows, "
        f"expected 3: {out}"
    )
    assert all(r["scope"] == "per_agency" for r in out)


def test_is_reytech_rows_are_not_competitors():
    """Reytech wins must never appear in competitor breakdown — even
    when the agency slice is otherwise sparse."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown
    prices = [
        _mp("REYTECH INC", 10.0, department="CDCR", is_reytech=True),
        _mp("ACME", 11.0, department="CDCR"),
        _mp("BOLT", 12.0, department="CDCR"),
        _mp("CORK", 13.0, department="CDCR"),
    ]
    out = _get_competitor_breakdown(prices, agency="CDCR")
    names = {r["supplier"] for r in out}
    assert "REYTECH INC" not in names
    assert "ACME" in names


def test_price_outliers_still_filtered():
    """The <0.001 / >50000 sanity guards must still apply in per-agency mode."""
    from src.core.pricing_oracle_v2 import _get_competitor_breakdown
    prices = [
        _mp("BOGUS_TINY", 0.0000001, department="CDCR"),
        _mp("BOGUS_HUGE", 9_999_999.0, department="CDCR"),
        _mp("ACME", 10.0, department="CDCR"),
        _mp("BOLT", 11.0, department="CDCR"),
        _mp("CORK", 12.0, department="CDCR"),
    ]
    out = _get_competitor_breakdown(prices, agency="CDCR")
    names = {r["supplier"] for r in out}
    assert "BOGUS_TINY" not in names
    assert "BOGUS_HUGE" not in names


# ─── Supplier/department leak from won_quotes ────────────────────────

def test_won_quotes_result_row_carries_supplier_and_department():
    """Source-level guard: `_search_won_quotes` must include supplier +
    department in every dict it appends. Pre-BUILD-8 it only used them
    internally for is_reytech — the richest competitor data source was
    invisible to the breakdown."""
    src = Path("src/core/pricing_oracle_v2.py").read_text(encoding="utf-8")
    # The `won_quotes` append block must mention supplier + department keys.
    # Match the dict body loosely to survive formatting changes.
    marker = src.find('"source": "won_quotes"')
    assert marker > 0, "Couldn't locate the won_quotes result append"
    # Look back 400 chars to find the surrounding dict body
    window = src[max(0, marker - 400):marker + 200]
    assert '"supplier"' in window, (
        "BUILD-8: won_quotes result dict must populate 'supplier'. "
        "Without it, SCPRS competitor data never reaches the breakdown."
    )
    assert '"department"' in window, (
        "BUILD-8: won_quotes result dict must populate 'department' — "
        "needed for per-agency slicing."
    )


# ─── Wiring guard: get_pricing passes department through ─────────────

def test_get_pricing_passes_department_to_breakdown():
    """Source-level guard: the call site inside get_pricing must forward
    the caller's `department` into `_get_competitor_breakdown`. A refactor
    that drops this argument silently regresses to global-only."""
    src = Path("src/core/pricing_oracle_v2.py").read_text(encoding="utf-8")
    pattern = r"_get_competitor_breakdown\(market_prices,\s*department\)"
    assert re.search(pattern, src), (
        "BUILD-8: get_pricing must call _get_competitor_breakdown "
        "with `department` as the second arg."
    )
