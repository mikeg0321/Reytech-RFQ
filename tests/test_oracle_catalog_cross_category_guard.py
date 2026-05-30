"""Regression guard for the cross-category contamination incident.

Incident 2026-05-29 (rfq_fca653f6 item 5): the operator clicked
"Apply + re-quote drifted lines" on a CCHCS PC→RFQ. A $2.00 composition
notebook (qty 150) was repriced through the Oracle to a $74.32 bid —
~3,600% markup — driving an $11,148 line, an $11,871 total and a 93%
"profit". Root cause was two compounding substrate defects:

  1. `_search_product_catalog` was the ONE market-search function that
     flat-OR'd all tokens, so the generic word "black" matched cross-
     category rows (heel boots, stethoscopes, combs). Those internal
     catalog sell-prices (~$70) became `weighted_avg` → the quote ceiling.

  2. No cost-relative sanity ceiling on the market-derived quote_price —
     the engine happily anchored a quote 35x the known cost.

These tests pin both fixes.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.core import pricing_oracle_v2


# ── Fix 1: _search_product_catalog must AND the tokens, not flat-OR ──

@pytest.fixture
def catalog_db():
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE product_catalog "
        "(name TEXT, sell_price REAL, cost REAL, best_supplier TEXT)"
    )
    rows = [
        # The legit match — a real composition notebook, cheap.
        ("Notebook, Composition, Black Marble Cover", 2.40, 1.80, "ACME"),
        # Cross-category rows that share ONLY the generic token "black".
        # Under the old flat-OR these matched and poisoned the average.
        ("Heel Protection Boot with Wedge, Petite Blue/Black", 71.00, 55.0, "MED"),
        ("Shears for Trauma, 7.5in Black", 68.50, 40.0, "MED"),
        ("General Exam Stethoscope, Black", 92.00, 70.0, "MED"),
    ]
    db.executemany("INSERT INTO product_catalog VALUES (?,?,?,?)", rows)
    db.commit()
    yield db
    db.close()


def test_product_catalog_does_not_match_on_generic_token_only(catalog_db):
    """Querying a composition notebook must NOT pull in the $70+ medical
    items that only share the word 'black'."""
    prices = pricing_oracle_v2._search_product_catalog(
        catalog_db, "NOTEBOOK, COMPOSITION, BLACK MARBLE"
    )
    descs = [p["description"] for p in prices]
    # The notebook matches (notebook AND composition AND black all present).
    assert any("Notebook" in d for d in descs), descs
    # None of the cross-category contaminants come through.
    assert not any("Boot" in d or "Stethoscope" in d or "Shears" in d
                   for d in descs), descs
    # And nothing priced anywhere near the contaminated ~$70 band.
    assert all(p["price"] < 10 for p in prices), prices


# ── Fix 2: cost-relative sanity backstop on the market ceiling ──

def test_market_ceiling_sanity_cap_rejects_contaminated_avg():
    """A contaminated market average (~$70) on a known $2.00 cost, with no
    calibration / dense data, must NOT become the quote — the engine falls
    back to cost-plus."""
    contaminated_market = {
        "data_points": 5,          # cautious tier, NOT dense (>=10)
        "competitor_avg": None,    # no real competitors...
        "competitor_low": None,
        "reytech_avg": 70.03,      # ...only poisoned internal catalog avg
        "weighted_avg": 70.03,
    }
    rec = pricing_oracle_v2._calculate_recommendation(
        cost=2.00, market=contaminated_market, quantity=150,
        category="office", agency="CCHCS", _db=None,
    )
    qp = rec.get("quote_price")
    assert qp is not None
    # Must be cost-plus sane, not the ~$70 contaminated anchor.
    assert qp < 5.0, f"expected cost-plus, got ${qp}"
    assert rec.get("markup_pct", 0) < 100


def test_market_ceiling_sanity_cap_allows_legit_high_data_density():
    """When data is dense (>=10 points), the ceiling is trusted even if the
    markup is high — we only reject the absurd + weak-basis combination."""
    dense_market = {
        "data_points": 25,
        "competitor_avg": 9.00,
        "competitor_low": 8.50,
        "weighted_avg": 9.00,
    }
    rec = pricing_oracle_v2._calculate_recommendation(
        cost=2.00, market=dense_market, quantity=10,
        category="office", agency="CCHCS", _db=None,
    )
    qp = rec.get("quote_price")
    # 350% markup but dense data → trusted (not forced to cost-plus).
    assert qp is not None and qp > 5.0, f"dense data should be trusted, got ${qp}"
