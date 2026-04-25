"""Phase 3 regression: oracle quote-wide markup recommendation.

Guards:
  * Pre-Phase-1 winning_prices rows (recorded_at < 2026-04-25) are EXCLUDED
    from the recommendation — those carry Amazon-poisoned markup_pct from
    the auto_processor 25% default.
  * Median is used (not mean), so a single 75% outlier can't pull the
    recommendation away from the operator's true historical pattern.
  * Confidence levels: high (≥5 wins at exact agency), medium (≥5 wins at
    parent-agency class), low (anything less, no chip shown).
  * Outlier samples (margin_pct outside [5, 60]) are dropped before computing.

These tests protect against the Phase 3 trap: silently surfacing polluted
historical markup data that would re-create the Barstow loss pattern (the
operator re-keying the recommendation after a wrong-but-confident chip).
"""
import os
import sqlite3
import tempfile

import pytest


@pytest.fixture
def temp_winning_prices_db(monkeypatch):
    """Build a temp DB with the winning_prices table, seedable per-test."""
    tmp_dir = tempfile.mkdtemp()
    tmp_db = os.path.join(tmp_dir, "test.db")

    conn = sqlite3.connect(tmp_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS winning_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            quote_number TEXT, po_number TEXT, order_id TEXT,
            agency TEXT, institution TEXT,
            description TEXT NOT NULL, part_number TEXT, sku TEXT,
            qty REAL DEFAULT 1, sell_price REAL NOT NULL,
            cost REAL DEFAULT 0, margin_pct REAL DEFAULT 0,
            supplier TEXT, category TEXT, catalog_product_id INTEGER,
            fingerprint TEXT
        )
    """)
    conn.commit()
    conn.close()
    return tmp_db


def _seed(db_path, **kw):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO winning_prices "
        "(recorded_at, agency, description, sell_price, cost, margin_pct) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (kw["recorded_at"], kw.get("agency", ""),
         kw.get("description", "test"),
         kw.get("sell_price", 100.0), kw.get("cost", 75.0),
         kw["margin_pct"]),
    )
    conn.commit()
    conn.close()


def _patch_get_db(monkeypatch, db_path):
    from contextlib import contextmanager

    @contextmanager
    def fake_get_db():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        try:
            yield c
        finally:
            c.close()

    import src.core.db as core_db
    monkeypatch.setattr(core_db, "get_db", fake_get_db)


# ── Date guard: pre-Phase-1 rows MUST be excluded ──────────────────


def test_pre_phase1_rows_excluded_from_recommendation(temp_winning_prices_db, monkeypatch):
    """A row recorded before 2026-04-25 has poisoned margin from the
    auto_processor 25% default applied to bogus Amazon costs. The
    recommender must not surface those values."""
    _patch_get_db(monkeypatch, temp_winning_prices_db)
    # 6 pre-Phase-1 rows at 25% (ALL poisoned)
    for _ in range(6):
        _seed(temp_winning_prices_db, recorded_at="2026-03-15T00:00:00",
              agency="CalVet Barstow", margin_pct=25.0)
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("CalVet Barstow")
    # No clean rows post-Phase-1 → low confidence → no chip
    assert result["confidence"] == "low", \
        f"pre-Phase-1 rows leaked into recommendation: {result}"
    assert result["markup_pct"] is None
    assert result["sample_size"] == 0


def test_post_phase1_rows_drive_recommendation(temp_winning_prices_db, monkeypatch):
    """Clean post-2026-04-25 rows should produce a high-confidence rec."""
    _patch_get_db(monkeypatch, temp_winning_prices_db)
    for _ in range(7):
        _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
              agency="CalVet Barstow", margin_pct=35.0)
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("CalVet Barstow")
    assert result["confidence"] == "high"
    assert result["markup_pct"] == 35.0
    assert result["sample_size"] == 7
    assert result["scope"] == "agency"


# ── Median (not mean) — outlier rejection ──────────────────────────


def test_median_used_not_mean_to_resist_outliers(temp_winning_prices_db, monkeypatch):
    """6 wins: 35, 35, 35, 35, 35, 80. Mean = 42.5. Median = 35.
    The 80 is filtered as an outlier (>60), so median over 5 = 35.
    Recommender must return 35, not 42 or 80."""
    _patch_get_db(monkeypatch, temp_winning_prices_db)
    for m in [35, 35, 35, 35, 35]:
        _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
              agency="CalVet Barstow", margin_pct=m)
    # outlier 80% — must be dropped
    _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
          agency="CalVet Barstow", margin_pct=80)
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("CalVet Barstow")
    assert result["markup_pct"] == 35.0, \
        f"outlier 80%% should have been dropped, got {result}"
    assert result["outliers_dropped"] == 1


def test_low_outlier_also_dropped(temp_winning_prices_db, monkeypatch):
    """A 2% margin sample is also out of range — dropped."""
    _patch_get_db(monkeypatch, temp_winning_prices_db)
    for m in [35, 35, 35, 35, 35]:
        _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
              agency="CalVet Barstow", margin_pct=m)
    _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
          agency="CalVet Barstow", margin_pct=2)
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("CalVet Barstow")
    assert result["outliers_dropped"] == 1
    assert result["markup_pct"] == 35.0


# ── Agency-class fallback ──────────────────────────────────────────


def test_parent_agency_fallback_returns_medium_confidence(temp_winning_prices_db, monkeypatch):
    """Only 2 wins at exact agency, but 6 across the parent class →
    medium confidence with parent_agency scope."""
    _patch_get_db(monkeypatch, temp_winning_prices_db)
    # 2 at exact (insufficient for high)
    _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
          agency="CalVet Barstow", margin_pct=37)
    _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
          agency="CalVet Barstow", margin_pct=33)
    # 5 more across CalVet (will match parent_agency LIKE '%CalVet%')
    for m in [30, 32, 34, 36, 38]:
        _seed(temp_winning_prices_db, recorded_at="2026-04-26T00:00:00",
              agency="CalVet Yountville", margin_pct=m)
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("CalVet Barstow", parent_agency="CalVet")
    # parent_clean has 7: [37, 33, 30, 32, 34, 36, 38] → median 34
    assert result["confidence"] == "medium"
    assert result["scope"] == "parent_agency"
    assert result["markup_pct"] == 34.0


# ── Empty / edge cases ─────────────────────────────────────────────


def test_no_agency_returns_no_recommendation():
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("")
    assert result["ok"] is False
    assert result["markup_pct"] is None


def test_zero_history_returns_low_confidence(temp_winning_prices_db, monkeypatch):
    _patch_get_db(monkeypatch, temp_winning_prices_db)
    from src.core.pricing_oracle_v2 import recommend_quote_markup
    result = recommend_quote_markup("CSP-SAC")
    assert result["confidence"] == "low"
    assert result["markup_pct"] is None
    assert result["sample_size"] == 0
