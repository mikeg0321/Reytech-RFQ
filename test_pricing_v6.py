"""
test_pricing_v6.py — Test suite for Won Quotes DB + Dynamic Pricing Oracle
Coverage target: 95%+ on core functions
"""

import json
import os
import sys
import shutil
import tempfile
import pytest
from datetime import datetime, timedelta

# Set up test data directory to avoid polluting real data
TEST_DATA_DIR = tempfile.mkdtemp(prefix="reytech_test_")


def setup_module():
    """Redirect data storage to temp directory for tests."""
    import won_quotes_db
    won_quotes_db.DATA_DIR = TEST_DATA_DIR
    won_quotes_db.WON_QUOTES_FILE = os.path.join(TEST_DATA_DIR, "won_quotes.json")
    os.makedirs(TEST_DATA_DIR, exist_ok=True)


def teardown_module():
    """Clean up test data directory."""
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
# WON QUOTES DB TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestTextProcessing:
    """Tests for normalize_text, tokenize, classify_category."""

    def test_normalize_text_basic(self):
        from won_quotes_db import normalize_text
        assert normalize_text("X-RESTRAINT Package by Stryker") == "x restraint package by stryker"

    def test_normalize_text_special_chars(self):
        from won_quotes_db import normalize_text
        assert normalize_text("Item #123-456 (medical)") == "item 123 456 medical"

    def test_normalize_text_whitespace(self):
        from won_quotes_db import normalize_text
        assert normalize_text("  lots   of    spaces  ") == "lots of spaces"

    def test_tokenize_removes_stop_words(self):
        from won_quotes_db import tokenize
        tokens = tokenize("the medical package for a hospital")
        assert "the" not in tokens
        assert "for" not in tokens
        assert "medical" in tokens
        assert "hospital" in tokens

    def test_tokenize_removes_short_tokens(self):
        from won_quotes_db import tokenize
        tokens = tokenize("I a an 1 2 stryker medical")
        assert "stryker" in tokens
        assert "medical" in tokens
        # Single char tokens removed
        for t in tokens:
            assert len(t) > 1

    def test_classify_medical(self):
        from won_quotes_db import classify_category
        assert classify_category("Stryker Medical Restraint Package") == "medical_equipment"

    def test_classify_office(self):
        from won_quotes_db import classify_category
        assert classify_category("HP Toner Cartridge 58A") == "office_supplies"

    def test_classify_industrial(self):
        from won_quotes_db import classify_category
        assert classify_category("Grainger Industrial Pipe Valve") == "industrial"

    def test_classify_general_fallback(self):
        from won_quotes_db import classify_category
        assert classify_category("Widget XYZ Unknown Product") == "general"

    def test_classify_janitorial(self):
        from won_quotes_db import classify_category
        assert classify_category("Bleach Disinfectant Cleaning Wipe") == "janitorial"


class TestFreshnessWeight:
    """Tests for freshness_weight calculation."""

    def test_recent_award(self):
        from won_quotes_db import freshness_weight
        recent = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        assert freshness_weight(recent) == 1.0

    def test_six_month_award(self):
        from won_quotes_db import freshness_weight
        date = (datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d")
        assert freshness_weight(date) == 0.8

    def test_one_year_award(self):
        from won_quotes_db import freshness_weight
        date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        assert freshness_weight(date) == 0.5

    def test_old_award(self):
        from won_quotes_db import freshness_weight
        assert freshness_weight("2020-01-01") == 0.2

    def test_invalid_date(self):
        from won_quotes_db import freshness_weight
        assert freshness_weight("not-a-date") == 0.2

    def test_empty_date(self):
        from won_quotes_db import freshness_weight
        assert freshness_weight("") == 0.2

    def test_slash_date_format(self):
        from won_quotes_db import freshness_weight
        recent = (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")
        assert freshness_weight(recent) == 1.0


class TestIngestion:
    """Tests for ingesting SCPRS results into the KB."""

    def setup_method(self):
        """Reset KB before each test."""
        import won_quotes_db
        won_quotes_db.save_won_quotes([])

    def test_ingest_single_record(self):
        from won_quotes_db import ingest_scprs_result, load_won_quotes
        record = ingest_scprs_result(
            po_number="4500012345",
            item_number="6500-001-430",
            description="X-RESTRAINT PACKAGE by Stryker Medical",
            unit_price=1245.00,
            quantity=2,
            supplier="Medline Industries",
            department="CCHCS",
            award_date="2025-09-15",
        )
        assert record["id"].startswith("wq_")
        assert record["unit_price"] == 1245.00
        assert record["category"] == "medical_equipment"
        assert "restraint" in record["tokens"]

        quotes = load_won_quotes()
        assert len(quotes) == 1

    def test_deduplication(self):
        from won_quotes_db import ingest_scprs_result, load_won_quotes
        # Ingest same record twice
        ingest_scprs_result("PO1", "ITEM1", "Test Product", 100.00)
        ingest_scprs_result("PO1", "ITEM1", "Test Product", 150.00)  # Price changed
        quotes = load_won_quotes()
        assert len(quotes) == 1
        assert quotes[0]["unit_price"] == 150.00  # Updated

    def test_bulk_ingest(self):
        from won_quotes_db import ingest_scprs_bulk, load_won_quotes
        results = [
            {"po_number": f"PO{i}", "item_number": f"ITEM{i}",
             "description": f"Product {i}", "unit_price": 100 + i * 10}
            for i in range(5)
        ]
        stats = ingest_scprs_bulk(results)
        assert stats["ingested"] == 5
        assert stats["updated"] == 0
        assert stats["skipped"] == 0
        assert len(load_won_quotes()) == 5

    def test_bulk_ingest_skips_zero_price(self):
        from won_quotes_db import ingest_scprs_bulk
        results = [
            {"po_number": "PO1", "item_number": "I1", "description": "P1", "unit_price": 0},
            {"po_number": "PO2", "item_number": "I2", "description": "P2", "unit_price": 100},
        ]
        stats = ingest_scprs_bulk(results)
        assert stats["skipped"] == 1
        assert stats["ingested"] == 1

    def test_lru_eviction(self):
        import won_quotes_db
        old_max = won_quotes_db.MAX_RECORDS
        won_quotes_db.MAX_RECORDS = 5  # Temporary low cap
        try:
            for i in range(10):
                ingest_result = won_quotes_db.ingest_scprs_result(
                    f"PO{i}", f"ITEM{i}", f"Product {i}", 100 + i
                )
            quotes = won_quotes_db.load_won_quotes()
            assert len(quotes) <= 5
        finally:
            won_quotes_db.MAX_RECORDS = old_max


class TestMatching:
    """Tests for find_similar_items matching engine."""

    def setup_method(self):
        """Seed KB with test data."""
        from won_quotes_db import save_won_quotes, tokenize, classify_category
        recent = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        older = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

        test_quotes = [
            {
                "id": "wq_test_001",
                "po_number": "PO001",
                "item_number": "6500-001-430",
                "description": "X-RESTRAINT PACKAGE by Stryker Medical",
                "normalized_description": "x restraint package stryker medical",
                "tokens": list(tokenize("X-RESTRAINT PACKAGE by Stryker Medical")),
                "category": "medical_equipment",
                "supplier": "Medline",
                "department": "CCHCS",
                "unit_price": 1245.00,
                "quantity": 2,
                "total": 2490.00,
                "award_date": recent,
                "source": "scprs_live",
                "confidence": 1.0,
                "ingested_at": datetime.now().isoformat(),
            },
            {
                "id": "wq_test_002",
                "po_number": "PO002",
                "item_number": "6500-001-430",
                "description": "RESTRAINT KIT Stryker Medical Equipment",
                "normalized_description": "restraint kit stryker medical equipment",
                "tokens": list(tokenize("RESTRAINT KIT Stryker Medical Equipment")),
                "category": "medical_equipment",
                "supplier": "Stryker Direct",
                "department": "CDCR",
                "unit_price": 1300.00,
                "quantity": 1,
                "total": 1300.00,
                "award_date": older,
                "source": "scprs_live",
                "confidence": 1.0,
                "ingested_at": datetime.now().isoformat(),
            },
            {
                "id": "wq_test_003",
                "po_number": "PO003",
                "item_number": "1234-567-890",
                "description": "HP Toner Cartridge 58A Black",
                "normalized_description": "hp toner cartridge 58a black",
                "tokens": list(tokenize("HP Toner Cartridge 58A Black")),
                "category": "office_supplies",
                "supplier": "Office Depot",
                "department": "CalVet",
                "unit_price": 89.99,
                "quantity": 10,
                "total": 899.90,
                "award_date": recent,
                "source": "scprs_live",
                "confidence": 1.0,
                "ingested_at": datetime.now().isoformat(),
            },
        ]
        save_won_quotes(test_quotes)

    def test_exact_item_number_match(self):
        from won_quotes_db import find_similar_items
        results = find_similar_items("6500-001-430", "Anything at all")
        assert len(results) >= 1
        assert results[0]["match_confidence"] == 1.0
        assert "exact_item_number" in results[0]["match_reasons"]

    def test_token_overlap_match(self):
        from won_quotes_db import find_similar_items
        results = find_similar_items("", "Stryker Medical Restraint Package System")
        assert len(results) >= 1
        assert results[0]["match_confidence"] >= 0.5

    def test_no_match(self):
        from won_quotes_db import find_similar_items
        results = find_similar_items("9999-999-999", "Completely Unrelated Quantum Widget")
        assert len(results) == 0

    def test_freshness_weighting(self):
        from won_quotes_db import find_similar_items
        results = find_similar_items("6500-001-430", "Restraint Package")
        # Recent result should sort higher
        if len(results) >= 2:
            assert results[0]["freshness_weight"] >= results[1]["freshness_weight"]

    def test_max_results_limit(self):
        from won_quotes_db import find_similar_items
        results = find_similar_items("6500-001-430", "Restraint", max_results=1)
        assert len(results) <= 1


class TestPriceHistory:
    """Tests for get_price_history."""

    def setup_method(self):
        """Use same seed data as TestMatching."""
        TestMatching.setup_method(self)

    def test_price_history_with_data(self):
        from won_quotes_db import get_price_history
        history = get_price_history("6500-001-430", "Stryker Restraint", months=24)
        assert history["matches"] >= 1
        assert history["min_price"] is not None
        assert history["median_price"] is not None

    def test_price_history_no_data(self):
        from won_quotes_db import get_price_history
        history = get_price_history("0000-000-000", "Nonexistent Widget", months=24)
        assert history["matches"] == 0
        assert history["trend"] == "insufficient_data"


class TestWinProbability:
    """Tests for win_probability estimation."""

    def setup_method(self):
        TestMatching.setup_method(self)

    def test_below_median_high_probability(self):
        from won_quotes_db import win_probability
        result = win_probability(1100.00, "6500-001-430", "Restraint")
        # Below median price → should have higher probability
        assert result["probability"] > 0.5

    def test_above_median_lower_probability(self):
        from won_quotes_db import win_probability
        result = win_probability(1500.00, "6500-001-430", "Restraint")
        # Well above → lower probability
        assert result["probability"] < 0.5

    def test_no_data_returns_coin_flip(self):
        from won_quotes_db import win_probability
        result = win_probability(100.00, "0000-000-000", "Unknown Widget")
        assert result["probability"] == 0.5
        assert result["confidence_level"] == "no_data"

    def test_reasoning_populated(self):
        from won_quotes_db import win_probability
        result = win_probability(1245.00, "6500-001-430", "Stryker Restraint")
        assert len(result["reasoning"]) > 0


class TestKBStats:
    """Tests for get_kb_stats."""

    def setup_method(self):
        TestMatching.setup_method(self)

    def test_stats_populated(self):
        from won_quotes_db import get_kb_stats
        stats = get_kb_stats()
        assert stats["total_records"] == 3
        assert "medical_equipment" in stats["categories"]
        assert stats["avg_unit_price"] is not None

    def test_stats_empty_db(self):
        from won_quotes_db import save_won_quotes, get_kb_stats
        save_won_quotes([])
        stats = get_kb_stats()
        assert stats["total_records"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# PRICING ORACLE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestPricingOracle:
    """Tests for pricing_oracle.recommend_price."""

    def setup_method(self):
        """Seed KB with known data for pricing tests."""
        TestMatching.setup_method(self)

    def test_full_data_recommendation(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="6500-001-430",
            description="X-RESTRAINT PACKAGE Stryker",
            supplier_cost=800.00,
            scprs_price=1245.00,
            agency="CCHCS",
        )
        assert result["data_quality"] == "full"
        assert result["recommended"] is not None
        assert result["aggressive"] is not None
        assert result["safe"] is not None
        # Recommended should be less than SCPRS
        assert result["recommended"]["price"] < 1245.00
        # Aggressive should be less than recommended
        assert result["aggressive"]["price"] <= result["recommended"]["price"]
        # All prices should be above cost
        assert result["recommended"]["price"] > 800.00
        assert result["aggressive"]["price"] > 800.00

    def test_scprs_only_recommendation(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="6500-001-430",
            description="Stryker Restraint",
            supplier_cost=None,
            scprs_price=1245.00,
        )
        assert result["data_quality"] == "scprs_only"
        assert result["recommended"]["price"] < 1245.00

    def test_cost_only_recommendation(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="9999-999-999",
            description="Unknown Custom Product",
            supplier_cost=500.00,
            scprs_price=None,
        )
        assert result["data_quality"] == "cost_only"
        # Should apply default markup
        assert result["recommended"]["price"] > 500.00

    def test_no_data_blocks_generation(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="9999-999-999",
            description="Unknown Widget",
            supplier_cost=None,
            scprs_price=None,
        )
        assert result["data_quality"] == "no_data"
        assert "no_pricing_data" in result["flags"]
        assert result["recommended"] is None

    def test_profit_floor_enforced(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="6500-001-430",
            description="Stryker Restraint",
            supplier_cost=1240.00,  # Very close to SCPRS
            scprs_price=1245.00,
        )
        # Should not bid below cost + floor
        assert result["recommended"]["price"] >= 1240.00 + 25  # hard floor

    def test_hard_floor_enforced(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="",
            description="Cheap Item",
            supplier_cost=10.00,
            scprs_price=12.00,
        )
        # Even aggressive tier shouldn't go below cost + $25
        assert result["aggressive"]["price"] >= 35.00

    def test_amazon_lower_floor(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="",
            description="Generic Product",
            supplier_cost=100.00,
            scprs_price=None,
            source_type="amazon",
        )
        assert result["recommended"]["price"] >= 150.00  # cost + $50 amazon floor

    def test_win_probability_in_tiers(self):
        from pricing_oracle import recommend_price
        result = recommend_price(
            item_number="6500-001-430",
            description="Stryker Restraint Package",
            supplier_cost=800.00,
            scprs_price=1245.00,
        )
        # Aggressive should have higher win prob than safe
        assert result["aggressive"]["win_probability"] >= result["safe"]["win_probability"]


class TestBatchPricing:
    """Tests for recommend_prices_for_rfq."""

    def setup_method(self):
        TestMatching.setup_method(self)

    def test_batch_pricing(self):
        from pricing_oracle import recommend_prices_for_rfq
        rfq_data = {
            "solicitation_number": "10838043",
            "agency": "CCHCS",
            "line_items": [
                {
                    "line_number": 1,
                    "item_number": "6500-001-430",
                    "description": "X-RESTRAINT PACKAGE by Stryker Medical",
                    "qty": 2,
                    "supplier_cost": 800.00,
                    "scprs_price": 1245.00,
                },
                {
                    "line_number": 2,
                    "item_number": "9999-999-999",
                    "description": "Unknown Product",
                    "qty": 5,
                },
            ],
        }
        result = recommend_prices_for_rfq(rfq_data)
        assert result["rfq_id"] == "10838043"
        assert result["summary"]["total_items"] == 2
        assert result["summary"]["priced"] >= 1
        assert result["summary"]["needs_manual"] >= 0


class TestLegacyCompatibility:
    """Tests for backward-compatible pricing function."""

    def test_legacy_with_scprs(self):
        from pricing_oracle import calculate_recommended_price_legacy
        price = calculate_recommended_price_legacy(
            cost=800.00,
            scprs_price=1245.00,
        )
        assert price > 800.00
        assert price < 1245.00

    def test_legacy_without_scprs(self):
        from pricing_oracle import calculate_recommended_price_legacy
        price = calculate_recommended_price_legacy(
            cost=100.00,
            scprs_price=None,
        )
        assert price >= 125.00  # 25% markup minimum

    def test_legacy_returns_float(self):
        from pricing_oracle import calculate_recommended_price_legacy
        price = calculate_recommended_price_legacy(cost=100.00, scprs_price=200.00)
        assert isinstance(price, float)


class TestPricingHealthCheck:
    """Tests for pricing system health check."""

    def test_health_check_with_data(self):
        TestMatching.setup_method(self)
        from pricing_oracle import pricing_health_check
        health = pricing_health_check()
        assert health["status"] in ("healthy", "degraded")
        assert health["kb_records"] >= 0

    def test_health_check_empty_kb(self):
        from won_quotes_db import save_won_quotes
        save_won_quotes([])
        from pricing_oracle import pricing_health_check
        health = pricing_health_check()
        assert health["status"] == "degraded"
        assert any("empty" in issue.lower() for issue in health["issues"])


# ═══════════════════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-x"])
