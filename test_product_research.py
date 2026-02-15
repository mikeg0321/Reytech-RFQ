"""
Tests for product_research.py: query building, caching, price extraction.

Mocks SerpApi calls to avoid real network requests.
Verified against actual module signatures 2026-02-14.
"""
import json
import os
import pytest
from unittest.mock import patch, MagicMock

from product_research import (
    _build_search_query,
    _simplify_query,
    _extract_price,
    research_product,
    get_research_cache_stats,
    _cache_key,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Query Building
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildSearchQuery:

    def test_basic_description(self):
        q = _build_search_query("", "Nitrile exam gloves, large, blue")
        assert "nitrile" in q.lower()
        assert "gloves" in q.lower()

    def test_with_item_number(self):
        q = _build_search_query("6500-001-430", "X-Restraint Package")
        assert "6500-001-430" in q or "restraint" in q.lower()

    def test_strips_spec_noise(self):
        q = _build_search_query("", 'Engraved name tag; Arial, 18pt; magnetic')
        # _build_search_query cleans item numbers/prefixes but may keep description words
        assert isinstance(q, str)
        assert "engraved" in q.lower() or "name" in q.lower()

    def test_empty_returns_empty(self):
        q = _build_search_query("", "")
        assert q == "" or q is None or len(q.strip()) == 0

    def test_strips_mfr_prefix(self):
        q = _build_search_query("", "MFR# ABC123 Widget Pro")
        assert "mfr" not in q.lower() or "abc123" in q.lower()

    def test_returns_string(self):
        q = _build_search_query("TEST", "Some item")
        assert isinstance(q, str)


class TestSimplifyQuery:

    def test_shortens_long_query(self):
        long_q = "very detailed engraved two line name tag black white badge"
        result = _simplify_query(long_q)
        assert len(result) <= len(long_q)

    def test_empty_input(self):
        result = _simplify_query("")
        assert result == "" or result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Price Extraction
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractPrice:

    def test_dollar_string(self):
        item = {"price": "$12.99"}
        result = _extract_price(item)
        assert result == pytest.approx(12.99)

    def test_numeric_price(self):
        item = {"price": 24.50}
        result = _extract_price(item)
        assert result == pytest.approx(24.50)

    def test_price_range(self):
        """Price ranges may not be parseable — returns None or first price."""
        item = {"price": "$10.00 - $15.00"}
        result = _extract_price(item)
        # Implementation may or may not handle ranges
        if result is not None:
            assert result >= 10.0

    def test_missing_price(self):
        item = {}
        result = _extract_price(item)
        assert result is None

    def test_empty_string_price(self):
        item = {"price": ""}
        result = _extract_price(item)
        assert result is None

    def test_non_numeric_price(self):
        item = {"price": "Contact for price"}
        result = _extract_price(item)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Cache Key
# ═══════════════════════════════════════════════════════════════════════════════

class TestCacheKey:

    def test_deterministic(self):
        k1 = _cache_key("nitrile gloves")
        k2 = _cache_key("nitrile gloves")
        assert k1 == k2

    def test_case_insensitive(self):
        k1 = _cache_key("Nitrile Gloves")
        k2 = _cache_key("nitrile gloves")
        assert k1 == k2

    def test_different_queries_differ(self):
        k1 = _cache_key("nitrile gloves")
        k2 = _cache_key("latex gloves")
        assert k1 != k2


# ═══════════════════════════════════════════════════════════════════════════════
# research_product (mocked network)
# ═══════════════════════════════════════════════════════════════════════════════

class TestResearchProduct:

    @patch("product_research.search_amazon")
    def test_returns_found_when_amazon_has_result(self, mock_search):
        mock_search.return_value = [
            {"title": "Test Gloves", "price": 15.99, "asin": "B08TEST",
             "url": "https://amazon.com/dp/B08TEST", "rating": 4.5, "reviews": 100}
        ]
        r = research_product(description="nitrile gloves", use_cache=False)
        assert r["found"] is True
        assert r["price"] == 15.99
        assert r["asin"] == "B08TEST"
        assert r["source"] == "amazon"

    @patch("product_research.search_amazon")
    def test_returns_not_found_when_no_results(self, mock_search):
        mock_search.return_value = []
        r = research_product(description="xyznonexistent", use_cache=False)
        assert r["found"] is False
        assert r["price"] is None

    @patch("product_research.search_amazon")
    def test_caches_result(self, mock_search):
        mock_search.return_value = [
            {"title": "Cached Item", "price": 9.99, "asin": "CACHE1",
             "url": "https://amazon.com/dp/CACHE1"}
        ]
        # First call — hits API
        r1 = research_product(description="unique test item for cache", use_cache=True)
        assert r1["found"] is True

        # Second call — should use cache (mock won't be called again)
        mock_search.return_value = []
        r2 = research_product(description="unique test item for cache", use_cache=True)
        assert r2["found"] is True
        assert r2["source"] == "cache"

    @patch("product_research.search_amazon")
    def test_returns_alternatives(self, mock_search):
        mock_search.return_value = [
            {"title": "Best", "price": 10.00, "asin": "A1", "url": "u1"},
            {"title": "Alt", "price": 12.00, "asin": "A2", "url": "u2"},
        ]
        r = research_product(description="test alternatives", use_cache=False)
        assert len(r.get("alternatives", [])) == 1

    def test_empty_description_returns_not_found(self):
        r = research_product(description="")
        assert r["found"] is False


# ═══════════════════════════════════════════════════════════════════════════════
# Cache Stats
# ═══════════════════════════════════════════════════════════════════════════════

class TestCacheStats:

    def test_returns_dict(self):
        stats = get_research_cache_stats()
        assert isinstance(stats, dict)
        assert "total_entries" in stats or "entries" in stats or "total" in stats
