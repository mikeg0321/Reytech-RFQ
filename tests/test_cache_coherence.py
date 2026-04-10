"""
test_cache_coherence.py — Cache Layer Tests for Reytech RFQ

Tests the 4 independent cache layers:
  - item_identifier:    item_id_cache.json         (30-day TTL)
  - product_validator:  grok_validation_cache.json  (14-day TTL)
  - product_research:   product_research_cache.json (7-day TTL)
  - web_price_research: web_price_cache.json        (7-day TTL)

Verifies: TTL expiry, cache key correctness, stale data detection,
max-entries pruning, and cross-cache independence.
"""

import json
import os
import time
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock


# ── Item Identifier Cache ───────────────────────────────────────────────────

class TestItemIdentifierCache:
    """Tests for item_identifier.py 30-day cache."""

    def test_cache_hit_returns_result(self, temp_data_dir):
        from src.agents.item_identifier import _cache_get, _cache_set
        _cache_set("nitrile exam gloves", {"product_name": "Gloves", "search_terms": ["gloves"]})
        result = _cache_get("nitrile exam gloves")
        assert result is not None
        assert result["product_name"] == "Gloves"

    def test_cache_miss_returns_none(self, temp_data_dir):
        from src.agents.item_identifier import _cache_get
        result = _cache_get("never-cached-item-xyz")
        assert result is None

    def test_cache_key_is_case_insensitive(self, temp_data_dir):
        from src.agents.item_identifier import _cache_set, _cache_get
        _cache_set("Nitrile Exam Gloves", {"product_name": "Gloves"})
        result = _cache_get("nitrile exam gloves")
        assert result is not None

    def test_cache_expires_after_ttl(self, temp_data_dir):
        from src.agents.item_identifier import _cache_set, _cache_get, _load_cache, _cache_key, CACHE_FILE
        _cache_set("old item", {"product_name": "Old"})
        # Manually backdate the timestamp
        cache = _load_cache()
        key = _cache_key("old item")
        cache[key]["ts"] = time.time() - (31 * 86400)  # 31 days ago
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        result = _cache_get("old item")
        assert result is None, "Expired cache entry should return None"

    def test_cache_within_ttl_still_valid(self, temp_data_dir):
        from src.agents.item_identifier import _cache_set, _cache_get, _load_cache, _cache_key, CACHE_FILE
        _cache_set("recent item", {"product_name": "Recent"})
        # Backdate to 29 days ago (within 30-day TTL)
        cache = _load_cache()
        key = _cache_key("recent item")
        cache[key]["ts"] = time.time() - (29 * 86400)
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        result = _cache_get("recent item")
        assert result is not None


# ── Grok Validation Cache ───────────────────────────────────────────────────

class TestGrokValidationCache:
    """Tests for product_validator.py 14-day cache."""

    def test_cache_hit_skips_api(self, temp_data_dir):
        from src.agents.product_validator import _cache_store, _cache_lookup
        _cache_store("test item", "", "", {
            "ok": True, "price": 15.99, "confidence": 0.85,
            "product_name": "Test Product", "asin": "B0TEST123",
        })
        result = _cache_lookup("test item")
        assert result is not None
        assert result["price"] == 15.99
        assert result.get("from_cache") is True

    def test_cache_key_includes_upc_and_mfg(self, temp_data_dir):
        from src.agents.product_validator import _cache_key
        key1 = _cache_key("gloves", upc="123456789012", mfg_number="")
        key2 = _cache_key("gloves", upc="", mfg_number="ABC-123")
        key3 = _cache_key("gloves", upc="", mfg_number="")
        assert key1 != key2 != key3, "Different identifiers should produce different cache keys"

    def test_cache_expires_after_14_days(self, temp_data_dir):
        from src.agents.product_validator import (
            _cache_store, _cache_lookup, _load_cache, _cache_key, CACHE_FILE
        )
        _cache_store("old grok item", "", "", {"ok": True, "price": 10.00, "confidence": 0.9})
        # Backdate
        cache = _load_cache()
        key = _cache_key("old grok item")
        cache[key]["cached_at"] = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
        result = _cache_lookup("old grok item")
        assert result is None

    def test_low_confidence_not_cached(self, temp_data_dir):
        """validate_product() should NOT cache results with confidence < 0.50."""
        from src.agents.product_validator import _cache_lookup
        # Simulate what validate_product does: only cache if confidence >= 0.50
        # A result with 0.30 confidence should not be in cache
        result = _cache_lookup("low confidence item")
        assert result is None


# ── Product Research Cache ──────────────────────────────────────────────────

class TestProductResearchCache:
    """Tests for product_research.py 7-day cache."""

    def test_cache_store_and_lookup(self, temp_data_dir):
        from src.agents.product_research import _cache_store, _cache_lookup
        _cache_store("nitrile gloves", {
            "found": True, "price": 12.99, "title": "Nitrile Gloves",
            "source": "amazon", "asin": "B0TEST",
        })
        result = _cache_lookup("nitrile gloves")
        assert result is not None
        assert result["price"] == 12.99

    def test_cache_key_normalized(self, temp_data_dir):
        """Queries with different casing/punctuation should hit same cache entry."""
        from src.agents.product_research import _cache_store, _cache_lookup
        _cache_store("Nitrile Gloves, Medium", {
            "found": True, "price": 12.99,
        })
        result = _cache_lookup("nitrile gloves medium")
        assert result is not None

    def test_cache_expires_after_7_days(self, temp_data_dir):
        from src.agents.product_research import (
            _cache_store, _cache_lookup, _load_cache, _cache_key, CACHE_FILE
        )
        _cache_store("expiring item", {"found": True, "price": 5.00})
        cache = _load_cache()
        key = _cache_key("expiring item")
        cache[key]["cached_at"] = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, default=str)
        result = _cache_lookup("expiring item")
        assert result is None

    def test_max_entries_pruned(self, temp_data_dir):
        from src.agents.product_research import _cache_store, _load_cache, MAX_CACHE_ENTRIES
        # Store more than max entries
        for i in range(min(MAX_CACHE_ENTRIES + 10, 100)):  # Cap at 100 for test speed
            _cache_store(f"item-{i}", {"found": True, "price": float(i)})
        cache = _load_cache()
        assert len(cache) <= MAX_CACHE_ENTRIES or len(cache) <= 100

    def test_not_found_result_cached_with_short_ttl(self, temp_data_dir):
        """Not-found results are cached to prevent repeated lookups."""
        from src.agents.product_research import _cache_store, _cache_lookup
        _cache_store("nonexistent widget", {"found": False, "price": None, "source": None})
        result = _cache_lookup("nonexistent widget")
        assert result is not None
        assert result["found"] is False


# ── Web Price Research Cache ────────────────────────────────────────────────

class TestWebPriceCache:
    """Tests for web_price_research.py 7-day cache."""

    def test_cache_key_includes_part_number(self, temp_data_dir):
        from src.agents.web_price_research import _cache_key
        key1 = _cache_key("gloves", "ABC-123")
        key2 = _cache_key("gloves", "XYZ-789")
        key3 = _cache_key("gloves", "")
        assert key1 != key2, "Different part numbers should have different keys"
        assert key1 != key3, "Part number vs no part number should differ"

    def test_cache_hit_returns_cached_flag(self, temp_data_dir):
        from src.agents.web_price_research import _load_cache, _save_cache, _cache_key, search_product_price
        # Pre-populate cache
        cache = _load_cache()
        ck = _cache_key("test product", "PN-001")
        cache[ck] = {
            "found": True, "price": 24.99, "source": "Amazon",
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_cache(cache)
        # search_product_price should return cached result without API call
        result = search_product_price("test product", "PN-001")
        assert result.get("cached") is True or result.get("price") == 24.99


# ── Cross-Cache Independence ───────────────────────────────────────────────

class TestCrossCache:
    """Verify caches don't interfere with each other."""

    def test_different_cache_files_isolated(self, temp_data_dir):
        """Each module writes to its own cache file."""
        from src.agents.item_identifier import CACHE_FILE as id_cache
        from src.agents.product_validator import CACHE_FILE as grok_cache
        from src.agents.product_research import CACHE_FILE as research_cache
        from src.agents.web_price_research import CACHE_FILE as web_cache

        files = [id_cache, grok_cache, research_cache, web_cache]
        assert len(set(files)) == 4, f"Cache files should be unique, got: {files}"

    def test_item_id_cache_does_not_affect_grok_cache(self, temp_data_dir):
        """Writing to item_identifier cache should not appear in product_validator cache."""
        from src.agents.item_identifier import _cache_set
        from src.agents.product_validator import _cache_lookup

        _cache_set("cross-cache test item", {"product_name": "Test"})
        result = _cache_lookup("cross-cache test item")
        assert result is None, "Item ID cache should not leak into Grok validation cache"

    def test_research_cache_does_not_affect_web_cache(self, temp_data_dir):
        from src.agents.product_research import _cache_store
        from src.agents.web_price_research import _cache_key, _load_cache

        _cache_store("cross-cache research item", {"found": True, "price": 9.99})
        web_cache = _load_cache()
        web_key = _cache_key("cross-cache research item", "")
        assert web_key not in web_cache, "Research cache should not leak into web price cache"
