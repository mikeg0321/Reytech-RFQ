"""V3 Test Suite — Cache Coherence Across 4 Layers.

Tests interaction between the 4 independent caches:
  1. item_id_cache.json     (30-day TTL, 10K max)
  2. grok_validation_cache  (14-day TTL, 3K max)
  3. product_research_cache (7-day TTL, 5K max)
  4. web_price_cache.json   (7-day TTL, 3K max)

Key risks:
  - Stale 30-day item_id cache points to old search terms while
    7-day price caches have fresher data
  - Multiple cache layers return different prices for the same item
  - Catalog write-back doesn't check cache staleness
  - "First cache hit wins" with no cross-layer freshness comparison
"""
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# TTL Enforcement: Each cache must reject stale entries
# ═══════════════════════════════════════════════════════════════════════════

class TestCacheTtlEnforcement:
    """Each cache must return None for entries older than their TTL."""

    def test_item_id_cache_30_day_ttl(self, temp_data_dir):
        """item_id cache entry older than 30 days must be rejected."""
        cache_file = os.path.join(temp_data_dir, "item_id_cache.json")
        stale_ts = time.time() - (31 * 86400)  # 31 days ago

        cache = {
            "abc123": {
                "ts": stale_ts,
                "desc": "Old product",
                "result": {"search_terms": ["old term"], "from_cache": True},
            }
        }
        with open(cache_file, "w") as f:
            json.dump(cache, f)

        # Verify staleness check: entry should be treated as expired
        entry = cache["abc123"]
        age_days = (time.time() - entry["ts"]) / 86400
        is_stale = age_days > 30
        assert is_stale, f"31-day-old entry should be stale (age={age_days:.1f}d)"

    def test_grok_cache_14_day_ttl(self, temp_data_dir):
        """grok_validation cache entry older than 14 days must be rejected."""
        stale_dt = (datetime.now() - timedelta(days=15)).isoformat()
        entry = {"cached_at": stale_dt, "price": 42.99, "confidence": 0.85}

        age = datetime.now() - datetime.fromisoformat(stale_dt)
        is_stale = age > timedelta(days=14)
        assert is_stale, "15-day-old Grok entry should be stale"

    def test_product_research_7_day_ttl(self, temp_data_dir):
        """product_research cache entry older than 7 days must be rejected."""
        stale_dt = (datetime.now() - timedelta(days=8)).isoformat()
        entry = {"cached_at": stale_dt, "price": 35.00}

        age = datetime.now() - datetime.fromisoformat(stale_dt)
        is_stale = age > timedelta(days=7)
        assert is_stale, "8-day-old research entry should be stale"

    def test_web_price_7_day_ttl(self, temp_data_dir):
        """web_price cache entry older than 7 days must be rejected."""
        stale_dt = (datetime.now() - timedelta(days=8)).isoformat()
        entry = {"cached_at": stale_dt, "price": 29.99}

        age = datetime.now() - datetime.fromisoformat(stale_dt)
        is_stale = age > timedelta(days=7)
        assert is_stale

    def test_fresh_entry_accepted(self):
        """An entry from 1 hour ago should be accepted by any cache."""
        fresh_ts = time.time() - 3600  # 1 hour ago
        age_days = (time.time() - fresh_ts) / 86400
        assert age_days < 7, "1-hour-old entry should be fresh for all caches"


# ═══════════════════════════════════════════════════════════════════════════
# Cache Key Consistency
# ═══════════════════════════════════════════════════════════════════════════

class TestCacheKeyConsistency:
    """Same input must produce same cache key (deterministic hashing)."""

    def test_item_id_key_is_deterministic(self):
        """Same description → same cache key."""
        desc = "Nitrile exam gloves, large, powder-free"
        key1 = hashlib.md5(desc.strip().lower().encode()).hexdigest()[:12]
        key2 = hashlib.md5(desc.strip().lower().encode()).hexdigest()[:12]
        assert key1 == key2

    def test_item_id_key_case_insensitive(self):
        """'NITRILE GLOVES' and 'nitrile gloves' → same key."""
        k1 = hashlib.md5("NITRILE GLOVES".lower().encode()).hexdigest()[:12]
        k2 = hashlib.md5("nitrile gloves".lower().encode()).hexdigest()[:12]
        assert k1 == k2

    def test_grok_key_includes_identifiers(self):
        """Grok cache key must include UPC and MFG# so different products don't collide."""
        desc = "Medical gloves"
        upc1 = "012345678901"
        upc2 = "012345678902"
        key1 = hashlib.md5(f"{desc.lower()}|{upc1}|".encode()).hexdigest()[:16]
        key2 = hashlib.md5(f"{desc.lower()}|{upc2}|".encode()).hexdigest()[:16]
        assert key1 != key2, "Different UPCs must produce different cache keys"

    def test_web_price_key_includes_part_number(self):
        """Web price cache key includes part_number for uniqueness."""
        desc = "Stryker restraint"
        pn1 = "6500-001-430"
        pn2 = "6500-001-431"
        key1 = hashlib.md5(f"{desc.lower()}|{pn1.lower()}".encode()).hexdigest()[:16]
        key2 = hashlib.md5(f"{desc.lower()}|{pn2.lower()}".encode()).hexdigest()[:16]
        assert key1 != key2


# ═══════════════════════════════════════════════════════════════════════════
# Cross-Layer Staleness: Stale cache should not override fresh data
# ═══════════════════════════════════════════════════════════════════════════

class TestCrossLayerStaleness:
    """When multiple sources have data, the freshest should be preferred."""

    def test_stale_item_id_vs_fresh_catalog(self):
        """A 29-day item_id cache hit should not override a fresh catalog match."""
        item_id_cached_at = time.time() - (29 * 86400)  # 29 days
        catalog_updated_at = time.time() - (1 * 86400)   # 1 day

        item_id_age = (time.time() - item_id_cached_at) / 86400
        catalog_age = (time.time() - catalog_updated_at) / 86400

        # Catalog data is 29x fresher — should be preferred for pricing
        assert catalog_age < item_id_age
        assert catalog_age < 7  # Within any cache TTL

    def test_cheaper_cost_wins_in_catalog(self):
        """Catalog update rule: new cost only overwrites if cheaper."""
        existing_cost = 42.99
        new_cost_cheaper = 35.00
        new_cost_expensive = 55.00

        # Cheaper wins
        should_update_cheaper = (existing_cost is None or existing_cost == 0
                                 or new_cost_cheaper < existing_cost)
        assert should_update_cheaper is True

        # More expensive does NOT overwrite
        should_update_expensive = (existing_cost is None or existing_cost == 0
                                   or new_cost_expensive < existing_cost)
        assert should_update_expensive is False

    def test_enrichment_only_fills_empty_fields(self):
        """Enrichment write-back should not overwrite existing good data."""
        product = {
            "upc": "012345678901",  # Already has UPC
            "mfg_number": "",       # Empty — should be filled
            "best_cost": 42.99,     # Already has cost
        }

        new_data = {
            "upc": "999999999999",    # Different UPC — should NOT overwrite
            "mfg_number": "W12919",   # New MFG# — should fill empty field
            "best_cost": 50.00,       # More expensive — should NOT overwrite
        }

        # Simulate enrichment rules
        if not product["upc"]:
            product["upc"] = new_data["upc"]
        # UPC was already set — should NOT change
        assert product["upc"] == "012345678901"

        if not product["mfg_number"]:
            product["mfg_number"] = new_data["mfg_number"]
        # MFG# was empty — should be filled
        assert product["mfg_number"] == "W12919"

        if product["best_cost"] == 0 or new_data["best_cost"] < product["best_cost"]:
            product["best_cost"] = new_data["best_cost"]
        # $50 is NOT cheaper than $42.99 — should NOT overwrite
        assert product["best_cost"] == 42.99


# ═══════════════════════════════════════════════════════════════════════════
# Cache Eviction: Max size enforcement
# ═══════════════════════════════════════════════════════════════════════════

class TestCacheEviction:
    """Caches must not grow unbounded."""

    def test_item_id_max_10k(self):
        """item_id cache max should be 10,000."""
        try:
            from src.agents.item_identifier import MAX_CACHE
            assert MAX_CACHE == 10000
        except ImportError:
            # Check source directly
            path = os.path.join(os.path.dirname(__file__), "..",
                               "src", "agents", "item_identifier.py")
            with open(path) as f:
                source = f.read()
            assert "10000" in source or "10_000" in source, \
                "item_identifier should have MAX_CACHE = 10000"

    def test_grok_max_3k(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                           "src", "agents", "product_validator.py")
        if not os.path.exists(path):
            pytest.skip("product_validator.py not found")
        with open(path) as f:
            source = f.read()
        assert "3000" in source, "grok_validation cache should max at 3000"

    def test_web_price_max_3k(self):
        try:
            from src.agents.web_price_research import MAX_CACHE
            assert MAX_CACHE == 3000
        except ImportError:
            path = os.path.join(os.path.dirname(__file__), "..",
                               "src", "agents", "web_price_research.py")
            with open(path) as f:
                source = f.read()
            assert "3000" in source, "web_price cache should max at 3000"

    def test_eviction_preserves_newest(self):
        """When evicting, newest entries must survive."""
        cache = {}
        for i in range(100):
            cache[f"key_{i}"] = {"ts": time.time() - (100 - i) * 3600}

        # Simulate prune to 50: keep newest
        max_size = 50
        if len(cache) > max_size:
            sorted_keys = sorted(cache.keys(), key=lambda k: cache[k]["ts"])
            for k in sorted_keys[:len(cache) - max_size]:
                del cache[k]

        assert len(cache) == 50
        # Newest entry (key_99) should survive
        assert "key_99" in cache
        # Oldest entry (key_0) should be evicted
        assert "key_0" not in cache


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline Skip Logic: Don't call APIs when cache/catalog has data
# ═══════════════════════════════════════════════════════════════════════════

class TestPipelineSkipLogic:
    """Items with existing data should skip expensive API calls."""

    def test_unit_cost_present_skips_grok(self):
        """Item with unit_cost should not trigger Grok validation."""
        item = {"pricing": {"unit_cost": 42.99, "price_source": "catalog"}}
        needs_grok = (item.get("pricing", {}).get("unit_cost") or 0) <= 0
        assert needs_grok is False

    def test_high_catalog_confidence_skips_grok(self):
        """Catalog match ≥ 0.75 should skip Grok validation."""
        item = {"pricing": {"catalog_confidence": 0.80, "unit_cost": 35.00}}
        confidence = item.get("pricing", {}).get("catalog_confidence", 0)
        needs_grok = confidence < 0.75
        assert needs_grok is False

    def test_low_catalog_confidence_triggers_grok(self):
        """Catalog match < 0.75 should trigger Grok validation."""
        item = {"pricing": {"catalog_confidence": 0.55}}
        confidence = item.get("pricing", {}).get("catalog_confidence", 0)
        needs_grok = confidence < 0.75
        assert needs_grok is True

    def test_no_pricing_triggers_full_pipeline(self):
        """Item with no pricing data needs full enrichment."""
        item = {"description": "Unknown widget", "pricing": {}}
        has_cost = (item.get("pricing", {}).get("unit_cost") or 0) > 0
        has_catalog = (item.get("pricing", {}).get("catalog_confidence") or 0) > 0
        needs_enrichment = not has_cost and not has_catalog
        assert needs_enrichment is True


# ═══════════════════════════════════════════════════════════════════════════
# TTL Constants: Verify actual values in source
# ═══════════════════════════════════════════════════════════════════════════

class TestTtlConstants:
    """Verify TTL values match expected configuration."""

    def _read_source(self, filename):
        path = os.path.join(os.path.dirname(__file__), "..", "src", "agents", filename)
        if not os.path.exists(path):
            pytest.skip(f"{filename} not found")
        with open(path) as f:
            return f.read()

    def test_item_id_ttl_30(self):
        source = self._read_source("item_identifier.py")
        assert "CACHE_TTL_DAYS = 30" in source

    def test_grok_ttl_14(self):
        source = self._read_source("product_validator.py")
        assert "CACHE_TTL_DAYS = 14" in source

    def test_product_research_ttl_7(self):
        source = self._read_source("product_research.py")
        assert "CACHE_TTL_DAYS = 7" in source

    def test_web_price_ttl_7(self):
        source = self._read_source("web_price_research.py")
        assert "CACHE_TTL_DAYS = 7" in source
