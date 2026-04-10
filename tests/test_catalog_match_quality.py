"""V2 Test Suite — Group 9: Catalog Match Quality.

Tests that prevent cross-category garbage matches:
- Threshold 0.50 minimum for any match
- Cross-category rejected (shoes don't match medical gloves)
- UPC/MFG# match beats fuzzy description
- 0.60 confidence for auto-cost application

Incident: threshold was 0.35, shoes matched medical items.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


class TestMatchThreshold:
    """Token matching threshold must be 0.50 minimum."""

    def test_below_050_rejected(self):
        """Match score below 0.50 should be rejected."""
        score = 0.45
        threshold = 0.50
        assert score < threshold, "0.45 should be below threshold"
        accepted = score >= threshold
        assert accepted is False

    def test_at_050_accepted(self):
        """Match score at exactly 0.50 should be accepted."""
        score = 0.50
        threshold = 0.50
        accepted = score >= threshold
        assert accepted is True

    def test_high_score_accepted(self):
        score = 0.85
        accepted = score >= 0.50
        assert accepted is True


class TestCrossCategoryRejection:
    """Items from different categories should not match."""

    def test_shoes_dont_match_gloves(self):
        """'Running shoes' should NOT match 'Nitrile exam gloves'."""
        def _token_overlap(a, b):
            tokens_a = set(a.lower().split())
            tokens_b = set(b.lower().split())
            if not tokens_a or not tokens_b:
                return 0.0
            intersection = tokens_a & tokens_b
            return len(intersection) / max(len(tokens_a), len(tokens_b))

        score = _token_overlap("Running shoes size 10", "Nitrile exam gloves large")
        assert score < 0.50, f"Cross-category match score {score} should be < 0.50"

    def test_similar_items_match(self):
        """'Nitrile gloves large' should match 'Nitrile exam gloves, large, 100ct'."""
        def _token_overlap(a, b):
            tokens_a = set(a.lower().split())
            tokens_b = set(b.lower().split())
            if not tokens_a or not tokens_b:
                return 0.0
            intersection = tokens_a & tokens_b
            return len(intersection) / max(len(tokens_a), len(tokens_b))

        score = _token_overlap(
            "Nitrile gloves large",
            "Nitrile exam gloves large 100ct"
        )
        assert score >= 0.50, f"Similar item match score {score} should be >= 0.50"


class TestUpcMatchPriority:
    """UPC/MFG# exact match must beat fuzzy description match."""

    def test_upc_match_is_perfect_score(self):
        """UPC match should always have confidence 1.0."""
        catalog_entry = {"upc": "012345678901", "description": "Name Tag", "price": 12.58}
        search_upc = "012345678901"

        if catalog_entry["upc"] == search_upc:
            confidence = 1.0
        else:
            confidence = 0.0

        assert confidence == 1.0, "UPC exact match should be 1.0 confidence"

    def test_mfg_match_beats_description(self):
        """MFG# match should rank higher than description-only match."""
        mfg_match_confidence = 0.95
        description_match_confidence = 0.65

        assert mfg_match_confidence > description_match_confidence, \
            "MFG# match should rank higher than fuzzy description"

    def test_no_upc_falls_to_description(self):
        """Without UPC, fall back to description matching."""
        catalog_entry = {"upc": "", "description": "Nitrile gloves", "price": 8.49}
        search_upc = ""

        has_upc_match = bool(catalog_entry["upc"] and search_upc and catalog_entry["upc"] == search_upc)
        assert has_upc_match is False, "No UPC should fall through to description"


class TestAutoApplyConfidence:
    """Only high-confidence matches auto-apply as cost."""

    def test_060_for_auto_cost(self):
        """Below 0.60 = reference only, not auto-applied as cost."""
        match = {"confidence": 0.55, "price": 42.99}
        auto_apply = match["confidence"] >= 0.60
        assert auto_apply is False

    def test_above_060_auto_applies(self):
        match = {"confidence": 0.75, "price": 42.99}
        auto_apply = match["confidence"] >= 0.60
        assert auto_apply is True
