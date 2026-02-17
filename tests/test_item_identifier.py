"""Tests for the Item Identification Agent."""

import pytest
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agents.item_identifier import (
    identify_item, identify_pc_items, _clean_description,
    _generate_search_terms, _detect_category, get_agent_status,
    _identify_rule_based,
)


class TestCleanDescription:
    def test_strips_whitespace(self):
        assert _clean_description("  hello world  ") == "hello world"

    def test_removes_embedded_quantities(self):
        result = _clean_description("50 BX Nitrile Gloves Large")
        assert "50 BX" not in result
        assert "Nitrile Gloves" in result

    def test_removes_multiple_qty_patterns(self):
        result = _clean_description("22 EA Engraved name tag 10 PK refills")
        assert "22 EA" not in result
        assert "10 PK" not in result

    def test_preserves_meaningful_content(self):
        result = _clean_description("Engraved two line name tag, black/white")
        assert "Engraved" in result
        assert "name tag" in result


class TestGenerateSearchTerms:
    def test_returns_list(self):
        terms = _generate_search_terms("Nitrile exam gloves, large, blue")
        assert isinstance(terms, list)
        assert len(terms) >= 1

    def test_removes_noise_words(self):
        terms = _generate_search_terms("Please provide the following items as needed")
        for term in terms:
            assert "please" not in term.lower()
            assert "provide" not in term.lower()

    def test_short_description(self):
        terms = _generate_search_terms("Pens")
        assert len(terms) >= 1
        assert "Pens" in terms[0]

    def test_long_description_truncated(self):
        terms = _generate_search_terms(
            "Heavy duty industrial grade stainless steel commercial kitchen "
            "grade wire shelving unit with adjustable shelves and casters"
        )
        # Should have a truncated version
        assert len(terms) >= 2


class TestDetectCategory:
    def test_medical_items(self):
        result = _detect_category("Nitrile exam gloves, large, blue")
        assert result["category"] == "medical"
        assert result["confidence"] > 0.5

    def test_office_items(self):
        result = _detect_category("Engraved two line name tag, black/white")
        assert result["category"] == "office"

    def test_janitorial_items(self):
        result = _detect_category("Industrial floor cleaner, 1 gallon")
        assert result["category"] == "janitorial"

    def test_unknown_items(self):
        result = _detect_category("Quantum flux capacitor assembly")
        assert result["category"] == "general"
        assert result["confidence"] < 0.5

    def test_multiple_category_match(self):
        # "soap" is janitorial, should have all_matches
        result = _detect_category("antibacterial hand soap dispenser wall mount")
        assert "all_matches" in result

    def test_safety_items(self):
        result = _detect_category("Hi-vis safety vest reflective")
        assert result["category"] == "safety"


class TestIdentifyItem:
    def test_basic_identification(self):
        result = identify_item("Nitrile exam gloves, large, blue")
        assert result["method"] == "rule_based"  # No API key in tests
        assert result["search_terms"]
        assert result["category"]["category"] == "medical"
        assert result["clean_description"]

    def test_empty_description(self):
        result = identify_item("")
        assert result["method"] == "none"
        assert "error" in result

    def test_caching(self):
        import time
        unique = f"Test item for caching {time.time()}"
        # First call
        r1 = identify_item(unique)
        assert not r1.get("from_cache")
        # Second call should be cached
        r2 = identify_item(unique)
        assert r2.get("from_cache") is True

    def test_name_tag_identification(self):
        result = identify_item("Engraved two line name tag, black/white", qty=22, uom="EA")
        assert result["search_terms"]
        assert result["category"]["category"] == "office"
        assert "name tag" in result["primary_search"].lower() or "engraved" in result["primary_search"].lower()


class TestIdentifyPcItems:
    def test_identifies_multiple_items(self):
        items = [
            {"description": "Nitrile exam gloves, large", "qty": 50, "uom": "BX"},
            {"description": "Hand sanitizer 8oz pump", "qty": 100, "uom": "EA"},
            {"description": "Stryker restraint package", "qty": 10, "uom": "KT"},
        ]
        results = identify_pc_items(items)
        assert len(results) == 3
        for item in results:
            assert "identification" in item
            assert "_search_query" in item
            assert "_category" in item

    def test_empty_items_list(self):
        results = identify_pc_items([])
        assert results == []


class TestAgentStatus:
    def test_returns_status(self):
        status = get_agent_status()
        assert status["agent"] == "item_identifier"
        assert status["version"] == "1.0.0"
        assert "mode" in status
        assert "categories" in status
        assert isinstance(status["categories"], list)
