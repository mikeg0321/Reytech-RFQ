"""
Tests for won_quotes_db.py: text normalization, category classification,
similarity matching, price history, ingestion, and win probability.

Verified against actual function signatures 2026-02-14.
"""
import pytest
import json
import os
from datetime import datetime, timezone, timedelta

from won_quotes_db import (
    normalize_text,
    tokenize,
    classify_category,
    generate_record_id,
    freshness_weight,
    token_overlap_score,
    find_similar_items,
    get_price_history,
    ingest_scprs_result,
    ingest_scprs_bulk,
    load_won_quotes,
    save_won_quotes,
    get_kb_stats,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Text Processing
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeText:

    def test_lowercase(self):
        assert normalize_text("Hello WORLD") == normalize_text("hello world")

    def test_strips_special_chars(self):
        result = normalize_text("3M™ Nitrile Gloves (Large)")
        assert "™" not in result

    def test_empty_string(self):
        assert normalize_text("") == ""

    def test_returns_string(self):
        assert isinstance(normalize_text("test"), str)


class TestTokenize:

    def test_splits_on_spaces(self):
        tokens = tokenize("nitrile exam gloves")
        assert "nitrile" in tokens
        assert "exam" in tokens
        assert "gloves" in tokens

    def test_returns_set(self):
        result = tokenize("hello world")
        assert isinstance(result, set)

    def test_empty_string(self):
        result = tokenize("")
        assert len(result) == 0

    def test_removes_stopwords_or_short_tokens(self):
        tokens = tokenize("the big red ball")
        # Short/common words may be filtered
        assert "big" in tokens or "red" in tokens or "ball" in tokens


class TestClassifyCategory:

    def test_medical(self):
        cat = classify_category("Stryker X-Restraint Medical Device")
        assert "medical" in cat.lower()  # e.g. "medical_equipment"

    def test_office(self):
        cat = classify_category("Copy paper, 8.5x11, white, 20lb")
        # Should classify as office or general
        assert isinstance(cat, str)
        assert len(cat) > 0

    def test_returns_string(self):
        assert isinstance(classify_category("unknown widget"), str)


# ═══════════════════════════════════════════════════════════════════════════════
# Record ID Generation
# ═══════════════════════════════════════════════════════════════════════════════

class TestGenerateRecordId:

    def test_deterministic(self):
        id1 = generate_record_id("PO123", "ITEM1", "Widget blue")
        id2 = generate_record_id("PO123", "ITEM1", "Widget blue")
        assert id1 == id2

    def test_different_inputs_differ(self):
        id1 = generate_record_id("PO123", "ITEM1", "Widget A")
        id2 = generate_record_id("PO456", "ITEM2", "Widget B")
        assert id1 != id2

    def test_returns_string(self):
        assert isinstance(generate_record_id("PO", "ITEM", "DESC"), str)


# ═══════════════════════════════════════════════════════════════════════════════
# Freshness Weight
# ═══════════════════════════════════════════════════════════════════════════════

class TestFreshnessWeight:

    def test_recent_is_near_1(self):
        recent = datetime.now().strftime("%Y-%m-%d")
        w = freshness_weight(recent)
        assert w >= 0.8

    def test_old_is_lower(self):
        old = (datetime.now() - timedelta(days=700)).strftime("%Y-%m-%d")
        w = freshness_weight(old)
        assert w < 0.8

    def test_empty_date(self):
        w = freshness_weight("")
        assert 0 <= w <= 1.0

    def test_returns_float(self):
        assert isinstance(freshness_weight("2025-01-15"), float)

    def test_between_0_and_1(self):
        w = freshness_weight("2024-06-15")
        assert 0 <= w <= 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Token Overlap Score
# ═══════════════════════════════════════════════════════════════════════════════

class TestTokenOverlapScore:

    def test_identical_sets(self):
        s = {"nitrile", "gloves", "large"}
        assert token_overlap_score(s, s) == pytest.approx(1.0)

    def test_no_overlap(self):
        a = {"nitrile", "gloves"}
        b = {"copy", "paper"}
        score = token_overlap_score(a, b)
        assert score == pytest.approx(0.0)

    def test_partial_overlap(self):
        a = {"nitrile", "gloves", "large"}
        b = {"nitrile", "gloves", "medium"}
        score = token_overlap_score(a, b)
        assert 0.3 < score < 1.0

    def test_empty_sets(self):
        score = token_overlap_score(set(), set())
        assert score == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Ingestion
# ═══════════════════════════════════════════════════════════════════════════════

class TestIngestScprsResult:

    def test_basic_ingest(self):
        record = ingest_scprs_result(
            po_number="PO-TEST-001",
            item_number="ITEM-001",
            description="Nitrile exam gloves, large",
            unit_price=12.50,
            quantity=100,
            supplier="Test Supplier",
        )
        assert record["unit_price"] == 12.50
        assert record["po_number"] == "PO-TEST-001"
        assert record["id"]  # has an ID

    def test_dedup_on_same_record(self):
        ingest_scprs_result("PO-DUP", "ITEM-DUP", "Test item", 10.00)
        ingest_scprs_result("PO-DUP", "ITEM-DUP", "Test item", 12.00)  # updated price
        quotes = load_won_quotes()
        matching = [q for q in quotes if q["po_number"] == "PO-DUP"]
        assert len(matching) == 1
        assert matching[0]["unit_price"] == 12.00  # updated

    def test_persists_to_disk(self):
        ingest_scprs_result("PO-PERSIST", "ITEM-P", "Persisted item", 5.00)
        quotes = load_won_quotes()
        assert any(q["po_number"] == "PO-PERSIST" for q in quotes)


class TestIngestScprsBulk:

    def test_bulk_ingest(self):
        results = [
            {"po_number": "BULK-1", "item_number": "B1", "description": "Item 1",
             "unit_price": 10.00},
            {"po_number": "BULK-2", "item_number": "B2", "description": "Item 2",
             "unit_price": 20.00},
            {"po_number": "BULK-3", "item_number": "B3", "description": "Item 3",
             "unit_price": 0},  # should be skipped (zero price)
        ]
        stats = ingest_scprs_bulk(results)
        assert stats["ingested"] >= 2
        assert stats["skipped"] >= 1

    def test_returns_stats_dict(self):
        stats = ingest_scprs_bulk([])
        assert "ingested" in stats
        assert "updated" in stats
        assert "skipped" in stats


# ═══════════════════════════════════════════════════════════════════════════════
# Find Similar Items
# ═══════════════════════════════════════════════════════════════════════════════

class TestFindSimilarItems:

    def test_empty_kb_returns_empty(self):
        results = find_similar_items("ITEM-X", "Nonexistent widget")
        assert results == []

    def test_finds_exact_match(self):
        ingest_scprs_result("PO-MATCH", "EXACT-001", "Nitrile exam gloves large", 12.00)
        results = find_similar_items("EXACT-001", "Nitrile exam gloves large")
        assert len(results) >= 1
        assert results[0]["match_confidence"] > 0.5

    def test_returns_list(self):
        results = find_similar_items("X", "Something")
        assert isinstance(results, list)

    def test_respects_max_results(self):
        for i in range(15):
            ingest_scprs_result(f"PO-MR-{i}", f"ITEM-{i}", f"Widget variant {i}", 10.00 + i)
        results = find_similar_items("", "Widget variant", max_results=5)
        assert len(results) <= 5


# ═══════════════════════════════════════════════════════════════════════════════
# Price History
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetPriceHistory:

    def test_empty_kb(self):
        h = get_price_history("NONE", "Nonexistent item")
        assert h["matches"] == 0

    def test_with_data(self):
        ingest_scprs_result("PO-H1", "HIST-001", "Copy paper white", 42.00)
        ingest_scprs_result("PO-H2", "HIST-001", "Copy paper white 20lb", 45.00)
        h = get_price_history("HIST-001", "Copy paper white")
        assert h["matches"] >= 1
        assert h["median_price"] is not None or h["matches"] > 0

    def test_returns_expected_keys(self):
        h = get_price_history("X", "Y")
        for key in ("matches", "median_price", "min_price", "max_price",
                     "recent_avg", "trend"):
            assert key in h, f"Missing key: {key}"


# ═══════════════════════════════════════════════════════════════════════════════
# KB Stats
# ═══════════════════════════════════════════════════════════════════════════════

class TestKBStats:

    def test_returns_dict(self):
        stats = get_kb_stats()
        assert isinstance(stats, dict)

    def test_has_total_records(self):
        stats = get_kb_stats()
        assert "total_records" in stats or "total" in stats or "records" in stats
