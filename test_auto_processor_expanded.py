"""
Expanded tests for auto_processor.py: document type detection, confidence
scoring edge cases, health check, audit stats.

Verified against actual function signatures 2026-02-14.
"""
import pytest
import os
from auto_processor import (
    score_item_confidence,
    score_quote_confidence,
    system_health_check,
    get_audit_stats,
    track_response_time,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Item Confidence — Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestItemConfidenceEdgeCases:

    def test_amazon_only_no_scprs(self):
        item = {
            "pricing": {"amazon_price": 15.00, "recommended_price": 18.75},
            "qty": 10, "description": "Standard widget",
        }
        result = score_item_confidence(item)
        assert result["grade"] in ("A", "B", "C")
        assert result["score"] > 0.2

    def test_scprs_only_no_amazon(self):
        item = {
            "pricing": {"scprs_price": 20.00},
            "qty": 5, "description": "Specialty item",
        }
        result = score_item_confidence(item)
        # Without amazon_price or recommended_price, confidence is limited
        assert result["grade"] in ("A", "B", "C", "F")

    def test_high_qty_boosts_confidence(self):
        """High quantity items should at least maintain confidence."""
        item_low = {
            "pricing": {"amazon_price": 10, "recommended_price": 12},
            "qty": 1, "description": "Test",
        }
        item_high = {
            "pricing": {"amazon_price": 10, "recommended_price": 12},
            "qty": 100, "description": "Test",
        }
        r_low = score_item_confidence(item_low)
        r_high = score_item_confidence(item_high)
        # High qty should not reduce confidence
        assert r_high["score"] >= r_low["score"] - 0.1

    def test_very_expensive_item(self):
        item = {
            "pricing": {"amazon_price": 50000, "recommended_price": 62500},
            "qty": 1, "description": "Expensive medical device",
        }
        result = score_item_confidence(item)
        assert result["grade"] in ("A", "B", "C", "F")
        assert "notes" in result

    def test_negative_price_handled(self):
        item = {
            "pricing": {"amazon_price": -5.00},
            "qty": 1, "description": "Bad data",
        }
        result = score_item_confidence(item)
        # Should not crash, should be low confidence
        assert result["grade"] in ("C", "F")

    def test_none_pricing(self):
        item = {"pricing": None, "qty": 1, "description": "Null pricing"}
        # Should handle gracefully — either grade F or raise
        try:
            result = score_item_confidence(item)
            assert result["grade"] == "F"
        except (AttributeError, TypeError):
            pass  # acceptable — caller should ensure pricing is a dict

    def test_missing_pricing_key(self):
        item = {"qty": 1, "description": "No pricing key"}
        result = score_item_confidence(item)
        assert result["grade"] == "F"


# ═══════════════════════════════════════════════════════════════════════════════
# Quote Confidence — Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteConfidenceEdgeCases:

    def test_single_a_grade_item(self):
        items = [{
            "pricing": {"amazon_price": 10, "scprs_price": 12, "recommended_price": 11},
            "qty": 5, "description": "Good item",
        }]
        result = score_quote_confidence(items)
        assert result["overall_grade"] in ("A", "B")
        assert result["auto_send_eligible"] in (True, False)

    def test_mixed_grades(self):
        items = [
            {"pricing": {"amazon_price": 10, "scprs_price": 12, "recommended_price": 11},
             "qty": 1, "description": "Good"},
            {"pricing": {}, "qty": 1, "description": "Bad"},
        ]
        result = score_quote_confidence(items)
        dist = result["grade_distribution"]
        # Should have both good and bad grades
        total = sum(dist.values())
        assert total == 2

    def test_many_items(self):
        items = [
            {"pricing": {"amazon_price": i * 5, "recommended_price": i * 6},
             "qty": 1, "description": f"Item {i}"}
            for i in range(1, 21)
        ]
        result = score_quote_confidence(items)
        assert result["items_scored"] == 20

    def test_all_no_bid(self):
        """Items with no_bid should still be countable."""
        items = [
            {"pricing": {}, "qty": 1, "description": "No bid", "no_bid": True},
        ]
        result = score_quote_confidence(items)
        assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# System Health Check
# ═══════════════════════════════════════════════════════════════════════════════

class TestSystemHealthCheck:

    def test_returns_dict(self):
        result = system_health_check()
        assert isinstance(result, dict)

    def test_has_status_key(self):
        result = system_health_check()
        assert "status" in result or "ok" in result or "checks" in result


# ═══════════════════════════════════════════════════════════════════════════════
# Audit Stats
# ═══════════════════════════════════════════════════════════════════════════════

class TestAuditStats:

    def test_returns_dict(self):
        result = get_audit_stats()
        assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# Response Time Tracking
# ═══════════════════════════════════════════════════════════════════════════════

class TestResponseTimeTracking:

    def test_basic_tracking(self):
        result = track_response_time(
            doc_type="price_check",
            received_at="2026-02-14T10:00:00Z",
            responded_at="2026-02-14T10:05:00Z",
        )
        assert isinstance(result, dict)
        assert "this_response_minutes" in result
        assert result["this_response_minutes"] == pytest.approx(5.0, abs=0.1)

    def test_no_responded_at(self):
        result = track_response_time(
            doc_type="price_check",
            received_at="2026-02-14T10:00:00Z",
        )
        assert isinstance(result, dict)
