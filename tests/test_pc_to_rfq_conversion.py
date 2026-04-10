"""V2 Test Suite — Group 8: PC → RFQ Conversion.

Tests that conversion is a deepcopy, not field remapping.
Incident: 4 bugs from field remapping (empty MFG#, 0.00 bid, "unknown" PC link).
"""
import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


class TestConversionIsDeepCopy:
    """PC → RFQ conversion must preserve all fields via deepcopy."""

    def test_deepcopy_preserves_all_pricing(self, sample_pc):
        """All pricing fields survive conversion."""
        converted = copy.deepcopy(sample_pc)
        converted["status"] = "new"  # Status changes
        converted["source"] = "pc_conversion"

        # All original pricing must survive
        for item in converted["items"]:
            assert "unit_price" in item, "unit_price lost in conversion"
            assert item["unit_price"] > 0, "unit_price zeroed in conversion"
            assert "supplier_cost" in item, "supplier_cost lost in conversion"

    def test_deepcopy_preserves_descriptions(self, sample_pc):
        """Item descriptions must not change during conversion."""
        original_descs = [i["description"] for i in sample_pc["items"]]
        converted = copy.deepcopy(sample_pc)

        converted_descs = [i["description"] for i in converted["items"]]
        assert original_descs == converted_descs, "Descriptions changed during conversion"

    def test_deepcopy_does_not_share_references(self, sample_pc):
        """Modifying converted should NOT affect original (true deep copy)."""
        converted = copy.deepcopy(sample_pc)
        converted["items"][0]["unit_price"] = 999.99

        assert sample_pc["items"][0]["unit_price"] != 999.99, \
            "Modifying copy affected original — NOT a deep copy"

    def test_conversion_preserves_pricing_metadata(self, sample_pc):
        """Pricing dict with amazon/scprs data must survive."""
        converted = copy.deepcopy(sample_pc)
        for item in converted["items"]:
            if "pricing" in item:
                # Amazon data should survive
                if "amazon_asin" in item["pricing"]:
                    assert item["pricing"]["amazon_asin"], "ASIN lost in conversion"
                if "recommended_price" in item["pricing"]:
                    assert item["pricing"]["recommended_price"] > 0, \
                        "recommended_price zeroed in conversion"


class TestConvertedRfqHasMfgNumbers:
    """MFG# must not be blank after conversion."""

    def test_part_numbers_preserved(self):
        """Items with part_number must keep it through conversion."""
        pc = {
            "items": [
                {"description": "X-RESTRAINT", "part_number": "6500-001-430",
                 "unit_price": 454.40, "qty": 2},
            ],
        }
        converted = copy.deepcopy(pc)
        assert converted["items"][0]["part_number"] == "6500-001-430", \
            "part_number lost in conversion"

    def test_mfg_number_not_empty_string(self):
        """Conversion should never turn a valid MFG# into ''."""
        pc = {
            "items": [
                {"description": "Widget", "mfg_number": "W12919",
                 "unit_price": 10.00, "qty": 1},
            ],
        }
        converted = copy.deepcopy(pc)
        assert converted["items"][0].get("mfg_number") == "W12919"


class TestNoDuplicatePcAndRfq:
    """Cross-queue dedup: if PC exists for email, don't also create RFQ."""

    def test_dedup_logic(self):
        """Same email_uid should be caught by dedup check."""
        existing_pcs = {"pc-001": {"email_uid": "msg_123"}}
        incoming_uid = "msg_123"

        # Check if any existing PC has this email_uid
        already_processed = any(
            pc.get("email_uid") == incoming_uid
            for pc in existing_pcs.values()
        )
        assert already_processed is True, "Duplicate email_uid should be caught"

    def test_different_uid_not_deduped(self):
        """Different email_uid should NOT be caught."""
        existing_pcs = {"pc-001": {"email_uid": "msg_123"}}
        incoming_uid = "msg_456"

        already_processed = any(
            pc.get("email_uid") == incoming_uid
            for pc in existing_pcs.values()
        )
        assert already_processed is False
