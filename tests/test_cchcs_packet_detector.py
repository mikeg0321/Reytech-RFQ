"""Tests for the CCHCS packet detection helper.

Built 2026-04-13 overnight. Phase 5 of the CCHCS automation.
"""
import pytest

from src.agents.cchcs_packet_detector import (
    tag_pc_if_packet,
    backfill_existing_pcs,
    PACKET_TYPE_CCHCS,
)


class TestTagPcIfPacket:
    def test_tags_by_filename(self):
        pc = {
            "id": "pc_1",
            "source_pdf": "/data/uploads/Non-Cloud RFQ Packet 12.3.25 - PREQ10843276.pdf",
            "email_subject": "scanner stuff",
        }
        assert tag_pc_if_packet(pc) is True
        assert pc["packet_type"] == PACKET_TYPE_CCHCS

    def test_tags_by_subject(self):
        pc = {
            "id": "pc_2",
            "source_pdf": "/data/uploads/some_random.pdf",
            "email_subject": "PREQ10843276 Quote Request",
        }
        assert tag_pc_if_packet(pc) is True
        assert pc["packet_type"] == PACKET_TYPE_CCHCS

    def test_tags_by_attachment_filename(self):
        pc = {
            "id": "pc_3",
            "source_pdf": "",
            "email_subject": "",
            "attachments": [
                {"filename": "RFQ Packet PREQ12345.pdf", "path": "/tmp/x.pdf"},
            ],
        }
        assert tag_pc_if_packet(pc) is True

    def test_does_not_tag_standard_704(self):
        pc = {
            "id": "pc_4",
            "source_pdf": "/data/uploads/AMS 704 Office Supplies.pdf",
            "email_subject": "Price Check Worksheet",
        }
        assert tag_pc_if_packet(pc) is False
        assert "packet_type" not in pc

    def test_idempotent_second_call(self):
        pc = {
            "id": "pc_5",
            "source_pdf": "/x/Non-Cloud RFQ Packet PREQ12345.pdf",
        }
        assert tag_pc_if_packet(pc) is True
        # Second call should still return True without re-tagging
        assert tag_pc_if_packet(pc) is True
        assert pc["packet_type"] == PACKET_TYPE_CCHCS

    def test_handles_empty_pc(self):
        pc = {"id": "pc_empty"}
        assert tag_pc_if_packet(pc) is False

    def test_handles_non_dict(self):
        assert tag_pc_if_packet(None) is False
        assert tag_pc_if_packet("not a dict") is False
        assert tag_pc_if_packet(42) is False


class TestBackfillExistingPcs:
    def test_walks_dict_and_summarizes(self):
        pcs = {
            "pc_a": {
                "id": "pc_a",
                "source_pdf": "/x/RFQ Packet PREQ10001.pdf",
            },
            "pc_b": {
                "id": "pc_b",
                "source_pdf": "/x/AMS 704 normal.pdf",
            },
            "pc_c": {
                "id": "pc_c",
                "source_pdf": "/x/Non-Cloud RFQ Packet PREQ20002.pdf",
                "packet_type": PACKET_TYPE_CCHCS,  # already tagged
            },
            "pc_d": {
                "id": "pc_d",
                "email_subject": "PREQ30003 Quote Request",
            },
        }
        summary = backfill_existing_pcs(pcs)
        assert summary["total"] == 4
        assert summary["tagged_now"] == 2  # pc_a + pc_d
        assert summary["already_tagged"] == 1  # pc_c
        assert summary["not_packet"] == 1  # pc_b
        assert "pc_a" in summary["tagged_ids"]
        assert "pc_d" in summary["tagged_ids"]
        # Verify the actual dict was mutated
        assert pcs["pc_a"]["packet_type"] == PACKET_TYPE_CCHCS
        assert pcs["pc_d"]["packet_type"] == PACKET_TYPE_CCHCS
        assert "packet_type" not in pcs["pc_b"]

    def test_empty_dict(self):
        assert backfill_existing_pcs({}) == {
            "total": 0, "tagged_now": 0, "already_tagged": 0,
            "not_packet": 0, "tagged_ids": []
        }

    def test_skips_non_dict_entries(self):
        pcs = {
            "pc_real": {"id": "pc_real", "source_pdf": "/x/PREQ12345.pdf",
                        "email_subject": "PREQ12345 quote"},
            "pc_garbage": "not a dict",
            "pc_none": None,
        }
        summary = backfill_existing_pcs(pcs)
        assert summary["total"] == 1  # only pc_real is a dict
