"""Tests for the CCHCS Non-IT RFQ Packet parser.

These tests run against the real Apr 2026 sample packet (PREQ10843276)
stashed in _overnight_review/source_packet.pdf. The packet has:

- 18 pages, fully fillable, 183 form fields, 25 pre-filled by buyer
- Buyer header: sol# 10843276, CA State Prison Sacramento, Folsom,
  Ashley.Russ@cdcr.ca.gov
- 1 line item (of 10 supported): DS8178 Symbol handheld scanner,
  qty 15, unit "sets"

Built overnight 2026-04-13 as part of the autonomous CCHCS packet
automation. See _overnight_review/MORNING_REVIEW.md.
"""
import os

import pytest

from src.forms.cchcs_packet_parser import (
    parse_cchcs_packet,
    looks_like_cchcs_packet,
    HEADER_FIELDS,
    LINE_ITEM_TEMPLATES,
    SUPPLIER_FIELDS,
    TOTALS_FIELDS,
    MAX_ROWS,
)


SAMPLE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "_overnight_review",
    "source_packet.pdf",
)


@pytest.fixture
def parsed():
    if not os.path.exists(SAMPLE):
        pytest.skip("_overnight_review/source_packet.pdf not present (dev fixture)")
    return parse_cchcs_packet(SAMPLE)


class TestPacketDetection:
    """looks_like_cchcs_packet is the cheap pre-check used by the poller."""

    def test_detects_filename(self):
        assert looks_like_cchcs_packet(
            filename="Non-Cloud RFQ Packet 12.3.25 - PREQ10843276.pdf"
        )

    def test_detects_subject(self):
        assert looks_like_cchcs_packet(
            subject="PREQ10843276 Quote Request for Scanners to Folsom"
        )

    def test_detects_rfq_packet_variant(self):
        assert looks_like_cchcs_packet(filename="RFQ Packet - 10843276.pdf")

    def test_ignores_plain_704(self):
        assert not looks_like_cchcs_packet(
            filename="AMS 704 - Office Supplies.pdf",
            subject="Price Check Worksheet",
        )

    def test_ignores_empty_inputs(self):
        assert not looks_like_cchcs_packet(filename="", subject="")


class TestHeaderExtraction:
    def test_ok_and_packet_type(self, parsed):
        assert parsed["ok"] is True
        assert parsed["packet_type"] == "cchcs_non_it"

    def test_solicitation_number(self, parsed):
        assert parsed["header"]["solicitation_number"] == "10843276"
        # pc_number alias mirrors sol# so downstream code can use either
        assert parsed["header"]["pc_number"] == "10843276"

    def test_institution(self, parsed):
        assert "Sacramento" in parsed["header"]["institution"]

    def test_requestor_email(self, parsed):
        assert parsed["header"]["requestor_email"] == "Ashley.Russ@cdcr.ca.gov"

    def test_due_date(self, parsed):
        assert parsed["header"]["due_date"]  # buyer filled it

    def test_zip_code_extracted(self, parsed):
        assert parsed["header"]["zip_code"] == "95671"

    def test_agency_normalized(self, parsed):
        # CCHCS quotes route under CDCR per agency_config
        assert parsed["header"]["agency"] == "CDCR"


class TestLineItemExtraction:
    def test_extracted_one_item(self, parsed):
        # Buyer filled exactly 1 line item (DS8178)
        assert len(parsed["line_items"]) == 1

    def test_item_has_mfg_number(self, parsed):
        item = parsed["line_items"][0]
        assert item["mfg_number"] == "DS8178"
        assert item["part_number"] == "DS8178"

    def test_item_has_qty(self, parsed):
        assert parsed["line_items"][0]["qty"] == 15

    def test_item_has_uom(self, parsed):
        assert parsed["line_items"][0]["uom"] == "SETS"

    def test_item_has_description(self, parsed):
        desc = parsed["line_items"][0]["description"].lower()
        assert "scanner" in desc
        assert "usb" in desc

    def test_row_index_and_item_number(self, parsed):
        item = parsed["line_items"][0]
        assert item["row_index"] == 1
        assert item["item_number"] == "1"

    def test_pricing_shape_compatible_with_pc_schema(self, parsed):
        item = parsed["line_items"][0]
        assert "pricing" in item
        assert "unit_cost" in item["pricing"]


class TestSupplierCurrentState:
    def test_supplier_info_empty_initially(self, parsed):
        # Before we fill, supplier fields should all be blank
        sc = parsed["supplier_current"]
        assert sc["company_name"] == ""
        assert sc["email"] == ""
        assert sc["phone"] == ""


class TestParseQuality:
    def test_score_is_high(self, parsed):
        # Buyer filled header + 1 item = should be grade A or B
        assert parsed["parse_quality"]["score"] >= 90
        assert parsed["parse_quality"]["grade"] in ("A", "B")

    def test_item_counts_match(self, parsed):
        pq = parsed["parse_quality"]
        assert pq["parsed_items"] == pq["expected_items"] == 1


class TestErrorPaths:
    def test_missing_file(self, tmp_path):
        r = parse_cchcs_packet(str(tmp_path / "does_not_exist.pdf"))
        assert r["ok"] is False
        assert "not found" in r["error"]

    def test_non_pdf(self, tmp_path):
        f = tmp_path / "not_a_pdf.pdf"
        f.write_text("this is not a pdf at all")
        r = parse_cchcs_packet(str(f))
        assert r["ok"] is False
        assert "error" in r


class TestSchemaConstants:
    """Catch regressions in the field-name constants the filler depends on."""

    def test_line_item_templates_have_all_slots(self):
        expected = {"qty", "uom", "description", "mfg_number",
                    "price_per_unit", "extension"}
        assert set(LINE_ITEM_TEMPLATES.keys()) == expected

    def test_max_rows_is_ten(self):
        assert MAX_ROWS == 10

    def test_header_fields_include_solicitation(self):
        assert "solicitation_number" in HEADER_FIELDS
        assert HEADER_FIELDS["solicitation_number"] == "Solicitation No"

    def test_supplier_fields_include_signature(self):
        assert "signature" in SUPPLIER_FIELDS

    def test_totals_fields_present(self):
        assert set(TOTALS_FIELDS.keys()) == {"subtotal", "freight", "sales_tax", "grand_total"}
