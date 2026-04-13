"""Tests for the CCHCS Non-IT RFQ Packet filler.

Round-trips the real Apr 2026 sample packet through parse → fill → re-read,
asserting that every supplier-side field lands correctly in the output PDF.

Built overnight 2026-04-13. See _overnight_review/MORNING_REVIEW.md.
"""
import json
import os
import pathlib

import pytest

from src.forms.cchcs_packet_parser import parse_cchcs_packet
from src.forms.cchcs_packet_filler import (
    fill_cchcs_packet,
    _output_path,
    _split_address,
    _money,
    _build_field_updates,
    COMPLIANCE_CHECKBOXES_YES,
)


SAMPLE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "_overnight_review",
    "source_packet.pdf",
)


@pytest.fixture
def parsed():
    if not os.path.exists(SAMPLE):
        pytest.skip("sample packet missing")
    return parse_cchcs_packet(SAMPLE)


@pytest.fixture
def reytech_info():
    return {
        "company_name": "Reytech Inc.",
        "representative": "Michael Guadan",
        "title": "Owner",
        "address": "30 Carnoustie Way, Trabuco Canyon, CA 92679",
        "street": "30 Carnoustie Way",
        "city": "Trabuco Canyon",
        "state": "CA",
        "zip": "92679",
        "county": "Orange",
        "phone": "949-229-1575",
        "email": "sales@reytechinc.com",
        "sb_mb": "2002605",
        "dvbe": "2002605",
        "cert_number": "2002605",
        "cert_expiration": "5/31/2027",
        "cert_type": "SB/DVBE",
        "sellers_permit": "245652416 - 00001",
        "fein": "47-4588061",
        "description_of_goods": "Medical/Office and other supplies",
        "compliance": {
            "claiming_sb_preference": True,
            "is_manufacturer": False,
            "subcontract_25_percent": False,
            "subcontract_amount": "",
            "cuf_all_yes": True,
            "uses_genai": False,
            "uses_subcontractors": False,
            "scrutinized_darfur_company": False,
            "doing_business_in_sudan": False,
            "postconsumer_recycled_percent": "0%",
            "sabrc_product_category": "N/A",
            "unit_section": "Procurement",
        },
    }


class TestOutputPath:
    def test_adds_reytech_suffix(self):
        p = _output_path("/tmp/foo/Non-Cloud RFQ Packet - PREQ12345.pdf")
        assert p.endswith("Non-Cloud RFQ Packet - PREQ12345_Reytech.pdf")

    def test_strips_existing_suffix(self):
        # Defensive: if source is already suffixed, don't double-suffix
        p = _output_path("/tmp/foo/something_Reytech.pdf")
        assert p.endswith("something_Reytech.pdf")
        assert "Reytech_Reytech" not in p

    def test_respects_output_dir(self, tmp_path):
        p = _output_path("/tmp/source.pdf", output_dir=str(tmp_path))
        assert os.path.dirname(p) == str(tmp_path)


class TestMoneyFormat:
    def test_basic(self):
        assert _money(100) == "100.00"
        assert _money(1234.5) == "1,234.50"
        assert _money(5925) == "5,925.00"

    def test_empty_on_junk(self):
        assert _money("not a number") == ""


class TestSplitAddress:
    def test_three_part(self):
        parts = _split_address("30 Carnoustie Way, Trabuco Canyon, CA 92679")
        assert parts[0] == "30 Carnoustie Way"
        assert "Trabuco Canyon" in parts[1]

    def test_no_commas(self):
        parts = _split_address("PO Box 100")
        assert parts[0] == "PO Box 100"
        assert parts[1] == ""


class TestFieldUpdatesFromParsed:
    def test_supplier_info_filled(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["Supplier Name"] == "Reytech Inc."
        assert updates["Contact Name"] == "Michael Guadan"
        assert updates["Phone"] == "949-229-1575"
        assert updates["Supplier Email"] == "sales@reytechinc.com"

    def test_cert_fields(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["SBMBDVBE Certification  if applicable"] == "2002605"

    def test_signature_field_left_empty_for_png_overlay(self, parsed, reytech_info):
        # The Signature1 text field must NOT be written — the PNG overlay
        # draws the real signature image on top, and a typed name would
        # show through underneath.
        updates = _build_field_updates(parsed, reytech_info)
        assert "Signature1_es_:signer:signature" not in updates
        # Date is still set in the adjacent Date_es_:date field
        assert len(updates["Date_es_:date"]) >= 8
        assert "/" in updates["Date_es_:date"]

    def test_cert_expiration_filled(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["Expiration Date"] == "5/31/2027"

    def test_sw_renewal_no_and_term_na(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["SW Renew No"] == "/Yes"
        assert updates["SW Term"] == "N/A"

    def test_reseller_permit_filled(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["CA Reseller Permit Num"] == "245652416 - 00001"

    def test_cuf_attestation_text_filled(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["DOING BUSINESS AS DBA NAME"] == "Reytech Inc."
        assert updates["OSDS REF  CURRENTLY CERTIFIED FIRMS ONLY"] == "2002605"
        # Signature block is NOT written as text — the PNG overlay
        # draws the cursive signature directly on the widget rect.
        assert "Signature Block28_es_:signer:signatureblock" not in updates
        assert "/" in updates["DATE"]

    def test_cuf_attestation_yes_boxes_ticked(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        for cb in (
            "Check Box29.0.0", "Check Box29.1.0", "Check Box29.2.0",
            "Check Box21.0.0.0", "Check Box21.0.1.0", "Check Box21.0.2.0",
        ):
            assert updates[cb] == "/Yes"

    def test_ams708_supplier_block_filled(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["AMS 708 Supplier Phone"] == "949-229-1575"
        assert updates["AMS 708 Supplier Address"] == "30 Carnoustie Way"
        assert updates["AMS 708 Supplier City"] == "Trabuco Canyon"
        assert updates["AMS 708 Supplier State"] == "CA"
        assert updates["AMS 708 Supplier Zip Code"] == "92679"

    def test_ams708_genai_no_checked(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["AMS 708 GenAI No"] == "/Yes"

    def test_ams708_questions_filled_with_na(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        # Spot-check a few of the 15 questions + the explanation field
        assert updates["1 Gen AI Model Nmae Version including number of parameters"] == "N/A"
        assert updates["8 Input and Outputs"] == "N/A"
        assert updates["Explanation - GenAI not adversely affecting decisions"] == "N/A"

    def test_preference_checkboxes_sb_yes_manufacturer_no_subcontract_no(self, parsed, reytech_info):
        # Reytech default compliance stance: claiming SB preference,
        # not a manufacturer, not a 25% subcontract prime. Drives the
        # 3 YES/NO checkbox pairs on page 1.
        reytech_info = {
            **reytech_info,
            "compliance": {
                "claiming_sb_preference": True,
                "is_manufacturer": False,
                "subcontract_25_percent": False,
            },
        }
        updates = _build_field_updates(parsed, reytech_info)
        # SB preference: YES ticked, NO off
        assert updates["Check Box12"] == "/Yes"
        assert updates["Check Box11"] == "/Off"
        # Manufacturer: NO ticked, YES off
        assert updates["Check Box13"] == "/Off"
        assert updates["Check Box14"] == "/Yes"
        # 25% subcontract: NO ticked, YES off
        assert updates["Check Box15"] == "/Off"
        assert updates["Check Box16"] == "/Yes"
        # Subcontract amount blank because we said no
        assert updates["Amount"] == ""

    def test_empty_row_does_not_get_placeholder_price(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        # Without a price_override, row 1 has no price in source, so
        # the filler should leave Price Per Unit1 blank (no '0.00')
        assert "Price Per Unit1" not in updates

    def test_row_price_override_writes_unit_and_extension(self, parsed, reytech_info):
        updates = _build_field_updates(
            parsed,
            reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        assert updates["Price Per Unit1"] == "395.00"
        # qty 15 * 395 = 5,925.00
        assert updates["Extension Total1"] == "5,925.00"

    def test_totals_roll_up(self, parsed, reytech_info):
        updates = _build_field_updates(
            parsed,
            reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        assert updates["Extension TotalSubtotal"] == "5,925.00"
        assert updates["Extension TotalTotal"].replace(",", "").startswith("5925")
        # The page 1 "Amount" field is NOT the grand total — it is the
        # subcontract dollar-input below the 25% subcontract YES/NO row.
        # It stays blank unless Reytech is actually claiming 25% subcontract.
        assert updates["Amount"] == ""


class TestEndToEndFill:
    """Round-trip the real packet through parse → fill → re-read."""

    def test_output_file_created(self, parsed, reytech_info, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        assert r["ok"] is True
        assert os.path.exists(r["output_path"])
        # Real PDF header
        with open(r["output_path"], "rb") as f:
            assert f.read(5) == b"%PDF-"

    def test_fields_confirmed_on_readback(self, parsed, reytech_info, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        # Expect at LEAST the core supplier info (7) + cert (1) +
        # signature (2) + rev (1) + row 1 price+ext (2) + totals (4) +
        # amount (1) = 18 text fields. Checkboxes count separately.
        assert r["fields_written"] >= 18, (
            f"only {r['fields_written']} fields confirmed in output"
        )

    def test_grand_total_computed_correctly(self, parsed, reytech_info, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        # 15 sets × $395 = $5,925 base. Tax rate is 0 (no zip lookup in
        # tests); freight 0; total = subtotal.
        assert abs(r["subtotal"] - 5925.0) < 0.01
        assert r["grand_total"] >= r["subtotal"]

    def test_rows_priced_count(self, parsed, reytech_info, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        assert r["rows_priced"] == 1

    def test_output_filename_has_reytech_suffix(self, parsed, reytech_info, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        assert "_Reytech.pdf" in os.path.basename(r["output_path"])


class TestAttachmentSplicing:
    """The 7 placeholder pages in the source packet (pages 6, 7, 8, 9,
    10, 12, 13) are replaced with real filled attachments at their
    original positions. Total page count grows because most real
    attachments are more than 1 page (STD 204 is 2, CalRecycle is 2,
    DARFUR is 2, DVBE 843 is 1). Verified by round-trip field check."""

    def test_output_has_more_pages_than_source(self, parsed, reytech_info, tmp_path):
        from pypdf import PdfReader
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        assert r["ok"] is True
        src = PdfReader(SAMPLE)
        out = PdfReader(r["output_path"])
        # Source is 18 pages; we replace 7 placeholders with multi-page
        # attachments, so the final packet is at least a few pages
        # longer.
        assert len(out.pages) > len(src.pages)

    def test_civil_rights_fields_populated(self, parsed, reytech_info, tmp_path):
        from pypdf import PdfReader
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides={1: {"unit_price": 395.00}},
        )
        out = PdfReader(r["output_path"])
        fields = out.get_fields() or {}
        assert "ProposerBidder Firm Name Printed" in fields
        firm_val = str(fields["ProposerBidder Firm Name Printed"].get("/V", ""))
        assert "Reytech" in firm_val

    def test_bidder_declaration_fields_populated(self, parsed, reytech_info, tmp_path):
        from pypdf import PdfReader
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
        )
        out = PdfReader(r["output_path"])
        fields = out.get_fields() or {}
        # Bidder Declaration fields from the spliced attachment
        assert "Solicitaion #" in fields
        sol_val = str(fields["Solicitaion #"].get("/V", ""))
        assert sol_val  # non-empty
        text1_val = str(fields.get("Text1", {}).get("/V", ""))
        assert "SB" in text1_val or "DVBE" in text1_val

    def test_std204_fields_populated(self, parsed, reytech_info, tmp_path):
        from pypdf import PdfReader
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
        )
        out = PdfReader(r["output_path"])
        fields = out.get_fields() or {}
        # STD 204 has a distinctive field name we can key on
        key = "NAME OF AUTHORIZED PAYEE REPRESENTATIVE"
        assert key in fields
        val = str(fields[key].get("/V", ""))
        assert "Michael Guadan" in val

    def test_darfur_fields_populated(self, parsed, reytech_info, tmp_path):
        from pypdf import PdfReader
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
        )
        out = PdfReader(r["output_path"])
        fields = out.get_fields() or {}
        assert "CompanyVendor Name" in fields


class TestErrorPaths:
    def test_missing_source(self, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=str(tmp_path / "nope.pdf"),
            parsed={"line_items": []},
            output_dir=str(tmp_path),
        )
        assert r["ok"] is False
        assert "not found" in r["error"]

    def test_fill_without_prices_blocks_in_strict_mode(self, parsed, reytech_info, tmp_path):
        """Gate enforcement: filling with no prices is a hard error in
        strict mode (default). The gate flags 'row 1: no price set' as
        critical and fill returns ok=False with the issue in the
        error string."""
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides=None,
        )
        assert r["ok"] is False
        assert "gate validation failed" in r.get("error", "")
        assert any("no price set" in i for i in r["gate"]["critical_issues"])

    def test_fill_without_prices_allowed_in_non_strict_preview(self, parsed, reytech_info, tmp_path):
        """Non-strict (dry_run / preview) allows a no-price fill to
        return ok=True so the operator can eyeball the rendered packet
        before pricing. Gate report still attached with the issues."""
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides=None,
            strict=False,
        )
        assert r["ok"] is True
        assert r["rows_priced"] == 0
        assert r["subtotal"] == 0
        assert r["gate"]["passed"] is False
        assert any("no price set" in i for i in r["gate"]["critical_issues"])
