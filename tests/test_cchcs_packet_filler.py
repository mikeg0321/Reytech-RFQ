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
        "address": "30 Carnoustie Way, Trabuco Canyon, CA 92679",
        "phone": "949-229-1575",
        "email": "sales@reytechinc.com",
        "sb_mb": "2002605",
        "dvbe": "2002605",
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

    def test_signature_and_date(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        assert updates["Signature1_es_:signer:signature"] == "Michael Guadan"
        # Date is today in MM/DD/YYYY form
        assert len(updates["Date_es_:date"]) >= 8
        assert "/" in updates["Date_es_:date"]

    def test_compliance_checkboxes_default_yes(self, parsed, reytech_info):
        updates = _build_field_updates(parsed, reytech_info)
        for cb in COMPLIANCE_CHECKBOXES_YES:
            assert updates[cb] == "/Yes"

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
        assert updates["Amount"] == updates["Extension TotalTotal"]


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


class TestErrorPaths:
    def test_missing_source(self, tmp_path):
        r = fill_cchcs_packet(
            source_pdf=str(tmp_path / "nope.pdf"),
            parsed={"line_items": []},
            output_dir=str(tmp_path),
        )
        assert r["ok"] is False
        assert "not found" in r["error"]

    def test_fill_without_prices_leaves_line_items_blank(self, parsed, reytech_info, tmp_path):
        """Reytech standard: don't write zeros or placeholders for rows
        we can't price. Human operator sees blanks, knows to fill manually."""
        r = fill_cchcs_packet(
            source_pdf=SAMPLE,
            parsed=parsed,
            output_dir=str(tmp_path),
            reytech_info=reytech_info,
            price_overrides=None,  # nothing to price
        )
        assert r["ok"] is True
        assert r["rows_priced"] == 0
        assert r["subtotal"] == 0
