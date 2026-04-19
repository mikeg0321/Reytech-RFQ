"""Tests for the Fill Engine (Phase 2)."""
import io
from decimal import Decimal

import pytest
from pypdf import PdfReader

from src.core.quote_model import Quote, LineItem, QuoteHeader, BuyerInfo, Address
from src.forms.fill_engine import fill, _fmt_money, _expiry_date
from src.forms.profile_registry import load_profiles


@pytest.fixture
def profile_704a():
    profiles = load_profiles()
    return profiles["704a_reytech_standard"]


@pytest.fixture
def sample_quote():
    return Quote(
        doc_type="pc",
        doc_id="test-fill-001",
        header=QuoteHeader(
            solicitation_number="OS - Den - Feb 2026",
            institution_key="CSP-Sacramento",
        ),
        buyer=BuyerInfo(
            requestor_name="John Doe",
            requestor_phone="916-555-0100",
        ),
        ship_to=Address(full="CSP-Sacramento, Represa, CA 95671", zip_code="95671"),
        line_items=[
            LineItem(line_no=1, description="Name tag black/white", qty=Decimal("22"),
                     uom="EA", unit_cost=Decimal("12.58"), markup_pct=Decimal("25"),
                     item_no="NT-001"),
            LineItem(line_no=2, description="Copy paper 10 reams", qty=Decimal("5"),
                     uom="BOX", unit_cost=Decimal("42.99"), markup_pct=Decimal("25")),
        ],
    )


class TestFillAcroform:
    """AcroForm fill via PyPDFForm."""

    def test_basic_fill(self, sample_quote, profile_704a):
        result = fill(sample_quote, profile_704a)
        assert isinstance(result, bytes)
        assert len(result) > 10000

    def test_vendor_fields_filled(self, sample_quote, profile_704a):
        result = fill(sample_quote, profile_704a)
        reader = PdfReader(io.BytesIO(result))
        fields = reader.get_fields()
        assert "Reytech" in str(fields.get("COMPANY NAME", {}).get("/V", ""))
        assert "Reytech" in str(fields.get("SUPPLIER NAME", {}).get("/V", ""))
        assert "Michael Guadan" in str(fields.get("COMPANY REPRESENTATIVE print name", {}).get("/V", ""))

    def test_buyer_fields_filled(self, sample_quote, profile_704a):
        result = fill(sample_quote, profile_704a)
        reader = PdfReader(io.BytesIO(result))
        fields = reader.get_fields()
        assert "John Doe" in str(fields.get("Requestor", {}).get("/V", ""))
        assert "CSP-Sacramento" in str(fields.get("Institution or HQ Program", {}).get("/V", ""))
        assert "95671" in str(fields.get("Delivery Zip Code", {}).get("/V", ""))

    def test_item_pricing_filled(self, sample_quote, profile_704a):
        result = fill(sample_quote, profile_704a)
        reader = PdfReader(io.BytesIO(result))
        fields = reader.get_fields()
        # Item 1: $12.58 * 1.25 = $15.73, ext = $15.73 * 22 = $346.06
        price1 = str(fields.get("PRICE PER UNITRow1", {}).get("/V", ""))
        assert "15.73" in price1
        ext1 = str(fields.get("EXTENSIONRow1", {}).get("/V", ""))
        assert "346.06" in ext1
        # Item 2: $42.99 * 1.25 = $53.74, ext = $53.74 * 5 = $268.70
        price2 = str(fields.get("PRICE PER UNITRow2", {}).get("/V", ""))
        assert "53.74" in price2

    def test_mfg_number_in_substituted(self, sample_quote, profile_704a):
        """MFG# goes in the SUBSTITUTED ITEM column (per feedback_item_identity)."""
        result = fill(sample_quote, profile_704a)
        reader = PdfReader(io.BytesIO(result))
        fields = reader.get_fields()
        sub_field = "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow1"
        val = str(fields.get(sub_field, {}).get("/V", ""))
        assert "NT-001" in val

    def test_description_filled(self, sample_quote, profile_704a):
        result = fill(sample_quote, profile_704a)
        reader = PdfReader(io.BytesIO(result))
        fields = reader.get_fields()
        desc_field = "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow1"
        val = str(fields.get(desc_field, {}).get("/V", ""))
        assert "Name tag" in val

    def test_empty_items(self, profile_704a):
        q = Quote(doc_type="pc", doc_id="empty")
        result = fill(q, profile_704a)
        assert isinstance(result, bytes)
        assert len(result) > 10000

    def test_no_bid_items_excluded(self, profile_704a):
        q = Quote(
            doc_type="pc",
            doc_id="nobid",
            line_items=[
                LineItem(line_no=1, description="Include", unit_cost=Decimal("10"), markup_pct=Decimal("25")),
                LineItem(line_no=2, description="Skip", no_bid=True, unit_cost=Decimal("20")),
                LineItem(line_no=3, description="Also include", unit_cost=Decimal("30"), markup_pct=Decimal("25")),
            ],
        )
        result = fill(q, profile_704a)
        reader = PdfReader(io.BytesIO(result))
        fields = reader.get_fields()
        # Row 1 should have "Include", Row 2 should have "Also include" (no_bid skipped)
        desc1 = str(fields.get("ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow1", {}).get("/V", ""))
        desc2 = str(fields.get("ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow2", {}).get("/V", ""))
        assert "Include" in desc1
        assert "Also include" in desc2


class TestFillEdgeCases:
    """Edge cases and error handling."""

    def test_missing_blank_pdf(self, sample_quote):
        from src.forms.profile_registry import FormProfile
        bad_profile = FormProfile(
            id="bad", form_type="test", blank_pdf="/nonexistent.pdf", fill_mode="acroform"
        )
        with pytest.raises(ValueError, match="not found"):
            fill(sample_quote, bad_profile)

    def test_overlay_not_implemented(self, sample_quote):
        from src.forms.profile_registry import FormProfile
        profile = FormProfile(
            id="overlay", form_type="test",
            blank_pdf="tests/fixtures/ams_704_blank.pdf",
            fill_mode="overlay",
        )
        with pytest.raises(NotImplementedError):
            fill(sample_quote, profile)


class TestStaticAttach:
    """static_attach mode: final-artifact PDFs served verbatim, hard-fail on missing source."""

    def test_sellers_permit_uses_static_attach(self):
        profiles = load_profiles()
        sp = profiles["sellers_permit_reytech"]
        assert sp.fill_mode == "static_attach", (
            "sellers_permit_reytech must use static_attach — missing the scan should "
            "hard-fail, not silently produce 0 bytes. See _fill_static_attach()."
        )

    def test_static_attach_returns_artifact_bytes(self, sample_quote):
        profiles = load_profiles()
        sp = profiles["sellers_permit_reytech"]
        result = fill(sample_quote, sp)
        assert isinstance(result, bytes)
        assert len(result) > 1000, "seller's permit artifact should be a real PDF"
        assert result.startswith(b"%PDF"), "output must be a valid PDF header"

    def test_static_attach_raises_when_source_missing(self, sample_quote, tmp_path):
        from src.forms.profile_registry import FormProfile
        missing = str(tmp_path / "does_not_exist.pdf")
        profile = FormProfile(
            id="test_missing",
            form_type="test",
            blank_pdf=missing,
            fill_mode="static_attach",
        )
        with pytest.raises((RuntimeError, ValueError), match="static_attach|Blank PDF"):
            fill(sample_quote, profile)


class TestHelpers:
    """Helper function tests."""

    def test_fmt_money(self):
        assert _fmt_money(Decimal("123.45")) == "123.45"
        assert _fmt_money(Decimal("0")) == ""
        assert _fmt_money(Decimal("1000.10")) == "1000.10"

    def test_expiry_date_format(self):
        d = _expiry_date()
        assert len(d) == 10  # MM/DD/YYYY
        assert "/" in d
