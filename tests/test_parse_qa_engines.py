"""Tests for Parse Engine + QA Engine (Phase 2)."""
import io
from decimal import Decimal

import pytest

from src.core.quote_model import Quote, LineItem, QuoteHeader, BuyerInfo, Address
from src.forms.profile_registry import load_profiles


class TestParseEngine:
    """Profile-driven PDF parsing."""

    def test_parse_blank_704(self, blank_704_path):
        """Blank 704 should match the 704A profile."""
        from src.forms.parse_engine import parse
        quote, warnings = parse(blank_704_path)
        assert quote is not None
        assert quote.provenance.classifier_shape == "704a_reytech_standard"
        # The fixture may have pre-filled data — just verify profile matched
        assert isinstance(quote.line_items, list)

    def test_parse_filled_704(self, blank_704_path):
        """Fill a 704, then parse it back — round-trip."""
        from src.forms.fill_engine import fill
        from src.forms.parse_engine import parse

        profiles = load_profiles()
        profile = profiles["704a_reytech_standard"]

        # Create and fill a quote
        original = Quote(
            header=QuoteHeader(
                solicitation_number="TEST-PARSE-001",
                institution_key="CSP-Sacramento",
            ),
            buyer=BuyerInfo(requestor_name="Jane Doe", requestor_phone="916-555-1234"),
            ship_to=Address(zip_code="95671"),
            line_items=[
                LineItem(line_no=1, description="Widget A", qty=Decimal("10"),
                         uom="EA", unit_cost=Decimal("5.00"), markup_pct=Decimal("25")),
                LineItem(line_no=2, description="Widget B", qty=Decimal("5"),
                         uom="BOX", unit_cost=Decimal("20.00"), markup_pct=Decimal("30")),
            ],
        )

        filled_bytes = fill(original, profile)

        # Write to temp file for parsing
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(filled_bytes)
            temp_path = f.name

        try:
            parsed, warnings = parse(temp_path)

            # Verify round-trip
            assert parsed.header.solicitation_number == "TEST-PARSE-001"
            assert parsed.header.institution_key == "CSP-Sacramento"
            assert parsed.buyer.requestor_name == "Jane Doe"
            assert parsed.ship_to.zip_code == "95671"
            assert len(parsed.line_items) >= 2
            descs = [it.description for it in parsed.line_items]
            assert any("Widget A" in d for d in descs)
            assert any("Widget B" in d for d in descs)
        finally:
            os.unlink(temp_path)

    def test_parse_nonexistent_file(self):
        from src.forms.parse_engine import parse
        quote, warnings = parse("/nonexistent.pdf")
        assert len(warnings) > 0
        assert warnings[0].severity == "error"

    def test_parse_no_profile_match(self, tmp_path):
        """PDF with no form fields shouldn't match any profile."""
        from pypdf import PdfWriter
        from src.forms.parse_engine import parse

        writer = PdfWriter()
        writer.add_blank_page(612, 792)
        path = str(tmp_path / "blank.pdf")
        with open(path, "wb") as f:
            writer.write(f)

        quote, warnings = parse(path)
        assert any("No profile" in w.message for w in warnings)


class TestQAEngine:
    """QA validation using same profile as filler."""

    def test_qa_passes_on_correct_fill(self):
        """Fill → QA should pass when everything is correct."""
        from src.forms.fill_engine import fill
        from src.forms.qa_engine import validate

        profiles = load_profiles()
        profile = profiles["704a_reytech_standard"]

        quote = Quote(
            header=QuoteHeader(solicitation_number="QA-TEST-001", institution_key="CIW"),
            buyer=BuyerInfo(requestor_name="Test User"),
            ship_to=Address(zip_code="91710"),
            line_items=[
                LineItem(line_no=1, description="Test Item", qty=Decimal("3"),
                         uom="EA", unit_cost=Decimal("10.00"), markup_pct=Decimal("25")),
            ],
        )

        filled_bytes = fill(quote, profile)
        report = validate(filled_bytes, quote, profile)

        assert report.passed
        assert report.fields_matched > 0
        assert report.fields_wrong == 0
        assert report.match_rate > 80

    def test_qa_checks_vendor_fields(self):
        """Vendor fields (Reytech name, email, certs) should all verify."""
        from src.forms.fill_engine import fill
        from src.forms.qa_engine import validate

        profiles = load_profiles()
        profile = profiles["704a_reytech_standard"]

        quote = Quote(
            line_items=[
                LineItem(line_no=1, description="Item", qty=Decimal("1"),
                         unit_cost=Decimal("10"), markup_pct=Decimal("25")),
            ],
        )

        filled_bytes = fill(quote, profile)
        report = validate(filled_bytes, quote, profile)

        # Vendor fields should be present and correct
        vendor_issues = [i for i in report.issues if "vendor" in i.field]
        vendor_errors = [i for i in vendor_issues if i.severity == "error"]
        assert len(vendor_errors) == 0, f"Vendor errors: {vendor_errors}"

    def test_qa_checks_pricing(self):
        """Item prices and extensions should match."""
        from src.forms.fill_engine import fill
        from src.forms.qa_engine import validate

        profiles = load_profiles()
        profile = profiles["704a_reytech_standard"]

        quote = Quote(
            line_items=[
                LineItem(line_no=1, description="Priced Item", qty=Decimal("5"),
                         uom="EA", unit_cost=Decimal("12.00"), markup_pct=Decimal("30")),
            ],
        )

        filled_bytes = fill(quote, profile)
        report = validate(filled_bytes, quote, profile)

        # Price should be 12.00 * 1.30 = 15.60
        # Extension should be 15.60 * 5 = 78.00
        price_issues = [i for i in report.issues if "unit_price" in i.field]
        ext_issues = [i for i in report.issues if "extension" in i.field]
        assert len(price_issues) == 0, f"Price issues: {price_issues}"
        assert len(ext_issues) == 0, f"Extension issues: {ext_issues}"

    def test_qa_detects_bad_pdf(self):
        """QA should fail on corrupted/empty PDF."""
        from src.forms.qa_engine import validate

        profiles = load_profiles()
        profile = profiles["704a_reytech_standard"]
        quote = Quote()

        report = validate(b"not a pdf", quote, profile)
        assert not report.passed
        assert len(report.issues) > 0

    def test_qa_report_summary(self):
        """Report summary is human-readable."""
        from src.forms.qa_engine import ValidationReport
        report = ValidationReport(
            passed=True, fields_checked=10, fields_matched=9,
            fields_missing=1, fields_wrong=0, profile_id="test",
        )
        assert "9/10" in report.summary
        assert "90.0%" in report.summary
        assert "PASS" in report.summary
