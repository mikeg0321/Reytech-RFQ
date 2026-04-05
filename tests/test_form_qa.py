"""Tests for src/forms/form_qa.py — Form QA verification system."""
import os
import sys
import pytest

# Ensure project root on path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.forms.form_qa import (
    FORM_FIELD_REGISTRY,
    BID_PACKAGE_INTERNAL_FORMS,
    _detect_prefix,
    _resolve_expected,
    _parse_currency,
    _is_buyer_field,
    verify_filled_form,
    verify_package_completeness,
    verify_single_form,
    verify_704b_computations,
    verify_value_ranges,
    verify_buyer_fields_untouched,
    verify_signature_file_exists,
)

TEMPLATES_DIR = os.path.join(_PROJECT_ROOT, "data", "templates")
AMS_704_BLANK = os.path.join(TEMPLATES_DIR, "ams_704_blank.pdf")


# ── _detect_prefix tests ──────────────────────────────────────────────

class TestDetectPrefix:
    def test_703b_prefix(self):
        fields = {"703B_Business Name", "703B_Address", "703B_Phone", "Signature1"}
        assert _detect_prefix(fields, ["703B_", "703C_", ""]) == "703B_"

    def test_703c_prefix(self):
        fields = {"703C_Business Name", "703C_Address", "Signature1"}
        assert _detect_prefix(fields, ["703C_", "703B_", ""]) == "703C_"

    def test_no_prefix(self):
        fields = {"Business Name", "Address", "Phone"}
        assert _detect_prefix(fields, ["703B_", "703C_", ""]) == ""

    def test_empty_fields(self):
        result = _detect_prefix(set(), ["703B_", "703C_", ""])
        assert isinstance(result, str)


# ── _resolve_expected tests ────────────────────────────────────────────

class TestResolveExpected:
    def test_company_field(self):
        config = {"company": {"name": "Reytech Inc."}}
        result = _resolve_expected("company.name", {}, config)
        assert result == "Reytech Inc."

    def test_rfq_field(self):
        rfq_data = {"solicitation_number": "SOL-2026-001"}
        result = _resolve_expected("rfq.solicitation_number", rfq_data, {})
        assert result == "SOL-2026-001"

    def test_static_value(self):
        result = _resolve_expected("static:SB/DVBE", {}, {})
        assert result == "SB/DVBE"

    def test_computed_value(self):
        result = _resolve_expected("computed", {}, {})
        assert result is None or isinstance(result, str)

    def test_sign_date(self):
        result = _resolve_expected("sign_date", {}, {})
        assert result is None or isinstance(result, str)

    def test_missing_nested_key(self):
        result = _resolve_expected("company.fein", {}, {"company": {}})
        assert result is None or result == ""


# ── _parse_currency tests ────────────────────────────────────────────

class TestParseCurrency:
    def test_plain_number(self):
        assert _parse_currency("1234.56") == 1234.56

    def test_dollar_sign(self):
        assert _parse_currency("$1,234.56") == 1234.56

    def test_empty_string(self):
        assert _parse_currency("") is None

    def test_none(self):
        assert _parse_currency(None) is None

    def test_negative(self):
        assert _parse_currency("-5.00") == -5.0

    def test_comma_only(self):
        assert _parse_currency("$10,000") == 10000.0

    def test_garbage(self):
        assert _parse_currency("abc") is None


# ── _is_buyer_field tests ────────────────────────────────────────────

class TestIsBuyerField:
    def test_header_fields(self):
        assert _is_buyer_field("DEPARTMENT")
        assert _is_buyer_field("PHONE")
        assert _is_buyer_field("EMAIL")
        assert _is_buyer_field("SOLICITATION NUMBER")

    def test_row_fields(self):
        assert _is_buyer_field("Row1")
        assert _is_buyer_field("Row5_2")
        assert _is_buyer_field("QTYRow3")
        assert _is_buyer_field("UOMRow1")
        assert _is_buyer_field("ITEM NUMBERRow7")

    def test_vendor_fields_not_buyer(self):
        assert not _is_buyer_field("COMPANY NAME")
        assert not _is_buyer_field("PERSON PROVIDING QUOTE")
        assert not _is_buyer_field("PRICE PER UNITRow1")
        assert not _is_buyer_field("EXTENSIONRow1")
        assert not _is_buyer_field("Signature1")


# ── verify_package_completeness tests ──────────────────────────────────

class TestPackageCompleteness:
    def test_cchcs_complete(self):
        required = {"703b", "704b", "bidpkg", "quote"}
        files = ["703B_SOL123.pdf", "704B_SOL123.pdf", "BidPackage_SOL123.pdf", "Quote_SOL123.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=True)
        assert result["passed"]

    def test_missing_form(self):
        required = {"703b", "704b", "bidpkg", "quote"}
        files = ["703B_SOL123.pdf", "BidPackage_SOL123.pdf", "Quote_SOL123.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=True)
        assert not result["passed"]
        assert any("704b" in i.lower() for i in result.get("issues", []))

    def test_standalone_dvbe_with_bidpkg_warns(self):
        required = {"703b", "704b", "bidpkg", "quote"}
        files = ["703B.pdf", "704B.pdf", "BidPackage.pdf", "Quote.pdf", "DVBE_843.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=True)
        warnings = result.get("warnings", [])
        assert any("dvbe" in w.lower() or "internal" in w.lower() for w in warnings) or result["passed"]

    def test_calvet_no_ams_forms_passes(self):
        """CalVet packages don't require 703B/704B."""
        required = {"quote", "calrecycle74", "bidder_decl", "dvbe843"}
        files = ["Quote_CV.pdf", "CalRecycle74.pdf", "BidderDecl.pdf", "DVBE843.pdf"]
        result = verify_package_completeness("calvet", required, files, has_bid_package=False)
        assert result["passed"]

    def test_both_703b_and_703c_warns(self):
        required = {"703b", "704b", "quote"}
        files = ["703B_SOL.pdf", "703C_SOL.pdf", "704B_SOL.pdf", "Quote.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=False)
        assert any("703B" in w and "703C" in w for w in result.get("warnings", []))


# ── BID_PACKAGE_INTERNAL_FORMS tests ──────────────────────────────────

class TestBidPackageInternal:
    def test_expected_forms_present(self):
        assert "dvbe843" in BID_PACKAGE_INTERNAL_FORMS
        assert "sellers_permit" in BID_PACKAGE_INTERNAL_FORMS
        assert "calrecycle74" in BID_PACKAGE_INTERNAL_FORMS

    def test_primary_forms_not_internal(self):
        assert "703b" not in BID_PACKAGE_INTERNAL_FORMS
        assert "704b" not in BID_PACKAGE_INTERNAL_FORMS
        assert "quote" not in BID_PACKAGE_INTERNAL_FORMS
        assert "bidpkg" not in BID_PACKAGE_INTERNAL_FORMS


# ── FORM_FIELD_REGISTRY tests ─────────────────────────────────────────

class TestRegistry:
    def test_all_form_ids_have_required_keys(self):
        for form_id, entry in FORM_FIELD_REGISTRY.items():
            assert "required_fields" in entry, f"{form_id} missing required_fields"
            assert "signature_fields" in entry, f"{form_id} missing signature_fields"

    def test_standalone_forms_added(self):
        expected = ["calrecycle74", "darfur", "cv012_cuf", "std204", "std1000", "bidder_decl"]
        for form_id in expected:
            assert form_id in FORM_FIELD_REGISTRY, f"Missing registry entry: {form_id}"

    def test_704b_has_pricing_required(self):
        assert FORM_FIELD_REGISTRY["704b"].get("pricing_required") is True

    def test_quote_has_pricing_required(self):
        assert FORM_FIELD_REGISTRY["quote"].get("pricing_required") is True


# ── verify_single_form tests ──────────────────────────────────────────

class TestVerifySingleForm:
    def test_missing_file(self):
        result = verify_single_form("/nonexistent/file.pdf", "704b")
        assert not result["passed"]
        assert any("not found" in i.lower() for i in result["issues"])

    def test_unknown_form_id_warns(self):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(b"%PDF-1.4 dummy")
            tmp = f.name
        try:
            result = verify_single_form(tmp, "nonexistent_form_id")
            assert result["passed"]
            assert any("no qa registry" in w.lower() for w in result["warnings"])
        finally:
            os.unlink(tmp)


# ── verify_704b_computations tests ────────────────────────────────────

class TestComputations:
    def test_missing_file_warns(self):
        result = verify_704b_computations("/nonexistent.pdf", {})
        assert result["passed"]  # Can't fail if we can't read the file
        assert len(result["warnings"]) > 0

    @pytest.mark.skipif(not os.path.exists(AMS_704_BLANK),
                        reason="ams_704_blank.pdf not available")
    def test_blank_704_has_no_pricing_rows(self):
        """A blank 704 should have no pricing rows to check."""
        result = verify_704b_computations(AMS_704_BLANK, {})
        assert result["passed"]
        assert any("no pricing rows" in w.lower() for w in result["warnings"])


# ── verify_value_ranges tests ─────────────────────────────────────────

class TestValueRanges:
    def test_unknown_form_passes(self):
        result = verify_value_ranges("/nonexistent.pdf", "unknown_form")
        assert result["passed"]

    def test_missing_file_warns(self):
        result = verify_value_ranges("/nonexistent.pdf", "703b")
        assert result["passed"]  # Warning, not failure
        assert len(result["warnings"]) > 0


# ── verify_buyer_fields_untouched tests ───────────────────────────────

class TestBuyerContamination:
    def test_no_template_passes(self):
        """Without template, can't check — should pass."""
        result = verify_buyer_fields_untouched("/some/filled.pdf", "")
        assert result["passed"]

    def test_missing_filled_pdf_fails(self):
        """If filled PDF doesn't exist, fail."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(b"%PDF-1.4 dummy")
            tmp = f.name
        try:
            result = verify_buyer_fields_untouched("/nonexistent.pdf", tmp)
            assert not result["passed"]
        finally:
            os.unlink(tmp)

    @pytest.mark.skipif(not os.path.exists(AMS_704_BLANK),
                        reason="ams_704_blank.pdf not available")
    def test_same_pdf_passes(self):
        """Comparing a PDF against itself should show no contamination."""
        result = verify_buyer_fields_untouched(AMS_704_BLANK, AMS_704_BLANK)
        assert result["passed"]
        assert len(result["contaminated"]) == 0


# ── verify_signature_file_exists tests ────────────────────────────────

class TestSignatureFileExists:
    def test_with_empty_config(self):
        """Should search common locations."""
        result = verify_signature_file_exists({})
        # May or may not find it depending on environment
        assert "passed" in result
        assert "path" in result

    def test_with_explicit_missing_path(self):
        result = verify_signature_file_exists({"signature_image": "/nonexistent/sig.png"})
        assert not result["passed"]

    def test_with_explicit_existing_path(self):
        """If we point to a real file, it should pass."""
        result = verify_signature_file_exists({"signature_image": AMS_704_BLANK})
        if os.path.exists(AMS_704_BLANK):
            assert result["passed"]


# ── verify_filled_form with page_stats tests ──────────────────────────

class TestPageStats:
    @pytest.mark.skipif(not os.path.exists(AMS_704_BLANK),
                        reason="ams_704_blank.pdf not available")
    def test_blank_704_has_page_stats(self):
        """A 704 template should return page_stats with field counts."""
        result = verify_filled_form(AMS_704_BLANK, "704b", {}, {})
        assert "page_stats" in result
        assert len(result["page_stats"]) >= 1
        # Verify page_stats structure
        for ps in result["page_stats"]:
            assert "page" in ps
            assert "total_fields" in ps
            assert "filled_fields" in ps
            assert ps["total_fields"] >= 0
            assert ps["filled_fields"] >= 0
            assert ps["filled_fields"] <= ps["total_fields"]


# ── Integration: full run_form_qa ─────────────────────────────────────

class TestRunFormQA:
    def test_import_and_call(self):
        """Verify run_form_qa is callable with minimal args."""
        from src.forms.form_qa import run_form_qa
        result = run_form_qa(
            out_dir="/nonexistent",
            output_files=[],
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="other",
            required_forms=set(),
        )
        assert result["passed"]  # No forms, no failures
        assert result["forms_checked"] == 0

    def test_missing_required_form_fails(self):
        from src.forms.form_qa import run_form_qa
        result = run_form_qa(
            out_dir="/nonexistent",
            output_files=[],
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="cchcs",
            required_forms={"703b", "704b", "bidpkg", "quote"},
        )
        assert not result["passed"]
        assert len(result["critical_issues"]) > 0
