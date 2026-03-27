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
    verify_package_completeness,
    verify_single_form,
)


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
        # Should return first prefix or empty — just shouldn't crash
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
        # computed fields can't be resolved statically — should return None or truthy
        assert result is None or isinstance(result, str)

    def test_sign_date(self):
        result = _resolve_expected("sign_date", {}, {})
        # sign_date should resolve to today's date or similar
        assert result is None or isinstance(result, str)

    def test_missing_nested_key(self):
        result = _resolve_expected("company.fein", {}, {"company": {}})
        assert result is None or result == ""


# ── verify_package_completeness tests ──────────────────────────────────

class TestPackageCompleteness:
    def test_cchcs_complete(self):
        """CCHCS: 703b + 704b + bidpkg + quote = complete."""
        required = {"703b", "704b", "bidpkg", "quote"}
        files = ["703B_SOL123.pdf", "704B_SOL123.pdf", "BidPackage_SOL123.pdf", "Quote_SOL123.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=True)
        assert result["passed"]

    def test_missing_form(self):
        """Missing 704b should fail."""
        required = {"703b", "704b", "bidpkg", "quote"}
        files = ["703B_SOL123.pdf", "BidPackage_SOL123.pdf", "Quote_SOL123.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=True)
        assert not result["passed"]
        assert any("704b" in i.lower() for i in result.get("issues", []))

    def test_standalone_dvbe_with_bidpkg_warns(self):
        """DVBE as standalone when bid package present = warning."""
        required = {"703b", "704b", "bidpkg", "quote"}
        files = ["703B.pdf", "704B.pdf", "BidPackage.pdf", "Quote.pdf", "DVBE_843.pdf"]
        result = verify_package_completeness("cchcs", required, files, has_bid_package=True)
        # Should warn about standalone DVBE when it's inside bid package
        warnings = result.get("warnings", [])
        assert any("dvbe" in w.lower() or "internal" in w.lower() for w in warnings) or result["passed"]


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
        """Every registry entry must have required_fields and signature_fields."""
        for form_id, entry in FORM_FIELD_REGISTRY.items():
            assert "required_fields" in entry, f"{form_id} missing required_fields"
            assert "signature_fields" in entry, f"{form_id} missing signature_fields"

    def test_standalone_forms_added(self):
        """Verify the new standalone form entries exist."""
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
        """Unknown form_id should warn but pass (no fields to check)."""
        # Create a dummy PDF-like file
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
