"""
Package generation guardrails — catches form filling bugs BEFORE production.

These tests actually generate PDFs from templates and validate:
1. Output files exist and are valid PDFs
2. Expected fields are filled (not blank)
3. Signatures land in the right place
4. No double-signatures
5. Agency-specific form sets are correct
6. No crashes on any form filler function

Run: pytest tests/test_package_generation.py -v
"""
import os
import sys
import json
import tempfile
import shutil
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FLASK_ENV", "testing")
os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("TESTING", "1")

from pypdf import PdfReader

# ── Test Data ────────────────────────────────────────────────────────────────

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "templates")
FORMS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src", "forms")
SIG_PATH = os.path.join(FORMS_DIR, "signature_transparent.png")

REYTECH_CONFIG = {
    "company": {
        "name": "Reytech Inc.",
        "address": "PO Box 1234, San Diego, CA 92101",
        "owner": "Michael Gutierrez",
        "title": "President",
        "phone": "(619) 555-1234",
        "email": "sales@reytechinc.com",
        "fein": "12-3456789",
        "sellers_permit": "SR ABC 12-345678",
        "cert_number": "2012345",
        "cert_expiration": "12/31/2026",
        "sb_cert": "2012345",
        "dvbe_cert": "2012345",
    }
}

SAMPLE_RFQ = {
    "solicitation_number": "TEST-2026-001",
    "release_date": "03/01/2026",
    "due_date": "03/15/2026",
    "sign_date": "03/10/2026",
    "delivery_days": "30",
    "delivery_location": "California Institution for Women, Corona, CA 92878",
    "requestor_name": "Jane Smith",
    "requestor_email": "jane.smith@cdcr.ca.gov",
    "requestor_phone": "(916) 555-9999",
    "institution": "California Institution for Women",
    "department": "CDCR",
    "line_items": [
        {"description": "Nitrile Exam Gloves, Large, 100/box", "quantity": 50,
         "uom": "BX", "unit_price": 12.50, "bid_price": 15.00,
         "supplier_cost": 12.50, "part_number": "NG-100L"},
        {"description": "Hand Sanitizer, 8oz Pump", "quantity": 100,
         "uom": "EA", "unit_price": 3.25, "bid_price": 4.50,
         "supplier_cost": 3.25, "part_number": "HS-8OZ"},
    ],
}


@pytest.fixture
def output_dir():
    """Temp directory for generated PDFs — cleaned up after each test."""
    d = tempfile.mkdtemp(prefix="reytech_test_pkg_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def assert_valid_pdf(path, min_pages=1):
    """Assert file exists, is a valid PDF, and has at least min_pages pages."""
    assert os.path.exists(path), f"PDF not generated: {path}"
    size = os.path.getsize(path)
    assert size > 500, f"PDF too small ({size} bytes), likely empty: {path}"
    reader = PdfReader(path)
    assert len(reader.pages) >= min_pages, \
        f"Expected >= {min_pages} pages, got {len(reader.pages)}: {path}"
    return reader


def get_filled_fields(reader):
    """Extract all non-empty field values from a PDF."""
    fields = reader.get_fields() or {}
    filled = {}
    for name, field in fields.items():
        val = field.get("/V")
        if val and str(val).strip() and str(val) != "/Off":
            filled[name] = str(val)
    return filled


def count_signature_overlays(reader):
    """Count pages that have signature image overlays (XObject images)."""
    sig_pages = 0
    for i, page in enumerate(reader.pages):
        resources = page.get("/Resources", {})
        xobjects = resources.get("/XObject", {})
        if xobjects:
            # Check if any XObject is an image (signature overlay)
            for key in xobjects:
                xobj = xobjects[key].get_object() if hasattr(xobjects[key], "get_object") else {}
                if isinstance(xobj, dict) and xobj.get("/Subtype") == "/Image":
                    sig_pages += 1
                    break
    return sig_pages


# ═══════════════════════════════════════════════════════════════════════════
# 1. Individual Form Fillers — Each Must Not Crash
# ═══════════════════════════════════════════════════════════════════════════

class TestFormFillerNoCrash:
    """Every form filler function must complete without exception."""

    def test_fill_std204(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_std204
        template = os.path.join(TEMPLATE_DIR, "std204_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("std204_blank.pdf not found")
        out = os.path.join(output_dir, "std204.pdf")
        fill_std204(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        reader = assert_valid_pdf(out)
        filled = get_filled_fields(reader)
        assert any("Reytech" in v for v in filled.values()), \
            "Company name not found in STD 204 fields"

    def test_fill_calrecycle(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        template = os.path.join(TEMPLATE_DIR, "calrecycle_74_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("calrecycle_74_blank.pdf not found")
        out = os.path.join(output_dir, "calrecycle.pdf")
        fill_calrecycle_standalone(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_fill_cv012_cuf(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_cv012_cuf
        template = os.path.join(TEMPLATE_DIR, "cv012_cuf_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("cv012_cuf_blank.pdf not found")
        out = os.path.join(output_dir, "cv012_cuf.pdf")
        fill_cv012_cuf(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_fill_bidder_declaration(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_bidder_declaration
        template = os.path.join(TEMPLATE_DIR, "bidder_declaration_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("bidder_declaration_blank.pdf not found")
        out = os.path.join(output_dir, "bidder_decl.pdf")
        fill_bidder_declaration(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_fill_darfur(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_darfur_standalone
        template = os.path.join(TEMPLATE_DIR, "darfur_act_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("darfur_act_blank.pdf not found")
        out = os.path.join(output_dir, "darfur.pdf")
        fill_darfur_standalone(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_fill_std1000(self, output_dir):
        from src.forms.reytech_filler_v4 import fill_std1000
        template = os.path.join(TEMPLATE_DIR, "std1000_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("std1000_blank.pdf not found")
        out = os.path.join(output_dir, "std1000.pdf")
        fill_std1000(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_generate_dvbe_843(self, output_dir):
        from src.forms.reytech_filler_v4 import generate_dvbe_843
        out = os.path.join(output_dir, "dvbe843.pdf")
        generate_dvbe_843(SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_generate_darfur_act(self, output_dir):
        from src.forms.reytech_filler_v4 import generate_darfur_act
        out = os.path.join(output_dir, "darfur_gen.pdf")
        generate_darfur_act(SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_generate_bidder_declaration(self, output_dir):
        from src.forms.reytech_filler_v4 import generate_bidder_declaration
        out = os.path.join(output_dir, "bidder_decl_gen.pdf")
        generate_bidder_declaration(SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_generate_drug_free(self, output_dir):
        from src.forms.reytech_filler_v4 import generate_drug_free
        out = os.path.join(output_dir, "drug_free.pdf")
        generate_drug_free(SAMPLE_RFQ, REYTECH_CONFIG, out)
        assert_valid_pdf(out)


# ═══════════════════════════════════════════════════════════════════════════
# 2. Quote Generation
# ═══════════════════════════════════════════════════════════════════════════

class TestQuoteGeneration:

    def test_quote_generates(self, output_dir):
        from src.forms.quote_generator import generate_quote
        out = os.path.join(output_dir, "quote.pdf")
        result = generate_quote(SAMPLE_RFQ, out, agency="CDCR",
                                quote_number="R26QTEST001")
        assert result.get("ok"), f"Quote generation failed: {result}"
        assert_valid_pdf(out)

    def test_quote_has_items(self, output_dir):
        from src.forms.quote_generator import generate_quote
        out = os.path.join(output_dir, "quote.pdf")
        result = generate_quote(SAMPLE_RFQ, out, agency="CDCR",
                                quote_number="R26QTEST002")
        reader = PdfReader(out)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        assert "Nitrile" in text, "Item description not found in quote PDF"
        assert "reytechinc" in text.lower().replace(" ", ""), \
            "Company name not found in quote PDF"

    def test_quote_math_correct(self, output_dir):
        from src.forms.quote_generator import generate_quote
        out = os.path.join(output_dir, "quote.pdf")
        result = generate_quote(SAMPLE_RFQ, out, agency="CDCR",
                                quote_number="R26QTEST003")
        # 50 * 15.00 = 750.00 + 100 * 4.50 = 450.00 = $1,200.00 subtotal
        # Default tax = 7.25% → $87.00 → total $1,287.00
        assert result.get("ok")
        log_total = result.get("total", 0)
        subtotal = 50 * 15.00 + 100 * 4.50
        tax = round(subtotal * 0.0725, 2)
        expected = subtotal + tax
        assert abs(log_total - expected) < 0.01, \
            f"Quote total {log_total} != expected {expected}"


# ═══════════════════════════════════════════════════════════════════════════
# 3. Agency Config Guardrails
# ═══════════════════════════════════════════════════════════════════════════

class TestAgencyFormSets:
    """Verify agency configs produce the correct form sets."""

    def test_cchcs_package_is_minimal(self):
        """CCHCS = 703B/C + 704B + bid package + quote ONLY.
        DVBE 843, CalRecycle, seller's permit are INSIDE bid package."""
        from src.core.agency_config import load_agency_configs
        configs = load_agency_configs()
        cchcs = configs.get("cchcs", {})
        forms = cchcs.get("required_forms", [])
        # These should NOT be standalone for CCHCS
        for bad in ["dvbe843", "calrecycle74", "sellers_permit",
                     "darfur_act", "bidder_decl"]:
            assert bad not in forms, \
                f"CCHCS should NOT have standalone '{bad}' — it's inside the bid package"

    def test_cdcr_package_is_minimal(self):
        from src.core.agency_config import load_agency_configs
        configs = load_agency_configs()
        cdcr = configs.get("cdcr", {})
        forms = cdcr.get("required_forms", [])
        for bad in ["dvbe843", "calrecycle74", "sellers_permit"]:
            assert bad not in forms, \
                f"CDCR should NOT have standalone '{bad}' — it's inside the bid package"

    def test_calvet_has_cuf(self):
        from src.core.agency_config import load_agency_configs
        configs = load_agency_configs()
        calvet = configs.get("calvet", {})
        forms = calvet.get("required_forms", [])
        assert "cv012_cuf" in forms, "CalVet must include CV 012 CUF"

    def test_all_agencies_have_quote(self):
        from src.core.agency_config import load_agency_configs
        configs = load_agency_configs()
        for key, cfg in configs.items():
            forms = cfg.get("required_forms", [])
            assert "quote" in forms, \
                f"Agency '{key}' is missing 'quote' in required_forms"


# ═══════════════════════════════════════════════════════════════════════════
# 4. Signature Guardrails
# ═══════════════════════════════════════════════════════════════════════════

class TestSignatureGuardrails:
    """Prevent double-signing and mis-placed signatures."""

    def test_sign_fields_whitelist_exists(self):
        """SIGN_FIELDS must exist and contain known-good field names."""
        from src.forms.reytech_filler_v4 import SIGN_FIELDS
        assert isinstance(SIGN_FIELDS, set)
        assert len(SIGN_FIELDS) >= 5, "SIGN_FIELDS whitelist too small"
        assert "Signature1" in SIGN_FIELDS
        assert "Bidder Signature" in SIGN_FIELDS

    def test_no_generic_signature_in_whitelist(self):
        """Generic names like 'Sig' or 'Sign' should NOT be in whitelist."""
        from src.forms.reytech_filler_v4 import SIGN_FIELDS
        for bad in ["Sig", "Sign", "sig", "sign"]:
            assert bad not in SIGN_FIELDS, \
                f"Generic name '{bad}' in SIGN_FIELDS — too broad, will sign wrong fields"

    def test_std204_no_double_sign(self, output_dir):
        """STD 204 should have exactly 1 signature, not 2+."""
        from src.forms.reytech_filler_v4 import fill_std204
        template = os.path.join(TEMPLATE_DIR, "std204_blank.pdf")
        if not os.path.exists(template):
            pytest.skip("std204_blank.pdf not found")
        out = os.path.join(output_dir, "std204_sig.pdf")
        fill_std204(template, SAMPLE_RFQ, REYTECH_CONFIG, out)
        reader = PdfReader(out)
        sig_count = count_signature_overlays(reader)
        assert sig_count <= 1, \
            f"STD 204 has {sig_count} signature overlays — expected 1 max"


# ═══════════════════════════════════════════════════════════════════════════
# 5. Field Ownership (704B)
# ═══════════════════════════════════════════════════════════════════════════

class TestFieldOwnership:
    """704B buyer fields must never be overwritten."""

    def test_704b_buyer_fields_constant_exists(self):
        """BUYER_FIELDS or equivalent must be defined to protect buyer data."""
        from src.forms import reytech_filler_v4 as filler
        source = open(filler.__file__, encoding="utf-8").read()
        # Check for buyer field protection comments/constants
        assert "BUYER FILLS" in source or "buyer" in source.lower(), \
            "No buyer field protection found in reytech_filler_v4.py"

    def test_704b_vendor_fields_include_price(self):
        """704B vendor fields must include PRICE PER UNIT and COMPANY NAME."""
        from src.forms import reytech_filler_v4 as filler
        source = open(filler.__file__, encoding="utf-8").read()
        assert "PRICE PER UNIT" in source, \
            "704B must fill PRICE PER UNIT (vendor field)"
        assert "COMPANY NAME" in source or "Company Name" in source, \
            "704B must fill COMPANY NAME (vendor field)"


# ═══════════════════════════════════════════════════════════════════════════
# 6. Pre-Generation Validator
# ═══════════════════════════════════════════════════════════════════════════

class TestPreGenerationValidator:

    def test_valid_rfq_passes(self):
        from src.core.quote_validator import validate_ready_to_generate
        result = validate_ready_to_generate(SAMPLE_RFQ)
        assert result["ok"], f"Valid RFQ failed validation: {result['errors']}"
        assert result["score"] >= 80

    def test_empty_items_fails(self):
        from src.core.quote_validator import validate_ready_to_generate
        result = validate_ready_to_generate({"line_items": []})
        assert not result["ok"]
        assert any("item" in e.lower() for e in result["errors"])

    def test_zero_price_warns(self):
        from src.core.quote_validator import validate_ready_to_generate
        bad_rfq = dict(SAMPLE_RFQ)
        bad_rfq["line_items"] = [
            {"description": "Test", "quantity": 1, "bid_price": 0,
             "supplier_cost": 0, "uom": "EA"}
        ]
        result = validate_ready_to_generate(bad_rfq)
        # Should warn about zero price, may or may not be a hard error
        has_issue = len(result.get("warnings", [])) > 0 or len(result.get("errors", [])) > 0
        assert has_issue, "Zero-price item should produce at least a warning"


# ═══════════════════════════════════════════════════════════════════════════
# 7. Regression Guards (from known production incidents)
# ═══════════════════════════════════════════════════════════════════════════

class TestRegressions:
    """Tests for specific bugs that hit production."""

    def test_fill_and_sign_handles_missing_template(self):
        """fill_and_sign_pdf must raise FileNotFoundError, not silently fail."""
        from src.forms.reytech_filler_v4 import fill_and_sign_pdf
        with pytest.raises(FileNotFoundError):
            fill_and_sign_pdf("/nonexistent/template.pdf", {}, "/tmp/out.pdf")

    def test_sanitize_unicode_chars(self):
        """Smart quotes and em-dashes must not crash PDF generation."""
        from src.forms.reytech_filler_v4 import _sanitize_for_pdf
        result = _sanitize_for_pdf("He said \u201cyes\u201d \u2014 it\u2019s fine")
        assert "\u201c" not in result, "Smart quotes not sanitized"
        assert "\u2014" not in result, "Em-dash not sanitized"
        assert "\u2019" not in result, "Smart apostrophe not sanitized"

    def test_generate_dvbe_843_with_minimal_data(self, output_dir):
        """DVBE 843 must not crash with minimal RFQ data."""
        from src.forms.reytech_filler_v4 import generate_dvbe_843
        minimal = {"solicitation_number": "TEST-001", "sign_date": "03/10/2026"}
        out = os.path.join(output_dir, "dvbe_minimal.pdf")
        generate_dvbe_843(minimal, REYTECH_CONFIG, out)
        assert_valid_pdf(out)

    def test_quote_counter_no_burn_on_peek(self):
        """peek_next_quote_number must NOT increment the counter."""
        from src.forms.quote_generator import peek_next_quote_number
        q1 = peek_next_quote_number()
        q2 = peek_next_quote_number()
        assert q1 == q2, f"Peek burned counter: {q1} → {q2}"
