"""CCHCS IT RFQ (Non-Cloud) profile regression tests.

Profile built 2026-04-19 from real buyer-issued packet (PREQ10843276) + Reytech's
submitted response. Covers 41 field mappings including vendor block, certification
identifiers, line items (1..10), totals, and the AMS 708 GenAI attachment
(Reytech default = GenAI=No, all disclosure fields N/A).

Why: CCHCS IT procurement ships this form for hardware/software bids distinct from
the AMS 703/704 packet family. Before this profile, Reytech had no automated fill
path for IT RFQs — operator filled every field by hand.
"""
import os
import pytest

from src.forms.profile_registry import load_profiles, validate_profile


PROFILE_ID = "cchcs_it_rfq_reytech_standard"


class TestCchcsItRfqProfileLoad:
    def test_profile_registered(self):
        profiles = load_profiles()
        assert PROFILE_ID in profiles, f"profile missing; have: {sorted(profiles.keys())}"

    def test_profile_validates_against_blank(self):
        """Every pdf_field in the profile must exist in the blank PDF."""
        p = load_profiles()[PROFILE_ID]
        issues = validate_profile(p)
        assert issues == [], f"profile has validation issues: {issues}"

    def test_form_type_and_fill_mode(self):
        p = load_profiles()[PROFILE_ID]
        assert p.form_type == "cchcs_it_rfq"
        assert p.fill_mode == "acroform"

    def test_line_item_capacity(self):
        p = load_profiles()[PROFILE_ID]
        assert p.total_row_capacity == 10


class TestCchcsItRfqFieldCoverage:
    """Profile must cover the 61 Reytech-filled fields observed in the
    PREQ10843276 golden submission — split into core (41 semantic entries
    after templating collapses 60 row fields into 6 `items[n]` templates)."""

    @pytest.fixture
    def profile(self):
        return load_profiles()[PROFILE_ID]

    def test_vendor_block_complete(self, profile):
        required = [
            "vendor.name", "vendor.dba", "vendor.address_1", "vendor.address_2",
            "vendor.contact_name", "vendor.phone", "vendor.email",
        ]
        for sem in required:
            assert profile.get_field(sem) is not None, f"missing: {sem}"

    def test_certification_block(self, profile):
        for sem in ("cert.sb_dvbe_number", "cert.sb_dvbe_expiration",
                    "cert.osds_ref", "cert.ca_reseller_permit", "cert.bidder_vendor_id"):
            assert profile.get_field(sem) is not None, f"missing: {sem}"

    def test_line_items_templated(self, profile):
        row1 = profile.get_row_fields(1, page=1)
        assert "items[1].description" in row1
        assert row1["items[1].description"] == "Item Description1"
        assert row1["items[1].unit_price"] == "Price Per Unit1"
        assert row1["items[1].extension"] == "Extension Total1"

    def test_line_items_row_10_resolves(self, profile):
        """Capacity 10 — row 10 must resolve to concrete field names."""
        row10 = profile.get_row_fields(10, page=1)
        assert row10["items[10].description"] == "Item Description10"

    def test_totals_block(self, profile):
        for sem, expected in [
            ("totals.subtotal", "Extension TotalSubtotal"),
            ("totals.sales_tax", "Extension TotalSales Tax"),
            ("totals.freight", "Extension TotalFOB Destination Freight Prepaid"),
            ("totals.total", "Extension TotalTotal"),
        ]:
            fm = profile.get_field(sem)
            assert fm is not None, f"missing: {sem}"
            assert fm.pdf_field == expected

    def test_genai_attachment_defaults_to_no(self, profile):
        """Reytech is not an AI vendor — AMS 708 GenAI No is checked by default."""
        defaults = profile.raw_yaml.get("defaults", {})
        assert defaults.get("genai.is_genai_no") == "/Yes", (
            "Reytech default must check 'GenAI No'; otherwise downstream legal disclosure "
            "questions trigger. Reason: feedback + project_reytech_canonical_identity.md "
            "confirm no AI product offerings."
        )

    def test_canonical_identity_in_defaults(self, profile):
        """project_reytech_canonical_identity.md: Michael Guadan + sales@reytechinc.com
        on ALL forms. No variants."""
        defaults = profile.raw_yaml.get("defaults", {})
        assert defaults.get("vendor.contact_name") == "Michael Guadan"
        assert defaults.get("vendor.email") == "sales@reytechinc.com"
        assert defaults.get("vendor.name") == "Reytech Inc."


class TestCchcsItRfqRoundTripFill:
    """Prove the profile's field map actually produces a valid filled PDF —
    every declared pdf_field accepts the declared default, PyPDFForm reads
    the values back out, and the output renders without error."""

    def test_fill_blank_with_defaults_and_verify(self, tmp_path):
        import pypdf
        from PyPDFForm import PdfWrapper

        p = load_profiles()[PROFILE_ID]
        defaults = (p.raw_yaml or {}).get("defaults", {}) or {}
        payload = {}
        for semantic, value in defaults.items():
            fm = p.get_field(semantic)
            if fm is None:
                continue
            pdf_field = fm.pdf_field
            # PyPDFForm checkbox convention: True/False for /Yes /Off
            payload[pdf_field] = True if value == "/Yes" else value

        # Also fill line item 1 — the canonical bid example shape.
        row1 = p.get_row_fields(1, page=1)
        payload[row1["items[1].description"]] = "Test item description"
        payload[row1["items[1].part_number"]] = "TEST-PN-001"
        payload[row1["items[1].qty"]] = "5"
        payload[row1["items[1].unit"]] = "EA"
        payload[row1["items[1].unit_price"]] = "100.00"
        payload[row1["items[1].extension"]] = "500.00"

        out_path = tmp_path / "it_rfq_filled.pdf"
        w = PdfWrapper(p.blank_pdf).fill(payload)
        with open(out_path, "wb") as f:
            f.write(w.read())

        assert out_path.exists() and out_path.stat().st_size > 10_000

        # Verify values survive the fill/read round-trip.
        r = pypdf.PdfReader(str(out_path))
        fields = r.get_fields() or {}

        def _v(k):
            o = fields.get(k)
            if not o:
                return None
            v = o.get("/V")
            return v.decode("utf-8", "ignore") if hasattr(v, "decode") else str(v) if v else None

        assert _v("Supplier Name") == "Reytech Inc.", "vendor.name must round-trip"
        assert _v("Contact Name") == "Michael Guadan"
        assert _v("Supplier Email") == "sales@reytechinc.com"
        assert _v("Item Description1") == "Test item description"
        assert _v("Qty1") == "5"
        assert _v("Price Per Unit1") == "100.00"
        # Checkbox fills — PyPDFForm serializes booleans to /Yes
        assert _v("AMS 708 GenAI No") in ("/Yes", "Yes"), "GenAI No checkbox missing after fill"
        assert _v("Check Box27.0.0") in ("/Yes", "Yes"), "compliance.box_27_0_0 missing after fill"


class TestCchcsItRfqGoldenFixture:
    """Sanity-check the committed golden fixture hasn't drifted in shape."""

    GOLDEN = os.path.join(
        os.path.dirname(__file__), "fixtures", "cchcs_it_rfq_reytech_golden.pdf",
    )

    def test_golden_exists(self):
        assert os.path.exists(self.GOLDEN)

    def test_golden_has_reytech_identity(self):
        """The golden submission embeds Reytech canonical identity — if this
        ever drifts, the fixture is corrupt and the profile tests lose meaning."""
        import pypdf
        r = pypdf.PdfReader(self.GOLDEN)
        fields = r.get_fields() or {}
        def _v(k):
            o = fields.get(k)
            if not o:
                return None
            v = o.get("/V")
            return v.decode("utf-8", "ignore") if hasattr(v, "decode") else str(v) if v else None

        assert _v("Supplier Name") == "Reytech Inc."
        assert _v("Contact Name") == "Michael Guadan"
        assert _v("Supplier Email") == "sales@reytechinc.com"
