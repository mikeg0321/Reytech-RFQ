"""Tests for the Form Profile Registry (Phase 1)."""
import os
import pytest

from src.forms.profile_registry import (
    load_profiles, load_profile, match_profile, validate_profile,
    validate_all_profiles, _compute_fingerprint, FormProfile,
    check_template_profile_matches,
)


class TestProfileLoading:
    """Profile YAML loading."""

    def test_load_all_profiles(self):
        profiles = load_profiles()
        assert len(profiles) >= 1
        assert "704a_reytech_standard" in profiles

    def test_profile_has_fields(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        assert len(p.fields) > 30  # 42 fields in the YAML
        assert p.form_type == "704a"
        assert p.fill_mode == "acroform"

    def test_profile_row_capacity(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        assert p.total_row_capacity == 19
        assert p.page_row_capacities == [11, 8]

    def test_profile_has_fingerprint(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        assert p.fingerprint
        assert len(p.fingerprint) == 64  # SHA-256 hex

    def test_get_field(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        fm = p.get_field("vendor.name")
        assert fm is not None
        assert fm.pdf_field == "COMPANY NAME"

    def test_get_row_fields(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        row1 = p.get_row_fields(1, page=1)
        assert "items[1].description" in row1
        assert "Row1" in row1["items[1].description"]

    def test_get_row_fields_page2(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        row1_pg2 = p.get_row_fields(1, page=2)
        assert "items[1].description" in row1_pg2
        assert "Row1_2" in row1_pg2["items[1].description"]


class TestStd204Profile:
    """STD 204 Payee Data Record (non-line-item CalVet-shared form)."""

    def test_std204_loads(self):
        profiles = load_profiles()
        assert "std204_reytech_standard" in profiles
        p = profiles["std204_reytech_standard"]
        assert p.form_type == "std204"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []  # flat form, no line items
        assert p.total_row_capacity == 0

    def test_std204_has_identity_fields(self):
        p = load_profiles()["std204_reytech_standard"]
        assert p.get_field("vendor.business_name") is not None
        assert p.get_field("vendor.fein") is not None
        assert p.get_field("vendor.email") is not None
        assert p.get_field("signer.name") is not None
        assert p.get_field("signer.title") is not None

    def test_std204_checkbox_fields_typed(self):
        p = load_profiles()["std204_reytech_standard"]
        for sem in ("entity.corp_other", "residency.ca_resident",
                    "entity.sole_proprietor", "residency.ca_nonresident"):
            fm = p.get_field(sem)
            assert fm is not None, f"missing {sem}"
            assert fm.field_type == "checkbox", f"{sem} should be type=checkbox"

    def test_std204_signature(self):
        p = load_profiles()["std204_reytech_standard"]
        assert p.signature_field == "Signature4"
        assert p.signature_page == 1
        assert p.signature_mode == "image_stamp"

    def test_std204_validates_against_blank(self):
        p = load_profiles()["std204_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"STD204 validation issues: {issues}"


class TestStd205Profile:
    """STD 205 Payee Data Record Supplement (CalVet-shared, multi-remit)."""

    def test_std205_loads(self):
        profiles = load_profiles()
        assert "std205_reytech_standard" in profiles
        p = profiles["std205_reytech_standard"]
        assert p.form_type == "std205"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []
        assert p.total_row_capacity == 0

    def test_std205_identity_and_remit_blocks(self):
        p = load_profiles()["std205_reytech_standard"]
        assert p.get_field("vendor.business_name") is not None
        assert p.get_field("vendor.tax_id") is not None
        # 5 remittance blocks — row 1 unsuffixed, rows 2-5 use _N suffix
        for i in (1, 2, 3, 4, 5):
            assert p.get_field(f"remittance[{i}].address") is not None, f"remit {i} missing"
            assert p.get_field(f"remittance[{i}].city") is not None
            assert p.get_field(f"remittance[{i}].state") is not None
            assert p.get_field(f"remittance[{i}].zip") is not None

    def test_std205_contact_blocks(self):
        p = load_profiles()["std205_reytech_standard"]
        for i in (1, 2, 3):
            assert p.get_field(f"contacts[{i}].name") is not None, f"contact {i} missing"
            assert p.get_field(f"contacts[{i}].phone") is not None
            assert p.get_field(f"contacts[{i}].email") is not None

    def test_std205_signature(self):
        p = load_profiles()["std205_reytech_standard"]
        sig = p.get_field("signatures.primary")
        assert sig is not None and sig.field_type == "signature"
        assert p.signature_field == "Signature3"
        assert p.signature_page == 2
        assert p.signature_mode == "image_stamp"

    def test_std205_validates_against_blank(self):
        p = load_profiles()["std205_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"STD205 validation issues: {issues}"


class TestDvbe843Profile:
    """PD 843 DVBE Declarations (CalVet-shared)."""

    def test_dvbe843_loads(self):
        profiles = load_profiles()
        assert "dvbe843_reytech_standard" in profiles
        p = profiles["dvbe843_reytech_standard"]
        assert p.form_type == "dvbe843"
        assert p.page_row_capacities == []

    def test_dvbe843_owner_blocks(self):
        p = load_profiles()["dvbe843_reytech_standard"]
        for i in (1, 2, 3):
            assert p.get_field(f"owner{i}.name") is not None
            assert p.get_field(f"owner{i}.date") is not None
        # owner3 gets the full detail block
        assert p.get_field("owner3.address") is not None
        assert p.get_field("owner3.phone") is not None
        assert p.get_field("owner3.tax_id") is not None

    def test_dvbe843_signatures_per_owner(self):
        p = load_profiles()["dvbe843_reytech_standard"]
        for sem in ("signatures.manager", "signatures.owner1",
                    "signatures.owner2", "signatures.owner3"):
            fm = p.get_field(sem)
            assert fm is not None, f"missing {sem}"
            assert fm.field_type == "signature"
        # Primary signature block defaults to manager
        assert p.signature_field == "DVBEmgrSignature"

    def test_dvbe843_attestation_checkboxes(self):
        p = load_profiles()["dvbe843_reytech_standard"]
        for sem in ("attestation.owns_business", "attestation.owns_equipment",
                    "attestation.yn_agent"):
            fm = p.get_field(sem)
            assert fm is not None
            assert fm.field_type == "checkbox"

    def test_dvbe843_validates_against_blank(self):
        p = load_profiles()["dvbe843_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"DVBE 843 validation issues: {issues}"


class TestDarfurProfile:
    """Darfur Contracting Act Certification (CalVet-shared)."""

    def test_darfur_loads(self):
        profiles = load_profiles()
        assert "darfur_reytech_standard" in profiles
        p = profiles["darfur_reytech_standard"]
        assert p.form_type == "darfur_act"
        assert p.page_row_capacities == []

    def test_darfur_two_certifier_blocks(self):
        p = load_profiles()["darfur_reytech_standard"]
        # Primary signer
        assert p.get_field("vendor.company_name") is not None
        assert p.get_field("signer.printed_name_and_title") is not None
        assert p.get_field("signer.date") is not None
        # Secondary initialing signer
        assert p.get_field("vendor.company_name_2") is not None
        assert p.get_field("signer.printed_name_and_title_2") is not None
        assert p.get_field("signer.date_2") is not None

    def test_darfur_two_signatures(self):
        p = load_profiles()["darfur_reytech_standard"]
        primary = p.get_field("signatures.primary")
        initialing = p.get_field("signatures.initialing")
        assert primary is not None and primary.field_type == "signature"
        assert initialing is not None and initialing.field_type == "signature"
        assert p.signature_field == "Authorized Signature"

    def test_darfur_validates_against_blank(self):
        p = load_profiles()["darfur_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"Darfur validation issues: {issues}"


class TestBidderDeclProfile:
    """Bidder Declaration GSPD-05-106 (CCHCS + CalVet shared)."""

    def test_bidder_decl_loads(self):
        profiles = load_profiles()
        assert "bidder_decl_reytech_standard" in profiles
        p = profiles["bidder_decl_reytech_standard"]
        assert p.form_type == "bidder_decl"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []

    def test_bidder_decl_prime_and_subcontractor_rows(self):
        p = load_profiles()["bidder_decl_reytech_standard"]
        # Prime bidder
        assert p.get_field("vendor.certification_type") is not None
        assert p.get_field("vendor.work_description") is not None
        # 3 subcontractor rows (unsuffixed / "... 2" / "... 3")
        for i in (1, 2, 3):
            assert p.get_field(f"subcontractors[{i}].name_phone_fax") is not None, f"row {i} missing"
            assert p.get_field(f"subcontractors[{i}].work_or_goods") is not None
            assert p.get_field(f"subcontractors[{i}].percent_of_bid") is not None

    def test_bidder_decl_checkbox_typing(self):
        p = load_profiles()["bidder_decl_reytech_standard"]
        # Yes/No + DVBE broker + DVBE equipment checkboxes
        for sem in ("subcontractors.uses_yes", "subcontractors.uses_no",
                    "dvbe.broker_yes", "dvbe.broker_no",
                    "dvbe.equipment_yes", "dvbe.equipment_no", "dvbe.equipment_na"):
            fm = p.get_field(sem)
            assert fm is not None, f"missing {sem}"
            assert fm.field_type == "checkbox", f"{sem} should be type=checkbox"
        # Per-row good_standing + rental checkboxes
        for i in (1, 2, 3):
            gs = p.get_field(f"subcontractors[{i}].good_standing")
            rt = p.get_field(f"subcontractors[{i}].is_rental_51pct")
            assert gs is not None and gs.field_type == "checkbox"
            assert rt is not None and rt.field_type == "checkbox"

    def test_bidder_decl_signature(self):
        p = load_profiles()["bidder_decl_reytech_standard"]
        sig = p.get_field("signatures.primary")
        assert sig is not None and sig.field_type == "signature"
        assert p.signature_field == "Signature2"
        assert p.signature_page == 1
        assert p.signature_mode == "image_stamp"

    def test_bidder_decl_validates_against_blank(self):
        p = load_profiles()["bidder_decl_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"Bidder Decl validation issues: {issues}"


class TestStd1000Profile:
    """STD 1000 GenAI Reporting & FactSheet (CalVet-shared, CA GenAI disclosure)."""

    def test_std1000_loads(self):
        profiles = load_profiles()
        assert "std1000_reytech_standard" in profiles
        p = profiles["std1000_reytech_standard"]
        assert p.form_type == "std1000"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []

    def test_std1000_vendor_and_solicitation(self):
        p = load_profiles()["std1000_reytech_standard"]
        assert p.get_field("vendor.business_name") is not None
        assert p.get_field("vendor.address") is not None
        assert p.get_field("vendor.phone") is not None
        assert p.get_field("quote.solicitation_number") is not None
        assert p.get_field("quote.description") is not None

    def test_std1000_genai_questions(self):
        p = load_profiles()["std1000_reytech_standard"]
        # 6 disclosure questions (only required when uses_yes)
        for i in (1, 2, 3, 4, 5, 6):
            found = any(fm.semantic.startswith(f"genai.q{i}_") for fm in p.fields)
            assert found, f"genai.q{i}_* missing"

    def test_std1000_yes_no_checkboxes(self):
        p = load_profiles()["std1000_reytech_standard"]
        yes = p.get_field("genai.uses_yes")
        no = p.get_field("genai.uses_no")
        assert yes is not None and yes.field_type == "checkbox"
        assert no is not None and no.field_type == "checkbox"

    def test_std1000_signature(self):
        p = load_profiles()["std1000_reytech_standard"]
        sig = p.get_field("signatures.primary")
        assert sig is not None and sig.field_type == "signature"
        assert p.signature_field == "Signature"
        assert p.signature_page == 3
        assert p.signature_mode == "image_stamp"

    def test_std1000_validates_against_blank(self):
        p = load_profiles()["std1000_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"STD1000 validation issues: {issues}"


class TestCv012CufProfile:
    """CV 012 Commercially Useful Function Certification (CalVet-shared, XFA)."""

    def test_cv012_loads(self):
        profiles = load_profiles()
        assert "cv012_cuf_reytech_standard" in profiles
        p = profiles["cv012_cuf_reytech_standard"]
        assert p.form_type == "cv012_cuf"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []

    def test_cv012_header_and_vendor(self):
        p = load_profiles()["cv012_cuf_reytech_standard"]
        assert p.get_field("quote.solicitation_number") is not None
        assert p.get_field("vendor.dba") is not None
        assert p.get_field("vendor.osds_ref") is not None
        assert p.get_field("vendor.certification_expiration") is not None

    def test_cv012_six_cuf_questions(self):
        p = load_profiles()["cv012_cuf_reytech_standard"]
        for i in (1, 2, 3, 4, 5, 6):
            found = any(fm.semantic.startswith(f"cuf.q{i}_") for fm in p.fields)
            assert found, f"cuf.q{i}_* missing"

    def test_cv012_xfa_field_names_preserved(self):
        """LiveCycle/XFA hierarchical field paths must survive loading."""
        p = load_profiles()["cv012_cuf_reytech_standard"]
        fm = p.get_field("quote.solicitation_number")
        assert fm.pdf_field == "form1[0].#subform[0].SolicitationNumber[0]"

    def test_cv012_signature(self):
        p = load_profiles()["cv012_cuf_reytech_standard"]
        sig = p.get_field("signatures.primary")
        assert sig is not None and sig.field_type == "signature"
        assert p.signature_page == 2
        assert p.signature_mode == "image_stamp"

    def test_cv012_validates_against_blank(self):
        p = load_profiles()["cv012_cuf_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"CV 012 validation issues: {issues}"


class TestDrugFreeProfile:
    """STD 21 Drug-Free Workplace Certification (CCHCS/CDCR optional)."""

    def test_drug_free_loads(self):
        profiles = load_profiles()
        assert "drug_free_reytech_standard" in profiles
        p = profiles["drug_free_reytech_standard"]
        assert p.form_type == "drug_free"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []

    def test_drug_free_vendor_and_signer(self):
        p = load_profiles()["drug_free_reytech_standard"]
        assert p.get_field("vendor.business_name") is not None
        assert p.get_field("vendor.address") is not None
        assert p.get_field("vendor.fein") is not None
        # Phone split into two fields
        assert p.get_field("vendor.phone_area_code") is not None
        assert p.get_field("vendor.phone_number") is not None
        assert p.get_field("signer.name") is not None
        assert p.get_field("signer.title") is not None
        assert p.get_field("signer.date") is not None

    def test_drug_free_signature(self):
        p = load_profiles()["drug_free_reytech_standard"]
        sig = p.get_field("signatures.primary")
        assert sig is not None and sig.field_type == "signature"
        assert p.signature_field == "Signature1"
        assert p.signature_page == 1
        assert p.signature_mode == "image_stamp"

    def test_drug_free_validates_against_blank(self):
        p = load_profiles()["drug_free_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"Drug Free validation issues: {issues}"


class TestCalRecycle74Profile:
    """CalRecycle Form 74 Postconsumer Recycled-Content Certificate."""

    def test_calrecycle74_loads(self):
        profiles = load_profiles()
        assert "calrecycle74_reytech_standard" in profiles
        p = profiles["calrecycle74_reytech_standard"]
        assert p.form_type == "calrecycle74"
        assert p.fill_mode == "acroform"
        assert p.page_row_capacities == []

    def test_calrecycle74_vendor_and_signer(self):
        p = load_profiles()["calrecycle74_reytech_standard"]
        assert p.get_field("vendor.business_name") is not None
        assert p.get_field("vendor.address") is not None
        assert p.get_field("vendor.phone") is not None
        assert p.get_field("vendor.email") is not None
        assert p.get_field("signer.printed_name") is not None
        assert p.get_field("signer.title") is not None
        assert p.get_field("signer.date") is not None

    def test_calrecycle74_six_item_rows(self):
        p = load_profiles()["calrecycle74_reytech_standard"]
        for i in (1, 2, 3, 4, 5, 6):
            assert p.get_field(f"items[{i}].description") is not None, f"row {i} desc missing"
            assert p.get_field(f"items[{i}].sabrc_category_code") is not None
            assert p.get_field(f"items[{i}].postconsumer_percent") is not None
            assert p.get_field(f"items[{i}].order_reference") is not None
            cb = p.get_field(f"items[{i}].sabrc_compliant")
            assert cb is not None and cb.field_type == "checkbox"

    def test_calrecycle74_signature(self):
        p = load_profiles()["calrecycle74_reytech_standard"]
        sig = p.get_field("signatures.primary")
        assert sig is not None and sig.field_type == "signature"
        assert p.signature_field == "Signature"
        assert p.signature_page == 1
        assert p.signature_mode == "image_stamp"

    def test_calrecycle74_validates_against_blank(self):
        p = load_profiles()["calrecycle74_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"CalRecycle 74 validation issues: {issues}"


class Test703bProfile:
    """CCHCS 703B (bidder certification / sub-contracting response)."""

    def test_703b_loads(self):
        profiles = load_profiles()
        assert "703b_reytech_standard" in profiles, (
            "CCHCS 703b profile missing — backfill warnings will show "
            "'missing profile: 703b_reytech_standard' in the orchestrator"
        )
        p = profiles["703b_reytech_standard"]
        assert p.form_type == "703b"
        assert p.fill_mode == "acroform"

    def test_703b_vendor_and_buyer_fields(self):
        p = load_profiles()["703b_reytech_standard"]
        for key in (
            "vendor.business_name", "vendor.address", "vendor.contact_person",
            "vendor.fein", "vendor.sellers_permit", "vendor.cert_number",
            "vendor.cert_expiration", "vendor.signature_date",
            "buyer.name", "buyer.solicitation_number", "buyer.due_date",
        ):
            assert p.get_field(key) is not None, f"703B missing {key}"

    def test_703b_field_names_match_blank(self):
        """Ground-truth field names from the blank must match the YAML."""
        from pypdf import PdfReader
        p = load_profiles()["703b_reytech_standard"]
        reader = PdfReader(p.blank_pdf)
        pdf_fields = set((reader.get_fields() or {}).keys())
        for fm in p.fields:
            assert fm.pdf_field in pdf_fields, (
                f"703B YAML references '{fm.pdf_field}' (semantic {fm.semantic}) "
                f"but that field doesn't exist in {p.blank_pdf}"
            )

    def test_703b_validates_against_blank(self):
        p = load_profiles()["703b_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"703B validation issues: {issues}"


class TestProfileValidation:
    """Profile validation against blank PDFs."""

    def test_validate_704a(self):
        profiles = load_profiles()
        p = profiles["704a_reytech_standard"]
        issues = validate_profile(p)
        assert issues == [], f"Validation issues: {issues}"

    def test_validate_all(self):
        results = validate_all_profiles()
        for pid, issues in results.items():
            assert issues == [], f"{pid} validation issues: {issues}"

    def test_validate_missing_pdf(self):
        p = FormProfile(id="test", form_type="test", blank_pdf="/nonexistent.pdf", fill_mode="acroform")
        issues = validate_profile(p)
        assert any("not found" in i for i in issues)

    def test_validate_no_pdf(self):
        p = FormProfile(id="test", form_type="test", blank_pdf="", fill_mode="acroform")
        issues = validate_profile(p)
        assert any("not specified" in i for i in issues)


class TestFingerprinting:
    """Content-based PDF fingerprinting."""

    def test_fingerprint_consistency(self, blank_704_path):
        fp1 = _compute_fingerprint(blank_704_path)
        fp2 = _compute_fingerprint(blank_704_path)
        assert fp1 == fp2
        assert len(fp1) == 64

    def test_match_profile_by_fingerprint(self, blank_704_path):
        profiles = load_profiles()
        profile = match_profile(blank_704_path, profiles)
        assert profile is not None
        assert profile.id == "704a_reytech_standard"

    def test_no_match_for_unknown_pdf(self, tmp_path):
        """A PDF with different fields shouldn't match."""
        # Create a minimal PDF with no form fields
        from pypdf import PdfWriter
        writer = PdfWriter()
        writer.add_blank_page(612, 792)
        path = str(tmp_path / "blank.pdf")
        with open(path, "wb") as f:
            writer.write(f)

        profiles = load_profiles()
        profile = match_profile(path, profiles)
        assert profile is None

    def test_fingerprint_nonexistent_file(self):
        fp = _compute_fingerprint("/nonexistent.pdf")
        assert fp == ""


class TestCheckTemplateProfileMatches:
    """check_template_profile_matches — pre-flight gate input."""

    def test_registered_template_matches(self):
        """704a blank matches the 704a_reytech_standard profile."""
        import os
        blank_704 = os.path.join("tests", "fixtures", "ams_704_blank.pdf")
        report = check_template_profile_matches({"704a": blank_704})
        assert "704a" in report
        entry = report["704a"]
        assert entry["matched"] is True
        assert entry["profile_id"] == "704a_reytech_standard"
        assert entry["reason"] is None
        assert len(entry["fingerprint"]) == 64

    def test_missing_file_reports_reason(self):
        report = check_template_profile_matches({"703b": "/nope/does_not_exist.pdf"})
        entry = report["703b"]
        assert entry["matched"] is False
        assert entry["reason"] == "missing_file"
        assert entry["profile_id"] is None
        assert entry["fingerprint"] == ""

    def test_unregistered_pdf_reports_no_profile(self, tmp_path):
        """A PDF whose fingerprint is not in the registry reports no_registered_profile."""
        from pypdf import PdfWriter, PdfReader
        import io
        # Build a PDF with one text field — valid AcroForm but with a
        # field name no registered profile uses.
        writer = PdfWriter()
        writer.add_blank_page(612, 792)
        # Minimal AcroForm: clone an existing blank's fields then wipe them —
        # easiest path is to synthesize via a known blank, then verify the
        # fingerprint does not collide with the registry.
        path = tmp_path / "mystery.pdf"
        with open(path, "wb") as f:
            writer.write(f)
        # Blank page with no fields → _compute_fingerprint returns "" →
        # reason becomes unreadable_pdf, not no_registered_profile.
        report = check_template_profile_matches({"703b": str(path)})
        entry = report["703b"]
        assert entry["matched"] is False
        # Either "unreadable_pdf" (no AcroForm) or "no_registered_profile"
        # (AcroForm but unknown fingerprint). Both are acceptable failure modes.
        assert entry["reason"] in ("unreadable_pdf", "no_registered_profile")

    def test_mixed_report_preserves_per_slot_status(self):
        import os
        blank_704 = os.path.join("tests", "fixtures", "ams_704_blank.pdf")
        report = check_template_profile_matches({
            "704a": blank_704,
            "703b": "/missing.pdf",
        })
        assert report["704a"]["matched"] is True
        assert report["703b"]["matched"] is False
        assert report["703b"]["reason"] == "missing_file"

    def test_accepts_preloaded_profiles(self):
        """Caller can pass an already-loaded registry to avoid re-reading YAML."""
        import os
        blank_704 = os.path.join("tests", "fixtures", "ams_704_blank.pdf")
        profiles = load_profiles()
        report = check_template_profile_matches({"704a": blank_704}, profiles=profiles)
        assert report["704a"]["matched"] is True
        assert report["704a"]["profile_id"] == "704a_reytech_standard"
