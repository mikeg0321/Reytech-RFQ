"""Tests for the Form Profile Registry (Phase 1)."""
import os
import pytest

from src.forms.profile_registry import (
    load_profiles, load_profile, match_profile, validate_profile,
    validate_all_profiles, _compute_fingerprint, FormProfile,
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
