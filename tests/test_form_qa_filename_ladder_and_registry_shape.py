"""Regression: Form QA layer false-positives that flagged a fully-filled
CalVet package as 21-issues incomplete.

Incident 2026-05-01 (rfq_7813c4e1, agency = Cal Vet / DVA):
  /rfq/<id>/review-package showed
    "Form QA Failed — 21 critical issue(s) found"
  with cv012_cuf + std1000 reported as "not generated" (the PDFs were on
  disk, 215KB and 363KB respectively, with 7/8 fields populated on cv012)
  and a wave of "Missing: Phone / Email / State Agency / FEIN / Date"
  warnings for forms whose template uses a ReportLab overlay or buyer-only
  field name.

Three layered bugs in src/forms/form_qa.py, all surgical:

1. `verify_package_completeness` filename ladder lacked branches for
   `cv012`, `std1000`, `std205`, `barstow`, `cuf`, `obs/1600`, `drug`,
   and the DSH attachments. CalVet's required_forms set includes
   cv012_cuf + std1000, but `WORKSHEET_CV012_CUF_Reytech.pdf` and
   `WORKSHEET_STD1000_Reytech.pdf` both fell through to fid='unknown'
   so the required-form gate flunked them.

2. `FORM_FIELD_REGISTRY['calrecycle74']` listed `Phone`, `Email`, and
   `State Agency` as expected vendor fields. Those AcroFields are
   intentionally written with a single-space sentinel by
   `fill_calrecycle_standalone` — they are buyer-side blocks for the
   purchasing agent / state agency to fill at PO time. The vendor's
   phone is `Phone_2`. Listed `Date` in `date_fields` but
   `_calrecycle_fix_date` overlays via ReportLab because the template's
   Date AcroField is unnamed and unfillable through the values dict.

3. `FORM_FIELD_REGISTRY['std204']` listed
   `Federal Employer Identification Number (FEIN)` as an expected field.
   `_overlay_std204_fein` draws the digits via ReportLab because the
   template uses individual underline rectangles per digit (no
   AcroField). Pypdf form-field readback always sees this empty.

These tests pin all three so a future refactor can't reintroduce them.
"""
from __future__ import annotations

import os
import sys

import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.forms.form_qa import (
    FORM_FIELD_REGISTRY,
    verify_package_completeness,
)


# ─── Filename ladder ───────────────────────────────────────────────────

class TestFilenameLadderRecognizesCalVetForms:
    """Without these branches, every CalVet package reports cv012_cuf +
    std1000 as 'Required form not generated' even with the PDFs on disk."""

    def _check(self, filename: str, form_id: str) -> None:
        result = verify_package_completeness(
            agency_key="calvet",
            required_forms={form_id},
            generated_files=[filename],
            has_bid_package=False,
        )
        assert result["passed"], (
            f"{filename!r} did not classify as {form_id!r}; "
            f"verify_package_completeness reported missing={result['missing']!r}"
        )
        assert form_id not in result["missing"]

    def test_cv012_cuf_filename_recognized(self):
        self._check("WORKSHEET_CV012_CUF_Reytech.pdf", "cv012_cuf")

    def test_cv012_uppercase_alias(self):
        self._check("RFQ_99999_CV012_CUF_Reytech.pdf", "cv012_cuf")

    def test_std1000_filename_recognized(self):
        self._check("WORKSHEET_STD1000_Reytech.pdf", "std1000")

    def test_std205_filename_recognized(self):
        self._check("WORKSHEET_STD205_Reytech.pdf", "std205")

    def test_barstow_cuf_filename_recognized(self):
        # barstow MUST be matched before generic 'cuf' so Barstow CUFs
        # don't get mislabelled cv012_cuf.
        self._check("WORKSHEET_BarstowCUF_Reytech.pdf", "barstow_cuf")

    def test_obs_1600_filename_recognized(self):
        self._check("WORKSHEET_OBS1600_Reytech.pdf", "obs_1600")

    def test_drug_free_filename_recognized(self):
        self._check("WORKSHEET_DrugFree_Reytech.pdf", "drug_free")


class TestFilenameLadderRegressionForExistingForms:
    """Pin the existing CCHCS / shared mappings so the new branches
    don't accidentally claim a 703B/704B/quote/bidpkg filename."""

    def _expect(self, filename: str, form_id: str) -> None:
        result = verify_package_completeness(
            agency_key="cchcs",
            required_forms={form_id},
            generated_files=[filename],
            has_bid_package=False,
        )
        assert result["passed"], (
            f"{filename!r} regressed away from {form_id!r}; "
            f"missing={result['missing']!r}"
        )

    def test_quote_still_quote(self):
        self._expect("WORKSHEET_Quote_Reytech.pdf", "quote")

    def test_703b_still_703b(self):
        self._expect("WORKSHEET_703B_Reytech.pdf", "703b")

    def test_704b_still_704b(self):
        self._expect("WORKSHEET_704B_Reytech.pdf", "704b")

    def test_calrecycle_still_calrecycle74(self):
        self._expect("WORKSHEET_CalRecycle74_Reytech.pdf", "calrecycle74")


# ─── FORM_FIELD_REGISTRY shape — calrecycle74 ──────────────────────────

class TestCalRecycle74RegistryHonorsBuyerFields:
    """Buyer-side AcroFields (Phone / Email / State Agency / Purchasing
    Agent / PO) must NOT be listed as vendor required_fields — the
    standalone filler intentionally writes a single-space sentinel into
    them so the buyer can pen-fill at PO time."""

    def test_no_buyer_fields_in_required(self):
        reg = FORM_FIELD_REGISTRY["calrecycle74"]["required_fields"]
        for buyer_field in ("Phone", "Email", "State Agency", "Purchasing Agent", "PO"):
            assert buyer_field not in reg, (
                f"calrecycle74 required_fields includes buyer-side field "
                f"{buyer_field!r} — fill_calrecycle_standalone writes ' ' "
                f"there intentionally; QA-checking it always fails"
            )

    def test_vendor_phone_uses_phone_2(self):
        # Phone_2 is the vendor block; Phone is the state agency's.
        reg = FORM_FIELD_REGISTRY["calrecycle74"]["required_fields"]
        assert "Phone_2" in reg, (
            "calrecycle74 required_fields must check `Phone_2` (vendor "
            "phone), not `Phone` (buyer phone)"
        )
        assert reg["Phone_2"] == "company.phone"

    def test_date_not_in_date_fields(self):
        # _calrecycle_fix_date overlays via ReportLab because Date is an
        # unnamed AcroField — pypdf readback always reports it empty.
        date_fields = FORM_FIELD_REGISTRY["calrecycle74"]["date_fields"]
        assert "Date" not in date_fields, (
            "calrecycle74 Date is overlay-only (`_calrecycle_fix_date`) — "
            "checking the AcroField via pypdf will always report empty"
        )


# ─── FORM_FIELD_REGISTRY shape — std204 ────────────────────────────────

class TestStd204RegistryHonorsOverlayFields:
    """FEIN on STD 204 is rendered via `_overlay_std204_fein` (per-digit
    underline rectangles). It is NOT an AcroField, so listing it as a
    required form field guarantees a false-positive every time."""

    def test_fein_not_in_required_fields(self):
        reg = FORM_FIELD_REGISTRY["std204"]["required_fields"]
        for fein_key in (
            "Federal Employer Identification Number (FEIN)",
            "Federal Employer Identification Number FEIN",
            "FEIN",
        ):
            assert fein_key not in reg, (
                f"std204 required_fields includes overlay-only field "
                f"{fein_key!r} — `_overlay_std204_fein` draws digits via "
                f"ReportLab; pypdf readback always sees it empty"
            )

    def test_required_fields_still_cover_authoritative_block(self):
        # Belt-and-suspenders: removing FEIN must not leave the block
        # under-checked. Identity block (NAME, ADDRESS, REPRESENTATIVE,
        # TITLE) must remain.
        reg = FORM_FIELD_REGISTRY["std204"]["required_fields"]
        assert any("NAME OF AUTHORIZED PAYEE" in k for k in reg)
        assert any("MAILING ADDRESS" in k for k in reg)
        assert "TITLE" in reg


# ─── Cross-cutting: end-to-end completeness on a CalVet package ───────

def test_calvet_complete_package_passes_with_correct_filenames():
    """The exact CalVet output_files shape from rfq_7813c4e1 — with
    cv012_cuf + std1000 PDFs present — must not be flunked by the
    filename ladder. Pre-fix: 2 'not generated' false-positives.
    Post-fix: pkg-level passed=True (field-level checks are exercised
    by the per-form QA path, not this one)."""
    calvet_required = {
        "quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act",
        "cv012_cuf", "std204", "std205", "std1000", "sellers_permit",
    }
    generated = [
        "WORKSHEET_Quote_Reytech.pdf",
        "WORKSHEET_CalRecycle74_Reytech.pdf",
        "WORKSHEET_BidderDecl_Reytech.pdf",
        "WORKSHEET_DVBE843_Reytech.pdf",
        "WORKSHEET_DarfurAct_Reytech.pdf",
        "WORKSHEET_CV012_CUF_Reytech.pdf",
        "WORKSHEET_STD204_Reytech.pdf",
        "WORKSHEET_STD205_Reytech.pdf",
        "WORKSHEET_STD1000_Reytech.pdf",
        "WORKSHEET_SellersPermit_Reytech.pdf",
    ]
    result = verify_package_completeness(
        agency_key="calvet",
        required_forms=calvet_required,
        generated_files=generated,
        has_bid_package=False,
    )
    assert result["passed"] is True, (
        f"CalVet complete package flunked completeness gate: "
        f"missing={result['missing']!r}, issues={result['issues']!r}"
    )
    assert result["missing"] == []
