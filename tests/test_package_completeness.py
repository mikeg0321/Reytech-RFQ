"""Tests for src/forms/package_completeness.py — the agency-agnostic
package completeness gate.

This is the scale-safety contract for CalVet/DGS/DSH/CalFire/etc. A
package is complete iff every required form (from agency_config) is
present in output_files AND passes form_qa. Anything less and the
send is blocked.
"""
import pytest

from src.forms.package_completeness import (
    NON_BLOCKING_FORMS,
    check_package_completeness,
)


# ─── Basic completeness checks ─────────────────────────────────────────

class TestBasicCompleteness:
    def test_empty_required_is_always_complete(self):
        r = check_package_completeness(required_forms=set(), generated_form_ids=set())
        assert r["complete"] is True
        assert r["reasons"] == []

    def test_all_required_present_and_passing(self):
        r = check_package_completeness(
            required_forms={"quote", "dvbe843", "cv012_cuf"},
            generated_form_ids={"quote", "dvbe843", "cv012_cuf"},
            qa_form_results={
                "quote": {"passed": True},
                "dvbe843": {"passed": True},
                "cv012_cuf": {"passed": True},
            },
        )
        assert r["complete"] is True
        assert r["missing_required"] == []
        assert r["failed_required"] == []

    def test_no_qa_results_still_counts_as_passing(self):
        """Back-compat: if qa_form_results is empty, don't penalize
        forms that didn't get a QA verdict. The gate only catches
        forms that explicitly FAILED QA."""
        r = check_package_completeness(
            required_forms={"quote"},
            generated_form_ids={"quote"},
            qa_form_results={},
        )
        assert r["complete"] is True


# ─── Missing required form → hard fail ─────────────────────────────────

class TestMissingRequiredForms:
    def test_missing_single_required_form_is_incomplete(self):
        r = check_package_completeness(
            required_forms={"quote", "dvbe843", "cv012_cuf"},
            generated_form_ids={"quote", "dvbe843"},  # missing cv012_cuf
        )
        assert r["complete"] is False
        assert r["missing_required"] == ["cv012_cuf"]
        assert "cv012_cuf" in r["reasons"][0]

    def test_missing_multiple_required_forms(self):
        r = check_package_completeness(
            required_forms={"quote", "dvbe843", "cv012_cuf", "std204"},
            generated_form_ids={"quote"},
        )
        assert r["complete"] is False
        assert set(r["missing_required"]) == {"dvbe843", "cv012_cuf", "std204"}
        assert r["missing_count"] == 3

    def test_missing_non_blocking_form_does_not_fail(self):
        """obs_1600 is in the non-blocking set — missing it should
        not flip complete to False even if it's in required_forms."""
        r = check_package_completeness(
            required_forms={"quote", "obs_1600"},
            generated_form_ids={"quote"},
        )
        assert r["complete"] is True
        assert r["missing_required"] == []


# ─── Failed QA on required form → hard fail ───────────────────────────

class TestFailedQaOnRequired:
    def test_generated_but_failed_qa_is_incomplete(self):
        r = check_package_completeness(
            required_forms={"quote", "dvbe843"},
            generated_form_ids={"quote", "dvbe843"},
            qa_form_results={
                "quote": {"passed": True},
                "dvbe843": {"passed": False, "issues": ["signature missing"]},
            },
        )
        assert r["complete"] is False
        assert r["failed_required"] == ["dvbe843"]
        assert "dvbe843" in r["reasons"][0]

    def test_failed_qa_on_non_required_does_not_fail(self):
        """Optional forms that failed QA are warnings, not blockers.
        Example: STD 1000 (GenAI attestation) isn't required by every
        agency — if it generates but fails QA, don't block the send."""
        r = check_package_completeness(
            required_forms={"quote"},
            generated_form_ids={"quote", "std1000"},
            qa_form_results={
                "quote": {"passed": True},
                "std1000": {"passed": False},  # not required — warning only
            },
        )
        assert r["complete"] is True


# ─── Combined missing + failed ─────────────────────────────────────────

class TestCombinedFailures:
    def test_both_missing_and_failed_reported(self):
        r = check_package_completeness(
            required_forms={"quote", "dvbe843", "cv012_cuf"},
            generated_form_ids={"quote", "dvbe843"},  # cv012_cuf missing
            qa_form_results={
                "quote": {"passed": True},
                "dvbe843": {"passed": False},  # failed
            },
        )
        assert r["complete"] is False
        assert r["missing_required"] == ["cv012_cuf"]
        assert r["failed_required"] == ["dvbe843"]
        assert len(r["reasons"]) == 2  # one for each category


# ─── Agency-specific required-form sets ─────────────────────────────────
# These pin the current agency_config.required_forms values so any
# change to those sets will cause a test failure and force the
# developer to update the tests intentionally.

CALVET_REQUIRED = {
    "quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act",
    "cv012_cuf", "std204", "std205", "std1000", "sellers_permit",
}
DGS_REQUIRED = {
    "quote", "std204", "sellers_permit", "dvbe843", "bidder_decl", "darfur_act",
}
DSH_REQUIRED = {
    "quote", "std204", "sellers_permit", "dvbe843", "bidder_decl",
    "darfur_act", "calrecycle74",
}
CALFIRE_REQUIRED = {
    "quote", "std204", "sellers_permit", "dvbe843",
}


class TestAgencySpecificCompleteness:
    def test_calvet_happy_path(self):
        """Full CalVet required set generated and passing — complete."""
        r = check_package_completeness(
            required_forms=CALVET_REQUIRED,
            generated_form_ids=CALVET_REQUIRED,
            qa_form_results={f: {"passed": True} for f in CALVET_REQUIRED},
        )
        assert r["complete"] is True, f"CalVet happy path blocked: {r['reasons']}"

    def test_calvet_missing_cv012_cuf_blocks(self):
        """CalVet without CV 012 CUF must be INCOMPLETE — this is
        exactly the failure mode Mike is guarding against."""
        gen = CALVET_REQUIRED - {"cv012_cuf"}
        r = check_package_completeness(
            required_forms=CALVET_REQUIRED,
            generated_form_ids=gen,
            qa_form_results={f: {"passed": True} for f in gen},
        )
        assert r["complete"] is False
        assert "cv012_cuf" in r["missing_required"]

    def test_calvet_missing_std205_blocks(self):
        """CalVet without STD 205 payee supplement must be INCOMPLETE.
        STD 205 is CalVet-specific per agency_config."""
        gen = CALVET_REQUIRED - {"std205"}
        r = check_package_completeness(
            required_forms=CALVET_REQUIRED,
            generated_form_ids=gen,
            qa_form_results={f: {"passed": True} for f in gen},
        )
        assert r["complete"] is False
        assert "std205" in r["missing_required"]

    def test_calvet_failed_dvbe843_qa_blocks(self):
        """Every required form must PASS QA, not just generate."""
        r = check_package_completeness(
            required_forms=CALVET_REQUIRED,
            generated_form_ids=CALVET_REQUIRED,
            qa_form_results={
                f: {"passed": (f != "dvbe843")} for f in CALVET_REQUIRED
            },
        )
        assert r["complete"] is False
        assert "dvbe843" in r["failed_required"]

    def test_dgs_happy_path(self):
        r = check_package_completeness(
            required_forms=DGS_REQUIRED,
            generated_form_ids=DGS_REQUIRED,
            qa_form_results={f: {"passed": True} for f in DGS_REQUIRED},
        )
        assert r["complete"] is True

    def test_dgs_does_not_require_cv012_cuf(self):
        """DGS doesn't use CV 012 CUF — missing it must NOT fail DGS
        even though it would fail CalVet."""
        r = check_package_completeness(
            required_forms=DGS_REQUIRED,
            generated_form_ids=DGS_REQUIRED,  # no cv012_cuf
        )
        assert r["complete"] is True

    def test_dsh_happy_path(self):
        r = check_package_completeness(
            required_forms=DSH_REQUIRED,
            generated_form_ids=DSH_REQUIRED,
            qa_form_results={f: {"passed": True} for f in DSH_REQUIRED},
        )
        assert r["complete"] is True

    def test_dsh_missing_calrecycle74_blocks(self):
        """DSH requires CalRecycle 74 (unlike DGS) — missing it must fail."""
        gen = DSH_REQUIRED - {"calrecycle74"}
        r = check_package_completeness(
            required_forms=DSH_REQUIRED,
            generated_form_ids=gen,
        )
        assert r["complete"] is False
        assert "calrecycle74" in r["missing_required"]

    def test_calfire_happy_path(self):
        r = check_package_completeness(
            required_forms=CALFIRE_REQUIRED,
            generated_form_ids=CALFIRE_REQUIRED,
            qa_form_results={f: {"passed": True} for f in CALFIRE_REQUIRED},
        )
        assert r["complete"] is True


# ─── Non-blocking form override ────────────────────────────────────────

class TestNonBlockingOverride:
    def test_caller_can_pass_custom_non_blocking_set(self):
        r = check_package_completeness(
            required_forms={"quote", "custom_optional"},
            generated_form_ids={"quote"},
            non_blocking_forms={"custom_optional"},
        )
        assert r["complete"] is True

    def test_default_non_blocking_set_only_content_triggered(self):
        """Only content-triggered forms belong in NON_BLOCKING_FORMS.
        Agency-required forms like std205/std1000/drug_free must block
        when missing because some agencies (CalVet) require them."""
        assert "obs_1600" in NON_BLOCKING_FORMS  # food-triggered
        assert "barstow_cuf" in NON_BLOCKING_FORMS  # location-triggered
        # These are agency-REQUIRED on CalVet, must NOT be non-blocking:
        assert "std205" not in NON_BLOCKING_FORMS
        assert "std1000" not in NON_BLOCKING_FORMS
        assert "drug_free" not in NON_BLOCKING_FORMS
