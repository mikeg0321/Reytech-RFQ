"""Tests for validate_against_requirements + run_form_qa integration.

Covers the Email-as-Contract Phase 2 spec: the buyer's email is the
authoritative source of requirements, and the generated package must
satisfy every listed requirement before being allowed to ship.
"""
import json

import pytest


# ─── validate_against_requirements unit tests ──────────────────────────

class TestValidateAgainstRequirements:
    def test_empty_requirements_returns_pass(self):
        from src.forms.form_qa import validate_against_requirements
        r = validate_against_requirements([], "", {}, {})
        assert r["passed"] is True
        assert r["gaps"] == []

    def test_required_form_present_is_confirmed(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": ["dvbe843"], "confidence": 0.9})
        r = validate_against_requirements(
            ["dvbe_843_blank.pdf", "703b.pdf"], reqs, {}, {}
        )
        assert "dvbe843" in r["confirmed"]
        assert r["passed"] is True

    def test_missing_form_gap_warning_in_lax_mode(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": ["std204"], "confidence": 0.9})
        r = validate_against_requirements(["703b.pdf"], reqs, {}, {}, strict=False)
        assert r["passed"] is True  # lax mode doesn't block
        gap = next(g for g in r["gaps"] if g["form_id"] == "std204")
        assert gap["severity"] == "warning"

    def test_missing_form_critical_in_strict_mode(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"forms_required": ["std204"], "confidence": 0.9})
        r = validate_against_requirements(
            ["703b.pdf"], reqs, {}, {}, strict=True
        )
        assert r["passed"] is False
        gap = next(g for g in r["gaps"] if g["form_id"] == "std204")
        assert gap["severity"] == "critical"

    def test_food_items_trigger_obs1600_suggestion(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({"food_items_present": True, "confidence": 0.8})
        r = validate_against_requirements(["703b.pdf"], reqs, {}, {})
        gaps_for_obs = [g for g in r["gaps"] if g["form_id"] == "obs_1600"]
        assert len(gaps_for_obs) == 1

    def test_deadline_in_past_is_critical(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({
            "due_date": "2020-01-01",
            "confidence": 0.9,
        })
        r = validate_against_requirements([], reqs, {"due_date": "2020-01-01"}, {})
        critical = [g for g in r["gaps"] if g["type"] == "deadline_passed"]
        assert len(critical) == 1
        assert critical[0]["severity"] == "critical"

    def test_cert_expired_is_critical_when_dvbe_required(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({
            "forms_required": ["dvbe843"],
            "confidence": 0.9,
        })
        config = {"company": {"cert_expiration": "2020-01-01"}}
        r = validate_against_requirements(
            ["dvbe843.pdf"], reqs, {}, config
        )
        expired = [g for g in r["gaps"] if g["type"] == "cert_expired"]
        assert len(expired) == 1
        assert expired[0]["severity"] == "critical"

    def test_cert_not_expired_produces_no_gap(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({
            "forms_required": ["dvbe843"],
            "confidence": 0.9,
        })
        config = {"company": {"cert_expiration": "2099-01-01"}}
        r = validate_against_requirements(
            ["dvbe843.pdf"], reqs, {}, config
        )
        expired = [g for g in r["gaps"] if g["type"] == "cert_expired"]
        assert not expired

    def test_solicitation_mismatch_flagged_critical(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({
            "solicitation_number": "10843276",
            "confidence": 0.9,
        })
        rfq = {"solicitation_number": "WRONG-NUMBER"}
        r = validate_against_requirements([], reqs, rfq, {})
        mismatches = [g for g in r["gaps"] if g["type"] == "solicitation_mismatch"]
        assert len(mismatches) == 1
        assert mismatches[0]["severity"] == "critical"

    def test_delivery_location_surfaced_as_info(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({
            "delivery_location": "CA State Prison Sacramento",
            "confidence": 0.9,
        })
        r = validate_against_requirements([], reqs, {}, {})
        deliv = [g for g in r["gaps"] if g["type"] == "delivery_reminder"]
        assert len(deliv) == 1
        assert deliv[0]["severity"] == "info"
        assert "Sacramento" in deliv[0]["msg"]

    def test_low_confidence_extraction_flagged_info(self):
        from src.forms.form_qa import validate_against_requirements
        reqs = json.dumps({
            "forms_required": [],
            "confidence": 0.3,
        })
        r = validate_against_requirements([], reqs, {}, {})
        low_conf = [g for g in r["gaps"] if g["type"] == "low_confidence_extraction"]
        assert len(low_conf) == 1


# ─── run_form_qa integration tests ─────────────────────────────────────

class TestRunFormQaRequirementsIntegration:
    def test_run_form_qa_skips_requirements_when_json_empty(self):
        from src.forms.form_qa import run_form_qa
        report = run_form_qa(
            out_dir="/tmp",
            output_files=[],
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="CDCR",
            required_forms=set(),
            requirements_json="",
        )
        assert "requirements_check" not in report

    def test_run_form_qa_includes_requirements_check_when_provided(self, tmp_path):
        from src.forms.form_qa import run_form_qa
        reqs = json.dumps({
            "forms_required": ["std204"],
            "confidence": 0.9,
        })
        report = run_form_qa(
            out_dir=str(tmp_path),
            output_files=["dvbe843.pdf"],  # missing std204
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="CDCR",
            required_forms=set(),
            requirements_json=reqs,
            strict_requirements=False,
        )
        assert "requirements_check" in report
        assert report["requirements_check"]["gaps"]
        # In lax mode, warnings don't fail the run
        # But package_check might fail for other reasons — check only
        # the requirements contribution
        req_warnings = [w for w in report["warnings"] if "[requirements]" in w]
        assert req_warnings

    def test_run_form_qa_strict_mode_blocks_on_missing_required_form(self, tmp_path):
        from src.forms.form_qa import run_form_qa
        reqs = json.dumps({
            "forms_required": ["std204"],
            "confidence": 0.9,
        })
        report = run_form_qa(
            out_dir=str(tmp_path),
            output_files=["dvbe843.pdf"],
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="CDCR",
            required_forms=set(),
            requirements_json=reqs,
            strict_requirements=True,
        )
        assert report["passed"] is False
        assert any("[requirements]" in i for i in report["critical_issues"])

    def test_run_form_qa_strict_blocks_on_expired_cert(self, tmp_path):
        from src.forms.form_qa import run_form_qa
        reqs = json.dumps({
            "forms_required": ["dvbe843"],
            "confidence": 0.9,
        })
        report = run_form_qa(
            out_dir=str(tmp_path),
            output_files=["dvbe843.pdf"],
            form_id_map=[],
            rfq_data={},
            config={"company": {"cert_expiration": "2020-01-01"}},
            agency_key="CDCR",
            required_forms=set(),
            requirements_json=reqs,
            strict_requirements=True,
        )
        assert report["passed"] is False
        cert_issues = [i for i in report["critical_issues"] if "cert_expired" in i.lower() or "cert" in i.lower()]
        assert cert_issues
