"""Tests for the Email-as-Contract requirements check in pc_qa_agent.

The buyer's email is a contract. When the extractor finds "food items require
OBS-1600" or "must be delivered by 2026-05-01", the QA pipeline must surface
those gaps before send.
"""
import json
import pytest

from src.agents.pc_qa_agent import run_qa, CAT_REQUIREMENTS, WARNING, INFO


def _pc(requirements, items=None, output_files=None):
    """Minimal PC shaped like the real thing — enough fields so run_qa won't
    crash on unrelated checks."""
    return {
        "id": "test-pc",
        "pc_number": "TEST-001",
        "agency": "CCHCS",
        "ship_to": "CCHCS HQ",
        "requirements_json": json.dumps(requirements) if isinstance(requirements, dict) else requirements,
        "output_files": output_files or [],
        "items": items or [
            {
                "item_number": "1",
                "description": "Test item",
                "qty": 1,
                "no_bid": False,
                "unit_price": 100.0,
                "pricing": {"unit_cost": 80.0, "recommended_price": 100.0},
            }
        ],
        "profit_summary": {"total_revenue": 100.0, "total_cost": 80.0, "gross_profit": 20.0},
    }


class TestRequirementsCheck:
    """Email-as-contract gap detection in PC QA."""

    def test_missing_required_form_surfaces_as_warning(self):
        """Email says certified SB/MB required, package has nothing → gap."""
        pc = _pc(
            {
                "forms_required": ["sb_mb_cert"],
                "confidence": 0.85,
            },
            output_files=["quote.pdf", "704b_filled.pdf"],
        )
        report = run_qa(pc, use_llm=False)
        req_issues = [i for i in report["issues"] if i.get("category") == CAT_REQUIREMENTS]
        assert len(req_issues) >= 1
        assert req_issues[0]["severity"] == WARNING
        assert "sb_mb_cert" in req_issues[0]["message"].lower() or "sb_mb_cert" in req_issues[0]["field"]

    def test_food_items_missing_obs_1600_surfaces_gap(self):
        """Email mentions food items — OBS 1600 cert is required by CA AMS."""
        pc = _pc(
            {
                "forms_required": [],
                "food_items_present": True,
                "confidence": 0.90,
            },
            output_files=["quote.pdf", "704b.pdf"],
        )
        report = run_qa(pc, use_llm=False)
        req_issues = [i for i in report["issues"] if i.get("category") == CAT_REQUIREMENTS]
        assert len(req_issues) >= 1
        assert any("obs" in (i.get("message") or "").lower() or "1600" in (i.get("message") or "")
                   for i in req_issues), f"Expected OBS-1600 gap, got: {req_issues}"

    def test_satisfied_requirement_does_not_warn(self):
        """When the required form IS in the generated package, no gap fires."""
        pc = _pc(
            {"forms_required": ["darfur_act"], "confidence": 0.85},
            output_files=["quote.pdf", "704b.pdf", "darfur_act_cert.pdf"],
        )
        report = run_qa(pc, use_llm=False)
        req_issues = [i for i in report["issues"] if i.get("category") == CAT_REQUIREMENTS]
        assert len(req_issues) == 0, (
            f"Expected no gaps when darfur_act is present, got: {req_issues}"
        )

    def test_low_extraction_confidence_downgrades_to_info(self):
        """When the extractor has <50% confidence in its requirements, gaps
        get INFO severity, not WARNING — we don't trust them enough to warn."""
        pc = _pc(
            {"forms_required": ["unknown_form"], "confidence": 0.30},
            output_files=["quote.pdf"],
        )
        report = run_qa(pc, use_llm=False)
        req_issues = [i for i in report["issues"] if i.get("category") == CAT_REQUIREMENTS]
        assert len(req_issues) >= 1
        assert req_issues[0]["severity"] == INFO

    def test_no_requirements_json_skips_silently(self):
        """Legacy PCs with no requirements_json must not crash or spam issues."""
        pc = _pc({})  # {} is the empty-requirements marker
        report = run_qa(pc, use_llm=False)
        req_issues = [i for i in report["issues"] if i.get("category") == CAT_REQUIREMENTS]
        assert len(req_issues) == 0

    def test_empty_string_requirements_skips_silently(self):
        pc = _pc("")
        report = run_qa(pc, use_llm=False)
        req_issues = [i for i in report["issues"] if i.get("category") == CAT_REQUIREMENTS]
        assert len(req_issues) == 0
