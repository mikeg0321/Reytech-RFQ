"""Tests that validate_package() surfaces a warning when the LLM gap check
was skipped instead of silently pretending it ran.

Today, `_run_llm_gap_check` returns `[]` for any of:
    * buyer_email_text empty
    * `anthropic` module not installed
    * `ANTHROPIC_API_KEY` not set
    * the LLM call itself raised

…and `validate_package` records the check as `{"name": "llm_gap", "ok": True,
"detail": "0 gap(s)"}`. From the operator's POV that is indistinguishable
from a real LLM run that found 0 gaps. We have no signal that the LLM
portion didn't actually happen, so an incomplete-but-clean-deterministic
package can sail through QA without the LLM ever weighing in.

Fix: when the LLM gap check is skipped, the `llm_gap` check entry must
report ok=False with the reason in `detail`, and `warnings` must include
a "compliance: LLM gap check skipped: <reason>" entry so the orchestrator
bubble (PR #178) surfaces it on the result.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from src.agents.compliance_validator import validate_package
from src.core.quote_model import Quote, DocType, LineItem, QuoteStatus


def _quote() -> Quote:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze 4x4", qty=10, unit_cost=Decimal("2.00"))],
        status=QuoteStatus.PRICED,
    )
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    q.buyer.requestor_email = "buyer@cchcs.ca.gov"
    return q


def _per_form_clean() -> list[dict]:
    """A per-form report that satisfies the deterministic checks for cchcs."""
    return [
        {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True, "bytes": 12345},
        {"profile_id": "703bc_reytech_standard", "filled": True, "qa_passed": True, "bytes": 12345},
        {"profile_id": "quote_reytech_letterhead", "filled": True, "qa_passed": True, "bytes": 12345},
    ]


class TestLlmGapCheckSkipSurfacesWarning:
    def test_skipped_when_buyer_email_empty(self):
        result = validate_package(
            quote=_quote(),
            per_form_reports=_per_form_clean(),
            buyer_email_text="",
        )
        llm_check = next(c for c in result["checks"] if c["name"] == "llm_gap")
        assert llm_check["ok"] is False, (
            f"llm_gap should report ok=False when skipped — got {llm_check}"
        )
        assert "skipped" in llm_check["detail"].lower(), llm_check
        assert "buyer email" in llm_check["detail"].lower(), llm_check
        assert any(
            "llm gap check skipped" in w.lower() and "buyer email" in w.lower()
            for w in result["warnings"]
        ), f"warnings missing skip notice: {result['warnings']}"

    def test_skipped_when_no_api_key(self):
        with patch.dict("os.environ", {}, clear=True):
            result = validate_package(
                quote=_quote(),
                per_form_reports=_per_form_clean(),
                buyer_email_text="Please include the DVBE certificate.",
            )
        llm_check = next(c for c in result["checks"] if c["name"] == "llm_gap")
        assert llm_check["ok"] is False, llm_check
        assert "api" in llm_check["detail"].lower() and "key" in llm_check["detail"].lower(), llm_check
        assert any(
            "llm gap check skipped" in w.lower() and "api" in w.lower() and "key" in w.lower()
            for w in result["warnings"]
        ), result["warnings"]

    def test_skipped_when_llm_call_fails(self):
        """An exception inside the LLM call must mark the check skipped, not pass it."""
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-fake"}):
            with patch(
                "src.agents.compliance_validator._invoke_llm_gap_check",
                side_effect=RuntimeError("anthropic 529 overloaded"),
            ):
                result = validate_package(
                    quote=_quote(),
                    per_form_reports=_per_form_clean(),
                    buyer_email_text="Please include the DVBE certificate.",
                )
        llm_check = next(c for c in result["checks"] if c["name"] == "llm_gap")
        assert llm_check["ok"] is False, llm_check
        assert "529" in llm_check["detail"] or "overloaded" in llm_check["detail"].lower(), llm_check
        assert any(
            "llm gap check skipped" in w.lower() for w in result["warnings"]
        ), result["warnings"]

    def test_clean_run_still_reports_ok(self):
        """When the LLM actually runs and returns 0 gaps, ok=True / no warning."""
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-fake"}):
            with patch(
                "src.agents.compliance_validator._invoke_llm_gap_check",
                return_value=[],
            ):
                result = validate_package(
                    quote=_quote(),
                    per_form_reports=_per_form_clean(),
                    buyer_email_text="Please include the DVBE certificate.",
                )
        llm_check = next(c for c in result["checks"] if c["name"] == "llm_gap")
        assert llm_check["ok"] is True, llm_check
        assert "0 gap" in llm_check["detail"], llm_check
        assert not any(
            "llm gap check skipped" in w.lower() for w in result["warnings"]
        ), result["warnings"]

    def test_real_gaps_surface_as_warnings(self):
        """When the LLM returns gaps, they show up as warnings and check is ok."""
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-fake"}):
            with patch(
                "src.agents.compliance_validator._invoke_llm_gap_check",
                return_value=["buyer asked for ISO 13485 cert — not included"],
            ):
                result = validate_package(
                    quote=_quote(),
                    per_form_reports=_per_form_clean(),
                    buyer_email_text="Please include ISO 13485 certificate.",
                )
        llm_check = next(c for c in result["checks"] if c["name"] == "llm_gap")
        assert llm_check["ok"] is True
        assert "1 gap" in llm_check["detail"], llm_check
        assert any("ISO 13485" in w for w in result["warnings"]), result["warnings"]
