"""Tests that compliance_validator warnings bubble up to result.warnings.

The qa_pass stage extracts compliance_gap["blockers"] and surfaces them as
stage-attempt reasons. But compliance_gap["warnings"] (e.g. LLM gap-check
findings: 'buyer requested 90-day terms; quote shows 45-day') were stored
in result.compliance_report["gap"]["warnings"] and NOWHERE ELSE.

Operators reading the standard OrchestratorResult.warnings list never saw
them. The dashboard's warning panel was empty even when the LLM had flagged
real concerns. Fix: extract gap warnings into result.warnings (prefixed
with "compliance:") so they flow through the same channel as every other
operator-facing warning.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


def _priced_quote() -> Quote:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze 4x4", qty=10, unit_cost=Decimal("2.00"))],
        status=QuoteStatus.PRICED,
    )
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    q.buyer.requestor_email = "buyer@cchcs.ca.gov"
    return q


class _FakeProfile:
    def __init__(self, pid):
        self.id = pid


class _FakeQAReport:
    def __init__(self):
        self.passed = True
        self.warnings = []
        self.errors = []


class _FakeDraft:
    def __init__(self, pid):
        self.profile_id = pid
        self.pdf_bytes = b"%PDF-1.4 fake"
        self.qa_report = _FakeQAReport()


class TestComplianceWarningsBubble:
    def test_compliance_warnings_appear_in_result_warnings(self):
        """When compliance_validator returns warnings (no blockers), those
        must be visible on result.warnings — not buried in compliance_report.
        """
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        fake_compliance = {
            "checked": True,
            "blockers": [],
            "warnings": [
                "buyer requested 90-day terms; quote shows 45-day",
                "buyer asked for shipping confirmation upon dispatch",
            ],
            "checks": [],
        }

        with patch(
            "src.core.quote_engine.draft",
            return_value=_FakeDraft("704b_reytech_standard"),
        ), patch(
            "src.agents.compliance_validator.validate_package",
            return_value=fake_compliance,
        ):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "advanced", attempt.reasons
        # The 2 LLM warnings must appear on result.warnings, prefixed.
        compliance_warnings = [w for w in result.warnings if "compliance" in w.lower()]
        assert len(compliance_warnings) >= 2, (
            f"Expected at least 2 compliance warnings on result.warnings, "
            f"got: {result.warnings}"
        )
        assert any("90-day terms" in w for w in compliance_warnings), result.warnings
        assert any("shipping confirmation" in w for w in compliance_warnings), result.warnings

    def test_no_compliance_warnings_means_no_pollution(self):
        """If validate_package returns zero warnings, result.warnings must
        not gain a phantom 'compliance:' entry."""
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        fake_compliance = {
            "checked": True,
            "blockers": [],
            "warnings": [],
            "checks": [],
        }

        with patch(
            "src.core.quote_engine.draft",
            return_value=_FakeDraft("704b_reytech_standard"),
        ), patch(
            "src.agents.compliance_validator.validate_package",
            return_value=fake_compliance,
        ):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "advanced", attempt.reasons
        compliance_warnings = [w for w in result.warnings if "compliance" in w.lower()]
        assert not compliance_warnings, result.warnings
