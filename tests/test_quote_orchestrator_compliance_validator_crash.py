"""Tests that qa_pass refuses to advance when the compliance validator crashes.

The qa_pass stage runs validate_package() inside a try/except that converts
any non-ImportError exception into `compliance_gap = {"checked": False,
"error": str(e)}`. Then it extracts `compliance_blocked = compliance_gap.get(
"blockers", [])` — which is `[]` when the dict has no "blockers" key.

So if validate_package crashes (DB connection error, LLM timeout, malformed
buyer_email_text), `compliance_blocked` was empty, fills/qa were OK, and
qa_pass advanced WITHOUT compliance ever running. The "checked: False" was
buried in result.compliance_report and the operator had no signal.

Fix: treat compliance_gap.get("error") as a hard qa_pass blocker. If the
validator failed to run, we cannot truthfully claim qa_pass.
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


class TestComplianceValidatorCrashBlocksQaPass:
    def test_validator_crash_blocks_qa_pass(self):
        """If validate_package raises (DB down, LLM timeout, etc.), qa_pass
        must NOT advance. We cannot claim compliance was checked when it
        wasn't, even if every fill+qa succeeded."""
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        with patch(
            "src.core.quote_engine.draft",
            return_value=_FakeDraft("704b_reytech_standard"),
        ), patch(
            "src.agents.compliance_validator.validate_package",
            side_effect=RuntimeError("compliance DB connection timeout"),
        ):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "error", (
            f"Expected qa_pass to refuse advance when validator crashed. "
            f"Got outcome={attempt.outcome}, reasons={attempt.reasons}"
        )
        assert any(
            "compliance validator failed" in r.lower()
            or "compliance check did not run" in r.lower()
            for r in attempt.reasons
        ), attempt.reasons
        assert any(
            "DB connection timeout" in r for r in attempt.reasons
        ), attempt.reasons
        # Status must NOT have transitioned
        assert quote.status == QuoteStatus.PRICED

    def test_validator_returning_unchecked_blocks_qa_pass(self):
        """Even without an exception — if validate_package returns a dict
        with checked=False AND no blockers (e.g., LLM gave up but didn't
        raise), we still don't have a real check, so qa_pass refuses."""
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        unchecked = {"checked": False, "blockers": [], "warnings": [], "checks": []}

        with patch(
            "src.core.quote_engine.draft",
            return_value=_FakeDraft("704b_reytech_standard"),
        ), patch(
            "src.agents.compliance_validator.validate_package",
            return_value=unchecked,
        ):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "error", attempt.reasons
        assert any(
            "compliance check did not run" in r.lower()
            for r in attempt.reasons
        ), attempt.reasons
        assert quote.status == QuoteStatus.PRICED

    def test_clean_validator_pass_still_advances(self):
        """Sanity — when validator returns checked=True with no blockers,
        qa_pass must advance. Otherwise we'd block every clean run."""
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        clean = {"checked": True, "blockers": [], "warnings": [], "checks": []}

        with patch(
            "src.core.quote_engine.draft",
            return_value=_FakeDraft("704b_reytech_standard"),
        ), patch(
            "src.agents.compliance_validator.validate_package",
            return_value=clean,
        ):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "advanced", attempt.reasons
        assert quote.status == QuoteStatus.QA_PASS
