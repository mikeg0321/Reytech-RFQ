"""Tests for qa_pass stage failure visibility.

Previously the qa_pass handler returned silently when forms failed to fill,
QA didn't pass, or the compliance validator flagged blockers. The audit row
showed only "transition ran but status is priced, expected qa_pass" — useless
for debugging. Operators had to dig into result.compliance_report by hand.

Now the handler raises with concrete reasons (which profile, which error,
which compliance blocker) so _try_advance records outcome="error" with the
actual failure mode in the audit row AND result.compliance_report still
holds the full structured report.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


class _FakeProfile:
    def __init__(self, pid: str):
        self.id = pid


def _priced_quote() -> Quote:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[
            LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00")),
        ],
        status=QuoteStatus.PRICED,
    )
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    return q


class TestQaPassVisibility:
    def test_fill_error_surfaces_in_audit_reasons(self):
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        with patch(
            "src.core.quote_engine.draft",
            side_effect=RuntimeError("blank PDF missing"),
        ):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "error", f"got {attempt.outcome}: {attempt.reasons}"
        joined = " ".join(attempt.reasons)
        assert "qa_pass incomplete" in joined, attempt.reasons
        assert "704b_reytech_standard" in joined, attempt.reasons
        assert "blank PDF missing" in joined, attempt.reasons
        assert quote.status == QuoteStatus.PRICED  # never transitioned
        # Structured report still preserved for the UI.
        assert result.compliance_report.get("per_form")

    def test_qa_fail_on_form_surfaces_specific_error(self):
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        fake_draft = MagicMock()
        fake_draft.qa_report.passed = False
        fake_draft.qa_report.warnings = []
        fake_draft.qa_report.errors = ["bid price mismatch on line 3"]
        fake_draft.pdf_bytes = b"%PDF-1.4 fake"

        with patch("src.core.quote_engine.draft", return_value=fake_draft):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
            )

        assert attempt.outcome == "error", f"got {attempt.outcome}: {attempt.reasons}"
        joined = " ".join(attempt.reasons)
        assert "qa_pass incomplete" in joined, attempt.reasons
        assert "bid price mismatch on line 3" in joined, attempt.reasons
        assert quote.status == QuoteStatus.PRICED

    def test_compliance_blocker_surfaces_in_audit_reasons(self):
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        fake_draft = MagicMock()
        fake_draft.qa_report.passed = True
        fake_draft.qa_report.warnings = []
        fake_draft.qa_report.errors = []
        fake_draft.pdf_bytes = b"%PDF-1.4 fake"

        gap = {
            "checked": True,
            "blockers": ["DVBE certificate not attached"],
            "warnings": [],
        }

        with patch("src.core.quote_engine.draft", return_value=fake_draft):
            with patch(
                "src.agents.compliance_validator.validate_package",
                return_value=gap,
            ):
                attempt = orch._try_advance(
                    quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
                )

        assert attempt.outcome == "error", f"got {attempt.outcome}: {attempt.reasons}"
        joined = " ".join(attempt.reasons)
        assert "DVBE certificate not attached" in joined, attempt.reasons

    def test_clean_qa_pass_advances(self):
        quote = _priced_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        fake_draft = MagicMock()
        fake_draft.qa_report.passed = True
        fake_draft.qa_report.warnings = []
        fake_draft.qa_report.errors = []
        fake_draft.pdf_bytes = b"%PDF-1.4 fake"

        gap = {"checked": True, "blockers": [], "warnings": []}

        with patch("src.core.quote_engine.draft", return_value=fake_draft):
            with patch(
                "src.agents.compliance_validator.validate_package",
                return_value=gap,
            ):
                attempt = orch._try_advance(
                    quote, "qa_pass", QuoteRequest(target_stage="qa_pass"), profiles, result,
                )

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.QA_PASS
