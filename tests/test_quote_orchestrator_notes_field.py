"""Tests that successful operational metadata lands in result.notes — not
result.warnings.

The sent stage previously did:
    result.warnings.append(f"sent to {to} ({bytes} bytes)")

Putting success info in the warnings list pollutes any dashboard or audit
consumer that counts warnings to flag problematic runs. A successful send
shouldn't bump the "warnings" counter — it's not a warning. result.notes
gives operational success metadata its own home.
"""
from __future__ import annotations

import os
from decimal import Decimal
from unittest.mock import patch

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


class _FakePackage:
    def __init__(self, *, ok=True, artifacts=None, merged_pdf=b"%PDF-1.4 fake",
                 errors=None, warnings=None):
        self.ok = ok
        self.artifacts = artifacts or []
        self.merged_pdf = merged_pdf
        self.errors = errors or []
        self.warnings = warnings or []


def _generated_quote_with_package():
    quote = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze 4x4", qty=10,
                             unit_cost=Decimal("2.00"))],
        status=QuoteStatus.QA_PASS,
    )
    quote.header.agency_key = "cchcs"
    quote.header.solicitation_number = "R26Q0042"
    quote.buyer.requestor_email = "buyer@cchcs.ca.gov"
    quote.transition(QuoteStatus.GENERATED)
    result = OrchestratorResult(quote=quote)
    result.package = _FakePackage(merged_pdf=b"%PDF-1.4 merged-fake")
    return quote, result


class TestNotesField:
    def test_orchestrator_result_has_notes_field(self):
        """Notes is a separate channel from warnings/blockers."""
        result = OrchestratorResult()
        assert hasattr(result, "notes")
        assert isinstance(result.notes, list)
        assert result.notes == []

    def test_successful_send_adds_to_notes_not_warnings(self):
        """The send 'sent to X (N bytes)' message belongs in notes, not warnings."""
        quote, result = _generated_quote_with_package()
        orch = QuoteOrchestrator(persist_audit=False)

        class FakeSender:
            def __init__(self, _config): pass
            def send(self, draft):
                pass  # success, no exception

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@x.com", "GMAIL_PASSWORD": "x"}):
            with patch("src.agents.email_poller.EmailSender", FakeSender):
                attempt = orch._try_advance(
                    quote, "sent",
                    QuoteRequest(target_stage="sent"),
                    [], result,
                )

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        # The send-success message lives in notes.
        assert any("sent to" in n for n in result.notes), (
            f"expected send metadata in notes, got: {result.notes}"
        )
        # And NOT in warnings (warnings is for problems).
        assert not any("sent to" in w for w in result.warnings), (
            f"warnings should not contain success metadata, got: {result.warnings}"
        )
