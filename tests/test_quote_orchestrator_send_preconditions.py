"""Tests for hard preconditions on the `sent` stage.

Previously _send_package coerced empty header fields to fallbacks:
  - solicitation_number → "quote"  → subject said "Solicitation #quote"
  - agency_key          → "buyer"  → body said "for BUYER Solicitation #quote"

The agency-key path was already protected by the agency-resolution gate in
run(), but _send_package can also be invoked directly (tests, future
callers) and the solicitation_number path was completely unguarded — a
quote could ship to a real buyer with a fake-looking subject line.

Now _send_package raises with a clear reason when either header field is
missing, so _try_advance records outcome="error" instead of sending a
malformed email.
"""
from __future__ import annotations

import os
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


def _generated_quote(*, sol: str = "R26Q0042", agency: str = "cchcs") -> tuple[Quote, OrchestratorResult]:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00"))],
        status=QuoteStatus.GENERATED,
    )
    q.header.solicitation_number = sol
    q.header.agency_key = agency
    q.buyer.requestor_email = "buyer@cchcs.ca.gov"
    result = OrchestratorResult(quote=q)
    pkg = MagicMock()
    pkg.merged_pdf = b"%PDF-1.4 fake bytes\n"
    result.package = pkg
    return q, result


class TestSendPreconditions:
    def test_send_blocks_when_solicitation_number_empty(self):
        """A real email with subject 'Solicitation #quote' is unprofessional;
        refuse to send."""
        quote, result = _generated_quote(sol="")
        orch = QuoteOrchestrator(persist_audit=False)

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@y.z", "GMAIL_PASSWORD": "p"}):
            with patch("src.agents.email_poller.EmailSender") as MockSender:
                attempt = orch._try_advance(
                    quote, "sent", QuoteRequest(target_stage="sent"), [], result,
                )
                MockSender.return_value.send.assert_not_called()

        assert attempt.outcome == "error", f"got {attempt.outcome}: {attempt.reasons}"
        assert any("solicitation_number" in r for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.GENERATED  # never advanced

    def test_send_blocks_when_agency_key_empty(self):
        """Even though the run() gate normally catches this, _send_package
        is the last line of defense for direct callers."""
        quote, result = _generated_quote(agency="")
        orch = QuoteOrchestrator(persist_audit=False)

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@y.z", "GMAIL_PASSWORD": "p"}):
            with patch("src.agents.email_poller.EmailSender") as MockSender:
                attempt = orch._try_advance(
                    quote, "sent", QuoteRequest(target_stage="sent"), [], result,
                )
                MockSender.return_value.send.assert_not_called()

        assert attempt.outcome == "error", f"got {attempt.outcome}: {attempt.reasons}"
        assert any("agency_key" in r for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.GENERATED

    def test_send_proceeds_when_headers_complete(self):
        """Sanity: full headers still send."""
        quote, result = _generated_quote(sol="R26Q0042", agency="cchcs")
        orch = QuoteOrchestrator(persist_audit=False)

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@y.z", "GMAIL_PASSWORD": "p"}):
            with patch("src.agents.email_poller.EmailSender") as MockSender:
                MockSender.return_value.send = MagicMock(return_value=None)
                attempt = orch._try_advance(
                    quote, "sent", QuoteRequest(target_stage="sent"), [], result,
                )

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.SENT
