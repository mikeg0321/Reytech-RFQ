"""Tests that _ingest does not raise when the legacy source is a closed
(WON / LOST) quote.

PR #169 tightened Quote.transition to refuse terminal → pipeline
transitions. _ingest was already calling quote.transition(PARSED) (or
DRAFT) right after Quote.from_legacy_dict — but that call is OUTSIDE the
existing try/except wrapping from_legacy_dict, so a legacy WON dict makes
quote.transition() raise ValueError, which propagates up through _ingest
and out of run() — violating the no-raise contract.

The realistic path for this:
  - A legacy quote dict written when status="won" is fed back into the
    orchestrator (re-import after a backup restore, an operator clicking
    "re-process" on a closed quote, an integration test fixture).

Either way, run() must return a clean OrchestratorResult with a blocker,
not a 500.
"""
from __future__ import annotations

from src.core.quote_model import QuoteStatus
from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _won_legacy_dict_with_items() -> dict:
    return {
        "doc_id": "test_won_quote",
        "status": "won",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0001"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


def _lost_legacy_dict_no_items() -> dict:
    return {
        "doc_id": "test_lost_empty",
        "status": "lost",
        "line_items": [],
        "header": {"agency_key": "cchcs"},
    }


class TestIngestHandlesTerminalLegacy:
    def test_won_dict_with_items_does_not_raise(self):
        """WON → PARSED transition is refused by Quote.transition; the
        orchestrator must catch it, not propagate."""
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(
            source=_won_legacy_dict_with_items(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="parsed",
        ))
        assert not result.ok
        assert any(
            "won" in b.lower() or "terminal" in b.lower() or "transition" in b.lower()
            for b in result.blockers
        ), f"expected a clear blocker about the terminal status, got: {result.blockers}"
        # Status should remain at the legacy value (not silently corrupted).
        assert result.quote.status == QuoteStatus.WON

    def test_lost_dict_no_items_does_not_raise(self):
        """LOST → DRAFT (no-items branch) — same refusal, same contract."""
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(
            source=_lost_legacy_dict_no_items(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="parsed",
        ))
        assert not result.ok
        assert any(
            "lost" in b.lower() or "terminal" in b.lower() or "transition" in b.lower()
            for b in result.blockers
        ), result.blockers
        assert result.quote.status == QuoteStatus.LOST
