"""Tests for the no-raise contract on invalid request.doc_type.

run() validates `target_stage` at the top (line 146) but not `doc_type`.
The downstream code calls `DocType(request.doc_type)` and `Quote.from_legacy_dict(..., doc_type=request.doc_type)`,
both of which raise ValueError on an unknown string. That escapes the
orchestrator's "never raises on business-logic failure" contract — a
caller passing doc_type="quote" instead of "rfq" gets a 500, not a
clean OrchestratorResult with a blocker.

These tests assert run() returns ok=False with a clear blocker for
each of the three source paths (None / dict / str) when doc_type is
invalid.
"""
from __future__ import annotations

from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _legacy_dict():
    return {
        "doc_id": "test_doc_type_safe",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


class TestInvalidDocType:
    def test_blank_source_with_invalid_doc_type_does_not_raise(self):
        """No-source path: Quote(doc_type=DocType('quote')) raises ValueError."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(source=None, doc_type="quote", target_stage="draft")
        # MUST NOT raise.
        result = orch.run(req)
        assert not result.ok
        assert any("doc_type" in b.lower() for b in result.blockers), result.blockers

    def test_dict_source_with_invalid_doc_type_does_not_raise(self):
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_legacy_dict(),
            doc_type="rfqx",  # bogus
            agency_key="cchcs",
            target_stage="qa_pass",
        )
        result = orch.run(req)
        assert not result.ok
        assert any("doc_type" in b.lower() for b in result.blockers), result.blockers

    def test_uppercase_doc_type_normalizes_or_blocks_cleanly(self):
        """Operators may pass 'PC' instead of 'pc' — should not 500."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(source=None, doc_type="PC", target_stage="draft")
        # Either the orchestrator normalizes it, or it returns a clean blocker.
        # The contract is: never raise.
        result = orch.run(req)
        # If it normalized, ok=True and quote.doc_type.value == "pc".
        # If it blocked, ok=False with a doc_type-related blocker.
        if result.ok:
            assert result.quote.doc_type.value == "pc"
        else:
            assert any("doc_type" in b.lower() for b in result.blockers), result.blockers
