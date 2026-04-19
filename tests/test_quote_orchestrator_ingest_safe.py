"""Tests for the orchestrator's contract: run() never raises on bad input.

The orchestrator's docstring promises:

    Never raises on business-logic failure — returns an OrchestratorResult
    with blockers/warnings populated so callers can surface them. Only
    programming errors (e.g. ImportError) propagate.

But _ingest called Quote.from_legacy_dict and quote_engine.ingest with no
try/except wrapper. A corrupt dict or unparseable PDF would propagate as
a 500 to the route caller — which then has to wrap orchestrator.run()
defensively, defeating the contract.

Now _ingest catches business-logic exceptions, populates blockers, and
returns None. run() honors the docstring.
"""
from __future__ import annotations

from unittest.mock import patch

from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
)


class TestIngestNeverRaises:
    def test_corrupt_dict_does_not_raise(self):
        """Garbage input should produce blockers, not a 500."""
        orch = QuoteOrchestrator(persist_audit=False)
        # A dict with line_items as an int — from_legacy_dict iterates it
        # and crashes with TypeError.
        bad = {"doc_id": "test", "line_items": 42}
        req = QuoteRequest(source=bad, doc_type="pc", target_stage="parsed")

        # Must NOT raise.
        result = orch.run(req)
        assert not result.ok
        assert result.blockers, "expected blockers, got none"
        assert any("ingest" in b.lower() for b in result.blockers), result.blockers

    def test_pdf_parse_failure_does_not_raise(self):
        """If quote_engine.ingest blows up on a malformed PDF, route must
        still get a clean OrchestratorResult, not an exception."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source="/nonexistent/garbage.pdf",
            doc_type="pc",
            target_stage="parsed",
        )

        with patch(
            "src.core.quote_engine.ingest",
            side_effect=RuntimeError("PDF parser exploded"),
        ):
            result = orch.run(req)

        assert not result.ok
        assert result.blockers
        assert any("PDF parser exploded" in b or "ingest" in b.lower() for b in result.blockers), result.blockers
