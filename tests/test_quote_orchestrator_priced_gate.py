"""Tests for the strict completeness gate at the `priced` stage.

Mirrors the generated-stage gate from test_quote_orchestrator_close_loop.py.
Previously, `priced` always returned outcome="advanced" even when items had
no price — the failure only surfaced when the operator clicked through to
`qa_pass`. Audit row was misleading and the operator wasted a click.

These tests pin the behavior:
  * Unpriced item → outcome="error" at the priced stage (not advanced)
  * No-bid items don't count as unpriced (intentional skip)
  * Mixed priced + no-bid items advance cleanly
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


def _parsed_quote(items: list[LineItem]) -> Quote:
    q = Quote(doc_type=DocType.PC, line_items=items, status=QuoteStatus.PARSED)
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    return q


class TestPricedStrictGate:
    def test_priced_blocks_when_item_has_zero_unit_cost(self):
        quote = _parsed_quote([
            LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00")),
            LineItem(line_no=2, description="Tape",  qty=5,  unit_cost=Decimal("0")),
        ])
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        # enrich_pricing is a no-op stub — we're testing the gate, not the pricer.
        with patch("src.core.quote_engine.enrich_pricing", return_value=None):
            attempt = orch._try_advance(quote, "priced", QuoteRequest(target_stage="priced"), [], result)

        assert attempt.outcome == "error", f"expected error, got {attempt.outcome}: {attempt.reasons}"
        assert any("priced incomplete" in r for r in attempt.reasons), attempt.reasons
        assert any("1/2" in r for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.PARSED  # never transitioned

    def test_priced_blocks_when_item_has_none_unit_cost(self):
        quote = _parsed_quote([
            LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00")),
            LineItem(line_no=7, description="Mystery", qty=1),  # no unit_cost set
        ])
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        with patch("src.core.quote_engine.enrich_pricing", return_value=None):
            attempt = orch._try_advance(quote, "priced", QuoteRequest(target_stage="priced"), [], result)

        assert attempt.outcome == "error"
        # Reason should name the offending line number so the operator can find it.
        assert any("7" in r for r in attempt.reasons), attempt.reasons

    def test_priced_advances_when_all_items_priced(self):
        quote = _parsed_quote([
            LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00")),
            LineItem(line_no=2, description="Tape",  qty=5,  unit_cost=Decimal("3.50")),
        ])
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        with patch("src.core.quote_engine.enrich_pricing", return_value=None):
            attempt = orch._try_advance(quote, "priced", QuoteRequest(target_stage="priced"), [], result)

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.PRICED

    def test_priced_skips_no_bid_items(self):
        """A no_bid item without a price is intentional — must not block."""
        quote = _parsed_quote([
            LineItem(line_no=1, description="Gauze", qty=10, unit_cost=Decimal("2.00")),
            LineItem(line_no=2, description="OOSI",  qty=1,  unit_cost=Decimal("0"), no_bid=True),
        ])
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        with patch("src.core.quote_engine.enrich_pricing", return_value=None):
            attempt = orch._try_advance(quote, "priced", QuoteRequest(target_stage="priced"), [], result)

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.PRICED
