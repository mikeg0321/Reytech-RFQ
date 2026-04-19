"""Tests for status-transition validation on Quote.

Quote.transition currently allows ANY-to-ANY status change with no validation.
That means a downstream bug — say a stale callback firing after a quote already
shipped — can silently regress status from SENT back to DRAFT, wiping the
implicit guarantees the orchestrator depends on (audit ordering, terminal
finality, "no double-send"). The audit trail shows the transition as a normal
event, so the regression is invisible until much later when something else
breaks.

Two classes of transition are clearly bug-shaped and should raise:

1. Re-opening a terminal status (WON / LOST) into the active pipeline.
   A closed quote is closed. If we want to re-bid, the operator clones it.

2. Backward jumps in the main pipeline by more than 1 stage. One step back is
   legitimate (e.g., re-pricing after QA caught something); jumping from
   GENERATED back to DRAFT is not.

Forward jumps (DRAFT → PRICED) and self-transitions remain allowed.
"""
from __future__ import annotations

import pytest

from src.core.quote_model import Quote, QuoteStatus


class TestTransitionValidation:
    def test_self_transition_is_allowed_and_idempotent(self):
        q = Quote(status=QuoteStatus.DRAFT)
        q.transition(QuoteStatus.DRAFT)
        assert q.status == QuoteStatus.DRAFT

    def test_forward_jump_is_allowed(self):
        """Skipping forward stages is fine — used by tests and one-shot flows."""
        q = Quote(status=QuoteStatus.DRAFT)
        q.transition(QuoteStatus.PRICED)
        assert q.status == QuoteStatus.PRICED

    def test_one_step_back_is_allowed(self):
        """Operator re-prices after QA flagged something — valid workflow."""
        q = Quote(status=QuoteStatus.QA_PASS)
        q.transition(QuoteStatus.PRICED)
        assert q.status == QuoteStatus.PRICED

    def test_two_step_back_raises(self):
        """GENERATED → PRICED is one step (allowed). GENERATED → PARSED is two."""
        q = Quote(status=QuoteStatus.GENERATED)
        with pytest.raises(ValueError, match="backward"):
            q.transition(QuoteStatus.PARSED)
        # Status must remain unchanged after a refused transition.
        assert q.status == QuoteStatus.GENERATED

    def test_sent_to_draft_raises(self):
        """The most dangerous silent regression — undo a real send."""
        q = Quote(status=QuoteStatus.SENT)
        with pytest.raises(ValueError, match="backward"):
            q.transition(QuoteStatus.DRAFT)
        assert q.status == QuoteStatus.SENT

    def test_sent_to_won_is_allowed(self):
        """Standard close-the-loop after the buyer awards us."""
        q = Quote(status=QuoteStatus.SENT)
        q.transition(QuoteStatus.WON)
        assert q.status == QuoteStatus.WON

    def test_won_to_lost_correction_is_allowed(self):
        """Operator marked won by mistake; correcting to lost is legal."""
        q = Quote(status=QuoteStatus.WON)
        q.transition(QuoteStatus.LOST)
        assert q.status == QuoteStatus.LOST

    def test_won_to_draft_raises(self):
        """A closed quote does not get re-opened. Clone it instead."""
        q = Quote(status=QuoteStatus.WON)
        with pytest.raises(ValueError, match="terminal"):
            q.transition(QuoteStatus.DRAFT)
        assert q.status == QuoteStatus.WON

    def test_lost_to_priced_raises(self):
        q = Quote(status=QuoteStatus.LOST)
        with pytest.raises(ValueError, match="terminal"):
            q.transition(QuoteStatus.PRICED)
        assert q.status == QuoteStatus.LOST

    def test_legacy_status_can_enter_pipeline(self):
        """Legacy READY/ENRICHED records must be importable into the pipeline."""
        q = Quote(status=QuoteStatus.ENRICHED)
        q.transition(QuoteStatus.PRICED)
        assert q.status == QuoteStatus.PRICED

    def test_audit_trail_records_refused_transitions(self):
        """A refused transition leaves status unchanged but should still log
        the attempt — operators investigating 'why didn't it advance?' need
        to see the rejected call in the trail."""
        q = Quote(status=QuoteStatus.SENT)
        try:
            q.transition(QuoteStatus.DRAFT)
        except ValueError:
            pass
        assert any(
            "refused" in entry.action or "rejected" in entry.action
            for entry in q.provenance.audit_trail
        ), [e.action for e in q.provenance.audit_trail]
