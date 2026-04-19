"""Tests that run() short-circuits when _ingest populates blockers.

`_ingest` has two return modes for the dict-source path: (a) `from_legacy_dict`
raises → return None, blocker recorded; (b) the post-ingest transition raises
because the legacy source is at WON/LOST → blocker recorded but the quote is
still returned. In mode (b), run() previously continued into _resolve_agency,
_resolve_profiles, and a stage-advancement loop that all generated additional
stage_history rows on top of the doomed run.

The PDF-source path already short-circuits via `return quote if not result.blockers else None`
on its own (line 300). The cleaner fix is for run() itself to honor any
blockers populated by _ingest, so both paths behave the same and the audit
trail reflects exactly one root cause instead of three downstream symptoms.
"""
from __future__ import annotations

from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _won_legacy_dict() -> dict:
    return {
        "doc_id": "test_short_circuit_won",
        "status": "won",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0001"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


class TestRunShortCircuitsOnIngestBlocker:
    def test_terminal_legacy_won_produces_only_ingest_blocker(self):
        """When _ingest blocks on a closed-quote re-ingest, run() must NOT
        run the agency/profile/stage loop — there's nothing useful those
        steps can produce, and they pollute stage_history with rows that
        weren't actually attempted in any meaningful sense.
        """
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_won_legacy_dict(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="qa_pass",
        )
        result = orch.run(req)

        assert not result.ok
        assert result.quote is not None  # ingest still returns the quote for inspection
        # Exactly one blocker — the ingest one. No "cannot skip stages"
        # noise from the stage loop running anyway.
        ingest_blockers = [b for b in result.blockers if b.startswith("ingest:")]
        assert len(ingest_blockers) == 1, result.blockers
        assert "cannot reuse won" in ingest_blockers[0].lower(), result.blockers
        # No spurious stage_history entries. The advance loop was correctly
        # short-circuited; the only thing we know is "ingest refused".
        assert len(result.stage_history) == 0, [
            (s.stage_from, s.stage_to, s.outcome) for s in result.stage_history
        ]
        # final_stage reflects the quote's actual status, not "draft" garbage
        assert result.final_stage == "won"

    def test_clean_ingest_still_runs_full_loop(self):
        """Sanity check — the short-circuit must NOT fire when _ingest
        produced no blockers. Otherwise we'd block every successful run.
        """
        clean_dict = {
            "doc_id": "test_clean",
            "line_items": [
                {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
            ],
            "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0042"},
            "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
        }
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=clean_dict,
            doc_type="pc",
            agency_key="cchcs",
            target_stage="priced",  # stop at priced so test doesn't depend on profile fill
        )
        result = orch.run(req)

        # Should have stage_history rows (at minimum the priced advance attempt)
        assert len(result.stage_history) >= 1
        assert any(s.stage_to == "priced" for s in result.stage_history), [
            (s.stage_from, s.stage_to, s.outcome) for s in result.stage_history
        ]
