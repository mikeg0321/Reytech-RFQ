"""Tests for the top-level guard in QuoteOrchestrator.run().

The docstring promises run() never raises on business-logic failure. But until
now, any unexpected programming error from an internal helper (KeyError on a
malformed dict, AttributeError from a refactor that renamed a field, etc.)
escaped straight to the route caller as a 500. The catch-all guard wraps the
run() body and converts every escaped exception into a clean blocker.
"""
from __future__ import annotations

from unittest.mock import patch

from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _priced_dict_source() -> dict:
    return {
        "doc_id": "test_top_level_guard",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


class TestRunTopLevelGuard:
    def test_unexpected_attribute_error_becomes_blocker(self):
        """A renamed-field AttributeError inside _resolve_agency must not 500."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="qa_pass",
        )

        with patch.object(
            QuoteOrchestrator,
            "_resolve_agency",
            side_effect=AttributeError("'Quote' object has no attribute 'foo'"),
        ):
            result = orch.run(req)

        assert not result.ok
        assert any(
            "orchestrator run aborted" in b.lower() and "attributeerror" in b.lower()
            for b in result.blockers
        ), result.blockers

    def test_unexpected_key_error_becomes_blocker(self):
        """A KeyError from a malformed internal lookup must not 500."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="qa_pass",
        )

        with patch.object(
            QuoteOrchestrator,
            "_resolve_profiles",
            side_effect=KeyError("missing_profile_key"),
        ):
            result = orch.run(req)

        assert not result.ok
        assert any(
            "orchestrator run aborted" in b.lower() for b in result.blockers
        ), result.blockers

    def test_guard_does_not_swallow_normal_blockers(self):
        """A clean, expected blocker path must not be wrapped as 'aborted'."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="not_a_real_stage",
        )

        result = orch.run(req)

        assert not result.ok
        assert any("Unknown target_stage" in b for b in result.blockers), result.blockers
        assert not any(
            "orchestrator run aborted" in b.lower() for b in result.blockers
        ), result.blockers

    def test_guard_returns_partial_result_quote_if_set(self):
        """If the quote was already attached to result before the crash, keep it."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="qa_pass",
        )

        with patch.object(
            QuoteOrchestrator,
            "_resolve_profiles",
            side_effect=RuntimeError("explosion in profile resolver"),
        ):
            result = orch.run(req)

        assert not result.ok
        # _ingest ran before the crash, so result.quote should be populated
        assert result.quote is not None
        assert any(
            "explosion in profile resolver" in b for b in result.blockers
        ), result.blockers
