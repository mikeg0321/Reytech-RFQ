"""Tests for the no-raise contract in profile resolution.

run() calls _resolve_profiles which calls quote_engine.get_profiles().
If get_profiles() raises (corrupt registry, IO failure on profile YAMLs,
etc.), the orchestrator currently propagates the exception — violating
the docstring promise that run() never raises on business-logic failure.

Now _resolve_profiles catches registry-load failures, populates blockers,
and returns []. run() proceeds to _try_advance which fails-cleanly with
"no form profiles resolved" preconditions.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _priced_dict_source() -> dict:
    return {
        "doc_id": "test_quote_resolve_safe",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


class TestResolveProfilesSafe:
    def test_get_profiles_failure_does_not_raise(self):
        """A corrupt registry must not 500 the route caller."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type="pc",
            agency_key="cchcs",  # bypass agency gate so we reach _resolve_profiles
            target_stage="qa_pass",
        )

        with patch(
            "src.core.quote_engine.get_profiles",
            side_effect=RuntimeError("profile registry corrupt"),
        ):
            # Must NOT raise.
            result = orch.run(req)

        assert not result.ok
        assert any(
            "profile registry corrupt" in b or "profile registry" in b.lower()
            for b in result.blockers
        ), result.blockers
