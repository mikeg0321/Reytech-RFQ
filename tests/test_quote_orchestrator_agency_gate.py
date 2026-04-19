"""Tests for the agency-resolution gate at orchestrator entry.

Closes a silent-failure path: previously, when no agency could be matched
from the source signals, agency_config.match_agency fell back to "other"
(a real config with minimal forms). The orchestrator treated that as a
successful resolution and let the quote advance — so a quote bound for an
unrecognized buyer would silently ship a generic 704 instead of the
agency-specific package.

Now the orchestrator distinguishes "operator set agency_key='other'
explicitly" from "system fell back to 'other'". The latter blocks at
target≥qa_pass with a clear "agency unresolved" message. Override by
passing request.agency_key explicitly.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _priced_dict_source(*, agency_hint: str = "") -> dict:
    """Round-trippable legacy dict that becomes a PARSED-ready Quote."""
    return {
        "doc_id": "test_quote_agency_gate",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": agency_hint, "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": ""},
    }


class TestAgencyResolutionGate:
    def test_unresolved_agency_blocks_target_qa_pass(self):
        """No agency_key, no buyer signals → must block before qa_pass."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(agency_hint=""),
            doc_type=DocType.PC,
            target_stage="qa_pass",
        )
        result = orch.run(req)
        assert not result.ok
        assert any("agency unresolved" in b for b in result.blockers), result.blockers

    def test_unresolved_agency_blocks_target_generated(self):
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(agency_hint=""),
            doc_type=DocType.PC,
            target_stage="generated",
        )
        result = orch.run(req)
        assert not result.ok
        assert any("agency unresolved" in b for b in result.blockers), result.blockers

    def test_unresolved_agency_allowed_for_target_priced(self):
        """parsed/priced are inspection stages — no agency required."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(agency_hint=""),
            doc_type=DocType.PC,
            target_stage="priced",
        )
        result = orch.run(req)
        # The pricing engine may still fail (no real prices), but agency
        # itself must NOT be the blocker for inspection-target runs.
        for b in result.blockers:
            assert "agency unresolved" not in b, f"unexpected blocker: {b}"

    def test_explicit_agency_key_overrides_unresolvable_signals(self):
        """Operator override path: set request.agency_key directly."""
        orch = QuoteOrchestrator(persist_audit=False)
        # No buyer signals, but operator says it's CCHCS.
        req = QuoteRequest(
            source=_priced_dict_source(agency_hint=""),
            doc_type=DocType.PC,
            agency_key="cchcs",
            target_stage="qa_pass",
        )
        result = orch.run(req)
        # Should NOT be blocked by the agency gate. (May still fail later
        # at qa_pass for other reasons — pricing, profiles, etc.)
        for b in result.blockers:
            assert "agency unresolved" not in b, f"unexpected blocker: {b}"


    def test_other_fallback_recorded_as_warning_not_resolution(self):
        """The 'other' fallback should NOT populate quote.header.agency_key.

        Previously match_agency returning 'other' looked like a real match, so
        downstream code would happily fill a generic 704. Now the orchestrator
        leaves agency_key blank when 'other' is the only match, and surfaces
        a 'no confident match' warning so operators can see what happened.
        """
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(agency_hint=""),
            doc_type=DocType.PC,
            target_stage="parsed",  # don't trip the qa_pass blocker
        )
        result = orch.run(req)
        assert result.quote is not None
        assert result.quote.header.agency_key == "", (
            "expected empty agency_key; got: " + repr(result.quote.header.agency_key)
        )
        assert any("no confident match" in w for w in result.warnings), result.warnings
