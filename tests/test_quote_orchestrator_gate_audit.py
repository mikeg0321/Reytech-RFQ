"""Tests for audit-log visibility of pre-stage gate firings.

When the agency-resolution gate fires (operator targets qa_pass+ but
quote.header.agency_key is empty), run() returns early with blockers
populated — but previously NO StageAttempt was appended to
stage_history, so the persistent audit log had no record of WHY the
quote stopped progressing. The dashboard would show a quote stuck at
PRICED with no audit row explaining the block.

Now the gate synthesizes a StageAttempt for the next un-attempted stage
with outcome="blocked" and the gate reason, so it's visible in
result.stage_history (and persisted via _persist_audit).
"""
from __future__ import annotations

from decimal import Decimal

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
)


def _priced_dict_source() -> dict:
    return {
        "doc_id": "test_quote_gate_audit",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "", "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": ""},
    }


class TestAgencyGateAudit:
    def test_agency_gate_appears_in_stage_history(self):
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type=DocType.PC,
            target_stage="qa_pass",
        )
        result = orch.run(req)

        assert not result.ok
        assert any("agency unresolved" in b for b in result.blockers), result.blockers
        # Stage history must contain an entry for the blocked attempt so
        # the dashboard's audit timeline shows where + why progress stopped.
        assert result.stage_history, "stage_history is empty — gate fired silently"
        gate_attempts = [
            a for a in result.stage_history
            if "agency unresolved" in " ".join(a.reasons)
        ]
        assert gate_attempts, (
            "no stage_history entry explains the agency gate; "
            f"got: {[(a.stage_to, a.outcome, a.reasons) for a in result.stage_history]}"
        )
        gate = gate_attempts[0]
        assert gate.outcome == "blocked"
        assert gate.stage_to == "qa_pass"

    def test_agency_gate_records_target_stage_when_jumping_far(self):
        """If target=generated but gate fires at qa_pass, audit row should
        name the stage where the gate is actually defined (qa_pass)."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_priced_dict_source(),
            doc_type=DocType.PC,
            target_stage="generated",
        )
        result = orch.run(req)
        assert not result.ok
        gate_attempts = [
            a for a in result.stage_history
            if "agency unresolved" in " ".join(a.reasons)
        ]
        assert gate_attempts
        # Gate is defined at qa_pass — record there, not "generated".
        assert gate_attempts[0].stage_to == "qa_pass"
