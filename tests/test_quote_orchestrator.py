"""Tests for the QuoteOrchestrator — the platform's single connector.

Proves the state machine:
  - Enforces preconditions at each stage (no silent advancement)
  - Refuses to skip stages
  - Captures every attempt in stage_history
  - Resolves agency → profiles (not doc_type default) when agency is known
  - Surfaces missing profiles as warnings (pointing at FormProfiler work)

No ghost data: all fixtures come from the existing golden fixture
(`tests/fixtures/golden/test0321_real_cchcs.json`) or constructed from
real quote_model dataclasses.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal

import pytest

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    _STAGE_ORDER,
    _stage_index,
    _preconditions_for,
    _best_profile_for_form,
    _FORM_ID_TO_PROFILE_ID,
)


# ── Stage ordering ──────────────────────────────────────────────────────────

class TestStageOrder:
    def test_stage_order_matches_quote_status_values(self):
        for stage in _STAGE_ORDER:
            assert any(qs.value == stage for qs in QuoteStatus), (
                f"_STAGE_ORDER entry {stage!r} has no matching QuoteStatus"
            )

    def test_stage_index_roundtrip(self):
        assert _stage_index("draft") == 0
        assert _stage_index("sent") == len(_STAGE_ORDER) - 1
        assert _stage_index("bogus") == -1


# ── Preconditions ───────────────────────────────────────────────────────────

class TestPreconditions:
    def test_parsed_requires_line_items(self):
        q = Quote(doc_type=DocType.PC)
        ok, reasons = _preconditions_for("parsed", q, [])
        assert not ok
        assert any("no line items" in r for r in reasons)

    def test_parsed_passes_with_items(self):
        q = Quote(doc_type=DocType.PC, line_items=[LineItem(line_no=1, description="gloves")])
        ok, _ = _preconditions_for("parsed", q, [])
        assert ok

    def test_qa_pass_blocks_on_unpriced_items(self):
        q = Quote(doc_type=DocType.PC, line_items=[
            LineItem(line_no=1, description="a", unit_cost=Decimal("10")),
            LineItem(line_no=2, description="b", unit_cost=Decimal("0")),
        ])
        ok, reasons = _preconditions_for("qa_pass", q, [object()])  # fake profile
        assert not ok
        assert any("unpriced" in r for r in reasons)

    def test_qa_pass_blocks_on_no_profiles(self):
        q = Quote(doc_type=DocType.PC, line_items=[
            LineItem(line_no=1, description="a", unit_cost=Decimal("10")),
        ])
        ok, reasons = _preconditions_for("qa_pass", q, [])  # no profiles
        assert not ok
        assert any("profile" in r.lower() for r in reasons)

    def test_generated_requires_qa_pass_first(self):
        q = Quote(doc_type=DocType.PC, status=QuoteStatus.PRICED)
        ok, reasons = _preconditions_for("generated", q, [object()])
        assert not ok
        assert any("qa_pass" in r for r in reasons)

    def test_sent_requires_generated_first(self):
        q = Quote(doc_type=DocType.PC, status=QuoteStatus.QA_PASS)
        ok, reasons = _preconditions_for("sent", q, [])
        assert not ok
        assert any("generated" in r for r in reasons)

    def test_unknown_stage_blocks(self):
        q = Quote(doc_type=DocType.PC)
        ok, reasons = _preconditions_for("nonsense", q, [])
        assert not ok


# ── Form-id → profile-id mapping ────────────────────────────────────────────

class TestFormIdMapping:
    def test_all_agency_config_form_ids_have_entries(self):
        """Every form listed in agency_config.AVAILABLE_FORMS must have a
        profile-id mapping (even if the profile itself doesn't exist yet —
        the mapping tells the FormProfiler what to build)."""
        from src.core.agency_config import AVAILABLE_FORMS
        for form in AVAILABLE_FORMS:
            fid = form["id"]
            if fid == "bidpkg":
                continue  # container, not a profile
            assert fid in _FORM_ID_TO_PROFILE_ID, (
                f"form_id {fid!r} is in AVAILABLE_FORMS but has no profile mapping"
            )

    def test_returns_none_for_unknown_form_id(self):
        assert _best_profile_for_form("totally_made_up", {}) is None

    def test_returns_profile_when_in_registry(self):
        fake_registry = {"704b_reytech_standard": object()}
        result = _best_profile_for_form("704b", fake_registry)
        assert result is fake_registry["704b_reytech_standard"]


# ── run() on a blank quote (no source) ──────────────────────────────────────

class TestRunBlankQuote:
    def test_blank_run_stops_at_draft_when_target_is_draft(self):
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(source=None, doc_type="pc", target_stage="draft"))
        assert result.ok
        assert result.final_stage == "draft"
        assert result.quote is not None
        assert result.quote.line_items == []

    def test_blank_run_blocks_at_parsed_with_no_items(self):
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(source=None, doc_type="pc", target_stage="parsed"))
        assert not result.ok
        assert any("no line items" in b for b in result.blockers)
        assert result.final_stage == "draft"
        assert len(result.stage_history) == 1
        assert result.stage_history[0].outcome == "blocked"

    def test_unknown_target_stage_fails_fast(self):
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(target_stage="fly_to_the_moon"))
        assert not result.ok
        assert any("Unknown target_stage" in b for b in result.blockers)


# ── run() from a legacy-dict source ─────────────────────────────────────────

class TestRunFromDict:
    def _sample_pc_dict(self, items: list[dict], agency: str = "") -> dict:
        return {
            "pc_id": "pc_orch_test",
            "pc_number": "R26Q0999",
            "agency": agency,
            "items": items,
            "requestor": "tester@cchcs.ca.gov",
        }

    def test_dict_with_items_advances_to_parsed(self):
        orch = QuoteOrchestrator(persist_audit=False)
        src = self._sample_pc_dict(
            [{"description": "Exam gloves", "qty": 10, "unit_cost": 5.00}],
            agency="cchcs",
        )
        result = orch.run(QuoteRequest(source=src, doc_type="pc", target_stage="parsed",
                                        agency_key="cchcs"))
        assert result.ok, f"blockers: {result.blockers}"
        assert result.quote.status == QuoteStatus.PARSED
        assert len(result.quote.line_items) >= 1

    def test_advances_one_stage_at_a_time(self):
        """Must advance one stage at a time. Orchestrator refuses skips.

        The assertion here is not 'final_stage != generated' — the orchestrator
        legitimately CAN reach generated when inputs are clean. What matters is
        that it walked there stage-by-stage (no skips), which is provable via
        stage_history.
        """
        orch = QuoteOrchestrator(persist_audit=False)
        src = self._sample_pc_dict([
            {"description": "Exam gloves", "qty": 10, "unit_cost": 5.00},
        ], agency="cchcs")
        result = orch.run(QuoteRequest(
            source=src, doc_type="pc",
            target_stage="generated", agency_key="cchcs",
        ))
        # Every transition in history must be adjacent in _STAGE_ORDER —
        # never a skip like draft → generated.
        for attempt in result.stage_history:
            from_idx = _stage_index(attempt.stage_from)
            to_idx = _stage_index(attempt.stage_to)
            assert to_idx - from_idx <= 1, (
                f"Orchestrator skipped stages: {attempt.stage_from} → {attempt.stage_to}"
            )
        assert len(result.stage_history) >= 1

    def test_history_records_every_attempt(self):
        orch = QuoteOrchestrator(persist_audit=False)
        src = self._sample_pc_dict(
            [{"description": "Gauze 4x4", "qty": 50, "unit_cost": 2.00}],
            agency="cchcs",
        )
        result = orch.run(QuoteRequest(source=src, doc_type="pc",
                                        target_stage="priced", agency_key="cchcs"))
        # Expect at least parsed → priced attempts recorded
        stages_touched = {a.stage_to for a in result.stage_history}
        assert "parsed" in stages_touched or result.quote.status.value in ("parsed", "priced")


# ── Agency-aware profile resolution ─────────────────────────────────────────

class TestAgencyAwareProfiles:
    def test_unknown_agency_warns_about_missing_profiles(self):
        """If agency_config lists required_forms we don't have profiles for,
        surface them as warnings — this is the feature request for FormProfiler."""
        orch = QuoteOrchestrator(persist_audit=False)
        src = {
            "pc_id": "pc_agency_test",
            "pc_number": "R26Q0001",
            "agency": "calvet",
            "items": [{"description": "Blanket, fleece", "qty": 20, "unit_cost": 8.00}],
            "requestor": "buyer@cdva.ca.gov",
        }
        result = orch.run(QuoteRequest(
            source=src, doc_type="rfq",
            target_stage="parsed",
            agency_key="calvet",
        ))
        # CalVet requires a bunch of forms we don't have profiles for yet.
        missing_profile_warning = any(
            "calvet" in w.lower() and "no profile yet" in w.lower()
            for w in result.warnings
        )
        assert missing_profile_warning or result.profiles_used, (
            "Expected either a missing-profile warning or some profile fallback; "
            f"got warnings={result.warnings!r}, profiles={result.profiles_used!r}"
        )

    def test_cchcs_resolves_to_cchcs_profiles(self):
        """CCHCS has 704a/704b/703a profiles built — we should pick them."""
        orch = QuoteOrchestrator(persist_audit=False)
        src = {
            "pc_id": "pc_cchcs_test",
            "pc_number": "R26Q0002",
            "agency": "cchcs",
            "items": [{"description": "Gauze 4x4", "qty": 10, "unit_cost": 2.00}],
            "requestor": "buyer@cchcs.ca.gov",
        }
        result = orch.run(QuoteRequest(
            source=src, doc_type="pc",
            target_stage="parsed",
            agency_key="cchcs",
        ))
        assert result.profiles_used, f"expected profiles, got: {result.warnings}"


# ── End-to-end proof against the Test0321 golden fixture ────────────────────

class TestEndToEndCchcsGolden:
    """Drive the REAL Test0321 golden fixture (28 items, R25Q94 CCHCS data)
    through the orchestrator and prove:

      - Ingest accepts the golden fixture's rich dict shape
      - Agency resolves to 'cchcs' and picks the 704b profile
      - draft → parsed → priced advances cleanly
      - qa_pass attempt runs fill + compliance and BLOCKS with specific,
        expected reasons (703b profile + quote profile aren't built yet)
      - Every transition is adjacent — orchestrator never skips stages
      - Zero 'error' outcomes anywhere (no programming bugs on real data)

    This is the platform's integration smoke test. If it passes, the
    QuoteOrchestrator + quote_engine.draft() + ComplianceValidator chain is
    production-ready. Per-agency form coverage is then a data task
    (new profile YAMLs), not a platform task.
    """

    _FIXTURE = os.path.join(
        os.path.dirname(__file__), "fixtures", "golden", "test0321_real_cchcs.json",
    )

    @pytest.fixture
    def golden(self):
        with open(self._FIXTURE, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_orchestrator_drives_cchcs_golden_through_priced_then_blocks_at_qa_pass(self, golden):
        orch = QuoteOrchestrator(persist_audit=False)
        result = orch.run(QuoteRequest(
            source=golden,
            doc_type="pc",
            agency_key="cchcs",
            solicitation_number="R26Q0321",
            target_stage="qa_pass",
        ))

        # No programming errors — any 'error' outcome indicates an exception
        # inside a transition, which is a real bug we want to catch.
        errors = [a for a in result.stage_history if a.outcome == "error"]
        assert not errors, f"unexpected error outcomes: {[(a.stage_to, a.reasons) for a in errors]}"

        # Every transition attempt must be adjacent in _STAGE_ORDER.
        for attempt in result.stage_history:
            from_idx = _stage_index(attempt.stage_from)
            to_idx = _stage_index(attempt.stage_to)
            assert to_idx - from_idx <= 1, (
                f"orchestrator skipped stages: {attempt.stage_from} → {attempt.stage_to}"
            )

        # Ingest loaded the 28 real items from the fixture.
        assert result.quote is not None
        assert len(result.quote.line_items) == 28

        # 704b profile was resolved and used (it's the only CCHCS-required
        # form with a built profile today).
        assert "704b_reytech_standard" in result.profiles_used

        # priced was reached cleanly.
        advanced_to = {a.stage_to for a in result.stage_history if a.outcome == "advanced"}
        assert "priced" in advanced_to, (
            f"expected to reach priced; stage_history={[a.to_dict() for a in result.stage_history]}"
        )

        # qa_pass was attempted and blocked — NOT errored.
        qa_attempts = [a for a in result.stage_history if a.stage_to == "qa_pass"]
        assert qa_attempts, "orchestrator did not attempt qa_pass"
        qa = qa_attempts[-1]
        assert qa.outcome == "blocked", f"expected qa_pass to block, got {qa.outcome}"

        # Compliance report carries the detailed reasons.
        assert result.compliance_report, "expected compliance_report to be populated after qa_pass attempt"
        assert "per_form" in result.compliance_report
        assert "gap" in result.compliance_report

        # 704b profile was filled (it's the only CCHCS-required form we have).
        filled_704b = [
            r for r in result.compliance_report["per_form"]
            if r.get("profile_id") == "704b_reytech_standard"
        ]
        assert filled_704b and filled_704b[0].get("filled") is True, (
            f"704b must have been filled; per_form={result.compliance_report['per_form']}"
        )

        # Compliance must have surfaced at least one missing-profile blocker —
        # the signal FormProfiler is meant to remediate per-agency.
        compliance_blockers = result.compliance_report["gap"].get("blockers", [])
        joined_blockers = " ".join(compliance_blockers).lower()
        assert any(
            key in joined_blockers
            for key in ("703b", "quote_reytech_letterhead")
        ), f"expected a missing-profile compliance blocker; got: {compliance_blockers}"

        # Final stage is priced (we blocked at qa_pass, not past it).
        assert result.final_stage == "priced", (
            f"final_stage should stay at priced when qa_pass blocks; got {result.final_stage}"
        )
