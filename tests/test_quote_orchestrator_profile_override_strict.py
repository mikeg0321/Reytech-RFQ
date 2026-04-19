"""Tests that explicit profile_ids overrides do not silently fall through.

The contract for `request.profile_ids` is "use exactly these profiles, NOT
the agency-driven set." Previously, if the operator passed a list where
NONE of the IDs existed in the registry (typo, stale UI selection, missing
profile YAML), `_resolve_profiles` quietly fell through to step 2
(agency-driven) and built whatever the agency required.

That's the worst kind of silent failure — the operator thinks they got
their override, but actually got the default set, and there's no blocker
or stage_history row to explain why. The fix: if profile_ids was provided
and produced zero matches, BLOCK with a clear reason.
"""
from __future__ import annotations

from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _cchcs_priced_dict() -> dict:
    return {
        "doc_id": "test_profile_override_strict",
        "line_items": [
            {"line_no": 1, "description": "Gauze 4x4", "qty": 10, "unit_cost": "2.00"},
        ],
        "header": {"agency_key": "cchcs", "solicitation_number": "R26Q0042"},
        "buyer": {"requestor_email": "buyer@cchcs.ca.gov"},
    }


class TestProfileOverrideStrict:
    def test_all_invalid_overrides_block_no_silent_fallthrough(self):
        """If every profile_id in the override is missing from the registry,
        block — do NOT silently switch to agency-driven resolution."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_cchcs_priced_dict(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="priced",
            profile_ids=["bogus_profile_id_does_not_exist"],
        )
        result = orch.run(req)

        # The override request resolved to zero profiles → blocker.
        assert any(
            "no requested profile_ids matched" in b
            or "all profile_ids unknown" in b.lower()
            for b in result.blockers
        ), f"Expected a blocker for the all-invalid override. Got: {result.blockers}"
        # And the result must NOT silently include agency-driven profiles.
        assert not result.profiles_used, (
            f"profiles_used should be empty when override blocks, got: {result.profiles_used}"
        )

    def test_partial_invalid_override_uses_valid_subset_and_warns(self):
        """If SOME of the overrides match (and others don't), the valid
        subset is used and a warning calls out the unknowns. This was
        already the existing behavior — preserve it."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_cchcs_priced_dict(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="priced",
            profile_ids=["704b_reytech_standard", "bogus_id"],
        )
        result = orch.run(req)

        # Should NOT block — we got at least one match.
        no_match_blockers = [
            b for b in result.blockers
            if "no requested profile_ids matched" in b
            or "all profile_ids unknown" in b.lower()
        ]
        assert not no_match_blockers, (
            f"Should not block when at least one override matched. "
            f"Blockers: {result.blockers}"
        )
        # The valid one should be used.
        assert "704b_reytech_standard" in result.profiles_used
        # The bogus one should be in warnings.
        assert any(
            "bogus_id" in w and "missing from registry" in w
            for w in result.warnings
        ), result.warnings

    def test_no_override_still_uses_agency_default(self):
        """Sanity — the strict-override block must NOT fire when profile_ids
        is None (default). Otherwise normal runs would all block."""
        orch = QuoteOrchestrator(persist_audit=False)
        req = QuoteRequest(
            source=_cchcs_priced_dict(),
            doc_type="pc",
            agency_key="cchcs",
            target_stage="priced",
            profile_ids=None,  # default — agency-driven
        )
        result = orch.run(req)

        no_match_blockers = [
            b for b in result.blockers
            if "no requested profile_ids matched" in b
            or "all profile_ids unknown" in b.lower()
        ]
        assert not no_match_blockers, result.blockers
        # agency-driven matched at least the existing 704b + quote profiles
        assert result.profiles_used, "agency-driven resolution should produce profiles"
