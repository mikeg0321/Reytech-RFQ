"""PC QA agent must read both `requestor` AND `requestor_name` (Surface #9).

Mike's screenshot 2026-05-04 (project_session_2026_05_04_calvet_quote_p0_chain
surface #9): QA panel fired the warning "Requestor/buyer name is empty" on
PC pc_177b18e6 — but the UI clearly displayed `Requestor: William Rice`.

Root cause: `ingest_pipeline.py:772` writes the buyer name to the
`requestor_name` key (cascade: header.requestor → header.requestor_name →
_derive_requestor_name(email_sender)). `pc_qa_agent._check_agency()` was
reading only `pc.get("requestor")`, missing the `_name` suffix.

Same field-name-drift family as PR #720 (requirements_json written but
never read). Per feedback_global_fix_not_one_off: read every key the
ingest pipeline writes, plus the contact/buyer fallbacks for older records.
"""
from __future__ import annotations


from src.agents.pc_qa_agent import _check_agency


def _has_requestor_warning(issues):
    return any(
        i.get("field") == "requestor" and "empty" in i.get("message", "").lower()
        for i in issues
    )


class TestRequestorReadsBothKeys:
    """The exact false-positive Mike saw on pc_177b18e6 must NOT recur."""

    def test_requestor_name_alone_clears_warning(self):
        """ingest_pipeline.py:772 writes `requestor_name`. With that
        populated, no `requestor` key needed, the warning must not fire."""
        pc = {"requestor_name": "William Rice", "items": []}
        issues = _check_agency(pc, [])
        assert not _has_requestor_warning(issues), (
            "Surface #9 regression: pc.requestor_name='William Rice' but "
            "QA still complained 'Requestor/buyer name is empty'. The "
            "QA agent must read both `requestor` AND `requestor_name` per "
            "feedback_global_fix_not_one_off."
        )

    def test_requestor_legacy_key_still_works(self):
        """Old PCs persisted `requestor` directly; don't break those."""
        pc = {"requestor": "Valentina Demidenko", "items": []}
        issues = _check_agency(pc, [])
        assert not _has_requestor_warning(issues)

    def test_contact_name_fallback(self):
        """Some ingest paths populate `contact_name` (per quote_model.py
        and db.py:2271 cascade); that should also count as a requestor."""
        pc = {"contact_name": "Buyer", "items": []}
        issues = _check_agency(pc, [])
        assert not _has_requestor_warning(issues)

    def test_buyer_fallback(self):
        """Older parser path on some forms wrote `buyer` directly."""
        pc = {"buyer": "Valentina Demidenko", "items": []}
        issues = _check_agency(pc, [])
        assert not _has_requestor_warning(issues)

    def test_both_keys_present_no_double_warning(self):
        """When both `requestor` and `requestor_name` exist, the cascade
        succeeds at the first hit. No warning either way."""
        pc = {
            "requestor": "From parser",
            "requestor_name": "From email-derive",
            "items": [],
        }
        issues = _check_agency(pc, [])
        assert not _has_requestor_warning(issues)

    def test_truly_empty_still_warns(self):
        """Don't over-correct — when ALL the keys are missing/empty,
        the warning is correct and should still fire."""
        pc = {
            "requestor": "",
            "requestor_name": "   ",  # whitespace only
            "contact_name": None,
            "items": [],
        }
        issues = _check_agency(pc, [])
        assert _has_requestor_warning(issues), (
            "When every requestor-related field is empty/whitespace, the "
            "QA agent must still warn — otherwise a real ingest miss "
            "(no header, no email-derive) would silently pass through."
        )
