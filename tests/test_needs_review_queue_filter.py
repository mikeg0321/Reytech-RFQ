"""Regression tests for the home queue's status filter.

PR-A added status='needs_review' as the triage bucket; PR #666 added
the orange badge in queue_helpers. PR-3 (#693) replaced the home
route's allow-list (`_pc_actionable` / `_actionable_rfq`) with the
canonical `is_active_queue` deny-list, so the contract is now: any
status NOT in {sent, pending_award, won, lost, no_bid, cancelled}
appears in the queue. needs_review is therefore included by default,
along with any future triage status someone adds.

These tests now lock the canonical predicate's behavior, not the
deleted set literals.
"""
from __future__ import annotations


def test_needs_review_pc_passes_active_queue_predicate():
    """Triage rows must surface in the home queue — the orange
    'Needs Review' badge has nowhere to render if the predicate
    drops them."""
    from src.core.canonical_state import is_active_queue
    rec = {"status": "needs_review", "is_test": 0}
    assert is_active_queue(rec) is True


def test_needs_review_rfq_passes_active_queue_predicate():
    from src.core.canonical_state import is_active_queue
    rec = {"status": "needs_review", "is_test": 0}
    assert is_active_queue(rec) is True


def test_home_route_uses_canonical_is_active_queue():
    """Lock the migration: home() must call canonical, not redefine
    its own status set. PR-6 will harden this with a pre-push lint
    guard; for now the test grep is the gate."""
    from src.api.modules.routes_rfq import home
    import inspect
    src = inspect.getsource(home)
    assert "is_active_queue" in src, (
        "PR-3: home() must use canonical is_active_queue predicate"
    )
    # Sanity: the deleted allow-list assignments should not have
    # crept back. Match the assignment pattern (`= {`) rather than
    # the bare name so archeology comments stay legal.
    assert "_pc_actionable = {" not in src, (
        "PR-3 regressed: home() reintroduced the _pc_actionable "
        "allow-list — use canonical is_active_queue instead"
    )
    assert "_actionable_rfq = {" not in src, (
        "PR-3 regressed: home() reintroduced the _actionable_rfq "
        "allow-list — use canonical is_active_queue instead"
    )


def test_pricecheck_list_display_status_maps_needs_review():
    """The /pricecheck list page builds DISPLAY_STATUS for filter chips.
    needs_review must map to a real bucket so records aren't dropped
    from the count when the operator filters. The route module is loaded
    via exec() (per CLAUDE.md) so we grep the source file directly
    rather than importing."""
    import os
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "api", "modules", "routes_pricecheck_pricing.py",
    )
    with open(path, encoding="utf-8") as f:
        src = f.read()
    # Find the DISPLAY_STATUS dict and verify needs_review key is mapped
    assert "DISPLAY_STATUS" in src
    assert '"needs_review"' in src, (
        "needs_review missing from /pricecheck DISPLAY_STATUS map"
    )
