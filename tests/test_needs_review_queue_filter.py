"""Regression tests for the home queue's status filter.

PR-A added status='needs_review' as the triage bucket; PR #666 added
the orange badge in queue_helpers. But the home route's actionable-
status SETs (`_pc_actionable` and `_actionable_rfq` in routes_rfq.home)
were missing 'needs_review' — so triage rows never reached the queue
table to render the badge.

Locks the active-set inclusion so a future tweak doesn't regress and
silently re-hide the operator's triage queue.
"""
from __future__ import annotations


def test_needs_review_in_pc_actionable_set():
    """The status set is defined inside the home() route function, so we
    introspect via the source rather than importing — but the easier
    contract test: load the home route and confirm a needs_review PC
    is in the queryset."""
    from src.api.modules.routes_rfq import home
    import inspect
    src = inspect.getsource(home)
    # Lock that needs_review appears in the actionable set literal
    assert "_pc_actionable" in src
    # Find the line with _pc_actionable assignment + verify needs_review present
    in_actionable = False
    for ln in src.splitlines():
        if "_pc_actionable" in ln and "{" in ln:
            assert "needs_review" in ln, (
                f"needs_review missing from _pc_actionable: {ln.strip()}"
            )
            in_actionable = True
            break
    assert in_actionable, "could not locate _pc_actionable assignment"


def test_needs_review_in_rfq_actionable_set():
    from src.api.modules.routes_rfq import home
    import inspect
    src = inspect.getsource(home)
    assert "_actionable_rfq" in src
    in_actionable = False
    for ln in src.splitlines():
        if "_actionable_rfq" in ln and "{" in ln:
            assert "needs_review" in ln, (
                f"needs_review missing from _actionable_rfq: {ln.strip()}"
            )
            in_actionable = True
            break
    assert in_actionable, "could not locate _actionable_rfq assignment"


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
