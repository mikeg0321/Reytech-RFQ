"""Lookup timeout + mismatch guard contract — Mike P0 2026-05-05.

Mike: "I would rather it take time and work, validate item, then time out."
Old 15s client / 14s server cap was too short for Claude web_search +
scrape + semantic-match chain. Bumped to 45s client / 42s server. Server
budget is intentionally 3s less than client to leave Flask + Railway
round-trip headroom — if the server returns at the buzzer, the client
should still process the result instead of timing out first.

Same change tightens the client-side cost-fill gate: when description-vs-
title token overlap is < 30%, BLOCK auto-fill regardless of whether
server-side AI claimed a verified match. The 2026-04-23 hallucination
incident (Anker Soundcore returned for an unrelated ASIN) showed the
ASIN-trail check is necessary but not sufficient; a hard description
mismatch is the last-line safety net.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


def test_client_lookup_budget_is_45s():
    """shared_item_utils.js must hold a 45000ms timeout — never less."""
    src = _read("src/static/shared_item_utils.js")
    assert "_LOOKUP_BUDGET_MS = 45000" in src, (
        "Client-side lookup timeout must be 45000ms (Mike P0 2026-05-05). "
        "Old 15000ms cap was insufficient when Claude web_search + scrape "
        "+ semantic-match all chained."
    )
    # No remnant of the old 15000 timer in this file
    m = re.search(r"setTimeout\([^)]+,\s*(\d{4,5})\)", src)
    assert m is None or int(m.group(1)) >= 30000, (
        f"Found a setTimeout with {m.group(1) if m else '?'}ms — must be >= 30000"
    )


def test_client_shows_progress_during_lookup():
    """Spinner must show elapsed seconds — Mike's directive: 'never silent stall'."""
    src = _read("src/static/shared_item_utils.js")
    assert "_renderProgress" in src
    assert "setInterval(_renderProgress, 1000)" in src
    assert "validating product" in src


def test_progress_timer_cleared_on_success_and_failure():
    """The 1s tick must stop when the response arrives, the timeout fires,
    OR the fetch rejects — otherwise it leaks and overwrites real result."""
    src = _read("src/static/shared_item_utils.js")
    # Three call sites: success then(), error catch(), and the 45s timeout
    clears = src.count("clearInterval(_progressTimer)")
    assert clears >= 3, (
        f"clearInterval(_progressTimer) appears {clears} times — needs at "
        "least 3 (success / error / timeout) so the 1s tick stops cleanly."
    )


def test_server_budget_lockstep_with_client():
    """Server _ENDPOINT_BUDGET must be slightly less than the client
    _LOOKUP_BUDGET_MS so the server returns BEFORE the client gives up."""
    server_src = _read("src/api/modules/routes_pricecheck_admin.py")
    assert "_ENDPOINT_BUDGET = 42.0" in server_src, (
        "Server budget must be 42s — 3s less than client 45s to leave "
        "Flask + Railway round-trip headroom."
    )


def test_low_match_score_hard_blocks_cost_fill():
    """When description-vs-title token overlap < 30%, NEVER auto-fill cost.

    The 2026-04-23 hallucination incident (Anker Soundcore returned for an
    unrelated ASIN) bypassed the ASIN-trail check because Claude included
    the right ASIN in tool_use but invented the rest. Description-vs-title
    overlap is the last-line guard: if the words don't match at all, the
    URL is wrong even if the ASIN-presence check passed.
    """
    src = _read("src/static/shared_item_utils.js")
    assert "_matchScore < 30" in src, (
        "Hard block at <30% match score must exist (Mike P0 2026-05-05: "
        "'another URL is still bringing over the anker product')."
    )
    assert "URL likely returned wrong product" in src
