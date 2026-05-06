"""Home page = Urgent ONLY, accurate.

Mike P0 2026-05-06: "ignore all because of stale data; the
action_needed is important but I do step A in the app directly".
Translation: Action Needed and Progress cards weren't earning their
real estate while quoting flow still has rough edges. Until quote-
sending is reliable, growth/progress widgets just compete for
attention without driving operator action.

Substrate rule pinned by this test:
  - home.html renders the Urgent card and only the Urgent card.
  - Action Items panel at top is gone.
  - Action Needed and Progress card divs are gone from DOM.
  - get_expiring_soon cross-checks the rfqs table to filter quotes
    whose linked RFQ was marked won/lost (the actual staleness
    Mike was complaining about — `api_rfq_mark_won` doesn't
    propagate to the quotes table).
"""
from __future__ import annotations

import os


def _read(path: str) -> str:
    full = os.path.join(os.path.dirname(__file__), "..", path)
    with open(full, encoding="utf-8") as f:
        return f.read()


# ── UI: deletions ─────────────────────────────────────────────────


def test_action_items_panel_deleted_from_top_of_home():
    """The Action Items widget at the top of home.html is gone."""
    src = _read("src/templates/home.html")
    assert 'id="action-dash"' in src, (
        "ad-urgent container should still exist (Urgent card lives there)"
    )
    # Old top-of-page widget marker — must be absent
    assert "Action Items <span" not in src, (
        "Top-of-page Action Items widget must be deleted"
    )
    # action_items|default is the Jinja conditional that gated the widget
    assert "{% if action_items|default([])|length > 0 %}" not in src


def test_action_needed_card_deleted_from_action_dashboard():
    src = _read("src/templates/home.html")
    assert 'id="ad-action"' not in src, (
        "Action Needed (yellow) card div must be deleted"
    )
    assert 'id="ad-action-items"' not in src, (
        "Action Needed render target must be deleted"
    )


def test_progress_card_deleted_from_action_dashboard():
    src = _read("src/templates/home.html")
    assert 'id="ad-progress"' not in src, (
        "Progress (green) card div must be deleted"
    )
    assert 'id="ad-progress-items"' not in src, (
        "Progress render target must be deleted"
    )


def test_urgent_card_still_renders():
    src = _read("src/templates/home.html")
    assert 'id="ad-urgent"' in src, "Urgent card must still exist"
    assert 'id="ad-urgent-items"' in src, "Urgent render target must exist"
    # JS must still call renderItems for urgent
    assert "renderItems(d.actions.urgent" in src


# ── Server: Urgent staleness fix ──────────────────────────────────


def test_get_expiring_soon_cross_checks_rfqs_terminal_status():
    """Mike's a5b09b56-style "stale data" complaint root cause:
    `api_rfq_mark_won` writes rfqs.status='won' but quotes.status
    stays at 'sent'. Without cross-check, Urgent shows the just-
    won quote as still expiring.
    """
    src = _read("src/agents/quote_lifecycle.py")

    # Find the get_expiring_soon function body
    start = src.index("def get_expiring_soon(")
    next_def = src.find("\ndef ", start + 1)
    body = src[start:next_def] if next_def > 0 else src[start:]

    # Must read from rfqs table to learn terminal status
    assert "FROM rfqs" in body, (
        "get_expiring_soon must cross-check the rfqs table — quotes.status "
        "alone is stale because api_rfq_mark_won doesn't propagate to it"
    )

    # Must filter on terminal RFQ statuses
    for terminal in ("won", "lost", "cancelled"):
        assert f"'{terminal}'" in body, (
            f"get_expiring_soon must filter quotes whose linked RFQ "
            f"is in terminal status '{terminal}'"
        )

    # Must return only quotes whose linked RFQ is NOT terminal
    assert "terminal_rfq_quote_nums" in body, (
        "get_expiring_soon must build a set of terminal-RFQ quote numbers "
        "and exclude them from the result"
    )
