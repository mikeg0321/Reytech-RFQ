"""RFQ-4: stale bundle suggestion banner was deleted 2026-04-21.

The banner at rfq_detail.html:350-360 used `_suggested_bundle_id` + `_suggested_pc_number`
to display "Suggested link: PC #… · Part of N-PC bundle" for unlinked RFQs matching a
bundled PC. Superseded by the richer cchcs-pc-link-panel (PR #290/#302) which shows
match percentages and candidate lists with confirm buttons.

Deletion decision: CCHCS-only UX per operator direction 2026-04-21. Non-CCHCS RFQs
currently do not surface bundle suggestions — follow-up if the need reappears.

This test guards against the block being re-introduced.
"""
from __future__ import annotations
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "templates" / "rfq_detail.html"


def test_suggested_bundle_banner_not_in_template():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "_suggested_bundle_id" not in html, (
        "RFQ-4 regression: stale bundle suggestion banner reintroduced in rfq_detail.html. "
        "The cchcs-pc-link-panel supersedes it."
    )


def test_bundle_suggestion_copy_removed():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Suggested link: PC #" not in html
    assert "Part of a" not in html or "-PC bundle" not in html


def test_cchcs_pc_link_panel_still_present():
    """Ensure we didn't accidentally delete the replacement panel too."""
    html = TEMPLATE.read_text(encoding="utf-8")
    assert 'id="cchcs-pc-link-panel"' in html
    assert 'data-testid="cchcs-pc-link-panel"' in html
