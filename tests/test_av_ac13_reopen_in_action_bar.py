"""PR-AV-AC13 — Reopen button hoisted into the primary action bar.

CONTEXT

When an RFQ is in `sent` / `generated` / `ready_to_send` / `won` / `lost`
status, the line items are locked. A lock banner reads:

    🔒 Sent record — line items locked.
       Click Reopen (in the action bar above) to move this back to
       generated and unlock.

The actual Reopen control existed already (rfq_detail.html L1498/1524/1688)
but in THREE different places, all buried — deliverables panel, secondary
status pill area. None in the top primary-actions bar with Finalize /
Generate / Send.

Mike 2026-05-15 (rebid surface for Mohammed Chechi PC #10846357):
"i cant click the reopen below, it should be on the flow, that i can
reopen back to draft".

THE FIX

Single new <form method=POST action=/rfq/{rid}/reopen> + button rendered
INSIDE the primary actions div (rfq_detail.html ~L840), immediately
after the Mark Sent Manually conditional block. Status gate matches the
post-send lock banner (sent/generated/ready_to_send/won/lost). Plus
banner text updated to point at "the action bar above" instead of
"(below)".

WHAT THIS TEST PINS

  - PR-AV-AC13 marker present in rfq_detail.html
  - The Reopen <form> is INSIDE the primary-actions div, BEFORE the
    closing </div> for that section, NOT just after L1498
  - The new button uses data-testid="rfq-reopen-primary" so chrome-mcp
    + future regression tests can target it directly
  - The lock banner text reads "(in the action bar above)" not "(below)"
  - Status gate is the same set the lock banner uses
"""
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPO_ROOT / "src" / "templates" / "rfq_detail.html"


def test_ac13_marker_present():
    src = TARGET.read_text(encoding="utf-8")
    assert "PR-AV-AC13" in src, "PR-AV-AC13 marker must remain in rfq_detail.html"


def test_ac13_reopen_form_present():
    src = TARGET.read_text(encoding="utf-8")
    assert 'data-testid="rfq-reopen-primary"' in src, (
        "AC13 must add a data-testid=rfq-reopen-primary button so chrome-mcp + "
        "regression tests can target the hoisted Reopen control"
    )
    # POST form to /reopen endpoint with the rid template var
    assert 'action="/rfq/{{rid}}/reopen"' in src, (
        "AC13 button must POST to /rfq/<rid>/reopen — same endpoint as the "
        "existing buried Reopen forms"
    )


def test_ac13_reopen_status_gate_matches_lock_banner():
    """The Reopen button must show for the EXACT same status set the lock
    banner targets — sent / generated / ready_to_send / won / lost — so
    the banner-pointer matches what the operator sees.
    """
    src = TARGET.read_text(encoding="utf-8")
    ac13_idx = src.find("PR-AV-AC13")
    assert ac13_idx > 0
    block = src[ac13_idx:ac13_idx + 2000]
    assert "'sent'" in block and "'generated'" in block and "'ready_to_send'" in block
    assert "'won'" in block and "'lost'" in block, (
        "AC13 must gate on the full post-send status set (sent, generated, "
        "ready_to_send, won, lost) so the banner-pointer always matches"
    )


def test_ac13_reopen_in_primary_actions_section():
    """The Reopen button must be inside the primary-actions div (where
    Finalize / Generate / Send live), not elsewhere on the page. We
    pin this by source-order: AC13 marker must appear AFTER the
    rfq-primary-actions container open AND BEFORE the More-actions
    dropdown markup that closes the same section.
    """
    src = TARGET.read_text(encoding="utf-8")
    primary_open = src.find('data-testid="rfq-primary-actions"')
    more_actions = src.find('id="rfq-more-actions-btn"')
    ac13_idx = src.find("PR-AV-AC13")
    assert primary_open > 0 and more_actions > 0 and ac13_idx > 0
    assert primary_open < ac13_idx < more_actions, (
        f"AC13 Reopen must sit INSIDE the primary-actions div: "
        f"primary_open={primary_open}, ac13={ac13_idx}, more={more_actions}"
    )


def test_ac13_lock_banner_points_to_action_bar():
    """The lock banner that prompts the operator to click Reopen must
    now point UP to the action bar, not 'below' (which was wrong since
    the buried buttons were mid-page, above the lock banner).
    """
    src = TARGET.read_text(encoding="utf-8")
    assert "in the action bar above" in src, (
        "AC13 must update lock banner text to point at 'the action bar "
        "above' instead of the misleading '(below)'"
    )
    # Defense: ensure the prior misleading text is gone from the banner.
    # Search just the post-send-lock-banner block.
    banner_idx = src.find('rfq-post-send-lock-banner')
    assert banner_idx > 0
    banner_block = src[banner_idx:banner_idx + 700]
    assert "(below)" not in banner_block, (
        "AC13 must remove the misleading '(below)' pointer from the lock "
        "banner (the actual buttons were above, not below)"
    )
