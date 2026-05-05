"""PC detail page: Mark Sent Manually escape valve (Surface #12).

Mike's screenshot 2026-05-04 chain (project_session_2026_05_04_calvet_quote_p0_chain
surface #12): the only PC mark-sent button on pc_detail.html was gated by
`status in ('completed','converted')`. When status drift kept a PC stuck in
`parsed`/`priced` (surface #11), there was no operator override to flip
status to sent — violating feedback_ten_minute_escape_valve which mandates
every quote-time problem be fixable BY MIKE, IN THE APP, IN UNDER 10 MIN.

Fix: mirror RFQ's Bundle-5 PR-5b modal — surface a `Mark Sent Manually`
entry in the More dropdown for any non-sent, non-terminal status. POSTs
multipart to `/api/pricecheck/<pcid>/mark-sent-manually` (already exists
in routes_pricecheck_pricing.py — no new route).
"""
from __future__ import annotations

from pathlib import Path


PC_DETAIL = Path("src/templates/pc_detail.html")


def _read_template() -> str:
    return PC_DETAIL.read_text(encoding="utf-8")


def test_template_defines_pc_allow_mark_sent_var():
    """Source-level guard: the gate variable must exist so other parts of
    the template can reference it. If a future PR drops the variable, the
    modal + menu both silently disappear (Jinja false-y resolution)."""
    src = _read_template()
    assert "_pc_allow_mark_sent" in src, (
        "_pc_allow_mark_sent variable missing — Surface #12 escape valve "
        "won't render."
    )
    assert "_pc_terminal_statuses" in src, (
        "_pc_terminal_statuses tuple missing — gate logic won't filter "
        "won/lost/cancelled correctly."
    )


def test_more_dropdown_includes_mark_sent_manually():
    """The More dropdown's Send section must include the Mark Sent option
    so it's reachable from every non-terminal stage."""
    src = _read_template()
    assert 'data-testid="pc-mark-sent-manually"' in src, (
        "Mark Sent Manually button missing from More dropdown. Surface #12 "
        "operator escape valve must be reachable in any non-sent stage."
    )
    assert "openPcMarkSentModal()" in src, (
        "openPcMarkSentModal handler not wired."
    )


def test_modal_posts_to_correct_endpoint():
    """Modal must POST to /api/pricecheck/<pcid>/mark-sent-manually so the
    server-side hook (status flip + lifecycle log + activity log + Drive
    archive) fires correctly. RFQ's modal hits /api/rfq/<rid>/... — easy
    to copy-paste-bug if not pinned."""
    src = _read_template()
    assert "/api/pricecheck/' + pcid + '/mark-sent-manually" in src, (
        "Modal not POSTing to /api/pricecheck/<pcid>/mark-sent-manually. "
        "Mike's flow will silently fail or hit the wrong endpoint."
    )
    # Catch the easy regression of accidentally pointing at the RFQ endpoint
    assert "/api/rfq/' + pcid + '/mark-sent-manually" not in src, (
        "Modal points at /api/rfq/ — copy-paste regression from RFQ template."
    )


def test_modal_captures_attachment_and_notes():
    """Per the audit log, Mark Sent must capture: sent_to, sent_at, optional
    attachment, optional notes. Server hook reads these from request.form
    + request.files['attachment']."""
    src = _read_template()
    for field in ('name="sent_to"', 'name="sent_at"',
                  'name="attachment"', 'name="notes"'):
        assert field in src, (
            f"Modal missing {field} field. Server-side audit log will be "
            f"incomplete and Mike won't have full record of manual sends."
        )


def test_modal_uses_multipart_form_data():
    """The server hook switches on `request.content_type.startswith(
    'multipart/')`. JSON body would silently bypass attachment handling."""
    src = _read_template()
    # FormData() automatically sends multipart/form-data — that's the right
    # client-side contract.
    assert "new FormData(form)" in src, (
        "Modal not using FormData — attachment field will be ignored "
        "by the server's multipart handler."
    )


def test_endpoint_exists_in_routes_pricecheck_pricing():
    """Source-level guard: don't ship a UI button that points at a route
    that doesn't exist."""
    routes_src = Path(
        "src/api/modules/routes_pricecheck_pricing.py"
    ).read_text(encoding="utf-8")
    assert (
        '@bp.route("/api/pricecheck/<pcid>/mark-sent-manually", '
        'methods=["POST"])'
    ) in routes_src, (
        "Backend route /api/pricecheck/<pcid>/mark-sent-manually missing — "
        "the UI button shipping without it leaves Mike's manual sends in "
        "the same broken state."
    )


def test_mark_sent_escape_valve_gated_by_status():
    """The modal + menu entry are inside `{% if _pc_allow_mark_sent %}` so
    a `sent` PC doesn't show duplicate Mark Sent buttons. Source-level
    check: both the dropdown entry AND the modal must be inside the gate."""
    src = _read_template()
    # Snapshot the lines around the data-testid to confirm the gate.
    idx = src.find('data-testid="pc-mark-sent-manually"')
    assert idx > 0
    # Walk back to find the gate
    preamble = src[max(0, idx - 800):idx]
    assert "{% if _pc_allow_mark_sent %}" in preamble, (
        "Mark Sent menu entry not gated by _pc_allow_mark_sent — will "
        "double-render on already-sent PCs."
    )

    # Modal block check
    mod_idx = src.find('id="pc-mark-sent-modal"')
    assert mod_idx > 0
    mod_preamble = src[max(0, mod_idx - 400):mod_idx]
    assert "{% if _pc_allow_mark_sent %}" in mod_preamble, (
        "Mark Sent modal not gated by _pc_allow_mark_sent — DOM clutter "
        "on terminal-state PCs."
    )
