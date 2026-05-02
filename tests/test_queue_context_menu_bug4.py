"""Bug 4 — right-click context menu on queue rows for Mark Sent.

Mike's bug 2026-05-02 (image #5): "manually on right click 'sent' if
not through app". Operator emails outside the app, then needs to reach
the Mark Sent flip without clicking into each RFQ. Right-click puts
that action one cursor-trip away.

This file locks:
  1. Each queue row has `oncontextmenu` wired to showQueueContextMenu
     and `data-status` / `data-url` so the menu can gate + open.
  2. The global context menu element + its three primary actions
     (Mark Sent, Open in new tab, Cancel) render on the home page.
  3. The supporting CSS hooks (#global-queue-ctx-menu) are present.
  4. The Mark Sent JS is wired to the existing /api/{type}/<id>/
     mark-sent-manually endpoint (no new backend route to maintain).
"""
from __future__ import annotations


def _seed_rfq(rid="rfq_ctx", status="generated"):
    from src.api.data_layer import _save_single_rfq
    _save_single_rfq(rid, {
        "id": rid, "status": status,
        "rfq_number": "CTX-1",
        "solicitation_number": "CTX-1",
        "institution": "CCHCS",
        "requestor_email": "buyer@x.gov",
        "line_items": [{"description": "X", "qty": 1, "price_per_unit": 50.0}],
    })
    return rid


def test_queue_row_has_context_menu_handlers(auth_client, temp_data_dir):
    rid = _seed_rfq()
    resp = auth_client.get("/")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    # Row testid + handler + new dataset attributes.
    assert f'data-testid="queue-row-rfq-{rid}"' in html
    assert "oncontextmenu=\"showQueueContextMenu(" in html
    # Status + URL exposed for menu gating + open-in-new-tab.
    assert 'data-status="generated"' in html
    assert 'data-url="' in html


def test_global_context_menu_renders_on_home(auth_client, temp_data_dir):
    """The popup itself must be in the page, with its three actions."""
    _seed_rfq()
    resp = auth_client.get("/")
    html = resp.data.decode("utf-8", errors="replace")
    assert 'id="global-queue-ctx-menu"' in html
    assert 'data-testid="queue-row-context-menu"' in html
    # Mark Sent action.
    assert 'data-testid="queue-ctx-mark-sent"' in html
    assert "_quickMarkSent()" in html
    # Open in new tab action.
    assert 'data-testid="queue-ctx-open"' in html
    assert 'target="_blank"' in html
    # Cancel action — no testid required, just the label.
    assert ">Cancel<" in html


def test_context_menu_styles_present(auth_client, temp_data_dir):
    _seed_rfq()
    resp = auth_client.get("/")
    html = resp.data.decode("utf-8", errors="replace")
    assert "#global-queue-ctx-menu {" in html
    assert "z-index:99999" in html
    # Disabled-state styling so terminal-status rows visibly gray Mark Sent.
    assert "#global-queue-ctx-menu button[disabled]" in html


def test_quick_mark_sent_uses_existing_endpoint(auth_client, temp_data_dir):
    """No new server route — JS must POST to the existing /api/{type}/<id>/
    mark-sent-manually that already supports manual operators."""
    _seed_rfq()
    resp = auth_client.get("/")
    html = resp.data.decode("utf-8", errors="replace")
    assert "/mark-sent-manually" in html
    # FormData send (multipart) keeps the existing endpoint contract intact.
    assert "new FormData()" in html


def test_terminal_status_row_marks_button_disabled_in_js(
        auth_client, temp_data_dir):
    """JS gates Mark Sent based on data-status. Verify the gate logic
    references the canonical terminal set so a right-click on a
    sent/won/lost row shows a disabled Mark Sent button."""
    _seed_rfq()
    resp = auth_client.get("/")
    html = resp.data.decode("utf-8", errors="replace")
    # Sentinel: the JS array of terminal statuses.
    assert "TERMINAL_STATUSES" in html
    for term in ("sent", "won", "lost", "no_bid", "cancelled"):
        assert f"'{term}'" in html
