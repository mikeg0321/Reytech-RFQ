"""Bug 1 — bottom action bar overlay blocked clicks on small viewports.

The sticky CTA (`#rfq-sticky-cta`, position:fixed bottom:0) is paired
with an in-flow `[data-testid="rfq-sticky-spacer"]` whose height
reserves the same vertical space so the last rows of main content
clear the bar. The old fixed 64px under-reserved when the bar wraps
to 2 rows on narrow viewports (~96-112px), letting the bar overlay
clickable buttons in the activity row.

This file locks:
  1. The spacer markup is present on every RFQ detail page (server-side
     guarantee — JS toggles its display:none/block but never removes it).
  2. The runtime ResizeObserver wiring is present in the served HTML so
     the spacer height auto-tracks the sticky bar's actual offsetHeight.
  3. The new 80px floor (up from 64px) is the static height attribute.
"""
from __future__ import annotations


def test_sticky_spacer_renders_with_80px_floor(auth_client, temp_data_dir):
    """Spacer markup is present with the new 80px floor — that floor is
    what reserves space if ResizeObserver hasn't fired yet (initial
    paint, browsers lacking ResizeObserver, etc.)."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_spacer_floor"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "SP-FLOOR",
        "solicitation_number": "SP-FLOOR",
        "line_items": [{"description": "X", "qty": 1}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-sticky-spacer"' in html
    assert "height:80px" in html
    # Old 64px constant must be gone — symptom of the regression.
    assert 'data-testid="rfq-sticky-spacer" aria-hidden="true" style="height:64px' not in html


def test_sticky_spacer_resize_observer_wired(auth_client, temp_data_dir):
    """ResizeObserver block must be in the served HTML so the spacer
    auto-grows when the sticky bar wraps onto multiple rows."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_spacer_ro"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "SP-RO",
        "solicitation_number": "SP-RO",
        "line_items": [{"description": "X", "qty": 1}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    # Wiring sentinels — both the function and the observer attach.
    assert "_syncSpacerHeight" in html
    assert "new ResizeObserver" in html
    assert "ro.observe(sticky)" in html


def test_sticky_spacer_resize_runs_on_show(auth_client, temp_data_dir):
    """When the IntersectionObserver toggles the bar visible, the spacer
    height must be re-synced — _show(true) calls _syncSpacerHeight()
    so the bar's just-rendered height lands in the spacer immediately
    instead of waiting for a separate ResizeObserver tick."""
    from src.api.data_layer import _save_single_rfq
    rid = "rfq_spacer_show"
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "SP-SHOW",
        "solicitation_number": "SP-SHOW",
        "line_items": [{"description": "X", "qty": 1}],
    })
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    # Inside the function _show(yes), the call must appear after the toggle.
    assert "if (yes) _syncSpacerHeight()" in html
