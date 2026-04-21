"""Regression guard: POST /api/rfq/<rid>/convert-to-pc must stay gone.

Timeline:
  - 2026-03-23 (59ad5aad): added the route + a 'Convert to PC' button
    on rfq_detail.html, for fixing misrouted CDCR/CCHCS emails.
  - 2026-04-03 (02fadac2): Mike removed the button with the note:
    "RFQs are never converted to PCs. The button was a leftover from
    early development when email classification was unreliable."
  - The backend route in routes_rfq_admin.py was NOT deleted in that
    cleanup and has been orphaned ever since. No UI callers, no tests,
    no docs. Audit 2026-04-21 flagged it as dead code.

This test locks the deletion in so a future refactor can't accidentally
revive a workflow Mike explicitly rejected.
"""
from __future__ import annotations


def test_convert_to_pc_route_returns_404(client, seed_rfq):
    """The route is deleted — POST must 404 (not 405, not 200)."""
    resp = client.post(f"/api/rfq/{seed_rfq}/convert-to-pc")
    assert resp.status_code == 404, (
        f"convert-to-pc route is back. This workflow was explicitly "
        f"deprecated 2026-04-03 ('RFQs are never converted to PCs'). "
        f"If you need it again, talk to Mike first. Got {resp.status_code}."
    )


def test_convert_to_pc_handler_not_in_source():
    """Belt-and-suspenders: the handler function and its route decorator
    are gone from the source file, not just unregistered at import time.

    routes_rfq_admin.py is loaded via exec() into dashboard.py's namespace
    (see CLAUDE.md "Module loading"), so inspecting it as an importable
    module doesn't work cleanly — grep the source instead."""
    import pathlib
    src = pathlib.Path("src/api/modules/routes_rfq_admin.py").read_text(encoding="utf-8")
    assert "api_rfq_convert_to_pc" not in src, (
        "api_rfq_convert_to_pc() function is back in routes_rfq_admin.py"
    )
    assert "/convert-to-pc" not in src, (
        "convert-to-pc route decorator is back in routes_rfq_admin.py"
    )
