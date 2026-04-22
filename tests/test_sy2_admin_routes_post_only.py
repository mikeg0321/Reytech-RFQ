"""SY-2 regression guard: destructive admin routes must reject GET.

Audited 2026-04-22 — three routes in routes_system.py registered both GET and
POST methods. The CSRF origin guard only fires for POST/PUT/DELETE/PATCH, so
a crafted `<img src="/api/system/resync-scprs">` on any page an admin opens
would run `DELETE FROM won_quotes` on their session. `backup-now` and
`sync-scprs` were CSRF'able the same way.

These tests are source-level guards — CI fails if any of the three routes
re-admits GET.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_SYSTEM = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_system.py"
)


def _find_decorator_for(route: str, src: str) -> str:
    """Return the @bp.route decorator line for a given path, or empty string."""
    pattern = rf'@bp\.route\(\s*"{re.escape(route)}"[^)]*\)'
    m = re.search(pattern, src)
    return m.group(0) if m else ""


def test_admin_backup_now_post_only():
    src = ROUTES_SYSTEM.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/admin/backup-now", src)
    assert dec, "SY-2: /api/admin/backup-now route decorator not found"
    assert '"GET"' not in dec, (
        f"SY-2 regression: /api/admin/backup-now admits GET — {dec!r}. "
        "Must be POST-only so CSRF origin guard fires."
    )
    assert '"POST"' in dec, (
        f"SY-2: /api/admin/backup-now must still accept POST — {dec!r}."
    )


def test_resync_scprs_post_only():
    src = ROUTES_SYSTEM.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/system/resync-scprs", src)
    assert dec, "SY-2: /api/system/resync-scprs route decorator not found"
    assert '"GET"' not in dec, (
        f"SY-2 regression: /api/system/resync-scprs admits GET — {dec!r}. "
        "This route runs DELETE FROM won_quotes — GET + CSRF bypass is a "
        "data-destruction vector."
    )
    assert '"POST"' in dec


def test_sync_scprs_post_only():
    src = ROUTES_SYSTEM.read_text(encoding="utf-8")
    dec = _find_decorator_for("/api/system/sync-scprs", src)
    assert dec, "SY-2: /api/system/sync-scprs route decorator not found"
    assert '"GET"' not in dec, (
        f"SY-2 regression: /api/system/sync-scprs admits GET — {dec!r}. "
        "Must be POST-only so CSRF origin guard fires."
    )
    assert '"POST"' in dec


def test_backup_now_ui_uses_post():
    """The settings.html button must fetch with method:'POST' — a plain
    `<a href>` would hit 405 now and regress CSRF posture if reverted."""
    settings_html = (
        Path(__file__).resolve().parents[1] / "src" / "templates" / "settings.html"
    )
    html = settings_html.read_text(encoding="utf-8")
    assert 'href="/api/admin/backup-now"' not in html, (
        "SY-2 UI regression: settings.html linked /api/admin/backup-now as a "
        "plain href — that issues GET and will 405 now. Use fetch(..., "
        "{method:'POST'}) instead."
    )
    assert "'/api/admin/backup-now'" in html or '"/api/admin/backup-now"' in html, (
        "settings.html should still reference /api/admin/backup-now via a "
        "POST fetch."
    )
    assert "method:'POST'" in html or 'method:"POST"' in html, (
        "settings.html backup button must POST to /api/admin/backup-now."
    )
