"""RE-AUDIT-9 regression guard.

Fourteen destructive admin routes in `routes_pricecheck_admin.py` still
accepted HTTP GET. A GET-accepting destructive route is a classic CSRF
vector: an `<img src=.../api/admin/purge-rfqs>` tag rendered inside any
page the admin's browser loads (Gmail web, a markdown preview, a stale
tab, even a phishing email) fires the route with the admin's session
cookie already attached.

The three dashboard.py delete routes were hardened under RE-AUDIT-8.
This guard pins the fourteen `routes_pricecheck_admin.py` siblings:

- /api/pricechecks/bulk-reenrich
- /api/pricecheck/<pcid>/retry-auto-price
- /api/admin/cleanup
- /api/admin/rescan-item-numbers
- /api/admin/counter-set
- /api/admin/delete-quotes
- /api/admin/recall
- /api/admin/purge-rfqs
- /api/admin/clean-activity
- /api/admin/undo-mark-won/<pcid>
- /api/admin/backfill-wins
- /api/admin/backfill-contacts
- /api/admin/import-contacts
- /api/admin/rfq-cleanup

Each must be `methods=["POST"]` only. The test greps the source, so it
catches both a revert to `["GET", "POST"]` and a sneak-in `["POST", "GET"]`.
"""
from __future__ import annotations

import re
from pathlib import Path

ROUTES = (
    Path(__file__).resolve().parents[1]
    / "src" / "api" / "modules" / "routes_pricecheck_admin.py"
)


DESTRUCTIVE_ROUTES = [
    "/api/pricechecks/bulk-reenrich",
    "/api/pricecheck/<pcid>/retry-auto-price",
    "/api/admin/cleanup",
    "/api/admin/rescan-item-numbers",
    "/api/admin/counter-set",
    "/api/admin/delete-quotes",
    "/api/admin/recall",
    "/api/admin/purge-rfqs",
    "/api/admin/clean-activity",
    "/api/admin/undo-mark-won/<pcid>",
    "/api/admin/backfill-wins",
    "/api/admin/backfill-contacts",
    "/api/admin/import-contacts",
    "/api/admin/rfq-cleanup",
]


def _source() -> str:
    return ROUTES.read_text(encoding="utf-8")


def _route_decorator(src: str, route: str) -> str:
    """Return the exact @bp.route(...) decorator line for `route`."""
    escaped = re.escape(route)
    m = re.search(
        rf'@bp\.route\("{escaped}",\s*methods=\[[^\]]+\]\)',
        src,
    )
    assert m, (
        f"RE-AUDIT-9 regression: route {route!r} not found in "
        f"routes_pricecheck_admin.py — was it renamed or deleted?"
    )
    return m.group(0)


def test_destructive_routes_reject_get():
    """Every destructive admin route in the list must be POST-only."""
    src = _source()
    leaks = []
    for route in DESTRUCTIVE_ROUTES:
        decorator = _route_decorator(src, route)
        if '"GET"' in decorator:
            leaks.append((route, decorator))
    assert not leaks, (
        "RE-AUDIT-9 regression: these destructive admin routes still "
        "accept GET. A <img src=...> CSRF tag will fire them with the "
        "admin's session cookie:\n" +
        "\n".join(f"  {r}  →  {d}" for r, d in leaks)
    )


def test_destructive_routes_still_accept_post():
    """POST must remain — the legit UI forms all POST."""
    src = _source()
    missing = []
    for route in DESTRUCTIVE_ROUTES:
        decorator = _route_decorator(src, route)
        if '"POST"' not in decorator:
            missing.append((route, decorator))
    assert not missing, (
        "RE-AUDIT-9 regression: these destructive routes lost POST in "
        "the GET-strip. The UI forms post to them — dropping POST "
        "breaks the admin panel:\n" +
        "\n".join(f"  {r}  →  {d}" for r, d in missing)
    )
