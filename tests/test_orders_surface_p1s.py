"""O-9 + O-14 + O-16: three small surface fixes on the Orders module.

O-9   aria-label="if qn  Linked  qn  else" — Jinja conditional delimiters
      were stripped. Rendered as literal DOM garbage; screen reader read
      "if qn Linked qn else". Fix: wrap in {% if qn %}…{% else %}…{% endif %}.

O-14  /api/order/<oid>/reply-all defaulted to GET — any browser prefetch
      or link-preview bot created phantom drafts. Must be POST only.

O-16  Aging badge: `elif stale_days >= 5:` ran before `elif stale_days >= 10:`,
      so the "critical" branch was unreachable. Reordered so >=10 fires first.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta


# ── O-9 ────────────────────────────────────────────────────────────────────

def test_order_detail_link_quote_button_aria_label_is_valid_jinja():
    tpl = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "templates", "order_detail.html",
    )
    with open(tpl, encoding="utf-8") as f:
        html = f.read()

    # Locate the link-quote button (aria-label was the broken spot)
    idx = html.find('id="link-quote-btn"')
    assert idx >= 0, "link-quote button not found"
    # Look at the surrounding 400 chars
    window = html[max(0, idx - 200):idx + 400]

    # The broken literal must be gone
    assert 'aria-label="if qn' not in window, (
        "O-9: literal Jinja text still leaking into aria-label"
    )
    # And a proper {% if %}…{% else %}…{% endif %} must be in place
    assert "{% if qn %}" in window and "{% endif %}" in window, (
        "O-9: aria-label must use a real Jinja {% if qn %}…{% endif %} block"
    )


# ── O-14 ───────────────────────────────────────────────────────────────────

def test_reply_all_route_requires_post():
    """Grep-invariant: route signature must declare methods=['POST']."""
    route_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "api", "modules", "routes_orders_full.py",
    )
    with open(route_file, encoding="utf-8") as f:
        src = f.read()

    # Find the decorator line for reply-all
    idx = src.find("/api/order/<oid>/reply-all")
    assert idx >= 0, "reply-all route not found"
    eol = src.find("\n", idx)
    decorator = src[idx:eol]

    assert 'methods=["POST"]' in decorator or "methods=['POST']" in decorator, (
        f"O-14: reply-all route must be POST-only. Got: {decorator!r}"
    )


def test_reply_all_rejects_get(auth_client):
    """Live route: GET must not execute the handler. Flask raises
    MethodNotAllowed; the catch-all error handler converts that to a 500
    JSON. Either way (405 or 500-with-error-body) the invariant is that
    GET didn't run the mutation. What we reject is a 2xx on GET."""
    resp = auth_client.get("/api/order/ORD-NOPE/reply-all")
    assert resp.status_code not in (200, 201, 302, 303), (
        f"O-14: GET reply-all must not succeed — got {resp.status_code}"
    )
    assert resp.status_code in (405, 500), (
        f"O-14: expected 405 or 500-via-errorhandler for GET, got {resp.status_code}"
    )


def test_reply_all_frontend_uses_fetch_post():
    """Grep-invariant: the button in order_detail.html must no longer use
    an <a href="…reply-all"> anchor (which is GET). It must POST via JS."""
    tpl = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "templates", "order_detail.html",
    )
    with open(tpl, encoding="utf-8") as f:
        html = f.read()

    assert 'href="/api/order/{{ oid }}/reply-all"' not in html, (
        "O-14: anchor-link to reply-all reintroduced (would GET on click)"
    )
    # The fetch POST call must be present
    assert "/reply-all', {method: 'POST'})" in html or \
           "/reply-all\", {method: \"POST\"})" in html, (
        "O-14: frontend must POST to reply-all"
    )


# ── O-16 ───────────────────────────────────────────────────────────────────

def _iso(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).isoformat()


def test_aging_badge_very_stale_is_critical():
    """An order untouched for 12 days (new/sourcing status) must come back
    as critical, not warning — before the fix the >=10 branch was dead code."""
    from src.api.modules.routes_orders_full import calc_order_aging
    result = calc_order_aging({
        "created_at": _iso(12),
        "updated_at": _iso(12),
        "status": "sourcing",
    })
    assert result["severity"] == "critical", (
        f"O-16: 12-day-stale order must be critical, got {result['severity']}"
    )
    assert result["badge"] == "🔴", f"O-16: expected 🔴, got {result['badge']}"


def test_aging_badge_moderately_stale_is_warning():
    """A 7-day-stale order is warning (5-10 band), not critical."""
    from src.api.modules.routes_orders_full import calc_order_aging
    result = calc_order_aging({
        "created_at": _iso(7),
        "updated_at": _iso(7),
        "status": "sourcing",
    })
    assert result["severity"] == "warning", (
        f"O-16: 7-day-stale order must be warning, got {result['severity']}"
    )


def test_aging_badge_closed_always_ok():
    """Closed/invoiced orders are always ok, even if very stale."""
    from src.api.modules.routes_orders_full import calc_order_aging
    result = calc_order_aging({
        "created_at": _iso(60),
        "updated_at": _iso(60),
        "status": "closed",
    })
    assert result["severity"] == "ok"
