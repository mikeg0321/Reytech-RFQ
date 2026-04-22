"""O-8 + RFQ-3 + PC-14 cross-module platform fix.

Registers the `agency_display` helper as a Jinja filter so every template
can render `{{ agency|agency_display }}` instead of carrying its own
lowercase→display dict. Closes the class of `cchcs` / `calvet` leaks in
the UI.

Tests:
  1. filter is registered on the live app.jinja_env
  2. filter renders known lowercase keys to canonical display casing
  3. grep-invariants on orders.html:120 and order_detail.html:17 — the
     two exact sites flagged by O-8 must use the filter
"""
from __future__ import annotations

import os

from flask import render_template_string


def test_agency_display_filter_registered(app):
    """App boot must register agency_display on jinja_env.filters."""
    assert "agency_display" in app.jinja_env.filters, (
        "O-8: agency_display filter not registered — templates that use "
        "`|agency_display` will render lowercase leaks"
    )


def test_agency_display_filter_normalizes_lowercase(app):
    """Smoke: 'cchcs' → 'CCHCS' through the filter pipeline."""
    with app.test_request_context():
        out = render_template_string(
            "{{ value|agency_display }}",
            value="cchcs",
        )
    assert out == "CCHCS", f"expected 'CCHCS', got {out!r}"


def test_agency_display_filter_passes_through_canonical(app):
    """'CalVet' (already canonical) must not be mangled."""
    with app.test_request_context():
        out = render_template_string(
            "{{ value|agency_display }}",
            value="calvet",
        )
    assert out == "CalVet", f"expected 'CalVet', got {out!r}"


def test_agency_display_filter_empty_safe(app):
    """Empty / None must render as empty string so `{% if agency %}`
    guards keep working."""
    with app.test_request_context():
        assert render_template_string("{{ value|agency_display }}", value="") == ""
        assert render_template_string("{{ value|agency_display }}", value=None) == ""


def test_orders_queue_template_pipes_agency_through_filter():
    """Grep-invariant: orders.html must apply |agency_display to o.agency."""
    tpl = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "templates", "orders.html",
    )
    with open(tpl, encoding="utf-8") as f:
        html = f.read()
    assert "o.agency|default('')|agency_display" in html, (
        "O-8: orders.html line 120 must render agency through |agency_display"
    )


def test_order_detail_template_pipes_agency_through_filter():
    """Grep-invariant: order_detail.html must apply |agency_display."""
    tpl = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "templates", "order_detail.html",
    )
    with open(tpl, encoding="utf-8") as f:
        html = f.read()
    assert "order.get('agency','')|agency_display" in html, (
        "O-8: order_detail.html line 17 must render agency through |agency_display"
    )
