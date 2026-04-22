"""Regression guard for O-18: po_tracking_dashboard_legacy was deleted.

The zombie route rendered a PC-era template that no longer existed and read
from purchase_orders (pre-V2). Keeping it live risked silent re-introduction
during refactors. This test fails if anything re-adds the function or its
URL path.
"""
from pathlib import Path

ROUTES = Path(__file__).resolve().parents[1] / "src" / "api" / "modules" / "routes_order_tracking.py"
TEMPLATES = Path(__file__).resolve().parents[1] / "src" / "templates"


def test_legacy_route_function_absent():
    src = ROUTES.read_text(encoding="utf-8")
    assert "po_tracking_dashboard_legacy" not in src, (
        "po_tracking_dashboard_legacy() was deleted in O-18 — do not resurrect it"
    )
    assert "/po-tracking-legacy" not in src, (
        "/po-tracking-legacy URL was deleted in O-18 — do not resurrect it"
    )


def test_orphan_template_absent():
    assert not (TEMPLATES / "po_tracking.html").exists(), (
        "src/templates/po_tracking.html was deleted in O-18 as an orphan — "
        "do not resurrect it without a live route"
    )
