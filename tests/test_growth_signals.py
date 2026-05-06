"""Growth signals endpoint — PR-3 from the 2026-05-06 audit.

The endpoint surfaces buyer-last-won + SCPRS-ceiling per line item
on the quote detail page, replacing a 2-click navigation to
`/growth-intel/buyer` with an inline panel.

These tests pin:
- The endpoint exists and is mounted on /api/quote/<doc_type>/<id>/...
- doc_type validation
- The shape returned per line item
- The rfq_detail.html and pc_detail.html templates fetch from it
- The dashboard registers routes_growth_signals
"""
import os


def _read(path):
    p = os.path.join(os.path.dirname(__file__), "..", path)
    with open(p, encoding="utf-8") as f:
        return f.read()


def test_module_registered_in_dashboard():
    src = _read("src/api/dashboard.py")
    assert '"routes_growth_signals"' in src, (
        "routes_growth_signals must be in _ROUTE_MODULES so the blueprint loads"
    )


def test_endpoint_route_pattern():
    src = _read("src/api/modules/routes_growth_signals.py")
    assert "/api/quote/<doc_type>/<rid>/growth-signals" in src
    assert "def api_quote_growth_signals" in src


def test_endpoint_validates_doc_type():
    src = _read("src/api/modules/routes_growth_signals.py")
    assert "doc_type not in (\"rfq\", \"pc\")" in src


def test_response_shape_documented():
    src = _read("src/api/modules/routes_growth_signals.py")
    # Each line returns last_won + scprs nested objects (or null)
    assert "\"last_won\"" in src
    assert "\"scprs\"" in src
    assert "\"line_no\"" in src
    assert "buyer_email" in src


def test_rfq_detail_fetches_signals():
    html = _read("src/templates/rfq_detail.html")
    assert "/api/quote/rfq/" in html
    assert "growth-signals" in html
    assert 'id="growth-signals-strip"' in html
    assert 'data-testid="growth-signals-strip"' in html


def test_pc_detail_fetches_signals():
    html = _read("src/templates/pc_detail.html")
    assert "/api/quote/pc/" in html
    assert "growth-signals" in html
    assert 'id="growth-signals-strip"' in html


def test_panel_paints_only_when_at_least_one_signal_hits():
    """Empty signals on every line → don't paint the strip. No noise."""
    for tpl in ("src/templates/rfq_detail.html", "src/templates/pc_detail.html"):
        html = _read(tpl)
        # The "anyHit" guard is the contract — strip stays display:none
        # when both last_won and scprs are null on every line.
        assert "anyHit" in html, (
            f"{tpl} must short-circuit empty-signal renders"
        )
        # Default visibility is hidden until at least one item paints
        assert "display:none" in html and "growth-signals-strip" in html


def test_dismiss_button_present():
    """User can dismiss the panel mid-quote without losing it forever."""
    for tpl in ("src/templates/rfq_detail.html", "src/templates/pc_detail.html"):
        html = _read(tpl)
        # Hidden by simply setting display:none; reload re-shows.
        assert ('document.getElementById(\'growth-signals-strip\').style.display=\'none\''
                in html)


def test_reuses_existing_last_won_helper():
    """Don't duplicate the buyer-last-won match logic — import it from
    routes_growth_intel where it already lives. Substrate principle."""
    src = _read("src/api/modules/routes_growth_signals.py")
    assert ("from src.api.modules.routes_growth_intel import" in src
            and "_last_won_price_for_buyer" in src)


def test_scprs_lookup_uses_part_number_and_description():
    src = _read("src/api/modules/routes_growth_signals.py")
    assert "_scprs_recent_for_item" in src
    # Both match arms documented at the top of the helper
    assert "part_number" in src
    assert "description" in src.lower()
