"""Smoke tests for the Spine operator editor at /spine/quotes/<id>/edit.

These tests verify the route serves the template without UndefinedError,
the rendered HTML contains the right structural pieces, and the
single-POST-per-save invariant is visibly enforced (no autosave JS
hooked to onChange/onInput).

This is the substitute for Chrome-MCP verification in a headless
test run; a real browser walkthrough is still the prod-merge gate
per CLAUDE.md's hard rule.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import LineItem, Quote, QuoteStatus, init_db, write_quote


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _quote(quote_id="Q-edit-001", *, status=QuoteStatus.PARSED) -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="Test - CCWF Chowchilla",
        solicitation_number="10847262",
        line_items=[
            LineItem(
                line_no=1,
                description="GLOVES, EXAM, NITRILE, LARGE, 100/BOX",
                mfg_number="MK-2103L",
                qty=10, uom="BX",
                cost_cents=3500,
                cost_source_url="https://supplier.example.com/sku/MK-2103L",
                cost_validated_at=_fresh_ts(),
                unit_price_cents=5000,
            ),
            LineItem(
                line_no=2,
                description="MASKS, SURGICAL, EAR LOOP, 50/BOX",
                mfg_number="PRM-1820",
                qty=12, uom="BX",
                cost_cents=8250,
                cost_source_url="https://supplier.example.com/sku/PRM-1820",
                cost_validated_at=_fresh_ts(),
                unit_price_cents=11000,
            ),
        ],
        tax_rate_bps=825,
        status=status,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_edit.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client_with_seeded(db_path) -> FlaskClient:
    """Flask app + a 'parsed' quote pre-seeded in the Spine DB."""
    # The template lookup needs to find src/templates/spine_pc_detail.html.
    # Point Flask at the project's template directory.
    import os
    template_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "src", "templates",
    )
    app = Flask(__name__, template_folder=template_dir)
    app.testing = True
    # Minimal context-processor: base.html references csrf_token_value.
    @app.context_processor
    def _ctx():
        return {"csrf_token_value": "test-csrf"}

    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)

    write_quote(db_path, _quote(), actor="test_seeder", note="seed for edit UI test")
    return app.test_client()


# ──────────────────────────────────────────────────────────────────────
# Smoke: page renders without error.
# ──────────────────────────────────────────────────────────────────────


def test_edit_page_renders_200(client_with_seeded):
    r = client_with_seeded.get("/spine/quotes/Q-edit-001/edit")
    assert r.status_code == 200
    assert r.mimetype == "text/html"


def test_edit_page_renders_quote_id_in_title(client_with_seeded):
    """Editor surface shows the buyer-facing display_number (assigned at
    write_quote time post-PR #1040) and ALSO the internal quote_id —
    operator needs both: display for buyer comms, quote_id for the URL.
    """
    r = client_with_seeded.get("/spine/quotes/Q-edit-001/edit")
    text = r.data.decode("utf-8")
    # Internal quote_id still present (URLs / debug surfaces / data attrs)
    assert "Q-edit-001" in text
    # And the buyer-facing R{yy}Q#### appears in the title.
    import re
    assert re.search(r"<title>Spine — R\d{2}Q\d{4}</title>", text)


def test_edit_page_404_for_missing_quote(client_with_seeded):
    r = client_with_seeded.get("/spine/quotes/Q-nonexistent/edit")
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# Structural content
# ──────────────────────────────────────────────────────────────────────


def test_edit_page_shows_status_pill(client_with_seeded):
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    assert 'spine-status-pill spine-status-parsed' in text
    assert ">parsed</span>" in text


def test_edit_page_shows_kpi_block_with_correct_totals(client_with_seeded):
    """Subtotal $1,820 + 8.25% tax $150.15 = total $1,970.15.

    qty 10 × $50 = $500. qty 12 × $110 = $1,320. Sum $1,820.
    Tax 1820 × 825 / 10000 = 150.15 (banker's rounded).
    """
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    assert "$1,820.00" in text
    assert "$150.15" in text
    assert "$1,970.15" in text
    assert "8.25%" in text
    assert "Shipping" in text or "SHIPPING" in text
    assert "$0.00" in text  # shipping line always present


def test_edit_page_lists_every_line_item(client_with_seeded):
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    assert "MK-2103L" in text
    assert "PRM-1820" in text
    assert "GLOVES" in text
    assert "MASKS" in text


def test_edit_page_has_editable_cost_price_url_inputs(client_with_seeded):
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    # 2 line items × 3 editable fields (cost, src, price) = 6 input names.
    for line_no in (1, 2):
        for field in ("cost", "src", "price"):
            assert f'name="{field}-{line_no}"' in text


# ──────────────────────────────────────────────────────────────────────
# Single-POST invariant — no per-keystroke autosave hooks.
# ──────────────────────────────────────────────────────────────────────


def test_no_autosave_hooks_in_spine_template():
    """The Spine template MUST NOT have onkeystroke autosave hooks.

    Searches the template file directly (not the rendered HTML —
    base.html has its own autosave machinery for legacy pages, which
    is fine; we're enforcing that the Spine template itself doesn't
    introduce any). If any of these tokens appear in spine_pc_detail.html,
    the operator UI has regressed to per-keystroke fan-out.
    """
    import os
    template_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "src", "templates", "spine_pc_detail.html",
    )
    text = open(template_path, encoding="utf-8").read()
    # The W-U-009 invariant is "no per-keystroke POST to the server."
    # Client-side recompute listeners (markup ↔ unit_price two-way
    # binding, KPI strip live updates) are allowed and expected —
    # they don't hit the network. The audit logic lives in
    # tests/spine/_template_audit.py so multiple future tests can
    # reuse it without copy-paste drift.
    from tests.spine._template_audit import (
        find_banned_literals,
        find_keystroke_network_calls,
    )
    found_literals = find_banned_literals(text)
    assert not found_literals, (
        "spine_pc_detail.html contains autosave / per-keystroke hooks. "
        f"The Spine requires single-POST-per-save. Found: {found_literals!r}"
    )
    keystroke_offenses = find_keystroke_network_calls(text)
    assert not keystroke_offenses, (
        "per-keystroke listener appears to fire a network call: "
        + "; ".join(keystroke_offenses[:3])
    )


def test_template_js_preserves_cost_validated_at_when_cost_unchanged(client_with_seeded):
    """Regression: 2026-05-15 live-walk caught the template auto-stamping
    cost_validated_at = new Date() on EVERY save. That created phantom
    divergence between the latest snapshot and a no-op post-snapshot Save
    triggered by the Mark Sent button's first phase. The substrate
    correctly 409'd because the timestamps diverged — but the operator
    workflow couldn't complete (snapshot → Mark Sent → stuck).

    The fix gates the restamp on costChanged. This test scans the
    rendered template's JS for the gate and the comment that names
    the bug so future edits can't quietly remove it.
    """
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    # The conditional restamp must be present.
    assert "costChanged" in text, (
        "spine_pc_detail.html JS must gate cost_validated_at restamp "
        "on whether cost actually changed. Auto-stamping on every save "
        "creates phantom snapshot divergence (caught 2026-05-15 live)."
    )
    # And it must reference li.cost_validated_at as the fallback path —
    # otherwise the gate exists but doesn't preserve the original
    # timestamp on no-op saves.
    assert "li.cost_validated_at" in text


def test_save_button_targets_post_state_endpoint(client_with_seeded):
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    # The JS POSTs to /spine/quotes/<id>/state via fetch.
    assert "/spine/quotes/" in text
    assert "/state" in text
    assert "method: 'POST'" in text


# ──────────────────────────────────────────────────────────────────────
# Status transition buttons match current status.
# ──────────────────────────────────────────────────────────────────────


def test_parsed_status_shows_only_mark_priced_button(client_with_seeded):
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    assert 'data-transition="priced"' in text
    assert 'data-transition="finalized"' not in text
    assert 'data-transition="sent"' not in text


def test_priced_status_shows_finalize_and_reopen_buttons(db_path, tmp_path):
    import os
    template_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "src", "templates",
    )
    app = Flask(__name__, template_folder=template_dir)
    app.testing = True
    @app.context_processor
    def _ctx():
        return {"csrf_token_value": "test-csrf"}
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)

    # Seed a priced quote.
    write_quote(db_path, _quote(quote_id="Q-priced", status=QuoteStatus.PRICED),
                actor="seed")

    text = app.test_client().get("/spine/quotes/Q-priced/edit").data.decode("utf-8")
    assert 'data-transition="finalized"' in text
    assert 'data-transition="parsed"' in text  # reopen
    assert 'data-transition="priced"' not in text
    assert 'data-transition="sent"' not in text


# ──────────────────────────────────────────────────────────────────────
# Footer surfaces (PDF link, event log link)
# ──────────────────────────────────────────────────────────────────────


def test_edit_page_has_pdf_and_event_log_links(client_with_seeded):
    text = client_with_seeded.get("/spine/quotes/Q-edit-001/edit").data.decode("utf-8")
    assert '/spine/quotes/Q-edit-001/pdf' in text
    assert '/spine/quotes/Q-edit-001/events' in text
