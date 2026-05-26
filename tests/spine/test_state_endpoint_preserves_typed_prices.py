"""PR #5 regression pin — operator-typed unit_price is sticky.

Closes Handoff A PR-2 (Mike's 88565c03 incident 2026-05-25): a user
typed $2.70 + $1,439.75 in the legacy editor, hit a tier action, and
the system recomputed `price = cost × (1 + markup/100)` and overwrote
both typed prices with $3.13 + $1,406.25 (the uniform 25% markup
default). The Handoff called for a `last_touched_field` plumbing fix
on LineItem to make the operator's typed value sticky.

Spine substrate ALREADY does the right thing — verified empirically by
Window 2 on 2026-05-25 (rfq_89bb9a3e Duffey: typed cost=$35 + price=
$43.75 persisted as cost_cents=3500 + unit_price_cents=4375 — exact
preservation). The substrate fix lives in TWO places:

  1. Client (`src/templates/spine_pc_detail.html:543-575`): typing in
     unit_price recomputes markup *for display*, NOT the other way —
     the typed price is preserved in the form. Typing in cost or
     markup recomputes unit_price (operator's explicit intent).

  2. Server (`src/spine/model.py` + `src/spine/db.py::write_quote`):
     no field-derivation on write. cost_cents and unit_price_cents are
     persisted as-typed. The server's `_COMPUTED_FIELD_NAMES` set
     excludes them from any post-write recompute.

This test pins the server-side half: POST a Quote payload to
`/spine/quotes/<id>/state` with cost_cents=1000 + unit_price_cents=
2000 — explicitly 100% markup, NOT the 25% default. Round-trip via GET
and assert the persisted unit_price is the typed 2000, NOT 1250 (which
would be cost × 1.25). A regression that re-introduces server-side
recompute will fail this assertion.

The client-side stickiness is pinned by source-grep tests in
test_edit_ui.py (the JS listener structure must stay intact).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import LineItem, Quote, QuoteStatus, init_db, write_quote, read_quote


def _ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=1)


def _quote_with_unconventional_markup(quote_id: str = "Q-sticky-001") -> Quote:
    """Build a Quote where the unit_price is NOT cost × 1.25 — explicitly
    100% markup. If a future regression introduces a server-side
    `price = cost × (1 + default_markup)` stomp, line 1's price will
    flip from 2000 to 1250 (cost 1000 × 1.25) and the assertion fails.
    """
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="Test — CSP-SAC",
        solicitation_number="10847187",
        line_items=[
            LineItem(
                line_no=1,
                description="OPERATOR-TYPED 100pct MARKUP",
                mfg_number="STICKY-1",
                qty=2, uom="EA",
                cost_cents=1000,        # $10.00 cost
                unit_price_cents=2000,  # $20.00 typed — 100% markup, NOT 25%
                cost_source_url="https://supplier.example.com/sticky-1",
                cost_validated_at=_ts(),
            ),
            LineItem(
                line_no=2,
                description="OPERATOR-TYPED 8pct MARKUP (Mike's 88565c03 shape)",
                mfg_number="STICKY-2",
                qty=2, uom="EA",
                cost_cents=143975,       # $1,439.75 — Mike's actual incident value
                unit_price_cents=143975, # $1,439.75 — same; 0% markup (operator override)
                cost_source_url="https://supplier.example.com/sticky-2",
                cost_validated_at=_ts(),
            ),
        ],
        tax_rate_bps=775,
        status=QuoteStatus.PARSED,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_sticky.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client_with_seeded(db_path) -> FlaskClient:
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
    write_quote(db_path, _quote_with_unconventional_markup(), actor="test_seeder")
    return app.test_client(), db_path


def test_state_post_preserves_operator_typed_unit_price(client_with_seeded):
    """POST cost=$10, unit_price=$20 (100% markup) — server must persist
    unit_price_cents=2000 verbatim. A server-side `price = cost × 1.25`
    stomp would persist 1250 instead, failing this assertion."""
    client, db_path = client_with_seeded
    # Pull the canonical state, then POST it back to /state — this
    # mimics the save round-trip the editor performs.
    r_get = client.get("/spine/quotes/Q-sticky-001",
                       headers={"Accept": "application/json"})
    assert r_get.status_code == 200
    body = r_get.get_json()
    # Sanity: seeded values came through
    assert body["line_items"][0]["cost_cents"] == 1000
    assert body["line_items"][0]["unit_price_cents"] == 2000
    assert body["line_items"][1]["cost_cents"] == 143975
    assert body["line_items"][1]["unit_price_cents"] == 143975

    # POST the unchanged state — this is what Save does when operator
    # hasn't edited (just clicked Save to no-op). The server must NOT
    # recompute the unit_price from cost × default_markup.
    r_post = client.post(
        "/spine/quotes/Q-sticky-001/state",
        data=json.dumps(body),
        headers={"Content-Type": "application/json"},
    )
    assert r_post.status_code == 200, f"POST failed: {r_post.data!r}"

    # Round-trip read confirms persistence
    persisted = read_quote(db_path, "Q-sticky-001")
    assert persisted is not None
    li1, li2 = persisted.line_items[0], persisted.line_items[1]
    assert li1.cost_cents == 1000, "cost stomped"
    assert li1.unit_price_cents == 2000, (
        f"Operator-typed unit_price NOT sticky — persisted as "
        f"{li1.unit_price_cents} (expected 2000). Server-side "
        f"recompute regression: a cost × default_markup formula was "
        f"likely re-introduced. See Handoff A PR-2 (Mike's 88565c03 "
        f"incident 2026-05-25)."
    )
    assert li2.cost_cents == 143975, "cost stomped"
    assert li2.unit_price_cents == 143975, (
        f"Operator-typed unit_price NOT sticky on line 2 — persisted as "
        f"{li2.unit_price_cents} (expected 143975 — operator's chosen "
        f"0% markup override). A cost × 1.25 stomp would produce "
        f"179969 (cost 143975 × 1.25 rounded). Regression."
    )


def test_state_post_with_modified_unit_price_persists_typed_value(client_with_seeded):
    """Mike types a NEW unit_price (different from the seeded value);
    save MUST persist the typed value, not recompute from cost.

    The editor's gatherFormState() reads input.value directly — so the
    POST body carries the typed number. This test mirrors that path by
    mutating the GET body before POSTing."""
    client, db_path = client_with_seeded
    r_get = client.get("/spine/quotes/Q-sticky-001",
                       headers={"Accept": "application/json"})
    body = r_get.get_json()
    # Operator types unit_price = $7.77 (cost stays $10 → 22.3% markup
    # — explicitly NOT 25%, the cost × 1.25 stomp would land at 1250).
    body["line_items"][0]["unit_price_cents"] = 777

    r_post = client.post(
        "/spine/quotes/Q-sticky-001/state",
        data=json.dumps(body),
        headers={"Content-Type": "application/json"},
    )
    assert r_post.status_code == 200, f"POST failed: {r_post.data!r}"

    persisted = read_quote(db_path, "Q-sticky-001")
    assert persisted.line_items[0].unit_price_cents == 777, (
        f"Typed unit_price=777 not preserved — persisted as "
        f"{persisted.line_items[0].unit_price_cents}. If 1250: server "
        f"applied cost × 1.25 stomp (regression). If 2000: server "
        f"ignored the POST body and reused prior state (also regression)."
    )
    # cost stays untouched
    assert persisted.line_items[0].cost_cents == 1000


def test_state_post_with_modified_cost_does_not_recompute_unit_price(
    client_with_seeded
):
    """Mike types a new COST without touching unit_price — server MUST
    persist new cost + ORIGINAL unit_price untouched. The display layer
    (client JS) recomputes markup-for-display from the new (cost,
    price) pair, but unit_price ITSELF stays as previously persisted."""
    client, db_path = client_with_seeded
    r_get = client.get("/spine/quotes/Q-sticky-001",
                       headers={"Accept": "application/json"})
    body = r_get.get_json()
    # Operator types cost = $12.50 (was $10.00). Leaves unit_price at $20.
    body["line_items"][0]["cost_cents"] = 1250

    r_post = client.post(
        "/spine/quotes/Q-sticky-001/state",
        data=json.dumps(body),
        headers={"Content-Type": "application/json"},
    )
    assert r_post.status_code == 200, f"POST failed: {r_post.data!r}"

    persisted = read_quote(db_path, "Q-sticky-001")
    assert persisted.line_items[0].cost_cents == 1250, "new cost not persisted"
    assert persisted.line_items[0].unit_price_cents == 2000, (
        f"Server recomputed unit_price after a cost change — should "
        f"stay 2000, persisted as {persisted.line_items[0].unit_price_cents}. "
        f"A cost × 1.25 stomp would produce 1562 (1250 × 1.25). "
        f"Regression of Handoff A PR-2."
    )
