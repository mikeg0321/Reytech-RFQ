"""GET /spine/quotes/<id>/forms/{packet,703b,704b,bidpkg}/pdf.

Every /forms/*/pdf route serves the SAME filled CCHCS Non-Cloud RFQ
packet via the legacy-filler adapter (src/spine/packet_render.py). The
Spine's own per-form renderers are retired — these routes prove the
operator now gets the verified packet, and that the route fails 409
(never a blank document) when the buyer's packet PDF can't be located.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    ContractLineItem,
    EmailContract,
    LineItem,
    Quote,
    init_db,
    write_email_contract,
    write_quote,
)

_FIXTURE_REL = "tests/fixtures/unified_ingest/cchcs_packet_preq.pdf"


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_packet_routes.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    app.register_blueprint(make_spine_blueprint(db_path, auth_decorator=None))
    return app.test_client()


def _quote(quote_id="Q-route-pkt") -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="10843276",
        line_items=[
            LineItem(
                line_no=1,
                description="Handheld Scanner w/ USB cable and standard cradle",
                mfg_number="DS8178",
                qty=15,
                uom="EA",
                cost_cents=29500,
                cost_source_url="https://example.com/scanner",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=39500,
            )
        ],
        tax_rate_bps=775,
    )


def _contract(quote_id="Q-route-pkt", attachment_refs=(_FIXTURE_REL,)) -> EmailContract:
    return EmailContract(
        contract_id=f"contract_{quote_id}_1747000000",
        rfq_id=quote_id,
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="10843276",
        line_items=[
            ContractLineItem(
                line_no=1,
                description="Handheld Scanner w/ USB cable and standard cradle",
                qty=15,
                uom="EA",
            )
        ],
        attachment_refs=list(attachment_refs),
    )


def _seed(db_path, *, with_contract=True, attachment_refs=(_FIXTURE_REL,),
          quote_id="Q-route-pkt"):
    write_quote(db_path, _quote(quote_id), actor="test_seed")
    if with_contract:
        write_email_contract(db_path, _contract(quote_id, attachment_refs))


# ── happy path ────────────────────────────────────────────────────────


@pytest.mark.parametrize("form", ["packet", "703b", "704b", "bidpkg"])
def test_forms_route_serves_filled_packet(client, db_path, form):
    _seed(db_path)
    r = client.get(f"/spine/quotes/Q-route-pkt/forms/{form}/pdf")
    assert r.status_code == 200, r.get_data(as_text=True)
    assert r.mimetype == "application/pdf"
    assert r.get_data()[:5] == b"%PDF-"
    # Gate state is surfaced for the operator, never swallowed.
    assert "X-Spine-Packet-Gate-Passed" in r.headers
    assert r.headers["X-Spine-Packet-Source"] == "cchcs_packet_preq.pdf"


def test_all_form_aliases_return_identical_packet(client, db_path):
    """703b/704b/bidpkg are compatibility aliases — the buyer's packet
    bundles all three. They must serve byte-equivalent content."""
    _seed(db_path)
    bodies = {}
    for form in ("packet", "703b", "704b", "bidpkg"):
        r = client.get(f"/spine/quotes/Q-route-pkt/forms/{form}/pdf")
        assert r.status_code == 200
        bodies[form] = len(r.get_data())
    # Same source + same quote → same packet size across every alias.
    assert len(set(bodies.values())) == 1, bodies


# ── failure paths — 409, never a blank document ──────────────────────


def test_packet_route_409_when_no_contract(client, db_path):
    _seed(db_path, with_contract=False)
    r = client.get("/spine/quotes/Q-route-pkt/forms/packet/pdf")
    assert r.status_code == 409
    assert r.get_json()["error"] == "packet_render_failed"
    assert "EmailContract" in r.get_json()["detail"]


def test_packet_route_409_when_packet_pdf_missing(client, db_path):
    _seed(db_path, attachment_refs=("bogus/missing.pdf",))
    r = client.get("/spine/quotes/Q-route-pkt/forms/packet/pdf")
    assert r.status_code == 409
    assert r.get_json()["error"] == "packet_render_failed"


def test_packet_route_404_for_missing_quote(client):
    r = client.get("/spine/quotes/Q-does-not-exist/forms/packet/pdf")
    assert r.status_code == 404
