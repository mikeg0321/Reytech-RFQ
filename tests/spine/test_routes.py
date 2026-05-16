"""The Spine — HTTP route tests.

Uses make_spine_blueprint() to mount the Spine routes on an isolated
Flask app with auth bypassed. The legacy dashboard.py + shared.bp are
NOT imported — these tests prove the Spine's transport surface works
in isolation, the same property we get at the model + DB layer.
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pdfplumber
import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    LineItem,
    Quote,
    QuoteStatus,
    init_db,
    read_event_log,
    read_quote,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _ok_line(line_no: int = 1, **overrides) -> LineItem:
    base = dict(
        line_no=line_no,
        description=f"item {line_no}",
        mfg_number=f"MFG-{line_no:03d}",
        qty=2,
        uom="EA",
        cost_cents=5000,
        cost_source_url="https://supplier.example.com/sku",
        cost_validated_at=_fresh_ts(),
        unit_price_cents=6750,
    )
    base.update(overrides)
    return LineItem(**base)


def _ok_quote(
    quote_id: str = "Q-route-001",
    *,
    status: QuoteStatus = QuoteStatus.PARSED,
    line_items: list[LineItem] | None = None,
    tax_rate_bps: int = 825,
) -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=line_items or [_ok_line(1), _ok_line(2)],
        tax_rate_bps=tax_rate_bps,
        status=status,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_routes.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    # auth_decorator=None — bypass auth for tests. Prod wiring passes
    # auth_required; tests prove route logic in isolation.
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


# ──────────────────────────────────────────────────────────────────────
# GET — basic shape + 404
# ──────────────────────────────────────────────────────────────────────


def test_get_missing_quote_returns_404(client):
    r = client.get("/spine/quotes/Q-missing")
    assert r.status_code == 404
    assert r.json["error"] == "not_found"


def test_post_then_get_round_trip(client):
    q = _ok_quote("Q-rt-001")
    r = client.post(
        "/spine/quotes/Q-rt-001/state",
        json=q.to_persisted_dict(),
    )
    assert r.status_code == 200, r.json
    posted = r.json
    assert posted["quote_id"] == "Q-rt-001"
    assert posted["status"] == "parsed"

    # GET returns the same persisted state.
    r2 = client.get("/spine/quotes/Q-rt-001")
    assert r2.status_code == 200
    fetched = r2.json
    assert fetched["quote_id"] == "Q-rt-001"
    assert fetched["agency"] == "CCHCS"
    assert len(fetched["line_items"]) == 2
    # Computed fields must NOT be in the persisted/transported dict.
    assert "subtotal_cents" not in fetched
    assert "tax_cents" not in fetched
    assert "total_cents" not in fetched
    for li in fetched["line_items"]:
        assert "extension_cents" not in li
        assert "markup_pct_display" not in li


# ──────────────────────────────────────────────────────────────────────
# POST validation
# ──────────────────────────────────────────────────────────────────────


def test_post_non_json_body_returns_400(client):
    r = client.post("/spine/quotes/Q-bad/state", data="not json")
    assert r.status_code == 400
    assert r.json["error"] == "bad_body"


def test_post_quote_id_mismatch_returns_400(client):
    q = _ok_quote("Q-correct-001")
    r = client.post(
        "/spine/quotes/Q-different-id/state",
        json=q.to_persisted_dict(),
    )
    assert r.status_code == 400
    assert r.json["error"] == "quote_id_mismatch"


def test_post_unknown_field_returns_422(client):
    """extra='forbid' bubbles up as 422 — the structural defense.

    Closes the persistence-P0 class at the API boundary: any client
    that POSTs a legacy alias field (bid_price, shipping_option, etc.)
    gets rejected with a Pydantic error message pointing at the field.
    """
    q = _ok_quote("Q-extra-001")
    body = q.to_persisted_dict()
    body["bid_price"] = 9999  # banned alias.
    r = client.post("/spine/quotes/Q-extra-001/state", json=body)
    assert r.status_code == 422
    assert r.json["error"] == "validation_failed"
    assert "bid_price" in r.json["detail"]


def test_post_shipping_option_returns_422(client):
    q = _ok_quote("Q-ship-001")
    body = q.to_persisted_dict()
    body["shipping_option"] = "included"  # the 5/15 tax-zeroing field.
    r = client.post("/spine/quotes/Q-ship-001/state", json=body)
    assert r.status_code == 422


def test_post_priced_status_requires_tax_rate_bps(client):
    q = _ok_quote("Q-tax-001", tax_rate_bps=0, status=QuoteStatus.PARSED)
    body = q.to_persisted_dict()
    body["status"] = "priced"  # try to advance via raw POST.
    r = client.post("/spine/quotes/Q-tax-001/state", json=body)
    assert r.status_code == 422
    assert "tax_rate_bps" in r.json["detail"]


# ──────────────────────────────────────────────────────────────────────
# POST state-machine semantics
# ──────────────────────────────────────────────────────────────────────


def test_post_to_sent_quote_returns_409(client):
    """Sent is terminal; any subsequent POST returns 409 (not 4xx generic)."""
    q = _ok_quote("Q-terminal-001", status=QuoteStatus.SENT,
                   line_items=[_ok_line(1)])
    r1 = client.post("/spine/quotes/Q-terminal-001/state",
                      json=q.to_persisted_dict())
    assert r1.status_code == 200

    r2 = client.post("/spine/quotes/Q-terminal-001/state",
                      json=q.to_persisted_dict())
    assert r2.status_code == 409
    assert r2.json["error"] == "state_transition_rejected"


def test_multiple_posts_append_to_event_log(client, db_path):
    """The whole flow: parse → price → finalize → sent. 4 events recorded."""
    q1 = _ok_quote("Q-flow-001", status=QuoteStatus.PARSED)
    r1 = client.post("/spine/quotes/Q-flow-001/state",
                      json=q1.to_persisted_dict(),
                      headers={"X-Spine-Actor": "ingest",
                               "X-Spine-Note": "initial parse"})
    assert r1.status_code == 200

    q2 = q1.model_copy(update={"status": QuoteStatus.PRICED})
    r2 = client.post("/spine/quotes/Q-flow-001/state",
                      json=q2.to_persisted_dict(),
                      headers={"X-Spine-Actor": "operator",
                               "X-Spine-Note": "priced at 35% markup"})
    assert r2.status_code == 200

    q3 = q2.model_copy(update={"status": QuoteStatus.FINALIZED})
    r3 = client.post("/spine/quotes/Q-flow-001/state",
                      json=q3.to_persisted_dict(),
                      headers={"X-Spine-Actor": "operator"})
    assert r3.status_code == 200

    q4 = q3.model_copy(update={"status": QuoteStatus.SENT})
    r4 = client.post("/spine/quotes/Q-flow-001/state",
                      json=q4.to_persisted_dict(),
                      headers={"X-Spine-Actor": "operator",
                               "X-Spine-Note": "sent to mohammed"})
    assert r4.status_code == 200

    # Event log shows all 4 stages with the actor + note metadata.
    events_r = client.get("/spine/quotes/Q-flow-001/events")
    assert events_r.status_code == 200
    events = events_r.json["events"]
    assert len(events) == 4
    assert [e["status"] for e in events] == ["parsed", "priced", "finalized", "sent"]
    assert events[0]["actor"] == "ingest"
    assert events[0]["note"] == "initial parse"
    assert events[3]["actor"] == "operator"
    assert events[3]["note"] == "sent to mohammed"


# ──────────────────────────────────────────────────────────────────────
# GET PDF — Day-3 gate exposed over HTTP
# ──────────────────────────────────────────────────────────────────────


def test_get_pdf_returns_pdf_bytes(client):
    q = _ok_quote("Q-pdf-001",
                   status=QuoteStatus.PRICED,
                   line_items=[_ok_line(1)])
    r1 = client.post("/spine/quotes/Q-pdf-001/state",
                      json=q.to_persisted_dict())
    assert r1.status_code == 200

    r2 = client.get("/spine/quotes/Q-pdf-001/pdf")
    assert r2.status_code == 200
    assert r2.mimetype == "application/pdf"
    assert r2.data.startswith(b"%PDF-")
    assert "inline" in r2.headers["Content-Disposition"]


def test_get_pdf_attachment_mode(client):
    q = _ok_quote("Q-pdf-002",
                   status=QuoteStatus.PRICED,
                   line_items=[_ok_line(1)])
    client.post("/spine/quotes/Q-pdf-002/state", json=q.to_persisted_dict())

    r = client.get("/spine/quotes/Q-pdf-002/pdf?inline=0")
    assert r.status_code == 200
    assert "attachment" in r.headers["Content-Disposition"]


def test_get_pdf_for_missing_quote_returns_404(client):
    r = client.get("/spine/quotes/Q-no-such-thing/pdf")
    assert r.status_code == 404


def test_pdf_renders_correct_totals_via_http(client):
    """Day-3 gate, exposed end-to-end over HTTP:
    POST a 9e63456e-shaped quote, GET its PDF, assert manifest math.
    """
    items = [
        _ok_line(1, qty=10, unit_price_cents=5000),
        _ok_line(2, qty=25, unit_price_cents=3500),
        _ok_line(3, qty=5, unit_price_cents=18000),
        _ok_line(4, qty=50, unit_price_cents=750),
        _ok_line(5, qty=20, unit_price_cents=4500),
        LineItem(
            line_no=6,
            description="LABELS, BLANK, CIRCLE, 3/4\" DIA, BLUE",
            mfg_number="2555",
            qty=1000,
            uom="PAC",
            cost_cents=2085,
            cost_source_url="https://supplier.example.com/labels/2555",
            cost_validated_at=_fresh_ts(),
            unit_price_cents=2815,
        ),
        _ok_line(7, qty=540, unit_price_cents=2803),
    ]
    q = Quote(
        quote_id="9e63456e-via-http",
        agency="CCHCS",
        facility="SATF Corcoran 93212",
        solicitation_number="10847262",
        line_items=items,
        tax_rate_bps=825,
        status=QuoteStatus.PRICED,
    )
    r1 = client.post("/spine/quotes/9e63456e-via-http/state",
                      json=q.to_persisted_dict())
    assert r1.status_code == 200

    r2 = client.get("/spine/quotes/9e63456e-via-http/pdf")
    assert r2.status_code == 200

    with pdfplumber.open(io.BytesIO(r2.data)) as pdf:
        text = "\n".join(p.extract_text() for p in pdf.pages if p.extract_text())

    assert "$46,836.20" in text
    assert "$3,863.99" in text
    assert "$50,700.19" in text


# ──────────────────────────────────────────────────────────────────────
# Single-writer invariant — no fan-out from one POST
# ──────────────────────────────────────────────────────────────────────


def test_one_post_writes_exactly_one_db_row(client, db_path):
    """One POST = one event_log entry = one row mutation. No fan-out."""
    q = _ok_quote("Q-single-001")
    r = client.post("/spine/quotes/Q-single-001/state",
                     json=q.to_persisted_dict())
    assert r.status_code == 200

    events = read_event_log(db_path, "Q-single-001")
    assert len(events) == 1
    assert events[0]["status"] == "parsed"

    # Re-read via DB directly — same state as just POSTed.
    persisted = read_quote(db_path, "Q-single-001")
    assert persisted is not None
    assert persisted.quote_id == "Q-single-001"
    assert len(persisted.line_items) == 2


def test_partial_post_does_not_mutate_existing_state(client, db_path):
    """A POST with a malformed body must not touch the persisted row.

    Replays the 'partial-write' failure class at the route boundary:
    pre-Spine, autosave could update one field and leave siblings
    stale. The Spine route either persists the FULL valid state or
    rejects with a 4xx — no in-between.
    """
    q = _ok_quote("Q-no-partial-001")
    client.post("/spine/quotes/Q-no-partial-001/state",
                 json=q.to_persisted_dict())
    before = read_quote(db_path, "Q-no-partial-001")
    assert before is not None
    before_dump = before.to_persisted_dict()

    # Bad POST — should fail with 422 because of the alias field.
    bad_body = before_dump.copy()
    bad_body["bid_price"] = 99999
    r = client.post("/spine/quotes/Q-no-partial-001/state", json=bad_body)
    assert r.status_code == 422

    after = read_quote(db_path, "Q-no-partial-001")
    assert after is not None
    # The stored state must be unchanged.
    assert after.to_persisted_dict() == before_dump
