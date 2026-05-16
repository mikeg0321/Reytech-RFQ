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


def test_post_rejects_illegal_transition_parsed_to_finalized(client):
    """W-S-006 — POST must consult _ALLOWED_TRANSITIONS at the trust boundary.

    Regression for the CHROME_WALKTHROUGH_GATE 2026-05-15 finding: the
    Spine model's state machine (parsed→priced→finalized→sent) is
    enforced by Quote.with_status(), but the POST /state route called
    Quote.model_validate(body) directly and never invoked with_status().
    A caller could short-circuit parsed→finalized in one POST if cost,
    source URL, and tax were already valid for the finalized
    preconditions. That is the exact substrate failure class the gate
    was built to prevent.
    """
    parsed = _ok_quote("Q-skip-001", status=QuoteStatus.PARSED,
                        line_items=[_ok_line(1)])
    r1 = client.post("/spine/quotes/Q-skip-001/state",
                      json=parsed.to_persisted_dict())
    assert r1.status_code == 200

    # Try parsed → finalized directly. The new line item is fully
    # valid for finalized (cost source + fresh validated_at), so the
    # model-validate stage will accept it. Only the transition check
    # can reject this.
    skip_body = parsed.to_persisted_dict()
    skip_body["status"] = "finalized"
    r2 = client.post("/spine/quotes/Q-skip-001/state", json=skip_body)
    assert r2.status_code == 409, (
        f"parsed→finalized must be rejected as illegal transition; "
        f"got {r2.status_code} {r2.json!r}"
    )
    assert r2.json["error"] == "state_transition_rejected"
    assert "illegal transition" in r2.json["detail"].lower()
    assert "priced" in r2.json["detail"]


def test_post_rejects_illegal_transition_parsed_to_sent(client):
    """W-S-006 sibling — parsed→sent in one POST must be 409."""
    parsed = _ok_quote("Q-skip-002", status=QuoteStatus.PARSED,
                        line_items=[_ok_line(1)])
    client.post("/spine/quotes/Q-skip-002/state",
                json=parsed.to_persisted_dict())

    skip_body = parsed.to_persisted_dict()
    skip_body["status"] = "sent"
    r = client.post("/spine/quotes/Q-skip-002/state", json=skip_body)
    assert r.status_code == 409
    assert r.json["error"] == "state_transition_rejected"


def test_post_allows_legal_reopen_priced_to_parsed(client):
    """W-S-007 regression — make sure the new gate doesn't block legal reopens."""
    parsed = _ok_quote("Q-reopen-001", status=QuoteStatus.PARSED,
                       line_items=[_ok_line(1)])
    client.post("/spine/quotes/Q-reopen-001/state",
                json=parsed.to_persisted_dict())

    priced_body = parsed.to_persisted_dict()
    priced_body["status"] = "priced"
    r = client.post("/spine/quotes/Q-reopen-001/state", json=priced_body)
    assert r.status_code == 200

    reopened_body = priced_body.copy()
    reopened_body["status"] = "parsed"
    r2 = client.post("/spine/quotes/Q-reopen-001/state", json=reopened_body)
    assert r2.status_code == 200, (
        f"priced→parsed reopen must be allowed; got {r2.status_code} {r2.json!r}"
    )


def test_multiple_posts_append_to_event_log(client, db_path):
    """The whole flow: parse → price → finalize → snapshot → sent.
    4 state events + 1 snapshot recorded.

    The snapshot step is required at the finalized→sent boundary by
    the route precondition added 2026-05-15. See W-S-009 in the
    walkthrough catalog.
    """
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

    # Snapshot the finalized state. The bytes captured here ARE what
    # ships to the buyer.
    snap_r = client.post("/spine/quotes/Q-flow-001/snapshot",
                          headers={"X-Spine-Actor": "operator",
                                   "X-Spine-Note": "approved for ship"})
    assert snap_r.status_code == 200, snap_r.json
    assert snap_r.json["snapshot_id"].startswith("snap_Q-flow-001_")

    q4 = q3.model_copy(update={"status": QuoteStatus.SENT})
    r4 = client.post("/spine/quotes/Q-flow-001/state",
                      json=q4.to_persisted_dict(),
                      headers={"X-Spine-Actor": "operator",
                               "X-Spine-Note": "sent to mohammed"})
    assert r4.status_code == 200, r4.json

    # Event log shows all 4 state stages with the actor + note metadata.
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
# Snapshot endpoints (W-Q-013/014, W-S-009)
# ──────────────────────────────────────────────────────────────────────


def test_snapshot_endpoint_requires_finalized_status(client):
    """A parsed or priced quote cannot be snapshotted."""
    q = _ok_quote("Q-snap-prec", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-snap-prec/state", json=q.to_persisted_dict())
    r = client.post("/spine/quotes/Q-snap-prec/snapshot")
    assert r.status_code == 409
    assert r.json["error"] == "snapshot_precondition_failed"
    assert "finalized" in r.json["detail"]


def test_snapshot_endpoint_succeeds_on_finalized(client):
    q = _ok_quote("Q-snap-ok", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-snap-ok/state", json=q.to_persisted_dict())
    p = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-snap-ok/state", json=p.to_persisted_dict())
    f = p.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-snap-ok/state", json=f.to_persisted_dict())

    r = client.post("/spine/quotes/Q-snap-ok/snapshot",
                     headers={"X-Spine-Actor": "operator",
                              "X-Spine-Note": "ship it"})
    assert r.status_code == 200
    body = r.json
    assert body["snapshot_id"].startswith("snap_Q-snap-ok_")
    assert len(body["sha256"]) == 64
    assert body["byte_len"] > 500


def test_snapshot_endpoint_idempotent_on_repeat_click(client):
    q = _ok_quote("Q-snap-idem", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-snap-idem/state", json=q.to_persisted_dict())
    p = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-snap-idem/state", json=p.to_persisted_dict())
    f = p.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-snap-idem/state", json=f.to_persisted_dict())

    r1 = client.post("/spine/quotes/Q-snap-idem/snapshot")
    r2 = client.post("/spine/quotes/Q-snap-idem/snapshot")
    assert r1.json["snapshot_id"] == r2.json["snapshot_id"]
    list_r = client.get("/spine/quotes/Q-snap-idem/snapshots")
    assert len(list_r.json["snapshots"]) == 1


def test_send_without_snapshot_rejected(client):
    """W-S-009: finalized→sent without any snapshot returns 409.
    This is the architectural enforcement of 'the bytes shipped to
    the buyer are the bytes the operator approved.'
    """
    q = _ok_quote("Q-no-snap", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-no-snap/state", json=q.to_persisted_dict())
    p = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-no-snap/state", json=p.to_persisted_dict())
    f = p.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-no-snap/state", json=f.to_persisted_dict())

    sent = f.model_copy(update={"status": QuoteStatus.SENT})
    r = client.post("/spine/quotes/Q-no-snap/state", json=sent.to_persisted_dict())
    assert r.status_code == 409
    assert r.json["error"] == "state_transition_rejected"
    assert "snapshot" in r.json["detail"].lower()


def test_send_with_diverged_state_rejected(client):
    """Operator snapshots, then edits a value, then tries to send.
    The send must fail because the state has diverged from what was
    approved. Operator must re-snapshot to commit to the new state.
    """
    q = _ok_quote("Q-diverge", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-diverge/state", json=q.to_persisted_dict())
    p = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-diverge/state", json=p.to_persisted_dict())
    f = p.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-diverge/state", json=f.to_persisted_dict())
    snap_r = client.post("/spine/quotes/Q-diverge/snapshot")
    assert snap_r.status_code == 200

    # Edit a value AFTER snapshotting.
    diverged_li = f.line_items[0].model_copy(update={"unit_price_cents": 99_999})
    diverged = f.model_copy(update={"line_items": [diverged_li, *f.line_items[1:]]})
    save_r = client.post("/spine/quotes/Q-diverge/state",
                          json=diverged.to_persisted_dict())
    assert save_r.status_code == 200

    # Try to send.
    sent_diverged = diverged.model_copy(update={"status": QuoteStatus.SENT})
    r = client.post("/spine/quotes/Q-diverge/state",
                     json=sent_diverged.to_persisted_dict())
    assert r.status_code == 409
    assert "diverged" in r.json["detail"].lower()
    assert snap_r.json["snapshot_id"] in r.json["detail"]


def test_send_after_resnapshot_succeeds(client):
    """Operator edits, re-snapshots, sends. The flow is allowed."""
    q = _ok_quote("Q-resnap", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-resnap/state", json=q.to_persisted_dict())
    p = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-resnap/state", json=p.to_persisted_dict())
    f = p.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-resnap/state", json=f.to_persisted_dict())
    client.post("/spine/quotes/Q-resnap/snapshot")

    # Edit and re-snapshot.
    new_li = f.line_items[0].model_copy(update={"unit_price_cents": 80_000})
    edited = f.model_copy(update={"line_items": [new_li, *f.line_items[1:]]})
    client.post("/spine/quotes/Q-resnap/state", json=edited.to_persisted_dict())
    r2 = client.post("/spine/quotes/Q-resnap/snapshot")
    assert r2.status_code == 200

    sent = edited.model_copy(update={"status": QuoteStatus.SENT})
    rs = client.post("/spine/quotes/Q-resnap/state",
                      json=sent.to_persisted_dict())
    assert rs.status_code == 200


def test_get_snapshot_pdf_returns_immutable_bytes(client):
    q = _ok_quote("Q-snap-pdf", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-snap-pdf/state", json=q.to_persisted_dict())
    p = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-snap-pdf/state", json=p.to_persisted_dict())
    f = p.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-snap-pdf/state", json=f.to_persisted_dict())
    sr = client.post("/spine/quotes/Q-snap-pdf/snapshot")
    sid = sr.json["snapshot_id"]
    sha = sr.json["sha256"]

    r = client.get(f"/spine/quotes/Q-snap-pdf/snapshot/{sid}/pdf")
    assert r.status_code == 200
    assert r.mimetype == "application/pdf"
    assert r.data.startswith(b"%PDF-")
    assert r.headers["X-Spine-Snapshot-Sha256"] == sha
    import hashlib
    assert hashlib.sha256(r.data).hexdigest() == sha


def test_get_snapshot_pdf_scope_check(client):
    """Snapshot ID must belong to the URL's quote_id (defense in depth
    against caller confusion / cross-quote leakage)."""
    q1 = _ok_quote("Q-scope-a", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-scope-a/state", json=q1.to_persisted_dict())
    p1 = q1.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-scope-a/state", json=p1.to_persisted_dict())
    f1 = p1.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-scope-a/state", json=f1.to_persisted_dict())
    sr = client.post("/spine/quotes/Q-scope-a/snapshot")
    sid_a = sr.json["snapshot_id"]

    q2 = _ok_quote("Q-scope-b", status=QuoteStatus.PARSED)
    client.post("/spine/quotes/Q-scope-b/state", json=q2.to_persisted_dict())

    # Try to fetch Q-scope-a's snapshot under Q-scope-b's URL.
    r = client.get(f"/spine/quotes/Q-scope-b/snapshot/{sid_a}/pdf")
    assert r.status_code == 400
    assert r.json["error"] == "snapshot_scope_mismatch"


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
