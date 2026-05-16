"""Tests for the send-prep endpoint.

Send-prep takes a snapshotted quote + a buyer email and produces a
complete Gmail compose envelope (subject + body + compose URL +
downloadable snapshot PDF link). The Spine does not call Gmail
itself — the operator (or parent app) handles the actual send.
This closes the snapshot loop structurally: the bytes that ship to
the buyer ARE the snapshot bytes, never a re-render.

Invariants exercised:
- Precondition: status in (finalized, sent) AND matching snapshot.
- Email validation at the trust boundary.
- Audit event written for every prep call.
- Envelope is deterministic for the same (quote_state, recipient).
- Subject + body contain the totals from the model (not a re-render).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    LineItem, Quote, QuoteStatus,
    init_db, read_event_log, write_quote,
)


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _line(n: int = 1, **kw) -> LineItem:
    base = dict(
        line_no=n, description=f"item {n}", mfg_number=f"M-{n}",
        qty=2, uom="EA", cost_cents=5000,
        cost_source_url="https://supplier.example.com/sku",
        cost_validated_at=_fresh_ts(),
        unit_price_cents=6750,
    )
    base.update(kw)
    return LineItem(**base)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_send.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def _seed_finalized_with_snapshot(client, quote_id: str = "Q-send-001"):
    """Drive a quote from parsed → priced → finalized + snapshot."""
    q1 = Quote(
        quote_id=quote_id, agency="CCHCS", facility="Test facility",
        solicitation_number="SOL-12345",
        line_items=[_line(1), _line(2)],
        tax_rate_bps=825, status=QuoteStatus.PARSED,
    )
    client.post(f"/spine/quotes/{quote_id}/state", json=q1.to_persisted_dict())
    q2 = q1.model_copy(update={"status": QuoteStatus.PRICED})
    client.post(f"/spine/quotes/{quote_id}/state", json=q2.to_persisted_dict())
    q3 = q2.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post(f"/spine/quotes/{quote_id}/state", json=q3.to_persisted_dict())
    snap_r = client.post(f"/spine/quotes/{quote_id}/snapshot")
    assert snap_r.status_code == 200
    return q3, snap_r.json


# ──────────────────────────────────────────────────────────────────────
# Precondition gates
# ──────────────────────────────────────────────────────────────────────


def test_send_prep_404_for_unknown_quote(client):
    r = client.post("/spine/quotes/Q-missing/send-prep", json={"to": "x@y.com"})
    assert r.status_code == 404


def test_send_prep_requires_finalized_or_sent_status(client):
    q = Quote(
        quote_id="Q-pre-001", agency="CCHCS", facility="t",
        solicitation_number="SOL", line_items=[_line(1)],
        tax_rate_bps=775, status=QuoteStatus.PARSED,
    )
    client.post("/spine/quotes/Q-pre-001/state", json=q.to_persisted_dict())
    r = client.post("/spine/quotes/Q-pre-001/send-prep", json={"to": "x@y.com"})
    assert r.status_code == 409
    assert r.json["error"] == "send_precondition_failed"
    assert "finalized" in r.json["detail"]


def test_send_prep_requires_snapshot(client):
    q1 = Quote(
        quote_id="Q-nosnap", agency="CCHCS", facility="t",
        solicitation_number="SOL", line_items=[_line(1)],
        tax_rate_bps=775, status=QuoteStatus.PARSED,
    )
    client.post("/spine/quotes/Q-nosnap/state", json=q1.to_persisted_dict())
    q2 = q1.model_copy(update={"status": QuoteStatus.PRICED})
    client.post("/spine/quotes/Q-nosnap/state", json=q2.to_persisted_dict())
    q3 = q2.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post("/spine/quotes/Q-nosnap/state", json=q3.to_persisted_dict())
    # No snapshot taken — send-prep must fail.
    r = client.post("/spine/quotes/Q-nosnap/send-prep", json={"to": "x@y.com"})
    assert r.status_code == 409
    assert r.json["error"] == "no_snapshot"


def test_send_prep_rejects_when_state_diverged_from_snapshot(client):
    q3, _ = _seed_finalized_with_snapshot(client, "Q-stale-001")
    # Edit unit price after snapshot.
    new_li = q3.line_items[0].model_copy(update={"unit_price_cents": 9999})
    diverged = q3.model_copy(update={
        "line_items": [new_li, *q3.line_items[1:]],
    })
    client.post("/spine/quotes/Q-stale-001/state", json=diverged.to_persisted_dict())
    r = client.post("/spine/quotes/Q-stale-001/send-prep", json={"to": "x@y.com"})
    assert r.status_code == 409
    assert r.json["error"] == "snapshot_stale"


# ──────────────────────────────────────────────────────────────────────
# Recipient validation
# ──────────────────────────────────────────────────────────────────────


def test_send_prep_requires_to(client):
    _seed_finalized_with_snapshot(client, "Q-noto")
    r = client.post("/spine/quotes/Q-noto/send-prep", json={})
    assert r.status_code == 400
    assert r.json["error"] == "missing_recipient"


def test_send_prep_validates_to_format(client):
    _seed_finalized_with_snapshot(client, "Q-badmail")
    r = client.post("/spine/quotes/Q-badmail/send-prep",
                     json={"to": "not-an-email"})
    assert r.status_code == 422
    assert r.json["error"] == "invalid_recipient"


def test_send_prep_validates_cc_format(client):
    _seed_finalized_with_snapshot(client, "Q-badcc")
    r = client.post("/spine/quotes/Q-badcc/send-prep",
                     json={"to": "buyer@agency.gov", "cc": "garbage"})
    assert r.status_code == 422
    assert r.json["error"] == "invalid_cc"


# ──────────────────────────────────────────────────────────────────────
# Envelope shape + content
# ──────────────────────────────────────────────────────────────────────


def test_send_prep_envelope_shape(client):
    q3, snap = _seed_finalized_with_snapshot(client, "Q-env-001")
    r = client.post("/spine/quotes/Q-env-001/send-prep",
                     json={"to": "buyer@cchcs.ca.gov"})
    assert r.status_code == 200
    e = r.json
    required = {
        "snapshot_id", "snapshot_pdf_url", "snapshot_pdf_filename",
        "sha256", "to", "cc", "subject", "body", "gmail_compose_url",
    }
    assert required.issubset(e.keys()), f"missing: {required - e.keys()}"
    assert e["snapshot_id"] == snap["snapshot_id"]
    assert e["sha256"] == snap["sha256"]
    assert e["to"] == ["buyer@cchcs.ca.gov"]
    assert e["cc"] == []
    assert "SOL-12345" in e["subject"]
    assert e["snapshot_pdf_url"].endswith(f"/{snap['snapshot_id']}/pdf")
    assert e["gmail_compose_url"].startswith(
        "https://mail.google.com/mail/?view=cm"
    )


def test_send_prep_body_contains_model_totals(client):
    """The send body must reflect the MODEL math, not a re-render.
    This is what closes the displayed-vs-delivered loop."""
    q3, _ = _seed_finalized_with_snapshot(client, "Q-totals")
    expected_total = f"${q3.total_cents/100:,.2f}"
    expected_subtotal = f"${q3.subtotal_cents/100:,.2f}"
    expected_tax = f"${q3.tax_cents/100:,.2f}"
    r = client.post("/spine/quotes/Q-totals/send-prep",
                     json={"to": "x@agency.gov"})
    assert r.status_code == 200
    body_text = r.json["body"]
    assert expected_total in body_text
    assert expected_subtotal in body_text
    assert expected_tax in body_text
    assert "$0.00" in body_text   # shipping line


def test_send_prep_envelope_is_deterministic(client):
    _seed_finalized_with_snapshot(client, "Q-det")
    r1 = client.post("/spine/quotes/Q-det/send-prep", json={"to": "x@a.com"})
    r2 = client.post("/spine/quotes/Q-det/send-prep", json={"to": "x@a.com"})
    # Subject + body + URLs are deterministic from (state, recipient).
    assert r1.json["subject"] == r2.json["subject"]
    assert r1.json["body"] == r2.json["body"]
    assert r1.json["gmail_compose_url"] == r2.json["gmail_compose_url"]
    assert r1.json["snapshot_id"] == r2.json["snapshot_id"]


def test_send_prep_records_event_log_entry(client, db_path):
    _seed_finalized_with_snapshot(client, "Q-audit")
    events_before = read_event_log(db_path, "Q-audit")
    r = client.post("/spine/quotes/Q-audit/send-prep",
                     json={"to": "buyer@cchcs.ca.gov"},
                     headers={"X-Spine-Actor": "operator-jane"})
    assert r.status_code == 200
    events_after = read_event_log(db_path, "Q-audit")
    assert len(events_after) == len(events_before) + 1
    new_event = events_after[-1]
    assert new_event["actor"] == "operator-jane"
    assert "prepared send envelope" in new_event["note"]
    assert "buyer@cchcs.ca.gov" in new_event["note"]
    # Status unchanged by send-prep.
    assert new_event["status"] == "finalized"


def test_send_prep_cc_flows_into_gmail_url(client):
    _seed_finalized_with_snapshot(client, "Q-cc")
    r = client.post("/spine/quotes/Q-cc/send-prep",
                     json={"to": "buyer@a.gov", "cc": "manager@a.gov"})
    assert r.status_code == 200
    url = r.json["gmail_compose_url"]
    assert "to=buyer%40a.gov" in url
    assert "cc=manager%40a.gov" in url
    assert r.json["cc"] == ["manager@a.gov"]
