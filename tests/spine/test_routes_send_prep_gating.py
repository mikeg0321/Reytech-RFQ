"""POST /spine/quotes/<id>/send-prep — Inspector gating + form attachments.

Job #1 PR-6 — every send through the 3-quote send gate must carry a
clean InspectorReport, and the envelope must list every form the
operator needs to attach (with ?flatten=1 so the bytes shipped are
non-editable).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    AttachmentDisposition,
    ContractLineItem,
    EmailContract,
    LineItem,
    Quote,
    QuoteStatus,
    init_db,
    latest_snapshot,
    write_email_contract,
    write_quote,
    write_snapshot,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_T703B = "tests/fixtures/703b_blank.pdf"
_T704B = "tests/fixtures/704b_blank.pdf"
_TBIDPKG = "tests/fixtures/cchcs_bidpkg_blank.pdf"
_TPACKET = "tests/fixtures/unified_ingest/cchcs_packet_preq.pdf"

_B_PRESENT = all((_REPO_ROOT / p).is_file() for p in (_T703B, _T704B, _TBIDPKG))
_A_PRESENT = (_REPO_ROOT / _TPACKET).is_file()

_needs_b = pytest.mark.skipif(not _B_PRESENT, reason="Format-B fixtures missing")
_needs_a = pytest.mark.skipif(not _A_PRESENT, reason="Format-A fixture missing")


# ── fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_send_prep.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    app.register_blueprint(make_spine_blueprint(db_path, auth_decorator=None))
    return app.test_client()


def _line(line_no=1, *, qty=5, unit_price_cents=12500, cost_cents=8000,
          validated_at=None):
    return LineItem(
        line_no=line_no, description="Test Item", mfg_number="X-1",
        qty=qty, uom="EA",
        cost_cents=cost_cents,
        cost_source_url="https://example.com/x",
        cost_validated_at=validated_at or datetime.now(timezone.utc),
        unit_price_cents=unit_price_cents,
    )


def _parsed_quote_b(quote_id="Q-sp", sol="10848901", line_items=None):
    """A parsed-status quote — the HTTP seed walks parsed→priced→finalized."""
    return Quote(
        quote_id=quote_id, agency="CCHCS", facility="SAC",
        solicitation_number=sol,
        line_items=line_items or [_line()],
        tax_rate_bps=775, status=QuoteStatus.PARSED,
    )


def _contract_b(quote_id="Q-sp", sol="10848901",
                attachment_refs=(_T703B, _T704B, _TBIDPKG)):
    return EmailContract(
        contract_id=f"contract_{quote_id}_b", rfq_id=quote_id,
        agency="CCHCS", facility="SAC", solicitation_number=sol,
        buyer_name="Grace Pfost", buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        line_items=[ContractLineItem(line_no=1, description="Test Item",
                                      qty=5, uom="EA")],
        attachment_refs=list(attachment_refs),
        # Every declared attachment is accounted for (parsed) so the
        # LAW-6 disposition gate passes and the happy path reaches the
        # Inspector gate / 200 envelope.
        attachment_dispositions=[
            AttachmentDisposition(ref=r, status="parsed")
            for r in attachment_refs
        ],
        response_packaging="separate_pdfs",
    )


def _seed_b(client, db_path, quote=None, contract=None, *, snapshot=True):
    """Seed a Spine quote + contract through the same HTTP route the
    operator uses (POST /state walks parsed→priced→finalized, POST
    /snapshot approves). This is the pattern that keeps the snapshot's
    state_json byte-equal to read_quote().to_persisted_dict() and so
    passes the snapshot-staleness check downstream.
    """
    q = quote or _parsed_quote_b()
    c = contract or _contract_b(quote_id=q.quote_id, sol=q.solicitation_number)
    write_email_contract(db_path, c)
    qid = q.quote_id
    client.post(f"/spine/quotes/{qid}/state", json=q.to_persisted_dict())
    q_priced = q.model_copy(update={"status": QuoteStatus.PRICED})
    client.post(f"/spine/quotes/{qid}/state", json=q_priced.to_persisted_dict())
    q_final = q_priced.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post(f"/spine/quotes/{qid}/state", json=q_final.to_persisted_dict())
    if snapshot:
        snap_r = client.post(f"/spine/quotes/{qid}/snapshot")
        assert snap_r.status_code == 200, snap_r.get_data(as_text=True)
    return q_final, c


def _send_prep(client, quote_id="Q-sp", to="buyer@cdcr.ca.gov"):
    return client.post(
        f"/spine/quotes/{quote_id}/send-prep",
        json={"to": to},
    )


# ── happy path — Format B ────────────────────────────────────────────


@_needs_b
def test_send_prep_envelope_includes_format_b_form_attachments(client, db_path):
    _seed_b(client, db_path)
    r = _send_prep(client)
    assert r.status_code == 200, r.get_data(as_text=True)
    env = r.get_json()
    # PR-6 additions present.
    assert env["inspector_ok"] is True
    assert env["inspector_blocking_count"] == 0
    assert env["response_packaging"] == "separate_pdfs"
    # Three non-quote attachments: 703b + 704b + bidpkg, all flat.
    codes = {a["form_code"] for a in env["form_attachments"]}
    assert codes == {"703b", "704b", "bidpkg"}
    for a in env["form_attachments"]:
        assert "?flatten=1" in a["url"]
        assert a["filename"].endswith(".pdf")
        assert "10848901" in a["filename"]


# ── Inspector gate blocks send ──────────────────────────────────────


@_needs_b
def test_send_prep_blocked_by_inspector_on_missing_form_template(client, db_path):
    """contract.required_forms declares 703b+704b+bidpkg but only the
    703B template is supplied — coverage failures block send-prep
    with the full report attached.

    The contract gives the 703B attachment a recorded disposition so the
    attachment-disposition gate passes and the Inspector gate is reached.
    The test proves that Inspector-only failures (missing rendered forms)
    still surface as inspector_blocked.
    """
    bad_contract = EmailContract(
        contract_id="contract_Q-sp_b",
        rfq_id="Q-sp",
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10848901",
        buyer_name="Grace Pfost",
        buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        line_items=[ContractLineItem(line_no=1, description="Test Item",
                                     qty=5, uom="EA")],
        attachment_refs=[_T703B],          # only 703B — no 704b/bidpkg
        attachment_dispositions=[
            AttachmentDisposition(ref=_T703B, status="parsed"),
        ],
        response_packaging="separate_pdfs",
    )
    _seed_b(client, db_path, contract=bad_contract)
    r = _send_prep(client)
    assert r.status_code == 409
    payload = r.get_json()
    assert payload["error"] == "inspector_blocked"
    assert "report" in payload
    assert any(i["kind"] in ("coverage", "render")
               for i in payload["report"]["issues"])


# ── Format A — packet attachment ─────────────────────────────────────


@_needs_a
def test_send_prep_envelope_packet_for_single_pdf_format(client, db_path):
    q_parsed = Quote(
        quote_id="Q-sp-pkt", agency="CCHCS", facility="CHCF",
        solicitation_number="10843276",
        line_items=[LineItem(
            line_no=1,
            description="Handheld Scanner w/ USB cable and standard cradle",
            mfg_number="DS8178",
            qty=15, uom="EA",
            cost_cents=29500,
            cost_source_url="https://example.com/scanner",
            cost_validated_at=datetime.now(timezone.utc),
            unit_price_cents=39500,
        )],
        tax_rate_bps=775, status=QuoteStatus.PARSED,
    )
    c = EmailContract(
        contract_id="contract_Q-sp-pkt_a", rfq_id="Q-sp-pkt",
        agency="CCHCS", facility="CHCF", solicitation_number="10843276",
        line_items=[ContractLineItem(line_no=1, description="Handheld Scanner",
                                      qty=15, uom="EA")],
        attachment_refs=[_TPACKET],
        attachment_dispositions=[
            AttachmentDisposition(ref=_TPACKET, status="parsed"),
        ],
        response_packaging="single_pdf",
    )
    _seed_b(client, db_path, quote=q_parsed, contract=c)
    r = _send_prep(client, quote_id="Q-sp-pkt")
    assert r.status_code == 200, r.get_data(as_text=True)
    env = r.get_json()
    assert env["response_packaging"] == "single_pdf"
    codes = [a["form_code"] for a in env["form_attachments"]]
    assert codes == ["packet"]
    assert "?flatten=1" in env["form_attachments"][0]["url"]


# ── pre-existing gates still block correctly ─────────────────────────


def test_send_prep_still_blocks_when_quote_not_finalized(client, db_path):
    """The existing finalized-or-sent gate must still fire BEFORE the
    Inspector check (the Inspector requires finalized state preconditions)."""
    q_parsed = Quote(
        quote_id="Q-sp-parsed", agency="CCHCS", facility="SAC",
        solicitation_number="10848901",
        line_items=[LineItem(
            line_no=1, description="X", qty=1, uom="EA",
            cost_cents=100, unit_price_cents=200,
        )],
        tax_rate_bps=0,    # not priced — status is parsed
    )
    write_quote(db_path, q_parsed, actor="t")
    r = client.post("/spine/quotes/Q-sp-parsed/send-prep",
                    json={"to": "x@x.com"})
    assert r.status_code == 409
    payload = r.get_json()
    assert payload["error"] == "send_precondition_failed"
