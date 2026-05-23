"""GET /spine/quotes/<id>/inspector — the Inspector-report route.

PR-5: the route the operator UI polls before send, and that the
PR-6 send-prep gating will call to enforce a clean report.

Plus the ``?flatten=1`` query-param coverage on /forms/*/pdf — the
buyer-bound download is non-editable, no password required.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient
from pypdf import PdfReader

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
    p = tmp_path / "spine_inspector_routes.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    app.register_blueprint(make_spine_blueprint(db_path, auth_decorator=None))
    return app.test_client()


def _quote_b(quote_id="Q-insp-route", sol="10848901"):
    return Quote(
        quote_id=quote_id, agency="CCHCS", facility="SAC",
        solicitation_number=sol,
        line_items=[LineItem(
            line_no=1, description="Test Item", mfg_number="X-1",
            qty=5, uom="EA",
            cost_cents=8000,
            cost_source_url="https://example.com/x",
            cost_validated_at=datetime.now(timezone.utc),
            unit_price_cents=12500,
        )],
        tax_rate_bps=775,
    )


def _contract_b(quote_id="Q-insp-route", sol="10848901",
                 attachment_refs=(_T703B, _T704B, _TBIDPKG)):
    return EmailContract(
        contract_id=f"contract_{quote_id}_b", rfq_id=quote_id,
        agency="CCHCS", facility="SAC", solicitation_number=sol,
        buyer_name="Grace Pfost", buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        line_items=[ContractLineItem(line_no=1, description="Test Item",
                                      qty=5, uom="EA")],
        attachment_refs=list(attachment_refs),
        response_packaging="separate_pdfs",
    )


def _quote_a(quote_id="Q-pkt-route"):
    return Quote(
        quote_id=quote_id, agency="CCHCS", facility="CHCF",
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
        tax_rate_bps=775,
    )


def _contract_a(quote_id="Q-pkt-route"):
    return EmailContract(
        contract_id=f"contract_{quote_id}_a", rfq_id=quote_id,
        agency="CCHCS", facility="CHCF", solicitation_number="10843276",
        line_items=[ContractLineItem(line_no=1, description="Handheld Scanner",
                                      qty=15, uom="EA")],
        attachment_refs=[_TPACKET],
        response_packaging="single_pdf",
    )


# ── inspector route ──────────────────────────────────────────────────


def test_inspector_route_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/Q-nope/inspector")
    assert r.status_code == 404


def test_inspector_route_blocks_when_no_contract(client, db_path):
    """No contract bound → report blocks immediately, route still 200
    (the report itself carries the verdict)."""
    write_quote(db_path, _quote_b(), actor="t")
    r = client.get("/spine/quotes/Q-insp-route/inspector")
    assert r.status_code == 200
    payload = r.get_json()
    assert payload["ok"] is False
    assert payload["blocking_count"] >= 1
    assert any(i["kind"] == "render" for i in payload["issues"])


@_needs_b
def test_inspector_route_returns_clean_report_for_happy_format_b(client, db_path):
    write_quote(db_path, _quote_b(), actor="t")
    write_email_contract(db_path, _contract_b())
    r = client.get("/spine/quotes/Q-insp-route/inspector")
    assert r.status_code == 200
    payload = r.get_json()
    assert payload["ok"] is True, payload["issues"]
    assert payload["response_packaging"] == "separate_pdfs"
    assert "704b" in payload["forms_checked"]
    assert payload["line_items_checked"] >= 1


@_needs_a
def test_inspector_route_returns_clean_report_for_happy_format_a(client, db_path):
    write_quote(db_path, _quote_a(), actor="t")
    write_email_contract(db_path, _contract_a())
    r = client.get("/spine/quotes/Q-pkt-route/inspector")
    assert r.status_code == 200
    payload = r.get_json()
    assert payload["ok"] is True, payload["issues"]
    assert payload["response_packaging"] == "single_pdf"


# ── ?flatten=1 query coverage ─────────────────────────────────────────


@_needs_b
def test_forms_route_flatten_query_strips_form_fields(client, db_path):
    """Without ?flatten=1 the 704B has form fields; with it, zero."""
    write_quote(db_path, _quote_b(), actor="t")
    write_email_contract(db_path, _contract_b())
    # Editable (default)
    r1 = client.get("/spine/quotes/Q-insp-route/forms/704b/pdf")
    assert r1.status_code == 200
    assert r1.headers.get("X-Spine-Flattened") == "0"
    n1 = len(PdfReader(io.BytesIO(r1.get_data())).get_fields() or {})
    assert n1 > 0
    # Flat
    r2 = client.get("/spine/quotes/Q-insp-route/forms/704b/pdf?flatten=1")
    assert r2.status_code == 200
    assert r2.headers.get("X-Spine-Flattened") == "1"
    n2 = len(PdfReader(io.BytesIO(r2.get_data())).get_fields() or {})
    assert n2 == 0


@_needs_a
def test_packet_route_flatten_query_strips_form_fields(client, db_path):
    write_quote(db_path, _quote_a(), actor="t")
    write_email_contract(db_path, _contract_a())
    r = client.get("/spine/quotes/Q-pkt-route/forms/packet/pdf?flatten=1")
    assert r.status_code == 200
    assert r.headers.get("X-Spine-Flattened") == "1"
    n = len(PdfReader(io.BytesIO(r.get_data())).get_fields() or {})
    assert n == 0
