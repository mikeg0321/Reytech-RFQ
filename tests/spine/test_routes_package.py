"""GET /spine/quotes/<id>/package — output-vs-contract gate.

Closes 5/15 finding #7 (generated package has standalone forms not in
agency contract) structurally. The endpoint refuses 409 whenever the
rendered form set would diverge from the contract's required_forms.
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


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_package.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def _make_quote(quote_id="Q-pkg-001") -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10847457",
        line_items=[
            LineItem(
                line_no=1,
                description="N95 mask, box of 20",
                qty=50, uom="BX",
                cost_cents=500, cost_source_url="https://uline.com/n95",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=750,
            )
        ],
        tax_rate_bps=875,
    )


def _make_contract(
    quote_id="Q-pkg-001",
    required_forms=None,
) -> EmailContract:
    return EmailContract(
        contract_id=f"contract_{quote_id}_1747000000",
        rfq_id=quote_id,
        source_email_id="msgSAC",
        source_thread_id="threadSAC",
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10847457",
        line_items=[
            ContractLineItem(line_no=1, description="N95 mask, box of 20",
                             qty=50, uom="BX"),
        ],
        required_forms=required_forms if required_forms is not None
                       else ["703b", "704b", "bidpkg", "quote"],
    )


# ── Happy path ───────────────────────────────────────────────────────


def test_package_returns_required_forms_with_urls(client, db_path):
    q = _make_quote()
    c = _make_contract()
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 200
    body = r.get_json()
    assert body["quote_id"] == q.quote_id
    assert body["contract_id"] == c.contract_id
    assert body["required_forms"] == ["703b", "704b", "bidpkg", "quote"]
    assert body["response_packaging"] == "separate_pdfs"
    # Every file points at the corresponding per-form route.
    codes = [f["form_code"] for f in body["files"]]
    assert codes == ["703b", "704b", "bidpkg", "quote"]
    for f in body["files"]:
        assert f["url"].startswith(f"/spine/quotes/{q.quote_id}/")
        assert f["filename"].endswith(".pdf")


# ── Gate: no contract → 409 ──────────────────────────────────────────


def test_package_returns_409_when_no_contract_bound(client, db_path):
    q = _make_quote(quote_id="Q-no-contract")
    write_quote(db_path, q, actor="test_seed")
    # NO write_email_contract call.

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 409
    body = r.get_json()
    assert body["error"] == "no_contract"


# ── Gate: contract requires a form with no registered renderer → 409 ─


def test_package_returns_409_when_renderer_missing(client, db_path):
    q = _make_quote(quote_id="Q-missing-renderer")
    # Contract demands `std_204` — known-deferred, not in FORM_REGISTRY.
    c = _make_contract(
        quote_id=q.quote_id,
        required_forms=["703b", "704b", "bidpkg", "quote", "std_204"],
    )
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 409
    body = r.get_json()
    assert body["error"] == "renderer_missing"
    assert "std_204" in body["missing"]
    assert "703b" in body["registered_forms"]
    assert "std_204" in body["required_forms"]


# ── Gate: contract requires multiple missing forms → all surfaced ────


def test_package_surfaces_all_missing_renderers(client, db_path):
    q = _make_quote(quote_id="Q-multi-missing")
    c = _make_contract(
        quote_id=q.quote_id,
        # Three known-deferred forms in one contract.
        required_forms=["703b", "quote", "std_204", "darfur", "cuf"],
    )
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 409
    body = r.get_json()
    assert body["error"] == "renderer_missing"
    assert set(body["missing"]) == {"std_204", "darfur", "cuf"}


# ── Quote not found → 404 ────────────────────────────────────────────


def test_package_returns_404_for_missing_quote(client):
    r = client.get("/spine/quotes/Q-does-not-exist/package")
    assert r.status_code == 404


# ── Default CCHCS set ships clean ────────────────────────────────────


def test_package_default_cchcs_contract_passes_gate(client, db_path):
    """The empirical CCHCS four-pack must always pass.

    If this fails, no CCHCS bid can ship via the Spine — production gate.
    """
    q = _make_quote(quote_id="Q-cchcs-default")
    c = _make_contract(quote_id=q.quote_id)  # uses CCHCS_DEFAULT_REQUIRED_FORMS
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 200
    body = r.get_json()
    assert len(body["files"]) == 4


# ── Contract order is preserved → operator UI consistency ────────────


def test_package_preserves_contract_form_order(client, db_path):
    q = _make_quote(quote_id="Q-order")
    # Buyer-specified order: 704b first, then 703b, etc.
    c = _make_contract(
        quote_id=q.quote_id,
        required_forms=["704b", "703b", "quote", "bidpkg"],
    )
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 200
    body = r.get_json()
    codes = [f["form_code"] for f in body["files"]]
    assert codes == ["704b", "703b", "quote", "bidpkg"]
