"""GET /spine/quotes/<id>/package — output-vs-contract gate.

Closes 5/15 finding #7 (generated package has standalone forms not in
agency contract) structurally. The endpoint refuses 409 whenever the
rendered form set would diverge from the contract's required_forms.
"""
from __future__ import annotations

import importlib.util
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

_HAS_FITZ = importlib.util.find_spec("fitz") is not None
_needs_fitz = pytest.mark.skipif(
    not _HAS_FITZ,
    reason="PyMuPDF (fitz) not installed — flatten degrades to no-op in prod",
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
#
# All FormCode literals are now registered in FORM_REGISTRY, so we
# must monkeypatch FORM_REGISTRY to exercise the 409 renderer_missing
# path. Remove "std_204" temporarily so the gate fires correctly.


def test_package_returns_409_when_renderer_missing(client, db_path, monkeypatch):
    import src.spine.agency_forms as _af

    q = _make_quote(quote_id="Q-missing-renderer")
    c = _make_contract(
        quote_id=q.quote_id,
        required_forms=["703b", "704b", "bidpkg", "quote", "std_204"],
    )
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    # Temporarily remove std_204 from FORM_REGISTRY so the gate fires.
    patched = {k: v for k, v in _af.FORM_REGISTRY.items() if k != "std_204"}
    monkeypatch.setattr(_af, "FORM_REGISTRY", patched)

    r = client.get(f"/spine/quotes/{q.quote_id}/package")
    assert r.status_code == 409
    body = r.get_json()
    assert body["error"] == "renderer_missing"
    assert "std_204" in body["missing"]
    assert "703b" in body["registered_forms"]
    assert "std_204" in body["required_forms"]


# ── Gate: contract requires multiple missing forms → all surfaced ────


def test_package_surfaces_all_missing_renderers(client, db_path, monkeypatch):
    import src.spine.agency_forms as _af

    q = _make_quote(quote_id="Q-multi-missing")
    c = _make_contract(
        quote_id=q.quote_id,
        required_forms=["703b", "quote", "std_204", "darfur", "cuf"],
    )
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    # Remove the three forms to exercise the multi-missing gate.
    patched = {
        k: v for k, v in _af.FORM_REGISTRY.items()
        if k not in {"std_204", "darfur", "cuf"}
    }
    monkeypatch.setattr(_af, "FORM_REGISTRY", patched)

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


# ── Newly-wired per-form routes — positive HTTP coverage ─────────────
#
# Each of the six Pillar-4 forms now has a /forms/<code>/pdf route.
# These tests confirm:
#   (a) /package returns HTTP 200 with the form in its URL list, and
#   (b) the per-form route itself returns HTTP 200 with valid PDF bytes.
# The renderer calls are real (they hit the legacy fillers + templates),
# which is the same pattern used by test_std_204.py / test_dvbe_843_and
# _darfur.py / etc. to verify that each renderer produces valid PDF.


@pytest.mark.parametrize("form_code", [
    "std_204",
    "dvbe_843",
    "darfur",
    "calrecycle_74",
    "cuf",
    "std_1000",
])
def test_newly_wired_form_appears_in_package_url_list(client, db_path, form_code):
    """/package returns 200 and maps the form to a per-form URL."""
    qid = f"Q-newroute-{form_code}"
    q = _make_quote(quote_id=qid)
    c = _make_contract(
        quote_id=qid,
        required_forms=["quote", form_code],
    )
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{qid}/package")
    assert r.status_code == 200, r.get_data(as_text=True)
    body = r.get_json()
    urls = {f["form_code"]: f["url"] for f in body["files"]}
    assert form_code in urls
    expected_url = f"/spine/quotes/{qid}/forms/{form_code}/pdf"
    assert urls[form_code] == expected_url


@pytest.mark.parametrize("form_code", [
    "std_204",
    "dvbe_843",
    "darfur",
    "calrecycle_74",
    "cuf",
    "std_1000",
])
def test_newly_wired_per_form_route_returns_pdf_bytes(client, db_path, form_code):
    """GET /forms/<code>/pdf returns 200 with valid PDF bytes."""
    qid = f"Q-formroute-{form_code}"
    q = _make_quote(quote_id=qid)
    c = _make_contract(quote_id=qid, required_forms=["quote", form_code])
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{qid}/forms/{form_code}/pdf")
    assert r.status_code == 200, (
        f"expected 200, got {r.status_code}: {r.get_data(as_text=True)[:300]}"
    )
    assert r.content_type == "application/pdf"
    data = r.data
    assert data[:5] == b"%PDF-", f"not a PDF header: {data[:8]!r}"
    assert len(data) > 1024, "PDF suspiciously small"


@_needs_fitz
@pytest.mark.parametrize("form_code", [
    "std_204",
    "dvbe_843",
    "darfur",
    "calrecycle_74",
    "cuf",
    "std_1000",
])
def test_newly_wired_per_form_route_flatten_strips_fields(
    client, db_path, form_code
):
    """?flatten=1 bakes AcroForm widgets into static content (field count→0).

    Skipped when PyMuPDF (fitz) is absent — the flatten primitive degrades
    gracefully to a no-op in that environment (see src/spine/flatten.py).
    """
    import io
    from pypdf import PdfReader

    qid = f"Q-flatten-{form_code}"
    q = _make_quote(quote_id=qid)
    c = _make_contract(quote_id=qid, required_forms=["quote", form_code])
    write_quote(db_path, q, actor="test_seed")
    write_email_contract(db_path, c)

    r = client.get(f"/spine/quotes/{qid}/forms/{form_code}/pdf?flatten=1")
    assert r.status_code == 200, r.get_data(as_text=True)[:300]
    data = r.data
    assert data[:5] == b"%PDF-"
    # After flatten the AcroForm widget count should be zero.
    field_count = len(PdfReader(io.BytesIO(data)).get_fields() or {})
    assert field_count == 0, (
        f"{form_code} ?flatten=1 still has {field_count} form field(s)"
    )
