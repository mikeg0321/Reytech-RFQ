"""Tests for the CCHCS bid package filler under the matching gate.

The bid package is the third agency-form filler. Identity-only in v1
(CalRecycle 74 + OBS 1600 line-item rows are follow-up). Tests verify:
- Identity fields land on every form section (CUF, Darfur, 105, PD843, STD21)
- Solicitation number renders on multiple sections (sol# is the single
  most-checked field on CCHCS audits)
- Matching gate raises on silent-no-op sabotage (both flat + fillable)
- Route returns valid PDF on 200 + appropriate errors on edge cases
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pdfplumber
import pypdf
import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import LineItem, Quote, QuoteStatus, init_db, write_quote
from src.spine.agency_forms import (
    ReytechIdentity, SpineFormFillError, fill_bidpkg_pdf,
)


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _quote(quote_id: str = "rfq_bidpkg_test", sol: str = "10846581") -> Quote:
    return Quote(
        quote_id=quote_id, agency="CCHCS", facility="Test - CCWF",
        solicitation_number=sol,
        line_items=[LineItem(
            line_no=1,
            description="GLOVES, NITRILE, MEDICAL EXAMINATION GRADE",
            mfg_number="MK-2103L",
            qty=10, uom="BX",
            cost_cents=1000, unit_price_cents=2000,
            cost_source_url="https://example.com",
            cost_validated_at=_fresh_ts(),
        )],
        tax_rate_bps=898,
        status=QuoteStatus.FINALIZED,
    )


def _identity() -> ReytechIdentity:
    return ReytechIdentity(
        business_name="Reytech Inc.",
        address="1 Reytech Way, Irvine, CA 92602",
        contact_person="Michael Greenwald",
        title="President",
        phone="949-229-1575",
        email="rfq@reytechinc.com",
        fein="99-1234567",
        sellers_permit="SR-100-12345",
        cert_number="0012345",
    )


def _extract(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join(p.extract_text() or "" for p in pdf.pages)


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_bidpkg_fills_and_passes_matching_gate():
    pdf_bytes = fill_bidpkg_pdf(_quote(), _identity(), today=datetime(2026, 5, 16))
    assert pdf_bytes.startswith(b"%PDF-")
    txt_no_ws = "".join(_extract(pdf_bytes).split())
    assert "10846581" in txt_no_ws  # sol# on CUF/105/PD843
    assert "ReytechInc" in txt_no_ws
    assert "99-1234567" in txt_no_ws
    assert "0012345" in txt_no_ws


def test_bidpkg_loads_identity_from_env_when_none(monkeypatch):
    monkeypatch.setenv("REYTECH_FEIN", "11-2222222")
    monkeypatch.setenv("REYTECH_CERT_NUMBER", "0099999")
    pdf_bytes = fill_bidpkg_pdf(_quote(), identity=None, today=datetime(2026, 5, 16))
    txt_no_ws = "".join(_extract(pdf_bytes).split())
    assert "11-2222222" in txt_no_ws
    assert "0099999" in txt_no_ws


def test_bidpkg_uses_quote_solicitation_number():
    q = _quote(sol="PREQ10999888")  # PREQ-prefix already stripped by ingest
    pdf_bytes = fill_bidpkg_pdf(q, _identity(), today=datetime(2026, 5, 16))
    txt_no_ws = "".join(_extract(pdf_bytes).split())
    assert "PREQ10999888" in txt_no_ws


def test_bidpkg_renders_owner_title_on_darfur():
    """Darfur Act signature block needs the printed-name-and-title."""
    pdf_bytes = fill_bidpkg_pdf(_quote(), _identity(), today=datetime(2026, 5, 16))
    txt = _extract(pdf_bytes)
    # Owner "Michael Greenwald" and title "President" must both appear.
    assert "Michael Greenwald" in txt or "MichaelGreenwald" in txt.replace(" ", "")
    assert "President" in txt


# ──────────────────────────────────────────────────────────────────────
# Flatten vs fillable
# ──────────────────────────────────────────────────────────────────────


def test_bidpkg_flatten_default_bakes_into_content():
    pdf_bytes = fill_bidpkg_pdf(_quote(), _identity(), today=datetime(2026, 5, 16))
    txt_no_ws = "".join(_extract(pdf_bytes).split())
    assert "10846581" in txt_no_ws
    assert "ReytechInc" in txt_no_ws


def test_bidpkg_fillable_keeps_widgets_with_values_set():
    pdf_bytes = fill_bidpkg_pdf(
        _quote(), _identity(),
        today=datetime(2026, 5, 16),
        flatten=False,
    )
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    # Template has 271 fields per inspection.
    assert len(fields) >= 200, (
        f"fillable mode lost form widgets: only {len(fields)} fields"
    )
    sol_cuf = fields.get("Text7_CUF")
    assert sol_cuf is not None
    assert sol_cuf.get("/V") == "10846581"
    company_cuf = fields.get("DOING BUSINESS AS DBA NAME_CUF")
    assert company_cuf is not None
    assert "Reytech" in str(company_cuf.get("/V"))


# ──────────────────────────────────────────────────────────────────────
# Matching gate — sabotage paths
# ──────────────────────────────────────────────────────────────────────


def test_bidpkg_gate_raises_when_acroform_fill_is_silent_noop(monkeypatch):
    """If pypdf's update_page_form_field_values silently no-ops, the
    field map is built but nothing is written. The gate's pdfplumber
    re-extract must catch missing identity content."""
    import pypdf

    def silent_noop(self, page, field_values, **kw):
        return None
    monkeypatch.setattr(
        pypdf.PdfWriter, "update_page_form_field_values", silent_noop,
    )

    with pytest.raises(SpineFormFillError) as excinfo:
        fill_bidpkg_pdf(_quote(), _identity(), today=datetime(2026, 5, 16))
    assert "bid package fill gate" in str(excinfo.value)


def test_bidpkg_gate_raises_in_fillable_mode_when_fill_noop(monkeypatch):
    import pypdf

    def silent_noop(self, page, field_values, **kw):
        return None
    monkeypatch.setattr(
        pypdf.PdfWriter, "update_page_form_field_values", silent_noop,
    )

    with pytest.raises(SpineFormFillError) as excinfo:
        fill_bidpkg_pdf(
            _quote(), _identity(),
            today=datetime(2026, 5, 16),
            flatten=False,
        )
    assert "fillable" in str(excinfo.value)


# ──────────────────────────────────────────────────────────────────────
# Route integration
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_bidpkg.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def test_route_bidpkg_returns_pdf(client, db_path):
    write_quote(db_path, _quote("Q-rt-bidpkg-001"), actor="seed")
    r = client.get("/spine/quotes/Q-rt-bidpkg-001/forms/bidpkg/pdf")
    assert r.status_code == 200
    assert r.mimetype == "application/pdf"
    assert r.data.startswith(b"%PDF-")
    assert "inline" in r.headers["Content-Disposition"]


def test_route_bidpkg_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/Q-no-such/forms/bidpkg/pdf")
    assert r.status_code == 404


def test_route_bidpkg_fillable_query_param(client, db_path):
    write_quote(db_path, _quote("Q-rt-bidpkg-fill"), actor="seed")
    r = client.get("/spine/quotes/Q-rt-bidpkg-fill/forms/bidpkg/pdf?fillable=1")
    assert r.status_code == 200
    fields = pypdf.PdfReader(io.BytesIO(r.data)).get_fields() or {}
    assert len(fields) >= 200


def test_route_bidpkg_attachment_mode(client, db_path):
    write_quote(db_path, _quote("Q-rt-bidpkg-att"), actor="seed")
    r = client.get("/spine/quotes/Q-rt-bidpkg-att/forms/bidpkg/pdf?inline=0")
    assert r.status_code == 200
    assert "attachment" in r.headers["Content-Disposition"]
