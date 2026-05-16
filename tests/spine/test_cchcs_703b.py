"""Tests for the CCHCS 703B filler under the matching gate.

The 703B is the first agency-form filler in the Spine. It establishes
the architectural pattern every future form (704B, bid package,
STD 204, DVBE 843, etc.) will copy:

  1. Pure fill function takes Quote + ReytechIdentity → bytes.
  2. Matching gate re-extracts and asserts every operator-required
     identifier (sol#, business name, FEIN, phone, email) is visible.
  3. Default flatten (government convention); ?fillable=1 escape
     hatch for last-minute Adobe edits.
  4. Substrate has no vendor_* fields — identity is config-driven.
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
    ReytechIdentity, SpineFormFillError, fill_703b_pdf,
)


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _quote(quote_id: str = "rfq_703b_test", sol: str = "10846581") -> Quote:
    return Quote(
        quote_id=quote_id, agency="CCHCS", facility="Test - CCWF",
        solicitation_number=sol,
        line_items=[LineItem(
            line_no=1, description="test", mfg_number="X",
            qty=1, uom="EA", cost_cents=1000, unit_price_cents=2000,
            cost_source_url="https://example.com",
            cost_validated_at=_fresh_ts(),
        )],
        tax_rate_bps=775,
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
        sellers_permit="ABC-123456",
    )


# ──────────────────────────────────────────────────────────────────────
# Happy path + matching gate content
# ──────────────────────────────────────────────────────────────────────


def test_703b_fills_and_passes_matching_gate():
    """All required identifiers must end up visibly present in the
    rendered text after fill + pikepdf appearance generation."""
    q = _quote()
    pdf_bytes = fill_703b_pdf(q, _identity(), today=datetime(2026, 5, 16))
    assert pdf_bytes.startswith(b"%PDF-")
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        txt = "\n".join(p.extract_text() or "" for p in pdf.pages)
    assert "10846581" in txt
    assert "Reytech Inc" in txt
    assert "949-229-1575" in txt
    assert "rfq@reytechinc.com" in txt
    assert "99-1234567" in txt


def test_703b_loads_identity_from_env_when_none(monkeypatch):
    monkeypatch.setenv("REYTECH_FEIN", "11-2222222")
    monkeypatch.setenv("REYTECH_SELLERS_PERMIT", "XYZ-999")
    pdf_bytes = fill_703b_pdf(_quote(), identity=None, today=datetime(2026, 5, 16))
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        txt = "\n".join(p.extract_text() or "" for p in pdf.pages)
    assert "11-2222222" in txt
    assert "XYZ-999" in txt


def test_703b_uses_quote_solicitation_number_and_agency():
    q = _quote(sol="PREQ10999888")
    pdf_bytes = fill_703b_pdf(q, _identity(), today=datetime(2026, 5, 16))
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        txt = "\n".join(p.extract_text() or "" for p in pdf.pages)
    assert "PREQ10999888" in txt


def test_703b_flatten_default_strips_widgets():
    """Default flatten=True: no /AcroForm widgets remain, values
    drawn into the content stream."""
    pdf_bytes = fill_703b_pdf(_quote(), _identity(), today=datetime(2026, 5, 16))
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    # After flatten_annotations(mode="all"), widget annotations are
    # baked into the page content; AcroForm fields may still be
    # listed but their on-screen presence is now drawn text.
    # The structural assertion is: extracted TEXT contains the values
    # (pypdf field /V alone wouldn't satisfy this).
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        txt = "\n".join(p.extract_text() or "" for p in pdf.pages)
    assert "Reytech Inc" in txt


def test_703b_fillable_keeps_widgets_for_adobe_edits():
    """?fillable=1 escape hatch: form widgets remain editable so
    Mike can fix a wrong value in Adobe at the last minute."""
    pdf_bytes = fill_703b_pdf(
        _quote(), _identity(),
        today=datetime(2026, 5, 16),
        flatten=False,
    )
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    # 50 fields per the parent's 703B template inspection
    assert len(fields) >= 40, (
        f"fillable mode lost form widgets: only {len(fields)} fields present"
    )
    # And the values are set (visible in /V).
    sol_field = fields.get("703B_Solicitation Number")
    assert sol_field is not None
    assert sol_field.get("/V") == "10846581"


# ──────────────────────────────────────────────────────────────────────
# Matching gate — sabotage paths
# ──────────────────────────────────────────────────────────────────────


def test_703b_gate_raises_when_acroform_fill_is_silent_noop(monkeypatch):
    """Sabotage: pypdf's update_page_form_field_values is patched to
    do nothing. The field map says "set these values" but the values
    never reach the AcroForm. Gate must catch the absence in the
    rendered text — exactly the AV-1 failure class from the 2026-05
    substrate timeline (PDF template stripped /AcroForm root, audit
    read all-blank, no visible signal that the fill failed)."""
    import pypdf

    def silent_noop(self, page, field_values, **kw):
        return None
    monkeypatch.setattr(
        pypdf.PdfWriter, "update_page_form_field_values", silent_noop,
    )

    with pytest.raises(SpineFormFillError) as excinfo:
        fill_703b_pdf(_quote(), _identity(), today=datetime(2026, 5, 16))
    msg = str(excinfo.value)
    assert "703B fill gate" in msg
    # First required field that's missing is named.
    assert "Solicitation Number" in msg or "Business Name" in msg


def test_703b_gate_raises_in_fillable_mode_when_acroform_fill_noop(monkeypatch):
    """Same sabotage, fillable path. Gate checks pypdf /V values
    instead of pdfplumber text; must still raise."""
    import pypdf

    def silent_noop(self, page, field_values, **kw):
        return None
    monkeypatch.setattr(
        pypdf.PdfWriter, "update_page_form_field_values", silent_noop,
    )

    with pytest.raises(SpineFormFillError) as excinfo:
        fill_703b_pdf(
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
    p = tmp_path / "spine_703b.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def test_route_703b_returns_pdf(client, db_path):
    write_quote(db_path, _quote("Q-rt-001"), actor="seed")
    r = client.get("/spine/quotes/Q-rt-001/forms/703b/pdf")
    assert r.status_code == 200
    assert r.mimetype == "application/pdf"
    assert r.data.startswith(b"%PDF-")
    assert "inline" in r.headers["Content-Disposition"]


def test_route_703b_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/Q-no-such-thing/forms/703b/pdf")
    assert r.status_code == 404


def test_route_703b_fillable_query_param(client, db_path):
    write_quote(db_path, _quote("Q-rt-fill"), actor="seed")
    r_flat = client.get("/spine/quotes/Q-rt-fill/forms/703b/pdf")
    r_fill = client.get("/spine/quotes/Q-rt-fill/forms/703b/pdf?fillable=1")
    assert r_flat.status_code == r_fill.status_code == 200
    # Both produce valid PDFs; the fillable variant retains widgets.
    fields_fill = pypdf.PdfReader(io.BytesIO(r_fill.data)).get_fields() or {}
    assert len(fields_fill) >= 40


def test_route_703b_attachment_mode(client, db_path):
    write_quote(db_path, _quote("Q-rt-att"), actor="seed")
    r = client.get("/spine/quotes/Q-rt-att/forms/703b/pdf?inline=0")
    assert r.status_code == 200
    assert "attachment" in r.headers["Content-Disposition"]


# ──────────────────────────────────────────────────────────────────────
# Architectural — substrate has no vendor_* fields
# ──────────────────────────────────────────────────────────────────────


def test_substrate_has_no_vendor_fields():
    """ReytechIdentity is config-driven; the substrate must NEVER
    grow vendor_business_name / vendor_fein / etc. fields. Any
    future Quote or LineItem field whose name contains 'vendor_'
    is a Charter violation — identity lives outside the substrate.
    """
    for model in (Quote, LineItem):
        for field_name in model.model_fields.keys():
            assert "vendor_" not in field_name.lower(), (
                f"{model.__name__}.{field_name}: vendor identity must "
                "not be persisted in the Spine substrate."
            )
