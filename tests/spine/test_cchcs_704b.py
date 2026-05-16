"""Tests for the CCHCS 704B filler under the matching gate.

The 704B is the second agency-form filler. Same architectural pattern
as 703B (see test_cchcs_703b.py) but exercises line-item rendering
across the 39-row capacity of the template (23 page-1 rows + 16
page-2 rows). Bounds tested: 1, 8, 23 (page-1 fill), 24 (first
page-2 row), 39 (full), 40 (must raise).
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
    ReytechIdentity, SpineFormFillError, fill_704b_pdf,
)


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _items(n: int) -> list[LineItem]:
    return [
        LineItem(
            line_no=i,
            description=f"Test product {i} ZZUNIQUE{i:03d}",
            mfg_number=f"MFG-{i:03d}",
            qty=10 + i,
            uom="EA",
            cost_cents=799 + i * 11,
            unit_price_cents=1099 + i * 11,
            cost_source_url="https://example.com",
            cost_validated_at=_fresh_ts(),
        )
        for i in range(1, n + 1)
    ]


def _quote(quote_id: str = "rfq_704b_test", *, n_items: int = 8,
           sol: str = "10846581") -> Quote:
    return Quote(
        quote_id=quote_id, agency="CCHCS", facility="Test - CCWF",
        solicitation_number=sol,
        line_items=_items(n_items),
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
        sellers_permit="ABC-123456",
    )


def _extract(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join(p.extract_text() or "" for p in pdf.pages)


# ──────────────────────────────────────────────────────────────────────
# Happy path + identity + per-line presence
# ──────────────────────────────────────────────────────────────────────


def test_704b_fills_header_and_passes_matching_gate():
    q = _quote(n_items=8)
    pdf_bytes = fill_704b_pdf(q, _identity(), today=datetime(2026, 5, 16))
    assert pdf_bytes.startswith(b"%PDF-")
    txt = _extract(pdf_bytes)
    assert "10846581" in txt
    assert "Reytech Inc" in txt
    assert "CCHCS" in txt


def test_704b_renders_every_line_description():
    pdf_bytes = fill_704b_pdf(_quote(n_items=8), _identity(),
                              today=datetime(2026, 5, 16))
    txt_no_ws = "".join(_extract(pdf_bytes).split())
    for i in range(1, 9):
        assert f"ZZUNIQUE{i:03d}" in txt_no_ws, f"line {i} description missing"


def test_704b_renders_every_subtotal():
    """Every line's extension (cents) must appear in rendered text."""
    q = _quote(n_items=5)
    pdf_bytes = fill_704b_pdf(q, _identity(), today=datetime(2026, 5, 16))
    txt = _extract(pdf_bytes).replace(",", "")
    for li in q.line_items:
        ext = li.extension_cents
        whole, frac = divmod(ext, 100)
        sub = f"{whole}.{frac:02d}"
        assert sub in txt, f"line {li.line_no} subtotal {sub} missing"


# ──────────────────────────────────────────────────────────────────────
# Row capacity boundaries
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("n", [1, 8, 23, 24, 39])
def test_704b_fills_at_capacity_boundary(n: int):
    q = _quote(quote_id=f"q-cap-{n}", n_items=n)
    pdf_bytes = fill_704b_pdf(q, _identity(), today=datetime(2026, 5, 16))
    txt = "".join(_extract(pdf_bytes).split())
    # First and last lines must both reach the page.
    assert f"ZZUNIQUE{1:03d}" in txt
    assert f"ZZUNIQUE{n:03d}" in txt


def test_704b_raises_when_over_capacity():
    q = _quote(quote_id="q-overflow", n_items=40)
    with pytest.raises(SpineFormFillError) as excinfo:
        fill_704b_pdf(q, _identity(), today=datetime(2026, 5, 16))
    msg = str(excinfo.value)
    assert "39" in msg  # template capacity
    assert "40" in msg  # actual count


# ──────────────────────────────────────────────────────────────────────
# Flatten vs fillable
# ──────────────────────────────────────────────────────────────────────


def test_704b_flatten_default_bakes_into_content():
    pdf_bytes = fill_704b_pdf(_quote(n_items=4), _identity(),
                              today=datetime(2026, 5, 16))
    txt = _extract(pdf_bytes)
    assert "Reytech Inc" in txt
    assert "10846581" in txt


def test_704b_fillable_keeps_widgets_with_values_set():
    pdf_bytes = fill_704b_pdf(
        _quote(n_items=4), _identity(),
        today=datetime(2026, 5, 16),
        flatten=False,
    )
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    fields = reader.get_fields() or {}
    # Template has 362 fields per inspection; assert most retained.
    assert len(fields) >= 300, (
        f"fillable mode lost form widgets: only {len(fields)} fields"
    )
    sol = fields.get("SOLICITATION")
    assert sol is not None
    assert sol.get("/V") == "10846581"
    company = fields.get("COMPANY NAME")
    assert company is not None
    assert "Reytech" in str(company.get("/V"))


# ──────────────────────────────────────────────────────────────────────
# Matching gate — sabotage paths
# ──────────────────────────────────────────────────────────────────────


def test_704b_gate_raises_when_acroform_fill_is_silent_noop(monkeypatch):
    """If pypdf's update_page_form_field_values silently no-ops, the
    field map is built but nothing is written. The gate's pdfplumber
    re-extract must catch missing line-item content."""
    import pypdf

    def silent_noop(self, page, field_values, **kw):
        return None
    monkeypatch.setattr(
        pypdf.PdfWriter, "update_page_form_field_values", silent_noop,
    )

    with pytest.raises(SpineFormFillError) as excinfo:
        fill_704b_pdf(_quote(n_items=3), _identity(),
                      today=datetime(2026, 5, 16))
    msg = str(excinfo.value)
    assert "704B fill gate" in msg


def test_704b_gate_raises_in_fillable_mode_when_acroform_fill_noop(monkeypatch):
    import pypdf

    def silent_noop(self, page, field_values, **kw):
        return None
    monkeypatch.setattr(
        pypdf.PdfWriter, "update_page_form_field_values", silent_noop,
    )

    with pytest.raises(SpineFormFillError) as excinfo:
        fill_704b_pdf(
            _quote(n_items=3), _identity(),
            today=datetime(2026, 5, 16),
            flatten=False,
        )
    assert "fillable" in str(excinfo.value)


# ──────────────────────────────────────────────────────────────────────
# Route integration
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_704b.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def test_route_704b_returns_pdf(client, db_path):
    write_quote(db_path, _quote("Q-rt-704-001", n_items=5), actor="seed")
    r = client.get("/spine/quotes/Q-rt-704-001/forms/704b/pdf")
    assert r.status_code == 200
    assert r.mimetype == "application/pdf"
    assert r.data.startswith(b"%PDF-")
    assert "inline" in r.headers["Content-Disposition"]


def test_route_704b_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/Q-no-such/forms/704b/pdf")
    assert r.status_code == 404


def test_route_704b_fillable_query_param(client, db_path):
    write_quote(db_path, _quote("Q-rt-704-fill", n_items=3), actor="seed")
    r = client.get("/spine/quotes/Q-rt-704-fill/forms/704b/pdf?fillable=1")
    assert r.status_code == 200
    fields = pypdf.PdfReader(io.BytesIO(r.data)).get_fields() or {}
    assert len(fields) >= 300


def test_route_704b_returns_409_when_over_capacity(client, db_path):
    write_quote(db_path, _quote("Q-rt-704-over", n_items=40), actor="seed")
    r = client.get("/spine/quotes/Q-rt-704-over/forms/704b/pdf")
    assert r.status_code == 409
    body = r.get_json()
    assert body["error"] == "form_fill_mismatch"
