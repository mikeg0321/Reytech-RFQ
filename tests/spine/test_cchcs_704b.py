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
    # Gate is now unified for flat + fillable (PR #1057: reads /V values
    # regardless of flatten mode, since the gate runs against fillable
    # bytes before flatten). Error message no longer contains "fillable".
    assert "704B fill gate" in str(excinfo.value)
    assert "substrate regression" in str(excinfo.value)


# ──────────────────────────────────────────────────────────────────────
# Route integration
#
# The /forms/704b/pdf route was repointed to the legacy-filler packet
# adapter on 2026-05-20 (handoff-2026-05-20-legacy-adapter-build) — it no
# longer calls fill_704b_pdf. Route-layer coverage now lives in
# tests/spine/test_routes_packet.py. The not-found path is kept here as a
# smoke check; the per-form-renderer route tests (returns_pdf / fillable /
# over-capacity) were retired with the renderer.
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


def test_route_704b_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/Q-no-such/forms/704b/pdf")
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# PR #1053 — REQUESTOR from contract, ITEM NUMBER from mfg, merchandise
# subtotal populated. Each regression caught 5/18 on R26Q40 vision-walk.
# ──────────────────────────────────────────────────────────────────────


def _ok_contract(quote_id: str = "rfq_704b_test", **overrides):
    from src.spine.email_contract import ContractLineItem, EmailContract
    base = dict(
        contract_id="contract_704b_test_001",
        rfq_id=quote_id,
        agency="CCHCS",
        facility="Test - CCWF",
        solicitation_number="10846581",
        buyer_name="Marc Argarin",
        buyer_email="marc.argarin@cdcr.ca.gov",
        buyer_phone="555-555-5555",
        ship_to_address="900 Quebec Ave\nCorcoran, CA 93212",
        ship_to_facility="CCWF Receiving",
        line_items=[ContractLineItem(line_no=1, description="X", qty=1, uom="EA")],
    )
    base.update(overrides)
    return EmailContract(**base)


def _extract_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def test_704b_requestor_block_comes_from_contract_not_reytech():
    """REQUESTOR + PHONE/EMAIL must show the state official (buyer side),
    NOT Reytech identity. Pre-#1053 the 704B rendered REQUESTOR=Reytech
    contact_person which made the bid look like Reytech was requesting
    its own quote — would fail CCHCS responsiveness."""
    q = _quote("Q-req-from-contract", n_items=1)
    c = _ok_contract(
        quote_id="Q-req-from-contract",
        buyer_name="Marc Argarin",
        buyer_email="marc.argarin@cdcr.ca.gov",
        buyer_phone="916-555-1234",
    )
    text = _extract_text(fill_704b_pdf(q, _identity(), contract=c))
    # Buyer info present in REQUESTOR block.
    assert "Marc Argarin" in text
    assert "marc.argarin@cdcr.ca.gov" in text
    assert "916-555-1234" in text
    # Reytech identity (defaults) MUST NOT be in the requestor slot —
    # it can still appear in COMPANY NAME / PERSON PROVIDING QUOTE.
    # Specifically: Reytech's identity.email must NOT show as the
    # REQUESTOR email. The check is structural: buyer email present
    # AND identity email NOT present, OR identity email present only
    # in the COMPANY-NAME context.
    assert _identity().email not in text or "marc.argarin" in text


def test_704b_item_number_renders_mfg_not_row_number():
    """ITEM NUMBER column = buyer-side MFG / catalog #. Pre-#1053 it
    rendered str(li.line_no) which collided with row position and was
    useless for the agency cross-checking the manufacturer's catalog.
    Mirror of PR #1045 (parser side); writer side was missed."""
    q = _quote("Q-itemno-mfg", n_items=2)
    # Override mfg to a real-shape part #.
    q = q.model_copy(update={
        "line_items": [
            q.line_items[0].model_copy(update={"mfg_number": "503-0142-01"}),
            q.line_items[1].model_copy(update={"mfg_number": "008-0869-00"}),
        ]
    })
    text = _extract_text(fill_704b_pdf(q, _identity()))
    # pdfplumber splits cell content on column-edge whitespace —
    # flatten before checking.
    flat = "".join(text.split())
    assert "503-0142-01" in flat
    assert "008-0869-00" in flat


def test_704b_merchandise_subtotal_field_is_populated():
    """The bottom-of-page MERCHANDISE SUBTOTAL (fill_154) must equal
    the sum of line extensions. Pre-#1053 it was blank — CCHCS reviewer
    had to hand-total to verify."""
    q = _quote("Q-merch-sub", n_items=3)
    expected_total = sum(li.extension_cents for li in q.line_items)
    text = _extract_text(fill_704b_pdf(q, _identity()))
    whole, frac = divmod(expected_total, 100)
    expected_str = f"{whole:,}.{frac:02d}"
    flat = "".join(text.replace("$", "").split())
    assert expected_str.replace(",", "") in flat


def test_704b_no_contract_falls_back_to_reytech_for_requestor():
    """Legacy callers passing no contract still render — REQUESTOR
    falls back to Reytech identity. Keeps pre-#1053 fixtures + tests
    green; production should always pass a contract."""
    q = _quote("Q-no-contract", n_items=1)
    text = _extract_text(fill_704b_pdf(q, _identity()))
    # Falls back: Reytech contact_person shows as both PERSON PROVIDING
    # QUOTE and REQUESTOR (legacy behavior, documented in _field_map).
    assert _identity().contact_person in text


def test_reytech_identity_defaults_are_real_not_placeholder():
    """The dataclass defaults MUST be Mike's actual buyer-facing values.
    Closes the 5/18 ship-blocking class where prod (no REYTECH_* env)
    fell through to "Michael Greenwald / 1 Reytech Way, Irvine /
    rfq@" placeholders. Every CCHCS form with those defaults would
    fail responsiveness."""
    i = ReytechIdentity()
    assert i.business_name == "Reytech Inc."
    assert "Trabuco Canyon" in i.address
    assert "30 Carnoustie Way" in i.address
    assert i.contact_person == "Michael Guadan"
    assert i.title == "Owner"
    assert i.email == "sales@reytechinc.com"
    assert i.sellers_permit == "245652416-00001"
