"""Spine → legacy CCHCS standalone-form-set adapter — src/spine/forms_render.py.

The adapter delegates to the verified legacy fillers in
src/forms/reytech_filler_v4.py (fill_703b / fill_703c / fill_704b /
fill_bid_package). These tests prove its jobs:

  * classify the buyer's template PDFs into 703/704b/bidpkg slots,
  * build the legacy rfq dict from the Spine Quote + EmailContract,
  * render the standalone three-form set + a merged PDF,
  * fail loudly (ok=False, never a blank document) when a contract or a
    required template is missing,

plus the format-aware /forms route dispatch — single_pdf keeps the
packet adapter, separate_pdfs uses this one.

Fixtures: tests/fixtures/{703b_blank,704b_blank,cchcs_bidpkg_blank}.pdf —
the blank CCHCS templates the legacy fillers were built to fill.
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
from src.spine.forms_render import (
    _build_legacy_rfq_dict,
    _classify_form_templates,
    _pick_703_code,
    render_cchcs_forms_via_legacy,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_T703B = "tests/fixtures/703b_blank.pdf"
_T704B = "tests/fixtures/704b_blank.pdf"
_TBIDPKG = "tests/fixtures/cchcs_bidpkg_blank.pdf"
_FORMAT_B_REFS = (_T703B, _T704B, _TBIDPKG)

# Skip the render tests when a fixture went missing — they exercise the
# real legacy fillers against real PDFs.
_FIXTURES_PRESENT = all((_REPO_ROOT / p).is_file() for p in _FORMAT_B_REFS)
_needs_fixtures = pytest.mark.skipif(
    not _FIXTURES_PRESENT, reason="CCHCS Format-B template fixtures missing"
)


# ── builders ──────────────────────────────────────────────────────────


def _line(line_no, description, *, mfg=None, qty=4, unit_price_cents=12500):
    return LineItem(
        line_no=line_no,
        description=description,
        mfg_number=mfg,
        qty=qty,
        uom="EA",
        cost_cents=8000,
        cost_source_url="https://example.com/item",
        cost_validated_at=datetime.now(timezone.utc),
        unit_price_cents=unit_price_cents,
    )


def _quote(line_items=None, *, quote_id="Q-forms-001", tax_rate_bps=775):
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10848888",
        line_items=line_items or [
            _line(1, "Nitrile Exam Gloves, Large, powder-free", mfg="N-GLV-L"),
            _line(2, "Isolation Gowns, yellow, universal", mfg="ISO-GWN", qty=10),
        ],
        tax_rate_bps=tax_rate_bps,
    )


def _contract(
    *,
    quote_id="Q-forms-001",
    attachment_refs=_FORMAT_B_REFS,
    required_forms=None,
    response_packaging="separate_pdfs",
):
    kwargs = dict(
        contract_id=f"contract_{quote_id}_1747900000",
        rfq_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10848888",
        buyer_name="Grace Pfost",
        buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        release_date=datetime(2026, 5, 18, tzinfo=timezone.utc),
        due_date=datetime(2026, 5, 25, tzinfo=timezone.utc),
        ship_to_address="California State Prison, Sacramento, Folsom, CA 95671",
        line_items=[
            ContractLineItem(line_no=1, description="Nitrile Exam Gloves", qty=4, uom="EA"),
            ContractLineItem(line_no=2, description="Isolation Gowns", qty=10, uom="EA"),
        ],
        attachment_refs=list(attachment_refs),
        response_packaging=response_packaging,
    )
    if required_forms is not None:
        kwargs["required_forms"] = required_forms
    return EmailContract(**kwargs)


# ── unit: attachment classification ───────────────────────────────────


def test_classify_maps_filenames_to_form_slots():
    slots = _classify_form_templates([
        "/x/10848888_AMS_703B_-_RFQ_-_Informal.pdf",
        "/x/10848888_AMS_704B_-_Acquisition_Quote_Worksheet.pdf",
        "/x/10848888_BID_PACKAGE___FORMS__Under_100k.pdf",
    ])
    assert slots["703b"].endswith("703B_-_RFQ_-_Informal.pdf")
    assert slots["704b"].endswith("Worksheet.pdf")
    assert slots["bidpkg"].endswith("Under_100k.pdf")


def test_classify_704b_not_misread_as_703b_by_rfq_marker():
    """A 704B worksheet filename containing 'RFQ' must not land in 703b."""
    slots = _classify_form_templates([
        "/x/AMS_704B_-_CCHCS_Acquisition_Quote_Worksheet_-_RFQ_10837703.pdf",
    ])
    assert slots.get("704b") is not None
    assert "703b" not in slots


def test_classify_703c_takes_precedence_over_703b_marker():
    slots = _classify_form_templates(["/x/10848888_AMS_703C_Fair_and_Reasonable.pdf"])
    assert "703c" in slots
    assert "703b" not in slots


# ── unit: 703 variant selection (LAW 6 — the contract decides) ─────────


def test_pick_703_defaults_to_703b():
    assert _pick_703_code(_contract()) == "703b"


def test_pick_703c_when_required_forms_declares_it():
    c = _contract(required_forms=["703c", "704b", "bidpkg", "quote"])
    assert _pick_703_code(c) == "703c"


# ── unit: Spine model → legacy rfq dict ───────────────────────────────


def test_legacy_rfq_dict_maps_every_field():
    quote = _quote()
    r = _build_legacy_rfq_dict(quote, _contract())

    assert r["solicitation_number"] == "10848888"
    assert r["sign_date"]                       # adapter stamps PST today
    assert r["release_date"] == "05/18/2026"    # datetime → US m/d/Y
    assert r["due_date"] == "05/25/2026"
    assert r["requestor_name"] == "Grace Pfost"
    assert r["requestor_email"] == "grace.pfost@cdcr.ca.gov"
    assert r["requestor_phone"] == "(916) 555-0142"
    assert r["agency"] == "CCHCS"
    assert "Folsom" in r["delivery_location"]
    assert len(r["line_items"]) == 2


def test_legacy_rfq_dict_line_items_carry_no_markup_key():
    """A markup_pct/markup key would let pricing_math forward-compute
    cost*markup and override the operator's typed unit_price."""
    r = _build_legacy_rfq_dict(_quote(), _contract())
    for li in r["line_items"]:
        assert "markup_pct" not in li
        assert "markup" not in li
        # unit_price + price_per_unit mirror the Spine unit_price_cents.
        assert li["unit_price"] == li["price_per_unit"]
        assert li["unit_price"] > 0


# ── render: the full standalone set ───────────────────────────────────


@_needs_fixtures
def test_renders_full_standalone_set(tmp_path):
    res = render_cchcs_forms_via_legacy(
        _quote(), _contract(), output_dir=str(tmp_path), strict=False
    )
    assert res["ok"], res["error"]
    # All three forms rendered.
    assert set(res["forms"]) == {"703", "704b", "bidpkg"}
    for key, sub in res["forms"].items():
        assert sub["ok"], f"{key}: {sub['error']}"
        assert sub["pdf_bytes"][:5] == b"%PDF-"
    # Merged PDF produced.
    assert res["pdf_bytes"][:5] == b"%PDF-"
    assert Path(res["output_path"]).is_file()
    assert res["match_report"]["form_703_variant"] == "703b"
    assert res["match_report"]["line_items"] == 2


@_needs_fixtures
def test_704b_header_completed_from_contract(tmp_path):
    """The adapter post-fills the 704B header that fill_704b leaves blank.

    fill_704b never writes SOLICITATION#/REQUESTOR/DEPARTMENT/PHONE-EMAIL/
    DATE or the # column ("buyer fills" rule). _complete_704b_form fills
    them from the EmailContract — LAW 6 (the Spine holds the spec).
    """
    from pypdf import PdfReader

    res = render_cchcs_forms_via_legacy(
        _quote(), _contract(), output_dir=str(tmp_path), strict=False
    )
    assert res["ok"], res["error"]
    fields = PdfReader(res["forms"]["704b"]["output_path"]).get_fields() or {}

    def _v(name: str) -> str:
        return str((fields.get(name) or {}).get("/V") or "")

    # Header — from the contract.
    assert _v("SOLICITATION") == "10848888"
    assert _v("REQUESTOR") == "Grace Pfost"
    assert "CCHCS" in _v("DEPARTMENT")
    assert "grace.pfost@cdcr.ca.gov" in _v("PHONEEMAIL")
    # The # line-number column.
    assert _v("Row1") == "1"
    assert _v("Row2") == "2"


@_needs_fixtures
def test_merged_pdf_spans_all_three_forms(tmp_path):
    """The merged PDF page count == sum of the per-form page counts."""
    from pypdf import PdfReader

    res = render_cchcs_forms_via_legacy(
        _quote(), _contract(), output_dir=str(tmp_path), strict=False
    )
    assert res["ok"], res["error"]
    per_form = sum(
        len(PdfReader(s["output_path"]).pages) for s in res["forms"].values()
    )
    merged = len(PdfReader(res["output_path"]).pages)
    assert merged == per_form
    assert merged > 3  # the bid package alone trims to several pages


# ── render: loud failure, never a blank document ──────────────────────


def test_no_contract_fails_loudly():
    res = render_cchcs_forms_via_legacy(_quote(), None)
    assert res["ok"] is False
    assert "EmailContract" in res["error"]
    assert res["pdf_bytes"] == b""


def test_unresolvable_refs_fail_loudly():
    res = render_cchcs_forms_via_legacy(
        _quote(), _contract(attachment_refs=("bogus/nope.pdf",))
    )
    assert res["ok"] is False
    assert "no PDF on disk" in res["error"]


@_needs_fixtures
def test_missing_one_template_fails_loudly():
    """Only the 703B present — the 704B + Bid Package are missing."""
    res = render_cchcs_forms_via_legacy(
        _quote(), _contract(attachment_refs=(_T703B,))
    )
    assert res["ok"] is False
    assert "704B" in res["error"] and "Bid Package" in res["error"]
    assert res["pdf_bytes"] == b""


# ── route dispatch — format-aware ─────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_forms_routes.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    app.register_blueprint(make_spine_blueprint(db_path, auth_decorator=None))
    return app.test_client()


def _seed(db_path, contract):
    write_quote(db_path, _quote(), actor="test_seed")
    write_email_contract(db_path, contract)


@_needs_fixtures
@pytest.mark.parametrize("form", ["703b", "704b", "bidpkg"])
def test_separate_pdfs_route_serves_standalone_form(client, db_path, form):
    _seed(db_path, _contract())
    r = client.get(f"/spine/quotes/Q-forms-001/forms/{form}/pdf")
    assert r.status_code == 200, r.get_data(as_text=True)
    assert r.mimetype == "application/pdf"
    assert r.get_data()[:5] == b"%PDF-"
    # Standalone-form headers (not the packet headers) — proves the
    # forms_render adapter served this, not packet_render.
    assert "X-Spine-Form-Code" in r.headers
    assert r.headers["X-Spine-Form-Ok"] == "True"


@_needs_fixtures
def test_separate_pdfs_703_route_reports_variant(client, db_path):
    _seed(db_path, _contract())
    r = client.get("/spine/quotes/Q-forms-001/forms/703b/pdf")
    assert r.status_code == 200
    assert r.headers["X-Spine-Form-Code"] == "703b"


@_needs_fixtures
def test_separate_pdfs_route_409_when_templates_missing(client, db_path):
    _seed(db_path, _contract(attachment_refs=("bogus/missing.pdf",)))
    r = client.get("/spine/quotes/Q-forms-001/forms/704b/pdf")
    assert r.status_code == 409
    assert r.get_json()["error"] == "form_render_failed"


def test_separate_pdfs_route_409_when_no_contract(client, db_path):
    write_quote(db_path, _quote(), actor="test_seed")  # quote, no contract
    r = client.get("/spine/quotes/Q-forms-001/forms/703b/pdf")
    assert r.status_code == 409
    assert r.get_json()["error"] == "form_render_failed"


def test_forms_route_404_for_missing_quote(client):
    r = client.get("/spine/quotes/Q-nope/forms/703b/pdf")
    assert r.status_code == 404


def test_single_pdf_packaging_routes_to_packet_adapter(client, db_path):
    """A single_pdf contract has no separate templates — the /forms/703b
    route must fall through to the packet adapter, which 409s here
    because no packet PDF is bound (proving it took the packet path,
    not forms_render: the error key is the packet adapter's)."""
    _seed(db_path, _contract(
        attachment_refs=(), response_packaging="single_pdf",
    ))
    r = client.get("/spine/quotes/Q-forms-001/forms/703b/pdf")
    assert r.status_code == 409
    # packet adapter's error key — forms_render would say form_render_failed.
    assert r.get_json()["error"] == "packet_render_failed"
