"""GET /spine/quotes/<id>/visual-qa — Tier-1 visual-fidelity gate route.

Peer to /inspector. Renders the buyer-bound artifacts (format-aware:
single_pdf → packet; separate_pdfs → 703B + 704B + bidpkg), flattens
each via the PR-10 appearance-regen helper, runs visual_qa Tier-1
detectors, returns aggregated report.
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
    QuoteStatus,
    init_db,
    write_email_contract,
    write_quote,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_T703B = "tests/fixtures/703b_blank.pdf"
_T704B = "tests/fixtures/704b_blank.pdf"
_TBIDPKG = "tests/fixtures/cchcs_bidpkg_blank.pdf"

_B_PRESENT = all((_REPO_ROOT / p).is_file() for p in (_T703B, _T704B, _TBIDPKG))
_needs_b = pytest.mark.skipif(not _B_PRESENT, reason="Format-B fixtures missing")


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_vqa.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    app.register_blueprint(make_spine_blueprint(db_path, auth_decorator=None))
    return app.test_client()


def _seed_quote(db_path: str, packaging: str = "separate_pdfs"):
    q = Quote(
        quote_id="Q-vqa", agency="CCHCS", facility="SAC",
        solicitation_number="10848901",
        line_items=[LineItem(
            line_no=1, description="Test Item", mfg_number="X-1",
            qty=5, uom="EA", cost_cents=8000,
            cost_source_url="https://example.com/x",
            cost_validated_at=datetime.now(timezone.utc),
            unit_price_cents=12500,
        )],
        tax_rate_bps=775, status=QuoteStatus.FINALIZED,
    )
    c = EmailContract(
        contract_id="contract_Q-vqa_b", rfq_id="Q-vqa",
        agency="CCHCS", facility="SAC", solicitation_number="10848901",
        buyer_name="Grace Pfost", buyer_email="grace.pfost@cdcr.ca.gov",
        buyer_phone="(916) 555-0142",
        line_items=[ContractLineItem(line_no=1, description="Test Item",
                                      qty=5, uom="EA")],
        attachment_refs=[_T703B, _T704B, _TBIDPKG],
        response_packaging=packaging,
    )
    write_quote(db_path, q, actor="t")
    write_email_contract(db_path, c)


def test_visual_qa_route_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/nope/visual-qa")
    assert r.status_code == 404


@_needs_b
def test_visual_qa_route_returns_per_form_reports(client, db_path):
    _seed_quote(db_path, packaging="separate_pdfs")
    r = client.get("/spine/quotes/Q-vqa/visual-qa")
    assert r.status_code == 200, r.get_data(as_text=True)
    env = r.get_json()
    assert env["quote_id"] == "Q-vqa"
    assert env["response_packaging"] == "separate_pdfs"
    assert "ok" in env
    assert "total_blocking" in env
    assert "total_warnings" in env
    codes = {f["form_code"] for f in env["per_form"]}
    assert codes == {"703b", "704b", "bidpkg"}
    # Each per-form report carries a structured VisualQAReport dict.
    for f in env["per_form"]:
        report = f["report"]
        assert "ok" in report
        assert "pdf_pages" in report
        assert "issues" in report
        assert "detectors_run" in report
        # Tier-1 detectors must always appear in detectors_run.
        assert "cid_glyph_artifacts" in report["detectors_run"]
        assert "comb_class_spacing" in report["detectors_run"]
        assert "empty_required_field" in report["detectors_run"]
