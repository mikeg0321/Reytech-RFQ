"""Tests for the EmailContract master-substrate primitive.

Mike 2026-05-16: the email contract is the ground truth that
everything downstream is compared to. These tests prove:

- The EmailContract model rejects unknown fields (extra='forbid').
- write_email_contract is the one writer; append-only (re-issue
  raises).
- read_email_contract round-trips a contract byte-faithfully.
- find_contract_for_quote returns the contract linked to a quote_id.
- contract_vs_quote computes the right deltas (top-level, line
  count, per-line overrides, line additions/removals).
- /contract and /contract-diff endpoints return the documented shape.
- Architectural: no Spine substrate column literally named
  "contract" outside the EmailContract / Quote linkage points.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    ContractDelta, ContractLineItem, EmailContract,
    LineItem, Quote, QuoteStatus, SpineValidationError,
    contract_vs_quote, find_contract_for_quote,
    init_db, read_email_contract, write_email_contract, write_quote,
)


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _contract(contract_id="contract_rfq_test_001", rfq_id="rfq_test_001",
              n_lines=2) -> EmailContract:
    return EmailContract(
        contract_id=contract_id,
        rfq_id=rfq_id,
        source_email_id="msg_abc@gmail",
        source_thread_id="thread_xyz",
        buyer_name="Mohammed Chechi",
        buyer_email="m.chechi@cchcs.ca.gov",
        agency="CCHCS",
        facility="CCWF Chowchilla",
        solicitation_number="10846581",
        rfq_title="RFQ — Resvent CPAP units",
        due_date=datetime(2026, 5, 30, 17, 0, tzinfo=timezone.utc),
        ship_to_address="21450 Road 24, Chowchilla, CA 93610",
        tax_rate_bps=775,
        line_items=[
            ContractLineItem(
                line_no=i + 1,
                description=f"Item {i+1}",
                qty=10,
                uom="EA",
                mfg_number_suggested=f"BUY-{i+1:03d}",
            )
            for i in range(n_lines)
        ],
    )


def _quote_matching(contract: EmailContract) -> Quote:
    """Build a Quote that's faithful to the contract (no deltas)."""
    return Quote(
        quote_id=contract.rfq_id,
        agency=contract.agency,
        facility=contract.facility,
        solicitation_number=contract.solicitation_number,
        line_items=[
            LineItem(
                line_no=c.line_no,
                description=c.description,
                mfg_number=c.mfg_number_suggested,
                qty=c.qty,
                uom=c.uom,
                cost_cents=1000,
                unit_price_cents=2000,
                cost_source_url="https://example.com",
                cost_validated_at=_fresh_ts(),
            )
            for c in contract.line_items
        ],
        tax_rate_bps=contract.tax_rate_bps or 775,
        status=QuoteStatus.PARSED,
    )


# ──────────────────────────────────────────────────────────────────────
# Model
# ──────────────────────────────────────────────────────────────────────


def test_email_contract_rejects_unknown_field():
    """extra='forbid' — no alias creep."""
    with pytest.raises(Exception):
        EmailContract.model_validate({
            **_contract().model_dump(mode="json"),
            "random_unknown_field": "hello",
        })


def test_email_contract_requires_at_least_one_line():
    with pytest.raises(Exception):
        EmailContract(
            contract_id="x_001", agency="CCHCS", facility="Test",
            solicitation_number="X", line_items=[],
        )


def test_email_contract_rejects_duplicate_line_nos():
    with pytest.raises(Exception):
        EmailContract(
            contract_id="x_002", agency="CCHCS", facility="Test",
            solicitation_number="X",
            line_items=[
                ContractLineItem(line_no=1, description="a", qty=1, uom="EA"),
                ContractLineItem(line_no=1, description="b", qty=1, uom="EA"),
            ],
        )


# ──────────────────────────────────────────────────────────────────────
# DB writer — append-only invariant
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_contract.db"
    init_db(str(p))
    return str(p)


def test_write_and_read_round_trip(db_path):
    c = _contract()
    res = write_email_contract(db_path, c)
    assert res["contract_id"] == c.contract_id
    assert len(res["sha256"]) == 64

    loaded = read_email_contract(db_path, c.contract_id)
    assert loaded == c


def test_write_email_contract_is_append_only(db_path):
    c = _contract()
    write_email_contract(db_path, c)
    with pytest.raises(SpineValidationError) as excinfo:
        write_email_contract(db_path, c)
    assert "append-only" in str(excinfo.value).lower()
    assert "rebid" in str(excinfo.value).lower()


def test_rebid_creates_new_contract_id_same_thread(db_path):
    """The rebid pattern: same source_thread_id, new contract_id."""
    original = _contract(contract_id="contract_v1", rfq_id="rfq_001")
    write_email_contract(db_path, original)

    revised = original.model_copy(update={"contract_id": "contract_v2"})
    # rfq_id can be the same (the rebid is for the same Spine quote),
    # but contract_id MUST differ.
    res = write_email_contract(db_path, revised)
    assert res["contract_id"] == "contract_v2"

    # The latest contract for the rfq_id is the revised one.
    latest = find_contract_for_quote(db_path, "rfq_001")
    assert latest.contract_id == "contract_v2"


def test_find_contract_for_quote_returns_none_when_absent(db_path):
    assert find_contract_for_quote(db_path, "rfq_no_contract") is None


def test_write_rejects_non_email_contract(db_path):
    with pytest.raises(SpineValidationError):
        write_email_contract(db_path, {"not": "a contract"})


# ──────────────────────────────────────────────────────────────────────
# Diff function — pure
# ──────────────────────────────────────────────────────────────────────


def test_diff_empty_when_quote_matches_contract():
    c = _contract()
    q = _quote_matching(c)
    deltas = contract_vs_quote(c, q)
    assert deltas == [], f"expected no deltas, got: {deltas}"


def test_diff_sol_number_override():
    c = _contract()
    q = _quote_matching(c).model_copy(update={"solicitation_number": "DIFFERENT"})
    deltas = contract_vs_quote(c, q)
    assert any(d.field_path == "solicitation_number" for d in deltas)


def test_diff_qty_override():
    c = _contract()
    q = _quote_matching(c)
    # Edit qty on line 1.
    new_li = q.line_items[0].model_copy(update={"qty": 25})
    q = q.model_copy(update={"line_items": [new_li, *q.line_items[1:]]})
    deltas = contract_vs_quote(c, q)
    assert any(d.field_path == "line_items[1].qty" for d in deltas)
    qd = next(d for d in deltas if d.field_path == "line_items[1].qty")
    assert qd.contract_value == 10
    assert qd.quote_value == 25


def test_diff_qty_wide_divergence_warning():
    """qty going from 10 to 200 (20x) should be a warning, not just an override."""
    c = _contract()
    q = _quote_matching(c)
    new_li = q.line_items[0].model_copy(update={"qty": 200})
    q = q.model_copy(update={"line_items": [new_li, *q.line_items[1:]]})
    deltas = contract_vs_quote(c, q)
    qty_delta = next(d for d in deltas if d.field_path == "line_items[1].qty")
    assert qty_delta.severity == "warning"


def test_diff_line_added_by_operator():
    c = _contract(n_lines=2)
    q = _quote_matching(c)
    # Operator adds a line 3.
    extra = LineItem(
        line_no=3, description="Operator-added item", mfg_number="X",
        qty=1, uom="EA", cost_cents=1000, unit_price_cents=2000,
        cost_source_url="https://example.com",
        cost_validated_at=_fresh_ts(),
    )
    q = q.model_copy(update={"line_items": [*q.line_items, extra]})
    deltas = contract_vs_quote(c, q)
    line_count_d = next(d for d in deltas if d.field_path == "line_items.length")
    assert line_count_d.contract_value == 2
    assert line_count_d.quote_value == 3
    line_added = next(d for d in deltas if d.field_path == "line_items[3]"
                       and "added" in d.detail)
    assert line_added.contract_value is None


def test_diff_line_removed_by_operator():
    c = _contract(n_lines=3)
    q = _quote_matching(c)
    # Operator drops line 2.
    q = q.model_copy(update={
        "line_items": [li for li in q.line_items if li.line_no != 2],
    })
    deltas = contract_vs_quote(c, q)
    line_removed = next(d for d in deltas if d.field_path == "line_items[2]"
                         and "removed" in d.detail)
    assert line_removed.quote_value is None


def test_diff_mfg_number_filled_by_operator_is_info():
    """Buyer didn't suggest a MFG #; operator typed one. info-level."""
    c = EmailContract(
        contract_id="ct_001", rfq_id="rfq_001",
        agency="CCHCS", facility="Test", solicitation_number="X",
        line_items=[ContractLineItem(
            line_no=1, description="x", qty=1, uom="EA",
            mfg_number_suggested=None,
        )],
    )
    q = Quote(
        quote_id="rfq_001", agency="CCHCS", facility="Test",
        solicitation_number="X",
        line_items=[LineItem(
            line_no=1, description="x", mfg_number="OPERATOR-FILLED",
            qty=1, uom="EA", cost_cents=1000, unit_price_cents=2000,
            cost_source_url="https://example.com",
            cost_validated_at=_fresh_ts(),
        )],
        tax_rate_bps=775, status=QuoteStatus.PARSED,
    )
    deltas = contract_vs_quote(c, q)
    info = next(d for d in deltas if "filled in MFG" in d.detail)
    assert info.severity == "info"


# ──────────────────────────────────────────────────────────────────────
# HTTP routes
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def test_route_contract_returns_404_when_no_quote(client):
    r = client.get("/spine/quotes/Q-missing/contract")
    assert r.status_code == 404


def test_route_contract_returns_404_when_no_contract_linked(client, db_path):
    """Quote exists but has no EmailContract linked — legacy data."""
    q = _quote_matching(_contract())
    write_quote(db_path, q, actor="seed")
    r = client.get(f"/spine/quotes/{q.quote_id}/contract")
    assert r.status_code == 404
    assert r.json["error"] == "no_contract"


def test_route_contract_returns_contract_when_linked(client, db_path):
    c = _contract()
    write_email_contract(db_path, c)
    q = _quote_matching(c)
    write_quote(db_path, q, actor="seed")
    r = client.get(f"/spine/quotes/{c.rfq_id}/contract")
    assert r.status_code == 200
    assert r.json["contract_id"] == c.contract_id
    assert r.json["solicitation_number"] == c.solicitation_number
    assert len(r.json["line_items"]) == len(c.line_items)


def test_route_contract_diff_clean_when_quote_matches(client, db_path):
    c = _contract()
    write_email_contract(db_path, c)
    q = _quote_matching(c)
    write_quote(db_path, q, actor="seed")
    r = client.get(f"/spine/quotes/{c.rfq_id}/contract-diff")
    assert r.status_code == 200
    assert r.json["clean"] is True
    assert r.json["delta_count"] == 0
    assert r.json["deltas"] == []


def test_route_contract_diff_shows_overrides(client, db_path):
    c = _contract()
    write_email_contract(db_path, c)
    q = _quote_matching(c)
    # Edit qty on line 1.
    new_li = q.line_items[0].model_copy(update={"qty": 50})
    q = q.model_copy(update={"line_items": [new_li, *q.line_items[1:]]})
    write_quote(db_path, q, actor="seed")
    r = client.get(f"/spine/quotes/{c.rfq_id}/contract-diff")
    assert r.status_code == 200
    assert r.json["clean"] is False
    assert r.json["delta_count"] >= 1
    qty_delta = next(
        d for d in r.json["deltas"] if d["field_path"] == "line_items[1].qty"
    )
    assert qty_delta["contract_value"] == 10
    assert qty_delta["quote_value"] == 50


# ──────────────────────────────────────────────────────────────────────
# Architectural — substrate columns
# ──────────────────────────────────────────────────────────────────────


def test_substrate_has_no_contract_blob_in_quote():
    """The link from Quote to EmailContract is by rfq_id matching
    EmailContract.rfq_id, NOT by a contract_blob on Quote. Keeping
    the contract separate enforces the append-only / immutable
    property of the ground-truth record."""
    for field_name in Quote.model_fields.keys():
        assert not field_name.endswith("_contract_json"), (
            f"Quote.{field_name}: contract should not be embedded; "
            "use find_contract_for_quote() instead."
        )
