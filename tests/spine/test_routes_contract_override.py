"""POST /spine/quotes/<id>/contract-override — operator field corrections.

Closes the 5/18 gap Mike caught: ingest parser sometimes misses
buyer_name / buyer_phone / ship_to_address; operator needs a path to
fill those in so the rendered packet (Quote PDF, 703B State Official
block, 704B REQUESTOR block, bidpkg) carries them.

Substrate rule: contracts are append-only. This endpoint writes a NEW
contract row with the same rfq_id but a new contract_id and later
ingested_at. `find_contract_for_quote` returns latest, so renderers
transparently pick up corrections without code changes.
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
    find_contract_for_quote,
    init_db,
    write_email_contract,
    write_quote,
)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_override.db"
    init_db(p)
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def _make_quote(quote_id="Q-ov-001") -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10847457",
        line_items=[
            LineItem(
                line_no=1,
                description="Power Supply 503-0142-01",
                qty=2, uom="EA",
                cost_cents=0,
                cost_source_url="https://example.com",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=270,
            ),
        ],
        tax_rate_bps=775,
    )


def _make_contract(
    quote_id="Q-ov-001",
    *,
    buyer_name=None,
    buyer_phone=None,
    ship_to_address=None,
) -> EmailContract:
    return EmailContract(
        contract_id=f"contract_{quote_id}_1747000000",
        rfq_id=quote_id,
        source_email_id="msgORIG",
        source_thread_id="threadORIG",
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10847457",
        buyer_name=buyer_name,
        buyer_phone=buyer_phone,
        buyer_email="marc.argarin@cdcr.ca.gov",
        ship_to_address=ship_to_address,
        line_items=[
            ContractLineItem(line_no=1, description="Power Supply", qty=2, uom="EA"),
        ],
    )


def _seed(db_path: str, quote_id: str = "Q-ov-001", *, buyer_name=None,
          buyer_phone=None, ship_to_address=None) -> None:
    write_quote(db_path, _make_quote(quote_id), actor="seed")
    write_email_contract(db_path, _make_contract(
        quote_id, buyer_name=buyer_name, buyer_phone=buyer_phone,
        ship_to_address=ship_to_address,
    ))


def test_override_adds_missing_buyer_fields_to_latest_contract(
    client: FlaskClient, db_path: str,
):
    """Operator fills in 3 fields the parser missed; find_contract_for_quote
    returns the corrected contract."""
    _seed(db_path)
    r = client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={
            "actor": "mike",
            "buyer_name": "Marc Argarin",
            "buyer_phone": "916-691-2719",
            "ship_to_address": "100 Prison Rd\nFolsom, CA 95671",
        },
    )
    assert r.status_code == 200, r.get_json()
    body = r.get_json()
    assert body["ok"] is True
    assert body["prior_contract_id"].startswith("contract_Q-ov-001_")
    assert "_op" in body["new_contract_id"]
    assert "mike" in body["new_contract_id"]
    assert sorted(body["fields_set"]) == ["buyer_name", "buyer_phone", "ship_to_address"]

    latest = find_contract_for_quote(db_path, "Q-ov-001")
    assert latest.buyer_name == "Marc Argarin"
    assert latest.buyer_phone == "916-691-2719"
    assert "100 Prison Rd" in (latest.ship_to_address or "")


def test_override_preserves_unmodified_fields_from_prior(
    client: FlaskClient, db_path: str,
):
    """Fields the operator does NOT touch must carry forward verbatim
    from the prior contract (so a partial override doesn't silently
    blank required_forms or solicitation_number)."""
    _seed(db_path, buyer_name="Old Name")
    r = client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={"actor": "mike", "buyer_phone": "555-555-5555"},
    )
    assert r.status_code == 200, r.get_json()

    latest = find_contract_for_quote(db_path, "Q-ov-001")
    assert latest.buyer_phone == "555-555-5555"
    # Unmodified fields carried forward.
    assert latest.buyer_name == "Old Name"
    assert latest.solicitation_number == "10847457"
    assert latest.source_email_id == "msgORIG"
    assert "703b" in latest.required_forms
    assert "704b" in latest.required_forms


def test_override_returns_404_when_no_prior_contract(
    client: FlaskClient, db_path: str,
):
    """An override without a prior contract is meaningless — the
    endpoint refuses 404 rather than fabricating a contract from
    operator input alone."""
    write_quote(db_path, _make_quote("Q-orphan"), actor="seed")
    r = client.post(
        "/spine/quotes/Q-orphan/contract-override",
        json={"actor": "mike", "buyer_name": "X"},
    )
    assert r.status_code == 404
    body = r.get_json()
    assert body["error"] == "no_contract"


def test_override_rejects_unknown_field_name(
    client: FlaskClient, db_path: str,
):
    """Boundary validation: typos in field names fail at the API edge,
    not silently as no-ops."""
    _seed(db_path)
    r = client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={"actor": "mike", "byer_name": "X"},  # typo
    )
    assert r.status_code == 400
    body = r.get_json()
    assert "byer_name" in body["detail"]


def test_override_blocks_provenance_field_writes(
    client: FlaskClient, db_path: str,
):
    """Operator must NOT overwrite contract_id, source_email_id, rfq_id,
    ingested_at — those are substrate-managed."""
    _seed(db_path)
    for forbidden in ("contract_id", "rfq_id", "source_email_id",
                      "ingested_at", "ingest_parser_version"):
        r = client.post(
            "/spine/quotes/Q-ov-001/contract-override",
            json={"actor": "mike", forbidden: "xxx"},
        )
        assert r.status_code == 400, f"{forbidden} should be rejected"


def test_override_requires_actor(
    client: FlaskClient, db_path: str,
):
    """Every operator correction is attributed; empty actor → 400."""
    _seed(db_path)
    r = client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={"buyer_name": "X"},
    )
    assert r.status_code == 400
    body = r.get_json()
    assert "actor" in body["detail"]


def test_override_chain_remains_shallow_on_repeated_corrections(
    client: FlaskClient, db_path: str,
):
    """If operator overrides twice, the contract_id chain doesn't
    accumulate `_op` nesting — second override's new_id derives from
    the original base, not the prior override's id."""
    _seed(db_path)
    r1 = client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={"actor": "mike", "buyer_name": "A"},
    )
    assert r1.status_code == 200
    first_new_id = r1.get_json()["new_contract_id"]

    r2 = client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={"actor": "mike", "buyer_phone": "1"},
    )
    assert r2.status_code == 200
    second_new_id = r2.get_json()["new_contract_id"]

    # Both derive from same base — exactly one `_op` segment each.
    assert first_new_id.count("_op") == 1
    assert second_new_id.count("_op") == 1
    assert first_new_id != second_new_id


def test_override_persists_through_to_render_path(
    client: FlaskClient, db_path: str,
):
    """End-to-end: after override, find_contract_for_quote (which is
    what every renderer route calls) returns the corrected contract.
    This is the substrate guarantee — no per-renderer wiring needed."""
    _seed(db_path)
    client.post(
        "/spine/quotes/Q-ov-001/contract-override",
        json={
            "actor": "mike",
            "buyer_name": "Marc Argarin",
            "ship_to_address": "100 Prison Rd\nFolsom, CA 95671",
        },
    )
    contract = find_contract_for_quote(db_path, "Q-ov-001")
    assert contract.buyer_name == "Marc Argarin"
    assert "100 Prison Rd" in contract.ship_to_address
