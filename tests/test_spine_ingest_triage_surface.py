"""Pin: GET /spine/queue/low-confidence surfaces parse_confidence
ambiguous EmailContracts as a triage queue.

Pillar-1 / G3 (chrome MCP audit 2026-05-26 follow-on): contracts the
parser accepted but flagged as low/medium confidence were silently
shelved into spine_email_contracts with no operator surface. This
endpoint exposes them so they're actionable — either ship as-is,
edit + ship, or no-bid.

Companion to the existing /spine/queue/rejected route (rejected
emails the parser refused). Together they cover the full ingest-
triage view.

Tests pin:
  1. JSON shape (count, limit, confidence_levels, contracts list).
  2. Confidence filter — `?level=low` narrows the set.
  3. High-confidence contracts are NOT included (don't pollute the
     triage view).
  4. Sort order — newest ingested first.
  5. Empty result when no qualifying contracts exist.
  6. limit param validated (400 on out-of-range).
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_contract(contract_id, parse_confidence, ingested_minutes_ago=0,
                   rfq_id=None, sol="SOL-1", agency="CCHCS"):
    """Build a minimal EmailContract for tests."""
    from src.spine.email_contract import EmailContract, ContractLineItem
    from datetime import timedelta
    ingested_at = (datetime.now(timezone.utc)
                   - timedelta(minutes=ingested_minutes_ago))
    return EmailContract(
        contract_id=contract_id,
        rfq_id=rfq_id,
        agency=agency,
        facility="CHCF",
        solicitation_number=sol,
        line_items=[ContractLineItem(
            line_no=1, description="t", qty=1, uom="EA",
        )],
        parse_confidence=parse_confidence,
        ingested_at=ingested_at,
    )


def _seed(db_path, contracts):
    from src.spine.db import write_email_contract, init_db
    init_db(db_path)
    for c in contracts:
        write_email_contract(db_path, c)


def _client(db_path):
    from flask import Flask
    from src.api.modules.routes_spine import make_spine_blueprint
    app = Flask(__name__)
    app.config["TESTING"] = True
    bp = make_spine_blueprint(db_path)
    app.register_blueprint(bp)
    return app.test_client()


# ─── Shape + default filter ──────────────────────────────────────────


def test_endpoint_returns_expected_shape(tmp_path):
    from src.spine.db import init_db
    init_db(str(tmp_path / "spine.db"))
    c = _client(str(tmp_path / "spine.db"))
    r = c.get("/spine/queue/low-confidence")
    assert r.status_code == 200
    data = r.get_json()
    for k in ("count", "limit", "confidence_levels", "contracts"):
        assert k in data, f"missing {k}"
    assert data["count"] == 0
    assert data["contracts"] == []
    # Default surfaces both low + medium.
    assert set(data["confidence_levels"]) == {"low", "medium"}


def test_low_and_medium_included_high_excluded(tmp_path):
    db = str(tmp_path / "spine.db")
    _seed(db, [
        _make_contract("c-high",   "high",   sol="HIGH-1"),
        _make_contract("c-medium", "medium", sol="MED-1"),
        _make_contract("c-low",    "low",    sol="LOW-1"),
    ])
    c = _client(db)
    r = c.get("/spine/queue/low-confidence")
    data = r.get_json()
    contract_ids = {row["contract_id"] for row in data["contracts"]}
    assert "c-high" not in contract_ids, "high-confidence leaked into triage"
    assert "c-medium" in contract_ids
    assert "c-low" in contract_ids
    assert data["count"] == 2


def test_level_filter_narrows_to_one(tmp_path):
    db = str(tmp_path / "spine.db")
    _seed(db, [
        _make_contract("c-med", "medium", sol="MED-2"),
        _make_contract("c-lo",  "low",    sol="LOW-2"),
    ])
    c = _client(db)
    r = c.get("/spine/queue/low-confidence?level=low")
    data = r.get_json()
    ids = {row["contract_id"] for row in data["contracts"]}
    assert ids == {"c-lo"}
    assert data["confidence_levels"] == ["low"]


def test_sort_newest_first(tmp_path):
    """write_email_contract stamps ingested_at at WRITE time (not from
    the model), so we can't pre-control timestamps. Instead assert the
    semantic: rows come back in ingested_at-DESC order."""
    import time as _t
    from src.spine.db import init_db, write_email_contract
    db = str(tmp_path / "spine.db")
    init_db(db)
    # Write 3 contracts with small gaps so ingested_at differs.
    write_email_contract(db, _make_contract("c-first", "low", sol="F-1"))
    _t.sleep(0.05)
    write_email_contract(db, _make_contract("c-second", "low", sol="S-1"))
    _t.sleep(0.05)
    write_email_contract(db, _make_contract("c-third", "low", sol="T-1"))

    c = _client(db)
    r = c.get("/spine/queue/low-confidence")
    data = r.get_json()
    # Newest-first: third should precede second should precede first.
    ids_in_order = [row["contract_id"] for row in data["contracts"]]
    assert ids_in_order == ["c-third", "c-second", "c-first"], ids_in_order
    # And the ingested_at field on each row should monotonically
    # decrease — definition of newest-first.
    timestamps = [row["ingested_at"] for row in data["contracts"]]
    assert timestamps == sorted(timestamps, reverse=True)


def test_limit_param_validation(tmp_path):
    from src.spine.db import init_db
    db = str(tmp_path / "spine.db")
    init_db(db)
    c = _client(db)

    # Bad ints → 400
    r = c.get("/spine/queue/low-confidence?limit=abc")
    assert r.status_code == 400
    r = c.get("/spine/queue/low-confidence?limit=0")
    assert r.status_code == 400
    r = c.get("/spine/queue/low-confidence?limit=1001")
    assert r.status_code == 400

    # Valid bounds → 200
    r = c.get("/spine/queue/low-confidence?limit=1")
    assert r.status_code == 200
    r = c.get("/spine/queue/low-confidence?limit=1000")
    assert r.status_code == 200


def test_summary_fields_populated(tmp_path):
    """Each contract row must include the operator-facing summary
    fields needed for a triage UI (agency, sol#, due_date, etc)."""
    db = str(tmp_path / "spine.db")
    _seed(db, [
        _make_contract("c-tri", "low", sol="SOL-TRI",
                       rfq_id="rfq-triage-1", agency="CCHCS"),
    ])
    c = _client(db)
    r = c.get("/spine/queue/low-confidence")
    row = r.get_json()["contracts"][0]
    for k in ("contract_id", "rfq_id", "parse_confidence", "agency",
              "solicitation_number", "facility", "line_item_count",
              "ingested_at"):
        assert k in row, f"summary row missing {k}"
    assert row["parse_confidence"] == "low"
    assert row["agency"] == "CCHCS"
    assert row["solicitation_number"] == "SOL-TRI"
    assert row["rfq_id"] == "rfq-triage-1"
    assert row["line_item_count"] == 1
