"""
Tests for /outreach/next "Contact Today" decision card and its
draft-generation API.

The page is the consolidated outreach view that ranks prospects by
prospect_scorer score, attaches concrete why-to-contact (top gap +
top win-back item per agency), and provides 1-click draft buttons.
"""
import sqlite3
from unittest.mock import patch

import pytest


@pytest.fixture
def seeded_outreach_db(tmp_path, monkeypatch):
    """Seed scprs tables + quotes so prospect_scorer has real input."""
    data_dir = tmp_path / "outreach_iso"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()

    db_path = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # quotes table — needed by prospect_scorer for existing/contacted lookups.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "status TEXT, source TEXT, created_at TEXT, is_test INTEGER DEFAULT 0)"
    )
    # email_outbox stub for the existing-drafts lookup.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS email_outbox ("
        "id INTEGER PRIMARY KEY, recipient TEXT, subject TEXT, "
        "status TEXT, created_at TEXT)"
    )

    # Seed 2 high-spend POs to one agency with a clear win-back + gap mix.
    cur = conn.execute(
        "INSERT INTO scprs_po_master "
        "(po_number, dept_code, dept_name, agency_code, supplier, "
        "grand_total, start_date, buyer_name, buyer_email, buyer_phone) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        ("PO-CCHCS-A", "4700", "CCHCS / Correctional Health", "4700",
         "Medline Industries", 12000.0, "2026-02-15",
         "Jane Buyer", "jane.buyer@cchcs.ca.gov", "555-0100"),
    )
    po_id = cur.lastrowid
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description, "
        "quantity, unit_price, line_total, reytech_sells, opportunity_flag) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (po_id, "PO-CCHCS-A", 0, "nitrile exam gloves medium box of 100",
         500, 8.00, 4000.0, 1, "WIN_BACK"),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description, "
        "quantity, unit_price, line_total, reytech_sells, opportunity_flag) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (po_id, "PO-CCHCS-A", 1, "ABD pads 5x9 sterile box of 25",
         320, 25.00, 8000.0, 0, "GAP_ITEM"),
    )

    # Second agency — lower priority but should still appear.
    cur = conn.execute(
        "INSERT INTO scprs_po_master "
        "(po_number, dept_code, dept_name, agency_code, supplier, "
        "grand_total, start_date, buyer_name, buyer_email) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        ("PO-CDCR-A", "5225", "CDCR / Corrections", "5225",
         "Henry Schein", 3500.0, "2026-01-10",
         "Bob Buyer", "bob@cdcr.ca.gov"),
    )
    po_id = cur.lastrowid
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, description, "
        "quantity, unit_price, line_total, reytech_sells, opportunity_flag) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (po_id, "PO-CDCR-A", 0, "disinfectant wipes container of 160",
         140, 25.00, 3500.0, 0, "GAP_ITEM"),
    )
    conn.commit()
    conn.close()
    return sup, db_path


def test_outreach_next_renders_top_prospects_with_why(
    auth_client, seeded_outreach_db, monkeypatch
):
    # Both prospect_scorer + the route helper resolve get_db by name at
    # call time. Monkeypatching the canonical src.core.db.get_db propagates
    # to both. (The route module is exec()'d into dashboard's namespace per
    # CLAUDE.md, so it's not a normal importable module.)
    sup, db_path = seeded_outreach_db
    from contextlib import contextmanager

    @contextmanager
    def _seeded_get_db():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr("src.core.db.get_db", _seeded_get_db)
    # prospect_scorer wraps get_db in its own _get_db helper.
    import src.agents.prospect_scorer as scorer
    monkeypatch.setattr(scorer, "_get_db", _seeded_get_db)

    resp = auth_client.get("/outreach/next")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")

    # Page header rendered.
    assert "Contact Today" in body
    assert "ranked by SCPRS score" in body

    # Highest-spend agency surfaces first.
    assert "CCHCS" in body
    # Buyer contact rendered.
    assert "Jane Buyer" in body or "jane.buyer@cchcs.ca.gov" in body

    # Why-block populated with concrete dollar amount + supplier name.
    assert "Medline" in body  # last_supplier of the win-back item
    assert "nitrile exam gloves" in body  # win-back item description
    assert "ABD pads" in body  # gap item description

    # Score breakdown rendered.
    assert "volume" in body and "recency" in body and "match" in body and "gap" in body

    # Both A and B draft buttons present (since buyer email exists).
    assert "Draft (price hook)" in body
    assert "Draft (relationship)" in body


def test_outreach_next_handles_empty_data_gracefully(
    auth_client, tmp_path, monkeypatch
):
    """No SCPRS data → page renders the 'no prospects' empty state, NOT
    a 500. Inverse-positive for the silent-fail shape from PR #484."""
    data_dir = tmp_path / "outreach_empty"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    import src.agents.prospect_scorer as scorer
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db_path = str(data_dir / "reytech.db")

    from contextlib import contextmanager

    @contextmanager
    def _empty_get_db():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr("src.core.db.get_db", _empty_get_db)
    monkeypatch.setattr(scorer, "_get_db", _empty_get_db)

    resp = auth_client.get("/outreach/next")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "No prospects scored yet" in body or "No SCPRS data" in body


def test_api_outreach_next_draft_validates_input(auth_client):
    # Missing buyer_email.
    resp = auth_client.post(
        "/api/outreach/next/draft", json={"strategy": "A"}
    )
    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False

    # Bad strategy.
    resp = auth_client.post(
        "/api/outreach/next/draft",
        json={"buyer_email": "x@y.com", "strategy": "Z"}
    )
    assert resp.status_code == 400


def test_api_outreach_next_draft_returns_generated_email(
    auth_client, monkeypatch
):
    """Mock the underlying generator and verify the wrapper passes through."""
    import src.agents.outreach_agent as oa
    monkeypatch.setattr(
        oa, "generate_outreach_email",
        lambda email, strategy="A": {
            "subject": f"Subject {strategy} for {email}",
            "body": "Hi there, this is the body.",
            "greeting": "Hi,",
            "buyer_email": email, "strategy": strategy,
        },
    )
    resp = auth_client.post(
        "/api/outreach/next/draft",
        json={"buyer_email": "test@cchcs.ca.gov", "strategy": "A"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["draft"]["subject"] == "Subject A for test@cchcs.ca.gov"
    assert data["draft"]["strategy"] == "A"


def test_api_outreach_next_draft_propagates_buyer_not_found(
    auth_client, monkeypatch
):
    """When generate_outreach_email returns an error dict, the API responds
    with 404 and the underlying error string."""
    import src.agents.outreach_agent as oa
    monkeypatch.setattr(
        oa, "generate_outreach_email",
        lambda email, strategy="A": {"error": f"Buyer {email} not found"},
    )
    resp = auth_client.post(
        "/api/outreach/next/draft",
        json={"buyer_email": "ghost@nowhere.gov", "strategy": "A"},
    )
    assert resp.status_code == 404
    data = resp.get_json()
    assert data["ok"] is False
    assert "ghost@nowhere.gov" in data["error"]
