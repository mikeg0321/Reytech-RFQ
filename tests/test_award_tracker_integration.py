"""Integration test for award_tracker.run_award_check → Oracle calibration.

The 16 unit tests in test_award_tracker_calibrate_on_loss.py lock the pure
helpers in isolation. This file locks the WIRING — that run_award_check
actually calls calibrate_from_outcome with the right shape when a SCPRS
match turns up a competitor-won PO for a sent quote.

Concretely covers the P1 gap flagged in the PR #279 product-engineer
review: silent regressions to `analysis` shape, `our_items` descriptions,
the try/except swallowing a real TypeError, or the gate logic drifting
would never be caught by the pure unit tests.

Strategy:
  - Seed a real sqlite DB with a sent quote row
  - Monkeypatch FiscalSession to return a canned "competitor won" PO
    with line-by-line pricing
  - Monkeypatch calibrate_from_outcome to a spy so we can assert the
    call args without touching the real Oracle EMA
  - Run run_award_check(force=True) and verify the spy fired once with
    outcome="lost", loss_reason="price", and winner_prices populated
"""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def _seeded_db(tmp_path, monkeypatch):
    """Build a temp reytech.db with the quotes + award-tracker tables
    needed by run_award_check, seeded with one sent-2-days-ago quote."""
    db_path = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE quotes (
            id TEXT PRIMARY KEY,
            quote_number TEXT,
            agency TEXT,
            institution TEXT,
            total REAL,
            line_items TEXT,
            items_text TEXT,
            sent_at TEXT,
            created_at TEXT,
            contact_email TEXT,
            contact_name TEXT,
            source_pc_id TEXT,
            status TEXT,
            status_notes TEXT,
            close_reason TEXT,
            closed_by_agent TEXT,
            updated_at TEXT,
            is_test INTEGER DEFAULT 0
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            rfq_number TEXT,
            agency TEXT,
            institution TEXT,
            total REAL,
            items TEXT,
            sent_at TEXT,
            received_at TEXT,
            requestor_email TEXT,
            requestor_name TEXT,
            status TEXT
        );
    """)
    line_items = json.dumps([
        {"description": "Blood pressure cuff adult", "qty": 5,
         "unit_price": 45.00, "cost": 25.00, "margin_pct": 45.0},
        {"description": "Stethoscope dual-head", "qty": 2,
         "unit_price": 120.00, "cost": 70.00, "margin_pct": 41.7},
    ])
    # sent_at 5 days ago — past the MIN_DAYS_AFTER_SENT=2 cutoff
    conn.execute("""
        INSERT INTO quotes (id, quote_number, agency, institution, total,
                            line_items, sent_at, created_at, contact_email,
                            status, is_test)
        VALUES (?,?,?,?,?,?, datetime('now', '-5 days'),
                              datetime('now', '-7 days'),
                ?, 'sent', 0)
    """, ("q-INT-1", "Q26T001", "CDCR", "CDCR HQ", 465.00,
          line_items, "buyer@cdcr.ca.gov"))
    conn.commit()
    conn.close()

    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    return str(db_path)


def _canned_competitor_po() -> list[dict]:
    """SCPRS-style PO payload: Acme beat us by ~10% across both lines.

    Fields shaped to clear _match_quote_to_po's 0.55 confidence threshold:
      - dept matches CDCR pattern   → score += 0.35 (agency_match)
      - amount ratio 0.903          → score += 0.25 (amount_close)
      - first_item shares 2+ words  → score += 0.20 (items_overlap)
      Total: 0.80 ≥ 0.55
    """
    return [{
        "po_number": "CDCR-2026-00999",
        "supplier_name": "Acme Medical Supply",
        "grand_total_num": 420.00,
        "dept": "CDCR HEADQUARTERS",
        "first_item": "Blood pressure cuff adult",
        "description": "Blood pressure cuff adult",
        "award_date": "04/15/2026",
        "ordering_agency": "CDCR",
        "line_items": [
            {"description": "Blood pressure cuff adult",
             "quantity": 5, "unit_price": 38.00},
            {"description": "Stethoscope dual-head",
             "quantity": 2, "unit_price": 115.00},
        ],
        "_results_html": "",
        "_row_index": 0,
        "_click_action": "",
    }]


def test_scprs_loss_fires_calibrate_once_with_winner_prices(
    _seeded_db, monkeypatch
):
    """The whole contract in one test: sent quote + SCPRS competitor hit
    → calibrate_from_outcome called exactly once with loss_reason='price'
    and winner_prices reflecting SCPRS line data."""
    # Stub FiscalSession — no real scraping
    session_mock = MagicMock()
    session_mock.init_session.return_value = True
    session_mock.search.return_value = _canned_competitor_po()
    session_mock.get_detail.return_value = {
        "line_items": _canned_competitor_po()[0]["line_items"],
    }
    monkeypatch.setattr(
        "src.agents.scprs_lookup.FiscalSession",
        lambda: session_mock,
    )

    # Skip the 1.2s rate-limit sleeps inside run_award_check
    monkeypatch.setattr("src.agents.award_tracker.time.sleep", lambda *_: None)

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as calibrate_spy:
        from src.agents.award_tracker import run_award_check
        result = run_award_check(force=True)

    assert result.get("ok") is not False, f"run_award_check errored: {result}"

    # The core contract: calibrate fired on the loss signal.
    assert calibrate_spy.call_count >= 1, (
        "calibrate_from_outcome never fired — the SCPRS→Oracle wiring is "
        "broken. Either the match didn't register, the already_matched / "
        "match_stored gate rejected it, or the import inside the try "
        "block silently failed."
    )
    # If multiple keywords matched, the idempotency guard should have
    # kept calibrate to exactly one fire.
    assert calibrate_spy.call_count == 1, (
        f"Expected exactly 1 calibrate call (idempotency via "
        f"already_matched + match_stored gate), got {calibrate_spy.call_count}"
    )

    args, kwargs = calibrate_spy.call_args
    # outcome is positional arg[1]
    assert args[1] == "lost"
    assert kwargs.get("loss_reason") == "price", (
        "Acme undercut us on price — loss_reason should map to 'price' so "
        "avg_losing_delta EMA updates. Got: " + repr(kwargs.get("loss_reason"))
    )
    assert kwargs.get("agency") == "CDCR"
    # winner_prices should be present so the EMA gets real signal, not
    # just a loss_on_price counter bump
    assert kwargs.get("winner_prices"), (
        "winner_prices was None/empty — avg_losing_delta won't update and "
        "we won't learn how far we were above market."
    )


def test_second_run_does_not_double_calibrate_same_match(
    _seeded_db, monkeypatch
):
    """award_tracker runs every 8h. A second run against the same SCPRS
    match must NOT re-fire calibrate — the already_matched guard covers
    this (quote_po_matches row from the first run short-circuits)."""
    session_mock = MagicMock()
    session_mock.init_session.return_value = True
    session_mock.search.return_value = _canned_competitor_po()
    session_mock.get_detail.return_value = {
        "line_items": _canned_competitor_po()[0]["line_items"],
    }
    monkeypatch.setattr(
        "src.agents.scprs_lookup.FiscalSession",
        lambda: session_mock,
    )
    monkeypatch.setattr("src.agents.award_tracker.time.sleep", lambda *_: None)

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as calibrate_spy:
        from src.agents.award_tracker import run_award_check
        run_award_check(force=True)
        first_count = calibrate_spy.call_count
        run_award_check(force=True)
        second_count = calibrate_spy.call_count

    assert first_count == 1, f"First run should fire calibrate once; got {first_count}"
    assert second_count == first_count, (
        f"Second run double-counted the same loss — already_matched guard "
        f"failed. First run fires: {first_count}, second: {second_count}"
    )
