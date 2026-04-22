"""BUILD-5 P1 regression guard — award_tracker → oracle_calibration
end-to-end chain.

Context: the existing award_tracker tests cover two halves of the
chain but never the whole thing end to end:

  tests/test_award_tracker_calibrate_on_loss.py — 16 pure-helper unit
    tests (loss-reason mapping, winner_prices shape, calibrate-gate
    logic). None hit the DB.

  tests/test_award_tracker_integration.py — wiring tests that spy on
    `calibrate_from_outcome` with a MagicMock. They prove the function
    is *called* with the right args but never prove that the call
    actually *writes* to `oracle_calibration`.

The gap: a regression that breaks `calibrate_from_outcome` internally
(schema drift on oracle_calibration, a silently-caught Exception in
the upsert, the item-category classifier returning None, etc.) would
never be caught. This test wires the DB through end to end so the
oracle_calibration table row is the assertion surface.

Strategy:
  - Seed a real sqlite DB with a sent quote (5 days ago, past the
    MIN_DAYS_AFTER_SENT cutoff)
  - Monkeypatch FiscalSession to return a canned "competitor won" PO
  - Monkeypatch both paths.DATA_DIR and src.core.db.DB_PATH so the
    calibration upsert lands in the test DB
  - DO NOT mock `calibrate_from_outcome` — let it execute for real
  - Run `run_award_check(force=True)` once
  - Query `oracle_calibration` and assert loss_on_price=1, agency=CDCR,
    avg_losing_delta updated from the winner_prices signal
"""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def _chain_db(tmp_path, monkeypatch):
    """Temp reytech.db with quotes + rfqs tables, seeded with one
    sent-5-days-ago CDCR quote whose line items match the canned PO."""
    db_path = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db_path))
    # Match production: WAL journaling lets calibrate_from_outcome's own
    # connection write while award_tracker's conn is mid-transaction.
    # Without this the two serialize on the default rollback journal and
    # calibrate_from_outcome gets "database is locked" inside the try/except,
    # silently swallowing the Oracle write.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
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
    # Priced at cost * 1.80 so there IS headroom for a winner price to
    # undercut us — avg_losing_delta needs comp_price < our_price to
    # produce a positive delta signal.
    line_items = json.dumps([
        {"description": "Blood pressure cuff adult", "qty": 5,
         "unit_price": 45.00, "cost": 25.00, "margin_pct": 45.0},
        {"description": "Stethoscope dual-head", "qty": 2,
         "unit_price": 120.00, "cost": 70.00, "margin_pct": 41.7},
    ])
    conn.execute("""
        INSERT INTO quotes (id, quote_number, agency, institution, total,
                            line_items, sent_at, created_at, contact_email,
                            status, is_test)
        VALUES (?,?,?,?,?,?, datetime('now', '-5 days'),
                              datetime('now', '-7 days'),
                ?, 'sent', 0)
    """, ("q-CHAIN-1", "Q26T777", "CDCR", "CDCR HQ", 465.00,
          line_items, "buyer@cdcr.ca.gov"))
    conn.commit()
    conn.close()

    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    return str(db_path)


def _canned_competitor_po() -> list[dict]:
    """SCPRS-style PO payload where Acme beat us by ~10% on both lines."""
    return [{
        "po_number": "CDCR-2026-CHAIN",
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


def test_loss_detected_writes_oracle_calibration_row(_chain_db, monkeypatch):
    """End-to-end: a SCPRS loss must land a real row in oracle_calibration.

    This is the test the existing spy-based integration tests can't
    write — they stop at the calibrate_from_outcome call boundary.
    Without this guard, schema drift on oracle_calibration, a swallowed
    Exception in the upsert, or a silent change to the item category
    classifier would all ship to prod undetected.
    """
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
    monkeypatch.setattr("src.agents.award_tracker.time.sleep", lambda *_: None)

    from src.agents.award_tracker import run_award_check
    result = run_award_check(force=True)
    assert result.get("ok") is not False, f"run_award_check errored: {result}"

    # Query the oracle_calibration table directly. If calibrate_from_outcome
    # silently failed (swallowed exception, table not initialized, wrong
    # DB path), this query returns nothing and the test fails loudly.
    conn = sqlite3.connect(_chain_db)
    try:
        rows = conn.execute(
            "SELECT category, agency, sample_size, win_count, "
            "loss_on_price, loss_on_other, avg_losing_delta, "
            "recommended_max_markup "
            "FROM oracle_calibration WHERE agency=?",
            ("CDCR",)
        ).fetchall()
    finally:
        conn.close()

    assert rows, (
        "BUILD-5: oracle_calibration has NO row for CDCR after the SCPRS "
        "loss chain ran. Either calibrate_from_outcome never fired "
        "(check award_tracker gate logic), the DB_PATH monkeypatch "
        "didn't take, or the upsert raised and got swallowed. Without "
        "this, the oracle never learns from the runtime loss signal."
    )

    # Collapse rows into a single loss snapshot (stethoscope + cuff may
    # classify to different categories, but both should contribute).
    total_losses = sum(r[4] for r in rows)  # loss_on_price column
    total_samples = sum(r[2] for r in rows)  # sample_size column
    total_wins = sum(r[3] for r in rows)  # win_count column

    assert total_losses >= 1, (
        f"BUILD-5: expected loss_on_price >= 1, got rows={rows}. The "
        f"calibrate call reached the DB but the loss branch didn't "
        f"increment — loss_reason probably wasn't 'price'."
    )
    assert total_wins == 0, (
        "BUILD-5: no wins in this scenario — win_count must stay 0. "
        "A non-zero count means the outcome=='won' branch fired on "
        "what should be a 'lost' record."
    )
    assert total_samples == total_losses, (
        f"BUILD-5: sample_size ({total_samples}) should equal "
        f"loss_on_price ({total_losses}) when there are no wins."
    )

    # At least one row must have a non-zero avg_losing_delta — that's
    # the proof that winner_prices threaded through. Without it,
    # avg_losing_delta stays at the default 0.0 and the oracle never
    # learns HOW FAR above market we were.
    any_delta_updated = any(abs(r[6]) > 0.01 for r in rows)
    assert any_delta_updated, (
        f"BUILD-5: avg_losing_delta is still 0 across all rows ({rows}). "
        f"winner_prices didn't reach the EMA — check the "
        f"_winner_prices_from_analysis helper and the calibrate call "
        f"site's winner_prices= kwarg."
    )


def test_second_run_does_not_double_write_calibration_row(
    _chain_db, monkeypatch
):
    """The already_matched gate in award_tracker keeps calibrate from
    re-firing on the same (quote, PO) pair. If it regresses, the
    oracle_calibration sample_size for this match would climb by 1 on
    every 8h poll instead of staying at 1. Lock that too."""
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

    from src.agents.award_tracker import run_award_check
    run_award_check(force=True)
    run_award_check(force=True)

    conn = sqlite3.connect(_chain_db)
    try:
        rows = conn.execute(
            "SELECT category, sample_size, loss_on_price "
            "FROM oracle_calibration WHERE agency=?",
            ("CDCR",)
        ).fetchall()
    finally:
        conn.close()

    total_samples = sum(r[1] for r in rows)
    total_losses = sum(r[2] for r in rows)
    # Both items bucket to the same category (or two at most), each
    # should have sample_size==1 / loss_on_price==1. The invariant:
    # totals must match a single-run total, not double it.
    assert total_samples <= 2, (
        f"BUILD-5: sample_size summed to {total_samples} after two "
        f"run_award_check calls — the already_matched guard let the "
        f"second run re-calibrate. rows={rows}"
    )
    assert total_losses <= 2, (
        f"BUILD-5: loss_on_price summed to {total_losses} — double-"
        f"counted. rows={rows}"
    )
