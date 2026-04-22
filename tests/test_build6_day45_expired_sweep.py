"""BUILD-6 P1 regression guard — day-45 expired-no-match Oracle calibration.

Before BUILD-6, award_tracker's expire sweep marked quotes `status='expired'`
after 45 silent days but never calibrated the oracle. Net effect: the
calibration table only ever learned from SCPRS-matched losses, overstating
win-rate because quotes that went silent weren't counted as losses at all.

This test locks three invariants:
  1. `_should_calibrate_expired` / `_items_for_expired_calibration` helpers
     behave correctly (pure unit coverage).
  2. The expire branch in `run_award_check` actually calls
     `calibrate_from_outcome` when the UPDATE lands a row (source-level
     guard + E2E DB write).
  3. A second `run_award_check` after the quote has already flipped to
     `expired` does NOT re-fire calibrate (idempotency — the sent_quotes
     filter excludes it, plus `expired_stored` gates on rowcount).
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ─────────────────────────────────────────────────────────────────────
# Pure-helper unit tests
# ─────────────────────────────────────────────────────────────────────

def test_should_calibrate_expired_fires_when_update_stored():
    """The only condition: UPDATE quotes SET status='expired' changed a row."""
    from src.agents.award_tracker import _should_calibrate_expired
    assert _should_calibrate_expired(True) is True


def test_should_calibrate_expired_skips_when_update_no_op():
    """If the UPDATE matched zero rows (row already expired, DB locked,
    concurrent status change), calibrate MUST NOT fire — otherwise every
    8h poll would re-calibrate the same quote."""
    from src.agents.award_tracker import _should_calibrate_expired
    assert _should_calibrate_expired(False) is False


def test_items_for_expired_calibration_parses_line_items_json():
    from src.agents.award_tracker import _items_for_expired_calibration
    q = {"line_items": json.dumps([
        {"description": "Blood pressure cuff", "qty": 5, "unit_price": 45.0},
        {"description": "Stethoscope", "qty": 2, "unit_price": 120.0},
    ])}
    items = _items_for_expired_calibration(q)
    assert len(items) == 2
    assert items[0]["description"] == "Blood pressure cuff"


def test_items_for_expired_calibration_returns_empty_on_missing_column():
    """Empty string, None, malformed JSON must all fall through to [] so
    calibrate_from_outcome never runs with junk input."""
    from src.agents.award_tracker import _items_for_expired_calibration
    assert _items_for_expired_calibration({"line_items": ""}) == []
    assert _items_for_expired_calibration({"line_items": None}) == []
    assert _items_for_expired_calibration({"line_items": "not-json"}) == []
    assert _items_for_expired_calibration({}) == []


def test_items_for_expired_calibration_handles_non_dict_input():
    from src.agents.award_tracker import _items_for_expired_calibration
    assert _items_for_expired_calibration(None) == []
    assert _items_for_expired_calibration("a string") == []
    assert _items_for_expired_calibration([1, 2, 3]) == []


# ─────────────────────────────────────────────────────────────────────
# Source-level wiring guards
# ─────────────────────────────────────────────────────────────────────

def test_expire_branch_calls_should_calibrate_expired():
    """Source guard: a refactor that removes the gate call would silently
    regress back to 'never calibrate on expire'."""
    src = Path("src/agents/award_tracker.py").read_text(encoding="utf-8")
    assert "_should_calibrate_expired(expired_stored)" in src, (
        "BUILD-6: expire branch must invoke _should_calibrate_expired. "
        "Did someone delete the gate or rename the local expired_stored?"
    )


def test_expire_branch_calls_calibrate_from_outcome_with_loss_other():
    """Source guard: the calibrate call inside the expire branch must
    pass loss_reason='other' — not 'price' (we have no competitor data
    to prove we were high) and not winner_prices (same reason). This
    guarantees sample_size/loss_on_other increment without poisoning
    the avg_losing_delta EMA."""
    src = Path("src/agents/award_tracker.py").read_text(encoding="utf-8")
    # Search for the specific call shape used inside the expire block.
    assert re.search(
        r"calibrate_from_outcome\(\s*_exp_items,\s*\"lost\"",
        src,
    ), "BUILD-6: expire branch must call calibrate_from_outcome(_exp_items, 'lost', ...)"
    assert "loss_reason=\"other\"" in src, (
        "BUILD-6: expire-sweep calibrate must use loss_reason='other' "
        "(no price signal) — found different literal"
    )


def test_expire_update_checks_rowcount():
    """The idempotency guard requires distinguishing 'UPDATE matched' from
    'UPDATE no-op'. Only rowcount distinguishes the two."""
    src = Path("src/agents/award_tracker.py").read_text(encoding="utf-8")
    assert "cur.rowcount" in src, (
        "BUILD-6: expire UPDATE must check rowcount to gate calibrate. "
        "Without this, a no-op UPDATE would re-fire calibrate every 8h."
    )


# ─────────────────────────────────────────────────────────────────────
# E2E chain test — real DB, no mock on calibrate_from_outcome
# ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def _expired_chain_db(tmp_path, monkeypatch):
    """Temp reytech.db with one CDCR quote sent 50 days ago (past the
    45-day expire threshold), status='sent'."""
    db_path = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
        CREATE TABLE quotes (
            id TEXT PRIMARY KEY, quote_number TEXT, agency TEXT,
            institution TEXT, total REAL, line_items TEXT,
            items_text TEXT, sent_at TEXT, created_at TEXT,
            contact_email TEXT, contact_name TEXT, source_pc_id TEXT,
            status TEXT, status_notes TEXT, close_reason TEXT,
            closed_by_agent TEXT, updated_at TEXT,
            is_test INTEGER DEFAULT 0
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY, rfq_number TEXT, agency TEXT,
            institution TEXT, total REAL, items TEXT, sent_at TEXT,
            received_at TEXT, requestor_email TEXT, requestor_name TEXT,
            status TEXT
        );
    """)
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
        VALUES (?,?,?,?,?,?, datetime('now', '-50 days'),
                              datetime('now', '-52 days'),
                ?, 'sent', 0)
    """, ("q-EXP-1", "Q26T888", "CDCR", "CDCR HQ", 465.00,
          line_items, "buyer@cdcr.ca.gov"))
    conn.commit()
    conn.close()

    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    return str(db_path)


def test_day45_expired_sweep_writes_oracle_calibration_row(
    _expired_chain_db, monkeypatch
):
    """End-to-end: a sent-50-days-ago quote with no SCPRS match must
    flip to status='expired' AND increment oracle_calibration.loss_on_other
    for (general, CDCR). Without BUILD-6's wiring the calibration row
    never lands — the quote just silently expires."""
    session_mock = MagicMock()
    session_mock.init_session.return_value = True
    session_mock.search.return_value = []  # no SCPRS match
    session_mock.get_detail.return_value = {"line_items": []}
    monkeypatch.setattr(
        "src.agents.scprs_lookup.FiscalSession",
        lambda: session_mock,
    )
    monkeypatch.setattr("src.agents.award_tracker.time.sleep", lambda *_: None)

    # CRITICAL: don't pass force=True here — the expire sweep is gated on
    # `if not force`, so force=True skips the whole expire path and this
    # test would silently prove nothing.
    from src.agents.award_tracker import run_award_check
    result = run_award_check(force=False)
    assert result.get("ok") is not False, f"run_award_check errored: {result}"

    # Quote should be expired now
    conn = sqlite3.connect(_expired_chain_db)
    try:
        quote_row = conn.execute(
            "SELECT status, closed_by_agent FROM quotes WHERE quote_number=?",
            ("Q26T888",)
        ).fetchone()
    finally:
        conn.close()
    assert quote_row is not None
    assert quote_row[0] == "expired", (
        f"BUILD-6: quote should have flipped to 'expired', got {quote_row[0]}. "
        f"get_check_phase(sent_at=-50d) must return 'expired' for this test "
        f"to exercise the calibrate branch."
    )
    assert quote_row[1] == "award_tracker"

    # Oracle calibration row should exist for (CDCR, loss_on_other)
    conn = sqlite3.connect(_expired_chain_db)
    try:
        rows = conn.execute(
            "SELECT category, sample_size, loss_on_price, loss_on_other, "
            "win_count FROM oracle_calibration WHERE agency=?",
            ("CDCR",)
        ).fetchall()
    finally:
        conn.close()

    assert rows, (
        "BUILD-6: oracle_calibration has NO row for CDCR after the day-45 "
        "expire sweep ran. Without this calibration signal, the oracle's "
        "win-rate is systematically overstated — quotes that went silent "
        "don't count as losses."
    )
    total_other = sum(r[3] for r in rows)
    total_price = sum(r[2] for r in rows)
    total_wins = sum(r[4] for r in rows)
    assert total_other >= 1, (
        f"BUILD-6: expected loss_on_other >= 1 (expire sweep is a non-"
        f"price signal), got rows={rows}"
    )
    assert total_price == 0, (
        f"BUILD-6: expire sweep must NOT bump loss_on_price — there's no "
        f"competitor price to compare against. Got loss_on_price={total_price}"
    )
    assert total_wins == 0


def test_second_run_does_not_recalibrate_already_expired_quote(
    _expired_chain_db, monkeypatch
):
    """Once the quote is status='expired', the sent_quotes filter in
    run_award_check excludes it, so a second run can't re-fire calibrate.
    Lock that invariant at the DB level — sample_size must not climb."""
    session_mock = MagicMock()
    session_mock.init_session.return_value = True
    session_mock.search.return_value = []
    session_mock.get_detail.return_value = {"line_items": []}
    monkeypatch.setattr(
        "src.agents.scprs_lookup.FiscalSession",
        lambda: session_mock,
    )
    monkeypatch.setattr("src.agents.award_tracker.time.sleep", lambda *_: None)

    from src.agents.award_tracker import run_award_check
    run_award_check(force=False)

    # Snapshot sample_size after first run
    conn = sqlite3.connect(_expired_chain_db)
    try:
        first_samples = sum(r[0] for r in conn.execute(
            "SELECT sample_size FROM oracle_calibration WHERE agency=?",
            ("CDCR",)
        ).fetchall())
    finally:
        conn.close()

    # Second run — quote is already expired, should be filtered out
    run_award_check(force=False)

    conn = sqlite3.connect(_expired_chain_db)
    try:
        second_samples = sum(r[0] for r in conn.execute(
            "SELECT sample_size FROM oracle_calibration WHERE agency=?",
            ("CDCR",)
        ).fetchall())
    finally:
        conn.close()

    assert second_samples == first_samples, (
        f"BUILD-6: sample_size climbed from {first_samples} to {second_samples} "
        f"on the second run — expire sweep is re-calibrating an already-"
        f"expired quote. Check either the sent_quotes filter or the rowcount "
        f"gate on _should_calibrate_expired."
    )


def test_expired_sweep_does_not_fire_for_fresh_quote(
    tmp_path, monkeypatch
):
    """Boundary guard: a 10-day-old quote (well before day 45) must NOT
    trigger the expire branch, even if the rest of the chain runs."""
    db_path = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE quotes (
            id TEXT PRIMARY KEY, quote_number TEXT, agency TEXT,
            institution TEXT, total REAL, line_items TEXT,
            items_text TEXT, sent_at TEXT, created_at TEXT,
            contact_email TEXT, contact_name TEXT, source_pc_id TEXT,
            status TEXT, status_notes TEXT, close_reason TEXT,
            closed_by_agent TEXT, updated_at TEXT,
            is_test INTEGER DEFAULT 0
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY, rfq_number TEXT, agency TEXT,
            institution TEXT, total REAL, items TEXT, sent_at TEXT,
            received_at TEXT, requestor_email TEXT, requestor_name TEXT,
            status TEXT
        );
    """)
    conn.execute("""
        INSERT INTO quotes (id, quote_number, agency, institution, total,
                            line_items, sent_at, created_at, contact_email,
                            status, is_test)
        VALUES (?,?,?,?,?,?, datetime('now', '-10 days'),
                              datetime('now', '-12 days'),
                ?, 'sent', 0)
    """, ("q-FRESH-1", "Q26T777", "CDCR", "CDCR HQ", 465.00,
          json.dumps([{"description": "Blood pressure cuff", "qty": 1,
                       "unit_price": 45.00, "cost": 25.00}]),
          "buyer@cdcr.ca.gov"))
    conn.commit()
    conn.close()

    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))

    session_mock = MagicMock()
    session_mock.init_session.return_value = True
    session_mock.search.return_value = []
    session_mock.get_detail.return_value = {"line_items": []}
    monkeypatch.setattr(
        "src.agents.scprs_lookup.FiscalSession",
        lambda: session_mock,
    )
    monkeypatch.setattr("src.agents.award_tracker.time.sleep", lambda *_: None)

    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as spy:
        from src.agents.award_tracker import run_award_check
        run_award_check(force=False)

    assert spy.call_count == 0, (
        f"BUILD-6: calibrate fired {spy.call_count} time(s) on a 10-day-old "
        f"quote. The expire sweep must ONLY fire at day 45+."
    )

    # Quote still 'sent', not 'expired'
    conn = sqlite3.connect(str(db_path))
    try:
        status = conn.execute(
            "SELECT status FROM quotes WHERE quote_number=?", ("Q26T777",)
        ).fetchone()[0]
    finally:
        conn.close()
    assert status == "sent"
