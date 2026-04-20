"""Contract tests for pricing_oracle_v2.calibrate_order_shipped_once.

Purpose: runtime counterpart to scripts/backfill_wins_from_orders.py. When an
order's status transitions into a realized-win class (shipped / delivered /
invoiced / complete / completed), Oracle must see the win — exactly once per
order, whether the trigger fires from _update_order_status, from the backfill
script, or from PC mark-won's auto-order path. Ledger `backfill_wins_ledger`
is the shared dedupe key.

Locking the contract so nothing drifts:
  - First call for an order_id: fires calibrate_from_outcome + writes ledger
  - Second call for same order_id: no-op, returns False
  - Empty items: skip (same as backfill script — no signal to register)
  - Empty order_id: skip (defensive)
  - Shares the ledger with the backfill script — pre-existing ledger row
    means skip even on first in-process call
"""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest


@pytest.fixture
def _tmp_db(tmp_path, monkeypatch):
    """Point pricing_oracle_v2.DB_PATH at an empty tmp DB."""
    db_path = tmp_path / "reytech.db"
    sqlite3.connect(str(db_path)).close()
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    return str(db_path)


def _ledger_rows(db_path: str) -> list:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT order_id, agency, items_count FROM backfill_wins_ledger"
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def test_first_call_fires_calibrate_and_writes_ledger(_tmp_db):
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once
    items = [{"description": "Thing", "qty": 1, "unit_price": 50.0}]
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        fired = calibrate_order_shipped_once(
            "ORD-TEST-1", items, agency="CDCR", order_total=6408.24
        )
    assert fired is True
    assert m.call_count == 1
    args, kwargs = m.call_args
    assert args[0] == items
    assert args[1] == "won"
    assert kwargs.get("agency") == "CDCR"
    rows = _ledger_rows(_tmp_db)
    assert len(rows) == 1
    assert rows[0][0] == "ORD-TEST-1"
    assert rows[0][1] == "CDCR"
    assert rows[0][2] == 1


def test_second_call_for_same_order_is_noop(_tmp_db):
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once
    items = [{"description": "x", "qty": 1, "unit_price": 50.0}]
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc1 = calibrate_order_shipped_once("ORD-DUP", items, agency="CDCR")
        rc2 = calibrate_order_shipped_once("ORD-DUP", items, agency="CDCR")
    assert rc1 is True
    assert rc2 is False, "Second call for same order_id must not re-fire"
    assert m.call_count == 1, (
        f"calibrate_from_outcome must be called exactly once; got {m.call_count}"
    )


def test_empty_items_skipped(_tmp_db):
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        fired = calibrate_order_shipped_once("ORD-EMPTY", [], agency="CDCR")
    assert fired is False
    assert m.call_count == 0
    assert _ledger_rows(_tmp_db) == [], (
        "Empty-items skip must NOT write a ledger row — otherwise a later "
        "legitimate backfill with the real items would be blocked"
    )


def test_empty_order_id_skipped(_tmp_db):
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        fired = calibrate_order_shipped_once("", [{"description": "x"}])
    assert fired is False
    assert m.call_count == 0


def test_preexisting_ledger_row_blocks_first_call(_tmp_db):
    """PC mark-won writes a ledger row when it auto-creates an order — so
    the later order-ship transition must see that row and skip."""
    conn = sqlite3.connect(_tmp_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backfill_wins_ledger (
            order_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL,
            win_total REAL,
            agency TEXT,
            items_count INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO backfill_wins_ledger
            (order_id, processed_at, win_total, agency, items_count)
        VALUES (?, datetime('now'), ?, ?, ?)
    """, ("ORD-PC-WON", 100.0, "CDCR", 1))
    conn.commit()
    conn.close()

    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once
    items = [{"description": "x", "qty": 1, "unit_price": 50.0}]
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        fired = calibrate_order_shipped_once("ORD-PC-WON", items, agency="CDCR")
    assert fired is False, (
        "PC mark-won already called calibrate + wrote ledger; order-ship "
        "must not double-count by re-firing"
    )
    assert m.call_count == 0


def test_calibrate_failure_after_ledger_write_returns_false(_tmp_db):
    """If calibrate_from_outcome raises, the helper must return False so
    callers can log — but the ledger row is already written, which prevents
    endless retry storms on a persistent downstream bug."""
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once
    items = [{"description": "x", "qty": 1, "unit_price": 50.0}]
    with patch(
        "src.core.pricing_oracle_v2.calibrate_from_outcome",
        side_effect=RuntimeError("oracle offline"),
    ):
        fired = calibrate_order_shipped_once("ORD-FAIL", items, agency="CDCR")
    assert fired is False
    # Ledger still written — avoid retry storms on a persistent calibrate bug
    assert len(_ledger_rows(_tmp_db)) == 1
