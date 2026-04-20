"""End-to-end integration test for the Oracle feedback loop.

Unit tests cover each layer in isolation:
  - calibrate_order_shipped_once (ledger + calibrate)      — test_calibrate_order_shipped_once.py
  - _build_oracle_calibration_card (status logic)          — test_oracle_health_card.py
  - /api/health/quoting JSON shape                         — test_health_quoting_json_oracle.py
  - calibrate_from_outcome (EMA math)                      — test_oracle_v5_feedback.py
  - narrator honesty                                        — test_oracle_calibration_honesty.py
  - award_tracker → calibrate wiring                       — test_award_tracker_integration.py

What's NOT covered: the full chain. If someone changes the status
thresholds, drops the ledger write, or breaks the read path, a single
layer test may still pass but the observable behavior regresses. This
file locks the contract across all layers with one end-to-end flow:

  1. Seed oracle_calibration with the exact losses_only shape prod had
     at the start of today (2026-04-20): 0 wins / 47 losses, fresh
     update. Card status should read 'losses_only'.
  2. Fire calibrate_order_shipped_once for a shipped order — the
     runtime counterpart to the backfill script.
  3. Card status should flip to 'healthy' (wins > 0, is_stale = False).
  4. Second call with same order_id must be a no-op — ledger guards
     double-counting whether the caller is order-status-change,
     backfill script, or PC mark-won auto-order.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest


@pytest.fixture
def _oracle_db(tmp_path, monkeypatch):
    """Tmp reytech.db with oracle_calibration + backfill_wins_ledger.

    Paths patched so both the card helper and calibrate_order_shipped_once
    point at the same isolated DB — any write done via the calibrate
    path is readable by the card path in the same process.
    """
    db_path = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE oracle_calibration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            agency TEXT DEFAULT '',
            sample_size INTEGER DEFAULT 0,
            win_count INTEGER DEFAULT 0,
            loss_on_price INTEGER DEFAULT 0,
            loss_on_other INTEGER DEFAULT 0,
            avg_winning_margin REAL DEFAULT 25,
            avg_losing_delta REAL DEFAULT 0,
            recommended_max_markup REAL DEFAULT 30,
            competitor_floor REAL DEFAULT 0,
            last_updated TEXT,
            UNIQUE(category, agency)
        )
    """)
    conn.commit()
    conn.close()

    # Both layers need the same DB_PATH. src.core.db.DB_PATH is read by
    # calibrate_order_shipped_once; src.core.paths.DATA_DIR is read by
    # _build_oracle_calibration_card.
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    return str(db_path)


def _seed_losses_only_prod_shape(db_path: str) -> None:
    """Replicate the 'Homepage Zeros' state: 0 wins / 47 losses, fresh.
    Prod's 2026-04-20 morning shape — 4 agencies, losses carved up across
    medical/safety/IT categories. Card must read 'losses_only' here."""
    fresh = (datetime.now() - timedelta(hours=2)).isoformat()
    conn = sqlite3.connect(db_path)
    rows = [
        ("medical", "CDCR", 20, 0, 15, 5, fresh),
        ("medical", "CCHCS", 15, 0, 10, 5, fresh),
        ("safety", "CalVet", 8, 0, 5, 3, fresh),
        ("it", "CDCR", 4, 0, 3, 1, fresh),
    ]
    conn.executemany("""
        INSERT INTO oracle_calibration
            (category, agency, sample_size, win_count,
             loss_on_price, loss_on_other, last_updated)
        VALUES (?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()


def test_order_ship_flips_card_from_losses_only_to_healthy(_oracle_db):
    """The entire chain in one test: prod losses_only shape → order ship
    → calibrate → card flips to healthy.

    If this test fails, one of the six PRs (#277-#282) has regressed and
    /health/quoting will lie to the operator again."""
    from src.api.modules.routes_health import _build_oracle_calibration_card
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once

    _seed_losses_only_prod_shape(_oracle_db)

    # Before: card reports the Homepage Zeros state honestly.
    before = _build_oracle_calibration_card()
    assert before["status"] == "losses_only", (
        f"Card should flag losses_only before any win — got {before['status']}. "
        f"If this fails, the status logic has regressed (test_oracle_health_card.py "
        f"covers the threshold — check there first)."
    )
    assert before["wins"] == 0
    assert before["losses_total"] == 47

    # Fire the runtime win-path (what dashboard.py does on status →
    # 'shipped'). Real prod data: ORD-PO-4500750017 CDCR shipment.
    fired = calibrate_order_shipped_once(
        order_id="ORD-E2E-TEST",
        items=[
            {"description": "Blood pressure cuff adult", "qty": 5,
             "unit_price": 45.00, "cost": 25.00},
            {"description": "Stethoscope dual-head", "qty": 2,
             "unit_price": 120.00, "cost": 70.00},
        ],
        agency="CDCR",
        order_total=465.00,
    )
    assert fired is True, (
        "calibrate_order_shipped_once should fire on a fresh order_id. "
        "If False, the ledger dedupe is incorrectly blocking new orders "
        "(test_calibrate_order_shipped_once.py covers the happy path)."
    )

    # After: card reads the new win and flips to healthy.
    after = _build_oracle_calibration_card()
    assert after["status"] == "healthy", (
        f"Card should flip to healthy after win fires — got {after['status']}. "
        f"wins={after['wins']}, losses={after['losses_total']}. If this fails, "
        f"either calibrate didn't write to oracle_calibration (check calibrate_"
        f"from_outcome) or the card read path broke."
    )
    assert after["wins"] >= 1
    assert after["is_stale"] is False
    assert after["win_rate_pct"] is not None
    assert after["win_rate_pct"] > 0


def test_second_ship_same_order_does_not_double_count(_oracle_db):
    """Idempotency: order-status-change fires calibrate_order_shipped_once,
    but the same order can transition shipped→delivered→invoiced, firing
    the helper repeatedly. Ledger MUST block duplicate calibrate calls or
    Oracle over-counts wins and compresses margins falsely.

    Covers the exact contract shared with scripts/backfill_wins_from_orders.py
    — both write to the same backfill_wins_ledger."""
    from src.api.modules.routes_health import _build_oracle_calibration_card
    from src.core.pricing_oracle_v2 import calibrate_order_shipped_once

    _seed_losses_only_prod_shape(_oracle_db)

    items = [{"description": "Thing A", "qty": 1, "unit_price": 50.0,
              "cost": 30.0}]
    first = calibrate_order_shipped_once("ORD-DUP-TEST", items,
                                          agency="CDCR", order_total=50.0)
    wins_after_first = _build_oracle_calibration_card()["wins"]

    # Status transition: shipped → delivered. Dashboard fires helper again.
    second = calibrate_order_shipped_once("ORD-DUP-TEST", items,
                                           agency="CDCR", order_total=50.0)
    wins_after_second = _build_oracle_calibration_card()["wins"]

    assert first is True, "First fire should succeed"
    assert second is False, (
        "Second fire with same order_id must return False — ledger dedup. "
        "If True, backfill_wins_ledger check is broken and every order that "
        "transitions through multiple win-states gets double-counted."
    )
    assert wins_after_second == wins_after_first, (
        f"wins should not change on duplicate fire: "
        f"{wins_after_first} → {wins_after_second}"
    )
