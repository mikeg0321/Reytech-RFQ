"""Tests for _build_oracle_calibration_card — the /health/quoting surface
for the Oracle feedback loop.

Locks the 4-state traffic-light semantics:
  no_data       → grey  (rows == 0)
  stale         → red   (last_updated > 14 days ago)
  losses_only   → amber (wins == 0 && losses > 0)
  healthy       → green (wins > 0 and fresh)

Without these, a silent drift (e.g., stale threshold moves to 30 days
because someone thought the weekly cadence changed) would quietly hide
a stuck feedback loop — the exact observability gap product-engineer
flagged on PR #279.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest


@pytest.fixture
def _seeded_db(tmp_path, monkeypatch):
    """Tmp reytech.db with an empty oracle_calibration table. Individual
    tests INSERT the specific rows their scenario needs."""
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

    monkeypatch.setattr("src.core.paths.DATA_DIR", str(tmp_path))
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    return str(db_path)


def _insert_cal(db_path: str, *, category: str, agency: str,
                wins: int, losses_price: int, losses_other: int,
                last_updated: str, sample_size: int | None = None):
    conn = sqlite3.connect(db_path)
    if sample_size is None:
        sample_size = wins + losses_price + losses_other
    conn.execute("""
        INSERT INTO oracle_calibration
            (category, agency, sample_size, win_count, loss_on_price,
             loss_on_other, last_updated)
        VALUES (?,?,?,?,?,?,?)
    """, (category, agency, sample_size, wins, losses_price,
          losses_other, last_updated))
    conn.commit()
    conn.close()


def test_no_data_status_when_table_empty(_seeded_db):
    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["status"] == "no_data"
    assert card["rows"] == 0
    assert card["wins"] == 0
    assert card["losses_total"] == 0
    assert card["win_rate_pct"] is None, (
        "Win rate must be None (not 0.0) when there's no signal — "
        "the template distinguishes '--' from '0%'"
    )


def test_stale_status_when_last_update_over_14_days(_seeded_db):
    # 20-day-old update = stuck loop; stale threshold is >14 days.
    old = (datetime.now() - timedelta(days=20)).isoformat()
    _insert_cal(_seeded_db, category="medical", agency="CDCR",
                wins=3, losses_price=5, losses_other=1, last_updated=old)

    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["status"] == "stale"
    assert card["is_stale"] is True
    assert card["days_since_update"] >= 14


def test_losses_only_status_when_wins_zero(_seeded_db):
    # The exact "homepage zeros" shape that kicked off this work:
    # 0 wins / N losses with a recent update = wins aren't being
    # captured even though losses are.
    fresh = (datetime.now() - timedelta(days=1)).isoformat()
    _insert_cal(_seeded_db, category="medical", agency="CDCR",
                wins=0, losses_price=10, losses_other=2, last_updated=fresh)

    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["status"] == "losses_only"
    assert card["wins"] == 0
    assert card["losses_total"] == 12
    assert card["win_rate_pct"] == 0.0


def test_healthy_status_when_wins_and_fresh(_seeded_db):
    fresh = (datetime.now() - timedelta(hours=6)).isoformat()
    _insert_cal(_seeded_db, category="medical", agency="CDCR",
                wins=5, losses_price=10, losses_other=2, last_updated=fresh)
    _insert_cal(_seeded_db, category="safety", agency="CalVet",
                wins=2, losses_price=1, losses_other=0, last_updated=fresh)

    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["status"] == "healthy"
    assert card["rows"] == 2
    assert card["wins"] == 7
    assert card["losses_price"] == 11
    assert card["losses_other"] == 2
    assert card["losses_total"] == 13
    # 7 wins / (7 + 13) = 35.0%
    assert card["win_rate_pct"] == 35.0
    assert card["agencies"] == 2
    assert card["is_stale"] is False


def test_stale_wins_as_stale_not_healthy(_seeded_db):
    # Wins present but stale: stale status wins — a locked-up loop is
    # more important to surface than the historical win count.
    old = (datetime.now() - timedelta(days=30)).isoformat()
    _insert_cal(_seeded_db, category="medical", agency="CDCR",
                wins=5, losses_price=10, losses_other=2, last_updated=old)

    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["status"] == "stale"


def test_boundary_14_days_is_not_stale(_seeded_db):
    # Stale threshold is strictly > 14; 14 days exactly = not stale.
    # Defensive: this boundary kicked off a false-alarm incident in
    # the weekly report before.
    thirteen_days = (datetime.now() - timedelta(days=13, hours=1)).isoformat()
    _insert_cal(_seeded_db, category="medical", agency="CDCR",
                wins=3, losses_price=2, losses_other=1,
                last_updated=thirteen_days)

    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["status"] == "healthy"
    assert card["is_stale"] is False


def test_handles_malformed_last_updated_gracefully(_seeded_db):
    # Writer bug: last_updated is a non-ISO string. Must not crash,
    # must not falsely mark stale.
    _insert_cal(_seeded_db, category="medical", agency="CDCR",
                wins=3, losses_price=2, losses_other=1,
                last_updated="not-a-date")

    from src.api.modules.routes_health import _build_oracle_calibration_card
    card = _build_oracle_calibration_card()
    assert card["days_since_update"] is None
    assert card["is_stale"] is None
    # Falls through to healthy because is_stale is None (not True)
    assert card["status"] == "healthy"
