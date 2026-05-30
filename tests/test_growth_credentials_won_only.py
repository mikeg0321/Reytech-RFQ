"""ISSUE-5 (2026-05-29 audit) — get_reytech_credentials must count WON only.

Pre-fix it summed EVERY quote in quotes_log.json (won + lost + pending) into
total_sales and counted un-normalized agency strings, so outbound sales
emails claimed ~$4.86M "sales" that included lost/pending deals across an
inflated 179 "agencies". This pins the honest behavior: only status='won',
is_test=0 quotes count, and agency names are canonicalized before counting.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.agents import growth_agent


def _make_quotes_db(path):
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_number TEXT, created_at TEXT, agency TEXT, institution TEXT,
            total REAL DEFAULT 0, items_count INTEGER DEFAULT 0,
            status TEXT, is_test INTEGER DEFAULT 0
        )
    """)
    rows = [
        # status, agency, total, items_count, is_test
        ("won",     "CIW - California Institution for Women", 1000.0, 3, 0),
        ("won",     "CIW California Institution For Women",    500.0, 2, 0),  # variant of above
        ("won",     "CA Dept of Veterans Affairs",            2000.0, 1, 0),  # calvet
        ("lost",    "CDCR",                                  99999.0, 9, 0),  # must NOT count
        ("pending", "CDCR",                                  88888.0, 9, 0),  # must NOT count
        ("won",     "CDCR",                                   750.0, 4, 1),  # is_test → skip
    ]
    for i, (st, ag, tot, ic, test) in enumerate(rows):
        conn.execute(
            "INSERT INTO quotes (quote_number, created_at, agency, total, "
            "items_count, status, is_test) VALUES (?,?,?,?,?,?,?)",
            (f"R26Q{i}", "2023-04-01T10:00:00", ag, tot, ic, st, test),
        )
    conn.commit()
    conn.close()


@pytest.fixture
def won_only_db(tmp_path, monkeypatch):
    db = str(tmp_path / "reytech.db")
    _make_quotes_db(db)

    def fake_get_db():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        return c

    # growth_agent imports get_db inside the function: `from src.core.db import get_db`
    monkeypatch.setattr("src.core.db.get_db", fake_get_db)
    # Keep flat-file sources inert so only the DB drives the numbers.
    monkeypatch.setattr(growth_agent, "DATA_DIR", str(tmp_path))
    return db


def test_credentials_count_won_only(won_only_db):
    creds = growth_agent.get_reytech_credentials()
    # Only the 3 non-test WON quotes: 1000 + 500 + 2000 = 3500
    assert creds["total_sales"] == 3500.0
    assert creds["total_pos"] == 3
    # lost ($99,999) and pending ($88,888) excluded
    assert creds["total_sales"] < 4000


def test_credentials_normalize_agencies(won_only_db):
    creds = growth_agent.get_reytech_credentials()
    # The two CIW spellings collapse to one canonical agency; +CalVet = 2 total
    assert creds["agencies_served"] == 2, creds["agency_list"]


def test_credentials_calvet_split(won_only_db):
    creds = growth_agent.get_reytech_credentials()
    assert creds["calvet_amount"] == 2000.0
    assert creds["calvet_pos"] == 1


def test_credentials_items_won_only(won_only_db):
    creds = growth_agent.get_reytech_credentials()
    # 3 + 2 + 1 = 6 items across the won quotes (lost/pending/test excluded)
    assert creds["total_items"] == 6
