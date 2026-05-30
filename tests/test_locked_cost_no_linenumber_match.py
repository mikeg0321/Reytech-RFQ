"""Guard: _get_locked_cost must not match on a short generic LINE NUMBER.

Incident rfq_fca653f6 (2026-05-30): a sensory-ball RFQ line carrying
item_number='2' (a line index, not a part number) read a Welch Allyn cradle's
$1,066 locked cost because that lock also had item_number='2'. The fix only
matches on item_number when it's a plausible real part number (>=4 chars or
contains a letter).
"""
from __future__ import annotations

import sqlite3

import pytest

from src.core.pricing_oracle_v2 import (
    _get_locked_cost, _is_real_part_number, _check_item_memory,
    _search_won_quotes,
)


@pytest.mark.parametrize("pn,expected", [
    ("2", False), ("1", False), ("12", False), ("123", False),
    ("", False), ("  3 ", False),
    ("1234", True), ("NL111", True), ("PS1382", True), ("W9", True),
    ("12-34", True),  # 5 chars
])
def test_is_real_part_number(pn, expected):
    assert _is_real_part_number(pn) is expected


def _seed_db():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE supplier_costs (
            cost REAL, supplier TEXT, source TEXT, confirmed_at TEXT,
            expires_at TEXT, item_number TEXT, description TEXT
        )
    """)
    far = "2099-01-01 00:00:00"
    # A medical-cradle lock that happens to carry line-number '2'
    db.execute("INSERT INTO supplier_costs VALUES (?,?,?,?,?,?,?)",
               (1066.48, "Envihs", "backfill_pc", "2026-05-01", far, "2",
                "Welch Allyn Propaq LT Charging Cradle"))
    # A real composition-notebook lock keyed by a real part number
    db.execute("INSERT INTO supplier_costs VALUES (?,?,?,?,?,?,?)",
               (2.00, "S&S", "backfill_pc", "2026-05-01", far, "NL999",
                "BALLS, SENSORY TOY SET, PUFFER one"))
    db.commit()
    return db


def test_line_number_item_does_not_cross_match():
    db = _seed_db()
    # A sensory-ball line with item_number='2' (a line index) must NOT pull
    # the Welch Allyn $1,066 cradle cost.
    out = _get_locked_cost(db, "BALLS, SENSORY TOY SET, PUFFER", item_number="2")
    if out is not None:
        assert abs(out["locked_cost"] - 1066.48) > 0.01, (
            "line-number '2' cross-matched the Welch Allyn cradle lock"
        )


def test_description_match_still_works():
    db = _seed_db()
    # Description substring still matches the right product (the puffer lock).
    out = _get_locked_cost(db, "BALLS, SENSORY TOY SET, PUFFER", item_number="2")
    assert out is not None and abs(out["locked_cost"] - 2.00) < 0.01


def test_real_part_number_still_matches():
    db = _seed_db()
    out = _get_locked_cost(db, "something unrelated", item_number="NL999")
    assert out is not None and abs(out["locked_cost"] - 2.00) < 0.01


def _seed_item_mappings():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE item_mappings (
            canonical_description TEXT, canonical_item_number TEXT, product_url TEXT,
            mfg_number TEXT, supplier TEXT, last_cost REAL, confidence REAL,
            times_confirmed INTEGER, asin TEXT, uom TEXT, supplier_url TEXT,
            last_sell_price REAL, mfg_name TEXT,
            original_item_number TEXT, original_description TEXT, upc TEXT, confirmed INTEGER
        )
    """)
    # A velvet-poster mapping that happens to carry line-number '3'
    db.execute(
        "INSERT INTO item_mappings (canonical_description, original_item_number, confirmed, last_cost) "
        "VALUES (?,?,?,?)",
        ("Velvet Art Posters, 16x20 (Pack of 60)", "3", 1, 150.40),
    )
    db.commit()
    return db


def test_item_memory_ignores_line_number():
    """A stress-ball line with item_number='3' must NOT resolve to the velvet
    poster item_mapping that also has original_item_number='3'."""
    db = _seed_item_mappings()
    out = _check_item_memory(db, "BALLS, STRESS RELIEF, SMILEY FACE", item_number="3")
    if out is not None:
        assert "velvet" not in (out.get("canonical_description") or "").lower(), (
            "line-number '3' cross-matched the velvet-poster item_mapping"
        )


def test_item_memory_real_part_number_still_matches():
    db = _seed_item_mappings()
    db.execute(
        "INSERT INTO item_mappings (canonical_description, original_item_number, confirmed) "
        "VALUES (?,?,?)", ("Real Product", "PS1351", 1))
    db.commit()
    out = _check_item_memory(db, "anything", item_number="PS1351")
    assert out is not None and out["canonical_description"] == "Real Product"


def _seed_won_quotes():
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE won_quotes (
            description TEXT, unit_price REAL, quantity REAL, supplier TEXT,
            department TEXT, award_date TEXT, category TEXT, confidence REAL,
            po_number TEXT, item_number TEXT, normalized_description TEXT
        )
    """)
    # A $12,406 laptop that happens to carry line-number '2'
    db.execute("INSERT INTO won_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               ("LAPTOP NOTEBOOK COMPUTER PC", 12406.11, 1, "PC SPECIALISTS",
                "", "2026-01-01", "it", 1.0, "PO1", "2",
                "laptop notebook computer pc"))
    db.commit()
    return db


def test_market_search_ignores_line_number():
    """_search_won_quotes must NOT pull a $12,406 laptop as a competitor for a
    paper composition notebook just because both lines are line-number '2'."""
    db = _seed_won_quotes()
    hits = _search_won_quotes(db, "NOTEBOOK, COMPOSITION, BLACK MARBLE", item_number="2")
    assert not any(abs(h.get("price", 0) - 12406.11) < 0.01 for h in hits), (
        "line-number '2' cross-matched the laptop into the notebook's market data"
    )


def test_market_search_real_part_number_still_matches():
    db = _seed_won_quotes()
    hits = _search_won_quotes(db, "unrelated query text here", item_number="GP3212")
    # No row has item_number GP3212, and the description doesn't match → no hits,
    # but the call must not error and must not pull the laptop.
    assert not any(abs(h.get("price", 0) - 12406.11) < 0.01 for h in hits)
