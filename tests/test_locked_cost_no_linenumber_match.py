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

from src.core.pricing_oracle_v2 import _get_locked_cost, _is_real_part_number


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
