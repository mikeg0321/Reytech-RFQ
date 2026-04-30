"""UPC-first item-memory lookup in pricing_oracle_v2._check_item_memory.

Mike's identifier-first rule: UPC > MFG# > description. UPC is the
strongest physical-product identifier — when present we hit it before
the existing MFG#/description paths so two SKUs that share an MFG#
(common when a supplier mis-keys an item) cannot collide.

Locks four invariants:
1. UPC match is checked first and wins over MFG# / description matches
   for the same row.
2. UPC match returns ``match_type='exact_upc'``.
3. UPC arg is optional — existing callers (no upc) keep working.
4. UPC fallthrough — when UPC is given but no row matches, MFG#/desc
   paths still fire.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_db(path):
    """Two confirmed rows. Identical descriptions + MFG#s to prove that
    the discriminator is UPC alone."""
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE item_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_description TEXT NOT NULL,
            original_item_number TEXT DEFAULT '',
            canonical_description TEXT DEFAULT '',
            canonical_item_number TEXT DEFAULT '',
            mfg_number TEXT DEFAULT '',
            mfg_name TEXT DEFAULT '',
            upc TEXT DEFAULT '',
            asin TEXT DEFAULT '',
            product_url TEXT DEFAULT '',
            supplier TEXT DEFAULT '',
            last_cost REAL DEFAULT 0,
            confidence REAL DEFAULT 0.5,
            confirmed INTEGER DEFAULT 0,
            times_confirmed INTEGER DEFAULT 0,
            uom TEXT DEFAULT '',
            supplier_url TEXT DEFAULT '',
            last_sell_price REAL DEFAULT 0,
            UNIQUE(original_description, original_item_number)
        );
    """)
    # Row A — UPC=111, last_cost=1.00, confirmed
    db.execute("""INSERT INTO item_mappings
        (original_description, original_item_number, canonical_description,
         canonical_item_number, mfg_number, upc, last_cost, confidence,
         confirmed, supplier)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        ("Glove nitrile", "MFG-SHARED", "Glove nitrile L", "MFG-SHARED",
         "MFG-SHARED", "111111111111", 1.00, 0.99, 1, "SupplierA"))
    # Row B — same MFG# + description prefix, UPC=222, last_cost=99.00
    db.execute("""INSERT INTO item_mappings
        (original_description, original_item_number, canonical_description,
         canonical_item_number, mfg_number, upc, last_cost, confidence,
         confirmed, supplier)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        ("Glove nitrile XL", "MFG-SHARED", "Glove nitrile XL", "MFG-SHARED",
         "MFG-SHARED", "222222222222", 99.00, 0.99, 1, "SupplierB"))
    db.commit()
    db.close()


@pytest.fixture
def seeded_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    _seed_db(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


def test_upc_match_wins_over_shared_mfg_number(seeded_db):
    """UPC=222 must return Row B (cost $99) even though the description
    + MFG# would also match Row A (cost $1)."""
    from src.core.pricing_oracle_v2 import _check_item_memory
    db = sqlite3.connect(seeded_db)
    try:
        result = _check_item_memory(
            db,
            description="Glove nitrile",   # matches Row A's exact desc
            item_number="MFG-SHARED",      # shared between A and B
            upc="222222222222",            # only on Row B
        )
    finally:
        db.close()
    assert result is not None
    assert result["match_type"] == "exact_upc"
    assert result["last_cost"] == 99.00
    assert result["supplier"] == "SupplierB"


def test_upc_match_type_tag(seeded_db):
    from src.core.pricing_oracle_v2 import _check_item_memory
    db = sqlite3.connect(seeded_db)
    try:
        result = _check_item_memory(db, description="x", upc="111111111111")
    finally:
        db.close()
    assert result["match_type"] == "exact_upc"


def test_no_upc_arg_falls_through_to_legacy_paths(seeded_db):
    """Existing callers without UPC must still resolve via MFG#/desc."""
    from src.core.pricing_oracle_v2 import _check_item_memory
    db = sqlite3.connect(seeded_db)
    try:
        result = _check_item_memory(
            db, description="Glove nitrile", item_number="MFG-SHARED",
        )
    finally:
        db.close()
    assert result is not None
    assert result["match_type"] in ("exact_item", "exact_desc")


def test_unknown_upc_falls_back_to_mfg_lookup(seeded_db):
    """UPC arg present but no row → still try item_number / description."""
    from src.core.pricing_oracle_v2 import _check_item_memory
    db = sqlite3.connect(seeded_db)
    try:
        result = _check_item_memory(
            db, description="Glove nitrile", item_number="MFG-SHARED",
            upc="999999999999",  # not in db
        )
    finally:
        db.close()
    assert result is not None
    assert result["match_type"] == "exact_item"


def test_get_pricing_accepts_upc_kwarg():
    """Smoke: get_pricing() signature accepts upc= without error."""
    import inspect
    from src.core.pricing_oracle_v2 import get_pricing
    sig = inspect.signature(get_pricing)
    assert "upc" in sig.parameters
    assert sig.parameters["upc"].default == ""


def test_recommend_for_item_accepts_upc_kwarg():
    """The flat-shape adapter that quote_engine imports must also accept upc."""
    import inspect
    from src.core.pricing_oracle_v2 import recommend_for_item
    sig = inspect.signature(recommend_for_item)
    assert "upc" in sig.parameters


def test_lineitem_has_upc_field():
    """quote_model.LineItem.upc must exist so quote_engine can pass it through."""
    from src.core.quote_model import LineItem
    item = LineItem(line_no=1, item_no="X", upc="123456789012", description="test")
    assert item.upc == "123456789012"
    # Default empty so existing constructions keep working
    assert LineItem(line_no=1, description="t").upc == ""
