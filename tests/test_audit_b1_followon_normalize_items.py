"""B-1 follow-on hotfix (audit 2026-05-07).

PR #823 closed `_item_status` (one reader) but `routes_orders_full.py`
has its OWN inline `it.get("sourcing_status")` and `it.get("description")`
calls (lines 68, 76, 77, 78, 88, 459, 531, 539+). Every one of those
crashed on the same legacy bare-string items the original B-1 was
supposed to fix — confirmed by prod smoke 2026-05-07 returning HTTP 500
on `/orders` AND `/api/funnel/stats` AFTER PR #823 deployed.

Substrate fix: normalize at the read boundary in `order_dal.list_orders`
+ `get_order` so every downstream consumer sees `list[dict]`. Bare
strings (the rare legacy shape from old SCPRS-stub orders) get coerced
to `{"description": str(x)}`. Patching individual call sites is whack-a-mole.
"""
from __future__ import annotations

import json

import pytest

from src.core.order_dal import _normalize_legacy_items


def test_normalize_passes_dicts_through():
    items = [{"description": "Widget", "qty": 1}, {"description": "Gadget"}]
    out = _normalize_legacy_items(items)
    assert out == items


def test_normalize_coerces_strings():
    items = ["bare string item", "another"]
    out = _normalize_legacy_items(items)
    assert out == [
        {"description": "bare string item"},
        {"description": "another"},
    ]


def test_normalize_handles_mixed():
    items = [{"description": "Widget", "qty": 5}, "lonely string", None]
    out = _normalize_legacy_items(items)
    assert out[0] == {"description": "Widget", "qty": 5}
    assert out[1] == {"description": "lonely string"}
    # `None` stringifies to "None" — better than crashing or silently dropping.
    assert out[2] == {"description": "None"}


def test_normalize_handles_none_and_empty():
    assert _normalize_legacy_items(None) == []
    assert _normalize_legacy_items([]) == []


def test_list_orders_handles_legacy_string_items_in_items_column(app, temp_data_dir):
    """End-to-end: an order row whose `items` column is a JSON list of
    bare strings (legacy SCPRS-stub shape) must NOT crash list_orders,
    and downstream readers must see dict items."""
    import os
    import sqlite3
    from src.core.order_dal import list_orders

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    try:
        # Insert a legacy order with bare-string items.
        oid = "ORD-LEGACY-STRING-001"
        conn.execute(
            "INSERT OR REPLACE INTO orders (id, status, items, created_at) "
            "VALUES (?, 'new', ?, datetime('now'))",
            (oid, json.dumps(["loose string A", "loose string B"]))
        )
        conn.commit()
    finally:
        conn.close()

    # MUST NOT raise — this was the prod 500.
    orders = list_orders()

    # Find our row and assert it's been normalized.
    matching = [o for o in orders if o.get("id") == oid]
    assert len(matching) == 1, "test order missing from list_orders output"
    o = matching[0]
    assert isinstance(o["line_items"], list)
    assert len(o["line_items"]) == 2
    for it in o["line_items"]:
        assert isinstance(it, dict), f"line item not a dict: {it!r}"
        assert "description" in it
    assert o["line_items"][0]["description"] == "loose string A"
    assert o["line_items"][1]["description"] == "loose string B"

    # Sourcing summaries must succeed (this was the actual `.get` call site).
    assert o["item_count"] == 2
    assert o["sourced_count"] == 0  # no sourcing_status on legacy strings
    assert o["pct_complete"] == 0


def test_get_order_handles_legacy_string_items(app, temp_data_dir):
    """Same fix in `get_order` (single-fetch path)."""
    import os
    import sqlite3
    from src.core.order_dal import get_order

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    try:
        oid = "ORD-LEGACY-STRING-002"
        conn.execute(
            "INSERT OR REPLACE INTO orders (id, status, items, created_at) "
            "VALUES (?, 'new', ?, datetime('now'))",
            (oid, json.dumps(["just one bare string"]))
        )
        conn.commit()
    finally:
        conn.close()

    o = get_order(oid)
    assert o is not None, f"get_order({oid!r}) returned None"
    assert len(o["line_items"]) == 1
    assert isinstance(o["line_items"][0], dict)
    assert o["line_items"][0]["description"] == "just one bare string"
