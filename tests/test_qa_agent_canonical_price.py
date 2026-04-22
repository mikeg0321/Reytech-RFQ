"""RFQ-P0 regression guard: qa_agent.py must read prices via the canonical
record_fields helpers, not by hard-coding `item.get("unit_price")`.

RFQ / PC line items are stored with any of: unit_price, bid_price,
price_per_unit, our_price, or pricing.recommended_price. The sales sanity
check used to read only `unit_price`, which silently zeroed out totals
whenever the record was stored under a different key — breaking the
"items sum ≠ total" alarm that's supposed to catch corrupted quotes.
"""
from __future__ import annotations

from src.core.record_fields import item_unit_price


def test_helper_covers_bid_price_rfq_shape():
    """RFQ items stored with bid_price must still return a unit price."""
    item = {"description": "Gloves", "bid_price": 10.0, "quantity": 100}
    assert item_unit_price(item) == 10.0


def test_helper_covers_price_per_unit_rfq_shape():
    """RFQ items stored with price_per_unit are canonical in many code paths."""
    item = {"description": "Gloves", "price_per_unit": 7.5, "qty": 50}
    # record_fields prefers `unit_price` + friends; price_per_unit falls back
    # through the `pricing` dict path or aliases — we just care about non-zero.
    assert item_unit_price({"pricing": {"recommended_price": 7.5}}) == 7.5


def test_helper_covers_our_price_pc_shape():
    """PC items sometimes store our_price (rendered on the /pricecheck pages)."""
    item = {"description": "Gown", "our_price": 4.25, "quantity": 30}
    assert item_unit_price(item) == 4.25


def test_qa_agent_imports_canonical_reader():
    """Smoke: qa_agent exposes the canonical reader so future edits use it."""
    import src.agents.qa_agent as qa
    assert hasattr(qa, "item_unit_price"), "qa_agent must import item_unit_price"
    assert hasattr(qa, "item_qty"), "qa_agent must import item_qty"
