"""Tests for the authoritative food-line-item signal used to reconcile the
email-derived `food_items_present` flag.

Incident 2026-05-28: the RFQ banner showed "🍎 Food items detected — OBS 1600
may be required" on an order whose only line item was "MOUNT, IV POLE, FOR
CHARGING CRADLE" (durable medical equipment). The LLM requirement extractor
(claude · 85% conf) had flipped `food_items_present` true from a standard
"expiration date / dated materials" boilerplate clause. OBS 1600 is a
per-line-item agricultural-food form, so the line items are the authoritative
answer to "are food items present?".
"""
from __future__ import annotations

from src.forms.food_classifier import any_food_line_item, is_food_item


def test_iv_pole_mount_is_not_food():
    # The exact incident description.
    assert is_food_item("MOUNT, IV POLE, FOR CHARGING CRADLE") is False


def test_durable_equipment_order_has_no_food():
    items = [
        {"description": "MOUNT, IV POLE, FOR CHARGING CRADLE", "qty": 2},
        {"description": "Nitrile Exam Gloves, Medium", "qty": 10},
    ]
    assert any_food_line_item(items) is False


def test_real_food_order_is_detected():
    items = [
        {"description": "Canned Peaches, Halves, #10 can", "qty": 24},
        {"description": "Brown Rice, Long Grain, 25lb", "qty": 5},
    ]
    assert any_food_line_item(items) is True


def test_mixed_order_with_one_food_line_is_detected():
    items = [
        {"description": "MOUNT, IV POLE, FOR CHARGING CRADLE", "qty": 2},
        {"description": "Whole Milk, 1 Gallon", "qty": 30},
    ]
    assert any_food_line_item(items) is True


def test_empty_or_missing_items():
    assert any_food_line_item([]) is False
    assert any_food_line_item(None) is False


def test_non_dict_items_are_safe():
    # Defensive: malformed item rows must not raise.
    assert any_food_line_item(["just a string", {"description": "Steak, ribeye"}]) is True
