"""
validation.py — Shared input validation for all save/update endpoints.

Used by routes_pricecheck.py, routes_rfq.py, and any future route module.
All functions return (sanitized_value, error_or_None). Callers decide
whether to log the error or reject the request.
"""

import logging

log = logging.getLogger("reytech.validation")


def validate_price(val):
    """Validate a price/cost value. Returns (float, error)."""
    try:
        v = float(val) if val else 0
        if v < 0: return (0, "Negative price")
        if v > 999999: return (v, "Price exceeds $999,999")
        return (v, None)
    except (ValueError, TypeError):
        return (0, f"Invalid price: {val!r}")


def validate_cost(val):
    """Validate a cost value. Returns (float, error)."""
    try:
        v = float(val) if val else 0
        if v < 0: return (0, "Negative cost")
        if v > 999999: return (v, "Cost exceeds $999,999")
        return (v, None)
    except (ValueError, TypeError):
        return (0, f"Invalid cost: {val!r}")


def validate_markup(val):
    """Validate a markup percentage. Returns (float, error)."""
    try:
        v = float(val) if val else 25
        if v < 0: v = 0
        if v > 500: v = 500
        return (v, None)
    except (ValueError, TypeError):
        return (25, f"Invalid markup: {val!r}")


def validate_qty(val):
    """Validate a quantity. Returns (int, error)."""
    try:
        v = int(float(val)) if val else 1
        if v < 1: v = 1
        if v > 999999: v = 999999
        return (v, None)
    except (ValueError, TypeError):
        return (1, f"Invalid qty: {val!r}")


def validate_text(val, max_len=5000, default=""):
    """Validate a text field (description, notes). Returns (str, error)."""
    if val is None:
        return (default, None)
    s = str(val)
    if len(s) > max_len:
        return (s[:max_len], f"Truncated from {len(s)} to {max_len} chars")
    return (s, None)


def validate_short_text(val, max_len=50, default=""):
    """Validate a short text field (UOM, item number). Returns (str, error)."""
    if val is None:
        return (default, None)
    s = str(val).strip()[:max_len]
    return (s, None)


def validate_url(val, max_len=2000):
    """Validate a URL field. Returns (str, error)."""
    if not val:
        return ("", None)
    s = str(val).strip()[:max_len]
    return (s, None)


def validate_bool(val):
    """Validate a boolean field. Returns (bool, None)."""
    return (bool(val), None)


def validate_int(val, min_val=0, max_val=999999, default=0):
    """Validate an integer field. Returns (int, error)."""
    try:
        v = int(float(val)) if val else default
        if v < min_val: v = min_val
        if v > max_val: v = max_val
        return (v, None)
    except (ValueError, TypeError):
        return (default, f"Invalid int: {val!r}")


def validate_rfq_item(update: dict, item: dict) -> list:
    """Validate and apply an RFQ item update dict to an item.
    Returns list of warning strings (empty = all clean).

    Fields handled: supplier_cost, scprs_last_price, price_per_unit,
    markup_pct, qty, uom, description, item_number, item_link.
    """
    warnings = []

    if "supplier_cost" in update and update["supplier_cost"] is not None:
        v, err = validate_cost(update["supplier_cost"])
        if err: warnings.append(f"supplier_cost: {err}")
        item["supplier_cost"] = v

    if "scprs_last_price" in update and update["scprs_last_price"] is not None:
        v, err = validate_price(update["scprs_last_price"])
        if err: warnings.append(f"scprs_last_price: {err}")
        item["scprs_last_price"] = v

    if "price_per_unit" in update and update["price_per_unit"] is not None:
        v, err = validate_price(update["price_per_unit"])
        if err: warnings.append(f"price_per_unit: {err}")
        item["price_per_unit"] = v

    if "markup_pct" in update and update["markup_pct"] is not None:
        v, err = validate_markup(update["markup_pct"])
        if err: warnings.append(f"markup_pct: {err}")
        item["markup_pct"] = v

    if "qty" in update and update["qty"] is not None:
        v, err = validate_qty(update["qty"])
        if err: warnings.append(f"qty: {err}")
        item["qty"] = v

    if "uom" in update:
        v, _ = validate_short_text(update["uom"], max_len=20, default="EA")
        item["uom"] = v.upper()

    if "description" in update:
        v, err = validate_text(update["description"], max_len=5000)
        if err: warnings.append(f"description: {err}")
        item["description"] = v

    if "item_number" in update:
        v, _ = validate_short_text(update["item_number"], max_len=100)
        item["item_number"] = v

    if "item_link" in update:
        v, _ = validate_url(update["item_link"])
        item["item_link"] = v

    if "line_number" in update:
        try:
            item["line_number"] = int(float(update["line_number"])) if update["line_number"] else 0
        except (ValueError, TypeError):
            item["line_number"] = 0

    return warnings


def validate_header_field(field: str, val) -> tuple:
    """Validate an RFQ/PC header field value.
    Returns (sanitized_value, error_or_None)."""
    if field in ("solicitation_number", "pc_number", "requestor_name",
                 "requestor_email", "institution", "agency_name"):
        return validate_text(val, max_len=200, default="")
    elif field in ("due_date",):
        return validate_text(val, max_len=30, default="")
    elif field in ("ship_to", "delivery_location"):
        return validate_text(val, max_len=500, default="")
    elif field in ("notes", "custom_notes"):
        return validate_text(val, max_len=5000, default="")
    elif field == "tax_rate":
        try:
            v = float(val) if val else 0
            if v < 0: v = 0
            if v > 50: v = 50
            return (v, None)
        except (ValueError, TypeError):
            return (0, f"Invalid tax_rate: {val!r}")
    else:
        return validate_text(val, max_len=2000, default="")


def self_test() -> dict:
    """Run validation self-test. Returns {"ok": True/False, "errors": [...]}."""
    errors = []

    # Price
    v, e = validate_price("12.50")
    if v != 12.5 or e: errors.append(f"price basic: got {v}, {e}")
    v, e = validate_price("-5")
    if v != 0: errors.append(f"price negative: got {v}")
    v, e = validate_price("abc")
    if v != 0 or e is None: errors.append(f"price junk: got {v}, {e}")

    # Cost
    v, e = validate_cost("0")
    if v != 0 or e: errors.append(f"cost zero: got {v}, {e}")

    # Markup
    v, e = validate_markup("600")
    if v != 500: errors.append(f"markup cap: got {v}")
    v, e = validate_markup("")
    if v != 25: errors.append(f"markup default: got {v}")

    # Qty
    v, e = validate_qty("0")
    if v != 1: errors.append(f"qty min: got {v}")
    v, e = validate_qty("5.7")
    if v != 5: errors.append(f"qty float: got {v}")

    # Text
    v, e = validate_text("x" * 6000, max_len=5000)
    if len(v) != 5000 or e is None: errors.append(f"text cap: len={len(v)}")

    # URL
    v, e = validate_url(None)
    if v != "": errors.append(f"url none: got {v!r}")

    # Header
    v, e = validate_header_field("tax_rate", "999")
    if v != 50: errors.append(f"tax_rate cap: got {v}")

    # RFQ item
    item = {"supplier_cost": 0, "qty": 1, "uom": "EA", "description": ""}
    errs = validate_rfq_item({"supplier_cost": "-10", "qty": "abc"}, item)
    if item["supplier_cost"] != 0: errors.append(f"rfq_item cost neg: got {item['supplier_cost']}")
    if item["qty"] != 1: errors.append(f"rfq_item qty junk: got {item['qty']}")

    return {"ok": len(errors) == 0, "errors": errors, "tests_run": 12}
