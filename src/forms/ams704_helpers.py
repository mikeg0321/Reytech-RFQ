"""
AMS 704 Shared Helpers — Pure functions used by both fill_ams704() and fill_704b().

These helpers eliminate duplicated logic between the PC (price_check.py) and
RFQ (reytech_filler_v4.py) form fillers. Each function is pure (no PDF I/O,
no side effects) and independently testable.

Phase 2 of the PDF architecture overhaul (Phase 1 = TemplateProfile).
"""

import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("reytech.ams704_helpers")


# ═══════════════════════════════════════════════════════════════════════════
# ROW FIELD NAME TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════
# Two naming conventions exist because 704A (PC) and 704B (RFQ) templates
# have differently-named fields for the same columns.
#
# Convention A (704 / Price Check):
#   Field names embed the row like: "PRICE PER UNITRow{n}"
#   Suffix appended: "PRICE PER UNITRow5" or "PRICE PER UNITRow5_2"
#
# Convention B (704B / RFQ):
#   Field names use the full suffix: "PRICE PER UNIT{suffix}"
#   Where suffix = "Row5" or "Row5_2"
#
# Both conventions produce identical output for the core pricing fields
# (PRICE PER UNIT, QTY, SUBTOTAL) because the pattern is the same.
# They differ for description and item_number fields.

ROW_FIELD_TEMPLATES_704A = {
    "item_number": "ITEM Row{n}",
    "qty": "QTYRow{n}",
    "uom": "UNIT OF MEASURE UOMRow{n}",
    "qty_per_uom": "QTY PER UOMRow{n}",
    "description": "ITEM DESCRIPTION NOUN FIRST Include manufacturer part number andor reference numberRow{n}",
    "substituted": "SUBSTITUTED ITEM Include manufacturer part number andor reference numberRow{n}",
    "unit_price": "PRICE PER UNITRow{n}",
    "extension": "EXTENSIONRow{n}",
}

ROW_FIELD_TEMPLATES_704B = {
    "hash": "#{suffix}",
    "qty": "QTY{suffix}",
    "uom": "UOM{suffix}",
    "item_number": "ITEM NUMBER{suffix}",
    "description": "ITEM DESCRIPTION PRODUCT SPECIFICATION{suffix}",
    "substituted": "SUBSTITUTED ITEM Include manufacturer part number andor reference number{suffix}",
    "unit_price": "PRICE PER UNIT{suffix}",
    "subtotal": "SUBTOTAL{suffix}",
}


# ═══════════════════════════════════════════════════════════════════════════
# ROW FIELD NAME BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def build_row_field_name(
    profile,  # TemplateProfile
    slot: int,
    field_key: str,
    convention: str = "704a",
) -> tuple[Optional[str], int, str]:
    """Build the PDF field name for a given item slot and field key.

    Args:
        profile: TemplateProfile instance for the template being filled.
        slot: 1-based sequential item number (1 = first item, 12 = twelfth item).
        field_key: Key from ROW_FIELD_TEMPLATES (e.g. "unit_price", "description").
        convention: "704a" for Price Check templates, "704b" for RFQ templates.

    Returns:
        (field_name, page_number, bare_suffix) where:
            field_name: Full PDF field name, e.g. "PRICE PER UNITRow5_2"
            page_number: 1-based page (1 or 2), or 0 if overflow
            bare_suffix: "" for page 1 unsuffixed, "_2" for page 2 suffixed
        Returns (None, 0, "") if slot exceeds form capacity.

    Examples:
        >>> build_row_field_name(profile, 1, "unit_price")
        ("PRICE PER UNITRow1", 1, "")

        >>> build_row_field_name(profile, 12, "unit_price")
        ("PRICE PER UNITRow4_2", 2, "_2")

        >>> build_row_field_name(profile, 20, "unit_price")
        (None, 0, "")  # overflow
    """
    row_suffix = profile.row_field_suffix(slot)
    if row_suffix is None:
        return None, 0, ""

    page = profile.row_page_number(slot)
    bare_suffix = "_2" if row_suffix.endswith("_2") else ""

    templates = ROW_FIELD_TEMPLATES_704A if convention == "704a" else ROW_FIELD_TEMPLATES_704B
    template = templates.get(field_key)
    if template is None:
        log.warning("build_row_field_name: unknown field_key %r for convention %s", field_key, convention)
        return None, 0, ""

    if convention == "704a":
        # 704A: extract row number from suffix like "Row5" or "Row5_2"
        # Template uses {n} for row number, suffix appended after
        import re
        m = re.match(r"Row(\d+)(_2)?$", row_suffix)
        if not m:
            return None, 0, ""
        n = int(m.group(1))
        field_suffix = m.group(2) or ""
        field_name = template.format(n=n) + field_suffix
    else:
        # 704B: template uses {suffix} directly
        field_name = template.format(suffix=row_suffix)

    return field_name, page, bare_suffix


# ═══════════════════════════════════════════════════════════════════════════
# LINE ITEM NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def normalize_line_item(item: dict) -> dict:
    """Normalize item field names and types for PDF generation.

    Handles both PC format and RFQ format items. Maps multiple field name
    aliases to canonical names, coerces types, strips currency symbols.

    This is the single normalizer for all 704 form filling. Replaces
    _normalize_item() in reytech_filler_v4.py and inline normalization
    in price_check.py.

    Returns a NEW dict (does not mutate the input).
    """
    n = dict(item)

    # Description: 3 aliases
    n["description"] = (
        item.get("description")
        or item.get("desc")
        or item.get("item_description")
        or ""
    ).strip()

    # Quantity: 4 aliases, coerce to float
    qty = item.get("qty") or item.get("quantity") or item.get("QTY") or 0
    try:
        n["qty"] = float(str(qty).replace(",", ""))
    except (ValueError, TypeError):
        n["qty"] = 0

    # Price per unit: 5 aliases, strip $ and commas
    price = (
        item.get("price_per_unit")
        or item.get("bid_price")
        or item.get("unit_price")
        or item.get("sell_price")
        or item.get("final_price")
        or 0
    )
    try:
        n["price_per_unit"] = float(str(price).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        n["price_per_unit"] = 0

    # Supplier cost: 3 aliases
    cost = (
        item.get("supplier_cost")
        or item.get("cost")
        or item.get("unit_cost")
        or 0
    )
    try:
        n["supplier_cost"] = float(str(cost).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        n["supplier_cost"] = 0

    # Part number: 4 aliases
    n["part_number"] = str(
        item.get("part_number")
        or item.get("item_number")
        or item.get("catalog_number")
        or item.get("mfg_number")
        or ""
    )

    # UOM: 3 aliases, default "EA"
    n["uom"] = str(
        item.get("uom")
        or item.get("UOM")
        or item.get("unit_of_measure")
        or "EA"
    )

    # Line number
    n["line_number"] = item.get("line_number") or item.get("#") or 0

    return n


# ═══════════════════════════════════════════════════════════════════════════
# DESCRIPTION OVERFLOW SPLITTING
# ═══════════════════════════════════════════════════════════════════════════

def split_description(text: str, char_limit: int = 140) -> tuple[str, Optional[str]]:
    """Split a long description at a natural break point.

    Returns (part1, part2) where part2 is None if no split needed.
    Tries to break at newline, comma-space, or space near the limit.

    Args:
        text: Description text to potentially split.
        char_limit: Maximum length before splitting (default 140).

    Returns:
        (part1, part2): part1 is always <= char_limit (approximately).
            part2 is None if text fits within limit.
    """
    if not text or len(text) <= char_limit:
        return text, None

    split_at = char_limit
    for break_char in ['\n', ', ', ' ']:
        pos = text.rfind(break_char, 0, split_at + 10)
        if pos > split_at - 40:
            split_at = pos + len(break_char)
            break

    part1 = text[:split_at].rstrip()
    part2 = text[split_at:].lstrip()

    if not part2:
        return text, None

    return part1, part2


# ═══════════════════════════════════════════════════════════════════════════
# TOTALS COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TotalsResult:
    """Computed totals for a set of line items."""
    subtotal: float
    freight: float
    tax: float
    total: float
    items_priced: int
    items_total: int


def compute_line_totals(
    items: list[dict],
    tax_rate: float = 0.0,
    freight: float = 0.0,
) -> TotalsResult:
    """Compute subtotal, tax, and total from a list of normalized line items.

    Each item should have 'qty' and either 'price_per_unit' or 'unit_price'.
    Items with zero or missing price are counted but not included in subtotal.

    Args:
        items: List of item dicts (normalized or raw).
        tax_rate: Tax rate as decimal (e.g. 0.0775 for 7.75%).
        freight: Freight charge (default 0.0).

    Returns:
        TotalsResult with subtotal, freight, tax, total, items_priced, items_total.
    """
    subtotal = 0.0
    items_priced = 0

    for item in items:
        price = (
            item.get("price_per_unit")
            or item.get("unit_price")
            or 0
        )
        try:
            price = float(price)
        except (ValueError, TypeError):
            price = 0

        qty = item.get("qty", 0)
        try:
            qty = float(qty)
        except (ValueError, TypeError):
            qty = 0

        if price > 0:
            subtotal += round(price * qty, 2)
            items_priced += 1

    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + freight + tax, 2)

    return TotalsResult(
        subtotal=round(subtotal, 2),
        freight=freight,
        tax=tax,
        total=total,
        items_priced=items_priced,
        items_total=len(items),
    )
