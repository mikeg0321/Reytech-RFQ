"""
AMS 704 Shared Helpers — Pure functions used by both fill_ams704() and fill_704b().

These helpers eliminate duplicated logic between the PC (price_check.py) and
RFQ (reytech_filler_v4.py) form fillers. Each function is pure (no PDF I/O,
no side effects) and independently testable.

Phase 2 of the PDF architecture overhaul (Phase 1 = TemplateProfile).
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

log = logging.getLogger("reytech.ams704_helpers")


# ═══════════════════════════════════════════════════════════════════════════
# CANONICAL LINE ITEM (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════
# Normalize once at the boundary. Every fill function consumes LineItem,
# never raw dicts with ambiguous field names.

@dataclass
class LineItem:
    """Canonical line item for 704 form filling.

    All fill functions should consume LineItem objects, not raw dicts.
    Use from_dict() to normalize from either PC or RFQ data formats.
    """
    line_number: int = 0
    description: str = ""
    qty: float = 0
    uom: str = "EA"
    unit_price: float = 0       # sell price to buyer
    supplier_cost: float = 0    # our cost
    part_number: str = ""
    mfg_number: str = ""
    is_substitute: bool = False
    no_bid: bool = False
    qty_per_uom: int = 1
    notes: str = ""
    # Enrichment metadata (read-only, not written to PDF)
    pricing: dict = field(default_factory=dict)
    row_index: int = 0          # original row position from parsed PDF

    @property
    def extension(self) -> float:
        """Line total: qty * unit_price."""
        return round(self.qty * self.unit_price, 2)

    @property
    def has_price(self) -> bool:
        return self.unit_price > 0

    @classmethod
    def from_dict(cls, raw: dict) -> "LineItem":
        """Normalize from any raw item dict (PC or RFQ format).

        Handles all field name aliases:
          qty/quantity/QTY → qty
          price_per_unit/bid_price/unit_price/sell_price/final_price → unit_price
          supplier_cost/cost/unit_cost → supplier_cost
          part_number/item_number/catalog_number/mfg_number → part_number
          uom/UOM/unit_of_measure → uom
        """
        def _float(val, strip_dollar=False):
            if not val:
                return 0
            s = str(val)
            if strip_dollar:
                s = s.replace("$", "")
            s = s.replace(",", "")
            try:
                return float(s)
            except (ValueError, TypeError):
                return 0

        qty = raw.get("qty") or raw.get("quantity") or raw.get("QTY") or 0
        price = (raw.get("price_per_unit") or raw.get("bid_price")
                 or raw.get("unit_price") or raw.get("sell_price")
                 or raw.get("final_price") or 0)
        cost = (raw.get("supplier_cost") or raw.get("cost")
                or raw.get("unit_cost") or 0)
        qpu = raw.get("qty_per_uom", 1)
        try:
            qpu = int(float(qpu)) if qpu else 1
        except (ValueError, TypeError):
            qpu = 1

        return cls(
            line_number=raw.get("line_number") or raw.get("#") or 0,
            description=(raw.get("description") or raw.get("desc")
                         or raw.get("item_description") or "").strip(),
            qty=_float(qty),
            uom=str(raw.get("uom") or raw.get("UOM")
                     or raw.get("unit_of_measure") or "EA"),
            unit_price=_float(price, strip_dollar=True),
            supplier_cost=_float(cost, strip_dollar=True),
            part_number=str(raw.get("part_number") or raw.get("item_number")
                           or raw.get("catalog_number") or raw.get("mfg_number") or ""),
            mfg_number=str(raw.get("mfg_number") or raw.get("manufacturer_part") or ""),
            is_substitute=bool(raw.get("is_substitute", False)),
            no_bid=bool(raw.get("no_bid", False)),
            qty_per_uom=qpu,
            notes=str(raw.get("notes") or "").strip(),
            pricing=raw.get("pricing") or {},
            row_index=raw.get("row_index") or 0,
        )

    def to_dict(self) -> dict:
        """Convert back to dict for backward compatibility with existing code."""
        return {
            "line_number": self.line_number,
            "description": self.description,
            "qty": self.qty,
            "uom": self.uom,
            "price_per_unit": self.unit_price,
            "unit_price": self.unit_price,
            "supplier_cost": self.supplier_cost,
            "part_number": self.part_number,
            "mfg_number": self.mfg_number,
            "is_substitute": self.is_substitute,
            "no_bid": self.no_bid,
            "qty_per_uom": self.qty_per_uom,
            "notes": self.notes,
            "pricing": self.pricing,
            "row_index": self.row_index,
        }


# ═══════════════════════════════════════════════════════════════════════════
# FILL STRATEGY (Phase 4)
# ═══════════════════════════════════════════════════════════════════════════
# Replaces boolean flags (original_mode, pricing_only, keep_all_pages)
# with explicit strategies. Each strategy defines what to write.

class FillStrategy(Enum):
    """How to fill a 704 form. Replaces boolean flag combinations."""

    PC_FULL = "pc_full"
    """PC on blank template — write ALL fields (description, qty, uom, pricing, vendor info).
    Used when source is DOCX-converted or blank 704 template."""

    PC_ORIGINAL = "pc_original"
    """PC on buyer's original template — only write pricing + vendor info.
    Buyer's descriptions, qty, uom are untouched. Was: original_mode=True."""

    RFQ_FULL = "rfq_full"
    """RFQ response on fresh 704B — write all item fields + vendor info.
    Used for Cal Vet and generic 704B templates."""

    RFQ_PREFILLED = "rfq_prefilled"
    """RFQ response on agency pre-filled 704B — only write PRICE PER UNIT + SUBTOTAL.
    Buyer already filled descriptions, qty, uom. Was: _is_prefilled branch in fill_704b."""

    @property
    def writes_descriptions(self) -> bool:
        """Whether this strategy writes item descriptions to form fields."""
        return self in (FillStrategy.PC_FULL, FillStrategy.RFQ_FULL)

    @property
    def writes_qty_uom(self) -> bool:
        """Whether this strategy writes qty/uom fields."""
        return self in (FillStrategy.PC_FULL, FillStrategy.PC_ORIGINAL, FillStrategy.RFQ_FULL)

    @property
    def writes_pricing(self) -> bool:
        """Whether this strategy writes price/extension/subtotal fields."""
        return True  # All strategies write pricing

    @property
    def writes_vendor_info(self) -> bool:
        """Whether this strategy writes vendor/company fields."""
        return True  # All strategies write vendor info

    @property
    def writes_item_numbers(self) -> bool:
        """Whether this strategy writes item # / line number fields."""
        return self in (FillStrategy.PC_FULL, FillStrategy.RFQ_FULL)

    @classmethod
    def for_pc(cls, is_prefilled: bool, is_docx_source: bool = False) -> "FillStrategy":
        """Determine the correct strategy for a Price Check.

        Args:
            is_prefilled: Template has buyer-filled QTY values.
            is_docx_source: Source was DOCX (needs blank template, full fill).
        """
        if is_docx_source:
            return cls.PC_FULL
        if is_prefilled:
            return cls.PC_ORIGINAL
        return cls.PC_FULL

    @classmethod
    def for_rfq(cls, is_prefilled: bool) -> "FillStrategy":
        """Determine the correct strategy for an RFQ response.

        Args:
            is_prefilled: Template has buyer-filled descriptions/qty.
        """
        if is_prefilled:
            return cls.RFQ_PREFILLED
        return cls.RFQ_FULL


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


# ═══════════════════════════════════════════════════════════════════════════
# UNIFIED 704 ITEM FIELD BUILDER (V2)
# ═══════════════════════════════════════════════════════════════════════════
# Single loop that builds field values for any 704 form, driven by
# FillStrategy and convention. No PDF I/O — returns a dict of field values.

@dataclass
class Fill704Result:
    """Result of build_704_item_fields()."""
    field_values: dict              # {field_name: value_str} for all items
    merchandise_subtotal: float
    items_priced: int
    items_total: int
    overflow_items: list            # LineItems beyond form capacity (need overflow pages)


def build_704_item_fields(
    profile,  # TemplateProfile
    raw_items: list[dict],
    strategy: FillStrategy,
    convention: str = "704a",
) -> Fill704Result:
    """Build field-name → value dict for all line items on a 704 form.

    This is the single item loop for both PC and RFQ 704 generation.
    It does NOT do PDF I/O — the caller passes the result to the
    appropriate PDF fill function.

    Args:
        profile: TemplateProfile for the template being filled.
        raw_items: List of raw item dicts (PC or RFQ format).
        strategy: FillStrategy controlling which fields to write.
        convention: "704a" for Price Check, "704b" for RFQ response.

    Returns:
        Fill704Result with field_values dict, subtotals, and overflow items.
    """
    values = {}
    merchandise_subtotal = 0.0
    items_priced = 0
    overflow_items = []
    prefilled_rows = dict(profile.prefilled_item_rows) if profile.is_prefilled else {}

    # Assign sequential line numbers
    for i, item in enumerate(raw_items, start=1):
        item.setdefault("line_number", i)

    seq = 0
    for raw_item in raw_items:
        seq += 1
        li = LineItem.from_dict(raw_item)

        # Determine row field suffix
        if strategy in (FillStrategy.PC_ORIGINAL, FillStrategy.RFQ_PREFILLED) and prefilled_rows:
            item_num = li.line_number or seq
            if item_num in prefilled_rows:
                row_suffix = prefilled_rows[item_num]
            else:
                row_suffix = profile.row_field_suffix(seq)
        else:
            row_suffix = profile.row_field_suffix(seq)

        # Beyond form capacity → overflow (pages 3+)
        if row_suffix is None:
            overflow_items.append(li)
            continue

        page = profile.row_page_number(seq) if row_suffix == profile.row_field_suffix(seq) else (2 if "_2" in row_suffix else 1)

        # Helper to build field name from key
        def _fname(key):
            if convention == "704a":
                import re
                m = re.match(r"Row(\d+)(_2)?$", row_suffix)
                if not m:
                    return None
                n = int(m.group(1))
                sfx = m.group(2) or ""
                tmpl = ROW_FIELD_TEMPLATES_704A.get(key)
                return tmpl.format(n=n) + sfx if tmpl else None
            else:
                tmpl = ROW_FIELD_TEMPLATES_704B.get(key)
                return tmpl.format(suffix=row_suffix) if tmpl else None

        # ── Pricing (all strategies write these) ──
        price = li.unit_price
        qty = li.qty
        extension = round(price * qty, 2)
        merchandise_subtotal += extension

        if price > 0:
            items_priced += 1
            price_field = _fname("unit_price")
            if price_field:
                values[price_field] = f"{price:,.2f}" if convention == "704a" else f"{price:.2f}"

            ext_key = "extension" if convention == "704a" else "subtotal"
            ext_field = _fname(ext_key)
            if ext_field:
                values[ext_field] = f"{extension:,.2f}" if convention == "704a" else f"{extension:.2f}"

        # ── QTY/UOM (strategies that allow it) ──
        if strategy.writes_qty_uom:
            qty_field = _fname("qty")
            if qty_field:
                values[qty_field] = str(int(qty)) if qty == int(qty) else str(qty)
            uom_field = _fname("uom")
            if uom_field:
                values[uom_field] = li.uom.upper()
            # QTY per UOM (704A only)
            if convention == "704a" and li.qty_per_uom > 1:
                qpu_field = _fname("qty_per_uom")
                if qpu_field:
                    values[qpu_field] = str(li.qty_per_uom)

        # ── Descriptions + item details (strategies that allow it) ──
        if strategy.writes_descriptions:
            desc_field = _fname("description")
            if desc_field:
                desc_text = li.description
                # Description overflow (split at 140 chars)
                part1, part2 = split_description(desc_text)
                values[desc_field] = part1
                # Overflow part2 → next row (if available and convention supports it)
                if part2 and convention == "704a":
                    next_suffix = profile.row_field_suffix(seq + 1)
                    if next_suffix:
                        next_desc = _fname_for_suffix("description", next_suffix, convention)
                        if next_desc:
                            values[next_desc] = part2

            # Item number
            if strategy.writes_item_numbers:
                if convention == "704b":
                    hash_field = _fname("hash")
                    if hash_field:
                        values[hash_field] = str(seq)
                    inum_field = _fname("item_number")
                    if inum_field:
                        values[inum_field] = li.part_number
                else:
                    inum_field = _fname("item_number")
                    if inum_field:
                        values[inum_field] = str(seq)

            # Substituted item
            sub_field = _fname("substituted")
            if sub_field:
                if li.is_substitute:
                    mfg = li.mfg_number
                    if convention == "704b":
                        values[sub_field] = f"{li.description} (MFG# {mfg})" if mfg else li.description
                    else:
                        values[sub_field] = f"MFG#: {mfg}\n{li.description}" if mfg else li.description
                else:
                    values[sub_field] = "" if convention == "704b" else " "

    return Fill704Result(
        field_values=values,
        merchandise_subtotal=round(merchandise_subtotal, 2),
        items_priced=items_priced,
        items_total=len(raw_items),
        overflow_items=overflow_items,
    )


def _fname_for_suffix(key: str, row_suffix: str, convention: str) -> Optional[str]:
    """Build a field name for a specific row suffix (used for overflow)."""
    import re
    if convention == "704a":
        m = re.match(r"Row(\d+)(_2)?$", row_suffix)
        if not m:
            return None
        n = int(m.group(1))
        sfx = m.group(2) or ""
        tmpl = ROW_FIELD_TEMPLATES_704A.get(key)
        return tmpl.format(n=n) + sfx if tmpl else None
    else:
        tmpl = ROW_FIELD_TEMPLATES_704B.get(key)
        return tmpl.format(suffix=row_suffix) if tmpl else None
