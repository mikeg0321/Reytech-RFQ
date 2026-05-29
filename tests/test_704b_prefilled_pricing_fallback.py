"""704B prefilled-pricing fallback — incident sol 10847187 (2026-05-29).

When a buyer prefills a 704B with description + QTY but leaves ITEM NUMBER
blank, `_aggregate_items_for_prefilled_rows` could key nothing (it matched
buyer rows only by ITEM NUMBER), so every priced item fell to overflow and the
visible row rendered blank PRICE PER UNIT / SUBTOTAL.

Fix: place leftover items into blank-ITEM-NUMBER buyer rows by description then
position, and fill the empty ITEM NUMBER cell with our MFG#/SKU.

Boundary coverage (per CLAUDE.md PDF guard rails — where 704B bugs hide):
1 / 8 (page-1 full) / 9 (page-1→page-2) / 16 / 20+ (overflow). A single-item
RFQ can't exercise these, which is exactly why the operator flagged the gap.
"""
from __future__ import annotations

from src.forms.ams704_helpers import build_704_item_fields, FillStrategy


class _PrefilledProfile:
    """Buyer-prefilled 704B stand-in. Page 1 = Row1..Row{pg1}, page 2 =
    Row1_2..Row{pg2}_2 (the convention `build_704_item_fields` expects)."""

    def __init__(self, n_rows, *, item_numbers=None, descriptions=None,
                 qtys=None, pg1=8, pg2=8):
        self._pg1 = list(range(1, pg1 + 1))
        self._pg2_suf = list(range(1, pg2 + 1))
        self.is_prefilled = True
        self.field_values: dict = {}
        self.prefilled_item_rows: dict = {}
        for i in range(1, n_rows + 1):
            suf = self.row_field_suffix(i)
            if suf is None:
                break  # beyond physical capacity → buyer couldn't prefill it
            self.prefilled_item_rows[i] = suf
            self.field_values[f"QTY{suf}"] = str(qtys[i - 1]) if qtys else "2"
            if descriptions:
                self.field_values[f"ITEM DESCRIPTION PRODUCT SPECIFICATION{suf}"] = descriptions[i - 1]
            if item_numbers:
                self.field_values[f"ITEM NUMBER{suf}"] = item_numbers[i - 1]

    @property
    def pg1_row_count(self):
        return len(self._pg1)

    @property
    def pg2_rows_suffixed(self):
        return self._pg2_suf

    @property
    def pg2_rows_plain(self):
        return []

    @property
    def field_names(self):
        return list(self.field_values.keys())

    def row_field_suffix(self, slot: int):
        if slot <= len(self._pg1):
            return f"Row{self._pg1[slot - 1]}"
        p2 = slot - len(self._pg1)
        if p2 <= len(self._pg2_suf):
            return f"Row{self._pg2_suf[p2 - 1]}_2"
        return None

    def row_page_number(self, slot: int):
        if slot <= len(self._pg1):
            return 1
        return 2 if slot <= len(self._pg1) + len(self._pg2_suf) else 0

    def validate_mapping(self, _values):
        return []


def _items(n, *, desc_prefix="MOUNT, IV POLE FOR CHARGING CRADLE", price=51.50):
    """n priced RFQ line items with distinct MFG#s that do NOT match any
    buyer ITEM NUMBER (the buyer left those blank)."""
    return [
        {
            "line_number": i,
            "description": f"{desc_prefix} {i}" if n > 1 else desc_prefix,
            "mfg_number": f"008-0862-{i:02d}",
            "qty": 2,
            "price_per_unit": price,
        }
        for i in range(1, n + 1)
    ]


def _build(n, **profile_kw):
    items = _items(n)
    descriptions = [it["description"] for it in items]
    profile = _PrefilledProfile(n, descriptions=descriptions, **profile_kw)
    return profile, build_704_item_fields(
        profile, items, FillStrategy.RFQ_PREFILLED, convention="704b"
    )


def _suffixes_in_form(profile, n):
    return [profile.row_field_suffix(i) for i in range(1, n + 1)
            if profile.row_field_suffix(i) is not None]


# ── The exact incident: 1 item, blank buyer ITEM NUMBER ──────────────────────

def test_single_item_blank_item_number_prices_land():
    profile, result = _build(1)
    fv = result.field_values
    assert fv.get("PRICE PER UNITRow1") == "51.50", (
        f"PRICE PER UNIT must land on Row1; got {fv.get('PRICE PER UNITRow1')!r}"
    )
    assert fv.get("SUBTOTALRow1") not in (None, "", "0.00"), "SUBTOTAL must render"
    assert fv.get("ITEM NUMBERRow1") == "008-0862-01", "blank ITEM NUMBER filled with our MFG#"
    assert not result.overflow_items, "the priced item must NOT fall to overflow"


def test_positional_fallback_when_buyer_desc_absent():
    # Buyer prefilled QTY only (no description, no item number) — must still
    # place the price positionally into Row1.
    items = _items(1)
    profile = _PrefilledProfile(1)  # no descriptions, no item_numbers
    result = build_704_item_fields(profile, items, FillStrategy.RFQ_PREFILLED, convention="704b")
    assert result.field_values.get("PRICE PER UNITRow1") == "51.50"
    assert not result.overflow_items


# ── Multi-page boundaries (the operator's untested concern) ──────────────────

def _assert_all_form_rows_priced(profile, result, n):
    placed = _suffixes_in_form(profile, n)
    for suf in placed:
        assert result.field_values.get(f"PRICE PER UNIT{suf}") == "51.50", (
            f"row {suf} should be priced; got {result.field_values.get(f'PRICE PER UNIT{suf}')!r}"
        )
        assert result.field_values.get(f"SUBTOTAL{suf}") not in (None, "", "0.00")


def test_eight_items_fill_page_one():
    profile, result = _build(8)
    _assert_all_form_rows_priced(profile, result, 8)
    assert not result.overflow_items


def test_nine_items_cross_to_page_two():
    profile, result = _build(9)
    _assert_all_form_rows_priced(profile, result, 9)
    # 9th item lands on the first page-2 row
    assert result.field_values.get("PRICE PER UNITRow1_2") == "51.50"
    assert not result.overflow_items


def test_sixteen_items_fill_both_pages():
    profile, result = _build(16)
    _assert_all_form_rows_priced(profile, result, 16)
    assert not result.overflow_items


def test_twenty_items_overflow_beyond_capacity():
    # 16 form rows (8 + 8) priced; items 17-20 → overflow pages.
    profile, result = _build(20)
    _assert_all_form_rows_priced(profile, result, 16)
    assert len(result.overflow_items) == 4, (
        f"items 17-20 should overflow; got {len(result.overflow_items)}"
    )


# ── Coleman-safe: when buyer ITEM NUMBERs ARE present, fallback stays inert ──

def test_buyer_item_numbers_present_no_fallback_misplacement():
    # Buyer prefilled 2 rows WITH item numbers. Our items match by MFG#.
    # A third item with a non-matching MFG# must go to overflow, NOT get
    # force-placed into a buyer row (that would resurrect the splat bug).
    profile = _PrefilledProfile(
        2,
        item_numbers=["8700-0893-01", "LF03699"],
        descriptions=["Trainer", "Airway"],
        qtys=[19, 2],
    )
    items = [
        {"line_number": 1, "description": "Trainer", "mfg_number": "8700-0893-01", "qty": 19, "price_per_unit": 100.0},
        {"line_number": 2, "description": "Airway", "mfg_number": "LF03699", "qty": 2, "price_per_unit": 50.0},
        {"line_number": 3, "description": "Mystery", "mfg_number": "ZZZ-NOMATCH", "qty": 1, "price_per_unit": 9.0},
    ]
    result = build_704_item_fields(profile, items, FillStrategy.RFQ_PREFILLED, convention="704b")
    fv = result.field_values
    assert fv.get("PRICE PER UNITRow1") == "100.00"
    assert fv.get("PRICE PER UNITRow2") == "50.00"
    # the non-matching item overflows — it is NOT forced into a buyer row
    assert any((it.mfg_number or "").upper() == "ZZZ-NOMATCH" for it in result.overflow_items), (
        "non-matching MFG# must overflow when buyer rows are item-numbered"
    )
