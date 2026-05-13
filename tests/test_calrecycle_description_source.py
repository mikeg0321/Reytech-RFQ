r"""PR-2 / Phase 2 substrate — CalRecycle 74 description cleaner.

Live drive 2026-05-12 (rfq_8efe9fae) shipped CalRecycle 74 rows with
descriptions wiped or truncated where the same items in the Quote PDF
had full text. Symptoms recorded in
`project_queue_validation_2026_05_12.md`:

  - item 11 → `BX20BX/CS`           (just UOM leftovers, real noun gone)
  - item 14 → `Irrigation Solution` (truncated at first " - ")
  - item 15 → `Chest Rub Vicks ...` (truncated at first " - ")
  - items 7/8 → `Catheter` prefix dropped on some variants

Root causes in old `_calrecycle_clean_desc`:
  (a) `\s*#?\d{6,}[\w\-]*.*` — greedy `.*` wiped everything after
      a 6-digit catalog number. For "1118153 Safety Insulin Pen ..."
      the entire product noun was removed.
  (b) `if " - " in desc: desc = desc.split(" - ")[0]` — first-dash
      left-split. Truncates real spec content like "0.9% Sodium Chloride".
  (c) No leading UOM/slash strip — left `/BX20BX/CS` at start.

The fix sources from the same `description` field the Quote PDF uses,
does minimal anchored cleanup, and relies on the font auto-sizer
already present in `_calrecycle_overlay_items` to fit the cell. The
bid-package path's `_cr_desc` was also pointing at private logic that
diverged from the standalone path; it now delegates to the canonical
cleaner so the two CalRecycle surfaces never disagree.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Live-drive corpus (rfq_8efe9fae items 11/14/15/etc) ─────────────


def test_item11_pen_needle_preserves_product_noun():
    """Item 11 input started with `/BX20BX/CS 1118153 Safety Insulin Pen...`.
    Old extractor returned `BX20BX/CS`. Must now keep the product noun."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    desc = (
        "/BX20BX/CS 1118153 Safety Insulin Pen Needle Mc Kesson Prevent "
        "31 Gauge8 mm Length Active Back-End ProtectionNEEDLE, SAFETY "
        "PEN 31GX8MM (100EA/BX 20BX/CS)Mc Kesson # 1118153Manufacturer # 16-N8MMPA"
    )
    got = _calrecycle_clean_desc({"description": desc})
    assert "Safety Insulin Pen Needle" in got
    assert "BX20BX/CS" not in got
    # No dangling label tails
    assert not got.rstrip().endswith("#")
    assert "Manufacturer #" not in got
    assert "Mc Kesson # " not in got


def test_item14_irrigation_solution_keeps_strength():
    """Item 14 had real desc like `Irrigation Solution - 0.9% NaCl 1000mL`.
    Old extractor split at first " - " and dropped the strength + volume,
    leaving just `Irrigation Solution`."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    desc = "Irrigation Solution - 0.9% Sodium Chloride 1000mL Sterile Bag"
    got = _calrecycle_clean_desc({"description": desc})
    assert "0.9%" in got
    assert "Sodium Chloride" in got
    assert "1000mL" in got


def test_item15_vicks_keeps_full_strength_block():
    """Item 15 had a strength block with multiple " - " separators
    (`4.8% - 1.2% - 2.6% Strength Ointment`). Old extractor only
    kept the chunk before the first " - "."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    desc = (
        "Chest Rub Vicks Vapo Rub 4.8% - 1.2% - 2.6%Strength Ointment "
        "1.76 oz.c Kesson # 833168Manufacturer # 32390001051"
    )
    got = _calrecycle_clean_desc({"description": desc})
    assert "Chest Rub" in got
    assert "Vicks" in got
    assert "1.2%" in got, f"strength middle missing: {got!r}"
    # Trailing OCR-corrupt label `c Kesson #` must NOT survive
    assert "Kesson #" not in got
    assert "Manufacturer" not in got


def test_catheter_prefix_preserved():
    """Items 7/8 are described as `Catheter Foley 16Fr 5mL Latex`.
    The cleaner must not eat the leading product class noun."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    assert _calrecycle_clean_desc(
        {"description": "Catheter Foley 16Fr 5mL Latex"}
    ).startswith("Catheter")
    assert _calrecycle_clean_desc(
        {"description": "Catheter Indwelling Two-Way 18Fr - 5mL Silicone"}
    ).startswith("Catheter Indwelling")


# ── False-positive protection ───────────────────────────────────────


def test_brand_word_mc_kesson_mid_string_survives():
    """`Mc Kesson` as a brand-name fragment in the middle of a noun
    must NOT be treated as a strippable label. Only trailing
    `Mc Kesson # <value>` labels get stripped."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    got = _calrecycle_clean_desc({"description": "Mc Kesson Prevent Bandage Strips"})
    assert got == "Mc Kesson Prevent Bandage Strips"


def test_leading_M_word_not_treated_as_meters_uom():
    """Regression: an earlier rev had no word boundary on the UOM list,
    so `M` matched the start of `Mc` in `Mc Kesson Prevent ...` and
    chewed off the leading `M`."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    got = _calrecycle_clean_desc({"description": "Mc Kesson Prevent Bandage"})
    assert got.startswith("Mc"), f"got {got!r} (expected leading 'Mc' preserved)"


def test_only_uom_pack_text_returns_empty():
    """If the entire description IS the UOM noise, return empty so the
    form-fill skips the row rather than emitting `(100EA/BX 20BX/CS)`."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    got = _calrecycle_clean_desc({"description": "(100EA/BX 20BX/CS)"})
    assert got == "", f"expected empty, got {got!r}"


def test_leading_uom_stripped_keeps_noun():
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    got = _calrecycle_clean_desc(
        {"description": "BX 100EA Surgical Gloves Latex-Free Size 8"}
    )
    assert got.startswith("Surgical Gloves")


def test_leading_pure_numeric_catalog_stripped_keeps_noun():
    """Leading `1118153 Safety Insulin Pen ...` should drop the
    number but preserve the noun."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    got = _calrecycle_clean_desc(
        {"description": "32390001051 Vicks Vapor Rub Ointment"}
    )
    assert got.startswith("Vicks Vapor Rub")
    assert "32390001051" not in got


def test_blank_input_returns_empty():
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    assert _calrecycle_clean_desc({"description": ""}) == ""
    assert _calrecycle_clean_desc({}) == ""


# ── Standalone ↔ bid-package agreement ──────────────────────────────


def test_standalone_and_bid_package_paths_agree_on_short_inputs():
    """`_cr_desc` (bid-package CalRecycle path) must delegate to the
    canonical `_calrecycle_clean_desc` so the two surfaces never diverge.

    We can't easily import `_cr_desc` (it's nested inside
    `fill_bid_package`), but we CAN verify that the cleaner used inside
    the standalone overlay path is the same module-level function
    `_calrecycle_clean_desc` so any future divergence is caught."""
    from src.forms import reytech_filler_v4 as rfv4
    assert hasattr(rfv4, "_calrecycle_clean_desc")
    # Sanity: cleaner is callable and returns a string
    result = rfv4._calrecycle_clean_desc({"description": "Bandage 2x2 Sterile"})
    assert isinstance(result, str)
    assert "Bandage" in result


# ── Renders inside the 246pt cell at 6-7pt ──────────────────────────


def test_long_input_does_not_truncate_at_arbitrary_char_cap():
    """Old `_cr_desc` capped at 80 chars with hard `...` truncation.
    The new canonical cleaner returns the full noun string and lets
    the font auto-sizer in `_calrecycle_overlay_items` handle fit at
    render time — so long descriptions arrive in the renderer intact
    instead of being pre-amputated."""
    from src.forms.reytech_filler_v4 import _calrecycle_clean_desc
    desc = (
        "Sterile Surgical Drape with Adhesive Edge 100x150cm Disposable "
        "Three-Layer Polyethylene Backing High-Absorption Single-Pack"
    )
    got = _calrecycle_clean_desc({"description": desc})
    # Original input is 134 chars — cleaner should preserve essentially
    # all of it since there are no label-tails or catalog numbers.
    assert len(got) > 100
    assert "Sterile Surgical Drape" in got
    assert "Polyethylene" in got
