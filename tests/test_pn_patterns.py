"""PR-1 / Phase 2 substrate — MFG# extractor labeled-priority + UOM denylist.

Mike P0 live-drive 2026-05-12 (rfq_8efe9fae): items 11/13/15 shipped
with wrong MFG#s — item 11 had `100EA` (a unit-of-measure token from
`(100EA/BX 20BX/CS)`), item 13 had `97QCNKJP` (ASIN suffix), item 15
was blank despite the description containing `Manufacturer # 32390001051`.

The fix has three parts and this file pins all three:
  1. Labeled-priority patterns in both extractors (`Manufacturer #`,
     `Mc Kesson #`, `Mfr #`, `OEM #`) — these were missing from the
     existing list, which only knew `MFG #` / `Part #` / `Item #`.
  2. UOM denylist (`_looks_like_uom_token`) — rejects candidates that
     look like pack/unit markers (`BX`, `100EA`, `BX20BX`, `20BX/CS`).
  3. Label-stop truncation — when descriptions glue two labels together
     ("Mc Kesson # 161574Manufacturer # 4062") the labeled capture
     would otherwise bleed through to the next label-word.

Per CLAUDE.md the project had no `test_pn_patterns.py` despite the
file citing `_PN_PATTERNS` coverage — this is that file.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── UOM denylist helpers ──────────────────────────────────────────


def test_uom_denylist_blocks_unit_tokens_in_price_check():
    """`_looks_like_uom_token` must flag every UOM/pack token shape
    observed in McKesson descriptions."""
    from src.forms.price_check import _looks_like_uom_token
    blocked = [
        "EA", "BX", "CS", "PK", "20BX", "100EA",
        "BX20BX", "EA10BX", "20BX/CS", "100EA/BX",
        "10PK", "PK10", "OZ", "LB",
    ]
    for s in blocked:
        assert _looks_like_uom_token(s), f"{s!r} should be flagged as UOM noise"


def test_uom_denylist_passes_real_part_numbers_in_price_check():
    """No false positives on real MFG#s from today's drive corpus."""
    from src.forms.price_check import _looks_like_uom_token
    real_pns = [
        "16-N8MMPA",       # Safety Insulin Pen Needle
        "H-3989",           # Uline dry erase board
        "1118153",          # McKesson Pen Needle catalog
        "32390001051",      # Vicks UPC-style
        "454621", "454620", # McKesson bandages
        "AWD-5-1010C",      # Osnovation Enluxtra
        "FW2B-6",           # First Wave Pill Crusher
        "W12919",           # S&S Worldwide format
        "FN4368",           # Pencil-style MFG#
        "NL304",            # 2-letter + digits
        "16753",            # 5-digit numeric code
    ]
    for s in real_pns:
        assert not _looks_like_uom_token(s), (
            f"FALSE POSITIVE: {s!r} is a real MFG# but flagged as UOM"
        )


def test_uom_denylist_mirror_in_product_research():
    """The product_research twin must agree with price_check on the
    same inputs — both extractors share the same UOM denylist."""
    from src.forms.price_check import _looks_like_uom_token as _u_pc
    from src.agents.product_research import _looks_like_uom_token as _u_pr
    for s in ["BX", "100EA", "BX20BX", "16-N8MMPA", "454621", "FW2B-6"]:
        assert _u_pc(s) == _u_pr(s), (
            f"price_check and product_research disagree on {s!r}: "
            f"pc={_u_pc(s)} pr={_u_pr(s)}"
        )


# ── _extract_part_number: labeled-priority + label-stop ───────────


def test_extract_part_number_grabs_manufacturer_label():
    """Adding 'Manufacturer' to the labeled-priority list — old code
    only knew MFG / Mfg / Part / Item / SKU."""
    from src.forms.price_check import _extract_part_number
    text = "Some product Manufacturer # 16-N8MMPA"
    assert _extract_part_number(text) == "16-N8MMPA"


def test_extract_part_number_grabs_mc_kesson_label():
    """McKesson catalog descriptions use 'Mc Kesson #' as the label."""
    from src.forms.price_check import _extract_part_number
    text = "Some catalog item Mc Kesson # 161574"
    assert _extract_part_number(text) == "161574"


def test_extract_part_number_rejects_uom_tokens():
    """Item 11 incident: description had '(100EA/BX 20BX/CS)' and the
    old positional regex picked up 'BX20BX' or similar as the MFG#.
    Now the UOM denylist refuses those candidates."""
    from src.forms.price_check import _extract_part_number
    # Strip the labeled patterns so we exercise the positional fallback
    text = "(100EA/BX 20BX/CS)"
    # No labeled-MFG signal, and the only candidate the positional regex
    # can find IS a UOM token — must return "" rather than UOM noise.
    assert _extract_part_number(text) == ""


def test_extract_part_number_item11_live_drive():
    """The exact item-11 description from rfq_8efe9fae — should now
    extract the McKesson catalog # (1118153) — NOT '100EA' or 'BX20BX'."""
    from src.forms.price_check import _extract_part_number
    desc = (
        "/BX20BX/CS 1118153 Safety Insulin Pen Needle Mc Kesson Prevent "
        "31 Gauge8 mm Length Active Back-End ProtectionNEEDLE, SAFETY "
        "PEN 31GX8MM (100EA/BX 20BX/CS)Mc Kesson # 1118153Manufacturer # 16-N8MMPA"
    )
    got = _extract_part_number(desc)
    # Either the catalog # or the manufacturer # are acceptable real MFG#s.
    # Forbidden: any UOM-shaped token.
    assert got in ("1118153", "16-N8MMPA"), (
        f"item11 extraction returned {got!r}; expected 1118153 or 16-N8MMPA"
    )


def test_extract_part_number_item15_live_drive():
    """Item 15 description had 'Manufacturer # 32390001051' but the
    old extractor returned blank because 'Manufacturer' wasn't in the
    labeled-priority list."""
    from src.forms.price_check import _extract_part_number
    desc = (
        "Chest Rub Vicks Vapo Rub 4.8% - 1.2% - 2.6%Strength Ointment "
        "1.76 oz.c Kesson # 833168Manufacturer # 32390001051"
    )
    assert _extract_part_number(desc) == "32390001051"


def test_extract_part_number_label_stop_no_label_bleed():
    """When two labels are glued together ('Mc Kesson # 161574Manufacturer
    # 4062'), the capture must NOT eat through the next label-word."""
    from src.forms.price_check import _extract_part_number
    desc = (
        "Penlight White Light DisposablePENLIGHT, DIAGNOSTIC GRAFCO (6/PK)"
        "Mc Kesson # 161574Manufacturer # 4062"
    )
    got = _extract_part_number(desc)
    assert got in ("161574", "4062"), (
        f"label-stop should yield 161574 or 4062, got {got!r}"
    )
    # Specifically must NOT bleed into the next label
    assert "Manufacturer" not in got
    assert "Mfg" not in got
    assert "OEM" not in got


# ── _extract_mfg_info: labeled-priority + UOM gate ────────────────


def test_extract_mfg_info_pulls_labeled_manufacturer():
    from src.agents.product_research import _extract_mfg_info
    title = "Safety Insulin Pen Needle (100EA/BX 20BX/CS) Manufacturer # 16-N8MMPA"
    r = _extract_mfg_info(title)
    assert r["mfg_number"] == "16-N8MMPA"


def test_extract_mfg_info_no_longer_returns_uom_token_from_pack_text():
    """Item 11 root cause: the positional regex was matching 'BX20BX'
    or 'EA10BX' from the title's pack markers. With UOM denylist these
    are rejected."""
    from src.agents.product_research import _extract_mfg_info
    title = "Safety Insulin Pen Needle (100EA/BX 20BX/CS)"
    r = _extract_mfg_info(title)
    # The title has NO labeled MFG#, and every positional candidate is
    # a UOM token. So mfg_number must be empty — never a UOM token.
    assert r["mfg_number"] == "", (
        f"Expected empty mfg_number for UOM-only title, got {r['mfg_number']!r}"
    )


def test_extract_mfg_info_label_stop():
    """product_research twin of the label-stop test."""
    from src.agents.product_research import _extract_mfg_info
    title = "Item Mc Kesson # 161574Manufacturer # 4062"
    r = _extract_mfg_info(title)
    assert r["mfg_number"] in ("161574", "4062"), r
    assert "Manufacturer" not in r["mfg_number"]


# ── Corpus regression: real MFG#s from CLAUDE.md guidance ─────────


def test_corpus_known_mfg_shapes_pass_through():
    """CLAUDE.md cites these MFG# shapes as legitimate. None should be
    flagged as UOM tokens, and a description with the labeled value
    should extract it cleanly."""
    from src.forms.price_check import _extract_part_number, _looks_like_uom_token
    shapes = ["W12919", "FN4368", "NL304", "16753"]
    for shape in shapes:
        assert not _looks_like_uom_token(shape), f"{shape} flagged as UOM"
        # Wrap in a description with a recognized label and extract
        desc = f"Generic product MFG # {shape}"
        assert _extract_part_number(desc) == shape, (
            f"Failed to extract {shape!r} from labeled description"
        )
