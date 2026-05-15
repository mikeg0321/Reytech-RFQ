"""PR-AV1 — form-code line-item filter substrate.

Pins the failure class from rfq_efbdef4a / 25CB021 (2026-05-14):
buyer's "Required Forms / Documents" table got parsed as 9 quote line
items (`Darfur`, `STD204`, `STD 1000`, `GSPD-05-106`, `STD843`,
`CalRecycle074`, `CCC`, `Exhibit G`, `VSDS`). Operator faced 16
"items" where 7 were real products and 9 were admin form references.

Tests pin:
  1. Each real form-code value from 25CB021 is classified to its
     canonical form_id (Darfur → darfur_act, etc.)
  2. Each real-product row from 25CB021 (alphanumeric MFG#s) is
     classified as None (= real item, keep it)
  3. filter_form_codes splits a 16-item list into 7 real + 9 form_ids
  4. Empty/malformed inputs fail open (treated as real items, never
     silently dropped)
  5. Description-keyword path catches "Payee Data Record" /
     "DVBE Declaration" / etc. even when part# is empty
"""
from __future__ import annotations

import pytest


def test_classify_part_darfur():
    from src.agents.form_code_filter import classify_item
    assert classify_item({"part": "Darfur"}) == "darfur_act"
    assert classify_item({"part": "DARFUR"}) == "darfur_act"
    assert classify_item({"part": "Darfur Act"}) == "darfur_act"


def test_classify_part_std_series():
    from src.agents.form_code_filter import classify_item
    assert classify_item({"part": "STD204"}) == "std204"
    assert classify_item({"part": "STD 204"}) == "std204"
    assert classify_item({"part": "STD 1000"}) == "std1000"
    assert classify_item({"part": "STD843"}) == "dvbe843"


def test_classify_part_gspd_05_10x():
    """Bidder Declaration ships as GSPD-05-105 or GSPD-05-106 depending
    on form revision. Both must classify to bidder_decl."""
    from src.agents.form_code_filter import classify_item
    assert classify_item({"part": "GSPD-05-105"}) == "bidder_decl"
    assert classify_item({"part": "GSPD-05-106"}) == "bidder_decl"
    assert classify_item({"part": "GSPD 05-106"}) == "bidder_decl"


def test_classify_part_calrecycle():
    from src.agents.form_code_filter import classify_item
    assert classify_item({"part": "CalRecycle074"}) == "calrecycle74"
    assert classify_item({"part": "CalRecycle 074"}) == "calrecycle74"
    assert classify_item({"part": "CALRECYCLE74"}) == "calrecycle74"


def test_classify_part_ccc_vsds_exhibit():
    from src.agents.form_code_filter import classify_item
    assert classify_item({"part": "CCC"}) == "ccc"
    assert classify_item({"part": "VSDS"}) == "vsds"
    assert classify_item({"part": "Exhibit G"}) == "exhibit"
    assert classify_item({"part": "EXHIBIT A"}) == "exhibit"


def test_classify_real_product_part_numbers_NOT_filtered():
    """The 7 real product MFG#s from 25CB021 must NOT classify as
    form codes. False-positives here would silently drop real items."""
    from src.agents.form_code_filter import classify_item
    real_parts = [
        "2010017786",      # generic 10-digit catalog #
        "QP210/80",        # vendor SKU with slash
        "WCA86920DOES",    # alphanumeric long string
        "B0F7X7QM6H",      # Amazon ASIN
        "MVP428-2XL",      # vendor SKU with size suffix
        "B0CL5JTQT8",      # Amazon ASIN
        "1400",            # plain integer SKU — must NOT eat as STD???
    ]
    for p in real_parts:
        assert classify_item({"part": p}) is None, (
            f"FALSE POSITIVE on real product MFG# {p!r} — would silently "
            f"drop a real quote line."
        )


def test_classify_part_700_series_real_products_safe():
    """Confirm "1400" (real qty/SKU) doesn't match the STD/DVBE shape.
    Also confirm part numbers starting with digits >999 are safe."""
    from src.agents.form_code_filter import classify_item
    assert classify_item({"part": "1400"}) is None
    assert classify_item({"part": "70499"}) is None
    assert classify_item({"part": "204X"}) is None  # not bare STD


def test_filter_splits_25cb021_realistic():
    """Mirror the actual rfq_efbdef4a item list — 7 real + 9 form codes
    → filter must return 7 real items and 9 distinct form_ids."""
    from src.agents.form_code_filter import filter_form_codes

    items = [
        # Real products (items 0-6)
        {"part": "2010017786", "description": "Catalog item"},
        {"part": "QP210/80", "description": "Vendor SKU"},
        {"part": "WCA86920DOES", "description": "Alphanumeric"},
        {"part": "B0F7X7QM6H", "description": "Amazon ASIN"},
        {"part": "MVP428-2XL", "description": "Sized SKU"},
        {"part": "B0CL5JTQT8", "description": "Amazon ASIN"},
        {"part": "1400", "description": "Plain integer SKU"},
        # Form-code rows (items 7-15)
        {"part": "Darfur", "description": "Darfur Contracting Act"},
        {"part": "STD204", "description": "Payee Data Record"},
        {"part": "STD 1000", "description": "GenAI Reporting"},
        {"part": "GSPD-05-106", "description": "Bidder Declaration"},
        {"part": "STD843", "description": "DVBE Declaration"},
        {"part": "CalRecycle074", "description": "Postconsumer Content"},
        {"part": "CCC", "description": "Conflict-of-Interest Cert"},
        {"part": "Exhibit G", "description": "Bid attachments"},
        {"part": "VSDS", "description": "Vendor Self-Disclosure"},
    ]
    real, form_ids = filter_form_codes(items)
    assert len(real) == 7, (
        f"Expected 7 real items, got {len(real)}: "
        f"{[r.get('part') for r in real]}"
    )
    assert len(form_ids) == 9, (
        f"Expected 9 form_ids, got {len(form_ids)}: {form_ids}"
    )
    # Real items preserved in order
    assert [r["part"] for r in real] == [
        "2010017786", "QP210/80", "WCA86920DOES", "B0F7X7QM6H",
        "MVP428-2XL", "B0CL5JTQT8", "1400",
    ]
    # Canonical form_ids present
    assert "darfur_act" in form_ids
    assert "std204" in form_ids
    assert "std1000" in form_ids
    assert "bidder_decl" in form_ids
    assert "dvbe843" in form_ids
    assert "calrecycle74" in form_ids
    assert "ccc" in form_ids
    assert "exhibit" in form_ids
    assert "vsds" in form_ids


def test_filter_empty_input_safe():
    from src.agents.form_code_filter import filter_form_codes
    real, fids = filter_form_codes([])
    assert real == [] and fids == []
    real, fids = filter_form_codes(None)
    assert real == [] and fids == []


def test_filter_fail_open_on_malformed_item():
    """A None or non-dict entry should fail open (kept as 'real' so
    operator can see + delete, not silently dropped)."""
    from src.agents.form_code_filter import filter_form_codes
    items = [{"part": "Darfur"}, None, "garbage", {"part": "REAL-001"}]
    real, fids = filter_form_codes(items)
    # Darfur dropped, REAL-001 kept; None/string fail-open kept
    assert len(real) == 3, real
    assert fids == ["darfur_act"]


def test_description_keyword_path_no_part():
    """When part# is missing but description spells out the form name
    in short form, classify via description path."""
    from src.agents.form_code_filter import classify_item
    assert classify_item({
        "part": "", "description": "Payee Data Record"
    }) == "std204"
    assert classify_item({
        "part": "", "description": "DVBE Declaration"
    }) == "dvbe843"
    assert classify_item({
        "part": "", "description": "Darfur Contracting Act"
    }) == "darfur_act"


def test_description_long_form_NOT_misclassified():
    """A long realistic product description that happens to contain a
    keyword fragment must NOT classify as a form code. Threshold = 80
    chars of stripped desc."""
    from src.agents.form_code_filter import classify_item
    # 100+ char realistic product blurb
    long_desc = (
        "Medical exam table with payee data record-keeping drawer "
        "and CCC certification compliance, qty 1 EA, MFG# EXAM-001"
    )
    assert classify_item({"part": "EXAM-001", "description": long_desc}) is None


def test_dedup_form_ids():
    """If two items both classify to the same form_id, the form_ids
    list dedupes — required_forms shouldn't include the same form
    twice."""
    from src.agents.form_code_filter import filter_form_codes
    items = [
        {"part": "STD204"},
        {"part": "STD 204"},                       # duplicate via spacing
        {"part": "", "description": "Payee Data Record"},  # match via desc
    ]
    real, fids = filter_form_codes(items)
    assert len(real) == 0, real  # all classified
    assert fids == ["std204"]    # deduped


def test_part_field_aliases():
    """The line-item parser uses different field names (`part`,
    `item_number`, `mfg_number`). All should be checked."""
    from src.agents.form_code_filter import classify_item
    assert classify_item({"item_number": "STD843"}) == "dvbe843"
    assert classify_item({"mfg_number": "STD843"}) == "dvbe843"
    assert classify_item({"part_number": "STD843"}) == "dvbe843"
