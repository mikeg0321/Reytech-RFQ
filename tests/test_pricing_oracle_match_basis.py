"""Regression: MFG#-first partition, poison-pill guard, brand-anchor signal.

Implements the 2026-04-23 product-engineer review of PR #485's wide-delta
problem (Stanley RoamAlert: YOUR PRICE $540 vs SCPRS AVG $113.06).

Three guards under test:

1. **MFG# poison-pill guard** — a row pulled in by `OR LOWER(item_number)=?`
   in a search function whose description shares <40% similarity with the
   target gets demoted from `match_basis="mfg_number"` to `"description"`.
   Defends against a single mis-keyed `item_number` row anchoring the
   weighted average.

2. **MFG# partition** (flag `oracle.mfg_first_partition`, default off) —
   when ≥2 rows survive as `match_basis="mfg_number"`, scprs_avg /
   competitor_low / reytech_avg are computed from MFG# rows only.
   Description-only rows stay in `samples` for context but stop driving
   the average. Honors `feedback_match_identifiers_first`.

3. **Brand-anchor signal** — when the target description has a real brand
   token (capitalized in source + not in the generic-noun stop list),
   each sample row carries `brand_anchored: True | False` based on
   whether the row's own description contains the brand. Generic
   descriptions (no brand) → `brand_anchored=None`.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.core.pricing_oracle_v2 import (
    _analyze_market_prices,
    _detect_brand_token,
    _tokenize,
)


def _row(price, *, quantity=1, description="", po_number="", supplier="",
         date="2025-08-01", source="scprs_po_lines", match_basis="description",
         is_reytech=False):
    return {
        "price": price,
        "quantity": quantity,
        "description": description,
        "po_number": po_number,
        "supplier": supplier,
        "department": "5225",
        "uom": "EA",
        "date": date,
        "source": source,
        "match_basis": match_basis,
        "is_reytech": is_reytech,
    }


# ── MFG# poison-pill guard ─────────────────────────────────────────────


def test_mfg_basis_demoted_when_description_too_dissimilar():
    """An mfg_number-tagged row whose description shares <40% similarity
    with the target gets demoted to description-basis (poison pill)."""
    rows = [
        _row(50.0, description="Garden hose 50ft rubber",
             match_basis="mfg_number", po_number="P-POISON"),
        _row(220.0, description="Stanley RoamAlert wrist strap 6in",
             match_basis="description", po_number="P-LEGIT"),
    ]
    out = _analyze_market_prices(
        rows, request_qty=1, target_quantity=1,
        target_description="Stanley RoamAlert Wrist Strap 6in",
    )
    by_po = {s["po_number"]: s for s in out["samples"]}
    assert by_po["P-POISON"]["match_basis"] == "description", (
        "Poison-pill mfg_number row not demoted"
    )
    assert by_po["P-LEGIT"]["match_basis"] == "description"


def test_mfg_basis_kept_when_description_similar_enough():
    """An mfg_number-tagged row whose description is similar to target
    keeps its mfg_number basis."""
    rows = [
        _row(220.0, description="Stanley RoamAlert wrist strap 6 in",
             match_basis="mfg_number", po_number="P-MATCH"),
    ]
    out = _analyze_market_prices(
        rows, request_qty=1, target_quantity=1,
        target_description="Stanley RoamAlert Wrist Strap 6in",
    )
    s = out["samples"][0]
    assert s["match_basis"] == "mfg_number"


def test_mfg_basis_skipped_when_no_target_description():
    """Without target_description, the poison-pill guard can't run —
    mfg_number tag passes through untouched."""
    rows = [
        _row(50.0, description="Anything", match_basis="mfg_number",
             po_number="P-X"),
    ]
    out = _analyze_market_prices(rows, request_qty=1, target_quantity=1)
    assert out["samples"][0]["match_basis"] == "mfg_number"


# ── MFG# partition (flag-gated) ────────────────────────────────────────


def _flag(value):
    """Patch context: simulate `oracle.mfg_first_partition` flag."""
    def _get_flag(key, default=False):
        if key == "oracle.mfg_first_partition":
            return value
        return default
    return patch("src.core.flags.get_flag", side_effect=_get_flag)


def test_partition_off_by_default_uses_blended_average():
    rows = [
        _row(100.0, description="Stanley RoamAlert wrist strap",
             match_basis="mfg_number", po_number="P-A"),
        _row(110.0, description="Stanley RoamAlert wrist strap 6in",
             match_basis="mfg_number", po_number="P-B"),
        _row(20.0, description="generic wrist strap", po_number="P-CHEAP"),
    ]
    out = _analyze_market_prices(
        rows, request_qty=1, target_quantity=1,
        target_description="Stanley RoamAlert Wrist Strap",
    )
    assert out["mfg_partition_active"] is False
    # Blended weighted_avg includes the cheap row → drags average down.
    assert out["weighted_avg"] < 90


def test_partition_on_with_two_mfg_rows_anchors_average():
    """With flag on AND ≥2 surviving mfg_number rows, the average is
    computed from MFG# rows only — the cheap description-only row no
    longer drags it down."""
    rows = [
        _row(100.0, description="Stanley RoamAlert wrist strap",
             match_basis="mfg_number", po_number="P-A"),
        _row(110.0, description="Stanley RoamAlert wrist strap 6in",
             match_basis="mfg_number", po_number="P-B"),
        _row(20.0, description="generic wrist strap", po_number="P-CHEAP"),
    ]
    with _flag(True):
        out = _analyze_market_prices(
            rows, request_qty=1, target_quantity=1,
            target_description="Stanley RoamAlert Wrist Strap",
        )
    assert out["mfg_partition_active"] is True
    assert out["mfg_anchored_count"] == 2
    # Weighted avg of 100 and 110 (similar weights) ≈ 105 — well above
    # the blended ~76 from the previous test.
    assert out["weighted_avg"] > 95
    # Drill-down still includes the cheap row for context.
    pos = {s["po_number"] for s in out["samples"]}
    assert "P-CHEAP" in pos


def test_partition_holds_off_when_only_one_mfg_row():
    """Single mfg_number row isn't enough to trust the partition — fall
    back to blended."""
    rows = [
        _row(100.0, description="Stanley RoamAlert wrist strap",
             match_basis="mfg_number", po_number="P-A"),
        _row(20.0, description="generic wrist strap", po_number="P-CHEAP"),
    ]
    with _flag(True):
        out = _analyze_market_prices(
            rows, request_qty=1, target_quantity=1,
            target_description="Stanley RoamAlert Wrist Strap",
        )
    assert out["mfg_partition_active"] is False
    assert out["mfg_anchored_count"] == 1


# ── Brand detector + brand-anchor signal ───────────────────────────────


def test_brand_detector_fires_for_capitalized_brand():
    src = "Stanley RoamAlert Wrist Strap, 6 in"
    groups = _tokenize(src)
    assert _detect_brand_token(src, groups) is True


def test_brand_detector_skips_generic_first_token():
    """Stop-noun first tokens like 'wrist', 'cane', 'wheel' aren't
    brands even if capitalized."""
    for desc in ("Wrist Strap, 6 in", "Cane Tip Heavy Duty",
                 "Wheel Locks Anti-Roll Back"):
        groups = _tokenize(desc)
        assert _detect_brand_token(desc, groups) is False, desc


def test_brand_detector_skips_lowercase_in_source():
    """A lowercase token in source is not a brand even if not in the
    stop list (e.g., 'sedeo' lowercased)."""
    src = "sedeo armrest pad replacement"
    groups = _tokenize(src)
    assert _detect_brand_token(src, groups) is False


def test_brand_anchored_tag_when_brand_detected_and_present_in_row():
    rows = [
        _row(220.0, description="Stanley RoamAlert wrist strap 6in",
             po_number="P-WITH-BRAND"),
        _row(20.0, description="generic wrist strap",
             po_number="P-NO-BRAND"),
    ]
    out = _analyze_market_prices(
        rows, request_qty=1, target_quantity=1,
        target_description="Stanley RoamAlert Wrist Strap",
        target_token_groups=_tokenize("Stanley RoamAlert Wrist Strap"),
    )
    assert out["brand_detected"] is True
    by_po = {s["po_number"]: s for s in out["samples"]}
    assert by_po["P-WITH-BRAND"]["brand_anchored"] is True
    assert by_po["P-NO-BRAND"]["brand_anchored"] is False


def test_brand_anchored_none_when_target_has_no_brand():
    """Generic target description → brand_anchored is None on every
    sample (not applicable signal)."""
    rows = [_row(100.0, description="any wrist strap", po_number="P-X")]
    out = _analyze_market_prices(
        rows, request_qty=1, target_quantity=1,
        target_description="Wrist Strap 6 in",
        target_token_groups=_tokenize("Wrist Strap 6 in"),
    )
    assert out["brand_detected"] is False
    assert out["samples"][0]["brand_anchored"] is None
