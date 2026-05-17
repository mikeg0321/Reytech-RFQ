"""The Spine — quote matcher.

Pure-function unit tests on the scoring rules. No DB, no I/O. The
ingest hookup that consumes these scores is tested in
test_shadow_ingest.py.

The scoring rules ARE substrate — changes to weights or thresholds
change what auto-links happen in prod. Update the tests in lockstep.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.spine.model import LineItem, Quote
from src.spine.quote_matcher import (
    AUTO_LINK_THRESHOLD,
    find_pc_candidates,
    score_quote_pair,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _fresh() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _line(
    line_no: int = 1,
    *,
    description: str = "Test item",
    mfg_number: str = "MFG-1",
    qty: int = 1,
    cost_cents: int = 5000,
    unit_price_cents: int = 6750,
) -> LineItem:
    return LineItem(
        line_no=line_no,
        description=description,
        mfg_number=mfg_number,
        qty=qty,
        uom="EA",
        cost_cents=cost_cents,
        cost_source_url="https://supplier.example.com/sku",
        cost_validated_at=_fresh(),
        unit_price_cents=unit_price_cents,
    )


def _quote(
    quote_id: str,
    *,
    facility: str = "SATF",
    solicitation_number: str = "10847262",
    line_items: list[LineItem] | None = None,
) -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility=facility,
        solicitation_number=solicitation_number,
        line_items=line_items or [_line(1)],
        tax_rate_bps=825,
    )


# ──────────────────────────────────────────────────────────────────────
# score_quote_pair — signals
# ──────────────────────────────────────────────────────────────────────


def test_different_facility_returns_zero():
    """Hard filter: different facilities → 0.0 regardless of other signals."""
    a = _quote("qq-a", facility="SATF")
    b = _quote("qq-b", facility="CHCF")  # same sol#, same MFG → would otherwise be high
    s = score_quote_pair(a, b)
    assert s["confidence"] == 0.0
    assert s["evidence"]["same_facility"] is False


def test_same_facility_case_insensitive():
    a = _quote("qq-a", facility="SATF")
    b = _quote("qq-b", facility="satf")
    s = score_quote_pair(a, b)
    assert s["evidence"]["same_facility"] is True


def test_same_facility_strips_whitespace():
    a = _quote("qq-a", facility="SATF")
    b = _quote("qq-b", facility="  SATF  ")
    s = score_quote_pair(a, b)
    assert s["evidence"]["same_facility"] is True


def test_no_mfg_no_sol_match_returns_zero():
    """Descriptions overlap but neither sol# nor MFG# match → refuse."""
    a = _quote(
        "qq-a",
        solicitation_number="SOL-A",
        line_items=[_line(1, mfg_number="UNIQUE-A", description="bandage gauze sterile pack")],
    )
    b = _quote(
        "qq-b",
        solicitation_number="SOL-B",
        line_items=[_line(1, mfg_number="UNIQUE-B", description="bandage gauze sterile pack")],
    )
    s = score_quote_pair(a, b)
    assert s["confidence"] == 0.0
    assert s["evidence"]["mfg_overlap_ratio"] == 0.0
    assert s["evidence"]["same_solicitation_number"] is False


def test_same_solicitation_alone_passes_threshold():
    """Buyer references prior solicitation # — strong signal even with
    no MFG# overlap. Confidence = 0.50 (the threshold)."""
    a = _quote(
        "qq-a",
        solicitation_number="10847262",
        line_items=[_line(1, mfg_number="X-1", description="cotton tape")],
    )
    b = _quote(
        "qq-b",
        solicitation_number="10847262",
        line_items=[_line(1, mfg_number="Y-1", description="unrelated stuff")],
    )
    s = score_quote_pair(a, b)
    assert s["confidence"] >= AUTO_LINK_THRESHOLD
    assert s["evidence"]["same_solicitation_number"] is True


def test_full_mfg_overlap_same_sol_max_confidence():
    """Every line MFG# matches AND same sol# → very high confidence."""
    a = _quote("qq-a", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage 4 inch"),
        _line(2, mfg_number="MFG-2", description="gauze sterile 4x4"),
    ])
    b = _quote("qq-b", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage 4 inch"),
        _line(2, mfg_number="MFG-2", description="gauze sterile 4x4"),
    ])
    s = score_quote_pair(a, b)
    # 0.50 (sol) + 0.35 (mfg) + 0.15 * 1.0 (desc) = 1.00
    assert s["confidence"] == 1.0
    assert s["evidence"]["mfg_overlap_ratio"] == 1.0
    assert s["evidence"]["mfg_matches_count"] == 2


def test_partial_mfg_overlap_different_sol():
    """Half the MFG#s match, different sol# → 0.35 * 0.5 + desc weight."""
    a = _quote("qq-a", solicitation_number="SOL-A", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-2", description="gauze"),
    ])
    b = _quote("qq-b", solicitation_number="SOL-B", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-X", description="syringe"),
    ])
    s = score_quote_pair(a, b)
    # MFG overlap = 1/2 = 0.5 → 0.35 * 0.5 = 0.175
    # desc: target = {bandage, gauze}; cand = {bandage, syringe};
    #   union = 3, intersection = 1 → Jaccard 0.333
    # confidence = 0.175 + 0.15 * 0.333 ≈ 0.225
    assert s["evidence"]["mfg_overlap_ratio"] == 0.5
    assert 0.20 <= s["confidence"] <= 0.25


def test_mfg_overlap_below_threshold_no_sol():
    """1 of 4 MFG# matches without sol# → below 0.50 threshold."""
    a = _quote("qq-a", solicitation_number="A", line_items=[
        _line(1, mfg_number="MFG-1", description="x"),
        _line(2, mfg_number="MFG-2", description="x"),
        _line(3, mfg_number="MFG-3", description="x"),
        _line(4, mfg_number="MFG-4", description="x"),
    ])
    b = _quote("qq-b", solicitation_number="B", line_items=[
        _line(1, mfg_number="MFG-1", description="x"),
        _line(2, mfg_number="MFG-Z", description="y"),
    ])
    s = score_quote_pair(a, b)
    assert s["confidence"] < AUTO_LINK_THRESHOLD


def test_mfg_normalization_strips_whitespace_and_case():
    a = _quote("qq-a", line_items=[_line(1, mfg_number="  mfg-1 ", description="x")])
    b = _quote("qq-b", line_items=[_line(1, mfg_number="MFG-1", description="x")])
    s = score_quote_pair(a, b)
    assert s["evidence"]["mfg_overlap_ratio"] == 1.0


def test_mfg_normalization_strips_trailing_punctuation():
    a = _quote("qq-a", line_items=[_line(1, mfg_number="MFG-1.", description="x")])
    b = _quote("qq-b", line_items=[_line(1, mfg_number="MFG-1", description="x")])
    s = score_quote_pair(a, b)
    assert s["evidence"]["mfg_overlap_ratio"] == 1.0


def test_short_description_tokens_ignored():
    """Tokens shorter than 3 chars don't enter Jaccard."""
    a = _quote("qq-a", line_items=[_line(1, description="a to be of it")])
    b = _quote("qq-b", line_items=[_line(1, description="x y z")])
    s = score_quote_pair(a, b)
    assert s["evidence"]["desc_jaccard"] == 0.0


def test_evidence_includes_counts():
    a = _quote("qq-a", line_items=[
        _line(1, mfg_number="MFG-1", description="x"),
        _line(2, mfg_number="MFG-2", description="x"),
        _line(3, mfg_number="MFG-3", description="x"),
    ])
    b = _quote("qq-b", line_items=[
        _line(1, mfg_number="MFG-1", description="x"),
        _line(2, mfg_number="MFG-2", description="x"),
    ])
    s = score_quote_pair(a, b)
    assert s["evidence"]["target_mfg_count"] == 3
    assert s["evidence"]["mfg_matches_count"] == 2


# ──────────────────────────────────────────────────────────────────────
# find_pc_candidates — top-N selection + threshold filter
# ──────────────────────────────────────────────────────────────────────


def test_find_pc_candidates_returns_sorted_above_threshold():
    target = _quote("target", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-2", description="gauze"),
    ])
    # cand_a: high (same sol# + full mfg)
    cand_a = _quote("cand-a", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-2", description="gauze"),
    ])
    # cand_b: medium (different sol#, full mfg)
    cand_b = _quote("cand-b", solicitation_number="DIFF", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-2", description="gauze"),
    ])
    # cand_c: noise (different facility)
    cand_c = _quote("cand-c", facility="CHCF", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-2", description="gauze"),
    ])
    out = find_pc_candidates(target, [cand_a, cand_b, cand_c])
    assert [c["quote_id"] for c in out] == ["cand-a", "cand-b"]
    assert out[0]["confidence"] > out[1]["confidence"]


def test_find_pc_candidates_skips_self_in_input():
    """Defensive: if the caller accidentally passes target in candidates,
    we drop it silently — self-link would otherwise show as max confidence."""
    target = _quote("target", line_items=[
        _line(1, mfg_number="MFG-1", description="x"),
    ])
    out = find_pc_candidates(target, [target])
    assert out == []


def test_find_pc_candidates_returns_empty_when_nothing_passes():
    target = _quote("target", line_items=[
        _line(1, mfg_number="UNIQUE-A", description="alpha beta"),
    ])
    weak = _quote("weak", solicitation_number="DIFF", line_items=[
        _line(1, mfg_number="UNIQUE-B", description="gamma delta"),
    ])
    out = find_pc_candidates(target, [weak])
    assert out == []


def test_find_pc_candidates_custom_threshold():
    """Caller can lower the bar — operator-search surface might want
    a wider net than auto-linking."""
    target = _quote("target", solicitation_number="A", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-2", description="gauze"),
    ])
    weak = _quote("weak", solicitation_number="B", line_items=[
        _line(1, mfg_number="MFG-1", description="bandage"),
        _line(2, mfg_number="MFG-Z", description="syringe"),
    ])
    # At default threshold (0.50) this won't qualify.
    assert find_pc_candidates(target, [weak]) == []
    # At lower threshold, it does — score around 0.225.
    out = find_pc_candidates(target, [weak], min_confidence=0.20)
    assert len(out) == 1
    assert out[0]["quote_id"] == "weak"


# ──────────────────────────────────────────────────────────────────────
# Threshold sanity
# ──────────────────────────────────────────────────────────────────────


def test_auto_link_threshold_is_documented_value():
    """If we change AUTO_LINK_THRESHOLD, tests that depend on it must
    update too. This test is the canary for substrate behavior change."""
    assert AUTO_LINK_THRESHOLD == 0.50
