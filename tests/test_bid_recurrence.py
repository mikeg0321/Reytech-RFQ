"""Tests for `src/core/bid_recurrence.py` — Mike's 2026-05-05 ask:
detect when the same institution sends substantially the same items+qty
back for re-bid, surface as a chip on the PC detail page so the operator
can reuse prior pricing.

Match contract:
  - same canonical institution (lowercased + whitespace-collapsed)
  - per-item: description token-overlap >= 0.65 AND qty equal
  - >= 75% of CURRENT items have a match in the prior record

Read-side only — these tests pin the match function shape; ingest-side
persistence is a follow-up PR.

Failure modes pinned:
  1. Different institutions never match (durable signal).
  2. Same desc but different qty doesn't match (different bid scale).
  3. Subset / superset cases — asymmetric overlap definition.
  4. Empty inputs short-circuit cleanly (don't crash detail render).
  5. URL prefix differs PC vs RFQ (id-prefix detection).
  6. Most-recent ordering — operator scans top-down.
  7. The current record itself is never returned (record_id exclusion).
"""
from __future__ import annotations

from src.core.bid_recurrence import (
    DEFAULT_DESC_THRESHOLD,
    DEFAULT_OVERLAP_THRESHOLD,
    _description_overlap,
    _items_match,
    _items_overlap_pct,
    _normalize_institution,
    _qty_int,
    find_recurring_bids,
)


# ─── Building blocks ─────────────────────────────────────────────────────

def test_normalize_institution_canonical():
    assert _normalize_institution("CCHCS") == "cchcs"
    assert _normalize_institution("  CIW  RHU ") == "ciw rhu"
    assert _normalize_institution("CIW   RHU") == "ciw rhu"  # collapse spaces
    assert _normalize_institution(None) == ""
    assert _normalize_institution("") == ""


def test_qty_int_coerces():
    assert _qty_int(10) == 10
    assert _qty_int("10") == 10
    assert _qty_int(10.0) == 10
    assert _qty_int("10.0") == 10
    assert _qty_int(None) is None
    assert _qty_int("") is None
    assert _qty_int("not a number") is None


def test_description_overlap_jaccard():
    # Identical → 1.0
    assert _description_overlap("Heel Donut Cushion", "Heel Donut Cushion") == 1.0
    # Disjoint → 0.0
    assert _description_overlap("Echo Dot Speaker", "Heel Donut Cushion") == 0.0
    # Partial — Mike's exact case shape
    overlap = _description_overlap(
        "Love Velvet - Fuzzy Coloring Poster",
        "Love Velvet Fuzzy Velvet Coloring Poster Stuff2Color",
    )
    assert overlap >= DEFAULT_DESC_THRESHOLD


# ─── Per-item match ──────────────────────────────────────────────────────

def test_items_match_same_desc_same_qty():
    a = {"description": "Love Velvet Fuzzy Coloring Poster", "qty": 10}
    b = {"description": "Love Velvet Fuzzy Coloring Poster Stuff2Color", "qty": 10}
    assert _items_match(a, b)


def test_items_match_different_qty_rejects():
    """Same-description but qty differs is a different bid scale, not a
    recurrence. Mike's heuristic: same QTY is a bid indicator."""
    a = {"description": "Heel Donut Cushion", "qty": 10}
    b = {"description": "Heel Donut Cushion", "qty": 50}
    assert not _items_match(a, b)


def test_items_match_qty_quantity_field_aliases():
    """Item shape varies: some carry `qty`, some `quantity`. Both must
    work — feedback_global_fix_not_one_off says read every key the
    pipeline writes."""
    a = {"description": "Foo", "qty": 5}
    b = {"description": "Foo", "quantity": 5}
    assert _items_match(a, b)


def test_items_match_missing_qty_rejects():
    """No qty signal = can't confirm bid scale = no match."""
    a = {"description": "Foo"}
    b = {"description": "Foo", "qty": 5}
    assert not _items_match(a, b)


def test_items_match_low_desc_overlap_rejects():
    """Below the description threshold, even matching qty is not enough."""
    a = {"description": "Echo Dot Smart Speaker", "qty": 10}
    b = {"description": "Heel Donut Pressure Relief Cushion", "qty": 10}
    assert not _items_match(a, b)


# ─── Items overlap percentage ────────────────────────────────────────────

def test_items_overlap_pct_identical_lists():
    items = [
        {"description": "Love Velvet Fuzzy Coloring Poster", "qty": 10},
        {"description": "Heart Hands Fuzzy Velvet Coloring Poster", "qty": 10},
        {"description": "Butterfly Eyes Fuzzy Coloring Poster", "qty": 10},
    ]
    assert _items_overlap_pct(items, items) == 1.0


def test_items_overlap_pct_subset_asymmetric():
    """Asymmetric on purpose: 'do my CURRENT items appear in this prior?'.
    A is a subset of B → overlap is 1.0 from A's perspective.
    B has extras → overlap from B's perspective would be lower."""
    a = [{"description": "Foo Cushion", "qty": 10}]
    b = [
        {"description": "Foo Cushion", "qty": 10},
        {"description": "Bar Speaker", "qty": 5},
    ]
    assert _items_overlap_pct(a, b) == 1.0
    # From B's perspective only 1/2 match.
    assert _items_overlap_pct(b, a) == 0.5


def test_items_overlap_pct_empty_short_circuits():
    assert _items_overlap_pct([], [{"description": "X", "qty": 1}]) == 0.0
    assert _items_overlap_pct([{"description": "X", "qty": 1}], []) == 0.0


# ─── find_recurring_bids — the public API ────────────────────────────────

def _carolyn_pc():
    """Mike's existing manual PC: AMS 704 - RHU Art Supplies - 03.23.2026.
    CIW RHU / 3 fuzzy poster items / qty 10 each."""
    return {
        "institution": "CIW RHU",
        "items": [
            {"description": "Love Velvet Fuzzy Velvet Coloring Poster", "qty": 10},
            {"description": "Heart Hands Fuzzy Velvet Coloring Poster", "qty": 10},
            {"description": "Butterfly Eyes Fuzzy Coloring Poster", "qty": 10},
        ],
        "pc_number": "RHU Art Supplies",
        "created_at": "2026-03-23T08:01:00",
        "status": "sent",
        "requestor": "Carolyn Montgomery",
    }


def _new_recurrence_pc():
    """A new ingest from CIW RHU asking for the same items —
    representative of the cadence Mike said triggers this feature."""
    return {
        "institution": "CIW  RHU",  # extra space — must canonicalize
        "items": [
            {"description": "Love Velvet Fuzzy Coloring Poster", "qty": 10},
            {"description": "Heart Hands Fuzzy Velvet Poster", "qty": 10},
            {"description": "Butterfly Eyes Fuzzy Poster", "qty": 10},
        ],
        "pc_number": "10838974",
        "created_at": "2026-05-05T00:00:00",
    }


def test_find_recurring_bids_matches_carolyn_to_new():
    new = _new_recurrence_pc()
    all_records = {
        "pc_old": _carolyn_pc(),
        "pc_unrelated": {
            "institution": "CCHCS",
            "items": [{"description": "Echo Dot", "qty": 1}],
            "pc_number": "X",
            "created_at": "2026-04-01T00:00:00",
        },
        "pc_self": new,
    }
    matches = find_recurring_bids(new, all_records, record_id="pc_self")
    assert len(matches) == 1
    m = matches[0]
    assert m["id"] == "pc_old"
    assert m["pc_number"] == "RHU Art Supplies"
    assert m["matched_items"] == 3
    assert m["total_items"] == 3
    assert m["overlap_pct"] == 1.0
    assert m["status"] == "sent"
    assert m["requestor"] == "Carolyn Montgomery"
    assert m["url"] == "/pricecheck/pc_old"


def test_find_recurring_bids_excludes_self():
    """The current record must never appear in its own recurrence list."""
    me = _carolyn_pc()
    all_records = {"my_id": me}
    assert find_recurring_bids(me, all_records, record_id="my_id") == []


def test_find_recurring_bids_different_institution_no_match():
    """Same items + qty but different institution — buyer programs are
    institution-scoped. Don't surface CCHCS history on a CDCR PC."""
    new = _new_recurrence_pc()
    other = _carolyn_pc()
    other["institution"] = "Different Institution"
    matches = find_recurring_bids(new, {"prior": other}, record_id="self")
    assert matches == []


def test_find_recurring_bids_partial_overlap_below_threshold_no_match():
    """1/3 items match → 33% overlap → below 75% default → no surface."""
    new = _new_recurrence_pc()
    prior = {
        "institution": "CIW RHU",
        "items": [
            {"description": "Love Velvet Fuzzy Coloring Poster", "qty": 10},
            {"description": "Different Item One", "qty": 5},
            {"description": "Different Item Two", "qty": 3},
        ],
        "pc_number": "X", "created_at": "2026-03-01T00:00:00",
    }
    matches = find_recurring_bids(new, {"prior": prior}, record_id="self")
    assert matches == []


def test_find_recurring_bids_above_threshold_with_one_drop():
    """2/3 items match (66%) — below 75% threshold. 3/4 (75%) — at threshold."""
    prior = _carolyn_pc()
    # New PC adds an item: 3/4 match = 75% — should match.
    new = _new_recurrence_pc()
    new["items"].append({"description": "Brand New Item", "qty": 1})
    matches = find_recurring_bids(new, {"prior": prior}, record_id="self")
    assert len(matches) == 1, "3/4 = 75% must hit threshold exactly"


def test_find_recurring_bids_sorts_by_created_at_descending():
    """Most recent prior bid first — operator scans top-down."""
    new = _new_recurrence_pc()
    older = _carolyn_pc()
    older["created_at"] = "2026-01-01T00:00:00"
    newer = _carolyn_pc()
    newer["created_at"] = "2026-04-01T00:00:00"
    matches = find_recurring_bids(
        new, {"older": older, "newer": newer}, record_id="self",
    )
    assert [m["id"] for m in matches] == ["newer", "older"]


def test_find_recurring_bids_caps_at_max_results():
    """When the same institution + items recur many times, cap to keep
    the chip readable."""
    new = _new_recurrence_pc()
    all_records = {}
    for i in range(10):
        p = _carolyn_pc()
        p["created_at"] = f"2026-{i+1:02d}-01T00:00:00"
        all_records[f"pc_{i}"] = p
    matches = find_recurring_bids(new, all_records, record_id="self", max_results=3)
    assert len(matches) == 3


def test_find_recurring_bids_url_for_rfq_id():
    """RFQ ids start with 'rfq_' and route to /rfq/, not /pricecheck/."""
    new = _new_recurrence_pc()
    rfq_prior = _carolyn_pc()
    matches = find_recurring_bids(
        new, {"rfq_abc123": rfq_prior}, record_id="self",
    )
    assert len(matches) == 1
    assert matches[0]["url"] == "/rfq/rfq_abc123"


def test_find_recurring_bids_handles_none_record():
    """Garbage dict values must not crash detail render."""
    new = _new_recurrence_pc()
    bad = {
        "good": _carolyn_pc(),
        "bad_none": None,
        "bad_str": "not a dict",
        "bad_no_inst": {"items": new["items"]},
        "bad_no_items": {"institution": "CIW RHU"},
    }
    matches = find_recurring_bids(new, bad, record_id="self")
    assert len(matches) == 1
    assert matches[0]["id"] == "good"


def test_find_recurring_bids_no_institution_returns_empty():
    """A record with no institution can't match anyone — return [] cleanly."""
    new = {"institution": "", "items": _carolyn_pc()["items"]}
    matches = find_recurring_bids(new, {"prior": _carolyn_pc()}, record_id="self")
    assert matches == []


def test_find_recurring_bids_no_items_returns_empty():
    """A record with no items can't match anyone — return [] cleanly."""
    new = {"institution": "CIW RHU", "items": []}
    matches = find_recurring_bids(new, {"prior": _carolyn_pc()}, record_id="self")
    assert matches == []


def test_default_thresholds_are_documented_and_loadbearing():
    """Pin the threshold constants — a future PR that bumps them needs to
    update this test, which forces the author to think about whether
    they're tightening (more false negatives) or loosening (more false
    positives) per Mike's volume_over_margin doctrine."""
    assert DEFAULT_OVERLAP_THRESHOLD == 0.75
    assert DEFAULT_DESC_THRESHOLD == 0.65, (
        "The catalog matcher uses 0.65 (per CLAUDE.md). Bid recurrence "
        "uses the same threshold for consistency — never lower without "
        "cross-category accuracy testing."
    )
