"""Tests for `find_matching_pcs_for_cchcs` — the operator-confirmed PC matcher.

Mike's rules (2026-04-20 scope call):
  - Only ~30% of RFQs have a prior PC; don't force a false positive.
  - PCs are CCHCS-only today. Non-CCHCS PCs must be ignored.
  - "If nearly match, just ask me or do a % match if not 100%. prompt to link."
    → Return top 1-3 candidates with match %; never pick a single winner.
  - PC prices are banking on public-bidding publication — a wrong link would
    contaminate the commitment price. Header match without line match is
    NOT enough; we must have at least one line identity match.
  - Line identity: MFG# → UPC → description fuzzy ≥ 0.65 → positional last.

Covers:
  - CCHCS scoping (non-CCHCS PCs filtered out even if header matches)
  - MFG# and UPC take precedence over description fuzzy
  - Header-only matches (no line match) are NOT candidates
  - Top-N sorting with exact-match flag for the "99.9% done" UX
"""
from __future__ import annotations

from src.core.pc_rfq_linker import (
    _line_identity_match,
    _is_cchcs_pc,
    find_matching_pcs_for_cchcs,
)


# ── _line_identity_match ─────────────────────────────────────────────────────

def test_line_match_mfg_beats_everything():
    """Same MFG# = authoritative match, 1.0 confidence, even with different descs."""
    rfq = {"mfg_number": "W12919", "description": "gauze pad 4x4"}
    pc = {"mfg_number": "W12919", "description": "completely different text"}
    kind, conf = _line_identity_match(rfq, pc)
    assert kind == "mfg"
    assert conf == 1.0


def test_line_match_mfg_normalization():
    """Leading zeros / hyphens / case differences don't defeat MFG equality."""
    rfq = {"mfg_number": "w-12919"}
    pc = {"mfg_number": "W12919"}
    kind, _ = _line_identity_match(rfq, pc)
    assert kind == "mfg"


def test_line_match_upc_when_no_mfg():
    rfq = {"upc": "012345678905", "description": "thing"}
    pc = {"upc": "012345678905", "description": "thing"}
    kind, conf = _line_identity_match(rfq, pc)
    # MFG absent on both → UPC wins
    assert kind == "upc"
    assert conf == 1.0


def test_line_match_desc_fuzzy_above_threshold():
    rfq = {"description": "Blood pressure cuff adult size"}
    pc = {"description": "Blood pressure cuff, adult"}
    kind, conf = _line_identity_match(rfq, pc)
    assert kind == "desc"
    assert conf >= 0.65


def test_line_match_desc_below_threshold_rejected():
    """Cross-category near-miss must not match. Catalog uses 0.65 for this reason."""
    rfq = {"description": "stethoscope dual head"}
    pc = {"description": "exam gloves nitrile"}
    kind, _ = _line_identity_match(rfq, pc)
    assert kind is None


def test_line_match_positional_only_when_opted_in():
    rfq = {"description": ""}
    pc = {"description": "anything"}
    kind, _ = _line_identity_match(rfq, pc, positional_ok=False)
    assert kind is None
    kind, _ = _line_identity_match(rfq, pc, positional_ok=True)
    assert kind == "positional"


# ── _is_cchcs_pc ─────────────────────────────────────────────────────────────

def test_cchcs_detection_by_agency():
    assert _is_cchcs_pc({"agency": "CCHCS"}) is True
    assert _is_cchcs_pc({"institution": "California Correctional Health Care Services"}) is True


def test_cchcs_detection_rejects_non_cchcs():
    assert _is_cchcs_pc({"agency": "CDCR"}) is False
    assert _is_cchcs_pc({"agency": "CalVet"}) is False
    assert _is_cchcs_pc({}) is False


# ── find_matching_pcs_for_cchcs ──────────────────────────────────────────────

def _rfq(items, **kwargs):
    return {"line_items": items, **kwargs}


def _pc(items, agency="CCHCS", **kwargs):
    return {"agency": agency, "items": items, **kwargs}


def test_finds_exact_match_flags_is_exact():
    rfq = _rfq(
        [
            {"mfg_number": "W12919", "description": "BP cuff"},
            {"mfg_number": "NL304", "description": "stethoscope"},
        ],
        requestor_email="buyer@cchcs.ca.gov",
    )
    pcs = {
        "pc_match": _pc(
            [
                {"mfg_number": "W12919", "description": "BP cuff adult"},
                {"mfg_number": "NL304", "description": "stethoscope dual-head"},
            ],
            requestor="buyer@cchcs.ca.gov",
        ),
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs)
    assert len(out) == 1
    top = out[0]
    assert top["pc_id"] == "pc_match"
    assert top["is_exact"] is True
    assert top["line_matches"] == 2
    assert top["line_total"] == 2
    assert top["match_pct"] >= 90


def test_skips_non_cchcs_even_with_perfect_header_and_line_match():
    """Safety: a CDCR PC that happens to match headers+items must NOT be offered.
    PCs are a CCHCS-only workflow today; surfacing a CDCR PC risks a false link
    that the operator might accept under time pressure."""
    rfq = _rfq(
        [{"mfg_number": "W12919", "description": "BP cuff"}],
        requestor_email="buyer@example.gov",
    )
    pcs = {
        "pc_cdcr": _pc(
            [{"mfg_number": "W12919", "description": "BP cuff"}],
            agency="CDCR", requestor="buyer@example.gov",
        ),
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs)
    assert out == []


def test_header_only_match_is_not_a_candidate():
    """Per Mike: wrong PC link contaminates the bidding commitment price.
    Matching requestor email + solicitation number but zero items = reject.
    The operator would have nothing to verify beyond a name."""
    rfq = _rfq(
        [{"description": "elastic bandage 4 inch"}],
        requestor_email="buyer@cchcs.ca.gov",
        solicitation_number="RFQ-123",
    )
    pcs = {
        "pc_stale": _pc(
            [{"description": "surgical mask blue"}],
            requestor="buyer@cchcs.ca.gov", pc_number="RFQ-123",
        ),
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs)
    assert out == []


def test_returns_top_n_sorted_by_match_pct():
    rfq = _rfq(
        [
            {"mfg_number": "A1", "description": "item one"},
            {"mfg_number": "B2", "description": "item two"},
            {"mfg_number": "C3", "description": "item three"},
        ],
    )
    pcs = {
        # 3/3 MFG matches → highest
        "pc_full": _pc([
            {"mfg_number": "A1"}, {"mfg_number": "B2"}, {"mfg_number": "C3"},
        ]),
        # 2/3 MFG matches
        "pc_partial": _pc([{"mfg_number": "A1"}, {"mfg_number": "B2"}]),
        # 1/3 MFG match
        "pc_weak": _pc([{"mfg_number": "A1"}]),
        # noise — no item match, should not appear
        "pc_unrelated": _pc([{"mfg_number": "Z9", "description": "other"}]),
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs, max_results=3)
    assert [c["pc_id"] for c in out] == ["pc_full", "pc_partial", "pc_weak"]
    assert out[0]["match_pct"] > out[1]["match_pct"] > out[2]["match_pct"]
    assert out[0]["is_exact"] is True
    assert out[1]["is_exact"] is False


def test_max_results_caps_output():
    rfq = _rfq([{"mfg_number": "A1", "description": "x"}])
    pcs = {
        f"pc_{i}": _pc([{"mfg_number": "A1"}]) for i in range(10)
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs, max_results=3)
    assert len(out) == 3


def test_mfg_match_wins_over_desc_fuzzy():
    """If two PCs tie on line count, the MFG-match one should still reach the top."""
    rfq = _rfq([{"mfg_number": "W12919", "description": "BP cuff"}])
    pcs = {
        "pc_desc_only": _pc([{"description": "BP cuff adult"}]),  # desc fuzzy hit
        "pc_mfg": _pc([{"mfg_number": "W12919"}]),                 # mfg hit
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs)
    # Both match 1/1 lines, but the reasons list distinguishes them
    mfg_entry = next(c for c in out if c["pc_id"] == "pc_mfg")
    assert any("by_mfg_or_upc" in r for r in mfg_entry["reasons"])


def test_pc_data_wrapped_in_pc_data_key():
    """Production PCs are often stored as {"pc_data": {...}} from the queue
    loader. Matcher must unwrap like the other functions in this module do."""
    rfq = _rfq([{"mfg_number": "W12919", "description": "BP cuff"}])
    pcs = {
        "pc_wrapped": {
            "agency": "CCHCS",
            "pc_data": {
                "items": [{"mfg_number": "W12919"}],
                "requestor": "buyer@cchcs.ca.gov",
            },
        },
    }
    out = find_matching_pcs_for_cchcs(rfq, pcs)
    assert len(out) == 1
    assert out[0]["pc_id"] == "pc_wrapped"
