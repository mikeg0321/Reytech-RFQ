"""The Spine — quote ↔ quote links substrate.

Covers write_quote_link's invariants (no self-links, bounded confidence,
required actor/method), find_links_from/to read paths (sorted by
confidence DESC), evidence JSON round-trip, and idempotency on
duplicate writes.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.spine.db import (
    AUTO_LINK_OPERATOR_CONFIDENCE,
    find_links_from,
    find_links_to,
    init_db,
    write_quote_link,
)
from src.spine.model import SpineValidationError


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "spine_links.db"
    init_db(p)
    return p


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_write_link_returns_metadata(db_path: Path):
    out = write_quote_link(
        db_path,
        from_quote_id="rfq_new_001",
        to_quote_id="pc_prior_001",
        match_method="auto_mfg_desc",
        confidence=0.85,
        evidence={"mfg_overlap_ratio": 0.9, "desc_jaccard_mean": 0.75},
        actor="spine_auto_linker",
    )
    assert out["link_id"].startswith("link_")
    assert out["linked_at"]
    assert out["duplicate"] is False


def test_find_links_from_returns_link(db_path: Path):
    write_quote_link(
        db_path,
        from_quote_id="rfq_new_001",
        to_quote_id="pc_prior_001",
        match_method="auto_mfg_desc",
        confidence=0.85,
        evidence={"mfg_overlap_ratio": 0.9},
        actor="spine_auto_linker",
    )
    links = find_links_from(db_path, "rfq_new_001")
    assert len(links) == 1
    assert links[0]["from_quote_id"] == "rfq_new_001"
    assert links[0]["to_quote_id"] == "pc_prior_001"
    assert links[0]["confidence"] == 0.85
    assert links[0]["match_method"] == "auto_mfg_desc"
    assert links[0]["actor"] == "spine_auto_linker"
    assert links[0]["evidence"]["mfg_overlap_ratio"] == 0.9


def test_find_links_to_returns_link(db_path: Path):
    write_quote_link(
        db_path,
        from_quote_id="rfq_new_001",
        to_quote_id="pc_prior_001",
        match_method="auto_mfg_desc",
        confidence=0.85,
        evidence={},
        actor="spine_auto_linker",
    )
    links = find_links_to(db_path, "pc_prior_001")
    assert len(links) == 1
    assert links[0]["from_quote_id"] == "rfq_new_001"


def test_find_links_returns_empty_when_none(db_path: Path):
    assert find_links_from(db_path, "rfq_unknown") == []
    assert find_links_to(db_path, "pc_unknown") == []


def test_evidence_round_trips_as_dict(db_path: Path):
    evidence = {
        "mfg_overlap_ratio": 0.75,
        "desc_jaccard_mean": 0.62,
        "same_facility": True,
        "matched_line_pairs": [[1, 1], [2, 3], [4, 4]],
    }
    write_quote_link(
        db_path,
        from_quote_id="rfq_new_001",
        to_quote_id="pc_prior_001",
        match_method="auto_mfg_desc",
        confidence=0.7,
        evidence=evidence,
        actor="spine_auto_linker",
    )
    links = find_links_from(db_path, "rfq_new_001")
    assert links[0]["evidence"] == evidence


def test_evidence_defaults_to_empty_dict(db_path: Path):
    write_quote_link(
        db_path,
        from_quote_id="rfq_new_001",
        to_quote_id="pc_prior_001",
        match_method="auto_mfg_desc",
        confidence=0.5,
        actor="spine_auto_linker",
    )
    links = find_links_from(db_path, "rfq_new_001")
    assert links[0]["evidence"] == {}


# ──────────────────────────────────────────────────────────────────────
# Multiple links + sort order
# ──────────────────────────────────────────────────────────────────────


def test_multiple_links_per_from_sorted_by_confidence_desc(db_path: Path):
    """When a new RFQ has both an auto-match and an operator-asserted
    manual match, the operator's link surfaces first."""
    write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_likely_match",
        match_method="auto_mfg_desc",
        confidence=0.72,
        actor="spine_auto_linker",
    )
    write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_actual",
        match_method="operator_manual",
        confidence=AUTO_LINK_OPERATOR_CONFIDENCE,
        actor="operator:mike",
    )
    write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_alt",
        match_method="auto_mfg_desc",
        confidence=0.55,
        actor="spine_auto_linker",
    )
    links = find_links_from(db_path, "rfq_001")
    assert len(links) == 3
    assert [l["to_quote_id"] for l in links] == ["pc_actual", "pc_likely_match", "pc_alt"]
    assert links[0]["confidence"] == 1.0


def test_inverse_lookup_returns_all_from_quotes(db_path: Path):
    """Multiple RFQs over time may all link to the same prior PC."""
    write_quote_link(
        db_path,
        from_quote_id="rfq_rebid_1",
        to_quote_id="pc_master",
        match_method="auto_mfg_desc",
        confidence=0.8,
        actor="spine_auto_linker",
    )
    write_quote_link(
        db_path,
        from_quote_id="rfq_rebid_2",
        to_quote_id="pc_master",
        match_method="auto_mfg_desc",
        confidence=0.9,
        actor="spine_auto_linker",
    )
    links = find_links_to(db_path, "pc_master")
    assert len(links) == 2
    # Highest-confidence first.
    assert links[0]["from_quote_id"] == "rfq_rebid_2"


# ──────────────────────────────────────────────────────────────────────
# Idempotency
# ──────────────────────────────────────────────────────────────────────


def test_duplicate_link_no_ops(db_path: Path):
    """Same (from, to, method) re-written → no second row, returns
    duplicate=True."""
    first = write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_001",
        match_method="auto_mfg_desc",
        confidence=0.5,
        actor="spine_auto_linker",
    )
    second = write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_001",
        match_method="auto_mfg_desc",
        confidence=0.9,  # different confidence, still de-duped by id
        actor="spine_auto_linker",
    )
    assert second["duplicate"] is True
    assert second["link_id"] == first["link_id"]
    links = find_links_from(db_path, "rfq_001")
    assert len(links) == 1


def test_different_method_creates_distinct_link(db_path: Path):
    """An operator manual link AND an auto link to the same target are
    BOTH valid records (different methods = different link_ids)."""
    write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_001",
        match_method="auto_mfg_desc",
        confidence=0.65,
        actor="spine_auto_linker",
    )
    write_quote_link(
        db_path,
        from_quote_id="rfq_001",
        to_quote_id="pc_001",
        match_method="operator_manual",
        confidence=1.0,
        actor="operator:mike",
    )
    links = find_links_from(db_path, "rfq_001")
    assert len(links) == 2
    methods = sorted(l["match_method"] for l in links)
    assert methods == ["auto_mfg_desc", "operator_manual"]


# ──────────────────────────────────────────────────────────────────────
# Invariants
# ──────────────────────────────────────────────────────────────────────


def test_self_link_refused(db_path: Path):
    with pytest.raises(SpineValidationError, match="self-link"):
        write_quote_link(
            db_path,
            from_quote_id="rfq_same",
            to_quote_id="rfq_same",
            match_method="auto_mfg_desc",
            confidence=0.5,
            actor="spine_auto_linker",
        )


def test_self_link_refused_with_whitespace(db_path: Path):
    """Trimming applies — "  X  " == "X" → still a self-link."""
    with pytest.raises(SpineValidationError, match="self-link"):
        write_quote_link(
            db_path,
            from_quote_id="rfq_same",
            to_quote_id="  rfq_same  ",
            match_method="auto_mfg_desc",
            confidence=0.5,
            actor="spine_auto_linker",
        )


@pytest.mark.parametrize("bad", [-0.01, 1.01, -1, 2, 999])
def test_out_of_range_confidence_rejected(db_path: Path, bad):
    with pytest.raises(SpineValidationError, match="confidence"):
        write_quote_link(
            db_path,
            from_quote_id="rfq_001",
            to_quote_id="pc_001",
            match_method="auto_mfg_desc",
            confidence=bad,
            actor="spine_auto_linker",
        )


def test_non_numeric_confidence_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        write_quote_link(
            db_path,
            from_quote_id="rfq_001",
            to_quote_id="pc_001",
            match_method="auto_mfg_desc",
            confidence="0.5",  # type: ignore[arg-type]
            actor="spine_auto_linker",
        )


@pytest.mark.parametrize("field", ["from_quote_id", "to_quote_id", "match_method", "actor"])
def test_empty_required_string_rejected(db_path: Path, field):
    kwargs = dict(
        from_quote_id="rfq_001",
        to_quote_id="pc_001",
        match_method="auto_mfg_desc",
        confidence=0.5,
        actor="spine_auto_linker",
    )
    kwargs[field] = ""
    with pytest.raises(SpineValidationError):
        write_quote_link(db_path, **kwargs)
    kwargs[field] = "   "
    with pytest.raises(SpineValidationError):
        write_quote_link(db_path, **kwargs)


# ──────────────────────────────────────────────────────────────────────
# Bounded-confidence sentinel
# ──────────────────────────────────────────────────────────────────────


def test_operator_confidence_constant_is_max(db_path: Path):
    """Operator-asserted links use the ceiling value so they always
    sort above any auto-link."""
    assert AUTO_LINK_OPERATOR_CONFIDENCE == 1.0


# ──────────────────────────────────────────────────────────────────────
# Reader input validation
# ──────────────────────────────────────────────────────────────────────


def test_find_links_from_empty_id_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        find_links_from(db_path, "")


def test_find_links_to_empty_id_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        find_links_to(db_path, "")
