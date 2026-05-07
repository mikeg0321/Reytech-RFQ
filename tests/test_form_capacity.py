"""Pin the form capacity registry against the prod incidents that
prompted it (Mike P0 2026-05-06 RFQ a5b09b56) and against the documented
truth in CLAUDE.md / source code.

If a future change either lowers a capacity or adds a new form whose
overflow drops items, the test_required_forms_at_realistic_counts
boundary tests will surface the regression.
"""
from __future__ import annotations

import pytest

from src.forms.form_capacity import (
    FORM_CAPACITY,
    check_overflow,
    check_required_forms,
    get_capacity,
)


# ─── Registry shape ───────────────────────────────────────────────────


def test_registry_entry_shape():
    """Every registered form must have all required keys + types."""
    required_keys = {"rows_pg1", "rows_pg2", "has_overflow", "overflow_fn"}
    for form_id, entry in FORM_CAPACITY.items():
        assert required_keys.issubset(entry.keys()), (
            f"Form '{form_id}' missing keys: {required_keys - entry.keys()}"
        )
        assert isinstance(entry["rows_pg1"], int)
        assert isinstance(entry["rows_pg2"], int)
        assert isinstance(entry["has_overflow"], bool)
        # overflow_fn is None or a name string
        assert entry["overflow_fn"] is None or isinstance(entry["overflow_fn"], str)


def test_704_master_has_overflow_path():
    """AMS 704 master is the only form with a documented overflow path
    (`_append_overflow_pages`). CLAUDE.md says items 20+ render via
    reportlab canvas overlay."""
    cap = FORM_CAPACITY["704"]
    assert cap["has_overflow"] is True
    assert cap["overflow_fn"] == "_append_overflow_pages"
    assert cap["rows_pg1"] == 11
    assert cap["rows_pg2"] == 8


def test_calrecycle74_has_no_overflow():
    """CalRecycle 74 silently dropped items 7-8 on Mike's 8-item quote.
    Capacity must be exactly 6 with no overflow path until one is built."""
    cap = FORM_CAPACITY["calrecycle74"]
    assert cap["has_overflow"] is False
    assert cap["rows_pg1"] == 6
    assert cap["rows_pg2"] == 0


# ─── check_overflow — happy paths ─────────────────────────────────────


def test_check_overflow_within_capacity_passes():
    result = check_overflow("calrecycle74", 6)
    assert result["ok"] is True
    assert result["severity"] == "ok"
    assert result["items_dropped"] == 0


def test_check_overflow_at_boundary_passes():
    """Exactly at capacity is OK, not a blocker."""
    result = check_overflow("calrecycle74", 6)
    assert result["ok"] is True


def test_check_overflow_704_master_has_overflow_so_no_blocker_at_25():
    """AMS 704 master has overflow path; even 25 items must NOT
    blocker."""
    result = check_overflow("704", 25)
    assert result["ok"] is True
    assert result["has_overflow"] is True


def test_check_overflow_704_master_no_blocker_at_40():
    """Mike has had 37-item quotes — 40 must still pass on master."""
    result = check_overflow("704", 40)
    assert result["ok"] is True


# ─── check_overflow — blocker paths ───────────────────────────────────


def test_check_overflow_calrecycle_at_8_items_today_bug():
    """The exact today-bug case: 8 items, 6 rows, no overflow → blocker.
    Items 7-8 will be silently dropped without this check."""
    result = check_overflow("calrecycle74", 8)
    assert result["ok"] is False
    assert result["severity"] == "blocker"
    assert result["items_dropped"] == 2
    assert "7-8" in result["message"]
    assert "CALRECYCLE74" in result["message"]


def test_check_overflow_calrecycle_at_37_items_mikes_max():
    """Mike's worst-case (37 items) on CalRecycle drops 31."""
    result = check_overflow("calrecycle74", 37)
    assert result["ok"] is False
    assert result["items_dropped"] == 31


def test_check_overflow_704b_at_16_items():
    """704B buyer template variant has 15 rows page 1, no overflow.
    16 items overflows by 1."""
    result = check_overflow("704b", 16)
    assert result["ok"] is False
    assert result["items_dropped"] == 1
    assert result["has_overflow"] is False


# ─── check_overflow — special cases ───────────────────────────────────


def test_check_overflow_unregistered_form_passes_with_hint():
    """Unknown forms don't block (false-positive avoidance) but emit
    a hint message so the operator can register them."""
    result = check_overflow("unknown_form_id", 50)
    assert result["registered"] is False
    assert result["ok"] is True
    assert "not in capacity registry" in result["message"]


def test_check_overflow_zero_capacity_passes_unconditionally():
    """703B has rows_pg1=0 (header form, no line items). Item count is
    irrelevant; never blocker."""
    result = check_overflow("703b", 100)
    assert result["ok"] is True
    assert result["items_capacity"] == 0


def test_check_overflow_form_id_case_insensitive():
    result_lower = check_overflow("calrecycle74", 8)
    result_upper = check_overflow("CALRECYCLE74", 8)
    result_mixed = check_overflow("CalRecycle74", 8)
    assert result_lower["ok"] is False
    assert result_upper["ok"] is False
    assert result_mixed["ok"] is False


# ─── check_required_forms — aggregate ─────────────────────────────────


def test_check_required_forms_all_pass():
    """Required = ['704', '703b'] with 5 items → no blockers."""
    result = check_required_forms(["704", "703b"], 5)
    assert result["ok"] is True
    assert result["blockers"] == []


def test_check_required_forms_blocker_on_one():
    """Required = ['704', 'calrecycle74'] with 8 items: 704 ok (overflow
    path), CalRecycle blocker."""
    result = check_required_forms(["704", "calrecycle74"], 8)
    assert result["ok"] is False
    assert len(result["blockers"]) == 1
    assert result["blockers"][0]["form_id"] == "calrecycle74"


def test_check_required_forms_unknown_form_does_not_block():
    """Unknown forms in required list don't break the check."""
    result = check_required_forms(["704", "some_unknown_form"], 8)
    assert result["ok"] is True


# ─── Realistic counts boundary scan ───────────────────────────────────


@pytest.mark.parametrize("count", [1, 6, 7, 8, 11, 12, 15, 16, 19, 20, 25, 37, 40])
def test_required_forms_at_realistic_counts(count):
    """Stress shape: walk every realistic item count Mike has hit (or
    might) against the standard CCHCS required-forms set, asserting
    every form's overflow status is the documented behavior."""
    required = ["704", "703b", "calrecycle74", "obs_1600"]
    result = check_required_forms(required, count)
    if count <= 6:
        assert result["ok"] is True, (
            f"At {count} items, all required forms should pass — got "
            f"blockers: {[b['form_id'] for b in result['blockers']]}"
        )
    if count > 6:
        # CalRecycle 74 should always blocker past 6.
        assert any(b["form_id"] == "calrecycle74" for b in result["blockers"]), (
            f"At {count} items, calrecycle74 must be a blocker"
        )
    if count > 18:
        # OBS 1600 cap is 18.
        assert any(b["form_id"] == "obs_1600" for b in result["blockers"])
    # 704 master should NEVER blocker (overflow path exists).
    assert all(b["form_id"] != "704" for b in result["blockers"])


# ─── get_capacity convenience ─────────────────────────────────────────


def test_get_capacity_returns_entry():
    cap = get_capacity("calrecycle74")
    assert cap is not None
    assert cap["rows_pg1"] == 6


def test_get_capacity_unknown_returns_none():
    assert get_capacity("nope") is None
