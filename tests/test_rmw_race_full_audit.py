"""Read-modify-write race audit — all user-facing route handlers
that load → mutate → save must wrap that sequence in the appropriate
save lock. Without this, two close-together POSTs to the same endpoint
can both load the same stale snapshot, each apply their own mutation,
then write — the later writer overwrites the earlier writer's edit.

Substrate fix shipped in PR #778 (autosave). This test pins the same
shape across 8 additional handlers identified by the 2026-05-06 audit:
  - api_rfq_mark_won / api_rfq_mark_lost (RFQ outcomes)
  - api_pricecheck_mark_won / api_pricecheck_mark_lost (PC outcomes)
  - api_pricecheck_mark_sent (PC final close)
  - api_pc_mark_no_response / api_pc_mark_auto_priced (PC flag flips)
  - convert_pc_to_rfq + api_bundle_convert_each + api_bundle_convert_single
    (PC → RFQ conversions, both stores)
"""
from __future__ import annotations

import os
import re


def _read_src(path: str) -> str:
    full = os.path.join(os.path.dirname(__file__), "..", path)
    with open(full, encoding="utf-8") as f:
        return f.read()


def _function_body(src: str, fn_name: str) -> str:
    """Return the source of `def fn_name(...)` up to the next top-level def
    or @bp.route decorator. Defensive against trailing imports."""
    m = re.search(rf"^def {re.escape(fn_name)}\(", src, re.MULTILINE)
    assert m, f"Function {fn_name} not found"
    start = m.start()
    # Find next top-level def or decorator at column 0
    rest = src[start + 1:]
    next_match = re.search(r"\n(?:@bp\.route|def [a-zA-Z_])", rest)
    end = start + 1 + (next_match.start() if next_match else len(rest))
    return src[start:end]


def _assert_rfq_lock_wraps_load_and_save(body: str, fn_name: str) -> None:
    """The function's body must:
      1. Import _save_rfqs_lock from src.api.data_layer
      2. Open `with _save_rfqs_lock:` BEFORE the load
      3. Have the load (`load_rfqs()`) AFTER `with _save_rfqs_lock:`
      4. Have the save (`_save_single_rfq(...)`) AFTER the load
    Catches the exact race shape we keep regressing.
    """
    assert "from src.api.data_layer import" in body, (
        f"{fn_name} must import the save lock from src.api.data_layer"
    )
    assert "_save_rfqs_lock" in body, f"{fn_name} must reference _save_rfqs_lock"
    with_idx = body.find("_save_rfqs_lock")
    load_idx = body.find("load_rfqs()", with_idx)
    save_idx = body.find("_save_single_rfq(", load_idx if load_idx > 0 else with_idx)
    assert 0 < with_idx < load_idx < save_idx, (
        f"{fn_name}: lock(@{with_idx}) → load(@{load_idx}) → save(@{save_idx}) "
        f"must appear in that order. The load and save must both be inside "
        f"the `with _save_rfqs_lock:` block."
    )


def _assert_pcs_lock_wraps_load_and_save(body: str, fn_name: str,
                                          load_fn: str = "_load_price_checks") -> None:
    assert "from src.api.data_layer import" in body, (
        f"{fn_name} must import the save lock from src.api.data_layer"
    )
    assert "_save_pcs_lock" in body, f"{fn_name} must reference _save_pcs_lock"
    with_idx = body.find("_save_pcs_lock")
    load_idx = body.find(f"{load_fn}(", with_idx)
    save_idx = body.find("_save_single_pc(", load_idx if load_idx > 0 else with_idx)
    assert 0 < with_idx < load_idx < save_idx, (
        f"{fn_name}: lock(@{with_idx}) → load(@{load_idx}) → save(@{save_idx}) "
        f"must appear in that order. The load and save must both be inside "
        f"the `with _save_pcs_lock:` block."
    )


# ─────────────────── RFQ outcome handlers ──────────────────────────


def test_rfq_mark_won_under_lock():
    src = _read_src("src/api/modules/routes_rfq.py")
    _assert_rfq_lock_wraps_load_and_save(_function_body(src, "api_rfq_mark_won"),
                                          "api_rfq_mark_won")


def test_rfq_mark_lost_under_lock():
    src = _read_src("src/api/modules/routes_rfq.py")
    _assert_rfq_lock_wraps_load_and_save(_function_body(src, "api_rfq_mark_lost"),
                                          "api_rfq_mark_lost")


# ─────────────────── PC outcome handlers ───────────────────────────


def test_pc_mark_won_under_lock():
    src = _read_src("src/api/modules/routes_pricecheck_admin.py")
    _assert_pcs_lock_wraps_load_and_save(
        _function_body(src, "api_pricecheck_mark_won"),
        "api_pricecheck_mark_won")


def test_pc_mark_lost_under_lock():
    src = _read_src("src/api/modules/routes_pricecheck_admin.py")
    _assert_pcs_lock_wraps_load_and_save(
        _function_body(src, "api_pricecheck_mark_lost"),
        "api_pricecheck_mark_lost")


def test_pc_mark_sent_under_lock():
    """mark-sent racing with a final autosave lost the autosave's last
    pricing edit (incident: 2026-05-06 RFQ a5b09b56 sibling pattern)."""
    src = _read_src("src/api/modules/routes_pricecheck_pricing.py")
    _assert_pcs_lock_wraps_load_and_save(
        _function_body(src, "api_pricecheck_mark_sent"),
        "api_pricecheck_mark_sent")


def test_pc_mark_no_response_under_lock():
    src = _read_src("src/api/modules/routes_pricecheck_pricing.py")
    _assert_pcs_lock_wraps_load_and_save(
        _function_body(src, "api_pc_mark_no_response"),
        "api_pc_mark_no_response")


def test_pc_mark_auto_priced_under_lock():
    src = _read_src("src/api/modules/routes_pricecheck_pricing.py")
    _assert_pcs_lock_wraps_load_and_save(
        _function_body(src, "api_pc_mark_auto_priced"),
        "api_pc_mark_auto_priced")


# ─────────────────── PC → RFQ conversion (two-store) ───────────────


def test_convert_pc_to_rfq_holds_both_locks():
    """The conversion handler mutates BOTH the PC and the RFQ stores.
    It must hold both locks across the whole load → convert → save."""
    src = _read_src("src/api/modules/routes_analytics.py")
    body = _function_body(src, "convert_pc_to_rfq")
    assert "_save_pcs_lock" in body, "convert_pc_to_rfq must hold _save_pcs_lock"
    assert "_save_rfqs_lock" in body, "convert_pc_to_rfq must hold _save_rfqs_lock"
    # Both saves must appear after both locks are open
    pc_lock_idx = body.find("_save_pcs_lock")
    rfq_lock_idx = body.find("_save_rfqs_lock")
    save_rfq = body.find("_save_single_rfq(", max(pc_lock_idx, rfq_lock_idx))
    save_pc = body.find("_save_single_pc(", max(pc_lock_idx, rfq_lock_idx))
    assert save_rfq > 0 and save_pc > 0, (
        "Both _save_single_rfq and _save_single_pc must appear AFTER both lock "
        "acquisitions in convert_pc_to_rfq"
    )


def test_bundle_convert_each_holds_both_locks():
    src = _read_src("src/api/modules/routes_pricecheck_gen.py")
    body = _function_body(src, "api_bundle_convert_each")
    assert "_save_pcs_lock" in body and "_save_rfqs_lock" in body, (
        "api_bundle_convert_each must hold both PC and RFQ save locks"
    )


def test_bundle_convert_single_holds_both_locks():
    src = _read_src("src/api/modules/routes_pricecheck_gen.py")
    body = _function_body(src, "api_bundle_convert_single")
    assert "_save_pcs_lock" in body and "_save_rfqs_lock" in body, (
        "api_bundle_convert_single must hold both PC and RFQ save locks"
    )
