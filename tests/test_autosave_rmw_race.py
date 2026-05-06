"""Autosave read-modify-write race regression — Mike P0 2026-05-06.

Incident: RFQ a5b09b56. Operator was editing prices, autosaves logged
`200 / 8 items saved` repeatedly, but on refresh his work was overwritten.
Root cause: api_rfq_autosave loaded the RFQ snapshot OUTSIDE the save
lock, mutated it, and then called _save_single_rfq inside the lock. Two
close-together autosaves could both load the same stale snapshot, each
apply their own item update, then write back — and the later writer
serialized over the earlier writer with a "stale + own-edit" view,
silently losing the earlier writer's edit.

Substrate fix: wrap load → mutate → save in a single reentrant lock so
the second writer sees the first writer's persisted edit before mutating.

These tests directly exercise the locking shape via source-pinning
assertions — concurrent threading would be flaky as a regression guard.
"""
from __future__ import annotations

import os


def _read_src(path: str) -> str:
    full = os.path.join(os.path.dirname(__file__), "..", path)
    with open(full, encoding="utf-8") as f:
        return f.read()


def test_save_locks_are_reentrant():
    """Both _save_rfqs_lock and _save_pcs_lock must be RLock so the
    autosave handler can acquire them around load+mutate, and the inner
    `_save_single_rfq` / `_save_single_pc` calls can re-acquire without
    deadlocking."""
    src = _read_src("src/api/data_layer.py")
    assert "_save_rfqs_lock = threading.RLock()" in src, (
        "_save_rfqs_lock must be RLock — Lock would deadlock when "
        "_save_single_rfq re-acquires inside the autosave critical section"
    )
    assert "_save_pcs_lock = threading.RLock()" in src, (
        "_save_pcs_lock must be RLock for the same reason on the PC path"
    )


def test_rfq_autosave_load_and_save_under_same_lock():
    """The RFQ autosave handler must wrap `load_rfqs()` and the
    subsequent `_save_single_rfq` call in `_save_rfqs_lock`. Without
    this, a concurrent autosave can load before us and save after us,
    overwriting our edit."""
    src = _read_src("src/api/modules/routes_rfq_gen.py")
    # Find api_rfq_autosave's body
    start = src.index("def api_rfq_autosave(")
    body_end = src.find("\n@bp.route", start) if "\n@bp.route" in src[start:] else len(src)
    if body_end == len(src):
        next_def = src.find("\ndef ", start + 1)
        if next_def > 0:
            body_end = next_def
    body = src[start:body_end]

    assert "from src.api.data_layer import _save_rfqs_lock" in body, (
        "api_rfq_autosave must import _save_rfqs_lock"
    )
    assert "with _save_rfqs_lock:" in body, (
        "api_rfq_autosave must hold _save_rfqs_lock across load → mutate → save"
    )
    # Inside the with block, both load_rfqs and _save_single_rfq must appear
    # (otherwise the lock isn't actually covering the critical section).
    with_idx = body.index("with _save_rfqs_lock:")
    after_with = body[with_idx:]
    load_idx = after_with.find("load_rfqs()")
    save_idx = after_with.find("_save_single_rfq(rid, r")
    assert 0 < load_idx < save_idx, (
        f"load_rfqs (at {load_idx}) and _save_single_rfq (at {save_idx}) must "
        f"both appear after `with _save_rfqs_lock:` and in that order"
    )


def test_pc_save_prices_load_and_save_under_same_lock():
    """The PC save-prices handler must hold `_save_pcs_lock` across
    `_load_price_checks()` and the subsequent `_save_single_pc` call.
    Same race shape as RFQ — same fix."""
    src = _read_src("src/api/modules/routes_pricecheck.py")
    # The locking-wrapper version of _do_save_prices must exist
    assert "def _do_save_prices(pcid):" in src
    assert "from src.api.data_layer import _save_pcs_lock" in src
    assert "with _save_pcs_lock:" in src

    # The wrapper must call into a `_locked` body so the lock spans the
    # whole load → mutate → save sequence
    assert "def _do_save_prices_locked(pcid):" in src, (
        "PC save must split into wrapper + _locked body so the with-lock "
        "covers the entire load-mutate-save"
    )
    assert "_do_save_prices_locked(pcid)" in src, (
        "Wrapper must delegate to the locked inner function"
    )
