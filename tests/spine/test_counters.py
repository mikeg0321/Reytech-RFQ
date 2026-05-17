"""The Spine — sequential counters substrate.

Covers next_value atomicity (sequential IDs under concurrent callers),
the max-jump guard on set_counter, the year-rollover pattern, manual
override semantics, and the read-side preview rule.
"""
from __future__ import annotations

import threading
from pathlib import Path

import pytest

from src.spine.db import (
    COUNTER_MAX_JUMP,
    get_counter,
    init_db,
    next_value,
    set_counter,
)
from src.spine.model import SpineValidationError


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "spine_counters.db"
    init_db(p)
    return p


# ──────────────────────────────────────────────────────────────────────
# next_value — atomic increment
# ──────────────────────────────────────────────────────────────────────


def test_first_call_returns_1(db_path: Path):
    assert next_value(db_path, "pc_2026", actor="spine_ingest") == 1


def test_subsequent_calls_increment_sequentially(db_path: Path):
    seen = [next_value(db_path, "pc_2026", actor="spine_ingest") for _ in range(5)]
    assert seen == [1, 2, 3, 4, 5]


def test_different_counters_are_independent(db_path: Path):
    assert next_value(db_path, "pc_2026", actor="spine_ingest") == 1
    assert next_value(db_path, "rfq_2026", actor="spine_ingest") == 1
    assert next_value(db_path, "pc_2026", actor="spine_ingest") == 2
    assert next_value(db_path, "quote_2026", actor="spine_ingest") == 1
    assert next_value(db_path, "rfq_2026", actor="spine_ingest") == 2


def test_counter_name_is_trimmed(db_path: Path):
    next_value(db_path, "pc_2026", actor="x")
    assert next_value(db_path, "  pc_2026  ", actor="x") == 2


def test_empty_counter_name_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        next_value(db_path, "", actor="x")
    with pytest.raises(SpineValidationError):
        next_value(db_path, "   ", actor="x")


def test_empty_actor_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        next_value(db_path, "pc_2026", actor="")
    with pytest.raises(SpineValidationError):
        next_value(db_path, "pc_2026", actor="   ")


# ──────────────────────────────────────────────────────────────────────
# Concurrency — no duplicates, no gaps under contention
# ──────────────────────────────────────────────────────────────────────


def test_concurrent_calls_produce_unique_sequential_values(db_path: Path):
    """Under 20 threads racing for next_value, the union of returned
    integers MUST be {1, 2, ..., 20} — no duplicates, no gaps."""
    results: list[int] = []
    lock = threading.Lock()

    def worker():
        v = next_value(db_path, "pc_2026", actor="thread")
        with lock:
            results.append(v)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == list(range(1, 21))


# ──────────────────────────────────────────────────────────────────────
# get_counter — pure read
# ──────────────────────────────────────────────────────────────────────


def test_get_counter_returns_none_when_never_set(db_path: Path):
    assert get_counter(db_path, "pc_2026") is None


def test_get_counter_reflects_last_next_value(db_path: Path):
    next_value(db_path, "pc_2026", actor="x")
    next_value(db_path, "pc_2026", actor="x")
    next_value(db_path, "pc_2026", actor="x")
    assert get_counter(db_path, "pc_2026") == 3


def test_get_counter_does_not_mutate(db_path: Path):
    next_value(db_path, "pc_2026", actor="x")
    assert get_counter(db_path, "pc_2026") == 1
    assert get_counter(db_path, "pc_2026") == 1
    assert next_value(db_path, "pc_2026", actor="x") == 2


def test_get_counter_empty_name_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        get_counter(db_path, "")


# ──────────────────────────────────────────────────────────────────────
# set_counter — manual override with max-jump guard
# ──────────────────────────────────────────────────────────────────────


def test_set_counter_initializes_when_absent(db_path: Path):
    set_counter(db_path, "pc_2026", 0, actor="operator")
    assert get_counter(db_path, "pc_2026") == 0
    assert next_value(db_path, "pc_2026", actor="x") == 1


def test_set_counter_allows_jump_at_max(db_path: Path):
    """A jump of exactly COUNTER_MAX_JUMP is allowed (5)."""
    set_counter(db_path, "pc_2026", COUNTER_MAX_JUMP, actor="operator")
    assert get_counter(db_path, "pc_2026") == COUNTER_MAX_JUMP


def test_set_counter_refuses_jump_above_max(db_path: Path):
    with pytest.raises(SpineValidationError, match="max_jump"):
        set_counter(db_path, "pc_2026", COUNTER_MAX_JUMP + 1, actor="operator")
    assert get_counter(db_path, "pc_2026") is None


def test_set_counter_allows_decrement(db_path: Path):
    """Back-corrections are allowed — sometimes an increment was wrong."""
    for _ in range(5):
        next_value(db_path, "pc_2026", actor="x")
    set_counter(db_path, "pc_2026", 3, actor="operator")
    assert get_counter(db_path, "pc_2026") == 3
    assert next_value(db_path, "pc_2026", actor="x") == 4


def test_set_counter_negative_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        set_counter(db_path, "pc_2026", -1, actor="operator")


def test_set_counter_non_int_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        set_counter(db_path, "pc_2026", 1.5, actor="operator")  # type: ignore[arg-type]


def test_set_counter_empty_actor_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        set_counter(db_path, "pc_2026", 1, actor="")


def test_set_counter_empty_name_rejected(db_path: Path):
    with pytest.raises(SpineValidationError):
        set_counter(db_path, "", 1, actor="operator")


# ──────────────────────────────────────────────────────────────────────
# Year rollover — different counter_name per year
# ──────────────────────────────────────────────────────────────────────


def test_year_rollover_via_separate_counter_names(db_path: Path):
    """The substrate doesn't reset a counter; the caller selects a
    fresh counter_name when the year changes."""
    for _ in range(347):
        next_value(db_path, "pc_2026", actor="ingest")
    assert get_counter(db_path, "pc_2026") == 347
    assert next_value(db_path, "pc_2027", actor="ingest") == 1
    assert get_counter(db_path, "pc_2026") == 347  # unaffected


# ──────────────────────────────────────────────────────────────────────
# Persistence across processes — same DB, fresh handle
# ──────────────────────────────────────────────────────────────────────


def test_counter_persists_across_connections(db_path: Path):
    next_value(db_path, "pc_2026", actor="x")
    next_value(db_path, "pc_2026", actor="x")
    next_value(db_path, "pc_2026", actor="x")
    # Same path, fresh _connect — value must survive.
    assert get_counter(db_path, "pc_2026") == 3
    assert next_value(db_path, "pc_2026", actor="x") == 4
