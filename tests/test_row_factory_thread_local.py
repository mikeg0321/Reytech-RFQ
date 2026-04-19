"""Regression: code that touches the shared thread-local DB connection
must NOT mutate `conn.row_factory`. Mutation persists for every later
caller on the same thread.

Incident 2026-04-19: `_fetch_recent_summary` and `_fetch_trail` in
routes_quoting_status.py and `get_rules_for_agency` in agency_rules.py
each set `conn.row_factory = None` on the get_db()-returned connection.
Because get_db() reuses a per-thread connection, the next call to
`_load_price_checks()` on the same thread saw row_factory=None and
crashed inside `dict(row)` with "dictionary update sequence element #0
has length 13; 2 is required". The exception was swallowed → empty PC
dict → the 1-click banner smoke check failed (PC not found → 302).

PR #215 was a wrong-theory fix targeting cache invalidation timing.
The real fix is to never mutate `row_factory` on the shared connection.

This test reproduces the precise bug pattern: call a function that used
to set row_factory=None, then call _load_price_checks, and assert it
returns rows (not an empty dict from a swallowed exception).
"""
from __future__ import annotations

import sqlite3

import pytest

from src.api.data_layer import _load_price_checks, _save_price_checks
from src.core.db import get_db


@pytest.fixture(autouse=True)
def _seed_one_pc():
    """Seed a real PC so _load_price_checks has something to return."""
    pc_id = "test_row_factory_pin"
    pc = {
        "id": pc_id,
        "pc_number": "RF-001",
        "institution": "TEST",
        "items": [{"description": "row_factory regression", "qty": 1}],
        "status": "parsed",
        "created_at": "2026-04-19T18:30:00Z",
        "is_test": True,
    }
    _save_price_checks({pc_id: pc})
    yield pc_id


def _trip_the_bug():
    """Replicate the exact pattern that broke production: take a get_db()
    connection and mutate row_factory = None on it. This is the
    pre-fix behavior of _fetch_recent_summary / _fetch_trail /
    get_rules_for_agency."""
    with get_db() as conn:
        conn.row_factory = None
        # Pre-fix code did this — index-access reads on a Row-less conn.
        conn.execute("SELECT 1").fetchone()


def test_load_price_checks_survives_after_row_factory_mutation(_seed_one_pc):
    """The bug: a callsite mutates the shared row_factory, then a later
    _load_price_checks() on the same thread silently returns {}.

    Fix: `get_db()` resets `conn.row_factory = sqlite3.Row` on every yield,
    so callers can't poison each other. This test runs the precise
    poison-then-load sequence and asserts the load still works.

    Also forces a cache miss before the load — otherwise _load_price_checks
    short-circuits and never hits get_db()."""
    from src.api import data_layer as _dl
    pc_id = _seed_one_pc

    # Simulate the pre-fix bug: a caller mutates the shared row_factory.
    _trip_the_bug()

    # Cache may have been populated by the seed save — force a real DB read.
    _dl._pc_cache = None
    _dl._pc_cache_time = 0

    pcs = _load_price_checks()
    assert pc_id in pcs, (
        f"After row_factory mutation by a sibling caller, "
        f"_load_price_checks returned {len(pcs)} PCs (expected at least 1 with id={pc_id}). "
        f"get_db() must reset row_factory on every yield."
    )


def test_no_callsite_sets_row_factory_to_none_on_shared_conn():
    """Guard: grep the codebase to ensure no module sets `conn.row_factory = None`
    on a connection returned by `get_db()`. Independent connections via direct
    `sqlite3.connect()` are fine — only the shared get_db conn is the hazard."""
    import pathlib
    import re

    src = pathlib.Path(__file__).resolve().parents[1] / "src"
    pat = re.compile(r"\bconn\.row_factory\s*=\s*None\b")
    offenders = []
    for py in src.rglob("*.py"):
        text = py.read_text(encoding="utf-8", errors="replace")
        if not pat.search(text):
            continue
        # Allow only if file uses direct sqlite3.connect (not get_db).
        # Heuristic: if file imports get_db, the mutation is unsafe.
        if "from src.core.db import get_db" in text or "from src.core.db import" in text and "get_db" in text:
            for m in pat.finditer(text):
                line_no = text[:m.start()].count("\n") + 1
                offenders.append(f"{py.relative_to(src.parent)}:{line_no}")

    assert not offenders, (
        "These callsites mutate `conn.row_factory = None` on what may be "
        "the shared thread-local get_db() connection. That mutation persists "
        "for every later caller on the same thread and will eventually break "
        "_load_price_checks (or any dict(row) consumer) with "
        "'dictionary update sequence element #0 has length N; 2 is required'.\n"
        "  Offenders: " + ", ".join(offenders) + "\n"
        "Use index access (r[0]..r[N]) on the Row instead — it works without "
        "changing row_factory."
    )
