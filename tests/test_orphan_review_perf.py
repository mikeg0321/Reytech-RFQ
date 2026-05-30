"""Regression tests for the orphan-review N*M performance fix.

Bug (2026-05-29 sweep): `/api/orders/orphan-review` hung on "Loading…"
because `find_quote_candidates` ran a full-table quotes SELECT *per
orphan*, and `_agency_canonical` re-resolved the same agency strings
through `match_agency` N*M times — un-memoized, no cache, no hoist.

The fix:
  - `fetch_scorable_quotes(conn)` hoists the quotes read so the route
    runs it ONCE and passes the snapshot via `quote_rows=`.
  - `find_quote_candidates(..., quote_rows=...)` uses the passed rows
    and only self-fetches when called standalone (quote_rows=None).
  - `_agency_canonical` is `functools.lru_cache`-memoized.

These tests pin all three so the N*M re-scan cannot silently return.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.core.orders_link_orphans import (
    _agency_canonical,
    fetch_scorable_quotes,
    find_quote_candidates,
)


@pytest.fixture
def conn():
    """In-memory SQLite with the minimal quotes schema the finder reads."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE quotes (
            quote_number TEXT, po_number TEXT, agency TEXT, total REAL,
            sent_at TEXT, created_at TEXT, is_test INTEGER DEFAULT 0
        );
        """
    )
    yield c
    c.close()


def _ins_quote(conn, **kw):
    cols = ", ".join(kw)
    ph = ", ".join("?" for _ in kw)
    conn.execute(f"INSERT INTO quotes ({cols}) VALUES ({ph})", tuple(kw.values()))
    conn.commit()


class _CountingConn:
    """Wraps a sqlite connection and counts SELECTs against `quotes`."""

    def __init__(self, real):
        self._real = real
        self.quotes_selects = 0

    def execute(self, sql, *args, **kwargs):
        if "FROM quotes" in sql:
            self.quotes_selects += 1
        return self._real.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _seed(conn, n_quotes=20):
    for i in range(n_quotes):
        _ins_quote(
            conn,
            quote_number=f"R26Q{i:03d}",
            po_number=f"PO{i:05d}",
            agency="CSP California State Prison - Sacramento",
            total=1000.0 + i,
            sent_at="2026-05-01T00:00:00",
            created_at="2026-05-01T00:00:00",
        )


def test_hoisted_path_matches_self_fetch(conn):
    """Passing quote_rows must yield identical candidates to self-fetch."""
    _seed(conn)
    orphan = {
        "total": 1005.0,
        "agency": "CCHCS",
        "created_at": "2026-05-02T00:00:00",
        "po_canonical": "PO00005",
    }
    self_fetched = find_quote_candidates(conn, orphan, limit=5)
    rows = fetch_scorable_quotes(conn)
    hoisted = find_quote_candidates(conn, orphan, limit=5, quote_rows=rows)
    assert hoisted == self_fetched
    # And it actually found the exact-PO match, so the test is meaningful.
    assert any(c["tier"] == "po_exact" for c in hoisted)


def test_route_pattern_fetches_quotes_once_regardless_of_orphan_count(conn):
    """The hoist must collapse N per-orphan SELECTs into ONE.

    This is the direct regression pin for the hang: before the fix the
    quotes table was re-SELECTed once per orphan (N times); after it,
    the route fetches once and passes the snapshot in.
    """
    _seed(conn)
    counting = _CountingConn(conn)

    # Route pattern: one hoisted fetch, then score every orphan against it.
    quote_rows = fetch_scorable_quotes(counting)
    orphans = [
        {"total": 1000.0 + i, "agency": "CCHCS",
         "created_at": "2026-05-02T00:00:00", "po_canonical": f"PO{i:05d}"}
        for i in range(50)
    ]
    for orphan in orphans:
        find_quote_candidates(counting, orphan, limit=5, quote_rows=quote_rows)

    assert counting.quotes_selects == 1, (
        f"expected exactly 1 quotes SELECT for 50 orphans, "
        f"got {counting.quotes_selects} (per-orphan re-scan regressed)"
    )


def test_find_quote_candidates_does_not_fetch_when_rows_passed(conn):
    """With quote_rows supplied, the finder must not touch the DB at all."""
    _seed(conn, n_quotes=5)
    rows = fetch_scorable_quotes(conn)
    counting = _CountingConn(conn)
    orphan = {"total": 1002.0, "agency": "CCHCS",
              "created_at": "2026-05-02T00:00:00", "po_canonical": "PO00002"}
    find_quote_candidates(counting, orphan, limit=5, quote_rows=rows)
    assert counting.quotes_selects == 0


def test_fetch_scorable_quotes_excludes_test_and_blank(conn):
    """Snapshot must drop is_test rows and rows with no quote_number."""
    _ins_quote(conn, quote_number="R26Q001", po_number="PO1",
               agency="CCHCS", total=100.0, is_test=0)
    _ins_quote(conn, quote_number="R26Q002", po_number="PO2",
               agency="CCHCS", total=200.0, is_test=1)
    _ins_quote(conn, quote_number="", po_number="PO3",
               agency="CCHCS", total=300.0, is_test=0)
    rows = fetch_scorable_quotes(conn)
    nums = {r["quote_number"] for r in rows}
    assert nums == {"R26Q001"}


def test_agency_canonical_is_memoized():
    """Repeated resolves of the same string must hit the cache."""
    _agency_canonical.cache_clear()
    s = "CSP California State Prison - Sacramento"
    first = _agency_canonical(s)
    for _ in range(10):
        assert _agency_canonical(s) == first
    info = _agency_canonical.cache_info()
    assert info.hits >= 10, f"expected cache hits, got {info}"
