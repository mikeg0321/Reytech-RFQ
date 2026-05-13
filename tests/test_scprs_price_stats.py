"""Phase 1.5-A — SCPRS per-SKU price rollup.

Migration #43 adds the `scprs_price_stats` table. Builder reads
`scprs_po_lines` + `scprs_po_master`, extracts MFG# and UNSPSC,
aggregates per (key, agency, year, qty_band) and writes percentile
stats. The oracle wire-up that READS from the rollup ships in a
follow-up — this PR's contract is just "the table exists with the
right shape and the builder produces sensible rows."
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest


# ── Helpers ───────────────────────────────────────────────────────


def test_extract_mfg_from_scprs_line_labeled():
    from src.agents.scprs_price_stats import extract_mfg_from_scprs_line
    assert extract_mfg_from_scprs_line("Bandage Sterile Mfg # 16-N8MMPA") == "16-N8MMPA"
    assert extract_mfg_from_scprs_line("Gloves Mfg Part # W12919") == "W12919"
    assert extract_mfg_from_scprs_line("Mfg# H-3989 Erase Board") == "H-3989"
    assert extract_mfg_from_scprs_line("Item: NL304 Cart") == "NL304"


def test_extract_mfg_from_scprs_line_no_label_returns_empty():
    """No positional fallback — SCPRS descriptions without a labeled MFG#
    contribute zero MFG# rollup rows. UNSPSC takes over for those lines."""
    from src.agents.scprs_price_stats import extract_mfg_from_scprs_line
    assert extract_mfg_from_scprs_line("Bandage Sterile 4x4") == ""
    assert extract_mfg_from_scprs_line("Catheter Foley 16Fr") == ""


def test_qty_band_buckets():
    from src.agents.scprs_price_stats import qty_band
    assert qty_band(1) == "1"
    assert qty_band(2) == "2-9"
    assert qty_band(9) == "2-9"
    assert qty_band(10) == "10-49"
    assert qty_band(49) == "10-49"
    assert qty_band(50) == "50-499"
    assert qty_band(499) == "50-499"
    assert qty_band(500) == "500+"
    assert qty_band(0) == "1"
    assert qty_band(None) == "1"
    assert qty_band("") == "1"


# ── Migration / table shape ──────────────────────────────────────


def _setup_schema(conn):
    """Minimal schema for the rollup test. Mirrors the production
    migration #43 + the SCPRS source tables we depend on."""
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY,
            agency_key TEXT, dept_code TEXT, dept_name TEXT,
            start_date TEXT, end_date TEXT
        );
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY,
            po_id INTEGER,
            description TEXT,
            unspsc TEXT,
            unit_price REAL,
            quantity REAL,
            line_status TEXT
        );
        CREATE TABLE scprs_price_stats (
            match_key_type TEXT NOT NULL CHECK(match_key_type IN ('mfg','unspsc')),
            match_key TEXT NOT NULL,
            agency TEXT NOT NULL,
            year TEXT NOT NULL,
            qty_band TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            mean REAL, p50 REAL, p75 REAL, p90 REAL,
            updated_at TEXT,
            PRIMARY KEY (match_key_type, match_key, agency, year, qty_band)
        );
        CREATE INDEX idx_sps_lookup
            ON scprs_price_stats(match_key_type, match_key, agency);
    """)


def test_migration_43_creates_table_with_index():
    """Pin the table shape against the migration source so a future edit
    can't silently drop the index or change the primary key."""
    from src.core import migrations as mig
    versions = [m[0] for m in mig.MIGRATIONS]
    assert 43 in versions, f"migration 43 missing from MIGRATIONS — versions = {sorted(versions)}"
    m43 = next(m for m in mig.MIGRATIONS if m[0] == 43)
    sql = m43[2]
    assert "CREATE TABLE IF NOT EXISTS scprs_price_stats" in sql
    assert "PRIMARY KEY (match_key_type, match_key, agency, year, qty_band)" in sql
    assert "CHECK(match_key_type IN ('mfg','unspsc'))" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_sps_lookup" in sql


# ── Rebuild builder against fixture data ─────────────────────────


@pytest.fixture
def rollup_conn(monkeypatch):
    """In-memory SQLite with the schema + a few seeded SCPRS rows."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _setup_schema(conn)

    # Two POs, three lines each — different agencies + qty bands.
    conn.executescript("""
        INSERT INTO scprs_po_master VALUES
            (1, 'cchcs', 'CDCR', 'CDCR Health', '2025-06-01', '2025-06-30'),
            (2, 'calvet', 'DVA',  'CalVet',      '2024-09-01', '2024-09-30');
        INSERT INTO scprs_po_lines
            (po_id, description, unspsc, unit_price, quantity, line_status) VALUES
            (1, 'Bandage Elastic Mfg # 16-N8MMPA', '42143000', 10.00, 30, 'open'),
            (1, 'Bandage Elastic Mfg # 16-N8MMPA', '42143000', 12.50, 25, 'open'),
            (1, 'Generic Bandage no mfg label',    '42143000',  8.75, 10, 'open'),
            (2, 'Bandage Elastic Mfg # 16-N8MMPA', '42143000', 11.00,  5, 'open'),
            (2, 'CANCELLED LINE Mfg # XXX',        '42143000',  9.99,  3, 'cancelled'),
            (2, 'Sterile Pad item: NL304',         '42143500', 30.00, 100, 'open');
    """)
    conn.commit()

    # Monkeypatch get_db to return our in-memory conn
    class _ConnCtx:
        def __init__(self, c):
            self._c = c
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False

    monkeypatch.setattr("src.core.db.get_db", lambda: _ConnCtx(conn))
    yield conn
    conn.close()


def test_rebuild_skips_cancelled_lines(rollup_conn):
    from src.agents.scprs_price_stats import rebuild_scprs_price_stats
    res = rebuild_scprs_price_stats(rollup_conn)
    # 6 rows total in the fixture; 1 is cancelled → 5 scanned
    assert res["lines_scanned"] == 5, res


def test_rebuild_skips_lines_without_keys(rollup_conn):
    """Generic Bandage (no Mfg label) still gets counted under UNSPSC
    but the no-key counter only fires when neither key is present.
    All fixture rows have UNSPSC, so skipped_no_key should be 0."""
    from src.agents.scprs_price_stats import rebuild_scprs_price_stats
    res = rebuild_scprs_price_stats(rollup_conn)
    assert res["skipped_no_key"] == 0, res


def test_rebuild_produces_mfg_aggregation(rollup_conn):
    """16-N8MMPA appears on 3 non-cancelled lines (cchcs×30, cchcs×25,
    calvet×5). Per-MFG×per-agency rollup should split correctly."""
    from src.agents.scprs_price_stats import (
        rebuild_scprs_price_stats, lookup_price_stat,
    )
    rebuild_scprs_price_stats(rollup_conn)
    # cchcs MFG# rollup: 2 lines, mean = (10+12.50)/2 = 11.25
    hit = lookup_price_stat(mfg_number="16-N8MMPA", agency="cchcs")
    assert hit is not None
    assert hit["match_key_type"] == "mfg"
    assert hit["count"] >= 1
    # mean should be in [9, 14] range (real samples: 10.00, 12.50)
    assert 9 <= hit["mean"] <= 14, hit


def test_rebuild_produces_unspsc_aggregation(rollup_conn):
    """UNSPSC 42143000 (surgical bandages) appears 4 times non-cancelled
    across 2 agencies. Cross-agency `*` rollup should aggregate all 4."""
    from src.agents.scprs_price_stats import (
        rebuild_scprs_price_stats, lookup_price_stat,
    )
    rebuild_scprs_price_stats(rollup_conn)
    hit = lookup_price_stat(unspsc="42143000", agency="*")
    assert hit is not None
    assert hit["match_key_type"] == "unspsc"
    assert hit["count"] >= 4


def test_rebuild_unspsc_family_rollup(rollup_conn):
    """8-digit UNSPSC 42143000 should also roll up under 4-digit family
    '4214' so the oracle has a fallback when the exact code is sparse."""
    from src.agents.scprs_price_stats import (
        rebuild_scprs_price_stats, lookup_price_stat,
    )
    rebuild_scprs_price_stats(rollup_conn)
    hit = lookup_price_stat(unspsc="4214", agency="*")
    assert hit is not None, "4-digit UNSPSC family rollup must exist"
    assert hit["count"] >= 4


def test_lookup_returns_none_when_no_match(rollup_conn):
    from src.agents.scprs_price_stats import (
        rebuild_scprs_price_stats, lookup_price_stat,
    )
    rebuild_scprs_price_stats(rollup_conn)
    assert lookup_price_stat(mfg_number="DOES-NOT-EXIST", agency="cchcs") is None
    assert lookup_price_stat(unspsc="99999999", agency="cchcs") is None


def test_lookup_falls_back_from_specific_to_wildcard_agency(rollup_conn):
    """When the exact agency bucket is empty but `*` has data, the
    lookup should return the cross-agency rollup rather than None."""
    from src.agents.scprs_price_stats import (
        rebuild_scprs_price_stats, lookup_price_stat,
    )
    rebuild_scprs_price_stats(rollup_conn)
    # `dsh` agency has zero SCPRS data in the fixture
    hit = lookup_price_stat(mfg_number="16-N8MMPA", agency="dsh")
    assert hit is not None
    assert hit["agency"] == "*", (
        f"should fall back to cross-agency, got {hit['agency']!r}"
    )


def test_rebuild_is_atomic_no_empty_window(rollup_conn):
    """Running rebuild twice should leave the table populated each
    time; readers must never see a transient empty state."""
    from src.agents.scprs_price_stats import rebuild_scprs_price_stats
    rebuild_scprs_price_stats(rollup_conn)
    n1 = rollup_conn.execute(
        "SELECT COUNT(*) FROM scprs_price_stats"
    ).fetchone()[0]
    rebuild_scprs_price_stats(rollup_conn)
    n2 = rollup_conn.execute(
        "SELECT COUNT(*) FROM scprs_price_stats"
    ).fetchone()[0]
    assert n1 == n2, f"rebuild row count should be stable; got {n1}, {n2}"
    assert n1 > 0


def test_rebuild_excludes_zero_or_negative_prices(monkeypatch):
    """Bad SCPRS rows with unit_price <= 0 must not poison the
    statistics. Verified via a small fixture with a zero-priced line."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _setup_schema(conn)
    conn.executescript("""
        INSERT INTO scprs_po_master VALUES
            (1, 'cchcs', 'CDCR', 'CDCR Health', '2025-06-01', '2025-06-30');
        INSERT INTO scprs_po_lines
            (po_id, description, unspsc, unit_price, quantity, line_status) VALUES
            (1, 'Bandage Mfg # X-1', '42143000', 10.00, 5, 'open'),
            (1, 'Bandage Mfg # X-1', '42143000',  0.00, 5, 'open'),
            (1, 'Bandage Mfg # X-1', '42143000', -1.00, 5, 'open');
    """)
    conn.commit()

    class _Ctx:
        def __init__(self, c):
            self._c = c
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False
    monkeypatch.setattr("src.core.db.get_db", lambda: _Ctx(conn))
    from src.agents.scprs_price_stats import (
        rebuild_scprs_price_stats, lookup_price_stat,
    )
    rebuild_scprs_price_stats(conn)
    hit = lookup_price_stat(mfg_number="X-1", agency="cchcs")
    assert hit is not None
    # Only the $10.00 row survives the filter — mean must equal 10
    assert hit["mean"] == 10.00, hit
    conn.close()
