"""PR-C — award_monitor per-line delta resolution.

Migration #44 adds `competitor_intel_lines` (child of competitor_intel,
one row per SCPRS line under a loss). `_check_scprs_award` now returns
all PO lines (not just the first). `log_competitor` writes per-line
rows via fuzzy match against `pc["items"]`:
  1. MFG# normalized + exact match — `matched_by='mfg_exact'`
  2. Description token-set overlap >= 0.5 — `matched_by='desc_tokens'`
  3. None — line still recorded with `matched_by='none'` so spend
     visibility survives even when we can't tell which of our items
     the competitor priced.

This pins the contract before the Phase 3 digest renderer reads the
per-line table.
"""
from __future__ import annotations

import os
import sqlite3
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Migration #44 shape ──────────────────────────────────────────


def test_migration_44_creates_competitor_intel_lines():
    from src.core import migrations as mig
    versions = [m[0] for m in mig.MIGRATIONS]
    assert 44 in versions, f"migration 44 missing — versions: {sorted(versions)}"
    m44 = next(m for m in mig.MIGRATIONS if m[0] == 44)
    sql = m44[2]
    assert "CREATE TABLE IF NOT EXISTS competitor_intel_lines" in sql
    assert "REFERENCES competitor_intel(id)" in sql
    assert "ON DELETE CASCADE" in sql
    # Critical columns for the digest renderer
    for col in ("competitor_intel_id", "line_num", "scprs_description",
                "scprs_unit_price", "scprs_mfg", "our_item_idx",
                "our_unit_price", "price_delta_pct", "matched_by"):
        assert col in sql, f"column {col!r} missing from migration #44"
    # Index on parent FK is required for per-PC digest queries
    assert "CREATE INDEX IF NOT EXISTS idx_cil_parent" in sql


# ── _persist_per_line_deltas ─────────────────────────────────────


def _setup_schema(conn):
    """Minimal schema mirroring competitor_intel + the new child table."""
    conn.executescript("""
        CREATE TABLE competitor_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            found_at TEXT, pc_id TEXT, quote_number TEXT,
            our_price REAL, competitor_name TEXT, competitor_price REAL,
            price_delta REAL, price_delta_pct REAL, po_number TEXT,
            agency TEXT, institution TEXT, item_summary TEXT,
            items_detail TEXT, solicitation TEXT, outcome TEXT, notes TEXT,
            loss_reason_class TEXT DEFAULT '',
            our_cost REAL DEFAULT 0, our_margin_pct REAL DEFAULT 0,
            margin_too_high INTEGER DEFAULT 0, category TEXT DEFAULT ''
        );
        CREATE TABLE competitor_intel_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor_intel_id INTEGER NOT NULL,
            line_num INTEGER, scprs_description TEXT,
            scprs_unit_price REAL, scprs_quantity REAL,
            scprs_mfg TEXT, scprs_unspsc TEXT,
            our_item_idx INTEGER, our_unit_price REAL, our_mfg TEXT,
            price_delta_pct REAL, matched_by TEXT DEFAULT 'none',
            created_at TEXT
        );
    """)


def test_per_line_mfg_exact_match_persists_with_delta():
    """When SCPRS line description contains a labeled MFG# that matches
    one of our PC item MFG#s, the row should land with
    matched_by='mfg_exact' and a non-null price_delta_pct."""
    from src.agents.award_monitor import _persist_per_line_deltas
    conn = sqlite3.connect(":memory:")
    _setup_schema(conn)
    # Seed a parent loss row
    cur = conn.execute(
        "INSERT INTO competitor_intel (found_at) VALUES ('2026-05-13')"
    )
    parent_id = cur.lastrowid

    pc = {
        "items": [
            {"mfg_number": "16-N8MMPA", "description": "Insulin Pen Needle",
             "unit_price": 20.00},
            {"mfg_number": "W12919",    "description": "Surgical Glove",
             "unit_price": 5.00},
        ],
    }
    scprs_lines = [
        # MFG# exact match — competitor at $18 vs our $20 = -10%
        {"line_num": 1, "description": "Insulin Pen Mfg # 16-N8MMPA",
         "unit_price": 18.00, "quantity": 30, "unspsc": "42143000"},
        # MFG# exact match — competitor at $5.50 vs our $5 = +10%
        {"line_num": 2, "description": "Surgical Glove Mfg # W12919",
         "unit_price": 5.50, "quantity": 100, "unspsc": "42132205"},
    ]
    n = _persist_per_line_deltas(conn, parent_id, scprs_lines, pc, "2026-05-13")
    assert n == 2
    rows = list(conn.execute(
        "SELECT line_num, scprs_unit_price, our_unit_price, "
        "price_delta_pct, matched_by FROM competitor_intel_lines "
        "ORDER BY line_num"
    ))
    assert len(rows) == 2
    assert rows[0][4] == "mfg_exact"
    assert rows[0][3] == -10.0          # competitor 10% cheaper
    assert rows[1][4] == "mfg_exact"
    assert rows[1][3] == 10.0           # we were 10% cheaper than competitor
    conn.close()


def test_per_line_desc_token_match_fallback():
    """When MFG# extraction misses (no labeled pattern in SCPRS desc),
    falls back to description-token overlap >= 0.5."""
    from src.agents.award_monitor import _persist_per_line_deltas
    conn = sqlite3.connect(":memory:")
    _setup_schema(conn)
    parent_id = conn.execute(
        "INSERT INTO competitor_intel (found_at) VALUES ('t')"
    ).lastrowid

    pc = {"items": [
        {"description": "Sterile Surgical Drape Adhesive 100x150",
         "unit_price": 30.00},
    ]}
    scprs_lines = [
        # No labeled MFG#, but the description shares 4+ tokens
        {"line_num": 1, "description": "sterile drape surgical adhesive",
         "unit_price": 28.00, "quantity": 50, "unspsc": "42143400"},
    ]
    _persist_per_line_deltas(conn, parent_id, scprs_lines, pc, "t")
    row = conn.execute(
        "SELECT matched_by, our_unit_price, price_delta_pct "
        "FROM competitor_intel_lines"
    ).fetchone()
    assert row[0] == "desc_tokens"
    assert row[1] == 30.00
    # Competitor $28 vs ours $30 — delta is roughly -6.7%
    assert -8.0 <= row[2] <= -5.0
    conn.close()


def test_per_line_no_match_still_persists():
    """When neither MFG# nor desc tokens match, the SCPRS line is
    still recorded with our_item_idx=None and matched_by='none' —
    spend visibility on the competitor side survives even when
    item correspondence is unknown."""
    from src.agents.award_monitor import _persist_per_line_deltas
    conn = sqlite3.connect(":memory:")
    _setup_schema(conn)
    parent_id = conn.execute(
        "INSERT INTO competitor_intel (found_at) VALUES ('t')"
    ).lastrowid

    pc = {"items": [
        {"mfg_number": "AAA-111", "description": "Bandage Sterile",
         "unit_price": 10.00},
    ]}
    scprs_lines = [
        {"line_num": 1, "description": "Catheter Foley silicone (totally different)",
         "unit_price": 50.00, "quantity": 1, "unspsc": "42321500"},
    ]
    _persist_per_line_deltas(conn, parent_id, scprs_lines, pc, "t")
    row = conn.execute(
        "SELECT our_item_idx, matched_by, price_delta_pct, scprs_unit_price "
        "FROM competitor_intel_lines"
    ).fetchone()
    assert row[0] is None        # our_item_idx — no match
    assert row[1] == "none"
    assert row[2] is None        # can't compute delta without our_unit_price
    assert row[3] == 50.0        # SCPRS line still persisted
    conn.close()


def test_per_line_does_not_double_match_same_pc_item():
    """If two SCPRS lines BOTH match the same PC item (e.g. two bulk
    orders of the same SKU under one PO), only the first claims it.
    The second falls back to no-match rather than recording two
    contradictory deltas against the same PC line."""
    from src.agents.award_monitor import _persist_per_line_deltas
    conn = sqlite3.connect(":memory:")
    _setup_schema(conn)
    parent_id = conn.execute(
        "INSERT INTO competitor_intel (found_at) VALUES ('t')"
    ).lastrowid

    pc = {"items": [
        {"mfg_number": "X-1", "description": "Bandage", "unit_price": 10.00},
    ]}
    scprs_lines = [
        {"line_num": 1, "description": "Bandage Mfg # X-1",
         "unit_price": 9.00, "quantity": 100, "unspsc": "42143000"},
        {"line_num": 2, "description": "Bandage Mfg # X-1",
         "unit_price": 8.50, "quantity": 50, "unspsc": "42143000"},
    ]
    _persist_per_line_deltas(conn, parent_id, scprs_lines, pc, "t")
    rows = list(conn.execute(
        "SELECT line_num, matched_by, our_item_idx FROM competitor_intel_lines "
        "ORDER BY line_num"
    ))
    assert rows[0][1] == "mfg_exact"
    assert rows[0][2] == 0
    # Second line — same MFG# — must NOT also claim idx 0
    assert rows[1][2] is None or rows[1][1] != "mfg_exact"
    conn.close()


def test_per_line_returns_zero_with_empty_lines():
    """Defensive: no SCPRS lines provided → 0 inserted, no exception."""
    from src.agents.award_monitor import _persist_per_line_deltas
    conn = sqlite3.connect(":memory:")
    _setup_schema(conn)
    parent_id = conn.execute(
        "INSERT INTO competitor_intel (found_at) VALUES ('t')"
    ).lastrowid
    n = _persist_per_line_deltas(conn, parent_id, [], {"items": []}, "t")
    assert n == 0
    conn.close()


# ── log_competitor integration ───────────────────────────────────


def test_log_competitor_writes_per_line_when_award_has_lines(monkeypatch):
    """Full path: an award with `lines` populated by _check_scprs_award
    should land both the parent competitor_intel row AND one
    competitor_intel_lines row per SCPRS line."""
    from src.agents import award_monitor
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _setup_schema(conn)

    class _Ctx:
        def __init__(self, c):
            self._c = c
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False
    monkeypatch.setattr(award_monitor, "get_db", lambda: _Ctx(conn))

    pc = {
        "id": "pc_test_001",
        "items": [
            {"mfg_number": "16-N8MMPA", "description": "Pen Needle",
             "unit_price": 20.00},
        ],
    }
    award = {
        "outcome": "lost",
        "supplier": "Acme Medical Supply",
        "po_number": "PO-12345",
        "total": 540.00,
        "lines": [
            {"line_num": 1, "description": "Insulin Pen Mfg # 16-N8MMPA",
             "unit_price": 18.00, "quantity": 30, "unspsc": "42143000"},
        ],
    }
    award_monitor.log_competitor(pc, award, our_quote_total=600.0)
    # Parent + child rows
    parent_count = conn.execute("SELECT COUNT(*) FROM competitor_intel").fetchone()[0]
    line_count = conn.execute("SELECT COUNT(*) FROM competitor_intel_lines").fetchone()[0]
    assert parent_count == 1
    assert line_count == 1
    line = conn.execute("SELECT matched_by, price_delta_pct FROM competitor_intel_lines").fetchone()
    assert line["matched_by"] == "mfg_exact"
    assert line["price_delta_pct"] == -10.0
    conn.close()


def test_log_competitor_back_compat_without_lines(monkeypatch):
    """Awards from old code paths (no `lines` key) must still log the
    parent row — the per-line table just stays empty for that loss.
    Back-compat is critical: we don't want to break existing callers."""
    from src.agents import award_monitor
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _setup_schema(conn)

    class _Ctx:
        def __init__(self, c):
            self._c = c
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False
    monkeypatch.setattr(award_monitor, "get_db", lambda: _Ctx(conn))

    pc = {"id": "pc_legacy", "items": [{"description": "thing"}]}
    award = {
        "outcome": "lost",
        "supplier": "Old Path",
        "po_number": "PO-OLD",
        "total": 100.0,
        # no "lines" key
    }
    award_monitor.log_competitor(pc, award, our_quote_total=120.0)
    assert conn.execute("SELECT COUNT(*) FROM competitor_intel").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM competitor_intel_lines").fetchone()[0] == 0
    conn.close()
