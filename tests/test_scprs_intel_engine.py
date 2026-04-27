"""Regression tests for the SCPRS intelligence engine SQL bugs found
2026-04-27 during audit punch-list cleanup.

Two latent (actually-active) bugs:

1. `get_competitor_intelligence(agency_filter=...)` had `" + agency_clause + "`
   as LITERAL TEXT inside a triple-quoted SQL string. Same with `{...}` braces
   in the dvbe_opportunities and stats queries. SQLite returned
   `OperationalError: near "+ agency_clause +": syntax error` on EVERY call
   regardless of agency_filter. The /api/intel/competitors endpoint silently
   500'd on prod (visible only because the route wraps in try/except).

2. `search_scprs_data(agency=...)` accepted an `agency` parameter but never
   threaded it into any of the 5 SQL branches. Operator filter was a silent
   no-op — same query results regardless of the agency dropdown.

These tests prove both functions are now wired correctly. Hits a real
in-memory SQLite DB seeded with realistic SCPRS shapes; covers the empty-DB
case (would have masked the bug because OperationalError fired before any
row scan).
"""
import sqlite3
from unittest.mock import patch

import pytest


def _make_scprs_db(db_path):
    """File-backed SQLite seeded with the SCPRS schema + 4 PO masters across
    3 agencies + supporting po_lines. File-backed because the production code
    calls `conn.close()` at the end of each function — an in-memory shared
    handle would be unusable on the second call."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pulled_at TEXT, po_number TEXT, dept_code TEXT, dept_name TEXT,
            institution TEXT, agency_key TEXT, supplier TEXT, supplier_id TEXT,
            status TEXT, start_date TEXT, end_date TEXT, acq_type TEXT,
            acq_method TEXT, merch_amount REAL, grand_total REAL,
            buyer_name TEXT, buyer_email TEXT, buyer_phone TEXT, search_term TEXT
        );
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER, line_number INTEGER, description TEXT,
            quantity REAL, unit_price REAL, line_total REAL,
            category TEXT, reytech_sells INTEGER DEFAULT 0
        );
        CREATE TABLE scprs_pull_schedule (
            agency_key TEXT, last_pull TEXT, next_pull TEXT
        );
    """)
    # Seed 4 POs: 2 CCHCS, 1 CalVet, 1 DSH
    pos = [
        # (po_number, agency_key, supplier, total, institution, acq_type)
        ("PO-1001", "CCHCS", "medline industries", 50000, "Folsom State Prison", "Statewide"),
        ("PO-1002", "CCHCS", "amazon llc", 30000, "Pelican Bay", "Open Market"),
        ("PO-2001", "CalVet", "medline industries", 20000, "Veterans Home Yountville", "Statewide"),
        ("PO-3001", "DSH", "kimberly-clark", 15000, "Atascadero State Hospital", "Master"),
    ]
    for n, ak, sup, tot, inst, at in pos:
        cur = conn.execute("""
            INSERT INTO scprs_po_master
            (po_number, agency_key, supplier, grand_total, institution, acq_type,
             acq_method, dept_code, dept_name, start_date, status,
             buyer_name, buyer_email)
            VALUES (?, ?, ?, ?, ?, ?, '', '', '', '2025-01-01', 'closed',
                    'Test Buyer', 'buyer@test.gov')
        """, (n, ak, sup, tot, inst, at))
        po_id = cur.lastrowid
        # one line per PO, with reytech_sells=1 for medline gloves
        conn.execute("""
            INSERT INTO scprs_po_lines
            (po_id, line_number, description, quantity, unit_price, line_total,
             category, reytech_sells)
            VALUES (?, 1, ?, 100, ?, ?, 'medical', ?)
        """, (po_id, f"nitrile gloves for {sup}", tot/100, tot,
              1 if "medline" in sup else 0))
    conn.commit()
    return conn


@pytest.fixture
def scprs_db(tmp_path):
    db_path = tmp_path / "scprs_test.db"
    seed_conn = _make_scprs_db(db_path)
    seed_conn.close()
    yield db_path


def _patch_db(db_path):
    """Patch the module-level _db() to open a fresh connection per call.
    Each call must return a NEW connection because the function under test
    calls conn.close() at the end — sharing a handle breaks test #2 onward."""
    def _open():
        c = sqlite3.connect(str(db_path))
        c.row_factory = sqlite3.Row
        return c
    return patch("src.agents.scprs_intelligence_engine._db", side_effect=_open)


class TestGetCompetitorIntelligence:
    """Bug 1 regression: triple-quoted SQL with literal Python expression text."""

    def test_no_filter_doesnt_crash(self, scprs_db):
        # Without the fix, this raised OperationalError on every call.
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        with _patch_db(scprs_db):
            r = get_competitor_intelligence(agency_filter="", limit=10)
        assert r["ok"] is True
        assert "competitors" in r
        # 3 distinct suppliers across our 4 POs (medline appears twice)
        assert len(r["competitors"]) == 3

    def test_agency_filter_uppercase_match(self, scprs_db):
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        with _patch_db(scprs_db):
            r = get_competitor_intelligence(agency_filter="CCHCS", limit=10)
        assert r["ok"] is True
        # 2 CCHCS POs, 2 distinct suppliers (medline + amazon)
        assert len(r["competitors"]) == 2
        suppliers = {c["supplier"] for c in r["competitors"]}
        assert suppliers == {"medline industries", "amazon llc"}

    def test_agency_filter_lowercase_match(self, scprs_db):
        # Operators may pass abbreviation in any case; comparison is
        # case-insensitive via UPPER on both sides.
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        with _patch_db(scprs_db):
            r = get_competitor_intelligence(agency_filter="cchcs", limit=10)
        assert r["ok"] is True
        assert len(r["competitors"]) == 2

    def test_agency_filter_isolates_other_agencies(self, scprs_db):
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        with _patch_db(scprs_db):
            r = get_competitor_intelligence(agency_filter="DSH", limit=10)
        assert r["ok"] is True
        assert len(r["competitors"]) == 1
        assert r["competitors"][0]["supplier"] == "kimberly-clark"

    def test_stats_block_works_with_filter(self, scprs_db):
        # The stats query had its own broken `{... if ... else ""}` literal;
        # this test asserts the WHERE clause now applies correctly.
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        with _patch_db(scprs_db):
            r_all = get_competitor_intelligence(agency_filter="", limit=10)
            r_cv = get_competitor_intelligence(agency_filter="CalVet", limit=10)
        assert r_all["stats"]["total_pos"] == 4
        assert r_cv["stats"]["total_pos"] == 1

    def test_dvbe_opportunities_query_doesnt_crash(self, scprs_db):
        # Line 1308 had broken f-style braces inside the triple-quote.
        from src.agents.scprs_intelligence_engine import get_competitor_intelligence
        with _patch_db(scprs_db):
            r = get_competitor_intelligence(agency_filter="", limit=10)
        assert "dvbe_opportunities" in r
        # Don't assert specific opportunities — depends on KNOWN_NON_DVBE_INCUMBENTS
        # contents. Just prove the query ran (returned a list, not crashed).
        assert isinstance(r["dvbe_opportunities"], list)


class TestSearchScprsData:
    """Bug 2 regression: agency parameter accepted but never used."""

    def test_no_agency_returns_all(self, scprs_db):
        from src.agents.scprs_intelligence_engine import search_scprs_data
        with _patch_db(scprs_db):
            r = search_scprs_data("medline", search_type="supplier", agency="")
        assert r["ok"] is True
        # medline appears in CCHCS + CalVet — both should surface when no filter
        assert r["total"] >= 1
        names = [x["name"] for x in r["results"]]
        assert any("medline" in n for n in names)

    def test_agency_filter_narrows_supplier_results(self, scprs_db):
        # With agency="CalVet", searching medline should only count CalVet POs.
        from src.agents.scprs_intelligence_engine import search_scprs_data
        with _patch_db(scprs_db):
            r_all = search_scprs_data("medline", search_type="supplier", agency="")
            r_cv = search_scprs_data("medline", search_type="supplier", agency="CalVet")
        assert r_all["ok"] is True and r_cv["ok"] is True
        # Same supplier name, but CalVet-only should show only the 1 PO
        if r_all["results"] and r_cv["results"]:
            all_pos = r_all["results"][0]["detail"]
            cv_pos = r_cv["results"][0]["detail"]
            # Detail string starts with "{N} POs"; CalVet should be ≤ all
            n_all = int(all_pos.split()[0])
            n_cv = int(cv_pos.split()[0])
            assert n_cv <= n_all
            assert n_cv == 1  # exactly 1 CalVet PO with medline

    def test_agency_filter_isolates_buyer_search(self, scprs_db):
        # Buyer search branch used to ignore agency entirely.
        from src.agents.scprs_intelligence_engine import search_scprs_data
        with _patch_db(scprs_db):
            r_all = search_scprs_data("buyer@test", search_type="buyer", agency="")
            r_dsh = search_scprs_data("buyer@test", search_type="buyer", agency="DSH")
        assert r_all["ok"] is True and r_dsh["ok"] is True
        # All 4 POs share the same buyer email → 1 grouped row in r_all,
        # but DSH-only should also be 1 grouped row (only DSH POs counted).
        # The proof of filtering is in the PO count + spend within that row.
        if r_all["results"] and r_dsh["results"]:
            assert "4 POs" in r_all["results"][0]["detail"]
            assert "1 PO" in r_dsh["results"][0]["detail"]

    def test_agency_filter_isolates_institution_search(self, scprs_db):
        from src.agents.scprs_intelligence_engine import search_scprs_data
        with _patch_db(scprs_db):
            # "veterans" matches the CalVet institution + nothing else
            r = search_scprs_data("veterans", search_type="institution", agency="DSH")
        # Filtered to DSH → no results (veterans home is CalVet)
        assert r["ok"] is True
        assert r["total"] == 0

    def test_agency_filter_lowercase_works(self, scprs_db):
        from src.agents.scprs_intelligence_engine import search_scprs_data
        with _patch_db(scprs_db):
            r = search_scprs_data("medline", search_type="supplier", agency="cchcs")
        assert r["ok"] is True
        assert r["total"] >= 1
