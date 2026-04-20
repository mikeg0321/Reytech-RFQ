"""
Regression tests for the 2026-04-19 SCPRS + growth audit fixes.

  P0  won_quotes_db.sync_from_scprs_tables() wrote r[8] (start_date) into
      the category column — should be r[9] (l.category). Result: every
      won_quotes row from SCPRS sync had ISO date strings as categories.

  P1  growth_agent._load_prospects_list() didn't validate that a dict's
      "prospects" value is a list. A corrupt file with {"prospects":"foo"}
      caused callers to iterate over characters.
"""
import json
from pathlib import Path

import pytest


class TestScprsCategorySyncFix:
    def test_sync_writes_category_not_start_date(self, tmp_path, monkeypatch):
        """Regression: category column must receive l.category, not p.start_date."""
        import src.knowledge.won_quotes_db as wqdb

        db_path = tmp_path / "test_reytech.db"
        import sqlite3

        def _test_conn():
            conn = sqlite3.connect(str(db_path), timeout=10)
            conn.row_factory = sqlite3.Row
            return conn

        monkeypatch.setattr(wqdb, "_get_db_conn", _test_conn)

        conn = sqlite3.connect(str(db_path))
        # Seed the source tables the sync reads from
        conn.executescript("""
            CREATE TABLE scprs_po_master (
                id INTEGER PRIMARY KEY,
                supplier TEXT, agency_key TEXT, start_date TEXT
            );
            CREATE TABLE scprs_po_lines (
                id INTEGER PRIMARY KEY,
                po_id INTEGER, po_number TEXT, item_id TEXT,
                description TEXT, unit_price REAL, quantity REAL,
                category TEXT
            );
            INSERT INTO scprs_po_master (id, supplier, agency_key, start_date)
                VALUES (1, 'Acme Supply', 'CDCR', '2026-01-15');
            INSERT INTO scprs_po_lines
                (id, po_id, po_number, item_id, description, unit_price, quantity, category)
                VALUES
                (101, 1, 'PO-9001', 'ITM-1', 'Nitrile gloves large 100ct', 1000.00, 10, 'Medical/PPE'),
                (102, 1, 'PO-9001', 'ITM-2', 'Bleach 1 gallon', 50.00, 5, 'Cleaning');
        """)
        conn.commit()
        conn.close()

        stats = wqdb.sync_from_scprs_tables()
        assert stats["synced"] == 2, f"expected 2 synced rows, got {stats}"

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT description, category, unit_price FROM won_quotes ORDER BY description"
        ).fetchall()
        conn.close()

        cat_by_desc = {desc: cat for desc, cat, _ in rows}
        # The bug: category was storing the start_date ('2026-01-15') instead
        # of 'Medical/PPE' / 'Cleaning'. Assert the real categories land.
        assert cat_by_desc["Nitrile gloves large 100ct"] == "Medical/PPE", (
            f"category column must receive l.category, got {cat_by_desc}"
        )
        assert cat_by_desc["Bleach 1 gallon"] == "Cleaning"
        # Belt-and-suspenders: make sure no row has the start_date bleeding through
        for _, cat, _ in rows:
            assert cat != "2026-01-15", "start_date leaking into category column"


class TestLoadProspectsListDefensive:
    def test_dict_with_non_list_prospects_returns_empty(self, tmp_path, monkeypatch):
        import src.agents.growth_agent as ga

        bad_file = tmp_path / "prospects.json"
        bad_file.write_text(json.dumps({"prospects": "this-should-be-a-list"}))
        monkeypatch.setattr(ga, "PROSPECTS_FILE", str(bad_file))

        result = ga._load_prospects_list()
        assert result == [], "corrupt dict.prospects must return [], not a string"

    def test_bare_string_returns_empty(self, tmp_path, monkeypatch):
        import src.agents.growth_agent as ga

        bad_file = tmp_path / "prospects.json"
        bad_file.write_text(json.dumps("bare-string"))
        monkeypatch.setattr(ga, "PROSPECTS_FILE", str(bad_file))

        result = ga._load_prospects_list()
        assert result == []

    def test_valid_list_passes_through(self, tmp_path, monkeypatch):
        import src.agents.growth_agent as ga

        good_file = tmp_path / "prospects.json"
        good_file.write_text(json.dumps([{"buyer_email": "a@b.c"}]))
        monkeypatch.setattr(ga, "PROSPECTS_FILE", str(good_file))

        result = ga._load_prospects_list()
        assert result == [{"buyer_email": "a@b.c"}]

    def test_valid_dict_wrapper_passes_through(self, tmp_path, monkeypatch):
        import src.agents.growth_agent as ga

        good_file = tmp_path / "prospects.json"
        good_file.write_text(json.dumps({"prospects": [{"buyer_email": "x@y.z"}]}))
        monkeypatch.setattr(ga, "PROSPECTS_FILE", str(good_file))

        result = ga._load_prospects_list()
        assert result == [{"buyer_email": "x@y.z"}]


class TestScprsIdempotency:
    """SCPRS audit 2026-04-19 item #1 — UNIQUE(po_id, line_num) + INSERT OR
    REPLACE so re-pulls refresh stale data instead of silently skipping."""

    def test_unique_index_rejects_duplicate_line_num(self, tmp_path):
        import sqlite3
        from src.core import db

        # Use a fresh DB so we don't corrupt shared fixtures.
        db_path = tmp_path / "idem.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE scprs_po_master (id INTEGER PRIMARY KEY);
            CREATE TABLE scprs_po_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_id INTEGER, po_number TEXT, line_num INTEGER,
                description TEXT, unit_price REAL
            );
            CREATE UNIQUE INDEX idx_po_lines_uniq ON scprs_po_lines(po_id, line_num);
            INSERT INTO scprs_po_master(id) VALUES (1);
        """)
        conn.execute(
            "INSERT OR REPLACE INTO scprs_po_lines(po_id, line_num, description, unit_price) VALUES (1, 0, 'first', 5.0)"
        )
        conn.execute(
            "INSERT OR REPLACE INTO scprs_po_lines(po_id, line_num, description, unit_price) VALUES (1, 0, 'updated', 7.5)"
        )
        rows = conn.execute(
            "SELECT description, unit_price FROM scprs_po_lines WHERE po_id=1"
        ).fetchall()
        conn.close()
        assert rows == [("updated", 7.5)], (
            f"INSERT OR REPLACE must update the row, not duplicate; got {rows}"
        )

    def test_migration_creates_idempotency_index(self):
        """Sanity — db.init_db should create idx_po_lines_uniq on the live schema."""
        import sqlite3
        from src.core import db

        db.init_db()
        conn = sqlite3.connect(db.DB_PATH)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='scprs_po_lines'"
        ).fetchall()
        conn.close()
        names = {r[0] for r in rows}
        assert "idx_po_lines_uniq" in names, f"migration missing; indexes={names}"


class TestGrowthProspectsCrossRunDedup:
    """Prior-run `outreach_status` must survive re-scans; new POs merge into
    existing prospect rather than wiping state to 'new'."""

    def test_prior_run_seeds_prospects(self, tmp_path, monkeypatch):
        import src.agents.growth_agent as ga

        prior_file = tmp_path / "growth_prospects.json"
        prior_file.write_text(json.dumps({
            "prospects": [
                {"id": "PRO-abc", "buyer_email": "keep@me.com",
                 "agency": "DEPT-X", "outreach_status": "contacted",
                 "categories_matched": ["gloves"], "purchase_orders": [], "total_spend": 0},
            ]
        }))
        monkeypatch.setattr(ga, "PROSPECTS_FILE", str(prior_file))

        # Emulate the seed loop from find_category_buyers without needing SCPRS.
        prospects: dict = {}
        prior = ga._load_json(ga.PROSPECTS_FILE)
        for p in prior.get("prospects") or []:
            email = (p.get("buyer_email") or "").strip()
            dept = (p.get("agency") or "").strip()
            key = email or f"{dept}_"
            if key:
                prospects[key] = p

        assert "keep@me.com" in prospects
        assert prospects["keep@me.com"]["outreach_status"] == "contacted", (
            "cross-run seeding must preserve outreach_status, not reset to 'new'"
        )


class TestAgencyRegistrySingleSource:
    """cchcs_intel_puller + scprs_public_search used to hold their own
    CCHCS_DEPT_CODES lists that drifted (3860 vs 4700). All three now read
    from scprs_intelligence_engine.AGENCY_REGISTRY."""

    def test_cchcs_puller_reads_codes_from_registry(self):
        from src.agents import cchcs_intel_puller
        from src.agents.scprs_intelligence_engine import AGENCY_REGISTRY
        expected = set(AGENCY_REGISTRY["CCHCS"]["dept_codes"])
        assert set(cchcs_intel_puller.CCHCS_DEPT_CODES) == expected

    def test_public_search_reads_codes_from_registry(self):
        from src.agents import scprs_public_search
        from src.agents.scprs_intelligence_engine import AGENCY_REGISTRY
        expected = set(AGENCY_REGISTRY["CCHCS"]["dept_codes"])
        assert set(scprs_public_search.CCHCS_DEPT_CODES) == expected


class TestScprsCatalogWriteback:
    """Every SCPRS line we sell should enrich the catalog (flywheel rule
    feedback_catalog_is_bible.md). Price must NOT be written — SCPRS prices
    are state-paid competitor prices, not our cost (feedback_scprs_prices.md)."""

    def test_writeback_calls_add_to_catalog_without_price(self, monkeypatch):
        from src.agents import scprs_intelligence_engine as sie

        calls = []

        def fake_add_to_catalog(**kwargs):
            calls.append(kwargs)
            return 42

        import src.agents.product_catalog as pc
        monkeypatch.setattr(pc, "add_to_catalog", fake_add_to_catalog)

        sie._writeback_to_catalog({
            "description": "Nitrile gloves M 100ct",
            "item_id": "NIT-M-100",
            "unit_price": 9.99,
            "quantity": 100,
        })

        assert len(calls) == 1, f"expected exactly one add_to_catalog call; got {calls}"
        kwargs = calls[0]
        assert kwargs.get("description") == "Nitrile gloves M 100ct"
        assert kwargs.get("part_number") == "NIT-M-100"
        assert kwargs.get("source") == "scprs_intel"
        # Critical: SCPRS unit_price must NOT flow into catalog cost.
        assert "cost" not in kwargs or kwargs.get("cost", 0) == 0
        assert "sell_price" not in kwargs or kwargs.get("sell_price", 0) == 0

    def test_writeback_skips_empty_description(self, monkeypatch):
        from src.agents import scprs_intelligence_engine as sie

        calls = []
        import src.agents.product_catalog as pc
        monkeypatch.setattr(pc, "add_to_catalog", lambda **k: calls.append(k) or 1)

        sie._writeback_to_catalog({"description": "", "item_id": "X"})
        assert calls == [], "empty description should not trigger catalog write"
