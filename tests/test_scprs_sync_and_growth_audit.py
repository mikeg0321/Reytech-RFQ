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
