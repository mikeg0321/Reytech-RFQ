"""
Regression tests for the 2026-04-19 catalog audit fixes:

  #20  upc column + index exists
  #21  UNIQUE(name) enforced on existing DBs (via unique index)
  #22  enrich_catalog_product surfaces failures as WARNING (not debug)
  #23  fuzzy-match threshold raised 0.50 → 0.65
  #24  _score_product awards +100 bonus for exact SKU / MFG# token match
"""
import logging

import pytest

from src.agents.product_catalog import (
    init_catalog_db, _get_conn, _score_product, match_item,
    enrich_catalog_product, smart_search,
)


@pytest.fixture
def clean_catalog():
    init_catalog_db()
    conn = _get_conn()
    conn.execute("DELETE FROM product_catalog")
    conn.commit()
    conn.close()
    yield


class TestUpcColumn:
    def test_upc_column_present(self, clean_catalog):
        conn = _get_conn()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(product_catalog)").fetchall()}
        conn.close()
        assert "upc" in cols, "upc column must exist for barcode lookup to work"

    def test_upc_index_present(self, clean_catalog):
        conn = _get_conn()
        idx_names = {row[1] for row in conn.execute("PRAGMA index_list(product_catalog)").fetchall()}
        conn.close()
        assert "idx_catalog_upc" in idx_names

    def test_upc_lookup_returns_match(self, clean_catalog):
        conn = _get_conn()
        now = "2026-04-19T00:00:00"
        conn.execute(
            "INSERT INTO product_catalog (name, sku, upc, sell_price, cost, "
            "created_at, updated_at) VALUES (?, ?, ?, 10.0, 6.0, ?, ?)",
            ("Barcode Item A", "SKU-A", "012345678905", now, now),
        )
        conn.commit()
        conn.close()
        matches = match_item("unrelated description text", upc="012345678905", top_n=3)
        assert any(m.get("match_reason", "").startswith("UPC exact match") for m in matches)


class TestUniqueNameIndex:
    def test_unique_name_index_present(self, clean_catalog):
        conn = _get_conn()
        idx_names = {row[1] for row in conn.execute("PRAGMA index_list(product_catalog)").fetchall()}
        conn.close()
        assert "idx_catalog_name_unique" in idx_names

    def test_duplicate_name_insert_rejected(self, clean_catalog):
        import sqlite3
        conn = _get_conn()
        now = "2026-04-19T00:00:00"
        conn.execute(
            "INSERT INTO product_catalog (name, sell_price, cost, created_at, updated_at) "
            "VALUES (?, 1.0, 0.5, ?, ?)", ("Duplicated Name", now, now),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO product_catalog (name, sell_price, cost, created_at, updated_at) "
                "VALUES (?, 2.0, 1.0, ?, ?)", ("Duplicated Name", now, now),
            )
        conn.close()


class TestEnrichmentWarnings:
    def test_enrich_failures_log_at_warning_level(self, clean_catalog, caplog, monkeypatch):
        """Force every UPDATE to raise, verify the handler logs at WARNING.

        enrich_catalog_product() calls init_catalog_db() first, so we swap
        _get_conn to a boom-conn AFTER init runs.
        """
        import src.agents.product_catalog as pc_mod

        class _BoomConn:
            total_changes = 0
            def execute(self, *a, **kw): raise RuntimeError("boom")
            def commit(self): pass
            def close(self): pass

        orig_init = pc_mod.init_catalog_db
        def _stub_init(): pass  # skip real init so _BoomConn isn't used during schema setup
        monkeypatch.setattr(pc_mod, "init_catalog_db", _stub_init)
        monkeypatch.setattr(pc_mod, "_get_conn", lambda: _BoomConn())
        with caplog.at_level(logging.WARNING, logger="reytech.product_catalog"):
            enrich_catalog_product(1, upc="012345678905", best_cost=5.0, photo_url="http://x/y.jpg")
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("enrich_catalog_product" in r.getMessage() for r in warnings), (
            "enrichment failures must surface as WARNING (not silently at DEBUG)"
        )
        # Restore for any later tests in the module (monkeypatch handles it too).
        monkeypatch.setattr(pc_mod, "init_catalog_db", orig_init)


class TestFuzzyThreshold:
    def test_threshold_raised_to_065(self):
        """Source-level check: the token-match filter is now 0.65, not 0.50."""
        import inspect
        import src.agents.product_catalog as pc_mod
        src = inspect.getsource(pc_mod.match_item)
        assert "similarity >= 0.65" in src, (
            "fuzzy-match threshold must be 0.65 per 2026-04-19 audit"
        )
        assert "similarity >= 0.50" not in src, "legacy 0.50 threshold must be removed"


class TestExactIdBonus:
    def test_exact_sku_token_awards_bonus(self):
        product = {
            "sku": "glv-nit-l-100",
            "mfg_number": "44794",
            "name": "Nitrile gloves large",
            "description": "", "category": "", "manufacturer": "", "tags": "",
            "times_quoted": 0,
        }
        # Token list includes exact SKU (lowercased as the tokenizer would emit).
        score_with_exact, hits = _score_product(product, ["glv-nit-l-100", "nitrile"])
        score_without, _ = _score_product(product, ["nitrile", "gloves"])
        assert score_with_exact >= score_without + 90, (
            f"exact SKU token should add ~100 bonus; got {score_with_exact} vs {score_without}"
        )
        assert "exact_id" in hits

    def test_exact_mfg_token_awards_bonus(self):
        product = {
            "sku": "generic-sku",
            "mfg_number": "44794",
            "name": "Nitrile gloves large",
            "description": "", "category": "", "manufacturer": "", "tags": "",
            "times_quoted": 0,
        }
        score_with_exact, hits = _score_product(product, ["44794", "gloves"])
        assert score_with_exact >= 100.0
        assert "exact_id" in hits

    def test_no_bonus_when_no_exact_match(self):
        product = {
            "sku": "glv-nit-l-100",
            "mfg_number": "44794",
            "name": "Nitrile gloves large",
            "description": "", "category": "", "manufacturer": "", "tags": "",
            "times_quoted": 0,
        }
        score, hits = _score_product(product, ["nitrile", "gloves"])
        assert score < 90.0, "no bonus should fire without exact SKU/MFG# token"
        assert "exact_id" not in hits
