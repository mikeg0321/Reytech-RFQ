"""Tests for the #4/#5 observability surface:

  #4  /health/quoting + /api/health/catalog must report UNIQUE(name)
      index presence so we don't have to ssh into prod to check.
  #5  Failing enrich_catalog_product writes surface as rows in
      catalog_enrichment_errors AND as a log.warning — not just one.
      The table is trim-on-insert capped at ENRICHMENT_ERRORS_KEEP.
"""
import logging
import sqlite3

import pytest

from src.agents.product_catalog import (
    init_catalog_db, _get_conn, enrich_catalog_product,
    _record_enrich_error, ENRICHMENT_ERRORS_KEEP,
)
from src.api.modules.routes_health import _build_catalog_health


@pytest.fixture
def clean_catalog():
    init_catalog_db()
    conn = _get_conn()
    conn.execute("DELETE FROM product_catalog")
    conn.execute("DELETE FROM catalog_enrichment_errors")
    conn.commit()
    conn.close()
    yield


class TestCatalogHealthBuilder:
    def test_reports_index_presence(self, clean_catalog):
        h = _build_catalog_health()
        assert h["upc_column"] is True
        assert h["upc_index"] is True
        assert h["unique_name_index"] is True

    def test_empty_enrichment_errors_reports_zero(self, clean_catalog):
        h = _build_catalog_health()
        assert h["enrichment_errors_24h"] == 0
        assert h["recent_enrichment_errors"] == []


class TestEnrichmentErrorRecording:
    def test_failure_writes_a_row_and_logs(self, clean_catalog, caplog, monkeypatch):
        """Force every UPDATE to raise, verify the error lands in the table."""
        import src.agents.product_catalog as pc_mod

        class _BoomConn:
            total_changes = 0
            def execute(self, *a, **kw): raise RuntimeError("boom-enrich")
            def commit(self): pass
            def close(self): pass

        orig_init = pc_mod.init_catalog_db
        monkeypatch.setattr(pc_mod, "init_catalog_db", lambda: None)

        # Patch _get_conn to return boom when enrich runs, but NOT when
        # _record_enrich_error runs (it uses the real DB for writes).
        real_get_conn = pc_mod._get_conn
        state = {"enrich_calls": 0}
        def _conn_factory():
            state["enrich_calls"] += 1
            # First 3 calls (the 3 UPDATE paths) get the boom connection
            if state["enrich_calls"] <= 3:
                return _BoomConn()
            return real_get_conn()
        monkeypatch.setattr(pc_mod, "_get_conn", _conn_factory)

        with caplog.at_level(logging.WARNING, logger="reytech.product_catalog"):
            enrich_catalog_product(42, upc="012345", best_cost=5.0,
                                   photo_url="http://x/y.jpg")

        # Restore
        monkeypatch.setattr(pc_mod, "init_catalog_db", orig_init)
        monkeypatch.setattr(pc_mod, "_get_conn", real_get_conn)

        # Row made it to the table
        conn = _get_conn()
        rows = conn.execute(
            "SELECT product_id, column FROM catalog_enrichment_errors "
            "WHERE product_id=42"
        ).fetchall()
        conn.close()
        assert len(rows) >= 1
        # WARNING log fired
        assert any("enrich_catalog_product" in r.getMessage()
                   for r in caplog.records if r.levelno >= logging.WARNING)

    def test_trim_keeps_table_bounded(self, clean_catalog):
        """After 600 inserts, row count stays at or below ENRICHMENT_ERRORS_KEEP."""
        for i in range(ENRICHMENT_ERRORS_KEEP + 100):
            _record_enrich_error(i, "upc", f"val-{i}", f"err-{i}")

        conn = _get_conn()
        n = conn.execute("SELECT COUNT(*) FROM catalog_enrichment_errors").fetchone()[0]
        conn.close()
        assert n <= ENRICHMENT_ERRORS_KEEP + 1, (
            f"trim-on-insert must cap rows; got {n} with keep={ENRICHMENT_ERRORS_KEEP}"
        )

    def test_build_catalog_health_counts_recent_errors(self, clean_catalog):
        _record_enrich_error(99, "photo_url", "http://x", "network")
        _record_enrich_error(99, "best_cost", 3.5, "bad-cast")
        h = _build_catalog_health()
        assert h["enrichment_errors_24h"] >= 2
        assert len(h["recent_enrichment_errors"]) >= 2
        assert h["recent_enrichment_errors"][0]["product_id"] == 99


class TestSmokeMinScoreFlag:
    def test_flag_parses_and_sets_threshold(self):
        """Smoke harness must accept --min-score; this validates the arg plumbing."""
        import os, subprocess, sys
        env = {**os.environ, "DASH_USER": "x", "DASH_PASS": "y"}
        result = subprocess.run(
            [sys.executable, "tests/smoke_test.py", "--help"],
            capture_output=True, text=True, timeout=30, env=env,
        )
        assert "--min-score" in result.stdout, (
            f"smoke_test.py must expose --min-score; stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )
