"""Tests for pricing oracle V2 migration.

Verifies that V2 is correctly wired into all call sites,
returns expected shapes, and the calibration feedback loop works.
"""
import os
import sqlite3
import threading
import pytest


# ── Helper to get a test DB connection ──────────────────────────────────────

def _get_test_db_path():
    """Get the DB_PATH that conftest patched."""
    try:
        from src.core.db import DB_PATH
        return DB_PATH
    except ImportError:
        return None


# ── Schema tests ────────────────────────────────────────────────────────────

class TestOracleSchema:
    """Verify oracle tables exist in the canonical schema."""

    def test_oracle_calibration_table_exists(self, temp_data_dir):
        """oracle_calibration should be created by init_db via SCHEMA."""
        from src.core.db import init_db, DB_PATH
        init_db()
        conn = sqlite3.connect(DB_PATH, timeout=5)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "oracle_calibration" in tables

    def test_winning_prices_table_exists(self, temp_data_dir):
        """winning_prices should be created by init_db via SCHEMA."""
        from src.core.db import init_db, DB_PATH
        init_db()
        conn = sqlite3.connect(DB_PATH, timeout=5)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "winning_prices" in tables

    def test_winning_prices_indexes_exist(self, temp_data_dir):
        """winning_prices indexes should be created by schema."""
        from src.core.db import init_db, DB_PATH
        init_db()
        conn = sqlite3.connect(DB_PATH, timeout=5)
        indexes = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()]
        conn.close()
        assert "idx_wp_fingerprint" in indexes
        assert "idx_wp_part" in indexes


# ── V2 get_pricing shape tests ──────────────────────────────────────────────

class TestV2PricingShape:
    """Verify get_pricing returns expected structure."""

    def test_get_pricing_returns_dict(self, temp_data_dir):
        """get_pricing should return a dict with recommendation key."""
        from src.core.db import init_db
        init_db()
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(description="Nitrile Exam Gloves Large", quantity=1)
        assert isinstance(result, dict)
        assert "recommendation" in result
        assert "market" in result

    def test_recommendation_has_quote_price(self, temp_data_dir):
        """recommendation.quote_price should be a number."""
        from src.core.db import init_db
        init_db()
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(
            description="Nitrile Exam Gloves Large",
            quantity=1,
            cost=15.00,
        )
        rec = result.get("recommendation", {})
        if rec.get("quote_price") is not None:
            assert isinstance(rec["quote_price"], (int, float))
            assert rec["quote_price"] > 0

    def test_recommendation_has_confidence(self, temp_data_dir):
        """recommendation.confidence should be low/medium/high."""
        from src.core.db import init_db
        init_db()
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(description="Toilet Paper 2-ply", quantity=1, cost=5.00)
        rec = result.get("recommendation", {})
        if rec.get("confidence"):
            assert rec["confidence"] in ("low", "medium", "high")

    def test_source_counts_present(self, temp_data_dir):
        """source_counts should be a dict (may be empty with no data)."""
        from src.core.db import init_db
        init_db()
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(description="Bandage Wrap 4 inch", quantity=1)
        sc = result.get("source_counts")
        assert sc is None or isinstance(sc, dict)


# ── Calibration tests ───────────────────────────────────────────────────────

class TestCalibration:
    """Verify the feedback loop writes calibration data."""

    def test_calibrate_from_win(self, temp_data_dir):
        """calibrate_from_outcome('won') should write to oracle_calibration."""
        from src.core.db import init_db, DB_PATH
        init_db()
        from src.core.pricing_oracle_v2 import calibrate_from_outcome

        items = [
            {"description": "Nitrile Gloves Large", "pricing": {"unit_price": 18.50}},
            {"description": "Face Masks N95", "pricing": {"unit_price": 12.00}},
        ]
        calibrate_from_outcome(items, "won", agency="CDCR")

        conn = sqlite3.connect(DB_PATH, timeout=5)
        rows = conn.execute("SELECT * FROM oracle_calibration").fetchall()
        conn.close()
        assert len(rows) > 0, "calibrate_from_outcome should have written at least one row"

    def test_calibrate_from_loss(self, temp_data_dir):
        """calibrate_from_outcome('lost') should update loss counts."""
        from src.core.db import init_db, DB_PATH
        init_db()
        from src.core.pricing_oracle_v2 import calibrate_from_outcome

        items = [
            {"description": "Paper Towels Bulk", "pricing": {"unit_price": 25.00}},
        ]
        calibrate_from_outcome(items, "lost", agency="CCHCS", loss_reason="price")

        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT loss_on_price FROM oracle_calibration WHERE agency='CCHCS' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] > 0, "loss_on_price should be incremented"


# ── Thread safety ───────────────────────────────────────────────────────────

class TestThreadSafety:
    """Verify get_pricing works from multiple threads."""

    def test_concurrent_pricing_calls(self, temp_data_dir):
        """5 concurrent get_pricing calls should all succeed."""
        from src.core.db import init_db
        init_db()
        from src.core.pricing_oracle_v2 import get_pricing

        results = [None] * 5
        errors = [None] * 5
        descriptions = [
            "Nitrile Gloves", "Face Masks", "Hand Sanitizer",
            "Toilet Paper", "Paper Towels"
        ]

        def worker(idx):
            try:
                results[idx] = get_pricing(
                    description=descriptions[idx], quantity=1, cost=10.0
                )
            except Exception as e:
                errors[idx] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        for i in range(5):
            assert errors[i] is None, f"Thread {i} failed: {errors[i]}"
            assert results[i] is not None, f"Thread {i} returned None"
            assert isinstance(results[i], dict), f"Thread {i} didn't return dict"


# ── Enrichment pipeline V1 removal ──────────────────────────────────────────

class TestEnrichmentPipelineV1Removed:
    """Verify the enrichment pipeline no longer imports V1."""

    def test_no_v1_import_in_enrichment(self):
        """pc_enrichment_pipeline.py should not import from pricing_oracle V1."""
        import ast
        pipeline_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "agents", "pc_enrichment_pipeline.py"
        )
        with open(pipeline_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert "knowledge.pricing_oracle" not in node.module, \
                        f"V1 pricing_oracle still imported at line {node.lineno}"


# ── price_check.py V1 removal ──────────────────────────────────────────────

class TestPriceCheckV1Removed:
    """Verify price_check.py no longer imports V1 at module level."""

    def test_no_v1_module_import_in_price_check(self):
        """price_check.py should not have module-level V1 import."""
        import ast
        pc_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "forms", "price_check.py"
        )
        with open(pc_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if "knowledge.pricing_oracle" in node.module:
                    if node.col_offset < 4:
                        pytest.fail(
                            f"Module-level V1 import at line {node.lineno}: {node.module}"
                        )
