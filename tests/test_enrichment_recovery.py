"""
Tests for enrichment crash recovery — Phase 1 of architecture gap fixes.

Verifies that PCs stuck in 'enriching' state (from interrupted deploys)
are correctly reset to 'failed' on startup.
"""
import json
import pytest
import src.api.dashboard as _dash


def _clear_pc_cache():
    """Reset the in-memory PC cache so tests get fresh DB reads."""
    _dash._pc_cache = None
    _dash._pc_cache_time = 0


def _seed_pc(pc_id, enrichment_status="raw", enrichment_error=None):
    """Insert a PC directly into the test DB via _save_single_pc."""
    pc = {
        "id": pc_id,
        "enrichment_status": enrichment_status,
        "items": [],
        "status": "parsed",
    }
    if enrichment_error:
        pc["enrichment_error"] = enrichment_error
    _dash._save_single_pc(pc_id, pc)
    _clear_pc_cache()


def _get_pc(pc_id):
    """Read a PC from the test DB (cache-busted)."""
    _clear_pc_cache()
    pcs = _dash._load_price_checks()
    return pcs.get(pc_id)


class TestRecoverStuckEnrichments:
    """Test recover_stuck_enrichments() resets interrupted PCs."""

    def test_resets_enriching_to_failed(self, temp_data_dir):
        """PC stuck in 'enriching' should be reset to 'failed'."""
        _seed_pc("pc-stuck-001", enrichment_status="enriching")

        from src.agents.pc_enrichment_pipeline import recover_stuck_enrichments
        _clear_pc_cache()
        count = recover_stuck_enrichments()

        assert count == 1
        pc = _get_pc("pc-stuck-001")
        assert pc is not None, "PC not found in DB after recovery"
        assert pc["enrichment_status"] == "failed"
        assert pc["enrichment_error"] == "interrupted by deploy/restart"

    def test_leaves_complete_untouched(self, temp_data_dir):
        """PC with 'complete' status should not be modified."""
        _seed_pc("pc-done-001", enrichment_status="complete")

        from src.agents.pc_enrichment_pipeline import recover_stuck_enrichments
        _clear_pc_cache()
        count = recover_stuck_enrichments()

        assert count == 0
        pc = _get_pc("pc-done-001")
        assert pc is not None, "PC not found in DB"
        assert pc["enrichment_status"] == "complete"

    def test_leaves_failed_untouched(self, temp_data_dir):
        """PC already 'failed' should not be modified again."""
        _seed_pc("pc-fail-001", enrichment_status="failed",
                 enrichment_error="original error")

        from src.agents.pc_enrichment_pipeline import recover_stuck_enrichments
        _clear_pc_cache()
        count = recover_stuck_enrichments()

        assert count == 0
        pc = _get_pc("pc-fail-001")
        assert pc is not None, "PC not found in DB"
        assert pc["enrichment_error"] == "original error"

    def test_zero_pcs_returns_zero(self, temp_data_dir):
        """Empty DB returns 0."""
        from src.agents.pc_enrichment_pipeline import recover_stuck_enrichments
        _clear_pc_cache()
        count = recover_stuck_enrichments()

        assert count == 0

    def test_multiple_stuck_pcs(self, temp_data_dir):
        """Multiple stuck PCs should all be recovered."""
        _seed_pc("pc-a", enrichment_status="enriching")
        _seed_pc("pc-b", enrichment_status="enriching")
        _seed_pc("pc-c", enrichment_status="complete")

        from src.agents.pc_enrichment_pipeline import recover_stuck_enrichments
        _clear_pc_cache()
        count = recover_stuck_enrichments()

        assert count == 2
        assert _get_pc("pc-a")["enrichment_status"] == "failed"
        assert _get_pc("pc-b")["enrichment_status"] == "failed"
        assert _get_pc("pc-c")["enrichment_status"] == "complete"
