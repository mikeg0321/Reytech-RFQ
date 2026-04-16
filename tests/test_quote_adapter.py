"""Tests for the Quote Model Adapter (feature-flagged dict→Quote→dict wrapper)."""
import pytest


class TestAdapterDisabled:
    """When flag is off, adapter is a passthrough."""

    def test_pc_passthrough(self, sample_pc):
        from src.core.quote_adapter import adapt_pc
        result = adapt_pc(sample_pc, "test-pc-001")
        assert result["id"] == "test-pc-001"
        assert len(result.get("items", [])) == len(sample_pc.get("items", []))

    def test_rfq_passthrough(self, sample_rfq):
        from src.core.quote_adapter import adapt_rfq
        result = adapt_rfq(sample_rfq, "test-rfq-001")
        assert result["id"] == "test-rfq-001"

    def test_none_input(self):
        from src.core.quote_adapter import adapt_pc
        assert adapt_pc(None) is None
        assert adapt_pc({}) == {}


class TestAdapterEnabled:
    """When flag is on, adapter round-trips through Quote model."""

    @pytest.fixture(autouse=True)
    def enable_flag(self):
        from src.core.flags import set_flag, _cache_clear_all
        set_flag("quote_model_v2_enabled", "true")
        _cache_clear_all()
        yield
        set_flag("quote_model_v2_enabled", "false")
        _cache_clear_all()

    def test_pc_round_trip(self, sample_pc):
        from src.core.quote_adapter import adapt_pc
        result = adapt_pc(sample_pc, "test-pc-001")
        assert result["id"] == "test-pc-001"
        assert result["institution"] == "CSP-Sacramento"
        assert len(result.get("items", [])) == len(sample_pc.get("items", []))
        assert len(result.get("line_items", [])) == len(sample_pc.get("items", []))

    def test_rfq_round_trip(self, sample_rfq):
        from src.core.quote_adapter import adapt_rfq
        result = adapt_rfq(sample_rfq, "test-rfq-001")
        assert result["id"] == "test-rfq-001"
        assert result["solicitation_number"] == "RFQ-2026-TEST"
        assert result["due_date"] == "03/15/2026"

    def test_pc_pricing_preserved(self, sample_pc):
        from src.core.quote_adapter import adapt_pc
        result = adapt_pc(sample_pc, "test-pc-001")
        items = result.get("items") or result.get("line_items") or []
        assert len(items) == 2
        # First item had supplier_cost=12.58
        assert items[0]["supplier_cost"] == 12.58 or items[0].get("unit_cost") == 12.58

    def test_adapter_survives_bad_data(self):
        from src.core.quote_adapter import adapt_pc
        # Malformed dict — adapter should not crash
        bad = {"id": "bad", "items": "not_a_list", "status": "weird_status"}
        result = adapt_pc(bad, "bad")
        # Should return something (either adapted or fallback)
        assert isinstance(result, dict)


class TestAdapterInRoutes:
    """Integration: adapter wired into route handlers."""

    def test_pc_detail_loads(self, auth_client, seed_pc):
        resp = auth_client.get(f"/pricecheck/{seed_pc}")
        assert resp.status_code == 200

    def test_rfq_detail_loads(self, auth_client, seed_rfq):
        resp = auth_client.get(f"/rfq/{seed_rfq}")
        assert resp.status_code == 200
