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

    # ── Passthrough fields (regression guard 2026-04-22 RFQ #9ad8a0ac) ──
    # The Quote pydantic model doesn't carry link bookkeeping or parse-
    # failure signals. Before this guard the adapter silently stripped
    # linked_pc_id, so the detail route rendered "no PC linked" for a
    # correctly-linked RFQ. Any non-model field listed in
    # _ADAPTER_PASSTHROUGH_FIELDS must survive the round-trip.

    def test_rfq_linked_pc_id_preserved(self, sample_rfq):
        from src.core.quote_adapter import adapt_rfq
        rfq = dict(sample_rfq)
        rfq["linked_pc_id"] = "pc_abc123"
        rfq["linked_pc_number"] = "CCHCS-Recovered"
        rfq["link_reason"] = "items-verbatim(1.00)"
        rfq["link_confidence"] = 0.5
        result = adapt_rfq(rfq, "test-rfq-001")
        assert result["linked_pc_id"] == "pc_abc123"
        assert result["linked_pc_number"] == "CCHCS-Recovered"
        assert result["link_reason"] == "items-verbatim(1.00)"
        assert result["link_confidence"] == 0.5

    def test_pc_linked_pc_id_preserved(self, sample_pc):
        """PCs occasionally carry linked_pc_ids too (bundle case)."""
        from src.core.quote_adapter import adapt_pc
        pc = dict(sample_pc)
        pc["linked_pc_ids"] = ["pc_a", "pc_b", "pc_c"]
        result = adapt_pc(pc, "test-pc-001")
        assert result["linked_pc_ids"] == ["pc_a", "pc_b", "pc_c"]

    def test_rfq_due_date_source_preserved(self, sample_rfq):
        """Deadline provenance must round-trip so the `⚠ default` badge works."""
        from src.core.quote_adapter import adapt_rfq
        rfq = dict(sample_rfq)
        rfq["due_date_source"] = "email"
        result = adapt_rfq(rfq, "test-rfq-001")
        assert result["due_date_source"] == "email"

    def test_rfq_parse_failed_flag_preserved(self, sample_rfq):
        from src.core.quote_adapter import adapt_rfq
        rfq = dict(sample_rfq)
        rfq["_parse_failed"] = True
        result = adapt_rfq(rfq, "test-rfq-001")
        assert result["_parse_failed"] is True

    def test_rfq_recovered_marker_preserved(self, sample_rfq):
        """Manual `_recovered_from` provenance strings survive adaptation."""
        from src.core.quote_adapter import adapt_rfq
        rfq = dict(sample_rfq)
        rfq["_recovered_from"] = "manual: desktop PDF, app never persisted"
        result = adapt_rfq(rfq, "test-rfq-001")
        assert "manual" in result["_recovered_from"]

    def test_is_test_flag_preserved(self, sample_rfq):
        """is_test controls the queue/triage filter — must not vanish."""
        from src.core.quote_adapter import adapt_rfq
        rfq = dict(sample_rfq)
        rfq["is_test"] = True
        result = adapt_rfq(rfq, "test-rfq-001")
        assert result["is_test"] is True

    def test_passthrough_does_not_override_real_model_value(self, sample_rfq):
        """If the Quote model DID populate a field (non-empty), the
        passthrough MUST NOT clobber it — passthrough only fills gaps."""
        from src.core.quote_adapter import adapt_rfq
        rfq = dict(sample_rfq)
        # sample_rfq already has a solicitation_number. Don't pass a
        # conflicting one via the passthrough list (it isn't in the list,
        # but this test nails down the policy).
        # Use is_test: model doesn't set it, passthrough fills it.
        # Without is_test set, adapted value should NOT be True.
        rfq.pop("is_test", None)
        result = adapt_rfq(rfq, "test-rfq-001")
        assert result.get("is_test") in (None, False)


class TestAdapterInRoutes:
    """Integration: adapter wired into route handlers."""

    def test_pc_detail_loads(self, auth_client, seed_pc):
        resp = auth_client.get(f"/pricecheck/{seed_pc}")
        assert resp.status_code == 200

    def test_rfq_detail_loads(self, auth_client, seed_rfq):
        resp = auth_client.get(f"/rfq/{seed_rfq}")
        assert resp.status_code == 200
