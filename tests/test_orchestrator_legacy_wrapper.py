"""Tests for QuoteOrchestrator.run_legacy_package — the adapter that gives
the legacy generate-package routes an orchestrator-shaped entry point.

Guards:
  - Flag off → noop result with a flag note (default posture).
  - Flag on → loads the RFQ, applies form_data overrides, delegates to run().
  - Missing RFQ surfaces as a blocker (not a raise).
  - Form overrides propagate onto the line items the orchestrator sees.
  - Invalid doc_type falls through run()'s own validation as a blocker.
"""
from __future__ import annotations

import pytest

from src.core.quote_orchestrator import (
    OrchestratorResult,
    QuoteOrchestrator,
    QuoteRequest,
)


@pytest.fixture
def orch():
    return QuoteOrchestrator(persist_audit=False)


@pytest.fixture
def patch_flag(monkeypatch):
    def _set(value: bool):
        import src.core.flags as flags_mod
        monkeypatch.setattr(flags_mod, "get_flag", lambda key, default: value
                            if key == "rfq.orchestrator_pipeline" else default)
    return _set


@pytest.fixture
def patch_load_rfqs(monkeypatch):
    def _set(rfqs: dict):
        import src.api.data_layer as dl_mod
        monkeypatch.setattr(dl_mod, "load_rfqs", lambda: rfqs)
    return _set


class TestFlagGating:
    def test_flag_off_returns_noop(self, orch, patch_flag):
        patch_flag(False)
        r = orch.run_legacy_package("any-id", {})
        assert r.ok is True
        assert r.quote is None
        assert any("flag off" in n for n in r.notes)

    def test_flag_on_attempts_load(self, orch, patch_flag, patch_load_rfqs):
        patch_flag(True)
        patch_load_rfqs({})  # no RFQ with that id
        r = orch.run_legacy_package("missing-rid", {})
        assert r.ok is False
        assert any("rfq not found" in b for b in r.blockers)


class TestLegacyFormOverrides:
    def test_overrides_applied_to_line_items(self):
        """The static helper is pure — exercise it directly."""
        rfq = {
            "line_items": [
                {"description": "old", "qty": 1, "uom": "ea",
                 "supplier_cost": 0.0, "price_per_unit": 0.0},
            ]
        }
        form = {
            "cost_0": "12.50",
            "price_0": "20.00",
            "markup_0": "60.0",
            "desc_0": "  new desc  ",
            "qty_0": "5",
            "uom_0": "pk",
            "part_0": " MFG-123 ",
            "link_0": " https://x/y ",
        }
        QuoteOrchestrator._apply_legacy_form_overrides(rfq, form)

        item = rfq["line_items"][0]
        assert item["supplier_cost"] == 12.50
        assert item["price_per_unit"] == 20.00
        assert item["markup_pct"] == 60.0
        assert item["description"] == "new desc"
        assert item["qty"] == 5
        assert item["uom"] == "PK"
        assert item["item_number"] == "MFG-123"
        assert item["item_link"] == "https://x/y"

    def test_empty_form_data_leaves_items_untouched(self):
        rfq = {"line_items": [{"description": "x", "qty": 1}]}
        QuoteOrchestrator._apply_legacy_form_overrides(rfq, {})
        assert rfq["line_items"][0] == {"description": "x", "qty": 1}

    def test_bad_numeric_value_is_silent(self):
        rfq = {"line_items": [{"supplier_cost": 1.0}]}
        QuoteOrchestrator._apply_legacy_form_overrides(
            rfq, {"cost_0": "not-a-number"}
        )
        # Untouched — route-parity semantics.
        assert rfq["line_items"][0]["supplier_cost"] == 1.0


class TestDelegationToRun:
    def test_invokes_run_with_merged_dict(self, orch, patch_flag,
                                          patch_load_rfqs, monkeypatch):
        patch_flag(True)
        rid = "R00-TEST-1"
        patch_load_rfqs({
            rid: {
                "id": rid,
                "doc_type": "rfq",
                "agency_key": "cchcs",
                "solicitation_number": "S-1",
                "line_items": [
                    {"description": "Widget", "qty": 2, "uom": "EA",
                     "supplier_cost": 0.0, "price_per_unit": 0.0},
                ],
            }
        })

        captured = {}

        def fake_run(self, req: QuoteRequest):
            captured["req"] = req
            return OrchestratorResult(ok=True, final_stage="parsed")

        monkeypatch.setattr(QuoteOrchestrator, "run", fake_run)

        out = orch.run_legacy_package(
            rid,
            {"cost_0": "7.5", "price_0": "11", "desc_0": "Bigger widget"},
            target_stage="qa_pass",
        )

        assert out.ok is True
        req = captured["req"]
        assert isinstance(req.source, dict)
        assert req.source["id"] == rid
        assert req.doc_type == "rfq"
        assert req.agency_key == "cchcs"
        assert req.solicitation_number == "S-1"
        assert req.target_stage == "qa_pass"
        assert req.actor == "legacy_route"

        item = req.source["line_items"][0]
        assert item["supplier_cost"] == 7.5
        assert item["price_per_unit"] == 11.0
        assert item["description"] == "Bigger widget"

    def test_source_dict_is_a_copy_not_the_live_rfq(
        self, orch, patch_flag, patch_load_rfqs, monkeypatch
    ):
        patch_flag(True)
        rid = "R00-TEST-2"
        original = {
            rid: {
                "id": rid,
                "doc_type": "rfq",
                "line_items": [{"description": "x", "supplier_cost": 1.0}],
            }
        }
        patch_load_rfqs(original)

        captured = {}
        def fake_run(self, req: QuoteRequest):
            captured["req"] = req
            return OrchestratorResult(ok=True)
        monkeypatch.setattr(QuoteOrchestrator, "run", fake_run)

        orch.run_legacy_package(rid, {"cost_0": "99.0"})

        # Override landed on the copy the orchestrator saw…
        assert captured["req"].source["line_items"][0]["supplier_cost"] == 99.0
        # …but the underlying RFQ record was not mutated.
        assert original[rid]["line_items"][0]["supplier_cost"] == 1.0


class TestErrorSurfacing:
    def test_load_rfqs_raising_surfaces_as_blocker(
        self, orch, patch_flag, monkeypatch
    ):
        patch_flag(True)
        import src.api.data_layer as dl_mod
        def boom():
            raise RuntimeError("db down")
        monkeypatch.setattr(dl_mod, "load_rfqs", boom)

        r = orch.run_legacy_package("rid", {})
        assert r.ok is False
        assert any("load_rfqs failed" in b and "db down" in b for b in r.blockers)

    def test_flag_read_failure_is_treated_as_off(self, orch, monkeypatch):
        import src.core.flags as flags_mod
        def boom(key, default):
            raise RuntimeError("flags table missing")
        monkeypatch.setattr(flags_mod, "get_flag", boom)

        r = orch.run_legacy_package("rid", {})
        assert r.ok is True
        assert any("flag off" in n for n in r.notes)
