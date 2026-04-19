"""Tests that QuoteOrchestrator drains module-level skip ledgers and
persists them into feature_status (PR #188).

The orchestrator is the single seam where every quoting flow ends up.
Wiring it to drain the per-module ledgers (item_link_lookup,
agency_config, pricing_oracle_v2, award_tracker, db) means the dashboard
banner stays current without each route having to remember to drain.

The drain happens in `run()`'s try/finally so EVERY return path
(success, blocked, crashed) feeds feature_status. Degraded features
don't become healthy because a single run failed.
"""
from __future__ import annotations

import pytest

from src.core import (
    feature_status,
    pricing_oracle_v2,
    agency_config,
    db as core_db,
)
from src.agents import item_link_lookup, award_tracker
from src.core.dependency_check import Severity, SkipReason
from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest


def _drain_all_safely():
    """Defensive drain — used in setup AND teardown. Tests may have
    monkeypatched a drain to raise; we don't want that to fail the
    fixture itself."""
    for mod in (item_link_lookup, agency_config, pricing_oracle_v2,
                award_tracker, core_db):
        try:
            mod.drain_skips()
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _isolate_feature_status_db(tmp_path, monkeypatch):
    """Each test gets its own feature_status DB so the table is empty."""
    monkeypatch.setattr(
        feature_status, "_DB_PATH_OVERRIDE",
        str(tmp_path / "feature_status_wiring.db"),
    )
    _drain_all_safely()
    yield
    _drain_all_safely()


class TestSweepDrainsAllModules:
    def test_sweep_drains_item_link_lookup(self):
        item_link_lookup._record_skip(SkipReason(
            name="ANTHROPIC_API_KEY",
            reason="env var unset",
            severity=Severity.WARNING,
            where="claude_amazon_lookup",
        ))
        orch = QuoteOrchestrator(persist_audit=False)
        # Use a synthetic OrchestratorResult so we don't have to spin up a
        # real run; sweep is the unit under test.
        from src.core.quote_orchestrator import OrchestratorResult
        result = OrchestratorResult()
        orch._sweep_module_skips(result)
        # The skip was drained from the module and routed into result.skips.
        names = [s.name for s in result.skips]
        assert "ANTHROPIC_API_KEY" in names, names
        # Module ledger is now empty.
        assert item_link_lookup.drain_skips() == []

    def test_sweep_drains_pricing_oracle(self):
        pricing_oracle_v2._record_skip(SkipReason(
            name="won_quotes",
            reason="OperationalError: no such table",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_won_quotes",
        ))
        orch = QuoteOrchestrator(persist_audit=False)
        from src.core.quote_orchestrator import OrchestratorResult
        result = OrchestratorResult()
        orch._sweep_module_skips(result)
        assert any(s.name == "won_quotes" for s in result.skips)

    def test_sweep_drains_all_five_modules_in_one_call(self):
        item_link_lookup._record_skip(SkipReason(
            name="ANTHROPIC_API_KEY", reason="r", severity=Severity.WARNING,
            where="ill",
        ))
        agency_config._record_skip(SkipReason(
            name="agency_package_configs", reason="r", severity=Severity.WARNING,
            where="ac",
        ))
        pricing_oracle_v2._record_skip(SkipReason(
            name="won_quotes", reason="r", severity=Severity.WARNING,
            where="po",
        ))
        award_tracker._record_skip(SkipReason(
            name="line_items_json", reason="r", severity=Severity.INFO,
            where="at",
        ))
        core_db._record_skip(SkipReason(
            name="items_detail_json", reason="r", severity=Severity.INFO,
            where="db",
        ))
        orch = QuoteOrchestrator(persist_audit=False)
        from src.core.quote_orchestrator import OrchestratorResult
        result = OrchestratorResult()
        orch._sweep_module_skips(result)
        names = sorted(s.name for s in result.skips)
        assert names == [
            "ANTHROPIC_API_KEY", "agency_package_configs",
            "items_detail_json", "line_items_json", "won_quotes",
        ], names


class TestSweepPersistsToFeatureStatus:
    def test_drained_skips_appear_in_feature_status_table(self):
        pricing_oracle_v2._record_skip(SkipReason(
            name="won_quotes",
            reason="OperationalError: no such table",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_won_quotes",
        ))
        orch = QuoteOrchestrator(persist_audit=False)
        from src.core.quote_orchestrator import OrchestratorResult
        result = OrchestratorResult()
        orch._sweep_module_skips(result)

        rows = feature_status.current_status()
        assert any(
            r["name"] == "won_quotes"
            and r["severity"] == "warning"
            and "_search_won_quotes" in r["where"]
            for r in rows
        ), rows

    def test_repeated_sweeps_increment_count_in_table(self):
        for _ in range(3):
            pricing_oracle_v2._record_skip(SkipReason(
                name="won_quotes", reason="r",
                severity=Severity.WARNING,
                where="pricing_oracle_v2._search_won_quotes",
            ))
            orch = QuoteOrchestrator(persist_audit=False)
            from src.core.quote_orchestrator import OrchestratorResult
            result = OrchestratorResult()
            orch._sweep_module_skips(result)

        rows = [r for r in feature_status.current_status() if r["name"] == "won_quotes"]
        assert len(rows) == 1
        assert rows[0]["count"] == 3


class TestSweepRobustness:
    def test_sweep_with_no_skips_is_noop(self):
        orch = QuoteOrchestrator(persist_audit=False)
        from src.core.quote_orchestrator import OrchestratorResult
        result = OrchestratorResult()
        orch._sweep_module_skips(result)  # must not raise
        assert result.skips == []
        assert feature_status.current_status() == []

    def test_sweep_swallows_module_drain_failure(self, monkeypatch):
        """If one module's drain crashes, the sweep must still process the
        others and persist what it has — observability is not load-bearing."""
        # Force item_link_lookup.drain_skips to raise.
        def _boom():
            raise RuntimeError("simulated drain crash")

        monkeypatch.setattr(item_link_lookup, "drain_skips", _boom)
        # Other module still has a real skip we expect to be persisted.
        pricing_oracle_v2._record_skip(SkipReason(
            name="won_quotes", reason="r",
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_won_quotes",
        ))
        orch = QuoteOrchestrator(persist_audit=False)
        from src.core.quote_orchestrator import OrchestratorResult
        result = OrchestratorResult()
        # Must not raise.
        orch._sweep_module_skips(result)
        # The non-crashed module's skip was persisted.
        rows = feature_status.current_status()
        assert any(r["name"] == "won_quotes" for r in rows), rows
