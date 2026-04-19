"""Tests for growth_agent._load_json corruption-vs-missing distinction (PR #190).

The 2026-04-18 silent-skip audit flagged 42 `_load_json(PATH) or []`
chains in growth_agent.py. The `or []` itself is fine — but the helper
it calls suppressed BOTH FileNotFoundError AND JSONDecodeError
identically, returning `[]` either way.

These two cases mean very different things:
  - FileNotFoundError: the prospects file hasn't been created yet
    (first-run state). Returning `[]` is correct and unremarkable.
  - JSONDecodeError: the file EXISTS but its contents are unparseable
    (truncated by a crashed writer, manual edit gone wrong, etc.).
    Returning `[]` here silently throws away the operator's data and
    the dashboard never knows. That's a real corruption signal.

PR #190 separates these. File-not-found stays silent. Parse failure
records an INFO skip into the module ledger so the orchestrator's
end-of-run sweep can persist it into feature_status — the dashboard
banner shows "prospects.json corrupted" until the operator fixes it.

The helper still returns `[]` so existing call sites keep working
(no caller-signature change). Observability is layered on top.
"""
from __future__ import annotations

import json

import pytest

from src.agents import growth_agent
from src.core.dependency_check import Severity


@pytest.fixture(autouse=True)
def _drain_between_tests():
    growth_agent.drain_skips()
    yield
    growth_agent.drain_skips()


class TestDrainContract:
    def test_drain_returns_list(self):
        assert growth_agent.drain_skips() == []

    def test_drain_clears_the_ledger(self):
        from src.core.dependency_check import SkipReason
        growth_agent._record_skip(SkipReason(
            name="x", reason="y", severity=Severity.INFO, where="z",
        ))
        assert len(growth_agent.drain_skips()) == 1
        assert growth_agent.drain_skips() == []


class TestFileNotFound:
    def test_missing_file_returns_empty_list_silently(self, tmp_path):
        """A nonexistent file is the first-run case — no skip emitted."""
        missing = tmp_path / "does_not_exist.json"
        out = growth_agent._load_json(str(missing))
        assert out == []
        assert growth_agent.drain_skips() == []


class TestCorruption:
    def test_truncated_json_emits_info_skip(self, tmp_path):
        """A truncated file (e.g. crashed writer left half a JSON blob)
        means the operator HAS data we just can't read. The dashboard
        must surface this so they know to investigate."""
        corrupt = tmp_path / "prospects.json"
        corrupt.write_text('[{"name": "Acme",')  # truncated mid-object
        out = growth_agent._load_json(str(corrupt))
        assert out == []  # caller signature preserved
        skips = growth_agent.drain_skips()
        assert any(
            s.name == "json_corruption"
            and s.severity == Severity.INFO
            and "prospects.json" in s.where
            for s in skips
        ), skips

    def test_garbage_content_emits_info_skip(self, tmp_path):
        """Random non-JSON content — same outcome as truncation."""
        corrupt = tmp_path / "history.json"
        corrupt.write_text("not json at all")
        out = growth_agent._load_json(str(corrupt))
        assert out == []
        skips = growth_agent.drain_skips()
        assert any(
            s.name == "json_corruption"
            and "history.json" in s.where
            for s in skips
        ), skips

    def test_skip_reason_includes_exception_detail(self, tmp_path):
        """The reason field must carry the underlying parse error so the
        operator can grep logs for the specific line/column that failed."""
        corrupt = tmp_path / "ab.json"
        corrupt.write_text("{,}")
        growth_agent._load_json(str(corrupt))
        skips = growth_agent.drain_skips()
        assert any("JSONDecodeError" in s.reason for s in skips), skips


class TestValidJson:
    def test_valid_list_returns_data_no_skip(self, tmp_path):
        good = tmp_path / "ok.json"
        good.write_text(json.dumps([{"a": 1}, {"b": 2}]))
        out = growth_agent._load_json(str(good))
        assert out == [{"a": 1}, {"b": 2}]
        assert growth_agent.drain_skips() == []

    def test_valid_dict_returns_data_no_skip(self, tmp_path):
        good = tmp_path / "ok.json"
        good.write_text(json.dumps({"k": "v"}))
        out = growth_agent._load_json(str(good))
        assert out == {"k": "v"}
        assert growth_agent.drain_skips() == []


class TestModuleRegisteredWithOrchestrator:
    """Like cchcs_packet_filler in PR #189, growth_agent must appear in
    the orchestrator's sweep tuple so its ledger drains into
    feature_status. Otherwise the dashboard banner stays blind to
    prospects-file corruption."""

    def test_growth_agent_in_sweep_modules(self):
        from src.core.quote_orchestrator import QuoteOrchestrator
        assert "src.agents.growth_agent" in QuoteOrchestrator._SKIP_LEDGER_MODULES
