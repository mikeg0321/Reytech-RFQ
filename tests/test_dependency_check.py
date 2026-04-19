"""Tests for src/core/dependency_check.py — the SkipReason foundation.

This is PR #181 of the silent-skip rollout. The contract:

  * SkipReason carries (name, reason, severity, where) — enough for the
    orchestrator to route into blockers/warnings/notes AND for the
    feature_status table to count occurrences across runs.
  * try_import / try_env return (value, skip_or_none). Caller sees a real
    SkipReason with the failure attached — no silent empty fallback.
  * safe_call wraps any callable; converts exceptions to SkipReason rather
    than swallowing them with `log.debug` and returning empty.

Why test-first: every consumer PR (#183-#190) calls into these helpers, so
the API has to be locked down before we start migrating.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.core.dependency_check import (
    DependencyMissing,
    Severity,
    SkipReason,
    safe_call,
    try_env,
    try_import,
)


class TestSkipReason:
    def test_carries_all_four_fields(self):
        s = SkipReason(
            name="ANTHROPIC_API_KEY",
            reason="env var unset",
            severity=Severity.WARNING,
            where="compliance_validator._run_llm_gap_check",
        )
        assert s.name == "ANTHROPIC_API_KEY"
        assert s.reason == "env var unset"
        assert s.severity is Severity.WARNING
        assert s.where == "compliance_validator._run_llm_gap_check"

    def test_severity_enum_has_three_levels(self):
        # BLOCKER → orchestrator refuses to advance
        # WARNING → operator-visible degraded feature
        # INFO    → informational, lands in result.notes
        assert {Severity.BLOCKER, Severity.WARNING, Severity.INFO} == set(Severity)

    def test_format_for_log_is_human_readable(self):
        s = SkipReason(
            name="agency_config",
            reason="ImportError: No module named 'src.core.agency_config'",
            severity=Severity.BLOCKER,
            where="compliance_validator._check_required_forms",
        )
        msg = s.format_for_log()
        assert "agency_config" in msg
        assert "BLOCKER" in msg
        assert "compliance_validator._check_required_forms" in msg
        assert "ImportError" in msg


class TestTryImport:
    def test_success_returns_module_and_none_skip(self):
        mod, skip = try_import("os", where="test")
        assert mod is os
        assert skip is None

    def test_failure_returns_none_module_and_skip_reason(self):
        mod, skip = try_import(
            "src.does.not.exist",
            severity=Severity.BLOCKER,
            where="test_dependency_check",
        )
        assert mod is None
        assert isinstance(skip, SkipReason)
        assert skip.name == "src.does.not.exist"
        assert skip.severity is Severity.BLOCKER
        assert skip.where == "test_dependency_check"
        assert "import" in skip.reason.lower() or "no module" in skip.reason.lower()

    def test_default_severity_is_blocker_for_imports(self):
        """An import failure of a named dependency is almost always
        configuration-level — operator needs to see it as a blocker by
        default. Callers can downgrade to WARNING if the dep is optional."""
        _, skip = try_import("src.does.not.exist", where="test")
        assert skip.severity is Severity.BLOCKER


class TestTryEnv:
    def test_success_returns_value_and_none_skip(self):
        with patch.dict(os.environ, {"TEST_VAR_PRESENT": "hello"}):
            val, skip = try_env("TEST_VAR_PRESENT", where="test")
        assert val == "hello"
        assert skip is None

    def test_missing_returns_none_value_and_skip(self):
        # Make sure the env var is unset
        with patch.dict(os.environ, {}, clear=True):
            val, skip = try_env(
                "TEST_VAR_ABSENT",
                severity=Severity.WARNING,
                where="test_dependency_check",
            )
        assert val is None
        assert isinstance(skip, SkipReason)
        assert skip.name == "TEST_VAR_ABSENT"
        assert skip.severity is Severity.WARNING
        assert "unset" in skip.reason.lower() or "not set" in skip.reason.lower()

    def test_empty_string_treated_as_missing(self):
        """An env var set to "" is the same as unset — common in .env files."""
        with patch.dict(os.environ, {"TEST_VAR_EMPTY": ""}):
            val, skip = try_env("TEST_VAR_EMPTY", where="test")
        assert val is None
        assert skip is not None

    def test_default_severity_is_warning(self):
        """Missing env vars usually mean a feature is degraded, not that the
        whole quote pipeline must refuse to advance."""
        with patch.dict(os.environ, {}, clear=True):
            _, skip = try_env("ANY_MISSING_VAR", where="test")
        assert skip.severity is Severity.WARNING


class TestSafeCall:
    def test_success_returns_value_and_none_skip(self):
        value, skip = safe_call("compute", lambda: 42, where="test")
        assert value == 42
        assert skip is None

    def test_exception_returns_none_value_and_skip(self):
        def boom():
            raise RuntimeError("DB connection timeout")

        value, skip = safe_call(
            "load_oracle_history",
            boom,
            severity=Severity.WARNING,
            where="pricing_oracle_v2._search_won_quotes",
        )
        assert value is None
        assert isinstance(skip, SkipReason)
        assert skip.name == "load_oracle_history"
        assert skip.where == "pricing_oracle_v2._search_won_quotes"
        assert "RuntimeError" in skip.reason
        assert "DB connection timeout" in skip.reason

    def test_passes_args_and_kwargs(self):
        def add(a, b, *, factor=1):
            return (a + b) * factor

        value, skip = safe_call("add", add, 2, 3, factor=4, where="test")
        assert value == 20
        assert skip is None

    def test_default_severity_is_warning(self):
        def boom():
            raise ValueError("bad data")

        _, skip = safe_call("decode", boom, where="test")
        assert skip.severity is Severity.WARNING

    def test_logs_at_appropriate_level(self, caplog):
        """A swallowed exception must hit the standard log channel — not
        log.debug. The whole point of this rollout is killing buried
        debug-only swallows that operators never see."""
        import logging

        def boom():
            raise RuntimeError("simulated failure")

        with caplog.at_level(logging.WARNING):
            safe_call("test_op", boom, severity=Severity.WARNING, where="test")
        assert any(
            "test_op" in rec.message and "simulated failure" in rec.message
            for rec in caplog.records
        ), [r.message for r in caplog.records]


class TestDependencyMissingException:
    def test_carries_skip_reason(self):
        """For call sites that prefer raise/catch over (value, skip) tuples,
        DependencyMissing wraps a SkipReason."""
        s = SkipReason(name="x", reason="y", severity=Severity.BLOCKER, where="z")
        e = DependencyMissing(s)
        assert e.skip is s
        assert "x" in str(e) and "BLOCKER" in str(e)
