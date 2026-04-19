"""Dependency-check primitives for the silent-skip rollout (PRs #181-#190).

The audit on 2026-04-18 found 127 instances of the silent-skip anti-pattern:
a function performs a check, validation, lookup, or external call; when
something *prevents* the check from running (import failure, missing API key,
DB timeout, missing input), the function silently returns an "OK-looking"
sentinel instead of surfacing that the check did not happen. Caller cannot
distinguish "ran and found nothing" from "did not run."

This module is the foundation. Consumers (PR #183 onward) replace their
silent fallbacks with `try_import / try_env / safe_call`, returning
`(value, list[SkipReason])` to their caller. The orchestrator (PR #182)
aggregates skips from every per-form report and routes them by severity:

    BLOCKER → result.blockers (qa_pass refuses to advance)
    WARNING → result.warnings (operator-visible degraded feature)
    INFO    → result.notes    (informational)

This feeds the existing 3-channel `OrchestratorResult` envelope rather than
inventing a parallel system.
"""
from __future__ import annotations

import importlib
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, Tuple

log = logging.getLogger("reytech.dependency_check")


class Severity(Enum):
    BLOCKER = "blocker"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class SkipReason:
    """One skip event. Frozen so consumers can stash it in dataclass collections
    or hash it into a counter for the feature_status dashboard banner."""
    name: str          # e.g. "ANTHROPIC_API_KEY", "src.core.agency_config"
    reason: str        # e.g. "env var unset", "ImportError: No module named ..."
    severity: Severity
    where: str         # e.g. "compliance_validator._check_required_forms"

    def format_for_log(self) -> str:
        return (
            f"[{self.severity.name}] {self.name} skipped at {self.where}: {self.reason}"
        )


class DependencyMissing(Exception):
    """Wraps a SkipReason for call sites that prefer raise/catch flow.

    The orchestrator's `_try_advance` already converts unhandled exceptions
    into `outcome="error"`, so raising this from inside a stage transition
    bubbles up cleanly. Most consumers should prefer the (value, skip) tuple
    form — explicit is better — but this is here for the cases where you
    want to short-circuit several layers of nested calls."""

    def __init__(self, skip: SkipReason):
        self.skip = skip
        super().__init__(skip.format_for_log())


def try_import(
    module_path: str,
    *,
    severity: Severity = Severity.BLOCKER,
    where: str = "",
) -> Tuple[Optional[Any], Optional[SkipReason]]:
    """Import `module_path`. Returns (module, None) on success, (None, skip)
    on any import failure.

    Default severity is BLOCKER because a missing core module is almost
    always a configuration/install error the operator must know about.
    Callers with a true optional dependency (anthropic SDK, twilio, etc.)
    should pass `severity=Severity.WARNING`.
    """
    try:
        mod = importlib.import_module(module_path)
        return mod, None
    except Exception as e:
        skip = SkipReason(
            name=module_path,
            reason=f"{type(e).__name__}: {e}",
            severity=severity,
            where=where,
        )
        log.warning(skip.format_for_log())
        return None, skip


def try_env(
    var_name: str,
    *,
    severity: Severity = Severity.WARNING,
    where: str = "",
) -> Tuple[Optional[str], Optional[SkipReason]]:
    """Read `os.environ[var_name]`. Returns (value, None) on success,
    (None, skip) when the var is unset or empty.

    Empty string is treated as missing — common in `.env` files where a
    var is declared but blank. Default severity is WARNING because most
    env-keyed features (LLM checks, SMTP send, optional integrations) are
    degradation-not-failure when the var is absent."""
    value = os.environ.get(var_name, "")
    if not value:
        skip = SkipReason(
            name=var_name,
            reason="env var unset (or empty)",
            severity=severity,
            where=where,
        )
        log.warning(skip.format_for_log())
        return None, skip
    return value, None


def safe_call(
    name: str,
    fn: Callable[..., Any],
    *args: Any,
    severity: Severity = Severity.WARNING,
    where: str = "",
    **kwargs: Any,
) -> Tuple[Optional[Any], Optional[SkipReason]]:
    """Call `fn(*args, **kwargs)`. Returns (value, None) on success,
    (None, skip) when fn raises.

    Replaces the `try: ... except Exception as _e: log.debug("..."); return []`
    pattern (86 instances in the audit). The exception is logged at WARNING
    by default so operators see real failures instead of silently degraded
    features. Callers can pass `severity=Severity.INFO` for genuinely
    informational call sites (cache misses, etc.)."""
    try:
        return fn(*args, **kwargs), None
    except Exception as e:
        skip = SkipReason(
            name=name,
            reason=f"{type(e).__name__}: {e}",
            severity=severity,
            where=where,
        )
        log.warning(skip.format_for_log())
        return None, skip
