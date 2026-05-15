"""PR-AV-AC6 — generate_rfq_package must define _include / _req_forms /
_user_forms / tmpl BEFORE its outer try block.

5/15 PREQ 10847262 (rfq_9e63456e) regen failed with:

    UnboundLocalError: cannot access local variable '_include'
    where it is not associated with a value
    at routes_rfq_gen.py:2605

ROOT CAUSE

`generate_rfq_package` had an outer try-except at L1786-L2600 that
wrapped the entire "Step 2: Fill State Forms" block. The inner
`def _include(form_id)` at L1946 was INSIDE that try. When any line
between L1786 and L1946 raised, the outer except absorbed the
exception (silently — only appended a string to `errors[]`) and
execution continued past the except block. The L2605 "703C master
template fallback" then unconditionally called `_include("703b")`
on a name that was never bound — UnboundLocalError 500.

Worse: because `errors.append(f"State forms: {e}")` didn't log the
exception, Railway had no diagnostic trail for the ORIGINAL cause
of the early abort. Only the downstream UnboundLocalError surfaced.

THE FIX

Hoist `_include` (with a safe user-checklist-only default), plus
`_req_forms` / `_opt_forms` / `_user_forms` / `tmpl` / `_agency_key`
/ `_agency_cfg`, to BEFORE the outer try. The inner def at L1946
overrides on the happy path; the defaults survive when the outer
try aborts. Plus add `log.exception(...)` to the outer except so
the original cause is visible in Railway logs.

CLAUDE.md principle (Build Quality Rules):
  "Never Reference Variables Across try/except Boundaries —
  if a variable is set inside a `try:` block, the `except:` block
  MUST also set it."

WHAT THIS TEST PINS
==================
  - generate_rfq_package's `def _include` happens BEFORE the outer
    `try:` (line-order check, AST-aware)
  - Defaults for _req_forms / _opt_forms / _user_forms / tmpl /
    _agency_key / _agency_cfg are bound before the outer try
  - The PR-AV-AC6 marker is present in the source
  - The outer except adds `log.exception(...)` so the original
    error gets a stack trace in Railway logs
"""
from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPO_ROOT / "src" / "api" / "modules" / "routes_rfq_gen.py"


def _find_function(name: str) -> ast.FunctionDef:
    src = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {TARGET}")


def _state_forms_try(fn: ast.FunctionDef) -> ast.Try:
    """Locate the outer 'State Forms' try block — the one that wraps
    the inner `def _include`. Identified by: largest try block (by
    line span) at the function's direct-statement level whose body
    contains a `def _include`.

    The function also has many inner try blocks (per-item float coerce
    at L1711, orchestrator observer, etc.). Those are not the target.
    """
    candidates = []
    for stmt in ast.walk(fn):
        if not isinstance(stmt, ast.Try):
            continue
        for child in ast.walk(stmt):
            if isinstance(child, ast.FunctionDef) and child.name == "_include":
                candidates.append(stmt)
                break
    assert candidates, (
        "no try block in generate_rfq_package contains a `def _include` — "
        "did the function shape change?"
    )
    # If multiple try blocks wrap the def (nested), pick the outermost.
    return min(candidates, key=lambda t: t.lineno)


def test_outer_try_preceded_by_include_default():
    """The `def _include` outside the State-forms try AND the State-
    forms try line: def must precede the try.

    Two separate `def _include` blocks: a defensive one BEFORE the
    State-forms try (PR-AV-AC6, safe defaults), and the original one
    INSIDE the try at the original L1946 site. We want the OUTER one.
    """
    fn = _find_function("generate_rfq_package")
    state_try = _state_forms_try(fn)
    # Find a def _include that is NOT inside state_try
    pre_try_def_lines = []
    inside_try_lines = set()
    for child in ast.walk(state_try):
        if isinstance(child, ast.FunctionDef) and child.name == "_include":
            inside_try_lines.add(child.lineno)
    for sub in ast.walk(fn):
        if isinstance(sub, ast.FunctionDef) and sub.name == "_include":
            if sub.lineno not in inside_try_lines:
                pre_try_def_lines.append(sub.lineno)
    assert pre_try_def_lines, (
        "expected at least one `def _include` BEFORE the State-forms "
        "try (the PR-AV-AC6 defensive default); none found"
    )
    earliest_pre = min(pre_try_def_lines)
    assert earliest_pre < state_try.lineno, (
        f"defensive `def _include` (line {earliest_pre}) must precede "
        f"the State-forms try (line {state_try.lineno}). PR-AV-AC6."
    )


def test_required_defaults_bound_before_outer_try():
    """The pre-try defaults block must bind _req_forms / _opt_forms /
    _user_forms / _agency_key / _agency_cfg / tmpl before the State-
    forms try.

    These names are read by the inner def _include and by code after
    the outer except (notably L2605 / Step 2.5). When the outer try
    aborts before the inner setup, defaults must already be in place.
    """
    fn = _find_function("generate_rfq_package")
    state_try_line = _state_forms_try(fn).lineno
    required = {
        "_req_forms", "_opt_forms", "_user_forms",
        "_agency_key", "_agency_cfg", "tmpl",
    }
    bound_before_try: set = set()
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Assign) and sub.lineno < state_try_line:
            for tgt in sub.targets:
                if isinstance(tgt, ast.Name) and tgt.id in required:
                    bound_before_try.add(tgt.id)
        if isinstance(sub, ast.AnnAssign) and sub.lineno < state_try_line:
            tgt = sub.target
            if isinstance(tgt, ast.Name) and tgt.id in required:
                bound_before_try.add(tgt.id)
    missing = required - bound_before_try
    assert not missing, (
        f"PR-AV-AC6: these names must be bound before the State-forms "
        f"try block (line {state_try_line}), none missing — found "
        f"missing: {sorted(missing)}. bound={sorted(bound_before_try)}"
    )


def test_outer_except_logs_with_traceback():
    """The outer except must call log.exception (or log.error with
    exc_info=True) so the original cause is captured in Railway logs.
    """
    src = TARGET.read_text(encoding="utf-8")
    ac6_idx = src.find("PR-AV-AC6")
    assert ac6_idx > 0, "PR-AV-AC6 marker missing"
    # Look for log.exception(...) anywhere within the function near the
    # State-forms except block. AST check would be fragile across line-
    # number drift; just confirm the source contains the call with the
    # State-forms-context message.
    assert "log.exception(" in src, (
        "outer except must use log.exception() so the original cause "
        "gets a stack trace in Railway logs"
    )
    assert "State forms try block failed" in src, (
        "PR-AV-AC6 outer-except log message must mention 'State forms "
        "try block failed' so it greps cleanly in logs"
    )


def test_source_grep_ac6_marker_present():
    """Pin the marker so future refactors can't drop the defensive
    pre-try hoist silently.
    """
    src = TARGET.read_text(encoding="utf-8")
    assert "PR-AV-AC6" in src, "PR-AV-AC6 marker must remain in routes_rfq_gen.py"
    # And there should be at least one occurrence in BOTH the pre-try
    # block AND the outer except logging block.
    first = src.find("PR-AV-AC6")
    second = src.find("PR-AV-AC6", first + 1)
    assert second > first, (
        "expected PR-AV-AC6 marker in both the pre-try defaults block "
        "AND the outer except's log.exception() comment block"
    )
