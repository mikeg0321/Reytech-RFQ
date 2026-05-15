"""PR-AV-AC7 — generate_rfq_package must not re-import dashboard
globals inline, and _capacity_blockers must be hoisted out of the
outer State-forms try.

CONTEXT
=======

The 5/15 rfq_9e63456e regen failed even AFTER AC6 hoisted _include
defaults. AC6's `log.exception(...)` revealed the actual root cause:

    UnboundLocalError: cannot access local variable 'list_rfq_files'
    where it is not associated with a value
    at routes_rfq_gen.py:1815 (db_files = list_rfq_files(...))

Why: PR-AV4 (AV-17, 2026-05-14) added `from src.api.dashboard import
list_rfq_files, get_rfq_file` INSIDE generate_rfq_package at line
2059. Because routes_rfq_gen.py is loaded via exec() into
dashboard.py's namespace (see CLAUDE.md "Module loading" rule),
list_rfq_files is already a module global there — and the inline
import is redundant.

The cost of "redundant" is severe: Python sees the future
assignment to `list_rfq_files` at L2059 and promotes the name to
LOCAL for the entire function. So the L1815 reference (which runs
BEFORE L2059) raises UnboundLocalError. The outer try absorbs the
exception, errors.append swallows it silently — and downstream
post-except reads (`_include`, `_capacity_blockers`) ALSO bomb
with UnboundLocalError on names that were supposed to be set
inside that try.

AC6 hoisted defaults for _include / _req_forms / _opt_forms /
_user_forms / tmpl / _agency_key / _agency_cfg. AC7 closes the
ROOT CAUSE (remove the inline import) AND hoists the last cohabit
in this scope (_capacity_blockers).

WHAT THIS TEST PINS
===================

  - No inline `from src.api.dashboard import list_rfq_files`
    (or `get_rfq_file`) inside generate_rfq_package
  - _capacity_blockers is bound before the State-forms try
  - PR-AV-AC7 marker present in the source

A wider style-check that no exec()-loaded route file inline-imports
ANY symbol it already gets via dashboard.py exec namespace is out
of scope for this PR — that's a refactor that touches >40 sites
and the symbol-by-symbol verification is tedious. AC7 targets the
specific symbols that broke production.
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


def test_no_inline_dashboard_import_of_rfq_files_helpers():
    """Inside generate_rfq_package, there must be NO `from
    src.api.dashboard import list_rfq_files` or `... import
    get_rfq_file`. These symbols are already in the module global
    namespace via exec — inline imports promote them to local and
    create the function-wide UnboundLocalError shadow that broke the
    5/15 rfq_9e63456e regen.
    """
    fn = _find_function("generate_rfq_package")
    bad_imports = []
    for sub in ast.walk(fn):
        if isinstance(sub, ast.ImportFrom):
            if sub.module != "src.api.dashboard":
                continue
            for alias in sub.names:
                if alias.name in ("list_rfq_files", "get_rfq_file"):
                    bad_imports.append(
                        f"L{sub.lineno}: from src.api.dashboard "
                        f"import {alias.name}"
                    )
    assert not bad_imports, (
        f"PR-AV-AC7: inline re-imports of {bad_imports} found inside "
        f"generate_rfq_package. routes_rfq_gen.py is exec'd into "
        f"dashboard.py's namespace; these symbols are already global. "
        f"Removing the inline import is the ROOT CAUSE fix for the "
        f"5/15 rfq_9e63456e UnboundLocalError on list_rfq_files."
    )


def test_capacity_blockers_hoisted_before_outer_try():
    """_capacity_blockers is read at the post-except L3201 site
    (Surface capacity blockers in the JSON response). It must be
    bound before the State-forms try to survive an early try abort.
    """
    fn = _find_function("generate_rfq_package")
    # Find the outer State-forms try (the one whose body contains
    # the inner def _include)
    state_try = None
    for stmt in ast.walk(fn):
        if isinstance(stmt, ast.Try):
            for child in ast.walk(stmt):
                if isinstance(child, ast.FunctionDef) and child.name == "_include":
                    if state_try is None or stmt.lineno < state_try.lineno:
                        state_try = stmt
    assert state_try is not None
    bound_before_try = False
    for sub in ast.walk(fn):
        # Both forms — bare assign and annotated assign
        if isinstance(sub, ast.Assign) and sub.lineno < state_try.lineno:
            for tgt in sub.targets:
                if isinstance(tgt, ast.Name) and tgt.id == "_capacity_blockers":
                    bound_before_try = True
        if isinstance(sub, ast.AnnAssign) and sub.lineno < state_try.lineno:
            tgt = sub.target
            if isinstance(tgt, ast.Name) and tgt.id == "_capacity_blockers":
                bound_before_try = True
    assert bound_before_try, (
        "_capacity_blockers must be bound before the State-forms "
        "outer try (line {}). PR-AV-AC7.".format(state_try.lineno)
    )


def test_source_grep_ac7_marker_present():
    """Pin the AC7 marker so future refactors can't drop the
    defensive hoist or re-introduce the shadow silently.
    """
    src = TARGET.read_text(encoding="utf-8")
    assert "PR-AV-AC7" in src, "PR-AV-AC7 marker must remain in routes_rfq_gen.py"


def test_list_rfq_files_callable_in_module_globals():
    """Sanity: confirm list_rfq_files and get_rfq_file are exposed
    at the dashboard module level so the implicit exec-namespace
    binding actually works. This is the architectural reason AC7's
    inline-import removal is safe.
    """
    from src.api import dashboard
    assert callable(getattr(dashboard, "list_rfq_files", None)), (
        "src.api.dashboard.list_rfq_files must be a module-level "
        "function — routes_rfq_gen.py relies on it being in the "
        "exec namespace without an inline import"
    )
    assert callable(getattr(dashboard, "get_rfq_file", None)), (
        "src.api.dashboard.get_rfq_file must be a module-level "
        "function — same exec-namespace reliance"
    )
