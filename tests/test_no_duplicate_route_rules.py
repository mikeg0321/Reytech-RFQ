"""App-wide duplicate-route guard.

PR O1: the two registrations of `POST /api/pricecheck/<pcid>/auto-price`
in `routes_pricecheck_admin.py` caused Werkzeug to silently route every
request to the FIRST-registered handler.  The first one was the
non-persisting handler; the second (race-safe, `_save_pcs_lock`-wrapped,
`_save_single_pc`-persisting) was unreachable dead code.  Result: every
click of the "Find Prices" button appeared to succeed but persisted
nothing — pricing was lost on reload.

This test scans all routes_*.py files in src/api/modules/ and asserts
that no two `@bp.route(...)` / `@app.route(...)` decorators register the
same (endpoint_path, methods) pair, so this class of bug cannot recur
anywhere in the application.

Static-analysis approach: mirrors the pattern used by
test_re_audit_9_pc_admin_post_only.py and test_rmw_race_lint.py — no
Flask app import needed, works offline, sub-second.
"""
from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

MODULES_DIR = (
    Path(__file__).resolve().parents[1] / "src" / "api" / "modules"
)

# Matches:  @bp.route("/some/path/<param>", methods=["POST"])
#           @bp.route("/some/path")          (no methods → defaults to GET)
#           @app.route(...)
_ROUTE_RE = re.compile(
    r'@(?:bp|app)\.route\(\s*"([^"]+)"'   # group 1: path
    r'(?:\s*,\s*methods=\[([^\]]*)\])?'    # group 2: methods list (optional)
    r'\s*\)',
    re.MULTILINE,
)

_METHOD_TOKEN_RE = re.compile(r'"([A-Z]+)"')


def _parse_methods(methods_str: str | None) -> frozenset[str]:
    """Return the set of HTTP methods from the methods=[...] string.

    When absent Flask defaults to {"GET"}.
    HEAD and OPTIONS are auto-added by Werkzeug — exclude them from the
    duplicate check so only user-specified method sets are compared.
    """
    if not methods_str:
        return frozenset({"GET"})
    return frozenset(_METHOD_TOKEN_RE.findall(methods_str))


def _collect_all_route_registrations() -> list[tuple[str, str, frozenset[str]]]:
    """Return a list of (filename, path, methods_frozenset) for every
    @bp.route / @app.route decorator across all routes_*.py modules."""
    out: list[tuple[str, str, frozenset[str]]] = []
    for py_file in sorted(MODULES_DIR.glob("routes_*.py")):
        src = py_file.read_text(encoding="utf-8")
        for m in _ROUTE_RE.finditer(src):
            path = m.group(1)
            methods = _parse_methods(m.group(2))
            out.append((py_file.name, path, methods))
    return out


def test_no_duplicate_url_map_rules():
    """No two route decorators in any routes_*.py may share (path, methods).

    Werkzeug always routes to the first-registered match, so a second
    registration of the same (path, methods) is silently dead code.
    Exactly this pattern caused the PR O1 auto-price data-loss bug:
    POST /api/pricecheck/<pcid>/auto-price was registered twice; the
    second (persisting) handler was never reached.
    """
    registrations = _collect_all_route_registrations()

    # Map (path, methods) → list of filenames where it appears
    seen: dict[tuple[str, frozenset[str]], list[str]] = defaultdict(list)
    for filename, path, methods in registrations:
        seen[(path, methods)].append(filename)

    duplicates = {k: v for k, v in seen.items() if len(v) > 1}
    if duplicates:
        lines = [
            "Duplicate (path, methods) route registrations detected across routes_*.py.",
            "  Werkzeug silently routes every request to the FIRST match.",
            "  The second registration is unreachable dead code.",
            "  Fix: delete the shadowed registration.",
            "",
        ]
        for (path, methods), filenames in sorted(
            duplicates.items(), key=lambda x: x[0][0]
        ):
            methods_str = ", ".join(sorted(methods))
            lines.append(f"  [{methods_str}]  {path}")
            for fn in filenames:
                lines.append(f"      in: {fn}")
        raise AssertionError("\n".join(lines))
