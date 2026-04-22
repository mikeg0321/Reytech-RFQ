"""AN-2 regression guard: /api/system/metrics had a double bug that made
uptime_seconds permanently broken.

Audited 2026-04-22:
  1. The endpoint body referenced bare `time.time()` — but the module
     imports `time as _time`, so if the guard ever let the body run it
     would hit NameError.
  2. The guard was `if '_plt_start' in dir() else None`. Inside a
     function, `dir()` returns locals, not module globals, so the guard
     was ALWAYS False and uptime was ALWAYS None.

Fix: define `_plt_start = _time.time()` at module import, and write the
uptime expression with `_time.time()`.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_ANALYTICS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_analytics.py"
)


def _strip_comment_lines(src: str) -> str:
    """Drop full-line comments so audit-intent comments don't false-trip
    regex guards. Inline comments on code lines are kept (too risky to
    strip, would need a real tokenizer)."""
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def test_plt_start_defined_at_module_scope():
    """`_plt_start` must be initialised at module import time so the
    metrics endpoint can compute uptime."""
    src = ROUTES_ANALYTICS.read_text(encoding="utf-8")
    # Look for `_plt_start = <something>` at column 0 (module-level)
    m = re.search(r"(?m)^_plt_start\s*=\s*_time\.time\(\s*\)", src)
    assert m, (
        "AN-2 regression: `_plt_start = _time.time()` is not defined at "
        "module scope. /api/system/metrics will report uptime=None."
    )


def test_no_bare_time_in_routes_analytics():
    """The module aliases `time as _time`. Any bare `time.time()` call
    raises NameError at runtime."""
    src = _strip_comment_lines(ROUTES_ANALYTICS.read_text(encoding="utf-8"))
    # Exclude substring matches inside words (e.g. `_time.time`), but
    # match a bare reference like `time.time(` not preceded by `_` or `.`.
    bad = re.findall(r"(?<![_.])\btime\.time\(", src)
    assert not bad, (
        f"AN-2 regression: {len(bad)} bare `time.time(` call(s) found in "
        "routes_analytics.py — module only imports `time as _time`, so "
        "these raise NameError at runtime. Use `_time.time()`."
    )


def test_no_dir_guard_around_plt_start():
    """The `'_plt_start' in dir()` guard was always False at function
    scope. Guard that it never returns."""
    src = _strip_comment_lines(ROUTES_ANALYTICS.read_text(encoding="utf-8"))
    assert "'_plt_start' in dir()" not in src, (
        "AN-2 regression: the always-False `'_plt_start' in dir()` guard "
        "is back. dir() at function scope doesn't see module globals."
    )


def test_metrics_endpoint_uses_time_alias():
    """The metrics endpoint must compute uptime via _time.time()."""
    src = ROUTES_ANALYTICS.read_text(encoding="utf-8")
    m = re.search(
        r"def api_system_metrics\(\)[\s\S]{0,2500}?uptime_seconds[^\n]*\n",
        src,
    )
    assert m, "api_system_metrics endpoint or uptime_seconds key not found"
    block = m.group(0)
    assert "_time.time()" in block, (
        "AN-2 regression: api_system_metrics must use `_time.time()` to "
        "compute uptime. Bare `time.time()` raises NameError."
    )
