"""Pin: startup_checks Code-patterns recognizes Spine substrate isolation.

2026-05-26 (Mike dashboard surfaced via vision-pass on PR #1095 deploy):
the Deploy Issue alert "1 check(s) failed: Code patterns" was firing
because `src/spine/catalog.py` has a bare `sqlite3.connect()` and lacks
the marker the heuristic looks for (`init_db` / `get_db` /
`from src.core`). It can't legally add a src.core import — §0 LAW 1
forbids cross-substrate imports from Spine.

Fix: extend Pattern C's guard list to recognize `rel.startswith("spine/")`
as a valid substrate-isolation signal (parallel to db.py / startup_checks.py
fname exclusion).

These tests assert: (a) the live repo passes Code patterns, (b) the
exclusion is path-scoped (not fname-only), so a hypothetical bare-connect
in src/core/anything.py would still trip.
"""
from __future__ import annotations

import os
import re
from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_SRC = _REPO / "src"


def _code_patterns_issues(src_root: str) -> list[str]:
    """Mirror of startup_checks.check_code_patterns Pattern C only.

    Inlined here so the test pins the EXACT semantics the prod check
    runs, including the Spine substrate guard. If startup_checks.py
    diverges from this logic, the test should be re-synced — that's
    deliberate (the test is the spec of the rule).
    """
    issues = []
    for root, dirs, files in os.walk(src_root):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for fname in files:
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(root, fname)
            with open(fpath, encoding="utf-8", errors="replace") as f:
                content = f.read()
            rel = os.path.relpath(fpath, src_root)
            if "sqlite3.connect(" in content and fname not in (
                "db.py", "startup_checks.py",
            ):
                is_spine_substrate = (
                    rel.startswith("spine" + os.sep) or rel.startswith("spine/")
                )
                has_guard = (
                    "init_db" in content or "get_db" in content
                    or "from src.core" in content or is_spine_substrate
                )
                if not has_guard:
                    issues.append(f"{rel}: direct sqlite3.connect without any db guard")
    return issues


def test_live_repo_passes_code_patterns_pattern_c():
    """The live repo should have ZERO Pattern-C violations after the
    Spine substrate guard lands. If this fails, a new file with a
    bare `sqlite3.connect()` was added outside Spine and without a
    core import marker — fix the file, don't loosen the test.
    """
    issues = _code_patterns_issues(str(_SRC))
    assert issues == [], (
        f"Code patterns Pattern C tripped: {issues}. "
        "Either add a src.core import / init_db call, or — if this is "
        "a new Spine substrate file — confirm it's under src/spine/."
    )


def test_spine_catalog_is_recognized_via_spine_guard():
    """Coleman regression: src/spine/catalog.py was specifically the
    file tripping the check on 2026-05-26. Pin it: spine/catalog.py
    must pass via the substrate-isolation guard, not via accidentally
    matching `init_db`/`get_db`/`from src.core`."""
    catalog = _SRC / "spine" / "catalog.py"
    assert catalog.exists(), "spine/catalog.py is the canonical fixture for this test"
    content = catalog.read_text(encoding="utf-8")
    # Sanity: the file actually does what the check is worried about.
    assert "sqlite3.connect(" in content, (
        "spine/catalog.py no longer has bare sqlite3.connect — update "
        "this test to use the new fixture (or delete if no longer needed)."
    )
    # And it actually lacks the three legacy markers (which is why the
    # substrate-isolation guard was needed in the first place).
    assert "init_db" not in content
    assert "get_db" not in content
    assert "from src.core" not in content
    # The full live-repo check above already exercises the guard;
    # this just documents which file motivated the rule.


def test_pattern_c_still_catches_core_violations():
    """The substrate guard is path-scoped: a hypothetical bare-connect
    in src/core/* still fails. Simulates by running the check against
    a tiny temp tree."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        tmp_src = Path(tmp)
        # A Spine-style file: should NOT trip (path guard)
        spine_dir = tmp_src / "spine"
        spine_dir.mkdir()
        (spine_dir / "newsub.py").write_text(
            "import sqlite3\nconn = sqlite3.connect('x.db')\n",
            encoding="utf-8",
        )
        # A core-style file with bare connect: SHOULD trip
        core_dir = tmp_src / "core"
        core_dir.mkdir()
        (core_dir / "badmodule.py").write_text(
            "import sqlite3\nconn = sqlite3.connect('x.db')\n",
            encoding="utf-8",
        )
        issues = _code_patterns_issues(str(tmp_src))
        # Exactly one violation, and it's the core file
        assert len(issues) == 1, f"expected 1 violation, got {issues}"
        assert "core" in issues[0] and "badmodule.py" in issues[0]
        assert "spine" not in issues[0]
