"""PR-6 (#696) lint: no NEW inline status/date filters outside src/core/.

Background — the .githooks/pre-push hook blocks new ad-hoc
`WHERE status IN (...)`, `WHERE created_at >= ...`, etc. from landing
outside `src/core/`. PR-6 hardened that gate from a ratchet (with a
.canonical_allowlist.txt grandfather list) into a hard ban on NEW
additions.

This test mirrors the hook so:
  1. CI catches the same regressions the hook would, even when a push
     bypasses the hook (e.g. force-push, contributor with hooks
     disabled, or a different VCS surface).
  2. The "what counts as an offending pattern" rule lives next to the
     other canonical-state tests instead of only in shell.

Like the pre-push hook, this scans only *new lines added since
origin/main* — older inline filters in unrelated query paths
(oracle weekly report, due-date reminder, etc.) aren't part of the
home-page truth-layer arc and are not in scope. PR-6 closed the arc
for the consumers that drove the 2026-05-02 home-page divergence.

The pattern set must match the hook's grep on .githooks/pre-push line
244. Update both together if the rule changes.
"""
from __future__ import annotations

import re
import subprocess

import pytest

INLINE_FILTER_RE = re.compile(
    r"WHERE\s+(LOWER\()?(status|created_at|sent_at|invoice_date|logged_at)",
    re.IGNORECASE,
)

COMMENT_PREFIXES = ("#", "//", "*", "--", "/*")


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def _have_origin_main() -> bool:
    try:
        _git("rev-parse", "--verify", "origin/main")
        return True
    except subprocess.CalledProcessError:
        return False


def test_no_new_inline_status_or_date_filters_outside_src_core():
    """Hard ban on NEW inline filters outside src/core/.

    Use src.core.canonical_state predicates (is_active_queue,
    is_real_sent, is_sourceable_po, is_awaiting_buyer, is_year_revenue)
    or SELECT from the matching VIEW.
    """
    if not _have_origin_main():
        pytest.skip("origin/main not available in this checkout — gate enforced by pre-push hook")

    # Diff vs origin/main, restricted to source files OUTSIDE src/core/
    # (where canonical_state.py + migrations.py legitimately define filters)
    # and tests/ (where seed inserts use the columns).
    try:
        diff = subprocess.check_output(
            [
                "git", "diff", "origin/main...HEAD", "--",
                "src/", ":(exclude)src/core/**", ":(exclude)tests/**",
            ],
            text=True,
            errors="replace",
        )
    except subprocess.CalledProcessError:
        pytest.skip("git diff against origin/main failed — gate enforced by pre-push hook")

    offenders: list[str] = []
    for line in diff.splitlines():
        # Only added lines, skip hunk headers (`+++ b/...`).
        if not line.startswith("+") or line.startswith("+++"):
            continue
        added = line[1:]
        stripped = added.lstrip()
        if not stripped or stripped.startswith(COMMENT_PREFIXES):
            continue
        if INLINE_FILTER_RE.search(added):
            offenders.append(added.strip())

    assert not offenders, (
        "PR-6 hard ban tripped: NEW inline WHERE status / created_at / "
        "sent_at / invoice_date / logged_at filter outside src/core/ "
        "vs origin/main. Use src.core.canonical_state predicates or "
        "the matching VIEW instead.\n\n"
        + "\n".join(f"  + {line}" for line in offenders[:20])
    )
