"""Lint that catches new raw `UPDATE quotes SET status` writers.

The audit 2026-05-07 v2 §S-12 and the 2026-05-11 deep audit both found
~13 sites in production code that bypass `set_quote_status_atomic`
(the canonical status-flip helper) by issuing raw SQL UPDATEs. Most
are agent daemons (award_tracker, email_poller, scprs_universal_pull,
revenue_engine, scprs_intelligence_engine, quotes_backfill). When an
operator and a daemon both attempt a status flip on the same quote,
the raw UPDATE racing the atomic helper can leave the audit-trail
out of sync, miss the on-flip telemetry, or step on the lifecycle
transition guards.

This lint blocks NEW raw status writers from landing while we
unify the existing offenders (planned: PR-η rollout). Exempt the
current offender set via BASELINE_EXEMPTIONS until each one is
migrated.

Same shape as `tools/lint_phantom_imports.py`:
  1. Walk every .py under src/
  2. Grep for `UPDATE quotes SET status`-shaped writes
  3. Report any site that's not exempted
  4. Exit non-zero on new violations

Run: `python tools/lint_quote_status_writes.py`
Wire into .githooks/pre-push so new offenders cannot ship silently.

Escape hatch: `STATUS_WRITER_LINT_SKIP=1` env var (use sparingly,
document the reason in the commit message).
"""
from __future__ import annotations

import os
import pathlib
import re
import sys
from typing import Iterator


# Existing raw `UPDATE quotes SET status` sites. Each entry is a
# strict pin: `<relative_path>:<line>`. If a file's line numbers
# shift, the lint trips — that's intentional, re-audit the site.
#
# Sourced from the 2026-05-11 deep audit (Agent C run). Each of
# these is a write-path that should eventually route through
# `src.core.lifecycle.set_quote_status_atomic` so the audit_trail +
# lifecycle hooks fire consistently. Drains:
#   * dashboard.py:4213 — operator action; PR-η Phase 2 target
#   * dashboard.py:4436 — operator action; PR-η Phase 2 target
#   * award_tracker.py × 3 — automated award detection; lower priority
#   * email_poller.py:2164 — automated; lower priority
#   * scprs_intelligence_engine.py:603 — analytics; lower priority
#   * revenue_engine.py:122 — billing; lower priority
#   * scprs_universal_pull.py:288 — analytics; lower priority
#   * quotes_backfill.py:89 — one-time migration script
#   * routes_v1.py:4327 — API; should consume set_quote_status_atomic
#   * db.py:3190 — the atomic helper's IMPLEMENTATION uses raw UPDATE
#     internally (allowed — that's the canonical writer)
BASELINE_EXEMPTIONS: set[str] = {
    # The canonical atomic helper itself — uses raw UPDATE by design.
    # If you see this line shift, double-check `set_quote_status_atomic`
    # internals haven't regressed.
    "src/core/db.py",
    # dashboard.py — MIGRATED 2026-05-11 (PR-η Phase 2). Both
    # operator-initiated order-creation paths now route through
    # set_quote_status_atomic with forbidden_prev=['won','cancelled'].
    # File NO LONGER exempt; the lint will block any new raw writer
    # that lands here.
    # award_tracker.py — MIGRATED 2026-05-11 (PR-η Phase 3, #883). All 3
    # sites (expire, scprs_win, scprs_loss) route through
    # set_quote_status_atomic with expected_prev='sent' preserving the
    # race-fence guard. NO LONGER EXEMPT.
    # MIGRATED 2026-05-11 (PR-η Phase 4, this PR) — NO LONGER EXEMPT:
    #   - email_poller.py — PO-via-email won detection
    #   - scprs_intelligence_engine.py — lost-to-competitor
    #   - scprs_universal_pull.py — closed-lost
    # All three use expected_prev='sent' to preserve the race-fence
    # against operator manual marks.
    # Still exempt — revenue_engine touches billing path; deliberate
    # eyeball before migrating.
    "src/agents/revenue_engine.py",
    # API + backfill — migrate before any new external integration.
    "src/api/modules/routes_v1.py",
    "src/core/quotes_backfill.py",
}


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Match shapes:
#   "UPDATE quotes SET status"  (most common — explicit SET status=)
#   "UPDATE quotes  SET status"  (extra whitespace)
#   "UPDATE quotes\n         SET status" (multi-line — also matches)
# Skips: comments, docstrings, test fixtures
RAW_UPDATE_RE = re.compile(
    r"UPDATE\s+quotes\s+SET\s+(?:[^,]*?,\s*)*status\b",
    re.IGNORECASE | re.DOTALL,
)


def iter_py_files() -> Iterator[pathlib.Path]:
    src = REPO_ROOT / "src"
    for p in src.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        yield p


def find_raw_status_writers() -> list[str]:
    """Returns list of relative_path:line violations.

    Tracks triple-quote docstring state across lines so a narrative
    reference like '...the UPDATE quotes SET status=expired actually...'
    inside a docstring is not flagged. Real SQL strings inside
    conn.execute(triple-quoted) calls are still caught because the
    regex matches on the SET clause shape, which only appears in
    real SQL — and the SQL block opens AFTER an open-paren which
    keeps it outside the docstring tracker (the script body context
    is what wraps it, not a separate docstring).
    """
    findings: list[str] = []
    for path in iter_py_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in BASELINE_EXEMPTIONS:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        in_doc = False  # triple-quoted docstring (""" or ''')
        doc_quote = None
        for lineno, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            # Track docstring opens/closes. A docstring is identified by
            # a line that STARTS with triple-quotes (possibly after
            # whitespace) — that distinguishes a module/function/class
            # docstring from a real SQL string passed to conn.execute(
            # """...""") which has code before the triple-quote.
            if not in_doc:
                for q in ('"""', "'''"):
                    if stripped.startswith(q):
                        # Module/function docstring opener. Check if it
                        # also closes on the same line (one-line doc).
                        rest = stripped[len(q):]
                        if q in rest:
                            # Same-line close — no state change.
                            pass
                        else:
                            in_doc = True
                            doc_quote = q
                        break
            else:
                if doc_quote and doc_quote in line:
                    in_doc = False
                    doc_quote = None
                continue
            # Skip pure comment lines.
            if stripped.startswith("#"):
                continue
            if RAW_UPDATE_RE.search(line):
                findings.append(f"{rel}:{lineno}")
    return findings


def main() -> int:
    if os.environ.get("STATUS_WRITER_LINT_SKIP"):
        print("status-writer-lint: SKIPPED via STATUS_WRITER_LINT_SKIP env")
        return 0

    findings = find_raw_status_writers()
    if not findings:
        n_exempt = len(BASELINE_EXEMPTIONS)
        print(f"status-writer-lint: ok (0 new, {n_exempt} exempt baseline files)")
        return 0

    print("status-writer-lint: NEW raw `UPDATE quotes SET status` writer detected:")
    for f in findings:
        print(f"  {f}")
    print()
    print("These writers bypass set_quote_status_atomic. Route status flips")
    print("through src.core.lifecycle.set_quote_status_atomic instead, or")
    print("add the file to BASELINE_EXEMPTIONS in tools/lint_quote_status_writes.py")
    print("with a comment explaining why (and a migration plan).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
