#!/usr/bin/env python3
"""tools/lint_inline_status_whitelists.py — Tier 1c lint (audit 2026-05-07).

Bans hardcoded `WHERE status [NOT] IN ('a', 'b', ...)` SQL literals in
route modules. Status acceptance MUST route through
`src.core.status_taxonomy.is_valid_status_for()` and queue filters
through `src.core.canonical_state` predicates. Pre-PR-#832 we had 5
inline `valid = {...}` Python whitelists that silently disagreed with
each other. PR #832 collapsed those; this lint catches the SQL flavor
of the same regression class going forward.

Differs from the diff-based pre-push gate in `.githooks/pre-push`:
  * Diff gate: flags NEW lines (`git diff origin/main...HEAD`) that
    add `WHERE status` outside `src/core/`. Catches incoming
    additions.
  * This lint: enumerates the FULL CURRENT STATE so existing tech
    debt is tracked in code (the EXEMPTIONS list below), and any
    silent re-introduction of the same shape after cleanup will trip.

Usage:
    python tools/lint_inline_status_whitelists.py
        # exit 0 if clean, 1 with details if any unexempted match.

Cleanup workflow:
    When a route is migrated to use canonical predicates, the
    matching EXEMPTIONS entry must be removed. The tool exits 1 if
    an exempted literal is missing (= file edited, literal removed
    or moved) — this prevents stale exemptions from masking new
    regressions.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List, Tuple

# Match `(WHERE|AND|OR) status [NOT] IN (...quoted...)`. The closing
# paren is non-greedy so a `WHERE status IN (...) AND created_at <
# datetime('now', ...)` only captures up to the first `)`. The
# leading word can be WHERE/AND/OR because compound clauses (`WHERE
# x = ? AND status IN (...)`) are common.
_WHERE_STATUS_IN = re.compile(
    r"(?:WHERE|AND|OR)\s+status\s+(?:NOT\s+)?IN\s*\(\s*['\"][^)]+?\)",
    re.IGNORECASE,
)
# Sub-pattern: extract just the parenthesized literal list.
_LITERAL_LIST = re.compile(r"\(\s*['\"][^)]+?\)")


SCAN_DIRS = ["src/api/modules"]


# Exemption list — known existing tech debt as of 2026-05-07 (audit
# Tier 1c follow-on). Each entry is `(repo-relative path, literal
# substring)`. The substring must appear verbatim in a `WHERE status
# IN (...)` match for the exemption to apply. Cleanup follow-up PRs
# remove an entry when the route is migrated to canonical predicates.
#
# Format: line numbers are NOT recorded — they shift under edits. The
# substring is enough to disambiguate.
EXEMPTIONS: List[Tuple[str, str]] = [
    # ─── routes_analytics.py — dashboard/funnel reads ───────────────
    # Read-side reports against `quotes` / `rfq_records` / `orders`.
    # Cleanup = introduce canonical predicates in canonical_state.py
    # (is_active_quote / is_terminal_quote / is_pending_quote etc.)
    # then migrate each call site.
    ("src/api/modules/routes_analytics.py",
     "AND status IN ('paid','invoiced','delivered','shipped','active')"),
    ("src/api/modules/routes_analytics.py",
     "AND status IN ('sent','draft','priced')"),
    ("src/api/modules/routes_analytics.py",
     "WHERE status IN ('new','draft','priced','quoted')"),
    ("src/api/modules/routes_analytics.py",
     "AND status NOT IN ('sent','won','lost','ordered')"),
    ("src/api/modules/routes_analytics.py",
     'and status in ("new", "draft", "priced")'),
    ("src/api/modules/routes_analytics.py",
     "WHERE status IN ('won','lost','sent','pending_award')"),
    ("src/api/modules/routes_analytics.py",
     "WHERE status IN ('won','lost')"),
    ("src/api/modules/routes_analytics.py",
     "AND status IN ('won','lost','sent')"),

    # ─── routes_catalog_finance.py — finance reads ──────────────────
    ("src/api/modules/routes_catalog_finance.py",
     "AND status IN ('sent','quoted')"),

    # ─── routes_health.py — system-health page ──────────────────────
    # `email_outbox` has its own state machine (failed /
    # permanently_failed). NOT a PC/RFQ status — different taxonomy.
    # Kept exempt indefinitely, but tracked here for visibility.
    ("src/api/modules/routes_health.py",
     "AND status IN ('open','shipped','closed','completed','invoiced')"),
    ("src/api/modules/routes_health.py",
     "WHERE status IN ('failed','permanently_failed')"),

    # ─── routes_intelligence.py ─────────────────────────────────────
    ("src/api/modules/routes_intelligence.py",
     "AND status NOT IN ('won','lost','cancelled','expired')"),

    # ─── routes_oracle_*.py — Oracle pricing intel reads ────────────
    # Bulk read-side queries enumerating won/lost/sent for win-rate
    # math. Cleanup = pricing_oracle_v2.is_terminal() predicate.
    ("src/api/modules/routes_oracle_category_intel.py",
     "AND status IN ('won', 'lost')"),
    ("src/api/modules/routes_oracle_item_history.py",
     "AND status IN ('won', 'lost', 'sent')"),
    ("src/api/modules/routes_oracle_item_history.py",
     "AND status IN ('won', 'lost')"),
    ("src/api/modules/routes_oracle_win_rate.py",
     "AND status IN ('won', 'lost')"),
    ("src/api/modules/routes_oracle_win_rate.py",
     "AND status IN ('won', 'lost', 'sent')"),

    # ─── routes_orders_full.py — order-list reads ───────────────────
    # `orders` table has its own status taxonomy (sent/pending/won
    # vs closed/invoiced). Cleanup = canonical_state.is_open_order().
    ("src/api/modules/routes_orders_full.py",
     "AND status IN ('sent','pending','won')"),
    ("src/api/modules/routes_orders_full.py",
     'and status not in ("closed", "invoiced")'),

    # ─── routes_outreach_next.py — outreach state ───────────────────
    # `outreach_messages` / `email_outbox` state machines are
    # distinct from PC/RFQ. Cleanup = consolidate outreach state.
    ("src/api/modules/routes_outreach_next.py",
     "WHERE status IN ('sent','delivered')"),
    ("src/api/modules/routes_outreach_next.py",
     "AND status IN ('draft', 'pending', 'queued')"),

    # ─── routes_prd28.py — PRD-28 backlog dashboard ─────────────────
    # The `auto_drafted` token is NOT in PC_VALID_STATUSES — real
    # drift the cleanup PR will surface (canonicalize or remove).
    ("src/api/modules/routes_prd28.py",
     "AND status IN ('new', 'parsed')"),
    ("src/api/modules/routes_prd28.py",
     "WHERE status IN ('priced', 'auto_drafted', 'draft', 'ready')"),
    ("src/api/modules/routes_prd28.py",
     "WHERE status NOT IN ('won', 'lost', 'expired', 'cancelled', 'dismissed')"),

    # ─── routes_pricecheck_admin.py / routes_pricecheck_pricing.py ──
    # PC and quote-side reads. Cleanup = is_terminal_pc() /
    # is_active_pc() in canonical_state.
    ("src/api/modules/routes_pricecheck_admin.py",
     "AND status IN ('draft','pending')"),
    ("src/api/modules/routes_pricecheck_pricing.py",
     "AND status IN ('draft','pending')"),
    ("src/api/modules/routes_pricecheck_pricing.py",
     "AND status IN ('won', 'lost')"),
    ("src/api/modules/routes_pricecheck_pricing.py",
     "AND status IN ('won', 'lost', 'sent', 'pending')"),

    # ─── routes_system.py — system status panel ─────────────────────
    ("src/api/modules/routes_system.py",
     "WHERE status IN ('new','parsed')"),

    # ─── routes_v1.py — legacy v1 API ───────────────────────────────
    # Three repeated reads of `quotes` filtering out void/cancelled.
    # Cleanup = is_active_quote() predicate.
    ("src/api/modules/routes_v1.py",
     "AND status NOT IN ('void', 'cancelled')"),
]


def _strip_comments(src: str) -> str:
    """Remove `# ...` line comments so commented-out SQL examples in
    docstrings/comments don't trip the lint. Preserves string literals
    intact. We do NOT strip triple-quoted blocks because legitimate
    SQL strings live there."""
    out_lines = []
    for line in src.split("\n"):
        # Naive: cut at first '#' not preceded by a quote on the same
        # line. Good enough for route-module conventions.
        idx = line.find("#")
        if idx == -1:
            out_lines.append(line)
            continue
        before = line[:idx]
        # If '#' is inside a string literal, leave the line alone.
        if before.count("'") % 2 == 1 or before.count('"') % 2 == 1:
            out_lines.append(line)
        else:
            out_lines.append(before)
    return "\n".join(out_lines)


def find_violations(repo_root: Path) -> Tuple[List[dict], List[Tuple[str, str]]]:
    """Walk SCAN_DIRS and report all `WHERE status [NOT] IN (...)`
    literal matches.

    Returns:
        (violations, unmatched_exemptions)

        violations: list of dicts {file, line, snippet} for matches
            NOT covered by EXEMPTIONS.
        unmatched_exemptions: list of (file, snippet) pairs from
            EXEMPTIONS that didn't match any source — i.e., the file
            was edited and the literal moved/removed. Cleanup PRs
            should remove the exemption when this happens.
    """
    violations: List[dict] = []
    matched_exemption_idxs = set()
    for d in SCAN_DIRS:
        scan_root = repo_root / d
        if not scan_root.exists():
            continue
        for py in sorted(scan_root.glob("*.py")):
            rel = py.relative_to(repo_root).as_posix()
            try:
                src = py.read_text(encoding="utf-8")
            except Exception as e:
                violations.append({"file": rel, "line": 0,
                                   "snippet": f"<read error: {e}>"})
                continue
            scrubbed = _strip_comments(src)
            for m in _WHERE_STATUS_IN.finditer(scrubbed):
                snippet = m.group(0)
                # Look up exemption by (file, substring).
                exempt = False
                for i, (efile, esubstr) in enumerate(EXEMPTIONS):
                    if efile == rel and esubstr in snippet:
                        matched_exemption_idxs.add(i)
                        exempt = True
                        break
                if exempt:
                    continue
                line_no = scrubbed[:m.start()].count("\n") + 1
                violations.append({
                    "file": rel,
                    "line": line_no,
                    "snippet": snippet[:140],
                })

    unmatched = [EXEMPTIONS[i] for i in range(len(EXEMPTIONS))
                 if i not in matched_exemption_idxs]
    return violations, unmatched


def main(argv: List[str]) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    violations, stale_exemptions = find_violations(repo_root)

    rc = 0
    if violations:
        rc = 1
        print("FAIL — inline `WHERE status IN (...)` literals in route modules:")
        print()
        for v in violations:
            print(f"  {v['file']}:{v['line']}")
            print(f"    {v['snippet']}")
        print()
        print("Status acceptance must route through")
        print("  `src.core.status_taxonomy.is_valid_status_for()`")
        print("Queue filters must route through")
        print("  `src.core.canonical_state.is_active_queue()` etc.")
        print()
        print("If this is intentional and the table has a different")
        print("state machine (e.g. email_outbox), add to EXEMPTIONS")
        print("in tools/lint_inline_status_whitelists.py with a comment.")
        print()

    if stale_exemptions:
        rc = 1
        print("FAIL — stale exemptions (literal no longer present):")
        print()
        for efile, esubstr in stale_exemptions:
            print(f"  {efile}")
            print(f"    {esubstr}")
        print()
        print("Remove the matching entry from EXEMPTIONS in")
        print("tools/lint_inline_status_whitelists.py — the literal")
        print("was either cleaned up (good!) or moved (also clean up).")
        print()

    if rc == 0:
        print("OK — no unexempted inline status whitelists found")
        print(f"     (tracking {len(EXEMPTIONS)} known exemptions)")
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv))
