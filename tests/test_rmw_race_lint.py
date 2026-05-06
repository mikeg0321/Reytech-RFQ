"""Codebase-wide ratcheting lint: every user-facing POST handler that
does load → mutate → save must hold the appropriate save lock.

This is the substrate-level enforcement of the rule that PR #778 /
PR #779 / PR #781 closed for individual handlers. Rather than
re-discovering this bug class one handler at a time, the build now
enforces the invariant.

**Ratcheting baseline:** The 2026-05-06 audit found 95 existing
violations. Fixing all 95 in one PR would be unreviewable. Instead,
this test:

  - Locks the current violation set as a frozen baseline.
  - Fails CI if a NEW violation appears (someone added a handler with
    the bad shape).
  - Fails CI if a fix is shipped without removing it from the baseline
    (positive ratchet — the list can only shrink, never grow).

To fix a violation:
  1. Wrap the load+mutate+save in `with _save_pcs_lock:` /
     `with _save_rfqs_lock:`. See PR #778 for the pattern.
  2. Remove the entry from KNOWN_VIOLATIONS below.
  3. Run `pytest tests/test_rmw_race_lint.py` — must stay green.

To add a new exempt handler (RARE — e.g. delegates to function that
manages its own atomicity):
  1. Add to KNOWN_EXEMPTIONS with a one-line comment justifying.
  2. The exemption skips the handler entirely (won't appear in
     baseline either).
"""
from __future__ import annotations

import os
import re
from pathlib import Path


# Handlers exempt from the lint (delegate to atomic-managed helpers,
# or are read-only-with-incidental-save patterns). Add justification.
KNOWN_EXEMPTIONS: set[str] = {
    # Delegates to enrich_pc which manages its own load/save loop with
    # status-dict TTL eviction. Wrapping the outer endpoint in
    # `_save_pcs_lock` would block PC autosaves for 10+ seconds during
    # web-search calls — net negative for operator latency.
    "api_pc_retry_auto_price",
}

# Frozen baseline of known violations as of PR #778-#783 substrate session.
# This list is the WORK QUEUE for incremental RMW race cleanup. Each
# (filename, function_name) tuple is a handler that needs the lock wrap.
# Remove entries as they're fixed — adding entries is forbidden by CI.
KNOWN_VIOLATIONS: frozenset[tuple[str, str]] = frozenset({
    ("routes_analytics.py", "rfq_auto_lookup"),
    ("routes_analytics.py", "apply_recommendations"),
    ("routes_analytics.py", "send_follow_up"),
    ("routes_analytics.py", "link_pc_to_rfq"),
    ("routes_analytics.py", "reclassify_pc_as_rfq"),
    ("routes_analytics.py", "rfq_relink_pc"),
    ("routes_analytics.py", "api_pcs_list"),
    ("routes_analytics.py", "api_rfq_import_from_pc"),
    ("routes_analytics.py", "api_rfq_import_from_catalog"),
    ("routes_analytics.py", "api_rfq_upload_pc"),
    ("routes_cchcs_packet.py", "api_cchcs_packet_generate"),
    ("routes_cchcs_packet.py", "api_cchcs_packets_backfill"),
    ("routes_rfq.py", "api_award_approve"),
    ("routes_rfq.py", "api_rfq_upload_parse_doc"),
    ("routes_rfq.py", "api_bind_email"),
    ("routes_rfq.py", "api_create_draft"),
    ("routes_rfq.py", "api_discard_draft"),
    ("routes_rfq.py", "api_lookup_tax_rate"),
    ("routes_rfq.py", "api_rfq_auto_price"),
    ("routes_rfq.py", "api_rfq_confirm_pc_link"),
    ("routes_rfq_gen.py", "api_rfq_screenshot_confirm"),
    ("routes_rfq_gen.py", "api_rfq_unlink_pc"),
    ("routes_rfq_gen.py", "rfq_lookup_single_item"),
    ("routes_rfq_gen.py", "rfq_upload_supplier_quote"),
    ("routes_rfq_gen.py", "api_rfq_manual_submit_704b"),
    ("routes_rfq_gen.py", "api_rfq_manual_submit_clear"),
    ("routes_rfq_gen.py", "api_rfq_submit_edited_quote"),
    ("routes_rfq_gen.py", "api_rfq_submit_edited_quote_clear"),
    ("routes_rfq_gen.py", "api_rfq_contract_upload"),
    ("routes_rfq_gen.py", "api_rfq_dismiss"),
    ("routes_rfq_gen.py", "api_rfq_cancel"),
    ("routes_rfq_gen.py", "api_rfq_reactivate"),
})


def _split_into_functions(src: str) -> list[tuple[str, str]]:
    """Yield (function_name, function_body) pairs for top-level defs."""
    out = []
    matches = list(re.finditer(r"^def ([a-z_][a-z0-9_]*)\(", src, re.MULTILINE))
    for i, m in enumerate(matches):
        name = m.group(1)
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(src)
        out.append((name, src[start:end]))
    return out


def _is_route_handler(body: str) -> bool:
    """Detect a top-level Flask route handler.

    Excludes:
      - `_locked` private helpers from the wrapper-rename pattern
        (PR #778). The outer wrapper holds the lock; the inner body
        runs under it. Linting the inner body for "lock not present"
        would be a false positive — the lock is in the wrapper.
    """
    first_line = body.splitlines()[0] if body else ""
    # Wrapper-rename pattern: `def _<name>_locked(...)` is the inner
    # body of a wrapped handler, not a standalone route.
    if first_line.startswith("def _") and "_locked(" in first_line:
        return False
    return (
        ("pcid" in first_line or "rid" in first_line or "(self" not in first_line)
        and ("return jsonify" in body or "request.get_json" in body)
    )


def _find_violations() -> set[tuple[str, str]]:
    """Scan routes_*.py and return the set of (filename, function_name)
    tuples that have unsafe load+mutate+save without the appropriate lock."""
    routes_dir = Path(__file__).parent.parent / "src" / "api" / "modules"
    out: set[tuple[str, str]] = set()
    for py_file in sorted(routes_dir.glob("routes_*.py")):
        text = py_file.read_text(encoding="utf-8")
        for name, body in _split_into_functions(text):
            if name in KNOWN_EXEMPTIONS:
                continue
            if not _is_route_handler(body):
                continue
            loads_pc = "_load_price_checks(" in body
            saves_pc = "_save_single_pc(" in body
            if loads_pc and saves_pc and "_save_pcs_lock" not in body:
                out.add((py_file.name, name))
                continue
            loads_rfq = "load_rfqs(" in body
            saves_rfq = "_save_single_rfq(" in body
            if loads_rfq and saves_rfq and "_save_rfqs_lock" not in body:
                out.add((py_file.name, name))
    return out


def test_no_new_rmw_race_handlers():
    """No NEW handlers may have the load+mutate+save race shape.

    Existing handlers are listed in KNOWN_VIOLATIONS as the cleanup
    backlog. Adding a handler not in that list (i.e., a new violation)
    fails this test. Removing one (i.e., shipping a fix) without also
    deleting the entry from KNOWN_VIOLATIONS also fails — keeps the
    backlog accurate."""
    found = _find_violations()
    new_violations = found - KNOWN_VIOLATIONS
    fixed_but_not_removed = KNOWN_VIOLATIONS - found

    msgs = []
    if new_violations:
        msgs.append(
            "NEW RMW race violations introduced (handlers added without "
            "save lock — see PR #778 for the pattern):"
        )
        for f, n in sorted(new_violations):
            msgs.append(f"  + {f}::{n}")

    if fixed_but_not_removed:
        msgs.append(
            "\nViolations FIXED but still listed in KNOWN_VIOLATIONS — "
            "delete these entries from the test to record progress:"
        )
        for f, n in sorted(fixed_but_not_removed):
            msgs.append(f"  - {f}::{n}")

    if msgs:
        msgs.insert(0, "")
        raise AssertionError("\n".join(msgs))


def test_baseline_violations_count():
    """Watchful info — surfaces the cleanup backlog size in test output."""
    backlog = len(KNOWN_VIOLATIONS)
    assert backlog <= 95, (
        f"Backlog should ratchet down from 95 to 0 over time — got {backlog}. "
        f"If you've added entries, that's a regression: every new handler "
        f"must hold the save lock."
    )
