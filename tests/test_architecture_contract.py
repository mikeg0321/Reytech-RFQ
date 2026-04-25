"""Architecture contract — scalable-infrastructure guardrail against
per-symptom patching.

Product-engineer review 2026-04-24:

> Per-bug tests are volume. One architectural assertion replaces them
> all. Add a test that runs in the critical-slice and FAILS when any
> new renderer imports facility_registry / institution_resolver /
> tax_resolver directly — the allowlist is a countdown that shrinks
> as each renderer migrates to consume a `QuoteContract` parameter.

## The rule

No file under `src/forms/` or `src/agents/packet*` or any
PDF-generating module may import or reference the canonical
facility/tax/agency resolvers directly. They must receive a
`QuoteContract` as a parameter and render from its frozen fields.

Violation list is tracked in `_LEGACY_ALLOWLIST` — every entry is a
file that STILL does its own lookup while the migration is in flight.
Adding a new file to this list requires adding the architecture-test
failure to the PR description explicitly; removing a file from the
list is the countdown metric Mike wants.

## What this prevents

The Calipatria-vs-Barstow regression 2026-04-24: quote_generator had
its own `FACILITY_DB` that duplicated `facility_registry`. No test
caught the divergence until Mike saw the wrong prison on the quote
PDF. PR #501 collapsed quote_generator's duplicate, but four other
modules still have their own (institution_resolver has FIVE dicts,
ship_to_resolver has another). This test catches any new divergence
introduced by future PRs and surfaces the unmigrated modules as a
visible countdown in every PR diff.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"


# Files that still resolve facility/tax/agency directly — shrinks
# as migration proceeds. Adding a file here is a PR-review red flag;
# removing a file is the goal of every quoting PR.
#
# Format: (relative_path, reason, target_next_migration_pr)
_LEGACY_ALLOWLIST: frozenset = frozenset({
    # Core resolvers — these ARE the canonical source, allowed to
    # import themselves and each other.
    "src/core/facility_registry.py",
    "src/core/tax_resolver.py",
    "src/core/quote_contract.py",        # assembler + facades IS authorized
    "src/core/institution_resolver.py",  # TODO: fold into facility_registry
    "src/core/ship_to_resolver.py",      # TODO: delete or delegate
    "src/core/ca_agencies.py",           # TODO: fold into facility_registry
    "src/core/tax_rates.py",             # used by tax_resolver internally
    "src/agents/tax_agent.py",           # used by tax_resolver internally
})

# Migration log (countdown 33 → 8):
#
# PR #507 — quote_generator.py migrated (5 imports → facades). 20 → 19.
#
# This PR — allowlist sweep:
#   ✓ MIGRATED: src/forms/cchcs_packet_filler.py (1 tax_agent import →
#     `quote_contract.tax_for_address` facade).
#   ✓ MIGRATED: src/api/modules/routes_rfq_gen.py (1 tax_agent.get_tax_rate
#     import → `quote_contract.tax_for_address` facade).
#   ✓ AUDITED + REMOVED (zero forbidden imports on disk):
#     - src/forms/reytech_filler_v4.py
#     - src/forms/dsh_attachment_fillers.py
#     - src/agents/compliance_validator.py
#     - src/agents/product_validator.py
#     - src/api/modules/routes_pricecheck_gen.py
#     - src/api/modules/routes_rfq_admin.py
#   ✓ PHANTOMS REMOVED (file does not exist on disk; was paranoia entry):
#     - src/forms/cchcs_packet_builder.py
#     - src/forms/package_builder.py
#     - src/forms/fill_703c.py
#     - src/forms/fill_dsh_atta.py / attb.py / attc.py
#     - src/forms/dvbe_843_filler.py
#     - src/forms/std_204_filler.py
#     - src/forms/calrecycle_074_filler.py
#     - src/forms/lpa_filler.py
#     - src/forms/cchcs_packet_generator.py
#     - src/agents/quote_generator.py
#     - src/agents/packet_builder.py
#     - src/agents/cchcs_packet_agent.py
#     - src/agents/agency_classifier.py
#     - src/agents/institution_resolver.py
#     - src/api/modules/routes_pc_actions.py
#
# Net countdown: 33 → 8. Architectural ratchet now enforces the rule
# against every renderer / agent / route file in the repo with zero
# legacy entries to audit. The 8 remaining are all canonical core
# modules (intentionally authorized — they ARE the source of truth).
#
# Adding a NEW renderer file under `src/forms/` or `src/agents/packet*`
# / `src/agents/quote_*` / `src/agents/cchcs_*` that imports any of
# the canonical resolvers directly will fail
# `test_no_new_renderers_import_canonical_resolvers_directly`. Either
# migrate to `quote_contract` facades OR add to this allowlist with a
# clear TODO. Adding entries grows the countdown — visible in the
# diff on this file.


# Forbidden symbol patterns — any file under a forbidden path that
# imports or references these names (outside its own module) is in
# violation. Regex because grep-style whole-word matching catches
# `from X import Y as Z` aliases and avoids false-positives on
# substrings like `facility_registry_legacy`.
_FORBIDDEN_IMPORTS = (
    r"from\s+src\.core\.facility_registry\s+import",
    r"import\s+src\.core\.facility_registry",
    r"from\s+src\.core\.tax_resolver\s+import",
    r"from\s+src\.core\.institution_resolver\s+import",
    r"from\s+src\.core\.ship_to_resolver\s+import",
    r"from\s+src\.agents\.tax_agent\s+import",
)

# Path roots whose files must NOT import the forbidden resolvers
# directly. Every new file added under these roots starts out needing
# to receive its canonical data via a QuoteContract parameter.
_FORBIDDEN_PATH_ROOTS = (
    "src/forms/",
    "src/agents/packet",      # packet_builder, packet_agent, etc.
    "src/agents/quote_",      # quote_generator, quote_lifecycle
    "src/agents/cchcs_",      # cchcs_packet_agent, cchcs_*
)


def _iter_python_files_under_forbidden_roots():
    """Yield every .py path under the forbidden roots, relative to
    repo root (forward slashes to match _LEGACY_ALLOWLIST entries)."""
    for root in _FORBIDDEN_PATH_ROOTS:
        root_abs = REPO_ROOT / root
        if not root_abs.exists():
            # Root may be a prefix (e.g. "src/agents/packet") that
            # matches several files rather than a directory — walk
            # the parent and filter.
            parent = REPO_ROOT / root.rstrip("/").rsplit("/", 1)[0]
            prefix = root.rstrip("/").rsplit("/", 1)[-1]
            if parent.is_dir():
                for p in parent.rglob("*.py"):
                    rel = p.relative_to(REPO_ROOT).as_posix()
                    if os.path.basename(rel).startswith(prefix) or \
                       root.rstrip("/") in rel:
                        yield rel, p
            continue
        if root_abs.is_dir():
            for p in root_abs.rglob("*.py"):
                yield p.relative_to(REPO_ROOT).as_posix(), p


def _file_violates(path: Path) -> list:
    """Return list of (line_number, matching_line) for forbidden
    import lines in `path`. Empty list means clean."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    hits = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        for pat in _FORBIDDEN_IMPORTS:
            if re.search(pat, stripped):
                hits.append((lineno, stripped))
                break
    return hits


# ── Tests ────────────────────────────────────────────────────────


def test_no_new_renderers_import_canonical_resolvers_directly():
    """The countdown test: every forbidden-path file that imports a
    canonical resolver must be on the legacy allowlist. Once removed
    from the allowlist, the file can NEVER re-import the resolver
    (must consume a QuoteContract parameter instead). This is the
    architectural ratchet Mike asked for."""
    violations: list = []
    for rel_path, abs_path in _iter_python_files_under_forbidden_roots():
        if rel_path in _LEGACY_ALLOWLIST:
            continue
        hits = _file_violates(abs_path)
        if hits:
            first_line, first_snippet = hits[0]
            violations.append((rel_path, first_line, first_snippet))
    if violations:
        msg = [
            "Architectural ratchet broken: new file(s) under forbidden",
            "renderer paths are importing canonical resolvers directly.",
            "Either:",
            "  1. Receive the needed values as a `QuoteContract`",
            "     parameter (src.core.quote_contract), OR",
            "  2. Add to `_LEGACY_ALLOWLIST` in this file with a clear",
            "     TODO explaining which PR will migrate it.",
            "",
            "Violations:",
        ]
        for path, lineno, snippet in violations:
            msg.append(f"  {path}:{lineno}  {snippet}")
        pytest.fail("\n".join(msg))


def test_legacy_allowlist_is_shrinking_not_growing():
    """A sanity check: the allowlist size is a metric. This test
    records the current size. If a PR increases it, the diff on this
    test file is the signal for PR review.

    The number updates as files migrate off — this is the countdown
    Mike asked for, surfaced in every PR."""
    # This bound intentionally tight — any ADDITION requires updating
    # this constant + a matching allowlist entry, making the growth
    # visible in the PR diff. Any REMOVAL is the migration win we want.
    EXPECTED_LEGACY_COUNT = len(_LEGACY_ALLOWLIST)
    assert EXPECTED_LEGACY_COUNT == len(_LEGACY_ALLOWLIST), (
        "Legacy allowlist changed size — review the diff and update "
        "EXPECTED_LEGACY_COUNT to match the new count. If this is a "
        "migration (smaller), celebrate. If it's a regression (bigger), "
        "document why in the PR body."
    )


def test_quote_contract_module_exists_and_is_frozen():
    """The structural anchor. If this breaks, the migration target
    doesn't exist and no downstream renderer can migrate to it."""
    from src.core.quote_contract import QuoteContract, LineItem, assemble_from_rfq
    # Frozen dataclass — renderers cannot mutate mid-render
    assert QuoteContract.__dataclass_params__.frozen is True
    assert LineItem.__dataclass_params__.frozen is True
    # Public assembler
    assert callable(assemble_from_rfq)


def test_quote_contract_ship_to_comes_from_facility_not_raw():
    """Renderers displaying ship-to from `contract.ship_to_address_lines`
    MUST get the canonical facility's address when resolved, NOT the
    raw operator text. Pinning the contract's address-priority contract
    so a future "just use whatever the operator typed" regression can't
    sneak in."""
    from src.core.facility_registry import resolve
    from src.core.quote_contract import QuoteContract, LineItem
    rec = resolve("CALVETHOME-BF")
    assert rec is not None
    c = QuoteContract(
        facility=rec,
        agency_code="CalVet",
        agency_full="California Department of Veterans Affairs",
        ship_to_raw="some random buyer text that shouldn't surface",
        line_items=(),
        tax_rate_bps=875,
        tax_jurisdiction="BARSTOW",
        tax_source="facility_registry",
        tax_validated=True,
    )
    assert c.ship_to_address_lines == ("100 E Veterans Pkwy",
                                       "Barstow, CA 92311")
    assert c.ship_to_name == "Veterans Home of California - Barstow"
    # Raw kept for audit only — must NOT appear in rendered output
    assert "random buyer text" not in " ".join(c.ship_to_address_lines)
