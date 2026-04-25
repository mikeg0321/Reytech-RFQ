"""Classify-agency facade — PR-A of the institution_resolver fold.

Per product-engineer review 2026-04-25:

> The 9 callers split into 3 functional groups, NOT one. The "swap one
> import line" framing breaks at the level of the API. Build 3 facades
> in `quote_contract` (`canonical_name`, `same_institution`,
> `classify_agency`), each delegating to `institution_resolver`
> internally. Add a negative-allowlist test. Zero caller migration in
> PR-A. This is the seam.

This file pins:

  1. The 3 facades return what callers expect (shape + semantics)
  2. The latent broken `resolve_agency` import in `routes_pricecheck.py`
     stays fixed — it lived for an unknown number of weeks silently
     no-opping the SCPRS staleness banner because `resolve_agency` has
     never been a name in institution_resolver
  3. The negative-allowlist that bounds NEW direct imports of
     institution_resolver — current 9 callers are whitelisted, any
     additional file fails CI
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.core.quote_contract import (
    canonical_name,
    classify_agency,
    same_institution,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"


# ── facade behavior ──────────────────────────────────────────────────


class TestCanonicalName:
    """`canonical_name(text)` — label normalizer. Returns input on miss
    so downstream aggregates don't silently lose un-canonicalizable
    strings (matches `institution_resolver.normalize` semantics)."""

    def test_known_facility_normalizes(self):
        assert canonical_name("CSP-SAC") != "CSP-SAC"  # gets expanded form

    def test_unknown_text_returns_unchanged(self):
        out = canonical_name("Some Random Buyer LLC")
        assert out == "Some Random Buyer LLC"

    def test_empty_input_returns_empty(self):
        assert canonical_name("") == ""
        assert canonical_name(None) == ""

    def test_whitespace_only_returns_unchanged(self):
        assert canonical_name("   ") == "   "


class TestSameInstitution:
    """`same_institution(a, b)` — comparator used by pc_rfq_linker.
    Preserves the lossy "same agency without facility code = match"
    branch that direct facility_registry.resolve()-and-compare would
    silently drop."""

    def test_identical_codes_match(self):
        assert same_institution("CSP-SAC", "CSP-SAC")

    def test_alias_and_code_match(self):
        # Both resolve to CSP-SAC
        assert same_institution(
            "CSP-SAC",
            "California State Prison, Sacramento",
        )

    def test_different_facilities_dont_match(self):
        assert not same_institution("CSP-SAC", "CIM")

    def test_blank_inputs_dont_match(self):
        assert not same_institution("", "CSP-SAC")
        assert not same_institution("CSP-SAC", "")
        assert not same_institution("", "")


class TestClassifyAgency:
    """`classify_agency(name, email, ship_to)` — 3-input fallback used
    by routes_pricecheck SCPRS staleness check + ingest classification.
    Returns the same dict shape institution_resolver.resolve does."""

    def test_returns_full_dict_shape(self):
        out = classify_agency("CDCR")
        assert isinstance(out, dict)
        for key in ("canonical", "agency", "facility_code", "original",
                    "source"):
            assert key in out

    def test_known_agency_resolves(self):
        out = classify_agency("CSP-SAC")
        # Whatever institution_resolver returns for agency, it must be
        # non-empty for a real facility name.
        assert out.get("agency", "")

    def test_email_domain_fallback(self):
        # raw_name unknown, but email domain pins agency
        out = classify_agency("Unknown Buyer", email="bob@cdcr.ca.gov")
        assert out.get("agency", "")  # CDCR via email

    def test_all_empty_returns_empty_dict(self):
        out = classify_agency()
        assert out["canonical"] == ""
        assert out["agency"] == ""
        assert out["facility_code"] == ""

    def test_garbage_label_does_not_match_facility(self):
        # "Delivery" is in _GARBAGE_NAMES — must not silently resolve
        out = classify_agency("Delivery")
        assert out.get("facility_code", "") == ""


# ── latent-bug regression guard ─────────────────────────────────────


def test_routes_pricecheck_no_resolve_agency_import():
    """The 2026-04-25 fix — `resolve_agency` has never existed in
    `institution_resolver`. The import lived inside try/except so it
    silently no-op'd the SCPRS staleness banner. This test fires if a
    future PR re-introduces the broken name.

    Only checks NON-COMMENT lines so the historical fix-up comment in
    routes_pricecheck.py (which documents the bug for future readers)
    doesn't trigger a false positive.
    """
    pc_routes = SRC / "api" / "modules" / "routes_pricecheck.py"
    text = pc_routes.read_text(encoding="utf-8", errors="replace")
    bad = re.compile(
        r"^\s*from\s+src\.core\.institution_resolver\s+import\s+[^#\n]*\bresolve_agency\b"
    )
    offenders = [
        f"  line {i}: {line.rstrip()}"
        for i, line in enumerate(text.splitlines(), start=1)
        if not line.lstrip().startswith("#") and bad.search(line)
    ]
    assert not offenders, (
        "routes_pricecheck.py is importing `resolve_agency` from "
        "institution_resolver — that name does not exist; use "
        "`from src.core.quote_contract import classify_agency` and "
        "read `result['agency']` instead.\n" + "\n".join(offenders)
    )


def test_classify_agency_facade_used_in_routes_pricecheck():
    """Positive guard: the SCPRS staleness path actually goes through
    the facade. Catches a future "revert to direct institution_resolver
    import" regression in this hot path."""
    pc_routes = SRC / "api" / "modules" / "routes_pricecheck.py"
    text = pc_routes.read_text(encoding="utf-8", errors="replace")
    assert "from src.core.quote_contract import classify_agency" in text


# ── negative-allowlist ratchet ──────────────────────────────────────


# Files that today still import directly from institution_resolver.
# Per product-engineer review: this is a NEGATIVE allowlist (bound the
# blast radius), NOT a countdown ratchet (forcing migration of code
# that works = volume-over-outcome anti-pattern). New files fail; the
# current 9 are grandfathered until Mike sees a real bug there.
_INSTITUTION_RESOLVER_DIRECT_IMPORT_ALLOWLIST = frozenset({
    "src/core/institution_resolver.py",   # the module itself
    "src/core/quote_contract.py",         # the facade is allowed to import
    "src/core/pc_rfq_linker.py",
    "src/core/ingest_pipeline.py",
    "src/api/modules/routes_pricecheck.py",
    "src/api/modules/routes_crm.py",
    "src/api/modules/routes_analytics.py",
    "src/api/dashboard.py",
    "src/agents/growth_agent.py",
    "src/agents/email_poller.py",
})


_DIRECT_IMPORT_PATTERNS = (
    re.compile(r"from\s+src\.core\.institution_resolver\s+import"),
    re.compile(r"import\s+src\.core\.institution_resolver"),
)


def _file_imports_institution_resolver(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("#"):
            continue
        for pat in _DIRECT_IMPORT_PATTERNS:
            if pat.search(s):
                return True
    return False


def test_no_new_files_directly_import_institution_resolver():
    """Bound the blast radius of `institution_resolver`. The 10 files
    that already import it are grandfathered (see allowlist above);
    any new file that adds a direct import fails CI. Migrate to the
    `quote_contract` facade (`canonical_name` / `same_institution` /
    `classify_agency` / `ship_to_for_text`) instead."""
    violations = []
    for py in SRC.rglob("*.py"):
        rel = py.relative_to(REPO_ROOT).as_posix()
        if rel in _INSTITUTION_RESOLVER_DIRECT_IMPORT_ALLOWLIST:
            continue
        if _file_imports_institution_resolver(py):
            violations.append(rel)
    if violations:
        pytest.fail(
            "New file(s) directly import `institution_resolver`. Use the "
            "`quote_contract` facade instead (canonical_name / "
            "same_institution / classify_agency / ship_to_for_text). "
            "If you genuinely need to import institution_resolver "
            "directly, add the file to "
            "`_INSTITUTION_RESOLVER_DIRECT_IMPORT_ALLOWLIST` in this "
            "test with a clear PR-body explanation. New violators:\n  "
            + "\n  ".join(violations)
        )


def test_allowlist_has_no_stale_entries():
    """If a file gets removed or its institution_resolver import gets
    deleted, the allowlist entry becomes dead weight. Surface it so the
    dead row gets pruned in the same PR."""
    stale = []
    for rel in _INSTITUTION_RESOLVER_DIRECT_IMPORT_ALLOWLIST:
        p = REPO_ROOT / rel
        if not p.exists():
            stale.append(f"{rel} (file does not exist)")
            continue
        # The file `quote_contract.py` doesn't actually import
        # institution_resolver at module-level (uses lazy imports
        # inside facades). Allow it through without import-line check.
        if rel == "src/core/quote_contract.py":
            continue
        if rel == "src/core/institution_resolver.py":
            continue
        if not _file_imports_institution_resolver(p):
            stale.append(f"{rel} (no longer imports institution_resolver)")
    if stale:
        pytest.fail(
            "Stale allowlist entries — remove from "
            "`_INSTITUTION_RESOLVER_DIRECT_IMPORT_ALLOWLIST`:\n  "
            + "\n  ".join(stale)
        )
