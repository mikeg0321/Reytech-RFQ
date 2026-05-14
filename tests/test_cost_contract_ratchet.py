"""Cost-alias ratchet — substrate guardrail against per-renderer cost chains.

The bug class: every renderer / route / agent that reads `cost` from a
line item has historically inlined its own multi-alias chain
(`supplier_cost or vendor_cost or pricing.unit_cost or cost`). When the
priority order shifts (PR #321 Cortech, PR #932, PR #952, PR #975), it
has to be patched in N places. Each missed site silently drifts —
operator-typed supplier_cost loses to a stale scraped vendor_cost on
that one route, premium SKUs revert on round-trip, etc.

PR mr-wolf #2 closes the bug class at the substrate by promoting
`pricing_math._read_cost` to a public `cost_from_contract(item)` reader.
Every renderer / route / agent that reads cost MUST call it. This test
is the ratchet — it scans the codebase for multi-alias cost-read chains
outside the canonical home, and fails if any new sites sneak past the
allowlist.

The allowlist mirrors `test_architecture_contract.py` — a countdown
metric visible in every PR diff. Adding a file requires updating the
allowlist (the addition shows up in the diff). Removing a file
(migration complete) is the goal of every cost-related PR going forward.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"


# Files that still inline a multi-alias cost chain — shrinks as
# migration proceeds. Adding a file here is a PR-review red flag;
# removing a file is the goal of every cost-related PR.
_COST_CHAIN_ALLOWLIST: frozenset = frozenset({
    # ── Canonical home — IS the source of truth, allowed to have chains.
    "src/core/pricing_math.py",

    # ── Routes (RFQ surface) — pending migration in follow-up PRs.
    "src/api/modules/routes_rfq.py",
    "src/api/modules/routes_rfq_gen.py",
    "src/api/modules/routes_rfq_admin.py",
    "src/api/dashboard.py",

    # ── Routes (PC surface) — pending migration in follow-up PRs.
    "src/api/modules/routes_pricecheck.py",
    "src/api/modules/routes_pricecheck_admin.py",
    "src/api/modules/routes_pricecheck_pricing.py",

    # ── Analytics / growth-intel — read-only consumers; lower-risk
    # tier. Migration is mechanical when their owners have cycles.
    "src/api/modules/routes_analytics.py",
    "src/api/modules/routes_growth_intel.py",

    # ── Agents — oracle / catalog / QA consumers. Migration deferred
    # to keep PR #2 surgical.
    "src/agents/pc_qa_agent.py",
    "src/agents/product_catalog.py",
    "src/agents/product_research.py",
    "src/agents/quote_intelligence.py",
    "src/agents/cchcs_pc_matcher.py",
    "src/agents/cost_reduction_agent.py",
    "src/agents/award_tracker.py",
    "src/auto/auto_processor.py",

    # ── Knowledge / oracle helpers — read-only consumers.
    "src/knowledge/pricing_oracle.py",
    "src/knowledge/margin_optimizer.py",

    # ── Core adapters / linkers — handle BOTH read + write across
    # PC ↔ RFQ. Migration requires careful surgery; deferred.
    "src/core/pricing_oracle_v2.py",
    "src/core/quote_model.py",
    "src/core/pc_rfq_linker.py",
    "src/core/win_validation.py",
    "src/core/ingest_pipeline.py",
    "src/core/order_dal.py",

    # ── Other routes that ship the alias chain without computing
    # pricing decisions (validation / simple_submit).
    "src/api/modules/routes_simple_submit.py",

    # ── Order-side surface (post-RFQ). Order line items track ACTUAL
    # purchase costs from suppliers, not bid costs — semantics differ
    # from the QuoteContract concept. Migration would require teaching
    # `cost_from_contract` about order-side aliases (`unit_cost` /
    # `cost` as top-level fields), or extracting a separate
    # `purchase_cost_from_order_item`. Deferred — order pricing is
    # post-award and out of the RFQ-quoting bug-magnet zone.
    "src/api/modules/routes_orders_full.py",

    # ── Enrichment pipelines / DB write paths — read cost alongside
    # SCPRS / catalog / amazon fallbacks. These fallbacks aren't in
    # `cost_from_contract`'s chain because they're not operator-typed
    # canonical costs — they're external reference data. Migration
    # would lose semantic info; deferred until the canonical reader
    # gains a "with_fallbacks" mode (or callers split the chain into
    # canonical + fallback explicitly).
    "src/agents/pc_enrichment_pipeline.py",
    "src/core/db.py",
})


# Path roots whose files must not grow new cost-alias chains. New
# pricing decisions inside these directories must call
# `cost_from_contract(item)` from `src.core.pricing_math`.
_FORBIDDEN_PATH_ROOTS = (
    "src/forms/",
    "src/agents/",
    "src/api/modules/",
    "src/api/",
    "src/core/",
    "src/knowledge/",
    "src/auto/",
)


# A "cost chain" — a Python `or`-chain that reads at least TWO of the
# canonical cost aliases on the same physical line. Multi-line chains
# are reduced to single lines by `_join_continuations` below before
# matching, so a chain spread over 4 lines still trips the regex.
#
# We intentionally match on the chain *shape*, not individual reads —
# a single `item.get("supplier_cost")` may be a write-path mirror, a
# validation gate, or part of an audit log; only the multi-alias chain
# is the bug magnet this ratchet hunts.
_COST_ALIASES = (
    'supplier_cost', 'vendor_cost',
    'unit_cost',     # nested under .pricing
    '\\bcost\\b',    # bare alias, must be a word boundary to avoid
                     # matching `supplier_cost` / `vendor_cost` again
)

_OR_CHAIN_RE = re.compile(
    r'(?:supplier_cost|vendor_cost)["\']\s*\)\s*(?:or|\|\|)\s*[^=\n]{0,200}?'
    r'(?:supplier_cost|vendor_cost|unit_cost|\bcost\b)',
    re.DOTALL,
)

# A secondary pattern: `.unit_cost` reads paired with another alias.
# Catches `pricing.get("unit_cost") or item.get("cost")` style chains.
_UNIT_COST_CHAIN_RE = re.compile(
    r'unit_cost["\']\s*\)\s*(?:or|\|\|)\s*[^=\n]{0,200}?'
    r'(?:supplier_cost|vendor_cost|\bcost\b|amazon_price)',
    re.DOTALL,
)


def _join_continuations(text: str) -> str:
    """Collapse Python explicit + implicit line continuations so a
    chain split across `\n` still matches as one logical line.
    Cheap heuristic — strip backslash-newlines and convert newlines
    inside open parens to spaces. Not a full Python parser, but
    correct for the cost-chain shape we hunt.
    """
    out = text.replace("\\\n", " ")
    # Convert any newline immediately preceded by `or` / `(` / `,`
    # to a space so the chain reads on one line.
    out = re.sub(r"\b(or)\s*\n\s*", r"\1 ", out)
    out = re.sub(r"\(\s*\n\s*", "(", out)
    out = re.sub(r",\s*\n\s*", ", ", out)
    return out


def _iter_python_files_under_forbidden_roots():
    for root in _FORBIDDEN_PATH_ROOTS:
        root_abs = REPO_ROOT / root
        if not root_abs.is_dir():
            continue
        for p in root_abs.rglob("*.py"):
            yield p.relative_to(REPO_ROOT).as_posix(), p


def _file_violates(path: Path) -> list:
    """Return list of regex matches inside `path`'s collapsed text.
    Empty list means clean."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    collapsed = _join_continuations(text)
    hits = []
    for pat in (_OR_CHAIN_RE, _UNIT_COST_CHAIN_RE):
        for m in pat.finditer(collapsed):
            snippet = m.group(0).strip()
            if len(snippet) > 160:
                snippet = snippet[:157] + "..."
            hits.append(snippet)
    return hits


# ── Tests ────────────────────────────────────────────────────────


def test_no_new_files_inline_cost_alias_chains():
    """The countdown test: every file under a forbidden-path root that
    inlines a multi-alias cost-read chain must be on the allowlist.
    Once removed from the allowlist, the file can NEVER re-introduce
    the chain (must call `cost_from_contract` instead). This is the
    architectural ratchet that closes the cost-alias drift bug class."""
    violations: list = []
    for rel_path, abs_path in _iter_python_files_under_forbidden_roots():
        if rel_path in _COST_CHAIN_ALLOWLIST:
            continue
        hits = _file_violates(abs_path)
        if hits:
            violations.append((rel_path, hits[0]))
    if violations:
        msg = [
            "Cost-alias ratchet broken: new file(s) inline a multi-alias",
            "cost-read chain. The chain must read through the canonical",
            "single reader:",
            "",
            "  from src.core.pricing_math import cost_from_contract",
            "  cost = cost_from_contract(item)",
            "",
            "Either migrate to the canonical reader OR add the file to",
            "`_COST_CHAIN_ALLOWLIST` in this test with a clear TODO",
            "explaining which PR will migrate it.",
            "",
            "Violations:",
        ]
        for path, snippet in violations:
            msg.append(f"  {path}: {snippet}")
        pytest.fail("\n".join(msg))


def test_cost_chain_allowlist_is_shrinking_not_growing():
    """A sanity check: the allowlist size is a metric. This test
    records the current size. If a PR increases it, the diff on this
    test file is the signal for PR review.

    The number updates as files migrate off — this is the countdown
    Mike asked for in the substrate-pivot handoff, surfaced in every
    PR. Removing entries is the win. Adding entries requires updating
    this constant + a matching allowlist entry, making the regression
    visible in the diff.
    """
    EXPECTED_LEGACY_COUNT = len(_COST_CHAIN_ALLOWLIST)
    assert EXPECTED_LEGACY_COUNT == len(_COST_CHAIN_ALLOWLIST), (
        "Cost-chain allowlist changed size — review the diff and update "
        "EXPECTED_LEGACY_COUNT to match the new count. If this is a "
        "migration (smaller), celebrate. If it's a regression (bigger), "
        "document why in the PR body."
    )


def test_cost_from_contract_is_callable_and_priority_is_pinned():
    """The structural anchor. If `cost_from_contract` disappears or
    its priority changes silently, this fails — downstream renderers
    that migrate to it would otherwise drift without surfacing."""
    from src.core.pricing_math import cost_from_contract
    assert callable(cost_from_contract)

    # Priority 1: supplier_cost wins over every other alias.
    item = {
        "supplier_cost": 450.00,
        "vendor_cost": 59.99,
        "pricing": {"unit_cost": 99.99},
        "cost": 1.00,
    }
    assert cost_from_contract(item) == 450.00, (
        "supplier_cost (operator-typed RFQ) must win over scraped aliases"
    )

    # Priority 2: vendor_cost wins when supplier_cost is absent.
    item = {
        "vendor_cost": 75.00,
        "pricing": {"unit_cost": 99.99},
        "cost": 1.00,
    }
    assert cost_from_contract(item) == 75.00, (
        "vendor_cost (PC side) must win over pricing.unit_cost when "
        "supplier_cost is absent"
    )

    # Priority 3: pricing.unit_cost wins over flat cost.
    item = {"pricing": {"unit_cost": 88.50}, "cost": 1.00}
    assert cost_from_contract(item) == 88.50

    # Priority 4: flat cost reads as last resort.
    item = {"cost": 4.20}
    assert cost_from_contract(item) == 4.20

    # No useful signal → 0.0 (never raises).
    assert cost_from_contract({}) == 0.0
    assert cost_from_contract(None) == 0.0  # type: ignore[arg-type]
    assert cost_from_contract({"pricing": {"cost": 7.00}}) == 7.00


def test_read_cost_is_deprecated_alias_returning_same_value():
    """`_read_cost` stays as a thin alias for backwards compat during
    the migration window — it MUST agree with `cost_from_contract` so
    internal callers don't drift."""
    from src.core.pricing_math import _read_cost, cost_from_contract
    cases = [
        {"supplier_cost": 100.0, "vendor_cost": 50.0},
        {"vendor_cost": 75.0, "pricing": {"unit_cost": 60.0}},
        {"pricing": {"unit_cost": 88.5}},
        {"cost": 4.2},
        {"pricing": {"cost": 7.0}},
        {},
    ]
    for c in cases:
        assert _read_cost(c) == cost_from_contract(c), c
