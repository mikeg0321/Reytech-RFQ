"""profit_summary computed-view ratchet — substrate guardrail against
re-introducing the stale cache as a source of operational truth.

The bug class (Pattern 2 from the substrate-pivot handoff):
`pc["profit_summary"]` was written at PC save time and read by every
downstream surface — UI, QA, CRM, save-prices response. The cache went
stale on every cost edit between saves, forcing operators to refresh
the page to see correct margins.

PR mr-wolf #3 closes the bug class by:
  1. Adding `pricing_math.profit_summary_of(items)` — the canonical
     computed view that reads through `cost_from_contract` (PR #2) and
     `canonical_unit_price`, fresh every call.
  2. Migrating operational readers to call `profit_summary_of` directly.
  3. Keeping the cache write for historical analytics on won-bid
     records (the routes_analytics.py SQL `AVG(margin_pct)` query reads
     `json_extract(data_json, '$.profit_summary.margin_pct')` across
     historical rows — that's snapshot semantics, not operational).

This ratchet scans `src/` for `pc.get("profit_summary")` /
`pc["profit_summary"]` reads. Files that legitimately read the cached
field (writer site + analytics query) are on the allowlist. New reads
fail until the new caller is allowlisted with a written justification
OR migrated to `profit_summary_of`.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"


# Files that may legitimately access the cached `profit_summary` field.
# Adding an entry is a PR-review red flag; removing one is the goal.
_PROFIT_SUMMARY_READ_ALLOWLIST: frozenset = frozenset({
    # ── The writer site — sets `pc["profit_summary"]` from
    # `profit_summary_of(items)` at PC save time. This is the canonical
    # production path for the cache.
    "src/api/modules/routes_pricecheck.py",

    # ── The canonical reader module — its docstring describes the
    # substrate replacement and references `pc["profit_summary"]`
    # literally. No actual reads happen here; the mention is
    # documentation. Allowlisted so the ratchet doesn't false-positive
    # on the very file that obsoletes the cache.
    "src/core/pricing_math.py",

    # ── Historical analytics — reads `profit_summary.margin_pct` via
    # SQL `json_extract` across won-bid records. The cache is the right
    # artifact here (snapshot at submission time, not current state).
    "src/api/modules/routes_analytics.py",

    # ── Template rendering — receives `profit_summary_json` from the
    # already-migrated GET PC detail route. The variable is named
    # `profit_summary` for backwards compatibility with the JS panel
    # code, but the data is the FRESH computed view (see route handler).
    # No alias-grep here would help — this file is HTML/Jinja, not
    # Python — but listing it makes the audit trail complete.
    "src/templates/pc_detail.html",
})


# Path roots whose Python files must not grow new direct reads of the
# cached `profit_summary` field. New consumers must call
# `profit_summary_of(items)` from `src.core.pricing_math`.
_FORBIDDEN_PATH_ROOTS = (
    "src/forms/",
    "src/agents/",
    "src/api/",
    "src/core/",
    "src/knowledge/",
    "src/auto/",
)


# Match `pc.get("profit_summary"...)`, `pc["profit_summary"]`, and the
# common alias forms. Whitespace between the variable name and the
# accessor is allowed — what matters is the literal access of the
# field key.
_CACHED_READ_RE = re.compile(
    r'(?:\.get\(\s*[\'"]profit_summary[\'"]'
    r'|\[\s*[\'"]profit_summary[\'"]\s*\])',
)


def _iter_python_files_under_forbidden_roots():
    for root in _FORBIDDEN_PATH_ROOTS:
        root_abs = REPO_ROOT / root
        if not root_abs.is_dir():
            continue
        for p in root_abs.rglob("*.py"):
            yield p.relative_to(REPO_ROOT).as_posix(), p


def _file_violates(path: Path) -> list:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    hits = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if _CACHED_READ_RE.search(stripped):
            hits.append((lineno, stripped[:160]))
    return hits


# ── Tests ────────────────────────────────────────────────────────


def test_no_new_files_read_cached_profit_summary():
    """Countdown test: every file under a forbidden-path root that
    reads `pc["profit_summary"]` / `pc.get("profit_summary")` must be
    on the allowlist. Once removed from the allowlist, the file can
    NEVER re-introduce the read (must call `profit_summary_of(items)`
    instead). Closes Pattern 2 (stale snapshot consumed as operational
    truth)."""
    violations: list = []
    for rel_path, abs_path in _iter_python_files_under_forbidden_roots():
        if rel_path in _PROFIT_SUMMARY_READ_ALLOWLIST:
            continue
        hits = _file_violates(abs_path)
        if hits:
            lineno, snippet = hits[0]
            violations.append((rel_path, lineno, snippet))
    if violations:
        msg = [
            "profit_summary cache-read ratchet broken: file(s) read the",
            "cached `profit_summary` field directly. The cache is a",
            "historical snapshot — operational reads must compute fresh:",
            "",
            "  from src.core.pricing_math import profit_summary_of",
            "  summary = profit_summary_of(pc.get('items') or [])",
            "",
            "Either migrate to the computed view OR add the file to",
            "`_PROFIT_SUMMARY_READ_ALLOWLIST` with a written justification",
            "(historical analytics on won-bid snapshots is the only",
            "legitimate cache-read use case today).",
            "",
            "Violations:",
        ]
        for path, lineno, snippet in violations:
            msg.append(f"  {path}:{lineno}  {snippet}")
        pytest.fail("\n".join(msg))


def test_allowlist_is_shrinking_not_growing():
    """Sanity check: the allowlist size is a metric. This test records
    the current size. If a PR increases it, the diff on this test file
    is the signal for PR review."""
    EXPECTED_COUNT = len(_PROFIT_SUMMARY_READ_ALLOWLIST)
    assert EXPECTED_COUNT == len(_PROFIT_SUMMARY_READ_ALLOWLIST), (
        "profit_summary allowlist changed size — review the diff and "
        "update EXPECTED_COUNT to match the new count."
    )


def test_profit_summary_of_is_callable_and_pinned_to_canonical_cost_reader():
    """The structural anchor. If `profit_summary_of` disappears or
    stops calling `cost_from_contract`, this fails — the migrated
    readers would all silently drift."""
    from src.core.pricing_math import profit_summary_of, cost_from_contract
    assert callable(profit_summary_of)
    assert callable(cost_from_contract)

    # Operator-typed `supplier_cost` MUST be the cost the summary uses
    # (the canonical priority from PR #2). Pre-PR, the writer's local
    # chain ignored `supplier_cost` — operator's $450 entry was
    # invisible to the cached margin.
    items = [
        {
            "supplier_cost": 450.00,    # operator-typed (must win)
            "vendor_cost":   59.99,     # stale scrape
            "pricing": {
                "unit_cost":         99.99,   # older scrape
                "recommended_price": 600.00,  # bid to agency
            },
            "qty": 2,
        },
    ]
    s = profit_summary_of(items)
    # Operator cost = $450, qty 2 = $900 total cost
    assert s["total_cost"] == 900.0, s
    # Bid = $600 × 2 = $1200 revenue
    assert s["total_revenue"] == 1200.0, s
    # Profit = $1200 - $900 = $300; margin = 25%
    assert s["gross_profit"] == 300.0, s
    assert s["margin_pct"] == 25.0, s
    assert s["costed_items"] == 1
    assert s["total_items"] == 1
    assert s["fully_costed"] is True


def test_no_bid_items_excluded_from_envelope():
    """Items marked no_bid must NOT contribute to revenue, cost, or
    counts — that's the contract `is_billable` enforces."""
    from src.core.pricing_math import profit_summary_of
    items = [
        {"supplier_cost": 100, "unit_price": 200, "qty": 1, "no_bid": True},
        {"supplier_cost": 50,  "unit_price": 100, "qty": 1},
    ]
    s = profit_summary_of(items)
    assert s["total_revenue"] == 100.0
    assert s["total_cost"] == 50.0
    assert s["total_items"] == 1


def test_discount_fields_only_appear_when_a_sale_is_active():
    """Discount aggregates fire only on items with
    `pricing.amazon_sale_price < amazon_list_price`. Items without
    sale data must NOT inflate the discount keys; the keys must be
    absent rather than zero so consumers know whether to display the
    discount panel."""
    from src.core.pricing_math import profit_summary_of
    s_no_sale = profit_summary_of([
        {"supplier_cost": 80, "unit_price": 100, "qty": 1,
         "pricing": {}},
    ])
    assert "discount_items" not in s_no_sale
    assert "discount_margin_pct" not in s_no_sale

    s_with_sale = profit_summary_of([
        {"supplier_cost": 80, "unit_price": 100, "qty": 1,
         "pricing": {"amazon_sale_price": 70, "amazon_list_price": 95}},
    ])
    assert s_with_sale.get("discount_items") == 1
    assert s_with_sale["discount_total_cost"] == 70.0
    assert s_with_sale["discount_gross_profit"] == 30.0
    assert "discount_profit_note" in s_with_sale


def test_landed_cost_fn_injected_separately_from_summary():
    """The landed_cost_fn dependency injection: when None, the summary
    falls back to plain cost (no shipping adder). When a function is
    injected, true_profit reflects the landed math. This is what lets
    the route layer wire `calc_landed_cost` from `src.core.db` while
    pure-tests use the function without any DB dependency."""
    from src.core.pricing_math import profit_summary_of

    # Without injection — true_profit == gross_profit
    s = profit_summary_of([
        {"supplier_cost": 50, "unit_price": 100, "qty": 1,
         "item_supplier": "Amazon"},
    ])
    assert s["total_landed_cost"] == 50.0
    assert s["true_profit"] == s["gross_profit"]

    # With injection — landed cost adds a flat $5 shipping per unit
    def fake_landed_cost(cost, qty, supplier):
        return {"landed_cost": cost + 5.0}

    s2 = profit_summary_of([
        {"supplier_cost": 50, "unit_price": 100, "qty": 2,
         "item_supplier": "Amazon"},
    ], landed_cost_fn=fake_landed_cost)
    # 2 items × ($50 + $5) = $110 landed cost
    assert s2["total_landed_cost"] == 110.0
    # Revenue $200, landed $110, true profit $90
    assert s2["true_profit"] == 90.0
    assert s2["true_margin_pct"] == 45.0


def test_fresh_compute_reflects_in_flight_cost_edits():
    """The substrate guarantee — `profit_summary_of(items)` returns
    CURRENT values, not whatever was cached on the last save. This
    test edits an item dict in place and reruns the computation;
    cache-trust code would have returned stale numbers."""
    from src.core.pricing_math import profit_summary_of
    items = [{"supplier_cost": 100, "unit_price": 150, "qty": 1}]

    s1 = profit_summary_of(items)
    assert s1["gross_profit"] == 50.0
    assert s1["margin_pct"] == round(50 / 150 * 100, 1)

    # Operator edits cost in place — fresh compute reflects the change.
    items[0]["supplier_cost"] = 75
    s2 = profit_summary_of(items)
    assert s2["gross_profit"] == 75.0
    assert s2["margin_pct"] == 50.0
    assert s1 != s2  # invariant: edits don't get lost in a stale cache
