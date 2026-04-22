"""AN-3 regression guard: BI dashboard inflated won_revenue / won count by
picking max() across overlapping source tables.

Audited 2026-04-22. Three sources overlapped:
  1. `quotes WHERE status='won'` — bid-phase estimate
  2. `orders WHERE status IN ('paid','invoiced','delivered',...)` — invoiced
  3. `revenue_log` — explicit log (optional)
A single won deal lands in all three, so summing triple-counts. The old
code picked `max(...)` to avoid triple-count, but that lets any stale/partial
source dominate the headline, and a late revenue_log write would overstate
reality. Fix: pick canonical source (orders first, then quotes-won, then log).

Separately, `won_quotes` (count) was max()'d against `orders_count` where
orders_count = COUNT(*) FROM orders WHERE status NOT IN ('cancelled','').
That clause includes draft/open/pending rows — a pipeline count, not a won
count. max()ing it into won_quotes inflated win_rate denominator and the
avg_deal divisor. Fix: stop overriding won_quotes with orders_count.
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
    regex guards."""
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def test_no_max_across_revenue_sources():
    """won_revenue must not be computed with `max(` across the three
    overlapping revenue sources."""
    src = _strip_comment_lines(ROUTES_ANALYTICS.read_text(encoding="utf-8"))
    # The exact broken expression and any near-variant with the three sources.
    banned = re.search(
        r"won_revenue\s*=\s*max\(\s*won_revenue_quotes\s*,\s*won_revenue_orders",
        src,
    )
    assert not banned, (
        "AN-3 regression: `won_revenue = max(won_revenue_quotes, "
        "won_revenue_orders, ...)` is back. This inflates dashboards "
        "because a single won deal appears in all three sources."
    )


def test_no_max_won_quotes_against_orders_count():
    """won_quotes (count of quotes.status='won') must not be override-maxed
    against orders_count."""
    src = _strip_comment_lines(ROUTES_ANALYTICS.read_text(encoding="utf-8"))
    banned = re.search(
        r"won_quotes\s*=\s*max\(\s*won_quotes\s*,\s*orders_count",
        src,
    )
    assert not banned, (
        "AN-3 regression: `won_quotes = max(won_quotes, orders_count)` is "
        "back. orders_count includes non-won statuses (draft/open/pending), "
        "so this inflates the win count and skews win_rate + avg_deal."
    )


def test_build_bi_data_prefers_canonical_revenue():
    """_build_bi_data must pick orders first, then quotes-won, then log —
    a canonical-source chain, not max()."""
    src = ROUTES_ANALYTICS.read_text(encoding="utf-8")
    m = re.search(
        r"def _build_bi_data\(conn\)[\s\S]{0,4000}?won_revenue\s*=",
        src,
    )
    assert m, "_build_bi_data / won_revenue assignment not found"
    block = m.group(0)
    # The canonical chain has to assign won_revenue from each source by name.
    assert "won_revenue_orders" in block, "orders branch missing"
    assert "won_revenue_quotes" in block, "quotes-won branch missing"
    # And the banned max() form must not be in this block.
    assert "max(won_revenue_quotes" not in block and "max(won_revenue_orders" not in block, (
        "AN-3 regression: max()-across-sources expression is back inside "
        "_build_bi_data."
    )


def test_an3_module_still_compiles():
    """Sanity: the edited module must still import without syntax errors."""
    import py_compile
    py_compile.compile(str(ROUTES_ANALYTICS), doraise=True)
