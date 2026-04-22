"""AN-P0 regression guard: /health/quoting funnel must filter is_test
rows on every price_checks + quotes query.

Audited 2026-04-22 — `_build_funnel` counted test fixtures into the prod
health dashboard numbers. Ops stares at this to decide whether
classifier_v2 is landing cleanly; synthetic success hides real crashes.

Same class as CR-5 (deadlines RFQ loop missing is_test) and IN-3/IN-4
(debug endpoints seeding ghost data). These tests are source-level so
CI fails if an is_test guard gets ripped out.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_HEALTH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_health.py"
)


def _extract_funnel_fn(src: str) -> str:
    m = re.search(
        r"def _build_funnel\(days: int\):[\s\S]*?(?=\ndef |\Z)",
        src,
    )
    assert m, "_build_funnel not found in routes_health.py"
    return m.group(0)


def test_funnel_pc_query_filters_is_test():
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_funnel_fn(src)
    pc_block = re.search(r"FROM price_checks[\s\S]*?\(since,\)", fn)
    assert pc_block, "price_checks query block not found in _build_funnel"
    assert "is_test" in pc_block.group(0), (
        "AN-P0 regression: _build_funnel price_checks query is missing "
        "the is_test guard — test PCs inflate the prod funnel counts."
    )


def _block_for(var_name: str, fn: str) -> str:
    """Return the full _safe_fetchone(...) call block assigned to var_name."""
    m = re.search(
        rf"{var_name} = _safe_fetchone\([\s\S]*?\)\s*or\s*\{{[^}}]*\}}",
        fn,
    )
    assert m, f"{var_name} _safe_fetchone block not found"
    return m.group(0)


def test_funnel_quote_count_filters_is_test():
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_funnel_fn(src)
    block = _block_for("quote_n", fn)
    assert "is_test" in block, (
        "AN-P0 regression: _build_funnel total-quotes query (quote_n) is "
        "missing the is_test guard — test quotes inflate the funnel."
    )


def test_funnel_quote_won_filters_is_test():
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_funnel_fn(src)
    block = _block_for("quote_won", fn)
    assert "is_test" in block, (
        "AN-P0 regression: quotes-won query (quote_won) is missing "
        "is_test guard. Synthetic wins inflate won-revenue health card."
    )


def test_funnel_quote_sent_filters_is_test():
    src = ROUTES_HEALTH.read_text(encoding="utf-8")
    fn = _extract_funnel_fn(src)
    sent_block = re.search(
        r'"SELECT COUNT\(\*\) AS n FROM quotes WHERE created_at >= \? "\s*'
        r'"AND \(sent_at IS NOT NULL[^"]*"',
        fn,
    )
    # Fallback — the exact split may vary. Just ensure the sent-at query
    # has is_test somewhere in it.
    sent_all = re.search(
        r"quote_sent = _safe_fetchone\([\s\S]*?\) or \{[^}]*\}",
        fn,
    )
    assert sent_all, "quote_sent block not found"
    assert "is_test" in sent_all.group(0), (
        "AN-P0 regression: quote_sent query is missing is_test guard."
    )
