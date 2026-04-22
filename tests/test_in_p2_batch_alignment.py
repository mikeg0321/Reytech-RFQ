"""Regression tests for the IN-P2 batch (IN-12, IN-13, IN-14, IN-16).

IN-12: oracle_backfill returns errors bucketed by agency so operator can
       tell whether a spike is one broken agency or scatter across many.
IN-13: weekly-report calibration seeder dedupes competitor_intel rows by
       quote_number — without this, a quote with 3 competitor losses gets
       calibrated 3x and loss stats inflate.
IN-14: _scprs_pull_freshness returns a `state` field with distinct sentinel
       values (fresh / stale / never / malformed / error) so the dashboard
       banner can render different hints per failure mode.
IN-16: reason breakdown applies an alias map before bucketing so case/spacing
       variants of the same reason land in one bucket.

IN-15 + IN-17 are scope-larger refactors deferred to a follow-up PR.
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


# ── IN-12: oracle_backfill agency histogram ────────────────────────────

def test_in12_backfill_result_has_errors_by_agency():
    body = _read("src/core/oracle_backfill.py")
    assert '"errors_by_agency": {}' in body, \
        "IN-12 regression: result dict missing errors_by_agency histogram"


def test_in12_backfill_buckets_errors_on_quote_path():
    body = _read("src/core/oracle_backfill.py")
    # The quote-loop except block must increment the per-agency counter
    assert 'result["errors_by_agency"][_ag]' in body, (
        "IN-12 regression: quote error path no longer increments the "
        "per-agency bucket — histogram will be empty on real failures."
    )


def test_in12_backfill_po_match_path_increments_bucket():
    body = _read("src/core/oracle_backfill.py")
    assert 'result["errors_by_agency"]["po_match"]' in body, (
        "IN-12 regression: quote_po_matches error path must log under "
        "'po_match' bucket (table has no agency column)."
    )


# ── IN-13: weekly-report dedup by quote_number ─────────────────────────

def test_in13_weekly_dedup_seen_set_exists():
    body = _read("src/agents/oracle_weekly_report.py")
    assert "_seen_quote_numbers = set()" in body, \
        "IN-13 regression: dedup set for competitor_intel rows was removed"


def test_in13_weekly_dedup_continues_on_dup():
    body = _read("src/agents/oracle_weekly_report.py")
    assert "if _qn in _seen_quote_numbers:" in body, \
        "IN-13 regression: dedup check-then-skip logic was removed"
    # Guard against someone flipping the check inverted
    assert "_seen_quote_numbers.add(_qn)" in body, \
        "IN-13 regression: dedup add-to-set was removed"


# ── IN-14: freshness state sentinel ────────────────────────────────────

def test_in14_freshness_returns_state_field():
    body = _read("src/api/modules/routes_growth_intel.py")
    # All 5 distinct state literals must appear in the function. Some are
    # set via variable assignment (state = "stale" if stale else "fresh"),
    # others via dict literal for the early-return error paths.
    for state_literal in ('"fresh"', '"stale"', '"never"',
                          '"malformed"', '"error"'):
        # Find the freshness function body and assert the literal is there
        idx = body.find("def _scprs_pull_freshness")
        end = body.find("\ndef ", idx + 1)
        fn_body = body[idx:end] if end > 0 else body[idx:]
        assert state_literal in fn_body, (
            f"IN-14 regression: freshness state literal {state_literal} "
            f"missing from _scprs_pull_freshness — dashboard banner will "
            f"regress to one-size-fits-all red."
        )


def test_in14_freshness_state_is_distinct_from_stale_bool():
    """The old code exposed only `stale: bool`. The fix keeps that for
    backwards-compat but adds `state` as the richer signal. Both must
    coexist so dashboard code paths on either contract keep working."""
    body = _read("src/api/modules/routes_growth_intel.py")
    # grep the function body — the return dict has both keys set.
    # we check that the return statements literally include both names.
    returns = [line for line in body.splitlines()
               if '"stale"' in line and '"state"' in line]
    assert len(returns) >= 3, (
        f"IN-14 regression: expected at least 3 return lines carrying "
        f"both 'stale' and 'state' keys, found {len(returns)}"
    )


# ── IN-16: reason alias map ────────────────────────────────────────────

def test_in16_reason_alias_map_exists():
    body = _read("src/api/modules/routes_growth_intel.py")
    assert "_REASON_ALIASES" in body, \
        "IN-16 regression: reason alias map _REASON_ALIASES removed"
    assert '"price_too_high": "price_too_high"' in body, \
        "IN-16 regression: canonical price_too_high key missing"


def test_in16_canon_helper_strips_and_lowers():
    body = _read("src/api/modules/routes_growth_intel.py")
    assert "_canon_reason" in body, \
        "IN-16 regression: _canon_reason helper was inlined or removed"
    assert '.strip().lower()' in body, \
        "IN-16 regression: canon helper no longer normalizes whitespace/case"


def test_in16_reason_bucket_key_uses_canonicalizer():
    body = _read("src/api/modules/routes_growth_intel.py")
    assert "_canon_reason(r.get('reason'))" in body, (
        "IN-16 regression: reason breakdown bucket key bypassed the "
        "canonicalizer — case/spacing variants will re-split."
    )
