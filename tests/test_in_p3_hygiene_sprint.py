"""Regression tests for IN-P3 hygiene sprint (IN-18, IN-19, IN-20, IN-21).

IN-18: /api/intel/quotes renders descriptions through markupsafe `esc()` —
       buyer PDF text carrying angle-brackets was concat'd raw, breaking
       layout and opening an XSS-flavored hole.
IN-19: unknown-buyer prior must not be flat 0.5. A flat prior makes
       EV = markup * P(win) strictly increasing, so the optimizer
       always rides markup_max on cold buyers — opposite of Reytech
       posture. Linear-decay prior creates an interior optimum.
IN-20: dead losses_price fallback removed from weekly-report calibration
       rendering. The key was renamed to losses_total in a prior
       migration; keeping the fallback masked future schema drift.
IN-21: debug endpoint appends a top-line status_summary aggregating the
       9 sub-checks so operators see green/yellow/red without scanning
       every section.
"""
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


# ── IN-18: HTML escape on description + part_number ─────────────────────

def test_in18_description_is_escaped():
    body = _read("src/api/modules/routes_intel.py")
    assert 'desc = esc(str(it.get("description", ""))[:80])' in body, (
        "IN-18 regression: description no longer wrapped in esc() — "
        "buyer PDFs with < > or quotes will break layout / XSS."
    )


def test_in18_part_number_is_escaped():
    body = _read("src/api/modules/routes_intel.py")
    assert "pn = esc(pn_raw)" in body, \
        "IN-18 regression: part_number no longer escaped"


def test_in18_items_text_is_escaped():
    body = _read("src/api/modules/routes_intel.py")
    assert "esc(items_text[:200])" in body, \
        "IN-18 regression: items_text fallback no longer escaped"


# ── IN-19: unknown-buyer prior decays with markup ──────────────────────

def test_in19_prior_is_markup_aware():
    body = _read("src/core/pricing_oracle_v2.py")
    # The old `return 0.5  # uninformed prior` is gone; the new prior
    # uses markup_pct in its formula.
    assert "return 0.5  # uninformed prior" not in body, (
        "IN-19 regression: flat 0.5 prior is back — optimizer will ride "
        "markup_max for unknown buyers."
    )
    # New formula references markup_pct
    assert "0.85 - (0.55 * m / float(_CURVE_MARKUP_MAX))" in body, (
        "IN-19 regression: linear-decay prior formula missing or changed "
        "— verify EV still has an interior optimum."
    )


def test_in19_prior_bounds_markup_input():
    body = _read("src/core/pricing_oracle_v2.py")
    assert "m = max(0.0, min(float(markup_pct), float(_CURVE_MARKUP_MAX)))" in body, (
        "IN-19 regression: markup input no longer clamped to [0, MAX] — "
        "extreme inputs could blow past sensible range."
    )


# ── IN-20: dead losses_price fallback removed ──────────────────────────

def test_in20_losses_price_fallback_removed():
    body = _read("src/agents/oracle_weekly_report.py")
    # The old dead code:
    #   losses_total=c.get("losses_total", c.get("losses_price", 0))
    # must not appear anywhere in the file.
    assert 'c.get("losses_total", c.get("losses_price", 0))' not in body, (
        "IN-20 regression: dead losses_price fallback is back — remove "
        "the get-or-get so schema drift surfaces loudly."
    )
    # And the new code uses just losses_total with 0 default
    assert 'losses_total=c.get("losses_total", 0)' in body, \
        "IN-20 regression: losses_total call pattern changed unexpectedly"


# ── IN-21: debug page top-line status summary ──────────────────────────

def test_in21_status_summary_is_computed():
    body = _read("src/api/modules/routes_intel.py")
    assert 'results["status_summary"]' in body, \
        "IN-21 regression: status_summary key no longer set in api_debug_run"


def test_in21_status_summary_has_all_levels():
    body = _read("src/api/modules/routes_intel.py")
    for level in ('"red"', '"yellow"', '"green"'):
        assert level in body, (
            f"IN-21 regression: status level {level} missing from "
            f"api_debug_run — roll-up will fail to distinguish failure modes."
        )


def test_in21_status_summary_lists_failed_and_warnings():
    body = _read("src/api/modules/routes_intel.py")
    assert '"failed": _failed' in body, \
        "IN-21 regression: failed list removed from status_summary"
    assert '"warnings": _warnings' in body, \
        "IN-21 regression: warnings list removed from status_summary"


def test_in21_status_summary_checks_sync_delta():
    body = _read("src/api/modules/routes_intel.py")
    assert '"sync_delta"' in body, (
        "IN-21 regression: sync_delta warning no longer surfaced in "
        "top-line status summary."
    )


def test_in21_status_summary_checks_crm_empty():
    body = _read("src/api/modules/routes_intel.py")
    assert '"crm_empty"' in body, \
        "IN-21 regression: crm_empty warning no longer surfaced"
