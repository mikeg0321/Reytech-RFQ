"""PR-A / Phase 2 substrate — markup-sanity gate.

2026-05-12 macro audit (project_url_paste_substrate_macro_2026_05_12)
on pc_5728f934 (R24Q49 vintage): pre-Phase-1 PCs carry hallucinated
markup_pct values like 912%, 327%, 280%, -4.6%. These were computed
against Amazon-hallucinated $2.50 costs but persisted unchanged when
URL-paste later refreshed cost to a real $4 supplier price. The
`prefer="markup"` semantic introduced earlier (Mike P0 rfq_8efe9fae,
2026-05-12) correctly protects operator-typed markup intent but treats
hallucinated markup as if it were intent — forward-computing
`$4 × 10.12 = $40.48` and shipping $40 on a $4 eraser to the buyer.

The fix is a markup-sanity gate. An out-of-range markup_pct is treated
as MISSING by `reconcile_line_item`, so the reconciler either reverse-
derives a sane markup from cost+price (when a non-hallucinated price
is present) or leaves the row alone for the operator.

Default bounds: [-10%, 200%]. Env-overridable via
`PRICING_MARKUP_MIN_PCT` / `PRICING_MARKUP_MAX_PCT` so we can tune from
telemetry without a deploy.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest


# ── Bounds ────────────────────────────────────────────────────────


def test_default_markup_bounds_match_doc():
    """Defaults span all realistic Reytech bids: small loss-leader
    negatives at the floor through premium markups at the ceiling."""
    from src.core.pricing_math import _markup_bounds
    lo, hi = _markup_bounds()
    assert lo == -10.0
    assert hi == 200.0


def test_markup_bounds_env_override(monkeypatch):
    from src.core.pricing_math import _markup_bounds
    monkeypatch.setenv("PRICING_MARKUP_MIN_PCT", "0")
    monkeypatch.setenv("PRICING_MARKUP_MAX_PCT", "150")
    lo, hi = _markup_bounds()
    assert (lo, hi) == (0.0, 150.0)


def test_markup_bounds_inverted_env_falls_back_to_defaults(monkeypatch):
    """Operator misconfiguration (min>max) must not invert the gate."""
    from src.core.pricing_math import _markup_bounds
    monkeypatch.setenv("PRICING_MARKUP_MIN_PCT", "100")
    monkeypatch.setenv("PRICING_MARKUP_MAX_PCT", "10")
    lo, hi = _markup_bounds()
    assert (lo, hi) == (-10.0, 200.0)


def test_markup_bounds_bad_env_strings_fall_back(monkeypatch):
    from src.core.pricing_math import _markup_bounds
    monkeypatch.setenv("PRICING_MARKUP_MIN_PCT", "not-a-number")
    monkeypatch.setenv("PRICING_MARKUP_MAX_PCT", "")
    lo, hi = _markup_bounds()
    assert (lo, hi) == (-10.0, 200.0)


# ── _markup_is_sane ───────────────────────────────────────────────


def test_markup_is_sane_window_endpoints():
    from src.core.pricing_math import _markup_is_sane
    assert _markup_is_sane(-10.0)
    assert _markup_is_sane(200.0)
    assert _markup_is_sane(0.0)
    assert _markup_is_sane(50.0)


def test_markup_is_sane_out_of_window():
    from src.core.pricing_math import _markup_is_sane
    assert not _markup_is_sane(-50.0)
    assert not _markup_is_sane(201.0)
    assert not _markup_is_sane(912.0)   # pc_5728f934 item 5
    assert not _markup_is_sane(327.0)   # pc_5728f934 item 6


def test_markup_is_sane_none_returns_false():
    from src.core.pricing_math import _markup_is_sane
    assert not _markup_is_sane(None)


# ── Reconciler honors the gate (the actual win) ───────────────────


def test_reconcile_drops_912pct_hallucination_and_reverse_derives_from_price():
    """pc_5728f934 item 5 — the original failure mode.

    Operator pasted a real Uline URL. Cost updated to $4. Stale
    markup_pct=912% (from when cost was hallucinated $2.50 against a
    persisted $25 price). Reconciler used to forward-compute
    `$4 × 10.12 = $40.48` and ship that. Gate now treats 912% as
    MISSING; reconciler reverse-derives a sane markup from cost+price."""
    from src.core.pricing_math import reconcile_line_item
    item = {
        "pricing": {"unit_cost": 4.00, "markup_pct": 912.0},
        "unit_price": 6.00,  # a sane operator-intended price
    }
    reconcile_line_item(item, prefer="markup")
    # Markup got reverse-derived from cost+price, NOT forward-computed from 912%
    assert item["unit_price"] == 6.00
    assert item["markup_pct"] == 50.0  # (6-4)/4 * 100


def test_reconcile_drops_negative_50pct_hallucination():
    """pc_5728f934 item 9 — `-4.6%` was within range, but a true
    hallucination at -50% (e.g. inverted-sign scrape) must also be
    treated as missing."""
    from src.core.pricing_math import reconcile_line_item
    item = {
        "pricing": {"unit_cost": 10.00, "markup_pct": -50.0},
        "unit_price": 12.00,
    }
    reconcile_line_item(item, prefer="markup")
    # Gate triggers — markup reverse-derived from cost+price
    assert item["markup_pct"] == 20.0  # (12-10)/10 * 100
    assert item["unit_price"] == 12.00


def _peek_markup(item):
    """Pull markup_pct from wherever the reconciler left it (mirrors
    `_read_markup` priority — flat key, then pricing.markup_pct).
    A sane unchanged markup may stay nested under `pricing` since the
    sticky-markup path doesn't rewrite when no derivation occurred."""
    if item.get("markup_pct") is not None:
        return item["markup_pct"]
    return (item.get("pricing") or {}).get("markup_pct")


def test_reconcile_keeps_sane_50pct_markup_under_prefer_markup():
    """Sanity check that the gate doesn't fire on normal markups.
    50% markup on $10 cost forward-computes to $15 price (sticky
    markup semantic for `prefer=markup`)."""
    from src.core.pricing_math import reconcile_line_item
    item = {
        "pricing": {"unit_cost": 10.00, "markup_pct": 50.0},
        "unit_price": 14.00,  # stale price disagrees with markup
    }
    reconcile_line_item(item, prefer="markup")
    assert _peek_markup(item) == 50.0       # markup respected (sane → not dropped)
    assert item["unit_price"] == 15.00      # forward-computed from markup


def test_reconcile_keeps_small_negative_loss_leader():
    """Mike's typical bids occasionally go slightly negative as a
    loss-leader for relationship-building. -5% must NOT trigger the
    gate (it's within [-10, 200])."""
    from src.core.pricing_math import reconcile_line_item
    item = {
        "pricing": {"unit_cost": 100.00, "markup_pct": -5.0},
        "unit_price": 90.00,  # stale; should be forward-computed to 95
    }
    reconcile_line_item(item, prefer="markup")
    assert _peek_markup(item) == -5.0
    assert item["unit_price"] == 95.00


def test_reconcile_with_no_price_and_hallucinated_markup_leaves_row_alone():
    """When markup is hallucinated AND no sane price exists to reverse-
    derive from, the reconciler must NOT invent a price. It leaves the
    row for the operator to fix rather than ship $40 on a $4 cost."""
    from src.core.pricing_math import reconcile_line_item
    item = {
        "pricing": {"unit_cost": 4.00, "markup_pct": 912.0},
        # no unit_price at all
    }
    reconcile_line_item(item, prefer="markup")
    # Markup treated as missing; no price to reverse-derive from →
    # neither markup nor price should be forward-computed from the
    # hallucinated value.
    assert item.get("unit_price", 0) == 0
    assert item["pricing"]["unit_cost"] == 4.00


def test_reconcile_gate_under_prefer_price_also_drops_hallucination():
    """The `prefer=price` semantic is used by PC `_do_save_prices`.
    Even there, an out-of-range markup_pct must not be considered
    operator truth — though the price-wins path will rewrite the
    markup from cost+price anyway, the log line must fire so we
    can audit."""
    from src.core.pricing_math import reconcile_line_item
    item = {
        "pricing": {"unit_cost": 4.00, "markup_pct": 912.0},
        "unit_price": 6.00,
    }
    reconcile_line_item(item, prefer="price")
    # `prefer=price` reverse-derives markup regardless; with the gate,
    # the reverse-derive runs against a "clean" None starting point.
    assert item["markup_pct"] == 50.0


def test_reconcile_gate_log_fires_on_out_of_range(caplog):
    import logging
    from src.core.pricing_math import reconcile_line_item
    item = {"pricing": {"unit_cost": 4.00, "markup_pct": 912.0}, "unit_price": 6.00}
    with caplog.at_level(logging.WARNING, logger="reytech.pricing_math"):
        reconcile_line_item(item, prefer="markup")
    msgs = [rec.message for rec in caplog.records]
    assert any("markup-sanity" in m and "912" in m for m in msgs), (
        f"expected sanity-gate warning, got: {msgs!r}"
    )


def test_reconcile_gate_silent_when_sane():
    """No log noise when markup is in-range — operators shouldn't see
    a flood of harmless lines."""
    import logging
    from src.core.pricing_math import reconcile_line_item
    item = {"pricing": {"unit_cost": 10.00, "markup_pct": 25.0}, "unit_price": 12.50}
    caplog_records = []

    class _Handler(logging.Handler):
        def emit(self, rec):
            if "markup-sanity" in rec.getMessage():
                caplog_records.append(rec)

    log = logging.getLogger("reytech.pricing_math")
    h = _Handler()
    log.addHandler(h)
    try:
        reconcile_line_item(item, prefer="markup")
    finally:
        log.removeHandler(h)
    assert caplog_records == [], (
        f"sanity log fired on in-range markup: {[r.getMessage() for r in caplog_records]!r}"
    )


def test_reconcile_env_override_tightens_bounds(monkeypatch):
    """When the operator tightens bounds to [0, 100], a sane-by-default
    150% markup gets dropped. Useful when the team wants to flag any
    triple-digit markup for manual review."""
    from src.core.pricing_math import reconcile_line_item
    monkeypatch.setenv("PRICING_MARKUP_MIN_PCT", "0")
    monkeypatch.setenv("PRICING_MARKUP_MAX_PCT", "100")
    item = {
        "pricing": {"unit_cost": 10.00, "markup_pct": 150.0},
        "unit_price": 15.00,  # operator-intended sane price
    }
    reconcile_line_item(item, prefer="markup")
    # 150 > 100 cap → treated as missing → reverse-derived from cost+price
    assert item["markup_pct"] == 50.0
    assert item["unit_price"] == 15.00
