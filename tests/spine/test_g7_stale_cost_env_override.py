"""Pin: COST_STALENESS_FINALIZE_DAYS env var overrides the 30-day default
for the stale-cost finalize gate.

Chrome MCP audit 2026-05-27 / G7 (Architect approval): the cost-
staleness gate that refuses parsed/priced → finalized when any line's
cost_validated_at is older than the threshold has always existed in
`_validate_status_invariants`. This PR adds env-override control so
staging / regression / per-deployment tuning is possible without
code change.

Tests pin:
  1. Unset env → 30-day default
  2. Valid positive integer in env → that value
  3. Invalid (non-int / negative / zero) → falls back to 30
  4. End-to-end: a cost validated 25 days ago passes under default 30,
     fails under env-override of 20.
"""
from __future__ import annotations

import importlib
import os
from datetime import datetime, timezone, timedelta

import pytest


def _reload_model():
    """Force re-import so the module-level env read fires fresh."""
    import src.spine.model as m
    importlib.reload(m)
    return m


@pytest.fixture(autouse=True)
def _restore_model_namespace_after_reload():
    """Quarantine the importlib.reload() pollution this file inflicts.

    Every test here calls `importlib.reload(src.spine.model)` to re-run
    the module-level `COST_VALIDATION_FRESHNESS_DAYS` env read. reload()
    rebuilds the module's classes IN PLACE — so after this file runs,
    `src.spine.model.Quote` / `LineItem` / `QuoteStatus` /
    `SpineValidationError` are BRAND-NEW class objects, distinct from
    the ones every other test module captured at its own import time.
    The mismatch breaks `pytest.raises(SpineValidationError)` and
    enum-identity transition checks in any test that runs AFTER this
    file in the same process — the cross-test ordering pollution that
    only surfaced under the full-suite run, never in isolation.

    Fix: snapshot the module's public attribute objects before the test,
    and after the test (a) clear the env override and reload once more so
    the constant returns to its 30-day default, then (b) re-bind the
    ORIGINAL class objects back onto the live module so every other
    test's captured references stay valid. This makes the reload
    strictly local to this file.
    """
    import src.spine.model as m

    # Snapshot the original public objects (the ones other modules hold).
    saved = {k: getattr(m, k) for k in dir(m) if not k.startswith("__")}
    saved_env = os.environ.get("COST_STALENESS_FINALIZE_DAYS")
    try:
        yield
    finally:
        # 1. Restore the env to its pre-test value so the final reload
        #    rebuilds COST_VALIDATION_FRESHNESS_DAYS at the real default.
        if saved_env is None:
            os.environ.pop("COST_STALENESS_FINALIZE_DAYS", None)
        else:
            os.environ["COST_STALENESS_FINALIZE_DAYS"] = saved_env
        importlib.reload(m)
        # 2. Re-bind the ORIGINAL class objects onto the live module so
        #    references captured by other test modules remain identical.
        for k, v in saved.items():
            setattr(m, k, v)


def test_unset_env_defaults_to_30(monkeypatch):
    monkeypatch.delenv("COST_STALENESS_FINALIZE_DAYS", raising=False)
    m = _reload_model()
    assert m.COST_VALIDATION_FRESHNESS_DAYS == 30


def test_env_int_override_applies(monkeypatch):
    monkeypatch.setenv("COST_STALENESS_FINALIZE_DAYS", "14")
    m = _reload_model()
    assert m.COST_VALIDATION_FRESHNESS_DAYS == 14


def test_env_invalid_falls_back_to_30(monkeypatch):
    """Non-integer env value → fallback to 30."""
    monkeypatch.setenv("COST_STALENESS_FINALIZE_DAYS", "not-a-number")
    m = _reload_model()
    assert m.COST_VALIDATION_FRESHNESS_DAYS == 30


def test_env_zero_falls_back_to_30(monkeypatch):
    """Zero would effectively disable the gate (no cost can be "0 days"
    fresh on the wrong side of midnight). Refuse the footgun — fall
    back to 30."""
    monkeypatch.setenv("COST_STALENESS_FINALIZE_DAYS", "0")
    m = _reload_model()
    assert m.COST_VALIDATION_FRESHNESS_DAYS == 30


def test_env_negative_falls_back_to_30(monkeypatch):
    monkeypatch.setenv("COST_STALENESS_FINALIZE_DAYS", "-7")
    m = _reload_model()
    assert m.COST_VALIDATION_FRESHNESS_DAYS == 30


def test_env_override_changes_finalize_behavior(monkeypatch):
    """End-to-end: a cost validated 25 days ago passes under default 30,
    fails under env-override of 20. Exercises the actual gate
    behavior, not just the constant."""
    # Tight threshold first.
    monkeypatch.setenv("COST_STALENESS_FINALIZE_DAYS", "20")
    m = _reload_model()

    twenty_five_days_ago = datetime.now(timezone.utc) - timedelta(days=25)
    line = m.LineItem(
        line_no=1, description="x", mfg_number="M1",
        qty=1, uom="EA",
        cost_cents=15000,  # > validation threshold, so the gate fires
        cost_source_url="https://example.com",
        cost_validated_at=twenty_five_days_ago,
        unit_price_cents=20000,
    )
    q = m.Quote(
        quote_id="q-g7-tight",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="S-1",
        tax_rate_bps=775,
        line_items=[line],
        status="priced",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    import pytest
    with pytest.raises(m.SpineValidationError, match="fresh"):
        q.with_status(m.QuoteStatus.FINALIZED)

    # Loosen threshold; same data should pass now.
    monkeypatch.setenv("COST_STALENESS_FINALIZE_DAYS", "30")
    m2 = _reload_model()
    line2 = m2.LineItem(
        line_no=1, description="x", mfg_number="M1",
        qty=1, uom="EA",
        cost_cents=15000,
        cost_source_url="https://example.com",
        cost_validated_at=twenty_five_days_ago,
        unit_price_cents=20000,
    )
    q2 = m2.Quote(
        quote_id="q-g7-loose",
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="S-1",
        tax_rate_bps=775,
        line_items=[line2],
        status="priced",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    # Should NOT raise.
    finalized = q2.with_status(m2.QuoteStatus.FINALIZED)
    assert finalized.status == m2.QuoteStatus.FINALIZED
