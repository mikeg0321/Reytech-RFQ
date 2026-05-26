"""Pin: liveness sweep env-threshold overrides + recovery close-out.

PR-B (back-window audit Item 2026-05-26): two related substrate
improvements to the liveness sweep, both motivated by the daily
duplicate-alert noise Mike's chat was accreting.

1. Env-driven thresholds: every CHECKS tuple's `max_age_seconds` can
   now be overridden via `LIVENESS_<LABEL_SLUG>_MAX_AGE_S` env var.
   Mike can bump Gmail's 2h default to 24h via Railway env with no PR
   needed (the Mike's-low-RFQ-volume threshold-tuning gap).

2. Recovery close-out: when a check transitions stale → ok, fire ONE
   `{event}_recovered` alert via channels_override=["bell"]. Bell-only
   by design — recovery info shouldn't pile on Telegram noise. Backed
   by a new `liveness_state` table (one row per label) so transitions
   are detectable across sweep cycles.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest


# ─── _label_to_env_slug + _threshold_for ──────────────────────────────


def test_label_to_env_slug_basic():
    from src.core.liveness_checks import _label_to_env_slug
    assert _label_to_env_slug("Gmail inbound poller") == "GMAIL_INBOUND_POLLER"
    assert _label_to_env_slug("SCPRS award scrape") == "SCPRS_AWARD_SCRAPE"


def test_label_to_env_slug_collapses_punctuation():
    from src.core.liveness_checks import _label_to_env_slug
    # Parens, slashes, multi-space → single underscores, no trailing
    assert _label_to_env_slug("Gmail OAuth (silent token loss)") == \
        "GMAIL_OAUTH_SILENT_TOKEN_LOSS"
    assert _label_to_env_slug("Quote ingestion pipeline") == \
        "QUOTE_INGESTION_PIPELINE"


def test_threshold_for_returns_default_when_no_env(monkeypatch):
    from src.core.liveness_checks import _threshold_for
    monkeypatch.delenv("LIVENESS_GMAIL_INBOUND_POLLER_MAX_AGE_S",
                       raising=False)
    assert _threshold_for("Gmail inbound poller", 7200) == 7200


def test_threshold_for_reads_env_override(monkeypatch):
    from src.core.liveness_checks import _threshold_for
    monkeypatch.setenv("LIVENESS_GMAIL_INBOUND_POLLER_MAX_AGE_S", "86400")
    assert _threshold_for("Gmail inbound poller", 7200) == 86400


def test_threshold_for_rejects_non_int(monkeypatch):
    """Operator typo (env value is not parseable as int) must fall
    through to the default — never crash the sweep."""
    from src.core.liveness_checks import _threshold_for
    monkeypatch.setenv("LIVENESS_GMAIL_INBOUND_POLLER_MAX_AGE_S", "twentyfour")
    assert _threshold_for("Gmail inbound poller", 7200) == 7200


def test_threshold_for_rejects_zero_and_negative(monkeypatch):
    """0 or negative is a logical error (no check should fire instantly
    or never) — fall through to default."""
    from src.core.liveness_checks import _threshold_for
    monkeypatch.setenv("LIVENESS_GMAIL_INBOUND_POLLER_MAX_AGE_S", "0")
    assert _threshold_for("Gmail inbound poller", 7200) == 7200
    monkeypatch.setenv("LIVENESS_GMAIL_INBOUND_POLLER_MAX_AGE_S", "-1")
    assert _threshold_for("Gmail inbound poller", 7200) == 7200


# ─── liveness_state load + persist ────────────────────────────────────


def test_persist_then_load_round_trip():
    from src.core.liveness_checks import (
        _persist_liveness_state, _load_liveness_state,
    )
    _persist_liveness_state(
        "Test Check Alpha",
        status="ok", alert_event="external_service_disconnected",
        age_seconds=600, fired_alert=False, fired_recovered=False,
    )
    row = _load_liveness_state("Test Check Alpha")
    assert row is not None
    assert row["last_status"] == "ok"
    assert row["alert_event"] == "external_service_disconnected"
    assert row["last_age_seconds"] == 600


def test_persist_preserves_last_alert_at_when_not_firing():
    """If this sweep didn't fire an alert, last_alert_at must retain
    the prior value (otherwise we forget when we last bothered Mike)."""
    from src.core.liveness_checks import (
        _persist_liveness_state, _load_liveness_state,
    )
    # First: a stale sweep that fires the alert
    _persist_liveness_state(
        "Test Check Beta",
        status="stale", alert_event="gmail_oauth_expired",
        age_seconds=99999, fired_alert=True, fired_recovered=False,
    )
    after_alert = _load_liveness_state("Test Check Beta")
    assert after_alert["last_alert_at"] is not None
    alert_ts = after_alert["last_alert_at"]

    # Second: another stale sweep, but inside the cooldown — no fire
    _persist_liveness_state(
        "Test Check Beta",
        status="stale", alert_event="gmail_oauth_expired",
        age_seconds=99999, fired_alert=False, fired_recovered=False,
    )
    after_no_fire = _load_liveness_state("Test Check Beta")
    assert after_no_fire["last_alert_at"] == alert_ts, (
        "last_alert_at must be preserved when this sweep didn't fire"
    )


def test_load_returns_none_for_unseen_label():
    from src.core.liveness_checks import _load_liveness_state
    assert _load_liveness_state("Never-Seen-Label-XYZ") is None


# ─── End-to-end sweep: stale → ok transition fires recovered ──────────


def _stub_checks_module(monkeypatch, label, ok, age, max_age,
                        event="external_service_disconnected"):
    """Override CHECKS to a single-row test fixture."""
    import src.core.liveness_checks as lc
    monkeypatch.setattr(lc, "CHECKS", [
        (label, event, lambda: (ok, age, "stubbed"), max_age),
    ])


def test_first_sweep_stale_fires_alert_not_recovered(monkeypatch):
    """No prior state → stale check fires the normal alert. No
    recovered card (it was never previously ok-to-stale)."""
    import src.core.liveness_checks as lc
    _stub_checks_module(
        monkeypatch, "Test Check One", ok=False, age=10**9, max_age=3600,
    )
    # Clear any prior state for this label.
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("DELETE FROM liveness_state WHERE label=?",
                     ("Test Check One",))

    alert_calls = []
    monkeypatch.setattr(
        "src.agents.notify_agent.send_alert",
        lambda **kw: alert_calls.append(kw),
    )
    summary = lc.run_liveness_sweep()
    assert "Test Check One" in summary["alerts_fired"]
    assert summary.get("recovered_fired", []) == []
    # The alert that fired was the stale one — recovered event was NOT used.
    fired_events = [c["event_type"] for c in alert_calls]
    assert "external_service_disconnected" in fired_events
    assert "external_service_disconnected_recovered" not in fired_events


def test_transition_stale_to_ok_fires_recovered(monkeypatch):
    """Two sweeps. First stale (seeds liveness_state.last_status='stale').
    Second OK (transition detected → recovery alert fires)."""
    import src.core.liveness_checks as lc
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("DELETE FROM liveness_state WHERE label=?",
                     ("Test Check Two",))

    alert_calls = []
    monkeypatch.setattr(
        "src.agents.notify_agent.send_alert",
        lambda **kw: alert_calls.append(kw),
    )

    # Sweep #1: stale
    _stub_checks_module(
        monkeypatch, "Test Check Two", ok=False, age=10**9, max_age=3600,
    )
    lc.run_liveness_sweep()
    assert any(c["event_type"] == "external_service_disconnected"
               for c in alert_calls)
    alert_calls.clear()

    # Sweep #2: OK — transition detected
    _stub_checks_module(
        monkeypatch, "Test Check Two", ok=True, age=60, max_age=3600,
    )
    summary = lc.run_liveness_sweep()
    assert "Test Check Two" in summary.get("recovered_fired", [])
    recovered_calls = [
        c for c in alert_calls
        if c["event_type"] == "external_service_disconnected_recovered"
    ]
    assert len(recovered_calls) == 1
    # Bell-only — recovery must NOT pile on Telegram noise.
    assert recovered_calls[0]["channels_override"] == ["bell"]


def test_repeated_ok_does_not_re_fire_recovered(monkeypatch):
    """Once a check is observed OK, subsequent OK sweeps must NOT
    re-fire the recovery card — only the stale→ok transition fires it."""
    import src.core.liveness_checks as lc
    from src.core.db import get_db
    label = "Test Check Three"
    with get_db() as conn:
        conn.execute("DELETE FROM liveness_state WHERE label=?", (label,))

    alert_calls = []
    monkeypatch.setattr(
        "src.agents.notify_agent.send_alert",
        lambda **kw: alert_calls.append(kw),
    )

    # Sweep #1: stale → seeds prior_status=stale
    _stub_checks_module(monkeypatch, label, ok=False, age=10**9, max_age=3600)
    lc.run_liveness_sweep()
    # Sweep #2: OK → fires recovered
    _stub_checks_module(monkeypatch, label, ok=True, age=60, max_age=3600)
    lc.run_liveness_sweep()
    # Sweep #3: still OK → must NOT re-fire recovered
    alert_calls.clear()
    lc.run_liveness_sweep()
    recovered_in_third = [
        c for c in alert_calls
        if c["event_type"].endswith("_recovered")
    ]
    assert recovered_in_third == [], (
        "Recovery alert must fire ONCE on stale→ok transition, not on "
        "every subsequent OK sweep"
    )


def test_env_threshold_override_changes_sweep_decision(monkeypatch):
    """A check that's stale under the default but fresh under the env
    override should report OK after the override is set."""
    import src.core.liveness_checks as lc
    from src.core.db import get_db
    label = "Test Check Four"
    with get_db() as conn:
        conn.execute("DELETE FROM liveness_state WHERE label=?", (label,))

    # 5000s old, default 3600 → stale; with env=86400 → ok.
    _stub_checks_module(
        monkeypatch, label, ok=True, age=5000, max_age=3600,
    )
    monkeypatch.setenv(
        f"LIVENESS_{lc._label_to_env_slug(label)}_MAX_AGE_S", "86400",
    )

    alert_calls = []
    monkeypatch.setattr(
        "src.agents.notify_agent.send_alert",
        lambda **kw: alert_calls.append(kw),
    )
    summary = lc.run_liveness_sweep()
    # Found the test row
    test_row = next(r for r in summary["checks"] if r["name"] == label)
    assert test_row["ok"] is True
    assert test_row["max_age_seconds"] == 86400
    assert summary["alerts_fired"] == [] or label not in summary["alerts_fired"]
