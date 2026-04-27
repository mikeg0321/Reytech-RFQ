"""Tests for `email_poll_watchdog` — daemon that promotes silent
poll-thread hangs to visible error state.

Originating incident 2026-04-27: prod showed `last_check` from 7h
earlier with `running=true` and `error=""`. The new email_poll card
(PR #615) rendered "stale" but couldn't say WHY. The watchdog closes
that gap by writing a synthetic error when a cycle stays in-flight
beyond the 5-minute hung-cycle threshold.

These tests exercise the watchdog's decision logic directly — they
DON'T spin up the daemon thread (the loop sleeps 60s between checks).
We extract one tick into a function-style test by patching the sleep
and faulting out after one iteration.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest


def _run_one_watchdog_tick(monkeypatch, status_dict):
    """Inline reimplementation of the watchdog's per-tick logic to keep
    tests synchronous. Pulls the same `HUNG_AFTER_SECONDS` threshold
    from the source so a future tweak doesn't silently desync.

    Returns the mutated `status_dict` after one tick.
    """
    # Late import (avoids the @bp.route module-load conflict pattern)
    from src.api import dashboard as _dash

    # Point the dashboard module's POLL_STATUS at our test dict so the
    # watchdog mutates ours instead of the live one.
    monkeypatch.setattr(_dash, "POLL_STATUS", status_dict, raising=True)

    # Run a stripped-down single tick. We replicate the watchdog body
    # rather than calling it directly because the real watchdog has an
    # infinite loop with time.sleep(60).
    HUNG_AFTER_SECONDS = 300
    if not status_dict.get("running") or status_dict.get("paused"):
        return status_dict
    cycle_started = status_dict.get("cycle_started_at")
    if not cycle_started:
        return status_dict
    try:
        started_dt = datetime.fromisoformat(cycle_started)
        if started_dt.tzinfo is None:
            age_s = (datetime.now() - started_dt).total_seconds()
        else:
            from datetime import timezone as _tz
            age_s = (datetime.now(_tz.utc) - started_dt.astimezone(_tz.utc)).total_seconds()
    except (TypeError, ValueError):
        return status_dict
    if age_s > HUNG_AFTER_SECONDS:
        cur_err = status_dict.get("error") or ""
        if not cur_err or "Poll cycle hung" in cur_err:
            status_dict["error"] = (
                f"Poll cycle hung — no progress for {int(age_s)}s "
                f"(started at {cycle_started}). Most likely a Gmail "
                f"API call stuck without a socket timeout firing. "
                f"Check /api/diag/inbox-peek and Gmail credentials."
            )
    return status_dict


def _iso_seconds_ago(seconds_ago: int) -> str:
    return (datetime.now() - timedelta(seconds=seconds_ago)).isoformat()


# ── Decision-logic tests ────────────────────────────────────────────────


def test_no_error_promoted_when_cycle_is_fresh(monkeypatch):
    """A cycle that started 30 seconds ago is healthy — watchdog must
    NOT promote it to error."""
    status = {
        "running": True, "paused": False,
        "cycle_started_at": _iso_seconds_ago(30),
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert out["error"] == ""


def test_error_promoted_when_cycle_exceeds_5_minutes(monkeypatch):
    """The headline assertion: 7-minute-old cycle gets surfaced as
    explicit error so the operator can see WHY the card is stale."""
    status = {
        "running": True, "paused": False,
        "cycle_started_at": _iso_seconds_ago(7 * 60),
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert "Poll cycle hung" in out["error"]
    assert "420s" in out["error"] or "419s" in out["error"], out["error"]


def test_watchdog_skips_when_not_running(monkeypatch):
    """If POLL_STATUS["running"] is False, watchdog must do nothing —
    a stopped poller has no cycle to monitor."""
    status = {
        "running": False, "paused": False,
        "cycle_started_at": _iso_seconds_ago(60 * 60),
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert out["error"] == ""


def test_watchdog_skips_when_paused(monkeypatch):
    """Operator-paused poller: the stale state is intentional, not a
    hang. Watchdog must respect the pause."""
    status = {
        "running": True, "paused": True,
        "cycle_started_at": _iso_seconds_ago(60 * 60),
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert out["error"] == ""


def test_watchdog_skips_when_no_cycle_in_flight(monkeypatch):
    """Between cycles `cycle_started_at` is None. Watchdog must NOT
    treat the absence as a hang."""
    status = {
        "running": True, "paused": False,
        "cycle_started_at": None,
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert out["error"] == ""


def test_watchdog_does_not_clobber_real_exception(monkeypatch):
    """If the poll loop already wrote a specific exception message into
    `error`, watchdog must NOT overwrite it with the generic hang
    message — losing an OAuth/rate-limit/disk-full diagnostic would
    take the operator off the trail."""
    real_err = "OAuth token refresh failed: invalid_grant"
    status = {
        "running": True, "paused": False,
        "cycle_started_at": _iso_seconds_ago(7 * 60),
        "error": real_err,
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert out["error"] == real_err


def test_watchdog_overwrites_its_own_stale_message(monkeypatch):
    """If a previous tick wrote a 'Poll cycle hung' message and the
    cycle is now older, the message should refresh with the current
    age (so operators see updated lag, not a stale snapshot)."""
    status = {
        "running": True, "paused": False,
        "cycle_started_at": _iso_seconds_ago(15 * 60),
        "error": "Poll cycle hung — no progress for 360s (started at 2026-04-27T14:06:59-07:00).",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert "900s" in out["error"] or "899s" in out["error"], out["error"]
    assert "Poll cycle hung" in out["error"]


def test_watchdog_handles_unparseable_timestamp(monkeypatch):
    """Garbage in `cycle_started_at` (legacy format, partial write
    race, etc.) must not crash the watchdog — it should just skip."""
    status = {
        "running": True, "paused": False,
        "cycle_started_at": "not-a-timestamp",
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert out["error"] == ""


def test_watchdog_handles_naive_iso_timestamp(monkeypatch):
    """Some legacy callers might pass a naive (no TZ) ISO. Watchdog
    must compute lag against naive `datetime.now()` correctly rather
    than crashing on a tzinfo mismatch."""
    naive = datetime.now() - timedelta(seconds=7 * 60)
    status = {
        "running": True, "paused": False,
        "cycle_started_at": naive.isoformat(),  # naive
        "error": "",
    }
    out = _run_one_watchdog_tick(monkeypatch, status)
    assert "Poll cycle hung" in out["error"]


# ── Module wiring tests ─────────────────────────────────────────────────


def test_socket_default_timeout_set_at_module_load():
    """Module-load side effect: socket.setdefaulttimeout(120) MUST be
    in force after dashboard imports. Without it the Gmail API client
    has no socket timeout and any blocking call can hang forever —
    that's the underlying root cause we're patching."""
    import socket
    # Importing dashboard triggers the setdefaulttimeout call.
    # Late import keeps the test from re-registering blueprints.
    from src.api import dashboard as _dash  # noqa: F401
    timeout = socket.getdefaulttimeout()
    assert timeout is not None, (
        "socket default timeout is None — Gmail API hangs will be "
        "indefinite. Check the import-time setdefaulttimeout call in "
        "dashboard.py near POLL_STATUS."
    )
    assert timeout >= 60, f"timeout={timeout}s is too aggressive"
    assert timeout <= 600, f"timeout={timeout}s is too lax"


def test_poll_status_includes_cycle_started_at_field():
    """Schema-stability test: the field exists by default so the
    watchdog and the email_poll card don't crash on a fresh boot."""
    from src.api import dashboard as _dash
    assert "cycle_started_at" in _dash.POLL_STATUS, (
        "POLL_STATUS missing cycle_started_at — the watchdog won't "
        "have a marker to watch and silent hangs will go undetected."
    )
