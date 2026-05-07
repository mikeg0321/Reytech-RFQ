"""Tier 1d — `src/core/external_call.py` (audit 2026-05-07).

Pins the behavior of `with_retry()` end-to-end + parity tests for the
two migrated wrappers (`gmail_api._with_gmail_retry` and `db.db_retry`)
against their pre-migration shapes. The remaining ad-hoc retries
(scprs_browser, scprs_lookup, product_validator) migrate in follow-on
PRs and gain pins there.
"""
from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from src.core.external_call import with_retry


# ── core with_retry behavior ────────────────────────────────────────

def test_returns_value_on_first_success():
    """No retry, no sleep, no log — clean path returns immediately."""
    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        return "ok"
    assert with_retry(fn, op="t") == "ok"
    assert calls["n"] == 1


def test_retries_on_transient_until_success():
    """Returns on the attempt that succeeds; counts retries correctly."""
    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"
    with patch("src.core.external_call.time.sleep") as mock_sleep:
        result = with_retry(fn, op="t", base_delay=0.1, attempts=5)
    assert result == "ok"
    assert calls["n"] == 3
    assert mock_sleep.call_count == 2  # slept before attempt 2 and 3


def test_raises_on_final_failure():
    """All attempts exhausted → last exception re-raised."""
    def fn():
        raise RuntimeError("never works")
    with patch("src.core.external_call.time.sleep"):
        with pytest.raises(RuntimeError, match="never works"):
            with_retry(fn, op="t", attempts=3)


def test_non_transient_raises_immediately():
    """is_transient=False → no retry, no sleep, original raise."""
    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        raise ValueError("permanent")
    with patch("src.core.external_call.time.sleep") as mock_sleep:
        with pytest.raises(ValueError, match="permanent"):
            with_retry(fn, op="t", attempts=5,
                       is_transient=lambda e: False)
    assert calls["n"] == 1
    assert mock_sleep.call_count == 0


def test_transient_predicate_filters_per_exception():
    """Only transient exceptions retry; others raise on the spot."""
    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("retry me")
        raise ValueError("don't retry me")
    with patch("src.core.external_call.time.sleep"):
        with pytest.raises(ValueError, match="don't retry"):
            with_retry(fn, op="t", attempts=5,
                       is_transient=lambda e: isinstance(e, RuntimeError))
    assert calls["n"] == 2


def test_exponential_backoff_progression():
    """0.5 * 2^i = 0.5, 1.0, 2.0, 4.0..."""
    def fn():
        raise RuntimeError("transient")
    with patch("src.core.external_call.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError):
            with_retry(fn, op="t", attempts=4, base_delay=0.5,
                       backoff="exponential")
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [0.5, 1.0, 2.0]


def test_linear_backoff_progression():
    """1.0 * (i+1) = 1.0, 2.0, 3.0..."""
    def fn():
        raise RuntimeError("transient")
    with patch("src.core.external_call.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError):
            with_retry(fn, op="t", attempts=4, base_delay=1.0,
                       backoff="linear")
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [1.0, 2.0, 3.0]


def test_on_final_failure_runs_before_raise():
    """Hook gets the exc + attempts, then the exc re-raises."""
    seen = {}
    def hook(exc, attempts):
        seen["exc"] = exc
        seen["attempts"] = attempts
    def fn():
        raise RuntimeError("die")
    with patch("src.core.external_call.time.sleep"):
        with pytest.raises(RuntimeError, match="die"):
            with_retry(fn, op="t", attempts=2, on_final_failure=hook)
    assert isinstance(seen["exc"], RuntimeError)
    assert seen["attempts"] == 2


def test_on_final_failure_hook_error_is_swallowed():
    """A buggy hook must not mask the original exception."""
    def bad_hook(exc, attempts):
        raise RuntimeError("hook is broken")
    def fn():
        raise ValueError("real error")
    with patch("src.core.external_call.time.sleep"):
        with pytest.raises(ValueError, match="real error"):
            with_retry(fn, op="t", attempts=2, on_final_failure=bad_hook)


def test_on_final_failure_skipped_on_non_transient():
    """Non-transient → raise immediately, hook does NOT fire.

    Matches the pre-migration db.db_retry behavior: the Slack alert was
    inside the `if "database is locked" in str(e)` branch, so a non-DB
    exception that fell through never alerted.
    """
    seen = {"n": 0}
    def hook(exc, attempts):
        seen["n"] += 1
    def fn():
        raise ValueError("permanent")
    with pytest.raises(ValueError):
        with_retry(fn, op="t",
                   is_transient=lambda e: False,
                   on_final_failure=hook)
    assert seen["n"] == 0


def test_invalid_attempts_rejected():
    with pytest.raises(ValueError, match="attempts must be"):
        with_retry(lambda: None, op="t", attempts=0)


def test_invalid_backoff_rejected():
    with pytest.raises(ValueError, match="backoff must be"):
        with_retry(lambda: None, op="t", backoff="quadratic")


def test_uses_provided_logger():
    """Caller's logger receives the warning, not the default one."""
    custom = logging.getLogger("test_retry_logger")
    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("transient")
        return "ok"
    with patch("src.core.external_call.time.sleep"):
        with patch.object(custom, "warning") as mock_warn:
            with_retry(fn, op="t", logger=custom, attempts=3)
            assert mock_warn.called


# ── parity test: gmail_api._with_gmail_retry ───────────────────────

def test_gmail_retry_wrapper_preserves_signature_and_behavior():
    """Migrated wrapper still: 3 attempts, exp backoff, transient-only."""
    from src.core.gmail_api import _with_gmail_retry, _GMAIL_TRANSIENT_ERRORS

    # Pick any transient-marker string actually used in the predicate
    transient_marker = next(iter(_GMAIL_TRANSIENT_ERRORS))

    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError(f"prefix {transient_marker} suffix")
        return {"messages": []}

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        result = _with_gmail_retry(fn, op="list_message_ids")
    assert result == {"messages": []}
    assert calls["n"] == 3
    # 0.5 * 2^0, 0.5 * 2^1 = exp backoff retained
    assert [c.args[0] for c in mock_sleep.call_args_list] == [0.5, 1.0]


def test_gmail_retry_wrapper_non_transient_raises_immediately():
    """A non-transient gmail error (e.g. 401 Unauthorized) does NOT retry."""
    from src.core.gmail_api import _with_gmail_retry

    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        raise RuntimeError("HttpError 401: Unauthorized")  # not in the marker set

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError, match="401"):
            _with_gmail_retry(fn, op="get_raw_message")
    assert calls["n"] == 1  # no retry
    assert mock_sleep.call_count == 0


# ── parity test: db.db_retry ────────────────────────────────────────

def test_db_retry_wrapper_locked_error_retries_with_linear_backoff():
    from src.core.db import db_retry

    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("database is locked")
        return "ok"

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        result = db_retry(fn, max_retries=3, delay=1.0)
    assert result == "ok"
    assert calls["n"] == 3
    # 1.0 * 1, 1.0 * 2 → linear retained
    assert [c.args[0] for c in mock_sleep.call_args_list] == [1.0, 2.0]


def test_db_retry_wrapper_non_lock_error_raises_immediately():
    """An IntegrityError (or anything non-lock) must NOT retry."""
    from src.core.db import db_retry

    calls = {"n": 0}
    def fn():
        calls["n"] += 1
        raise RuntimeError("UNIQUE constraint failed")

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError, match="UNIQUE"):
            db_retry(fn, max_retries=5)
    assert calls["n"] == 1
    assert mock_sleep.call_count == 0


def test_db_retry_wrapper_fires_slack_on_final_lock_failure():
    """All-locked exhaustion → fire_event('db_lock_timeout', ...)."""
    from src.core import db as db_mod

    def fn():
        raise RuntimeError("database is locked")

    fire_calls = []
    fake_webhooks = type(
        "FakeWebhooks", (),
        {"fire_event": staticmethod(lambda *a, **kw: fire_calls.append((a, kw)))}
    )
    with patch("src.core.external_call.time.sleep"):
        with patch.dict(
            __import__("sys").modules,
            {"src.core.webhooks": fake_webhooks},
        ):
            with pytest.raises(RuntimeError, match="database is locked"):
                db_mod.db_retry(fn, max_retries=2, delay=0.01)
    assert len(fire_calls) == 1
    args, _ = fire_calls[0]
    assert args[0] == "db_lock_timeout"
    assert args[1]["attempts"] == 2
    assert "database is locked" in args[1]["error"]


def test_db_retry_wrapper_non_lock_error_does_not_fire_slack():
    """Non-lock error → no Slack alert, original exception re-raises."""
    from src.core import db as db_mod

    def fn():
        raise RuntimeError("UNIQUE constraint failed")

    fire_calls = []
    fake_webhooks = type(
        "FakeWebhooks", (),
        {"fire_event": staticmethod(lambda *a, **kw: fire_calls.append((a, kw)))}
    )
    with patch.dict(
        __import__("sys").modules,
        {"src.core.webhooks": fake_webhooks},
    ):
        with pytest.raises(RuntimeError, match="UNIQUE"):
            db_mod.db_retry(fn, max_retries=3)
    assert fire_calls == []  # alert intentionally suppressed
