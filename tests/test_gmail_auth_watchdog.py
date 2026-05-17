"""Tests for src/agents/gmail_auth_watchdog.py.

Substrate liveness probe for Gmail OAuth. Closes the silent-OAuth-break
class from 2026-05-16 — when CLIENT_ID was truncated in Railway and the
prod app went Gmail-blind for hours with nothing surfaced to a human.

Mocks Gmail entirely (the watchdog must NEVER hit prod Gmail during
tests; circuit-breaker contagion is real).
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import src.agents.gmail_auth_watchdog as wd


# ── Per-test state isolation ─────────────────────────────────────────


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """Each test gets its own gmail_health.json — no cross-contamination."""
    state_file = tmp_path / "gmail_health.json"
    monkeypatch.setattr(wd, "_state_path", lambda: state_file)
    yield state_file


@pytest.fixture
def fake_gmail_ok(monkeypatch):
    """Mock get_service to return a service whose getProfile() succeeds."""
    import src.core.gmail_api as gmail_mod
    fake_svc = MagicMock()
    fake_svc.users().getProfile().execute.return_value = {
        "emailAddress": "sales@reytechinc.com",
    }
    monkeypatch.setattr(gmail_mod, "is_configured", lambda: True)
    monkeypatch.setattr(gmail_mod, "get_service", lambda inbox: fake_svc)
    return fake_svc


@pytest.fixture
def fake_gmail_invalid_client(monkeypatch):
    """Mock get_service to simulate the 2026-05-16 prod failure."""
    import src.core.gmail_api as gmail_mod
    def _raise(*a, **kw):
        raise Exception(
            "('invalid_client: The OAuth client was not found.', "
            "{'error': 'invalid_client', 'error_description': "
            "'The OAuth client was not found.'})"
        )
    monkeypatch.setattr(gmail_mod, "is_configured", lambda: True)
    monkeypatch.setattr(gmail_mod, "get_service", _raise)


# ── _classify_error ──────────────────────────────────────────────────


@pytest.mark.parametrize("err_text,expected", [
    ("invalid_client: blah", "invalid_client"),
    ("Bad Request {'error': 'invalid_grant'}", "invalid_grant"),
    ("invalid_scope: Bad Request", "invalid_scope"),
    ("Quota exceeded for user", "rate_limited"),
    ("rate limit", "rate_limited"),
    ("Read timed out after 30s", "timeout"),
    ("Connection reset by peer", "connection_error"),
    ("Some other random exception text", "other"),
    ("", "other"),
])
def test_classify_error_categorizes_known_classes(err_text, expected):
    assert wd._classify_error(err_text) == expected


# ── check_inbox ──────────────────────────────────────────────────────


def test_check_inbox_ok_returns_profile(fake_gmail_ok):
    result = wd.check_inbox("sales")
    assert result["ok"] is True
    assert result["error_class"] == ""
    assert result["profile_email"] == "sales@reytechinc.com"
    assert "checked_at" in result


def test_check_inbox_invalid_client_returns_classified_error(
    fake_gmail_invalid_client,
):
    result = wd.check_inbox("sales")
    assert result["ok"] is False
    assert result["error_class"] == "invalid_client"
    assert result["profile_email"] == ""


def test_check_inbox_unconfigured_returns_not_configured(monkeypatch):
    import src.core.gmail_api as gmail_mod
    monkeypatch.setattr(gmail_mod, "is_configured", lambda: False)
    result = wd.check_inbox("sales")
    assert result["ok"] is False
    assert result["error_class"] == "not_configured"


def test_check_inbox_never_raises_on_unexpected_failure(monkeypatch):
    """Substrate guarantee — every failure mode is captured, never raised."""
    import src.core.gmail_api as gmail_mod
    monkeypatch.setattr(gmail_mod, "is_configured", lambda: True)
    def _explode(*a, **kw):
        raise RuntimeError("totally unexpected boom")
    monkeypatch.setattr(gmail_mod, "get_service", _explode)
    result = wd.check_inbox("sales")
    assert result["ok"] is False
    assert result["error_class"] == "other"


def test_check_all_inboxes_returns_both(fake_gmail_ok):
    result = wd.check_all_inboxes()
    assert set(result.keys()) == set(wd.INBOXES)
    assert all(v["ok"] for v in result.values())


# ── reconcile_and_alert state transitions ────────────────────────────


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def test_reconcile_ok_to_broken_fires_immediate_alert():
    sent = []
    prior = {"sales": {"ok": True, "state_changed_at": _now_iso()}}
    probes = {"sales": {"ok": False, "error_class": "invalid_client",
                        "checked_at": _now_iso(), "profile_email": ""}}
    new_state = wd.reconcile_and_alert(probes, prior, sms_sender=sent.append)
    assert len(sent) == 1
    assert "BROKEN" in sent[0]
    assert "sales" in sent[0]
    assert "invalid_client" in sent[0]
    assert new_state["sales"]["last_alert_at"] is not None
    assert new_state["sales"]["consecutive_failures"] == 1
    assert new_state["sales"]["rewarn_count"] == 0


def test_reconcile_broken_to_ok_fires_recovery_alert():
    sent = []
    prior = {"sales": {
        "ok": False, "error_class": "invalid_client",
        "consecutive_failures": 3, "rewarn_count": 1,
        "last_alert_at": _now_iso(), "state_changed_at": _now_iso(),
    }}
    probes = {"sales": {"ok": True, "error_class": "",
                        "checked_at": _now_iso(),
                        "profile_email": "sales@reytechinc.com"}}
    new_state = wd.reconcile_and_alert(probes, prior, sms_sender=sent.append)
    assert len(sent) == 1
    assert "RESTORED" in sent[0]
    assert new_state["sales"]["consecutive_failures"] == 0
    assert new_state["sales"]["rewarn_count"] == 0


def test_reconcile_still_broken_within_rewarn_window_no_alert():
    """Re-warn cadence prevents per-poll SMS spam."""
    sent = []
    prior = {"sales": {
        "ok": False, "error_class": "invalid_client",
        "consecutive_failures": 2, "rewarn_count": 1,
        "last_alert_at": _now_iso(),  # just now → no rewarn yet
        "state_changed_at": _now_iso(),
    }}
    probes = {"sales": {"ok": False, "error_class": "invalid_client",
                        "checked_at": _now_iso(), "profile_email": ""}}
    new_state = wd.reconcile_and_alert(
        probes, prior, sms_sender=sent.append,
        rewarn_interval_sec=1800,
    )
    assert sent == []
    assert new_state["sales"]["consecutive_failures"] == 3
    assert new_state["sales"]["rewarn_count"] == 1  # unchanged


def test_reconcile_still_broken_past_rewarn_window_alerts():
    sent = []
    long_ago = (datetime.now(timezone.utc)
                - timedelta(seconds=2000)).isoformat()
    prior = {"sales": {
        "ok": False, "error_class": "invalid_client",
        "consecutive_failures": 5, "rewarn_count": 1,
        "last_alert_at": long_ago, "state_changed_at": long_ago,
    }}
    probes = {"sales": {"ok": False, "error_class": "invalid_client",
                        "checked_at": _now_iso(), "profile_email": ""}}
    new_state = wd.reconcile_and_alert(
        probes, prior, sms_sender=sent.append,
        rewarn_interval_sec=1800,
    )
    assert len(sent) == 1
    assert "STILL BROKEN" in sent[0]
    assert "Down for" in sent[0]
    assert new_state["sales"]["rewarn_count"] == 2


def test_reconcile_rewarn_cap_prevents_unbounded_sms():
    sent = []
    long_ago = (datetime.now(timezone.utc)
                - timedelta(seconds=2000)).isoformat()
    prior = {"sales": {
        "ok": False, "error_class": "invalid_client",
        "consecutive_failures": 30, "rewarn_count": 6,  # cap hit
        "last_alert_at": long_ago, "state_changed_at": long_ago,
    }}
    probes = {"sales": {"ok": False, "error_class": "invalid_client",
                        "checked_at": _now_iso(), "profile_email": ""}}
    new_state = wd.reconcile_and_alert(
        probes, prior, sms_sender=sent.append,
        rewarn_interval_sec=1800, rewarn_max_count=6,
    )
    assert sent == []
    assert new_state["sales"]["rewarn_count"] == 6  # unchanged


def test_reconcile_first_run_with_no_prior_state_does_not_alert_on_ok():
    sent = []
    probes = {"sales": {"ok": True, "error_class": "",
                        "checked_at": _now_iso(),
                        "profile_email": "sales@reytechinc.com"}}
    new_state = wd.reconcile_and_alert({**probes}, {}, sms_sender=sent.append)
    assert sent == []  # no spurious alert on healthy first run
    assert new_state["sales"]["ok"] is True


def test_reconcile_first_run_broken_alerts_immediately():
    """First-ever probe finds it broken — alert (prior_ok defaults to True)."""
    sent = []
    probes = {"sales": {"ok": False, "error_class": "invalid_client",
                        "checked_at": _now_iso(), "profile_email": ""}}
    wd.reconcile_and_alert(probes, {}, sms_sender=sent.append)
    assert len(sent) == 1
    assert "BROKEN" in sent[0]


# ── State persistence ────────────────────────────────────────────────


def test_load_state_missing_file_returns_empty_dict(isolated_state):
    assert wd.load_state() == {}


def test_save_then_load_round_trip(isolated_state):
    state = {"sales": {"ok": True, "checked_at": "2026-05-17T00:00:00Z"}}
    wd._save_state(state)
    loaded = wd.load_state()
    assert loaded == state


def test_load_state_corrupt_file_returns_empty(isolated_state, tmp_path):
    isolated_state.write_text("not valid json {{{", encoding="utf-8")
    assert wd.load_state() == {}


# ── SMS hook safety ──────────────────────────────────────────────────


def test_send_sms_alert_no_op_when_operator_phone_unset(monkeypatch):
    monkeypatch.delenv("OPERATOR_PHONE", raising=False)
    # Should not raise; should not attempt to import twilio.
    wd._send_sms_alert("test alert body")


def test_send_sms_alert_no_op_when_twilio_unconfigured(monkeypatch):
    monkeypatch.setenv("OPERATOR_PHONE", "+15551234567")
    import src.core.twilio_client as tw
    monkeypatch.setattr(tw, "is_configured", lambda: False)
    wd._send_sms_alert("test alert body")  # no raise


def test_send_sms_alert_never_raises_on_twilio_failure(monkeypatch):
    monkeypatch.setenv("OPERATOR_PHONE", "+15551234567")
    import src.core.twilio_client as tw
    monkeypatch.setattr(tw, "is_configured", lambda: True)
    def _explode(*a, **kw):
        raise RuntimeError("twilio fell over")
    monkeypatch.setattr(tw, "send_sms", _explode)
    wd._send_sms_alert("test alert body")  # no raise — watchdog stays alive


# ── Daemon loop ──────────────────────────────────────────────────────


def test_watchdog_loop_exits_on_stop_event(monkeypatch, fake_gmail_ok):
    """Crucial property — clean shutdown for tests + prod restarts."""
    stop = threading.Event()
    t = threading.Thread(
        target=wd.run_watchdog_loop,
        kwargs={"interval_sec": 1, "stop_event": stop},
        daemon=True,
    )
    t.start()
    time.sleep(0.2)  # let one iteration land
    stop.set()
    t.join(timeout=3)
    assert not t.is_alive()


def test_watchdog_loop_iteration_persists_state(
    isolated_state, fake_gmail_ok, monkeypatch,
):
    """One full loop iteration should write state to disk."""
    stop = threading.Event()
    def _quick(interval_sec, *, stop_event):
        try:
            probes = wd.check_all_inboxes()
            prior = wd.load_state()
            wd._save_state(wd.reconcile_and_alert(probes, prior))
        finally:
            stop_event.set()
    _quick(1, stop_event=stop)
    loaded = wd.load_state()
    assert "sales" in loaded
    assert "mike" in loaded
    assert loaded["sales"]["ok"] is True


def test_start_watchdog_thread_idempotent(monkeypatch, fake_gmail_ok):
    """Calling start twice must not spawn two threads."""
    wd.stop_watchdog_thread()
    monkeypatch.setenv("GMAIL_WATCHDOG_INTERVAL_SEC", "60")
    t1 = wd.start_watchdog_thread(interval_sec=60)
    try:
        t2 = wd.start_watchdog_thread(interval_sec=60)
        assert t1 is t2
        assert t1 is not None
    finally:
        wd.stop_watchdog_thread()


def test_start_watchdog_disabled_by_env(monkeypatch):
    monkeypatch.setenv("GMAIL_WATCHDOG_DISABLED", "1")
    wd.stop_watchdog_thread()
    result = wd.start_watchdog_thread(interval_sec=60)
    assert result is None
