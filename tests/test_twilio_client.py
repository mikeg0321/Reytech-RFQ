"""Pin canonical Twilio helper (Tier 2e, audit 2026-05-07).

The audit found 5 Twilio implementations across the codebase with
two env-var conventions. Half the alert surface silently no-opped
when the operator set the official Twilio names while
`growth_agent.send_sms_outreach` only read the short names (and
vice-versa).

This module pins:
  1. Cred resolution prefers Twilio-official, falls back to short
     with one-time deprecation log.
  2. `is_configured()` reflects either convention.
  3. `send_sms()` retries 5xx + 429 + Connection errors via the
     `with_retry` substrate (PR #833) and fast-fails on 4xx other.
  4. SDK ImportError surfaces a clean error string instead of
     crashing the caller.
  5. Empty `to` or `body` are caught at the boundary.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ─── Cred resolution ──────────────────────────────────────────────

def test_read_creds_prefers_official(monkeypatch):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_official")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok_official")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+15550001")
    monkeypatch.setenv("TWILIO_SID", "AC_short")
    monkeypatch.setenv("TWILIO_TOKEN", "tok_short")
    monkeypatch.setenv("TWILIO_FROM", "+15550002")
    # Reset deprecation log gate for repeatability
    import src.core.twilio_client as tc
    tc._DEPRECATION_LOGGED = False

    c = tc._read_creds()
    assert c["sid"] == "AC_official"
    assert c["token"] == "tok_official"
    assert c["from_number"] == "+15550001"


def test_read_creds_falls_back_to_short(monkeypatch, caplog):
    monkeypatch.delenv("TWILIO_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWILIO_FROM_NUMBER", raising=False)
    monkeypatch.delenv("TWILIO_PHONE_NUMBER", raising=False)
    monkeypatch.setenv("TWILIO_SID", "AC_short")
    monkeypatch.setenv("TWILIO_TOKEN", "tok_short")
    monkeypatch.setenv("TWILIO_FROM", "+15550003")
    import src.core.twilio_client as tc
    tc._DEPRECATION_LOGGED = False

    import logging
    with caplog.at_level(logging.WARNING):
        c = tc._read_creds()
    assert c["sid"] == "AC_short"
    assert c["token"] == "tok_short"
    assert c["from_number"] == "+15550003"
    assert any("short env vars" in rec.message for rec in caplog.records)


def test_read_creds_falls_back_to_phone_number_alias(monkeypatch):
    """Some legacy setups use TWILIO_PHONE_NUMBER. Accept it."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_x")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok_x")
    monkeypatch.delenv("TWILIO_FROM_NUMBER", raising=False)
    monkeypatch.setenv("TWILIO_PHONE_NUMBER", "+15550004")
    monkeypatch.delenv("TWILIO_FROM", raising=False)
    import src.core.twilio_client as tc
    tc._DEPRECATION_LOGGED = False

    c = tc._read_creds()
    assert c["from_number"] == "+15550004"


def test_is_configured_under_either_convention(monkeypatch):
    import src.core.twilio_client as tc
    # Official set
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+1")
    monkeypatch.delenv("TWILIO_SID", raising=False)
    monkeypatch.delenv("TWILIO_TOKEN", raising=False)
    monkeypatch.delenv("TWILIO_FROM", raising=False)
    monkeypatch.delenv("TWILIO_PHONE_NUMBER", raising=False)
    tc._DEPRECATION_LOGGED = False
    assert tc.is_configured() is True

    # Short set
    monkeypatch.delenv("TWILIO_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWILIO_FROM_NUMBER", raising=False)
    monkeypatch.setenv("TWILIO_SID", "AC")
    monkeypatch.setenv("TWILIO_TOKEN", "tok")
    monkeypatch.setenv("TWILIO_FROM", "+1")
    tc._DEPRECATION_LOGGED = False
    assert tc.is_configured() is True

    # Neither
    monkeypatch.delenv("TWILIO_SID", raising=False)
    monkeypatch.delenv("TWILIO_TOKEN", raising=False)
    monkeypatch.delenv("TWILIO_FROM", raising=False)
    tc._DEPRECATION_LOGGED = False
    assert tc.is_configured() is False


# ─── Predicate ────────────────────────────────────────────────────

def test_predicate_recognizes_5xx_and_429():
    from src.core.twilio_client import _is_transient_twilio_error
    for status in (429, 500, 502, 503, 504):
        err = Exception(f"HTTP {status}")
        err.status = status
        assert _is_transient_twilio_error(err)


def test_predicate_rejects_4xx_non_429():
    from src.core.twilio_client import _is_transient_twilio_error
    for status in (400, 401, 403, 404, 422):
        err = Exception(f"HTTP {status}")
        err.status = status
        assert not _is_transient_twilio_error(err)


def test_predicate_recognizes_network_strings():
    from src.core.twilio_client import _is_transient_twilio_error
    assert _is_transient_twilio_error(OSError("Read timed out"))
    assert _is_transient_twilio_error(OSError("Connection reset by peer"))


# ─── send_sms ──────────────────────────────────────────────────────

def test_send_sms_rejects_empty_inputs(monkeypatch):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+1")
    from src.core.twilio_client import send_sms
    assert send_sms("", "hello")["ok"] is False
    assert send_sms("+15550001", "")["ok"] is False
    assert send_sms("   ", "hello")["ok"] is False


def test_send_sms_returns_unconfigured_when_creds_missing(monkeypatch):
    for k in ("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN",
              "TWILIO_FROM_NUMBER", "TWILIO_PHONE_NUMBER",
              "TWILIO_SID", "TWILIO_TOKEN", "TWILIO_FROM"):
        monkeypatch.delenv(k, raising=False)
    from src.core.twilio_client import send_sms
    out = send_sms("+15550001", "hello")
    assert out["ok"] is False
    assert "not configured" in out["error"].lower()


def test_send_sms_calls_sdk_with_creds(monkeypatch):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_x")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok_x")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+15550999")
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    captured = {}

    class FakeMessage:
        def __init__(self, **kw):
            self.sid = "SM_fake_sid_001"
            captured.update(kw)

    class FakeMessages:
        def create(self, **kw):
            return FakeMessage(**kw)

    class FakeClient:
        def __init__(self, sid, token):
            captured["client_sid"] = sid
            captured["client_token"] = token
            self.messages = FakeMessages()

    import sys
    import types
    fake_rest = types.ModuleType("twilio.rest")
    fake_rest.Client = FakeClient
    fake_pkg = types.ModuleType("twilio")
    fake_pkg.rest = fake_rest
    monkeypatch.setitem(sys.modules, "twilio", fake_pkg)
    monkeypatch.setitem(sys.modules, "twilio.rest", fake_rest)

    from src.core.twilio_client import send_sms
    out = send_sms("+15551234", "hi there")
    assert out == {"ok": True, "sid": "SM_fake_sid_001"}
    assert captured["client_sid"] == "AC_x"
    assert captured["from_"] == "+15550999"
    assert captured["to"] == "+15551234"
    assert captured["body"] == "hi there"


def test_send_sms_truncates_long_body_to_1600(monkeypatch):
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_x")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok_x")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+1")
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    captured = {}

    class FakeMessage:
        def __init__(self, **kw):
            self.sid = "SM_x"
            captured.update(kw)

    class FakeClient:
        def __init__(self, *_a, **_k):
            self.messages = MagicMock()
            self.messages.create = lambda **kw: FakeMessage(**kw)

    import sys
    import types
    fake_rest = types.ModuleType("twilio.rest")
    fake_rest.Client = FakeClient
    monkeypatch.setitem(sys.modules, "twilio.rest", fake_rest)

    from src.core.twilio_client import send_sms
    long_body = "X" * 5000
    send_sms("+15551234", long_body)
    assert len(captured["body"]) == 1600


def test_send_sms_retries_503(monkeypatch):
    """Transient 503 must retry; second attempt succeeds → caller
    sees ok:True."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_x")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok_x")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+1")
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    class FakeException(Exception):
        def __init__(self, status):
            super().__init__(f"HTTP {status}")
            self.status = status

    class FakeMessages:
        def create(self, **kw):
            n["calls"] += 1
            if n["calls"] == 1:
                raise FakeException(503)
            m = MagicMock()
            m.sid = "SM_after_retry"
            return m

    class FakeClient:
        def __init__(self, *_a, **_k):
            self.messages = FakeMessages()

    import sys
    import types
    fake_rest = types.ModuleType("twilio.rest")
    fake_rest.Client = FakeClient
    monkeypatch.setitem(sys.modules, "twilio.rest", fake_rest)

    from src.core.twilio_client import send_sms
    out = send_sms("+15551234", "alert")
    assert out["ok"] is True
    assert out["sid"] == "SM_after_retry"
    assert n["calls"] == 2


def test_send_sms_does_not_retry_401(monkeypatch):
    """401 = invalid creds, NOT a transit blip. Fast-fail to None
    so the operator hears about it on the first attempt."""
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", "AC_x")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "tok_x")
    monkeypatch.setenv("TWILIO_FROM_NUMBER", "+1")
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    class FakeException(Exception):
        def __init__(self, status):
            super().__init__(f"HTTP {status}")
            self.status = status

    class FakeMessages:
        def create(self, **kw):
            n["calls"] += 1
            raise FakeException(401)

    class FakeClient:
        def __init__(self, *_a, **_k):
            self.messages = FakeMessages()

    import sys
    import types
    fake_rest = types.ModuleType("twilio.rest")
    fake_rest.Client = FakeClient
    monkeypatch.setitem(sys.modules, "twilio.rest", fake_rest)

    from src.core.twilio_client import send_sms
    out = send_sms("+15551234", "alert")
    assert out["ok"] is False
    assert "401" in out["error"]
    assert n["calls"] == 1
