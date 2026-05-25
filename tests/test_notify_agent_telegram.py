"""Telegram channel + reports-tier routing in notify_agent.

Substrate-tier tests for the 2026-05-25 oracle-substrate PR:
  - The reports-tier events (oracle_weekly, cross_sell_weekly, order_digest,
    scprs_pull_done, quote_lost_signal, award_tracker_idle) route to Telegram
    via CHANNEL_MAP and NOT to email by default.
  - Actionable events (cs_draft_ready, rfq_arrived, quote_won, po_received,
    server_error, email_permanent_failure) still route to SMS+email — they
    must NOT be regressed onto Telegram.
  - _send_telegram POSTs to the Bot API with the right shape and escapes
    MarkdownV2 reserved characters in the body.
  - When Telegram env vars are missing, the channel silently degrades —
    no exception, no failed alert (the bell + email backup paths still fire).
"""
from unittest.mock import patch, MagicMock

import pytest


# ── _send_telegram unit tests ─────────────────────────────────────────────


def test_send_telegram_posts_to_bot_api(monkeypatch):
    """One sendMessage POST with chat_id + bold title + MarkdownV2 parse mode."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "FAKE:TOKEN")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    captured = {}

    class _FakeResp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok": true, "result": {"message_id": 42}}'

    def _fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["data"] = req.data.decode("utf-8")
        return _FakeResp()

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        result = na._send_telegram(
            event_type="oracle_weekly",
            title="Oracle Weekly: 3W / 5L",
            body="Calibration ticked on 4 categories",
            urgency="info",
            context={"quote_number": "R26Q42"},
        )

    assert result["ok"] is True
    assert result["message_id"] == 42
    assert "api.telegram.org/botFAKE:TOKEN/sendMessage" in captured["url"]
    assert "chat_id=12345" in captured["data"]
    assert "parse_mode=MarkdownV2" in captured["data"]
    assert "Oracle+Weekly" in captured["data"]   # urlencoded title


def test_send_telegram_escapes_markdown_reserved_chars(monkeypatch):
    """Body with reserved MarkdownV2 chars must be backslash-escaped or
    Telegram returns 400 and we lose the message."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    captured = {}

    class _FakeResp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok": true, "result": {}}'

    def _fake_urlopen(req, timeout=None):
        captured["data"] = req.data.decode("utf-8")
        return _FakeResp()

    body_with_reserved = "lost 5 bids (3 on price) — markup +30.5%"
    with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        na._send_telegram(
            event_type="award_tracker_idle",
            title="award scanner idle",
            body=body_with_reserved,
            urgency="warning",
            context={},
        )

    # urlencoded: reserved chars get a leading backslash in the payload.
    # %5C is `\` urlencoded. We escape (, ), -, ., +, etc.
    data = captured["data"]
    assert "%5C%28" in data or "%5C(" in data   # `\(`
    assert "%5C." in data or "%5C%2E" in data   # `\.`


def test_send_telegram_no_config_returns_silently(monkeypatch):
    """Missing token/chat_id must not raise."""
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    result = na._send_telegram(
        event_type="oracle_weekly",
        title="x", body="y", urgency="info", context={},
    )
    assert result["ok"] is False
    assert "not configured" in result["reason"].lower()


# ── CHANNEL_MAP routing tests ──────────────────────────────────────────────


@pytest.mark.parametrize("event_type,expects_telegram", [
    ("oracle_weekly",      True),
    ("cross_sell_weekly",  True),
    ("order_digest",       True),
    ("scprs_pull_done",    True),
    ("award_tracker_idle", True),
    ("quote_lost_signal",  True),
    # Actionable — Telegram must NOT be in the default routing.
    ("cs_draft_ready",     False),
    ("rfq_arrived",        False),
    ("quote_won",          False),
    ("po_received",        False),
    ("server_error",       False),
    ("email_permanent_failure", False),
    ("invoice_unpaid",     False),
])
def test_channel_map_reports_route_to_telegram(monkeypatch, event_type, expects_telegram):
    """The reports tier must route through Telegram (or its degraded fallback);
    actionable events must NOT — they keep SMS/email."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    # Disable real send paths so we don't accidentally fire Twilio/Gmail.
    monkeypatch.setenv("NOTIFY_SMS", "false")
    monkeypatch.setenv("NOTIFY_EMAIL_ALERTS", "false")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    captured = {"called": False}

    def _fake_telegram(*a, **kw):
        captured["called"] = True
        return {"ok": True}

    with patch.object(na, "_send_telegram", side_effect=_fake_telegram), \
         patch.object(na, "_push_bell", return_value={"ok": True}), \
         patch.object(na, "_send_sms", return_value={"ok": True}), \
         patch.object(na, "_send_alert_email", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type=event_type,
            title="t", body="b", urgency="info",
            context={}, channels_override=None,
        )

    assert captured["called"] is expects_telegram, (
        f"{event_type}: expected telegram={expects_telegram}, got={captured['called']}"
    )


def test_channel_map_keeps_email_for_actionable_events(monkeypatch):
    """rfq_arrived / cs_draft_ready / po_received must still hit email."""
    monkeypatch.setenv("NOTIFY_EMAIL", "ops@example.com")
    monkeypatch.setenv("NOTIFY_EMAIL_ALERTS", "true")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    email_calls = []

    def _fake_email(*a, **kw):
        email_calls.append(a[0] if a else None)
        return {"ok": True}

    with patch.object(na, "_send_alert_email", side_effect=_fake_email), \
         patch.object(na, "_push_bell", return_value={"ok": True}), \
         patch.object(na, "_send_sms", return_value={"ok": True}), \
         patch.object(na, "_send_telegram", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        for ev in ("rfq_arrived", "cs_draft_ready", "po_received"):
            na._dispatch_alert(
                event_type=ev,
                title="t", body="b", urgency="urgent",
                context={}, channels_override=None,
            )

    assert email_calls == ["rfq_arrived", "cs_draft_ready", "po_received"], (
        f"email skipped for actionable events: {email_calls}"
    )


def test_channels_override_still_wins(monkeypatch):
    """Explicit channels=[...] still overrides CHANNEL_MAP — back-compat."""
    monkeypatch.setenv("NOTIFY_EMAIL", "ops@example.com")
    monkeypatch.setenv("NOTIFY_EMAIL_ALERTS", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    bell_calls = []
    email_calls = []
    with patch.object(na, "_push_bell", side_effect=lambda *a, **kw: bell_calls.append(1) or {"ok": True}), \
         patch.object(na, "_send_alert_email", side_effect=lambda *a, **kw: email_calls.append(1) or {"ok": True}), \
         patch.object(na, "_send_sms", return_value={"ok": True}), \
         patch.object(na, "_send_telegram", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type="oracle_weekly",   # default → telegram+bell
            title="t", body="b", urgency="info",
            context={},
            channels_override=["bell"],   # caller forces bell only
        )

    assert bell_calls == [1]
    assert email_calls == []
