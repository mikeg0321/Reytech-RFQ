"""Pins the 2026-05-27 fix: _telegram_api surfaces Telegram's JSON
`description` field on HTTP 4xx/5xx, not just the urllib status code.

Prod log 2026-05-27 04:22:33 (rfq-app boot):
  [wrn] Telegram API setWebhook failed: HTTP Error 400: Bad Request
  [wrn] Telegram setWebhook returned: {'ok': False, 'error': 'HTTP Error 400: Bad Request'}

The actual reason for the 400 (invalid URL? wrong secret_token? URL
not HTTPS?) was buried in the Telegram response body — and lost because
the catch-all `except Exception` only captured `str(HTTPError)`.

Post-fix: a 400 produces logs and a return dict that include Telegram's
own `description` (e.g. "Bad Request: bad webhook: An HTTPS URL must be
provided for webhook"). Operator can fix the config without a debug
round-trip.
"""
from __future__ import annotations

import io
import json
import os

import pytest
import urllib.error

from src.api.modules import routes_telegram


@pytest.fixture(autouse=True)
def _set_token(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-not-real")


def _make_http_error(code: int, body: bytes) -> urllib.error.HTTPError:
    """Build an HTTPError whose .read() returns the given body."""
    return urllib.error.HTTPError(
        url="https://api.telegram.org/bot/test",
        code=code,
        msg="Bad Request",
        hdrs=None,
        fp=io.BytesIO(body),
    )


def test_http_400_surfaces_telegram_description(monkeypatch, caplog):
    """A 400 with a JSON body MUST surface the `description` field —
    not just `HTTP Error 400: Bad Request`."""
    body = json.dumps({
        "ok": False,
        "error_code": 400,
        "description": (
            "Bad Request: bad webhook: An HTTPS URL must be "
            "provided for webhook"
        ),
    }).encode("utf-8")

    def fake_urlopen(req, timeout=None):
        raise _make_http_error(400, body)

    monkeypatch.setattr(
        "src.api.modules.routes_telegram.urllib.request.urlopen",
        fake_urlopen,
    )

    import logging
    with caplog.at_level(logging.WARNING, logger="reytech.telegram_webhook"):
        result = routes_telegram._telegram_api("setWebhook", {"url": "x"})

    assert result["ok"] is False
    assert result["error_code"] == 400
    assert "An HTTPS URL must be provided" in result["description"], (
        "description from Telegram body must propagate to the return dict"
    )
    # Log line must include the description, not the bare status line
    log_text = " ".join(r.getMessage() for r in caplog.records)
    assert "An HTTPS URL must be provided" in log_text, (
        "log must include Telegram's description so operator sees WHY"
    )


def test_http_400_with_empty_body_falls_back_to_status(monkeypatch):
    """If Telegram returns no body (weird edge case), don't crash —
    fall back to a useful string built from the status code."""

    def fake_urlopen(req, timeout=None):
        raise _make_http_error(400, b"")

    monkeypatch.setattr(
        "src.api.modules.routes_telegram.urllib.request.urlopen",
        fake_urlopen,
    )
    result = routes_telegram._telegram_api("setWebhook", {"url": "x"})
    assert result["ok"] is False
    assert result["error_code"] == 400
    # Description is empty-string after json parse; that's acceptable
    # as long as the function doesn't raise.
    assert "error" in result


def test_http_400_with_non_json_body_returns_truncated_text(monkeypatch):
    """Some non-Telegram error responses (gateway 4xx) won't be JSON.
    Return the first 200 chars of the body, not crash."""

    body = b"<html><body><h1>Bad Gateway</h1></body></html>"

    def fake_urlopen(req, timeout=None):
        raise _make_http_error(400, body)

    monkeypatch.setattr(
        "src.api.modules.routes_telegram.urllib.request.urlopen",
        fake_urlopen,
    )
    result = routes_telegram._telegram_api("setWebhook", {"url": "x"})
    assert result["ok"] is False
    assert result["error_code"] == 400
    assert "Bad Gateway" in result["description"]


def test_success_path_unaffected(monkeypatch):
    """The happy path must still return Telegram's parsed JSON unchanged."""

    class _FakeResp:
        def read(self):
            return b'{"ok":true,"result":true}'
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        "src.api.modules.routes_telegram.urllib.request.urlopen",
        lambda req, timeout=None: _FakeResp(),
    )
    result = routes_telegram._telegram_api("setWebhook", {"url": "x"})
    assert result == {"ok": True, "result": True}


def test_network_error_still_caught(monkeypatch):
    """Non-HTTP failures (connection refused, timeout) still hit the
    catch-all `except Exception` — preserved behavior."""

    def fake_urlopen(req, timeout=None):
        raise ConnectionError("Connection refused")

    monkeypatch.setattr(
        "src.api.modules.routes_telegram.urllib.request.urlopen",
        fake_urlopen,
    )
    result = routes_telegram._telegram_api("setWebhook", {"url": "x"})
    assert result["ok"] is False
    assert "Connection refused" in result["error"]
