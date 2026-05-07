"""Audit B-1/B-2/B-3 live prod failure pins (2026-05-07).

Three tiny tests, one per fix. Each pins the specific shape of the
prod failure described in `docs/AUDIT_DEEP_E2E_2026_05_07.md` so a
future regression triggers a red CI line not a silent prod incident.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ─────────────────── B-1: order_dal._item_status string-safe ───────────────────

def test_b1_item_status_handles_string_legacy_item():
    """Legacy line items occasionally arrive as bare description strings.
    `_item_status` must not crash with AttributeError."""
    from src.core.order_dal import _item_status
    # Prod crash shape: item is a string, not a dict
    assert _item_status("Widget A description") == "pending"
    assert _item_status(None) == "pending"
    assert _item_status([]) == "pending"
    assert _item_status({}) == "pending"
    assert _item_status({"sourcing_status": "ordered"}) == "ordered"


# ─────────────────── B-2: invoice_processor._get_email_config ───────────────────

def test_b2_get_email_config_returns_dict_from_env(monkeypatch):
    """`_get_email_config` was called but never defined → poller raised
    NameError at boot. Helper must exist and return a dict with `email`
    and `password` keys."""
    monkeypatch.setenv("GMAIL_ADDRESS", "ops@example.com")
    monkeypatch.setenv("GMAIL_PASSWORD", "secret")
    from src.agents.invoice_processor import _get_email_config
    cfg = _get_email_config()
    assert cfg["email"] == "ops@example.com"
    assert cfg["password"] == "secret"


def test_b2_get_email_config_empty_when_unset(monkeypatch):
    monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("GMAIL_PASSWORD", raising=False)
    from src.agents.invoice_processor import _get_email_config
    cfg = _get_email_config()
    assert cfg == {"email": "", "password": ""}


def test_b2_start_invoice_poller_handles_unconfigured(monkeypatch, caplog):
    """Poller must log+return cleanly when GMAIL_ADDRESS unset (was
    crashing with NameError before B-2)."""
    monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
    from src.agents.invoice_processor import start_invoice_poller
    # Should not raise
    start_invoice_poller()


# ─────────────────── B-3: Gmail _with_gmail_retry transient retry ───────────────────

def test_b3_with_retry_succeeds_after_transient():
    """Transient IncompleteRead retried, then succeeds."""
    from src.core.gmail_api import _with_gmail_retry
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise OSError("IncompleteRead occurred during read")
        return {"ok": True}

    out = _with_gmail_retry(flaky, op="test", base_delay=0.001)
    assert out == {"ok": True}
    assert calls["n"] == 2


def test_b3_with_retry_raises_on_non_transient():
    """4xx/5xx and other non-transport errors raise on first try."""
    from src.core.gmail_api import _with_gmail_retry

    def perm():
        raise ValueError("400 Bad Request: bogus query")

    with pytest.raises(ValueError):
        _with_gmail_retry(perm, op="test", base_delay=0.001)


def test_b3_with_retry_gives_up_after_max_attempts():
    """Persistent transient error eventually raises (doesn't retry forever)."""
    from src.core.gmail_api import _with_gmail_retry
    calls = {"n": 0}

    def always_transient():
        calls["n"] += 1
        raise OSError("[SSL] record layer failure")

    with pytest.raises(OSError):
        _with_gmail_retry(always_transient, op="test",
                          attempts=3, base_delay=0.001)
    assert calls["n"] == 3


def test_b3_is_transient_recognises_known_shapes():
    from src.core.gmail_api import _is_transient_gmail_error
    assert _is_transient_gmail_error(OSError("IncompleteRead"))
    assert _is_transient_gmail_error(OSError("[SSL] record layer failure"))
    assert _is_transient_gmail_error(OSError("Connection reset by peer"))
    assert _is_transient_gmail_error(OSError("Connection aborted"))
    assert _is_transient_gmail_error(TimeoutError("TimeoutError"))
    # Definitely-not transient
    assert not _is_transient_gmail_error(ValueError("400 Bad Request"))
    assert not _is_transient_gmail_error(KeyError("messages"))


def test_b3_list_message_ids_retries_transient(monkeypatch):
    """The wrapper is wired into list_message_ids — verify a transient
    error on .execute() retries instead of failing the whole call."""
    from src.core import gmail_api
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    request = MagicMock()
    call_count = {"n": 0}

    def execute():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise OSError("IncompleteRead")
        return {"messages": [{"id": "m1"}, {"id": "m2"}]}

    request.execute = execute
    service = MagicMock()
    service.users().messages().list.return_value = request
    service.users().messages().list_next.return_value = None

    ids = gmail_api.list_message_ids(service, query="x", max_results=10)
    assert ids == ["m1", "m2"]
    assert call_count["n"] == 2  # one transient retry
