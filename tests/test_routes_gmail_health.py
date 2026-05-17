"""Tests for /api/admin/gmail/health route surface."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

import src.agents.gmail_auth_watchdog as wd


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    state_file = tmp_path / "gmail_health.json"
    monkeypatch.setattr(wd, "_state_path", lambda: state_file)
    yield state_file


def _seed_state(persisted):
    wd._save_state(persisted)


def test_gmail_health_requires_auth(anon_client):
    r = anon_client.get("/api/admin/gmail/health")
    assert r.status_code == 401


def test_gmail_health_first_run_does_live_probe(auth_client, monkeypatch):
    """No persisted state yet → endpoint must return meaningful answer
    via a one-shot live probe (substrate must be useful from boot 0)."""
    import src.core.gmail_api as gmail_mod
    fake_svc = MagicMock()
    fake_svc.users().getProfile().execute.return_value = {
        "emailAddress": "sales@reytechinc.com",
    }
    monkeypatch.setattr(gmail_mod, "is_configured", lambda: True)
    monkeypatch.setattr(gmail_mod, "get_service", lambda inbox: fake_svc)

    r = auth_client.get("/api/admin/gmail/health")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["any_broken"] is False
    assert body["watchdog"]["source"] == "not_yet_run"
    assert "sales" in body["inboxes"]
    assert "mike" in body["inboxes"]


def test_gmail_health_persisted_returns_state(auth_client):
    _seed_state({
        "sales": {
            "ok": True, "error_class": "", "profile_email": "sales@reytechinc.com",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "state_changed_at": datetime.now(timezone.utc).isoformat(),
            "consecutive_failures": 0, "rewarn_count": 0, "last_alert_at": None,
        },
        "mike": {
            "ok": True, "error_class": "", "profile_email": "mike@reytechinc.com",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "state_changed_at": datetime.now(timezone.utc).isoformat(),
            "consecutive_failures": 0, "rewarn_count": 0, "last_alert_at": None,
        },
    })
    r = auth_client.get("/api/admin/gmail/health")
    assert r.status_code == 200
    body = r.get_json()
    assert body["any_broken"] is False
    assert body["watchdog"]["source"] == "persisted"
    assert body["inboxes"]["sales"]["ok"] is True


def test_gmail_health_surfaces_any_broken_rollup(auth_client):
    _seed_state({
        "sales": {"ok": True, "error_class": "", "checked_at": "2026-05-17T00:00:00Z"},
        "mike": {"ok": False, "error_class": "invalid_grant",
                  "checked_at": "2026-05-17T00:00:00Z"},
    })
    r = auth_client.get("/api/admin/gmail/health")
    body = r.get_json()
    assert body["any_broken"] is True
    assert body["inboxes"]["mike"]["error_class"] == "invalid_grant"


def test_gmail_health_live_param_forces_fresh_probe(
    auth_client, monkeypatch,
):
    """?live=1 bypasses persisted state — for "is it healthy NOW" checks."""
    _seed_state({
        "sales": {"ok": True, "checked_at": "2020-01-01T00:00:00Z"},  # stale
    })
    import src.core.gmail_api as gmail_mod
    def _explode(*a, **kw):
        raise Exception("invalid_client: live probe found a real break")
    monkeypatch.setattr(gmail_mod, "is_configured", lambda: True)
    monkeypatch.setattr(gmail_mod, "get_service", _explode)

    r = auth_client.get("/api/admin/gmail/health?live=1")
    body = r.get_json()
    assert body["watchdog"]["source"] == "live_probe"
    assert body["any_broken"] is True
    assert body["inboxes"]["sales"]["error_class"] == "invalid_client"


def test_gmail_health_missing_inbox_in_state_filled_with_no_data(auth_client):
    """Operator views the dashboard before watchdog has covered every inbox.
    Substrate guarantee: every monitored inbox appears in the response."""
    _seed_state({
        # mike row missing — first watchdog iteration only got to sales
        "sales": {"ok": True, "checked_at": "2026-05-17T00:00:00Z"},
    })
    r = auth_client.get("/api/admin/gmail/health")
    body = r.get_json()
    assert "mike" in body["inboxes"]
    assert body["inboxes"]["mike"]["error_class"] == "no_data_yet"
