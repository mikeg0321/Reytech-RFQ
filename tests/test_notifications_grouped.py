"""Pin: /notifications grouped-by-event-type view + API.

PR-C (back-window audit 2026-05-26): Mike's bell archive accretes
quickly when liveness sweeps fire daily. Without grouping he can't
see at a glance "this fires daily, ignore" vs "new today, investigate."

Tests pin:
  1. /api/notifications/grouped JSON shape + groups by event_type
  2. Resolved-pairing: base event paired with its _recovered event,
     resolved flag set when _recovered last_seen is newer than base
  3. /notifications page renders (no UndefinedError on template)
  4. Empty state (no rows in window) renders without crashing
  5. days= query param clamps to [1, 90]
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest


def _seed_notification(conn, event_type, title="t", body="b",
                       urgency="info", created_at=None):
    conn.execute(
        "INSERT INTO notifications "
        "(created_at, event_type, urgency, title, body) "
        "VALUES (?, ?, ?, ?, ?)",
        (created_at or datetime.utcnow().isoformat(),
         event_type, urgency, title, body),
    )


def _purge_notifications(conn):
    """Test isolation — strip any pre-existing notifications so the
    grouped query is deterministic for this test."""
    conn.execute("DELETE FROM notifications")


# ─── API endpoint ────────────────────────────────────────────────────


def test_grouped_api_returns_one_row_per_event_type(auth_client):
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)
        _seed_notification(conn, "gmail_oauth_expired", title="silent 24h",
                           urgency="warning")
        _seed_notification(conn, "gmail_oauth_expired", title="silent 48h",
                           urgency="warning")
        _seed_notification(conn, "external_service_disconnected",
                           title="award_tracker", urgency="warning")

    r = auth_client.get("/api/notifications/grouped?days=7")
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    events = {g["event_type"]: g for g in data["groups"]}
    assert "gmail_oauth_expired" in events
    assert "external_service_disconnected" in events
    assert events["gmail_oauth_expired"]["count"] == 2
    assert events["external_service_disconnected"]["count"] == 1


def test_grouped_api_pairs_recovered_event(auth_client):
    """A base event with a NEWER _recovered companion is marked resolved."""
    from src.core.db import get_db
    base_ts = (datetime.utcnow() - timedelta(hours=2)).isoformat()
    recovered_ts = datetime.utcnow().isoformat()
    with get_db() as conn:
        _purge_notifications(conn)
        _seed_notification(conn, "gmail_oauth_expired",
                           title="silent 24h", created_at=base_ts)
        _seed_notification(conn, "gmail_oauth_expired_recovered",
                           title="recovered", created_at=recovered_ts)

    r = auth_client.get("/api/notifications/grouped?days=7")
    data = r.get_json()
    events = {g["event_type"]: g for g in data["groups"]}
    assert events["gmail_oauth_expired"]["resolved"] is True
    assert events["gmail_oauth_expired"]["resolved_at"] == recovered_ts
    # The _recovered row itself is also returned (history is preserved)
    assert events["gmail_oauth_expired_recovered"]["is_recovered_event"] is True


def test_grouped_api_does_not_resolve_when_recovered_is_older(auth_client):
    """A _recovered card OLDER than the latest base alert is stale —
    the base event is NOT resolved."""
    from src.core.db import get_db
    old_recovered = (datetime.utcnow() - timedelta(hours=5)).isoformat()
    new_base = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    with get_db() as conn:
        _purge_notifications(conn)
        _seed_notification(conn, "gmail_oauth_expired_recovered",
                           created_at=old_recovered)
        _seed_notification(conn, "gmail_oauth_expired",
                           title="re-broke", created_at=new_base)
    r = auth_client.get("/api/notifications/grouped?days=7")
    data = r.get_json()
    events = {g["event_type"]: g for g in data["groups"]}
    assert events["gmail_oauth_expired"]["resolved"] is False


def test_grouped_api_returns_empty_groups_when_no_data(auth_client):
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)
    r = auth_client.get("/api/notifications/grouped?days=1")
    data = r.get_json()
    assert data["ok"] is True
    assert data["groups"] == []


def test_grouped_api_clamps_days_param(auth_client):
    r = auth_client.get("/api/notifications/grouped?days=99999")
    data = r.get_json()
    assert data["days"] == 90  # clamped to max
    r2 = auth_client.get("/api/notifications/grouped?days=0")
    data2 = r2.get_json()
    assert data2["days"] == 1  # clamped to min
    r3 = auth_client.get("/api/notifications/grouped?days=bogus")
    data3 = r3.get_json()
    assert data3["days"] == 7  # default fallback


# ─── /notifications page renders ─────────────────────────────────────


def test_notifications_page_renders_empty_state(auth_client):
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)
    r = auth_client.get("/notifications?days=1")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "No notifications in the last 1 day" in body


def test_notifications_page_renders_filled_state(auth_client):
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)
        _seed_notification(conn, "gmail_oauth_expired",
                           title="⚠️ Gmail inbound poller: silent 96h",
                           body="Detail: ...",
                           urgency="warning")
    r = auth_client.get("/notifications?days=7")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "Gmail inbound poller" in body
    assert "gmail_oauth_expired" in body
    # Default 7d window button is highlighted
    assert "7d" in body


def test_notifications_page_renders_resolved_indicator(auth_client):
    """The RESOLVED pill appears when a _recovered companion is newer."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge_notifications(conn)
        _seed_notification(conn, "scprs_pull_failed_persistent",
                           title="silent 24d",
                           created_at=(datetime.utcnow() - timedelta(hours=2)).isoformat())
        _seed_notification(conn, "scprs_pull_failed_persistent_recovered",
                           title="recovered",
                           created_at=datetime.utcnow().isoformat())
    r = auth_client.get("/notifications?days=7")
    body = r.get_data(as_text=True)
    assert "RESOLVED" in body
