"""Tests for `_build_gmail_send_card` — the /health/quoting card that
surfaces Gmail outbound-send health.

Companion to `test_email_poll_card.py` (inbound poll). Locks the
5-state traffic-light semantics so a future tweak to thresholds doesn't
silently flip a healthy operator into red:

    error    → ≥1 failed send in last 24h
    stale    → no successful send in >7 days
    warn     → last successful send 24h-7d ago
    healthy  → sent within 24h, no failures in 24h
    unknown  → email_outbox missing or no sent rows ever

Plan §4.3 sub-2 lever: paired with the inbound-poll card, both gaps in
the receive→send loop are now visible to the operator on /health/quoting.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


def _build():
    """Late-import the function under test inside each call.

    Mirrors the pattern in test_email_poll_card.py — module-level import
    of `routes_health` collides with conftest's `create_app()` re-runs."""
    from src.api.modules.routes_health import _build_gmail_send_card
    return _build_gmail_send_card()


def _seed_outbox(rows):
    """Insert a list of dicts as email_outbox rows on the per-test isolated
    DB. Uses the conftest's seeded DB (each test runs against a fresh temp
    file). Only fields we care about for the card are populated.
    """
    from src.core.db import get_db
    with get_db() as conn:
        # Wipe whatever conftest may have seeded so the baseline is empty.
        conn.execute("DELETE FROM email_outbox")
        for r in rows:
            conn.execute("""
                INSERT INTO email_outbox
                  (id, created_at, status, type, to_address, subject, body,
                   sent_at, last_error, retry_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r["id"], r.get("created_at", _iso(0)),
                r["status"], r.get("type", "quote"),
                r.get("to_address", "buyer@example.gov"),
                r.get("subject", "Quote 12345"),
                r.get("body", ""),
                r.get("sent_at", ""),
                r.get("last_error", ""),
                r.get("retry_count", 0),
            ))
        conn.commit()


def _iso(seconds_ago: int) -> str:
    return (datetime.now() - timedelta(seconds=seconds_ago)).isoformat()


def _iso_aware(seconds_ago: int) -> str:
    """TZ-aware variant — mirrors what the prod write path actually stores
    (sent_at is written via datetime.now().isoformat() in routes_crm.py
    which on a UTC server produces UTC-naive). We test both shapes."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


# ── Status semantics ────────────────────────────────────────────────────


def test_unknown_when_outbox_empty():
    """No rows means we have no signal — render grey, not green or red."""
    _seed_outbox([])
    out = _build()
    assert out["status"] == "unknown"
    assert out["lag_seconds"] is None
    assert out["sent_24h"] == 0
    assert out["failed_24h"] == 0


def test_healthy_when_recent_send_no_failures():
    _seed_outbox([
        {"id": "e1", "status": "sent",
         "sent_at": _iso(60 * 30),  # 30 min ago
         "created_at": _iso(60 * 31)},
    ])
    out = _build()
    assert out["status"] == "healthy"
    assert out["sent_24h"] == 1
    assert out["sent_7d"] == 1
    assert out["failed_24h"] == 0
    assert out["lag_seconds"] is not None and out["lag_seconds"] < 60 * 60


def test_warn_when_last_send_2_days_ago():
    _seed_outbox([
        {"id": "e1", "status": "sent",
         "sent_at": _iso(2 * 86400),
         "created_at": _iso(2 * 86400 + 60)},
    ])
    out = _build()
    assert out["status"] == "warn"
    assert out["sent_24h"] == 0
    assert out["sent_7d"] == 1


def test_stale_when_last_send_over_7_days_ago():
    _seed_outbox([
        {"id": "e1", "status": "sent",
         "sent_at": _iso(10 * 86400),
         "created_at": _iso(10 * 86400 + 60)},
    ])
    out = _build()
    assert out["status"] == "stale"
    assert out["sent_24h"] == 0
    assert out["sent_7d"] == 0


def test_error_overrides_recent_success():
    """Originating concern: an OAuth refresh failure mid-day shouldn't be
    masked by the morning's successful send. Failure in 24h ALWAYS wins."""
    _seed_outbox([
        {"id": "e_ok", "status": "sent",
         "sent_at": _iso(60 * 60),  # 1h ago
         "created_at": _iso(60 * 60 + 60)},
        {"id": "e_fail", "status": "failed",
         "created_at": _iso(60 * 30),  # 30 min ago failure
         "last_error": "OAuth token refresh failed: invalid_grant"},
    ])
    out = _build()
    assert out["status"] == "error"
    assert out["failed_24h"] == 1
    assert "invalid_grant" in out["last_error"]


def test_error_includes_permanently_failed():
    _seed_outbox([
        {"id": "ef", "status": "permanently_failed",
         "created_at": _iso(60 * 60),
         "last_error": "Quota exceeded"},
    ])
    out = _build()
    assert out["status"] == "error"
    assert out["failed_24h"] == 1


def test_old_failure_does_not_count():
    """A failure 2 days ago should NOT keep the card red forever — only
    failures in the last 24h drive the error state."""
    _seed_outbox([
        {"id": "e_ok", "status": "sent",
         "sent_at": _iso(60 * 30),
         "created_at": _iso(60 * 31)},
        {"id": "e_old_fail", "status": "failed",
         "created_at": _iso(2 * 86400),
         "last_error": "old error"},
    ])
    out = _build()
    assert out["status"] == "healthy"
    assert out["failed_24h"] == 0


def test_pending_drafts_counts_all_unsent_states():
    _seed_outbox([
        {"id": "d1", "status": "draft", "created_at": _iso(60)},
        {"id": "d2", "status": "cs_draft", "created_at": _iso(60)},
        {"id": "d3", "status": "outreach_draft", "created_at": _iso(60)},
        {"id": "d4", "status": "approved", "created_at": _iso(60)},
        {"id": "s1", "status": "sent",
         "sent_at": _iso(60), "created_at": _iso(120)},
    ])
    out = _build()
    assert out["pending_drafts"] == 4
    assert out["sent_24h"] == 1


# ── TZ-aware lag (the same bug PR #617 fixed for the inbound card) ─────


def test_lag_handles_tz_aware_sent_at():
    """sent_at may be written TZ-aware ('…+00:00' or '…-07:00'). The lag
    computation must compare in UTC when aware to avoid the 7-8h
    over-report PR #617 fixed for the inbound card. Lock the same
    invariant for outbound here so a future write-path TZ change can't
    silently flip the dashboard red."""
    _seed_outbox([
        {"id": "e1", "status": "sent",
         "sent_at": _iso_aware(60 * 30),
         "created_at": _iso_aware(60 * 31)},
    ])
    out = _build()
    assert out["status"] == "healthy", (
        f"got {out['status']}, lag={out['lag_seconds']} — expected healthy")
    assert out["lag_seconds"] is not None and out["lag_seconds"] < 60 * 60


# ── Shape contract ──────────────────────────────────────────────────────


def test_response_shape_is_stable():
    """Templates rely on these keys — any rename is a breaking change."""
    _seed_outbox([])
    out = _build()
    expected = {
        "status", "last_send_at", "lag_seconds", "lag_human",
        "sent_24h", "sent_7d", "failed_24h", "pending_drafts", "last_error",
    }
    assert set(out.keys()) == expected


def test_last_error_truncated_to_200_chars():
    _seed_outbox([
        {"id": "ef", "status": "failed",
         "created_at": _iso(60),
         "last_error": "x" * 500},
    ])
    out = _build()
    assert len(out["last_error"]) == 200


def test_safe_default_returned_when_db_query_raises(monkeypatch):
    """Schema-tolerance: if anything in the SQL path raises (table
    missing on a fresh boot, malformed row, etc.) the card must return
    its safe default — never bubble up and crash /health/quoting."""
    from src.core import db as _db
    class _BoomConn:
        def __enter__(self): raise RuntimeError("simulated db error")
        def __exit__(self, *a): return False
    monkeypatch.setattr(_db, "get_db", lambda: _BoomConn())
    out = _build()
    assert out["status"] == "unknown"
    assert out["sent_24h"] == 0
    assert out["last_send_at"] == ""
