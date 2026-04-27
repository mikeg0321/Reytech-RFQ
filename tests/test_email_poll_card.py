"""Tests for `_build_email_poll_card` — the /health/quoting card that
surfaces email poller lag.

Locks the 6-state traffic-light semantics so a future tweak to the
thresholds doesn't silently flip a healthy operator into red:

    error    → POLL_STATUS["error"] non-empty
    paused   → POLL_STATUS["paused"] truthy
    stale    → last_check > 15 minutes ago
    warn     → last_check > 5 minutes ago
    healthy  → last_check ≤ 5 minutes ago
    unknown  → POLL_STATUS missing or last_check absent

Plan §4.3 lever: if polling stalls the operator never sees the next
RFQ, so time-to-send blows out and the §4.1 KPI degrades. The card
makes that condition visible at a glance instead of only via
`/api/poll-now` JSON.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest


def _iso(seconds_ago: int) -> str:
    return (datetime.now() - timedelta(seconds=seconds_ago)).isoformat()


def _build(poll_status=None):
    """Late-import the function under test inside each call.

    Module-level imports of `routes_health` register `@bp.route` decorators
    onto the shared blueprint. Combined with the conftest's app fixture
    that re-runs `create_app()`, that triggers a double-registration
    error. Pattern matches `test_oracle_health_card.py`.
    """
    from src.api.modules.routes_health import _build_email_poll_card
    return _build_email_poll_card(poll_status)


def _fmt(seconds):
    from src.api.modules.routes_health import _format_lag
    return _format_lag(seconds)


# ── Status semantics ────────────────────────────────────────────────────


def test_healthy_when_last_check_fresh():
    out = _build({
        "last_check": _iso(30), "running": True, "paused": False,
        "error": None, "emails_found": 12,
    })
    assert out["status"] == "healthy"
    assert out["lag_seconds"] is not None and 0 <= out["lag_seconds"] <= 60
    assert out["running"] is True
    assert out["paused"] is False
    assert "ago" in out["lag_human"]


def test_warn_when_last_check_5_to_15_min_ago():
    out = _build({
        "last_check": _iso(7 * 60), "running": True, "paused": False,
        "error": None,
    })
    assert out["status"] == "warn"


def test_stale_when_last_check_over_15_min_ago():
    out = _build({
        "last_check": _iso(20 * 60), "running": True, "paused": False,
        "error": None,
    })
    assert out["status"] == "stale"


def test_error_overrides_recency():
    """Even if the most recent cycle finished 30s ago, a non-empty error
    field means the operator should see red — the most recent CYCLE
    might've succeeded, but Mike still needs the failure visible."""
    out = _build({
        "last_check": _iso(30), "running": True, "paused": False,
        "error": "imap auth failed: invalid grant",
    })
    assert out["status"] == "error"
    assert "imap auth failed" in out["error"]


def test_paused_overrides_lag_but_not_error():
    out = _build({
        "last_check": _iso(30), "running": True, "paused": True,
        "error": None,
    })
    assert out["status"] == "paused"

    # paused + error → error wins (we want failure surfaced over deferred)
    out = _build({
        "last_check": _iso(30), "running": True, "paused": True,
        "error": "boom",
    })
    assert out["status"] == "error"


def test_unknown_when_poll_status_missing_entirely():
    """No POLL_STATUS dict at all (e.g. boot-time call before the poll
    thread starts) — return a defined shape, not a crash."""
    out = _build({})
    assert out["status"] == "unknown"
    assert out["lag_seconds"] is None
    assert out["lag_human"] == "—"

    # Non-dict input also resolves to unknown (defensive against weird state)
    out = _build("not-a-dict")
    assert out["status"] == "unknown"


def test_unknown_when_dashboard_import_returns_none(monkeypatch):
    """Default-arg path: when caller passes no POLL_STATUS, the function
    falls through to importing it from `src.api.dashboard`. If that
    import yields None (e.g. dashboard hasn't been loaded yet, or the
    POLL_STATUS attribute was wiped), the card must surface 'unknown'
    rather than crashing."""
    import src.api.dashboard as _dash
    monkeypatch.setattr(_dash, "POLL_STATUS", None, raising=False)
    out = _build()
    assert out["status"] == "unknown"


def test_unknown_when_last_check_unparseable():
    out = _build({
        "last_check": "not-a-timestamp", "running": True,
        "paused": False, "error": None,
    })
    assert out["status"] == "unknown"
    assert out["lag_seconds"] is None


# ── Shape contract ──────────────────────────────────────────────────────


def test_response_shape_is_stable():
    """Templates rely on these keys — any rename is a breaking change."""
    out = _build({})
    expected_keys = {
        "status", "running", "paused", "last_check_at",
        "lag_seconds", "lag_human", "error", "emails_found_lifetime",
    }
    assert set(out.keys()) == expected_keys


def test_error_field_truncated_to_200_chars():
    """Long stack traces shouldn't blow up the card layout."""
    out = _build({
        "last_check": _iso(30),
        "error": "x" * 500,
    })
    assert len(out["error"]) == 200


def test_emails_found_lifetime_default_zero():
    out = _build({"last_check": _iso(30)})
    assert out["emails_found_lifetime"] == 0


# ── _format_lag thresholds ──────────────────────────────────────────────


def test_format_lag_thresholds():
    assert _fmt(None) == "—"
    assert _fmt(0) == "0s ago"
    assert _fmt(45) == "45s ago"
    assert _fmt(60) == "1m ago"
    assert _fmt(7 * 60) == "7m ago"
    assert _fmt(3600) == "1h ago"
    assert _fmt(86400) == "1d ago"
    assert _fmt(3 * 86400) == "3d ago"


