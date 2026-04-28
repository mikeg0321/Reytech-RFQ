"""Tests for `_build_pending_drafts_breakdown_card` — the /health/quoting
card that breaks down the email_outbox pending pile by status × age.

PR #618's Gmail send card surfaces "N pending drafts" as a single
integer. This card answers WHAT is in that pile so an operator can
decide "purge vs review queue" without leaving the page.

The card was added because PR #618 surfaced 268 drafts on prod — too
many to investigate row-by-row, but old enough rows (>30d) are a
strong signal that the pile is accruing rather than draining.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest


def _build():
    from src.api.modules.routes_health import _build_pending_drafts_breakdown_card
    return _build_pending_drafts_breakdown_card()


def _wipe(conn):
    """email_outbox is in SCHEMA already — id is TEXT PRIMARY KEY,
    recipient column is `to_address`."""
    conn.execute("DELETE FROM email_outbox")
    conn.commit()


_SEQ = [0]


def _seed(conn, *, status="draft", recipient="x@y.gov", subject="test",
          days_ago=0):
    when = (datetime.now() - timedelta(days=days_ago)).isoformat()
    _SEQ[0] += 1
    rid = f"test-{_SEQ[0]}-{int(datetime.now().timestamp()*1000)}"
    conn.execute("""
        INSERT INTO email_outbox (id, status, to_address, subject, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (rid, status, recipient, subject, when))


# ── Empty / unknown ─────────────────────────────────────────────────────


def test_healthy_when_zero_pending():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
    out = _build()
    assert out["status"] == "healthy"
    assert out["total"] == 0
    assert out["by_status"] == {}


def test_unknown_when_table_missing(monkeypatch):
    """If the email_outbox table doesn't exist, the card returns its
    safe default rather than crashing /health/quoting."""
    from src.api.modules import routes_health as _rh

    class _Boom:
        def __enter__(self): raise RuntimeError("simulated DB failure")
        def __exit__(self, *a): return False

    monkeypatch.setattr(_rh, "get_db", lambda: _Boom())
    out = _build()
    assert out["status"] == "unknown"
    assert out["total"] == 0


# ── Status thresholds ───────────────────────────────────────────────────


def test_warn_when_small_working_pile():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(10):
            _seed(c, days_ago=0)
        c.commit()
    out = _build()
    assert out["status"] == "warn"
    assert out["total"] == 10
    assert out["by_age"]["lt_1d"] == 10


def test_error_when_50_or_more_pending():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(50):
            _seed(c, days_ago=0)
        c.commit()
    out = _build()
    assert out["status"] == "error"


def test_error_when_any_row_is_over_30d_old():
    """The genuine triage signal — even a single >30d row means the
    pile is accruing, regardless of total size."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, days_ago=0)   # working pile
        _seed(c, days_ago=45)  # the smoking gun
        c.commit()
    out = _build()
    assert out["status"] == "error"
    assert out["by_age"]["30_90d"] == 1


# ── Status + age axes ───────────────────────────────────────────────────


def test_groups_by_status():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(3):
            _seed(c, status="draft", days_ago=0)
        for i in range(2):
            _seed(c, status="cs_draft", days_ago=0)
        _seed(c, status="follow_up_draft", days_ago=0)
        c.commit()
    out = _build()
    assert out["by_status"]["draft"] == 3
    assert out["by_status"]["cs_draft"] == 2
    assert out["by_status"]["follow_up_draft"] == 1


def test_buckets_into_correct_age_bands():
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, days_ago=0)    # <1d
        _seed(c, days_ago=3)    # 1-7d
        _seed(c, days_ago=14)   # 7-30d
        _seed(c, days_ago=60)   # 30-90d
        _seed(c, days_ago=120)  # >90d
        c.commit()
    out = _build()
    assert out["by_age"]["lt_1d"] == 1
    assert out["by_age"]["1_7d"] == 1
    assert out["by_age"]["7_30d"] == 1
    assert out["by_age"]["30_90d"] == 1
    assert out["by_age"]["gt_90d"] == 1


def test_excludes_sent_and_failed_rows():
    """Only pending statuses count. status='sent' or 'failed' rows are
    not the operator's triage problem."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, status="sent", days_ago=0)
        _seed(c, status="failed", days_ago=0)
        _seed(c, status="permanently_failed", days_ago=0)
        _seed(c, status="draft", days_ago=0)
        c.commit()
    out = _build()
    assert out["total"] == 1
    assert "sent" not in out["by_status"]
    assert "failed" not in out["by_status"]


# ── Samples ─────────────────────────────────────────────────────────────


def test_samples_show_oldest_first():
    """The triage value is in the OLD rows. ORDER BY created_at ASC
    surfaces them first so the sample table starts with the worst
    cases."""
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, status="draft", recipient="new@x.gov",
              subject="new", days_ago=0)
        _seed(c, status="draft", recipient="old@x.gov",
              subject="ancient", days_ago=120)
        _seed(c, status="draft", recipient="middle@x.gov",
              subject="mid", days_ago=10)
        c.commit()
    out = _build()
    # Samples are sorted by created_at ASC → oldest first.
    assert out["samples"][0]["recipient"] == "old@x.gov"
    assert out["samples"][0]["age_days"] is not None
    assert out["samples"][0]["age_days"] >= 100


# ── /health/quoting integration ─────────────────────────────────────────


def test_health_quoting_json_includes_pending_drafts_breakdown(auth_client):
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "pending_drafts_breakdown" in data
    pdb = data["pending_drafts_breakdown"]
    assert pdb["status"] in ("healthy", "warn", "error", "unknown")
    assert "by_status" in pdb
    assert "by_age" in pdb


def test_health_quoting_html_renders_pending_drafts_card(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Pending drafts" in body
