"""Phase 3.3 (2026-05-05): SQLite-side companion to the JSON cs_drafts
purge.

Background: prod 2026-05-04's gmail_send card surfaced 268+ pending
drafts. Investigation revealed two parallel data stores —
`email_outbox.json` (swept by `purge_stale_cs_drafts`) and the SQLite
`email_outbox` table (untouched by any sweep). The 268 drafts lived in
the SQLite half, so the existing JSON purge never saw them.

`purge_stale_email_outbox(max_age_days=30)` issues a single DELETE
against the SQLite table for the four "untouched draft" statuses:
cs_draft / draft / outreach_draft / follow_up_draft. `queued` and
`approved` are deliberately preserved — those represent operator
intent to send, and stale rows there are a delivery problem, not a
triage one.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest


def _seed(conn, *, row_id, status, days_ago, type_="quote", subject="t"):
    when = (datetime.now() - timedelta(days=days_ago)).isoformat()
    conn.execute("""
        INSERT INTO email_outbox
          (id, created_at, status, type, to_address, subject, body)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (row_id, when, status, type_, "buyer@a.gov", subject, ""))


def _wipe(conn):
    conn.execute("DELETE FROM email_outbox")
    conn.commit()


# ── Stale untouched-draft rows get purged ──────────────────────────


def test_purges_stale_cs_draft():
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, row_id="old-1", status="cs_draft", days_ago=45)
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 1
    assert result["kept"] == 0
    with get_db() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM email_outbox").fetchone()["n"]
    assert n == 0


def test_purges_all_four_untouched_draft_statuses():
    """cs_draft / draft / outreach_draft / follow_up_draft all qualify."""
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, row_id="cs-1", status="cs_draft", days_ago=60)
        _seed(c, row_id="dr-1", status="draft", days_ago=60)
        _seed(c, row_id="or-1", status="outreach_draft", days_ago=60)
        _seed(c, row_id="fu-1", status="follow_up_draft", days_ago=60)
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 4
    assert result["kept"] == 0


def test_handles_268_drafts_at_scale():
    """Reproduce the 2026-05-04 prod scenario: 268 stale + 12 fresh."""
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        for i in range(268):
            _seed(c, row_id=f"old-{i}", status="cs_draft", days_ago=60)
        for i in range(12):
            _seed(c, row_id=f"fresh-{i}", status="cs_draft", days_ago=2)
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 268
    assert result["kept"] == 12
    with get_db() as c:
        rows = c.execute(
            "SELECT id FROM email_outbox ORDER BY id"
        ).fetchall()
    assert len(rows) == 12
    assert all(r["id"].startswith("fresh-") for r in rows)


# ── Fresh rows preserved ───────────────────────────────────────────


def test_preserves_fresh_drafts():
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, row_id="fresh-1", status="cs_draft", days_ago=10)
        _seed(c, row_id="fresh-2", status="draft", days_ago=29)
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 0
    assert result["kept"] == 2


# ── queued / approved are NEVER purged (operator intent preserved) ─


def test_preserves_queued_and_approved_even_when_old():
    """A `queued` or `approved` draft that's been sitting for 90 days is
    a delivery problem (something prevented the actual send), not a
    triage problem. Don't silently delete operator-intended rows."""
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, row_id="q-1", status="queued", days_ago=90)
        _seed(c, row_id="a-1", status="approved", days_ago=90)
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 0
    # `kept` only counts the four target statuses, so queued/approved
    # don't show up in `kept` — but they MUST still be in the table.
    with get_db() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM email_outbox").fetchone()["n"]
    assert n == 2


# ── sent / failed never targeted ───────────────────────────────────


def test_preserves_sent_and_failed_rows():
    """`sent` / `failed` / `permanently_failed` are operator-actioned
    history. The DELETE WHERE status IN (...) clause excludes them."""
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, row_id="sent-1", status="sent", days_ago=200)
        _seed(c, row_id="fail-1", status="failed", days_ago=200)
        _seed(c, row_id="perm-1", status="permanently_failed", days_ago=200)
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 0


# ── Idempotence ────────────────────────────────────────────────────


def test_idempotent_reruns():
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        _seed(c, row_id="old-1", status="cs_draft", days_ago=60)
        c.commit()
    first = purge_stale_email_outbox(max_age_days=30)
    assert first["purged"] == 1
    second = purge_stale_email_outbox(max_age_days=30)
    assert second["purged"] == 0
    assert second["kept"] == 0


# ── Edge cases ─────────────────────────────────────────────────────


def test_empty_table_returns_zero():
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
    result = purge_stale_email_outbox(max_age_days=30)
    assert result == {"purged": 0, "kept": 0, "errors": 0}


def test_null_created_at_is_skipped():
    """Defensive: a row with NULL/empty created_at can't be aged
    confidently. Skip it (the WHERE clause filters on `created_at IS
    NOT NULL AND created_at != ''`)."""
    from src.agents.cs_agent import purge_stale_email_outbox
    from src.core.db import get_db
    with get_db() as c:
        _wipe(c)
        c.execute("""
            INSERT INTO email_outbox (id, created_at, status, type, to_address, subject)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("weird-1", "", "cs_draft", "quote", "x@a.gov", "weird"))
        c.commit()
    result = purge_stale_email_outbox(max_age_days=30)
    assert result["purged"] == 0
    # Row preserved despite missing timestamp
    with get_db() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM email_outbox").fetchone()["n"]
    assert n == 1


# ── Boot wiring ────────────────────────────────────────────────────


def test_purge_is_wired_into_deferred_init():
    """Pin the import + call site in app.py so the SQL purge actually
    runs on every deploy alongside the JSON purge."""
    body = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    assert "from src.agents.cs_agent import purge_stale_email_outbox" in body
    assert "purge_stale_email_outbox(" in body
