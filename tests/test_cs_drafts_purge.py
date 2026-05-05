"""Mike P0 2026-05-05 cont.: stale cs_drafts purge boot job.

Background: prod 2026-05-04 surfaced 132 cs_drafts >30d old in
email_outbox.json. Auto-generated update-request replies that Mike
never triaged piled up because the digest email was disabled (PR #604)
and there was no purge sweep. Original buyer emails are long past the
responding window — these drafts are noise, not work.

`purge_stale_cs_drafts(max_age_days=30)` removes outbox entries where:
  - status == "cs_draft" (not yet sent/approved/dismissed)
  - created_at older than max_age_days

Sent / approved / dismissed drafts are preserved. Idempotent — re-runs
are safe because the WHERE clause filters by age. Boot wiring in
app.py _deferred_init() runs it on every deploy.
"""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytest


@pytest.fixture
def tmp_outbox(tmp_path, monkeypatch):
    outbox_path = tmp_path / "email_outbox.json"
    monkeypatch.setattr("src.agents.cs_agent.OUTBOX_FILE", str(outbox_path))
    monkeypatch.setattr("src.agents.cs_agent.DATA_DIR", str(tmp_path))
    return outbox_path


def _write(path: Path, rows: list):
    path.write_text(json.dumps(rows, indent=2, default=str))


def _read(path: Path) -> list:
    return json.loads(path.read_text())


# ── Stale cs_drafts get purged ─────────────────────────────────────


def test_purge_removes_stale_cs_drafts(tmp_outbox):
    """45-day-old cs_draft must be purged."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    old_ts = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    _write(tmp_outbox, [
        {"id": "old-1", "status": "cs_draft", "type": "cs_response",
         "created_at": old_ts, "to": "buyer@a.gov", "subject": "old draft"},
    ])
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result["purged"] == 1
    assert result["kept"] == 0
    assert _read(tmp_outbox) == []


def test_purge_handles_132_drafts_at_scale(tmp_outbox):
    """Reproduce the 2026-05-04 prod scenario: 132 stale + 5 fresh."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    old_ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    fresh_ts = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    rows = []
    for i in range(132):
        rows.append({"id": f"old-{i}", "status": "cs_draft",
                     "type": "cs_response", "created_at": old_ts,
                     "to": f"b{i}@a.gov", "subject": f"sub {i}"})
    for i in range(5):
        rows.append({"id": f"fresh-{i}", "status": "cs_draft",
                     "type": "cs_response", "created_at": fresh_ts,
                     "to": f"f{i}@a.gov", "subject": f"sub {i}"})
    _write(tmp_outbox, rows)
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result["purged"] == 132
    assert result["kept"] == 5
    remaining = _read(tmp_outbox)
    assert len(remaining) == 5
    assert all(r["id"].startswith("fresh-") for r in remaining)


# ── Fresh cs_drafts are preserved ──────────────────────────────────


def test_purge_preserves_fresh_cs_drafts(tmp_outbox):
    """Drafts <30d old must NOT be purged."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    fresh_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    _write(tmp_outbox, [
        {"id": "fresh-1", "status": "cs_draft", "type": "cs_response",
         "created_at": fresh_ts, "to": "x@a.gov", "subject": "recent"},
    ])
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result["purged"] == 0
    assert result["kept"] == 1


# ── Sent/approved/dismissed drafts are preserved even if old ────────


def test_purge_preserves_sent_drafts(tmp_outbox):
    """Old drafts whose status moved off cs_draft (sent/approved/etc.)
    are operator-actioned history — never purge them."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    old_ts = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    _write(tmp_outbox, [
        # type==cs_response but status==sent — operator approved
        {"id": "sent-1", "status": "sent", "type": "cs_response",
         "created_at": old_ts, "to": "x@a.gov", "subject": "sent reply"},
        # type==cs_response but status==dismissed — operator rejected
        {"id": "dis-1", "status": "dismissed", "type": "cs_response",
         "created_at": old_ts, "to": "y@a.gov", "subject": "dismissed"},
        # type==cs_response but status==approved
        {"id": "app-1", "status": "approved", "type": "cs_response",
         "created_at": old_ts, "to": "z@a.gov", "subject": "approved"},
    ])
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result["purged"] == 0
    assert result["kept"] == 3


# ── Non-cs_draft outbox entries untouched ──────────────────────────


def test_purge_ignores_non_cs_draft_entries(tmp_outbox):
    """Quote-send drafts, regular emails — anything that's NOT a
    cs_draft — must be untouched even if old."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    old_ts = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
    _write(tmp_outbox, [
        {"id": "q-1", "status": "draft", "type": "quote_send",
         "created_at": old_ts, "to": "buyer@a.gov", "subject": "quote"},
        {"id": "raw-1", "type": "outreach",
         "created_at": old_ts, "to": "lead@b.com", "subject": "intro"},
    ])
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result["purged"] == 0
    assert result["kept"] == 2


# ── Idempotence ────────────────────────────────────────────────────


def test_purge_is_idempotent(tmp_outbox):
    """Re-runs find nothing because already-purged rows are gone."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    old_ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    _write(tmp_outbox, [
        {"id": "old-1", "status": "cs_draft", "type": "cs_response",
         "created_at": old_ts, "to": "x@a.gov", "subject": "old"},
    ])
    first = purge_stale_cs_drafts(max_age_days=30)
    assert first["purged"] == 1
    second = purge_stale_cs_drafts(max_age_days=30)
    assert second["purged"] == 0
    assert second["kept"] == 0


# ── Edge cases ─────────────────────────────────────────────────────


def test_purge_on_missing_outbox_returns_zero(tmp_outbox):
    """Outbox file doesn't exist yet — return zero counts, no errors."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    if tmp_outbox.exists():
        tmp_outbox.unlink()
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result == {"purged": 0, "kept": 0, "errors": 0}


def test_purge_on_corrupt_json_returns_zero(tmp_outbox):
    """Corrupt outbox JSON — fail safe, don't wipe the file."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    tmp_outbox.write_text("{corrupt json}")
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result == {"purged": 0, "kept": 0, "errors": 0}
    # File contents preserved (don't blow away corrupt data — may have
    # something Mike can recover from manually)
    assert tmp_outbox.read_text() == "{corrupt json}"


def test_purge_handles_unparseable_timestamp_conservatively(tmp_outbox):
    """If created_at can't be parsed, KEEP the row — better to leave
    a stale draft than to delete an unknown one."""
    from src.agents.cs_agent import purge_stale_cs_drafts
    _write(tmp_outbox, [
        {"id": "weird-1", "status": "cs_draft", "type": "cs_response",
         "created_at": "not-a-date", "to": "x@a.gov", "subject": "weird"},
    ])
    result = purge_stale_cs_drafts(max_age_days=30)
    assert result["purged"] == 0
    assert result["kept"] == 1
    assert result["errors"] == 1


# ── Boot wiring ────────────────────────────────────────────────────


def test_purge_is_wired_into_deferred_init():
    """Pin the import + call site in app.py so the purge actually runs
    on every deploy."""
    body = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    assert "from src.agents.cs_agent import purge_stale_cs_drafts" in body
    assert "purge_stale_cs_drafts(" in body
