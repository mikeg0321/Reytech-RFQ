"""RFQ-side dedup-at-ingest by Gmail thread_id (hotfix 2026-05-13).

Mike P0: "i just manually sent the quote we sent earlier, and the
queue populated back with ghost data". Two RT-CCHCS-260513-* RFQs
spawned in his Price Checks panel after Mark Sent on RFQ e02b7fa6
(sol 10846357, Mohammad@CDCR). Root cause: buyer reply landed in
the same Gmail thread but the ingest pipeline's RFQ branch had no
dedup gate. PR-N (#959) added dedup for PCs by pc_number; this
hotfix adds the analogous gate for RFQs keyed on
`email_thread_id` because RT-synthesized sol#s are unique per
ingest and won't catch buyer-reply duplicates.

These tests pin the new behavior so a future PR can't drop the
gate without surfacing in the diff.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pytest


def _temp_db_with_rfqs_schema(tmp_path: Path) -> Path:
    """Build a minimal SQLite file matching the columns the dedup
    helpers read from `rfqs` AND `price_checks`. Hermetic — no
    dependency on init_db."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            rfq_number TEXT,
            solicitation_number TEXT,
            agency TEXT,
            institution TEXT,
            status TEXT,
            created_at TEXT,
            sent_at TEXT,
            closed_at TEXT,
            email_thread_id TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY,
            pc_number TEXT,
            agency TEXT,
            institution TEXT,
            status TEXT,
            created_at TEXT,
            sent_at TEXT,
            closed_at TEXT,
            email_thread_id TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def _insert_pc(db_path: Path, **fields):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cols = ", ".join(fields.keys())
    placeholders = ", ".join("?" * len(fields))
    conn.execute(
        f"INSERT INTO price_checks ({cols}) VALUES ({placeholders})",
        tuple(fields.values()),
    )
    conn.commit()
    conn.close()


def _insert_rfq(db_path: Path, **fields):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cols = ", ".join(fields.keys())
    placeholders = ", ".join("?" * len(fields))
    conn.execute(
        f"INSERT INTO rfqs ({cols}) VALUES ({placeholders})",
        tuple(fields.values()),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    """Point `src.core.db.get_db` at a hermetic sqlite file with the
    rfqs schema pre-seeded. Lets the helper run without booting the
    full app DB stack."""
    db_path = _temp_db_with_rfqs_schema(tmp_path)

    class _Conn:
        def __init__(self, path):
            self._raw = sqlite3.connect(str(path))
            self._raw.row_factory = sqlite3.Row

        def __enter__(self):
            return self._raw

        def __exit__(self, *args):
            self._raw.close()

        def execute(self, *args, **kwargs):
            return self._raw.execute(*args, **kwargs)

    def _fake_get_db():
        return _Conn(db_path)

    import src.core.db as _db
    monkeypatch.setattr(_db, "get_db", _fake_get_db, raising=False)
    return db_path


def test_no_thread_id_returns_none(isolated_db):
    """Empty / missing thread_id falls back to no-dedup. Matches the
    PC-side empty-pc_number semantics — legacy poller paths without
    Gmail thread context still ingest cleanly."""
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    assert _find_active_rfq_by_thread("") is None
    assert _find_active_rfq_by_thread("   ") is None
    assert _find_active_rfq_by_thread(None) is None  # type: ignore[arg-type]


def test_match_finds_active_rfq_same_thread_and_agency(isolated_db):
    """The canonical hit: a prior RFQ in the same Gmail thread + same
    agency surfaces as the dedup target."""
    _insert_rfq(
        isolated_db,
        id="rfq_e02b7fa6",
        rfq_number="10846357",
        solicitation_number="10846357",
        agency="cchcs",
        institution="CCHCS — PVSP",
        status="sent",
        created_at=datetime.now().isoformat(),
        email_thread_id="thread_abc123",
    )
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    hit = _find_active_rfq_by_thread("thread_abc123", agency="cchcs")
    assert hit is not None
    assert hit["id"] == "rfq_e02b7fa6"
    assert hit["status"] == "sent"


def test_match_returns_none_when_thread_id_differs(isolated_db):
    """Different thread_id, even with same agency, must not dedup."""
    _insert_rfq(
        isolated_db,
        id="rfq_e02b7fa6",
        agency="cchcs",
        institution="CCHCS",
        status="sent",
        created_at=datetime.now().isoformat(),
        email_thread_id="thread_abc123",
    )
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    assert _find_active_rfq_by_thread("thread_NEW_RFQ", agency="cchcs") is None


def test_skip_existing_statuses_dont_dedup(isolated_db):
    """`duplicate`, `deleted`, `archived` are the skip set — matches
    PC-side semantics. A prior RFQ with one of these statuses must
    NOT be returned (otherwise dedup chains forever)."""
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    for skip_status in ("duplicate", "deleted", "archived"):
        _temp_db = isolated_db  # reuse fixture file
        # Wipe + re-insert with the skip status.
        conn = sqlite3.connect(str(_temp_db))
        conn.execute("DELETE FROM rfqs")
        conn.commit()
        conn.close()
        _insert_rfq(
            _temp_db,
            id=f"rfq_{skip_status}",
            agency="cchcs",
            institution="CCHCS",
            status=skip_status,
            created_at=datetime.now().isoformat(),
            email_thread_id="thread_xyz",
        )
        assert _find_active_rfq_by_thread("thread_xyz", agency="cchcs") is None, skip_status


def test_active_statuses_all_dedup(isolated_db):
    """Sent, completed, won, parsed, ready, priced — all are
    "real" states a buyer-reply must dedup against. The skip set
    is the COMPLEMENT of what dedups, not an explicit allowlist."""
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    for active_status in ("parsed", "ready", "priced", "sent", "completed", "won", "lost"):
        conn = sqlite3.connect(str(isolated_db))
        conn.execute("DELETE FROM rfqs")
        conn.commit()
        conn.close()
        _insert_rfq(
            isolated_db,
            id=f"rfq_{active_status}",
            agency="cchcs",
            institution="CCHCS",
            status=active_status,
            created_at=datetime.now().isoformat(),
            email_thread_id="thread_active",
        )
        hit = _find_active_rfq_by_thread("thread_active", agency="cchcs")
        assert hit is not None, f"status={active_status} should dedup"
        assert hit["status"] == active_status


def test_agency_mismatch_excludes_match(isolated_db):
    """Two different agencies happening to share a Gmail thread is
    rare but must not collapse — the same procurement officer can
    technically be on multiple inboxes. Different agency ≠ match."""
    _insert_rfq(
        isolated_db,
        id="rfq_calvet",
        agency="calvet",
        institution="CalVet Barstow",
        status="sent",
        created_at=datetime.now().isoformat(),
        email_thread_id="thread_shared",
    )
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    assert _find_active_rfq_by_thread("thread_shared", agency="cchcs") is None


def test_pc_side_thread_dedup_finds_active_match(isolated_db):
    """PC-side parity: `_find_active_record_by_thread("pc", ...)`
    catches buyer-reply ghosts on RT-synthesized pc_numbers — exactly
    the live class Mike hit on Mohammad@CDCR 2026-05-13 (two pc rows
    with RT-CCHCS-260513-* sol#s spawned from the e02b7fa6 thread)."""
    from src.core.ingest_pipeline import _find_active_record_by_thread
    _insert_pc(
        isolated_db,
        id="pc_canonical",
        pc_number="10846357",
        agency="cchcs",
        institution="CCHCS",
        status="sent",
        created_at=datetime.now().isoformat(),
        email_thread_id="thread_mohammad",
    )
    hit = _find_active_record_by_thread("pc", "thread_mohammad", agency="cchcs")
    assert hit is not None
    assert hit["id"] == "pc_canonical"
    assert hit["status"] == "sent"


def test_record_type_must_be_pc_or_rfq(isolated_db):
    """Defensive: the consolidated helper accepts only "pc" or "rfq"
    as record_type. Any other value (typo, future record type that
    doesn't share the schema) returns None safely."""
    from src.core.ingest_pipeline import _find_active_record_by_thread
    assert _find_active_record_by_thread("quote", "thread_x") is None
    assert _find_active_record_by_thread("", "thread_x") is None
    assert _find_active_record_by_thread("orders", "thread_x") is None


def test_empty_agency_on_either_side_still_matches(isolated_db):
    """Legacy rows missing the agency column (pre-Gmail-ingest) must
    still dedup so they can be cleaned up. Matches PC-side semantics
    where agency-empty rows are forgiven."""
    _insert_rfq(
        isolated_db,
        id="rfq_legacy",
        agency="",  # missing
        institution="CCHCS",
        status="sent",
        created_at=datetime.now().isoformat(),
        email_thread_id="thread_legacy",
    )
    from src.core.ingest_pipeline import _find_active_rfq_by_thread
    hit = _find_active_rfq_by_thread("thread_legacy", agency="cchcs")
    assert hit is not None
    # Reverse: caller has empty agency, prior has set agency — also matches.
    conn = sqlite3.connect(str(isolated_db))
    conn.execute("DELETE FROM rfqs")
    conn.commit()
    conn.close()
    _insert_rfq(
        isolated_db,
        id="rfq_full",
        agency="cchcs",
        institution="CCHCS",
        status="sent",
        created_at=datetime.now().isoformat(),
        email_thread_id="thread_full",
    )
    assert _find_active_rfq_by_thread("thread_full", agency="") is not None
