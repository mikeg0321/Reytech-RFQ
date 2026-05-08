"""Pin S-14 — quotes ON CONFLICT must update or preserve lifecycle fields.

Audit 2026-05-07 v2 §S-14: pre-fix the UPDATE SET clause silently
omitted 11 fields (sent_at, source, source_pc_id, source_rfq_id,
expires_at, closed_by_agent, close_reason, revision_count,
win_probability, last_follow_up, follow_up_count). Re-imports +
retry-saves on existing quote rows could not propagate any of these.
The audit explicitly noted is_test/created_at omission is intentional
and stays untouched.

These tests pin:
  1. New row: lifecycle fields land in their columns on INSERT.
  2. Update with non-empty value applies (closed_by_agent, win_probability).
  3. Update with empty/missing sent_at preserves existing sent_at
     (COALESCE strategy — partial-write retries don't clear timestamps).
  4. Update propagates revision_count + follow_up_count (counters).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _seed_quote_db(monkeypatch, tmp_path):
    from src.core import db as core_db
    db_path = tmp_path / "test.db"
    monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
    core_db.init_db()
    return core_db


def _valid_quote(qn, **overrides):
    """Quote that satisfies the contract: R\\d{2}Q\\d+ format, items, total, institution."""
    base = {
        "quote_number": qn,
        "agency": "CDCR",
        "institution": "CCHCS",
        "status": "sent",
        "total": 100.0,
        "items_count": 1,
        "items_detail": [{"description": "test", "qty": 1, "price_per_unit": 100}],
        "source_rfq_id": "rfq-test",
    }
    base.update(overrides)
    return base


class TestQuoteLifecycleFieldsInsert:
    def test_new_row_persists_lifecycle_fields(self, tmp_path, monkeypatch):
        from src.core.db import upsert_quote, get_db
        _seed_quote_db(monkeypatch, tmp_path)

        ok = upsert_quote(_valid_quote("R26Q90001",
            sent_at="2026-05-08T10:00:00",
            source="rfq",
            source_rfq_id="rfq-abc",
            expires_at="2026-06-08",
            win_probability=0.65,
            revision_count=1,
            follow_up_count=0,
        ))
        assert ok is True

        with get_db() as conn:
            row = conn.execute("""
                SELECT sent_at, source, source_rfq_id, expires_at,
                       win_probability, revision_count, follow_up_count
                FROM quotes WHERE quote_number=?
            """, ("R26Q90001",)).fetchone()
            assert row[0] == "2026-05-08T10:00:00"
            assert row[1] == "rfq"
            assert row[2] == "rfq-abc"
            assert row[3] == "2026-06-08"
            assert row[4] == 0.65
            assert row[5] == 1
            assert row[6] == 0


class TestQuoteLifecycleFieldsUpdate:
    def test_status_close_fields_update_on_conflict(self, tmp_path, monkeypatch):
        from src.core.db import upsert_quote, get_db
        _seed_quote_db(monkeypatch, tmp_path)

        upsert_quote(_valid_quote("R26Q90002"))
        # Status flip — close fields should propagate.
        upsert_quote(_valid_quote("R26Q90002",
            status="lost",
            closed_by_agent="AI_NIGHT_AGENT",
            close_reason="buyer awarded competitor",
            win_probability=0.0,
        ))
        with get_db() as conn:
            row = conn.execute("""
                SELECT closed_by_agent, close_reason, win_probability
                FROM quotes WHERE quote_number=?
            """, ("R26Q90002",)).fetchone()
            assert row is not None, "row missing — contract may have blocked"
            assert row[0] == "AI_NIGHT_AGENT", \
                "S-14 regression: closed_by_agent dropped on UPDATE"
            assert row[1] == "buyer awarded competitor"
            assert row[2] == 0.0

    def test_empty_sent_at_does_not_clobber_existing(self, tmp_path, monkeypatch):
        """Partial-write retry path: re-save without sent_at must not
        clear the existing send timestamp. COALESCE strategy."""
        from src.core.db import upsert_quote, get_db
        _seed_quote_db(monkeypatch, tmp_path)

        upsert_quote(_valid_quote("R26Q90003",
            sent_at="2026-05-08T11:00:00",
        ))
        # Subsequent save without sent_at — must preserve.
        upsert_quote(_valid_quote("R26Q90003",
            notes="follow-up scheduled",
            # sent_at missing — should NOT clobber to ""
        ))
        with get_db() as conn:
            row = conn.execute(
                "SELECT sent_at, notes FROM quotes WHERE quote_number=?",
                ("R26Q90003",)
            ).fetchone()
            assert row is not None
            assert row[0] == "2026-05-08T11:00:00", \
                f"S-14 regression: sent_at clobbered to {row[0]!r}"
            assert row[1] == "follow-up scheduled"

    def test_revision_and_follow_up_count_propagate(self, tmp_path, monkeypatch):
        from src.core.db import upsert_quote, get_db
        _seed_quote_db(monkeypatch, tmp_path)

        upsert_quote(_valid_quote("R26Q90004", revision_count=0, follow_up_count=0))
        upsert_quote(_valid_quote("R26Q90004", revision_count=2, follow_up_count=3))
        with get_db() as conn:
            row = conn.execute(
                "SELECT revision_count, follow_up_count FROM quotes "
                "WHERE quote_number=?",
                ("R26Q90004",)
            ).fetchone()
            assert row is not None
            assert row[0] == 2
            assert row[1] == 3

    def test_empty_source_does_not_clobber_existing_lineage(self, tmp_path, monkeypatch):
        from src.core.db import upsert_quote, get_db
        _seed_quote_db(monkeypatch, tmp_path)

        upsert_quote(_valid_quote("R26Q90005",
            source="rfq",
            source_rfq_id="rfq-xyz",
            source_pc_id="pc-abc",
        ))
        upsert_quote(_valid_quote("R26Q90005",
            status="won",
            source="",  # empty — must NOT clobber
            source_rfq_id="",
            source_pc_id="",
        ))
        with get_db() as conn:
            row = conn.execute(
                "SELECT source, source_rfq_id, source_pc_id FROM quotes "
                "WHERE quote_number=?",
                ("R26Q90005",)
            ).fetchone()
            assert row is not None
            assert row[0] == "rfq", \
                "S-14 regression: source clobbered on partial update"
            assert row[1] == "rfq-xyz"
            assert row[2] == "pc-abc"
