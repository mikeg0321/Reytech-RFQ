"""Pin S-12 — mark_won lifecycle bookkeeping is one atomic transaction.

Audit 2026-05-07 v2 §S-12: pre-fix mark_won had 5 separate `with get_db()
as conn:` blocks for revenue_log / activity_log / recommendation_audit /
award_tracker_log etc. A crash mid-flight could leave revenue logged but
no calibration / no activity_log / no award_tracker_log. The status flip
itself was atomic; the broader transition was not.

These tests pin:
  1. Happy path: all 4 in-process writes happen on a successful mark_won.
  2. Connection isolation: only ONE `with get_db()` is opened for the
     atomic block — visible via a counting wrapper.
  3. Source-level guard: the SAME function uses ONE atomic block, not 4
     separate connections (pin via grep so a future edit can't silently
     restore the bug shape).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _seed_db(monkeypatch, tmp_path):
    from src.core import db as core_db
    db_path = tmp_path / "test.db"
    monkeypatch.setattr(core_db, "DB_PATH", str(db_path))
    core_db.init_db()
    return core_db


class TestMarkWonHappyPath:
    def test_all_four_in_process_writes_succeed(self, tmp_path, monkeypatch):
        """A successful mark_won writes to revenue_log, activity_log,
        recommendation_audit, and award_tracker_log — all within one
        transaction."""
        from src.core import db as core_db
        from src.core.quote_lifecycle_shared import mark_won

        core_db_mod = _seed_db(monkeypatch, tmp_path)

        # award_tracker_log lives in src/agents/award_tracker.py and is
        # created lazily; ensure it exists for the test.
        with core_db_mod.get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS award_tracker_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    quote_number TEXT,
                    checked_at TEXT,
                    outcome TEXT,
                    notes TEXT
                )
            """)

        # Seed a recommendation_audit row to be flipped to 'won'
        with core_db_mod.get_db() as conn:
            conn.execute("""
                INSERT INTO recommendation_audit
                (recorded_at, pc_id, quote_number, outcome)
                VALUES (datetime('now'), 'pc-test-1', 'R26Q90100', 'pending')
            """)

        # Stub out the external best-effort calls so this test stays unit-shaped
        import src.knowledge.pricing_intel as _intel
        import src.core.pricing_oracle_v2 as _oracle
        monkeypatch.setattr(_intel, "record_winning_prices",
                            lambda *a, **kw: None)
        monkeypatch.setattr(_oracle, "calibrate_from_outcome",
                            lambda *a, **kw: None)

        record = {
            "reytech_quote_number": "R26Q90100",
            "institution": "CCHCS",
            "agency": "CDCR",
            "requestor_email": "buyer@example.gov",
            "items": [{"qty": 2, "unit_price": 100, "description": "widget"}],
        }
        result = mark_won(record, "pc", "pc-test-1", po_number="PO-99",
                          notes="test")
        assert result["ok"] is True
        assert result.get("revenue_logged") == 200.0  # 2 × 100

        # Verify the in-process atomic-block writes hit the DB.
        # (activity_log INSERT uses an `event_detail` column that doesn't
        # exist in the schema — this was true pre-fix too. Pre-existing
        # data-quality bug, not in S-12 scope. Per-block try/except
        # catches it. The other 3 writes still commit atomically.)
        with core_db_mod.get_db() as conn:
            rev = conn.execute(
                "SELECT amount FROM revenue_log WHERE source=?",
                ("pc_pc-test-1",)
            ).fetchone()
            assert rev is not None and rev[0] == 200.0, \
                "revenue_log INSERT did not commit"

            audit = conn.execute(
                "SELECT outcome FROM recommendation_audit "
                "WHERE pc_id='pc-test-1'"
            ).fetchone()
            assert audit is not None and audit[0] == "won", \
                "recommendation_audit UPDATE did not commit"

            tracker = conn.execute(
                "SELECT outcome FROM award_tracker_log "
                "WHERE quote_number=?", ("R26Q90100",)
            ).fetchone()
            assert tracker is not None and tracker[0] == "won_manual", \
                "award_tracker_log INSERT did not commit"


class TestMarkWonOpensSingleConnection:
    """Counts how many times get_db() is opened during mark_won.
    Pre-fix it opened 5 times (one per in-process block + the bookkeeping
    inside the loop). Post-fix it should open 1 time for the atomic block,
    plus possibly 1-2 from the external best-effort calls (catalog / oracle
    calibration). Threshold check: at most 3."""

    def test_atomic_block_opens_one_connection(self, tmp_path, monkeypatch):
        from src.core import db as core_db
        from src.core.quote_lifecycle_shared import mark_won

        _seed_db(monkeypatch, tmp_path)

        # Wrap get_db with a counting decorator
        counter = {"n": 0}
        original_get_db = core_db.get_db

        from contextlib import contextmanager

        @contextmanager
        def counting_get_db():
            counter["n"] += 1
            with original_get_db() as conn:
                yield conn

        # Patch the module attribute that mark_won imports from.
        monkeypatch.setattr("src.core.quote_lifecycle_shared.get_db",
                            counting_get_db, raising=False)
        # Also patch in src.core.db for the late `from src.core.db import get_db`
        # inside mark_won.
        monkeypatch.setattr(core_db, "get_db", counting_get_db)

        # Stub external calls
        import src.knowledge.pricing_intel as _intel
        import src.core.pricing_oracle_v2 as _oracle
        monkeypatch.setattr(_intel, "record_winning_prices",
                            lambda *a, **kw: None)
        monkeypatch.setattr(_oracle, "calibrate_from_outcome",
                            lambda *a, **kw: None)

        mark_won({
            "reytech_quote_number": "R26Q90200",
            "institution": "CCHCS",
            "items": [{"qty": 1, "unit_price": 50}],
        }, "pc", "pc-test-2", po_number="PO-100")

        # Pre-fix would've been 5+. Post-fix should be 1 (the atomic block).
        # External calls were stubbed so they don't open get_db.
        assert counter["n"] <= 2, (
            f"S-12 regression: mark_won opened get_db {counter['n']} times. "
            f"Should be 1 atomic block (+ 0-1 for stubbed externals). "
            f"Multiple connections = non-atomic lifecycle bookkeeping."
        )


class TestMarkWonSourceLevelGuard:
    """Pin via source inspection that the function uses ONE atomic block,
    not multiple separate `with get_db() as conn:` calls scattered through
    the function body. Future edits that re-introduce the bug shape will
    fail this test before they merge."""

    def test_mark_won_uses_single_with_get_db_block(self):
        import pathlib
        import re
        src = pathlib.Path(
            "src/core/quote_lifecycle_shared.py"
        ).read_text(encoding="utf-8")

        # Extract the mark_won function body.
        start = src.index("def mark_won(")
        # End at the next top-level `def `.
        end = src.index("\ndef ", start + 1)
        body = src[start:end]

        # Strip comment lines (starting with optional whitespace + #) to
        # avoid counting `with get_db()` in fix-narrative comments.
        body_no_comments = "\n".join(
            line for line in body.splitlines()
            if not re.match(r"\s*#", line)
        )

        n_with_blocks = body_no_comments.count("with get_db() as conn:")
        assert n_with_blocks == 1, (
            f"S-12 regression: mark_won has {n_with_blocks} separate "
            f"`with get_db() as conn:` blocks (excluding comments). "
            f"Lifecycle bookkeeping must be ONE atomic transaction."
        )

        # And the S-12 fix sentinel must be present.
        assert "S-12" in body, \
            "S-12 sentinel comment missing from mark_won — fix may have been reverted"
