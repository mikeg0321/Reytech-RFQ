"""Phase 0.4 race-fence regression tests for quote status writes.

Background agents (revenue_engine, scprs_intelligence_engine,
scprs_universal_pull) MUST NOT overwrite an operator's manual win/loss
mark. Each UPDATE statement gains a status guard in the WHERE clause.

Also tests the new set_quote_status_atomic() helper which provides the
same race-protected UPDATE for future writers.
"""

from src.core.db import get_db
from src.core.quote_lifecycle_shared import set_quote_status_atomic


def _seed(qnum, status, **extra):
    from datetime import datetime
    with get_db() as conn:
        cols = ["quote_number", "status", "created_at"]
        vals = [qnum, status, datetime.now().isoformat()]
        for k, v in extra.items():
            cols.append(k)
            vals.append(v)
        placeholders = ",".join("?" * len(vals))
        conn.execute(
            f"INSERT INTO quotes ({','.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        conn.commit()


def _status(qnum):
    with get_db() as conn:
        row = conn.execute(
            "SELECT status FROM quotes WHERE quote_number=?", (qnum,)
        ).fetchone()
    return row["status"] if row else None


class TestSetQuoteStatusAtomic:
    def test_unconditional_update_works(self):
        _seed("R26Q-RACE-1", "sent")
        ok = set_quote_status_atomic("R26Q-RACE-1", "won", source="operator_test")
        assert ok is True
        assert _status("R26Q-RACE-1") == "won"

    def test_expected_prev_match_updates(self):
        _seed("R26Q-RACE-2", "sent")
        ok = set_quote_status_atomic(
            "R26Q-RACE-2", "won", expected_prev="sent", source="award_tracker"
        )
        assert ok is True
        assert _status("R26Q-RACE-2") == "won"

    def test_expected_prev_mismatch_does_not_update(self):
        # Operator already marked 'won'. Background tracker tries to
        # flip from 'sent' → 'lost'. Must be refused.
        _seed("R26Q-RACE-3", "won")
        ok = set_quote_status_atomic(
            "R26Q-RACE-3", "lost",
            expected_prev="sent", source="award_tracker"
        )
        assert ok is False
        assert _status("R26Q-RACE-3") == "won"

    def test_missing_quote_returns_false(self):
        ok = set_quote_status_atomic("R26Q-DOES-NOT-EXIST", "won")
        assert ok is False

    def test_extra_columns_set(self):
        _seed("R26Q-RACE-4", "sent")
        ok = set_quote_status_atomic(
            "R26Q-RACE-4", "won",
            extra_columns={"po_number": "PO-12345"},
            source="email_poller",
        )
        assert ok is True
        with get_db() as conn:
            row = conn.execute(
                "SELECT status, po_number FROM quotes WHERE quote_number=?",
                ("R26Q-RACE-4",)
            ).fetchone()
        assert row["status"] == "won"
        assert row["po_number"] == "PO-12345"


class TestSqlSiteGuards:
    """Static checks: each previously-unsafe site now has a status guard."""

    def _read(self, path):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    def test_revenue_engine_has_status_guard(self):
        body = self._read("src/agents/revenue_engine.py")
        # The UPDATE block must include "status NOT IN" guard
        assert "status NOT IN ('won', 'lost', 'cancelled')" in body, (
            "revenue_engine.py UPDATE quotes SET status='won' must include "
            "a status guard so a concurrent operator manual mark is not "
            "overwritten."
        )

    def test_scprs_intelligence_engine_has_sent_guard(self):
        body = self._read("src/agents/scprs_intelligence_engine.py")
        # The auto-close-lost UPDATE must guard on status='sent'
        assert "WHERE id=? AND status='sent'" in body, (
            "scprs_intelligence_engine.py auto-close UPDATE must guard "
            "on status='sent' so it can't overwrite a manual mark."
        )

    def test_scprs_universal_pull_has_sent_guard(self):
        body = self._read("src/agents/scprs_universal_pull.py")
        assert "WHERE id=? AND status='sent'" in body, (
            "scprs_universal_pull.py auto-close UPDATE must guard on "
            "status='sent' so it can't overwrite a manual mark."
        )
