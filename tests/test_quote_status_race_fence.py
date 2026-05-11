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

    def test_scprs_intelligence_engine_uses_atomic_helper(self):
        # PR-η Phase 4 (2026-05-11): migrated from raw `WHERE id=? AND
        # status='sent'` to set_quote_status_atomic with
        # expected_prev='sent'. The atomic helper's expected_prev IS the
        # canonical race-fence — its single-statement UPDATE with that
        # guard is equivalent to the old raw SQL.
        body = self._read("src/agents/scprs_intelligence_engine.py")
        assert "set_quote_status_atomic(" in body, (
            "scprs_intelligence_engine.py must route status flips "
            "through set_quote_status_atomic (PR-η Phase 4)."
        )
        assert 'expected_prev="sent"' in body, (
            "scprs_intelligence_engine.py status flip must pass "
            "expected_prev='sent' to preserve the race-fence."
        )

    def test_scprs_universal_pull_uses_atomic_helper(self):
        body = self._read("src/agents/scprs_universal_pull.py")
        assert "set_quote_status_atomic(" in body, (
            "scprs_universal_pull.py must route status flips through "
            "set_quote_status_atomic (PR-η Phase 4)."
        )
        assert 'expected_prev="sent"' in body, (
            "scprs_universal_pull.py status flip must pass "
            "expected_prev='sent' to preserve the race-fence."
        )

    def test_email_poller_uses_atomic_helper(self):
        # PR-η Phase 4 (2026-05-11): PO-via-email won detection.
        body = self._read("src/agents/email_poller.py")
        assert "set_quote_status_atomic(" in body, (
            "email_poller.py PO-via-email won path must route through "
            "set_quote_status_atomic (PR-η Phase 4)."
        )
        # email_poller has multiple callers; the won-via-email one must
        # use expected_prev='sent'.
        assert 'expected_prev="sent"' in body


class TestForbiddenPrev:
    """PR-η Phase 2 (2026-05-11): the dashboard.py operator paths flip
    quote→won when an order is created, but must NOT clobber 'cancelled'
    or re-fire if status is already 'won' (idempotency). The forbidden_prev
    parameter encodes that semantic — equivalent to the old raw SQL's
    `AND status NOT IN ('won', 'cancelled')` guard."""

    def test_forbidden_prev_blocks_when_already_won(self):
        _seed("R26Q-FORBID-1", "won")
        ok = set_quote_status_atomic(
            "R26Q-FORBID-1", "won",
            forbidden_prev=["won", "cancelled"],
            source="dashboard_order_created",
        )
        assert ok is False
        assert _status("R26Q-FORBID-1") == "won"

    def test_forbidden_prev_blocks_when_cancelled(self):
        _seed("R26Q-FORBID-2", "cancelled")
        ok = set_quote_status_atomic(
            "R26Q-FORBID-2", "won",
            forbidden_prev=["won", "cancelled"],
            source="dashboard_order_created",
        )
        assert ok is False
        assert _status("R26Q-FORBID-2") == "cancelled"

    def test_forbidden_prev_allows_normal_flip(self):
        _seed("R26Q-FORBID-3", "sent")
        ok = set_quote_status_atomic(
            "R26Q-FORBID-3", "won",
            forbidden_prev=["won", "cancelled"],
            source="dashboard_order_created",
        )
        assert ok is True
        assert _status("R26Q-FORBID-3") == "won"

    def test_forbidden_prev_and_expected_prev_are_mutex(self):
        import pytest
        with pytest.raises(ValueError, match="mutually exclusive"):
            set_quote_status_atomic(
                "R26Q-FORBID-4", "won",
                expected_prev="sent",
                forbidden_prev=["cancelled"],
            )

    def test_forbidden_prev_with_extra_columns(self):
        _seed("R26Q-FORBID-5", "sent")
        ok = set_quote_status_atomic(
            "R26Q-FORBID-5", "won",
            forbidden_prev=["won", "cancelled"],
            extra_columns={"po_number": "PO-12345"},
            source="dashboard_order_from_po_email",
        )
        assert ok is True
        with get_db() as conn:
            row = conn.execute(
                "SELECT status, po_number FROM quotes WHERE quote_number=?",
                ("R26Q-FORBID-5",),
            ).fetchone()
        assert row["status"] == "won"
        assert row["po_number"] == "PO-12345"
