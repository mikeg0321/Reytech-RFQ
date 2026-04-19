"""Tests for audit-log persistence visibility.

Previously a broken DB caused every audit row to disappear silently — the
exception was swallowed at log.debug level and the operator saw an empty
dashboard with no idea why. Now failures surface to result.warnings AND
log at error level so prod logs show the regression.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    OrchestratorResult,
    StageAttempt,
)


def _attempt() -> StageAttempt:
    return StageAttempt(
        stage_from="parsed",
        stage_to="priced",
        outcome="advanced",
        at=datetime.now().isoformat(),
    )


def _quote() -> Quote:
    q = Quote(doc_type=DocType.PC, line_items=[
        LineItem(line_no=1, description="X", qty=1, unit_cost=Decimal("1.00")),
    ])
    q.header.solicitation_number = "R26Q0042"
    return q


class TestAuditPersistVisibility:
    def test_db_failure_appends_warning_to_result(self):
        """When the DB write fails, the operator must see a warning, not silence."""
        orch = QuoteOrchestrator(persist_audit=True)
        result = OrchestratorResult(quote=_quote())

        # Make get_db raise on use to simulate a busted DB connection.
        broken_db = MagicMock()
        broken_db.__enter__ = MagicMock(side_effect=RuntimeError("disk I/O error"))

        with patch("src.core.db.get_db", return_value=broken_db):
            orch._persist_audit(_quote(), _attempt(), actor="operator", result=result)

        assert any(
            "audit row dropped" in w and "parsed" in w and "priced" in w
            for w in result.warnings
        ), result.warnings

    def test_db_failure_logs_at_error_level(self):
        """Prod logs must show the regression so an oncall can grep it."""
        orch = QuoteOrchestrator(persist_audit=True)

        broken_db = MagicMock()
        broken_db.__enter__ = MagicMock(side_effect=RuntimeError("disk full"))

        with patch("src.core.db.get_db", return_value=broken_db):
            with patch("src.core.quote_orchestrator.log") as mock_log:
                orch._persist_audit(_quote(), _attempt(), actor="operator", result=None)
                mock_log.error.assert_called_once()
                call_args = mock_log.error.call_args
                # The error message must include enough context to debug.
                assert "audit row dropped" in call_args.args[0]

    def test_db_module_unavailable_is_silent(self):
        """If the DB module itself can't import (e.g. test context with no
        SQLite path), stay silent — that's not a regression worth surfacing."""
        orch = QuoteOrchestrator(persist_audit=True)
        result = OrchestratorResult(quote=_quote())

        with patch.dict("sys.modules", {"src.core.db": None}):
            # ImportError path — no warning, no log.error.
            orch._persist_audit(_quote(), _attempt(), actor="operator", result=result)
        assert not any("audit row dropped" in w for w in result.warnings), result.warnings
