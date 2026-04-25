"""Regression guard for the post-send pipeline after the S7 silo fix.

Background: `award_check_queue` was a vestigial table written by
`on_quote_sent` since 2026-03-16 but never read anywhere — the consumer
shipped 2 weeks later (`agents/award_tracker.py`) used a different design.
Migration 29 dropped the table; this PR removed the write side. These
tests fail loudly if either re-appears.

See `docs/DATA_ARCHITECTURE_MAP.md` §7 row S7 for the full audit trail.
"""
from __future__ import annotations

import inspect

from src.agents import post_send_pipeline


def test_on_quote_sent_returns_summary_dict():
    """Function still returns the {tracked, follow_ups, total} contract callers expect."""
    result = post_send_pipeline.on_quote_sent(
        record_type="quote",
        record_id="test-123",
        record_data={
            "solicitation_number": "SOL-TEST-1",
            "line_items": [
                {"price_per_unit": 10.50, "quantity": 2},
                {"bid_price": "$5.00", "qty": "3"},
            ],
        },
    )
    assert result["tracked"] is True
    assert result["follow_ups"] == 3  # day 3, 7, 14
    # 10.50 * 2 + 5.00 * 3 = 36.00
    assert abs(result["total"] - 36.0) < 0.01


def test_on_quote_sent_handles_empty_items():
    """Empty quote — pure log call, no totals."""
    result = post_send_pipeline.on_quote_sent("quote", "empty-1", {})
    assert result == {"tracked": True, "follow_ups": 3, "total": 0}


def test_no_function_in_module_references_award_check_queue():
    """S7 closure: no function body references the dropped table. The module
    docstring keeps a historical note about why the queue is gone, so we scope
    the check to function source, not module source."""
    func_source = inspect.getsource(post_send_pipeline.on_quote_sent)
    assert "award_check_queue" not in func_source, (
        "award_check_queue was dropped in migration 29 (S7). "
        "Do not re-introduce writes — award_tracker.py is the live consumer "
        "and reads quotes/rfqs directly via scprs_schedule.should_check_record."
    )
    # _ensure_tables was deleted entirely; if it comes back, the attribute
    # check below catches it.
    assert not hasattr(post_send_pipeline, "_ensure_tables"), (
        "_ensure_tables() was deleted along with the queue table — restore via "
        "core/migrations.py if a new table needs creating, not boot-time DDL here."
    )


def test_get_sent_quotes_dashboard_no_longer_exists():
    """Tombstone check: the helper that always returned [] is gone, and so
    is the route that called it (/api/v1/quotes/sent-tracker)."""
    assert not hasattr(post_send_pipeline, "get_sent_quotes_dashboard"), (
        "get_sent_quotes_dashboard returned [] since migration 16. Removed "
        "in S7 cleanup along with its only caller in routes_v1.py."
    )
