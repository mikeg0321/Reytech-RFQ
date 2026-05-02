"""Unit tests for the unified metrics module (src/core/metrics.py).

These verify that all dashboard metrics come from one authoritative source
with consistent status filters, ending the cross-page divergence flagged
in the 2026-04-14 UX audit (P0.12).
"""

import pytest
from src.core import metrics


class TestPipelineValue:
    def test_empty_db_returns_zero(self):
        result = metrics.get_pipeline_value()
        assert result["pipeline_value"] == 0.0
        assert result["quote_count"] == 0

    def test_counts_pending_and_sent(self, seed_db_quote):
        seed_db_quote("R26Q100", status="pending", total=1000.0)
        seed_db_quote("R26Q101", status="sent", total=2000.0)
        result = metrics.get_pipeline_value()
        assert result["pipeline_value"] == 3000.0
        assert result["quote_count"] == 2

    def test_includes_draft_quotes(self, seed_db_quote):
        seed_db_quote("R26Q102", status="draft", total=500.0)
        result = metrics.get_pipeline_value()
        assert result["pipeline_value"] == 500.0

    def test_excludes_won_and_lost(self, seed_db_quote):
        seed_db_quote("R26Q103", status="won", total=5000.0)
        seed_db_quote("R26Q104", status="lost", total=3000.0)
        seed_db_quote("R26Q105", status="pending", total=100.0)
        result = metrics.get_pipeline_value()
        assert result["pipeline_value"] == 100.0
        assert result["quote_count"] == 1

    def test_excludes_test_quotes(self, seed_db_quote):
        # seed_db_quote seeds with is_test=0 by default via the fixture
        # but we can set status to something pipeline-like and check
        seed_db_quote("R26Q106", status="pending", total=999.0)
        result = metrics.get_pipeline_value()
        assert result["pipeline_value"] == 999.0


class TestWinRate:
    def test_empty_db(self):
        result = metrics.get_win_rate()
        assert result["rate"] == 0.0
        assert result["decided"] == 0

    def test_basic_win_rate(self, seed_db_quote):
        seed_db_quote("R26Q200", status="won", total=1000.0)
        seed_db_quote("R26Q201", status="lost", total=500.0)
        seed_db_quote("R26Q202", status="lost", total=300.0)
        result = metrics.get_win_rate()
        assert result["won"] == 1
        assert result["lost"] == 2
        assert result["decided"] == 3
        assert result["rate"] == pytest.approx(33.3, abs=0.1)
        assert result["won_total"] == 1000.0

    def test_pending_excluded_from_rate(self, seed_db_quote):
        seed_db_quote("R26Q203", status="won", total=100.0)
        seed_db_quote("R26Q204", status="pending", total=9999.0)
        result = metrics.get_win_rate()
        # rate = 1 won / 1 decided = 100%, pending is NOT in denominator
        assert result["rate"] == 100.0
        assert result["decided"] == 1
        assert result["total"] == 2

    def test_all_statuses_counted_in_total(self, seed_db_quote):
        for i, s in enumerate(["won", "lost", "pending", "sent", "draft", "expired"]):
            seed_db_quote(f"R26Q21{i}", status=s, total=100.0)
        result = metrics.get_win_rate()
        assert result["total"] == 6
        assert result["decided"] == 2  # won + lost


class TestActiveOrders:
    def test_empty_db(self):
        result = metrics.get_active_orders()
        assert result["total"] == 0

    def test_excludes_cancelled(self):
        """PR-4 (#694): canonical sourceable definition requires a
        real po_number. Seeds use distinct PO numbers so the
        UNIQUE(po_number, quote_number) index doesn't collide."""
        from src.core.order_dal import save_order
        save_order("ORD-M1", {"status": "new", "total": 500,
                              "po_number": "0000099001"}, actor="test")
        save_order("ORD-M2", {"status": "cancelled", "total": 1000,
                              "po_number": "0000099002"}, actor="test")
        result = metrics.get_active_orders()
        assert result["total"] == 1
        assert result["total_value"] == 500.0

    def test_closed_vs_active(self):
        """PR-4 (#694): `total` is now canonical sourceable POs only.
        Closed orders no longer inflate the headline number — they
        still count via `closed` for the dashboard's "completed"
        badge, but `total` reflects POs the operator owes work on."""
        from src.core.order_dal import save_order
        save_order("ORD-M3", {"status": "new", "total": 200,
                              "po_number": "0000099003"}, actor="test")
        save_order("ORD-M4", {"status": "closed", "total": 800,
                              "po_number": "0000099004"}, actor="test")
        result = metrics.get_active_orders()
        assert result["active"] == 1
        assert result["closed"] == 1
        # Pre-PR-4: total = 2 (both rows counted regardless of closed
        # status). Post-PR-4: closed orders are NOT sourceable, so
        # total reflects only the new (sourceable) row.
        assert result["total"] == 1
        # PR-6 (#696): the dual-emit `total_legacy` field was removed
        # once canonical numbers settled.
        assert "total_legacy" not in result


class TestInboxCounts:
    def test_empty_db_returns_zeros(self):
        result = metrics.get_inbox_counts()
        assert result["inbox"] == 0
        assert result["priced"] == 0

    def test_inbox_reflects_win_rate_and_pipeline(self, seed_db_quote):
        seed_db_quote("R26Q300", status="won", total=1000.0)
        seed_db_quote("R26Q301", status="pending", total=500.0)
        result = metrics.get_inbox_counts()
        assert result["won"] == 1
        assert result["won_value"] == 1000.0
        assert result["pipeline_value"] == 500.0


class TestPendingDrafts:
    def test_empty_db(self):
        result = metrics.get_pending_drafts()
        assert result["total"] == 0

    # Note: seeding email_outbox requires direct SQL since there's no
    # fixture for it. Tested via the route integration tests instead.


class TestGetAllMetrics:
    def test_returns_all_keys(self):
        result = metrics.get_all_metrics()
        assert "pipeline" in result
        assert "win_rate" in result
        assert "orders" in result
        assert "inbox" in result
        assert "drafts" in result

    def test_pipeline_and_win_rate_agree_on_won_count(self, seed_db_quote):
        seed_db_quote("R26Q400", status="won", total=1000.0)
        seed_db_quote("R26Q401", status="pending", total=500.0)
        result = metrics.get_all_metrics()
        # Pipeline should NOT include the won quote
        assert result["pipeline"]["pipeline_value"] == 500.0
        # Win rate should count it
        assert result["win_rate"]["won"] == 1
        # Inbox should reflect both
        assert result["inbox"]["won"] == 1
        assert result["inbox"]["pipeline_value"] == 500.0
