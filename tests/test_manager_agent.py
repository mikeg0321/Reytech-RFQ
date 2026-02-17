"""Tests for Manager Agent — brief, approvals, activity feed."""

import pytest
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestManagerBrief:
    def test_generate_brief_structure(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        assert "generated_at" in brief
        assert "headline" in brief
        assert "headlines" in brief
        assert "pending_approvals" in brief
        assert "approval_count" in brief
        assert "activity" in brief
        assert "summary" in brief
        assert "recommendations" in brief
        assert isinstance(brief["pending_approvals"], list)
        assert isinstance(brief["activity"], list)
        assert isinstance(brief["approval_count"], int)

    def test_headline_not_empty(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        assert len(brief["headline"]) > 0
        assert len(brief["headlines"]) > 0

    def test_summary_has_all_sections(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        s = brief["summary"]
        assert "price_checks" in s
        assert "quotes" in s
        assert "leads" in s
        assert "outbox" in s
        assert "revenue" in s

    def test_summary_quotes_has_win_rate(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        q = brief["summary"]["quotes"]
        assert "pending" in q
        assert "won" in q
        assert "lost" in q
        assert "win_rate" in q

    def test_summary_pipeline_numbers(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        pc = brief["summary"]["price_checks"]
        assert "total" in pc
        assert "parsed" in pc
        assert "priced" in pc


class TestApprovals:
    def test_pending_approvals_list(self):
        from src.agents.manager_agent import _get_pending_approvals
        approvals = _get_pending_approvals()
        assert isinstance(approvals, list)
        for a in approvals:
            assert "type" in a
            assert "icon" in a
            assert "title" in a

    def test_approval_types(self):
        """All approval types have required fields."""
        from src.agents.manager_agent import _get_pending_approvals
        valid_types = {"email_draft", "email_send", "lead_new", "stale_quote"}
        approvals = _get_pending_approvals()
        for a in approvals:
            assert a["type"] in valid_types


class TestActivityFeed:
    def test_activity_feed_list(self):
        from src.agents.manager_agent import _get_activity_feed
        activity = _get_activity_feed()
        assert isinstance(activity, list)
        for item in activity:
            assert "icon" in item
            assert "text" in item
            assert "timestamp" in item

    def test_activity_feed_limit(self):
        from src.agents.manager_agent import _get_activity_feed
        feed = _get_activity_feed(limit=3)
        assert len(feed) <= 3

    def test_activity_sorted_newest_first(self):
        from src.agents.manager_agent import _get_activity_feed
        feed = _get_activity_feed()
        timestamps = [f["timestamp"] for f in feed if f["timestamp"]]
        # Should be descending
        assert timestamps == sorted(timestamps, reverse=True)


class TestPipelineSummary:
    def test_pipeline_summary_structure(self):
        from src.agents.manager_agent import _get_pipeline_summary
        s = _get_pipeline_summary()
        assert "price_checks" in s
        assert "quotes" in s
        assert "leads" in s
        assert "outbox" in s
        assert "revenue" in s

    def test_pipeline_counts_are_ints(self):
        from src.agents.manager_agent import _get_pipeline_summary
        s = _get_pipeline_summary()
        assert isinstance(s["price_checks"]["total"], int)
        assert isinstance(s["quotes"]["total"], int)
        assert isinstance(s["leads"]["total"], int)


class TestAgeStr:
    def test_empty(self):
        from src.agents.manager_agent import _age_str
        assert _age_str("") == ""
        assert _age_str(None) == ""

    def test_recent(self):
        from src.agents.manager_agent import _age_str
        from datetime import datetime
        now = datetime.now().isoformat()
        result = _age_str(now)
        assert result == "just now"

    def test_hours_ago(self):
        from src.agents.manager_agent import _age_str
        from datetime import datetime, timedelta
        two_hours_ago = (datetime.now() - timedelta(hours=2)).isoformat()
        result = _age_str(two_hours_ago)
        assert "h ago" in result

    def test_days_ago(self):
        from src.agents.manager_agent import _age_str
        from datetime import datetime, timedelta
        three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
        result = _age_str(three_days_ago)
        assert "3d ago" in result


class TestManagerStatus:
    def test_agent_status(self):
        from src.agents.manager_agent import get_agent_status
        status = get_agent_status()
        assert status["agent"] == "manager"
        assert status["version"] == "1.0.0"
        assert status["brief_available"] is True
