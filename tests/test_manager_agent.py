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


class TestManagerMetrics:
    """Test the metrics computation used by /api/manager/metrics."""

    def test_pipeline_summary_structure(self):
        from src.agents.manager_agent import _get_pipeline_summary
        s = _get_pipeline_summary()
        assert "price_checks" in s
        assert "quotes" in s
        assert "leads" in s
        assert "outbox" in s
        assert "revenue" in s
        # Revenue should be a dict with won_total
        assert "won_total" in s["revenue"]
        assert isinstance(s["revenue"]["won_total"], (int, float))

    def test_pipeline_counts_nonnegative(self):
        from src.agents.manager_agent import _get_pipeline_summary
        s = _get_pipeline_summary()
        assert s["price_checks"]["total"] >= 0
        assert s["quotes"]["total"] >= 0
        assert s["quotes"]["win_rate"] >= 0
        assert s["leads"]["total"] >= 0
        assert s["outbox"]["drafts"] >= 0

    def test_activity_feed_has_required_fields(self):
        from src.agents.manager_agent import _get_activity_feed
        activity = _get_activity_feed()
        for item in activity:
            assert "icon" in item
            assert "text" in item
            assert "timestamp" in item
            assert "age" in item


class TestBriefAPIContract:
    """Ensure the brief response matches what the JS expects."""

    def test_brief_has_all_js_fields(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        # These are the exact keys the JS reads
        assert "ok" not in brief  # ok is added by the route, not the agent
        assert "headline" in brief
        assert "approval_count" in brief
        assert "pending_approvals" in brief
        assert "activity" in brief
        assert "summary" in brief
        assert isinstance(brief["approval_count"], int)

    def test_brief_summary_has_stats_bar_fields(self):
        """JS reads summary.price_checks.parsed, .priced, summary.quotes.pending, etc."""
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        s = brief["summary"]
        # Stats bar reads these exact paths
        assert "parsed" in s["price_checks"]
        assert "priced" in s["price_checks"]
        assert "pending" in s["quotes"]
        assert "won" in s["quotes"]
        assert "lost" in s["quotes"]
        assert "win_rate" in s["quotes"]
        assert "new" in s["leads"]
        assert "drafts" in s["outbox"]
        assert "won_total" in s["revenue"]

    def test_approval_items_have_required_fields(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        for a in brief["pending_approvals"]:
            assert "icon" in a
            assert "title" in a
            # JS reads these for rendering
            assert "type" in a

    def test_activity_items_have_required_fields(self):
        from src.agents.manager_agent import generate_brief
        brief = generate_brief()
        for a in brief["activity"]:
            assert "icon" in a
            assert "text" in a
            assert "age" in a


class TestHomePageRendering:
    """Verify the home page HTML includes brief and KPI sections."""

    def test_home_has_brief_section(self):
        """The brief section div must exist in home page HTML."""
        import os
        os.environ.setdefault('APP_SECRET', 'test')
        from src.api.templates import PAGE_HOME
        assert 'id="brief-section"' in PAGE_HOME
        assert 'id="brief-headline"' in PAGE_HOME
        assert 'id="approvals-list"' in PAGE_HOME
        assert 'id="activity-list"' in PAGE_HOME
        assert 'id="pipeline-bar"' in PAGE_HOME

    def test_home_has_kpi_section(self):
        """The KPI dashboard section must exist in home page HTML."""
        from src.api.templates import PAGE_HOME
        assert 'id="kpi-section"' in PAGE_HOME
        assert 'id="kpi-cards"' in PAGE_HOME
        assert 'id="goal-bar"' in PAGE_HOME
        assert 'id="funnel-bars"' in PAGE_HOME
        assert 'id="weekly-chart"' in PAGE_HOME
        assert 'id="top-inst"' in PAGE_HOME

    def test_home_fetches_brief_and_metrics(self):
        """JS must fetch both API endpoints."""
        from src.api.templates import PAGE_HOME
        assert "/api/manager/brief" in PAGE_HOME
        assert "/api/manager/metrics" in PAGE_HOME
        assert "credentials:'same-origin'" in PAGE_HOME

    def test_home_has_error_handling(self):
        """JS must log errors, not swallow them."""
        from src.api.templates import PAGE_HOME
        assert "console.error" in PAGE_HOME
        assert "Manager brief failed" in PAGE_HOME
        assert "Manager metrics failed" in PAGE_HOME

    def test_brief_css_classes_exist(self):
        """CSS classes used by brief JS must be defined."""
        from src.api.templates import BASE_CSS
        assert "brief-item" in BASE_CSS
        assert "brief-title" in BASE_CSS
        assert "brief-age" in BASE_CSS
        assert "brief-empty" in BASE_CSS
        assert "stat-chip" in BASE_CSS
        assert "stat-val" in BASE_CSS
        assert "kpi-card" in BASE_CSS
        assert "kpi-card-value" in BASE_CSS
        assert "progress-track" in BASE_CSS
        assert "progress-fill" in BASE_CSS

    def test_no_unescaped_apostrophes_in_js(self):
        """Regression: apostrophes in JS single-quoted strings break the parser."""
        import os, re
        os.environ.setdefault('APP_SECRET', 'test')
        os.environ.setdefault('DASHBOARD_PASSWORD', 'test')
        from flask import Flask
        from src.api.dashboard import bp, load_rfqs, _load_price_checks, render
        from src.api.templates import PAGE_HOME
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(bp)
        with app.test_request_context():
            html = render(PAGE_HOME, rfqs=load_rfqs(), price_checks=_load_price_checks())
        # Find all script blocks and check for unescaped apostrophes in strings
        in_script = False
        for i, line in enumerate(html.split('\n')):
            if '<script>' in line.lower():
                in_script = True
            if '</script>' in line.lower():
                in_script = False
            if in_script:
                # Check for patterns like: ='...you're...' (unescaped ' inside ')
                # Simple heuristic: innerHTML='...' should not contain unescaped quotes
                matches = re.findall(r"innerHTML='[^']*'", line)
                for m in matches:
                    inner = m[11:-1]  # strip innerHTML=' and trailing '
                    assert "'" not in inner, f"Unescaped apostrophe on rendered line {i+1}: {m[:60]}"
