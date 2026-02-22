"""
tests/test_prd28_agents.py — Tests for PRD-28 agents and routes

Tests all 5 new agents:
  WI-1: quote_lifecycle (8 tests)
  WI-2: email_lifecycle (7 tests)
  WI-3: lead_nurture_agent (7 tests)
  WI-4: revenue_engine (6 tests)
  WI-5: vendor_intelligence (5 tests)
  Routes: 10 endpoint tests
"""

import json
import os
import sys
import sqlite3
import pytest
from datetime import datetime, timedelta, timezone

# Ensure repo root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DASH_USER", "reytech")
os.environ.setdefault("DASH_PASS", "changeme")


# ══════════════════════════════════════════════════════════════════════════════
# WI-1: Quote Lifecycle
# ══════════════════════════════════════════════════════════════════════════════

class TestQuoteLifecycle:

    def test_get_agent_status(self):
        from src.agents.quote_lifecycle import get_agent_status
        s = get_agent_status()
        assert s["name"] == "quote_lifecycle"
        assert "pipeline" in s

    def test_get_pipeline_summary(self):
        from src.agents.quote_lifecycle import get_pipeline_summary
        r = get_pipeline_summary()
        assert r.get("ok") is True
        assert "pipeline" in r
        assert "conversion_rate" in r

    def test_check_expirations(self):
        from src.agents.quote_lifecycle import check_expirations
        r = check_expirations()
        assert r.get("ok") is True
        assert "expired" in r
        assert "follow_ups_due" in r

    def test_get_expiring_soon(self):
        from src.agents.quote_lifecycle import get_expiring_soon
        r = get_expiring_soon(30)
        assert isinstance(r, list)

    def test_process_reply_signal_missing_qn(self):
        from src.agents.quote_lifecycle import process_reply_signal
        r = process_reply_signal("", "win")
        assert r["ok"] is False
        assert "no quote_number" in r.get("error", "")

    def test_process_reply_signal_not_found(self):
        from src.agents.quote_lifecycle import process_reply_signal
        r = process_reply_signal("NONEXISTENT-Q999", "win", 0.9)
        assert r["ok"] is False
        assert "not found" in r.get("error", "")

    def test_save_revision_not_found(self):
        from src.agents.quote_lifecycle import save_revision
        r = save_revision("NONEXISTENT-Q999")
        assert r["ok"] is False

    def test_get_revisions_empty(self):
        from src.agents.quote_lifecycle import get_revisions
        r = get_revisions("NONEXISTENT-Q999")
        assert isinstance(r, list)
        assert len(r) == 0

    def test_close_lost_to_competitor(self):
        from src.agents.quote_lifecycle import close_lost_to_competitor
        r = close_lost_to_competitor("NONEXISTENT-Q999", "Medline", 1234.56)
        assert r["ok"] is False  # Quote doesn't exist


# ══════════════════════════════════════════════════════════════════════════════
# WI-2: Email Lifecycle
# ══════════════════════════════════════════════════════════════════════════════

class TestEmailLifecycle:

    def test_get_agent_status(self):
        from src.agents.email_lifecycle import get_agent_status
        s = get_agent_status()
        assert s["name"] == "email_lifecycle"
        assert "drafts" in s

    def test_get_outbox_summary(self):
        from src.agents.email_lifecycle import get_outbox_summary
        r = get_outbox_summary()
        assert "drafts" in r
        assert "total" in r
        assert "open_rate" in r

    def test_get_engagement_stats(self):
        from src.agents.email_lifecycle import get_engagement_stats
        r = get_engagement_stats()
        assert "total_sent" in r
        assert "open_rate" in r
        assert "click_rate" in r

    def test_bulk_approve_no_ids(self):
        from src.agents.email_lifecycle import bulk_approve
        r = bulk_approve([])  # Empty list = approve nothing
        assert r["ok"] is True
        # approved might be 0 since no IDs matched

    def test_retry_failed_emails(self):
        from src.agents.email_lifecycle import retry_failed_emails
        r = retry_failed_emails()
        assert r["ok"] is True
        assert "retried" in r

    def test_generate_tracking_id(self):
        from src.agents.email_lifecycle import generate_tracking_id
        tid = generate_tracking_id()
        assert tid.startswith("trk-")
        assert len(tid) > 8

    def test_record_engagement(self):
        from src.agents.email_lifecycle import record_engagement, generate_tracking_id
        tid = generate_tracking_id()
        r = record_engagement(tid, "open", "127.0.0.1", "TestBot")
        assert r["ok"] is True


# ══════════════════════════════════════════════════════════════════════════════
# WI-3: Lead Nurture Agent
# ══════════════════════════════════════════════════════════════════════════════

class TestLeadNurture:

    def test_get_agent_status(self):
        from src.agents.lead_nurture_agent import get_agent_status
        s = get_agent_status()
        assert s["name"] == "lead_nurture"
        assert "total_leads" in s

    def test_get_unified_pipeline(self):
        from src.agents.lead_nurture_agent import get_unified_pipeline
        r = get_unified_pipeline()
        assert "leads" in r
        assert "total" in r
        assert "by_status" in r
        assert "by_source" in r

    def test_start_nurture_missing_lead(self):
        from src.agents.lead_nurture_agent import start_nurture
        r = start_nurture("nonexistent-lead-999")
        assert r["ok"] is False
        assert "not found" in r.get("error", "")

    def test_pause_nurture_missing(self):
        from src.agents.lead_nurture_agent import pause_nurture
        r = pause_nurture("nonexistent-lead-999")
        assert r["ok"] is False

    def test_process_nurture_queue(self):
        from src.agents.lead_nurture_agent import process_nurture_queue
        r = process_nurture_queue()
        assert r["ok"] is True
        assert "drafts_created" in r

    def test_rescore_all_leads(self):
        from src.agents.lead_nurture_agent import rescore_all_leads
        r = rescore_all_leads()
        assert r["ok"] is True
        assert "rescored" in r
        assert "total" in r

    def test_convert_lead_missing(self):
        from src.agents.lead_nurture_agent import convert_lead_to_customer
        r = convert_lead_to_customer("nonexistent-lead-999")
        assert r["ok"] is False


# ══════════════════════════════════════════════════════════════════════════════
# WI-4: Revenue Engine
# ══════════════════════════════════════════════════════════════════════════════

class TestRevenueEngine:

    def test_get_agent_status(self):
        from src.agents.revenue_engine import get_agent_status
        s = get_agent_status()
        assert s["name"] == "revenue_engine"
        assert "ytd_revenue" in s

    def test_reconcile_revenue(self):
        from src.agents.revenue_engine import reconcile_revenue
        r = reconcile_revenue()
        assert r.get("ok") is True
        assert "synced" in r

    def test_forecast_pipeline(self):
        from src.agents.revenue_engine import forecast_pipeline
        r = forecast_pipeline()
        assert r.get("ok") is True
        assert "total_raw" in r
        assert "total_weighted" in r

    def test_get_monthly_revenue(self):
        from src.agents.revenue_engine import get_monthly_revenue
        r = get_monthly_revenue(6)
        assert r.get("ok") is True
        assert "months" in r
        assert len(r["months"]) <= 12

    def test_get_goal_progress(self):
        from src.agents.revenue_engine import get_goal_progress
        r = get_goal_progress()
        assert r.get("ok") is True
        assert "goal" in r
        assert r["goal"] == 2_000_000
        assert "pct_of_goal" in r

    def test_get_margin_analysis(self):
        from src.agents.revenue_engine import get_margin_analysis
        r = get_margin_analysis()
        assert r.get("ok") is True
        assert "avg_margin" in r

    def test_get_revenue_dashboard(self):
        from src.agents.revenue_engine import get_revenue_dashboard
        r = get_revenue_dashboard()
        assert r.get("ok") is True
        assert "goal" in r
        assert "monthly" in r
        assert "pipeline" in r
        assert "top_customers" in r


# ══════════════════════════════════════════════════════════════════════════════
# WI-5: Vendor Intelligence
# ══════════════════════════════════════════════════════════════════════════════

class TestVendorIntelligence:

    def test_get_agent_status(self):
        from src.agents.vendor_intelligence import get_agent_status
        s = get_agent_status()
        assert s["name"] == "vendor_intelligence"
        assert "total_vendors" in s

    def test_get_enrichment_status(self):
        from src.agents.vendor_intelligence import get_enrichment_status
        r = get_enrichment_status()
        assert "total_vendors" in r
        assert "email_pct" in r

    def test_score_all_vendors(self):
        from src.agents.vendor_intelligence import score_all_vendors
        r = score_all_vendors()
        assert r.get("ok") is True
        assert "scored" in r

    def test_get_preferred_vendors(self):
        from src.agents.vendor_intelligence import get_preferred_vendors
        r = get_preferred_vendors()
        assert r.get("ok") is True
        assert "matrix" in r

    def test_compare_vendors(self):
        from src.agents.vendor_intelligence import compare_vendors
        r = compare_vendors("nitrile gloves")
        assert isinstance(r, list)


# ══════════════════════════════════════════════════════════════════════════════
# Route / API Endpoint Tests
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def client():
    import logging
    logging.disable(logging.WARNING)
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


class TestPRD28Routes:

    def test_revenue_page(self, client):
        r = client.get("/revenue", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        assert r.status_code == 200
        assert b"Revenue" in r.data

    def test_api_dashboard_actions(self, client):
        r = client.get("/api/dashboard/actions", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        assert r.status_code == 200
        d = r.get_json()
        assert d.get("ok") is True
        assert "urgent" in d
        assert "action_needed" in d
        assert "progress" in d

    def test_api_quote_lifecycle_status(self, client):
        r = client.get("/api/quote-lifecycle/status", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        assert r.status_code == 200

    def test_api_quote_pipeline(self, client):
        r = client.get("/api/quote-lifecycle/pipeline", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        d = r.get_json()
        assert d.get("ok") is True

    def test_api_outbox_summary(self, client):
        r = client.get("/api/outbox/summary", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        d = r.get_json()
        assert "drafts" in d

    def test_api_revenue_goal(self, client):
        r = client.get("/api/revenue/goal", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        d = r.get_json()
        assert d.get("ok") is True
        assert d.get("goal") == 2_000_000

    def test_api_revenue_monthly(self, client):
        r = client.get("/api/revenue/monthly?months=6", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        d = r.get_json()
        assert d.get("ok") is True

    def test_api_leads_pipeline(self, client):
        r = client.get("/api/leads/pipeline", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        d = r.get_json()
        assert "leads" in d

    def test_api_vendor_enrichment(self, client):
        r = client.get("/api/vendor/enrichment", headers={"Authorization": "Basic cmV5dGVjaDpjaGFuZ2VtZQ=="})
        d = r.get_json()
        assert "total_vendors" in d

    def test_email_tracking_pixel(self, client):
        r = client.get("/api/email/track/test-trk-123/open")
        assert r.status_code == 200
        assert r.content_type == "image/gif"
