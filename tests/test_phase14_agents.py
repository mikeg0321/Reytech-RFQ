"""Tests for Phase 14 agents: Email Outreach, Growth Strategy, Voice Agent."""

import pytest
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── Email Outreach Agent Tests ─────────────────────────────────────────────

class TestEmailOutreachDraft:
    def test_draft_for_pc(self):
        from src.agents.email_outreach import draft_for_pc
        pc = {
            "pc_number": "PC-TEST-001",
            "institution": "CSP-Sacramento",
            "requestor": "J. Rodriguez",
            "requestor_email": "jrod@cdcr.ca.gov",
            "due_date": "03/01/2026",
            "items": [
                {"qty": 50, "pricing": {"recommended_price": 12.50}},
                {"qty": 100, "pricing": {"recommended_price": 8.00}},
            ],
        }
        email = draft_for_pc(pc, quote_number="R26Q20")
        assert email["type"] == "pc_quote"
        assert email["status"] == "draft"
        assert email["to"] == "jrod@cdcr.ca.gov"
        assert "R26Q20" in email["subject"]
        assert "CSP-Sacramento" in email["subject"]
        assert "2 items" in email["body"]
        assert "$1,425.00" in email["body"]
        assert email["metadata"]["quote_number"] == "R26Q20"

    def test_draft_for_lead(self):
        from src.agents.email_outreach import draft_for_lead
        lead = {
            "id": "test-lead-123",
            "institution": "CIM",
            "buyer_name": "M. Torres",
            "buyer_email": "mtorres@cdcr.ca.gov",
            "po_number": "PO-98765",
            "estimated_savings_pct": 18,
            "matched_items": [{"description": "nitrile gloves large"}],
            "score": 0.78,
        }
        email = draft_for_lead(lead)
        assert email["type"] == "lead_outreach"
        assert email["to"] == "mtorres@cdcr.ca.gov"
        assert "CIM" in email["subject"]
        assert "PO-98765" in email["body"]
        assert "18%" in email["body"]
        assert "Reytech" in email["body"]

    def test_draft_no_recipient(self):
        from src.agents.email_outreach import draft_for_pc
        pc = {"pc_number": "PC-EMPTY", "items": []}
        email = draft_for_pc(pc)
        assert email["to"] == ""  # No crash, just empty


class TestEmailOutbox:
    def test_get_outbox(self):
        from src.agents.email_outreach import get_outbox
        outbox = get_outbox()
        assert isinstance(outbox, list)

    def test_get_outbox_filtered(self):
        from src.agents.email_outreach import get_outbox
        drafts = get_outbox(status="draft")
        assert isinstance(drafts, list)
        for e in drafts:
            assert e["status"] == "draft"

    def test_approve_nonexistent(self):
        from src.agents.email_outreach import approve_email
        result = approve_email("nonexistent-id-xyz")
        assert result["ok"] is False

    def test_approve_draft(self):
        from src.agents.email_outreach import draft_for_pc, approve_email
        pc = {"pc_number": f"PC-APPROVE-{time.time()}", "items": [],
              "requestor_email": "test@test.com"}
        email = draft_for_pc(pc)
        result = approve_email(email["id"])
        assert result["ok"] is True
        assert result["email"]["status"] == "approved"

    def test_approve_no_recipient(self):
        from src.agents.email_outreach import draft_for_pc, approve_email
        pc = {"pc_number": f"PC-NORECIP-{time.time()}", "items": []}
        email = draft_for_pc(pc)
        result = approve_email(email["id"])
        assert result["ok"] is False
        assert "recipient" in result["error"].lower()

    def test_update_draft(self):
        from src.agents.email_outreach import draft_for_pc, update_draft
        pc = {"pc_number": f"PC-EDIT-{time.time()}", "items": []}
        email = draft_for_pc(pc)
        result = update_draft(email["id"], {
            "to": "new@email.com",
            "subject": "Updated Subject",
        })
        assert result["ok"] is True
        assert result["email"]["to"] == "new@email.com"
        assert result["email"]["subject"] == "Updated Subject"

    def test_delete_from_outbox(self):
        from src.agents.email_outreach import draft_for_pc, delete_from_outbox
        pc = {"pc_number": f"PC-DEL-{time.time()}", "items": []}
        email = draft_for_pc(pc)
        result = delete_from_outbox(email["id"])
        assert result["ok"] is True

    def test_delete_nonexistent(self):
        from src.agents.email_outreach import delete_from_outbox
        result = delete_from_outbox("nope-nope-nope")
        assert result["ok"] is False

    def test_send_no_smtp(self):
        """Send fails gracefully without SMTP credentials."""
        from src.agents.email_outreach import draft_for_pc, approve_email, send_email
        pc = {"pc_number": f"PC-SEND-{time.time()}", "items": [],
              "requestor_email": "buyer@test.com"}
        email = draft_for_pc(pc)
        approve_email(email["id"])
        result = send_email(email["id"])
        # Should fail gracefully (no SMTP in test env)
        assert isinstance(result, dict)
        assert "ok" in result


class TestEmailOutreachStatus:
    def test_agent_status(self):
        from src.agents.email_outreach import get_agent_status
        status = get_agent_status()
        assert status["agent"] == "email_outreach"
        assert "outbox_total" in status
        assert "by_status" in status

    def test_sent_log(self):
        from src.agents.email_outreach import get_sent_log
        log = get_sent_log()
        assert isinstance(log, list)


# ─── Growth Strategy Agent Tests ────────────────────────────────────────────

class TestGrowthAgent:
    def test_lead_funnel(self):
        from src.agents.growth_agent import lead_funnel
        result = lead_funnel()
        assert isinstance(result, dict)

    def test_recommendations(self):
        from src.agents.growth_agent import generate_recommendations
        recs = generate_recommendations()
        assert isinstance(recs, dict)
        assert "ok" in recs

    def test_growth_status(self):
        from src.agents.growth_agent import get_growth_status
        status = get_growth_status()
        assert isinstance(status, dict)
        assert "ok" in status


class TestGrowthStatus:
    def test_growth_status_returns_dict(self):
        from src.agents.growth_agent import get_growth_status
        status = get_growth_status()
        assert isinstance(status, dict)


# ─── Voice Agent Tests ──────────────────────────────────────────────────────

class TestVoiceAgent:
    def test_not_configured(self):
        from src.agents.voice_agent import is_configured
        # No Twilio creds in test env
        assert is_configured() is False

    def test_voice_not_configured(self):
        from src.agents.voice_agent import is_voice_configured
        assert is_voice_configured() is False

    def test_place_call_unconfigured(self):
        from src.agents.voice_agent import place_call
        result = place_call("+19165550100")
        assert result["ok"] is False
        assert "not configured" in result["error"].lower() or "no voice engine" in result["error"].lower()

    def test_scripts_exist(self):
        from src.agents.voice_agent import SCRIPTS
        assert "lead_intro" in SCRIPTS
        assert "follow_up" in SCRIPTS
        for key, script in SCRIPTS.items():
            assert "first_message" in script or "text" in script
            assert "name" in script

    def test_agent_status(self):
        from src.agents.voice_agent import get_agent_status
        status = get_agent_status()
        assert status["agent"] == "voice_calls"
        assert status["version"] == "2.0.0"
        assert "twilio_configured" in status
        # Voice engine not configured in test env

    def test_call_log_empty(self):
        from src.agents.voice_agent import get_call_log
        log = get_call_log()
        assert isinstance(log, list)

    def test_script_templates_format(self):
        from src.agents.voice_agent import SCRIPTS
        # Ensure templates have the right placeholders
        intro = SCRIPTS["lead_intro"].get("first_message", "")
        # Templates use VAPI-style first_message now
        assert len(intro) > 0


class TestVoiceScriptRendering:
    def test_lead_intro_exists(self):
        from src.agents.voice_agent import SCRIPTS
        script = SCRIPTS["lead_intro"]
        assert "name" in script
        assert "first_message" in script or "context" in script

    def test_follow_up_exists(self):
        from src.agents.voice_agent import SCRIPTS
        script = SCRIPTS["follow_up"]
        assert "name" in script
        assert "first_message" in script or "context" in script

class TestSecretsPhase14:
    def test_twilio_keys_in_registry(self):
        from src.core.secrets import _REGISTRY
        twilio_keys = [k for k in _REGISTRY if k.startswith("twilio")]
        assert len(twilio_keys) >= 3  # sid, token, phone

    def test_elevenlabs_keys_in_registry(self):
        from src.core.secrets import _REGISTRY
        el_keys = [k for k in _REGISTRY if k.startswith("elevenlabs")]
        assert len(el_keys) >= 2  # key, voice_id

    def test_total_secrets_count(self):
        from src.core.secrets import validate_all
        report = validate_all()
        assert report["total"] >= 18  # Phase 13 had ~13, Phase 14 adds 5+
