"""Tests for the Lead Generation Agent."""

import pytest
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agents.lead_gen_agent import (
    evaluate_po, add_lead, get_leads, update_lead_status,
    score_opportunity, draft_outreach_email, get_agent_status,
    get_lead_analytics, _create_lead, KNOWN_INSTITUTIONS,
)


class TestScoreOpportunity:
    def test_high_score_known_institution_with_history(self):
        po = {"institution": "CSP-Sacramento", "total_value": 2000,
               "due_date": "03/01/2026"}
        match = {"match_confidence": 0.8, "our_price": 10.0, "scprs_price": 15.0}
        score = score_opportunity(po, match)
        assert score > 0.6  # Should be a strong lead

    def test_low_score_unknown_institution(self):
        po = {"institution": "Random Corp", "total_value": 150}
        match = {"match_confidence": 0.1, "our_price": 0, "scprs_price": 0}
        score = score_opportunity(po, match)
        assert score < 0.3

    def test_sweet_spot_value(self):
        po1 = {"institution": "CSP-Sacramento", "total_value": 5000}
        po2 = {"institution": "CSP-Sacramento", "total_value": 50}
        match = {"match_confidence": 0.5}
        s1 = score_opportunity(po1, match.copy())
        s2 = score_opportunity(po2, match.copy())
        assert s1 > s2  # $5000 PO scores higher than $50

    def test_price_advantage_boosts_score(self):
        po = {"institution": "CSP-Sacramento", "total_value": 3000}
        m1 = {"match_confidence": 0.5, "our_price": 8.0, "scprs_price": 15.0}
        m2 = {"match_confidence": 0.5, "our_price": 14.0, "scprs_price": 15.0}
        s1 = score_opportunity(po, m1)
        s2 = score_opportunity(po, m2)
        assert s1 > s2  # Bigger price gap = higher score

    def test_score_breakdown_populated(self):
        po = {"institution": "CSP-Sacramento", "total_value": 2000}
        match = {"match_confidence": 0.6}
        score_opportunity(po, match)
        assert "score_breakdown" in match
        assert "item_match" in match["score_breakdown"]
        assert "institution" in match["score_breakdown"]


class TestEvaluatePO:
    def test_rejects_too_small(self):
        po = {"total_value": 10, "institution": "CSP-Sacramento"}
        result = evaluate_po(po)
        assert result is None

    def test_rejects_too_large(self):
        po = {"total_value": 100000, "institution": "CSP-Sacramento"}
        result = evaluate_po(po)
        assert result is None

    def test_accepts_good_opportunity(self):
        po = {
            "po_number": f"PO-TEST-{time.time()}",
            "institution": "CSP-Sacramento",
            "agency": "CDCR",
            "total_value": 3000,
            "items_count": 5,
            "items": [{"description": "nitrile exam gloves large"}],
        }
        won_history = [
            {"description": "nitrile exam gloves medium", "unit_price": 8.50},
        ]
        result = evaluate_po(po, won_history)
        assert result is not None
        assert result["status"] == "new"
        assert result["score"] > 0

    def test_low_confidence_rejected(self):
        po = {
            "po_number": "PO-LOWCONF",
            "institution": "Unknown Place",
            "total_value": 200,
            "items": [{"description": "quantum flux capacitor"}],
        }
        result = evaluate_po(po)
        assert result is None  # Below confidence threshold


class TestLeadManagement:
    def test_add_and_get_leads(self):
        lead = _create_lead(
            {"po_number": f"PO-ADD-{time.time()}", "institution": "CSP-Sacramento", "total_value": 1000},
            {"type": "test", "category": "office"},
            0.75,
        )
        result = add_lead(lead)
        assert result["ok"] is True

        leads = get_leads()
        assert any(l["id"] == lead["id"] for l in leads)

    def test_duplicate_rejection(self):
        po_num = f"PO-DUP-{time.time()}"
        lead1 = _create_lead(
            {"po_number": po_num, "institution": "CSP-Sacramento", "total_value": 1000},
            {"type": "test"}, 0.7,
        )
        lead2 = _create_lead(
            {"po_number": po_num, "institution": "CSP-Sacramento", "total_value": 1000},
            {"type": "test"}, 0.8,
        )
        add_lead(lead1)
        result = add_lead(lead2)
        assert result["ok"] is False
        assert result["reason"] == "duplicate"

    def test_update_status(self):
        lead = _create_lead(
            {"po_number": f"PO-STATUS-{time.time()}", "institution": "Test", "total_value": 500},
            {"type": "test"}, 0.6,
        )
        add_lead(lead)

        result = update_lead_status(lead["id"], "contacted", "Called buyer")
        assert result["ok"] is True
        assert result["lead"]["status"] == "contacted"

    def test_invalid_status(self):
        result = update_lead_status("fake-id", "invalid_status")
        assert result["ok"] is False
        assert "Invalid status" in result["error"]


class TestOutreachDraft:
    def test_draft_has_required_fields(self):
        lead = _create_lead(
            {"po_number": "PO-12345", "institution": "CSP-Sacramento",
             "buyer_name": "J. Rodriguez", "buyer_email": "jrod@cdcr.ca.gov",
             "total_value": 2000},
            {"type": "test", "matched_items": [{"description": "name tags"}],
             "savings_pct": 15},
            0.8,
        )
        draft = draft_outreach_email(lead)
        assert "subject" in draft
        assert "body" in draft
        assert "CSP-Sacramento" in draft["subject"]
        assert "PO-12345" in draft["body"]
        assert "Reytech" in draft["body"]

    def test_draft_mentions_savings(self):
        lead = _create_lead(
            {"po_number": "PO-99", "institution": "CIM", "total_value": 1000},
            {"type": "test", "savings_pct": 20, "matched_items": []},
            0.7,
        )
        draft = draft_outreach_email(lead)
        assert "20%" in draft["body"]


class TestAgentStatus:
    def test_returns_status(self):
        status = get_agent_status()
        assert status["agent"] == "lead_gen"
        assert "total_leads" in status
        assert "leads_by_status" in status

    def test_analytics(self):
        analytics = get_lead_analytics()
        assert "total_leads" in analytics
        assert "conversion_rate" in analytics


class TestKnownInstitutions:
    def test_cdcr_prisons_included(self):
        assert "CSP-Sacramento" in KNOWN_INSTITUTIONS
        assert "CIM" in KNOWN_INSTITUTIONS

    def test_calvet_included(self):
        assert "CalVet-Barstow" in KNOWN_INSTITUTIONS

    def test_dsh_included(self):
        assert "DSH-Atascadero" in KNOWN_INSTITUTIONS
