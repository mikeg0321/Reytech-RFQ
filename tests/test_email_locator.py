"""Tests for src/api/email_locator.py — Gmail-side hunt for the original
buyer email when an RFQ was manually uploaded or the thread binding was
lost. Covers query construction (the hard part) and the candidate-fetch
path with the Gmail service stubbed.

PR-B1.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.api.email_locator import build_locator_query, locate_candidate_emails


# ─────────────────────────────────────────────────────────────────────────
# build_locator_query — pure logic, no Gmail
# ─────────────────────────────────────────────────────────────────────────

class TestBuildLocatorQuery:

    def test_buyer_email_and_sol_number(self):
        rfq = {
            "requestor_email": "Keith.Alsing@calvet.ca.gov",
            "solicitation_number": "R26Q38",
        }
        q = build_locator_query(rfq)
        # Buyer email is lowercased and quoted; full local part preserved
        assert 'from:"keith.alsing@calvet.ca.gov"' in q
        assert '"R26Q38"' in q
        assert "newer_than:120d" in q
        assert "in:anywhere" in q

    def test_multiple_buyer_emails_or_grouped(self):
        rfq = {
            "requestor_email": "buyer@cdcr.ca.gov",
            "original_sender": "fwd@example.com",
            "solicitation_number": "ABC123",
        }
        q = build_locator_query(rfq)
        # OR grouping wraps both emails
        assert 'from:"buyer@cdcr.ca.gov"' in q
        assert 'from:"fwd@example.com"' in q
        assert " OR " in q

    def test_dedupes_buyer_emails(self):
        rfq = {
            "requestor_email": "Buyer@ca.gov",
            "original_sender": "buyer@ca.gov",  # case-different duplicate
            "solicitation_number": "X12345",
        }
        q = build_locator_query(rfq)
        assert q.count("buyer@ca.gov") == 1

    def test_falls_back_to_subject_when_no_sol_number(self):
        rfq = {
            "requestor_email": "k@x.gov",
            "email_subject": "Re: RFQ for Flushable Wipes — bid by Friday",
        }
        q = build_locator_query(rfq)
        # Re: prefix stripped, short words filtered, single-quoted phrase
        assert "Flushable" in q
        assert "Wipes" in q
        assert "Re:" not in q

    def test_skips_placeholder_sol_numbers(self):
        # WORKSHEET / TBD / RFQ etc. are sentinels — must not enter the query
        rfq = {
            "requestor_email": "k@x.gov",
            "solicitation_number": "WORKSHEET",
            "email_subject": "Real Bid Subject Phrase",
        }
        q = build_locator_query(rfq)
        assert "WORKSHEET" not in q
        # Falls back to subject
        assert "Real" in q or "Bid" in q

    def test_empty_when_nothing_to_search_on(self):
        rfq = {}
        assert build_locator_query(rfq) == ""

    def test_max_age_days_overridable(self):
        rfq = {"requestor_email": "k@x.gov", "solicitation_number": "X12345"}
        assert "newer_than:30d" in build_locator_query(rfq, max_age_days=30)


# ─────────────────────────────────────────────────────────────────────────
# locate_candidate_emails — service stubbed
# ─────────────────────────────────────────────────────────────────────────

class TestLocateCandidateEmails:

    def test_returns_empty_when_query_unbuildable(self):
        # No buyer email, no sol#, no subject → empty query → empty list
        assert locate_candidate_emails(service=object(), rfq={}) == []

    def test_returns_candidates_with_metadata(self):
        rfq = {
            "id": "rfq_x",
            "requestor_email": "k@x.gov",
            "solicitation_number": "R26Q38",
        }
        with patch("src.core.gmail_api.list_message_ids",
                   return_value=["g1", "g2"]) as mock_list, \
             patch("src.core.gmail_api.get_message_metadata",
                   side_effect=lambda svc, gid: {
                       "thread_id": f"t_{gid}",
                       "subject": f"Bid for thing ({gid})",
                       "from": "k@x.gov",
                       "to": "sales@reytechinc.com",
                       "cc": "",
                       "date": "Mon, 21 Apr 2026 09:00:00 -0700",
                       "message_id": f"<msgid-{gid}@x.gov>",
                   }):
            cands = locate_candidate_emails(service=object(), rfq=rfq,
                                            max_results=2)
        assert mock_list.called
        assert len(cands) == 2
        c = cands[0]
        # Contract: all the fields the picker UI consumes are present
        assert c["gmail_id"] == "g1"
        assert c["thread_id"] == "t_g1"
        assert c["message_id"].startswith("<msgid-")
        assert c["subject"].startswith("Bid for thing")

    def test_swallows_metadata_failures(self):
        # A single bad message id should not nuke the whole picker.
        rfq = {"requestor_email": "k@x.gov", "solicitation_number": "R26Q38"}

        def _meta(svc, gid):
            if gid == "bad":
                raise RuntimeError("boom")
            return {"thread_id": f"t_{gid}", "subject": "ok", "from": "k@x.gov",
                    "date": "", "message_id": f"<{gid}>", "to": "", "cc": ""}

        with patch("src.core.gmail_api.list_message_ids",
                   return_value=["good", "bad", "good2"]), \
             patch("src.core.gmail_api.get_message_metadata", side_effect=_meta):
            cands = locate_candidate_emails(service=object(), rfq=rfq)
        # Two good candidates survive
        assert [c["gmail_id"] for c in cands] == ["good", "good2"]
