"""Tests for src/api/email_locator.py — Gmail-side hunt for the original
buyer email when an RFQ was manually uploaded or the thread binding was
lost. Covers query construction (the hard part) and the candidate-fetch
path with the Gmail service stubbed.

PR-B1.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.api.email_locator import (
    build_locator_query, build_locator_queries, locate_candidate_emails,
)


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


# ─────────────────────────────────────────────────────────────────────────
# Bug-8 — progressive query broadening for forwarded mail
# ─────────────────────────────────────────────────────────────────────────

class TestProgressiveQueryBroadening:
    """Bug-8 2026-05-02: a forwarded RFQ has From: = operator's own
    address, not the buyer's. Tier-1 from:<buyer> filter excludes it.
    Tier-2 must drop the from: filter and search by sol# alone, which
    matches the forwarded message's body. Mike hit this on rfq_7813c4e1
    (forwarded the buyer's email to himself, locator returned 0 hits)."""

    def test_forwarded_rfq_emits_sol_only_tier(self):
        rfq = {
            "requestor_email": "mike@reytechinc.com",
            "original_sender": "keith@calvet.ca.gov",
            "solicitation_number": "R26Q38",
        }
        qs = build_locator_queries(rfq)
        assert len(qs) >= 2, "Expected tier-1 + tier-2 for forwarded RFQ"
        # Tier 1 has from: filter
        assert "from:" in qs[0]
        # Tier 2 is sol# only — no from: filter
        assert qs[1].startswith('"R26Q38"')
        assert "from:" not in qs[1]

    def test_direct_rfq_still_emits_sol_only_tier_as_fallback(self):
        # Even when from: matches the buyer, run the sol-only tier as a
        # safety net (e.g. buyer used a different reply address that we
        # don't have on file).
        rfq = {"requestor_email": "k@x.gov", "solicitation_number": "ABC"}
        qs = build_locator_queries(rfq)
        assert len(qs) == 2
        assert "from:" in qs[0]
        assert qs[1] == '"ABC" newer_than:120d in:anywhere'

    def test_no_sol_subject_only_single_tier(self):
        rfq = {"email_subject": "Bid for Veterans Home Wipes"}
        qs = build_locator_queries(rfq)
        # Only one tier — subject keywords with no from: filter
        assert len(qs) == 1
        assert "Veterans" in qs[0]

    def test_runs_tier2_when_tier1_returns_zero(self):
        """The locator accumulates across tiers — if tier-1 (with from:)
        returns nothing, tier-2 (sol-only) still gets a chance."""
        rfq = {
            "requestor_email": "mike@reytechinc.com",  # forwarder
            "solicitation_number": "R26Q38",
        }
        # First call (T1, with from:) returns nothing; second (T2 sol-only) returns one
        list_calls = []

        def _list_side(service, query="", max_results=10):
            list_calls.append(query)
            if "from:" in query:
                return []
            return ["g_via_body"]

        with patch("src.core.gmail_api.list_message_ids",
                   side_effect=_list_side), \
             patch("src.core.gmail_api.get_message_metadata",
                   return_value={"thread_id": "thr1", "subject": "Fwd: bid",
                                 "from": "mike@reytechinc.com", "to": "",
                                 "cc": "", "date": "", "message_id": "<m@x>"}):
            cands = locate_candidate_emails(service=object(), rfq=rfq)
        # Both tiers were tried
        assert len(list_calls) == 2
        assert "from:" in list_calls[0]
        assert "from:" not in list_calls[1]
        # And tier-2 result is in the candidates with match_tier=2
        assert len(cands) == 1
        assert cands[0]["gmail_id"] == "g_via_body"
        assert cands[0]["match_tier"] == 2

    def test_dedupes_across_tiers(self):
        """The same gmail_id surfacing in tier-1 + tier-2 is reported once
        (with tier=1, since that's where it matched first)."""
        rfq = {"requestor_email": "k@x.gov", "solicitation_number": "ABC"}
        with patch("src.core.gmail_api.list_message_ids",
                   side_effect=lambda svc, query="", max_results=10: ["dup_g"]), \
             patch("src.core.gmail_api.get_message_metadata",
                   return_value={"thread_id": "thr", "subject": "s",
                                 "from": "k@x.gov", "to": "", "cc": "",
                                 "date": "", "message_id": "<m>"}):
            cands = locate_candidate_emails(service=object(), rfq=rfq)
        assert len(cands) == 1
        assert cands[0]["match_tier"] == 1
