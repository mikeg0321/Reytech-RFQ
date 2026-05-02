"""Tests for src/api/draft_builder.py — pure logic builders for the
Gmail draft that goes out for an RFQ. No Gmail API, no DB, no IO.

PR-B3.
"""
from __future__ import annotations

import os
import tempfile

import pytest

from src.api.draft_builder import (
    build_recipients, build_subject, build_body, build_threading_params,
    build_draft_params, resolve_attachments, agency_label_name,
    gmail_draft_url,
)


class TestBuildRecipients:

    def test_prefers_original_sender_over_requestor(self):
        rfq = {
            "original_sender": "buyer@x.gov",
            "requestor_email": "forwarder@y.gov",
        }
        to, cc = build_recipients(rfq)
        assert to == "buyer@x.gov"
        assert cc == ""

    def test_falls_back_to_requestor_email(self):
        rfq = {"requestor_email": "buyer@x.gov"}
        to, _ = build_recipients(rfq)
        assert to == "buyer@x.gov"

    def test_falls_back_to_email_sender(self):
        rfq = {"email_sender": "buyer@x.gov"}
        to, _ = build_recipients(rfq)
        assert to == "buyer@x.gov"

    def test_returns_empty_when_no_address(self):
        to, cc = build_recipients({})
        assert to == "" and cc == ""

    def test_skips_invalid_addresses(self):
        # "name@" and bare strings without @ should be skipped
        rfq = {"original_sender": "no-at-symbol", "requestor_email": "real@x.gov"}
        to, _ = build_recipients(rfq)
        assert to == "real@x.gov"

    def test_picks_up_cc(self):
        rfq = {"requestor_email": "a@x.gov", "cc_emails": "boss@x.gov"}
        _, cc = build_recipients(rfq)
        assert cc == "boss@x.gov"


class TestBuildSubject:

    def test_re_prefixes_original_subject(self):
        s = build_subject({"email_subject": "Bid for thing", "solicitation_number": "ABC"})
        assert s == "Re: Bid for thing"

    def test_strips_existing_re_fwd_prefixes(self):
        # No "Re: Re: Re:" stack-ups
        s = build_subject({"email_subject": "Re: FW: Fwd: original"})
        assert s == "Re: original"

    def test_falls_back_to_solicitation(self):
        s = build_subject({"solicitation_number": "R26Q38"})
        assert "R26Q38" in s
        assert s.startswith("Quote")

    def test_handles_blank_rfq(self):
        s = build_subject({"id": "rfq_x"})
        assert "rfq_x" in s


class TestBuildBody:

    def test_uses_first_name_only(self):
        body = build_body({"requestor_name": "Keith Alsing", "solicitation_number": "X"})
        assert body.startswith("Dear Keith,")
        assert "Alsing" not in body

    def test_handles_lastname_first_format(self):
        # "Alsing, Keith" → first name = Keith
        body = build_body({"requestor_name": "Alsing, Keith"})
        assert body.startswith("Dear Keith,")

    def test_falls_back_to_procurement_officer(self):
        body = build_body({"requestor_name": "", "solicitation_number": "Y"})
        assert body.startswith("Dear Procurement Officer,")

    def test_no_signature_block(self):
        # Per CLAUDE.md "Gmail Handles Signatures" — never include sig text
        body = build_body({"requestor_name": "Jane", "solicitation_number": "Z"})
        # No common sig phrases
        for phrase in ("Sales Team", "Reytech Inc", "VAT", "FEIN", "555-"):
            assert phrase not in body

    def test_includes_solicitation_when_set(self):
        body = build_body({"solicitation_number": "R26Q38"})
        assert "Solicitation #R26Q38" in body


class TestBuildThreadingParams:

    def test_returns_msg_id_and_thread_id(self):
        rfq = {"email_message_id": "<m@x>", "email_thread_id": "thr_abc"}
        p = build_threading_params(rfq)
        assert p["in_reply_to"] == "<m@x>"
        assert p["references"] == "<m@x>"
        assert p["thread_id"] == "thr_abc"

    def test_returns_none_when_unset(self):
        p = build_threading_params({})
        assert p["in_reply_to"] is None
        assert p["thread_id"] is None


class TestResolveAttachments:

    def test_uses_stored_path_when_on_disk(self, tmp_path):
        target = tmp_path / "quote.pdf"
        target.write_bytes(b"%PDF-1.4\n")
        rfq = {"reytech_quote_pdf": str(target), "solicitation_number": "X"}
        out = resolve_attachments(rfq, manifest=None, data_dir=str(tmp_path))
        assert out == [str(target)]

    def test_uses_manifest_generated_forms(self, tmp_path):
        sol = "R26Q38"
        outdir = tmp_path / "output" / sol
        outdir.mkdir(parents=True)
        f1 = outdir / "704b.pdf"
        f1.write_bytes(b"%PDF\n")
        rfq = {"id": "rfq_x", "solicitation_number": sol}
        manifest = {"generated_forms": [{"form_id": "704b", "filename": "704b.pdf"}]}
        out = resolve_attachments(rfq, manifest, str(tmp_path))
        assert str(f1) in out

    def test_dedupes_when_path_appears_in_multiple_locations(self, tmp_path):
        sol = "X"
        outdir = tmp_path / "output" / sol
        outdir.mkdir(parents=True)
        f = outdir / "quote.pdf"
        f.write_bytes(b"%PDF\n")
        rfq = {
            "id": "rfq_x",
            "solicitation_number": sol,
            "reytech_quote_pdf": str(f),
            "output_files": ["quote.pdf"],
        }
        manifest = {"generated_forms": [{"form_id": "quote", "filename": "quote.pdf"}]}
        out = resolve_attachments(rfq, manifest, str(tmp_path))
        assert out.count(str(f)) == 1

    def test_returns_empty_when_nothing_on_disk(self, tmp_path):
        rfq = {"id": "rfq_x", "solicitation_number": "X"}
        assert resolve_attachments(rfq, None, str(tmp_path)) == []


class TestAgencyLabelName:

    def test_known_agencies_resolve(self):
        assert agency_label_name("calvet") == "CalVet"
        assert agency_label_name("cchcs") == "CCHCS"
        assert agency_label_name("dsh") == "DSH"

    def test_calvet_barstow_uses_parent_calvet_label(self):
        # Barstow ships under the CalVet label — same buyer org
        assert agency_label_name("calvet_barstow") == "CalVet"

    def test_unknown_agency_returns_none(self):
        assert agency_label_name("acme_corp") is None
        assert agency_label_name("") is None

    def test_case_and_whitespace_tolerant(self):
        assert agency_label_name(" CalVet ") == "CalVet"
        assert agency_label_name("CCHCS") == "CCHCS"


class TestBuildDraftParams:

    def test_full_round_trip(self):
        rfq = {
            "id": "rfq_1",
            "requestor_name": "Keith Alsing",
            "requestor_email": "keith@calvet.ca.gov",
            "solicitation_number": "R26Q38",
            "email_subject": "Bid for veterans home wipes",
            "email_message_id": "<m@x>",
            "email_thread_id": "thr_abc",
        }
        out = build_draft_params(rfq, manifest=None, attachments=["/tmp/a.pdf"])
        assert out["to"] == "keith@calvet.ca.gov"
        assert out["subject"] == "Re: Bid for veterans home wipes"
        assert out["body_plain"].startswith("Dear Keith,")
        assert out["thread_id"] == "thr_abc"
        assert out["in_reply_to"] == "<m@x>"
        assert out["attachments"] == ["/tmp/a.pdf"]

    def test_no_thread_id_when_unbound(self):
        rfq = {"requestor_email": "k@x.gov"}
        out = build_draft_params(rfq, None, [])
        assert out["thread_id"] is None
        assert out["in_reply_to"] is None
        assert out["attachments"] is None


class TestGmailDraftUrl:

    def test_builds_drafts_url_from_message_id(self):
        resp = {"id": "draft_123", "message": {"id": "msg_abc", "threadId": "thr_x"}}
        url = gmail_draft_url(resp)
        assert url == "https://mail.google.com/mail/u/0/#drafts/msg_abc"

    def test_falls_back_to_drafts_folder(self):
        # Missing fields → land on Drafts folder rather than 404
        url = gmail_draft_url({})
        assert url == "https://mail.google.com/mail/u/0/#drafts"
