"""Tests for the close-the-loop hardening in QuoteOrchestrator.

Covers:
  1. `generated` stage strict completeness — blocks (outcome=error) when
     finalize() returns a package missing required artifacts or with errors.
  2. `sent` stage actually sends — blocks with a clear reason when recipient
     is empty, transport is unconfigured, or no merged package exists.
  3. Compliance validator rejects 0-byte filled forms (silent pass-through
     gap that previously let incomplete packages advance).

These tests exercise the orchestrator's own logic, not the upstream
fill_engine — that's already covered by `test_calvet_r25q86_e2e.py`.
"""
from __future__ import annotations

import os
from decimal import Decimal
from unittest.mock import patch

import pytest

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
    StageAttempt,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _qa_passed_quote() -> Quote:
    """A Quote that's already at QA_PASS — the entry condition for `generated`."""
    q = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze 4x4", qty=10, unit_cost=Decimal("2.00"))],
        status=QuoteStatus.QA_PASS,
    )
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    q.buyer.requestor_email = "buyer@cchcs.ca.gov"
    return q


class _FakeProfile:
    def __init__(self, pid: str):
        self.id = pid


class _FakeArtifact:
    def __init__(self, profile_id: str, pdf_bytes: bytes):
        self.profile_id = profile_id
        self.pdf_bytes = pdf_bytes


class _FakePackage:
    def __init__(self, *, ok=True, artifacts=None, merged_pdf=b"%PDF-1.4 fake", errors=None, warnings=None):
        self.ok = ok
        self.artifacts = artifacts or []
        self.merged_pdf = merged_pdf
        self.errors = errors or []
        self.warnings = warnings or []


# ── 1. Generated stage strict completeness ─────────────────────────────────

class TestGeneratedStrictCompleteness:
    def test_generated_blocks_when_artifact_missing_for_required_profile(self):
        """If finalize() returns artifacts for only 1 of 2 profiles, the
        orchestrator must NOT silently transition to GENERATED."""
        quote = _qa_passed_quote()
        profiles = [_FakeProfile("704b_reytech_standard"), _FakeProfile("quote_reytech_letterhead")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        # Package has only 1 of 2 expected artifacts.
        pkg = _FakePackage(artifacts=[_FakeArtifact("704b_reytech_standard", b"%PDF-1.4 four-zero-four-b")])

        with patch("src.core.quote_engine.finalize", return_value=pkg):
            attempt = orch._try_advance(quote, "generated", QuoteRequest(target_stage="generated"), profiles, result)

        assert attempt.outcome == "error", f"expected error, got {attempt.outcome}: {attempt.reasons}"
        assert any("missing or empty artifacts" in r for r in attempt.reasons), attempt.reasons
        assert any("quote_reytech_letterhead" in r for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.QA_PASS  # never transitioned

    def test_generated_blocks_when_finalize_reports_errors(self):
        quote = _qa_passed_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        pkg = _FakePackage(
            ok=False,
            artifacts=[_FakeArtifact("704b_reytech_standard", b"%PDF-1.4 fake")],
            errors=["704b_reytech_standard: blank PDF missing"],
        )
        with patch("src.core.quote_engine.finalize", return_value=pkg):
            attempt = orch._try_advance(quote, "generated", QuoteRequest(target_stage="generated"), profiles, result)

        assert attempt.outcome == "error"
        assert any("blank PDF missing" in r for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.QA_PASS

    def test_generated_blocks_when_merged_pdf_empty(self):
        quote = _qa_passed_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        pkg = _FakePackage(
            artifacts=[_FakeArtifact("704b_reytech_standard", b"%PDF-1.4 fake")],
            merged_pdf=b"",  # nothing to send
        )
        with patch("src.core.quote_engine.finalize", return_value=pkg):
            attempt = orch._try_advance(quote, "generated", QuoteRequest(target_stage="generated"), profiles, result)

        assert attempt.outcome == "error"
        assert any("merged_pdf is empty" in r for r in attempt.reasons), attempt.reasons

    def test_generated_advances_when_all_artifacts_present(self):
        quote = _qa_passed_quote()
        profiles = [_FakeProfile("704b_reytech_standard"), _FakeProfile("quote_reytech_letterhead")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        pkg = _FakePackage(artifacts=[
            _FakeArtifact("704b_reytech_standard", b"%PDF-1.4 four-zero-four-b"),
            _FakeArtifact("quote_reytech_letterhead", b"%PDF-1.4 letterhead"),
        ])
        with patch("src.core.quote_engine.finalize", return_value=pkg):
            attempt = orch._try_advance(quote, "generated", QuoteRequest(target_stage="generated"), profiles, result)

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.GENERATED
        assert result.package is pkg


# ── 2. Sent stage real send ────────────────────────────────────────────────

class TestSentStageRealSend:
    def _generated_quote_with_package(self):
        quote = _qa_passed_quote()
        quote.transition(QuoteStatus.GENERATED)
        result = OrchestratorResult(quote=quote)
        result.package = _FakePackage(merged_pdf=b"%PDF-1.4 merged-fake")
        return quote, result

    def test_sent_blocks_when_recipient_empty(self):
        quote, result = self._generated_quote_with_package()
        quote.buyer.requestor_email = ""
        orch = QuoteOrchestrator(persist_audit=False)
        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@x.com", "GMAIL_PASSWORD": "x"}):
            attempt = orch._try_advance(quote, "sent", QuoteRequest(target_stage="sent"), [], result)
        assert attempt.outcome == "error"
        assert any("requestor_email is empty" in r for r in attempt.reasons)
        assert quote.status == QuoteStatus.GENERATED

    def test_sent_blocks_when_no_merged_package(self):
        quote, result = self._generated_quote_with_package()
        result.package = None
        orch = QuoteOrchestrator(persist_audit=False)
        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@x.com", "GMAIL_PASSWORD": "x"}):
            attempt = orch._try_advance(quote, "sent", QuoteRequest(target_stage="sent"), [], result)
        assert attempt.outcome == "error"
        assert any("no merged package" in r for r in attempt.reasons)

    def test_sent_blocks_when_transport_unconfigured(self):
        quote, result = self._generated_quote_with_package()
        orch = QuoteOrchestrator(persist_audit=False)
        # Drop both GMAIL_* vars
        with patch.dict(os.environ, {}, clear=True):
            attempt = orch._try_advance(quote, "sent", QuoteRequest(target_stage="sent"), [], result)
        assert attempt.outcome == "error"
        assert any("GMAIL_ADDRESS/GMAIL_PASSWORD not configured" in r for r in attempt.reasons)

    def test_sent_calls_email_sender_with_attachment(self):
        quote, result = self._generated_quote_with_package()
        orch = QuoteOrchestrator(persist_audit=False)

        sent_drafts: list = []

        class FakeSender:
            def __init__(self, _config): pass
            def send(self, draft):
                sent_drafts.append(draft)

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "sales@reytechinc.com", "GMAIL_PASSWORD": "x"}):
            with patch("src.agents.email_poller.EmailSender", FakeSender):
                attempt = orch._try_advance(quote, "sent", QuoteRequest(target_stage="sent"), [], result)

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.SENT
        assert len(sent_drafts) == 1
        draft = sent_drafts[0]
        assert draft["to"] == "buyer@cchcs.ca.gov"
        assert "R26Q0042" in draft["subject"]
        assert len(draft["attachments"]) == 1
        # The attachment must be a real file with the merged PDF bytes.
        with open(draft["attachments"][0], "rb") as f:
            assert f.read() == b"%PDF-1.4 merged-fake"

    def test_sent_propagates_smtp_failure_as_error(self):
        quote, result = self._generated_quote_with_package()
        orch = QuoteOrchestrator(persist_audit=False)

        class BoomSender:
            def __init__(self, _config): pass
            def send(self, draft):
                raise RuntimeError("smtp 535 auth failure")

        with patch.dict(os.environ, {"GMAIL_ADDRESS": "x@x.com", "GMAIL_PASSWORD": "x"}):
            with patch("src.agents.email_poller.EmailSender", BoomSender):
                attempt = orch._try_advance(quote, "sent", QuoteRequest(target_stage="sent"), [], result)

        assert attempt.outcome == "error"
        assert any("SMTP send failed" in r and "535" in r for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.GENERATED


# ── 3. Compliance validator: 0-byte forms are not "filled" ─────────────────

class TestComplianceBytesGate:
    def test_zero_byte_form_does_not_count_as_filled(self):
        from src.agents.compliance_validator import _check_required_forms

        class _Hdr:
            agency_key = "cchcs"
            solicitation_number = "R26Q0042"

        class _Q:
            header = _Hdr()

        # Per-form report claims filled+qa_passed but bytes=0.
        per_form = [
            {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True, "bytes": 0},
        ]
        blockers = _check_required_forms(_Q(), per_form)
        # CCHCS requires 704b — the 0-byte profile must not satisfy it.
        assert any("704b" in b for b in blockers), blockers

    def test_nonzero_bytes_form_counts_as_filled(self):
        from src.agents.compliance_validator import _check_required_forms

        class _Hdr:
            agency_key = "cchcs"
            solicitation_number = "R26Q0042"

        class _Q:
            header = _Hdr()

        per_form = [
            {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True, "bytes": 12345},
        ]
        blockers = _check_required_forms(_Q(), per_form)
        # 704b satisfied; only other CCHCS-required forms (if any) might block.
        assert not any("704b" in b for b in blockers), blockers
