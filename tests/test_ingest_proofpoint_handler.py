"""PR-A Step 8 (2026-05-11) — Proofpoint SecureMessage handler in
ingest_pipeline.process_buyer_request.

End-to-end behavior pinned:
  - When classifier returns SHAPE_PROOFPOINT_SECUREMESSAGE and
    `proofpoint_pull.is_available()` is True, the auto-pull runs.
    Downloaded files become the new attachment list and the request
    is RE-CLASSIFIED so the rest of the pipeline sees the real RFQ
    shape (cchcs / ams_704 / generic).
  - When auto-pull is unavailable (no creds / flag off / no
    playwright), the record persists with `needs_manual_pull=True`,
    status="needs_manual_pull", and `proofpoint_portal_url` carrying
    the extracted link so the operator can click through.
  - When auto-pull is available but returns [] (login failed,
    no attachments, timeout), the same manual-pull fallback fires.
  - The classifier override on shape=proofpoint_securemessage doesn't
    affect downstream classification when re-classify succeeds — the
    record's stored classification reflects the REAL shape, not the
    wrapper shape.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest


# ─── IngestResult shape additions ────────────────────────────────────────


class TestIngestResultProofpointFields:

    def test_defaults_false_empty(self):
        from src.core.ingest_pipeline import IngestResult
        r = IngestResult()
        assert r.needs_manual_pull is False
        assert r.proofpoint_portal_url == ""

    def test_to_dict_includes_proofpoint_fields(self):
        from src.core.ingest_pipeline import IngestResult
        r = IngestResult(
            needs_manual_pull=True,
            proofpoint_portal_url="https://securereader.proofpoint.com/abc",
        )
        d = r.to_dict()
        assert d["needs_manual_pull"] is True
        assert d["proofpoint_portal_url"].startswith("https://securereader")


# ─── Manual-pull fallback path ───────────────────────────────────────────


class TestManualPullFallback:

    def _wrapper_email_kwargs(self):
        return dict(
            email_subject="*** Secure Mail *** Quote Request",
            email_body=(
                "You have received a secure message. "
                "Click here: https://securereader.proofpoint.com/?u=abc123 "
                "to read via Proofpoint Encryption."
            ),
            email_sender="securemail@dsh.ca.gov",
        )

    def test_no_creds_falls_back_to_manual(self, temp_data_dir):
        """Default state: no creds, flag off → SecureMessage handler
        marks record with needs_manual_pull and portal URL persists."""
        from src.core.ingest_pipeline import process_buyer_request
        with patch("src.agents.proofpoint_pull.is_available",
                   return_value=False):
            result = process_buyer_request(
                files=[],
                **self._wrapper_email_kwargs(),
            )
        assert result.ok is True
        assert result.needs_manual_pull is True
        assert "securereader.proofpoint.com" in result.proofpoint_portal_url

        from src.api.dashboard import load_rfqs
        rfqs = load_rfqs()
        rfq = rfqs.get(result.record_id)
        assert rfq is not None
        assert rfq.get("needs_manual_pull") is True
        assert "securereader.proofpoint.com" in rfq.get("proofpoint_portal_url", "")
        assert rfq.get("status") == "needs_manual_pull"

    def test_auto_pull_returns_empty_falls_back_to_manual(self, temp_data_dir):
        """Auto-pull was available but came back empty (timeout, login
        failure, no attachments). Same manual-pull fallback."""
        from src.core.ingest_pipeline import process_buyer_request
        with patch("src.agents.proofpoint_pull.is_available",
                   return_value=True), \
             patch("src.agents.proofpoint_pull.pull_via_url",
                   return_value=[]):
            result = process_buyer_request(
                files=[],
                **self._wrapper_email_kwargs(),
            )
        assert result.needs_manual_pull is True
        from src.api.dashboard import load_rfqs
        rfq = load_rfqs().get(result.record_id)
        assert rfq.get("needs_manual_pull") is True
        assert rfq.get("status") == "needs_manual_pull"

    def test_no_portal_url_extractable_still_marks_manual(self, temp_data_dir):
        """Wrapper email matched 2-of-4 signals but body has no portal
        URL — record still routes to manual triage."""
        from src.core.ingest_pipeline import process_buyer_request
        with patch("src.agents.proofpoint_pull.is_available",
                   return_value=False):
            result = process_buyer_request(
                files=[],
                email_subject="*** Secure Mail ***",
                email_body="You have received a secure message via Proofpoint Encryption.",
                email_sender="securemail@dsh.ca.gov",
            )
        assert result.needs_manual_pull is True
        assert result.proofpoint_portal_url == ""


# ─── Auto-pull success → re-classify path ────────────────────────────────


class TestAutoPullSuccess:

    def test_auto_pull_success_reclassifies_and_parses(
        self, tmp_path, temp_data_dir,
    ):
        """Auto-pull returns a real RFQ PDF; the handler re-classifies
        the request against the downloaded file so the rest of the
        pipeline sees the actual shape (NOT proofpoint_securemessage).
        """
        from src.core.ingest_pipeline import process_buyer_request
        from src.core.request_classifier import RequestClassification

        downloaded = tmp_path / "AMS_704_Atascadero.pdf"
        downloaded.write_bytes(b"%PDF-1.4 RFQ")

        # Sequenced classify calls: first the wrapper, then the real RFQ.
        wrapper_cls = RequestClassification(
            shape="proofpoint_securemessage",
            agency="dsh",
            confidence=0.9,
            reasons=[],
            primary_file="",
            primary_file_type="proofpoint_wrapper",
            solicitation_number="",
            institution="",
            is_quote_only=False,
            required_forms=[],
        )
        real_cls = RequestClassification(
            shape="pc_704_pdf_fillable",
            agency="dsh",
            confidence=0.95,
            reasons=[],
            primary_file=downloaded.name,
            primary_file_type="pdf",
            solicitation_number="PC-789",
            institution="Atascadero",
            is_quote_only=True,
            required_forms=[],
        )
        call_seq = [wrapper_cls, real_cls]

        with patch("src.core.request_classifier.classify_request",
                   side_effect=call_seq), \
             patch("src.agents.proofpoint_pull.is_available",
                   return_value=True), \
             patch("src.agents.proofpoint_pull.pull_via_url",
                   return_value=[str(downloaded)]), \
             patch("src.forms.price_check.parse_ams704",
                   return_value={"line_items": [{"description": "i1"}],
                                 "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": "v1"}]):
            result = process_buyer_request(
                files=[],
                email_subject="*** Secure Mail *** Quote Request",
                email_body=(
                    "Click here: https://securereader.proofpoint.com/?u=xyz "
                    "to read via Proofpoint Encryption."
                ),
                email_sender="securemail@dsh.ca.gov",
            )
        assert result.ok is True
        assert result.needs_manual_pull is False
        # Re-classified — record_type now reflects the real shape.
        assert result.record_type == "pc"  # is_quote_only=True for 704
        assert result.classification["shape"] == "pc_704_pdf_fillable"
        # Items parsed from the real PDF via Vision-primary.
        assert result.items_parsed >= 1
