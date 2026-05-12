"""PR-A Step 9 (2026-05-11) — End-to-end email-contract substrate.

One golden-path test per scenario, exercising the full Steps 1-8
substrate as a single user-visible workflow:

  Scenario A: Buyer-RFQ via plain email
    - Wrapper email never fires (sender doesn't match Proofpoint patterns)
    - Vision-primary parses the PDF
    - All attachments persist to rfq_files
    - needs_review fires when Vision and base parser disagree
    - Record's needs_review banner survives reload

  Scenario B: DSH SecureMessage with auto-pull DISABLED (default state)
    - Wrapper email detected
    - Auto-pull skipped (flag off / no creds)
    - Record persists with needs_manual_pull=True
    - Operator gets the portal URL one-click handoff

  Scenario C: DSH SecureMessage with auto-pull SUCCEEDS
    - Wrapper email detected
    - Auto-pull returns the real RFQ PDF
    - Re-classify against the downloaded file
    - Vision-primary parses; record reflects the REAL shape

These tests are the "would this have caught Mike's bottleneck before
production?" backstop. Per-step tests in the other files cover the
component contracts; this file pins that the contracts compose.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest


# ─── Scenario A: plain buyer RFQ, Vision-primary, attachment persist ────


class TestPlainBuyerRFQ:

    def test_full_path_persists_attachments_and_fires_needs_review(
        self, tmp_path, temp_data_dir,
    ):
        from src.core.ingest_pipeline import process_buyer_request
        from src.core.request_classifier import RequestClassification
        from src.api.dashboard import (
            load_rfqs, list_rfq_files, _init_rfq_files_table,
        )
        # Re-init the rfq_files table against the temp DB. The dashboard
        # module's auto-init at import time ran against the wrong path
        # before the fixture pointed env at the temp data dir.
        _init_rfq_files_table()

        f1 = tmp_path / "calvet_rfq.pdf"; f1.write_bytes(b"%PDF-1.4 RFQ")
        f2 = tmp_path / "calvet_spec.pdf"; f2.write_bytes(b"%PDF-1.4 SPEC")

        cls = RequestClassification(
            shape="generic_rfq_pdf",
            agency="calvet",
            confidence=0.9,
            reasons=[],
            primary_file="calvet_rfq.pdf",
            solicitation_number="",
            institution="CalVet Yountville",
            is_quote_only=False,
            required_forms=[],
        )

        # Vision finds 15 items, base parser finds 5. Disagreement >
        # threshold → needs_review fires.
        base_items = [{"description": f"b{i}"} for i in range(5)]
        vision_items = [{"description": f"v{i}"} for i in range(15)]

        with patch("src.core.request_classifier.classify_request",
                   return_value=cls), \
             patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": base_items, "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=vision_items):
            result = process_buyer_request(
                files=[str(f1), str(f2)],
                email_subject="CalVet RFQ — Medical Supplies",
                email_body="Please quote attached.",
                email_sender="buyer@calvet.ca.gov",
            )

        # Substrate end-state.
        assert result.ok is True
        assert result.record_type == "rfq"
        # Vision-primary: 15 items wins, not 5.
        assert result.items_parsed == 15
        # Disagreement above threshold → operator review banner.
        assert result.needs_review is True
        assert any(
            w.get("kind") == "count_disagreement"
            for w in result.ingest_warnings
        )
        # No SecureMessage handoff fields.
        assert result.needs_manual_pull is False
        assert result.proofpoint_portal_url == ""

        # Record persistence.
        rfq = load_rfqs().get(result.record_id)
        assert rfq is not None
        assert len(rfq.get("line_items", [])) == 15
        assert rfq.get("needs_review") is True
        # Status flips into triage queue because needs_review.
        assert rfq.get("status") == "needs_review"

        # Both attachments persisted to rfq_files (not just the primary).
        files = list_rfq_files(result.record_id, category="buyer_attachment")
        names = {f["filename"] for f in files}
        assert "calvet_rfq.pdf" in names
        assert "calvet_spec.pdf" in names


# ─── Scenario B: DSH SecureMessage, auto-pull disabled ──────────────────


class TestSecureMessageManualPull:

    def test_full_path_manual_pull_with_portal_url(self, temp_data_dir):
        """Mike's DSH bottleneck before this PR: wrapper email creates an
        empty record with no clue why. Post-PR: classifier IDs the
        wrapper, handler skips auto-pull (default state), record
        persists with the portal URL and a clear status."""
        from src.core.ingest_pipeline import process_buyer_request
        from src.api.dashboard import load_rfqs

        with patch("src.agents.proofpoint_pull.is_available",
                   return_value=False):
            result = process_buyer_request(
                files=[],
                email_subject="*** Secure Mail *** Quote Request - DSH Atascadero",
                email_body=(
                    "<html><body>"
                    "<p>You have received a secure message from the "
                    "Department of State Hospitals.</p>"
                    "<p><a href='https://securereader.proofpoint.com/?u=abc123def456'>"
                    "Read the Message</a></p>"
                    "<p>Click here to view the encrypted message via "
                    "Proofpoint Encryption.</p>"
                    "</body></html>"
                ),
                email_sender="securemail@dsh.ca.gov",
            )

        assert result.ok is True
        # Classifier IDed the wrapper.
        assert result.classification["shape"] == "proofpoint_securemessage"
        assert result.classification["agency"] == "dsh"
        # Handler routed to manual-pull fallback.
        assert result.needs_manual_pull is True
        assert "securereader.proofpoint.com" in result.proofpoint_portal_url

        rfq = load_rfqs().get(result.record_id)
        assert rfq is not None
        assert rfq.get("needs_manual_pull") is True
        assert rfq.get("status") == "needs_manual_pull"
        # The portal URL persists so the operator's banner can render it.
        assert "securereader.proofpoint.com" in rfq.get("proofpoint_portal_url", "")


# ─── Scenario C: DSH SecureMessage with auto-pull success ───────────────


class TestSecureMessageAutoPullSuccess:

    def test_full_path_auto_pull_reclassify_vision_parse(
        self, tmp_path, temp_data_dir,
    ):
        """The whole point of PR-A end-to-end: a DSH SecureMessage
        arrives, the auto-pull module downloads the real RFQ PDF, the
        request is re-classified against the downloaded file, and
        Vision-primary parses it. By the time `process_buyer_request`
        returns, the operator has a fully-populated record with no
        intermediate triage step."""
        from src.core.ingest_pipeline import process_buyer_request
        from src.core.request_classifier import RequestClassification
        from src.api.dashboard import load_rfqs

        downloaded = tmp_path / "AMS_704_DSH_Atascadero.pdf"
        downloaded.write_bytes(b"%PDF-1.4 RFQ")

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
            solicitation_number="PC-DSH-789",
            institution="Atascadero",
            is_quote_only=True,
            required_forms=[],
        )

        # Vision finds the same number as the base parser — within
        # tolerance, no needs_review.
        items_payload = [{"description": f"i{i}"} for i in range(6)]

        with patch("src.core.request_classifier.classify_request",
                   side_effect=[wrapper_cls, real_cls]), \
             patch("src.agents.proofpoint_pull.is_available",
                   return_value=True), \
             patch("src.agents.proofpoint_pull.pull_via_url",
                   return_value=[str(downloaded)]), \
             patch("src.forms.price_check.parse_ams704",
                   return_value={"line_items": items_payload,
                                 "header": {"requestor": "Atascadero Buyer"}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=items_payload):
            result = process_buyer_request(
                files=[],
                email_subject="*** Secure Mail *** Quote Request",
                email_body=(
                    "Click https://securereader.proofpoint.com/?u=xyz "
                    "to read via Proofpoint Encryption."
                ),
                email_sender="securemail@dsh.ca.gov",
            )

        assert result.ok is True
        # No manual-pull fallback — auto-pull succeeded.
        assert result.needs_manual_pull is False
        # Re-classified against the downloaded file.
        assert result.classification["shape"] == "pc_704_pdf_fillable"
        assert result.record_type == "pc"  # 704 is quote-only
        # Vision-primary parsed the real PDF.
        assert result.items_parsed == 6
        # Vision and base agreed → no review flag.
        assert result.needs_review is False

        # Record persisted as a real PC with items, NOT a wrapper stub.
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(result.record_id)
        assert pc is not None
        assert len(pc.get("items", [])) == 6
        assert pc.get("status") == "parsed"
        assert pc.get("needs_manual_pull") is False
        assert pc.get("needs_review") is False
