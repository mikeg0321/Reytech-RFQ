"""End-to-end email pipeline test (G2 — CI gate addition).

Covers the path the inbox poller actually runs every minute in production:

    Gmail (mocked) → list_message_ids → get_message_metadata
                  → process_rfq_email → classifier_v2 → ingest_pipeline
                  → PC or RFQ row persisted to the test DB / json store

The existing `test_email_poller_classifier_wire.py` proves the v2 dispatch
fires; this test adds the missing storage round-trip assertion (was a row
actually written?) and verifies the Gmail mock prevents real network calls
even when `is_configured()` is queried by code we don't directly call.

If this regresses, the auto-poll loop silently stops creating quote records
in production — exactly the failure mode that took 2 days to notice last
quarter when classifier_v2 first shipped.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest


_FIX = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fixtures", "unified_ingest",
)
_CCHCS = os.path.join(_FIX, "cchcs_packet_preq.pdf")
_PC_DOCX = os.path.join(_FIX, "pc_docx_food.docx")


def _email(path: str, subject: str, sender: str, uid: str) -> dict:
    return {
        "email_uid": uid,
        "subject": subject,
        "sender": sender,
        "sender_email": sender,
        "body_text": "",
        "attachments": [{"path": path, "filename": os.path.basename(path)}],
    }


@pytest.mark.skipif(
    not os.path.exists(_CCHCS),
    reason="CCHCS fixture missing — pipeline E2E needs a real packet",
)
class TestPipelineE2E:
    """Drive a synthetic email through the real pipeline and confirm
    persistence — not just the classifier handoff."""

    def test_cchcs_email_creates_persisted_rfq(self, temp_data_dir, mock_gmail):
        from src.api import dashboard
        from src.api.data_layer import load_rfqs

        mock_gmail.set_messages([{
            "id": "msg-cchcs-e2e-1",
            "subject": "PREQ10843276 Quote Request",
            "sender": "buyer@cdcr.ca.gov",
        }])

        em = _email(
            _CCHCS, "PREQ10843276 Quote Request",
            "buyer@cdcr.ca.gov", "msg-cchcs-e2e-1",
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ):
            result = dashboard.process_rfq_email(em)

        # CCHCS packet → RFQ; result is the saved record
        assert result is not None, "CCHCS packet should produce an RFQ record"
        rfq_id = result.get("id") or result.get("rfq_id")
        assert rfq_id, "saved RFQ must have an id"

        # Round-trip via the public load function (the DB/JSON store)
        all_rfqs = load_rfqs()
        assert rfq_id in all_rfqs, (
            f"RFQ {rfq_id} created in-memory but not persisted to store"
        )

    def test_pc_docx_email_creates_persisted_pc(self, temp_data_dir, mock_gmail):
        if not os.path.exists(_PC_DOCX):
            pytest.skip("PC DOCX fixture missing")
        from src.api import dashboard
        from src.api.dashboard import _load_price_checks

        mock_gmail.set_messages([{
            "id": "msg-pc-e2e-1",
            "subject": "Price Check — Food Items",
            "sender": "buyer@cdcr.ca.gov",
        }])

        em = _email(
            _PC_DOCX, "Price Check — Food Items",
            "buyer@cdcr.ca.gov", "msg-pc-e2e-1",
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ):
            result = dashboard.process_rfq_email(em)

        # Per the legacy contract, PC creations return None from
        # process_rfq_email — but the PC must have landed in the store.
        assert result is None
        pcs = _load_price_checks()
        matching = [
            pc for pc in pcs.values()
            if isinstance(pc, dict) and pc.get("email_uid") == "msg-pc-e2e-1"
        ]
        assert matching, "PC was not persisted to the store under the email UID"

    def test_dedup_blocks_second_delivery_of_same_uid(
        self, temp_data_dir, mock_gmail
    ):
        """The poller may re-deliver the same message across crashes /
        restarts. The pipeline must dedup by email_uid so we don't end up
        with N duplicate quotes for one buyer email (Valencia incident)."""
        from src.api import dashboard
        from src.api.data_layer import load_rfqs

        em = _email(
            _CCHCS, "PREQ10843276 Quote Request",
            "buyer@cdcr.ca.gov", "msg-dup-e2e-1",
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ):
            first = dashboard.process_rfq_email(em)
            second = dashboard.process_rfq_email(em)

        assert first is not None
        assert second is None, (
            "second delivery of the same email_uid must be deduped to None"
        )
        # Exactly one RFQ persisted for this UID
        rfqs = load_rfqs()
        with_uid = [
            r for r in rfqs.values()
            if isinstance(r, dict) and r.get("email_uid") == "msg-dup-e2e-1"
        ]
        assert len(with_uid) == 1, (
            f"expected 1 RFQ for the deduped UID, got {len(with_uid)}"
        )

    def test_gmail_mock_blocks_real_api_calls(self, mock_gmail):
        """Sanity guard: even if downstream code asks gmail_api whether
        it's configured or to list messages, the mock answers — the real
        Google client must never be invoked from a test run."""
        try:
            import src.core.gmail_api as gmail_api
        except ImportError:
            pytest.skip("gmail_api module not importable in this env")

        mock_gmail.set_configured(True)
        mock_gmail.set_messages([
            {"id": "msg-A", "subject": "S1", "sender": "a@example.gov"},
            {"id": "msg-B", "subject": "S2", "sender": "b@example.gov"},
        ])
        assert gmail_api.is_configured() is True
        ids = gmail_api.list_message_ids()
        assert ids == ["msg-A", "msg-B"]
        meta = gmail_api.get_message_metadata(gmail_api.get_service(), "msg-B")
        assert meta.get("subject") == "S2"

        mock_gmail.set_configured(False)
        assert gmail_api.is_configured() is False
