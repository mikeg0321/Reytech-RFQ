"""Wire-in test for the classifier_v2 path inside process_rfq_email.

Phase 5 of the PC↔RFQ unification: the email poller now dispatches to
`process_buyer_request` when `ingest.classifier_v2_enabled` is on.
These tests pin the flag-gated behavior so:
  - Flag OFF: process_rfq_email uses the legacy parallel branches.
  - Flag ON: process_rfq_email short-circuits to v2 and returns the
    saved record (RFQ) or None (PC), matching the legacy contract.
  - V2 failure falls through to legacy (no silent email loss).
"""
import os
from unittest.mock import patch

import pytest


FIX_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "unified_ingest",
)
CCHCS_PACKET = os.path.join(FIX_DIR, "cchcs_packet_preq.pdf")
PC_DOCX_FOOD = os.path.join(FIX_DIR, "pc_docx_food.docx")


@pytest.mark.skipif(
    not os.path.exists(CCHCS_PACKET),
    reason="CCHCS fixture missing",
)
class TestEmailPollerClassifierWire:
    def _build_email(self, path: str, subject: str, sender: str) -> dict:
        return {
            "email_uid": f"test-{os.path.basename(path)}",
            "subject": subject,
            "sender": sender,
            "sender_email": sender,
            "body_text": "",
            "attachments": [{"path": path, "filename": os.path.basename(path)}],
        }

    def test_flag_off_uses_legacy_path(self, temp_data_dir):
        """When classify_enabled()==False, v2 block is skipped entirely.
        Confirm by patching process_buyer_request and asserting it was
        never called."""
        from src.api import dashboard
        email = self._build_email(
            CCHCS_PACKET, "PREQ10843276 Quote", "buyer@cdcr.ca.gov"
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=False
        ), patch(
            "src.core.ingest_pipeline.process_buyer_request"
        ) as mock_v2:
            try:
                dashboard.process_rfq_email(email)
            except Exception:
                # Legacy path may fail in test env — we only care about
                # whether v2 was called.
                pass
            mock_v2.assert_not_called()

    def test_flag_on_dispatches_to_v2_for_cchcs(self, temp_data_dir):
        """When flag is on and files are present, v2 is called and its
        result short-circuits the legacy branches."""
        from src.api import dashboard
        email = self._build_email(
            CCHCS_PACKET, "PREQ10843276 Quote", "buyer@cdcr.ca.gov"
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ):
            result = dashboard.process_rfq_email(email)
        # CCHCS packet → RFQ record → returned dict (not None)
        assert result is not None
        assert result.get("_classification", {}).get("shape") == "cchcs_packet"

    def test_flag_on_pc_docx_returns_none(self, temp_data_dir):
        """PC DOCX classifies as quote-only → v2 creates a PC record.
        process_rfq_email returns None for PC creations, matching the
        legacy 704→PC early-exit contract."""
        if not os.path.exists(PC_DOCX_FOOD):
            pytest.skip("PC DOCX fixture missing")
        from src.api import dashboard
        email = self._build_email(
            PC_DOCX_FOOD, "Price Check Food", "buyer@cdcr.ca.gov"
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ):
            result = dashboard.process_rfq_email(email)
        assert result is None
        # A PC should now exist
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        assert any(
            isinstance(pc, dict)
            and pc.get("_classification", {}).get("shape", "").startswith("pc_704")
            for pc in pcs.values()
        )

    def test_v2_failure_falls_through_to_legacy(self, temp_data_dir):
        """When process_buyer_request raises, the wire-in must not
        crash process_rfq_email — the legacy branches run instead.
        Guards against silent email loss if v2 has a bug."""
        from src.api import dashboard
        email = self._build_email(
            CCHCS_PACKET, "PREQ10843276 Quote", "buyer@cdcr.ca.gov"
        )
        with patch(
            "src.core.request_classifier.classify_enabled", return_value=True
        ), patch(
            "src.core.ingest_pipeline.process_buyer_request",
            side_effect=RuntimeError("simulated v2 crash"),
        ):
            # The v2 exception must be caught; if legacy path then
            # fails for unrelated reasons on the minimal fixture that
            # is acceptable — we only care the v2 RuntimeError did
            # not escape the wire-in.
            try:
                dashboard.process_rfq_email(email)
            except RuntimeError as e:
                if "simulated v2 crash" in str(e):
                    pytest.fail(
                        "v2 exception escaped the wire-in try/except"
                    )
            except Exception:
                pass
