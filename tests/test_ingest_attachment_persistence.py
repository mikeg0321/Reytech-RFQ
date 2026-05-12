"""PR-A Step 4 (2026-05-11) — every buyer attachment persists to rfq_files.

Background: `scripts/reingest_rfqs_through_vision.py` discovered that
buyer attachments were NOT being saved to the `rfq_files` table by the
new `process_buyer_request` ingest path. The legacy email_poller code
in `dashboard.py` did persist the primary file, but only the primary —
sibling attachments were silently dropped, and the classifier_v2
pipeline skipped persistence entirely.

Consequence: when the re-ingest script wanted to re-run Vision on a
historical RFQ to recover dropped items, it had to fall back to Gmail
re-fetch (slow, brittle, OAuth-scoped). PR #910 added the Gmail
fallback as a workaround; this step closes the underlying gap.

Pin behavior:
  - `_create_record` persists every entry in `all_paths` to rfq_files
    with category="buyer_attachment"
  - Idempotent on re-ingest (no duplicate rows for same record_id +
    filename + category — save_rfq_file dedupes)
  - PC and RFQ both honored
  - Missing/empty files skipped silently
  - Failure of one attachment does not block ingest
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest


# ─── _persist_all_attachments helper, called from both branches ──────────


class TestPersistAllAttachments:

    def test_persists_every_attachment(self, tmp_path):
        """Three files → three save_rfq_file calls with category set."""
        from src.core.ingest_pipeline import _persist_all_attachments
        f1 = tmp_path / "a.pdf"; f1.write_bytes(b"%PDF-1.4 A")
        f2 = tmp_path / "b.pdf"; f2.write_bytes(b"%PDF-1.4 B")
        f3 = tmp_path / "c.pdf"; f3.write_bytes(b"%PDF-1.4 C")
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            count = _persist_all_attachments(
                "rfq_abc", "rfq", [str(f1), str(f2), str(f3)],
            )
        assert count == 3
        assert mock_save.call_count == 3
        for call in mock_save.call_args_list:
            args, kwargs = call
            # save_rfq_file(rfq_id, filename, file_type, data,
            #               category=..., uploaded_by=..., gmail_message_id=...)
            assert args[0] == "rfq_abc"
            assert kwargs.get("category") == "buyer_attachment"
            assert kwargs.get("uploaded_by") == "ingest_rfq"

    def test_pc_uploaded_by_label(self, tmp_path):
        from src.core.ingest_pipeline import _persist_all_attachments
        f = tmp_path / "x.pdf"; f.write_bytes(b"%PDF-1.4")
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            _persist_all_attachments("pc_abc", "pc", [str(f)])
        _args, kwargs = mock_save.call_args
        assert kwargs.get("uploaded_by") == "ingest_pc"

    def test_skips_missing_file(self, tmp_path):
        from src.core.ingest_pipeline import _persist_all_attachments
        real = tmp_path / "real.pdf"
        real.write_bytes(b"%PDF-1.4")
        missing = tmp_path / "missing.pdf"  # never written
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            count = _persist_all_attachments(
                "rfq_abc", "rfq", [str(real), str(missing)],
            )
        assert count == 1
        assert mock_save.call_count == 1

    def test_skips_empty_paths_list(self):
        from src.core.ingest_pipeline import _persist_all_attachments
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            count = _persist_all_attachments("rfq_abc", "rfq", [])
        assert count == 0
        mock_save.assert_not_called()

    def test_skips_none_paths(self):
        from src.core.ingest_pipeline import _persist_all_attachments
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            count = _persist_all_attachments("rfq_abc", "rfq", None)
        assert count == 0
        mock_save.assert_not_called()

    def test_one_failure_does_not_block_others(self, tmp_path):
        """If save_rfq_file errors on file #1, file #2 still persists."""
        from src.core.ingest_pipeline import _persist_all_attachments
        f1 = tmp_path / "a.pdf"; f1.write_bytes(b"%PDF-1.4 A")
        f2 = tmp_path / "b.pdf"; f2.write_bytes(b"%PDF-1.4 B")
        call_count = {"n": 0}

        def _flaky_save(*a, **k):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("disk full simulated")
            return "rf_ok"

        with patch("src.api.dashboard.save_rfq_file", side_effect=_flaky_save):
            count = _persist_all_attachments(
                "rfq_abc", "rfq", [str(f1), str(f2)],
            )
        assert count == 1  # only the second succeeded

    def test_passes_gmail_message_id_through(self, tmp_path):
        from src.core.ingest_pipeline import _persist_all_attachments
        f = tmp_path / "x.pdf"; f.write_bytes(b"%PDF-1.4")
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            _persist_all_attachments(
                "rfq_abc", "rfq", [str(f)],
                gmail_message_id="MSG_18a0bc1de2",
            )
        _args, kwargs = mock_save.call_args
        assert kwargs.get("gmail_message_id") == "MSG_18a0bc1de2"

    def test_guesses_mime_type(self, tmp_path):
        """PDFs → application/pdf; unknown → application/octet-stream."""
        from src.core.ingest_pipeline import _persist_all_attachments
        f_pdf = tmp_path / "doc.pdf"; f_pdf.write_bytes(b"%PDF")
        f_xlsx = tmp_path / "sheet.xlsx"; f_xlsx.write_bytes(b"PK\x03\x04")
        with patch("src.api.dashboard.save_rfq_file") as mock_save:
            _persist_all_attachments(
                "rfq_abc", "rfq", [str(f_pdf), str(f_xlsx)],
            )
        # Check file_type (3rd positional arg).
        types = [c.args[2] for c in mock_save.call_args_list]
        assert "application/pdf" in types
        # xlsx mime varies by platform; just confirm it's not empty.
        assert all(t for t in types)


# ─── End-to-end: _create_record calls _persist_all_attachments ───────────


class TestCreateRecordPersistsAttachments:

    def _classification(self, shape="generic_rfq_pdf"):
        from src.core.request_classifier import RequestClassification
        return RequestClassification(
            shape=shape,
            agency="calvet",
            confidence=0.9,
            reasons=[],
            primary_file="rfq.pdf",
            solicitation_number="",
            institution="CalVet Yountville",
            is_quote_only=False,
            required_forms=[],
        )

    def test_create_rfq_persists_all_paths(self, tmp_path, temp_data_dir):
        """All three buyer attachments land in rfq_files when an RFQ is
        created, even though only one was the primary."""
        from src.core.ingest_pipeline import process_buyer_request
        f1 = tmp_path / "rfq.pdf"; f1.write_bytes(b"%PDF-1.4 RFQ")
        f2 = tmp_path / "spec.pdf"; f2.write_bytes(b"%PDF-1.4 SPEC")
        f3 = tmp_path / "drawing.pdf"; f3.write_bytes(b"%PDF-1.4 DWG")

        with patch("src.core.ingest_pipeline._persist_all_attachments") as mock_persist, \
             patch("src.core.request_classifier.classify_request",
                   return_value=self._classification()), \
             patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": "i"}], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": "v"}]):
            result = process_buyer_request(
                files=[str(f1), str(f2), str(f3)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
            )
        assert result.ok is True
        mock_persist.assert_called_once()
        args, kwargs = mock_persist.call_args
        # called as _persist_all_attachments(record_id, record_type, paths, gmail_message_id=...)
        assert args[1] == "rfq"
        passed_paths = args[2]
        assert sorted(passed_paths) == sorted([str(f1), str(f2), str(f3)])

    def test_create_pc_persists_all_paths(self, tmp_path, temp_data_dir):
        from src.core.ingest_pipeline import process_buyer_request
        from src.core.request_classifier import RequestClassification
        f = tmp_path / "704.docx"; f.write_bytes(b"PK")
        cls = RequestClassification(
            shape="pc_704_docx",
            agency="cdcr",
            confidence=0.9,
            reasons=[],
            primary_file="704.docx",
            solicitation_number="PC-123",
            institution="CSP-SAC",
            is_quote_only=True,
            required_forms=[],
        )
        with patch("src.core.ingest_pipeline._persist_all_attachments") as mock_persist, \
             patch("src.core.request_classifier.classify_request", return_value=cls), \
             patch("src.forms.price_check.parse_ams704",
                   return_value={"line_items": [{"description": "i"}], "header": {}}):
            result = process_buyer_request(
                files=[str(f)],
                email_subject="PC 704",
                email_sender="buyer@cdcr.ca.gov",
            )
        assert result.ok is True
        mock_persist.assert_called_once()
        args, _kwargs = mock_persist.call_args
        assert args[1] == "pc"


# ─── End-to-end: _update_existing_record persists too ────────────────────


class TestUpdateExistingPersistsAttachments:

    def _classification(self, shape="generic_rfq_pdf"):
        from src.core.request_classifier import RequestClassification
        return RequestClassification(
            shape=shape,
            agency="calvet",
            confidence=0.9,
            reasons=[],
            primary_file="rfq.pdf",
            solicitation_number="",
            institution="CalVet Yountville",
            is_quote_only=False,
            required_forms=[],
        )

    def test_buyer_reply_attachments_persist_on_reingest(
        self, tmp_path, temp_data_dir,
    ):
        """Buyer replies with a fresh attachment on a followup email —
        the re-ingest must persist the new PDF to rfq_files."""
        from src.core.ingest_pipeline import process_buyer_request
        f1 = tmp_path / "rfq.pdf"; f1.write_bytes(b"%PDF-1.4")

        with patch("src.core.ingest_pipeline._persist_all_attachments"), \
             patch("src.core.request_classifier.classify_request",
                   return_value=self._classification()), \
             patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": "i"}], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": "v"}]):
            r1 = process_buyer_request(
                files=[str(f1)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
            )

        f2 = tmp_path / "rev2.pdf"; f2.write_bytes(b"%PDF-1.4 REV2")
        with patch("src.core.ingest_pipeline._persist_all_attachments") as mock_persist, \
             patch("src.core.request_classifier.classify_request",
                   return_value=self._classification()), \
             patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": "i"}], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": "v"}]):
            process_buyer_request(
                files=[str(f2)],
                email_subject="CalVet RFQ (rev2)",
                email_sender="buyer@calvet.ca.gov",
                existing_record_id=r1.record_id,
                existing_record_type="rfq",
                gmail_message_id="MSG_REV2",
            )

        # _persist_all_attachments was called on the update path with
        # the new attachment path and the gmail_message_id from the
        # followup message.
        assert mock_persist.called
        last_call = mock_persist.call_args
        args, kwargs = last_call
        assert args[1] == "rfq"
        assert str(f2) in args[2]
        assert kwargs.get("gmail_message_id") == "MSG_REV2"
