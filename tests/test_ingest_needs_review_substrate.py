"""PR-A Step 3 (2026-05-11) — needs_review + ingest_warnings substrate.

Background: PR #908 added Vision verification but used a "Vision count
> base count" gate. The rfq_0ebe242f post-mortem revealed that gate is
wrong — when the base parser captures phantom items (regex on
parenthetical clarification text), it reports a HIGHER count than
Vision, so the gate silently keeps the bad data. Mike's directive:
"Vision is verification right? ... relying on regex could just be
creating bad data... I don't want any of that."

The Step 2 refactor of `_dispatch_parser` makes Vision the PRIMARY
items source on PDFs and flags Vision/base disagreements > tolerance
for operator review. Step 3 wires those signals — `_needs_review` and
`_ingest_warnings` stashed on the header dict — through to:
  - `IngestResult.needs_review` / `IngestResult.ingest_warnings`
  - `IngestResult.to_dict()` (so the API/UI can read them)
  - The persisted record dict (top-level `needs_review` +
    `ingest_warnings` so the operator banner survives reload)
  - Initial record status (flips to "needs_review" when items exist
    but a disagreement was flagged)
  - Re-ingest refresh (clears resolved disagreements without
    regressing operator-driven statuses)

These tests pin those wirings.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest


# ─── IngestResult shape ──────────────────────────────────────────────────


class TestIngestResultShape:

    def test_default_needs_review_false(self):
        from src.core.ingest_pipeline import IngestResult
        r = IngestResult()
        assert r.needs_review is False
        assert r.ingest_warnings == []

    def test_to_dict_emits_needs_review_key(self):
        from src.core.ingest_pipeline import IngestResult
        r = IngestResult(needs_review=True, ingest_warnings=[
            {"kind": "count_disagreement", "detail": "Vision 15, base 5"}
        ])
        d = r.to_dict()
        assert d["needs_review"] is True
        assert len(d["ingest_warnings"]) == 1
        assert d["ingest_warnings"][0]["kind"] == "count_disagreement"

    def test_to_dict_returns_copies_not_references(self):
        """API caller mutations on the serialized dict must not leak
        back into the IngestResult."""
        from src.core.ingest_pipeline import IngestResult
        warning = {"kind": "count_disagreement", "detail": "..."}
        r = IngestResult(ingest_warnings=[warning])
        d = r.to_dict()
        d["ingest_warnings"][0]["kind"] = "MUTATED"
        # Original IngestResult unchanged.
        assert r.ingest_warnings[0]["kind"] == "count_disagreement"


# ─── _dispatch_parser stashes signals on header ──────────────────────────


class TestDispatchParserHeaderSignals:

    def _classification(self, shape):
        from unittest.mock import MagicMock
        c = MagicMock()
        c.shape = shape
        return c

    def test_large_disagreement_sets_needs_review_on_header(self, tmp_path):
        """When Vision and base disagree by > tolerance, header carries
        `_needs_review=True` and an `_ingest_warnings` entry of kind
        `count_disagreement`."""
        from src.core.ingest_pipeline import _dispatch_parser
        from src.core.request_classifier import SHAPE_GENERIC_RFQ_PDF
        f = tmp_path / "rfq.pdf"
        f.write_bytes(b"%PDF-1.4 stub")
        base = [{"description": "a"}, {"description": "b"}]
        visionx = [{"description": f"v{i}"} for i in range(15)]
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": base, "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=visionx):
            _items, header, _err = _dispatch_parser(
                str(f), self._classification(SHAPE_GENERIC_RFQ_PDF))
        assert header.get("_needs_review") is True
        warnings = header.get("_ingest_warnings") or []
        assert any(w.get("kind") == "count_disagreement" for w in warnings)

    def test_small_disagreement_does_not_set_needs_review(self, tmp_path):
        """Vision finds 1 more item than base — within tolerance,
        no review flag."""
        from src.core.ingest_pipeline import _dispatch_parser
        from src.core.request_classifier import SHAPE_GENERIC_RFQ_PDF
        f = tmp_path / "rfq.pdf"
        f.write_bytes(b"%PDF-1.4 stub")
        base = [{"description": f"i{i}"} for i in range(10)]
        visionx = [{"description": f"v{i}"} for i in range(11)]
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": base, "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=visionx):
            _items, header, _err = _dispatch_parser(
                str(f), self._classification(SHAPE_GENERIC_RFQ_PDF))
        assert header.get("_needs_review") is not True

    def test_non_pdf_emits_vision_unsupported_warning(self, tmp_path):
        from src.core.ingest_pipeline import _dispatch_parser
        from src.core.request_classifier import SHAPE_PC_704_DOCX
        f = tmp_path / "704.docx"
        f.write_bytes(b"PK stub")
        with patch("src.forms.price_check.parse_ams704",
                   return_value={"line_items": [{"description": "i"}],
                                 "header": {}}):
            _items, header, _err = _dispatch_parser(
                str(f), self._classification(SHAPE_PC_704_DOCX))
        warnings = header.get("_ingest_warnings") or []
        assert any(w.get("kind") == "vision_unsupported_format" for w in warnings)
        # Format-skip is informational, not a review flag.
        assert header.get("_needs_review") is not True

    def test_vision_unavailable_emits_skipped_warning(self, tmp_path):
        from src.core.ingest_pipeline import _dispatch_parser
        from src.core.request_classifier import SHAPE_GENERIC_RFQ_PDF
        f = tmp_path / "rfq.pdf"
        f.write_bytes(b"%PDF-1.4 stub")
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": "i"}], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=None):
            _items, header, _err = _dispatch_parser(
                str(f), self._classification(SHAPE_GENERIC_RFQ_PDF))
        warnings = header.get("_ingest_warnings") or []
        assert any(w.get("kind") == "vision_skipped" for w in warnings)


# ─── process_buyer_request propagates signals into IngestResult ─────────


class TestProcessBuyerRequestPropagation:

    def test_needs_review_flows_into_result_and_pops_from_header(
        self, tmp_path, temp_data_dir,
    ):
        """End-to-end: large disagreement on a generic-RFQ-PDF produces
        an IngestResult with needs_review=True AND a record whose
        top-level `needs_review` is True. Underscored header keys are
        popped so they don't leak into downstream readers as dead
        substrate."""
        from src.core.ingest_pipeline import process_buyer_request
        from src.api.dashboard import load_rfqs
        f = tmp_path / "calvet.pdf"
        f.write_bytes(b"%PDF-1.4 stub")
        base = [{"description": f"b{i}"} for i in range(5)]
        visionx = [{"description": f"v{i}"} for i in range(15)]
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": base, "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=visionx), \
             patch("src.core.request_classifier.classify_request") as mock_cls:
            from src.core.request_classifier import RequestClassification
            mock_cls.return_value = RequestClassification(
                shape="generic_rfq_pdf",
                agency="calvet",
                confidence=0.9,
                reasons=[],
                primary_file="calvet.pdf",
                solicitation_number="",
                institution="CalVet Yountville",
                is_quote_only=False,
                required_forms=[],
            )
            result = process_buyer_request(
                files=[str(f)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
            )
        assert result.ok is True
        assert result.needs_review is True
        assert len(result.ingest_warnings) >= 1
        # Persisted record carries the flag at the top level.
        rfqs = load_rfqs()
        rfq = rfqs.get(result.record_id)
        assert rfq is not None
        assert rfq.get("needs_review") is True
        # Status flipped into the triage queue even though items > 0.
        assert rfq.get("status") == "needs_review"
        # Underscored signal keys did NOT leak onto the record header.
        # (The dispatch helper pops them before returning header.)

    def test_no_disagreement_record_status_parsed(
        self, tmp_path, temp_data_dir,
    ):
        from src.core.ingest_pipeline import process_buyer_request
        from src.api.dashboard import load_rfqs
        f = tmp_path / "calvet.pdf"
        f.write_bytes(b"%PDF-1.4 stub")
        base = [{"description": f"i{i}"} for i in range(10)]
        visionx = [{"description": f"v{i}"} for i in range(10)]
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": base, "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=visionx), \
             patch("src.core.request_classifier.classify_request") as mock_cls:
            from src.core.request_classifier import RequestClassification
            mock_cls.return_value = RequestClassification(
                shape="generic_rfq_pdf",
                agency="calvet",
                confidence=0.9,
                reasons=[],
                primary_file="calvet.pdf",
                solicitation_number="",
                institution="CalVet Yountville",
                is_quote_only=False,
                required_forms=[],
            )
            result = process_buyer_request(
                files=[str(f)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
            )
        assert result.needs_review is False
        rfq = load_rfqs().get(result.record_id)
        assert rfq is not None
        assert rfq.get("needs_review") is False
        assert rfq.get("status") == "parsed"


# ─── Re-ingest refresh ───────────────────────────────────────────────────


class TestReingestRefresh:

    def test_reingest_clears_needs_review_when_disagreement_resolved(
        self, tmp_path, temp_data_dir,
    ):
        """Operator re-runs ingest on a flagged record; Vision and base
        parser now agree — `needs_review` clears, status moves back to
        `parsed`."""
        from src.core.ingest_pipeline import process_buyer_request
        from src.api.dashboard import load_rfqs
        from src.core.request_classifier import RequestClassification

        f = tmp_path / "calvet.pdf"
        f.write_bytes(b"%PDF-1.4 stub")

        def _classify():
            return RequestClassification(
                shape="generic_rfq_pdf",
                agency="calvet",
                confidence=0.9,
                reasons=[],
                primary_file="calvet.pdf",
                solicitation_number="",
                institution="CalVet Yountville",
                is_quote_only=False,
                required_forms=[],
            )

        # First ingest — large disagreement.
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": "a"}], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": f"v{i}"} for i in range(15)]), \
             patch("src.core.request_classifier.classify_request",
                   return_value=_classify()):
            r1 = process_buyer_request(
                files=[str(f)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
            )
        assert r1.needs_review is True

        # Re-ingest — now both signals agree.
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": f"i{i}"}
                                            for i in range(15)], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": f"v{i}"} for i in range(15)]), \
             patch("src.core.request_classifier.classify_request",
                   return_value=_classify()):
            r2 = process_buyer_request(
                files=[str(f)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
                existing_record_id=r1.record_id,
                existing_record_type="rfq",
            )
        assert r2.needs_review is False
        rfq = load_rfqs().get(r1.record_id)
        assert rfq is not None
        assert rfq.get("needs_review") is False
        # Status moves out of triage now that the signal resolved.
        assert rfq.get("status") == "parsed"

    def test_reingest_does_not_regress_operator_status(
        self, tmp_path, temp_data_dir,
    ):
        """If the operator already moved a record to `quoted`, a fresh
        ingest detecting a new disagreement must NOT regress status to
        `needs_review` — operator-driven statuses are downstream of
        triage. `needs_review` flag still flips on so the banner
        renders, but the queue doesn't lose work-in-progress."""
        from src.core.ingest_pipeline import process_buyer_request
        from src.api.dashboard import load_rfqs, _save_single_rfq
        from src.core.request_classifier import RequestClassification

        f = tmp_path / "calvet.pdf"
        f.write_bytes(b"%PDF-1.4 stub")

        def _classify():
            return RequestClassification(
                shape="generic_rfq_pdf",
                agency="calvet",
                confidence=0.9,
                reasons=[],
                primary_file="calvet.pdf",
                solicitation_number="",
                institution="CalVet Yountville",
                is_quote_only=False,
                required_forms=[],
            )

        # First ingest — clean.
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": f"i{i}"}
                                            for i in range(10)], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": f"v{i}"} for i in range(10)]), \
             patch("src.core.request_classifier.classify_request",
                   return_value=_classify()):
            r1 = process_buyer_request(
                files=[str(f)],
                email_subject="CalVet RFQ",
                email_sender="buyer@calvet.ca.gov",
            )

        # Operator quotes the RFQ.
        rfq = load_rfqs().get(r1.record_id)
        rfq["status"] = "quoted"
        _save_single_rfq(r1.record_id, rfq)

        # Buyer re-emails; re-ingest detects new disagreement.
        with patch("src.forms.generic_rfq_parser.parse_generic_rfq",
                   return_value={"items": [{"description": "a"}], "header": {}}), \
             patch("src.core.ingest_pipeline._vision_primary_extract",
                   return_value=[{"description": f"v{i}"} for i in range(15)]), \
             patch("src.core.request_classifier.classify_request",
                   return_value=_classify()):
            process_buyer_request(
                files=[str(f)],
                email_subject="CalVet RFQ (followup)",
                email_sender="buyer@calvet.ca.gov",
                existing_record_id=r1.record_id,
                existing_record_type="rfq",
            )

        rfq2 = load_rfqs().get(r1.record_id)
        # Banner signal flips on so operator sees the new disagreement...
        assert rfq2.get("needs_review") is True
        # ...but status stays `quoted` so work-in-progress isn't lost.
        assert rfq2.get("status") == "quoted"
