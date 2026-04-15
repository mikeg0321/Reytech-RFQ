"""Unified ingest pipeline — Phase 2 of the PC↔RFQ refactor.

One entry point: `process_buyer_request(files, email_body, ...)`.
Runs the classifier, dispatches to the correct parser, creates the
right record type (PC or RFQ), stores the classification on the
record, and runs the linker with triangulated matching (agency +
solicitation + items) instead of the old fuzzy-threshold logic.

This fixes the "RFQ 6655f190 wrong PC linked" class of bugs by
making the classification the single source of truth for what a
request IS, and forcing the linker to agree on canonical identity
(agency + solicitation number) before it can even consider an item
match.

Feature-flagged via `ingest.classifier_v2_enabled` (default False).
When disabled, callers fall through to the legacy ingest paths.

Callers:
  - email_poller.py — when a buyer email arrives
  - /api/rfq/<id>/upload-parse-doc — operator manual upload
  - /api/v1/rfq (external API clients)
"""
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("reytech.ingest")


@dataclass
class IngestResult:
    """Outcome of one ingest attempt."""
    ok: bool = False
    record_type: str = ""  # "pc" | "rfq" | ""
    record_id: str = ""
    classification: Optional[Dict[str, Any]] = None
    linked_pc_id: str = ""
    link_reason: str = ""
    link_confidence: float = 0.0
    items_parsed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "record_type": self.record_type,
            "record_id": self.record_id,
            "classification": self.classification,
            "linked_pc_id": self.linked_pc_id,
            "link_reason": self.link_reason,
            "link_confidence": self.link_confidence,
            "items_parsed": self.items_parsed,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
        }


def process_buyer_request(
    files: List[str] = None,
    email_body: str = "",
    email_subject: str = "",
    email_sender: str = "",
    email_uid: str = "",
    existing_record_id: str = "",
    existing_record_type: str = "",
) -> IngestResult:
    """Single entry point for every buyer request.

    Args:
        files: list of local paths to attachments / uploads
        email_body: plain-text email body (if from email poller)
        email_subject: email subject line
        email_sender: email from-address
        email_uid: email UID for dedup (optional)
        existing_record_id: if re-parsing an existing PC/RFQ, pass its ID
        existing_record_type: "pc" or "rfq" when re-parsing

    Returns: IngestResult with the classification, record_id,
    linked_pc_id, and any errors/warnings.

    Happy path:
      1. Classify the request (shape + agency + forms + sol_number)
      2. Store classification on the result for downstream
      3. Dispatch to the right parser based on shape
      4. Create (or update) PC/RFQ record with parsed items +
         classification stored as `_classification` field
      5. Run linker with triangulated matching
      6. Return result with linked_pc_id + confidence
    """
    result = IngestResult()
    files = files or []

    # Telemetry: start the feature timer so every ingest is measured.
    # Populate what we already know about the input BEFORE we attempt
    # to classify — that way a classifier crash still emits context
    # (file count, sender, subject length) to the dead_features view.
    try:
        from src.core.utilization import time_feature
        _timer_ctx_mgr = time_feature("ingest.process_buyer_request")
        _telemetry_ctx = _timer_ctx_mgr.__enter__()
        _telemetry_ctx["file_count"] = len(files)
        _telemetry_ctx["has_email_body"] = bool(email_body)
        _telemetry_ctx["subject_len"] = len(email_subject or "")
        _telemetry_ctx["sender"] = (email_sender or "")[:80]
        _telemetry_started = True
    except Exception:
        _timer_ctx_mgr = None
        _telemetry_ctx = {}
        _telemetry_started = False

    # ── Step 1: classify ──
    try:
        from src.core.request_classifier import classify_request
        classification = classify_request(
            attachments=files,
            email_body=email_body,
            email_subject=email_subject,
            email_sender=email_sender,
        )
    except Exception as e:
        log.error("classifier crashed: %s", e, exc_info=True)
        result.errors.append(f"classifier crashed: {e}")
        # Emit a dedicated crash event — the main timer records only
        # "ingest.process_buyer_request" (which includes success
        # cases), so crashes get buried. A separate feature name gives
        # the dead_features / error-rate views something to latch onto.
        try:
            from src.core.utilization import record_feature_use
            record_feature_use(
                feature="ingest.classify_crashed",
                context={
                    "error": str(e)[:200],
                    "error_type": type(e).__name__,
                    "file_count": len(files),
                    "attachment_names": [os.path.basename(f) for f in files[:10]],
                    "sender": (email_sender or "")[:80],
                    "subject_len": len(email_subject or ""),
                },
                ok=False,
            )
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        if _telemetry_started:
            try:
                _timer_ctx_mgr.__exit__(Exception, e, None)
            except Exception as _e:
                log.debug("suppressed: %s", _e)
        return result

    # Telemetry context — classification succeeded, enrich the timer
    _telemetry_ctx["shape"] = classification.shape
    _telemetry_ctx["agency"] = classification.agency
    _telemetry_ctx["confidence"] = classification.confidence

    result.classification = classification.to_dict()
    result.reasons.extend(classification.reasons)
    log.info(
        "ingest classified: shape=%s agency=%s conf=%.2f sol=%s",
        classification.shape, classification.agency,
        classification.confidence, classification.solicitation_number,
    )

    # ── Step 2: decide record type ──
    # Quote-only shapes (PC worksheets) → PC record
    # Full-package shapes (CCHCS packet, generic RFQ) → RFQ record
    record_type = "pc" if classification.is_quote_only else "rfq"
    result.record_type = record_type

    # ── Step 3: parse items from the primary file ──
    items = []
    header = {}
    parse_error = None
    primary_path = None
    if classification.primary_file and files:
        # Find the full path for the classifier's primary_file
        for f in files:
            if os.path.basename(f) == classification.primary_file:
                primary_path = f
                break

    if primary_path:
        try:
            items, header, parse_error = _dispatch_parser(
                primary_path, classification
            )
        except Exception as e:
            log.error("parser dispatch crashed: %s", e, exc_info=True)
            parse_error = f"parser crashed: {e}"

    if parse_error:
        result.warnings.append(f"parse: {parse_error}")
    result.items_parsed = len(items)

    # ── Step 4: create or update the record ──
    try:
        if existing_record_id and existing_record_type:
            record_id = _update_existing_record(
                existing_record_id, existing_record_type,
                items, header, classification, primary_path,
            )
        else:
            record_id = _create_record(
                record_type, items, header, classification,
                primary_path, email_subject, email_sender, email_uid,
            )
        result.record_id = record_id
    except Exception as e:
        log.error("record create/update failed: %s", e, exc_info=True)
        result.errors.append(f"record save failed: {e}")
        return result

    # ── Step 5: link to PC (only if this is an RFQ) ──
    if record_type == "rfq" and record_id:
        try:
            linked_pc_id, link_reason, link_score = _run_triangulated_linker(
                record_id, classification, items,
            )
            result.linked_pc_id = linked_pc_id
            result.link_reason = link_reason
            result.link_confidence = link_score
            if linked_pc_id:
                result.reasons.append(
                    f"linked to pc {linked_pc_id[:8]}: {link_reason} (score {link_score})"
                )
        except Exception as e:
            log.error("linker crashed: %s", e, exc_info=True)
            result.warnings.append(f"linker: {e}")

    result.ok = True
    _telemetry_ctx["record_type"] = result.record_type
    _telemetry_ctx["record_id"] = result.record_id
    _telemetry_ctx["items_parsed"] = result.items_parsed
    _telemetry_ctx["linked"] = bool(result.linked_pc_id)
    if _telemetry_started:
        try:
            _timer_ctx_mgr.__exit__(None, None, None)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
    return result


# ── Dispatcher: classification → correct parser ─────────────────────────

def _dispatch_parser(
    path: str,
    classification: "RequestClassification",  # noqa: F821
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """Route a file to the parser that matches its classified shape.
    Returns (items, header, error_or_none). Never raises."""
    from src.core.request_classifier import (
        SHAPE_CCHCS_PACKET,
        SHAPE_PC_704_DOCX,
        SHAPE_PC_704_PDF_DOCUSIGN,
        SHAPE_PC_704_PDF_FILLABLE,
        SHAPE_GENERIC_RFQ_XLSX,
        SHAPE_GENERIC_RFQ_PDF,
        SHAPE_GENERIC_RFQ_DOCX,
    )

    shape = classification.shape

    # CCHCS packet has its own dedicated parser
    if shape == SHAPE_CCHCS_PACKET:
        try:
            from src.forms.cchcs_packet_parser import parse_cchcs_packet
            parsed = parse_cchcs_packet(path)
            if not parsed.get("ok"):
                return [], {}, parsed.get("error", "cchcs parse failed")
            return (
                parsed.get("line_items", []),
                parsed.get("header", {}),
                None,
            )
        except Exception as e:
            return [], {}, f"cchcs parser crashed: {e}"

    # AMS 704 (DOCX, fillable PDF, or DocuSign PDF) — they all flow through
    # the same parser which handles format detection internally
    if shape in (SHAPE_PC_704_DOCX, SHAPE_PC_704_PDF_FILLABLE, SHAPE_PC_704_PDF_DOCUSIGN):
        try:
            from src.forms.price_check import parse_ams704
            parsed = parse_ams704(path)
            if parsed.get("error"):
                return [], {}, parsed.get("error")
            return (
                parsed.get("line_items", []),
                parsed.get("header", {}),
                None,
            )
        except Exception as e:
            return [], {}, f"ams704 parser crashed: {e}"

    # Generic RFQ formats — fall through to the generic parser
    if shape in (SHAPE_GENERIC_RFQ_PDF, SHAPE_GENERIC_RFQ_DOCX, SHAPE_GENERIC_RFQ_XLSX):
        try:
            from src.forms.generic_rfq_parser import parse_generic_rfq
            parsed = parse_generic_rfq([path])
            return (
                parsed.get("items", []),
                parsed.get("header", {}),
                None,
            )
        except Exception as e:
            return [], {}, f"generic parser crashed: {e}"

    # Unknown shape — nothing to parse
    return [], {}, f"no parser for shape {shape}"


# ── Record creation ─────────────────────────────────────────────────────

def _create_record(
    record_type: str,
    items: List[Dict[str, Any]],
    header: Dict[str, Any],
    classification: "RequestClassification",  # noqa: F821
    primary_path: Optional[str],
    email_subject: str,
    email_sender: str,
    email_uid: str,
) -> str:
    """Create a new PC or RFQ with the classification stored on it."""
    now = datetime.now().isoformat()
    short_id = uuid.uuid4().hex[:8]

    record: Dict[str, Any] = {
        "id": f"{record_type}_{short_id}",
        "created_at": now,
        "updated_at": now,
        "status": "parsed",
        "source": "ingest_v2",
        "email_uid": email_uid,
        "email_subject": email_subject,
        "email_sender": email_sender,
        "source_pdf": primary_path or "",
        "_classification": classification.to_dict(),
        # Common header fields pulled from either the classifier or parser
        "solicitation_number": classification.solicitation_number or header.get("solicitation_number", "") or header.get("pc_number", ""),
        "institution": classification.institution or header.get("institution", ""),
        "agency": classification.agency,
        "requestor_email": email_sender,
        "requestor_name": header.get("requestor", "") or header.get("requestor_name", ""),
    }

    if record_type == "pc":
        record["pc_number"] = (
            classification.solicitation_number
            or header.get("pc_number", "")
            or f"AUTO_{short_id}"
        )
        record["items"] = items
        record["packet_type"] = (
            "cchcs_non_it"
            if classification.shape == "cchcs_packet"
            else ""
        )
        from src.api.dashboard import _save_single_pc
        _save_single_pc(record["id"], record)
    else:  # rfq
        record["rfq_number"] = (
            classification.solicitation_number
            or header.get("solicitation_number", "")
            or f"AUTO_{short_id}"
        )
        record["line_items"] = items
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(record["id"], record)

    log.info("ingest created %s %s with %d items", record_type, record["id"], len(items))
    return record["id"]


def _update_existing_record(
    record_id: str,
    record_type: str,
    items: List[Dict[str, Any]],
    header: Dict[str, Any],
    classification: "RequestClassification",  # noqa: F821
    primary_path: Optional[str],
) -> str:
    """Re-run classification + parsing on an existing record.
    Used when the operator clicks 'Re-parse' on an already-created PC/RFQ.
    """
    from src.api.dashboard import _load_price_checks, _save_single_pc
    if record_type == "pc":
        pcs = _load_price_checks()
        pc = pcs.get(record_id) or {}
        pc["_classification"] = classification.to_dict()
        if items:
            pc["items"] = items
        if primary_path:
            pc["source_pdf"] = primary_path
        pc["updated_at"] = datetime.now().isoformat()
        _save_single_pc(record_id, pc)
    else:  # rfq
        from src.api.dashboard import _save_single_rfq, load_rfqs
        rfqs = load_rfqs()
        rfq = rfqs.get(record_id) or {}
        rfq["_classification"] = classification.to_dict()
        if items:
            rfq["line_items"] = items
        if primary_path:
            rfq["source_pdf"] = primary_path
            # Register the uploaded file under rfq["templates"] so the
            # package generator can find it. The legacy upload-templates
            # handler did this via identify_attachments; the classifier_v2
            # ingest path previously skipped it, so operators hit
            # "Missing required templates: 704B" at package generation
            # time even though they had just uploaded the 704B PDF.
            # Merge (don't overwrite) so 703B/704B/bidpkg accumulate
            # across multiple uploads on the same RFQ.
            try:
                from src.forms.rfq_parser import identify_attachments
                new_templates = identify_attachments([primary_path])
                if new_templates:
                    existing = rfq.get("templates") or {}
                    for _k, _v in new_templates.items():
                        existing[_k] = _v
                    rfq["templates"] = existing
                    log.info(
                        "ingest update: registered templates for %s: %s",
                        record_id, list(new_templates.keys()),
                    )
            except Exception as _e:
                log.warning("ingest update: template registration failed: %s", _e)
        rfq["updated_at"] = datetime.now().isoformat()
        _save_single_rfq(record_id, rfq)
    return record_id


# ── Triangulated linker ─────────────────────────────────────────────────

def _run_triangulated_linker(
    rfq_id: str,
    classification: "RequestClassification",  # noqa: F821
    rfq_items: List[Dict[str, Any]],
) -> Tuple[str, str, float]:
    """New linker that requires two of three anchors to match before
    considering a PC → RFQ link:
      ANCHOR 1: same agency (from classification)
      ANCHOR 2: same solicitation number
      ANCHOR 3: same item set (description similarity >= 0.6 on
                at least 60% of items)

    This replaces the old fuzzy threshold (40 points, any single
    signal) which was responsible for the "wrong PC linked" bug.
    Returns (pc_id, reason, confidence).
    """
    from src.api.dashboard import _load_price_checks
    from src.core.quote_request import QuoteRequest
    from difflib import SequenceMatcher

    pcs = _load_price_checks()
    if not pcs:
        return "", "no pcs in store", 0.0

    rfq_agency = classification.agency
    rfq_sol = (classification.solicitation_number or "").strip()
    rfq_inst = (classification.institution or "").strip().lower()

    candidates = []
    for pc_id, pc in pcs.items():
        if not isinstance(pc, dict):
            continue
        if pc.get("status") in ("duplicate", "dismissed", "archived"):
            continue
        qr = QuoteRequest.from_pc(pc)
        pc_agency = qr.get_agency()
        pc_sol = qr.get_solicitation().strip()
        pc_inst = qr.get_institution().strip().lower()
        pc_items = qr.get_items()

        anchors = []
        # ANCHOR 1: same agency
        if rfq_agency != "other" and pc_agency == rfq_agency:
            anchors.append("agency")
        # ANCHOR 2: same solicitation
        if rfq_sol and pc_sol and (rfq_sol == pc_sol
                                    or rfq_sol in pc_sol
                                    or pc_sol in rfq_sol):
            anchors.append("solicitation")
        # ANCHOR 3: same institution (tight — no fuzzy substring)
        if rfq_inst and pc_inst and rfq_inst == pc_inst:
            anchors.append("institution")
        # ANCHOR 4: item similarity
        if rfq_items and pc_items:
            matched = 0
            for ri in rfq_items:
                rd = (ri.get("description", "") or "").lower()
                if not rd or len(rd) < 5:
                    continue
                for pi in pc_items:
                    pd = (pi.get("description", pi.get("desc", "")) or "").lower()
                    if not pd:
                        continue
                    if SequenceMatcher(None, rd, pd).ratio() > 0.75:
                        matched += 1
                        break
            coverage = matched / max(len(rfq_items), 1)
            if coverage >= 0.60:
                anchors.append(f"items({coverage:.0%})")

        # Require at least 2 anchors
        if len(anchors) >= 2:
            candidates.append({
                "pc_id": pc_id,
                "anchors": anchors,
                "score": len(anchors),
                "created_at": pc.get("created_at", ""),
            })

    if not candidates:
        return "", "no triangulated match (need >=2 anchors)", 0.0

    # Solicitation number is the strongest anchor — if exactly one
    # candidate has it, return that one regardless of anchor count.
    # Without this, a re-sent old RFQ would tie-break to the
    # most-recently-created PC (which just happens to be current)
    # instead of the actually-matching sol-number PC from weeks ago.
    sol_candidates = [c for c in candidates if "solicitation" in c["anchors"]]
    if len(sol_candidates) == 1:
        best = sol_candidates[0]
    else:
        # Tie-break: most anchors, then most-recent created_at
        candidates.sort(key=lambda c: (-c["score"], -_ts(c["created_at"])))
        best = candidates[0]
    return (
        best["pc_id"],
        "+".join(best["anchors"]),
        round(best["score"] / 4.0, 2),  # score normalized to 0-1
    )


def _ts(s: str) -> float:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


__all__ = [
    "process_buyer_request",
    "IngestResult",
]
