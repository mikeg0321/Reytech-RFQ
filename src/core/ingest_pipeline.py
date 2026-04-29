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

Canonical ingest path as of 2026-04-29 (Plan §3.3 flag sprint —
`ingest.classifier_v2_enabled` removed). Live in production since
2026-04-14 with no rollback signal.

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
        # Emit BOTH the canonical (items_parsed) and legacy-compatible
        # (items_found / items_added / parser) fields so every Upload & Parse
        # caller — frontend `uploadDoc()` included — reads the same shape
        # regardless of whether classifier_v2 or the legacy parser chain ran.
        parser_label = (self.classification or {}).get("shape", "classifier_v2")
        return {
            "ok": self.ok,
            "record_type": self.record_type,
            "record_id": self.record_id,
            "classification": self.classification,
            "linked_pc_id": self.linked_pc_id,
            "link_reason": self.link_reason,
            "link_confidence": self.link_confidence,
            "items_parsed": self.items_parsed,
            "items_found": self.items_parsed,
            "items_added": self.items_parsed,
            "parser": parser_label,
            "parser_used": parser_label,
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

    # ── Step 4b: ghost-record detection (Bundle-2 PR-2c) ──
    # Marks-not-deletes: a matching ghost pattern stamps `hidden_reason`
    # on the record so the triage / queue / NEXT UP filters skip it.
    # Always emits telemetry; only stamps when the
    # `ingest.ghost_quarantine_enabled` flag is on (default False for
    # the first deploy — shadow-compare period).
    if record_id:
        try:
            from src.core.ghost_detection import detect_ghost_pattern
            from src.core.flags import get_flag
            # Re-read the freshly-saved record so we reason against the
            # canonical post-create dict (with normalized fields, agency
            # resolution side-effects, etc.).
            _saved = None
            try:
                if record_type == "pc":
                    from src.api.data_layer import _load_price_checks
                    _saved = _load_price_checks().get(record_id)
                else:
                    from src.api.data_layer import load_rfqs
                    _saved = load_rfqs().get(record_id)
            except Exception as _re:
                log.debug("ghost-detect re-read failed: %s", _re)
            ghost_record = _saved or {
                "buyer_name": header.get("buyer_name", ""),
                "institution": header.get("institution", "")
                                or classification.agency,
                "pc_number": classification.solicitation_number or "",
            }
            reason = detect_ghost_pattern(
                ghost_record,
                email_sender=email_sender,
                items_parsed=len(items),
            )
            if reason:
                _quarantine_on = bool(get_flag(
                    "ingest.ghost_quarantine_enabled", False
                ))
                if _quarantine_on:
                    # Mark in place + persist the stamp.
                    try:
                        from datetime import datetime
                        ghost_record["hidden_reason"] = reason
                        ghost_record["hidden_at"] = datetime.utcnow().isoformat()
                        if record_type == "pc":
                            from src.api.dashboard import _save_single_pc
                            _save_single_pc(record_id, ghost_record)
                        else:
                            from src.api.dashboard import _save_single_rfq
                            _save_single_rfq(record_id, ghost_record)
                        result.warnings.append(
                            f"quarantined: {reason}"
                        )
                        result.reasons.append(
                            f"ghost-detected: {reason} (quarantined)"
                        )
                    except Exception as _se:
                        log.error(
                            "ghost-stamp save failed: %s", _se, exc_info=True
                        )
                        result.warnings.append(
                            f"ghost-stamp save failed: {_se}"
                        )
                else:
                    # Shadow mode: don't stamp; record telemetry so we
                    # can verify detection accuracy before flipping the
                    # flag on.
                    result.reasons.append(
                        f"ghost-detected (shadow): {reason}"
                    )
                # Telemetry — always fires regardless of flag state.
                try:
                    from src.core.utilization import record_feature_use
                    record_feature_use("ingest.ghost_detected", context={
                        "reason": reason,
                        "record_type": record_type,
                        "record_id": record_id,
                        "quarantined": _quarantine_on,
                        "items_parsed": len(items),
                        "sender_domain": (email_sender or "").split("@", 1)[-1][:80],
                    })
                except Exception as _te:
                    log.debug("ghost telemetry suppressed: %s", _te)
        except Exception as _ge:
            log.error("ghost-detection crashed: %s", _ge, exc_info=True)
            result.warnings.append(f"ghost-detect: {_ge}")

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
                # Step 5b (Bundle-6 PR-6a, audit item linker→pricing copy):
                # when a PC is linked, copy its per-item pricing subdict onto
                # the RFQ's line items by description similarity. Idempotent —
                # operator's manual edits survive re-runs because items already
                # carrying a pricing_copied_from_pc marker are skipped. Surfaces
                # a banner on RFQ detail so the operator knows pricing didn't
                # appear by magic. Never raises into the main pipeline.
                try:
                    copy_report = _copy_pc_pricing_to_rfq(
                        record_id, linked_pc_id, items,
                    )
                    if copy_report.get("copied"):
                        result.reasons.append(
                            f"pricing copied from pc {linked_pc_id[:8]}: "
                            f"{copy_report['copied']} item(s)"
                        )
                except Exception as _e:
                    log.error("pricing-copy post-link hook failed: %s", _e, exc_info=True)
                    result.warnings.append(f"pricing-copy: {_e}")
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


# ── Preview variant — Bundle-2 PR-2a ────────────────────────────────────

def preview_buyer_request(
    files: List[str] = None,
    email_body: str = "",
    email_subject: str = "",
    email_sender: str = "",
) -> Dict[str, Any]:
    """Preview-only variant of `process_buyer_request`.

    Runs the same classifier + parser + institution resolver + deadline
    extractor pipeline, but does NOT create or update any record. Used
    by the two-step upload UI (audit item B in 2026-04-22 session audit):
    operator drops files, app shows what it detected (shape, agency,
    facility, deadline, required forms, line-item preview), operator
    confirms before anything persists.

    Returns a plain dict — not IngestResult — because the preview has a
    different contract than the ingest result (no record_id, adds
    `facility` and `deadline` subdicts that are only available after
    resolution, omits linker + pricing-copy steps).

    Safe to call with zero side effects. Designed to be a no-op on
    error: any crash in a pipeline stage turns into a `warnings[]`
    entry instead of bubbling up — the UI still needs something to
    render so the operator can at least fix / retry the upload.
    """
    files = files or []
    result: Dict[str, Any] = {
        "ok": True,
        "classification": None,
        "shape": "",
        "agency": "",
        "confidence": 0.0,
        "solicitation_number": "",
        "required_forms": [],
        "primary_file": "",
        "items": [],
        "items_parsed": 0,
        "header": {},
        "facility": None,
        "deadline": None,
        "warnings": [],
        "errors": [],
        "reasons": [],
    }

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
        log.error("preview classifier crashed: %s", e, exc_info=True)
        result["ok"] = False
        result["errors"].append(f"classifier crashed: {e}")
        return result

    result["classification"] = classification.to_dict()
    result["shape"] = classification.shape
    result["agency"] = classification.agency
    result["confidence"] = classification.confidence
    result["solicitation_number"] = classification.solicitation_number or ""
    result["required_forms"] = list(classification.required_forms or [])
    result["primary_file"] = classification.primary_file or ""
    result["reasons"].extend(classification.reasons)

    # ── Step 2: parse items from the primary file ──
    primary_path: Optional[str] = None
    if classification.primary_file and files:
        for f in files:
            if os.path.basename(f) == classification.primary_file:
                primary_path = f
                break

    items: List[Dict[str, Any]] = []
    header: Dict[str, Any] = {}
    if primary_path:
        try:
            items, header, parse_error = _dispatch_parser(
                primary_path, classification
            )
            if parse_error:
                result["warnings"].append(f"parse: {parse_error}")
        except Exception as e:
            log.error("preview parser crashed: %s", e, exc_info=True)
            result["warnings"].append(f"parser crashed: {e}")

    result["items"] = items
    result["items_parsed"] = len(items)
    result["header"] = header

    # ── Step 3: resolve facility / institution ──
    # institution_resolver.resolve() returns a dict with canonical
    # code + name when it can map; None when it can't. Fed by header
    # fields first (buyer's explicit delivery address beats email
    # metadata), falling back to classifier-derived agency.
    try:
        from src.core.institution_resolver import resolve as _resolve_inst
        inst_seed = (
            header.get("institution")
            or header.get("ship_to")
            or header.get("delivery_location")
            or header.get("agency")
            or classification.agency
            or ""
        )
        if inst_seed:
            inst = _resolve_inst(str(inst_seed))
            if inst and (inst.get("code") or inst.get("facility_code")):
                result["facility"] = {
                    "code": inst.get("code") or inst.get("facility_code"),
                    "name": (
                        inst.get("canonical_name")
                        or inst.get("name")
                        or inst.get("facility_name")
                        or ""
                    ),
                    "agency": (
                        inst.get("parent_agency")
                        or inst.get("agency")
                        or classification.agency
                        or ""
                    ),
                    "confidence": inst.get("confidence"),
                    "raw": inst_seed,
                }
    except Exception as e:
        log.debug("preview facility resolve skipped: %s", e)
        result["warnings"].append(f"facility resolver: {e}")

    # ── Step 4: extract deadline ──
    # `apply_default_if_missing` runs header → email-body → default.
    # Call it on a synthetic doc that carries the header + body fields
    # it expects so we can report what the operator would see WITHOUT
    # stamping anything on a real record.
    try:
        from src.core.deadline_defaults import apply_default_if_missing
        preview_doc: Dict[str, Any] = {
            "header": header,
            "body_text": email_body or "",
        }
        # Also pull any explicit due_date the header already carried —
        # apply_default_if_missing treats doc-level keys as authoritative.
        if header.get("due_date"):
            preview_doc["due_date"] = header["due_date"]
        if header.get("due_time"):
            preview_doc["due_time"] = header["due_time"]
        source = apply_default_if_missing(preview_doc, email_body=email_body)
        if preview_doc.get("due_date"):
            result["deadline"] = {
                "due_date": preview_doc.get("due_date"),
                "due_time": preview_doc.get("due_time"),
                "source": preview_doc.get(
                    "due_date_source", source or "default"
                ),
            }
    except Exception as e:
        log.debug("preview deadline extract skipped: %s", e)
        result["warnings"].append(f"deadline: {e}")

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
        SHAPE_CCHCS_IT_RFQ,
        SHAPE_PC_704_DOCX,
        SHAPE_PC_704_PDF_DOCUSIGN,
        SHAPE_PC_704_PDF_FILLABLE,
        SHAPE_GENERIC_RFQ_XLSX,
        SHAPE_GENERIC_RFQ_PDF,
        SHAPE_GENERIC_RFQ_DOCX,
    )

    shape = classification.shape

    # CCHCS LPA IT Goods RFQ — parse as generic RFQ PDF for now (the LPA
    # template's line items live in AcroForm fields that the generic parser
    # can read). The dedicated LPA parser + row-by-row extraction is a
    # follow-up; for now we get header + rows via the field-value scan.
    if shape == SHAPE_CCHCS_IT_RFQ:
        try:
            from src.forms.generic_rfq_parser import parse_generic_rfq
            parsed = parse_generic_rfq([path])
            return (
                parsed.get("items", []),
                parsed.get("header", {}),
                None,
            )
        except Exception as e:
            return [], {}, f"cchcs_it_rfq (via generic parser) crashed: {e}"

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

    # Fire-and-forget: refresh web MSRP for any catalog-matched items
    # whose price is stale. Scoped to just THIS record's items — no full-
    # catalog sweep, no scheduled cron. By the time the operator opens
    # the PC/RFQ to price it, the catalog reflects current market MSRP.
    # Rule honored: "MSRP/list price always used as cost, because quotes
    # are 45 days valid and discounts could expire."
    try:
        from src.agents.product_catalog import refresh_prices_for_items_async
        refresh_prices_for_items_async(items, context=f"ingest_{record_type}_{record['id'][:8]}")
    except Exception as _e:
        log.debug("refresh_prices_for_items_async skipped: %s", _e)

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
        # Re-parse replaces items — refresh catalog MSRP for the new set too
        if items:
            try:
                from src.agents.product_catalog import refresh_prices_for_items_async
                refresh_prices_for_items_async(items, context=f"reparse_pc_{record_id[:8]}")
            except Exception as _e:
                log.debug("refresh_prices_for_items_async skipped on reparse: %s", _e)
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
        if items:
            try:
                from src.agents.product_catalog import refresh_prices_for_items_async
                refresh_prices_for_items_async(items, context=f"reparse_rfq_{record_id[:8]}")
            except Exception as _e:
                log.debug("refresh_prices_for_items_async skipped on reparse: %s", _e)
    return record_id


# ── Triangulated linker ─────────────────────────────────────────────────

def _run_triangulated_linker(
    rfq_id: str,
    classification: "RequestClassification",  # noqa: F821
    rfq_items: List[Dict[str, Any]],
) -> Tuple[str, str, float]:
    """Link an incoming RFQ to an existing PC.

    Default rule: **≥2 of 4 anchors** match.
      ANCHOR 1: same agency (from classification)
      ANCHOR 2: same solicitation number (substring match either way)
      ANCHOR 3: same institution (tight string equality)
      ANCHOR 4: item similarity ≥0.75 on ≥60% of items

    Strong single-anchor override (added 2026-04-22, RFQ #10840486 incident):
    item coverage == 100% AND mean similarity ≥0.90 alone qualifies. Verbatim
    items on both sides is a stronger signal than any 2-anchor combo — when
    the prior PC is an informal price check with no agency/sol/institution
    captured, item identity is the only thing that can link them.

    Agency fallback (added 2026-04-22): PCs pre-dating classification
    enrichment may have blank `agency` but a valid `institution`. Resolve
    institution→agency on the fly via institution_resolver before comparing,
    so anchor #1 can still fire on legacy records.

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

        # Fallback: PC missing (or "other") agency but has institution —
        # resolve on the fly. institution_resolver.resolve() returns a dict
        # with "agency" key (or None when no match). QuoteRequest.get_agency()
        # normalizes a blank field to "other", so check for both.
        if pc_agency in ("", "other") and pc_inst:
            try:
                from src.core.institution_resolver import resolve as _resolve_inst
                resolved = _resolve_inst(pc_inst)
                if isinstance(resolved, dict):
                    resolved_agency = (resolved.get("agency") or "").lower()
                    if resolved_agency:
                        pc_agency = resolved_agency
            except Exception as _e:
                log.debug("institution_resolver unavailable: %s", _e)

        anchors = []
        item_coverage = 0.0
        item_sim_mean = 0.0

        # ANCHOR 1: same agency
        if rfq_agency and rfq_agency != "other" and pc_agency and pc_agency == rfq_agency:
            anchors.append("agency")
        # ANCHOR 2: same solicitation
        if rfq_sol and pc_sol and (rfq_sol == pc_sol
                                    or rfq_sol in pc_sol
                                    or pc_sol in rfq_sol):
            anchors.append("solicitation")
        # ANCHOR 3: same institution (tight — no fuzzy substring)
        if rfq_inst and pc_inst and rfq_inst == pc_inst:
            anchors.append("institution")
        # ANCHOR 4: item similarity — track coverage AND mean sim, not just yes/no.
        if rfq_items and pc_items:
            matched = 0
            sim_sum = 0.0
            scored_items = 0
            for ri in rfq_items:
                rd = (ri.get("description", "") or "").lower().strip()
                if not rd or len(rd) < 5:
                    continue
                best_sim = 0.0
                for pi in pc_items:
                    pd = (pi.get("description", pi.get("desc", "")) or "").lower().strip()
                    if not pd:
                        continue
                    s = SequenceMatcher(None, rd, pd).ratio()
                    if s > best_sim:
                        best_sim = s
                if best_sim > 0.75:
                    matched += 1
                sim_sum += best_sim
                scored_items += 1
            if scored_items > 0:
                item_coverage = matched / scored_items
                item_sim_mean = sim_sum / scored_items
                if item_coverage >= 0.60:
                    anchors.append(f"items({item_coverage:.0%})")

        # Candidate qualifies on (a) ≥2 anchors OR (b) strong verbatim item match.
        # Strong item match requires item-count symmetry too — a 1-item RFQ
        # that finds its item inside a 10-item PC should NOT link on items
        # alone (that big PC was quoting a different batch that just happens
        # to include this item).
        strong_item_match = (
            bool(rfq_items)
            and bool(pc_items)
            and item_coverage >= 1.0
            and item_sim_mean >= 0.90
            and abs(len(rfq_items) - len(pc_items)) <= 1
        )
        if len(anchors) >= 2 or strong_item_match:
            candidates.append({
                "pc_id": pc_id,
                "anchors": anchors if anchors else [
                    f"items-verbatim({item_sim_mean:.2f})"
                ],
                "score": len(anchors) if len(anchors) >= 2 else 2,
                "item_coverage": item_coverage,
                "item_sim_mean": item_sim_mean,
                "strong_item": strong_item_match,
                "created_at": pc.get("created_at", ""),
            })

    if not candidates:
        return (
            "",
            "no triangulated match (need >=2 anchors or verbatim items)",
            0.0,
        )

    # Solicitation number is the strongest anchor — if exactly one candidate
    # has it, return that one regardless of anchor count.
    sol_candidates = [c for c in candidates if "solicitation" in c["anchors"]]
    if len(sol_candidates) == 1:
        best = sol_candidates[0]
    else:
        # Tie-break: verbatim item-match wins over weak 2-anchor matches
        # (item identity is the ground truth for "is this the same quote"),
        # then most anchors, then most-recent created_at.
        candidates.sort(key=lambda c: (
            0 if c["strong_item"] else 1,
            -c["item_sim_mean"],
            -c["score"],
            -_ts(c["created_at"]),
        ))
        best = candidates[0]
    return (
        best["pc_id"],
        "+".join(best["anchors"]),
        round(max(best["score"], 2) / 4.0, 2),  # min 0.5 for any link
    )


def _ts(s: str) -> float:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


# ── Post-link pricing copy (Bundle-6 PR-6a) ─────────────────────────────────
#
# When the triangulated linker binds an RFQ to a PC, the PC holds the pricing
# Mike already decided on for that buyer + those items. Previously the link
# was passive: the operator had to navigate to the PC to read the price or
# re-enter it on the RFQ (audit 2026-04-22, RFQ 9ad8a0ac / PC 5063d1cd).
# This helper copies PC item['pricing'] onto matching RFQ items by
# description similarity. Idempotent: items stamped pricing_copied_from_pc
# are left alone so the operator's manual edits survive a re-run.

def _copy_pc_pricing_to_rfq(
    rfq_id: str,
    pc_id: str,
    rfq_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Copy per-item pricing subdicts from a linked PC onto the RFQ's items.

    Matching is by lowercased-description SequenceMatcher ratio >= 0.75
    (same threshold the triangulated linker uses for item-coverage — a
    tighter bar than fuzzy token match).

    Idempotency rules:
      * An RFQ item already marked pricing_copied_from_pc=<any> is NOT
        touched (preserves manual edits and prior copies).
      * An RFQ item that already has non-zero price_per_unit OR
        supplier_cost OR pricing.recommended_price is NOT touched (caller
        already priced it, don't clobber).

    Side-effects:
      * Mutates `rfq_items` in place so the caller's `items` reference
        updates — process_buyer_request stores that reference on the record.
      * Loads + saves the RFQ record via the DAL so a persisted copy lands
        in SQLite even when the caller doesn't persist items again.

    Returns {copied, skipped, reason}.
    """
    from difflib import SequenceMatcher
    report = {"copied": 0, "skipped": 0, "reason": ""}
    try:
        from src.api.dashboard import _load_price_checks, load_rfqs, _save_single_rfq
    except ImportError:
        report["reason"] = "dashboard imports unavailable"
        return report

    pcs = _load_price_checks()
    pc = pcs.get(pc_id)
    if not pc:
        report["reason"] = f"pc {pc_id} not found"
        return report

    pc_items = pc.get("items") or []
    if not isinstance(pc_items, list) or not pc_items:
        report["reason"] = "pc has no items"
        return report

    def _has_pricing(item: Dict[str, Any]) -> bool:
        """True when the RFQ item is already priced by a human or prior run."""
        if item.get("pricing_copied_from_pc"):
            return True
        if (item.get("price_per_unit") or 0) > 0:
            return True
        if (item.get("supplier_cost") or 0) > 0:
            return True
        p = item.get("pricing") or {}
        if isinstance(p, dict) and (p.get("recommended_price") or 0) > 0:
            return True
        return False

    def _best_pc_match(rfq_desc: str) -> Optional[Dict[str, Any]]:
        if not rfq_desc or len(rfq_desc) < 5:
            return None
        rd = rfq_desc.lower().strip()
        best_item = None
        best_sim = 0.0
        for pi in pc_items:
            if not isinstance(pi, dict):
                continue
            pd = (pi.get("description") or pi.get("desc") or "").lower().strip()
            if not pd:
                continue
            s = SequenceMatcher(None, rd, pd).ratio()
            if s > best_sim:
                best_sim = s
                best_item = pi
        if best_sim >= 0.75:
            return best_item
        return None

    for rfq_item in rfq_items:
        if not isinstance(rfq_item, dict):
            report["skipped"] += 1
            continue
        if _has_pricing(rfq_item):
            report["skipped"] += 1
            continue
        pc_match = _best_pc_match(rfq_item.get("description", "") or "")
        if not pc_match:
            report["skipped"] += 1
            continue
        src_pricing = pc_match.get("pricing") or {}
        # Copy the pricing subdict only when it carries real values — an
        # empty dict from an unpriced PC item isn't useful and would look
        # like "copied" when it isn't.
        has_real_values = False
        if isinstance(src_pricing, dict):
            for k in ("recommended_price", "amazon_price",
                      "scprs_last_price", "unit_cost", "catalog_cost"):
                if (src_pricing.get(k) or 0) > 0:
                    has_real_values = True
                    break
        pc_unit = pc_match.get("price_per_unit") or pc_match.get("unit_price") or 0
        pc_cost = pc_match.get("supplier_cost") or pc_match.get("vendor_cost") or 0
        if not has_real_values and pc_unit <= 0 and pc_cost <= 0:
            report["skipped"] += 1
            continue
        # Apply: flat fields FIRST (so UI tables that read unit_price see
        # the value), THEN the nested pricing dict (for price_source badges).
        if pc_unit > 0:
            rfq_item["price_per_unit"] = pc_unit
        if pc_cost > 0:
            rfq_item["supplier_cost"] = pc_cost
        mk = pc_match.get("markup_pct")
        if mk:
            rfq_item["markup_pct"] = mk
        if isinstance(src_pricing, dict) and src_pricing:
            # Don't shadow anything already on the RFQ item's pricing dict —
            # merge, not clobber. Prior keys (from enrichment runs) win.
            dst_pricing = rfq_item.get("pricing") or {}
            if not isinstance(dst_pricing, dict):
                dst_pricing = {}
            for k, v in src_pricing.items():
                dst_pricing.setdefault(k, v)
            rfq_item["pricing"] = dst_pricing
        rfq_item["pricing_copied_from_pc"] = pc_id
        rfq_item["pricing_copied_at"] = datetime.now().isoformat()
        report["copied"] += 1

    # Persist once at the end so a single save writes all item copies.
    if report["copied"]:
        try:
            rfqs = load_rfqs()
            rfq = rfqs.get(rfq_id)
            if rfq is not None:
                rfq["line_items"] = rfq_items
                rfq["items"] = rfq_items
                rfq["pricing_copied_from_pc"] = pc_id
                rfq["pricing_copied_at"] = datetime.now().isoformat()
                _save_single_rfq(rfq_id, rfq)
        except Exception as _e:
            log.debug("pricing-copy persist suppressed: %s", _e)

    log.info("pricing-copy rfq=%s pc=%s copied=%d skipped=%d",
             rfq_id, pc_id, report["copied"], report["skipped"])
    return report


__all__ = [
    "process_buyer_request",
    "IngestResult",
]
