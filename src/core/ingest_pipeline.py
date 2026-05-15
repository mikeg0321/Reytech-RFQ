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
import re
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
    # PR-A substrate (2026-05-11): structured signals from
    # `_dispatch_parser` that the operator surface needs to see.
    # `needs_review=True` when Vision and base parser disagree by more
    # than the tolerance threshold.
    # `ingest_warnings` carries the structured warning dicts
    # (kind/detail/counts) so the UI can render a banner that names
    # the specific class of disagreement instead of a free-text blob.
    # `needs_manual_pull=True` when the Proofpoint SecureMessage
    # auto-pull (Step 7) was skipped or failed and the operator must
    # open the portal manually to retrieve the real RFQ PDF.
    # `proofpoint_portal_url` carries the extracted portal link so the
    # operator surface can render it as a one-click handoff.
    needs_review: bool = False
    ingest_warnings: List[Dict[str, str]] = field(default_factory=list)
    needs_manual_pull: bool = False
    proofpoint_portal_url: str = ""

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
            "needs_review": bool(self.needs_review),
            "ingest_warnings": [dict(w) for w in self.ingest_warnings],
            "needs_manual_pull": bool(self.needs_manual_pull),
            "proofpoint_portal_url": self.proofpoint_portal_url,
        }


def process_buyer_request(
    files: List[str] = None,
    email_body: str = "",
    email_subject: str = "",
    email_sender: str = "",
    email_uid: str = "",
    existing_record_id: str = "",
    existing_record_type: str = "",
    email_received_at: str = "",
    gmail_thread_id: str = "",
    gmail_message_id: str = "",
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
        email_received_at: RFC822 Date header from the buyer email (Gmail
            poller passes msg["Date"]). When provided, persisted as
            `received_at`; otherwise falls back to ingest time.

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

    # ── Step 2b: Proofpoint SecureMessage handler ──
    # PR-A Step 8 (2026-05-11). When the classifier identifies a
    # SecureMessage wrapper email, the real RFQ PDF is behind the
    # Proofpoint Encryption portal. Try the auto-pull (Step 7); on
    # success, swap the downloaded files in as the new attachment list
    # AND re-classify against them so the downstream pipeline sees
    # the actual RFQ shape (cchcs_packet / ams_704 / generic). On
    # failure, mark `needs_manual_pull=True` and let the record save
    # with empty items — the operator gets a one-click portal link
    # plus a "pull this manually" banner.
    if classification.shape == "proofpoint_securemessage":
        portal_url = ""
        try:
            from src.agents.proofpoint_pull import (
                extract_portal_url, is_available, pull_via_url,
            )
            portal_url = extract_portal_url(email_body) or ""
            result.proofpoint_portal_url = portal_url
            if portal_url and is_available():
                log.info(
                    "ingest proofpoint: auto-pull attempting (%s)",
                    portal_url[:80],
                )
                downloaded = pull_via_url(portal_url)
                if downloaded:
                    log.info(
                        "ingest proofpoint: auto-pull returned %d file(s) — "
                        "re-classifying", len(downloaded),
                    )
                    # Replace the file list with the real RFQ files
                    # and re-run classify so the downstream pipeline
                    # sees the actual shape (cchcs / 704 / generic).
                    files = downloaded
                    try:
                        from src.core.request_classifier import classify_request as _reclassify
                        classification = _reclassify(
                            attachments=files,
                            email_body=email_body,
                            email_subject=email_subject,
                            email_sender=email_sender,
                        )
                        result.classification = classification.to_dict()
                        result.reasons.append(
                            f"proofpoint: auto-pull → re-classified as {classification.shape}"
                        )
                        # Re-evaluate record_type with the new shape.
                        record_type = "pc" if classification.is_quote_only else "rfq"
                        result.record_type = record_type
                    except Exception as _rce:
                        log.error("proofpoint re-classify failed: %s", _rce, exc_info=True)
                        result.warnings.append(f"proofpoint re-classify: {_rce}")
                else:
                    log.warning(
                        "ingest proofpoint: auto-pull returned 0 files — "
                        "falling back to needs_manual_pull"
                    )
                    result.needs_manual_pull = True
                    result.reasons.append(
                        "proofpoint: auto-pull empty — manual portal visit required"
                    )
            else:
                # Either no portal URL extractable, or auto-pull not
                # configured (no creds / flag off / no playwright).
                result.needs_manual_pull = True
                if not portal_url:
                    result.reasons.append(
                        "proofpoint: no portal URL extractable from email body"
                    )
                else:
                    result.reasons.append(
                        "proofpoint: auto-pull not available — manual portal visit required"
                    )
        except Exception as _pe:
            log.error("proofpoint handler crashed: %s", _pe, exc_info=True)
            result.warnings.append(f"proofpoint: {_pe}")
            result.needs_manual_pull = True

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

    # ── Step 3a: extract substrate diagnostics from header ──
    # `_dispatch_parser` stashes `_needs_review` + `_ingest_warnings` on
    # the header dict so they ride along with the parse result without
    # changing the parser API. Pop them out here so they propagate to
    # both the IngestResult (for the API/UI) and the persisted record
    # (so the operator banner survives a page reload). Keep them off
    # the header that the record stores under top-level fields —
    # otherwise downstream readers that don't know about them will
    # serialize a `header._needs_review` blob no one reads.
    ingest_warnings: List[Dict[str, str]] = []
    needs_review = False
    if isinstance(header, dict):
        ingest_warnings = list(header.pop("_ingest_warnings", None) or [])
        needs_review = bool(header.pop("_needs_review", False))
    result.ingest_warnings = list(ingest_warnings)
    result.needs_review = needs_review
    if needs_review:
        result.reasons.append(
            "needs_review: Vision/base parser disagreement above threshold"
        )

    # ── Step 3a-multi: Multi-attachment item union (P000 substrate #3) ──
    # The classifier's pricing-page tiebreak (PR #921) picks the BEST
    # single PDF as primary. But buyer bundles can split items across
    # multiple PDFs — e.g., a 30-item pricing page sent as Attachment-B-1
    # + Attachment-B-2 (continuation). Without this union step, only the
    # primary's items land on the record.
    #
    # Rule: after the primary parse, run Vision on EVERY other buyer-RFQ
    # PDF in `files` (skip the primary, skip non-RFQ-shaped PDFs like
    # `Bidder_Declaration.pdf`). Dedup union into the primary list using
    # a description+qty signature. Items contributed by sibling
    # attachments are tagged `_source_attachment` for operator audit.
    #
    # Flag-gated: ingest.multi_attachment_union_enabled (default True).
    # Conservative cap: skip when `primary_path` is None OR `files` has
    # <2 PDFs (no siblings to scan).
    try:
        from src.core.flags import get_flag
        _multi_enabled = get_flag("ingest.multi_attachment_union_enabled", True)
    except Exception:
        _multi_enabled = True

    if (
        _multi_enabled
        and primary_path
        and files
        and len([f for f in files if f.lower().endswith(".pdf")]) >= 2
    ):
        try:
            extra_items = _multi_attachment_vision_union(
                primary_path=primary_path,
                all_files=files,
                classification=classification,
                primary_items=items,
            )
            if extra_items:
                items = items + extra_items
                result.reasons.append(
                    f"multi-attach union: +{len(extra_items)} items "
                    f"from sibling RFQ PDFs"
                )
                log.info(
                    "multi_attach_union: +%d items from sibling RFQ PDFs "
                    "(primary=%s)",
                    len(extra_items), os.path.basename(primary_path),
                )
        except Exception as _mu:
            log.warning("multi-attachment union failed (non-fatal): %s", _mu)
            result.warnings.append(f"multi-attach-union: {_mu}")

    # ── Step 3b: body-text fallback when attachment yielded zero items ──
    # Buyers who paste the RFQ into the email body (no parseable attachment)
    # used to land as zero-item placeholders. The body extractor is regex-only
    # — production-quality preprocessor (HTML/signature strip) + 5 pattern
    # stages — so it stays fast on the ingest hot path.
    #
    # Flag-gated: ingest.body_extraction_enabled (default True). If extraction
    # yields anything, items are tagged source='email_body_regex' so downstream
    # surfaces can flag operator review.
    if not items and email_body:
        try:
            from src.core.flags import get_flag
            if get_flag("ingest.body_extraction_enabled", True):
                from src.forms.email_body_extractor import extract_items as _bx
                body_items = _bx(email_body)
                if body_items:
                    items = body_items
                    result.reasons.append(
                        f"body-extract: {len(body_items)} items from email body"
                    )
                    log.info(
                        "body_extract: rescued %d items from email body "
                        "(no parseable attachment)", len(body_items),
                    )
                else:
                    result.reasons.append("body-extract: 0 items found in email body")
        except Exception as _e:
            log.warning("body extraction failed: %s", _e)
            result.warnings.append(f"body-extract: {_e}")

    # ── Step 3c: form-code line-item filter (PR-AV1) ──
    # Buyer "Required Forms / Documents" tables get parsed as quote
    # line items by the Vision/regex parsers — rfq_efbdef4a / 25CB021
    # landed with 16 "items" where rows 7-15 were `Darfur`, `STD204`,
    # `STD843`, `CalRecycle074`, `CCC`, `Exhibit G`, `VSDS`. Operator
    # had to delete by hand or quote garbage. Detect form-code-shaped
    # rows and route them to required_forms instead of line_items.
    detected_form_ids: List[str] = []
    if items:
        try:
            from src.agents.form_code_filter import filter_form_codes
            real_items, detected_form_ids = filter_form_codes(items)
            if detected_form_ids:
                log.info(
                    "form_code_filter: dropped %d form-code rows → form_ids=%s",
                    len(items) - len(real_items), detected_form_ids,
                )
                result.reasons.append(
                    f"form-code filter: kept {len(real_items)}/{len(items)} items, "
                    f"routed {len(detected_form_ids)} form-codes to required_forms"
                )
                items = real_items
        except Exception as _fce:
            log.debug("form_code_filter suppressed: %s", _fce)

    # Stash detected form_ids on the header so the record-creator can
    # union them into the requirements envelope.
    if detected_form_ids and isinstance(header, dict):
        existing = list(header.get("_detected_form_ids") or [])
        for fid in detected_form_ids:
            if fid not in existing:
                existing.append(fid)
        header["_detected_form_ids"] = existing

    result.items_parsed = len(items)

    # ── Step 4: create or update the record ──
    try:
        # Pass the FULL list of files (not just primary_path) so the
        # template registration step inside both record-handlers can run
        # `identify_attachments` over every sibling attachment. Pre-fix,
        # only the primary file was ever classified — buyer emails with
        # 703B + 704B + bidpkg as separate PDFs landed with only one
        # template slot filled (or none, if the primary was the wrong shape).
        # Mike P0 2026-05-06 RFQ a5b09b56 hit this: buyer attached
        # AMS_703B_*.pdf + AMS_704B_*.pdf + BID_PACKAGE_*.pdf and operator
        # saw "Missing required templates: 704B" at package-generation
        # time even though the 704B was right there in the email.
        all_paths = list(files) if files else None
        if existing_record_id and existing_record_type:
            record_id = _update_existing_record(
                existing_record_id, existing_record_type,
                items, header, classification, primary_path,
                all_paths=all_paths,
                gmail_thread_id=gmail_thread_id,
                gmail_message_id=gmail_message_id,
                needs_review=needs_review,
                ingest_warnings=ingest_warnings,
                needs_manual_pull=result.needs_manual_pull,
                proofpoint_portal_url=result.proofpoint_portal_url,
            )
        else:
            record_id = _create_record(
                record_type, items, header, classification,
                primary_path, email_subject, email_sender, email_uid,
                email_body=email_body,
                email_received_at=email_received_at,
                gmail_thread_id=gmail_thread_id,
                gmail_message_id=gmail_message_id,
                all_paths=all_paths,
                needs_review=needs_review,
                ingest_warnings=ingest_warnings,
                needs_manual_pull=result.needs_manual_pull,
                proofpoint_portal_url=result.proofpoint_portal_url,
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
    base_items: List[Dict[str, Any]] = []
    base_header: Dict[str, Any] = {}
    base_parser_label = "?"
    base_error: Optional[str] = None

    # ── Stage 1: base parser dispatch by shape ──
    # The base parser provides:
    #   1. Item-extraction signal for sanity-checking Vision
    #   2. Header fields with agency-specific tuning (institution,
    #      requestor_name, delivery_zip) that the generic parser learns
    #      over time and Vision doesn't always know about
    #   3. Primary source for DOCX/XLSX (Vision can't read those)
    # For PDFs, Stage 2 makes Vision the PRIMARY items source.

    if shape == SHAPE_CCHCS_IT_RFQ:
        base_parser_label = "generic_rfq (cchcs_it_rfq)"
        try:
            from src.forms.generic_rfq_parser import parse_generic_rfq
            parsed = parse_generic_rfq([path])
            base_items = parsed.get("items", []) or []
            base_header = parsed.get("header", {}) or {}
        except Exception as e:
            base_error = f"cchcs_it_rfq (via generic parser) crashed: {e}"

    elif shape == SHAPE_CCHCS_PACKET:
        base_parser_label = "cchcs_packet"
        try:
            from src.forms.cchcs_packet_parser import parse_cchcs_packet
            parsed = parse_cchcs_packet(path)
            if not parsed.get("ok"):
                base_error = parsed.get("error", "cchcs parse failed")
            else:
                base_items = parsed.get("line_items", []) or []
                base_header = parsed.get("header", {}) or {}
        except Exception as e:
            base_error = f"cchcs parser crashed: {e}"

    elif shape in (SHAPE_PC_704_DOCX, SHAPE_PC_704_PDF_FILLABLE,
                   SHAPE_PC_704_PDF_DOCUSIGN):
        base_parser_label = "ams_704"
        try:
            from src.forms.price_check import parse_ams704
            parsed = parse_ams704(path)
            if parsed.get("error"):
                base_error = parsed.get("error")
            else:
                base_items = parsed.get("line_items", []) or []
                base_header = parsed.get("header", {}) or {}
        except Exception as e:
            base_error = f"ams704 parser crashed: {e}"

    elif shape in (SHAPE_GENERIC_RFQ_PDF, SHAPE_GENERIC_RFQ_DOCX,
                   SHAPE_GENERIC_RFQ_XLSX):
        base_parser_label = "generic_rfq"
        try:
            from src.forms.generic_rfq_parser import parse_generic_rfq
            parsed = parse_generic_rfq([path])
            base_items = parsed.get("items", []) or []
            base_header = parsed.get("header", {}) or {}
        except Exception as e:
            base_error = f"generic parser crashed: {e}"

    else:
        return [], {}, f"no parser for shape {shape}"

    # ── Stage 2: Vision-primary item extraction (PDF only) ──
    # Mike directive 2026-05-11 after the rfq_0ebe242f phantom-item
    # incident: "Vision is verification right? ... relying on regex could
    # just be creating bad data... garbage in, garbage out... I don't
    # want any of that."
    #
    # Architecture: for PDF shapes, Vision is the TRUTH source for
    # items. Regex (the base parser) is now a SANITY-CHECK signal,
    # consulted only to flag disagreements for operator review.
    #
    # Decision matrix:
    #   - Both parsers ran successfully:
    #       * If |vision - base| > threshold: needs_review=True,
    #         warnings record both counts. Vision items win.
    #       * Else: Vision items used (no review needed).
    #   - Vision unavailable: fall back to base items, tag
    #     vision_skipped=True warning.
    #   - Base errored, Vision succeeded: Vision recovers the parse.
    #   - Both failed: propagate base error.
    #
    # Non-PDF paths (DOCX/XLSX): base parser stays primary, Vision
    # can't read those formats. Tag vision_unsupported_format warning.

    is_pdf = path.lower().endswith(".pdf")
    warnings: List[Dict[str, str]] = []
    needs_review = False

    if not is_pdf:
        # DOCX / XLSX / etc: Vision can't read these; base parser is
        # the authoritative source. No verification possible.
        if base_items:
            warnings.append({
                "kind": "vision_unsupported_format",
                "detail": f"Vision verification skipped for {os.path.splitext(path)[1]} — base parser is sole source.",
            })
        # Annotate header with substrate diagnostics for the record.
        if warnings:
            base_header.setdefault("_ingest_warnings", []).extend(warnings)
        if base_error and not base_items:
            return [], base_header, base_error
        return (base_items, base_header, None)

    # ── PDF path: try Vision (primary) ──
    try:
        vision_items = _vision_primary_extract(path)
    except Exception as _ve:
        log.debug("vision primary call suppressed: %s", _ve)
        vision_items = None

    # Vision unavailable / errored / quota-capped → fall back to base.
    if vision_items is None:
        warnings.append({
            "kind": "vision_skipped",
            "detail": "Vision AI unavailable; relying on base parser items only. Operator should spot-check.",
        })
        base_header.setdefault("_ingest_warnings", []).extend(warnings)
        if base_error and not base_items:
            return [], base_header, base_error
        return (base_items, base_header, None)

    # Both signals present. Decide based on disagreement.
    primary_items, primary_warnings, primary_needs_review = (
        _reconcile_vision_and_base(
            vision_items=vision_items,
            base_items=base_items,
            base_parser_label=base_parser_label,
            path=path,
        )
    )
    warnings.extend(primary_warnings)
    if primary_needs_review:
        needs_review = True

    # Merge headers — Vision provides defaults, base parser overrides
    # on agency-specific fields where it has learned tuning over time.
    merged_header = _merge_headers(vision_header={}, base_header=base_header)
    merged_header.setdefault("_ingest_warnings", []).extend(warnings)
    if needs_review:
        merged_header["_needs_review"] = True

    if not primary_items and base_error:
        # Both Vision and base failed to surface usable items.
        return [], merged_header, base_error

    return (primary_items, merged_header, None)


def _vision_primary_extract(path: str) -> Optional[List[Dict[str, Any]]]:
    """Run Vision AI as the PRIMARY items extractor on a PDF.

    Returns the items list from `parse_with_vision`, or None if Vision
    is unavailable / errored / quota-capped (caller falls back to base
    parser items in that case).

    Mike P000 2026-05-11: Vision is the truth source for content
    extraction (proven against rfq_8efe9fae 5 -> 15, rfq_0ebe242f
    phantom-item drop, pc_5728f934 8 -> 10). The base parser stays in
    place for header tuning + DOCX/XLSX coverage + sanity-check signal.
    """
    try:
        from src.forms.vision_parser import parse_with_vision, is_available
        if not is_available():
            return None
        parsed = parse_with_vision(path)
        if not parsed:
            return None
        items = parsed.get("line_items") or parsed.get("items") or []
        return items if items else []
    except Exception as e:
        log.debug("vision primary extract suppressed: %s", e)
        return None


def _item_signature(item: Dict[str, Any]) -> str:
    """Stable dedup signature for an extracted line item.

    Used by the multi-attachment Vision union to skip items already
    present in the primary parse. Normalizes whitespace + case + qty
    type so `'2 EA POWER SUPPLY'` and `'2.0  ea  power supply'`
    collide. MFG# is included when present because two items with the
    same description but different part numbers are legitimately
    different items (e.g., size variants).
    """
    desc = (item.get("description") or "").strip().lower()
    desc = re.sub(r"\s+", " ", desc)[:120] if desc else ""
    try:
        qty = float(item.get("qty") or item.get("quantity") or 0)
    except (ValueError, TypeError):
        qty = 0.0
    mfg = (
        item.get("item_number")
        or item.get("mfg_number")
        or item.get("part_number")
        or ""
    )
    mfg = str(mfg).strip().upper()
    return f"{desc}|qty={qty:g}|mfg={mfg}"


def _multi_attachment_vision_union(
    primary_path: str,
    all_files: List[str],
    classification: "RequestClassification",  # noqa: F821
    primary_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Run Vision on sibling buyer-RFQ PDFs and return any items the
    primary parse missed (deduped against `primary_items`).

    P000 substrate #3 (Mike's email-contract spec 2026-05-11): "the
    app must upload every single attachment and review it". The
    pricing-page tiebreak picks one PDF as primary; this union step
    closes the gap when items are split across siblings (Attachment-B-1
    + Attachment-B-2, multi-page bundles that arrive as separate PDFs).

    Conservative scope:
      - Skip the primary itself.
      - Skip non-PDF files (DOCX/XLSX/etc — base parser already covered).
      - Skip PDFs whose classifier shape is NOT generic-RFQ-PDF (don't
        Vision-parse `Bidder_Declaration.pdf`, `DVBE_Declaration.pdf`,
        `Darfur_Contracting_Act.pdf`, etc — those are blank forms not
        item tables).
      - Cap at 4 sibling Vision calls to bound cost/latency on bundles
        with many attachments.
      - Each contributed item gets `_source_attachment: <basename>` and
        `_extraction_source: 'vision_sibling'` for operator traceability.

    Performance follow-on 2026-05-12:
      - Reads classifier's `_per_file_info` cache instead of calling
        `_classify_pdf` again per sibling (~200-500ms saved per file).
      - Runs the eligible Vision calls in parallel via a thread pool
        (Vision API is I/O bound — threads beat sequential awaits).
        4 sibling calls go from ~20-60s wall-clock to ~5-15s.
    """
    if not primary_path or not all_files:
        return []

    try:
        from src.forms.vision_parser import is_available
        if not is_available():
            return []
    except Exception:
        return []

    # Build the dedup signature set from items the primary parse found.
    seen_sigs = set()
    for it in primary_items or []:
        if isinstance(it, dict):
            seen_sigs.add(_item_signature(it))

    # Lazy import — classifier module is heavy.
    from src.core.request_classifier import (
        _classify_pdf,
        SHAPE_GENERIC_RFQ_PDF,
    )

    # Pull the per-PDF classification cache the classifier already built
    # during `classify_request`. Falls back to live _classify_pdf when
    # cache misses (defensive — older callers may pass a synthetic
    # classification with no cache).
    _cache = getattr(classification, "_per_file_info", None) or {}

    def _resolve_shape(path: str) -> Tuple[str, Dict[str, Any]]:
        """Read shape + info from cache, fall back to live classify."""
        hit = _cache.get(os.path.basename(path))
        if hit and isinstance(hit, dict):
            return hit.get("shape", ""), (hit.get("info") or {})
        try:
            return _classify_pdf(path)
        except Exception:
            return "", {}

    primary_basename = os.path.basename(primary_path)
    MAX_SIBLING_CALLS = 4

    # ── Stage 1: filter candidates (no Vision yet) ──
    candidates: List[str] = []
    for path in all_files:
        if not path or not os.path.exists(path):
            continue
        if os.path.basename(path) == primary_basename:
            continue
        if not path.lower().endswith(".pdf"):
            continue
        if len(candidates) >= MAX_SIBLING_CALLS:
            log.info(
                "multi_attach_union: hit MAX_SIBLING_CALLS=%d cap, "
                "skipping remaining PDFs", MAX_SIBLING_CALLS,
            )
            break

        shape, info = _resolve_shape(path)
        if shape != SHAPE_GENERIC_RFQ_PDF:
            continue
        pricing_score = int(info.get("pricing_page_score", 0) or 0)
        fname_lower = os.path.basename(path).lower()
        looks_like_cert = any(
            kw in fname_lower for kw in (
                "bidder_decl", "bidder declaration",
                "dvbe_decl", "dvbe declaration",
                "darfur", "drug_free", "drug free",
                "postconsumer", "post_consumer", "recycled",
                "std_204", "std204", "payee_data",
                "calrecycle", "std_205", "std205",
            )
        )
        if pricing_score == 0 and looks_like_cert:
            log.debug(
                "multi_attach_union: skipping cert/blank-form PDF %s",
                os.path.basename(path),
            )
            continue
        candidates.append(path)

    if not candidates:
        return []

    # ── Stage 2: parallel Vision calls ──
    # Vision is I/O bound — a thread pool delivers near-linear speedup
    # without the complexity of async. Match worker count to candidate
    # count so we don't spin idle threads.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results: Dict[str, List[Dict[str, Any]]] = {}
    workers = min(len(candidates), MAX_SIBLING_CALLS)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_vision_primary_extract, path): path
            for path in candidates
        }
        for fut in as_completed(futures):
            path = futures[fut]
            try:
                sibling_items = fut.result() or []
            except Exception as e:
                log.debug(
                    "multi_attach_union: Vision call for %s failed: %s",
                    os.path.basename(path), e,
                )
                sibling_items = []
            results[path] = sibling_items

    # ── Stage 3: union, deduped against primary ──
    # Iterate in `candidates` order (deterministic) rather than completion
    # order so the merged item list is reproducible run-to-run on the
    # same input bundle.
    extra_items: List[Dict[str, Any]] = []
    for path in candidates:
        sibling_items = results.get(path) or []
        if not sibling_items:
            continue
        contributed = 0
        for it in sibling_items:
            if not isinstance(it, dict):
                continue
            sig = _item_signature(it)
            if sig in seen_sigs:
                continue
            seen_sigs.add(sig)
            # Tag the source so the operator can audit which attachment
            # contributed this item if the union surfaces a mistake.
            it["_source_attachment"] = os.path.basename(path)
            it["_extraction_source"] = "vision_sibling"
            extra_items.append(it)
            contributed += 1
        if contributed:
            log.info(
                "multi_attach_union: +%d items from sibling %s (Vision %d total)",
                contributed, os.path.basename(path), len(sibling_items),
            )

    return extra_items


def _reconcile_vision_and_base(
    vision_items: List[Dict[str, Any]],
    base_items: List[Dict[str, Any]],
    base_parser_label: str,
    path: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]], bool]:
    """Decide which items list to ship + whether the disagreement is
    large enough to flag for operator review.

    Returns (primary_items, warnings, needs_review).

    Rules:
      - Vision items are always the primary source on PDFs (Mike's
        directive: regex creates garbage, don't trust its count).
      - When Vision and base counts disagree by more than max(2,
        20% of max), set needs_review=True and emit a structured
        warning carrying both counts so the operator can audit.
      - Disagreements WITHIN tolerance produce no warning (most
        common case — Vision and base both report similar counts).
      - PR substrate 2026-05-12: ALSO flag review when both parsers
        return 0 items on a multi-page PDF — this is the DSH bundle
        signature (Vision parsed the cover sheet because the items
        page lives in a separate attachment). The pricing-page tiebreak
        in `classify_request` is the primary fix; this gate is the
        belt-and-suspenders for variants the tiebreak misses.
    """
    warnings: List[Dict[str, str]] = []
    v_count = len(vision_items)
    b_count = len(base_items)
    delta = abs(v_count - b_count)
    threshold = max(2, int(0.2 * max(v_count, b_count, 1)))

    # ── Page-count confidence gate ──
    # When both Vision and base extract 0 items from a >=2-page PDF,
    # something is wrong — either Vision misread, the PDF is image-only
    # and Vision skipped it, or this PDF is the wrong attachment (cover
    # sheet in a multi-attachment bundle, vendor cert, etc.). Flag for
    # operator review so the no-items state never silently auto-confirms.
    if v_count == 0 and b_count == 0:
        try:
            page_count = _pdf_page_count(path)
        except Exception:
            page_count = 0
        if page_count >= 1:
            warnings.append({
                "kind": "zero_items_on_pdf",
                "detail": (
                    f"Both Vision and {base_parser_label} returned 0 items "
                    f"on a {page_count}-page PDF ({os.path.basename(path)}). "
                    f"Possible causes: wrong attachment selected as primary, "
                    f"image-only scan without OCR, or items table the parser "
                    f"didn't recognize. Operator should review."
                ),
                "vision_count": "0",
                "base_count": "0",
                "page_count": str(page_count),
                "base_parser": base_parser_label,
            })
            log.warning(
                "ingest reconcile: zero items extracted from %s (%d-page PDF) — needs_review",
                os.path.basename(path), page_count,
            )
            return (vision_items, warnings, True)

    # ── Low-density confidence gate (P000 substrate #2 — full form) ──
    # The zero-items gate above catches binary failures. This gate
    # catches the silent failure class Mike named on 2026-05-11: the
    # CalVet 4-page RFQ where the parser extracted 5 items when the
    # real RFQ had 15-16. Rule: pages * MIN_ITEMS_PER_PAGE > extracted
    # ⇒ suspect missed items, flag for review.
    #
    # Threshold tuning: real RFQ pricing pages observed in the
    # codebase carry 4-8 line items per page (CCHCS LPA, DSH ATTACHMENT
    # B, CalVet equivalents). A floor of 2 items/page is the
    # conservative "almost-certainly-missing-rows" line; anything
    # below that on a multi-page PDF deserves a second look.
    primary_count = max(v_count, b_count)
    if primary_count > 0 and primary_count < 100:  # 100 cap = sanity guard
        try:
            page_count = _pdf_page_count(path)
        except Exception:
            page_count = 0
        # Only fire on multi-page PDFs (1-page low-density is normal —
        # a single-item RFQ is legitimate).
        MIN_ITEMS_PER_PAGE = 2.0
        if page_count >= 2:
            density = primary_count / page_count
            if density < MIN_ITEMS_PER_PAGE:
                warnings.append({
                    "kind": "low_item_density",
                    "detail": (
                        f"Only {primary_count} items extracted from a "
                        f"{page_count}-page PDF ({os.path.basename(path)}) "
                        f"— {density:.1f} items/page, below the "
                        f"{MIN_ITEMS_PER_PAGE:.0f} items/page floor. "
                        f"Probable missed rows on a multi-page items table. "
                        f"Operator should review against the source PDF."
                    ),
                    "vision_count": str(v_count),
                    "base_count": str(b_count),
                    "page_count": str(page_count),
                    "items_per_page": f"{density:.2f}",
                    "base_parser": base_parser_label,
                })
                log.warning(
                    "ingest reconcile: low item density %.2f items/page on "
                    "%s (%d items, %d pages) — needs_review",
                    density, os.path.basename(path), primary_count, page_count,
                )
                # Continue to the count_disagreement check below — both
                # warnings can fire on the same parse. Don't early-return.
                _low_density_review = True
            else:
                _low_density_review = False
        else:
            _low_density_review = False
    else:
        _low_density_review = False

    if delta > threshold:
        needs_review = True
        warnings.append({
            "kind": "count_disagreement",
            "detail": (
                f"Vision found {v_count} items; {base_parser_label} "
                f"found {b_count}. Diff {delta} > threshold {threshold} "
                f"(20% of max). Operator should review which is "
                f"correct."
            ),
            "vision_count": str(v_count),
            "base_count": str(b_count),
            "base_parser": base_parser_label,
        })
        if v_count > b_count:
            log.info(
                "ingest reconcile: Vision %d > base %d on %s — "
                "needs_review (using Vision).",
                v_count, b_count, os.path.basename(path),
            )
        else:
            log.warning(
                "ingest reconcile: base %d > Vision %d on %s — "
                "needs_review (using Vision per Mike directive; base "
                "may contain phantoms or Vision may have missed).",
                b_count, v_count, os.path.basename(path),
            )
    else:
        needs_review = False
        if v_count != b_count:
            # Small disagreement — log but don't flag.
            log.debug(
                "ingest reconcile: Vision %d, base %d on %s — within "
                "tolerance, no review flag.",
                v_count, b_count, os.path.basename(path),
            )

    # Vision is primary. Always. The point of this substrate is to
    # stop trusting base-parser counts blindly.
    return (vision_items, warnings, needs_review or _low_density_review)


def _merge_headers(
    vision_header: Dict[str, Any], base_header: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge two header dicts.

    Base parser wins on agency-keyed fields where it has accumulated
    tuning (institution, requestor_name, delivery_zip, due_date). Vision
    fills any field base parser left empty. The merge is shallow but
    intentional: don't let Vision overwrite agency-keyed fields it
    doesn't have special handling for.
    """
    if not vision_header:
        return dict(base_header)
    merged = dict(vision_header)
    # Base header takes precedence on these agency-specific fields:
    for k in ("institution", "agency", "requestor_name", "requestor_email",
              "delivery_zip", "due_date", "price_check_number",
              "solicitation_number"):
        v = base_header.get(k)
        if v not in (None, ""):
            merged[k] = v
    # Fold any base-only fields in
    for k, v in base_header.items():
        if k not in merged or merged[k] in (None, ""):
            merged[k] = v
    return merged


def _vision_verify_items_if_pdf(
    path: str, base_items: list, base_header: dict,
) -> Optional[list]:
    """Run Vision AI as a verification pass against the generic parser.

    Returns the Vision-extracted items if Vision ran successfully AND
    found more items than the base parser; None otherwise (signaling
    "no upgrade — keep base").

    Runs only on PDFs. Image+office formats already use Vision directly
    in their own routes; this helper is the missing verification layer
    for the generic-RFQ-PDF shape.

    Mike P000 2026-05-11 RFQ rfq_8efe9fae: the buyer's CalVet RFQ landed
    with 5 items from generic_rfq_parser. The true count was 15-16.
    Vision AI (Claude) reliably reads multi-page PDFs and would have
    surfaced all 15. The classifier_v2 pipeline never invoked Vision
    on this shape — this helper closes that gap.
    """
    try:
        if not path.lower().endswith(".pdf"):
            return None
        # ── Heuristic gate: only spend a Vision call when there's
        # reason to suspect generic-parser missed items. The gate
        # triggers when ANY of these are true:
        #   - PDF has multiple pages (multi-page is the failure class)
        #   - Generic parser returned 0 items (full miss)
        #   - Items/page ratio is suspicious (< 3 per page)
        page_count = _pdf_page_count(path)
        base_count = len(base_items)
        items_per_page = (base_count / page_count) if page_count > 0 else base_count
        suspicious = (
            page_count > 1
            or base_count == 0
            or items_per_page < 3.0
        )
        if not suspicious:
            return None

        from src.forms.vision_parser import parse_with_vision, is_available
        if not is_available():
            log.debug("vision verify: Vision AI not available")
            return None
        log.info(
            "ingest._vision_verify: running Vision (pages=%d, base_items=%d)",
            page_count, base_count,
        )
        parsed = parse_with_vision(path)
        if not parsed:
            return None
        vision_items = parsed.get("line_items") or parsed.get("items") or []
        return vision_items if vision_items else None
    except Exception as e:
        log.debug("vision verify suppressed: %s", e)
        return None


def _pdf_page_count(path: str) -> int:
    """Cheap page count via pypdf. Returns 0 on error (caller treats
    that as "unknown" — does not block Vision verification)."""
    try:
        from pypdf import PdfReader
        return len(PdfReader(path).pages)
    except Exception:
        return 0


def _looks_like_sol_placeholder(value: str) -> bool:
    """Mirror of `dashboard._is_placeholder_number` used by the CalVet
    sol# synthesizer (above) to decide whether to synthesize.

    True when the extracted string is empty, blank, a known sentinel
    (WORKSHEET / GOOD / RFQ / QUOTE / etc.), or a single all-caps word
    of the kind regex parsers cough up on no-match. False for real
    solicitation numbers AND for already-synthesized `RT-…` strings.
    """
    if not value:
        return True
    s = str(value).strip()
    if not s:
        return True
    if s.startswith("RT-"):
        return False  # already a synthesized Reytech-internal sol#
    if s.startswith("AUTO_"):
        return True  # AUTO_<id> is itself a placeholder
    if s.isupper() and s.isalpha() and 2 <= len(s) <= 20:
        return True
    if s.lower() in {
        "unknown", "rfq", "quote", "request", "worksheet", "good",
        "bid", "vendor", "price", "check", "form",
    }:
        return True
    if s.isdigit() and len(s) <= 2:
        return True
    return False


# ── Record creation ─────────────────────────────────────────────────────


def _find_active_pc_by_number(
    pc_number: str,
    agency: str = "",
    lookback_days: int = 90,
) -> Optional[Dict[str, Any]]:
    """Return an existing non-deleted PC with the same `pc_number` + `agency`.

    PR-N (2026-05-13) — dedup-at-ingest. Mike: "I had to mark a lot
    duplicate, because they kept showing in the queue, even after sent."
    Re-polled emails with the same buyer pc_number were creating fresh PC
    rows because dedup-by-email_uid only fires when the UID matches —
    different forwards / re-sends / inbox cross-poll bypass it. This
    helper closes the gap by matching on the canonical buyer pc_number.

    Returns the existing PC dict (or None) when:
      - `pc_number` is not a placeholder (AUTO_, RT-, WORKSHEET, etc.)
      - a row exists in `price_checks` with the same pc_number AND same
        agency (case-insensitive) within `lookback_days`
      - that row is NOT itself already `duplicate` / `deleted` (don't
        chain dedup to a dup of a dup)

    Different agencies that happen to use the same numeric pc_number
    (e.g. CCHCS #10844466 + CDCR #10844466 — confirmed in prod) are
    kept separate because state agencies number their own quote
    requests independently.
    """
    if _looks_like_sol_placeholder(pc_number):
        return None
    canonical = str(pc_number).strip()
    if not canonical:
        return None
    canonical_agency = (agency or "").strip().lower()
    try:
        from datetime import timedelta as _td
        from src.core.db import get_db
    except Exception:
        return None
    cutoff = (datetime.now() - _td(days=lookback_days)).isoformat()
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, pc_number, agency, institution, status, "
                "created_at, sent_at, closed_at "
                "FROM price_checks "
                "WHERE pc_number = ? AND created_at >= ? "
                "ORDER BY created_at DESC",
                (canonical, cutoff),
            ).fetchall()
    except Exception as e:
        log.debug("dedup lookup skipped (%s): %s", canonical, e)
        return None
    # Status set we will NOT match against (already-classified noise).
    # `duplicate` excluded so we don't chain. `deleted`/`archived`
    # excluded because the operator already decided the row is gone.
    _skip_existing = {"duplicate", "deleted", "archived"}
    for r in rows:
        existing_agency = (r["agency"] or r["institution"] or "").strip().lower()
        # Match when agency is empty on either side (legacy rows missing
        # the field) OR when both sides agree.
        if canonical_agency and existing_agency and existing_agency != canonical_agency:
            continue
        existing_status = (r["status"] or "").strip().lower()
        if existing_status in _skip_existing:
            continue
        return dict(r)
    return None


def _find_active_record_by_thread(
    record_type: str,
    thread_id: str,
    agency: str = "",
    lookback_days: int = 90,
) -> Optional[Dict[str, Any]]:
    """Return an existing non-deleted PC or RFQ with the same
    `email_thread_id`. Same semantics as `_find_active_pc_by_number`
    (skip set, lookback window, agency match) but keyed on Gmail
    thread_id so buyer replies with RT-synthesized sol#s still dedup.

    `record_type` — "pc" or "rfq". Selects the table to query.
    """
    if not thread_id or not str(thread_id).strip():
        return None
    if record_type not in ("pc", "rfq"):
        return None
    canonical_thread = str(thread_id).strip()
    canonical_agency = (agency or "").strip().lower()
    table = "price_checks" if record_type == "pc" else "rfqs"
    number_col = "pc_number" if record_type == "pc" else "rfq_number"
    try:
        from datetime import timedelta as _td
        from src.core.db import get_db
    except Exception:
        return None
    cutoff = (datetime.now() - _td(days=lookback_days)).isoformat()
    try:
        with get_db() as conn:
            rows = conn.execute(
                f"SELECT id, {number_col} AS number, agency, institution, "
                f"status, created_at, sent_at, closed_at, email_thread_id "
                f"FROM {table} "
                f"WHERE email_thread_id = ? AND created_at >= ? "
                f"ORDER BY created_at DESC",
                (canonical_thread, cutoff),
            ).fetchall()
    except Exception as e:
        log.debug(
            "%s dedup lookup skipped (%s): %s",
            record_type, canonical_thread[:24], e,
        )
        return None
    _skip_existing = {"duplicate", "deleted", "archived"}
    for r in rows:
        existing_agency = (r["agency"] or r["institution"] or "").strip().lower()
        if canonical_agency and existing_agency and existing_agency != canonical_agency:
            continue
        existing_status = (r["status"] or "").strip().lower()
        if existing_status in _skip_existing:
            continue
        return dict(r)
    return None


def _find_active_rfq_by_thread(
    thread_id: str,
    agency: str = "",
    lookback_days: int = 90,
) -> Optional[Dict[str, Any]]:
    """Thin wrapper for backwards compat — delegates to
    `_find_active_record_by_thread('rfq', ...)`. See that helper's
    docstring for the canonical semantics.

    Hotfix 2026-05-13 — Mike: "i just manually sent the quote we sent
    earlier, and the queue populated back with ghost data". Buyer-reply
    emails (Mohammad@CDCR on RFQ e02b7fa6 after Mark Sent) arrived as
    NEW Gmail messages inside the SAME thread, and neither the PC nor
    RFQ ingest branch had thread-based dedup. PR-N (#959) added
    dedup-by-pc_number for PCs, but RT-synthesized sol#s are unique
    per ingest so the gate fell through. `email_thread_id` is the
    stable key across every reply in a conversation.
    """
    return _find_active_record_by_thread("rfq", thread_id, agency, lookback_days)


def _persist_all_attachments(
    record_id: str,
    record_type: str,
    paths: Optional[List[str]],
    gmail_message_id: str = "",
) -> int:
    """Persist EVERY buyer-email attachment to the `rfq_files` table so
    re-ingest / Vision-recheck scripts can find the source bytes without
    a Gmail re-fetch fallback.

    PR-A 2026-05-11: pre-fix, only the LEGACY email_poller code path
    (in `dashboard.py`) called `save_rfq_file` — and only for the
    primary file. The new `process_buyer_request` ingest path skipped
    persistence entirely. Consequence: when `scripts/reingest_rfqs_through_vision.py`
    walked rfq_files looking for the buyer's source PDF, it found
    nothing and had to fall back to Gmail re-fetch (slow, brittle,
    requires OAuth scope). This closes the gap so every ingest writes
    durable copies of all attachments at create-time.

    Category = "buyer_attachment" — distinguishes from "template"
    (operator-uploaded), "source" (legacy primary-file persistence),
    "buyer_reply" (PR-E thread-aware path), "package" (generated
    output bundles).

    Returns the count of attachments successfully persisted.
    """
    if not paths:
        return 0
    persisted = 0
    try:
        from src.api.dashboard import save_rfq_file
        import mimetypes
    except Exception as _e:
        log.debug("attachment persistence imports unavailable: %s", _e)
        return 0
    for path in paths:
        if not path:
            continue
        try:
            if not os.path.exists(path):
                continue
            with open(path, "rb") as fh:
                data = fh.read()
            if not data:
                continue
            ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
            save_rfq_file(
                record_id,
                os.path.basename(path),
                ctype,
                data,
                category="buyer_attachment",
                uploaded_by=f"ingest_{record_type}",
                gmail_message_id=gmail_message_id or "",
            )
            persisted += 1
        except Exception as e:
            log.warning(
                "ingest %s %s: attachment persist failed for %s: %s",
                record_type, record_id, path, e,
            )
    if persisted:
        log.info(
            "ingest %s %s: persisted %d attachment(s) to rfq_files",
            record_type, record_id, persisted,
        )
    return persisted


def _derive_requestor_name(email_sender: str) -> str:
    """Derive a display name from an email address when the PDF header
    didn't capture a Requestor.

    Priority:
      1. RFC822 display part — "Valentina Demidenko <addr>" → "Valentina Demidenko"
      2. Local-part heuristic — "valentina.demidenko@cdcr.ca.gov"
         → "Valentina Demidenko"
      3. Empty string when input is empty/garbage.

    Drift surface #4 from project_ams704_ingest_drift_2026_05_03.md.
    Real human display names from the PDF header always take priority
    over this fallback (see _create_record).
    """
    if not email_sender:
        return ""
    try:
        from email.utils import parseaddr
        disp, addr = parseaddr(email_sender)
        if disp:
            return disp.strip().strip('"').strip("'")
        if addr and "@" in addr:
            local = addr.split("@", 1)[0]
            return local.replace(".", " ").replace("_", " ").title()
    except Exception:
        pass
    return ""


def _attachment_filename_title(primary_path: str) -> str:
    """Derive a display-quality title from the attachment filename when the
    PDF/PRICE-CHECK number field is empty.

    Surface #17 fix (2026-05-04). Pre-fix, when the AMS 704 PRICE CHECK field
    was blank (which buyers leave blank routinely per
    project_ams704_ingest_drift_2026_05_03.md surface #1), the cascade
    immediately fell to `AUTO_<short_id>` — a useless 8-char hex hash on the
    queue. Mike's screenshot showed "AMS 704 - Heel Donut - 04.29.26" working
    on some rows (where the title slipped through some other path) and
    "AUTO_db670ad9" on others, on the same buyer.

    This helper inserts a 4th cascade step: use the email-attachment
    filename, stripped of:
      - .pdf / .docx / .xlsx extension
      - leading boilerplate prefixes ("AMS 704 -", "Quote -", "RFQ -",
        "Price Check -")
      - trailing whitespace and underscores

    Returns "" when no usable title can be derived (operator handles via
    rename UI in pc_detail.html).
    """
    if not primary_path:
        return ""
    try:
        import os as _os
        import re as _re
        base = _os.path.basename(primary_path)
        # Strip extension (handles .pdf, .docx, .xlsx, double-extensions)
        title, _ext = _os.path.splitext(base)
        # Strip leading boilerplate (case-insensitive)
        title = _re.sub(
            r"^\s*(AMS\s*704|Quote|RFQ|Price\s*Check|PC)\s*[-_:]\s*",
            "",
            title,
            flags=_re.IGNORECASE,
        )
        title = title.strip().strip("_-").strip()
        # Reject if too short, looks like a hash, or got stripped to empty
        if not title or len(title) < 3:
            return ""
        # Reject pure hex/uuid-shaped titles — they're worse than AUTO_
        if _re.fullmatch(r"[0-9a-f]{8,}", title, flags=_re.IGNORECASE):
            return ""
        return title
    except Exception:
        return ""


def _create_record(
    record_type: str,
    items: List[Dict[str, Any]],
    header: Dict[str, Any],
    classification: "RequestClassification",  # noqa: F821
    primary_path: Optional[str],
    email_subject: str,
    email_sender: str,
    email_uid: str,
    email_body: str = "",
    email_received_at: str = "",
    all_paths: Optional[List[str]] = None,
    gmail_thread_id: str = "",
    gmail_message_id: str = "",
    needs_review: bool = False,
    ingest_warnings: Optional[List[Dict[str, str]]] = None,
    needs_manual_pull: bool = False,
    proofpoint_portal_url: str = "",
) -> str:
    """Create a new PC or RFQ with the classification stored on it."""
    now = datetime.now().isoformat()
    short_id = uuid.uuid4().hex[:8]

    # Zero-items gate: a "parsed" record with no items is misleading — the
    # operator queue treats it as done when it actually needs manual triage.
    # Body-only RFQs (no parseable attachment) hit this when the body-text
    # extractor isn't wired yet. Mark needs_review so it surfaces as triage.
    #
    # PR-A 2026-05-11: also flip to needs_review when the Vision/base
    # disagreement substrate set the flag. Zero-items still wins (the
    # banner copy differs), but a record with items + disagreement
    # surfaces in the same triage queue so the operator sees both
    # classes of "look at me" before downstream actions run.
    if needs_manual_pull:
        # SecureMessage auto-pull skipped / failed — record sits in
        # operator triage with a portal link until they pull manually.
        # Distinct status from generic needs_review so the dashboard
        # can render a Proofpoint-specific banner with the URL.
        initial_status = "needs_manual_pull"
    elif not items:
        initial_status = "needs_review"
    elif needs_review:
        initial_status = "needs_review"
    else:
        initial_status = "parsed"

    # Canonicalize institution via facility_registry so two different
    # buyer-text labels for the same facility (e.g. "CSP-SAC" and
    # "CSP-Sacramento") collapse to one canonical code. Drift #3 from
    # the 2026-04-30 codebase audit. Fall back to whatever the
    # classifier/header gave us when the registry can't resolve.
    raw_institution = classification.institution or header.get("institution", "") or classification.agency or ""
    canonical_institution = raw_institution
    canonical_ship_to = ""
    try:
        from src.core.facility_registry import resolve as _resolve_facility
        _fac = _resolve_facility(raw_institution)
        if _fac:
            canonical_institution = _fac.code
            # Surface #15 (2026-05-04): when the institution resolves to a
            # known facility, populate ship_to from the canonical record at
            # ingest. Pre-fix Mike retyped "100 Prison Road, Represa, CA
            # 95671" on every CSP-SAC PC even though facility_registry
            # already had it. Per feedback_app_is_source_of_truth the registry
            # IS source-of-truth; per feedback_canonical_not_verbatim the
            # buyer's free-form text is not authoritative.
            canonical_ship_to = f"{_fac.address_line1}, {_fac.address_line2}"
    except Exception as _e:
        log.debug("facility_registry resolve skipped: %s", _e)

    # Buyer's explicit ship_to in the parsed PDF/email overrides the canonical
    # registry only when it's substantive (>3 chars, not just "CA"). A
    # blank/whitespace/2-char header value is treated as absent.
    _hdr_ship_to = (
        (header.get("ship_to") or "").strip()
        or (header.get("delivery_address") or "").strip()
    )
    resolved_ship_to = _hdr_ship_to if len(_hdr_ship_to) > 3 else canonical_ship_to

    # ── PR-AI (2026-05-14): tax resolution at ingest ────────────────
    # Mike's 3-month spec: every RFQ produces ONE structured contract
    # (due_date, buyer, ship_to, tax_rate, agency, sol#, items) ready
    # before the operator touches the page. Pre-fix every ingested
    # record landed without `tax_rate` set, surfacing as the ⚠ DEFAULT
    # jurisdiction warning on every queue row + a manual Verify Tax
    # click per detail-page load. tax_for_address() routes through
    # the canonical tax_resolver facade per the QuoteContract Prime
    # Directive (CLAUDE.md). Stored format = PERCENT (e.g. 8.975) to
    # match the existing autosave conventions in
    # routes_pricecheck.py:2996 + routes_rfq.py:3410.
    #
    # Defensive: any tax-resolve failure must NEVER block ingest. Fall
    # back to unset fields so the existing operator path (Verify Tax
    # click) still works.
    _ingest_tax_rate_pct = 0
    _ingest_tax_source = ""
    _ingest_tax_jurisdiction = ""
    _ingest_tax_validated = False
    if resolved_ship_to and len(str(resolved_ship_to).strip()) > 3:
        try:
            from src.core.quote_contract import tax_for_address
            _tax = tax_for_address(resolved_ship_to) or {}
            _rate = float(_tax.get("rate") or 0.0)
            if _rate > 0:
                _ingest_tax_rate_pct = round(_rate * 100, 3)
                _ingest_tax_source = str(_tax.get("source") or "")
                _ingest_tax_jurisdiction = str(_tax.get("jurisdiction") or "")
                _ingest_tax_validated = bool(_tax.get("validated", False))
                log.info(
                    "auto-tax at ingest: ship_to=%r → rate=%.3f%% "
                    "jurisdiction=%s source=%s validated=%s",
                    resolved_ship_to[:60], _ingest_tax_rate_pct,
                    _ingest_tax_jurisdiction, _ingest_tax_source,
                    _ingest_tax_validated,
                )
        except Exception as _tax_e:
            log.debug("auto-tax at ingest suppressed: %s", _tax_e)

    # ── PR-AJ (2026-05-14): auto-price reference fields at ingest ───
    # Mike's funnel: pre-fix every ingested item landed with empty
    # catalog_cost / supplier / source_url / asin / confidence, forcing
    # the operator to engage with each row (Oracle lookup, URL paste,
    # supplier scrape) before they could even SEE the cost basis. This
    # hook calls `recommend_for_item()` (the canonical flat-shape
    # Oracle adapter at src/core/pricing_oracle_v2.py:550) per item
    # and stamps the REFERENCE fields. Unit_cost is left UNSET — that
    # is the operator's explicit decision per Mike's standing rule
    # ("operator-typed cost is sacred"; see PR-AC URL-paste protection
    # + the 3x sanity guard in Quote.set_price). The operator confirms
    # in one click instead of typing cost basis from scratch.
    #
    # Surfaces an `auto_priced_at_ingest` flag + `auto_price_at`
    # timestamp on each enriched item so the operator UI can show a
    # subtle badge ("Oracle suggested cost") and the audit trail
    # carries provenance. Reads via legacy item dict keys to stay
    # in lock-step with how downstream consumers (renderer, save
    # path, autosave) already consume items.
    #
    # Defensive: per-item failure is logged and skipped — that item
    # lands with empty reference fields (existing operator path).
    # Total enrichment failure (Oracle import error) is also logged
    # and the whole hook becomes a no-op. Ingest NEVER blocks on
    # auto-pricing.
    if items:
        try:
            from src.core.pricing_oracle_v2 import recommend_for_item
            _auto_priced_count = 0
            for _it in items:
                if not isinstance(_it, dict):
                    continue
                _desc = (_it.get("description") or "").strip()
                if not _desc:
                    continue
                # Skip items that already carry operator-confirmed
                # cost — don't clobber prior pricing on re-ingest.
                if _it.get("unit_cost") or _it.get("supplier_cost"):
                    continue
                try:
                    _rec = recommend_for_item(
                        description=_desc,
                        part_number=str(_it.get("part_number") or _it.get("item_number") or ""),
                        qty=float(_it.get("quantity") or _it.get("qty") or 1) or 1,
                        upc=str(_it.get("upc") or ""),
                    )
                except Exception as _ie:
                    log.debug("auto-price per-item failed for %r: %s", _desc[:40], _ie)
                    continue
                if not _rec:
                    continue
                _stamped = False
                # Reference fields — only fill blanks, never overwrite.
                if not _it.get("catalog_cost") and _rec.get("catalog_cost"):
                    _it["catalog_cost"] = _rec["catalog_cost"]; _stamped = True
                if not _it.get("supplier") and _rec.get("supplier"):
                    _it["supplier"] = _rec["supplier"]; _stamped = True
                if not _it.get("source_url") and _rec.get("source_url"):
                    _it["source_url"] = _rec["source_url"]; _stamped = True
                if not _it.get("asin") and _rec.get("asin"):
                    _it["asin"] = _rec["asin"]; _stamped = True
                if _it.get("confidence") in (None, 0, 0.0) and _rec.get("confidence") is not None:
                    _it["confidence"] = float(_rec["confidence"]); _stamped = True
                if _stamped:
                    _it["auto_priced_at_ingest"] = True
                    _it["auto_price_at"] = now
                    _auto_priced_count += 1
            if _auto_priced_count:
                log.info(
                    "auto-price at ingest: enriched %d/%d items with Oracle "
                    "reference fields (record type=%s)",
                    _auto_priced_count, len(items), record_type,
                )
        except Exception as _ape:
            log.debug("auto-price at ingest suppressed (Oracle unavailable?): %s", _ape)

    # received_at = email arrival time when poller passed it through,
    # else ingest time. Stored as RFC822 string when from Gmail; downstream
    # readers already coerce both shapes.
    received_at = email_received_at or now

    # ── Universal sol# synthesizer (PR substrate 2026-05-12) ──
    # Expanded from CalVet-only to all agencies after Mike hit the
    # placeholder block on a real quoting attempt (rfq_8efe9fae,
    # Keith Alsing CalVet, sol#='WORKSHEET'/rfq#='WORKSHEET'). The
    # failure class is universal: any buyer that ships an RFQ without
    # a real solicitation number leaves the parser falling back to a
    # placeholder string. The allocation gate blocks Generate on those
    # placeholders even when the record has real items + ship-to.
    #
    # Rule: when the extracted sol# is a placeholder AND items > 0,
    # synthesize `RT-<AGENCY>-<YYMMDD>-<short_id>` deterministically.
    # The `RT-` prefix is registered in `_is_placeholder_number`
    # (dashboard.py) as NOT a placeholder, so the synthesized number
    # cleanly clears the allocation gate without making genuinely-junk
    # strings ("WORKSHEET", "GOOD", "RFQ") pass. Operator can override
    # the synthesized value by typing a real sol# in the detail header.
    extracted_sol = (
        classification.solicitation_number
        or header.get("solicitation_number", "")
        or header.get("pc_number", "")
    )
    resolved_sol = extracted_sol
    if _looks_like_sol_placeholder(extracted_sol) and items:
        try:
            _ymd = (now or "")[:10].replace("-", "")[2:]  # YYMMDD slice
        except Exception:
            _ymd = ""
        _agency_tag = ((classification.agency or "unk") or "unk").upper()
        resolved_sol = f"RT-{_agency_tag}-{_ymd}-{short_id}"
        log.info(
            "sol synthesizer: %r → %r (no real sol# from buyer, agency=%s)",
            extracted_sol, resolved_sol, _agency_tag,
        )

    record: Dict[str, Any] = {
        "id": f"{record_type}_{short_id}",
        "created_at": now,
        "updated_at": now,
        "received_at": received_at,
        "status": initial_status,
        "source": "ingest_v2",
        "email_uid": email_uid,
        "email_subject": email_subject,
        "email_sender": email_sender,
        "source_pdf": primary_path or "",
        "_classification": classification.to_dict(),
        # PR-A 2026-05-07 (post-quote queue item 24, thread-aware ingest):
        # capture Gmail thread id + initial message id so subsequent buyer
        # replies can be matched against this record by thread membership
        # rather than each spawning a new RFQ/PC.
        "email_thread_id": gmail_thread_id or "",
        "gmail_message_ids": [gmail_message_id] if gmail_message_id else [],
        # Common header fields pulled from either the classifier or parser
        # (or `RT-CALVET-…` synthesized form when CalVet RFQ has no real
        # sol#; see synthesizer above).
        "solicitation_number": resolved_sol,
        "institution": canonical_institution,
        "ship_to": resolved_ship_to,
        "agency": classification.agency,
        "requestor_email": email_sender,
        # contact_email mirrors requestor_email at write time so the buyer-
        # rollup surfaces (PR #621-era) and quote-keyed views see the same
        # buyer regardless of which column they read.
        "contact_email": email_sender,
        # requestor_name: PDF header takes priority; if blank, derive from
        # email-sender display part or local-part heuristic. Drift surface
        # #4 from project_ams704_ingest_drift_2026_05_03.md.
        "requestor_name": (
            header.get("requestor", "")
            or header.get("requestor_name", "")
            or _derive_requestor_name(email_sender)
        ),
        # Persist email body so the support view can show it as a copy-paste
        # reference when the body extractor missed items (operator-fallback per
        # project_email_body_rfq_parser_gap.md fix shape #3).
        "body_text": (email_body or "")[:10000],
        # PR-A 2026-05-11: substrate signal — operator banner should
        # render when needs_review=True. ingest_warnings carries the
        # structured kind/detail/count payload so the UI can show a
        # specific message ("Vision found 15 items, regex found 5 —
        # review which is correct") instead of a free-text blob.
        "needs_review": bool(needs_review),
        "ingest_warnings": list(ingest_warnings or []),
        # PR-A Step 8: Proofpoint SecureMessage handoff fields. When
        # the auto-pull (Step 7) is unavailable or returns nothing,
        # the operator opens the portal at this URL and uploads the
        # decrypted PDF via the manual-upload route.
        "needs_manual_pull": bool(needs_manual_pull),
        "proofpoint_portal_url": proofpoint_portal_url or "",
        # PR-AI (2026-05-14): tax resolution at ingest (see block above
        # where these are populated). Stored as PERCENT to match the
        # autosave path. Zero/empty signals "not resolved at ingest" —
        # the existing detail-page Verify Tax button can re-resolve.
        "tax_rate": _ingest_tax_rate_pct,
        "tax_source": _ingest_tax_source,
        "tax_jurisdiction": _ingest_tax_jurisdiction,
        "tax_validated": _ingest_tax_validated,
    }

    # Surface #17 (2026-05-04): when the buyer's PRICE CHECK / Solicitation #
    # field is blank, fall back to the email-attachment filename BEFORE the
    # AUTO_<hash> last resort. Shared helper so PC and RFQ cascades stay in
    # parity per feedback_global_fix_not_one_off.
    _attachment_title = _attachment_filename_title(primary_path)

    if record_type == "pc":
        record["pc_number"] = (
            resolved_sol
            or header.get("pc_number", "")
            or _attachment_title
            or f"AUTO_{short_id}"
        )
        record["items"] = items
        # PR-AV13 (AV-13): audit-trail snapshot of buyer-asked items.
        # See _snapshot_buyer_source_items() for why this exists.
        record["buyer_source_items"] = _snapshot_buyer_source_items(items)
        record["packet_type"] = (
            "cchcs_non_it"
            if classification.shape == "cchcs_packet"
            else ""
        )
        # PR-N (2026-05-13) — dedup-at-ingest by pc_number. When the
        # buyer's pc_number already exists on a non-terminated PC for
        # this same agency, auto-mark the new row as duplicate so it
        # doesn't pollute the operator queue. Audit row still gets
        # written (we want the trail) — operator can find it via the
        # /admin/funnel resolved-other breakdown. `closed_reason`
        # explains why and points at the surviving PC id.
        try:
            from src.core.flags import get_flag
            _dedup_on = bool(get_flag("ingest.dedup_by_pc_number_enabled", True))
        except Exception:
            _dedup_on = True
        if _dedup_on:
            existing = _find_active_pc_by_number(
                record["pc_number"],
                agency=record.get("agency", "") or record.get("institution", ""),
            )
            if existing:
                existing_id = existing.get("id", "")
                existing_status = (existing.get("status") or "parsed").strip().lower()
                _dedup_reason = (
                    f"auto-dedup: pc_number={record['pc_number']} already in "
                    f"{existing_status} (pc {existing_id[:12]})"
                )
                record["status"] = "duplicate"
                record["closed_reason"] = _dedup_reason
                record["closed_at"] = now
                record["dedup_of"] = existing_id
                # Audit trail entry — mirrors the shape PR-M writes from
                # status-change routes so the funnel breakdown can read it.
                _hist = record.get("status_history") or []
                _hist.append({
                    "from": initial_status,
                    "to": "duplicate",
                    "at": now,
                    "actor": "ingest_pipeline",
                    "reason": _dedup_reason,
                })
                record["status_history"] = _hist
                log.info(
                    "ingest dedup-at-ingest: pc_number=%s collides with %s "
                    "(status=%s) — new pc auto-marked duplicate",
                    record["pc_number"], existing_id, existing_status,
                )
                try:
                    from src.core.utilization import record_feature_use
                    record_feature_use("ingest.pc_duplicate_skipped", context={
                        "pc_number": record["pc_number"],
                        "agency": record.get("agency", ""),
                        "existing_pc_id": existing_id,
                        "existing_status": existing_status,
                    })
                except Exception as _te:
                    log.debug("dedup telemetry suppressed: %s", _te)

        # Hotfix 2026-05-13 — PC-side thread dedup. When dedup-by-
        # pc_number falls through (the RT-synthesized sol# case Mike hit
        # this evening: each buyer-reply mints a UNIQUE RT-CCHCS-260513-*
        # number, so pc_number dedup can't catch it), fall back to
        # email_thread_id matching. PRs #959 + this hotfix cover both
        # buyer-typed pc_numbers AND Reytech-synthesized fallbacks.
        if (record.get("status") or "") != "duplicate" and gmail_thread_id:
            try:
                from src.core.flags import get_flag
                _thread_dedup_on = bool(get_flag(
                    "ingest.dedup_pc_by_thread_enabled", True,
                ))
            except Exception:
                _thread_dedup_on = True
            if _thread_dedup_on:
                existing_thread = _find_active_record_by_thread(
                    "pc", gmail_thread_id,
                    agency=record.get("agency", "") or record.get("institution", ""),
                )
                if existing_thread:
                    existing_id = existing_thread.get("id", "")
                    existing_status = (existing_thread.get("status") or "parsed").strip().lower()
                    _dedup_reason = (
                        f"auto-dedup: gmail_thread_id matches active pc "
                        f"{existing_id[:12]} (status={existing_status}) — "
                        f"buyer reply in existing thread"
                    )
                    record["status"] = "duplicate"
                    record["closed_reason"] = _dedup_reason
                    record["closed_at"] = now
                    record["dedup_of"] = existing_id
                    _hist = record.get("status_history") or []
                    _hist.append({
                        "from": initial_status,
                        "to": "duplicate",
                        "at": now,
                        "actor": "ingest_pipeline",
                        "reason": _dedup_reason,
                    })
                    record["status_history"] = _hist
                    log.info(
                        "ingest dedup-at-ingest [pc, thread]: thread_id=%s "
                        "collides with %s (status=%s) — new pc auto-marked duplicate",
                        str(gmail_thread_id)[:24], existing_id, existing_status,
                    )
                    try:
                        from src.core.utilization import record_feature_use
                        record_feature_use("ingest.pc_duplicate_skipped_by_thread", context={
                            "thread_id": str(gmail_thread_id)[:24],
                            "agency": record.get("agency", ""),
                            "existing_pc_id": existing_id,
                            "existing_status": existing_status,
                        })
                    except Exception as _te:
                        log.debug("pc thread dedup telemetry suppressed: %s", _te)

        from src.api.dashboard import _save_single_pc
        _save_single_pc(record["id"], record)
    else:  # rfq
        record["rfq_number"] = (
            resolved_sol
            or header.get("solicitation_number", "")
            or _attachment_title
            or f"AUTO_{short_id}"
        )
        record["line_items"] = items
        # PR-AV13 (AV-13): audit-trail snapshot of buyer-asked items.
        # See _snapshot_buyer_source_items() for why this exists.
        record["buyer_source_items"] = _snapshot_buyer_source_items(items)

        # Register every classifiable sibling attachment as a template
        # slot (703b / 704b / bidpkg / dsh_attA-C / 703c). Pre-fix this
        # only ran on _update_existing_record for re-ingests; new RFQs
        # via classifier_v2 had EMPTY templates dict even when the buyer
        # attached the right PDFs. Result: package generator hit
        # "Missing required templates" at gen-time even though the
        # email had everything. Mike P0 2026-05-06 RFQ a5b09b56.
        try:
            from src.forms.rfq_parser import identify_attachments
            _paths_for_classify = all_paths or ([primary_path] if primary_path else [])
            if _paths_for_classify:
                _new_templates = identify_attachments(_paths_for_classify)
                if _new_templates:
                    record["templates"] = _new_templates
                    log.info(
                        "ingest create: registered templates for %s: %s "
                        "(from %d sibling attachment(s))",
                        record["id"], list(_new_templates.keys()),
                        len(_paths_for_classify),
                    )
        except Exception as _e:
            log.warning("ingest create: template registration failed: %s", _e)

        # RFQ-side dedup-at-ingest by email_thread_id (hotfix 2026-05-13).
        # Mike: "i just manually sent the quote we sent earlier, and the
        # queue populated back with ghost data". Mohammad's buyer-reply
        # arrived in the same Gmail thread as the original RFQ; the RFQ
        # branch had no dedup (PR-N #959 only handled PCs by pc_number),
        # so the reply's attachments spawned fresh RFQs with unique
        # RT-CCHCS-260513-* synthesized sol#s. Mirror the PC-side gate
        # using thread_id (the stable key across replies).
        try:
            from src.core.flags import get_flag
            _rfq_dedup_on = bool(get_flag(
                "ingest.dedup_rfq_by_thread_enabled", True,
            ))
        except Exception:
            _rfq_dedup_on = True
        if _rfq_dedup_on and gmail_thread_id:
            existing_rfq = _find_active_record_by_thread(
                "rfq", gmail_thread_id,
                agency=record.get("agency", "") or record.get("institution", ""),
            )
            if existing_rfq:
                existing_id = existing_rfq.get("id", "")
                existing_status = (existing_rfq.get("status") or "parsed").strip().lower()
                _dedup_reason = (
                    f"auto-dedup: gmail_thread_id matches active rfq {existing_id[:12]} "
                    f"(status={existing_status}) — buyer reply in existing thread"
                )
                record["status"] = "duplicate"
                record["closed_reason"] = _dedup_reason
                record["closed_at"] = now
                record["dedup_of"] = existing_id
                _hist = record.get("status_history") or []
                _hist.append({
                    "from": initial_status,
                    "to": "duplicate",
                    "at": now,
                    "actor": "ingest_pipeline",
                    "reason": _dedup_reason,
                })
                record["status_history"] = _hist
                log.info(
                    "ingest dedup-at-ingest [rfq]: thread_id=%s collides with %s "
                    "(status=%s) — new rfq auto-marked duplicate",
                    str(gmail_thread_id)[:24], existing_id, existing_status,
                )
                try:
                    from src.core.utilization import record_feature_use
                    record_feature_use("ingest.rfq_duplicate_skipped", context={
                        "thread_id": str(gmail_thread_id)[:24],
                        "agency": record.get("agency", ""),
                        "existing_rfq_id": existing_id,
                        "existing_status": existing_status,
                    })
                except Exception as _te:
                    log.debug("rfq dedup telemetry suppressed: %s", _te)

        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(record["id"], record)

    # PR-A 2026-05-11: persist every buyer-email attachment so re-ingest
    # / Vision-recheck scripts can find the source bytes without a
    # Gmail re-fetch fallback. Fire-and-forget — failure does not block
    # the ingest (operator can still re-upload manually).
    try:
        _persist_all_attachments(
            record["id"],
            record_type,
            all_paths or ([primary_path] if primary_path else []),
            gmail_message_id=gmail_message_id,
        )
    except Exception as _ape:
        log.debug("attachment persistence skipped: %s", _ape)

    # ── PR-AO (2026-05-14): attachment-deadline upgrade at ingest ────
    # When the save above stamped `due_date_source = "default"` because
    # neither header nor email subject nor email body carried a parsable
    # deadline, scan the buyer's attached PDF(s) for one before we leave
    # ingest. Many CalVet / CCHCS RFP cover pages state the deadline in
    # the PDF text ("Due By: 5/13/26") that the parser ignored — the
    # buyer just writes "see attached" in the body.
    #
    # Uses pdfplumber + the same regex extractor as body/subject, so the
    # detection surface is consistent across all three sources. New
    # source value is "attachment" and the audit field `due_date_attachment`
    # pins which file yielded the hit. No-op when source is anything other
    # than "default" (header/subject/email/attachment wins stay put).
    try:
        if (record.get("due_date_source") or "").lower() == "default":
            _pdf_paths = [
                p for p in (all_paths or ([primary_path] if primary_path else []))
                if p and p.lower().endswith(".pdf")
            ]
            if _pdf_paths:
                from src.core.deadline_defaults import apply_attachment_if_default
                _upgrade = apply_attachment_if_default(record, _pdf_paths)
                if _upgrade == "attachment":
                    log.info(
                        "auto-deadline at ingest: upgraded %s default → "
                        "attachment (%s) from %s",
                        record["id"],
                        record.get("due_date"),
                        record.get("due_date_attachment"),
                    )
                    # Re-save with the upgraded deadline.
                    if record_type == "pc":
                        from src.api.dashboard import _save_single_pc
                        _save_single_pc(record["id"], record)
                    else:
                        from src.api.dashboard import _save_single_rfq
                        _save_single_rfq(record["id"], record)
    except Exception as _ade:
        log.debug("attachment-deadline upgrade skipped: %s", _ade)

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


_PRESERVE_FIELDS_ON_REPARSE = (
    # Operator-set pricing dict (unit_cost, recommended_price, markup_pct, ...)
    "pricing",
    # Top-level pricing fields (legacy — newer code keeps everything in `pricing`)
    "vendor_cost",
    "unit_price",
    "markup_pct",
    # Operator-curated supplier link + label
    "item_link",
    "item_supplier",
    # Operator notes + bid intent
    "notes",
    "no_bid",
    # Catalog enrichment — don't lose the resolved match
    "catalog_match",
)


_DESC_TOKEN_RE = None  # lazy-compiled


def _desc_tokens(s: str) -> set:
    """Lowercase, split on non-alphanumeric, drop 1-char tokens.
    Mirrors `_smart_tokenize` in agents/product_catalog.py — same word-split
    semantics so descriptions tokenize the same way regardless of comma /
    hyphen / slash punctuation differences between buyer's two emails."""
    import re
    global _DESC_TOKEN_RE
    if _DESC_TOKEN_RE is None:
        _DESC_TOKEN_RE = re.compile(r"[^a-zA-Z0-9]+")
    return {t for t in _DESC_TOKEN_RE.split((s or "").lower()) if len(t) >= 2}


def _desc_jaccard(a: str, b: str) -> float:
    """Token-set Jaccard similarity; 0.0 when either side has no tokens."""
    ta, tb = _desc_tokens(a), _desc_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _match_old_to_new(
    old_items: List[Dict[str, Any]],
    new_item: Dict[str, Any],
    used_old_indexes: Optional[set] = None,
) -> Optional[Dict[str, Any]]:
    """Find an old item that corresponds to `new_item`. Strategies in order:
      1. Exact MFG# match (case-insensitive, whitespace-stripped)
      2. Description token-Jaccard ≥ 0.85
      3. row_index match (legacy AMS704 form-field re-parse fallback)
    Returns the first old item that hasn't already been claimed
    (`used_old_indexes` tracks claims), or None.
    """
    used = used_old_indexes if used_old_indexes is not None else set()

    new_mfg = (new_item.get("mfg_number") or "").strip().upper()
    if new_mfg:
        for i, old in enumerate(old_items):
            if i in used:
                continue
            if (old.get("mfg_number") or "").strip().upper() == new_mfg:
                return old

    new_desc = (new_item.get("description") or "").strip()
    if new_desc:
        for i, old in enumerate(old_items):
            if i in used:
                continue
            if _desc_jaccard(new_desc, old.get("description") or "") >= 0.85:
                return old

    new_ri = new_item.get("row_index")
    if new_ri:
        for i, old in enumerate(old_items):
            if i in used:
                continue
            if old.get("row_index") == new_ri:
                return old

    return None


def _merge_items_preserving_pricing(
    old_items: List[Dict[str, Any]],
    new_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge a freshly-parsed item list into an existing one, preserving
    operator-set pricing/links/notes on items that match.

    Match strategy: MFG# → description Jaccard ≥ 0.85 → row_index.

    Semantics:
      - Matched: copy `_PRESERVE_FIELDS_ON_REPARSE` from old onto new
        (when old has a non-None value). Buyer's qty/desc/uom on the new
        item wins over old.
      - New, unmatched: appended as-is (net-new items from buyer).
      - Old, unmatched: KEPT UNCHANGED (don't delete on buyer-email re-parse —
        buyers who say "please add this item" rarely restate the original;
        operator-driven removal must be explicit, not implicit).

    Each old item can only be claimed once (no double-attribution of pricing
    when the new parse contains duplicates).
    """
    used_old_idx: set = set()
    merged: List[Dict[str, Any]] = []

    for new in new_items:
        old = _match_old_to_new(old_items, new, used_old_idx)
        if old is not None:
            try:
                old_idx = old_items.index(old)
                used_old_idx.add(old_idx)
            except ValueError:
                pass
            for field in _PRESERVE_FIELDS_ON_REPARSE:
                v = old.get(field)
                if v is None:
                    continue
                # Don't overwrite a non-empty new value with old (rare —
                # would happen only if the parser pre-fills pricing)
                if new.get(field):
                    continue
                new[field] = v
        merged.append(new)

    # Old items not matched survive — buyer's add-an-item email did not
    # ask to remove them. Operator can trim explicitly via UI.
    for i, old in enumerate(old_items):
        if i not in used_old_idx:
            merged.append(old)

    return merged


def _snapshot_buyer_source_items(
    items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Freeze the parser's view of buyer-asked items for the audit trail.

    Persisted on the record as `buyer_source_items` at ingest (and on
    every re-parse) BEFORE operator edits touch line_items. The
    /rfq/<id>/review-package alignment table reads this to render the
    "Buyer asked X / You replied Y" comparison.

    Without this, operator edits to line_items silently overwrite the
    buyer's original asks, and there's no way to verify post-hoc that
    every row in our quote corresponds to a row in the buyer's RFQ.

    Only keeps audit-relevant fields (description, qty, MFG/part,
    uom, notes). Pricing/cost/markup are deliberately EXCLUDED —
    those are Reytech's response, not buyer-source. Storing pricing
    here would double-count and confuse the alignment row diff.
    """
    out: List[Dict[str, Any]] = []
    for it in (items or []):
        if not isinstance(it, dict):
            continue
        qty = it.get("qty") or it.get("quantity") or 0
        try:
            qty = int(qty) if qty else 0
        except (TypeError, ValueError):
            qty = 0
        out.append({
            "description": (it.get("description") or "").strip(),
            "qty": qty,
            "part_number": (
                it.get("part_number")
                or it.get("mfg_number")
                or ""
            ).strip(),
            "uom": (
                it.get("uom")
                or it.get("unit_of_measure")
                or ""
            ).strip(),
            "notes": (it.get("notes") or "").strip(),
        })
    return out


def _update_existing_record(
    record_id: str,
    record_type: str,
    items: List[Dict[str, Any]],
    header: Dict[str, Any],
    classification: "RequestClassification",  # noqa: F821
    primary_path: Optional[str],
    all_paths: Optional[List[str]] = None,
    gmail_thread_id: str = "",
    gmail_message_id: str = "",
    needs_review: bool = False,
    ingest_warnings: Optional[List[Dict[str, str]]] = None,
    needs_manual_pull: bool = False,
    proofpoint_portal_url: str = "",
) -> str:
    """Re-run classification + parsing on an existing record.
    Used when the operator clicks 'Re-parse' on an already-created PC/RFQ
    AND when a buyer's reply email triggers an automated re-ingest.

    Item handling: NEW items are merged into existing via
    `_merge_items_preserving_pricing` so operator-set pricing on existing
    items survives the re-parse. Pre-2026-05-05 this path destructively
    overwrote items, which silently wiped Mike's pricing every time a
    buyer re-emailed (regression incident: pc_177b18e6, 22 hours of
    operator pricing lost).
    """
    from src.api.dashboard import _load_price_checks, _save_single_pc
    if record_type == "pc":
        pcs = _load_price_checks()
        pc = pcs.get(record_id) or {}
        pc["_classification"] = classification.to_dict()
        # PR-A 2026-05-11: refresh substrate diagnostics from the new
        # ingest pass. If the re-parse no longer detects disagreement
        # (Vision and base parser now align), `needs_review` clears so
        # the operator sees the resolved state. Warnings are REPLACED,
        # not appended — the latest signal is the truth source; stale
        # warnings from prior runs would just confuse triage.
        pc["needs_review"] = bool(needs_review)
        pc["ingest_warnings"] = list(ingest_warnings or [])
        # PR-A Step 8: SecureMessage handoff fields. The portal URL is
        # only overwritten when the new pass actually carried one —
        # otherwise we preserve whatever was captured at the original
        # ingest so the operator's bookmark stays valid.
        if proofpoint_portal_url:
            pc["proofpoint_portal_url"] = proofpoint_portal_url
        pc["needs_manual_pull"] = bool(needs_manual_pull)
        _cur_status = pc.get("status", "")
        if needs_manual_pull and _cur_status in ("parsed", "needs_review", ""):
            pc["status"] = "needs_manual_pull"
        elif not needs_manual_pull and _cur_status == "needs_manual_pull":
            # Auto-pull (or operator) resolved the pull — drop back so
            # the normal needs_review / parsed logic below can re-evaluate.
            _cur_status = "parsed"
            pc["status"] = "parsed"
        if needs_review and _cur_status in ("parsed", ""):
            # Flip status into the triage queue so the operator sees
            # the disagreement on the next queue refresh. Don't override
            # downstream statuses (quoted, sent, won, lost) — those are
            # operator-driven and should never be regressed by a re-ingest.
            pc["status"] = "needs_review"
        elif not needs_review and _cur_status == "needs_review":
            # Disagreement resolved on this re-ingest — drop back to
            # `parsed` so the record falls out of the triage queue.
            # Only clears the status when it was set BY a prior ingest;
            # operator-driven statuses are untouched.
            pc["status"] = "parsed"
        if items:
            pc["items"] = _merge_items_preserving_pricing(
                pc.get("items") or [], items,
            )
            # PR-AV13 (AV-13): on every re-parse, OVERWRITE the audit
            # snapshot with the fresh parser view of buyer-asked items.
            # `items` here is the new parse output BEFORE the merge with
            # existing pricing — that's the canonical buyer-asked set
            # (the merge step preserves operator pricing on rows that
            # match, but the buyer-asked shape is exactly `items`).
            pc["buyer_source_items"] = _snapshot_buyer_source_items(items)
            # PR-ε (2026-05-11): re-parse can introduce cost/markup/price
            # drift if the merged item dict carries one alias but not another.
            # Run the canonical reconciler so the saved record stays coherent
            # with what canonical_unit_price will read at render time.
            try:
                from src.core.pricing_math import reconcile_items as _reconcile_reparse
                _reconcile_reparse(pc["items"])
            except Exception as _re:
                log.debug("reparse PC: reconcile_items suppressed: %s", _re)
        if primary_path:
            pc["source_pdf"] = primary_path
        # PR-A 2026-05-07: append the new message_id to the thread's
        # message-graph so the RFQ holds the full conversation history.
        # Backfill thread_id when the existing record is missing it (e.g.
        # legacy records from before threadId capture was wired in).
        if gmail_thread_id and not pc.get("email_thread_id"):
            pc["email_thread_id"] = gmail_thread_id
        if gmail_message_id:
            _msgs = pc.get("gmail_message_ids") or []
            if isinstance(_msgs, str):
                try: _msgs = __import__("json").loads(_msgs) or []
                except Exception: _msgs = []
            if gmail_message_id not in _msgs:
                _msgs.append(gmail_message_id)
            pc["gmail_message_ids"] = _msgs
        pc["updated_at"] = datetime.now().isoformat()
        _save_single_pc(record_id, pc)
        # PR-A 2026-05-11: persist re-parse attachments too. Buyer-reply
        # ingests (PR-E flow) carry NEW PDFs on the followup message —
        # they must be saved or the re-ingest substrate can't replay
        # them either.
        try:
            _persist_all_attachments(
                record_id, "pc",
                all_paths or ([primary_path] if primary_path else []),
                gmail_message_id=gmail_message_id,
            )
        except Exception as _ape:
            log.debug("reparse PC attachment persistence skipped: %s", _ape)
        # Re-parse replaces items — refresh catalog MSRP for the merged set too
        if items:
            try:
                from src.agents.product_catalog import refresh_prices_for_items_async
                refresh_prices_for_items_async(pc["items"], context=f"reparse_pc_{record_id[:8]}")
            except Exception as _e:
                log.debug("refresh_prices_for_items_async skipped on reparse: %s", _e)
    else:  # rfq
        from src.api.dashboard import _save_single_rfq, load_rfqs
        rfqs = load_rfqs()
        rfq = rfqs.get(record_id) or {}
        rfq["_classification"] = classification.to_dict()
        # PR-A 2026-05-11: same substrate refresh as the PC branch.
        rfq["needs_review"] = bool(needs_review)
        rfq["ingest_warnings"] = list(ingest_warnings or [])
        if proofpoint_portal_url:
            rfq["proofpoint_portal_url"] = proofpoint_portal_url
        rfq["needs_manual_pull"] = bool(needs_manual_pull)
        _cur_status = rfq.get("status", "")
        if needs_manual_pull and _cur_status in ("parsed", "needs_review", ""):
            rfq["status"] = "needs_manual_pull"
        elif not needs_manual_pull and _cur_status == "needs_manual_pull":
            _cur_status = "parsed"
            rfq["status"] = "parsed"
        if needs_review and _cur_status in ("parsed", ""):
            rfq["status"] = "needs_review"
        elif not needs_review and _cur_status == "needs_review":
            rfq["status"] = "parsed"
        if items:
            rfq["line_items"] = _merge_items_preserving_pricing(
                rfq.get("line_items") or [], items,
            )
            # PR-AV13 (AV-13): twin of the PC re-parse audit snapshot
            # above. Overwrite buyer_source_items with the fresh parse
            # output BEFORE the pricing-preserving merge — that's the
            # canonical buyer-asked set after re-parse.
            rfq["buyer_source_items"] = _snapshot_buyer_source_items(items)
            # PR-ε (2026-05-11): twin of the PC re-parse reconcile call
            # above. RFQ re-parse merges items the same way and needs the
            # same canonical-coherence pass.
            try:
                from src.core.pricing_math import reconcile_items as _reconcile_reparse_rfq
                _reconcile_reparse_rfq(rfq["line_items"])
            except Exception as _re:
                log.debug("reparse RFQ: reconcile_items suppressed: %s", _re)
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
                # Classify EVERY sibling attachment, not just the primary.
                # Pre-fix only the primary file was classified — buyer
                # emails with multiple attachments only ever filled one
                # template slot. Mike P0 2026-05-06 RFQ a5b09b56.
                _paths_for_classify = all_paths or [primary_path]
                new_templates = identify_attachments(_paths_for_classify)
                if new_templates:
                    existing = rfq.get("templates") or {}
                    for _k, _v in new_templates.items():
                        existing[_k] = _v
                    rfq["templates"] = existing
                    log.info(
                        "ingest update: registered templates for %s: %s "
                        "(from %d sibling attachment(s))",
                        record_id, list(new_templates.keys()),
                        len(_paths_for_classify),
                    )
            except Exception as _e:
                log.warning("ingest update: template registration failed: %s", _e)
        # PR-A 2026-05-07: append the new message_id to the thread's
        # message-graph so the RFQ holds the full conversation history.
        if gmail_thread_id and not rfq.get("email_thread_id"):
            rfq["email_thread_id"] = gmail_thread_id
        if gmail_message_id:
            _msgs = rfq.get("gmail_message_ids") or []
            if isinstance(_msgs, str):
                try: _msgs = __import__("json").loads(_msgs) or []
                except Exception: _msgs = []
            if gmail_message_id not in _msgs:
                _msgs.append(gmail_message_id)
            rfq["gmail_message_ids"] = _msgs
        rfq["updated_at"] = datetime.now().isoformat()
        _save_single_rfq(record_id, rfq)
        # PR-A 2026-05-11: twin of the PC branch — persist re-parse
        # attachments so re-ingest scripts can replay them later.
        try:
            _persist_all_attachments(
                record_id, "rfq",
                all_paths or ([primary_path] if primary_path else []),
                gmail_message_id=gmail_message_id,
            )
        except Exception as _ape:
            log.debug("reparse RFQ attachment persistence skipped: %s", _ape)
        if items:
            try:
                from src.agents.product_catalog import refresh_prices_for_items_async
                refresh_prices_for_items_async(rfq["line_items"], context=f"reparse_rfq_{record_id[:8]}")
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
