"""Flask routes for the CCHCS Non-IT RFQ Packet automation.

Endpoints:
    POST /api/pricecheck/<pcid>/cchcs-packet/generate
    POST /api/admin/cchcs-packets/backfill

The generate route reads the PC's source PDF, parses it via
cchcs_packet_parser, matches items against all active PCs via
cchcs_pc_matcher, fills the packet via cchcs_packet_filler, and returns
a download URL for the `<source>_Reytech.pdf` output.

The backfill route walks every existing PC and tags any that look like
a CCHCS packet (via filename / subject patterns) so the inbox badge and
filter work for historical PCs.

Built 2026-04-13 overnight. Phase 4 of 5. See
_overnight_review/MORNING_REVIEW.md.
"""
import logging
import os

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route
from src.core.paths import DATA_DIR, OUTPUT_DIR

log = logging.getLogger("reytech")


@bp.route("/api/pricecheck/<pcid>/cchcs-packet/generate", methods=["POST"])
@auth_required
@safe_route
def api_cchcs_packet_generate(pcid):
    """Generate a filled CCHCS Non-IT RFQ Packet for the given PC.

    Expects the PC to have a source_pdf pointing at a CCHCS packet
    (18-page fillable PDF with "PREQ" or "RFQ Packet" in filename).
    Returns the download path for the `<name>_Reytech.pdf` output.

    Query params:
        dry_run=1 — parse + match but don't write the filled PDF
                    (useful for seeing what the matcher found before
                    committing to a fill)
    """
    try:
        from src.api.dashboard import _load_price_checks
        from src.forms.cchcs_packet_parser import (
            parse_cchcs_packet,
            looks_like_cchcs_packet,
        )
        from src.forms.cchcs_packet_filler import fill_cchcs_packet
        from src.agents.cchcs_pc_matcher import match_packet_to_pcs
    except Exception as e:
        return jsonify({"ok": False, "error": f"imports failed: {e}"}), 500

    # Telemetry: every CCHCS packet generation attempt recorded
    try:
        from src.core.utilization import record_feature_use
        record_feature_use("cchcs_packet.generate", context={
            "pc_id": pcid,
            "dry_run": request.args.get("dry_run", "0") == "1",
        })
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404

    source_pdf = pc.get("source_pdf", "") or ""
    if not source_pdf or not os.path.exists(source_pdf):
        return jsonify({
            "ok": False,
            "error": (
                "Source PDF not found on disk. Upload the CCHCS packet "
                "via More -> Upload PDF & Parse, then retry."
            ),
        }), 400

    # Guard: only run on CCHCS packet sources. If the filename doesn't
    # look like a packet, return 400 with a pointer to the normal 704
    # generate endpoint. Also tag the PC's packet_type now if it isn't
    # already set, so the home queue / UI can show a packet badge.
    basename = os.path.basename(source_pdf)
    email_subject = pc.get("email_subject", "") or ""
    if not looks_like_cchcs_packet(filename=basename, subject=email_subject):
        return jsonify({
            "ok": False,
            "error": (
                f"Source '{basename}' does not look like a CCHCS Non-IT "
                f"RFQ Packet. Use /pricecheck/<id>/generate for standard 704s."
            ),
        }), 400
    try:
        from src.agents.cchcs_packet_detector import tag_pc_if_packet
        tag_pc_if_packet(pc)
    except Exception as _de:
        log.debug("cchcs packet tag: %s", _de)

    dry_run = request.args.get("dry_run", "0") == "1"

    # ── Parse ──
    parsed = parse_cchcs_packet(source_pdf)
    if not parsed.get("ok"):
        return jsonify({
            "ok": False,
            "error": f"parse failed: {parsed.get('error')}",
        }), 500

    # ── Match against all active PCs ──
    match_result = match_packet_to_pcs(parsed, pcs)

    if dry_run:
        return jsonify({
            "ok": True,
            "dry_run": True,
            "packet_sol": parsed["header"].get("solicitation_number"),
            "packet_items": len(parsed["line_items"]),
            "match_result": match_result,
            "parsed_header": parsed["header"],
        })

    # ── Fill ──
    # CC-1: filled packets must land in OUTPUT_DIR (persistent volume on
    # Railway), not in the upload directory next to the source. Before
    # this fix the filled PDF sat in uploads/ forever, which
    # (a) polluted the raw-source directory with generated artifacts and
    # (b) left the output vulnerable to upload-dir cleanup routines.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_dir = OUTPUT_DIR
    # Gate enforcement: strict mode blocks fill from returning ok=True
    # if any critical business rule is violated. Dry-run preview still
    # runs every check but surfaces issues without blocking.
    fill_result = fill_cchcs_packet(
        source_pdf=source_pdf,
        parsed=parsed,
        output_dir=output_dir,
        price_overrides=match_result.get("price_overrides"),
        strict=True,
    )
    if not fill_result.get("ok"):
        # Distinguish gate failures (business rule violations, 422)
        # from infrastructure failures (fill crash, 500). Gate failures
        # are actionable by the operator; crashes need engineering.
        gate_report = fill_result.get("gate") or {}
        if gate_report and not gate_report.get("passed", True):
            return jsonify({
                "ok": False,
                "error": fill_result.get("error", "gate validation failed"),
                "gate": {
                    "passed": False,
                    "critical_issues": gate_report.get("critical_issues", []),
                    "warnings": gate_report.get("warnings", []),
                    "checks_run": gate_report.get("checks_run", 0),
                    "by_check": gate_report.get("by_check", {}),
                },
                "match_result": match_result,
            }), 422
        return jsonify({
            "ok": False,
            "error": f"fill failed: {fill_result.get('error')}",
            "match_result": match_result,
        }), 500

    output_path = fill_result["output_path"]
    output_name = os.path.basename(output_path)
    download_url = f"/api/pricecheck/download/{output_name}"

    # ── Persist output on the PC so the UI shows a download button ──
    # CC-2: store the packet output under its OWN slot
    # (cchcs_packet_output_pdf). The generic `output_pdf` slot is used
    # by the 704 generator; overwriting it meant that whichever fill
    # ran last won the single download slot, and earlier artifacts went
    # silently invisible in the UI.
    try:
        pc["cchcs_packet_output_pdf"] = output_path
        # Keep `output_pdf` pointing at the packet only if nothing else
        # has claimed the slot yet — don't clobber an already-generated 704.
        pc.setdefault("output_pdf", output_path)
        pc["cchcs_packet_last_generated"] = {
            "at": _utc_now_iso(),
            "rows_priced": fill_result["rows_priced"],
            "subtotal": fill_result["subtotal"],
            "grand_total": fill_result["grand_total"],
            "matched": match_result["matched_count"],
            "unmatched": match_result["unmatched_count"],
        }
        from src.api.dashboard import _save_single_pc
        _save_single_pc(pcid, pc)
    except Exception as _se:
        log.debug("cchcs_packet save-pc metadata: %s", _se)

    # ── CC-5: register the generated PDF in rfq_files so it surfaces
    # on the Files tab and participates in the DB-backed file index.
    # Before this fix the packet output existed only on disk — the
    # /Files tab and analytics that enumerate rfq_files never saw it.
    try:
        from src.api.dashboard import save_rfq_file
        with open(output_path, "rb") as _fh:
            _pdf_bytes = _fh.read()
        save_rfq_file(
            pcid,
            output_name,
            "application/pdf",
            _pdf_bytes,
            category="cchcs_packet",
            uploaded_by="system",
        )
    except Exception as _re:
        log.warning("cchcs_packet register in rfq_files failed for %s: %s", pcid, _re)

    log.info(
        "cchcs_packet generated for %s: matched=%d unmatched=%d total=$%.2f file=%s",
        pcid, match_result["matched_count"], match_result["unmatched_count"],
        fill_result["grand_total"], output_name,
    )

    return jsonify({
        "ok": True,
        "pc_id": pcid,
        "packet_sol": parsed["header"].get("solicitation_number"),
        "output_path": output_path,
        "output_name": output_name,
        "download_url": download_url,
        "rows_priced": fill_result["rows_priced"],
        "subtotal": fill_result["subtotal"],
        "grand_total": fill_result["grand_total"],
        "matched": match_result["matched_count"],
        "unmatched": match_result["unmatched_count"],
        "match_report": match_result.get("report", []),
        "fields_written": fill_result.get("fields_written", 0),
        "gate": {
            "passed": fill_result.get("gate", {}).get("passed", False),
            "critical_issues": fill_result.get("gate", {}).get("critical_issues", []),
            "warnings": fill_result.get("gate", {}).get("warnings", []),
            "checks_run": fill_result.get("gate", {}).get("checks_run", 0),
        },
    })


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


@bp.route("/api/admin/cchcs-packets/backfill", methods=["POST"])
@auth_required
@safe_route
def api_cchcs_packets_backfill():
    """Walk every existing PC and tag any that look like a CCHCS Non-IT
    RFQ Packet with `packet_type=cchcs_non_it`. Idempotent — already-
    tagged PCs are counted as already_tagged and not re-touched.

    Returns:
        {
            "ok": bool,
            "total": int,           # total PCs scanned
            "tagged_now": int,      # newly tagged by this call
            "already_tagged": int,  # already had packet_type set
            "not_packet": int,      # PCs that don't match packet patterns
            "tagged_ids": [str],    # IDs newly tagged by this call
        }

    This is a one-shot admin operation for after initial deployment of
    the CCHCS packet automation. Running it multiple times is safe.
    """
    try:
        from src.api.dashboard import _load_price_checks, _save_single_pc
        from src.agents.cchcs_packet_detector import backfill_existing_pcs
    except Exception as e:
        return jsonify({"ok": False, "error": f"imports failed: {e}"}), 500

    pcs = _load_price_checks()
    summary = backfill_existing_pcs(pcs)

    # Persist any PCs that were newly tagged. backfill_existing_pcs
    # mutates in place, so we save only the ones in tagged_ids.
    persisted = 0
    persist_errors = []
    for pc_id in summary.get("tagged_ids", []):
        pc = pcs.get(pc_id)
        if not pc:
            continue
        try:
            _save_single_pc(pc_id, pc)
            persisted += 1
        except Exception as e:
            persist_errors.append(f"{pc_id}: {e}")
            log.warning("backfill: persist failed for %s: %s", pc_id, e)

    log.info(
        "cchcs backfill: total=%d tagged_now=%d already=%d not_packet=%d persisted=%d",
        summary.get("total", 0),
        summary.get("tagged_now", 0),
        summary.get("already_tagged", 0),
        summary.get("not_packet", 0),
        persisted,
    )

    return jsonify({
        "ok": True,
        "total": summary.get("total", 0),
        "tagged_now": summary.get("tagged_now", 0),
        "already_tagged": summary.get("already_tagged", 0),
        "not_packet": summary.get("not_packet", 0),
        "tagged_ids": summary.get("tagged_ids", []),
        "persisted": persisted,
        "persist_errors": persist_errors,
    })
