"""Flask routes for the CCHCS Non-IT RFQ Packet automation.

One endpoint:
    POST /api/pricecheck/<pcid>/cchcs-packet/generate

Reads the PC's source PDF, parses it via cchcs_packet_parser, matches
items against all active PCs via cchcs_pc_matcher, fills the packet
via cchcs_packet_filler, and returns a download URL for the
`<source>_Reytech.pdf` output.

Built 2026-04-13 overnight. Phase 4 of 5. See
_overnight_review/MORNING_REVIEW.md.
"""
import logging
import os

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route
from src.core.paths import DATA_DIR

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
    output_dir = os.path.dirname(source_pdf) or DATA_DIR
    fill_result = fill_cchcs_packet(
        source_pdf=source_pdf,
        parsed=parsed,
        output_dir=output_dir,
        price_overrides=match_result.get("price_overrides"),
    )
    if not fill_result.get("ok"):
        return jsonify({
            "ok": False,
            "error": f"fill failed: {fill_result.get('error')}",
            "match_result": match_result,
        }), 500

    output_path = fill_result["output_path"]
    output_name = os.path.basename(output_path)
    download_url = f"/api/pricecheck/download/{output_name}"

    # ── Persist output_pdf on the PC so the UI shows a download button ──
    try:
        pc["output_pdf"] = output_path
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
    })


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
