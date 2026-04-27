# routes_buyer_templates.py
#
# Phase 1.6 PR3c (2026-04-26).
#
# Surfaces buyer_template_candidates for review/promotion. Every
# incoming attached PDF gets fingerprinted; candidates that don't match
# an existing FormProfile show up here and the operator can promote
# them to a buyer-specific YAML (PR3f) or ignore.
#
# Read-only in this PR + a manual-scan endpoint to backfill from
# already-imported quotes.

import logging

from flask import jsonify

from src.api.shared import bp, auth_required
from src.agents.buyer_template_capture import (
    list_candidates,
    register_attachment,
    get_candidate_for_fingerprint,
)

log = logging.getLogger("reytech")


@bp.route("/api/buyer-templates/candidates", methods=["GET"])
@auth_required
def api_buyer_template_candidates():
    """List candidate buyer templates pending review."""
    from flask import request
    status = (request.args.get("status") or "candidate").strip()
    limit = int(request.args.get("limit") or 200)
    rows = list_candidates(status=status, limit=limit)
    # Truncate fingerprints in response (full hex is 64 chars; 16 is enough)
    for r in rows:
        if r.get("fingerprint"):
            r["fingerprint"] = r["fingerprint"][:16]
    return jsonify({
        "ok": True,
        "count": len(rows),
        "status": status,
        "candidates": rows,
    })


@bp.route("/api/buyer-templates/scan/<quote_type>/<quote_id>",
          methods=["POST"])
@auth_required
def api_buyer_template_scan(quote_type, quote_id):
    """Run capture on every attachment of the given quote.

    Useful for backfilling candidates from already-imported quotes
    before the auto-hook in PR3h ships.
    """
    qt = (quote_type or "").lower().strip()
    if qt not in ("pc", "rfq"):
        return jsonify({"ok": False, "error": "quote_type must be pc or rfq"}), 400

    try:
        from src.agents.fill_plan_builder import _list_attachments, _resolve_agency, _load_quote
    except ImportError as e:
        return jsonify({"ok": False, "error": f"deps missing: {e}"}), 500

    qd = _load_quote(quote_id, qt)
    if not qd:
        return jsonify({"ok": False, "error": "quote not found"}), 404

    agency_key, _ = _resolve_agency(qd)
    attached = _list_attachments(quote_id, qt) or []

    results = []
    for att in attached:
        r = register_attachment(quote_id, qt, att, agency_key=agency_key)
        results.append({
            "filename": att.get("filename", ""),
            "outcome": r.get("status", "error"),
            "fingerprint": r.get("fingerprint", ""),
            "form_type_guess": r.get("form_type_guess", ""),
            "profile_id": r.get("profile_id", ""),
            "candidate_id": r.get("candidate_id"),
        })

    summary = {
        "scanned": len(results),
        "matched_profile": sum(1 for r in results if r["outcome"] == "matched_profile"),
        "new_candidates": sum(1 for r in results if r["outcome"] == "new_candidate"),
        "existing_candidates": sum(1 for r in results if r["outcome"] == "existing_candidate"),
        "skipped": sum(1 for r in results
                       if r["outcome"] in ("skipped_no_pdf", "skipped_no_fingerprint")),
    }
    return jsonify({"ok": True, "agency_key": agency_key,
                    "summary": summary, "results": results})


@bp.route("/api/buyer-templates/lookup/<fingerprint>", methods=["GET"])
@auth_required
def api_buyer_template_lookup(fingerprint):
    """Look up a candidate by fingerprint (full or truncated)."""
    from flask import request
    agency_key = (request.args.get("agency") or "").strip()
    # Allow truncated fingerprint by doing prefix lookup if length < 64
    if len(fingerprint) < 64:
        from src.core.db import get_db
        try:
            with get_db() as conn:
                row = conn.execute(
                    """SELECT id, fingerprint, agency_key, form_type_guess,
                              status, seen_count, promoted_profile_id
                       FROM buyer_template_candidates
                       WHERE fingerprint LIKE ? || '%'
                       LIMIT 1""",
                    (fingerprint,),
                ).fetchone()
                if row:
                    return jsonify({"ok": True, "candidate": dict(row)})
        except Exception as e:
            log.debug("buyer-template lookup prefix error: %s", e)

    cand = get_candidate_for_fingerprint(fingerprint, agency_key=agency_key)
    if not cand:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "candidate": cand})
