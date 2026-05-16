# routes_admin_export_eml.py — Admin: fetch raw RFC 2822 bytes for an inbox message.
#
# Path-B substrate primitive (2026-05-16) from
# project_handoff_2026_05_16_spine_to_production_bids.md §5.
#
# Lets an assistant or operator pull the original .eml for an RFQ subject
# (e.g. CCHCS sol# 10847457) without OAuth gymnastics. Auth-gated by the
# existing DASH_PASS / X-API-Key surface. Read-only: never modifies a
# Gmail message. Replaces the prior asks-Mike-for-credentials anti-pattern.

from flask import request, Response, jsonify
from src.api.shared import bp, auth_required
import logging
import re

log = logging.getLogger("reytech")

# Permissive enough to cover sol#s like "10847457", "PREQ 10847262",
# "RFQ-2026-001"; restrictive enough that the value is safe to log /
# reflect into a filename without escaping.
_SOL_RE = re.compile(r"^[A-Za-z0-9_\- ]{1,64}$")
_FILENAME_SANITIZE = re.compile(r"[^A-Za-z0-9_\-]")


@bp.route("/api/admin/export_eml", methods=["GET"])
@auth_required
def admin_export_eml():
    """Return raw RFC 2822 bytes for the first message matching the query.

    Query params:
      sol   — solicitation number, subject-line search. Validated.
      q     — free-form Gmail query (overrides sol). Power-user escape.
      inbox — "mike" or "sales"; default tries mike first then sales.

    Responses:
      200  message/rfc822 attachment, X-Reytech-{Inbox,Match-Count} headers
      400  missing/invalid params
      404  no message matched in any tried inbox
      503  Gmail OAuth not configured (env vars missing)
    """
    sol = (request.args.get("sol") or "").strip()
    q = (request.args.get("q") or "").strip()
    inbox_arg = (request.args.get("inbox") or "").strip().lower()

    if not sol and not q:
        return jsonify({"ok": False, "error": "sol or q required"}), 400
    if sol and not _SOL_RE.match(sol):
        return jsonify({"ok": False, "error": "sol failed validation"}), 400
    if inbox_arg and inbox_arg not in ("mike", "sales"):
        return jsonify({"ok": False, "error": "inbox must be mike or sales"}), 400

    try:
        from src.core.gmail_api import (
            is_configured, get_service, list_message_ids, get_raw_message,
        )
    except Exception as e:
        log.error("admin_export_eml: gmail_api import failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": "gmail_api unavailable"}), 503

    if not is_configured():
        return jsonify({"ok": False, "error": "Gmail OAuth not configured"}), 503

    query = q if q else f"subject:{sol}"
    inboxes = [inbox_arg] if inbox_arg else ["mike", "sales"]

    last_err = None
    for inbox in inboxes:
        try:
            svc = get_service(inbox)
            ids = list_message_ids(svc, query=query, max_results=5)
        except Exception as e:
            log.warning("admin_export_eml: list failed inbox=%s err=%s", inbox, e)
            last_err = str(e)
            continue
        if not ids:
            continue
        try:
            raw = get_raw_message(svc, ids[0])
        except Exception as e:
            log.error("admin_export_eml: get_raw failed inbox=%s id=%s err=%s",
                      inbox, ids[0], e, exc_info=True)
            last_err = str(e)
            continue
        base = sol or "message"
        filename = _FILENAME_SANITIZE.sub("_", base)[:60] + ".eml"
        return Response(
            raw,
            mimetype="message/rfc822",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Reytech-Match-Count": str(len(ids)),
                "X-Reytech-Inbox": inbox,
            },
        )

    return jsonify({
        "ok": False,
        "error": "not found",
        "detail": last_err or "no match",
        "tried_inboxes": inboxes,
        "query": query,
    }), 404
