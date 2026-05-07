"""Admin endpoints for the observed-send detector + store
(PR-G2 of post-quote queue item 23, 2026-05-07).

Four endpoints:

  POST /api/admin/observed-sends/scan
      Body (optional): {"since_days": 7, "max_messages": 200}
      Runs the detector against Gmail Sent folder and persists each
      match as a pending observation. Returns the detector's raw
      result + the upsert summary.

  GET /api/admin/observed-sends?status=pending&limit=200
      List observations. status defaults to None (all). limit caps
      at 500 — the operator review UI rarely needs more.

  POST /api/admin/observed-sends/<id>/confirm
      Body (optional): {"by": "mike", "notes": "..."}
      Mark observation confirmed, append gmail_message_id to the
      matched record's gmail_message_ids list (PR #808 column /
      PR-E forward path).

  POST /api/admin/observed-sends/<id>/reject
      Body (optional): {"by": "mike", "reason": "..."}
      Mark observation rejected. Row stays in the table so future
      scans don't re-import the same message as a missed send.

UI for these endpoints lands in PR-G3 (CHROME-VERIFIED). Until then,
operator can curl them directly:

    curl -u "$BASIC_AUTH" -X POST \
      https://web-production-dcee9.up.railway.app/api/admin/observed-sends/scan \
      -H 'Content-Type: application/json' \
      -d '{"since_days": 14}'

Auto-attach (status='auto_attached', PR-G4) reuses the same `confirm`
helper but skips the operator gate after 8 weeks of 100% confirm rate.
This module exposes the gate; PR-G4 will add the auto-fire trigger.
"""
import json
import re
from flask import request, jsonify
from src.api.shared import bp, auth_required
from src.api.render import render_page
import logging
log = logging.getLogger("reytech.observed_sends")


_DRIVE_BACKUP_RE = re.compile(r'\{"kind":\s*"drive_backup".*?\}')


def _drive_backup_url_from_notes(notes: str) -> str:
    """Pull the most recent successful drive_backup folder URL out of
    an observation's notes field. The backup helper appends a JSON
    marker per attempt; surface the last one without an error so the
    UI can show '📁 Drive' next to confirmed observations.
    """
    if not notes:
        return ""
    last_url = ""
    for m in _DRIVE_BACKUP_RE.finditer(notes):
        try:
            payload = json.loads(m.group(0))
            if payload.get("kind") != "drive_backup":
                continue
            if payload.get("error"):
                continue
            url = payload.get("folder_url") or ""
            if url:
                last_url = url
        except (ValueError, TypeError):
            continue
    return last_url


@bp.route("/admin/observed-sends", methods=["GET"])
@auth_required
def page_observed_sends():
    """Operator review page — list pending observations with confirm /
    reject / backup buttons. PR-G3 of post-quote queue item 23.
    """
    try:
        from src.agents.observed_send_store import list_observed_sends
        status_filter = (request.args.get("status") or "").strip()
        try:
            limit = min(int(request.args.get("limit", 200) or 200), 500)
        except (TypeError, ValueError):
            limit = 200
        rows = list_observed_sends(
            status=status_filter or None, limit=limit) or []
        for r in rows:
            r["drive_backup_url"] = _drive_backup_url_from_notes(
                r.get("notes") or "")
        # Total count is across all statuses for the "all" tab badge.
        total_rows = list_observed_sends(status=None, limit=500) or []
        return render_page(
            "observed_sends.html",
            active_page="Observed Sends",
            rows=rows,
            status_filter=status_filter,
            total_count=len(total_rows),
        )
    except Exception as e:
        log.error("observed-sends page error: %s", e, exc_info=True)
        return render_page(
            "observed_sends.html",
            active_page="Observed Sends",
            rows=[],
            status_filter="",
            total_count=0,
            error=str(e),
        )


@bp.route("/api/admin/observed-sends/scan", methods=["POST"])
@auth_required
def api_observed_sends_scan():
    """Run the detector against Gmail Sent folder and persist matches."""
    try:
        body = request.get_json(silent=True) or {}
        since_days = int(body.get("since_days", 7))
        max_messages = int(body.get("max_messages", 200))

        from src.agents.observed_send import detect_observed_sends
        from src.agents.observed_send_store import upsert_from_detection

        result = detect_observed_sends(
            since_days=since_days, max_messages=max_messages)
        if not result.get("ok"):
            return jsonify({"ok": False,
                            "error": result.get("error",
                                                "detection failed"),
                            "detection": result}), 503

        upsert = upsert_from_detection(result)
        return jsonify({
            "ok": True,
            "detection": {
                "since_days": result["since_days"],
                "scanned": result["scanned"],
                "matches": len(result["matches"]),
                "unmatched": len(result["unmatched"]),
                "skipped_non_quote": result["skipped_non_quote"],
            },
            "upsert": upsert,
        })
    except Exception as e:
        log.error("observed-sends scan error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/admin/observed-sends", methods=["GET"])
@auth_required
def api_observed_sends_list():
    """List observations. ?status= filters; ?limit= caps results."""
    try:
        status = (request.args.get("status") or "").strip() or None
        limit = min(int(request.args.get("limit", 200)), 500)

        from src.agents.observed_send_store import list_observed_sends
        rows = list_observed_sends(status=status, limit=limit)
        return jsonify({
            "ok": True,
            "status_filter": status,
            "count": len(rows),
            "rows": rows,
        })
    except Exception as e:
        log.error("observed-sends list error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/admin/observed-sends/<int:obs_id>/confirm",
          methods=["POST"])
@auth_required
def api_observed_sends_confirm(obs_id):
    """Confirm observation; attaches gmail_message_id to record."""
    try:
        body = request.get_json(silent=True) or {}
        by = (body.get("by") or "operator").strip()[:100]
        notes = (body.get("notes") or "").strip()[:500]

        from src.agents.observed_send_store import confirm
        result = confirm(obs_id, by=by, notes=notes)
        if not result.get("ok"):
            return jsonify(result), 400 \
                if result.get("error") else 500
        return jsonify(result)
    except Exception as e:
        log.error("observed-sends confirm error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/admin/observed-sends/<int:obs_id>/reject",
          methods=["POST"])
@auth_required
def api_observed_sends_reject(obs_id):
    """Reject observation; row stays so future scans skip it."""
    try:
        body = request.get_json(silent=True) or {}
        by = (body.get("by") or "operator").strip()[:100]
        reason = (body.get("reason") or "").strip()[:500]

        from src.agents.observed_send_store import reject
        result = reject(obs_id, by=by, reason=reason)
        if not result.get("ok"):
            return jsonify(result), 400 \
                if result.get("error") else 500
        return jsonify(result)
    except Exception as e:
        log.error("observed-sends reject error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/admin/observed-sends/<int:obs_id>/backup-to-drive",
          methods=["POST"])
@auth_required
def api_observed_sends_backup_to_drive(obs_id):
    """Pull the original Sent Gmail message + upload its attachments
    to Drive at Backups/Sent Quote Packages/{year}/{quarter}/{record}/.
    Operator-driven; only runs when the observation is already
    confirmed. Idempotent — re-running re-uses the same folder.
    """
    try:
        from src.agents.observed_send_backup import backup_observation
        result = backup_observation(obs_id)
        if not result.get("ok"):
            return jsonify(result), 400 \
                if result.get("error") else 500
        return jsonify(result)
    except Exception as e:
        log.error("observed-sends backup error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
