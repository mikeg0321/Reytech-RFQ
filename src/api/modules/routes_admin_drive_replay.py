# routes_admin_drive_replay.py — Admin: backfill Drive forms-archive from Gmail SENT.
#
# Closes the substrate gap Mike caught 2026-05-25: Drive forms-archive last
# wrote on 2026-05-15 00:02 (folder `10847262`). Every quote Mike sent
# manually since then bypassed the operator-button workflow that fires
# `drive_triggers.on_quote_sent()` / `on_package_generated()`, so the
# archive went silent without any error surface.
#
# Workflow this endpoint enables:
#   1. Operator hits POST /api/admin/drive/replay?since=2026-05-15&dry_run=1
#   2. Endpoint scans Gmail SENT for threads with attachments + sol#-shaped
#      subjects since the given date.
#   3. For each, plans: which sol#, target Drive folder, which attachments
#      to upload (deduped against existing Drive contents).
#   4. dry_run=1 returns the plan as JSON for operator review.
#   5. dry_run=0 executes the uploads idempotently and returns the result.
#
# Sol# extraction patterns (in priority order):
#   - 10\d{6}             — CCHCS 8-digit PREQ #
#   - \d{2}CB\d{3,4}      — DSH (e.g., 25CB021)
#
# Idempotency:
#   - Drive uploads are deduped on (folder_id, filename). gdrive.upload_file
#     already handles this — it checks for an existing file with the same
#     name before creating, falling through to update if found.
#   - Gmail threads can be re-scanned freely; the endpoint never modifies
#     thread state.
#
# Auth: @auth_required (matches the rest of the /api/admin/ namespace).
#
# Companion: PR-1 of the substrate wave (continuous Gmail-SENT watcher)
# is the durable fix — this endpoint is the one-shot backfill.

import base64
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from typing import Optional

from flask import jsonify, request

from src.api.shared import auth_required, bp

log = logging.getLogger("reytech.drive_replay")


# ── Sol# extraction ──────────────────────────────────────────────────────

_SOL_PATTERNS = [
    # CCHCS / CDCR 8-digit PREQ
    (re.compile(r"\b(10\d{6})\b"), "cchcs_preq"),
    # DSH CBxxx format (e.g., 25CB021)
    (re.compile(r"\b(\d{2}CB\d{3,4})\b", re.IGNORECASE), "dsh_cb"),
]


def _extract_sol_number(subject: str) -> Optional[tuple[str, str]]:
    """Try each sol# pattern in priority order. Returns (sol#, pattern_id)
    on first match or None."""
    if not subject:
        return None
    for pat, pid in _SOL_PATTERNS:
        m = pat.search(subject)
        if m:
            return m.group(1), pid
    return None


# ── Gmail attachment download ────────────────────────────────────────────

def _download_attachment(service, msg_id: str, attachment_id: str) -> bytes:
    """Pull raw bytes for a Gmail attachment by id."""
    att = service.users().messages().attachments().get(
        userId="me", id=attachment_id, messageId=msg_id,
    ).execute()
    data = att.get("data", "")
    # Gmail API returns URL-safe base64.
    return base64.urlsafe_b64decode(data)


def _iter_attachments(msg_payload):
    """Walk a Gmail message payload tree, yielding (filename, mime, attachment_id, size)
    for every part with a filename + attachmentId."""
    stack = [msg_payload] if msg_payload else []
    while stack:
        part = stack.pop()
        if not isinstance(part, dict):
            continue
        body = part.get("body") or {}
        filename = part.get("filename") or ""
        attachment_id = body.get("attachmentId")
        if filename and attachment_id:
            yield (
                filename,
                part.get("mimeType") or "",
                attachment_id,
                int(body.get("size") or 0),
            )
        for child in (part.get("parts") or []):
            stack.append(child)


# ── Drive folder lookup ──────────────────────────────────────────────────

def _list_drive_files_in_folder(service, folder_id: str) -> dict[str, str]:
    """Return {filename: file_id} for direct children of a Drive folder.
    Used to dedupe uploads."""
    out: dict[str, str] = {}
    page_token: Optional[str] = None
    while True:
        req = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id,name)",
            pageSize=200,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_token,
        )
        results = req.execute()
        for f in results.get("files", []):
            out[f["name"]] = f["id"]
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    return out


# ── The endpoint ─────────────────────────────────────────────────────────

@bp.route("/api/admin/drive/replay", methods=["POST"])
@auth_required
def admin_drive_replay():
    """Backfill Drive forms-archive from Gmail SENT since a given date.

    Body (JSON):
        since (str, required): ISO date like "2026-05-15". Inclusive.
        dry_run (bool, default true): if true, return plan without uploading.

    Returns 200 with:
        {
          "ok": true,
          "dry_run": bool,
          "since": "...",
          "scanned_threads": int,
          "matched_sol_count": int,
          "plan": [
            {
              "sol_number": "10847776",
              "thread_id": "...", "message_id": "...",
              "subject": "...", "date": "...",
              "attachments": [{"filename": "...", "size": N, "mime": "..."}],
              "drive_folder_id": "...",
              "uploads_planned": [...],
              "uploads_skipped_existing": [...],
              "uploaded": [...],            # populated only when dry_run=false
              "errors": [...]               # populated only when dry_run=false
            },
            ...
          ],
          "totals": {
              "uploads_planned": int,
              "uploads_skipped_existing": int,
              "uploaded": int,
              "errors": int
          }
        }
    """
    try:
        body = request.get_json(silent=True) or {}
        since = body.get("since")
        if not since:
            return jsonify({"ok": False, "error": "missing 'since' date (ISO)"}), 400
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except Exception as e:
            return jsonify({
                "ok": False, "error": f"invalid 'since': {e}",
            }), 400
        dry_run = bool(body.get("dry_run", True))

        from src.core import gmail_api, gdrive
        if not gmail_api.is_configured():
            return jsonify({
                "ok": False, "error": "gmail_api not configured",
            }), 503
        if not gdrive.is_configured():
            return jsonify({
                "ok": False,
                "error": "gdrive not configured — set GOOGLE_DRIVE_CREDENTIALS + GOOGLE_DRIVE_ROOT_FOLDER_ID",
            }), 503

        gmail = gmail_api.get_service("sales")
        drive = gdrive._get_service()  # noqa: SLF001  — internal helper, reused intentionally

        # Build Gmail query — only SENT, only attachments, only since date.
        since_q = since_dt.strftime("%Y/%m/%d")
        q = f"in:sent has:attachment after:{since_q}"
        msg_ids = gmail_api.list_message_ids(gmail, q, max_results=200)

        plan: list[dict] = []
        seen_threads: set[str] = set()
        totals = {
            "uploads_planned": 0,
            "uploads_skipped_existing": 0,
            "uploaded": 0,
            "errors": 0,
        }

        # Year for Drive folder path
        year_for_folder = str(since_dt.year)

        for msg_id in msg_ids:
            try:
                msg = gmail.users().messages().get(
                    userId="me", id=msg_id, format="full",
                ).execute()
            except Exception as e:
                log.warning("messages.get failed for %s: %s", msg_id, e)
                totals["errors"] += 1
                continue

            thread_id = msg.get("threadId") or msg_id
            if thread_id in seen_threads:
                continue  # one reply per thread is enough

            payload = msg.get("payload") or {}
            headers = {h["name"].lower(): h["value"] for h in payload.get("headers", [])}
            subject = headers.get("subject", "")
            date_hdr = headers.get("date", "")

            sol_match = _extract_sol_number(subject)
            if sol_match is None:
                continue
            sol_num, sol_pattern = sol_match

            attachments = list(_iter_attachments(payload))
            if not attachments:
                continue

            seen_threads.add(thread_id)

            # Locate / plan Drive folder. We rely on the existing
            # get_folder_path("YYYY", category="Pending") helper to land at
            # the Pending parent for the year, then create/find the sol#
            # subfolder under it.
            try:
                pending_id = gdrive.get_folder_path(
                    year_for_folder, category="Pending",
                )
                sol_folder_id = gdrive._get_or_create_folder(  # noqa: SLF001
                    sol_num, pending_id,
                )
            except Exception as e:
                log.exception("drive folder resolve failed for sol=%s", sol_num)
                plan.append({
                    "sol_number": sol_num, "thread_id": thread_id,
                    "message_id": msg_id, "subject": subject, "date": date_hdr,
                    "attachments": [
                        {"filename": f, "size": s, "mime": m}
                        for (f, m, _aid, s) in attachments
                    ],
                    "drive_folder_id": None,
                    "uploads_planned": [],
                    "uploads_skipped_existing": [],
                    "uploaded": [],
                    "errors": [f"drive folder resolve failed: {type(e).__name__}: {e}"],
                })
                totals["errors"] += 1
                continue

            existing = _list_drive_files_in_folder(drive, sol_folder_id)

            planned_uploads: list[dict] = []
            skipped_existing: list[dict] = []
            for (filename, mime, attachment_id, size) in attachments:
                drive_name = filename if filename.startswith(sol_num) else f"{sol_num}_{filename}"
                if drive_name in existing:
                    skipped_existing.append({
                        "filename": drive_name, "drive_file_id": existing[drive_name],
                    })
                    totals["uploads_skipped_existing"] += 1
                else:
                    planned_uploads.append({
                        "filename": drive_name, "mime": mime, "size": size,
                        "attachment_id": attachment_id,
                    })
                    totals["uploads_planned"] += 1

            uploaded: list[dict] = []
            errors: list[str] = []

            if not dry_run and planned_uploads:
                for u in planned_uploads:
                    try:
                        raw = _download_attachment(gmail, msg_id, u["attachment_id"])
                        with tempfile.NamedTemporaryFile(
                            delete=False, suffix=os.path.splitext(u["filename"])[1] or ".bin",
                        ) as tf:
                            tf.write(raw)
                            tmp_path = tf.name
                        try:
                            file_id = gdrive.upload_file(
                                tmp_path, sol_folder_id, u["filename"], u["mime"],
                            )
                            if file_id:
                                uploaded.append({
                                    "filename": u["filename"], "drive_file_id": file_id,
                                })
                                totals["uploaded"] += 1
                            else:
                                errors.append(f"upload returned None for {u['filename']}")
                                totals["errors"] += 1
                        finally:
                            try:
                                os.unlink(tmp_path)
                            except OSError:
                                pass
                    except Exception as e:
                        log.exception("upload failed for sol=%s file=%s", sol_num, u["filename"])
                        errors.append(f"{u['filename']}: {type(e).__name__}: {e}")
                        totals["errors"] += 1

            plan.append({
                "sol_number": sol_num,
                "sol_pattern": sol_pattern,
                "thread_id": thread_id,
                "message_id": msg_id,
                "subject": subject,
                "date": date_hdr,
                "attachments": [
                    {"filename": f, "size": s, "mime": m}
                    for (f, m, _aid, s) in attachments
                ],
                "drive_folder_id": sol_folder_id,
                "uploads_planned": [
                    {"filename": p["filename"], "size": p["size"], "mime": p["mime"]}
                    for p in planned_uploads
                ],
                "uploads_skipped_existing": skipped_existing,
                "uploaded": uploaded,
                "errors": errors,
            })

        log.info(
            "drive replay since=%s dry_run=%s scanned=%d matched=%d planned=%d skipped=%d uploaded=%d errors=%d",
            since, dry_run, len(msg_ids), len(plan),
            totals["uploads_planned"], totals["uploads_skipped_existing"],
            totals["uploaded"], totals["errors"],
        )

        return jsonify({
            "ok": True,
            "dry_run": dry_run,
            "since": since,
            "scanned_threads": len(msg_ids),
            "matched_sol_count": len(plan),
            "plan": plan,
            "totals": totals,
        })

    except Exception as e:
        log.exception("admin_drive_replay failed")
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500
