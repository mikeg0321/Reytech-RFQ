"""Drive backup of confirmed observed-sends (PR-H of post-quote
queue item 23, 2026-05-07).

PR-G1 (#814) detected outbound RFQ-quote messages in Mike's Sent
folder. PR-G2 (#815) gave the operator a way to confirm/reject each
observation. This module is the third leg: when an observation is
confirmed, the operator can trigger a Drive backup that:

  1. Pulls the original outbound message from Gmail by gmail_message_id.
  2. Extracts every attachment (PDF quotes, 704B, CalRecycle 74,
     bid package, the operator's actual deliverable).
  3. Uploads them to Drive at:
        Backups/Sent Quote Packages/{year}/Q{quarter}/{record_id}/
  4. Records the Drive folder URL on the observation row's notes
     (no schema change — keeps the substrate small until we know
     what fields Mike wants surfaced in the future UI).

This is the "permanent searchable record of every sent quote"
substrate. Today operators rely on Mark-Sent-Manually + Gmail search;
neither is durable across years. Drive backup makes the sent quote
queryable by year/quarter and accessible alongside the buyer record.

This PR ships the helper + admin endpoint only. Auto-fire from
`observed_send_store.confirm()` is deliberately NOT wired — the
operator triggers backup explicitly so a Drive flake never silently
loses the confirm decision. Once the 8-week confirm-rate flip
(PR-G4) lands, auto-backup can ride alongside it.

Doctrine
--------
* Source of truth: Gmail server. We download the actual sent message,
  not what we generated. If the operator hand-edited a PDF before
  sending, the Drive backup reflects what the buyer received.
* Reytech Law 22: We never modify the observation row's status from
  this helper. Backup failure leaves the obs `confirmed` and the row
  surfaces a `backup_status` of error so the operator can retry.
* Idempotent. Re-running on the same observation finds the existing
  Drive folder and re-uploads any missing attachments.
"""
from __future__ import annotations

import email as _email
import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

_BACKUP_FOLDER_ROOT = ("Backups", "Sent Quote Packages")


def _parse_sent_at(date_header: str) -> datetime:
    """Best-effort parse of an RFC 2822 Date header.

    Returns a naive datetime. The fallback is `datetime.now()` so the
    folder placement degrades to "current quarter" instead of crashing
    when Gmail returns an oddly-formatted Date.
    """
    if not date_header:
        return datetime.now()
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_header)
        if dt is None:
            return datetime.now()
        # Strip tz so quarter math doesn't fight UTC offsets.
        return dt.replace(tzinfo=None)
    except Exception:
        return datetime.now()


def _quarter_for(dt: datetime) -> str:
    """Return 'Q1' .. 'Q4' for a datetime — calendar quarter."""
    q = (dt.month - 1) // 3 + 1
    return f"Q{q}"


def _safe_folder_segment(s: str) -> str:
    """Replace path-unfriendly chars in a folder name segment.
    Preserves spaces (Drive supports them) but strips slashes,
    backslashes, control chars, and reserved Windows chars."""
    if not s:
        return ""
    s = re.sub(r'[\\/\x00-\x1f<>:"|?*]', "_", str(s))
    return s.strip()[:120] or "unknown"


def _extract_attachments(raw_bytes: bytes) -> List[Tuple[str, str, bytes]]:
    """Walk an RFC 2822 message and return [(filename, mime_type, bytes), ...]
    for every attachment part. Skips inline images (Content-Disposition
    header missing or not 'attachment')."""
    out: List[Tuple[str, str, bytes]] = []
    try:
        msg = _email.message_from_bytes(raw_bytes)
    except Exception as e:
        log.error("attachment parse failed: %s", e)
        return out

    for part in msg.walk():
        if part.is_multipart():
            continue
        disp = (part.get("Content-Disposition") or "").lower()
        filename = part.get_filename() or ""
        if "attachment" not in disp and not filename:
            continue
        try:
            data = part.get_payload(decode=True) or b""
        except Exception as e:
            log.debug("attachment decode failed for %s: %s", filename, e)
            continue
        if not data:
            continue
        mime = part.get_content_type() or "application/octet-stream"
        out.append((filename or "unnamed", mime, data))
    return out


def _ensure_backup_folder_chain(year: int, quarter: str,
                                record_label: str) -> Optional[str]:
    """Idempotently create Backups/Sent Quote Packages/{year}/{quarter}/{record_label}/
    in Drive, return the leaf folder ID. None when Drive isn't configured."""
    try:
        from src.core.gdrive import (
            is_configured, _get_or_create_folder,
            GOOGLE_DRIVE_ROOT_FOLDER_ID,
        )
    except Exception as e:
        log.error("gdrive import failed: %s", e)
        return None
    if not is_configured() or not GOOGLE_DRIVE_ROOT_FOLDER_ID:
        return None
    parent = GOOGLE_DRIVE_ROOT_FOLDER_ID
    for seg in _BACKUP_FOLDER_ROOT + (str(year), quarter, record_label):
        parent = _get_or_create_folder(_safe_folder_segment(seg), parent)
        if not parent:
            log.error("folder create failed at segment %r", seg)
            return None
    return parent


def _set_backup_marker_on_obs(observation_id: int, *,
                              folder_id: str,
                              uploaded_count: int,
                              error: str = "") -> None:
    """Stamp the observation row's notes field with a structured tag
    so the UI can surface 'Backed up to Drive' or 'Backup error'."""
    try:
        from src.core.db import get_db
    except Exception as e:
        log.debug("db import failed: %s", e)
        return
    marker = json.dumps({
        "kind": "drive_backup",
        "at": datetime.now().isoformat(timespec="seconds"),
        "folder_id": folder_id,
        "folder_url": (f"https://drive.google.com/drive/folders/{folder_id}"
                       if folder_id else ""),
        "uploaded": uploaded_count,
        "error": error,
    })
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT notes FROM observed_sends WHERE id=?",
                (int(observation_id),),
            ).fetchone()
            if not row:
                return
            existing = (row["notes"] or "").strip()
            new_notes = (existing + ("\n" if existing else "")) + marker
            conn.execute(
                "UPDATE observed_sends SET notes=?, "
                "updated_at=datetime('now') WHERE id=?",
                (new_notes[:8000], int(observation_id)),
            )
            conn.commit()
    except Exception as e:
        log.debug("backup marker write suppressed: %s", e)


def backup_observation(observation_id: int) -> Dict:
    """Pull the Sent message + upload its attachments to Drive.

    Result keys:
      ok            : bool
      observation_id: int
      folder_id     : Drive folder id (when ok)
      folder_url    : public Drive URL (when ok)
      uploaded      : list of {filename, mime, file_id, bytes}
      skipped       : list of {filename, reason}
      error         : str (populated on hard failure)

    Idempotent. Re-running re-uses the same folder (gdrive
    `upload_bytes` updates existing same-name files in-place).
    """
    # 1. Load observation row.
    try:
        from src.core.db import get_db
    except Exception as e:
        return {"ok": False,
                "error": f"db unavailable: {e}",
                "observation_id": observation_id}

    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM observed_sends WHERE id=?",
            (int(observation_id),),
        ).fetchone()
    if not row:
        return {"ok": False, "error": "observation not found",
                "observation_id": observation_id}
    if row["status"] != "confirmed":
        return {"ok": False,
                "error": f"only confirmed observations can be backed up "
                         f"(status={row['status']})",
                "observation_id": observation_id}

    gmail_id = row["gmail_message_id"]
    if not gmail_id:
        return {"ok": False, "error": "observation has no gmail_message_id",
                "observation_id": observation_id}

    # 2. Pull the raw Sent message + parse attachments.
    try:
        from src.core.gmail_api import get_service, is_configured, get_raw_message
        if not is_configured():
            err = "Gmail not configured"
            _set_backup_marker_on_obs(observation_id, folder_id="",
                                      uploaded_count=0, error=err)
            return {"ok": False, "error": err,
                    "observation_id": observation_id}
        service = get_service("sales")
        raw = get_raw_message(service, gmail_id)
    except Exception as e:
        err = f"gmail fetch failed: {type(e).__name__}: {e}"
        _set_backup_marker_on_obs(observation_id, folder_id="",
                                  uploaded_count=0, error=err)
        return {"ok": False, "error": err,
                "observation_id": observation_id}

    attachments = _extract_attachments(raw)
    if not attachments:
        err = "no attachments in Sent message"
        _set_backup_marker_on_obs(observation_id, folder_id="",
                                  uploaded_count=0, error=err)
        return {"ok": False, "error": err,
                "observation_id": observation_id,
                "uploaded": [], "skipped": []}

    # 3. Compute folder placement.
    sent_at = _parse_sent_at(row["sent_at"] or "")
    year = sent_at.year
    quarter = _quarter_for(sent_at)
    label_parts = [
        row["matched_record_id"] or "no-record",
        row["match_value"] or "",
    ]
    record_label = " - ".join(p for p in label_parts if p) or "unknown"

    folder_id = _ensure_backup_folder_chain(year, quarter, record_label)
    if not folder_id:
        err = "Drive folder chain create failed"
        _set_backup_marker_on_obs(observation_id, folder_id="",
                                  uploaded_count=0, error=err)
        return {"ok": False, "error": err,
                "observation_id": observation_id}

    # 4. Upload each attachment.
    uploaded = []
    skipped = []
    try:
        from src.core.gdrive import upload_bytes
    except Exception as e:
        err = f"gdrive import failed: {e}"
        _set_backup_marker_on_obs(observation_id,
                                  folder_id=folder_id,
                                  uploaded_count=0, error=err)
        return {"ok": False, "error": err,
                "observation_id": observation_id}

    for filename, mime, data in attachments:
        clean_name = _safe_folder_segment(filename) or "unnamed"
        try:
            file_id = upload_bytes(data, folder_id, clean_name,
                                   mime_type=mime)
            uploaded.append({
                "filename": clean_name,
                "mime": mime,
                "file_id": file_id,
                "bytes": len(data),
            })
        except Exception as e:
            log.warning("upload failed for %s: %s", clean_name, e)
            skipped.append({
                "filename": clean_name,
                "reason": f"{type(e).__name__}: {e}",
            })

    err = "" if uploaded else "all uploads failed"
    _set_backup_marker_on_obs(observation_id,
                              folder_id=folder_id,
                              uploaded_count=len(uploaded),
                              error=err)
    folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

    return {
        "ok": bool(uploaded),
        "observation_id": observation_id,
        "folder_id": folder_id,
        "folder_url": folder_url,
        "uploaded": uploaded,
        "skipped": skipped,
        "error": err,
    }
