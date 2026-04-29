"""Background watcher that auto-ingests SCPRS Detail Information exports
from a Drive folder.

Mike's directive 2026-04-28: "automate process, I dont want to do this
manually, should happen on email + SCPRS poller". The existing
`/api/admin/import-scprs-reytech-wins` endpoint already accepts the
HTML body and idempotently UPSERTs into `scprs_reytech_wins`. This
module wires a 5-minute poll loop on Drive `Backups/SCPRS Exports/`
so the operator workflow becomes:
  1. Export Detail Information from SCPRS portal
  2. Drag the .xls into Drive `Backups/SCPRS Exports/`
  3. Within 5 min the watcher picks it up + ingests + logs

Idempotent: skips files whose Drive `modifiedTime` is older than the
most-recent `imported_at` in `scprs_reytech_wins`. The `import_html`
function itself is also UPSERT-keyed on po_number so re-ingesting the
same file is harmless.

The watcher is observational (read Drive, write DB). It never modifies
or deletes Drive files.
"""
from __future__ import annotations

import io
import logging
import os
import threading
import time
from datetime import datetime
from typing import Optional

log = logging.getLogger("scprs_export_watcher")

WATCH_FOLDER_PATH = ["Backups", "SCPRS Exports"]
POLL_INTERVAL_SEC = 300  # 5 minutes

_thread: Optional[threading.Thread] = None
_status = {
    "running": False,
    "last_run_at": None,
    "last_file_seen": None,
    "files_ingested": 0,
    "scans_completed": 0,
    "last_error": None,
    "watch_path": "/".join(WATCH_FOLDER_PATH),
    "watch_folder_id": None,
    "bootstrap_status": None,
    "bootstrap_at": None,
}


def _find_watch_folder_id() -> Optional[str]:
    """Walk Drive root → Backups → SCPRS Exports. Returns None if any
    segment is missing — operator hasn't set up the folder yet.
    Read-only — never creates."""
    from src.core.gdrive import (
        is_configured, find_folder, GOOGLE_DRIVE_ROOT_FOLDER_ID,
    )
    if not is_configured() or not GOOGLE_DRIVE_ROOT_FOLDER_ID:
        return None
    parent = GOOGLE_DRIVE_ROOT_FOLDER_ID
    for seg in WATCH_FOLDER_PATH:
        nxt = find_folder(seg, parent)
        if not nxt:
            log.debug("watch path segment missing: %s", seg)
            return None
        parent = nxt
    return parent


def _ensure_watch_folder_id() -> Optional[str]:
    """Create the Backups/SCPRS Exports/ folder chain if missing,
    return its ID. Called once on watcher boot so the operator never
    has to manually create folders just to trigger auto-ingest.

    Idempotent — `_get_or_create_folder` is itself a get-or-create, so
    repeated boots find the existing folder and return its cached ID.

    Returns None if Drive isn't configured (no Drive creds, no root
    folder env). The watcher still starts in that case; scan_once just
    finds zero files.
    """
    from src.core.gdrive import (
        is_configured, _get_or_create_folder, GOOGLE_DRIVE_ROOT_FOLDER_ID,
    )
    if not is_configured() or not GOOGLE_DRIVE_ROOT_FOLDER_ID:
        _status["bootstrap_status"] = "skipped_no_drive_config"
        return None
    parent = GOOGLE_DRIVE_ROOT_FOLDER_ID
    try:
        for seg in WATCH_FOLDER_PATH:
            parent = _get_or_create_folder(seg, parent)
        _status["bootstrap_status"] = "ok"
        _status["watch_folder_id"] = parent
        _status["bootstrap_at"] = datetime.now().isoformat()
        log.info(
            "SCPRS watch folder ensured: %s (id=%s)",
            "/".join(WATCH_FOLDER_PATH), parent,
        )
        return parent
    except Exception as e:
        _status["bootstrap_status"] = f"error: {e}"
        log.error("ensure_watch_folder_id failed: %s", e, exc_info=True)
        return None


def _last_imported_at_iso() -> str:
    """Most-recent imported_at across all SCPRS-wins rows. Used as the
    watermark so we don't re-ingest files we already processed."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT MAX(imported_at) AS m FROM scprs_reytech_wins"
            ).fetchone()
        return (row["m"] or "") if row else ""
    except Exception:
        return ""


def _list_xls_in_watch_folder() -> list:
    """Return .xls files in the watch folder, sorted oldest→newest by
    modifiedTime so we ingest them in chronological order."""
    from src.core.gdrive import list_files
    fid = _find_watch_folder_id()
    if not fid:
        return []
    files = list_files(fid)
    xls = [f for f in files
           if (f.get("name") or "").lower().endswith(".xls")
           and f.get("mimeType") != "application/vnd.google-apps.folder"]
    xls.sort(key=lambda f: f.get("modifiedTime", ""))
    return xls


def _download_file_bytes(file_id: str) -> Optional[bytes]:
    """Download a Drive file's raw bytes. SCPRS exports are HTML disguised
    as .xls — we treat the bytes as text after decode."""
    try:
        from src.core.gdrive import _get_service
        from googleapiclient.http import MediaIoBaseDownload
        service = _get_service()
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()
    except Exception as e:
        log.error("Drive download failed for %s: %s", file_id, e)
        return None


def scan_once() -> dict:
    """Run one watcher cycle. Public so admin endpoints can trigger it
    on demand."""
    summary = {
        "scanned": 0,
        "ingested": 0,
        "skipped_older": 0,
        "errors": [],
        "files": [],
    }

    files = _list_xls_in_watch_folder()
    summary["scanned"] = len(files)
    last_iso = _last_imported_at_iso()

    for f in files:
        modified = f.get("modifiedTime", "")
        name = f.get("name", "")
        # Drive ISO is always Z-suffixed; SQLite imported_at is naive
        # ('2026-04-28 21:53:14'). Compare the date portions only — good
        # enough for 5-minute granularity, no TZ math needed.
        if last_iso and modified and modified[:19].replace("T", " ") <= last_iso[:19]:
            summary["skipped_older"] += 1
            continue

        data = _download_file_bytes(f["id"])
        if not data:
            summary["errors"].append(f"download_fail:{name}")
            continue

        try:
            from scripts.import_scprs_reytech_wins import import_html
            html = data.decode("utf-8", errors="replace")
            result = import_html(html, dry_run=False)
            if result.get("ok"):
                summary["ingested"] += 1
                summary["files"].append({
                    "name": name,
                    "parsed": result.get("pos_parsed", 0),
                    "inserted": result.get("pos_inserted", 0),
                    "updated": result.get("pos_updated", 0),
                    "items": result.get("items_total", 0),
                })
                _status["files_ingested"] += 1
                _status["last_file_seen"] = name
                log.info(
                    "scprs_export_watcher ingested %s: parsed=%d "
                    "updated=%d inserted=%d items=%d",
                    name, result.get("pos_parsed", 0),
                    result.get("pos_updated", 0),
                    result.get("pos_inserted", 0),
                    result.get("items_total", 0),
                )
                # Update watermark so subsequent files in this same scan
                # only ingest if they're newer than this one.
                last_iso = datetime.now().isoformat(sep=" ")[:19]
            else:
                summary["errors"].append(f"ingest_fail:{name}")
        except Exception as e:
            summary["errors"].append(f"exception:{name}:{e}")
            log.exception("scprs ingest failed for %s", name)

    _status["scans_completed"] += 1
    _status["last_run_at"] = datetime.now().isoformat()
    _status["last_error"] = "; ".join(summary["errors"]) if summary["errors"] else None
    return summary


def _run_loop():
    """Daemon-thread loop. Survives transient Drive outages — never dies
    on a single failed scan."""
    while True:
        try:
            scan_once()
        except Exception as e:
            log.error("scprs_export_watcher loop error: %s", e)
        time.sleep(POLL_INTERVAL_SEC)


def start_watcher() -> None:
    """Start the watcher in a daemon thread. Idempotent — calling twice
    in quick succession is a no-op.

    Bootstraps the Drive watch folder before starting the loop. If
    creation fails (missing creds / Drive 5xx), we still start the
    loop — `scan_once()` re-resolves the folder via `_find_watch_folder_id`
    each tick, so a recovered Drive will pick it up automatically.
    """
    global _thread
    if _thread and _thread.is_alive():
        return
    _ensure_watch_folder_id()
    _thread = threading.Thread(
        target=_run_loop, daemon=True, name="scprs-export-watcher"
    )
    _thread.start()
    _status["running"] = True
    log.info(
        "SCPRS export watcher started (interval=%ds, watch=%s, "
        "bootstrap=%s)",
        POLL_INTERVAL_SEC, "/".join(WATCH_FOLDER_PATH),
        _status.get("bootstrap_status"),
    )


def get_status() -> dict:
    """Snapshot of the watcher state for /health/quoting card."""
    return dict(_status)
