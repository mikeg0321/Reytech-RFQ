"""
Drive Backup Agent — Nightly backups + Disaster Recovery

1. Nightly backup: snapshots all JSON + SQLite files to Drive/Backups/YYYY-MM-DD/
2. Disaster recovery: on startup, if local data is empty, restore from latest backup
3. Backup validation: verifies JSON integrity before uploading
4. Failure alerting: tracks last successful backup timestamp
"""

import os
import json
import shutil
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("reytech.drive_backup")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# Files to back up — dynamically scan all JSON, excluding caches
import glob as _glob
_BACKUP_EXCLUDE = {"product_research_cache.json", "scprs_public_cache.json", "qa_reports.json"}
BACKUP_FILES = sorted(
    os.path.basename(f) for f in _glob.glob(os.path.join(DATA_DIR, "*.json"))
    if os.path.basename(f) not in _BACKUP_EXCLUDE
)

BACKUP_DBS = [
    "reytech.db",
    "catalog.db",
]

LAST_BACKUP_FILE = os.path.join(DATA_DIR, ".last_backup_timestamp")
_backup_scheduler_started = False


def run_nightly_backup(force: bool = False) -> dict:
    """Run full backup to Google Drive. Returns result summary."""
    from src.core.gdrive import is_configured, get_folder_path, upload_file, _audit

    if not is_configured():
        return {"ok": False, "error": "Google Drive not configured"}

    # Check if already backed up today (unless forced)
    today = datetime.now().strftime("%Y-%m-%d")
    if not force and os.path.exists(LAST_BACKUP_FILE):
        try:
            with open(LAST_BACKUP_FILE) as f:
                last = f.read().strip()
            if last == today:
                return {"ok": True, "skipped": True, "last_backup": today}
        except Exception:
            pass

    result = {
        "ok": True,
        "date": today,
        "files_uploaded": 0,
        "files_failed": 0,
        "errors": [],
        "validated": 0,
    }

    try:
        # Get backup folder: Backups/YYYY-MM-DD/
        backups_root = get_folder_path(category="Backups")
        from src.core.gdrive import _get_or_create_folder
        day_folder = _get_or_create_folder(today, backups_root)

        # Validate and upload JSON files
        for fname in BACKUP_FILES:
            fpath = os.path.join(DATA_DIR, fname)
            if not os.path.exists(fpath):
                continue

            # Validate JSON integrity
            try:
                with open(fpath) as f:
                    data = json.load(f)
                if isinstance(data, (dict, list)):
                    result["validated"] += 1
                else:
                    result["errors"].append(f"{fname}: not a dict/list")
                    continue
            except json.JSONDecodeError as e:
                result["errors"].append(f"{fname}: invalid JSON — {e}")
                result["files_failed"] += 1
                continue

            # Upload
            try:
                upload_file(fpath, day_folder, fname)
                result["files_uploaded"] += 1
            except Exception as e:
                result["errors"].append(f"{fname}: upload failed — {e}")
                result["files_failed"] += 1

        # Upload SQLite databases
        for dbname in BACKUP_DBS:
            dbpath = os.path.join(DATA_DIR, dbname)
            if not os.path.exists(dbpath):
                continue
            try:
                # Use sqlite3.backup() for a consistent snapshot (safe during writes)
                import sqlite3
                tmp = dbpath + ".backup"
                src_conn = sqlite3.connect(dbpath, timeout=30)
                dst_conn = sqlite3.connect(tmp)
                src_conn.backup(dst_conn)
                dst_conn.close()
                src_conn.close()
                upload_file(tmp, day_folder, dbname)
                os.remove(tmp)
                result["files_uploaded"] += 1
            except Exception as e:
                result["errors"].append(f"{dbname}: {e}")
                result["files_failed"] += 1

        # Upload the drive audit log itself
        from src.core.gdrive import AUDIT_LOG_PATH, DRIVE_INDEX_PATH
        for extra in [AUDIT_LOG_PATH, DRIVE_INDEX_PATH]:
            if os.path.exists(extra):
                try:
                    upload_file(extra, day_folder, os.path.basename(extra))
                    result["files_uploaded"] += 1
                except Exception:
                    pass

        # Record success
        _audit("backup_complete", f"backup_{today}", day_folder, "",
               result["files_uploaded"])

        # Save timestamp
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(LAST_BACKUP_FILE, "w") as f:
            f.write(today)

        if result["files_failed"] > 0:
            result["ok"] = False

        log.info("Nightly backup complete: %d files uploaded, %d failed, %d validated",
                 result["files_uploaded"], result["files_failed"], result["validated"])

    except Exception as e:
        result["ok"] = False
        result["errors"].append(f"Backup failed: {e}")
        log.error("Nightly backup failed: %s", e, exc_info=True)

    return result


def check_and_restore() -> dict:
    """
    Disaster recovery: if local data is empty, restore from Drive backup.
    Called on app startup.
    """
    from src.core.gdrive import is_configured

    if not is_configured():
        return {"restored": False, "reason": "Drive not configured"}

    # Check if local data exists
    essential_files = ["rfqs.json", "reytech.db"]
    has_data = any(
        os.path.exists(os.path.join(DATA_DIR, f)) and
        os.path.getsize(os.path.join(DATA_DIR, f)) > 10
        for f in essential_files
    )

    if has_data:
        return {"restored": False, "reason": "Local data exists"}

    log.warning("LOCAL DATA MISSING — attempting restore from Drive backup")

    try:
        from src.core.gdrive import get_folder_path, list_files, download_file, _get_or_create_folder

        backups_root = get_folder_path(category="Backups")
        # Find most recent backup folder
        backup_folders = list_files(backups_root)
        # Sort by name (YYYY-MM-DD format sorts chronologically)
        backup_folders = sorted(
            [f for f in backup_folders if f.get("mimeType") == "application/vnd.google-apps.folder"],
            key=lambda x: x.get("name", ""),
            reverse=True
        )

        if not backup_folders:
            return {"restored": False, "reason": "No backups found on Drive"}

        latest = backup_folders[0]
        latest_files = list_files(latest["id"])

        restored = []
        for bf in latest_files:
            fname = bf.get("name", "")
            local_path = os.path.join(DATA_DIR, fname)
            os.makedirs(DATA_DIR, exist_ok=True)

            if download_file(bf["id"], local_path):
                restored.append(fname)
                log.info("Restored from backup: %s", fname)

        log.warning("DISASTER RECOVERY: Restored %d files from backup dated %s",
                     len(restored), latest["name"])

        return {
            "restored": True,
            "backup_date": latest["name"],
            "files_restored": restored,
            "count": len(restored),
        }

    except Exception as e:
        log.error("Disaster recovery failed: %s", e, exc_info=True)
        return {"restored": False, "reason": f"Error: {e}"}


def get_backup_health() -> dict:
    """Return backup health status for dashboard display."""
    last_backup = None
    if os.path.exists(LAST_BACKUP_FILE):
        try:
            with open(LAST_BACKUP_FILE) as f:
                last_backup = f.read().strip()
        except Exception:
            pass

    from src.core.gdrive import is_configured

    hours_since = None
    if last_backup:
        try:
            last_dt = datetime.strptime(last_backup, "%Y-%m-%d")
            hours_since = (datetime.now() - last_dt).total_seconds() / 3600
        except Exception:
            pass

    return {
        "configured": is_configured(),
        "last_backup": last_backup,
        "hours_since_backup": round(hours_since) if hours_since else None,
        "overdue": hours_since and hours_since > 48,
        "status": "ok" if (last_backup and hours_since and hours_since < 48)
                  else "overdue" if hours_since and hours_since > 48
                  else "never" if not last_backup
                  else "unknown",
    }


# ═══════════════════════════════════════════════════════════════════════
# Scheduler
# ═══════════════════════════════════════════════════════════════════════

def start_backup_scheduler():
    """Start background thread that runs nightly backup at 11pm PST."""
    global _backup_scheduler_started
    if _backup_scheduler_started:
        return
    _backup_scheduler_started = True

    def _loop():
        from src.core.scheduler import _shutdown_event
        _shutdown_event.wait(120)  # Wait 2 min after startup
        if _shutdown_event.is_set():
            log.info("Shutdown requested — drive backup exiting before first cycle")
            return
        # Run disaster recovery check on startup
        try:
            result = check_and_restore()
            if result.get("restored"):
                log.warning("Startup restore: %s", result)
        except Exception as e:
            log.error("Startup restore check failed: %s", e)

        while not _shutdown_event.is_set():
            try:
                # PST = UTC-8
                now_pst = datetime.now(timezone(timedelta(hours=-8)))
                if now_pst.hour == 23 and now_pst.minute < 15:
                    result = run_nightly_backup()
                    if not result.get("ok") and not result.get("skipped"):
                        # Alert on failure
                        try:
                            from src.agents.notify_agent import send_alert
                            send_alert(
                                event_type="backup_failed",
                                title="⚠️ Drive Backup Failed",
                                body=f"Errors: {result.get('errors', [])}",
                                urgency="high",
                                cooldown_key="backup_fail",
                            )
                        except Exception:
                            pass
            except Exception as e:
                log.error("Backup scheduler error: %s", e)
            _shutdown_event.wait(900)  # Wakes immediately on shutdown
        log.info("Shutdown requested — drive backup exiting")

    t = threading.Thread(target=_loop, daemon=True, name="drive-backup")
    t.start()
    log.info("Drive backup scheduler started (runs at 11pm PST)")
