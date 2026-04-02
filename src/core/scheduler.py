"""
Centralized Scheduler & Thread Health Monitor (F4 + F5)

Wraps existing daemon threads with observability:
- Tracks all registered background jobs
- Monitors heartbeats, detects dead jobs
- Daily SQLite backups with rotation
- GET /api/scheduler/status — full job dashboard
"""

import os
import time
import shutil
import sqlite3
import threading
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable


log = logging.getLogger("reytech.scheduler")

# ── Graceful Shutdown ────────────────────────────────────────────────────────

_shutdown_event = threading.Event()


def request_shutdown():
    """Signal all background threads to stop."""
    _shutdown_event.set()
    log.info("Shutdown requested — all background threads signaled")


def should_run():
    """Check if threads should continue running. Returns False after shutdown requested."""
    return not _shutdown_event.is_set()


# ── Job Registry ──────────────────────────────────────────────────────────────

_jobs = {}  # name -> JobInfo
_lock = threading.Lock()


class JobInfo:
    """Tracks a registered background job's health."""
    __slots__ = (
        "name", "interval_sec", "func", "thread",
        "last_run", "last_success", "last_error",
        "run_count", "error_count", "started_at", "status",
    )

    def __init__(self, name: str, interval_sec: int, func: Optional[Callable] = None):
        self.name = name
        self.interval_sec = interval_sec
        self.func = func
        self.thread: Optional[threading.Thread] = None
        self.last_run: Optional[str] = None
        self.last_success: Optional[str] = None
        self.last_error: Optional[str] = None
        self.run_count = 0
        self.error_count = 0
        self.started_at: Optional[str] = None
        self.status = "registered"

    def to_dict(self) -> dict:
        is_alive = self.thread.is_alive() if self.thread else False
        # Dead job detection: no heartbeat in 3x interval
        is_dead = False
        if self.last_run and self.interval_sec > 0:
            last = datetime.fromisoformat(self.last_run)
            if (datetime.now(timezone.utc) - last).total_seconds() > self.interval_sec * 3:
                is_dead = True

        return {
            "name": self.name,
            "interval_sec": self.interval_sec,
            "status": "dead" if is_dead else ("running" if is_alive else self.status),
            "thread_alive": is_alive,
            "last_run": self.last_run,
            "last_success": self.last_success,
            "last_error": self.last_error,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "started_at": self.started_at,
        }


def register_job(name: str, interval_sec: int = 0, func: Callable = None) -> JobInfo:
    """Register a background job for health tracking."""
    with _lock:
        job = JobInfo(name, interval_sec, func)
        _jobs[name] = job
        log.debug("Job registered: %s (interval=%ds)", name, interval_sec)
        return job


def heartbeat(name: str, success: bool = True, error: str = None):
    """Called by background jobs to report health."""
    with _lock:
        job = _jobs.get(name)
        if not job:
            job = JobInfo(name, 0)
            _jobs[name] = job
        now = datetime.now(timezone.utc).isoformat()
        job.last_run = now
        job.run_count += 1
        if success:
            job.last_success = now
            job.status = "running"
        else:
            job.error_count += 1
            job.last_error = error or "unknown"
            job.status = "error"


def mark_started(name: str, thread: threading.Thread = None):
    """Mark a job as started with its thread reference."""
    with _lock:
        job = _jobs.get(name)
        if not job:
            job = JobInfo(name, 0)
            _jobs[name] = job
        job.started_at = datetime.now(timezone.utc).isoformat()
        job.status = "running"
        if thread:
            job.thread = thread


def get_all_jobs() -> list:
    """Get status of all registered jobs."""
    with _lock:
        dead_jobs = []
        result = []
        for name, job in _jobs.items():
            info = job.to_dict()
            result.append(info)
            if info["status"] == "dead":
                dead_jobs.append(name)

    # Log critical for dead jobs (outside lock)
    for name in dead_jobs:
        log.critical("DEAD JOB DETECTED: %s — no heartbeat in 3x interval", name)

    return result


def restart_dead_jobs():
    """Check for dead jobs and restart them if a func reference is stored."""
    restarted = []
    with _lock:
        for name, job in _jobs.items():
            if not job.func:
                continue
            is_alive = job.thread.is_alive() if job.thread else False
            if is_alive:
                continue
            # Check if job was previously running and has gone silent
            if job.last_run and job.interval_sec > 0:
                try:
                    last = datetime.fromisoformat(job.last_run)
                    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
                    if elapsed > job.interval_sec * 3:
                        # Dead — restart it
                        t = threading.Thread(target=job.func, daemon=True, name=f"restart-{name}")
                        t.start()
                        job.thread = t
                        job.started_at = datetime.now(timezone.utc).isoformat()
                        job.status = "restarted"
                        restarted.append(name)
                        log.warning("RESTARTED dead job: %s (was silent for %.0fs)", name, elapsed)
                except Exception as e:
                    log.error("Failed to restart job %s: %s", name, e)
    return restarted


def start_watchdog(check_interval: int = 300):
    """Start a watchdog thread that restarts dead jobs every check_interval seconds."""
    def _watchdog_loop():
        time.sleep(120)  # Wait 2 min after boot before first check
        while True:
            try:
                restarted = restart_dead_jobs()
                if restarted:
                    try:
                        from src.agents.notify_agent import send_alert
                        send_alert(
                            event_type="job_restarted",
                            title=f"Restarted {len(restarted)} dead job(s)",
                            body=f"Jobs restarted: {', '.join(restarted)}",
                            urgency="warning",
                            channels=["bell"],
                            run_async=False,
                        )
                    except Exception:
                        pass
            except Exception as e:
                log.error("Watchdog error: %s", e)
            time.sleep(check_interval)

    t = threading.Thread(target=_watchdog_loop, daemon=True, name="job-watchdog")
    t.start()
    log.info("Job watchdog started (check every %ds)", check_interval)


# ── Database Backup (F5) ─────────────────────────────────────────────────────

def _get_data_dir():
    try:
        from src.core.paths import DATA_DIR
        return DATA_DIR
    except ImportError:
        return os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))), "data")


def run_backup(data_dir: str = None) -> dict:
    """
    Create a SQLite backup using .backup API.
    Runs VACUUM first to compact the DB, then backs up.
    Rotates: keep 3 daily + 1 weekly.
    """
    data_dir = data_dir or _get_data_dir()
    db_path = os.path.join(data_dir, "reytech.db")
    backup_dir = os.path.join(data_dir, "backups")
    os.makedirs(backup_dir, exist_ok=True)

    if not os.path.exists(db_path):
        return {"ok": False, "error": "Database file not found"}

    now = datetime.now(timezone.utc)
    filename = f"reytech_{now.strftime('%Y%m%d_%H%M%S')}.db"
    backup_path = os.path.join(backup_dir, filename)

    try:
        # VACUUM disabled — DB is 577MB, VACUUM needs 2x memory (1.2GB)
        # Run /api/disk-cleanup?action=vacuum manually once to shrink to ~10MB
        # After that, re-enable auto-VACUUM
        # try:
        #     vc = sqlite3.connect(db_path, timeout=60)
        #     vc.execute("VACUUM")
        #     vc.close()
        # except Exception:
        #     pass

        # Use sqlite3 backup API (safe, consistent snapshot)
        src_conn = sqlite3.connect(db_path, timeout=30)
        dst_conn = sqlite3.connect(backup_path, timeout=15)
        src_conn.backup(dst_conn)
        dst_conn.close()
        src_conn.close()

        size = os.path.getsize(backup_path)
        log.info("Backup created: %s (%s)", filename, _fmt_size(size))

        # Rotate old backups (3 daily + 1 weekly)
        _rotate_backups(backup_dir, keep_daily=1, keep_weekly=0)  # 577MB × 3 = 1.7GB, too much

        heartbeat("db-backup", success=True)
        return {
            "ok": True,
            "filename": filename,
            "size": size,
            "size_human": _fmt_size(size),
            "created_at": now.isoformat(),
        }

    except Exception as e:
        log.error("Backup failed: %s", e)
        heartbeat("db-backup", success=False, error=str(e))
        return {"ok": False, "error": str(e)}


def _rotate_backups(backup_dir: str, keep_daily: int = 7, keep_weekly: int = 4):
    """Keep last N daily + M weekly backups, delete the rest."""
    files = sorted(
        [f for f in os.listdir(backup_dir) if f.startswith("reytech_") and f.endswith(".db")],
        reverse=True  # newest first
    )

    if len(files) <= keep_daily:
        return  # Nothing to rotate

    now = datetime.now(timezone.utc)
    kept = set()
    weekly_kept = 0

    for i, f in enumerate(files):
        if i < keep_daily:
            kept.add(f)
            continue
        # Parse date from filename
        try:
            date_str = f.replace("reytech_", "").replace(".db", "")[:8]
            file_date = datetime.strptime(date_str, "%Y%m%d")
            # Keep if it's a Sunday (weekly) and we haven't kept enough weeklies
            if file_date.weekday() == 6 and weekly_kept < keep_weekly:
                kept.add(f)
                weekly_kept += 1
        except ValueError:
            pass

    # Delete files not in kept set
    deleted = 0
    for f in files:
        if f not in kept:
            try:
                os.remove(os.path.join(backup_dir, f))
                deleted += 1
            except OSError:
                pass
    if deleted:
        log.info("Rotated %d old backups (kept %d daily + %d weekly)", deleted, keep_daily, weekly_kept)


def list_backups(data_dir: str = None) -> list:
    """List available backups with metadata."""
    data_dir = data_dir or _get_data_dir()
    backup_dir = os.path.join(data_dir, "backups")
    if not os.path.exists(backup_dir):
        return []

    backups = []
    for f in sorted(os.listdir(backup_dir), reverse=True):
        if f.startswith("reytech_") and f.endswith(".db"):
            path = os.path.join(backup_dir, f)
            stat = os.stat(path)
            backups.append({
                "filename": f,
                "size": stat.st_size,
                "size_human": _fmt_size(stat.st_size),
                "created_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
    return backups


def backup_health(data_dir: str = None) -> dict:
    """Check if backups are healthy (latest < 36h old)."""
    backups = list_backups(data_dir)
    if not backups:
        return {"healthy": False, "reason": "No backups found", "latest": None}
    latest = backups[0]
    latest_time = datetime.fromisoformat(latest["created_at"])
    age_hours = (datetime.now(timezone.utc) - latest_time).total_seconds() / 3600
    return {
        "healthy": age_hours < 36,
        "latest": latest["filename"],
        "age_hours": round(age_hours, 1),
        "count": len(backups),
    }


def _fmt_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


# ── Backup Scheduler Thread ──────────────────────────────────────────────────

_backup_thread_started = False


def start_backup_scheduler(interval_hours: int = 24):
    """Start a background thread that runs daily backups."""
    global _backup_thread_started
    if _backup_thread_started:
        return
    _backup_thread_started = True

    register_job("db-backup", interval_sec=interval_hours * 3600)

    def _backup_loop():
        # Delay first backup — don't compete with boot for CPU/memory
        # 577MB VACUUM + backup was killing Railway on every deploy
        time.sleep(6 * 3600)  # 6 hours after boot
        while True:
            try:
                result = run_backup()
                if result.get("ok"):
                    log.info("Scheduled backup OK: %s", result.get("filename"))
                else:
                    log.error("Scheduled backup failed: %s", result.get("error"))
            except Exception as e:
                log.error("Backup scheduler error: %s", e)
                heartbeat("db-backup", success=False, error=str(e))
            time.sleep(interval_hours * 3600)

    t = threading.Thread(target=_backup_loop, daemon=True, name="db-backup")
    t.start()
    mark_started("db-backup", t)
    log.info("Backup scheduler started (every %dh)", interval_hours)
