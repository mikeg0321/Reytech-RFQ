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
    """Signal all background threads to stop and clean up thread-local DB connections."""
    _shutdown_event.set()
    log.info("Shutdown requested — all background threads signaled")
    # Close thread-local DB connection for THIS thread (main/signal handler thread)
    try:
        from src.core.db import close_thread_db
        close_thread_db()
    except Exception as _e:
        log.debug("suppressed: %s", _e)


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
        "restart_func", "restart_count", "max_restarts",
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
        self.restart_func: Optional[Callable] = None
        self.restart_count = 0
        self.max_restarts = 3

    def to_dict(self) -> dict:
        is_alive = self.thread.is_alive() if self.thread else False
        # Dead job detection: no heartbeat in 3x interval
        is_dead = False
        if self.last_run and self.interval_sec > 0:
            last = datetime.fromisoformat(self.last_run)
            if (datetime.now(timezone.utc) - last).total_seconds() > self.interval_sec * 3:
                is_dead = True
        # Also dead if thread existed but is no longer alive and had been running
        if not is_alive and self.status == "running" and self.started_at:
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
            "restart_count": self.restart_count,
            "max_restarts": self.max_restarts,
            "restartable": self.restart_func is not None or self.func is not None,
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
    """Check for dead jobs and restart them via restart_func or func reference.

    Tracks restart count per job. After max_restarts (default 3), stops trying
    and sends a critical alert instead of retrying forever.
    """
    restarted = []
    exhausted = []
    with _lock:
        for name, job in _jobs.items():
            has_restart = job.restart_func or job.func
            if not has_restart:
                continue
            is_alive = job.thread.is_alive() if job.thread else False
            if is_alive:
                # Thread alive and healthy — reset restart counter
                if job.restart_count > 0 and job.status not in ("dead", "restarted"):
                    job.restart_count = 0
                continue

            # Determine if job is dead (silent for 3x interval or thread died)
            is_dead = False
            elapsed = 0
            if job.last_run and job.interval_sec > 0:
                try:
                    last = datetime.fromisoformat(job.last_run)
                    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
                    if elapsed > job.interval_sec * 3:
                        is_dead = True
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
            # Thread was running but died
            if not is_alive and job.status in ("running", "restarted") and job.started_at:
                is_dead = True

            if not is_dead:
                continue

            # Check restart budget
            if job.restart_count >= job.max_restarts:
                exhausted.append(name)
                continue

            # Attempt restart
            try:
                if job.restart_func:
                    job.restart_func()
                elif job.func:
                    t = threading.Thread(target=job.func, daemon=True, name=f"restart-{name}")
                    t.start()
                    job.thread = t
                job.restart_count += 1
                job.started_at = datetime.now(timezone.utc).isoformat()
                job.status = "restarted"
                restarted.append(name)
                log.warning("RESTARTED dead job: %s (attempt %d/%d, silent for %.0fs)",
                            name, job.restart_count, job.max_restarts, elapsed)
                try:
                    from src.core.structured_log import log_event
                    log_event(log, "warning", "job_restart",
                              job=name, attempt=job.restart_count,
                              max_restarts=job.max_restarts, silent_secs=int(elapsed))
                except ImportError as _e:
                    log.debug("suppressed: %s", _e)
            except Exception as e:
                job.restart_count += 1
                log.error("Failed to restart job %s (attempt %d/%d): %s",
                          name, job.restart_count, job.max_restarts, e)

    # Alert for exhausted jobs (outside lock)
    for name in exhausted:
        log.critical("Job %s EXHAUSTED all %d restart attempts — requires manual intervention",
                     name, _jobs[name].max_restarts)
        try:
            from src.core.structured_log import log_event
            log_event(log, "critical", "job_exhausted",
                      job=name, max_restarts=_jobs[name].max_restarts)
        except ImportError as _e:
            log.debug("suppressed: %s", _e)

    return restarted, exhausted


def start_watchdog(check_interval: int = 300):
    """Start a watchdog thread that restarts dead jobs every check_interval seconds."""
    def _watchdog_loop():
        time.sleep(120)  # Wait 2 min after boot before first check
        while True:
            try:
                restarted, exhausted = restart_dead_jobs()
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
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)
                if exhausted:
                    try:
                        from src.agents.notify_agent import send_alert
                        send_alert(
                            event_type="job_exhausted",
                            title=f"CRITICAL: {len(exhausted)} job(s) failed all restarts",
                            body=f"Jobs exhausted: {', '.join(exhausted)}. Manual restart required.",
                            urgency="critical",
                            channels=["email", "bell"],
                            cooldown_key="exhausted:" + ",".join(exhausted),
                        )
                    except Exception as _e:
                        log.debug("suppressed: %s", _e)
            except Exception as e:
                log.error("Watchdog error: %s", e)
            time.sleep(check_interval)

    t = threading.Thread(target=_watchdog_loop, daemon=True, name="job-watchdog")
    t.start()
    log.info("Job watchdog started (check every %ds)", check_interval)


def register_restartable(name: str, interval_sec: int, module, guard_attr: str,
                         start_func: Callable, max_restarts: int = 3):
    """Register a job that can be auto-restarted by the watchdog.

    When the watchdog detects this job is dead, it resets the module's guard
    attribute (e.g. _scheduler_started = False) and calls start_func() again.

    Args:
        name: Job name (must match heartbeat calls)
        interval_sec: Expected heartbeat interval
        module: The agent module object (e.g. src.agents.follow_up_engine)
        guard_attr: Module-level boolean that prevents double-start (e.g. "_scheduler_started")
        start_func: The module's start function (e.g. start_follow_up_scheduler)
        max_restarts: Max restart attempts before giving up (default 3)
    """
    def _restart():
        setattr(module, guard_attr, False)
        start_func()

    with _lock:
        job = _jobs.get(name)
        if not job:
            job = JobInfo(name, interval_sec)
            _jobs[name] = job
        job.interval_sec = interval_sec
        job.restart_func = _restart
        job.max_restarts = max_restarts
        log.info("Job %s registered as restartable (guard=%s.%s, max_restarts=%d)",
                 name, module.__name__, guard_attr, max_restarts)
    return job


def get_scheduler_status() -> dict:
    """Summary status for health checks."""
    jobs = get_all_jobs()
    dead = [j for j in jobs if j["status"] == "dead"]
    return {
        "job_count": len(jobs),
        "dead_count": len(dead),
        "dead_jobs": [j["name"] for j in dead],
        "restartable_count": sum(1 for j in jobs if j.get("restartable")),
    }


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

        # WAL checkpoint before backup — keep WAL file size manageable
        try:
            wal_conn = sqlite3.connect(db_path, timeout=30)
            wal_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            wal_conn.close()
        except Exception as we:
            log.warning("WAL checkpoint before backup failed: %s", we)

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
    """Keep last N daily + M weekly backups, delete the rest. Supports .db and .db.gz."""
    files = sorted(
        [f for f in os.listdir(backup_dir)
         if f.startswith("reytech_") and (f.endswith(".db") or f.endswith(".db.gz"))],
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
            date_str = f.replace("reytech_", "").replace(".db.gz", "").replace(".db", "")[:8]
            file_date = datetime.strptime(date_str, "%Y%m%d")
            # Keep if it's a Sunday (weekly) and we haven't kept enough weeklies
            if file_date.weekday() == 6 and weekly_kept < keep_weekly:
                kept.add(f)
                weekly_kept += 1
        except ValueError as _e:
            log.debug("suppressed: %s", _e)

    # Delete files not in kept set
    deleted = 0
    for f in files:
        if f not in kept:
            try:
                os.remove(os.path.join(backup_dir, f))
                deleted += 1
            except OSError as _e:
                log.debug("suppressed: %s", _e)
    if deleted:
        log.info("Rotated %d old backups (kept %d daily + %d weekly)", deleted, keep_daily, weekly_kept)


def list_backups(data_dir: str = None) -> list:
    """List available backups with metadata. Supports both .db and .db.gz files."""
    data_dir = data_dir or _get_data_dir()
    backup_dir = os.path.join(data_dir, "backups")
    if not os.path.exists(backup_dir):
        return []

    backups = []
    for f in sorted(os.listdir(backup_dir), reverse=True):
        if f.startswith("reytech_") and (f.endswith(".db") or f.endswith(".db.gz")):
            path = os.path.join(backup_dir, f)
            stat = os.stat(path)
            backups.append({
                "filename": f,
                "size": stat.st_size,
                "size_human": _fmt_size(stat.st_size),
                "compressed": f.endswith(".gz"),
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

    def _cleanup_old_uploads(data_dir: str = None):
        """Remove uploaded files older than 30 days to prevent disk bloat."""
        data_dir = data_dir or _get_data_dir()
        upload_dir = os.path.join(data_dir, "uploads")
        if not os.path.isdir(upload_dir):
            return
        cutoff = time.time() - (30 * 86400)
        removed = 0
        for root, dirs, files in os.walk(upload_dir):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    if os.path.getmtime(fp) < cutoff:
                        os.remove(fp)
                        removed += 1
                except OSError as _e:
                    log.debug("suppressed: %s", _e)
        if removed:
            log.info("Cleaned up %d old uploads (>30 days)", removed)

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
            try:
                _cleanup_old_uploads()
            except Exception as e:
                log.warning("Upload cleanup failed: %s", e)
            time.sleep(interval_hours * 3600)

    t = threading.Thread(target=_backup_loop, daemon=True, name="db-backup")
    t.start()
    mark_started("db-backup", t)
    log.info("Backup scheduler started (every %dh)", interval_hours)
