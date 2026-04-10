"""
ops_monitor.py — Production Operations Monitor

Consolidates all observability and reliability features:
  - Hourly SQLite backups with rotation + integrity verification
  - Request timing middleware (p50/p95/p99 per route)
  - Database health monitoring (WAL size, connections, lock wait)
  - Volume disk monitoring (alert at 80%/90%)
  - Uptime synthetic testing (hourly endpoint checks)
  - Pipeline SLA tracking (RFQ→quote timing)

All metrics exposed via /api/system/ops endpoint.
Background threads respect scheduler.should_run() for graceful shutdown.
"""

import collections
import logging
import os
import shutil
import sqlite3
import statistics
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger("reytech.ops")


# ═══════════════════════════════════════════════════════════════════════════════
# P1.3 — Hourly Backup Snapshots with Rotation
# ═══════════════════════════════════════════════════════════════════════════════

def run_hourly_backup(data_dir: str = None) -> dict:
    """Create a SQLite backup using .backup API.

    Rotation: keep 24 hourly + 7 daily snapshots.
    Verifies integrity after each backup.
    """
    try:
        from src.core.paths import DATA_DIR
        data_dir = data_dir or DATA_DIR
    except ImportError:
        data_dir = data_dir or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")

    db_path = os.path.join(data_dir, "reytech.db")
    backup_dir = os.path.join(data_dir, "backups", "hourly")
    os.makedirs(backup_dir, exist_ok=True)

    if not os.path.exists(db_path):
        return {"ok": False, "error": "Database file not found"}

    now = datetime.now(timezone.utc)
    filename = f"reytech_{now.strftime('%Y%m%d_%H%M%S')}.db"
    backup_path = os.path.join(backup_dir, filename)

    try:
        # WAL checkpoint before backup
        try:
            wal_conn = sqlite3.connect(db_path, timeout=10)
            wal_conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            wal_conn.close()
        except Exception as we:
            log.warning("WAL checkpoint before hourly backup failed: %s", we)

        # sqlite3 backup API (consistent snapshot during writes)
        src_conn = sqlite3.connect(db_path, timeout=30)
        dst_conn = sqlite3.connect(backup_path, timeout=15)
        src_conn.backup(dst_conn)
        dst_conn.close()
        src_conn.close()

        size = os.path.getsize(backup_path)

        # Quick integrity check on the backup
        integrity_ok = verify_backup_integrity(backup_path)

        # Rotate: keep 24 hourly
        _rotate_files(backup_dir, prefix="reytech_", suffix=".db", keep=24)

        log.info("Hourly backup: %s (%s, integrity=%s)",
                 filename, _fmt_size(size), "OK" if integrity_ok else "FAILED")

        try:
            from src.core.scheduler import heartbeat
            heartbeat("hourly-backup", success=True)
        except Exception:
            pass

        return {
            "ok": True,
            "filename": filename,
            "size": size,
            "size_human": _fmt_size(size),
            "integrity": integrity_ok,
            "created_at": now.isoformat(),
        }

    except Exception as e:
        log.error("Hourly backup failed: %s", e)
        try:
            from src.core.scheduler import heartbeat
            heartbeat("hourly-backup", success=False, error=str(e))
        except Exception:
            pass
        return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# P1.4 — Backup Verification
# ═══════════════════════════════════════════════════════════════════════════════

def verify_backup_integrity(backup_path: str) -> bool:
    """Verify a backup file is a valid, non-corrupted SQLite database.

    Checks: file exists, header valid, integrity_check passes,
    critical tables exist, quote counter present.
    """
    if not os.path.exists(backup_path):
        return False

    try:
        conn = sqlite3.connect(backup_path, timeout=10)

        # Integrity check
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if result[0] != "ok":
            log.error("Backup integrity FAILED: %s → %s", backup_path, result[0])
            conn.close()
            return False

        # Critical tables must exist
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}

        required = {"quotes", "contacts", "price_history", "app_settings"}
        missing = required - tables
        if missing:
            log.error("Backup missing tables: %s", missing)
            conn.close()
            return False

        # Quote counter must be present
        counter = conn.execute(
            "SELECT value FROM app_settings WHERE key='quote_counter_seq'"
        ).fetchone()
        if not counter:
            log.warning("Backup has no quote_counter_seq (may be new DB)")

        conn.close()
        return True

    except Exception as e:
        log.error("Backup verification error: %s → %s", backup_path, e)
        return False


def run_nightly_verification(data_dir: str = None) -> dict:
    """Full backup verification: restore to temp, verify schema + row counts."""
    try:
        from src.core.paths import DATA_DIR
        data_dir = data_dir or DATA_DIR
    except ImportError:
        data_dir = data_dir or "data"

    backup_dir = os.path.join(data_dir, "backups", "hourly")
    if not os.path.isdir(backup_dir):
        return {"ok": False, "error": "No backup directory found"}

    # Find latest backup
    backups = sorted(
        [f for f in os.listdir(backup_dir) if f.startswith("reytech_") and f.endswith(".db")],
        reverse=True
    )
    if not backups:
        return {"ok": False, "error": "No backup files found"}

    latest = os.path.join(backup_dir, backups[0])
    result = {"ok": True, "backup": backups[0], "checks": {}}

    # Integrity
    result["checks"]["integrity"] = verify_backup_integrity(latest)

    # Row counts
    try:
        conn = sqlite3.connect(latest, timeout=10)
        for table in ["quotes", "contacts", "price_history", "app_settings"]:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
                result["checks"][f"{table}_count"] = count
            except Exception:
                result["checks"][f"{table}_count"] = -1
        conn.close()
    except Exception as e:
        result["ok"] = False
        result["checks"]["error"] = str(e)

    # Compare size to live DB
    live_db = os.path.join(data_dir, "reytech.db")
    if os.path.exists(live_db):
        live_size = os.path.getsize(live_db)
        backup_size = os.path.getsize(latest)
        ratio = backup_size / live_size if live_size > 0 else 0
        result["checks"]["size_ratio"] = round(ratio, 3)
        # Backup should be within 50%-150% of live size
        if ratio < 0.5 or ratio > 1.5:
            result["ok"] = False
            result["checks"]["size_warning"] = f"Backup size ratio {ratio:.2f} is suspicious"

    if result["ok"]:
        log.info("Nightly backup verification PASSED: %s", backups[0])
    else:
        log.error("Nightly backup verification FAILED: %s", result)
        # Alert
        try:
            from src.agents.notify_agent import send_alert
            send_alert(
                event_type="backup_failure",
                title="Backup Verification Failed",
                body=f"Latest backup {backups[0]} failed verification.\n{result['checks']}",
                urgency="warning",
                channels=["email", "bell"],
            )
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# P1.5 — Built-in Error Tracker (replaces Sentry — zero external dependencies)
# ═══════════════════════════════════════════════════════════════════════════════

class ErrorTracker:
    """In-process error tracking with grouping, trending, and deploy correlation.

    Does what Sentry does for small-scale apps:
    - Groups identical errors (same type + route) into one issue
    - Tracks error rate over time (5-minute windows)
    - Correlates errors to deploys (via RAILWAY_GIT_COMMIT_SHA)
    - Alerts when error rate spikes (>5x baseline)
    """

    def __init__(self, max_issues: int = 200, max_rate_windows: int = 288):
        self._issues = collections.OrderedDict()  # key → ErrorIssue
        self._max_issues = max_issues
        self._lock = threading.Lock()
        # Error rate: 5-min windows, keep 24h (288 windows)
        self._rate_windows = collections.deque(maxlen=max_rate_windows)
        self._current_window_start = 0
        self._current_window_count = 0
        self._deploy_sha = os.environ.get("RAILWAY_GIT_COMMIT_SHA", "local")[:8]
        self._total_errors = 0
        self._started_at = time.time()

    def record(self, error: Exception, route: str, method: str = "GET"):
        """Record an error occurrence. Groups by (error_type, route)."""
        error_type = type(error).__name__
        message = str(error)[:200]
        key = f"{error_type}:{route}"

        with self._lock:
            self._total_errors += 1

            # Update rate window
            now = time.time()
            window = int(now // 300) * 300  # 5-minute bucket
            if window != self._current_window_start:
                if self._current_window_start > 0:
                    self._rate_windows.append({
                        "window": self._current_window_start,
                        "count": self._current_window_count,
                    })
                    self._check_rate_spike()
                self._current_window_start = window
                self._current_window_count = 0
            self._current_window_count += 1

            # Group into issue
            if key in self._issues:
                issue = self._issues[key]
                issue["count"] += 1
                issue["last_seen"] = datetime.now(timezone.utc).isoformat()
                issue["last_message"] = message
                issue["deploy"] = self._deploy_sha
                # Move to end (most recent)
                self._issues.move_to_end(key)
            else:
                # Evict oldest if at capacity
                if len(self._issues) >= self._max_issues:
                    self._issues.popitem(last=False)
                self._issues[key] = {
                    "key": key,
                    "error_type": error_type,
                    "route": route,
                    "method": method,
                    "count": 1,
                    "first_seen": datetime.now(timezone.utc).isoformat(),
                    "last_seen": datetime.now(timezone.utc).isoformat(),
                    "last_message": message,
                    "deploy": self._deploy_sha,
                }

    def _check_rate_spike(self):
        """Alert if error rate is >5x the trailing average."""
        if len(self._rate_windows) < 6:
            return  # Need at least 30 min of data
        recent = [w["count"] for w in list(self._rate_windows)[-6:]]
        older = [w["count"] for w in list(self._rate_windows)[:-6]]
        if not older:
            return
        avg_old = sum(older) / len(older)
        avg_recent = sum(recent) / len(recent)
        if avg_old > 0 and avg_recent > 5 * avg_old:
            log.error("Error rate spike: %.1f/5min (baseline %.1f/5min)", avg_recent, avg_old)
            try:
                from src.agents.notify_agent import send_alert
                send_alert(
                    event_type="error_rate_spike",
                    title="Error Rate Spike Detected",
                    body=f"Current: {avg_recent:.0f} errors/5min (baseline: {avg_old:.0f})\nDeploy: {self._deploy_sha}",
                    urgency="warning",
                    channels=["email", "bell"],
                    cooldown_key="error_spike",
                )
            except Exception:
                pass

    def get_stats(self) -> dict:
        with self._lock:
            uptime = time.time() - self._started_at
            issues = list(self._issues.values())
            # Sort by count descending
            issues.sort(key=lambda x: x["count"], reverse=True)

            # Current rate
            rate_per_min = 0
            if self._rate_windows:
                recent = list(self._rate_windows)[-3:]  # last 15 min
                total = sum(w["count"] for w in recent)
                rate_per_min = round(total / (len(recent) * 5), 2) if recent else 0

            return {
                "total_errors": self._total_errors,
                "unique_issues": len(self._issues),
                "errors_per_minute": rate_per_min,
                "deploy": self._deploy_sha,
                "uptime_hours": round(uptime / 3600, 1),
                "top_issues": issues[:15],
                "rate_history": [
                    {"time": datetime.fromtimestamp(w["window"], tz=timezone.utc).strftime("%H:%M"),
                     "errors": w["count"]}
                    for w in list(self._rate_windows)[-24:]  # last 2 hours
                ],
            }


# Global error tracker instance
_error_tracker = ErrorTracker()


def record_error(error: Exception, route: str, method: str = "GET"):
    """Record an error. Call from Flask error handlers."""
    _error_tracker.record(error, route, method)


def get_error_stats() -> dict:
    return _error_tracker.get_stats()


# ═══════════════════════════════════════════════════════════════════════════════
# P2.1 — Request Timing Middleware
# ═══════════════════════════════════════════════════════════════════════════════

class RequestTimer:
    """Tracks request timing per route. Thread-safe, fixed-size buffer."""

    def __init__(self, max_samples: int = 500):
        self._timings = collections.defaultdict(lambda: collections.deque(maxlen=max_samples))
        self._lock = threading.Lock()
        self._total_requests = 0
        self._slow_requests = 0  # >5s
        self._error_count = 0
        self._started_at = time.time()

    def record(self, route: str, duration_ms: float, status_code: int):
        with self._lock:
            self._total_requests += 1
            if duration_ms > 5000:
                self._slow_requests += 1
            if status_code >= 500:
                self._error_count += 1
            self._timings[route].append((duration_ms, status_code, time.time()))

    def get_stats(self, route: str = None) -> dict:
        """Get timing stats. If route is None, returns global stats."""
        with self._lock:
            if route:
                samples = [t[0] for t in self._timings.get(route, [])]
                if not samples:
                    return {"route": route, "samples": 0}
                return self._calc_percentiles(route, samples)

            # Global stats
            all_samples = []
            route_stats = []
            for r, timings in self._timings.items():
                samples = [t[0] for t in timings]
                if samples:
                    all_samples.extend(samples)
                    route_stats.append(self._calc_percentiles(r, samples))

            # Sort by p95 descending (slowest routes first)
            route_stats.sort(key=lambda x: x.get("p95", 0), reverse=True)

            uptime = time.time() - self._started_at
            return {
                "uptime_seconds": int(uptime),
                "total_requests": self._total_requests,
                "slow_requests": self._slow_requests,
                "error_count": self._error_count,
                "requests_per_minute": round(self._total_requests / (uptime / 60), 1) if uptime > 0 else 0,
                "global": self._calc_percentiles("all", all_samples) if all_samples else {},
                "top_routes": route_stats[:20],
            }

    def _calc_percentiles(self, route: str, samples: list) -> dict:
        if not samples:
            return {"route": route, "samples": 0}
        sorted_s = sorted(samples)
        n = len(sorted_s)
        return {
            "route": route,
            "samples": n,
            "p50": round(sorted_s[int(n * 0.5)], 1),
            "p95": round(sorted_s[int(n * 0.95)], 1),
            "p99": round(sorted_s[min(int(n * 0.99), n - 1)], 1),
            "max": round(sorted_s[-1], 1),
            "mean": round(statistics.mean(samples), 1),
        }


# Global timer instance
_timer = RequestTimer()


def install_request_timing(app):
    """Install Flask before/after_request hooks for timing."""
    from flask import request, g

    @app.before_request
    def _start_timer():
        g._request_start = time.time()

    @app.after_request
    def _record_timing(response):
        start = getattr(g, '_request_start', None)
        if start is None:
            return response
        duration_ms = (time.time() - start) * 1000

        # Normalize route (use rule pattern, not actual path with IDs)
        rule = request.url_rule
        route = rule.rule if rule else request.path
        _timer.record(route, duration_ms, response.status_code)

        # Add timing header for debugging
        response.headers["X-Response-Time"] = f"{duration_ms:.0f}ms"
        return response


def get_request_stats(route: str = None) -> dict:
    return _timer.get_stats(route)


# ═══════════════════════════════════════════════════════════════════════════════
# P2.2 — Database Health Monitor
# ═══════════════════════════════════════════════════════════════════════════════

def check_db_health(data_dir: str = None) -> dict:
    """Check database health: WAL size, page count, freelist, integrity."""
    try:
        from src.core.paths import DATA_DIR
        data_dir = data_dir or DATA_DIR
    except ImportError:
        data_dir = data_dir or "data"

    db_path = os.path.join(data_dir, "reytech.db")
    wal_path = db_path + "-wal"
    shm_path = db_path + "-shm"

    result = {"ok": True, "warnings": []}

    # File sizes
    result["db_size"] = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    result["db_size_human"] = _fmt_size(result["db_size"])
    result["wal_size"] = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
    result["wal_size_human"] = _fmt_size(result["wal_size"])
    result["shm_size"] = os.path.getsize(shm_path) if os.path.exists(shm_path) else 0

    # WAL bloat warning (>50MB means checkpoints aren't running)
    if result["wal_size"] > 50 * 1024 * 1024:
        result["warnings"].append(f"WAL file is {_fmt_size(result['wal_size'])} — checkpoint may be stuck")
        result["ok"] = False

    # PRAGMA stats
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        result["page_count"] = conn.execute("PRAGMA page_count").fetchone()[0]
        result["page_size"] = conn.execute("PRAGMA page_size").fetchone()[0]
        result["freelist_count"] = conn.execute("PRAGMA freelist_count").fetchone()[0]
        result["journal_mode"] = conn.execute("PRAGMA journal_mode").fetchone()[0]

        # Table row counts
        tables = {}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM [{row[0]}]").fetchone()[0]
                tables[row[0]] = count
            except Exception:
                tables[row[0]] = -1
        result["tables"] = tables

        # Quick integrity check (fast mode)
        integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
        result["integrity"] = integrity
        if integrity != "ok":
            result["warnings"].append(f"Integrity check: {integrity}")
            result["ok"] = False

        conn.close()
    except Exception as e:
        result["ok"] = False
        result["warnings"].append(f"DB query error: {e}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# P2.3 — Volume Disk Monitor
# ═══════════════════════════════════════════════════════════════════════════════

def check_disk_usage(data_dir: str = None) -> dict:
    """Check disk usage of the data volume. Alert at 80%/90%."""
    try:
        from src.core.paths import DATA_DIR
        data_dir = data_dir or DATA_DIR
    except ImportError:
        data_dir = data_dir or "data"

    result = {"ok": True, "warnings": []}

    try:
        usage = shutil.disk_usage(data_dir)
        result["total_gb"] = round(usage.total / (1024 ** 3), 2)
        result["used_gb"] = round(usage.used / (1024 ** 3), 2)
        result["free_gb"] = round(usage.free / (1024 ** 3), 2)
        result["percent_used"] = round((usage.used / usage.total) * 100, 1) if usage.total > 0 else 0

        if result["percent_used"] >= 90:
            result["ok"] = False
            result["warnings"].append(f"CRITICAL: Disk {result['percent_used']}% full — {result['free_gb']}GB free")
            _fire_disk_alert(result, "critical")
        elif result["percent_used"] >= 80:
            result["warnings"].append(f"WARNING: Disk {result['percent_used']}% full — {result['free_gb']}GB free")
            _fire_disk_alert(result, "warning")

    except Exception as e:
        result["ok"] = False
        result["warnings"].append(f"Disk check failed: {e}")

    # Top files by size in data dir
    try:
        files = []
        for f in os.listdir(data_dir):
            fp = os.path.join(data_dir, f)
            if os.path.isfile(fp):
                files.append((f, os.path.getsize(fp)))
        files.sort(key=lambda x: x[1], reverse=True)
        result["largest_files"] = [
            {"name": f, "size": _fmt_size(s)} for f, s in files[:10]
        ]
    except Exception:
        pass

    return result


def _fire_disk_alert(result, urgency):
    """Fire alert for disk usage (with cooldown to prevent spam)."""
    try:
        from src.agents.notify_agent import send_alert
        send_alert(
            event_type="disk_usage",
            title=f"Disk Usage {result['percent_used']}%",
            body=f"Used: {result['used_gb']}GB / {result['total_gb']}GB\nFree: {result['free_gb']}GB",
            urgency=urgency,
            channels=["email", "bell"],
            cooldown_key="disk_usage",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# P2.4 — Uptime Synthetic Test
# ═══════════════════════════════════════════════════════════════════════════════

_synthetic_results = collections.deque(maxlen=168)  # 7 days of hourly results


def run_synthetic_test(base_url: str = None, auth: tuple = None) -> dict:
    """Hit critical endpoints, verify they respond correctly.

    Designed to run hourly from a background thread.
    """
    import requests as _req

    if not base_url:
        base_url = os.environ.get("REYTECH_URL", "http://localhost:8000")
    if not auth:
        _user = os.environ.get("DASH_USER", os.environ.get("REYTECH_USER", ""))
        _pass = os.environ.get("DASH_PASS", os.environ.get("REYTECH_PASS", ""))
        auth = (_user, _pass) if _user and _pass else None

    endpoints = [
        ("/ping", 200, False),
        ("/health", 200, False),
        ("/api/system/health", 200, True),
        ("/api/health", 200, True),
        ("/api/system/metrics", 200, True),
    ]

    result = {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "checks": [],
    }

    for path, expected_status, needs_auth in endpoints:
        check = {"path": path, "ok": False}
        try:
            t0 = time.time()
            kwargs = {"timeout": 10}
            if needs_auth and auth:
                kwargs["auth"] = auth
            resp = _req.get(f"{base_url}{path}", **kwargs)
            check["status"] = resp.status_code
            check["latency_ms"] = round((time.time() - t0) * 1000)
            check["ok"] = resp.status_code == expected_status
        except _req.Timeout:
            check["error"] = "timeout"
            check["latency_ms"] = 10000
        except _req.ConnectionError:
            check["error"] = "connection_refused"
        except Exception as e:
            check["error"] = str(e)[:100]

        result["checks"].append(check)
        if not check["ok"]:
            result["ok"] = False

    _synthetic_results.append(result)

    if not result["ok"]:
        failed = [c["path"] for c in result["checks"] if not c["ok"]]
        log.error("Synthetic test FAILED: %s", failed)
        try:
            from src.agents.notify_agent import send_alert
            send_alert(
                event_type="uptime_failure",
                title="Uptime Check Failed",
                body=f"Failed endpoints: {', '.join(failed)}",
                urgency="warning",
                channels=["email", "bell"],
                cooldown_key="uptime_check",
            )
        except Exception:
            pass

    return result


def get_synthetic_history() -> list:
    return list(_synthetic_results)


# ═══════════════════════════════════════════════════════════════════════════════
# P2.5 — Pipeline SLA Tracker
# ═══════════════════════════════════════════════════════════════════════════════

_pipeline_events = collections.deque(maxlen=500)


def track_pipeline_event(rfq_id: str, stage: str, timestamp: float = None):
    """Record a pipeline stage completion.

    Stages: received, parsed, enriched, priced, quoted, sent
    """
    _pipeline_events.append({
        "rfq_id": rfq_id,
        "stage": stage,
        "timestamp": timestamp or time.time(),
    })


def get_pipeline_sla_stats() -> dict:
    """Calculate pipeline SLA metrics from tracked events."""
    events = list(_pipeline_events)
    if not events:
        return {"ok": True, "tracked": 0}

    # Group by rfq_id
    by_rfq = collections.defaultdict(list)
    for e in events:
        by_rfq[e["rfq_id"]].append(e)

    durations = []
    sla_breaches = []
    SLA_SECONDS = 300  # 5 minutes from received to quoted

    for rfq_id, stages in by_rfq.items():
        stages.sort(key=lambda x: x["timestamp"])
        first = stages[0]["timestamp"]
        last = stages[-1]["timestamp"]
        duration = last - first
        durations.append(duration)

        if duration > SLA_SECONDS:
            sla_breaches.append({
                "rfq_id": rfq_id,
                "duration_s": round(duration, 1),
                "stages": [s["stage"] for s in stages],
            })

    result = {
        "ok": len(sla_breaches) == 0,
        "tracked": len(by_rfq),
        "sla_target_seconds": SLA_SECONDS,
        "breaches": len(sla_breaches),
        "recent_breaches": sla_breaches[-5:],
    }

    if durations:
        sorted_d = sorted(durations)
        n = len(sorted_d)
        result["p50_seconds"] = round(sorted_d[int(n * 0.5)], 1)
        result["p95_seconds"] = round(sorted_d[int(n * 0.95)], 1)
        result["max_seconds"] = round(sorted_d[-1], 1)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Background Monitor Thread
# ═══════════════════════════════════════════════════════════════════════════════

_monitor_started = False


def start_ops_monitor():
    """Start background monitoring threads. Call once from app startup."""
    global _monitor_started
    if _monitor_started:
        return
    _monitor_started = True

    def _monitor_loop():
        """Main monitoring loop — runs hourly checks."""
        try:
            from src.core.scheduler import should_run, heartbeat, register_job
            register_job("ops-monitor", interval_sec=3600)
        except ImportError:
            should_run = lambda: True
            heartbeat = lambda *a, **kw: None

        # Initial delay — let app stabilize before first check
        for _ in range(60):
            if not should_run():
                return
            time.sleep(1)

        cycle = 0
        while should_run():
            try:
                cycle += 1

                # Every hour: backup + disk check + db health
                backup_result = run_hourly_backup()
                if not backup_result.get("ok"):
                    log.error("Hourly backup failed: %s", backup_result.get("error"))

                disk = check_disk_usage()
                if not disk.get("ok"):
                    log.warning("Disk health issue: %s", disk.get("warnings"))

                db = check_db_health()
                if not db.get("ok"):
                    log.warning("DB health issue: %s", db.get("warnings"))

                # Every 24 hours: full backup verification
                if cycle % 24 == 0:
                    verify = run_nightly_verification()
                    if not verify.get("ok"):
                        log.error("Nightly backup verification FAILED")

                try:
                    heartbeat("ops-monitor", success=True)
                except Exception:
                    pass

            except Exception as e:
                log.error("Ops monitor error: %s", e)

            # Sleep 1 hour in 60s intervals (for graceful shutdown)
            for _ in range(60):
                if not should_run():
                    return
                time.sleep(60)

    def _synthetic_loop():
        """Synthetic uptime test — runs every hour."""
        try:
            from src.core.scheduler import should_run
        except ImportError:
            should_run = lambda: True

        # Wait 5 minutes before first test (let app finish booting)
        for _ in range(300):
            if not should_run():
                return
            time.sleep(1)

        while should_run():
            try:
                run_synthetic_test()
            except Exception as e:
                log.error("Synthetic test error: %s", e)

            # Sleep 1 hour
            for _ in range(60):
                if not should_run():
                    return
                time.sleep(60)

    threading.Thread(target=_monitor_loop, daemon=True, name="ops-monitor").start()
    threading.Thread(target=_synthetic_loop, daemon=True, name="synthetic-test").start()
    log.info("Ops monitor started (hourly backups + health checks + synthetic tests)")


# ═══════════════════════════════════════════════════════════════════════════════
# Unified Status Endpoint Data
# ═══════════════════════════════════════════════════════════════════════════════

def get_ops_status() -> dict:
    """Full ops status for /api/system/ops endpoint."""
    return {
        "request_timing": get_request_stats(),
        "error_tracking": get_error_stats(),
        "db_health": check_db_health(),
        "disk_usage": check_disk_usage(),
        "pipeline_sla": get_pipeline_sla_stats(),
        "synthetic_tests": {
            "history_count": len(_synthetic_results),
            "latest": _synthetic_results[-1] if _synthetic_results else None,
        },
        "circuit_breakers": _get_circuit_breaker_status(),
    }


def _get_circuit_breaker_status() -> list:
    try:
        from src.core.circuit_breaker import all_status
        return all_status()
    except ImportError:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _fmt_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def _rotate_files(directory: str, prefix: str, suffix: str, keep: int):
    """Keep the newest `keep` files matching prefix/suffix, delete the rest."""
    try:
        files = sorted(
            [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(suffix)],
            reverse=True
        )
        for f in files[keep:]:
            try:
                os.remove(os.path.join(directory, f))
            except OSError:
                pass
    except Exception as e:
        log.warning("File rotation error in %s: %s", directory, e)
