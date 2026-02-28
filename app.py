#!/usr/bin/env python3
"""
Reytech RFQ — Application Entry Point
Creates Flask app and registers the dashboard Blueprint.
"""

# ── Clear stale bytecode BEFORE any imports ──
# Railway's persistent volume caches .pyc across deploys, causing old code to run.
import sys, pathlib
sys.dont_write_bytecode = True
for _pyc in pathlib.Path(__file__).parent.rglob("*.pyc"):
    try:
        _pyc.unlink()
    except OSError:
        pass
for _cache in pathlib.Path(__file__).parent.rglob("__pycache__"):
    try:
        _cache.rmdir()
    except OSError:
        pass

import os
import logging
import time
from flask import Flask

print(f"[BOOT] app.py loading at {time.time():.0f}", flush=True)

def create_app():
    """Application factory."""
    print("[BOOT] create_app() called", flush=True)
    _app_dir = os.path.dirname(os.path.abspath(__file__))
    app = Flask(
        __name__,
        template_folder=os.path.join(_app_dir, "src", "templates"),
        static_folder=os.path.join(_app_dir, "src", "static"),
        static_url_path="/static",
    )
    _secret = os.environ.get("SECRET_KEY")
    if not _secret:
        raise RuntimeError(
            "SECRET_KEY environment variable is required. "
            "Set it in Railway: Settings → Variables → SECRET_KEY = <random 32+ char string>"
        )
    app.secret_key = _secret
    print("[BOOT] Flask app created, SECRET_KEY set", flush=True)

    # ── FORCE_CLEAN_BOOT: nuke corrupted volume data ──────────────────────
    if os.environ.get("FORCE_CLEAN_BOOT"):
        print("[BOOT] FORCE_CLEAN_BOOT: clearing corrupted files...", flush=True)
        import glob
        data_dir = os.path.join(_app_dir, "data")
        # Delete SQLite lock/journal/WAL files that cause hangs
        for pattern in ["*.db-journal", "*.db-wal", "*.db-shm"]:
            for f in glob.glob(os.path.join(data_dir, pattern)):
                try:
                    os.remove(f)
                    print(f"[BOOT] Removed lock: {f}", flush=True)
                except Exception:
                    pass
        # Delete main DB — it will rebuild from JSON on init
        db_file = os.path.join(data_dir, "reytech.db")
        if os.path.exists(db_file):
            db_size = os.path.getsize(db_file) / 1024 / 1024
            os.remove(db_file)
            print(f"[BOOT] Removed reytech.db ({db_size:.0f} MB) — will rebuild", flush=True)
        # Clear processed emails to unstick poller
        for f in ["processed_emails.json"]:
            p = os.path.join(data_dir, f)
            if os.path.exists(p):
                try:
                    os.remove(p)
                    print(f"[BOOT] Removed: {p}", flush=True)
                except Exception:
                    pass
        # Clear huge upload directories that bloat volume
        uploads_dir = os.path.join(_app_dir, "uploads")
        if os.path.isdir(uploads_dir):
            total_size = sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, _, fns in os.walk(uploads_dir)
                for f in fns
            )
            print(f"[BOOT] uploads/ size: {total_size / 1024 / 1024:.0f} MB", flush=True)
            if total_size > 500 * 1024 * 1024:  # > 500MB
                import shutil
                shutil.rmtree(uploads_dir, ignore_errors=True)
                os.makedirs(uploads_dir, exist_ok=True)
                print("[BOOT] Cleared oversized uploads/", flush=True)

    # ── Persistent database init ──────────────────────────────────────────────
    # ── Product catalog init ──────────────────────────────────────────────────
    print("[BOOT] Initializing catalog...", flush=True)
    try:
        from src.core.catalog import init_catalog
        init_catalog()
    except Exception as e:
        logging.getLogger("reytech").warning("Catalog init skipped: %s", e)
    print("[BOOT] Catalog done", flush=True)

    print("[BOOT] Initializing DB...", flush=True)
    try:
        if os.environ.get("FORCE_CLEAN_BOOT"):
            # Minimal DB init — just create tables, skip all migration/sync
            print("[BOOT] FORCE_CLEAN_BOOT: minimal DB init (schema only)", flush=True)
            from src.core.db import get_db, SCHEMA, DB_PATH, _is_railway_volume
            with get_db() as conn:
                conn.executescript(SCHEMA)
            print("[BOOT] Schema created", flush=True)
            result = {"ok": True, "db_path": DB_PATH, "stats": {"quotes": 0, "contacts": 0, "price_history": 0}, "is_volume": _is_railway_volume()}
        else:
            # Run startup with a hard timeout to prevent infinite hang
            import signal
            def _timeout_handler(signum, frame):
                raise TimeoutError("DB startup took >45s — skipping heavy init")
            old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(45)
            try:
                from src.core.db import startup as db_startup
                print("[BOOT] Calling db_startup()...", flush=True)
                result = db_startup()
            except TimeoutError as te:
                print(f"[BOOT] DB TIMEOUT: {te}", flush=True)
                logging.getLogger("reytech").warning("DB startup timeout — using minimal init")
                # Fall back to minimal init
                from src.core.db import get_db, SCHEMA, DB_PATH, _is_railway_volume
                with get_db() as conn:
                    conn.executescript(SCHEMA)
                result = {"ok": True, "db_path": DB_PATH, "stats": {"quotes": 0, "contacts": 0, "price_history": 0}, "is_volume": _is_railway_volume()}
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        logging.getLogger("reytech").info(
            "DB: %s | volume=%s | quotes=%d contacts=%d prices=%d",
            result["db_path"],
            result.get("is_volume", False),
            result["stats"].get("quotes", 0),
            result["stats"].get("contacts", 0),
            result["stats"].get("price_history", 0),
        )
    except Exception as e:
        logging.getLogger("reytech").warning("DB init skipped: %s", e)
        print(f"[BOOT] DB init error: {e}", flush=True)
    print("[BOOT] DB done", flush=True)

    # ── Schema migrations (Sprint 5.2) ──────────────────────────────────────
    print("[BOOT] Running migrations...", flush=True)
    try:
        from src.core.migrations import run_migrations
        mig = run_migrations()
        if mig.get("applied", 0) > 0:
            logging.getLogger("reytech").info(
                "Migrations: applied %d, now at v%d",
                mig["applied"], mig["version"])
    except Exception as e:
        logging.getLogger("reytech").warning("Migrations skipped: %s", e)

    # ── Structured logging (Sprint 5.3) ─────────────────────────────────────
    try:
        from src.core.structured_log import setup_structured_logging
        setup_structured_logging()
    except Exception as e:
        logging.getLogger("reytech").debug("Structured logging skipped: %s", e)

    # Register the dashboard blueprint (all routes)
    print("[BOOT] Importing dashboard...", flush=True)
    from src.api.dashboard import bp, start_polling
    print("[BOOT] Registering blueprint...", flush=True)
    app.register_blueprint(bp)
    print("[BOOT] Blueprint registered", flush=True)

    # ── Security middleware (rate limiting, CSRF, headers) ──────────
    print("[BOOT] Security init...", flush=True)
    try:
        from src.core.security import init_security
        init_security(app)
    except Exception as e:
        logging.getLogger("reytech").warning("Security init skipped: %s", e)

    # ── Runtime self-test — catches path/route/data bugs at boot ──────────
    print("[BOOT] Startup checks...", flush=True)
    try:
        from src.core.startup_checks import run_startup_checks
        with app.app_context():
            checks = run_startup_checks(app)
            if checks["failed"] > 0:
                logging.getLogger("reytech").error(
                    "STARTUP: %d checks FAILED — review logs", checks["failed"])
    except Exception as e:
        logging.getLogger("reytech").warning("Startup checks skipped: %s", e)

    print("[BOOT] Scheduler init...", flush=True)

    # Start email polling in background (production only)
    if os.environ.get("ENABLE_EMAIL_POLLING", "").lower() == "true":
        with app.app_context():
            start_polling(app)

    # ── Scheduler: backup + job health monitoring (F4 + F5) ──
    try:
        from src.core.scheduler import start_backup_scheduler, register_job
        start_backup_scheduler(interval_hours=24)
        # Register known background jobs for health tracking
        for job_name, interval in [
            ("email-poller", 300), ("award-monitor", 3600),
            ("follow-up-engine", 3600), ("quote-lifecycle", 3600),
            ("email-retry", 900), ("lead-nurture", 86400),
            ("qa-monitor", 900), ("growth-agent", 86400),
        ]:
            register_job(job_name, interval_sec=interval)
        logging.getLogger("reytech").info("Scheduler initialized: backup + 8 job monitors")
    except Exception as e:
        logging.getLogger("reytech").warning("Scheduler init skipped: %s", e)

    print("[BOOT] create_app() complete ✅", flush=True)
    return app


# For gunicorn: gunicorn app:app
print("[BOOT] Creating app at module level...", flush=True)
app = create_app()
print("[BOOT] Module ready ✅", flush=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
