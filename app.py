#!/usr/bin/env python3
"""
Reytech RFQ — Application Entry Point
Creates Flask app and registers the dashboard Blueprint.
"""

# ── Clear stale bytecode BEFORE any imports ──
# Railway's persistent volume caches .pyc across deploys, causing old code to run.
# ONLY clean src/ — don't walk the 4GB data volume!
import sys, pathlib
sys.dont_write_bytecode = True
_src_dir = pathlib.Path(__file__).parent / "src"
if _src_dir.exists():
    for _pyc in _src_dir.rglob("*.pyc"):
        try:
            _pyc.unlink()
        except OSError:
            pass
    for _cache in _src_dir.rglob("__pycache__"):
        try:
            _cache.rmdir()
        except OSError:
            pass

import os
import logging
import time
from flask import Flask, request

print(f"[BOOT] app.py loading at {time.time():.0f}", flush=True)

def create_app():
    """Application factory — optimized for fast startup."""
    t0 = time.time()
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

    # ── FORCE_CLEAN_BOOT: nuke corrupted volume data ──────────────────────
    if os.environ.get("FORCE_CLEAN_BOOT"):
        print("[BOOT] FORCE_CLEAN_BOOT: clearing corrupted files...", flush=True)
        import glob
        data_dir = os.path.join(_app_dir, "data")
        for pattern in ["*.db-journal", "*.db-wal", "*.db-shm"]:
            for f in glob.glob(os.path.join(data_dir, pattern)):
                try:
                    os.remove(f)
                except Exception:
                    pass
        db_file = os.path.join(data_dir, "reytech.db")
        if os.path.exists(db_file):
            db_size = os.path.getsize(db_file) / 1024 / 1024
            os.remove(db_file)
            print(f"[BOOT] Removed reytech.db ({db_size:.0f} MB)", flush=True)
        for f in ["processed_emails.json"]:
            p = os.path.join(data_dir, f)
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    # ── CRITICAL PATH: DB schema + data sync + blueprint ──
    print("[BOOT] DB schema init...", flush=True)
    try:
        if os.environ.get("FORCE_CLEAN_BOOT"):
            from src.core.db import get_db, SCHEMA, DB_PATH, _is_railway_volume
            with get_db() as conn:
                conn.executescript(SCHEMA)
        else:
            import signal
            def _timeout_handler(signum, frame):
                raise TimeoutError("DB init >30s")
            old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(30)
            try:
                from src.core.db import (get_db, SCHEMA, DB_PATH, init_db, _is_railway_volume,
                                         _reconcile_quotes_json, _boot_sync_quotes, _boot_sync_pcs,
                                         _fix_data_on_boot, get_db_stats, migrate_json_to_db,
                                         init_db_deferred)
                init_db()
                # Data sync — must complete before serving requests
                init_db_deferred()  # DAL migration
                _reconcile_quotes_json()
                _fix_data_on_boot()
                stats = get_db_stats()
                if stats.get("quotes", 0) == 0 and stats.get("contacts", 0) == 0:
                    migrate_json_to_db()
                else:
                    _boot_sync_quotes()
                    _boot_sync_pcs()
            except TimeoutError:
                print("[BOOT] DB TIMEOUT — minimal schema", flush=True)
                from src.core.db import get_db, SCHEMA, DB_PATH, _is_railway_volume
                with get_db() as conn:
                    conn.executescript(SCHEMA)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
    except Exception as e:
        logging.getLogger("reytech").warning("DB init: %s", e)
    print(f"[BOOT] DB ready ({time.time()-t0:.1f}s)", flush=True)

    # Register blueprint (all routes)
    from src.api.dashboard import bp, start_polling
    app.register_blueprint(bp)
    print(f"[BOOT] Routes registered ({time.time()-t0:.1f}s)", flush=True)

    # ── Security middleware ──
    try:
        from src.core.security import init_security
        init_security(app)
    except Exception as e:
        logging.getLogger("reytech").warning("Security init: %s", e)

    # ── Response compression + caching ──
    @app.after_request
    def _optimize_response(response):
        # Cache static assets for 1 hour
        if request.path.startswith("/static/"):
            response.cache_control.max_age = 3600
            response.cache_control.public = True
        # Gzip HTML/JSON responses
        if (response.content_type and 
            any(ct in response.content_type for ct in ("text/html", "application/json")) and
            response.content_length and response.content_length > 500):
            try:
                import gzip as _gz
                accept = request.headers.get("Accept-Encoding", "")
                if "gzip" in accept and response.status_code == 200:
                    data = response.get_data()
                    compressed = _gz.compress(data, compresslevel=4)
                    if len(compressed) < len(data):
                        response.set_data(compressed)
                        response.headers["Content-Encoding"] = "gzip"
                        response.headers["Content-Length"] = len(compressed)
            except Exception:
                pass
        return response

    # ── DEFERRED: only non-critical tasks in background ──────────────────
    def _deferred_init():
        """Non-critical startup tasks — app already serves requests."""
        time.sleep(2)
        try:
            from src.core.catalog import init_catalog
            init_catalog()
        except Exception:
            pass
        try:
            from src.core.db import _dedup_price_checks_on_boot
            _dedup_price_checks_on_boot()
        except Exception:
            pass
        try:
            from src.core.migrations import run_migrations
            run_migrations()
        except Exception:
            pass
        try:
            from src.core.structured_log import setup_structured_logging
            setup_structured_logging()
        except Exception:
            pass
        try:
            from src.core.scheduler import start_backup_scheduler, register_job
            start_backup_scheduler(interval_hours=24)
            for job_name, interval in [
                ("email-poller", 300), ("award-monitor", 3600),
                ("follow-up-engine", 3600), ("quote-lifecycle", 3600),
                ("email-retry", 900), ("lead-nurture", 86400),
                ("qa-monitor", 900), ("growth-agent", 86400),
            ]:
                register_job(job_name, interval_sec=interval)
        except Exception:
            pass
        logging.getLogger("reytech").info("Deferred init complete")

    import threading
    threading.Thread(target=_deferred_init, daemon=True, name="deferred-init").start()

    # Start email polling (production only)
    if os.environ.get("ENABLE_EMAIL_POLLING", "").lower() == "true":
        with app.app_context():
            start_polling(app)

    elapsed = time.time() - t0
    print(f"[BOOT] create_app() complete ✅ ({elapsed:.1f}s)", flush=True)
    return app


# For gunicorn: gunicorn app:app
print("[BOOT] Creating app at module level...", flush=True)
app = create_app()
print("[BOOT] Module ready ✅", flush=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
