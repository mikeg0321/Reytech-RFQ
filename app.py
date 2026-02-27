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
from flask import Flask

def create_app():
    """Application factory."""
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

    # ── Persistent database init ──────────────────────────────────────────────
    # ── Product catalog init ──────────────────────────────────────────────────
    try:
        from src.core.catalog import init_catalog
        init_catalog()
    except Exception as e:
        logging.getLogger("reytech").warning("Catalog init skipped: %s", e)

    try:
        from src.core.db import startup as db_startup
        result = db_startup()
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

    # Register the dashboard blueprint (all routes)
    from src.api.dashboard import bp, start_polling
    app.register_blueprint(bp)

    # ── Security middleware (rate limiting, CSRF, headers) ──────────
    try:
        from src.core.security import init_security
        init_security(app)
    except Exception as e:
        logging.getLogger("reytech").warning("Security init skipped: %s", e)

    # ── Runtime self-test — catches path/route/data bugs at boot ──────────
    try:
        from src.core.startup_checks import run_startup_checks
        with app.app_context():
            checks = run_startup_checks(app)
            if checks["failed"] > 0:
                logging.getLogger("reytech").error(
                    "STARTUP: %d checks FAILED — review logs", checks["failed"])
    except Exception as e:
        logging.getLogger("reytech").warning("Startup checks skipped: %s", e)

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

    return app


# For gunicorn: gunicorn app:app
app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
