#!/usr/bin/env python3
"""
Reytech RFQ — Application Entry Point
Creates Flask app and registers the dashboard Blueprint.
"""

import os
import logging
from flask import Flask

def create_app():
    """Application factory."""
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "reytech-rfq-2026")

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

    return app


# For gunicorn: gunicorn app:app
app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
