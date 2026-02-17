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

    # Register the dashboard blueprint (all routes)
    from src.api.dashboard import bp, start_polling
    app.register_blueprint(bp)

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
