"""Phase 0.0 hotfix: prove that `dashboard.py` re-exports the same Flask
app instance as `app.py`. Railway dashboard override forces
`gunicorn dashboard:app` even though railway.toml says `app:app`.

This test guards against the shim being deleted (or `app.app` being
renamed) until the Railway dashboard override is cleared.
"""

import os


def _ensure_env():
    """app.py:622 calls create_app() at import-time which validates
    SECRET_KEY. Provide a deterministic dev value just for this test
    so importing the shim doesn't require railway-secrets shape."""
    os.environ.setdefault("SECRET_KEY", "shim-test-key-deterministic-32chars")
    os.environ.setdefault("FLASK_ENV", "development")


def test_dashboard_shim_re_exports_app():
    _ensure_env()
    import app as app_module
    import dashboard as dashboard_module
    assert dashboard_module.app is app_module.app, (
        "dashboard.py must re-export the same `app` object that "
        "app.py exports — Railway dashboard's startCommand override "
        "boots via `gunicorn dashboard:app`."
    )


def test_dashboard_shim_app_is_flask():
    _ensure_env()
    from flask import Flask
    import dashboard
    assert isinstance(dashboard.app, Flask)
