"""Compat shim — re-exports `app` so `gunicorn dashboard:app` resolves
to the same Flask app object as `gunicorn app:app`.

Background (2026-04-25): the Railway dashboard service config drifted
to use `gunicorn dashboard:app --preload` as the startCommand, which
overrides the `gunicorn app:app` defined in `railway.toml` and `Procfile`.
Until the dashboard override is cleared, this shim keeps deploys healthy.

Either invocation now boots the same `create_app()` instance:
    gunicorn app:app          # uses railway.toml / Procfile
    gunicorn dashboard:app    # uses Railway dashboard override

Once the dashboard override is removed (Settings → Deploy → clear
Start Command), this file can be deleted — `app:app` resolves on its
own. See docs/PLAN_EXECUTION_LOG_2026_04_25.md "Deploy issue" section
for the full story.
"""

from app import app  # noqa: F401  re-export for gunicorn

__all__ = ["app"]
