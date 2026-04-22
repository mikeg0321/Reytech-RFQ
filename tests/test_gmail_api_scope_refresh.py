"""Regression: Gmail API credential refresh must not force scopes.

Background — 2026-04-22 prod incident. The stored refresh token in
Railway env was issued with only `gmail.readonly` scope. After the
SCOPES constant was expanded to include `gmail.send` and
`drive.readonly`, every token refresh returned
`invalid_scope: Bad Request` from Google. Cascade:

  1. `Credentials.refresh()` raises on the broadened scope list.
  2. `EmailPoller.connect()` returns False, circuit breaker opens.
  3. `POLL_STATUS["error"]` fires, smoke test flags poller error.
  4. Auto-rollback triggers on a healthy deploy.

The fix: `_build_credentials()` omits the `scopes` argument so the
refresh carries whatever scopes were originally granted. Send/Drive
calls degrade gracefully (403 from Google) until the user re-runs
`scripts/gmail_oauth_setup.py` to widen the grant.

This guard scans the source to make sure nobody re-adds `scopes=SCOPES`
to the Credentials constructor without a matching re-auth of the
prod refresh token.
"""
from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
GMAIL_API = REPO / "src" / "core" / "gmail_api.py"


def test_build_credentials_omits_scopes_on_refresh():
    """The `scopes=` kwarg must NOT appear in the Credentials(...) call
    inside `_build_credentials`. If a future change wants to widen the
    active scopes, it must also re-run the OAuth setup script and update
    both refresh tokens in Railway env — not just edit SCOPES and hope.
    """
    src = GMAIL_API.read_text(encoding="utf-8")
    # Isolate the _build_credentials function body.
    m = re.search(
        r"def _build_credentials\(.*?\):\n(.*?)(?=\ndef |\Z)",
        src,
        re.DOTALL,
    )
    assert m, "Could not locate _build_credentials in gmail_api.py"
    body = m.group(1)
    assert "scopes=" not in body, (
        "gmail_api._build_credentials must not pass `scopes=` to "
        "Credentials — forcing scopes on refresh produces "
        "`invalid_scope: Bad Request` when the token was granted a "
        "narrower set. Let the refresh use originally-granted scopes. "
        "See tests/test_gmail_api_scope_refresh.py docstring for the "
        "2026-04-22 incident details."
    )
