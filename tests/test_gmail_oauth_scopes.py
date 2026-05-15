"""PR-AV5 (AV-15) — Gmail OAuth scope contract.

The `/api/rfq/<rid>/create-draft` route called save_draft() and got
a 403 "Insufficient Permission" today. Root cause: SCOPES had
`gmail.readonly + gmail.send + drive.readonly` but NOT
`gmail.compose`. Google's drafts.create endpoint requires
gmail.compose (or gmail.modify); gmail.send alone authorizes
outbound message-send but NOT draft creation.

This file pins:
  1. SCOPES in src/core/gmail_api.py includes gmail.compose
  2. scripts/gmail_oauth_setup.py SCOPES list stays in lockstep
     (divergence here causes invalid_scope on token refresh)
  3. save_draft() surfaces a clear RuntimeError on 403 instead of
     propagating the raw googleapiclient exception
"""
from __future__ import annotations

import re
import pytest


GMAIL_COMPOSE = "https://www.googleapis.com/auth/gmail.compose"
GMAIL_SEND = "https://www.googleapis.com/auth/gmail.send"
GMAIL_READONLY = "https://www.googleapis.com/auth/gmail.readonly"


def test_gmail_api_scopes_include_compose():
    from src.core.gmail_api import SCOPES
    assert GMAIL_COMPOSE in SCOPES, (
        "gmail.compose missing from src.core.gmail_api.SCOPES — "
        "save_draft() will 403 on every call until granted."
    )


def test_gmail_api_scopes_still_include_send_and_readonly():
    """Sanity: don't regress the existing scopes that the poller +
    send paths depend on."""
    from src.core.gmail_api import SCOPES
    assert GMAIL_SEND in SCOPES
    assert GMAIL_READONLY in SCOPES


def test_setup_script_scopes_match_api_scopes():
    """If the script SCOPES diverge from gmail_api.SCOPES, refreshing
    the token will fail with `invalid_scope: Bad Request`. Pin
    lockstep by reading both lists.

    The script file isn't a regular import target (top-level main()
    code), so parse the literal list out of the source.
    """
    from src.core.gmail_api import SCOPES as API_SCOPES
    with open("scripts/gmail_oauth_setup.py", encoding="utf-8") as f:
        src = f.read()
    # Pull every quoted googleapis URL inside the SCOPES = [ ... ] block
    block_match = re.search(r"SCOPES\s*=\s*\[(.*?)\]", src, re.DOTALL)
    assert block_match, "Could not find SCOPES = [ … ] in setup script"
    block = block_match.group(1)
    script_scopes = set(re.findall(
        r"[\"\'](https://www\.googleapis\.com/auth/[a-z\.]+)[\"\']",
        block,
    ))
    assert GMAIL_COMPOSE in script_scopes, (
        "scripts/gmail_oauth_setup.py SCOPES missing gmail.compose"
    )
    # And the lockstep — every API scope must be in the setup script
    for s in API_SCOPES:
        assert s in script_scopes, (
            f"setup script missing {s} from gmail_api.SCOPES; "
            f"running it would produce a token that 403s in prod."
        )


def test_save_draft_403_raises_actionable_runtime_error():
    """When googleapiclient raises a 403 'Insufficient Permission',
    save_draft must convert it to a RuntimeError naming the OAuth
    setup script. Tests use the docstring text in the message to
    catch wording regressions."""
    import src.core.gmail_api as gm_mod

    class _FakeRequest:
        def execute(self):
            raise Exception(
                'HttpError 403: Request had insufficient authentication '
                'scopes. Details: "[{\'message\': \'Insufficient '
                "Permission'}]\""
            )

    class _FakeDrafts:
        def create(self, userId="me", body=None):
            return _FakeRequest()

    class _FakeUsers:
        def drafts(self):
            return _FakeDrafts()

    class _FakeService:
        def users(self):
            return _FakeUsers()

    with pytest.raises(RuntimeError) as ei:
        gm_mod.save_draft(
            service=_FakeService(),
            to="test@example.com",
            subject="x",
            body_plain="y",
        )
    msg = str(ei.value)
    assert "gmail.compose" in msg
    assert "gmail_oauth_setup" in msg


def test_save_draft_non_403_error_propagates_unchanged():
    """A non-permission error (network, payload, server) must
    propagate unchanged — don't mask real bugs as scope problems."""
    import src.core.gmail_api as gm_mod

    class _FakeRequest:
        def execute(self):
            raise Exception("HttpError 500: Internal Server Error")

    class _FakeDrafts:
        def create(self, userId="me", body=None):
            return _FakeRequest()

    class _FakeUsers:
        def drafts(self):
            return _FakeDrafts()

    class _FakeService:
        def users(self):
            return _FakeUsers()

    with pytest.raises(Exception) as ei:
        gm_mod.save_draft(
            service=_FakeService(),
            to="test@example.com",
            subject="x",
            body_plain="y",
        )
    # Must NOT be the RuntimeError variant — should be the raw exception
    assert "Internal Server Error" in str(ei.value)
    assert "gmail.compose" not in str(ei.value)
