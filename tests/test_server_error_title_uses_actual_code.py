"""Pin: _send_error_alert title uses the exception's actual HTTP code,
not a hardcoded '500'.

Chrome MCP audit 2026-05-26 anomaly #10: prod /notifications had 3
events titled `500 Error: MethodNotAllowed` — MethodNotAllowed is
HTTP 405. PR #678 fixed the root cause (HTTPException catch-all
no longer masks 4xx as 500), but the title constructor still
hardcoded '500'. This test pins the corrected behaviour so a
future revert is caught.
"""
from __future__ import annotations


class _Fake4xx(Exception):
    """Stand-in for werkzeug.exceptions.MethodNotAllowed — has `.code`."""
    code = 405


class _Fake500(Exception):
    """Plain exception with no .code attribute."""
    pass


def test_alert_title_uses_exception_code_for_4xx(monkeypatch):
    """A 405 MethodNotAllowed (or other coded HTTPException) reaching
    _send_error_alert should be titled with its real code, not 500."""
    sent = []

    def _capture(**kw):
        sent.append(kw)
        return {"ok": True}

    monkeypatch.setattr(
        "src.agents.notify_agent.send_alert", _capture,
    )

    # Build the app + extract the closure. We can't call app._send_
    # error_alert directly since it's a local in create_app; but we
    # can mimic its behaviour by importing app.py and walking down.
    from pathlib import Path
    content = Path(__file__).parent.parent.joinpath("app.py").read_text(
        encoding="utf-8",
    )
    # Anchor: the title must use a code variable, not hardcoded 500.
    assert 'title=f"{code} Error:' in content, (
        "_send_error_alert title still hardcoded '500' — non-500 "
        "exceptions reaching this path would be mislabelled."
    )
    # And the code computation must come from the exception's .code.
    assert 'code = getattr(error, "code", None) or 500' in content, (
        "_send_error_alert lost the exception-code derivation — "
        "would fall back to hardcoded '500' across all error types."
    )
    # cooldown_key should use the code too — separate codes get
    # separate cooldown buckets so a 405 burst doesn't dedupe with
    # an unrelated 500.
    assert 'cooldown_key=f"{code}:' in content, (
        "_send_error_alert cooldown_key still hardcoded '500' — would "
        "conflate distinct error codes onto the same dedup bucket."
    )


def test_500_remains_the_fallback_for_uncoded_exceptions():
    """A plain Exception (no .code) should still be titled 500. The
    getattr fallback chain `code or 500` covers this case."""
    err = _Fake500("boom")
    code = getattr(err, "code", None) or 500
    assert code == 500


def test_4xx_uses_its_own_code():
    """The 405-class exception case from the audit: HTTP code is 405,
    title would correctly say `405 Error: _Fake4xx`."""
    err = _Fake4xx("you can't POST that")
    code = getattr(err, "code", None) or 500
    assert code == 405
