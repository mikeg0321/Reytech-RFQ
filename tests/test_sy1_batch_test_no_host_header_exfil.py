"""SY-1 regression guard: api_agents_batch_test must not leak admin Basic Auth
creds via an attacker-controlled Host header.

Audited 2026-04-22 — `flask_req.host_url` echoed whatever Host header the caller
sent (SERVER_NAME unset on Railway), so a crafted `<form action="...">` on a
page the admin visits could coerce the self-test to hit `evil.example.com` with
`auth=(user, pass)` and `verify=False`. Fix: pin base to loopback, drop the
verify flag.

These tests are source-level guards — if the loopback pin or verify=False
regression sneaks back in, CI fails before it can ship.
"""
from __future__ import annotations

import io
import re
import tokenize
from pathlib import Path


ROUTES_AGENTS = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_agents.py"
)


def _strip_comments_and_docstrings(src: str) -> str:
    """Return src with # comments and triple-quoted strings removed.

    Prevents tests from matching their own explanatory comments or docstrings.
    Preserves line structure so regex anchored on `def ...` still works.
    """
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    out_lines: list[str] = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            out_lines.append("")
            continue
        if "#" in line:
            in_single = False
            in_double = False
            cut = None
            for i, ch in enumerate(line):
                if ch == "'" and not in_double:
                    in_single = not in_single
                elif ch == '"' and not in_single:
                    in_double = not in_double
                elif ch == "#" and not in_single and not in_double:
                    cut = i
                    break
            if cut is not None:
                line = line[:cut].rstrip()
        out_lines.append(line)
    return "\n".join(out_lines)


def _extract_batch_test_fn(src: str) -> str:
    """Return just the body of api_agents_batch_test for targeted checks."""
    m = re.search(
        r"def api_agents_batch_test\(\):[\s\S]*?(?=\n(?:@bp\.route|def |# ═+))",
        src,
    )
    assert m, "api_agents_batch_test not found in routes_agents.py"
    return m.group(0)


def test_batch_test_does_not_use_host_url():
    """host_url is attacker-controlled. Loopback pin must replace it."""
    src = ROUTES_AGENTS.read_text(encoding="utf-8")
    fn = _extract_batch_test_fn(_strip_comments_and_docstrings(src))
    assert "host_url" not in fn, (
        "SY-1 regression: api_agents_batch_test uses flask_req.host_url. "
        "Host header is attacker-controllable — pin base to 127.0.0.1 instead."
    )


def test_batch_test_does_not_disable_tls_verify():
    """verify=False combined with auth= ships creds to any spoofed endpoint."""
    src = ROUTES_AGENTS.read_text(encoding="utf-8")
    fn = _extract_batch_test_fn(_strip_comments_and_docstrings(src))
    assert "verify=False" not in fn, (
        "SY-1 regression: api_agents_batch_test passes verify=False to "
        "requests.get. Loopback does not need verify=False — remove it."
    )


def test_batch_test_pins_to_loopback():
    """Positive check: base URL must be 127.0.0.1 (loopback)."""
    src = ROUTES_AGENTS.read_text(encoding="utf-8")
    fn = _extract_batch_test_fn(_strip_comments_and_docstrings(src))
    assert "127.0.0.1" in fn, (
        "SY-1 fix missing: api_agents_batch_test must build its base URL from "
        "127.0.0.1 + os.environ['PORT'] instead of a caller-controlled host."
    )


def test_batch_test_reads_port_from_env():
    """Railway's gunicorn binds $PORT, not 8080 blindly."""
    src = ROUTES_AGENTS.read_text(encoding="utf-8")
    fn = _extract_batch_test_fn(_strip_comments_and_docstrings(src))
    assert 'os.environ.get("PORT"' in fn or "os.environ.get('PORT'" in fn, (
        "SY-1 fix must read PORT from env so the self-test hits the same "
        "port gunicorn is listening on."
    )


def test_batch_test_still_scheme_http():
    """Loopback should be plain http — no TLS on 127.0.0.1 inside the pod."""
    src = ROUTES_AGENTS.read_text(encoding="utf-8")
    fn = _extract_batch_test_fn(_strip_comments_and_docstrings(src))
    assert 'f"http://127.0.0.1' in fn or "f'http://127.0.0.1" in fn, (
        "SY-1: loopback base must be http://127.0.0.1:<PORT> — gunicorn "
        "does not terminate TLS inside the container."
    )


def test_batch_test_keeps_auth_passthrough():
    """The self-test still needs Basic Auth (routes are @auth_required).
    Loopback pin alone is the fix — do not accidentally drop the auth kwarg.
    """
    src = ROUTES_AGENTS.read_text(encoding="utf-8")
    fn = _extract_batch_test_fn(_strip_comments_and_docstrings(src))
    assert "auth.username" in fn and "auth.password" in fn, (
        "api_agents_batch_test needs to forward Basic Auth to @auth_required "
        "endpoints. Loopback target is safe because the creds never leave "
        "the pod."
    )
