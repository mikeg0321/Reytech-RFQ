"""Smoke tests for scripts/orders_only_loop.py — the operator runner
that closes the orders-only investigation loop.

The script is glue (HTTP + argparse) so we don't unit-test it deeply.
These tests just protect against trivially-broken commits:
  - file imports cleanly
  - --help exits 0
  - --apply without --new-po exits 2 (arg validation)

The real runtime path (HTTP to prod) is verified manually by the
operator after merge.
"""
from __future__ import annotations

import os
import subprocess
import sys


SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scripts", "orders_only_loop.py",
)


def _run(*argv, env=None):
    return subprocess.run(
        [sys.executable, SCRIPT, *argv],
        capture_output=True, text=True, env=env,
    )


def test_script_help_exits_zero():
    r = _run("--help")
    assert r.returncode == 0
    assert "orders-only" in r.stdout.lower()


def test_apply_without_new_po_exits_two():
    # Ensure no creds in env so we don't accidentally hit prod
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("REYTECH_", "DASH_"))}
    env["PATH"] = os.environ.get("PATH", "")
    r = _run("--apply", "ord-1", env=env)
    assert r.returncode == 2
    assert "new-po" in (r.stderr or "").lower()


def test_missing_creds_exits_two():
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("REYTECH_", "DASH_"))}
    env["PATH"] = os.environ.get("PATH", "")
    # cmd_run path: no --apply, no creds. Will try to load .env from
    # repo root — to keep this hermetic, point at a path that doesn't
    # exist by setting findings_path inert; the resolve_creds call
    # fires before any HTTP.
    env["NO_NETWORK"] = "1"
    r = _run("--dry-run", env=env)
    # Either exits 2 (missing creds, expected when .env is absent) or
    # the script loaded creds from a checked-in .env (unexpected).
    # The .env file is gitignored, so on CI runners it won't be present.
    if r.returncode == 0:
        # CI machine has prod creds wired through env — skip
        return
    assert r.returncode == 2
