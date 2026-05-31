#!/bin/bash
# SessionStart hook for Claude Code on the web.
#
# Why this exists: the web container ships Python 3.11 as the default
# `python3`, but this repo targets 3.12 (pyproject.toml) and uses 3.12-only
# syntax in places (e.g. src/api/modules/routes_pricecheck.py). The container
# also has none of the runtime deps installed (flask, reportlab, pypdf,
# pdfplumber, pydantic, pytest). Without this hook the Spine test suite and
# the LAW-6 forcing gates simply cannot run in a web session — they error on
# collection ("No module named flask") and reviewers are forced to ship blind.
#
# This builds a cached 3.12 venv with the pinned requirements and makes it the
# session's default `python`, so `python -m pytest tests/...` Just Works.
# Container state is cached after the hook completes, so the slow first run
# (full pip install) only happens once per environment.
#
# Runs ASYNC: the session starts immediately while the venv builds in the
# background, so you are not blocked on the first (uncached) pip install.
# Trade-off — a race window exists: a command issued in the first ~seconds of
# a *fresh* container may run before deps finish installing (or before
# CLAUDE_ENV_FILE's PATH export is applied), and would hit "No module named
# flask". Cached resumes (~2.4s) are effectively race-free. If you'd rather
# trade startup latency for a hard guarantee, drop the async line below to
# make it synchronous again.
set -euo pipefail

# Web-only. Local sessions already have the developer's own environment.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Async: emit the directive first, then keep working in the background.
echo '{"async": true, "asyncTimeout": 300000}'

cd "${CLAUDE_PROJECT_DIR:-$(pwd)}"

VENV=".venv"
PY312="$(command -v python3.12 || true)"

if [ -z "$PY312" ]; then
  echo "session-start: python3.12 not found on PATH — cannot build the 3.12 venv" >&2
  exit 1
fi

# Build the venv once; reuse the cached one on resume/clear/compact.
if [ ! -x "$VENV/bin/python" ]; then
  "$PY312" -m venv "$VENV"
fi

# Idempotent: pip skips already-satisfied pins, so re-runs are fast.
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/python" -m pip install --quiet -r requirements.txt

# Test-only deps (PyMuPDF/fitz etc.) — kept out of production requirements
# on purpose, but installed here so the fitz-gated flatten/visual_qa tests
# actually run in web sessions instead of skipping.
if [ -f requirements-dev.txt ]; then
  "$VENV/bin/python" -m pip install --quiet -r requirements-dev.txt
fi

# Surface the venv as the session's default interpreter. Everything the agent
# runs afterwards (python, pip, pytest) resolves to the 3.12 venv.
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  {
    echo "export VIRTUAL_ENV=\"$CLAUDE_PROJECT_DIR/$VENV\""
    echo "export PATH=\"$CLAUDE_PROJECT_DIR/$VENV/bin:\$PATH\""
    echo "export PYTHONPATH=\"$CLAUDE_PROJECT_DIR\""
  } >> "$CLAUDE_ENV_FILE"
fi

echo "session-start: 3.12 venv ready ($("$VENV/bin/python" -V 2>&1))"
