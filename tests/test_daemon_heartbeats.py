"""Pin S-11 — daemon loops emit scheduler heartbeats.

Audit 2026-05-07 v2 §S-11: 6 daemon `while True` loops were invisible
to the scheduler watchdog because none called `heartbeat()`. A silent
crash in `gdrive-worker` accumulated Drive tasks forever; nothing
alerted.

This PR patches the 3 named, locatable daemons:
  * scprs-export-watcher (src/agents/scprs_export_watcher.py)
  * gdrive-worker        (src/core/gdrive.py)
  * utilization-flusher  (src/core/utilization.py)

The 3 unnamed dashboard.py loops are out of scope for this PR (riskier
to patch without a clearer ownership model).

These tests pin source-level guards: each daemon's body imports
register_job + heartbeat and calls them inside its loop.
"""
from __future__ import annotations

import os
import sys
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(path):
    import pathlib
    return pathlib.Path(path).read_text(encoding="utf-8")


def _extract_function(src: str, fn_name: str) -> str:
    """Extract a Python function body from source text."""
    start = src.find(f"def {fn_name}(")
    assert start >= 0, f"Function {fn_name} not found"
    end = src.find("\ndef ", start + 1)
    return src[start:end if end > start else len(src)]


class TestScprsExportWatcherHasHeartbeat:
    def test_run_loop_imports_scheduler(self):
        src = _read("src/agents/scprs_export_watcher.py")
        body = _extract_function(src, "_run_loop")
        assert "register_job" in body, \
            "S-11: scprs-export-watcher does not register with scheduler"
        assert "heartbeat" in body, \
            "S-11: scprs-export-watcher does not emit heartbeats"
        assert "scprs-export-watcher" in body, \
            "S-11: register_job called with wrong name"

    def test_run_loop_emits_heartbeat_in_both_branches(self):
        src = _read("src/agents/scprs_export_watcher.py")
        body = _extract_function(src, "_run_loop")
        # Heartbeat on success
        assert re.search(r"_hb\(.*success=True", body), \
            "S-11: missing success heartbeat"
        # Heartbeat on error
        assert re.search(r"_hb\(.*success=False", body), \
            "S-11: missing error heartbeat"


class TestGdriveWorkerHasHeartbeat:
    def test_worker_imports_scheduler(self):
        src = _read("src/core/gdrive.py")
        body = _extract_function(src, "_ensure_worker")
        assert "register_job" in body
        assert "heartbeat" in body
        assert "gdrive-worker" in body, \
            "S-11: register_job called with wrong name"


class TestUtilizationFlusherHasHeartbeat:
    def test_flusher_loop_imports_scheduler(self):
        src = _read("src/core/utilization.py")
        body = _extract_function(src, "_flusher_loop")
        assert "register_job" in body
        assert "heartbeat" in body
        assert "utilization-flusher" in body, \
            "S-11: register_job called with wrong name"


class TestAllThreeHaveS11Sentinel:
    """The S-11 sentinel comment must be present in each patched
    daemon so a future maintainer who searches for the audit ID
    finds the fix."""

    def test_scprs_export_watcher_has_s11(self):
        assert "S-11" in _read("src/agents/scprs_export_watcher.py")

    def test_gdrive_has_s11(self):
        assert "S-11" in _read("src/core/gdrive.py")

    def test_utilization_has_s11(self):
        assert "S-11" in _read("src/core/utilization.py")
