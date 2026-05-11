"""Pin S-11 — daemon loops emit scheduler heartbeats.

Audit 2026-05-07 v2 §S-11: 6 daemon `while True` loops were invisible
to the scheduler watchdog because none called `heartbeat()`. A silent
crash in `gdrive-worker` accumulated Drive tasks forever; nothing
alerted.

Phase 1 (PR #857) patched the 3 named, locatable daemons:
  * scprs-export-watcher (src/agents/scprs_export_watcher.py)
  * gdrive-worker        (src/core/gdrive.py)
  * utilization-flusher  (src/core/utilization.py)

Phase 2 (this PR) closes the remaining 3 unnamed dashboard.py loops:
  * daily-cleanup        (dashboard.py:_daily_cleanup)
  * form-updater         (dashboard.py:_form_update_scheduler)
  * forms-drift          (dashboard.py:_forms_drift_scheduler)

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


# ─── Phase 2: dashboard.py daemons ─────────────────────────────────────

def _extract_dashboard_daemon(fn_name: str) -> str:
    """Pull a dashboard.py nested daemon closure out of the file.

    Closures inside boot blocks; cut from `def <fn_name>(` until the
    next `threading.Thread(target=<fn_name>` line that starts the thread
    (or until 100 lines, whichever comes first)."""
    src = _read("src/api/dashboard.py")
    start = src.find(f"def {fn_name}(")
    assert start >= 0, f"Daemon {fn_name} not found in dashboard.py"
    # End at the Thread(target=fn_name ...) starter, which always
    # immediately follows the def block.
    end_marker = f"target={fn_name}"
    end = src.find(end_marker, start)
    if end < 0:
        end = start + 6000  # generous fallback
    return src[start:end]


class TestDailyCleanupHasHeartbeat:
    def test_imports_scheduler(self):
        body = _extract_dashboard_daemon("_daily_cleanup")
        assert "register_job" in body, \
            "S-11: daily-cleanup does not register with scheduler"
        assert "heartbeat" in body, \
            "S-11: daily-cleanup does not emit heartbeats"
        assert '"daily-cleanup"' in body, \
            "S-11: register_job called with wrong name"

    def test_emits_heartbeat_in_both_branches(self):
        body = _extract_dashboard_daemon("_daily_cleanup")
        assert re.search(r"_hb\(.*success=True", body), \
            "S-11: missing success heartbeat in daily-cleanup"
        assert re.search(r"_hb\(.*success=False", body), \
            "S-11: missing error heartbeat in daily-cleanup"


class TestFormUpdaterHasHeartbeat:
    def test_imports_scheduler(self):
        body = _extract_dashboard_daemon("_form_update_scheduler")
        assert "register_job" in body, \
            "S-11: form-updater does not register with scheduler"
        assert "heartbeat" in body, \
            "S-11: form-updater does not emit heartbeats"
        assert '"form-updater"' in body, \
            "S-11: register_job called with wrong name"

    def test_emits_heartbeat_in_both_branches(self):
        body = _extract_dashboard_daemon("_form_update_scheduler")
        assert re.search(r"_hb\(.*success=True", body), \
            "S-11: missing success heartbeat in form-updater"
        assert re.search(r"_hb\(.*success=False", body), \
            "S-11: missing error heartbeat in form-updater"


class TestFormsDriftHasHeartbeat:
    def test_imports_scheduler(self):
        body = _extract_dashboard_daemon("_forms_drift_scheduler")
        assert "register_job" in body, \
            "S-11: forms-drift does not register with scheduler"
        assert "heartbeat" in body, \
            "S-11: forms-drift does not emit heartbeats"
        assert '"forms-drift"' in body, \
            "S-11: register_job called with wrong name"

    def test_emits_heartbeat_in_both_branches(self):
        body = _extract_dashboard_daemon("_forms_drift_scheduler")
        assert re.search(r"_hb\(.*success=True", body), \
            "S-11: missing success heartbeat in forms-drift"
        assert re.search(r"_hb\(.*success=False", body), \
            "S-11: missing error heartbeat in forms-drift"


class TestDashboardHasS11Sentinel:
    """The S-11 sentinel comment must be present in dashboard.py
    so a future maintainer who searches for the audit ID finds
    Phase 2's fixes too."""

    def test_dashboard_has_s11(self):
        body = _read("src/api/dashboard.py")
        # Must reference S-11 at least 3 times (one per daemon comment)
        assert body.count("S-11") >= 3, \
            "S-11: expected at least 3 S-11 sentinel comments in dashboard.py"
