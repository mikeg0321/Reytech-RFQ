"""Tests for scripts/railway_deploy_watcher.py (Item B of the
P0 resilience backlog). Mocks the Railway API and /ping so the tests
run offline and cannot touch prod.
"""
import os
import sys
from unittest.mock import patch, MagicMock

import pytest

# Load the watcher module directly from scripts/ (not a package)
import importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.join(os.path.dirname(HERE), "scripts",
                             "railway_deploy_watcher.py")
spec = importlib.util.spec_from_file_location("railway_deploy_watcher", SCRIPT_PATH)
watcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watcher)


def _deploy(id="dep-1", status="SUCCESS", commit="abc123", reason=""):
    return {
        "id": id,
        "status": status,
        "created": "2026-04-13T20:00:00",
        "commit": commit,
        "reason": reason,
    }


class TestFindPreviousSuccess:
    def test_returns_first_success_that_isnt_current(self):
        deploys = [
            _deploy("dep-new", "FAILED"),
            _deploy("dep-prev", "SUCCESS"),
            _deploy("dep-older", "SUCCESS"),
        ]
        got = watcher._find_previous_success(deploys, "dep-new")
        assert got is not None
        assert got["id"] == "dep-prev"

    def test_skips_previous_auto_rollback_entries(self):
        deploys = [
            _deploy("dep-new", "FAILED"),
            _deploy("dep-recent", "SUCCESS",
                    reason=watcher.ROLLBACK_REASON_MARKER + " target"),
            _deploy("dep-older", "SUCCESS"),
        ]
        got = watcher._find_previous_success(deploys, "dep-new")
        assert got is not None
        assert got["id"] == "dep-older"

    def test_returns_none_when_no_clean_success_exists(self):
        deploys = [
            _deploy("dep-new", "FAILED"),
            _deploy("dep-a", "FAILED"),
            _deploy("dep-b", "CRASHED"),
        ]
        assert watcher._find_previous_success(deploys, "dep-new") is None

    def test_skips_current_id_even_if_success(self):
        deploys = [
            _deploy("dep-new", "SUCCESS"),
        ]
        assert watcher._find_previous_success(deploys, "dep-new") is None


class TestWatchDeploy:
    def test_early_exit_when_latest_is_success(self, monkeypatch):
        deploys = [_deploy("dep-new", "SUCCESS")]
        monkeypatch.setattr(watcher, "_fetch_deployments", lambda: deploys)
        # Not strictly needed but safe
        monkeypatch.setattr(watcher, "_ping_healthy", lambda url, timeout_sec=5: True)
        monkeypatch.setattr(watcher.time, "sleep", lambda *a, **kw: None)
        rc = watcher.watch_deploy(max_poll_minutes=1, dry_run=True)
        assert rc == 0

    def test_no_rollback_env_var_shortcircuits(self, monkeypatch):
        monkeypatch.setenv("NO_ROLLBACK", "1")
        rc = watcher.watch_deploy(max_poll_minutes=1, dry_run=True)
        assert rc == 0

    def test_rollback_triggers_on_failed_with_unhealthy_ping(self, monkeypatch):
        """FAILED for >=90s AND /ping unhealthy → rollback fires."""
        deploys = [
            _deploy("dep-new", "FAILED"),
            _deploy("dep-prev", "SUCCESS"),
        ]
        monkeypatch.setattr(watcher, "_fetch_deployments", lambda: deploys)
        monkeypatch.setattr(watcher, "_ping_healthy", lambda url, timeout_sec=5: False)
        monkeypatch.setattr(watcher.time, "sleep", lambda *a, **kw: None)

        # Bypass the FAILED hold timer by patching time.time so it jumps
        # forward past FAILED_MIN_SECONDS_BEFORE_ROLLBACK between polls
        fake_now = [1000.0]
        def fake_time():
            current = fake_now[0]
            fake_now[0] += 100  # each call advances 100s
            return current
        monkeypatch.setattr(watcher.time, "time", fake_time)

        trigger_calls = []
        def fake_trigger(target_id, dry_run=False):
            trigger_calls.append((target_id, dry_run))
            return True
        monkeypatch.setattr(watcher, "_trigger_rollback", fake_trigger)

        rc = watcher.watch_deploy(max_poll_minutes=10, dry_run=True)
        assert rc == 0
        assert trigger_calls, "rollback should have been triggered"
        assert trigger_calls[0][0] == "dep-prev"
        assert trigger_calls[0][1] is True  # dry_run propagated

    def test_no_rollback_when_ping_is_healthy_despite_failed(self, monkeypatch):
        """Flaky-healthcheck case: Railway says FAILED but /ping 200s.
        Should NOT rollback — deploy is actually fine."""
        deploys = [
            _deploy("dep-new", "FAILED"),
            _deploy("dep-prev", "SUCCESS"),
        ]
        monkeypatch.setattr(watcher, "_fetch_deployments", lambda: deploys)
        monkeypatch.setattr(watcher, "_ping_healthy", lambda url, timeout_sec=5: True)
        monkeypatch.setattr(watcher.time, "sleep", lambda *a, **kw: None)

        fake_now = [1000.0]
        def fake_time():
            current = fake_now[0]
            fake_now[0] += 100
            return current
        monkeypatch.setattr(watcher.time, "time", fake_time)

        trigger_calls = []
        monkeypatch.setattr(
            watcher, "_trigger_rollback",
            lambda tid, dry_run=False: trigger_calls.append(tid) or True,
        )

        # Will eventually hit deadline since we never SUCCESS and never
        # rollback. rc=0 because we reached max_poll_minutes cleanly.
        rc = watcher.watch_deploy(max_poll_minutes=1, dry_run=True)
        assert rc == 0
        assert not trigger_calls, "ping-healthy FAILED should not trigger rollback"

    def test_refuses_rollback_loop_on_prior_auto_rollback(self, monkeypatch):
        """If the FAILED deploy is itself a prior auto-rollback,
        refuse to rollback again. Need human."""
        deploys = [
            _deploy("dep-new", "FAILED",
                    reason=watcher.ROLLBACK_REASON_MARKER + " from dep-prev"),
            _deploy("dep-prev", "SUCCESS"),
        ]
        monkeypatch.setattr(watcher, "_fetch_deployments", lambda: deploys)
        monkeypatch.setattr(watcher, "_ping_healthy", lambda url, timeout_sec=5: False)
        monkeypatch.setattr(watcher.time, "sleep", lambda *a, **kw: None)

        fake_now = [1000.0]
        def fake_time():
            current = fake_now[0]
            fake_now[0] += 100
            return current
        monkeypatch.setattr(watcher.time, "time", fake_time)

        trigger_calls = []
        monkeypatch.setattr(
            watcher, "_trigger_rollback",
            lambda tid, dry_run=False: trigger_calls.append(tid) or True,
        )

        rc = watcher.watch_deploy(max_poll_minutes=10, dry_run=True)
        assert rc == 1  # unrecoverable
        assert not trigger_calls, "must refuse rollback loop"

    def test_no_previous_success_to_rollback_to(self, monkeypatch):
        """First-ever deploy fails — no previous SUCCESS to roll back
        to. Should exit 1 and notify for human intervention."""
        deploys = [
            _deploy("dep-new", "FAILED"),
            _deploy("dep-a", "FAILED"),
        ]
        monkeypatch.setattr(watcher, "_fetch_deployments", lambda: deploys)
        monkeypatch.setattr(watcher, "_ping_healthy", lambda url, timeout_sec=5: False)
        monkeypatch.setattr(watcher.time, "sleep", lambda *a, **kw: None)

        fake_now = [1000.0]
        def fake_time():
            current = fake_now[0]
            fake_now[0] += 100
            return current
        monkeypatch.setattr(watcher.time, "time", fake_time)

        rc = watcher.watch_deploy(max_poll_minutes=10, dry_run=True)
        assert rc == 1
