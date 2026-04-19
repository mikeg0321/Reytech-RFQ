"""Regression guard: ops_monitor.check_disk_usage must not NameError on shutil.

Incident 2026-04-19: prod logs showed `Disk check failed: name 'shutil' is not
defined` in ops_monitor._monitor_loop because `shutil` was used but never
imported in src/core/ops_monitor.py. Disk-health monitoring was blind.
"""
import pytest

from src.core.ops_monitor import check_disk_usage


def test_check_disk_usage_does_not_nameerror_on_shutil(tmp_path):
    result = check_disk_usage(str(tmp_path))
    for w in result.get("warnings", []):
        assert "name 'shutil' is not defined" not in w, (
            "ops_monitor.check_disk_usage regressed — shutil import missing"
        )


def test_check_disk_usage_returns_expected_keys(tmp_path):
    result = check_disk_usage(str(tmp_path))
    assert "ok" in result
    assert "total_gb" in result
    assert "used_gb" in result
    assert "free_gb" in result
    assert "percent_used" in result
