"""Phase 1.6 Enhancement A: scheduler wiring smoke tests.

Verifies the two new daemon threads are registered at boot. Doesn't
exercise the actual schedule — just confirms wiring + that the
underlying functions they call work in isolation.
"""

import os
import threading
from unittest.mock import patch

import pytest


class TestSchedulerThreadsRegistered:
    """At boot, both new daemons should appear in the thread list."""

    def test_forms_drift_thread_starts(self, app):
        # The app fixture triggers create_app() which registers the threads
        names = {t.name for t in threading.enumerate()}
        assert "forms-drift" in names, (
            f"forms-drift thread not registered. Found: {sorted(names)}"
        )

    def test_training_bootstrap_thread_starts(self, app):
        names = {t.name for t in threading.enumerate()}
        assert "training-bootstrap" in names, (
            f"training-bootstrap thread not registered. Found: {sorted(names)}"
        )


class TestUnderlyingFunctions:
    """The functions the schedulers call don't crash on dev DB."""

    def test_drift_scan_returns_dict(self, app):
        from src.agents.forms_drift_monitor import scan_forms_drift
        r = scan_forms_drift(days=30)
        assert isinstance(r, dict)
        assert "lookback_days" in r

    def test_coverage_report_returns_dict(self, app):
        from src.agents.training_corpus import coverage_report
        r = coverage_report()
        assert isinstance(r, dict)
        assert "total_pairs" in r

    def test_bootstrap_runs_without_crashing_on_empty_orders(self, app, tmp_path):
        from src.agents.training_corpus import bootstrap_from_orders
        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            r = bootstrap_from_orders(days=30, limit=5)
        assert isinstance(r, dict)
        assert "scanned" in r
