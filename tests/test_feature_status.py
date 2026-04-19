"""Tests for the feature_status table + record/query API (PR #188).

PRs #181-#187 added drainable skip ledgers to five modules
(item_link_lookup, agency_config, pricing_oracle_v2, award_tracker,
core/db). Each ledger fills with SkipReason events and is drained by
its caller (orchestrator, scheduler, route).

But each drain only covers ONE call site at ONE moment. An operator
opening the dashboard 14 minutes after the last quote ran has no idea
which features are currently degraded — by the time they look, every
ledger is empty.

`feature_status` solves this by persisting the drained skips into a
SQLite table keyed by (name, where, severity). Each occurrence bumps
a counter and updates `last_seen`. The dashboard banner reads this
table to show "Claude amazon lookup: degraded since 14m ago (37 hits)"
without having to wait for the next pipeline run.

The table is auto-created on first use, auto-pruned on read so stale
entries don't haunt the banner forever, and the public surface is
two functions:

    record_skips(skips: list[SkipReason]) -> None
    current_status() -> list[dict]

This test file covers the data layer only; the dashboard banner JSON
endpoint and partial render are exercised in route tests.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from src.core import feature_status
from src.core.dependency_check import Severity, SkipReason


@pytest.fixture(autouse=True)
def _isolate_table(tmp_path, monkeypatch):
    """Each test gets its own SQLite file so they don't poison each other."""
    db_path = tmp_path / "feature_status_test.db"
    monkeypatch.setattr(feature_status, "_DB_PATH_OVERRIDE", str(db_path))
    yield


def _skip(name="claude_amazon_lookup", where="claude_amazon_lookup",
          severity=Severity.WARNING, reason="ANTHROPIC_API_KEY env var unset"):
    return SkipReason(name=name, reason=reason, severity=severity, where=where)


class TestTableAutoCreation:
    def test_first_call_creates_table(self):
        # No setup — record_skips on a fresh DB must work.
        feature_status.record_skips([_skip()])
        rows = feature_status.current_status()
        assert len(rows) == 1
        assert rows[0]["name"] == "claude_amazon_lookup"

    def test_idempotent_reinit(self):
        feature_status.record_skips([_skip()])
        feature_status.record_skips([_skip()])
        rows = feature_status.current_status()
        assert len(rows) == 1, "same key must dedupe, not double-row"


class TestRecordingAndDedup:
    def test_same_key_increments_count(self):
        feature_status.record_skips([_skip()])
        feature_status.record_skips([_skip()])
        feature_status.record_skips([_skip()])
        rows = feature_status.current_status()
        assert len(rows) == 1
        assert rows[0]["count"] == 3

    def test_different_where_creates_separate_rows(self):
        feature_status.record_skips([
            _skip(where="claude_amazon_lookup"),
            _skip(where="claude_product_lookup"),
        ])
        rows = feature_status.current_status()
        assert len(rows) == 2
        wheres = sorted(r["where"] for r in rows)
        assert wheres == ["claude_amazon_lookup", "claude_product_lookup"]

    def test_different_severity_creates_separate_rows(self):
        feature_status.record_skips([
            _skip(severity=Severity.WARNING),
            _skip(severity=Severity.INFO),
        ])
        rows = feature_status.current_status()
        assert len(rows) == 2

    def test_different_name_creates_separate_rows(self):
        feature_status.record_skips([
            _skip(name="ANTHROPIC_API_KEY"),
            _skip(name="requests"),
        ])
        rows = feature_status.current_status()
        assert len(rows) == 2

    def test_last_seen_updates_on_repeat(self):
        feature_status.record_skips([_skip()])
        time.sleep(0.05)  # ensure timestamps differ
        feature_status.record_skips([_skip()])
        rows = feature_status.current_status()
        assert len(rows) == 1
        assert rows[0]["count"] == 2
        # first_seen != last_seen after a repeat
        assert rows[0]["first_seen"] <= rows[0]["last_seen"]


class TestEmptyAndNoOpInputs:
    def test_record_empty_list_is_noop(self):
        feature_status.record_skips([])
        assert feature_status.current_status() == []

    def test_current_status_on_fresh_db_returns_empty(self):
        # No skips recorded yet — must not raise, returns [].
        assert feature_status.current_status() == []


class TestPruning:
    def test_old_entries_pruned_on_read(self, monkeypatch):
        """Entries older than the prune horizon must not appear in current_status."""
        feature_status.record_skips([_skip()])
        # Force last_seen far in the past via the test seam.
        very_old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        feature_status._set_last_seen_for_test(name="claude_amazon_lookup",
                                                where="claude_amazon_lookup",
                                                severity="warning",
                                                last_seen=very_old)
        rows = feature_status.current_status(prune_older_than_days=14)
        assert rows == [], rows

    def test_recent_entries_survive_prune(self):
        feature_status.record_skips([_skip()])
        rows = feature_status.current_status(prune_older_than_days=14)
        assert len(rows) == 1


class TestOrdering:
    def test_severity_then_recency_order(self):
        # Insert INFO first, then a WARNING (more severe), then BLOCKER (most).
        feature_status.record_skips([
            _skip(name="info_one", where="x", severity=Severity.INFO),
        ])
        time.sleep(0.05)
        feature_status.record_skips([
            _skip(name="warn_one", where="x", severity=Severity.WARNING),
        ])
        time.sleep(0.05)
        feature_status.record_skips([
            _skip(name="block_one", where="x", severity=Severity.BLOCKER),
        ])
        rows = feature_status.current_status()
        # BLOCKER first, then WARNING, then INFO — most-severe-first so the
        # banner draws the operator's eye to the worst feature outage.
        severities = [r["severity"] for r in rows]
        assert severities == ["blocker", "warning", "info"], severities


class TestRowShape:
    def test_returned_row_has_expected_keys(self):
        feature_status.record_skips([_skip()])
        rows = feature_status.current_status()
        assert len(rows) == 1
        r = rows[0]
        for key in ("name", "where", "severity", "reason", "count",
                    "first_seen", "last_seen"):
            assert key in r, f"missing key {key!r} in {r}"

    def test_reason_reflects_most_recent(self):
        """When repeat skips have different reasons (DB error message
        varies), the stored reason should be the most recent — operators
        want to see the freshest failure message."""
        feature_status.record_skips([_skip(reason="first failure mode")])
        feature_status.record_skips([_skip(reason="second failure mode")])
        rows = feature_status.current_status()
        assert len(rows) == 1
        assert rows[0]["reason"] == "second failure mode"
        assert rows[0]["count"] == 2
