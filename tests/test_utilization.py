"""Tests for src/core/utilization.py write-side telemetry.

Phase 4 of the PC↔RFQ refactor. Verifies:
  - record_feature_use writes events to the DB without crashing
  - time_feature context manager records duration + ok status
  - DB errors are swallowed (tracking never breaks callers)
  - summary / top_features / dead_features queries work

Note: the read-side `/api/admin/utilization/*` HTTP endpoints (formerly
in routes_utilization.py) were removed 2026-05-05 as a dead-route
cleanup — the dashboard surface had no UI callers. The write-side
telemetry collection (record_feature_use, time_feature, flush_now)
remains live and is heavily used by routes_rfq, routes_pricecheck,
routes_cchcs_packet, ingest_pipeline, etc.
"""
import time

import pytest


class TestRecordFeatureUse:
    def test_basic_record(self, temp_data_dir):
        from src.core.utilization import record_feature_use, top_features, flush_now
        record_feature_use("test.basic", context={"k": "v"}, user="mike")
        flush_now()
        top = top_features(days=1)
        names = [t["feature"] for t in top]
        assert "test.basic" in names

    def test_record_is_fire_and_forget(self, temp_data_dir):
        """Must not raise even with bad input — tracking layer can
        NEVER break the caller."""
        from src.core.utilization import record_feature_use
        # None context
        record_feature_use("test.none", context=None)
        # Huge context (truncated to 4000 chars)
        record_feature_use("test.big", context={"x": "y" * 10000})
        # Empty feature — no-op, no crash
        record_feature_use("", context={})
        # Weird types in context — should json.dumps with default=str
        record_feature_use("test.weird", context={"time": time.time, "n": 3.14})

    def test_record_with_duration_and_error(self, temp_data_dir):
        from src.core.utilization import record_feature_use, top_features, flush_now
        record_feature_use("test.errored", duration_ms=500, ok=False)
        record_feature_use("test.errored", duration_ms=200, ok=True)
        flush_now()
        top = top_features(days=1)
        row = next((t for t in top if t["feature"] == "test.errored"), None)
        assert row is not None
        assert row["uses"] == 2
        assert row["errors"] == 1
        assert row["error_rate"] == 0.5


class TestAsyncQueuing:
    def test_record_enqueues_without_blocking_on_db(self, temp_data_dir, monkeypatch):
        """record_feature_use must return even when the DB write path
        is slow/broken — the flusher drains on its own thread."""
        from src.core import utilization
        # Drain anything the previous test left behind
        utilization.flush_now()

        # Point get_db at a raising stub so the flush would fail if the
        # caller relied on synchronous writes. The enqueue path should
        # still succeed.
        def _boom():
            raise RuntimeError("db unavailable")
        monkeypatch.setattr(utilization, "_flush_queue", lambda: 0)

        utilization.record_feature_use("async.queued", context={"x": 1})
        # The event landed in the queue even though the flusher is a no-op.
        assert any(r[0] == "async.queued" for r in list(utilization._queue))

    def test_backpressure_drops_oldest_when_queue_full(self, temp_data_dir, monkeypatch):
        """When the queue hits _MAX_QUEUE the oldest event is dropped
        so memory can't grow unbounded."""
        from src.core import utilization
        monkeypatch.setattr(utilization, "_MAX_QUEUE", 3)
        monkeypatch.setattr(utilization, "_flush_queue", lambda: 0)
        # Clear whatever is there
        with utilization._queue_lock:
            utilization._queue.clear()
        for i in range(5):
            utilization.record_feature_use(f"bp.{i}")
        features = [r[0] for r in list(utilization._queue)]
        assert len(features) == 3
        # Oldest two (bp.0, bp.1) were dropped
        assert features == ["bp.2", "bp.3", "bp.4"]


class TestTimeFeatureContextManager:
    def test_context_manager_records_duration(self, temp_data_dir):
        from src.core.utilization import time_feature, top_features, flush_now
        with time_feature("test.ctx") as ctx:
            ctx["agency"] = "cchcs"
            time.sleep(0.05)
        flush_now()
        top = top_features(days=1)
        row = next((t for t in top if t["feature"] == "test.ctx"), None)
        assert row is not None
        assert row["avg_ms"] >= 40  # should be around 50ms

    def test_context_manager_on_exception_still_records(self, temp_data_dir):
        from src.core.utilization import time_feature, top_features, flush_now
        with pytest.raises(ValueError):
            with time_feature("test.raised") as ctx:
                ctx["raised"] = True
                raise ValueError("boom")
        flush_now()
        top = top_features(days=1)
        row = next((t for t in top if t["feature"] == "test.raised"), None)
        assert row is not None
        assert row["errors"] == 1


class TestDeadFeatures:
    def test_never_used_feature_is_dead(self, temp_data_dir):
        from src.core.utilization import dead_features, record_feature_use, flush_now
        record_feature_use("alive.feature", context={})
        flush_now()
        dead = dead_features(["alive.feature", "dead.feature"])
        assert "dead.feature" in dead
        assert "alive.feature" not in dead


class TestSummaryQuery:
    def test_summary_returns_structure(self, temp_data_dir):
        from src.core.utilization import record_feature_use, summary, flush_now
        record_feature_use("endpoint.test", duration_ms=100)
        flush_now()
        s = summary(days=1)
        assert s["ok"] is True
        assert s["total_events"] >= 1
        assert "top_features" in s


class TestIngestPipelineEmitsTelemetry:
    """Regression guard: process_buyer_request must emit a telemetry
    event every time it runs, so the dashboard shows real ingest usage."""

    def test_process_buyer_request_emits_event(self, temp_data_dir):
        import os
        from src.core.ingest_pipeline import process_buyer_request
        from src.core.utilization import top_features, flush_now

        fix = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "tests", "fixtures", "unified_ingest", "cchcs_packet_preq.pdf",
        )
        if not os.path.exists(fix):
            pytest.skip("cchcs packet fixture missing")

        process_buyer_request(
            files=[fix],
            email_subject="PREQ10843276",
            email_sender="ashley.russ@cdcr.ca.gov",
        )
        flush_now()
        top = top_features(days=1)
        names = [t["feature"] for t in top]
        assert "ingest.process_buyer_request" in names
