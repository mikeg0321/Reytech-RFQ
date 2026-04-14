"""Tests for src/core/utilization.py and /api/admin/utilization/*.

Phase 4 of the PC↔RFQ refactor. Verifies:
  - record_feature_use writes events to the DB without crashing
  - time_feature context manager records duration + ok status
  - DB errors are swallowed (tracking never breaks callers)
  - summary / top_features / dead_features queries work
  - /api/admin/utilization/* endpoints return usable data
"""
import time

import pytest


class TestRecordFeatureUse:
    def test_basic_record(self, temp_data_dir):
        from src.core.utilization import record_feature_use, top_features
        record_feature_use("test.basic", context={"k": "v"}, user="mike")
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
        from src.core.utilization import record_feature_use, top_features
        record_feature_use("test.errored", duration_ms=500, ok=False)
        record_feature_use("test.errored", duration_ms=200, ok=True)
        top = top_features(days=1)
        row = next((t for t in top if t["feature"] == "test.errored"), None)
        assert row is not None
        assert row["uses"] == 2
        assert row["errors"] == 1
        assert row["error_rate"] == 0.5


class TestTimeFeatureContextManager:
    def test_context_manager_records_duration(self, temp_data_dir):
        from src.core.utilization import time_feature, top_features
        with time_feature("test.ctx") as ctx:
            ctx["agency"] = "cchcs"
            time.sleep(0.05)
        top = top_features(days=1)
        row = next((t for t in top if t["feature"] == "test.ctx"), None)
        assert row is not None
        assert row["avg_ms"] >= 40  # should be around 50ms

    def test_context_manager_on_exception_still_records(self, temp_data_dir):
        from src.core.utilization import time_feature, top_features
        with pytest.raises(ValueError):
            with time_feature("test.raised") as ctx:
                ctx["raised"] = True
                raise ValueError("boom")
        top = top_features(days=1)
        row = next((t for t in top if t["feature"] == "test.raised"), None)
        assert row is not None
        assert row["errors"] == 1


class TestDeadFeatures:
    def test_never_used_feature_is_dead(self, temp_data_dir):
        from src.core.utilization import dead_features, record_feature_use
        record_feature_use("alive.feature", context={})
        dead = dead_features(["alive.feature", "dead.feature"])
        assert "dead.feature" in dead
        assert "alive.feature" not in dead


class TestSummaryEndpoint:
    def test_summary_returns_structure(self, temp_data_dir):
        from src.core.utilization import record_feature_use, summary
        record_feature_use("endpoint.test", duration_ms=100)
        s = summary(days=1)
        assert s["ok"] is True
        assert s["total_events"] >= 1
        assert "top_features" in s

    def test_summary_endpoint_route(self, client, temp_data_dir):
        """GET /api/admin/utilization/summary returns valid JSON."""
        from src.core.utilization import record_feature_use
        record_feature_use("route.test", duration_ms=50)
        r = client.get("/api/admin/utilization/summary?days=1")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert "top_features" in d

    def test_top_endpoint_route(self, client, temp_data_dir):
        from src.core.utilization import record_feature_use
        record_feature_use("toproute.test")
        r = client.get("/api/admin/utilization/top?days=1&limit=5")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert isinstance(d["top"], list)

    def test_dead_endpoint_route(self, client, temp_data_dir):
        r = client.get("/api/admin/utilization/dead?days=30")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert "dead" in d
        assert "tracked" in d

    def test_feature_series_endpoint(self, client, temp_data_dir):
        from src.core.utilization import record_feature_use
        record_feature_use("series.test", duration_ms=10)
        r = client.get("/api/admin/utilization/feature/series.test?days=1")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["feature"] == "series.test"


class TestIngestPipelineEmitsTelemetry:
    """Regression guard: process_buyer_request must emit a telemetry
    event every time it runs, so the dashboard shows real ingest usage."""

    def test_process_buyer_request_emits_event(self, temp_data_dir):
        import os
        from src.core.ingest_pipeline import process_buyer_request
        from src.core.utilization import top_features

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
        top = top_features(days=1)
        names = [t["feature"] for t in top]
        assert "ingest.process_buyer_request" in names
