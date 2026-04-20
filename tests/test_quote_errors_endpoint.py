"""Regression tests for /api/health/quote-errors.

Guards:
  - Trace(...).fail(...) persists a row to utilization_events with ok=0.
  - Trace(...).ok(...) persists with ok=1.
  - /api/health/quote-errors returns only quote-workflow traces and
    filters by hours window.
  - Recent failures unpack trace_id / rfq_id / pc_id / error from the
    context blob so ops don't have to re-parse JSON themselves.
"""
from __future__ import annotations

import time


def _flush():
    """Drain the utilization flusher so SELECTs see our writes."""
    from src.core.utilization import flush_now
    flush_now()


class TestTracePersistence:
    def test_failed_trace_writes_utilization_event(self, auth_client):
        from src.api.trace import Trace
        t = Trace("rfq_package", rfq_id="rfq_test_abc")
        t.step("parsing")
        t.fail("bid package exploded", error="KeyError: 'Annots'")
        _flush()

        r = auth_client.get("/api/health/quote-errors?hours=1").get_json()
        assert r["ok"] is True
        assert r["summary"]["failures"] >= 1
        matching = [x for x in r["recent"] if x["trace_id"] == t.id]
        assert len(matching) == 1
        m = matching[0]
        assert m["feature"] == "trace.rfq_package"
        assert m["rfq_id"] == "rfq_test_abc"
        assert "bid package exploded" in m["error"]
        assert m["status"] == "fail"

    def test_successful_trace_not_in_failures(self, auth_client):
        from src.api.trace import Trace
        t = Trace("rfq_package", rfq_id="rfq_test_ok")
        t.ok("done", files=4)
        _flush()

        r = auth_client.get("/api/health/quote-errors?hours=1").get_json()
        # Successful traces are counted in total_attempts but NOT in recent
        trace_ids = {x["trace_id"] for x in r["recent"]}
        assert t.id not in trace_ids

    def test_non_quote_workflows_excluded(self, auth_client):
        from src.api.trace import Trace
        # Email pipeline is tracked elsewhere — must not pollute this signal.
        t = Trace("email_pipeline", subject="Quote — blah")
        t.fail("gmail timeout")
        _flush()

        r = auth_client.get("/api/health/quote-errors?hours=1").get_json()
        features = {x["feature"] for x in r["recent"]}
        assert "trace.email_pipeline" not in features


class TestQuoteErrorsEndpoint:
    def test_response_shape(self, auth_client):
        r = auth_client.get("/api/health/quote-errors")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["hours"] == 24
        assert "summary" in d
        for k in ("total_attempts", "failures", "failure_rate"):
            assert k in d["summary"]
        assert "by_workflow" in d
        assert "recent" in d

    def test_hours_param_clamped(self, auth_client):
        r = auth_client.get("/api/health/quote-errors?hours=9999")
        assert r.status_code == 200
        assert r.get_json()["hours"] == 720

        r = auth_client.get("/api/health/quote-errors?hours=abc")
        assert r.status_code == 200
        assert r.get_json()["hours"] == 24

    def test_by_workflow_aggregates_failures(self, auth_client):
        from src.api.trace import Trace
        for _ in range(3):
            Trace("rfq_package", rfq_id="rfq_agg").fail("boom")
        Trace("rfq_package", rfq_id="rfq_agg").ok("done")
        _flush()

        d = auth_client.get("/api/health/quote-errors?hours=1").get_json()
        rfq_pkg = next(
            (r for r in d["by_workflow"] if r["feature"] == "trace.rfq_package"),
            None,
        )
        assert rfq_pkg is not None
        assert rfq_pkg["failures"] >= 3
        assert rfq_pkg["attempts"] >= rfq_pkg["failures"]
