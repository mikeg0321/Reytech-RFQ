"""
Tests for the 2026-04-19 homepage tune-up: surface the quoting orchestrator
status on the home dashboard.

Background: /quoting/status (Mike's live orchestrator audit) was unlinked
from the homepage. Operators couldn't see it without typing the URL. This
batch wires:
  1. A nav button in the top bar ("📡 Quoting").
  2. A live pulse pill in the agent status card that hits
     /api/quoting/status every 30s and shows blocked vs advanced counts.

Both are pure HTML/JS injection — guards keep the markers in place so a
future template refactor doesn't silently drop them.
"""
from __future__ import annotations


class TestHomeQuotingPulse:
    def test_home_renders_quoting_nav(self, auth_client):
        resp = auth_client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        # Nav button to /quoting/status must exist in the top bar
        assert 'href="/quoting/status"' in body
        assert "📡 Quoting" in body

    def test_home_renders_quoting_pulse_pill(self, auth_client):
        resp = auth_client.get("/")
        body = resp.get_data(as_text=True)
        # Pulse pill DOM hooks
        assert 'id="as-quoting-dot"' in body
        assert 'id="as-quoting-txt"' in body
        # Refresh wiring
        assert "_refreshQuotingPulse" in body
        assert "/api/quoting/status" in body


class TestQuotingApiContractForPulse:
    """The pulse pill consumes outcome_counts. Lock the contract so a future
    /api/quoting/status reshape doesn't silently break the homepage pill."""

    def test_status_response_shape(self, auth_client):
        resp = auth_client.get("/api/quoting/status?limit=1")
        assert resp.status_code == 200
        j = resp.get_json()
        assert j.get("ok") is True
        # Pulse depends on these keys existing
        assert "outcome_counts" in j
        assert isinstance(j["outcome_counts"], dict)
        assert "rows" in j
