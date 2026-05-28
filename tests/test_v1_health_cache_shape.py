"""Tests for /api/v1/health response-envelope consistency.

Regression for 2026-05-28: the 60-second in-memory cache at
routes_v1.api_v1_health stored the inner result dict and returned it
raw on cache hits — but the cache-miss path wrapped via api_response()
into ``{ok: true, data: {...}}``. Two different shapes from the same
endpoint depending on cache state broke home.html's
``if(!d.ok||!d.data)return;`` guard, leaving the agent panel
"loading..." for 60s out of every 61. Fixed alongside PR #1180 home-
audit trio (PR #1180 was B3 typo fix; this is the upstream guard fix
that lets the typo-fix actually surface a green dot).
"""
import time


class TestV1HealthEnvelope:
    """Both cache-miss AND cache-hit paths must return the same shape."""

    def test_first_call_wraps_with_ok_and_data(self, auth_client):
        resp = auth_client.get("/api/v1/health")
        assert resp.status_code == 200
        body = resp.get_json()
        assert "ok" in body, f"missing ok envelope: {list(body.keys())}"
        assert "data" in body, f"missing data envelope: {list(body.keys())}"
        assert body["ok"] is True
        # Inner data has the real fields
        assert "agents" in body["data"]

    def test_cached_second_call_uses_same_envelope(self, auth_client):
        """The 60s cache used to return the inner dict directly. After
        the fix, the cache-hit path wraps too."""
        # Prime the cache
        first = auth_client.get("/api/v1/health").get_json()
        # Immediate second call — well within the 60s cache window
        second = auth_client.get("/api/v1/health").get_json()
        assert second.get("ok") is True, (
            f"cache-hit response missing 'ok' — got keys "
            f"{list(second.keys())}. Fix: cache-hit path must wrap "
            f"with {{'ok': True, 'data': cached_dict}}."
        )
        assert "data" in second, f"cache-hit response missing 'data': {list(second.keys())}"
        assert "agents" in second["data"]
        # Both responses describe the same underlying state
        assert set(first["data"].keys()) == set(second["data"].keys())

    def test_agents_key_present_in_data(self, auth_client):
        """home.html JS reads d.data.agents — that nesting must hold
        through both cache states."""
        resp = auth_client.get("/api/v1/health")
        body = resp.get_json()
        # Agents is dict, never list
        assert isinstance(body["data"]["agents"], dict)
