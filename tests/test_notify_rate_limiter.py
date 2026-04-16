"""Unit tests for the notification rate-limiter / snooze helpers.

Covers the fix for the "⏰ N Drafts Waiting for Review" spam: the same
notification used to post 12+ times per day. Now bounded to once per
calendar day with an explicit snooze action.
"""

import pytest

from src.agents import notify_agent


@pytest.fixture(autouse=True)
def reset_cooldowns():
    notify_agent._reset_cooldowns_for_test()
    yield
    notify_agent._reset_cooldowns_for_test()


class FakeClock:
    def __init__(self, start: float = 1_700_000_000.0):
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float):
        self.now += float(seconds)


class TestCooldownTTL:
    def test_default_cooldown_blocks_duplicate(self):
        clk = FakeClock()
        # First call — fires.
        assert notify_agent._is_cooled_down("k1", _now_fn=clk) is True
        # Second call within 15 min default — suppressed.
        clk.advance(60)
        assert notify_agent._is_cooled_down("k1", _now_fn=clk) is False

    def test_default_cooldown_clears_after_window(self):
        clk = FakeClock()
        assert notify_agent._is_cooled_down("k1", _now_fn=clk) is True
        clk.advance(notify_agent.COOLDOWN_MIN * 60 + 1)
        assert notify_agent._is_cooled_down("k1", _now_fn=clk) is True

    def test_24h_ttl_blocks_for_full_day(self):
        clk = FakeClock()
        assert notify_agent._is_cooled_down("stale", ttl_seconds=86400, _now_fn=clk) is True
        # Same key 12h later — still blocked.
        clk.advance(12 * 3600)
        assert notify_agent._is_cooled_down("stale", ttl_seconds=86400, _now_fn=clk) is False
        # 23h59m later — still blocked.
        clk.advance(11 * 3600 + 59 * 60)
        assert notify_agent._is_cooled_down("stale", ttl_seconds=86400, _now_fn=clk) is False
        # Just past 24h — fires again.
        clk.advance(120)
        assert notify_agent._is_cooled_down("stale", ttl_seconds=86400, _now_fn=clk) is True


class TestSnoozeAlert:
    def test_snooze_blocks_until_expiry(self):
        clk = FakeClock()
        # Initial fire.
        assert notify_agent._is_cooled_down("k", ttl_seconds=86400, _now_fn=clk) is True
        # Snooze 24h.
        notify_agent.snooze_alert("k", hours=24, _now_fn=clk)
        # 1h in — blocked.
        clk.advance(3600)
        assert notify_agent._is_cooled_down("k", ttl_seconds=86400, _now_fn=clk) is False
        # 23h in — still blocked.
        clk.advance(22 * 3600)
        assert notify_agent._is_cooled_down("k", ttl_seconds=86400, _now_fn=clk) is False
        # Past 24h — fires.
        clk.advance(2 * 3600)
        assert notify_agent._is_cooled_down("k", ttl_seconds=86400, _now_fn=clk) is True

    def test_snooze_zero_hours_clears_immediately(self):
        clk = FakeClock()
        notify_agent.snooze_alert("k", hours=0, _now_fn=clk)
        # Snoozed by 0 = effectively cleared, next call fires.
        assert notify_agent._is_cooled_down("k", _now_fn=clk) is True


class TestSnoozeEndpoint:
    def test_snooze_endpoint_exists(self, client):
        r = client.post("/api/notify/snooze",
                        json={"key": "outbox_stale_drafts_waiting", "hours": 24})
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["key"] == "outbox_stale_drafts_waiting"

    def test_snooze_endpoint_requires_key(self, client):
        r = client.post("/api/notify/snooze", json={"hours": 24})
        assert r.status_code == 400
        body = r.get_json()
        assert body["ok"] is False
