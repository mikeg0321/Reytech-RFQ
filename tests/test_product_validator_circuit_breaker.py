"""Grok validator must trip the circuit breaker after repeated failures.

Before this guard: if xAI started returning 503s, every pending PC would
hammer the failing endpoint (retry loop × N pending items × however long
the outage lasts). No shared kill-switch, no back-pressure.

Fix: wrap the POST with `get_breaker("grok").call(...)`. Defaults for the
"grok" breaker live in src/core/circuit_breaker.py (threshold 3, recovery
120s, success 1). When the breaker opens, validate_product returns a
graceful `{"ok": False, "circuit_open": True}` so callers can skip ahead
instead of stalling behind a dead service.

We assert two behaviors:

  1. When the breaker is already OPEN, validate_product short-circuits
     WITHOUT calling requests.post — proving the breaker rejects before
     the retry loop runs.

  2. Three consecutive 5xx responses flip the breaker OPEN. The fourth
     validate_product call then returns circuit_open=True — even though
     requests.post is still stubbed to fail, confirming the breaker
     stopped the hammering.

The cache is cleared and the singleton breaker is reset between tests
so state doesn't leak.
"""
from __future__ import annotations

import os

import pytest


def _reset_grok_breaker():
    """Drop the singleton breaker so each test starts from CLOSED."""
    from src.core.circuit_breaker import _breakers, _registry_lock
    with _registry_lock:
        _breakers.pop("grok", None)


def _clear_grok_cache():
    import src.agents.product_validator as pv
    if os.path.exists(pv.CACHE_FILE):
        os.unlink(pv.CACHE_FILE)


@pytest.fixture(autouse=True)
def _isolate_breaker_and_cache(monkeypatch):
    _reset_grok_breaker()
    _clear_grok_cache()
    # Ensure api key check passes so we reach the HTTP path
    monkeypatch.setenv("XAI_API_KEY", "test_key")
    # Ensure the feature flag doesn't short-circuit us before the
    # breaker gets a chance.
    from src.core.flags import delete_flag
    delete_flag("pricing.grok_validator_enabled")
    yield
    _reset_grok_breaker()
    _clear_grok_cache()


class TestGrokCircuitBreakerPrevention:
    def test_open_circuit_short_circuits_without_network_call(
            self, monkeypatch):
        """Pre-open the breaker. validate_product must return the
        circuit_open response without calling requests.post."""
        from src.core.circuit_breaker import get_breaker, State
        from src.agents.product_validator import validate_product
        import src.agents.product_validator as pv

        # Force breaker to OPEN
        breaker = get_breaker("grok")
        breaker._state = State.OPEN
        breaker._failure_count = breaker.failure_threshold
        breaker._last_failure_time = 9999999999  # far-future so it stays OPEN

        def _explode(*a, **kw):
            raise AssertionError(
                "requests.post must not be called when circuit is OPEN — "
                "the breaker should reject before the HTTP layer.")
        monkeypatch.setattr(pv.requests, "post", _explode)

        result = validate_product(
            description="Anything at all",
            upc="012345678905",
        )
        assert result["ok"] is False
        assert result.get("circuit_open") is True, (
            f"expected circuit_open=True, got {result}")

    def test_three_consecutive_5xx_opens_circuit(self, monkeypatch):
        """After three 5xx failures (across validate_product calls), the
        breaker opens. The fourth call returns circuit_open=True without
        the HTTP stub even getting a chance to fire."""
        from src.agents.product_validator import validate_product
        import src.agents.product_validator as pv

        call_count = {"n": 0}

        class _FiveHundred:
            status_code = 503
            text = "service unavailable"

            def json(self):
                return {}

        def _always_503(*a, **kw):
            call_count["n"] += 1
            return _FiveHundred()

        monkeypatch.setattr(pv.requests, "post", _always_503)

        # The "grok" breaker config: failure_threshold=3. Each call runs
        # the retry loop (_MAX_RETRIES=2), so each validate_product
        # contributes up to 2 failures. Two calls = up to 4 failures,
        # which exceeds the threshold — breaker should be OPEN by then.
        #
        # Call 1: 2 failures (attempt 1 + 2) → fail_count=2, still CLOSED
        # Call 2: 1 failure on attempt 1 → fail_count=3 → OPEN.
        #         attempt 2 hits OPEN breaker → CircuitOpenError path →
        #         returns circuit_open=True early.
        result1 = validate_product(description="Item one", upc="")
        assert result1["ok"] is False, result1

        result2 = validate_product(description="Item two", upc="")
        # Either this call tripped the breaker itself (circuit_open=True)
        # or it burned through retries with 5xx errors. Either is fine —
        # what matters is that a THIRD call must see circuit_open=True.
        assert result2["ok"] is False, result2

        # Now the circuit is definitely open. Any further call must
        # short-circuit without calling requests.post again.
        before = call_count["n"]
        result3 = validate_product(description="Item three", upc="")
        after = call_count["n"]
        assert result3.get("circuit_open") is True, (
            f"third call should see OPEN breaker; got {result3}")
        assert after == before, (
            f"requests.post should not have been called again once breaker "
            f"opened (before={before}, after={after})")
