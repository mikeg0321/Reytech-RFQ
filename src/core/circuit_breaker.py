"""
circuit_breaker.py — Circuit Breaker for External API Calls

Prevents cascade failures when external APIs (SCPRS, Amazon, QuickBooks) go down.
When an API fails repeatedly, the breaker opens and returns fallback responses
instead of hammering a dead service.

Usage:
    from src.core.circuit_breaker import get_breaker

    breaker = get_breaker("scprs")
    result = breaker.call(lookup_price, item_number="ABC123")
    # If SCPRS is down (5+ failures in 60s), raises CircuitOpenError
    # After 60s cool-down, allows one test call (half-open state)

    # Or use the decorator:
    @circuit_protected("amazon", fallback=lambda *a, **kw: {"price": 0, "error": "circuit open"})
    def lookup_amazon_price(asin):
        ...
"""

import logging
import threading
import time
from datetime import datetime
from enum import Enum

log = logging.getLogger("reytech.circuit_breaker")


class State(Enum):
    CLOSED = "closed"        # Normal operation — calls go through
    OPEN = "open"            # Failing — reject calls immediately
    HALF_OPEN = "half_open"  # Testing recovery — allow one call


class CircuitOpenError(Exception):
    """Raised when circuit is open and call is rejected."""
    def __init__(self, name, failures, last_failure):
        self.name = name
        self.failures = failures
        self.last_failure = last_failure
        super().__init__(f"Circuit '{name}' is OPEN ({failures} failures, last: {last_failure})")


class CircuitBreaker:
    """Per-service circuit breaker with configurable thresholds."""

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: int = 60, success_threshold: int = 2):
        """
        Args:
            name: Service identifier (e.g. "scprs", "amazon")
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds to wait before half-open test
            success_threshold: Successes in half-open before closing
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = State.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._last_failure_error = ""
        self._lock = threading.Lock()

        # Stats
        self._total_calls = 0
        self._total_failures = 0
        self._total_rejected = 0
        self._opened_at = None

    @property
    def state(self) -> str:
        with self._lock:
            # Auto-transition from OPEN → HALF_OPEN after timeout
            if self._state == State.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = State.HALF_OPEN
                    log.info("Circuit '%s' → HALF_OPEN (testing recovery)", self.name)
            return self._state.value

    def call(self, fn, *args, **kwargs):
        """Execute fn through the circuit breaker.

        Raises CircuitOpenError if circuit is open.
        """
        with self._lock:
            self._total_calls += 1

            # Check state
            if self._state == State.OPEN:
                elapsed = time.time() - self._last_failure_time
                if elapsed < self.recovery_timeout:
                    self._total_rejected += 1
                    raise CircuitOpenError(self.name, self._failure_count, self._last_failure_error)
                # Transition to half-open
                self._state = State.HALF_OPEN
                self._success_count = 0
                log.info("Circuit '%s' → HALF_OPEN (testing recovery after %ds)", self.name, int(elapsed))

        # Execute the call (outside lock to avoid holding it during IO)
        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self):
        with self._lock:
            if self._state == State.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = State.CLOSED
                    self._failure_count = 0
                    self._opened_at = None
                    log.info("Circuit '%s' → CLOSED (recovered after %d successes)", self.name, self._success_count)
            else:
                # Reset failure count on success in closed state
                self._failure_count = 0

    def _on_failure(self, error):
        with self._lock:
            self._failure_count += 1
            self._total_failures += 1
            self._last_failure_time = time.time()
            self._last_failure_error = f"{type(error).__name__}: {str(error)[:100]}"

            if self._state == State.HALF_OPEN:
                # Failed during test — back to open
                self._state = State.OPEN
                self._opened_at = datetime.now().isoformat()
                log.warning("Circuit '%s' → OPEN (half-open test failed: %s)", self.name, error)
            elif self._failure_count >= self.failure_threshold:
                self._state = State.OPEN
                self._opened_at = datetime.now().isoformat()
                log.warning("Circuit '%s' → OPEN (%d failures: %s)", self.name, self._failure_count, error)

    def status(self) -> dict:
        """Return current circuit status for monitoring."""
        return {
            "name": self.name,
            "state": self.state,
            "failure_count": self._failure_count,
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "total_rejected": self._total_rejected,
            "last_failure": self._last_failure_error,
            "opened_at": self._opened_at,
            "config": {
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "success_threshold": self.success_threshold,
            },
        }


# ─── Global Registry ─────────────────────────────────────────────────────────

_breakers = {}
_registry_lock = threading.Lock()

# Default configs per service
_DEFAULTS = {
    "scprs":       {"failure_threshold": 3, "recovery_timeout": 120, "success_threshold": 1},
    "amazon":      {"failure_threshold": 5, "recovery_timeout": 60,  "success_threshold": 2},
    "quickbooks":  {"failure_threshold": 3, "recovery_timeout": 300, "success_threshold": 1},
    "web_search":  {"failure_threshold": 5, "recovery_timeout": 60,  "success_threshold": 2},
    "grok":        {"failure_threshold": 3, "recovery_timeout": 120, "success_threshold": 1},
    "gmail":       {"failure_threshold": 3, "recovery_timeout": 300, "success_threshold": 1},
}


def get_breaker(name: str) -> CircuitBreaker:
    """Get or create a circuit breaker for the named service."""
    with _registry_lock:
        if name not in _breakers:
            config = _DEFAULTS.get(name, {"failure_threshold": 5, "recovery_timeout": 60, "success_threshold": 2})
            _breakers[name] = CircuitBreaker(name, **config)
        return _breakers[name]


def all_status() -> list:
    """Return status of all registered circuit breakers."""
    with _registry_lock:
        return [b.status() for b in _breakers.values()]


def circuit_protected(service_name: str, fallback=None):
    """Decorator to protect a function with a circuit breaker.

    Args:
        service_name: Circuit breaker name
        fallback: Optional callable returning fallback value when circuit is open.
                  If None, CircuitOpenError propagates.

    Usage:
        @circuit_protected("scprs", fallback=lambda *a, **kw: {"price": 0})
        def lookup_scprs_price(item_number):
            ...
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            breaker = get_breaker(service_name)
            try:
                return breaker.call(fn, *args, **kwargs)
            except CircuitOpenError:
                if fallback is not None:
                    log.debug("Circuit '%s' open — using fallback for %s", service_name, fn.__name__)
                    return fallback(*args, **kwargs)
                raise
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper
    return decorator
