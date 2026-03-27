"""
tracing.py — Distributed Request Tracing

Assigns a unique trace_id to each request/operation so related log entries
can be correlated across the email_poller → enrichment → quote_generator chain.

Usage:
    from src.core.tracing import get_trace_id, set_trace_id, trace_context

    # In Flask before_request:
    set_trace_id()  # Auto-generates UUID

    # In background threads:
    with trace_context("email-poll-cycle"):
        process_emails()  # All logs within get the same trace_id

    # In logging:
    log.info("Processing", extra={"trace_id": get_trace_id()})
"""

import logging
import uuid
from contextvars import ContextVar
from contextlib import contextmanager

log = logging.getLogger("reytech.tracing")

# Context variable — inherits across async calls within same context
_trace_id: ContextVar[str] = ContextVar("trace_id", default="")
_trace_op: ContextVar[str] = ContextVar("trace_op", default="")


def get_trace_id() -> str:
    """Get current trace ID (empty string if not set)."""
    return _trace_id.get("")


def get_trace_op() -> str:
    """Get current trace operation name."""
    return _trace_op.get("")


def set_trace_id(trace_id: str = None, operation: str = ""):
    """Set trace ID for current context. Auto-generates if not provided."""
    tid = trace_id or uuid.uuid4().hex[:12]
    _trace_id.set(tid)
    if operation:
        _trace_op.set(operation)
    return tid


@contextmanager
def trace_context(operation: str, trace_id: str = None):
    """Context manager for tracing a named operation.

    Usage:
        with trace_context("email-poll"):
            # All log entries within this block share the same trace_id
            process_emails()
    """
    prev_id = _trace_id.get("")
    prev_op = _trace_op.get("")
    tid = set_trace_id(trace_id, operation)
    try:
        yield tid
    finally:
        _trace_id.set(prev_id)
        _trace_op.set(prev_op)


class TracingFilter(logging.Filter):
    """Logging filter that injects trace_id and trace_op into every log record.

    Add to any handler to automatically include trace context:
        handler.addFilter(TracingFilter())

    Then in formatters:
        %(trace_id)s  %(trace_op)s
    """

    def filter(self, record):
        record.trace_id = _trace_id.get("")
        record.trace_op = _trace_op.get("")
        return True


def install_tracing():
    """Install tracing filter on the root logger so all loggers inherit it.
    Call once at app startup."""
    root = logging.getLogger()
    # Add filter to all existing handlers
    tf = TracingFilter()
    for handler in root.handlers:
        handler.addFilter(tf)
    # Also add to root logger so new handlers get it
    root.addFilter(tf)
    log.info("Request tracing installed")
