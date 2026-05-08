"""Daily-cap quota gate for Anthropic (Claude) callers.

Tier 3c (audit 2026-05-07). `src/core/api_quota.py` exists with
`can_call(service)` + `log_call(service, ...)` but only one caller
(`src/agents/item_identifier.py`) actually uses it — for Grok, not
Claude. ~14 Anthropic call sites in `src/agents/` + `src/forms/`
have NO daily $ cap, so a runaway agent (e.g. an infinite-retry
bug or a wedge in the buyer-reply diff loop) could burn unbounded
spend before the operator notices.

This module is the thin gate Anthropic callers should put around
`client.messages.create(...)` (SDK shape) or
`requests.post(api.anthropic.com/v1/messages)` (REST shape):

    from src.core.anthropic_quota import check_quota, log_call

    skip = check_quota(agent="form_profiler")
    if skip:
        return {}, skip   # surface to caller as a skip reason

    t0 = time.time()
    try:
        resp = client.messages.create(...)
        # Pull token usage from response.usage when available.
        usage = getattr(resp, "usage", None)
        log_call(agent="form_profiler",
                 tokens_in=getattr(usage, "input_tokens", 0),
                 tokens_out=getattr(usage, "output_tokens", 0),
                 response_time_ms=int((time.time() - t0) * 1000),
                 model=getattr(resp, "model", ""))
    except Exception as e:
        log_call(agent="form_profiler", error=str(e)[:100])
        raise

Both helpers fail-open on any exception — quota tracking must NEVER
break a real business path. The underlying `api_quota.can_call` is
already fail-open; this layer just sugars the import boilerplate
and pins "claude" as the service constant so call sites can't drift
to "Claude" / "claude-haiku" / etc.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)

_SERVICE = "claude"


def check_quota(agent: str = "") -> Optional[str]:
    """Pre-call gate. Returns a short skip-reason string when the
    daily $ cap is exhausted, else None.

    Caller pattern:
        skip = check_quota(agent="form_profiler")
        if skip:
            return _empty_result(skip)

    Fail-open: any tracking-layer error returns None (= proceed)
    so a transient SQLite blip never blocks revenue work.
    """
    try:
        from src.core.api_quota import api_quota
        if not api_quota.can_call(_SERVICE):
            log.warning(
                "Anthropic daily quota exceeded — skipping call from %s",
                agent or "<unknown>")
            return f"{_SERVICE}:daily_quota_exceeded"
    except Exception as e:
        log.debug("anthropic_quota.check_quota suppressed: %s", e)
    return None


def log_call(agent: str = "",
             tokens_in: int = 0,
             tokens_out: int = 0,
             response_time_ms: int = 0,
             error: str = "",
             model: str = "",
             pc_id: str = "") -> None:
    """Post-call telemetry. Records cost/usage row in `api_usage`.
    Fail-open — never raises, never blocks the calling path.

    Pass either `tokens_in`/`tokens_out` (preferred — drives accurate
    cost calc against per-million-token rates), or omit them (falls
    back to the flat `DEFAULT_COSTS["claude"]` per-call estimate).

    `error` is a short string when the API call failed (e.g.
    `"http_429"`, `"http_500"`, `"timeout"`) so the daily summary
    can break out error rate by reason.
    """
    try:
        from src.core.api_quota import api_quota
        api_quota.log_call(
            _SERVICE,
            agent=agent or "",
            pc_id=pc_id or "",
            tokens_in=tokens_in or 0,
            tokens_out=tokens_out or 0,
            response_time_ms=response_time_ms or 0,
            error=error or "",
            model=model or "",
        )
    except Exception as e:
        log.debug("anthropic_quota.log_call suppressed: %s", e)


def _extract_usage(resp) -> tuple[int, int, str]:
    """Helper: pull (tokens_in, tokens_out, model) from an Anthropic
    SDK response object. Returns zeros if the attributes don't
    exist (older SDK versions, mock objects, etc.).

    Handy at call sites so the telemetry one-liner stays readable:

        ti, to, m = _extract_usage(resp)
        log_call(agent="x", tokens_in=ti, tokens_out=to, model=m,
                 response_time_ms=elapsed)
    """
    usage = getattr(resp, "usage", None)
    if usage is None:
        return 0, 0, getattr(resp, "model", "") or ""
    return (
        int(getattr(usage, "input_tokens", 0) or 0),
        int(getattr(usage, "output_tokens", 0) or 0),
        getattr(resp, "model", "") or "",
    )
