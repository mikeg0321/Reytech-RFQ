"""Canonical retry substrate for external calls.

Tier 1d (audit 2026-05-07). Five ad-hoc retry implementations across
the codebase do nearly the same thing with subtly different shapes:

  src/core/gmail_api.py:175      _with_gmail_retry   3x, 0.5s exp, transient-str predicate
  src/core/db.py:200             db_retry            3x, 1.0s linear, "database is locked"
  src/agents/product_validator.py:250  inline Grok    Nx, 2.0s linear, status==429
  src/agents/scprs_browser.py:955  _scrape_with_retry 3x, 10.0s linear, any Exception
  src/agents/scprs_lookup.py:203  _load_page         3x, 0.5s linear, content predicate

This module's `with_retry()` covers all five shapes. PR-1 migrates the
gmail and db helpers to call into it (preserving behavior). The three
agent-side helpers stay as-is for now and migrate in follow-on PRs.

Design choices:
  * Keep the function shape `with_retry(fn, *, op=...)` so the gmail
    call sites need a one-line change (just import path + arg order).
  * `is_transient=None` means "retry on any Exception" — matches the
    scprs_browser default. Callers usually pass a narrower predicate.
  * `on_final_failure` runs *before* the exception re-raises, so the
    db helper's Slack alert can fire without changing the exception
    contract.
  * No global mutable state. The whole module is a single function +
    helper. No factory classes, no decorator magic, no tenacity dep.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

_DEFAULT_LOG = logging.getLogger(__name__)


def with_retry(
    fn: Callable[[], T],
    *,
    op: str,
    attempts: int = 3,
    base_delay: float = 0.5,
    backoff: str = "exponential",
    is_transient: Optional[Callable[[BaseException], bool]] = None,
    on_final_failure: Optional[Callable[[BaseException, int], None]] = None,
    logger: Optional[logging.Logger] = None,
) -> T:
    """Call `fn()` with retry on transient errors.

    Args:
      fn: zero-arg callable; the work to retry.
      op: human-readable op name; used in log messages.
      attempts: total attempts including the first try (>=1).
      base_delay: seconds before retry #1. Subsequent waits scale via `backoff`.
      backoff: "exponential" → base * 2^i; "linear" → base * (i+1).
      is_transient: predicate returning True iff an exception should be
                    retried. None means "retry any Exception" (matches the
                    scprs_browser default). Returning False on the first
                    call lets non-transient errors raise immediately.
      on_final_failure: optional hook fired with `(exc, attempts)` after
                        the last attempt fails — runs *before* the raise,
                        so callers like the db helper can fire Slack
                        without changing the exception contract.
      logger: logger to use; defaults to this module's logger.

    Returns: whatever `fn()` returns on success.

    Raises: the last caught exception if all attempts are exhausted, or
            the first non-transient exception encountered.

    Examples:
      # gmail-style: transient predicate + exp backoff
      with_retry(req.execute, op="list_message_ids",
                 is_transient=_is_transient_gmail_error)

      # db-style: locked-only + linear backoff + Slack on final
      with_retry(work, op="db_save", base_delay=1.0, backoff="linear",
                 is_transient=lambda e: "database is locked" in str(e),
                 on_final_failure=_fire_db_lock_alert)
    """
    if attempts < 1:
        raise ValueError(f"attempts must be >= 1, got {attempts}")
    if backoff not in ("exponential", "linear"):
        raise ValueError(f"backoff must be 'exponential' or 'linear', got {backoff!r}")

    log = logger or _DEFAULT_LOG
    last_err: Optional[BaseException] = None

    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last_err = e
            # Non-transient → raise immediately, no retry, no final-hook.
            if is_transient is not None and not is_transient(e):
                raise
            # Last attempt → break out and let the final-hook + raise run.
            if i == attempts - 1:
                break
            delay = (
                base_delay * (2 ** i) if backoff == "exponential"
                else base_delay * (i + 1)
            )
            log.warning(
                "%s transient error (attempt %d/%d): %s — retry in %.1fs",
                op, i + 1, attempts, e, delay,
            )
            time.sleep(delay)

    # Exhausted. Fire final hook (if any), log, re-raise.
    assert last_err is not None  # for type-checkers; loop ran ≥1 iteration
    if on_final_failure is not None:
        try:
            on_final_failure(last_err, attempts)
        except Exception as hook_err:
            log.debug("on_final_failure hook raised: %s", hook_err)
    log.error("%s failed after %d attempts: %s", op, attempts, last_err)
    raise last_err
