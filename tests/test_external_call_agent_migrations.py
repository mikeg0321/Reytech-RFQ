"""Tier 1d PR-2 — agent-side migrations to `with_retry()` (audit 2026-05-07).

Two of the three agent-side ad-hoc retry implementations migrate cleanly
in this PR:

  src/agents/scprs_browser.py::_scrape_with_retry  (3x linear 10s, any Exception)
  src/agents/scprs_lookup.py::SCPRSLookup._load_page (3x linear 0.5s, content predicate)

The third — `src/agents/product_validator.py` inline Grok 429-retry —
has 4 distinct retry paths (status==429, HTTPError 5xx, Timeout,
CircuitOpenError) with status-code branching and 5xx/Timeout retries
that don't sleep. A "thin wrapper" migration would silently change
those semantics, so it's deferred to a follow-on PR with explicit
rework rather than smashed into a wrapper here.

These tests pin the migrated behavior against the pre-migration shape:
backoff progression, retry conditions, exhaustion handling.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest


# ── scprs_browser._scrape_with_retry ────────────────────────────────

def test_scprs_browser_scrape_succeeds_on_first_try():
    """Clean path — no retry, no sleep, returns whatever inner returns."""
    from src.agents import scprs_browser

    sentinel = [{"row": 1}]
    fake_loop = MagicMock()
    fake_loop.run_until_complete = MagicMock(return_value=sentinel)

    with patch.object(scprs_browser.asyncio, "new_event_loop",
                      return_value=fake_loop):
        with patch.object(scprs_browser.asyncio, "set_event_loop"):
            with patch("src.core.external_call.time.sleep") as mock_sleep:
                out = scprs_browser._scrape_with_retry(
                    search_params={}, seen_pos=set(), max_rows=10,
                )
    assert out is sentinel
    assert mock_sleep.call_count == 0
    fake_loop.close.assert_called_once()


def test_scprs_browser_scrape_retries_then_succeeds_with_linear_backoff():
    """3rd attempt succeeds; backoff stays linear 10s, 20s."""
    from src.agents import scprs_browser

    calls = {"n": 0}
    sentinel = [{"row": 1}]

    def _flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("playwright timeout")
        return sentinel

    fake_loop = MagicMock()
    fake_loop.run_until_complete = MagicMock(side_effect=lambda *a, **kw: _flaky())

    with patch.object(scprs_browser.asyncio, "new_event_loop",
                      return_value=fake_loop):
        with patch.object(scprs_browser.asyncio, "set_event_loop"):
            with patch("src.core.external_call.time.sleep") as mock_sleep:
                out = scprs_browser._scrape_with_retry(
                    search_params={}, seen_pos=set(), max_rows=10,
                )
    assert out is sentinel
    assert calls["n"] == 3
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [10.0, 20.0]  # base_delay * (i+1) for i=0,1
    assert fake_loop.close.call_count == 3  # close runs in finally each attempt


def test_scprs_browser_scrape_exhausts_and_raises():
    """All attempts fail → original exception bubbles up."""
    from src.agents import scprs_browser

    fake_loop = MagicMock()
    fake_loop.run_until_complete = MagicMock(
        side_effect=RuntimeError("permanent")
    )

    with patch.object(scprs_browser.asyncio, "new_event_loop",
                      return_value=fake_loop):
        with patch.object(scprs_browser.asyncio, "set_event_loop"):
            with patch("src.core.external_call.time.sleep"):
                with pytest.raises(RuntimeError, match="permanent"):
                    scprs_browser._scrape_with_retry(
                        search_params={}, seen_pos=set(), max_rows=10,
                        max_retries=3,
                    )


def test_scprs_browser_scrape_respects_max_retries_arg():
    """max_retries=5 → 5 attempts total before raising."""
    from src.agents import scprs_browser

    calls = {"n": 0}
    fake_loop = MagicMock()

    def _always_fail():
        calls["n"] += 1
        raise RuntimeError("permanent")
    fake_loop.run_until_complete = MagicMock(side_effect=lambda *a, **kw: _always_fail())

    with patch.object(scprs_browser.asyncio, "new_event_loop",
                      return_value=fake_loop):
        with patch.object(scprs_browser.asyncio, "set_event_loop"):
            with patch("src.core.external_call.time.sleep"):
                with pytest.raises(RuntimeError):
                    scprs_browser._scrape_with_retry(
                        search_params={}, seen_pos=set(), max_rows=10,
                        max_retries=5,
                    )
    assert calls["n"] == 5


# ── scprs_lookup.SCPRSLookup._load_page ─────────────────────────────

def _make_lookup():
    """Build a SCPRSLookup with a mocked session for `_load_page` tests."""
    from src.agents.scprs_lookup import FiscalSession
    obj = FiscalSession.__new__(FiscalSession)  # bypass __init__
    obj.session = MagicMock()
    obj._last_state_num = None
    obj.detail_session = None
    obj._detail_icsid = None
    return obj


def _resp(text, status=200):
    r = MagicMock()
    r.text = text
    r.status_code = status
    return r


def test_scprs_lookup_load_page_succeeds_on_first_try_with_marker():
    """First response has the marker → return immediately, no retry."""
    obj = _make_lookup()
    obj.session.get = MagicMock(return_value=_resp("...ZZ_SCPRS..."))

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        page = obj._load_page()
    assert "ZZ_SCPRS" in page
    assert obj.session.get.call_count == 1
    assert mock_sleep.call_count == 0


def test_scprs_lookup_load_page_retries_until_marker_found():
    """Two missing-marker responses, then a good one — linear backoff 0.5/1.0."""
    obj = _make_lookup()
    responses = [
        _resp("loading..."),
        _resp("still loading..."),
        _resp("here is ICSID=abc123"),
    ]
    obj.session.get = MagicMock(side_effect=responses)

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        page = obj._load_page(max_attempts=3)
    assert "ICSID" in page
    assert obj.session.get.call_count == 3
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [0.5, 1.0]


def test_scprs_lookup_load_page_returns_last_page_when_marker_never_appears():
    """All attempts missed marker → return last fetched page, do not raise.

    This is the hard parity check: the original loop also returned the
    last page rather than raising on exhaustion. Downstream parsers
    tolerate the empty/malformed body and surface a meaningful error
    later. We MUST preserve that contract.
    """
    obj = _make_lookup()
    last_body = "still no markers anywhere"
    obj.session.get = MagicMock(return_value=_resp(last_body))

    with patch("src.core.external_call.time.sleep"):
        page = obj._load_page(max_attempts=3)
    assert page == last_body  # not raised, returned the last body


def test_scprs_lookup_load_page_propagates_http_errors():
    """A network error during the GET propagates as before — no retry.

    The pre-migration loop also did not catch exceptions from
    self.session.get; only the missing-marker condition triggered retry.
    """
    obj = _make_lookup()
    obj.session.get = MagicMock(
        side_effect=ConnectionError("network unreachable")
    )

    with patch("src.core.external_call.time.sleep") as mock_sleep:
        with pytest.raises(ConnectionError, match="network unreachable"):
            obj._load_page(max_attempts=3)
    # is_transient=lambda e: isinstance(e, _ContentMissing), so
    # ConnectionError is NOT transient → no retry, no sleep.
    assert obj.session.get.call_count == 1
    assert mock_sleep.call_count == 0


def test_scprs_lookup_load_page_logs_each_attempt():
    """Per-attempt info log is preserved — observability parity."""
    obj = _make_lookup()
    obj.session.get = MagicMock(side_effect=[
        _resp("loading...", status=200),
        _resp("ZZ_SCPRS here", status=200),
    ])

    with patch("src.core.external_call.time.sleep"):
        with patch("src.agents.scprs_lookup.log") as mock_log:
            obj._load_page(max_attempts=3)
    # The success-case INFO log is still emitted on each attempt
    info_calls = [c for c in mock_log.info.call_args_list
                  if "SCPRS load" in (c.args[0] if c.args else "")]
    assert len(info_calls) == 2
