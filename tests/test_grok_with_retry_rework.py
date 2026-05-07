"""Tier 1d PR-3 — Grok retry rework (audit 2026-05-07).

The pre-migration loop in `src/agents/product_validator.py:validate_product`
had FOUR distinct retry paths with different sleep semantics:

  - status==429          → sleep(2 * (attempt+1)), continue
  - HTTPError 5xx        → no sleep, continue
  - Timeout              → no sleep, continue
  - CircuitOpenError     → return {"ok": False, "circuit_open": True}

Plus two terminal returns:
  - status not 200/429   → return {"ok": False, "error": f"API {code}"}
  - generic Exception    → return {"ok": False, "error": str(e)}

The migration to `src.core.external_call.with_retry`:
  * Translates 429, 5xx, and Timeout to private sentinel exceptions
    (`_GrokRateLimited`, `_GrokTransport5xx`, `_GrokTimeoutError`).
  * is_transient retries all three uniformly with linear 2s backoff.
  * CircuitOpenError + the sentinels-on-exhaustion + generic Exception
    are caught at the outer layer to preserve the error-dict contract.

Behavior change pinned by these tests: 5xx and Timeout retries now
sleep between attempts (previously didn't). Mike approved this change
explicitly as part of Tier 1d PR-3.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
import requests

from src.agents import product_validator as pv


# ── Test harness ────────────────────────────────────────────────────

def _stub_breaker(post_xai_side_effect):
    """Patch get_breaker so breaker.call(_post_xai, headers, payload)
    invokes post_xai_side_effect(headers, payload). Returns a MagicMock.
    """
    fake_breaker = MagicMock()

    def _call(fn, headers, payload):
        if callable(post_xai_side_effect):
            return post_xai_side_effect(headers, payload)
        if isinstance(post_xai_side_effect, list):
            v = post_xai_side_effect.pop(0)
            if isinstance(v, BaseException) or (isinstance(v, type) and issubclass(v, BaseException)):
                raise v
            return v
        return post_xai_side_effect

    fake_breaker.call = MagicMock(side_effect=_call)
    return fake_breaker


def _resp(status, body=None):
    r = MagicMock()
    r.status_code = status
    if body is not None:
        r.json = MagicMock(return_value=body)
        r.text = str(body)
    else:
        r.json = MagicMock(return_value={})
        r.text = ""
    return r


def _good_grok_body():
    """Build a successful Grok response body matching the parser's expectations."""
    import json as _json
    payload = {
        "is_correct_match": True,
        "product_name": "Widget",
        "url": "https://www.amazon.com/dp/B01ABCDE12",
        "price": 12.34,
        "asin": "B01ABCDE12",
        "supplier": "Amazon",
        "confidence": 0.85,
        "reasoning": "matched",
    }
    return {"choices": [{"message": {"content": _json.dumps(payload)}}],
            "usage": {"total_tokens": 42}}


def _validate_kwargs():
    """Common kwargs for `validate_product` calls in tests."""
    return dict(description="Widget", upc="", mfg_number="", qty=1, uom="EA",
                qty_per_uom=1, best_match_title="", best_match_price=0,
                best_match_confidence=0, best_match_source="")


@pytest.fixture(autouse=True)
def _bypass_cache_and_flag():
    """Force every call to hit the API path (no cache, flag on)."""
    with patch.object(pv, "_cache_lookup", return_value=None):
        with patch.object(pv, "_cache_store", return_value=None):
            with patch.object(pv, "_get_api_key", return_value="test-key"):
                with patch("src.core.flags.get_flag", return_value=True):
                    yield


# ── Success path (no retry) ─────────────────────────────────────────

def test_grok_returns_success_dict_on_first_try():
    """Clean 200 response → returns parsed dict, no retry."""
    fake_breaker = _stub_breaker([_resp(200, _good_grok_body())])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is True
    assert result["price"] == 12.34
    assert result["asin"] == "B01ABCDE12"
    # `breaker.call` is invoked exactly once → no retry happened.
    assert fake_breaker.call.call_count == 1


# ── 429 retry path ──────────────────────────────────────────────────

def test_grok_429_retries_with_linear_backoff_then_succeeds():
    """429 → 200 sequence: retries with substrate's linear 2s backoff."""
    fake_breaker = _stub_breaker([
        _resp(429, {}),
        _resp(200, _good_grok_body()),
    ])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is True
    assert fake_breaker.call.call_count == 2
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [2.0]  # base_delay * (i+1) for i=0


def test_grok_429_exhaustion_returns_max_retries_exceeded_dict():
    """All attempts 429 → returns {"ok": False, "error": "Max retries exceeded"}."""
    fake_breaker = _stub_breaker([_resp(429, {})] * pv._MAX_RETRIES)
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep"):
            result = pv.validate_product(**_validate_kwargs())
    assert result == {"ok": False, "error": "Max retries exceeded"}
    assert fake_breaker.call.call_count == pv._MAX_RETRIES


# ── 5xx retry path (BEHAVIOR CHANGE: now sleeps) ───────────────────

def test_grok_5xx_retries_and_now_sleeps():
    """5xx → 200 sequence. New behavior: substrate adds linear 2s sleep.

    Pre-Tier-1d-PR-3, 5xx retries had NO sleep. Mike approved this
    change because back-to-back 5xx retries were hammering xAI; a 2s
    pause is gentler and the circuit breaker still tracks the failures.
    """
    fake_breaker = _stub_breaker([
        requests.exceptions.HTTPError("503 Service Unavailable"),
        _resp(200, _good_grok_body()),
    ])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is True
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert delays == [2.0]


def test_grok_5xx_exhaustion_returns_max_retries_exceeded_dict():
    """All attempts 5xx → error dict, breaker has counted each."""
    fake_breaker = _stub_breaker(
        [requests.exceptions.HTTPError("503")] * pv._MAX_RETRIES
    )
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep"):
            result = pv.validate_product(**_validate_kwargs())
    assert result == {"ok": False, "error": "Max retries exceeded"}


# ── Timeout retry path (BEHAVIOR CHANGE: now sleeps) ───────────────

def test_grok_timeout_retries_and_now_sleeps():
    """Timeout → 200 sequence. New behavior: substrate adds 2s sleep."""
    fake_breaker = _stub_breaker([
        requests.exceptions.Timeout("request timed out"),
        _resp(200, _good_grok_body()),
    ])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is True
    assert [c.args[0] for c in mock_sleep.call_args_list] == [2.0]


def test_grok_timeout_exhaustion_returns_max_retries_exceeded_dict():
    """All attempts timeout → same error-dict shape as the original."""
    fake_breaker = _stub_breaker(
        [requests.exceptions.Timeout("timeout")] * pv._MAX_RETRIES
    )
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep"):
            result = pv.validate_product(**_validate_kwargs())
    assert result == {"ok": False, "error": "Max retries exceeded"}


# ── 4xx (non-429) fast-fail — must NOT retry ───────────────────────

def test_grok_4xx_non_429_returns_immediately_no_retry():
    """A 401/403/etc. is a client error: fast-fail with API code in error."""
    fake_breaker = _stub_breaker([_resp(401, {})])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result == {"ok": False, "error": "API 401"}
    assert fake_breaker.call.call_count == 1
    assert mock_sleep.call_count == 0


# ── CircuitOpenError fast-return ───────────────────────────────────

def test_grok_circuit_open_returns_immediately_with_circuit_open_flag():
    """Open breaker → circuit_open=True, no retry, no sleep."""
    fake_breaker = _stub_breaker([
        pv.CircuitOpenError("grok", 5, "503 timeout")
    ])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result == {"ok": False, "error": "grok circuit open",
                      "circuit_open": True}
    assert fake_breaker.call.call_count == 1
    assert mock_sleep.call_count == 0


# ── Generic Exception fast-fail ────────────────────────────────────

def test_grok_generic_exception_returns_error_dict_no_retry():
    """Non-transient Exception (e.g. ConnectionError) → fast-fail."""
    fake_breaker = _stub_breaker([
        ConnectionError("DNS lookup failed"),
    ])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is False
    assert "DNS lookup failed" in result["error"]
    assert fake_breaker.call.call_count == 1
    assert mock_sleep.call_count == 0


# ── Mixed transient sequence ───────────────────────────────────────

def test_grok_mixed_transient_429_then_5xx_then_success():
    """Different transient errors in sequence all retry uniformly."""
    fake_breaker = _stub_breaker([
        _resp(429, {}),
        requests.exceptions.HTTPError("503"),
        _resp(200, _good_grok_body()),
    ])
    # Force _MAX_RETRIES >=3 for this test — the constant is 2 by default.
    with patch.object(pv, "_MAX_RETRIES", 3):
        with patch.object(pv, "get_breaker", return_value=fake_breaker):
            with patch("src.core.external_call.time.sleep") as mock_sleep:
                result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is True
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    # Linear backoff: 2 * (0+1) = 2.0, 2 * (1+1) = 4.0
    assert delays == [2.0, 4.0]


# ── Bad JSON fast-fail (success path returns error dict) ────────────

def test_grok_non_json_response_returns_error_dict_no_retry():
    """The original returned `{"ok": False, "error": "Response not JSON"}`
    inside the loop body. Must still happen — no retry on parse failure.
    """
    bad_resp = MagicMock()
    bad_resp.status_code = 200
    bad_resp.text = "this is not json"
    bad_resp.json = MagicMock(return_value={
        "choices": [{"message": {"content": "this is not json"}}],
        "usage": {},
    })
    fake_breaker = _stub_breaker([bad_resp])
    with patch.object(pv, "get_breaker", return_value=fake_breaker):
        with patch("src.core.external_call.time.sleep") as mock_sleep:
            result = pv.validate_product(**_validate_kwargs())
    assert result["ok"] is False
    assert result["error"] == "Response not JSON"
    assert fake_breaker.call.call_count == 1
    assert mock_sleep.call_count == 0
