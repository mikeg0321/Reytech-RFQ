"""Pin transient-error retry on QuickBooks Online API calls (Tier 1d
follow-on, audit 2026-05-07).

QB had NO retry before this PR. A flaky 503 from Intuit's gateway
during invoice POST silently failed the route — the calling code
(`_qb_request`) returned None and the operator saw "QB sync failed"
with no automatic recovery. Same shape on `get_invoice_pdf` and
`_refresh_access_token`.

This pins:
  1. 5xx + 429 + ConnectionError + Timeout from QB are transient,
     retried up to 3x with linear 2.0s/4.0s backoff.
  2. 401 / 403 / 404 / 400 (operator/data errors) are NOT retried
     and fast-fail. 401 routes through `_qb_request`'s existing
     refresh-then-retry-once flow without consuming retry budget.
  3. The token-refresh response is correctly handled when it goes
     500 once then 200 (typical Intuit gateway behavior).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ─────────────────────────────────────────────────────────────────────
# Predicate parity
# ─────────────────────────────────────────────────────────────────────

def _http_error(status: int):
    """Build a real `requests.exceptions.HTTPError` carrying
    `.response.status_code = <status>`."""
    import requests
    resp = MagicMock()
    resp.status_code = status
    resp.text = f"fake body {status}"
    err = requests.exceptions.HTTPError(f"HTTP {status}")
    err.response = resp
    return err


def test_predicate_recognizes_transient_http_statuses():
    from src.agents.quickbooks_agent import _is_transient_qb_error
    for status in (429, 500, 502, 503, 504):
        assert _is_transient_qb_error(_http_error(status)), (
            f"status={status} should be transient")


def test_predicate_rejects_4xx_non_transient():
    """401 / 403 / 404 / 400 — operator/data error, not transit blip."""
    from src.agents.quickbooks_agent import _is_transient_qb_error
    for status in (400, 401, 403, 404, 409, 422):
        assert not _is_transient_qb_error(_http_error(status)), (
            f"status={status} must NOT be transient")


def test_predicate_recognizes_connection_errors():
    """ConnectionError / Timeout from `requests` — pre-server transit blip."""
    from src.agents.quickbooks_agent import _is_transient_qb_error
    import requests
    assert _is_transient_qb_error(
        requests.exceptions.ConnectionError("connection failed"))
    assert _is_transient_qb_error(
        requests.exceptions.Timeout("read timed out"))


def test_predicate_rejects_unrelated_exceptions():
    from src.agents.quickbooks_agent import _is_transient_qb_error
    assert not _is_transient_qb_error(ValueError("bad json"))
    assert not _is_transient_qb_error(KeyError("missing"))


# ─────────────────────────────────────────────────────────────────────
# Helper wraps with_retry
# ─────────────────────────────────────────────────────────────────────

def test_with_qb_retry_succeeds_after_one_5xx(monkeypatch):
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def fn():
        n["calls"] += 1
        if n["calls"] == 1:
            raise _http_error(503)
        return {"ok": True}

    out = qb._with_qb_retry(fn, op="test")
    assert n["calls"] == 2
    assert out["ok"] is True


def test_with_qb_retry_propagates_401_immediately(monkeypatch):
    """401 is non-transient — operator reauth required, no retry."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def fn():
        n["calls"] += 1
        raise _http_error(401)

    try:
        qb._with_qb_retry(fn, op="test")
        raise AssertionError("expected HTTPError")
    except Exception as e:
        assert "401" in str(e) or "401" in str(getattr(e, "response", ""))
    assert n["calls"] == 1


def test_with_qb_retry_exhausts_then_raises(monkeypatch):
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

    n = {"calls": 0}

    def fn():
        n["calls"] += 1
        raise _http_error(500)

    try:
        qb._with_qb_retry(fn, op="test")
        raise AssertionError("expected HTTPError")
    except Exception as e:
        assert "500" in str(e) or "500" in str(getattr(e, "response", ""))
    assert n["calls"] == 3  # 1 initial + 2 retries


# ─────────────────────────────────────────────────────────────────────
# Integration: real call sites use the helper
# ─────────────────────────────────────────────────────────────────────

def test_refresh_access_token_retries_5xx_then_succeeds(monkeypatch, tmp_path):
    """`_refresh_access_token`: a 503 then a 200 must yield the new
    token. Pre-PR, the first 503 raised raise_for_status, was caught
    by the outer except, and returned None — operator saw a stale
    token even though the second attempt would have worked."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    # Sandbox token file in tmp.
    monkeypatch.setattr(qb, "TOKEN_FILE", str(tmp_path / "qb_tokens.json"))
    monkeypatch.setattr(qb, "QB_CLIENT_ID", "test_id")
    monkeypatch.setattr(qb, "QB_CLIENT_SECRET", "test_secret")
    monkeypatch.setattr(qb, "_get_refresh_token", lambda: "refresh_X")

    n = {"calls": 0}

    def fake_post(url, headers=None, data=None, timeout=None):
        n["calls"] += 1
        resp = MagicMock()
        if n["calls"] == 1:
            # First call: 503 transient
            resp.status_code = 503
            resp.text = "Service Unavailable"

            def raise_503():
                raise _http_error(503)
            resp.raise_for_status = raise_503
            return resp
        # Second call: success
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        resp.json.return_value = {
            "access_token": "new_access",
            "refresh_token": "new_refresh",
            "expires_in": 3600,
        }
        return resp

    monkeypatch.setattr(qb._requests, "post", fake_post)

    out = qb._refresh_access_token()
    assert out == "new_access"
    assert n["calls"] == 2


def test_refresh_access_token_fast_fails_on_401(monkeypatch, tmp_path):
    """A 401 from Intuit on the refresh endpoint means the refresh
    token is dead — operator must re-OAuth. Don't waste retry budget."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(qb, "TOKEN_FILE", str(tmp_path / "qb_tokens.json"))
    monkeypatch.setattr(qb, "QB_CLIENT_ID", "test_id")
    monkeypatch.setattr(qb, "QB_CLIENT_SECRET", "test_secret")
    monkeypatch.setattr(qb, "_get_refresh_token", lambda: "stale_refresh")

    n = {"calls": 0}

    def fake_post(url, headers=None, data=None, timeout=None):
        n["calls"] += 1
        resp = MagicMock()
        resp.status_code = 401
        resp.text = "invalid_grant"

        def raise_401():
            raise _http_error(401)
        resp.raise_for_status = raise_401
        return resp

    monkeypatch.setattr(qb._requests, "post", fake_post)

    out = qb._refresh_access_token()
    assert out is None
    assert n["calls"] == 1  # NO retry on stale-refresh-token


def test_qb_request_retries_503_then_succeeds(monkeypatch, tmp_path):
    """`_qb_request` core flow: a 503 then 200 returns the JSON, so
    a transient gateway blip doesn't fail the calling route."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(qb, "get_access_token", lambda: "tok_X")
    monkeypatch.setattr(qb, "_get_realm_id", lambda: "realm_X")
    monkeypatch.setattr(qb, "_get_api_base",
                        lambda: "https://qb.test/v3/company/realm_X")

    n = {"calls": 0}

    def fake_get(url, headers=None, timeout=None):
        n["calls"] += 1
        resp = MagicMock()
        if n["calls"] == 1:
            resp.status_code = 503
            resp.text = "Service Unavailable"

            def raise_503():
                raise _http_error(503)
            resp.raise_for_status = raise_503
            return resp
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        resp.json.return_value = {"QueryResponse": {"Customer": [{"Id": "1"}]}}
        return resp

    monkeypatch.setattr(qb._requests, "get", fake_get)

    out = qb._qb_request("GET", "query?foo=bar")
    assert out == {"QueryResponse": {"Customer": [{"Id": "1"}]}}
    assert n["calls"] == 2


def test_qb_request_401_triggers_refresh_then_retry_once(monkeypatch):
    """`_qb_request` 401 path: a stale access token causes a 401, the
    refresh flow runs and a fresh attempt succeeds — without consuming
    the 5xx retry budget. Pre-existing behavior preserved across the
    retry-substrate migration."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(qb, "_get_realm_id", lambda: "realm_X")
    monkeypatch.setattr(qb, "_get_api_base",
                        lambda: "https://qb.test/v3/company/realm_X")

    tokens = {"value": "stale"}

    def fake_access():
        return tokens["value"]

    def fake_refresh():
        tokens["value"] = "fresh"
        return "fresh"

    monkeypatch.setattr(qb, "get_access_token", fake_access)
    monkeypatch.setattr(qb, "_refresh_access_token", fake_refresh)

    log_calls = {"posts": []}

    def fake_post(url, headers=None, json=None, timeout=None):
        bearer = headers.get("Authorization", "")
        log_calls["posts"].append(bearer)
        resp = MagicMock()
        if "stale" in bearer:
            # 401 — token expired mid-request
            resp.status_code = 401
            resp.text = "AuthenticationFailed"

            def raise_401():
                raise _http_error(401)
            resp.raise_for_status = raise_401
            return resp
        # Fresh token — 200
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        resp.json.return_value = {"Invoice": {"Id": "INV-1"}}
        return resp

    monkeypatch.setattr(qb._requests, "post", fake_post)

    out = qb._qb_request("POST", "invoice", data={"foo": "bar"})
    assert out == {"Invoice": {"Id": "INV-1"}}
    # Two POSTs: one with stale (401), one with fresh (200). Retry
    # budget was NOT consumed — the 401 path is separate.
    assert len(log_calls["posts"]) == 2
    assert "stale" in log_calls["posts"][0]
    assert "fresh" in log_calls["posts"][1]


def test_qb_request_5xx_exhausts_then_returns_none(monkeypatch):
    """All 3 attempts return 503 → caller sees None, no exception
    leaks. The route handler at `_qb_request`'s callsites already
    branches on None; this preserves that contract."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(qb, "get_access_token", lambda: "tok_X")
    monkeypatch.setattr(qb, "_get_realm_id", lambda: "realm_X")
    monkeypatch.setattr(qb, "_get_api_base",
                        lambda: "https://qb.test/v3/company/realm_X")

    n = {"calls": 0}

    def fake_get(url, headers=None, timeout=None):
        n["calls"] += 1
        resp = MagicMock()
        resp.status_code = 503
        resp.text = "down"

        def raise_503():
            raise _http_error(503)
        resp.raise_for_status = raise_503
        return resp

    monkeypatch.setattr(qb._requests, "get", fake_get)

    out = qb._qb_request("GET", "query?x=1")
    assert out is None
    assert n["calls"] == 3  # full retry budget


def test_get_invoice_pdf_retries_transient(monkeypatch):
    """`get_invoice_pdf`: a 502 then 200 must return the PDF bytes."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(qb, "is_configured", lambda: True)
    monkeypatch.setattr(qb, "get_access_token", lambda: "tok_X")
    monkeypatch.setattr(qb, "_get_api_base",
                        lambda: "https://qb.test/v3/company/realm_X")

    n = {"calls": 0}

    def fake_get(url, headers=None, timeout=None):
        n["calls"] += 1
        resp = MagicMock()
        if n["calls"] == 1:
            resp.status_code = 502
            resp.text = "bad gateway"

            def raise_502():
                raise _http_error(502)
            resp.raise_for_status = raise_502
            return resp
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        resp.content = b"%PDF-1.4 fake invoice"
        return resp

    monkeypatch.setattr(qb._requests, "get", fake_get)

    out = qb.get_invoice_pdf("INV-1")
    assert out == b"%PDF-1.4 fake invoice"
    assert n["calls"] == 2


def test_get_invoice_pdf_404_fast_fails(monkeypatch):
    """404 (invoice not found) is a real not-found, not a transit
    blip — return None immediately, don't waste retry budget."""
    from src.agents import quickbooks_agent as qb
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(qb, "is_configured", lambda: True)
    monkeypatch.setattr(qb, "get_access_token", lambda: "tok_X")
    monkeypatch.setattr(qb, "_get_api_base",
                        lambda: "https://qb.test/v3/company/realm_X")

    n = {"calls": 0}

    def fake_get(url, headers=None, timeout=None):
        n["calls"] += 1
        resp = MagicMock()
        resp.status_code = 404
        resp.text = "Not Found"

        def raise_404():
            raise _http_error(404)
        resp.raise_for_status = raise_404
        return resp

    monkeypatch.setattr(qb._requests, "get", fake_get)

    out = qb.get_invoice_pdf("INV-MISSING")
    assert out is None
    assert n["calls"] == 1  # NO retry on 404
