"""Pin: when SCRAPER_SERVICE_URL is set, the exhaustive scraper daemon
routes through the remote scprs-scraper service instead of bailing on
missing local playwright.

Chrome MCP audit 2026-05-26 anomaly #9: 25 days of silent zero-row
writes because `_scrape_with_retry` went directly to
`_scrape_full_async` (local playwright path), and the container had
no playwright installed. Meanwhile the `scprs-scraper` Railway
service was happily running with playwright and exposing
`/scrape/exhaustive` — just not being called. This PR adds the
client wrapper + routes through it when configured.

Tests pin:
  1. With SCRAPER_SERVICE_URL set, `_scrape_with_retry` calls the
     client's `scrape_exhaustive` (not the local async path).
  2. With SCRAPER_SERVICE_URL unset, behavior is unchanged (still
     hits local path — preserves dev-loop and pre-remote semantics).
  3. The client's scrape_exhaustive POSTs to /scrape/exhaustive with
     the expected payload shape (supplier_name, from_date, to_date,
     description, max_rows, seen_pos).
  4. Remote failure (ConnectionError) falls back to local via
     _fallback_local.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock


def test_scrape_with_retry_prefers_remote_when_env_set(monkeypatch):
    monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scprs-scraper:8001")

    # Mock the client's scrape_exhaustive to capture the call.
    captured = {}

    def _fake_exhaustive(**kwargs):
        captured.update(kwargs)
        return [{"po_number": "PO-REMOTE-1"}]

    monkeypatch.setattr(
        "src.agents.scprs_scraper_client.scrape_exhaustive",
        _fake_exhaustive,
    )

    from src.agents.scprs_browser import _scrape_with_retry
    result = _scrape_with_retry(
        search_params={
            "supplier_name": "",
            "from_date": "05/01/2026",
            "to_date": "05/26/2026",
            "description": "",
        },
        seen_pos=set(),
        max_rows=500,
        max_retries=1,
    )

    assert result == [{"po_number": "PO-REMOTE-1"}], (
        "_scrape_with_retry did not route through the remote client"
    )
    # Payload shape must include the date window.
    assert captured.get("from_date") == "05/01/2026"
    assert captured.get("to_date") == "05/26/2026"
    assert captured.get("max_rows") == 500


def test_scrape_with_retry_uses_local_when_env_unset(monkeypatch):
    """Preserve pre-remote behavior when SCRAPER_SERVICE_URL is unset
    (dev loops, tests without the env var, etc.)."""
    monkeypatch.delenv("SCRAPER_SERVICE_URL", raising=False)

    # Spy: if the remote client gets called we'd see it; instead expect
    # the local `_scrape_full_async` path.
    remote_calls = []

    def _spy_remote(**kw):
        remote_calls.append(kw)
        return []

    monkeypatch.setattr(
        "src.agents.scprs_scraper_client.scrape_exhaustive",
        _spy_remote,
    )

    # Stub out the local async path so the test doesn't actually try
    # to launch playwright.
    monkeypatch.setattr(
        "src.agents.scprs_browser._playwright_available",
        lambda: False,
    )

    from src.agents.scprs_browser import _scrape_with_retry
    result = _scrape_with_retry(
        search_params={"supplier_name": "", "from_date": "", "to_date": ""},
        seen_pos=set(),
        max_rows=10,
        max_retries=1,
    )

    # Local path returns [] when playwright unavailable — preserve that.
    assert result == []
    assert remote_calls == [], (
        "remote scrape_exhaustive was called even though "
        "SCRAPER_SERVICE_URL is unset"
    )


def test_client_scrape_exhaustive_posts_correct_shape(monkeypatch):
    """The new client wrapper must POST to /scrape/exhaustive with the
    full payload the remote service expects (matches its app.py
    `data.get(...)` reads)."""
    monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scprs-scraper:8001")
    monkeypatch.setenv("SCRAPER_SECRET", "test-secret")

    # Re-import to pick up the env vars.
    import importlib
    import src.agents.scprs_scraper_client as client
    importlib.reload(client)

    captured = {}

    class _FakeResp:
        status_code = 200
        def raise_for_status(self):
            pass
        def json(self):
            return {"ok": True, "data": [{"po_number": "PO-XYZ"}]}

    def _fake_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return _FakeResp()

    monkeypatch.setattr(client.requests, "post", _fake_post)

    result = client.scrape_exhaustive(
        supplier_name="",
        from_date="05/01/2026",
        to_date="05/26/2026",
        description="",
        max_rows=500,
        seen_pos=["PO-A", "PO-B"],
    )

    assert result == [{"po_number": "PO-XYZ"}]
    assert "/scrape/exhaustive" in captured["url"]
    assert captured["json"]["from_date"] == "05/01/2026"
    assert captured["json"]["to_date"] == "05/26/2026"
    assert captured["json"]["max_rows"] == 500
    assert captured["json"]["seen_pos"] == ["PO-A", "PO-B"]
    # Auth header propagates.
    assert captured["headers"].get("X-Scraper-Secret") == "test-secret"


def test_client_falls_back_to_local_on_connection_error(monkeypatch):
    """When the remote service is unreachable, the client falls back to
    local via `_fallback_local` so the daemon still gets a result
    (empty if local playwright is also unavailable, but no crash)."""
    monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://scprs-scraper:8001")

    import importlib
    import src.agents.scprs_scraper_client as client
    importlib.reload(client)

    # Force ConnectionError on the POST.
    def _boom(*a, **kw):
        import requests
        raise requests.ConnectionError("scprs-scraper unreachable")
    monkeypatch.setattr(client.requests, "post", _boom)

    # Capture _fallback_local calls so we can prove it was tried.
    fallback_calls = []
    real_fallback = client._fallback_local

    def _spy_fallback(fn_name, *a, **kw):
        fallback_calls.append({"fn_name": fn_name, "kwargs": kw})
        return []  # safe degraded result

    monkeypatch.setattr(client, "_fallback_local", _spy_fallback)

    result = client.scrape_exhaustive(
        supplier_name="", from_date="01/01/2026", to_date="01/31/2026",
        max_rows=100, seen_pos=set(),
    )

    assert result == []
    assert len(fallback_calls) == 1
    assert fallback_calls[0]["fn_name"] == "scrape_exhaustive"
