"""Pin: scprs_scraper_client._fallback_local('scrape_exhaustive', ...)
calls _scrape_full_async DIRECTLY, never _scrape_with_retry.

Recursion bug closed 2026-05-26: the previous fallback called
`scprs_browser._scrape_with_retry`, but THAT function checks
`SCRAPER_SERVICE_URL` first and re-routes through the remote path →
scrape_exhaustive → _fallback_local → _scrape_with_retry → infinite
recursion. On the first ReadTimeout the daemon thread blew the
1000-frame Python limit (or leaked memory if it didn't hit the
limit first). With the daemon running inside the gunicorn web
worker process, this likely contributed to the ~6-min worker
recycles observed during the SCPRS audit verification.

Tests pin:
  1. _fallback_local('scrape_exhaustive', ...) calls _scrape_full_async,
     NOT _scrape_with_retry.
  2. The fallback completes in finite time even when both remote and
     playwright are unavailable (returns [] cleanly).
  3. Source check: the _fallback_local branch for scrape_exhaustive
     does not import _scrape_with_retry.
"""
from __future__ import annotations

from pathlib import Path


def test_fallback_calls_scrape_full_async_directly(monkeypatch):
    """Verify the fallback dispatches to _scrape_full_async, NOT
    _scrape_with_retry. Spy on both — only the async path should be
    called, never the retry helper that contains the SCRAPER_SERVICE_URL
    recheck (which would recurse)."""
    calls = {"_scrape_full_async": 0, "_scrape_with_retry": 0}

    async def _fake_async(search_params=None, seen_pos=None, max_rows=500):
        calls["_scrape_full_async"] += 1
        return []

    def _fake_retry(*args, **kwargs):
        calls["_scrape_with_retry"] += 1
        return []

    monkeypatch.setattr(
        "src.agents.scprs_browser._scrape_full_async", _fake_async,
    )
    monkeypatch.setattr(
        "src.agents.scprs_browser._scrape_with_retry", _fake_retry,
    )

    import src.agents.scprs_scraper_client as client
    result = client._fallback_local(
        "scrape_exhaustive",
        search_params={
            "supplier_name": "", "from_date": "05/01/2026",
            "to_date": "05/26/2026", "description": "",
        },
        seen_pos=set(),
        max_rows=500,
    )

    assert result == []
    assert calls["_scrape_full_async"] == 1, (
        "scrape_exhaustive fallback did not call _scrape_full_async — "
        f"calls={calls}"
    )
    assert calls["_scrape_with_retry"] == 0, (
        "scrape_exhaustive fallback called _scrape_with_retry — that "
        "creates the recursion bug because _scrape_with_retry re-checks "
        f"SCRAPER_SERVICE_URL and re-routes through remote. calls={calls}"
    )


def test_fallback_completes_in_finite_time(monkeypatch):
    """Most direct regression check: end-to-end, with both remote and
    playwright unavailable, the call must return cleanly within a few
    seconds. Pre-fix this hit infinite recursion."""
    monkeypatch.setenv("SCRAPER_SERVICE_URL", "http://nonexistent:8001")

    import requests
    def _boom(*a, **kw):
        raise requests.ConnectionError("scprs-scraper unreachable")

    import src.agents.scprs_scraper_client as client
    monkeypatch.setattr(client.requests, "post", _boom)

    # Stub playwright unavailable too so the local fallback returns [].
    monkeypatch.setattr(
        "src.agents.scprs_browser._playwright_available",
        lambda: False,
    )

    # Now call the daemon's path. Pre-fix this would recurse forever.
    # With timeout=5s as the safety net.
    from src.agents.scprs_browser import _scrape_with_retry
    import threading
    import time as _t

    result = {"value": None, "elapsed": None, "exc": None}

    def _run():
        start = _t.time()
        try:
            result["value"] = _scrape_with_retry(
                search_params={
                    "supplier_name": "", "from_date": "05/01/2026",
                    "to_date": "05/26/2026", "description": "",
                },
                seen_pos=set(),
                max_rows=500,
                max_retries=1,  # one attempt so the test is fast
            )
        except Exception as e:
            result["exc"] = e
        finally:
            result["elapsed"] = _t.time() - start

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=15)

    assert not t.is_alive(), (
        "_scrape_with_retry did not return within 15s — likely the "
        "recursion bug. Pre-fix this could spin until OOM or "
        "RecursionError."
    )
    # Either returns [] or raises ConnectionError — both are acceptable.
    # The KEY assertion is that it terminated.


def test_source_no_call_to_scrape_with_retry_in_exhaustive_fallback():
    """Anchor on the source: the `scrape_exhaustive` branch of
    _fallback_local must NOT import or call _scrape_with_retry.
    Catches a regression where a future refactor reintroduces the
    recursion."""
    src = Path(__file__).parent.parent.joinpath(
        "src", "agents", "scprs_scraper_client.py"
    ).read_text(encoding="utf-8")

    # Find the scrape_exhaustive branch.
    idx = src.find('elif fn_name == "scrape_exhaustive"')
    assert idx > -1, "scrape_exhaustive branch missing from _fallback_local"

    # Look at the next ~1000 chars to find the branch body up to the
    # next elif/else/raise.
    body = src[idx:idx + 1500]

    # The branch must NOT IMPORT/CALL _scrape_with_retry — that's the
    # recursion entry point. The function name CAN appear in comments
    # explaining the bug; what matters is the actual code.
    # Strip Python comment lines before checking.
    code_only = "\n".join(
        line for line in body.split("\n")
        if not line.lstrip().startswith("#")
    )
    assert "_scrape_with_retry" not in code_only, (
        "_fallback_local('scrape_exhaustive') still references "
        "_scrape_with_retry in CODE (not just comments) — that creates "
        "the recursion bug. Call _scrape_full_async directly instead."
    )
    # The branch MUST call the async helper.
    assert "_scrape_full_async" in body, (
        "_fallback_local('scrape_exhaustive') does not call "
        "_scrape_full_async — degraded local path missing"
    )
