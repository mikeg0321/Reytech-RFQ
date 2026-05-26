"""
scprs_scraper_client.py — HTTP client for the SCPRS Scraper microservice.

Replaces direct Playwright imports. Falls back to local Playwright if
the scraper service is not configured (SCRAPER_SERVICE_URL not set).

Circuit breaker protection: after 3 consecutive failures, the client
stops trying the remote service for 120s and falls back immediately.

Usage:
    from src.agents.scprs_scraper_client import scrape_details, scrape_po_detail, search_public

    # These call the remote scraper service via HTTP, or fall back to local Playwright
    results = scrape_details(supplier_name="reytech", max_rows=100)
"""
import os
import logging
import requests

log = logging.getLogger("reytech.scprs_client")

SCRAPER_URL = os.environ.get("SCRAPER_SERVICE_URL", "")
SCRAPER_SECRET = os.environ.get("SCRAPER_SECRET", "")

# Timeouts: connect fast (5s), read slow (scraping is genuinely slow)
# But cap at 120s — if it takes longer than Gunicorn's 120s timeout, the
# request dies anyway. Better to fail fast and fall back.
_CONNECT_TIMEOUT = 5
_READ_TIMEOUT = 120

try:
    from src.core.circuit_breaker import get_breaker, CircuitOpenError
    _scraper_breaker = get_breaker("scraper_service")
except ImportError:
    _scraper_breaker = None
    class CircuitOpenError(Exception):
        pass


def _headers():
    h = {"Content-Type": "application/json"}
    if SCRAPER_SECRET:
        h["X-Scraper-Secret"] = SCRAPER_SECRET
    return h


def _call_scraper(endpoint: str, payload: dict) -> dict:
    """Call the scraper microservice. Returns the response data or raises."""
    if not SCRAPER_URL:
        raise ConnectionError("SCRAPER_SERVICE_URL not configured")
    url = f"{SCRAPER_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    resp = requests.post(
        url, json=payload, headers=_headers(),
        timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT),
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("error", "Scraper returned error"))
    return data.get("data", {})


def _call_with_breaker(endpoint: str, payload: dict) -> dict:
    """Call scraper through circuit breaker. Raises CircuitOpenError if breaker is open."""
    if _scraper_breaker:
        return _scraper_breaker.call(_call_scraper, endpoint, payload)
    return _call_scraper(endpoint, payload)


def _fallback_local(fn_name, *args, **kwargs):
    """Try local Playwright import as fallback."""
    log.info("Falling back to local Playwright for %s", fn_name)
    if fn_name == "scrape_details":
        from src.agents.scprs_browser import scrape_details
        return scrape_details(*args, **kwargs)
    elif fn_name == "scrape_po_detail":
        from src.agents.scprs_browser import scrape_po_detail
        return scrape_po_detail(*args, **kwargs)
    elif fn_name == "scrape_exhaustive":
        # Local fallback: call the same _scrape_with_retry path the
        # daemon used pre-remote. If local playwright is unavailable
        # it returns []; the daemon's outer loop is tolerant of empty.
        from src.agents.scprs_browser import _scrape_with_retry
        return _scrape_with_retry(*args, **kwargs)
    elif fn_name == "search_scprs_public":
        from src.agents.scprs_public_search import search_scprs_public
        return search_scprs_public(*args, **kwargs)
    elif fn_name == "search_scprs_intercept":
        from src.agents.scprs_public_search import search_scprs_intercept
        return search_scprs_intercept(*args, **kwargs)
    raise ValueError(f"Unknown fallback function: {fn_name}")


# ── Public API (matches original function signatures) ────────────────────────

def scrape_details(supplier_name="reytech", from_date="", max_rows=200):
    """Scrape FI$Cal SCPRS detail pages for a supplier."""
    try:
        return _call_with_breaker("/scrape/details", {
            "supplier_name": supplier_name,
            "from_date": from_date,
            "max_rows": max_rows,
        })
    except CircuitOpenError:
        log.debug("Scraper circuit open — immediate fallback")
        return _fallback_local("scrape_details",
                               supplier_name=supplier_name,
                               from_date=from_date, max_rows=max_rows)
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("scrape_details",
                               supplier_name=supplier_name,
                               from_date=from_date, max_rows=max_rows)


def scrape_po_detail(po_number):
    """Scrape a single PO detail from FI$Cal."""
    try:
        return _call_with_breaker("/scrape/po", {"po_number": po_number})
    except CircuitOpenError:
        log.debug("Scraper circuit open — immediate fallback")
        return _fallback_local("scrape_po_detail", po_number)
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("scrape_po_detail", po_number)


def search_scprs_public(keyword="", department_code="", max_results=50):
    """Search the public CaleProcure SCPRS site."""
    try:
        return _call_with_breaker("/scrape/public-search", {
            "keyword": keyword,
            "department_code": department_code,
            "max_results": max_results,
        })
    except CircuitOpenError:
        log.debug("Scraper circuit open — immediate fallback")
        return _fallback_local("search_scprs_public",
                               keyword=keyword, department_code=department_code,
                               max_results=max_results)
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("search_scprs_public",
                               keyword=keyword, department_code=department_code,
                               max_results=max_results)


def scrape_exhaustive(supplier_name="", from_date="", to_date="",
                      description="", max_rows=500, seen_pos=None):
    """Run an exhaustive FI$Cal scrape for a date window via the remote
    scraper service. Returns the list of PO dicts the service collected.

    Chrome MCP audit 2026-05-26 anomaly #9: the daemon at
    `scprs_browser._run_exhaustive_scrape` called
    `_scrape_with_retry` → `_scrape_full_async` directly, which bailed
    on `_playwright_available()=False` and returned []. The remote
    scraper service has been running with playwright since 2026-05-12
    and exposes the matching `/scrape/exhaustive` endpoint, but no
    client wrapper existed — so the daemon never tried it. This
    wrapper closes that gap.

    `seen_pos`: passed through as a list (JSON-serializable). The
    remote service rebuilds the set on its side.
    """
    payload = {
        "supplier_name": supplier_name,
        "from_date": from_date,
        "to_date": to_date,
        "description": description,
        "max_rows": max_rows,
        "seen_pos": list(seen_pos) if seen_pos else [],
    }
    try:
        return _call_with_breaker("/scrape/exhaustive", payload)
    except CircuitOpenError:
        log.debug("Scraper circuit open — immediate fallback to local")
        return _fallback_local(
            "scrape_exhaustive",
            search_params={
                "supplier_name": supplier_name, "from_date": from_date,
                "to_date": to_date, "description": description,
            },
            seen_pos=seen_pos or set(),
            max_rows=max_rows,
        )
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error on /scrape/exhaustive: %s", e)
        return _fallback_local(
            "scrape_exhaustive",
            search_params={
                "supplier_name": supplier_name, "from_date": from_date,
                "to_date": to_date, "description": description,
            },
            seen_pos=seen_pos or set(),
            max_rows=max_rows,
        )


def search_scprs_intercept(keyword="", department_code="3860"):
    """Search SCPRS via network interception."""
    try:
        return _call_with_breaker("/scrape/intercept", {
            "keyword": keyword,
            "department_code": department_code,
        })
    except CircuitOpenError:
        log.debug("Scraper circuit open — immediate fallback")
        return _fallback_local("search_scprs_intercept",
                               keyword=keyword, department_code=department_code)
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("search_scprs_intercept",
                               keyword=keyword, department_code=department_code)
