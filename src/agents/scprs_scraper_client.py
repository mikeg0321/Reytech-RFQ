"""
scprs_scraper_client.py — HTTP client for the SCPRS Scraper microservice.

Replaces direct Playwright imports. Falls back to local Playwright if
the scraper service is not configured (SCRAPER_SERVICE_URL not set).

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
_TIMEOUT = 300  # 5 min — scraping is slow


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
    resp = requests.post(url, json=payload, headers=_headers(), timeout=_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("error", "Scraper returned error"))
    return data.get("data", {})


def _fallback_local(fn_name, *args, **kwargs):
    """Try local Playwright import as fallback."""
    log.info("Scraper service unavailable — falling back to local Playwright for %s", fn_name)
    if fn_name == "scrape_details":
        from src.agents.scprs_browser import scrape_details
        return scrape_details(*args, **kwargs)
    elif fn_name == "scrape_po_detail":
        from src.agents.scprs_browser import scrape_po_detail
        return scrape_po_detail(*args, **kwargs)
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
        return _call_scraper("/scrape/details", {
            "supplier_name": supplier_name,
            "from_date": from_date,
            "max_rows": max_rows,
        })
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("scrape_details",
                               supplier_name=supplier_name,
                               from_date=from_date, max_rows=max_rows)


def scrape_po_detail(po_number):
    """Scrape a single PO detail from FI$Cal."""
    try:
        return _call_scraper("/scrape/po", {"po_number": po_number})
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("scrape_po_detail", po_number)


def search_scprs_public(keyword="", department_code="", max_results=50):
    """Search the public CaleProcure SCPRS site."""
    try:
        return _call_scraper("/scrape/public-search", {
            "keyword": keyword,
            "department_code": department_code,
            "max_results": max_results,
        })
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("search_scprs_public",
                               keyword=keyword, department_code=department_code,
                               max_results=max_results)


def search_scprs_intercept(keyword="", department_code="3860"):
    """Search SCPRS via network interception."""
    try:
        return _call_scraper("/scrape/intercept", {
            "keyword": keyword,
            "department_code": department_code,
        })
    except (ConnectionError, requests.RequestException) as e:
        log.warning("Scraper service error: %s", e)
        return _fallback_local("search_scprs_intercept",
                               keyword=keyword, department_code=department_code)
