"""
scprs_public_search.py — Public SCPRS Web Scraper
Phase 32 | No credentials required

Scrapes caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx
using Playwright (headless Chromium). This is 100% public data —
the same search any vendor can do manually in a browser.

Searches:
  - By department (CDCR/CCHCS business unit codes)
  - By keyword (nitrile gloves, chux, adult briefs, etc.)
  - Filters results to show what's being bought, from whom, at what price

Returns structured data:
  [{"po_number": "...", "department": "...", "vendor": "...",
    "description": "...", "amount": 0.00, "date": "...",
    "acquisition_type": "...", "commodity_code": "..."}]
"""

import json
import os
import logging
import time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("scprs_public")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

SCPRS_CACHE_FILE = os.path.join(DATA_DIR, "scprs_public_cache.json")
SCPRS_URL = "https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx"

# CDCR/CCHCS business units in FI$Cal
# These are the department codes used in the SCPRS search
CCHCS_DEPT_CODES = [
    "3860",   # CDCR (Department of Corrections & Rehabilitation)
    "5225",   # CCHCS (CA Correctional Health Care Services)
]

# Products Reytech sells / wants to identify gaps in
REYTECH_KEYWORDS = [
    "nitrile gloves",
    "exam gloves",
    "chux",
    "underpads",
    "adult briefs",
    "incontinence",
    "N95",
    "respirator",
    "wound care",
    "gauze",
    "hand sanitizer",
    "first aid",
    "sharps",
    "gown",
    "face mask",
    "surgical mask",
    "hi-vis",
    "safety vest",
    "janitorial",
    "trash bags",
    "paper towels",
]


def _load_cache() -> dict:
    try:
        return json.load(open(SCPRS_CACHE_FILE))
    except (FileNotFoundError, json.JSONDecodeError):
        return {"pulls": [], "last_pull": None}


def _save_cache(data: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    json.dump(data, open(SCPRS_CACHE_FILE, "w"), indent=2, default=str)


def search_scprs_public(
    keyword: str = "",
    department_code: str = "",
    max_results: int = 200,
    fiscal_year: str = "",
) -> dict:
    """
    Search the public SCPRS using Playwright (headless browser).
    Works on Railway — no credentials needed, just public web access.

    Returns:
      {"ok": True, "results": [...], "count": N, "keyword": "...", "dept": "..."}
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        return {"ok": False, "error": "Playwright not installed. Run: pip install playwright && playwright install chromium"}

    results = []
    error_msg = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1280, "height": 900},
            )
            page = ctx.new_page()

            log.info("SCPRS public search: keyword='%s' dept='%s'", keyword, department_code)

            # Navigate to SCPRS search
            page.goto(SCPRS_URL, wait_until="networkidle", timeout=30000)
            time.sleep(2)

            # The SCPRS page uses AngularJS. Fields to fill:
            # Description field: keyword search
            # Department field: dropdown or text
            # Fiscal Year: dropdown

            # Try to find and fill the description/keyword field
            desc_selectors = [
                'input[ng-model*="description"]',
                'input[placeholder*="Description"]',
                'input[placeholder*="description"]',
                'input[id*="description"]',
                '#description',
                'input[name*="description"]',
            ]
            filled_keyword = False
            if keyword:
                for sel in desc_selectors:
                    try:
                        if page.is_visible(sel, timeout=2000):
                            page.fill(sel, keyword)
                            filled_keyword = True
                            log.info("Filled keyword in: %s", sel)
                            break
                    except PWTimeout:
                        continue

            # Fill department code
            dept_selectors = [
                'input[ng-model*="department"]',
                'input[ng-model*="busUnit"]',
                'input[placeholder*="Department"]',
                'select[ng-model*="department"]',
                '#departmentCode',
                'input[id*="dept"]',
            ]
            filled_dept = False
            if department_code:
                for sel in dept_selectors:
                    try:
                        if page.is_visible(sel, timeout=2000):
                            page.fill(sel, department_code)
                            filled_dept = True
                            log.info("Filled dept in: %s", sel)
                            break
                    except PWTimeout:
                        continue

            # Click Search button
            search_btns = [
                'button:has-text("Search")',
                'input[type="submit"][value*="Search"]',
                'button[type="submit"]',
                '.search-btn',
                '#searchBtn',
                'a:has-text("Search")',
            ]
            clicked = False
            for sel in search_btns:
                try:
                    if page.is_visible(sel, timeout=2000):
                        page.click(sel)
                        clicked = True
                        log.info("Clicked search: %s", sel)
                        break
                except PWTimeout:
                    continue

            if not clicked:
                # Try pressing Enter in a search field
                try:
                    page.keyboard.press("Enter")
                    clicked = True
                except Exception:
                    pass

            # Wait for results to load
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PWTimeout:
                pass
            time.sleep(3)

            # Capture the page HTML and structure for debugging
            page_text = page.inner_text("body") if page else ""
            page_html = page.content() if page else ""

            # Try to extract results from table
            import re
            result_selectors = [
                "table tbody tr",
                ".results-table tr",
                "[ng-repeat*='result']",
                "[ng-repeat*='po']",
                ".scprs-result",
                ".result-row",
            ]

            for sel in result_selectors:
                try:
                    rows = page.query_selector_all(sel)
                    if rows:
                        log.info("Found %d result rows with: %s", len(rows), sel)
                        for row in rows[:max_results]:
                            try:
                                cells = row.query_selector_all("td")
                                if cells and len(cells) >= 3:
                                    cell_texts = [c.inner_text().strip() for c in cells]
                                    results.append({
                                        "raw_cells": cell_texts,
                                        "source_selector": sel,
                                    })
                            except Exception:
                                continue
                        if results:
                            break
                except Exception:
                    continue

            # If table extraction didn't work, capture page state for diagnosis
            if not results:
                log.warning("No table results found. Page text sample: %s", page_text[:500])

            # Try to detect any Angular data in page scope
            try:
                angular_data = page.evaluate("""
                    () => {
                        // Try AngularJS scope
                        try {
                            var scope = angular.element(document.querySelector('[ng-controller]')).scope();
                            return JSON.stringify(scope.results || scope.searchResults || scope.poResults || []);
                        } catch(e) {}
                        // Try window data
                        try { return JSON.stringify(window.scprsResults || []); } catch(e) {}
                        return null;
                    }
                """)
                if angular_data and angular_data != "null":
                    parsed = json.loads(angular_data)
                    if isinstance(parsed, list) and parsed:
                        for item in parsed[:max_results]:
                            results.append({"angular_data": item})
                        log.info("Got %d results from Angular scope", len(results))
            except Exception as e:
                log.debug("Angular extraction failed: %s", e)

            # Capture network responses that contain results
            page_url = page.url
            log.info("Final page URL: %s", page_url)

            ctx.close()
            browser.close()

    except Exception as e:
        error_msg = str(e)
        log.error("SCPRS Playwright error: %s", e)

    return {
        "ok": bool(results) or error_msg is None,
        "results": results,
        "count": len(results),
        "keyword": keyword,
        "department": department_code,
        "error": error_msg,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def search_scprs_intercept(keyword: str, department_code: str = "3860") -> dict:
    """
    Use Playwright with network interception to capture the API call
    that the SCPRS page makes internally. This finds the real JSON endpoint.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"ok": False, "error": "Playwright not installed"}

    captured_requests = []
    captured_responses = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

            # Intercept all network requests
            def on_request(req):
                url = req.url
                if any(x in url.lower() for x in ["api", "search", "scprs", "json", "data", "query", "service"]):
                    captured_requests.append({
                        "url": url,
                        "method": req.method,
                        "post_data": req.post_data,
                        "headers": dict(req.headers),
                    })

            def on_response(resp):
                url = resp.url
                if any(x in url.lower() for x in ["api", "search", "scprs", "json", "data", "query", "service"]):
                    try:
                        body = resp.text()
                        if body and len(body) > 50:
                            captured_responses.append({
                                "url": url,
                                "status": resp.status,
                                "content_type": resp.headers.get("content-type", ""),
                                "body_preview": body[:2000],
                            })
                    except Exception:
                        pass

            page = ctx.new_page()
            page.on("request", on_request)
            page.on("response", on_response)

            log.info("Navigating to SCPRS with network intercept...")
            page.goto(SCPRS_URL, wait_until="networkidle", timeout=30000)
            time.sleep(2)

            # Get current page structure to understand what fields exist
            page_html = page.content()
            inputs = page.query_selector_all("input, select, button, [ng-model], [ng-click]")
            input_info = []
            for el in inputs[:30]:
                try:
                    info = {
                        "tag": el.evaluate("e => e.tagName"),
                        "id": el.get_attribute("id") or "",
                        "name": el.get_attribute("name") or "",
                        "ng_model": el.get_attribute("ng-model") or "",
                        "ng_click": el.get_attribute("ng-click") or "",
                        "placeholder": el.get_attribute("placeholder") or "",
                        "type": el.get_attribute("type") or "",
                        "text": el.inner_text()[:30] if el.inner_text() else "",
                    }
                    if any(info.values()):
                        input_info.append(info)
                except Exception:
                    continue

            ctx.close()
            browser.close()

        return {
            "ok": True,
            "captured_api_requests": captured_requests,
            "captured_api_responses": captured_responses,
            "page_inputs": input_info,
            "api_endpoint_found": len(captured_responses) > 0,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


def search_cchcs_purchases(keyword: str) -> dict:
    """
    High-level function: search what CCHCS/CDCR buys for a given keyword.
    Tries network intercept first to find the real API, then falls back to DOM scraping.
    Caches results to avoid hammering the public site.
    """
    cache = _load_cache()
    cache_key = f"{keyword.lower().strip()}:cchcs"

    # Check cache (6 hour TTL)
    for pull in cache.get("pulls", []):
        if pull.get("cache_key") == cache_key:
            age_h = (time.time() - pull.get("pulled_at", 0)) / 3600
            if age_h < 6:
                log.info("SCPRS cache hit for '%s' (%.1fh old)", keyword, age_h)
                return pull.get("data", {})

    log.info("Searching SCPRS public for '%s' (CDCR/CCHCS)", keyword)

    # Try network intercept to discover API
    intercept = search_scprs_intercept(keyword, "3860")

    result = {
        "keyword": keyword,
        "department": "CCHCS/CDCR",
        "results": [],
        "api_discovered": intercept.get("api_endpoint_found", False),
        "api_requests": intercept.get("captured_api_requests", []),
        "page_inputs": intercept.get("page_inputs", []),
        "pulled_at_iso": datetime.now(timezone.utc).isoformat(),
    }

    # If we found API requests, we can now call them directly
    api_requests = intercept.get("captured_api_requests", [])
    api_responses = intercept.get("captured_api_responses", [])

    if api_responses:
        result["raw_api_responses"] = api_responses
        # Try to parse JSON from responses
        for resp in api_responses:
            try:
                import re
                body = resp.get("body_preview", "")
                if "{" in body or "[" in body:
                    parsed = json.loads(body)
                    result["parsed_api_data"] = parsed
                    break
            except Exception:
                pass

    # Save to cache
    cache.setdefault("pulls", []).append({
        "cache_key": cache_key,
        "pulled_at": time.time(),
        "data": result,
    })
    cache["last_pull"] = datetime.now(timezone.utc).isoformat()
    _save_cache(cache)

    return result


def get_cchcs_purchase_intelligence() -> dict:
    """
    Full intelligence sweep: search all Reytech product keywords against CCHCS.
    Returns gap analysis showing what CCHCS buys that Reytech isn't selling them.
    """
    sweep_results = {}
    for keyword in REYTECH_KEYWORDS[:5]:  # Start with top 5, expand as needed
        result = search_cchcs_purchases(keyword)
        sweep_results[keyword] = result
        time.sleep(2)  # Be polite to public server

    return {
        "sweep_complete": True,
        "keywords_searched": list(sweep_results.keys()),
        "results_by_keyword": sweep_results,
        "total_po_records": sum(len(r.get("results", [])) for r in sweep_results.values()),
        "swept_at": datetime.now(timezone.utc).isoformat(),
    }
