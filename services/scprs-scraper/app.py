"""
SCPRS Scraper Microservice — Playwright-based SCPRS data extraction.

Stateless service that runs Chromium for scraping PeopleSoft FI$Cal and
public CaleProcure. Returns structured JSON. No database, no state.

The main Reytech RFQ app calls this via HTTP instead of running
Playwright/Chromium in its own container (saves 500MB+).
"""
import os
import logging
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("scprs-scraper")

app = Flask(__name__)

# Auth via shared secret
SCRAPER_SECRET = os.environ.get("SCRAPER_SECRET", "")


def _check_auth():
    if SCRAPER_SECRET:
        token = request.headers.get("X-Scraper-Secret", "")
        if token != SCRAPER_SECRET:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
    return None


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "scprs-scraper"})


@app.route("/scrape/details", methods=["POST"])
def scrape_details():
    """Scrape FI$Cal SCPRS detail pages for a supplier."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err
    try:
        from scprs_browser import scrape_details as _scrape
        data = request.get_json(silent=True) or {}
        result = _scrape(
            supplier_name=data.get("supplier_name", "reytech"),
            from_date=data.get("from_date", ""),
            max_rows=data.get("max_rows", 200),
        )
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("scrape/details failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


@app.route("/scrape/po", methods=["POST"])
def scrape_po():
    """Scrape a single PO detail from FI$Cal."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err
    try:
        from scprs_browser import scrape_po_detail as _scrape
        data = request.get_json(silent=True) or {}
        po_number = data.get("po_number", "")
        if not po_number:
            return jsonify({"ok": False, "error": "po_number required"}), 400
        result = _scrape(po_number)
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("scrape/po failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


@app.route("/scrape/exhaustive", methods=["POST"])
def scrape_exhaustive():
    """Run an exhaustive FI$Cal scrape with date range."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err
    try:
        from scprs_browser import _scrape_with_retry
        data = request.get_json(silent=True) or {}
        result = _scrape_with_retry(
            search_params={
                "supplier_name": data.get("supplier_name", ""),
                "from_date": data.get("from_date", ""),
                "to_date": data.get("to_date", ""),
                "description": data.get("description", ""),
            },
            seen_pos=set(data.get("seen_pos", [])),
            max_rows=data.get("max_rows", 500),
        )
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("scrape/exhaustive failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


@app.route("/scrape/public-search", methods=["POST"])
def public_search():
    """Search the public CaleProcure SCPRS site."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err
    try:
        from scprs_public_search import search_scprs_public as _search
        data = request.get_json(silent=True) or {}
        result = _search(
            keyword=data.get("keyword", ""),
            department_code=data.get("department_code", ""),
            max_results=data.get("max_results", 50),
        )
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("scrape/public-search failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


@app.route("/scrape/intercept", methods=["POST"])
def intercept_search():
    """Search SCPRS via network interception (discovers API endpoints)."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err
    try:
        from scprs_public_search import search_scprs_intercept as _search
        data = request.get_json(silent=True) or {}
        result = _search(
            keyword=data.get("keyword", ""),
            department_code=data.get("department_code", "3860"),
        )
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("scrape/intercept failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:500]}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    app.run(host="0.0.0.0", port=port)
