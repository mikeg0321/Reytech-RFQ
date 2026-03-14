"""
usaspending_agent.py — Federal procurement data from USASpending.gov API.

No authentication required. Free, public REST API.
Returns federal contract awards normalized to scprs_po_master schema.

Usage:
    from src.agents.usaspending_agent import search_awards, pull_reytech_federal
    awards = search_awards(["medical supply", "surgical instrument"])
    reytech = pull_reytech_federal()
"""

import os
import json
import time
import logging
import hashlib
from datetime import datetime, timezone, timedelta

log = logging.getLogger("usaspending")

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

BASE_URL = "https://api.usaspending.gov/api/v2"

# NAICS codes relevant to Reytech's business
REYTECH_NAICS = {
    "339112": "Surgical and Medical Instruments",
    "339113": "Surgical Appliance and Supplies",
    "423450": "Medical Equipment Merchant Wholesale",
    "423490": "Other Professional Equipment Wholesale",
    "339999": "All Other Miscellaneous Manufacturing",
    "424210": "Drugs and Druggists Sundries",
    "561210": "Facilities Support Services",
}

# Keywords that match Reytech's product catalog
REYTECH_KEYWORDS = [
    "medical supply", "surgical supply", "nitrile glove",
    "wound care", "adult brief", "incontinence",
    "N95 respirator", "PPE", "safety glasses",
    "restraint", "sharps container", "gauze",
    "catheter", "first aid", "exam glove",
]

# Rate limit: max 10 requests/minute
_last_request_time = 0
_REQUEST_INTERVAL = 6.0  # seconds between requests


def _rate_limit():
    """Enforce API rate limiting."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _REQUEST_INTERVAL:
        time.sleep(_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def search_awards(keywords: list, agency_codes: list = None,
                  date_range_days: int = 730, limit: int = 100,
                  page: int = 1) -> list:
    """Search USASpending for contract awards.

    Input: keywords (product descriptions), optional agency codes, date range
    Output: list of award dicts with fields matching scprs_po_master schema
    Side effects: HTTP requests to USASpending API
    """
    if not HAS_REQUESTS:
        log.warning("requests library not available")
        return []

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=date_range_days)).strftime("%Y-%m-%d")

    filters = {
        "keywords": keywords,
        "date_type": "action_date",
        "date_range": {"start_date": start_date, "end_date": end_date},
        "award_type_codes": ["A", "B", "C", "D"],  # Contract types
    }
    if agency_codes:
        filters["agency_codes"] = agency_codes

    body = {
        "filters": filters,
        "fields": [
            "Award ID", "Recipient Name", "Awarding Agency",
            "Total Obligated Amount", "Description",
            "Place of Performance State Code",
            "Period of Performance Current End Date",
            "Start Date", "Award Type",
        ],
        "page": page,
        "limit": limit,
        "sort": "Total Obligated Amount",
        "order": "desc",
    }

    _rate_limit()
    try:
        resp = requests.post(f"{BASE_URL}/search/spending_by_award/",
                             json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        log.info("USASpending search: %d results (page %d, keywords=%s)",
                 len(results), page, keywords[:3])
        return [normalize_to_po_master(r) for r in results]
    except Exception as e:
        log.error("USASpending search failed: %s", e)
        return []


def search_recipient(recipient_name: str) -> list:
    """Search for a recipient (vendor) by name.

    Input: vendor name to search
    Output: list of recipient dicts with DUNS/UEI
    """
    if not HAS_REQUESTS:
        return []

    _rate_limit()
    try:
        resp = requests.get(
            f"{BASE_URL}/recipient/search/",
            params={"keyword": recipient_name},
            timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        log.error("Recipient search failed: %s", e)
        return []


def get_recipient_awards(recipient_name: str,
                         date_range_days: int = 730) -> list:
    """Get all federal awards for a specific vendor.

    Input: recipient_name, date range in days
    Output: list of award dicts normalized to po_master schema
    """
    return search_awards(
        keywords=[recipient_name],
        date_range_days=date_range_days,
        limit=100)


def pull_federal_category(keywords: list,
                          naics_codes: list = None,
                          date_range_days: int = 730,
                          max_pages: int = 10) -> list:
    """Pull all federal awards in a product category with pagination.

    Input: keywords, optional NAICS codes, date range, max pages
    Output: list of all award dicts (paginated)
    """
    all_results = []
    for page in range(1, max_pages + 1):
        results = search_awards(keywords, date_range_days=date_range_days,
                                limit=100, page=page)
        if not results:
            break
        all_results.extend(results)
        log.info("Federal category pull: page %d, %d results so far",
                 page, len(all_results))
    return all_results


def normalize_to_po_master(award: dict, source: str = "usaspending") -> dict:
    """Map USASpending award fields to scprs_po_master schema.

    Sets state='federal', source_system='usaspending', jurisdiction='federal'.
    """
    award_id = award.get("Award ID") or award.get("generated_internal_id", "")
    description = award.get("Description", "")
    recipient = award.get("Recipient Name", "")
    agency = award.get("Awarding Agency", "")
    amount = award.get("Total Obligated Amount", 0)
    state_code = award.get("Place of Performance State Code", "")
    start_date = award.get("Start Date", "")
    end_date = award.get("Period of Performance Current End Date", "")

    # Generate stable ID
    row_id = "USG-" + hashlib.md5(
        f"{award_id}:{recipient}:{amount}".encode()
    ).hexdigest()[:12]

    return {
        "id": row_id,
        "po_number": award_id,
        "dept_name": agency,
        "institution": agency,
        "supplier": recipient,
        "supplier_id": "",
        "status": "Active",
        "start_date": start_date,
        "end_date": end_date,
        "grand_total": float(amount) if amount else 0,
        "buyer_name": "",
        "buyer_email": "",
        "search_term": description[:100] if description else "",
        "agency_key": "federal",
        "state": "federal",
        "jurisdiction": "federal",
        "source_system": "usaspending",
        # Line item (single item per award for now)
        "description": description[:500] if description else "",
        "unit_price": float(amount) if amount else 0,
    }


def pull_reytech_federal(days_back: int = 730) -> dict:
    """Pull all federal awards for Reytech and related keywords.

    Returns: {ok, reytech_awards, category_awards, total}
    """
    results = {
        "ok": True,
        "reytech_awards": [],
        "category_awards": [],
        "total": 0,
    }

    # Search for Reytech by name
    log.info("Searching federal awards for 'reytech'...")
    reytech = get_recipient_awards("reytech", days_back)
    results["reytech_awards"] = reytech
    results["total"] += len(reytech)
    log.info("Found %d Reytech federal awards", len(reytech))

    # Search by product categories (first 5 keywords to stay within rate limit)
    for kw in REYTECH_KEYWORDS[:5]:
        log.info("Searching federal awards for '%s'...", kw)
        awards = search_awards([kw], date_range_days=days_back, limit=50)
        results["category_awards"].extend(awards)
        results["total"] += len(awards)

    log.info("Federal pull complete: %d total awards", results["total"])
    return results
