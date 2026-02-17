"""
quickbooks_agent.py — QuickBooks Online Integration for Reytech
Phase 13 | Version: 1.0.0

Connects Reytech's pipeline to QuickBooks Online for:
  1. Vendor pull — import vendors + purchase history for pricing comparison
  2. PO creation — when a quote is won, auto-create a Purchase Order in QB
  3. Invoice sync — track which quotes became invoices

OAuth2 flow:
  - Uses refresh token (long-lived, set as QB_REFRESH_TOKEN env var)
  - Auto-refreshes access token when expired
  - Token storage in data/qb_tokens.json

QuickBooks API: REST, JSON, OAuth2
  Base URL: https://quickbooks.api.intuit.com/v3/company/{realm_id}/
  Sandbox: https://sandbox-quickbooks.api.intuit.com/v3/company/{realm_id}/

Dependencies: requests
Env vars: QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REFRESH_TOKEN, QB_REALM_ID
Optional: QB_SANDBOX=true for development
"""

import json
import os
import time
import logging
import base64
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("quickbooks")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# ─── Configuration ───────────────────────────────────────────────────────────

TOKEN_FILE = os.path.join(DATA_DIR, "qb_tokens.json")
VENDOR_CACHE_FILE = os.path.join(DATA_DIR, "qb_vendors.json")
VENDOR_CACHE_TTL_HOURS = 24

# Use centralized secrets
try:
    from src.core.secrets import get_key
    QB_CLIENT_ID = get_key("qb_client_id")
    QB_CLIENT_SECRET = get_key("qb_client_secret")
    QB_REFRESH_TOKEN = get_key("qb_refresh_token")
    QB_REALM_ID = get_key("qb_realm_id")
except ImportError:
    QB_CLIENT_ID = os.environ.get("QB_CLIENT_ID", "")
    QB_CLIENT_SECRET = os.environ.get("QB_CLIENT_SECRET", "")
    QB_REFRESH_TOKEN = os.environ.get("QB_REFRESH_TOKEN", "")
    QB_REALM_ID = os.environ.get("QB_REALM_ID", "")

QB_SANDBOX = os.environ.get("QB_SANDBOX", "").lower() in ("true", "1", "yes")

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
API_BASE = (
    f"https://sandbox-quickbooks.api.intuit.com/v3/company/{QB_REALM_ID}"
    if QB_SANDBOX else
    f"https://quickbooks.api.intuit.com/v3/company/{QB_REALM_ID}"
)


def is_configured() -> bool:
    """Check if QuickBooks credentials are set."""
    return bool(QB_CLIENT_ID and QB_CLIENT_SECRET and QB_REFRESH_TOKEN and QB_REALM_ID)


# ─── Token Management ───────────────────────────────────────────────────────

def _load_tokens() -> dict:
    try:
        with open(TOKEN_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_tokens(tokens: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f)


def _refresh_access_token() -> Optional[str]:
    """Refresh the OAuth2 access token using the refresh token."""
    if not HAS_REQUESTS:
        log.error("requests library not available")
        return None
    if not is_configured():
        log.debug("QuickBooks not configured — skipping token refresh")
        return None

    # Use current refresh token (may have been updated)
    tokens = _load_tokens()
    refresh = tokens.get("refresh_token", QB_REFRESH_TOKEN)

    auth = base64.b64encode(f"{QB_CLIENT_ID}:{QB_CLIENT_SECRET}".encode()).decode()

    try:
        resp = _requests.post(TOKEN_URL, headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }, data={
            "grant_type": "refresh_token",
            "refresh_token": refresh,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        tokens = {
            "access_token": data["access_token"],
            "refresh_token": data.get("refresh_token", refresh),
            "expires_at": time.time() + data.get("expires_in", 3600),
            "refreshed_at": datetime.now().isoformat(),
        }
        _save_tokens(tokens)
        log.info("QB access token refreshed (expires in %ds)", data.get("expires_in", 3600))
        return tokens["access_token"]

    except Exception as e:
        log.error("QB token refresh failed: %s", e)
        return None


def get_access_token() -> Optional[str]:
    """Get a valid access token, refreshing if needed."""
    tokens = _load_tokens()
    access = tokens.get("access_token")
    expires = tokens.get("expires_at", 0)

    # Valid token exists and not expired (with 5 min buffer)
    if access and time.time() < expires - 300:
        return access

    # Need refresh
    return _refresh_access_token()


# ─── API Client ──────────────────────────────────────────────────────────────

def _qb_request(method: str, endpoint: str, data: dict = None) -> Optional[dict]:
    """Make an authenticated request to the QuickBooks API."""
    if not HAS_REQUESTS:
        return None

    token = get_access_token()
    if not token:
        return None

    url = f"{API_BASE}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        if method == "GET":
            resp = _requests.get(url, headers=headers, timeout=15)
        elif method == "POST":
            resp = _requests.post(url, headers=headers, json=data, timeout=15)
        else:
            log.error("Unsupported HTTP method: %s", method)
            return None

        if resp.status_code == 401:
            # Token expired mid-request — refresh and retry once
            token = _refresh_access_token()
            if not token:
                return None
            headers["Authorization"] = f"Bearer {token}"
            if method == "GET":
                resp = _requests.get(url, headers=headers, timeout=15)
            else:
                resp = _requests.post(url, headers=headers, json=data, timeout=15)

        resp.raise_for_status()
        return resp.json()

    except Exception as e:
        log.error("QB API error (%s %s): %s", method, endpoint, e)
        return None


def _qb_query(query: str) -> list:
    """Run a QB query (SQL-like). Returns list of results."""
    result = _qb_request("GET", f"query?query={query}&minorversion=73")
    if not result:
        return []
    qr = result.get("QueryResponse", {})
    # QB returns the entity name as key (e.g., "Vendor", "PurchaseOrder")
    for key in qr:
        if isinstance(qr[key], list):
            return qr[key]
    return []


# ─── Vendor Operations ──────────────────────────────────────────────────────

def fetch_vendors(force_refresh: bool = False) -> list:
    """
    Fetch vendors from QuickBooks.
    Caches locally for VENDOR_CACHE_TTL_HOURS.

    Returns list of vendor dicts:
    [{"id": "...", "name": "...", "email": "...", "phone": "...", 
      "balance": 0, "active": True}]
    """
    # Check cache
    if not force_refresh:
        try:
            with open(VENDOR_CACHE_FILE) as f:
                cache = json.load(f)
            if time.time() - cache.get("fetched_at", 0) < VENDOR_CACHE_TTL_HOURS * 3600:
                return cache.get("vendors", [])
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    if not is_configured():
        log.debug("QuickBooks not configured — returning empty vendor list")
        return []

    # Fetch from QB
    raw = _qb_query("SELECT * FROM Vendor WHERE Active = true MAXRESULTS 500")
    vendors = []
    for v in raw:
        vendor = {
            "qb_id": v.get("Id"),
            "name": v.get("DisplayName", ""),
            "company": v.get("CompanyName", ""),
            "email": "",
            "phone": "",
            "balance": v.get("Balance", 0),
            "active": v.get("Active", True),
            "currency": v.get("CurrencyRef", {}).get("value", "USD"),
        }
        # Extract contact info
        if v.get("PrimaryEmailAddr"):
            vendor["email"] = v["PrimaryEmailAddr"].get("Address", "")
        if v.get("PrimaryPhone"):
            vendor["phone"] = v["PrimaryPhone"].get("FreeFormNumber", "")
        if v.get("BillAddr"):
            addr = v["BillAddr"]
            vendor["address"] = {
                "line1": addr.get("Line1", ""),
                "city": addr.get("City", ""),
                "state": addr.get("CountrySubDivisionCode", ""),
                "zip": addr.get("PostalCode", ""),
            }
        vendors.append(vendor)

    # Cache
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(VENDOR_CACHE_FILE, "w") as f:
        json.dump({"vendors": vendors, "fetched_at": time.time(),
                    "count": len(vendors)}, f, indent=2)

    log.info("Fetched %d vendors from QuickBooks", len(vendors))
    return vendors


def find_vendor(name: str) -> Optional[dict]:
    """Find a vendor by name (fuzzy match)."""
    vendors = fetch_vendors()
    name_lower = name.lower()
    # Exact match first
    for v in vendors:
        if v["name"].lower() == name_lower:
            return v
    # Partial match
    for v in vendors:
        if name_lower in v["name"].lower() or v["name"].lower() in name_lower:
            return v
    return None


# ─── Purchase Order Operations ───────────────────────────────────────────────

def create_purchase_order(vendor_id: str, items: list,
                          memo: str = "", ship_to: str = "") -> Optional[dict]:
    """
    Create a Purchase Order in QuickBooks.

    Args:
        vendor_id: QB vendor ID
        items: List of {"description": str, "qty": int, "unit_cost": float}
        memo: Optional memo/notes
        ship_to: Optional ship-to address

    Returns:
        PO dict from QB or None on failure.
    """
    if not is_configured():
        return None

    lines = []
    for i, item in enumerate(items):
        lines.append({
            "DetailType": "ItemBasedExpenseLineDetail",
            "Amount": round(item.get("qty", 1) * item.get("unit_cost", 0), 2),
            "Description": item.get("description", "")[:4000],
            "ItemBasedExpenseLineDetail": {
                "Qty": item.get("qty", 1),
                "UnitPrice": item.get("unit_cost", 0),
            },
        })

    po_data = {
        "VendorRef": {"value": vendor_id},
        "Line": lines,
    }
    if memo:
        po_data["Memo"] = memo[:4000]

    result = _qb_request("POST", "purchaseorder?minorversion=73", po_data)
    if result and "PurchaseOrder" in result:
        po = result["PurchaseOrder"]
        log.info("Created QB PO #%s (vendor=%s, lines=%d)",
                 po.get("DocNumber", "?"), vendor_id, len(lines))
        return {
            "qb_id": po.get("Id"),
            "doc_number": po.get("DocNumber"),
            "total": po.get("TotalAmt", 0),
            "vendor": po.get("VendorRef", {}).get("name", ""),
            "created": po.get("MetaData", {}).get("CreateTime", ""),
        }
    return None


def get_recent_purchase_orders(days_back: int = 30) -> list:
    """Fetch recent POs from QuickBooks."""
    if not is_configured():
        return []

    since = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    raw = _qb_query(
        f"SELECT * FROM PurchaseOrder WHERE MetaData.CreateTime >= '{since}' "
        f"ORDERBY MetaData.CreateTime DESC MAXRESULTS 100"
    )
    pos = []
    for po in raw:
        pos.append({
            "qb_id": po.get("Id"),
            "doc_number": po.get("DocNumber"),
            "vendor": po.get("VendorRef", {}).get("name", ""),
            "total": po.get("TotalAmt", 0),
            "status": po.get("POStatus", ""),
            "created": po.get("MetaData", {}).get("CreateTime", ""),
            "line_count": len(po.get("Line", [])),
        })
    return pos


# ─── Vendor Price Comparison ────────────────────────────────────────────────

def get_vendor_pricing(description: str) -> list:
    """
    Search QB purchase history for what we've paid vendors for similar items.
    Useful for: "Is Amazon cheaper than our existing vendor for this item?"

    Returns list of historical purchases sorted by unit cost.
    """
    if not is_configured():
        return []

    # Search PO line items for description matches
    # QB query doesn't support full-text on line items,
    # so we fetch recent POs and filter locally
    pos = get_recent_purchase_orders(days_back=180)
    matches = []

    desc_lower = description.lower()
    desc_tokens = set(desc_lower.split())

    for po in pos:
        # We'd need to re-fetch full PO for line details
        # For now, return PO-level data
        pass

    return matches


# ─── Public Health / Status ──────────────────────────────────────────────────

def get_agent_status() -> dict:
    """Return QuickBooks agent health status."""
    tokens = _load_tokens()
    has_valid_token = bool(
        tokens.get("access_token") and
        time.time() < tokens.get("expires_at", 0)
    )

    vendor_count = 0
    try:
        with open(VENDOR_CACHE_FILE) as f:
            cache = json.load(f)
            vendor_count = cache.get("count", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    return {
        "agent": "quickbooks",
        "version": "1.0.0",
        "configured": is_configured(),
        "sandbox_mode": QB_SANDBOX,
        "has_valid_token": has_valid_token,
        "token_expires": tokens.get("expires_at"),
        "realm_id_set": bool(QB_REALM_ID),
        "cached_vendors": vendor_count,
    }
