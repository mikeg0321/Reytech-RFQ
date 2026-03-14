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
import re
import time
import logging
import base64
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("quickbooks")

# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Full access to live CRM, quotes, revenue, price history, voice calls from SQLite.
try:
    from src.core.agent_context import (
        get_context, format_context_for_agent,
        get_contact_by_agency, get_best_price,
    )
    HAS_AGENT_CTX = True
except ImportError:
    HAS_AGENT_CTX = False
    def get_context(**kw): return {}
    def format_context_for_agent(c, **kw): return ""
    def get_contact_by_agency(a): return []
    def get_best_price(d): return None

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


def _get_realm_id() -> str:
    """Get realm_id from env, module constant, or saved tokens (dynamic)."""
    rid = os.environ.get("QB_REALM_ID", "") or QB_REALM_ID
    if rid:
        return rid
    tokens = _load_tokens()
    return tokens.get("realm_id", "")


def _get_refresh_token() -> str:
    """Get refresh token from env, module constant, or saved tokens (dynamic)."""
    rt = os.environ.get("QB_REFRESH_TOKEN", "") or QB_REFRESH_TOKEN
    if rt:
        return rt
    tokens = _load_tokens()
    return tokens.get("refresh_token", "")


def _get_api_base() -> str:
    """Build API base URL dynamically using current realm_id."""
    rid = _get_realm_id()
    if QB_SANDBOX:
        return f"https://sandbox-quickbooks.api.intuit.com/v3/company/{rid}"
    return f"https://quickbooks.api.intuit.com/v3/company/{rid}"


def is_configured() -> bool:
    """Check if QuickBooks credentials are set (checks env + token file)."""
    has_creds = bool(QB_CLIENT_ID or os.environ.get("QB_CLIENT_ID", ""))
    has_secret = bool(QB_CLIENT_SECRET or os.environ.get("QB_CLIENT_SECRET", ""))
    has_refresh = bool(_get_refresh_token())
    has_realm = bool(_get_realm_id())
    return has_creds and has_secret and has_refresh and has_realm


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

    client_id = QB_CLIENT_ID or os.environ.get("QB_CLIENT_ID", "")
    client_secret = QB_CLIENT_SECRET or os.environ.get("QB_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        log.debug("QuickBooks client credentials not set — skipping token refresh")
        return None

    # Use current refresh token from file or env (dynamic)
    refresh = _get_refresh_token()
    if not refresh:
        log.debug("No refresh token available — skipping token refresh")
        return None

    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

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

        new_access = data.get("access_token")
        if not new_access:
            log.error("QB token refresh: response missing access_token key")
            return None

        # Merge with existing tokens to preserve realm_id, connected_at, etc.
        existing = _load_tokens()
        existing.update({
            "access_token": new_access,
            "refresh_token": data.get("refresh_token", refresh),
            "expires_at": time.time() + data.get("expires_in", 3600),
            "refreshed_at": datetime.now().isoformat(),
        })
        _save_tokens(existing)
        log.info("QB access token refreshed (expires in %ds)", data.get("expires_in", 3600))
        return new_access

    except _requests.exceptions.HTTPError as e:
        log.error("QB token refresh HTTP error: %s (status %s)", e, getattr(e.response, 'status_code', '?'))
        return None
    except _requests.exceptions.ConnectionError as e:
        log.error("QB token refresh connection failed: %s", e)
        return None
    except Exception as e:
        log.error("QB token refresh failed: %s — %s", type(e).__name__, e)
        return None


def get_access_token() -> Optional[str]:
    """Get a valid access token, refreshing if needed."""
    stored = _load_tokens()
    access = stored.get("access_token")
    expires = stored.get("expires_at", 0)

    # Valid token exists and not expired (with 5 min buffer)
    if access and time.time() < expires - 300:
        return access

    # Need refresh
    try:
        return _refresh_access_token()
    except Exception as e:
        log.error("QB get_access_token: refresh crashed — %s: %s", type(e).__name__, e)
        return None


# ─── API Client ──────────────────────────────────────────────────────────────

def _qb_request(method: str, endpoint: str, data: dict = None) -> Optional[dict]:
    """Make an authenticated request to the QuickBooks API."""
    if not HAS_REQUESTS:
        return None

    token = get_access_token()
    if not token:
        log.error("QB: No access token available")
        return None

    api_base = _get_api_base()
    if not _get_realm_id():
        log.error("QB: No realm_id — cannot make API calls")
        return None

    url = f"{api_base}/{endpoint}"
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
        vn = (v.get("name") or v.get("CompanyName") or v.get("DisplayName") or "").lower()
        if vn == name_lower:
            return v
    # Partial match
    for v in vendors:
        vn = (v.get("name") or v.get("CompanyName") or v.get("DisplayName") or "").lower()
        if name_lower in vn or vn in name_lower:
            return v
    return None


def create_vendor(name: str, email: str = "", phone: str = "") -> Optional[dict]:
    """Create a new vendor in QuickBooks.

    Returns the created vendor dict with Id, or None on failure.
    """
    if not is_configured():
        return None
    if not name:
        return None

    vendor_data = {
        "DisplayName": name,
        "CompanyName": name,
    }
    if email:
        vendor_data["PrimaryEmailAddr"] = {"Address": email}
    if phone:
        vendor_data["PrimaryPhone"] = {"FreeFormNumber": phone}

    result = _qb_request("POST", "vendor", vendor_data)
    if result and "Vendor" in result:
        v = result["Vendor"]
        log.info("Created QB vendor: %s (ID: %s)", name, v.get("Id"))
        # Clear cache so new vendor shows in dropdowns
        try:
            os.remove(VENDOR_CACHE_FILE)
        except Exception:
            pass
        return {
            "Id": v.get("Id"),
            "CompanyName": v.get("CompanyName", name),
            "DisplayName": v.get("DisplayName", name),
            "PrimaryEmailAddr": v.get("PrimaryEmailAddr", {}),
        }
    log.error("Failed to create QB vendor '%s': %s", name, result)
    return None


# ─── Purchase Order Operations ───────────────────────────────────────────────

def create_purchase_order(vendor_id: str, items: list,
                          memo: str = "", ship_to: str = "",
                          po_number: str = "") -> Optional[dict]:
    """
    Create a Purchase Order in QuickBooks.

    Args:
        vendor_id: QB vendor ID (required)
        items: List of {"description": str, "qty": int, "unit_cost": float}
        memo: Optional memo/notes
        ship_to: Optional ship-to address
        po_number: Optional custom PO/reference number

    Returns:
        PO dict with qb_id, doc_number, total. None on failure.
    """
    if not is_configured():
        log.warning("QB not configured - skipping PO creation")
        return None
    if not vendor_id:
        log.warning("QB PO skipped: no vendor_id provided")
        return None

    lines = []
    for i, item in enumerate(items):
        qty = item.get("qty", 0) or item.get("quantity", 0) or 1
        cost = item.get("unit_cost", 0) or item.get("unit_price", 0) or 0
        cost = float(cost)
        qty = int(qty)
        if cost <= 0:
            continue
        lines.append({
            "DetailType": "AccountBasedExpenseLineDetail",
            "Amount": round(qty * cost, 2),
            "Description": item.get("description", "")[:4000],
            "AccountBasedExpenseLineDetail": {"AccountRef": {"value": "1"},
                "Qty": qty,
                "UnitPrice": cost,
            },
        })

    if not lines:
        log.warning("QB PO skipped: no priced line items")
        return None

    po_data = {
        "VendorRef": {"value": vendor_id},
        "Line": lines,
    }
    if memo:
        po_data["Memo"] = memo[:4000]
    if po_number:
        po_data["DocNumber"] = po_number[:20]
    if ship_to:
        po_data["ShipAddr"] = {"Line1": ship_to[:100]}

    result = _qb_request("POST", "purchaseorder?minorversion=73", po_data)
    if result and "PurchaseOrder" in result:
        po = result["PurchaseOrder"]
        log.info("Created QB PO #%s (vendor=%s, lines=%d, total=$%.2f)",
                 po.get("DocNumber", "?"), vendor_id, len(lines), po.get("TotalAmt", 0))
        return {
            "qb_id": po.get("Id"),
            "doc_number": po.get("DocNumber"),
            "total": po.get("TotalAmt", 0),
            "vendor": po.get("VendorRef", {}).get("name", ""),
            "created": po.get("MetaData", {}).get("CreateTime", ""),
        }
    log.error("QB PO creation failed: %s", result)
    return None


def get_recent_purchase_orders(days_back: int = 30) -> list:
    """Fetch recent POs from QuickBooks."""
    if not is_configured():
        return []

    since = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    # Validate date format to prevent any injection via days_back manipulation
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", since):
        log.error("Invalid date format for QB PO query: %s", since)
        return []
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
    """Search QB purchase history for what we've paid vendors for similar items."""
    if not is_configured():
        return []
    pos = get_recent_purchase_orders(days_back=180)
    return []  # TODO: line-item search requires full PO refetch


# ─── Invoice Operations ────────────────────────────────────────────────────

INVOICE_CACHE_FILE = os.path.join(DATA_DIR, "qb_invoices_cache.json")

def fetch_invoices(status: str = "all", days_back: int = 90,
                    force_refresh: bool = False) -> list:
    """
    Fetch invoices from QuickBooks.

    Args:
        status: "all", "open" (unpaid), "overdue", "paid"
        days_back: how far back to look
        force_refresh: bypass cache

    Returns list of invoice dicts with line items.
    """
    if not is_configured():
        return []

    # Cache check
    if not force_refresh:
        try:
            with open(INVOICE_CACHE_FILE) as f:
                cache = json.load(f)
            if time.time() - cache.get("fetched_at", 0) < 3600:  # 1hr cache
                invoices = cache.get("invoices", [])
                return _filter_invoices(invoices, status)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", cutoff):
        log.error("Invalid date format for QB Invoice query: %s", cutoff)
        return []
    query = f"SELECT * FROM Invoice WHERE TxnDate >= '{cutoff}' ORDERBY TxnDate DESC MAXRESULTS 200"
    raw = _qb_query(query)

    invoices = []
    for inv in raw:
        due_date = inv.get("DueDate", "")
        balance = float(inv.get("Balance", 0))
        total = float(inv.get("TotalAmt", 0))

        # Determine status
        if balance == 0:
            inv_status = "paid"
        elif due_date and due_date < datetime.now().strftime("%Y-%m-%d") and balance > 0:
            inv_status = "overdue"
        else:
            inv_status = "open"

        # Parse line items
        lines = []
        for line in inv.get("Line", []):
            if line.get("DetailType") == "SalesItemLineDetail":
                detail = line.get("SalesItemLineDetail", {})
                lines.append({
                    "description": line.get("Description", ""),
                    "qty": detail.get("Qty", 0),
                    "unit_price": float(detail.get("UnitPrice", 0)),
                    "amount": float(line.get("Amount", 0)),
                    "item_ref": detail.get("ItemRef", {}).get("name", ""),
                })

        customer_name = inv.get("CustomerRef", {}).get("name", "")
        invoices.append({
            "id": inv.get("Id"),
            "doc_number": inv.get("DocNumber", ""),
            "customer_name": customer_name,
            "customer_id": inv.get("CustomerRef", {}).get("value", ""),
            "txn_date": inv.get("TxnDate", ""),
            "due_date": due_date,
            "total": total,
            "balance": balance,
            "status": inv_status,
            "email_status": inv.get("EmailStatus", ""),
            "line_items": lines,
            "po_number": inv.get("CustomField", [{}])[0].get("StringValue", "") if inv.get("CustomField") else "",
            "memo": inv.get("CustomerMemo", {}).get("value", "") if inv.get("CustomerMemo") else "",
        })

    # Cache
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(INVOICE_CACHE_FILE, "w") as f:
        json.dump({"invoices": invoices, "fetched_at": time.time(), "count": len(invoices)}, f, indent=2)

    log.info("Fetched %d invoices from QB", len(invoices))
    return _filter_invoices(invoices, status)


def _filter_invoices(invoices: list, status: str) -> list:
    if status == "all":
        return invoices
    return [i for i in invoices if i.get("status") == status]


def get_invoice_summary() -> dict:
    """Get aggregated invoice metrics."""
    invoices = fetch_invoices(status="all")
    open_inv = [i for i in invoices if i["status"] == "open"]
    overdue_inv = [i for i in invoices if i["status"] == "overdue"]
    paid_inv = [i for i in invoices if i["status"] == "paid"]

    return {
        "total_invoices": len(invoices),
        "open_count": len(open_inv),
        "overdue_count": len(overdue_inv),
        "paid_count": len(paid_inv),
        "open_total": sum(i["balance"] for i in open_inv),
        "overdue_total": sum(i["balance"] for i in overdue_inv),
        "paid_total": sum(i["total"] for i in paid_inv),
        "total_receivable": sum(i["balance"] for i in invoices if i["balance"] > 0),
        "avg_invoice": sum(i["total"] for i in invoices) / len(invoices) if invoices else 0,
        "oldest_overdue": min((i["due_date"] for i in overdue_inv), default=""),
    }


def create_invoice(customer_id: str, items: list, po_number: str = "",
                    memo: str = "", doc_number: str = "", terms: str = "",
                    sales_rep: str = "") -> Optional[dict]:
    """
    Create an invoice in QuickBooks — let QB handle formatting, tax, terms.
    
    We send: customer, line items (with MFG# + UOM baked into description), PO ref.
    QB handles: invoice #, date, tax, terms, bill-to, logo, formatting.
    """
    if not is_configured():
        log.warning("QB not configured - skipping invoice creation")
        return None
    if not customer_id:
        log.warning("QB invoice skipped: no customer_id")
        return None

    lines = []
    for item in items:
        qty = item.get("qty", 0) or item.get("quantity", 0) or 1
        price = item.get("unit_price", 0) or item.get("price", 0) or 0
        price = float(price)
        qty = int(qty)
        if price <= 0:
            continue

        # Build description: "MFG#\nFull description\nUOM: EA"
        desc_parts = []
        mfg = item.get("mfg_number", "") or item.get("part_number", "") or ""
        if mfg:
            desc_parts.append(mfg)
        desc_parts.append(item.get("description", ""))
        uom = item.get("uom", "")
        if uom and uom.upper() not in ("", "EA"):
            desc_parts.append(f"UOM: {uom.upper()}")
        
        full_desc = "\n".join(d for d in desc_parts if d)

        lines.append({
            "DetailType": "SalesItemLineDetail",
            "Amount": round(qty * price, 2),
            "Description": full_desc[:4000],
            "SalesItemLineDetail": {
                "Qty": qty,
                "UnitPrice": price,
            },
        })

    if not lines:
        log.warning("QB invoice skipped: no priced line items")
        return None

    invoice_data = {
        "CustomerRef": {"value": customer_id},
        "Line": lines,
    }
    
    # PO number as reference (shows on invoice)
    if po_number:
        invoice_data["CustomField"] = [
            {"DefinitionId": "1", "StringValue": po_number, "Type": "StringType", "Name": "P.O. Number"}
        ]
    
    if memo:
        invoice_data["CustomerMemo"] = {"value": memo}

    # Let QB auto-number unless explicit
    if doc_number:
        invoice_data["DocNumber"] = doc_number[:20]

    result = _qb_request("POST", "invoice?minorversion=73", invoice_data)
    if result and result.get("Invoice"):
        inv = result["Invoice"]
        log.info("QB Invoice created: #%s for $%s (customer=%s)",
                 inv.get("DocNumber"), inv.get("TotalAmt"), customer_id)
        return {
            "id": inv.get("Id"),
            "doc_number": inv.get("DocNumber"),
            "total": float(inv.get("TotalAmt", 0)),
            "customer": inv.get("CustomerRef", {}).get("name", ""),
            "due_date": inv.get("DueDate", ""),
            "email_status": inv.get("EmailStatus", ""),
        }
    log.error("QB invoice creation failed: %s", result)
    return None


def send_invoice_email(invoice_id: str, to_email: str = "") -> bool:
    """Tell QB to email the invoice to the customer.
    
    QB sends the formatted invoice PDF from their servers.
    If to_email is empty, QB uses the email on file for the customer.
    """
    if not is_configured() or not invoice_id:
        return False
    
    endpoint = f"invoice/{invoice_id}/send"
    if to_email:
        endpoint += f"?sendTo={to_email}"
    
    result = _qb_request("POST", endpoint)
    if result and result.get("Invoice"):
        inv = result["Invoice"]
        log.info("QB Invoice #%s emailed to %s (status: %s)",
                 inv.get("DocNumber"), to_email or "customer on file",
                 inv.get("EmailStatus"))
        return True
    log.error("QB invoice email failed for ID %s: %s", invoice_id, result)
    return False


def get_invoice_pdf(invoice_id: str) -> Optional[bytes]:
    """Download the invoice PDF from QB for local storage."""
    if not is_configured() or not invoice_id:
        return None
    
    token = get_access_token()
    if not token:
        return None
    
    base = _get_api_base()
    url = f"{base}/invoice/{invoice_id}/pdf?minorversion=73"
    
    try:
        resp = _requests.get(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/pdf",
        }, timeout=30)
        if resp.status_code == 200:
            log.info("Downloaded QB invoice PDF for ID %s (%d bytes)", invoice_id, len(resp.content))
            return resp.content
        log.error("QB invoice PDF download failed: %s %s", resp.status_code, resp.text[:200])
    except Exception as e:
        log.error("QB invoice PDF download error: %s", e)
    return None

def fetch_customers(force_refresh: bool = False) -> list:
    """Fetch customers from QuickBooks with balances."""
    if not is_configured():
        return []

    if not force_refresh:
        try:
            with open(CUSTOMER_CACHE_FILE) as f:
                cache = json.load(f)
            if time.time() - cache.get("fetched_at", 0) < 3600 * 4:  # 4hr cache
                return cache.get("customers", [])
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    raw = _qb_query("SELECT * FROM Customer MAXRESULTS 500")
    customers = []
    for c in raw:
        addr = c.get("BillAddr", {})
        customers.append({
            "id": c.get("Id"),
            "name": c.get("DisplayName", ""),
            "company": c.get("CompanyName", ""),
            "email": c.get("PrimaryEmailAddr", {}).get("Address", "") if c.get("PrimaryEmailAddr") else "",
            "phone": c.get("PrimaryPhone", {}).get("FreeFormNumber", "") if c.get("PrimaryPhone") else "",
            "balance": float(c.get("Balance", 0)),
            "active": c.get("Active", True),
            "address": addr.get("Line1", ""),
            "city": addr.get("City", ""),
            "state": addr.get("CountrySubDivisionCode", ""),
            "zip": addr.get("PostalCode", ""),
            "notes": c.get("Notes", ""),
            "created": c.get("MetaData", {}).get("CreateTime", ""),
        })

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CUSTOMER_CACHE_FILE, "w") as f:
        json.dump({"customers": customers, "fetched_at": time.time(), "count": len(customers)}, f, indent=2)

    log.info("Fetched %d customers from QB", len(customers))
    return customers


def find_customer(name: str) -> Optional[dict]:
    """Find a customer by name (fuzzy match)."""
    customers = fetch_customers()
    name_lower = name.lower()
    # Exact match first
    for c in customers:
        if c["name"].lower() == name_lower:
            return c
    # Partial match
    for c in customers:
        if name_lower in c["name"].lower() or name_lower in c.get("company", "").lower():
            return c
    return None



def create_customer(name: str, email: str = "", phone: str = "",
                     bill_address: str = "") -> Optional[dict]:
    """Create a new customer in QuickBooks.
    
    Returns the created customer dict with Id, or None on failure.
    """
    if not is_configured():
        return None
    if not name:
        return None

    cust_data = {
        "DisplayName": name,
        "CompanyName": name,
    }
    if email:
        cust_data["PrimaryEmailAddr"] = {"Address": email}
    if phone:
        cust_data["PrimaryPhone"] = {"FreeFormNumber": phone}
    if bill_address:
        cust_data["BillAddr"] = {"Line1": bill_address}

    result = _qb_request("POST", "customer", cust_data)
    if result and "Customer" in result:
        c = result["Customer"]
        log.info("Created QB customer: %s (ID: %s)", name, c.get("Id"))
        # Clear cache
        try:
            os.remove(CUSTOMER_CACHE_FILE)
        except Exception:
            pass
        return {
            "Id": c.get("Id"),
            "CompanyName": c.get("CompanyName", name),
            "DisplayName": c.get("DisplayName", name),
        }
    log.error("Failed to create QB customer '%s': %s", name, result)
    return None


def get_customer_balance_summary() -> dict:
    """Aggregate customer balance data."""
    customers = fetch_customers()
    with_balance = [c for c in customers if c.get("balance", 0) > 0]
    total_ar = sum(c["balance"] for c in with_balance)

    return {
        "total_customers": len(customers),
        "active_customers": sum(1 for c in customers if c.get("active")),
        "customers_with_balance": len(with_balance),
        "total_receivable": total_ar,
        "top_balances": sorted(with_balance, key=lambda c: c["balance"], reverse=True)[:10],
    }


# ─── Financial Context (for all agents) ────────────────────────────────────

QB_CONTEXT_CACHE_FILE = os.path.join(DATA_DIR, "qb_context_cache.json")

def get_financial_context(force_refresh: bool = False) -> dict:
    """
    Build a comprehensive financial context for all agents.
    Cached for 1 hour to avoid hammering QB API.

    Used by: voice agent, manager brief, pipeline, order management
    """
    if not force_refresh:
        try:
            with open(QB_CONTEXT_CACHE_FILE) as f:
                cache = json.load(f)
            if time.time() - cache.get("fetched_at", 0) < 3600:
                return cache
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    if not is_configured():
        return {"ok": False, "error": "QuickBooks not connected"}

    # Pull everything
    invoices = fetch_invoices(status="all", force_refresh=force_refresh)
    customers = fetch_customers(force_refresh=force_refresh)
    vendors = fetch_vendors(force_refresh=force_refresh)

    open_inv = [i for i in invoices if i["status"] == "open"]
    overdue_inv = [i for i in invoices if i["status"] == "overdue"]
    paid_inv = [i for i in invoices if i["status"] == "paid"]

    context = {
        "ok": True,
        "fetched_at": time.time(),
        "fetched_at_str": datetime.now().isoformat(),

        # Revenue
        "total_invoiced": sum(i["total"] for i in invoices),
        "total_collected": sum(i["total"] for i in paid_inv),
        "total_receivable": sum(i["balance"] for i in invoices if i["balance"] > 0),
        "overdue_amount": sum(i["balance"] for i in overdue_inv),

        # Counts
        "invoice_count": len(invoices),
        "open_invoices": len(open_inv),
        "overdue_invoices": len(overdue_inv),
        "paid_invoices": len(paid_inv),
        "customer_count": len(customers),
        "vendor_count": len(vendors),

        # Pending invoices detail (for orders page)
        "pending_invoices": [{
            "doc_number": i["doc_number"],
            "customer": i["customer_name"],
            "total": i["total"],
            "balance": i["balance"],
            "due_date": i["due_date"],
            "status": i["status"],
            "days_outstanding": (datetime.now() - datetime.strptime(i["txn_date"], "%Y-%m-%d")).days if i.get("txn_date") else 0,
        } for i in (open_inv + overdue_inv)],

        # Top customers by balance
        "top_ar_customers": sorted(
            [{"name": c["name"], "balance": c["balance"]} for c in customers if c["balance"] > 0],
            key=lambda x: x["balance"], reverse=True
        )[:10],

        # Overdue detail
        "overdue_detail": [{
            "doc_number": i["doc_number"],
            "customer": i["customer_name"],
            "balance": i["balance"],
            "due_date": i["due_date"],
            "days_overdue": (datetime.now() - datetime.strptime(i["due_date"], "%Y-%m-%d")).days if i.get("due_date") else 0,
        } for i in overdue_inv],
    }

    # Cache it
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(QB_CONTEXT_CACHE_FILE, "w") as f:
        json.dump(context, f, indent=2, default=str)

    log.info("QB financial context built: $%.2f receivable, %d open invoices",
             context["total_receivable"], context["open_invoices"])
    return context


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

    invoice_count = 0
    try:
        with open(INVOICE_CACHE_FILE) as f:
            cache = json.load(f)
            invoice_count = cache.get("count", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    customer_count = 0
    try:
        with open(CUSTOMER_CACHE_FILE) as f:
            cache = json.load(f)
            customer_count = cache.get("count", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    return {
        "agent": "quickbooks",
        "version": "2.1.0",
        "configured": is_configured(),
        "sandbox_mode": QB_SANDBOX,
        "has_valid_token": has_valid_token,
        "token_expires": tokens.get("expires_at"),
        "realm_id_set": bool(_get_realm_id()),
        "realm_id": _get_realm_id()[:4] + "..." if _get_realm_id() else "",
        "cached_vendors": vendor_count,
        "cached_invoices": invoice_count,
        "cached_customers": customer_count,
    }


# ─── New Functions (Phase 24) ───────────────────────────────────────────────

def get_company_info() -> Optional[dict]:
    """Fetch QuickBooks company information."""
    result = _qb_request("GET", "companyinfo/" + _get_realm_id() + "?minorversion=73")
    if not result:
        return None
    info = result.get("CompanyInfo", {})
    return {
        "name": info.get("CompanyName", ""),
        "legal_name": info.get("LegalName", ""),
        "address": info.get("CompanyAddr", {}),
        "phone": info.get("PrimaryPhone", {}).get("FreeFormNumber", ""),
        "email": info.get("Email", {}).get("Address", ""),
        "fiscal_year_start": info.get("FiscalYearStartMonth", ""),
        "country": info.get("Country", ""),
        "industry": info.get("IndustryType", ""),
    }


def get_profit_loss(start_date: str = None, end_date: str = None) -> Optional[dict]:
    """Fetch Profit & Loss report from QuickBooks."""
    if not start_date:
        start_date = datetime.now().strftime("%Y-01-01")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    endpoint = f"reports/ProfitAndLoss?start_date={start_date}&end_date={end_date}&minorversion=73"
    result = _qb_request("GET", endpoint)
    if not result:
        return None
    # Parse the report structure
    header = result.get("Header", {})
    rows = result.get("Rows", {}).get("Row", [])
    summary = {"period": f"{start_date} to {end_date}", "sections": []}
    for row in rows:
        if row.get("type") == "Section" and row.get("Header"):
            section_name = row["Header"].get("ColData", [{}])[0].get("value", "")
            section_total = ""
            if row.get("Summary"):
                cols = row["Summary"].get("ColData", [])
                if len(cols) > 1:
                    section_total = cols[1].get("value", "")
            summary["sections"].append({"name": section_name, "total": section_total})
        elif row.get("Summary"):
            cols = row["Summary"].get("ColData", [])
            if len(cols) > 1:
                summary["net_income"] = cols[1].get("value", "0")
    return summary


def get_ar_aging() -> Optional[dict]:
    """Fetch Accounts Receivable Aging Summary from QuickBooks."""
    endpoint = "reports/AgedReceivables?minorversion=73"
    result = _qb_request("GET", endpoint)
    if not result:
        return None
    rows = result.get("Rows", {}).get("Row", [])
    aging = []
    for row in rows:
        if row.get("type") == "Data":
            cols = row.get("ColData", [])
            if len(cols) >= 6:
                aging.append({
                    "customer": cols[0].get("value", ""),
                    "current": cols[1].get("value", "0"),
                    "1_30": cols[2].get("value", "0"),
                    "31_60": cols[3].get("value", "0"),
                    "61_90": cols[4].get("value", "0"),
                    "over_90": cols[5].get("value", "0"),
                })
    return {"aging": aging, "count": len(aging)}


def get_recent_payments(days_back: int = 30) -> list:
    """Fetch recent payments received."""
    since = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", since):
        log.error("Invalid date format for QB Payment query: %s", since)
        return []
    query = f"SELECT * FROM Payment WHERE TxnDate >= '{since}' ORDERBY TxnDate DESC MAXRESULTS 50"
    payments = _qb_query(query)
    return [{
        "id": p.get("Id"),
        "date": p.get("TxnDate"),
        "amount": float(p.get("TotalAmt", 0)),
        "customer": p.get("CustomerRef", {}).get("name", ""),
        "memo": p.get("PrivateNote", ""),
    } for p in payments]


def diagnose_connection() -> dict:
    """Full diagnostic of QB connection — token status, API reachability, config."""
    diag = {
        "timestamp": datetime.now().isoformat(),
        "client_id_set": bool(QB_CLIENT_ID or os.environ.get("QB_CLIENT_ID", "")),
        "client_secret_set": bool(QB_CLIENT_SECRET or os.environ.get("QB_CLIENT_SECRET", "")),
        "realm_id": _get_realm_id() or "(empty)",
        "refresh_token_set": bool(_get_refresh_token()),
        "sandbox_mode": QB_SANDBOX,
        "token_file_exists": os.path.exists(TOKEN_FILE),
        "is_configured": is_configured(),
    }
    # Check token file
    tokens = _load_tokens()
    if tokens:
        diag["token_file_keys"] = list(tokens.keys())
        diag["token_expires_at"] = tokens.get("expires_at")
        diag["token_expired"] = time.time() > tokens.get("expires_at", 0)
        diag["connected_at"] = tokens.get("connected_at", tokens.get("refreshed_at", "unknown"))
    else:
        diag["token_file_keys"] = []

    # Try to get access token
    try:
        token = get_access_token()
        diag["access_token_available"] = bool(token)
        if token:
            diag["access_token_preview"] = token[:10] + "..."
    except Exception as e:
        diag["access_token_error"] = str(e)

    # Try a simple API call
    if diag.get("access_token_available"):
        try:
            info = get_company_info()
            if info:
                diag["api_reachable"] = True
                diag["company_name"] = info.get("name", "")
            else:
                diag["api_reachable"] = False
                diag["api_error"] = "get_company_info returned None"
        except Exception as e:
            diag["api_reachable"] = False
            diag["api_error"] = str(e)

    return diag


def get_qb_health() -> dict:
    """Returns QB connectivity status without raising.
    Input: none
    Output: {status, last_sync, token_expires, error}
    Side effects: none (read-only token check)
    """
    try:
        tokens = _load_tokens()
        if not tokens or not tokens.get("refresh_token"):
            return {"status": "disconnected", "last_sync": None,
                    "token_expires": None, "error": "No refresh token"}
        expires = tokens.get("expires_at")
        last_sync = tokens.get("last_sync")
        if not is_configured():
            return {"status": "disconnected", "last_sync": last_sync,
                    "token_expires": expires, "error": "Missing QB credentials"}
        # Check if token is valid (without making API call)
        access = tokens.get("access_token")
        if not access:
            return {"status": "error", "last_sync": last_sync,
                    "token_expires": expires, "error": "No access token — needs refresh"}
        return {"status": "connected", "last_sync": last_sync,
                "token_expires": expires, "error": None}
    except Exception as e:
        return {"status": "error", "last_sync": None,
                "token_expires": None, "error": str(e)[:200]}


# ─── Invoice Numbering ─────────────────────────────────────────────────────
