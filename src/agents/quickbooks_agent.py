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
                    memo: str = "") -> Optional[dict]:
    """
    Create an invoice in QuickBooks.

    Args:
        customer_id: QB Customer ID
        items: [{description, qty, unit_price}]
        po_number: Reference PO number
        memo: Customer memo
    """
    if not is_configured():
        return None

    lines = []
    for i, item in enumerate(items):
        lines.append({
            "DetailType": "SalesItemLineDetail",
            "Amount": round(item.get("qty", 1) * item.get("unit_price", 0), 2),
            "Description": item.get("description", ""),
            "SalesItemLineDetail": {
                "Qty": item.get("qty", 1),
                "UnitPrice": item.get("unit_price", 0),
            },
        })

    invoice_data = {
        "CustomerRef": {"value": customer_id},
        "Line": lines,
    }
    if memo:
        invoice_data["CustomerMemo"] = {"value": memo}
    if po_number:
        invoice_data["CustomField"] = [{"DefinitionId": "1", "StringValue": po_number, "Type": "StringType"}]

    result = _qb_request("POST", "invoice", invoice_data)
    if result and result.get("Invoice"):
        inv = result["Invoice"]
        log.info("QB Invoice created: #%s for $%s", inv.get("DocNumber"), inv.get("TotalAmt"))
        return {
            "id": inv.get("Id"),
            "doc_number": inv.get("DocNumber"),
            "total": float(inv.get("TotalAmt", 0)),
            "customer": inv.get("CustomerRef", {}).get("name", ""),
        }
    return None


# ─── Customer Operations ────────────────────────────────────────────────────

CUSTOMER_CACHE_FILE = os.path.join(DATA_DIR, "qb_customers_cache.json")

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
        "version": "2.0.0",
        "configured": is_configured(),
        "sandbox_mode": QB_SANDBOX,
        "has_valid_token": has_valid_token,
        "token_expires": tokens.get("expires_at"),
        "realm_id_set": bool(QB_REALM_ID),
        "cached_vendors": vendor_count,
        "cached_invoices": invoice_count,
        "cached_customers": customer_count,
    }
