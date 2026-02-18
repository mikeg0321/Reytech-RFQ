"""
vendor_ordering_agent.py — Multi-Vendor Ordering Automation for Reytech
Phase 29 | Version: 1.0.0

VENDOR INTEGRATION PRIORITY:
  P0 — Grainger REST API (free, public — industrial/medical supplies)
  P0 — Amazon SP-API (Business account ordering — everything)
  P0 — Email PO Automation (5 vendors already have emails in DB)
  P1 — Global Industrial (request API access)
  P1 — Medline (major medical distributor)

WORKFLOW (triggered when quote is WON):
  1. Quote won → extract line items + quantities
  2. For each item → identify best vendor + check live pricing
  3. Generate Purchase Order per vendor
  4. P0: Submit via API | P1: Send PO email | P2: Flag for manual
  5. Log to SQLite vendor_orders table + CRM activity
  6. Notify Mike via bell + SMS when PO submitted

SETUP (Railway env vars):
  GRAINGER_CLIENT_ID     — from Grainger developer portal (free)
  GRAINGER_CLIENT_SECRET — OAuth2 credentials
  AMZN_ACCESS_KEY        — Amazon SP-API
  AMZN_SECRET_KEY        — Amazon SP-API
  AMZN_REFRESH_TOKEN     — Amazon SP-API (Seller/Business account)
  AMZN_MARKETPLACE_ID    — ATVPDKIKX0DER (US)

VENDOR CATALOG (mapped to item categories we commonly quote):
  Grainger    → safety, PPE, gloves, sanitizer, cleaning, industrial
  Amazon Biz  → electronics, office, general, any ASIN we can identify
  Curbell     → medical equipment (specialized)
  IMS/Echelon → medical supplies (we have their emails)
"""

import os
import json
import logging
import time
import threading
from datetime import datetime
from typing import Optional

log = logging.getLogger("vendor_order")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# ── API Credentials ─────────────────────────────────────────────────────────
GRAINGER_CLIENT_ID     = os.environ.get("GRAINGER_CLIENT_ID", "")
GRAINGER_CLIENT_SECRET = os.environ.get("GRAINGER_CLIENT_SECRET", "")
GRAINGER_ACCOUNT_NUM   = os.environ.get("GRAINGER_ACCOUNT_NUMBER", "")

AMZN_ACCESS_KEY        = os.environ.get("AMZN_ACCESS_KEY", "")
AMZN_SECRET_KEY        = os.environ.get("AMZN_SECRET_KEY", "")
AMZN_REFRESH_TOKEN     = os.environ.get("AMZN_REFRESH_TOKEN", "")
AMZN_MARKETPLACE_ID    = os.environ.get("AMZN_MARKETPLACE_ID", "ATVPDKIKX0DER")
AMZN_SELLER_ID         = os.environ.get("AMZN_SELLER_ID", "")

GMAIL_ADDRESS  = os.environ.get("GMAIL_ADDRESS", "sales@reytechinc.com")
GMAIL_PASSWORD = os.environ.get("GMAIL_PASSWORD", "")


# ══════════════════════════════════════════════════════════════════════════════
# VENDOR CATALOG — maps item categories to vendor routing rules
# ══════════════════════════════════════════════════════════════════════════════

VENDOR_CATALOG = {
    "grainger": {
        "name": "Grainger",
        "api_type": "rest",
        "api_url": "https://api.grainger.com",
        "categories": ["safety", "ppe", "gloves", "sanitizer", "cleaning",
                       "industrial", "janitorial", "respirator", "masks",
                       "first aid", "fire safety", "tool", "electrical"],
        "configured": bool(GRAINGER_CLIENT_ID and GRAINGER_CLIENT_SECRET),
        "can_order": bool(GRAINGER_CLIENT_ID and GRAINGER_CLIENT_SECRET and GRAINGER_ACCOUNT_NUM),
        "env_needed": ["GRAINGER_CLIENT_ID", "GRAINGER_CLIENT_SECRET", "GRAINGER_ACCOUNT_NUMBER"],
        "signup_url": "https://www.grainger.com/content/grainger-api",
    },
    "amazon_business": {
        "name": "Amazon Business",
        "api_type": "sp_api",
        "categories": ["electronics", "office", "general", "equipment", "technology"],
        "configured": bool(AMZN_ACCESS_KEY and AMZN_SECRET_KEY and AMZN_REFRESH_TOKEN),
        "can_order": bool(AMZN_ACCESS_KEY and AMZN_SECRET_KEY and AMZN_REFRESH_TOKEN and AMZN_SELLER_ID),
        "env_needed": ["AMZN_ACCESS_KEY", "AMZN_SECRET_KEY", "AMZN_REFRESH_TOKEN", "AMZN_SELLER_ID"],
        "signup_url": "https://business.amazon.com",
        "note": "SP-API for Business accounts — search already via SerpApi, this adds ordering",
    },
    "curbell_medical": {
        "name": "Curbell Medical Products",
        "api_type": "email_po",
        "contact_email": "mbleecher@curbellmedical.com",
        "categories": ["medical equipment", "patient care", "clinical", "restraints", "hospital"],
        "configured": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "can_order": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "env_needed": [],
        "note": "Email PO — contact already in vendor list",
    },
    "integrated_medical": {
        "name": "Integrated Medical Supplies",
        "api_type": "email_po",
        "contact_email": "adimalanta@imsla.com",
        "categories": ["medical supplies", "nitrile", "gloves", "ppe", "medical"],
        "configured": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "can_order": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "env_needed": [],
        "note": "Email PO — contact already in vendor list",
    },
    "echelon_distribution": {
        "name": "Echelon Distribution",
        "api_type": "email_po",
        "contact_email": "c.marler@echelondistribution.com",
        "categories": ["general", "distribution", "supplies", "misc"],
        "configured": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "can_order": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "env_needed": [],
    },
    "tsi_incorporated": {
        "name": "TSI Incorporated",
        "api_type": "email_po",
        "contact_email": "psullivan@tsi.com",
        "categories": ["industrial instruments", "measurement", "testing", "scientific"],
        "configured": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "can_order": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "env_needed": [],
    },
    "global_industrial": {
        "name": "Global Industrial",
        "api_type": "rest_pending",
        "categories": ["industrial", "safety", "cleaning", "storage", "material handling"],
        "configured": False,
        "can_order": False,
        "env_needed": ["GLOBAL_IND_API_KEY"],
        "signup_url": "https://www.globalindustrial.com/business",
        "note": "Request API access at globalindustrial.com — similar catalog to Grainger",
    },
    "medline": {
        "name": "Medline Industries",
        "api_type": "edi_pending",
        "categories": ["medical", "surgical", "clinical", "patient care", "hospital grade"],
        "configured": False,
        "can_order": False,
        "env_needed": ["MEDLINE_ACCOUNT_NUM"],
        "note": "Major medical distributor — contact medline.com for B2B API/EDI setup",
    },
}

# ── Category → vendor routing (ordered by preference) ───────────────────────
CATEGORY_ROUTING = {
    "medical":     ["integrated_medical", "curbell_medical", "medline", "grainger"],
    "ppe":         ["grainger", "integrated_medical", "amazon_business"],
    "gloves":      ["integrated_medical", "grainger", "amazon_business"],
    "sanitizer":   ["grainger", "integrated_medical", "amazon_business"],
    "industrial":  ["grainger", "global_industrial", "amazon_business"],
    "electronics": ["amazon_business", "grainger"],
    "office":      ["amazon_business", "grainger"],
    "general":     ["amazon_business", "echelon_distribution", "grainger"],
    "equipment":   ["curbell_medical", "amazon_business", "grainger"],
}


# ══════════════════════════════════════════════════════════════════════════════
# GRAINGER REST API
# ══════════════════════════════════════════════════════════════════════════════

_grainger_token = None
_grainger_token_expiry = 0

def _get_grainger_token() -> Optional[str]:
    """OAuth2 bearer token for Grainger API."""
    global _grainger_token, _grainger_token_expiry

    if _grainger_token and time.time() < _grainger_token_expiry - 60:
        return _grainger_token

    if not (GRAINGER_CLIENT_ID and GRAINGER_CLIENT_SECRET):
        return None

    try:
        import requests
        r = requests.post(
            "https://api.grainger.com/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": GRAINGER_CLIENT_ID,
                "client_secret": GRAINGER_CLIENT_SECRET,
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        _grainger_token = data["access_token"]
        _grainger_token_expiry = time.time() + data.get("expires_in", 3600)
        log.info("Grainger token refreshed (expires in %ds)", data.get("expires_in", 3600))
        return _grainger_token
    except Exception as e:
        log.warning("Grainger auth failed: %s", e)
        return None


def grainger_search(query: str, max_results: int = 10) -> list:
    """
    Search Grainger catalog for a product.

    Returns:
        [{item_number, title, price, uom, availability, url, brand, category}, ...]

    API docs: https://api.grainger.com/search/v1/products
    Note: Search works without auth (public catalog); ordering requires auth.
    """
    try:
        import requests
        params = {
            "searchQuery": query,
            "pageSize": max_results,
            "categoryPath": "",
        }
        headers = {"Accept": "application/json"}
        token = _get_grainger_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        r = requests.get(
            "https://api.grainger.com/search/v1/products",
            params=params,
            headers=headers,
            timeout=15,
        )
        if r.status_code == 401 and not token:
            log.info("Grainger search requires auth — set GRAINGER_CLIENT_ID/SECRET")
            return []
        r.raise_for_status()
        data = r.json()

        results = []
        for item in data.get("products", data.get("results", [])):
            price_raw = item.get("price", {}) or {}
            results.append({
                "item_number": item.get("itemNumber", item.get("sku", "")),
                "title": item.get("name", item.get("title", "")),
                "price": float(price_raw.get("unitPrice", price_raw.get("value", 0)) or 0),
                "uom": price_raw.get("uom", "EA"),
                "availability": item.get("availability", {}).get("status", "unknown"),
                "brand": item.get("brand", {}).get("name", "") if isinstance(item.get("brand"), dict) else item.get("brand", ""),
                "category": item.get("primaryCategory", ""),
                "url": f"https://www.grainger.com/product/{item.get('itemNumber', '')}",
                "vendor": "grainger",
            })
        log.info("Grainger search '%s': %d results", query[:40], len(results))
        return results
    except Exception as e:
        log.warning("Grainger search failed: %s", e)
        return []


def grainger_get_price(item_number: str) -> Optional[dict]:
    """Get live price + availability for a specific Grainger item number."""
    try:
        import requests
        token = _get_grainger_token()
        if not token:
            return None
        r = requests.get(
            f"https://api.grainger.com/catalog/v1/products/{item_number}/price",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params={"accountNumber": GRAINGER_ACCOUNT_NUM} if GRAINGER_ACCOUNT_NUM else {},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "item_number": item_number,
            "price": float(data.get("unitPrice", 0) or 0),
            "uom": data.get("unitOfMeasure", "EA"),
            "availability": data.get("availabilityStatus", "unknown"),
            "vendor": "grainger",
        }
    except Exception as e:
        log.warning("Grainger price lookup failed for %s: %s", item_number, e)
        return None


def grainger_place_order(items: list, po_number: str, ship_to: dict = None) -> dict:
    """
    Place order via Grainger API.

    Args:
        items: [{item_number, quantity, unit_price}, ...]
        po_number: Our PO number (e.g. R26Q4-PO1)
        ship_to: {name, address, city, state, zip} — defaults to Reytech

    Returns:
        {ok, order_number, vendor, total, items}
    """
    token = _get_grainger_token()
    if not token:
        return {"ok": False, "error": "Grainger not authenticated — set GRAINGER_CLIENT_ID/SECRET"}
    if not GRAINGER_ACCOUNT_NUM:
        return {"ok": False, "error": "GRAINGER_ACCOUNT_NUMBER not set in Railway"}

    try:
        import requests

        default_ship = {
            "name": "Reytech Inc.",
            "address": "Reytech Inc.",
            "city": "Irvine",
            "state": "CA",
            "zip": "92618",
        }
        addr = ship_to or default_ship

        order_payload = {
            "accountNumber": GRAINGER_ACCOUNT_NUM,
            "purchaseOrderNumber": po_number,
            "shipTo": {
                "name": addr.get("name", "Reytech Inc."),
                "address1": addr.get("address", ""),
                "city": addr.get("city", ""),
                "state": addr.get("state", "CA"),
                "postalCode": addr.get("zip", ""),
                "country": "US",
            },
            "lineItems": [
                {
                    "itemNumber": it["item_number"],
                    "quantity": int(it.get("quantity", 1)),
                    "unitPrice": float(it.get("unit_price", 0)),
                }
                for it in items
            ],
        }

        r = requests.post(
            "https://api.grainger.com/orders/v1/orders",
            json=order_payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        order_number = data.get("orderNumber", data.get("confirmationNumber", ""))
        total = sum(it.get("unit_price", 0) * it.get("quantity", 1) for it in items)
        log.info("Grainger order placed: %s | PO: %s | Total: $%.2f", order_number, po_number, total)

        _log_vendor_order("grainger", "Grainger", po_number, order_number, items, total, "submitted")
        return {"ok": True, "order_number": order_number, "vendor": "grainger", "total": total, "items": items}
    except Exception as e:
        log.error("Grainger order failed: %s", e)
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# AMAZON SP-API (Catalog + Ordering)
# ══════════════════════════════════════════════════════════════════════════════

def amazon_search_catalog(query: str, max_results: int = 10) -> list:
    """
    Search Amazon catalog via SP-API CatalogItems endpoint.
    Falls back to SerpApi (existing product_research.py) if SP-API not configured.
    """
    # Try SP-API first
    if AMZN_ACCESS_KEY and AMZN_SECRET_KEY and AMZN_REFRESH_TOKEN:
        return _amazon_spapi_search(query, max_results)
    
    # Fall back to existing SerpApi integration
    try:
        from src.agents.product_research import search_amazon
        results = search_amazon(query, max_results)
        for r in results:
            r["vendor"] = "amazon_business"
        return results
    except Exception as e:
        log.debug("Amazon search fallback failed: %s", e)
        return []


def _amazon_spapi_search(query: str, max_results: int = 10) -> list:
    """Search Amazon SP-API CatalogItems v2022-04-01."""
    try:
        import requests
        token = _get_amazon_access_token()
        if not token:
            return []

        r = requests.get(
            "https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items",
            params={
                "keywords": query,
                "marketplaceIds": AMZN_MARKETPLACE_ID,
                "includedData": "summaries,offers",
                "pageSize": max_results,
            },
            headers={
                "x-amz-access-token": token,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()

        results = []
        for item in data.get("items", []):
            summaries = item.get("summaries", [{}])
            summary = summaries[0] if summaries else {}
            offers = item.get("offers", [{}])
            offer = offers[0] if offers else {}
            price = 0.0
            if offer.get("buyingPrice", {}).get("listingPrice", {}).get("amount"):
                price = float(offer["buyingPrice"]["listingPrice"]["amount"])

            results.append({
                "asin": item.get("asin", ""),
                "title": summary.get("itemName", ""),
                "price": price,
                "brand": summary.get("brand", ""),
                "url": f"https://www.amazon.com/dp/{item.get('asin', '')}",
                "vendor": "amazon_business",
            })
        return results
    except Exception as e:
        log.warning("SP-API search failed: %s", e)
        return []


def _get_amazon_access_token() -> Optional[str]:
    """Exchange refresh token for access token via Amazon LWA."""
    if not (AMZN_ACCESS_KEY and AMZN_SECRET_KEY and AMZN_REFRESH_TOKEN):
        return None
    try:
        import requests
        r = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": AMZN_REFRESH_TOKEN,
                "client_id": AMZN_ACCESS_KEY,
                "client_secret": AMZN_SECRET_KEY,
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception as e:
        log.warning("Amazon LWA token failed: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL PO AUTOMATION (for vendors with emails in DB)
# ══════════════════════════════════════════════════════════════════════════════

def send_email_po(
    vendor_key: str,
    items: list,
    po_number: str,
    quote_number: str = "",
    ship_to_agency: str = "",
    notes: str = "",
) -> dict:
    """
    Generate and email a Purchase Order to a vendor.

    Works for: curbell_medical, integrated_medical, echelon_distribution, tsi_incorporated.
    Uses existing EmailSender infrastructure (no new deps).

    Args:
        vendor_key: One of the VENDOR_CATALOG keys
        items: [{description, quantity, unit_price, item_number?}, ...]
        po_number: Our internal PO number
        quote_number: The won quote this PO is for

    Returns:
        {ok, vendor, po_number, to, subject, preview}
    """
    vendor = VENDOR_CATALOG.get(vendor_key)
    if not vendor:
        return {"ok": False, "error": f"Unknown vendor: {vendor_key}"}
    if vendor.get("api_type") != "email_po":
        return {"ok": False, "error": f"{vendor_key} is not an email PO vendor"}
    
    contact_email = vendor.get("contact_email", "")
    if not contact_email:
        return {"ok": False, "error": f"No contact email for {vendor_key}"}

    if not (GMAIL_ADDRESS and GMAIL_PASSWORD):
        return {"ok": False, "error": "Gmail not configured — set GMAIL_ADDRESS/GMAIL_PASSWORD"}

    # Build PO
    today = datetime.now().strftime("%B %d, %Y")
    po_date = datetime.now().strftime("%Y-%m-%d")
    
    # Line items table
    lines = []
    total = 0.0
    for i, item in enumerate(items, 1):
        qty = int(item.get("quantity", 1))
        price = float(item.get("unit_price", 0))
        ext = qty * price
        total += ext
        lines.append(
            f"  {i:>3}. {item.get('description','')[:50]:<52} "
            f"Qty: {qty:>4}  Unit: ${price:>8.2f}  Ext: ${ext:>10.2f}"
        )

    lines_text = "\n".join(lines)

    body = f"""PURCHASE ORDER

To: {vendor['name']}
   {contact_email}

From: Reytech Inc.
      Michael Guadan
      949-229-1575
      sales@reytechinc.com
      CA SB/DVBE Certified #2002605

PO Number:     {po_number}
PO Date:       {today}
Quote Ref:     {quote_number}
Ship To:       {ship_to_agency or 'Per Quote Details'}

─────────────────────────────────────────────────────────────────────────────
LINE ITEMS:
─────────────────────────────────────────────────────────────────────────────
{lines_text}
─────────────────────────────────────────────────────────────────────────────
                                                       TOTAL: ${total:>10,.2f}
─────────────────────────────────────────────────────────────────────────────

TERMS:
  • Payment: Net 30 (Reytech is a CA SB/DVBE — state agency expedited approval)
  • Ship Via: F.O.B. Destination, freight prepaid
  • Delivery: Standard lead time — please confirm via email or phone
  • This PO is issued in fulfillment of a California state agency contract

{('NOTES: ' + notes) if notes else ''}

Please confirm receipt of this PO and expected delivery timeline.
Reply to this email or call 949-229-1575.

Thank you for your continued partnership.

Michael Guadan
Reytech Inc. | CA SB/DVBE #2002605
949-229-1575 | sales@reytechinc.com
"""

    subject = f"Purchase Order {po_number} — Reytech Inc. | {today}"

    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        msg = MIMEMultipart()
        msg["From"] = f"Michael Guadan - Reytech Inc. <{GMAIL_ADDRESS}>"
        msg["To"] = contact_email
        msg["Subject"] = subject
        msg["CC"] = GMAIL_ADDRESS  # BCC ourselves for records
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_PASSWORD)
            server.send_message(msg)

        log.info("Email PO sent: %s → %s (PO: %s)", subject[:50], contact_email, po_number)

        # Log to email_log
        try:
            from src.agents.notify_agent import log_email_event
            log_email_event(
                direction="sent",
                sender=GMAIL_ADDRESS,
                recipient=contact_email,
                subject=subject,
                body_preview=body[:500],
                full_body=body,
                quote_number=quote_number,
                po_number=po_number,
                intent="vendor_po",
                status="sent",
            )
        except Exception:
            pass

        _log_vendor_order(vendor_key, vendor["name"], po_number, f"EMAIL-{po_number}", items, total, "po_emailed")

        return {
            "ok": True, "vendor": vendor["name"], "po_number": po_number,
            "to": contact_email, "subject": subject,
            "preview": body[:300], "total": total,
        }
    except Exception as e:
        log.error("Email PO failed to %s: %s", contact_email, e)
        return {"ok": False, "error": str(e), "vendor": vendor_key}


# ══════════════════════════════════════════════════════════════════════════════
# SMART VENDOR ROUTING — pick the best vendor for each item
# ══════════════════════════════════════════════════════════════════════════════

def route_items_to_vendors(items: list) -> dict:
    """
    Given a list of items from a won quote, route each to the best vendor.

    Returns:
        {
          vendor_key: [
            {description, quantity, unit_price, suggested_item_number, source_price, ...}
          ],
          "_unrouted": [items that couldn't be matched]
        }
    """
    routed = {}
    unrouted = []

    for item in items:
        desc = (item.get("description") or item.get("name") or "").lower()
        vendor_key = _classify_item_to_vendor(desc)

        if vendor_key:
            routed.setdefault(vendor_key, []).append(item)
        else:
            unrouted.append(item)

    if unrouted:
        routed["_unrouted"] = unrouted

    log.info("Routed %d items: %s", len(items),
             {k: len(v) for k, v in routed.items() if k != "_unrouted"})
    return routed


def _classify_item_to_vendor(description: str) -> Optional[str]:
    """Classify an item description to a vendor key based on keywords."""
    desc = description.lower()

    # Medical/clinical keywords → prefer medical vendors
    if any(k in desc for k in ["nitrile", "glove", "exam glove", "medical glove", "latex"]):
        return "integrated_medical" if VENDOR_CATALOG["integrated_medical"]["can_order"] else "grainger"

    if any(k in desc for k in ["patient", "restraint", "clinical", "surgical", "catheter", "hospital"]):
        return "curbell_medical" if VENDOR_CATALOG["curbell_medical"]["can_order"] else "grainger"

    if any(k in desc for k in ["stryker", "medline", "cardinal health"]):
        return "curbell_medical"

    # PPE / sanitizer / safety → Grainger
    if any(k in desc for k in ["sanitizer", "hand sanitizer", "ppe", "n95", "mask",
                                 "respirator", "safety", "protective", "glove"]):
        return "grainger" if VENDOR_CATALOG["grainger"]["can_order"] else "integrated_medical"

    # Industrial → Grainger
    if any(k in desc for k in ["industrial", "cleaning", "janitorial", "tool",
                                 "equipment", "facility", "maintenance"]):
        return "grainger"

    # Electronics / IT → Amazon Business
    if any(k in desc for k in ["electronic", "computer", "laptop", "tablet",
                                 "printer", "cable", "software", "usb", "monitor"]):
        return "amazon_business"

    # General / office → Amazon
    if any(k in desc for k in ["office", "paper", "staple", "binder", "pen"]):
        return "amazon_business"

    # Instruments / testing → TSI
    if any(k in desc for k in ["instrument", "meter", "sensor", "monitor", "detector"]):
        return "tsi_incorporated" if VENDOR_CATALOG["tsi_incorporated"]["can_order"] else "grainger"

    # Default: try Grainger first, then Amazon
    if VENDOR_CATALOG["grainger"]["can_order"]:
        return "grainger"
    if VENDOR_CATALOG["amazon_business"]["can_order"]:
        return "amazon_business"
    return None


# ══════════════════════════════════════════════════════════════════════════════
# FULL ORDER PIPELINE — called when a quote is WON
# ══════════════════════════════════════════════════════════════════════════════

def process_won_quote_ordering(
    quote_number: str,
    items: list,
    agency: str = "",
    po_number: str = "",
    run_async: bool = True,
) -> dict:
    """
    Main entry point — called from quote-won flow.

    1. Route items to vendors
    2. Search for pricing/item numbers if needed
    3. Submit orders (API or email PO)
    4. Log everything to SQLite
    5. Alert Mike via notification bell + SMS

    Args:
        quote_number: The won quote (e.g. R26Q4)
        items: [{description, quantity, unit_price, ...}, ...]
        agency: Buying agency (for ship_to on email POs)
        po_number: State PO number if known
        run_async: Fire in background thread

    Returns:
        {ok, orders_submitted, orders_pending, total}
    """
    if run_async:
        t = threading.Thread(
            target=_run_ordering_pipeline,
            args=(quote_number, items, agency, po_number),
            daemon=True,
            name=f"order-{quote_number}",
        )
        t.start()
        return {"ok": True, "async": True, "quote": quote_number}

    return _run_ordering_pipeline(quote_number, items, agency, po_number)


def _run_ordering_pipeline(quote_number, items, agency, po_number):
    """Blocking order pipeline."""
    log.info("[OrderPipeline] Starting for quote %s (%d items)", quote_number, len(items))
    t0 = time.time()

    routed = route_items_to_vendors(items)
    orders_submitted = []
    orders_pending = []
    total_ordered = 0.0

    for vendor_key, vendor_items in routed.items():
        if vendor_key == "_unrouted":
            for item in vendor_items:
                orders_pending.append({"item": item.get("description","")[:50], "reason": "no vendor match"})
            continue

        vendor = VENDOR_CATALOG.get(vendor_key, {})
        our_po = f"{quote_number}-PO-{vendor_key[:4].upper()}"

        if not vendor.get("can_order"):
            for item in vendor_items:
                orders_pending.append({
                    "item": item.get("description","")[:50],
                    "vendor": vendor.get("name",""),
                    "reason": f"Set up {vendor.get('env_needed',[])} in Railway to enable",
                })
            continue

        api_type = vendor.get("api_type", "")

        if api_type == "rest":  # Grainger
            # Build Grainger order payload
            order_items = []
            for item in vendor_items:
                # Try to get Grainger item number if not already set
                item_num = item.get("grainger_item_number") or item.get("item_number", "")
                if not item_num:
                    results = grainger_search(item.get("description",""), max_results=1)
                    if results:
                        item_num = results[0].get("item_number","")
                        item["unit_price"] = item["unit_price"] or results[0].get("price", 0)

                if item_num:
                    order_items.append({
                        "item_number": item_num,
                        "quantity": int(item.get("quantity", 1)),
                        "unit_price": float(item.get("unit_price") or 0),
                        "description": item.get("description",""),
                    })
                else:
                    orders_pending.append({"item": item.get("description","")[:50], "reason": "no Grainger item# found"})

            if order_items:
                result = grainger_place_order(order_items, our_po)
                if result.get("ok"):
                    orders_submitted.append(result)
                    total_ordered += result.get("total", 0)
                else:
                    orders_pending.append({"vendor": "Grainger", "error": result.get("error","")})

        elif api_type == "email_po":
            result = send_email_po(
                vendor_key=vendor_key,
                items=vendor_items,
                po_number=our_po,
                quote_number=quote_number,
                ship_to_agency=agency,
            )
            if result.get("ok"):
                orders_submitted.append(result)
                total_ordered += result.get("total", 0)
            else:
                orders_pending.append({"vendor": vendor.get("name",""), "error": result.get("error","")})

    elapsed = time.time() - t0
    summary = {
        "ok": True,
        "quote_number": quote_number,
        "orders_submitted": len(orders_submitted),
        "orders_pending": len(orders_pending),
        "total_ordered": total_ordered,
        "details": orders_submitted,
        "pending": orders_pending,
        "elapsed_sec": round(elapsed, 1),
    }

    # Alert Mike
    try:
        from src.agents.notify_agent import send_alert
        if orders_submitted:
            send_alert(
                event_type="quote_won",
                title=f"📦 {len(orders_submitted)} PO(s) Submitted for {quote_number}",
                body=f"Reytech ordered ${total_ordered:,.2f} across {len(orders_submitted)} vendor(s). "
                     f"{len(orders_pending)} item(s) need manual review.",
                urgency="deal",
                context={"quote_number": quote_number, "amount": total_ordered},
            )
        if orders_pending:
            send_alert(
                event_type="outbox_stale",
                title=f"⚠️ {len(orders_pending)} Items Need Manual Ordering",
                body=f"Quote {quote_number}: {len(orders_pending)} items couldn't be auto-ordered. Review vendor setup.",
                urgency="warning",
                context={"quote_number": quote_number},
                cooldown_key=f"pending_order_{quote_number}",
            )
    except Exception:
        pass

    log.info("[OrderPipeline] Done: %d submitted, %d pending, $%.2f | %.1fs",
             len(orders_submitted), len(orders_pending), total_ordered, elapsed)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# CROSS-VENDOR PRICE COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

def compare_vendor_prices(description: str, quantity: int = 1) -> dict:
    """
    Search multiple vendors simultaneously and return price comparison.

    Used in price check flow to find best sourcing option.

    Returns:
        {
          best: {vendor, price, title, url},
          comparison: [{vendor, price, title, url, availability}, ...],
          savings_vs_worst: float,
        }
    """
    results = []

    # Search Grainger
    g_results = grainger_search(description, max_results=3)
    for r in g_results[:1]:  # best match
        if r.get("price", 0) > 0:
            results.append({**r, "vendor_display": "Grainger"})

    # Search Amazon (via SerpApi fallback or SP-API)
    a_results = amazon_search_catalog(description, max_results=3)
    for r in a_results[:1]:
        if r.get("price", 0) > 0:
            results.append({**r, "vendor_display": "Amazon Business"})

    # Check our SCPRS won-quotes history for reference pricing
    try:
        from src.knowledge.won_quotes_db import search_pricing
        scprs = search_pricing(description, limit=1)
        if scprs:
            s = scprs[0]
            results.append({
                "vendor_display": f"SCPRS ({s.get('vendor','')})",
                "price": float(s.get("unit_price", 0) or 0),
                "title": s.get("description",""),
                "url": "",
                "vendor": "scprs",
            })
    except Exception:
        pass

    if not results:
        return {"best": None, "comparison": [], "savings_vs_worst": 0}

    valid = [r for r in results if r.get("price", 0) > 0]
    if not valid:
        return {"best": None, "comparison": results, "savings_vs_worst": 0}

    valid.sort(key=lambda r: r["price"])
    best = valid[0]
    worst_price = valid[-1]["price"]
    savings = (worst_price - best["price"]) * quantity if len(valid) > 1 else 0

    return {
        "best": best,
        "comparison": valid,
        "savings_vs_worst": round(savings, 2),
        "quantity": quantity,
    }


# ══════════════════════════════════════════════════════════════════════════════
# VENDOR ENRICHMENT — enhance vendors.json with API metadata
# ══════════════════════════════════════════════════════════════════════════════

def get_enriched_vendor_list() -> list:
    """
    Return vendors.json enriched with:
    - API type (rest|email_po|rest_pending|none)
    - Integration status (active|ready|setup_needed|manual_only)
    - Product categories
    - Missing distributors to add
    """
    try:
        vendors_path = os.path.join(DATA_DIR, "vendors.json")
        vendors = json.load(open(vendors_path))
    except Exception:
        vendors = []

    # Build name → API catalog lookup
    api_lookup = {}
    for key, v in VENDOR_CATALOG.items():
        api_lookup[v["name"].lower()] = {**v, "vendor_key": key}

    enriched = []
    for v in vendors:
        name = v.get("name", "")
        api_info = api_lookup.get(name.lower(), {})
        enriched.append({
            **v,
            "api_type": api_info.get("api_type", "none"),
            "api_configured": api_info.get("configured", False),
            "can_order": api_info.get("can_order", False),
            "categories": api_info.get("categories", []),
            "vendor_key": api_info.get("vendor_key", ""),
            "integration_status": (
                "active" if api_info.get("can_order") else
                "ready" if api_info.get("configured") else
                "setup_needed" if api_info.get("api_type") and api_info["api_type"] not in ("none","email_po") else
                "email_po" if api_info.get("api_type") == "email_po" else
                "manual_only"
            ),
        })

    # Add missing major distributors
    MISSING = [
        {"name": "Grainger", "company": "Grainger Industrial Supply", "api_type": "rest",
         "integration_status": "setup_needed", "note": "Set GRAINGER_CLIENT_ID/SECRET in Railway"},
        {"name": "Medline Industries", "company": "Medline Industries LP", "api_type": "edi_pending",
         "integration_status": "setup_needed", "note": "Contact medline.com for B2B EDI"},
        {"name": "CDW-G", "company": "CDW Government LLC", "api_type": "rest_pending",
         "integration_status": "setup_needed", "note": "Government IT procurement"},
    ]
    existing_names = {v["name"].lower() for v in enriched}
    for m in MISSING:
        if m["name"].lower() not in existing_names:
            enriched.append({**m, "source": "reytech_recommended", "can_order": False, "api_configured": False})

    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# SQLITE LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def _log_vendor_order(vendor_key, vendor_name, po_number, order_number, items, total, status):
    """Log vendor order to SQLite for tracking and QB sync."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO vendor_orders (
                    vendor_key, vendor_name, po_number, order_number,
                    items_json, total, status, submitted_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                vendor_key, vendor_name, po_number, order_number,
                json.dumps(items), total, status,
                datetime.now().isoformat(), datetime.now().isoformat(),
            ))
    except Exception as e:
        log.debug("vendor_order log failed: %s", e)


def get_vendor_orders(limit=50, status=None) -> list:
    """Get vendor orders from SQLite."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            where = f"WHERE status='{status}'" if status else ""
            rows = conn.execute(
                f"SELECT * FROM vendor_orders {where} ORDER BY submitted_at DESC LIMIT ?", (limit,)
            ).fetchall()
            results = []
            for r in rows:
                row = dict(r)
                try:
                    row["items"] = json.loads(row.get("items_json", "[]"))
                except Exception:
                    row["items"] = []
                results.append(row)
            return results
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# AGENT STATUS
# ══════════════════════════════════════════════════════════════════════════════

def get_agent_status() -> dict:
    vendors_active = [k for k, v in VENDOR_CATALOG.items() if v.get("can_order")]
    vendors_ready = [k for k, v in VENDOR_CATALOG.items() if v.get("configured") and not v.get("can_order")]
    vendors_setup_needed = [k for k, v in VENDOR_CATALOG.items()
                             if not v.get("configured") and v.get("env_needed")]
    return {
        "agent": "vendor_ordering_agent",
        "version": "1.0.0",
        "vendors_active": vendors_active,
        "vendors_ready_partial": vendors_ready,
        "vendors_setup_needed": vendors_setup_needed,
        "vendor_count": len(VENDOR_CATALOG),
        "grainger_configured": bool(GRAINGER_CLIENT_ID and GRAINGER_CLIENT_SECRET),
        "grainger_can_order": bool(GRAINGER_CLIENT_ID and GRAINGER_CLIENT_SECRET and GRAINGER_ACCOUNT_NUM),
        "amazon_configured": bool(AMZN_ACCESS_KEY and AMZN_SECRET_KEY and AMZN_REFRESH_TOKEN),
        "email_po_active": bool(GMAIL_ADDRESS and GMAIL_PASSWORD),
        "email_po_vendors": [k for k, v in VENDOR_CATALOG.items()
                              if v.get("api_type") == "email_po" and v.get("can_order")],
        "setup_guide": {
            "grainger": {
                "step1": "Create account at grainger.com",
                "step2": "Request API access at api.grainger.com (free)",
                "step3": "Set GRAINGER_CLIENT_ID, GRAINGER_CLIENT_SECRET, GRAINGER_ACCOUNT_NUMBER in Railway",
                "note": "Grainger API gives: search, pricing, availability, and ORDER placement",
            },
            "amazon_business": {
                "step1": "Create Amazon Business account at business.amazon.com",
                "step2": "Register as SP-API developer at sellercentral.amazon.com",
                "step3": "Set AMZN_ACCESS_KEY, AMZN_SECRET_KEY, AMZN_REFRESH_TOKEN, AMZN_SELLER_ID",
                "note": "Already using SerpApi for search — SP-API adds ordering + bulk pricing",
            },
        },
    }
