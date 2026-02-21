"""
QuickBooks Online API Integration
==================================

Maps Reytech order/invoice data to QuickBooks Online API format.
Handles: Invoice create/update, Customer sync, Item catalog sync.

QB API Reference: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/invoice

Required env vars (when connected):
  QB_CLIENT_ID       - OAuth2 app client ID
  QB_CLIENT_SECRET   - OAuth2 app client secret
  QB_REALM_ID        - Company ID (from OAuth callback)
  QB_ACCESS_TOKEN    - Bearer token (refreshed automatically)
  QB_REFRESH_TOKEN   - For token refresh

Status: STUB — all functions return the QB-format payload without making API calls.
When QB is connected, these functions will POST to the QB API.
"""

import os
import logging
from datetime import datetime, timedelta

log = logging.getLogger("reytech.qb")

# ─── Configuration ───────────────────────────────────────────────────────────

QB_API_BASE = "https://quickbooks.api.intuit.com/v3/company"
QB_SANDBOX_BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company"

def _qb_configured() -> bool:
    """Check if QB credentials are set."""
    return bool(os.environ.get("QB_REALM_ID") and os.environ.get("QB_ACCESS_TOKEN"))


# ─── Invoice: Draft → QB Payload ─────────────────────────────────────────────

def draft_invoice_to_qb_payload(draft_invoice: dict, order: dict) -> dict:
    """
    Convert our draft_invoice dict to QuickBooks Online Invoice API format.
    
    Our format → QB format mapping:
      invoice_number     → DocNumber
      bill_to_email      → BillEmail.Address
      qb_customer_id     → CustomerRef.value
      bill_to_name       → CustomerRef.name (display only)
      items[].description → Line[].Description
      items[].qty        → Line[].SalesItemLineDetail.Qty
      items[].unit_price → Line[].SalesItemLineDetail.UnitPrice
      items[].extended   → Line[].Amount
      items[].qb_item_ref → Line[].SalesItemLineDetail.ItemRef.value
      tax                → TxnTaxDetail.TotalTax
      terms              → SalesTermRef (lookup by name)
      po_number          → CustomField or PrivateNote
      ship_to_*          → ShipAddr
    """
    
    # Build Line items
    qb_lines = []
    for i, item in enumerate(draft_invoice.get("items", [])):
        line = {
            "LineNum": i + 1,
            "Amount": item.get("extended", 0),
            "Description": item.get("description", ""),
            "DetailType": "SalesItemLineDetail",
            "SalesItemLineDetail": {
                "Qty": item.get("qty", 0),
                "UnitPrice": item.get("unit_price", 0),
            }
        }
        
        # ItemRef — required by QB. Maps to product catalog.
        # If no QB item ID, QB will need a generic "Sales" item
        qb_item_id = item.get("qb_item_ref", "")
        qb_item_name = item.get("qb_item_name", "")
        if qb_item_id:
            line["SalesItemLineDetail"]["ItemRef"] = {
                "value": qb_item_id,
                "name": qb_item_name,
            }
        else:
            # Default: use a generic "Products" or "Sales" item
            # This should be configured per-company
            line["SalesItemLineDetail"]["ItemRef"] = {
                "value": "1",  # Default QB item — configure in settings
                "name": "Products",
            }
        
        qb_lines.append(line)
    
    # Build the QB Invoice payload
    payload = {
        "DocNumber": draft_invoice.get("invoice_number", ""),
        "CustomerRef": {
            "value": draft_invoice.get("qb_customer_id", ""),
            "name": draft_invoice.get("bill_to_name", ""),
        },
        "Line": qb_lines,
    }
    
    # BillEmail
    bill_email = draft_invoice.get("bill_to_email", "")
    if bill_email:
        payload["BillEmail"] = {"Address": bill_email}
    
    # Due date (from terms)
    terms = draft_invoice.get("terms", "Net 45")
    days = 45  # default
    try:
        days = int(''.join(c for c in terms if c.isdigit()))
    except Exception:
        pass
    payload["DueDate"] = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    
    # Tax
    tax = draft_invoice.get("tax", 0)
    if tax > 0:
        payload["TxnTaxDetail"] = {
            "TotalTax": tax,
        }
    
    # PO number as memo/private note
    po = draft_invoice.get("po_number", "")
    if po:
        payload["PrivateNote"] = f"PO# {po}"
        # Also set as custom field if configured
        payload["CustomField"] = [
            {
                "DefinitionId": "1",
                "Name": "PO Number",
                "Type": "StringType",
                "StringValue": po,
            }
        ]
    
    # Ship address
    ship_to = draft_invoice.get("ship_to_address", [])
    ship_name = draft_invoice.get("ship_to_name", "")
    if ship_to or ship_name:
        ship_addr = {"Line1": ship_name}
        if len(ship_to) > 0:
            ship_addr["Line1"] = ship_to[0]
        if len(ship_to) > 1:
            # Parse "City, CA 95671" format
            ship_addr["Line2"] = ship_to[1] if len(ship_to) > 1 else ""
        payload["ShipAddr"] = ship_addr
    
    return payload


def push_invoice_to_qb(draft_invoice: dict, order: dict) -> dict:
    """
    Push a draft invoice to QuickBooks Online.
    
    Returns: {ok, qb_invoice_id, qb_sync_token, error}
    
    Currently a STUB — returns the payload that WOULD be sent.
    When QB_REALM_ID + QB_ACCESS_TOKEN are set, this will POST to QB API.
    """
    payload = draft_invoice_to_qb_payload(draft_invoice, order)
    
    if not _qb_configured():
        log.info("QB not configured — invoice payload generated but not pushed")
        return {
            "ok": False,
            "error": "QuickBooks not connected. Set QB_REALM_ID + QB_ACCESS_TOKEN.",
            "payload": payload,
            "would_push": True,
        }
    
    # ── Real QB API call (when connected) ──
    # import requests
    # realm_id = os.environ["QB_REALM_ID"]
    # access_token = os.environ["QB_ACCESS_TOKEN"]
    # url = f"{QB_API_BASE}/{realm_id}/invoice?minorversion=75"
    # headers = {
    #     "Authorization": f"Bearer {access_token}",
    #     "Content-Type": "application/json",
    #     "Accept": "application/json",
    # }
    # resp = requests.post(url, json=payload, headers=headers)
    # if resp.status_code == 200:
    #     data = resp.json()
    #     inv = data.get("Invoice", {})
    #     return {
    #         "ok": True,
    #         "qb_invoice_id": inv.get("Id", ""),
    #         "qb_sync_token": inv.get("SyncToken", ""),
    #     }
    # else:
    #     return {"ok": False, "error": resp.text}
    
    return {"ok": False, "error": "QB integration stub — not yet connected", "payload": payload}


# ─── Customer Sync ────────────────────────────────────────────────────────────

def customer_to_qb_payload(customer: dict) -> dict:
    """
    Map a Reytech CRM customer to QB Customer API format.
    
    Our format → QB format:
      name           → DisplayName
      email          → PrimaryEmailAddr.Address
      phone          → PrimaryPhone.FreeFormNumber
      institution    → CompanyName
      agency         → Notes or custom field
    """
    return {
        "DisplayName": customer.get("name", "") or customer.get("institution", ""),
        "CompanyName": customer.get("institution", ""),
        "PrimaryEmailAddr": {"Address": customer.get("email", "")},
        "PrimaryPhone": {"FreeFormNumber": customer.get("phone", "")},
        "Notes": f"Agency: {customer.get('agency', '')}",
    }


# ─── Item/Product Sync ───────────────────────────────────────────────────────

def product_to_qb_payload(product: dict) -> dict:
    """
    Map a Reytech catalog item to QB Item API format.
    
    Our format → QB format:
      description    → Description, Name
      part_number    → Sku
      unit_price     → UnitPrice
    """
    return {
        "Name": product.get("description", "")[:100],
        "Sku": product.get("part_number", ""),
        "Description": product.get("description", ""),
        "Type": "NonInventory",  # or "Inventory" if tracking stock
        "UnitPrice": product.get("unit_price", 0),
        "IncomeAccountRef": {"value": "1"},  # Configure: Sales income account
    }


# ─── QB Product List Import ──────────────────────────────────────────────────

def parse_qb_product_export(csv_path: str) -> list:
    """
    Parse a QuickBooks Product/Service List CSV export.
    
    Expected columns: Name, SKU, Description, Type, Sales Price, Cost, 
                      Income Account, Taxable, Qty on Hand
    
    Returns list of product dicts ready for catalog merge.
    """
    import csv
    products = []
    try:
        with open(csv_path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                products.append({
                    "description": row.get("Name", "") or row.get("Product/Service", ""),
                    "part_number": row.get("SKU", "") or row.get("Sku", ""),
                    "full_description": row.get("Description", ""),
                    "unit_price": float(row.get("Sales Price", 0) or row.get("Rate", 0) or 0),
                    "cost": float(row.get("Cost", 0) or row.get("Purchase Cost", 0) or 0),
                    "type": row.get("Type", "NonInventory"),
                    "taxable": row.get("Taxable", "").lower() in ("yes", "true", "1"),
                    "qb_item_name": row.get("Name", ""),
                    "source": "quickbooks",
                })
    except Exception as e:
        log.error("QB product CSV parse failed: %s", e)
    
    return products
