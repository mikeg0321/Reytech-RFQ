"""
voice_knowledge.py — Knowledge layer for Reytech Voice Agent
Phase 18 | Provides real-time data access for AI phone calls

Builds rich context from:
- Quote history (quotes_log.json)
- Customer database (customers.json)
- SCPRS pricing intel (scprs_prices.json)
- Lead pipeline (leads.json)
- CRM activity log (crm_activity.json)
- Order tracking (orders.json)

Two modes:
1. Pre-call context: injected into system prompt before dialing
2. Mid-call tools: Vapi function calling for live lookups
"""

import os
import json
import logging
from datetime import datetime

log = logging.getLogger("voice_knowledge")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")


def _load(filename: str) -> list | dict:
    try:
        with open(os.path.join(DATA_DIR, filename)) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return [] if filename != "customers.json" else []


# ─── Pre-Call Context Builder ───────────────────────────────────────────────

def build_call_context(institution: str = "", po_number: str = "",
                        quote_number: str = "", buyer_name: str = "",
                        buyer_email: str = "") -> str:
    """
    Build rich context string for the voice agent system prompt.
    Pulls all relevant data about the institution, buyer, and history.
    """
    sections = []

    # ── Institution / Agency History ──
    if institution:
        inst_context = _get_institution_context(institution)
        if inst_context:
            sections.append(inst_context)

    # ── Specific Quote Details ──
    if quote_number:
        qt_context = _get_quote_context(quote_number)
        if qt_context:
            sections.append(qt_context)

    # ── Buyer Info ──
    if buyer_name or buyer_email:
        buyer_context = _get_buyer_context(buyer_name, buyer_email, institution)
        if buyer_context:
            sections.append(buyer_context)

    # ── Lead / PO Intel ──
    if po_number:
        lead_context = _get_lead_context(po_number)
        if lead_context:
            sections.append(lead_context)

    # ── Pricing Intel ──
    if institution:
        pricing = _get_pricing_context(institution)
        if pricing:
            sections.append(pricing)

    # ── Financial Context (QB) ──
    financial = _get_financial_context(institution)
    if financial:
        sections.append(financial)

    if not sections:
        return ""

    return "\n\n--- REYTECH KNOWLEDGE BASE ---\n" + "\n\n".join(sections)


def _get_institution_context(institution: str) -> str:
    """Get history with this institution."""
    quotes = _load("quotes_log.json")
    inst_lower = institution.lower()

    # Find all quotes for this institution
    inst_quotes = [q for q in quotes if
                   inst_lower in q.get("institution", "").lower() or
                   inst_lower in q.get("ship_to_name", "").lower()]

    if not inst_quotes:
        return f"INSTITUTION: {institution}\nNo prior quote history. This is a new relationship."

    won = [q for q in inst_quotes if q.get("status") == "won"]
    lost = [q for q in inst_quotes if q.get("status") == "lost"]
    pending = [q for q in inst_quotes if q.get("status") in ("pending", "sent")]
    total_quoted = sum(q.get("total", 0) for q in inst_quotes)
    total_won = sum(q.get("total", 0) for q in won)

    lines = [f"INSTITUTION: {institution}"]
    lines.append(f"History: {len(inst_quotes)} quotes — {len(won)} won, {len(lost)} lost, {len(pending)} pending")
    lines.append(f"Total quoted: ${total_quoted:,.0f} | Won value: ${total_won:,.0f}")

    if won:
        win_rate = round(len(won) / (len(won) + len(lost)) * 100) if (len(won) + len(lost)) > 0 else 0
        lines.append(f"Win rate: {win_rate}%")

    # Recent quotes
    recent = sorted(inst_quotes, key=lambda q: q.get("created_at", ""), reverse=True)[:3]
    lines.append("Recent quotes:")
    for q in recent:
        lines.append(f"  - {q.get('quote_number','?')} ({q.get('date','?')}) ${q.get('total',0):,.0f} [{q.get('status','?')}]")

    return "\n".join(lines)


def _get_quote_context(quote_number: str) -> str:
    """Get details of a specific quote."""
    quotes = _load("quotes_log.json")
    qt = next((q for q in quotes if q.get("quote_number") == quote_number), None)
    if not qt:
        return ""

    lines = [f"QUOTE DETAILS: {quote_number}"]
    lines.append(f"Date: {qt.get('date', '?')} | Status: {qt.get('status', '?')}")
    lines.append(f"Institution: {qt.get('institution', '?')}")
    lines.append(f"Total: ${qt.get('total', 0):,.2f} | Items: {qt.get('items_count', 0)}")

    if qt.get("po_number"):
        lines.append(f"PO Number: {qt['po_number']}")
    if qt.get("rfq_number"):
        lines.append(f"RFQ: {qt['rfq_number']}")

    # Line items
    items = qt.get("items_detail", [])
    if items:
        lines.append("Line items:")
        for it in items[:8]:
            desc = it.get("description", "")[:60]
            lines.append(f"  - {desc} (Qty {it.get('qty',0)}) ${it.get('unit_price',0):,.2f}")

    return "\n".join(lines)


def _get_buyer_context(name: str, email: str, institution: str) -> str:
    """Get info about a specific buyer."""
    customers = _load("customers.json")
    if not customers:
        return ""

    # Search by name or email
    matches = []
    name_lower = (name or "").lower()
    email_lower = (email or "").lower()

    for c in customers:
        if isinstance(c, dict):
            if name_lower and name_lower in c.get("display_name", "").lower():
                matches.append(c)
            elif email_lower and email_lower == c.get("email", "").lower():
                matches.append(c)

    if not matches:
        return ""

    buyer = matches[0]
    lines = [f"BUYER INFO: {buyer.get('display_name', name)}"]
    if buyer.get("email"):
        lines.append(f"Email: {buyer['email']}")
    if buyer.get("phone"):
        lines.append(f"Phone: {buyer['phone']}")
    if buyer.get("company"):
        lines.append(f"Company: {buyer['company']}")
    if buyer.get("open_balance"):
        lines.append(f"Open balance: ${buyer['open_balance']:,.2f}")

    return "\n".join(lines)


def _get_lead_context(po_number: str) -> str:
    """Get lead intel for a PO number."""
    leads = _load("leads.json")
    if not leads:
        return ""

    lead = None
    for l in (leads if isinstance(leads, list) else leads.values()):
        if isinstance(l, dict) and po_number.lower() in l.get("po_number", "").lower():
            lead = l
            break

    if not lead:
        return ""

    lines = [f"LEAD INTEL: {po_number}"]
    lines.append(f"Institution: {lead.get('institution', '?')}")
    lines.append(f"Value: ${lead.get('po_value', 0):,.0f} | Items: {lead.get('items_count', 0)}")
    lines.append(f"Score: {lead.get('score', 0):.0%} | Category: {lead.get('category', '?')}")

    if lead.get("matched_items"):
        lines.append("Matched items we've supplied:")
        for item in lead["matched_items"][:5]:
            if isinstance(item, dict):
                lines.append(f"  - {item.get('description', '')[:60]}")

    if lead.get("estimated_savings_pct"):
        lines.append(f"Estimated savings: {lead['estimated_savings_pct']:.0%} vs current vendor")

    return "\n".join(lines)


def _get_pricing_context(institution: str) -> str:
    """Get competitive pricing intel for this institution's typical items."""
    scprs = _load("scprs_prices.json")
    if not scprs:
        return ""

    # Find SCPRS prices related to this institution
    inst_lower = institution.lower()
    relevant = []
    for p in (scprs if isinstance(scprs, list) else [scprs]):
        if isinstance(p, dict):
            dept = p.get("department", "") or p.get("institution", "")
            if inst_lower in dept.lower():
                relevant.append(p)

    if not relevant:
        return ""

    lines = ["COMPETITIVE PRICING INTEL:"]
    for p in relevant[:5]:
        desc = p.get("description", "")[:50]
        price = p.get("unit_price", p.get("price", 0))
        vendor = p.get("vendor", "?")
        lines.append(f"  - {desc}: ${price:,.2f} (vendor: {vendor})")

    return "\n".join(lines)


def _get_financial_context(institution: str = "") -> str:
    """Get QuickBooks financial context for calls."""
    try:
        with open(os.path.join(DATA_DIR, "qb_context_cache.json")) as f:
            ctx = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return ""

    if not ctx.get("ok"):
        return ""

    lines = ["FINANCIAL STATUS (from QuickBooks):"]
    lines.append(f"Total receivable: ${ctx.get('total_receivable', 0):,.2f}")
    lines.append(f"Overdue: ${ctx.get('overdue_amount', 0):,.2f}")
    lines.append(f"Open invoices: {ctx.get('open_invoices', 0)} | Overdue: {ctx.get('overdue_invoices', 0)}")

    # Check if this institution has open invoices
    if institution:
        inst_lower = institution.lower()
        pending = ctx.get("pending_invoices", [])
        inst_invoices = [i for i in pending if inst_lower in i.get("customer", "").lower()]
        if inst_invoices:
            lines.append(f"\n⚠️ THIS CUSTOMER has {len(inst_invoices)} open invoice(s):")
            for inv in inst_invoices[:3]:
                lines.append(f"  Invoice #{inv['doc_number']}: ${inv['balance']:,.2f} due {inv['due_date']} ({inv['status']})")
            lines.append("Note: Do NOT bring up unpaid invoices unprompted on sales calls. "
                         "Only reference if they ask about account status or you're doing an invoice follow-up.")

    return "\n".join(lines)


# ─── Mid-Call Tool Functions (for Vapi function calling) ────────────────────

def handle_tool_call(function_name: str, parameters: dict) -> str:
    """
    Handle a Vapi function call during a live conversation.
    Returns a string response that gets fed back to the AI.
    """
    handlers = {
        "lookup_pricing": _tool_lookup_pricing,
        "check_quote_status": _tool_check_quote_status,
        "get_institution_info": _tool_get_institution_info,
        "schedule_follow_up": _tool_schedule_follow_up,
        "check_order_status": _tool_check_order_status,
    }

    handler = handlers.get(function_name)
    if not handler:
        return f"Function {function_name} not available."

    try:
        return handler(parameters)
    except Exception as e:
        log.error("Tool call %s failed: %s", function_name, e)
        return f"Sorry, I couldn't look that up right now."


def _tool_lookup_pricing(params: dict) -> str:
    """Look up what we've charged for similar items."""
    description = params.get("description", "")
    if not description:
        return "I need an item description to look up pricing."

    quotes = _load("quotes_log.json")
    desc_lower = description.lower()
    desc_tokens = set(desc_lower.split())

    matches = []
    for q in quotes:
        for item in q.get("items_detail", []):
            item_desc = item.get("description", "").lower()
            overlap = len(desc_tokens & set(item_desc.split()))
            if overlap >= 2:
                matches.append({
                    "description": item.get("description", ""),
                    "price": item.get("unit_price", 0),
                    "qty": item.get("qty", 0),
                    "quote": q.get("quote_number", ""),
                    "institution": q.get("institution", ""),
                    "date": q.get("date", ""),
                })

    if not matches:
        return f"I don't have historical pricing for '{description}' yet, but I can get a competitive quote to you quickly."

    matches.sort(key=lambda m: m["date"], reverse=True)
    results = matches[:3]
    lines = [f"I've supplied similar items before:"]
    for m in results:
        lines.append(f"  {m['description'][:50]} at ${m['price']:,.2f} each for {m['institution']}")

    return "\n".join(lines)


def _tool_check_quote_status(params: dict) -> str:
    """Check the status of a specific quote."""
    qn = params.get("quote_number", "")
    if not qn:
        return "I need a quote number to check."

    quotes = _load("quotes_log.json")
    qt = next((q for q in quotes if q.get("quote_number") == qn), None)
    if not qt:
        return f"I don't see quote {qn} in our system."

    status = qt.get("status", "unknown")
    total = qt.get("total", 0)
    return f"Quote {qn} is currently {status}, total ${total:,.2f}, submitted on {qt.get('date', 'unknown')}."


def _tool_get_institution_info(params: dict) -> str:
    """Get info about an institution."""
    inst = params.get("institution", "")
    if not inst:
        return "Which institution would you like information about?"

    context = _get_institution_context(inst)
    return context if context else f"I don't have data on {inst} yet."


def _tool_schedule_follow_up(params: dict) -> str:
    """Note a follow-up request."""
    when = params.get("when", "")
    topic = params.get("topic", "")
    return f"Got it — I'll note that for a follow-up{' about ' + topic if topic else ''}{' on ' + when if when else ''}. Our team will reach out."


def _tool_check_order_status(params: dict) -> str:
    """Check order delivery status."""
    order_id = params.get("order_id", "") or params.get("po_number", "")
    if not order_id:
        return "I need an order ID or PO number to check."

    orders = _load("orders.json")
    if isinstance(orders, dict):
        # Search by order ID or PO number
        for oid, order in orders.items():
            if order_id.lower() in oid.lower() or order_id.lower() in order.get("po_number", "").lower():
                status = order.get("status", "unknown")
                items = order.get("line_items", [])
                delivered = sum(1 for i in items if i.get("sourcing_status") == "delivered")
                return f"Order {oid}: {status}. {delivered} of {len(items)} items delivered."

    return f"I don't see an order matching {order_id}."


# ─── Vapi Tool Definitions (sent with assistant config) ────────────────────

VAPI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "lookup_pricing",
            "description": "Look up what Reytech has charged for similar items in the past. Use when a buyer asks about pricing for a specific item.",
            "parameters": {
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": "The item description to search for"
                    }
                },
                "required": ["description"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_quote_status",
            "description": "Check the current status of a Reytech quote. Use when following up on a specific quote.",
            "parameters": {
                "type": "object",
                "properties": {
                    "quote_number": {
                        "type": "string",
                        "description": "The quote number (e.g. R26Q1)"
                    }
                },
                "required": ["quote_number"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_institution_info",
            "description": "Get Reytech's history with a specific institution. Use when you need to know our track record.",
            "parameters": {
                "type": "object",
                "properties": {
                    "institution": {
                        "type": "string",
                        "description": "The institution name (e.g. CSP-Sacramento)"
                    }
                },
                "required": ["institution"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "schedule_follow_up",
            "description": "Note a follow-up request from the buyer. Use when they want a callback or more info.",
            "parameters": {
                "type": "object",
                "properties": {
                    "when": {"type": "string", "description": "When to follow up"},
                    "topic": {"type": "string", "description": "What to follow up about"}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_order_status",
            "description": "Check delivery status of an order. Use when buyer asks about shipping or delivery.",
            "parameters": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string", "description": "Order ID or PO number"}
                },
                "required": ["order_id"]
            }
        }
    },
]
