"""
voice_knowledge.py — Knowledge layer for Reytech Voice Agent
Phase 18→27 | Version: 3.0.0

Provides real-time data access for AI phone calls.
Now backed by SQLite (via agent_context + core/db) instead of raw JSON.

Builds rich context from the FULL DB:
  - Quote history (SQLite quotes table)
  - CRM contacts (SQLite contacts table)
  - Price history (SQLite price_history table)
  - Orders (SQLite orders table)
  - Activity log (SQLite activity_log table)
  - SCPRS intel (intel_buyers.json via agent_context)
  - Revenue goal (intel_revenue.json via agent_context)
  - Voice call log (voice_call_log.json)
  - QuickBooks financial context (qb_context_cache.json)

Two modes:
  1. Pre-call context: injected into system prompt before dialing
  2. Mid-call tools: Vapi function calling for live lookups
"""

import os
import json
import logging
from datetime import datetime, timedelta

log = logging.getLogger("voice_knowledge")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# ── Shared DB Context ──────────────────────────────────────────────────────
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
    from src.core.db import get_db, get_all_quotes_db, get_price_history_db
    HAS_DB = True
except ImportError:
    HAS_DB = False


def _load(filename: str) -> list | dict:
    """JSON fallback loader for files not yet in SQLite."""
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
    Pulls from SQLite (via agent_context + core/db) with JSON fallback.
    """
    sections = []

    # ── Full system context snapshot ──────────────────────────────────────
    if HAS_AGENT_CTX:
        try:
            ctx = get_context(
                include_contacts=True, include_quotes=True,
                include_revenue=True,
                include_prices=bool(institution),
                price_query=institution or "",
            )
            # Revenue goal
            rev = ctx.get("revenue", {})
            if rev:
                pct = rev.get("pct", 0)
                sections.append(
                    f"REYTECH REVENUE GOAL {rev.get('year','')}: "
                    f"${rev.get('goal',2_000_000):,.0f} | "
                    f"Closed: ${rev.get('closed',0):,.0f} ({pct:.1f}%) | "
                    f"Monthly needed: ${rev.get('monthly_needed',181818):,.0f}"
                )
            # Quote pipeline
            qt = ctx.get("quotes", {})
            if qt:
                sections.append(
                    f"PIPELINE: {qt.get('total',0)} quotes — "
                    f"{qt.get('sent',0)} sent, {qt.get('won',0)} won, "
                    f"{qt.get('lost',0)} lost | "
                    f"Win rate: {qt.get('win_rate',0)}% | "
                    f"Pipeline value: ${qt.get('pipeline_value',0):,.0f}"
                )
            # Intel
            intel = ctx.get("intel", {})
            if intel:
                sections.append(
                    f"SCPRS INTEL: {intel.get('total_buyers',0)} buyers tracked | "
                    f"Top categories: {', '.join(intel.get('top_categories',[])[:4])}"
                )
        except Exception as e:
            log.debug("agent_context error: %s", e)

    # ── Institution / Agency History ──────────────────────────────────────
    if institution:
        inst_ctx = _get_institution_context(institution)
        if inst_ctx:
            sections.append(inst_ctx)

    # ── Specific Quote Details ────────────────────────────────────────────
    if quote_number:
        qt_ctx = _get_quote_context(quote_number)
        if qt_ctx:
            sections.append(qt_ctx)

    # ── Buyer Info (CRM) ──────────────────────────────────────────────────
    if buyer_name or buyer_email:
        buyer_ctx = _get_buyer_context(buyer_name, buyer_email, institution)
        if buyer_ctx:
            sections.append(buyer_ctx)

    # ── SCPRS Lead / PO Intel ─────────────────────────────────────────────
    if po_number:
        lead_ctx = _get_lead_context(po_number)
        if lead_ctx:
            sections.append(lead_ctx)

    # ── Competitive Pricing Intel ─────────────────────────────────────────
    if institution:
        pricing = _get_pricing_context(institution)
        if pricing:
            sections.append(pricing)

    # ── Price History (SQLite) ────────────────────────────────────────────
    if institution and HAS_DB:
        ph = _get_price_history_context(institution)
        if ph:
            sections.append(ph)

    # ── Recent Call History ───────────────────────────────────────────────
    if institution:
        call_ctx = _get_call_history_context(institution)
        if call_ctx:
            sections.append(call_ctx)

    # ── Orders (SQLite) ───────────────────────────────────────────────────
    if institution:
        order_ctx = _get_order_context(institution)
        if order_ctx:
            sections.append(order_ctx)

    # ── Email Communication History (for CS dispute resolution) ──────────────
    if buyer_email or quote_number or po_number:
        try:
            from src.agents.notify_agent import build_cs_communication_summary
            comm = build_cs_communication_summary(buyer_email, quote_number, po_number)
            if comm:
                sections.append(comm)
        except Exception:
            pass

    # ── Financial Context (QB) ────────────────────────────────────────────
    financial = _get_financial_context(institution)
    if financial:
        sections.append(financial)

    if not sections:
        return ""

    return "\n\n--- REYTECH KNOWLEDGE BASE (LIVE DB) ---\n" + "\n\n".join(sections)


def _get_institution_context(institution: str) -> str:
    """Get full history with this institution from SQLite."""
    inst_lower = institution.lower()

    # Try SQLite first
    if HAS_DB:
        try:
            with get_db() as conn:
                rows = conn.execute("""
                    SELECT quote_number, created_at, agency, institution,
                           total, status, po_number, items_text, items_count
                    FROM quotes
                    WHERE lower(institution) LIKE ? OR lower(agency) LIKE ?
                    ORDER BY created_at DESC
                    LIMIT 20
                """, (f"%{inst_lower}%", f"%{inst_lower}%")).fetchall()
                if rows:
                    quotes = [dict(r) for r in rows]
                    won = [q for q in quotes if q.get("status") == "won"]
                    lost = [q for q in quotes if q.get("status") == "lost"]
                    pending = [q for q in quotes if q.get("status") in ("pending","sent","draft")]
                    total_won = sum(q.get("total",0) for q in won)
                    total_quoted = sum(q.get("total",0) for q in quotes)
                    decided = len(won) + len(lost)
                    win_rate = round(len(won)/decided*100) if decided else 0

                    lines = [f"INSTITUTION: {institution}"]
                    lines.append(f"History: {len(quotes)} quotes — {len(won)} won, {len(lost)} lost, {len(pending)} pending")
                    lines.append(f"Total quoted: ${total_quoted:,.0f} | Won: ${total_won:,.0f} | Win rate: {win_rate}%")
                    lines.append("Recent quotes:")
                    for q in quotes[:4]:
                        lines.append(f"  - {q.get('quote_number','?')} ({q.get('created_at','?')[:10]}) ${q.get('total',0):,.0f} [{q.get('status','?')}]")
                    return "\n".join(lines)
        except Exception as e:
            log.debug("SQLite institution context: %s", e)

    # JSON fallback
    quotes = _load("quotes_log.json")
    if not quotes:
        return f"INSTITUTION: {institution}\nNo prior quote history. New relationship."
    inst_quotes = [q for q in quotes if inst_lower in q.get("institution","").lower()
                   or inst_lower in q.get("ship_to_name","").lower()]
    if not inst_quotes:
        return f"INSTITUTION: {institution}\nNo prior quote history. New relationship."

    won = [q for q in inst_quotes if q.get("status") == "won"]
    lost = [q for q in inst_quotes if q.get("status") == "lost"]
    pending = [q for q in inst_quotes if q.get("status") in ("pending","sent")]
    total_quoted = sum(q.get("total",0) for q in inst_quotes)
    total_won = sum(q.get("total",0) for q in won)
    decided = len(won) + len(lost)

    lines = [f"INSTITUTION: {institution}"]
    lines.append(f"History: {len(inst_quotes)} quotes — {len(won)} won, {len(lost)} lost, {len(pending)} pending")
    lines.append(f"Total quoted: ${total_quoted:,.0f} | Won: ${total_won:,.0f}")
    if decided:
        lines.append(f"Win rate: {round(len(won)/decided*100)}%")
    recent = sorted(inst_quotes, key=lambda q: q.get("created_at",""), reverse=True)[:4]
    lines.append("Recent quotes:")
    for q in recent:
        lines.append(f"  - {q.get('quote_number','?')} ({q.get('date','?')}) ${q.get('total',0):,.0f} [{q.get('status','?')}]")
    return "\n".join(lines)


def _get_quote_context(quote_number: str) -> str:
    """Get full details of a specific quote from SQLite."""
    if HAS_DB:
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT * FROM quotes WHERE quote_number=?", (quote_number,)
                ).fetchone()
                if row:
                    qt = dict(row)
                    lines = [f"QUOTE: {quote_number}"]
                    lines.append(f"Date: {(qt.get('created_at') or '')[:10]} | Status: {qt.get('status','?')}")
                    lines.append(f"Agency: {qt.get('agency','?')} | Institution: {qt.get('institution','?')}")
                    lines.append(f"Total: ${qt.get('total',0):,.2f} | Items: {qt.get('items_count',0)}")
                    if qt.get("po_number"): lines.append(f"PO: {qt['po_number']}")
                    if qt.get("rfq_number"): lines.append(f"RFQ: {qt['rfq_number']}")
                    if qt.get("contact_email"): lines.append(f"Contact: {qt.get('requestor','')} {qt.get('contact_email','')}")
                    try:
                        items = json.loads(qt.get("items_detail") or "[]")
                        if items:
                            lines.append("Line items:")
                            for it in items[:8]:
                                desc = it.get("description","")[:60]
                                lines.append(f"  - {desc} (Qty {it.get('qty',it.get('quantity',0))}) ${it.get('unit_price',it.get('our_price',0)):,.2f}")
                    except Exception:
                        pass
                    return "\n".join(lines)
        except Exception as e:
            log.debug("SQLite quote context: %s", e)

    # JSON fallback
    quotes = _load("quotes_log.json")
    qt = next((q for q in quotes if q.get("quote_number") == quote_number), None)
    if not qt:
        return ""
    lines = [f"QUOTE: {quote_number}"]
    lines.append(f"Date: {qt.get('date','?')} | Status: {qt.get('status','?')}")
    lines.append(f"Institution: {qt.get('institution','?')}")
    lines.append(f"Total: ${qt.get('total',0):,.2f} | Items: {qt.get('items_count',0)}")
    if qt.get("po_number"): lines.append(f"PO: {qt['po_number']}")
    items = qt.get("items_detail",[])
    if items:
        lines.append("Line items:")
        for it in items[:8]:
            lines.append(f"  - {it.get('description','')[:60]} (Qty {it.get('qty',0)}) ${it.get('unit_price',0):,.2f}")
    return "\n".join(lines)


def _get_buyer_context(name: str, email: str, institution: str) -> str:
    """Get buyer info from SQLite contacts first, then customers.json."""
    # SQLite contacts
    if HAS_AGENT_CTX:
        try:
            contacts = get_contact_by_agency(institution) if institution else []
            if not contacts and (name or email):
                ctx = get_context(include_contacts=True)
                all_contacts = ctx.get("contacts", [])
                name_l = (name or "").lower()
                email_l = (email or "").lower()
                contacts = [c for c in all_contacts if
                           (name_l and name_l in (c.get("name") or "").lower()) or
                           (email_l and email_l == (c.get("email") or "").lower())]
            if contacts:
                c = contacts[0]
                lines = [f"CRM CONTACT: {c.get('name',name)}"]
                if c.get("email"): lines.append(f"Email: {c['email']}")
                if c.get("phone"): lines.append(f"Phone: {c['phone']}")
                if c.get("agency"): lines.append(f"Agency: {c['agency']}")
                if c.get("spend"): lines.append(f"Annual spend: ${c['spend']:,.0f}")
                if c.get("categories"): lines.append(f"Buys: {', '.join(c['categories'][:4])}")
                if c.get("status"): lines.append(f"Outreach status: {c['status']}")
                if c.get("po_count"): lines.append(f"PO count: {c['po_count']}")
                return "\n".join(lines)
        except Exception as e:
            log.debug("CRM contact context: %s", e)

    # customers.json fallback
    customers = _load("customers.json")
    name_l = (name or "").lower()
    email_l = (email or "").lower()
    matches = []
    for c in (customers if isinstance(customers, list) else []):
        if isinstance(c, dict):
            if name_l and name_l in c.get("display_name","").lower():
                matches.append(c)
            elif email_l and email_l == c.get("email","").lower():
                matches.append(c)
    if not matches:
        return ""
    buyer = matches[0]
    lines = [f"BUYER: {buyer.get('display_name', name)}"]
    if buyer.get("email"): lines.append(f"Email: {buyer['email']}")
    if buyer.get("phone"): lines.append(f"Phone: {buyer['phone']}")
    if buyer.get("open_balance"): lines.append(f"Open balance: ${buyer['open_balance']:,.2f}")
    return "\n".join(lines)


def _get_lead_context(po_number: str) -> str:
    """Get SCPRS lead intel for a PO number."""
    leads = _load("leads.json")
    if not leads:
        return ""
    lead = None
    for l in (leads if isinstance(leads, list) else leads.values()):
        if isinstance(l, dict) and po_number.lower() in l.get("po_number","").lower():
            lead = l
            break
    if not lead:
        return ""
    lines = [f"SCPRS LEAD: {po_number}"]
    lines.append(f"Institution: {lead.get('institution','?')}")
    lines.append(f"Value: ${lead.get('po_value',0):,.0f} | Items: {lead.get('items_count',0)}")
    lines.append(f"Score: {lead.get('score',0):.0%} | Category: {lead.get('category','?')}")
    if lead.get("matched_items"):
        lines.append("Matched items we carry:")
        for item in lead["matched_items"][:5]:
            if isinstance(item, dict):
                lines.append(f"  - {item.get('description','')[:60]}")
    if lead.get("estimated_savings_pct"):
        lines.append(f"Estimated savings vs current vendor: {lead['estimated_savings_pct']:.0%}")
    return "\n".join(lines)


def _get_pricing_context(institution: str) -> str:
    """Get SCPRS competitive pricing intel for this institution."""
    if HAS_AGENT_CTX:
        try:
            ctx = get_context(include_prices=True, price_query=institution)
            ph = ctx.get("prices", {})
            if ph.get("count",0) > 0:
                lines = [f"SCPRS PRICE INTEL for {institution} items:"]
                lines.append(f"  Best price seen: ${ph.get('best_price',0):,.2f} | Avg: ${ph.get('avg_price',0):,.2f}")
                for r in ph.get("results",[])[:4]:
                    lines.append(f"  - {r.get('description','')[:50]}: ${r.get('unit_price',0):,.2f} ({r.get('source','')})")
                return "\n".join(lines)
        except Exception:
            pass

    # scprs_prices.json fallback
    scprs = _load("scprs_prices.json")
    if not scprs:
        return ""
    inst_l = institution.lower()
    relevant = [p for p in (scprs if isinstance(scprs, list) else [])
                if isinstance(p, dict) and inst_l in (p.get("department","") or p.get("institution","")).lower()]
    if not relevant:
        return ""
    lines = ["COMPETITIVE PRICING INTEL:"]
    for p in relevant[:5]:
        lines.append(f"  - {p.get('description','')[:50]}: ${p.get('unit_price',p.get('price',0)):,.2f} (vendor: {p.get('vendor','?')})")
    return "\n".join(lines)


def _get_price_history_context(institution: str) -> str:
    """Pull price history from SQLite for items this institution buys."""
    if not HAS_DB:
        return ""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT description, unit_price, source, found_at
                FROM price_history
                WHERE lower(agency) LIKE ?
                ORDER BY found_at DESC
                LIMIT 8
            """, (f"%{institution.lower()}%",)).fetchall()
            if not rows:
                return ""
            lines = [f"PRICE HISTORY (SQLite) for {institution}:"]
            for r in rows:
                lines.append(f"  - {r['description'][:50]}: ${r['unit_price']:,.2f} ({r['source']})")
            return "\n".join(lines)
    except Exception as e:
        log.debug("Price history context: %s", e)
        return ""


def _get_call_history_context(institution: str) -> str:
    """Show recent voice calls to this institution."""
    try:
        call_log_path = os.path.join(DATA_DIR, "voice_call_log.json")
        with open(call_log_path) as f:
            calls = json.load(f)
        inst_l = institution.lower()
        related = [c for c in (calls if isinstance(calls, list) else [])
                   if inst_l in (c.get("institution","") or c.get("to","")).lower()]
        if not related:
            return ""
        related = sorted(related, key=lambda c: c.get("timestamp",""), reverse=True)[:3]
        lines = [f"RECENT CALLS to {institution}:"]
        for c in related:
            ts = (c.get("timestamp","") or "")[:10]
            script = c.get("script","?")
            status = c.get("status","?")
            lines.append(f"  - {ts}: {script} → {status}")
        return "\n".join(lines)
    except Exception:
        return ""


def _get_order_context(institution: str) -> str:
    """Get active orders for this institution from SQLite."""
    if not HAS_DB:
        return ""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, created_at, status, quote_number, total, items_count
                FROM orders
                WHERE lower(institution) LIKE ? OR lower(agency) LIKE ?
                ORDER BY created_at DESC
                LIMIT 5
            """, (f"%{institution.lower()}%", f"%{institution.lower()}%")).fetchall()
            if not rows:
                return ""
            lines = [f"ORDERS for {institution}:"]
            for r in rows:
                lines.append(f"  - Order {r['id']}: {r.get('status','?')} | ${r.get('total',0):,.0f} | {(r.get('created_at') or '')[:10]}")
            return "\n".join(lines)
    except Exception as e:
        log.debug("Order context: %s", e)
        return ""


def _get_financial_context(institution: str = "") -> str:
    """Get QuickBooks financial context."""
    try:
        with open(os.path.join(DATA_DIR, "qb_context_cache.json")) as f:
            ctx = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return ""
    if not ctx.get("ok"):
        return ""
    lines = ["QUICKBOOKS FINANCIAL STATUS:"]
    lines.append(f"Total receivable: ${ctx.get('total_receivable',0):,.2f}")
    lines.append(f"Overdue: ${ctx.get('overdue_amount',0):,.2f} | Open invoices: {ctx.get('open_invoices',0)}")
    if institution:
        inst_l = institution.lower()
        pending = ctx.get("pending_invoices", [])
        inst_inv = [i for i in pending if inst_l in (i.get("customer","") or "").lower()]
        if inst_inv:
            lines.append(f"\n⚠️ {institution} has {len(inst_inv)} open invoice(s):")
            for inv in inst_inv[:3]:
                lines.append(f"  Invoice #{inv['doc_number']}: ${inv['balance']:,.2f} due {inv['due_date']} ({inv['status']})")
            lines.append("Note: Do NOT mention unpaid invoices on sales calls unprompted.")
    return "\n".join(lines)


# ─── Mid-Call Tool Functions (for Vapi function calling) ────────────────────

def handle_tool_call(function_name: str, parameters: dict) -> str:
    """Handle a Vapi function call during a live conversation."""
    handlers = {
        "lookup_pricing": _tool_lookup_pricing,
        "check_quote_status": _tool_check_quote_status,
        "get_institution_info": _tool_get_institution_info,
        "schedule_follow_up": _tool_schedule_follow_up,
        "check_order_status": _tool_check_order_status,
        "get_crm_contact": _tool_get_crm_contact,
        "get_best_price_for_item": _tool_get_best_price,
        "get_recent_po": _tool_get_recent_po,
    }
    handler = handlers.get(function_name)
    if not handler:
        return f"Function {function_name} not available."
    try:
        return handler(parameters)
    except Exception as e:
        log.error("Tool call %s failed: %s", function_name, e)
        return "Sorry, I couldn't look that up right now."


def _tool_lookup_pricing(params: dict) -> str:
    description = params.get("description", "")
    if not description:
        return "I need an item description to look up pricing."

    # Try SQLite price_history first
    if HAS_DB:
        try:
            rows = get_price_history_db(description=description, limit=5)
            if rows:
                lines = [f"Pricing history for '{description}':"]
                for r in rows:
                    lines.append(f"  {r.get('description','')[:50]} — ${r.get('unit_price',0):,.2f} ({r.get('source','')})")
                return "\n".join(lines)
        except Exception:
            pass

    # Quote history fallback
    if HAS_DB:
        try:
            with get_db() as conn:
                rows = conn.execute("""
                    SELECT description, unit_price, quote_number, agency, found_at
                    FROM price_history
                    WHERE lower(description) LIKE ?
                    ORDER BY unit_price ASC LIMIT 5
                """, (f"%{description.lower()}%",)).fetchall()
                if rows:
                    lines = [f"I've seen '{description}' priced at:"]
                    for r in rows:
                        lines.append(f"  ${r['unit_price']:,.2f} ({r['source'] or r['agency'] or '?'})")
                    return "\n".join(lines)
        except Exception:
            pass

    # JSON fallback
    quotes = _load("quotes_log.json")
    desc_lower = description.lower()
    desc_tokens = set(desc_lower.split())
    matches = []
    for q in quotes:
        for item in q.get("items_detail", []):
            item_desc = item.get("description","").lower()
            overlap = len(desc_tokens & set(item_desc.split()))
            if overlap >= 2:
                matches.append({
                    "description": item.get("description",""),
                    "price": item.get("unit_price",0),
                    "institution": q.get("institution",""),
                    "date": q.get("date",""),
                })
    if not matches:
        return f"No historical pricing for '{description}' — I can get a competitive quote quickly."
    matches.sort(key=lambda m: m["date"], reverse=True)
    lines = [f"I've supplied similar items:"]
    for m in matches[:3]:
        lines.append(f"  {m['description'][:50]} at ${m['price']:,.2f} for {m['institution']}")
    return "\n".join(lines)


def _tool_check_quote_status(params: dict) -> str:
    qn = params.get("quote_number","")
    if not qn:
        return "I need a quote number to check."
    if HAS_DB:
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT status, total, created_at, institution, po_number FROM quotes WHERE quote_number=?",
                    (qn,)
                ).fetchone()
                if row:
                    msg = f"Quote {qn}: {row['status']} — ${row['total']:,.2f}, submitted {(row['created_at'] or '')[:10]}"
                    if row.get("po_number"):
                        msg += f", PO: {row['po_number']}"
                    return msg
        except Exception:
            pass
    quotes = _load("quotes_log.json")
    qt = next((q for q in quotes if q.get("quote_number") == qn), None)
    if not qt:
        return f"I don't see quote {qn} in our system."
    return f"Quote {qn}: {qt.get('status','unknown')} — ${qt.get('total',0):,.2f}, submitted {qt.get('date','?')}."


def _tool_get_institution_info(params: dict) -> str:
    inst = params.get("institution","")
    if not inst:
        return "Which institution do you mean?"
    ctx = _get_institution_context(inst)
    return ctx if ctx else f"No data on {inst} yet."


def _tool_schedule_follow_up(params: dict) -> str:
    when = params.get("when","")
    topic = params.get("topic","")
    # Log to activity_log if we can
    try:
        from src.core.db import log_activity
        log_activity(
            contact_id="voice_call",
            event_type="follow_up_requested",
            subject=f"Follow-up requested: {topic}",
            body=f"When: {when}",
            actor="voice_agent",
        )
    except Exception:
        pass
    return f"Got it — noted for follow-up{' about ' + topic if topic else ''}{' on ' + when if when else ''}. Our team will reach out."


def _tool_check_order_status(params: dict) -> str:
    order_id = params.get("order_id","") or params.get("po_number","")
    if not order_id:
        return "I need an order ID or PO number."
    if HAS_DB:
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT id, status, items_count, created_at FROM orders WHERE id=? OR quote_number=?",
                    (order_id, order_id)
                ).fetchone()
                if row:
                    return f"Order {row['id']}: {row['status']}, {row['items_count']} items, placed {(row['created_at'] or '')[:10]}."
        except Exception:
            pass
    # JSON fallback
    orders = _load("orders.json")
    if isinstance(orders, dict):
        for oid, order in orders.items():
            if order_id.lower() in oid.lower() or order_id.lower() in order.get("po_number","").lower():
                status = order.get("status","unknown")
                items = order.get("line_items",[])
                delivered = sum(1 for i in items if i.get("sourcing_status") == "delivered")
                return f"Order {oid}: {status}. {delivered} of {len(items)} items delivered."
    return f"No order matching {order_id} found."


def _tool_get_crm_contact(params: dict) -> str:
    """New tool: look up CRM contact during call."""
    name = params.get("name","")
    agency = params.get("agency","")
    if not name and not agency:
        return "I need a name or agency to look up."
    if HAS_AGENT_CTX:
        contacts = get_contact_by_agency(agency) if agency else []
        if contacts:
            c = contacts[0]
            return (f"CRM: {c.get('name','')} @ {c.get('agency','')} | "
                    f"Email: {c.get('email','')} | Spend: ${c.get('spend',0):,.0f}/yr | "
                    f"Buys: {', '.join((c.get('categories') or [])[:3])}")
    return f"No CRM contact found for {name or agency}."


def _tool_get_best_price(params: dict) -> str:
    """New tool: get best price we've ever seen for an item."""
    desc = params.get("description","")
    if not desc:
        return "I need an item description."
    if HAS_AGENT_CTX:
        best = get_best_price(desc)
        if best:
            return f"Best price we've seen for '{desc}': ${best:,.2f}"
    return f"No price history for '{desc}' — I can quote it fresh."


def _tool_get_recent_po(params: dict) -> str:
    """New tool: get most recent PO from this institution."""
    institution = params.get("institution","")
    if not institution:
        return "Which institution?"
    ctx = _get_institution_context(institution)
    return ctx if ctx else f"No PO history with {institution} yet."


# ─── Vapi Tool Definitions (sent with assistant config) ────────────────────

VAPI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "lookup_pricing",
            "description": "Look up what Reytech has charged for similar items. Use when buyer asks about pricing.",
            "parameters": {
                "type": "object",
                "properties": {"description": {"type": "string", "description": "Item description to search"}},
                "required": ["description"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_quote_status",
            "description": "Check the current status of a specific Reytech quote.",
            "parameters": {
                "type": "object",
                "properties": {"quote_number": {"type": "string", "description": "Quote number e.g. R26Q1"}},
                "required": ["quote_number"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_institution_info",
            "description": "Get Reytech's full history with an institution — quotes, won, lost, pipeline.",
            "parameters": {
                "type": "object",
                "properties": {"institution": {"type": "string", "description": "Institution name"}},
                "required": ["institution"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "schedule_follow_up",
            "description": "Log a follow-up request from the buyer.",
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
            "description": "Check delivery/order status by order ID or PO number.",
            "parameters": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string", "description": "Order ID or PO number"}
                },
                "required": ["order_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_crm_contact",
            "description": "Look up a buyer in the Reytech CRM by name or agency.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Buyer name"},
                    "agency": {"type": "string", "description": "Agency or institution name"}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_best_price_for_item",
            "description": "Get the best (lowest) price Reytech has ever seen for an item.",
            "parameters": {
                "type": "object",
                "properties": {
                    "description": {"type": "string", "description": "Item description"}
                },
                "required": ["description"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_po",
            "description": "Get the most recent purchase order history with an institution.",
            "parameters": {
                "type": "object",
                "properties": {
                    "institution": {"type": "string", "description": "Institution name"}
                },
                "required": ["institution"]
            }
        }
    },
]
