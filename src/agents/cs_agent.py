"""
cs_agent.py — Inbound Customer Service Agent for Reytech
Phase 27 | Version: 1.0.0

Handles INBOUND email and call update requests automatically:
  - Order status inquiries → auto-draft reply with current status
  - Delivery confirmations → auto-draft with tracking info
  - Invoice/payment questions → auto-draft with invoice details
  - Quote status checks → auto-draft with current quote status
  - General questions → auto-draft professional response

Architecture:
  1. email_poller detects inbound "update request" (not RFQ, not shipping)
  2. classify_inbound_email() → determines intent + entity (PO#, quote#, etc.)
  3. build_cs_response_draft() → pulls live DB data, drafts professional reply
  4. Draft saved to email_outbox.json with status "cs_draft" for Mike to review
  5. Optional: place_cs_call() for inbound voice support using Vapi

CS Voice Agent uses the same Vapi infrastructure as the outbound sales agent
but with a DIFFERENT persona — professional customer service, not sales.
"""

import os
import json
import logging
import re
import uuid
from datetime import datetime
from typing import Optional

log = logging.getLogger("cs_agent")

# ── JSON→SQLite compatibility (Phase 32c migration) ──────────────────────────
try:
    from src.core.db import (
        get_all_customers, get_all_vendors, get_all_price_checks, get_price_check,
        upsert_price_check, get_outbox, upsert_outbox_email, update_outbox_status,
        get_email_templates, upsert_email_template, get_vendor_registrations,
        upsert_vendor_registration, get_market_intelligence, upsert_market_intelligence,
        get_intel_agencies, upsert_intel_agency, get_growth_outreach, save_growth_campaign,
        get_qa_reports, save_qa_report, get_latest_qa_report,
        upsert_customer, upsert_vendor,
    )
    _HAS_DB_DAL = True
except ImportError:
    _HAS_DB_DAL = False
# ─────────────────────────────────────────────────────────────────────────────

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
    from src.core.db import get_db, log_activity
    HAS_DB = True
except ImportError:
    HAS_DB = False
    def log_activity(*a, **kw): pass

OUTBOX_FILE = os.path.join(DATA_DIR, "email_outbox.json")
CS_LOG_FILE = os.path.join(DATA_DIR, "cs_log.json")

# ─── Intent Classification ─────────────────────────────────────────────────

# Patterns to detect inbound update requests (not RFQs, not shipping)
UPDATE_REQUEST_PATTERNS = [
    # Order status
    r"(?:order|purchase order|po)\s+(?:status|update|tracking|where|when)",
    r"(?:where|when).{0,30}(?:order|shipment|package|delivery)",
    r"(?:has|have).{0,20}(?:shipped|been shipped|been sent|been dispatched)",
    r"(?:status|update).{0,30}(?:order|po|quote|invoice)",
    r"(?:any update|any news|heard anything|any word).{0,40}(?:order|po|quote)",
    # Delivery / tracking
    r"tracking\s+(?:number|info|information|update)",
    r"(?:expected|estimated).{0,20}(?:delivery|arrival|ship)",
    r"(?:arrive|arrival|delivery).{0,30}(?:when|date|timeline|estimate)",
    r"(?:received|delivered|arrived).{0,20}(?:yet|still waiting|not yet)",
    # Invoice / payment
    r"invoice\s+(?:status|update|number|#|\d+)",
    r"(?:payment|invoice).{0,30}(?:received|processed|posted|cleared)",
    r"(?:when|have).{0,30}(?:invoice|payment|bill)\b",
    # Quote status
    r"quote\s+(?:status|update|still valid|expired)",
    r"(?:following up|checking in).{0,40}(?:quote|pricing|proposal)",
    r"(?:heard back|any update).{0,40}quote",
    # General CS signals
    r"(?:need to know|please advise|please update|let me know).{0,50}(?:status|update|when|where)",
    r"(?:have not|haven't).{0,30}(?:received|heard|seen|gotten)",
    # Broader customer service patterns
    r"(?:can you|could you|would you).{0,30}(?:send|provide|check|confirm|verify|update|help)",
    r"(?:question|inquiry|asking).{0,30}(?:about|regarding|on|for)",
    r"(?:need|looking for|requesting).{0,30}(?:information|help|assistance|clarification)",
    r"(?:do you|can you).{0,20}(?:carry|stock|sell|have|offer)",
    r"(?:price|pricing|cost|how much).{0,30}(?:for|on|of)\b",
    r"(?:catalog|product list|item list|availability)",
    r"(?:return|exchange|credit|refund|warranty|replacement)",
    r"(?:urgent|asap|rush|expedite|time sensitive|priority)",
]

UPDATE_PATTERNS_COMPILED = [re.compile(p, re.I) for p in UPDATE_REQUEST_PATTERNS]


def is_update_request(subject: str, body: str) -> bool:
    """Return True if this email is an inbound status/update request (not RFQ, not spam)."""
    text = f"{subject} {body[:800]}"
    for pattern in UPDATE_PATTERNS_COMPILED:
        if pattern.search(text):
            return True
    return False


def classify_inbound_email(subject: str, body: str, sender: str = "") -> dict:
    """
    Classify an inbound email and extract key entities.

    Returns:
        {
          intent: "order_status" | "delivery" | "invoice" | "quote_status" | "general",
          confidence: float,
          entities: {po_number, quote_number, invoice_number, tracking_number},
          is_update_request: bool,
          sender_name: str,
          sender_email: str,
        }
    """
    text = f"{subject} {body[:1500]}"

    # Extract entities
    po_numbers = re.findall(r'\bPO[-\s#]?(\d{4,})\b|\bP\.O\.[-\s#]?(\d{4,})\b', text, re.I)
    po_numbers = [p[0] or p[1] for p in po_numbers]

    quote_numbers = re.findall(r'\b(R\d{2}Q\d+|Q-\d{4,}|QUOTE[-\s#]?\d+)\b', text, re.I)

    invoice_numbers = re.findall(r'\bINV[-\s#]?(\d{4,})\b|\bINVOICE[-\s#]?(\d{4,})\b', text, re.I)
    invoice_numbers = [i[0] or i[1] for i in invoice_numbers]

    tracking_numbers = re.findall(r'\b(1Z[A-Z0-9]{16}|9\d{21}|\d{22})\b', text)

    # Determine intent — body content takes priority over subject keywords
    text_l = text.lower()
    body_l = body[:1200].lower()

    # Invoice detected from entities or body content → always wins over "purchase order" in subject
    has_invoice_signal = bool(invoice_numbers) or any(
        w in body_l for w in ["invoice", "payment received", "billing", "payment status", "been paid", "ap department"]
    )
    has_delivery_signal = bool(tracking_numbers) or any(
        w in text_l for w in ["tracking", "tracking number", "has shipped", "been shipped", "been dispatched",
                               "delivery date", "estimated arrival", "expected arrival"]
    )
    has_quote_signal = bool(quote_numbers) or any(
        w in text_l for w in ["quote", "pricing", "proposal", "bid", "quotation"]
    )
    has_order_signal = bool(po_numbers) or any(
        w in text_l for w in ["order status", "order update", "where is my order", "when will my order",
                               "purchase order status"]
    )

    if has_delivery_signal:
        intent = "delivery"
    elif has_invoice_signal:
        intent = "invoice"
    elif has_quote_signal:
        intent = "quote_status"
    elif has_order_signal:
        intent = "order_status"
    else:
        intent = "general"

    # Parse sender
    sender_email = ""
    sender_name = ""
    em = re.search(r'[\w.+-]+@[\w.-]+\.\w+', sender)
    if em:
        sender_email = em.group(0)
    name_m = re.match(r'^([^<]+)<', sender.strip())
    if name_m:
        sender_name = name_m.group(1).strip().strip('"')

    return {
        "intent": intent,
        "is_update_request": is_update_request(subject, body),
        "entities": {
            "po_numbers": po_numbers,
            "quote_numbers": [q.upper() for q in quote_numbers],
            "invoice_numbers": invoice_numbers,
            "tracking_numbers": tracking_numbers,
        },
        "sender_email": sender_email,
        "sender_name": sender_name,
    }


# ─── DB Lookup Helpers ─────────────────────────────────────────────────────

def _lookup_quote(quote_number: str) -> Optional[dict]:
    if not HAS_DB or not quote_number:
        return None
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM quotes WHERE quote_number=?", (quote_number,)
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def _lookup_order_by_po(po_number: str) -> Optional[dict]:
    if not HAS_DB or not po_number:
        return None
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE id=? OR quote_number IN (SELECT quote_number FROM quotes WHERE po_number=?)",
                (po_number, po_number)
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def _lookup_recent_quotes_for_sender(sender_email: str) -> list:
    if not HAS_DB or not sender_email:
        return []
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, created_at, agency, status, total, po_number
                FROM quotes
                WHERE lower(contact_email) = ?
                ORDER BY created_at DESC LIMIT 5
            """, (sender_email.lower(),)).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def _lookup_contact(sender_email: str) -> Optional[dict]:
    """Find CRM contact by email."""
    if HAS_AGENT_CTX and sender_email:
        try:
            ctx = get_context(include_contacts=True)
            for c in ctx.get("contacts", []):
                if (c.get("email") or "").lower() == sender_email.lower():
                    return c
        except Exception:
            pass
    return None


# ─── CS Response Draft Builder ────────────────────────────────────────────

CS_SIGNATURE = (
    "\n\nBest regards,\n"
    "Mike Guadan\n"
    "Reytech Inc. | CA SB/DVBE Certified\n"
    "sales@reytechinc.com | 949-229-1575\n"
    "www.reytechinc.com"
)


def build_cs_response_draft(
    classification: dict,
    subject: str,
    body: str,
    sender: str = "",
) -> dict:
    """
    Build a customer service response draft based on the email classification.
    Pulls live data from SQLite and returns a draft ready for Mike to review.

    Returns:
        {
          ok: bool,
          draft: {to, subject, body, intent, entities_resolved},
          auto_saved: bool,
          note: str,
        }
    """
    intent = classification.get("intent", "general")
    entities = classification.get("entities", {})
    sender_email = classification.get("sender_email", "")
    sender_name = classification.get("sender_name", "") or "there"

    first_name = sender_name.split()[0] if sender_name and sender_name != "there" else "there"

    draft_subject = f"Re: {subject}" if not subject.startswith("Re:") else subject
    entities_resolved = {}

    # ── ORDER STATUS ────────────────────────────────────────────────────────
    if intent == "order_status":
        po_nums = entities.get("po_numbers", [])
        order = _lookup_order_by_po(po_nums[0]) if po_nums else None

        if order:
            entities_resolved["order"] = order
            status = order.get("status", "processing")
            body_text = (
                f"Hi {first_name},\n\n"
                f"Thank you for reaching out. Here is an update on your order"
                + (f" (PO #{po_nums[0]})" if po_nums else "") + ":\n\n"
                f"Status: {status.title()}\n"
            )
            if status == "delivered":
                body_text += "Your order has been delivered. Please let us know if anything was missing or damaged.\n"
            elif status in ("shipped", "in_transit"):
                body_text += "Your order is on its way. Tracking information will follow separately if not already provided.\n"
            else:
                body_text += "Your order is being processed and will ship shortly. We will send tracking once it leaves our warehouse.\n"
            body_text += "\nIf you have any other questions, please don't hesitate to reach out." + CS_SIGNATURE
        else:
            # No DB match — generic helpful response
            po_ref = f" (PO #{po_nums[0]})" if po_nums else ""
            body_text = (
                f"Hi {first_name},\n\n"
                f"Thank you for your inquiry about your order{po_ref}. "
                f"I'm looking into the current status and will have an update for you shortly.\n\n"
                f"If you need immediate assistance, please call us at 949-229-1575." + CS_SIGNATURE
            )

    # ── DELIVERY ────────────────────────────────────────────────────────────
    elif intent == "delivery":
        tracking = entities.get("tracking_numbers", [])
        po_nums = entities.get("po_numbers", [])
        tracking_ref = tracking[0] if tracking else None
        po_ref = f" for PO #{po_nums[0]}" if po_nums else ""

        if tracking_ref:
            entities_resolved["tracking_number"] = tracking_ref
            body_text = (
                f"Hi {first_name},\n\n"
                f"Your tracking number{po_ref} is: {tracking_ref}\n\n"
                f"You can track your shipment at:\n"
                f"  UPS: https://www.ups.com/track?tracknum={tracking_ref}\n"
                f"  USPS: https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking_ref}\n"
                f"  FedEx: https://www.fedex.com/fedextrack/?tracknumbers={tracking_ref}\n\n"
                f"If your package has not arrived within the estimated timeframe, "
                f"please contact us and we will follow up with the carrier." + CS_SIGNATURE
            )
        else:
            body_text = (
                f"Hi {first_name},\n\n"
                f"Thank you for following up on your delivery{po_ref}. "
                f"I'll look into the shipping status and send you tracking information shortly.\n\n"
                f"For immediate assistance, call us at 949-229-1575." + CS_SIGNATURE
            )

    # ── INVOICE ─────────────────────────────────────────────────────────────
    elif intent == "invoice":
        inv_nums = entities.get("invoice_numbers", [])
        inv_ref = f" (Invoice #{inv_nums[0]})" if inv_nums else ""
        body_text = (
            f"Hi {first_name},\n\n"
            f"Thank you for reaching out about your invoice{inv_ref}.\n\n"
            f"Reytech accepts payment by check, ACH, or credit card. "
            f"If you have questions about a specific invoice, please reply with the invoice number "
            f"and I'll pull up the details right away.\n\n"
            f"For immediate assistance or to discuss payment terms, call us at 949-229-1575 or "
            f"email sales@reytechinc.com." + CS_SIGNATURE
        )
        if inv_nums:
            entities_resolved["invoice_numbers"] = inv_nums

    # ── EMAIL THREAD (for CS body context) ──────────────────────────────────
    prior_thread = []
    try:
        from src.agents.notify_agent import get_email_thread
        prior_thread = get_email_thread(contact_email=sender_email, limit=10)
    except Exception:
        pass

    # ── QUOTE STATUS ─────────────────────────────────────────────────────────
    if intent == "quote_status":
        q_nums = entities.get("quote_numbers", [])
        quote = _lookup_quote(q_nums[0]) if q_nums else None
        recent = _lookup_recent_quotes_for_sender(sender_email) if sender_email else []

        if quote:
            entities_resolved["quote"] = {
                "quote_number": quote.get("quote_number"),
                "status": quote.get("status"),
                "total": quote.get("total"),
                "created_at": (quote.get("created_at") or "")[:10],
            }
            status = quote.get("status","pending")
            total = quote.get("total", 0)
            q_num = quote.get("quote_number","")
            body_text = (
                f"Hi {first_name},\n\n"
                f"Thank you for following up on Quote {q_num}.\n\n"
                f"Current status: {status.title()}\n"
                f"Quote total: ${total:,.2f}\n"
                f"Submitted: {(quote.get('created_at') or '')[:10]}\n\n"
            )
            if status == "won":
                body_text += (
                    f"This quote has been converted to a confirmed order"
                    + (f" (PO #{quote.get('po_number')})" if quote.get("po_number") else "") + ". "
                    f"Thank you for the business!\n"
                )
            elif status in ("pending","sent","draft"):
                body_text += (
                    f"Our pricing is still valid. If you'd like to proceed or have any questions about "
                    f"specific items, please let me know and I can adjust quantities or clarify anything.\n"
                )
            elif status == "lost":
                body_text += (
                    f"It looks like this quote was closed out. If you have a new requirement, "
                    f"I'd be happy to put together fresh pricing — often we can do better than our initial quote.\n"
                )
            body_text += CS_SIGNATURE
        elif recent:
            # Show most recent quotes for this sender
            q = recent[0]
            body_text = (
                f"Hi {first_name},\n\n"
                f"Thank you for reaching out. The most recent quote on file for you is "
                f"{q.get('quote_number','')} (${q.get('total',0):,.2f}, status: {q.get('status','?')}, "
                f"submitted {(q.get('created_at') or '')[:10]}).\n\n"
                f"If you're asking about a different quote, please share the quote number and I'll pull it up immediately.\n"
                + CS_SIGNATURE
            )
        else:
            q_ref = f" ({q_nums[0]})" if q_nums else ""
            body_text = (
                f"Hi {first_name},\n\n"
                f"Thank you for following up on your quote{q_ref}. "
                f"I'll look this up and get back to you with a full status update shortly.\n\n"
                f"If you need pricing right away, call us at 949-229-1575." + CS_SIGNATURE
            )

    # ── GENERAL ─────────────────────────────────────────────────────────────
    else:
        body_text = (
            f"Hi {first_name},\n\n"
            f"Thank you for reaching out to Reytech. I received your message and will follow up shortly.\n\n"
            f"For immediate assistance:\n"
            f"  Phone: 949-229-1575\n"
            f"  Email: sales@reytechinc.com\n\n"
            f"We typically respond within 2 business hours." + CS_SIGNATURE
        )

    draft = {
        "id": f"cs_{uuid.uuid4().hex[:8]}",
        "status": "cs_draft",
        "type": "cs_response",
        "to": sender_email or sender,
        "subject": draft_subject,
        "body": body_text,
        "intent": intent,
        "entities_resolved": entities_resolved,
        "original_subject": subject,
        "original_sender": sender,
        "created_at": datetime.now().isoformat(),
        "note": f"Auto-drafted by CS agent | intent={intent} | review before sending",
    }

    # Save to outbox
    auto_saved = _save_cs_draft(draft)

    # Log to DB
    try:
        log_activity(
            contact_id=sender_email or "unknown",
            event_type="cs_auto_draft",
            subject=f"CS auto-draft: {intent} — {subject[:60]}",
            body=f"Entities: {entities} | Auto-drafted reply ready for review",
            actor="cs_agent",
            metadata={"intent": intent, "draft_id": draft["id"]},
        )
    except Exception:
        pass

    log.info("CS draft created: intent=%s, to=%s, draft_id=%s", intent, sender_email, draft["id"])

    # 🔔 Fire alert — SMS + email + bell
    try:
        from src.agents.notify_agent import send_alert, log_email_event
        send_alert(
            event_type="cs_draft_ready",
            title=f"📬 CS Draft Ready: {intent.replace('_',' ').title()}",
            body=f"Inbound from {sender_name or sender_email}: {subject[:80]}\nDraft reply ready for your review.",
            urgency="urgent",
            context={
                "intent": intent,
                "contact": sender,
                "entity_id": draft["id"],
                **{k: v for k, v in entities_resolved.items() if isinstance(v, str)},
            },
            cooldown_key=f"cs_draft_{sender_email}",
        )
        # Log the received email for CS dispute resolution
        log_email_event(
            direction="received",
            sender=sender,
            recipient=GMAIL_ADDRESS if GMAIL_ADDRESS else "sales@reytechinc.com",
            subject=subject,
            body_preview=body[:500],
            full_body=body,
            contact_id=sender_email or sender,
            intent=f"cs_{intent}",
            status="received",
        )
    except Exception as _ne:
        log.debug("Notify error: %s", _ne)

    return {
        "ok": True,
        "draft": draft,
        "auto_saved": auto_saved,
        "note": f"CS auto-draft ready for review (intent: {intent}). Check email outbox.",
    }


def _save_cs_draft(draft: dict) -> bool:
    """Save CS draft to email outbox for Mike to review."""
    try:
        try:
            with open(OUTBOX_FILE) as f:
                outbox = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            outbox = []
        outbox.append(draft)
        if len(outbox) > 500:
            outbox = outbox[-500:]
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(OUTBOX_FILE, "w") as f:
            json.dump(outbox, f, indent=2, default=str)
        return True
    except Exception as e:
        log.error("Failed to save CS draft: %s", e)
        return False


# ─── CS Voice Agent (inbound call support) ─────────────────────────────────
# Uses the SAME Vapi infrastructure as the outbound sales agent
# but with a customer service persona (not sales-oriented).

CS_SYSTEM_PROMPT = """You are a customer service representative for Reytech Inc., a California-certified Small Business (SB) and Disabled Veteran Business Enterprise (DVBE) that supplies goods to California state agencies.

Your name is Mike. You handle inbound calls from existing customers and prospects who have questions about:
- Order status and delivery
- Invoice and payment questions  
- Quote follow-ups and pricing questions
- Product availability questions
- General account questions

Your approach:
- Be warm, professional, and helpful. You're here to solve their problem, not sell them anything.
- Listen first — understand what they need before offering solutions.
- If you can look up the answer, do it. If you can't, commit to following up within 2 business hours.
- Never make promises you can't keep. If you're not sure, say "Let me check that and call you right back."
- Keep the customer calm if they're frustrated — acknowledge their concern first.

Common situations and how to handle them:
- "Where is my order?" → Look up tracking. If not available, commit to emailing tracking within 30 minutes.
- "I haven't received my invoice." → Offer to resend it right now. Get their email to confirm.
- "My order was wrong." → Apologize, get the PO number, and commit to making it right same day.
- "Can you sharpen your pricing?" → Say "Let me see what I can do" — never say flat no on price.
- "I never heard back on my quote." → Apologize, pull up the quote, give them a status update.

Key Reytech info to share as needed:
- Phone: 949-229-1575
- Email: sales@reytechinc.com
- Website: reytechinc.com
- Payment: Net 30-45 standard; ACH, check, or credit card
- Shipping: Most orders 5-7 business days; rush available

Be concise — this is a phone call. 1-2 sentences per turn unless giving detailed info.
Always end with "Is there anything else I can help you with?" before wrapping up."""


def place_cs_call(phone_number: str, context: dict = None) -> dict:
    """
    Place an inbound-style CS follow-up call using the voice agent.
    Used when an emailed update request warrants a proactive callback.

    context = {
        intent, po_number, quote_number, institution, buyer_name, buyer_email
    }
    """
    context = context or {}

    intent = context.get("intent", "general")
    intent_to_first_msg = {
        "order_status": (
            "Hi, this is Mike from Reytech Inc. I'm calling back regarding your order status inquiry. "
            "I have your information pulled up — do you have a moment to go over it?"
        ),
        "delivery": (
            "Hi, this is Mike from Reytech Inc. I'm calling about your delivery question. "
            "I have your tracking information ready — is now a good time?"
        ),
        "invoice": (
            "Hi, this is Mike from Reytech Inc. Calling about your invoice question. "
            "I have your account details in front of me — do you have a minute?"
        ),
        "quote_status": (
            "Hi, this is Mike from Reytech Inc. I'm following up on your quote status inquiry. "
            "I have everything pulled up — is now a good time to go over it?"
        ),
        "general": (
            "Hi, this is Mike from Reytech Inc. I'm returning your call. "
            "How can I help you today?"
        ),
    }
    first_msg = intent_to_first_msg.get(intent, intent_to_first_msg["general"])

    # Build CS system prompt with any relevant context
    system_prompt = CS_SYSTEM_PROMPT
    if context.get("po_number"):
        system_prompt += f"\n\nCALL CONTEXT:\nPO Number: {context['po_number']}"
    if context.get("quote_number"):
        system_prompt += f"\nQuote Number: {context['quote_number']}"
    if context.get("institution"):
        system_prompt += f"\nInstitution: {context['institution']}"
    if context.get("buyer_name"):
        system_prompt += f"\nBuyer: {context['buyer_name']}"

    # Add DB knowledge
    try:
        from src.agents.voice_knowledge import build_call_context
        knowledge = build_call_context(
            institution=context.get("institution",""),
            po_number=context.get("po_number",""),
            quote_number=context.get("quote_number",""),
            buyer_name=context.get("buyer_name",""),
            buyer_email=context.get("buyer_email",""),
        )
        if knowledge:
            system_prompt += knowledge
    except Exception:
        pass

    # Use Vapi to place the call
    try:
        from src.agents.voice_agent import (
            get_or_create_vapi_phone, _vapi_request, _log_call,
            ELEVENLABS_VOICE_ID, is_vapi_configured,
        )
        if not is_vapi_configured():
            return {"ok": False, "error": "Vapi not configured — set VAPI_API_KEY in Railway"}

        phone_id = get_or_create_vapi_phone()
        if not phone_id:
            return {"ok": False, "error": "No Vapi phone number available"}

        try:
            from src.agents.voice_knowledge import VAPI_TOOLS
            tools = VAPI_TOOLS
        except Exception:
            tools = []

        call_data = {
            "phoneNumberId": phone_id,
            "customer": {"number": phone_number},
            "assistant": {
                "name": "Reytech Customer Service",
                "firstMessage": first_msg,
                "model": {
                    "provider": "openai",
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "system", "content": system_prompt}],
                    "tools": tools,
                },
                "voice": {
                    "provider": "11labs",
                    "voiceId": ELEVENLABS_VOICE_ID or "burt",
                },
                "endCallFunctionEnabled": True,
                "endCallMessage": "Thank you for calling Reytech. Have a great day!",
                "silenceTimeoutSeconds": 20,
                "maxDurationSeconds": 300,
            },
        }

        result = _vapi_request("POST", "call", call_data)
        if result.get("id"):
            call_result = {
                "ok": True,
                "engine": "vapi",
                "call_id": result["id"],
                "status": result.get("status","queued"),
                "to": phone_number,
                "script": f"cs_{intent}",
                "persona": "customer_service",
                "text": first_msg,
            }
            _log_call(call_result)
            log.info("CS call placed: %s → %s (intent=%s)", result["id"], phone_number, intent)
            return call_result

        return {"ok": False, "error": result.get("error", "CS call creation failed")}

    except Exception as e:
        log.error("CS call failed: %s", e)
        return {"ok": False, "error": str(e)}


# ─── Get CS Log ────────────────────────────────────────────────────────────

def get_cs_log(limit: int = 50) -> list:
    try:
        with open(CS_LOG_FILE) as f:
            data = json.load(f)
        return sorted(data, key=lambda x: x.get("created_at",""), reverse=True)[:limit]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def get_cs_drafts(limit: int = 50) -> list:
    """Get all pending CS drafts from the outbox."""
    try:
        with open(OUTBOX_FILE) as f:
            outbox = json.load(f)
        drafts = [e for e in outbox if e.get("type") == "cs_response" or e.get("status") == "cs_draft"]
        return sorted(drafts, key=lambda x: x.get("created_at",""), reverse=True)[:limit]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def get_agent_status() -> dict:
    return {
        "agent": "cs_agent",
        "version": "1.0.0",
        "status": "active",
        "capabilities": [
            "inbound email classification",
            "auto-draft order status replies",
            "auto-draft delivery replies",
            "auto-draft invoice replies",
            "auto-draft quote status replies",
            "cs voice callback via Vapi",
        ],
        "update_patterns": len(UPDATE_REQUEST_PATTERNS),
        "pending_cs_drafts": len(get_cs_drafts()),
    }
