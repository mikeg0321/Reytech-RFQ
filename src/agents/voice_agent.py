"""
voice_agent.py — AI Voice Call Agent for Reytech
Phase 18 | Version: 2.0.0

Conversational AI phone agent powered by Vapi.ai
Falls back to Twilio TTS if Vapi not configured.

Architecture:
  1. Vapi.ai handles the AI conversation (LLM + voice + telephony)
  2. Twilio provides the phone number (imported into Vapi)
  3. Agent has full Reytech context — pricing, quote details, institution info
  4. Calls are logged with transcripts for CRM timeline

SETUP:
  Required: VAPI_API_KEY (from dashboard.vapi.ai)
  Optional: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER
            (for Twilio fallback or to import number into Vapi)

  First call will auto-create a free Vapi phone number if none exists.
  For caller ID matching Reytech, import Twilio number via Vapi dashboard.
"""

import os
import logging
import json
import time
from datetime import datetime
from typing import Optional

log = logging.getLogger("voice")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

CALL_LOG_FILE = os.path.join(DATA_DIR, "voice_call_log.json")
VAPI_CONFIG_FILE = os.path.join(DATA_DIR, "vapi_config.json")

# ─── Configuration ───────────────────────────────────────────────────────────

# Vapi (primary — conversational AI)
VAPI_API_KEY = os.environ.get("VAPI_API_KEY", "")
VAPI_BASE_URL = "https://api.vapi.ai"

# Twilio (fallback — basic TTS)
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE = os.environ.get("TWILIO_PHONE_NUMBER", "")
ELEVENLABS_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "")

# Check for SDKs
try:
    from twilio.rest import Client as TwilioClient
    HAS_TWILIO = True
except ImportError:
    HAS_TWILIO = False

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from src.agents.voice_knowledge import build_call_context, VAPI_TOOLS, handle_tool_call
    HAS_KNOWLEDGE = True
except ImportError:
    HAS_KNOWLEDGE = False


def is_vapi_configured() -> bool:
    return bool(VAPI_API_KEY and HAS_REQUESTS)

def is_configured() -> bool:
    """Check if any voice calling method is available."""
    return is_vapi_configured() or bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_PHONE)

def is_voice_configured() -> bool:
    return bool(ELEVENLABS_KEY and ELEVENLABS_VOICE_ID)


# ─── Reytech AI Assistant Prompt ────────────────────────────────────────────

REYTECH_SYSTEM_PROMPT = """You are a sales representative calling on behalf of Reytech Inc., a certified Small Business (SB/DVBE) reseller that supplies goods to California state agencies — primarily CDCR, CCHCS, CalVet, DSH, and DGS.

Your name is Mike. You are male. Be natural, professional, warm, and concise. You're NOT reading a script — you're having a real conversation. Adapt to what the other person says.

Key facts about Reytech:
- Certified Small Business (SB) and Disabled Veteran Business Enterprise (DVBE)
- Supplies office, medical, janitorial, IT, and facility supplies to CA state agencies
- Competitive pricing — often 10-30% below contract rates
- Fast turnaround — most orders ship same or next day
- Been doing business with CDCR and CCHCS for years
- Contact: 949-229-1575, sales@reytechinc.com
- Website: reytechinc.com
- Located in Southern California

Common objections and how to handle them:
- "We already have a vendor" → "Totally understand. We're not looking to replace anyone — just to be an option. A lot of facilities use us as a secondary vendor for rush orders or when they need better pricing on specific items."
- "Send me an email" → "Absolutely, I'll send that right over. Can I get your direct email? And just so I include the right info, are there any categories you buy frequently — office supplies, janitorial, medical?"
- "We're not buying right now" → "No problem at all. Would it be okay if I check back when your next fiscal quarter starts? In the meantime, I'll send our capabilities sheet so you have us on file."
- "How are your prices?" → "We're typically 10-30% below CMAS and DGS contract rates. We source direct and pass the savings through. Happy to run a comparison on any items you're currently buying."
- "Are you on CMAS?" → "We're not on CMAS — we're a direct reseller, which actually works in your favor. No contract markups. We quote item-by-item so you get the best price on each line. For orders under $10K, agencies can purchase direct from a certified SB like us without going through CMAS."
- "I need to check with my supervisor" → "Of course. Would it help if I sent a brief one-pager about Reytech that you could pass along? And what's the best number to reach you for a follow-up?"

Voicemail rules:
- Keep it under 30 seconds
- State your name, company, reason for calling, and callback number
- Speak clearly and slowly on the phone number
- Always repeat the phone number: 949-229-1575

Your goals on calls:
1. Reference items they already buy — show you know their needs
2. Say "we sell those same items" and mention competitive pricing (10-30% below contract)
3. Ask to be added to their RFQ distribution list for future orders
4. Give our email: sales@reytechinc.com (spell it out: S-A-L-E-S at R-E-Y-T-E-C-H-I-N-C dot com)
5. Get THEIR purchasing contact name, email, and direct phone
6. If they show interest, offer to send a quote on whatever they're currently buying
7. If voicemail, leave a brief message with callback number 949-229-1575
8. Never be pushy — be helpful and respectful of their time

If they ask questions you don't know, say "Let me have our team get back to you on that" and note the question.

Keep responses SHORT — this is a phone call, not an essay. 1-2 sentences at a time."""


# ─── Call Scripts (context injected into system prompt) ─────────────────────

SCRIPTS = {
    # ── Prospecting Scripts ──
    "lead_intro": {
        "name": "SCPRS Lead Introduction",
        "category": "prospecting",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. I'm reaching out because "
            "I noticed {institution} recently purchased some items that we also carry — "
            "things like {top_items}. We're a certified Small Business and DVBE, and "
            "we'd really appreciate the chance to get on your RFQ list. "
            "Is someone in purchasing available?"
        ),
        "context": (
            "You found this buyer on the State Controller's SCPRS system. Their recent "
            "purchase orders show they buy items Reytech sells.\n\n"
            "YOUR APPROACH:\n"
            "1. Lead with THEIR items — reference what they recently bought\n"
            "2. Say: 'We sell those same types of items you recently purchased, and we're "
            "typically 10-30% below contract rates'\n"
            "3. Ask: 'Would it be possible to get added to your RFQ distribution list?'\n"
            "4. Ask: 'What's the best email to send quotes to?'\n"
            "5. Always give our contact: 'Our email is sales@reytechinc.com — "
            "that's S-A-L-E-S at R-E-Y-T-E-C-H-I-N-C dot com'\n\n"
            "IF THEY ASK HOW YOU FOUND THEM: 'We monitor the state procurement system — "
            "your recent purchases are a match for items we specialize in.'\n\n"
            "GOAL: Get on RFQ list + get purchasing contact email"
        ),
    },
    "intro_cold": {
        "name": "Cold Intro (no PO)",
        "category": "prospecting",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. We're a certified Small Business "
            "and DVBE that supplies office, janitorial, medical, and facility items to "
            "California state agencies. I'd love to get on your vendor list for quotes. "
            "Is someone in purchasing available?"
        ),
        "context": (
            "Cold call — no specific PO data. Focus on getting on the RFQ list.\n"
            "- Say: 'We sell the types of items your facility orders regularly'\n"
            "- Ask: 'What's the best way to get added to your RFQ distribution list?'\n"
            "- Ask: 'What categories do you purchase most — office supplies, janitorial, medical?'\n"
            "- Give email: 'You can reach us at sales@reytechinc.com'\n"
            "- Mention SB/DVBE helps with their procurement mandates\n"
            "- GOAL: Get on the RFQ list, get a name and email"
        ),
    },
    "gatekeeper": {
        "name": "Gatekeeper Navigation",
        "category": "prospecting",
        "first_message": (
            "Hi, good morning. This is Mike from Reytech Inc. I'm trying to reach "
            "the purchasing department regarding vendor opportunities for {institution}. "
            "Could you connect me or let me know the best person to speak with?"
        ),
        "context": (
            "You've reached a front desk or general line. Be polite and professional. "
            "Your goal is to get transferred to purchasing or get a name/direct number. "
            "If they ask what it's regarding, say 'We're a certified Small Business vendor and "
            "I wanted to inquire about being added to the approved vendor list for supply orders.' "
            "If they give you a name, ask for the direct line and email."
        ),
    },
    "dvbe_intro": {
        "name": "DVBE Set-Aside Intro",
        "category": "prospecting",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. We're a certified Disabled Veteran Business "
            "Enterprise — a DVBE — and I'm reaching out to see if {institution} has any "
            "upcoming DVBE set-aside opportunities or supply needs I could help with."
        ),
        "context": (
            "Lead with the DVBE certification — many agencies have DVBE participation goals "
            "and mandated spending thresholds. This gives you a strong angle. Ask if they have "
            "a DVBE coordinator or if purchasing handles DVBE allocations directly. "
            "Mention that using Reytech counts toward their DVBE spending requirements."
        ),
    },

    # ── Follow-Up Scripts ──
    "follow_up": {
        "name": "Quote Follow-Up",
        "category": "follow_up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm following up on Quote {quote_number} "
            "that we submitted for {institution}. Do you have a few minutes?"
        ),
        "context": (
            "You are following up on a quote you already submitted. Key goals: "
            "1) Confirm they received the quote, 2) Ask if they have questions about pricing, "
            "3) Offer to adjust if needed — we can be flexible on margins for new relationships, "
            "4) Ask about their timeline for making a decision, "
            "5) If they went with another vendor, ask what price they got (competitor intel)."
        ),
    },
    "follow_up_no_response": {
        "name": "Follow-Up (No Response to Email)",
        "category": "follow_up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I sent over a quote last week for "
            "{institution} and wanted to make sure it didn't get lost in the shuffle. "
            "Did you have a chance to review it?"
        ),
        "context": (
            "They haven't responded to your emailed quote. Be casual — don't make them feel bad. "
            "Frame it as making sure the email arrived. If they haven't looked at it, ask when "
            "would be a good time to touch base. If they have, ask for feedback on pricing."
        ),
    },
    "follow_up_lost": {
        "name": "Lost Quote Recovery",
        "category": "follow_up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I wanted to touch base about the quote we "
            "submitted for {institution} — I understand you went in a different direction, "
            "and that's totally fine. I just wanted to ask a quick question if you have a second."
        ),
        "context": (
            "This quote was lost to a competitor. Your goals: "
            "1) Find out who won and roughly what price (say 'just so we can be more competitive next time'), "
            "2) Ask what would have made the difference — price, delivery speed, product selection, "
            "3) Stay positive — leave the door open for next time, "
            "4) Ask to stay on the quote list for future orders. "
            "NEVER be bitter or negative about losing. Be gracious."
        ),
    },

    # ── Relationship Scripts ──
    "thank_you": {
        "name": "Thank You (Won Order)",
        "category": "relationship",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling to say thank you for the "
            "order on {po_number}. We really appreciate the business and wanted to make "
            "sure everything arrived as expected."
        ),
        "context": (
            "You won this order. This is a relationship call. Goals: "
            "1) Thank them sincerely, 2) Confirm delivery went smoothly, "
            "3) Ask if there's anything they need going forward, "
            "4) Ask about their next procurement cycle, "
            "5) Ask if there are other departments or facilities you should reach out to. "
            "This is how you turn one order into a recurring customer."
        ),
    },
    "check_in": {
        "name": "Quarterly Check-In",
        "category": "relationship",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm just doing a quick check-in with "
            "some of our contacts at {institution}. How have things been? Any upcoming "
            "supply needs I can help with?"
        ),
        "context": (
            "Regular relationship maintenance. Keep it brief and conversational. "
            "Ask about upcoming budget cycles, any new projects that need supplies, "
            "or if their needs have changed. Mention any new product lines Reytech carries. "
            "Goal: stay top of mind without being annoying."
        ),
    },
    "reactivation": {
        "name": "Dormant Account Reactivation",
        "category": "relationship",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. We worked together on some orders a while back "
            "and I wanted to reconnect. I know things get busy — are you still handling "
            "purchasing for {institution}?"
        ),
        "context": (
            "This is a past customer who hasn't ordered in a while. Be warm and no-pressure. "
            "Goals: confirm they're still the right contact, learn if anything changed "
            "(new buyer, reorganization), offer to re-quote any recurring items, "
            "mention any new capabilities or certifications."
        ),
    },

    # ── Delivery / Order Scripts ──
    "delivery_check": {
        "name": "Delivery Confirmation",
        "category": "order",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling to confirm that your order "
            "{po_number} arrived. Our tracking shows it was delivered — did everything "
            "come through okay?"
        ),
        "context": (
            "You're confirming delivery of a shipped order. This builds trust and shows "
            "you care about service, not just the sale. If there are issues (missing items, "
            "damage), offer to resolve immediately. Ask if they need anything else."
        ),
    },
    "shipping_delay": {
        "name": "Shipping Delay Notification",
        "category": "order",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling about your order {po_number} — "
            "I wanted to give you a heads up that we're seeing a slight delay on one of the "
            "items. I wanted to let you know proactively rather than have it be a surprise."
        ),
        "context": (
            "There's a shipping delay. Be honest and proactive. Explain the delay briefly, "
            "give a new estimated timeline, and ask if they need a rush on any specific items. "
            "Offer alternatives if available. Proactive communication builds more trust than "
            "perfect delivery."
        ),
    },
    "invoice_follow_up": {
        "name": "Invoice / Payment Follow-Up",
        "category": "order",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling about Invoice {po_number} that "
            "we sent over. I wanted to check if it was received and if there's anything "
            "needed from our side to get it processed."
        ),
        "context": (
            "You're following up on an unpaid invoice. Be professional and non-confrontational. "
            "State agencies have specific payment cycles (Net 30-45). Ask if the invoice was "
            "received, if it's in the approval queue, and if there's a reference number you "
            "should track. If there are issues, offer to resubmit or provide additional documentation."
        ),
    },
}


# ─── Vapi Phone Number Management ──────────────────────────────────────────

def _load_vapi_config() -> dict:
    try:
        with open(VAPI_CONFIG_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_vapi_config(cfg: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(VAPI_CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


def _vapi_request(method: str, endpoint: str, data: dict = None) -> dict:
    """Make authenticated request to Vapi API."""
    if not HAS_REQUESTS or not VAPI_API_KEY:
        return {"error": "Vapi not configured"}
    url = f"{VAPI_BASE_URL}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {VAPI_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        if method == "GET":
            resp = _requests.get(url, headers=headers, timeout=15)
        elif method == "POST":
            resp = _requests.post(url, headers=headers, json=data or {}, timeout=30)
        else:
            return {"error": f"Unsupported method: {method}"}

        if resp.status_code >= 400:
            return {"error": f"Vapi API {resp.status_code}: {resp.text[:500]}"}
        return resp.json()
    except Exception as e:
        log.error("Vapi API error: %s", e)
        return {"error": str(e)}


def get_or_create_vapi_phone() -> Optional[str]:
    """Get existing Vapi phone number ID, or create a free one."""
    cfg = _load_vapi_config()
    phone_id = cfg.get("phone_number_id")

    # Already have one? Verify it's still valid
    if phone_id:
        check = _vapi_request("GET", f"phone-number/{phone_id}")
        if not check.get("error"):
            return phone_id

    # List existing phone numbers
    existing = _vapi_request("GET", "phone-number")
    if isinstance(existing, list) and len(existing) > 0:
        phone_id = existing[0].get("id")
        cfg["phone_number_id"] = phone_id
        cfg["phone_number"] = existing[0].get("number", "")
        _save_vapi_config(cfg)
        log.info("Using existing Vapi phone: %s", cfg["phone_number"])
        return phone_id

    # Create a free Vapi number
    create_data = {"provider": "vapi"}
    if TWILIO_PHONE:
        create_data["fallbackDestination"] = {"type": "number", "number": TWILIO_PHONE}
    result = _vapi_request("POST", "phone-number", create_data)
    if result.get("id"):
        cfg["phone_number_id"] = result["id"]
        cfg["phone_number"] = result.get("number", "")
        cfg["created_at"] = datetime.now().isoformat()
        _save_vapi_config(cfg)
        log.info("Created Vapi phone number: %s", cfg["phone_number"])
        return result["id"]

    log.error("Failed to get/create Vapi phone: %s", result.get("error"))
    return None


def import_twilio_to_vapi() -> dict:
    """Import Twilio phone number into Vapi for outbound calls with Reytech caller ID."""
    if not TWILIO_SID or not TWILIO_TOKEN or not TWILIO_PHONE:
        return {"ok": False, "error": "Twilio credentials not set"}

    result = _vapi_request("POST", "phone-number", {
        "provider": "twilio",
        "number": TWILIO_PHONE,
        "twilioAccountSid": TWILIO_SID,
        "twilioAuthToken": TWILIO_TOKEN,
    })
    if result.get("id"):
        cfg = _load_vapi_config()
        cfg["phone_number_id"] = result["id"]
        cfg["phone_number"] = TWILIO_PHONE
        cfg["provider"] = "twilio"
        cfg["imported_at"] = datetime.now().isoformat()
        _save_vapi_config(cfg)
        return {"ok": True, "phone_id": result["id"], "number": TWILIO_PHONE}
    return {"ok": False, "error": result.get("error", "Unknown error")}


# ─── Place Call (Vapi primary, Twilio fallback) ────────────────────────────

def place_call(phone_number: str, script_key: str = "lead_intro",
               variables: dict = None) -> dict:
    """
    Place an outbound call.
    Uses Vapi (conversational AI) if configured, falls back to Twilio TTS.
    """
    variables = variables or {}

    # Try Vapi first
    if is_vapi_configured():
        return _place_vapi_call(phone_number, script_key, variables)

    # Fallback to Twilio TTS
    return _place_twilio_call(phone_number, script_key, variables)


def _place_vapi_call(phone_number: str, script_key: str, variables: dict) -> dict:
    """Place a conversational AI call via Vapi with full Reytech knowledge."""
    phone_id = get_or_create_vapi_phone()
    if not phone_id:
        return {"ok": False, "error": "No Vapi phone number available. Import Twilio number or create free one via /api/voice/import-twilio"}

    script = SCRIPTS.get(script_key, SCRIPTS["lead_intro"])

    # Build first message with variables
    # Pull top items from lead data for SCPRS-based calls
    top_items = variables.get("top_items", "")
    if not top_items and variables.get("po_number"):
        try:
            from src.agents.voice_knowledge import _load
            leads = _load("leads.json")
            if leads:
                po = variables["po_number"].lower()
                for l in (leads if isinstance(leads, list) else leads.values()):
                    if isinstance(l, dict) and po in l.get("po_number", "").lower():
                        items = l.get("matched_items", []) or l.get("items", [])
                        if items:
                            descs = [i.get("description", "")[:40] if isinstance(i, dict) else str(i)[:40] for i in items[:3]]
                            top_items = ", ".join(d for d in descs if d)
                        break
        except Exception:
            pass
    if not top_items:
        top_items = "office and facility supplies"

    first_msg = script["first_message"].format(**{
        k: variables.get(k, f"[{k}]")
        for k in ["po_number", "institution", "quote_number", "top_items"]
    } | {"top_items": top_items})

    # Build context-aware system prompt
    system_prompt = REYTECH_SYSTEM_PROMPT + f"\n\n--- CALL CONTEXT ---\n{script['context']}"
    if variables.get("institution"):
        system_prompt += f"\nInstitution: {variables['institution']}"
    if variables.get("po_number"):
        system_prompt += f"\nPurchase Order: {variables['po_number']}"
    if variables.get("quote_number"):
        system_prompt += f"\nQuote Number: {variables['quote_number']}"

    # ── Inject DB knowledge ──
    if HAS_KNOWLEDGE:
        knowledge_context = build_call_context(
            institution=variables.get("institution", ""),
            po_number=variables.get("po_number", ""),
            quote_number=variables.get("quote_number", ""),
            buyer_name=variables.get("buyer_name", ""),
            buyer_email=variables.get("buyer_email", ""),
        )
        if knowledge_context:
            system_prompt += knowledge_context

    # Build model config with tools if knowledge available
    model_config = {
        "provider": "openai",
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt}
        ],
    }

    # Add function calling tools for mid-call lookups
    if HAS_KNOWLEDGE:
        model_config["tools"] = VAPI_TOOLS

    # Create the call with transient assistant
    call_data = {
        "phoneNumberId": phone_id,
        "customer": {
            "number": phone_number,
        },
        "assistant": {
            "firstMessage": first_msg,
            "model": model_config,
            "voice": {
                "provider": "11labs",
                "voiceId": "burt",
            },
            "endCallFunctionEnabled": True,
            "endCallMessage": "Thanks for your time. Have a great day!",
            "silenceTimeoutSeconds": 30,
            "maxDurationSeconds": 300,
            "name": "Reytech Sales",
        },
    }

    # Set server URL for function calling webhook if we have tools
    if HAS_KNOWLEDGE and variables.get("server_url"):
        call_data["assistant"]["serverUrl"] = variables["server_url"]

    result = _vapi_request("POST", "call", call_data)

    if result.get("id"):
        call_result = {
            "ok": True,
            "engine": "vapi",
            "call_id": result["id"],
            "call_sid": result.get("id", ""),
            "status": result.get("status", "queued"),
            "to": phone_number,
            "script": script_key,
            "text": first_msg,
            "conversational": True,
        }
        _log_call(call_result)
        log.info("VAPI CALL placed: %s → %s (script=%s)", result["id"], phone_number, script_key)
        return call_result

    return {"ok": False, "error": result.get("error", "Vapi call creation failed")}


def _place_twilio_call(phone_number: str, script_key: str, variables: dict) -> dict:
    """Place a basic TTS call via Twilio (fallback)."""
    if not TWILIO_SID or not TWILIO_TOKEN or not TWILIO_PHONE:
        return {"ok": False, "error": "No voice engine configured. Set VAPI_API_KEY or Twilio credentials."}
    if not HAS_TWILIO:
        return {"ok": False, "error": "twilio package not installed"}

    script = SCRIPTS.get(script_key, SCRIPTS["lead_intro"])
    spoken_text = script["first_message"].format(**{
        k: variables.get(k, f"[{k}]")
        for k in ["po_number", "institution", "quote_number"]
    })

    try:
        client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
        twiml = f'<Response><Say voice="Google.en-US-Neural2-D">{spoken_text}</Say></Response>'
        call = client.calls.create(to=phone_number, from_=TWILIO_PHONE, twiml=twiml)

        result = {
            "ok": True,
            "engine": "twilio_tts",
            "call_sid": call.sid,
            "status": call.status,
            "to": phone_number,
            "script": script_key,
            "text": spoken_text,
            "conversational": False,
        }
        _log_call(result)
        log.info("TWILIO CALL placed: %s → %s (script=%s)", call.sid, phone_number, script_key)
        return result
    except Exception as e:
        log.error("Twilio call failed: %s", e)
        return {"ok": False, "error": str(e)}


# ─── Call Log ────────────────────────────────────────────────────────────────

def _log_call(call_data: dict):
    try:
        with open(CALL_LOG_FILE) as f:
            call_log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        call_log = []
    call_log.append({**call_data, "timestamp": datetime.now().isoformat()})
    if len(call_log) > 2000:
        call_log = call_log[-2000:]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CALL_LOG_FILE, "w") as f:
        json.dump(call_log, f)


def get_call_log(limit: int = 50) -> list:
    try:
        with open(CALL_LOG_FILE) as f:
            data = json.load(f)
        return sorted(data, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# ─── Vapi Call Details (transcripts, recordings) ───────────────────────────

def get_vapi_call_details(call_id: str) -> dict:
    """Fetch call details including transcript from Vapi."""
    if not is_vapi_configured():
        return {"error": "Vapi not configured"}
    return _vapi_request("GET", f"call/{call_id}")


def get_vapi_calls(limit: int = 20) -> list:
    """List recent Vapi calls with transcripts."""
    if not is_vapi_configured():
        return []
    result = _vapi_request("GET", f"call?limit={limit}")
    if isinstance(result, list):
        return result
    return result.get("results", result.get("calls", []))


# ─── Status & Verification ─────────────────────────────────────────────────

def verify_credentials() -> dict:
    """Verify voice agent credentials."""
    results = {}

    # Check Vapi
    if VAPI_API_KEY:
        vapi_check = _vapi_request("GET", "phone-number")
        if isinstance(vapi_check, list) or not vapi_check.get("error"):
            nums = vapi_check if isinstance(vapi_check, list) else []
            results["vapi"] = {
                "ok": True,
                "phone_numbers": len(nums),
                "numbers": [n.get("number", "?") for n in nums[:5]],
            }
        else:
            results["vapi"] = {"ok": False, "error": vapi_check.get("error")}
    else:
        results["vapi"] = {"ok": False, "error": "VAPI_API_KEY not set"}

    # Check Twilio
    if TWILIO_SID and TWILIO_TOKEN and HAS_TWILIO:
        try:
            client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
            account = client.api.accounts(TWILIO_SID).fetch()
            results["twilio"] = {
                "ok": True,
                "account_name": account.friendly_name,
                "phone": TWILIO_PHONE,
            }
        except Exception as e:
            results["twilio"] = {"ok": False, "error": str(e)}
    else:
        results["twilio"] = {"ok": False, "error": "Twilio not configured"}

    results["ok"] = results.get("vapi", {}).get("ok") or results.get("twilio", {}).get("ok")
    results["primary_engine"] = "vapi" if is_vapi_configured() else ("twilio" if HAS_TWILIO else "none")
    return results


def get_agent_status() -> dict:
    """Agent health + configuration status."""
    cfg = _load_vapi_config()
    return {
        "agent": "voice_calls",
        "version": "2.0.0",
        "status": "ready" if is_configured() else "not_configured",
        "primary_engine": "vapi" if is_vapi_configured() else ("twilio" if TWILIO_SID else "none"),
        "vapi_configured": is_vapi_configured(),
        "vapi_phone_id": cfg.get("phone_number_id", ""),
        "vapi_phone_number": cfg.get("phone_number", ""),
        "twilio_configured": bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_PHONE),
        "twilio_sdk_installed": HAS_TWILIO,
        "phone_number": cfg.get("phone_number") or (TWILIO_PHONE[:6] + "****" if TWILIO_PHONE else "(not set)"),
        "available_scripts": list(SCRIPTS.keys()),
        "call_log_count": len(get_call_log(limit=9999)),
        "setup_steps": [] if is_configured() else [
            "1. Set VAPI_API_KEY in Railway (from dashboard.vapi.ai)",
            "2. Optional: Import Twilio number for Reytech caller ID",
        ],
    }
