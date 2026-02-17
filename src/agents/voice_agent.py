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
- Certified Small Business and Disabled Veteran Business Enterprise (DVBE)
- Supplies office, medical, janitorial, IT, and facility supplies to CA state agencies
- Competitive pricing — often 10-30% below contract rates
- Fast turnaround — most orders ship same or next day
- Contact: 949-229-1575, sales@reytechinc.com
- Website: reytechinc.com

Your goals on calls:
1. Introduce Reytech and establish credibility
2. Ask to be added to the vendor/quote list
3. Offer to provide competitive quotes on current or upcoming orders
4. If you reach voicemail, leave a brief professional message with callback number
5. Never be pushy — be helpful and respectful of their time

If they ask questions you don't know, say "Let me have our team get back to you on that" and note the question.

Keep responses SHORT — this is a phone call, not an essay. 1-2 sentences at a time."""


# ─── Call Scripts (context injected into system prompt) ─────────────────────

SCRIPTS = {
    "lead_intro": {
        "name": "Lead Introduction",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. I'm reaching out about "
            "Purchase Order {po_number} for {institution}. We're a certified Small Business "
            "reseller and I was wondering if I could speak with someone in purchasing?"
        ),
        "context": "You are calling about a specific Purchase Order. Your goal is to introduce Reytech and get on the vendor quote list.",
    },
    "follow_up": {
        "name": "Quote Follow-Up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm following up on Quote {quote_number} "
            "that we submitted for {institution}. Do you have a few minutes?"
        ),
        "context": "You are following up on a quote you already submitted. Ask if they have questions, offer to adjust pricing if needed.",
    },
    "intro_cold": {
        "name": "Cold Intro",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. We're a certified Small Business "
            "reseller that works with California state agencies. I was hoping to introduce "
            "our services — is someone in purchasing available?"
        ),
        "context": "This is a cold call. You don't have a specific PO. Focus on introducing Reytech and learning about their upcoming needs.",
    },
    "thank_you": {
        "name": "Thank You / Won",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling to say thank you for the "
            "order on {po_number}. We really appreciate the business and wanted to make "
            "sure everything arrived as expected."
        ),
        "context": "You are calling to thank them for a won order. Build the relationship, ask if they need anything else.",
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
    first_msg = script["first_message"].format(**{
        k: variables.get(k, f"[{k}]")
        for k in ["po_number", "institution", "quote_number"]
    })

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
