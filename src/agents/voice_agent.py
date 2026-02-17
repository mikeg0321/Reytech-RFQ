"""
voice_agent.py — AI Voice Call Agent for Reytech
Phase 14 | Version: 0.1.0 (scaffold — needs Twilio account)

Architecture for AI-powered outbound calls to buyers:
  1. Lead Gen finds high-score opportunity
  2. Voice agent calls buyer: "Hi, I'm calling from Reytech Inc.
     regarding PO {number}. We've supplied similar items at competitive
     pricing and would love to be on the quote list."
  3. ElevenLabs provides natural voice synthesis
  4. Twilio handles telephony (outbound calls, DTMF, voicemail detection)
  5. Conversation transcribed + logged for audit trail

SETUP REQUIRED:
  1. Create Twilio account: https://www.twilio.com/try-twilio
  2. Get a phone number (local CA number recommended)
  3. Create ElevenLabs account: https://elevenlabs.io
  4. Set env vars:
     TWILIO_ACCOUNT_SID=AC...
     TWILIO_AUTH_TOKEN=...
     TWILIO_PHONE_NUMBER=+1949...
     ELEVENLABS_API_KEY=...
     ELEVENLABS_VOICE_ID=... (clone Mike's voice or use preset)

Cost estimate:
  Twilio: ~$0.014/min outbound + $1/mo per number
  ElevenLabs: ~$0.30/min for voice synthesis
  Total per call (avg 2 min): ~$0.63
  Budget: 50 calls/month = ~$32

Dependencies: twilio, requests (for ElevenLabs)
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

# ─── Configuration ───────────────────────────────────────────────────────────

try:
    from src.core.secrets import get_key
    TWILIO_SID = get_key("twilio_sid") if hasattr(__import__('src.core.secrets', fromlist=['_REGISTRY']), '_REGISTRY') else ""
    TWILIO_TOKEN = ""
    TWILIO_PHONE = ""
    ELEVENLABS_KEY = ""
    ELEVENLABS_VOICE_ID = ""
except ImportError:
    pass

# Direct env var fallback (always works)
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE = os.environ.get("TWILIO_PHONE_NUMBER", "")
ELEVENLABS_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "")

# Check for twilio SDK
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


def is_configured() -> bool:
    """Check if voice agent has all required credentials."""
    return bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_PHONE)


def is_voice_configured() -> bool:
    """Check if ElevenLabs voice synthesis is configured."""
    return bool(ELEVENLABS_KEY and ELEVENLABS_VOICE_ID)


# ─── Call Scripts ────────────────────────────────────────────────────────────

SCRIPTS = {
    "lead_intro": {
        "name": "Lead Introduction",
        "text": (
            "Hi, this is Mike from Reytech Inc. calling about Purchase Order {po_number} "
            "for {institution}. We're a certified Small Business reseller and have supplied "
            "similar items at competitive pricing. I'd love to discuss being added to the "
            "quote list for this and future orders. Could I speak with someone in purchasing?"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech Inc. regarding PO {po_number} for {institution}. "
            "We're a certified SB reseller with competitive pricing on the items in this order. "
            "Please call us back at 949-229-1575 or email sales@reytechinc.com. "
            "Again, that's Reytech Inc., 949-229-1575. Thank you."
        ),
    },
    "follow_up": {
        "name": "Quote Follow-Up",
        "text": (
            "Hi, this is Mike from Reytech Inc. following up on Quote {quote_number} we "
            "submitted for {institution}. I wanted to check if you had any questions about "
            "our pricing or if there's anything we can adjust. We're happy to work with you "
            "on this."
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech following up on Quote {quote_number} for "
            "{institution}. If you have any questions, please reach us at 949-229-1575 "
            "or sales@reytechinc.com. Thank you."
        ),
    },
}


# ─── Voice Synthesis (ElevenLabs) ────────────────────────────────────────────

def synthesize_speech(text: str) -> Optional[bytes]:
    """
    Convert text to speech using ElevenLabs API.
    Returns audio bytes (mp3) or None on failure.
    """
    if not is_voice_configured() or not HAS_REQUESTS:
        return None

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}"
    try:
        resp = _requests.post(url, headers={
            "xi-api-key": ELEVENLABS_KEY,
            "Content-Type": "application/json",
        }, json={
            "text": text,
            "model_id": "eleven_monolingual_v1",
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75,
            },
        }, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        log.error("ElevenLabs synthesis failed: %s", e)
        return None


# ─── Call Management ─────────────────────────────────────────────────────────

def place_call(phone_number: str, script_key: str = "lead_intro",
               variables: dict = None) -> dict:
    """
    Place an outbound call using Twilio.

    Args:
        phone_number: Buyer's phone number (E.164 format: +19165550100)
        script_key: Which script to use
        variables: Template variables (po_number, institution, etc.)

    Returns:
        Call result dict with SID, status.
    """
    if not is_configured():
        return {"ok": False, "error": "Twilio not configured. Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER"}

    if not HAS_TWILIO:
        return {"ok": False, "error": "twilio package not installed. Run: pip install twilio"}

    script = SCRIPTS.get(script_key, SCRIPTS["lead_intro"])
    variables = variables or {}
    spoken_text = script["text"].format(**{k: variables.get(k, f"[{k}]") for k in
        ["po_number", "institution", "quote_number"]})

    try:
        client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)

        # Use TwiML to speak the script
        twiml = f'<Response><Say voice="alice">{spoken_text}</Say></Response>'

        call = client.calls.create(
            to=phone_number,
            from_=TWILIO_PHONE,
            twiml=twiml,
        )

        result = {
            "ok": True,
            "call_sid": call.sid,
            "status": call.status,
            "to": phone_number,
            "script": script_key,
            "text": spoken_text,
        }

        # Log the call
        _log_call(result)
        log.info("CALL placed: %s → %s (script=%s)", call.sid, phone_number, script_key)
        return result

    except Exception as e:
        log.error("Call failed: %s", e)
        return {"ok": False, "error": str(e)}


def _log_call(call_data: dict):
    """Log a call attempt."""
    try:
        with open(CALL_LOG_FILE) as f:
            call_log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        call_log = []
    call_log.append({
        **call_data,
        "timestamp": datetime.now().isoformat(),
    })
    if len(call_log) > 2000:
        call_log = call_log[-2000:]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CALL_LOG_FILE, "w") as f:
        json.dump(call_log, f)


def get_call_log(limit: int = 50) -> list:
    """Get recent call log."""
    try:
        with open(CALL_LOG_FILE) as f:
            data = json.load(f)
        return sorted(data, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# ─── Status ──────────────────────────────────────────────────────────────────

def verify_credentials() -> dict:
    """Actually ping Twilio API to verify credentials are valid."""
    if not is_configured():
        return {"ok": False, "error": "Not configured — env vars missing"}
    if not HAS_TWILIO:
        return {"ok": False, "error": "twilio SDK not installed"}
    try:
        client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
        account = client.api.accounts(TWILIO_SID).fetch()
        # Check that phone number exists
        numbers = client.incoming_phone_numbers.list(phone_number=TWILIO_PHONE, limit=1)
        return {
            "ok": True,
            "account_name": account.friendly_name,
            "account_status": account.status,
            "phone_verified": len(numbers) > 0,
            "phone_number": TWILIO_PHONE,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_agent_status() -> dict:
    """Agent health + configuration status."""
    return {
        "agent": "voice_calls",
        "version": "0.1.0",
        "status": "scaffold" if not is_configured() else "ready",
        "twilio_configured": is_configured(),
        "twilio_sdk_installed": HAS_TWILIO,
        "elevenlabs_configured": is_voice_configured(),
        "phone_number": TWILIO_PHONE[:6] + "****" if TWILIO_PHONE else "(not set)",
        "sid_set": bool(TWILIO_SID),
        "token_set": bool(TWILIO_TOKEN),
        "available_scripts": list(SCRIPTS.keys()),
        "call_log_count": len(get_call_log(limit=9999)),
        "setup_steps": [
            "1. Create Twilio account: https://www.twilio.com/try-twilio",
            "2. Buy a phone number ($1/mo)",
            "3. Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER in Railway",
            "4. Optional: Set ELEVENLABS_API_KEY + ELEVENLABS_VOICE_ID for AI voice",
            "5. Install SDK: pip install twilio",
        ] if not is_configured() else [],
    }
