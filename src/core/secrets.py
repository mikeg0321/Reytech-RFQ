"""
secrets.py — Centralized Agent Secret Management for Reytech
Phase 13 | Version: 1.0.0

Single source of truth for all agent API keys and credentials.
Each agent gets its own scoped key that falls back to shared keys.

Railway env vars:
  ANTHROPIC_API_KEY      — Shared Claude API key (fallback for all agents)
  AGENT_ITEM_ID_KEY      — Item Identifier agent (Claude Haiku)
  AGENT_LEADGEN_KEY      — Lead Gen agent (Claude Haiku)
  AGENT_PRICING_KEY      — Pricing agent (Claude Haiku)
  SERPAPI_KEY             — Amazon product research
  GMAIL_ADDRESS           — Outbound email sender
  GMAIL_PASSWORD           — Gmail app password
  QB_CLIENT_ID            — QuickBooks OAuth2 client
  QB_CLIENT_SECRET        — QuickBooks OAuth2 secret
  QB_REFRESH_TOKEN        — QuickBooks refresh token (long-lived)
  QB_REALM_ID             — QuickBooks company ID

Security:
  - Keys are never logged in full (masked to first 8 chars)
  - Agent-specific keys isolate blast radius
  - Health endpoint shows which keys are set (not values)
  - Validate on startup — warn loudly about missing keys
"""

import os
import logging

log = logging.getLogger("secrets")

# ─── Secret Definitions ─────────────────────────────────────────────────────

_REGISTRY = {
    # Claude API keys (per-agent isolation)
    "anthropic_shared": {
        "env": "ANTHROPIC_API_KEY",
        "required": False,
        "desc": "Shared Claude API key — fallback for all agents",
        "agents": ["all"],
    },
    "agent_item_id": {
        "env": "AGENT_ITEM_ID_KEY",
        "fallback": "ANTHROPIC_API_KEY",
        "required": False,
        "desc": "Item Identifier agent — Claude Haiku",
        "agents": ["item_identifier"],
    },
    "agent_leadgen": {
        "env": "AGENT_LEADGEN_KEY",
        "fallback": "ANTHROPIC_API_KEY",
        "required": False,
        "desc": "Lead Gen agent — Claude Haiku",
        "agents": ["lead_gen"],
    },
    "agent_pricing": {
        "env": "AGENT_PRICING_KEY",
        "fallback": "ANTHROPIC_API_KEY",
        "required": False,
        "desc": "Pricing agent — Claude Haiku",
        "agents": ["pricing"],
    },
    # External services
    "serpapi": {
        "env": "SERPAPI_KEY",
        "required": False,
        "desc": "SerpApi — Amazon product research",
        "agents": ["product_research"],
    },
    "gmail_address": {
        "env": "GMAIL_ADDRESS",
        "required": False,
        "desc": "Gmail sender address",
        "agents": ["email_poller"],
    },
    "gmail_password": {
        "env": "GMAIL_PASSWORD",
        "required": False,
        "desc": "Gmail app password",
        "agents": ["email_poller"],
        "sensitive": True,
    },
    # QuickBooks
    "qb_client_id": {
        "env": "QB_CLIENT_ID",
        "required": False,
        "desc": "QuickBooks OAuth2 client ID",
        "agents": ["quickbooks"],
    },
    "qb_client_secret": {
        "env": "QB_CLIENT_SECRET",
        "required": False,
        "desc": "QuickBooks OAuth2 client secret",
        "agents": ["quickbooks"],
        "sensitive": True,
    },
    "qb_refresh_token": {
        "env": "QB_REFRESH_TOKEN",
        "required": False,
        "desc": "QuickBooks OAuth2 refresh token",
        "agents": ["quickbooks"],
        "sensitive": True,
    },
    "qb_realm_id": {
        "env": "QB_REALM_ID",
        "required": False,
        "desc": "QuickBooks company/realm ID",
        "agents": ["quickbooks"],
    },
    # Twilio (voice calls)
    "twilio_sid": {
        "env": "TWILIO_ACCOUNT_SID",
        "required": False,
        "desc": "Twilio account SID",
        "agents": ["voice_calls"],
    },
    "twilio_token": {
        "env": "TWILIO_AUTH_TOKEN",
        "required": False,
        "desc": "Twilio auth token",
        "agents": ["voice_calls"],
        "sensitive": True,
    },
    "twilio_phone": {
        "env": "TWILIO_PHONE_NUMBER",
        "required": False,
        "desc": "Twilio outbound phone number",
        "agents": ["voice_calls"],
    },
    # ElevenLabs (AI voice synthesis)
    "elevenlabs_key": {
        "env": "ELEVENLABS_API_KEY",
        "required": False,
        "desc": "ElevenLabs API key for voice synthesis",
        "agents": ["voice_calls"],
        "sensitive": True,
    },
    "elevenlabs_voice": {
        "env": "ELEVENLABS_VOICE_ID",
        "required": False,
        "desc": "ElevenLabs voice ID (clone or preset)",
        "agents": ["voice_calls"],
    },
    # Dashboard auth
    "dash_user": {
        "env": "DASH_USER",
        "required": True,
        "desc": "Dashboard login username",
        "agents": ["dashboard"],
        "default": "reytech",
    },
    "dash_pass": {
        "env": "DASH_PASS",
        "required": True,
        "desc": "Dashboard login password",
        "agents": ["dashboard"],
        "sensitive": True,
    },
}


# ─── Public API ──────────────────────────────────────────────────────────────

def get_key(name: str) -> str:
    """Get a secret value by registry name. Returns empty string if not set."""
    entry = _REGISTRY.get(name)
    if not entry:
        log.warning("Unknown secret requested: %s", name)
        return ""

    val = os.environ.get(entry["env"], "")
    if not val and "fallback" in entry:
        val = os.environ.get(entry["fallback"], "")
    if not val and "default" in entry:
        val = entry["default"]
    return val


def get_agent_key(agent_name: str) -> str:
    """Get the API key for a specific agent. Falls back to shared key."""
    agent_map = {
        "item_identifier": "agent_item_id",
        "lead_gen": "agent_leadgen",
        "pricing": "agent_pricing",
        "product_research": "serpapi",
    }
    reg_name = agent_map.get(agent_name, "anthropic_shared")
    return get_key(reg_name)


def mask(value: str) -> str:
    """Mask a secret for safe logging. Shows first 8 chars."""
    if not value:
        return "(not set)"
    if len(value) <= 12:
        return value[:4] + "****"
    return value[:8] + "****" + f"({len(value)} chars)"


def validate_all() -> dict:
    """Validate all secrets. Returns status report."""
    results = {}
    warnings = []
    for name, entry in _REGISTRY.items():
        val = get_key(name)
        is_set = bool(val)
        results[name] = {
            "set": is_set,
            "env": entry["env"],
            "desc": entry["desc"],
            "masked": mask(val) if not entry.get("sensitive") else ("✅ set" if is_set else "❌ not set"),
            "required": entry.get("required", False),
            "agents": entry["agents"],
        }
        if entry.get("required") and not is_set:
            warnings.append(f"REQUIRED secret missing: {entry['env']} ({entry['desc']})")
        if "fallback" in entry:
            results[name]["fallback"] = entry["fallback"]
            results[name]["using_fallback"] = (
                not os.environ.get(entry["env"]) and bool(os.environ.get(entry["fallback"]))
            )

    return {
        "secrets": results,
        "total": len(results),
        "set": sum(1 for r in results.values() if r["set"]),
        "missing": sum(1 for r in results.values() if not r["set"]),
        "warnings": warnings,
    }


def startup_check():
    """Run on startup. Logs warnings for missing critical secrets."""
    report = validate_all()
    set_count = report["set"]
    total = report["total"]
    log.info("Secrets: %d/%d configured", set_count, total)
    for w in report["warnings"]:
        log.warning("SECRET: %s", w)

    # Log which agents are active
    active_agents = set()
    for name, info in report["secrets"].items():
        if info["set"]:
            active_agents.update(info["agents"])
    if active_agents:
        log.info("Active agent keys: %s", ", ".join(sorted(active_agents)))

    return report
