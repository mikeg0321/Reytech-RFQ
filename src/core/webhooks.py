"""
src/core/webhooks.py — Centralized webhook dispatcher for Reytech events.

Fires outgoing webhooks for key business events. Supports Slack (native format),
generic JSON POST (Zapier/Make/custom), and configurable per-event toggles.

Usage:
    from src.core.webhooks import fire_event
    fire_event("new_rfq", {"rfq_id": "R-123", "agency": "CDCR", "items": 5})
"""

import os
import json
import logging
import threading
from datetime import datetime

log = logging.getLogger(__name__)

# ── Settings ─────────────────────────────────────────────────────────
try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.environ.get("REYTECH_DATA_DIR",
                              os.environ.get("DATA_DIR",
                              os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")))
WEBHOOK_CONFIG_FILE = os.path.join(DATA_DIR, "webhook_config.json")

# Event types and their descriptions
EVENT_TYPES = {
    "new_rfq": "New RFQ/PC received",
    "quote_sent": "Quote sent to buyer",
    "quote_won": "Quote marked as won",
    "quote_lost": "Quote marked as lost",
    "order_created": "Order created from quote",
    "order_shipped": "Order item shipped",
    "order_delivered": "Order delivered",
    "follow_up_due": "Follow-up is overdue",
    "scprs_complete": "SCPRS data pull completed",
    "intel_complete": "Buyer intelligence scan completed",
}


def _load_config() -> dict:
    """Load webhook configuration."""
    try:
        with open(WEBHOOK_CONFIG_FILE) as f:
            return json.load(f)
    except Exception:
        return {"webhooks": [], "enabled": True}


def _save_config(config: dict):
    """Save webhook configuration."""
    try:
        with open(WEBHOOK_CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        log.error("Failed to save webhook config: %s", e)


def get_config() -> dict:
    """Get webhook configuration for settings UI."""
    config = _load_config()
    # Also include env-var webhooks
    slack_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    growth_url = os.environ.get("GROWTH_WEBHOOK_URL", "")
    return {
        "webhooks": config.get("webhooks", []),
        "enabled": config.get("enabled", True),
        "env_slack": bool(slack_url),
        "env_growth": bool(growth_url),
        "event_types": EVENT_TYPES,
    }


def save_webhook(name: str, url: str, events: list, format_type: str = "json") -> dict:
    """Add or update a webhook endpoint."""
    config = _load_config()
    webhooks = config.get("webhooks", [])

    # Update existing or add new
    existing = next((w for w in webhooks if w["name"] == name), None)
    if existing:
        existing["url"] = url
        existing["events"] = events
        existing["format"] = format_type
        existing["updated_at"] = datetime.now().isoformat()
    else:
        webhooks.append({
            "name": name,
            "url": url,
            "events": events,
            "format": format_type,
            "enabled": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "fire_count": 0,
            "last_fired": "",
            "last_error": "",
        })

    config["webhooks"] = webhooks
    _save_config(config)
    return {"ok": True}


def delete_webhook(name: str) -> dict:
    """Remove a webhook endpoint."""
    config = _load_config()
    config["webhooks"] = [w for w in config.get("webhooks", []) if w["name"] != name]
    _save_config(config)
    return {"ok": True}


def fire_event(event_type: str, payload: dict):
    """Fire webhook for an event. Runs in background thread to not block."""
    if event_type not in EVENT_TYPES:
        return

    def _do_fire():
        import urllib.request
        import urllib.error

        config = _load_config()
        if not config.get("enabled", True):
            return

        # Build event data
        event_data = {
            "event": event_type,
            "event_label": EVENT_TYPES.get(event_type, event_type),
            "timestamp": datetime.now().isoformat(),
            "source": "reytech-rfq",
            **payload,
        }

        # Fire to configured webhooks
        for webhook in config.get("webhooks", []):
            if not webhook.get("enabled", True):
                continue
            if event_type not in webhook.get("events", []):
                continue

            url = webhook.get("url", "")
            if not url:
                continue

            try:
                fmt = webhook.get("format", "json")
                if fmt == "slack":
                    # Slack format: {text: "..."}
                    text = f"*{EVENT_TYPES.get(event_type, event_type)}*\n"
                    for k, v in payload.items():
                        if k not in ("event", "timestamp", "source"):
                            text += f"• {k}: {v}\n"
                    body = json.dumps({"text": text}).encode()
                else:
                    body = json.dumps(event_data).encode()

                req = urllib.request.Request(
                    url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    webhook["fire_count"] = webhook.get("fire_count", 0) + 1
                    webhook["last_fired"] = datetime.now().isoformat()
                    webhook["last_error"] = ""
            except Exception as e:
                webhook["last_error"] = str(e)[:200]
                log.debug("Webhook %s error: %s", webhook["name"], e)

        # Also fire to env-var Slack webhook for key events
        slack_url = os.environ.get("SLACK_WEBHOOK_URL", "")
        if slack_url and event_type in ("new_rfq", "quote_won", "order_created"):
            try:
                text = f"🔔 *{EVENT_TYPES.get(event_type, event_type)}*\n"
                for k, v in payload.items():
                    text += f"• {k}: {v}\n"
                body = json.dumps({"text": text}).encode()
                req = urllib.request.Request(
                    slack_url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                log.debug("Slack webhook error: %s", e)

        _save_config(config)

    threading.Thread(target=_do_fire, daemon=True).start()
