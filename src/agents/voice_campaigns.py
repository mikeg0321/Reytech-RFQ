"""
voice_campaigns.py — Campaign Manager for Reytech Voice Agent
Phase 23 | Version: 1.0.0

Manages outbound call campaigns:
- Define campaign with target list, script, schedule
- Pull targets from customers.json, leads.json, or manual list
- Track call outcomes per campaign
- Analytics: connect rate, conversation rate, conversion rate

Campaign types:
  1. New vendor intro — cold call purchasing departments
  2. Quote follow-up — chase pending/sent quotes
  3. Win thank-you — relationship building after won quotes
  4. Reactivation — reach out to institutions we haven't contacted in 30+ days
  5. SCPRS opportunity — call about specific POs found via SCPRS scanner
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("voice_campaigns")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

CAMPAIGNS_FILE = os.path.join(DATA_DIR, "voice_campaigns.json")


# ─── Enhanced Scripts for Real Campaigns ────────────────────────────────────

CAMPAIGN_SCRIPTS = {
    # ── New Vendor Introduction ──
    "vendor_intro_cdcr": {
        "name": "CDCR Vendor Introduction",
        "campaign_type": "new_vendor",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. I'm reaching out to "
            "{institution}'s purchasing department. We're a certified Small Business "
            "and DVBE reseller, and we specialize in supplying goods to CDCR facilities. "
            "I was hoping to introduce our services — do you have a moment?"
        ),
        "context": (
            "You are introducing Reytech to a CDCR facility's purchasing department. "
            "Key talking points:\n"
            "- We're SB/DVBE certified — helps them meet procurement mandates\n"
            "- We supply office, janitorial, medical, IT, and facility supplies\n"
            "- We respond to quotes within 24 hours\n"
            "- We've worked with other CDCR facilities like CSP-Sacramento\n"
            "- Ask: 'What's the best way to get on your vendor list for quotes?'\n"
            "- Ask: 'Do you have any upcoming procurement needs we could quote on?'\n"
            "- If they say they use BidSync or Cal eProcure, say you're registered there too\n"
            "- GOAL: Get their direct email for the purchasing contact and get on the bid list"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech Inc., a certified Small Business and DVBE "
            "reseller. We specialize in supplying goods to CDCR facilities and I'd love "
            "to introduce our services. Please call us back at 949-229-1575 or email "
            "sales@reytechinc.com. That's Reytech, R-E-Y-T-E-C-H. Thank you."
        ),
    },

    "vendor_intro_calvet": {
        "name": "CalVet Vendor Introduction",
        "campaign_type": "new_vendor",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. I'm reaching out to the "
            "purchasing department at {institution}. We're a certified Small Business "
            "and Disabled Veteran Business Enterprise that supplies goods to California "
            "veterans homes. Do you have a minute?"
        ),
        "context": (
            "You are introducing Reytech to a CalVet facility. "
            "Key talking points:\n"
            "- DVBE certification is especially relevant — CalVet values veteran-owned businesses\n"
            "- We supply medical, office, janitorial, and facility supplies\n"
            "- We've worked with CalVet Barstow Veterans Home\n"
            "- Emphasize the DVBE angle — it's personal, Mike is a veteran\n"
            "- Ask: 'What procurement method do you use for supplies?'\n"
            "- Ask: 'Is there a bid list I can get added to?'\n"
            "- GOAL: Get purchasing contact info and learn their procurement cycle"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech Inc. We're a Disabled Veteran Business "
            "Enterprise that supplies goods to California veterans homes. I'd love to "
            "discuss how we can support {institution}. Please reach us at 949-229-1575 "
            "or sales@reytechinc.com. Thank you for your service."
        ),
    },

    # ── Quote Follow-Up ──
    "quote_follow_up_warm": {
        "name": "Quote Follow-Up (Warm)",
        "campaign_type": "follow_up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm following up on Quote {quote_number} "
            "that we submitted for {institution} on {quote_date}. I wanted to check if "
            "you had a chance to review our pricing. Do you have a quick minute?"
        ),
        "context": (
            "You submitted a quote and are following up. Be helpful, not pushy.\n"
            "- If they haven't reviewed it: Offer to resend or walk through key items\n"
            "- If they have concerns about pricing: Ask which items specifically, "
            "offer to sharpen pricing on those\n"
            "- If they went with another vendor: Ask what the deciding factor was "
            "(price? relationship? delivery time?) — this is intel for future bids\n"
            "- If they're still deciding: Ask about their timeline and offer to "
            "hold pricing for a specific period\n"
            "- If they need different items: Offer to revise the quote\n"
            "- GOAL: Either close the deal or learn why we lost for future improvement"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech following up on Quote {quote_number} for "
            "{institution}. Just checking if you had any questions about our pricing. "
            "Feel free to reach me at 949-229-1575 or sales@reytechinc.com. "
            "Happy to adjust anything. Thank you."
        ),
    },

    "quote_follow_up_urgent": {
        "name": "Quote Follow-Up (Expiring)",
        "campaign_type": "follow_up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling about Quote {quote_number} "
            "for {institution}. I wanted to give you a heads up that our pricing on this "
            "will be expiring soon. Is there anything I can do to help move this forward?"
        ),
        "context": (
            "This quote is close to expiring (>30 days old). Create mild urgency.\n"
            "- Don't be aggressive — just helpful\n"
            "- Offer: 'I can extend the pricing if you need more time'\n"
            "- Ask: 'Is there a different timeline for this procurement?'\n"
            "- If they need a revised scope: Offer to update the quote same day\n"
            "- GOAL: Get a commitment or extend the quote with updated pricing"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech about Quote {quote_number} for {institution}. "
            "Wanted to let you know our pricing will be expiring soon — happy to extend "
            "it if you need more time. Call me at 949-229-1575. Thanks."
        ),
    },

    # ── Win Thank You / Upsell ──
    "win_thank_you": {
        "name": "Won Order Thank You",
        "campaign_type": "relationship",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling to say thank you for "
            "the recent order — PO {po_number} for {institution}. We really appreciate "
            "the business. I wanted to make sure everything arrived as expected?"
        ),
        "context": (
            "You won an order and are building the relationship.\n"
            "- Confirm delivery was satisfactory\n"
            "- Ask: 'Is there anything we could have done better?'\n"
            "- Ask: 'Do you have any upcoming needs we could help with?'\n"
            "- Mention: 'We can usually beat pricing on most office and facility supplies'\n"
            "- If they mention other items: Note them for a follow-up quote\n"
            "- GOAL: Strengthen relationship, learn about future needs, plant seeds for repeat business"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech. Just calling to thank you for the recent "
            "order and make sure everything arrived okay. If you need anything, I'm at "
            "949-229-1575. We appreciate your business."
        ),
    },

    # ── Reactivation ──
    "reactivation": {
        "name": "Reactivation (30+ Days No Contact)",
        "campaign_type": "reactivation",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. We've worked with {institution} "
            "in the past and I wanted to check in. We've been expanding our product "
            "lines and I thought there might be some upcoming needs we could help with. "
            "Is someone in purchasing available?"
        ),
        "context": (
            "It's been a while since we've been in touch. Re-engage warmly.\n"
            "- Reference past work if we have history\n"
            "- Ask about current procurement needs\n"
            "- Mention we've expanded capabilities (new product lines, faster shipping)\n"
            "- Ask: 'What's your procurement cycle look like for Q2?'\n"
            "- Offer to send a capabilities sheet via email\n"
            "- GOAL: Re-establish contact, learn about upcoming bids"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech Inc. We've worked with {institution} before "
            "and I wanted to check in about any upcoming procurement needs. We've expanded "
            "our product lines and would love to quote. Reach me at 949-229-1575 or "
            "sales@reytechinc.com. Thank you."
        ),
    },

    # ── SCPRS Opportunity ──
    "scprs_opportunity": {
        "name": "SCPRS PO Opportunity",
        "campaign_type": "lead_gen",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I noticed a recent purchase order — "
            "PO {po_number} — for {institution}, and we supply many of those same "
            "items at competitive pricing. Could I speak with someone in purchasing "
            "about future orders like this?"
        ),
        "context": (
            "You found a PO on SCPRS and are cold-calling about it.\n"
            "- Be specific about the PO but don't be creepy — it's public data\n"
            "- Mention specific items if available from the lead data\n"
            "- Emphasize SB/DVBE certification — helps their procurement goals\n"
            "- Ask: 'For orders like this, how do vendors typically get on the quote list?'\n"
            "- Ask: 'Who would be the right person to send a capabilities sheet to?'\n"
            "- If they ask how you found the PO: 'It's publicly listed on the state procurement system'\n"
            "- GOAL: Get on their vendor list for future similar orders"
        ),
        "voicemail": (
            "Hi, this is Mike from Reytech Inc., a certified SB/DVBE reseller. "
            "I'm calling about a recent purchase order for {institution} — we supply "
            "similar items at competitive pricing. Please reach me at 949-229-1575 or "
            "sales@reytechinc.com. Thank you."
        ),
    },

    # ── Keep the original scripts for backward compat ──
    "lead_intro": {
        "name": "Lead Introduction",
        "campaign_type": "lead_gen",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. I'm reaching out about "
            "Purchase Order {po_number} for {institution}. We're a certified Small Business "
            "reseller and I was wondering if I could speak with someone in purchasing?"
        ),
        "context": "You are calling about a specific Purchase Order. Your goal is to introduce Reytech and get on the vendor quote list.",
    },
    "follow_up": {
        "name": "Quote Follow-Up",
        "campaign_type": "follow_up",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm following up on Quote {quote_number} "
            "that we submitted for {institution}. Do you have a few minutes?"
        ),
        "context": "You are following up on a quote you already submitted. Ask if they have questions, offer to adjust pricing if needed.",
    },
    "intro_cold": {
        "name": "Cold Intro",
        "campaign_type": "new_vendor",
        "first_message": (
            "Hi, this is Mike calling from Reytech Inc. We're a certified Small Business "
            "reseller that works with California state agencies. I was hoping to introduce "
            "our services — is someone in purchasing available?"
        ),
        "context": "This is a cold call. You don't have a specific PO. Focus on introducing Reytech and learning about their upcoming needs.",
    },
    "thank_you": {
        "name": "Thank You / Won",
        "campaign_type": "relationship",
        "first_message": (
            "Hi, this is Mike from Reytech Inc. I'm calling to say thank you for the "
            "order on {po_number}. We really appreciate the business and wanted to make "
            "sure everything arrived as expected."
        ),
        "context": "You are calling to thank them for a won order. Build the relationship, ask if they need anything else.",
    },
}


# ─── Campaign Data Layer ────────────────────────────────────────────────────

def _load_campaigns() -> dict:
    try:
        with open(CAMPAIGNS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_campaigns(campaigns: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CAMPAIGNS_FILE, "w") as f:
        json.dump(campaigns, f, indent=2, default=str)


# ─── Campaign Builder ───────────────────────────────────────────────────────

def create_campaign(name: str, script_key: str, target_type: str,
                     filters: dict = None) -> dict:
    """
    Create a new outbound call campaign.
    
    Args:
        name: Campaign name (e.g., "CDCR Q1 Intro Blitz")
        script_key: Which script to use (from CAMPAIGN_SCRIPTS)
        target_type: "cdcr", "calvet", "all_customers", "leads", "manual"
        filters: Optional filters (e.g., {"agency": "CDCR", "min_score": 0.7})
    """
    campaigns = _load_campaigns()
    cid = f"CAMP-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    filters = filters or {}

    # Build target list
    targets = _build_target_list(target_type, filters)

    campaign = {
        "id": cid,
        "name": name,
        "script_key": script_key,
        "script_name": CAMPAIGN_SCRIPTS.get(script_key, {}).get("name", script_key),
        "target_type": target_type,
        "filters": filters,
        "status": "draft",  # draft → active → paused → completed
        "targets": targets,
        "total_targets": len(targets),
        "calls_made": 0,
        "calls_connected": 0,
        "calls_voicemail": 0,
        "calls_no_answer": 0,
        "calls_error": 0,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "call_log": [],
    }

    campaigns[cid] = campaign
    _save_campaigns(campaigns)
    log.info("Campaign created: %s (%d targets)", cid, len(targets))
    return campaign


def _build_target_list(target_type: str, filters: dict) -> list:
    """Build list of call targets based on type and filters."""
    targets = []

    if target_type in ("cdcr", "calvet", "all_customers"):
        try:
            with open(os.path.join(DATA_DIR, "customers.json")) as f:
                customers = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            customers = []

        for c in customers:
            if not isinstance(c, dict) or not c.get("phone"):
                continue
            agency = c.get("agency", "").upper()
            if target_type == "cdcr" and agency != "CDCR":
                continue
            if target_type == "calvet" and agency.upper() not in ("CALVET", "CALVET"):
                continue

            targets.append({
                "name": c.get("display_name", ""),
                "phone": _normalize_phone(c.get("phone", "")),
                "institution": c.get("company", c.get("display_name", "")),
                "agency": agency,
                "email": c.get("email", ""),
                "source": "customers",
                "status": "pending",  # pending → called → connected → voicemail → no_answer → error
            })

    elif target_type == "leads":
        try:
            with open(os.path.join(DATA_DIR, "leads.json")) as f:
                leads = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            leads = []

        min_score = filters.get("min_score", 0.5)
        for l in (leads if isinstance(leads, list) else []):
            if not isinstance(l, dict) or not l.get("buyer_phone"):
                continue
            if l.get("score", 0) < min_score:
                continue
            targets.append({
                "name": l.get("buyer_name", ""),
                "phone": _normalize_phone(l.get("buyer_phone", "")),
                "institution": l.get("institution", ""),
                "po_number": l.get("po_number", ""),
                "score": l.get("score", 0),
                "source": "leads",
                "status": "pending",
            })

    elif target_type == "quotes_pending":
        # Follow up on pending/sent quotes
        try:
            with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
                quotes = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            quotes = []

        # Match quotes to customer phones
        try:
            with open(os.path.join(DATA_DIR, "customers.json")) as f:
                customers = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            customers = []

        cust_by_name = {}
        for c in customers:
            if isinstance(c, dict) and c.get("phone"):
                cust_by_name[c.get("display_name", "").lower()] = c
                cust_by_name[c.get("company", "").lower()] = c

        for q in quotes:
            if q.get("status") not in ("pending", "sent"):
                continue
            inst = q.get("institution", "") or q.get("ship_to_name", "")
            cust = cust_by_name.get(inst.lower())
            if cust and cust.get("phone"):
                targets.append({
                    "name": inst,
                    "phone": _normalize_phone(cust["phone"]),
                    "institution": inst,
                    "quote_number": q.get("quote_number", ""),
                    "quote_date": q.get("date", ""),
                    "total": q.get("total", 0),
                    "source": "quotes",
                    "status": "pending",
                })

    return targets


def _normalize_phone(phone: str) -> str:
    """Convert various phone formats to E.164."""
    import re
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return phone  # return as-is if can't normalize


# ─── Campaign Execution ─────────────────────────────────────────────────────

def execute_campaign_call(campaign_id: str, target_index: int = None) -> dict:
    """
    Place the next call in a campaign (or a specific target).
    Returns call result.
    """
    campaigns = _load_campaigns()
    camp = campaigns.get(campaign_id)
    if not camp:
        return {"ok": False, "error": "Campaign not found"}

    if camp["status"] not in ("active", "draft"):
        return {"ok": False, "error": f"Campaign is {camp['status']}"}

    # Find next pending target
    targets = camp.get("targets", [])
    if target_index is not None:
        if 0 <= target_index < len(targets):
            target = targets[target_index]
        else:
            return {"ok": False, "error": "Invalid target index"}
    else:
        target = next((t for t in targets if t["status"] == "pending"), None)
        if not target:
            camp["status"] = "completed"
            camp["updated_at"] = datetime.now().isoformat()
            campaigns[campaign_id] = camp
            _save_campaigns(campaigns)
            return {"ok": False, "error": "No more pending targets — campaign complete"}

    # Build variables for the script
    variables = {
        "institution": target.get("institution", "your facility"),
        "po_number": target.get("po_number", ""),
        "quote_number": target.get("quote_number", ""),
        "quote_date": target.get("quote_date", ""),
        "buyer_name": target.get("name", ""),
    }

    # Place the call
    try:
        from src.agents.voice_agent import place_call
        result = place_call(
            phone_number=target["phone"],
            script_key=camp["script_key"],
            variables=variables,
        )
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    # Update target status
    if result.get("ok"):
        target["status"] = "called"
        target["call_id"] = result.get("call_id") or result.get("call_sid", "")
        target["called_at"] = datetime.now().isoformat()
        camp["calls_made"] += 1
    else:
        target["status"] = "error"
        target["error"] = result.get("error", "")
        camp["calls_error"] += 1

    camp["status"] = "active"
    camp["updated_at"] = datetime.now().isoformat()

    # Log
    camp["call_log"].append({
        "target": target.get("name", target.get("phone", "")),
        "phone": target["phone"],
        "result": "ok" if result.get("ok") else "error",
        "timestamp": datetime.now().isoformat(),
        "call_id": target.get("call_id", ""),
    })

    campaigns[campaign_id] = camp
    _save_campaigns(campaigns)
    return {**result, "campaign_id": campaign_id, "target": target.get("name", ""),
            "remaining": sum(1 for t in targets if t["status"] == "pending")}


def update_call_outcome(campaign_id: str, phone: str, outcome: str) -> dict:
    """Update a call's outcome after it completes.
    Outcomes: connected, voicemail, no_answer, callback_requested, interested, not_interested"""
    campaigns = _load_campaigns()
    camp = campaigns.get(campaign_id)
    if not camp:
        return {"ok": False, "error": "Campaign not found"}

    for target in camp.get("targets", []):
        if target.get("phone") == phone:
            target["outcome"] = outcome
            target["outcome_at"] = datetime.now().isoformat()
            if outcome == "connected":
                camp["calls_connected"] += 1
            elif outcome == "voicemail":
                camp["calls_voicemail"] += 1
            elif outcome == "no_answer":
                camp["calls_no_answer"] += 1
            break

    camp["updated_at"] = datetime.now().isoformat()
    campaigns[campaign_id] = camp
    _save_campaigns(campaigns)
    return {"ok": True}


# ─── Campaign Analytics ─────────────────────────────────────────────────────

def get_campaign_stats(campaign_id: str = None) -> dict:
    """Get campaign analytics."""
    campaigns = _load_campaigns()

    if campaign_id:
        camp = campaigns.get(campaign_id)
        if not camp:
            return {"ok": False, "error": "Campaign not found"}
        return {"ok": True, **_calc_stats(camp)}

    # All campaigns summary
    all_camps = []
    for cid, camp in sorted(campaigns.items(), key=lambda x: x[1].get("created_at", ""), reverse=True):
        all_camps.append({
            "id": cid,
            "name": camp.get("name", ""),
            "script": camp.get("script_name", ""),
            "status": camp.get("status", ""),
            "total": camp.get("total_targets", 0),
            "called": camp.get("calls_made", 0),
            "connected": camp.get("calls_connected", 0),
            "created_at": camp.get("created_at", ""),
            **_calc_stats(camp),
        })

    return {"ok": True, "campaigns": all_camps, "total": len(all_camps)}


def _calc_stats(camp: dict) -> dict:
    """Calculate campaign metrics."""
    total = camp.get("total_targets", 0) or 1
    made = camp.get("calls_made", 0)
    connected = camp.get("calls_connected", 0)
    vm = camp.get("calls_voicemail", 0)

    targets = camp.get("targets", [])
    interested = sum(1 for t in targets if t.get("outcome") == "interested")
    callbacks = sum(1 for t in targets if t.get("outcome") == "callback_requested")

    return {
        "completion_rate": round(made / total * 100),
        "connect_rate": round(connected / made * 100) if made > 0 else 0,
        "voicemail_rate": round(vm / made * 100) if made > 0 else 0,
        "interest_rate": round(interested / connected * 100) if connected > 0 else 0,
        "remaining": sum(1 for t in targets if t.get("status") == "pending"),
        "interested": interested,
        "callbacks": callbacks,
    }


# ─── Pre-built Campaign Templates ───────────────────────────────────────────

CAMPAIGN_TEMPLATES = {
    "cdcr_intro_blitz": {
        "name": "CDCR Vendor Introduction Blitz",
        "description": "Introduce Reytech to all CDCR purchasing departments",
        "script_key": "vendor_intro_cdcr",
        "target_type": "cdcr",
    },
    "calvet_intro": {
        "name": "CalVet Veterans Homes Outreach",
        "description": "Introduce Reytech to CalVet facilities (DVBE angle)",
        "script_key": "vendor_intro_calvet",
        "target_type": "calvet",
    },
    "quote_chase": {
        "name": "Pending Quote Follow-Up",
        "description": "Follow up on all pending/sent quotes",
        "script_key": "quote_follow_up_warm",
        "target_type": "quotes_pending",
    },
    "full_blitz": {
        "name": "Full Customer Outreach",
        "description": "Call every customer with a phone number",
        "script_key": "intro_cold",
        "target_type": "all_customers",
    },
}


def list_scripts() -> list:
    """List all available campaign scripts."""
    return [
        {"key": k, "name": v["name"], "type": v.get("campaign_type", "general"),
         "has_voicemail": bool(v.get("voicemail"))}
        for k, v in CAMPAIGN_SCRIPTS.items()
    ]


def list_templates() -> list:
    """List pre-built campaign templates."""
    result = []
    for tid, tmpl in CAMPAIGN_TEMPLATES.items():
        targets = _build_target_list(tmpl["target_type"], {})
        result.append({
            "id": tid,
            "name": tmpl["name"],
            "description": tmpl["description"],
            "script": tmpl["script_key"],
            "estimated_targets": len(targets),
        })
    return result
