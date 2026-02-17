"""
lead_gen_agent.py — SCPRS Lead Generation Agent for Reytech
Phase 13 | Version: 1.0.0

The proactive revenue agent. Instead of waiting for RFQs, this agent:
1. Monitors SCPRS for new Purchase Orders matching Reytech's product capabilities
2. Cross-references against won_quotes_db for items we've sold before (cheaper)
3. Scores opportunities by estimated margin × probability of winning
4. Queues high-value leads for outreach (email/phone)

This is Agent 1 from the Phase 14 multi-agent architecture.

Pipeline:
  Poll SCPRS → Filter by category → Match against won history →
  Score opportunity → Queue for outreach → Draft contact message

Revenue model: Each lead costs ~$0.01 in API calls.
  Even 1 won bid per month pays for the entire agent infrastructure.

Dependencies: requests
Optional: anthropic SDK for smart opportunity scoring
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("leadgen")

# ─── Configuration ───────────────────────────────────────────────────────────

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

LEADS_FILE = os.path.join(DATA_DIR, "leads.json")
LEAD_HISTORY_FILE = os.path.join(DATA_DIR, "lead_history.json")
MAX_LEADS = 500

# Agent-specific API key — use centralized secret registry
try:
    from src.core.secrets import get_agent_key
    ANTHROPIC_API_KEY = get_agent_key("lead_gen")
except ImportError:
    ANTHROPIC_API_KEY = os.environ.get("AGENT_LEADGEN_KEY",
                       os.environ.get("ANTHROPIC_API_KEY", ""))

# SCPRS polling config
POLL_INTERVAL_SECONDS = 60  # Check every minute when running
MIN_PO_VALUE = 100          # Minimum PO value worth pursuing ($)
MAX_PO_VALUE = 50000        # Cap — too large = prime contracts we can't compete on
CONFIDENCE_THRESHOLD = 0.5  # Minimum match confidence to generate a lead

# ─── Categories we can source ────────────────────────────────────────────────
# Maps to item_identifier.py categories — only pursue what we can supply

SOURCEABLE_CATEGORIES = {
    "office", "medical", "janitorial", "food_service", "safety", "technology",
}

# Institutions we've sold to (seeded, grows with won quotes)
KNOWN_INSTITUTIONS = {
    "CSP-Sacramento", "CSP-Solano", "CSP-LAC", "CSP-Corcoran",
    "CIM", "CIW", "CMC", "CMF", "CTF", "CHCF", "SATF",
    "SCC", "MCSP", "HDSP", "PBSP", "KVSP", "SVSP", "WSP",
    "CalVet-Barstow", "CalVet-Yountville", "CalVet-Chula Vista",
    "DSH-Atascadero", "DSH-Coalinga", "DSH-Metropolitan", "DSH-Napa", "DSH-Patton",
}


# ─── Lead Data Model ────────────────────────────────────────────────────────

def _create_lead(po_data: dict, match_data: dict, score: float) -> dict:
    """Create a lead record from a SCPRS PO match."""
    return {
        "id": hashlib.md5(
            f"{po_data.get('po_number', '')}{time.time()}".encode()
        ).hexdigest()[:12],
        "status": "new",  # new → contacted → quoted → won/lost/expired
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),

        # PO data from SCPRS
        "po_number": po_data.get("po_number", ""),
        "institution": po_data.get("institution", ""),
        "agency": po_data.get("agency", ""),
        "buyer_name": po_data.get("buyer_name", ""),
        "buyer_email": po_data.get("buyer_email", ""),
        "buyer_phone": po_data.get("buyer_phone", ""),
        "po_value": po_data.get("total_value", 0),
        "items_count": po_data.get("items_count", 0),
        "po_date": po_data.get("date", ""),
        "due_date": po_data.get("due_date", ""),

        # Match analysis
        "match_type": match_data.get("type", "category"),
        "matched_items": match_data.get("matched_items", []),
        "our_historical_price": match_data.get("our_price", 0),
        "scprs_listed_price": match_data.get("scprs_price", 0),
        "estimated_savings_pct": match_data.get("savings_pct", 0),
        "category": match_data.get("category", "general"),

        # Scoring
        "score": round(score, 2),
        "score_breakdown": match_data.get("score_breakdown", {}),

        # Outreach tracking
        "outreach_draft": None,
        "outreach_sent_at": None,
        "response_received_at": None,
        "notes": "",
    }


# ─── Lead Storage ────────────────────────────────────────────────────────────

def _load_leads() -> list:
    try:
        with open(LEADS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_leads(leads: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    if len(leads) > MAX_LEADS:
        leads = sorted(leads, key=lambda x: x.get("score", 0), reverse=True)[:MAX_LEADS]
    with open(LEADS_FILE, "w") as f:
        json.dump(leads, f, indent=2)


def _log_history(lead: dict, action: str):
    """Append to lead history log for analytics."""
    try:
        with open(LEAD_HISTORY_FILE) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []
    history.append({
        "lead_id": lead["id"],
        "action": action,
        "timestamp": datetime.now().isoformat(),
        "po_number": lead.get("po_number"),
        "score": lead.get("score"),
        "institution": lead.get("institution"),
    })
    # Keep last 2000 entries
    if len(history) > 2000:
        history = history[-2000:]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(LEAD_HISTORY_FILE, "w") as f:
        json.dump(history, f)


# ─── Opportunity Scoring ─────────────────────────────────────────────────────

def score_opportunity(po_data: dict, match_data: dict) -> float:
    """
    Score a lead opportunity 0.0 → 1.0.

    Factors:
    - Item match confidence (have we sold this before?)
    - Price advantage (can we undercut SCPRS price?)
    - Institution familiarity (have we worked with this buyer?)
    - PO value (sweet spot: $500-$10,000)
    - Urgency (sooner due date = more value)
    """
    score = 0.0
    breakdown = {}

    # 1. Item match (0-0.3)
    match_conf = match_data.get("match_confidence", 0)
    item_score = min(0.3, match_conf * 0.35)
    breakdown["item_match"] = round(item_score, 3)
    score += item_score

    # 2. Price advantage (0-0.25)
    our_price = match_data.get("our_price", 0)
    scprs_price = match_data.get("scprs_price", 0)
    if our_price > 0 and scprs_price > 0:
        savings = (scprs_price - our_price) / scprs_price
        price_score = min(0.25, max(0, savings * 0.5))
        breakdown["price_advantage"] = round(price_score, 3)
        score += price_score

    # 3. Institution familiarity (0-0.2)
    inst = po_data.get("institution", "")
    if inst in KNOWN_INSTITUTIONS:
        inst_score = 0.2
    elif any(k in inst for k in ["CSP", "CIM", "CMC", "CalVet", "DSH"]):
        inst_score = 0.1
    else:
        inst_score = 0.0
    breakdown["institution"] = round(inst_score, 3)
    score += inst_score

    # 4. PO value sweet spot (0-0.15)
    value = po_data.get("total_value", 0)
    if 500 <= value <= 10000:
        val_score = 0.15
    elif 200 <= value <= 20000:
        val_score = 0.08
    elif value > 0:
        val_score = 0.03
    else:
        val_score = 0.0
    breakdown["po_value"] = round(val_score, 3)
    score += val_score

    # 5. Urgency bonus (0-0.1)
    due = po_data.get("due_date", "")
    if due:
        try:
            due_dt = datetime.strptime(due, "%m/%d/%Y")
            days_out = (due_dt - datetime.now()).days
            if 3 <= days_out <= 14:
                urg_score = 0.1
            elif 14 < days_out <= 30:
                urg_score = 0.05
            else:
                urg_score = 0.0
        except ValueError:
            urg_score = 0.0
    else:
        urg_score = 0.0
    breakdown["urgency"] = round(urg_score, 3)
    score += urg_score

    match_data["score_breakdown"] = breakdown
    return min(1.0, score)


# ─── Outreach Draft ──────────────────────────────────────────────────────────

def draft_outreach_email(lead: dict) -> dict:
    """
    Draft an outreach email for a lead.
    Rule-based template. LLM-enhanced version uses Claude for personalization.
    """
    inst = lead.get("institution", "the institution")
    buyer = lead.get("buyer_name", "Purchasing Department")
    po = lead.get("po_number", "")
    items = lead.get("matched_items", [])
    savings = lead.get("estimated_savings_pct", 0)

    item_mention = ""
    if items:
        item_names = [it.get("description", "")[:50] for it in items[:3]]
        item_mention = f" including {', '.join(item_names)}"

    subject = f"Reytech Inc. — Competitive Pricing for {inst} PO {po}"

    body = f"""Dear {buyer},

I noticed Purchase Order {po} for {inst}{item_mention} and wanted to reach out. Reytech Inc. is a certified Small Business reseller specializing in state procurement, and we've previously supplied similar items{f' at {savings:.0f}% below current listed pricing' if savings > 5 else ' at competitive rates'}.

We'd love the opportunity to be added to the quote list for this and future orders. We can typically respond within 24 hours with competitive pricing.

Would you be open to a brief call or email exchange to discuss?

Best regards,
Mike Gonzales
Reytech Inc.
sales@reytechinc.com"""

    return {
        "subject": subject,
        "body": body,
        "to": lead.get("buyer_email", ""),
        "lead_id": lead["id"],
    }


# ─── Public API ──────────────────────────────────────────────────────────────

def evaluate_po(po_data: dict, won_history: list = None) -> Optional[dict]:
    """
    Evaluate a SCPRS Purchase Order as a potential lead.

    Args:
        po_data: PO from SCPRS with po_number, institution, items, total_value, etc.
        won_history: Optional list of won quotes to match against.

    Returns:
        Lead dict if opportunity scores above threshold, None otherwise.
    """
    value = po_data.get("total_value", 0)
    if value < MIN_PO_VALUE or value > MAX_PO_VALUE:
        return None

    # Build match data
    match_data = {
        "type": "scprs_scan",
        "match_confidence": 0,
        "our_price": 0,
        "scprs_price": value,
        "matched_items": [],
        "category": po_data.get("category", "general"),
    }

    # Check won history for item matches
    if won_history:
        po_items = po_data.get("items", [])
        for po_item in po_items:
            desc = po_item.get("description", "").lower()
            for hist in won_history:
                hist_desc = hist.get("description", "").lower()
                # Simple token overlap
                po_tokens = set(desc.split())
                hist_tokens = set(hist_desc.split())
                overlap = len(po_tokens & hist_tokens)
                if overlap >= 2:
                    conf = min(0.9, overlap / max(len(po_tokens), 1))
                    if conf > match_data["match_confidence"]:
                        match_data["match_confidence"] = conf
                        match_data["our_price"] = hist.get("unit_price", 0)
                    match_data["matched_items"].append({
                        "description": po_item.get("description", ""),
                        "our_historical": hist.get("description", ""),
                        "confidence": round(conf, 2),
                    })

    # Score the opportunity
    score = score_opportunity(po_data, match_data)

    if score < CONFIDENCE_THRESHOLD:
        return None

    lead = _create_lead(po_data, match_data, score)
    _log_history(lead, "created")
    return lead


def add_lead(lead: dict) -> dict:
    """Add a lead to the active leads list."""
    leads = _load_leads()

    # Deduplicate by PO number
    existing = [l for l in leads if l.get("po_number") == lead.get("po_number")]
    if existing:
        return {"ok": False, "reason": "duplicate", "existing_id": existing[0]["id"]}

    leads.append(lead)
    _save_leads(leads)
    return {"ok": True, "lead_id": lead["id"]}


def get_leads(status: str = None, min_score: float = 0,
              limit: int = 50) -> list:
    """Get leads, optionally filtered by status and minimum score."""
    leads = _load_leads()
    if status:
        leads = [l for l in leads if l.get("status") == status]
    if min_score > 0:
        leads = [l for l in leads if l.get("score", 0) >= min_score]
    leads.sort(key=lambda x: x.get("score", 0), reverse=True)
    return leads[:limit]


def update_lead_status(lead_id: str, new_status: str, notes: str = "") -> dict:
    """Update a lead's status. Valid: new, contacted, quoted, won, lost, expired."""
    valid = {"new", "contacted", "quoted", "won", "lost", "expired"}
    if new_status not in valid:
        return {"ok": False, "error": f"Invalid status. Must be one of: {valid}"}

    leads = _load_leads()
    for lead in leads:
        if lead["id"] == lead_id:
            lead["status"] = new_status
            lead["updated_at"] = datetime.now().isoformat()
            if notes:
                lead["notes"] = notes
            _save_leads(leads)
            _log_history(lead, f"status→{new_status}")
            return {"ok": True, "lead": lead}

    return {"ok": False, "error": "Lead not found"}


def get_agent_status() -> dict:
    """Return agent health status."""
    leads = _load_leads()
    by_status = {}
    for lead in leads:
        s = lead.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1

    return {
        "agent": "lead_gen",
        "version": "1.0.0",
        "poll_interval": POLL_INTERVAL_SECONDS,
        "min_po_value": MIN_PO_VALUE,
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "total_leads": len(leads),
        "leads_by_status": by_status,
        "api_key_set": bool(ANTHROPIC_API_KEY),
        "known_institutions": len(KNOWN_INSTITUTIONS),
    }


def get_lead_analytics() -> dict:
    """Return lead conversion analytics."""
    try:
        with open(LEAD_HISTORY_FILE) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    leads = _load_leads()
    total = len(leads)
    won = sum(1 for l in leads if l.get("status") == "won")
    lost = sum(1 for l in leads if l.get("status") == "lost")
    contacted = sum(1 for l in leads if l.get("status") == "contacted")

    return {
        "total_leads": total,
        "won": won,
        "lost": lost,
        "contacted": contacted,
        "conversion_rate": round(won / max(total, 1) * 100, 1),
        "avg_score": round(sum(l.get("score", 0) for l in leads) / max(total, 1), 2),
        "history_entries": len(history),
    }
