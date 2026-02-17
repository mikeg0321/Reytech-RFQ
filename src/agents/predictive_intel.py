"""
predictive_intel.py — Predictive Win Scoring + Competitor Intelligence
Phase 19 | Version: 1.0.0

1. Win Prediction: Uses historical win/loss patterns to predict probability
   of winning a new opportunity at a specific institution/agency.
   
2. Competitor Intelligence: When quotes are lost or expire, analyzes
   patterns and stores pricing intel for future bidding strategy.

3. Shipping Monitor: Parses incoming emails for tracking numbers,
   shipping confirmations, and delivery notifications.
"""

import os
import json
import re
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger("predictive_intel")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

INTEL_FILE = os.path.join(DATA_DIR, "competitor_intel.json")


def _load(filename: str):
    try:
        with open(os.path.join(DATA_DIR, filename)) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return [] if "log" in filename else {}


def _load_intel() -> list:
    try:
        with open(INTEL_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_intel(intel: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    if len(intel) > 2000:
        intel = intel[-2000:]
    with open(INTEL_FILE, "w") as f:
        json.dump(intel, f, indent=2, default=str)


# ═══════════════════════════════════════════════════════════════════════
# 1. Predictive Win Scoring
# ═══════════════════════════════════════════════════════════════════════

def predict_win_probability(institution: str = "", agency: str = "",
                             category: str = "", po_value: float = 0) -> dict:
    """
    Predict probability of winning a bid based on historical patterns.
    
    Returns:
        {probability: 0-1, confidence: low/med/high, factors: {...}, recommendation: str}
    """
    quotes = _load("quotes_log.json")
    if not quotes:
        return {"probability": 0.5, "confidence": "low",
                "factors": {}, "recommendation": "No historical data — submit competitive pricing"}

    inst_lower = (institution or "").lower()
    agency_lower = (agency or "").lower()

    # ── Gather relevant history ──
    inst_quotes = []
    agency_quotes = []
    all_decided = []

    for q in quotes:
        status = q.get("status", "")
        if status in ("won", "lost"):
            all_decided.append(q)
            q_inst = q.get("institution", "").lower()
            q_agency = q.get("agency", "").lower()
            if inst_lower and inst_lower in q_inst:
                inst_quotes.append(q)
            if agency_lower and agency_lower in q_agency:
                agency_quotes.append(q)

    factors = {}
    score = 0.5  # base

    # ── Institution-specific win rate ──
    if inst_quotes:
        inst_won = sum(1 for q in inst_quotes if q["status"] == "won")
        inst_total = len(inst_quotes)
        inst_rate = inst_won / inst_total
        factors["institution_win_rate"] = {
            "rate": round(inst_rate, 2),
            "won": inst_won, "total": inst_total,
            "impact": round((inst_rate - 0.5) * 0.4, 3),
        }
        score += (inst_rate - 0.5) * 0.4  # strong signal

    # ── Agency-level win rate ──
    if agency_quotes and not inst_quotes:
        ag_won = sum(1 for q in agency_quotes if q["status"] == "won")
        ag_total = len(agency_quotes)
        ag_rate = ag_won / ag_total
        factors["agency_win_rate"] = {
            "rate": round(ag_rate, 2),
            "won": ag_won, "total": ag_total,
            "impact": round((ag_rate - 0.5) * 0.25, 3),
        }
        score += (ag_rate - 0.5) * 0.25

    # ── Overall win rate ──
    if all_decided:
        total_won = sum(1 for q in all_decided if q["status"] == "won")
        overall_rate = total_won / len(all_decided)
        factors["overall_win_rate"] = {
            "rate": round(overall_rate, 2),
            "won": total_won, "total": len(all_decided),
        }
        if not inst_quotes and not agency_quotes:
            score += (overall_rate - 0.5) * 0.15

    # ── Value sweet spot ──
    if po_value > 0 and all_decided:
        # Find win rate by value range
        similar_value = [q for q in all_decided
                         if 0.5 * po_value <= q.get("total", 0) <= 2 * po_value]
        if similar_value:
            sv_won = sum(1 for q in similar_value if q["status"] == "won")
            sv_rate = sv_won / len(similar_value)
            factors["value_range_rate"] = {
                "rate": round(sv_rate, 2),
                "range": f"${po_value*0.5:,.0f}-${po_value*2:,.0f}",
                "sample": len(similar_value),
            }
            score += (sv_rate - 0.5) * 0.1

    # ── Recent momentum ──
    recent = sorted(all_decided, key=lambda q: q.get("created_at", ""), reverse=True)[:10]
    if len(recent) >= 3:
        recent_won = sum(1 for q in recent if q["status"] == "won")
        recent_rate = recent_won / len(recent)
        factors["recent_momentum"] = {
            "rate": round(recent_rate, 2),
            "last_n": len(recent),
        }
        score += (recent_rate - 0.5) * 0.1

    # Clamp
    probability = max(0.05, min(0.95, score))

    # Confidence based on sample size
    sample_size = len(inst_quotes) + len(agency_quotes)
    if sample_size >= 10:
        confidence = "high"
    elif sample_size >= 3:
        confidence = "medium"
    else:
        confidence = "low"

    # Recommendation
    if probability >= 0.7:
        recommendation = "Strong fit — prioritize this bid, price competitively"
    elif probability >= 0.5:
        recommendation = "Good chance — submit with standard margins"
    elif probability >= 0.3:
        recommendation = "Competitive — consider aggressive pricing to break in"
    else:
        recommendation = "Tough win — only pursue if strategic value beyond this bid"

    return {
        "probability": round(probability, 3),
        "confidence": confidence,
        "factors": factors,
        "recommendation": recommendation,
        "sample_size": sample_size,
    }


# ═══════════════════════════════════════════════════════════════════════
# 2. Competitor Intelligence
# ═══════════════════════════════════════════════════════════════════════

def log_competitor_intel(quote_number: str, event: str, data: dict = None):
    """Log competitor intelligence when we lose or expire a quote."""
    intel = _load_intel()
    data = data or {}

    # Pull quote details
    quotes = _load("quotes_log.json")
    qt = next((q for q in quotes if q.get("quote_number") == quote_number), None)

    entry = {
        "id": f"ci-{datetime.now().strftime('%Y%m%d%H%M%S')}-{len(intel)}",
        "quote_number": quote_number,
        "event": event,  # lost, expired, undercut, competitor_found
        "timestamp": datetime.now().isoformat(),
        "institution": qt.get("institution", "") if qt else data.get("institution", ""),
        "agency": qt.get("agency", "") if qt else data.get("agency", ""),
        "our_total": qt.get("total", 0) if qt else 0,
        "items_count": qt.get("items_count", 0) if qt else 0,
        "competitor": data.get("competitor", ""),
        "competitor_price": data.get("competitor_price", 0),
        "price_difference": data.get("price_difference", 0),
        "notes": data.get("notes", ""),
    }

    # Analyze: what items did we lose on?
    if qt and qt.get("items_detail"):
        entry["lost_items"] = [
            {"description": it.get("description", "")[:80],
             "our_price": it.get("unit_price", 0),
             "qty": it.get("qty", 0)}
            for it in qt.get("items_detail", [])[:10]
        ]

    intel.append(entry)
    _save_intel(intel)
    log.info("Competitor intel logged: %s %s for %s", event, quote_number, entry["institution"])
    return entry


def get_competitor_insights(institution: str = "", agency: str = "",
                             limit: int = 20) -> dict:
    """
    Get competitor intelligence summary.
    Returns patterns in lost/expired quotes.
    """
    intel = _load_intel()
    if not intel:
        return {"total_events": 0, "insights": [], "patterns": {}}

    # Filter
    filtered = intel
    if institution:
        inst_lower = institution.lower()
        filtered = [i for i in intel if inst_lower in i.get("institution", "").lower()]
    if agency:
        ag_lower = agency.lower()
        filtered = [i for i in intel if ag_lower in i.get("agency", "").lower()]

    filtered = sorted(filtered, key=lambda i: i.get("timestamp", ""), reverse=True)[:limit]

    # Analyze patterns
    total_lost_value = sum(i.get("our_total", 0) for i in filtered if i.get("event") == "lost")
    total_expired_value = sum(i.get("our_total", 0) for i in filtered if i.get("event") == "expired")

    # Most lost institutions
    inst_losses = {}
    for i in filtered:
        inst = i.get("institution", "Unknown")
        inst_losses[inst] = inst_losses.get(inst, 0) + 1

    return {
        "total_events": len(filtered),
        "lost_count": sum(1 for i in filtered if i.get("event") == "lost"),
        "expired_count": sum(1 for i in filtered if i.get("event") == "expired"),
        "total_lost_value": total_lost_value,
        "total_expired_value": total_expired_value,
        "top_loss_institutions": sorted(inst_losses.items(), key=lambda x: x[1], reverse=True)[:5],
        "recent": filtered[:10],
    }


# ═══════════════════════════════════════════════════════════════════════
# 3. Shipping / Tracking Email Monitor
# ═══════════════════════════════════════════════════════════════════════

# Common tracking number patterns
TRACKING_PATTERNS = {
    "ups": re.compile(r"\b1Z[A-Z0-9]{16}\b"),
    "fedex": re.compile(r"\b\d{12,22}\b"),
    "usps": re.compile(r"\b(9[0-9]{15,21}|[A-Z]{2}\d{9}US)\b"),
    "amazon": re.compile(r"\bTBA\d{12,}\b"),
}

SHIPPING_KEYWORDS = [
    "shipped", "tracking number", "track your", "out for delivery",
    "delivery confirmation", "has been delivered", "shipment notification",
    "carrier:", "shipped via", "estimated delivery", "order shipped",
    "in transit", "package has shipped", "tracking info",
    "your order has been shipped", "shipping confirmation",
]

ORDER_REF_PATTERN = re.compile(r"(?:order|PO|quote|ORD)[#:\s-]*([A-Z0-9-]+)", re.IGNORECASE)


def detect_shipping_email(subject: str, body: str, sender: str = "") -> dict:
    """
    Analyze an email for shipping/tracking information.
    
    Returns:
        {"is_shipping": bool, "tracking_numbers": [...], "carrier": str,
         "order_ref": str, "delivery_status": str}
    """
    text = f"{subject} {body}".lower()
    subject_lower = subject.lower()

    # Check for shipping keywords
    keyword_hits = sum(1 for kw in SHIPPING_KEYWORDS if kw in text)
    is_shipping = keyword_hits >= 2 or any(kw in subject_lower for kw in
                                            ["shipped", "tracking", "delivery", "in transit"])

    if not is_shipping:
        return {"is_shipping": False}

    # Extract tracking numbers
    full_text = f"{subject} {body}"
    tracking_numbers = []
    carrier = ""

    for carrier_name, pattern in TRACKING_PATTERNS.items():
        matches = pattern.findall(full_text)
        if matches:
            tracking_numbers.extend(matches)
            carrier = carrier_name

    # Extract order reference
    order_ref = ""
    ref_match = ORDER_REF_PATTERN.search(full_text)
    if ref_match:
        order_ref = ref_match.group(1)

    # Determine delivery status
    if "delivered" in text:
        delivery_status = "delivered"
    elif "out for delivery" in text:
        delivery_status = "out_for_delivery"
    elif "in transit" in text:
        delivery_status = "in_transit"
    elif "shipped" in text:
        delivery_status = "shipped"
    else:
        delivery_status = "unknown"

    return {
        "is_shipping": True,
        "tracking_numbers": list(set(tracking_numbers))[:5],
        "carrier": carrier,
        "order_ref": order_ref,
        "delivery_status": delivery_status,
        "keyword_hits": keyword_hits,
        "sender": sender,
    }


def match_tracking_to_order(tracking_info: dict, orders: dict) -> Optional[str]:
    """
    Try to match a shipping email to an existing order.
    Returns order_id if matched, None otherwise.
    """
    order_ref = tracking_info.get("order_ref", "")

    # Direct match on order ID or PO number
    if order_ref:
        ref_lower = order_ref.lower()
        for oid, order in orders.items():
            if ref_lower in oid.lower():
                return oid
            if ref_lower in order.get("po_number", "").lower():
                return oid
            if ref_lower in order.get("quote_number", "").lower():
                return oid

    # Try matching by sender domain to institution
    # (e.g., amazon.com emails for Amazon orders)
    sender = tracking_info.get("sender", "").lower()
    if "amazon" in sender:
        # Find orders with Amazon supplier links that are still pending/ordered
        for oid, order in orders.items():
            if order.get("status") in ("new", "sourcing", "shipped"):
                has_amazon = any("amazon" in it.get("supplier", "").lower()
                                 for it in order.get("line_items", []))
                if has_amazon:
                    return oid

    return None


def update_order_from_tracking(order_id: str, tracking_info: dict,
                                orders: dict) -> dict:
    """
    Update an order's line items with tracking information.
    Returns updated order.
    """
    order = orders.get(order_id)
    if not order:
        return {}

    tracking = tracking_info.get("tracking_numbers", [])
    carrier = tracking_info.get("carrier", "")
    status = tracking_info.get("delivery_status", "shipped")

    # Map delivery_status to sourcing_status
    sourcing_map = {
        "shipped": "shipped",
        "in_transit": "shipped",
        "out_for_delivery": "shipped",
        "delivered": "delivered",
    }
    new_sourcing = sourcing_map.get(status, "shipped")

    updated_count = 0
    for it in order.get("line_items", []):
        if it.get("sourcing_status") in ("pending", "ordered", "shipped"):
            if not it.get("tracking_number") or new_sourcing == "delivered":
                if tracking:
                    it["tracking_number"] = tracking[0]
                it["carrier"] = carrier
                it["sourcing_status"] = new_sourcing
                if new_sourcing == "shipped" and not it.get("ship_date"):
                    it["ship_date"] = datetime.now().strftime("%Y-%m-%d")
                if new_sourcing == "delivered":
                    it["delivery_date"] = datetime.now().strftime("%Y-%m-%d")
                updated_count += 1

    order["updated_at"] = datetime.now().isoformat()
    return {"order_id": order_id, "updated_items": updated_count,
            "tracking": tracking, "carrier": carrier, "status": new_sourcing}
