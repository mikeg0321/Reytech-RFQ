"""
Growth Discovery Engine
========================
First-principles approach to revenue growth for a DVBE/SB/DBE government reseller.

THE MATH:
  California state agencies spend ~$20B+/year on goods and services.
  3% DVBE mandate = ~$600M+ in mandated DVBE spend.
  Reytech currently sells $5K/year from ONE agency (CDCR).
  839 products across medical, office, janitorial, food service, safety.
  DBE certified with every DOT but not leveraging it.

THIS ENGINE:
  1. DISCOVER — Find agencies NOT working with that buy products we sell
  2. QUANTIFY — Calculate addressable spend per agency, DVBE mandate amounts
  3. IDENTIFY — Find the actual buyers (names, emails) at those agencies
  4. ANALYZE — Why we lose, how to win (price gaps, vehicle gaps)
  5. GUIDE — Which contract vehicles to pursue, when they open
  6. EXPAND — DBE/DOT opportunities beyond state agencies
"""

import os
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("growth_discovery")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import DB_PATH
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    DB_PATH = os.path.join(DATA_DIR, "reytech.db")


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# California State Department Registry — ALL departments, not just current ones
# ═══════════════════════════════════════════════════════════════════════════════

CA_DEPARTMENTS = {
    # Healthcare / Social Services — HIGH PRIORITY (medical supplies)
    "5225": {"code": "CCHCS", "name": "Correctional Health Care Services", "segment": "healthcare", "current_customer": True},
    "4700": {"code": "CDCR", "name": "Dept of Corrections & Rehabilitation", "segment": "corrections", "current_customer": True},
    "4440": {"code": "DSH", "name": "Dept of State Hospitals", "segment": "healthcare", "current_customer": False},
    "4260": {"code": "CDPH", "name": "Dept of Public Health", "segment": "healthcare", "current_customer": False},
    "4300": {"code": "DHCS", "name": "Dept of Health Care Services", "segment": "healthcare", "current_customer": False},
    "5180": {"code": "DSS", "name": "Dept of Social Services", "segment": "social", "current_customer": False},
    "4120": {"code": "EMSA", "name": "Emergency Medical Services Authority", "segment": "healthcare", "current_customer": False},
    "7700": {"code": "CalVet", "name": "Dept of Veterans Affairs", "segment": "healthcare", "current_customer": False},
    "5160": {"code": "DOR", "name": "Dept of Rehabilitation", "segment": "social", "current_customer": False},
    "4100": {"code": "SCDD", "name": "State Council on Developmental Disabilities", "segment": "social", "current_customer": False},

    # Education — HUGE (UC/CSU buy medical, safety, janitorial, office)
    "6120": {"code": "CSU", "name": "California State University", "segment": "education", "current_customer": False},
    "6440": {"code": "UC", "name": "University of California", "segment": "education", "current_customer": False},
    "6100": {"code": "DOE", "name": "Dept of Education", "segment": "education", "current_customer": False},
    "6360": {"code": "CCC", "name": "Community Colleges Chancellor's Office", "segment": "education", "current_customer": False},

    # Public Safety — (PPE, first aid, safety equipment)
    "2720": {"code": "CHP", "name": "California Highway Patrol", "segment": "public_safety", "current_customer": False},
    "0690": {"code": "CalOES", "name": "Office of Emergency Services", "segment": "public_safety", "current_customer": False},
    "5227": {"code": "BSCC", "name": "Board of State & Community Corrections", "segment": "corrections", "current_customer": False},

    # Transportation — DBE OPPORTUNITY
    "2660": {"code": "CalTrans", "name": "Dept of Transportation", "segment": "transportation", "current_customer": False,
             "dbe_opportunity": True, "notes": "~$15B annual budget. DBE mandate applies to federally-funded projects."},
    "2740": {"code": "DMV", "name": "Dept of Motor Vehicles", "segment": "transportation", "current_customer": False},

    # Natural Resources (safety gear, PPE, equipment)
    "3540": {"code": "CalFire", "name": "Dept of Forestry and Fire Protection", "segment": "public_safety", "current_customer": False},
    "3600": {"code": "DFW", "name": "Dept of Fish and Wildlife", "segment": "natural_resources", "current_customer": False},
    "3790": {"code": "DPR", "name": "Dept of Parks and Recreation", "segment": "natural_resources", "current_customer": False},
    "3480": {"code": "DCA", "name": "Dept of Conservation", "segment": "natural_resources", "current_customer": False},
    "3940": {"code": "SWRCB", "name": "State Water Resources Control Board", "segment": "natural_resources", "current_customer": False},

    # General Government (office supplies, janitorial)
    "1760": {"code": "DGS", "name": "Dept of General Services", "segment": "general", "current_customer": False},
    "7100": {"code": "EDD", "name": "Employment Development Department", "segment": "general", "current_customer": False},
    "7350": {"code": "DIR", "name": "Dept of Industrial Relations", "segment": "general", "current_customer": False},
    "0950": {"code": "SCO", "name": "State Controller's Office", "segment": "general", "current_customer": False},
    "7501": {"code": "CalHR", "name": "CA Human Resources", "segment": "general", "current_customer": False},

    # Environmental
    "3900": {"code": "CARB", "name": "CA Air Resources Board", "segment": "environmental", "current_customer": False},
    "3930": {"code": "DTSC", "name": "Dept of Toxic Substances Control", "segment": "environmental", "current_customer": False},

    # Agriculture
    "8570": {"code": "CDFA", "name": "Dept of Food and Agriculture", "segment": "agriculture", "current_customer": False},

    # Military
    "8940": {"code": "CSMR", "name": "CA Military Dept / National Guard", "segment": "military", "current_customer": False},
}


# What Reytech can sell to each segment
SEGMENT_PRODUCTS = {
    "healthcare": {
        "categories": ["Medical/Clinical", "Gloves", "Safety/PPE", "Personal Care",
                        "Cleaning/Sanitation", "Food Service", "General"],
        "pitch": "Medical supplies, PPE, infection control, patient care, and facility maintenance",
        "dvbe_angle": "Healthcare facilities typically fall short on DVBE spend — medical supply sourcing through DVBE vendors helps meet the mandate",
    },
    "corrections": {
        "categories": ["Medical/Clinical", "Gloves", "Personal Care", "Food Service",
                        "Cleaning/Sanitation", "General", "Safety/PPE"],
        "pitch": "Institutional supplies for correctional facilities — medical, food service, personal care, janitorial",
        "dvbe_angle": "CDCR is one of the largest state spenders — consistent DVBE shortfall creates ongoing opportunity",
    },
    "education": {
        "categories": ["Office Supplies", "Toner/Printer", "Cleaning/Sanitation",
                        "Safety/PPE", "Furniture", "General", "Batteries"],
        "pitch": "Campus supplies — office, janitorial, safety equipment, classroom furnishings",
        "dvbe_angle": "CSU/UC systems have independent procurement with DVBE goals — less competitive than state agencies",
    },
    "public_safety": {
        "categories": ["Safety/PPE", "Medical/Clinical", "Gloves", "General", "Batteries"],
        "pitch": "First responder supplies — PPE, first aid, tactical equipment, batteries",
        "dvbe_angle": "Emergency services procurement is often expedited — DVBE vendors get preference in urgent buys",
    },
    "transportation": {
        "categories": ["Safety/PPE", "Office Supplies", "Cleaning/Sanitation", "General"],
        "pitch": "DOT office and field supplies — safety gear, office products, janitorial",
        "dvbe_angle": "CalTrans has BOTH DVBE (state) and DBE (federal) mandates. Reytech holds both certifications — double advantage",
        "dbe_note": "Federal DBE mandate on federally-funded projects is separate from state DVBE. You're certified for both.",
    },
    "social": {
        "categories": ["Office Supplies", "General", "Cleaning/Sanitation", "Personal Care"],
        "pitch": "Office and facility supplies for social services operations",
        "dvbe_angle": "Social service agencies often have smaller procurement teams — easier to establish vendor relationships",
    },
    "general": {
        "categories": ["Office Supplies", "Toner/Printer", "Paper/Towels", "Cleaning/Sanitation"],
        "pitch": "Standard office supplies, janitorial, and facility maintenance",
        "dvbe_angle": "General government offices often buy from the same few vendors — DVBE certification opens the door",
    },
    "natural_resources": {
        "categories": ["Safety/PPE", "Cleaning/Sanitation", "General", "Gloves", "Batteries"],
        "pitch": "Field supplies — PPE, safety equipment, janitorial for remote facilities",
        "dvbe_angle": "Parks, wildlife, and conservation have field operations that need regular supply replenishment",
    },
    "environmental": {
        "categories": ["Safety/PPE", "Gloves", "Cleaning/Sanitation", "General"],
        "pitch": "Lab and field safety supplies — PPE, chemical-resistant gloves, cleanup materials",
        "dvbe_angle": "Environmental agencies handle hazardous materials — specialized PPE sourcing opportunity",
    },
    "agriculture": {
        "categories": ["Safety/PPE", "Gloves", "General", "Cleaning/Sanitation"],
        "pitch": "Agricultural safety and facility supplies",
        "dvbe_angle": "CDFA has inspection and lab operations that need PPE and safety supplies",
    },
    "military": {
        "categories": ["Safety/PPE", "Medical/Clinical", "General", "Batteries", "Gloves"],
        "pitch": "Military and National Guard facility supplies, medical, PPE",
        "dvbe_angle": "Military procurement has both state DVBE and federal small business set-asides",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Core Discovery: Analyze SCPRS data to find untapped agencies
# ═══════════════════════════════════════════════════════════════════════════════

def discover_new_agencies(min_spend: float = 10000) -> dict:
    """
    First principles growth discovery:
    1. Look at ALL agencies in SCPRS data (not just our 8)
    2. Find ones buying products we sell
    3. Calculate DVBE mandate addressable spend
    4. Rank by opportunity size
    5. Identify the buyers at each agency
    """
    conn = _db()
    now = datetime.now(timezone.utc).isoformat()

    # Get our product categories for matching
    our_categories = set()
    catalog_items = conn.execute("""
        SELECT DISTINCT category FROM product_catalog
        WHERE category IS NOT NULL AND category != ''
    """).fetchall()
    for c in catalog_items:
        our_categories.add(c["category"])

    # Get our catalog descriptions for fuzzy matching
    catalog_keywords = set()
    for row in conn.execute("""
        SELECT search_tokens FROM product_catalog
        WHERE search_tokens IS NOT NULL AND search_tokens != ''
    """).fetchall():
        for token in (row["search_tokens"] or "").split():
            if len(token) > 3:
                catalog_keywords.add(token)

    # ── Find ALL departments in SCPRS data (discovered agencies) ──
    all_depts = conn.execute("""
        SELECT dept_code, dept_name, agency_key,
               COUNT(DISTINCT po_number) as po_count,
               SUM(grand_total) as total_spend,
               COUNT(DISTINCT supplier) as supplier_count,
               COUNT(DISTINCT buyer_email) as buyer_count,
               MIN(start_date) as first_po,
               MAX(start_date) as last_po,
               GROUP_CONCAT(DISTINCT acq_type) as vehicles
        FROM scprs_po_master
        WHERE dept_name IS NOT NULL AND dept_name != ''
        GROUP BY dept_code
        HAVING total_spend >= ?
        ORDER BY total_spend DESC
    """, (min_spend,)).fetchall()

    # ── Current Reytech agencies ──
    current_agencies = set()
    for qr in conn.execute("SELECT DISTINCT UPPER(agency) FROM quotes WHERE is_test=0").fetchall():
        current_agencies.add(qr[0])
    # Also from registry
    from src.agents.scprs_intelligence_engine import AGENCY_REGISTRY
    for k in AGENCY_REGISTRY:
        current_agencies.add(k.upper())
        reg = AGENCY_REGISTRY[k]
        for dc in reg.get("dept_codes", []):
            current_agencies.add(dc)

    # ── Analyze each department ──
    opportunities = []

    for dept in all_depts:
        d = dict(dept)
        dept_code = d.get("dept_code", "")
        dept_name = d.get("dept_name", "")
        agency_key = d.get("agency_key", "")
        total_spend = d.get("total_spend", 0) or 0

        # Is this a current customer?
        is_current = (dept_code in current_agencies or
                      (agency_key or "").upper() in current_agencies or
                      any(k.upper() in dept_name.upper() for k in current_agencies if len(k) > 2))

        # Calculate DVBE mandate amount (3% of total spend)
        dvbe_mandate = round(total_spend * 0.03, 2)

        # Find matching products — what items does this dept buy that we also sell?
        dept_items = conn.execute("""
            SELECT l.description, l.category, l.unit_price, l.quantity,
                   SUM(l.line_total) as item_spend,
                   COUNT(DISTINCT p.po_number) as frequency,
                   GROUP_CONCAT(DISTINCT p.supplier) as suppliers
            FROM scprs_po_lines l
            JOIN scprs_po_master p ON l.po_id = p.id
            WHERE p.dept_code = ?
            GROUP BY LOWER(SUBSTR(l.description, 1, 40))
            ORDER BY item_spend DESC
            LIMIT 30
        """, (dept_code,)).fetchall()

        # Score product overlap
        overlap_items = []
        overlap_spend = 0
        for item in dept_items:
            desc = (item["description"] or "").lower()
            desc_tokens = set(desc.split())
            # Check if we sell something similar
            match_score = len(desc_tokens & catalog_keywords) / max(len(desc_tokens), 1)
            category_match = item["category"] in our_categories if item["category"] else False

            if match_score >= 0.2 or category_match:
                overlap_items.append({
                    "description": item["description"][:60],
                    "unit_price": item["unit_price"],
                    "total_spend": item["item_spend"],
                    "frequency": item["frequency"],
                    "suppliers": item["suppliers"],
                    "match_score": round(match_score, 2),
                })
                overlap_spend += item["item_spend"] or 0

        # Get buyers at this department
        buyers = conn.execute("""
            SELECT buyer_name, buyer_email, buyer_phone,
                   COUNT(DISTINCT po_number) as po_count,
                   SUM(grand_total) as total_spend
            FROM scprs_po_master
            WHERE dept_code = ?
              AND buyer_email IS NOT NULL AND buyer_email != ''
            GROUP BY LOWER(buyer_email)
            ORDER BY total_spend DESC
            LIMIT 10
        """, (dept_code,)).fetchall()

        # Look up segment info
        dept_info = CA_DEPARTMENTS.get(dept_code, {})
        segment = dept_info.get("segment", "general")
        segment_info = SEGMENT_PRODUCTS.get(segment, {})

        # Calculate opportunity score
        opp_score = 0
        if overlap_spend > 0: opp_score += min(overlap_spend / 10000, 40)  # Product overlap
        if dvbe_mandate > 5000: opp_score += min(dvbe_mandate / 5000, 25)  # DVBE size
        if len(buyers) > 0: opp_score += min(len(buyers) * 5, 15)  # Buyer access
        if not is_current: opp_score += 10  # New agency bonus
        if dept_info.get("dbe_opportunity"): opp_score += 10  # DBE bonus
        opp_score = min(round(opp_score), 100)

        opportunities.append({
            "dept_code": dept_code,
            "dept_name": dept_name,
            "agency_key": agency_key or dept_info.get("code", ""),
            "segment": segment,
            "is_current_customer": is_current,
            "total_spend": total_spend,
            "dvbe_mandate_3pct": dvbe_mandate,
            "po_count": d["po_count"],
            "supplier_count": d["supplier_count"],
            "buyer_count": d["buyer_count"],
            "vehicles": d.get("vehicles", ""),
            "first_po": d.get("first_po", ""),
            "last_po": d.get("last_po", ""),
            "overlap_items": overlap_items[:10],
            "overlap_spend": round(overlap_spend, 2),
            "overlap_pct": round(overlap_spend / total_spend * 100, 1) if total_spend > 0 else 0,
            "buyers": [dict(b) for b in buyers],
            "opportunity_score": opp_score,
            "segment_pitch": segment_info.get("pitch", ""),
            "dvbe_angle": segment_info.get("dvbe_angle", ""),
            "dbe_note": segment_info.get("dbe_note", ""),
            "dbe_opportunity": dept_info.get("dbe_opportunity", False),
        })

    # Sort by opportunity score
    opportunities.sort(key=lambda x: (-x["opportunity_score"], -x["total_spend"]))

    # ── Summary stats ──
    new_agencies = [o for o in opportunities if not o["is_current_customer"]]
    total_addressable = sum(o["dvbe_mandate_3pct"] for o in new_agencies)
    total_overlap = sum(o["overlap_spend"] for o in new_agencies)
    total_buyers = sum(o["buyer_count"] for o in new_agencies)

    conn.close()

    return {
        "ok": True,
        "summary": {
            "total_agencies_found": len(opportunities),
            "new_agencies": len(new_agencies),
            "current_agencies": len(opportunities) - len(new_agencies),
            "total_addressable_dvbe": round(total_addressable, 2),
            "total_product_overlap": round(total_overlap, 2),
            "total_new_buyers": total_buyers,
        },
        "opportunities": opportunities,
        "segments": {seg: {
            "count": sum(1 for o in opportunities if o["segment"] == seg),
            "spend": sum(o["total_spend"] for o in opportunities if o["segment"] == seg),
            **info,
        } for seg, info in SEGMENT_PRODUCTS.items()},
        "generated_at": now,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Loss Intelligence: Why we lose and how to win
# ═══════════════════════════════════════════════════════════════════════════════

def get_loss_intelligence() -> dict:
    """
    Analyze all losses: price gaps, competitor patterns, product gaps.
    Returns actionable intelligence on how to win more.
    """
    conn = _db()

    # Get all loss data from award tracker
    losses = conn.execute("""
        SELECT m.quote_number, m.scprs_supplier as winner, m.scprs_total as winner_price,
               m.our_total, m.match_confidence, m.loss_report, m.line_analysis,
               q.agency, q.institution, q.items_text
        FROM quote_po_matches m
        LEFT JOIN quotes q ON q.quote_number = m.quote_number
        WHERE m.outcome = 'lost_to_competitor'
        ORDER BY m.matched_at DESC
    """).fetchall()

    # Aggregate by competitor
    by_competitor = {}
    for l in losses:
        winner = l["winner"] or "Unknown"
        if winner not in by_competitor:
            by_competitor[winner] = {"count": 0, "total_lost": 0, "avg_delta": 0, "quotes": []}
        by_competitor[winner]["count"] += 1
        by_competitor[winner]["total_lost"] += l["our_total"] or 0
        delta = (l["our_total"] or 0) - (l["winner_price"] or 0)
        by_competitor[winner]["avg_delta"] += delta
        by_competitor[winner]["quotes"].append({
            "quote": l["quote_number"],
            "agency": l["agency"],
            "our_price": l["our_total"],
            "winner_price": l["winner_price"],
        })

    for comp in by_competitor.values():
        if comp["count"] > 0:
            comp["avg_delta"] = round(comp["avg_delta"] / comp["count"], 2)

    # Price gap analysis from SCPRS
    price_gaps = conn.execute("""
        SELECT l.description, l.unit_price as market_price,
               pc.sell_price as our_price, pc.cost as our_cost,
               pc.name as catalog_name
        FROM scprs_po_lines l
        JOIN product_catalog pc ON LOWER(pc.search_tokens) LIKE '%' || LOWER(SUBSTR(l.description, 1, 15)) || '%'
        WHERE l.unit_price > 0 AND pc.sell_price > 0
          AND l.unit_price < pc.sell_price
        ORDER BY (pc.sell_price - l.unit_price) DESC
        LIMIT 20
    """).fetchall()

    conn.close()

    # Generate recommendations
    recommendations = []
    if losses:
        avg_loss_delta = sum((l["our_total"] or 0) - (l["winner_price"] or 0) for l in losses) / len(losses)
        if avg_loss_delta > 0:
            recommendations.append({
                "priority": "HIGH",
                "action": f"Reduce pricing — we're ${avg_loss_delta:,.0f} higher than winners on average",
                "detail": "Review cost basis and margin targets. Consider volume discounts or catalog-level repricing.",
            })
        top_competitor = max(by_competitor.items(), key=lambda x: x[1]["count"]) if by_competitor else None
        if top_competitor:
            recommendations.append({
                "priority": "HIGH",
                "action": f"Study {top_competitor[0]} — they've beaten us {top_competitor[1]['count']} times",
                "detail": "Research their pricing, contract vehicles, and delivery terms. Consider DVBE displacement angle.",
            })

    if price_gaps:
        recommendations.append({
            "priority": "MEDIUM",
            "action": f"{len(price_gaps)} catalog items are priced above market",
            "detail": "SCPRS shows competitors winning at lower prices. Adjust catalog sell prices.",
        })

    return {
        "ok": True,
        "total_losses": len(losses),
        "by_competitor": by_competitor,
        "price_gaps": [dict(g) for g in price_gaps],
        "recommendations": recommendations,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# DVBE/DBE Budget Calculator
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_dvbe_opportunity() -> dict:
    """
    Work backwards from the 3% DVBE mandate:
    - Total spend by agency from SCPRS
    - 3% = mandated DVBE spend
    - Current DVBE spend (estimated from known DVBE suppliers)
    - Shortfall = your opportunity
    """
    conn = _db()

    # Known DVBE suppliers (simplified — ideally from DGS DVBE directory)
    from src.agents.scprs_intelligence_engine import KNOWN_NON_DVBE_INCUMBENTS

    agencies = conn.execute("""
        SELECT agency_key, dept_name,
               SUM(grand_total) as total_spend,
               COUNT(DISTINCT po_number) as po_count,
               COUNT(DISTINCT supplier) as supplier_count
        FROM scprs_po_master
        WHERE agency_key IS NOT NULL AND agency_key != ''
        GROUP BY agency_key
        ORDER BY total_spend DESC
    """).fetchall()

    results = []
    for a in agencies:
        agency = dict(a)
        total = agency["total_spend"] or 0

        # Estimate non-DVBE spend (from known incumbents)
        non_dvbe = conn.execute("""
            SELECT SUM(grand_total) as spend
            FROM scprs_po_master
            WHERE agency_key = ? AND LOWER(supplier) IN ({})
        """.format(",".join("?" for _ in KNOWN_NON_DVBE_INCUMBENTS)),
            (agency["agency_key"], *[s.lower() for s in KNOWN_NON_DVBE_INCUMBENTS])
        ).fetchone()

        non_dvbe_spend = (non_dvbe["spend"] or 0) if non_dvbe else 0
        dvbe_mandate = round(total * 0.03, 2)
        estimated_dvbe = total - non_dvbe_spend  # Very rough estimate
        shortfall = max(0, dvbe_mandate - estimated_dvbe * 0.5)  # Conservative

        results.append({
            **agency,
            "dvbe_mandate_3pct": dvbe_mandate,
            "non_dvbe_spend": round(non_dvbe_spend, 2),
            "non_dvbe_pct": round(non_dvbe_spend / total * 100, 1) if total > 0 else 0,
            "estimated_shortfall": round(shortfall, 2),
            "opportunity": round(min(dvbe_mandate, shortfall + dvbe_mandate * 0.5), 2),
        })

    total_opportunity = sum(r["opportunity"] for r in results)

    conn.close()

    return {
        "ok": True,
        "agencies": results,
        "total_dvbe_mandate": sum(r["dvbe_mandate_3pct"] for r in results),
        "total_non_dvbe_spend": sum(r["non_dvbe_spend"] for r in results),
        "total_opportunity": round(total_opportunity, 2),
        "message": (
            f"Across {len(results)} agencies, {sum(r['dvbe_mandate_3pct'] for r in results):,.0f} "
            f"in DVBE-mandated spend. Non-DVBE incumbents hold "
            f"${sum(r['non_dvbe_spend'] for r in results):,.0f} — "
            f"estimated ${total_opportunity:,.0f} addressable opportunity."
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# DBE / DOT Opportunity Identification
# ═══════════════════════════════════════════════════════════════════════════════

DBE_OPPORTUNITIES = {
    "CalTrans": {
        "name": "California Department of Transportation",
        "annual_budget": 15_000_000_000,  # ~$15B
        "dbe_pct": 0.12,  # Federal DBE goal ~12%
        "addressable": 15_000_000_000 * 0.12,  # $1.8B DBE goal
        "categories": ["Safety/PPE", "Office Supplies", "Cleaning/Sanitation",
                        "Traffic Safety", "Construction Supplies"],
        "how_to_enter": [
            "Register in CalTrans B2GNow DBE directory",
            "Respond to CalTrans Advertised Projects on dot.ca.gov",
            "Contact prime contractors bidding CalTrans projects — they need DBE subs",
            "Target District offices for office/safety supplies (non-construction)",
        ],
        "contact": "CalTrans Office of Business and Economic Opportunity: (916) 324-1700",
    },
    "BART": {
        "name": "Bay Area Rapid Transit",
        "annual_budget": 2_500_000_000,
        "dbe_pct": 0.10,
        "addressable": 250_000_000,
        "categories": ["Safety/PPE", "Cleaning/Sanitation", "Office Supplies", "General"],
        "how_to_enter": [
            "Register in BART's vendor database (bart.gov/procurement)",
            "Monitor BART bid opportunities",
            "Target facility maintenance and office supply contracts",
        ],
        "contact": "BART Office of Civil Rights: (510) 464-6100",
    },
    "LA_Metro": {
        "name": "Los Angeles Metro",
        "annual_budget": 8_000_000_000,
        "dbe_pct": 0.10,
        "addressable": 800_000_000,
        "categories": ["Safety/PPE", "Cleaning/Sanitation", "Office Supplies"],
        "how_to_enter": [
            "Register in Metro's vendor database (metro.net/business)",
            "Apply for Metro's SBE/DBE certification recognition",
            "Target non-construction procurement (supplies, services)",
        ],
        "contact": "Metro DEOD: (213) 922-2600",
    },
    "Sacramento_RT": {
        "name": "Sacramento Regional Transit",
        "annual_budget": 400_000_000,
        "dbe_pct": 0.08,
        "addressable": 32_000_000,
        "categories": ["Safety/PPE", "Cleaning/Sanitation", "Office Supplies"],
        "how_to_enter": [
            "Register as DBE vendor with SacRT",
            "Monitor bid postings on sacrt.com",
            "Local advantage — Reytech is CA-based",
        ],
        "contact": "SacRT Procurement: (916) 321-2800",
    },
}


def get_dbe_opportunities() -> dict:
    """Identify DBE/DOT opportunities Reytech isn't leveraging."""
    total_addressable = sum(v["addressable"] for v in DBE_OPPORTUNITIES.values())

    return {
        "ok": True,
        "opportunities": DBE_OPPORTUNITIES,
        "total_addressable": total_addressable,
        "certifications": {
            "DVBE": "CA Dept of General Services — active",
            "SB": "CA Small Business — active",
            "DBE": "Every state DOT — active but not leveraged",
        },
        "immediate_actions": [
            "Register in CalTrans B2GNow DBE directory if not already",
            "Contact 3 prime contractors on active CalTrans projects — offer to be DBE sub",
            "Register in BART and LA Metro vendor databases",
            "Set up alerts on BidSync/Cal eProcure for DOT procurement opportunities",
            "Target non-construction CalTrans procurement (office supplies, safety gear) — lower barrier",
        ],
        "estimated_first_year": round(total_addressable * 0.001),  # Conservative 0.1% capture
        "message": (
            f"DBE certification with every DOT opens ${total_addressable/1e9:.1f}B in addressable spend. "
            f"Even capturing 0.1% = ${total_addressable * 0.001:,.0f} in first year revenue. "
            f"CalTrans alone is ${DBE_OPPORTUNITIES['CalTrans']['addressable']/1e9:.1f}B in DBE-mandated spend."
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Contract Vehicle Advisory
# ═══════════════════════════════════════════════════════════════════════════════

CONTRACT_VEHICLES = {
    "CMAS": {
        "name": "California Multiple Award Schedule",
        "type": "Standing offer — pre-negotiated prices",
        "threshold": "No dollar limit",
        "how_it_helps": "Once on CMAS, agencies can order directly without bidding. Repeat revenue.",
        "how_to_get_on": [
            "Apply through DGS Procurement Division",
            "Submit pricing catalog for your product categories",
            "Process takes 60-90 days",
            "dgs.ca.gov/PD/About/Page-Content/PD-Branch-Introduction-702",
        ],
        "priority": "HIGH — enables recurring revenue without rebidding",
    },
    "LPA": {
        "name": "Leveraged Procurement Agreement",
        "type": "Volume discount contract",
        "threshold": "Varies by agreement",
        "how_it_helps": "Competitive pricing locks out competitors. Multi-year contracts.",
        "how_to_get_on": [
            "Respond to LPA solicitations when posted on Cal eProcure",
            "caleprocure.ca.gov — set alerts for your product categories",
        ],
        "priority": "MEDIUM — competitive but high-value if won",
    },
    "NCB": {
        "name": "Non-Competitive Bid",
        "type": "Direct purchase under threshold",
        "threshold": "Under $10,000 (often $5,000)",
        "how_it_helps": "No formal bid process. Agency buyer can purchase directly from you.",
        "how_to_get_on": [
            "Build relationships with agency buyers",
            "Be in their vendor database",
            "Competitive pricing on common items",
            "DVBE certification gives you an edge — buyers prefer DVBE for NCBs to help meet their quota",
        ],
        "priority": "HIGH — fastest path to revenue. Build buyer relationships.",
    },
    "SB_DVBE_Set_Aside": {
        "name": "Small Business / DVBE Set-Aside",
        "type": "Contracts reserved for certified businesses",
        "threshold": "Under $250,000 (SB set-aside)",
        "how_it_helps": "Only SB/DVBE firms can bid. Less competition.",
        "how_to_get_on": [
            "You already qualify — SB and DVBE certified",
            "Monitor Cal eProcure for set-aside solicitations",
            "Respond to every relevant SB/DVBE set-aside",
        ],
        "priority": "HIGH — you already qualify, just need to respond to solicitations",
    },
    "GSA_Schedule": {
        "name": "GSA Federal Supply Schedule",
        "type": "Federal government-wide contract",
        "threshold": "No dollar limit",
        "how_it_helps": "Opens federal government sales. State/local can piggyback.",
        "how_to_get_on": [
            "Apply through GSA Advantage (gsa.gov)",
            "Requires financial documentation and pricing",
            "Process takes 3-6 months",
            "Consider using a GSA schedule consultant for first application",
        ],
        "priority": "LOW — high effort but opens federal market long-term",
    },
    "Cooperative_Purchasing": {
        "name": "Cooperative Purchasing (NASPO, Sourcewell, OMNIA)",
        "type": "Multi-state purchasing cooperatives",
        "threshold": "Varies",
        "how_it_helps": "One contract = access to all member agencies across multiple states.",
        "how_to_get_on": [
            "Respond to cooperative RFPs when posted",
            "NASPO ValuePoint (naspovaluepoint.org)",
            "Sourcewell (sourcewell-mn.gov)",
            "Significant effort but massive reach if awarded",
        ],
        "priority": "LOW — future growth play, not immediate",
    },
}


def get_contract_vehicle_advisory() -> dict:
    """Advise on which contract vehicles to pursue and when."""
    conn = _db()

    # Analyze which vehicles competitors use
    vehicle_usage = conn.execute("""
        SELECT acq_type, acq_method,
               COUNT(DISTINCT po_number) as po_count,
               SUM(grand_total) as total_spend,
               COUNT(DISTINCT supplier) as supplier_count
        FROM scprs_po_master
        WHERE acq_type IS NOT NULL AND acq_type != ''
        GROUP BY acq_type ORDER BY total_spend DESC
    """).fetchall()

    conn.close()

    return {
        "ok": True,
        "vehicles": CONTRACT_VEHICLES,
        "vehicle_usage_scprs": [dict(v) for v in vehicle_usage],
        "immediate_priority": [
            CONTRACT_VEHICLES["NCB"],
            CONTRACT_VEHICLES["SB_DVBE_Set_Aside"],
            CONTRACT_VEHICLES["CMAS"],
        ],
        "recommended_sequence": [
            "1. NCB + buyer relationships (0-3 months) — fastest revenue",
            "2. Respond to SB/DVBE set-asides on Cal eProcure (ongoing)",
            "3. Apply for CMAS schedule (3-6 months) — enables repeat orders",
            "4. Register in CalTrans/transit DBE directories (1-2 months)",
            "5. Consider GSA Schedule (6-12 months) — opens federal",
        ],
    }
