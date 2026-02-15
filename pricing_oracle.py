"""
pricing_oracle.py — Dynamic Pricing Oracle for Reytech RFQ Automation
Version: 6.0 | Module: Intelligent Bid Pricing Engine

Replaces static pricing rules with a multi-factor engine that produces
tiered bid recommendations (Recommended / Aggressive / Safe) with
win-probability estimates based on the Won Quotes Knowledge Base.

Dependencies: won_quotes_db.py
"""

import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("reytech.oracle")

# Import from our Won Quotes KB
from won_quotes_db import (
    find_similar_items,
    get_price_history,
    win_probability,
    classify_category,
)

# ─── Configuration ───────────────────────────────────────────────────────────

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reytech_config.json")

# Default pricing rules (loaded from config if available)
DEFAULT_CONFIG = {
    "scprs_undercut_pct": 0.01,       # 1% below SCPRS winning bid
    "aggressive_undercut_pct": 0.03,   # 3% below for aggressive tier
    "safe_markup_pct": 0.30,           # 30% above cost for safe tier
    "default_markup_pct": 0.25,        # 25% markup when no SCPRS data
    "profit_floor_general": 100,       # Minimum $100 profit
    "profit_floor_amazon": 50,         # Minimum $50 for Amazon-sourced
    "profit_floor_aggressive": 50,     # Minimum $50 for aggressive tier
    "hard_floor_margin": 25,           # Absolute minimum: cost + $25
    "ceiling_alert_pct": 0.10,         # Flag if >10% above SCPRS median
    "stale_data_months": 18,           # Flag if best match is older
    "weights": {
        "scprs_historical": 0.60,      # 60% weight on historical wins
        "supplier_cost": 0.30,         # 30% weight on supplier cost
        "margin_goals": 0.10,          # 10% weight on margin targets
    },
}


def load_config() -> dict:
    """Load pricing config, merging file config with defaults."""
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                file_config = json.load(f)
            pricing_rules = file_config.get("pricing_rules", {})
            config.update(pricing_rules)
        except (json.JSONDecodeError, IOError):
            pass
    return config


# ─── Pricing Recommendation ─────────────────────────────────────────────────

class PricingRecommendation:
    """Container for a tiered pricing recommendation."""

    def __init__(self):
        self.recommended = None
        self.aggressive = None
        self.safe = None
        self.flags = []
        self.reasoning = ""
        self.scprs_data = None
        self.data_quality = "no_data"

    def to_dict(self) -> dict:
        return {
            "recommended": self.recommended,
            "aggressive": self.aggressive,
            "safe": self.safe,
            "flags": self.flags,
            "reasoning": self.reasoning,
            "data_quality": self.data_quality,
            "scprs_data": self.scprs_data,
        }


class PriceTier:
    """A single pricing tier."""

    def __init__(self, price: float, margin_pct: float, win_prob: float, label: str):
        self.price = round(price, 2)
        self.margin_pct = round(margin_pct, 3)
        self.win_probability = round(win_prob, 3)
        self.label = label

    def to_dict(self) -> dict:
        return {
            "price": self.price,
            "margin_pct": self.margin_pct,
            "margin_pct_display": f"{self.margin_pct * 100:.1f}%",
            "win_probability": self.win_probability,
            "win_probability_display": f"{self.win_probability * 100:.0f}%",
            "label": self.label,
        }


# ─── Core Pricing Engine ─────────────────────────────────────────────────────

def recommend_price(
    item_number: str,
    description: str,
    supplier_cost: Optional[float] = None,
    scprs_price: Optional[float] = None,
    agency: str = "CCHCS",
    source_type: str = "general",
    quantity: float = 1,
    config_overrides: Optional[dict] = None,
) -> dict:
    """
    Generate a three-tier pricing recommendation for a single line item.

    This is the primary entry point for the pricing engine.

    Args:
        item_number: State item number (e.g., "6500-001-430")
        description: Item description text
        supplier_cost: Known supplier cost (if available)
        scprs_price: Direct SCPRS price (if already looked up)
        agency: Target agency (CCHCS, CDCR, CalVet)
        source_type: Supplier type ("amazon", "medical", "industrial", "general")
        quantity: Order quantity
        config_overrides: Override specific config values

    Returns:
        PricingRecommendation as dict with recommended/aggressive/safe tiers
    """
    config = load_config()
    if config_overrides:
        config.update(config_overrides)

    log.info("Pricing %s — cost=%s scprs=%s agency=%s qty=%s",
             item_number or description[:40], supplier_cost, scprs_price, agency, quantity)

    rec = PricingRecommendation()
    reasoning_parts = []

    # ─── Step 1: Gather SCPRS intelligence ───────────────────────────────
    history = get_price_history(item_number, description, months=24)
    rec.scprs_data = {
        "matches": history["matches"],
        "median": history["median_price"],
        "min": history["min_price"],
        "max": history["max_price"],
        "recent_avg": history["recent_avg"],
        "trend": history["trend"],
    }

    # Determine best SCPRS reference price
    scprs_ref = None
    if scprs_price and scprs_price > 0:
        scprs_ref = scprs_price
        reasoning_parts.append(f"Direct SCPRS price: ${scprs_price:.2f}")
    elif history["recent_avg"] and history["matches"] >= 3:
        scprs_ref = history["recent_avg"]
        reasoning_parts.append(
            f"SCPRS recent avg: ${scprs_ref:.2f} ({history['matches']} data points)"
        )
    elif history["median_price"]:
        scprs_ref = history["median_price"]
        reasoning_parts.append(
            f"SCPRS median: ${scprs_ref:.2f} ({history['matches']} data points)"
        )

    # ─── Step 2: Determine data quality ──────────────────────────────────
    has_scprs = scprs_ref is not None
    has_cost = supplier_cost is not None and supplier_cost > 0

    if has_scprs and has_cost:
        rec.data_quality = "full"
    elif has_scprs:
        rec.data_quality = "scprs_only"
    elif has_cost:
        rec.data_quality = "cost_only"
    else:
        rec.data_quality = "no_data"
        rec.flags.append("no_pricing_data")
        rec.reasoning = "No SCPRS history or supplier cost available. Manual pricing required."
        return rec.to_dict()

    # ─── Step 3: Calculate weighted reference price ──────────────────────
    weights = config["weights"]

    if has_scprs and has_cost:
        # Full data: weighted blend
        weighted_price = (
            scprs_ref * weights["scprs_historical"]
            + supplier_cost * (1 + config["default_markup_pct"]) * weights["supplier_cost"]
            + supplier_cost * (1 + config["safe_markup_pct"]) * weights["margin_goals"]
        )
        reasoning_parts.append(
            f"Weighted blend: SCPRS({weights['scprs_historical']*100:.0f}%) "
            f"+ Cost+markup({weights['supplier_cost']*100:.0f}%) "
            f"+ Margin goal({weights['margin_goals']*100:.0f}%)"
        )
    elif has_scprs:
        weighted_price = scprs_ref
        reasoning_parts.append("Pricing based on SCPRS historical data only")
    else:
        weighted_price = supplier_cost * (1 + config["default_markup_pct"])
        reasoning_parts.append(
            f"Pricing based on cost + {config['default_markup_pct']*100:.0f}% markup (no SCPRS data)"
        )

    # ─── Step 4: Calculate three tiers ───────────────────────────────────
    profit_floor = (
        config["profit_floor_amazon"] if source_type == "amazon"
        else config["profit_floor_general"]
    )
    hard_floor = (supplier_cost + config["hard_floor_margin"]) if has_cost else 0

    # RECOMMENDED: Slight undercut of SCPRS or weighted blend
    rec_price = weighted_price
    if has_scprs:
        rec_price = scprs_ref * (1 - config["scprs_undercut_pct"])

    # Apply profit floor
    if has_cost and rec_price < supplier_cost + profit_floor:
        rec_price = supplier_cost + profit_floor
        rec.flags.append("profit_floor_applied_recommended")

    # Apply hard floor
    if has_cost and rec_price < hard_floor:
        rec_price = hard_floor
        rec.flags.append("hard_floor_applied_recommended")

    rec_margin = ((rec_price - supplier_cost) / supplier_cost) if has_cost and supplier_cost > 0 else None
    rec_wp = win_probability(rec_price, item_number, description)

    rec.recommended = PriceTier(
        price=rec_price,
        margin_pct=rec_margin if rec_margin is not None else 0,
        win_prob=rec_wp["probability"],
        label="Recommended",
    ).to_dict()

    # AGGRESSIVE: Deeper undercut
    if has_scprs:
        agg_price = scprs_ref * (1 - config["aggressive_undercut_pct"])
    else:
        agg_price = rec_price * 0.92  # 8% below recommended

    agg_floor = (
        config["profit_floor_aggressive"] if has_cost
        else config["hard_floor_margin"]
    )
    if has_cost and agg_price < supplier_cost + agg_floor:
        agg_price = supplier_cost + agg_floor
        rec.flags.append("profit_floor_applied_aggressive")
    if has_cost and agg_price < hard_floor:
        agg_price = hard_floor
        rec.flags.append("hard_floor_applied_aggressive")

    agg_margin = ((agg_price - supplier_cost) / supplier_cost) if has_cost and supplier_cost > 0 else None
    agg_wp = win_probability(agg_price, item_number, description)

    rec.aggressive = PriceTier(
        price=agg_price,
        margin_pct=agg_margin if agg_margin is not None else 0,
        win_prob=agg_wp["probability"],
        label="Aggressive",
    ).to_dict()

    # SAFE: Conservative markup
    if has_cost:
        safe_price = supplier_cost * (1 + config["safe_markup_pct"])
    elif has_scprs:
        safe_price = scprs_ref * 0.98  # Just below SCPRS
    else:
        safe_price = rec_price * 1.08

    if has_cost and safe_price < supplier_cost + profit_floor:
        safe_price = supplier_cost + profit_floor

    # Safe tier should not exceed SCPRS median (competitive ceiling)
    if has_scprs and safe_price > scprs_ref:
        safe_price = scprs_ref * 0.99  # Just below median
        rec.flags.append("safe_capped_at_scprs")

    # Enforce tier ordering: safe >= recommended >= aggressive
    if safe_price < rec_price:
        safe_price = rec_price * 1.02  # Safe always slightly above recommended
    if agg_price > rec_price:
        agg_price = rec_price * 0.97  # Aggressive always below recommended

    safe_margin = ((safe_price - supplier_cost) / supplier_cost) if has_cost and supplier_cost > 0 else None
    safe_wp = win_probability(safe_price, item_number, description)

    rec.safe = PriceTier(
        price=safe_price,
        margin_pct=safe_margin if safe_margin is not None else 0,
        win_prob=safe_wp["probability"],
        label="Safe",
    ).to_dict()

    # ─── Step 5: Flags and warnings ──────────────────────────────────────
    if has_scprs and rec_price > scprs_ref * (1 + config["ceiling_alert_pct"]):
        rec.flags.append("price_above_recent_wins")

    if history["trend"] == "rising":
        rec.flags.append("prices_trending_up")
        reasoning_parts.append("📈 SCPRS prices trending upward — room for higher bids")
    elif history["trend"] == "falling":
        rec.flags.append("prices_trending_down")
        reasoning_parts.append("📉 SCPRS prices trending downward — bid conservatively")

    if history["matches"] > 0 and history["matches"] < 3:
        rec.flags.append("limited_scprs_data")

    # Check for stale data
    if history["data_points"]:
        best_freshness = max(dp["freshness_weight"] for dp in history["data_points"])
        if best_freshness < 0.5:
            rec.flags.append("stale_scprs_data")
            reasoning_parts.append("⚠️ SCPRS data is >12 months old — verify pricing")

    if has_cost and has_scprs:
        cost_vs_scprs = (supplier_cost / scprs_ref) * 100
        if cost_vs_scprs > 85:
            rec.flags.append("thin_margin_opportunity")
            reasoning_parts.append(
                f"⚠️ Supplier cost is {cost_vs_scprs:.0f}% of SCPRS price — thin margins"
            )

    rec.reasoning = " | ".join(reasoning_parts)
    result = rec.to_dict()
    log.info("Priced %s → $%.2f (%s) quality=%s flags=%s",
             item_number or description[:30],
             result.get("recommended", {}).get("price", 0),
             result.get("recommended", {}).get("margin_pct_display", "?"),
             rec.data_quality, rec.flags or "none")
    return result


# ─── Batch Pricing ───────────────────────────────────────────────────────────

def recommend_prices_for_rfq(rfq_data: dict, config_overrides: Optional[dict] = None) -> dict:
    """
    Generate pricing recommendations for all line items in an RFQ.

    Args:
        rfq_data: Parsed RFQ data with line_items array
        config_overrides: Optional pricing config overrides

    Returns:
        {
            "rfq_id": str,
            "agency": str,
            "items": [PricingRecommendation for each line item],
            "summary": {
                "total_items": int,
                "priced": int,
                "needs_manual": int,
                "avg_win_probability": float,
                "total_recommended": float,
                "total_aggressive": float,
                "total_safe": float,
            }
        }
    """
    agency = rfq_data.get("agency", "CCHCS")
    line_items = rfq_data.get("line_items", [])
    results = []
    priced = 0
    needs_manual = 0
    total_rec = 0.0
    total_agg = 0.0
    total_safe = 0.0
    win_probs = []

    for item in line_items:
        rec = recommend_price(
            item_number=item.get("item_number", ""),
            description=item.get("description", ""),
            supplier_cost=item.get("supplier_cost") or item.get("price_per_unit") or None,
            scprs_price=item.get("scprs_price"),
            agency=agency,
            source_type=item.get("source_type", "general"),
            quantity=item.get("qty", 1),
            config_overrides=config_overrides,
        )

        rec["line_number"] = item.get("line_number")
        rec["item_number"] = item.get("item_number", "")
        rec["description"] = item.get("description", "")
        rec["quantity"] = item.get("qty", 1)

        if rec["data_quality"] != "no_data":
            priced += 1
            qty = item.get("qty", 1)
            if rec["recommended"]:
                total_rec += rec["recommended"]["price"] * qty
                win_probs.append(rec["recommended"]["win_probability"])
            if rec["aggressive"]:
                total_agg += rec["aggressive"]["price"] * qty
            if rec["safe"]:
                total_safe += rec["safe"]["price"] * qty
        else:
            needs_manual += 1

        results.append(rec)

    avg_wp = sum(win_probs) / len(win_probs) if win_probs else 0

    return {
        "rfq_id": rfq_data.get("solicitation_number", "unknown"),
        "agency": agency,
        "items": results,
        "summary": {
            "total_items": len(line_items),
            "priced": priced,
            "needs_manual": needs_manual,
            "avg_win_probability": round(avg_wp, 3),
            "avg_win_probability_display": f"{avg_wp * 100:.0f}%",
            "total_recommended": round(total_rec, 2),
            "total_aggressive": round(total_agg, 2),
            "total_safe": round(total_safe, 2),
        },
    }


# ─── Legacy Compatibility Layer ──────────────────────────────────────────────

def calculate_recommended_price_legacy(
    cost: float,
    scprs_price: Optional[float],
    source_type: str = "general",
    config: Optional[dict] = None,
) -> float:
    """
    Backward-compatible wrapper that returns a single price.

    Drop-in replacement for reytech_filler_v4.calculate_recommended_price().
    Uses the new oracle internally but returns just the recommended price.
    """
    rec = recommend_price(
        item_number="",
        description="",
        supplier_cost=cost if cost > 0 else None,
        scprs_price=scprs_price,
        source_type=source_type,
        config_overrides=config,
    )

    if rec["recommended"]:
        return rec["recommended"]["price"]

    # Fallback to old logic if oracle returns no data
    cfg = load_config()
    if scprs_price and scprs_price > 0:
        price = scprs_price * (1 - cfg["scprs_undercut_pct"])
    else:
        price = cost * (1 + cfg["default_markup_pct"])

    floor = cfg["profit_floor_amazon"] if source_type == "amazon" else cfg["profit_floor_general"]
    if cost > 0 and price < cost + floor:
        price = cost + floor

    return round(price, 2)


# ─── Utility ─────────────────────────────────────────────────────────────────

def pricing_health_check() -> dict:
    """
    Check the health of the pricing system.

    Returns diagnostics about KB size, data freshness, coverage.
    """
    from won_quotes_db import get_kb_stats
    stats = get_kb_stats()

    health = {
        "status": "healthy",
        "kb_records": stats["total_records"],
        "categories_covered": len(stats.get("categories", {})),
        "config_loaded": os.path.exists(CONFIG_FILE),
        "issues": [],
    }

    if stats["total_records"] == 0:
        health["status"] = "degraded"
        health["issues"].append("Won Quotes KB is empty — run SCPRS bulk lookups to populate")

    if stats["total_records"] < 50:
        health["issues"].append(
            f"Only {stats['total_records']} records in KB — accuracy improves with >100 records"
        )

    return health
