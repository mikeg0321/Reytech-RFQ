"""
pricing_oracle.py — Legacy facade. Delegates to pricing_oracle_v2.

All callers of recommend_price() and recommend_prices_for_rfq() are routed
to the V2 engine (src.core.pricing_oracle_v2). This file exists only for
backward compatibility — new code should import from pricing_oracle_v2 directly.
"""

import logging
from typing import Optional

log = logging.getLogger("reytech.oracle")

# Re-export find_similar_items for callers that import it from here
try:
    from src.knowledge.won_quotes_db import find_similar_items
except ImportError:
    def find_similar_items(*args, **kwargs):
        return []


def recommend_price(
    item_number: str = "",
    description: str = "",
    supplier_cost: Optional[float] = None,
    scprs_price: Optional[float] = None,
    agency: str = "CCHCS",
    source_type: str = "general",
    quantity: float = 1,
    config_overrides: Optional[dict] = None,
) -> dict:
    """Thin wrapper around pricing_oracle_v2.get_pricing()."""
    try:
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(
            description, quantity=int(quantity or 1),
            cost=supplier_cost, item_number=item_number,
            department=agency,
        )
        rec = result.get("recommendation", {})
        market = result.get("market", {})

        # Map V2 result shape to V1 expected shape
        def _tier(price, label):
            if not price:
                return None
            return {"price": price, "label": label, "win_probability": 0.5}

        return {
            "recommended": _tier(rec.get("recommended_price"), "Recommended"),
            "aggressive": _tier(rec.get("aggressive_price"), "Aggressive"),
            "safe": _tier(rec.get("safe_price"), "Safe"),
            "flags": rec.get("flags", []),
            "reasoning": rec.get("reasoning", "V2 engine"),
            "data_quality": "good" if result.get("sources_used") else "no_data",
            "scprs_data": {
                "median": market.get("median", 0),
                "min": market.get("min", 0),
                "max": market.get("max", 0),
                "count": market.get("count", 0),
            } if market.get("count") else None,
            "cost": supplier_cost or 0,
            "market": market,
            "confidence": result.get("confidence", 0),
        }
    except Exception as e:
        log.error("recommend_price facade error: %s", e, exc_info=True)
        return {
            "recommended": None, "aggressive": None, "safe": None,
            "flags": [f"V2 error: {e}"], "reasoning": "fallback",
            "data_quality": "no_data", "scprs_data": None,
            "cost": supplier_cost or 0, "market": {}, "confidence": 0,
        }


def recommend_prices_for_rfq(rfq_data: dict, config_overrides: Optional[dict] = None) -> dict:
    """Batch pricing for all items in an RFQ. Delegates to recommend_price()."""
    agency = rfq_data.get("agency", "CCHCS")
    line_items = rfq_data.get("line_items", [])
    results = []
    priced = 0
    total_rec = 0.0

    for item in line_items:
        rec = recommend_price(
            item_number=item.get("item_number", ""),
            description=item.get("description", ""),
            supplier_cost=item.get("supplier_cost") or item.get("price_per_unit"),
            scprs_price=item.get("scprs_price"),
            agency=agency,
            quantity=item.get("qty", 1),
            config_overrides=config_overrides,
        )
        rec["line_number"] = item.get("line_number")
        rec["item_number"] = item.get("item_number", "")
        rec["description"] = item.get("description", "")
        rec["quantity"] = item.get("qty", 1)

        if rec["data_quality"] != "no_data":
            priced += 1
            if rec["recommended"]:
                total_rec += rec["recommended"]["price"] * (item.get("qty", 1) or 1)

        results.append(rec)

    return {
        "rfq_id": rfq_data.get("rfq_id", ""),
        "agency": agency,
        "items": results,
        "summary": {
            "total_items": len(line_items),
            "priced": priced,
            "needs_manual": len(line_items) - priced,
            "total_recommended": total_rec,
        },
    }


def pricing_health_check() -> dict:
    """Health check — reports V2 status."""
    try:
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing("test item health check", quantity=1)
        return {
            "ok": True,
            "oracle_version": "v2",
            "status": "active",
            "sources_available": len(result.get("sources_used", [])),
        }
    except Exception as e:
        return {"ok": False, "oracle_version": "v2", "error": str(e)}


# Legacy compat
def calculate_recommended_price_legacy(*args, **kwargs):
    """Deprecated — use recommend_price()."""
    return recommend_price(*args, **kwargs)
