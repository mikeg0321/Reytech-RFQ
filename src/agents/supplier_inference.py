"""
supplier_inference.py — Infer competitor supply chains from SCPRS pricing data.

When we lose a quote, this engine analyzes the winner's pricing to infer
WHERE they sourced the products (direct manufacturer, distributor, retail).
This helps Mike decide: negotiate better terms, find alternate suppliers,
or accept that cost basis is unbeatable.
"""
import logging
log = logging.getLogger("reytech.supplier_inference")

# Known supplier tiers — typical markup ranges by channel
_SUPPLIER_PROFILES = {
    # Manufacturer-direct (they ARE the manufacturer)
    "manufacturers": {
        "names": ["cardinal health", "3m", "medline industries", "kimberly-clark",
                  "halyard", "ansell", "dynarex", "mckesson medical", "becton dickinson",
                  "bd", "honeywell", "dupont", "stryker", "hill-rom"],
        "typical_margin": (8, 15),  # 8-15% markup on COGS
        "inference": "Manufacturer-direct — cannot beat on cost basis",
        "action": "Compete on service/delivery, not price",
    },
    # Major distributors with volume tiers
    "distributors": {
        "names": ["medline", "henry schein", "mckesson", "cardinal", "owens & minor",
                  "concordance", "bound tree", "moore medical", "fisher scientific",
                  "vwr", "grainger industrial", "fastenal"],
        "typical_margin": (15, 30),
        "inference": "Distributor account — likely has volume tier pricing",
        "action": "Negotiate volume tier with same distributor or source alternate",
    },
    # Retail/catalog resellers
    "retail": {
        "names": ["amazon", "staples", "office depot", "uline", "zoro",
                  "global industrial", "walmart", "costco", "sams club"],
        "typical_margin": (25, 60),
        "inference": "Retail/catalog sourcing — no special pricing",
        "action": "Should be beatable with distributor pricing",
    },
    # Government specialty resellers (like Reytech)
    "gov_resellers": {
        "names": ["cal-micro", "gsa advantage", "calpia", "prison industry",
                  "ability one", "skilcraft", "unicor"],
        "typical_margin": (10, 25),
        "inference": "Government specialty reseller",
        "action": "Similar channel — compete on markup and relationship",
    },
}


def infer_supply_chain(competitor_name, competitor_price, our_cost=0,
                       our_supplier="", item_description="", category=""):
    """Infer a competitor's likely supply chain from their pricing.

    Returns:
        {
            "inferred_source": str,     # e.g., "Medline Direct Account"
            "channel": str,             # manufacturer/distributor/retail/gov_reseller/unknown
            "confidence": str,          # high/medium/low
            "reasoning": str,           # Human-readable explanation
            "cost_gap": float,          # competitor_price - our_cost (negative = they're cheaper)
            "cost_gap_pct": float,      # percentage
            "actionable": bool,         # Can we do something about it?
            "action": str,              # Specific recommendation
        }
    """
    result = {
        "inferred_source": "Unknown",
        "channel": "unknown",
        "confidence": "low",
        "reasoning": "",
        "cost_gap": 0,
        "cost_gap_pct": 0,
        "actionable": False,
        "action": "",
    }

    if not competitor_price or competitor_price <= 0:
        return result

    comp_lower = (competitor_name or "").lower().strip()

    # 1. Check if competitor IS a known manufacturer
    for name in _SUPPLIER_PROFILES["manufacturers"]["names"]:
        if name in comp_lower or comp_lower in name:
            result["inferred_source"] = f"{competitor_name} (Manufacturer-Direct)"
            result["channel"] = "manufacturer"
            result["confidence"] = "high"
            result["reasoning"] = (f"{competitor_name} IS the manufacturer — "
                                   f"selling at ${competitor_price:.2f} which is near their COGS")
            result["action"] = "Cannot undercut manufacturer. Compete on service, delivery speed, or bundle value."
            result["actionable"] = False
            if our_cost > 0:
                result["cost_gap"] = round(competitor_price - our_cost, 2)
                result["cost_gap_pct"] = round((competitor_price - our_cost) / our_cost * 100, 1)
            return result

    # 2. Check if competitor is a known distributor
    for name in _SUPPLIER_PROFILES["distributors"]["names"]:
        if name in comp_lower or comp_lower in name:
            result["inferred_source"] = f"{competitor_name} (Distributor)"
            result["channel"] = "distributor"
            result["confidence"] = "high"
            result["reasoning"] = (f"{competitor_name} is a major distributor — "
                                   f"likely has volume tier at ${competitor_price:.2f}")
            result["action"] = "Negotiate matching tier with your distributor rep, or source direct."
            result["actionable"] = True
            if our_cost > 0:
                result["cost_gap"] = round(competitor_price - our_cost, 2)
                result["cost_gap_pct"] = round((competitor_price - our_cost) / our_cost * 100, 1)
            return result

    # 3. Price-based inference (when competitor is unknown reseller)
    if our_cost > 0:
        cost_gap = competitor_price - our_cost
        cost_gap_pct = (cost_gap / our_cost) * 100
        result["cost_gap"] = round(cost_gap, 2)
        result["cost_gap_pct"] = round(cost_gap_pct, 1)

        if cost_gap_pct < -15:
            # They're selling way below our cost — must have direct/volume deal
            result["inferred_source"] = "Direct Manufacturer or Volume Tier Account"
            result["channel"] = "manufacturer"
            result["confidence"] = "medium"
            result["reasoning"] = (f"Competitor price ${competitor_price:.2f} is {abs(cost_gap_pct):.0f}% "
                                   f"BELOW our cost ${our_cost:.2f} from {our_supplier or 'supplier'}. "
                                   f"They likely have a direct manufacturer account or deep volume discount.")
            result["action"] = (f"Contact {our_supplier or 'your supplier'} about volume tier pricing, "
                                f"or find alternate manufacturer source.")
            result["actionable"] = True

        elif cost_gap_pct < -5:
            # Moderately below our cost — better distributor deal
            result["inferred_source"] = "Better Distributor Tier"
            result["channel"] = "distributor"
            result["confidence"] = "medium"
            result["reasoning"] = (f"Competitor price ${competitor_price:.2f} is {abs(cost_gap_pct):.0f}% "
                                   f"below our cost ${our_cost:.2f}. "
                                   f"They likely have a better distributor tier or contract pricing.")
            result["action"] = (f"Negotiate better terms with {our_supplier or 'supplier'} "
                                f"or request competitive pricing match.")
            result["actionable"] = True

        elif cost_gap_pct < 15:
            # Near our cost — similar supply chain, just lower markup
            result["inferred_source"] = "Similar Supply Chain, Lower Markup"
            result["channel"] = "gov_reseller"
            result["confidence"] = "medium"
            result["reasoning"] = (f"Competitor price ${competitor_price:.2f} is only {cost_gap_pct:+.0f}% "
                                   f"from our cost ${our_cost:.2f}. "
                                   f"Similar supply chain but running at thinner margins.")
            result["action"] = "Reduce markup to compete — you have the same cost basis."
            result["actionable"] = True

        elif cost_gap_pct < 40:
            # Above our cost but not extreme — retail pricing
            result["inferred_source"] = "Catalog/Retail Pricing"
            result["channel"] = "retail"
            result["confidence"] = "low"
            result["reasoning"] = (f"Competitor price ${competitor_price:.2f} is {cost_gap_pct:.0f}% "
                                   f"above our cost. Likely buying at retail or catalog pricing.")
            result["action"] = "You should be able to beat this — check your markup."
            result["actionable"] = True
        else:
            result["inferred_source"] = "Retail/No Volume Discount"
            result["channel"] = "retail"
            result["confidence"] = "low"
            result["reasoning"] = (f"Competitor price ${competitor_price:.2f} is {cost_gap_pct:.0f}% "
                                   f"above our cost — likely retail with no special pricing.")
            result["action"] = "Easy win opportunity — your cost basis is much better."
            result["actionable"] = True
    else:
        # No cost data — can only infer from competitor name
        result["inferred_source"] = "Unknown (no cost data to compare)"
        result["confidence"] = "low"
        result["reasoning"] = "Cannot infer supply chain without our cost basis for comparison."
        result["action"] = "Enter supplier cost to enable competitive analysis."

    return result


def batch_infer(line_comparisons, our_supplier=""):
    """Run inference on all line items from a loss analysis.

    Args:
        line_comparisons: list of dicts from award_tracker._analyze_loss()
            Each has: our_unit_price, winner_unit_price, our_cost, description, etc.
        our_supplier: default supplier name

    Returns: list of inference dicts (same order as input)
    """
    results = []
    for comp in line_comparisons:
        inf = infer_supply_chain(
            competitor_name=comp.get("winner_supplier", ""),
            competitor_price=comp.get("winner_unit_price", 0),
            our_cost=comp.get("our_cost", 0),
            our_supplier=comp.get("our_supplier", our_supplier),
            item_description=comp.get("description", comp.get("our_description", "")),
            category=comp.get("category", ""),
        )
        results.append(inf)
    return results
