"""
Outreach Agent
Generates personalized prospect emails citing Reytech's
FI$Cal win data. A/B tests messaging strategies.
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.outreach_agent")


def generate_outreach_email(buyer_email, strategy="A"):
    """Generate personalized outreach email for a prospect."""
    from src.agents.buyer_intelligence import get_buyer_profile

    profile = get_buyer_profile(buyer_email)
    if not profile:
        return {"error": f"Buyer {buyer_email} not found"}

    buyer_items = profile.get("purchase_history", [])[:20]
    overlap = profile.get("overlap_items", [])
    department = profile.get("department", "your department")
    buyer_name = profile.get("buyer_name", "")
    first_name = buyer_name.split()[0] if buyer_name else ""

    # Reytech stats
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    rt_stats = db.execute("""
        SELECT COUNT(DISTINCT po_number),
               COUNT(DISTINCT dept_name),
               SUM(CAST(REPLACE(REPLACE(grand_total,'$',''),',','') AS REAL))
        FROM scprs_po_master WHERE UPPER(supplier) LIKE '%REYTECH%'
    """).fetchone()
    total_pos = rt_stats[0] or 0
    total_depts = rt_stats[1] or 0
    total_value = rt_stats[2] or 0
    db.close()

    # Find price advantages
    price_advantages = []
    for item in buyer_items[:10]:
        try:
            buyer_price = float(str(item.get("unit_price", "")).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            continue
        desc = item.get("description", "")
        if not desc or buyer_price <= 0:
            continue
        from src.agents.quote_intelligence import get_reytech_prices, _parse_price_str
        rt_prices = get_reytech_prices(desc, limit=1)
        if rt_prices:
            rt_price = _parse_price_str(rt_prices[0].get("unit_price"))
            if rt_price and rt_price > 0:
                savings_pct = ((buyer_price - rt_price) / buyer_price) * 100
                if savings_pct > 10:
                    price_advantages.append({
                        "item": desc[:80], "their_price": buyer_price,
                        "our_price": rt_price, "savings_pct": round(savings_pct, 0),
                    })

    reytech_wins = [{"item": o.get("reytech_item", ""), "price": o.get("reytech_price", ""),
                     "po": o.get("reytech_po", "")} for o in overlap[:5]]

    if strategy == "A":
        email = _build_price_email(first_name, department, reytech_wins,
                                   price_advantages, total_pos, total_depts, total_value)
    else:
        email = _build_relationship_email(first_name, department, reytech_wins,
                                          total_pos, total_depts, total_value)

    email["buyer_email"] = buyer_email
    email["buyer_name"] = buyer_name
    email["strategy"] = strategy
    email["generated_at"] = datetime.now().isoformat()
    email["data_used"] = {
        "overlap_items": len(overlap),
        "price_advantages": len(price_advantages),
        "buyer_items_analyzed": len(buyer_items),
    }
    return email


def _build_price_email(first_name, department, wins, advantages, total_pos, total_depts, total_value):
    """Strategy A: Lead with specific price savings."""
    greeting = f"Hi {first_name}," if first_name else "Hello,"

    hook = ""
    if advantages:
        best = max(advantages, key=lambda x: x["savings_pct"])
        hook = (f"I noticed {department} recently purchased {best['item'][:50]}... "
                f"at ${best['their_price']:.2f}/unit. Reytech has supplied the same item "
                f"to California state agencies at ${best['our_price']:.2f} — "
                f"that's {best['savings_pct']:.0f}% less.")
    elif wins:
        hook = (f"Reytech has been supplying {wins[0]['item'][:50]}... to California agencies, "
                f"and I wanted to reach out about how we might help {department}.")
    else:
        hook = (f"Reytech is a California Small Business that supplies {department}'s "
                f"product categories, and I'd like to explore how we can help.")

    body_parts = [hook, ""]
    if total_pos > 5:
        body_parts.append(
            f"As a certified California SB/MB, Reytech has fulfilled {total_pos} purchase orders "
            f"across {total_depts} state departments, totaling over ${total_value:,.0f} in value.")

    if len(advantages) > 1:
        body_parts.append("")
        body_parts.append("Based on your recent purchasing, here are areas where we can offer competitive pricing:")
        for adv in advantages[:3]:
            body_parts.append(f"  - {adv['item'][:60]}... — current: ${adv['their_price']:.2f}, "
                              f"our price: ${adv['our_price']:.2f} ({adv['savings_pct']:.0f}% savings)")

    body_parts.extend(["", "I'd be happy to provide a detailed quote on any items you're sourcing. "
                        "Would a quick call this week work?", "",
                        "Best regards,", "Mike Gonzalez", "Reytech Inc.", "sales@reytechinc.com"])

    return {
        "subject": f"Cost savings opportunity for {department}",
        "subject_variants": [f"Cost savings opportunity for {department}",
                             "Competitive pricing on items you're sourcing",
                             f"California SB pricing for {department}"],
        "body": "\n".join(body_parts), "greeting": greeting,
    }


def _build_relationship_email(first_name, department, wins, total_pos, total_depts, total_value):
    """Strategy B: Lead with relationship and SB certification."""
    greeting = f"Hi {first_name}," if first_name else "Hello,"

    body_parts = [
        f"I'm reaching out from Reytech Inc., a California-certified Small Business "
        f"and Micro Business that has been proudly serving state agencies.", ""]

    if total_pos > 5:
        body_parts.append(
            f"We've fulfilled {total_pos} purchase orders across {total_depts} departments, "
            f"and we'd love the opportunity to support {department} as well.")

    if wins:
        body_parts.extend(["", "We specialize in many of the product categories your team regularly sources:"])
        seen = set()
        for w in wins[:5]:
            item_short = w["item"][:50]
            if item_short not in seen:
                body_parts.append(f"  - {item_short}...")
                seen.add(item_short)

    body_parts.extend(["",
                        "As a SB/MB, we understand the value of responsive service and competitive pricing. "
                        "We'd welcome the chance to earn your business.", "",
                        "Would you have 15 minutes this week for a brief introduction?", "",
                        "Best regards,", "Mike Gonzalez", "Reytech Inc.", "sales@reytechinc.com"])

    return {
        "subject": f"Introduction — California SB serving {department}",
        "subject_variants": [f"Introduction — California SB serving {department}",
                             f"Reytech Inc. — SB/MB supplier for {department}",
                             "Quick intro — competitive SB pricing for your team"],
        "body": "\n".join(body_parts), "greeting": greeting,
    }


def generate_batch_outreach(limit=20, min_score=30):
    """Generate outreach emails for top prospects with A/B variants."""
    from src.agents.buyer_intelligence import get_top_prospects

    prospects = get_top_prospects(limit=limit, min_score=min_score, exclude_customers=True)
    batch = []
    for prospect in prospects:
        email = prospect["email"]
        try:
            variant_a = generate_outreach_email(email, strategy="A")
            variant_b = generate_outreach_email(email, strategy="B")
            batch.append({"prospect": prospect, "variant_a": variant_a, "variant_b": variant_b})
        except Exception as e:
            log.warning("Outreach gen for %s failed: %s", email, str(e)[:40])

    log.info("Generated %d outreach email pairs (A/B)", len(batch))
    return batch
