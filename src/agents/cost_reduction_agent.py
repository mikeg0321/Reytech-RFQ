"""
Oracle V4 — Cost Reduction Agent
Researches cheaper sourcing strategies when we lose on cost.
Uses Claude Haiku + web_search to find wholesale/MFG direct/volume pricing.
"""
import json
import os
import re
import time
import logging
from datetime import datetime

log = logging.getLogger("reytech.cost_reduction")

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

_MIN_SPACING_SECS = 12
_last_api_call = 0.0


def _get_api_key():
    for var in ("AGENT_PRICING_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var, "")
        if key:
            return key
    return ""


def _wait_rate_limit():
    global _last_api_call
    elapsed = time.time() - _last_api_call
    if elapsed < _MIN_SPACING_SECS:
        time.sleep(_MIN_SPACING_SECS - elapsed)
    _last_api_call = time.time()


def research_cost_reduction(description, current_cost, competitor_price=None,
                            mfg_number="", category="", quantity=1, uom="EA"):
    """Research cheaper sourcing strategies for a product.

    Args:
        description: Item description
        current_cost: Our current cost per unit
        competitor_price: What the competitor sold at (optional)
        mfg_number: Manufacturer part number
        category: Product category
        quantity: Typical order quantity
        uom: Unit of measure

    Returns:
        {
            "ok": True/False,
            "strategies": [
                {
                    "type": "contact_mfg_rep|sign_up_wholesale|negotiate_volume|alternative_supplier|direct_from_mfg",
                    "supplier": "Company Name",
                    "contact": "phone/email/url",
                    "estimated_price": float or None,
                    "savings_pct": float or None,
                    "description": "Human-readable action item",
                    "priority": "high|medium|low",
                }
            ],
            "summary": str,
        }
    """
    if not description:
        return {"ok": False, "error": "No description"}

    api_key = _get_api_key()
    if not api_key:
        return {"ok": False, "error": "ANTHROPIC_API_KEY not set"}

    if not HAS_REQUESTS:
        return {"ok": False, "error": "requests library not available"}

    target_price = competitor_price or (current_cost * 0.8)
    cost_str = f"${current_cost:.2f}" if current_cost else "unknown"
    comp_str = f"${competitor_price:.2f}" if competitor_price else "unknown"

    prompt = f"""You are a procurement specialist for Reytech Inc., a California SB/DVBE government reseller.

We lost a government bid because our cost is too high. I need you to research sourcing strategies to get a lower cost.

ITEM: {description}
{f'MFG/Part#: {mfg_number}' if mfg_number else ''}
Our current cost: {cost_str} per {uom}
Competitor's selling price: {comp_str} per {uom}
Typical order quantity: {quantity} {uom}
{f'Category: {category}' if category else ''}

Research and find:
1. WHO manufactures this product? Do they sell direct to resellers? Find their institutional/wholesale sales contact.
2. WHOLESALE DISTRIBUTORS that carry this — especially ones with government/institutional pricing. Find sign-up URLs.
3. VOLUME DISCOUNT programs — can we get better pricing by committing to annual volume?
4. ALTERNATIVE equivalent products from other manufacturers that cost less.
5. CONTACT INFO for sales reps — phone numbers, email addresses, wholesale application URLs.

Focus on finding SPECIFIC, ACTIONABLE leads with real contact info. Not generic advice.

Respond in this exact JSON format:
{{
  "manufacturer": {{
    "name": "Company Name",
    "sells_direct": true/false,
    "contact": "phone/email/url",
    "notes": "details"
  }},
  "strategies": [
    {{
      "type": "contact_mfg_rep|sign_up_wholesale|negotiate_volume|alternative_supplier|direct_from_mfg",
      "supplier": "Company Name",
      "contact": "phone/email/url for sales",
      "estimated_price": null or price as number,
      "description": "Specific action to take",
      "priority": "high|medium|low"
    }}
  ],
  "summary": "One-sentence summary of best strategy"
}}"""

    try:
        _wait_rate_limit()

        body = {
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 2048,
            "tools": [{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 5,
            }],
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "web-search-2025-03-05",
            "content-type": "application/json",
        }

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers, json=body, timeout=90,
        )

        if resp.status_code == 429:
            log.warning("Rate limited on cost reduction research, backing off...")
            time.sleep(30)
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers, json=body, timeout=90,
            )

        if resp.status_code != 200:
            return {"ok": False, "error": f"API {resp.status_code}: {resp.text[:200]}"}

        data = resp.json()
        # Extract text from response content blocks
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        # Parse JSON from response
        json_match = re.search(r'\{[\s\S]*\}', text)
        if not json_match:
            return {"ok": False, "error": "No JSON in response", "raw": text[:500]}

        result = json.loads(json_match.group())

        # Enrich strategies with savings calculations
        strategies = result.get("strategies", [])
        for s in strategies:
            est = s.get("estimated_price")
            if est and current_cost and current_cost > 0:
                s["savings_pct"] = round((1 - est / current_cost) * 100, 1)
            else:
                s["savings_pct"] = None

        return {
            "ok": True,
            "manufacturer": result.get("manufacturer"),
            "strategies": strategies,
            "summary": result.get("summary", ""),
        }

    except json.JSONDecodeError as e:
        return {"ok": False, "error": f"JSON parse error: {e}"}
    except Exception as e:
        log.error("Cost reduction research failed: %s", e, exc_info=True)
        return {"ok": False, "error": str(e)}


def research_and_create_action_items(items, agency="", source_quote=""):
    """Research cost reduction for multiple items and create action_items.

    Args:
        items: list of dicts with description, cost, competitor_price, mfg_number
        agency: agency code
        source_quote: quote number for tracking

    Returns:
        {"ok": True, "items_researched": N, "action_items_created": N}
    """
    from src.core.db import get_db

    stats = {"items_researched": 0, "action_items_created": 0, "errors": 0}
    now = datetime.now().isoformat()

    for item in items:
        desc = item.get("description", "")
        cost = float(item.get("cost") or item.get("current_cost") or 0)
        comp = float(item.get("competitor_price") or 0)
        mfg = item.get("mfg_number", "")
        cat = item.get("category", "")
        qty = item.get("quantity", 1)
        uom = item.get("uom", "EA")

        if not desc or cost <= 0:
            continue

        try:
            result = research_cost_reduction(
                description=desc, current_cost=cost, competitor_price=comp,
                mfg_number=mfg, category=cat, quantity=qty, uom=uom,
            )
            stats["items_researched"] += 1

            if not result.get("ok"):
                stats["errors"] += 1
                continue

            # Create action items from strategies
            with get_db() as conn:
                for s in result.get("strategies", []):
                    action_desc = (
                        f"{s.get('description', '')} | "
                        f"Supplier: {s.get('supplier', '?')} | "
                        f"Contact: {s.get('contact', 'N/A')}"
                    )
                    if s.get("estimated_price"):
                        action_desc += f" | Est. price: ${s['estimated_price']:.2f}"
                    if s.get("savings_pct"):
                        action_desc += f" ({s['savings_pct']:.0f}% savings)"
                    action_desc += f" | Item: {desc[:60]}"

                    conn.execute("""
                        INSERT INTO action_items
                            (created_at, source_quote, action_type, description, priority, status)
                        VALUES (?, ?, ?, ?, ?, 'pending')
                    """, (now, source_quote, s.get("type", "alternative_supplier"),
                          action_desc, s.get("priority", "medium")))
                    stats["action_items_created"] += 1

        except Exception as e:
            log.error("Cost reduction research error for '%s': %s", desc[:40], e)
            stats["errors"] += 1

        # Rate limit between items
        time.sleep(2)

    log.info("V4 cost reduction: %s", stats)
    return {"ok": True, **stats}
