"""
web_price_research.py — Claude-Powered Web Price Research
Phase 35 | Version 1.0.0

Uses Anthropic API with web_search tool to find real-time product prices.
Replaces SerpApi ($50/mo) with Claude Haiku + web search.

Architecture:
  1. Check local cache (7-day TTL)
  2. Call Claude Haiku with web_search tool
  3. Claude searches Google Shopping / Amazon / supplier sites
  4. Extract structured price data from response
  5. Cache result → feed into pricing pipeline

Cost: ~$0.001-0.003 per item lookup (Haiku + web search)
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

log = logging.getLogger("web_price")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

CACHE_FILE = os.path.join(DATA_DIR, "web_price_cache.json")
CACHE_TTL_DAYS = 7
MAX_CACHE = 3000

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ── API Key ──────────────────────────────────────────────────────────────────

def _get_api_key() -> str:
    """Get Anthropic API key from env."""
    for var in ("AGENT_PRICING_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var, "")
        if key:
            return key
    return ""


# ── Cache ────────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_cache(cache: dict):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        # Evict expired + trim
        now = datetime.now(timezone.utc).isoformat()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS)).isoformat()
        cache = {k: v for k, v in cache.items()
                 if v.get("cached_at", "") > cutoff}
        if len(cache) > MAX_CACHE:
            items = sorted(cache.items(), key=lambda x: x[1].get("cached_at",""))
            cache = dict(items[-MAX_CACHE:])
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=1)
    except Exception as e:
        log.debug("Cache save error: %s", e)

def _cache_key(description: str, part_number: str = "") -> str:
    raw = f"{description.lower().strip()}|{part_number.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ── Claude Web Search ────────────────────────────────────────────────────────

def search_product_price(
    description: str,
    part_number: str = "",
    qty: int = 1,
    uom: str = "EA",
    context: str = "",
) -> dict:
    """
    Search the web for a product's price using Claude + web_search tool.
    
    Returns:
        {
            "found": True/False,
            "price": float,          # best unit price found
            "source": str,           # retailer/supplier name
            "url": str,              # product page URL
            "title": str,            # product title as listed
            "options": [             # all prices found
                {"price": float, "source": str, "url": str, "title": str}
            ],
            "confidence": float,     # 0-1 how confident the match is
            "cached": bool,
            "error": str or None,
        }
    """
    if not description and not part_number:
        return {"found": False, "error": "No description or part number"}
    
    # Check cache first
    ck = _cache_key(description, part_number)
    cache = _load_cache()
    if ck in cache:
        cached = cache[ck]
        cached["cached"] = True
        return cached
    
    api_key = _get_api_key()
    if not api_key:
        return {"found": False, "error": "ANTHROPIC_API_KEY not set"}
    
    if not HAS_REQUESTS:
        return {"found": False, "error": "requests library not available"}
    
    # Build the search prompt
    search_query = part_number if part_number and len(part_number) > 3 else description
    if part_number and description:
        search_query = f"{part_number} {description}"
    
    prompt = f"""Find the current retail/wholesale price for this product. Search Google Shopping, Amazon, and medical supply sites.

Product: {description}
{f'Part/Item Number: {part_number}' if part_number else ''}
{f'Quantity needed: {qty} {uom}' if qty > 1 else ''}
{f'Context: {context}' if context else ''}

Search for this exact product or the closest match. I need:
1. The unit price (per {uom})
2. The retailer/supplier name
3. The product URL
4. The MFG part number, SKU, or item number from the product page
5. How confident you are this is the right product (0-100%)

If you find multiple sources, list them all sorted by price (lowest first).

IMPORTANT: Respond in this exact JSON format only, no other text:
{{
  "found": true,
  "results": [
    {{"price": 12.99, "source": "Amazon", "url": "https://...", "title": "Product Name", "part_number": "ABC-123", "confidence": 85}}
  ]
}}
If you can't find the product, respond: {{"found": false, "reason": "..."}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "web-search-2025-03-05",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "tools": [{
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": 3,
                }],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        
        # Retry once on transient errors (502/503/529)
        if resp.status_code in (502, 503, 529):
            log.warning("Claude API %d on first try, retrying...", resp.status_code)
            time.sleep(1)
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "anthropic-beta": "web-search-2025-03-05",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1024,
                    "tools": [{
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": 3,
                    }],
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=60,
            )
        
        if resp.status_code != 200:
            err = resp.text[:200]
            log.error("Claude API error %d: %s", resp.status_code, err)
            return {"found": False, "error": f"API error {resp.status_code}: {err[:100]}"}
        
        data = resp.json()
        
        # Extract text from response (may have multiple content blocks)
        full_text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                full_text += block.get("text", "")
        
        if not full_text:
            return {"found": False, "error": "Empty response from Claude"}
        
        # Parse JSON from response
        result = _parse_price_response(full_text, description)
        
        # Cache if found
        if result.get("found"):
            result["cached"] = False
            result["cached_at"] = datetime.now(timezone.utc).isoformat()
            result["query"] = search_query[:80]
            cache[ck] = result
            _save_cache(cache)
        
        return result
        
    except requests.Timeout:
        return {"found": False, "error": "API timeout (30s)"}
    except Exception as e:
        log.error("Web price search error: %s", e)
        return {"found": False, "error": str(e)[:100]}


def _parse_price_response(text: str, original_desc: str) -> dict:
    """Extract structured price data from Claude's response."""
    result = {
        "found": False, "price": 0, "source": "", "url": "", "title": "",
        "options": [], "confidence": 0, "error": None,
    }
    
    # Try to find JSON in the response
    json_match = re.search(r'\{[\s\S]*\}', text)
    if json_match:
        try:
            parsed = json.loads(json_match.group())
            if parsed.get("found") is False:
                result["error"] = parsed.get("reason", "Product not found")
                return result
            
            results_list = parsed.get("results", [])
            if not results_list:
                return result
            
            # Sort by confidence * inverse_price (favor cheap + confident)
            for r in results_list:
                r["price"] = float(r.get("price", 0) or 0)
                r["confidence"] = float(r.get("confidence", 50) or 50)
            
            results_list = [r for r in results_list if r["price"] > 0]
            if not results_list:
                return result
            
            results_list.sort(key=lambda x: x["price"])
            
            best = results_list[0]
            result["found"] = True
            result["price"] = best["price"]
            result["source"] = best.get("source", "")
            result["url"] = best.get("url", "")
            result["title"] = best.get("title", "")[:100]
            result["part_number"] = best.get("part_number", "")
            result["confidence"] = best["confidence"] / 100
            result["options"] = [{
                "price": r["price"],
                "source": r.get("source", ""),
                "url": r.get("url", ""),
                "title": r.get("title", "")[:80],
                "part_number": r.get("part_number", ""),
            } for r in results_list[:6]]
            
            return result
            
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            log.debug("JSON parse error: %s", e)
    
    # Fallback: extract prices from plain text
    prices = re.findall(r'\$(\d+(?:,\d{3})*\.?\d{0,2})', text)
    if prices:
        price_vals = [float(p.replace(",", "")) for p in prices if float(p.replace(",", "")) > 0]
        if price_vals:
            result["found"] = True
            result["price"] = min(price_vals)
            result["confidence"] = 0.5
            result["source"] = "web search"
            result["options"] = [{"price": p, "source": "web"} for p in sorted(set(price_vals))[:5]]
    
    return result


# ── Batch Operations ─────────────────────────────────────────────────────────

def bulk_web_search(items: list, max_items: int = 15) -> list:
    """
    Search prices for multiple items. Rate-limited to avoid API abuse.
    
    items: [{description, part_number, qty, uom, idx}, ...]
    Returns: [{idx, found, price, source, url, options, ...}, ...]
    """
    results = []
    searched = 0
    
    for item in items[:max_items]:
        desc = (item.get("description") or "").strip()
        pn = str(item.get("part_number") or item.get("item_number") or "").strip()
        
        if not desc and not pn:
            results.append({"idx": item.get("idx", 0), "found": False})
            continue
        
        result = search_product_price(
            description=desc,
            part_number=pn,
            qty=item.get("qty", 1),
            uom=item.get("uom", "EA"),
        )
        result["idx"] = item.get("idx", 0)
        results.append(result)
        searched += 1
        
        # Rate limit: 1 request per second (uncached only)
        if not result.get("cached") and searched < len(items):
            time.sleep(1.0)
    
    return results


def web_search_for_pc(pc_id: str) -> dict:
    """
    Run web price search for all unpriced items in a Price Check.
    Used by the 🔬 Amazon / 🛒 Sweep buttons.
    
    Returns: {ok, found, total, results: [{idx, found, price, ...}]}
    """
    try:
        from src.api.dashboard import _load_price_checks, _save_price_checks
    except ImportError:
        try:
            import json as _j
            pcs_path = os.path.join(DATA_DIR, "price_checks.json")
            with open(pcs_path) as f:
                pcs = _j.load(f)
        except Exception:
            return {"ok": False, "error": "Cannot load price checks"}
    
    try:
        pcs = _load_price_checks()
    except Exception:
        pcs_path = os.path.join(DATA_DIR, "price_checks.json")
        try:
            with open(pcs_path) as f:
                pcs = json.load(f)
        except Exception:
            return {"ok": False, "error": "Cannot load PCs"}
    
    pc = pcs.get(pc_id)
    if not pc:
        return {"ok": False, "error": "PC not found"}
    
    items_to_search = []
    for i, item in enumerate(pc.get("items", [])):
        p = item.get("pricing", {})
        # Skip already-priced items
        if p.get("amazon_price") or p.get("unit_cost") or p.get("recommended_price"):
            continue
        items_to_search.append({
            "idx": i,
            "description": item.get("description", ""),
            "part_number": str(item.get("item_number", "") or ""),
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "EA"),
        })
    
    if not items_to_search:
        return {"ok": True, "found": 0, "total": len(pc.get("items", [])),
                "message": "All items already have prices"}
    
    results = bulk_web_search(items_to_search)
    found = 0
    
    # Apply results to PC items
    for r in results:
        if not r.get("found"):
            continue
        idx = r["idx"]
        if idx >= len(pc["items"]):
            continue
        
        item = pc["items"][idx]
        if not item.get("pricing"):
            item["pricing"] = {}
        
        p = item["pricing"]
        p["web_price"] = r["price"]
        p["web_source"] = r.get("source", "")
        p["web_url"] = r.get("url", "")
        p["web_title"] = r.get("title", "")
        p["web_confidence"] = r.get("confidence", 0)
        p["web_options"] = r.get("options", [])[:5]
        p["web_searched_at"] = datetime.now(timezone.utc).isoformat()
        
        # Store part/MFG number if found
        web_pn = r.get("part_number", "")
        if web_pn:
            p["web_part_number"] = web_pn
            # Also set on item if empty
            if not item.get("mfg_number"):
                item["mfg_number"] = web_pn
        
        # Set as unit_cost if not already set
        if not p.get("unit_cost") and r["price"] > 0:
            p["unit_cost"] = r["price"]
        # Also set amazon_price for backward compat with UI
        if not p.get("amazon_price") and r["price"] > 0:
            p["amazon_price"] = r["price"]
            p["amazon_source"] = r.get("source", "web")
            p["amazon_title"] = r.get("title", "")
            p["amazon_url"] = r.get("url", "")
        
        found += 1

        # Write-back to catalog DB
        try:
            from src.agents.product_catalog import (
                match_item as _wm, add_to_catalog as _wa,
                add_supplier_price as _ws, init_catalog_db as _wi
            )
            _wi()
            _desc = item.get("description", "")
            _pn = str(item.get("item_number", "") or web_pn or "")
            _wmatches = _wm(_desc, _pn, top_n=1) if (_desc or _pn) else []
            if _wmatches and _wmatches[0].get("match_confidence", 0) >= 0.55:
                _wpid = _wmatches[0]["id"]
            else:
                _wpid = _wa(description=_desc, part_number=_pn,
                            cost=r["price"], source="web_search_pc")
            if _wpid and r.get("source"):
                _ws(_wpid, r["source"], r["price"],
                    url=r.get("url", ""), sku=web_pn)
        except Exception:
            pass
    
    # Save
    if found > 0:
        pc["web_searched"] = True
        pc["web_searched_at"] = datetime.now(timezone.utc).isoformat()
        try:
            _save_price_checks(pcs)
        except Exception:
            pcs_path = os.path.join(DATA_DIR, "price_checks.json")
            with open(pcs_path, "w") as f:
                json.dump(pcs, f, indent=2, default=str)
    
    return {
        "ok": True, "found": found, "total": len(pc.get("items", [])),
        "searched": len(items_to_search),
        "results": [{
            "idx": r["idx"], "found": r.get("found", False),
            "price": r.get("price", 0), "source": r.get("source", ""),
            "url": r.get("url", ""), "title": r.get("title", ""),
            "options": r.get("options", [])[:4],
            "confidence": r.get("confidence", 0),
        } for r in results],
    }


# ── Status / Diagnostics ────────────────────────────────────────────────────

def get_status() -> dict:
    """Return status for diagnostics page."""
    cache = _load_cache()
    api_key = _get_api_key()
    return {
        "available": bool(api_key) and HAS_REQUESTS,
        "api_key_set": bool(api_key),
        "api_key_source": "AGENT_PRICING_KEY" if os.environ.get("AGENT_PRICING_KEY") else (
            "ANTHROPIC_API_KEY" if os.environ.get("ANTHROPIC_API_KEY") else "none"),
        "cache_entries": len(cache),
        "cache_file": CACHE_FILE,
        "requests_available": HAS_REQUESTS,
        "model": "claude-haiku-4-5-20251001",
        "cost_per_search": "~$0.001-0.003",
    }
