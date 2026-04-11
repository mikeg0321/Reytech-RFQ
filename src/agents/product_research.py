"""
product_research.py — Product Research Agent for Reytech RFQ Automation
Phase 6 | Version: 7.0.0

Searches for product prices using Grok (xAI) with built-in web search.
Results feed into the Pricing Oracle as supplier_cost data.

Architecture:
  1. Check local cache (7-day TTL)
  2. Search via Grok → extract price, ASIN, product title
  3. Cache result
  4. Feed into pricing_oracle.recommend_price() as supplier_cost

Dependencies: requests (already in requirements.txt)
Requires: XAI_API_KEY environment variable set in Railway

History: Replaced SerpApi ($50/mo subscription) with Grok (pay-per-use, ~$0.001/call).
"""

import json
import os
import re
import time
import logging
import hashlib
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

log = logging.getLogger("research")

# ─── Configuration ───────────────────────────────────────────────────────────

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
CACHE_FILE = os.path.join(DATA_DIR, "product_research_cache.json")
CACHE_TTL_DAYS = 7
MAX_CACHE_ENTRIES = 5000

# Grok API config
XAI_API_URL = "https://api.x.ai/v1/chat/completions"
XAI_MODEL = "grok-3-mini"
XAI_API_KEY = os.environ.get("XAI_API_KEY", "")

RESEARCH_STATUS = {
    "running": False, "progress": "", "items_done": 0, "items_total": 0,
    "prices_found": 0, "errors": [], "started_at": None, "finished_at": None,
}
_status_lock = threading.Lock()


# ─── Cache Layer ─────────────────────────────────────────────────────────────

def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _load_cache() -> dict:
    _ensure_data_dir()
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_cache(cache: dict):
    _ensure_data_dir()
    now = datetime.now(timezone.utc)
    ttl = timedelta(days=CACHE_TTL_DAYS)
    valid = {}
    for key, entry in cache.items():
        try:
            cached_at = datetime.fromisoformat(entry.get("cached_at", ""))
            if now - cached_at < ttl:
                valid[key] = entry
        except (ValueError, TypeError):
            continue
    if len(valid) > MAX_CACHE_ENTRIES:
        sorted_entries = sorted(valid.items(), key=lambda x: x[1].get("cached_at", ""), reverse=True)
        valid = dict(sorted_entries[:MAX_CACHE_ENTRIES])
    with open(CACHE_FILE, "w") as f:
        json.dump(valid, f, indent=2, default=str)


def _cache_key(query: str) -> str:
    normalized = re.sub(r'[^a-z0-9\s]', '', query.lower().strip())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return hashlib.md5(normalized.encode()).hexdigest()


def _cache_lookup(query: str) -> Optional[dict]:
    cache = _load_cache()
    key = _cache_key(query)
    entry = cache.get(key)
    if not entry:
        return None
    try:
        cached_at = datetime.fromisoformat(entry["cached_at"])
        if datetime.now(timezone.utc) - cached_at > timedelta(days=CACHE_TTL_DAYS):
            return None
    except (ValueError, TypeError):
        return None
    return entry


def _cache_store(query: str, result: dict):
    cache = _load_cache()
    key = _cache_key(query)
    result["cached_at"] = datetime.now(timezone.utc).isoformat()
    result["query"] = query
    cache[key] = result
    _save_cache(cache)


# ─── MFG Info Extraction ────────────────────────────────────────────────────

def _extract_mfg_info(title: str, asin: str = "") -> dict:
    """Extract manufacturer name and part/model number from product title.

    Returns: {"manufacturer": str, "mfg_number": str, "item_number": str}
    """
    mfg = {"manufacturer": "", "mfg_number": "", "item_number": asin}
    if not title:
        return mfg

    parts = title.split(",")[0].split(" - ")[0].strip()

    model_patterns = re.findall(
        r'\b([A-Z]{1,5}[-]?\d{2,}[A-Z0-9-]*)\b'
        r'|'
        r'\b(\d{2,}[-][A-Z0-9]{2,}[-]?[A-Z0-9]*)\b'
        r'|'
        r'\b([A-Z]{2,}\d+[A-Z]+\d*)\b',
        title
    )
    models = [m for groups in model_patterns for m in groups if m and len(m) >= 4]

    if models:
        mfg["mfg_number"] = models[0]

    brand_match = re.match(r'^([A-Za-z][A-Za-z\s&.]{1,25}?)(?:\s+[-–]|\s+[A-Z0-9]{2,}\d|\s*,)', title)
    if brand_match:
        mfg["manufacturer"] = brand_match.group(1).strip()
    elif parts:
        words = parts.split()[:2]
        mfg["manufacturer"] = " ".join(words)

    # NEVER use ASIN as a part/MFG number — procurement requires real MFG#
    if mfg["mfg_number"]:
        mfg["item_number"] = mfg["mfg_number"]
    else:
        mfg["item_number"] = ""

    return mfg


# ─── Grok-Powered Product Search ────────────────────────────────────────────

def _grok_search(query: str) -> Optional[dict]:
    """Search for a product using Grok with built-in web search.

    Returns dict with: product_name, price, url, asin, supplier, confidence
    or None on failure.
    """
    api_key = XAI_API_KEY or os.environ.get("XAI_API_KEY", "")
    if not api_key or not HAS_REQUESTS:
        return None

    prompt = f"""Find this product on Amazon or any online retailer. Return the current retail price.

Product: {query}

Search for this exact product. I need:
1. The exact product name/title as listed
2. The price (USD)
3. The product URL (prefer Amazon.com)
4. The ASIN if it's on Amazon (10-character code starting with B0)
5. The supplier/retailer name

Respond in this exact JSON format only, no other text:
{{"product_name": "exact title", "price": 12.99, "url": "https://...", "asin": "B0XXXXXXXX", "supplier": "Amazon", "confidence": 0.85}}
If you cannot find the product, respond: {{"product_name": "", "price": 0, "url": "", "asin": "", "supplier": "", "confidence": 0}}"""

    try:
        resp = requests.post(
            XAI_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": XAI_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a product research assistant. Always respond with valid JSON only. No markdown formatting."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 500,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
        if resp.status_code == 429:
            log.warning("Grok rate limited in product research")
            return None
        if resp.status_code != 200:
            log.warning("Grok API %d: %s", resp.status_code, resp.text[:200])
            return None

        data = resp.json()
        text = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

        # Strip markdown code blocks
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                log.warning("Grok response not JSON: %s", text[:200])
                return None

        # Normalize
        result["price"] = float(result.get("price") or 0)
        result["confidence"] = float(result.get("confidence") or 0)
        result.setdefault("product_name", "")
        result.setdefault("url", "")
        result.setdefault("asin", "")
        result.setdefault("supplier", "")

        # Extract ASIN from URL if not provided
        if not result["asin"] and "amazon.com" in result.get("url", ""):
            asin_m = re.search(r'/dp/([A-Z0-9]{10})', result["url"])
            if asin_m:
                result["asin"] = asin_m.group(1)

        return result

    except Exception as e:
        log.error("Grok product search error: %s", e)
        return None


def search_amazon(query: str, max_results: int = 5) -> list:
    """
    Search for products by query. Uses Grok with web search.

    Returns list of dicts:
        [{"title": str, "price": float, "asin": str, "url": str, "source": "amazon"}, ...]
    """
    if not HAS_REQUESTS:
        log.warning("requests not available")
        return []

    # Check cache first
    cached = _cache_lookup(query)
    if cached and cached.get("found"):
        r = cached
        return [{
            "title": r.get("title", ""),
            "price": r.get("price", 0),
            "asin": r.get("asin", ""),
            "url": r.get("url", ""),
            "source": "amazon",
            "manufacturer": r.get("manufacturer", ""),
            "mfg_number": r.get("mfg_number", ""),
            "item_number": r.get("item_number", ""),
        }]

    log.info("Product search: '%s'", query)
    result = _grok_search(query)

    if not result or result.get("price", 0) <= 0:
        return []

    title = result.get("product_name", "")[:200]
    asin = result.get("asin", "")
    url = result.get("url", "")
    if not url and asin:
        url = f"https://www.amazon.com/dp/{asin}"

    mfg_info = _extract_mfg_info(title, asin)

    items = [{
        "title": title,
        "price": result["price"],
        "asin": asin,
        "url": url,
        "source": "amazon",
        "manufacturer": mfg_info.get("manufacturer", ""),
        "mfg_number": mfg_info.get("mfg_number", ""),
        "item_number": mfg_info.get("item_number", asin),
        "photo_url": "",
    }]

    # Cache the result
    _cache_store(query, {
        "found": True,
        "price": result["price"],
        "title": title,
        "source": "amazon",
        "url": url,
        "asin": asin,
        "manufacturer": mfg_info.get("manufacturer", ""),
        "mfg_number": mfg_info.get("mfg_number", ""),
        "item_number": mfg_info.get("item_number", ""),
    })

    return items[:max_results]


def lookup_amazon_product(asin: str) -> Optional[dict]:
    """Look up a product by ASIN. Uses Grok with web search.
    Returns dict with title, price, asin, url, etc. or None."""
    if not asin:
        return None

    # Check cache
    cached = _cache_lookup(f"asin:{asin}")
    if cached and cached.get("found"):
        return cached

    result = _grok_search(f"Amazon ASIN {asin}")
    if not result or result.get("price", 0) <= 0:
        return None

    title = result.get("product_name", "")[:200]
    price = result["price"]
    url = result.get("url", "") or f"https://www.amazon.com/dp/{asin}"

    mfg_info = _extract_mfg_info(title, asin)
    output = {
        "title": title,
        "price": price,
        "list_price": price,  # Grok returns retail price
        "sale_price": None,
        "asin": asin,
        "url": url,
        "source": "amazon_product",
        "manufacturer": mfg_info.get("manufacturer", ""),
        "mfg_number": mfg_info.get("mfg_number", ""),
        "found": True,
    }

    _cache_store(f"asin:{asin}", output)
    log.info("Product lookup: %s → $%.2f '%s'", asin, price, title[:50])
    return output


# ─── Product Research (main entry point) ─────────────────────────────────────

def research_product(
    item_number: str = "",
    description: str = "",
    use_cache: bool = True,
) -> dict:
    """
    Research a single product. Checks cache first, then searches via Grok.

    Returns:
        {
            "found": bool,
            "price": float or None,
            "title": str,
            "source": "cache" | "amazon" | None,
            "url": str,
            "asin": str,
            "alternatives": [...],
            "searched": str,
        }
    """
    query = _build_search_query(item_number, description)
    if not query:
        return {"found": False, "price": None, "source": None, "searched": "",
                "error": "No searchable description"}

    # 1. Check cache
    if use_cache:
        cached = _cache_lookup(query)
        if cached and cached.get("found"):
            log.info("Cache hit for: %s", query)
            cached["source"] = "cache"
            return cached

    # 2. Search via Grok
    results = search_amazon(query, max_results=5)

    if results:
        best = results[0]
        result = {
            "found": True,
            "price": best["price"],
            "title": best["title"],
            "source": "amazon",
            "url": best["url"],
            "asin": best.get("asin", ""),
            "manufacturer": best.get("manufacturer", ""),
            "mfg_number": best.get("mfg_number", ""),
            "item_number": best.get("item_number", best.get("asin", "")),
            "alternatives": results[1:] if len(results) > 1 else [],
            "searched": query,
        }
        _cache_store(query, result)
        return result

    # 3. Try simplified search
    short_query = _simplify_query(query)
    if short_query and short_query != query:
        results = search_amazon(short_query, max_results=3)
        if results:
            best = results[0]
            result = {
                "found": True,
                "price": best["price"],
                "title": best["title"],
                "source": "amazon",
                "url": best["url"],
                "asin": best.get("asin", ""),
                "manufacturer": best.get("manufacturer", ""),
                "mfg_number": best.get("mfg_number", ""),
                "item_number": best.get("item_number", best.get("asin", "")),
                "alternatives": results[1:] if len(results) > 1 else [],
                "searched": f"{query} → fallback: {short_query}",
            }
            _cache_store(query, result)
            return result

    # 4. Nothing found
    not_found = {
        "found": False, "price": None, "source": None,
        "title": "", "url": "", "asin": "",
        "alternatives": [], "searched": query,
        "note": "No results found. Manual cost entry required.",
    }
    not_found["cached_at"] = (datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS - 1)).isoformat()
    _cache_store(query, not_found)
    return not_found


def _build_search_query(item_number: str = "", description: str = "") -> str:
    """Build a clean search query from item number and description."""
    parts = []

    if description:
        desc = description.strip()
        first_line = desc.split("\n")[0].strip()

        clean = re.sub(
            r'\b(qty|quantity|each|per|unit|uom|set|pkg|package|box|case|'
            r'item\s*#?\s*|part\s*#?\s*|mfr\s*#?\s*|mfg\s*#?\s*|'
            r'solicitation|rfq|bid|quote|delivery|ship\s*to)\b',
            ' ', first_line, flags=re.I
        )
        clean = re.sub(r'^\d+\s+', '', clean)
        clean = re.sub(r'[,;()\[\]{}#*]', ' ', clean)
        clean = ' '.join(clean.split()).strip()

        if clean:
            if len(clean) > 60:
                words = clean.split()
                clean = ' '.join(words[:8])
            parts.append(clean)

    if item_number:
        item = item_number.strip()
        if not re.match(r'^\d{4}-\d{3}-\d{3}$', item):
            parts.append(item)

    return ' '.join(parts).strip()


def _simplify_query(query: str) -> str:
    """Reduce a query to its core 2-3 words for broader search."""
    words = query.split()
    meaningful = [w for w in words if len(w) > 3
                  and w.lower() not in {"with", "from", "that", "this", "have", "been"}]
    if len(meaningful) >= 2:
        return ' '.join(meaningful[:3])
    return ""


# ─── Bulk Research for RFQ ───────────────────────────────────────────────────

def research_rfq_items(rfq_data: dict) -> dict:
    """Research prices for all line items in an RFQ."""
    line_items = rfq_data.get("line_items", [])
    results = []
    found = 0
    cached = 0
    not_found = 0

    with _status_lock:
        RESEARCH_STATUS.update({
            "running": True, "progress": "starting",
            "items_done": 0, "items_total": len(line_items),
            "prices_found": 0, "errors": [],
            "started_at": datetime.now().isoformat(), "finished_at": None,
        })

    for idx, item in enumerate(line_items):
        with _status_lock:
            RESEARCH_STATUS["items_done"] = idx
            RESEARCH_STATUS["progress"] = f"Researching: {item.get('description', '')[:50]}"

        if item.get("supplier_cost") and item["supplier_cost"] > 0:
            results.append({
                "line_number": item.get("line_number"),
                "found": True, "price": item["supplier_cost"],
                "source": "existing", "searched": "skipped — already has cost",
            })
            found += 1
            continue

        try:
            result = research_product(
                item_number=item.get("item_number", ""),
                description=item.get("description", ""),
            )
            result["line_number"] = item.get("line_number")
            results.append(result)

            if result.get("found"):
                found += 1
                if result.get("source") == "cache":
                    cached += 1
                item["supplier_cost"] = result["price"]
                item["supplier_source"] = result["source"]
                item["supplier_url"] = result.get("url", "")
                item["item_link"] = result.get("url", "")
                item["item_supplier"] = result.get("source", "")
                with _status_lock:
                    RESEARCH_STATUS["prices_found"] = found
            else:
                not_found += 1

        except Exception as e:
            log.error("Research error item %d: %s", idx, e)
            with _status_lock:
                RESEARCH_STATUS["errors"].append(str(e))
            results.append({
                "line_number": item.get("line_number"),
                "found": False, "error": str(e),
            })
            not_found += 1

        if idx < len(line_items) - 1:
            time.sleep(0.5)

    with _status_lock:
        RESEARCH_STATUS.update({
            "running": False, "progress": "complete",
            "items_done": len(line_items), "prices_found": found,
            "finished_at": datetime.now().isoformat(),
        })

    return {
        "rfq_id": rfq_data.get("solicitation_number", "") or "RFQ",
        "items": results,
        "summary": {
            "total": len(line_items),
            "found": found,
            "cached": cached,
            "not_found": not_found,
        },
    }


# ─── Single Item Lookup (for dashboard) ──────────────────────────────────────

def quick_lookup(query: str) -> dict:
    """Simple search: pass any text, get back product results."""
    cached = _cache_lookup(query)
    if cached and cached.get("found"):
        cached["source"] = "cache"
        return cached
    results = search_amazon(query, max_results=5)
    if results:
        result = {
            "found": True,
            "price": results[0]["price"],
            "title": results[0]["title"],
            "source": "amazon",
            "url": results[0]["url"],
            "asin": results[0].get("asin", ""),
            "alternatives": results[1:],
            "searched": query,
        }
        _cache_store(query, result)
        return result
    return {"found": False, "price": None, "source": None, "searched": query}


# ─── Cache Stats ─────────────────────────────────────────────────────────────

def get_research_cache_stats() -> dict:
    cache = _load_cache()
    if not cache:
        return {"total_entries": 0, "found": 0, "not_found": 0, "sources": {}}
    found = sum(1 for e in cache.values() if e.get("found"))
    sources = {}
    for e in cache.values():
        s = e.get("source", "unknown")
        sources[s] = sources.get(s, 0) + 1
    return {
        "total_entries": len(cache),
        "found": found,
        "not_found": len(cache) - found,
        "sources": sources,
    }


# ─── Test ────────────────────────────────────────────────────────────────────

def test_amazon_search(query: str = "nitrile exam gloves") -> dict:
    """Test product search connectivity."""
    try:
        api_key = XAI_API_KEY or os.environ.get("XAI_API_KEY", "")
        results = search_amazon(query, max_results=3)

        return {
            "query": query,
            "results_count": len(results),
            "results": results,
            "status": "ok" if results else "no_results",
            "debug": {
                "has_api_key": bool(api_key),
                "engine": "grok_web_search",
            },
        }
    except Exception as e:
        import traceback
        return {"query": query, "error": str(e), "traceback": traceback.format_exc(), "status": "error"}
