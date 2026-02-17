import sys
from pathlib import Path

# Compatibility for refactored structure
sys.path.insert(0, str(Path(__file__).parent.parent))


"""
product_research.py — Product Research Agent for Reytech RFQ Automation
Phase 6 | Version: 6.1.1

Searches Amazon via SerpApi to find product prices for items that SCPRS
doesn't have. Results feed into the Pricing Oracle as supplier_cost data.

Architecture:
  1. Check local cache (7-day TTL)
  2. Search Amazon via SerpApi → extract price, ASIN, product title
  3. Cache result
  4. Feed into pricing_oracle.recommend_price() as supplier_cost

Dependencies: requests (already in requirements.txt)
Requires: SERPAPI_KEY environment variable set in Railway
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlencode

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

SERPAPI_BASE = "https://serpapi.com/search.json"
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")

# Fallback: read key from volume file (bypasses Railway env var issues)
_KEY_FILE = os.path.join(DATA_DIR, ".serpapi_key")

def _get_api_key() -> str:
    """Get SerpApi key from env var or volume file."""
    key = SERPAPI_KEY or os.environ.get("SERPAPI_KEY", "")
    if key:
        return key
    try:
        if os.path.exists(_KEY_FILE):
            with open(_KEY_FILE) as f:
                key = f.read().strip()
                if key:
                    return key
    except Exception:
        pass
    return ""

RESEARCH_STATUS = {
    "running": False, "progress": "", "items_done": 0, "items_total": 0,
    "prices_found": 0, "errors": [], "started_at": None, "finished_at": None,
}


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


# ─── SerpApi Amazon Search ──────────────────────────────────────────────────

def _extract_price(item: dict) -> Optional[float]:
    """Extract price from a SerpApi organic result item."""
    # Try extracted_price first (numeric)
    ep = item.get("extracted_price")
    if ep is not None and isinstance(ep, (int, float)) and ep > 0:
        return float(ep)

    # Try price dict (SerpApi sometimes nests it)
    price_obj = item.get("price")
    if isinstance(price_obj, dict):
        val = price_obj.get("extracted_price") or price_obj.get("raw") or price_obj.get("current")
        if val:
            try:
                cleaned = str(val).replace("$", "").replace(",", "").strip()
                return float(cleaned)
            except (ValueError, TypeError):
                pass
    elif isinstance(price_obj, (int, float)) and price_obj > 0:
        return float(price_obj)
    elif isinstance(price_obj, str) and price_obj:
        try:
            cleaned = price_obj.replace("$", "").replace(",", "").strip()
            if cleaned:
                return float(cleaned)
        except (ValueError, TypeError):
            pass

    # Try price_raw or price_string
    for field in ("price_raw", "price_string"):
        price_str = item.get(field, "")
        if price_str:
            m = re.search(r'\$?([\d,]+\.?\d{0,2})', str(price_str).replace(',', ''))
            if m:
                try:
                    return float(m.group(1))
                except (ValueError, TypeError):
                    pass

    return None


def _extract_mfg_info(title: str, asin: str = "") -> dict:
    """Extract manufacturer name and part/model number from Amazon title.
    
    Amazon titles typically follow: 'Brand ModelXYZ - Description...'
    Returns: {"manufacturer": str, "mfg_number": str, "item_number": str}
    """
    mfg = {"manufacturer": "", "mfg_number": "", "item_number": asin}
    if not title:
        return mfg
    
    # Common patterns: "Brand Model# - desc" or "Brand - Model# desc"
    parts = title.split(",")[0].split(" - ")[0].strip()
    
    # Extract potential part/model numbers (alphanumeric with dashes, 4+ chars)
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
    
    # Brand = first word(s) of title
    brand_match = re.match(r'^([A-Za-z][A-Za-z\s&.]{1,25}?)(?:\s+[-–]|\s+[A-Z0-9]{2,}\d|\s*,)', title)
    if brand_match:
        mfg["manufacturer"] = brand_match.group(1).strip()
    elif parts:
        words = parts.split()[:2]
        mfg["manufacturer"] = " ".join(words)
    
    if not mfg["mfg_number"]:
        mfg["item_number"] = asin
    else:
        mfg["item_number"] = mfg["mfg_number"]
    
    return mfg


def search_amazon(query: str, max_results: int = 5) -> list:
    """
    Search Amazon via SerpApi and extract product prices.

    Returns list of dicts:
        [{"title": str, "price": float, "asin": str, "url": str, "source": "amazon"}, ...]
    """
    if not HAS_REQUESTS:
        log.warning("requests not available")
        return []

    api_key = _get_api_key()
    if not api_key:
        log.error("SERPAPI_KEY not set")
        return []

    params = {
        "engine": "amazon",
        "k": query,
        "amazon_domain": "amazon.com",
        "api_key": api_key,
        "output": "json",
    }

    results = []
    try:
        url = f"{SERPAPI_BASE}?{urlencode(params)}"
        log.info(f"SerpApi Amazon search: '{query}'")
        resp = requests.get(url, timeout=30)

        if resp.status_code != 200:
            log.warning(f"SerpApi returned {resp.status_code}: {resp.text[:200]}")
            return []

        data = resp.json()

        if "error" in data:
            log.warning(f"SerpApi error: {data['error']}")
            return []

        # Parse organic_results
        organic = data.get("organic_results", [])
        log.info(f"SerpApi: {len(organic)} organic results for '{query}'")

        for item in organic[:max_results * 2]:
            asin = item.get("asin", "")
            title = item.get("title", "")
            if not title:
                continue

            price = _extract_price(item)
            if price is None or price <= 0 or price > 100000:
                continue

            link = item.get("link", "")
            if not link and asin:
                link = f"https://www.amazon.com/dp/{asin}"

            mfg_info = _extract_mfg_info(title, asin)
            results.append({
                "title": title[:200],
                "price": price,
                "asin": asin,
                "url": link,
                "source": "amazon",
                "rating": item.get("rating"),
                "reviews": item.get("reviews"),
                "manufacturer": mfg_info.get("manufacturer", ""),
                "mfg_number": mfg_info.get("mfg_number", ""),
                "item_number": mfg_info.get("item_number", asin),
            })

            if len(results) >= max_results:
                break

    except requests.exceptions.Timeout:
        log.warning("SerpApi request timed out")
    except Exception as e:
        log.error(f"SerpApi search error: {e}", exc_info=True)

    return results


# ─── Product Research (main entry point) ─────────────────────────────────────

def research_product(
    item_number: str = "",
    description: str = "",
    use_cache: bool = True,
) -> dict:
    """
    Research a single product. Checks cache first, then searches via SerpApi.

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
            log.info(f"Cache hit for: {query}")
            cached["source"] = "cache"
            return cached

    # 2. Search Amazon via SerpApi
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
            "rating": best.get("rating"),
            "reviews": best.get("reviews"),
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
        "note": "No Amazon results. Manual cost entry required.",
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
    """
    Research prices for all line items in an RFQ.
    Runs sequentially with delays to respect SerpApi rate limits.
    """
    line_items = rfq_data.get("line_items", [])
    results = []
    found = 0
    cached = 0
    not_found = 0

    RESEARCH_STATUS.update({
        "running": True, "progress": "starting",
        "items_done": 0, "items_total": len(line_items),
        "prices_found": 0, "errors": [],
        "started_at": datetime.now().isoformat(), "finished_at": None,
    })

    for idx, item in enumerate(line_items):
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
                RESEARCH_STATUS["prices_found"] = found
            else:
                not_found += 1

        except Exception as e:
            log.error(f"Research error item {idx}: {e}")
            RESEARCH_STATUS["errors"].append(str(e))
            results.append({
                "line_number": item.get("line_number"),
                "found": False, "error": str(e),
            })
            not_found += 1

        # SerpApi basic plan: ~1 req/sec. Use 1.5s to be safe.
        # Cached hits don't count toward limit.
        if idx < len(line_items) - 1:
            time.sleep(1.5)

    RESEARCH_STATUS.update({
        "running": False, "progress": "complete",
        "items_done": len(line_items), "prices_found": found,
        "finished_at": datetime.now().isoformat(),
    })

    return {
        "rfq_id": rfq_data.get("solicitation_number", "unknown"),
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
    """Simple search: pass any text, get back Amazon results."""
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
    """Test SerpApi Amazon search connectivity and parsing."""
    try:
        api_key = _get_api_key()

        # Debug: find any env vars with SERP or API_KEY in the name
        env_keys = [k for k in os.environ.keys() if "SERP" in k.upper() or "API_KEY" in k.upper()]

        results = search_amazon(query, max_results=3)

        debug = {
            "has_api_key": bool(api_key),
            "api_key_preview": f"{api_key[:8]}..." if api_key else "NOT SET",
            "key_source": "env" if os.environ.get("SERPAPI_KEY") else ("file" if api_key else "none"),
            "engine": "serpapi_amazon",
            "env_keys_matching": env_keys,
            "total_env_vars": len(os.environ),
        }

        return {
            "query": query,
            "results_count": len(results),
            "results": results,
            "status": "ok" if results else "no_results",
            "debug": debug,
        }
    except Exception as e:
        import traceback
        return {"query": query, "error": str(e), "traceback": traceback.format_exc(), "status": "error"}
