"""
product_research.py — Product Research Agent for Reytech RFQ Automation
Phase 6 | Version: 6.1

Searches supplier sites (starting with Amazon) to find product prices
for items that SCPRS doesn't have. Results feed into the Pricing Oracle
as supplier_cost data.

Architecture:
  1. Check local cache (7-day TTL)
  2. Search Amazon → extract price, ASIN, product title
  3. Cache result
  4. Feed into pricing_oracle.recommend_price() as supplier_cost

Fallback chain: Cache → Amazon search → manual entry required

Dependencies: requests, beautifulsoup4 (already in requirements.txt)
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import quote_plus

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_WEB = True
except ImportError:
    HAS_WEB = False

log = logging.getLogger("research")

# ─── Configuration ───────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CACHE_FILE = os.path.join(DATA_DIR, "product_research_cache.json")
CACHE_TTL_DAYS = 7
MAX_CACHE_ENTRIES = 5000

# Amazon search config
AMAZON_SEARCH_URL = "https://www.amazon.com/s"
AMAZON_PRODUCT_URL = "https://www.amazon.com/dp/{asin}"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
]

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
    # Evict expired entries + enforce max size
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
    # LRU eviction if still too large
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
    # Check TTL
    try:
        cached_at = datetime.fromisoformat(entry["cached_at"])
        if datetime.now(timezone.utc) - cached_at > timedelta(days=CACHE_TTL_DAYS):
            return None  # Expired
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


# ─── Amazon Search ───────────────────────────────────────────────────────────

def _get_session() -> requests.Session:
    """Create a session that mimics a real browser."""
    import random
    s = requests.Session()
    ua = random.choice(USER_AGENTS)
    s.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    })
    return s


def _extract_price_from_text(text: str) -> Optional[float]:
    """Extract a dollar price from text like '$12.99' or '12.99'."""
    if not text:
        return None
    # Match patterns like $1,234.56 or $12.99
    m = re.search(r'\$?([\d,]+\.?\d{0,2})', text.replace(',', ''))
    if m:
        try:
            val = float(m.group(1))
            if 0.01 < val < 100000:  # Sanity check
                return val
        except (ValueError, TypeError):
            pass
    return None


def search_amazon(query: str, max_results: int = 5) -> list:
    """
    Search Amazon for a product and extract prices from search results.

    Returns list of dicts:
        [{"title": str, "price": float, "asin": str, "url": str, "source": "amazon"}, ...]
    """
    if not HAS_WEB:
        log.warning("requests/bs4 not available for Amazon search")
        return []

    session = _get_session()
    results = []

    try:
        # Search Amazon
        params = {"k": query, "ref": "nb_sb_noss"}
        log.info(f"Amazon search: '{query}'")
        resp = session.get(AMAZON_SEARCH_URL, params=params, timeout=15, allow_redirects=True)

        if resp.status_code != 200:
            log.warning(f"Amazon returned {resp.status_code}")
            return []

        html = resp.text

        # Check for CAPTCHA / bot detection
        if "captcha" in html.lower() or "robot" in html.lower() or len(html) < 5000:
            log.warning("Amazon CAPTCHA detected or blocked")
            return []

        soup = BeautifulSoup(html, "html.parser")

        # Find search result items
        # Amazon uses data-asin attribute on result divs
        items = soup.find_all("div", attrs={"data-asin": True, "data-component-type": "s-search-result"})
        if not items:
            # Fallback: try other selectors
            items = soup.find_all("div", attrs={"data-asin": True})
            items = [i for i in items if i.get("data-asin", "").strip()]

        log.info(f"Amazon: {len(items)} result items found")

        for item in items[:max_results * 2]:  # Check more than needed, skip priceless
            asin = item.get("data-asin", "").strip()
            if not asin or len(asin) != 10:
                continue

            # Extract title
            title_el = (
                item.find("span", class_="a-text-normal")
                or item.find("h2")
                or item.find("span", class_=re.compile(r"a-size-medium"))
            )
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            # Extract price — try multiple selectors
            price = None

            # Method 1: Price whole + fraction
            whole = item.find("span", class_="a-price-whole")
            fraction = item.find("span", class_="a-price-fraction")
            if whole:
                price_text = whole.get_text(strip=True).rstrip(".")
                if fraction:
                    price_text += "." + fraction.get_text(strip=True)
                price = _extract_price_from_text(price_text)

            # Method 2: a-price span with a-offscreen
            if price is None:
                price_span = item.find("span", class_="a-price")
                if price_span:
                    offscreen = price_span.find("span", class_="a-offscreen")
                    if offscreen:
                        price = _extract_price_from_text(offscreen.get_text(strip=True))

            # Method 3: Any dollar amount in price area
            if price is None:
                price_area = item.find("div", class_=re.compile(r"a-row.*a-spacing"))
                if price_area:
                    price = _extract_price_from_text(price_area.get_text())

            if price is None:
                continue  # Skip items with no price

            results.append({
                "title": title[:200],
                "price": price,
                "asin": asin,
                "url": f"https://www.amazon.com/dp/{asin}",
                "source": "amazon",
            })

            if len(results) >= max_results:
                break

    except requests.exceptions.Timeout:
        log.warning("Amazon search timed out")
    except Exception as e:
        log.error(f"Amazon search error: {e}", exc_info=True)

    return results


# ─── Product Research (main entry point) ─────────────────────────────────────

def research_product(
    item_number: str = "",
    description: str = "",
    use_cache: bool = True,
) -> dict:
    """
    Research a single product. Checks cache first, then searches Amazon.

    Returns:
        {
            "found": bool,
            "price": float or None,
            "title": str,
            "source": "cache" | "amazon" | None,
            "url": str,
            "asin": str,
            "alternatives": [...],  # other prices found
            "searched": str,        # what was searched
        }
    """
    # Build search query from description
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

    # 2. Search Amazon
    results = search_amazon(query, max_results=5)

    if results:
        best = results[0]  # First result is usually most relevant
        result = {
            "found": True,
            "price": best["price"],
            "title": best["title"],
            "source": "amazon",
            "url": best["url"],
            "asin": best.get("asin", ""),
            "alternatives": results[1:] if len(results) > 1 else [],
            "searched": query,
        }
        _cache_store(query, result)
        return result

    # 3. Try simplified search (fewer words)
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
    # Cache the miss too (shorter TTL — 1 day)
    not_found["cached_at"] = (datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS - 1)).isoformat()
    _cache_store(query, not_found)
    return not_found


def _build_search_query(item_number: str = "", description: str = "") -> str:
    """Build a clean search query from item number and description."""
    parts = []

    if description:
        desc = description.strip()
        # Take first line only
        first_line = desc.split("\n")[0].strip()

        # Extract manufacturer/brand and product name
        # Remove common state procurement jargon
        clean = re.sub(
            r'\b(qty|quantity|each|per|unit|uom|set|pkg|package|box|case|'
            r'item\s*#?\s*|part\s*#?\s*|mfr\s*#?\s*|mfg\s*#?\s*|'
            r'solicitation|rfq|bid|quote|delivery|ship\s*to)\b',
            ' ', first_line, flags=re.I
        )
        # Remove pure numbers like sizes/quantities at start
        clean = re.sub(r'^\d+\s+', '', clean)
        # Remove special chars
        clean = re.sub(r'[,;()\[\]{}#*]', ' ', clean)
        # Collapse whitespace
        clean = ' '.join(clean.split()).strip()

        if clean:
            # Cap at ~60 chars for search
            if len(clean) > 60:
                words = clean.split()
                clean = ' '.join(words[:8])
            parts.append(clean)

    # Add item number only if it looks like a real product code, not a state code
    if item_number:
        item = item_number.strip()
        # Skip state item numbers like "6500-001-430" (X-XXX-XXX pattern)
        if not re.match(r'^\d{4}-\d{3}-\d{3}$', item):
            parts.append(item)

    return ' '.join(parts).strip()


def _simplify_query(query: str) -> str:
    """Reduce a query to its core 2-3 words for broader search."""
    words = query.split()
    # Keep only words > 3 chars, take first 3
    meaningful = [w for w in words if len(w) > 3
                  and w.lower() not in {"with", "from", "that", "this", "have", "been"}]
    if len(meaningful) >= 2:
        return ' '.join(meaningful[:3])
    return ""


# ─── Bulk Research for RFQ ───────────────────────────────────────────────────

def research_rfq_items(rfq_data: dict) -> dict:
    """
    Research prices for all line items in an RFQ.
    Runs sequentially with delays to avoid Amazon rate limits.

    Returns:
        {
            "rfq_id": str,
            "items": [research_result for each item],
            "summary": {"total": int, "found": int, "cached": int, "not_found": int}
        }
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

        # Skip if already has supplier cost
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
                # Update the item's supplier cost
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

        # Rate limit: 2-4 seconds between Amazon searches
        if idx < len(line_items) - 1:
            import random
            time.sleep(random.uniform(2.0, 4.0))

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
    """Test Amazon search connectivity and parsing."""
    try:
        results = search_amazon(query, max_results=3)
        return {
            "query": query,
            "results_count": len(results),
            "results": results,
            "status": "ok" if results else "no_results",
        }
    except Exception as e:
        return {"query": query, "error": str(e), "status": "error"}
