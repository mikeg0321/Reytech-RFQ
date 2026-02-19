"""
item_link_lookup.py — Supplier URL → Product Data

Paste any supplier URL → get back:
  - description / title
  - cost / price
  - part number / MFG number
  - shipping cost (if parseable)
  - supplier name

Supported suppliers (with structured parsing):
  amazon.com       → ASIN-based lookup via SerpApi
  grainger.com     → SKU from URL path
  mcmaster.com     → Part number from URL
  fishersci.com    → catalog number
  medline.com      → product code
  boundtree.com    → item number
  henryschein.com  → item SKU
  concordancehealth.com
  waxie.com
  staples.com
  Any other URL    → generic HTML title + meta price scrape
"""

import re, os, logging
from urllib.parse import urlparse

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

log = logging.getLogger("item_link")

# ─── Supplier detection ───────────────────────────────────────────────────────

SUPPLIER_MAP = {
    "amazon.com":            "Amazon",
    "grainger.com":          "Grainger",
    "mcmaster.com":          "McMaster-Carr",
    "fishersci.com":         "Fisher Scientific",
    "fishersci":             "Fisher Scientific",
    "medline.com":           "Medline",
    "boundtree.com":         "Bound Tree Medical",
    "henryschein.com":       "Henry Schein",
    "concordancehealth.com": "Concordance Healthcare",
    "concordancehealthcare": "Concordance Healthcare",
    "waxie.com":             "Waxie Sanitary Supply",
    "staples.com":           "Staples",
    "officedepot.com":       "Office Depot",
    "uline.com":             "Uline",
    "zoro.com":              "Zoro",
    "globalindustrial.com":  "Global Industrial",
    "safetycompany.com":     "The Safety Company",
    "vwr.com":               "VWR",
    "thermofisher.com":      "Thermo Fisher",
    "fastenal.com":          "Fastenal",
    "homedes.com":           "Home Depot",
    "homedepot.com":         "Home Depot",
    "lowes.com":             "Lowe's",
    "sysco.com":             "Sysco",
    "usfoods.com":           "US Foods",
    "performancefoodsvc.com":"Performance Food Group",
    "nambe.com":             "Nambé",
    "shoplet.com":           "Shoplet",
    "quill.com":             "Quill",
}

def detect_supplier(url: str) -> str:
    """Return supplier name from URL domain."""
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        for domain, name in SUPPLIER_MAP.items():
            if domain in host:
                return name
        # Capitalize the domain as fallback
        parts = host.split(".")
        return parts[-2].capitalize() if len(parts) >= 2 else host
    except Exception:
        return "Unknown"


def _extract_asin(url: str) -> str:
    """Extract Amazon ASIN from URL."""
    patterns = [
        r"/dp/([A-Z0-9]{10})",
        r"/gp/product/([A-Z0-9]{10})",
        r"ASIN=([A-Z0-9]{10})",
        r"/product/([A-Z0-9]{10})",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return ""


def _extract_grainger_sku(url: str) -> str:
    """Extract Grainger item number from URL."""
    # https://www.grainger.com/product/TITLE--XXXXXXXX
    m = re.search(r'--([A-Z0-9]{8,12})(?:\?|$|/)', url)
    if m:
        return m.group(1)
    # Also try /product/TITLE/p/XXXXXXXX
    m = re.search(r'/p/([A-Z0-9]{6,12})(?:\?|$)', url)
    if m:
        return m.group(1)
    return ""


def _extract_mcmaster_part(url: str) -> str:
    """Extract McMaster-Carr part number from URL."""
    m = re.search(r'/(\d{4,6}[A-Z]\d{0,4})(?:/|$|\?)', url)
    if m:
        return m.group(1)
    return ""


def _scrape_generic(url: str) -> dict:
    """
    Generic HTML scrape for title, price, description, part number.
    Uses simple regex patterns — works for most product pages.
    """
    if not HAS_REQUESTS:
        return {"error": "requests not available"}

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
        html = resp.text
    except requests.exceptions.Timeout:
        return {"error": "Request timed out"}
    except Exception as e:
        return {"error": str(e)}

    result = {}

    # Title
    m = re.search(r'<title[^>]*>([^<]{5,200})</title>', html, re.IGNORECASE)
    if m:
        title = m.group(1).strip()
        # Strip " - Grainger" " | Amazon" etc.
        title = re.split(r'\s*[|\-–]\s*(Grainger|Amazon|McMaster|Fisher|Medline|Bound Tree|Henry Schein|Uline|Zoro|Staples|Waxie)', title)[0].strip()
        result["title"] = title[:200]

    # Price — common patterns: $12.34 or "price":"12.34" or data-price="12.34"
    price = None
    # JSON-LD or data attributes
    price_patterns = [
        r'"price"\s*:\s*"?(\d{1,6}\.\d{2})"?',
        r'data-price\s*=\s*"(\d{1,6}\.\d{2})"',
        r'data-unit-price\s*=\s*"(\d{1,6}\.\d{2})"',
        r'itemprop\s*=\s*"price"[^>]*content\s*=\s*"(\d{1,6}\.\d{2})"',
        r'<span[^>]*class\s*=\s*"[^"]*price[^"]*"[^>]*>\s*\$?([\d,]+\.\d{2})',
        r'"unitPrice"\s*:\s*(\d+\.?\d*)',
        r'"listPrice"\s*:\s*"?\$?([\d,]+\.?\d*)"?',
    ]
    for pat in price_patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            try:
                v = float(m.group(1).replace(",", ""))
                if 0.01 < v < 100000:
                    price = v
                    break
            except Exception:
                pass
    if price:
        result["price"] = price

    # Part number / SKU / MFG number
    part_patterns = [
        r'"sku"\s*:\s*"([A-Z0-9\-]{4,30})"',
        r'"mpn"\s*:\s*"([A-Z0-9\-]{4,30})"',
        r'data-sku\s*=\s*"([A-Z0-9\-]{4,30})"',
        r'data-item-number\s*=\s*"([A-Z0-9\-]{4,30})"',
        r'[Ii]tem\s*#?:?\s*([A-Z0-9\-]{5,20})',
        r'[Mm]odel\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Mm][Ff][Gg]\.?\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Pp]art\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Cc]atalog\s*#?:?\s*([A-Z0-9\-]{4,20})',
    ]
    for pat in part_patterns:
        m = re.search(pat, html)
        if m:
            result["part_number"] = m.group(1).strip()
            break

    # Shipping — look for "free shipping" or "$X.XX shipping"
    ship_patterns = [
        r'[Ff]ree\s+[Ss]hipping',
        r'[Ss]hipping\s*[:\s]\s*\$?([\d]+\.?\d*)',
        r'[Ff]reight\s*[:\s]\s*\$?([\d]+\.?\d*)',
    ]
    for pat in ship_patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            if "Free" in pat or "free" in pat.lower():
                result["shipping"] = 0.0
                result["shipping_note"] = "Free shipping"
            elif m.lastindex and m.group(1):
                try:
                    result["shipping"] = float(m.group(1))
                except Exception:
                    pass
            break

    # Description — meta description
    m = re.search(r'<meta\s+name\s*=\s*"description"\s+content\s*=\s*"([^"]{10,400})"', html, re.IGNORECASE)
    if not m:
        m = re.search(r'<meta\s+content\s*=\s*"([^"]{20,400})"\s+name\s*=\s*"description"', html, re.IGNORECASE)
    if m:
        result["meta_description"] = m.group(1).strip()[:300]

    return result


# ─── Supplier-specific handlers ───────────────────────────────────────────────

def _lookup_amazon(url: str) -> dict:
    """Amazon: extract ASIN, use SerpApi product lookup."""
    asin = _extract_asin(url)
    if not asin:
        return {"error": "Could not extract ASIN from Amazon URL", "supplier": "Amazon"}

    try:
        from src.agents.product_research import search_amazon
        # Use ASIN as the search query for a direct hit
        results = search_amazon(f"ASIN {asin}", max_results=1)
        if results:
            r = results[0]
            return {
                "supplier":     "Amazon",
                "title":        r.get("title", ""),
                "description":  r.get("title", ""),
                "price":        r.get("price"),
                "cost":         r.get("price"),   # Amazon price = your cost
                "part_number":  asin,
                "mfg_number":   r.get("mfg_number", ""),
                "manufacturer": r.get("manufacturer", ""),
                "url":          url,
                "asin":         asin,
                "shipping":     0.0,
                "shipping_note": "Prime/standard shipping",
                "source":       "amazon_asin",
            }
        # Fallback: scrape the product page
        scraped = _scrape_generic(url)
        scraped["supplier"] = "Amazon"
        scraped["asin"] = asin
        scraped["part_number"] = scraped.get("part_number") or asin
        return scraped
    except Exception as e:
        return {"error": str(e), "supplier": "Amazon", "asin": asin}


def _lookup_grainger(url: str) -> dict:
    """Grainger: scrape product page."""
    sku = _extract_grainger_sku(url)
    result = _scrape_generic(url)
    result["supplier"] = "Grainger"
    if sku:
        result["part_number"] = result.get("part_number") or sku
        result["item_number"] = sku
    return result


def _lookup_mcmaster(url: str) -> dict:
    """McMaster-Carr: scrape product page."""
    part = _extract_mcmaster_part(url)
    result = _scrape_generic(url)
    result["supplier"] = "McMaster-Carr"
    if part:
        result["part_number"] = result.get("part_number") or part
    return result


# ─── Main entry point ─────────────────────────────────────────────────────────

def lookup_from_url(url: str) -> dict:
    """
    Given any supplier product URL, return structured product data.

    Returns:
      {
        supplier:     str,
        title:        str,
        description:  str,
        price:        float | None,   ← what you'd pay (cost to Reytech)
        part_number:  str,
        mfg_number:   str,
        manufacturer: str,
        shipping:     float | None,
        shipping_note: str,
        asin:         str,            ← Amazon only
        url:          str,
        error:        str | None,
      }
    """
    url = url.strip()
    if not url:
        return {"error": "No URL provided"}
    if not url.startswith("http"):
        url = "https://" + url

    supplier = detect_supplier(url)
    host = urlparse(url).netloc.lower()

    try:
        if "amazon.com" in host:
            result = _lookup_amazon(url)
        elif "grainger.com" in host:
            result = _lookup_grainger(url)
        elif "mcmaster.com" in host:
            result = _lookup_mcmaster(url)
        else:
            # Generic HTML scrape for all other suppliers
            result = _scrape_generic(url)
            result["supplier"] = supplier

        # Normalize
        result.setdefault("supplier", supplier)
        result.setdefault("url", url)
        result.setdefault("title", "")
        result.setdefault("description", result.get("title", ""))
        result.setdefault("part_number", "")
        result.setdefault("mfg_number", "")
        result.setdefault("manufacturer", "")
        result.setdefault("shipping", None)
        result.setdefault("shipping_note", "")

        # Use title as description if description is empty
        if not result["description"] and result["title"]:
            result["description"] = result["title"]
        # If only meta_description, use it
        if not result["description"] and result.get("meta_description"):
            result["description"] = result["meta_description"]

        result["ok"] = "error" not in result
        log.info("item_link_lookup: %s → supplier=%s price=%s part=%s",
                 url[:60], result["supplier"], result.get("price"), result.get("part_number"))
        return result

    except Exception as e:
        log.error("lookup_from_url %s: %s", url[:80], e)
        return {"ok": False, "error": str(e), "supplier": supplier, "url": url}
