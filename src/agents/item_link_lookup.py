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
    "ssww.com":              "S&S Worldwide",
    "aedstore.com":          "AED Store",
    "aed.com":               "AED Superstore",
    "aedbrands.com":         "AED Brands",
    "buyaedsusa.com":        "Buy AEDs USA",
    "lifesaversinc.com":     "Life Savers Inc",
    "moore.com":             "Moore Medical",
    "mooremedical.com":      "Moore Medical",
    "techlinemedical.com":   "TechLine Medical",
    "myotcstore.com":        "MyOTCStore",
    "allegromedical.com":    "Allegro Medical",
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


def _normalize_amazon_url(url: str) -> str:
    """Normalize any Amazon URL to clean canonical dp/ form.
    Strips tracking params, ref tags, search terms, affiliate codes."""
    asin = _extract_asin(url)
    if asin:
        return f"https://www.amazon.com/dp/{asin}"
    return url


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

    # Price — extract BOTH list price (non-discounted) and sale price
    # Default to list price for quoting (Amazon prices fluctuate)
    price = None
    list_price = None
    sale_price = None

    # List price (non-discounted) — check first
    for pat in [r'"listPrice"\s*:\s*"?\$?([\d,]+\.?\d*)"?',
                r'"was_price"\s*:\s*"?\$?([\d,]+\.?\d*)"?',
                r'<span[^>]*class\s*=\s*"[^"]*(?:list|was|original|strike|basis)[^"]*"[^>]*>\s*\$?([\d,]+\.\d{2})',
                r'"typicalPrice"\s*:\s*\{[^}]*"amount"\s*:\s*(\d+\.?\d*)']:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            try:
                v = float(m.group(1).replace(",", ""))
                if 0.01 < v < 100000:
                    list_price = v
                    break
            except Exception:
                pass

    # Sale/current price
    for pat in [r'"price"\s*:\s*"?(\d{1,6}\.\d{2})"?',
                r'data-price\s*=\s*"(\d{1,6}\.\d{2})"',
                r'data-unit-price\s*=\s*"(\d{1,6}\.\d{2})"',
                r'itemprop\s*=\s*"price"[^>]*content\s*=\s*"(\d{1,6}\.\d{2})"',
                r'<span[^>]*class\s*=\s*"[^"]*price[^"]*"[^>]*>\s*\$?([\d,]+\.\d{2})',
                r'"unitPrice"\s*:\s*(\d+\.?\d*)']:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            try:
                v = float(m.group(1).replace(",", ""))
                if 0.01 < v < 100000:
                    sale_price = v
                    break
            except Exception:
                pass

    # Use list price for quoting (stable), sale price as reference
    price = list_price or sale_price
    if price:
        result["price"] = price
    if list_price:
        result["list_price"] = list_price
    if sale_price and sale_price != list_price:
        result["sale_price"] = sale_price
        result["discount_pct"] = round((1 - sale_price / list_price) * 100, 1) if list_price else 0

    # Part number / SKU / MFG number
    part_patterns = [
        r'"sku"\s*:\s*"([A-Z0-9\-]{4,30})"',
        r'"mpn"\s*:\s*"([A-Z0-9\-]{4,30})"',
        r'"productID"\s*:\s*"([A-Z0-9\-]{4,30})"',
        r'data-sku\s*=\s*"([A-Z0-9\-]{4,30})"',
        r'data-item-number\s*=\s*"([A-Z0-9\-]{4,30})"',
        r'data-product-id\s*=\s*"([A-Z0-9\-]{4,30})"',
        r'[Ii]tem\s*#?:?\s*([A-Z0-9\-]{5,20})',
        r'[Mm]odel\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Mm][Ff][Gg]\.?\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Pp]art\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Cc]atalog\s*#?:?\s*([A-Z0-9\-]{4,20})',
        r'[Ss][Kk][Uu]\s*:?\s*([A-Z0-9\-]{4,20})',
    ]
    for pat in part_patterns:
        m = re.search(pat, html)
        if m:
            result["part_number"] = m.group(1).strip()
            break

    # MFG / manufacturer number (separate from part/SKU)
    mfg_patterns = [
        r'"mpn"\s*:\s*"([A-Z0-9\-]{3,30})"',
        r'[Mm]anufacturer\s*(?:#|[Nn]umber|[Pp]art)\s*:?\s*([A-Z0-9\-]{3,25})',
        r'[Mm][Ff][Gg]\s*(?:#|[Nn]o\.?|[Nn]umber)\s*:?\s*([A-Z0-9\-]{3,25})',
    ]
    for pat in mfg_patterns:
        m = re.search(pat, html)
        if m:
            result["mfg_number"] = m.group(1).strip()
            break

    # Manufacturer / brand name
    brand_patterns = [
        r'"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]{2,50})"',
        r'"brand"\s*:\s*"([^"]{2,50})"',
        r'[Bb]rand\s*:?\s*<[^>]*>([^<]{2,40})</[^>]*>',
        r'[Mm]anufacturer\s*:?\s*<[^>]*>([^<]{2,40})</[^>]*>',
    ]
    for pat in brand_patterns:
        m = re.search(pat, html)
        if m:
            result["manufacturer"] = m.group(1).strip()
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

    # Description — meta description + OG tags
    m = re.search(r'<meta\s+name\s*=\s*"description"\s+content\s*=\s*"([^"]{10,400})"', html, re.IGNORECASE)
    if not m:
        m = re.search(r'<meta\s+content\s*=\s*"([^"]{20,400})"\s+name\s*=\s*"description"', html, re.IGNORECASE)
    if m:
        result["meta_description"] = m.group(1).strip()[:300]
    
    # OG title and description (often better than <title>)
    og_title = re.search(r'<meta\s+(?:property|name)\s*=\s*"og:title"\s+content\s*=\s*"([^"]{5,300})"', html, re.IGNORECASE)
    if not og_title:
        og_title = re.search(r'<meta\s+content\s*=\s*"([^"]{5,300})"\s+(?:property|name)\s*=\s*"og:title"', html, re.IGNORECASE)
    if og_title and not result.get("title"):
        title = og_title.group(1).strip()
        title = re.split(r'\s*[|\-–]\s*(Grainger|Amazon|McMaster|Fisher|Medline|Bound Tree|Henry Schein|Uline|Zoro|Staples|Waxie)', title)[0].strip()
        result["title"] = title[:200]

    # JSON-LD product name (most structured/reliable)
    jld_name = re.search(r'"@type"\s*:\s*"Product"[^}]*"name"\s*:\s*"([^"]{5,200})"', html, re.IGNORECASE | re.DOTALL)
    if jld_name:
        result["title"] = jld_name.group(1).strip()[:200]

    # Product image — extract from JSON-LD, OG, or meta
    image_patterns = [
        r'"@type"\s*:\s*"Product"[^}]*"image"\s*:\s*"([^"]{10,500})"',
        r'<meta\s+(?:property|name)\s*=\s*"og:image"\s+content\s*=\s*"([^"]{10,500})"',
        r'<meta\s+content\s*=\s*"([^"]{10,500})"\s+(?:property|name)\s*=\s*"og:image"',
        r'"image"\s*:\s*\[\s*"([^"]{10,500})"',
        r'data-main-image\s*=\s*"([^"]{10,500})"',
        r'id\s*=\s*"[^"]*main[^"]*image[^"]*"\s+src\s*=\s*"([^"]{10,500})"',
    ]
    for pat in image_patterns:
        m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
        if m:
            img_url = m.group(1).strip()
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            if img_url.startswith("http"):
                result["photo_url"] = img_url
                break

    # UOM detection from description/title
    uom_patterns = [
        (r'\b(\d+)\s*(?:per|/)\s*(?:case|cs)\b', 'CS'),
        (r'\b(\d+)\s*(?:per|/)\s*(?:box|bx)\b', 'BX'),
        (r'\b(\d+)\s*(?:per|/)\s*(?:pack|pk)\b', 'PK'),
        (r'\beach\b', 'EA'),
        (r'\bper\s+pair\b', 'PR'),
        (r'\bper\s+roll\b', 'RL'),
    ]
    text_to_check = (result.get("title", "") + " " + result.get("meta_description", "")).lower()
    for pat, uom in uom_patterns:
        if re.search(pat, text_to_check, re.IGNORECASE):
            result["uom"] = uom
            break

    return result


# ─── Supplier-specific handlers ───────────────────────────────────────────────

def _lookup_amazon(url: str) -> dict:
    """Amazon: extract ASIN, use SerpApi product lookup."""
    asin = _extract_asin(url)
    if not asin:
        return {"error": "Could not extract ASIN from Amazon URL", "supplier": "Amazon"}

    # Detect ISBN (starts with digit, not B0-style ASIN) — use as MFG# for books
    _is_isbn = asin and asin[0].isdigit()

    try:
        from src.agents.product_research import search_amazon, lookup_amazon_product
        # Try direct product lookup first (most reliable for ASIN/ISBN)
        direct = lookup_amazon_product(asin)
        results = []
        if direct and direct.get("price") and direct["price"] > 0:
            results = [direct]
        else:
            # Fallback to search
            results = search_amazon(f"ASIN {asin}", max_results=1)
            if (not results or not results[0].get("price")) and _is_isbn:
                results = search_amazon(f"ISBN {asin}", max_results=1)
        if results:
            r = results[0]
            title = r.get("title", "")
            # Extract size from title if present (e.g., "...Scrub Set, X-Small")
            size = ""
            import re as _re
            size_match = _re.search(
                r',\s*(XX?-?(?:Small|Large)|(?:Small|Medium|Large|X-Large|XX-Large|XS|XL|XXL|S|M|L)\b)',
                title, _re.IGNORECASE
            )
            if size_match:
                size = size_match.group(1).strip()
            # Price: always use list/typical price — never sale/coupon price
            _list = r.get("list_price") or r.get("typical_price")
            _sale = r.get("price")
            _use_price = _list or _sale

            # MFG#: use scraped MFG#, or ISBN for books
            mfg = r.get("mfg_number", "") or r.get("part_number", "") or ""
            if not mfg and _is_isbn:
                mfg = asin  # ISBN-10 serves as MFG# for books
            # Description = clean title only. No ASIN — procurement doesn't want it.
            # MFG# stored in part_number field, ASIN in asin field only.
            structured_desc = title

            # If no MFG# yet, try product page specs
            if not mfg and asin and not _is_isbn:
                try:
                    _page = _scrape_generic(f"https://www.amazon.com/dp/{asin}")
                    import re as _re_mfg
                    _page_text = str(_page.get("raw_text", "") or _page.get("meta_description", ""))
                    _model_match = _re_mfg.search(
                        r'(?:Item model number|Part Number|Model Number|Manufacturer Part Number)'
                        r'[:\s]+([A-Z0-9][A-Z0-9\-/]{2,25})',
                        _page_text, _re_mfg.IGNORECASE)
                    if _model_match:
                        mfg = _model_match.group(1).strip()
                except Exception:
                    pass

            clean_url = _normalize_amazon_url(url)

            return {
                "supplier":      "Amazon",
                "title":         title,
                "description":   structured_desc,
                "price":         _use_price,
                "list_price":    _list,
                "sale_price":    _sale if _sale and _sale != _list else None,
                "cost":          _use_price,
                "part_number":   mfg,
                "mfg_number":    mfg,
                "manufacturer":  r.get("manufacturer", ""),
                "url":           clean_url,
                "original_url":  url,
                "asin":          asin,
                "size":          size,
                "shipping":      0.0,
                "shipping_note": "Prime/standard shipping — verify delivery window",
                "source":        "amazon_asin",
                "price_note":    f"List: ${_list:.2f}" if _list and _sale and _list != _sale else "",
            }
        # Fallback: scrape the product page
        scraped = _scrape_generic(url)
        scraped["supplier"] = "Amazon"
        scraped["asin"] = asin
        scraped["url"] = _normalize_amazon_url(url)
        # Description = title only, no ASIN appended
        _title = scraped.get("title") or scraped.get("description") or ""
        if _title:
            scraped["description"] = _title
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


def _extract_ssww_item(url: str):
    """Extract S&S Worldwide item# and description from URL slug.
    URL format: /item/{slug-with-dashes-ITEMNUM}/
    Item# is the last segment matching [A-Z]{0,3}\\d{3,6} pattern."""
    m = re.search(r'/item/([^/?#]+)', url)
    if not m:
        return "", ""
    slug = m.group(1).rstrip("/")
    parts = slug.split("-")
    item_num = ""
    desc_parts = parts
    # Last segment is usually the item number
    for i in range(len(parts) - 1, -1, -1):
        p = parts[i]
        if re.match(r'^[A-Z]{0,3}\d{3,6}$', p):
            item_num = p
            desc_parts = parts[:i]
            break
    desc = " ".join(w.capitalize() if len(w) > 2 else w for w in desc_parts).strip()
    # Fix common words
    desc = desc.replace(" X ", " x ").replace(" Of ", " of ").replace(" And ", " and ")
    return item_num, desc


def _normalize_ssww_url(url: str) -> str:
    """Strip tracking params from S&S Worldwide URLs."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _lookup_ssww(url: str) -> dict:
    """S&S Worldwide: scrape the S&S page for list (non-discount) price.
    ALWAYS keeps the S&S URL — never overrides with Amazon.
    Uses list_price (not sale price) as cost basis since discounts may expire
    within the 45-day quote window. If S&S is blocked, falls back to Amazon
    for price reference only (still keeps S&S URL)."""
    item_num, desc_from_url = _extract_ssww_item(url)
    clean_url = _normalize_ssww_url(url)

    result = {
        "supplier": "S&S Worldwide",
        "url": clean_url,  # ALWAYS keep original S&S URL
        "part_number": item_num or "",
        "mfg_number": item_num or "",
        "ok": True,
    }

    # Try scraping S&S directly first
    scraped = _scrape_generic(clean_url)
    title = scraped.get("title") or scraped.get("description") or ""
    _blocked = not title or "just a moment" in title.lower() or "cloudflare" in title.lower()

    if not _blocked and title:
        # S&S page loaded — use LIST price (non-discount) as cost
        result["title"] = title
        result["description"] = title
        result["manufacturer"] = scraped.get("manufacturer", "")
        # Prefer list_price over sale_price — discount may expire in 45-day window
        list_price = scraped.get("list_price") or scraped.get("price") or 0
        sale_price = scraped.get("sale_price") or scraped.get("price") or 0
        result["price"] = list_price if list_price > 0 else sale_price
        result["cost"] = result["price"]
        result["list_price"] = list_price
        result["sale_price"] = sale_price
        if sale_price and list_price and sale_price < list_price:
            discount_pct = round((1 - sale_price / list_price) * 100)
            result["shipping_note"] = (
                f"S&S sale: ${sale_price:.2f} ({discount_pct}% off list ${list_price:.2f}). "
                f"Using list price as cost basis — discount may expire."
            )
            log.info("SSWW: %s list=$%.2f sale=$%.2f (%d%% off) — using list price",
                     item_num, list_price, sale_price, discount_pct)
        return result

    # S&S blocked (Cloudflare) — try multiple fallbacks for pricing
    ref_price = None
    ref_title = desc_from_url
    ref_source = ""

    # Fallback 1: Amazon search (try item_num, then description)
    if item_num:
        try:
            from src.agents.product_research import search_amazon
            for query in [item_num, desc_from_url, f"{item_num} {desc_from_url}"]:
                if not query:
                    continue
                results = search_amazon(query, max_results=1)
                if results and results[0].get("price"):
                    r = results[0]
                    ref_price = r.get("list_price") or r.get("price")
                    ref_title = r.get("title", desc_from_url)
                    result["asin"] = r.get("asin", "")
                    result["amazon_reference_price"] = r["price"]
                    ref_source = "Amazon"
                    log.info("SSWW blocked, Amazon ref: %s → $%s", item_num, r["price"])
                    break
        except Exception as e:
            log.warning("SSWW→Amazon fallback: %s", e)

    # Fallback 2: Catalog match
    if not ref_price:
        try:
            from src.agents.product_catalog import match_item, init_catalog_db
            init_catalog_db()
            matches = match_item(item_num, part_number=item_num, top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.80:
                m = matches[0]
                ref_title = m.get("name", desc_from_url)
                ref_price = m.get("cost") or m.get("sell_price")
                result["manufacturer"] = m.get("manufacturer", "")
                ref_source = "Catalog"
        except Exception:
            pass

    # Fallback 3: Web price research (Claude-powered)
    if not ref_price and desc_from_url:
        try:
            from src.agents.web_price_research import research_price
            web = research_price(f"{desc_from_url} {item_num or ''}", quantity=1)
            if web and web.get("price") and web["price"] > 0:
                ref_price = web["price"]
                ref_source = web.get("source", "Web")
                log.info("SSWW blocked, web research: %s → $%s", item_num, ref_price)
        except Exception:
            pass

    if ref_source:
        result["shipping_note"] = (
            f"S&S site blocked — {ref_source} reference: ${ref_price:.2f}. "
            f"Verify on ssww.com directly."
        )

    result["title"] = ref_title or desc_from_url
    result["description"] = result["title"]
    result["price"] = ref_price or 0
    result["cost"] = result["price"]
    return result


def _lookup_aedstore(url: str) -> dict:
    """AED Store / AED Superstore: extract product details."""
    result = _scrape_generic(url)
    host = urlparse(url).netloc.lower()
    if "aedstore.com" in host:
        result["supplier"] = "AED Store"
    elif "aed.com" in host:
        result["supplier"] = "AED Superstore"
    elif "aedbrands.com" in host:
        result["supplier"] = "AED Brands"
    else:
        result["supplier"] = detect_supplier(url)
    
    # Try extracting SKU from URL path (e.g. /product/SKU-123/)
    path = urlparse(url).path
    sku_match = re.search(r'/(?:product|p)/([A-Z0-9\-]{4,30})(?:/|$|\?)', path, re.IGNORECASE)
    if sku_match and not result.get("part_number"):
        result["part_number"] = sku_match.group(1)
    
    return result


# ─── Main entry point ─────────────────────────────────────────────────────────

# Domains that require login — try authenticated scraper first,
# fall back to "paste manually" if no credentials configured.
LOGIN_REQUIRED_DOMAINS = [
    "henryschein.com",
    "medline.com",
    "cardinalhealth.com",
    "owens-minor.com",
    "mckesson.com",
    "concordance.com",
    "boundtree.com",
]


def _is_login_required(url: str) -> bool:
    """Check if a URL belongs to a domain that requires login to view products."""
    host = urlparse(url).netloc.lower()
    return any(d in host for d in LOGIN_REQUIRED_DOMAINS)


def _try_authenticated_lookup(url: str) -> dict | None:
    """Try authenticated scraping for login-required suppliers.

    Returns product dict if credentials exist and scraping succeeds,
    or None to fall through to the "paste manually" fast-fail.
    """
    try:
        from src.agents.supplier_auth_scraper import lookup_product, _has_credentials, _detect_supplier_key
        supplier_key = _detect_supplier_key(url)
        if not supplier_key:
            return None
        if not _has_credentials(supplier_key):
            return None  # No creds — fall through to manual entry
        result = lookup_product(url, supplier_key)
        return result
    except ImportError:
        return None
    except Exception as e:
        log.debug("Authenticated lookup failed: %s", e)
        return None


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

    # Login-required sites: try authenticated scraper first
    if _is_login_required(url):
        auth_result = _try_authenticated_lookup(url)
        if auth_result is not None:
            # Authenticated scraper handled it (success or credential-specific error)
            auth_result.setdefault("url", url)
            auth_result.setdefault("supplier", detect_supplier(url))
            return auth_result
        # No credentials configured — fall back to manual entry
        supplier = detect_supplier(url)
        return {
            "ok": False,
            "error": f"{supplier} requires login — paste your cost manually",
            "supplier": supplier,
            "url": url,
            "login_required": True,
        }

    supplier = detect_supplier(url)
    host = urlparse(url).netloc.lower()

    # Normalize Amazon URLs before lookup — strip tracking params
    if "amazon.com" in host:
        url = _normalize_amazon_url(url) or url

    try:
        if "amazon.com" in host:
            result = _lookup_amazon(url)
        elif "grainger.com" in host:
            result = _lookup_grainger(url)
        elif "mcmaster.com" in host:
            result = _lookup_mcmaster(url)
        elif any(d in host for d in ("aedstore.com", "aed.com", "aedbrands.com", "buyaedsusa.com")):
            result = _lookup_aedstore(url)
        elif "ssww.com" in host:
            result = _lookup_ssww(url)
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
