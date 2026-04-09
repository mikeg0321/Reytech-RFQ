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
    "target.com":            "Target",
    "dollartree.com":        "Dollar Tree",
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
        r"/gp/aw/d/([A-Z0-9]{10})",
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
                r'"typicalPrice"\s*:\s*\{[^}]*"amount"\s*:\s*(\d+\.?\d*)',
                # S&S Worldwide: "List: $37.59" plain text
                r'List:\s*[$]?([\d,]+\.\d{2})',
                # Generic "Was $XX.XX" or "Reg. $XX.XX"
                r'(?:Was|Reg\.?|Regular|MSRP)\s*:?\s*[$]?([\d,]+\.\d{2})']:
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
                r'"unitPrice"\s*:\s*(\d+\.?\d*)',
                # S&S Worldwide: "SALE" followed by "$31.99"
                r'SALE[^$]*[$]\s*([\d,]+\.\d{2})',
                # Generic "Sale: $XX.XX" or "Now: $XX.XX"
                r'(?:Sale|Now|Our Price)\s*:?\s*[$]?([\d,]+\.\d{2})']:
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
            _sale = r.get("sale_price") or r.get("price")
            # If search_amazon fallback ran (no list_price), do a product lookup for MSRP
            if not _list and _sale and asin and r.get("source") != "amazon_product":
                try:
                    _prod = lookup_amazon_product(asin)
                    if _prod and _prod.get("list_price"):
                        _list = _prod["list_price"]
                        if _prod.get("sale_price"):
                            _sale = _prod["sale_price"]
                except Exception:
                    pass
            _use_price = _list or _sale
            import logging as _ll
            _ll.getLogger(__name__).info("Amazon ASIN %s: list=$%s sale=$%s use=$%s",
                                         asin, _list, _sale, _use_price)

            # MFG#: use scraped MFG#, or ISBN for books
            mfg = r.get("mfg_number", "") or r.get("part_number", "") or ""
            if not mfg and _is_isbn:
                mfg = asin  # ISBN-10 serves as MFG# for books
            # Description = clean title only. No ASIN — procurement doesn't want it.
            # MFG# stored in part_number field, ASIN in asin field only.
            structured_desc = title

            # Scrape product page for MFG# and/or list price when SerpApi didn't have them
            _page = None
            if (not mfg and asin and not _is_isbn) or (not _list and _sale):
                try:
                    _page = _scrape_generic(f"https://www.amazon.com/dp/{asin}")
                    # Extract MFG# from page
                    if not mfg:
                        import re as _re_mfg
                        _page_text = str(_page.get("raw_text", "") or _page.get("meta_description", ""))
                        _model_match = _re_mfg.search(
                            r'(?:Item model number|Part Number|Model Number|Manufacturer Part Number)'
                            r'[:\s]+([A-Z0-9][A-Z0-9\-/]{2,25})',
                            _page_text, _re_mfg.IGNORECASE)
                        if _model_match:
                            mfg = _model_match.group(1).strip()
                    # Extract list price from HTML scrape (double validation)
                    if not _list and _page.get("list_price"):
                        _list = _page["list_price"]
                        _use_price = _list
                        import logging as _ll2
                        _ll2.getLogger(__name__).info(
                            "Amazon ASIN %s: list price $%.2f found via HTML scrape (SerpApi missed it)",
                            asin, _list)
                    # Also grab sale_price from scrape if we didn't have it
                    if not _sale and _page.get("sale_price"):
                        _sale = _page["sale_price"]
                    elif not _sale and _page.get("price") and _page["price"] != _list:
                        _sale = _page["price"]
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


def _lookup_uline(url: str) -> dict:
    """Uline: extract product data from JSON-LD + page content.
    Uline serves clean JSON-LD with Product schema."""
    import json as _json
    result = _scrape_generic(url)
    result["supplier"] = "Uline"

    # Extract SKU from URL: /Product/Detail/S-XXXXX/...
    sku_match = re.search(r'/Detail/(S-\d{4,6})/', url)
    if sku_match:
        result["part_number"] = sku_match.group(1)
        result["mfg_number"] = sku_match.group(1)

    # Try JSON-LD for structured data (Uline serves clean Product schema)
    try:
        html = result.get("_html", "")
        if not html:
            resp = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=12)
            html = resp.text

        ld_matches = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
        for ld_text in ld_matches:
            try:
                ld = _json.loads(ld_text)
                if ld.get("@type") == "Product":
                    if ld.get("name"):
                        result["title"] = ld["name"].replace("&quot;", '"').replace("&amp;", "&")
                        result["description"] = result["title"]
                    if ld.get("sku"):
                        result["part_number"] = ld["sku"]
                        result["mfg_number"] = ld["sku"]
                    if ld.get("description"):
                        desc = ld["description"][:200]
                        if len(desc) > len(result.get("description", "")):
                            result["description"] = desc
                    # Price from offers
                    offers = ld.get("offers", {})
                    if isinstance(offers, dict) and offers.get("price"):
                        result["price"] = float(offers["price"])
                        result["cost"] = result["price"]
                    break
            except (ValueError, TypeError):
                pass

        # Extract pricing table from attrib tags (Uline-specific)
        attribs = re.findall(r'<attrib[^>]*>(.*?)</attrib>', html, re.DOTALL)
        clean_attribs = [re.sub(r'<[^>]+>', ' ', a).strip() for a in attribs]
        # Find case qty, per-unit prices at different tiers
        _case_qty = 0
        _tier_prices = []
        for i, a in enumerate(clean_attribs):
            # Case quantity (the number after "ROLLS/CASE" or "PER CASE" header)
            if a.isdigit() and int(a) >= 2 and int(a) <= 500:
                # Check context: is the previous attrib "ROLLS/CASE" or similar?
                if i > 0 and any(w in clean_attribs[i-1].upper() for w in ['CASE', 'ROLL', 'PACK']):
                    _case_qty = int(a)
                elif not _case_qty and i > 1:
                    _case_qty = int(a)
            # Price tiers ($X.XX)
            if a.startswith('$'):
                try:
                    _tier_prices.append(float(a.replace('$', '').replace(',', '')))
                except ValueError:
                    pass

        if _tier_prices:
            # Best price for state orders = 1-case tier (usually middle price)
            # Tiers are typically: half-case, 1 case, 2+ cases
            if len(_tier_prices) >= 2:
                _one_case_price = _tier_prices[1]  # 1-case tier
                _half_case_price = _tier_prices[0]  # half-case tier
                _bulk_price = _tier_prices[-1] if len(_tier_prices) >= 3 else _one_case_price
            else:
                _one_case_price = _tier_prices[0]
                _half_case_price = _one_case_price
                _bulk_price = _one_case_price

            result["price"] = _one_case_price
            result["cost"] = _one_case_price
            uom = "CS" if _case_qty else "EA"
            note_parts = [f"${_one_case_price:.2f}/ea (1 case)"]
            if _half_case_price != _one_case_price:
                note_parts.append(f"${_half_case_price:.2f}/ea (1/2 case)")
            if _bulk_price != _one_case_price:
                note_parts.append(f"${_bulk_price:.2f}/ea (2+ cases)")
            if _case_qty:
                note_parts.append(f"{_case_qty}/case")
                result["uom"] = "CS"
                result["qty_per_uom"] = _case_qty
            result["shipping_note"] = " | ".join(note_parts)
        elif not result.get("price"):
            # Fallback to productPrice
            pp_match = re.search(r"'productPrice'\s*:\s*'([\d.]+)'", html)
            if pp_match:
                result["price"] = float(pp_match.group(1))
                result["cost"] = result["price"]

    except Exception as e:
        log.debug("Uline JSON-LD parse: %s", e)

    # Ensure photo URL
    if not result.get("photo_url"):
        img_match = re.search(r'"image"\s*:\s*"([^"]+)"', html if 'html' in dir() else "")
        if img_match:
            result["photo_url"] = img_match.group(1)

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

    # Fallback 3: Claude web search — search for the S&S item directly
    if not ref_price and (desc_from_url or item_num):
        try:
            from src.agents.web_price_research import search_product_price
            _query = f"S&S Worldwide {item_num}" if item_num else desc_from_url
            web = search_product_price(
                description=_query,
                part_number=item_num or "",
                qty=1, uom="EA",
                context="S&S Worldwide ssww.com product",
            )
            if web and web.get("found") and web.get("price", 0) > 0:
                ref_price = web["price"]
                ref_title = web.get("title", ref_title) or ref_title
                ref_source = "Claude Web"
                log.info("SSWW blocked, Claude web search: %s → $%.2f via %s",
                         item_num, ref_price, web.get("source", "web"))
        except Exception as _web_err:
            log.debug("SSWW→Claude web search error: %s", _web_err)

    if ref_source:
        result["shipping_note"] = (
            f"S&S site blocked — {ref_source} found: ${ref_price:.2f}. "
            f"Using as MSRP (list price)."
        )

    result["title"] = ref_title or desc_from_url
    result["description"] = result["title"]

    # S&S is a TRUSTED SUPPLIER — prices from any source are valid cost basis.
    # Amazon/Claude prices for S&S items represent MSRP (list price).
    # Set as list_price; sale_price populated later if user enters discount.
    if ref_price and ref_price > 0:
        result["price"] = ref_price
        result["cost"] = ref_price
        result["list_price"] = ref_price  # MSRP — safe bid basis
        result["reference_source"] = ref_source
    else:
        result["price"] = 0  # Triggers quick-entry field in JS
        result["cost"] = 0

    return result


def _extract_target_tcin(url: str) -> str:
    """Extract Target TCIN from URL.  /A-XXXXXXXXXX at end of path."""
    m = re.search(r'/A-(\d{8,12})(?:\?|$|/)', url)
    return m.group(1) if m else ""


def _lookup_target(url: str) -> dict:
    """Target.com: extract product data from __TGT_DATA__ embedded JSON.
    Returns MSRP (reg_retail) as list_price and sale price when discounted."""
    import json as _json
    if not HAS_REQUESTS:
        return {"error": "requests not available", "supplier": "Target"}

    tcin = _extract_target_tcin(url)
    result = {"supplier": "Target", "url": url}
    if tcin:
        result["part_number"] = f"A-{tcin}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        html = resp.text
    except requests.exceptions.Timeout:
        return {**result, "error": "Request timed out"}
    except Exception as e:
        return {**result, "error": str(e)}

    # Parse __TGT_DATA__ — it's inside deepFreeze(JSON.parse("..."))
    # The JSON is escaped inside a JS string literal
    tgt_match = re.search(
        r"'__TGT_DATA__'\s*:\s*\{[^}]*value:\s*deepFreeze\(JSON\.parse\(\"(.*?)\"\)\)",
        html, re.DOTALL)
    tgt_data = None
    if tgt_match:
        try:
            unescaped = tgt_match.group(1).encode().decode('unicode_escape')
            tgt_data = _json.loads(unescaped)
        except (ValueError, TypeError, UnicodeDecodeError):
            pass

    if not tgt_data:
        # Fallback: try generic scrape
        fallback = _scrape_generic(url)
        fallback["supplier"] = "Target"
        if tcin:
            fallback.setdefault("part_number", f"A-{tcin}")
        return fallback

    # Navigate to product data in preloaded queries
    product = None
    try:
        queries = tgt_data.get("__PRELOADED_QUERIES__", {}).get("queries", [])
        for q in queries:
            if not isinstance(q, (list, tuple)) or len(q) < 2:
                continue
            qdata = q[1] if isinstance(q[1], dict) else {}
            prod = qdata.get("data", {}).get("product")
            if prod and isinstance(prod, dict):
                product = prod
                break
    except Exception:
        pass

    if not product:
        fallback = _scrape_generic(url)
        fallback["supplier"] = "Target"
        return fallback

    # Title / description
    item = product.get("item", {})
    result["title"] = item.get("product_description", {}).get("title", "")
    result["description"] = (
        item.get("product_description", {}).get("downstream_description", "")
        or result["title"])
    if len(result["description"]) > 300:
        result["description"] = result["description"][:300]

    # Brand
    brand = item.get("primary_brand", {}).get("name", "")
    if brand:
        result["manufacturer"] = brand

    # DPCI / part number
    dpci = item.get("dpci", "")
    if dpci:
        result["mfg_number"] = dpci

    # Image
    enrichment = item.get("enrichment", {})
    images = enrichment.get("images", {}).get("primary_image_url", "")
    if images:
        result["photo_url"] = images

    # ── Pricing — find the matching variant by TCIN ──
    price_data = product.get("price", {})
    children = product.get("children", [])

    # Try variant-specific pricing first (multi-variant products)
    variant_price = None
    if tcin and children:
        for child in children:
            if not isinstance(child, dict):
                continue
            child_tcin = str(child.get("tcin", ""))
            if child_tcin == tcin:
                variant_price = child.get("price", {})
                # Also grab variant-specific item data
                child_item = child.get("item", {})
                if child_item.get("product_description", {}).get("title"):
                    result["title"] = child_item["product_description"]["title"]
                break

    px = variant_price or price_data
    reg_retail = px.get("reg_retail")
    current_retail = px.get("current_retail")
    price_type = px.get("formatted_current_price_type", "")

    if reg_retail:
        result["list_price"] = float(reg_retail)
    if current_retail:
        current = float(current_retail)
        if price_type == "sale" and reg_retail:
            # Sale price — MSRP is reg_retail, sale is current_retail
            result["price"] = float(reg_retail)       # Use MSRP as cost basis
            result["cost"] = float(reg_retail)
            result["sale_price"] = current
            result["discount_pct"] = round(
                (1 - current / float(reg_retail)) * 100, 1)
            result["shipping_note"] = (
                f"MSRP ${reg_retail:.2f} | Sale ${current:.2f} "
                f"({result['discount_pct']}% off)")
        else:
            # Regular price — no discount
            result["price"] = current
            result["cost"] = current

    # Shipping
    if "free" in str(enrichment.get("shipping", "")).lower():
        result["shipping"] = 0.0
        result["shipping_note"] = (result.get("shipping_note", "") +
                                   " | Free shipping").strip(" |")

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


def _lookup_dollartree(url: str) -> dict:
    """Dollar Tree: SPA site (Oracle Commerce Cloud) — HTML has no product data.
    Strategy: extract product ID from URL path, or extract search terms from slug,
    then hit their public search API for structured product data."""
    import json as _json
    if not HAS_REQUESTS:
        return {"error": "requests not available", "supplier": "Dollar Tree"}

    result = {"supplier": "Dollar Tree", "url": url}
    path = urlparse(url).path.rstrip("/")

    # Extract product ID from URL — e.g., /product-name/343586
    product_id = ""
    _id_match = re.search(r'/(\d{4,8})$', path)
    if _id_match:
        product_id = _id_match.group(1)
        result["part_number"] = product_id

    # Extract search terms from URL slug for API query
    slug = path.split("/")[-1] if "/" in path else path
    # Remove trailing product ID if present
    slug = re.sub(r'/?\d{4,8}$', '', slug)
    # Convert slug to search query: "colgater-sensitive-" → "colgater sensitive"
    search_q = re.sub(r'[-_]+', ' ', slug).strip()
    if not search_q and not product_id:
        return {**result, "error": "Could not extract product info from URL"}

    # Hit Dollar Tree's public search API (Oracle Commerce Cloud)
    api_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    try:
        api_url = f"https://www.dollartree.com/ccstoreui/v1/search?Ntt={requests.utils.quote(search_q)}&No=0&Nrpp=5"
        resp = requests.get(api_url, headers=api_headers, timeout=15)
        if resp.status_code != 200:
            return {**result, "error": f"API returned {resp.status_code}"}
        data = resp.json()
        records = data.get("resultsList", {}).get("records", [])
        if not records:
            return {**result, "error": "No products found"}

        # Find best match — prefer exact product ID match
        best = None
        for rec in records:
            inner = rec.get("records", [{}])
            attrs = (inner[0] if inner else rec).get("attributes", {})
            pid = (attrs.get("product.id") or [""])[0]
            if product_id and pid == product_id:
                best = attrs
                break
        if not best:
            # Use first result
            inner = records[0].get("records", [{}])
            best = (inner[0] if inner else records[0]).get("attributes", {})

        # Extract fields
        result["title"] = (best.get("product.displayName") or [""])[0]
        result["description"] = (best.get("product.longDescription") or [result.get("title", "")])[0]
        result["manufacturer"] = (best.get("product.brand") or [""])[0]
        result["part_number"] = (best.get("product.id") or [product_id])[0]

        # Price: unit price (per item)
        unit_price = (best.get("product.x_unitprice") or best.get("sku.activePrice") or [""])[0]
        if unit_price:
            try:
                result["price"] = float(unit_price)
            except (ValueError, TypeError):
                pass

        # Also capture case pricing for reference
        case_price = (best.get("product.casePrice") or [""])[0]
        case_pack = (best.get("DollarProductType.casePackSize") or [""])[0]
        if case_price and case_pack:
            try:
                cp = float(case_price)
                cpk = int(case_pack)
                if cp > 0 and cpk > 0:
                    result["shipping_note"] = f"Case of {cpk}: ${cp:.2f} (${cp/cpk:.2f}/ea)"
            except (ValueError, TypeError):
                pass

        # UPC as mfg_number fallback
        upcs = (best.get("DollarProductType.UPCs") or [""])[0]
        if upcs and not result.get("mfg_number"):
            # Take first UPC (may be comma-separated)
            result["mfg_number"] = upcs.split(",")[0].strip()

        # Canonical URL
        route = (best.get("product.route") or [""])[0]
        if route:
            result["url"] = f"https://www.dollartree.com{route}"

    except Exception as e:
        log.debug("Dollar Tree API error: %s", e)
        return {**result, "error": str(e)}

    return result


# ── Claude Semantic Match — AI product comparison ────────────────────────────

def claude_semantic_match(
    pc_description: str,
    found_title: str,
    found_price: float = 0,
) -> dict:
    """Compare PC description to found product title using Claude Haiku.

    Pure text comparison — no web search tool. ~$0.0005/call, ~0.5s latency.
    Returns {"ok": True, "is_match": bool, "confidence": 0-1, "reasoning": str}
    On any error: {"ok": False, "is_match": False, "confidence": 0, "reasoning": "..."}
    """
    if not pc_description or not found_title:
        return {"ok": False, "is_match": False, "confidence": 0,
                "reasoning": "Missing description"}

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"ok": False, "is_match": False, "confidence": 0,
                "reasoning": "ANTHROPIC_API_KEY not set"}

    if not HAS_REQUESTS:
        return {"ok": False, "is_match": False, "confidence": 0,
                "reasoning": "requests not available"}

    prompt = (
        "You are a procurement product matcher. Compare these two product descriptions "
        "and determine if they refer to the same product (same item, same specs, same quantity).\n\n"
        f'Buyer requested: "{pc_description}"\n'
        f'Found product: "{found_title}"\n'
    )
    if found_price > 0:
        prompt += f"Found price: ${found_price:.2f}\n"
    prompt += (
        "\nDifferences in brand name alone do NOT make it a mismatch — "
        "generic vs branded versions of the same item are a match.\n"
        "Reply ONLY with JSON: "
        '{"match": true/false, "confidence": 0.0-1.0, "reason": "one sentence"}'
    )

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 100,
        "messages": [{"role": "user", "content": prompt}],
    }

    import json as _json
    for attempt in range(2):
        try:
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers, json=body, timeout=5,
            )
            if resp.status_code == 429:
                import time
                time.sleep(2)
                continue
            if resp.status_code != 200:
                log.debug("Claude semantic match API %d: %s",
                          resp.status_code, resp.text[:100])
                return {"ok": False, "is_match": False, "confidence": 0,
                        "reasoning": f"API {resp.status_code}"}

            data = resp.json()
            text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")
            if not text:
                return {"ok": False, "is_match": False, "confidence": 0,
                        "reasoning": "Empty response"}

            # Parse JSON from response
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()
            jm = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if jm:
                parsed = _json.loads(jm.group())
            else:
                parsed = _json.loads(text)

            is_match = bool(parsed.get("match", False))
            confidence = float(parsed.get("confidence", 0))
            reasoning = str(parsed.get("reason", ""))[:200]

            log.info("Claude semantic match: '%s' vs '%s' → %s (%.0f%%)",
                     pc_description[:30], found_title[:30],
                     "MATCH" if is_match else "NO MATCH", confidence * 100)

            return {
                "ok": True,
                "is_match": is_match,
                "confidence": confidence,
                "reasoning": reasoning,
            }

        except requests.exceptions.Timeout:
            log.debug("Claude semantic match timeout (attempt %d)", attempt + 1)
            continue
        except (_json.JSONDecodeError, ValueError, TypeError) as e:
            log.debug("Claude semantic match parse error: %s", e)
            return {"ok": False, "is_match": False, "confidence": 0,
                    "reasoning": f"Parse error: {e}"}
        except Exception as e:
            log.debug("Claude semantic match error: %s", e)
            return {"ok": False, "is_match": False, "confidence": 0,
                    "reasoning": str(e)[:100]}

    return {"ok": False, "is_match": False, "confidence": 0,
            "reasoning": "Max retries exceeded"}


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
        elif "uline.com" in host:
            result = _lookup_uline(url)
        elif "grainger.com" in host:
            result = _lookup_grainger(url)
        elif "mcmaster.com" in host:
            result = _lookup_mcmaster(url)
        elif any(d in host for d in ("aedstore.com", "aed.com", "aedbrands.com", "buyaedsusa.com")):
            result = _lookup_aedstore(url)
        elif "ssww.com" in host:
            result = _lookup_ssww(url)
        elif "target.com" in host:
            result = _lookup_target(url)
        elif "dollartree.com" in host:
            result = _lookup_dollartree(url)
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
