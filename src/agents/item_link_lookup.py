"""
item_link_lookup.py — Supplier URL → Product Data

Paste any supplier URL → get back:
  - description / title
  - cost / price
  - part number / MFG number
  - shipping cost (if parseable)
  - supplier name

Supported suppliers (with structured parsing):
  amazon.com       → ASIN-based lookup via Grok
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
import logging

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

log = logging.getLogger("item_link")

# ─── Skip ledger ──────────────────────────────────────────────────────────────
# Best-effort enrichment paths (claude_amazon_lookup, claude_product_lookup)
# return {} on missing-dep failures so callers don't have to special-case
# the optional-LLM tier. But those skips were previously invisible — log.debug
# only — leaving operators to wonder why "no enrichment data" appeared on
# every item. The ledger lets the orchestrator/route surface those skips
# via the OrchestratorResult 3-channel envelope (PRs #181-#183).
from src.core.dependency_check import Severity, SkipReason, try_env  # noqa: E402

_SKIP_LEDGER: list[SkipReason] = []


def _record_skip(skip: SkipReason) -> None:
    """Append a skip to the module ledger. try_env/try_import already log
    at WARNING — this just persists the event so a later drain can pick it
    up and route it through OrchestratorResult.add_skip()."""
    _SKIP_LEDGER.append(skip)


def drain_skips() -> list[SkipReason]:
    """Pop and return every skip recorded since the last drain.

    Callers (the orchestrator, the routes that run enrichment) call this
    AFTER a batch of lookups completes, then push each skip into
    `OrchestratorResult.add_skip()` so they appear in result.warnings.
    Drain is destructive so two consecutive calls don't double-warn."""
    drained = list(_SKIP_LEDGER)
    _SKIP_LEDGER.clear()
    return drained

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
    "costco.com":            "Costco",
    "sears.com":             "Sears",
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
    """Extract Amazon ASIN or ISBN from URL."""
    patterns = [
        r"/dp/([A-Z0-9]{10,13})",
        r"/gp/aw/d/([A-Z0-9]{10,13})",
        r"/gp/product/([A-Z0-9]{10,13})",
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

    # 4xx/5xx pages still have a <title> ("404 Not Found", "Whoops, we
    # couldn't find that") that the parser would happily lift as a
    # product name. Empirical incident 2026-04-19: Grainger and Waxie
    # 404 pages were returning verdict=TITLE_ONLY with the 404 string
    # as the product title. Refuse to parse non-200 responses.
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code} from {url}",
                "status_code": resp.status_code}

    result = {}

    # Title
    m = re.search(r'<title[^>]*>([^<]{5,200})</title>', html, re.IGNORECASE)
    if m:
        title = m.group(1).strip()
        # Strip " - Grainger" " | Amazon" etc.
        title = re.split(r'\s*[|\-–]\s*(Grainger|Amazon|McMaster|Fisher|Medline|Bound Tree|Henry Schein|Uline|Zoro|Staples|Waxie|Costco|Sears)', title)[0].strip()
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
            except Exception as _e:
                log.debug("suppressed: %s", _e)

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
            except Exception as _e:
                log.debug("suppressed: %s", _e)

    # JSON-LD offer price (structured, reliable when present)
    jld_offer_price = re.search(
        r'"offers"\s*:\s*\{[^}]*"price"\s*:\s*"?(\d+\.?\d*)"?', html, re.IGNORECASE | re.DOTALL)
    if jld_offer_price:
        try:
            v = float(jld_offer_price.group(1))
            # Skip placeholder prices (e.g. Costco uses price=1 for out-of-stock)
            if v >= 2.0 and v < 100000:
                if not sale_price:
                    sale_price = v
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    # Costco: real prices hidden behind React state, but marketing statement
    # has pattern like "$$199.99 After $90 OFF" → sale=$199.99, original=$289.99
    if not list_price and not sale_price:
        costco_promo = re.search(
            r'\$\$(\d+\.?\d{0,2})\s*(?:After|after)\s*\$(\d+\.?\d{0,2})\s*(?:OFF|off)',
            html)
        if costco_promo:
            try:
                _sale = float(costco_promo.group(1))
                _discount = float(costco_promo.group(2))
                if _sale > 2 and _discount > 0:
                    sale_price = _sale
                    list_price = _sale + _discount
            except Exception as _e:
                log.debug("suppressed: %s", _e)

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
        # Amazon product detail table: "Item model number : ABC123" /
        # "Manufacturer Part Number : XYZ456"
        r'Item\s+model\s+number\s*[:\s]+([A-Z0-9\-]{3,30})',
        r'Manufacturer\s+Part\s+Number\s*[:\s]+([A-Z0-9\-]{3,30})',
    ]
    for pat in mfg_patterns:
        m = re.search(pat, html)
        if m:
            result["mfg_number"] = m.group(1).strip()
            break

    # UPC / GTIN — JSON-LD uses "gtin8"/"gtin12"/"gtin13" and "gtin".
    # Amazon also exposes it in the product detail table. This is the
    # last-resort identifier when MFG# and item# are missing.
    upc_patterns = [
        r'"gtin13"\s*:\s*"?(\d{13})"?',
        r'"gtin12"\s*:\s*"?(\d{12})"?',
        r'"gtin8"\s*:\s*"?(\d{8,13})"?',
        r'"gtin"\s*:\s*"?(\d{8,14})"?',
        r'UPC\s*[:\s]+(\d{12,13})',
        r'EAN\s*[:\s]+(\d{12,13})',
    ]
    for pat in upc_patterns:
        m = re.search(pat, html)
        if m:
            result["upc"] = m.group(1).strip()
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
        r'[Ss]hip(?:ping)?\s+[Ii]ncluded',   # Costco: "UPS 5-7 days ship included"
        r'[Ss]hipping\s*[:\s]\s*\$?([\d]+\.?\d*)',
        r'[Ff]reight\s*[:\s]\s*\$?([\d]+\.?\d*)',
    ]
    for pat in ship_patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            if "Free" in pat or "free" in pat.lower() or "ncluded" in pat:
                result["shipping"] = 0.0
                result["shipping_note"] = "Free shipping"
            elif m.lastindex and m.group(1):
                try:
                    result["shipping"] = float(m.group(1))
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
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
        title = re.split(r'\s*[|\-–]\s*(Grainger|Amazon|McMaster|Fisher|Medline|Bound Tree|Henry Schein|Uline|Zoro|Staples|Waxie|Costco|Sears)', title)[0].strip()
        result["title"] = title[:200]

    # JSON-LD product name (most structured/reliable)
    # Non-greedy [^}]*? so we match the Product's own "name", not a nested brand "name"
    jld_name = re.search(r'"@type"\s*:\s*"Product"[^}]*?"name"\s*:\s*"([^"]{5,200})"', html, re.IGNORECASE | re.DOTALL)
    if jld_name:
        result["title"] = jld_name.group(1).strip()[:200]

    # JSON-LD product description (often richer than meta description)
    jld_desc = re.search(r'"@type"\s*:\s*"Product"[^}]*?"description"\s*:\s*"([^"]{10,500})"', html, re.IGNORECASE | re.DOTALL)
    if jld_desc:
        result["jld_description"] = jld_desc.group(1).strip()[:400]
        # Use JSON-LD description if meta_description is missing or shorter
        if not result.get("meta_description") or len(jld_desc.group(1)) > len(result.get("meta_description", "")):
            result["meta_description"] = jld_desc.group(1).strip()[:400]

    # Product image — extract from JSON-LD, OG, or meta
    image_patterns = [
        r'"@type"\s*:\s*"Product"[^}]*?"image"\s*:\s*"([^"]{10,500})"',
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
    """Amazon: extract ASIN then fetch product data.

    Strategy (rewritten 2026-04-14 after regression):
      1. Direct HTML scrape first — fast (~2s), reliable, returns
         list_price + sale_price separately via JSON-LD / Amazon's
         own markup, and exposes MFG# from the "Item model number"
         row and UPC from gtin fields.
      2. Grok ASIN lookup second — only used as enrichment if the
         scrape came back with no title or no price. Grok became
         unreliable after the Apr 10 SerpApi swap (timeouts on most
         Amazon ASINs) and was the single biggest cause of user-
         reported parser failures.
      3. MFG fallback chain: mfg_number → part_number → upc → asin.
         The return dict always has *something* in mfg_number so
         downstream callers can at least store an identifier.

    Time budget still 12s. Scrape-first means most lookups return
    in under 3s, leaving plenty of budget for the enrichment hop.
    """
    import time as _time
    _t0 = _time.monotonic()
    _BUDGET = 12.0

    def _over_budget():
        return (_time.monotonic() - _t0) >= _BUDGET

    asin = _extract_asin(url)
    if not asin:
        return {"error": "Could not extract ASIN from Amazon URL", "supplier": "Amazon"}

    _is_isbn = asin and asin[0].isdigit()
    clean_url = _normalize_amazon_url(url)

    try:
        # ── Step 1: direct HTML scrape (primary path) ───────────────
        scraped = _scrape_generic(clean_url) or {}
        _elapsed_scrape = _time.monotonic() - _t0
        log.info("Amazon ASIN %s: scrape %.1fs title=%r price=%s list=%s upc=%s",
                 asin, _elapsed_scrape,
                 (scraped.get("title") or "")[:40],
                 scraped.get("price"), scraped.get("list_price"),
                 scraped.get("upc"))

        title = scraped.get("title", "")
        if _is_garbage_title(title):
            log.info("Amazon ASIN %s: garbage scrape title %r → treating as empty",
                     asin, title)
            title = ""
        _list = scraped.get("list_price")
        _sale = scraped.get("sale_price") or scraped.get("price")
        # Real mfg and item numbers tracked separately so the fallback
        # chain below can label the source correctly.
        scraped_mfg = scraped.get("mfg_number", "") or ""
        scraped_item = scraped.get("part_number", "") or ""
        mfg = scraped_mfg  # starts empty if scrape didn't find one
        manufacturer = scraped.get("manufacturer", "")
        photo_url = scraped.get("photo_url", "")
        upc = scraped.get("upc", "")

        # ── Step 2: Grok enrichment — only when scrape came back empty
        # A successful scrape already has title + price; the Grok hop
        # is expensive and frequently times out, so skip it when we
        # already have what we need.
        def _have_price():
            return bool(_list) or bool(_sale)

        scrape_good = bool(title) and _have_price()
        if (not scrape_good) and (not _over_budget()):
            try:
                from src.agents.product_research import lookup_amazon_product
                direct = lookup_amazon_product(asin)
                if direct:
                    title = title or direct.get("title", "")
                    _list = _list or direct.get("list_price") or direct.get("typical_price")
                    _sale = _sale or direct.get("sale_price") or direct.get("price")
                    if not mfg:
                        mfg = direct.get("mfg_number", "") or direct.get("part_number", "")
                    manufacturer = manufacturer or direct.get("manufacturer", "")
                    photo_url = photo_url or direct.get("photo_url", "") or direct.get("thumbnail", "")
                    log.info("Amazon ASIN %s: Grok enrichment hit (%.1fs)",
                             asin, _time.monotonic() - _t0)
            except Exception as _e:
                log.debug("Amazon Grok enrichment failed: %s", _e)

        # ── Step 3: Claude web search — final fallback when both the
        # direct scrape and Grok came back without a price. Claude's
        # web_search tool fetches from Anthropic's side, so it gets
        # past Amazon's datacenter-IP block that defeats our scraper.
        # ~4s latency so only worth it when we're still missing data.
        if (not title or not _have_price()) and (not _over_budget()):
            try:
                claude_result = claude_amazon_lookup(asin)
                if claude_result:
                    title = title or claude_result.get("title", "")
                    _list = _list or claude_result.get("list_price")
                    _sale = _sale or claude_result.get("sale_price")
                    if not mfg:
                        mfg = claude_result.get("mfg_number", "") or ""
                        if mfg:
                            scraped_mfg = mfg  # upgrade the source label
                    manufacturer = manufacturer or claude_result.get("manufacturer", "")
                    upc = upc or claude_result.get("upc", "")
                    photo_url = photo_url or claude_result.get("photo_url", "")
                    log.info("Amazon ASIN %s: Claude web search hit (%.1fs)",
                             asin, _time.monotonic() - _t0)
            except Exception as _e:
                log.debug("Amazon Claude lookup failed: %s", _e)

        # Still empty? Return a minimal error frame — but never the
        # old "Lookup timed out" message, since we now always at
        # least tried the scrape.
        if not title and not _list and not _sale:
            return {
                "supplier": "Amazon",
                "asin": asin,
                "url": clean_url,
                "error": "No product data found — scrape blocked and AI lookups returned nothing. Paste cost manually.",
                "mfg_number": asin,  # last-resort identifier
                "part_number": asin,
            }

        # ── Size extraction from title ──────────────────────────────
        size = ""
        import re as _re
        if title:
            size_match = _re.search(
                r',\s*(XX?-?(?:Small|Large)|(?:Small|Medium|Large|X-Large|XX-Large|XS|XL|XXL|S|M|L)\b)',
                title, _re.IGNORECASE
            )
            if size_match:
                size = size_match.group(1).strip()

        # ── MFG fallback chain: MFG → Item → UPC → ASIN ─────────────
        # User ask: "amazon should add MFG/item/or last UPC number if
        # no MFG/Item". The normalized mfg_number is always populated
        # so downstream catalog / quote writers never see an empty id.
        #
        # `mfg` at this point = scraped_mfg OR (if the Grok enrichment
        # hop fired and found one) the Grok mfg. If that's populated
        # the label is "mfg". Otherwise walk the chain: item → upc →
        # asin. scraped_item captures the scraper's part_number at
        # entry time so the Grok enrichment hop can't muddy the label.
        if mfg:
            mfg_source = "mfg"
        elif scraped_item:
            mfg = scraped_item
            mfg_source = "item"
        elif upc:
            mfg = upc
            mfg_source = "upc"
        elif _is_isbn:
            mfg = asin
            mfg_source = "isbn"
        else:
            mfg = asin
            mfg_source = "asin_fallback"

        # ── Pricing semantics (user ask §9): ────────────────────────
        # - unit_cost should be list/MSRP (stable price you'd quote
        #   against, not a time-limited sale).
        # - If a discount exists, log it separately and expose a
        #   "cost_if_discount_holds" field so the pricing oracle can
        #   compute both profit scenarios downstream.
        # When Amazon shows only a single price (no strikethrough MSRP),
        # that price IS the MSRP — treat it as list so downstream code
        # never warns "MSRP not found" and never pollutes the discount
        # calculator with a fake sale. User incident 2026-04-14: paint
        # marker B0CX1BD86P had one price and UI warned cost-not-found.
        if _list is None and _sale is not None:
            log.info("Amazon ASIN %s: single price $%.2f — promoting to list_price", asin, _sale)
            _list = _sale
            _sale = None

        _use_price = _list or _sale  # fall back to sale if list is missing
        discount_pct = None
        discount_amount = None
        cost_if_discount_holds = None
        if _list and _sale and _list > _sale + 0.005:
            discount_amount = round(_list - _sale, 2)
            discount_pct = round((1 - _sale / _list) * 100, 1)
            cost_if_discount_holds = round(_sale, 2)
            log.info("Amazon ASIN %s: discount detected MSRP=$%.2f sale=$%.2f (%.1f%% off)",
                     asin, _list, _sale, discount_pct)

        price_note = ""
        if discount_pct is not None:
            price_note = (f"MSRP ${_list:.2f} (${_sale:.2f} sale, "
                          f"{discount_pct:.0f}% off — profit shown for both)")
        elif _list:
            price_note = f"List: ${_list:.2f}"

        _elapsed = _time.monotonic() - _t0
        log.info("Amazon ASIN %s: done in %.1fs mfg_source=%s",
                 asin, _elapsed, mfg_source)

        return {
            "supplier":      "Amazon",
            "title":         title,
            "description":   title,
            # Quoting price: always MSRP when available. Pricing
            # oracle should NOT treat this as a cost floor — Amazon
            # retail is reference data, not supplier cost.
            "price":         _use_price,
            "list_price":    _list,
            "sale_price":    _sale if (_sale and _sale != _list) else None,
            "cost":          _use_price,
            "cost_if_discount_holds": cost_if_discount_holds,
            "discount_amount":        discount_amount,
            "discount_pct":           discount_pct,
            # MFG fallback chain populates mfg_number + part_number
            # + upc so downstream code can pick whichever shape it
            # needs without having to re-derive the fallback.
            "part_number":   mfg,
            "mfg_number":    mfg,
            "mfg_source":    mfg_source,
            "upc":           upc,
            "manufacturer":  manufacturer,
            "url":           clean_url,
            "original_url":  url,
            "asin":          asin,
            "size":          size,
            "shipping":      0.0,
            "shipping_note": "Prime/standard shipping — verify delivery window",
            "source":        "amazon_lookup",
            "price_note":    price_note,
            "photo_url":     photo_url,
        }
    except Exception as e:
        log.error("Amazon lookup failed for %s: %s", asin, e, exc_info=True)
        return {"error": str(e), "supplier": "Amazon", "asin": asin,
                "mfg_number": asin, "part_number": asin}


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
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

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
                except ValueError as _e:
                    log.debug("suppressed: %s", _e)

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
        except Exception as _e:
            log.debug("suppressed: %s", _e)

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
        except (ValueError, TypeError, UnicodeDecodeError) as _e:
            log.debug("suppressed: %s", _e)

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
    except Exception as _e:
        log.debug("suppressed: %s", _e)

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
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

        # Also capture case pricing for reference
        case_price = (best.get("product.casePrice") or [""])[0]
        case_pack = (best.get("DollarProductType.casePackSize") or [""])[0]
        if case_price and case_pack:
            try:
                cp = float(case_price)
                cpk = int(case_pack)
                if cp > 0 and cpk > 0:
                    result["shipping_note"] = f"Case of {cpk}: ${cp:.2f} (${cp/cpk:.2f}/ea)"
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

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


def _lookup_sears(url: str) -> dict:
    """Sears: Cloudflare-protected — cannot scrape HTML directly.
    Strategy: parse item ID from URL, hit Sears search API for structured data.
    Falls back to URL slug parsing if API fails.

    URL format: sears.com/{brand}-{slug}/p-{item_id}?sid=...
    API: /api/sal/v3/products/search?q={item_id}&startIndex=1&endIndex=5&storeId=10153
    """
    result = {"supplier": "Sears", "url": url}
    path = urlparse(url).path.rstrip("/")

    # Extract Sears item ID: /p-A129152885
    item_id = ""
    _id_match = re.search(r'/p-([A-Z0-9]+)(?:\?|$)', url)
    if _id_match:
        item_id = _id_match.group(1)
        result["part_number"] = item_id

    # Fallback: parse product name from URL slug
    slug = path.rsplit("/p-", 1)[0] if "/p-" in path else path
    slug = slug.split("/")[-1] if "/" in slug else slug
    title_from_slug = re.sub(r'[-_]+', ' ', slug).strip()
    title_from_slug = " ".join(w.capitalize() if len(w) > 2 else w.upper()
                               for w in title_from_slug.split())
    if title_from_slug:
        result["title"] = title_from_slug
        result["description"] = title_from_slug

    # Hit Sears search API — returns full structured product data
    if item_id and HAS_REQUESTS:
        try:
            api_url = (f"https://www.sears.com/api/sal/v3/products/search"
                       f"?q={item_id}&startIndex=1&endIndex=5&storeId=10153&zipCode=90210")
            api_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Authorization": "SEARS",
            }
            resp = requests.get(api_url, headers=api_headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("items", [])
                if items:
                    p = items[0]
                    attrs = p.get("additionalAttributes", {})

                    # Title / description
                    _name = p.get("name", "")
                    if _name:
                        result["title"] = _name
                        result["description"] = _name

                    # Brand / manufacturer
                    _brand = p.get("brandName", "")
                    if _brand:
                        result["manufacturer"] = _brand

                    # MFG part number
                    _mfg = attrs.get("mfgPartNum", "")
                    if _mfg:
                        result["mfg_number"] = _mfg

                    # UPC
                    _upc = p.get("upc", "")
                    if _upc:
                        result["upc"] = _upc

                    # Pricing from price block (most reliable)
                    price_block = p.get("price", {})
                    _reg = price_block.get("regularPrice")
                    _final = price_block.get("finalPrice")
                    _savings_pct = price_block.get("savingsPercent")

                    if _reg:
                        try:
                            result["list_price"] = float(_reg)
                            result["price"] = float(_reg)
                        except (ValueError, TypeError) as _e:
                            log.debug("suppressed: %s", _e)
                    if _final:
                        try:
                            result["sale_price"] = float(_final)
                            if not result.get("price"):
                                result["price"] = float(_final)
                        except (ValueError, TypeError) as _e:
                            log.debug("suppressed: %s", _e)
                    if result.get("list_price") and result.get("sale_price"):
                        lp, sp = result["list_price"], result["sale_price"]
                        if lp > sp > 0:
                            result["discount_pct"] = round((1 - sp / lp) * 100, 1)

                    # Image
                    images = attrs.get("imageUrls", [])
                    if images and isinstance(images, list) and images[0].get("url"):
                        _img = images[0]["url"]
                        if _img.startswith("//"):
                            _img = "https:" + _img
                        result["photo_url"] = _img

                    # Shipping
                    if attrs.get("freeShippingInd"):
                        result["shipping"] = 0.0
                        result["shipping_note"] = "Free shipping"

                    # Canonical URL
                    _seo = attrs.get("seoUrl", "")
                    if _seo:
                        result["url"] = f"https://www.sears.com{_seo}"
            else:
                log.debug("Sears search API returned %d for %s", resp.status_code, item_id)
        except Exception as e:
            log.debug("Sears API error: %s", e)

    return result


# ── Claude Amazon Lookup — web-search-powered product fetch ─────────────────

def claude_amazon_lookup(asin: str) -> dict:
    """Fetch Amazon product data for an ASIN using Claude + web search.

    Amazon blocks our datacenter HTTP fetches (bot detection) and the
    Grok/xAI integration is unreliable after the Apr 10 SerpApi swap.
    Claude's web_search tool runs the fetch from Anthropic's side and
    reads the real product page, so it produces results on URLs where
    our own scraper comes back empty.

    Used as a third-tier fallback inside `_lookup_amazon` after the
    direct scrape and Grok enrichment both return nothing. ~$0.003
    per call, ~4s latency (one extra round-trip vs direct scrape).

    Returns the same shape as `_scrape_generic` so the caller can
    merge results in without special-casing. Fields:
        title, list_price, sale_price, mfg_number, manufacturer,
        upc, photo_url, price, source="claude_web_search".
    On any error returns an empty dict (never raises).
    """
    if not asin:
        return {}
    api_key, _key_skip = try_env(
        "ANTHROPIC_API_KEY",
        severity=Severity.WARNING,
        where="claude_amazon_lookup",
    )
    if _key_skip is not None:
        _record_skip(_key_skip)
        return {}
    if not HAS_REQUESTS:
        _record_skip(SkipReason(
            name="requests",
            reason="requests library not installed",
            severity=Severity.WARNING,
            where="claude_amazon_lookup",
        ))
        return {}

    url = f"https://www.amazon.com/dp/{asin}"
    prompt = (
        f"Fetch this Amazon product page: {url}\n\n"
        "Use the web search tool to read the actual page, then return "
        "ONLY a JSON object with these fields (no prose, no code fence):\n"
        "{\n"
        '  "title": "<product title, 5-200 chars>",\n'
        '  "list_price": <number or null, the MSRP / "List:" price>,\n'
        '  "sale_price": <number or null, the current displayed price>,\n'
        '  "manufacturer": "<brand/manufacturer name>",\n'
        '  "mfg_number": "<Item model number or Manufacturer Part Number>",\n'
        '  "upc": "<UPC or GTIN digits only, no dashes>",\n'
        '  "photo_url": "<main product image URL>"\n'
        "}\n\n"
        "Rules:\n"
        "- list_price is the original/MSRP; if only one price is shown, "
        "put it in list_price and leave sale_price null.\n"
        "- If a price shows \"Was $X\" or \"List: $X\" with a different "
        "current price, list_price is the Was/List value.\n"
        "- mfg_number: look specifically in the 'Product information', "
        "'Product details', 'Item details', or 'Technical Details' "
        "section of the page for fields labeled 'Item model number', "
        "'Manufacturer Part Number', 'Part Number', or 'Model Number'. "
        "Amazon's Rufus AI finds these in that same section — you should "
        "too. If multiple are present, prefer 'Manufacturer Part Number' "
        "over 'Item model number'. Use empty string only if none exist.\n"
        "- If any field is missing from the page, use null (for numbers) "
        "or empty string (for strings). Do NOT make up values."
    )

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": "claude-haiku-4-5-20251001",
        # Web search tool results can be large; need headroom for tool
        # use + final JSON block. 600 tokens was too tight and
        # truncated the final response in production.
        "max_tokens": 1500,
        "tools": [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 2,
        }],
        "messages": [{"role": "user", "content": prompt}],
    }
    log.info("claude_amazon_lookup %s: calling Claude web_search", asin)

    import json as _json
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers, json=body, timeout=10,
        )
        if resp.status_code != 200:
            log.debug("claude_amazon_lookup %s: HTTP %d %s",
                      asin, resp.status_code, resp.text[:200])
            return {}
        data = resp.json()
    except Exception as e:
        log.debug("claude_amazon_lookup %s: request error: %s", asin, e)
        return {}

    # Pull text out of assistant message blocks. With web_search, the
    # response has tool-use and tool-result blocks interleaved with
    # text; concatenate only the final text blocks.
    text = ""
    for block in (data.get("content") or []):
        if isinstance(block, dict) and block.get("type") == "text":
            text += block.get("text", "") or ""
    text = text.strip()
    if not text:
        log.debug("claude_amazon_lookup %s: empty response", asin)
        return {}

    # Tolerate a fenced response even though we asked for none.
    if text.startswith("```"):
        import re as _re_f
        m = _re_f.match(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", text, _re_f.DOTALL)
        if m:
            text = m.group(1).strip()

    # First balanced JSON object — any prose outside gets discarded.
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        log.debug("claude_amazon_lookup %s: no JSON in response: %r",
                  asin, text[:200])
        return {}
    try:
        parsed = _json.loads(text[start:end + 1])
    except Exception as e:
        log.debug("claude_amazon_lookup %s: JSON parse error: %s (raw=%r)",
                  asin, e, text[:200])
        return {}

    out = {"source": "claude_web_search"}
    for key in ("title", "manufacturer", "mfg_number", "upc", "photo_url"):
        v = parsed.get(key)
        if isinstance(v, str) and v.strip():
            out[key] = v.strip()
    for key in ("list_price", "sale_price"):
        v = parsed.get(key)
        if v is None:
            continue
        try:
            num = float(str(v).replace("$", "").replace(",", ""))
            if 0 < num < 100000:
                out[key] = num
        except (ValueError, TypeError):
            continue

    if "list_price" in out:
        out["price"] = out["list_price"]
    elif "sale_price" in out:
        out["price"] = out["sale_price"]

    if out.get("title") or out.get("price"):
        log.info("claude_amazon_lookup %s: got title=%r price=%s",
                 asin, (out.get("title") or "")[:40], out.get("price"))
    return out


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


# ── Universal result hygiene ─────────────────────────────────────────────────
# Bot-stub pages and login walls register as truthy <title> text ("Amazon.com",
# "Staples | Official Online Store", "Access Denied", etc.) and short-circuit
# the rest of the pipeline. Treat them the same as no title — forces the
# Claude web search fallback tier to fire.
_GARBAGE_TITLE_MARKERS = (
    "amazon.com", "amazon",
    "sign-in", "sign in", "log in", "login",
    "robot check", "captcha", "are you a robot",
    "access denied", "access to this page has been denied",
    "page not found", "not found", "404", "error",
    "just a moment", "cloudflare", "attention required",
    "staples", "uline", "target", "hcl", "home depot",
    "the home depot", "walmart.com", "costco wholesale",
    # Empirical finds 2026-04-19 — bot-stub / 404 / footer titles
    # that the generic scraper was passing through as product names.
    "whoops", "we couldn't find", "we cannot find", "we can't find",
    "follow us", "follow us on facebook", "follow us on instagram",
    "accessibility menu", "accessibility statement",
    "dialog, popup", "dialog popup",
    "mcmaster-carr", "mcmaster",
    "fisher scientific", "thermo fisher",
    "grainger", "concordance", "waxie",
    "henry schein", "medline", "bound tree",
    "file or directory not found",
)


_GARBAGE_EXACT_TITLES = frozenset({
    "amazon.com. spend less. smile more.",
    "amazon.com: online shopping for electronics, apparel, "
    "computers, books, dvds & more",
    "staples\u00ae official online store",
    "staples: official online store",
    "uline - shipping boxes, shipping supplies, packaging materials, "
    "packing supplies",
    "target : expect more. pay less.",
    "target: expect more. pay less.",
    "the home depot",
    "robot check", "page not found", "access denied",
})


def _is_garbage_title(title: str) -> bool:
    """True if the <title> looks like a bot-stub / site landing page.

    Bot detection on Amazon, Staples, HCL, Target, etc. often serves a
    generic landing page whose <title> is just the brand name. A real
    product title is almost always longer than ~20 chars and contains
    something product-specific (size, pack count, model number). Short
    brand-only titles are garbage.
    """
    t = (title or "").strip().lower()
    if not t:
        return True
    # Exact-match known stub pages
    if t in _GARBAGE_EXACT_TITLES:
        return True
    # Bare brand names ("Amazon.com", "Uline", "Staples", "HCL")
    if t in _GARBAGE_TITLE_MARKERS:
        return True
    # Phrases that ALWAYS mean "not a product page", regardless of length.
    # Empirical 2026-04-19: Grainger 404 returns "Whoops, we couldn't find
    # that." (31 chars) which was bypassing the len<30 gate below.
    _ALWAYS_GARBAGE = (
        "whoops", "we couldn't find", "we cannot find", "we can't find",
        "page not found", "not found", "404", "file or directory",
        "access denied", "robot check", "captcha", "are you a robot",
        "just a moment", "attention required", "follow us on",
        "accessibility menu", "dialog, popup", "dialog popup",
    )
    for m in _ALWAYS_GARBAGE:
        if m in t:
            return True
    # Short titles (< 30 chars) that contain a stub marker are garbage.
    # A real product title is almost always longer AND contains product
    # specifics (size, pack count, material) beyond the brand/stub word.
    if len(t) < 30:
        for m in _GARBAGE_TITLE_MARKERS:
            if m in t:
                return True
    return False


def claude_product_lookup(url: str, supplier: str = "") -> dict:
    """Generic Claude web_search product fallback for non-Amazon URLs.

    Mirrors `claude_amazon_lookup` but prompts for any supplier page so
    datacenter-IP-blocked sites (Staples, HCL, Target, S&S, Uline when
    Cloudflare trips) still resolve to structured data. Returns the
    same shape (`title`, `list_price`, `sale_price`, `mfg_number`,
    `manufacturer`, `upc`, `photo_url`, `price`, `source`) so callers
    can merge without branching.

    Returns `{}` on any error — this is a best-effort fallback, never
    fatal.
    """
    if not HAS_REQUESTS:
        _record_skip(SkipReason(
            name="requests",
            reason="requests library not installed",
            severity=Severity.WARNING,
            where="claude_product_lookup",
        ))
        return {}
    api_key, _key_skip = try_env(
        "ANTHROPIC_API_KEY",
        severity=Severity.WARNING,
        where="claude_product_lookup",
    )
    if _key_skip is not None:
        _record_skip(_key_skip)
        return {}

    _sup = supplier or "the retailer"
    prompt = (
        f"Fetch this {_sup} product page: {url}\n\n"
        "Use the web search tool to read the actual page, then return "
        "ONLY a JSON object with these fields (no prose, no code fence):\n"
        "{\n"
        '  "title": "<product title, 5-200 chars>",\n'
        '  "list_price": <number or null, the MSRP / "List:" / strikethrough>,\n'
        '  "sale_price": <number or null, the current displayed price>,\n'
        '  "manufacturer": "<brand/manufacturer name>",\n'
        '  "mfg_number": "<Item model number or Manufacturer Part Number>",\n'
        '  "upc": "<UPC or GTIN digits only, no dashes>",\n'
        '  "photo_url": "<main product image URL>"\n'
        "}\n\n"
        "Rules:\n"
        "- list_price is the original/MSRP; if only one price is shown, "
        "put it in list_price and leave sale_price null.\n"
        "- If a price shows \"Was $X\" or \"List: $X\" with a different "
        "current price, list_price is the Was/List value.\n"
        "- mfg_number: look in the product 'Specifications', 'Product "
        "information', 'Item details', or 'Technical Details' section. "
        "Prefer 'Manufacturer Part Number' over 'Item model number' or "
        "'Model Number'. Use empty string if none exist.\n"
        "- If any field is missing from the page, use null (for numbers) "
        "or empty string (for strings). Do NOT make up values."
    )

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1500,
        "tools": [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 2,
        }],
        "messages": [{"role": "user", "content": prompt}],
    }
    log.info("claude_product_lookup: calling Claude web_search for %s %s",
             _sup, url[:80])

    import json as _json
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers, json=body, timeout=15,
        )
        if resp.status_code != 200:
            log.debug("claude_product_lookup: HTTP %d %s",
                      resp.status_code, resp.text[:200])
            return {}
        data = resp.json()
    except Exception as e:
        log.debug("claude_product_lookup request error: %s", e)
        return {}

    text = ""
    for block in (data.get("content") or []):
        if isinstance(block, dict) and block.get("type") == "text":
            text += block.get("text", "") or ""
    text = text.strip()
    if not text:
        return {}

    if text.startswith("```"):
        import re as _re_f
        m = _re_f.match(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", text, _re_f.DOTALL)
        if m:
            text = m.group(1).strip()

    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        log.debug("claude_product_lookup: no JSON in %r", text[:200])
        return {}
    try:
        parsed = _json.loads(text[start:end + 1])
    except Exception as e:
        log.debug("claude_product_lookup JSON parse error: %s", e)
        return {}

    out = {"source": "claude_web_search"}
    for key in ("title", "manufacturer", "mfg_number", "upc", "photo_url"):
        v = parsed.get(key)
        if isinstance(v, str) and v.strip():
            out[key] = v.strip()
    for key in ("list_price", "sale_price"):
        v = parsed.get(key)
        if v is None:
            continue
        try:
            num = float(str(v).replace("$", "").replace(",", ""))
            if 0 < num < 100000:
                out[key] = num
        except (ValueError, TypeError):
            continue

    if "list_price" in out:
        out["price"] = out["list_price"]
    elif "sale_price" in out:
        out["price"] = out["sale_price"]

    if out.get("title") or out.get("price"):
        log.info("claude_product_lookup %s: got title=%r price=%s",
                 _sup, (out.get("title") or "")[:40], out.get("price"))
    return out


def _merge_claude_fallback(result: dict, url: str, supplier: str) -> dict:
    """If `result` is missing title or price, try Claude web_search and
    merge anything it finds. Never overwrites existing non-empty fields.
    """
    claude = claude_product_lookup(url, supplier)
    if not claude:
        return result
    for k in ("title", "manufacturer", "mfg_number", "upc", "photo_url"):
        if claude.get(k) and not result.get(k):
            result[k] = claude[k]
    for k in ("list_price", "sale_price", "price"):
        if claude.get(k) and not result.get(k):
            result[k] = claude[k]
    if not result.get("description") and result.get("title"):
        result["description"] = result["title"]
    result["fallback_source"] = "claude_web_search"
    return result


def _stamp_ref_identifier(desc: str, asin: str = "", upc: str = "") -> str:
    """Append `REF ASIN:<x>` or `REF UPC:<x>` to a description, idempotent.

    Mike's rule (feedback_item_identity): every catalog/quote line that
    has a known canonical identifier should carry it on the description
    so operators can spot the link back to the source product even when
    the supplier URL is gone (rotated, expired, blocked).

    ASIN preferred over UPC (Amazon is the dominant lookup path).
    Returns the description unchanged when no identifier is available
    or when the same identifier is already stamped on the description.
    """
    desc = (desc or "").strip()
    asin = (asin or "").strip()
    upc = (upc or "").strip()
    if not desc:
        return desc
    # Idempotency — don't re-stamp on repeat lookups
    if asin and (f"REF ASIN:{asin}" in desc or f"REF:ASIN:{asin}" in desc):
        return desc
    if upc and (f"REF UPC:{upc}" in desc or f"REF:UPC:{upc}" in desc):
        return desc
    if asin:
        return f"{desc} (REF ASIN:{asin})"
    if upc:
        return f"{desc} (REF UPC:{upc})"
    return desc


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
    # Bare ASIN/ISBN → construct Amazon URL
    # ASIN: B0 + 8 alphanumeric (e.g. B09V3KXJPB)
    # ISBN-10: 10 digits (e.g. 1644729415)
    if re.match(r'^B0[A-Z0-9]{8}$', url):
        url = f"https://www.amazon.com/dp/{url}"
    elif re.match(r'^\d{10}$', url):
        url = f"https://www.amazon.com/dp/{url}"
    elif re.match(r'^\d{13}$', url):
        # ISBN-13
        url = f"https://www.amazon.com/dp/{url}"
    elif not url.startswith("http"):
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
        elif "sears.com" in host:
            result = _lookup_sears(url)
        else:
            # Generic HTML scrape for all other suppliers
            result = _scrape_generic(url)
            result["supplier"] = supplier

        # ── Universal garbage-title scrub ────────────────────────────
        # Bot stubs and landing pages look like valid titles to the
        # scraper. Treat them as empty so the Claude fallback below can
        # fire. Amazon's own lookup already scrubs internally; this is
        # the safety net for every other supplier.
        if _is_garbage_title(result.get("title", "")):
            log.info("lookup_from_url: garbage title %r from %s → clearing",
                     (result.get("title") or "")[:60], supplier)
            result["title"] = ""
            if _is_garbage_title(result.get("description", "")):
                result["description"] = ""

        # ── Universal Claude web_search fallback ─────────────────────
        # If the primary scraper came back weak (no title or no usable
        # price) and the host isn't login-required and isn't Amazon
        # (Amazon has its own in-line Claude tier), ask Claude to
        # re-fetch the page via web_search. This is the tier that
        # unblocks Staples / HCL / Target / Uline / S&S when datacenter
        # IPs are blocked.
        _has_title = bool(result.get("title"))
        _has_price = bool(result.get("price") or result.get("list_price")
                          or result.get("sale_price"))
        _weak = not _has_title or not _has_price
        if _weak and "amazon.com" not in host and not _is_login_required(url):
            log.info("lookup_from_url: weak result for %s (title=%s price=%s) "
                     "— trying Claude web_search fallback",
                     supplier, _has_title, _has_price)
            result = _merge_claude_fallback(result, url, supplier)

        # ── Universal single-price-as-MSRP promotion ─────────────────
        # When a page shows only one price (no strikethrough / no
        # "Was" / no List label), that price IS the MSRP. Promote sale
        # → list so the UI never warns "MSRP not found" and the
        # discount calculator isn't fed a fake sale. Applied for every
        # supplier, not just Amazon. Incident 2026-04-14.
        if (result.get("list_price") is None
                and result.get("sale_price") is not None):
            log.info("lookup_from_url %s: single price $%s — promoting to list_price",
                     supplier, result["sale_price"])
            result["list_price"] = result["sale_price"]
            result["sale_price"] = None
            if not result.get("price"):
                result["price"] = result["list_price"]
            if not result.get("cost"):
                result["cost"] = result["list_price"]

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

        # Use the richest description available
        # Prefer meta_description (often richer from JSON-LD or meta tags) over bare title
        if result.get("meta_description") and len(result["meta_description"]) > len(result.get("description") or ""):
            result["description"] = result["meta_description"]
        # Fall back to title if still empty
        if not result["description"] and result["title"]:
            result["description"] = result["title"]

        # ── Stamp REF: ASIN/UPC onto the description ─────────────────
        # Mike 2026-04-19: "for catalog, URL should add REF: ASIN or
        # UPC when known". Stamping at lookup time means every
        # downstream writer (catalog, PC item, RFQ line) inherits the
        # canonical identifier without each call site having to
        # remember. Idempotent — won't re-stamp if already present.
        result["description"] = _stamp_ref_identifier(
            result.get("description", ""),
            asin=result.get("asin", ""),
            upc=result.get("upc", ""),
        )

        result["ok"] = "error" not in result and bool(
            result.get("title") or result.get("price"))
        log.info("item_link_lookup: %s → supplier=%s price=%s part=%s",
                 url[:60], result["supplier"], result.get("price"), result.get("part_number"))
        return result

    except Exception as e:
        log.error("lookup_from_url %s: %s", url[:80], e)
        return {"ok": False, "error": str(e), "supplier": supplier, "url": url}
