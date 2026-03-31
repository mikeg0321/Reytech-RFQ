"""
supplier_auth_scraper.py — Authenticated Supplier Scraper Framework

Handles login-required medical/industrial distributor websites.
Each supplier gets a cached session (login once, reuse cookies).
Sessions auto-refresh when expired or on auth failure.

Supported suppliers:
  - Medline Industries (medline.com)
  - Henry Schein (henryschein.com)
  - Bound Tree Medical (boundtree.com)
  - Cardinal Health (cardinalhealth.com)
  - McKesson (mckesson.com)
  - Concordance Healthcare (concordance.com)
  - Owens & Minor (owens-minor.com)

Config: Set env vars per supplier (e.g., MEDLINE_USERNAME, MEDLINE_PASSWORD).
If credentials are missing, falls back to "paste cost manually" message.
"""

import logging
import os
import re
import time
import threading
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

log = logging.getLogger("supplier_auth")

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# ── Session Cache ────────────────────────────────────────────────────────────

_sessions = {}          # supplier_key -> requests.Session
_session_expiry = {}    # supplier_key -> float (timestamp)
_session_lock = threading.Lock()

SESSION_TTL_SECONDS = 3600  # Re-login after 1 hour
REQUEST_TIMEOUT = 15

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ══════════════════════════════════════════════════════════════════════════════
# SUPPLIER REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

SUPPLIER_AUTH_REGISTRY = {
    "medline": {
        "name": "Medline Industries",
        "domain": "medline.com",
        "login_url": "https://www.medline.com/login/",
        "env_user": "MEDLINE_USERNAME",
        "env_pass": "MEDLINE_PASSWORD",
        "env_account": "MEDLINE_ACCOUNT_NUM",
        "product_url_pattern": r"medline\.com/product/([A-Za-z0-9\-]+)",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
            r'data-price="([\d,.]+)"',
            r'class="price[^"]*"[^>]*>\$?([\d,.]+)',
            r'"unitPrice"\s*:\s*"?\$?([\d,.]+)',
            r'"listPrice"\s*:\s*"?\$?([\d,.]+)',
            r'Your\s+Price[^$]*\$\s*([\d,.]+)',
            r'Contract\s+Price[^$]*\$\s*([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
            r'data-sku="([^"]+)"',
            r'Item\s*#?\s*:?\s*([A-Z0-9]{3,20})',
            r'Medline\s*#?\s*:?\s*([A-Z0-9]{3,20})',
            r'"productID"\s*:\s*"([^"]+)"',
        ],
    },
    "henry_schein": {
        "name": "Henry Schein",
        "domain": "henryschein.com",
        "login_url": "https://www.henryschein.com/us-en/Login.aspx",
        "env_user": "HENRY_SCHEIN_USERNAME",
        "env_pass": "HENRY_SCHEIN_PASSWORD",
        "env_account": "HENRY_SCHEIN_ACCOUNT",
        "product_url_pattern": r"henryschein\.com/.*/p/(\d+)",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
            r'class="price"[^>]*>\$?([\d,.]+)',
            r'Your\s+Price[^$]*\$\s*([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
            r'Catalog\s*#?\s*:?\s*(\d{5,10})',
            r'Item\s*#?\s*:?\s*(\d{5,10})',
        ],
    },
    "bound_tree": {
        "name": "Bound Tree Medical",
        "domain": "boundtree.com",
        "login_url": "https://www.boundtree.com/login",
        "env_user": "BOUND_TREE_USERNAME",
        "env_pass": "BOUND_TREE_PASSWORD",
        "env_account": "",
        "product_url_pattern": r"boundtree\.com/[^?]*?([A-Z0-9\-]{4,})",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
            r'class="price"[^>]*>\$?([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
            r'Item\s*#?\s*:?\s*([A-Z0-9\-]{4,15})',
        ],
    },
    "cardinal_health": {
        "name": "Cardinal Health",
        "domain": "cardinalhealth.com",
        "login_url": "https://www.cardinalhealth.com/en/login.html",
        "env_user": "CARDINAL_USERNAME",
        "env_pass": "CARDINAL_PASSWORD",
        "env_account": "CARDINAL_ACCOUNT",
        "product_url_pattern": r"cardinalhealth\.com/.*[/\-](\d{5,})",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
            r'Your\s+Price[^$]*\$\s*([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
            r'Cardinal\s+Item\s*#?\s*:?\s*(\d{5,})',
        ],
    },
    "mckesson": {
        "name": "McKesson",
        "domain": "mckesson.com",
        "login_url": "https://connect.mckesson.com/portal/site/smo/login",
        "env_user": "MCKESSON_USERNAME",
        "env_pass": "MCKESSON_PASSWORD",
        "env_account": "MCKESSON_ACCOUNT",
        "product_url_pattern": r"mckesson\.com/.*[/\-](\d{5,})",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
            r'McKesson\s*#?\s*:?\s*(\d{5,})',
        ],
    },
    "concordance": {
        "name": "Concordance Healthcare",
        "domain": "concordance.com",
        "login_url": "https://www.concordance.com/login",
        "env_user": "CONCORDANCE_USERNAME",
        "env_pass": "CONCORDANCE_PASSWORD",
        "env_account": "",
        "product_url_pattern": r"concordance\.com/.*[/\-]([A-Z0-9]{4,})",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
        ],
    },
    "owens_minor": {
        "name": "Owens & Minor",
        "domain": "owens-minor.com",
        "login_url": "https://www.owens-minor.com/login",
        "env_user": "OWENS_MINOR_USERNAME",
        "env_pass": "OWENS_MINOR_PASSWORD",
        "env_account": "",
        "product_url_pattern": r"owens-minor\.com/.*[/\-]([A-Z0-9]{4,})",
        "price_selectors": [
            r'"price"\s*:\s*"?\$?([\d,.]+)',
        ],
        "sku_selectors": [
            r'"sku"\s*:\s*"([^"]+)"',
        ],
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# SESSION MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def _get_credentials(supplier_key: str) -> tuple:
    """Get (username, password, account_num) from env vars for a supplier."""
    cfg = SUPPLIER_AUTH_REGISTRY.get(supplier_key)
    if not cfg:
        return ("", "", "")
    username = os.environ.get(cfg["env_user"], "")
    password = os.environ.get(cfg["env_pass"], "")
    account = os.environ.get(cfg.get("env_account", ""), "") if cfg.get("env_account") else ""
    return (username, password, account)


def _has_credentials(supplier_key: str) -> bool:
    """Check if credentials are configured for this supplier."""
    user, pw, _ = _get_credentials(supplier_key)
    return bool(user and pw)


def _get_session(supplier_key: str) -> Optional["requests.Session"]:
    """Get or create an authenticated session for a supplier.

    Returns a requests.Session with login cookies, or None if login fails.
    Sessions are cached and reused for SESSION_TTL_SECONDS.
    """
    if not HAS_REQUESTS:
        return None

    with _session_lock:
        # Check cache
        now = time.time()
        if supplier_key in _sessions and now < _session_expiry.get(supplier_key, 0):
            return _sessions[supplier_key]

        # Need to login
        user, pw, account = _get_credentials(supplier_key)
        if not user or not pw:
            log.debug("AUTH: No credentials for %s", supplier_key)
            return None

        cfg = SUPPLIER_AUTH_REGISTRY.get(supplier_key)
        if not cfg:
            return None

        session = requests.Session()
        session.headers.update(_HEADERS)

        try:
            # Step 1: GET login page (collect CSRF/session cookies)
            login_url = cfg["login_url"]
            log.info("AUTH: Logging into %s at %s", cfg["name"], login_url)

            resp = session.get(login_url, timeout=REQUEST_TIMEOUT)

            # Step 2: POST login credentials
            # Extract CSRF token if present
            csrf_token = ""
            csrf_patterns = [
                r'name="csrf[_-]?token"\s+value="([^"]+)"',
                r'name="_token"\s+value="([^"]+)"',
                r'name="__RequestVerificationToken"\s+value="([^"]+)"',
                r'"csrfToken"\s*:\s*"([^"]+)"',
            ]
            for pattern in csrf_patterns:
                m = re.search(pattern, resp.text, re.IGNORECASE)
                if m:
                    csrf_token = m.group(1)
                    break

            # Build login payload
            login_data = {
                "username": user,
                "password": pw,
                "email": user,  # Some sites use email field
            }
            if csrf_token:
                login_data["_token"] = csrf_token
                login_data["csrf_token"] = csrf_token
            if account:
                login_data["account"] = account
                login_data["accountNumber"] = account

            login_resp = session.post(
                login_url, data=login_data,
                timeout=REQUEST_TIMEOUT, allow_redirects=True,
            )

            # Validate login success
            # Check for common failure indicators
            resp_text = login_resp.text.lower()
            if any(indicator in resp_text for indicator in [
                "invalid password", "login failed", "incorrect credentials",
                "authentication failed", "invalid username", "access denied",
            ]):
                log.warning("AUTH: Login FAILED for %s — check credentials", cfg["name"])
                return None

            # Check for success indicators (redirected to dashboard/home, got session cookie)
            if login_resp.status_code in (200, 302) and len(session.cookies) > 0:
                _sessions[supplier_key] = session
                _session_expiry[supplier_key] = now + SESSION_TTL_SECONDS
                log.info("AUTH: %s login SUCCESS (cookies: %d, expires in %ds)",
                         cfg["name"], len(session.cookies), SESSION_TTL_SECONDS)
                return session

            # Ambiguous — try using the session anyway (some sites don't redirect)
            _sessions[supplier_key] = session
            _session_expiry[supplier_key] = now + SESSION_TTL_SECONDS
            log.info("AUTH: %s login (ambiguous response %d, trying session anyway)",
                     cfg["name"], login_resp.status_code)
            return session

        except requests.exceptions.Timeout:
            log.warning("AUTH: %s login timed out", cfg["name"])
            return None
        except Exception as e:
            log.warning("AUTH: %s login error: %s", cfg["name"], e)
            return None


def invalidate_session(supplier_key: str):
    """Force re-login on next request."""
    with _session_lock:
        _sessions.pop(supplier_key, None)
        _session_expiry.pop(supplier_key, None)


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT SCRAPING
# ══════════════════════════════════════════════════════════════════════════════

def lookup_product(url: str, supplier_key: str = "") -> dict:
    """Scrape a product page from a login-required supplier.

    Args:
        url: Full product URL (e.g., https://www.medline.com/product/...)
        supplier_key: Registry key (e.g., "medline"). Auto-detected from URL if empty.

    Returns:
        Dict matching item_link_lookup.py schema:
        {ok, supplier, url, title, description, price, list_price, part_number,
         mfg_number, manufacturer, error}
    """
    if not HAS_REQUESTS:
        return {"ok": False, "error": "requests library not installed", "url": url}

    # Auto-detect supplier from URL
    if not supplier_key:
        supplier_key = _detect_supplier_key(url)

    cfg = SUPPLIER_AUTH_REGISTRY.get(supplier_key)
    if not cfg:
        return {
            "ok": False,
            "error": f"Unknown supplier for URL: {url}",
            "url": url, "login_required": True,
        }

    # Check if we have credentials
    if not _has_credentials(supplier_key):
        return {
            "ok": False,
            "error": f"{cfg['name']} requires login — set {cfg['env_user']} and "
                     f"{cfg['env_pass']} env vars, or paste cost manually",
            "supplier": cfg["name"],
            "url": url,
            "login_required": True,
            "credentials_missing": True,
        }

    # Get authenticated session
    session = _get_session(supplier_key)
    if not session:
        return {
            "ok": False,
            "error": f"{cfg['name']} login failed — check credentials",
            "supplier": cfg["name"],
            "url": url,
            "login_required": True,
        }

    # Fetch product page
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)

        # Check if we got redirected to login (session expired)
        if "login" in resp.url.lower() and resp.url != url:
            log.info("AUTH: %s session expired (redirected to login), re-authenticating",
                     cfg["name"])
            invalidate_session(supplier_key)
            session = _get_session(supplier_key)
            if not session:
                return {"ok": False, "error": f"{cfg['name']} re-login failed",
                        "supplier": cfg["name"], "url": url}
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)

        html = resp.text

        # Parse product data
        result = _parse_product_page(html, url, cfg)
        result["supplier"] = cfg["name"]
        result["url"] = url

        if result.get("price") and result["price"] > 0:
            result["ok"] = True
            log.info("SCRAPE: %s — %s $%.2f (SKU: %s)",
                     cfg["name"], result.get("title", "?")[:40],
                     result["price"], result.get("part_number", "?"))
        else:
            result["ok"] = True  # Page loaded, just no price found
            result.setdefault("error", "Product found but price not visible — "
                              "may need contract pricing setup")
            log.info("SCRAPE: %s — page loaded but no price extracted (%s)",
                     cfg["name"], url[:60])

        return result

    except requests.exceptions.Timeout:
        return {"ok": False, "error": f"{cfg['name']} request timed out",
                "supplier": cfg["name"], "url": url}
    except Exception as e:
        log.warning("SCRAPE: %s error: %s", cfg["name"], e)
        return {"ok": False, "error": str(e)[:100],
                "supplier": cfg["name"], "url": url}


def _parse_product_page(html: str, url: str, cfg: dict) -> dict:
    """Extract product data from HTML using supplier-specific patterns."""
    result = {
        "title": "",
        "description": "",
        "price": 0,
        "list_price": 0,
        "part_number": "",
        "mfg_number": "",
        "manufacturer": "",
        "uom": "",
    }

    # ── Title ────────────────────────────────────────────────────────────
    # Try JSON-LD first
    m = re.search(r'"name"\s*:\s*"([^"]{5,200})"', html)
    if m:
        result["title"] = m.group(1).strip()
    else:
        # Fall back to <title> tag
        m = re.search(r"<title>([^<]{5,200})</title>", html, re.IGNORECASE)
        if m:
            result["title"] = m.group(1).split("|")[0].split("-")[0].strip()

    # OG title as fallback
    if not result["title"]:
        m = re.search(r'property="og:title"\s+content="([^"]+)"', html, re.IGNORECASE)
        if m:
            result["title"] = m.group(1).strip()[:200]

    # ── Description ──────────────────────────────────────────────────────
    m = re.search(r'"description"\s*:\s*"([^"]{10,400})"', html)
    if m:
        result["description"] = m.group(1).strip()
    else:
        m = re.search(r'name="description"\s+content="([^"]+)"', html, re.IGNORECASE)
        if m:
            result["description"] = m.group(1).strip()[:400]

    if not result["description"]:
        result["description"] = result["title"]

    # ── Price (supplier-specific patterns) ───────────────────────────────
    for pattern in cfg.get("price_selectors", []):
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            try:
                price = float(m.group(1).replace(",", ""))
                if 0.01 < price < 100000:
                    if not result["price"]:
                        result["price"] = price
                    elif not result["list_price"]:
                        result["list_price"] = price
            except (ValueError, TypeError):
                pass

    # If list_price found but not price, swap
    if result["list_price"] and not result["price"]:
        result["price"] = result["list_price"]

    # ── Part Number / SKU ────────────────────────────────────────────────
    for pattern in cfg.get("sku_selectors", []):
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            result["part_number"] = m.group(1).strip()
            break

    # Also try URL-based extraction
    url_pattern = cfg.get("product_url_pattern", "")
    if url_pattern and not result["part_number"]:
        m = re.search(url_pattern, url, re.IGNORECASE)
        if m:
            result["part_number"] = m.group(1)

    # ── Manufacturer ─────────────────────────────────────────────────────
    m = re.search(r'"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"', html)
    if m:
        result["manufacturer"] = m.group(1).strip()
    else:
        m = re.search(r'"manufacturer"\s*:\s*"([^"]+)"', html)
        if m:
            result["manufacturer"] = m.group(1).strip()

    # ── MFG Number ───────────────────────────────────────────────────────
    m = re.search(r'"mpn"\s*:\s*"([^"]+)"', html, re.IGNORECASE)
    if m:
        result["mfg_number"] = m.group(1).strip()
    else:
        m = re.search(r'MFG\.?\s*#?\s*:?\s*([A-Z0-9\-]{3,20})', html)
        if m and m.group(1) != result["part_number"]:
            result["mfg_number"] = m.group(1)

    # ── UOM ──────────────────────────────────────────────────────────────
    uom_m = re.search(r'\b(each|ea|case|cs|box|bx|pack|pk|roll|rl|pair|pr)\b',
                       html[max(0, html.lower().find("price")-200):
                            html.lower().find("price")+200] if "price" in html.lower() else "",
                       re.IGNORECASE)
    if uom_m:
        result["uom"] = uom_m.group(1).upper()

    return result


def _detect_supplier_key(url: str) -> str:
    """Detect supplier key from URL domain."""
    host = urlparse(url).netloc.lower()
    for key, cfg in SUPPLIER_AUTH_REGISTRY.items():
        if cfg["domain"] in host:
            return key
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK & STATUS
# ══════════════════════════════════════════════════════════════════════════════

def get_supplier_auth_status() -> dict:
    """Return status of all authenticated supplier connections."""
    status = {}
    for key, cfg in SUPPLIER_AUTH_REGISTRY.items():
        has_creds = _has_credentials(key)
        session_active = key in _sessions and time.time() < _session_expiry.get(key, 0)
        user, _, _ = _get_credentials(key)
        status[key] = {
            "name": cfg["name"],
            "domain": cfg["domain"],
            "credentials_configured": has_creds,
            "username": user[:3] + "***" if user else "(not set)",
            "session_active": session_active,
            "session_expires_in": max(0, int(_session_expiry.get(key, 0) - time.time()))
                                  if session_active else 0,
            "env_vars_needed": [cfg["env_user"], cfg["env_pass"]],
        }
    return status


def test_supplier_login(supplier_key: str) -> dict:
    """Test login for a specific supplier. Returns {ok, message, cookies}."""
    invalidate_session(supplier_key)  # Force fresh login
    session = _get_session(supplier_key)
    cfg = SUPPLIER_AUTH_REGISTRY.get(supplier_key, {})
    if session:
        return {
            "ok": True,
            "supplier": cfg.get("name", supplier_key),
            "message": f"Login successful — {len(session.cookies)} cookies stored",
            "cookies": len(session.cookies),
        }
    else:
        if not _has_credentials(supplier_key):
            return {
                "ok": False,
                "supplier": cfg.get("name", supplier_key),
                "message": f"No credentials — set {cfg.get('env_user', '?')} and "
                           f"{cfg.get('env_pass', '?')} env vars",
            }
        return {
            "ok": False,
            "supplier": cfg.get("name", supplier_key),
            "message": "Login failed — check credentials or site may be down",
        }
