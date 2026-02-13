#!/usr/bin/env python3
"""
SCPRS Price Lookup — checks local price history + scrapes Cal eProcure.
Builds a local price database that gets smarter with each bid.
"""

import json, os, re, logging
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False

log = logging.getLogger("scprs")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "scprs_prices.json")


def _load_db():
    if os.path.exists(DB_PATH):
        with open(DB_PATH) as f:
            return json.load(f)
    return {}

def _save_db(db):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2, default=str)


def lookup_price(item_number=None, description=None):
    """
    Look up last winning SCPRS price.
    First checks local DB, then scrapes Cal eProcure if available.
    Returns dict: {price, source, date, confidence} or None
    """
    db = _load_db()
    
    # 1. Check local DB by item number
    if item_number:
        key = item_number.strip()
        if key in db:
            entry = db[key]
            return {
                "price": entry["price"],
                "source": "local_db",
                "date": entry.get("date", ""),
                "confidence": "high",
                "vendor": entry.get("vendor", ""),
            }
    
    # 2. Check local DB by description fuzzy match
    if description:
        desc_lower = description.lower().split("\n")[0].strip()
        best_match = None
        best_score = 0
        for key, entry in db.items():
            entry_desc = entry.get("description", "").lower()
            # Simple word overlap scoring
            words_a = set(desc_lower.split())
            words_b = set(entry_desc.split())
            if words_a and words_b:
                overlap = len(words_a & words_b)
                score = overlap / max(len(words_a), len(words_b))
                if score > best_score and score > 0.5:
                    best_score = score
                    best_match = entry
        
        if best_match:
            return {
                "price": best_match["price"],
                "source": "local_db_fuzzy",
                "date": best_match.get("date", ""),
                "confidence": "medium",
                "vendor": best_match.get("vendor", ""),
            }
    
    # 3. Try Cal eProcure SCPRS search
    if HAS_SCRAPER and (item_number or description):
        result = _scrape_caleprocure(item_number, description)
        if result:
            # Cache in local DB
            save_price(
                item_number=item_number or "",
                description=description or "",
                price=result["price"],
                vendor=result.get("vendor", ""),
                source="caleprocure"
            )
            return result
    
    return None


def save_price(item_number, description, price, vendor="", source="manual"):
    """Save a price to the local SCPRS database."""
    db = _load_db()
    key = item_number if item_number else description[:50]
    db[key] = {
        "price": float(price),
        "description": description,
        "item_number": item_number,
        "vendor": vendor,
        "source": source,
        "date": datetime.now().isoformat(),
    }
    _save_db(db)


def save_prices_from_rfq(rfq_data):
    """After a successful bid, save all SCPRS prices for future lookups."""
    for item in rfq_data.get("line_items", []):
        if item.get("scprs_last_price") and item["scprs_last_price"] > 0:
            save_price(
                item_number=item.get("item_number", ""),
                description=item.get("description", ""),
                price=item["scprs_last_price"],
                source="user_entry"
            )


def bulk_lookup(line_items):
    """Look up SCPRS prices for all line items. Returns updated items."""
    results = []
    for item in line_items:
        result = lookup_price(
            item_number=item.get("item_number"),
            description=item.get("description")
        )
        if result:
            item["scprs_last_price"] = result["price"]
            item["scprs_source"] = result["source"]
            item["scprs_confidence"] = result["confidence"]
        results.append(item)
    return results


def _scrape_caleprocure(item_number=None, description=None):
    """
    Scrape Cal eProcure SCPRS search for price data.
    URL: https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx
    """
    if not HAS_SCRAPER:
        return None
    
    try:
        search_term = item_number or (description.split("\n")[0][:50] if description else "")
        if not search_term:
            return None
        
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        
        # Load search page to get viewstate
        url = "https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx"
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Extract ASP.NET form fields
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate = viewstate["value"] if viewstate else ""
        validation = soup.find("input", {"name": "__EVENTVALIDATION"})
        validation = validation["value"] if validation else ""
        
        # Submit search
        form_data = {
            "__VIEWSTATE": viewstate,
            "__EVENTVALIDATION": validation,
            "ctl00$ContentPlaceHolder1$txtDescription": search_term,
            "ctl00$ContentPlaceHolder1$btnSearch": "Search",
        }
        
        resp = session.post(url, data=form_data, timeout=15)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Parse results table
        table = soup.find("table", {"id": lambda x: x and "GridView" in x})
        if not table:
            return None
        
        rows = table.find_all("tr")[1:]  # Skip header
        if not rows:
            return None
        
        # Get most recent (first row typically)
        cells = rows[0].find_all("td")
        if len(cells) >= 6:
            try:
                price_text = cells[5].get_text(strip=True)  # Unit price column
                price = float(re.sub(r'[^\d.]', '', price_text))
                vendor = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                return {
                    "price": price,
                    "source": "caleprocure",
                    "date": cells[0].get_text(strip=True) if cells else "",
                    "confidence": "high",
                    "vendor": vendor,
                }
            except (ValueError, IndexError):
                pass
        
        return None
        
    except Exception as e:
        log.warning(f"Cal eProcure scrape failed: {e}")
        return None


def get_price_db_stats():
    """Return stats about the local price database."""
    db = _load_db()
    return {
        "total_items": len(db),
        "sources": {},
    }
