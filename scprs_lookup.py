#!/usr/bin/env python3
"""
SCPRS Price Lookup — FI$Cal PeopleSoft scraper + local price cache.
Searches the State Contract & Procurement Registration System for
historical purchase prices on items requested in RFQs.

Strategy: search by description/item#, parse results, click into detail
pages for unit pricing, prioritize recent (18mo) lowest-price wins.
"""

import json, os, re, logging, time
from datetime import datetime, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False

log = logging.getLogger("scprs")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "scprs_prices.json")

# FI$Cal SCPRS PeopleSoft URLs
SCPRS_BASE = "https://suppliers.fiscal.ca.gov"
SCPRS_SEARCH_URL = f"{SCPRS_BASE}/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL"
SCPRS_DETAIL_URL = f"{SCPRS_BASE}/psc/psfpd1_1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS2_CMP.GBL"

# PeopleSoft form field IDs
FIELD_DESCRIPTION = "ZZ_SCPRS_SP_WRK_DESCR254"
FIELD_DEPT = "ZZ_SCPRS_SP_WRK_BUSINESS_UNIT"
FIELD_PO_NUM = "ZZ_SCPRS_SP_WRK_CRDMEM_ACCT_NBR"
FIELD_SUPPLIER_ID = "ZZ_SCPRS_SP_WRK_SUPPLIER_ID"
FIELD_SUPPLIER_NAME = "ZZ_SCPRS_SP_WRK_NAME1"
FIELD_ACQ_TYPE = "ZZ_SCPRS_SP_WRK_ZZ_ACQ_TYPE"
FIELD_ACQ_METHOD = "ZZ_SCPRS_SP_WRK_ZZ_ACQ_MTHD"
FIELD_FROM_DATE = "ZZ_SCPRS_SP_WRK_FROM_DATE"
FIELD_TO_DATE = "ZZ_SCPRS_SP_WRK_TO_DATE"
SEARCH_BUTTON = "ZZ_SCPRS_SP_WRK_BUTTON"
CLEAR_BUTTON = "ZZ_SCPRS_SP_WRK_BUTTON1"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


# ── Local Price DB ─────────────────────────────────────────────────

def _load_db():
    if os.path.exists(DB_PATH):
        with open(DB_PATH) as f:
            return json.load(f)
    return {}

def _save_db(db):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2, default=str)


def save_price(item_number, description, price, vendor="", source="manual",
               unit_price=None, quantity=None, po_number="", start_date=""):
    """Save a price to the local SCPRS database."""
    db = _load_db()
    key = item_number if item_number else description[:50]
    db[key] = {
        "price": float(unit_price or price),
        "grand_total": float(price),
        "unit_price": float(unit_price) if unit_price else None,
        "quantity": float(quantity) if quantity else None,
        "description": description,
        "item_number": item_number,
        "vendor": vendor,
        "po_number": po_number,
        "source": source,
        "date": start_date or datetime.now().isoformat(),
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


# ── PeopleSoft Session Manager ────────────────────────────────────

class FiscalSession:
    """Manages a session with FI$Cal PeopleSoft for SCPRS searches."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.icsid = None
        self.initialized = False
        self._last_page_html = None

    def init_session(self):
        """Load the SCPRS search page to get cookies + ICSID token."""
        try:
            url = f"{SCPRS_SEARCH_URL}?&"
            # First hit — sets PeopleSoft cookies
            r1 = self.session.get(url, timeout=15, allow_redirects=True)
            log.info(f"SCPRS init hit 1: {r1.status_code} ({len(r1.text)} bytes)")

            page = r1.text
            if "ZZ_SCPRS" not in page and "ICSID" not in page:
                # Second hit — PeopleSoft often needs this
                time.sleep(1)
                r2 = self.session.get(url, timeout=15, allow_redirects=True)
                page = r2.text
                log.info(f"SCPRS init hit 2: {r2.status_code} ({len(page)} bytes)")

            if "ZZ_SCPRS" not in page and "ICSID" not in page:
                # Third try just in case
                time.sleep(1)
                r3 = self.session.get(url, timeout=15, allow_redirects=True)
                page = r3.text
                log.info(f"SCPRS init hit 3: {r3.status_code} ({len(page)} bytes)")

            self.icsid = self._extract_icsid(page)
            self._last_page_html = page
            if self.icsid:
                self.initialized = True
                log.info(f"SCPRS session initialized, ICSID={self.icsid[:20]}...")
                return True
            else:
                log.error("Could not extract ICSID from SCPRS page")
                return False
        except Exception as e:
            log.error(f"SCPRS session init failed: {e}")
            return False

    def _extract_icsid(self, html):
        m = re.search(r"name='ICSID'\s+id='ICSID'\s+value='([^']*)'", html)
        if m:
            return m.group(1)
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("input", {"id": "ICSID"})
        return tag["value"] if tag else None

    def _extract_state_num(self, html):
        m = re.search(r"name='ICStateNum'\s+id='ICStateNum'\s+value='(\d+)'", html)
        return m.group(1) if m else "1"

    def _build_form_data(self, html, action, extra_fields=None):
        """Build a PeopleSoft form POST body."""
        data = {
            "ICType": "Panel",
            "ICElementNum": "0",
            "ICStateNum": self._extract_state_num(html),
            "ICAction": action,
            "ICModelCancel": "0",
            "ICXPos": "0",
            "ICYPos": "0",
            "ResponsetoDiffFrame": "-1",
            "TargetFrameName": "None",
            "FacetPath": "None",
            "ICFocus": "",
            "ICSaveWarningFilter": "0",
            "ICChanged": "-1",
            "ICSkipPending": "0",
            "ICAutoSave": "0",
            "ICResubmit": "0",
            "ICSID": self.icsid or "",
            "ICActionPrompt": "false",
            "ICBcDomData": "",
            "ICPanelName": "",
            "ICFind": "",
            "ICAddCount": "",
            "ICAppClsData": "",
        }
        m = re.search(r"name='DUMMY_FIELD\$hnewpers\$0'[^>]*value='([^']*)'", html)
        if m:
            data["DUMMY_FIELD$hnewpers$0"] = m.group(1)
        if extra_fields:
            data.update(extra_fields)
        return data

    def search(self, description="", from_date="", to_date=""):
        """Search SCPRS by description keyword. Returns list of result dicts."""
        if not self.initialized:
            if not self.init_session():
                return []

        # Load search page — may need double-load like init
        page_html = None
        try:
            url = f"{SCPRS_SEARCH_URL}?&"
            r = self.session.get(url, timeout=15)
            page_html = r.text
            
            if "ZZ_SCPRS" not in page_html and "ICSID" not in page_html:
                time.sleep(0.5)
                r = self.session.get(url, timeout=15)
                page_html = r.text
            
            new_icsid = self._extract_icsid(page_html)
            if new_icsid:
                self.icsid = new_icsid
        except Exception as e:
            log.error(f"Failed to load search page: {e}")
            page_html = self._last_page_html or ""

        search_fields = {
            FIELD_DESCRIPTION: description,
            FIELD_DEPT: "",
            FIELD_PO_NUM: "",
            FIELD_ACQ_TYPE: "",
            FIELD_SUPPLIER_ID: "",
            FIELD_SUPPLIER_NAME: "",
            FIELD_ACQ_METHOD: "",
            FIELD_FROM_DATE: from_date,
            FIELD_TO_DATE: to_date,
        }

        form_data = self._build_form_data(page_html, SEARCH_BUTTON, search_fields)

        try:
            r = self.session.post(SCPRS_SEARCH_URL, data=form_data, timeout=30)
            log.info(f"SCPRS search '{description}': {r.status_code} ({len(r.text)} bytes)")
            self._last_page_html = r.text
            new_icsid = self._extract_icsid(r.text)
            if new_icsid:
                self.icsid = new_icsid
            return self._parse_results(r.text)
        except Exception as e:
            log.error(f"SCPRS search failed: {e}")
            return []

    def get_detail(self, results_html, row_index):
        """Click into a search result to get detail page with unit prices."""
        action_id = f"ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR${row_index}"
        form_data = self._build_form_data(results_html, action_id)

        # Re-include search field values (they stay blank since we already searched)
        for fld in [FIELD_DESCRIPTION, FIELD_DEPT, FIELD_PO_NUM, FIELD_ACQ_TYPE,
                    FIELD_SUPPLIER_ID, FIELD_SUPPLIER_NAME, FIELD_ACQ_METHOD,
                    FIELD_FROM_DATE, FIELD_TO_DATE]:
            # Extract current value from the results page
            m = re.search(rf"name='{re.escape(fld)}'[^>]*value=\"([^\"]*)\"", results_html)
            if m:
                form_data[fld] = m.group(1)
            elif fld not in form_data:
                form_data[fld] = ""

        try:
            r = self.session.post(SCPRS_SEARCH_URL, data=form_data, timeout=30)
            log.info(f"SCPRS detail row {row_index}: {r.status_code}")
            if r.status_code == 200:
                return self._parse_detail(r.text)
        except Exception as e:
            log.error(f"SCPRS detail fetch failed: {e}")
        return None

    def _parse_results(self, html):
        """Parse the SCPRS search results table."""
        results = []
        soup = BeautifulSoup(html, "html.parser")

        if "No matching values" in html or "no data" in html.lower():
            return []

        # Find results grid
        grid = soup.find("table", class_="PSLEVEL1GRID")
        if not grid:
            count_match = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
            if count_match:
                log.info(f"Result count {count_match.group(0)} but grid parse failed")
            return []

        rows = grid.find_all("tr")
        data_rows = rows[1:] if len(rows) > 1 else []

        # Columns (from screenshot): Dept Name, PO#, Assoc POs, First Item Title,
        #   Start Date, End Date, Grand Total, Supplier ID, Supplier Name, Cert Type, Acq Type
        col_fields = ["dept", "po_number", "associated_pos", "first_item",
                      "start_date", "end_date", "grand_total", "supplier_id",
                      "supplier_name", "cert_type", "acq_type"]

        for idx, row in enumerate(data_rows):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            entry = {"_row_index": idx}
            for i, cell in enumerate(cells):
                if i < len(col_fields):
                    entry[col_fields[i]] = cell.get_text(strip=True)

            # Parse grand_total to float
            if "grand_total" in entry:
                try:
                    entry["grand_total_num"] = float(re.sub(r'[^\d.]', '', entry["grand_total"]))
                except ValueError:
                    entry["grand_total_num"] = 0.0

            # Parse start_date
            if "start_date" in entry:
                try:
                    entry["start_date_parsed"] = datetime.strptime(entry["start_date"], "%m/%d/%Y")
                except (ValueError, TypeError):
                    entry["start_date_parsed"] = None

            entry["_results_html"] = html
            results.append(entry)

        log.info(f"SCPRS parsed {len(results)} result rows")
        return results

    def _parse_detail(self, html):
        """Parse the SCPRS detail page for line item unit prices."""
        soup = BeautifulSoup(html, "html.parser")
        detail = {}

        field_map = {
            "ZZ_SCPR_SBP_WRK_BUSINESS_UNIT": "dept_code",
            "ZZ_SCPR_SBP_WRK_DESCR": "dept_name",
            "ZZ_SCPR_SBP_WRK_CRDMEM_ACCT_NBR": "po_number",
            "ZZ_SCPR_SBP_WRK_STATUS1": "status",
            "ZZ_SCPR_SBP_WRK_START_DATE": "start_date",
            "ZZ_SCPR_SBP_WRK_END_DATE": "end_date",
            "ZZ_SCPR_SBP_WRK_NAME1": "supplier",
            "ZZ_SCPR_SBP_WRK_ZZ_COMMENT1": "acq_type",
            "ZZ_SCPR_SBP_WRK_ZZ_ACQ_MTHD": "acq_method",
            "ZZ_SCPR_SBP_WRK_MERCH_AMT_TTL": "merch_amount",
            "ZZ_SCPR_SBP_WRK_ADJ_AMT_TTL": "freight_tax",
            "ZZ_SCPR_SBP_WRK_AWARDED_AMT": "grand_total",
            "ZZ_SCPR_SBP_WRK_BUYER_DESCR": "buyer_name",
            "ZZ_SCPR_SBP_WRK_EMAILID": "buyer_email",
        }

        for field_id, key in field_map.items():
            el = soup.find(id=field_id)
            if el:
                val = el.get_text(strip=True)
                if val and val != "\xa0":
                    detail[key] = val

        # Parse line items
        detail["line_items"] = []
        row_idx = 0
        while True:
            desc_el = soup.find(id=f"ZZ_SCPR_PDL_DVW_DESCR254_MIXED${row_idx}")
            if not desc_el:
                break

            line = {
                "line_num": _get_text(soup, f"ZZ_SCPR_PDL_DVW_CRDMEM_ACCT_NBR${row_idx}"),
                "item_id": _get_text(soup, f"ZZ_SCPR_PDL_DVW_INV_ITEM_ID${row_idx}"),
                "description": _get_text(soup, f"ZZ_SCPR_PDL_DVW_DESCR254_MIXED${row_idx}"),
                "unspsc": _get_text(soup, f"ZZ_SCPR_PDL_DVW_PV_UNSPSC_CODE${row_idx}"),
                "unit_of_measure": _get_text(soup, f"ZZ_SCPR_PDL_DVW_DESCR${row_idx}"),
                "quantity": _get_text(soup, f"ZZ_SCPR_PDL_DVW_QUANTITY${row_idx}"),
                "unit_price": _get_text(soup, f"ZZ_SCPR_PDL_DVW_UNIT_PRICE${row_idx}"),
                "line_total": _get_text(soup, f"ZZ_SCPR_PDL_DVW_LINE_TOTAL${row_idx}"),
                "status": _get_text(soup, f"ZZ_SCPR_PDL_DVW_DESCR1${row_idx}"),
            }

            if line["unit_price"]:
                try:
                    line["unit_price_num"] = float(re.sub(r'[^\d.]', '', line["unit_price"]))
                except ValueError:
                    line["unit_price_num"] = None
            else:
                line["unit_price_num"] = None

            if line["quantity"]:
                try:
                    line["quantity_num"] = float(line["quantity"].replace(",", ""))
                except ValueError:
                    line["quantity_num"] = None

            detail["line_items"].append(line)
            row_idx += 1

        log.info(f"SCPRS detail: PO={detail.get('po_number')}, {len(detail['line_items'])} lines")
        return detail


def _get_text(soup, element_id):
    el = soup.find(id=element_id)
    if el:
        t = el.get_text(strip=True)
        return t if t and t != "\xa0" else ""
    return ""


# ── High-Level Lookup Functions ────────────────────────────────────

_fiscal_session = None

def _get_session():
    global _fiscal_session
    if _fiscal_session is None or not _fiscal_session.initialized:
        _fiscal_session = FiscalSession()
    return _fiscal_session


def lookup_price(item_number=None, description=None):
    """
    Look up last winning SCPRS price for an item.
    1. Check local cache  2. Scrape FI$Cal SCPRS
    Returns dict: {price, source, date, confidence, vendor} or None
    """
    db = _load_db()

    # 1. Local DB by item number
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

    # 2. Local DB fuzzy match
    if description:
        desc_lower = description.lower().split("\n")[0].strip()
        best_match = None
        best_score = 0
        for key, entry in db.items():
            entry_desc = entry.get("description", "").lower()
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

    # 3. Scrape FI$Cal SCPRS
    if HAS_SCRAPER and (item_number or description):
        result = _scrape_fiscal(item_number, description)
        if result:
            save_price(
                item_number=item_number or "",
                description=description or "",
                price=result.get("unit_price") or result["price"],
                vendor=result.get("vendor", ""),
                unit_price=result.get("unit_price"),
                quantity=result.get("quantity"),
                po_number=result.get("po_number", ""),
                start_date=result.get("date", ""),
                source="fiscal_scprs"
            )
            return result

    return None


def _scrape_fiscal(item_number=None, description=None):
    """
    Search FI$Cal SCPRS for the best price.
    Runs multiple keyword searches, collects results, filters to 18 months,
    clicks into details for unit prices, returns lowest recent price.
    """
    if not HAS_SCRAPER:
        return None

    try:
        session = _get_session()
        if not session.initialized:
            if not session.init_session():
                return None

        search_terms = _build_search_terms(item_number, description)
        log.info(f"SCPRS search terms: {search_terms}")

        all_results = []
        seen_pos = set()

        for term in search_terms[:3]:
            results = session.search(description=term)
            for r in results:
                po = r.get("po_number", "")
                if po and po not in seen_pos:
                    seen_pos.add(po)
                    all_results.append(r)
            time.sleep(0.5)

        if not all_results:
            return None

        log.info(f"SCPRS total unique results: {len(all_results)}")

        # Prioritize last 18 months
        cutoff = datetime.now() - timedelta(days=548)
        recent = [r for r in all_results
                  if r.get("start_date_parsed") and r["start_date_parsed"] >= cutoff]
        candidates = recent if recent else all_results
        candidates.sort(key=lambda x: x.get("start_date_parsed") or datetime.min, reverse=True)

        # Try getting unit prices from detail pages for top candidates
        best_result = None

        for candidate in candidates[:5]:
            po = candidate.get("po_number", "")
            results_html = candidate.get("_results_html", "")
            row_idx = candidate.get("_row_index", 0)

            detail = None
            if results_html:
                try:
                    detail = session.get_detail(results_html, row_idx)
                    time.sleep(0.3)
                except Exception as e:
                    log.warning(f"Detail failed for PO {po}: {e}")

            if detail and detail.get("line_items"):
                best_line = _find_best_line_match(detail["line_items"], item_number, description)
                if best_line and best_line.get("unit_price_num"):
                    result = {
                        "price": best_line["unit_price_num"],
                        "unit_price": best_line["unit_price_num"],
                        "quantity": best_line.get("quantity_num"),
                        "source": "fiscal_scprs",
                        "date": candidate.get("start_date", ""),
                        "confidence": "high",
                        "vendor": candidate.get("supplier_name", ""),
                        "po_number": po,
                        "line_desc": best_line.get("description", ""),
                        "dept": candidate.get("dept", ""),
                    }
                    # Keep the lowest price found
                    if best_result is None or result["price"] < best_result["price"]:
                        best_result = result
            else:
                # Fallback: grand total from summary
                grand = candidate.get("grand_total_num", 0)
                if grand > 0 and best_result is None:
                    best_result = {
                        "price": grand,
                        "unit_price": None,
                        "source": "fiscal_scprs_summary",
                        "date": candidate.get("start_date", ""),
                        "confidence": "low",
                        "vendor": candidate.get("supplier_name", ""),
                        "po_number": po,
                        "note": "Grand total only — unit price unavailable",
                    }

        return best_result

    except Exception as e:
        log.error(f"SCPRS scrape error: {e}", exc_info=True)
        return None


def _build_search_terms(item_number=None, description=None):
    """Build search keywords from item info."""
    terms = []

    if item_number:
        terms.append(item_number.strip())

    if description:
        desc = description.strip()
        first_line = desc.split("\n")[0].strip()

        # Extract manufacturer part number patterns
        mfr_match = re.search(r'(?:mfr|mfg|manufacturer|item\s*#?)[#:\s]*(\S+)', desc, re.I)
        if mfr_match:
            terms.append(mfr_match.group(1))

        # Clean first line as keyword search
        clean = re.sub(r'\b(the|and|for|with|each|per|unit|item|qty|no|number)\b', '', first_line, flags=re.I)
        clean = re.sub(r'[,;()\[\]{}#]', ' ', clean)
        clean = ' '.join(clean.split())[:50]
        if clean and clean not in terms:
            terms.append(clean)

        # Shorter 2-3 keyword version
        words = [w for w in first_line.split() if len(w) > 3 and w.lower() not in
                 {"the", "and", "for", "with", "each", "per", "unit", "item"}]
        if len(words) >= 2:
            short = ' '.join(words[:3])
            if short not in terms:
                terms.append(short)

    return terms


def _find_best_line_match(line_items, item_number=None, description=None):
    """Find the line item that best matches our item."""
    if not line_items:
        return None
    if len(line_items) == 1:
        return line_items[0]

    best = None
    best_score = -1

    for line in line_items:
        score = 0
        line_desc = (line.get("description") or "").lower()

        if item_number:
            item_clean = item_number.strip().lower()
            if item_clean in line_desc:
                score += 100
            elif item_clean.replace("-", "") in line_desc.replace("-", ""):
                score += 80

        if description:
            desc_words = set(description.lower().split())
            line_words = set(line_desc.split())
            score += len(desc_words & line_words) * 5

        if line.get("status", "").lower() == "active":
            score += 2
        if line.get("unit_price_num") and line["unit_price_num"] > 0:
            score += 1

        if score > best_score:
            best_score = score
            best = line

    return best


def bulk_lookup(line_items):
    """Look up SCPRS prices for all line items."""
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
            item["scprs_vendor"] = result.get("vendor", "")
            item["scprs_date"] = result.get("date", "")
            item["scprs_po"] = result.get("po_number", "")
        results.append(item)
    return results


def test_connection():
    """Test if we can reach FI$Cal SCPRS (may need 2 loads like browser)."""
    if not HAS_SCRAPER:
        return False, "requests/beautifulsoup not installed"
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        url = f"{SCPRS_SEARCH_URL}?&"

        # First hit — sets cookies/session
        r1 = s.get(url, timeout=8, allow_redirects=True)
        page = r1.text
        info = f"Hit1: {r1.status_code} ({len(page)}b)"

        if "SCPRS" not in page and "ZZ_SCPRS" not in page:
            # Second hit — PeopleSoft often needs this to initialize
            time.sleep(0.5)
            r2 = s.get(url, timeout=8, allow_redirects=True)
            page = r2.text
            info += f" | Hit2: {r2.status_code} ({len(page)}b)"

        if "SCPRS" in page or "ZZ_SCPRS" in page:
            icsid = re.search(r"name='ICSID'[^>]*value='([^']*)'", page)
            return True, f"Connected, ICSID={'found' if icsid else 'missing'} [{info}]"

        # Show what we got for debugging
        title = re.search(r'<title>([^<]*)</title>', page)
        title_txt = title.group(1) if title else "no title"
        snippet = page[:200].replace('\n', ' ')
        return False, f"Page loaded but no SCPRS form. Title: {title_txt} [{info}] Snippet: {snippet}"

    except requests.exceptions.ProxyError as e:
        return False, f"Proxy blocked: {e}"
    except requests.exceptions.ConnectionError as e:
        return False, f"Connection error: {e}"
    except Exception as e:
        return False, f"Error: {e}"


def get_price_db_stats():
    """Return stats about the local price database."""
    db = _load_db()
    sources = {}
    for entry in db.values():
        src = entry.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1
    return {"total_items": len(db), "sources": sources}
