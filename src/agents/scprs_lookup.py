


#!/usr/bin/env python3
"""
SCPRS Price Lookup — FI$Cal PeopleSoft scraper + local price cache.

Architecture:
  1. Local price DB (JSON) — instant lookups for previously seen items
  2. FI$Cal PeopleSoft scraper — live searches against SCPRS
     - Uses ID-based element extraction (reliable for PeopleSoft)
     - Handles PeopleSoft's double-load session initialization
     - Prioritizes last 18 months, finds lowest winning bid

PeopleSoft Naming Convention:
  Search page: ZZ_SCPRS1_CMP (form: win0)
  Results grid: ZZ_SCPR_RD_DVW_*$N (N = row index)
  Detail page: ZZ_SCPRS2_CMP (form: win1)
  Detail grid:  ZZ_SCPR_PDL_DVW_*$N (N = row index)
"""

import json, os, re, logging, time
from datetime import datetime, timedelta

try:
    from src.core.circuit_breaker import get_breaker, CircuitOpenError
    _scprs_breaker = get_breaker("scprs")
except ImportError:
    _scprs_breaker = None
    class CircuitOpenError(Exception):
        pass

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False

log = logging.getLogger("scprs")

# v6.0: Won Quotes KB integration (graceful fallback)
try:
    from src.knowledge.won_quotes_db import ingest_scprs_result as _ingest_wq
except ImportError:
    try:
        from src.knowledge.won_quotes_db import ingest_scprs_result as _ingest_wq
    except ImportError:
        _ingest_wq = None

try:
    from src.core.paths import SCPRS_DB_PATH as DB_PATH
except ImportError:
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "scprs_prices.json")

# ── FI$Cal URLs & Field IDs ───────────────────────────────────────

SCPRS_BASE = "https://suppliers.fiscal.ca.gov"
SCPRS_SEARCH_URL = f"{SCPRS_BASE}/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL"
SCPRS_DETAIL_URL = f"{SCPRS_BASE}/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS2_CMP.GBL?Page=ZZ_SCPRS_PDDTL_PG&Action=U"

# Search form fields
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

ALL_SEARCH_FIELDS = [FIELD_DESCRIPTION, FIELD_DEPT, FIELD_PO_NUM, FIELD_ACQ_TYPE,
                     FIELD_SUPPLIER_ID, FIELD_SUPPLIER_NAME, FIELD_ACQ_METHOD,
                     FIELD_FROM_DATE, FIELD_TO_DATE]

# Detail page line-item field ID prefixes (row index appended as $0, $1, ...)
DETAIL_DESCRIPTION  = "ZZ_SCPR_PDL_DVW_DESCR254_MIXED"
DETAIL_ITEM_ID      = "ZZ_SCPR_PDL_DVW_INV_ITEM_ID"
DETAIL_LINE_NUM     = "ZZ_SCPR_PDL_DVW_CRDMEM_ACCT_NBR"
DETAIL_UNSPSC       = "ZZ_SCPR_PDL_DVW_PV_UNSPSC_CODE"
DETAIL_UOM          = "ZZ_SCPR_PDL_DVW_DESCR"
DETAIL_QUANTITY     = "ZZ_SCPR_PDL_DVW_QUANTITY"
DETAIL_UNIT_PRICE   = "ZZ_SCPR_PDL_DVW_UNIT_PRICE"
DETAIL_LINE_TOTAL   = "ZZ_SCPR_PDL_DVW_LINE_TOTAL"
DETAIL_LINE_STATUS  = "ZZ_SCPR_PDL_DVW_DESCR1"

DETAIL_HEADER_FIELDS = {
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
    "ZZ_SCPR_SBP_WRK_PHONE": "buyer_phone",
}

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


# ── Local Price DB ─────────────────────────────────────────────────

def _load_db():
    try:
        if os.path.exists(DB_PATH):
            with open(DB_PATH) as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"Price DB load error: {e}")
    return {}

def _save_db(db):
    try:
        from src.core.data_guard import atomic_json_save
        atomic_json_save(DB_PATH, db)
    except Exception as e:
        log.warning(f"Price DB save error: {e}")

def save_price(item_number, description, price, vendor="", source="manual",
               unit_price=None, quantity=None, po_number="", start_date=""):
    db = _load_db()
    key = (item_number or "").strip() or (description or "")[:50]
    if not key: return
    db[key] = {
        "price": float(unit_price or price),
        "grand_total": float(price) if price else None,
        "unit_price": float(unit_price) if unit_price else None,
        "quantity": float(quantity) if quantity else None,
        "description": description or "",
        "item_number": item_number or "",
        "vendor": vendor, "po_number": po_number,
        "source": source,
        "date": start_date or datetime.now().isoformat(),
    }
    _save_db(db)

def save_prices_from_rfq(rfq_data):
    for item in rfq_data.get("line_items", []):
        if item.get("scprs_last_price") and item["scprs_last_price"] > 0:
            save_price(item_number=item.get("item_number", ""),
                       description=item.get("description", ""),
                       price=item["scprs_last_price"], source="user_entry")


# ── HTML Helpers ───────────────────────────────────────────────────

def _get_text(soup, element_id):
    el = soup.find(id=element_id)
    if el:
        t = el.get_text(strip=True)
        return t if t and t != "\xa0" else ""
    return ""

def _parse_dollar(text):
    if not text: return None
    try: return float(re.sub(r'[^\d.]', '', text))
    except Exception: return None

def _parse_date(text, fmt="%m/%d/%Y"):
    if not text: return None
    try: return datetime.strptime(text.strip(), fmt)
    except Exception: return None

def _discover_grid_ids(soup, prefix):
    """Find all PeopleSoft grid fields for row 0 matching prefix_*$0."""
    fields = {}
    pattern = re.compile(rf'^{re.escape(prefix)}_(\w+)\$0$')
    for el in soup.find_all(id=pattern):
        m = pattern.match(el.get('id', ''))
        if m:
            fields[m.group(1)] = True
    return fields


# ── PeopleSoft Session ─────────────────────────────────────────────

class FiscalSession:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.icsid = None
        self.initialized = False
        self._last_html = None
        self._last_state_num = None
        # Separate session for detail pages (different PeopleSoft server)
        self.detail_session = None
        self._detail_icsid = None

    def _load_page(self, max_attempts=3):
        url = f"{SCPRS_SEARCH_URL}?&"
        page = ""
        for attempt in range(1, max_attempts + 1):
            r = self.session.get(url, timeout=20, allow_redirects=True)
            page = r.text
            log.info(f"SCPRS load {attempt}: {r.status_code} ({len(page)}b)")
            if "ZZ_SCPRS" in page or "ICSID" in page:
                break
            time.sleep(0.5 * attempt)
        return page

    def _extract_icsid(self, html):
        m = re.search(r"name='ICSID'\s+id='ICSID'\s+value='([^']*)'", html)
        return m.group(1) if m else None

    def _extract_state_num(self, html):
        m = re.search(r"name='ICStateNum'\s+id='ICStateNum'\s+value='(\d+)'", html)
        return m.group(1) if m else "1"

    def _build_form_data(self, html, action, search_values=None):
        data = {
            "ICType": "Panel", "ICElementNum": "0",
            "ICStateNum": self._extract_state_num(html),
            "ICAction": action, "ICModelCancel": "0",
            "ICXPos": "0", "ICYPos": "0",
            "ResponsetoDiffFrame": "-1", "TargetFrameName": "None",
            "FacetPath": "None", "ICFocus": "",
            "ICSaveWarningFilter": "0", "ICChanged": "-1",
            "ICSkipPending": "0", "ICAutoSave": "0",
            "ICResubmit": "0", "ICSID": self.icsid or "",
            "ICActionPrompt": "false", "ICBcDomData": "",
            "ICPanelName": "", "ICFind": "", "ICAddCount": "",
            "ICAppClsData": "",
        }
        m = re.search(r"name='DUMMY_FIELD\$hnewpers\$0'[^>]*value='([^']*)'", html)
        if m: data["DUMMY_FIELD$hnewpers$0"] = m.group(1)
        if search_values: data.update(search_values)
        return data

    def init_session(self):
        try:
            page = self._load_page(3)
            self.icsid = self._extract_icsid(page)
            self._last_html = page
            if self.icsid:
                self.initialized = True
                log.info(f"SCPRS session OK")
                return True
            log.error("SCPRS init: no ICSID")
            return False
        except Exception as e:
            log.error(f"SCPRS init failed: {e}")
            return False

    def _init_detail_session(self):
        """Initialize a separate session for ZZ_SCPRS2 (different PeopleSoft server)."""
        try:
            self.detail_session = requests.Session()
            self.detail_session.headers.update({"User-Agent": USER_AGENT})
            url = f"{SCPRS_DETAIL_URL.split('?')[0]}?&"
            for attempt in range(1, 3):
                r = self.detail_session.get(url, timeout=20, allow_redirects=True)
                log.info("SCPRS2 init %d: %d (%db)", attempt, r.status_code, len(r.text))
                if "ICSID" in r.text or "ZZ_SCPRS" in r.text:
                    self._detail_icsid = self._extract_icsid(r.text)
                    if self._detail_icsid:
                        log.info("SCPRS2 session OK")
                        return True
                time.sleep(0.5)
            log.error("SCPRS2 init: no ICSID")
            return False
        except Exception as e:
            log.error("SCPRS2 init failed: %s", e)
            return False

    def search(self, description="", from_date="", to_date="", supplier_name=""):
        if not self.initialized and not self.init_session():
            return []

        try:
            page = self._load_page(2)
            self.icsid = self._extract_icsid(page) or self.icsid
        except Exception as e:
            log.error(f"Search page load: {e}")
            page = self._last_html or ""
            if not page: return []

        search_values = {f: "" for f in ALL_SEARCH_FIELDS}
        search_values[FIELD_DESCRIPTION] = description
        search_values[FIELD_FROM_DATE] = from_date
        search_values[FIELD_TO_DATE] = to_date
        if supplier_name:
            search_values[FIELD_SUPPLIER_NAME] = supplier_name
        form_data = self._build_form_data(page, SEARCH_BUTTON, search_values)

        try:
            r = self.session.post(SCPRS_SEARCH_URL, data=form_data, timeout=20)
            log.info(f"SCPRS search '{description}': {r.status_code} ({len(r.text)}b)")
        except Exception as e:
            log.error(f"SCPRS search POST: {e}")
            return []

        html = r.text
        self._last_html = html
        new_id = self._extract_icsid(html)
        if new_id: self.icsid = new_id
        # Store current ICStateNum from search response for detail clicks
        self._last_state_num = self._extract_state_num(html)
        log.info("Search: ICSID=%s ICStateNum=%s", self.icsid, self._last_state_num)
        return self._parse_results(html)

    def get_po_detail(self, po_number, s2=None):
        """Fetch detail via ZZ_SCPRS2_CMP. Uses provided session or creates one."""
        try:
            if not s2:
                s2 = requests.Session()
                s2.headers.update(self.session.headers)
                s2.cookies.update(self.session.cookies)
                r1 = s2.get(f"{SCPRS_DETAIL_URL}?&", timeout=20, allow_redirects=True)
                log.info("SCPRS2 load: %db", len(r1.text))

            # Load SCPRS2 page to get hidden fields
            page_r = s2.get(f"{SCPRS_DETAIL_URL}?&", timeout=20, allow_redirects=True)
            page = page_r.text
            icsid = self._extract_icsid(page)
            if not icsid:
                time.sleep(0.5)
                page_r = s2.get(f"{SCPRS_DETAIL_URL}?&", timeout=20, allow_redirects=True)
                page = page_r.text
                icsid = self._extract_icsid(page)

            if not icsid:
                log.error("SCPRS2: no ICSID for PO=%s", po_number)
                return None

            # Discover SCPRS2 form structure
            from bs4 import BeautifulSoup as _BS
            _soup = _BS(page, "html.parser")
            _ic_fields = {i.get("name"): i.get("value", "")
                          for i in _soup.find_all("input")
                          if (i.get("name") or "").startswith("IC")}
            _links = [a.get("id", "") for a in _soup.find_all("a")
                      if "SCPR" in (a.get("id") or "") or "SEARCH" in (a.get("id") or "").upper()]
            _buttons = [i.get("name", "") for i in _soup.find_all("input", {"type": "button"})
                        if "SEARCH" in (i.get("name") or "").upper() or "SCPR" in (i.get("name") or "")]
            log.info("SCPRS2 IC fields: %s", _ic_fields)
            log.info("SCPRS2 links: %s", _links[:20])
            log.info("SCPRS2 buttons: %s", _buttons[:10])
            # Also find all input names containing SCPR or PO
            _po_fields = [i.get("name", "") for i in _soup.find_all("input")
                          if "SCPR" in (i.get("name") or "") or "CRDMEM" in (i.get("name") or "")]
            log.info("SCPRS2 PO/SCPR inputs: %s", _po_fields[:10])

            # POST search with PO number on ZZ_SCPRS2
            search_values = {f: "" for f in ALL_SEARCH_FIELDS}
            search_values[FIELD_PO_NUM] = po_number
            form_data = self._build_form_data(page, SEARCH_BUTTON, search_values)
            form_data["ICSID"] = icsid

            r2 = s2.post(SCPRS_DETAIL_URL, data=form_data, timeout=20)
            has_pdl = "ZZ_SCPR_PDL_DVW" in r2.text
            log.info("Detail POST: %db has_PDL_DVW=%s (SCPRS2 PO=%s)",
                     len(r2.text), has_pdl, po_number)

            if r2.status_code == 200 and has_pdl:
                return self._parse_detail(r2.text)

            # Discover clickable result links in search response
            if r2.status_code == 200:
                _soup2 = _BS(r2.text, "html.parser")
                _links2 = [a.get("id", "") for a in _soup2.find_all("a")
                           if "RSLT" in (a.get("id") or "") or "SCPR" in (a.get("id") or "")]
                log.info("SCPRS2 result links: %s", _links2[:10])

                # Click row 0 directly
                click_sv = {}
                for fld in ALL_SEARCH_FIELDS:
                    m2 = re.search(rf"name='{re.escape(fld)}'[^>]*value=\"([^\"]*)\"", r2.text)
                    click_sv[fld] = m2.group(1) if m2 else ""

                # Try the first RSLT link found, or default to ZZ_SCPR_RSLT_VW$0
                click_id = "ZZ_SCPR_RSLT_VW$0"
                for lid in _links2:
                    if "RSLT" in lid and "$0" in lid:
                        click_id = lid
                        break
                click_data = self._build_form_data(r2.text, click_id, click_sv)

                r3 = s2.post(SCPRS_DETAIL_URL, data=click_data, timeout=20)
                has_pdl3 = "ZZ_SCPR_PDL_DVW" in r3.text
                log.info("Detail POST: %db has_PDL_DVW=%s (SCPRS2 click %s PO=%s)",
                         len(r3.text), has_pdl3, click_id, po_number)
                if r3.status_code == 200:
                    return self._parse_detail(r3.text)
        except Exception as e:
            log.error("get_po_detail failed PO=%s: %s", po_number, e)
        return None

    def get_detail(self, results_html, row_index, click_action=None):
        """Click result row on SCPRS1 — parse modal response for detail data."""
        if not click_action:
            click_action = f"ZZ_SCPR_RSLT_VW$hmodal${row_index}"

        current_html = self._last_html or results_html

        # Modal click — the 553KB response contains PO data
        search_values = {}
        for fld in ALL_SEARCH_FIELDS:
            m = re.search(rf"name='{re.escape(fld)}'[^>]*value=\"([^\"]*)\"", current_html)
            search_values[fld] = m.group(1) if m else ""
        form_data = self._build_form_data(current_html, click_action, search_values)

        log.info("Detail click: %s", click_action)
        try:
            modal_r = self.session.post(SCPRS_SEARCH_URL, data=form_data, timeout=20)
            if modal_r.status_code != 200:
                return None

            modal_html = modal_r.text

            # Modal click sets PO context server-side on SCPRS1 session.
            # SCPRS2 reads that context via a separate GET.
            DETAIL_URL = (
                'https://suppliers.fiscal.ca.gov/psc/'
                'psfpd1_1/SUPPLIER/ERP/c/'
                'ZZ_PO.ZZ_SCPRS2_CMP.GBL'
                '?Page=ZZ_SCPRS_PDDTL_PG&Action=U'
            )
            try:
                detail_r = self.session.get(DETAIL_URL, timeout=20)
                log.info('Detail GET: %db has_PDL=%s',
                         len(detail_r.content),
                         'ZZ_SCPR_PDL_DVW' in detail_r.text)
                # If SCPRS2 detail page has line items, use it
                if 'ZZ_SCPR_PDL_DVW' in detail_r.text:
                    return self._parse_detail(detail_r.text)
            except Exception as _det_e:
                log.debug('Detail GET failed: %s', _det_e)

            has_pdl = "ZZ_SCPR_PDL_DVW" in modal_html

            # Try standard detail parse first (fallback to modal response)
            if has_pdl:
                log.info("Detail POST: %db has_PDL_DVW=True", len(modal_html))
                return self._parse_detail(modal_html)

            # Extract PO number for browser fallback
            po_nums = re.findall(r'4500\d{6}', modal_html)
            po_number = po_nums[0] if po_nums else None

            # Try browser-based detail extraction (Playwright)
            if po_number:
                try:
                    from src.agents.scprs_browser import scrape_po_detail
                    browser_detail = scrape_po_detail(po_number)
                    if browser_detail and browser_detail.get("line_items"):
                        log.info("Detail via browser: PO=%s, %d lines",
                                 po_number, len(browser_detail["line_items"]))
                        return browser_detail
                except ImportError:
                    log.debug("scprs_browser not available")
                except Exception as e:
                    log.warning("Browser detail failed PO=%s: %s", po_number, e)

            # Parse the modal response for detail data using ZZ_SCPR_RD_DVW IDs
            # (modal uses RD_DVW prefix instead of PDL_DVW for line items)
            soup = BeautifulSoup(modal_html, "html.parser")

            def get_span(id_val):
                el = soup.find(id=id_val)
                return el.get_text(strip=True) if el else ""

            # Find all ZZ_ prefixes that contain data (for discovery)
            zz_with_data = set()
            for el in soup.find_all(id=re.compile(r'^ZZ_.*\$0$')):
                text = el.get_text(strip=True)
                if text and text != "\xa0":
                    zz_with_data.add(el.get("id", ""))
            if zz_with_data:
                log.info("Modal $0 elements with data: %s", sorted(zz_with_data))

            # Extract PO numbers
            po_nums = re.findall(r'4500\d{6}', modal_html)

            # Try RD_DVW prefix for line items (confirmed from earlier scprs-raw)
            line_items = []
            for prefix in ["ZZ_SCPR_RD_DVW", "ZZ_SCPR_PDL_DVW", "ZZ_SCPR_RSLT_VW"]:
                i = 0
                while True:
                    desc = get_span(f"{prefix}_DESCR254_MIXED${i}")
                    if not desc:
                        desc = get_span(f"{prefix}_DESCR254${i}")
                    if not desc:
                        break
                    up_raw = get_span(f"{prefix}_UNIT_PRICE${i}")
                    lt_raw = get_span(f"{prefix}_LINE_TOTAL${i}")
                    qty_raw = get_span(f"{prefix}_QUANTITY${i}")
                    up = _parse_dollar(up_raw) or 0.0
                    qty = _parse_dollar(qty_raw) or 0.0
                    line_items.append({
                        "line_num": i + 1,
                        "item_id": get_span(f"{prefix}_INV_ITEM_ID${i}"),
                        "description": desc,
                        "unit_price": up, "unit_price_num": up,
                        "quantity": qty, "quantity_num": qty,
                        "line_total": _parse_dollar(lt_raw) or 0.0,
                    })
                    i += 1
                if line_items:
                    log.info("Detail: %d lines from prefix %s", len(line_items), prefix)
                    break

            # Extract header info
            po_number = po_nums[0] if po_nums else ""
            buyer_name = get_span("ZZ_SCPR_SBP_WRK_BUYER_DESCR") or get_span("ZZ_SCPR_RD_DVW_BUYER_DESCR$0")
            buyer_email = get_span("ZZ_SCPR_SBP_WRK_EMAILID") or get_span("ZZ_SCPR_RD_DVW_EMAILID$0")
            acq_method = get_span("ZZ_SCPR_SBP_WRK_ZZ_ACQ_MTHD")

            log.info("Detail: PO=%s, %d lines, buyer=%s", po_number, len(line_items), buyer_name)

            header = {
                "po_number": po_number, "buyer_name": buyer_name,
                "buyer_email": buyer_email, "acq_method": acq_method,
            }
            return {
                "header": header, "po_number": po_number,
                "buyer_name": buyer_name, "buyer_email": buyer_email,
                "acq_method": acq_method, "line_items": line_items,
            }

        except Exception as e:
            log.error("Detail click failed: %s", e)
        return None

    # ── Parsers ────────────────────────────────────────────────────

    def _parse_results(self, html):
        """
        Parse SCPRS search results using confirmed PeopleSoft element IDs.
        Field IDs verified from live FI$Cal output on 2026-02-13.
        Pattern: ZZ_SCPR_RSLT_VW_{SUFFIX}$N where N = row index
        PO link: ZZ_SCPR_RSLT_VW$N (no suffix — just prefix$N)
        """
        soup = BeautifulSoup(html, "html.parser")

        count_match = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
        if not count_match:
            log.info("SCPRS: no results found")
            return []

        total = int(count_match.group(3))
        log.info(f"SCPRS: {count_match.group(0)} results")

        # Confirmed field IDs from live /api/scprs-raw output
        PREFIX = "ZZ_SCPR_RSLT_VW"
        FIELD_MAP = {
            "DESCR": "dept",
            "DESCR254_MIXED": "first_item",
            "START_DATE": "start_date",
            "AWARDED_AMT": "grand_total",
            "SUPPLIER_ID": "supplier_id",
            "NAME1": "supplier_name",
            "ZZ_CERT_TYPE": "cert_type",
            "ZZ_COMMENT1": "acq_type",
            "ZZ_ACQ_MTHD": "acq_method",
            "ZZ_LPACONTRACTNBR": "lpa_id",
            "EMAILID": "buyer_email",
            "STATUS2": "status",
        }

        # Discover actual clickable element IDs in row 0
        row0_links = soup.find_all("a", id=re.compile(r'\$0$'))
        if row0_links:
            link_ids = [a.get("id", "") for a in row0_links if "SCPR" in a.get("id", "")]
            log.info("Row 0 clickable links: %s", link_ids[:10])
        else:
            log.warning("No <a> elements with $0 IDs found in results")

        results = []
        for row_idx in range(total):
            entry = {"_row_index": row_idx}

            # Extract all fields by confirmed IDs
            for suffix, key in FIELD_MAP.items():
                val = _get_text(soup, f"{PREFIX}_{suffix}${row_idx}")
                if val:
                    entry[key] = val

            if len(entry) <= 1:
                break

            # PO number: link with id='ZZ_SCPR_RSLT_VW$N' (prefix$N, no suffix)
            # PO link: try modal pattern first, then bare prefix
            po_link = soup.find("a", id=f"{PREFIX}$hmodal${row_idx}")
            if not po_link:
                po_link = soup.find("a", id=f"{PREFIX}${row_idx}")
            if po_link:
                entry["po_number"] = po_link.get_text(strip=True)
                entry["_click_action"] = po_link.get("id", "")

            entry["grand_total_num"] = _parse_dollar(entry.get("grand_total", ""))
            entry["start_date_parsed"] = _parse_date(entry.get("start_date", ""))
            entry["_results_html"] = html
            results.append(entry)
            log.info(f"  Row {row_idx}: PO={entry.get('po_number','?')} "
                     f"{entry.get('grand_total','?')} {entry.get('supplier_name','?')}")

        log.info(f"Parsed {len(results)} results")
        return results

    def _parse_detail(self, html):
        """Parse detail page by PeopleSoft span IDs (not table cells)."""
        if isinstance(html, bytes):
            html = html.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        def get_span(id_val):
            el = soup.find(id=id_val)
            return el.get_text(strip=True) if el else ""

        def parse_price(s):
            try:
                return float(s.replace("$", "").replace(",", ""))
            except Exception:
                return 0.0

        # Header fields by span ID
        po_number = get_span("ZZ_SCPR_SBP_WRK_CRDMEM_ACCT_NBR")
        buyer_name = get_span("ZZ_SCPR_SBP_WRK_BUYER_DESCR")
        buyer_email = get_span("ZZ_SCPR_SBP_WRK_EMAILID")
        buyer_phone = get_span("ZZ_SCPR_SBP_WRK_PHONE")
        acq_method = get_span("ZZ_SCPR_SBP_WRK_ZZ_ACQ_MTHD")
        dept_code = get_span("ZZ_SCPR_SBP_WRK_BUSINESS_UNIT")
        dept_name = get_span("ZZ_SCPR_SBP_WRK_DESCR")
        supplier = get_span("ZZ_SCPR_SBP_WRK_NAME1")
        status = get_span("ZZ_SCPR_SBP_WRK_STATUS1")
        start_date = get_span("ZZ_SCPR_SBP_WRK_START_DATE")
        end_date = get_span("ZZ_SCPR_SBP_WRK_END_DATE")
        acq_type = get_span("ZZ_SCPR_SBP_WRK_ZZ_COMMENT1")
        merch_amount = get_span("ZZ_SCPR_SBP_WRK_MERCH_AMT_TTL")
        grand_total = get_span("ZZ_SCPR_SBP_WRK_AWARDED_AMT")

        # Regex phone fallback
        if not buyer_phone:
            phone_match = re.search(
                r'(?:phone|tel|fax)[:\s]*[\(]?(\d{3})[\)\-\.\s]+(\d{3})[\-\.\s]+(\d{4})',
                html, re.IGNORECASE)
            if phone_match:
                buyer_phone = f"({phone_match.group(1)}) {phone_match.group(2)}-{phone_match.group(3)}"

        # Line items — indexed by PeopleSoft span IDs: $0, $1, $2, ...
        line_items = []
        i = 0
        while True:
            desc = get_span(f"ZZ_SCPR_PDL_DVW_DESCR254_MIXED${i}")
            if not desc:
                break
            unit_price_raw = get_span(f"ZZ_SCPR_PDL_DVW_UNIT_PRICE${i}")
            line_total_raw = get_span(f"ZZ_SCPR_PDL_DVW_LINE_TOTAL${i}")
            quantity_raw = get_span(f"ZZ_SCPR_PDL_DVW_QUANTITY${i}")
            up = parse_price(unit_price_raw)
            qty = parse_price(quantity_raw)
            line_items.append({
                "line_num": i + 1,
                "item_id": get_span(f"ZZ_SCPR_PDL_DVW_INV_ITEM_ID${i}"),
                "description": desc,
                "unspsc": get_span(f"ZZ_SCPR_PDL_DVW_PV_UNSPSC_CODE${i}"),
                "unspsc_description": get_span(f"ZZ_CAT_ID_VW_DESCR254${i}"),
                "unit_of_measure": get_span(f"ZZ_SCPR_PDL_DVW_DESCR${i}"),
                "quantity": qty,
                "unit_price": up,
                "line_total": parse_price(line_total_raw),
                "line_status": get_span(f"ZZ_SCPR_PDL_DVW_DESCR1${i}"),
                # Backward compat keys for callers expecting _num fields
                "unit_price_num": up,
                "quantity_num": qty,
            })
            i += 1

        log.info("Detail: PO=%s, %d lines, buyer=%s", po_number, len(line_items), buyer_name)

        # Build header dict for callers that use detail.get("header", {})
        header = {
            "po_number": po_number, "buyer_name": buyer_name,
            "buyer_email": buyer_email, "buyer_phone": buyer_phone,
            "acq_method": acq_method, "dept_code": dept_code,
            "dept_name": dept_name, "supplier": supplier,
            "status": status, "start_date": start_date,
            "end_date": end_date, "acq_type": acq_type,
            "merch_amount": merch_amount, "grand_total": grand_total,
        }

        return {
            "header": header,
            "po_number": po_number,
            "buyer_name": buyer_name,
            "buyer_email": buyer_email,
            "acq_method": acq_method,
            "line_items": line_items,
        }


# ── High-Level Lookup ──────────────────────────────────────────────

_fiscal_session = None

def _get_session():
    global _fiscal_session
    if _fiscal_session is None:
        _fiscal_session = FiscalSession()
    return _fiscal_session


def lookup_price(item_number=None, description=None):
    db = _load_db()
    # 1. Local exact match
    if item_number:
        key = item_number.strip()
        if key in db:
            e = db[key]
            return {"price": e["price"], "source": "local_db",
                    "date": e.get("date", ""), "confidence": "high",
                    "vendor": e.get("vendor", "")}
    # 2. Local fuzzy match
    if description:
        dl = description.lower().split("\n")[0].strip()
        best, bs = None, 0
        for key, entry in db.items():
            ed = entry.get("description", "").lower()
            wa, wb = set(dl.split()), set(ed.split())
            if wa and wb:
                s = len(wa & wb) / max(len(wa), len(wb))
                if s > bs and s > 0.5:
                    bs, best = s, entry
        if best:
            return {"price": best["price"], "source": "local_db_fuzzy",
                    "date": best.get("date", ""), "confidence": "medium",
                    "vendor": best.get("vendor", "")}
    # 3. Live FI$Cal search (circuit-breaker protected)
    if HAS_SCRAPER and (item_number or description):
        if _scprs_breaker:
            try:
                result = _scprs_breaker.call(_scrape_fiscal, item_number, description)
            except CircuitOpenError:
                log.warning("SCPRS circuit breaker OPEN — skipping live search")
                return None
            except Exception:
                # _scrape_fiscal handles its own exceptions and returns None
                result = None
        else:
            result = _scrape_fiscal(item_number, description)
        if result:
            save_price(
                item_number=item_number or "", description=description or "",
                price=result.get("unit_price") or result["price"],
                vendor=result.get("vendor", ""),
                unit_price=result.get("unit_price"),
                quantity=result.get("quantity"),
                po_number=result.get("po_number", ""),
                start_date=result.get("date", ""), source="fiscal_scprs")
            # v6.0: Also store in Won Quotes KB
            if _ingest_wq:
                try:
                    _ingest_wq(
                        po_number=result.get("po_number", ""),
                        item_number=item_number or "",
                        description=description or "",
                        unit_price=result.get("unit_price") or result["price"],
                        quantity=result.get("quantity", 1) or 1,
                        supplier=result.get("vendor", ""),
                        department="",
                        award_date=result.get("date", ""),
                        source="scprs_live",
                    )
                except Exception:
                    pass
            return result
    return None


def _scrape_fiscal(item_number=None, description=None):
    try:
        session = _get_session()
        if not session.initialized and not session.init_session():
            return None

        terms = _build_search_terms(item_number, description)
        log.info(f"SCPRS terms: {terms}")

        # Search with the BEST term (most specific first)
        # Don't do multiple searches — it invalidates session state for detail clicks
        all_results = []
        last_search_term = None
        for term in terms[:3]:
            try:
                results = session.search(description=term)
                if results:
                    all_results = results  # Keep only the latest search results (session state matches)
                    last_search_term = term
                    if len(results) >= 3:
                        break  # Good enough results, don't invalidate session
            except Exception as e:
                log.warning(f"Search '{term}': {e}")
            time.sleep(0.5)

        if not all_results:
            return None

        log.info(f"SCPRS: {len(all_results)} results for '{last_search_term}'")

        cutoff = datetime.now() - timedelta(days=548)
        recent = [r for r in all_results
                  if r.get("start_date_parsed") and r["start_date_parsed"] >= cutoff]
        cands = (recent or all_results)
        cands.sort(key=lambda x: x.get("start_date_parsed") or datetime.min, reverse=True)

        best_detail = None
        best_summary = None

        # Try detail extraction on top 3 candidates only
        for c in cands[:3]:
            po = c.get("po_number", "")
            detail = None
            if c.get("_results_html"):
                try:
                    ca = c.get("_click_action")
                    detail = session.get_detail(c["_results_html"], c["_row_index"], ca)
                    time.sleep(0.3)
                except Exception as e:
                    log.warning(f"Detail PO {po}: {e}")

            if detail and detail.get("line_items"):
                line = _find_best_line_match(detail["line_items"], item_number, description)
                if line and line.get("unit_price_num"):
                    r = {"price": line["unit_price_num"], "unit_price": line["unit_price_num"],
                         "quantity": line.get("quantity_num"), "source": "fiscal_scprs",
                         "date": c.get("start_date", ""), "confidence": "high",
                         "vendor": c.get("supplier_name", ""), "po_number": po,
                         "line_desc": line.get("description", "")}
                    if best_detail is None or r["price"] < best_detail["price"]:
                        best_detail = r

            # Always track best summary-level price as fallback
            gt = c.get("grand_total_num")
            if gt and gt > 0 and not best_summary:
                best_summary = {
                    "price": gt, "unit_price": None,
                    "source": "fiscal_scprs_summary",
                    "date": c.get("start_date", ""), "confidence": "low",
                    "vendor": c.get("supplier_name", ""), "po_number": po,
                    "first_item": c.get("first_item", ""),
                    "department": c.get("dept", ""),
                }

            # Re-init session for next detail attempt (session state is fragile)
            if detail is None and c.get("_results_html"):
                try:
                    session.init_session()
                except Exception:
                    pass

        # Return best detail price, or summary fallback
        if best_detail:
            return best_detail
        if best_summary:
            log.info("SCPRS: no detail prices, using summary total $%.2f from %s",
                     best_summary["price"], best_summary.get("vendor", "?"))
            return best_summary
        return None
    except Exception as e:
        log.error(f"SCPRS scrape: {e}", exc_info=True)
        return None


def _build_search_terms(item_number=None, description=None):
    terms = []
    if item_number: terms.append(item_number.strip())
    if description:
        desc = description.strip()
        fl = desc.split("\n")[0].strip()
        mfr = re.search(r'(?:mfr|mfg|manufacturer|item\s*#?|part\s*#?)[#:\s]*(\S+)', desc, re.I)
        if mfr: terms.append(mfr.group(1))
        clean = re.sub(r'\b(the|and|for|with|each|per|unit|item|qty|no|number)\b', '', fl, flags=re.I)
        clean = re.sub(r'[,;()\[\]{}#]', ' ', clean)
        clean = ' '.join(clean.split())[:50]
        if clean and clean not in terms: terms.append(clean)
        words = [w for w in fl.split() if len(w) > 3
                 and w.lower() not in {"the","and","for","with","each","per","unit","item"}]
        if len(words) >= 2:
            short = ' '.join(words[:3])
            if short not in terms: terms.append(short)
    return terms


def _find_best_line_match(lines, item_number=None, description=None):
    if not lines: return None
    if len(lines) == 1: return lines[0]
    best, bs = None, -1
    for line in lines:
        s = 0; ld = (line.get("description") or "").lower()
        if item_number:
            ic = item_number.strip().lower()
            if ic in ld: s += 100
            elif ic.replace("-","") in ld.replace("-",""): s += 80
        if description:
            s += len(set(description.lower().split()) & set(ld.split())) * 5
        if line.get("status","").lower() == "active": s += 2
        if line.get("unit_price_num") and line["unit_price_num"] > 0: s += 1
        if s > bs: bs, best = s, line
    return best


def bulk_lookup(line_items):
    for item in line_items:
        result = lookup_price(item.get("item_number"), item.get("description"))
        if result:
            item["scprs_last_price"] = result["price"]
            item["scprs_source"] = result["source"]
            item["scprs_confidence"] = result["confidence"]
            item["scprs_vendor"] = result.get("vendor", "")
            item["scprs_date"] = result.get("date", "")
            item["scprs_po"] = result.get("po_number", "")
    return line_items


# ── Diagnostics ────────────────────────────────────────────────────

def test_connection():
    if not HAS_SCRAPER: return False, "requests/bs4 missing"
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        url = f"{SCPRS_SEARCH_URL}?&"
        r1 = s.get(url, timeout=8, allow_redirects=True)
        page, info = r1.text, f"Hit1:{r1.status_code}({len(r1.text)}b)"
        if "ZZ_SCPRS" not in page and "ICSID" not in page:
            time.sleep(0.5)
            r2 = s.get(url, timeout=8, allow_redirects=True)
            page = r2.text; info += f" Hit2:{r2.status_code}({len(page)}b)"
        if "ZZ_SCPRS" in page or "ICSID" in page:
            icsid = re.search(r"name='ICSID'[^>]*value='([^']*)'", page)
            return True, f"Connected, ICSID={'found' if icsid else 'missing'} [{info}]"
        title = re.search(r'<title>([^<]*)</title>', page)
        return False, f"No form. Title:{title.group(1) if title else '?'} [{info}]"
    except Exception as e:
        return False, f"Error: {e}"


def test_search(query="stryker xpr"):
    """Run test search with full debug output."""
    session = _get_session()
    if not session.initialized and not session.init_session():
        return {"error": "init failed"}
    results = session.search(description=query)
    clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in results]
    return {"query": query, "count": len(results), "results": clean[:10]}


def get_price_db_stats():
    db = _load_db()
    sources = {}
    for e in db.values():
        s = e.get("source", "?")
        sources[s] = sources.get(s, 0) + 1
    return {"total_items": len(db), "sources": sources}


def migrate_local_db_to_won_quotes():
    """Import all existing scprs_prices.json entries into the Won Quotes KB.
    Call once to backfill, then the live hook handles new data."""
    if not _ingest_wq:
        return {"error": "won_quotes_db not available", "migrated": 0}
    db = _load_db()
    migrated = 0
    skipped = 0
    for key, entry in db.items():
        price = entry.get("unit_price") or entry.get("price")
        if not price or float(price) <= 0:
            skipped += 1
            continue
        try:
            _ingest_wq(
                po_number=entry.get("po_number", ""),
                item_number=entry.get("item_number", ""),
                description=entry.get("description", "") or key,
                unit_price=float(price),
                quantity=float(entry.get("quantity", 1) or 1),
                supplier=entry.get("vendor", ""),
                department="",
                award_date=entry.get("date", ""),
                source="migrated_local_db",
            )
            migrated += 1
        except Exception:
            skipped += 1
    return {"migrated": migrated, "skipped": skipped, "total_in_local_db": len(db)}


# ── Bulk KB Seeder ────────────────────────────────────────────────

SEED_CATEGORIES = [
    "gloves nitrile", "exam gloves", "toner cartridge", "copy paper",
    "trash bags liners", "cleaning supplies", "disinfectant wipes",
    "hand sanitizer", "face mask", "surgical gown",
    "bandage gauze", "syringe needle", "catheter",
    "toilet paper tissue", "paper towels", "soap dispenser",
    "office supplies pens", "file folders binders",
    "batteries alkaline", "light bulbs LED",
]

SEED_STATUS = {
    "running": False, "progress": "", "categories_done": 0,
    "categories_total": 0, "records_ingested": 0, "errors": [],
    "started_at": None, "finished_at": None,
}


def bulk_seed_won_quotes(max_categories=None, max_pos_per_category=3):
    """Search SCPRS for common categories, drill into PO details, ingest unit prices.
    Designed to run in a background thread. Updates SEED_STATUS for progress."""
    if not _ingest_wq:
        SEED_STATUS["errors"].append("won_quotes_db not available")
        return SEED_STATUS
    if not HAS_SCRAPER:
        SEED_STATUS["errors"].append("requests/bs4 not available")
        return SEED_STATUS

    categories = SEED_CATEGORIES[:max_categories] if max_categories else SEED_CATEGORIES
    SEED_STATUS.update({
        "running": True, "progress": "starting", "categories_done": 0,
        "categories_total": len(categories), "records_ingested": 0,
        "errors": [], "started_at": datetime.now().isoformat(), "finished_at": None,
    })

    session = _get_session()
    if not session.initialized and not session.init_session():
        SEED_STATUS["running"] = False
        SEED_STATUS["errors"].append("FI$Cal session init failed")
        return SEED_STATUS

    total_ingested = 0

    for cat_idx, category in enumerate(categories):
        SEED_STATUS["progress"] = f"Searching: {category}"
        SEED_STATUS["categories_done"] = cat_idx
        log.info(f"Seed [{cat_idx+1}/{len(categories)}]: searching '{category}'")

        try:
            results = session.search(description=category)
            if not results:
                log.info(f"  No results for '{category}'")
                time.sleep(0.5)
                continue

            log.info(f"  {len(results)} results for '{category}'")

            # Sort by most recent first
            results.sort(
                key=lambda x: x.get("start_date_parsed") or datetime.min,
                reverse=True
            )

            # Drill into top N POs for unit prices
            pos_checked = 0
            for r in results:
                if pos_checked >= max_pos_per_category:
                    break

                po = r.get("po_number", "") or r.get("first_item", "")[:20]
                dept = r.get("dept", "")
                supplier = r.get("supplier_name", "")
                start_date = r.get("start_date", "")

                # Try to get detail page for unit prices
                detail = None
                if r.get("_results_html") and r.get("_row_index") is not None:
                    try:
                        ca = r.get("_click_action")
                        detail = session.get_detail(r["_results_html"], r["_row_index"], ca)
                        time.sleep(0.5)
                    except Exception as e:
                        log.warning(f"  Detail failed for PO in '{category}': {e}")

                if detail and detail.get("line_items"):
                    for line in detail["line_items"]:
                        up = line.get("unit_price_num")
                        if up and up > 0:
                            try:
                                _ingest_wq(
                                    po_number=detail.get("header", {}).get("po_number", po),
                                    item_number=line.get("item_id", ""),
                                    description=line.get("description", category),
                                    unit_price=up,
                                    quantity=line.get("quantity_num", 1) or 1,
                                    supplier=detail.get("header", {}).get("supplier", supplier),
                                    department=dept,
                                    award_date=detail.get("header", {}).get("start_date", start_date),
                                    source="scprs_bulk_seed",
                                )
                                total_ingested += 1
                            except Exception:
                                pass
                    pos_checked += 1
                else:
                    # Fallback: ingest summary-level data (grand total, lower confidence)
                    gt = r.get("grand_total_num")
                    first_item = r.get("first_item", "")
                    if gt and gt > 0 and first_item:
                        try:
                            _ingest_wq(
                                po_number=po,
                                item_number="",
                                description=first_item[:200],
                                unit_price=gt,
                                quantity=1,
                                supplier=supplier,
                                department=dept,
                                award_date=start_date,
                                source="scprs_bulk_seed_summary",
                            )
                            total_ingested += 1
                            pos_checked += 1
                        except Exception:
                            pass

                SEED_STATUS["records_ingested"] = total_ingested

        except Exception as e:
            log.warning(f"  Category '{category}' failed: {e}")
            SEED_STATUS["errors"].append(f"{category}: {str(e)}")

        time.sleep(1)  # Rate limit between categories

    SEED_STATUS.update({
        "running": False, "progress": "complete",
        "categories_done": len(categories),
        "records_ingested": total_ingested,
        "finished_at": datetime.now().isoformat(),
    })
    log.info(f"Seed complete: {total_ingested} records ingested from {len(categories)} categories")
    return SEED_STATUS
