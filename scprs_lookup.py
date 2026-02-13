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
    import requests
    from bs4 import BeautifulSoup
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False

log = logging.getLogger("scprs")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "scprs_prices.json")

# ── FI$Cal URLs & Field IDs ───────────────────────────────────────

SCPRS_BASE = "https://suppliers.fiscal.ca.gov"
SCPRS_SEARCH_URL = f"{SCPRS_BASE}/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL"

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
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with open(DB_PATH, "w") as f:
            json.dump(db, f, indent=2, default=str)
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
    except: return None

def _parse_date(text, fmt="%m/%d/%Y"):
    if not text: return None
    try: return datetime.strptime(text.strip(), fmt)
    except: return None

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

    def _load_page(self, max_attempts=3):
        url = f"{SCPRS_SEARCH_URL}?&"
        page = ""
        for attempt in range(1, max_attempts + 1):
            r = self.session.get(url, timeout=15, allow_redirects=True)
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

    def search(self, description="", from_date="", to_date=""):
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
        form_data = self._build_form_data(page, SEARCH_BUTTON, search_values)

        try:
            r = self.session.post(SCPRS_SEARCH_URL, data=form_data, timeout=30)
            log.info(f"SCPRS search '{description}': {r.status_code} ({len(r.text)}b)")
        except Exception as e:
            log.error(f"SCPRS search POST: {e}")
            return []

        html = r.text
        self._last_html = html
        new_id = self._extract_icsid(html)
        if new_id: self.icsid = new_id
        return self._parse_results(html)

    def get_detail(self, results_html, row_index):
        action_id = f"ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR${row_index}"
        search_values = {}
        for fld in ALL_SEARCH_FIELDS:
            m = re.search(rf"name='{re.escape(fld)}'[^>]*value=\"([^\"]*)\"", results_html)
            search_values[fld] = m.group(1) if m else ""
        form_data = self._build_form_data(results_html, action_id, search_values)
        try:
            r = self.session.post(SCPRS_SEARCH_URL, data=form_data, timeout=30)
            log.info(f"SCPRS detail row {row_index}: {r.status_code} ({len(r.text)}b)")
            if r.status_code == 200:
                return self._parse_detail(r.text)
        except Exception as e:
            log.error(f"SCPRS detail: {e}")
        return None

    # ── Parsers ────────────────────────────────────────────────────

    def _parse_results(self, html):
        """Parse search results using BOTH ID-based and table-based strategies."""
        soup = BeautifulSoup(html, "html.parser")

        count_match = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
        if not count_match:
            log.info("SCPRS: no results found")
            return []

        total = int(count_match.group(3))
        log.info(f"SCPRS: {count_match.group(0)} results")

        # Strategy 1: ID-based (ZZ_SCPR_RD_DVW_*$N)
        discovered = _discover_grid_ids(soup, "ZZ_SCPR_RD_DVW")
        log.info(f"SCPRS ID-discovered fields: {list(discovered.keys())}")

        field_map = {
            "CRDMEM_ACCT_NBR": "po_number", "AWARDED_AMT": "grand_total",
            "START_DATE": "start_date", "END_DATE": "end_date",
            "NAME1": "supplier_name", "SUPPLIER_ID": "supplier_id",
            "DESCR254_MIXED": "first_item", "DESCR254": "first_item",
            "BUSINESS_UNIT": "dept_code", "DESCR": "dept",
            "ZZ_COMMENT1": "acq_type", "ZZ_ACQ_MTHD": "acq_method",
        }

        results = []
        if discovered:
            for row_idx in range(total):
                entry = {"_row_index": row_idx}
                for suffix in discovered:
                    val = _get_text(soup, f"ZZ_SCPR_RD_DVW_{suffix}${row_idx}")
                    if val:
                        entry[field_map.get(suffix, suffix.lower())] = val

                # Also check for hyperlink-style PO number
                if "po_number" not in entry:
                    link = soup.find("a", id=f"ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR${row_idx}")
                    if link: entry["po_number"] = link.get_text(strip=True)

                if len(entry) <= 1: break
                entry["grand_total_num"] = _parse_dollar(entry.get("grand_total", ""))
                entry["start_date_parsed"] = _parse_date(entry.get("start_date", ""))
                entry["_results_html"] = html
                results.append(entry)
                log.info(f"  Row {row_idx}: PO={entry.get('po_number','?')} "
                         f"${entry.get('grand_total','?')} {entry.get('supplier_name','?')}")

        # Strategy 2: Table fallback if ID parsing found nothing
        if not results:
            log.info("ID parsing: 0 rows — trying table fallback")
            results = self._table_fallback(soup, html, total)

        return results

    def _table_fallback(self, soup, html, expected):
        """Parse results grid by walking table rows."""
        results = []
        for grid in soup.find_all("table", class_=re.compile(r'PSLEVEL1GRID')):
            rows = grid.find_all("tr")
            data_rows = []
            for row in rows:
                if row.find("th"): continue
                cells = row.find_all("td")
                if len(cells) >= 5:
                    data_rows.append(cells)

            if not data_rows: continue

            log.info(f"Table fallback: {len(data_rows)} rows, {len(data_rows[0])} cols")

            col_fields = ["dept", "po_number", "associated_pos", "first_item",
                          "start_date", "end_date", "grand_total", "supplier_id",
                          "supplier_name", "cert_type", "acq_type"]

            for idx, cells in enumerate(data_rows):
                entry = {"_row_index": idx}
                for i, cell in enumerate(cells):
                    if i < len(col_fields):
                        entry[col_fields[i]] = cell.get_text(strip=True)
                entry["grand_total_num"] = _parse_dollar(entry.get("grand_total", ""))
                entry["start_date_parsed"] = _parse_date(entry.get("start_date", ""))
                entry["_results_html"] = html
                results.append(entry)
            break
        return results

    def _parse_detail(self, html):
        """Parse detail page for line item unit prices."""
        soup = BeautifulSoup(html, "html.parser")
        detail = {}
        for fid, key in DETAIL_HEADER_FIELDS.items():
            val = _get_text(soup, fid)
            if val: detail[key] = val

        detail["line_items"] = []
        for row_idx in range(200):
            desc = _get_text(soup, f"{DETAIL_DESCRIPTION}${row_idx}")
            if not desc: break
            up_text = _get_text(soup, f"{DETAIL_UNIT_PRICE}${row_idx}")
            qty_text = _get_text(soup, f"{DETAIL_QUANTITY}${row_idx}")
            line = {
                "line_num": _get_text(soup, f"{DETAIL_LINE_NUM}${row_idx}"),
                "item_id": _get_text(soup, f"{DETAIL_ITEM_ID}${row_idx}"),
                "description": desc,
                "unit_of_measure": _get_text(soup, f"{DETAIL_UOM}${row_idx}"),
                "quantity": qty_text,
                "unit_price": up_text,
                "line_total": _get_text(soup, f"{DETAIL_LINE_TOTAL}${row_idx}"),
                "status": _get_text(soup, f"{DETAIL_LINE_STATUS}${row_idx}"),
                "unit_price_num": _parse_dollar(up_text),
                "quantity_num": None,
            }
            if qty_text:
                try: line["quantity_num"] = float(qty_text.replace(",", ""))
                except: pass
            detail["line_items"].append(line)

        log.info(f"Detail: PO={detail.get('po_number','?')}, {len(detail['line_items'])} lines")
        return detail


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
    # 3. Live FI$Cal search
    if HAS_SCRAPER and (item_number or description):
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
            return result
    return None


def _scrape_fiscal(item_number=None, description=None):
    try:
        session = _get_session()
        if not session.initialized and not session.init_session():
            return None

        terms = _build_search_terms(item_number, description)
        log.info(f"SCPRS terms: {terms}")

        all_results, seen = [], set()
        for term in terms[:3]:
            try:
                for r in session.search(description=term):
                    po = r.get("po_number", "")
                    if po and po not in seen:
                        seen.add(po); all_results.append(r)
            except Exception as e:
                log.warning(f"Search '{term}': {e}")
            time.sleep(0.5)

        if not all_results:
            return None

        log.info(f"SCPRS: {len(all_results)} unique POs")

        cutoff = datetime.now() - timedelta(days=548)
        recent = [r for r in all_results
                  if r.get("start_date_parsed") and r["start_date_parsed"] >= cutoff]
        cands = (recent or all_results)
        cands.sort(key=lambda x: x.get("start_date_parsed") or datetime.min, reverse=True)

        best = None
        for c in cands[:5]:
            po = c.get("po_number", "")
            detail = None
            if c.get("_results_html"):
                try:
                    detail = session.get_detail(c["_results_html"], c["_row_index"])
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
                    if best is None or r["price"] < best["price"]:
                        best = r
            elif not best:
                gt = c.get("grand_total_num")
                if gt and gt > 0:
                    best = {"price": gt, "unit_price": None,
                            "source": "fiscal_scprs_summary",
                            "date": c.get("start_date", ""), "confidence": "low",
                            "vendor": c.get("supplier_name", ""), "po_number": po}
        return best
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
