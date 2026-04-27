"""
ca_scprs.py — California SCPRS connector.

Wraps the existing FiscalSession scraper in the BaseConnector interface.
Does NOT rewrite scraping logic — delegates to src/agents/scprs_lookup.py.
"""
import time
import hashlib
import logging
from datetime import datetime, timedelta

from src.agents.connectors.base import BaseConnector

log = logging.getLogger("reytech.connectors.ca_scprs")


class CASCPRSConnector(BaseConnector):
    connector_id = "ca_scprs"
    name = "California SCPRS"
    jurisdiction_level = "state"
    state = "CA"

    def __init__(self):
        self.session = None

    def authenticate(self) -> bool:
        try:
            from src.agents.scprs_lookup import FiscalSession
            self.session = FiscalSession()
            return self.session.init_session()
        except Exception as e:
            log.error("SCPRS auth failed: %s", e)
            return False

    def _fetch_with_detail(self, raw_results: list, max_detail: int = 50) -> list:
        """For each search result, fetch detail page to get line items + buyer info."""
        results = []
        detail_count = 0
        for r in (raw_results or []):
            normalized = self.normalize(r)
            # Fetch detail page if we have the HTML + row index
            if (detail_count < max_detail
                    and r.get("_results_html") and r.get("_row_index") is not None):
                try:
                    detail = self.session.get_detail(r["_results_html"], r["_row_index"])
                    if detail:
                        header = detail.get("header", {})
                        items = detail.get("line_items", [])
                        if items:
                            normalized["line_items"] = items
                        # Real PO number from detail page overrides hash
                        if header.get("po_number"):
                            normalized["po_number"] = header["po_number"]
                            normalized["id"] = header["po_number"]
                        # Buyer info from detail page
                        if header.get("buyer_name"):
                            normalized["buyer_name"] = header["buyer_name"]
                        if header.get("buyer_email"):
                            normalized["buyer_email"] = header["buyer_email"]
                        if header.get("acq_method"):
                            normalized["acq_method"] = header["acq_method"]
                        detail_count += 1
                    time.sleep(1)  # Rate limit
                except Exception as e:
                    log.debug("Detail fetch failed: %s", e)
            results.append(normalized)
        return results

    def search_by_vendor(self, vendor_names: list,
                         from_date: datetime,
                         to_date: datetime = None,
                         fetch_detail: bool = True,
                         max_detail: int = 50) -> list:
        if not self.session:
            if not self.authenticate():
                return []
        results = []
        from_str = from_date.strftime("%m/%d/%Y")
        to_str = (to_date or datetime.now()).strftime("%m/%d/%Y")
        for name in vendor_names:
            try:
                raw = self.session.search(
                    supplier_name=name, from_date=from_str, to_date=to_str)
                if fetch_detail:
                    batch = self._fetch_with_detail(raw, max_detail=max_detail)
                else:
                    batch = [self.normalize(r) for r in (raw or [])]
                detail_ct = sum(1 for r in batch if r.get("line_items"))
                log.info("SCPRS vendor search '%s': %d results, %d with detail",
                         name, len(batch), detail_ct)
                results.extend(batch)
                time.sleep(3)
            except Exception as e:
                log.error("SCPRS vendor search '%s' failed: %s", name, e)
        return results

    def search_by_agency(self, agency: str,
                         from_date: datetime,
                         to_date: datetime = None) -> list:
        if not self.session:
            if not self.authenticate():
                return []
        from_str = from_date.strftime("%m/%d/%Y")
        try:
            from src.agents.scprs_lookup import FIELD_DEPT
            raw = self.session.search(from_date=from_str)
            # SCPRS dept fields store expanded names ("California Correctional
            # Health Care Services") — operator passes "CCHCS" abbreviation.
            # Expand to full pattern list before substring matching.
            try:
                from src.core.agency_config import resolve_agency_patterns
                patterns = resolve_agency_patterns(agency or "")
            except Exception:
                patterns = [(agency or "").lower()] if agency else []
            filtered = [r for r in (raw or [])
                        if any(p in (r.get("dept", "") or "").lower()
                               for p in patterns)]
            return [self.normalize(r) for r in filtered]
        except Exception as e:
            log.error("SCPRS agency search '%s' failed: %s", agency, e)
            return []

    def search_by_keyword(self, keyword: str,
                          from_date: datetime,
                          to_date: datetime = None,
                          fetch_detail: bool = True,
                          max_detail: int = 50) -> list:
        """Search by keyword and fetch line item detail per PO."""
        if not self.session:
            if not self.authenticate():
                return []
        from_str = from_date.strftime("%m/%d/%Y")
        try:
            raw = self.session.search(description=keyword, from_date=from_str)
            if fetch_detail:
                results = self._fetch_with_detail(raw, max_detail=max_detail)
            else:
                results = [self.normalize(r) for r in (raw or [])]
            detail_ct = sum(1 for r in results if r.get("line_items"))
            log.info("SCPRS keyword '%s': %d results, %d with detail",
                     keyword, len(results), detail_ct)
            return results
        except Exception as e:
            log.error("SCPRS keyword search '%s' failed: %s", keyword, e)
            return []

    def get_all_agencies(self) -> list:
        """Discover agencies dynamically from existing SCPRS data + broad search."""
        agencies = set()
        # Method 1: Pull from existing DB data
        try:
            import sqlite3
            from src.core.db import DB_PATH
            conn = sqlite3.connect(DB_PATH, timeout=10)
            rows = conn.execute(
                "SELECT DISTINCT dept_name FROM scprs_po_master WHERE dept_name IS NOT NULL"
            ).fetchall()
            for r in rows:
                if r[0]:
                    agencies.add(r[0])
            conn.close()
        except Exception as e:
            log.debug("DB agency scan: %s", e)

        # Method 2: Broad search to discover new agencies
        if self.session or self.authenticate():
            try:
                from_str = (datetime.now() - timedelta(days=90)).strftime("%m/%d/%Y")
                raw = self.session.search(from_date=from_str)
                for r in (raw or []):
                    dept = r.get("dept", "")
                    if dept:
                        agencies.add(dept)
            except Exception as e:
                log.debug("SCPRS broad search: %s", e)

        # Upsert into agency_registry
        if agencies:
            try:
                import sqlite3
                from src.core.db import DB_PATH
                conn = sqlite3.connect(DB_PATH, timeout=10)
                for name in agencies:
                    conn.execute("""
                        INSERT OR IGNORE INTO agency_registry
                        (agency_name, state, jurisdiction, active, tenant_id)
                        VALUES (?, 'CA', 'state', 1, 'reytech')
                    """, (name,))
                conn.commit()
                conn.close()
            except Exception as e:
                log.debug("Agency registry upsert: %s", e)

        return sorted(agencies)

    def normalize(self, raw: dict) -> dict:
        po_num = raw.get("po_number", "")
        if not po_num:
            # Stable composite key from real PO fields (not search HTML)
            # Same real PO always generates the same ID
            key = "|".join([
                raw.get("supplier_name", raw.get("vendor", "")),
                raw.get("dept", raw.get("dept_name", "")),
                str(raw.get("grand_total_num", raw.get("grand_total", ""))),
                raw.get("start_date", ""),
            ])
            po_num = "SCPRS-" + hashlib.md5(key.encode()).hexdigest()[:12].upper()
        return {
            "id": po_num,
            "po_number": po_num,
            "vendor_name": raw.get("supplier_name", raw.get("vendor", "")),
            "agency": raw.get("dept", raw.get("dept_name", "")),
            "award_date": raw.get("start_date", ""),
            "total_value": float(raw.get("grand_total_num", raw.get("grand_total", 0)) or 0),
            "buyer_email": raw.get("buyer_email", ""),
            "state": "CA",
            "source_system": "scprs",
            "jurisdiction_level": "state",
            "connector_id": "ca_scprs",
            "tenant_id": "reytech",
            "supplier_id": raw.get("supplier_id", ""),
            "status": raw.get("status", "Active"),
            "search_term": raw.get("first_item", "")[:100],
        }

    def health_check(self) -> dict:
        t0 = time.time()
        try:
            from src.agents.scprs_lookup import FiscalSession
            s = FiscalSession()
            ok = s.init_session()
            ms = int((time.time() - t0) * 1000)
            return {"status": "ok" if ok else "error",
                    "latency_ms": ms,
                    "message": "Session init OK" if ok else "Session init failed"}
        except Exception as e:
            return {"status": "error", "latency_ms": int((time.time() - t0) * 1000),
                    "message": str(e)[:200]}
