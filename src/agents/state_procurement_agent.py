"""
state_procurement_agent.py — Base class for state procurement scrapers.

Each state subclasses this with state-specific search logic.
All agents normalize output to the scprs_po_master schema so
intelligence tables stay consistent regardless of data source.

Usage:
    class TexasProcurementAgent(StateProcurementAgent):
        state = "TX"
        source_system = "txsmartbuy"
        base_url = "https://comptroller.texas.gov/purchasing/"

        def search_by_vendor(self, vendor_name, days_back=730):
            # Texas-specific scraping logic
            ...
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger("procurement")


class StateProcurementAgent:
    """Base class for state procurement data scrapers.

    Subclass for each state and implement the search methods.
    All methods return lists of dicts in scprs_po_master schema.
    """

    state: str = ""
    source_system: str = ""
    base_url: str = ""
    auth_required: bool = False

    def search_by_vendor(self, vendor_name: str,
                         days_back: int = 730) -> list:
        """Find all awards to a specific vendor.
        Input: vendor_name, days_back lookback window
        Output: list of award dicts in po_master schema
        """
        raise NotImplementedError(f"{self.__class__.__name__}.search_by_vendor()")

    def search_by_keyword(self, keyword: str,
                          days_back: int = 730) -> list:
        """Search awards by product keyword/description.
        Input: keyword, days_back
        Output: list of award dicts
        """
        raise NotImplementedError(f"{self.__class__.__name__}.search_by_keyword()")

    def search_by_agency(self, agency: str,
                         days_back: int = 730) -> list:
        """Search all awards for a specific agency.
        Input: agency name or code, days_back
        Output: list of award dicts
        """
        raise NotImplementedError(f"{self.__class__.__name__}.search_by_agency()")

    def normalize(self, raw: dict) -> dict:
        """Map state-specific fields to scprs_po_master schema.
        Sets state, source_system, jurisdiction automatically.
        """
        raw_id = raw.get("id", "")
        if not raw_id:
            raw_id = hashlib.md5(
                json.dumps(raw, sort_keys=True, default=str).encode()
            ).hexdigest()[:16]

        return {
            "id": f"{self.state}-{raw_id}",
            "po_number": raw.get("po_number", raw_id),
            "dept_name": raw.get("agency", ""),
            "institution": raw.get("institution", ""),
            "supplier": raw.get("vendor_name", ""),
            "supplier_id": raw.get("vendor_id", ""),
            "status": raw.get("status", "Active"),
            "start_date": raw.get("award_date", ""),
            "end_date": raw.get("end_date", ""),
            "grand_total": float(raw.get("total", 0) or 0),
            "buyer_name": raw.get("buyer_name", ""),
            "buyer_email": raw.get("buyer_email", ""),
            "search_term": raw.get("search_term", ""),
            "agency_key": raw.get("agency_code", ""),
            "state": self.state,
            "jurisdiction": "state",
            "source_system": self.source_system,
        }

    def store_results(self, results: list, conn=None) -> int:
        """Store normalized results to scprs_po_master. Returns count stored."""
        close_conn = False
        if conn is None:
            import sqlite3
            from src.core.db import DB_PATH
            conn = sqlite3.connect(DB_PATH, timeout=30)
            conn.row_factory = sqlite3.Row
            close_conn = True

        count = 0
        now = datetime.now(timezone.utc).isoformat()
        for r in results:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO scprs_po_master
                    (id, pulled_at, po_number, dept_name, institution, supplier,
                     supplier_id, status, start_date, end_date, grand_total,
                     buyer_name, buyer_email, search_term, agency_key,
                     state, jurisdiction, source_system)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (r["id"], now, r["po_number"], r["dept_name"],
                      r["institution"], r["supplier"], r["supplier_id"],
                      r["status"], r["start_date"], r["end_date"],
                      r["grand_total"], r["buyer_name"], r["buyer_email"],
                      r["search_term"], r["agency_key"],
                      r.get("state", self.state),
                      r.get("jurisdiction", "state"),
                      r.get("source_system", self.source_system)))
                count += 1
            except Exception as e:
                log.debug("Store result: %s", e)

        conn.commit()
        if close_conn:
            conn.close()

        log.info("%s: stored %d/%d results", self.source_system, count, len(results))
        return count


# ── Available state procurement systems (for future expansion) ───────────────
# Each entry documents the public procurement portal for a state.
# Agents will be built as demand requires.

STATE_PROCUREMENT_SYSTEMS = {
    "CA": {
        "name": "SCPRS (State Contracting and Procurement Registration System)",
        "url": "https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx",
        "agent": "FiscalSession",  # Already implemented
        "status": "active",
    },
    "TX": {
        "name": "TxSmartBuy",
        "url": "https://comptroller.texas.gov/purchasing/",
        "agent": None,  # Not yet implemented
        "status": "planned",
    },
    "NY": {
        "name": "NYS Procurement Services",
        "url": "https://ogs.ny.gov/procurement",
        "agent": None,
        "status": "planned",
    },
    "FL": {
        "name": "Florida MFMP",
        "url": "https://www.dms.myflorida.com/business_operations/state_purchasing",
        "agent": None,
        "status": "planned",
    },
    "federal": {
        "name": "USASpending.gov",
        "url": "https://api.usaspending.gov/api/v2",
        "agent": "USASpendingAgent",  # Implemented
        "status": "active",
    },
}
