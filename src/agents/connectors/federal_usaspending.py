"""
federal_usaspending.py — USASpending.gov connector.

SCOPE: Federal awards filtered to CA place-of-performance + Reytech NAICS codes.
Reytech is NOT pursuing federal contracts — this is for pricing intelligence only.
"""
import time
import logging
from datetime import datetime

from src.agents.connectors.base import BaseConnector

log = logging.getLogger("reytech.connectors.federal_usaspending")

REYTECH_NAICS_CODES = [
    "339112", "339113", "423450", "423490", "339999", "424210",
]


class USASpendingConnector(BaseConnector):
    connector_id = "federal_usaspending"
    name = "USASpending.gov"
    jurisdiction_level = "federal"
    state = "federal"

    CA_PLACE_FILTER = "CA"

    def search_by_vendor(self, vendor_names: list,
                         from_date: datetime,
                         to_date: datetime = None) -> list:
        try:
            from src.agents.usaspending_agent import search_awards
            results = []
            for name in vendor_names:
                raw = search_awards([name],
                    date_range_days=(datetime.now() - from_date).days)
                # Filter to CA place of performance
                ca_only = [r for r in raw
                           if r.get("place_of_performance", "") == self.CA_PLACE_FILTER
                           or not r.get("place_of_performance")]
                results.extend(ca_only)
                time.sleep(6)
            return results
        except Exception as e:
            log.error("USASpending vendor search failed: %s", e)
            return []

    def search_by_keyword(self, keyword: str,
                          from_date: datetime,
                          to_date: datetime = None) -> list:
        try:
            from src.agents.usaspending_agent import search_awards
            raw = search_awards([keyword],
                date_range_days=(datetime.now() - from_date).days)
            return [r for r in raw
                    if r.get("place_of_performance", "") == self.CA_PLACE_FILTER
                    or not r.get("place_of_performance")]
        except Exception as e:
            log.error("USASpending keyword search failed: %s", e)
            return []

    def normalize(self, raw: dict) -> dict:
        # USASpending agent already normalizes
        raw.setdefault("connector_id", "federal_usaspending")
        raw.setdefault("jurisdiction_level", "federal")
        return raw

    def health_check(self) -> dict:
        t0 = time.time()
        try:
            import requests
            r = requests.get("https://api.usaspending.gov/api/v2/references/agency/", timeout=10)
            ms = int((time.time() - t0) * 1000)
            return {"status": "ok" if r.status_code == 200 else "error",
                    "latency_ms": ms, "message": f"HTTP {r.status_code}"}
        except Exception as e:
            return {"status": "error", "latency_ms": int((time.time() - t0) * 1000),
                    "message": str(e)[:200]}
