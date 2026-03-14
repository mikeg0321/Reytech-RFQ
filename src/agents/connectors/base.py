"""
base.py — Base connector interface for all procurement data sources.

Every active connector must implement: authenticate, search_by_vendor,
normalize, health_check. Other methods are optional.
"""
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger("reytech.connectors")


class BaseConnector:
    """Base class for procurement data connectors."""

    connector_id: str = ""
    name: str = ""
    jurisdiction_level: str = ""
    state: str = ""

    def authenticate(self) -> bool:
        """Authenticate with the data source. Returns True on success."""
        return True

    def search_by_vendor(self, vendor_names: list,
                         from_date: datetime,
                         to_date: datetime = None) -> list:
        """Find all awards where any of these vendor names won."""
        raise NotImplementedError

    def search_by_agency(self, agency: str,
                         from_date: datetime,
                         to_date: datetime = None) -> list:
        """Find all awards for a specific agency."""
        raise NotImplementedError

    def search_by_keyword(self, keyword: str,
                          from_date: datetime,
                          to_date: datetime = None) -> list:
        """Search by product keyword/description."""
        raise NotImplementedError

    def get_all_agencies(self) -> list:
        """Discover all agencies dynamically. No hardcoded lists."""
        return []

    def normalize(self, raw: dict) -> dict:
        """Map source fields to canonical po_master schema."""
        raise NotImplementedError

    def health_check(self) -> dict:
        """Returns {status, latency_ms, message}."""
        return {"status": "unknown", "latency_ms": 0, "message": "Not implemented"}
