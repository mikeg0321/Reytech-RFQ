"""
scprs_orchestrator.py — Unified facade for SCPRS agent operations.

Wraps the 7 existing SCPRS agents behind a single interface.
Uses lazy imports to avoid circular dependencies and keep startup light.

Agents wrapped:
  1. scprs_lookup.py        — Local price cache, FI$Cal scraper
  2. scprs_browser.py       — Selenium-based detail page scraper
  3. scprs_intelligence_engine.py — Agency pulls, award monitoring
  4. scprs_universal_pull.py — Unified pull routine
  5. scprs_scanner.py       — Background continuous scanner
  6. scprs_public_search.py — Public CCHCS API search
  7. scprs_scraper_client.py — Remote scraper HTTP client

Usage:
    from src.agents.scprs_orchestrator import scprs
    results = scprs.lookup_price("Nitrile Gloves XL")
    scprs.pull_all()
    status = scprs.get_status()
"""

import logging

log = logging.getLogger("reytech.scprs")


class ScprsOrchestrator:
    """Single entry point for all SCPRS operations."""

    # ── Price Lookup (local cache) ──────────────────────────────────────

    def lookup_price(self, description, item_number="", agency=""):
        """Look up cached SCPRS prices for an item."""
        from src.agents.scprs_lookup import lookup_price
        return lookup_price(description, item_number=item_number, agency=agency)

    def bulk_lookup(self, items, agency=""):
        """Bulk price lookup for multiple items."""
        from src.agents.scprs_lookup import bulk_lookup
        return bulk_lookup(items, agency=agency)

    # ── Intelligence Engine (agency pulls) ──────────────────────────────

    def pull_agency(self, agency_key, **kwargs):
        """Pull SCPRS data for a specific agency."""
        from src.agents.scprs_intelligence_engine import pull_agency
        return pull_agency(agency_key, **kwargs)

    def pull_all(self, **kwargs):
        """Pull all agencies in background."""
        from src.agents.scprs_intelligence_engine import pull_all_agencies_background
        return pull_all_agencies_background(**kwargs)

    def run_award_monitor(self, **kwargs):
        """Check for new awards matching our quotes."""
        from src.agents.scprs_intelligence_engine import run_po_award_monitor
        return run_po_award_monitor(**kwargs)

    # ── Universal Pull ──────────────────────────────────────────────────

    def universal_pull(self, **kwargs):
        """Run unified pull routine across all categories."""
        from src.agents.scprs_universal_pull import run_universal_pull
        return run_universal_pull(**kwargs)

    def check_quotes_against_scprs(self, **kwargs):
        """Compare our quotes against SCPRS market data."""
        from src.agents.scprs_universal_pull import check_quotes_against_scprs
        return check_quotes_against_scprs(**kwargs)

    # ── Scanner (continuous monitoring) ─────────────────────────────────

    def scan_once(self):
        """Run a single scan cycle for new POs."""
        from src.agents.scprs_scanner import scan_once
        return scan_once()

    def get_scanner_status(self):
        """Get background scanner status."""
        from src.agents.scprs_scanner import get_scanner_status
        return get_scanner_status()

    # ── Public Search (CCHCS API) ───────────────────────────────────────

    def public_search(self, query, **kwargs):
        """Search SCPRS via public API (no login required)."""
        from src.agents.scprs_public_search import search_scprs_public
        return search_scprs_public(query, **kwargs)

    # ── Connection Health ───────────────────────────────────────────────

    def test_connection(self):
        """Test SCPRS connectivity."""
        try:
            from src.agents.scprs_lookup import test_connection
            return test_connection()
        except Exception as e:
            log.error("SCPRS connection test failed: %s", e)
            return {"ok": False, "error": str(e)}

    def health_check(self):
        """Comprehensive health check across all SCPRS agents."""
        status = {"ok": True, "agents": {}}
        try:
            conn = self.test_connection()
            status["agents"]["lookup"] = {
                "ok": conn.get("ok", False),
                "detail": conn.get("message", ""),
            }
        except Exception as e:
            status["agents"]["lookup"] = {"ok": False, "detail": str(e)}
            status["ok"] = False

        try:
            scanner = self.get_scanner_status()
            status["agents"]["scanner"] = {
                "ok": True,
                "running": scanner.get("running", False),
            }
        except Exception:
            status["agents"]["scanner"] = {"ok": False, "running": False}

        return status

    # ── Aggregated Status ───────────────────────────────────────────────

    def get_status(self):
        """Aggregate status from all SCPRS agents."""
        result = {"agents": {}}

        # Intelligence engine status
        try:
            from src.agents.scprs_intelligence_engine import get_engine_status
            result["agents"]["intelligence_engine"] = get_engine_status()
        except Exception:
            result["agents"]["intelligence_engine"] = {"error": "unavailable"}

        # Scanner status
        try:
            result["agents"]["scanner"] = self.get_scanner_status()
        except Exception:
            result["agents"]["scanner"] = {"error": "unavailable"}

        # Connection health
        try:
            result["agents"]["lookup"] = self.test_connection()
        except Exception:
            result["agents"]["lookup"] = {"error": "unavailable"}

        return result


# Module-level singleton for convenience
scprs = ScprsOrchestrator()
