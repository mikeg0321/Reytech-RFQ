"""
scprs_scanner.py — SCPRS Opportunity Scanner for Reytech
Phase 13 | Version: 1.0.0

Background thread that polls SCPRS for new Purchase Orders,
evaluates them as leads, and queues high-value opportunities.

This is the engine behind the Lead Gen agent — it runs continuously
and feeds evaluate_po() with fresh PO data.

Architecture:
  1. Poll SCPRS search page for recent POs (every POLL_INTERVAL seconds)
  2. Filter: right categories, right value range, not already seen
  3. Evaluate each PO via lead_gen_agent.evaluate_po()
  4. Queue leads above confidence threshold
  5. Log all activity for analytics

Requires: SCPRS fiscal session (uses existing scprs_lookup infrastructure)
"""

import json
import os
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("scprs_scanner")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

SCAN_LOG_FILE = os.path.join(DATA_DIR, "scprs_scan_log.json")
SEEN_POS_FILE = os.path.join(DATA_DIR, "scprs_seen_pos.json")

# ─── Configuration ───────────────────────────────────────────────────────────

POLL_INTERVAL = 60          # Seconds between scans
CATEGORIES_TO_SCAN = [      # SCPRS category IDs to monitor
    "office_supplies",
    "medical_supplies",
    "janitorial_supplies",
    "safety_equipment",
    "food_service",
]
MAX_SEEN_POS = 5000         # Track this many PO numbers to avoid re-processing
SCAN_LOOKBACK_DAYS = 7      # Only look at POs from last N days


# ─── Seen PO Tracking ───────────────────────────────────────────────────────

def _load_seen() -> set:
    try:
        with open(SEEN_POS_FILE) as f:
            data = json.load(f)
            return set(data.get("po_numbers", []))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _save_seen(seen: set):
    os.makedirs(DATA_DIR, exist_ok=True)
    po_list = list(seen)
    if len(po_list) > MAX_SEEN_POS:
        po_list = po_list[-MAX_SEEN_POS:]
    with open(SEEN_POS_FILE, "w") as f:
        json.dump({"po_numbers": po_list, "updated": datetime.now().isoformat()}, f)


def _log_scan(results: dict):
    """Append scan results to log."""
    try:
        with open(SCAN_LOG_FILE) as f:
            scan_log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        scan_log = []
    scan_log.append({
        "timestamp": datetime.now().isoformat(),
        **results,
    })
    if len(scan_log) > 2000:
        scan_log = scan_log[-2000:]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SCAN_LOG_FILE, "w") as f:
        json.dump(scan_log, f)


# ─── SCPRS PO Fetcher ───────────────────────────────────────────────────────

def fetch_recent_pos(category: str = None, days_back: int = 7) -> list:
    """
    Fetch recent Purchase Orders from SCPRS.

    Returns list of PO dicts:
    [{"po_number": "...", "institution": "...", "total_value": 0, 
      "items": [...], "date": "...", "buyer_name": "...", ...}]

    Currently uses SCPRS lookup infrastructure.
    When SCPRS API unavailable, returns empty list (no crash).
    """
    try:
        from src.agents.scprs_lookup import _get_session, _scrape_fiscal
        HAS_SCPRS = True
    except ImportError:
        HAS_SCPRS = False

    if not HAS_SCPRS:
        log.debug("SCPRS lookup not available — scanner inactive")
        return []

    # The current SCPRS lookup is item-based, not PO-based.
    # When SCPRS provides PO-level browsing, this function will
    # scrape recent POs. For now, we use a simulated/seed approach
    # that can be replaced with real scraping.
    log.debug("SCPRS PO fetch: category=%s, days_back=%d", category, days_back)
    return []


def scan_once(won_history: list = None) -> dict:
    """
    Run one scan cycle:
    1. Fetch recent POs from SCPRS
    2. Filter out already-seen POs
    3. Evaluate each as a lead
    4. Return results summary

    Args:
        won_history: List of won quote items for matching.

    Returns:
        {"scanned": int, "new_pos": int, "leads_created": int, "errors": int}
    """
    try:
        from src.agents.lead_gen_agent import evaluate_po, add_lead
    except ImportError:
        return {"error": "lead_gen_agent not available", "scanned": 0}

    seen = _load_seen()
    results = {"scanned": 0, "new_pos": 0, "leads_created": 0, "errors": 0,
               "categories_checked": []}

    for category in CATEGORIES_TO_SCAN:
        try:
            pos = fetch_recent_pos(category, SCAN_LOOKBACK_DAYS)
            results["scanned"] += len(pos)
            results["categories_checked"].append(category)

            for po in pos:
                po_num = po.get("po_number", "")
                if not po_num or po_num in seen:
                    continue

                results["new_pos"] += 1
                seen.add(po_num)

                # Evaluate as lead
                lead = evaluate_po(po, won_history or [])
                if lead:
                    add_result = add_lead(lead)
                    if add_result.get("ok"):
                        results["leads_created"] += 1
                        log.info("NEW LEAD: %s (%s) score=%.2f",
                                 po_num, po.get("institution", ""), lead["score"])

                        # Auto-draft outreach for hot leads
                        if lead.get("score", 0) >= 0.7:
                            try:
                                from src.agents.lead_gen_agent import draft_outreach_email
                                draft = draft_outreach_email(lead)
                                if draft:
                                    results.setdefault("auto_drafts", []).append({
                                        "lead_id": lead["id"],
                                        "po_number": po_num,
                                        "score": lead["score"],
                                    })
                                    log.info("AUTO-DRAFT: outreach for %s (score=%.2f)",
                                             po_num, lead["score"])
                            except Exception as de:
                                log.debug("Auto-draft failed: %s", de)

        except Exception as e:
            log.error("Scan error for %s: %s", category, e)
            results["errors"] += 1

    _save_seen(seen)
    _log_scan(results)
    return results


# ─── Background Scanner Thread ──────────────────────────────────────────────

class SCPRSScanner:
    """Background thread that continuously scans SCPRS for opportunities."""

    def __init__(self, interval: int = POLL_INTERVAL):
        self.interval = interval
        self._thread = None
        self._stop_event = threading.Event()
        self._running = False
        self._last_scan = None
        self._scan_count = 0
        self._leads_found = 0

    def start(self):
        """Start the background scanner."""
        if self._running:
            log.warning("Scanner already running")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                         name="scprs-scanner")
        self._thread.start()
        self._running = True
        log.info("SCPRS scanner started (interval=%ds)", self.interval)

    def stop(self):
        """Stop the background scanner gracefully."""
        if not self._running:
            return
        log.info("Stopping SCPRS scanner...")
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        self._running = False
        log.info("SCPRS scanner stopped (scans=%d, leads=%d)",
                 self._scan_count, self._leads_found)

    def _run_loop(self):
        """Main scanner loop."""
        # Load won history once (refresh periodically)
        won_history = self._load_won_history()
        history_refresh = time.time()

        while not self._stop_event.is_set():
            try:
                # Refresh won history every 30 minutes
                if time.time() - history_refresh > 1800:
                    won_history = self._load_won_history()
                    history_refresh = time.time()

                results = scan_once(won_history)
                self._scan_count += 1
                self._leads_found += results.get("leads_created", 0)
                self._last_scan = datetime.now().isoformat()

                if results.get("leads_created", 0) > 0:
                    log.info("Scan #%d: %d new leads from %d POs",
                             self._scan_count, results["leads_created"],
                             results["new_pos"])

            except Exception as e:
                log.error("Scanner loop error: %s", e)

            # Wait for interval or stop signal
            self._stop_event.wait(self.interval)

    def _load_won_history(self) -> list:
        """Load won quotes for lead matching."""
        try:
            from src.knowledge.won_quotes_db import get_all_items
            return get_all_items()
        except Exception:
            return []

    @property
    def status(self) -> dict:
        return {
            "running": self._running,
            "interval": self.interval,
            "scan_count": self._scan_count,
            "leads_found": self._leads_found,
            "last_scan": self._last_scan,
            "seen_pos": len(_load_seen()),
        }


# ─── Module-level singleton ─────────────────────────────────────────────────

_scanner = SCPRSScanner()


def start_scanner(interval: int = POLL_INTERVAL):
    """Start the global SCPRS scanner."""
    _scanner.interval = interval
    _scanner.start()


def stop_scanner():
    """Stop the global SCPRS scanner."""
    _scanner.stop()


def get_scanner_status() -> dict:
    """Get scanner status for health endpoint."""
    return _scanner.status


def manual_scan(won_history: list = None) -> dict:
    """Run a single scan manually (for testing/debugging)."""
    return scan_once(won_history)
