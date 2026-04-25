"""
cost_tier_lookup.py — Phase 2 cost-cascade for PC items (2026-04-25)

The 3-tier cascade for empty `unit_cost` cells, in order:

  Tier 1: catalog_first      → find_by_mfg_exact (operator-confirmed catalog hit)
  Tier 2: past_quote         → find_recent_quote_cost (last operator-saved cost
                                for this exact MFG#/UPC across all PCs)
  Tier 3: supplier_scrape    → resolve_sku_url + lookup_from_url (live scrape
                                of Grainger / Uline / S&S / Amazon supplier pages)

Returns a single recommendation per item with:
  {tier, cost, supplier, url, source, confidence, raw}

Provenance discipline (the load-bearing risk for this entire feature):
  * Tier 1 + Tier 2 only return values written by an operator. Catalog rows
    flagged 'legacy_unknown'/'amazon_scrape'/'scprs_scrape' are filtered out
    by find_by_mfg_exact. quote_line_costs only ever contains 'operator' rows.
  * Tier 3 surfaces a live scrape — operator must explicitly Accept before it
    becomes truth. The Accept click writes back through the existing
    cost_source='operator' path so the catalog flywheel still owns the
    long-term truth.

Per-host throttling: 3 requests/sec/host token bucket. Without this, a 6-item
PC × Grainger lookups can spike rapid-fire and trigger 429/Cloudflare. Phase 1
auto_processor used time.sleep(0.5) between Amazon lookups — same idea here,
hardened for concurrent invocation.

The S&S Cloudflare-fallback case (where lookup_from_url returns an Amazon
reference price under the S&S supplier label) is reported with
`confidence='reference_only'` so the UI can warn the operator that the price
is a reference value, not a live S&S quote.
"""
import logging
import threading
import time
from typing import Optional
from urllib.parse import urlparse

log = logging.getLogger("cost_tier_lookup")


# ── Per-host throttle ──────────────────────────────────────────────────────

_HOST_THROTTLE_LOCK = threading.Lock()
_HOST_LAST_CALL = {}  # host → list of recent call timestamps (sliding window)
_HOST_RATE_LIMIT = 3  # max calls per second per host
_HOST_WINDOW_SEC = 1.0


def _host_throttle(url: str) -> None:
    """Sliding-window token-bucket throttle. Sleeps if the host has hit
    the rate limit so the caller doesn't see 429s. Safe under concurrent
    daemon-thread dispatch.

    The window is 1 second; >3 calls in any 1-second window for the same
    host blocks the latest caller until the oldest call rolls off the
    window.
    """
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return
    if not host:
        return
    while True:
        now = time.monotonic()
        with _HOST_THROTTLE_LOCK:
            calls = _HOST_LAST_CALL.setdefault(host, [])
            # Drop expired entries
            cutoff = now - _HOST_WINDOW_SEC
            calls[:] = [t for t in calls if t > cutoff]
            if len(calls) < _HOST_RATE_LIMIT:
                calls.append(now)
                return
            # Sleep until the oldest call rolls off
            sleep_for = calls[0] + _HOST_WINDOW_SEC - now + 0.05
        if sleep_for > 0:
            time.sleep(min(sleep_for, 2.0))


# ── Tier cascade ───────────────────────────────────────────────────────────


def _tier1_catalog(mfg: str, upc: str) -> Optional[dict]:
    """Tier 1: operator-confirmed catalog hit on exact MFG# or UPC.

    find_by_mfg_exact already gates on
    cost_source IN ('operator', 'catalog_confirmed') — Phase 1 guarantee.
    """
    try:
        from src.agents.product_catalog import find_by_mfg_exact
    except Exception as e:
        log.debug("tier1 import: %s", e)
        return None
    try:
        hit = find_by_mfg_exact(mfg or None, upc=upc or None)
    except Exception as e:
        log.debug("tier1 lookup: %s", e)
        return None
    if not hit or not hit.get("cost", 0) > 0:
        return None
    return {
        "tier": "catalog",
        "cost": float(hit["cost"]),
        "supplier": hit.get("best_supplier") or "Catalog",
        "url": hit.get("cost_source_url") or "",
        "source": "Catalog (operator-confirmed)",
        "confidence": "high",
        "raw": {"product_id": hit.get("id"), "name": hit.get("name")},
    }


def _tier2_past_quote(mfg: str, upc: str) -> Optional[dict]:
    """Tier 2: most recent operator-confirmed cost for this exact MFG#/UPC,
    across all prior PCs.

    Reads quote_line_costs which only ever contains operator-confirmed rows
    (see routes_pricecheck._do_save_prices). No Amazon/SCPRS pollution path.
    """
    try:
        from src.agents.product_catalog import find_recent_quote_cost
    except Exception as e:
        log.debug("tier2 import: %s", e)
        return None
    try:
        hit = find_recent_quote_cost(mfg or None, upc=upc or None)
    except Exception as e:
        log.debug("tier2 lookup: %s", e)
        return None
    if not hit or not hit.get("cost", 0) > 0:
        return None
    return {
        "tier": "past_quote",
        "cost": float(hit["cost"]),
        "supplier": hit.get("supplier_name") or "",
        "url": hit.get("cost_source_url") or "",
        "source": f"Last quote {hit.get('pc_id') or ''} ({hit.get('accepted_at', '')[:10]})".strip(),
        "confidence": "high",
        "raw": {"pc_id": hit.get("pc_id"), "accepted_at": hit.get("accepted_at")},
    }


# Phase 4-A (2026-04-25): explicit allowlist of supplier hosts that may
# return a `cost`. Amazon, Walmart, Target etc are RETAIL — their prices
# are reference data per CLAUDE.md "Amazon Prices Are NOT Supplier Costs"
# and Phase 1's whole architecture. We allowlist only known wholesale
# distributors. Adding a new host here is a deliberate decision.
_SUPPLIER_HOST_ALLOWLIST = frozenset({
    "grainger.com", "www.grainger.com",
    "uline.com", "www.uline.com",
    "ssww.com", "www.ssww.com",
    "mcmaster.com", "www.mcmaster.com",
    "fishersci.com", "www.fishersci.com",
    "medline.com", "www.medline.com",
    "aedstore.com", "www.aedstore.com",
    "aedbrands.com", "www.aedbrands.com",
    "buyaedsusa.com", "www.buyaedsusa.com",
    "quickie-wheelchairs.com", "www.quickie-wheelchairs.com",
})


def _host_in_allowlist(url: str) -> bool:
    """True iff the URL's host is a wholesale supplier we trust to return
    a cost basis. Amazon / retail / unknown hosts are refused."""
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host in _SUPPLIER_HOST_ALLOWLIST


def _tier3_supplier_scrape(item: dict) -> Optional[dict]:
    """Tier 3: live supplier scrape via SKU-prefix routing OR item.item_link
    URL-host fallback.

    Two routing paths, in order:
      1. MFG# → resolve_sku_url maps a MFG# to a supplier URL.
      2. URL fallback (Phase 4-A): if MFG# routing returned no allowlisted URL,
         and `item.item_link` is set, AND its host is in the supplier allowlist,
         scrape the item_link directly. This recovers items where the operator
         pasted a Grainger/Uline URL but never set the MFG#.

    Both paths gate on `_SUPPLIER_HOST_ALLOWLIST` so Amazon / retail / unknown
    hosts can never become a cost basis (Phase 1 architectural rule).

    Cloudflare-fallback caveat: when S&S is blocked, lookup_from_url returns
    an Amazon-derived reference price under the S&S supplier label. We mark
    confidence='reference_only' so the UI warns instead of presenting it as
    a clean S&S quote.
    """
    if not isinstance(item, dict):
        return None
    mfg = (item.get("mfg_number") or item.get("item_number") or "").strip()
    item_link = (item.get("item_link") or "").strip()

    try:
        from src.agents.sku_url_resolver import resolve_sku_url
        from src.agents.item_link_lookup import lookup_from_url
    except Exception as e:
        log.debug("tier3 import: %s", e)
        return None

    # Path 1: try MFG# routing first
    url = ""
    routed_supplier = ""
    if mfg:
        routed = resolve_sku_url(mfg) or {}
        candidate = (routed.get("url") or "").strip()
        if candidate and _host_in_allowlist(candidate):
            url = candidate
            routed_supplier = routed.get("supplier") or ""
        elif candidate:
            log.debug("tier3 MFG-route REFUSED non-allowlisted host: %s", candidate)

    # Path 2: fall back to item_link URL host (Phase 4-A new)
    if not url and item_link and _host_in_allowlist(item_link):
        url = item_link
        log.info("tier3 URL-host fallback: %s", item_link)

    if not url:
        return None

    _host_throttle(url)

    try:
        result = lookup_from_url(url)
    except Exception as e:
        log.debug("tier3 lookup_from_url: %s", e)
        return None

    if not result:
        return None
    cost = result.get("price") or result.get("cost") or 0
    try:
        cost = float(cost)
    except (TypeError, ValueError):
        cost = 0
    if cost <= 0:
        return None

    supplier = result.get("supplier") or routed_supplier or ""
    confidence = "high"
    source_label = f"{supplier} (live)"

    # S&S Cloudflare-fallback signal: lookup_from_url sets reference_source
    # when S&S was blocked and price came from Amazon/Catalog/Claude instead.
    ref_source = result.get("reference_source")
    if ref_source:
        confidence = "reference_only"
        source_label = f"{supplier} blocked — {ref_source} reference"

    return {
        "tier": "supplier_scrape",
        "cost": cost,
        "supplier": supplier,
        "url": result.get("url") or url,
        "source": source_label,
        "confidence": confidence,
        "raw": {"reference_source": ref_source, "title": result.get("title", "")},
    }


def lookup_tiers(item: dict) -> Optional[dict]:
    """Run the full tier cascade for one PC item. Returns the FIRST tier hit
    (most authoritative wins), or None if no tier produced a usable result.

    Caller decides what to do with the recommendation — typically present it
    in the UI for operator Accept. Operator's Accept click is what writes the
    cost back through the existing cost_source='operator' flywheel.

    `item` shape: PC item dict — reads `mfg_number` / `item_number` / `upc`.
    """
    if not isinstance(item, dict):
        return None
    mfg = (item.get("mfg_number") or item.get("item_number") or "").strip()
    upc = (item.get("upc") or "").strip()
    if not mfg and not upc:
        return None

    # Tier 1
    hit = _tier1_catalog(mfg, upc)
    if hit:
        return hit

    # Tier 2
    hit = _tier2_past_quote(mfg, upc)
    if hit:
        return hit

    # Tier 3 (Phase 4-A: receives full item dict so it can fall back to
    # item.item_link when MFG# routing returns no allowlisted URL)
    hit = _tier3_supplier_scrape(item)
    if hit:
        return hit

    return None
