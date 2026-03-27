"""
pc_enrichment_pipeline.py — Unified PC Auto-Enrichment Pipeline
Version: 1.0

Single entry point for all PC enrichment: identifier extraction, catalog match,
SCPRS lookup, URL/web price lookup, and pricing oracle recommendations.
Replaces the two overlapping functions _auto_enrich_pc (routes_pricecheck.py)
and _auto_price_new_pc (dashboard.py).

Called in a background daemon thread after PC creation (email, upload, split).
Never overwrites user-entered data.
"""

import logging
import os
import re
import threading
import time
from datetime import datetime

log = logging.getLogger("reytech.enrichment")

# ─── Module-level status dict for live polling ────────────────────────────────
# Keyed by pc_id. Polled by /api/pricecheck/<pcid>/enrichment-status.
ENRICHMENT_STATUS = {}

# Max concurrent enrichments
_LOCK = threading.Lock()


def enrich_pc(pc_id: str, force: bool = False):
    """Unified PC auto-enrichment pipeline.

    Steps:
        1. Extract identifiers (MFG#, ASIN, UPC, NSN) from descriptions
        2. Batch catalog match
        3. SCPRS KB lookup
        4. URL extraction + web price lookup (limited to 3 items)
        5. Pricing oracle recommendations

    Args:
        pc_id: Price check ID (e.g. "pc_abc12345")
        force: If True, re-enrich even if already complete
    """
    # Set trace context for this enrichment run
    try:
        from src.core.tracing import set_trace_id
        set_trace_id(operation=f"enrich-{pc_id}")
    except Exception:
        pass

    # Guard against double-runs
    with _LOCK:
        if ENRICHMENT_STATUS.get(pc_id, {}).get("running"):
            log.info("ENRICH %s: already running, skipping", pc_id)
            return
        ENRICHMENT_STATUS[pc_id] = {
            "running": True,
            "started": datetime.now().isoformat(),
            "phase": "loading",
            "progress": "",
            "steps_done": [],
            "error": None,
        }

    try:
        _run_pipeline(pc_id, force)
    except Exception as e:
        log.error("ENRICH %s FAILED: %s", pc_id, e, exc_info=True)
        ENRICHMENT_STATUS[pc_id]["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        ENRICHMENT_STATUS[pc_id]["phase"] = "failed"
        # Persist failure on PC
        try:
            from src.api.dashboard import _load_price_checks, _save_single_pc
            pcs = _load_price_checks()
            pc = pcs.get(pc_id)
            if pc:
                pc["enrichment_status"] = "failed"
                pc["enrichment_error"] = str(e)[:200]
                _save_single_pc(pc_id, pc)
        except Exception:
            pass
    finally:
        ENRICHMENT_STATUS[pc_id]["running"] = False


def _run_pipeline(pc_id: str, force: bool):
    """Core pipeline logic — called by enrich_pc() with error wrapping."""
    from src.api.dashboard import _load_price_checks, _save_single_pc

    # ── Load PC ──────────────────────────────────────────────────────────
    pcs = _load_price_checks()
    pc = pcs.get(pc_id)
    if not pc:
        log.warning("ENRICH %s: PC not found", pc_id)
        return

    # Check if already enriched
    if not force and pc.get("enrichment_status") == "complete":
        log.info("ENRICH %s: already complete, skipping (use force=True to re-run)", pc_id)
        return

    items = pc.get("items", [])
    if not items:
        items = pc.get("parsed", {}).get("line_items", [])
    if not items:
        log.info("ENRICH %s: no items to enrich", pc_id)
        return

    # Mark as enriching immediately
    pc["enrichment_status"] = "enriching"
    _save_single_pc(pc_id, pc)

    # Ensure each item has a pricing dict
    for it in items:
        if "pricing" not in it:
            it["pricing"] = {}

    institution = pc.get("institution", "")
    total = len(items)
    counters = {
        "identifiers_extracted": 0,
        "catalog_matched": 0,
        "scprs_matched": 0,
        "urls_extracted": 0,
        "web_prices_found": 0,
        "oracle_priced": 0,
        "total_items": total,
    }

    # ── Step 1: Extract identifiers from descriptions ────────────────────
    _update_status(pc_id, "identifiers", f"0/{total} items")
    try:
        from src.agents.item_enricher import parse_identifiers
        for i, it in enumerate(items):
            desc = it.get("description", "")
            if not desc:
                continue
            ids = parse_identifiers(desc)
            if not ids:
                continue
            # Populate MFG# if not already set
            pmfg = ids.get("primary_mfg_number", "")
            if pmfg and not it.get("mfg_number"):
                it["mfg_number"] = pmfg
                counters["identifiers_extracted"] += 1
            # Populate ASIN/Amazon URL
            asin = ids.get("primary_asin", "")
            if asin and not it["pricing"].get("asin"):
                it["pricing"]["asin"] = asin
            amazon_url = ids.get("amazon_url", "")
            if amazon_url and not it.get("item_link"):
                it["item_link"] = amazon_url
                it["item_supplier"] = "Amazon"
            # Search URL for manual fallback
            search_url = ids.get("search_url", "")
            if search_url and not it["pricing"].get("search_url"):
                it["pricing"]["search_url"] = search_url
            _update_status(pc_id, "identifiers", f"{i+1}/{total} items")
    except Exception as e:
        log.warning("ENRICH %s: identifier extraction error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("identifiers")

    # ── Step 2: Extract URLs from descriptions ───────────────────────────
    _update_status(pc_id, "url_extraction", "scanning descriptions")
    try:
        counters["urls_extracted"] = _extract_urls_from_items(items)
    except Exception as e:
        log.debug("ENRICH %s: URL extraction error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("url_extraction")

    # ── Step 3: Catalog batch match ──────────────────────────────────────
    _update_status(pc_id, "catalog_match", f"matching {total} items")
    try:
        from src.agents.product_catalog import match_items_batch
        batch_input = [
            {"idx": i, "description": it.get("description", ""),
             "part_number": it.get("mfg_number", "") or it.get("part_number", "")}
            for i, it in enumerate(items)
        ]
        batch_results = match_items_batch(batch_input)
        for r in (batch_results or []):
            idx = r.get("idx", -1)
            if idx < 0 or idx >= len(items):
                continue
            if not r.get("matched") or r.get("confidence", 0) < 0.50:
                continue
            it = items[idx]
            it["pricing"]["catalog_match"] = r.get("canonical_name", r.get("catalog_match", ""))
            it["pricing"]["catalog_cost"] = r.get("best_cost") or r.get("last_cost", 0)
            it["pricing"]["catalog_confidence"] = r.get("confidence", 0)
            it["pricing"]["catalog_product_id"] = r.get("product_id") or r.get("id")
            if r.get("recommended_price") and not it["pricing"].get("recommended_price"):
                it["pricing"]["catalog_recommended"] = r["recommended_price"]
                it["pricing"]["recommended_price"] = r["recommended_price"]
            cat_cost = r.get("best_cost") or r.get("last_cost", 0)
            if cat_cost > 0 and not it["pricing"].get("unit_cost"):
                it["pricing"]["unit_cost"] = cat_cost
            if not it.get("mfg_number") and r.get("mfg_number"):
                it["mfg_number"] = r["mfg_number"]
            if not it.get("item_link") and r.get("supplier_url"):
                it["item_link"] = r["supplier_url"]
                it["item_supplier"] = r.get("supplier_name", "")
            counters["catalog_matched"] += 1
    except Exception as e:
        log.warning("ENRICH %s: catalog match error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("catalog_match")

    # ── Step 4: SCPRS KB lookup ──────────────────────────────────────────
    _update_status(pc_id, "scprs_lookup", f"0/{total} items")
    try:
        from src.knowledge.won_quotes_db import find_similar_items
        for i, it in enumerate(items):
            if it["pricing"].get("scprs_price"):
                counters["scprs_matched"] += 1
                continue  # already has SCPRS data
            desc = it.get("description", "")
            pn = it.get("mfg_number", "") or it.get("part_number", "")
            matches = find_similar_items(pn, desc, max_results=1, min_confidence=0.30)
            if matches:
                best = matches[0]
                q = best.get("quote", best)
                scprs_price = q.get("unit_price", 0)
                if scprs_price and scprs_price > 0:
                    it["pricing"]["scprs_price"] = scprs_price
                    it["pricing"]["scprs_match"] = (q.get("description", "") or "")[:60]
                    it["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    it["pricing"]["scprs_source"] = "scprs_kb"
                    it["pricing"]["scprs_po"] = q.get("po_number", "")
                    # Propagate part number from SCPRS
                    scprs_pn = q.get("item_number", "")
                    if scprs_pn and not it.get("mfg_number"):
                        it["mfg_number"] = scprs_pn
                    # Use as cost if nothing better
                    if not it["pricing"].get("unit_cost"):
                        it["pricing"]["unit_cost"] = scprs_price
                    counters["scprs_matched"] += 1
            _update_status(pc_id, "scprs_lookup", f"{i+1}/{total} items")
    except Exception as e:
        log.warning("ENRICH %s: SCPRS lookup error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("scprs_lookup")

    # ── Step 5: Web price lookup for items with URLs (max 3) ─────────────
    _update_status(pc_id, "web_lookup", "checking supplier URLs")
    try:
        from src.agents.item_link_lookup import lookup_from_url
        from src.core.circuit_breaker import get_breaker, CircuitOpenError
        _web_breaker = get_breaker("web_search")
        web_count = 0
        for it in items:
            if web_count >= 3:
                break  # Rate limit — web lookups are slow
            url = it.get("item_link", "")
            if not url or not url.startswith("http"):
                continue
            if it["pricing"].get("web_price"):
                continue  # already has web price
            try:
                result = _web_breaker.call(lookup_from_url, url)
                if result.get("ok") and result.get("price"):
                    it["pricing"]["web_price"] = result["price"]
                    it["pricing"]["web_source"] = result.get("supplier", "")
                    if result.get("part_number") and not it.get("mfg_number"):
                        it["mfg_number"] = result["part_number"]
                    if result.get("manufacturer"):
                        it["pricing"]["manufacturer"] = result["manufacturer"]
                    if not it["pricing"].get("unit_cost") and result["price"] > 0:
                        it["pricing"]["unit_cost"] = result["price"]
                    counters["web_prices_found"] += 1
                    web_count += 1
                time.sleep(1.0)  # Rate limit between lookups
            except Exception as e:
                log.debug("ENRICH %s: web lookup error for %s: %s", pc_id, url[:50], e)
    except ImportError:
        log.debug("item_link_lookup not available")
    except Exception as e:
        log.warning("ENRICH %s: web lookup error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("web_lookup")

    # ── Step 5b: Claude web search for unpriced items (max 5) ────────────
    _update_status(pc_id, "web_search", "searching web for unpriced items")
    try:
        from src.agents.web_price_research import search_product_price
        from src.core.circuit_breaker import get_breaker as _gb2, CircuitOpenError as _coe2
        _api_breaker = _gb2("web_search")
        ws_count = 0
        for it in items:
            if ws_count >= 5:
                break  # Limit API calls
            # Skip items that already have pricing data
            if it.get("unit_price") and it["unit_price"] > 0:
                continue
            if it["pricing"].get("unit_cost") or it["pricing"].get("recommended_price"):
                continue
            desc = it.get("description", "")
            pn = it.get("mfg_number", "") or it.get("part_number", "")
            if not desc and not pn:
                continue
            try:
                result = _api_breaker.call(
                    search_product_price,
                    description=desc, part_number=pn,
                    qty=it.get("qty", 1), uom=it.get("uom", "EA"),
                )
                if result.get("found") and result.get("price", 0) > 0:
                    it["pricing"]["web_price"] = result["price"]
                    it["pricing"]["web_source"] = result.get("source", "")
                    it["pricing"]["web_url"] = result.get("url", "")
                    it["pricing"]["unit_cost"] = result["price"]
                    web_url = result.get("url", "")
                    if web_url and not it.get("item_link"):
                        it["item_link"] = web_url
                        try:
                            from src.agents.item_link_lookup import detect_supplier
                            it["item_supplier"] = detect_supplier(web_url)
                        except Exception:
                            it["item_supplier"] = result.get("source", "Web")
                    web_pn = result.get("part_number", "")
                    if web_pn and not it.get("mfg_number"):
                        it["mfg_number"] = web_pn
                    counters["web_prices_found"] += 1
                    ws_count += 1
                    log.debug("ENRICH %s: web search found %s → $%.2f via %s",
                              pc_id, desc[:40], result["price"], result.get("source", ""))
                time.sleep(1.0)  # Rate limit
            except Exception as e:
                log.debug("ENRICH %s: web search error for '%s': %s", pc_id, desc[:40], e)
    except ImportError:
        log.debug("web_price_research not available")
    except Exception as e:
        log.debug("ENRICH %s: web search error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("web_search")

    # ── Step 6: Pricing oracle recommendations ───────────────────────────
    _update_status(pc_id, "pricing_oracle", f"0/{total} items")
    try:
        from src.knowledge.pricing_oracle import recommend_price
        for i, it in enumerate(items):
            # Skip items already priced by user or earlier steps
            if it.get("unit_price") and it["unit_price"] > 0:
                continue
            if it["pricing"].get("recommended_price"):
                continue
            desc = it.get("description", "")
            pn = it.get("mfg_number", "") or it.get("part_number", "")
            cost = it["pricing"].get("unit_cost") or it["pricing"].get("catalog_cost") or 0
            scprs = it["pricing"].get("scprs_price") or 0
            rec = recommend_price(
                pn, desc,
                supplier_cost=cost if cost > 0 else None,
                scprs_price=scprs if scprs > 0 else None,
                agency=institution,
            )
            if rec:
                if rec.get("recommended_price"):
                    it["pricing"]["recommended_price"] = rec["recommended_price"]
                if rec.get("aggressive_price"):
                    it["pricing"]["aggressive_price"] = rec["aggressive_price"]
                if rec.get("safe_price"):
                    it["pricing"]["safe_price"] = rec["safe_price"]
                if rec.get("data_quality"):
                    it["pricing"]["data_quality"] = rec["data_quality"]
                counters["oracle_priced"] += 1
            _update_status(pc_id, "pricing_oracle", f"{i+1}/{total} items")
    except Exception as e:
        log.warning("ENRICH %s: pricing oracle error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("pricing_oracle")

    # ── Step 7: Pricing Oracle V2 (FI$Cal intelligence) ──────────────────
    _update_status(pc_id, "oracle_v2", "checking FI$Cal intelligence")
    try:
        from src.core.pricing_oracle_v2 import get_pricing, lock_cost, auto_learn_mapping
        for it in items:
            desc = it.get("description", "")
            if not desc:
                continue
            oracle = get_pricing(
                description=desc,
                quantity=it.get("quantity", it.get("qty", 1)),
                cost=it["pricing"].get("unit_cost") or it.get("supplier_cost"),
                item_number=it.get("item_number", ""),
            )
            if oracle.get("recommendation", {}).get("quote_price"):
                it["oracle_price"] = oracle["recommendation"]["quote_price"]
                it["oracle_confidence"] = oracle["recommendation"]["confidence"]
                it["oracle_rationale"] = oracle["recommendation"]["rationale"]
                if not it["pricing"].get("recommended_price"):
                    it["pricing"]["recommended_price"] = oracle["recommendation"]["quote_price"]
                    it["pricing"]["price_source"] = f"oracle_{oracle['recommendation']['confidence']}"
                    counters["oracle_priced"] += 1
            # Auto-lock cost
            cost_val = it["pricing"].get("unit_cost") or it.get("supplier_cost")
            if cost_val:
                try:
                    lock_cost(desc, float(cost_val), source="auto_enrich", expires_days=30,
                              item_number=it.get("item_number", ""))
                except Exception:
                    pass
            # Auto-learn mapping
            if it["pricing"].get("catalog_match"):
                try:
                    auto_learn_mapping(desc, it["pricing"]["catalog_match"],
                                       item_number=it.get("item_number", ""), confidence=0.6)
                except Exception:
                    pass
    except ImportError:
        log.debug("pricing_oracle_v2 not available")
    except Exception as e:
        log.debug("ENRICH %s: oracle v2 error: %s", pc_id, e)
    ENRICHMENT_STATUS[pc_id]["steps_done"].append("oracle_v2")

    # ── Save enriched PC ─────────────────────────────────────────────────
    _update_status(pc_id, "saving", "persisting results")
    pc["items"] = items
    if "parsed" in pc and pc["parsed"].get("line_items"):
        pc["parsed"]["line_items"] = items
    pc["enrichment_status"] = "complete"
    pc["enrichment_at"] = datetime.now().isoformat()
    pc["enrichment_summary"] = counters
    pc["auto_priced"] = True
    pc["auto_priced_count"] = counters["catalog_matched"] + counters["scprs_matched"] + counters["oracle_priced"]
    pc["auto_priced_at"] = datetime.now().isoformat()
    if pc.get("status") == "parsed":
        pc["status"] = "priced"
    _save_single_pc(pc_id, pc)

    # Update live status
    ENRICHMENT_STATUS[pc_id] = {
        "running": False,
        "completed": datetime.now().isoformat(),
        "summary": counters,
        "error": None,
    }

    total_found = counters["catalog_matched"] + counters["scprs_matched"] + counters["web_prices_found"]
    log.info("ENRICH %s COMPLETE: %d ids, %d catalog, %d SCPRS, %d URLs, %d web, %d oracle — %d/%d items enriched",
             pc_id, counters["identifiers_extracted"], counters["catalog_matched"],
             counters["scprs_matched"], counters["urls_extracted"],
             counters["web_prices_found"], counters["oracle_priced"],
             total_found, total)

    # Send notification
    try:
        from src.agents.notify_agent import send_alert
        pc_num = pc.get("pc_number", pc_id)
        send_alert("bell", f"Auto-enriched: {pc_num} — {total_found}/{total} items",
                    {"type": "auto_enrich", "pc_id": pc_id})
    except Exception:
        pass


def enrich_pc_background(pc_id: str, force: bool = False):
    """Launch enrichment in a background daemon thread."""
    threading.Thread(target=enrich_pc, args=(pc_id, force), daemon=True).start()


def _update_status(pc_id: str, phase: str, progress: str):
    """Update the live polling status dict."""
    if pc_id in ENRICHMENT_STATUS:
        ENRICHMENT_STATUS[pc_id]["phase"] = phase
        ENRICHMENT_STATUS[pc_id]["progress"] = progress


def _extract_urls_from_items(items: list) -> int:
    """Extract supplier URLs embedded in item descriptions.
    Many DocuSign 704s have URLs like 'Toothpaste https://www.dollartree.com/...'
    Sets item['item_link'] and item['item_supplier']. Returns count extracted."""
    extracted = 0
    try:
        from src.agents.item_link_lookup import SUPPLIER_MAP
    except ImportError:
        SUPPLIER_MAP = {}

    _url_re = re.compile(r'(https?://[^\s"\'<>)\]]+)')

    for it in items:
        if it.get("item_link"):
            continue  # already has a link
        desc = it.get("description", "")
        m = _url_re.search(desc)
        if m:
            url = m.group(1).rstrip(".,;:")
            it["item_link"] = url
            # Detect supplier from domain
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc.lower().replace("www.", "")
                for map_domain, supplier_name in SUPPLIER_MAP.items():
                    if map_domain in domain:
                        it["item_supplier"] = supplier_name
                        break
                else:
                    it["item_supplier"] = domain.split(".")[0].title()
            except Exception:
                it["item_supplier"] = ""
            extracted += 1
    return extracted
