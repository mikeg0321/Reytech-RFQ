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
# Protected by _LOCK for all mutations. TTL-evicted to prevent memory leaks.
ENRICHMENT_STATUS = {}

_LOCK = threading.Lock()
_MAX_STATUS_AGE_SECS = 3600  # evict completed entries after 1 hour


def _evict_stale_entries():
    """Remove completed/failed entries older than _MAX_STATUS_AGE_SECS. Call inside _LOCK."""
    now = time.time()
    stale = [
        k for k, v in ENRICHMENT_STATUS.items()
        if not v.get("running") and now - v.get("_ts", 0) > _MAX_STATUS_AGE_SECS
    ]
    for k in stale:
        del ENRICHMENT_STATUS[k]
    if stale:
        log.debug("Evicted %d stale enrichment status entries", len(stale))


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
        _evict_stale_entries()
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
            "_ts": time.time(),
        }

    try:
        _run_pipeline(pc_id, force)
    except Exception as e:
        log.error("ENRICH %s FAILED: %s", pc_id, e, exc_info=True)
        with _LOCK:
            if pc_id in ENRICHMENT_STATUS:
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
        with _LOCK:
            if pc_id in ENRICHMENT_STATUS:
                ENRICHMENT_STATUS[pc_id]["running"] = False
                ENRICHMENT_STATUS[pc_id]["_ts"] = time.time()


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
            # Also include substituted item column (has UPCs/MFG#s on 704 forms)
            _sub = it.get("substituted", "")
            _parse_text = (desc + " " + _sub).strip() if _sub else desc
            if not _parse_text:
                continue
            ids = parse_identifiers(_parse_text)
            if not ids:
                continue
            # Populate MFG# if not already set
            pmfg = ids.get("primary_mfg_number", "")
            if pmfg and not it.get("mfg_number"):
                it["mfg_number"] = pmfg
                counters["identifiers_extracted"] += 1
            # Populate UPC if extracted
            pupc = ids.get("primary_upc", "")
            if pupc and not it.get("upc"):
                it["upc"] = pupc
                it["pricing"]["upc"] = pupc
            # Detect barcode MFG# (12-13 digit number = UPC barcode)
            _mfg = it.get("mfg_number", "")
            if _mfg and _mfg.isdigit() and len(_mfg) in (12, 13) and not it.get("upc"):
                it["upc"] = _mfg
                it["pricing"]["upc"] = _mfg
            # Store supplier-specific SKUs (S&S, Uline, Grainger, etc.)
            _sup_skus = ids.get("supplier_skus", {})
            if _sup_skus:
                existing = it.get("supplier_skus") or {}
                existing.update(_sup_skus)
                it["supplier_skus"] = existing
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
    _mark_step_done(pc_id,"identifiers")

    # ── Step 1.5: Resolve UPCs via Amazon product lookup ───────────────
    _update_status(pc_id, "upc_resolution", "resolving barcodes")
    _upc_lookups = 0
    _UPC_LIMIT = 3  # max Amazon lookups per PC to preserve SerpApi credits
    try:
        from src.agents.product_research import search_amazon, lookup_amazon_product
        for i, it in enumerate(items):
            if _upc_lookups >= _UPC_LIMIT:
                break
            _upc = it.get("upc", "")
            if not _upc:
                continue
            if it["pricing"].get("amazon_asin") and it["pricing"].get("amazon_price"):
                continue  # already resolved
            try:
                results = search_amazon(_upc, max_results=1)
                if results and results[0].get("price", 0) > 0:
                    r = results[0]
                    _amz_asin = r.get("asin", "")
                    it["pricing"]["amazon_price"] = r["price"]
                    it["pricing"]["amazon_asin"] = _amz_asin
                    it["pricing"]["amazon_url"] = r.get("url", "")
                    it["pricing"]["amazon_title"] = r.get("title", "")[:200]
                    # Capture product image
                    _photo = r.get("photo_url", "")
                    if _photo:
                        it["pricing"]["photo_url"] = _photo
                    if not it.get("item_link"):
                        it["item_link"] = r.get("url", "")
                        it["item_supplier"] = "Amazon"
                    # ASIN product lookup for list/sale price split + higher-res image
                    if _amz_asin:
                        try:
                            _prod = lookup_amazon_product(_amz_asin)
                            if _prod:
                                if _prod.get("photo_url") and not _photo:
                                    it["pricing"]["photo_url"] = _prod["photo_url"]
                                if _prod.get("list_price"):
                                    it["pricing"]["list_price"] = _prod["list_price"]
                                    it["list_price"] = _prod["list_price"]
                                if _prod.get("sale_price"):
                                    it["pricing"]["sale_price"] = _prod["sale_price"]
                                    it["sale_price"] = _prod["sale_price"]
                        except Exception:
                            pass
                    counters.setdefault("upc_resolved", 0)
                    counters["upc_resolved"] += 1
                    log.info("ENRICH %s: UPC %s → %s $%.2f (ASIN: %s)",
                             pc_id, _upc, r.get("title", "")[:30], r["price"], _amz_asin)
                    _upc_lookups += 1
                    time.sleep(0.5)  # rate limit
            except Exception as e:
                log.debug("ENRICH %s: UPC resolution error for %s: %s", pc_id, _upc, e)
    except ImportError:
        pass
    except Exception as e:
        log.debug("ENRICH %s: UPC resolution error: %s", pc_id, e)
    _mark_step_done(pc_id, "upc_resolution")

    # ── Step 1.5b: S&S / supplier items → direct S&S lookup with pricing ─
    # Recognise S&S item numbers, build the S&S URL, look up current MSRP
    # + sale price. Catalog first, then direct S&S scrape/Claude, Amazon last.
    _update_status(pc_id, "ssww_resolution", "resolving S&S items")
    _ssww_lookups = 0
    _SSWW_LIMIT = 8
    try:
        from src.agents.product_catalog import find_by_supplier_sku
        from src.agents.sku_url_resolver import resolve_sku_url
        for i, it in enumerate(items):
            if _ssww_lookups >= _SSWW_LIMIT:
                break
            _sup_skus = it.get("supplier_skus") or {}
            _is_ssww = _sup_skus.get("ssww") or _sup_skus.get("ssww_item")
            # Also detect S&S from item_link, description, or MFG# pattern
            _link = it.get("item_link", "")
            _desc = it.get("description", "")
            _mfg = it.get("mfg_number", "") or it.get("item_number", "") or ""
            if not _is_ssww and "ssww.com" in _link:
                _is_ssww = True
            if not _is_ssww and ("S&S" in _desc or "S & S" in _desc):
                _is_ssww = True
            # Auto-detect S&S from MFG# pattern via SKU resolver
            if not _is_ssww and _mfg:
                _resolved = resolve_sku_url(_mfg)
                if _resolved.get("supplier") == "S&S Worldwide":
                    _is_ssww = True
                    _sup_skus["ssww"] = _mfg.upper()
            if not _is_ssww:
                continue
            # Skip if already has pricing
            if it["pricing"].get("unit_cost") and float(it["pricing"].get("unit_cost", 0)) > 0:
                continue

            _ssww_sku = _sup_skus.get("ssww", "") or _mfg
            _priced = False

            # Step A: Check catalog for existing S&S cross-ref
            if _ssww_sku:
                try:
                    existing = find_by_supplier_sku(_ssww_sku, "S&S")
                    if existing:
                        cat = existing[0]
                        it["pricing"]["catalog_match"] = cat.get("name", "")
                        it["pricing"]["catalog_cost"] = cat.get("cost") or cat.get("best_cost", 0)
                        it["pricing"]["catalog_confidence"] = 0.95
                        it["pricing"]["catalog_product_id"] = cat.get("id")
                        if it["pricing"]["catalog_cost"]:
                            it["pricing"]["unit_cost"] = it["pricing"]["catalog_cost"]
                            it["pricing"]["price_source"] = "ssww_catalog"
                            _priced = True
                        counters.setdefault("ssww_catalog_hit", 0)
                        counters["ssww_catalog_hit"] += 1
                except Exception:
                    pass

            # Step B: Build S&S URL and do direct lookup for current pricing
            if not _priced and _ssww_sku:
                try:
                    _ssww_url = f"https://www.ssww.com/item/{_ssww_sku}/"
                    from src.agents.item_link_lookup import _lookup_ssww
                    _ssww_result = _lookup_ssww(_ssww_url)
                    if _ssww_result.get("ok") and _ssww_result.get("price", 0) > 0:
                        _price = _ssww_result["price"]
                        it["pricing"]["unit_cost"] = _price
                        it["pricing"]["price_source"] = "ssww_direct"
                        it["pricing"]["web_price"] = _price
                        it["pricing"]["web_source"] = "S&S Worldwide"
                        it["item_link"] = _ssww_result.get("url") or _ssww_url
                        it["item_supplier"] = "S&S Worldwide"
                        # MSRP + sale price
                        if _ssww_result.get("list_price"):
                            it["pricing"]["list_price"] = _ssww_result["list_price"]
                            it["list_price"] = _ssww_result["list_price"]
                        if _ssww_result.get("sale_price"):
                            it["pricing"]["sale_price"] = _ssww_result["sale_price"]
                            it["sale_price"] = _ssww_result["sale_price"]
                        if _ssww_result.get("title"):
                            it["pricing"]["amazon_title"] = _ssww_result["title"][:200]
                        if _ssww_result.get("mfg_number") and not it.get("mfg_number"):
                            it["mfg_number"] = _ssww_result["mfg_number"]
                        if _ssww_result.get("shipping_note"):
                            it["pricing"]["shipping_note"] = _ssww_result["shipping_note"]
                        _priced = True
                        counters.setdefault("ssww_direct_priced", 0)
                        counters["ssww_direct_priced"] += 1
                        log.info("ENRICH %s: S&S %s → $%.2f (list=$%s sale=$%s)",
                                 pc_id, _ssww_sku, _price,
                                 _ssww_result.get("list_price", "?"),
                                 _ssww_result.get("sale_price", "?"))
                    elif not _ssww_result.get("price"):
                        # S&S blocked — still set the URL for manual lookup
                        it["item_link"] = _ssww_url
                        it["item_supplier"] = "S&S Worldwide"
                except Exception as e:
                    log.debug("ENRICH %s: S&S direct lookup error item %d: %s", pc_id, i+1, e)

            # Step C: Amazon fallback only if S&S direct failed
            if not _priced and _desc and len(_desc) >= 8:
                try:
                    from src.agents.product_research import search_amazon, lookup_amazon_product
                    results = search_amazon(_desc[:100], max_results=1)
                    if results and results[0].get("price", 0) > 0:
                        r = results[0]
                        _amz_asin = r.get("asin", "")
                        it["pricing"]["amazon_price"] = r["price"]
                        it["pricing"]["amazon_asin"] = _amz_asin
                        it["pricing"]["amazon_url"] = r.get("url", "")
                        it["pricing"]["amazon_title"] = r.get("title", "")[:200]
                        # Keep S&S URL if set, Amazon as reference only
                        if not it.get("item_link"):
                            it["item_link"] = r.get("url", "")
                            it["item_supplier"] = "Amazon"
                        if _ssww_sku:
                            it["pricing"]["ssww_sku"] = _ssww_sku
                            it["pricing"]["source_note"] = f"S&S #{_ssww_sku} → Amazon ref"
                        # ASIN lookup for list/sale split
                        if _amz_asin:
                            try:
                                _prod = lookup_amazon_product(_amz_asin)
                                if _prod and _prod.get("list_price"):
                                    it["pricing"]["list_price"] = _prod["list_price"]
                                    it["list_price"] = _prod["list_price"]
                                if _prod and _prod.get("sale_price"):
                                    it["pricing"]["sale_price"] = _prod["sale_price"]
                                    it["sale_price"] = _prod["sale_price"]
                            except Exception:
                                pass
                        counters.setdefault("ssww_amazon_resolved", 0)
                        counters["ssww_amazon_resolved"] += 1
                        log.info("ENRICH %s: S&S item %d → Amazon %s $%.2f",
                                 pc_id, i+1, _amz_asin, r["price"])
                except Exception as e:
                    log.debug("ENRICH %s: S&S→Amazon error item %d: %s", pc_id, i+1, e)

            if _priced or it.get("item_link"):
                _ssww_lookups += 1
                time.sleep(0.5)
    except ImportError:
        pass
    except Exception as e:
        log.debug("ENRICH %s: S&S resolution error: %s", pc_id, e)
    _mark_step_done(pc_id, "ssww_resolution")

    # ── Step 2: Extract URLs from descriptions ───────────────────────────
    _update_status(pc_id, "url_extraction", "scanning descriptions")
    try:
        counters["urls_extracted"] = _extract_urls_from_items(items)
    except Exception as e:
        log.debug("ENRICH %s: URL extraction error: %s", pc_id, e)
    _mark_step_done(pc_id,"url_extraction")

    # ── Step 3: Catalog batch match ──────────────────────────────────────
    _update_status(pc_id, "catalog_match", f"matching {total} items")
    try:
        from src.agents.product_catalog import match_items_batch
        batch_input = [
            {"idx": i, "description": it.get("description", ""),
             "part_number": it.get("mfg_number", "") or it.get("part_number", ""),
             "upc": it.get("upc", "")}
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
                it["pricing"]["price_source"] = "catalog"
            cat_cost = r.get("best_cost") or r.get("last_cost", 0)
            _conf = r.get("confidence", 0)
            # Only auto-fill unit_cost from catalog if confidence >= 0.60
            # Low-confidence matches show as reference chips but don't set cost
            if cat_cost > 0 and not it["pricing"].get("unit_cost") and _conf >= 0.60:
                it["pricing"]["unit_cost"] = cat_cost
                if not it["pricing"].get("price_source"):
                    it["pricing"]["price_source"] = "catalog"
            if not it.get("mfg_number") and r.get("mfg_number"):
                it["mfg_number"] = r["mfg_number"]
            # Populate item_link from catalog supplier URL
            # For EXACT matches (>=0.95), catalog URL takes priority over Amazon
            _cat_url = r.get("supplier_url", "")
            _cat_sup = r.get("supplier_name", "")
            if _cat_url:
                _conf = r.get("confidence", 0)
                if not it.get("item_link") or _conf >= 0.95:
                    it["item_link"] = _cat_url
                    it["item_supplier"] = _cat_sup
                # Always store catalog URL for reference even if item_link is Amazon
                it["pricing"]["catalog_url"] = _cat_url
                it["pricing"]["catalog_best_supplier"] = _cat_sup
            counters["catalog_matched"] += 1
    except Exception as e:
        log.warning("ENRICH %s: catalog match error: %s", pc_id, e)
    _mark_step_done(pc_id,"catalog_match")

    # ── Step 3b: Ensure won_quotes KB is populated from SCPRS harvest ──
    try:
        from src.knowledge.won_quotes_db import sync_from_scprs_tables
        sync_from_scprs_tables()
    except Exception:
        pass

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
            _upc = it.get("upc", "")
            matches = find_similar_items(pn, desc, upc=_upc, max_results=1, min_confidence=0.30)
            if matches:
                best = matches[0]
                q = best.get("quote", best)
                scprs_price = q.get("unit_price", 0)
                scprs_qty = q.get("quantity", 1) or 1
                # Derive per-unit price if SCPRS stored line totals
                if scprs_qty > 1 and scprs_price > 0:
                    per_unit = round(scprs_price / scprs_qty, 2)
                else:
                    per_unit = scprs_price
                if scprs_price and scprs_price > 0:
                    it["pricing"]["scprs_price"] = per_unit
                    it["pricing"]["scprs_line_total"] = scprs_price
                    it["pricing"]["scprs_qty"] = scprs_qty
                    it["pricing"]["scprs_match"] = (q.get("description", "") or "")[:60]
                    it["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    it["pricing"]["scprs_source"] = "scprs_kb"
                    it["pricing"]["scprs_po"] = q.get("po_number", "")
                    # Propagate part number from SCPRS
                    scprs_pn = q.get("item_number", "")
                    if scprs_pn and not it.get("mfg_number"):
                        it["mfg_number"] = scprs_pn
                    # ONLY use as cost if reasonable (< $5,000 per unit)
                    # SCPRS prices can be line totals, not per-unit
                    if not it["pricing"].get("unit_cost") and per_unit < 5000:
                        it["pricing"]["unit_cost"] = per_unit
                    counters["scprs_matched"] += 1
            _update_status(pc_id, "scprs_lookup", f"{i+1}/{total} items")
    except Exception as e:
        log.warning("ENRICH %s: SCPRS lookup error: %s", pc_id, e)
    _mark_step_done(pc_id,"scprs_lookup")

    # ── Step 4b: Grok first-pass for total unknowns ─────────────────────
    # Items with NO catalog match AND NO SCPRS match → go straight to Grok.
    # This is BEFORE web search — Grok with web search is faster and smarter
    # than blind URL scraping for unidentified products.
    _update_status(pc_id, "llm_first_pass", "identifying unknowns via AI")
    _LLM_FIRST_LIMIT = 5
    try:
        from src.agents.product_validator import validate_product
        from src.agents.product_research import lookup_amazon_product
        from src.agents.product_catalog import (
            add_to_catalog, enrich_catalog_product, add_supplier_price, match_item as _cat_match
        )
        _fp_calls = 0
        for i, it in enumerate(items):
            if _fp_calls >= _LLM_FIRST_LIMIT:
                break
            p = it.get("pricing", {})
            # Only for TRUE unknowns: no cost, no catalog match, no SCPRS match
            if p.get("unit_cost") or p.get("catalog_match") or p.get("scprs_price"):
                continue
            if p.get("amazon_price"):
                continue  # already found via UPC/S&S resolution
            desc = it.get("description", "")
            if not desc or len(desc) < 5:
                continue
            try:
                result = validate_product(
                    description=desc,
                    upc=it.get("upc", ""),
                    mfg_number=it.get("mfg_number", ""),
                    qty=it.get("qty", 1),
                    uom=it.get("uom", "EA"),
                    qty_per_uom=it.get("qty_per_uom", 1),
                )
                if result.get("ok") and result.get("price", 0) > 0 and result.get("confidence", 0) >= 0.70:
                    _price = result["price"]
                    _asin = result.get("asin", "")
                    _url = result.get("url", "")
                    _prod_name = result.get("product_name", "")[:200]
                    # Apply to item
                    p["unit_cost"] = _price
                    it["vendor_cost"] = _price
                    p["price_source"] = "llm_grok_first"
                    p["llm_validated"] = True
                    p["llm_confidence"] = result["confidence"]
                    p["llm_reasoning"] = result.get("reasoning", "")[:200]
                    p["llm_product_name"] = _prod_name
                    if _asin:
                        p["amazon_asin"] = _asin
                        p["amazon_price"] = _price
                        p["amazon_url"] = _url
                        p["amazon_title"] = _prod_name
                    if _url and not it.get("item_link"):
                        it["item_link"] = _url
                        it["item_supplier"] = result.get("supplier", "Amazon")
                    # ASIN lookup for list/sale split + image
                    if _asin:
                        try:
                            _prod = lookup_amazon_product(_asin)
                            if _prod:
                                if _prod.get("list_price"):
                                    p["list_price"] = _prod["list_price"]
                                    it["list_price"] = _prod["list_price"]
                                if _prod.get("sale_price"):
                                    p["sale_price"] = _prod["sale_price"]
                                    it["sale_price"] = _prod["sale_price"]
                        except Exception:
                            pass
                    # Catalog write-back (flywheel)
                    try:
                        _pn = it.get("mfg_number", "")
                        _upc_fp = it.get("upc", "")
                        _existing = _cat_match(_prod_name or desc, _pn, top_n=1, upc=_upc_fp)
                        if _existing and _existing[0].get("match_confidence", 0) >= 0.80:
                            _pid = _existing[0]["id"]
                        else:
                            _pid = add_to_catalog(
                                description=_prod_name or desc, part_number=_pn,
                                cost=_price, supplier_url=_url,
                                supplier_name=result.get("supplier", ""),
                                mfg_number=_pn, source="grok_first_pass"
                            )
                        if _pid:
                            enrich_catalog_product(
                                _pid, upc=_upc_fp, asin=_asin, mfg_number=_pn,
                                best_cost=_price,
                                photo_url=p.get("photo_url", ""),
                                supplier_name=result.get("supplier", "Amazon"),
                                supplier_sku=_asin or _pn, supplier_url=_url,
                                supplier_price=_price,
                                amazon_price=_price if _asin else 0,
                            )
                            p["catalog_product_id"] = _pid
                    except Exception:
                        pass
                    counters.setdefault("llm_first_pass", 0)
                    counters["llm_first_pass"] += 1
                    log.info("ENRICH %s: Grok first-pass item %d → %s $%.2f",
                             pc_id, i+1, _prod_name[:40], _price)
                _fp_calls += 1
                time.sleep(0.5)
            except Exception as e:
                log.debug("ENRICH %s: Grok first-pass error item %d: %s", pc_id, i+1, e)
    except ImportError:
        log.debug("product_validator not available — skipping Grok first pass")
    except Exception as e:
        log.debug("ENRICH %s: Grok first-pass error: %s", pc_id, e)
    _mark_step_done(pc_id, "llm_first_pass")

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
                    # Store list/sale price for "if discount holds" calculator
                    if result.get("list_price"):
                        it["pricing"]["list_price"] = result["list_price"]
                        it["list_price"] = result["list_price"]
                    if result.get("sale_price"):
                        it["pricing"]["sale_price"] = result["sale_price"]
                        it["sale_price"] = result["sale_price"]
                    # Store Amazon-specific fields if from Amazon
                    if result.get("asin"):
                        it["pricing"]["amazon_asin"] = result["asin"]
                        it["pricing"]["amazon_price"] = result["price"]
                        it["pricing"]["amazon_url"] = result.get("url", "")
                        it["pricing"]["amazon_title"] = result.get("title", "")[:200]
                    counters["web_prices_found"] += 1
                    web_count += 1
                time.sleep(1.0)  # Rate limit between lookups
            except Exception as e:
                log.debug("ENRICH %s: web lookup error for %s: %s", pc_id, url[:50], e)
    except ImportError:
        log.debug("item_link_lookup not available")
    except Exception as e:
        log.warning("ENRICH %s: web lookup error: %s", pc_id, e)
    _mark_step_done(pc_id,"web_lookup")

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
                    _found_title = result.get("title", "")
                    _sem_ok = True  # default: trust the result

                    # Semantic validation: is the found product the right one?
                    if _found_title and desc:
                        try:
                            from src.agents.item_link_lookup import claude_semantic_match
                            _sem = claude_semantic_match(desc, _found_title, result["price"])
                            if _sem.get("ok") and _sem.get("confidence", 1) < 0.60:
                                _sem_ok = False
                                log.info("ENRICH %s: web search '%s' rejected by semantic match (%.0f%%)",
                                         pc_id, _found_title[:40], _sem.get("confidence", 0) * 100)
                                it["pricing"]["web_suggestion"] = _found_title[:200]
                                it["pricing"]["web_suggestion_price"] = result["price"]
                                it["pricing"]["web_suggestion_url"] = result.get("url", "")
                                it["pricing"]["web_suggestion_confidence"] = _sem.get("confidence", 0)
                        except Exception:
                            pass  # Claude unavailable — trust the result

                    if _sem_ok:
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
    _mark_step_done(pc_id,"web_search")

    # ── Step 5c: LLM product validator for low-confidence items ──────────
    # Uses Grok (xAI) with web search to validate/correct matches.
    # Only fires for items that are unpriced OR have low-confidence matches.
    _update_status(pc_id, "llm_validation", "validating products via AI")
    _LLM_LIMIT = 5  # max Grok calls per PC
    try:
        from src.agents.product_validator import validate_product
        from src.agents.product_research import lookup_amazon_product
        _llm_calls = 0
        for i, it in enumerate(items):
            if _llm_calls >= _LLM_LIMIT:
                break
            p = it.get("pricing", {})
            _has_cost = bool(p.get("unit_cost") and float(p.get("unit_cost", 0)) > 0)
            _cat_conf = float(p.get("catalog_confidence", 0))
            _scprs_conf = float(p.get("scprs_confidence", 0))
            _best_conf = max(_cat_conf, _scprs_conf)
            # Skip items already well-matched
            if _has_cost and _best_conf >= 0.75:
                continue
            # Skip items with no description
            desc = it.get("description", "")
            if not desc or len(desc) < 5:
                continue
            # Build best match info for context
            _bm_title = p.get("catalog_match") or p.get("amazon_title") or ""
            _bm_price = float(p.get("unit_cost") or p.get("catalog_cost") or p.get("amazon_price") or 0)
            _bm_source = p.get("price_source") or ("catalog" if _cat_conf else "none")
            try:
                result = validate_product(
                    description=desc,
                    upc=it.get("upc", ""),
                    mfg_number=it.get("mfg_number", ""),
                    qty=it.get("qty", 1),
                    uom=it.get("uom", "EA"),
                    qty_per_uom=it.get("qty_per_uom", 1),
                    best_match_title=_bm_title,
                    best_match_price=_bm_price,
                    best_match_confidence=_best_conf,
                    best_match_source=_bm_source,
                )
                if result.get("ok") and result.get("price", 0) > 0:
                    _conf = result.get("confidence", 0)
                    # Apply LLM result if confidence is reasonable
                    if _conf >= 0.70:
                        _price = result["price"]
                        _asin = result.get("asin", "")
                        _url = result.get("url", "")
                        # Set cost if not already set or LLM is more confident
                        if not _has_cost or _conf > _best_conf:
                            p["unit_cost"] = _price
                            it["vendor_cost"] = _price
                            p["price_source"] = "llm_grok"
                        # Set Amazon data if found
                        if _asin:
                            p["amazon_asin"] = _asin
                            p["amazon_price"] = _price
                            p["amazon_url"] = _url
                            p["amazon_title"] = result.get("product_name", "")[:200]
                        # Set item link if empty or LLM found better
                        if _url and (not it.get("item_link") or _conf > _best_conf):
                            it["item_link"] = _url
                            it["item_supplier"] = result.get("supplier", "Amazon")
                        # ASIN product lookup for list/sale price split
                        if _asin and not p.get("list_price"):
                            try:
                                _prod = lookup_amazon_product(_asin)
                                if _prod and _prod.get("list_price"):
                                    p["list_price"] = _prod["list_price"]
                                    it["list_price"] = _prod["list_price"]
                                if _prod and _prod.get("sale_price"):
                                    p["sale_price"] = _prod["sale_price"]
                                    it["sale_price"] = _prod["sale_price"]
                            except Exception:
                                pass
                        # Store LLM metadata
                        p["llm_validated"] = True
                        p["llm_confidence"] = _conf
                        p["llm_reasoning"] = result.get("reasoning", "")[:200]
                        p["llm_product_name"] = result.get("product_name", "")[:200]
                        counters.setdefault("llm_validated", 0)
                        counters["llm_validated"] += 1
                        # ── CATALOG WRITE-BACK: enrich catalog from Grok result ──
                        # Every Grok validation is an investment in future automation
                        try:
                            from src.agents.product_catalog import (
                                add_to_catalog, enrich_catalog_product, add_supplier_price, match_item
                            )
                            _prod_name = result.get("product_name", desc)[:200]
                            _pn = it.get("mfg_number", "")
                            _upc_wb = it.get("upc", "")
                            # Find or create catalog entry
                            _existing = match_item(_prod_name, _pn, top_n=1, upc=_upc_wb)
                            if _existing and _existing[0].get("match_confidence", 0) >= 0.80:
                                _pid = _existing[0]["id"]
                            else:
                                _pid = add_to_catalog(
                                    description=_prod_name, part_number=_pn,
                                    cost=_price, supplier_url=_url,
                                    supplier_name=result.get("supplier", ""),
                                    mfg_number=_pn, source="grok_validation"
                                )
                            if _pid:
                                enrich_catalog_product(
                                    _pid,
                                    upc=_upc_wb,
                                    asin=_asin,
                                    mfg_number=_pn,
                                    manufacturer=it.get("pricing", {}).get("manufacturer", ""),
                                    best_cost=_price,
                                    photo_url=p.get("photo_url", ""),
                                    supplier_name=result.get("supplier", "Amazon"),
                                    supplier_sku=_asin or _pn,
                                    supplier_url=_url,
                                    supplier_price=_price,
                                    amazon_price=_price if _asin else 0,
                                )
                                p["catalog_product_id"] = _pid
                                log.info("ENRICH %s: Grok → catalog #%s enriched (%s)",
                                         pc_id, _pid, _prod_name[:40])
                        except Exception as _wb_e:
                            log.debug("ENRICH %s: catalog write-back error: %s", pc_id, _wb_e)
                    else:
                        # Low LLM confidence — store as reference only
                        p["llm_suggestion"] = result.get("product_name", "")[:200]
                        p["llm_suggestion_price"] = result["price"]
                        p["llm_suggestion_url"] = result.get("url", "")
                _llm_calls += 1
                _update_status(pc_id, "llm_validation", f"{_llm_calls}/{_LLM_LIMIT} validated")
                time.sleep(0.5)  # rate limit
            except Exception as e:
                log.debug("ENRICH %s: LLM validation error item %d: %s", pc_id, i+1, e)
    except ImportError:
        log.debug("product_validator not available — skipping LLM step")
    except Exception as e:
        log.debug("ENRICH %s: LLM validation error: %s", pc_id, e)
    _mark_step_done(pc_id, "llm_validation")

    # ── Step 6: Pricing oracle recommendations (with per-item timeout) ───
    _update_status(pc_id, "pricing_oracle", f"0/{total} items")
    try:
        from src.knowledge.pricing_oracle import recommend_price
        import signal
        _ORACLE_TIMEOUT_SEC = 10  # Max 10s per item

        for i, it in enumerate(items):
            # Skip items already priced by user or earlier steps
            if it.get("unit_price") and it["unit_price"] > 0:
                _update_status(pc_id, "pricing_oracle", f"{i+1}/{total} items")
                continue
            if it["pricing"].get("recommended_price"):
                _update_status(pc_id, "pricing_oracle", f"{i+1}/{total} items")
                continue
            desc = it.get("description", "")
            pn = it.get("mfg_number", "") or it.get("part_number", "")
            cost = it["pricing"].get("unit_cost") or it["pricing"].get("catalog_cost") or 0
            scprs = it["pricing"].get("scprs_price") or 0
            try:
                import threading
                _oracle_result = [None]
                def _run_oracle():
                    _oracle_result[0] = recommend_price(
                        pn, desc,
                        supplier_cost=cost if cost > 0 else None,
                        scprs_price=scprs if scprs > 0 else None,
                        agency=institution,
                    )
                t = threading.Thread(target=_run_oracle, daemon=True)
                t.start()
                t.join(timeout=_ORACLE_TIMEOUT_SEC)
                rec = _oracle_result[0]
                if t.is_alive():
                    log.warning("ENRICH %s: pricing oracle TIMEOUT on item %d/%d (%s)",
                                pc_id, i+1, total, desc[:40])
                    rec = None
            except Exception as oe:
                log.debug("Oracle item %d: %s", i, oe)
                rec = None
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
    except ImportError:
        log.debug("pricing_oracle not available")
    except Exception as e:
        log.warning("ENRICH %s: pricing oracle error: %s", pc_id, e)
    _mark_step_done(pc_id,"pricing_oracle")

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
                qty_per_uom=it.get("qty_per_uom", 1),
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
    _mark_step_done(pc_id,"oracle_v2")

    # ── Record oracle recommendations for accuracy tracking ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for i, it in enumerate(items):
                oracle_price = it.get("oracle_price", 0)
                if oracle_price and oracle_price > 0:
                    conn.execute("""
                        INSERT OR REPLACE INTO recommendation_audit
                        (recorded_at, pc_id, item_index, description, item_number,
                         oracle_price, oracle_source, oracle_confidence, outcome)
                        VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, 'pending')
                    """, (pc_id, i, it.get("description", "")[:200],
                          it.get("mfg_number", "") or it.get("item_number", ""),
                          oracle_price,
                          it["pricing"].get("price_source", "oracle_v2"),
                          it.get("oracle_confidence", "")))
    except Exception as _ra_e:
        log.debug("ENRICH %s: recommendation audit: %s", pc_id, _ra_e)

    # ── Step 8: Price trend detection (flag items with falling/rising prices) ─
    _update_status(pc_id, "trends", "analyzing price trends")
    trend_alerts = []
    try:
        from src.knowledge.won_quotes_db import get_price_history
        for it in items[:15]:  # Limit to avoid slow lookups
            desc = it.get("description", "")
            pn = it.get("mfg_number", "") or it.get("part_number", "")
            if not desc:
                continue
            # Only check items that have SCPRS pricing data
            if not it["pricing"].get("scprs_price"):
                continue
            try:
                history = get_price_history(pn, desc, months=6)
                if history.get("trend") in ("rising", "falling") and history.get("matches", 0) >= 3:
                    it["pricing"]["price_trend"] = history["trend"]
                    it["pricing"]["trend_data"] = {
                        "avg": history.get("avg_price"),
                        "recent_avg": history.get("recent_avg"),
                        "min": history.get("min_price"),
                        "max": history.get("max_price"),
                        "matches": history.get("matches"),
                    }
                    if history["trend"] == "falling":
                        trend_alerts.append(f"{desc[:40]}: prices FALLING (avg ${history.get('avg_price', 0):.2f} → recent ${history.get('recent_avg', 0):.2f})")
                    elif history["trend"] == "rising":
                        trend_alerts.append(f"{desc[:40]}: prices RISING (avg ${history.get('avg_price', 0):.2f} → recent ${history.get('recent_avg', 0):.2f})")
            except Exception:
                pass
    except ImportError:
        pass
    except Exception as e:
        log.debug("ENRICH %s: trend detection error: %s", pc_id, e)
    if trend_alerts:
        log.info("ENRICH %s: %d price trend alerts", pc_id, len(trend_alerts))
    _mark_step_done(pc_id,"trends")

    # ── Save enriched PC ─────────────────────────────────────────────────
    _update_status(pc_id, "saving", "persisting results")
    pc["items"] = items
    if "parsed" in pc and pc["parsed"].get("line_items"):
        pc["parsed"]["line_items"] = items
    pc["enrichment_status"] = "complete"
    pc["enrichment_at"] = datetime.now().isoformat()
    counters["trend_alerts"] = len(trend_alerts)
    pc["enrichment_summary"] = counters
    if trend_alerts:
        pc["trend_alerts"] = trend_alerts
    pc["auto_priced"] = True
    pc["auto_priced_count"] = counters["catalog_matched"] + counters["scprs_matched"] + counters["oracle_priced"]
    pc["auto_priced_at"] = datetime.now().isoformat()
    if pc.get("status") == "parsed":
        pc["status"] = "priced"
    _save_single_pc(pc_id, pc)

    # Update live status
    with _LOCK:
        ENRICHMENT_STATUS[pc_id] = {
            "running": False,
            "completed": datetime.now().isoformat(),
            "summary": counters,
            "error": None,
            "_ts": time.time(),
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
    """Update the live polling status dict (lock-protected)."""
    with _LOCK:
        if pc_id in ENRICHMENT_STATUS:
            ENRICHMENT_STATUS[pc_id]["phase"] = phase
            ENRICHMENT_STATUS[pc_id]["progress"] = progress


def _mark_step_done(pc_id: str, step: str):
    """Record a completed pipeline step (lock-protected)."""
    with _LOCK:
        entry = ENRICHMENT_STATUS.get(pc_id)
        if entry and "steps_done" in entry:
            entry["steps_done"].append(step)


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
