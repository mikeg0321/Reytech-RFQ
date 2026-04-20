# routes_rfq_gen.py — Generation, Screenshot Parser, Templates, Send-to-Buyer
# Split from routes_rfq.py — loaded via importlib in dashboard.py

from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route, safe_page
from src.core.security import rate_limit
from flask import redirect, flash, send_file, session
from src.core.paths import DATA_DIR, OUTPUT_DIR, UPLOAD_DIR
from src.core.db import get_db
from src.api.render import render_page
import os, json, re as _re_mod
from datetime import datetime, timedelta, timezone


# ═══════════════════════════════════════════════════════════════════════
# Screenshot URL Parser — parse screenshot → dedup → scrape → enrich
# ═══════════════════════════════════════════════════════════════════════

def _description_similarity(a: str, b: str) -> float:
    """Jaccard similarity of word tokens for dedup."""
    def _tokens(s):
        return set(_re_mod.sub(r'[^a-z0-9\s]', '', s.lower()).split())
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _is_duplicate_item(new_item: dict, existing_items: list) -> tuple:
    """Check if new_item duplicates an existing RFQ item.
    Returns (is_dup: bool, reason: str or None)."""
    new_url = (new_item.get("item_link") or "").strip().lower()
    new_desc = (new_item.get("description") or "").strip()

    for idx, ex in enumerate(existing_items):
        # URL match
        ex_url = (ex.get("item_link") or "").strip().lower()
        if new_url and ex_url and new_url == ex_url:
            return (True, f"URL match: item #{idx + 1}")
        # Description similarity
        ex_desc = (ex.get("description") or "").strip()
        if new_desc and ex_desc:
            sim = _description_similarity(new_desc, ex_desc)
            if sim >= 0.6:
                return (True, f"Similar to item #{idx + 1} ({int(sim * 100)}% match)")
    return (False, None)


def _enrich_description_with_asin(description: str, asin: str) -> str:
    """Append ASIN to description if not already present."""
    if not asin:
        return description
    if asin in description:
        return description
    return f"{description} (ASIN: {asin})"


@bp.route("/api/rfq/<rid>/parse-screenshot", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_rfq_parse_screenshot(rid):
    """Upload a screenshot → parse URLs + descriptions → dedup → scrape → enrich → preview."""
    import os, tempfile
    bad = _validate_rid(rid)
    if bad:
        return bad

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    ext = os.path.splitext(f.filename)[1].lower()
    allowed = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif"}
    if ext not in allowed:
        return jsonify({"ok": False, "error": f"Unsupported format: {ext}. Use PNG, JPG, etc."}), 400

    # Save to temp file
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext,
                                          dir=str(UPLOAD_DIR) if UPLOAD_DIR.exists() else None)
        f.save(tmp)
        tmp_path = tmp.name
        tmp.close()
    except Exception as e:
        log.error("Screenshot save error: %s", e)
        return jsonify({"ok": False, "error": "Failed to save uploaded file"}), 500

    # Parse with vision (URL-focused mode)
    try:
        from src.forms.vision_parser import parse_with_vision
        result = parse_with_vision(tmp_path, mode="screenshot_urls")
    except Exception as e:
        log.error("Vision parse error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": f"Vision parse failed: {str(e)[:80]}"}), 500
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as _e:
            log.debug("suppressed: %s", _e)

    if not result or not result.get("line_items"):
        return jsonify({"ok": False, "error": "No items extracted from screenshot"}), 400

    parsed_items = result["line_items"]
    existing_items = r.get("line_items", [])

    # Dedup + scrape + enrich each item
    preview = []
    for item in parsed_items:
        entry = {
            "description": item.get("description", ""),
            "item_link": item.get("item_link", ""),
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "each"),
            "part_number": item.get("part_number", ""),
            "mfg_number": "",
            "asin": "",
            "supplier": "",
            "price": None,
            "is_duplicate": False,
            "duplicate_reason": None,
            "scrape_status": "no_url",
            "scrape_error": None,
        }

        # Dedup check
        is_dup, dup_reason = _is_duplicate_item(item, existing_items)
        entry["is_duplicate"] = is_dup
        entry["duplicate_reason"] = dup_reason

        # Scrape URL if present
        url = (item.get("item_link") or "").strip()
        if url:
            try:
                from src.agents.item_link_lookup import lookup_from_url, detect_supplier
                res = lookup_from_url(url)
                if res.get("ok"):
                    entry["scrape_status"] = "ok"
                    entry["supplier"] = res.get("supplier") or detect_supplier(url)
                    entry["price"] = res.get("price") or res.get("list_price") or res.get("cost")
                    entry["mfg_number"] = res.get("mfg_number") or res.get("part_number") or ""
                    entry["asin"] = res.get("asin", "")
                    # Use scraped description if parsed one is too short
                    scraped_desc = res.get("title") or res.get("description") or ""
                    if scraped_desc and len(entry["description"]) < 10:
                        entry["description"] = scraped_desc
                    # Enrich description with ASIN
                    entry["description"] = _enrich_description_with_asin(
                        entry["description"], entry["asin"])
                    # Use MFG# from scrape if we don't have one
                    if not entry["part_number"] and entry["mfg_number"]:
                        entry["part_number"] = entry["mfg_number"]
                elif res.get("login_required"):
                    entry["scrape_status"] = "login_required"
                    entry["supplier"] = res.get("supplier", "")
                else:
                    entry["scrape_status"] = "no_price"
                    entry["supplier"] = res.get("supplier") or ""
            except Exception as e:
                log.error("Screenshot scrape error for %s: %s", url[:60], e, exc_info=True)
                entry["scrape_status"] = "error"
                entry["scrape_error"] = str(e)[:80]

        if entry["price"]:
            try:
                entry["price"] = round(float(entry["price"]), 2)
            except (ValueError, TypeError):
                entry["price"] = None

        preview.append(entry)

    duplicates_found = sum(1 for p in preview if p["is_duplicate"])
    scrape_failures = sum(1 for p in preview if p["scrape_status"] in ("error", "login_required"))

    return jsonify({
        "ok": True,
        "items": preview,
        "duplicates_found": duplicates_found,
        "scrape_failures": scrape_failures,
        "total": len(preview),
    })


@bp.route("/api/rfq/<rid>/screenshot-confirm", methods=["POST"])
@auth_required
@safe_route
def api_rfq_screenshot_confirm(rid):
    """Confirm and add screenshot-parsed items to RFQ line items."""
    bad = _validate_rid(rid)
    if bad:
        return bad

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    data = request.get_json(force=True, silent=True) or {}
    new_items = data.get("items", [])
    if not new_items:
        return jsonify({"ok": False, "error": "No items to add"})

    existing = r.get("line_items", [])
    added = 0
    for item in new_items:
        line_item = {
            "description": item.get("description", ""),
            "item_number": item.get("part_number") or item.get("mfg_number") or "",
            "qty": item.get("qty", 1),
            "uom": item.get("uom", "each"),
            "item_link": item.get("item_link", ""),
            "item_supplier": item.get("supplier", ""),
            "mfg_number": item.get("mfg_number", ""),
        }
        # Set pricing if available — never overwrite existing
        price = item.get("price")
        if price:
            try:
                cost = float(price)
                line_item["supplier_cost"] = cost
                line_item["cost_source"] = "Screenshot Scrape"
                line_item["cost_supplier_name"] = item.get("supplier", "")
                markup = r.get("default_markup") or 25
                try:
                    markup = float(markup)
                except (ValueError, TypeError):
                    markup = 25
                line_item["markup_pct"] = markup
                line_item["price_per_unit"] = round(cost * (1 + markup / 100), 2)
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)
        existing.append(line_item)
        added += 1

    r["line_items"] = existing
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    return jsonify({"ok": True, "added": added, "total_items": len(existing)})


@bp.route("/api/rfq/<rid>/autosave", methods=["POST"])
@auth_required
@safe_route
def api_rfq_autosave(rid):
    """AJAX auto-save: persist line item edits without page reload."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "not found"}), 404

    data = request.get_json(force=True, silent=True) or {}
    items_data = data.get("items", [])

    from src.core.validation import validate_rfq_item
    for update in items_data:
        idx = update.get("idx")
        if idx is None or idx >= len(r["line_items"]):
            continue
        item = r["line_items"][idx]
        errs = validate_rfq_item(update, item)
        if errs:
            log.warning("RFQ autosave %s item[%s]: %s", rid, idx, "; ".join(errs))
        # Auto-detect supplier from link
        if "item_link" in update and item.get("item_link"):
            try:
                from src.agents.item_link_lookup import detect_supplier
                item["item_supplier"] = detect_supplier(item["item_link"])
            except Exception as _e:
                log.debug('suppressed in api_rfq_autosave: %s', _e)

    # Save package form checklist if provided
    pkg_forms = data.get("package_forms")
    if pkg_forms is not None and isinstance(pkg_forms, dict):
        r["package_forms"] = pkg_forms

    # Save tax rate if provided
    tax_rate = data.get("tax_rate")
    if tax_rate is not None:
        from src.core.validation import validate_header_field
        v, _ = validate_header_field("tax_rate", tax_rate)
        r["tax_rate"] = v

    # Save shipping if provided
    if data.get("shipping_option") is not None:
        r["shipping_option"] = str(data["shipping_option"])[:20]
    if data.get("shipping_amount") is not None:
        try:
            r["shipping_amount"] = max(0, min(99999, float(data["shipping_amount"])))
        except (ValueError, TypeError) as _e:
            log.debug("suppressed: %s", _e)

    # Save delivery location if provided (belt-and-suspenders with saveField)
    if data.get("delivery_location"):
        r["delivery_location"] = str(data["delivery_location"])[:500]

    # Save quote notes — ALWAYS preserve, even if not in this request
    if "quote_notes" in data:
        from src.core.validation import validate_text
        _qn_val, _ = validate_text(data["quote_notes"], max_len=2000)
        r["quote_notes"] = _qn_val
    # Ensure quote_notes key exists (prevents None on first save)
    r.setdefault("quote_notes", "")

    # Save delivery_location — ensure it persists
    r.setdefault("delivery_location", "")

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    try:
        from src.core.dal import log_lifecycle_event
        _has_markup = any(u.get("markup_pct") for u in items_data)
        log_lifecycle_event("rfq", rid, "items_edited",
            f"Autosaved {len(r.get('line_items', []))} items" + (" (markup changed)" if _has_markup else ""),
            actor="user")
    except Exception as _e:
        log.debug('suppressed in api_rfq_autosave: %s', _e)

    # F11: Check guardrails on saved data
    guardrail_warnings = _check_guardrails(r.get("line_items", []))

    # F6: Record price audits for changed items
    try:
        from src.core.db import record_audit
        sol = r.get("solicitation_number", "")
        for update in items_data:
            idx = update.get("idx")
            if idx is None or idx >= len(r["line_items"]):
                continue
            item = r["line_items"][idx]
            desc = (item.get("description", "") or "")[:60]
            # Record cost changes
            if "supplier_cost" in update and update["supplier_cost"]:
                record_audit(
                    item_description=desc, field_changed="supplier_cost",
                    old_value=0, new_value=float(update["supplier_cost"]),
                    source="manual_edit", rfq_id=rid, actor="user"
                )
            # Record bid changes
            if "price_per_unit" in update and update["price_per_unit"]:
                record_audit(
                    item_description=desc, field_changed="price_per_unit",
                    old_value=0, new_value=float(update["price_per_unit"]),
                    source="manual_edit", rfq_id=rid, actor="user"
                )
    except Exception as _e:
        log.debug('suppressed in api_rfq_autosave: %s', _e)

    # Write priced items to catalog (same as full save does)
    try:
        from src.agents.product_catalog import add_to_catalog, init_catalog_db
        init_catalog_db()
        sol = r.get("solicitation_number", "")
        _cat_saved = 0
        for update in items_data:
            idx = update.get("idx")
            if idx is None or idx >= len(r["line_items"]):
                continue
            item = r["line_items"][idx]
            cost = item.get("supplier_cost") or 0
            bid = item.get("price_per_unit") or 0
            desc = item.get("description", "")
            if desc and (cost > 0 or bid > 0):
                try:
                    add_to_catalog(
                        description=desc,
                        part_number=item.get("item_number", ""),
                        cost=float(cost) if cost else 0,
                        sell_price=float(bid) if bid else 0,
                        source=f"rfq_autosave_{sol}",
                        supplier_name=item.get("item_supplier", ""),
                        supplier_url=item.get("item_link", ""),
                        photo_url=item.get("photo_url", ""),
                        manufacturer=item.get("manufacturer", ""),
                        mfg_number=item.get("mfg_number", ""),
                    )
                    _cat_saved += 1
                except Exception as _ce:
                    log.debug("Catalog save item %d: %s", idx, _ce)
        if _cat_saved:
            log.info("Autosave catalog: %d items saved for %s", _cat_saved, rid)
    except Exception as _ce:
        log.warning("Catalog autosave failed: %s", _ce)

    return jsonify({
        "ok": True, "saved": len(items_data),
        "guardrails": guardrail_warnings if guardrail_warnings else None,
    })


@bp.route("/rfq/<rid>/add-item", methods=["POST"])
@auth_required
@safe_route
def rfq_add_item(rid):
    """Add a line item to an RFQ (for generic/Cal Vet RFQs or manual entry)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    if "line_items" not in r:
        r["line_items"] = []

    next_num = len(r["line_items"]) + 1

    from src.core.validation import validate_qty, validate_short_text, validate_text
    _qty, _ = validate_qty(request.form.get("qty", 1))
    _uom, _ = validate_short_text(request.form.get("uom", "EA"), max_len=20, default="EA")
    _desc, _ = validate_text(request.form.get("description", ""), max_len=5000)
    _itemno, _ = validate_short_text(request.form.get("item_number", ""), max_len=100)
    new_item = {
        "line_number": next_num,
        "qty": _qty,
        "uom": _uom.upper(),
        "description": _desc.strip(),
        "item_number": _itemno,
        "supplier_cost": 0,
        "scprs_last_price": None,
        "source_type": "manual",
        "price_per_unit": 0,
    }

    r["line_items"].append(new_item)
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    _log_rfq_activity(rid, "item_added",
        f"Line item #{next_num} added: {new_item['description'][:60]}",
        actor="user")
    flash(f"Item #{next_num} added", "success")
    return redirect(f"/rfq/{rid}#add-item-section")


@bp.route("/rfq/<rid>/remove-item/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def rfq_remove_item(rid, idx):
    """Remove a line item from an RFQ by index."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    items = r.get("line_items") or r.get("items") or []
    # Ensure canonical key is set
    item_key = "line_items" if "line_items" in r else "items"
    if 0 <= idx < len(items):
        removed = items.pop(idx)
        _renumber_items(items)
        r[item_key] = items
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        _log_rfq_activity(rid, "item_removed",
            f"Line item removed: {removed.get('description','')[:60]}",
            actor="user")
        return _item_response(rid, True, "Item removed")
    return _item_response(rid, False, f"Invalid item index {idx} (have {len(items)} items)")


@bp.route("/rfq/<rid>/duplicate-item/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def rfq_duplicate_item(rid, idx):
    """Duplicate a line item (insert copy right after the original)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    items = r.get("line_items") or r.get("items") or []
    if 0 <= idx < len(items):
        import copy
        dupe = copy.deepcopy(items[idx])
        dupe.pop("_catalog_product_id", None)
        items.insert(idx + 1, dupe)
        _renumber_items(items)
        r["line_items"] = items
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        return _item_response(rid, True, f"Item duplicated at #{idx + 2}")
    return _item_response(rid, False, "Invalid item index")


@bp.route("/rfq/<rid>/move-item/<int:idx>/<direction>", methods=["POST"])
@auth_required
@safe_route
def rfq_move_item(rid, idx, direction):
    """Move a line item up or down."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    items = r.get("line_items") or r.get("items") or []
    if direction == "up" and idx > 0:
        items[idx], items[idx - 1] = items[idx - 1], items[idx]
    elif direction == "down" and idx < len(items) - 1:
        items[idx], items[idx + 1] = items[idx + 1], items[idx]
    else:
        return _item_response(rid, False, "Cannot move")

    _renumber_items(items)
    r["line_items"] = items
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    return _item_response(rid, True, f"Item moved {direction}")


@bp.route("/rfq/<rid>/reset-items", methods=["POST"])
@auth_required
@safe_route
def rfq_reset_items(rid):
    """Clear all line items from an RFQ so it can be re-imported."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    old_count = len(r.get("line_items") or r.get("items") or [])
    r["line_items"] = []
    r.pop("items", None)  # Remove legacy key to avoid confusion
    r.pop("linked_pc_id", None)
    r.pop("linked_pc_number", None)
    r.pop("linked_pc_match_reason", None)
    r.pop("uploaded_pc_pdf", None)
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    _log_rfq_activity(rid, "items_reset",
        f"All {old_count} line items cleared for re-import",
        actor="user")
    return _item_response(rid, True, f"Cleared {old_count} items")


@bp.route("/api/rfq/<rid>/unlink-pc", methods=["POST"])
@auth_required
@safe_route
def api_rfq_unlink_pc(rid):
    """Remove PC linkage from an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404
    r.pop("linked_pc_id", None)
    r.pop("linked_pc_number", None)
    r.pop("linked_pc_match_reason", None)
    r.pop("source_pc", None)
    r.pop("source_pc_number", None)
    r.pop("source_pc_status", None)
    r.pop("source_pc_requestor", None)
    if r.get("source") == "pc_conversion":
        r["source"] = "direct"
    # Remove PC badges from items
    for item in r.get("line_items", r.get("items", [])):
        item.pop("source_pc", None)
        item.pop("imported_from_pc", None)
        item.pop("_from_pc", None)
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    log.info("RFQ %s unlinked from PC", rid)
    return jsonify({"ok": True, "msg": "PC linkage removed"})


@bp.route("/rfq/<rid>/lookup-item/<int:idx>", methods=["POST"])
@auth_required
@safe_page
def rfq_lookup_single_item(rid, idx):
    """Run SCPRS + Catalog + Amazon lookup on a single line item by index."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    items = r.get("line_items", [])
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Invalid item index"})

    item = items[idx]
    desc = item.get("description", "")
    pn = item.get("item_number", "")
    results = {"scprs": None, "catalog": None, "amazon": None}

    # 1. SCPRS lookup
    try:
        from src.agents.scprs_lookup import bulk_lookup
        wrapped = bulk_lookup([item])
        if wrapped and wrapped[0].get("scprs_last_price"):
            items[idx] = wrapped[0]
            results["scprs"] = {
                "price": wrapped[0]["scprs_last_price"],
                "vendor": wrapped[0].get("scprs_vendor", ""),
                "source": wrapped[0].get("scprs_source", ""),
            }
    except Exception as e:
        results["scprs"] = {"error": str(e)[:80]}
        log.debug("Single item SCPRS error: %s", e)

    # 2. Catalog lookup
    try:
        from src.agents.product_catalog import match_item, init_catalog_db
        init_catalog_db()
        matches = match_item(desc, pn, top_n=3)
        if matches and matches[0].get("match_confidence", 0) >= 0.3:
            best = matches[0]
            items[idx]["catalog_match"] = best
            cost = best.get("best_cost") or best.get("cost") or 0
            results["catalog"] = {
                "name": best.get("name", "")[:60],
                "cost": cost,
                "supplier": best.get("best_supplier", ""),
                "confidence": round(best.get("match_confidence", 0), 2),
                "sell_price": best.get("sell_price") or best.get("recommended_price") or 0,
            }
            # Fill cost if empty
            if cost and not items[idx].get("supplier_cost"):
                items[idx]["supplier_cost"] = round(float(cost), 2)
                items[idx]["item_supplier"] = best.get("best_supplier", "")
    except Exception as e:
        results["catalog"] = {"error": str(e)[:80]}
        log.debug("Single item catalog error: %s", e)

    # 3. Amazon/web lookup
    try:
        from src.agents.web_price_research import research_items
        wrapped = research_items([items[idx]])
        if wrapped and wrapped[0].get("amazon_price"):
            items[idx] = wrapped[0]
            results["amazon"] = {
                "price": wrapped[0].get("amazon_price"),
                "url": wrapped[0].get("item_link", ""),
                "source": wrapped[0].get("item_supplier", ""),
            }
    except Exception as e:
        results["amazon"] = {"error": str(e)[:80]}
        log.debug("Single item Amazon error: %s", e)

    # 4. Unified Pricing Oracle
    try:
        from src.core.pricing_oracle_v2 import get_pricing
        oracle = get_pricing(
            description=desc,
            quantity=item.get("quantity", item.get("qty", 1)),
            cost=item.get("supplier_cost") or item.get("unit_price"),
            item_number=pn,
            qty_per_uom=item.get("qty_per_uom", 1),
        )
        items[idx]["oracle"] = oracle
        results["oracle"] = {
            "quote_price": oracle.get("recommendation", {}).get("quote_price"),
            "confidence": oracle.get("recommendation", {}).get("confidence"),
            "market_avg": oracle.get("market", {}).get("weighted_avg"),
            "competitors": len(oracle.get("competitors", [])),
            "cross_sell": len(oracle.get("cross_sell", [])),
            "sources": oracle.get("sources_used", []),
        }
    except Exception as e:
        results["oracle"] = {"error": str(e)[:80]}

    # 5. Loss Intelligence — competitive pricing feedback from past losses
    try:
        from src.agents.pricing_feedback import get_pricing_recommendation
        agency = r.get("agency", "")
        cost = item.get("supplier_cost") or item.get("unit_price") or 0
        loss_intel = get_pricing_recommendation(desc, agency, float(cost) if cost else 0)
        if loss_intel.get("sources_used", 0) > 0:
            results["loss_intelligence"] = {
                "competitor_floor": loss_intel.get("competitor_floor"),
                "suggested_range": loss_intel.get("suggested_range"),
                "margin_warning": loss_intel.get("margin_warning"),
                "confidence": loss_intel.get("confidence", 0),
                "loss_count": loss_intel.get("sources_used", 0),
            }
            items[idx]["loss_intel"] = results["loss_intelligence"]
    except Exception as e:
        log.debug("Loss intelligence lookup: %s", e)

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    # Build summary
    found = []
    if results["scprs"] and not results["scprs"].get("error"):
        found.append(f"SCPRS: ${results['scprs']['price']:.2f}")
    if results["catalog"] and not results["catalog"].get("error") and results["catalog"].get("cost"):
        found.append(f"Catalog: ${results['catalog']['cost']:.2f} ({results['catalog']['supplier']})")
    if results["amazon"] and not results["amazon"].get("error"):
        found.append(f"Amazon: ${results['amazon']['price']:.2f}")
    if results.get("oracle") and not results["oracle"].get("error") and results["oracle"].get("quote_price"):
        found.append(f"Oracle: ${results['oracle']['quote_price']:.2f} ({results['oracle'].get('confidence','?')})")
    if results.get("loss_intelligence") and results["loss_intelligence"].get("competitor_floor"):
        li = results["loss_intelligence"]
        found.append(f"Loss Intel: floor ${li['competitor_floor']:.2f} ({li['loss_count']} losses)")
        if li.get("margin_warning"):
            found.append(f"WARNING: {li['margin_warning'][:60]}")

    return jsonify({
        "ok": True,
        "idx": idx,
        "description": desc[:60],
        "results": results,
        "summary": " | ".join(found) if found else "No prices found",
    })


@bp.route("/rfq/<rid>/upload-supplier-quote", methods=["POST"])
@auth_required
@safe_page
@rate_limit("heavy")
def rfq_upload_supplier_quote(rid):
    """Upload a supplier quote PDF → parse → match to RFQ items → fill costs + update catalog."""
    _bad = _validate_rid(rid)
    if _bad: return _bad
    import os
    from src.core.paths import DATA_DIR

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded"})
    from src.forms.doc_converter import is_office_doc, ALL_UPLOAD_EXTS
    fname_lower = (f.filename or "").lower()
    is_pdf = fname_lower.endswith(".pdf")
    is_office = is_office_doc(fname_lower)
    if not is_pdf and not is_office:
        return jsonify({"ok": False, "error": "Upload a PDF or office document (XLS, XLSX, DOC, DOCX)"})

    # Save uploaded file
    upload_dir = os.path.join(DATA_DIR, "uploads", "supplier_quotes")
    os.makedirs(upload_dir, exist_ok=True)
    _safe_fn = _re_mod.sub(r'[^a-zA-Z0-9._-]', '_', os.path.basename(f.filename or 'upload.pdf'))
    save_path = os.path.join(upload_dir, f"sq_{rid}_{_safe_fn}")
    f.save(save_path)

    # Parse the quote — PDF uses supplier_quote_parser, office docs use AI text extraction
    try:
        from src.forms.supplier_quote_parser import parse_supplier_quote, match_quote_to_rfq
    except ImportError as e:
        return jsonify({"ok": False, "error": f"Parser not available: {e}"})

    if is_pdf:
        parsed = parse_supplier_quote(save_path)
    else:
        # Office doc: extract text → Claude AI extraction → convert to supplier quote format
        try:
            from src.forms.doc_converter import extract_text as _extr_text
            from src.forms.vision_parser import parse_from_text, is_available as _vis_avail
            doc_text = _extr_text(save_path)
            ai_items = None
            if _vis_avail():
                ai_parsed = parse_from_text(doc_text, source_path=save_path)
                if ai_parsed and ai_parsed.get("line_items"):
                    ai_items = ai_parsed["line_items"]
            # Regex fallback
            if not ai_items:
                from src.forms.doc_converter import parse_items_from_text
                ai_items = parse_items_from_text(doc_text)
            if ai_items:
                # Convert to supplier quote format
                sq_items = []
                for it in ai_items:
                    sq_items.append({
                        "item_number": it.get("part_number") or it.get("item_number", ""),
                        "description": it.get("description", ""),
                        "qty": it.get("qty", 1),
                        "uom": it.get("uom", "EA"),
                        "unit_price": float(it.get("price") or it.get("unit_price") or it.get("cost") or 0),
                        "line_number": int(it.get("item_number", 0)) if str(it.get("item_number", "")).isdigit() else 0,
                    })
                parsed = {
                    "ok": True, "supplier": "Unknown", "quote_number": "",
                    "quote_date": "", "items": sq_items, "raw_text": doc_text[:2000],
                    "total_pages": 1,
                }
            else:
                parsed = {"ok": False, "error": "Could not extract items from office document"}
        except ValueError as ve:
            parsed = {"ok": False, "error": str(ve)}
        except Exception as e:
            parsed = {"ok": False, "error": f"Office doc parse error: {e}"}

    pdf_path = save_path  # keep variable name for downstream code

    if not parsed.get("ok"):
        return jsonify({"ok": False, "error": parsed.get("error", "Parse failed"),
                        "raw_text": parsed.get("raw_text", "")[:500]})

    quote_items = parsed.get("items", [])
    if not quote_items:
        return jsonify({"ok": False, "error": "No priced items found in document",
                        "raw_text": parsed.get("raw_text", "")[:500]})

    supplier = parsed.get("supplier", "Unknown")
    quote_num = parsed.get("quote_number", "")

    # Match to RFQ line items
    rfq_items = r.get("line_items", [])
    
    # ── If RFQ has NO items, create them from supplier quote directly ──
    if not rfq_items and quote_items:
        log.info("RFQ has 0 items — creating %d items from supplier quote", len(quote_items))
        for qi, q in enumerate(quote_items):
            new_item = {
                "line_number": qi + 1,
                "qty": q.get("qty", 1),
                "uom": (q.get("uom") or "EA").upper(),
                "description": q.get("description", ""),
                "item_number": q.get("item_number", ""),
                "supplier_cost": round(q.get("unit_price", 0), 2),
                "cost_source": "Supplier Quote",
                "cost_supplier_name": supplier,
                "item_supplier": supplier,
                "price_per_unit": 0,
                "scprs_last_price": 0,
                "amazon_price": 0,
                "_desc_source": "supplier",
            }
            rfq_items.append(new_item)
        r["line_items"] = rfq_items
        applied = len(quote_items)
        desc_upgraded = len(quote_items)
        unmatched = []
        catalog_added = 0
        catalog_updated = 0
        
        # Catalog sync for all items
        try:
            from src.agents.product_catalog import match_item, add_to_catalog, add_supplier_price, init_catalog_db
            init_catalog_db()
            for q in quote_items:
                _desc = q.get("description", "")
                _pn = q.get("item_number", "")
                _cost = q.get("unit_price", 0)
                if not _desc or _cost <= 0:
                    continue
                cat_matches = match_item(_desc, _pn, top_n=1)
                if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                    pid = cat_matches[0]["id"]
                    add_supplier_price(pid, supplier, _cost)
                    catalog_updated += 1
                else:
                    pid = add_to_catalog(description=_desc, part_number=_pn, cost=_cost,
                                         supplier_name=supplier, uom=q.get("uom", "EA"),
                                         source=f"supplier_quote_{quote_num or 'upload'}")
                    if pid:
                        add_supplier_price(pid, supplier, _cost)
                        catalog_added += 1
        except Exception as _ce:
            log.debug("Catalog sync (new items): %s", _ce)
        
        # Skip the matching loop — go straight to save
        r["_last_supplier_quote"] = {
            "supplier": supplier, "quote_number": quote_num, "pdf": pdf_path,
            "items_parsed": len(quote_items), "items_matched": applied,
            "uploaded_at": __import__("datetime").datetime.now().isoformat(),
        }
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)

        try:
            from src.api.dashboard import save_rfq_file
            with open(pdf_path, "rb") as _qf:
                pdf_data = _qf.read()
            save_rfq_file(rid, f.filename, "application/pdf", pdf_data,
                         category="supplier_quote", uploaded_by="user")
        except Exception as _fe:
            log.warning("Failed to save supplier quote to DB: %s", _fe)
        
        _log_rfq_activity(rid, "supplier_quote_uploaded",
            f"Supplier quote from {supplier} ({quote_num or f.filename}): "
            f"CREATED {len(quote_items)} items (RFQ was empty), catalog +{catalog_added}/~{catalog_updated}",
            actor="user")
        
        try:
            from src.agents.drive_triggers import on_supplier_quote_uploaded
            on_supplier_quote_uploaded(r, pdf_path, supplier, quote_num)
        except Exception as _e:
            log.debug('suppressed in rfq_upload_supplier_quote: %s', _e)
        
        return jsonify({
            "ok": True, "supplier": supplier, "quote_number": quote_num,
            "items_parsed": len(quote_items), "items_matched": applied,
            "desc_upgraded": desc_upgraded,
            "unmatched": [], "catalog_added": catalog_added, "catalog_updated": catalog_updated,
            "reconciliation": [{
                "line": i+1, "supplier_desc": q.get("description","")[:60],
                "supplier_pn": q.get("item_number",""), "supplier_qty": q.get("qty",0),
                "supplier_uom": q.get("uom",""), "supplier_price": q.get("unit_price",0),
                "matched": True, "confidence": 1.0, "rfq_line": i+1,
                "qty_match": True, "uom_match": True,
            } for i, q in enumerate(quote_items)],
            "warnings": {"qty_mismatches": 0, "uom_mismatches": 0},
            "created_items": True,
        })

    # ── Diagnostic logging for remote debugging ──
    log.info("SUPPLIER QUOTE MATCH: %d quote items vs %d RFQ items", len(quote_items), len(rfq_items))
    for qi, q in enumerate(quote_items[:5]):
        log.info("  Q[%d] pn='%s' $%.2f desc='%s'", qi, q.get("item_number",""), q.get("unit_price",0), q.get("description","")[:60])
    for ri, r_item in enumerate(rfq_items[:5]):
        log.info("  R[%d] pn='%s' desc='%s'", ri, r_item.get("item_number",""), r_item.get("description","")[:60])
    
    matches = match_quote_to_rfq(quote_items, rfq_items)
    
    matched_count = sum(1 for m in matches if m.get("matched"))
    log.info("MATCH RESULT: %d/%d matched", matched_count, len(matches))
    for m in matches:
        if not m.get("matched"):
            log.info("  UNMATCHED: Q[%d] pn='%s' $%.2f conf=%.2f", m["quote_idx"], m["quote_pn"], m["unit_price"], m.get("confidence",0))

    # Apply matches: fill cost, pick best description, update catalog
    applied = 0
    unmatched = []
    catalog_added = 0
    catalog_updated = 0
    desc_upgraded = 0

    for m in matches:
        cost = m.get("unit_price", 0)
        q_desc = m.get("quote_desc", "")
        q_pn = m.get("quote_pn", "")
        q_qty = m.get("qty", 1)
        q_uom = m.get("uom", "EA")

        if m["matched"] and m["rfq_idx"] is not None and cost > 0:
            idx = m["rfq_idx"]
            if idx < len(rfq_items):
                item = rfq_items[idx]
                item["supplier_cost"] = round(cost, 2)
                item["cost_source"] = "Supplier Quote"
                item["cost_supplier_name"] = supplier
                item["item_supplier"] = supplier

                # ── Description: ALWAYS use supplier's when matched ──
                # Supplier is the source of truth for what they're selling.
                # Their descriptions include McKesson #, MFG #, pack sizes.
                if q_desc and len(q_desc) > 10:
                    item["description"] = q_desc
                    item["_desc_source"] = "supplier"
                    desc_upgraded += 1

                # Fill part number if empty or from a richer source
                if q_pn and not item.get("item_number"):
                    item["item_number"] = q_pn

                applied += 1

                # ── Catalog update: always save/enrich ──
                try:
                    from src.agents.product_catalog import (
                        match_item, add_to_catalog, add_supplier_price,
                        init_catalog_db
                    )
                    init_catalog_db()
                    pn_search = q_pn or item.get("item_number", "")
                    cat_matches = match_item(q_desc, pn_search, top_n=1)
                    
                    if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                        pid = cat_matches[0]["id"]
                        # Always update catalog description with supplier's (richer)
                        if q_desc and len(q_desc) > len(cat_matches[0].get("description", "") or ""):
                            try:
                                from src.agents.product_catalog import _get_conn, _tokenize
                                conn = _get_conn()
                                conn.execute(
                                    "UPDATE product_catalog SET description=?, search_tokens=?, updated_at=? WHERE id=?",
                                    (q_desc, _tokenize(q_desc),
                                     __import__('datetime').datetime.now().isoformat(), pid)
                                )
                                conn.commit()
                                conn.close()
                            except Exception as _e:
                                log.debug('suppressed in rfq_upload_supplier_quote: %s', _e)
                        add_supplier_price(pid, supplier, cost)
                        catalog_updated += 1
                    else:
                        pid = add_to_catalog(
                            description=q_desc,
                            part_number=pn_search,
                            cost=cost,
                            supplier_name=supplier,
                            uom=q_uom,
                            source=f"supplier_quote_{quote_num or 'upload'}",
                        )
                        if pid:
                            add_supplier_price(pid, supplier, cost)
                            catalog_added += 1
                except Exception as _ce:
                    log.debug("Catalog update from supplier quote: %s", _ce)

        else:
            unmatched.append({
                "description": q_desc,
                "part_number": q_pn,
                "unit_price": cost,
                "qty": q_qty,
            })
            # Still add unmatched items to catalog
            if cost > 0 and q_desc:
                try:
                    from src.agents.product_catalog import (
                        match_item, add_to_catalog, add_supplier_price,
                        init_catalog_db
                    )
                    init_catalog_db()
                    cat_matches = match_item(q_desc, q_pn, top_n=1)
                    if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                        pid = cat_matches[0]["id"]
                        add_supplier_price(pid, supplier, cost)
                        catalog_updated += 1
                    else:
                        pid = add_to_catalog(
                            description=q_desc,
                            part_number=q_pn,
                            cost=cost,
                            supplier_name=supplier,
                            source=f"supplier_quote_{quote_num or 'upload'}",
                        )
                        if pid:
                            add_supplier_price(pid, supplier, cost)
                            catalog_added += 1
                except Exception as _ce:
                    log.debug("Catalog update (unmatched): %s", _ce)

    # Auto-detect supplier from the PDF text/filename
    try:
        from src.agents.item_link_lookup import detect_supplier
        _sq_filename = f.filename or ""
        _sq_supplier = detect_supplier(_sq_filename) or ""
        if not _sq_supplier:
            # Try to detect from parsed text
            _sq_text = " ".join(str(v) for v in parsed.values() if isinstance(v, str))[:500]
            for _test_name in ["Henry Schein", "McKesson", "Medline", "Cardinal", "Grainger", "Amazon", "Bound Tree"]:
                if _test_name.lower() in _sq_text.lower():
                    _sq_supplier = _test_name
                    break
        if _sq_supplier:
            for item in r.get("line_items", []):
                if item.get("supplier_cost") and not item.get("item_supplier"):
                    item["item_supplier"] = _sq_supplier
                    item["cost_source"] = f"Supplier Quote ({_sq_supplier})"
    except Exception as _e:
        log.debug('suppressed in rfq_upload_supplier_quote: %s', _e)

    # Save RFQ
    r["_last_supplier_quote"] = {
        "supplier": supplier,
        "quote_number": quote_num,
        "pdf": pdf_path,
        "items_parsed": len(quote_items),
        "items_matched": applied,
        "uploaded_at": __import__("datetime").datetime.now().isoformat(),
    }
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    # Save supplier quote PDF to rfq_files DB (persists across deploys)
    try:
        from src.api.dashboard import save_rfq_file
        with open(pdf_path, "rb") as _qf:
            pdf_data = _qf.read()
        save_rfq_file(
            rid,
            f.filename,
            "application/pdf",
            pdf_data,
            category="supplier_quote",
            uploaded_by="user",
        )
        log.info("Supplier quote saved to DB: %s (%d bytes) for RFQ %s", f.filename, len(pdf_data), rid)
    except Exception as _fe:
        log.warning("Failed to save supplier quote to DB: %s", _fe)

    # Log activity
    sol = r.get("solicitation_number", rid)
    _log_rfq_activity(rid, "supplier_quote_uploaded",
        f"Supplier quote from {supplier} ({quote_num or f.filename}): "
        f"{len(quote_items)} parsed, {applied} matched, {desc_upgraded} desc upgraded, "
        f"catalog +{catalog_added}/~{catalog_updated}",
        actor="user")

    # ── Google Drive: archive supplier quote ──
    try:
        from src.agents.drive_triggers import on_supplier_quote_uploaded
        on_supplier_quote_uploaded(r, pdf_path, supplier, quote_num)
    except Exception as _gde:
        log.debug("Drive trigger (supplier_quote): %s", _gde)

    # ── Build reconciliation table for validation ──
    reconciliation = []
    for m in matches:
        recon = {
            "line": m.get("quote_idx", 0) + 1,
            "supplier_desc": (m.get("quote_desc") or "")[:60],
            "supplier_pn": m.get("quote_pn", ""),
            "supplier_qty": m.get("qty", 0),
            "supplier_uom": m.get("uom", ""),
            "supplier_price": m.get("unit_price", 0),
            "matched": m.get("matched", False),
            "confidence": m.get("confidence", 0),
        }
        if m["matched"] and m["rfq_idx"] is not None and m["rfq_idx"] < len(rfq_items):
            ri = rfq_items[m["rfq_idx"]]
            recon["rfq_line"] = m["rfq_idx"] + 1
            recon["rfq_qty"] = ri.get("qty", 0)
            recon["rfq_uom"] = (ri.get("uom") or "").upper()
            recon["qty_match"] = recon["supplier_qty"] == recon["rfq_qty"]
            recon["uom_match"] = recon["supplier_uom"].upper() == recon["rfq_uom"]
        reconciliation.append(recon)

    # Validation flags
    qty_mismatches = [r for r in reconciliation if r.get("matched") and not r.get("qty_match", True)]
    uom_mismatches = [r for r in reconciliation if r.get("matched") and not r.get("uom_match", True)]

    return jsonify({
        "ok": True,
        "supplier": supplier,
        "quote_number": quote_num,
        "items_parsed": len(quote_items),
        "items_matched": applied,
        "desc_upgraded": desc_upgraded,
        "unmatched": unmatched,
        "catalog_added": catalog_added,
        "catalog_updated": catalog_updated,
        "reconciliation": reconciliation,
        "warnings": {
            "qty_mismatches": len(qty_mismatches),
            "uom_mismatches": len(uom_mismatches),
        },
    })


def _renumber_items(items):
    """Re-number line items sequentially."""
    for i, it in enumerate(items):
        it["line_number"] = i + 1


def _item_response(rid, ok, msg):
    """Return JSON for AJAX or redirect for form POST."""
    if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": ok, "message": msg})
    from flask import flash as _flash
    _flash(msg, "success" if ok else "error")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/upload-templates", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def upload_templates(rid):
    """Upload 703B/704B/Bid Package template PDFs for an RFQ."""
    _bad = _validate_rid(rid)
    if _bad: return _bad
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    files = request.files.getlist("templates")
    if not files:
        flash("No files uploaded", "error"); return redirect(f"/rfq/{rid}")

    rfq_dir = os.path.join(UPLOAD_DIR, rid)
    os.makedirs(rfq_dir, exist_ok=True)

    saved = []
    for f in files:
        safe_fn = _safe_filename(f.filename)
        if safe_fn and safe_fn.lower().endswith(".pdf"):
            p = os.path.join(rfq_dir, safe_fn)
            f.save(p)
            saved.append(p)
            # Store in DB
            try:
                f.seek(0)
                file_data = f.read()
                if not file_data:
                    with open(p, "rb") as _rb:
                        file_data = _rb.read()
                save_rfq_file(rid, safe_fn, "template_upload", file_data, category="template", uploaded_by="user")
            except Exception:
                try:
                    with open(p, "rb") as _rb:
                        save_rfq_file(rid, safe_fn, "template_upload", _rb.read(), category="template", uploaded_by="user")
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)

    if not saved:
        flash("No PDFs found in upload", "error"); return redirect(f"/rfq/{rid}")

    # Identify which forms were uploaded
    new_templates = identify_attachments(saved)

    # Merge with existing templates (don't overwrite)
    existing = r.get("templates", {})
    for key, path in new_templates.items():
        existing[key] = path
    r["templates"] = existing

    # If we now have a 704B and didn't have line items, re-parse
    if "704b" in new_templates and not r.get("line_items"):
        try:
            parsed = parse_rfq_attachments(existing)
            r["line_items"] = parsed.get("line_items", r.get("line_items", []))
            r["solicitation_number"] = parsed.get("solicitation_number", r.get("solicitation_number", ""))
            r["delivery_location"] = parsed.get("delivery_location", r.get("delivery_location", ""))
            # Auto SCPRS lookup on new items
            r["line_items"] = bulk_lookup(r.get("line_items", []))
        except Exception as e:
            log.error(f"Re-parse error: {e}")

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    found = [k for k in new_templates.keys()]
    _log_rfq_activity(rid, "templates_uploaded",
        f"Templates uploaded: {', '.join(found).upper()} for #{r.get('solicitation_number','?')}",
        actor="user", metadata={"templates": found})
    flash(f"Templates uploaded: {', '.join(found).upper()}", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/generate-package", methods=["POST"])
@auth_required
@safe_route
def generate_rfq_package(rid):
    """ONE BUTTON — generates complete RFQ package:
    1. Filled 703B (RFQ form)
    2. Filled 704B (Quote Worksheet)
    3. Filled Bid Package
    4. Reytech Quote on letterhead
    5. Draft email with all attachments
    """
    _bad = _validate_rid(rid)
    if _bad: return _bad
    from src.api.trace import Trace
    t = Trace("rfq_package", rfq_id=rid)
    
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        t.fail("RFQ not found")
        flash("RFQ not found", "error")
        return redirect("/")
    
    sol = r.get("solicitation_number", "") or "RFQ"
    t.step("Starting", sol=sol, items=len(r.get("line_items", [])))
    
    # ── Step 1: Save ALL fields from form (not just pricing) ──
    for i, item in enumerate(r.get("line_items", [])):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit"), ("markup", "markup_pct")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try:
                    item[key] = float(v)
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
        # Save description, qty, uom, part# from form too
        desc_val = request.form.get(f"desc_{i}")
        if desc_val is not None:
            item["description"] = desc_val.strip()
        qty_val = request.form.get(f"qty_{i}")
        if qty_val:
            try: item["qty"] = int(float(qty_val))
            except (ValueError, TypeError) as e: log.debug("qty coerce %r: %s", qty_val, e)
        uom_val = request.form.get(f"uom_{i}")
        if uom_val is not None:
            item["uom"] = uom_val.strip().upper()
        part_val = request.form.get(f"part_{i}")
        if part_val is not None:
            item["item_number"] = part_val.strip()
        link_val = request.form.get(f"link_{i}", "").strip()
        if link_val:
            item["item_link"] = link_val
    
    r["sign_date"] = get_pst_date()
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    
    # ── Step 1.5: Archive old files (don't delete until new generation succeeds) ──
    import shutil as _sh_clean
    _old_dir = None
    if os.path.exists(out_dir):
        _old_dir = out_dir + "_prev"
        try:
            if os.path.exists(_old_dir):
                _sh_clean.rmtree(_old_dir)
            os.rename(out_dir, _old_dir)
            t.step(f"Archived old files to {sol}_prev/")
        except Exception as _ce:
            _old_dir = None
            t.warn("Archive failed, removing old files", error=str(_ce))
            try:
                _sh_clean.rmtree(out_dir)
            except Exception as _e:
                log.debug('suppressed in generate_rfq_package: %s', _e)
    os.makedirs(out_dir, exist_ok=True)

    r["output_files"] = []
    r.pop("draft_email", None)
    
    output_files = []
    errors = []
    
    # ── Step 2: Fill State Forms (703B, 704B, Bid Package) ──
    try:
        tmpl = r.get("templates", {})
        
        # ── DB Fallback: if template files don't exist on disk (post-redeploy),
        # reconstruct them from rfq_files DB table ──
        db_files = list_rfq_files(rid, category="template")
        type_map = {"703b": "703b", "704b": "704b", "bidpkg": "bidpkg", "bid_package": "bidpkg"}
        for db_f in db_files:
            # Determine template type from filename or file_type
            ft = db_f.get("file_type", "").lower().replace("template_", "")
            fname = db_f.get("filename", "").lower()
            ttype = None
            if "703b" in ft or "703b" in fname:
                ttype = "703b"
            elif "704b" in ft or "704b" in fname:
                ttype = "704b"
            elif "bid" in ft or "bid" in fname:
                ttype = "bidpkg"
            
            if ttype and (ttype not in tmpl or not os.path.exists(tmpl.get(ttype, ""))):
                # Restore from DB to temp location
                full_f = get_rfq_file(db_f["id"])
                if full_f and full_f.get("data"):
                    restore_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
                    os.makedirs(restore_dir, exist_ok=True)
                    restore_path = os.path.join(restore_dir, db_f["filename"])
                    with open(restore_path, "wb") as _fw:
                        _fw.write(full_f["data"])
                    tmpl[ttype] = restore_path
                    t.step(f"Restored {ttype} from DB: {db_f['filename']}")
        
        # ── Auto-fallback: Use saved CDCR bid package template ONLY for CDCR/CCHCS ──
        _agency = (r.get("agency", "") or "").upper()
        _is_cdcr = any(x in _agency for x in ["CDCR", "CCHCS", "CORRECTIONS"])
        if _is_cdcr and ("bidpkg" not in tmpl or not os.path.exists(tmpl.get("bidpkg", ""))):
            default_bidpkg = os.path.join(DATA_DIR, "templates", "cdcr_bid_package_template.pdf")
            if os.path.exists(default_bidpkg):
                tmpl["bidpkg"] = default_bidpkg
                t.step("Using default CDCR bid package template")
        
        # Update templates in RFQ data
        r["templates"] = tmpl
        
        # ── Match agency FIRST — determines which forms to generate ──
        try:
            from src.core.agency_config import match_agency
            _agency_key, _agency_cfg = match_agency(r)
            _req_forms = set(_agency_cfg.get("required_forms", []))
            _opt_forms = set(_agency_cfg.get("optional_forms", []))
            t.step(f"Agency matched: {_agency_key} ({_agency_cfg.get('name','')}), {len(_req_forms)} required forms: {', '.join(sorted(_req_forms))}")
        except Exception as _ae:
            t.warn(f"Agency config load failed, using CCHCS default: {_ae}")
            _req_forms = {"703b", "704b", "bidpkg", "quote"}
            _opt_forms = set()
            _agency_key = "cchcs"
            _agency_cfg = {"name": "CCHCS / CDCR (fallback)", "required_forms": list(_req_forms)}
        
        # Helper: should this form be included?
        _user_forms = r.get("package_forms", {})
        def _include(form_id):
            # User checklist overrides if set — BUT if agency requires it, always include
            if form_id in _req_forms:
                # Agency requires this form — include regardless of user override
                if form_id in _user_forms and not bool(_user_forms[form_id]):
                    log.warning("FORM OVERRIDE: %s is required by %s but user disabled it — including anyway",
                                form_id, _agency_key)
                return True
            if form_id in _user_forms:
                return bool(_user_forms[form_id])
            return False

        # ── Check buyer preferences before generating ──
        try:
            from src.core.dal import get_buyer_preferences as _gbp
            _buyer_email = r.get("requestor_email", "")
            if _buyer_email:
                for _bp in _gbp(_buyer_email):
                    _bk = _bp.get("preference_key", "")
                    _bv = _bp.get("preference_value", "")
                    if _bk == "ship_to_override" and _bv and _bv != r.get("delivery_location", ""):
                        r["delivery_location"] = _bv
                        log.info("GENERATE %s: ship_to overridden by buyer pref: %s", rid, _bv[:40])
                        t.step(f"Buyer pref: ship_to → {_bv[:40]}")
                    elif _bk == "required_forms" and _bv:
                        for _ef in [f.strip() for f in _bv.split(",") if f.strip()]:
                            if _ef not in _req_forms:
                                _req_forms.add(_ef)
                                t.step(f"Buyer pref: added form {_ef}")
                    elif _bk == "no_modify_buyer_fields":
                        t.step("Buyer pref: no_modify_buyer_fields active")
        except Exception as _bp_e:
            log.debug("Buyer pref check: %s", _bp_e)

        # ── Auto-validate tax rate if not yet validated ──
        if not r.get("tax_validated") and r.get("delivery_location"):
            try:
                from src.agents.tax_agent import get_tax_rate as _gtr
                _dl = r["delivery_location"]
                _zip_m = __import__('re').search(r'\b(\d{5})\b', _dl)
                _city_m = __import__('re').search(r',\s*([A-Za-z\s]+),\s*[A-Z]{2}', _dl)
                _street_m = __import__('re').search(r'^(\d+\s+.+?)(?:,|$)', _dl)
                _tax_r = _gtr(
                    street=_street_m.group(1).strip() if _street_m else "",
                    city=_city_m.group(1).strip() if _city_m else "",
                    zip_code=_zip_m.group(1) if _zip_m else ""
                )
                if _tax_r and _tax_r.get("rate"):
                    r["tax_rate"] = round(_tax_r["rate"] * 100, 3)
                    r["tax_validated"] = True
                    r["tax_source"] = _tax_r.get("source", "cdtfa_api")
                    r["tax_jurisdiction"] = _tax_r.get("jurisdiction", "")
                    t.step(f"Tax auto-validated: {r['tax_rate']}% ({r['tax_jurisdiction']})")
            except Exception as _te:
                log.debug("Tax auto-validate: %s", _te)

        try:
            from src.core.dal import log_lifecycle_event as _lle_start
            _lle_start("rfq", rid, "package_generate_started",
                f"Generating package for {_agency_key} ({len(_req_forms)} required forms)",
                actor="user", detail={"agency": _agency_key, "required_forms": list(_req_forms)})
        except Exception as _e:
            log.debug('suppressed in _include: %s', _e)

        # ── Pre-flight: verify required templates exist BEFORE generating ──
        _buyer_templates = {"703b", "703c", "704b"}
        _missing_templates = []
        for _ft in _req_forms:
            if _ft in _buyer_templates:
                # 703b/703c — either one satisfies the requirement
                if _ft in ("703b", "703c"):
                    if not (("703b" in tmpl and os.path.exists(tmpl.get("703b", "")))
                            or ("703c" in tmpl and os.path.exists(tmpl.get("703c", "")))):
                        _missing_templates.append("703B/703C")
                elif _ft == "704b":
                    if not ("704b" in tmpl and os.path.exists(tmpl.get("704b", ""))):
                        _missing_templates.append("704B")
        if _missing_templates:
            _mt_str = ", ".join(sorted(set(_missing_templates)))
            t.warn(f"Missing required templates: {_mt_str}")
            return jsonify({
                "ok": False,
                "error": f"Missing required templates: {_mt_str}. "
                         f"Upload them on this page first (they must come from the buyer's RFQ email).",
                "missing_templates": list(set(_missing_templates)),
                "agency_key": _agency_key,
                "required_forms": list(_req_forms),
            }), 400

        # Pre-flight: verify signature image exists
        try:
            from src.forms.form_qa import verify_signature_file_exists
            _sig_check = verify_signature_file_exists(CONFIG)
            if not _sig_check["passed"]:
                t.warn(f"Signature image not found: {_sig_check.get('issue', '')}")
        except Exception as _e:
            log.debug('suppressed in _include: %s', _e)

        # ── Template-based forms (only if agency requires them) ──
        # 703B or 703C — use whichever template was provided by the buyer
        if _include("703b") or _include("703c") or "703c" in tmpl or "703b" in tmpl:
            _703_key = "703c" if "703c" in tmpl else "703b"
            _703_label = "703C" if _703_key == "703c" else "703B"
            if _703_key in tmpl and os.path.exists(tmpl[_703_key]):
                try:
                    _fill_fn = fill_703b
                    if _703_key == "703c":
                        try:
                            from src.forms.reytech_filler_v4 import fill_703c
                            _fill_fn = fill_703c
                        except ImportError as _e:
                            log.debug("suppressed: %s", _e)
                    _fill_fn(tmpl[_703_key], r, CONFIG, f"{out_dir}/{sol}_{_703_label}_Reytech.pdf")
                    output_files.append(f"{sol}_{_703_label}_Reytech.pdf")
                    t.step(f"{_703_label} filled")
                except Exception as e:
                    errors.append(f"{_703_label}: {e}")
                    t.warn(f"{_703_label} fill failed", error=str(e))
            else:
                t.step(f"{_703_label} skipped — no template")
                errors.append(f"{_703_label}: no template uploaded — upload {_703_label} PDF on this RFQ page")
        
        if _include("704b"):
            # Phase 0 emergency: if operator uploaded a manual 704B, preserve
            # it instead of overwriting via auto-fill. Clear the flag via
            # DELETE /api/rfq/<rid>/manual-submit to resume auto-fill.
            _manual_704b = r.get("manual_704b")
            _manual_704b_path = f"{out_dir}/{sol}_704B_Reytech.pdf"
            if _manual_704b and os.path.exists(_manual_704b_path):
                output_files.append(f"{sol}_704B_Reytech.pdf")
                t.step("704B preserved from manual submit",
                       uploaded_at=_manual_704b.get("uploaded_at"))
            elif "704b" in tmpl and os.path.exists(tmpl["704b"]):
                try:
                    fill_704b(tmpl["704b"], r, CONFIG, f"{out_dir}/{sol}_704B_Reytech.pdf")
                    output_files.append(f"{sol}_704B_Reytech.pdf")
                    t.step("704B filled")
                    # Shadow-mode: run new fill engine in background
                    try:
                        from src.forms.shadow_mode import shadow_fill
                        shadow_fill(pc_or_rfq_dict=r, doc_type="rfq", doc_id=rid,
                                    legacy_output_path=f"{out_dir}/{sol}_704B_Reytech.pdf")
                    except Exception as _shadow_e:
                        log.debug("Shadow fill setup failed: %s", _shadow_e)
                except Exception as e:
                    errors.append(f"704B: {e}")
                    t.warn("704B fill failed", error=str(e))
            else:
                t.step("704B skipped — no template")
                errors.append("704B: no template uploaded — upload 704B PDF on this RFQ page")
        
        _843_in_bidpkg = False  # Track if 843 was already handled inside bid package
        if _include("bidpkg"):
            if "bidpkg" in tmpl and os.path.exists(tmpl["bidpkg"]):
                try:
                    _bidpkg_path = f"{out_dir}/{sol}_BidPackage_Reytech.pdf"
                    fill_bid_package(tmpl["bidpkg"], r, CONFIG, _bidpkg_path)
                    output_files.append(f"{sol}_BidPackage_Reytech.pdf")
                    t.step("Bid Package filled")
                    # Replace 843 pages in bid package with master template version
                    try:
                        import tempfile as _tmpf
                        _843_tmp = _tmpf.mktemp(suffix=".pdf")
                        from src.forms.reytech_filler_v4 import generate_dvbe_843
                        generate_dvbe_843(r, CONFIG, _843_tmp)
                        from pypdf import PdfReader as _PR843, PdfWriter as _PW843
                        _bp_reader = _PR843(_bidpkg_path)
                        _843_reader = _PR843(_843_tmp)
                        _bp_writer = _PW843()
                        _replaced = False
                        for _bp_page in _bp_reader.pages:
                            try:
                                _ptxt = (_bp_page.extract_text() or "").upper()
                            except Exception:
                                _ptxt = ""
                            if ("DVBE DECLARATIONS" in _ptxt or "DGS PD 843" in _ptxt) and not _replaced:
                                for _843p in _843_reader.pages:
                                    _bp_writer.add_page(_843p)
                                _replaced = True
                                continue
                            _bp_writer.add_page(_bp_page)
                        if _replaced:
                            with open(_bidpkg_path, "wb") as _bpf:
                                _bp_writer.write(_bpf)
                            _843_in_bidpkg = True
                            t.step("843 replaced with master template in bid package")
                        os.remove(_843_tmp)
                    except Exception as _843e:
                        log.warning("843 replacement in bid package failed: %s", _843e)
                except Exception as e:
                    errors.append(f"Bid Package: {e}")
                    t.warn("Bid Package fill failed", error=str(e))
            else:
                t.step("Bid Package skipped — no template")
                errors.append("Bid Package: no template uploaded — upload Bid Package PDF on this RFQ page")
        
        # ── AGENCY-GATED FORMS ─────
        
        # STD 204 Payee Data Record
        if _include("std204"):
            try:
                from src.forms.reytech_filler_v4 import fill_std204
                std204_tmpl = os.path.join(DATA_DIR, "templates", "std204_blank.pdf")
                if os.path.exists(std204_tmpl):
                    fill_std204(std204_tmpl, r, CONFIG, f"{out_dir}/{sol}_STD204_Reytech.pdf")
                    output_files.append(f"{sol}_STD204_Reytech.pdf")
                    t.step("STD 204 filled")
            except Exception as e:
                errors.append(f"STD 204: {e}")
        
        # Seller's Permit
        if _include("sellers_permit"):
            try:
                sellers_permit = os.path.join(DATA_DIR, "templates", "sellers_permit_reytech.pdf")
                if os.path.exists(sellers_permit):
                    import shutil
                    shutil.copy2(sellers_permit, f"{out_dir}/{sol}_SellersPermit_Reytech.pdf")
                    output_files.append(f"{sol}_SellersPermit_Reytech.pdf")
                    t.step("Seller's Permit included")
            except Exception as e:
                t.warn("Seller's Permit copy failed", error=str(e))
        
        # DVBE 843 — skip standalone if already inside bid package
        if _include("dvbe843") and not _843_in_bidpkg:
            try:
                from src.forms.reytech_filler_v4 import generate_dvbe_843
                log.info("FORM GENERATING dvbe843 → %s/%s_DVBE843_Reytech.pdf", out_dir, sol)
                generate_dvbe_843(r, CONFIG, f"{out_dir}/{sol}_DVBE843_Reytech.pdf")
                output_files.append(f"{sol}_DVBE843_Reytech.pdf")
                t.step("DVBE 843 generated (standalone)")
            except Exception as e:
                log.error("FORM FAILED dvbe843: %s", e, exc_info=True)
                errors.append(f"DVBE 843: {e}")
        elif _843_in_bidpkg and _include("dvbe843"):
            t.step("DVBE 843 skipped — already inside bid package")
        
        # CV 012 CUF (Cal Vet)
        log.info("FORM CHECK cv012_cuf: _include=%s", _include("cv012_cuf"))
        if _include("cv012_cuf"):
            try:
                from src.forms.reytech_filler_v4 import fill_cv012_cuf
                cuf_tmpl = os.path.join(DATA_DIR, "templates", "cv012_cuf_blank.pdf")
                log.info("FORM cv012_cuf: template exists=%s path=%s", os.path.exists(cuf_tmpl), cuf_tmpl)
                if os.path.exists(cuf_tmpl):
                    fill_cv012_cuf(cuf_tmpl, r, CONFIG, f"{out_dir}/{sol}_CV012_CUF_Reytech.pdf")
                    output_files.append(f"{sol}_CV012_CUF_Reytech.pdf")
                    t.step("CV 012 CUF filled")
                else:
                    errors.append(f"CV 012 CUF: template not found at {cuf_tmpl}")
                    log.error("FORM TEMPLATE MISSING: %s", cuf_tmpl)
            except Exception as e:
                log.error("FORM FAILED cv012_cuf: %s", e, exc_info=True)
                t.warn("CV 012 CUF failed", error=str(e))
        
        # Barstow CUF (facility-specific)
        if _include("barstow_cuf") or "barstow_cuf" in _opt_forms:
            try:
                from src.forms.reytech_filler_v4 import generate_barstow_cuf
                _rfq_text = " ".join([
                    str(r.get("ship_to", "")), str(r.get("delivery_location", "")),
                    str(r.get("institution", "")),
                ]).lower()
                if "barstow" in _rfq_text or "92311" in _rfq_text:
                    generate_barstow_cuf(r, CONFIG, f"{out_dir}/{sol}_BarstowCUF_Reytech.pdf")
                    output_files.append(f"{sol}_BarstowCUF_Reytech.pdf")
                    t.step("Barstow CUF generated")
            except Exception as e:
                t.warn("Barstow CUF failed", error=str(e))
        
        # Bidder Declaration
        if _include("bidder_decl"):
            try:
                _bd_tmpl = os.path.join(DATA_DIR, "templates", "bidder_declaration_blank.pdf")
                if os.path.exists(_bd_tmpl):
                    from src.forms.reytech_filler_v4 import fill_bidder_declaration
                    fill_bidder_declaration(_bd_tmpl, r, CONFIG, f"{out_dir}/{sol}_BidderDecl_Reytech.pdf")
                    t.step("Bidder Declaration filled from template")
                else:
                    from src.forms.reytech_filler_v4 import generate_bidder_declaration
                    generate_bidder_declaration(r, CONFIG, f"{out_dir}/{sol}_BidderDecl_Reytech.pdf")
                    t.step("Bidder Declaration generated via ReportLab (no template)")
                output_files.append(f"{sol}_BidderDecl_Reytech.pdf")
            except Exception as e:
                errors.append(f"Bidder Declaration: {e}")
                t.warn("Bidder Declaration failed", error=str(e))

        # Darfur Act — template-first, ReportLab fallback
        if _include("darfur_act"):
            try:
                _da_tmpl = os.path.join(DATA_DIR, "templates", "darfur_act_blank.pdf")
                if os.path.exists(_da_tmpl):
                    from src.forms.reytech_filler_v4 import fill_darfur_standalone
                    fill_darfur_standalone(_da_tmpl, r, CONFIG, f"{out_dir}/{sol}_DarfurAct_Reytech.pdf")
                    t.step("Darfur Act filled from template")
                else:
                    from src.forms.reytech_filler_v4 import generate_darfur_act
                    generate_darfur_act(r, CONFIG, f"{out_dir}/{sol}_DarfurAct_Reytech.pdf")
                    t.step("Darfur Act generated via ReportLab (no template)")
                output_files.append(f"{sol}_DarfurAct_Reytech.pdf")
            except Exception as e:
                errors.append(f"Darfur Act: {e}")
                t.warn("Darfur Act failed", error=str(e))
        
        # CalRecycle 74
        log.info("FORM CHECK calrecycle74: _include=%s", _include("calrecycle74"))
        if _include("calrecycle74"):
            try:
                from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
                cr_tmpl = os.path.join(DATA_DIR, "templates", "calrecycle_74_blank.pdf")
                log.info("FORM calrecycle74: template exists=%s path=%s", os.path.exists(cr_tmpl), cr_tmpl)
                if os.path.exists(cr_tmpl):
                    fill_calrecycle_standalone(cr_tmpl, r, CONFIG, f"{out_dir}/{sol}_CalRecycle74_Reytech.pdf")
                    output_files.append(f"{sol}_CalRecycle74_Reytech.pdf")
                    t.step("CalRecycle 74 filled")
                else:
                    errors.append(f"CalRecycle 74: template not found at {cr_tmpl}")
                    log.error("FORM TEMPLATE MISSING: %s", cr_tmpl)
            except Exception as e:
                log.error("FORM FAILED calrecycle74: %s", e, exc_info=True)
                t.warn("CalRecycle 74 failed", error=str(e))

        # ── DSH packet attachments (per-solicitation flat PDFs from buyer) ──
        # AttA = bidder identity, AttB = pricing, AttC = forms checklist.
        # Source PDFs come from the buyer (tmpl["dsh_attA"]/B/C registered by
        # identify_attachments at upload time). Each filler returns BytesIO;
        # we write to disk under out_dir like every other form here.
        _dsh_specs = (
            ("dsh_attA", "AttachmentA", "fill_dsh_attachment_a"),
            ("dsh_attB", "AttachmentB", "fill_dsh_attachment_b"),
            ("dsh_attC", "AttachmentC", "fill_dsh_attachment_c"),
        )
        if any(_include(k) or k in tmpl for k, _, _ in _dsh_specs):
            try:
                from src.forms.dsh_attachment_fillers import FILLERS as _DSH_FILLERS
                _dsh_parsed = {
                    "header": {"solicitation_number": sol},
                    "sol_expires": r.get("due_date", "") or r.get("sol_expires", ""),
                    "lead_time": r.get("lead_time", "") or "5-7 business days",
                    "warranty": r.get("warranty", "") or "Per manufacturer",
                    "dvbe_pct": r.get("dvbe_pct", "") or "100%",
                    "items": [
                        {
                            "qty": it.get("qty", 0),
                            "unit_price": it.get("price_per_unit") or it.get("unit_price") or 0,
                        }
                        for it in r.get("line_items", []) or []
                    ],
                    "other_charges": r.get("other_charges", 0) or 0,
                }
                for _key, _label, _fn_name in _dsh_specs:
                    if not (_include(_key) or _key in tmpl):
                        continue
                    _src = tmpl.get(_key)
                    if not _src or not os.path.exists(_src):
                        errors.append(f"DSH {_label}: source PDF missing — buyer's {_label}.pdf must be uploaded with the RFQ")
                        t.step(f"DSH {_label} skipped — no source PDF")
                        continue
                    try:
                        _filler = _DSH_FILLERS.get(_fn_name)
                        if _filler is None:
                            errors.append(f"DSH {_label}: filler {_fn_name} not found")
                            continue
                        _buf = _filler(CONFIG, _dsh_parsed, src_pdf=_src)
                        if _buf is None:
                            errors.append(f"DSH {_label}: filler returned None")
                            continue
                        _out = f"{out_dir}/{sol}_{_label}_Reytech.pdf"
                        with open(_out, "wb") as _df:
                            _df.write(_buf.getvalue())
                        output_files.append(f"{sol}_{_label}_Reytech.pdf")
                        t.step(f"DSH {_label} filled")
                    except Exception as _de:
                        log.error("FORM FAILED %s: %s", _key, _de, exc_info=True)
                        errors.append(f"DSH {_label}: {_de}")
                        t.warn(f"DSH {_label} failed", error=str(_de))
            except ImportError as _ie:
                log.error("dsh_attachment_fillers import failed: %s", _ie)
                errors.append(f"DSH attachments: filler module unavailable ({_ie})")

        # STD 1000 GenAI
        if _include("std1000"):
            try:
                from src.forms.reytech_filler_v4 import fill_std1000
                std1000_tmpl = os.path.join(DATA_DIR, "templates", "std1000_blank.pdf")
                if os.path.exists(std1000_tmpl):
                    fill_std1000(std1000_tmpl, r, CONFIG, f"{out_dir}/{sol}_STD1000_Reytech.pdf")
                    output_files.append(f"{sol}_STD1000_Reytech.pdf")
                    t.step("STD 1000 filled")
                else:
                    errors.append(f"STD 1000: template not found at {std1000_tmpl}")
                    log.error("FORM TEMPLATE MISSING: %s", std1000_tmpl)
            except Exception as e:
                t.warn("STD 1000 failed", error=str(e))
        
        # STD 205
        if _include("std205"):
            try:
                _std205_template = os.path.join(DATA_DIR, "templates", "std205_blank.pdf")
                if os.path.exists(_std205_template):
                    from src.forms.reytech_filler_v4 import fill_std205
                    fill_std205(_std205_template, r, CONFIG, f"{out_dir}/{sol}_STD205_Reytech.pdf")
                else:
                    from src.forms.reytech_filler_v4 import generate_std205
                    generate_std205(r, CONFIG, f"{out_dir}/{sol}_STD205_Reytech.pdf")
                output_files.append(f"{sol}_STD205_Reytech.pdf")
                t.step("STD 205 generated")
            except Exception as e:
                t.warn("STD 205 failed", error=str(e))
        
        # Drug-Free Workplace
        if _include("drug_free"):
            try:
                from src.forms.reytech_filler_v4 import generate_drug_free
                generate_drug_free(r, CONFIG, f"{out_dir}/{sol}_DrugFree_Reytech.pdf")
                output_files.append(f"{sol}_DrugFree_Reytech.pdf")
                t.step("Drug-Free STD 21 generated")
            except Exception as e:
                t.warn("Drug-Free failed", error=str(e))
        # GenAI 708
        if _include("genai_708"):
            _genai_tmpl = os.path.join(DATA_DIR, "templates", "genai_708_blank.pdf")
            if os.path.exists(_genai_tmpl):
                try:
                    from src.forms.reytech_filler_v4 import fill_genai_708
                    fill_genai_708(_genai_tmpl, r, CONFIG, f"{out_dir}/{sol}_GenAI708_Reytech.pdf")
                    output_files.append(f"{sol}_GenAI708_Reytech.pdf")
                    t.step("GenAI 708 filled")
                except Exception as e:
                    errors.append(f"GenAI 708: {e}")

        # STD 205 from template (prefer over ReportLab version)
        if _include("std205"):
            _std205_tmpl = os.path.join(DATA_DIR, "templates", "STD205_Payee_Data_Record_Supplement.pdf")
            if os.path.exists(_std205_tmpl) and f"{sol}_STD205_Reytech.pdf" not in output_files:
                try:
                    from src.forms.reytech_filler_v4 import fill_std205
                    fill_std205(_std205_tmpl, r, CONFIG, f"{out_dir}/{sol}_STD205_Reytech.pdf")
                    output_files.append(f"{sol}_STD205_Reytech.pdf")
                    t.step("STD 205 filled from template")
                except Exception as e:
                    errors.append(f"STD 205 template: {e}")

        # W-9 (static copy) — only if agency requires it (CalVet doesn't)
        if _include("w9"):
            _w9_path = os.path.join(DATA_DIR, "templates", "w9_reytech.pdf")
            if os.path.exists(_w9_path):
                import shutil as _sh_w9
                try:
                    _sh_w9.copy2(_w9_path, f"{out_dir}/{sol}_W9_Reytech.pdf")
                    output_files.append(f"{sol}_W9_Reytech.pdf")
                except Exception as _e:
                    log.debug('suppressed in _include: %s', _e)

        # Seller's Permit (static copy if not already added)
        if _include("sellers_permit"):
            _sp_path = os.path.join(DATA_DIR, "templates", "sellers_permit_reytech.pdf")
            if os.path.exists(_sp_path) and f"{sol}_SellersPermit_Reytech.pdf" not in output_files:
                import shutil as _sh_sp
                try:
                    _sh_sp.copy2(_sp_path, f"{out_dir}/{sol}_SellersPermit_Reytech.pdf")
                    output_files.append(f"{sol}_SellersPermit_Reytech.pdf")
                except Exception as _e:
                    log.debug('suppressed in _include: %s', _e)

    except Exception as e:
        errors.append(f"State forms: {e}")
        t.warn("State forms exception", error=str(e))

    # ── Step 2.5: 703C master template fallback (ONLY if agency requires it) ──
    if (_include("703b") or _include("703c")) and not any(f.endswith("_703B_Reytech.pdf") or f.endswith("_703C_Reytech.pdf") for f in output_files):
        _master_703c = os.path.join(DATA_DIR, "templates", "AMS 703C - RFQ.pdf")
        if os.path.exists(_master_703c):
            try:
                fill_703b(_master_703c, r, CONFIG, f"{out_dir}/{sol}_703C_Reytech.pdf")
                output_files.append(f"{sol}_703C_Reytech.pdf")
                t.step("703C filled from master template")
            except Exception as e:
                t.warn("703C master fill failed", error=str(e))

    # ── Step 3: Generate Reytech Quote on letterhead ──
    if QUOTE_GEN_AVAILABLE:
        try:
            quote_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")
            locked_qn = r.get("reytech_quote_number", "")

            # SQLite doesn't store reytech_quote_number — check JSON directly
            if not locked_qn:
                try:
                    import json as _jqn
                    _rfq_json_path = os.path.join(DATA_DIR, "rfqs.json")
                    if os.path.exists(_rfq_json_path):
                        with open(_rfq_json_path) as _jf:
                            _jrfqs = _jqn.load(_jf)
                        locked_qn = _jrfqs.get(rid, {}).get("reytech_quote_number", "")
                        if locked_qn:
                            r["reytech_quote_number"] = locked_qn
                            t.step(f"Recovered quote number from JSON: {locked_qn}")
                except Exception as _e:
                    log.debug('suppressed in _include: %s', _e)

            # GUARDRAIL: if this RFQ already has a quote number locked,
            # ALWAYS reuse it — never burn a new counter on regenerate.
            _was_existing_qn = bool(locked_qn)
            if locked_qn:
                t.step(f"Reusing locked quote number: {locked_qn}")
            else:
                # Allocate number BEFORE generating and save immediately
                from src.forms.quote_generator import _next_quote_number
                locked_qn = _next_quote_number()
                r["reytech_quote_number"] = locked_qn
                from src.api.dashboard import _save_single_rfq
                _save_single_rfq(rid, r)  # persist NOW so next generate sees it
                t.step(f"Allocated new quote number: {locked_qn}")

            result = generate_quote_from_rfq(
                r, quote_path,
                include_tax=True,
                quote_number=locked_qn,
            )

            if result.get("ok"):
                qn = result.get("quote_number", locked_qn)
                r["reytech_quote_number"] = qn
                output_files.append(f"{safe_sol}_Quote_Reytech.pdf")
                t.step("Reytech Quote generated", quote_number=qn, total=result.get("total", 0))
                # Save pricing snapshot for revert capability
                r["pricing_snapshot"] = {
                    "snapshot_at": datetime.now().isoformat(),
                    "quote_number": qn,
                    "total": result.get("total", 0),
                    "tax_rate": r.get("tax_rate", 0),
                    "items": [
                        {
                            "line_number": it.get("line_number", i+1),
                            "description": it.get("description", "")[:100],
                            "qty": it.get("qty", 0),
                            "uom": it.get("uom", ""),
                            "supplier_cost": it.get("supplier_cost", 0),
                            "price_per_unit": it.get("price_per_unit", 0),
                            "markup_pct": it.get("markup_pct", 0),
                        }
                        for i, it in enumerate(r.get("line_items", []))
                    ]
                }
                # CRM log
                _log_crm_activity(qn, "quote_generated",
                                  f"Quote {qn} generated for RFQ {sol} — ${result.get('total',0):,.2f}",
                                  actor="user", metadata={"rfq_id": rid})
            else:
                errors.append(f"Quote: {result.get('error', 'unknown')}")
                t.warn("Quote generation failed", error=result.get("error", "unknown"))
                # Rollback: if we just allocated a new number and generation failed, release it
                if locked_qn and not _was_existing_qn:
                    r["reytech_quote_number"] = ""
                    log.warning("Rolled back quote number %s after generation failure", locked_qn)
        except Exception as e:
            errors.append(f"Quote: {e}")
            t.warn("Quote exception", error=str(e))
    else:
        t.step("Quote generator not available — skipped")
    
    if not output_files and not r.get("form_type") == "generic_rfq":
        t.fail("No files generated", errors=errors)
        flash(f"No files generated — {'; '.join(errors) if errors else 'No templates found'}", "error")
        # Restore old files on failure
        if _old_dir and os.path.exists(_old_dir) and not os.listdir(out_dir):
            try:
                _sh_clean.rmtree(out_dir)
                os.rename(_old_dir, out_dir)
                log.info("Restored old files after generation failure for %s", rid)
            except Exception as _e:
                log.debug('suppressed in _include: %s', _e)
        return redirect(f"/rfq/{rid}")
    
    # ── Step 3.5: Collect ALL package PDFs (state forms + original RFQ attachments) ──
    # These will be merged into one single package PDF
    package_pdfs = []  # (filepath, label) — order matters
    
    # ── Split output files into 4 separate attachments ──
    # Attachment 1: 703B (standalone filled bidder form)
    # Attachment 2: 704B (standalone filled pricing worksheet)
    # Attachment 3: Formal quote on Reytech letterhead
    # Attachment 4: RFQ Package — all supporting compliance docs merged in this order:
    #   BidPackage (CDCR Terms + CUF MC-345 + GenAI 708 + Voluntary Stats)
    #   CalRecycle 74, Bidder Declaration, Darfur Act, DVBE 843,
    #   Drug-Free STD 21, Seller's Permit, STD 204
    #
    # RULE: To add a form to the package, add its form_id to the agency config
    # required_forms AND add its filename pattern to _FORM_ORDER below.
    # NEVER remove BidPackage from the package — it contains CUF/708/Stats.

    file_703b = None
    file_704b = None
    quote_file = None

    for f in output_files:
        fpath = os.path.join(out_dir, f)
        if not os.path.exists(fpath):
            continue
        fu = f.upper()
        if "703B" in fu:
            file_703b = f
        elif "704B" in fu:
            file_704b = f
        elif "QUOTE" in fu:
            quote_file = f
        else:
            package_pdfs.append((fpath, f))

    # ── Canonical package order — matches model R25Q120_Reytech_CCHCS_BidPackage ──
    # RULE: This order is locked to the model. Do not reorder without a new sample.
    # BidPackage (pg1 CDCR Terms, pg2 CalRecycle) come first, then standalone forms
    # fill the gap (BidderDecl, Darfur), then rest of BidPackage (CUF, DVBE, GenAI, DrugFree, PD802).
    # BUT since we can't interleave pages from BidPackage with standalone files mid-merge,
    # the actual runtime order is determined by _FORM_ORDER key matching filename patterns.
    # BidPackage sorts first (position 0), then BidderDecl (2), DarfurAct (3), etc.
    # The BidPackage page filter already removed BidDecl from the template output.
    _FORM_ORDER = [
        "BidPackage",    # 0. CDCR Terms + CalRecycle + CUF + DVBE + GenAI + DrugFree + PD802
        "CalRecycle74",  # 1. Standalone CalRecycle (if present — normally inside BidPackage)
        "BidderDecl",    # 2. Standalone Bidder Declaration (ReportLab — cleaner signature)
        "DarfurAct",     # 3. Standalone Darfur Act
        "DVBE843",       # 4. Standalone DVBE (if present — normally inside BidPackage)
        "DrugFree",      # 5. Standalone Drug-Free (if present — normally inside BidPackage)
        "SellersPermit", # 6. CA Seller's Permit
        "STD204",        # 7. STD 204 Payee Data Record (if included)
        "CV012_CUF",     # 8. CUF CV 012 (CalVet only)
        "BarstowCUF",    # 9. Barstow CUF (conditional)
        "STD1000",       # 10. STD 1000 (non-CCHCS agencies)
        "STD205",        # 11. STD 205 Payee Supplement
    ]

    def _form_sort_key(filename):
        fn_upper = filename.upper().replace("-", "").replace("_", "").replace(" ", "")
        for idx, pattern in enumerate(_FORM_ORDER):
            if pattern.upper().replace("_", "") in fn_upper:
                return idx
        return len(_FORM_ORDER)

    package_pdfs.sort(key=lambda pair: _form_sort_key(pair[1]))
    
    # ── Step 4: Merge all package PDFs into ONE file ──
    final_output_files = []
    try:
        _safe_agency = (_agency_cfg.get("name", "") or "").replace(" ", "").replace("/", "")[:20]
    except NameError:
        _safe_agency = ""
    package_filename = f"RFQ_Package_{_safe_agency}_{safe_sol}_ReytechInc.pdf" if _safe_agency else f"RFQ_Package_{safe_sol}_ReytechInc.pdf"

    final_output_files = []

    # ── Attachment 1: 703B ──
    if file_703b:
        final_output_files.append(file_703b)
        t.step(f"Attachment 1 — 703B: {file_703b}")

    # ── Attachment 2: 704B ──
    if file_704b:
        final_output_files.append(file_704b)
        t.step(f"Attachment 2 — 704B: {file_704b}")

    # ── Attachment 3: Formal Quote ──
    if quote_file:
        final_output_files.append(quote_file)
        t.step(f"Attachment 3 — Quote: {quote_file}")

    # ── Attachment 4: RFQ Package (supporting compliance docs merged) ──
    if package_pdfs:
        try:
            from pypdf import PdfReader, PdfWriter
            writer = PdfWriter()
            merge_count = 0

            try:
                from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason as _skip_reason
            except Exception:
                _skip_reason = None

            for pdf_path, label in package_pdfs:
                try:
                    reader = PdfReader(pdf_path)
                    pages_added = 0

                    # ONLY apply bid-package page filter to the actual BidPackage PDF
                    # Standalone forms (BidderDecl, Darfur, etc.) must NOT be filtered
                    _is_bidpkg = "BidPackage" in label or "bidpkg" in label.lower() or "bid_package" in label.lower()

                    for page_idx, page in enumerate(reader.pages):
                        try:
                            text = page.extract_text() or ""
                        except Exception:
                            text = ""

                        # Skip XFA placeholder pages
                        if text.strip().startswith("Please wait") or (
                                "Please wait" in text and len(text.strip()) < 300):
                            continue

                        # Skip pages flagged by the bid-package page filter
                        # ONLY for the actual bid package — NOT standalone forms
                        if _is_bidpkg and _skip_reason is not None:
                            try:
                                skip = _skip_reason(page)
                            except Exception:
                                skip = None
                            if skip:
                                continue

                        # Do NOT touch /Rotate — form field appearance streams are
                        # positioned in the rotated coordinate system. Changing rotation
                        # here misplaces all field values. PDF viewers handle /Rotate natively.

                        writer.add_page(page)
                        pages_added += 1

                    if pages_added > 0:
                        merge_count += 1
                        t.step(f"  Package includes: {label} ({pages_added} pg)")
                    else:
                        t.step(f"  Package skipped: {label} (all pages filtered)")
                except Exception as _me:
                    t.warn(f"Could not merge {label}", error=str(_me))

            if merge_count > 0:
                merged_path = os.path.join(out_dir, package_filename)
                with open(merged_path, "wb") as _mf:
                    writer.write(_mf)
                final_output_files.append(package_filename)
                t.step(f"Attachment 4 — RFQ Package: {merge_count} docs → {package_filename}")
            else:
                t.warn("RFQ Package: no docs to merge")
        except Exception as _merge_err:
            t.warn("Package merge failed", error=str(_merge_err))
            final_output_files.extend([os.path.basename(p) for p, _ in package_pdfs])
    
    if not final_output_files:
        t.fail("No files generated", errors=errors)
        flash(f"No files generated — {'; '.join(errors) if errors else 'No templates found'}", "error")
        return redirect(f"/rfq/{rid}")
    
    # ── Create package manifest for audit trail ──
    # Telemetry: record package generation attempt with agency + outcome
    try:
        from src.core.utilization import record_feature_use
        record_feature_use("rfq.generate_package", context={
            "rfq_id": rid,
            "agency": _agency_key if '_agency_key' in dir() else "",
            "file_count": len(output_files),
            "required_forms": list(_req_forms) if '_req_forms' in dir() else [],
        }, ok=not errors)
    except Exception as _e:
        log.debug('suppressed in _form_sort_key: %s', _e)

    _manifest_id = None
    try:
        from src.core.dal import create_package_manifest, log_lifecycle_event as _lle

        log.info("PACKAGE %s: output_files=%s", rid, output_files)
        _gen_forms = []
        for _of in output_files:
            _fid = "unknown"
            _of_lower = _of.lower()
            if "quote" in _of_lower and "704" not in _of_lower: _fid = "quote"
            elif "703b" in _of_lower or "703c" in _of_lower: _fid = "703b"
            elif "704b" in _of_lower: _fid = "704b"
            elif "calrecycle" in _of_lower: _fid = "calrecycle74"
            elif "bidderdecl" in _of_lower or "bidder" in _of_lower: _fid = "bidder_decl"
            elif "dvbe" in _of_lower or "843" in _of_lower: _fid = "dvbe843"
            elif "darfur" in _of_lower: _fid = "darfur_act"
            elif "cuf" in _of_lower or "cv012" in _of_lower: _fid = "cv012_cuf"
            elif "std205" in _of_lower: _fid = "std205"
            elif "std204" in _of_lower or "payee" in _of_lower: _fid = "std204"
            elif "std1000" in _of_lower: _fid = "std1000"
            elif "seller" in _of_lower or "permit" in _of_lower: _fid = "sellers_permit"
            elif "bidpkg" in _of_lower or "bidpackage" in _of_lower: _fid = "bidpkg"
            elif "obs" in _of_lower or "1600" in _of_lower: _fid = "obs_1600"
            elif "drug" in _of_lower: _fid = "drug_free"
            _entry = {"form_id": _fid, "filename": _of}
            # Pass template path for buyer field contamination check
            if _fid == "704b" and "704b" in tmpl:
                _entry["template_path"] = tmpl["704b"]
            _gen_forms.append(_entry)

        _gen_ids = {f["form_id"] for f in _gen_forms}
        _missing = [f for f in _req_forms if f not in _gen_ids]
        log.info("PACKAGE %s: gen_forms=%s | gen_ids=%s | missing=%s | req=%s",
                 rid, [f["form_id"] for f in _gen_forms], _gen_ids, _missing, _req_forms)

        # ── Source validation: cross-reference email ──
        _source_val = None
        try:
            from src.forms.price_check import validate_source_email
            _source_val = validate_source_email(r)
            if _source_val and not _source_val.get("ok"):
                _lle("rfq", rid, "source_validation_warning",
                     _source_val.get("summary", "Source validation issues"),
                     actor="system", detail=_source_val)
        except Exception as _sv_e:
            log.debug("Source validation: %s", _sv_e)

        # ── Form QA: comprehensive field, signature, and package verification ──
        # This runs the full form_qa suite (field verification, signature
        # check, 704b computation audit, buyer-field contamination guard,
        # overlay bounds self-check, Email-as-Contract requirements gate)
        # against every generated file. Passing requirements_json +
        # strict_requirements=True activates the Email-as-Contract
        # blocking check from PR #45 for every agency, not just CCHCS.
        _field_audits = {}
        _req_json_for_qa = r.get("requirements_json", "") or ""
        try:
            from src.forms.form_qa import run_form_qa
            _qa_report = run_form_qa(
                out_dir=out_dir,
                output_files=output_files,
                form_id_map=_gen_forms,
                rfq_data=r,
                config=CONFIG,
                agency_key=_agency_key,
                required_forms=_req_forms,
                requirements_json=_req_json_for_qa,
                strict_requirements=True,
            )
            # Transform QA report into flat per-form structure for the review template
            # Template expects: audits[form_id] = {checks: [], warnings: [], errors: [], page_count, field_count}
            _field_audits = {}
            for _fid, _fr in _qa_report.get("form_results", {}).items():
                _checks = []
                _warnings = list(_fr.get("fields", {}).get("warnings", []))
                _errors = list(_fr.get("fields", {}).get("issues", []))
                _warnings.extend(_fr.get("signatures", {}).get("warnings", []))
                _errors.extend(_fr.get("signatures", {}).get("issues", []))
                if _fr.get("passed"):
                    _checks.append("All fields verified")
                if _fr.get("signatures", {}).get("passed"):
                    _checks.append("Signature present")
                _field_audits[_fid] = {
                    "checks": _checks,
                    "warnings": _warnings,
                    "errors": _errors,
                    "passed": _fr.get("passed", True),
                    "page_count": _fr.get("fields", {}).get("page_count", 0),
                    "field_count": _fr.get("fields", {}).get("field_count", 0),
                }
            # Store overall QA pass/fail for blocking
            _field_audits["_qa_passed"] = _qa_report.get("passed", True)
            _field_audits["_qa_summary"] = {
                "forms_checked": _qa_report.get("forms_checked", 0),
                "critical_issues": _qa_report.get("critical_issues", []),
                "duration_ms": _qa_report.get("duration_ms", 0),
            }
            if _qa_report.get("critical_issues"):
                for _qi in _qa_report["critical_issues"]:
                    errors.append(f"QA: {_qi}")
                t.warn("Form QA found issues", detail=_qa_report)
            else:
                t.step(f"Form QA PASSED: {_qa_report['forms_checked']} forms, {_qa_report['duration_ms']}ms")
            # Log structured QA lifecycle event for effectiveness tracking
            try:
                _qa_cats = {}
                _qa_form_pf = {}
                for _qfid, _qfr in _qa_report.get("form_results", {}).items():
                    _qa_form_pf[_qfid] = _qfr.get("passed", True)
                    for _qk in ("fields", "signatures", "computations", "buyer_fields", "value_ranges"):
                        if _qfr.get(_qk) and not _qfr[_qk].get("passed", True):
                            _qa_cats[_qk] = _qa_cats.get(_qk, 0) + 1
                if not _qa_report.get("package_check", {}).get("passed", True):
                    _qa_cats["package"] = _qa_cats.get("package", 0) + 1
                from src.core.dal import get_lifecycle_events as _gle_qa
                _gen_seq = sum(1 for e in _gle_qa("rfq", rid, limit=50)
                               if e.get("event_type") == "form_qa_completed") + 1
                _lle("rfq", rid, "form_qa_completed",
                     f"Form QA {'PASSED' if _qa_report.get('passed') else 'FAILED'}: "
                     f"{_qa_report.get('forms_checked', 0)} forms, "
                     f"{len(_qa_report.get('critical_issues', []))} critical",
                     actor="system", detail={
                         "passed": _qa_report.get("passed", True),
                         "forms_checked": _qa_report.get("forms_checked", 0),
                         "critical_count": len(_qa_report.get("critical_issues", [])),
                         "warning_count": sum(len(_qfr.get("fields", {}).get("warnings", []))
                                              for _qfr in _qa_report.get("form_results", {}).values()),
                         "duration_ms": _qa_report.get("duration_ms", 0),
                         "critical_issues": _qa_report.get("critical_issues", [])[:5],
                         "categories": _qa_cats,
                         "form_pass_fail": _qa_form_pf,
                         "generation_sequence": _gen_seq,
                     })
            except Exception as _e:
                log.debug('suppressed in _form_sort_key: %s', _e)
        except Exception as _fa_e:
            log.warning("Form QA error: %s", _fa_e)
            # Fallback to shallow audit
            try:
                from src.forms.price_check import audit_generated_form
                _expected = {"company_name": CONFIG.get("company", {}).get("name", "Reytech"), "solicitation": sol}
                for _gf in _gen_forms:
                    _gf_path = os.path.join(out_dir, _gf.get("filename", ""))
                    if os.path.exists(_gf_path):
                        _audit = audit_generated_form(_gf_path, _gf["form_id"], _expected)
                        _field_audits[_gf["form_id"]] = _audit
            except Exception as _e:
                log.debug('suppressed in _form_sort_key: %s', _e)

        _qtotal = 0
        _qnum = r.get("reytech_quote_number", "")
        _icount = len(r.get("line_items", []))
        for _it in r.get("line_items", []):
            try:
                _p = float(_it.get("price_per_unit") or _it.get("unit_price") or 0)
                _q = int(float(_it.get("qty", 1)))
                _qtotal += _p * _q
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

        _items_snap = [{"desc": (_it.get("description") or "")[:60],
            "qty": _it.get("qty", 1),
            "price": _it.get("price_per_unit") or _it.get("unit_price") or 0,
            "cost": _it.get("vendor_cost") or _it.get("supplier_cost") or 0,
            } for _it in r.get("line_items", [])]

        _manifest_id = create_package_manifest(
            rfq_id=rid, agency_key=_agency_key,
            agency_name=_agency_cfg.get("name", ""),
            required_forms=list(_req_forms), generated_forms=_gen_forms,
            missing_forms=_missing, quote_number=_qnum,
            quote_total=round(_qtotal, 2), item_count=_icount, created_by="user",
            items_snapshot=_items_snap)

        if _missing:
            _lle("rfq", rid, "package_missing_forms",
                 f"Package missing {len(_missing)} required forms: {', '.join(_missing)}",
                 actor="system", detail={"missing": _missing, "generated": [f["form_id"] for f in _gen_forms]})

        _lle("rfq", rid, "package_generated",
             f"Generated {len(output_files)} forms, {len(_missing)} missing",
             actor="user", detail={
                 "manifest_id": _manifest_id, "forms": [f["form_id"] for f in _gen_forms],
                 "missing": _missing, "quote_number": _qnum,
                 "quote_total": round(_qtotal, 2), "errors": errors[:5] if errors else []})

        r["_manifest_id"] = _manifest_id
        r["_package_filename"] = package_filename
        if _manifest_id:
            from src.core.dal import update_manifest_status as _ums
            _ums(_manifest_id, "draft", package_filename=package_filename)
            # Store validation + audit on manifest
            if _source_val or _field_audits:
                try:
                    import json as _jm
                    from src.core.db import get_db as _gdb
                    with _gdb() as _conn:
                        _conn.execute("""UPDATE package_manifest SET source_validation = ?, field_audit = ? WHERE id = ?""",
                            (_jm.dumps(_source_val, default=str) if _source_val else None,
                             _jm.dumps(_field_audits, default=str) if _field_audits else None,
                             _manifest_id))
                except Exception as _e:
                    log.debug('suppressed in _form_sort_key: %s', _e)
    except Exception as _me:
        log.error("MANIFEST CREATION FAILED: %s", _me, exc_info=True)
        errors.append(f"Manifest: {_me}")

    # ── Step 5: Store final files in DB (survive redeploys) ──
    for f in final_output_files:
        fpath = os.path.join(out_dir, f)
        try:
            if os.path.exists(fpath):
                with open(fpath, "rb") as _fb:
                    ftype = "generated_quote" if "Quote" in f else "generated_package"
                    save_rfq_file(rid, f, ftype, _fb.read(), category="generated", uploaded_by="user")
                    t.step(f"DB stored: {f}")
        except Exception as _de:
            t.warn(f"DB store failed: {f}", error=str(_de))
    
    # ── Package completeness gate (agency-agnostic hard fail) ──
    # PR D: every required form in agency_config.required_forms MUST be
    # in output_files AND pass form_qa, OR the package is incomplete
    # and the transition to "generated" is BLOCKED. This gives every
    # agency (CalVet, DGS, DSH, CalFire, etc.) the same guarantee that
    # the CCHCS packet got in PR #40.
    try:
        from src.forms.package_completeness import check_package_completeness
        _gen_form_ids = {f["form_id"] for f in _gen_forms}
        _qa_form_results = (
            _qa_report.get("form_results", {}) if '_qa_report' in dir() else {}
        )
        _completeness = check_package_completeness(
            required_forms=_req_forms,
            generated_form_ids=_gen_form_ids,
            qa_form_results=_qa_form_results,
        )
    except Exception as _ce:
        log.error("completeness check crashed: %s", _ce, exc_info=True)
        # Fail-open on gate errors — don't want a bug in the gate to
        # block every package generation. The errors list still shows
        # the crash so the operator can investigate.
        _completeness = {"complete": True, "reasons": [],
                         "missing_required": [], "failed_required": []}
        errors.append(f"completeness check crashed: {_ce}")

    _package_complete = _completeness["complete"]
    _missing_required = _completeness["missing_required"]
    _failed_required = _completeness["failed_required"]
    _package_incomplete_reasons = _completeness["reasons"]
    _incomplete_msg = "; ".join(_package_incomplete_reasons)

    if not _package_complete:
        log.error("PACKAGE INCOMPLETE %s: %s", rid, _incomplete_msg)
        errors.append(f"Package incomplete: {_incomplete_msg}")
        try:
            _lle("rfq", rid, "package_incomplete",
                 f"Package generation incomplete: {_incomplete_msg}",
                 actor="system",
                 detail={
                     "missing_required": list(_missing_required),
                     "failed_required": list(_failed_required),
                     "agency": _agency_key,
                     "required_forms": list(_req_forms),
                 })
        except Exception as _e:
            log.debug('suppressed in _form_sort_key: %s', _e)

    # ── Step 6: Save, transition, create draft email ──
    # Only transition to "generated" when the package is complete.
    # Incomplete packages transition to "generated_incomplete" so the
    # review UI shows them but blocks send.
    _final_status = "generated" if _package_complete else "generated_incomplete"
    _transition_notes = (
        f"Package: {len(final_output_files)} files"
        if _package_complete
        else f"INCOMPLETE ({_incomplete_msg}) — {len(final_output_files)} files"
    )
    _transition_status(r, _final_status, actor="user", notes=_transition_notes)

    # Notify: package ready to review
    try:
        from src.agents.notify_agent import notify_package_ready
        notify_package_ready(r, result)
    except Exception as _e:
        log.debug('suppressed in _form_sort_key: %s', _e)

    r["output_files"] = final_output_files

    # Learn which forms were used for this agency/buyer (improves future matching)
    try:
        from src.core.agency_config import learn_agency_forms
        learn_agency_forms(
            rid, _agency_key if '_agency_key' in dir() else r.get("agency", "unknown"),
            output_files,
            buyer_email=r.get("requestor_email", ""))
    except Exception as _e:
        log.debug('suppressed in _form_sort_key: %s', _e)
    r["generated_at"] = datetime.now().isoformat()
    
    # ── Google Drive: upload package to Pending ──
    try:
        from src.agents.drive_triggers import on_package_generated
        on_package_generated(r, out_dir, final_output_files)
    except Exception as _gde:
        log.debug("Drive trigger (package_generated): %s", _gde)
    
    # Draft email with final files attached (quote + merged package).
    # PR D: blocked when package is incomplete — operator must fix the
    # missing/failed forms before a draft can be created.
    if _package_complete:
        try:
            sender = EmailSender(CONFIG.get("email", {}))
            all_paths = [os.path.join(out_dir, f) for f in final_output_files]
            r["draft_email"] = sender.create_draft_email(r, all_paths)
            t.step("Draft email created", attachments=len(all_paths))
        except Exception as e:
            t.warn("Draft email failed", error=str(e))
    else:
        t.warn(
            "Draft email SKIPPED: package is incomplete",
            reasons=_package_incomplete_reasons,
        )
    
    # Save SCPRS prices for history
    try:
        save_prices_from_rfq(r)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "generated")
    except Exception as _e:
        log.debug('suppressed in _form_sort_key: %s', _e)

    # Build success message
    parts = []
    for f in final_output_files:
        if "Quote" in f: parts.append(f"Quote #{r.get('reytech_quote_number', '?')}")
        elif "Package" in f: parts.append(f"RFQ Package ({len(package_pdfs)} docs merged)")
        else: parts.append(os.path.basename(f))
    
    if _package_complete:
        msg = f"✅ RFP Package ready: {', '.join(parts)}"
        if errors:
            # Package is complete but had non-blocking errors (warnings)
            msg += f" | ⚠️ {'; '.join(errors[:3])}"
        _flash_level = "success" if not errors else "info"
        t.ok("Package complete", files=len(output_files), errors=len(errors))
    else:
        msg = (
            f"❌ Package INCOMPLETE: {_incomplete_msg}. "
            f"Fix the missing/failed required forms and re-generate — "
            f"send is blocked until every required form passes QA."
        )
        _flash_level = "error"
        t.fail("Package incomplete", files=len(output_files),
               reasons=_package_incomplete_reasons)

    # Log activity
    _log_rfq_activity(rid, "package_generated", msg, actor="user",
        metadata={
            "files": output_files,
            "quote_number": r.get("reytech_quote_number", ""),
            "errors": errors,
            "package_complete": _package_complete,
            "missing_required": list(_missing_required) if not _package_complete else [],
            "failed_required": list(_failed_required) if not _package_complete else [],
        })

    flash(msg, _flash_level)

    # Clean up archived old files ONLY after successful generation
    if _old_dir and os.path.exists(_old_dir):
        try:
            _sh_clean.rmtree(_old_dir)
        except Exception as _e:
            log.debug('suppressed in _form_sort_key: %s', _e)
    # Clean old DB files not in new output
    try:
        from src.core.db import get_db as _gdb_clean
        with _gdb_clean() as _conn_clean:
            if output_files:
                _ph = ",".join("?" for _ in output_files)
                _conn_clean.execute(f"DELETE FROM rfq_files WHERE rfq_id = ? AND category = 'generated' AND filename NOT IN ({_ph})", [rid] + list(output_files))
            else:
                _conn_clean.execute("DELETE FROM rfq_files WHERE rfq_id = ? AND category = 'generated'", (rid,))
    except Exception as _e:
        log.debug('suppressed in _form_sort_key: %s', _e)

    return redirect(f"/rfq/{rid}/review-package")


@bp.route("/rfq/<rid>/generate", methods=["POST"])
@auth_required
@safe_page
def generate(rid):
    _bad = _validate_rid(rid)
    if _bad: return _bad
    log.info("Generate bid package for RFQ %s", rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    # Update pricing from form
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit"), ("markup", "markup_pct")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try: item[key] = float(v)
                except Exception as e:

                    log.debug("Suppressed: %s", e)
    
    r["sign_date"] = get_pst_date()
    sol = r["solicitation_number"]
    out = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out, exist_ok=True)
    
    try:
        t = r.get("templates", {})
        output_files = []
        
        if "703b" in t and os.path.exists(t["703b"]):
            fill_703b(t["703b"], r, CONFIG, f"{out}/{sol}_703B_Reytech.pdf")
            output_files.append(f"{sol}_703B_Reytech.pdf")
        
        # Phase 0: preserve manual-submitted 704B (see docs/DESIGN_704_REBUILD.md)
        _manual_704b = r.get("manual_704b")
        _manual_704b_path = f"{out}/{sol}_704B_Reytech.pdf"
        if _manual_704b and os.path.exists(_manual_704b_path):
            output_files.append(f"{sol}_704B_Reytech.pdf")
        elif "704b" in t and os.path.exists(t["704b"]):
            fill_704b(t["704b"], r, CONFIG, f"{out}/{sol}_704B_Reytech.pdf")
            output_files.append(f"{sol}_704B_Reytech.pdf")
        
        if "bidpkg" in t and os.path.exists(t["bidpkg"]):
            fill_bid_package(t["bidpkg"], r, CONFIG, f"{out}/{sol}_BidPackage_Reytech.pdf")
            output_files.append(f"{sol}_BidPackage_Reytech.pdf")
        
        if not output_files:
            flash("No template PDFs found — upload the original RFQ PDFs first", "error")
            return redirect(f"/rfq/{rid}")
        
        _transition_status(r, "generated", actor="system", notes="Bid package filled")

        # Notify: package ready to review
        try:
            from src.agents.notify_agent import notify_package_ready
            notify_package_ready(r, {})
        except Exception as _e:
            log.debug('suppressed in generate: %s', _e)

        r["output_files"] = output_files
        r["generated_at"] = datetime.now().isoformat()
        
        # Note which forms are missing
        missing = []
        if "703b" not in t: missing.append("703B")
        if "704b" not in t: missing.append("704B")
        if "bidpkg" not in t: missing.append("Bid Package")
        
        # Create draft email
        sender = EmailSender(CONFIG.get("email", {}))
        output_paths = [f"{out}/{f}" for f in r["output_files"]]
        r["draft_email"] = sender.create_draft_email(r, output_paths)
        
        # Save SCPRS prices
        save_prices_from_rfq(r)

        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        try:
            from src.core.dal import update_rfq_status as _dal_ur
            _dal_ur(rid, "generated")
        except Exception as _e:
            log.debug('suppressed in generate: %s', _e)
        msg = f"Generated {len(output_files)} form(s) for #{sol}"
        if missing:
            msg += f" — missing: {', '.join(missing)}"
        else:
            msg += " — draft email ready"
        flash(msg, "success" if not missing else "info")
    except Exception as e:
        flash(f"Error: {e}", "error")
    
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/generate-quote")
@auth_required
@safe_route
def rfq_generate_quote(rid):
    """Generate a standalone Reytech-branded quote PDF from an RFQ."""
    _bad = _validate_rid(rid)
    if _bad: return _bad
    from src.api.trace import Trace
    t = Trace("quote_generation", rfq_id=rid)
    
    if not QUOTE_GEN_AVAILABLE:
        t.fail("Quote generator not available")
        flash("Quote generator not available", "error")
        return redirect(f"/rfq/{rid}")
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        t.fail("RFQ not found")
        flash("RFQ not found", "error"); return redirect("/")

    # Validate before generating
    from src.core.quote_validator import validate_ready_to_generate
    validation = validate_ready_to_generate(r)
    if not validation["ok"]:
        t.fail("Validation failed", errors=validation["errors"])
        flash(f"Cannot generate: {'; '.join(validation['errors'])}", "error")
        return redirect(f"/rfq/{rid}")

    sol = r.get("solicitation_number", "") or "RFQ"
    t.step("Starting", sol=sol, items=len(r.get("line_items",[])))
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")

    locked_qn = r.get("reytech_quote_number", "")
    # SQLite doesn't store reytech_quote_number — check JSON directly
    if not locked_qn:
        try:
            import json as _jqn2
            _rfq_json_path2 = os.path.join(DATA_DIR, "rfqs.json")
            if os.path.exists(_rfq_json_path2):
                with open(_rfq_json_path2) as _jf2:
                    _jrfqs2 = _jqn2.load(_jf2)
                locked_qn = _jrfqs2.get(rid, {}).get("reytech_quote_number", "")
                if locked_qn:
                    r["reytech_quote_number"] = locked_qn
        except Exception as _e:
            log.debug('suppressed in rfq_generate_quote: %s', _e)
    if not locked_qn:
        from src.forms.quote_generator import _next_quote_number
        locked_qn = _next_quote_number()
        r["reytech_quote_number"] = locked_qn
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)  # persist NOW to prevent duplicates

    result = generate_quote_from_rfq(r, output_path,
                                      quote_number=locked_qn)

    if result.get("ok"):
        fname = os.path.basename(output_path)
        if "output_files" not in r:
            r["output_files"] = []
        if fname not in r["output_files"]:
            r["output_files"].append(fname)
        r["reytech_quote_number"] = result.get("quote_number", locked_qn)
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        t.ok("Quote generated", quote_number=result.get("quote_number",""), total=result.get("total",0))
        log.info("Quote #%s generated for RFQ %s — $%s", result.get("quote_number"), rid, f"{result['total']:,.2f}")
        flash(f"Reytech Quote #{result['quote_number']} generated — ${result['total']:,.2f}", "success")
        _log_crm_activity(result.get("quote_number", ""), "quote_generated",
                          f"Quote {result.get('quote_number','')} generated from RFQ {sol} — ${result.get('total',0):,.2f}",
                          actor="user", metadata={"rfq_id": rid, "agency": result.get("agency","")})
    else:
        t.fail("Quote generation failed", error=result.get("error","unknown"))
        log.error("Quote generation failed for RFQ %s: %s", rid, result.get("error", "unknown"))
        flash(f"Quote generation failed: {result.get('error', 'unknown')}", "error")

    return redirect(f"/rfq/{rid}")

@bp.route("/rfq/<rid>/send", methods=["POST"])
@auth_required
@safe_page
def send_email(rid):
    _bad = _validate_rid(rid)
    if _bad: return _bad
    from src.api.trace import Trace
    t = Trace("email_send", rfq_id=rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r or not r.get("draft_email"):
        t.fail("No draft to send")
        flash("No draft to send", "error"); return redirect(f"/rfq/{rid}")

    # Validate before sending
    from src.core.quote_validator import validate_ready_to_send
    validation = validate_ready_to_send(r)
    if not validation["ok"]:
        t.fail("Send validation failed", errors=validation["errors"])
        flash(f"Cannot send: {'; '.join(validation['errors'])}", "error")
        return redirect(f"/rfq/{rid}")

    try:
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(r["draft_email"])
        _transition_status(r, "sent", actor="user", notes="Email sent to buyer")
        r["sent_at"] = datetime.now().isoformat()
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        try:
            from src.core.dal import update_rfq_status as _dal_ur
            _dal_ur(rid, "sent")
        except Exception as _e:
            log.debug('suppressed in send_email: %s', _e)
        t.ok("Email sent", to=r["draft_email"].get("to",""), sol=r.get("solicitation_number","?"))
        flash(f"Bid response sent to {r['draft_email']['to']}", "success")
        _log_rfq_activity(rid, "email_sent",
            f"Bid response emailed to {r['draft_email'].get('to','')} for #{r.get('solicitation_number','?')}",
            actor="user", metadata={"to": r["draft_email"].get("to",""), "quote": r.get("reytech_quote_number","")})
        qn = r.get("reytech_quote_number", "")
        if qn and QUOTE_GEN_AVAILABLE:
            update_quote_status(qn, "sent", actor="system")
            _log_crm_activity(qn, "email_sent",
                              f"Quote {qn} emailed to {r['draft_email'].get('to','')}",
                              actor="user", metadata={"to": r['draft_email'].get('to','')})
        # Lifecycle event + package delivery record
        try:
            from src.core.dal import log_lifecycle_event as _lle_send, record_package_delivery, get_latest_manifest
            _to = r["draft_email"].get("to", "")
            _subj = r["draft_email"].get("subject", "")
            _lle_send("rfq", rid, "package_sent", f"Sent to {_to}: {_subj[:60]}", actor="user",
                detail={"recipient": _to, "subject": _subj})
            _mf = get_latest_manifest(rid)
            if _mf:
                record_package_delivery(_mf["id"], rid, _to,
                    recipient_name=r.get("requestor_name", ""), email_subject=_subj)
        except Exception as _e:
            log.debug('suppressed in send_email: %s', _e)
    except Exception as e:
        t.fail("Send failed", error=str(e))
        flash(f"Send failed: {e}. Use 'Open in Mail App' instead.", "error")
    
    return redirect(f"/rfq/{rid}")


# ═══════════════════════════════════════════════════════════════════════
# Phase 0 emergency: manual 704B submit
# ═══════════════════════════════════════════════════════════════════════
# When auto-fill is broken on a buyer variant, the operator uploads a
# hand-filled 704B PDF. We drop it at the conventional output path so the
# existing Generate Package + Send flow treats it as the authoritative
# 704B. A flag on the RFQ record tells generate_rfq_package to SKIP the
# auto-fill for this RFQ so subsequent regenerations don't overwrite the
# operator's manual file.
@bp.route("/api/rfq/<rid>/manual-submit", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_rfq_manual_submit_704b(rid):
    """Emergency route: operator uploads a hand-filled 704B PDF that
    replaces the auto-fill output. See docs/DESIGN_704_REBUILD.md Phase 0."""
    _bad = _validate_rid(rid)
    if _bad:
        return _bad

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    filename_lower = (f.filename or "").lower()
    if not filename_lower.endswith(".pdf"):
        return jsonify({"ok": False, "error": "Upload must be a PDF"}), 400

    # Read bytes once — we validate + persist from the same buffer
    try:
        raw = f.read()
    except Exception as e:
        log.error("manual-submit read failed for %s: %s", rid, e)
        return jsonify({"ok": False, "error": f"Could not read upload: {e}"}), 400

    if not raw or len(raw) < 100:
        return jsonify({"ok": False, "error": "Upload is empty or truncated"}), 400

    # Validate AcroForm-parseable PDF via pypdf
    try:
        from pypdf import PdfReader
        from io import BytesIO
        reader = PdfReader(BytesIO(raw))
        page_count = len(reader.pages)
        if page_count < 1:
            return jsonify({"ok": False, "error": "PDF has no pages"}), 400
    except Exception as e:
        log.warning("manual-submit invalid PDF for %s: %s", rid, e)
        return jsonify({"ok": False, "error": f"Not a valid PDF: {e}"}), 400

    sol = r.get("solicitation_number", "") or "RFQ"
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)

    target_name = f"{sol}_704B_Reytech.pdf"
    target_path = os.path.join(out_dir, target_name)

    # Archive any existing 704B (auto-filled or prior manual) before overwriting
    archived_to = None
    if os.path.exists(target_path):
        try:
            import shutil as _sh
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            archive_dir = os.path.join(out_dir, "_prev")
            os.makedirs(archive_dir, exist_ok=True)
            archived_to = os.path.join(archive_dir, f"{stamp}_{target_name}")
            _sh.move(target_path, archived_to)
        except Exception as e:
            log.warning("manual-submit archive failed for %s: %s", rid, e)
            archived_to = None

    # Persist the uploaded PDF at the conventional 704B path
    try:
        with open(target_path, "wb") as fh:
            fh.write(raw)
    except Exception as e:
        log.error("manual-submit write failed for %s: %s", rid, e)
        return jsonify({"ok": False, "error": f"Could not save file: {e}"}), 500

    now_iso = datetime.now(timezone.utc).isoformat()
    r["manual_704b"] = {
        "uploaded_at": now_iso,
        "original_filename": f.filename,
        "bytes": len(raw),
        "pages": page_count,
        "archived_prev": archived_to,
    }

    # Keep output_files in sync so the existing send flow attaches the file
    output_files = list(r.get("output_files") or [])
    if target_name not in output_files:
        output_files.append(target_name)
    r["output_files"] = output_files

    # Audit trail
    try:
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
    except Exception as _e:
        log.warning("manual-submit single-save fallback for %s: %s", rid, _e)
        save_rfqs(rfqs)

    log.info("manual-submit 704B saved rid=%s sol=%s bytes=%d pages=%d",
             rid, sol, len(raw), page_count)

    return jsonify({
        "ok": True,
        "filename": target_name,
        "bytes": len(raw),
        "pages": page_count,
        "uploaded_at": now_iso,
        "message": "704B uploaded. Auto-fill is disabled for this RFQ — use Generate Package to fill other forms, then Send.",
    })


@bp.route("/api/rfq/<rid>/manual-submit", methods=["DELETE"])
@auth_required
@safe_route
def api_rfq_manual_submit_clear(rid):
    """Clear the manual-704b flag so auto-fill resumes on next Generate Package.
    Does NOT delete the file on disk — operator can still Generate Package to
    overwrite with fresh auto-fill output."""
    _bad = _validate_rid(rid)
    if _bad:
        return _bad

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    had_flag = bool(r.pop("manual_704b", None))
    try:
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
    except Exception as _e:
        log.warning("manual-submit-clear fallback for %s: %s", rid, _e)
        save_rfqs(rfqs)

    return jsonify({"ok": True, "cleared": had_flag})


# ────────────────────────────────────────────────────────────────────
# Contract Builder — single-upload entry point
#
# One drop target on the RFQ detail page that accepts any mix of files
# (buyer 704B, 703B, bid package, email screenshot, other attachments)
# and auto-classifies each one. Replaces the need to hunt for the right
# button per file type.
# ────────────────────────────────────────────────────────────────────
@bp.route("/api/rfq/<rid>/contract-upload", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_rfq_contract_upload(rid):
    """Multi-file upload that fans into email screenshot / template slots /
    attachments via src.forms.form_classifier.classify."""
    _bad = _validate_rid(rid)
    if _bad:
        return _bad

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    files = request.files.getlist("files") or request.files.getlist("file")
    if not files:
        return jsonify({"ok": False, "error": "No files uploaded"}), 400

    from src.forms.form_classifier import classify
    rfq_dir = os.path.join(UPLOAD_DIR, rid)
    os.makedirs(rfq_dir, exist_ok=True)
    templates = dict(r.get("templates") or {})
    attachments = list(r.get("attachments") or [])
    now_iso = datetime.now(timezone.utc).isoformat()
    results = []

    for f in files:
        if not f or not f.filename:
            continue
        raw = f.read()
        if not raw:
            results.append({"filename": f.filename, "kind": "skipped",
                             "reason": "empty file"})
            continue

        decision = classify(f.filename, raw)
        safe_fn = _safe_filename(f.filename) or f.filename

        # Images — store under a stable email_<stamp>.<ext> name so repeat
        # uploads don't collide and we keep the operator's original name in
        # the flag record.
        if decision["kind"] == "email_screenshot":
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            ext = os.path.splitext(safe_fn)[1].lower() or ".png"
            target_name = f"email_{stamp}{ext}"
            target_path = os.path.join(rfq_dir, target_name)
            with open(target_path, "wb") as fh:
                fh.write(raw)
            try:
                save_rfq_file(rid, target_name, "email_screenshot", raw,
                              category="email_screenshot", uploaded_by="user")
            except Exception as _e:
                log.debug("save email screenshot file row: %s", _e)
            r["email_screenshot"] = {
                "path": target_path, "filename": target_name,
                "original_filename": f.filename, "bytes": len(raw),
                "uploaded_at": now_iso,
            }
            results.append({"filename": f.filename, "kind": "email_screenshot",
                             "slot": None, "reason": decision["reason"],
                             "bytes": len(raw)})
            continue

        # Templates — slot into r["templates"][slot]
        if decision["kind"] == "template" and decision.get("slot"):
            target_path = os.path.join(rfq_dir, safe_fn)
            with open(target_path, "wb") as fh:
                fh.write(raw)
            try:
                save_rfq_file(rid, safe_fn, f"template_{decision['slot']}",
                              raw, category="template", uploaded_by="user")
            except Exception as _e:
                log.debug("save template file row: %s", _e)
            templates[decision["slot"]] = target_path
            results.append({"filename": f.filename, "kind": "template",
                             "slot": decision["slot"],
                             "reason": decision["reason"],
                             "bytes": len(raw)})
            continue

        # Fallback — attachment
        target_path = os.path.join(rfq_dir, safe_fn)
        with open(target_path, "wb") as fh:
            fh.write(raw)
        try:
            save_rfq_file(rid, safe_fn, "attachment", raw,
                          category="attachment", uploaded_by="user")
        except Exception as _e:
            log.debug("save attachment file row: %s", _e)
        attachments.append({"path": target_path, "filename": safe_fn,
                            "uploaded_at": now_iso, "bytes": len(raw)})
        results.append({"filename": f.filename, "kind": "attachment",
                         "slot": None, "reason": decision["reason"],
                         "bytes": len(raw)})

    r["templates"] = templates
    r["attachments"] = attachments

    try:
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
    except Exception as _e:
        log.warning("contract-upload single-save fallback for %s: %s", rid, _e)
        save_rfqs(rfqs)

    _log_rfq_activity(rid, "contract_upload",
        f"Contract upload: {len(results)} file(s)",
        actor="user", metadata={"results": results})

    counts = {"email_screenshot": 0, "template": 0, "attachment": 0,
              "skipped": 0}
    for res in results:
        counts[res["kind"]] = counts.get(res["kind"], 0) + 1

    return jsonify({"ok": True, "results": results, "counts": counts,
                    "templates": list(templates.keys())})


@bp.route("/api/quote/<qn>/regenerate", methods=["POST"])
@auth_required
@safe_route
def api_quote_regenerate(qn):
    """Regenerate the formal quote PDF for a given quote number.
    Finds the quote in quotes_log.json, regenerates PDF, and updates pdf_path.
    """
    from src.api.trace import Trace
    t = Trace("quote_regenerate", quote_number=qn)
    
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"}), 503
    
    try:
        quotes = get_all_quotes()
        qt = None
        for q in quotes:
            if q.get("quote_number") == qn:
                qt = q
                break
        if not qt:
            return jsonify({"ok": False, "error": f"Quote {qn} not found"}), 404
        
        # Build output path
        rfq_num = qt.get("rfq_number", "") or qn
        safe_rfq = re.sub(r'[^a-zA-Z0-9_-]', '_', str(rfq_num).strip()) or qn
        out_dir = os.path.join(OUTPUT_DIR, safe_rfq)
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, f"{safe_rfq}_Quote_Reytech.pdf")
        
        # Build quote_data from existing quote — map to generate_quote_from_rfq's expected fields
        quote_data = {
            "agency_name": qt.get("institution", ""),
            "institution": qt.get("institution", ""),
            "delivery_location": "",  # Not stored in quote, use ship_to_name instead
            "ship_to": "",
            "ship_to_name": qt.get("ship_to_name", qt.get("institution", "")),
            "ship_to_address": qt.get("ship_to_address", []),
            "agency": qt.get("agency", ""),
            "solicitation_number": rfq_num,
            "rfq_number": rfq_num,
            "requestor_email": qt.get("requestor_email", ""),
            "line_items": qt.get("items_detail", []),
            "source_pc_id": qt.get("source_pc_id", ""),
            "source_rfq_id": qt.get("source_rfq_id", ""),
        }
        
        result = generate_quote_from_rfq(
            quote_data, output_path,
            include_tax=True,
            quote_number=qn,
        )
        
        if result.get("ok"):
            # Update pdf_path in quotes_log.json
            quotes_path = os.path.join(DATA_DIR, "quotes_log.json")
            try:
                import json as _json
                all_quotes = _json.load(open(quotes_path))
                for i, q in enumerate(all_quotes):
                    if q.get("quote_number") == qn:
                        all_quotes[i]["pdf_path"] = output_path
                        break
                with open(quotes_path, "w") as f:
                    _json.dump(all_quotes, f, indent=2)
            except Exception as _e:
                t.warn(f"Could not update quotes_log: {_e}")
            
            # Store in DB for redeploy survival
            fname = os.path.basename(output_path)
            try:
                with open(output_path, "rb") as _fb:
                    save_rfq_file(qn, fname, "generated_quote", _fb.read(),
                                  category="generated", uploaded_by="user")
            except Exception as _e:
                log.debug('suppressed in api_quote_regenerate: %s', _e)
            
            t.ok("Regenerated", path=output_path, total=result.get("total", 0))
            return jsonify({
                "ok": True,
                "quote_number": qn,
                "total": result.get("total", 0),
                "pdf_path": output_path,
                "download_url": f"/api/pricecheck/download/{fname}",
                "view_url": f"/api/pricecheck/view-pdf/{fname}",
            })
        else:
            t.fail(result.get("error", "unknown"))
            return jsonify({"ok": False, "error": result.get("error", "Generation failed")}), 500
    
    except Exception as e:
        import traceback
        t.fail(str(e))
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/dismiss", methods=["POST"])
@auth_required
@safe_route
def api_rfq_dismiss(rid):
    """Dismiss an RFQ from the active queue with a reason.
    Keeps data for SCPRS intelligence. reason=delete does hard delete."""
    from datetime import datetime

    data = request.get_json(force=True) if request.data else {}
    reason = data.get("reason", "other")

    rfqs = load_rfqs()

    if rid not in rfqs:
        return jsonify({"ok": False, "error": "RFQ not found"})

    # Hard delete path
    if reason == "delete":
        sol = rfqs[rid].get("solicitation_number", "?")
        del rfqs[rid]
        save_rfqs(rfqs)
        # Direct SQLite delete — save_rfqs only does INSERT OR REPLACE
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM rfqs WHERE id=?", (rid,))
        except Exception as e:
            log.debug("DAL RFQ delete: %s", e)
        log.info("Hard deleted RFQ #%s (id=%s)", sol, rid)
        return jsonify({"ok": True, "deleted": rid})

    r = rfqs[rid]
    r["status"] = "dismissed"
    r["dismiss_reason"] = reason
    r["dismissed_at"] = datetime.now().isoformat()
    rfqs[rid] = r
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    # Also update SQLite status via DAL
    try:
        from src.core.dal import update_rfq_status as _dal_update_rfq
        _dal_update_rfq(rid, "dismissed")
    except Exception as e:
        log.debug("DAL RFQ dismiss update: %s", e)
    
    sol = r.get("solicitation_number", "?")
    log.info("RFQ #%s dismissed: reason=%s", sol, reason)
    
    # Queue SCPRS price intelligence on line items (async)
    scprs_queued = False
    items = r.get("line_items", [])
    if items:
        try:
            from src.agents.scprs_lookup import queue_background_lookup
            for item in items[:20]:
                desc = item.get("description", "")
                if desc and len(desc) > 3:
                    queue_background_lookup(desc, source=f"dismissed_rfq_{rid}")
            scprs_queued = True
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    return jsonify({
        "ok": True,
        "dismissed": rid,
        "solicitation": sol,
        "reason": reason,
        "scprs_queued": scprs_queued,
    })


@bp.route("/rfq/<rid>/delete", methods=["POST"])
@auth_required
@safe_route
def delete_rfq(rid):
    """Delete an RFQ from the queue and remove its UID from processed list."""
    _bad = _validate_rid(rid)
    if _bad: return _bad
    rfqs = load_rfqs()
    if rid in rfqs:
        sol = rfqs[rid].get("solicitation_number", "?")
        # Remove this email's UID from processed list so it can be re-imported
        email_uid = rfqs[rid].get("email_uid")
        if email_uid:
            _remove_processed_uid(email_uid)
        del rfqs[rid]
        save_rfqs(rfqs)
        # Direct SQLite delete — save_rfqs only does INSERT OR REPLACE, never DELETE
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM rfqs WHERE id = ?", (rid,))
            log.info("Deleted RFQ %s from SQLite", rid)
        except Exception as _db_e:
            log.warning("SQLite delete for RFQ %s failed: %s", rid, _db_e)
        log.info("Deleted RFQ #%s (id=%s)", sol, rid)
        _log_rfq_activity(rid, "deleted", f"RFQ #{sol} deleted", actor="user")
        flash(f"Deleted RFQ #{sol}", "success")
    return redirect("/")


@bp.route("/api/rfq/<rid>/cancel", methods=["POST"])
@auth_required
@safe_route
def api_rfq_cancel(rid):
    """Cancel an RFQ — preserves record for tracking but stops follow-ups.
    Unlike delete, cancelled RFQs remain visible in analytics and can be reactivated."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    data = request.get_json(force=True, silent=True) or {}
    reason = data.get("reason", "Cancelled by user")
    prev_status = r.get("status", "")
    r["status"] = "cancelled"
    r["cancelled_at"] = datetime.now().isoformat()
    r["cancelled_reason"] = reason
    r["_prev_status"] = prev_status  # For reactivation
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("rfq", rid, "cancelled", f"Cancelled: {reason} (was: {prev_status})",
                            actor="user", detail={"reason": reason, "prev_status": prev_status})
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    log.info("Cancelled RFQ %s (sol=%s): %s", rid, r.get("solicitation_number", "?"), reason)
    return jsonify({"ok": True, "status": "cancelled", "prev_status": prev_status})


@bp.route("/api/rfq/<rid>/reactivate", methods=["POST"])
@auth_required
@safe_route
def api_rfq_reactivate(rid):
    """Reactivate a cancelled RFQ — restores previous status."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    if r.get("status") != "cancelled":
        return jsonify({"ok": False, "error": f"RFQ is not cancelled (status: {r.get('status')})"})
    prev = r.pop("_prev_status", "new")
    r["status"] = prev
    r.pop("cancelled_at", None)
    r.pop("cancelled_reason", None)
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("rfq", rid, "reactivated", f"Reactivated: restored to {prev}",
                            actor="user")
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    log.info("Reactivated RFQ %s (sol=%s) → %s", rid, r.get("solicitation_number", "?"), prev)
    return jsonify({"ok": True, "status": prev})


