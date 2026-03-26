# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route
from flask import redirect, flash, send_file, session
from src.core.paths import DATA_DIR, OUTPUT_DIR, UPLOAD_DIR
from src.core.db import get_db
from src.api.render import render_page

import json as _json

import os as _os

def _safe_do_poll_check():
    """Call do_poll_check via direct import to guarantee dashboard globals are in scope.
    Always routes through src.api.dashboard to avoid NameError on injected globals."""
    import sys as _sys
    _dashboard = _sys.modules.get('src.api.dashboard') or _sys.modules.get('dashboard')
    if _dashboard and hasattr(_dashboard, 'do_poll_check'):
        return _dashboard.do_poll_check()
    # Last resort: injected version
    return do_poll_check()

# Price Check Routes
# 26 routes, 985 lines
# Loaded by dashboard.py via load_module()


def _sync_pc_items(pc, items):
    """Safely sync items list to both pc['items'] and pc['parsed']['line_items'].
    Creates pc['parsed'] if it doesn't exist (e.g. after SQLite restore).
    Uses list() to prevent aliasing — items and line_items must be separate objects."""
    pc["items"] = items
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": list(items)}
    else:
        pc["parsed"]["line_items"] = list(items)


# Price Check Pages (v6.2)
# ═══════════════════════════════════════════════════════════════════════


# ── PC Revision System ─────────────────────────────────────────────────────
# Every save snapshots the previous version. Buyer changes QTY? Previous version preserved.

def _pc_revisions_path():
    return os.path.join(DATA_DIR, "pc_revisions.json")

def _load_pc_revisions():
    path = _pc_revisions_path()
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception:
            return {}
    return {}

def _save_pc_revisions(revisions):
    with open(_pc_revisions_path(), "w") as f:
        json.dump(revisions, f, indent=2, default=str)

def _save_pc_revision(pcid, pc, reason="edit"):
    """Snapshot current PC state before changes are applied."""
    from datetime import datetime as _dt
    revisions = _load_pc_revisions()
    if pcid not in revisions:
        revisions[pcid] = []

    # Create snapshot — just items + metadata (not the full parsed object)
    snapshot = {
        "revision": len(revisions[pcid]) + 1,
        "saved_at": _dt.now().isoformat(),
        "reason": reason,
        "items": json.loads(json.dumps(pc.get("items", []), default=str)),
        "status": pc.get("status", ""),
        "pc_number": pc.get("pc_number", ""),
    }
    revisions[pcid].append(snapshot)

    # Keep last 20 revisions per PC
    if len(revisions[pcid]) > 20:
        revisions[pcid] = revisions[pcid][-20:]

    _save_pc_revisions(revisions)
    log.info("PC revision saved: %s rev#%d (%s)", pcid, snapshot["revision"], reason)
    return snapshot


@bp.route("/api/pricecheck/<pcid>/revisions")
@auth_required
def api_pc_revisions(pcid):
    """Get revision history for a price check."""
    try:
        revisions = _load_pc_revisions()
        pc_revs = revisions.get(pcid, [])
        return jsonify({"ok": True, "revisions": pc_revs, "count": len(pc_revs)})
    except Exception as e:
        log.error("api_pc_revisions error pcid=%s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/revert", methods=["POST"])
@auth_required
def api_pc_revert(pcid):
    """Revert a price check to a previous revision."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        rev_num = int(data.get("revision", 0))

        revisions = _load_pc_revisions()
        pc_revs = revisions.get(pcid, [])
        target = None
        for r in pc_revs:
            if r.get("revision") == rev_num:
                target = r
                break

        if not target:
            return jsonify({"ok": False, "error": f"Revision {rev_num} not found"})

        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})

        # Save current state as a revision before reverting
        _save_pc_revision(pcid, pc, reason=f"before revert to rev#{rev_num}")

        # Apply the revision
        pc["items"] = target["items"]
        _save_single_pc(pcid, pc)

        return jsonify({"ok": True, "reverted_to": rev_num,
                        "items_count": len(target["items"])})
    except Exception as e:
        log.error("api_pc_revert error pcid=%s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/trim-items", methods=["POST"])
@auth_required
def pricecheck_trim_items(pcid):
    """Trim items list to keep only the first N items. Used when a multi-page
    source PDF was parsed into too many items for this PC."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.get_json(force=True, silent=True) or {}
    keep = data.get("keep", 0)
    if not keep or not isinstance(keep, int) or keep < 1:
        return jsonify({"ok": False, "error": "Invalid keep count"})

    items = pc.get("items", [])
    if keep >= len(items):
        return jsonify({"ok": False, "error": f"Already have {len(items)} items, keep={keep} does nothing"})

    # Save revision before trimming
    try:
        _save_pc_revision(pcid, pc, reason="trim")
    except Exception:
        pass

    removed = len(items) - keep
    pc["items"] = items[:keep]

    # Sync parsed.line_items
    if "parsed" in pc:
        pc["parsed"]["line_items"] = pc["items"]

    # Reset generate count so next generate starts fresh
    pc["_generate_count"] = 0

    _save_single_pc(pcid, pc)

    log.info("TRIM PC %s: kept %d, removed %d items", pcid, keep, removed)
    return jsonify({"ok": True, "kept": keep, "removed": removed})


@bp.route("/api/pricecheck/<pcid>/split", methods=["POST"])
@auth_required
def pricecheck_split(pcid):
    """Split a PC at a given item index. Creates a new PC with items from split_at onwards.
    Original PC keeps items 0..split_at-1."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.get_json(force=True, silent=True) or {}
    split_at = data.get("split_at", 0)
    if not split_at or not isinstance(split_at, int) or split_at < 1:
        return jsonify({"ok": False, "error": "Invalid split_at"})

    items = pc.get("items", [])
    if split_at >= len(items):
        return jsonify({"ok": False, "error": f"split_at={split_at} >= {len(items)} items"})

    try:
        _save_pc_revision(pcid, pc, reason="split")
    except Exception:
        pass

    import uuid as _uuid
    new_id = f"pc_{str(_uuid.uuid4())[:8]}"

    new_items = items[split_at:]
    pc["items"] = items[:split_at]

    if "parsed" in pc:
        pc["parsed"]["line_items"] = list(pc["items"])
    pc["_generate_count"] = 0
    if "_split_hint" in pc:
        del pc["_split_hint"]

    pcs[new_id] = {
        "id": new_id,
        "pc_number": pc.get("pc_number", "") + "_split",
        "institution": pc.get("institution", ""),
        "due_date": pc.get("due_date", ""),
        "requestor": pc.get("requestor", ""),
        "requestor_email": pc.get("requestor_email", ""),
        "ship_to": pc.get("ship_to", ""),
        "items": new_items,
        "source_pdf": pc.get("source_pdf", ""),
        "status": "parsed",
        "created_at": datetime.now().isoformat(),
        "source": f"split_from_{pcid}",
        "email_uid": pc.get("email_uid", ""),
        "reytech_quote_number": "",
        "linked_quote_number": "",
        "parsed": {"header": (pc.get("parsed") or {}).get("header", {}), "line_items": list(new_items)},
    }

    _save_price_checks(pcs)

    log.info("SPLIT PC %s at item %d: kept %d, new PC %s with %d items",
             pcid, split_at, len(pc["items"]), new_id, len(new_items))

    return jsonify({
        "ok": True,
        "original_id": pcid,
        "original_items": len(pc["items"]),
        "new_id": new_id,
        "new_items": len(new_items),
    })


@bp.route("/api/pricecheck/<pcid>/merge-items", methods=["POST"])
@auth_required
def api_pc_merge_items(pcid):
    """Merge an item into the one above it (for false multi-line splits).
    POST {index: 2} merges item[2] into item[1]."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        idx = int(data.get("index", -1))

        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})

        items = pc.get("items", [])
        if idx < 1 or idx >= len(items):
            return jsonify({"ok": False, "error": f"Invalid index {idx} (need 1-{len(items)-1})"})

        # Save revision before merging
        _save_pc_revision(pcid, pc, reason=f"before merge item {idx}")

        # Merge item[idx] description into item[idx-1]
        target = items[idx]
        prev = items[idx - 1]
        merged_desc = (prev.get("description", "") + " " + target.get("description", "")).strip()
        prev["description"] = merged_desc
        if target.get("mfg_number") and not prev.get("mfg_number"):
            prev["mfg_number"] = target["mfg_number"]

        # Remove the merged item
        items.pop(idx)
        pc["items"] = items

        # Sync parsed
        if "parsed" in pc:
            pc["parsed"]["line_items"] = items

        _save_single_pc(pcid, pc)
        return jsonify({"ok": True, "merged_into": idx - 1, "remaining_items": len(items),
                        "description": merged_desc})
    except Exception as e:
        log.error("api_pc_merge_items error pcid=%s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/status", methods=["POST"])
@auth_required
def api_pc_change_status(pcid):
    """Change PC status with history tracking.
    POST {status: "sent"} or {status: "draft"}
    Valid: new, draft, sent, pending_award, won, lost, no_response, archived"""
    try:
        data = request.get_json(force=True, silent=True) or {}
        new_status = (data.get("status") or "").strip().lower()
        valid = {"new", "draft", "sent", "pending_award", "won", "lost",
                 "no_response", "archived", "duplicate", "completed", "converted",
                 "expired", "parsed", "priced", "ready"}
        if new_status not in valid:
            return jsonify({"ok": False, "error": f"Invalid status: {new_status}. Valid: {sorted(valid)}"})

        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})

        old_status = pc.get("status", "")
        _transition_status(pc, new_status, actor="user",
                           notes=data.get("notes", f"Manual: {old_status} → {new_status}"))

        # If marking as sent, record sent_at
        if new_status == "sent" and not pc.get("sent_at"):
            pc["sent_at"] = __import__('datetime').datetime.now().isoformat()

        # Record closed_at and reason for terminal statuses
        if new_status in ("won", "lost", "expired"):
            pc["closed_at"] = __import__('datetime').datetime.now().isoformat()
            if data.get("reason"):
                pc["closed_reason"] = data["reason"]

        _save_single_pc(pcid, pc)
        
        # Auto-save items to catalog on terminal states (sent, won, completed)
        # This is how the catalog grows organically from daily quoting
        catalog_result = {}
        if new_status in ("sent", "won", "completed"):
            try:
                from src.agents.product_catalog import save_pc_items_to_catalog, init_catalog_db
                init_catalog_db()
                catalog_result = save_pc_items_to_catalog(pc)
                log.info("Auto-saved PC %s items to catalog on status=%s: %s", pcid, new_status, catalog_result)
            except Exception as _ce:
                log.warning("Catalog auto-save failed for PC %s: %s", pcid, _ce)
                catalog_result = {"error": str(_ce)}
        
        return jsonify({"ok": True, "old_status": old_status, "new_status": new_status,
                        "catalog": catalog_result})
    except Exception as e:
        log.error("api_pc_change_status error pcid=%s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/pricecheck/<pcid>")
@auth_required
def pricecheck_detail(pcid):
    try:
        return _pricecheck_detail_inner(pcid)
    except Exception as e:
        log.exception("Price check detail crashed for %s", pcid)
        return f"<h2>Error loading price check {pcid}</h2><pre>{type(e).__name__}: {e}</pre><p><a href='/'>← Home</a></p>", 500


def _pricecheck_detail_inner(pcid):
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        flash("Price Check not found", "error"); return redirect("/")

    import copy as _copy
    # CRITICAL: deep copy for rendering — never mutate cached objects.
    # _load_price_checks() has a 30s in-memory cache. If we mutate items here
    # (description cleaning line ~268, link promotion line ~348, line_number
    # assignment line ~704), those changes persist in the cache and get written
    # to DB on the next /save-prices call — causing "data replaced without
    # prompt" where just OPENING a PC page silently changes stored data.
    items = _copy.deepcopy(pc.get("items") or [])
    header = _copy.deepcopy((pc.get("parsed") or {}).get("header") or {})

    items_html = ""
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            item = {"description": str(item), "qty": 1, "pricing": {}}
        p = item.get("pricing") or {}
        # Clean description for display (strip font specs, dimensions, etc.)
        # ONLY auto-clean on first load (when description_raw hasn't been set yet).
        # Once description_raw exists, the user may have edited description — don't overwrite.
        raw_desc = item.get("description_raw") or ""
        if PRICE_CHECK_AVAILABLE and not raw_desc and item.get("description", ""):
            # First time: clean the parsed description, save original as description_raw
            original = item.get("description", "")
            cleaned = clean_description(original)
            if cleaned != original:
                item["description_raw"] = original
                item["description"] = cleaned
        display_desc = item.get("description") or raw_desc or ""
        # Cost sources (ensure numeric types — JSON data can have strings)
        def _safe_float(v, default=0):
            if v is None: return default
            try: return float(v)
            except (ValueError, TypeError): return default
        
        amazon_cost = _safe_float(p.get("amazon_price"), None)
        scprs_cost = _safe_float(p.get("scprs_price"), None)
        # Best available cost
        unit_cost = _safe_float(p.get("unit_cost")) or amazon_cost or scprs_cost or 0
        # Markup and final price
        markup_pct = _safe_float(p.get("markup_pct"), 25)
        final_price = _safe_float(p.get("recommended_price")) or (round(unit_cost * (1 + markup_pct/100), 2) if unit_cost else 0)

        # Type-safe qty (could be string, None, or already int/float)
        _raw_qty = item.get("qty", 1)
        try:
            qty = int(float(_raw_qty)) if _raw_qty else 1
        except (ValueError, TypeError):
            qty = 1
        qpu = item.get("qty_per_uom", 1)

        amazon_str = f"${amazon_cost:.2f}" if amazon_cost else "—"
        amazon_data = f'data-amazon="{amazon_cost:.2f}"' if amazon_cost else 'data-amazon="0"'
        scprs_str = f"${scprs_cost:.2f}" if scprs_cost else "—"
        cost_str = f"{unit_cost:.2f}" if unit_cost else ""
        final_str = f"{final_price:.2f}" if final_price else ""
        ext = f"${final_price * qty:.2f}" if final_price else "—"

        # Amazon match link + ASIN
        title = (p.get("amazon_title") or "")[:40]
        url = p.get("amazon_url", "")
        asin = p.get("amazon_asin", "")
        link_parts = []
        if url and title:
            link_parts.append(f'<a href="{url}" target="_blank" title="{p.get("amazon_title","")}">{title}</a>')
        if asin:
            link_parts.append(f'<span style="color:#58a6ff;font-size:13px;font-family:JetBrains Mono,monospace">ASIN: {asin}</span>')
        # Web search source
        web_src = p.get("web_source", "")
        web_title = p.get("web_title", "")
        web_url = p.get("web_url", "")
        if web_src and not link_parts:
            if web_url:
                link_parts.append(f'<a href="{web_url}" target="_blank" style="font-size:14px;color:#f0883e">{web_src}: {web_title[:35]}</a>')
            else:
                link_parts.append(f'<span style="color:#f0883e;font-size:14px">{web_src}</span>')
        # Catalog match
        cat_match = p.get("catalog_match", "")
        if cat_match and not link_parts:
            link_parts.append(f'<span style="color:#3fb950;font-size:14px">📦 {cat_match[:40]}</span>')
        link = "<br>".join(link_parts) if link_parts else "—"

        # SCPRS confidence indicator
        scprs_conf = _safe_float(p.get("scprs_confidence"), 0)
        scprs_badge = ""
        if scprs_cost:
            color = "#3fb950" if scprs_conf > 0.7 else ("#d29922" if scprs_conf > 0.4 else "#8b949e")
            scprs_badge = f' <span style="color:{color};font-size:13px" title="Confidence: {scprs_conf:.0%}">●</span>'

        # Confidence grade if scored
        conf = item.get("confidence") or {}
        if not isinstance(conf, dict): conf = {}
        grade = conf.get("grade", "")
        grade_color = {"A": "#3fb950", "B": "#58a6ff", "C": "#d29922", "F": "#f85149"}.get(grade, "#8b949e")
        grade_html = f'<span style="color:{grade_color};font-weight:bold">{grade}</span>' if grade else "—"

        # Per-item profit
        item_profit = round((final_price - unit_cost) * qty, 2) if (final_price and unit_cost) else 0
        profit_color = "#3fb950" if item_profit > 0 else ("#f85149" if item_profit < 0 else "#8b949e")
        profit_str = f'<span style="color:{profit_color}">${item_profit:.2f}</span>' if (final_price and unit_cost) else "—"
        
        # Item link — check item_link first, then pricing sub-fields
        item_link = item.get("item_link") or p.get("web_url") or p.get("catalog_url") or p.get("amazon_url") or ""
        item_supplier = item.get("item_supplier") or p.get("catalog_best_supplier") or p.get("web_source") or ""
        # Promote from pricing to item level so Save & Generate persists it
        if item_link and not item.get("item_link"):
            item["item_link"] = item_link
        if item_supplier and not item.get("item_supplier"):
            item["item_supplier"] = item_supplier
        link_display = f'<a href="{item_link}" target="_blank" style="font-size:14px;color:#58a6ff;word-break:break-all">{item_supplier or item_link[:30]}</a>' if item_link else ""
        supplier_badge = f'<span style="font-size:13px;color:#8b949e;display:block;margin-top:1px">{item_supplier}</span>' if item_supplier else ""
        # Price history toggle link for this item
        _ph_num = (item.get("mfg_number") or p.get("mfg_number") or p.get("manufacturer_part") or "").strip()
        _ph_num_safe = str(_ph_num).replace("'", "\\'").replace('"', '&quot;')
        ph_link = f' <a onclick="togglePriceHistory(\'{pcid}\',\'{_ph_num_safe}\',this)" style="cursor:pointer;color:#8b949e;font-size:12px;margin-left:4px">&#x25b8; Price history</a>' if _ph_num else ""

        # ── Unified Sources column: all price sources as compact chips ──
        sources = []  # list of (price, label, url, color, is_preferred)
        known_supplier = (item.get("item_supplier") or "").lower()  # supplier from pasted URL
        cat_best_sup = (p.get("catalog_best_supplier") or "").lower()

        if scprs_cost:
            scprs_conf_str = f" ({scprs_conf:.0%})" if scprs_conf else ""
            sources.append((scprs_cost, f"SCPRS{scprs_conf_str}", "", "#3fb950", True))
        if amazon_cost:
            a_url = p.get("amazon_url", "")
            # Detect actual source from URL — don't assume Amazon
            a_source = "Amazon"
            if a_url:
                _domain = ""
                try:
                    from urllib.parse import urlparse
                    _domain = urlparse(a_url).hostname or ""
                except Exception:
                    pass
                if "walmart" in _domain: a_source = "Walmart"
                elif "ebay" in _domain: a_source = "eBay"
                elif "staples" in _domain: a_source = "Staples"
                elif "grainger" in _domain: a_source = "Grainger"
                elif "mckesson" in _domain: a_source = "McKesson"
                elif "henryschein" in _domain: a_source = "Henry Schein"
                elif "boundtree" in _domain: a_source = "Bound Tree"
                elif "medline" in _domain: a_source = "Medline"
                elif "officedepot" in _domain: a_source = "Office Depot"
                elif "uline" in _domain: a_source = "Uline"
                elif "amazon" not in _domain: a_source = _domain.replace("www.","").split(".")[0].title()
            a_label = a_source + (f" · {asin}" if asin and "amazon" in (a_url or "").lower() else "")
            # Preferred if we've used this supplier before
            a_pref = a_source.lower() in cat_best_sup or a_source.lower() in known_supplier or "amazon" in known_supplier
            sources.append((amazon_cost, a_label, a_url, "#ff9900", a_pref))
        web_price = _safe_float(p.get("web_price"), 0)
        if web_price and web_price != amazon_cost:
            w_src = p.get("web_source", "Web")[:20]
            w_pref = w_src.lower() in cat_best_sup or w_src.lower() in known_supplier
            sources.append((web_price, w_src, p.get("web_url", ""), "#d2a8ff", w_pref))
        cat_cost = _safe_float(p.get("catalog_cost")) or _safe_float(p.get("last_cost"), 0)
        cat_match = p.get("catalog_match", "")
        cat_pid = p.get("catalog_product_id")
        if cat_cost and cat_match:
            cat_url = p.get("catalog_url", p.get("best_supplier_url", ""))
            if not cat_url and cat_pid:
                cat_url = f"/catalog/{cat_pid}"
            cat_sup = p.get("catalog_best_supplier", "")
            cat_label = f"📦 {cat_sup}" if cat_sup else "📦 Catalog"
            sources.append((cat_cost, cat_label, cat_url, "#58a6ff", True))

        # Item link URL as a source (if user pasted a URL with a price)
        _item_link = item.get("item_link", "")
        _item_link_price = _safe_float(item.get("item_link_price"), 0)
        if _item_link and _item_link_price and _item_link_price not in [s[0] for s in sources]:
            _il_supplier = item.get("item_supplier", "Link")
            sources.append((_item_link_price, _il_supplier, _item_link, "#f59e0b", True))

        # Sort by price, preferred suppliers get a small boost (within 10% of cheapest = preferred wins)
        if sources:
            cheapest = min(s[0] for s in sources)
            def _sort_key(s):
                price, label, url, color, preferred = s
                # If preferred and within 10% of cheapest, rank it first
                if preferred and price <= cheapest * 1.10:
                    return (0, price)
                return (1, price)
            sources.sort(key=_sort_key)

        # Build source chips HTML
        source_chips = []
        for i_src, (sprice, slabel, surl, scolor, spref) in enumerate(sources):
            pref_icon = "★ " if spref else ""
            price_fmt = f"${sprice:.2f}"
            if surl:
                chip = f'<a href="{surl}" target="_blank" style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;font-size:13px;background:{scolor}15;border:1px solid {scolor}40;color:{scolor};text-decoration:none;white-space:nowrap;cursor:pointer" title="{slabel} · {price_fmt}">{pref_icon}<b>{price_fmt}</b> {slabel}</a>'
            else:
                chip = f'<span style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;font-size:13px;background:{scolor}15;border:1px solid {scolor}40;color:{scolor};white-space:nowrap" title="{slabel}">{pref_icon}<b>{price_fmt}</b> {slabel}</span>'
            # First source gets "Use" action
            if i_src == 0 and len(sources) > 1 and sprice != unit_cost:
                chip += f' <a href="#" onclick="document.querySelector(\'[name=cost_{idx}]\').value=\'{sprice:.2f}\';recalcRow({idx});recalcPC();return false" style="color:{scolor};font-size:13px;text-decoration:none" title="Use this price">⬇</a>'
            source_chips.append(chip)
        source_html = '<div style="display:flex;flex-wrap:wrap;gap:3px">' + ''.join(source_chips) + '</div>' if source_chips else '<span style="color:#484f58;font-size:14px">No sources</span>'

        # Per-item notes
        item_notes = item.get("notes") or ""
        notes_escaped = item_notes.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')

        # No-bid state
        no_bid = item.get("no_bid", False)
        bid_checked = "" if no_bid else "checked"
        row_opacity = "opacity:0.4" if no_bid else ""

        # Substitute item state
        is_sub = item.get("is_substitute", False)
        sub_checked = "checked" if is_sub else ""

        # MFG# display: use mfg_number or pricing mfg_number — NOT item_number (that's the line#)
        _raw_num = (item.get("mfg_number")
                    or p.get("mfg_number") or p.get("manufacturer_part") or "")
        _raw_num = str(_raw_num).strip()
        # Hide sequential row numbers (1-50 digit-only values) from MFG# field
        try:
            if _raw_num.isdigit() and 0 < int(_raw_num) <= 50:
                _raw_num = ""
        except (ValueError, TypeError):
            pass
        mfg_display = _raw_num.replace('"', '&quot;')

        # Line item # from original 704 form
        line_num = item.get("row_index") or item.get("item_number", idx + 1)
        try:
            _ln = str(line_num).strip()
            # If item_number looks like a part number (not 1-50), fall back to row_index
            if not (_ln.replace('.','',1).isdigit() and 0 < float(_ln) <= 50):
                line_num = item.get("row_index", idx + 1)
        except (ValueError, TypeError):
            line_num = item.get("row_index", idx + 1)

        items_html += f"""<tr style="{row_opacity}" data-row="{idx}">
         <td style="text-align:center"><input type="checkbox" name="bid_{idx}" {bid_checked} onchange="toggleBid({idx},this)" style="width:18px;height:18px;cursor:pointer"></td>
         <td style="text-align:center;position:relative"><input type="number" name="linenum_{idx}" value="{line_num}" class="lockable-field" style="width:36px;text-align:center;font-weight:600;font-size:13px;color:#8b949e;font-family:'JetBrains Mono',monospace;background:transparent;border:1px solid transparent;padding:2px" onchange="autoSequenceLineNums({idx})" min="1">{'<button onclick=\"mergeUp('+str(idx)+');event.stopPropagation()\" title=\"Merge into item above\" style=\"position:absolute;top:-2px;right:-2px;background:#21262d;border:1px solid #30363d;border-radius:3px;color:#a78bfa;font-size:10px;cursor:pointer;padding:1px 3px;display:none\" class=\"merge-btn\">⬆</button>' if idx > 0 else ''}</td>
         <td><input type="text" name="itemnum_{idx}" value="{mfg_display}" class="text-in lockable-field" style="width:80px;text-align:center;font-weight:600;font-size:14px;font-family:'JetBrains Mono',monospace;padding:6px 4px" placeholder="MFG#" onblur="handleMfgInput({idx}, this)"></td>
         <td><input type="number" name="qty_{idx}" value="{qty}" class="num-in sm" style="width:55px" onchange="recalcPC()"><input type="hidden" name="qpu_{idx}" value="{qpu}"></td>
         <td><input type="text" name="uom_{idx}" value="{(item.get('uom') or 'EA').upper()}" class="text-in" style="width:45px;text-transform:uppercase;text-align:center;font-weight:600"></td>
         <td><textarea name="desc_{idx}" class="text-in" style="width:100%;min-height:38px;max-height:120px;overflow-y:auto;resize:vertical;font-family:inherit;font-size:13px;line-height:1.4;padding:6px 8px" title="{raw_desc.replace('"','&quot;').replace('<','&lt;')}" oninput="detectDescUrl({idx},this)" placeholder="Enter description or paste URL">{display_desc.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')}</textarea></td>
         <td style="text-align:center"><input type="checkbox" name="substitute_{idx}" {sub_checked} style="width:16px;height:16px;cursor:pointer;accent-color:#d29922" title="Check if quoting a replacement/substitute item — unlocks description editing" onchange="toggleSubstitute({idx},this)"></td>
         <td style="min-width:180px">
          <div style="display:flex;flex-direction:column;gap:3px">
           <div style="display:flex;gap:2px;align-items:center">
            <input type="text" name="link_{idx}" value="{item_link.replace(chr(34), '&quot;')}" placeholder="Paste supplier URL…" class="text-in" style="flex:1;font-size:14px;color:#58a6ff;padding:5px 7px" oninput="handleLinkInput({idx}, this)" onpaste="setTimeout(()=>handleLinkInput({idx},this),50)">
            <a href="{item_link}" target="_blank" id="linkopen_{idx}" onclick="return !!this.href && this.href!==''" style="display:{'flex' if item_link else 'none'};align-items:center;justify-content:center;width:28px;height:28px;border-radius:4px;background:#21262d;border:1px solid #30363d;color:#58a6ff;font-size:14px;text-decoration:none;flex-shrink:0" title="Open link">↗</a>
           </div>
           <div id="link_meta_{idx}" style="font-size:13px;color:#8b949e">{supplier_badge}{ph_link}</div>
          </div>
         </td>
         <td style="min-width:160px;max-width:220px;vertical-align:top;padding:6px 4px">{source_html}</td>
         <td><div class="currency-wrap"><input type="text" inputmode="decimal" name="cost_{idx}" value="{cost_str}" class="num-in" placeholder="0.00" oninput="sanitizePrice(this)" onchange="recalcRow({idx},true)" onblur="fmtCurrency(this)"></div></td>
         <td><input type="text" inputmode="numeric" name="markup_{idx}" value="{markup_pct}" class="num-in sm" style="width:48px" oninput="sanitizeInt(this)" onchange="recalcRow({idx},true)"><span style="color:#8b949e;font-size:13px">%</span></td>
         <td><div class="currency-wrap"><input type="text" inputmode="decimal" name="price_{idx}" value="{final_str}" class="num-in price-out" placeholder="0.00" oninput="sanitizePrice(this)" onchange="recalcPC()" onblur="fmtCurrency(this)"></div></td>
         <td class="ext" style="font-weight:600;font-size:14px">{ext}</td>
         <td class="profit" style="font-size:14px">{profit_str}</td>
        </tr>
        <tr class="notes-row" data-row="{idx}" style="display:{'table-row' if item_notes else 'none'}">
         <td colspan="14" style="padding:0 8px 6px 120px;border-top:none">
          <div style="display:flex;align-items:center;gap:6px">
           <span style="font-size:13px;color:#8b949e">📝</span>
           <input type="text" name="notes_{idx}" value="{notes_escaped}" placeholder="Add note (prints on quote)…" class="text-in" style="flex:1;font-size:14px;padding:3px 8px;color:#d2a8ff">
          </div>
         </td>
        </tr>"""

    download_html = ""
    if pc.get("output_pdf") and os.path.exists(pc.get("output_pdf", "")):
        fname = os.path.basename(pc["output_pdf"])
        download_html += f'<a href="/api/pricecheck/download/{fname}" class="btn btn-sm btn-g" style="font-size:13px">📥 Download 704</a>'
    if pc.get("reytech_quote_pdf") and os.path.exists(pc.get("reytech_quote_pdf", "")):
        qfname = os.path.basename(pc["reytech_quote_pdf"])
        qnum = pc.get("reytech_quote_number", "")
        download_html += f' <a href="/api/pricecheck/download/{qfname}" class="btn btn-sm" style="background:#1a3a5c;color:#fff;font-size:13px">📥 Quote {qnum}</a>'

    # Diagnostic link (small, unobtrusive)
    download_html += f' <a href="/pricecheck/{pcid}/diagnose" target="_blank" style="font-size:13px;color:#484f58;margin-left:8px" title="Check data integrity">🔍 diagnose</a>'

    # 45-day expiry from TODAY (not upload date)
    try:
        expiry = datetime.now() + timedelta(days=45)
        expiry_date = expiry.strftime("%m/%d/%Y")
    except Exception as e:
        log.debug("Suppressed: %s", e)
        expiry_date = (datetime.now() + timedelta(days=45)).strftime("%m/%d/%Y")
    today_date = datetime.now().strftime("%m/%d/%Y")

    # Delivery dropdown state
    saved_delivery = pc.get("delivery_option", "5-7 business days")
    preset_options = ("3-5 business days", "5-7 business days", "7-14 business days")
    is_custom = saved_delivery not in preset_options and saved_delivery != ""
    del_sel = {opt: ("selected" if saved_delivery == opt else "") for opt in preset_options}
    del_sel["custom"] = "selected" if is_custom else ""
    # Default to 5-7 if nothing saved
    if not any(del_sel.values()):
        del_sel["5-7 business days"] = "selected"
    custom_val = saved_delivery if is_custom else ""
    custom_display = "inline-block" if is_custom else "none"

    # Pre-compute next quote number preview
    next_quote_preview = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else ""
    
    profit_summary_json = _json.dumps(pc.get("profit_summary"), default=str).replace("</", "<\\/") if pc.get("profit_summary") else "null"
    # Build pipeline status tracker — simplified to 3 steps
    _status = pc.get('status', 'new')
    _display_map = {
        'new': 0, 'parsed': 0, 'parse_error': 0,
        'draft': 1, 'priced': 1, 'ready': 1, 'auto_drafted': 1,
        'quoted': 1, 'generated': 1, 'completed': 1, 'converted': 1,
        'sent': 2, 'pending_award': 2, 'won': 2,
        'lost': 2, 'expired': 2, 'no_response': 2,
        'dismissed': 2, 'archived': 2, 'duplicate': 2,
    }
    _steps = [('new', '🆕', 'New'), ('draft', '📝', 'Draft'), ('sent', '📨', 'Sent')]
    _reached = _display_map.get(_status, 0)
    _pip_parts = []
    for i, (step, icon, label) in enumerate(_steps):
        if i <= _reached:
            style = "padding:6px 14px;border-radius:8px;background:rgba(52,211,153,.12);color:#3fb950;font-size:15px;font-weight:600;cursor:pointer;border:none"
        else:
            style = "padding:6px 14px;border-radius:8px;background:#21262d;color:#484f58;font-size:15px;cursor:pointer;border:1px solid #30363d"
        _pip_parts.append(f"<button onclick=\"changeStatus('{step}')\" style=\"{style}\" title=\"Click to set status to {label}\">{icon} {label}</button>")
        if i < len(_steps) - 1:
            _pip_parts.append("<span style=\"color:#484f58;margin:0 6px;font-size:16px\">→</span>")
    # Add current raw status if it's not one of the 3 main steps
    if _status not in ('new', 'parsed', 'draft', 'ready', 'auto_drafted', 'quoted',
                        'generated', 'completed', 'converted', 'sent', 'pending_award', 'won'):
        _pip_parts.append(f"<span style=\"margin-left:8px;padding:4px 10px;border-radius:6px;background:rgba(248,113,113,.15);color:#f87171;font-size:14px;font-weight:600\">{_status}</span>")
    pipeline_html = "".join(_pip_parts)

    from src.api.render import render_page
    
    # ── Server-side CRM match (eliminates unreliable JS fetch) ──
    crm_data = {"matched": False, "candidates": [], "is_new": True}
    institution = header.get("institution", pc.get("institution", ""))
    inst_upper = institution.upper() if institution else ""
    if institution:
        try:
            # _load_customers and _guess_agency are in dashboard globals (exec'd from routes_crm)
            customers = _load_customers()
            # Exact match
            for c in customers:
                names = [c.get("display_name",""), c.get("company",""),
                         c.get("abbreviation",""), c.get("qb_name","")]
                if any(inst_upper == n.upper() for n in names if n):
                    crm_data = {"matched": True, "customer": c, "is_new": False}
                    break
            # Abbreviation expansion
            if not crm_data["matched"]:
                _ABBR = {"CSP":"California State Prison","SCC":"Sierra Conservation Center",
                         "CIM":"California Institution for Men","CIW":"California Institution for Women",
                         "CMC":"California Men's Colony","CMF":"California Medical Facility",
                         "CTF":"Correctional Training Facility","CHCF":"California Health Care Facility",
                         "SATF":"Substance Abuse Treatment Facility"}
                expanded = institution
                for abbr, full in _ABBR.items():
                    if inst_upper.startswith(abbr + "-") or inst_upper.startswith(abbr + " "):
                        suffix = institution[len(abbr):].lstrip("- ")
                        expanded = f"{full}, {suffix}" if suffix else full
                        break
                if expanded != institution:
                    exp_upper = expanded.upper()
                    for c in customers:
                        c_name = c.get("display_name", "").upper()
                        # Match if expanded name contains the customer name OR vice versa
                        if c_name and (c_name in exp_upper or exp_upper in c_name):
                            crm_data = {"matched": True, "customer": c, "is_new": False}
                            break
                    # Also try matching just the base facility name (without suffix)
                    if not crm_data["matched"]:
                        for abbr, full in _ABBR.items():
                            if inst_upper.startswith(abbr + "-") or inst_upper.startswith(abbr + " "):
                                base_upper = full.upper()
                                for c in customers:
                                    c_name = c.get("display_name", "").upper()
                                    if c_name and (base_upper in c_name or c_name in base_upper):
                                        crm_data = {"matched": True, "customer": c, "is_new": False}
                                        break
                                break
            # Fuzzy fallback
            if not crm_data["matched"]:
                q_tokens = set(inst_upper.split())
                scored = []
                for c in customers:
                    search_text = " ".join([c.get("display_name",""), c.get("company",""),
                                            c.get("abbreviation","")]).upper()
                    c_tokens = set(search_text.split())
                    overlap = len(q_tokens & c_tokens)
                    if overlap > 0:
                        scored.append((overlap / max(len(q_tokens), 1), c))
                scored.sort(key=lambda x: -x[0])
                candidates = [s[1] for s in scored[:5] if s[0] > 0.3]
                if candidates and scored[0][0] >= 0.6:
                    crm_data = {"matched": True, "customer": candidates[0], "is_new": False, "candidates": candidates[:3]}
                else:
                    crm_data = {"matched": False, "is_new": True, "candidates": candidates[:3],
                                "suggested_agency": _guess_agency(institution)}
        except Exception as e:
            log.debug("CRM match error: %s", e)
    
    # ── Server-side quote history ──
    # P2-E: Normalize institution name via resolver for better history matching
    try:
        from src.core.institution_resolver import resolve
        _resolved = resolve(institution)
        if _resolved.get("canonical"):
            institution = _resolved["canonical"]
            inst_upper = institution.upper()
    except ImportError:
        pass
    quote_history = []
    if institution and QUOTE_GEN_AVAILABLE:
        try:
            quotes = get_all_quotes()
            for qt in reversed(quotes):
                qt_inst = qt.get("institution", "").upper()
                from src.core.contracts import safe_match as _sm
                if _sm(inst_upper, qt_inst):
                    source_pc = qt.get("source_pc_id", "")
                    source_rfq = qt.get("source_rfq_id", "")
                    created = qt.get("created_at", "")
                    days_ago = ""
                    if created:
                        try:
                            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                            delta = datetime.now() - created_dt.replace(tzinfo=None)
                            days_ago = f"{delta.days}d ago" if delta.days > 0 else "today"
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                    quote_history.append({
                        "quote_number": qt.get("quote_number"),
                        "date": qt.get("date"),
                        "total": qt.get("total", 0),
                        "items_count": qt.get("items_count", 0),
                        "status": qt.get("status", "pending"),
                        "po_number": qt.get("po_number", ""),
                        "items_text": qt.get("items_text", ""),
                        "days_ago": days_ago,
                        "source_pc_url": f"/pricecheck/{source_pc}" if source_pc else "",
                        "source_rfq_url": f"/rfq/{source_rfq}" if source_rfq else "",
                        "quote_url": f"/quotes?q={qt.get('quote_number', '')}",
                    })
                    if len(quote_history) >= 10:
                        break
        except Exception as e:
            log.debug("Quote history error: %s", e)
    
    # ── Server-side tax rate ──
    tax_rate = 0.0725
    tax_source = "CA Default"
    ship_to_val = pc.get("ship_to", "")
    if ship_to_val:
        import re as _re
        zip_match = _re.search(r'\b(\d{5})\b', ship_to_val)
        if zip_match:
            try:
                from src.agents.tax_agent import get_tax_rate as _get_tax
                result = _get_tax(zip_code=zip_match.group(1))
                if result and result.get("rate"):
                    tax_rate = result["rate"]
                    tax_source = result.get("jurisdiction", "CDTFA")
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    
    # Sanitize JSON for safe embedding in <script type="application/json"> tags
    def _safe_json(obj):
        """Serialize to JSON and escape chars that could break script blocks."""
        s = _json.dumps(obj, default=str)
        return s.replace("</", "<\\/").replace("<!--", "<\\!--")
    crm_json = _safe_json(crm_data)
    history_json = _safe_json(quote_history)

    # Catalog count for match button
    try:
        from src.agents.product_catalog import get_catalog_stats
        _cat_stats = get_catalog_stats()
        catalog_count = _cat_stats.get("total_products", 0)
    except Exception:
        catalog_count = 0

    # Fix duplicate line numbers — always sequential
    for _i, _item in enumerate(items, start=1):
        _item["line_number"] = _i

    # Resolve existing generated PDF URLs for inline preview
    _existing_704_url = ""
    _existing_quote_url = ""
    _op = pc.get("original_pdf") or pc.get("output_pdf") or ""
    if _op and os.path.exists(_op):
        _existing_704_url = f"/api/pricecheck/download/{os.path.basename(_op)}"
    _qp = pc.get("reytech_quote_pdf") or ""
    if _qp and os.path.exists(_qp):
        _existing_quote_url = f"/api/pricecheck/download/{os.path.basename(_qp)}"

    html = render_page("pc_detail.html", active_page="PCs",
        pcid=pcid, pc=pc, items=items, items_html=items_html,
        download_html=download_html, expiry_date=expiry_date,
        header=header, custom_val=custom_val, custom_display=custom_display,
        del_sel=del_sel, next_quote_preview=next_quote_preview,
        today_date=today_date, profit_summary_json=profit_summary_json,
        pipeline_html=pipeline_html,
        crm_json=crm_json, history_json=history_json,
        tax_rate=tax_rate, tax_source=tax_source,
        catalog_count=catalog_count,
        existing_704_url=_existing_704_url,
        existing_quote_url=_existing_quote_url,
    )
    # Sanitize any surrogate chars that could cause UnicodeEncodeError
    return html.encode("utf-8", "replace").decode("utf-8")


@bp.route("/pricecheck/<pcid>/lookup")
@auth_required
def pricecheck_lookup(pcid):
    """Run product price lookup — SerpApi first, Claude web search fallback."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    found = 0
    source = "none"

    # Try 1: SerpApi (if available + key set)
    if PRICE_CHECK_AVAILABLE:
        try:
            parsed = pc.get("parsed", {})
            parsed = lookup_prices(parsed)
            pc["parsed"] = parsed
            # MERGE pricing into existing items (don't replace — preserves item_link, notes, etc.)
            fresh_items = parsed.get("line_items", [])
            existing_items = pc.get("items", [])
            if existing_items and len(existing_items) == len(fresh_items):
                for i, fresh in enumerate(fresh_items):
                    fp = fresh.get("pricing", {})
                    if fp:
                        if not existing_items[i].get("pricing"):
                            existing_items[i]["pricing"] = {}
                        existing_items[i]["pricing"].update(fp)
                    # Copy new fields that don't overwrite user edits
                    for k in ("mfg_number", "description_raw"):
                        if fresh.get(k) and not existing_items[i].get(k):
                            existing_items[i][k] = fresh[k]
                pc["items"] = existing_items
            else:
                pc["items"] = fresh_items
            found = sum(1 for i in pc["items"] if i.get("pricing", {}).get("amazon_price"))
            source = "serpapi"
        except Exception as e:
            log.debug("SerpApi lookup failed: %s", e)

    # Try 2: Claude web search (if SerpApi found nothing or unavailable)
    if found == 0:
        try:
            from src.agents.web_price_research import web_search_for_pc
            result = web_search_for_pc(pcid)
            if result.get("ok") and result.get("found", 0) > 0:
                # Reload PC after web_search_for_pc saved it
                pcs = _load_price_checks()
                pc = pcs.get(pcid, pc)
                found = result["found"]
                source = "claude_web"
        except ImportError:
            log.debug("web_price_research not available")
        except Exception as e:
            log.debug("Claude web search failed: %s", e)

    if found > 0:
        # Auto-populate item_link from pricing source URLs if not already set
        for item in pc.get("items", []):
            if item.get("item_link"):
                continue  # User already pasted a URL, don't overwrite
            p = item.get("pricing", {})
            best_url = (p.get("amazon_url") or p.get("web_search_url") or "").strip()
            if best_url:
                item["item_link"] = best_url
                # Detect supplier from the URL
                try:
                    from src.agents.item_link_lookup import detect_supplier
                    item["item_supplier"] = detect_supplier(best_url)
                except Exception:
                    # Fallback: guess supplier from URL domain
                    if "amazon.com" in best_url:
                        item["item_supplier"] = "Amazon"
        _transition_status(pc, "draft", actor="user", notes=f"Prices found via {source}")
        _save_single_pc(pcid, pc)

    return jsonify({"ok": True, "found": found, "total": len(pc.get("items", [])),
                    "source": source})


@bp.route("/pricecheck/<pcid>/scprs-lookup")
@auth_required
def pricecheck_scprs_lookup(pcid):
    """Run SCPRS Won Quotes lookup for all items."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    found = 0
    if PRICING_ORACLE_AVAILABLE:
        for item in items:
            try:
                matches = find_similar_items(
                    item_number=item.get("item_number", ""),
                    description=item.get("description", ""),
                )
                if matches:
                    best = matches[0]
                    quote = best.get("quote", best)
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    scprs_price = quote.get("unit_price")
                    item["pricing"]["scprs_price"]      = scprs_price
                    item["pricing"]["scprs_match"]      = quote.get("description", "")[:60]
                    item["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    item["pricing"]["scprs_source"]     = quote.get("source", "scprs_kb")
                    item["pricing"]["scprs_po"]         = quote.get("po_number", "")
                    # Propagate part number from SCPRS match to mfg_number
                    scprs_item_num = quote.get("item_number", "")
                    if scprs_item_num and not item.get("mfg_number"):
                        item["mfg_number"] = scprs_item_num
                    # GAP 4 FIX: record this match to price_history
                    if scprs_price and scprs_price > 0:
                        try:
                            from src.core.db import record_price as _rp3
                            _rp3(
                                description=item.get("description", ""),
                                unit_price=float(scprs_price),
                                source="scprs_kb_match",
                                part_number=str(item.get("item_number", "") or ""),
                                agency=pc.get("institution", ""),
                                price_check_id=pcid,
                                notes=f"conf={best.get('match_confidence',0):.2f}|po={quote.get('po_number','')}",
                            )
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                        # Write to catalog so future auto-price finds it
                        try:
                            from src.agents.product_catalog import add_to_catalog, init_catalog_db
                            init_catalog_db()
                            add_to_catalog(
                                description=item.get("description", ""),
                                part_number=str(item.get("item_number", "") or ""),
                                cost=float(scprs_price),
                                sell_price=0,
                                source="scprs_kb_match",
                            )
                        except Exception:
                            pass
                    found += 1
            except Exception as e:
                log.error(f"SCPRS lookup error: {e}")

    _sync_pc_items(pc, items)
    _save_single_pc(pcid, pc)
    return jsonify({"ok": True, "found": found, "total": len(items)})


@bp.route("/pricecheck/<pcid>/rescan-mfg", methods=["POST"])
@auth_required
def pricecheck_rescan_mfg(pcid):
    """Re-scan this PC's source PDF and items to extract MFG/part/reference numbers."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    if not items:
        items = pc.get("parsed", {}).get("line_items", [])
    updated = 0

    # Step 1: Re-parse source PDF if available (to get substituted column)
    source_pdf = pc.get("source_pdf", "")
    if source_pdf and os.path.exists(source_pdf):
        try:
            from src.forms.price_check import parse_ams704
            fresh = parse_ams704(source_pdf)
            fresh_items = fresh.get("line_items", [])
            for fi in fresh_items:
                row_idx = fi.get("row_index", 0)
                for item in items:
                    if item.get("row_index") == row_idx:
                        if fi.get("substituted") and not item.get("substituted"):
                            item["substituted"] = fi["substituted"]
                        if fi.get("mfg_number"):
                            item["mfg_number"] = fi["mfg_number"]
                            updated += 1
                        break
        except Exception as e:
            log.debug("Rescan PDF %s: %s", pcid, e)

    # Step 2: Run extraction on all items that still lack a real part number
    from src.forms.price_check import extract_item_numbers, _is_sequential_number
    for item in items:
        current_mfg = (item.get("mfg_number") or "").strip()
        if current_mfg:
            continue  # Already has a real MFG number
        pn = extract_item_numbers(item)
        if pn:
            item["mfg_number"] = pn
            updated += 1

    _sync_pc_items(pc, items)
    _save_single_pc(pcid, pc)

    return jsonify({"ok": True, "updated": updated, "total": len(items)})


@bp.route("/pricecheck/<pcid>/client-error", methods=["POST"])
@auth_required
def pricecheck_client_error(pcid):
    """Receive JS errors from the PC detail page for server-side logging."""
    data = request.get_json(force=True, silent=True) or {}
    errors = data.get("errors", [])
    log_entries = data.get("log", [])
    if errors:
        log.warning("PC %s CLIENT JS ERRORS: %s", pcid, 
                     "; ".join(e.get("msg","?") for e in errors[:5]))
    if log_entries:
        log.info("PC %s client log: %s", pcid, " → ".join(log_entries[:20]))
    return jsonify({"ok": True})


@bp.route("/pricecheck/<pcid>/rename", methods=["POST"])
@auth_required
def pricecheck_rename(pcid):
    """Rename a price check's display number."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    new_name = data.get("pc_number", "").strip()
    if not new_name:
        return jsonify({"ok": False, "error": "Name cannot be empty"})
    pcs[pcid]["pc_number"] = new_name
    _save_single_pc(pcid, pc)
    log.info("RENAME PC %s → %s", pcid, new_name)
    return jsonify({"ok": True, "pc_number": new_name})


@bp.route("/pricecheck/<pcid>/diagnose")
@auth_required
def pricecheck_diagnose(pcid):
    """Diagnostic endpoint — checks data integrity for a PC."""
    diag = {"pcid": pcid, "checks": [], "errors": []}

    # 1. Can we load the PC?
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        diag["errors"].append("PC not found in price_checks.json")
        return jsonify(diag)
    diag["checks"].append(f"PC loaded: {pc.get('pc_number','?')}")

    # 2. Items state
    items = pc.get("items", [])
    diag["item_count"] = len(items)
    diag["checks"].append(f"{len(items)} items")
    
    # 3. Check each item for key fields
    item_issues = []
    for i, it in enumerate(items):
        issues = []
        desc = (it.get("description") or "").strip()
        if not desc:
            issues.append("no description")
        mfg = it.get("mfg_number", "")
        cost = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost")
        price = it.get("unit_price") or it.get("pricing", {}).get("recommended_price")
        row_idx = it.get("row_index")
        if not row_idx:
            issues.append(f"no row_index (fill_ams704 will SKIP this item)")
        if not price:
            issues.append("no price (fill_ams704 will SKIP this item)")
        if issues:
            item_issues.append(f"item[{i}] '{desc[:30]}': {', '.join(issues)}")
        else:
            diag["checks"].append(f"item[{i}] OK: desc='{desc[:30]}' mfg='{mfg}' cost={cost} price={price} row={row_idx}")
    if item_issues:
        diag["item_issues"] = item_issues
        diag["errors"].extend(item_issues)

    # 4. parsed dict state
    parsed = pc.get("parsed")
    if not parsed:
        diag["errors"].append("pc['parsed'] is MISSING — fill_ams704 will get no items!")
    else:
        p_items = parsed.get("line_items", [])
        diag["checks"].append(f"parsed.line_items: {len(p_items)} items")
        if len(p_items) != len(items):
            diag["errors"].append(f"DESYNC: pc.items has {len(items)} but parsed.line_items has {len(p_items)}")

    # 5. Source PDF exists?
    src = pc.get("source_pdf", "")
    if src and os.path.exists(src):
        diag["checks"].append(f"source_pdf exists: {os.path.basename(src)}")
        # Dump PDF field names and values for debugging parse issues
        try:
            from pypdf import PdfReader as _PR
            _r = _PR(src)
            _f = _r.get_fields() or {}
            pdf_fields = {}
            for fn, fv in sorted(_f.items()):
                val = fv.get("/V", "") if isinstance(fv, dict) else ""
                if val:
                    pdf_fields[fn] = str(val)[:80]
            diag["pdf_fields_with_data"] = pdf_fields
            diag["pdf_field_count"] = len(_f)
            diag["checks"].append(f"PDF has {len(_f)} fields, {len(pdf_fields)} with data")
        except Exception as e:
            diag["errors"].append(f"PDF field read error: {e}")
    elif src:
        diag["errors"].append(f"source_pdf MISSING: {src}")
    else:
        diag["errors"].append("no source_pdf set")

    # 6. Catalog DB state
    try:
        from src.agents.product_catalog import init_catalog_db, _get_conn
        init_catalog_db()
        conn = _get_conn()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(product_catalog)").fetchall()}
        count = conn.execute("SELECT COUNT(*) FROM product_catalog").fetchone()[0]
        conn.close()
        has_search_tokens = "search_tokens" in cols
        diag["catalog"] = {"count": count, "has_search_tokens": has_search_tokens, "columns": sorted(cols)}
        if not has_search_tokens:
            diag["errors"].append("Catalog DB MISSING search_tokens column!")
        else:
            diag["checks"].append(f"Catalog: {count} products, search_tokens=OK")
    except Exception as e:
        diag["errors"].append(f"Catalog DB error: {e}")

    # 7. DATA_DIR info
    try:
        from src.core.paths import DATA_DIR as _dd
        diag["data_dir"] = _dd
        diag["data_dir_writable"] = os.access(_dd, os.W_OK)
        diag["data_dir_files"] = len(os.listdir(_dd))
    except Exception as e:
        diag["errors"].append(f"DATA_DIR error: {e}")

    diag["ok"] = len(diag["errors"]) == 0
    return jsonify(diag)


@bp.route("/pricecheck/<pcid>/save-prices", methods=["POST"])
@auth_required
def pricecheck_save_prices(pcid):
    """Save manually edited prices, costs, and markups from the UI."""
    try:
        return _do_save_prices(pcid)
    except Exception as e:
        log.error("SAVE-PRICES %s CRASHED: %s", pcid, e)
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": f"Server error: {e}"})


def _validate_item_field(field_type, val):
    """Validate and sanitize a single PC item field value.
    Delegates to shared src.core.validation module.
    Returns (sanitized_value, error_string_or_None)."""
    from src.core.validation import (
        validate_price, validate_cost, validate_markup, validate_qty,
        validate_text, validate_url, validate_short_text, validate_bool, validate_int
    )
    _dispatch = {
        "price": validate_price,
        "cost": validate_cost,
        "markup": validate_markup,
        "qty": validate_qty,
    }
    if field_type in _dispatch:
        return _dispatch[field_type](val)
    elif field_type == "desc":
        return validate_text(val, max_len=5000)
    elif field_type == "link":
        return validate_url(val)
    elif field_type in ("uom", "itemno", "itemnum"):
        return validate_short_text(val, max_len=50)
    elif field_type == "notes":
        return validate_text(val, max_len=2000)
    elif field_type == "linenum":
        return validate_int(val, min_val=0, max_val=999, default=0)
    elif field_type in ("bid", "substitute"):
        return validate_bool(val)
    elif field_type == "qpu":
        return validate_int(val, min_val=1, max_val=9999, default=1)
    elif field_type == "linkopen":
        return (None, None)  # UI-only, intentionally dropped
    else:
        return (val, None)


def _do_save_prices(pcid):
    """Inner save handler — separated so exceptions always return JSON."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)

    # If not in JSON, try to recover from SQLite
    if not pc:
        log.warning("SAVE-PRICES %s: PC not in JSON, trying DAL recovery...", pcid)
        try:
            from src.core.dal import get_pc as _dal_get_pc
            _recovered = _dal_get_pc(pcid)
            if _recovered:
                pc = {
                    "id": pcid,
                    "pc_number": _recovered.get("pc_number") or _recovered.get("quote_number") or pcid,
                    "institution": _recovered.get("institution") or _recovered.get("agency") or "",
                    "requestor": _recovered.get("requestor") or "",
                    "items": _recovered.get("items") if isinstance(_recovered.get("items"), list) else [],
                    "status": _recovered.get("status") or "parsed",
                    "created_at": _recovered.get("created_at") or "",
                    "source": "recovered_from_db",
                }
                pcs[pcid] = pc
                log.info("SAVE-PRICES %s: Recovered from DAL", pcid)
        except Exception as e:
            log.error("SAVE-PRICES %s: SQLite recovery failed: %s", pcid, e)

    if not pc:
        log.error("SAVE-PRICES %s: PC not found in JSON or DB", pcid)
        return jsonify({"ok": False, "error": "PC not found"})

    # ── Save revision snapshot BEFORE applying changes ──
    try:
        _save_pc_revision(pcid, pc, reason="edit")
    except Exception as e:
        log.debug("Revision snapshot failed (non-fatal): %s", e)

    # Use force=True to parse JSON even if Content-Type header is wrong
    data = request.get_json(force=True, silent=True) or {}
    if not data:
        log.error("SAVE-PRICES %s: Empty request body! Content-Type=%s, body=%s",
                  pcid, request.content_type, request.get_data(as_text=True)[:200])
        return jsonify({"ok": False, "error": "Empty request body"})

    items = pc.get("items", [])
    log.info("SAVE-PRICES %s: %d keys in data, %d existing items",
             pcid, len(data), len(items))
    
    # Ensure parsed dict exists (may be missing after SQLite restore or manual creation)
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": items}
    
    # Save tax state
    pc["tax_enabled"] = data.get("tax_enabled", False)
    pc["tax_rate"] = data.get("tax_rate", 0)
    pc["delivery_option"] = data.get("delivery_option", "5-7 business days")
    pc["custom_notes"] = data.get("custom_notes", "")
    pc["price_buffer"] = data.get("price_buffer", 0)
    pc["default_markup"] = data.get("default_markup", 25)
    if data.get("ship_to") is not None:
        pc["ship_to"] = data.get("ship_to", "")

    for key, val in data.items():
        try:
            if key in ("tax_enabled", "tax_rate"):
                continue
            parts = key.split("_", 1)
            if len(parts) != 2:
                continue
            field_type = parts[0]
            idx = int(parts[1])
            # Expand items list if new rows were added via UI
            while idx >= len(items):
                new_row_idx = len(items) + 1  # 1-based row index for PDF
                items.append({"item_number": "", "qty": 1, "uom": "ea",
                              "description": "", "pricing": {},
                              "row_index": new_row_idx})
            if 0 <= idx < len(items):
                if field_type in ("price", "cost", "markup"):
                    if not items[idx].get("pricing"):
                        items[idx]["pricing"] = {}
                    if field_type == "price":
                        v, _err = _validate_item_field("price", val)
                        if _err: log.warning("SAVE validation: item[%d] %s", idx, _err)
                        items[idx]["pricing"]["recommended_price"] = v if v else None
                        items[idx]["unit_price"] = v if v else None
                    elif field_type == "cost":
                        v, _err = _validate_item_field("cost", val)
                        if _err: log.warning("SAVE validation: item[%d] %s", idx, _err)
                        items[idx]["pricing"]["unit_cost"] = v if v else None
                        items[idx]["vendor_cost"] = v if v else None
                    elif field_type == "markup":
                        v, _err = _validate_item_field("markup", val)
                        items[idx]["pricing"]["markup_pct"] = v
                        items[idx]["markup_pct"] = v
                    # Recalculate derived profit fields whenever any of these change
                    it = items[idx]
                    vc = it.get("vendor_cost") or it["pricing"].get("unit_cost") or 0
                    up = it.get("unit_price") or it["pricing"].get("recommended_price") or 0
                    qty = it.get("qty", 1) or 1
                    if up and vc:
                        it["profit_unit"] = round(up - vc, 4)
                        it["profit_total"] = round((up - vc) * qty, 2)
                        it["margin_pct"] = round((up - vc) / up * 100, 1) if up else 0
                    elif up and not vc:
                        # Cost unknown — can't calculate profit yet
                        it["profit_unit"] = None
                        it["profit_total"] = None
                        it["margin_pct"] = None
                elif field_type == "qty":
                    v, _err = _validate_item_field("qty", val)
                    items[idx]["qty"] = v
                    # Recalc profit_total when qty changes
                    it = items[idx]
                    vc = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
                    up = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
                    qty = it["qty"]
                    if up and vc:
                        it["profit_unit"] = round(up - vc, 4)
                        it["profit_total"] = round((up - vc) * qty, 2)
                elif field_type == "desc":
                    v, _ = _validate_item_field("desc", val)
                    items[idx]["description"] = v
                elif field_type == "uom":
                    items[idx]["uom"] = str(val).upper() if val else "EA"
                elif field_type in ("itemno", "itemnum"):
                    items[idx]["mfg_number"] = str(val) if val else ""
                elif field_type == "bid":
                    items[idx]["no_bid"] = not bool(val)
                elif field_type == "substitute":
                    items[idx]["is_substitute"] = bool(val)
                elif field_type == "link":
                    items[idx]["item_link"] = str(val).strip() if val else ""
                    # Auto-detect supplier from the URL when it's saved
                    if items[idx]["item_link"]:
                        try:
                            from src.agents.item_link_lookup import detect_supplier
                            items[idx]["item_supplier"] = detect_supplier(items[idx]["item_link"])
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                elif field_type == "notes":
                    items[idx]["notes"] = str(val).strip() if val else ""
                elif field_type == "linenum":
                    try:
                        items[idx]["row_index"] = int(float(val)) if val else idx + 1
                    except (ValueError, TypeError):
                        items[idx]["row_index"] = idx + 1
                elif field_type == "qpu":
                    try:
                        items[idx]["qty_per_uom"] = int(float(val)) if val else 1
                    except (ValueError, TypeError):
                        items[idx]["qty_per_uom"] = 1
                elif field_type == "linkopen":
                    pass  # UI-only toggle, no server-side storage needed
        except (ValueError, IndexError):
            pass

    _sync_pc_items(pc, items)

    # Auto-compute missing prices: if cost exists but price doesn't, apply markup
    for it in items:
        if it.get("no_bid"):
            continue
        cost = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
        price = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
        if cost > 0 and not price:
            markup = it.get("markup_pct") or it.get("pricing", {}).get("markup_pct") or 25
            computed_price = round(cost * (1 + markup / 100), 2)
            it["unit_price"] = computed_price
            if not it.get("pricing"):
                it["pricing"] = {}
            it["pricing"]["recommended_price"] = computed_price
            log.info("SAVE-PRICES auto-price: cost=%.2f × (1+%d%%) = %.2f",
                     cost, markup, computed_price)

    # Log what we're about to save for debugging
    for i, it in enumerate(items[:3]):  # first 3 items
        log.info("SAVE-PRICES %s item[%d]: desc='%s' mfg='%s' cost=%s price=%s link='%s'",
                 pcid, i,
                 (it.get("description") or "")[:40],
                 it.get("mfg_number", ""),
                 it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost"),
                 it.get("unit_price") or it.get("pricing", {}).get("recommended_price"),
                 (it.get("item_link") or "")[:40])

    # Compute PC-level profit summary — always kept current
    total_revenue = 0
    total_cost = 0
    total_profit = 0
    costed_items = 0
    for it in items:
        if it.get("no_bid"):
            continue
        up = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
        vc = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
        qty = it.get("qty", 1) or 1
        total_revenue += up * qty
        if vc:
            total_cost += vc * qty
            total_profit += (up - vc) * qty
            costed_items += 1
    pc["profit_summary"] = {
        "total_revenue":    round(total_revenue, 2),
        "total_cost":       round(total_cost, 2),
        "gross_profit":     round(total_profit, 2),
        "margin_pct":       round(total_profit / total_revenue * 100, 1) if total_revenue else 0,
        "costed_items":     costed_items,
        "total_items":      len([i for i in items if not i.get("no_bid")]),
        "fully_costed":     costed_items == len([i for i in items if not i.get("no_bid")]),
    }

    # Keep parsed.line_items in sync with items (source of truth)
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": items}
    else:
        pc["parsed"]["line_items"] = items

    # Save ONLY this PC — prevents background agents from overwriting user edits on other PCs
    try:
        from src.api.dashboard import _save_single_pc
        _save_single_pc(pcid, pc)
    except Exception as _single_e:
        log.warning("Single-PC save failed, falling back to full save: %s", _single_e)
        _save_price_checks(pcs)

    # ── Save items to product catalog — only when items are priced ──
    _priced_count = sum(1 for it in items if not it.get("no_bid") and (it.get("unit_price") or it.get("pricing", {}).get("recommended_price")))
    _should_sync_catalog = _priced_count >= len([it for it in items if not it.get("no_bid")]) * 0.5  # at least 50% priced
    if _should_sync_catalog:
        try:
            from src.agents.product_catalog import (
                match_item, add_to_catalog, add_supplier_price, init_catalog_db
            )
            init_catalog_db()
            _cat_added, _cat_updated = 0, 0
            for _item in items:
                if _item.get("no_bid"):
                    continue
                _desc = _item.get("description", "")
                _pn = str(_item.get("mfg_number") or _item.get("item_number") or "")
                _cost = _item.get("vendor_cost") or _item.get("pricing", {}).get("unit_cost") or 0
                _price = _item.get("unit_price") or _item.get("pricing", {}).get("recommended_price") or 0
                _supplier = _item.get("item_supplier", "")
                _uom = _item.get("uom", "EA")
                _url = _item.get("item_link", "")
                if not _desc or (not _cost and not _price):
                    continue
                cat_matches = match_item(_desc, _pn, top_n=1)
                if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                    pid = cat_matches[0]["id"]
                    if _cost > 0 and _supplier:
                        add_supplier_price(pid, _supplier, _cost, url=_url)
                    # Update URL on existing catalog entry if we have one
                    if _url:
                        try:
                            from src.agents.product_catalog import _get_conn
                            conn = _get_conn()
                            conn.execute(
                                "UPDATE product_catalog SET photo_url=COALESCE(NULLIF(photo_url,''),?) WHERE id=?",
                                (_url, pid))
                            conn.commit(); conn.close()
                        except Exception:
                            pass
                    _cat_updated += 1
                else:
                    pid = add_to_catalog(
                        description=_desc, part_number=_pn,
                        cost=_cost if _cost > 0 else 0,
                        sell_price=_price if _price > 0 else 0,
                        supplier_name=_supplier, uom=_uom,
                        supplier_url=_url,
                        source=f"pc_{pcid}",
                    )
                    if pid and _cost > 0 and _supplier:
                        add_supplier_price(pid, _supplier, _cost, url=_url)
                        _cat_added += 1
            if _cat_added or _cat_updated:
                log.info("PC %s catalog sync: +%d new, ~%d updated", pcid, _cat_added, _cat_updated)
        except Exception as _ce:
            log.debug("PC catalog sync: %s", _ce)

    # ── GAP 3 FIX: write confirmed prices to price_history + won_quotes ───────
    institution = pc.get("institution", "")
    pc_num      = pc.get("pc_number", "")
    try:
        from src.core.db import record_price as _rp
        from src.knowledge.won_quotes_db import ingest_scprs_result as _ingest_wq2
        for _item in items:
            if _item.get("no_bid"):
                continue
            _up   = _item.get("unit_price") or _item.get("pricing", {}).get("recommended_price") or 0
            _cost = _item.get("vendor_cost") or _item.get("pricing", {}).get("unit_cost") or 0
            _desc = _item.get("description", "")
            _qty  = _item.get("qty", 1) or 1
            _part = str(_item.get("item_number", "") or "")
            if _up > 0 and _desc:
                _rp(description=_desc, unit_price=float(_up), source="pc_confirmed",
                    part_number=_part, quantity=float(_qty),
                    agency=institution, price_check_id=pcid, notes=f"PC#{pc_num}")
            if _cost > 0 and _desc:
                _rp(description=_desc, unit_price=float(_cost), source="pc_vendor_cost",
                    part_number=_part, quantity=float(_qty),
                    agency=institution, price_check_id=pcid,
                    notes=f"PC#{pc_num} vendor cost")
                _ingest_wq2(
                    po_number=f"PC-{pc_num}",
                    item_number=_part,
                    description=_desc,
                    unit_price=float(_cost),
                    quantity=float(_qty),
                    department=institution,
                    award_date=datetime.now().strftime("%Y-%m-%d"),
                    source="pc_vendor_cost",
                )
    except Exception as _e:
        log.debug("price learning write: %s", _e)

    # ── CATALOG ENRICHMENT: feed PC items back into product catalog ─────
    _cat_result = _enrich_catalog_from_pc(pc)

    # ── STATUS TRANSITION: new/parsed → priced when items have prices ─────
    current_status = pc.get("status", "new")
    if current_status in ("new", "parsed", "parse_error"):
        bid_items = [i for i in items if not i.get("no_bid")]
        priced_items = [i for i in bid_items if (i.get("unit_price") or i.get("pricing", {}).get("recommended_price"))]
        if priced_items:
            _transition_status(pc, "draft", actor="user", 
                             notes=f"Saved: {len(priced_items)}/{len(bid_items)} items priced")
            _save_single_pc(pcid, pc)

    summary = pc.get("profit_summary", {})
    resp = {"ok": True, "profit_summary": summary}
    if _cat_result:
        resp["catalog"] = _cat_result
    return jsonify(resp)


def _sanitize_pc_items(pc):
    """Sanitize stored PC data to fix issues from older code versions.
    
    Fixes:
    - is_substitute defaults to False (not True) unless user explicitly checked it
    - mfg_number: clear if it looks like a year range or sequential number (not a real part#)
    - item_number: keep original parsed value but it won't be used for ITEM # column
      (fill_ams704 uses sequential counter instead)
    - Ensure parsed.line_items and items stay in sync
    """
    items = pc.get("items", [])
    for item in items:
        # is_substitute: only True if user explicitly checked the Sub? box
        # Old code may have auto-set this; force False unless clearly user-set
        if "is_substitute" not in item:
            item["is_substitute"] = False
        
        # Clean mfg_number: reject values that look like year ranges (2025-2026),
        # pure sequential numbers (1-50), or ISBN-like numbers from substituted field
        mfg = (item.get("mfg_number") or "").strip()
        if mfg:
            import re as _re
            # Year ranges like 2025-2026, 2024-2025
            if _re.match(r'^\d{4}-\d{4}$', mfg):
                item["mfg_number"] = ""
            # Pure digits 1-50 (sequential row numbers)
            elif mfg.isdigit() and 0 < int(mfg) <= 50:
                item["mfg_number"] = ""
        
        # Also clean mfg_number in pricing dict
        pricing = item.get("pricing", {})
        p_mfg = (pricing.get("mfg_number") or "").strip()
        if p_mfg:
            import re as _re
            if _re.match(r'^\d{4}-\d{4}$', p_mfg):
                pricing["mfg_number"] = ""
            elif p_mfg.isdigit() and 0 < int(p_mfg) <= 50:
                pricing["mfg_number"] = ""
    
    # Keep parsed.line_items in sync
    pc["items"] = items
    if "parsed" in pc:
        pc["parsed"]["line_items"] = items


def _enrich_catalog_from_pc(pc):
    """Write all PC line items to the product catalog.
    
    Called on Save, Generate 704, and Generate Quote.
    - Existing catalog items get updated (pricing, times_quoted)
    - New items get added (description alone is enough)
    - MFG# is used for matching (not item_number which is just a row number)
    - Records full quote context: agency, institution, qty, PC#, URL
    
    Returns: {"updated": int, "added": int} or None on error
    """
    try:
        from src.agents.product_catalog import (
            match_item as _cat_match, update_product_pricing as _cat_update,
            add_supplier_price as _cat_add_sup, init_catalog_db as _cat_init,
            add_to_catalog as _cat_add, record_catalog_quote as _cat_record,
        )
        _cat_init()
    except Exception as e:
        log.warning("catalog enrichment: import failed: %s", e)
        return None

    items = pc.get("items", [])
    institution = pc.get("institution", "")
    agency = pc.get("agency", "") or institution
    pc_num = pc.get("pc_number", "")
    pcid = pc.get("id", "")
    added = 0
    updated = 0
    for item in items:
        if item.get("no_bid"):
            continue
        desc = (item.get("description") or "").strip()
        mfg = str(item.get("mfg_number") or "").strip()
        up = item.get("unit_price") or item.get("pricing", {}).get("recommended_price") or 0
        cost = item.get("vendor_cost") or item.get("pricing", {}).get("unit_cost") or 0
        scprs = item.get("pricing", {}).get("scprs_price") or item.get("scprs_last_price") or 0
        amazon = item.get("pricing", {}).get("amazon_price") or item.get("amazon_price") or 0
        qty = item.get("qty", 1) or 1
        link = (item.get("item_link") or "").strip()
        supplier = (item.get("item_supplier") or "").strip()
        uom = (item.get("uom") or "EA").upper()
        if not desc and not mfg:
            continue
        try:
            matches = _cat_match(desc, mfg, top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.60:
                pid = matches[0]["id"]
                updates = {"times_quoted": (matches[0].get("times_quoted") or 0) + 1}
                if up > 0:
                    updates["last_sold_price"] = float(up)
                    updates["last_sold_date"] = datetime.now().isoformat()
                if cost > 0:
                    updates["sell_price"] = float(up) if up > 0 else None
                    updates["cost"] = float(cost)
                if scprs and scprs > 0:
                    updates["scprs_last_price"] = float(scprs)
                    updates["scprs_last_date"] = datetime.now().isoformat()
                    updates["scprs_agency"] = agency
                if amazon and amazon > 0:
                    updates["web_lowest_price"] = float(amazon)
                    updates["web_lowest_source"] = "Amazon"
                    updates["web_lowest_date"] = datetime.now().isoformat()
                _cat_update(pid, **updates)
                # Record supplier with URL (even without cost — URL is valuable)
                if supplier and link:
                    _cat_add_sup(pid, supplier, float(cost) if cost > 0 else 0, url=link)
                elif link and cost > 0:
                    _cat_add_sup(pid, "Web", float(cost), url=link)
                # Record full price events for history
                if up > 0:
                    _cat_record(pid, "quoted", float(up), quantity=float(qty),
                               source="pc_save", agency=agency, institution=institution,
                               quote_number=pc_num, pc_id=pcid, supplier_url=link)
                if cost > 0:
                    _cat_record(pid, "cost", float(cost), quantity=float(qty),
                               source="pc_save", agency=agency, institution=institution,
                               quote_number=pc_num, pc_id=pcid, supplier_url=link)
                updated += 1
            else:
                # New item — catalog it even without pricing
                new_pid = _cat_add(
                    description=desc,
                    part_number=mfg,
                    mfg_number=mfg,
                    cost=float(cost) if cost else 0,
                    sell_price=float(up) if up else 0,
                    supplier_url=link, supplier_name=supplier,
                    uom=uom,
                    source="pc_save"
                )
                if new_pid:
                    # Record full price events for the new item too
                    if up > 0:
                        _cat_record(new_pid, "quoted", float(up), quantity=float(qty),
                                   source="pc_save", agency=agency, institution=institution,
                                   quote_number=pc_num, pc_id=pcid, supplier_url=link)
                    if cost > 0:
                        _cat_record(new_pid, "cost", float(cost), quantity=float(qty),
                                   source="pc_save", agency=agency, institution=institution,
                                   quote_number=pc_num, pc_id=pcid, supplier_url=link)
                    # Also register supplier if URL given
                    if link and supplier:
                        _cat_add_sup(new_pid, supplier, float(cost) if cost > 0 else 0, url=link)
                    elif link:
                        _cat_add_sup(new_pid, "Web", float(cost) if cost > 0 else 0, url=link)
                    added += 1
        except Exception as e:
            log.debug("catalog enrichment item error: %s", e)

    log.info("catalog enrichment: updated=%d added=%d", updated, added)
    return {"updated": updated, "added": added}


@bp.route("/pricecheck/<pcid>/reparse", methods=["POST"])
@auth_required
def pricecheck_reparse(pcid):
    """Re-parse a price check from its source PDF, preserving user-edited pricing."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        # Try to recover from DB
        recovered = False
        try:
            from src.core.db import get_db
            with get_db() as conn:
                # Check rfq_files first
                row = conn.execute(
                    "SELECT data, filename FROM rfq_files WHERE rfq_id=? AND category='source' ORDER BY id DESC LIMIT 1",
                    (pcid,)
                ).fetchone()
                if not row:
                    # Also try email_attachments
                    try:
                        row = conn.execute(
                            "SELECT data, filename FROM email_attachments WHERE pc_id=? ORDER BY id DESC LIMIT 1",
                            (pcid,)
                        ).fetchone()
                    except Exception:
                        pass
                if row and row["data"]:
                    restore_dir = os.path.join(os.environ.get("DATA_DIR", "data"), "pc_pdfs")
                    os.makedirs(restore_dir, exist_ok=True)
                    source_pdf = os.path.join(restore_dir, row["filename"] or f"{pcid}.pdf")
                    with open(source_pdf, "wb") as _fw:
                        _fw.write(row["data"])
                    pc["source_pdf"] = source_pdf
                    recovered = True
                    log.info("REPARSE %s: recovered PDF from DB (%d bytes)", pcid, len(row["data"]))
        except Exception as _dbe:
            log.warning("REPARSE %s: DB recovery failed: %s", pcid, _dbe)

        if not recovered:
            return jsonify({"ok": False, "error": "Source PDF not found on disk or in DB. Upload the PDF manually."})

    from src.forms.price_check import parse_ams704

    # Save user-edited pricing data keyed by row_index
    old_items = pc.get("items", [])
    user_data = {}
    for item in old_items:
        row = item.get("row_index", 0)
        if row:
            user_data[row] = {
                "pricing": item.get("pricing", {}),
                "vendor_cost": item.get("vendor_cost"),
                "unit_price": item.get("unit_price"),
                "markup_pct": item.get("markup_pct"),
                "item_link": item.get("item_link", ""),
                "item_supplier": item.get("item_supplier", ""),
                "notes": item.get("notes", ""),
                "no_bid": item.get("no_bid", False),
                # Explicitly do NOT carry over is_substitute or mfg_number
            }

    # Re-parse from source PDF
    fresh = parse_ams704(source_pdf)
    if not fresh.get("line_items"):
        return jsonify({"ok": False, "error": "Re-parse found no line items"})

    # Merge user pricing back onto fresh items
    for item in fresh["line_items"]:
        row = item.get("row_index", 0)
        if row in user_data:
            ud = user_data[row]
            item["pricing"] = ud["pricing"]
            if ud.get("vendor_cost") is not None:
                item["vendor_cost"] = ud["vendor_cost"]
            if ud.get("unit_price") is not None:
                item["unit_price"] = ud["unit_price"]
            if ud.get("markup_pct") is not None:
                item["markup_pct"] = ud["markup_pct"]
            item["item_link"] = ud.get("item_link", "")
            item["item_supplier"] = ud.get("item_supplier", "")
            item["notes"] = ud.get("notes", "")
            item["no_bid"] = ud.get("no_bid", False)
        # Ensure is_substitute defaults to False on reparse
        item["is_substitute"] = False

    # Update PC with fresh parse
    pc["parsed"] = fresh
    _sync_pc_items(pc, fresh["line_items"])
    _save_single_pc(pcid, pc)

    log.info("REPARSE PC %s: %d items re-parsed from source PDF", pcid, len(fresh["line_items"]))
    return jsonify({"ok": True, "items": len(fresh["line_items"]),
                    "msg": f"Re-parsed {len(fresh['line_items'])} items from source PDF"})


@bp.route("/api/pricecheck/<pcid>/lookup-tax-rate", methods=["POST"])
@auth_required
def api_pc_lookup_tax_rate(pcid):
    """Look up CA sales tax rate from ship-to address for a PC."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    address = data.get("address") or pc.get("ship_to") or ""
    if not address:
        return jsonify({"ok": False, "error": "No address — enter Ship To first"})
    try:
        import re as _re_tax
        _zips = _re_tax.findall(r'\b(\d{5})\b', address)
        _d_zip = _zips[-1] if _zips else ""
        _city_m = (_re_tax.search(r',\s*([A-Za-z\s]+),?\s*[A-Z][A-Za-z]\.?\s*\d{5}', address) or
                   _re_tax.search(r',\s*([A-Za-z][A-Za-z\s]+?)\s*,\s*[A-Z]{2}', address))
        _d_city = _city_m.group(1).strip() if _city_m else ""
        if not _d_city and _d_zip:
            _cfz = _re_tax.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,?\s*[Cc][Aa]\.?\s*' + _d_zip, address)
            if _cfz: _d_city = _cfz.group(1).strip()
        from src.agents.tax_agent import get_tax_rate
        result = get_tax_rate(city=_d_city, zip_code=_d_zip)
        if result and result.get("rate"):
            rate_pct = round(result["rate"] * 100, 3)
            pc["tax_rate"] = rate_pct
            pc["tax_validated"] = True
            pc["tax_source"] = result.get("source", "")
            _save_single_pc(pcid, pc)
            return jsonify({"ok": True, "rate": rate_pct,
                "jurisdiction": result.get("jurisdiction", ""),
                "source": result.get("source", "")})
        return jsonify({"ok": False, "error": "Tax lookup returned no rate"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/pricecheck/<pcid>/upload-pdf", methods=["POST"])
@auth_required
def pricecheck_upload_pdf(pcid):
    """Upload a PDF to a PC and parse it. Use when source PDF is lost after deploy."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file"}), 400
    if not f.filename.lower().endswith('.pdf'):
        return jsonify({"ok": False, "error": "Only PDF files allowed"}), 400
    # Read and check size + magic bytes
    content = f.read()
    if len(content) > 10 * 1024 * 1024:
        return jsonify({"ok": False, "error": "File too large (10MB max)"}), 413
    if not content[:5].startswith(b'%PDF'):
        return jsonify({"ok": False, "error": "Invalid PDF file"}), 400
    # Reset stream for downstream use
    from io import BytesIO
    f.stream = BytesIO(content)
    f.seek(0)

    # Save to disk
    upload_dir = os.path.join(os.environ.get("DATA_DIR", "data"), "pc_pdfs")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = re.sub(r'[^a-zA-Z0-9_.\-]', '_', f.filename)
    pdf_path = os.path.join(upload_dir, f"{pcid}_{safe_name}")
    f.save(pdf_path)
    pc["source_pdf"] = pdf_path

    # Also save to DB for persistence across deploys
    try:
        from src.api.dashboard import save_rfq_file
        with open(pdf_path, "rb") as _pf:
            pdf_data = _pf.read()
        save_rfq_file(pcid, safe_name, "application/pdf", pdf_data,
                      category="source", uploaded_by="user")
        log.info("PC %s: saved uploaded PDF to DB (%d bytes)", pcid, len(pdf_data))
    except Exception as _e:
        log.warning("PC %s: DB save failed: %s", pcid, _e)

    # Parse
    from src.forms.price_check import parse_ams704
    result = parse_ams704(pdf_path)
    items = result.get("line_items", [])
    header = result.get("header", {})

    if items:
        pc["items"] = items
        pc["parsed"] = result
        # Fill header fields ONLY if user hasn't already entered data.
        # Protect user-entered fields (ship_to, delivery_location, institution, requestor)
        # from being overwritten by parsed PDF values.
        _protected_fields = {"ship_to", "delivery_location", "delivery_zip"}
        for hk, hv in header.items():
            if hv and not pc.get(hk) and hk not in _protected_fields:
                pc[hk] = hv
        # Only fill protected fields if they're truly empty (not user-entered)
        for _pf in _protected_fields:
            if header.get(_pf) and not pc.get(_pf):
                pc[_pf] = header[_pf]
        if not pc.get("requestor") and header.get("requestor"):
            pc["requestor"] = header["requestor"]
        if not pc.get("institution") and header.get("institution"):
            pc["institution"] = header["institution"]
        pc["status"] = "parsed"
        _sync_pc_items(pc, items)
        _save_single_pc(pcid, pc)
        try:
            from src.core.dal import save_pc as _dal_save_pc
            _dal_save_pc(pc)
        except Exception as _e:
            log.debug("DAL save_pc: %s", _e)
        log.info("PC %s: uploaded PDF parsed → %d items", pcid, len(items))
        # Auto-enrich with catalog, SCPRS, URL extraction
        try:
            _auto_enrich_pc(pcid)
        except Exception as _ae:
            log.warning("PC %s: auto-enrich failed: %s", pcid, _ae)
        from flask import flash
        flash(f"Parsed {len(items)} items from uploaded PDF", "success")
    else:
        _save_single_pc(pcid, pc)
        log.warning("PC %s: uploaded PDF parsed 0 items", pcid)
        from flask import flash
        flash("PDF uploaded but no items found — try vision parse", "error")

    return redirect(f"/pricecheck/{pcid}")


@bp.route("/pricecheck/<pcid>/generate", methods=["POST"])
@auth_required
def pricecheck_generate(pcid):
    """Generate completed Price Check PDF and ingest into Won Quotes KB (POST only — writes data)."""
    try:
        return _do_generate(pcid)
    except Exception as e:
        log.error("GENERATE %s CRASHED: %s", pcid, e)
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": f"Server error: {e}"})


def _do_generate(pcid):
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    from src.forms.price_check import fill_ams704

    # ── Sanitize stored data before PDF generation ──
    _sanitize_pc_items(pc)

    # ALWAYS sync parsed.line_items from pc.items (the source of truth)
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": []}
    pc["parsed"]["line_items"] = pc.get("items", [])

    log.info("GENERATE %s: synced %d items from pc['items'] to parsed['line_items']",
             pcid, len(pc.get("items", [])))

    # Auto-compute missing prices before PDF generation.
    # Track whether we actually changed anything so we only save when needed.
    _auto_priced = 0
    for it in pc.get("items", []):
        cost = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
        price = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
        if cost > 0 and not price and not it.get("no_bid"):
            markup = it.get("markup_pct") or it.get("pricing", {}).get("markup_pct") or 25
            computed = round(cost * (1 + markup / 100), 2)
            it["unit_price"] = computed
            if not it.get("pricing"):
                it["pricing"] = {}
            it["pricing"]["recommended_price"] = computed
            _auto_priced += 1
    if _auto_priced:
        log.info("GENERATE %s: auto-computed %d missing prices", pcid, _auto_priced)
        _save_single_pc(pcid, pc)

    parsed = pc.get("parsed", {})
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        recovered = False
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT data, filename FROM rfq_files WHERE rfq_id=? AND category='source' ORDER BY id DESC LIMIT 1",
                    (pcid,)
                ).fetchone()
                if not row:
                    try:
                        row = conn.execute(
                            "SELECT data, filename FROM email_attachments WHERE pc_id=? ORDER BY id DESC LIMIT 1",
                            (pcid,)
                        ).fetchone()
                    except Exception:
                        pass
                if row and row["data"]:
                    restore_dir = os.path.join(DATA_DIR, "pc_pdfs")
                    os.makedirs(restore_dir, exist_ok=True)
                    source_pdf = os.path.join(restore_dir, row["filename"] or f"{pcid}.pdf")
                    with open(source_pdf, "wb") as _fw:
                        _fw.write(row["data"])
                    pc["source_pdf"] = source_pdf
                    _save_single_pc(pcid, pc)
                    recovered = True
                    log.info("GENERATE %s: recovered PDF from DB (%d bytes)", pcid, len(row["data"]))
        except Exception as _dbe:
            log.warning("GENERATE %s: DB recovery failed: %s", pcid, _dbe)

        if not recovered:
            return jsonify({"ok": False, "error": "Source PDF not found. Upload the 704 PDF (More → Upload PDF & Parse), then try again."})

    # Detailed logging: what exactly will fill_ams704 receive?
    _fill_items = parsed.get("line_items", [])
    log.info("GENERATE %s: %d items going to fill_ams704 (source: %s)",
             pcid, len(_fill_items), os.path.basename(source_pdf))
    for i, it in enumerate(_fill_items):
        log.info("  → item[%d]: row_idx=%s desc='%s' qty=%s uom=%s price=%s cost=%s mfg='%s'",
                 i, it.get("row_index"), (it.get("description") or "")[:50],
                 it.get("qty"), it.get("uom"),
                 it.get("unit_price") or it.get("pricing", {}).get("recommended_price"),
                 it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost"),
                 it.get("mfg_number", ""))

    pc_num = pc.get("pc_number", "") or ""
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip()) if pc_num.strip() else ""
    # Always include pcid to prevent filename collisions between PCs with same/empty pc_number
    safe_name = f"{safe_name}_{pcid}" if safe_name else pcid

    # Determine revision suffix: first download = no suffix, subsequent = _Revised, _Revised_2, etc.
    # Resets if pc_number changes (rename/reparse)
    _prev_gen_pcnum = pc.get("_gen_for_pcnum", "")
    _current_pcnum = pc.get("pc_number", "")
    if _prev_gen_pcnum != _current_pcnum:
        pc["_generate_count"] = 0
    gen_count = pc.get("_generate_count", 0) + 1
    pc["_generate_count"] = gen_count
    pc["_gen_for_pcnum"] = _current_pcnum
    if gen_count <= 1:
        suffix = ""
    elif gen_count == 2:
        suffix = "_Revised"
    else:
        suffix = f"_Revised_{gen_count - 1}"

    output_path = os.path.join(DATA_DIR, f"PC_{safe_name}_Reytech{suffix}.pdf")

    # ── Multi-PC source: extract only this PC's pages from the combined PDF ──
    if pc.get("multi_pc_source") and pc.get("page_start") is not None:
        try:
            from pypdf import PdfReader as _ExtractReader, PdfWriter as _ExtractWriter
            _r = _ExtractReader(source_pdf)
            _ps = int(pc["page_start"])
            _pe = int(pc.get("page_end", _ps))
            if _pe >= _ps and _pe < len(_r.pages) and len(_r.pages) > _pe + 1:
                _w = _ExtractWriter()
                for _pi in range(_ps, _pe + 1):
                    _w.add_page(_r.pages[_pi])
                _extracted_path = os.path.join(DATA_DIR, f"pc_pdfs/{pcid}_pages_{_ps+1}-{_pe+1}.pdf")
                os.makedirs(os.path.dirname(_extracted_path), exist_ok=True)
                with open(_extracted_path, "wb") as _ef:
                    _w.write(_ef)
                source_pdf = _extracted_path
                log.info("GENERATE %s: extracted pages %d-%d from multi-PC PDF → %s",
                         pcid, _ps + 1, _pe + 1, os.path.basename(_extracted_path))
        except Exception as _ex:
            log.warning("GENERATE %s: page extraction failed, using full PDF: %s", pcid, _ex)

    # Tax: use stored rate only if tax_enabled is true (or not explicitly false)
    _gen_tax = 0.0
    _tax_enabled = pc.get("tax_enabled", False)
    _sr = pc.get("tax_rate", 0)
    if _tax_enabled and _sr and float(_sr) > 0:
        _rv = float(_sr)
        _gen_tax = _rv / 100.0 if _rv > 1.0 else _rv

    result = fill_ams704(
        source_pdf=source_pdf,
        parsed_pc=parsed,
        output_pdf=output_path,
        tax_rate=_gen_tax,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
    )

    # Log result
    log.info("GENERATE %s: fill_ams704 result: ok=%s, items_priced=%s, subtotal=%s",
             pcid, result.get("ok"), result.get("summary", {}).get("items_priced"),
             result.get("summary", {}).get("subtotal"))
    if not result.get("ok"):
        log.error("GENERATE %s FAILED: %s", pcid, result.get("error"))

    if result.get("ok"):
        pc["output_pdf"] = output_path
        # Don't downgrade: if already sent/won, keep that status (this is a revision)
        if pc.get("status") not in ("sent", "pending_award", "won", "lost", "no_response"):
            _transition_status(pc, "draft", actor="system", notes="704 PDF filled")
        else:
            _transition_status(pc, pc["status"], actor="system", notes="704 PDF revised (status preserved)")
        pc["summary"] = result.get("summary", {})
        _save_single_pc(pcid, pc)

        # Ingest completed prices into Won Quotes KB for future reference
        _ingest_pc_to_won_quotes(pc)

        # Catalog all line items for future matching
        _enrich_catalog_from_pc(pc)

        return jsonify({"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(output_path)}"})
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


@bp.route("/pricecheck/<pcid>/source-pdf")
@auth_required
def pricecheck_source_pdf(pcid):
    """Serve the original source PDF for inline viewing."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return "PC not found", 404
    source_pdf = pc.get("source_pdf", "")
    if source_pdf and os.path.exists(source_pdf):
        return send_file(source_pdf, mimetype="application/pdf",
                         download_name=os.path.basename(source_pdf))
    # Fallback: try rfq_files DB
    try:
        from src.core.db import list_rfq_files
        files = list_rfq_files(pcid, category="template")
        if files:
            from src.core.db import get_rfq_file
            f = get_rfq_file(files[0]["id"])
            if f and f.get("data"):
                from flask import Response
                return Response(f["data"], mimetype="application/pdf",
                    headers={"Content-Disposition": f"inline; filename=\"{f.get('filename', 'source.pdf')}\""})
    except Exception as e:
        log.debug("Source PDF DB fallback error: %s", e)
    return "Source PDF not found", 404


@bp.route("/pricecheck/<pcid>/generate-original", methods=["POST"])
@auth_required
def pricecheck_generate_original(pcid):
    """Generate 'Original 704' — company info + pricing only, buyer fields untouched (POST only — writes data)."""
    try:
        return _do_generate_original(pcid)
    except Exception as e:
        log.error("GENERATE-ORIGINAL %s CRASHED: %s", pcid, e)
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": f"Server error: {e}"})


def _do_generate_original(pcid):
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    from src.forms.price_check import fill_ams704

    _sanitize_pc_items(pc)

    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": []}
    pc["parsed"]["line_items"] = pc.get("items", [])

    _auto_priced = 0
    for it in pc.get("items", []):
        cost = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
        price = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
        if cost > 0 and not price and not it.get("no_bid"):
            markup = it.get("markup_pct") or it.get("pricing", {}).get("markup_pct") or 25
            computed = round(cost * (1 + markup / 100), 2)
            it["unit_price"] = computed
            if not it.get("pricing"):
                it["pricing"] = {}
            it["pricing"]["recommended_price"] = computed
            _auto_priced += 1
    if _auto_priced:
        log.info("GENERATE-ORIGINAL %s: auto-computed %d missing prices", pcid, _auto_priced)
        _save_single_pc(pcid, pc)

    parsed = pc.get("parsed", {})
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        recovered = False
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT data, filename FROM rfq_files WHERE rfq_id=? AND category='source' ORDER BY id DESC LIMIT 1",
                    (pcid,)
                ).fetchone()
                if not row:
                    try:
                        row = conn.execute(
                            "SELECT data, filename FROM email_attachments WHERE pc_id=? ORDER BY id DESC LIMIT 1",
                            (pcid,)
                        ).fetchone()
                    except Exception:
                        pass
                if row and row["data"]:
                    restore_dir = os.path.join(DATA_DIR, "pc_pdfs")
                    os.makedirs(restore_dir, exist_ok=True)
                    source_pdf = os.path.join(restore_dir, row["filename"] or f"{pcid}.pdf")
                    with open(source_pdf, "wb") as _fw:
                        _fw.write(row["data"])
                    pc["source_pdf"] = source_pdf
                    _save_single_pc(pcid, pc)
                    recovered = True
                    log.info("GENERATE-ORIGINAL %s: recovered PDF from DB (%d bytes)", pcid, len(row["data"]))
        except Exception as _dbe:
            log.warning("GENERATE-ORIGINAL %s: DB recovery failed: %s", pcid, _dbe)

        if not recovered:
            return jsonify({"ok": False, "error": "Source PDF not found. Upload the buyer's original 704 PDF first (More \u2192 Upload PDF & Parse), then try again."})

    pc_num = pc.get("pc_number", "") or ""
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip()) if pc_num.strip() else ""
    safe_name = f"{safe_name}_{pcid}" if safe_name else pcid
    output_path = os.path.join(DATA_DIR, f"PC_{safe_name}_Original.pdf")

    log.info("GENERATE-ORIGINAL %s: %d items, source=%s, output=%s",
             pcid, len(parsed.get("line_items", [])), os.path.basename(source_pdf),
             os.path.basename(output_path))

    # Tax: respect tax_enabled flag — default off for government price checks
    _pc_tax_rate = 0.0
    _tax_enabled = pc.get("tax_enabled", False)
    _stored_rate = pc.get("tax_rate", 0)
    if _tax_enabled and _stored_rate and float(_stored_rate) > 0:
        _r = float(_stored_rate)
        _pc_tax_rate = _r / 100.0 if _r > 1.0 else _r
    elif _tax_enabled and ((pc.get("header") or {}).get("zip_code") or pc.get("ship_to")):
        try:
            from src.agents.tax_agent import get_tax_rate as _gtr
            import re as _re_tax
            _zip = ((pc.get("header") or {}).get("zip_code") or "").strip()
            if not _zip:
                _zm = _re_tax.search(r'\b(\d{5})\b', pc.get("ship_to", ""))
                _zip = _zm.group(1) if _zm else ""
            if _zip:
                _tr = _gtr(zip_code=_zip)
                if _tr and _tr.get("rate"):
                    _pc_tax_rate = _tr["rate"]
        except Exception:
            pass
    if _pc_tax_rate == 0.0:
        log.warning("GENERATE-ORIGINAL %s: tax_rate=0 — no rate stored and lookup failed", pcid)

    result = fill_ams704(
        source_pdf=source_pdf,
        parsed_pc=parsed,
        output_pdf=output_path,
        tax_rate=_pc_tax_rate,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
        original_mode=True,
    )

    if result.get("ok"):
        pc["original_pdf"] = output_path
        _save_single_pc(pcid, pc)
        log.info("GENERATE-ORIGINAL %s: SUCCESS \u2014 %d items priced, subtotal=$%.2f",
                 pcid, result.get("summary", {}).get("items_priced", 0),
                 result.get("summary", {}).get("subtotal", 0))
        return jsonify({"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(output_path)}"})

    log.error("GENERATE-ORIGINAL %s FAILED: %s", pcid, result.get("error"))
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


# NOTE: /api/pricecheck/download/<filename> is defined in routes_crm.py
# (broader search with subdirectory scan + DB fallback)


@bp.route("/pricecheck/<pcid>/generate-quote", methods=["POST"])
@auth_required
def pricecheck_generate_quote(pcid):
    """Generate a standalone Reytech-branded quote PDF from a Price Check (POST only — writes data)."""
    from src.api.trace import Trace
    t = Trace("quote_generation", pc_id=pcid)
    
    if not QUOTE_GEN_AVAILABLE:
        t.fail("quote_generator.py not available")
        return jsonify({"ok": False, "error": "quote_generator.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        t.fail("PC not found", pc_id=pcid)
        return jsonify({"ok": False, "error": "PC not found"})

    # Validate before generating
    try:
        from src.core.quote_validator import validate_ready_to_generate
        validation = validate_ready_to_generate(pc)
        if not validation["ok"]:
            t.fail("Validation failed", errors=validation["errors"])
            return jsonify({"ok": False, "error": f"Cannot generate: {'; '.join(validation['errors'])}"})
    except Exception:
        pass

    pc_num = pc.get("pc_number", "") or ""
    t.step("Starting", pc_number=pc_num, institution=pc.get("institution",""), items=len(pc.get("items",[])))
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip()) if pc_num.strip() else ""
    safe_name = f"{safe_name}_{pcid}" if safe_name else pcid
    output_path = os.path.join(DATA_DIR, f"Quote_{safe_name}_Reytech.pdf")

    locked_qn = pc.get("reytech_quote_number", "")
    # Allocate quote number BEFORE generating to prevent burns on repeated clicks
    if not locked_qn:
        from src.forms.quote_generator import _next_quote_number
        locked_qn = _next_quote_number()
        pc["reytech_quote_number"] = locked_qn
        _save_single_pc(pcid, pc)

    result = generate_quote_from_pc(
        pc, output_path,
        include_tax=True,
        quote_number=locked_qn,
    )

    if result.get("ok"):
        pc["reytech_quote_pdf"] = output_path
        pc["reytech_quote_number"] = result.get("quote_number", locked_qn)
        pc["status"] = "draft"
        _save_single_pc(pcid, pc)
        _enrich_catalog_from_pc(pc)
        _log_crm_activity(result.get("quote_number", ""), "quote_generated",
                          f"Quote {result.get('quote_number','')} generated — ${result.get('total',0):,.2f} for {pc.get('institution','')}",
                          actor="user", metadata={"institution": pc.get("institution",""), "agency": result.get("agency","")})
        t.ok("Quote generated", quote_number=result.get("quote_number",""), total=result.get("total",0))
        return jsonify({
            "ok": True,
            "download": f"/api/pricecheck/download/{os.path.basename(output_path)}",
            "quote_number": result.get("quote_number"),
        })
    t.fail("Quote generation failed", error=result.get("error", "Unknown"))
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


def _ingest_pc_to_won_quotes(pc):
    """Ingest completed Price Check pricing into Won Quotes KB."""
    if not PRICING_ORACLE_AVAILABLE:
        return
    try:
        items = pc.get("items", [])
        institution = pc.get("institution", "")
        pc_num = pc.get("pc_number", "")
        for item in items:
            pricing = item.get("pricing", {})
            price = pricing.get("recommended_price")
            if not price:
                continue
            ingest_scprs_result({
                "po_number": f"PC-{pc_num}",
                "item_number": item.get("item_number", ""),
                "description": item.get("description", ""),
                "unit_price": price,
                "supplier": "Reytech Inc.",
                "department": institution,
                "award_date": datetime.now().strftime("%Y-%m-%d"),
                "source": "price_check",
            })
        log.info(f"Ingested {len(items)} items from PC #{pc_num} into Won Quotes KB")
    except Exception as e:
        log.error(f"KB ingestion error: {e}")


@bp.route("/pricecheck/<pcid>/convert-to-quote")
@auth_required
def pricecheck_convert_to_quote(pcid):
    """Convert a Price Check into a full RFQ with 704A/B and Bid Package."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    header = pc.get("parsed", {}).get("header", {})

    # Build RFQ record from PC data
    rfq_id = str(uuid.uuid4())[:8]
    line_items = []
    for idx, item in enumerate(items):
        pricing = item.get("pricing", {})
        # First-class fields take precedence over oracle suggestions
        vendor_cost = item.get("vendor_cost") or pricing.get("unit_cost") or pricing.get("amazon_price") or 0
        unit_price  = item.get("unit_price")  or pricing.get("recommended_price") or 0
        markup_pct  = item.get("markup_pct")  or pricing.get("markup_pct", 25)
        qty         = item.get("qty", 1) or 1
        profit_unit  = round(unit_price - vendor_cost, 4) if (unit_price and vendor_cost) else None
        profit_total = round(profit_unit * qty, 2) if profit_unit is not None else None
        margin_pct   = round((unit_price - vendor_cost) / unit_price * 100, 1) if (unit_price and vendor_cost) else None

        # row_index = original 704A form row (1-based), used for placing data in 704B
        row_idx = item.get("row_index", idx + 1)

        li = {
            "item_number":     item.get("item_number", ""),
            "row_index":       row_idx,
            "form_row":        row_idx,
            "line_number":     row_idx,
            "mfg_number":      item.get("mfg_number", ""),
            "is_substitute":   item.get("is_substitute", False),
            "description":     item.get("description", ""),
            "qty":             qty,
            "uom":             item.get("uom", "ea"),
            "qty_per_uom":     item.get("qty_per_uom", 1),
            # Cost & profit (the fields that matter for business intelligence)
            "vendor_cost":     vendor_cost,
            "markup_pct":      markup_pct,
            "unit_price":      unit_price,
            "price_per_unit":  unit_price,
            "extension":       round(unit_price * qty, 2),
            "profit_unit":     profit_unit,
            "profit_total":    profit_total,
            "margin_pct":      margin_pct,
            # Backwards compat names
            "unit_cost":       vendor_cost,
            "supplier_cost":   vendor_cost,
            "our_price":       unit_price,
            # Source intelligence
            "scprs_last_price": pricing.get("scprs_price"),
            "amazon_price":     pricing.get("amazon_price"),
            "price_source":     pricing.get("price_source", "manual"),
            "supplier_source":  pricing.get("price_source", "price_check"),
            "supplier_url":     pricing.get("amazon_url", ""),
        }
        line_items.append(li)

    rfq = {
        "id": rfq_id,
        "solicitation_number": f"PC-{pc.get('pc_number', 'unknown')}",
        "requestor_name": header.get("requestor", pc.get("requestor", "")),
        "requestor_email": "",
        "department": header.get("institution", pc.get("institution", "")),
        "ship_to": pc.get("ship_to", ""),
        "delivery_zip": header.get("zip_code", ""),
        "due_date": pc.get("due_date", ""),
        "phone": header.get("phone", ""),
        "line_items": line_items,
        "status": "pending",
        "source": "price_check",
        "source_pc_id": pcid,
        "is_test": pcid.startswith("test_") or pc.get("is_test", False),
        "award_method": "all_or_none",
        "created_at": datetime.now().isoformat(),
    }

    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq
    save_rfqs(rfqs)

    # Update PC status
    _transition_status(pc, "draft", actor="system", notes="Reytech quote generated")
    pc["converted_rfq_id"] = rfq_id
    _save_single_pc(pcid, pc)

    return jsonify({"ok": True, "rfq_id": rfq_id})


# NOTE: /api/pc/<pcid>/convert-to-rfq is defined in routes_analytics.py
# (more thorough version that copies all fields, files, and PO screenshots)


@bp.route("/api/pricecheck/split-pdf", methods=["POST"])
@auth_required
def api_pc_split_pdf():
    """Upload a combined AMS 704 PDF containing multiple price checks.
    Auto-detects boundaries, creates one PC record per section."""
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"})
    f = request.files["file"]
    if not f.filename or not f.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "Must be a PDF file"})

    import uuid as _uuid
    upload_dir = os.path.join(DATA_DIR, "uploads", "multi_pc")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', f.filename)
    pdf_path = os.path.join(upload_dir, f"{_uuid.uuid4().hex[:8]}_{safe_name}")
    f.save(pdf_path)
    log.info("SPLIT-PDF: saved %s (%d bytes)", safe_name, os.path.getsize(pdf_path))

    try:
        from src.forms.price_check import parse_multi_pc
        sections = parse_multi_pc(pdf_path)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Parse failed: {e}"})
    if not sections:
        return jsonify({"ok": False, "error": "No PC sections found in PDF"})

    created = []
    for section in sections:
        pc_id = "pc_" + _uuid.uuid4().hex[:8]
        header = section.get("header", {})
        institution = header.get("institution", "").strip()
        requestor = header.get("requestor", "").strip()
        pc_number = header.get("price_check_number", "").strip()
        items = section.get("line_items", [])
        # ── Resolve institution → agency using the resolver ──
        agency_name = ""
        agency_key = ""
        canonical_inst = institution
        try:
            from src.core.institution_resolver import resolve as _resolve_inst
            _resolved = _resolve_inst(institution)
            if _resolved.get("agency"):
                agency_key = _resolved["agency"]
                agency_name = _resolved.get("canonical", institution)
                canonical_inst = agency_name
                log.info("SPLIT-PDF: resolved '%s' → agency=%s canonical='%s'",
                         institution, agency_key, canonical_inst)
        except Exception as _re:
            log.debug("Institution resolve failed: %s", _re)

        pc = {
            "id": pc_id,
            "pc_number": pc_number or (f"PC-{institution[:12].replace(' ','-')}" if institution else pc_id),
            "institution": canonical_inst, "requestor": requestor, "requestor_name": requestor,
            "requestor_email": "", "agency": agency_key, "agency_name": agency_name or agency_key,
            "due_date": header.get("due_date", ""),
            "ship_to": f"{canonical_inst}, {header.get('zip_code','')}" if canonical_inst else "",
            "zip_code": header.get("zip_code", ""),
            "phone": header.get("phone", ""),
            "status": "parsed", "source": "multi_pc_upload", "source_pdf": pdf_path,
            "created_at": datetime.now().isoformat(), "items": items,
            "parsed": {"header": header, "line_items": items},
            "page_start": section.get("page_start", 0), "page_end": section.get("page_end", 0),
            "multi_pc_source": safe_name,
        }
        from src.api.dashboard import _save_single_pc
        _save_single_pc(pc_id, pc)
        # Auto-enrich with catalog, SCPRS, URL extraction
        try:
            _auto_enrich_pc(pc_id)
        except Exception as _ae:
            log.warning("SPLIT-PDF %s: auto-enrich failed: %s", pc_id, _ae)
        created.append({
            "pc_id": pc_id, "institution": canonical_inst or institution or "Unknown",
            "requestor": requestor, "pc_number": pc_number,
            "items": len(items),
            "pages": f"{section.get('page_start',0)+1}-{section.get('page_end',0)+1}",
            "url": f"/pricecheck/{pc_id}",
        })
        log.info("SPLIT-PDF: created PC %s — %s — %d items (pages %d-%d)",
                 pc_id, canonical_inst or institution, len(items), section.get("page_start",0), section.get("page_end",0))

    by_institution = {}
    for pc in created:
        by_institution.setdefault(pc["institution"], []).append(pc)
    return jsonify({"ok": True, "total": len(created), "pcs": created,
                    "by_institution": by_institution, "source_file": safe_name})


@bp.route("/api/pricecheck/create-manual", methods=["POST"])
@auth_required
def api_pc_create_manual():
    """Create a Price Check manually from the dashboard."""
    data = request.get_json(force=True, silent=True) or {}
    sol = data.get("solicitation_number", "").strip()
    inst = data.get("institution", "").strip()
    if not sol and not inst:
        return jsonify({"ok": False, "error": "solicitation_number or institution required"})

    import uuid
    pcid = "pc_" + uuid.uuid4().hex[:8]

    pc = {
        "id": pcid,
        "pc_number": sol or inst,
        "solicitation_number": sol,
        "institution": inst,
        "requestor": data.get("requestor", ""),
        "buyer": data.get("requestor", ""),
        "due_date": data.get("due_date", ""),
        "status": "new",
        "source": "manual",
        "created_at": datetime.now().isoformat(),
        "items": [],
    }

    pcs = _load_price_checks()
    pcs[pcid] = pc
    _save_single_pc(pcid, pc)

    return jsonify({"ok": True, "pc_id": pcid, "sol": sol or inst})


@bp.route("/api/resync")
@auth_required
def api_resync():
    """Re-import emails WITHOUT destroying user work.
    
    PRESERVES:
    - RFQs with terminal status (sent, won, lost, generated, draft)
    - All price checks (PCs persist until explicitly dismissed)
    - All user-set pricing, notes, quote numbers
    
    CLEARS:
    - RFQs with status 'new' or 'parse_error' (stale imports)
    - Processed email UID list (so missed emails get re-imported)
    """
    log.info("Smart resync triggered — preserving terminal statuses")
    
    try:
        # ── 1. Snapshot what we want to keep ──
        rfqs = load_rfqs()
        pcs_before = _load_price_checks()
        pc_count = len(pcs_before)
        
        TERMINAL_STATUSES = {"sent", "not_responding", "draft", "dismissed", "archived"}
        
        # Keep RFQs with terminal status — keyed by BOTH id and email_uid
        kept_rfqs = {}           # id → full rfq data (preserved)
        kept_by_uid = set()      # email_uids we're keeping (skip on re-import)
        kept_by_sol = set()      # solicitation numbers we're keeping
        cleared_count = 0
        
        for rid, r in rfqs.items():
            status = (r.get("status") or "new").lower()
            if status in TERMINAL_STATUSES:
                kept_rfqs[rid] = r
                uid = r.get("email_uid")
                if uid:
                    kept_by_uid.add(uid)
                sol = r.get("solicitation_number", "")
                if sol and sol != "unknown":
                    kept_by_sol.add(sol.strip())
            else:
                cleared_count += 1
        
        # Also build set of PC email_uids to skip (don't re-create PCs that already exist)
        # BUT: exclude parse_error PCs with 0 items — those need re-processing after fixes
        pc_uids = set()
        for pc in pcs_before.values():
            uid = pc.get("email_uid")
            if uid:
                # Skip broken PCs — they should be re-imported after a fix
                if pc.get("status") == "parse_error" and not pc.get("items"):
                    continue
                pc_uids.add(uid)
        
        log.info("Resync: keeping %d terminal RFQs, clearing %d stale, %d PCs preserved",
                 len(kept_rfqs), cleared_count, pc_count)
        
        # ── 2. Save only the kept RFQs ──
        save_rfqs(kept_rfqs)
        
        # ── 3. Clear processed UIDs completely ──
        # The dedup logic in process_rfq_email handles duplicates:
        #   - email_uid match → skip
        #   - solicitation_number match → skip or link as amendment
        # So we don't need to pre-seed. This ensures emails that failed
        # processing before (e.g. after a bug fix) get a fresh chance.
        proc_file = os.path.join(DATA_DIR, "processed_emails.json")
        if os.path.exists(proc_file):
            os.remove(proc_file)
        # Also clear SQLite processed_emails + fingerprints tables
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM processed_emails")
                try:
                    conn.execute("DELETE FROM email_fingerprints")
                except Exception:
                    pass
        except Exception:
            pass
        log.info("Resync: cleared processed_emails (JSON + SQLite + fingerprints)")
        
        # ── 4. Reset poller + re-poll ──
        global _shared_poller
        _shared_poller = None
        imported = _safe_do_poll_check()
        
        # ── 5. Report ──
        rfqs_after = load_rfqs()
        pcs_after = _load_price_checks()
        
        log.info("Resync complete: %d new imported, %d preserved, %d total RFQs, %d PCs",
                 len(imported), len(kept_rfqs), len(rfqs_after), len(pcs_after))
        
        return jsonify({
            "ok": True,
            "cleared": cleared_count,
            "found": len(imported),
            "preserved": len(kept_rfqs),
            "total_rfqs": len(rfqs_after),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
            "pcs_preserved": pc_count,
            "pcs_total": len(pcs_after),
            "last_check": POLL_STATUS.get("last_check"),
        })
    except Exception as e:
        log.error("Resync failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "found": 0, "error": str(e)})


def _remove_processed_uid(uid):
    """Remove a single UID from processed_emails.json."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if not os.path.exists(proc_file):
        return
    try:
        with open(proc_file) as f:
            processed = json.load(f)
        if isinstance(processed, list) and uid in processed:
            processed.remove(uid)
            with open(proc_file, "w") as f:
                json.dump(processed, f)
            log.info(f"Removed UID {uid} from processed list")
        elif isinstance(processed, dict) and uid in processed:
            del processed[uid]
            with open(proc_file, "w") as f:
                json.dump(processed, f)
    except Exception as e:
        log.error(f"Error removing UID: {e}")


@bp.route("/api/email-debug")
@auth_required
def api_email_debug():
    """Diagnostic: show processed email count, poller state, recent traces."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    proc_count = 0
    try:
        if os.path.exists(proc_file):
            with open(proc_file) as f:
                proc_data = json.load(f)
                proc_count = len(proc_data) if isinstance(proc_data, (list, dict)) else 0
    except Exception:
        pass
    
    # Get poller diagnostics
    diag = {}
    global _shared_poller
    if _shared_poller and hasattr(_shared_poller, '_diag'):
        diag = _shared_poller._diag.copy()
        diag.pop("subjects_seen", None)  # too verbose
    
    traces = POLL_STATUS.get("_email_traces", [])[-10:]
    
    return jsonify({
        "ok": True,
        "processed_count": proc_count,
        "poll_status": {
            "running": POLL_STATUS.get("running"),
            "last_check": POLL_STATUS.get("last_check"),
            "emails_found": POLL_STATUS.get("emails_found"),
            "error": POLL_STATUS.get("error"),
        },
        "poller_diag": diag,
        "recent_traces": traces,
    })


@bp.route("/api/force-reprocess", methods=["GET", "POST"])
@auth_required
def api_force_reprocess():
    """Nuclear option: clear ALL processed UIDs and re-poll.
    Use when a specific email isn't being picked up despite code fixes."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    old_count = 0
    
    try:
        if os.path.exists(proc_file):
            with open(proc_file) as f:
                old_data = json.load(f)
                old_count = len(old_data) if isinstance(old_data, (list, dict)) else 0
            os.remove(proc_file)
            log.info("Force-reprocess: cleared %d processed UIDs", old_count)
    except Exception as e:
        log.error("Force-reprocess clear failed: %s", e)
    
    # Reset poller
    global _shared_poller
    _shared_poller = None
    
    # Re-poll
    try:
        imported = _safe_do_poll_check()
        return jsonify({
            "ok": True,
            "cleared_uids": old_count,
            "found": len(imported),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/force-recapture", methods=["GET", "POST"])
@auth_required
def api_force_recapture():
    """Delete a specific RFQ/PC by keyword match, clear its UID, and re-poll.
    
    POST body: {"match": "calvet"} or {"rfq_id": "exact_id"}
    Searches solicitation_number, email_sender, email_subject, agency.
    """
    data = request.get_json(force=True, silent=True) or {}
    match_kw = (data.get("match") or "").lower().strip()
    exact_id = data.get("rfq_id", "").strip()
    
    if not match_kw and not exact_id:
        return jsonify({"ok": False, "error": "Provide 'match' keyword or 'rfq_id'"})
    
    removed_rfqs = []
    removed_pcs = []
    cleared_uids = []
    
    # ── Remove matching RFQs ──
    rfqs = load_rfqs()
    to_remove = []
    for rid, r in rfqs.items():
        if exact_id and rid == exact_id:
            to_remove.append(rid)
        elif match_kw:
            searchable = " ".join([
                r.get("solicitation_number", ""),
                r.get("email_sender", ""),
                r.get("email_subject", ""),
                r.get("agency", ""),
                r.get("agency_name", ""),
                r.get("requestor_email", ""),
            ]).lower()
            if match_kw in searchable:
                to_remove.append(rid)
    
    for rid in to_remove:
        r = rfqs.pop(rid)
        uid = r.get("email_uid")
        if uid:
            cleared_uids.append(uid)
        removed_rfqs.append({
            "id": rid,
            "sol": r.get("solicitation_number", "?"),
            "sender": r.get("email_sender", "?"),
            "items": len(r.get("line_items", [])),
        })
        log.info("Force-recapture: removed RFQ %s (sol=%s)", rid, r.get("solicitation_number", "?"))
    
    if to_remove:
        save_rfqs(rfqs)
    
    # ── Remove matching PCs ──
    pcs = _load_price_checks()
    pc_remove = []
    for pid, pc in pcs.items():
        if exact_id and pid == exact_id:
            pc_remove.append(pid)
        elif match_kw:
            searchable = " ".join([
                pc.get("pc_number", ""),
                pc.get("email_subject", ""),
                pc.get("requestor", ""),
                str(pc.get("institution", "")),
            ]).lower()
            if match_kw in searchable:
                pc_remove.append(pid)
    
    for pid in pc_remove:
        pc = pcs.pop(pid)
        uid = pc.get("email_uid")
        if uid:
            cleared_uids.append(uid)
        removed_pcs.append({"id": pid, "pc_number": pc.get("pc_number", "?")})
        log.info("Force-recapture: removed PC %s", pid)
    
    if pc_remove:
        _save_price_checks(pcs)
    
    # ── Clear UIDs from processed list ──
    if cleared_uids:
        proc_file = os.path.join(DATA_DIR, "processed_emails.json")
        try:
            if os.path.exists(proc_file):
                with open(proc_file) as f:
                    processed = json.load(f)
                if isinstance(processed, list):
                    before = len(processed)
                    processed = [u for u in processed if u not in cleared_uids]
                    with open(proc_file, "w") as f:
                        json.dump(processed, f)
                    log.info("Cleared %d UIDs from processed list", before - len(processed))
        except Exception as e:
            log.warning("UID clearing failed: %s", e)
    
    if not removed_rfqs and not removed_pcs:
        return jsonify({"ok": False, "error": f"No matches found for '{match_kw or exact_id}'"})
    
    # ── Reset poller and re-poll ──
    global _shared_poller
    _shared_poller = None
    
    try:
        imported = _safe_do_poll_check()
    except Exception as e:
        imported = []
        log.error("Re-poll failed: %s", e)
    
    return jsonify({
        "ok": True,
        "removed_rfqs": removed_rfqs,
        "removed_pcs": removed_pcs,
        "cleared_uids": len(cleared_uids),
        "reimported": len(imported),
        "new_rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
    })


@bp.route("/api/clear-queue", methods=["POST"])
@auth_required
def api_clear_queue():
    """Clear all RFQs from the queue (POST only — destructive operation)."""
    rfqs = load_rfqs()
    count = len(rfqs)
    if not count:
        return jsonify({"ok": True, "message": "Queue already empty"})
    rfqs.clear()
    save_rfqs(rfqs)
    log.warning("Queue cleared: %d RFQs removed by user", count)
    return jsonify({"ok": True, "message": f"Queue cleared ({count} RFQs removed)"})


@bp.route("/dl/<rid>/<fname>")
@auth_required
def download(rid, fname):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    sol = r["solicitation_number"] if r else rid
    safe = os.path.basename(fname)
    
    # Search filesystem — targeted directories only (no full os.walk)
    for search_dir in [
        os.path.join(OUTPUT_DIR, sol),
        os.path.join(OUTPUT_DIR, rid),
        os.path.join(DATA_DIR, "output", sol),
        os.path.join(DATA_DIR, "output", rid),
        os.path.join(DATA_DIR, "outputs"),
        OUTPUT_DIR,
    ]:
        candidate = os.path.join(search_dir, safe)
        if os.path.exists(candidate):
            return send_file(candidate, as_attachment=True, download_name=safe)
    
    # Fallback: check DB (rfq_files table — survives redeploys)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT data, filename FROM rfq_files WHERE (rfq_id=? OR rfq_id=?) AND filename=? ORDER BY id DESC LIMIT 1",
                (rid, sol, safe)).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT data, filename FROM rfq_files WHERE filename=? ORDER BY id DESC LIMIT 1",
                    (safe,)).fetchone()
            if row and row["data"]:
                restore_dir = os.path.join(OUTPUT_DIR, sol or rid, "_restored")
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, safe)
                with open(restore_path, "wb") as _fw:
                    _fw.write(row["data"])
                return send_file(restore_path, as_attachment=True, download_name=safe)
    except Exception as _e:
        log.debug("DB file lookup failed for %s: %s", safe, _e)
    
    flash("File not found", "error")
    return redirect(f"/rfq/{rid}")


@bp.route("/api/scprs/<rid>")
@auth_required
def api_scprs(rid):
    """SCPRS lookup API endpoint — batch search, single session."""
    log.info("SCPRS lookup requested for RFQ %s", rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return jsonify({"error": "not found"})
    
    items = r.get("line_items", [])
    if not items:
        return jsonify({"results": [], "errors": ["No line items"]})
    
    results = []
    errors = []
    
    try:
        from src.agents.scprs_lookup import (
            _get_session, _build_search_terms, _find_best_line_match,
            _load_db, save_price
        )
        
        # Step 1: Try local DB first for each item
        db = _load_db()
        items_needing_search = []
        
        for i, item in enumerate(items):
            item_num = item.get("item_number", "")
            desc = item.get("description", "")
            search_terms = _build_search_terms(item_num, desc)
            
            # Check local DB
            local_hit = None
            if item_num and item_num.strip() in db:
                e = db[item_num.strip()]
                local_hit = {
                    "price": e["price"], "source": "local_db",
                    "date": e.get("date", ""), "confidence": "high",
                    "vendor": e.get("vendor", ""), "searched": search_terms,
                }
            
            if not local_hit and desc:
                dl = desc.lower().split("\n")[0].strip()
                for key, entry in db.items():
                    ed = (entry.get("description", "") or "").lower()
                    wa, wb = set(dl.split()), set(ed.split())
                    if wa and wb and len(wa & wb) / max(len(wa), len(wb)) > 0.5:
                        local_hit = {
                            "price": entry["price"], "source": "local_db_fuzzy",
                            "date": entry.get("date", ""), "confidence": "medium",
                            "vendor": entry.get("vendor", ""), "searched": search_terms,
                        }
                        break
            
            if local_hit:
                results.append(local_hit)
            else:
                results.append(None)  # placeholder
                items_needing_search.append((i, item_num, desc, search_terms))
        
        # Step 2: Batch SCPRS live search — ONE session for all items
        if items_needing_search:
            session = _get_session()
            if not session.initialized:
                session.init_session()
            
            if session.initialized:
                for idx, item_num, desc, search_terms in items_needing_search:
                    best_result = None
                    
                    for term in search_terms[:2]:  # Max 2 terms per item
                        try:
                            search_results = session.search(description=term)
                            if not search_results:
                                continue
                            
                            import time
                            time.sleep(0.3)
                            
                            # Sort by most recent
                            from datetime import datetime, timedelta
                            cutoff = datetime.now() - timedelta(days=548)
                            recent = [sr for sr in search_results
                                     if sr.get("start_date_parsed") and sr["start_date_parsed"] >= cutoff]
                            cands = sorted(recent or search_results,
                                          key=lambda x: x.get("start_date_parsed") or datetime.min,
                                          reverse=True)
                            
                            # Try detail page on top candidate
                            for c in cands[:2]:
                                try:
                                    if c.get("_results_html"):
                                        detail = session.get_detail(
                                            c["_results_html"], c["_row_index"],
                                            c.get("_click_action"))
                                        time.sleep(0.3)
                                        
                                        if detail and detail.get("line_items"):
                                            line = _find_best_line_match(
                                                detail["line_items"], item_num, desc)
                                            if line and line.get("unit_price_num"):
                                                best_result = {
                                                    "price": line["unit_price_num"],
                                                    "unit_price": line["unit_price_num"],
                                                    "quantity": line.get("quantity_num"),
                                                    "source": "fiscal_scprs",
                                                    "date": c.get("start_date", ""),
                                                    "confidence": "high",
                                                    "vendor": c.get("supplier_name", ""),
                                                    "po_number": c.get("po_number", ""),
                                                    "department": c.get("dept", ""),
                                                    "searched": search_terms,
                                                }
                                                break
                                        
                                        # Re-init session after detail (state is fragile)
                                        try:
                                            session.init_session()
                                        except Exception:
                                            pass
                                except Exception as _de:
                                    log.debug("Detail attempt: %s", _de)
                            
                            if best_result:
                                break
                            
                            # Fallback: use search-level data (PO total + vendor)
                            if not best_result and cands:
                                c = cands[0]
                                gt = c.get("grand_total_num", 0)
                                if gt and gt > 0:
                                    best_result = {
                                        "price": gt,
                                        "source": "fiscal_scprs_summary",
                                        "date": c.get("start_date", ""),
                                        "confidence": "low",
                                        "vendor": c.get("supplier_name", ""),
                                        "po_number": c.get("po_number", ""),
                                        "department": c.get("dept", ""),
                                        "first_item": c.get("first_item", ""),
                                        "note": "PO total (not unit price)",
                                        "searched": search_terms,
                                    }
                                    break
                            
                        except Exception as _se:
                            log.warning("SCPRS search '%s': %s", term, _se)
                            # Try to recover session
                            try:
                                session.init_session()
                            except Exception:
                                pass
                    
                    if best_result:
                        results[idx] = best_result
                        # Cache for future lookups
                        if best_result.get("price") and best_result.get("source") != "fiscal_scprs_summary":
                            try:
                                save_price(
                                    item_number=item_num or "",
                                    description=desc or "",
                                    price=best_result["price"],
                                    vendor=best_result.get("vendor", ""),
                                    unit_price=best_result.get("unit_price"),
                                    quantity=best_result.get("quantity"),
                                    po_number=best_result.get("po_number", ""),
                                    source="fiscal_scprs"
                                )
                            except Exception:
                                pass
                    else:
                        results[idx] = {
                            "price": None,
                            "note": "No SCPRS data found",
                            "item_number": item_num,
                            "description": (desc or "")[:80],
                            "searched": search_terms,
                        }
            else:
                errors.append("SCPRS session init failed")
                for idx, item_num, desc, search_terms in items_needing_search:
                    results[idx] = {
                        "price": None,
                        "error": "SCPRS session init failed",
                        "item_number": item_num,
                        "searched": search_terms,
                    }
    
    except Exception as e:
        import traceback
        errors.append(str(e))
        log.error("SCPRS batch lookup: %s", e, exc_info=True)
    
    # Fill any remaining None slots
    for i in range(len(results)):
        if results[i] is None:
            results[i] = {"price": None, "note": "Lookup skipped"}
    
    # Auto-ingest results to catalog + KB
    for i, res in enumerate(results):
        if not res or not res.get("price"):
            continue
        item = items[i] if i < len(items) else {}
        item_num = item.get("item_number", "")
        desc = item.get("description", "")
        
        if PRICING_ORACLE_AVAILABLE:
            try:
                ingest_scprs_result(
                    po_number=res.get("po_number", ""),
                    item_number=item_num, description=desc,
                    unit_price=res["price"], quantity=1,
                    supplier=res.get("vendor", ""),
                    department=res.get("department", ""),
                    award_date=res.get("date", ""),
                    source=res.get("source", "scprs_live"),
                )
            except Exception:
                pass
        
        try:
            from src.core.db import record_price as _rp_scprs
            _rp_scprs(
                description=desc, unit_price=res["price"],
                source="scprs_live", part_number=item_num,
                source_id=res.get("po_number", ""),
                agency=res.get("department", ""),
                notes=f"SCPRS vendor: {res.get('vendor', '')}"
            )
        except Exception:
            pass
        
        try:
            from src.agents.product_catalog import add_to_catalog, init_catalog_db
            init_catalog_db()
            add_to_catalog(
                description=desc, part_number=item_num,
                cost=float(res["price"]), sell_price=0,
                source="scprs_live",
                supplier_name=res.get("vendor", ""),
            )
        except Exception:
            pass
    
    found = sum(1 for r in results if r and r.get("price"))
    log.info("SCPRS batch: %d/%d prices found for RFQ %s", found, len(items), rid)
    return jsonify({"results": results, "errors": errors if errors else None})


@bp.route("/api/scprs-test")
@auth_required
def api_scprs_test():
    """SCPRS search test — ?q=stryker+xpr"""
    q = (request.args.get("q", "") or "").strip()
    if not q:
        return jsonify({"error": "Missing required parameter: q"}), 400
    try:
        from src.agents.scprs_lookup import test_search
        return jsonify(test_search(q))
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@bp.route("/api/scprs-bulk/<rid>")
@auth_required
def api_scprs_bulk(rid):
    """Bulk SCPRS search — one session, searches each RFQ item, returns summary table.
    
    Hit: /api/scprs-bulk/{rfq_id}
    Returns clean JSON with per-item SCPRS results (PO#, vendor, total, date).
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"error": "RFQ not found"})
    
    items = r.get("line_items", [])
    if not items:
        return jsonify({"error": "No line items"})
    
    try:
        from src.agents.scprs_lookup import _get_session, _build_search_terms
        import time
        
        session = _get_session()
        if not session.initialized:
            if not session.init_session():
                return jsonify({"error": "SCPRS session init failed"})
        
        results = []
        for i, item in enumerate(items):
            pn = item.get("item_number", "")
            desc = item.get("description", "")
            cost = item.get("supplier_cost", 0)
            terms = _build_search_terms(pn, desc)
            
            # Search with first term (most specific)
            search_results = []
            searched_term = ""
            for term in terms[:2]:
                try:
                    search_results = session.search(description=term)
                    searched_term = term
                    if search_results:
                        break
                    time.sleep(0.3)
                except Exception as e:
                    log.debug("Bulk SCPRS search '%s': %s", term, e)
                    try:
                        session.init_session()
                    except Exception:
                        pass
            
            # Extract best result
            best = None
            for sr in sorted(search_results, 
                           key=lambda x: x.get("start_date_parsed") or __import__("datetime").datetime.min,
                           reverse=True)[:3]:
                gt = sr.get("grand_total_num", 0)
                if gt and gt > 0:
                    best = {
                        "po_number": sr.get("po_number", ""),
                        "vendor": sr.get("supplier_name", ""),
                        "grand_total": sr.get("grand_total", ""),
                        "date": sr.get("start_date", ""),
                        "dept": sr.get("dept", ""),
                        "first_item": sr.get("first_item", ""),
                        "acq_method": sr.get("acq_method", ""),
                    }
                    break
            
            results.append({
                "line": i + 1,
                "part_number": pn,
                "description": (desc or "")[:50],
                "echelon_cost": cost,
                "searched": searched_term,
                "scprs_results_count": len(search_results),
                "best_match": best,
            })
            time.sleep(0.5)  # Be gentle with FI$Cal
        
        return jsonify({
            "rfq": rid,
            "items": len(items),
            "results": results,
        })
    
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@bp.route("/api/scprs-raw")
@auth_required
def api_scprs_raw():
    """Raw SCPRS debug — shows HTML field IDs found in search results."""
    q = (request.args.get("q", "") or "").strip()
    if not q:
        return jsonify({"error": "Missing required parameter: q"}), 400
    try:
        from src.agents.scprs_lookup import _get_session, _discover_grid_ids, SCPRS_SEARCH_URL, SEARCH_BUTTON, ALL_SEARCH_FIELDS, FIELD_DESCRIPTION
        from bs4 import BeautifulSoup
        
        session = _get_session()
        if not session.initialized:
            session.init_session()
        
        # Load search page
        page = session._load_page(2)
        icsid = session._extract_icsid(page)
        if icsid: session.icsid = icsid
        
        # POST search
        sv = {f: "" for f in ALL_SEARCH_FIELDS}
        sv[FIELD_DESCRIPTION] = q
        fd = session._build_form_data(page, SEARCH_BUTTON, sv)
        r = session.session.post(SCPRS_SEARCH_URL, data=fd, timeout=30)
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        
        import re
        count = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
        discovered = _discover_grid_ids(soup, "ZZ_SCPR_RD_DVW")
        
        # Sample row 0 values
        row0 = {}
        for suffix in discovered:
            eid = f"ZZ_SCPR_RD_DVW_{suffix}$0"
            el = soup.find(id=eid)
            val = el.get_text(strip=True) if el else None
            row0[eid] = val
        
        # Also check for link-style elements
        link0 = soup.find("a", id="ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR$0")
        
        # Broad scan: find ALL element IDs ending in $0
        all_row0_ids = {}
        for el in soup.find_all(id=re.compile(r'\$0$')):
            eid = el.get('id', '')
            if eid and ('SCPR' in eid or 'DVW' in eid or 'RSLT' in eid):
                all_row0_ids[eid] = el.get_text(strip=True)[:80]
        
        # Also discover with correct prefix
        discovered2 = _discover_grid_ids(soup, "ZZ_SCPR_RSLT_VW")
        
        # Table class scan
        tables = [(t.get("class",""), t.get("id",""), len(t.find_all("tr")))
                  for t in soup.find_all("table") if t.get("class")]
        grid_tables = [t for t in tables if "PSLEVEL1GRID" in str(t[0])]
        
        return jsonify({
            "query": q, "status": r.status_code, "size": len(html),
            "result_count": count.group(0) if count else "none",
            "id_discovered_RD_DVW": list(discovered.keys()),
            "id_discovered_RSLT_VW": list(discovered2.keys()),
            "all_row0_ids": all_row0_ids,
            "row0_values": row0,
            "po_link_found": link0.get_text(strip=True) if link0 else None,
            "grid_tables": grid_tables[:5],
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@bp.route("/api/status")
@auth_required
def api_status():
    return jsonify({
        "poll": POLL_STATUS,
        "scprs_db": get_price_db_stats(),
        "rfqs": len(load_rfqs()),
    })


@bp.route("/api/poll-now")
@auth_required
def api_poll_now():
    """Manual trigger: check email inbox right now."""
    try:
        imported = _safe_do_poll_check()
        return jsonify({
            "ok": True,
            "found": len(imported),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
            "last_check": POLL_STATUS.get("last_check"),
            "error": POLL_STATUS.get("error"),
            "diag": POLL_STATUS.get("_diag", {}),
        })
    except Exception as e:
        import traceback as _tb
        return jsonify({"ok": False, "found": 0, "error": str(e), "traceback": _tb.format_exc()})


@bp.route("/api/poll/reset-processed", methods=["GET", "POST"])
@auth_required
def api_poll_reset_processed():
    """Atomic: clear processed UIDs → immediately re-poll → return results.
    Prevents background thread from re-saving UIDs between reset and poll.
    """
    global _shared_poller
    
    # Step 1: Delete the processed emails file
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    old_count = 0
    try:
        if os.path.exists(proc_file):
            import json as _json2
            with open(proc_file) as f:
                old_count = len(_json2.load(f))
            os.remove(proc_file)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    # Step 1b: Clear SQLite processed_emails + fingerprints (prevents recovery)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM processed_emails")
            try:
                conn.execute("DELETE FROM email_fingerprints")
            except Exception:
                pass
    except Exception:
        pass
    
    # Step 2: Kill the shared poller so a fresh one gets created
    _shared_poller = None
    
    # Step 3: Immediately run poll (creates new poller with empty processed set)
    try:
        imported = _safe_do_poll_check()
        return jsonify({
            "ok": True,
            "cleared": old_count,
            "found": len(imported),
            "items": [{"id": r.get("id","?"), "sol": r.get("solicitation_number","?"), 
                       "subject": r.get("email_subject", r.get("subject",""))[:60]}
                      for r in imported],
            "poll_diag": POLL_STATUS.get("_diag", {}),
        })
    except Exception as e:
        return jsonify({"ok": False, "cleared": old_count, "error": str(e)})


@bp.route("/api/diag/inbox-peek")
@auth_required
def api_inbox_peek():
    """Show all emails in inbox with filter decisions - NO processing."""
    import imaplib, email as email_mod
    from email.header import decode_header
    try:
        gmail_user = os.environ.get("GMAIL_ADDRESS", "")
        gmail_pass = os.environ.get("GMAIL_PASSWORD", "")
        if not gmail_user or not gmail_pass:
            return jsonify({"error": "No email credentials"})
        
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(gmail_user, gmail_pass)
        mail.select("INBOX", readonly=True)
        
        from datetime import datetime, timedelta
        since = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        # Use UID search to match what the poller uses
        _, data = mail.uid("search", None, f'(SINCE "{since}")')
        uids = data[0].split() if data[0] else []
        
        # Load processed UIDs from JSON
        proc_file = os.path.join(DATA_DIR, "processed_emails.json")
        processed_json = set()
        try:
            if os.path.exists(proc_file):
                import json as _j
                with open(proc_file) as f:
                    processed_json = set(str(x) for x in _j.load(f))
        except Exception:
            pass
        
        # Load processed UIDs from SQLite
        processed_db = set()
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute("SELECT uid FROM processed_emails").fetchall()
                processed_db = set(str(r[0]) for r in rows)
        except Exception:
            pass
        
        all_processed = processed_json | processed_db
        
        emails = []
        for uid in uids[-10:]:  # Last 10
            uid_str = uid.decode()
            _, msg_data = mail.uid("fetch", uid, "(RFC822.HEADER)")
            if not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1] if isinstance(msg_data[0], tuple) else b""
            msg = email_mod.message_from_bytes(raw)
            
            subj = ""
            for part, enc in decode_header(msg.get("Subject", "")):
                if isinstance(part, bytes):
                    subj += part.decode(enc or "utf-8", errors="replace")
                else:
                    subj += part
            
            sender = msg.get("From", "")
            sender_email = ""
            if "<" in sender:
                sender_email = sender.split("<")[1].split(">")[0].lower()
            else:
                sender_email = sender.lower().strip()
            
            our_domains = ["reytechinc.com", "reytech.com"]
            is_self = any(sender_email.endswith(f"@{d}") for d in our_domains)
            is_fwd_subj = any(subj.lower().strip().startswith(p) for p in ["fwd:", "fw:"])
            in_json = uid_str in processed_json
            in_db = uid_str in processed_db
            
            emails.append({
                "uid": uid_str,
                "subject": subj[:80],
                "sender": sender_email,
                "is_self": is_self,
                "is_fwd": is_fwd_subj,
                "in_json": in_json,
                "in_db": in_db,
                "blocked": in_json or in_db,
                "date": msg.get("Date", "")[:30],
            })
        
        mail.logout()
        return jsonify({
            "ok": True,
            "total_in_window": len(uids),
            "processed_json": sorted(list(processed_json))[:20],
            "processed_db": sorted(list(processed_db))[:20],
            "emails": emails,
        })
    except Exception as e:
        import traceback
        return jsonify({"ok": False, "error": str(e), "tb": traceback.format_exc()})


@bp.route("/api/diag/nuke-and-poll")
@auth_required
def api_nuke_and_poll():
    """Nuclear option: clear ALL dedup layers and re-poll. GET to bypass CSRF."""
    try:
        _DATA_DIR = DATA_DIR
    except NameError:
        from src.core.paths import DATA_DIR as _DATA_DIR
    # CRITICAL: _shared_poller lives in dashboard.py's module globals.
    # `global _shared_poller` here would reference routes_pricecheck's copy,
    # NOT the one that _safe_do_poll_check() reads. Must access directly.
    import src.api.dashboard as _dash
    cleared = {}
    
    # 0. PAUSE background poller to prevent race condition
    POLL_STATUS["paused"] = True
    import time as _time
    _time.sleep(0.5)  # Let any in-flight poll finish
    
    # 0b. Clear in-memory processed set on existing poller
    _old_poller = getattr(_dash, '_shared_poller', None)
    if _old_poller and hasattr(_old_poller, '_processed'):
        cleared["in_memory_cleared"] = len(_old_poller._processed)
        _old_poller._processed.clear()
        try:
            _old_poller._save_processed()
        except Exception:
            pass
    
    # Kill dashboard's poller (this is the one _safe_do_poll_check uses)
    _dash._shared_poller = None
    
    # 1. Clear JSON processed file(s) — both inboxes
    for _pf_name in ("processed_emails.json", "processed_emails_mike.json"):
        _pf = os.path.join(_DATA_DIR, _pf_name)
        try:
            if os.path.exists(_pf):
                with open(_pf) as f:
                    old = json.load(f)
                cleared[_pf_name] = len(old) if isinstance(old, list) else 0
            else:
                cleared[_pf_name] = "not found"
            # Write empty list (not delete — prevents re-creation race)
            with open(_pf, "w") as f:
                json.dump([], f)
        except Exception as e:
            cleared[f"{_pf_name}_error"] = str(e)
    
    # 2. Clear SQLite processed_emails
    try:
        from src.core.db import get_db
        with get_db() as conn:
            n = conn.execute("SELECT COUNT(*) FROM processed_emails").fetchone()[0]
            conn.execute("DELETE FROM processed_emails")
            cleared["db_processed"] = n
    except Exception as e:
        cleared["db_error"] = str(e)
    
    # 3. Clear SQLite email_fingerprints
    try:
        from src.core.db import get_db
        with get_db() as conn:
            n = conn.execute("SELECT COUNT(*) FROM email_fingerprints").fetchone()[0]
            conn.execute("DELETE FROM email_fingerprints")
            cleared["db_fingerprints"] = n
    except Exception as e:
        cleared["fp_error"] = str(e)
    
    # 4. Poller already killed above via _dash._shared_poller = None
    cleared["poller"] = "reset"
    
    # 5. Re-poll (with background poller still paused)
    try:
        imported = _safe_do_poll_check()
        # 6. Unpause background poller
        POLL_STATUS["paused"] = False
        return jsonify({
            "ok": True,
            "cleared": cleared,
            "found": len(imported),
            "rfqs": [{"id": r.get("id","?"), "subject": r.get("subject","")[:60]} for r in imported],
            "traces": POLL_STATUS.get("_email_traces", [])[-30:],
            "sales_diag": POLL_STATUS.get("_diag", {}),
            "mike_diag": POLL_STATUS.get("_mike_diag", {}),
        })
    except Exception as e:
        POLL_STATUS["paused"] = False
        return jsonify({"ok": False, "cleared": cleared, "error": str(e)})


@bp.route("/api/diag/find-rfq")
@auth_required
def api_diag_find_rfq():
    """Search all RFQs and PCs for a keyword (sol number, subject, sender).
    Usage: /api/diag/find-rfq?q=10840486
    """
    q = request.args.get("q", "").lower()
    if not q:
        return jsonify({"error": "Pass ?q=keyword"})
    
    rfqs = load_rfqs()
    pcs = _load_price_checks()
    
    rfq_hits = []
    for rid, r in rfqs.items():
        searchable = json.dumps(r, default=str).lower()
        if q in searchable:
            rfq_hits.append({
                "id": rid,
                "sol": r.get("solicitation_number", "?"),
                "status": r.get("status", "?"),
                "subject": r.get("email_subject", "")[:80],
                "sender": r.get("email_sender", r.get("requestor_email", "")),
                "email_uid": r.get("email_uid", "")[:20],
                "created_at": r.get("created_at", ""),
            })
    
    pc_hits = []
    for pid, p in pcs.items():
        searchable = json.dumps(p, default=str).lower()
        if q in searchable:
            pc_hits.append({
                "id": pid,
                "pc_number": p.get("pc_number", "?"),
                "status": p.get("status", "?"),
                "institution": p.get("institution", ""),
                "email_uid": p.get("email_uid", "")[:20],
            })
    
    return jsonify({
        "query": q,
        "rfq_matches": rfq_hits,
        "pc_matches": pc_hits,
        "total_rfqs": len(rfqs),
        "total_pcs": len(pcs),
        "all_rfq_uids": {rid: {"uid": r.get("email_uid", ""), "sol": r.get("solicitation_number", "?")} for rid, r in rfqs.items()},
    })


@bp.route("/api/diag")
@auth_required
def api_diag():
    """Diagnostic endpoint — shows email config, connection test, and inbox status."""
    import traceback
    try:
        return _api_diag_inner()
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})

def _api_diag_inner():
    import traceback
    email_cfg = CONFIG.get("email", {})
    addr = email_cfg.get("email", "NOT SET")
    has_pw = bool(email_cfg.get("email_password"))
    host = email_cfg.get("imap_host", "imap.gmail.com")
    port = email_cfg.get("imap_port", 993)
    
    diag = {
        "config": {
            "email_address": addr,
            "has_password": has_pw,
            "password_length": len(email_cfg.get("email_password", "")),
            "imap_host": host,
            "imap_port": port,
            "imap_folder": email_cfg.get("imap_folder", "INBOX"),
        },
        "env_vars": {
            "GMAIL_ADDRESS_set": bool(os.environ.get("GMAIL_ADDRESS")),
            "GMAIL_PASSWORD_set": bool(os.environ.get("GMAIL_PASSWORD")),
            "GMAIL_ADDRESS_value": os.environ.get("GMAIL_ADDRESS", "NOT SET"),
        },
        "poll_status": POLL_STATUS,
        "connection_test": None,
        "inbox_test": None,
    }
    
    # Test IMAP connection
    try:
        import imaplib
        mail = imaplib.IMAP4_SSL(host, port)
        diag["connection_test"] = "SSL connected OK"
        
        try:
            mail.login(addr, email_cfg.get("email_password", ""))
            diag["connection_test"] = f"Logged in as {addr} OK"
            
            try:
                mail.select("INBOX")
                # Check total
                status, messages = mail.search(None, "ALL")
                total = len(messages[0].split()) if status == "OK" and messages[0] else 0
                # Check recent (last 3 days) — same as poller
                since_date = (datetime.now() - timedelta(days=3)).strftime("%d-%b-%Y")
                status3, recent = mail.uid("search", None, f"(SINCE {since_date})")
                recent_count = len(recent[0].split()) if status3 == "OK" and recent[0] else 0
                
                # Check how many already processed
                proc_file = os.path.join(DATA_DIR, "processed_emails.json")
                processed_uids = set()
                if os.path.exists(proc_file):
                    try:
                        with open(proc_file) as pf:
                            processed_uids = set(json.load(pf))
                    except Exception as e:

                        log.debug("Suppressed: %s", e)
                
                recent_uids = recent[0].split() if status3 == "OK" and recent[0] else []
                new_to_process = [u.decode() for u in recent_uids if u.decode() not in processed_uids]
                
                diag["inbox_test"] = {
                    "total_emails": total,
                    "recent_3_days": recent_count,
                    "already_processed": recent_count - len(new_to_process),
                    "new_to_process": len(new_to_process),
                }
                
                # Show subjects of emails that would be processed
                if new_to_process:
                    subjects = []
                    for uid_str in new_to_process[:5]:
                        st, data = mail.uid("fetch", uid_str.encode(), "(BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)])")
                        if st == "OK":
                            subjects.append(data[0][1].decode("utf-8", errors="replace").strip())
                    diag["inbox_test"]["new_email_subjects"] = subjects
                
            except Exception as e:
                diag["inbox_test"] = f"SELECT/SEARCH failed: {e}"
            
            mail.logout()
        except imaplib.IMAP4.error as e:
            diag["connection_test"] = f"LOGIN FAILED: {e}"
        except Exception as e:
            diag["connection_test"] = f"LOGIN ERROR: {e}"
    except Exception as e:
        diag["connection_test"] = f"SSL CONNECT FAILED: {e}"
        diag["connection_traceback"] = traceback.format_exc()
    
    # Check processed emails file
    proc_file = email_cfg.get("processed_file", os.path.join(DATA_DIR, "processed_emails.json"))
    if os.path.exists(proc_file):
        try:
            with open(proc_file) as f:
                processed = json.load(f)
            diag["processed_emails"] = {"count": len(processed), "ids": processed[-10:] if isinstance(processed, list) else list(processed)[:10]}
        except Exception as e:
            log.debug("Suppressed: %s", e)
            diag["processed_emails"] = "corrupt file"
    else:
        diag["processed_emails"] = "file not found"
    
    # SCPRS diagnostics
    diag["scprs"] = {
        "db_stats": get_price_db_stats(),
        "db_exists": os.path.exists(os.path.join(BASE_DIR, "data", "scprs_prices.json")),
    }
    try:
        from src.agents.scprs_lookup import test_connection
        import threading
        result = [False, "timeout"]
        def _test():
            try:
                result[0], result[1] = test_connection()
            except Exception as ex:
                result[1] = str(ex)
        t = threading.Thread(target=_test, daemon=True)
        t.start()
        t.join(timeout=15)  # Max 15 seconds for connectivity test (may need 2-3 loads)
        diag["scprs"]["fiscal_reachable"] = result[0]
        diag["scprs"]["fiscal_status"] = result[1]
    except Exception as e:
        diag["scprs"]["fiscal_reachable"] = False
        diag["scprs"]["fiscal_error"] = str(e)
    
    return jsonify(diag)


@bp.route("/api/reset-processed")
@auth_required
def api_reset_processed():
    """Clear the processed emails list so all recent emails get re-scanned."""
    global _shared_poller
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if os.path.exists(proc_file):
        os.remove(proc_file)
    _shared_poller = None  # Force new poller instance
    return jsonify({"ok": True, "message": "Processed emails list cleared. Hit Check Now to re-scan."})


# ═══════════════════════════════════════════════════════════════════════
# Pricing Oracle API (v6.0)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricing/recommend", methods=["POST"])
@auth_required
def api_pricing_recommend():
    """Get three-tier pricing recommendation for an RFQ's line items."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Pricing oracle not available — check won_quotes_db.py and pricing_oracle.py are in repo"}), 503

    data = request.get_json(force=True, silent=True) or {}
    rid = data.get("rfq_id")

    if rid:
        rfqs = load_rfqs()
        rfq = rfqs.get(rid)
        if not rfq:
            return jsonify({"error": f"RFQ {rid} not found"}), 404
        result = recommend_prices_for_rfq(rfq, config_overrides=data.get("config"))
    else:
        result = recommend_prices_for_rfq(data, config_overrides=data.get("config"))

    return jsonify(result)


@bp.route("/api/won-quotes/search")
@auth_required
def api_won_quotes_search():
    """Search the Won Quotes Knowledge Base."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    query = request.args.get("q", "")
    item_number = request.args.get("item", "")
    max_results = int(request.args.get("max", 10))

    if not query and not item_number:
        return jsonify({"error": "Provide ?q=description or ?item=number"}), 400

    results = find_similar_items(
        item_number=item_number,
        description=query,
        max_results=max_results,
    )
    return jsonify({"query": query, "item_number": item_number, "results": results})


@bp.route("/api/won-quotes/stats")
@auth_required
def api_won_quotes_stats():
    """Get Won Quotes KB statistics and pricing health check."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    stats = get_kb_stats()
    health = pricing_health_check()
    return jsonify({"stats": stats, "health": health})


@bp.route("/api/won-quotes/dump")
@auth_required
def api_won_quotes_dump():
    """Debug: show first 10 raw KB records to verify what's stored."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import load_won_quotes
    quotes = load_won_quotes()
    return jsonify({"total": len(quotes), "first_10": quotes[:10]})


@bp.route("/api/debug/paths")
@auth_required
@safe_route
def api_debug_paths():
    """Debug: show actual filesystem paths and what exists."""
    try:
        from src.knowledge import won_quotes_db
    except ImportError:
        import won_quotes_db
    results = {
        "dashboard_BASE_DIR": BASE_DIR,
        "dashboard_DATA_DIR": DATA_DIR,
        "won_quotes_DATA_DIR": won_quotes_db.DATA_DIR,
        "won_quotes_FILE": won_quotes_db.WON_QUOTES_FILE,
        "cwd": os.getcwd(),
        "app_file_location": os.path.abspath(__file__),
    }
    # Check what exists
    for path_name, path_val in list(results.items()):
        if path_val and os.path.exists(path_val):
            if os.path.isdir(path_val):
                try:
                    results[f"{path_name}_contents"] = os.listdir(path_val)
                except Exception as e:
                    log.debug("Suppressed: %s", e)
                    results[f"{path_name}_contents"] = "permission denied"
            else:
                results[f"{path_name}_exists"] = True
                results[f"{path_name}_size"] = os.path.getsize(path_val)
        else:
            results[f"{path_name}_exists"] = False
    # Check /app/data specifically
    for check_path in ["/app/data", "/app", DATA_DIR]:
        key = check_path.replace("/", "_")
        results[f"check{key}_exists"] = os.path.exists(check_path)
        if os.path.exists(check_path) and os.path.isdir(check_path):
            try:
                results[f"check{key}_contents"] = os.listdir(check_path)
            except Exception as e:
                log.debug("Suppressed: %s", e)
                results[f"check{key}_contents"] = "permission denied"
    return jsonify(results)


@bp.route("/api/debug/pcs")
@auth_required
def api_debug_pcs():
    """Debug: show price_checks.json state for persistence troubleshooting."""
    pc_path = os.path.join(DATA_DIR, "price_checks.json")
    result = {
        "data_dir": DATA_DIR,
        "pc_path": pc_path,
        "pc_file_exists": os.path.exists(pc_path),
    }
    if os.path.exists(pc_path):
        result["pc_file_size"] = os.path.getsize(pc_path)
        result["pc_file_mtime"] = os.path.getmtime(pc_path)
        try:
            pcs = _load_price_checks()
            result["pc_count"] = len(pcs)
            result["pc_ids"] = list(pcs.keys())[:20]
            result["pc_statuses"] = {pid: pc.get("status", "?") for pid, pc in list(pcs.items())[:20]}
            # Check user-facing filter
            from src.api.dashboard import _is_user_facing_pc
            user_facing = {pid: pc for pid, pc in pcs.items() if _is_user_facing_pc(pc)}
            result["user_facing_count"] = len(user_facing)
            result["filtered_out"] = len(pcs) - len(user_facing)
            if result["filtered_out"] > 0:
                filtered = {pid: {"status": pc.get("status"), "source": pc.get("source"), 
                                  "is_auto_draft": pc.get("is_auto_draft"), "rfq_id": pc.get("rfq_id")}
                            for pid, pc in pcs.items() if not _is_user_facing_pc(pc)}
                result["filtered_details"] = filtered
        except Exception as e:
            result["error"] = str(e)
    else:
        result["pc_count"] = 0
        result["note"] = "price_checks.json does not exist!"
    # Also check volume status
    try:
        from src.core.paths import _USING_VOLUME
        result["using_volume"] = _USING_VOLUME
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    return jsonify(result)


@bp.route("/api/won-quotes/migrate")
@auth_required
def api_won_quotes_migrate():
    """One-time migration: import existing scprs_prices.json into Won Quotes KB."""
    try:
        from src.agents.scprs_lookup import migrate_local_db_to_won_quotes
        result = migrate_local_db_to_won_quotes()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/won-quotes/seed")
@auth_required
def api_won_quotes_seed():
    """Start bulk SCPRS seed: searches ~20 common categories, drills into PO details,
    ingests unit prices into Won Quotes KB. Runs in background thread (~3-5 min)."""
    try:
        from src.agents.scprs_lookup import bulk_seed_won_quotes, SEED_STATUS
        if SEED_STATUS.get("running"):
            return jsonify({"ok": False, "message": "Seed already running", "status": SEED_STATUS})
        t = threading.Thread(target=bulk_seed_won_quotes, daemon=True)
        t.start()
        return jsonify({"ok": True, "message": "Seed started in background. Check progress at /api/won-quotes/seed-status"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/won-quotes/seed-status")
@auth_required
def api_won_quotes_seed_status():
    """Check progress of bulk SCPRS seed job."""
    try:
        from src.agents.scprs_lookup import SEED_STATUS
        return jsonify(SEED_STATUS)
    except Exception as e:
        return jsonify({"error": str(e)})


@bp.route("/api/pricecheck/<pcid>/dismiss", methods=["POST"])
@auth_required
def api_pricecheck_dismiss(pcid):
    """Dismiss a PC from the active queue with a reason.
    Keeps data for SCPRS intelligence. reason=delete does hard delete.
    Valid reasons: dismissed, archived, duplicate, no_response, delete"""
    from datetime import datetime

    data = request.get_json(force=True) if request.data else {}
    reason = data.get("reason", "other")
    
    # Hard delete path
    if reason == "delete":
        return api_pricecheck_delete(pcid)
    
    pcs = _load_price_checks()

    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})

    pc = pcs[pcid]
    # Use the reason as the status directly for known actions
    valid_statuses = {"not_responding", "dismissed", "archived", "duplicate", "no_response", "won", "lost"}
    # Map UI reasons to appropriate statuses
    _reason_map = {"cs_question": "dismissed", "other": "dismissed"}
    new_status = _reason_map.get(reason, reason) if reason not in valid_statuses else reason
    if new_status not in valid_statuses:
        new_status = "dismissed"
    pc["status"] = new_status
    pc["dismiss_reason"] = reason
    pc["dismissed_at"] = datetime.now().isoformat()
    pcs[pcid] = pc

    _save_single_pc(pcid, pc)

    log.info("PC %s dismissed: reason=%s pc_number=%s", pcid, reason, pc.get("pc_number","?"))
    
    # Queue SCPRS price intelligence pull on the items (async)
    scprs_queued = False
    items = pc.get("items", [])
    if items:
        try:
            from src.agents.scprs_lookup import queue_background_lookup
            for item in items[:20]:
                desc = item.get("description", "")
                if desc and len(desc) > 3:
                    queue_background_lookup(desc, source=f"dismissed_pc_{pcid}")
            scprs_queued = True
        except Exception as e:
            log.debug("SCPRS queue for dismissed PC: %s", e)
    
    return jsonify({
        "ok": True,
        "dismissed": pcid,
        "reason": reason,
        "scprs_queued": scprs_queued,
    })


@bp.route("/api/pricecheck/<pcid>/delete", methods=["GET", "POST"])
@auth_required
def api_pricecheck_delete(pcid):
    """Delete a price check by ID. Also removes linked quote draft and recalculates counter."""
    pcs = _load_price_checks()

    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})

    pc = pcs[pcid]
    pc_num = pc.get("pc_number", pcid)
    linked_qn = pc.get("reytech_quote_number", "") or pc.get("linked_quote_number", "")

    # Mark dismissed (Law 22: never truly delete)
    pcs[pcid]["status"] = "dismissed"
    _save_single_pc(pcid, pc)

    # Also remove from SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM price_checks WHERE id=?", (pcid,))
    except Exception as e:
        log.debug("SQLite PC delete: %s", e)

    # Remove the linked draft quote from quotes_log.json so the number is freed
    quote_removed = False
    if linked_qn:
        try:
            from src.forms.quote_generator import get_all_quotes, _save_all_quotes
            all_quotes = get_all_quotes()
            before = len(all_quotes)
            all_quotes = [q for q in all_quotes
                          if not (q.get("quote_number") == linked_qn
                                  and q.get("status") in ("draft", "pending"))]
            if len(all_quotes) < before:
                _save_all_quotes(all_quotes)
                quote_removed = True
                log.info("Removed draft quote %s (linked to deleted PC %s)", linked_qn, pcid)

                # Also remove from SQLite quotes table
                try:
                    with get_db() as conn:
                        conn.execute("DELETE FROM quotes WHERE quote_number=? AND status IN ('draft','pending')", (linked_qn,))
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
        except Exception as e:
            log.debug("Quote cleanup: %s", e)

    # Recalculate counter — set to highest remaining quote number
    counter_reset = None
    if quote_removed:
        try:
            import re as _re
            from src.forms.quote_generator import get_all_quotes, _load_counter, _save_counter
            all_quotes = get_all_quotes()
            max_seq = 0
            for q in all_quotes:
                qn = q.get("quote_number", "")
                m = _re.search(r'R\d{2}Q(\d+)', qn)
                if m and not q.get("is_test"):
                    max_seq = max(max_seq, int(m.group(1)))
            # Also check remaining PCs
            remaining_pcs = _load_price_checks()
            for rpc in remaining_pcs.values():
                qn = rpc.get("reytech_quote_number", "") or ""
                m = _re.search(r'R\d{2}Q(\d+)', qn)
                if m:
                    max_seq = max(max_seq, int(m.group(1)))
            old_counter = _load_counter()
            if max_seq < old_counter.get("seq", 0):
                _save_counter({"year": old_counter.get("year", 2026), "seq": max_seq})
                counter_reset = f"Q{old_counter['seq']} → Q{max_seq} (next will be Q{max_seq + 1})"
                log.info("Quote counter reset: %s", counter_reset)
        except Exception as e:
            log.debug("Counter recalc: %s", e)

    log.info("DELETED PC %s (%s)%s", pcid, pc_num,
             f" + quote {linked_qn}" if quote_removed else "")
    return jsonify({
        "ok": True, "deleted": pcid,
        "quote_removed": linked_qn if quote_removed else None,
        "counter_reset": counter_reset,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# PC Lifecycle Endpoints + Award Monitor + Competitors
# ═══════════════════════════════════════════════════════════════════════════════

PC_STATUS_LABELS = {
    "new":            ("New",             "#4f8cff"),
    "parsed":         ("New",             "#4f8cff"),
    "parse_error":    ("New",             "#4f8cff"),
    "draft":          ("Draft",           "#fbbf24"),
    "priced":         ("Draft",           "#fbbf24"),
    "ready":          ("Draft",           "#fbbf24"),
    "auto_drafted":   ("Draft",           "#fbbf24"),
    "quoted":         ("Draft",           "#fbbf24"),
    "generated":      ("Draft",           "#fbbf24"),
    "completed":      ("Draft",           "#fbbf24"),
    "converted":      ("Draft",           "#fbbf24"),
    "pending_award":  ("Sent",            "#3fb950"),
    "sent":           ("Sent",            "#3fb950"),
    "won":            ("Sent",            "#3fb950"),
    "lost":           ("Not Responding",  "#f85149"),
    "expired":        ("Not Responding",  "#f85149"),
    "no_response":    ("Not Responding",  "#f85149"),
    "dismissed":      ("Not Responding",  "#f85149"),
    "archived":       ("Not Responding",  "#f85149"),
    "duplicate":      ("Not Responding",  "#f85149"),
}


@bp.route("/pricecheck")
@auth_required
def pricecheck_redirect():
    """Redirect /pricecheck → /pricechecks (common typo/nav issue)"""
    return redirect("/pricechecks")


@bp.route("/pricechecks")
@auth_required
def pricechecks_archive():
    """PC Archive — searchable, filterable list of all price checks."""
    pcs = _load_price_checks()
    pc_list = []
    for pcid, pc in pcs.items():
        pc_list.append({
            "id": pcid, "pc_number": pc.get("pc_number", "?"),
            "institution": pc.get("institution", ""), "requestor": pc.get("requestor", ""),
            "status": pc.get("status", "new"), "items_count": len(pc.get("items", [])),
            "quote_number": pc.get("reytech_quote_number", ""),
            "created_at": pc.get("created_at", ""), "sent_at": pc.get("sent_at", ""),
            "due_date": pc.get("due_date", "") or pc.get("parsed", {}).get("header", {}).get("due_date", ""),
            "source": pc.get("source", ""),
            "competitor_name": pc.get("competitor_name", ""),
            "competitor_price": pc.get("competitor_price", 0),
            "revision_of": pc.get("revision_of", ""),
            "total": sum((it.get("unit_price") or it.get("pricing", {}).get("recommended_price", 0) or 0) * it.get("qty", 1)
                        for it in pc.get("items", [])),
        })
    pc_list.sort(key=lambda x: (
        # Overdue items first (0 = overdue, 1 = not)
        0 if x.get("due_date") and x["due_date"][:10] < datetime.now().strftime("%Y-%m-%d") else 1,
        # Then by due date ascending (soonest first)
        x.get("due_date", "9999") or "9999",
        # Then by created_at descending
        "" if not x.get("created_at") else x["created_at"],
    ))
    # Reverse created_at within non-due items
    total = len(pc_list)

    # Map internal statuses → 4 display statuses
    DISPLAY_STATUS = {
        "new": "new", "parsed": "new", "parse_error": "new",
        "draft": "draft", "priced": "draft", "ready": "draft", "auto_drafted": "draft",
        "quoted": "draft", "generated": "draft", "completed": "draft", "converted": "draft",
        "sent": "sent", "pending_award": "sent", "won": "sent",
        "lost": "not_responding", "expired": "not_responding", "no_response": "not_responding",
        "dismissed": "not_responding", "archived": "not_responding", "duplicate": "not_responding",
    }
    # Add display_status to each PC for filtering
    for p in pc_list:
        p["display_status"] = DISPLAY_STATUS.get(p["status"], "new")

    by_display = {}
    for p in pc_list:
        ds = p["display_status"]
        by_display[ds] = by_display.get(ds, 0) + 1
    total_sent = by_display.get("sent", 0)
    total_not_responding = by_display.get("not_responding", 0)
    total_draft = by_display.get("draft", 0)
    total_new = by_display.get("new", 0)

    status_options = ""
    if total_new: status_options += f'<option value="new">🆕 New ({total_new})</option>'
    if total_draft: status_options += f'<option value="draft">📝 Draft ({total_draft})</option>'
    if total_sent: status_options += f'<option value="sent">📨 Sent ({total_sent})</option>'
    if total_not_responding: status_options += f'<option value="not_responding">📭 Not Responding ({total_not_responding})</option>'

    # Status badge styling — 4 clean statuses
    STATUS_BADGE = {
        "new":            ("🆕 New",            "rgba(79,140,255,.15)",  "#4f8cff"),
        "parsed":         ("🆕 New",            "rgba(79,140,255,.15)",  "#4f8cff"),
        "parse_error":    ("🆕 New",            "rgba(79,140,255,.15)",  "#4f8cff"),
        "draft":          ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "priced":         ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "ready":          ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "auto_drafted":   ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "quoted":         ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "generated":      ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "completed":      ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "converted":      ("📝 Draft",          "rgba(251,191,36,.15)",  "#fbbf24"),
        "pending_award":  ("📨 Sent",           "rgba(63,185,80,.2)",    "#3fb950"),
        "sent":           ("📨 Sent",           "rgba(63,185,80,.2)",    "#3fb950"),
        "won":            ("📨 Sent",           "rgba(63,185,80,.2)",    "#3fb950"),
        "lost":           ("📭 Not Responding", "rgba(248,81,73,.15)",   "#f85149"),
        "expired":        ("📭 Not Responding", "rgba(248,81,73,.15)",   "#f85149"),
        "no_response":    ("📭 Not Responding", "rgba(248,81,73,.15)",   "#f85149"),
        "dismissed":      ("📭 Not Responding", "rgba(248,81,73,.15)",   "#f85149"),
        "archived":       ("📭 Not Responding", "rgba(248,81,73,.15)",   "#f85149"),
        "duplicate":      ("📭 Not Responding", "rgba(248,81,73,.15)",   "#f85149"),
    }

    rows = ""
    for p in pc_list:
        st = p["status"]
        badge_label, badge_bg, badge_color = STATUS_BADGE.get(st, (st, "rgba(139,144,160,.15)", "#8b90a0"))
        date_str = p["created_at"][:10] if p["created_at"] else "—"
        due_str = p.get("due_date", "")[:10] if p.get("due_date") else "—"
        total_str = f"${p['total']:,.2f}" if p["total"] else "—"
        qn = p.get("quote_number", "")
        src_icon = "📧" if p.get("source") == "email_auto" else "📄" if p.get("source") == "manual_upload" else ""
        sent_elapsed = ""
        if p.get("sent_at"):
            try:
                from datetime import datetime as _dt
                _sd = _dt.fromisoformat(p["sent_at"][:19])
                _dd = (_dt.now() - _sd).days
                if _dd == 0: sent_elapsed = "today"
                elif _dd == 1: sent_elapsed = "1d ago"
                elif _dd < 30: sent_elapsed = f"{_dd}d ago"
                elif _dd < 60: sent_elapsed = "1mo ago"
                else: sent_elapsed = f"{_dd // 30}mo ago"
            except Exception:
                pass
        # Build rich search index with all visible fields
        search_index = f"{p['pc_number'].lower()} {p['institution'].lower()} {p['requestor'].lower()} {qn.lower()} {p['display_status']} {badge_label.lower()} {due_str} {date_str}"
        # Overdue detection
        is_overdue = False
        try:
            if p.get("due_date") and p["due_date"][:10] < datetime.now().strftime("%Y-%m-%d") and st not in ('sent','won','lost','archived','no_response','duplicate','dismissed'):
                is_overdue = True
        except Exception:
            pass
        overdue_style = "border-left:3px solid #f85149;" if is_overdue else ""
        due_color = "#f85149;font-weight:700" if is_overdue else "var(--tx2)"
        rows += f'''<tr data-status="{p['display_status']}" data-search="{search_index}" data-id="{p['id']}" style="cursor:pointer;{overdue_style}" onclick="if(!event.target.closest('input,button'))location.href='/pricecheck/{p['id']}'">
         <td style="padding:8px 6px;text-align:center" onclick="event.stopPropagation()"><input type="checkbox" class="pc-bulk-check" value="{p['id']}" onchange="updateBulkBar()" style="width:16px;height:16px;cursor:pointer"></td>
         <td style="padding:14px 12px"><a href="/pricecheck/{p['id']}" style="color:#58a6ff;font-family:'JetBrains Mono',monospace;font-weight:700;font-size:15px">#{p['pc_number']}</a></td>
         <td style="padding:14px 12px;font-size:15px;font-weight:500">{p['institution']}</td>
         <td style="padding:14px 12px;font-size:15px">{p['requestor'][:30]}</td>
         <td style="padding:14px 12px;font-size:15px;font-family:'JetBrains Mono',monospace;color:{due_color}">{due_str}{' 🔴' if is_overdue else ''}</td>
         <td style="padding:14px 12px;font-size:15px;font-family:'JetBrains Mono',monospace;color:var(--tx2)">{date_str}</td>
         <td style="padding:14px 12px;text-align:center;font-size:16px;font-weight:700">{p['items_count']}</td>
         <td style="padding:14px 12px;text-align:right;font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace">{total_str}</td>
         <td style="padding:14px 12px;text-align:center">{f'<span style="color:#58a6ff;font-family:JetBrains Mono,monospace;font-weight:700;font-size:14px">{qn}</span>' if qn else chr(8212)}</td>
         <td style="padding:14px 12px;text-align:center"><span style="display:inline-block;padding:4px 12px;border-radius:14px;font-size:14px;font-weight:600;background:{badge_bg};color:{badge_color};white-space:nowrap">{badge_label}</span> {src_icon}</td>
         <td style="padding:14px 12px;text-align:center;font-size:14px;color:#8b949e">{sent_elapsed}</td>
         <td style="padding:6px 8px;text-align:center" onclick="event.stopPropagation()"><button onclick="quickDismiss('{p['id']}','archived')" title="Archive" style="background:none;border:none;color:#8b949e;cursor:pointer;font-size:16px;padding:4px">🗄️</button><button onclick="quickDismiss('{p['id']}','duplicate')" title="Duplicate" style="background:none;border:none;color:#8b949e;cursor:pointer;font-size:16px;padding:4px">📋</button><button onclick="quickDismiss('{p['id']}','delete')" title="Delete" style="background:none;border:none;color:#f85149;cursor:pointer;font-size:16px;padding:4px">🗑</button></td></tr>'''

    content = f'''
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
      <h2 style="margin:0;font-size:26px;font-weight:700">📋 Price Check Archive</h2>
      <div style="display:flex;gap:10px;align-items:center">
        <form method="POST" action="/upload" enctype="multipart/form-data" style="display:inline-flex;gap:6px;align-items:center">
          <input type="file" name="files" accept=".pdf" id="pc-upload-file" style="display:none" onchange="this.form.submit()">
          <button type="button" onclick="document.getElementById('pc-upload-file').click()" class="btn btn-g" style="font-size:15px;font-weight:600;padding:10px 20px">📄 Upload 704 PDF</button>
        </form>
        <a href="/competitors" class="btn btn-p" style="font-size:15px;font-weight:600;padding:10px 20px;text-decoration:none">📊 Competitors</a>
      </div>
    </div>
    <div style="display:flex;gap:14px;margin-bottom:20px;flex-wrap:wrap">
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#4f8cff">{total}</div><div style="font-size:14px;color:var(--tx2);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Total</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#4f8cff">{total_new}</div><div style="font-size:14px;color:var(--tx2);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">New</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#fbbf24">{total_draft}</div><div style="font-size:14px;color:var(--tx2);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Draft</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#3fb950">{total_sent}</div><div style="font-size:14px;color:var(--tx2);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Sent</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#f85149">{total_not_responding}</div><div style="font-size:14px;color:var(--tx2);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Not Responding</div></div>
    </div>
    <div style="display:flex;gap:10px;margin-bottom:14px;align-items:center">
      <input id="pc-search" placeholder="🔍 Search PC#, institution, requestor, status..." oninput="filterPCs()" style="flex:1;padding:10px 16px;background:var(--sf);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:16px">
      <select id="pc-status" onchange="filterPCs()" style="padding:10px 14px;background:var(--sf);border:1px solid var(--bd);border-radius:8px;color:var(--tx);font-size:15px">
        <option value="">All Statuses</option>{status_options}</select>
      <span id="pc-count" style="font-size:15px;color:var(--tx2);white-space:nowrap">{total} PCs</span>
    </div>
    <div id="bulk-bar" style="display:none;align-items:center;gap:12px;padding:8px 16px;background:rgba(88,166,255,.08);border:1px solid rgba(88,166,255,.25);border-radius:8px;margin-bottom:8px">
      <span id="bulk-count" style="font-size:14px;font-weight:600;color:#58a6ff">0 selected</span>
      <button onclick="bulkAction('archived')" style="padding:4px 12px;background:#21262d;border:1px solid #30363d;border-radius:6px;color:#8b949e;font-size:13px;cursor:pointer">🗄️ Archive</button>
      <button onclick="bulkAction('duplicate')" style="padding:4px 12px;background:#21262d;border:1px solid #30363d;border-radius:6px;color:#8b949e;font-size:13px;cursor:pointer">📋 Duplicate</button>
      <button onclick="bulkAction('delete')" style="padding:4px 12px;background:#21262d;border:1px solid #30363d;border-radius:6px;color:#f85149;font-size:13px;cursor:pointer">🗑 Delete</button>
    </div>
    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:10px;overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <thead><tr style="border-bottom:2px solid var(--bd);text-transform:uppercase;font-size:14px;color:var(--tx2);letter-spacing:.5px">
          <th style="padding:8px 6px;text-align:center;width:30px"><input type="checkbox" onchange="toggleAllPCs(this)" style="width:16px;height:16px;cursor:pointer" title="Select all"></th>
          <th style="padding:14px 12px;text-align:left;font-weight:600">PC #</th><th style="padding:14px 12px;text-align:left;font-weight:600">Institution</th>
          <th style="padding:14px 12px;text-align:left;font-weight:600">Requestor</th><th style="padding:14px 12px;text-align:left;font-weight:600">Due</th><th style="padding:14px 12px;text-align:left;font-weight:600">Created</th>
          <th style="padding:14px 12px;text-align:center;font-weight:600">Items</th><th style="padding:14px 12px;text-align:right;font-weight:600">Total</th>
          <th style="padding:14px 12px;text-align:center;font-weight:600">Quote</th><th style="padding:14px 12px;text-align:center;font-weight:600">Status</th><th style="padding:14px 12px;text-align:center;font-weight:600">Sent</th><th style="padding:6px 8px;text-align:center;font-weight:600"></th>
        </tr></thead>
        <tbody id="pc-tbody">{rows}</tbody>
      </table>
    </div>
    <script>
    function filterPCs(){{var q=document.getElementById('pc-search').value.toLowerCase();var st=document.getElementById('pc-status').value;var rows=document.querySelectorAll('#pc-tbody tr');var v=0;rows.forEach(function(r){{var ok=(!q||r.dataset.search.includes(q))&&(!st||r.dataset.status===st);r.style.display=ok?'':'none';if(ok)v++;}});document.getElementById('pc-count').textContent=v+' PCs';}}
    function quickDismiss(pcid, action){{
      var labels={{'archived':'Archive','duplicate':'Mark Duplicate','delete':'Delete'}};
      if(!confirm(labels[action]+' this PC?'))return;
      fetch('/api/pricecheck/'+pcid+'/dismiss',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{reason:action}})}})
      .then(function(r){{return r.json()}}).then(function(d){{
        if(d.ok){{location.reload()}}else{{alert('Error: '+(d.error||'unknown'))}}
      }});
    }}
    function toggleAllPCs(master){{
      document.querySelectorAll('.pc-bulk-check').forEach(function(cb){{
        if(cb.closest('tr').style.display!=='none') cb.checked=master.checked;
      }});
      updateBulkBar();
    }}
    function updateBulkBar(){{
      var checked=document.querySelectorAll('.pc-bulk-check:checked');
      var bar=document.getElementById('bulk-bar');
      if(checked.length>0){{
        bar.style.display='flex';
        document.getElementById('bulk-count').textContent=checked.length+' selected';
      }}else{{
        bar.style.display='none';
      }}
    }}
    function bulkAction(action){{
      var ids=Array.from(document.querySelectorAll('.pc-bulk-check:checked')).map(function(cb){{return cb.value}});
      if(!ids.length) return;
      var labels={{'archived':'Archive','duplicate':'Mark Duplicate','delete':'Delete'}};
      if(!confirm(labels[action]+' '+ids.length+' Price Check'+(ids.length>1?'s':'')+'?')) return;
      var done=0;var total=ids.length;
      ids.forEach(function(id){{
        fetch('/api/pricecheck/'+id+'/dismiss',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{reason:action}})}})
        .then(function(r){{return r.json()}}).then(function(){{done++;if(done>=total)location.reload()}});
      }});
    }}
    </script>'''

    from src.api.render import render_page
    return render_page("generic.html", active_page="PCs", page_title="Price Checks", content=content)


@bp.route("/api/pricechecks")
@auth_required
def api_pricechecks_list():
    """API: List all PCs with optional status filter."""
    pcs = _load_price_checks()
    status_filter = request.args.get("status", "")
    result = []
    for pcid, pc in pcs.items():
        if status_filter and pc.get("status", "new") != status_filter:
            continue
        result.append({"id": pcid, "pc_number": pc.get("pc_number", "?"),
            "institution": pc.get("institution", ""), "status": pc.get("status", "new"),
            "items_count": len(pc.get("items", [])), "quote_number": pc.get("reytech_quote_number", ""),
            "created_at": pc.get("created_at", ""), "competitor_name": pc.get("competitor_name", "")})
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"ok": True, "pcs": result, "count": len(result)})


@bp.route("/api/pricecheck/<pcid>/mark-sent", methods=["POST"])
@auth_required
def api_pricecheck_mark_sent(pcid):
    """Mark PC as sent — creates versioned document record in DB."""
    pcs = _load_price_checks()
    if pcid not in pcs: return jsonify({"ok": False, "error": "PC not found"})
    pc = pcs[pcid]
    data = request.get_json(force=True, silent=True) or {}
    
    now = datetime.now().isoformat()
    _transition_status(pc, "sent", actor="user", 
                      notes=data.get("notes", "704 sent to requestor"))
    pc["sent_at"] = now
    pc["award_status"] = "pending"
    pc["sent_to"] = data.get("sent_to", pc.get("requestor", ""))
    pc["sent_method"] = data.get("method", "email")
    
    # Create versioned document record
    doc_id = 0
    output_pdf = pc.get("output_pdf", "")
    if output_pdf and os.path.exists(output_pdf):
        import shutil
        # Copy to versioned filename: PC_BLS_IT_{pcid}_v1_sent_20260224.pdf
        pc_num = pc.get("pc_number", "") or ""
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip()) if pc_num.strip() else ""
        safe_name = f"{safe_name}_{pcid}" if safe_name else pcid
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get next version
        try:
            from src.core.db import get_sent_documents
            existing = get_sent_documents(pcid)
            ver = len(existing) + 1
        except Exception:
            ver = 1
        
        versioned_name = f"PC_{safe_name}_v{ver}_sent_{date_str}.pdf"
        versioned_path = os.path.join(DATA_DIR, versioned_name)
        shutil.copy2(output_pdf, versioned_path)
        
        # Store in DB with full item snapshot
        try:
            from src.core.db import create_sent_document
            doc_id = create_sent_document(
                pc_id=pcid, filepath=versioned_path,
                items=pc.get("items", []),
                header=pc.get("parsed", {}).get("header", {}),
                notes=data.get("notes", "Initial send"),
                created_by="user"
            )
            pc["current_doc_id"] = doc_id
        except Exception as e:
            log.warning("sent_document DB write failed: %s", e)
    
    _save_single_pc(pcid, pc)

    try:
        from src.core.dal import save_pc as _dal_save_pc
        _dal_save_pc(pc)
    except Exception as _e:
        log.debug("DAL save_pc: %s", _e)
    
    _log_crm_activity(pc.get("reytech_quote_number", pcid), "quote_sent",
        f"Quote sent for PC #{pc.get('pc_number','')} to {pc.get('institution','')}", actor="user")
    
    log.info("PC %s marked SENT: pc#=%s institution=%s doc_id=%s", 
             pcid, pc.get("pc_number"), pc.get("institution"), doc_id)
    return jsonify({"ok": True, "status": "sent", "sent_at": now, 
                    "doc_id": doc_id,
                    "doc_url": f"/pricecheck/{pcid}/document/{doc_id}" if doc_id else ""})


# ═══════════════════════════════════════════════════════════════════════
# PC Follow-Up Scanner (PRD-v32 F3)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricecheck/follow-up-scan")
@auth_required
def api_pc_follow_up_scan():
    """Scan PCs in 'sent' status that need follow-up.
    Returns PCs where sent_at is 3+ days ago with no response.
    ?days=3 (default) — minimum days since sent
    ?days=5 — 5 day threshold
    """
    from datetime import datetime as _dt, timedelta
    days_threshold = int(request.args.get("days", 3))
    cutoff = _dt.now() - timedelta(days=days_threshold)

    pcs = _load_price_checks()
    follow_ups = []
    for pcid, pc in pcs.items():
        status = pc.get("status", "")
        # Only look at "sent" or "pending_award" PCs
        if status not in ("sent", "pending_award"):
            continue

        sent_at = pc.get("sent_at", "")
        if not sent_at:
            continue

        try:
            sent_dt = _dt.fromisoformat(sent_at[:19])
        except (ValueError, TypeError):
            continue

        if sent_dt > cutoff:
            continue  # Not old enough

        days_since = (_dt.now() - sent_dt).days
        institution = pc.get("institution", "") or "Unknown"
        requestor = pc.get("requestor", "") or ""
        requestor_email = ""
        # Try to extract email from requestor field or contact info
        if "@" in requestor:
            requestor_email = requestor
        elif pc.get("contact_email"):
            requestor_email = pc["contact_email"]
        elif pc.get("parsed", {}).get("header", {}).get("buyer_email"):
            requestor_email = pc["parsed"]["header"]["buyer_email"]

        total = sum(
            (it.get("unit_price") or it.get("pricing", {}).get("recommended_price", 0) or 0)
            * it.get("qty", 1)
            for it in pc.get("items", [])
        )

        urgency = "normal"
        if days_since >= 10:
            urgency = "stale"
        elif days_since >= 7:
            urgency = "overdue"
        elif days_since >= 5:
            urgency = "due"

        follow_ups.append({
            "pc_id": pcid,
            "pc_number": pc.get("pc_number", ""),
            "institution": institution,
            "requestor": requestor,
            "requestor_email": requestor_email,
            "sent_at": sent_at,
            "days_since_sent": days_since,
            "total": round(total, 2),
            "items_count": len(pc.get("items", [])),
            "due_date": pc.get("due_date", ""),
            "urgency": urgency,
            "follow_up_count": pc.get("follow_up_count", 0),
            "last_follow_up": pc.get("last_follow_up_at", ""),
        })

    follow_ups.sort(key=lambda x: x["days_since_sent"], reverse=True)

    return jsonify({
        "ok": True,
        "total": len(follow_ups),
        "threshold_days": days_threshold,
        "follow_ups": follow_ups,
        "summary": {
            "stale": sum(1 for f in follow_ups if f["urgency"] == "stale"),
            "overdue": sum(1 for f in follow_ups if f["urgency"] == "overdue"),
            "due": sum(1 for f in follow_ups if f["urgency"] == "due"),
            "normal": sum(1 for f in follow_ups if f["urgency"] == "normal"),
            "total_value": round(sum(f["total"] for f in follow_ups), 2),
        },
    })


@bp.route("/api/pricecheck/<pcid>/log-follow-up", methods=["POST"])
@auth_required
def api_pc_log_follow_up(pcid):
    """Log that a follow-up was done on a sent PC."""
    from datetime import datetime as _dt
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.get_json(force=True, silent=True) or {}
    method = data.get("method", "email")  # email, phone, in_person
    notes = data.get("notes", "")
    now = _dt.now().isoformat()

    pc["follow_up_count"] = pc.get("follow_up_count", 0) + 1
    pc["last_follow_up_at"] = now

    # Add to history
    if "follow_up_history" not in pc:
        pc["follow_up_history"] = []
    pc["follow_up_history"].append({
        "timestamp": now,
        "method": method,
        "notes": notes,
        "follow_up_number": pc["follow_up_count"],
    })

    _save_single_pc(pcid, pc)

    _log_crm_activity(pc.get("reytech_quote_number", pcid), "pc_follow_up",
        f"Follow-up #{pc['follow_up_count']} ({method}) on PC #{pc.get('pc_number','')} — {pc.get('institution','')}",
        actor="user")

    return jsonify({"ok": True, "follow_up_count": pc["follow_up_count"]})


@bp.route("/api/pricecheck/<pcid>/mark-no-response", methods=["POST"])
@auth_required
def api_pc_mark_no_response(pcid):
    """Mark a PC as not responding after follow-up attempts."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    _transition_status(pc, "not_responding", actor="user",
                       notes=f"No response after {pc.get('follow_up_count', 0)} follow-ups")
    _save_single_pc(pcid, pc)
    return jsonify({"ok": True, "status": "not_responding"})


@bp.route("/pricecheck/<pcid>/documents")
@auth_required
def pricecheck_documents(pcid):
    """List all sent document versions for a PC."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return redirect("/pricechecks")
    from src.core.db import get_sent_documents
    docs = get_sent_documents(pcid)
    
    rows = ""
    for d in docs:
        status_badge = {"current": ("Current", "#3fb950"), "superseded": ("Superseded", "#8b949e")}.get(
            d.get("status", ""), ("?", "#8b949e"))
        rows += f'''<tr style="cursor:pointer" onclick="location.href='/pricecheck/{pcid}/document/{d['id']}'">
         <td style="font-family:monospace;font-weight:600;color:#58a6ff">v{d['version']}</td>
         <td>{d['created_at'][:19].replace('T',' ')}</td>
         <td>{d.get('notes','')[:40]}</td>
         <td>{d.get('change_summary','')[:60]}</td>
         <td><span style="background:{status_badge[1]};color:#0d1117;padding:2px 8px;border-radius:4px;font-size:14px;font-weight:600">{status_badge[0]}</span></td>
         <td style="text-align:right;font-family:monospace">{d.get('file_size',0)//1024}KB</td>
         <td><a href="/api/pricecheck/document/{d['id']}/pdf" style="color:#58a6ff">📥 Download</a></td>
        </tr>'''
    
    content = f'''
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 style="margin:0">📄 Sent Documents — PC #{pc.get("pc_number","?")}</h2>
      <a href="/pricecheck/{pcid}" style="color:#58a6ff;text-decoration:none;font-size:13px">← Back to PC Detail</a>
    </div>
    <div style="font-size:13px;color:var(--tx2);margin-bottom:16px">{pc.get("institution","")} · {len(docs)} version(s)</div>
    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;overflow:hidden">
     <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="border-bottom:1px solid var(--bd);font-size:14px;color:var(--tx2);text-transform:uppercase">
       <th style="padding:10px;text-align:left">Ver</th><th style="padding:10px">Date</th>
       <th style="padding:10px">Notes</th><th style="padding:10px">Changes</th>
       <th style="padding:10px">Status</th><th style="padding:10px;text-align:right">Size</th>
       <th style="padding:10px"></th>
      </tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--tx2)">No documents yet — mark PC as Sent to create the first version</td></tr>'}</tbody>
     </table>
    </div>'''
    from src.api.render import render_page
    return render_page("generic.html", active_page="PCs", page_title=f"Documents — PC #{pc.get('pc_number','?')}", content=content)


@bp.route("/api/pricecheck/document/<int:doc_id>/pdf")
@auth_required
def serve_sent_document_pdf(doc_id):
    """Serve a specific document version's PDF."""
    from src.core.db import get_sent_document
    doc = get_sent_document(doc_id)
    if not doc or not doc.get("filepath"):
        return jsonify({"ok": False, "error": "Document not found"}), 404
    fp = doc["filepath"]
    if not os.path.exists(fp):
        return jsonify({"ok": False, "error": "PDF file not found on disk"}), 404
    return send_file(fp, mimetype="application/pdf")


@bp.route("/pricecheck/<pcid>/document/<int:doc_id>")
@auth_required
def pricecheck_document_editor(pcid, doc_id):
    """Inline PDF viewer + editor for a sent document version."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return redirect("/pricechecks")
    
    from src.core.db import get_sent_document, get_sent_documents
    doc = get_sent_document(doc_id)
    if not doc:
        return redirect(f"/pricecheck/{pcid}/documents")
    
    all_docs = get_sent_documents(pcid)
    items = doc.get("items", []) or pc.get("items", [])
    header = doc.get("header", {}) or pc.get("parsed", {}).get("header", {})
    
    # Build version selector
    ver_options = "".join(
        f'<option value="{d["id"]}" {"selected" if d["id"]==doc_id else ""}>'
        f'v{d["version"]} — {d["created_at"][:16].replace("T"," ")}'
        f'{" (current)" if d.get("status")=="current" else ""}</option>'
        for d in all_docs
    )
    
    # Build editable item rows
    item_rows = ""
    for i, item in enumerate(items):
        desc = (item.get("description") or "").replace('"', '&quot;')
        mfg = (item.get("mfg_number") or "").replace('"', '&quot;')
        qty = item.get("qty", 1)
        uom = (item.get("uom") or "EA").upper()
        price = item.get("unit_price") or item.get("pricing", {}).get("recommended_price") or 0
        cost = item.get("vendor_cost") or item.get("pricing", {}).get("unit_cost") or 0
        ext = round(float(price) * int(qty), 2) if price else 0
        item_rows += f'''<tr>
         <td style="text-align:center;padding:8px;font-weight:600">{i+1}</td>
         <td style="padding:4px"><input name="ed_qty_{i}" value="{qty}" type="number" min="1" style="width:60px;background:var(--sf);border:1px solid var(--bd);border-radius:4px;padding:6px;color:var(--tx);font-size:13px;text-align:center" onchange="recalcDoc()"></td>
         <td style="padding:4px"><input name="ed_uom_{i}" value="{uom}" style="width:60px;background:var(--sf);border:1px solid var(--bd);border-radius:4px;padding:6px;color:var(--tx);font-size:13px;text-align:center"></td>
         <td style="padding:4px"><textarea name="ed_desc_{i}" rows="2" style="width:100%;background:var(--sf);border:1px solid var(--bd);border-radius:4px;padding:6px;color:var(--tx);font-size:14px;resize:vertical">{desc}</textarea></td>
         <td style="padding:4px"><input name="ed_mfg_{i}" value="{mfg}" style="width:120px;background:var(--sf);border:1px solid var(--bd);border-radius:4px;padding:6px;color:var(--tx);font-size:14px;font-family:monospace"></td>
         <td style="padding:4px"><input name="ed_price_{i}" value="{float(price):.2f}" type="number" step="0.01" min="0" style="width:90px;background:var(--sf);border:1px solid var(--bd);border-radius:4px;padding:6px;color:var(--tx);font-size:13px;text-align:right" onchange="recalcDoc()"></td>
         <td style="padding:8px;text-align:right;font-weight:600;font-family:monospace" class="doc-ext">${ext:,.2f}</td>
        </tr>'''
    
    change_log = ""
    if doc.get("change_summary"):
        change_log = f'<div style="font-size:14px;color:#d29922;margin-top:4px">Changes: {doc["change_summary"]}</div>'
    
    content = f'''
    <style>
     .doc-split {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; height:calc(100vh - 180px); }}
     .doc-pdf {{ border:1px solid var(--bd); border-radius:8px; overflow:hidden; background:#1e1e1e; }}
     .doc-editor {{ overflow-y:auto; }}
     @media(max-width:1100px) {{ .doc-split {{ grid-template-columns:1fr; height:auto; }} .doc-pdf {{ height:600px; }} }}
    </style>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
     <div>
      <h2 style="margin:0;font-size:18px">📄 PC #{pc.get("pc_number","")} — {pc.get("institution","")}</h2>
      <div style="font-size:14px;color:var(--tx2);margin-top:2px">
       Version {doc.get("version",1)} · {doc.get("created_at","")[:19].replace("T"," ")}
       · <span style="color:{("#3fb950" if doc.get("status")=="current" else "#8b949e")}">{doc.get("status","").title()}</span>
       {change_log}
      </div>
     </div>
     <div style="display:flex;gap:8px;align-items:center">
      <select id="verSelect" onchange="location.href='/pricecheck/{pcid}/document/'+this.value" style="background:var(--sf);border:1px solid var(--bd);border-radius:6px;padding:6px 10px;color:var(--tx);font-size:14px">{ver_options}</select>
      <a href="/pricecheck/{pcid}/documents" style="color:#58a6ff;font-size:14px;text-decoration:none">📋 All Versions</a>
      <a href="/pricecheck/{pcid}" style="color:#58a6ff;font-size:14px;text-decoration:none">← PC Detail</a>
     </div>
    </div>
    <div class="doc-split">
     <div class="doc-pdf">
      <iframe src="/api/pricecheck/document/{doc_id}/pdf" style="width:100%;height:100%;border:none"></iframe>
     </div>
     <div class="doc-editor" style="background:var(--sf2);border:1px solid var(--bd);border-radius:8px;padding:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
       <span style="font-size:14px;font-weight:700;color:var(--tx)">✏️ Edit Line Items</span>
       <div style="display:flex;gap:8px">
        <button onclick="saveDocument(this)" class="btn btn-sm" style="background:#238636;color:#fff;font-size:13px;padding:6px 16px;border-radius:6px;border:none;cursor:pointer;font-weight:600">💾 Save & Regenerate</button>
        <a href="/api/pricecheck/document/{doc_id}/pdf" download class="btn btn-sm" style="background:#21262d;color:#58a6ff;font-size:14px;padding:6px 12px;border-radius:6px;border:1px solid #30363d;text-decoration:none">📥 Download</a>
       </div>
      </div>
      <div id="docMsg" style="display:none;padding:8px 12px;border-radius:6px;font-size:14px;margin-bottom:10px"></div>
      <textarea id="ed_notes" placeholder="Revision notes (optional)" style="width:100%;background:var(--sf);border:1px solid var(--bd);border-radius:4px;padding:6px;color:var(--tx);font-size:14px;resize:none;margin-bottom:10px;height:32px">{doc.get("notes","")}</textarea>
      <div style="overflow-x:auto">
       <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead><tr style="border-bottom:1px solid var(--bd);font-size:13px;color:var(--tx2);text-transform:uppercase">
         <th style="padding:8px;width:30px">#</th><th style="padding:8px;width:60px">Qty</th><th style="padding:8px;width:60px">UOM</th>
         <th style="padding:8px">Description</th><th style="padding:8px;width:120px">MFG#</th>
         <th style="padding:8px;width:90px;text-align:right">Price</th><th style="padding:8px;width:90px;text-align:right">Extension</th>
        </tr></thead>
        <tbody>{item_rows}</tbody>
        <tfoot>
         <tr style="border-top:2px solid var(--bd)">
          <td colspan="6" style="text-align:right;padding:10px;font-weight:700;font-size:14px">Subtotal:</td>
          <td style="text-align:right;padding:10px;font-weight:700;font-size:14px;font-family:monospace" id="docSubtotal">—</td>
         </tr>
        </tfoot>
       </table>
      </div>
     </div>
    </div>
    <script>
    var ITEM_COUNT={len(items)};
    function recalcDoc(){{
     var sub=0;
     for(var i=0;i<ITEM_COUNT;i++){{
      var q=parseInt(document.querySelector('[name=ed_qty_'+i+']').value)||1;
      var p=parseFloat(document.querySelector('[name=ed_price_'+i+']').value)||0;
      var ext=Math.round(q*p*100)/100;
      sub+=ext;
      var cells=document.querySelectorAll('.doc-ext');
      if(cells[i]) cells[i].textContent='$'+ext.toFixed(2);
     }}
     document.getElementById('docSubtotal').textContent='$'+sub.toFixed(2);
    }}
    recalcDoc();
    function saveDocument(btn){{
     btn.disabled=true;btn.textContent='⏳ Saving...';
     var items=[];
     for(var i=0;i<ITEM_COUNT;i++){{
      items.push({{
       qty:parseInt(document.querySelector('[name=ed_qty_'+i+']').value)||1,
       uom:document.querySelector('[name=ed_uom_'+i+']').value||'EA',
       description:document.querySelector('[name=ed_desc_'+i+']').value||'',
       mfg_number:document.querySelector('[name=ed_mfg_'+i+']').value||'',
       unit_price:parseFloat(document.querySelector('[name=ed_price_'+i+']').value)||0,
      }});
     }}
     var notes=document.getElementById('ed_notes').value;
     fetch('/pricecheck/{pcid}/document/save',{{
      method:'POST',headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{items:items,notes:notes,from_doc_id:{doc_id}}})
     }}).then(r=>r.json()).then(d=>{{
      btn.disabled=false;
      if(d.ok){{
       var msg=document.getElementById('docMsg');
       msg.style.display='block';msg.style.background='rgba(52,211,153,.1)';
       msg.style.border='1px solid rgba(52,211,153,.3)';msg.style.color='#3fb950';
       msg.textContent='✅ Saved as v'+d.version+'. Reloading...';
       setTimeout(()=>location.href='/pricecheck/{pcid}/document/'+d.doc_id,1500);
      }}else{{
       btn.textContent='💾 Save & Regenerate';
       var msg=document.getElementById('docMsg');
       msg.style.display='block';msg.style.background='rgba(248,81,73,.1)';
       msg.style.border='1px solid rgba(248,81,73,.3)';msg.style.color='#f85149';
       msg.textContent='❌ '+(d.error||'Save failed');
      }}
     }}).catch(e=>{{btn.disabled=false;btn.textContent='💾 Save & Regenerate';alert('Error: '+e.message)}});
    }}
    </script>'''
    
    from src.api.render import render_page
    return render_page("generic.html", active_page="PCs", 
                      page_title=f"Document Editor — PC #{pc.get('pc_number','?')}",
                      content=content)


@bp.route("/pricecheck/<pcid>/document/save", methods=["POST"])
@auth_required
def pricecheck_document_save(pcid):
    """Save edits from document editor → re-generates PDF → creates new version."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    
    data = request.get_json(force=True, silent=True) or {}
    edited_items = data.get("items", [])
    notes = data.get("notes", "")
    
    if not edited_items:
        return jsonify({"ok": False, "error": "No items provided"})
    
    # Merge edits into PC items (preserve pricing/catalog data, update user-editable fields)
    items = pc.get("items", [])
    for i, edit in enumerate(edited_items):
        if i < len(items):
            items[i]["qty"] = edit.get("qty", items[i].get("qty", 1))
            items[i]["uom"] = edit.get("uom", items[i].get("uom", "EA"))
            items[i]["description"] = edit.get("description", items[i].get("description", ""))
            items[i]["mfg_number"] = edit.get("mfg_number", items[i].get("mfg_number", ""))
            items[i]["unit_price"] = edit.get("unit_price", 0)
            if not items[i].get("pricing"):
                items[i]["pricing"] = {}
            items[i]["pricing"]["recommended_price"] = edit.get("unit_price", 0)
    
    # Sync to parsed
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": items}
    else:
        pc["parsed"]["line_items"] = items
    _save_single_pc(pcid, pc)
    
    # Re-generate the PDF
    from src.forms.price_check import fill_ams704
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        return jsonify({"ok": False, "error": "Source PDF not found"})
    
    pc_num = pc.get("pc_number", "") or ""
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip()) if pc_num.strip() else ""
    safe_name = f"{safe_name}_{pcid}" if safe_name else pcid

    # Get next version number
    from src.core.db import get_sent_documents, create_sent_document
    existing = get_sent_documents(pcid)
    ver = len(existing) + 1
    
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned_name = f"PC_{safe_name}_v{ver}_sent_{date_str}.pdf"
    output_path = os.path.join(DATA_DIR, versioned_name)
    
    _regen_tax = 0.0
    if pc.get("tax_enabled", False):
        _rsr = pc.get("tax_rate", 0)
        if _rsr and float(_rsr) > 0:
            _rrv = float(_rsr)
            _regen_tax = _rrv / 100.0 if _rrv > 1.0 else _rrv

    result = fill_ams704(
        source_pdf=source_pdf,
        parsed_pc=pc.get("parsed", {}),
        output_pdf=output_path,
        tax_rate=_regen_tax,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
    )
    
    if not result.get("ok"):
        return jsonify({"ok": False, "error": result.get("error", "PDF generation failed")})
    
    # Update the main output_pdf to this latest version
    pc["output_pdf"] = output_path
    _save_single_pc(pcid, pc)
    
    # Create document version record
    doc_id = create_sent_document(
        pc_id=pcid, filepath=output_path,
        items=items,
        header=pc.get("parsed", {}).get("header", {}),
        notes=notes or "Edited from document viewer",
        created_by="user"
    )
    
    log.info("DOCUMENT SAVE pc=%s v%d doc_id=%d: %d items, file=%s",
             pcid, ver, doc_id, len(items), versioned_name)
    
    return jsonify({"ok": True, "doc_id": doc_id, "version": ver, "filename": versioned_name})


@bp.route("/api/pricecheck/<pcid>/mark-auto-priced", methods=["POST"])
@auth_required
def api_pc_mark_auto_priced(pcid):
    """Mark a PC as auto-priced so the on-load auto-pricing doesn't re-run."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    pc["auto_priced"] = True
    _save_single_pc(pcid, pc)
    return jsonify({"ok": True})


# ── Auto-Enrich Pipeline ─────────────────────────────────────────────────────

def _auto_enrich_pc(pc_id: str):
    """Auto-enrich a freshly-created PC with catalog, SCPRS, and URL data.
    Called immediately after PC creation (upload, split, email, manual).
    Never overwrites user-entered prices — only populates item['pricing'] dict.
    Runs synchronously (<5s for typical 10-item PC)."""
    try:
        from src.api.dashboard import _load_price_checks, _save_single_pc
        pcs = _load_price_checks()
        pc = pcs.get(pc_id)
        if not pc:
            log.warning("AUTO-ENRICH %s: PC not found", pc_id)
            return
        items = pc.get("items", [])
        if not items:
            items = pc.get("parsed", {}).get("line_items", [])
        if not items:
            log.info("AUTO-ENRICH %s: no items to enrich", pc_id)
            return

        # Ensure each item has a pricing dict
        for it in items:
            if "pricing" not in it:
                it["pricing"] = {}

        catalog_matched = 0
        scprs_matched = 0
        urls_extracted = 0
        institution = pc.get("institution", "")

        # ── Step 1: Extract URLs from descriptions ──
        try:
            urls_extracted = _extract_urls_from_items(items)
        except Exception as e:
            log.debug("AUTO-ENRICH %s: URL extraction error: %s", pc_id, e)

        # ── Step 2: Catalog batch match ──
        try:
            from src.agents.product_catalog import match_items_batch
            batch_input = [{"idx": i, "description": it.get("description", ""),
                            "part_number": it.get("mfg_number", "") or it.get("part_number", "")}
                           for i, it in enumerate(items)]
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
                if r.get("recommended_price"):
                    it["pricing"]["catalog_recommended"] = r["recommended_price"]
                if not it.get("mfg_number") and r.get("mfg_number"):
                    it["mfg_number"] = r["mfg_number"]
                if not it.get("item_link") and r.get("supplier_url"):
                    it["item_link"] = r["supplier_url"]
                    it["item_supplier"] = r.get("supplier_name", "")
                catalog_matched += 1
        except Exception as e:
            log.warning("AUTO-ENRICH %s: catalog match error: %s", pc_id, e)

        # ── Step 3: SCPRS lookup ──
        try:
            from src.knowledge.won_quotes_db import find_similar_items
            for i, it in enumerate(items):
                if it["pricing"].get("scprs_price"):
                    scprs_matched += 1
                    continue  # already has SCPRS data
                desc = it.get("description", "")
                pn = it.get("mfg_number", "") or it.get("part_number", "")
                matches = find_similar_items(pn, desc, max_results=1, min_confidence=0.30)
                if matches:
                    best = matches[0]
                    q = best.get("quote", {})
                    it["pricing"]["scprs_price"] = q.get("unit_price", 0)
                    it["pricing"]["scprs_match"] = (q.get("description", "") or "")[:60]
                    it["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    it["pricing"]["scprs_source"] = "scprs_kb"
                    it["pricing"]["scprs_po"] = q.get("po_number", "")
                    scprs_matched += 1
        except Exception as e:
            log.warning("AUTO-ENRICH %s: SCPRS lookup error: %s", pc_id, e)

        # ── Step 4: Pricing recommendations ──
        try:
            from src.knowledge.pricing_oracle import recommend_price
            for it in items:
                if it.get("unit_price") or it["pricing"].get("recommended_price"):
                    continue  # already priced
                desc = it.get("description", "")
                pn = it.get("mfg_number", "") or it.get("part_number", "")
                cost = it["pricing"].get("catalog_cost") or it["pricing"].get("unit_cost") or 0
                scprs = it["pricing"].get("scprs_price") or 0
                rec = recommend_price(pn, desc, supplier_cost=cost if cost > 0 else None,
                                      scprs_price=scprs if scprs > 0 else None,
                                      agency=institution)
                if rec:
                    if rec.get("recommended_price"):
                        it["pricing"]["recommended_price"] = rec["recommended_price"]
                    if rec.get("aggressive_price"):
                        it["pricing"]["aggressive_price"] = rec["aggressive_price"]
                    if rec.get("safe_price"):
                        it["pricing"]["safe_price"] = rec["safe_price"]
                    if rec.get("data_quality"):
                        it["pricing"]["data_quality"] = rec["data_quality"]
        except Exception as e:
            log.warning("AUTO-ENRICH %s: pricing oracle error: %s", pc_id, e)

        # ── Save enriched PC ──
        pc["items"] = items
        if "parsed" in pc and pc["parsed"].get("line_items"):
            pc["parsed"]["line_items"] = items
        pc["enrichment_status"] = "complete"
        pc["enrichment_at"] = datetime.now().isoformat()
        pc["enrichment_summary"] = {
            "catalog_matched": catalog_matched,
            "scprs_matched": scprs_matched,
            "urls_extracted": urls_extracted,
            "total_items": len(items),
        }
        _save_single_pc(pc_id, pc)

        log.info("AUTO-ENRICH %s: %d/%d catalog, %d/%d SCPRS, %d URLs extracted",
                 pc_id, catalog_matched, len(items), scprs_matched, len(items), urls_extracted)

    except Exception as e:
        log.error("AUTO-ENRICH %s FAILED: %s", pc_id, e, exc_info=True)


def _extract_urls_from_items(items: list) -> int:
    """Extract supplier URLs embedded in item descriptions.
    Many DocuSign 704s have URLs like 'Toothpaste https://www.dollartree.com/...'
    Sets item['item_link'] and item['item_supplier']. Returns count extracted."""
    import re as _re
    extracted = 0
    try:
        from src.agents.item_link_lookup import SUPPLIER_MAP
    except ImportError:
        SUPPLIER_MAP = {}

    _url_re = _re.compile(r'(https?://[^\s"\'<>)\]]+)')

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


@bp.route("/api/pricecheck/<pcid>/retry-auto-price", methods=["POST", "GET"])
@auth_required
def api_pc_retry_auto_price(pcid):
    """Manually retry auto-pricing — reads PC from DB or JSON directly, runs inline."""
    import sqlite3
    from src.core.paths import DATA_DIR as _DATA_DIR
    pc = None
    source = "none"

    # Ensure table exists
    try:
        from src.core.db import init_db
        init_db()
    except Exception:
        pass

    # Try 1: DB with pc_data blob
    try:
        from src.core.dal import get_pc as _dal_get_pc
        _db_pc = _dal_get_pc(pcid)
        if _db_pc:
            pc = _db_pc
            source = "dal"
    except Exception as e:
        log.warning("retry-auto-price DAL read: %s", e)

    # Try 2: JSON file
    if not pc or not pc.get("items"):
        try:
            json_path = os.path.join(_DATA_DIR, "price_checks.json")
            if os.path.exists(json_path):
                with open(json_path) as f:
                    jdata = json.load(f)
                if pcid in jdata:
                    pc = jdata[pcid]
                    source = "json"
        except Exception as e:
            log.warning("retry-auto-price JSON read: %s", e)

    # Try 3: _load_price_checks as last resort
    if not pc or not pc.get("items"):
        try:
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            if pcid in pcs:
                pc = pcs[pcid]
                source = "load_func"
        except Exception:
            pass

    if not pc:
        return jsonify({"ok": False, "error": "PC not found in DB, JSON, or load function", "pc_id": pcid})

    items = pc.get("items", [])
    if not items:
        # Try to reparse from source PDF before giving up
        log.info("retry-auto-price: PC %s has 0 items, attempting reparse", pcid)
        try:
            source_pdf = pc.get("source_pdf", "")
            if source_pdf and os.path.exists(source_pdf):
                from src.forms.price_check import parse_ams704
                fresh = parse_ams704(source_pdf)
                if fresh.get("line_items"):
                    items = fresh["line_items"]
                    pc["items"] = items
                    pc["parsed"] = fresh
                    if fresh.get("header"):
                        for hk, hv in fresh["header"].items():
                            if hv and not pc.get(hk):
                                pc[hk] = hv
                    log.info("retry-auto-price: reparse got %d items from %s", len(items), source_pdf)
                    pc["_reparsed"] = True
            
            # Also try DB-stored PDF
            if not items:
                try:
                    from src.core.db import get_db
                    with get_db() as conn:
                        row = conn.execute(
                            "SELECT data, filename FROM rfq_files WHERE rfq_id=? AND category='source' ORDER BY id DESC LIMIT 1",
                            (pcid,)
                        ).fetchone()
                        if row and row["data"]:
                            import tempfile
                            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
                                tf.write(row["data"])
                                tf_path = tf.name
                            from src.forms.price_check import parse_ams704
                            fresh = parse_ams704(tf_path)
                            if fresh.get("line_items"):
                                items = fresh["line_items"]
                                pc["items"] = items
                                pc["parsed"] = fresh
                                if fresh.get("header"):
                                    for hk, hv in fresh["header"].items():
                                        if hv and not pc.get(hk):
                                            pc[hk] = hv
                                log.info("retry-auto-price: DB reparse got %d items", len(items))
                                pc["_reparsed"] = True
                            os.unlink(tf_path)
                except Exception as _dbe:
                    log.debug("retry-auto-price DB reparse: %s", _dbe)
        except Exception as _rpe:
            log.warning("retry-auto-price reparse failed: %s", _rpe)
        
        if not items:
            return jsonify({"ok": False, "error": "PC found but has 0 items — reparse also failed. Upload the PDF manually on the PC detail page.", "source": source})

    # Run auto-pricing INLINE (not in thread) so we see results immediately
    found = 0
    errors = []

    # Catalog match
    try:
        from src.agents.product_catalog import match_item, init_catalog_db
        init_catalog_db()
        for item in items:
            desc = item.get("description", "")
            pn = str(item.get("item_number", "") or item.get("mfg_number", "") or "")
            if not desc and not pn: continue
            matches = match_item(desc, pn, top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.50:
                best = matches[0]
                if not item.get("pricing"): item["pricing"] = {}
                cat_price = best.get("recommended_price") or best.get("sell_price", 0)
                cat_cost = best.get("cost", 0)
                if cat_price > 0:
                    item["pricing"]["catalog_match"] = best.get("name", "")[:60]
                    item["pricing"]["catalog_confidence"] = best.get("match_confidence", 0)
                    item["pricing"]["recommended_price"] = round(cat_price, 2)
                    if cat_cost > 0: item["pricing"]["unit_cost"] = cat_cost
                    found += 1
    except Exception as e:
        errors.append(f"catalog: {e}")

    # SCPRS
    try:
        from src.knowledge.pricing_oracle import find_similar_items
        for item in items:
            if item.get("pricing", {}).get("recommended_price"): continue
            desc = item.get("description", "")
            pn = str(item.get("item_number", "") or item.get("mfg_number", "") or "")
            matches = find_similar_items(item_number=pn, description=desc)
            if matches:
                best = matches[0]
                quote = best.get("quote", best)
                price = quote.get("unit_price", 0)
                if price and price > 0:
                    if not item.get("pricing"): item["pricing"] = {}
                    item["pricing"]["scprs_price"] = price
                    item["pricing"]["scprs_match"] = quote.get("description", "")[:60]
                    item["pricing"]["recommended_price"] = round(price * 1.25, 2)
                    item["pricing"]["unit_cost"] = price
                    found += 1
    except Exception as e:
        errors.append(f"scprs: {e}")

    # Save results
    save_ok = False
    reparsed = pc.get("_reparsed", False)  # Set during reparse above
    if found > 0 or reparsed:
        pc["items"] = items
        if found > 0:
            pc["auto_priced"] = True
            pc["auto_priced_count"] = found
        
        # Use _save_single_pc for proper dual-write (DB + JSON) + cache invalidation
        try:
            pcs = _load_price_checks()
            pcs[pcid] = pc
            _save_single_pc(pcid, pc)
            save_ok = True
        except Exception as e:
            errors.append(f"save: {e}")
        
        # Write to catalog so pricing intelligence persists
        try:
            _enrich_catalog_from_pc(pc)
        except Exception as e:
            errors.append(f"catalog: {e}")
        
        # Also save via standard pipeline (SQLite + JSON + cache invalidation)
        try:
            _save_single_pc(pcid, pc)
        except Exception as e:
            errors.append(f"save: {e}")

    return jsonify({
        "ok": True,
        "source": source,
        "items": len(items),
        "priced": found,
        "saved": save_ok,
        "errors": errors,
        "message": f"Found prices for {found}/{len(items)} items" + (f" (errors: {errors})" if errors else ""),
    })


@bp.route("/api/pricecheck/<pcid>/auto-price-status")
@auth_required
def api_pc_auto_price_status(pcid):
    """Check auto-price debug status for a PC."""
    import os, json
    from src.core.paths import DATA_DIR as _DATA_DIR
    status_file = os.path.join(_DATA_DIR, "auto_price_status.json")
    if os.path.exists(status_file):
        with open(status_file) as f:
            data = json.load(f)
        if pcid in data:
            return jsonify({"ok": True, "status": data[pcid]})
    return jsonify({"ok": True, "status": None, "message": "No auto-price record found — may not have run yet"})


@bp.route("/api/pricecheck/<pcid>/mark-won", methods=["POST"])
@auth_required
def api_pricecheck_mark_won(pcid):
    """Manually mark PC as won — records to DB, catalog, CRM."""
    pcs = _load_price_checks()
    if pcid not in pcs: return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    pc = pcs[pcid]
    _transition_status(pc, "sent", actor="user", notes=data.get("notes", "Won"))
    pc.update({"award_status": "won",
        "closed_at": datetime.now().isoformat(), "closed_reason": data.get("notes", "Won")})
    _save_single_pc(pcid, pc)
    try:
        upsert_price_check(pcid, pc)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    _log_crm_activity(pc.get("reytech_quote_number", pcid), "quote_won",
        f"WON: PC #{pc.get('pc_number','')} — {pc.get('institution','')}", actor="user")
    # ── Feed win data back to product catalog ──
    try:
        from src.agents.product_catalog import record_outcome_to_catalog, init_catalog_db
        init_catalog_db()
        result = record_outcome_to_catalog(pc, outcome="won")
        log.info("mark-won catalog feedback: %s", result)
    except Exception as e:
        log.debug("mark-won catalog feedback error: %s", e)
    _enrich_catalog_from_pc(pc)
    # ── Feed won items to FI$Cal catalog for future intelligence ──
    try:
        from src.agents.quote_intelligence import learn_new_item
        for item in pc.get("items", []):
            desc = item.get("description", "")
            price = item.get("pricing", {}).get("recommended_price") or item.get("pricing", {}).get("unit_cost")
            if desc and price and float(price) > 0:
                learn_new_item(
                    description=desc, unit_price=float(price),
                    quantity=item.get("qty", 1),
                    uom=item.get("uom", ""),
                    supplier="REYTECH INC",
                    department=pc.get("institution", ""),
                    po_number=pc.get("pc_number", pcid),
                    date=datetime.now().strftime("%m/%d/%Y") if "datetime" in dir() else "",
                )
    except Exception as e:
        log.debug("FI$Cal catalog learning on win: %s", e)
    log.info("PC %s marked WON: pc#=%s institution=%s", pcid, pc.get("pc_number"), pc.get("institution"))
    return jsonify({"ok": True, "status": "won",
                    "message": "Pricing accepted. When official RFQ/PO arrives, create the order to generate supplier POs."})



@bp.route("/api/pricecheck/<pcid>/mark-lost", methods=["POST"])
@auth_required
def api_pricecheck_mark_lost(pcid):
    """Mark PC as lost with competitor details — records to DB, competitor tracking."""
    pcs = _load_price_checks()
    if pcid not in pcs: return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    pc = pcs[pcid]
    comp_name = data.get("competitor_name", "Unknown")
    _transition_status(pc, "not_responding", actor="user", 
                      notes=f"Lost to {comp_name}")
    pc.update({"award_status": "lost",
        "competitor_name": comp_name,
        "competitor_price": data.get("competitor_price", 0),
        "competitor_po": data.get("po_number", ""),
        "closed_at": datetime.now().isoformat(),
        "closed_reason": f"Lost to {comp_name}"})
    _save_single_pc(pcid, pc)
    try:
        upsert_price_check(pcid, pc)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    try:
        from src.agents.award_monitor import log_competitor
        our_total = sum((it.get("pricing", {}).get("recommended_price", 0) or 0) * it.get("qty", 1)
                       for it in pc.get("items", []))
        log_competitor(pc, {"supplier": pc["competitor_name"], "total": pc["competitor_price"],
            "po_number": pc.get("competitor_po", "")}, our_total)
    except Exception: pass
    _log_crm_activity(pc.get("reytech_quote_number", pcid), "quote_lost",
        f"LOST: PC #{pc.get('pc_number','')} to {pc['competitor_name']}", actor="user")
    # ── Feed loss data back to product catalog ──
    try:
        from src.agents.product_catalog import record_outcome_to_catalog, init_catalog_db
        init_catalog_db()
        result = record_outcome_to_catalog(
            pc, outcome="lost",
            competitor_name=pc.get("competitor_name", "Unknown"),
            competitor_price=float(pc.get("competitor_price", 0) or 0)
        )
        log.info("mark-lost catalog feedback: %s", result)
    except Exception as e:
        log.debug("mark-lost catalog feedback error: %s", e)
    return jsonify({"ok": True, "status": "lost"})


@bp.route("/api/award-monitor/run", methods=["GET", "POST"])
@auth_required
def api_award_monitor_run():
    """Manually trigger award check cycle."""
    try:
        from src.agents.award_monitor import run_award_check
        return jsonify({"ok": True, **run_award_check()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/award-monitor/status")
@auth_required
def api_award_monitor_status():
    try:
        from src.agents.award_monitor import get_monitor_status
        return jsonify({"ok": True, **get_monitor_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/competitors")
@auth_required
def api_competitors():
    try:
        from src.agents.award_monitor import get_competitor_dashboard
        return jsonify({"ok": True, **get_competitor_dashboard()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/suggestions")
@auth_required
def api_pricecheck_suggestions(pcid):
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc: return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.award_monitor import get_price_suggestions
        suggestions = get_price_suggestions(pc.get("items", []), pc.get("institution", ""))
        return jsonify({"ok": True, "suggestions": suggestions, "count": len(suggestions)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/auto-price", methods=["POST"])
@auth_required
def api_pricecheck_auto_price(pcid):
    """Smart per-item pricing using catalog history, SCPRS, competitor data."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.product_catalog import bulk_smart_price, init_catalog_db
        init_catalog_db()
        items = []
        for i, it in enumerate(pc.get("items", [])):
            items.append({
                "idx": i,
                "description": it.get("description", ""),
                "item_number": str(it.get("item_number", "")),
                "cost": it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0,
                "qty": it.get("qty", 1),
            })
        results = bulk_smart_price(items, agency=pc.get("institution", ""))
        matched = sum(1 for r in results if r.get("matched"))
        priced = sum(1 for r in results if r.get("recommended"))
        return jsonify({
            "ok": True, "results": results,
            "matched": matched, "priced": priced, "total": len(items)
        })
    except Exception as e:
        log.exception("auto-price error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/price-sweep", methods=["POST"])
@auth_required
def api_pricecheck_price_sweep(pcid):
    """Multi-supplier price sweep using Google Shopping via SerpApi."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.product_catalog import (
            match_item, add_supplier_price, init_catalog_db
        )
        from src.agents.product_research import _get_api_key, SERPAPI_BASE
        import requests as _req

        init_catalog_db()
        api_key = _get_api_key()
        if not api_key:
            return jsonify({"ok": False, "error": "SERPAPI_KEY not configured"})

        items = pc.get("items", [])
        results = []
        found_count = 0

        for i, it in enumerate(items):
            desc = (it.get("description") or "").strip()
            pn = str(it.get("item_number") or "").strip()
            if not desc and not pn:
                results.append({"idx": i, "found": False})
                continue

            query = pn if pn and len(pn) > 3 else ""
            if desc:
                words = [w for w in desc.split() if len(w) > 2][:6]
                if query:
                    query += " " + " ".join(words[:3])
                else:
                    query = " ".join(words)

            if not query:
                results.append({"idx": i, "found": False})
                continue

            try:
                resp = _req.get(SERPAPI_BASE, params={
                    "engine": "google_shopping",
                    "q": query,
                    "api_key": api_key,
                    "num": 5,
                }, timeout=15)
                data = resp.json()
                shopping = data.get("shopping_results", [])[:5]

                if not shopping:
                    results.append({"idx": i, "found": False, "query": query})
                    continue

                options = []
                for sr in shopping:
                    price_str = sr.get("extracted_price") or sr.get("price", "")
                    price_val = 0
                    if isinstance(price_str, (int, float)):
                        price_val = float(price_str)
                    elif isinstance(price_str, str):
                        import re as _re
                        m = _re.search(r"[\d,]+\.?\d*", price_str.replace(",", ""))
                        if m:
                            price_val = float(m.group())

                    options.append({
                        "title": (sr.get("title") or "")[:80],
                        "price": round(price_val, 2),
                        "source": sr.get("source", ""),
                        "link": sr.get("link", ""),
                        "thumbnail": sr.get("thumbnail", ""),
                        "shipping": sr.get("delivery", ""),
                    })

                options = sorted([o for o in options if o["price"] > 0], key=lambda x: x["price"])
                best = options[0] if options else None

                if best and best["price"] > 0:
                    cat_matches = match_item(desc, pn, top_n=1)
                    if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.55:
                        pid = cat_matches[0]["id"]
                        add_supplier_price(
                            pid, best["source"], best["price"],
                            url=best.get("link", "")
                        )

                results.append({
                    "idx": i, "found": True,
                    "query": query,
                    "best_price": best["price"] if best else 0,
                    "best_source": best["source"] if best else "",
                    "options": options[:5],
                })
                found_count += 1

            except Exception as se:
                log.debug("sweep item %d error: %s", i, se)
                results.append({"idx": i, "found": False, "error": str(se)})

        return jsonify({
            "ok": True, "results": results,
            "found": found_count, "total": len(items)
        })
    except ImportError as e:
        # Fallback to Claude web search when SerpApi deps missing
        try:
            from src.agents.web_price_research import web_search_for_pc
            result = web_search_for_pc(pcid)
            if result.get("ok"):
                result["source"] = "claude_web_fallback"
                return jsonify(result)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        return jsonify({"ok": False, "error": f"Missing dependency: {e}. Set ANTHROPIC_API_KEY for Claude web search fallback."})
    except Exception as e:
        log.exception("price-sweep error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/web-search", methods=["POST"])
@auth_required
def api_pricecheck_web_search(pcid):
    """Claude-powered web price search — uses Anthropic API + web_search tool.
    Runs in background thread to avoid gunicorn timeout.
    Poll /api/pricecheck/<pcid>/web-search/status for progress."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    
    # Check if already running
    status_key = f"web_search_{pcid}"
    from src.api.dashboard import POLL_STATUS
    if POLL_STATUS.get(status_key, {}).get("running"):
        return jsonify({"ok": True, "status": "already_running", 
                        "message": "Web search already in progress"})
    
    # Start background thread
    POLL_STATUS[status_key] = {"running": True, "started": datetime.now().timestamp()}
    
    def _run():
        try:
            from src.agents.web_price_research import web_search_for_pc
            result = web_search_for_pc(pcid)
            POLL_STATUS[status_key] = {"running": False, "result": result, "done": True}
        except Exception as e:
            log.exception("web-search background error")
            POLL_STATUS[status_key] = {"running": False, "result": {"ok": False, "error": str(e)}, "done": True}
    
    import threading
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "status": "started", "message": "Web search started in background"})


@bp.route("/api/pricecheck/<pcid>/web-search/status")
@auth_required
def api_pricecheck_web_search_status(pcid):
    """Poll web search progress."""
    from src.api.dashboard import POLL_STATUS
    status_key = f"web_search_{pcid}"
    status = POLL_STATUS.get(status_key, {})
    if status.get("done"):
        result = status.get("result", {})
        # Clean up
        POLL_STATUS.pop(status_key, None)
        return jsonify(result)
    elif status.get("running"):
        elapsed = int(datetime.now().timestamp() - status.get("started", datetime.now().timestamp()))
        return jsonify({"ok": True, "status": "running", "elapsed": elapsed})
    else:
        return jsonify({"ok": True, "status": "idle"})


@bp.route("/api/pricecheck/<pcid>/portfolio-price", methods=["POST"])
@auth_required
def api_pricecheck_portfolio_price(pcid):
    """Portfolio pricing — optimizes entire quote as a portfolio."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.product_catalog import optimize_portfolio, init_catalog_db
        init_catalog_db()
        items = []
        for i, it in enumerate(pc.get("items", [])):
            cost = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
            # Also try reading from form input if cost was recently entered
            items.append({
                "idx": i,
                "description": it.get("description", ""),
                "item_number": str(it.get("item_number", "")),
                "cost": cost,
                "qty": it.get("qty", 1),
            })
        result = optimize_portfolio(items, agency=pc.get("institution", ""))
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.exception("portfolio-price error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/competitor-intel")
@auth_required
def api_pricecheck_competitor_intel(pcid):
    """Get competitor intelligence relevant to this PC's items."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.award_monitor import get_price_suggestions
        suggestions = get_price_suggestions(pc.get("items", []), pc.get("institution", ""))

        # Also get catalog competitor data for each item
        from src.agents.product_catalog import match_item, init_catalog_db
        init_catalog_db()
        catalog_intel = []
        for i, it in enumerate(pc.get("items", [])):
            desc = (it.get("description") or "").strip()
            pn = str(it.get("item_number") or "").strip()
            if not desc and not pn:
                continue
            matches = match_item(desc, pn, top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.50:
                m = matches[0]
                if m.get("competitor_low_price") or m.get("scprs_last_price"):
                    catalog_intel.append({
                        "idx": i,
                        "description": desc[:60],
                        "scprs_price": m.get("scprs_last_price"),
                        "scprs_agency": m.get("scprs_agency", ""),
                        "scprs_po": m.get("scprs_po", m.get("last_po_number", "")),
                        "scprs_source": m.get("scprs_source", ""),
                        "competitor_price": m.get("competitor_low_price"),
                        "competitor_source": m.get("competitor_source", ""),
                        "web_lowest": m.get("web_lowest_price"),
                        "win_rate": m.get("win_rate", 0),
                        "times_won": m.get("times_won", 0),
                        "times_lost": m.get("times_lost", 0),
                    })

        return jsonify({
            "ok": True,
            "suggestions": suggestions,
            "catalog_intel": catalog_intel,
            "suggestion_count": len(suggestions),
            "intel_count": len(catalog_intel),
        })
    except Exception as e:
        log.exception("competitor-intel error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/freshness")
@auth_required
def api_catalog_freshness():
    """Get catalog price freshness overview."""
    try:
        from src.agents.product_catalog import get_freshness_summary, init_catalog_db
        init_catalog_db()
        return jsonify({"ok": True, **get_freshness_summary()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/stale-products")
@auth_required
def api_catalog_stale_products():
    """Get products with stale pricing that need re-checking."""
    try:
        from src.agents.product_catalog import get_stale_products, init_catalog_db
        init_catalog_db()
        max_age = int(request.args.get("max_age", 14))
        limit = min(int(request.args.get("limit", 50)), 200)
        products = get_stale_products(max_age_days=max_age, limit=limit)
        return jsonify({"ok": True, "products": products, "count": len(products)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/save-to-catalog", methods=["POST"])
@auth_required
def api_pricecheck_save_to_catalog(pcid):
    """Save all PC line items to the product catalog.
    Called automatically on PC save + available as manual action.
    This is how the catalog grows from daily quoting work."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.product_catalog import save_pc_items_to_catalog, init_catalog_db
        init_catalog_db()
        result = save_pc_items_to_catalog(pc)
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.exception("save-to-catalog error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/add-item", methods=["POST"])
@auth_required
def api_catalog_add_item():
    """Manually add a single item to the catalog from PC detail page."""
    try:
        from src.agents.product_catalog import add_to_catalog, init_catalog_db
        init_catalog_db()
        data = request.get_json(force=True, silent=True) or {}
        pid = add_to_catalog(
            description=data.get("description", ""),
            part_number=data.get("part_number", ""),
            cost=float(data.get("cost", 0) or 0),
            sell_price=float(data.get("sell_price", 0) or 0),
            supplier_url=data.get("supplier_url", ""),
            supplier_name=data.get("supplier_name", ""),
            uom=data.get("uom", "EA"),
            manufacturer=data.get("manufacturer", ""),
            mfg_number=data.get("mfg_number", ""),
            photo_url=data.get("photo_url", ""),
            source="manual_add",
        )
        if pid:
            return jsonify({"ok": True, "product_id": pid})
        else:
            return jsonify({"ok": False, "error": "Could not add — may already exist"})
    except Exception as e:
        log.exception("add-item error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/competitors")
@auth_required
def competitors_page():
    """Competitor Intelligence Dashboard — combines award tracking + catalog margin analysis."""
    try:
        from src.agents.award_monitor import get_competitor_dashboard
        data = get_competitor_dashboard()
    except Exception:
        data = {"top_competitors": [], "by_agency": [], "recent_losses": [], "stats": {}}

    stats = data.get("stats", {})
    total_losses = stats.get("total_losses", 0) or 0
    avg_delta = stats.get("avg_delta_pct", 0) or 0
    unique_comp = stats.get("unique_competitors", 0) or 0

    comp_rows = ""
    for c in data.get("top_competitors", []):
        comp_rows += f'''<tr><td style="font-weight:600">{c.get('competitor_name','?')}</td>
          <td style="text-align:center">{c.get('losses',0)}</td>
          <td style="text-align:center;color:{'#f85149' if (c.get('avg_delta_pct') or 0) > 0 else '#3fb950'}">{c.get('avg_delta_pct',0):+.1f}%</td>
          <td style="text-align:right">${c.get('total_won',0):,.0f}</td>
          <td>{c.get('agencies','')}</td></tr>'''

    loss_rows = ""
    for l in data.get("recent_losses", []):
        loss_rows += f'''<tr><td>{(l.get('found_at') or '')[:10]}</td>
          <td>{l.get('institution','')}</td>
          <td style="font-weight:600;color:#f85149">{l.get('competitor_name','?')}</td>
          <td style="text-align:right">${l.get('competitor_price',0):,.2f}</td>
          <td style="text-align:right">${l.get('our_price',0):,.2f}</td>
          <td style="text-align:center;color:{'#f85149' if (l.get('price_delta_pct') or 0) > 0 else '#3fb950'}">{l.get('price_delta_pct',0):+.1f}%</td></tr>'''

    empty = '<tr><td colspan="6" style="text-align:center;color:var(--tx2);padding:20px">No award tracking data yet</td></tr>'

    # ── Pull catalog margin data for pricing positioning ──
    margin_risk_rows = ""
    margin_opp_rows = ""
    catalog_stats = {"total": 0, "negative": 0, "low": 0, "mid": 0, "high": 0, "avg_margin": 0}
    try:
        from src.core.db import get_db as _gdb
        import sqlite3
        with _gdb() as conn:
            conn.row_factory = sqlite3.Row
            products = [dict(r) for r in conn.execute(
                "SELECT name, sku, sell_price, cost, margin_pct, category, price_strategy "
                "FROM product_catalog WHERE sell_price > 0 AND cost > 0 ORDER BY margin_pct ASC"
            ).fetchall()]
            catalog_stats["total"] = len(products)
            for p in products:
                m = p.get("margin_pct") or 0
                if m < 0:
                    catalog_stats["negative"] += 1
                elif m < 10:
                    catalog_stats["low"] += 1
                elif m < 25:
                    catalog_stats["mid"] += 1
                else:
                    catalog_stats["high"] += 1
            if products:
                catalog_stats["avg_margin"] = sum(p.get("margin_pct", 0) for p in products) / len(products)

            # Risk items: negative or very low margin (vulnerable to competitors)
            risk_items = [p for p in products if (p.get("margin_pct") or 0) < 5][:10]
            for p in risk_items:
                m = p.get("margin_pct", 0) or 0
                clr = "#f85149" if m < 0 else "#d29922"
                margin_risk_rows += f'''<tr>
                  <td style="font-size:14px">{p.get("name","")[:50]}</td>
                  <td class="mono" style="font-size:14px">{p.get("sku","")}</td>
                  <td class="mono" style="text-align:right">${p.get("sell_price",0):,.2f}</td>
                  <td class="mono" style="text-align:right">${p.get("cost",0):,.2f}</td>
                  <td class="mono" style="text-align:center;color:{clr};font-weight:700">{m:.1f}%</td>
                  <td style="font-size:14px;color:var(--tx2)">{p.get("category","")}</td>
                </tr>'''

            # Opportunity items: high value, low margin (room to increase price)
            opp_items = sorted(
                [p for p in products if 0 < (p.get("margin_pct") or 0) < 15 and (p.get("sell_price") or 0) > 50],
                key=lambda x: (x.get("sell_price", 0) or 0) * (15 - (x.get("margin_pct") or 0)) / 100,
                reverse=True
            )[:10]
            for p in opp_items:
                m = p.get("margin_pct", 0) or 0
                target_price = (p.get("cost") or 0) / (1 - 0.15) if p.get("cost") else 0
                gain = target_price - (p.get("sell_price") or 0)
                margin_opp_rows += f'''<tr>
                  <td style="font-size:14px">{p.get("name","")[:50]}</td>
                  <td class="mono" style="text-align:right">${p.get("sell_price",0):,.2f}</td>
                  <td class="mono" style="text-align:center;color:#d29922">{m:.1f}%</td>
                  <td class="mono" style="text-align:right;color:var(--gn)">${target_price:,.2f}</td>
                  <td class="mono" style="text-align:right;color:var(--gn)">${gain:,.2f}</td>
                </tr>'''
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    has_award_data = total_losses > 0
    neg = catalog_stats["negative"]
    low = catalog_stats["low"]

    content = f'''
    <h2 style="margin-bottom:4px">🎯 Competitive Intelligence</h2>
    <p style="font-size:13px;color:var(--tx2);margin-bottom:16px">Award tracking + pricing position analysis from catalog ({catalog_stats["total"]} products)</p>

    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:100px">
        <div style="font-size:28px;font-weight:800;color:#f85149">{total_losses}</div><div style="font-size:13px;color:var(--tx2)">LOSSES TRACKED</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:100px">
        <div style="font-size:28px;font-weight:800;color:var(--tx)">{unique_comp}</div><div style="font-size:13px;color:var(--tx2)">COMPETITORS</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:100px">
        <div style="font-size:28px;font-weight:800;color:{'#f85149' if neg > 0 else '#d29922'}">{neg + low}</div><div style="font-size:13px;color:var(--tx2)">AT-RISK ITEMS</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:100px">
        <div style="font-size:28px;font-weight:800;color:{'#3fb950' if catalog_stats['avg_margin'] > 15 else '#d29922'}">{catalog_stats['avg_margin']:.1f}%</div><div style="font-size:13px;color:var(--tx2)">AVG MARGIN</div></div>
    </div>'''

    if has_award_data:
        content += f'''
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px">
        <h3 style="margin:0 0 12px;font-size:14px;color:var(--tx2)">TOP COMPETITORS</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr style="border-bottom:1px solid var(--bd);font-size:14px;color:var(--tx2)">
          <th style="text-align:left;padding:6px">Vendor</th><th style="text-align:center;padding:6px">Losses</th>
          <th style="text-align:center;padding:6px">Avg Gap</th><th style="text-align:right;padding:6px">$ Won</th>
          <th style="text-align:left;padding:6px">Agencies</th>
        </tr></thead><tbody>{comp_rows or empty}</tbody></table></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px">
        <h3 style="margin:0 0 12px;font-size:14px;color:var(--tx2)">RECENT LOSSES</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr style="border-bottom:1px solid var(--bd);font-size:14px;color:var(--tx2)">
          <th style="text-align:left;padding:6px">Date</th><th style="text-align:left;padding:6px">Institution</th>
          <th style="text-align:left;padding:6px">Winner</th><th style="text-align:right;padding:6px">Their $</th>
          <th style="text-align:right;padding:6px">Our $</th><th style="text-align:center;padding:6px">Gap</th>
        </tr></thead><tbody>{loss_rows or empty}</tbody></table></div>
    </div>'''

    # Always show pricing position from catalog
    content += f'''
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
      <div style="background:var(--sf);border:1px solid {'#f8514930' if neg > 0 else 'var(--bd)'};border-radius:8px;padding:16px">
        <h3 style="margin:0 0 4px;font-size:14px;color:#f85149">⚠️ Margin Risk — Vulnerable to Undercutting</h3>
        <p style="font-size:14px;color:var(--tx2);margin:0 0 12px">Items below 5% margin — competitors can easily beat these prices</p>
        <table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr style="border-bottom:1px solid var(--bd);font-size:13px;color:var(--tx2)">
          <th style="text-align:left;padding:5px">Product</th><th style="text-align:left;padding:5px">SKU</th>
          <th style="text-align:right;padding:5px">Sell</th><th style="text-align:right;padding:5px">Cost</th>
          <th style="text-align:center;padding:5px">Margin</th><th style="text-align:left;padding:5px">Category</th>
        </tr></thead><tbody>{margin_risk_rows or '<tr><td colspan="6" style="text-align:center;color:var(--tx2);padding:16px">No at-risk items 🎉</td></tr>'}</tbody></table>
        <div style="text-align:right;margin-top:8px"><a href="/catalog" style="font-size:14px;color:var(--ac)">View full catalog →</a></div>
      </div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px">
        <h3 style="margin:0 0 4px;font-size:14px;color:var(--gn)">💰 Repricing Opportunities</h3>
        <p style="font-size:14px;color:var(--tx2);margin:0 0 12px">High-value items below 15% margin — room to increase price</p>
        <table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr style="border-bottom:1px solid var(--bd);font-size:13px;color:var(--tx2)">
          <th style="text-align:left;padding:5px">Product</th><th style="text-align:right;padding:5px">Current</th>
          <th style="text-align:center;padding:5px">Margin</th><th style="text-align:right;padding:5px">Target (15%)</th>
          <th style="text-align:right;padding:5px">Gain/Unit</th>
        </tr></thead><tbody>{margin_opp_rows or '<tr><td colspan="5" style="text-align:center;color:var(--tx2);padding:16px">No repricing opportunities</td></tr>'}</tbody></table>
        <div style="text-align:right;margin-top:8px"><a href="/catalog" style="font-size:14px;color:var(--ac)">Pricing engine →</a></div>
      </div>
    </div>

    <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px">
      <h3 style="margin:0 0 8px;font-size:14px;color:var(--tx2)">📊 Catalog Margin Distribution</h3>
      <div style="display:flex;height:24px;border-radius:6px;overflow:hidden;background:var(--sf2)">
        {'<div style="width:' + str(round(catalog_stats["negative"]/max(catalog_stats["total"],1)*100,1)) + '%;background:#f85149;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700" title="Negative margin">' + str(catalog_stats["negative"]) + '</div>' if catalog_stats["negative"] else ''}
        <div style="width:{round(catalog_stats['low']/max(catalog_stats['total'],1)*100,1)}%;background:#d29922;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700" title="Low margin (0-10%)">{catalog_stats['low']}</div>
        <div style="width:{round(catalog_stats['mid']/max(catalog_stats['total'],1)*100,1)}%;background:#3fb950;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700" title="Mid margin (10-25%)">{catalog_stats['mid']}</div>
        <div style="width:{round(catalog_stats['high']/max(catalog_stats['total'],1)*100,1)}%;background:#58a6ff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700" title="High margin (25%+)">{catalog_stats['high']}</div>
      </div>
      <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:13px;color:var(--tx2)">
        <span><span style="color:#f85149">●</span> Negative: {catalog_stats['negative']}</span>
        <span><span style="color:#d29922">●</span> Low (&lt;10%): {catalog_stats['low']}</span>
        <span><span style="color:#3fb950">●</span> Mid (10-25%): {catalog_stats['mid']}</span>
        <span><span style="color:#58a6ff">●</span> High (25%+): {catalog_stats['high']}</span>
      </div>
    </div>'''

    from src.api.render import render_page
    return render_page("generic.html", active_page="Compete", page_title="Competitive Intelligence", content=content)


@bp.route("/api/admin/cleanup", methods=["GET", "POST"])
@auth_required
def api_admin_cleanup():
    """
    Fix Railway data issues:
    - Remove duplicate PCs (same pc_number + institution)
    - Remove test/blank PCs that have no real data
    - Reset quote counter to match actual highest quote number
    - Clean up orphaned quote references on PCs
    """
    results = {"removed_pcs": [], "kept_pcs": [], "quote_counter_before": None, "quote_counter_after": None, "errors": []}

    try:
        pcs = _load_price_checks()
        results["total_before"] = len(pcs)

        # --- Step 1: Remove clearly blank/empty PCs ---
        to_delete = []
        for pcid, pc in list(pcs.items()):
            pc_num = pc.get("pc_number", "").strip()
            institution = pc.get("institution", "").strip()
            items = pc.get("items", [])
            # Blank PC number with no institution and no items = junk
            if not pc_num and not institution and len(items) == 0:
                to_delete.append(pcid)
                results["removed_pcs"].append(f"{pcid[:8]}: blank/empty")
        for pcid in to_delete:
            pcs[pcid]["status"] = "dismissed"  # Law 22: never delete

        # --- Step 2: Deduplicate by (pc_number, institution) ---
        # Keep the most recent one (highest pcid / latest updated_at)
        seen = {}  # key -> best pcid
        for pcid, pc in pcs.items():
            key = (pc.get("pc_number", "").strip(), pc.get("institution", "").strip())
            if key not in seen:
                seen[key] = pcid
            else:
                # Keep whichever was updated more recently or has more data
                existing = pcs[seen[key]]
                existing_items = len(existing.get("items", []))
                this_items = len(pc.get("items", []))
                # Prefer one with more items, then newer by ID string sort
                if this_items > existing_items or (this_items == existing_items and pcid > seen[key]):
                    results["removed_pcs"].append(f"{seen[key][:8]}: dup of {pcid[:8]} ({key[0]})")
                    seen[key] = pcid
                else:
                    results["removed_pcs"].append(f"{pcid[:8]}: dup of {seen[key][:8]} ({key[0]})")

        # Rebuild pcs with only kept entries
        kept_ids = set(seen.values())
        for pcid in list(pcs.keys()):
            if pcid not in kept_ids:
                pcs[pcid]["status"] = "dismissed"  # Law 22: never delete

        _save_price_checks(pcs)
        results["total_after"] = len(pcs)
        results["kept_pcs"] = [f"{pid[:8]}: {pc.get('pc_number','?')}" for pid, pc in pcs.items()]

        # Also sync to SQLite
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM price_checks WHERE id NOT IN ({})".format(
                    ",".join("?" * len(pcs))
                ), list(pcs.keys()))
        except Exception as e:
            results["errors"].append(f"SQLite sync: {e}")

    except Exception as e:
        results["errors"].append(f"PC cleanup: {e}")

    # --- Step 3: Fix quote counter ---
    try:
        from src.forms.quote_generator import _load_counter, _save_counter
        from src.core.db import get_db
        import re as _re
        from datetime import datetime as _dt

        counter = _load_counter()
        results["quote_counter_before"] = counter.copy()

        # Find highest real (non-test) quote number in DB
        with get_db() as conn:
            rows = conn.execute(
                "SELECT quote_number FROM quotes WHERE is_test=0 OR is_test IS NULL ORDER BY rowid"
            ).fetchall()

        max_seq = counter.get("seq", 16)
        year = _dt.now().year % 100  # 26 for 2026
        for row in rows:
            qn = row[0] or ""
            m = _re.match(r"R\d{2}Q(\d+)", qn)
            if m:
                max_seq = max(max_seq, int(m.group(1)))

        # Also scan price_checks for reytech_quote_numbers (may have test ones like R26Q1-R26Q8)
        # Do NOT update counter based on test PCs — only real quotes count
        new_seq = max_seq  # Already at or beyond highest real quote
        counter["seq"] = new_seq
        _save_counter(counter)
        results["quote_counter_after"] = counter.copy()

    except Exception as e:
        results["errors"].append(f"Counter fix: {e}")

    results["ok"] = True
    return jsonify(results)


@bp.route("/api/admin/rescan-item-numbers", methods=["GET", "POST"])
@auth_required
def api_admin_rescan_item_numbers():
    """
    Re-scan ALL Price Checks to extract MFG/part/reference numbers.
    
    For each PC:
    1. Re-reads the source PDF if available (gets substituted column)
    2. Runs extract_item_numbers() on each line item
    3. Updates item_number and mfg_number fields
    
    POST body: { "reparse_pdfs": true } to also re-read source PDFs
    Returns: { ok, scanned, updated, details: [{pcid, pc_number, items_updated}] }
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        reparse_pdfs = data.get("reparse_pdfs", True)
    
        pcs = _load_price_checks()
        total_scanned = 0
        total_updated = 0
        details = []
    
        for pcid, pc in pcs.items():
            items = pc.get("items", [])
            if not items:
                items = pc.get("parsed", {}).get("line_items", [])
            if not items:
                continue
        
            total_scanned += 1
            items_updated = 0
        
            # Option 1: Re-parse the source PDF to get substituted column
            if reparse_pdfs:
                source_pdf = pc.get("source_pdf", "")
                if source_pdf and os.path.exists(source_pdf):
                    try:
                        from src.forms.price_check import parse_ams704
                        fresh = parse_ams704(source_pdf)
                        fresh_items = fresh.get("line_items", [])
                        # Merge substituted field + mfg_number from fresh parse
                        for fi in fresh_items:
                            row_idx = fi.get("row_index", 0)
                            # Find matching item by row_index
                            for item in items:
                                if item.get("row_index") == row_idx:
                                    # Copy substituted field if not already set
                                    if fi.get("substituted") and not item.get("substituted"):
                                        item["substituted"] = fi["substituted"]
                                    # Copy mfg_number if fresh parse found one
                                    if fi.get("mfg_number") and not item.get("mfg_number"):
                                        item["mfg_number"] = fi["mfg_number"]
                                        items_updated += 1
                                    break
                    except Exception as e:
                        log.debug("Rescan PDF %s: %s", pcid, e)
        
            # Option 2: Run extraction on existing item data
            from src.forms.price_check import extract_item_numbers, _is_sequential_number
            for item in items:
                current_mfg = (item.get("mfg_number") or "").strip()
                # Skip if already has a real MFG number
                if current_mfg:
                    continue
            
                pn = extract_item_numbers(item)
                if pn:
                    item["mfg_number"] = pn
                    items_updated += 1
        
            if items_updated > 0:
                total_updated += items_updated
                _sync_pc_items(pc, items)
                details.append({
                    "pcid": pcid,
                    "pc_number": pc.get("pc_number", ""),
                    "items_updated": items_updated,
                })
    
        if total_updated > 0:
            _save_price_checks(pcs)
    
        return jsonify({
            "ok": True,
            "scanned": total_scanned,
            "updated": total_updated,
            "details": details,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

def _is_sequential(val):
    """Helper: check if value is just a row number."""
    try:
        return 0 < int(float(str(val).strip())) <= 50
    except (ValueError, TypeError):
        return False


@bp.route("/api/admin/status")
@auth_required
def api_admin_status():
    """Quick system status — quote counter, PC count, quote count, full PC detail, RFQ queue."""
    try:
        from src.forms.quote_generator import _load_counter
        from src.core.db import get_db
        pcs = _load_price_checks()
        counter = _load_counter()
        rfqs = load_rfqs()
        with get_db() as conn:
            q_count = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 OR is_test IS NULL").fetchone()[0]
            quotes = [dict(r) for r in conn.execute(
                "SELECT quote_number, total, status FROM quotes WHERE is_test=0 ORDER BY rowid DESC LIMIT 20"
            ).fetchall()]
        # Full PC detail
        pc_detail = {}
        for pcid, pc in pcs.items():
            pc_detail[pcid] = {
                "pc_number": pc.get("pc_number", "?"),
                "institution": pc.get("institution", "?"),
                "reytech_quote_number": pc.get("reytech_quote_number", ""),
                "status": pc.get("status", "?"),
                "items_count": len(pc.get("items", [])),
                "email_subject": pc.get("email_subject", ""),
            }
        # RFQ detail
        rfq_detail = {}
        for rid, r in rfqs.items():
            rfq_detail[rid] = {
                "solicitation": r.get("solicitation", "?"),
                "requestor": r.get("requestor", "?"),
                "status": r.get("status", "?"),
                "items_count": len(r.get("items", [])),
                "email_uid": r.get("email_uid", ""),
                "email_subject": r.get("email_subject", ""),
            }
        return jsonify({
            "ok": True,
            "pc_count": len(pcs),
            "pcs": pc_detail,
            "rfq_count": len(rfqs),
            "rfqs": rfq_detail,
            "quote_count": q_count,
            "all_quotes": quotes,
            "counter": counter,
            "next_quote": f"R{str(counter.get('year',2026))[-2:]}Q{counter.get('seq',0)+1}",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/counter-set", methods=["GET", "POST"])
@auth_required
def api_admin_counter_set():
    """Force-set the quote counter. POST body: {"seq": 16}
    Next quote will be R26Q(seq+1).
    """
    data = request.get_json(force=True, silent=True) or {}
    new_seq = data.get("seq")
    if new_seq is None:
        return jsonify({"ok": False, "error": "Missing 'seq' in body"})
    try:
        from src.forms.quote_generator import set_quote_counter, _load_counter
        old = _load_counter()
        set_quote_counter(int(new_seq))
        new = _load_counter()
        log.info("ADMIN counter force-set: Q%d → Q%d (next = Q%d)",
                 old.get("seq", 0), new["seq"], new["seq"] + 1)
        return jsonify({
            "ok": True,
            "before": old,
            "after": new,
            "next_quote": f"R{str(new.get('year',2026))[-2:]}Q{new['seq']+1}",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/delete-quotes", methods=["GET", "POST"])
@auth_required
def api_admin_delete_quotes():
    """Delete quotes by number. POST body: {"quote_numbers": ["R26Q9","R26Q10"]}"""
    data = request.get_json(force=True, silent=True) or {}
    qns = data.get("quote_numbers", [])
    if not qns:
        return jsonify({"ok": False, "error": "Missing 'quote_numbers' list"})
    deleted = []
    try:
        from src.forms.quote_generator import get_all_quotes, _save_all_quotes
        from src.core.db import get_db
        all_q = get_all_quotes()
        before = len(all_q)
        all_q = [q for q in all_q if q.get("quote_number") not in qns]
        _save_all_quotes(all_q)
        try:
            with get_db() as conn:
                for qn in qns:
                    conn.execute("DELETE FROM quotes WHERE quote_number=?", (qn,))
                    deleted.append(qn)
        except Exception as e:
            log.debug("SQLite quote delete: %s", e)
        log.info("ADMIN delete-quotes: removed %s", qns)
        return jsonify({"ok": True, "deleted": qns, "quotes_before": before, "quotes_after": len(all_q)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/recall", methods=["GET", "POST"])
@auth_required
def api_admin_recall():
    """Retroactive recall: delete PCs matching a pattern + free quote numbers.
    
    POST body: {"pattern": "02.17.26"} or {"pc_ids": ["auto_xxx", ...]}
    Deletes matching PCs, removes linked draft quotes, resets counter.
    """
    data = request.get_json(force=True, silent=True) or {}
    pattern = data.get("pattern", "").strip()
    pc_ids = data.get("pc_ids", [])
    
    results = {"deleted": [], "errors": [], "before": {}, "after": {}}
    
    try:
        from src.forms.quote_generator import get_all_quotes, _save_all_quotes, _load_counter, _save_counter
        from src.core.db import get_db
        
        pcs = _load_price_checks()
        results["before"]["pc_count"] = len(pcs)
        results["before"]["counter"] = _load_counter()
        results["before"]["pc_list"] = {k: {"num": v.get("pc_number",""), "qn": v.get("reytech_quote_number","")} for k,v in pcs.items()}
        
        # Find PCs to delete
        to_delete = []
        if pc_ids:
            to_delete = [pid for pid in pc_ids if pid in pcs]
        elif pattern:
            for pcid, pc in pcs.items():
                searchable = f"{pc.get('pc_number','')} {pc.get('email_subject','')} {pc.get('source_pdf','')}".lower()
                if pattern.lower() in searchable:
                    to_delete.append(pcid)
        
        if not to_delete:
            return jsonify({"ok": False, "error": f"No PCs match pattern='{pattern}' ids={pc_ids}", "pcs": results["before"]["pc_list"]})
        
        # Delete each PC + cascade
        for pcid in to_delete:
            pc = pcs[pcid]
            pc_num = pc.get("pc_number", pcid)
            linked_qn = pc.get("reytech_quote_number", "") or pc.get("linked_quote_number", "")
            
            pcs[pcid]["status"] = "dismissed"  # Law 22: never delete
            
            # SQLite cleanup
            try:
                with get_db() as conn:
                    conn.execute("DELETE FROM price_checks WHERE id=?", (pcid,))
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
            
            # Remove linked draft quote
            quote_freed = None
            if linked_qn:
                try:
                    all_quotes = get_all_quotes()
                    before_len = len(all_quotes)
                    all_quotes = [q for q in all_quotes
                                  if not (q.get("quote_number") == linked_qn
                                          and q.get("status") in ("draft", "pending"))]
                    if len(all_quotes) < before_len:
                        _save_all_quotes(all_quotes)
                        quote_freed = linked_qn
                        try:
                            with get_db() as conn:
                                conn.execute("DELETE FROM quotes WHERE quote_number=? AND status IN ('draft','pending')", (linked_qn,))
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                except Exception as e:
                    results["errors"].append(f"Quote cleanup for {linked_qn}: {e}")
            
            results["deleted"].append({
                "pcid": pcid, "pc_number": pc_num,
                "quote_freed": quote_freed,
            })
        
        # Save updated PCs
        _save_price_checks(pcs)
        
        # Recalculate counter
        import re as _re
        all_quotes = get_all_quotes()
        max_seq = 0
        for q in all_quotes:
            qn = q.get("quote_number", "")
            m = _re.search(r'R\d{2}Q(\d+)', qn)
            if m and not q.get("is_test"):
                max_seq = max(max_seq, int(m.group(1)))
        for rpc in pcs.values():
            qn = rpc.get("reytech_quote_number", "") or ""
            m = _re.search(r'R\d{2}Q(\d+)', qn)
            if m:
                max_seq = max(max_seq, int(m.group(1)))
        
        old_counter = _load_counter()
        if max_seq < old_counter.get("seq", 0):
            _save_counter({"year": old_counter.get("year", 2026), "seq": max_seq})
        
        results["after"]["pc_count"] = len(pcs)
        results["after"]["counter"] = _load_counter()
        results["after"]["next_quote"] = f"R{str(results['after']['counter'].get('year',2026))[-2:]}Q{results['after']['counter'].get('seq',0)+1}"
        results["after"]["pc_list"] = {k: {"num": v.get("pc_number",""), "qn": v.get("reytech_quote_number","")} for k,v in pcs.items()}
        results["ok"] = True
        
        log.info("ADMIN RECALL: deleted %d PCs matching '%s', counter %s → %s",
                 len(results["deleted"]), pattern or pc_ids,
                 results["before"]["counter"], results["after"]["counter"])
        
    except Exception as e:
        results["ok"] = False
        results["errors"].append(str(e))
    
    return jsonify(results)


@bp.route("/api/admin/purge-rfqs", methods=["GET", "POST"])
@auth_required
def api_admin_purge_rfqs():
    """Delete RFQs from the queue.
    
    POST body options:
      {"rfq_ids": ["rfq_0", "rfq_1"]}  — delete specific IDs
      {"empty": true}                   — delete all RFQs with 0 items
      {"pattern": "valentina"}          — delete RFQs matching pattern in requestor/subject
      {"all": true}                     — nuclear: delete ALL RFQs
    Returns before/after counts.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        rfqs = load_rfqs()
        before_count = len(rfqs)
        before_list = {k: {"sol": v.get("solicitation","?"), "req": v.get("requestor","?"),
                           "items": len(v.get("items",[])), "status": v.get("status","?")}
                       for k, v in rfqs.items()}
    
        to_delete = set()
    
        if data.get("rfq_ids"):
            to_delete = {rid for rid in data["rfq_ids"] if rid in rfqs}
        elif data.get("empty"):
            to_delete = {rid for rid, r in rfqs.items() if len(r.get("items", [])) == 0}
        elif data.get("pattern"):
            pat = data["pattern"].lower()
            for rid, r in rfqs.items():
                searchable = f"{r.get('requestor','')} {r.get('email_subject','')} {r.get('solicitation','')}".lower()
                if pat in searchable:
                    to_delete.add(rid)
        elif data.get("all"):
            to_delete = set(rfqs.keys())
        else:
            return jsonify({"ok": False, "error": "Provide rfq_ids, empty:true, pattern, or all:true",
                            "rfqs": before_list})
    
        deleted = []
        for rid in to_delete:
            r = rfqs.pop(rid, None)
            if r:
                deleted.append({"id": rid, "sol": r.get("solicitation","?"),
                               "req": r.get("requestor","?"), "items": len(r.get("items",[]))})
    
        save_rfqs(rfqs)
    
        # Also clean SQLite
        try:
            from src.core.db import get_db
            with get_db() as conn:
                for d in deleted:
                    conn.execute("DELETE FROM rfqs WHERE id=?", (d["id"],))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
        log.info("ADMIN PURGE-RFQS: deleted %d of %d RFQs", len(deleted), before_count)
    
        return jsonify({
            "ok": True,
            "deleted": deleted,
            "deleted_count": len(deleted),
            "before": before_count,
            "after": len(rfqs),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/admin/clean-activity", methods=["GET", "POST"])
@auth_required
def api_admin_clean_activity():
    """Remove entries from crm_activity.json.
    
    POST body options:
      {"event_types": ["quote_lost"]}       — remove by event type
      {"pattern": "R26Q19"}                 — remove entries matching pattern in detail
      {"before": "2026-02-18"}              — remove entries before date
      {"all": true}                         — nuclear: clear all activity
    Returns before/after counts.
    """
    data = request.get_json(force=True, silent=True) or {}
    crm_path = os.path.join(DATA_DIR, "crm_activity.json")
    try:
        with open(crm_path) as f:
            activities = json.load(f)
    except Exception:
        activities = []
    
    before_count = len(activities)
    
    if data.get("all"):
        activities = []
    elif data.get("event_types"):
        types = set(data["event_types"])
        activities = [a for a in activities if a.get("event_type") not in types]
    elif data.get("pattern"):
        pat = data["pattern"].lower()
        activities = [a for a in activities
                      if pat not in (a.get("detail","") + " " + a.get("event_type","")).lower()]
    elif data.get("before"):
        cutoff = data["before"]
        activities = [a for a in activities if a.get("timestamp","") >= cutoff]
    else:
        return jsonify({"ok": False, "error": "Provide event_types, pattern, before, or all:true"})
    
    with open(crm_path, "w") as f:
        json.dump(activities, f, indent=2, default=str)
    
    log.info("ADMIN CLEAN-ACTIVITY: %d → %d entries", before_count, len(activities))
    
    return jsonify({
        "ok": True,
        "before": before_count,
        "after": len(activities),
        "removed": before_count - len(activities),
    })


@bp.route("/api/admin/backfill-contacts", methods=["GET", "POST"])
@auth_required
def api_admin_backfill_contacts():
    """Backfill CRM contacts from existing price checks and RFQ senders.
    Scans all PCs/RFQs for requestor emails and creates CRM contacts.
    """
    import re as _re, hashlib
    from src.core.db import upsert_contact
    
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            crm = json.load(f)
    except Exception:
        crm = {}
    
    before_count = len(crm)
    created = []
    
    agency_map = {
        "cdcr.ca.gov": "CDCR", "cdph.ca.gov": "CDPH", "dgs.ca.gov": "DGS",
        "dhcs.ca.gov": "DHCS", "cchcs.org": "CCHCS",
    }
    
    def _add_contact(email_raw, name_hint=""):
        if not email_raw:
            return
        m = _re.search(r'[\w.+-]+@[\w.-]+', str(email_raw))
        if not m:
            return
        em = m.group(0).lower().strip()
        cid = hashlib.md5(em.encode()).hexdigest()[:16]
        if cid in crm:
            return  # already exists
        
        # Derive name
        if name_hint and name_hint != em and "@" not in name_hint:
            name = name_hint
        else:
            local = em.split("@")[0]
            name = " ".join(w.capitalize() for w in _re.split(r'[._-]', local))
        
        domain = em.split("@")[-1].lower()
        agency = agency_map.get(domain, domain.split(".")[0].upper() if ".gov" in domain else "")
        
        crm[cid] = {
            "id": cid, "buyer_name": name, "buyer_email": em,
            "buyer_phone": "", "agency": agency, "title": "", "department": "",
            "linkedin": "", "notes": "Backfilled from PC/RFQ records",
            "tags": ["email_sender", "buyer"], "total_spend": 0, "po_count": 0,
            "categories": {}, "items_purchased": [], "purchase_orders": [],
            "last_purchase": "", "score": 50, "opportunity_score": 0,
            "outreach_status": "active", "activity": [],
        }
        upsert_contact({"id": cid, "buyer_name": name, "buyer_email": em,
                        "agency": agency, "source": "backfill",
                        "outreach_status": "active", "is_reytech_customer": True})
        created.append({"email": em, "name": name, "agency": agency})
    
    # Scan price checks
    pcs = _load_price_checks()
    for pc in pcs.values():
        req = pc.get("requestor", "")
        req_email = pc.get("contact_email", "") or pc.get("requestor_email", "")
        if "@" in req:
            _add_contact(req)
        elif req_email:
            _add_contact(req_email, req)
    
    # Scan RFQs
    rfqs = load_rfqs()
    for r in rfqs.values():
        _add_contact(r.get("email_sender", ""), r.get("requestor_name", ""))
        _add_contact(r.get("requestor_email", ""), r.get("requestor_name", ""))
    
    if created:
        with open(crm_path, "w") as f:
            json.dump(crm, f, indent=2, default=str)
    
    log.info("ADMIN BACKFILL-CONTACTS: created %d new contacts from PC/RFQ data", len(created))
    
    return jsonify({
        "ok": True,
        "created": created,
        "created_count": len(created),
        "before": before_count,
        "after": len(crm),
    })


@bp.route("/api/admin/import-contacts", methods=["GET", "POST"])
@auth_required
def api_admin_import_contacts():
    """Import contacts from a list.
    
    POST body: {"contacts": [{"email": "...", "name": "...", "agency": "..."}, ...]}
    Deduplicates by email. Merges with existing CRM contacts.
    """
    import re as _re, hashlib
    from src.core.db import upsert_contact
    
    data = request.get_json(force=True, silent=True) or {}
    incoming = data.get("contacts", [])
    if not incoming:
        return jsonify({"ok": False, "error": "No contacts provided"})
    
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            crm = json.load(f)
    except Exception:
        crm = {}
    
    before_count = len(crm)
    created = []
    skipped = []
    
    for c in incoming:
        em = (c.get("email") or "").lower().strip()
        if not em or "@" not in em:
            continue
        cid = hashlib.md5(em.encode()).hexdigest()[:16]
        
        if cid in crm:
            skipped.append(em)
            continue
        
        name = c.get("name", "")
        agency = c.get("agency", "")
        tags = c.get("tags", ["imported"])
        
        crm[cid] = {
            "id": cid, "buyer_name": name, "buyer_email": em,
            "buyer_phone": c.get("phone", ""), "agency": agency,
            "title": c.get("title", ""), "department": c.get("department", ""),
            "linkedin": "", "notes": c.get("notes", "Imported from Google Contacts"),
            "tags": tags, "total_spend": 0, "po_count": 0,
            "categories": {}, "items_purchased": [], "purchase_orders": [],
            "last_purchase": "", "score": 40, "opportunity_score": 0,
            "outreach_status": "new", "activity": [],
        }
        upsert_contact({"id": cid, "buyer_name": name, "buyer_email": em,
                        "agency": agency, "source": "google_import",
                        "outreach_status": "new", "is_reytech_customer": False})
        created.append({"email": em, "name": name, "agency": agency})
    
    if created:
        with open(crm_path, "w") as f:
            json.dump(crm, f, indent=2, default=str)
    
    log.info("ADMIN IMPORT-CONTACTS: %d created, %d skipped (already exist)", len(created), len(skipped))
    
    return jsonify({
        "ok": True,
        "created": created,
        "created_count": len(created),
        "skipped": skipped,
        "before": before_count,
        "after": len(crm),
    })


@bp.route("/api/pricecheck/<pcid>/clear-quote", methods=["POST"])
@auth_required
def api_pricecheck_clear_quote(pcid):
    """Clear a stale/wrong reytech_quote_number from a PC."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    old_qnum = pcs[pcid].get("reytech_quote_number", "")
    pcs[pcid]["reytech_quote_number"] = ""
    pcs[pcid]["status"] = "parsed"  # Reset to parsed so it can be re-generated
    _save_single_pc(pcid, pcs[pcid])
    try:
        from src.core.dal import update_pc_status as _dal_update_pc
        _dal_update_pc(pcid, "parsed")
    except Exception as e:
        log.debug("DAL clear-quote status: %s", e)
    log.info("CLEARED quote number %s from PC %s", old_qnum, pcid)
    return jsonify({"ok": True, "cleared": old_qnum})



@bp.route("/api/admin/rfq-cleanup", methods=["GET", "POST"])
@auth_required
def api_admin_rfq_cleanup():
    """Remove AMS 704 price check PDFs that incorrectly landed in the RFQ queue.
    These appear when the same 704 email was processed before the routing fix.
    Moves them to PC queue if not already there, then removes from rfq queue.
    """
    try:
        from src.api.dashboard import load_rfqs, save_rfqs
        import uuid as _uuid

        rfqs = load_rfqs()
        removed = []
        kept = []

        for rid, r in list(rfqs.items()):
            # Detect if this RFQ entry is actually a 704 price check:
            # 1. Attachments include a 704 form type
            atts = r.get("attachments_raw", []) or []
            templates = r.get("templates", {}) or {}
            is_704 = (
                "704" in " ".join(str(a) for a in atts).lower() or
                "704a" in templates or
                "704" in str(r.get("email_subject", "")).lower() or
                # Has no 704B (full RFQ requires 704B)
                ("704b" not in templates and r.get("source") == "email" and 
                 any("704" in str(a).lower() for a in atts))
            )
        
            # Also flag if it exactly matches a PC we have
            pcs = _load_price_checks()
            sol = r.get("solicitation_number", "")
            matching_pc = any(
                str(pc.get("pc_number","")).replace("-","").replace(" ","").replace("#","") ==
                str(sol).replace("-","").replace(" ","").replace("#","")
                for pc in pcs.values()
            )
        
            if is_704 or matching_pc:
                removed.append({
                    "rfq_id": rid,
                    "solicitation": sol,
                    "requestor": r.get("requestor_name", r.get("requestor_email", "")),
                    "reason": "matching_pc" if matching_pc else "detected_704_form",
                })
                del rfqs[rid]
            else:
                kept.append(rid)

        save_rfqs(rfqs)
        log.info("RFQ cleanup: removed %d entries (%s), kept %d",
                 len(removed), [r["solicitation"] for r in removed], len(kept))
        return jsonify({
            "ok": True,
            "removed": len(removed),
            "kept": len(kept),
            "removed_entries": removed,
        })



    # ═══════════════════════════════════════════════════════════════════════════════
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/item-link/lookup", methods=["POST"])
@auth_required
def api_item_link_lookup():
    """
    POST { url: "https://grainger.com/product/..." }
    Returns structured product data: title, price, part_number, shipping, supplier.
    Used for the item_link autofill on PC and RFQ line items.
    Also writes price+supplier to catalog DB for future intelligence.
    """
    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "url required"})

    try:
        from src.agents.item_link_lookup import lookup_from_url
        result = lookup_from_url(url)

        # ── Write-back to catalog DB ──
        if result.get("ok") and result.get("price"):
            try:
                from src.agents.product_catalog import (
                    match_item, add_to_catalog, add_supplier_price, init_catalog_db
                )
                init_catalog_db()
                desc = result.get("title") or result.get("description", "")
                pn = result.get("mfg_number") or result.get("part_number", "")
                supplier = result.get("supplier", "")
                price = float(result["price"])

                # Find or create catalog product
                matches = match_item(desc, pn, top_n=1) if (desc or pn) else []
                if matches and matches[0].get("match_confidence", 0) >= 0.55:
                    pid = matches[0]["id"]
                    result["catalog_product_id"] = pid
                else:
                    pid = add_to_catalog(
                        description=desc, part_number=pn,
                        cost=price, supplier_url=url,
                        manufacturer=result.get("manufacturer", ""),
                        mfg_number=result.get("mfg_number", ""),
                        source=f"link_lookup_{supplier.lower()[:20]}"
                    )
                    if pid:
                        result["catalog_product_id"] = pid

                # Record supplier price
                if pid and supplier and price > 0:
                    add_supplier_price(
                        product_id=pid,
                        supplier_name=supplier,
                        price=price,
                        url=url,
                        sku=result.get("part_number", ""),
                        shipping=result.get("shipping") or 0,
                    )
                    log.info("link_lookup → catalog pid=%d supplier=%s $%.2f", pid, supplier, price)
            except Exception as cat_err:
                log.debug("link_lookup catalog write-back: %s", cat_err)

        return jsonify(result)
    except Exception as e:
        log.error("item_link_lookup API error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/system-reset", methods=["GET", "POST"])
@auth_required
def api_admin_system_reset():
    """Full system reset: clean slate, re-process inbox through new auto-price pipeline.
    
    Steps:
    1. Delete all ghost/auto-draft quotes (keep real sent ones)
    2. Delete all auto-draft PCs (keep manually uploaded)
    3. Clear RFQ queue
    4. Reset quote counter to highest real quote
    5. Clear processed_emails.json → poller re-fetches all emails
    6. Clear stale CRM activity
    7. New emails flow through auto-PRICE pipeline (no ghost quotes)
    
    POST body (all optional):
      keep_quotes: list of quote numbers to keep (e.g. ["R26Q16"])
      keep_pcs: list of PC IDs to keep
      reset_processed: true/false (default true — clears processed emails)
      dry_run: true/false (default false)
    """
    data = request.get_json(force=True, silent=True) or {}
    keep_quotes = set(data.get("keep_quotes", []))
    keep_pcs = set(data.get("keep_pcs", []))
    reset_processed = data.get("reset_processed", True)
    dry_run = data.get("dry_run", False)
    
    # Pause background poller so it doesn't race with reset
    if not dry_run:
        try:
            from src.api.dashboard import POLL_STATUS
            POLL_STATUS["paused"] = True
            log.info("Background poller paused for system reset")
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    results = {
        "dry_run": dry_run,
        "quotes_before": 0, "quotes_after": 0, "quotes_removed": [],
        "pcs_before": 0, "pcs_after": 0, "pcs_removed": [],
        "rfqs_before": 0, "rfqs_cleared": False,
        "counter_before": 0, "counter_after": 0,
        "processed_cleared": False,
        "activity_cleaned": 0,
    }
    
    # Step 1: Clean quotes — quotes_log.json is the source of truth
    try:
        q_path = os.path.join(DATA_DIR, 'quotes_log.json')
        if os.path.exists(q_path):
            with open(q_path) as f:
                all_q = json.load(f)
            if isinstance(all_q, list):
                results["quotes_before"] = len(all_q)
                kept = []
                for q in all_q:
                    qn = q.get("quote_number", "")
                    if qn in keep_quotes:
                        kept.append(q)
                    else:
                        results["quotes_removed"].append(qn or "(blank)")
                if not dry_run:
                    with open(q_path, "w") as f:
                        json.dump(kept, f, indent=2, default=str)
                results["quotes_after"] = len(kept)
        # Also clean quotes.json if it exists (legacy)
        legacy_q = os.path.join(DATA_DIR, 'quotes.json')
        if os.path.exists(legacy_q) and not dry_run:
            with open(legacy_q, "w") as f:
                json.dump([], f)
        # Also clean SQLite quotes table
        if not dry_run:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    if keep_quotes:
                        placeholders = ",".join("?" for _ in keep_quotes)
                        conn.execute("DELETE FROM quotes WHERE quote_number NOT IN (" + placeholders + ")",
                                     list(keep_quotes))
                    else:
                        conn.execute("DELETE FROM quotes")
                    conn.commit()
                results["sqlite_cleaned"] = True
            except Exception as dbe:
                results["sqlite_error"] = str(dbe)
    except Exception as e:
        results["quotes_error"] = str(e)
    
    # Step 2: Clean PCs — remove auto-draft source PCs, keep manual uploads
    try:
        pcs = _load_price_checks()
        results["pcs_before"] = len(pcs)
        cleaned = {}
        for pid, pc in pcs.items():
            src = pc.get("source", "")
            if pid in keep_pcs:
                cleaned[pid] = pc
            elif src in ("email_auto_draft", "email_auto"):
                results["pcs_removed"].append(f'{pid[:12]} ({pc.get("pc_number","?")})')
            elif pc.get("is_auto_draft"):
                results["pcs_removed"].append(f'{pid[:12]} ({pc.get("pc_number","?")})')
            else:
                cleaned[pid] = pc
        if not dry_run:
            _save_price_checks(cleaned)
        results["pcs_after"] = len(cleaned)
    except Exception as e:
        results["pcs_error"] = str(e)
    
    # Step 3: Clear RFQ queue (rfqs.json is what load_rfqs() reads)
    try:
        for rfq_file in ['rfqs.json', 'rfq_queue.json']:
            rfq_path = os.path.join(DATA_DIR, rfq_file)
            if os.path.exists(rfq_path):
                with open(rfq_path) as f:
                    rfqs = json.load(f)
                results["rfqs_before"] = max(results.get("rfqs_before", 0), len(rfqs))
                if not dry_run:
                    with open(rfq_path, "w") as f:
                        json.dump({}, f)
                    results["rfqs_cleared"] = True
    except Exception as e:
        results["rfqs_error"] = str(e)
    
    # Step 4: Reset quote counter
    try:
        counter_path = os.path.join(DATA_DIR, 'quote_counter.json')
        if os.path.exists(counter_path):
            with open(counter_path) as f:
                counter = json.load(f)
            results["counter_before"] = counter.get("seq", 0)
        
        # Find highest kept quote number, or default to 15 (next = R26Q16)
        highest = 15  # default: next quote will be R26Q16
        if keep_quotes:
            import re as _re
            for qn in keep_quotes:
                m = _re.search(r'Q(\d+)', qn)
                if m:
                    highest = max(highest, int(m.group(1)))
        
        results["counter_after"] = highest
        if not dry_run:
            with open(counter_path, "w") as f:
                json.dump({"year": 2026, "seq": highest}, f)
    except Exception as e:
        results["counter_error"] = str(e)
    
    # Step 5: Clear processed emails → poller re-fetches everything
    if reset_processed:
        try:
            for _rpf in ('processed_emails.json', 'processed_emails_mike.json'):
                _rp = os.path.join(DATA_DIR, _rpf)
                if os.path.exists(_rp):
                    if not dry_run:
                        with open(_rp, "w") as f:
                            json.dump([], f)
                    results[f"processed_cleared_{_rpf}"] = True
        except Exception as e:
            results["processed_error"] = str(e)
    
    # Step 6: Clean stale CRM activity (auto_draft entries)
    try:
        act_path = os.path.join(DATA_DIR, 'crm_activity.json')
        if os.path.exists(act_path):
            with open(act_path) as f:
                acts = json.load(f)
            before = len(acts)
            cleaned_acts = [a for a in acts if a.get("event_type") not in ("auto_draft_generated", "auto_draft_ready")]
            results["activity_cleaned"] = before - len(cleaned_acts)
            if not dry_run:
                with open(act_path, "w") as f:
                    json.dump(cleaned_acts, f, indent=2, default=str)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    action = "DRY RUN" if dry_run else "EXECUTED"
    log.info(f"SYSTEM RESET {action}: quotes {results['quotes_before']}→{results['quotes_after']}, "
             f"PCs {results['pcs_before']}→{results['pcs_after']}, "
             f"RFQs {results['rfqs_before']}→cleared, "
             f"counter {results['counter_before']}→{results['counter_after']}")
    
    return jsonify({"ok": True, **results})


@bp.route("/api/admin/reset-and-poll", methods=["GET", "POST"])
@auth_required
def api_admin_reset_and_poll():
    """Atomic operation: pause poller → reset → set counter → poll → unpause.
    
    This is the correct way to do a full system restart.
    Prevents background poller from racing with the reset.
    
    POST body:
      keep_quotes: [] (default empty)  
      counter: 15 (default — next = R26Q16)
    """
    data = request.get_json(force=True, silent=True) or {}
    keep_quotes = data.get("keep_quotes", [])
    counter = data.get("counter", 15)
    
    from src.api.dashboard import POLL_STATUS, do_poll_check
    
    steps = {}
    
    # Step 1: Pause background poller
    POLL_STATUS["paused"] = True
    steps["poller_paused"] = True
    log.info("RESET+POLL: Step 1 — poller paused")
    
    # Step 2: Run system reset
    try:
        # Clean ALL quotes
        q_path = os.path.join(DATA_DIR, 'quotes_log.json')
        q_removed = 0
        if os.path.exists(q_path):
            with open(q_path) as f:
                all_q = json.load(f)
            q_removed = len(all_q)
            kept = [q for q in all_q if q.get("quote_number") in set(keep_quotes)]
            with open(q_path, "w") as f:
                json.dump(kept, f, indent=2, default=str)
        legacy_q = os.path.join(DATA_DIR, 'quotes.json')
        if os.path.exists(legacy_q):
            with open(legacy_q, "w") as f:
                json.dump([], f)
        try:
            from src.core.db import get_db
            with get_db() as conn:
                if keep_quotes:
                    placeholders = ",".join("?" for _ in keep_quotes)
                    conn.execute("DELETE FROM quotes WHERE quote_number NOT IN (" + placeholders + ")", list(keep_quotes))
                else:
                    conn.execute("DELETE FROM quotes")
                conn.commit()
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        steps["quotes_cleaned"] = q_removed
        # Invalidate quotes cache
        try:
            from src.api.dashboard import _invalidate_cache
            _invalidate_cache(q_path)
            if os.path.exists(legacy_q):
                _invalidate_cache(legacy_q)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        
        # Clean PCs — preserve any that have been worked on (priced, quoted, sent, completed)
        pcs = _load_price_checks()
        steps["pcs_before"] = len(pcs)
        preserved_statuses = {"draft", "sent", "not_responding"}
        preserved_pcs = {}
        removed_pcs = []
        for pcid, pc in pcs.items():
            st = pc.get("status", "new")
            has_prices = any(it.get("our_price") or it.get("unit_price") for it in pc.get("items", []))
            has_quote = bool(pc.get("reytech_quote_number"))
            if st in preserved_statuses or has_prices or has_quote:
                preserved_pcs[pcid] = pc
            else:
                removed_pcs.append(pcid)
        _save_price_checks(preserved_pcs)
        steps["pcs_after"] = len(preserved_pcs)
        steps["pcs_preserved"] = len(preserved_pcs)
        steps["pcs_removed"] = len(removed_pcs)
        log.info("RESET+POLL: Preserved %d active PCs, removed %d unworked PCs",
                 len(preserved_pcs), len(removed_pcs))
        
        # Also clear any cached PC data
        try:
            pc_path = os.path.join(DATA_DIR, 'price_checks.json')
            from src.api.dashboard import _invalidate_cache
            _invalidate_cache(pc_path)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        
        # Clear RFQs — must match rfq_db_path() which is rfqs.json
        rfq_path = os.path.join(DATA_DIR, 'rfqs.json')
        with open(rfq_path, "w") as f:
            json.dump({}, f)
        # Also clear rfq_queue.json in case anything reads from there
        rfq_queue_path = os.path.join(DATA_DIR, 'rfq_queue.json')
        if os.path.exists(rfq_queue_path):
            with open(rfq_queue_path, "w") as f:
                json.dump({}, f)
        # CRITICAL: invalidate the in-memory cache or load_rfqs() returns stale data
        try:
            from src.api.dashboard import _invalidate_cache, _json_cache, _json_cache_lock
            _invalidate_cache(rfq_path)
            _invalidate_cache(rfq_queue_path)
            with _json_cache_lock:
                _json_cache.clear()
            log.info("RESET+POLL: Cleared rfqs.json + rfq_queue.json + cache")
        except Exception as ce:
            log.warning("RESET+POLL: Cache invalidation failed: %s", ce)
        steps["rfqs_cleared"] = True
        
        # Set counter
        counter_path = os.path.join(DATA_DIR, 'quote_counter.json')
        with open(counter_path, "w") as f:
            json.dump({"year": 2026, "seq": counter}, f)
        steps["counter"] = counter
        steps["next_quote"] = f"R26Q{counter + 1}"
        
        # Clear processed emails (both inboxes)
        for _rpf2 in ('processed_emails.json', 'processed_emails_mike.json'):
            _rp2 = os.path.join(DATA_DIR, _rpf2)
            with open(_rp2, "w") as f:
                json.dump([], f)
        steps["processed_cleared"] = True
        
        # Clean CRM activity
        act_path = os.path.join(DATA_DIR, 'crm_activity.json')
        if os.path.exists(act_path):
            try:
                with open(act_path) as f:
                    acts = json.load(f)
                cleaned_acts = [a for a in acts if a.get("event_type") not in ("auto_draft_generated", "auto_draft_ready")]
                with open(act_path, "w") as f:
                    json.dump(cleaned_acts, f, indent=2, default=str)
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
        
        log.info("RESET+POLL: Step 2 — reset complete (cleared %d PCs, %d quotes)", steps["pcs_before"], q_removed)
    except Exception as e:
        steps["reset_error"] = str(e)
        log.error("RESET+POLL: reset error: %s", e, exc_info=True)
    
    # Step 3: Kick off poll in background thread (IMAP takes >30s, would timeout Railway proxy)
    import threading
    
    def _background_poll():
        """Run poll in background, store results in POLL_STATUS."""
        try:
            pcs_before = len(_load_price_checks())
            imported = _safe_do_poll_check()
            pcs_after = _load_price_checks()
            new_pcs = len(pcs_after) - pcs_before
            
            POLL_STATUS["_reset_poll_result"] = {
                "poll_pcs_created": new_pcs,
                "poll_rfqs_imported": len(imported),
                "poll_found": len(imported) + new_pcs,
                "final_pcs": len(pcs_after),
                "pc_names": [pc.get("pc_number", "?")[:40] for pc in pcs_after.values()],
                "final_rfqs": 0,
                "rfq_sols": [],
                "email_traces": POLL_STATUS.get("_email_traces", []),
                "poll_diag": POLL_STATUS.get("_diag", {}),
                "completed": True,
            }
            # Count RFQs
            try:
                rfq_path = os.path.join(DATA_DIR, 'rfqs.json')
                if os.path.exists(rfq_path):
                    with open(rfq_path) as f:
                        final_rfqs = json.load(f)
                    POLL_STATUS["_reset_poll_result"]["final_rfqs"] = len(final_rfqs)
                    POLL_STATUS["_reset_poll_result"]["rfq_sols"] = [r.get("solicitation_number", "?") for r in final_rfqs.values()]
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
            # Grab poller diag
            try:
                from src.api.dashboard import _shared_poller
                if _shared_poller and hasattr(_shared_poller, '_diag'):
                    _raw_d = _shared_poller._diag
                    POLL_STATUS["_reset_poll_result"]["poller_diag"] = {
                        k: list(v) if isinstance(v, set) else v
                        for k, v in _raw_d.items()
                    }
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
                
            log.info("RESET+POLL background: PCs=%d RFQs=%d", new_pcs, len(imported))
            
            # ── Post-poll collision resolver: RFQ takes precedence over PC ──
            try:
                final_pcs = _load_price_checks()
                final_rfqs_data = load_rfqs()
                rfq_sols = {v.get("solicitation_number") for v in final_rfqs_data.values() if v.get("solicitation_number")}
                collisions = []
                for pid, pc in list(final_pcs.items()):
                    pc_num = pc.get("pc_number", "").replace("AD-", "").strip()
                    if pc_num in rfq_sols:
                        del final_pcs[pid]
                        collisions.append(f"{pid} (pc#{pc_num})")
                if collisions:
                    _save_price_checks(final_pcs)
                    POLL_STATUS["_reset_poll_result"]["collisions_resolved"] = collisions
                    POLL_STATUS["_reset_poll_result"]["final_pcs"] = len(final_pcs)
                    log.info("Post-poll collision: removed %d PCs that matched RFQ sols: %s", len(collisions), collisions)
            except Exception as _cre:
                log.warning("Post-poll collision check: %s", _cre)
        except Exception as e:
            POLL_STATUS["_reset_poll_result"] = {"error": str(e), "completed": True}
            log.error("RESET+POLL background error: %s", e, exc_info=True)
        finally:
            POLL_STATUS["paused"] = False
            log.info("RESET+POLL: poller unpaused")
    
    POLL_STATUS["_reset_poll_result"] = {"completed": False, "status": "polling..."}
    t = threading.Thread(target=_background_poll, daemon=True, name="reset-poll")
    t.start()
    steps["poll_status"] = "started_async"
    steps["check_results"] = "GET /api/admin/poll-result"
    
    return jsonify({"ok": True, **steps})


@bp.route("/api/admin/poll-result", methods=["GET"])
@auth_required
def api_admin_poll_result():
    """Check the result of the async poll triggered by reset-and-poll."""
    from src.api.dashboard import POLL_STATUS
    result = POLL_STATUS.get("_reset_poll_result", {"completed": False, "status": "no poll running"})
    return jsonify(result)


@bp.route("/api/admin/poller-control", methods=["GET", "POST"])
@auth_required
def api_admin_poller_control():
    """Pause or unpause the background email poller.
    POST {"action": "pause"} or {"action": "unpause"}
    """
    data = request.get_json(force=True, silent=True) or {}
    action = data.get("action", "")
    from src.api.dashboard import POLL_STATUS
    
    if action == "pause":
        POLL_STATUS["paused"] = True
        return jsonify({"ok": True, "paused": True})
    elif action == "unpause":
        POLL_STATUS["paused"] = False
        return jsonify({"ok": True, "paused": False})
    else:
        return jsonify({"ok": False, "error": "action must be 'pause' or 'unpause'",
                        "paused": POLL_STATUS.get("paused", False)})


# ═══════════════════════════════════════════════════════════════════════════════
# Email Pipeline QA
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/qa/email-pipeline", methods=["GET", "POST"])
@auth_required
def api_qa_email_pipeline():
    """Run full email pipeline QA: inbox audit + classification tests."""
    try:
        from src.agents.email_pipeline_qa import full_inbox_audit
        result = full_inbox_audit()
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.error("Email pipeline QA error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/classification-test")
@auth_required
def api_qa_classification_test():
    """Run offline classification tests only (no IMAP needed)."""
    try:
        from src.agents.email_pipeline_qa import test_classification
        return jsonify({"ok": True, **test_classification()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/trends")
@auth_required
def api_qa_trends():
    """Get QA score trends over time."""
    try:
        from src.agents.email_pipeline_qa import get_qa_trends
        return jsonify({"ok": True, **get_qa_trends()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/qa/email-pipeline")
@auth_required
def qa_email_pipeline_page():
    """Email Pipeline QA dashboard page."""
    try:
        from src.agents.email_pipeline_qa import get_qa_trends
        trends = get_qa_trends()
    except Exception:
        trends = {"runs": 0, "trend": "no_data"}

    content = f'''
    <h2>Email Pipeline QA</h2>
    <p style="color:var(--tx2);margin-bottom:16px">
      Tests the full email intake pipeline: classification accuracy, inbox vs system state, gap detection.
    </p>

    <div style="display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap">
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:110px">
        <div style="font-size:28px;font-weight:800;color:var(--tx)">{trends.get('latest_score','—')}</div>
        <div style="font-size:14px;color:var(--tx2)">LATEST SCORE</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:110px">
        <div style="font-size:28px;font-weight:800;color:var(--tx)">{trends.get('latest_grade','—')}</div>
        <div style="font-size:14px;color:var(--tx2)">GRADE</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:110px">
        <div style="font-size:28px;font-weight:800;color:var(--tx)">{trends.get('runs',0)}</div>
        <div style="font-size:14px;color:var(--tx2)">QA RUNS</div></div>
      <div style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:12px 20px;text-align:center;min-width:110px">
        <div style="font-size:28px;font-weight:800;color:var(--tx)">{trends.get('trend','—')}</div>
        <div style="font-size:14px;color:var(--tx2)">TREND</div></div>
    </div>

    <div style="display:flex;gap:12px;margin-bottom:20px">
      <button onclick="runFullQA()" style="background:#238636;color:white;padding:10px 20px;border:none;border-radius:6px;cursor:pointer;font-weight:600">
        Run Full Inbox Audit</button>
      <button onclick="runClassTests()" style="background:#1f6feb;color:white;padding:10px 20px;border:none;border-radius:6px;cursor:pointer;font-weight:600">
        Run Classification Tests</button>
    </div>

    <div id="qa-results" style="background:var(--sf);border:1px solid var(--bd);border-radius:8px;padding:16px;min-height:200px">
      <p style="color:var(--tx2)">Click a button above to run QA tests...</p>
    </div>

    <script>
    function runFullQA() {{
      var el = document.getElementById('qa-results');
      el.innerHTML = '<p style="color:var(--yl)">Running full inbox audit... (connects to Gmail, may take 10-30s)</p>';
      fetch('/api/qa/email-pipeline', {{method:'POST'}})
        .then(function(r) {{ return r.json(); }})
        .then(function(d) {{
          if (!d.ok) {{ el.innerHTML = '<p style="color:#f85149">Error: ' + (d.error||'unknown') + '</p>'; return; }}
          var h = '<h3>Score: ' + d.score + '/100 (Grade ' + d.grade + ')</h3>';
          h += '<p>Emails scanned: ' + d.emails_scanned + ' | Actionable: ' + d.total_actionable + ' | Matched: ' + d.matched + ' | Gaps: ' + d.gap_count + '</p>';
          if (d.gaps && d.gaps.length > 0) {{
            h += '<h4 style="color:#f85149;margin-top:12px">GAPS (missing from system):</h4><table style="width:100%;font-size:13px;border-collapse:collapse">';
            h += '<tr style="border-bottom:1px solid var(--bd)"><th style="text-align:left;padding:6px">Subject</th><th>Expected</th><th>Sender</th><th>PDFs</th><th>Confidence</th></tr>';
            d.gaps.forEach(function(g) {{
              h += '<tr style="border-bottom:1px solid var(--bd);color:#f85149"><td style="padding:6px">' + g.subject + '</td><td>' + g.expected_type + '</td><td>' + (g.sender||'').substring(0,30) + '</td><td>' + g.pdf_count + '</td><td>' + g.confidence + '%</td></tr>';
            }});
            h += '</table>';
          }}
          if (d.classification_tests) {{
            var ct = d.classification_tests;
            h += '<h4 style="margin-top:16px">Classification Tests: ' + ct.passed + '/' + ct.total_tests + ' (' + ct.score + '%)</h4>';
            if (ct.results) {{
              h += '<table style="width:100%;font-size:14px;border-collapse:collapse">';
              h += '<tr style="border-bottom:1px solid var(--bd)"><th style="text-align:left;padding:4px">Test</th><th>RFQ</th><th>Recall</th><th>CS</th><th>Pass</th></tr>';
              ct.results.forEach(function(t) {{
                var color = t.passed ? '#3fb950' : '#f85149';
                h += '<tr style="border-bottom:1px solid var(--bd);color:' + color + '"><td style="padding:4px">' + t.label + '</td>';
                h += '<td>' + (t.rfq.ok ? 'OK' : 'FAIL') + '</td>';
                h += '<td>' + (t.recall.ok ? 'OK' : 'FAIL') + '</td>';
                h += '<td>' + (t.cs.ok ? 'OK' : 'FAIL') + '</td>';
                h += '<td>' + (t.passed ? 'PASS' : 'FAIL') + '</td></tr>';
              }});
              h += '</table>';
            }}
          }}
          el.innerHTML = h;
        }})
        .catch(function(e) {{ el.innerHTML = '<p style="color:#f85149">Error: ' + e + '</p>'; }});
    }}
    function runClassTests() {{
      var el = document.getElementById('qa-results');
      el.innerHTML = '<p style="color:var(--yl)">Running classification tests...</p>';
      fetch('/api/qa/classification-test')
        .then(function(r) {{ return r.json(); }})
        .then(function(d) {{
          var h = '<h3>Classification: ' + d.passed + '/' + d.total_tests + ' passed (' + d.score + '% — Grade ' + d.grade + ')</h3>';
          h += '<table style="width:100%;font-size:13px;border-collapse:collapse">';
          h += '<tr style="border-bottom:1px solid var(--bd)"><th style="text-align:left;padding:6px">Test</th><th>Subject</th><th>RFQ</th><th>Recall</th><th>CS</th><th>Result</th></tr>';
          (d.results||[]).forEach(function(t) {{
            var color = t.passed ? '#3fb950' : '#f85149';
            h += '<tr style="border-bottom:1px solid var(--bd)"><td style="padding:6px;color:' + color + ';font-weight:600">' + t.label + '</td>';
            h += '<td style="font-size:14px">' + t.subject + '</td>';
            h += '<td style="text-align:center;color:' + (t.rfq.ok ? '#3fb950' : '#f85149') + '">' + (t.rfq.ok ? 'OK' : t.rfq.expected + '!=' + t.rfq.actual) + '</td>';
            h += '<td style="text-align:center;color:' + (t.recall.ok ? '#3fb950' : '#f85149') + '">' + (t.recall.ok ? 'OK' : 'FAIL') + '</td>';
            h += '<td style="text-align:center;color:' + (t.cs.ok ? '#3fb950' : '#f85149') + '">' + (t.cs.ok ? 'OK' : 'FAIL') + '</td>';
            h += '<td style="text-align:center;color:' + color + ';font-weight:700">' + (t.passed ? 'PASS' : 'FAIL') + '</td></tr>';
          }});
          h += '</table>';
          el.innerHTML = h;
        }});
    }}
    </script>
    '''

    from src.api.render import render_page
    return render_page("generic.html", active_page="Intel", page_title="Email Pipeline QA", content=content)


@bp.route("/api/diag/pc/<pcid>")
@auth_required
def api_diag_pc(pcid):
    """Full diagnostic: where does this PC exist?"""
    import os, json, sqlite3
    result = {"pc_id": pcid, "found_in": []}
    from src.core.paths import DATA_DIR as _DATA_DIR
    
    # Ensure tables exist
    try:
        from src.core.db import init_db
        init_db()
    except Exception as ie:
        result["init_db_error"] = str(ie)

    # 1. Check DB directly
    try:
        from src.core.dal import get_pc as _dal_get_pc
        from src.core.db import DB_PATH as _db_path
        result["db_path"] = _db_path
        _db_pc = _dal_get_pc(pcid)
        if _db_pc:
            result["found_in"].append("db")
            result["db"] = {k: _db_pc[k] for k in ("id", "pc_number", "status", "total_items", "created_at") if k in _db_pc}
        else:
            result["db"] = None
            from src.core.dal import list_pcs as _dal_list_pcs
            all_pcs = _dal_list_pcs(limit=10000)
            result["db_total"] = len(all_pcs)
            result["db_sample"] = [p["id"] for p in all_pcs[:5]]
    except Exception as e:
        result["db_error"] = str(e)

    # 2. Check JSON directly
    try:
        json_path = os.path.join(_DATA_DIR, "price_checks.json")
        if os.path.exists(json_path):
            with open(json_path) as f:
                jdata = json.load(f)
            if pcid in jdata:
                result["found_in"].append("json")
                pc = jdata[pcid]
                result["json"] = {"pc_number": pc.get("pc_number"), "status": pc.get("status"),
                                  "items": len(pc.get("items", [])), "institution": pc.get("institution")}
            else:
                result["json"] = None
                result["json_total"] = len(jdata)
                result["json_sample"] = list(jdata.keys())[:5]
        else:
            result["json"] = "FILE NOT FOUND"
    except Exception as e:
        result["json_error"] = str(e)

    # 3. Check _load_price_checks
    try:
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        if pcid in pcs:
            result["found_in"].append("load_func")
            result["load_func"] = {"items": len(pcs[pcid].get("items", [])), "status": pcs[pcid].get("status")}
        else:
            result["load_func"] = None
            result["load_func_total"] = len(pcs)
    except Exception as e:
        result["load_func_error"] = str(e)

    # 4. Check if pc_data column exists
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            conn.execute("SELECT pc_data FROM price_checks LIMIT 0")
            result["pc_data_column"] = True
        except Exception:
            result["pc_data_column"] = False
        conn.close()
    except Exception:
        pass

    result["diagnosis"] = "PC not found anywhere" if not result["found_in"] else f"Found in: {', '.join(result['found_in'])}"
    return jsonify(result)


@bp.route("/api/disk-emergency", methods=["GET", "POST"])
@auth_required
def api_disk_emergency():
    """Emergency disk cleanup — delete backups + temp files."""
    import shutil
    from src.core.paths import DATA_DIR as _DD
    freed = 0
    deleted = []
    
    # Delete ALL backups (2.2GB)
    backup_dir = os.path.join(_DD, "backups")
    if os.path.isdir(backup_dir):
        for f in os.listdir(backup_dir):
            fp = os.path.join(backup_dir, f)
            try:
                sz = os.path.getsize(fp)
                os.remove(fp)
                freed += sz
                deleted.append(f"{f} ({sz//1048576}MB)")
            except OSError: pass

    # Delete temp/cache files
    for pattern in ["*.pyc", "auto_price_status.json", "growth_outreach_cache.json"]:
        for root, dirs, files in os.walk(_DD):
            for f in files:
                if f.endswith(".pyc") or f == pattern:
                    try:
                        fp = os.path.join(root, f)
                        sz = os.path.getsize(fp)
                        os.remove(fp)
                        freed += sz
                    except OSError: pass
    
    return jsonify({
        "ok": True,
        "freed_mb": round(freed / 1048576, 1),
        "deleted": deleted,
    })


@bp.route("/api/diag/home-timing")
@auth_required
def api_diag_home_timing():
    """Time every step of what the home page does."""
    import time as _t
    from src.core.paths import DATA_DIR as _DD
    steps = []
    
    t0 = _t.time()
    
    # 1. Check if JSON files exist
    import os
    json_pc = os.path.join(_DD, "price_checks.json")
    json_rfq = os.path.join(_DD, "rfqs.json")
    pc_exists = os.path.exists(json_pc)
    rfq_exists = os.path.exists(json_rfq)
    pc_size = os.path.getsize(json_pc) if pc_exists else 0
    rfq_size = os.path.getsize(json_rfq) if rfq_exists else 0
    steps.append({"step": "check_files", "ms": round((_t.time()-t0)*1000),
                  "pc_json_exists": pc_exists, "pc_json_kb": round(pc_size/1024,1),
                  "rfq_json_exists": rfq_exists, "rfq_json_kb": round(rfq_size/1024,1)})
    
    # 2. Load PCs
    t1 = _t.time()
    try:
        pcs = _load_price_checks()
        steps.append({"step": "load_pcs", "ms": round((_t.time()-t1)*1000), "count": len(pcs)})
    except Exception as e:
        steps.append({"step": "load_pcs", "ms": round((_t.time()-t1)*1000), "error": str(e)})
    
    # 3. Load RFQs
    t2 = _t.time()
    try:
        from src.api.dashboard import load_rfqs
        rfqs = load_rfqs()
        steps.append({"step": "load_rfqs", "ms": round((_t.time()-t2)*1000), "count": len(rfqs)})
    except Exception as e:
        steps.append({"step": "load_rfqs", "ms": round((_t.time()-t2)*1000), "error": str(e)})
    
    # 4. DB size
    db_path = os.path.join(_DD, "reytech.db")
    db_mb = round(os.path.getsize(db_path)/1048576, 1) if os.path.exists(db_path) else 0
    steps.append({"step": "db_size", "mb": db_mb})
    
    steps.append({"step": "total", "ms": round((_t.time()-t0)*1000)})
    
    return jsonify({"steps": steps})


@bp.route("/api/db-repair", methods=["GET", "POST"])
@auth_required
def api_db_repair():
    """Repair corrupted SQLite DB by rebuilding it."""
    import sqlite3, shutil
    from src.core.paths import DATA_DIR as _DD
    
    db_path = os.path.join(_DD, "reytech.db")
    backup_path = db_path + ".corrupt_backup"
    new_path = db_path + ".rebuilt"
    
    if not os.path.exists(db_path):
        return jsonify({"ok": False, "error": "DB not found"})
    
    steps = []
    db_size = os.path.getsize(db_path)
    steps.append(f"Original DB: {db_size // 1048576}MB")
    
    try:
        # Step 1: Try integrity check
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            result = conn.execute("PRAGMA integrity_check").fetchone()
            steps.append(f"Integrity: {result[0]}")
        except Exception as e:
            steps.append(f"Integrity check failed: {e}")
        conn.close()
        
        # Step 2: Rebuild via dump + reimport
        steps.append("Rebuilding via .dump → reimport...")
        old_conn = sqlite3.connect(db_path, timeout=30)
        new_conn = sqlite3.connect(new_path, timeout=30)
        
        # Dump and reimport
        dumped = 0
        errors = 0
        for line in old_conn.iterdump():
            try:
                new_conn.execute(line)
                dumped += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    steps.append(f"Skip: {str(e)[:80]}")
        
        new_conn.commit()
        new_conn.close()
        old_conn.close()
        
        new_size = os.path.getsize(new_path)
        steps.append(f"Rebuilt: {dumped} statements, {errors} errors, {new_size // 1048576}MB")
        
        # Step 3: Swap
        shutil.move(db_path, backup_path)
        shutil.move(new_path, db_path)
        steps.append("Swapped: corrupt → .corrupt_backup, rebuilt → reytech.db")
        
        # Step 4: Verify
        conn = sqlite3.connect(db_path, timeout=10)
        result = conn.execute("PRAGMA integrity_check").fetchone()
        steps.append(f"New integrity: {result[0]}")
        
        # WAL mode
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()
        steps.append("WAL mode enabled")
        
        return jsonify({"ok": True, "steps": steps, 
                        "old_mb": db_size // 1048576, "new_mb": new_size // 1048576})
    
    except Exception as e:
        # Cleanup
        if os.path.exists(new_path):
            os.remove(new_path)
        return jsonify({"ok": False, "error": str(e), "steps": steps})


@bp.route("/api/db-rebuild", methods=["GET", "POST"])
@auth_required
def api_db_rebuild():
    """Nuclear option: delete corrupt DB, create fresh, reimport from JSON files."""
    import sqlite3, shutil
    from src.core.paths import DATA_DIR as _DD
    from src.core.db import init_db
    
    db_path = os.path.join(_DD, "reytech.db")
    wal_path = db_path + "-wal"
    shm_path = db_path + "-shm"
    corrupt_path = db_path + ".corrupt"
    
    steps = []
    
    # Step 1: Move corrupt DB aside
    old_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    steps.append(f"Corrupt DB: {old_size // 1048576}MB")
    
    try:
        if os.path.exists(db_path):
            shutil.move(db_path, corrupt_path)
            steps.append("Moved corrupt DB to .corrupt")
        for p in [wal_path, shm_path]:
            if os.path.exists(p):
                os.remove(p)
                steps.append(f"Removed {os.path.basename(p)}")
    except Exception as e:
        return jsonify({"ok": False, "error": f"Move failed: {e}", "steps": steps})
    
    # Step 2: Create fresh DB
    try:
        init_db()
        new_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        steps.append(f"Fresh DB created: {new_size // 1024}KB")
    except Exception as e:
        # Restore corrupt DB if init fails
        if os.path.exists(corrupt_path) and not os.path.exists(db_path):
            shutil.move(corrupt_path, db_path)
        return jsonify({"ok": False, "error": f"init_db failed: {e}", "steps": steps})
    
    # Step 3: Reimport from JSON files
    imported = {}
    
    # Price checks
    pc_json = os.path.join(_DD, "price_checks.json")
    if os.path.exists(pc_json):
        try:
            with open(pc_json) as f:
                pcs = json.load(f)
            from src.api.dashboard import _save_price_checks
            _save_price_checks(pcs)
            imported["price_checks"] = len(pcs)
            steps.append(f"Imported {len(pcs)} price checks from JSON")
        except Exception as e:
            steps.append(f"PC import error: {e}")
    
    # RFQs
    rfq_json = os.path.join(_DD, "rfqs.json")
    if os.path.exists(rfq_json):
        try:
            with open(rfq_json) as f:
                rfqs = json.load(f)
            from src.api.dashboard import save_rfqs
            save_rfqs(rfqs)
            imported["rfqs"] = len(rfqs)
            steps.append(f"Imported {len(rfqs)} RFQs from JSON")
        except Exception as e:
            steps.append(f"RFQ import error: {e}")
    
    # Orders
    orders_json = os.path.join(_DD, "orders.json")
    if os.path.exists(orders_json):
        try:
            with open(orders_json) as f:
                orders = json.load(f)
            from src.api.dashboard import _save_orders
            _save_orders(orders)
            imported["orders"] = len(orders)
            steps.append(f"Imported {len(orders)} orders from JSON")
        except Exception as e:
            steps.append(f"Orders import error: {e}")
    
    # Step 4: Enable WAL mode
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()
        steps.append("WAL mode enabled")
    except Exception as e:
        steps.append(f"WAL mode error: {e}")
    
    # Step 5: Delete corrupt backup (save disk space)
    if os.path.exists(corrupt_path):
        corrupt_size = os.path.getsize(corrupt_path) // 1048576
        os.remove(corrupt_path)
        steps.append(f"Deleted corrupt backup ({corrupt_size}MB freed)")
    
    final_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    steps.append(f"Final DB: {final_size // 1024}KB")
    
    return jsonify({
        "ok": True,
        "old_mb": old_size // 1048576,
        "new_kb": final_size // 1024,
        "imported": imported,
        "steps": steps,
    })


@bp.route("/api/quote-fix", methods=["GET", "POST"])
@auth_required
def api_quote_fix():
    """Fix duplicate R26Q17 and set counter correctly."""
    from src.core.db import get_db
    import re
    
    result = {"fixes": [], "counter_before": None, "counter_after": None}
    
    try:
        with get_db() as conn:
            # 1. Find ALL quotes and their numbers
            quotes = conn.execute("SELECT quote_number, status, total, created_at, agency FROM quotes ORDER BY created_at").fetchall()
            result["all_quotes"] = [dict(q) for q in quotes]
            
            # 2. Find the max quote number
            max_num = 0
            for q in quotes:
                m = re.match(r'R26Q(\d+)', q["quote_number"] or "")
                if m:
                    max_num = max(max_num, int(m.group(1)))
            
            # 3. Also scan price_checks for quote numbers
            try:
                pcs = conn.execute("SELECT id, quote_number FROM price_checks WHERE quote_number IS NOT NULL AND quote_number != ''").fetchall()
                for pc in pcs:
                    m = re.match(r'R26Q(\d+)', pc["quote_number"] or "")
                    if m:
                        max_num = max(max_num, int(m.group(1)))
                    result["fixes"].append(f"PC {pc['id'][:20]} has quote {pc['quote_number']}")
            except Exception:
                pass
            
            # 4. Also scan rfqs.json for quote numbers
            try:
                from src.api.dashboard import load_rfqs
                rfqs = load_rfqs()
                for rid, r in rfqs.items():
                    qn = r.get("reytech_quote_number", "")
                    if qn:
                        m = re.match(r'R26Q(\d+)', qn)
                        if m:
                            max_num = max(max_num, int(m.group(1)))
                        result["fixes"].append(f"RFQ {rid[:25]} has quote {qn}")
            except Exception:
                pass
            
            # 5. Set counter to max
            conn.execute("""CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT, updated_by TEXT DEFAULT 'system'
            )""")
            
            old_row = conn.execute("SELECT value FROM app_settings WHERE key='quote_counter'").fetchone()
            result["counter_before"] = old_row[0] if old_row else "NOT SET"
            
            conn.execute("""
                INSERT INTO app_settings (key, value, updated_at, updated_by) 
                VALUES ('quote_counter', ?, datetime('now'), 'quote_fix')
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """, (str(max_num),))
            
            result["counter_after"] = max_num
            result["next_quote"] = f"R26Q{max_num + 1}"
            result["max_found"] = max_num
            result["fixes"].append(f"Counter set to {max_num} → next will be R26Q{max_num + 1}")
    
    except Exception as e:
        result["error"] = str(e)
    
    return jsonify(result)


@bp.route("/api/rfq/<rid>/package-contents")
@auth_required
def api_rfq_package_contents(rid):
    """Show what's inside the generated package PDF."""
    from pypdf import PdfReader
    from src.core.paths import DATA_DIR as _DD
    
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    sol = r.get("solicitation_number", "unknown")
    out_dir = os.path.join(_DD, "output", sol)
    
    result = {"rfq_id": rid, "sol": sol, "agency": r.get("agency", ""), "files_in_dir": [], "package_pages": []}
    
    # List all files in output dir
    if os.path.exists(out_dir):
        for f in sorted(os.listdir(out_dir)):
            fpath = os.path.join(out_dir, f)
            result["files_in_dir"].append({
                "name": f,
                "size_kb": round(os.path.getsize(fpath) / 1024, 1),
            })
    
    # Analyze the merged package PDF
    pkg_name = f"RFQ_Package_{sol.replace(' ','_')}_ReytechInc.pdf"
    pkg_path = os.path.join(out_dir, pkg_name)
    if not os.path.exists(pkg_path):
        # Try alternate names
        for f in os.listdir(out_dir) if os.path.exists(out_dir) else []:
            if "Package" in f and f.endswith(".pdf"):
                pkg_path = os.path.join(out_dir, f)
                break
    
    if os.path.exists(pkg_path):
        reader = PdfReader(pkg_path)
        result["package_total_pages"] = len(reader.pages)
        for i, page in enumerate(reader.pages):
            try:
                text = (page.extract_text() or "")[:200].strip()
            except Exception:
                text = "(could not extract)"
            result["package_pages"].append({
                "page": i + 1,
                "text_preview": text[:150],
            })
    else:
        result["package_error"] = f"Package not found at {pkg_path}"
    
    # Show what output_files the RFQ thinks it has
    result["rfq_output_files"] = r.get("output_files", [])
    
    return jsonify(result)


@bp.route("/api/quote-set-counter/<int:num>", methods=["GET", "POST"])
@auth_required
def api_quote_set_counter(num):
    """Manually set the quote counter. Next quote will be R26Q(num+1).
    Writes ALL counter keys used by quote_generator._load_counter() to prevent drift."""
    from src.forms.quote_generator import set_quote_counter
    import datetime as _dt
    set_quote_counter(num, year=_dt.datetime.now().year)
    return jsonify({"ok": True, "counter": num, "next_quote": f"R{str(_dt.datetime.now().year)[-2:]}Q{num+1}",
                    "note": "All counter keys synced (quote_counter, quote_counter_seq, quote_counter_year)"})


@bp.route("/api/pricecheck/<pcid>/rescrape-unpriced", methods=["POST"])
@auth_required
def api_rescrape_unpriced(pcid):
    """Re-scrape items that have a URL but no price."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items", [])
    attempted = 0
    priced = 0
    for i, item in enumerate(items):
        url = (item.get("item_link") or "").strip()
        existing_cost = item.get("vendor_cost") or item.get("unit_price") or 0
        try:
            existing_cost = float(existing_cost)
        except (ValueError, TypeError):
            existing_cost = 0
        if not url or existing_cost > 0:
            continue
        attempted += 1
        try:
            from src.agents.item_link_lookup import lookup_from_url
            r = lookup_from_url(url)
            price = r.get("price") or r.get("list_price") or r.get("cost")
            if price:
                try:
                    price = float(price)
                except (ValueError, TypeError):
                    price = 0
            else:
                price = 0
            # Amazon fallback for non-Amazon URLs
            if price <= 0 and "amazon.com" not in url.lower():
                _search_q = r.get("title") or r.get("description") or item.get("description", "")
                if _search_q and len(_search_q) >= 8:
                    try:
                        from src.agents.product_research import search_amazon
                        amz = search_amazon(_search_q, max_results=1)
                        if amz and amz[0].get("price", 0) > 0:
                            price = float(amz[0]["price"])
                            _amz_asin = amz[0].get("asin", "")
                            if _amz_asin:
                                item["item_link"] = amz[0].get("url", "") or f"https://www.amazon.com/dp/{_amz_asin}"
                                item["item_supplier"] = "Amazon"
                                if not item.get("pricing"):
                                    item["pricing"] = {}
                                item["pricing"]["amazon_asin"] = _amz_asin
                    except Exception:
                        pass
            # Update MFG#/description from scrape
            _pn = r.get("mfg_number") or r.get("part_number") or ""
            if _pn and not item.get("item_number"):
                item["item_number"] = _pn
                item["mfg_number"] = _pn
            _title = r.get("title") or r.get("description") or ""
            if _title and (not item.get("description") or len(item.get("description", "")) < 10):
                item["description"] = _title
            if price > 0:
                if not item.get("pricing"):
                    item["pricing"] = {}
                item["pricing"]["unit_cost"] = price
                item["pricing"]["source"] = "rescrape"
                item["vendor_cost"] = price
                markup = item.get("markup_pct") or pc.get("default_markup") or 25
                try:
                    markup = float(markup)
                except (ValueError, TypeError):
                    markup = 25
                item["markup_pct"] = markup
                unit_price = round(price * (1 + markup / 100), 2)
                item["unit_price"] = unit_price
                item["pricing"]["recommended_price"] = unit_price
                qty = item.get("qty", 1) or 1
                try:
                    qty = float(qty)
                except (ValueError, TypeError):
                    qty = 1
                item["extension"] = round(unit_price * qty, 2)
                priced += 1
        except Exception as e:
            log.error("Rescrape error line %d: %s", i + 1, e, exc_info=True)
    if priced > 0 or attempted > 0:
        _save_single_pc(pcid, pc)
    return jsonify({"ok": True, "attempted": attempted, "priced": priced,
                    "total_items": len(items)})


@bp.route("/api/pricecheck/<pcid>/bulk-scrape-urls", methods=["POST"])
@auth_required
def api_bulk_scrape_urls(pcid):
    """Bulk paste URLs → scrape each → apply cost + supplier to items by index."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    urls = data.get("urls", [])
    if not urls:
        return jsonify({"ok": False, "error": "No URLs provided"})
    items = pc.get("items", [])
    results = []
    applied = 0
    for i, url in enumerate(urls):
        url = (url or "").strip()
        # Strip numbered prefixes like "1. " or "19. "
        import re as _re_temp
        url = _re_temp.sub(r'^\d+\.\s*', '', url)
        if not url or i >= len(items):
            results.append({"line": i + 1, "url": url[:60], "status": "skipped"})
            continue
        try:
            from src.agents.item_link_lookup import lookup_from_url
            r = lookup_from_url(url)
            item = items[i]
            # Always apply URL, supplier, MFG#, description — even without price
            item["item_link"] = url
            item["item_supplier"] = r.get("supplier", "")
            _pn = r.get("mfg_number") or r.get("part_number") or ""
            if _pn:
                item["item_number"] = _pn
                item["mfg_number"] = _pn
            _title = r.get("title") or r.get("description") or ""
            if _title and (not item.get("description") or len(item.get("description", "")) < 10):
                item["description"] = _title
            # Apply pricing if found
            price = r.get("price") or r.get("list_price") or r.get("cost")
            if price:
                try:
                    price = float(price)
                except (ValueError, TypeError):
                    price = 0
            else:
                price = 0
            # Amazon fallback: if non-Amazon URL has no price, search Amazon by description
            amazon_fallback = False
            if price <= 0 and "amazon.com" not in url.lower():
                _search_q = _title or item.get("description", "")
                if _search_q and len(_search_q) >= 8:
                    try:
                        from src.agents.product_research import search_amazon
                        amz = search_amazon(_search_q, max_results=1)
                        if amz and amz[0].get("price", 0) > 0:
                            price = float(amz[0]["price"])
                            amazon_fallback = True
                            # Store Amazon ASIN for reference
                            _amz_asin = amz[0].get("asin", "")
                            if _amz_asin:
                                if not item.get("pricing"):
                                    item["pricing"] = {}
                                item["pricing"]["amazon_asin"] = _amz_asin
                                item["pricing"]["amazon_url"] = amz[0].get("url", "")
                                # Switch entire item to Amazon source for easier ordering
                                item["item_link"] = amz[0].get("url", "") or f"https://www.amazon.com/dp/{_amz_asin}"
                                item["item_supplier"] = "Amazon"
                                url = item["item_link"]  # update url for pricing source_url
                            log.info("Amazon fallback for line %d: %s → $%.2f (ASIN: %s)",
                                     i + 1, _search_q[:40], price, _amz_asin)
                    except Exception as e:
                        log.debug("Amazon fallback error line %d: %s", i + 1, e)
            if price > 0:
                if not item.get("pricing"):
                    item["pricing"] = {}
                item["pricing"]["unit_cost"] = price
                item["pricing"]["source_url"] = url
                item["pricing"]["source"] = "amazon_fallback" if amazon_fallback else "bulk_scrape"
                item["vendor_cost"] = price
                markup = item.get("markup_pct") or pc.get("default_markup") or 25
                try:
                    markup = float(markup)
                except (ValueError, TypeError):
                    markup = 25
                item["markup_pct"] = markup
                unit_price = round(price * (1 + markup / 100), 2)
                item["unit_price"] = unit_price
                item["pricing"]["recommended_price"] = unit_price
                qty = item.get("qty", 1) or 1
                try:
                    qty = float(qty)
                except (ValueError, TypeError):
                    qty = 1
                item["extension"] = round(unit_price * qty, 2)
                _status = "ok" if not amazon_fallback else "ok_amazon"
                results.append({"line": i + 1, "url": url[:60], "status": _status,
                               "price": price, "supplier": r.get("supplier", ""),
                               "note": "Price from Amazon" if amazon_fallback else ""})
            else:
                results.append({"line": i + 1, "url": url[:60], "status": "linked",
                               "supplier": r.get("supplier", ""), "note": "URL linked, no price found"})
            applied += 1
        except Exception as e:
            results.append({"line": i + 1, "url": url[:60], "status": "error", "error": str(e)[:80]})
    if applied:
        _save_single_pc(pcid, pc)
        # Auto-confirm scraped items to catalog
        try:
            from src.agents.product_catalog import save_pc_items_to_catalog
            cat_result = save_pc_items_to_catalog(pc)
            log.info("Bulk-scrape catalog sync: added=%d existing=%d skipped=%d",
                     cat_result.get("added", 0), cat_result.get("existing", 0), cat_result.get("skipped", 0))
        except Exception as e:
            log.error("Bulk-scrape catalog sync error: %s", e, exc_info=True)
    return jsonify({"ok": True, "results": results, "applied": applied, "total": len(urls)})


@bp.route("/api/pricecheck/<pcid>/send-quote", methods=["POST"])
@auth_required
def api_pc_send_quote(pcid):
    """Send the generated PC quote PDF via email."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.get_json(force=True, silent=True) or {}
    to_email = data.get("to") or pc.get("requestor_email", pc.get("requestor", ""))
    pc_num = pc.get("pc_number", pcid)
    subject = data.get("subject") or f"Price Quote — {pc_num}"
    body_text = data.get("body") or f"Please find attached our price quote for {pc_num}.\n\nThank you,\nReytech Inc."

    if not to_email or "@" not in to_email:
        return jsonify({"ok": False, "error": "No valid recipient email"})

    # Find the latest generated PDF
    pdf_path = ""
    qn = pc.get("reytech_quote_number", "")
    if qn:
        import re as _re
        safe = _re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
        for candidate in [
            os.path.join(DATA_DIR, f"Quote_{safe}_Reytech.pdf"),
            os.path.join(DATA_DIR, f"PC_{safe}_Reytech.pdf"),
        ]:
            if os.path.exists(candidate):
                pdf_path = candidate
                break
    if not pdf_path:
        # Try rfq_files DB
        try:
            from src.api.dashboard import list_rfq_files, get_rfq_file
            files = list_rfq_files(pcid, category="generated")
            if files:
                full = get_rfq_file(files[0]["id"])
                if full and full.get("data"):
                    import tempfile
                    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                    tmp.write(full["data"])
                    tmp.close()
                    pdf_path = tmp.name
        except Exception:
            pass

    if not pdf_path:
        return jsonify({"ok": False, "error": "No generated PDF found — generate first"})

    # Send via Gmail
    try:
        gmail_user = os.environ.get("GMAIL_ADDRESS", "")
        gmail_pass = os.environ.get("GMAIL_PASSWORD", "")
        if not gmail_user or not gmail_pass:
            return jsonify({"ok": False, "error": "Gmail not configured"})

        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders

        msg = MIMEMultipart()
        msg["From"] = f"Reytech Inc. <{gmail_user}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body_text, "plain"))

        with open(pdf_path, "rb") as f:
            part = MIMEBase("application", "pdf")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="Quote_{pc_num}_Reytech.pdf"')
            msg.attach(part)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, [to_email], msg.as_string())

        # Update PC status
        pc["status"] = "sent"
        pc["sent_at"] = datetime.now().isoformat()
        pc["sent_to"] = to_email
        _save_single_pc(pcid, pc)

        # Log activity
        try:
            _log_crm_activity(pcid, "pc_quote_sent",
                f"Quote {qn} sent to {to_email} for PC #{pc_num}",
                actor="user")
        except Exception:
            pass

        return jsonify({"ok": True, "sent_to": to_email, "quote": qn})
    except Exception as e:
        log.error("PC send-quote: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:200]})


@bp.route("/api/pricecheck/<pcid>/duplicate", methods=["POST"])
@auth_required
def api_pc_duplicate(pcid):
    """Duplicate a PC with all items and pricing. New PC number."""
    import uuid, copy
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    new_id = f"pc_{str(uuid.uuid4())[:8]}"
    new_pc = copy.deepcopy(pc)
    new_pc["id"] = new_id
    new_pc["status"] = "draft"
    new_pc["reytech_quote_number"] = ""
    new_pc["output_pdf"] = ""
    new_pc["reytech_quote_pdf"] = ""
    new_pc["created_at"] = datetime.now().isoformat()
    new_pc["duplicated_from"] = pcid
    # Keep items, pricing, institution — user changes what they need
    pcs[new_id] = new_pc
    _save_single_pc(new_id, new_pc)
    log.info("Duplicated PC %s → %s", pcid, new_id)
    return jsonify({"ok": True, "new_id": new_id, "redirect": f"/pricecheck/{new_id}"})


@bp.route("/api/pricecheck/<pcid>/update-status", methods=["POST"])
@auth_required
def api_pc_update_status(pcid):
    """Update PC status (won, lost, sent, etc.)."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    new_status = data.get("status", "").strip()
    valid = ("new", "parsed", "draft", "priced", "ready", "sent", "won", "lost", "expired", "no_response")
    if new_status not in valid:
        return jsonify({"ok": False, "error": f"Invalid status. Valid: {', '.join(valid)}"})
    old = pc.get("status", "")
    pc["status"] = new_status
    if new_status in ("won", "lost", "expired"):
        pc["closed_at"] = datetime.now().isoformat()
        if data.get("reason"):
            pc["closed_reason"] = data["reason"]
    _save_single_pc(pcid, pc)
    log.info("PC %s status: %s → %s", pcid, old, new_status)
    return jsonify({"ok": True, "old": old, "new": new_status})
