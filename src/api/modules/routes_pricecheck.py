# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route
from src.core.security import rate_limit
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
    """Snapshot current PC state before changes are applied.
    Safe to call without try/except — logs errors internally."""
    try:
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
    except Exception as e:
        log.warning("PC revision save failed for %s (%s): %s", pcid, reason, e)
        return None


@bp.route("/api/pricecheck/<pcid>/revisions")
@auth_required
@safe_route
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
@safe_route
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
@safe_route
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
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

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
@safe_route
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
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

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
        "email_message_id": pc.get("email_message_id", ""),
        "original_sender": pc.get("original_sender", ""),
        "email_subject": pc.get("email_subject", ""),
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
@safe_route
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
@safe_route
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
@safe_page
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

    # Quote Model V2 adapter: when flag is on, round-trip through pydantic
    # for validation + computed fields. Falls back to raw dict if flag is off.
    try:
        from src.core.quote_adapter import adapt_pc
        pc = adapt_pc(pc, pcid)
    except Exception as _adapt_e:
        log.debug("Quote adapter skipped: %s", _adapt_e)

    import copy as _copy
    # CRITICAL: deep copy for rendering — never mutate cached objects.
    # _load_price_checks() has a 30s in-memory cache. If we mutate items here
    # (description cleaning line ~268, link promotion line ~348, line_number
    # assignment line ~704), those changes persist in the cache and get written
    # to DB on the next /save-prices call — causing "data replaced without
    # prompt" where just OPENING a PC page silently changes stored data.
    items = _copy.deepcopy(pc.get("items") or [])
    header = _copy.deepcopy((pc.get("parsed") or {}).get("header") or {})

    # Catalog-is-bible hydration: backfill item_link / photo_url / mfg_number /
    # upc from the catalog on every render. Autosave writes to catalog; read
    # hydrates from catalog — so reloads never lose URLs/photos the operator
    # already sourced.
    try:
        from src.core.record_fields import hydrate_item_from_catalog
        for _it in items:
            try:
                hydrate_item_from_catalog(_it)
            except Exception as _he:
                log.debug("pc hydrate item: %s", _he)
    except Exception as _e:
        log.debug("pc hydrate import: %s", _e)

    items_html = ""
    # Review UX: classify each item into a tier so the user knows at a glance
    # what's safe to quote vs what needs a second look. Counts feed the
    # summary banner above the table.
    tier_counts = {"READY": 0, "REVIEW": 0, "MANUAL": 0, "SKIP": 0}
    # Confidence gate for READY tier. Flag-tunable so prod can nudge the bar
    # up/down via /api/admin/flags (key: pipeline.confidence_threshold).
    try:
        from src.core.flags import get_flag
        _ready_threshold = float(get_flag("pipeline.confidence_threshold", 0.75))
    except Exception:
        _ready_threshold = 0.75
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
        # Strip URLs that got pasted into descriptions (from LLM bulk paste)
        import re as _re_desc
        _desc_val = item.get("description", "")
        if _desc_val and _re_desc.search(r'https?://', _desc_val):
            _cleaned_desc = _re_desc.sub(r'\s*https?://\S+', '', _desc_val).strip()
            if _cleaned_desc and len(_cleaned_desc) >= 5:
                item["description"] = _cleaned_desc
        display_desc = item.get("description") or raw_desc or ""
        # Cost sources (ensure numeric types — JSON data can have strings)
        def _safe_float(v, default=0):
            if v is None: return default
            try: return float(v)
            except (ValueError, TypeError): return default
        
        amazon_cost = _safe_float(p.get("amazon_price"), None)
        scprs_cost = _safe_float(p.get("scprs_price"), None)
        # Best available SUPPLIER cost — NEVER use SCPRS or Amazon as cost.
        # SCPRS = what the STATE paid (a sell-price ceiling, not our cost).
        # Amazon = retail price (not our wholesale cost).
        # Only use actual supplier costs (unit_cost, catalog_cost, web_cost).
        unit_cost = (_safe_float(p.get("unit_cost"))
                     or _safe_float(p.get("catalog_cost"))
                     or _safe_float(p.get("web_cost"))
                     or _safe_float(item.get("vendor_cost"))
                     or 0)

        # ── GUARDRAIL: Sanity check cost against known references ────────
        # If SCPRS or Catalog gives a reference AND our cost is >3x higher,
        # something is wrong (bad Amazon match, wrong product scraped).
        # Use the lower reference as cost instead.
        _ref_price = _safe_float(p.get("catalog_cost")) or scprs_cost or 0
        if unit_cost > 0 and _ref_price > 0 and unit_cost > _ref_price * 3:
            log.warning("COST_GUARDRAIL: item '%s' cost $%.2f is >3x reference $%.2f — "
                        "using reference as cost (likely bad scrape)",
                        (item.get("description") or "")[:40], unit_cost, _ref_price)
            item["_cost_override_reason"] = (
                f"Cost ${unit_cost:.2f} was >3x reference ${_ref_price:.2f} — auto-corrected"
            )
            unit_cost = _ref_price

        # ── LANDED COST: factor in shipping + tax based on supplier profile ──
        _supplier_name = item.get("item_supplier") or p.get("source") or ""
        _landed_cost = unit_cost
        _landed_ship = 0.0
        _landed_tax = 0.0
        _landed_note = ""
        if unit_cost > 0 and _supplier_name:
            try:
                _raw_qty_lc = item.get("qty", 1)
                try:
                    _qty_lc = int(float(_raw_qty_lc)) if _raw_qty_lc else 1
                except (ValueError, TypeError):
                    _qty_lc = 1
                from src.core.db import calc_landed_cost
                _lc = calc_landed_cost(unit_cost, _qty_lc, _supplier_name)
                _landed_cost = _lc["landed_cost"]
                _landed_ship = _lc["shipping_per_unit"]
                _landed_tax = _lc["tax_per_unit"]
                _landed_note = _lc["breakdown"]
            except Exception as _e:
                log.debug("suppressed: %s", _e)
        item["_landed_cost"] = _landed_cost
        item["_landed_ship"] = _landed_ship
        item["_landed_tax"] = _landed_tax
        item["_landed_note"] = _landed_note

        # Markup and final price — user markup ALWAYS wins over Oracle recommendation
        # Apply markup to LANDED cost (includes shipping + tax if applicable)
        markup_pct = _safe_float(p.get("markup_pct"), 25)
        if _landed_cost > 0 and markup_pct > 0:
            final_price = round(_landed_cost * (1 + markup_pct/100), 2)
        elif unit_cost > 0 and markup_pct > 0:
            final_price = round(unit_cost * (1 + markup_pct/100), 2)
        else:
            # No cost or markup — fall back to Oracle recommendation
            final_price = _safe_float(p.get("recommended_price")) or 0

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

        # Discount / sale price — for "if discount holds" profit calculator
        _sale_price = _safe_float(item.get("sale_price") or p.get("sale_price"), 0)
        _list_price_val = _safe_float(item.get("list_price") or p.get("list_price"), 0)
        # sale_price is the discounted cost; if it's less than unit_cost, it's the discount cost
        discount_cost = _sale_price if (_sale_price > 0 and _sale_price < unit_cost) else 0

        # Per-item profit. Dual-pill rendering when the item has a discount cost
        # below MSRP — operators need to see "what we'd actually clear" alongside
        # the conservative MSRP-based number, not buried in a tooltip.
        item_profit = round((final_price - unit_cost) * qty, 2) if (final_price and unit_cost) else 0
        profit_color = "#3fb950" if item_profit > 0 else ("#f85149" if item_profit < 0 else "#8b949e")
        profit_str = f'<span style="color:{profit_color}">${item_profit:.2f}</span>' if (final_price and unit_cost) else "—"
        if discount_cost > 0 and final_price > 0:
            disc_profit = round((final_price - discount_cost) * qty, 2)
            extra = round(disc_profit - item_profit, 2)
            profit_str = (
                f'<div style="display:flex;flex-direction:column;gap:2px;align-items:flex-end">'
                f'<span class="profit-pill profit-pill-msrp" '
                f'style="padding:1px 6px;border-radius:999px;background:rgba(139,148,158,.15);'
                f'color:{profit_color};font-size:12px;font-weight:600" '
                f'title="MSRP-based profit: cost ${unit_cost:.2f}/ea">${item_profit:.2f}</span>'
                f'<span class="profit-pill profit-pill-disc" '
                f'style="padding:1px 6px;border-radius:999px;background:rgba(52,211,153,.18);'
                f'color:#34d399;font-size:12px;font-weight:600" '
                f'title="If discount holds: cost ${discount_cost:.2f}/ea → profit ${disc_profit:.2f} (+${extra:.2f})">'
                f'${disc_profit:.2f} disc</span>'
                f'</div>'
            )
        
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
        if _sale_price > 0 and unit_cost > 0 and _sale_price < unit_cost:
            _disc_pct = round((1 - _sale_price / unit_cost) * 100)
            supplier_badge += f'<span style="font-size:12px;color:#34d399;display:block">sale ${_sale_price:.2f} ({_disc_pct}% off)</span>'
        # Price history toggle link for this item (P2.1 — uses item index, not mfg_number)
        ph_link = f' <a onclick="loadItemPriceHistory({idx},this)" style="cursor:pointer;color:#8b949e;font-size:12px;margin-left:4px">&#x25b8; Price history</a>'

        # ── Unified Sources column: all price sources as compact chips ──
        sources = []  # list of (price, label, url, color, is_preferred)
        known_supplier = (item.get("item_supplier") or "").lower()  # supplier from pasted URL
        cat_best_sup = (p.get("catalog_best_supplier") or "").lower()

        if scprs_cost:
            scprs_conf_str = f" ({scprs_conf:.0%})" if scprs_conf else ""
            sources.append((scprs_cost, f"SCPRS{scprs_conf_str}", "", "#3fb950", True, scprs_conf))
        # Fallback: if amazon_price not stored but item supplier is Amazon with ASIN, use unit_cost
        if not amazon_cost and asin and (item.get("item_supplier") or "").lower() == "amazon" and unit_cost:
            amazon_cost = unit_cost
        if amazon_cost:
            a_url = p.get("amazon_url", "")
            # Detect actual source from URL — don't assume Amazon
            a_source = "Amazon"
            if a_url:
                _domain = ""
                try:
                    from urllib.parse import urlparse
                    _domain = urlparse(a_url).hostname or ""
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
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
            # AI attribution: when the Grok validator found/verified this price,
            # annotate so the user can see it wasn't a blind scrape.
            if p.get("llm_validated") or p.get("price_source") == "llm_grok":
                a_label += " · AI"
            # Preferred if we've used this supplier before
            a_pref = a_source.lower() in cat_best_sup or a_source.lower() in known_supplier or "amazon" in known_supplier
            # Amazon/retail: exact if ASIN confirmed or part# matched in title
            _a_conf = 0.90 if asin else 0.70
            sources.append((amazon_cost, a_label, a_url, "#ff9900", a_pref, _a_conf))
        web_price = _safe_float(p.get("web_price"), 0)
        if web_price and web_price != amazon_cost:
            w_src = p.get("web_source", "Web")[:20]
            w_pref = w_src.lower() in cat_best_sup or w_src.lower() in known_supplier
            sources.append((web_price, w_src, p.get("web_url", ""), "#d2a8ff", w_pref, 0.65))
        cat_cost = _safe_float(p.get("catalog_cost")) or _safe_float(p.get("last_cost"), 0)
        cat_match = p.get("catalog_match", "")
        cat_pid = p.get("catalog_product_id")
        if cat_cost and cat_match:
            cat_url = p.get("catalog_url", p.get("best_supplier_url", ""))
            if not cat_url and cat_pid:
                cat_url = f"/catalog/{cat_pid}"
            cat_sup = p.get("catalog_best_supplier", "")
            cat_label = f"📦 {cat_sup}" if cat_sup else "📦 Catalog"
            _cat_conf = _safe_float(p.get("catalog_confidence"), 0.80)
            sources.append((cat_cost, cat_label, cat_url, "#58a6ff", True, _cat_conf))

        # Item link URL as a source (if user pasted a URL with a price)
        _item_link = item.get("item_link", "")
        _item_link_price = _safe_float(item.get("item_link_price"), 0)
        if _item_link and _item_link_price and _item_link_price not in [s[0] for s in sources]:
            _il_supplier = item.get("item_supplier", "Link")
            sources.append((_item_link_price, _il_supplier, _item_link, "#f59e0b", True, 0.99))

        # ── AI validator source chips (Phase 2 UI) ──
        # The Grok validator runs on items with confidence < 0.75 and either:
        #  (a) sets unit_cost + price_source="llm_grok" when it's confident, or
        #  (b) stores llm_suggestion_* when it has a lead but isn't confident enough
        #      to auto-apply.
        # Case (a) is usually already carried by the Amazon chip (Grok writes
        # amazon_price). Case (b) has no existing chip — surface it so the user
        # can choose to accept the AI lead instead of quoting blind.
        _llm_validated = bool(p.get("llm_validated") or p.get("price_source") == "llm_grok")
        _llm_cost = _safe_float(p.get("unit_cost"), 0) if _llm_validated else 0
        if _llm_validated and _llm_cost > 0:
            # If no source chip mirrors the Grok price yet, add a dedicated one
            if not any(abs(s[0] - _llm_cost) < 0.01 for s in sources):
                _llm_name = (p.get("llm_product_name") or "")[:24]
                _llm_label = f"AI · {_llm_name}" if _llm_name else "AI"
                _llm_conf = _safe_float(p.get("llm_confidence"), 0.75)
                sources.append((_llm_cost, _llm_label, "", "#d2a8ff", True, _llm_conf))

        # AI suggestion (low-confidence — stored but not auto-applied to cost)
        _llm_sugg_price = _safe_float(p.get("llm_suggestion_price"), 0)
        if _llm_sugg_price > 0 and not any(abs(s[0] - _llm_sugg_price) < 0.01 for s in sources):
            _llm_sugg_name = (p.get("llm_suggestion") or "")[:20]
            _llm_sugg_url = p.get("llm_suggestion_url", "")
            _llm_sugg_conf = _safe_float(p.get("llm_suggestion_confidence"), 0.50)
            _sugg_label = f"AI suggest: {_llm_sugg_name}" if _llm_sugg_name else "AI suggest"
            sources.append((_llm_sugg_price, _sugg_label, _llm_sugg_url, "#d2a8ff", False, _llm_sugg_conf))

        # Sort by price, preferred suppliers get a small boost (within 10% of cheapest = preferred wins)
        if sources:
            cheapest = min(s[0] for s in sources)
            def _sort_key(s):
                price, label, url, color, preferred, conf = s
                # If preferred and within 10% of cheapest, rank it first
                if preferred and price <= cheapest * 1.10:
                    return (0, price)
                return (1, price)
            sources.sort(key=_sort_key)

        # Suppress fuzzy sources when an EXACT match exists.
        # EXACT = UPC/barcode-verified identity (conf >= 0.99 per the ladder
        # in docs/PRD_PRICING_PIPELINE.md §Stage 2). Description matches at
        # 0.96-0.98 are STRONG, not EXACT — they don't trigger suppression
        # because a 96% description match is not proof that we have the
        # identity pinned down.
        has_exact = any(s[5] >= 0.99 for s in sources)
        if has_exact:
            sources = [s for s in sources if s[5] >= 0.75]

        # Build source chips HTML with confidence text badges + reject/accept actions
        source_chips = []
        for i_src, (sprice, slabel, surl, scolor, spref, sconf) in enumerate(sources):
            pref_icon = "★ " if spref else ""
            price_fmt = f"${sprice:.2f}"
            # Derive source key for feedback API
            _sl = slabel.lower()
            if "scprs" in _sl: source_key = "scprs"
            elif "catalog" in _sl or "📦" in slabel: source_key = "catalog"
            elif "amazon" in _sl: source_key = "amazon"
            else: source_key = "web"
            # Truncate long supplier names
            slabel_short = slabel[:15] + "…" if len(slabel) > 15 else slabel
            # Confidence tier: EXACT (>=0.99 — UPC-verified only), normal
            # (0.75-0.98), ~FUZZY (0.50-0.75). The 0.99 floor reserves EXACT
            # for identifier-verified matches (UPC/barcode); a 96% description
            # match is STRONG but not proof of identity. Text labels (not
            # color-only) — user is colorblind.
            if sconf >= 0.99:
                conf_tag = ' <b style="font-size:9px;padding:1px 3px;border-radius:2px;background:#3fb95030;letter-spacing:.3px">EXACT</b>'
                border_style = f"border:2px solid {scolor}80"
            elif sconf >= 0.75:
                conf_tag = ""
                border_style = f"border:1px solid {scolor}40"
            else:
                conf_tag = ' <span style="font-size:9px;padding:1px 3px;border-radius:2px;background:#d2992230;letter-spacing:.3px">~FUZZY</span>'
                border_style = f"border:1px dashed {scolor}60"
            conf_title = f" ({sconf:.0%} match)" if sconf else ""
            # Surface Grok reasoning in the tooltip for AI-attributed chips.
            # The reasoning is short (≤200 chars, truncated by validator) and is
            # the only place in the UI where a user can see WHY the AI picked it.
            _ai_reason = ""
            if "AI" in slabel and p.get("llm_reasoning"):
                _ai_reason = " — " + str(p.get("llm_reasoning", ""))[:160].replace('"', "'")
            _chip_style = f"display:inline-flex;align-items:center;gap:2px;padding:2px 5px;border-radius:4px;font-size:12px;background:{scolor}15;{border_style};color:{scolor};white-space:nowrap"
            if surl:
                chip = f'<a href="{surl}" target="_blank" style="{_chip_style};text-decoration:none;cursor:pointer" title="{slabel} · {price_fmt}{conf_title}{_ai_reason}">{pref_icon}<b>{price_fmt}</b> {slabel_short}{conf_tag}</a>'
            else:
                chip = f'<span style="{_chip_style}" title="{slabel}{conf_title}{_ai_reason}">{pref_icon}<b>{price_fmt}</b> {slabel_short}{conf_tag}</span>'
            # Reject button for non-EXACT matches (× to dismiss bad match).
            # Non-EXACT here = anything below the UPC-verified floor (0.99).
            if sconf < 0.99 and source_key in ("scprs", "catalog", "web"):
                chip += (f'<a href="#" onclick="rejectMatch({idx},\'{source_key}\',this);return false" '
                         f'style="color:#f85149;font-size:11px;text-decoration:none;opacity:0.4;margin-left:1px" '
                         f'title="Wrong match — reject" onmouseover="this.style.opacity=1" '
                         f'onmouseout="this.style.opacity=0.4">&times;</a>')
            # First source gets "Use" action + accept signal
            if i_src == 0 and len(sources) > 1 and sprice != unit_cost:
                _use_onclick = (
                    f"document.querySelector('[name=cost_{idx}]').value='{sprice:.2f}';"
                    f"recalcRow({idx});recalcPC();"
                    f"fetch('/api/pricecheck/'+_pcid+'/accept-match/{idx}',"
                    f"{{method:'POST',headers:{{'Content-Type':'application/json'}},"
                    f"body:JSON.stringify({{match_source:'{source_key}'}})}})"
                    f";return false"
                )
                chip = f'<span style="display:inline-flex;align-items:center;gap:2px">{chip}<a href="#" onclick="{_use_onclick}" style="color:{scolor};font-size:14px;text-decoration:none;flex-shrink:0" title="Use this price as cost">⬇</a></span>'
            source_chips.append(chip)
        source_html = '<div style="display:flex;flex-wrap:wrap;gap:3px;max-width:100%">' + ''.join(source_chips) + '</div>' if source_chips else '<span style="color:#484f58;font-size:14px">No sources</span>'
        # Oracle pricing intelligence badge
        _oracle_price = _safe_float(item.get("oracle_price"), 0)
        _oracle_conf = item.get("oracle_confidence", "")
        _oracle_rationale = item.get("oracle_rationale", "")
        # Suppress stale Oracle prices that are wildly wrong (>20x cost = pre-QPU-fix artifact)
        if _oracle_price > 0 and unit_cost > 0 and _oracle_price > unit_cost * 20:
            _oracle_price = 0  # Stale — will recalculate on next Oracle Auto-Price
        if _oracle_price > 0:
            _oc_color = "#3fb950" if _oracle_conf == "high" else ("#d29922" if _oracle_conf == "medium" else "#8b949e")
            _oc_dot = "●" if _oracle_conf in ("high", "medium") else "○"
            _oc_title = f"Oracle: ${_oracle_price:.2f} ({_oracle_conf}) — {_oracle_rationale}" if _oracle_rationale else f"Oracle recommends ${_oracle_price:.2f}"
            source_html += (
                f'<div style="margin-top:3px">'
                f'<a href="#" onclick="loadOracleDetail({idx});return false" '
                f'style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;'
                f'font-size:12px;background:{_oc_color}15;border:1px solid {_oc_color}40;color:{_oc_color};'
                f'text-decoration:none;cursor:pointer" title="{_oc_title}">'
                f'{_oc_dot} Oracle <b>${_oracle_price:.2f}</b></a></div>'
            )
        # Price trend indicator
        _price_trend = p.get("price_trend", "")
        if _price_trend:
            _trend_arrow = "↗" if _price_trend == "rising" else "↘"
            _trend_color = "#f85149" if _price_trend == "rising" else "#3fb950"
            _trend_data = p.get("trend_data", {})
            _trend_title = f"Prices {_price_trend}: avg ${_trend_data.get('avg', 0):.2f} → recent ${_trend_data.get('recent_avg', 0):.2f}" if _trend_data else ""
            source_html += f'<span style="font-size:12px;color:{_trend_color};margin-left:4px" title="{_trend_title}">{_trend_arrow} {_price_trend}</span>'

        # Guardrail warning badge
        if item.get("_cost_override_reason"):
            source_html += f'<div style="font-size:11px;color:#f85149;margin-top:2px" title="{item["_cost_override_reason"]}">⚠ Cost auto-corrected</div>'

        # Per-item notes
        item_notes = item.get("notes") or ""
        notes_escaped = item_notes.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')

        # No-bid state
        no_bid = item.get("no_bid", False)
        bid_checked = "" if no_bid else "checked"
        row_opacity = "opacity:0.4" if no_bid else ""

        # ── Review tier classification (Phase 3 UX) ──
        # READY   = has cost AND best source confidence >= 0.75 — safe to quote
        # REVIEW  = has cost but best confidence 0.50-0.75 — needs a second look
        # MANUAL  = no cost OR confidence < 0.50 — human must lookup
        # SKIP    = user marked no-bid — excluded from counts/coverage
        if no_bid:
            _tier = "SKIP"
        elif not unit_cost or unit_cost <= 0:
            _tier = "MANUAL"
        else:
            _best_src_conf = max((s[5] for s in sources), default=0) if sources else 0
            if _best_src_conf >= _ready_threshold:
                _tier = "READY"
            elif _best_src_conf >= 0.50:
                _tier = "REVIEW"
            else:
                _tier = "MANUAL"
        tier_counts[_tier] = tier_counts.get(_tier, 0) + 1

        # Tier pill shown next to the line# — text first (colorblind safe),
        # colour is secondary. Hidden for SKIP to avoid visual noise on
        # skipped rows (they already get a Skip badge).
        _tier_meta = {
            "READY":  ("#3fb950", "READY"),
            "REVIEW": ("#d29922", "REVIEW"),
            "MANUAL": ("#f85149", "MANUAL"),
        }
        if _tier in _tier_meta:
            _tc, _tlabel = _tier_meta[_tier]
            _tier_pill = (
                f'<div class="row-tier row-tier-{_tier.lower()}" '
                f'data-tier="{_tier}" '
                f'style="display:block;width:fit-content;max-width:100%;'
                f'box-sizing:border-box;margin-top:3px;padding:1px 5px;'
                f'border-radius:3px;font-size:9px;font-weight:700;letter-spacing:.3px;'
                f'background:{_tc}22;color:{_tc};border:1px solid {_tc}55;'
                f'font-family:\'JetBrains Mono\',monospace;'
                f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap" '
                f'title="Review tier: {_tlabel}">{_tlabel}</div>'
            )
        else:
            _tier_pill = ""

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
        except (ValueError, TypeError) as _e:
            log.debug("suppressed: %s", _e)
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

        # QTY per UOM badge (e.g., "(36/pk)" under QTY when pack size > 1)
        try:
            _qpu_int = int(qpu) if qpu else 1
        except (ValueError, TypeError):
            _qpu_int = 1
        if _qpu_int > 1:
            _UOM_LABELS = {"PK":"pack","BX":"box","BOX":"box","CS":"case","EA":"each","CT":"carton","DZ":"dozen","RL":"roll","ST":"set","PR":"pair","BG":"bag","BT":"bottle","GL":"gallon","LB":"lb"}
            _uom_raw = (item.get("uom") or "EA").upper().strip()
            _uom_short = _UOM_LABELS.get(_uom_raw, _uom_raw.lower())
            _qpu_badge = (
                f'<div style="font-size:11px;color:#a78bfa;font-weight:600;'
                f'text-align:center;margin-top:2px" '
                f'title="Pack size: {_qpu_int} per {_uom_short}">'
                f'({_qpu_int}/{_uom_short})</div>'
            )
        else:
            _qpu_badge = ""

        # UNSPSC + Country of Origin badge
        _unspsc = item.get("unspsc_code", "")
        _unspsc_desc = item.get("unspsc_description", "")
        _coo = item.get("country_of_origin", "")
        _taa = item.get("taa_compliant", -1)
        _intel_badges = ""
        # Inline catalog chips: at-a-glance proof of what enrichment exists.
        # Each chip = one stable signal the catalog has captured for this item.
        # Operators can spot a thin row (no chips) immediately and request a re-enrich.
        _cat_chip_specs = [
            ("📷", "Image stored", bool(item.get("photo_url"))),
            ("🔗", "Supplier URL", bool(item.get("item_link") or p.get("web_url") or p.get("amazon_url"))),
            ("📦", "UPC captured", bool(item.get("upc") or p.get("upc"))),
            ("🆔", "ASIN captured", bool(asin or item.get("asin"))),
            ("🏷️", "MFG# present", bool((item.get("mfg_number") or "").strip())),
        ]
        _cat_chips = "".join(
            f'<span style="padding:1px 5px;border-radius:3px;background:rgba(63,185,80,.10);'
            f'font-size:11px;color:#3fb950" title="{_label}">{_icon}</span>'
            for _icon, _label, _have in _cat_chip_specs if _have
        )
        if _unspsc or _coo or _cat_chips:
            _parts = []
            if _cat_chips:
                _parts.append(_cat_chips)
            if _unspsc:
                _tip = f' title="{_unspsc_desc}"' if _unspsc_desc else ""
                _parts.append(f'<span style="padding:1px 5px;border-radius:3px;background:rgba(139,148,158,.12);font-size:11px;color:#8b949e"{_tip}>{_unspsc}</span>')
            if _coo:
                _taa_color = "#3fb950" if _taa == 1 else ("#f87171" if _taa == 0 else "#8b949e")
                _taa_label = "TAA" if _taa == 1 else ("TAA Risk" if _taa == 0 else "")
                _parts.append(f'<span style="padding:1px 5px;border-radius:3px;background:rgba(139,148,158,.12);font-size:11px;color:{_taa_color}">{_coo}{" · " + _taa_label if _taa_label else ""}</span>')
            _intel_badges = f'<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:2px">{"".join(_parts)}</div>'

        _disc_attr = f' data-discount-cost="{discount_cost:.2f}"' if discount_cost > 0 else ''
        items_html += f"""<tr style="{row_opacity}" data-row="{idx}"{_disc_attr}>
         <td style="text-align:center;position:relative;overflow:visible"><input type="checkbox" name="bid_{idx}" {bid_checked} style="display:none"><input type="checkbox" name="substitute_{idx}" {sub_checked} style="display:none"><input type="text" name="linenum_{idx}" value="{line_num}" style="width:32px;text-align:center;font-weight:700;font-size:15px;color:#8b949e;font-family:'JetBrains Mono',monospace;background:transparent;border:1px solid transparent;border-radius:4px;padding:2px" title="Line #"><details class="row-actions" onclick="event.stopPropagation()"><summary class="row-actions-btn" title="Row actions">&#8942;</summary><div class="row-actions-menu"><button type="button" class="skip-toggle-btn{' skip-active' if no_bid else ''}" onclick="toggleSkip({idx});this.closest('details').open=false">{'&#10003; Skipped' if no_bid else '&#10060; Skip Item'}</button><button type="button" class="sub-toggle-btn{' active-item' if sub_checked else ''}" onclick="toggleSubstitute({idx});this.closest('details').open=false">{'&#10003; Substitute' if sub_checked else '&#8644; Substitute'}</button><button type="button" onclick="toggleRowNotes({idx});this.closest('details').open=false">&#128221; Notes</button>{'<button type=&quot;button&quot; onclick=&quot;mergeUp('+str(idx)+');this.closest(&#39;details&#39;).open=false&quot;>&#11014; Merge Up</button>' if idx > 0 else ''}<button type=&quot;button&quot; onclick=&quot;findBetterPricing('+str(idx)+');this.closest(&#39;details&#39;).open=false&quot;>&#128269; Find Better Pricing</button><button type=&quot;button&quot; onclick=&quot;competitorScan('+str(idx)+');this.closest(&#39;details&#39;).open=false&quot;>&#128202; Scan Competitors</button></div></details>{'<div class=&quot;row-badge row-badge-skip&quot;>Skip</div>' if no_bid else ''}{'<div class=&quot;row-badge row-badge-sub&quot;>Sub</div>' if sub_checked else ''}{_tier_pill}</td>
         <td><input type="text" name="itemnum_{idx}" value="{mfg_display}" class="text-in" style="width:100%;box-sizing:border-box;text-align:center;font-weight:600;font-size:13px;font-family:'JetBrains Mono',monospace;padding:5px 3px" placeholder="MFG#" onblur="handleMfgInput({idx}, this)"></td>
         <td><input type="number" name="qty_{idx}" value="{qty}" class="num-in sm" style="width:48px" onchange="recalcPC()"><input type="hidden" name="qpu_{idx}" value="{qpu}">{'<input type="hidden" name="saleprice_'+str(idx)+'" value="'+str(_sale_price)+'">' if _sale_price > 0 else ''}{'<input type="hidden" name="listprice_'+str(idx)+'" value="'+str(_list_price_val)+'">' if _list_price_val > 0 else ''}{'<input type="hidden" name="photo_url_'+str(idx)+'" value="'+str(item.get("photo_url",""))+'">' if item.get("photo_url") else ''}{_qpu_badge}</td>
         <td><input type="text" name="uom_{idx}" value="{(item.get('uom') or 'EA').upper()}" class="text-in" style="width:45px;text-transform:uppercase;text-align:center;font-weight:600"></td>
         <td style="position:relative"><textarea name="desc_{idx}" class="text-in desc-area" style="width:100%;font-size:13px;padding:6px 8px;resize:none;min-height:28px;height:28px;line-height:1.4;overflow:hidden;transition:height 0.15s;box-sizing:border-box" title="{raw_desc.replace('"','&quot;').replace('<','&lt;')}" onclick="expandDesc(this)" onblur="collapseDesc(this)" oninput="detectDescUrl({idx},this)" placeholder="Enter description or paste URL">{display_desc.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')}</textarea><button type="button" class="desc-expand-btn" onclick="toggleDescFull(this.previousElementSibling)" title="Expand description" style="position:absolute;bottom:2px;right:4px;background:rgba(88,166,255,.15);border:1px solid rgba(88,166,255,.25);color:#58a6ff;font-size:11px;padding:1px 5px;border-radius:3px;cursor:pointer;opacity:0;transition:opacity 0.15s;line-height:1.4">⤢</button><button type="button" class="amazon-match-btn" onclick="matchAmazon({idx})" title="Search Amazon for exact product match">🔍 Amazon</button><button type="button" class="item-history-btn" onclick="openItemHistory({idx})" title="Show prior bids for this buyer × item (oracle history)" style="margin-left:4px;background:rgba(167,139,250,.15);border:1px solid rgba(167,139,250,.35);color:#a78bfa;font-size:11px;padding:1px 6px;border-radius:3px;cursor:pointer">📊 Hist</button><span id="amz_status_{idx}" style="display:none;font-size:11px;margin-left:4px"></span>{_intel_badges}</td>
         <td>
          <div style="display:flex;flex-direction:column;gap:3px">
           <div style="display:flex;gap:2px;align-items:center">
            <input type="text" name="link_{idx}" value="{item_link.replace(chr(34), '&quot;')}" placeholder="Paste URL…" class="text-in" style="flex:1;min-width:0;font-size:13px;color:#58a6ff;padding:4px 6px" oninput="handleLinkInput({idx}, this)" onpaste="setTimeout(()=>handleLinkInput({idx},this),50)">
            <a href="{item_link}" target="_blank" id="linkopen_{idx}" onclick="return !!this.href && this.href!==''" style="display:{'flex' if item_link else 'none'};align-items:center;justify-content:center;width:28px;height:28px;border-radius:4px;background:#21262d;border:1px solid #30363d;color:#58a6ff;font-size:14px;text-decoration:none;flex-shrink:0" title="Open link">↗</a>
           </div>
           <div id="link_meta_{idx}" style="font-size:13px;color:#8b949e">{supplier_badge}{ph_link}</div>
          </div>
         </td>
         <td style="vertical-align:top;padding:6px 4px;overflow:hidden">{source_html}</td>
         <td><div class="currency-wrap"><input type="text" inputmode="decimal" name="cost_{idx}" value="{cost_str}" class="num-in {'cost-needs-lookup' if (not cost_str and not no_bid) else ''}" placeholder="0.00" oninput="sanitizePrice(this)" onchange="(window.handleManualCostChange||function(){{recalcRow({idx},true);}})({idx})" onblur="fmtCurrency(this)" data-cost-source="{(item.get('pricing', dict()).get('cost_source') or '')}"></div>{("<div class='cost-needs-chip' style=\"margin-top:3px;padding:2px 6px;border-radius:3px;font-size:10px;font-weight:700;background:#f8514922;color:#f85149;border:1px solid #f8514955;display:inline-block\" title=\"Cost not auto-filled — Amazon/SCPRS prices are reference only. Enter the supplier cost manually.\">⚠️ NEEDS COST</div>") if (not cost_str and not no_bid) else ""}</td>
         <td style="white-space:nowrap"><div style="display:flex;align-items:center;gap:2px"><input type="text" inputmode="numeric" name="markup_{idx}" value="{markup_pct}" class="num-in sm" style="width:52px" oninput="sanitizeInt(this)" onchange="recalcRow({idx},true)"><span style="color:#8b949e;font-size:13px">%</span></div></td>
         <td><div class="currency-wrap"><input type="text" inputmode="decimal" name="price_{idx}" value="{final_str}" class="num-in price-out" placeholder="0.00" oninput="sanitizePrice(this)" onchange="recalcPC()" onblur="fmtCurrency(this)"></div></td>
         <td class="ext" style="font-weight:600;font-size:14px">{ext}</td>
         <td class="profit" style="font-size:14px">{profit_str}</td>
        </tr>
        <tr class="notes-row" data-row="{idx}" style="display:{'table-row' if item_notes else 'none'}">
         <td colspan="12" style="padding:0 8px 6px 80px;border-top:none">
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

    # Diagnostic endpoint still available at /pricecheck/<id>/diagnose for admin debug

    # 45-day expiry from TODAY (not upload date)
    try:
        expiry = datetime.now() + timedelta(days=45)
        expiry_date = expiry.strftime("%m/%d/%Y")
    except Exception as e:
        log.debug("Suppressed: %s", e)
        expiry_date = (datetime.now() + timedelta(days=45)).strftime("%m/%d/%Y")
    today_date = datetime.now().strftime("%m/%d/%Y")

    # Delivery dropdown state. Default is 7-14 business days — safe
    # window for Amazon/Grainger/backorder scenarios. User can drop to
    # 3-5 or 5-7 for in-stock catalog items.
    saved_delivery = pc.get("delivery_option", "7-14 business days")
    preset_options = ("3-5 business days", "5-7 business days", "7-14 business days")
    is_custom = saved_delivery not in preset_options and saved_delivery != ""
    del_sel = {opt: ("selected" if saved_delivery == opt else "") for opt in preset_options}
    del_sel["custom"] = "selected" if is_custom else ""
    if not any(del_sel.values()):
        del_sel["7-14 business days"] = "selected"
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
    if institution or pc.get("requestor_email") or pc.get("ship_to"):
        # Step 1: Resolve institution via authoritative resolver FIRST
        # Pass email + ship_to for fallback when institution is garbage (e.g., "Delivery")
        _resolved = {"canonical": "", "agency": "", "facility_code": ""}
        try:
            from src.core.institution_resolver import resolve as _resolve_inst
            _resolved = _resolve_inst(
                institution,
                email=pc.get("requestor_email", ""),
                ship_to=pc.get("ship_to", ""),
            ) or _resolved
            # Self-heal: if resolver found a better institution, update the PC
            if (_resolved.get("agency") and _resolved.get("source") in ("ship_to", "email")
                    and _resolved["canonical"] != institution):
                pc["institution"] = _resolved["canonical"]
                institution = _resolved["canonical"]
                inst_upper = institution.upper()
                header["institution"] = institution
                try:
                    from src.api.dashboard import _save_single_pc
                    _save_single_pc(pcid, pc)
                    log.info("Self-healed institution: '%s' -> '%s' (via %s)",
                             _resolved.get("original", ""), institution, _resolved["source"])
                except Exception as _e:
                    log.warning("PC %s self-heal save failed: %s", pcid, _e)
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        _canonical = _resolved.get("canonical", "") or institution
        _canonical_upper = _canonical.upper()
        _resolved_agency = _resolved.get("agency", "")
        # Map resolver agency keys to display names (shared helper, covers
        # all 9 canonical keys — see src/core/agency_display.py)
        from src.core.agency_display import agency_display as _agency_display_fn
        _agency_display = _agency_display_fn(_resolved_agency)

        try:
            customers = _load_customers()
            # Exact match on raw institution name
            for c in customers:
                names = [c.get("display_name",""), c.get("company",""),
                         c.get("abbreviation",""), c.get("qb_name","")]
                if any(inst_upper == n.upper() for n in names if n):
                    crm_data = {"matched": True, "customer": c, "is_new": False}
                    break
            # Match on resolver's canonical name (e.g. "California State Prison, Sacramento")
            if not crm_data["matched"] and _canonical_upper != inst_upper:
                for c in customers:
                    c_name = c.get("display_name", "").upper()
                    if c_name and (c_name == _canonical_upper
                                   or c_name in _canonical_upper
                                   or _canonical_upper in c_name):
                        crm_data = {"matched": True, "customer": c, "is_new": False}
                        break
            # Fuzzy fallback — tokenize on whitespace AND hyphens/slashes
            if not crm_data["matched"]:
                import re as _re_crm
                q_tokens = set(_re_crm.split(r'[\s\-/]+', _canonical_upper))
                q_tokens.discard("")
                scored = []
                for c in customers:
                    search_text = " ".join([c.get("display_name",""), c.get("company",""),
                                            c.get("abbreviation","")]).upper()
                    c_tokens = set(_re_crm.split(r'[\s\-/,]+', search_text))
                    c_tokens.discard("")
                    overlap = len(q_tokens & c_tokens)
                    if overlap > 0:
                        scored.append((overlap / max(len(q_tokens), 1), c))
                scored.sort(key=lambda x: -x[0])
                candidates = [s[1] for s in scored[:5] if s[0] > 0.3]
                if candidates and scored[0][0] >= 0.6:
                    crm_data = {"matched": True, "customer": candidates[0], "is_new": False, "candidates": candidates[:3]}
                else:
                    # Auto-create CRM customer for known CA facilities
                    _agency = _agency_display or (
                        _guess_agency(institution) if callable(_guess_agency) else "CDCR")
                    if not _agency or _agency == "DEFAULT":
                        _agency = "CDCR"
                    if _resolved.get("canonical") and _resolved.get("agency"):
                        _auto_customer = {
                            "display_name": _resolved["canonical"],
                            "abbreviation": _resolved.get("facility_code", ""),
                            "agency": _agency,
                            "company": _resolved["canonical"],
                            "source": "auto_resolved",
                            "state": "CA",
                        }
                        # Persist to CRM DB so it matches next time
                        try:
                            from src.core.db import get_db as _crm_db
                            with _crm_db() as _conn:
                                _conn.execute("""
                                    INSERT OR IGNORE INTO contacts
                                    (display_name, company, abbreviation, agency, state, source, created_at)
                                    VALUES (?,?,?,?,?,?,datetime('now'))
                                """, (_resolved["canonical"], _resolved["canonical"],
                                      _resolved.get("facility_code", ""), _agency, "CA", "auto_resolved"))
                            log.info("CRM_AUTO: Created customer '%s' (%s/%s) from institution resolver",
                                     _resolved["canonical"], _resolved.get("facility_code", ""), _agency)
                        except Exception as _ce:
                            log.debug("CRM auto-create DB: %s", _ce)
                        crm_data = {"matched": True, "customer": _auto_customer, "is_new": False,
                                    "auto_created": True}
                    else:
                        crm_data = {"matched": False, "is_new": True, "candidates": candidates[:3],
                                    "suggested_agency": _agency}
        except Exception as e:
            log.debug("CRM match error: %s", e)
            # Even if _load_customers fails, try auto-create from resolver
            if _resolved.get("canonical") and _resolved.get("agency"):
                _agency = _agency_display or "CDCR"
                crm_data = {"matched": True, "auto_created": True, "is_new": False,
                            "customer": {
                                "display_name": _resolved["canonical"],
                                "abbreviation": _resolved.get("facility_code", ""),
                                "agency": _agency, "company": _resolved["canonical"],
                                "source": "auto_resolved", "state": "CA",
                            }}
    
    # ── Server-side quote history ──
    # P2-E: Normalize institution name via resolver for better history matching
    try:
        from src.core.institution_resolver import resolve
        _resolved = resolve(institution,
                            email=pc.get("requestor_email", ""),
                            ship_to=pc.get("ship_to", ""))
        if _resolved.get("canonical"):
            institution = _resolved["canonical"]
            inst_upper = institution.upper()
    except ImportError as _e:
        log.debug("suppressed: %s", _e)
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
    
    # ── Auto-fill ship-to via full priority chain ──
    # PO history → CRM contact → institution resolver. Updated
    # 2026-04-14 from the old institution-only path so buyers with
    # established PO history resolve to their real delivery address,
    # not just the agency's canonical HQ.
    if not pc.get("ship_to"):
        try:
            from src.core.ship_to_resolver import lookup_buyer_ship_to
            _inst = pc.get("institution") or header.get("institution", "")
            _buyer_name = (pc.get("requestor_name")
                           or header.get("requestor_name", ""))
            _buyer_email = (pc.get("requestor_email")
                            or header.get("requestor_email", ""))
            _resolved = lookup_buyer_ship_to(
                name=_buyer_name, email=_buyer_email, institution=_inst)
            if _resolved.get("ship_to"):
                pc["ship_to"] = _resolved["ship_to"]
                log.info("SHIP_TO auto-filled (%s) for %s: %s",
                         _resolved.get("source", "?"),
                         _buyer_name or _inst,
                         _resolved["ship_to"][:50])
        except Exception as _sta:
            log.debug("ship_to auto-fill: %s", _sta)

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
    _existing_packet_url = ""
    # Packet has its own dedicated slot (CC-2). Resolve first so we can
    # exclude it from the 704 probe below — the CC-2 setdefault may have
    # landed the packet path into `output_pdf` as a fallback, and we do
    # NOT want to mislabel it as "AMS 704" in the compose panel.
    _pk = pc.get("cchcs_packet_output_pdf") or ""
    if _pk and os.path.exists(_pk):
        _existing_packet_url = f"/api/pricecheck/download/{os.path.basename(_pk)}"
    # Check stored 704 paths. Skip any entry that matches the packet path
    # so the packet isn't double-surfaced as a 704 attachment.
    for _op_key in ("output_pdf", "original_pdf"):
        _op = pc.get(_op_key, "")
        if _op and os.path.exists(_op) and _op != _pk:
            _existing_704_url = f"/api/pricecheck/download/{os.path.basename(_op)}"
            break
    _qp = pc.get("reytech_quote_pdf") or ""
    if _qp and os.path.exists(_qp):
        _existing_quote_url = f"/api/pricecheck/download/{os.path.basename(_qp)}"
    # Fallback: scan DATA_DIR for matching PDFs by PC ID or number.
    # UI-2: CCHCS packet outputs are named `<source>_Reytech.pdf` (see
    # cchcs_packet_filler._output_path), so they also match the "Reytech"
    # branch below. Exclude the already-resolved packet basename so the
    # packet doesn't get mislabeled as "Reytech Quote" in the compose panel.
    _packet_basename = os.path.basename(_pk) if _pk else ""
    if not _existing_704_url or not _existing_quote_url:
        import re as _re_scan
        _safe_num = _re_scan.sub(r'[^a-zA-Z0-9_-]', '_', (pc.get("pc_number", "") or pcid).strip())
        try:
            for _f in os.listdir(DATA_DIR):
                if not _f.endswith(".pdf"):
                    continue
                if _packet_basename and _f == _packet_basename:
                    continue
                if pcid in _f or _safe_num in _f:
                    _dl = f"/api/pricecheck/download/{_f}"
                    if "Reytech" in _f and not _existing_quote_url:
                        _existing_quote_url = _dl
                    elif not _existing_704_url:
                        _existing_704_url = _dl
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    # SCPRS data staleness check.
    # Was: `from src.core.institution_resolver import resolve_agency` —
    # that name has never existed in institution_resolver, so the import
    # silently failed and this whole staleness banner has been a no-op
    # since whenever the call was added. Migrated to the canonical
    # facade 2026-04-25; `classify_agency(...).agency` is what was meant.
    _scprs_staleness = None
    try:
        from src.core.quote_contract import classify_agency
        _inst_text = pc.get("institution", "") or header.get("institution", "")
        _agency_key = classify_agency(_inst_text).get("agency", "")
        if _agency_key:
            from src.core.db import get_db as _sdb
            with _sdb() as _sconn:
                _srow = _sconn.execute(
                    "SELECT last_pull, pull_interval_hours FROM scprs_pull_schedule WHERE agency_key=?",
                    (_agency_key,)
                ).fetchone()
                if _srow and _srow[0]:
                    from datetime import datetime as _sdt
                    _last = _sdt.fromisoformat(_srow[0].replace("Z", "+00:00") if "Z" in _srow[0] else _srow[0])
                    _age_hrs = (datetime.now() - _last.replace(tzinfo=None)).total_seconds() / 3600
                    _interval = _srow[1] or 24
                    if _age_hrs > _interval * 1.5:
                        _scprs_staleness = {"agency": _agency_key, "hours_old": round(_age_hrs, 1), "interval": _interval}
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # ── Bundle siblings for banner ──
    _bundle_siblings = []
    if pc.get("bundle_id"):
        _all_pcs = _load_price_checks()
        for _sid, _spc in _all_pcs.items():
            if _spc.get("bundle_id") == pc["bundle_id"] and _sid != pcid:
                _bundle_siblings.append({"id": _sid, "pc_number": _spc.get("pc_number", "")})

    # ── Normalize due_date to ISO for <input type="date"> ──
    _due_date_iso = ""
    _raw_due = pc.get("due_date", "")
    if _raw_due:
        from datetime import datetime as _ddt
        for _dfmt in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                _due_date_iso = _ddt.strptime(str(_raw_due).strip(), _dfmt).strftime("%Y-%m-%d")
                break
            except (ValueError, TypeError):
                continue

    # Normalize due_time to HH:MM for <input type="time">. Empty → UI shows ⚠.
    _due_time_hhmm = ""
    _raw_time = pc.get("due_time", "")
    if _raw_time:
        from datetime import datetime as _ddt2
        for _tfmt in ("%I:%M %p", "%I:%M%p", "%H:%M", "%I %p", "%I%p"):
            try:
                _due_time_hhmm = _ddt2.strptime(str(_raw_time).strip(), _tfmt).strftime("%H:%M")
                break
            except (ValueError, TypeError):
                continue

    # Bid recurrence detection: surface prior PCs at the same institution
    # whose item set substantially matches the current one. Mike's
    # 2026-05-05 ask — government buyers re-bid the same SKUs on a cadence,
    # so a "we bid this for them before — click to see prior pricing"
    # chip is the operator-leverage payoff. Read-side only (no writes).
    _recurring_bids = []
    try:
        from src.core.bid_recurrence import find_recurring_bids
        _all_pcs_for_recurrence = _load_price_checks()
        _recurring_bids = find_recurring_bids(
            {"institution": pc.get("institution", ""), "items": items},
            _all_pcs_for_recurrence,
            record_id=pcid,
        )
    except Exception as _br_e:
        log.debug("bid_recurrence: %s", _br_e)

    html = render_page("pc_detail.html", active_page="PCs",
        pcid=pcid, pc=pc, items=items, items_html=items_html,
        tier_counts=tier_counts,
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
        existing_packet_url=_existing_packet_url,
        scprs_staleness=_scprs_staleness,
        bundle_siblings=_bundle_siblings,
        due_date_iso=_due_date_iso,
        due_time_hhmm=_due_time_hhmm,
        recurring_bids=_recurring_bids,
    )
    # Sanitize any surrogate chars that could cause UnicodeEncodeError
    return html.encode("utf-8", "replace").decode("utf-8")


@bp.route("/pricecheck/<pcid>/lookup")
@auth_required
@safe_page
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
@safe_page
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
                    # CP-2: canonical per-unit extractor.
                    from src.knowledge.won_quotes_db import scprs_per_unit
                    scprs_price = scprs_per_unit(quote)
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
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                    found += 1
            except Exception as e:
                log.error(f"SCPRS lookup error: {e}")

    _sync_pc_items(pc, items)
    _save_single_pc(pcid, pc)
    return jsonify({"ok": True, "found": found, "total": len(items)})


@bp.route("/pricecheck/<pcid>/rescan-mfg", methods=["POST"])
@auth_required
@safe_page
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
@safe_page
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
@safe_page
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
    _save_single_pc(pcid, pcs[pcid])
    log.info("RENAME PC %s → %s", pcid, new_name)
    return jsonify({"ok": True, "pc_number": new_name})


@bp.route("/pricecheck/<pcid>/diagnose")
@auth_required
@safe_page
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
@safe_page
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


def _recompute_unit_price(item: dict) -> None:
    """Re-derive unit_price from cost × (1 + markup_pct/100).

    Called after cost or markup edits so persisted `unit_price` +
    `pricing.recommended_price` stay in sync with what the UI renders
    on the fly. Without this, email previews and PDFs read the stale
    `unit_price` and ship the wrong price to the customer (incident
    2026-04-21: pc_f7ba7a6b Cortech mattress, $558.48 shipped vs
    $567.79 displayed).
    """
    p = item.get("pricing") or {}
    try:
        cost = p.get("unit_cost")
        if cost is None:
            cost = item.get("vendor_cost")
        markup = p.get("markup_pct")
        if markup is None:
            markup = item.get("markup_pct")
        if cost is None or markup is None:
            return
        cost_f = float(cost)
        markup_f = float(markup)
        if cost_f <= 0:
            return
        new_price = round(cost_f * (1 + markup_f / 100.0), 2)
        item["unit_price"] = new_price
        item["pricing"]["recommended_price"] = new_price
    except (TypeError, ValueError):
        return


def _do_save_prices(pcid):
    """Inner save handler — separated so exceptions always return JSON."""
    def _safe_float(v, default=0):
        if v is None: return default
        try: return float(v)
        except (ValueError, TypeError): return default
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
    pc["delivery_option"] = data.get("delivery_option", "7-14 business days")
    pc["custom_notes"] = data.get("custom_notes", "")
    pc["price_buffer"] = data.get("price_buffer", 0)
    pc["default_markup"] = data.get("default_markup", 25)
    if data.get("ship_to") is not None:
        pc["ship_to"] = data.get("ship_to", "")
    if data.get("due_date") is not None:
        pc["due_date"] = data.get("due_date", "")
    if data.get("due_time") is not None:
        pc["due_time"] = data.get("due_time", "")
    if data.get("requestor") is not None:
        pc["requestor"] = data.get("requestor", "")
        if "parsed" in pc and isinstance(pc["parsed"], dict):
            hdr = pc["parsed"].setdefault("header", {})
            hdr["requestor"] = data["requestor"]

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
                        # Phase 4-A (2026-04-25): stamp operator provenance on
                        # every operator-saved cost. Without this, a cost
                        # typed by the operator is indistinguishable from a
                        # legacy/Amazon-poisoned value at lookup time, and
                        # the upcoming "Refresh costs" workflow would wipe
                        # operator work. Empty/zero saves clear the source
                        # so a re-typed cell starts fresh.
                        if v and v > 0:
                            items[idx]["pricing"]["cost_source"] = "operator"
                        else:
                            items[idx]["pricing"].pop("cost_source", None)
                        _recompute_unit_price(items[idx])
                        # Implicit feedback: detect significant price override vs match
                        if v and v > 0:
                            _p = items[idx].get("pricing") or {}
                            _overrides = []
                            for _ms, _mk in [("scprs", "scprs_price"), ("catalog", "catalog_cost")]:
                                _mp = _safe_float(_p.get(_mk), 0)
                                if _mp > 0 and abs(v - _mp) / max(_mp, 0.01) > 0.40:
                                    _overrides.append((_ms, _mp, _p.get(f"{_ms}_match", "")))
                            if _overrides:
                                try:
                                    from src.core.db import get_db as _fb_db
                                    from src.knowledge.won_quotes_db import normalize_text as _fb_norm
                                    _idesc = items[idx].get("description", "")
                                    with _fb_db() as _fbc:
                                        for _os, _op, _od in _overrides:
                                            _fbc.execute(
                                                "INSERT INTO match_feedback "
                                                "(pc_id,item_index,item_description,match_source,"
                                                "match_description,feedback_type,user_price,match_price,"
                                                "normalized_query,normalized_match) "
                                                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                                                (pcid, idx, _idesc[:200], _os, _od[:200],
                                                 "override", v, _op,
                                                 _fb_norm(_idesc), _fb_norm(_od))
                                            )
                                        _fbc.commit()
                                except Exception as _e:
                                    log.debug("suppressed: %s", _e)
                    elif field_type == "markup":
                        v, _err = _validate_item_field("markup", val)
                        items[idx]["pricing"]["markup_pct"] = v
                        items[idx]["markup_pct"] = v
                        _recompute_unit_price(items[idx])
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
                elif field_type == "saleprice":
                    v = _safe_float(val, 0)
                    if v > 0:
                        items[idx]["sale_price"] = v
                        if not items[idx].get("pricing"):
                            items[idx]["pricing"] = {}
                        items[idx]["pricing"]["sale_price"] = v
                elif field_type == "listprice":
                    v = _safe_float(val, 0)
                    if v > 0:
                        items[idx]["list_price"] = v
                        if not items[idx].get("pricing"):
                            items[idx]["pricing"] = {}
                        items[idx]["pricing"]["list_price"] = v
                elif field_type == "qpu":
                    try:
                        items[idx]["qty_per_uom"] = int(float(val)) if val else 1
                    except (ValueError, TypeError):
                        items[idx]["qty_per_uom"] = 1
                elif field_type == "photo_url":
                    _pv = str(val).strip() if val else ""
                    if _pv.startswith("http"):
                        items[idx]["photo_url"] = _pv
                elif field_type == "linkopen":
                    pass  # UI-only toggle, no server-side storage needed
        except (ValueError, IndexError) as _e:
            log.debug("suppressed: %s", _e)

    _sync_pc_items(pc, items)

    # Update recommendation_audit with user's actual prices
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for i, it in enumerate(items):
                user_price = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
                if user_price and user_price > 0:
                    conn.execute("""
                        UPDATE recommendation_audit
                        SET user_price=?, delta_pct=ROUND((? - oracle_price) / NULLIF(oracle_price, 0) * 100, 1),
                            followed=CASE WHEN ABS(? - oracle_price) / NULLIF(oracle_price, 0) <= 0.05 THEN 1 ELSE 0 END,
                            updated_at=datetime('now')
                        WHERE pc_id=? AND item_index=? AND outcome='pending' AND oracle_price > 0
                    """, (user_price, user_price, user_price, pcid, i))
    except Exception as _ra_e:
        log.debug("recommendation_audit update: %s", _ra_e)

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
    total_landed_cost = 0
    costed_items = 0
    # Discount-scenario aggregates: when an item has a sale price < MSRP, the
    # MSRP is the conservative cost basis but the discount profit shows what
    # we'd actually clear if the discount holds at PO time. Mike's directive
    # 2026-04-19: surface both numbers so we don't accidentally bid against
    # an MSRP and lose the discount upside.
    total_discount_cost = 0
    total_discount_profit = 0
    discount_items = 0
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
            # Landed cost for true margin
            _sup = it.get("item_supplier", "")
            if _sup:
                try:
                    from src.core.db import calc_landed_cost
                    _lc = calc_landed_cost(vc, qty, _sup)
                    total_landed_cost += _lc["landed_cost"] * qty
                except Exception:
                    total_landed_cost += vc * qty
            else:
                total_landed_cost += vc * qty
        # Discount scenario — only items where lookup_prices() recorded a
        # distinct sale_price. Falls back to MSRP cost if no discount.
        _pricing = it.get("pricing", {}) or {}
        _sale = _pricing.get("amazon_sale_price")
        _list = _pricing.get("amazon_list_price")
        if up and _sale and _list and _sale < _list:
            total_discount_cost += _sale * qty
            total_discount_profit += (up - _sale) * qty
            discount_items += 1
        elif up and vc:
            total_discount_cost += vc * qty
            total_discount_profit += (up - vc) * qty
    true_profit = total_revenue - total_landed_cost
    _summary = {
        "total_revenue":    round(total_revenue, 2),
        "total_cost":       round(total_cost, 2),
        "gross_profit":     round(total_profit, 2),
        "margin_pct":       round(total_profit / total_revenue * 100, 1) if total_revenue else 0,
        "total_landed_cost": round(total_landed_cost, 2),
        "true_profit":      round(true_profit, 2),
        "true_margin_pct":  round(true_profit / total_revenue * 100, 1) if total_revenue else 0,
        "costed_items":     costed_items,
        "total_items":      len([i for i in items if not i.get("no_bid")]),
        "fully_costed":     costed_items == len([i for i in items if not i.get("no_bid")]),
    }
    if discount_items > 0:
        _summary["discount_items"] = discount_items
        _summary["discount_total_cost"] = round(total_discount_cost, 2)
        _summary["discount_gross_profit"] = round(total_discount_profit, 2)
        _summary["discount_margin_pct"] = (
            round(total_discount_profit / total_revenue * 100, 1) if total_revenue else 0
        )
        _summary["discount_profit_note"] = "if discount holds for profit calculation"
    pc["profit_summary"] = _summary

    # Keep parsed.line_items in sync with items (source of truth)
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": items}
    else:
        pc["parsed"]["line_items"] = items

    # Save ONLY this PC — prevents background agents from overwriting user edits on other PCs.
    # raise_on_error=True is critical here: this is the user-facing autosave path. If the DB
    # write fails, the response MUST return ok:false so the frontend keeps the localStorage
    # backup banner up and the user knows their prices didn't land. Silent failure here is
    # how prices get lost between autosave and page reload (incident: 2026-04-16 PC session).
    from src.api.dashboard import _save_single_pc
    try:
        _save_single_pc(pcid, pc, raise_on_error=True)
    except Exception as _save_e:
        log.error("PC save-prices persistence failed for %s: %s", pcid, _save_e)
        return jsonify({
            "ok": False,
            "error": f"Prices could not be saved: {_save_e}. Your edits are in the browser backup — retry or use the Restore button.",
        }), 500

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
                _upc = str(_item.get("upc") or "")
                _cost = _item.get("vendor_cost") or _item.get("pricing", {}).get("unit_cost") or 0
                _price = _item.get("unit_price") or _item.get("pricing", {}).get("recommended_price") or 0
                _supplier = _item.get("item_supplier", "")
                _uom = _item.get("uom", "EA")
                _url = _item.get("item_link", "")
                if not _desc or (not _cost and not _price):
                    continue
                # Phase 2 Tier-2 denormalization (2026-04-25): every operator-saved
                # cost gets one row in quote_line_costs so find_recent_quote_cost
                # can fast-lookup by (mfg_number, upc) without decoding JSON.
                # Only operator-confirmed costs land here — no Amazon/SCPRS ghosts.
                if _cost > 0 and (_pn or _upc):
                    try:
                        from src.core.db import get_db
                        with get_db() as _qlc_conn:
                            _qlc_conn.execute(
                                "INSERT INTO quote_line_costs "
                                "(mfg_number, upc, description, cost, cost_source, "
                                " cost_source_url, pc_id, supplier_name) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (_pn or None, _upc or None, _desc, float(_cost),
                                 "operator", _url or "", pcid, _supplier or ""),
                            )
                            _qlc_conn.commit()
                    except Exception as _qlc_e:
                        log.debug("quote_line_costs write suppressed: %s", _qlc_e)
                cat_matches = match_item(_desc, _pn, top_n=1)
                if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                    pid = cat_matches[0]["id"]
                    if _cost > 0 and _supplier:
                        add_supplier_price(pid, _supplier, _cost, url=_url)
                    # 2026-04-24: write OPERATOR-CONFIRMED cost into product_catalog
                    # so the next quote for this MFG# auto-fills from catalog. The
                    # cost_source='operator' tag tells enrich_catalog_product to
                    # ALWAYS overwrite (not "only if lower" — that locked in $24.99
                    # Amazon ghosts for months. See project_lost_revenue_2026_04_24).
                    if _cost > 0:
                        try:
                            from src.agents.product_catalog import enrich_catalog_product
                            enrich_catalog_product(
                                pid,
                                cost=float(_cost),
                                cost_source="operator",
                                cost_source_url=_url or "",
                                cost_accepted_by_quote_id=pcid,
                            )
                        except Exception as _ec:
                            log.debug("operator-cost write-back suppressed: %s", _ec)
                    # Update URL on existing catalog entry if we have one
                    # Enrich existing catalog entry with photo, manufacturer, mfg#
                    _photo = _item.get("photo_url", "")
                    _mfg_name = _item.get("manufacturer", "")
                    _mfg_num = str(_item.get("mfg_number") or _item.get("item_number") or "")
                    if _photo or _mfg_name or _mfg_num:
                        try:
                            from src.agents.product_catalog import _get_conn
                            conn = _get_conn()
                            if _photo:
                                conn.execute(
                                    "UPDATE product_catalog SET photo_url=COALESCE(NULLIF(photo_url,''),?) WHERE id=?",
                                    (_photo, pid))
                            if _mfg_name:
                                conn.execute(
                                    "UPDATE product_catalog SET manufacturer=COALESCE(NULLIF(manufacturer,''),?) WHERE id=?",
                                    (_mfg_name, pid))
                            if _mfg_num:
                                conn.execute(
                                    "UPDATE product_catalog SET mfg_number=COALESCE(NULLIF(mfg_number,''),?) WHERE id=?",
                                    (_mfg_num, pid))
                            conn.commit(); conn.close()
                        except Exception as _e:
                            log.debug("Suppressed: %s", _e)
                    _cat_updated += 1
                else:
                    pid = add_to_catalog(
                        description=_desc, part_number=_pn,
                        cost=_cost if _cost > 0 else 0,
                        sell_price=_price if _price > 0 else 0,
                        supplier_name=_supplier, uom=_uom,
                        supplier_url=_url,
                        photo_url=_item.get("photo_url", ""),
                        manufacturer=_item.get("manufacturer", ""),
                        mfg_number=_mfg_num,
                        source=f"pc_{pcid}",
                    )
                    if pid and _cost > 0 and _supplier:
                        add_supplier_price(pid, _supplier, _cost, url=_url)
                        _cat_added += 1
                    # 2026-04-24: stamp NEW catalog entries with operator provenance
                    # so future find_by_mfg_exact lookups treat them as trusted.
                    if pid and _cost > 0:
                        try:
                            from src.agents.product_catalog import enrich_catalog_product
                            enrich_catalog_product(
                                pid,
                                cost=float(_cost),
                                cost_source="operator",
                                cost_source_url=_url or "",
                                cost_accepted_by_quote_id=pcid,
                            )
                        except Exception as _ec2:
                            log.debug("operator-cost write-back (new entry) suppressed: %s", _ec2)
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

    # Fix-C (2026-04-24): stamp the save timestamp so the Convert
    # route can log it for race-fence observability + so the client's
    # `_flushPcAutosave()` can verify completion. Cheap and never
    # raises. Pair with `last_save_at` log line in convert_pc_to_rfq.
    pc["last_save_at"] = datetime.now().isoformat()
    pc["last_save_seq"] = (pc.get("last_save_seq") or 0) + 1
    _save_single_pc(pcid, pc)

    summary = pc.get("profit_summary", {})
    resp = {
        "ok": True,
        "profit_summary": summary,
        "last_save_at": pc["last_save_at"],
        "last_save_seq": pc["last_save_seq"],
    }
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
@safe_page
def pricecheck_reparse(pcid):
    """Re-parse a price check from its source PDF, preserving user-edited pricing."""
    # Telemetry: reparse is one of the most-used debug actions
    try:
        from src.core.utilization import record_feature_use
        record_feature_use("pc.reparse", context={"pc_id": pcid})
    except Exception as _e:
        log.debug("suppressed: %s", _e)

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
                    except Exception as _e:
                        log.debug("Suppressed: %s", _e)
                if row and row["data"]:
                    restore_dir = os.path.join(DATA_DIR, "pc_pdfs")
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

    # ── Phase 3: unified ingest shortcut (feature-flagged) ──
    # When classifier_v2 is enabled, route the re-parse through the new
    # pipeline. Re-parses an existing PC in place and stores the
    # classification so downstream UI can branch on request.shape.
    try:
        from src.core.request_classifier import classify_enabled
        if classify_enabled():
            from src.core.ingest_pipeline import process_buyer_request
            log.info("pc/%s/reparse: routing through classifier_v2", pcid)
            result = process_buyer_request(
                files=[source_pdf],
                email_body=pc.get("body_text", ""),
                email_subject=pc.get("email_subject", ""),
                email_sender=pc.get("requestor_email", ""),
                existing_record_id=pcid,
                existing_record_type="pc",
            )
            return jsonify({
                "ok": result.ok,
                "items": result.items_parsed,
                "classification": result.classification,
                "errors": result.errors,
                "warnings": result.warnings,
            })
    except Exception as _cv2_e:
        log.warning("pc/reparse classifier_v2 fallthrough: %s", _cv2_e)

    from src.forms.price_check import parse_ams704
    from src.forms.doc_converter import is_office_doc as _is_office

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

    # Re-parse from source file (PDF or office doc)
    if _is_office(source_pdf):
        try:
            from src.forms.doc_converter import extract_text as _extr_text
            from src.forms.vision_parser import parse_from_text, is_available as _vis_avail
            doc_text = _extr_text(source_pdf)
            fresh = None
            # Try AI extraction first
            if _vis_avail():
                fresh = parse_from_text(doc_text, source_path=source_pdf)
                if not fresh or not fresh.get("line_items"):
                    log.warning("REPARSE %s: AI returned no items, trying regex fallback", pcid)
                    fresh = None
            else:
                log.warning("REPARSE %s: AI unavailable, trying regex fallback", pcid)
            # Regex fallback (handles structured 704 DOCX text directly)
            if not fresh:
                from src.forms.doc_converter import parse_items_from_text
                fallback_items = parse_items_from_text(doc_text)
                if fallback_items:
                    fresh = {"line_items": fallback_items, "header": {},
                             "parse_method": "regex_fallback", "source_pdf": source_pdf}
                else:
                    fresh = {}
        except Exception as e:
            return jsonify({"ok": False, "error": f"Office doc re-parse failed: {e}"})
    else:
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
    pc["parse_quality"] = fresh.get("parse_quality", {})
    _sync_pc_items(pc, fresh["line_items"])

    # Clear stale metadata — items changed, old data doesn't apply
    pc.pop("enrichment_status", None)
    pc.pop("enrichment_summary", None)
    pc.pop("_split_hint", None)
    pc["status"] = "parsed"  # Reset to parsed so user re-runs pricing

    _save_single_pc(pcid, pc)

    _pq = fresh.get("parse_quality", {})
    log.info("REPARSE PC %s: %d items re-parsed, parse quality=%s, enrichment cleared",
             pcid, len(fresh["line_items"]), _pq.get("grade", "?"))
    return jsonify({"ok": True, "items": len(fresh["line_items"]),
                    "parse_quality": _pq,
                    "msg": f"Re-parsed {len(fresh['line_items'])} items (Parse {_pq.get('grade','?')} {_pq.get('score',0)}%) — run Find Prices to re-enrich"})


@bp.route("/api/pricecheck/<pcid>/lookup-tax-rate", methods=["POST"])
@auth_required
@safe_route
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
        # Route through the canonical resolver so this PC button gets the
        # same answer the RFQ route + quote_generator do for the same
        # address. The resolver checks the operator-verified facility
        # registry FIRST (e.g. Barstow's manually-confirmed 8.75%) and
        # only falls through to CDTFA / cache / base-rate when no
        # canonical record is set. Closes audit Y for the PC path.
        from src.core.tax_resolver import resolve_tax
        _force = bool(data.get("force_live"))
        result = resolve_tax(address, force_live=_force)
        if result and result.get("rate") is not None:
            rate_pct = round(result["rate"] * 100, 3)
            _source = result.get("source", "")
            _is_validated = bool(result.get("validated"))
            pc["tax_rate"] = rate_pct
            pc["tax_validated"] = _is_validated
            pc["tax_source"] = _source
            _save_single_pc(pcid, pc)
            warning = None
            if not _is_validated:
                warning = ("Lookup fell back to the CA base rate — CDTFA did not "
                           "confirm this rate for the ship-to address. Verify at "
                           "https://maps.cdtfa.ca.gov/ and correct manually.")
            return jsonify({
                "ok": True, "rate": rate_pct,
                "jurisdiction": result.get("jurisdiction", ""),
                "source": _source,
                "validated": _is_validated,
                "warning": warning,
            })
        return jsonify({"ok": False, "error": "Tax lookup returned no rate"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ─────────────────────────────────────────────────────────────────────
# Phase 2: Tier-cascade cost lookup (PR-A server side)
# 2026-04-25
#
# When a PC arrives with empty cost cells (Phase 1 default), the operator
# can fire this background lookup to fill what the system can find without
# having to type each row's cost. The cascade in cost_tier_lookup.lookup_tiers
# returns the FIRST hit per item: catalog → past_quote → supplier_scrape.
#
# Wire flow:
#   1. JS sends POST /api/pricecheck/<pcid>/lookup-costs (kicks daemon thread)
#   2. JS polls GET /api/pricecheck/<pcid>/lookup-costs/status every 1.5s
#   3. Per-item results stream into the status dict; UI renders an Accept
#      chip per filled cell
#   4. Operator clicks Accept → existing handleManualCostChange() path fires →
#      cost saves with cost_source='operator' → catalog flywheel closes
#
# Tier-3 (live scrape) results are NEVER auto-applied. They surface as a
# recommendation the operator must click to accept. This preserves the
# Phase 1 contract: only operator-confirmed costs become catalog truth.
# ─────────────────────────────────────────────────────────────────────

# Per-pcid status dict, keyed by pcid so concurrent tabs/PCs don't clobber.
# Status shape:
#   {pcid: {
#     "started_at": iso,
#     "completed_at": iso | None,
#     "items": {item_idx: {"status": "pending"|"hit"|"miss"|"error", ...recommendation}}
#   }}
import threading as _threading_lookup_costs
_LOOKUP_STATUS = {}
_LOOKUP_STATUS_LOCK = _threading_lookup_costs.Lock()


def _run_tier_lookup_for_pc(app_obj, pcid: str):
    """Daemon thread worker. Iterates PC items, runs the tier cascade,
    streams results into _LOOKUP_STATUS as it goes. Holds the Flask app
    context so DB calls work outside the request thread."""
    try:
        from src.agents.cost_tier_lookup import lookup_tiers
    except Exception as _e:
        log.error("lookup-costs worker import failed: %s", _e)
        with _LOOKUP_STATUS_LOCK:
            _LOOKUP_STATUS.setdefault(pcid, {})["error"] = str(_e)
            _LOOKUP_STATUS[pcid]["completed_at"] = datetime.now().isoformat()
        return

    with app_obj.app_context():
        try:
            pcs = _load_price_checks()
            pc = pcs.get(pcid)
            if not pc:
                with _LOOKUP_STATUS_LOCK:
                    _LOOKUP_STATUS[pcid]["error"] = "PC not found"
                    _LOOKUP_STATUS[pcid]["completed_at"] = datetime.now().isoformat()
                return
            items = pc.get("items", []) or []
            for idx, item in enumerate(items):
                # Skip items that already have a real cost or are no-bid
                if item.get("no_bid"):
                    with _LOOKUP_STATUS_LOCK:
                        _LOOKUP_STATUS[pcid]["items"][idx] = {"status": "skip", "reason": "no_bid"}
                    continue
                _existing_cost = (item.get("vendor_cost")
                                  or item.get("pricing", {}).get("unit_cost") or 0)
                try:
                    _existing_cost = float(_existing_cost)
                except (TypeError, ValueError):
                    _existing_cost = 0
                if _existing_cost > 0:
                    with _LOOKUP_STATUS_LOCK:
                        _LOOKUP_STATUS[pcid]["items"][idx] = {"status": "skip", "reason": "has_cost"}
                    continue
                # Mark pending
                with _LOOKUP_STATUS_LOCK:
                    _LOOKUP_STATUS[pcid]["items"][idx] = {"status": "pending"}
                # Run cascade
                try:
                    rec = lookup_tiers(item)
                except Exception as e:
                    log.warning("lookup-costs item %d: %s", idx, e)
                    with _LOOKUP_STATUS_LOCK:
                        _LOOKUP_STATUS[pcid]["items"][idx] = {"status": "error", "error": str(e)[:100]}
                    continue
                with _LOOKUP_STATUS_LOCK:
                    if rec:
                        _LOOKUP_STATUS[pcid]["items"][idx] = {"status": "hit", **rec}
                    else:
                        _LOOKUP_STATUS[pcid]["items"][idx] = {"status": "miss"}
            with _LOOKUP_STATUS_LOCK:
                _LOOKUP_STATUS[pcid]["completed_at"] = datetime.now().isoformat()
        except Exception as _e:
            log.error("lookup-costs worker crashed: %s", _e)
            with _LOOKUP_STATUS_LOCK:
                _LOOKUP_STATUS[pcid]["error"] = str(_e)
                _LOOKUP_STATUS[pcid]["completed_at"] = datetime.now().isoformat()


@bp.route("/api/pricecheck/<pcid>/lookup-costs", methods=["POST"])
@auth_required
@safe_route
def api_pc_lookup_costs(pcid):
    """Kick off background tier-cascade cost lookup for empty cells in this PC.
    Returns immediately with `started_at`. Poll
    /api/pricecheck/<pcid>/lookup-costs/status for results."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404

    started = datetime.now().isoformat()
    with _LOOKUP_STATUS_LOCK:
        # Reset any prior run for this pcid
        _LOOKUP_STATUS[pcid] = {
            "started_at": started,
            "completed_at": None,
            "items": {},
        }

    from flask import current_app
    app_obj = current_app._get_current_object()
    t = _threading_lookup_costs.Thread(
        target=_run_tier_lookup_for_pc,
        args=(app_obj, pcid),
        daemon=True,
        name=f"lookup-costs-{pcid}",
    )
    t.start()
    return jsonify({"ok": True, "started_at": started})


@bp.route("/api/pricecheck/<pcid>/lookup-costs/status", methods=["GET"])
@auth_required
@safe_route
def api_pc_lookup_costs_status(pcid):
    """Return the current state of the background tier-cascade for this PC.
    Returns {ok, started_at, completed_at, items: {idx: {status, ...}}}.
    items not yet processed are absent from the dict."""
    with _LOOKUP_STATUS_LOCK:
        state = _LOOKUP_STATUS.get(pcid)
        if not state:
            return jsonify({"ok": True, "started_at": None, "completed_at": None, "items": {}})
        # Snapshot under the lock
        snap = {
            "ok": True,
            "started_at": state.get("started_at"),
            "completed_at": state.get("completed_at"),
            "items": dict(state.get("items", {})),
        }
        if state.get("error"):
            snap["error"] = state["error"]
    return jsonify(snap)


@bp.route("/pricecheck/<pcid>/upload-pdf", methods=["POST"])
@auth_required
@safe_page
@rate_limit("heavy")
def pricecheck_upload_pdf(pcid):
    """Upload a PDF to a PC and parse it. Use when source PDF is lost after deploy."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file"}), 400
    from src.forms.doc_converter import is_office_doc
    fname_lower = (f.filename or "").lower()
    is_pdf = fname_lower.endswith('.pdf')
    is_office = is_office_doc(fname_lower)
    if not is_pdf and not is_office:
        return jsonify({"ok": False, "error": "Upload a PDF or office document (XLS, XLSX, DOC, DOCX)"}), 400

    # Read and check size
    content = f.read()
    if len(content) > 10 * 1024 * 1024:
        return jsonify({"ok": False, "error": "File too large (10MB max)"}), 413
    if is_pdf and not content[:5].startswith(b'%PDF'):
        return jsonify({"ok": False, "error": "Invalid PDF file"}), 400
    # Reset stream for downstream use
    from io import BytesIO
    f.stream = BytesIO(content)
    f.seek(0)

    # Save to disk
    upload_dir = os.path.join(DATA_DIR, "pc_pdfs")
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = re.sub(r'[^a-zA-Z0-9_.\-]', '_', f.filename)
    save_path = os.path.join(upload_dir, f"{pcid}_{safe_name}")
    f.save(save_path)
    pc["source_pdf"] = save_path

    # Also save to DB for persistence across deploys
    try:
        from src.api.dashboard import save_rfq_file
        with open(save_path, "rb") as _pf:
            file_data = _pf.read()
        _mime = "application/pdf" if is_pdf else "application/octet-stream"
        save_rfq_file(pcid, safe_name, _mime, file_data,
                      category="source", uploaded_by="user")
        log.info("PC %s: saved uploaded file to DB (%d bytes)", pcid, len(file_data))
    except Exception as _e:
        log.warning("PC %s: DB save failed: %s", pcid, _e)

    # Parse — PDF uses AMS 704 parser, office docs use AI text extraction
    _parse_error = None
    if is_pdf:
        from src.forms.price_check import parse_ams704
        result = parse_ams704(save_path)
    else:
        try:
            from src.forms.doc_converter import extract_text as _extr_text
            doc_text = _extr_text(save_path)
            log.info("PC %s: extracted %d chars from office doc", pcid, len(doc_text))
            # Try AI extraction first
            from src.forms.vision_parser import parse_from_text, is_available as _vis_avail
            result = None
            if _vis_avail():
                result = parse_from_text(doc_text, source_path=save_path)
                if not result or not result.get("line_items"):
                    log.warning("PC %s: AI extraction returned no items, trying regex fallback", pcid)
                    _parse_error = "AI extraction returned no items"
                    result = None
            else:
                log.warning("PC %s: AI unavailable, trying regex fallback", pcid)
                _parse_error = "AI unavailable"
            # Regex fallback: parse simple item lists (description + qty lines)
            if not result:
                from src.forms.doc_converter import parse_items_from_text
                fallback_items = parse_items_from_text(doc_text)
                if fallback_items:
                    result = {"line_items": fallback_items, "header": {},
                              "parse_method": "regex_fallback", "source_pdf": save_path}
                    _parse_error = None
                else:
                    result = {"line_items": [], "header": {}}
        except ValueError as ve:
            return jsonify({"ok": False, "error": str(ve)}), 400
        except Exception as e:
            log.error("PC %s: office doc parse failed: %s", pcid, e, exc_info=True)
            return jsonify({"ok": False, "error": f"Office doc parse error: {e}"}), 500
    pdf_path = save_path  # keep variable for downstream
    items = result.get("line_items", [])
    header = result.get("header", {})

    if items:
        pc["items"] = items
        pc["parsed"] = result
        pc["parse_quality"] = result.get("parse_quality", {})
        # Overwrite ALL header fields from the uploaded doc — user is explicitly
        # re-uploading to replace what's there
        for hk, hv in header.items():
            if hv:
                pc[hk] = hv
        if header.get("requestor"):
            pc["requestor"] = header["requestor"]
        if header.get("institution"):
            pc["institution"] = header["institution"]
        if header.get("ship_to") or header.get("delivery_zip"):
            pc["ship_to"] = header.get("ship_to") or header.get("delivery_zip", "")
        pc["status"] = "parsed"
        _sync_pc_items(pc, items)
        _save_single_pc(pcid, pc)
        log.info("PC %s: uploaded file parsed → %d items", pcid, len(items))
        # Auto-enrich in background thread
        try:
            from src.agents.pc_enrichment_pipeline import enrich_pc_background
            enrich_pc_background(pcid)
        except Exception as _ae:
            log.warning("PC %s: auto-enrich failed to start: %s", pcid, _ae)
        from flask import flash
        _pq = result.get("parse_quality", {})
        _pq_msg = f" (Parse {_pq.get('grade','?')} {_pq.get('score',0)}%)" if _pq else ""
        flash(f"Parsed {len(items)} items from uploaded file{_pq_msg}", "success")
    else:
        _save_single_pc(pcid, pc)
        log.warning("PC %s: uploaded file parsed 0 items (error: %s)", pcid, _parse_error)
        from flask import flash
        _err_msg = f"File uploaded but no items found"
        if _parse_error:
            _err_msg += f" ({_parse_error})"
        flash(_err_msg, "error")

    return redirect(f"/pricecheck/{pcid}")


@bp.route("/pricecheck/<pcid>/generate", methods=["POST"])
@auth_required
@safe_page
def pricecheck_generate(pcid):
    """Generate completed Price Check PDF and ingest into Won Quotes KB (POST only — writes data)."""
    try:
        return _do_generate(pcid)
    except Exception as e:
        log.error("GENERATE %s CRASHED: %s", pcid, e)
        import traceback; traceback.print_exc()
        return jsonify({"ok": False, "error": f"Server error: {e}"})


def _generate_pc_pdf(pcid):
    """Core PC PDF generation logic. Returns dict (not Flask response).
    Used by both the HTTP route wrapper and the bundle generate route.
    Returns: {"ok": True, "output_path": "...", "summary": {...}} or {"ok": False, "error": "..."}
    """
    if not PRICE_CHECK_AVAILABLE:
        return {"ok": False, "error": "price_check.py not available"}
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return {"ok": False, "error": "PC not found"}

    from src.forms.price_check import fill_ams704

    # ── Sanitize stored data before PDF generation ──
    _sanitize_pc_items(pc)

    # 2026-04-24 PRE-FINALIZE GATE: refuse generation if any non-no-bid item has
    # zero unit_cost. Without this, we'd ship a Quote PDF with $0 line items —
    # and the auto-compute step below would silently leave prices at $0 too.
    # See project_lost_revenue_2026_04_24_barstow.md for what this prevents.
    _missing_cost = []
    for _idx, _it in enumerate(pc.get("items", [])):
        if _it.get("no_bid"):
            continue
        _ic = _it.get("vendor_cost") or _it.get("pricing", {}).get("unit_cost") or 0
        try:
            _ic = float(_ic)
        except (TypeError, ValueError):
            _ic = 0
        if _ic <= 0:
            _missing_cost.append(_idx + 1)
    if _missing_cost:
        msg = (
            f"Cannot generate PC PDF — {len(_missing_cost)} item(s) have no "
            f"supplier cost: row(s) {', '.join(str(x) for x in _missing_cost[:8])}"
            f"{'...' if len(_missing_cost) > 8 else ''}. "
            f"Enter a real supplier cost in each row (Amazon/SCPRS values are "
            f"reference only, never auto-filled). Or mark the row as no-bid."
        )
        log.warning("GENERATE %s blocked: %s", pcid, msg)
        return {"ok": False, "error": msg, "missing_cost_rows": _missing_cost}

    # Phase 3 (2026-04-25): also gate on zero markup. Without this, an
    # operator could ship a quote where rows have valid cost but markup_pct=0,
    # producing a sell-at-cost line. The Phase 3 oracle markup recommendation
    # makes a non-zero markup the expected default, so a zero value at this
    # stage is almost always an oversight.
    _missing_markup = []
    for _idx, _it in enumerate(pc.get("items", [])):
        if _it.get("no_bid"):
            continue
        _mp = _it.get("markup_pct")
        if _mp is None:
            _mp = (_it.get("pricing") or {}).get("markup_pct")
        try:
            _mp = float(_mp) if _mp is not None else 0
        except (TypeError, ValueError):
            _mp = 0
        if _mp <= 0:
            _missing_markup.append(_idx + 1)
    if _missing_markup:
        msg = (
            f"Cannot generate PC PDF — {len(_missing_markup)} item(s) have "
            f"zero markup: row(s) {', '.join(str(x) for x in _missing_markup[:8])}"
            f"{'...' if len(_missing_markup) > 8 else ''}. "
            f"Enter a markup % per row, or use the suggested-markup chip above "
            f"the line items to apply a recommendation across all rows."
        )
        log.warning("GENERATE %s blocked: %s", pcid, msg)
        return {"ok": False, "error": msg, "missing_markup_rows": _missing_markup}

    # ALWAYS sync parsed.line_items from pc.items (the source of truth)
    if "parsed" not in pc:
        pc["parsed"] = {"header": {}, "line_items": []}
    pc["parsed"]["line_items"] = pc.get("items", [])

    log.info("GENERATE %s: synced %d items from pc['items'] to parsed['line_items']",
             pcid, len(pc.get("items", [])))

    # Auto-compute missing prices before PDF generation.
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
                    except Exception as _ea_e:
                        # Audit P1 #11 (2026-05-06): bumped from log.debug to
                        # log.warning. Schema drift here was masked as a
                        # generic "Source PDF not found" — operator spent 20
                        # min uploading when email_attachments table was the
                        # actual cause.
                        log.warning(
                            "PC %s: email_attachments lookup failed (likely "
                            "schema drift) — %s", pcid, _ea_e,
                        )
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
            return {"ok": False, "error": "Source PDF not found. Upload the 704 PDF (More \u2192 Upload PDF & Parse), then try again."}

    # If source is an office doc (DOCX/XLSX/etc.), convert to PDF via LibreOffice.
    # The converted PDF preserves the buyer's exact form layout. Since it's a flat PDF
    # (no form fields), fill_ams704 will use the text overlay path which dynamically
    # detects row positions via pdfplumber and draws supplier info + pricing.
    _src_ext = os.path.splitext(source_pdf)[1].lower()
    _is_docx_source = _src_ext in (".docx", ".doc", ".xlsx", ".xls")
    _src_basename = os.path.basename(source_pdf).lower()
    _is_converted_docx = (not _is_docx_source and _src_ext == ".pdf"
                          and any(x in _src_basename for x in (".docx.", ".doc.", ".xlsx.", ".xls.")))
    # For DOCX/office sources: convert to PDF via LibreOffice to preserve
    # the buyer's original form layout. The overlay uses pdfplumber to detect
    # row positions, column boundaries, supplier cells, and totals dynamically.
    if _is_docx_source:
        try:
            from src.forms.doc_converter import convert_to_pdf, can_convert_to_pdf
            if can_convert_to_pdf():
                _convert_dir = os.path.join(DATA_DIR, "pc_pdfs")
                os.makedirs(_convert_dir, exist_ok=True)
                _converted = convert_to_pdf(source_pdf, _convert_dir)
                source_pdf = _converted
                log.info("GENERATE %s: converted %s → PDF (%s)", pcid, _src_ext, os.path.basename(_converted))
            else:
                _blank_704 = os.path.join(DATA_DIR, "templates", "ams_704_blank.pdf")
                if os.path.exists(_blank_704):
                    log.warning("GENERATE %s: LibreOffice unavailable — using blank 704 template", pcid)
                    source_pdf = _blank_704
                else:
                    return {"ok": False, "error": "LibreOffice unavailable and no blank template found."}
        except Exception as _conv_e:
            log.error("GENERATE %s: DOCX→PDF conversion failed: %s", pcid, _conv_e)
            _blank_704 = os.path.join(DATA_DIR, "templates", "ams_704_blank.pdf")
            if os.path.exists(_blank_704):
                source_pdf = _blank_704
            else:
                return {"ok": False, "error": f"DOCX→PDF conversion failed: {_conv_e}"}

    # Detailed logging: what exactly will fill_ams704 receive?
    _fill_items = parsed.get("line_items", [])
    log.info("GENERATE %s: %d items going to fill_ams704 (source: %s)",
             pcid, len(_fill_items), os.path.basename(source_pdf))
    for i, it in enumerate(_fill_items):
        log.info("  \u2192 item[%d]: row_idx=%s desc='%s' qty=%s uom=%s price=%s cost=%s mfg='%s'",
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

    # ── Self-Healing Pipeline: generate → verify → repair → gate ──
    from src.forms.document_pipeline import DocumentPipeline

    pipeline = DocumentPipeline(
        source_file=source_pdf,
        parsed_data=parsed,
        output_pdf=output_path,
        tax_rate=_gen_tax,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
        keep_all_pages=_is_docx_source or _is_converted_docx,
        pc_id=pcid,
        buyer_agency=pc.get("agency", ""),
    )
    pipe_result = pipeline.execute()

    log.info("GENERATE %s: pipeline result: ok=%s, score=%d, strategy=%s, attempts=%d",
             pcid, pipe_result.ok, pipe_result.verification_score,
             pipe_result.strategy_used, len(pipe_result.attempts))

    if pipe_result.ok:
        pc["output_pdf"] = pipe_result.output_path
        pc["verification_score"] = pipe_result.verification_score
        pc["generation_strategy"] = pipe_result.strategy_used
        pc["generation_attempts"] = len(pipe_result.attempts)
        # Don't downgrade: if already sent/won, keep that status (this is a revision)
        # Surface #11+#13 fix 2026-05-04: flip to "completed" not "draft" so the
        # workflow guide pill highlights "Generated" and the action bar exposes
        # Send Quote (gated on st in ('completed','converted'). Was: "draft" →
        # mapped to "Priced" step → action bar stuck on Save & Generate forever.
        if pc.get("status") not in ("sent", "pending_award", "won", "lost", "no_response"):
            _transition_status(pc, "completed", actor="system", notes="704 PDF filled (verified 100%)")
        else:
            _transition_status(pc, pc["status"], actor="system", notes="704 PDF revised (verified 100%)")
        pc["summary"] = pipe_result.summary
        _save_single_pc(pcid, pc)

        # Run Form QA on generated 704 (additional check on top of pipeline)
        _qa_warnings = []
        try:
            from src.forms.form_qa import verify_single_form
            _qa = verify_single_form(pipe_result.output_path, "704b", pc, CONFIG)
            if not _qa["passed"]:
                log.warning("GENERATE %s: Form QA FAIL — %s", pcid, "; ".join(_qa["issues"]))
            _qa_warnings = _qa.get("warnings", [])
        except Exception as _qe:
            log.debug("GENERATE %s: Form QA skipped: %s", pcid, _qe)

        # Visual QA now runs INSIDE the pipeline verify step (V2).

        # Shadow-mode: run new fill engine in background, diff against legacy output
        try:
            from src.forms.shadow_mode import shadow_fill
            shadow_fill(pc_or_rfq_dict=pc, doc_type="pc", doc_id=pcid,
                        legacy_output_path=pipe_result.output_path)
        except Exception as _shadow_e:
            log.debug("Shadow fill setup failed: %s", _shadow_e)

        # Ingest completed prices into Won Quotes KB for future reference
        _ingest_pc_to_won_quotes(pc)

        # Catalog all line items for future matching
        _enrich_catalog_from_pc(pc)

        gen_result = {
            "ok": True,
            "output_path": pipe_result.output_path,
            "summary": pipe_result.summary,
            "verification_score": pipe_result.verification_score,
            "strategy_used": pipe_result.strategy_used,
            "strategies_tried": len(pipe_result.attempts),
        }
        if _qa_warnings:
            gen_result["qa_warnings"] = _qa_warnings
        return gen_result

    # Pipeline BLOCKED — all strategies failed
    log.error("GENERATE %s: PIPELINE BLOCKED — score=%d, strategies tried=%d",
              pcid, pipe_result.verification_score, len(pipe_result.attempts))
    return {
        "ok": False,
        "error": pipe_result.error,
        "verification_score": pipe_result.verification_score,
        "attempts": pipe_result.attempt_summaries,
        "failed_fields": pipe_result.failed_fields,
    }


def _do_generate(pcid):
    """HTTP wrapper for _generate_pc_pdf — returns Flask jsonify response."""
    result = _generate_pc_pdf(pcid)
    if result.get("ok"):
        resp = {"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(result['output_path'])}"}
        if result.get("qa_warnings"):
            resp["qa_warnings"] = result["qa_warnings"]
        try:
            if not result.get("passed", True):
                resp["qa_failed"] = True
                resp["qa_issues"] = result.get("issues", [])
        except NameError as _e:
            log.debug("suppressed: %s", _e)  # _qa not defined if QA was skipped
        return jsonify(resp)
    err_resp = {"ok": False, "error": result.get("error", "Unknown error")}
    for _ek in ("verification_score", "attempts", "failed_fields"):
        if result.get(_ek) is not None:
            err_resp[_ek] = result[_ek]
    return jsonify(err_resp)


@bp.route("/pricecheck/<pcid>/source-pdf")
@auth_required
@safe_page
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
@safe_page
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
                    except Exception as _ea_e:
                        # Audit P1 #11 (2026-05-06): bumped from log.debug to
                        # log.warning. Schema drift here was masked as a
                        # generic "Source PDF not found" — operator spent 20
                        # min uploading when email_attachments table was the
                        # actual cause.
                        log.warning(
                            "PC %s: email_attachments lookup failed (likely "
                            "schema drift) — %s", pcid, _ea_e,
                        )
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
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
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

        # Run Form QA on generated original 704
        _qa_warnings = []
        try:
            from src.forms.form_qa import verify_single_form
            _qa = verify_single_form(output_path, "704b", pc, CONFIG)
            if not _qa["passed"]:
                log.warning("GENERATE-ORIGINAL %s: Form QA FAIL — %s", pcid, "; ".join(_qa["issues"]))
            _qa_warnings = _qa.get("warnings", [])
        except Exception as _qe:
            log.debug("GENERATE-ORIGINAL %s: Form QA skipped: %s", pcid, _qe)

        # Run Visual QA — vision-based rendering check (V4)
        try:
            from src.forms.pdf_visual_qa import inspect_pdf
            _vqa = inspect_pdf(output_path, company_name="Reytech Inc.")
            if not _vqa.passed:
                for _vi in _vqa.errors:
                    log.warning("GENERATE-ORIGINAL %s: Visual QA ERROR — %s", pcid, _vi.description)
                _qa_warnings.extend([f"[Visual] {i.description}" for i in _vqa.warnings])
            elif _vqa.warnings:
                _qa_warnings.extend([f"[Visual] {i.description}" for i in _vqa.warnings])
            if _vqa.pages_inspected > 0:
                log.info("GENERATE-ORIGINAL %s: Visual QA %s (%d pages, %d issues)",
                         pcid, "PASSED" if _vqa.passed else "FAILED",
                         _vqa.pages_inspected, len(_vqa.issues))
        except Exception as _vqe:
            log.info("GENERATE-ORIGINAL %s: Visual QA skipped: %s", pcid, _vqe)

        resp = {"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(output_path)}"}
        if _qa_warnings:
            resp["qa_warnings"] = _qa_warnings
        try:
            if not _qa.get("passed", True):
                resp["qa_failed"] = True
                resp["qa_issues"] = _qa.get("issues", [])
        except NameError as _e:
            log.debug("suppressed: %s", _e)
        return jsonify(resp)

    log.error("GENERATE-ORIGINAL %s FAILED: %s", pcid, result.get("error"))
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


# NOTE: /api/pricecheck/download/<filename> is defined in routes_crm.py
# (broader search with subdirectory scan + DB fallback)


@bp.route("/pricecheck/<pcid>/generate-quote", methods=["POST"])
@auth_required
@safe_page
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
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    pc_num = pc.get("pc_number", "") or ""
    t.step("Starting", pc_number=pc_num, institution=pc.get("institution",""), items=len(pc.get("items",[])))
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip()) if pc_num.strip() else ""
    safe_name = f"{safe_name}_{pcid}" if safe_name else pcid
    output_path = os.path.join(DATA_DIR, f"Quote_{safe_name}_Reytech.pdf")

    locked_qn = pc.get("reytech_quote_number", "")
    # Allocate quote number BEFORE generating to prevent burns on repeated clicks
    if not locked_qn:
        # Ghost-data gate (parallel to PR #675's RFQ-side gate at
        # routes_rfq_gen.py:2057-2071): never burn a real counter seq
        # on a PC that hasn't passed ingest sanity — placeholder
        # pc_number, zero items, Reytech buyer.
        from src.api.dashboard import is_ready_for_pc_quote_allocation
        _ok, _reasons = is_ready_for_pc_quote_allocation(pc)
        if not _ok:
            t.fail("Quote number allocation BLOCKED — ghost data",
                   reasons=_reasons)
            return jsonify({
                "ok": False,
                "error": (
                    "Cannot allocate a Reytech quote number — "
                    + "; ".join(_reasons)
                    + ". Fix the issues on the PC detail page, then re-generate."
                ),
                "reasons": _reasons,
            })
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

        # Run Form QA on generated quote
        _qa_warnings = []
        try:
            from src.forms.form_qa import verify_single_form
            _qa = verify_single_form(output_path, "quote", pc, CONFIG)
            if not _qa["passed"]:
                log.warning("GENERATE-QUOTE %s: Form QA FAIL — %s", pcid, "; ".join(_qa["issues"]))
            _qa_warnings = _qa.get("warnings", [])
        except Exception as _qe:
            log.debug("GENERATE-QUOTE %s: Form QA skipped: %s", pcid, _qe)

        t.ok("Quote generated", quote_number=result.get("quote_number",""), total=result.get("total",0))
        resp = {
            "ok": True,
            "download": f"/api/pricecheck/download/{os.path.basename(output_path)}",
            "quote_number": result.get("quote_number"),
        }
        if _qa_warnings:
            resp["qa_warnings"] = _qa_warnings
        return jsonify(resp)
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
        ingested = 0
        for item in items:
            if item.get("no_bid"):
                continue
            pricing = item.get("pricing", {})
            # Price fallback: user-entered → oracle recommended → bid price
            price = (item.get("unit_price") or pricing.get("recommended_price")
                     or pricing.get("bid_price") or 0)
            try:
                price = float(price)
            except (ValueError, TypeError):
                price = 0
            if not price or price <= 0:
                continue
            ingest_scprs_result(
                po_number=f"PC-{pc_num}",
                item_number=item.get("item_number", ""),
                description=item.get("description", ""),
                unit_price=price,
                supplier="Reytech Inc.",
                department=institution,
                award_date=datetime.now().strftime("%Y-%m-%d"),
                source="price_check",
            )
            ingested += 1
        log.info("Ingested %d/%d items from PC #%s into Won Quotes KB", ingested, len(items), pc_num)
    except Exception as e:
        log.error(f"KB ingestion error: {e}")


@bp.route("/pricecheck/<pcid>/convert-to-quote")
@auth_required
@safe_page
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
            # P0.2: Preserve enrichment fields through conversion
            "item_link":        item.get("item_link", ""),
            "item_supplier":    item.get("item_supplier", ""),
            "notes":            item.get("notes", ""),
            "pricing":          item.get("pricing", {}),
            "sale_price":       item.get("sale_price", 0),
            "list_price":       item.get("list_price", 0),
        }
        line_items.append(li)

    rfq = {
        "id": rfq_id,
        "solicitation_number": f"PC-{pc.get('pc_number', 'RFQ')}",
        "requestor_name": header.get("requestor", pc.get("requestor", "")),
        "requestor_email": pc.get("original_sender") or pc.get("requestor_email", pc.get("requestor", "")),
        "email_message_id": pc.get("email_message_id", ""),
        "original_sender": pc.get("original_sender", ""),
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

    # Update PC status — "converted" indicates the PC has been promoted to an
    # RFQ (carries the converted_rfq_id), distinct from a fresh PC at "draft".
    # Maps to the "Generated" workflow step (pc_detail.html:565).
    _transition_status(pc, "converted", actor="system", notes="Reytech quote generated")
    pc["converted_rfq_id"] = rfq_id
    _save_single_pc(pcid, pc)

    return jsonify({"ok": True, "rfq_id": rfq_id})


# NOTE: /api/pc/<pcid>/convert-to-rfq is defined in routes_analytics.py
# (more thorough version that copies all fields, files, and PO screenshots)


@bp.route("/api/pricecheck/split-pdf", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
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

    # ── Bundle identity: link all PCs from this combined PDF ──
    bundle_id = "bnd_" + _uuid.uuid4().hex[:8] if len(sections) > 1 else ""
    non_pc_pages = sections[0].get("non_pc_pages", []) if sections else []

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
            "bundle_id": bundle_id,
            "bundle_non_pc_pages": non_pc_pages,
            "bundle_total_pcs": len(sections) if bundle_id else 0,
        }
        from src.api.dashboard import _save_single_pc
        _save_single_pc(pc_id, pc)
        # Auto-enrich in background thread
        try:
            from src.agents.pc_enrichment_pipeline import enrich_pc_background
            enrich_pc_background(pc_id)
        except Exception as _ae:
            log.warning("SPLIT-PDF %s: auto-enrich failed to start: %s", pc_id, _ae)
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
    resp = {"ok": True, "total": len(created), "pcs": created,
            "by_institution": by_institution, "source_file": safe_name}
    if bundle_id:
        resp["bundle_id"] = bundle_id
        resp["bundle_url"] = f"/pricecheck/bundle/{bundle_id}"
        log.info("SPLIT-PDF: created bundle %s with %d PCs, %d non-PC pages",
                 bundle_id, len(created), len(non_pc_pages))
    return jsonify(resp)


