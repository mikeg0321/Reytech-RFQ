# routes_pricecheck_admin.py — Auto-Enrich, Match Feedback, Email QA, Bulk Scrape, Diagnostics
# Split from routes_pricecheck.py

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
import os
import json
from datetime import datetime, timedelta, timezone


# ── Auto-Enrich Pipeline ─────────────────────────────────────────────────────


# NOTE: _auto_enrich_pc and _extract_urls_from_items moved to
# src/agents/pc_enrichment_pipeline.py (unified pipeline)


@bp.route("/api/pricechecks/bulk-reenrich", methods=["POST", "GET"])
@auth_required
@safe_route
def api_pc_bulk_reenrich():
    """Re-enrich ALL price checks with corrected SCPRS prices. Runs in background."""
    import threading
    pcs = _load_price_checks()
    pc_ids = [pcid for pcid, pc in pcs.items() if len(pc.get("items", [])) > 0]
    def _run():
        from src.agents.pc_enrichment_pipeline import enrich_pc
        for pcid in pc_ids:
            try:
                enrich_pc(pcid, force=True)
                log.info("BULK RE-ENRICH: %s complete", pcid)
            except Exception as e:
                log.warning("BULK RE-ENRICH: %s failed: %s", pcid, e)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "pcs_queued": len(pc_ids), "message": f"Re-enriching {len(pc_ids)} PCs in background"})


@bp.route("/api/pricecheck/<pcid>/retry-auto-price", methods=["POST", "GET"])
@auth_required
@safe_route
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
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

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
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

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

    # Save reparsed items if needed
    reparsed = pc.get("_reparsed", False)
    if reparsed:
        pc["items"] = items
        try:
            _save_single_pc(pcid, pc)
        except Exception as e:
            log.warning("retry-auto-price save: %s", e)

    # Run unified enrichment pipeline (force=True to re-run even if previously enriched)
    try:
        from src.agents.pc_enrichment_pipeline import enrich_pc
        enrich_pc(pcid, force=True)
        # Reload to get enrichment results
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        pc = pcs.get(pcid, pc)
        summary = pc.get("enrichment_summary", {})
        found = summary.get("catalog_matched", 0) + summary.get("scprs_matched", 0) + summary.get("oracle_priced", 0)
        return jsonify({
            "ok": True,
            "source": source,
            "items": len(items),
            "priced": found,
            "saved": True,
            "enrichment_summary": summary,
            "message": f"Enriched {found}/{len(items)} items",
        })
    except Exception as e:
        log.error("retry-auto-price enrichment failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e), "source": source, "items": len(items)})


@bp.route("/api/pricecheck/<pcid>/auto-price-status")
@auth_required
@safe_route
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


@bp.route("/api/pricecheck/<pcid>/enrichment-status")
@auth_required
@safe_route
def api_pc_enrichment_status(pcid):
    """Poll enrichment pipeline status for a PC."""
    try:
        from src.agents.pc_enrichment_pipeline import ENRICHMENT_STATUS
        live = ENRICHMENT_STATUS.get(pcid)
        if live and live.get("running"):
            return jsonify({"ok": True, **live})
        # Fall back to persisted status on the PC
        pcs = _load_price_checks()
        pc = pcs.get(pcid, {})
        return jsonify({
            "ok": True,
            "running": False,
            "status": pc.get("enrichment_status", "none"),
            "phase": pc.get("enrichment_phase", ""),
            "progress": pc.get("enrichment_progress", ""),
            "completed": pc.get("enrichment_at"),
            "summary": pc.get("enrichment_summary"),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pricecheck/<pcid>/mark-won", methods=["POST"])
@auth_required
@safe_route
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
    # ── Record winning prices to pricing intelligence (ALWAYS, not just with PO) ──
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        line_items = []
        for it in pc.get("items", []):
            if it.get("no_bid"):
                continue
            price = (it.get("unit_price") or (it.get("pricing") or {}).get("recommended_price") or 0)
            cost = (it.get("vendor_cost") or (it.get("pricing") or {}).get("unit_cost") or 0)
            if not price or not it.get("description"):
                continue
            line_items.append({
                "description": it.get("description", ""),
                "part_number": it.get("mfg_number", "") or it.get("part_number", ""),
                "sku": it.get("mfg_number", ""),
                "qty": it.get("qty", 1) or 1,
                "unit_price": float(price),
                "cost": float(cost),
                "supplier": it.get("item_supplier", "") or it.get("supplier", ""),
            })
        recorded = record_winning_prices({
            "quote_number": pc.get("reytech_quote_number", pcid),
            "po_number": data.get("po_number", ""),
            "agency": pc.get("institution") or pc.get("agency", ""),
            "institution": pc.get("institution", ""),
            "line_items": line_items,
        })
        log.info("mark-won winning_prices: recorded %s items from PC %s", recorded, pcid)
    except Exception as e:
        log.warning("mark-won winning_prices error: %s", e)
    # ── Ingest won items into Won Quotes KB ──
    try:
        _ingest_pc_to_won_quotes(pc)
    except Exception as e:
        log.warning("mark-won won_quotes ingest error: %s", e)
    # ── Calibrate Oracle from win (V5: feedback loop) ──
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        calibrate_from_outcome(
            pc.get("items", []), "won",
            agency=pc.get("institution") or pc.get("agency", ""),
        )
    except Exception as e:
        log.warning("mark-won calibration error: %s", e)
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
    # ── Auto-create order if PO number provided (P1.1) ──
    po_number = data.get("po_number", "")
    order_created = False
    if po_number:
        try:
            import uuid as _uuid
            order_id = str(_uuid.uuid4())[:12]
            order_items = []
            order_total = 0
            for it in pc.get("items", []):
                if it.get("no_bid"):
                    continue
                unit_price = it.get("unit_price") or (it.get("pricing") or {}).get("recommended_price", 0) or 0
                cost = it.get("vendor_cost") or (it.get("pricing") or {}).get("unit_cost", 0) or 0
                qty = it.get("qty", 1) or 1
                order_items.append({
                    "description": it.get("description", ""),
                    "qty": qty,
                    "unit_price": unit_price,
                    "cost": cost,
                    "mfg_number": it.get("mfg_number", ""),
                    "supplier": it.get("item_supplier", ""),
                })
                order_total += float(unit_price) * int(qty)
            from src.core.db import get_db
            with get_db() as _oconn:
                _oconn.execute("""
                    INSERT OR IGNORE INTO orders (id, quote_number, agency, institution, po_number, status, total, items, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (order_id, pc.get("reytech_quote_number", ""),
                      pc.get("institution") or pc.get("agency", ""),
                      pc.get("institution", ""),
                      po_number, "new", order_total,
                      json.dumps(order_items, default=str),
                      datetime.now().isoformat(), datetime.now().isoformat()))
            order_created = True
            log.info("AUTO_ORDER: Created order %s for PO %s from PC %s", order_id, po_number, pcid)
        except Exception as _oe:
            log.warning("Auto-order creation failed for %s: %s", pcid, _oe)

    log.info("PC %s marked WON: pc#=%s institution=%s", pcid, pc.get("pc_number"), pc.get("institution"))
    return jsonify({"ok": True, "status": "won", "order_created": order_created,
                    "message": "Pricing accepted." + (" Order created for PO " + po_number if order_created else " When official RFQ/PO arrives, create the order to generate supplier POs.")})



@bp.route("/api/pricecheck/<pcid>/mark-lost", methods=["POST"])
@auth_required
@safe_route
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
    # ── Write competitor prices to won_quotes as competitor_intel ──
    comp_price_total = float(pc.get("competitor_price", 0) or 0)
    try:
        items = pc.get("items", [])
        num_items = max(len([it for it in items if not it.get("no_bid")]), 1)
        for it in items:
            if it.get("no_bid"):
                continue
            desc = it.get("description", "")
            if not desc:
                continue
            # Use per-item competitor price if available, else prorate total
            item_comp_price = comp_price_total / num_items if comp_price_total > 0 else 0
            if item_comp_price <= 0:
                continue
            qty = it.get("qty", 1) or 1
            per_unit_comp = item_comp_price / float(qty) if qty > 1 else item_comp_price
            ingest_scprs_result(
                po_number=f"LOST-{pc.get('pc_number', pcid)}",
                item_number=it.get("item_number", ""),
                description=desc,
                unit_price=per_unit_comp,
                supplier=comp_name,
                department=pc.get("institution", ""),
                award_date=datetime.now().strftime("%Y-%m-%d"),
                source="competitor_intel",
            )
        log.info("mark-lost competitor_intel: wrote %d items to won_quotes for PC %s", num_items, pcid)
    except Exception as e:
        log.warning("mark-lost competitor_intel won_quotes error: %s", e)
    # ── Calibrate Oracle from loss ──
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        items = pc.get("items", [])
        loss_type = "price" if comp_price_total > 0 else "other"
        calibrate_from_outcome(
            items, "lost",
            agency=pc.get("institution") or pc.get("agency", ""),
            loss_reason=loss_type,
        )
    except Exception as e:
        log.warning("mark-lost calibration error: %s", e)
    return jsonify({"ok": True, "status": "lost"})


@bp.route("/api/award-monitor/run", methods=["GET", "POST"])
@auth_required
@safe_route
def api_award_monitor_run():
    """Manually trigger award check cycle."""
    try:
        from src.agents.award_monitor import run_award_check
        return jsonify({"ok": True, **run_award_check()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/award-monitor/status")
@auth_required
@safe_route
def api_award_monitor_status():
    try:
        from src.agents.award_monitor import get_monitor_status
        return jsonify({"ok": True, **get_monitor_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricing-intel/backfill-wins", methods=["POST"])
@auth_required
@safe_route
def api_backfill_wins():
    """Backfill winning prices from all PCs marked as won."""
    try:
        pcs = _load_price_checks()
        total_recorded = 0
        total_ingested = 0
        won_pcs = [pcid for pcid, pc in pcs.items()
                   if pc.get("award_status") == "won"]

        from src.knowledge.pricing_intel import record_winning_prices
        from src.core.pricing_oracle_v2 import calibrate_from_outcome

        for pcid in won_pcs:
            pc = pcs[pcid]
            # Record winning prices
            line_items = []
            for it in pc.get("items", []):
                if it.get("no_bid"):
                    continue
                price = (it.get("unit_price") or (it.get("pricing") or {}).get("recommended_price") or 0)
                cost = (it.get("vendor_cost") or (it.get("pricing") or {}).get("unit_cost") or 0)
                try:
                    price = float(price)
                    cost = float(cost)
                except (ValueError, TypeError):
                    continue
                if not price or not it.get("description"):
                    continue
                line_items.append({
                    "description": it.get("description", ""),
                    "part_number": it.get("mfg_number", "") or it.get("part_number", ""),
                    "sku": it.get("mfg_number", ""),
                    "qty": it.get("qty", 1) or 1,
                    "unit_price": price,
                    "cost": cost,
                    "supplier": it.get("item_supplier", "") or it.get("supplier", ""),
                })
            if line_items:
                recorded = record_winning_prices({
                    "quote_number": pc.get("reytech_quote_number", pcid),
                    "po_number": pc.get("competitor_po", ""),
                    "agency": pc.get("institution") or pc.get("agency", ""),
                    "institution": pc.get("institution", ""),
                    "line_items": line_items,
                })
                total_recorded += recorded

            # Ingest to Won Quotes KB
            try:
                _ingest_pc_to_won_quotes(pc)
                total_ingested += 1
            except Exception:
                pass

            # Calibrate Oracle
            try:
                calibrate_from_outcome(
                    pc.get("items", []), "won",
                    agency=pc.get("institution") or pc.get("agency", ""),
                )
            except Exception:
                pass

        log.info("Backfill wins: %d PCs, %d prices recorded, %d ingested",
                 len(won_pcs), total_recorded, total_ingested)
        return jsonify({
            "ok": True,
            "won_pcs": len(won_pcs),
            "prices_recorded": total_recorded,
            "pcs_ingested": total_ingested,
        })
    except Exception as e:
        log.error("Backfill wins error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pricing-intel/backfill-losses", methods=["POST"])
@auth_required
@safe_route
def api_backfill_losses():
    """Backfill competitor intel from all PCs marked as lost."""
    try:
        pcs = _load_price_checks()
        total_items = 0
        lost_pcs = [pcid for pcid, pc in pcs.items()
                    if pc.get("award_status") == "lost"]

        from src.core.pricing_oracle_v2 import calibrate_from_outcome

        for pcid in lost_pcs:
            pc = pcs[pcid]
            comp_name = pc.get("competitor_name", "Unknown")
            comp_price_total = float(pc.get("competitor_price", 0) or 0)
            items = pc.get("items", [])
            active_items = [it for it in items if not it.get("no_bid")]
            num_items = max(len(active_items), 1)

            for it in active_items:
                desc = it.get("description", "")
                if not desc:
                    continue
                item_comp_price = comp_price_total / num_items if comp_price_total > 0 else 0
                if item_comp_price <= 0:
                    continue
                qty = it.get("qty", 1) or 1
                per_unit_comp = item_comp_price / float(qty) if qty > 1 else item_comp_price
                try:
                    ingest_scprs_result(
                        po_number=f"LOST-{pc.get('pc_number', pcid)}",
                        item_number=it.get("item_number", ""),
                        description=desc,
                        unit_price=per_unit_comp,
                        supplier=comp_name,
                        department=pc.get("institution", ""),
                        award_date=pc.get("closed_at", datetime.now().strftime("%Y-%m-%d"))[:10],
                        source="competitor_intel",
                    )
                    total_items += 1
                except Exception:
                    pass

            # Calibrate Oracle
            try:
                loss_type = "price" if comp_price_total > 0 else "other"
                calibrate_from_outcome(
                    items, "lost",
                    agency=pc.get("institution") or pc.get("agency", ""),
                    loss_reason=loss_type,
                )
            except Exception:
                pass

        log.info("Backfill losses: %d PCs, %d competitor items ingested",
                 len(lost_pcs), total_items)
        return jsonify({
            "ok": True,
            "lost_pcs": len(lost_pcs),
            "competitor_items_ingested": total_items,
        })
    except Exception as e:
        log.error("Backfill losses error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pricing-intel/requote-triggers")
@auth_required
@safe_route
def api_requote_triggers():
    """V5 Phase 6: Scan for PCs that need re-quoting."""
    try:
        from src.core.pricing_oracle_v2 import check_requote_triggers
        triggers = check_requote_triggers()
        # Summarize by type
        by_type = {}
        for t in triggers:
            tt = t["trigger_type"]
            by_type[tt] = by_type.get(tt, 0) + 1
        return jsonify({
            "ok": True,
            "triggers": triggers,
            "total": len(triggers),
            "by_type": by_type,
        })
    except Exception as e:
        log.error("Requote triggers error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/competitors")
@auth_required
@safe_route
def api_competitors():
    try:
        from src.agents.award_monitor import get_competitor_dashboard
        return jsonify({"ok": True, **get_competitor_dashboard()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/suggestions")
@auth_required
@safe_route
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
@safe_route
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
@safe_route
def api_pricecheck_price_sweep(pcid):
    """Multi-supplier price sweep using Grok web search."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    try:
        from src.agents.product_catalog import (
            match_item, add_supplier_price, init_catalog_db
        )
        from src.agents.product_research import search_amazon

        init_catalog_db()
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
                search_results = search_amazon(query, max_results=1)
                if not search_results:
                    results.append({"idx": i, "found": False, "query": query})
                    continue

                best = search_results[0]
                if best.get("price", 0) > 0:
                    cat_matches = match_item(desc, pn, top_n=1)
                    if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.55:
                        pid = cat_matches[0]["id"]
                        add_supplier_price(
                            pid, best.get("source", "Amazon"), best["price"],
                            url=best.get("url", "")
                        )

                results.append({
                    "idx": i, "found": True,
                    "query": query,
                    "best_price": best["price"],
                    "best_source": best.get("source", "Amazon"),
                    "options": [{"title": best.get("title", "")[:80],
                                 "price": best["price"],
                                 "source": best.get("source", "Amazon"),
                                 "link": best.get("url", "")}],
                })
                found_count += 1
                import time as _t
                _t.sleep(0.5)

            except Exception as se:
                log.debug("sweep item %d error: %s", i, se)
                results.append({"idx": i, "found": False, "error": str(se)})

        return jsonify({
            "ok": True, "results": results,
            "found": found_count, "total": len(items)
        })
    except Exception as e:
        log.exception("price-sweep error")
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/web-search", methods=["POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_page
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


@bp.route("/api/admin/convert-docx/<pcid>")
@auth_required
@safe_route
def api_admin_convert_docx(pcid):
    """Convert a DOCX-sourced PC to PDF via LibreOffice and return it for measurement."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    source = pc.get("source_pdf", "")
    if not source or not os.path.exists(source):
        return jsonify({"ok": False, "error": f"Source not found: {source}"})
    ext = os.path.splitext(source)[1].lower()
    if ext not in (".docx", ".doc", ".xlsx", ".xls"):
        return jsonify({"ok": False, "error": f"Source is {ext}, not a DOCX"})
    from src.forms.doc_converter import convert_to_pdf, can_convert_to_pdf
    if not can_convert_to_pdf():
        return jsonify({"ok": False, "error": "LibreOffice not available"})
    converted = convert_to_pdf(source, os.path.join(DATA_DIR, "pc_pdfs"))
    return send_file(converted, mimetype="application/pdf",
                     download_name=f"{pcid}_converted.pdf")


@bp.route("/api/admin/cleanup", methods=["GET", "POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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


@bp.route("/api/admin/undo-mark-won/<pcid>", methods=["GET", "POST"])
@auth_required
@safe_route
def api_admin_undo_mark_won(pcid):
    """Revert a mark-won: reset PC status to sent, delete winning_prices rows, delete won_quotes rows."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    quote_num = pc.get("reytech_quote_number", pcid)
    pc_num = pc.get("pc_number", "")
    # Revert PC status
    old_status = pc.get("status", "")
    old_award = pc.get("award_status", "")
    pc["status"] = "sent"
    pc.pop("award_status", None)
    pc.pop("closed_at", None)
    pc.pop("closed_reason", None)
    pc.pop("outcome", None)
    pc.pop("outcome_date", None)
    _save_single_pc(pcid, pc)
    try:
        upsert_price_check(pcid, pc)
    except Exception:
        pass
    # Delete winning_prices rows
    wp_deleted = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute("DELETE FROM winning_prices WHERE quote_number = ?", (quote_num,))
            wp_deleted = cur.rowcount
    except Exception as e:
        log.warning("undo-mark-won winning_prices cleanup: %s", e)
    # Delete won_quotes rows from this PC
    wq_deleted = 0
    try:
        from src.knowledge.won_quotes_db import _get_db_conn
        wqconn = _get_db_conn()
        cur = wqconn.execute("DELETE FROM won_quotes WHERE po_number = ?", (f"PC-{pc_num}",))
        wq_deleted = cur.rowcount
        wqconn.commit()
        wqconn.close()
    except Exception as e:
        log.warning("undo-mark-won won_quotes cleanup: %s", e)
    log.info("UNDO_MARK_WON: %s — status %s→sent, award %s→none, wp=%d, wq=%d",
             pcid, old_status, old_award, wp_deleted, wq_deleted)
    return jsonify({
        "ok": True, "pcid": pcid,
        "status_reverted": f"{old_status}→sent",
        "winning_prices_deleted": wp_deleted,
        "won_quotes_deleted": wq_deleted,
    })


@bp.route("/api/admin/backfill-wins", methods=["GET", "POST"])
@auth_required
@safe_route
def api_admin_backfill_wins():
    """Retroactively record winning prices for all won PCs.
    Scans PCs with award_status=won and feeds them into winning_prices + won_quotes."""
    pcs = _load_price_checks()
    won_pcs = {k: v for k, v in pcs.items()
                if v.get("award_status") == "won"
                or v.get("outcome") == "won"
                or v.get("status") == "won"}

    wp_total = 0
    wq_total = 0
    errors = []

    for pcid, pc in won_pcs.items():
        # Record to winning_prices
        try:
            from src.knowledge.pricing_intel import record_winning_prices
            line_items = []
            for it in pc.get("items", []):
                if it.get("no_bid"):
                    continue
                price = (it.get("unit_price") or (it.get("pricing") or {}).get("recommended_price") or 0)
                cost = (it.get("vendor_cost") or (it.get("pricing") or {}).get("unit_cost") or 0)
                if not price or not it.get("description"):
                    continue
                line_items.append({
                    "description": it.get("description", ""),
                    "part_number": it.get("mfg_number", "") or it.get("part_number", ""),
                    "sku": it.get("mfg_number", ""),
                    "qty": it.get("qty", 1) or 1,
                    "unit_price": float(price),
                    "cost": float(cost),
                    "supplier": it.get("item_supplier", "") or it.get("supplier", ""),
                })
            recorded = record_winning_prices({
                "quote_number": pc.get("reytech_quote_number", pcid),
                "po_number": pc.get("po_number", ""),
                "agency": pc.get("institution") or pc.get("agency", ""),
                "institution": pc.get("institution", ""),
                "line_items": line_items,
            })
            wp_total += recorded
        except Exception as e:
            errors.append(f"winning_prices {pcid}: {e}")

        # Record to won_quotes
        try:
            _ingest_pc_to_won_quotes(pc)
            wq_total += len([it for it in pc.get("items", [])
                            if (it.get("pricing") or {}).get("recommended_price")])
        except Exception as e:
            errors.append(f"won_quotes {pcid}: {e}")

    log.info("BACKFILL_WINS: %d PCs, %d winning_prices rows, %d won_quotes rows, %d errors",
             len(won_pcs), wp_total, wq_total, len(errors))
    return jsonify({
        "ok": True,
        "won_pcs_found": len(won_pcs),
        "winning_prices_recorded": wp_total,
        "won_quotes_ingested": wq_total,
        "errors": errors[:20],
    })


@bp.route("/api/admin/backfill-contacts", methods=["GET", "POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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

@bp.route("/api/pricecheck/<pcid>/oracle/<int:item_idx>")
@auth_required
@safe_route
def api_pc_item_oracle(pcid, item_idx):
    """Get full oracle analysis for a single PC item (on-demand)."""
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})
        items = pc.get("items", [])
        if item_idx < 0 or item_idx >= len(items):
            return jsonify({"ok": False, "error": "Item not found"})
        item = items[item_idx]
        desc = item.get("description", "")
        cost = item.get("vendor_cost") or item.get("pricing", {}).get("unit_cost") or 0
        item_num = item.get("mfg_number", "") or item.get("item_number", "")
        qty = item.get("qty", 1) or 1
        qpu = item.get("qty_per_uom", 1) or 1
        from src.core.pricing_oracle_v2 import get_pricing
        result = get_pricing(
            description=desc, quantity=qty, cost=cost if cost > 0 else None,
            item_number=item_num, qty_per_uom=qpu
        )
        result["ok"] = True
        result["item_idx"] = item_idx
        return jsonify(result)
    except Exception as e:
        log.error("Oracle lookup for %s item %d: %s", pcid, item_idx, e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/oracle/weekly-report", methods=["POST"])
@auth_required
@safe_route
def api_oracle_weekly_report():
    """Manually trigger Oracle V3 weekly report."""
    from src.agents.oracle_weekly_report import run_weekly_report
    result = run_weekly_report()
    return jsonify(result)


# ═══ Match Feedback / Rejection ═════════════════════════════════════════════

@bp.route("/api/pricecheck/<pcid>/reject-match/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def api_pc_reject_match(pcid, idx):
    """Reject a source match for an item. Stores in match_feedback blocklist."""
    data = request.get_json(force=True, silent=True) or {}
    match_source = data.get("match_source", "")
    match_desc = data.get("match_description", "")
    reason = data.get("reason", "")
    if not match_source:
        return jsonify({"ok": False, "error": "match_source required"})

    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items") or []
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Item not found"})

    item = items[idx]
    p = item.get("pricing") or {}
    item_desc = item.get("description", "")

    # Normalize for blocklist lookup
    from src.knowledge.won_quotes_db import normalize_text
    norm_query = normalize_text(item_desc)
    norm_match = normalize_text(match_desc)

    # Determine match price and ID
    match_price = 0
    match_id = ""
    match_conf = 0
    if match_source == "scprs":
        match_price = float(p.get("scprs_price") or 0)
        match_id = p.get("scprs_po", "")
        match_conf = float(p.get("scprs_confidence") or 0)
        if not match_desc:
            match_desc = p.get("scprs_match", "")
            norm_match = normalize_text(match_desc)
    elif match_source == "catalog":
        match_price = float(p.get("catalog_cost") or 0)
        match_id = str(p.get("catalog_product_id") or "")
        match_conf = float(p.get("catalog_confidence") or 0)
        if not match_desc:
            match_desc = p.get("catalog_match", "")
            norm_match = normalize_text(match_desc)

    # Insert feedback record
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO match_feedback "
                "(pc_id, item_index, item_description, match_source, match_id, "
                "match_description, match_confidence, feedback_type, match_price, "
                "reason, normalized_query, normalized_match) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (pcid, idx, item_desc[:200], match_source, match_id,
                 match_desc[:200], match_conf, "reject", match_price,
                 reason[:200], norm_query, norm_match)
            )
            conn.commit()
    except Exception as e:
        log.error("match_feedback insert: %s", e)

    # Clear rejected source data from item pricing
    if match_source == "scprs":
        for k in ("scprs_price", "scprs_confidence", "scprs_match", "scprs_source",
                   "scprs_po", "scprs_line_total", "scprs_qty"):
            p.pop(k, None)
    elif match_source == "catalog":
        for k in ("catalog_match", "catalog_confidence", "catalog_cost",
                   "catalog_product_id", "catalog_recommended", "catalog_url",
                   "catalog_best_supplier"):
            p.pop(k, None)
    elif match_source == "web":
        for k in ("web_price", "web_source", "web_url"):
            p.pop(k, None)

    # Save PC
    try:
        _save_single_pc(pcid, pc)
    except Exception as e:
        log.error("reject-match save: %s", e)

    log.info("MATCH REJECTED: pc=%s item=%d source=%s match='%s' conf=%.2f",
             pcid, idx, match_source, match_desc[:40], match_conf)
    return jsonify({"ok": True, "cleared": match_source})


@bp.route("/api/pricecheck/<pcid>/accept-match/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def api_pc_accept_match(pcid, idx):
    """Record that a source match was accepted (user clicked Use)."""
    data = request.get_json(force=True, silent=True) or {}
    match_source = data.get("match_source", "")
    match_id = data.get("match_id", "")

    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items") or []
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Item not found"})

    item = items[idx]
    p = item.get("pricing") or {}
    item_desc = item.get("description", "")

    from src.knowledge.won_quotes_db import normalize_text
    norm_query = normalize_text(item_desc)

    match_desc = ""
    match_price = 0
    match_conf = 0
    if match_source == "scprs":
        match_desc = p.get("scprs_match", "")
        match_price = float(p.get("scprs_price") or 0)
        match_conf = float(p.get("scprs_confidence") or 0)
    elif match_source == "catalog":
        match_desc = p.get("catalog_match", "")
        match_price = float(p.get("catalog_cost") or 0)
        match_conf = float(p.get("catalog_confidence") or 0)

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO match_feedback "
                "(pc_id, item_index, item_description, match_source, match_id, "
                "match_description, match_confidence, feedback_type, match_price, "
                "normalized_query, normalized_match) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (pcid, idx, item_desc[:200], match_source, match_id,
                 match_desc[:200], match_conf, "accept", match_price,
                 norm_query, normalize_text(match_desc))
            )
            # Strengthen item_mappings if catalog match
            if match_source == "catalog" and match_desc:
                conn.execute(
                    "UPDATE item_mappings SET confirmed=1, times_confirmed=times_confirmed+1, "
                    "last_confirmed=datetime('now') "
                    "WHERE original_description=?",
                    (item_desc[:200],)
                )
            conn.commit()
    except Exception as e:
        log.error("match_feedback accept insert: %s", e)

    return jsonify({"ok": True})


@bp.route("/api/pricecheck/<pcid>/qa", methods=["POST", "GET"])
@auth_required
@safe_route
def api_pc_qa(pcid):
    """Run QA/QC audit on a Price Check. Returns structured report."""
    import copy as _copy
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    use_llm = request.args.get("llm", "1") != "0"
    try:
        from src.agents.pc_qa_agent import run_qa
        report = run_qa(_copy.deepcopy(pc), use_llm=use_llm)
        # Log structured QA lifecycle event for effectiveness tracking
        try:
            from src.core.dal import log_lifecycle_event as _lle_pcqa
            _pc_cats = {}
            for _iss in report.get("issues", []):
                _c = _iss.get("category", "unknown")
                _pc_cats[_c] = _pc_cats.get(_c, 0) + 1
            _lle_pcqa("pricecheck", pcid, "pc_qa_completed",
                      f"PC QA {'PASSED' if report.get('pass') else 'FAILED'}: "
                      f"score {report.get('score', 0)}/100",
                      actor="user", detail={
                          "passed": report.get("pass", False),
                          "score": report.get("score", 0),
                          "blocker_count": len([i for i in report.get("issues", []) if i.get("severity") == "blocker"]),
                          "warning_count": len([i for i in report.get("issues", []) if i.get("severity") == "warning"]),
                          "info_count": len([i for i in report.get("issues", []) if i.get("severity") == "info"]),
                          "categories": _pc_cats,
                          "used_llm": use_llm,
                      })
        except Exception:
            pass
        return jsonify(report)
    except Exception as e:
        log.error("QA error for %s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/grok/test")
@auth_required
@safe_route
def api_grok_test():
    """Quick test: validate a product via Grok API."""
    desc = request.args.get("desc", "S&S Worldwide Mini Velvet Art Posters - 840614150049")
    upc = request.args.get("upc", "840614150049")
    try:
        from src.agents.product_validator import validate_product
        result = validate_product(description=desc, upc=upc, qty=4, uom="PK", qty_per_uom=100)
        return jsonify(result)
    except Exception as e:
        log.error("Grok test error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/match-feedback/stats")
@auth_required
@safe_route
def api_match_feedback_stats():
    """Return aggregate match feedback stats."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            total_rej = conn.execute(
                "SELECT COUNT(*) FROM match_feedback WHERE feedback_type='reject'"
            ).fetchone()[0]
            total_acc = conn.execute(
                "SELECT COUNT(*) FROM match_feedback WHERE feedback_type='accept'"
            ).fetchone()[0]
            by_source = {}
            for row in conn.execute(
                "SELECT match_source, COUNT(*) FROM match_feedback "
                "WHERE feedback_type='reject' GROUP BY match_source"
            ).fetchall():
                by_source[row[0]] = row[1]
            top_rejected = []
            for row in conn.execute(
                "SELECT normalized_query, normalized_match, COUNT(*) as c "
                "FROM match_feedback WHERE feedback_type='reject' "
                "GROUP BY normalized_query, normalized_match ORDER BY c DESC LIMIT 10"
            ).fetchall():
                top_rejected.append({"query": row[0], "match": row[1], "count": row[2]})
        total = total_rej + total_acc
        return jsonify({
            "ok": True,
            "total_rejections": total_rej,
            "total_accepts": total_acc,
            "rejection_rate": round(total_rej / total, 2) if total > 0 else 0,
            "by_source": by_source,
            "top_rejected": top_rejected,
        })
    except Exception as e:
        log.error("match_feedback stats: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/item-sources/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def api_pc_item_sources(pcid, idx):
    """Return fresh source chips HTML for a single item (after price/link update)."""
    log.info("Refresh sources: %s item %d", pcid, idx)
    import copy as _copy
    def _safe_float(v, default=0):
        if v is None: return default
        try: return float(v)
        except (ValueError, TypeError): return default
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items") or []
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Item not found"})

    item = _copy.deepcopy(items[idx])
    p = item.get("pricing") or {}
    unit_cost = float(item.get("vendor_cost") or p.get("unit_cost") or 0)

    # Build source chips using same logic as detail page rendering
    sources = []
    # Catalog
    cat_cost = _safe_float(p.get("catalog_cost"), 0)
    if cat_cost > 0:
        cat_label = p.get("catalog_supplier") or "Catalog"
        cat_url = p.get("catalog_url") or ""
        cat_conf = _safe_float(p.get("catalog_confidence"), 0.5)
        sources.append((cat_cost, cat_label, cat_url, "#f59e0b", True, cat_conf))
    # Oracle/SCPRS
    oracle_p = _safe_float(p.get("oracle_cost") or p.get("scprs_price"), 0)
    if oracle_p > 0:
        sources.append((oracle_p, "Oracle", "", "#8b949e", False, 0.7))
    # Web/scraped
    web_cost = _safe_float(p.get("web_cost"), 0)
    if web_cost > 0:
        web_label = p.get("web_supplier") or "Web"
        web_url = p.get("web_url") or item.get("item_link") or ""
        sources.append((web_cost, web_label, web_url, "#58a6ff", False, 0.85))
    # Amazon
    amz_price = _safe_float(p.get("amazon_price"), 0)
    if amz_price > 0:
        amz_url = p.get("amazon_url") or ""
        sources.append((amz_price, "Amazon", amz_url, "#f59e0b", False, 0.8))
    # Item link (current supplier)
    item_link = (item.get("item_link") or "").strip()
    il_price = _safe_float(p.get("item_link_price") or unit_cost, 0)
    il_supplier = p.get("item_supplier") or ""
    if il_price > 0 and il_supplier:
        sources.append((il_price, il_supplier, item_link, "#f59e0b", True, 0.99))

    if sources:
        sources.sort(key=lambda s: s[0])

    # Build chips HTML
    chips = []
    for sprice, slabel, surl, scolor, spref, sconf in sources:
        pref_icon = "★ " if spref else ""
        price_fmt = f"${sprice:.2f}"
        if sconf > 0.95:
            conf_tag = ' <b style="font-size:10px;padding:1px 4px;border-radius:3px;background:#3fb95030;border:1px solid #3fb95060">EXACT</b>'
        elif sconf >= 0.75:
            conf_tag = ""
        else:
            conf_tag = ' <span style="font-size:10px;padding:1px 4px;border-radius:3px;background:#d2992230;border:1px solid #d2992260">~FUZZY</span>'
        if surl:
            chips.append(f'<a href="{surl}" target="_blank" style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;font-size:13px;background:{scolor}15;border:1px solid {scolor}40;color:{scolor};text-decoration:none;white-space:nowrap">{pref_icon}<b>{price_fmt}</b> {slabel}{conf_tag}</a>')
        else:
            chips.append(f'<span style="display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:4px;font-size:13px;background:{scolor}15;border:1px solid {scolor}40;color:{scolor};white-space:nowrap">{pref_icon}<b>{price_fmt}</b> {slabel}{conf_tag}</span>')

    html = '<div style="display:flex;flex-wrap:wrap;gap:3px">' + ''.join(chips) + '</div>' if chips else '<span style="color:#484f58;font-size:14px">No sources</span>'
    return jsonify({"ok": True, "html": html})


@bp.route("/api/pricecheck/<pcid>/find-better-pricing/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def api_find_better_pricing(pcid, idx):
    """V4: Research cheaper sourcing for a specific item."""
    import copy as _copy
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items") or []
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Item not found"})
    item = items[idx]
    p = item.get("pricing") or {}
    desc = (item.get("description") or "").strip()
    cost = float(item.get("vendor_cost") or p.get("unit_cost") or 0)
    mfg = item.get("mfg_number") or p.get("mfg_number") or ""
    qty = item.get("qty", 1) or 1
    uom = (item.get("uom") or "EA").upper()

    if not desc:
        return jsonify({"ok": False, "error": "No description"})
    if cost <= 0:
        return jsonify({"ok": False, "error": "No cost data — set a cost first"})

    from src.agents.cost_reduction_agent import research_cost_reduction
    result = research_cost_reduction(
        description=desc, current_cost=cost, mfg_number=mfg,
        quantity=qty, uom=uom,
    )
    return jsonify(result)


@bp.route("/api/pc/health-check", methods=["POST"])
@auth_required
@safe_route
def api_pc_health_check():
    """Batch health check: validate all existing PC output PDFs."""
    try:
        from src.forms.batch_health import run_health_check
        body = request.get_json(silent=True) or {}
        auto_regen = body.get("auto_regenerate", False)
        max_items = body.get("max", 50)
        report = run_health_check(
            auto_regenerate=auto_regen,
            max_items=max_items,
        )
        return jsonify({"ok": True, "report": report})
    except Exception as e:
        log.error("health-check error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pc/health-summary")
@auth_required
@safe_route
def api_pc_health_summary():
    """Quick health summary without full re-verification."""
    try:
        from src.forms.batch_health import get_health_summary
        return jsonify({"ok": True, "summary": get_health_summary()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/oracle/health")
@auth_required
@safe_route
def api_oracle_health():
    """Oracle forcing function: verify the feedback loop is running."""
    from src.core.db import get_db
    result = {"ok": True}
    try:
        with get_db() as conn:
            # Last successful weekly report
            try:
                row = conn.execute("""
                    SELECT sent_at, win_count, loss_count, supplier_leads, calibrations
                    FROM oracle_report_log WHERE success=1
                    ORDER BY sent_at DESC LIMIT 1
                """).fetchone()
                if row:
                    result["last_report"] = {
                        "sent_at": row[0], "wins": row[1], "losses": row[2],
                        "supplier_leads": row[3], "calibrations": row[4],
                    }
                    from datetime import datetime as _dt
                    try:
                        days = (_dt.now() - _dt.fromisoformat(row[0])).days
                    except Exception:
                        days = 99
                    result["days_since_report"] = days
                    result["report_overdue"] = days > 9
                else:
                    result["last_report"] = None
                    result["days_since_report"] = None
                    result["report_overdue"] = None
            except Exception:
                result["last_report"] = None

            # Calibration table health
            try:
                cal = conn.execute("SELECT COUNT(*), MAX(last_updated) FROM oracle_calibration").fetchone()
                result["calibration_rows"] = cal[0] or 0
                result["calibration_last_updated"] = cal[1]
            except Exception:
                result["calibration_rows"] = 0

            # Scheduler heartbeat
            try:
                from src.core.scheduler import get_all_jobs
                jobs = get_all_jobs()
                oracle_job = next((j for j in jobs if j.get("name") == "oracle-weekly-report"), None)
                result["scheduler_job"] = oracle_job
            except Exception:
                result["scheduler_job"] = None

    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)


@bp.route("/api/oracle/seed-calibration", methods=["POST"])
@auth_required
@safe_route
def api_oracle_seed_calibration():
    """One-time seed: process all historical wins/losses into calibration table."""
    from src.agents.oracle_weekly_report import seed_calibration_from_history
    stats = seed_calibration_from_history()
    return jsonify({"ok": True, "stats": stats})


@bp.route("/api/pricecheck/<pcid>/oracle-auto-price", methods=["POST"])
@auth_required
@safe_route
def api_pc_oracle_auto_price(pcid):
    """Oracle Auto-Price: holistic pricing for the full quote.

    Runs Oracle on every item, then does a portfolio balance pass:
    - Items with strong SCPRS data → price to win (just below competitor avg)
    - Items with no data → cost + default markup
    - Portfolio pass: if overall markup is too high vs market, reduce outliers
    Each item gets a clear "Price at $X to win" recommendation.
    """
    import copy as _copy
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})
        items = _copy.deepcopy(pc.get("items") or [])
        if not items:
            return jsonify({"ok": False, "error": "No items"})
        if len(items) > 30:
            log.warning("Oracle auto-price: capping %d items to 30 for %s", len(items), pcid)
            items = items[:30]
        log.info("Oracle auto-price: %s — %d items", pcid, len(items))

        from src.core.pricing_oracle_v2 import get_pricing

        # Pass 1: Get Oracle recommendation for each item
        item_recs = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            p = item.get("pricing") or {}
            desc = (item.get("description") or "").strip()
            if not desc:
                item_recs.append({"idx": idx, "skip": True})
                continue
            cost = 0
            for k in ["vendor_cost", "unit_cost"]:
                v = item.get(k) or p.get(k) or 0
                try:
                    cost = float(v)
                except (ValueError, TypeError):
                    cost = 0
                if cost > 0:
                    break
            item_num = item.get("mfg_number") or p.get("mfg_number") or ""
            qty = item.get("qty", 1) or 1
            qpu = item.get("qty_per_uom", 1) or 1

            oracle = get_pricing(
                description=desc, quantity=qty,
                cost=cost if cost > 0 else None,
                item_number=item_num, qty_per_uom=qpu
            )
            rec = oracle.get("recommendation") or {}
            market = oracle.get("market") or {}
            strategies = oracle.get("strategies") or []

            # Pick optimal price: MAXIMIZE margin while still WINNING.
            # Strategy: price just below competitor average (highest viable price).
            # Floor at cost+20% to cover 90-day price risk (45-day quote + net45).
            rec_price = rec.get("quote_price")
            confidence = rec.get("confidence", "low")
            rationale = rec.get("rationale", "")

            # Floor: cost + 20% covers price fluctuations over 90-day exposure
            floor_price = round(cost * 1.20, 2) if cost > 0 else 0

            # Build a sorted list of candidate prices from strategies
            # Priority: Maximize Margin (highest winning) > Win Price > Undercut All
            _candidates = []
            for strat in strategies:
                sp = strat.get("price", 0)
                sn = strat.get("name", "").lower()
                if sp > 0 and "floor" not in sn:
                    _candidates.append((sp, strat))
            # Sort descending — prefer highest price that's still viable
            _candidates.sort(key=lambda x: -x[0])

            _best = None
            for sp, strat in _candidates:
                # Skip if below floor
                if floor_price > 0 and sp < floor_price:
                    continue
                # Sanity cap: without real competitor data, cap at 3x cost
                _has_real_comps = bool(market.get("competitor_avg") or market.get("competitor_low"))
                ceiling_cap = round(cost * (10 if _has_real_comps else 3), 2) if cost > 0 else 0
                if ceiling_cap > 0 and sp > ceiling_cap:
                    continue
                _best = (sp, strat)
                break

            if _best:
                sp, strat = _best
                rec_price = sp
                rationale = f"{strat['name']}: ${sp:.2f} ({strat.get('markup_pct', 0):.0f}% on ${cost:.2f})"
            elif floor_price > 0:
                # All strategies below floor — use floor
                rec_price = floor_price
                rationale = f"Market below floor — ${floor_price:.2f} (20% on ${cost:.2f})"

            # Fallback: cost + 25% if no Oracle data
            if not rec_price and cost > 0:
                rec_price = round(cost * 1.25, 2)
                confidence = "low"
                rationale = f"No market data — 25% on ${cost:.2f}"

            comp_avg = market.get("competitor_avg")
            comp_low = market.get("competitor_low")
            data_pts = market.get("data_points", 0)
            cal_data = rec.get("calibration")

            # V5 Phase 3: Cost sourcing alert
            cost_alert = None
            if cost > 0 and comp_avg and cost > comp_avg:
                delta_pct = round((cost - comp_avg) / comp_avg * 100, 1)
                cost_alert = {
                    "type": "above_market",
                    "our_cost": round(cost, 2),
                    "market_avg": round(comp_avg, 2),
                    "delta_pct": delta_pct,
                }

            item_recs.append({
                "idx": idx,
                "skip": False,
                "recommended_price": rec_price,
                "cost": cost if cost > 0 else None,
                "confidence": confidence,
                "rationale": rationale,
                "comp_avg": comp_avg,
                "comp_low": comp_low,
                "calibration": cal_data,
                "data_points": data_pts,
                "data_confidence": rec.get("data_confidence", ""),
                "qty": qty,
                "cost_alert": cost_alert,
            })

        # ═══ Pass 2: Portfolio-level optimization ═══
        # Goal: ALWAYS win. Margin covers cost proportional to risk + effort.
        #
        # 1. Calculate minimum profit needed (effort cost + cost of capital)
        # 2. If current profit already covers it → leave prices alone
        # 3. If under → find items with market room and push UP (never above market)
        # 4. If over AND items are above market → pull DOWN to ensure winning
        active = [r for r in item_recs if not r.get("skip") and r.get("recommended_price")]
        n_items = len(active)

        total_cost = sum(r.get("cost", 0) or 0 for r in active)
        total_revenue = sum((r.get("recommended_price", 0) or 0) * max(r.get("qty", 1), 1) for r in active)
        total_item_cost = sum((r.get("cost", 0) or 0) * max(r.get("qty", 1), 1) for r in active)
        total_profit = total_revenue - total_item_cost

        # Minimum profit = effort + capital at risk
        effort_cost = n_items * 25  # ~$25/item for research, pricing, forms
        capital_cost = total_item_cost * 0.08 * (90 / 365)  # 8% annual, 90-day exposure
        min_profit = effort_cost + capital_cost

        log.info("Oracle portfolio: %d items, cost=$%.0f, revenue=$%.0f, profit=$%.0f "
                 "(min=$%.0f = $%d effort + $%.0f capital)",
                 n_items, total_item_cost, total_revenue, total_profit,
                 min_profit, effort_cost, capital_cost)

        # Step A: Pull DOWN items priced above market average (ensure we WIN)
        for r in active:
            if r.get("comp_avg") and r["recommended_price"] > r["comp_avg"]:
                new_price = round(r["comp_avg"] * 0.98, 2)
                floor = r["cost"] * 1.20 if r.get("cost") and r["cost"] > 0 else 0
                if new_price > floor:
                    r["recommended_price"] = new_price
                    r["rationale"] = f"Win: ${new_price:.2f} (2% under avg ${r['comp_avg']:.2f})"
                    r["confidence"] = "high"

        # Recalculate after pull-down
        total_revenue = sum((r.get("recommended_price", 0) or 0) * max(r.get("qty", 1), 1) for r in active)
        total_profit = total_revenue - total_item_cost

        # Step B: If profit is below minimum, push UP items that have market room
        if total_profit < min_profit and n_items > 0:
            shortfall = min_profit - total_profit
            # Find items with room: price is well below comp_avg (>15% gap)
            flex_items = []
            for r in active:
                ca = r.get("comp_avg")
                rp = r.get("recommended_price", 0) or 0
                q = max(r.get("qty", 1), 1)
                if ca and rp < ca * 0.95:
                    room = (ca * 0.98 - rp) * q  # max $ we can add per this item
                    if room > 0:
                        flex_items.append((r, room, ca))
            # Distribute shortfall proportionally across flex items
            total_room = sum(room for _, room, _ in flex_items)
            if total_room > 0:
                for r, room, ca in flex_items:
                    share = min(room, shortfall * (room / total_room))
                    q = max(r.get("qty", 1), 1)
                    new_price = round(r["recommended_price"] + share / q, 2)
                    # Never exceed 98% of comp_avg (still winning)
                    cap = round(ca * 0.98, 2)
                    new_price = min(new_price, cap)
                    if new_price > r["recommended_price"]:
                        old = r["recommended_price"]
                        r["recommended_price"] = new_price
                        r["rationale"] = f"Portfolio +${(new_price-old)*q:.0f}: ${new_price:.2f} (still under avg ${ca:.2f})"
                        r["confidence"] = "high"
                log.info("Oracle portfolio: pushed up %d items to cover $%.0f shortfall",
                         len(flex_items), shortfall)

        # Final totals for response
        total_revenue = sum((r.get("recommended_price", 0) or 0) * max(r.get("qty", 1), 1) for r in active)
        total_profit = total_revenue - total_item_cost
        overall_margin = round(total_profit / total_revenue * 100, 1) if total_revenue > 0 else 0

        log.info("Oracle portfolio final: revenue=$%.0f, profit=$%.0f (%.1f%%), min_needed=$%.0f, %s",
                 total_revenue, total_profit, overall_margin, min_profit,
                 "COVERED" if total_profit >= min_profit else "SHORTFALL")

        # Build win labels
        for r in item_recs:
            if r.get("skip") or not r.get("recommended_price"):
                continue
            if r.get("comp_avg") and r["recommended_price"] <= r["comp_avg"]:
                pct_under = round((1 - r["recommended_price"] / r["comp_avg"]) * 100)
                r["win_label"] = f"${r['recommended_price']:.2f} ↓{pct_under}%"
            elif r.get("recommended_price"):
                r["win_label"] = f"${r['recommended_price']:.2f}"

        # Calculate overall win probability
        total = len([r for r in item_recs if not r.get("skip")])
        competitive = len([r for r in item_recs if not r.get("skip")
                          and r.get("comp_avg") and r.get("recommended_price")
                          and r["recommended_price"] <= r["comp_avg"]])
        no_data = len([r for r in item_recs if not r.get("skip") and not r.get("comp_avg")])
        win_prob = max(0, min(95, int((competitive / total * 85) + 10))) if total > 0 else 0
        if no_data > total * 0.5:
            win_prob = min(win_prob, 40)

        # Persist corrected oracle_price on items (clears stale pre-QPU values)
        try:
            real_pcs = _load_price_checks()
            real_pc = real_pcs.get(pcid)
            if real_pc:
                real_items = real_pc.get("items") or []
                for r in item_recs:
                    if r.get("skip") or not r.get("recommended_price"):
                        continue
                    ridx = r["idx"]
                    if ridx < len(real_items):
                        real_items[ridx]["oracle_price"] = r["recommended_price"]
                        real_items[ridx]["oracle_confidence"] = r.get("confidence", "")
                        real_items[ridx]["oracle_rationale"] = r.get("rationale", "")
                _save_price_checks(real_pcs)
        except Exception as e:
            log.warning("Oracle auto-price persist failed: %s", e)

        # V5 Phase 3: Count cost alerts
        cost_alerts = [r for r in item_recs if not r.get("skip") and r.get("cost_alert")]

        return jsonify({
            "ok": True,
            "items": [r for r in item_recs if not r.get("skip")],
            "win_probability": win_prob,
            "total_items": total,
            "items_competitive": competitive,
            "cost_alerts_count": len(cost_alerts),
            "portfolio": {
                "total_cost": round(total_item_cost, 2),
                "total_revenue": round(total_revenue, 2),
                "total_profit": round(total_profit, 2),
                "margin_pct": overall_margin,
                "min_profit_needed": round(min_profit, 2),
                "effort_cost": effort_cost,
                "capital_cost": round(capital_cost, 2),
                "profit_covers_cost": total_profit >= min_profit,
            },
        })
    except Exception as e:
        log.error("Oracle auto-price for %s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/quote-analysis", methods=["POST"])
@auth_required
@safe_route
def api_pc_quote_analysis(pcid):
    """Portfolio-level Oracle analysis — win probability for the full quote."""
    try:
        import copy as _copy
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})
        items = _copy.deepcopy(pc.get("items") or [])
        if not items:
            return jsonify({"ok": False, "error": "No items"})

        from src.core.pricing_oracle_v2 import get_pricing

        analysis_items = []
        total_competitive = 0
        total_at_risk = 0
        total_no_data = 0
        total_scprs_points = 0
        markup_pcts = []

        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            p = item.get("pricing") or {}
            desc = (item.get("description") or "").strip()
            if not desc:
                continue
            cost = item.get("vendor_cost") or p.get("unit_cost") or 0
            try:
                cost = float(cost)
            except (ValueError, TypeError):
                cost = 0
            item_num = item.get("mfg_number") or p.get("mfg_number") or ""
            qty = item.get("qty", 1) or 1
            qpu = item.get("qty_per_uom", 1) or 1
            # Get current bid price — PC items use "unit_price", RFQ uses "final_price"/"bid_price"
            your_price = (item.get("unit_price") or p.get("final_price")
                          or p.get("bid_price") or p.get("unit_price") or 0)
            try:
                your_price = float(your_price)
            except (ValueError, TypeError):
                your_price = 0

            # Run Oracle for this item
            oracle = get_pricing(
                description=desc, quantity=qty,
                cost=cost if cost > 0 else None,
                item_number=item_num, qty_per_uom=qpu
            )
            market = oracle.get("market") or {}
            sc = oracle.get("source_counts") or {}
            scprs_pts = sum(sc.get(k, 0) for k in
                           ["won_quotes", "winning_prices", "scprs_catalog", "scprs_po_lines"])
            total_scprs_points += scprs_pts

            comp_avg = market.get("competitor_avg")
            comp_low = market.get("competitor_low")
            scprs_avg = market.get("weighted_avg")

            # Determine status
            status = "no_data"
            if comp_avg and your_price > 0:
                if your_price <= comp_avg:
                    status = "competitive"
                    total_competitive += 1
                else:
                    status = "at_risk"
                    total_at_risk += 1
            elif scprs_avg and your_price > 0:
                if your_price <= scprs_avg:
                    status = "competitive"
                    total_competitive += 1
                else:
                    status = "at_risk"
                    total_at_risk += 1
            else:
                total_no_data += 1

            # Markup
            mkp = None
            if cost > 0 and your_price > 0:
                mkp = ((your_price - cost) / cost) * 100
                markup_pcts.append(mkp)

            analysis_items.append({
                "line_num": idx + 1,
                "description": desc[:60],
                "your_price": your_price if your_price > 0 else None,
                "scprs_avg": scprs_avg,
                "comp_low": comp_low,
                "status": status,
                "markup_pct": round(mkp, 1) if mkp is not None else None,
                "data_points": market.get("data_points", 0),
            })

        total_items = len(analysis_items)
        # Win probability: weighted score
        if total_items > 0:
            comp_ratio = total_competitive / total_items
            risk_ratio = total_at_risk / total_items
            # Base: % competitive items, penalize at-risk items heavily
            win_prob = max(0, min(95, int(comp_ratio * 85 - risk_ratio * 30 + 10)))
            if total_no_data > total_items * 0.5:
                win_prob = min(win_prob, 40)  # Cap if >50% items lack data
        else:
            win_prob = 0

        avg_markup = round(sum(markup_pcts) / len(markup_pcts), 1) if markup_pcts else 0

        return jsonify({
            "ok": True,
            "summary": {
                "total_items": total_items,
                "items_competitive": total_competitive,
                "items_at_risk": total_at_risk,
                "items_no_data": total_no_data,
                "win_probability": win_prob,
                "total_scprs_points": total_scprs_points,
                "avg_markup": avg_markup,
            },
            "items": analysis_items,
        })
    except Exception as e:
        log.error("Quote analysis for %s: %s", pcid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/price-history/<int:item_idx>")
@auth_required
@safe_route
def api_pc_item_price_history(pcid, item_idx):
    """Get historical pricing for a specific PC item (P2.1 Pricing Memory)."""
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})
        items = pc.get("items", [])
        if item_idx < 0 or item_idx >= len(items):
            return jsonify({"ok": False, "error": "Item not found"})
        item = items[item_idx]
        desc = item.get("description", "")
        item_num = item.get("mfg_number", "") or item.get("item_number", "")
        agency = pc.get("institution", "") or pc.get("agency", "")
        from src.core.pricing_oracle_v2 import get_price_history_for_item
        history = get_price_history_for_item(desc, item_num, agency)
        return jsonify({"ok": True, "history": history})
    except Exception as e:
        log.error("price-history %s item %d: %s", pcid, item_idx, e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/amazon-match/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def amazon_match_item(pcid, idx):
    """Per-item Amazon search with pack-size-aware matching."""
    import re as _re
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items") or []
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Item index out of range"})
    item = items[idx]
    p = item.get("pricing") or {}

    desc = (item.get("description") or "").strip()
    mfg = (item.get("mfg_number") or p.get("mfg_number") or "").strip()
    qpu = 1
    try:
        qpu = int(float(item.get("qty_per_uom", 1)))
    except (ValueError, TypeError):
        pass
    uom = (item.get("uom") or "EA").upper().strip()
    _UOM_LABELS = {"PK": "pack", "BX": "box", "BOX": "box", "CS": "case",
                   "EA": "each", "CT": "carton", "DZ": "dozen"}

    # Build qty-aware search query
    query_parts = []
    # Use first ~80 chars of description (avoid noise)
    if desc:
        query_parts.append(desc[:80])
    if mfg:
        query_parts.append(mfg)
    if qpu > 1:
        uom_label = _UOM_LABELS.get(uom, uom.lower())
        query_parts.append(f"{qpu} {uom_label}")
    query = " ".join(query_parts)
    if not query.strip():
        return jsonify({"ok": False, "error": "No description or MFG# to search"})

    try:
        from src.agents.product_research import search_amazon
        results = search_amazon(query, max_results=5)
    except Exception as e:
        log.error("Amazon match error for %s item %d: %s", pcid, idx, e)
        return jsonify({"ok": False, "error": f"Search failed: {e}"})

    if not results:
        return jsonify({"ok": True, "matches": [], "best": None,
                        "message": "No Amazon results found"})

    # Score results — pack size matching is the key differentiator
    _QTY_PATTERNS = [
        _re.compile(r'(\d+)\s*[-/]?\s*(?:pack|pk|count|ct)', _re.I),
        _re.compile(r'(?:pack|box|case|set)\s+(?:of\s+)?(\d+)', _re.I),
        _re.compile(r'(\d+)\s*(?:pc|pcs|pieces?|sheets?|per\s+box)', _re.I),
        _re.compile(r',\s*(\d+)\s*$', _re.I),
    ]

    def _extract_title_qty(title):
        for pat in _QTY_PATTERNS:
            m = pat.search(title)
            if m:
                try:
                    return int(m.group(1))
                except (ValueError, IndexError):
                    pass
        return None

    desc_tokens = set(_re.findall(r'[a-z]{3,}', desc.lower()))
    scored = []
    for r in results:
        score = 0
        title = r.get("title", "")
        title_lower = title.lower()

        # MFG# match
        if mfg and mfg.lower() in title_lower:
            score += 30

        # Description token overlap
        title_tokens = set(_re.findall(r'[a-z]{3,}', title_lower))
        if desc_tokens and title_tokens:
            overlap = len(desc_tokens & title_tokens)
            score += min(20, overlap * 4)

        # Pack size match (most important)
        title_qty = _extract_title_qty(title)
        if qpu > 1 and title_qty:
            if title_qty == qpu:
                score += 50  # Exact match
            elif abs(title_qty - qpu) / max(qpu, 1) < 0.1:
                score += 30  # Within 10%
            else:
                score -= 40  # Wrong size
        elif qpu > 1 and not title_qty:
            score -= 10  # Can't verify qty

        r["_score"] = score
        r["_title_qty"] = title_qty
        scored.append(r)

    scored.sort(key=lambda x: x["_score"], reverse=True)
    best = scored[0] if scored and scored[0]["_score"] >= 10 else None

    # Persist to catalog if confident match
    catalog_id = None
    if best and best.get("price") and best["_score"] >= 30:
        try:
            from src.agents.product_catalog import (
                match_item, add_to_catalog, add_supplier_price, init_catalog_db
            )
            init_catalog_db()
            _desc = best.get("title", "")
            _pn = mfg or best.get("mfg_number", "")
            _price = float(best["price"])
            _asin = best.get("asin", "")
            _url = best.get("url", "")

            matches = match_item(_desc, _pn, top_n=1) if (_desc or _pn) else []
            if matches and matches[0].get("match_confidence", 0) >= 0.55:
                catalog_id = matches[0]["id"]
            else:
                catalog_id = add_to_catalog(
                    description=_desc, part_number=_pn,
                    cost=_price, supplier_url=_url,
                    manufacturer=best.get("manufacturer", ""),
                    mfg_number=_pn,
                    source="amazon_match"
                )
            if catalog_id and _price > 0:
                add_supplier_price(
                    product_id=catalog_id, supplier_name="Amazon",
                    price=_price, url=_url,
                    source="amazon_match"
                )
            log.info("Amazon match → catalog %s for %s item %d (score %d)",
                     catalog_id, pcid, idx, best["_score"])
        except Exception as e:
            log.warning("Amazon match catalog write failed: %s", e)

    return jsonify({
        "ok": True,
        "matches": [{"title": s["title"], "price": s.get("price", 0),
                      "asin": s.get("asin", ""), "url": s.get("url", ""),
                      "score": s["_score"], "title_qty": s.get("_title_qty")}
                     for s in scored[:3]],
        "best": {"title": best["title"], "price": best.get("price", 0),
                 "asin": best.get("asin", ""), "url": best.get("url", ""),
                 "score": best["_score"], "title_qty": best.get("_title_qty"),
                 "catalog_id": catalog_id}
                if best else None,
    })


@bp.route("/api/item-link/lookup", methods=["POST"])
@auth_required
@safe_route
def api_item_link_lookup():
    """
    POST { url: "https://grainger.com/product/..." }
    Returns structured product data: title, price, part_number, shipping, supplier.
    Used for the item_link autofill on PC and RFQ line items.
    Also writes price+supplier to catalog DB for future intelligence.
    """
    import time as _time
    _t0 = _time.monotonic()
    _ENDPOINT_BUDGET = 14.0  # seconds — client timeout is 15s

    data = request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "url required"})

    try:
        from src.agents.item_link_lookup import lookup_from_url
        result = lookup_from_url(url)

        # ── Write-back to catalog DB (skip if near timeout) ──
        _has_useful_data = result.get("price") or result.get("photo_url") or (result.get("title") and len(result.get("title", "")) > 10)
        _time_left = _ENDPOINT_BUDGET - (_time.monotonic() - _t0)
        if result.get("ok") and _has_useful_data and _time_left > 2.0:
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
                        photo_url=result.get("photo_url", ""),
                        source=f"link_lookup_{supplier.lower()[:20]}"
                    )
                    if pid:
                        result["catalog_product_id"] = pid

                # Enrich catalog with all available data (flywheel)
                if pid:
                    try:
                        from src.agents.product_catalog import enrich_catalog_product
                        enrich_catalog_product(
                            pid,
                            upc=result.get("upc", ""),
                            asin=result.get("asin", ""),
                            mfg_number=pn,
                            manufacturer=result.get("manufacturer", ""),
                            photo_url=result.get("photo_url", ""),
                            supplier_name=supplier,
                            supplier_sku=pn,
                            supplier_url=url,
                            supplier_price=price,
                            amazon_price=price if result.get("asin") else 0,
                        )
                    except Exception:
                        pass

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

        # ── Claude semantic match: AI product validation ──
        # When client sends pc_description, compare it to found title.
        # Only call Claude if token match is uncertain (< 70%) AND we have time budget.
        _pc_desc = (data.get("pc_description") or "").strip()
        _time_left = _ENDPOINT_BUDGET - (_time.monotonic() - _t0)
        if _pc_desc and result.get("ok") and result.get("title") and _time_left > 3.0:
            _found_title = result.get("title", "")
            _token_score = _quick_token_match(_pc_desc, _found_title)
            if _token_score < 70:
                try:
                    from src.agents.item_link_lookup import claude_semantic_match
                    _sem = claude_semantic_match(
                        _pc_desc, _found_title, float(result.get("price") or 0))
                    if _sem.get("ok"):
                        result["server_confidence"] = _sem["confidence"]
                        result["server_match"] = _sem["is_match"]
                        result["server_reasoning"] = _sem.get("reasoning", "")
                except Exception as _sem_err:
                    log.debug("Semantic match error: %s", _sem_err)

        return jsonify(result)
    except Exception as e:
        log.error("item_link_lookup API error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


def _quick_token_match(desc_a: str, desc_b: str) -> int:
    """Server-side recall-weighted token match (mirrors JS _productMatchScore).
    Returns 0-100 score."""
    import re as _re
    _stops = {'the','and','for','with','pack','of','per','ea','each','box',
              'pk','set','in','by','to','is','it','at','on','or','an','as',
              'from','bulk','assorted','count','ct','qty','quantity','item',
              'product','new','brand'}
    def _tok(s):
        s = s.lower()
        # Normalize dimensions
        s = _re.sub(r'(\d+\.?\d*)\s*["\u201D]?\s*[xX\u00D7]\s*(\d+\.?\d*)\s*["\u201D]?', r'\1x\2', s)
        # Preserve decimals
        s = _re.sub(r'(\d)\.(\d)', r'\1_D_\2', s)
        s = _re.sub(r'[^a-z0-9\s_]', ' ', s)
        s = s.replace('_D_', '.')
        return {w for w in s.split() if len(w) > 1 and w not in _stops}
    a, b = _tok(desc_a), _tok(desc_b)
    if not a or not b:
        return 0
    overlap = len(a & b)
    recall = overlap / len(a)
    precision = overlap / len(b)
    return round((2 * recall + precision) / 3 * 100)


@bp.route("/api/admin/system-reset", methods=["GET", "POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
def api_admin_poll_result():
    """Check the result of the async poll triggered by reset-and-poll."""
    from src.api.dashboard import POLL_STATUS
    result = POLL_STATUS.get("_reset_poll_result", {"completed": False, "status": "no poll running"})
    return jsonify(result)


@bp.route("/api/admin/poller-control", methods=["GET", "POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
def api_qa_classification_test():
    """Run offline classification tests only (no IMAP needed)."""
    try:
        from src.agents.email_pipeline_qa import test_classification
        return jsonify({"ok": True, **test_classification()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/trends")
@auth_required
@safe_route
def api_qa_trends():
    """Get QA score trends over time."""
    try:
        from src.agents.email_pipeline_qa import get_qa_trends
        return jsonify({"ok": True, **get_qa_trends()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/qa/email-pipeline")
@auth_required
@safe_page
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
@safe_route
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
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    result["diagnosis"] = "PC not found anywhere" if not result["found_in"] else f"Found in: {', '.join(result['found_in'])}"
    return jsonify(result)


@bp.route("/api/disk-emergency", methods=["GET", "POST"])
@auth_required
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
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
    
    # Orders — migrate from JSON if not yet done
    orders_json = os.path.join(_DD, "orders.json")
    if os.path.exists(orders_json):
        try:
            with open(orders_json) as f:
                orders = json.load(f)
            from src.api.dashboard import _save_single_order
            for oid, o in orders.items():
                _save_single_order(oid, o)
            imported["orders"] = len(orders)
            steps.append(f"Imported {len(orders)} orders from JSON to SQLite")
            import os as _os2
            _os2.rename(orders_json, orders_json + ".migrated")
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
@safe_route
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
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
            
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
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
            
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
@safe_route
def api_rfq_package_contents(rid):
    """Show what's inside the generated package PDF."""
    from pypdf import PdfReader
    from src.core.paths import DATA_DIR as _DD
    
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    sol = r.get("solicitation_number", "RFQ") or "RFQ"
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
@safe_route
def api_quote_set_counter(num):
    """Manually set the quote counter. Next quote will be R26Q(num+1).
    Writes ALL counter keys used by quote_generator._load_counter() to prevent drift."""
    from src.forms.quote_generator import set_quote_counter
    import datetime as _dt
    set_quote_counter(num, year=_dt.datetime.now().year)
    return jsonify({"ok": True, "counter": num, "next_quote": f"R{str(_dt.datetime.now().year)[-2:]}Q{num+1}",
                    "note": "All counter keys synced (quote_counter, quote_counter_seq, quote_counter_year)"})


def _reconcile_mfg_from_descriptions(items):
    """Extract S&S/supplier item numbers from descriptions and fix MFG# mismatches.
    Patterns: 'S&S Item #: PS1474', 'Item #: NL304', 'MFG#: W12919', 'SKU: 343586'."""
    import re as _re_rec
    _patterns = [
        _re_rec.compile(r'S&S\s+Item\s*#?\s*:?\s*([A-Z]{1,3}\d{2,6})', _re_rec.IGNORECASE),
        _re_rec.compile(r'(?:Item|MFG|SKU|PN)\s*#?\s*:?\s*([A-Z]{1,3}\d{2,6})', _re_rec.IGNORECASE),
        _re_rec.compile(r'(?:Item|MFG|SKU|PN)\s*#?\s*:?\s*(\d{4,7})', _re_rec.IGNORECASE),
    ]
    fixes = []
    for idx, item in enumerate(items):
        desc = item.get("description", "") or ""
        if not desc:
            continue
        current_mfg = (item.get("mfg_number") or item.get("item_number") or "").strip()
        for pat in _patterns:
            m = pat.search(desc)
            if m:
                extracted = m.group(1).strip().upper()
                if extracted and extracted != current_mfg.upper():
                    old_mfg = current_mfg
                    item["mfg_number"] = extracted
                    item["item_number"] = extracted
                    fixes.append({"line": idx + 1, "old": old_mfg, "new": extracted,
                                  "desc": desc[:60]})
                    log.info("MFG# reconcile line %d: %s → %s (from desc: %s)",
                             idx + 1, old_mfg, extracted, desc[:60])
                break
    return fixes


@bp.route("/api/pricecheck/<pcid>/reconcile-mfg", methods=["POST"])
@auth_required
@safe_route
def api_reconcile_mfg(pcid):
    """Scan descriptions for embedded supplier item numbers and fix MFG# mismatches."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items", [])
    fixes = _reconcile_mfg_from_descriptions(items)
    if fixes:
        _save_single_pc(pcid, pc)
        log.info("MFG# reconcile for %s: %d fixes applied", pcid, len(fixes))
    return jsonify({"ok": True, "fixes": fixes, "count": len(fixes)})


@bp.route("/api/pricecheck/<pcid>/rescrape-unpriced", methods=["POST"])
@auth_required
@safe_route
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
                                item["pricing"]["amazon_url"] = amz[0].get("url", "")
                                item["pricing"]["amazon_price"] = price
                                item["pricing"]["amazon_title"] = amz[0].get("title", "")[:200]
                                # Follow up with ASIN product lookup for list/sale price split
                                try:
                                    from src.agents.product_research import lookup_amazon_product
                                    _prod = lookup_amazon_product(_amz_asin)
                                    if _prod:
                                        if _prod.get("list_price"):
                                            item["pricing"]["list_price"] = _prod["list_price"]
                                            item["list_price"] = _prod["list_price"]
                                        if _prod.get("sale_price"):
                                            item["pricing"]["sale_price"] = _prod["sale_price"]
                                            item["sale_price"] = _prod["sale_price"]
                                except Exception:
                                    pass
                    except Exception as _e:
                        log.debug("Suppressed: %s", _e)
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


@bp.route("/api/pricecheck/<pcid>/auto-price", methods=["POST"])
@auth_required
@safe_route
def api_pc_auto_price(pcid):
    """Auto-price all items: catalog match → scrape catalog URLs → Amazon fallback."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    items = pc.get("items", [])
    if not items:
        return jsonify({"ok": False, "error": "No items"})

    results = []
    priced = 0
    default_markup = pc.get("default_markup") or 25

    # Step 1: Catalog batch match
    catalog_urls = {}
    try:
        from src.agents.product_catalog import match_items_batch, init_catalog_db
        init_catalog_db()
        batch_input = [
            {"idx": i, "description": it.get("description", ""),
             "part_number": it.get("mfg_number", "") or it.get("item_number", "") or it.get("part_number", "")}
            for i, it in enumerate(items)
        ]
        batch_results = match_items_batch(batch_input)
        for m in batch_results:
            idx = m.get("idx")
            if idx is not None and m.get("matched"):
                catalog_urls[idx] = {
                    "url": m.get("supplier_url") or m.get("best_supplier_url") or "",
                    "cost": m.get("best_cost") or m.get("cost") or 0,
                    "supplier": m.get("best_supplier") or m.get("best_supplier_name") or "",
                    "mfg": m.get("mfg_number") or m.get("sku") or "",
                    "name": m.get("canonical_name") or "",
                    "confidence": m.get("confidence") or 0,
                }
    except Exception as e:
        log.warning("Auto-price PC catalog match: %s", e)

    # Step 2: For each item — catalog URL scrape → catalog cost → Amazon
    for i, item in enumerate(items):
        desc = item.get("description", "")
        if not desc or len(desc) < 5:
            results.append({"line": i + 1, "status": "skipped", "note": "No description"})
            continue

        p = item.get("pricing") or {}
        cat = catalog_urls.get(i)
        price = 0
        source = ""
        supplier = ""
        mfg = ""
        url = ""

        # 2a: Catalog URL → fresh scrape
        if cat and cat.get("url"):
            try:
                from src.agents.item_link_lookup import lookup_from_url, detect_supplier
                res = lookup_from_url(cat["url"])
                _p = res.get("price") or res.get("list_price") or res.get("cost")
                if _p:
                    try:
                        price = float(_p)
                    except (ValueError, TypeError):
                        price = 0
                if price > 0:
                    source = "catalog_url"
                    supplier = detect_supplier(cat["url"])
                    url = cat["url"]
                    mfg = res.get("mfg_number") or res.get("part_number") or cat.get("mfg", "")
            except Exception as e:
                log.debug("Auto-price PC catalog URL line %d: %s", i + 1, e)

        # 2b: Catalog cost fallback
        if price <= 0 and cat and float(cat.get("cost", 0)) > 0:
            price = float(cat["cost"])
            source = "catalog"
            supplier = cat.get("supplier", "")
            mfg = cat.get("mfg", "")
            url = cat.get("url", "")

        # 2c: Amazon search
        if price <= 0:
            try:
                from src.agents.product_research import search_amazon
                amz = search_amazon(desc[:120], max_results=1)
                if amz and amz[0].get("price", 0) > 0:
                    price = float(amz[0]["price"])
                    source = "amazon"
                    supplier = "Amazon"
                    url = amz[0].get("url", "")
                    mfg = amz[0].get("mfg_number", "") or amz[0].get("item_number", "")
            except Exception as e:
                log.debug("Auto-price PC Amazon line %d: %s", i + 1, e)

        # 2d: Apply
        if price > 0:
            if not p:
                p = {}
                item["pricing"] = p
            p["unit_cost"] = price
            p["source"] = source
            if url:
                p["source_url"] = url
                item["item_link"] = url
            if supplier:
                item["item_supplier"] = supplier
            if mfg:
                item["mfg_number"] = mfg
                item["item_number"] = mfg
            markup = p.get("markup_pct") or default_markup
            try:
                markup = float(markup)
            except (ValueError, TypeError):
                markup = 25
            p["markup_pct"] = markup
            p["recommended_price"] = round(price * (1 + markup / 100), 2)
            item["vendor_cost"] = price
            item["unit_price"] = p["recommended_price"]
            priced += 1
            results.append({
                "line": i + 1, "status": "ok", "source": source,
                "price": price, "supplier": supplier, "mfg": mfg,
                "url": url[:60] if url else "",
                "catalog_confidence": cat.get("confidence", 0) if cat else 0,
            })
        elif cat:
            if cat.get("url") and not item.get("item_link"):
                item["item_link"] = cat["url"]
            if cat.get("mfg") and not item.get("mfg_number"):
                item["mfg_number"] = cat["mfg"]
            results.append({
                "line": i + 1, "status": "linked", "source": "catalog",
                "note": "Catalog matched, no live price",
                "catalog_confidence": cat.get("confidence", 0),
            })
        else:
            results.append({"line": i + 1, "status": "no_match"})

        if source in ("catalog_url", "amazon"):
            import time
            time.sleep(0.5)

    if priced > 0:
        _save_single_pc(pcid, pc)
        try:
            from src.agents.product_catalog import save_pc_items_to_catalog
            save_pc_items_to_catalog(pc)
        except Exception:
            pass

    return jsonify({"ok": True, "results": results, "priced": priced, "total": len(items),
                    "catalog_matched": len(catalog_urls)})


# ── Bulk scrape progress tracking (V5 UX) ──
import threading as _bs_threading
BULK_SCRAPE_STATUS = {}
_BULK_SCRAPE_LOCK = _bs_threading.Lock()


@bp.route("/api/pricecheck/<pcid>/bulk-scrape-status")
@auth_required
@safe_route
def api_bulk_scrape_status(pcid):
    """Poll bulk scrape progress."""
    with _BULK_SCRAPE_LOCK:
        status = BULK_SCRAPE_STATUS.get(pcid, {})
    return jsonify({"ok": True, **status})


@bp.route("/api/pricecheck/<pcid>/bulk-scrape-urls", methods=["POST"])
@auth_required
@safe_route
def api_bulk_scrape_urls(pcid):
    """Bulk paste URLs → scrape each → apply cost + supplier to items by index."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    items = pc.get("items", [])
    results = []
    applied = 0
    import re as _re_bulk

    # Structured mode: item_map = {item_number: url}
    item_map = data.get("item_map")
    _line_labels = {}
    if item_map and isinstance(item_map, dict):
        log.info("Bulk scrape (structured): %s — %d item mappings", pcid, len(item_map))
        _item_idx = {}
        for idx_i, it in enumerate(items):
            _num = str(it.get("item_number", "")).strip()
            if _num:
                _item_idx[_num] = idx_i
            _ridx = str(it.get("row_index", "")).strip()
            if _ridx and _ridx not in _item_idx:
                _item_idx[_ridx] = idx_i
        _structured_map = {}
        for item_num, url in item_map.items():
            target_idx = _item_idx.get(str(item_num).strip())
            if target_idx is not None:
                _structured_map[target_idx] = (url, int(item_num))
                _line_labels[target_idx] = int(item_num)
            else:
                results.append({"line": int(item_num), "url": url[:60], "status": "skipped",
                                "error": f"Item #{item_num} not found in PC"})
        urls = []
        for idx_j in range(len(items)):
            urls.append(_structured_map[idx_j][0] if idx_j in _structured_map else "")
    else:
        urls = data.get("urls", [])

    if not urls and not item_map:
        return jsonify({"ok": False, "error": "No URLs provided"})

    # Auto-reconcile MFG# from descriptions before scraping
    _mfg_fixes = _reconcile_mfg_from_descriptions(items)
    if _mfg_fixes:
        log.info("Bulk scrape auto-reconcile: %d MFG# fixes for %s", len(_mfg_fixes), pcid)

    # Initialize progress tracking
    with _BULK_SCRAPE_LOCK:
        BULK_SCRAPE_STATUS[pcid] = {
            "running": True, "total": len(urls), "completed": 0,
            "applied": 0, "current_item": "", "results": [],
        }

    for i, raw_line in enumerate(urls):
        raw_line = (raw_line or "").strip()
        raw_line = _re_bulk.sub(r'^\d+[\.\)]\s*', '', raw_line)
        _display_line = _line_labels.get(i, i + 1)
        # Update progress
        with _BULK_SCRAPE_LOCK:
            if pcid in BULK_SCRAPE_STATUS:
                BULK_SCRAPE_STATUS[pcid]["completed"] = i
                BULK_SCRAPE_STATUS[pcid]["current_item"] = f"Item {_display_line}: looking up..."
        if not raw_line or i >= len(items):
            results.append({"line": _display_line, "url": raw_line[:60], "status": "skipped"})
            continue

        # ── Smart extraction: pull URL from mixed-content lines ──
        # Handles LLM output like: "1,24,SKU 343586,Each,Description,https://..."
        # or "Colgate Toothpaste https://www.dollartree.com/..." or just a plain URL
        _url_match = _re_bulk.search(r'(https?://\S+)', raw_line)
        if _url_match:
            url = _url_match.group(1).rstrip('.,;)')
            # Extract metadata from the non-URL part (CSV or free text)
            _pre = raw_line[:_url_match.start()].strip().rstrip(',')
        else:
            url = raw_line
            _pre = ""

        # Parse CSV fields from prefix if present (LLM formats: line#,qty,sku,uom,desc)
        _parsed_desc = ""
        _parsed_mfg = ""
        _parsed_qty = 0
        if _pre and ',' in _pre:
            _parts = [p.strip() for p in _pre.split(',')]
            for _part in _parts:
                if not _part:
                    continue
                if _re_bulk.match(r'^(SKU|MFG|PN|Item)\s*#?\s*\d', _part, _re_bulk.IGNORECASE):
                    _parsed_mfg = _re_bulk.sub(r'^(SKU|MFG|PN|Item)\s*#?\s*', '', _part, flags=_re_bulk.IGNORECASE).strip()
                elif _re_bulk.match(r'^\d{1,4}$', _part) and not _parsed_qty:
                    _v = int(_part)
                    if _v > 0 and _v < 10000:
                        _parsed_qty = _v
                elif len(_part) > 10 and not _re_bulk.match(r'^(Each|EA|CS|BX|PK|Case|Box|Pack|DZ|CT)$', _part, _re_bulk.IGNORECASE):
                    _parsed_desc = _part
        elif _pre and len(_pre) > 5:
            _parsed_desc = _pre

        try:
            from src.agents.item_link_lookup import lookup_from_url
            r = lookup_from_url(url)
            item = items[i]
            # Always apply URL, supplier, MFG#, description — even without price
            item["item_link"] = r.get("url", url)  # use canonical URL if lookup resolved it
            item["item_supplier"] = r.get("supplier", "")
            _pn = r.get("mfg_number") or r.get("part_number") or _parsed_mfg or ""
            if _pn:
                item["item_number"] = _pn
                item["mfg_number"] = _pn
            _title = r.get("title") or r.get("description") or _parsed_desc or ""
            # Strip any URLs from title/desc before saving to description
            if _title:
                _title = _re_bulk.sub(r'\s*https?://\S+', '', _title).strip()
            if _title and (not item.get("description") or len(item.get("description", "")) < 10):
                item["description"] = _title
            # Apply parsed qty if we got one and item has no qty
            if _parsed_qty > 0 and (not item.get("qty") or item.get("qty") == 1):
                item["qty"] = _parsed_qty
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
                                item["pricing"]["amazon_price"] = price
                                item["pricing"]["amazon_title"] = amz[0].get("title", "")[:200]
                                # Switch entire item to Amazon source for easier ordering
                                item["item_link"] = amz[0].get("url", "") or f"https://www.amazon.com/dp/{_amz_asin}"
                                item["item_supplier"] = "Amazon"
                                url = item["item_link"]  # update url for pricing source_url
                                # Follow up with ASIN product lookup for list/sale price split
                                try:
                                    from src.agents.product_research import lookup_amazon_product
                                    _prod = lookup_amazon_product(_amz_asin)
                                    if _prod:
                                        if _prod.get("list_price"):
                                            item["pricing"]["list_price"] = _prod["list_price"]
                                            item["list_price"] = _prod["list_price"]
                                        if _prod.get("sale_price"):
                                            item["pricing"]["sale_price"] = _prod["sale_price"]
                                            item["sale_price"] = _prod["sale_price"]
                                except Exception:
                                    pass
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
                results.append({"line": _display_line, "url": url[:60], "status": _status,
                               "price": price, "supplier": r.get("supplier", ""),
                               "note": "Price from Amazon" if amazon_fallback else ""})
            else:
                results.append({"line": _display_line, "url": url[:60], "status": "linked",
                               "supplier": r.get("supplier", ""), "note": "URL linked, no price found"})
            applied += 1
        except Exception as e:
            results.append({"line": _display_line, "url": url[:60], "status": "error", "error": str(e)[:80]})
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
    # Finalize progress tracking
    with _BULK_SCRAPE_LOCK:
        if pcid in BULK_SCRAPE_STATUS:
            BULK_SCRAPE_STATUS[pcid].update({
                "running": False, "completed": len(urls),
                "applied": applied, "current_item": "Done",
                "results": results[:20],
            })
    return jsonify({"ok": True, "results": results, "applied": applied, "total": len(urls)})


# ── SSE streaming bulk scrape — real-time per-item progress ──

def _process_single_bulk_item(i, raw_line, items, pc, _line_labels, _re_bulk):
    """Process one bulk-scrape item. Returns (result_dict, was_applied, updated_item_or_None)."""
    raw_line = (raw_line or "").strip()
    raw_line = _re_bulk.sub(r'^\d+[\.\)]\s*', '', raw_line)
    _display_line = _line_labels.get(i, i + 1)
    if not raw_line or i >= len(items):
        return {"line": _display_line, "url": raw_line[:60], "status": "skipped"}, False, None

    _url_match = _re_bulk.search(r'(https?://\S+)', raw_line)
    if _url_match:
        url = _url_match.group(1).rstrip('.,;)')
        _pre = raw_line[:_url_match.start()].strip().rstrip(',')
    else:
        url = raw_line
        _pre = ""

    _parsed_desc = ""
    _parsed_mfg = ""
    _parsed_qty = 0
    if _pre and ',' in _pre:
        _parts = [p.strip() for p in _pre.split(',')]
        for _part in _parts:
            if not _part:
                continue
            if _re_bulk.match(r'^(SKU|MFG|PN|Item)\s*#?\s*\d', _part, _re_bulk.IGNORECASE):
                _parsed_mfg = _re_bulk.sub(r'^(SKU|MFG|PN|Item)\s*#?\s*', '', _part, flags=_re_bulk.IGNORECASE).strip()
            elif _re_bulk.match(r'^\d{1,4}$', _part) and not _parsed_qty:
                _v = int(_part)
                if _v > 0 and _v < 10000:
                    _parsed_qty = _v
            elif len(_part) > 10 and not _re_bulk.match(r'^(Each|EA|CS|BX|PK|Case|Box|Pack|DZ|CT)$', _part, _re_bulk.IGNORECASE):
                _parsed_desc = _part
    elif _pre and len(_pre) > 5:
        _parsed_desc = _pre

    try:
        from src.agents.item_link_lookup import lookup_from_url
        r = lookup_from_url(url)
        item = items[i]
        item["item_link"] = r.get("url", url)
        item["item_supplier"] = r.get("supplier", "")
        _pn = r.get("mfg_number") or r.get("part_number") or _parsed_mfg or ""
        if _pn:
            item["item_number"] = _pn
            item["mfg_number"] = _pn
        _title = r.get("title") or r.get("description") or _parsed_desc or ""
        if _title:
            _title = _re_bulk.sub(r'\s*https?://\S+', '', _title).strip()
        if _title and (not item.get("description") or len(item.get("description", "")) < 10):
            item["description"] = _title
        if _parsed_qty > 0 and (not item.get("qty") or item.get("qty") == 1):
            item["qty"] = _parsed_qty
        price = r.get("price") or r.get("list_price") or r.get("cost")
        if price:
            try:
                price = float(price)
            except (ValueError, TypeError):
                price = 0
        else:
            price = 0
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
                        _amz_asin = amz[0].get("asin", "")
                        if _amz_asin:
                            if not item.get("pricing"):
                                item["pricing"] = {}
                            item["pricing"]["amazon_asin"] = _amz_asin
                            item["pricing"]["amazon_url"] = amz[0].get("url", "")
                            item["pricing"]["amazon_price"] = price
                            item["pricing"]["amazon_title"] = amz[0].get("title", "")[:200]
                            item["item_link"] = amz[0].get("url", "") or f"https://www.amazon.com/dp/{_amz_asin}"
                            item["item_supplier"] = "Amazon"
                            url = item["item_link"]
                            try:
                                from src.agents.product_research import lookup_amazon_product
                                _prod = lookup_amazon_product(_amz_asin)
                                if _prod:
                                    if _prod.get("list_price"):
                                        item["pricing"]["list_price"] = _prod["list_price"]
                                        item["list_price"] = _prod["list_price"]
                                    if _prod.get("sale_price"):
                                        item["pricing"]["sale_price"] = _prod["sale_price"]
                                        item["sale_price"] = _prod["sale_price"]
                            except Exception:
                                pass
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
            return {"line": _display_line, "url": url[:60], "status": _status,
                    "price": price, "supplier": r.get("supplier", ""),
                    "note": "Price from Amazon" if amazon_fallback else ""}, True, item
        else:
            return {"line": _display_line, "url": url[:60], "status": "linked",
                    "supplier": r.get("supplier", ""), "note": "URL linked, no price found"}, True, item
    except Exception as e:
        return {"line": _display_line, "url": url[:60], "status": "error", "error": str(e)[:80]}, False, None


@bp.route("/api/pricecheck/<pcid>/bulk-scrape-urls-stream", methods=["POST"])
@auth_required
@safe_route
def api_bulk_scrape_urls_stream(pcid):
    """SSE streaming bulk scrape — sends per-item results as they resolve."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(force=True, silent=True) or {}
    items = pc.get("items", [])
    import re as _re_bulk

    # Parse input — same logic as non-streaming endpoint
    item_map = data.get("item_map")
    _line_labels = {}
    skipped_results = []
    if item_map and isinstance(item_map, dict):
        log.info("Bulk scrape SSE (structured): %s — %d item mappings", pcid, len(item_map))
        _item_idx = {}
        for idx_i, it in enumerate(items):
            _num = str(it.get("item_number", "")).strip()
            if _num:
                _item_idx[_num] = idx_i
            _ridx = str(it.get("row_index", "")).strip()
            if _ridx and _ridx not in _item_idx:
                _item_idx[_ridx] = idx_i
        _structured_map = {}
        for item_num, url in item_map.items():
            target_idx = _item_idx.get(str(item_num).strip())
            if target_idx is not None:
                _structured_map[target_idx] = (url, int(item_num))
                _line_labels[target_idx] = int(item_num)
            else:
                skipped_results.append({"line": int(item_num), "url": url[:60], "status": "skipped",
                                        "error": f"Item #{item_num} not found in PC"})
        urls = []
        for idx_j in range(len(items)):
            urls.append(_structured_map[idx_j][0] if idx_j in _structured_map else "")
    else:
        urls = data.get("urls", [])

    if not urls and not item_map:
        return jsonify({"ok": False, "error": "No URLs provided"})

    total = len(urls)
    # Count non-empty URLs for accurate progress
    real_count = sum(1 for u in urls if (u or "").strip())

    # Auto-reconcile MFG# from descriptions before scraping
    _mfg_fixes = _reconcile_mfg_from_descriptions(items)
    if _mfg_fixes:
        log.info("Bulk scrape SSE auto-reconcile: %d MFG# fixes for %s", len(_mfg_fixes), pcid)

    def generate():
        import json as _json_sse
        applied = 0
        processed = 0

        # Emit reconciliation fixes if any
        if _mfg_fixes:
            yield f"data: {_json_sse.dumps({'type': 'reconcile', 'fixes': _mfg_fixes, 'count': len(_mfg_fixes)})}\n\n"

        # Emit initial event with total count
        yield f"data: {_json_sse.dumps({'type': 'start', 'total': total, 'real_count': real_count})}\n\n"

        # Emit any pre-skipped items from structured mapping
        for sr in skipped_results:
            sr["type"] = "item"
            processed += 1
            sr["progress"] = processed
            yield f"data: {_json_sse.dumps(sr)}\n\n"

        for i, raw_line in enumerate(urls):
            result, was_applied, _updated = _process_single_bulk_item(
                i, raw_line, items, pc, _line_labels, _re_bulk
            )
            if was_applied:
                applied += 1
            processed += 1
            result["type"] = "item"
            result["progress"] = processed
            yield f"data: {_json_sse.dumps(result)}\n\n"

        # Save and sync catalog
        if applied:
            _save_single_pc(pcid, pc)
            try:
                from src.agents.product_catalog import save_pc_items_to_catalog
                cat_result = save_pc_items_to_catalog(pc)
                log.info("Bulk-scrape SSE catalog sync: added=%d existing=%d skipped=%d",
                         cat_result.get("added", 0), cat_result.get("existing", 0), cat_result.get("skipped", 0))
            except Exception as e:
                log.error("Bulk-scrape SSE catalog sync error: %s", e, exc_info=True)

        # Final done event
        yield f"data: {_json_sse.dumps({'type': 'done', 'applied': applied, 'total': total})}\n\n"

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


def _resolve_buyer_name(pc, buyer_email):
    """Resolve buyer display name from CRM contacts, falling back to email parse."""
    buyer_name = ""
    # Try CRM lookup first
    if buyer_email:
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT buyer_name FROM contacts WHERE LOWER(buyer_email)=LOWER(?) LIMIT 1",
                    (buyer_email,)
                ).fetchone()
                if row and row[0]:
                    # buyer_name may be "Last, First" or "First Last"
                    raw = row[0].strip()
                    if "," in raw:
                        parts = raw.split(",", 1)
                        buyer_name = parts[1].strip()  # First name from "Last, First"
                    else:
                        buyer_name = raw.split()[0]  # First name from "First Last"
        except Exception:
            pass
    # Fallback: parse from requestor field
    if not buyer_name:
        requestor = pc.get("requestor", "") or ""
        original = pc.get("original_sender", "") or ""
        # Prefer full email for parsing (has first.last pattern)
        if original and "." in original.split("@")[0]:
            buyer_name = original.split("@")[0].replace(".", " ").title()
        elif "@" in requestor:
            # "katrina.valencia@cdcr.ca.gov" → "Katrina Valencia"
            local = requestor.split("@")[0]
            buyer_name = local.replace(".", " ").replace("_", " ").title()
        elif requestor:
            # Strip any @Agency suffix: "Katrina@CDCR" → "Katrina"
            clean = requestor.split("@")[0].strip() if "@" in requestor else requestor
            buyer_name = clean.split()[0] if " " in clean else clean
    # Final cleanup: remove agency codes that snuck in
    if buyer_name and "@" in buyer_name:
        buyer_name = buyer_name.split("@")[0].strip()
    return buyer_name or "Team"


def _build_item_summary(pc, max_items=5):
    """Build a short text summary of quote line items for email body."""
    items = pc.get("items", [])
    lines = []
    for it in items[:max_items]:
        if it.get("no_bid"):
            continue
        desc = (it.get("description") or "")[:50]
        price = it.get("unit_price") or 0
        if not price:
            pricing = it.get("pricing") or {}
            price = pricing.get("recommended_price") or pricing.get("bid_price") or 0
        qty = it.get("qty", 1)
        if desc:
            try:
                lines.append(f"  - {desc} — Qty {qty} @ ${float(price):.2f}")
            except (ValueError, TypeError):
                lines.append(f"  - {desc} — Qty {qty}")
    if len(items) > max_items:
        remaining = len(items) - max_items
        lines.append(f"  ... and {remaining} more item{'s' if remaining != 1 else ''}")
    return "\n".join(lines)


def _build_pc_quote_email_body(pc, pcid, buyer_email):
    """Build a personalized quote email body using template + CRM data."""
    buyer_name = _resolve_buyer_name(pc, buyer_email)
    pc_number = pc.get("pc_number") or pc.get("reytech_quote_number") or pcid
    item_summary = _build_item_summary(pc)

    body = (
        f"Dear {buyer_name},\n\n"
        f"Please find attached our pricing response for {pc_number}.\n\n"
    )
    if item_summary:
        body += f"{item_summary}\n\n"
    body += (
        "Pricing is valid for 45 days from the date of this quote. "
        "Please don't hesitate to reach out with any questions.\n\n"
        "Thank you for the opportunity."
    )
    # No signature — Gmail auto-appends the configured Gmail signature
    return body


@bp.route("/api/pricecheck/<pcid>/send-quote", methods=["POST"])
@auth_required
@safe_route
def api_pc_send_quote(pcid):
    """Send the generated PC quote PDF via email."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.get_json(force=True, silent=True) or {}
    # Prefer original buyer email (from forwarded emails) over requestor fields
    to_email = data.get("to") or pc.get("original_sender") or pc.get("requestor_email", pc.get("requestor", ""))
    pc_num = pc.get("pc_number", pcid)
    # Build reply subject from original email subject
    import re as _re_subj
    orig_subject = pc.get("email_subject", "")
    if orig_subject and not data.get("subject"):
        clean_subj = _re_subj.sub(r'^(Re:\s*|Fwd?:\s*|FW:\s*)*', '', orig_subject, flags=re.IGNORECASE).strip()
        subject = f"Re: {clean_subj}" if clean_subj else f"Price Quote — {pc_num}"
    else:
        subject = data.get("subject") or f"Price Quote — {pc_num}"
    body_text = data.get("body") or _build_pc_quote_email_body(pc, pcid, to_email)

    if not to_email or "@" not in to_email:
        return jsonify({"ok": False, "error": "No valid recipient email"})

    # Find the latest generated PDF — check ALL possible locations
    pdf_path = ""
    qn = pc.get("reytech_quote_number", "")
    import re as _re
    safe = _re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    safe_id = _re.sub(r'[^a-zA-Z0-9_-]', '_', pcid.strip())

    # Priority 1: paths stored on the PC record
    for stored_path in [pc.get("output_pdf", ""), pc.get("reytech_quote_pdf", "")]:
        if stored_path and os.path.exists(stored_path):
            pdf_path = stored_path
            break

    # Priority 2: search by naming patterns
    if not pdf_path:
        candidates = [
            os.path.join(DATA_DIR, f"Quote_{safe}_Reytech.pdf"),
            os.path.join(DATA_DIR, f"PC_{safe}_Reytech.pdf"),
            os.path.join(DATA_DIR, f"Quote_{safe}_{safe_id}_Reytech.pdf"),
            os.path.join(DATA_DIR, f"PC_{safe}_{safe_id}_Reytech.pdf"),
        ]
        # Also search for any PDF with the PC ID in the filename
        try:
            for f in os.listdir(DATA_DIR):
                if f.endswith(".pdf") and (pcid in f or safe in f) and "Reytech" in f:
                    candidates.append(os.path.join(DATA_DIR, f))
        except Exception:
            pass
        for candidate in candidates:
            if os.path.exists(candidate):
                pdf_path = candidate
                log.info("SEND: Found PDF at %s", os.path.basename(candidate))
                break

    # Priority 3: rfq_files DB
    if not pdf_path:
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
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    if not pdf_path:
        log.warning("SEND %s: No PDF found. output_pdf=%s, quote_pdf=%s, safe=%s",
                     pcid, pc.get("output_pdf", ""), pc.get("reytech_quote_pdf", ""), safe)
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

        msg = MIMEMultipart("mixed")
        msg["From"] = f"Reytech Inc. <{gmail_user}>"
        msg["To"] = to_email
        msg["Subject"] = subject

        # Threading — reply in buyer's email thread
        email_message_id = pc.get("email_message_id", "")
        if email_message_id:
            msg["In-Reply-To"] = email_message_id
            msg["References"] = email_message_id

        # Plain text only — Gmail auto-appends the configured signature
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
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

        return jsonify({"ok": True, "sent_to": to_email, "quote": qn})
    except Exception as e:
        log.error("PC send-quote: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:200]})


@bp.route("/api/pricecheck/<pcid>/email-preview", methods=["GET"])
@auth_required
@safe_route
def api_pc_email_preview(pcid):
    """Return pre-built email body for the send-quote dialog."""
    try:
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})
        buyer_email = pc.get("original_sender") or pc.get("requestor_email", pc.get("requestor", ""))
        body = _build_pc_quote_email_body(pc, pcid, buyer_email)
        return jsonify({"ok": True, "body": body})
    except Exception as e:
        log.error("email-preview %s: %s", pcid, e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/pricecheck/<pcid>/duplicate", methods=["POST"])
@auth_required
@safe_route
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
@safe_route
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


# ═══════════════════════════════════════════════════════════════════════════════
# PC Visibility Diagnostics
# ═══════════════════════════════════════════════════════════════════════════════

_TERMINAL_STATUSES = ("dismissed", "archived", "deleted", "duplicate",
                      "no_response", "not_responding", "expired")


def _diagnose_pc_visibility(pc, pcid):
    """Run every homepage filter step and return a detailed verdict."""
    import json as _json

    status = pc.get("status", "new")
    is_terminal = status in _TERMINAL_STATUSES

    # Item count (mirrors _is_user_facing_pc logic)
    items = pc.get("items", [])
    items_type = type(items).__name__
    if isinstance(items, str):
        try:
            items = _json.loads(items)
            items_type = "json_string_parsed"
        except Exception:
            items = []
            items_type = "json_string_corrupt"
    item_count = len(items) if isinstance(items, list) else 0

    # Solicitation check
    sol = pc.get("solicitation_number", "") or pc.get("pc_number", "")
    has_sol = bool(sol and sol != "unknown")

    # Final verdict
    if is_terminal:
        visible = False
        reason = f"Status '{status}' is terminal — hidden from homepage"
    elif item_count > 0:
        visible = True
        reason = f"Has {item_count} items — visible"
    elif has_sol:
        visible = True
        reason = f"Has solicitation '{sol}' — visible despite 0 items"
    else:
        visible = False
        reason = "No items AND no solicitation number — hidden as empty ghost"

    return {
        "pc_id": pcid,
        "visible_on_homepage": visible,
        "reason": reason,
        "filters": {
            "status": status,
            "is_terminal": is_terminal,
            "item_count": item_count,
            "items_storage_type": items_type,
            "solicitation_number": sol or None,
            "has_valid_solicitation": has_sol,
        },
        "metadata": {
            "institution": pc.get("institution", ""),
            "agency": pc.get("agency", ""),
            "requestor": pc.get("requestor", ""),
            "source": pc.get("source", ""),
            "created_at": pc.get("created_at", ""),
            "converted_to_rfq": pc.get("converted_to_rfq", False),
            "linked_rfq_id": pc.get("linked_rfq_id", ""),
        },
    }


@bp.route("/api/pc/<pcid>/diagnostic")
@auth_required
@safe_route
def api_pc_diagnostic(pcid):
    """Explain exactly why a specific PC is visible or hidden on the homepage."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found", "pc_id": pcid}), 404
    result = _diagnose_pc_visibility(pc, pcid)
    result["ok"] = True
    return jsonify(result)


@bp.route("/api/pcs/diagnostic-all")
@auth_required
@safe_route
def api_pcs_diagnostic_all():
    """Show why each hidden PC is filtered out. Returns up to 50 entries."""
    from src.api.dashboard import _is_user_facing_pc
    pcs = _load_price_checks()
    hidden = []
    visible_count = 0
    for pid, pc in pcs.items():
        if _is_user_facing_pc(pc):
            visible_count += 1
        else:
            if len(hidden) < 50:
                hidden.append(_diagnose_pc_visibility(pc, pid))
    return jsonify({
        "ok": True,
        "total_pcs": len(pcs),
        "visible_count": visible_count,
        "hidden_count": len(pcs) - visible_count,
        "hidden_pcs": hidden,
    })


# ═══════════════════════════════════════════════════════════════════════
# Admin bulk maintenance — reparse empty PCs + PC/RFQ duplicate resolver
#
# 2026-04-12 context: 12 Valencia/CDCR PCs on prod had empty item lists
# because they were ingested with old parser code that couldn't handle
# Docusign-signed 704 PDFs. Mike had to ask me to `curl` the reparse
# endpoint for each one individually. These endpoints fix both issues:
#
#   POST /api/admin/reparse-empty-pcs        — batch reparse
#   POST /api/admin/resolve-pc-rfq-dupes     — dismiss PC side when the
#                                              same solicitation is in
#                                              both the PC and RFQ queues
# ═══════════════════════════════════════════════════════════════════════

def _pc_came_from_email(pc: dict) -> bool:
    """A PC qualifies for bulk reparse only if it originated from a real
    buyer email — we don't want to touch half-abandoned manual entries.
    """
    return bool(
        pc.get("email_subject")
        or pc.get("sender_email")
        or pc.get("original_sender")
        or pc.get("email_uid")
    )


def _source_pdf_exists(pc: dict, pcid: str) -> str:
    """Return an on-disk path to the PC's source PDF, recovering from the
    rfq_files / email_attachments tables if the stored path is stale.
    Returns empty string if the PDF cannot be located or restored.
    """
    source_pdf = pc.get("source_pdf", "") or ""
    if source_pdf and os.path.exists(source_pdf):
        return source_pdf
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT data, filename FROM rfq_files "
                "WHERE rfq_id=? AND category='source' ORDER BY id DESC LIMIT 1",
                (pcid,)
            ).fetchone()
            if not row:
                try:
                    row = conn.execute(
                        "SELECT data, filename FROM email_attachments "
                        "WHERE pc_id=? ORDER BY id DESC LIMIT 1",
                        (pcid,)
                    ).fetchone()
                except Exception:
                    row = None
            if row and row["data"]:
                restore_dir = os.path.join(DATA_DIR, "pc_pdfs")
                os.makedirs(restore_dir, exist_ok=True)
                restored = os.path.join(
                    restore_dir, row["filename"] or f"{pcid}.pdf"
                )
                with open(restored, "wb") as f:
                    f.write(row["data"])
                return restored
    except Exception as e:
        log.debug("reparse recovery for %s failed: %s", pcid, e)
    return ""


@bp.route("/api/admin/reparse-empty-pcs", methods=["POST"])
@auth_required
@safe_route
def api_admin_reparse_empty_pcs():
    """Reparse every PC that has zero items AND came from a real email.

    Query params:
      dry_run=1  — don't mutate anything, just return the candidate list
      limit=N    — cap the number of PCs to touch in this call (default 100)

    Use case: a batch of PCs was ingested with old parser code that
    failed on their layout. Current code works. One POST unsticks them
    all without having to curl /pricecheck/<id>/reparse in a loop.
    """
    dry_run = request.args.get("dry_run", "0") == "1"
    try:
        limit = int(request.args.get("limit", "100"))
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 500))

    try:
        from src.api.dashboard import _load_price_checks, _save_single_pc
        from src.api.config import PRICE_CHECK_AVAILABLE
        from src.forms.price_check import parse_ams704
    except Exception as e:
        return jsonify({"ok": False, "error": f"imports failed: {e}"}), 500

    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check module unavailable"}), 500

    pcs = _load_price_checks()

    candidates = []
    for pcid, pc in pcs.items():
        if not isinstance(pc, dict):
            continue
        if pc.get("status") in (
            "dismissed", "archived", "deleted", "sent", "won", "lost",
            "duplicate", "reclassified", "expired",
        ):
            continue
        items = pc.get("items") or []
        if len(items) > 0:
            continue
        if not _pc_came_from_email(pc):
            continue
        candidates.append((pcid, pc))
        if len(candidates) >= limit:
            break

    if dry_run:
        return jsonify({
            "ok": True,
            "dry_run": True,
            "would_reparse": len(candidates),
            "candidates": [
                {
                    "pc_id": pid,
                    "pc_number": p.get("pc_number", ""),
                    "subject": p.get("email_subject", "")[:100],
                    "sender": p.get("sender_email") or p.get("original_sender", ""),
                }
                for pid, p in candidates
            ],
        })

    results = {"reparsed": [], "skipped": [], "errors": []}
    for pcid, pc in candidates:
        source_pdf = _source_pdf_exists(pc, pcid)
        if not source_pdf:
            results["skipped"].append({
                "pc_id": pcid, "reason": "source PDF missing"
            })
            continue
        try:
            parsed = parse_ams704(source_pdf)
        except Exception as e:
            results["errors"].append({"pc_id": pcid, "error": str(e)[:200]})
            continue
        fresh_items = parsed.get("line_items") or []
        if not fresh_items:
            results["skipped"].append({
                "pc_id": pcid, "reason": "parser still returned 0 items"
            })
            continue
        # Merge the fresh parse back into the PC without destroying any
        # user-set top-level fields. Empty PCs by definition have no
        # user pricing, so we overwrite items + parsed block cleanly.
        pc["items"] = fresh_items
        pc["parsed"] = parsed
        pc["source_pdf"] = source_pdf
        pc["status"] = "parsed"
        pc["_reparsed_at"] = datetime.now(timezone.utc).isoformat()
        try:
            _save_single_pc(pcid, pc)
            results["reparsed"].append({
                "pc_id": pcid,
                "pc_number": pc.get("pc_number", ""),
                "items": len(fresh_items),
            })
            log.info("BULK REPARSE: %s → %d items", pcid, len(fresh_items))
        except Exception as e:
            results["errors"].append({"pc_id": pcid, "error": f"save failed: {e}"})

    return jsonify({
        "ok": True,
        "dry_run": False,
        "total_candidates": len(candidates),
        "reparsed_count": len(results["reparsed"]),
        "skipped_count": len(results["skipped"]),
        "error_count": len(results["errors"]),
        "results": results,
    })


def _normalize_sol(value) -> str:
    """Normalize a solicitation/pc number for equality comparison across
    PC and RFQ sides. Strips whitespace, lowercases, removes common
    prefixes/suffixes that differ between how PCs and RFQs store them."""
    if not value:
        return ""
    s = str(value).strip().lower()
    # Strip a leading '#' some templates prepend
    if s.startswith("#"):
        s = s[1:]
    return s


@bp.route("/api/admin/resolve-pc-rfq-dupes", methods=["POST"])
@auth_required
@safe_route
def api_admin_resolve_pc_rfq_dupes():
    """Find (PC, RFQ) pairs that point to the same solicitation and
    dismiss the PC side so the solicitation only appears once in the
    home queue (on the RFQ side, which is the richer data model).

    Query params:
      dry_run=1  — return matches without mutating
      keep=pc    — invert: dismiss the RFQ side instead of the PC
                   (default 'rfq' keeps the RFQ)

    2026-04-12 observed case: Drew Sims CalVet #26-04-003 appeared as
    BOTH auto_20260410_1775831475 (PC) AND 20260410_142935_19d77a (RFQ)
    simultaneously. This endpoint dismisses the PC clone automatically.
    """
    from src.api.dashboard import _load_price_checks, _save_single_pc, load_rfqs

    dry_run = request.args.get("dry_run", "0") == "1"
    keep_side = request.args.get("keep", "rfq").lower()
    if keep_side not in ("rfq", "pc"):
        keep_side = "rfq"

    pcs = _load_price_checks()
    rfqs = load_rfqs()

    # Build a sol → [pc_ids] index of ACTIVE PCs only
    _active_pc_statuses = {
        "new", "draft", "parsed", "parse_error", "priced", "ready",
        "auto_drafted", "quoted", "generated", "enriching", "enriched",
    }
    pc_by_sol: dict = {}
    for pid, pc in pcs.items():
        if not isinstance(pc, dict):
            continue
        if pc.get("status", "new") not in _active_pc_statuses:
            continue
        sol = _normalize_sol(
            pc.get("solicitation_number") or pc.get("pc_number", "")
        )
        if not sol:
            continue
        pc_by_sol.setdefault(sol, []).append(pid)

    # Scan RFQs for matching solicitations among the same active set
    _active_rfq_statuses = {
        "new", "draft", "ready", "generated", "parsed", "priced", "quoted"
    }
    matches = []
    for rid, rfq in rfqs.items():
        if not isinstance(rfq, dict):
            continue
        if rfq.get("status", "new") not in _active_rfq_statuses:
            continue
        sol = _normalize_sol(
            rfq.get("solicitation_number") or rfq.get("rfq_number", "")
        )
        if not sol or sol not in pc_by_sol:
            continue
        for pid in pc_by_sol[sol]:
            matches.append({
                "solicitation": sol,
                "rfq_id": rid,
                "pc_id": pid,
                "rfq_subject": rfq.get("email_subject", "")[:100],
                "pc_subject": pcs[pid].get("email_subject", "")[:100],
            })

    if dry_run:
        return jsonify({
            "ok": True,
            "dry_run": True,
            "keep_side": keep_side,
            "matches": matches,
            "would_dismiss": len(matches),
        })

    dismissed = []
    for m in matches:
        if keep_side == "rfq":
            target_id = m["pc_id"]
            target = pcs.get(target_id)
            if target:
                target["status"] = "dismissed"
                target["_dismissed_reason"] = (
                    f"duplicate of RFQ {m['rfq_id']} (sol {m['solicitation']})"
                )
                try:
                    _save_single_pc(target_id, target)
                    dismissed.append({"type": "pc", "id": target_id,
                                      "sol": m["solicitation"]})
                except Exception as e:
                    log.error("PC/RFQ dedupe save failed for %s: %s", target_id, e)
        else:
            target_id = m["rfq_id"]
            target = rfqs.get(target_id)
            if target:
                target["status"] = "dismissed"
                target["_dismissed_reason"] = (
                    f"duplicate of PC {m['pc_id']} (sol {m['solicitation']})"
                )
                try:
                    from src.api.dashboard import _save_single_rfq
                    _save_single_rfq(target_id, target)
                    dismissed.append({"type": "rfq", "id": target_id,
                                      "sol": m["solicitation"]})
                except Exception as e:
                    log.error("PC/RFQ dedupe save failed for %s: %s", target_id, e)

    return jsonify({
        "ok": True,
        "dry_run": False,
        "keep_side": keep_side,
        "dismissed_count": len(dismissed),
        "dismissed": dismissed,
        "total_matches": len(matches),
    })


@bp.route("/api/admin/email-rescue", methods=["POST"])
@auth_required
@safe_route
def api_admin_email_rescue():
    """Rescue a specific email that the poller silently dropped.

    POST body or query params:
      inbox  — "mike" or "sales" (default: mike)
      query  — Gmail search query, e.g. "subject:10841813" or
               "from:kevin.jensen@cdcr.ca.gov"
      limit  — max messages to inspect (default 20, hard cap 100)
      dry_run=1 — don't process, just return the matches

    For each matching message the endpoint:
      1. Clears any stale dedup fingerprint blocking it
      2. Removes the UID from processed_emails_<inbox>.json + the
         processed_emails SQLite table
      3. Calls the poller's per-message processing path so the email
         re-runs the classifier and (if accepted) reaches process_rfq_email
      4. Returns subject/sender/result for each message

    Built 2026-04-12 after Kevin Jensen's CDCR RFQ from Apr 10 was
    silently dropped by the cross-inbox dedup gate. Stops the
    "wait for the next poll cycle" workaround that didn't actually
    work because the same fingerprint kept blocking the email.
    """
    data = request.get_json(force=True, silent=True) or {}

    def _arg(key, default=""):
        return (data.get(key) if data.get(key) is not None
                else request.args.get(key, default))

    inbox_name = (_arg("inbox", "mike") or "mike").strip().lower()
    if inbox_name not in ("mike", "sales"):
        return jsonify({"ok": False, "error": "inbox must be 'mike' or 'sales'"}), 400
    query = (_arg("query", "") or "").strip()
    if not query:
        return jsonify({
            "ok": False,
            "error": "query is required (e.g. query=subject:10841813 or query=from:kevin.jensen@cdcr.ca.gov)"
        }), 400
    try:
        limit = int(_arg("limit", "20"))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(limit, 100))
    dry_run = str(_arg("dry_run", "0")).lower() in ("1", "true", "yes")

    # ── Connect to the right inbox ──
    try:
        from src.core.gmail_api import (
            get_service, list_message_ids, get_raw_message, get_message_metadata
        )
    except Exception as e:
        return jsonify({"ok": False, "error": f"gmail_api unavailable: {e}"}), 500

    try:
        service = get_service(inbox_name)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Gmail API connect failed: {e}"}), 502

    try:
        msg_ids = list_message_ids(service, query=query, max_results=limit)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Gmail search failed: {e}"}), 502

    if not msg_ids:
        return jsonify({
            "ok": True,
            "inbox": inbox_name,
            "query": query,
            "matches": 0,
            "results": [],
        })

    # ── Build a preview list (always returned) ──
    previews = []
    for mid in msg_ids:
        try:
            meta = get_message_metadata(service, mid)
            previews.append({
                "id": mid,
                "subject": (meta.get("subject") or "")[:120],
                "from": (meta.get("from") or "")[:120],
                "date": (meta.get("date") or "")[:40],
            })
        except Exception as e:
            previews.append({"id": mid, "error": str(e)[:120]})

    if dry_run:
        return jsonify({
            "ok": True,
            "inbox": inbox_name,
            "query": query,
            "dry_run": True,
            "matches": len(previews),
            "messages": previews,
        })

    # ── Clear blockers for each matching message and trigger a poll ──
    # Strategy: don't try to replicate check_for_rfqs's internal email-dict
    # building (it's tightly coupled to the poller class). Instead, clear
    # every cache that would prevent the email from being re-considered:
    #   - in-memory poller._processed set
    #   - JSON processed_emails_<inbox>.json file
    #   - SQLite processed_emails table (rows where inbox matches)
    #   - email_fingerprints rows for this message's fingerprint
    # Then trigger a manual poll — the poller will re-encounter the message
    # and run it through the full classifier + processing pipeline as if
    # it were brand new.
    try:
        from src.api.dashboard import _shared_poller
    except Exception:
        _shared_poller = None

    proc_file = os.path.join(
        DATA_DIR,
        "processed_emails.json" if inbox_name == "sales" else "processed_emails_mike.json",
    )

    cleared = {"json": 0, "sqlite": 0, "fingerprint": 0, "in_memory": 0}
    for preview in previews:
        mid = preview.get("id")
        if not mid:
            continue
        # 1. Remove from in-memory _processed if the running poller is for
        # this inbox. Different inbox? Skip — it's a different set.
        try:
            if _shared_poller and getattr(_shared_poller, "_inbox_name", "") == inbox_name:
                if mid in _shared_poller._processed:
                    _shared_poller._processed.discard(mid)
                    cleared["in_memory"] += 1
        except Exception:
            pass
        # 2. Remove from JSON processed file
        try:
            if os.path.exists(proc_file):
                with open(proc_file) as f:
                    uids = json.load(f)
                if isinstance(uids, list) and mid in uids:
                    uids.remove(mid)
                    with open(proc_file, "w") as f:
                        json.dump(uids, f)
                    cleared["json"] += 1
        except Exception as e:
            log.debug("rescue clear json (%s): %s", mid, e)
        # 3. Remove from SQLite processed_emails for the right inbox
        try:
            with get_db() as conn:
                cur = conn.execute(
                    "DELETE FROM processed_emails WHERE uid=? AND inbox=?",
                    (mid, inbox_name),
                )
                if cur.rowcount:
                    cleared["sqlite"] += cur.rowcount
                conn.commit()
        except Exception as e:
            log.debug("rescue clear sqlite (%s): %s", mid, e)
        # 4. Clear stale fingerprint (the silent-skip blocker)
        try:
            from src.api.modules.routes_catalog_finance import _email_fingerprint
            fp = _email_fingerprint(
                preview.get("subject", ""),
                preview.get("from", ""),
                preview.get("date", ""),
            )
            with get_db() as conn:
                cur = conn.execute(
                    "DELETE FROM email_fingerprints WHERE fingerprint=?", (fp,)
                )
                if cur.rowcount:
                    cleared["fingerprint"] += cur.rowcount
                conn.commit()
        except Exception as e:
            log.debug("rescue clear fingerprint: %s", e)

    # ── Trigger a fresh poll now so the cleared messages get reprocessed ──
    poll_result = {"triggered": False}
    try:
        from src.api.dashboard import do_poll_check
        imported = do_poll_check()
        poll_result = {
            "triggered": True,
            "imported_count": len(imported) if imported else 0,
        }
    except Exception as e:
        poll_result = {"triggered": False, "error": str(e)[:200]}
        log.warning("rescue: poll trigger failed: %s", e)

    return jsonify({
        "ok": True,
        "inbox": inbox_name,
        "query": query,
        "matches": len(previews),
        "messages": previews,
        "cleared": cleared,
        "poll_result": poll_result,
        "next_step": (
            "Check the homepage or run /api/diag/find-rfq?q=<sol_or_subject> "
            "to confirm the rescued email produced a record. If not, the "
            "classifier rejected it — check Railway logs for "
            "'is_rfq_email' decisions on these subjects."
        ),
    })


@bp.route("/api/admin/clear-tentative-fingerprints", methods=["POST"])
@auth_required
@safe_route
def api_admin_clear_tentative_fingerprints():
    """Delete every email_fingerprints row that has no result_type — these
    are leftovers from the buggy pre-2026-04-12 code path where every
    first-pass scan locked in a fingerprint regardless of downstream
    success. Safe to run anytime; one-shot fix for the silent-skip bug.
    """
    try:
        from src.api.modules.routes_catalog_finance import clear_tentative_fingerprints
        removed = clear_tentative_fingerprints()
        return jsonify({"ok": True, "removed": removed})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
