# routes_pricecheck_pricing.py — Pricing Oracle API, PC Lifecycle, Award Monitor, Competitors
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

# ═══════════════════════════════════════════════════════════════════════
# Pricing Oracle API (v6.0)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricing/recommend", methods=["POST"])
@auth_required
@safe_route
def api_pricing_recommend():
    """Get pricing recommendations for an RFQ's line items (V2 oracle)."""
    data = request.get_json(force=True, silent=True) or {}
    rid = data.get("rfq_id")

    source = data
    if rid:
        rfqs = load_rfqs()
        rfq = rfqs.get(rid)
        if not rfq:
            return jsonify({"error": f"RFQ {rid} not found"}), 404
        source = rfq

    # Feature-flagged: V2 oracle (default ON), V1 fallback
    from src.core.feature_flags import get_flag
    if get_flag("pricing_v2", default=True):
        from src.core.pricing_oracle_v2 import get_pricing
        items_data = source.get("line_items", [])
        agency = source.get("agency", data.get("agency", "CCHCS"))
        # BUILD-2: total line count narrows the volume-aware band. A
        # qty=1 line in a 20-line quote competes differently than the
        # same line in a 2-line quote — pass len(items_data) so the
        # oracle hits the right (agency, qty, line_count) cell.
        line_count = len(items_data)
        priced = []
        for item in items_data:
            r = get_pricing(
                description=item.get("description", ""),
                quantity=item.get("qty", 1) or 1,
                cost=item.get("supplier_cost") or item.get("price_per_unit"),
                item_number=item.get("item_number", ""),
                department=agency,
                line_count=line_count,
                upc=item.get("upc", ""),
            )
            priced.append(r)
        result = {
            "rfq_id": source.get("solicitation_number", rid or ""),
            "agency": agency,
            "items": priced,
            "summary": {
                "total_items": len(priced),
                "priced": sum(1 for p in priced if (p.get("recommendation") or {}).get("quote_price")),
            }
        }
    else:
        if not PRICING_ORACLE_AVAILABLE:
            return jsonify({"error": "Pricing oracle not available"}), 503
        result = recommend_prices_for_rfq(source, config_overrides=data.get("config"))

    return jsonify(result)


# ─────────────────────────────────────────────────────────────────
# Phase 3 (2026-04-25): quote-wide markup recommendation
#
# When operator opens a PC, oracle queries winning_prices for prior
# operator-confirmed wins at this agency (or parent agency class) and
# recommends a single quote-wide markup %. The PC UI surfaces this as a
# chip with an "Apply to all rows" button. Closes the second half of the
# Barstow loss (Phase 1+2 fixed cost; auto_processor's hard-coded 25%
# default is what made operator re-key 35% on every row).
#
# Provenance discipline: only reads winning_prices.recorded_at >=
# '2026-04-25' (Phase 1 ship date) AND filters outlier margins outside
# [5, 60] AND uses median (not mean) so a single $50k 75%-markup outlier
# can't pull the recommendation. See pricing_oracle_v2.recommend_quote_markup.
# ─────────────────────────────────────────────────────────────────

@bp.route("/api/pricecheck/<pcid>/markup-recommendation", methods=["GET"])
@auth_required
@safe_route
def api_pc_markup_recommendation(pcid):
    """Recommend a quote-wide markup % for this PC based on prior wins
    at the same agency (or agency class as fallback)."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"}), 404

    agency = (pc.get("agency") or pc.get("institution") or "").strip()
    if not agency:
        return jsonify({"ok": True, "markup_pct": None, "confidence": "low",
                        "rationale": "PC has no agency — operator default"})

    # Resolve parent agency class via facility_registry. Barstow → CalVet,
    # CSP-SAC → CDCR, etc. Falls back to None if the PC's facility isn't
    # in the registry (older PCs predating the registry).
    parent_agency = None
    try:
        agency_key = pc.get("agency_key") or ""
        if agency_key:
            from src.core.facility_registry import resolve_by_agency_key
            facility = resolve_by_agency_key(agency_key)
            if facility:
                parent_agency = (getattr(facility, "parent_agency", "") or "").strip() or None
    except Exception as e:
        log.debug("markup-rec parent_agency resolve: %s", e)

    try:
        from src.core.pricing_oracle_v2 import recommend_quote_markup
    except Exception as e:
        log.debug("markup-rec import: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

    result = recommend_quote_markup(agency, items=pc.get("items"),
                                    parent_agency=parent_agency)
    result["pcid"] = pcid
    result["agency"] = agency
    result["parent_agency"] = parent_agency
    return jsonify(result)


@bp.route("/api/won-quotes/search")
@auth_required
@safe_route
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
@safe_route
def api_won_quotes_stats():
    """Get Won Quotes KB statistics and pricing health check."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    stats = get_kb_stats()
    health = pricing_health_check()
    return jsonify({"stats": stats, "health": health})


@bp.route("/api/won-quotes/dump")
@auth_required
@safe_route
def api_won_quotes_dump():
    """Debug: show first 10 raw KB records to verify what's stored."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import load_won_quotes
    quotes = load_won_quotes()
    # `tokens` is a set in-memory (the matcher does set intersection on it);
    # listify only here, at the JSON boundary, so the dump serializes.
    sample = [{**q, "tokens": sorted(q.get("tokens") or [])} for q in quotes[:10]]
    return jsonify({"total": len(quotes), "first_10": sample})


@bp.route("/api/admin/won-quotes/diagnostic")
@auth_required
@safe_route
def api_won_quotes_diagnostic():
    """READ-ONLY. Quantify the existing-row bloat/corruption (ISSUE-3,
    2026-05-29 audit) without touching a single row."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import diagnose_bloat
    return jsonify({"ok": True, "diagnostic": diagnose_bloat()})


@bp.route("/api/admin/won-quotes/price-quality")
@auth_required
@safe_route
def api_won_quotes_price_quality():
    """READ-ONLY. Investigate the post-repair avg-$71k/max-$1M unit_price tail —
    genuine per-unit data, or line-totals stored as unit_price? (ISSUE-3
    follow-up, 2026-05-29.)"""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import diagnose_price_quality
    return jsonify({"ok": True, "diagnostic": diagnose_price_quality()})


@bp.route("/api/admin/won-quotes/commodity-coverage")
@auth_required
@safe_route
def api_won_quotes_commodity_coverage():
    """READ-ONLY. Verify (before any purge) how scoping the KB to commodity-only
    would split the existing rows — keep vs purge by price bucket — so we don't
    gut legitimate commodity rows. `?n=` overrides the per-bucket sample size."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    try:
        n = int(request.args.get("n", 300))
    except (TypeError, ValueError):
        n = 300
    from src.knowledge.won_quotes_db import diagnose_commodity_coverage
    return jsonify({"ok": True, "diagnostic": diagnose_commodity_coverage(sample_per_bucket=n)})


@bp.route("/api/admin/won-quotes/acq-type-coverage")
@auth_required
@safe_route
def api_won_quotes_acq_type_coverage():
    """READ-ONLY. Check whether scprs_po_master.acq_type cleanly isolates the
    non-product awards (grants/IAs/service contracts) vs commodity products —
    the narrower scoping signal after reytech_sells proved too blunt."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import diagnose_acq_type_coverage
    return jsonify({"ok": True, "diagnostic": diagnose_acq_type_coverage()})


@bp.route("/api/admin/won-quotes/scope-to-goods", methods=["POST"])
@auth_required
@safe_route
def api_won_quotes_scope_to_goods():
    """Purge non-product (Services/grants/IAs/leases/encumbrance) rows from the
    won_quotes KB, scoping it to GOODS. Dry-run by default; real purge needs
    BOTH confirm=scope_to_goods AND dry_run=0. Keeps Goods/Telecom/unmatched
    rows. Idempotent."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    confirm = (request.args.get("confirm") or request.form.get("confirm") or "")
    dry_run = (request.args.get("dry_run") or request.form.get("dry_run") or "1") not in ("0", "false", "False")
    if not dry_run and confirm != "scope_to_goods":
        return jsonify({"ok": False,
                        "error": "real purge requires confirm=scope_to_goods"}), 400
    from src.knowledge.won_quotes_db import repair_noncommodity_scope
    return jsonify({"ok": True, "result": repair_noncommodity_scope(dry_run=dry_run)})


@bp.route("/api/admin/oracle/match-debug")
@auth_required
@safe_route
def api_oracle_match_debug():
    """READ-ONLY. Run the oracle's per-item market searches for a raw
    description and return the matched rows (description, per-unit, supplier,
    source) — to see exactly which rows a query pulls and why (e.g. why an IT
    laptop shows as a competitor for a paper notebook). `?desc=` required,
    `?item_number=` optional."""
    desc = request.args.get("desc", "").strip()
    if not desc:
        return jsonify({"ok": False, "error": "desc query param required"}), 400
    item_number = request.args.get("item_number", "").strip()
    from src.core.db import get_db
    from src.core import pricing_oracle_v2 as ov2
    out = {"desc": desc, "item_number": item_number, "tokens": ov2._tokenize(desc),
           "sources": {}}
    searches = [
        ("won_quotes", ov2._search_won_quotes),
        ("winning_prices", ov2._search_winning_prices),
        ("scprs_catalog", ov2._search_scprs_catalog),
    ]
    with get_db() as db:
        for name, fn in searches:
            try:
                rows = fn(db, desc, item_number)
                rows_sorted = sorted(rows, key=lambda r: r.get("price", 0), reverse=True)
                out["sources"][name] = {
                    "n": len(rows),
                    "rows": [{
                        "per_unit": round(r.get("price", 0), 2),
                        "desc": (r.get("description") or "")[:70],
                        "supplier": (r.get("supplier") or "")[:30],
                        "match_basis": r.get("match_basis"),
                        "po": r.get("po_number", ""),
                    } for r in rows_sorted[:12]],
                }
            except Exception as e:
                out["sources"][name] = {"error": f"{type(e).__name__}: {e}"}
    return jsonify({"ok": True, "debug": out})


@bp.route("/api/admin/quotes/misbid-audit")
@auth_required
@safe_route
def api_quotes_misbid_audit():
    """READ-ONLY. Did the cost-contamination bugs (SCPRS-as-cost #1247, the
    line-number cross-match #1252/#1255/#1259) cause any ALREADY-SENT/WON quote
    to bid off a wrong cost? The true contamination signature is a CHEAP
    commodity line (notebook/ball/poster/card/journal…) carrying a high cost —
    a $2 notebook reading $130. So we flag: (a) a cheap-commodity description
    with cost >= cheap_cost (default $40); (b) any non-positive margin (bid <=
    cost); (c) very high absolute cost (>= min_cost, default $1500) as a coarse
    backstop. Cheap-commodity flags sort first. Returns a sample of line-item
    keys so the bid/cost fields are auditable. Read-only; no mutation."""
    def _f(args, key, default):
        try:
            return float(request.args.get(key, default))
        except (TypeError, ValueError):
            return float(default)
    min_cost = _f(request.args, "min_cost", 1500)
    cheap_cost = _f(request.args, "cheap_cost", 40)
    CHEAP = ("notebook", "journal", "poster", "ball", " card", "pencil", "marker",
             "crayon", "sticker", "magnet", "ornament", "puzzle", "velvet art",
             "scratch art", "gratitude", " toy", "game", "craft", "eraser",
             "construction paper", "coloring", "mandala", "sketch")
    BID_FIELDS = ("price_per_unit", "our_price", "sell_price", "unit_price",
                  "price", "bid_price", "quoted_price", "extended_price")
    from src.core.db import get_db
    try:
        from src.core.pricing_math import cost_from_contract
    except Exception:
        cost_from_contract = None
    flagged = []
    scanned = 0
    sample_keys = None
    with get_db() as conn:
        rows = conn.execute(
            "SELECT quote_number, status, created_at, line_items FROM quotes "
            "WHERE is_test=0 AND status IN ('sent','won','pc_sent','pc_won')"
        ).fetchall()
        for r in rows:
            scanned += 1
            try:
                items = json.loads(r["line_items"] or "[]")
            except Exception:
                continue
            for it in items if isinstance(items, list) else []:
                if not isinstance(it, dict):
                    continue
                if sample_keys is None:
                    sample_keys = sorted(it.keys())
                if cost_from_contract:
                    cost = float(cost_from_contract(it) or 0)
                else:
                    cost = float(it.get("supplier_cost")
                                 or (it.get("pricing") or {}).get("unit_cost")
                                 or it.get("cost") or 0)
                bid = 0.0
                for bf in BID_FIELDS:
                    v = it.get(bf)
                    if v:
                        try:
                            bid = float(v)
                            break
                        except (TypeError, ValueError):
                            pass
                descl = (it.get("description") or "").lower()
                reason = None
                if cost >= cheap_cost and any(k in descl for k in CHEAP):
                    reason = "CHEAP-COMMODITY w/ elevated cost (contamination signature)"
                elif bid > 0 and cost > 0 and bid <= cost:
                    reason = "non-positive margin (bid <= cost)"
                elif cost >= min_cost:
                    reason = f"very high cost (>= ${min_cost:.0f})"
                if reason:
                    flagged.append({
                        "quote": r["quote_number"], "status": r["status"],
                        "date": (r["created_at"] or "")[:10],
                        "desc": (it.get("description") or "")[:55],
                        "cost": round(cost, 2), "bid": round(bid, 2),
                        "markup_pct": round((bid - cost) / cost * 100, 1) if cost > 0 else None,
                        "reason": reason,
                    })
    flagged.sort(key=lambda x: (0 if "CHEAP" in x["reason"] else
                                1 if "margin" in x["reason"] else 2, -(x["cost"] or 0)))
    return jsonify({"ok": True, "quotes_scanned": scanned,
                    "thresholds": {"min_cost": min_cost, "cheap_cost": cheap_cost},
                    "sample_line_keys": sample_keys,
                    "n_flagged": len(flagged),
                    "n_cheap_commodity": sum(1 for f in flagged if "CHEAP" in f["reason"]),
                    "flagged": flagged[:150]})


@bp.route("/api/admin/won-quotes/repair", methods=["POST"])
@auth_required
@safe_route
def api_won_quotes_repair():
    """Collapse the existing won_quotes bloat onto the canonical id scheme.

    Dry-run by default — returns the plan without writing. To actually
    repair, pass BOTH confirm=collapse_bloat AND dry_run=0. Idempotent:
    a second real run is a no-op. PR #1228 must be live first (the writers
    must be converged or the bloat regrows next harvest cycle)."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    confirm = (request.args.get("confirm") or request.form.get("confirm") or "")
    dry_run = (request.args.get("dry_run") or request.form.get("dry_run") or "1") not in ("0", "false", "False")
    if not dry_run and confirm != "collapse_bloat":
        return jsonify({"ok": False,
                        "error": "real repair requires confirm=collapse_bloat"}), 400
    from src.knowledge.won_quotes_db import repair_existing_rows
    return jsonify({"ok": True, "result": repair_existing_rows(dry_run=dry_run)})


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
@safe_route
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
@safe_route
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
@safe_route
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
@safe_route
def api_won_quotes_seed_status():
    """Check progress of bulk SCPRS seed job."""
    try:
        from src.agents.scprs_lookup import SEED_STATUS
        return jsonify(SEED_STATUS)
    except Exception as e:
        return jsonify({"error": str(e)})


@bp.route("/api/pricecheck/<pcid>/dismiss", methods=["POST"])
@auth_required
@safe_route
def api_pricecheck_dismiss(pcid):
    """Dismiss a PC from the active queue with a reason.
    Keeps data for SCPRS intelligence. reason=delete does hard delete.
    Valid reasons: dismissed, archived, duplicate, no_response, delete

    Race-safe wrapper (PR #778 pattern). RLock is re-entrant so the
    delete branch (which calls `api_pricecheck_delete` and re-acquires
    the lock) is safe.
    """
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        return _api_pricecheck_dismiss_locked(pcid)


def _api_pricecheck_dismiss_locked(pcid):
    """Inner body — always runs under `_save_pcs_lock`."""
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
    # Use the reason as the status directly for known actions.
    # Narrow set on purpose: this endpoint is reached via the
    # "Did not respond / Archive / Duplicate" UI buttons, not arbitrary
    # status changes. See src/core/status_taxonomy.PC_DISMISSAL_STATUSES.
    from src.core.status_taxonomy import PC_DISMISSAL_STATUSES
    # Map UI reasons to appropriate statuses
    _reason_map = {"cs_question": "dismissed", "other": "dismissed"}
    new_status = _reason_map.get(reason, reason) if reason not in PC_DISMISSAL_STATUSES else reason
    if new_status not in PC_DISMISSAL_STATUSES:
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
            log.warning("SCPRS queue for dismissed PC failed: %s", e)
    
    return jsonify({
        "ok": True,
        "dismissed": pcid,
        "reason": reason,
        "scprs_queued": scprs_queued,
    })


@bp.route("/api/pricecheck/<pcid>/delete", methods=["GET", "POST"])
@auth_required
@safe_route
def api_pricecheck_delete(pcid):
    """Delete a price check by ID. Also removes linked quote draft and recalculates counter.

    Race-safe wrapper (PR #778 pattern).
    """
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        return _api_pricecheck_delete_locked(pcid)


def _api_pricecheck_delete_locked(pcid):
    """Inner body — always runs under `_save_pcs_lock`."""
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
@safe_page
def pricecheck_redirect():
    """Redirect /pricecheck → /pricechecks (common typo/nav issue)"""
    return redirect("/pricechecks")


@bp.route("/pricechecks/today")
@auth_required
@safe_page
def pricechecks_today():
    """Today's price checks — batch review dashboard."""
    from src.api.render import render_page
    pcs = _load_price_checks()

    # Get PCs from last 48h, sorted by creation time
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(hours=48)).isoformat()
    recent = []
    for pcid, pc in pcs.items():
        created = pc.get("created_at", "")
        if created >= cutoff or pc.get("status") in ("new", "parsed"):
            # Compute readiness
            items = pc.get("items", [])
            active = [it for it in items if not it.get("no_bid")]
            total = len(active)
            costed = sum(1 for it in active if (it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0) > 0)
            priced = sum(1 for it in active if (it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0) > 0)

            recent.append({
                "id": pcid,
                "pc_number": pc.get("pc_number", pcid),
                "institution": pc.get("institution", ""),
                "requestor": pc.get("requestor", ""),
                "status": pc.get("status", "new"),
                "created_at": created[:16] if created else "",
                "due_date": pc.get("due_date", ""),
                "total_items": total,
                "costed": costed,
                "priced": priced,
                "pct": round(priced / total * 100) if total > 0 else 0,
                "enrichment_status": pc.get("enrichment_status", ""),
            })

    # Sort: needs attention first (lowest pct), then by creation time
    recent.sort(key=lambda x: (x["pct"], x["created_at"]))

    return render_page("pc_batch.html", active_page="Today", pcs=recent)


@bp.route("/pricechecks")
@auth_required
@safe_page
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

    # Map internal statuses → display statuses. `needs_review` is the
    # PR-A triage bucket — folded into 'new' here so it shows up in the
    # New tab; the orange badge in the row UI distinguishes it from
    # genuinely new records.
    DISPLAY_STATUS = {
        "new": "new", "parsed": "new", "parse_error": "new", "needs_review": "new",
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
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
        # Build rich search index with all visible fields
        search_index = f"{p['pc_number'].lower()} {p['institution'].lower()} {p['requestor'].lower()} {qn.lower()} {p['display_status']} {badge_label.lower()} {due_str} {date_str}"
        # Overdue detection
        is_overdue = False
        try:
            if p.get("due_date") and p["due_date"][:10] < datetime.now().strftime("%Y-%m-%d") and st not in ('sent','won','lost','archived','no_response','duplicate','dismissed'):
                is_overdue = True
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        overdue_style = "border-left:3px solid #f85149;" if is_overdue else ""
        due_color = "#f85149;font-weight:700" if is_overdue else "var(--r-text-muted)"
        rows += f'''<tr data-status="{p['display_status']}" data-search="{search_index}" data-id="{p['id']}" style="cursor:pointer;{overdue_style}" onclick="if(!event.target.closest('input,button'))location.href='/pricecheck/{p['id']}'">
         <td style="padding:8px 6px;text-align:center" onclick="event.stopPropagation()"><input type="checkbox" class="pc-bulk-check" value="{p['id']}" onchange="updateBulkBar()" style="width:16px;height:16px;cursor:pointer"></td>
         <td style="padding:14px 12px"><a href="/pricecheck/{p['id']}" style="color:var(--r-accent);font-family:'JetBrains Mono',monospace;font-weight:700;font-size:15px">#{p['pc_number']}</a></td>
         <td style="padding:14px 12px;font-size:15px;font-weight:500">{p['institution']}</td>
         <td style="padding:14px 12px;font-size:15px">{p['requestor'][:30]}</td>
         <td style="padding:14px 12px;font-size:15px;font-family:'JetBrains Mono',monospace;color:{due_color}">{due_str}{' 🔴' if is_overdue else ''}</td>
         <td style="padding:14px 12px;font-size:15px;font-family:'JetBrains Mono',monospace;color:var(--r-text-muted)">{date_str}</td>
         <td style="padding:14px 12px;text-align:center;font-size:16px;font-weight:700">{p['items_count']}</td>
         <td style="padding:14px 12px;text-align:right;font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace">{total_str}</td>
         <td style="padding:14px 12px;text-align:center">{f'<span style="color:var(--r-accent);font-family:JetBrains Mono,monospace;font-weight:700;font-size:14px">{qn}</span>' if qn else chr(8212)}</td>
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
      <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--r-accent)">{total}</div><div style="font-size:14px;color:var(--r-text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Total</div></div>
      <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:var(--r-accent)">{total_new}</div><div style="font-size:14px;color:var(--r-text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">New</div></div>
      <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#fbbf24">{total_draft}</div><div style="font-size:14px;color:var(--r-text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Draft</div></div>
      <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#3fb950">{total_sent}</div><div style="font-size:14px;color:var(--r-text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Sent</div></div>
      <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:10px;padding:16px 28px;text-align:center;min-width:100px">
        <div style="font-size:32px;font-weight:800;font-family:'JetBrains Mono',monospace;color:#f85149">{total_not_responding}</div><div style="font-size:14px;color:var(--r-text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:.5px">Not Responding</div></div>
    </div>
    <div style="display:flex;gap:10px;margin-bottom:14px;align-items:center">
      <input id="pc-search" placeholder="🔍 Search PC#, institution, requestor, status..." oninput="filterPCs()" style="flex:1;padding:10px 16px;background:var(--r-surface);border:1px solid var(--r-border);border-radius:8px;color:var(--r-text);font-size:16px">
      <select id="pc-status" onchange="filterPCs()" style="padding:10px 14px;background:var(--r-surface);border:1px solid var(--r-border);border-radius:8px;color:var(--r-text);font-size:15px">
        <option value="">All Statuses</option>{status_options}</select>
      <span id="pc-count" style="font-size:15px;color:var(--r-text-muted);white-space:nowrap">{total} PCs</span>
    </div>
    <div id="bulk-bar" style="display:none;align-items:center;gap:12px;padding:8px 16px;background:rgba(16,185,129,.08);border:1px solid rgba(16,185,129,.25);border-radius:8px;margin-bottom:8px">
      <span id="bulk-count" style="font-size:14px;font-weight:600;color:var(--r-accent)">0 selected</span>
      <button onclick="bulkAction('archived')" style="padding:4px 12px;background:#21262d;border:1px solid #30363d;border-radius:6px;color:#8b949e;font-size:13px;cursor:pointer">🗄️ Archive</button>
      <button onclick="bulkAction('duplicate')" style="padding:4px 12px;background:#21262d;border:1px solid #30363d;border-radius:6px;color:#8b949e;font-size:13px;cursor:pointer">📋 Duplicate</button>
      <button onclick="bulkAction('delete')" style="padding:4px 12px;background:#21262d;border:1px solid #30363d;border-radius:6px;color:#f85149;font-size:13px;cursor:pointer">🗑 Delete</button>
    </div>
    <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:10px;overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <thead><tr style="border-bottom:2px solid var(--r-border);text-transform:uppercase;font-size:14px;color:var(--r-text-muted);letter-spacing:.5px">
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
@safe_route
def api_pricechecks_list():
    """API: List all PCs with optional status filter. Add ?debug=1 for filter diagnostics."""
    pcs = _load_price_checks()
    status_filter = request.args.get("status", "")
    debug = request.args.get("debug", "")
    from src.api.dashboard import _is_user_facing_pc
    result = []
    for pcid, pc in pcs.items():
        if status_filter and pc.get("status", "new") != status_filter:
            continue
        entry = {"id": pcid, "pc_number": pc.get("pc_number", "?"),
            "institution": pc.get("institution", ""), "status": pc.get("status", "new"),
            "items_count": len(pc.get("items", [])), "quote_number": pc.get("reytech_quote_number", ""),
            "created_at": pc.get("created_at", ""), "competitor_name": pc.get("competitor_name", "")}
        if debug:
            entry["_source"] = pc.get("source", "")
            entry["_is_auto_draft"] = pc.get("is_auto_draft", False)
            entry["_rfq_id"] = pc.get("rfq_id", "")
            entry["_user_facing"] = _is_user_facing_pc(pc)
        result.append(entry)
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"ok": True, "pcs": result, "count": len(result)})


@bp.route("/api/pricecheck/<pcid>/mark-sent", methods=["POST"])
@auth_required
@safe_route
def api_pricecheck_mark_sent(pcid):
    """Mark PC as sent — creates versioned document record in DB.

    Concurrency: load → mutate → save under `_save_pcs_lock` so the
    final autosave's pricing edits aren't lost when mark-sent races
    against them. (Same race shape as PR #778.)
    """
    data = request.get_json(force=True, silent=True) or {}
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        pcs = _load_price_checks()
        if pcid not in pcs: return jsonify({"ok": False, "error": "PC not found"})
        pc = pcs[pcid]

        now = datetime.now().isoformat()
        # PR #11 substrate-singleness: single writer for 'sent'.
        from src.core.quote_lifecycle_shared import mark_sent_in_place
        mark_sent_in_place(
            pc, sent_at=now,
            sent_to=data.get("sent_to", pc.get("requestor", "")),
            sent_method=data.get("method", "email"),
            notes=data.get("notes", "704 sent to requestor"),
            source="user",
        )
        pc["award_status"] = "pending"

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

    _log_crm_activity(pc.get("reytech_quote_number", pcid), "quote_sent",
        f"Quote sent for PC #{pc.get('pc_number','')} to {pc.get('institution','')}", actor="user")

    # propagate to quotes table now handled INSIDE mark_sent_in_place
    # above (PR #11 substrate-singleness — single writer for 'sent').

    # PR-U (2026-05-13): fire drift + shadow drift logs on the canonical
    # operator Mark-Sent path. Pre-PR-U the substrate only logged on
    # /send-quote (which Mike's workflow doesn't use), so PR-S
    # auto-recommendations had near-zero input data even with weeks of
    # operator activity. Best-effort — never blocks the mark-sent flip.
    try:
        from src.core.operator_kpi import fire_drift_logs_on_send
        fire_drift_logs_on_send(pcid, "pc", pc)
    except Exception as _drift_e:
        log.debug("fire_drift_logs_on_send (mark-sent) suppressed: %s", _drift_e)

    log.info("PC %s marked SENT: pc#=%s institution=%s doc_id=%s",
             pcid, pc.get("pc_number"), pc.get("institution"), doc_id)
    return jsonify({"ok": True, "status": "sent", "sent_at": now,
                    "doc_id": doc_id,
                    "doc_url": f"/pricecheck/{pcid}/document/{doc_id}" if doc_id else ""})


# ═══════════════════════════════════════════════════════════════════════
# Manual mark-as-sent for PCs (Bundle-5 PR-5b, audit AA 2026-04-22).
# Parallel to /api/rfq/<rid>/mark-sent-manually. Accepts multipart with
# an attachment the operator actually sent; stamps manual_sent metadata;
# fires Drive + lifecycle + activity hooks. See routes_rfq_admin.py for
# rationale.
# ═══════════════════════════════════════════════════════════════════════


@bp.route("/api/pricecheck/<pcid>/mark-sent-manually", methods=["POST"])
@auth_required
@safe_route
def api_pricecheck_mark_sent_manually(pcid):
    """Mark PC as sent when operator emailed it outside the app's Send flow.

    Race-safe wrapper (PR #778 pattern).
    """
    # Read the request OUTSIDE the lock so the locked inner is pure-data
    # and reusable from non-Flask-request contexts (gmail_sent_watcher
    # PR #9 2026-05-26).
    is_multipart = (request.content_type or "").startswith("multipart/")
    if is_multipart:
        payload = request.form.to_dict()
        uploaded = request.files.get("attachment")
    else:
        payload = request.get_json(force=True, silent=True) or {}
        uploaded = None
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        return _api_pricecheck_mark_sent_manually_locked(
            pcid, payload=payload, uploaded=uploaded,
        )


def _api_pricecheck_mark_sent_manually_locked(pcid, *, payload=None, uploaded=None):
    """Inner body — always runs under `_save_pcs_lock`.

    Pure-data entry point — see _api_rfq_mark_sent_manually_locked
    docstring for the call-from-background-thread pattern.
    """
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"}), 404
    pc = pcs[pcid]
    payload = payload or {}

    now_iso = datetime.now().isoformat()
    sent_to = (payload.get("sent_to")
               or pc.get("requestor_email") or pc.get("requestor") or "").strip()
    sent_at = (payload.get("sent_at") or now_iso).strip()
    notes = (payload.get("notes") or "").strip()

    # Save attachment under uploads/manual_sent/pc_<id>/
    attachment = {}
    if uploaded and getattr(uploaded, "filename", ""):
        fname = os.path.basename(uploaded.filename)
        fname = re.sub(r"[\\\\/]", "_", fname)[:200] or "attachment.pdf"
        dest_dir = os.path.join(DATA_DIR, "uploads", "manual_sent", pcid)
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, fname)
            uploaded.save(dest)
            attachment = {"filename": fname, "path": dest,
                          "size": os.path.getsize(dest)}
        except OSError as _e:
            log.error("PC manual-sent attachment save failed: %s", _e)

    old_status = pc.get("status", "")
    # PR #11 substrate-singleness: single writer for 'sent'.
    from src.core.quote_lifecycle_shared import mark_sent_in_place
    mark_sent_in_place(
        pc, sent_at=sent_at, sent_to=sent_to, sent_method="manual",
        notes=notes or "Marked sent manually (out-of-band)",
        source="user",
    )
    pc["manual_sent_metadata"] = {
        "marked_at": now_iso,
        "sent_at_reported": sent_at,
        "sent_to": sent_to,
        "actor": "user",
        "notes": notes,
        "attachment": attachment or None,
        "prior_status": old_status,
    }

    _save_single_pc(pcid, pc)

    # On-sent hooks, each wrapped so one failure can't block the flip.
    try:
        _log_crm_activity(pc.get("reytech_quote_number", pcid),
                          "quote_sent_manually",
                          f"Quote for PC #{pc.get('pc_number','')} marked sent manually"
                          + (f" to {sent_to}" if sent_to else ""),
                          actor="user")
    except Exception as _e:
        log.debug("_log_crm_activity (manual sent) suppressed: %s", _e)
    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("pc", pcid, "quote_sent_manual",
                            f"Marked sent manually to {sent_to or '—'}"
                            + (f" · {notes}" if notes else ""),
                            actor="user")
    except Exception as _e:
        log.debug("log_lifecycle_event(pc sent manual) suppressed: %s", _e)

    # propagate to quotes table now handled INSIDE mark_sent_in_place
    # above (PR #11 substrate-singleness — single writer for 'sent').

    # PR-U (2026-05-13): drift + shadow logs on the manual mark-sent path
    # too. Same fix as /mark-sent above; operator emails out-of-band ➜
    # still hits this endpoint ➜ digest needs the signal.
    try:
        from src.core.operator_kpi import fire_drift_logs_on_send
        fire_drift_logs_on_send(pcid, "pc", pc)
    except Exception as _drift_e:
        log.debug("fire_drift_logs_on_send (mark-sent-manually) suppressed: %s", _drift_e)

    log.info("PC %s marked SENT manually: sent_to=%s attachment=%s prior=%s",
             pcid, sent_to, bool(attachment), old_status)
    return jsonify({
        "ok": True,
        "status": "sent",
        "sent_at": sent_at,
        "sent_to": sent_to,
        "attachment": attachment or None,
        "prior_status": old_status,
    })


# ═══════════════════════════════════════════════════════════════════════
# PC Follow-Up Scanner (PRD-v32 F3)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricecheck/follow-up-scan")
@auth_required
@safe_route
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
@safe_route
def api_pc_log_follow_up(pcid):
    """Log that a follow-up was done on a sent PC.

    Race-safe wrapper (PR #778 pattern).
    """
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        return _api_pc_log_follow_up_locked(pcid)


def _api_pc_log_follow_up_locked(pcid):
    """Inner body — always runs under `_save_pcs_lock`."""
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
@safe_route
def api_pc_mark_no_response(pcid):
    """Mark a PC as not responding after follow-up attempts."""
    # RMW under lock — see PR #778.
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
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
@safe_page
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
         <td style="font-family:monospace;font-weight:600;color:var(--r-accent)">v{d['version']}</td>
         <td>{d['created_at'][:19].replace('T',' ')}</td>
         <td>{d.get('notes','')[:40]}</td>
         <td>{d.get('change_summary','')[:60]}</td>
         <td><span style="background:{status_badge[1]};color:#0d1117;padding:2px 8px;border-radius:4px;font-size:14px;font-weight:600">{status_badge[0]}</span></td>
         <td style="text-align:right;font-family:monospace">{d.get('file_size',0)//1024}KB</td>
         <td><a href="/api/pricecheck/document/{d['id']}/pdf" style="color:var(--r-accent)">📥 Download</a></td>
        </tr>'''
    
    content = f'''
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 style="margin:0">📄 Sent Documents — PC #{pc.get("pc_number","?")}</h2>
      <a href="/pricecheck/{pcid}" style="color:var(--r-accent);text-decoration:none;font-size:13px">← Back to PC Detail</a>
    </div>
    <div style="font-size:13px;color:var(--r-text-muted);margin-bottom:16px">{pc.get("institution","")} · {len(docs)} version(s)</div>
    <div style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:8px;overflow:hidden">
     <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="border-bottom:1px solid var(--r-border);font-size:14px;color:var(--r-text-muted);text-transform:uppercase">
       <th style="padding:10px;text-align:left">Ver</th><th style="padding:10px">Date</th>
       <th style="padding:10px">Notes</th><th style="padding:10px">Changes</th>
       <th style="padding:10px">Status</th><th style="padding:10px;text-align:right">Size</th>
       <th style="padding:10px"></th>
      </tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--r-text-muted)">No documents yet — mark PC as Sent to create the first version</td></tr>'}</tbody>
     </table>
    </div>'''
    from src.api.render import render_page
    return render_page("generic.html", active_page="PCs", page_title=f"Documents — PC #{pc.get('pc_number','?')}", content=content)


@bp.route("/api/pricecheck/document/<int:doc_id>/pdf")
@auth_required
@safe_route
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
@safe_page
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
         <td style="padding:4px"><input name="ed_qty_{i}" value="{qty}" type="number" min="1" style="width:60px;background:var(--r-surface);border:1px solid var(--r-border);border-radius:4px;padding:6px;color:var(--r-text);font-size:13px;text-align:center" onchange="recalcDoc()"></td>
         <td style="padding:4px"><input name="ed_uom_{i}" value="{uom}" style="width:60px;background:var(--r-surface);border:1px solid var(--r-border);border-radius:4px;padding:6px;color:var(--r-text);font-size:13px;text-align:center"></td>
         <td style="padding:4px"><textarea name="ed_desc_{i}" rows="2" style="width:100%;background:var(--r-surface);border:1px solid var(--r-border);border-radius:4px;padding:6px;color:var(--r-text);font-size:14px;resize:vertical">{desc}</textarea></td>
         <td style="padding:4px"><input name="ed_mfg_{i}" value="{mfg}" style="width:120px;background:var(--r-surface);border:1px solid var(--r-border);border-radius:4px;padding:6px;color:var(--r-text);font-size:14px;font-family:monospace"></td>
         <td style="padding:4px"><input name="ed_price_{i}" value="{float(price):.2f}" type="text" inputmode="decimal" style="min-width:90px;field-sizing:content;background:var(--r-surface);border:1px solid var(--r-border);border-radius:4px;padding:6px;color:var(--r-text);font-size:13px;text-align:right" oninput="if(window.sanitizePrice)sanitizePrice(this)" onblur="if(window.fmtCurrency)fmtCurrency(this)" onchange="recalcDoc()"></td>
         <td style="padding:8px;text-align:right;font-weight:600;font-family:monospace" class="doc-ext">${ext:,.2f}</td>
        </tr>'''
    
    change_log = ""
    if doc.get("change_summary"):
        change_log = f'<div style="font-size:14px;color:#d29922;margin-top:4px">Changes: {doc["change_summary"]}</div>'
    
    content = f'''
    <style>
     .doc-split {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; height:calc(100vh - 180px); }}
     .doc-pdf {{ border:1px solid var(--r-border); border-radius:8px; overflow:hidden; background:#1e1e1e; }}
     .doc-editor {{ overflow-y:auto; }}
     @media(max-width:1100px) {{ .doc-split {{ grid-template-columns:1fr; height:auto; }} .doc-pdf {{ height:600px; }} }}
    </style>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
     <div>
      <h2 style="margin:0;font-size:18px">📄 PC #{pc.get("pc_number","")} — {pc.get("institution","")}</h2>
      <div style="font-size:14px;color:var(--r-text-muted);margin-top:2px">
       Version {doc.get("version",1)} · {doc.get("created_at","")[:19].replace("T"," ")}
       · <span style="color:{("#3fb950" if doc.get("status")=="current" else "#8b949e")}">{doc.get("status","").title()}</span>
       {change_log}
      </div>
     </div>
     <div style="display:flex;gap:8px;align-items:center">
      <select id="verSelect" onchange="location.href='/pricecheck/{pcid}/document/'+this.value" style="background:var(--r-surface);border:1px solid var(--r-border);border-radius:6px;padding:6px 10px;color:var(--r-text);font-size:14px">{ver_options}</select>
      <a href="/pricecheck/{pcid}/documents" style="color:var(--r-accent);font-size:14px;text-decoration:none">📋 All Versions</a>
      <a href="/pricecheck/{pcid}" style="color:var(--r-accent);font-size:14px;text-decoration:none">← PC Detail</a>
     </div>
    </div>
    <div class="doc-split">
     <div class="doc-pdf">
      <iframe src="/api/pricecheck/document/{doc_id}/pdf" style="width:100%;height:100%;border:none"></iframe>
     </div>
     <div class="doc-editor" style="background:var(--r-surface-2);border:1px solid var(--r-border);border-radius:8px;padding:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
       <span style="font-size:14px;font-weight:700;color:var(--r-text)">✏️ Edit Line Items</span>
       <div style="display:flex;gap:8px">
        <button onclick="saveDocument(this)" class="btn btn-sm" style="background:#238636;color:#fff;font-size:13px;padding:6px 16px;border-radius:6px;border:none;cursor:pointer;font-weight:600">💾 Save & Regenerate</button>
        <a href="/api/pricecheck/document/{doc_id}/pdf" download class="btn btn-sm" style="background:#21262d;color:var(--r-accent);font-size:14px;padding:6px 12px;border-radius:6px;border:1px solid #30363d;text-decoration:none">📥 Download</a>
       </div>
      </div>
      <div id="docMsg" style="display:none;padding:8px 12px;border-radius:6px;font-size:14px;margin-bottom:10px"></div>
      <textarea id="ed_notes" placeholder="Revision notes (optional)" style="width:100%;background:var(--r-surface);border:1px solid var(--r-border);border-radius:4px;padding:6px;color:var(--r-text);font-size:14px;resize:none;margin-bottom:10px;height:32px">{doc.get("notes","")}</textarea>
      <div style="overflow-x:auto">
       <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead><tr style="border-bottom:1px solid var(--r-border);font-size:13px;color:var(--r-text-muted);text-transform:uppercase">
         <th style="padding:8px;width:30px">#</th><th style="padding:8px;width:60px">Qty</th><th style="padding:8px;width:60px">UOM</th>
         <th style="padding:8px">Description</th><th style="padding:8px;width:120px">MFG#</th>
         <th style="padding:8px;width:90px;text-align:right">Price</th><th style="padding:8px;width:90px;text-align:right">Extension</th>
        </tr></thead>
        <tbody>{item_rows}</tbody>
        <tfoot>
         <tr style="border-top:2px solid var(--r-border)">
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
@safe_page
def pricecheck_document_save(pcid):
    """Save edits from document editor → re-generates PDF → creates new version.

    Race-safe wrapper (PR #778 pattern).
    """
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        return _pricecheck_document_save_locked(pcid)


def _pricecheck_document_save_locked(pcid):
    """Inner body — always runs under `_save_pcs_lock`."""
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
    
    # Sync all aliases atomically (alias-drift substrate)
    _sync_pc_items(pc, items)
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
@safe_route
def api_pc_mark_auto_priced(pcid):
    """Mark a PC as auto-priced so the on-load auto-pricing doesn't re-run."""
    # RMW under lock — see PR #778.
    from src.api.data_layer import _save_pcs_lock
    with _save_pcs_lock:
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({"ok": False, "error": "PC not found"})
        pc["auto_priced"] = True
        _save_single_pc(pcid, pc)
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════
# Oracle V5.5 — Per-Buyer Win-Rate Curves (read-only API)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/oracle/buyer-curve/<institution>")
@auth_required
@safe_route
def api_buyer_curve(institution):
    """Return the fitted win-rate curve for a single institution, plus
    the EV-maximizing markup. Used by the quoting UI to show the
    "recommended markup = X% (P(win) = Y%)" widget and by smoke tests
    to sanity-check that the curve fitter is producing results.

    Query params:
        days  — lookback window (default 365)

    Response:
        {
          "ok": True,
          "institution": "cchcs",
          "optimal": {
            "sufficient": True,
            "markup_pct": 32.0,
            "expected_value": 17.6,
            "win_probability": 0.55
          },
          "curve": {
            "total_samples": 27,
            "won": 14,
            "lost": 13,
            "global_win_rate": 0.518,
            "buckets": [...]
          }
        }

    When sample_size is below the threshold the curve still returns but
    `optimal.sufficient == False` and the caller should fall back to
    V5 scalar logic.
    """
    from src.core.pricing_oracle_v2 import (
        _fit_buyer_curve,
        optimal_markup_for_expected_profit,
    )

    try:
        days = int(request.args.get("days", "365"))
    except (TypeError, ValueError):
        days = 365
    days = max(30, min(1095, days))

    # _fit_buyer_curve + optimizer both need a DB handle
    try:
        with get_db() as conn:
            curve = _fit_buyer_curve(conn, institution, days=days)
            optimal = optimal_markup_for_expected_profit(institution, conn)
    except Exception as e:
        log.warning("buyer-curve lookup failed for %s: %s", institution, e)
        return jsonify({"ok": False, "error": str(e)}), 500

    if curve is None:
        return jsonify({
            "ok": True,
            "institution": institution,
            "optimal": {"sufficient": False, "markup_pct": None,
                        "expected_value": None, "win_probability": None},
            "curve": None,
        })

    return jsonify({
        "ok": True,
        "institution": institution,
        "days": days,
        "optimal": {
            "sufficient": bool(optimal.get("sufficient")),
            "markup_pct": optimal.get("markup_pct"),
            "expected_value": optimal.get("expected_value"),
            "win_probability": optimal.get("win_probability"),
        },
        "curve": {
            "total_samples": curve.get("total_samples", 0),
            "won": curve.get("won", 0),
            "lost": curve.get("lost", 0),
            "global_win_rate": curve.get("global_win_rate", 0),
            "sufficient": curve.get("sufficient", False),
            "buckets": curve.get("buckets", []),
        },
    })


@bp.route("/api/oracle/buyer-list")
@auth_required
@safe_route
def api_buyer_list():
    """Return the top-N institutions by quote volume with their fitted
    curves summarized. Feeds the /buyer-intelligence page — one query
    builds the initial render so the client doesn't fan out 20 parallel
    /api/oracle/buyer-curve requests.

    Query params:
        limit — max institutions to return (default 20, max 100)
        days  — lookback window for curve fitting (default 365)
    """
    from src.core.pricing_oracle_v2 import optimal_markup_for_expected_profit
    from datetime import datetime, timedelta

    try:
        limit = int(request.args.get("limit", "20"))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(100, limit))

    try:
        days = int(request.args.get("days", "365"))
    except (TypeError, ValueError):
        days = 365
    days = max(30, min(1095, days))

    since = (datetime.now() - timedelta(days=days)).isoformat()

    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT COALESCE(NULLIF(institution, ''), agency) AS name,
                          COUNT(*) AS total,
                          SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS won,
                          SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS lost,
                          MAX(created_at) AS last_quote_at
                     FROM quotes
                    WHERE created_at >= ?
                      AND is_test = 0
                      AND status IN ('won', 'lost')
                      AND COALESCE(NULLIF(institution, ''), agency) IS NOT NULL
                      AND COALESCE(NULLIF(institution, ''), agency) != ''
                    GROUP BY name
                    ORDER BY total DESC
                    LIMIT ?""",
                (since, limit),
            ).fetchall()

            buyers = []
            for row in rows:
                d = dict(row) if hasattr(row, "keys") else {
                    "name": row[0], "total": row[1],
                    "won": row[2], "lost": row[3],
                    "last_quote_at": row[4],
                }
                name = d.get("name") or ""
                if not name:
                    continue
                total = int(d.get("total") or 0)
                won = int(d.get("won") or 0)
                lost = int(d.get("lost") or 0)
                opt = optimal_markup_for_expected_profit(name, conn)
                buyers.append({
                    "institution": name,
                    "total_quotes": total,
                    "won": won,
                    "lost": lost,
                    "win_rate": round(won / total, 3) if total else 0,
                    "last_quote_at": d.get("last_quote_at") or "",
                    "sufficient": bool(opt.get("sufficient")),
                    "optimal_markup_pct": opt.get("markup_pct"),
                    "expected_value": opt.get("expected_value"),
                    "win_probability": opt.get("win_probability"),
                })
    except Exception as e:
        log.warning("buyer-list query failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "days": days,
        "count": len(buyers),
        "buyers": buyers,
    })


def _compute_outreach_triggers(conn, silence_days: int = 60,
                                 min_quotes: int = 5, limit: int = 20):
    """Return buyers who look like they've gone quiet and are worth a
    nudge. The heuristic:

      - We've quoted them at least `min_quotes` times (real relationship,
        not a one-off).
      - Their most recent quote is at least `silence_days` days old.
      - Rank by *lost-revenue potential* using a **debiased** win
        rate — won / (won + lost), ignoring quotes stuck in 'sent' or
        'pending' status. An early version of this function computed
        win_rate = won / total_quotes, which under-valued buyers whose
        later quotes went through without being marked won/lost. On
        prod (2026-04-14) only 16% of quotes reached a terminal
        status, so the naive formula would have penalized every
        high-volume buyer.

      - Display columns also include `captured_quotes` (won+lost) and
        `unresolved_quotes` (sent+pending) so the UI can show a
        "capture confidence" signal for each buyer.

    Never raises — SQL errors return an empty list so the page still
    renders if the quotes table isn't available.
    """
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=silence_days)).isoformat()
    try:
        rows = conn.execute(
            """SELECT COALESCE(NULLIF(institution, ''), agency) AS name,
                      COUNT(*) AS quote_count,
                      SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS won_count,
                      SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS lost_count,
                      SUM(CASE WHEN status='won' THEN COALESCE(total,0) ELSE 0 END) AS won_dollars,
                      AVG(COALESCE(total,0)) AS avg_dollars,
                      MAX(created_at) AS last_quote_at
                 FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost', 'sent', 'pending')
                  AND COALESCE(NULLIF(institution, ''), agency) IS NOT NULL
                  AND COALESCE(NULLIF(institution, ''), agency) != ''
                GROUP BY name
               HAVING COUNT(*) >= ?
                  AND (MAX(created_at) IS NULL OR MAX(created_at) < ?)
                ORDER BY (AVG(COALESCE(total,0)) *
                          (CAST(SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS REAL)
                           / NULLIF(SUM(CASE WHEN status IN ('won','lost') THEN 1 ELSE 0 END), 0)) *
                          COUNT(*)) DESC
                LIMIT ?""",
            (min_quotes, cutoff, limit),
        ).fetchall()
    except Exception as e:
        log.debug("_compute_outreach_triggers failed: %s", e)
        return []

    now = datetime.now()
    out = []
    for row in rows:
        d = dict(row) if hasattr(row, "keys") else {
            "name": row[0], "quote_count": row[1], "won_count": row[2],
            "lost_count": row[3], "won_dollars": row[4], "avg_dollars": row[5],
            "last_quote_at": row[6],
        }
        name = d.get("name") or ""
        if not name:
            continue
        total = int(d.get("quote_count") or 0)
        won = int(d.get("won_count") or 0)
        lost = int(d.get("lost_count") or 0)
        captured = won + lost
        unresolved = max(0, total - captured)
        # Debiased win rate: only count quotes that reached a terminal
        # status. None means "not enough captured data to judge"
        # rather than a false 0%.
        win_rate = (won / captured) if captured > 0 else None
        avg_total = float(d.get("avg_dollars") or 0)
        last_quote_at = d.get("last_quote_at") or ""
        days_since = None
        try:
            if last_quote_at:
                last_dt = datetime.fromisoformat(
                    last_quote_at.replace("Z", "+00:00").split("+")[0]
                )
                days_since = (now - last_dt).days
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        # Opportunity: avg_quote × debiased_win_rate × total_quotes.
        # When win_rate is None (no captured data), treat it as 0 so
        # the buyer doesn't rank above anyone we actually know is
        # converting — but still surfaces in the list so the outreach
        # can double as a "ask for status update" nudge.
        effective_wr = win_rate if win_rate is not None else 0.0
        opportunity = round(avg_total * effective_wr * total, 2)
        out.append({
            "institution": name,
            "total_quotes": total,
            "won": won,
            "lost": lost,
            "captured_quotes": captured,
            "unresolved_quotes": unresolved,
            "capture_rate": round(captured / total, 3) if total else 0,
            # win_rate is None-capable now — UI handles the display
            "win_rate": round(win_rate, 3) if win_rate is not None else None,
            "avg_total": round(avg_total, 2),
            "won_total": round(float(d.get("won_dollars") or 0), 2),
            "last_quote_at": last_quote_at,
            "days_since_last_quote": days_since,
            "opportunity_score": opportunity,
        })
    return out


def _compute_capture_gap(conn, min_age_days: int = 30, limit: int = 20):
    """Find quotes stuck in 'sent' status past min_age_days. These are
    the rows that need a mark-won/mark-lost decision before the
    outreach and buyer-curve systems can trust their numbers.

    Returns a list of per-institution summaries (not per-quote) so
    the UI can show "CCHCS has 8 quotes stuck in sent, oldest 87d"
    instead of a 200-row flat list. The caller can drill down via the
    /quotes page filtered on status=sent.
    """
    from datetime import datetime, timedelta
    try:
        cutoff = (datetime.now() - timedelta(days=min_age_days)).isoformat()
        rows = conn.execute(
            """SELECT COALESCE(NULLIF(institution, ''), agency) AS name,
                      COUNT(*) AS stuck_count,
                      SUM(COALESCE(total,0)) AS stuck_dollars,
                      MIN(COALESCE(sent_at, created_at)) AS oldest_at
                 FROM quotes
                WHERE is_test = 0
                  AND status = 'sent'
                  AND COALESCE(sent_at, created_at) < ?
                  AND COALESCE(NULLIF(institution, ''), agency) IS NOT NULL
                  AND COALESCE(NULLIF(institution, ''), agency) != ''
                GROUP BY name
                ORDER BY stuck_count DESC, stuck_dollars DESC
                LIMIT ?""",
            (cutoff, limit),
        ).fetchall()
    except Exception as e:
        log.debug("_compute_capture_gap failed: %s", e)
        return []

    now = datetime.now()
    out = []
    for row in rows:
        d = dict(row) if hasattr(row, "keys") else {
            "name": row[0], "stuck_count": row[1],
            "stuck_dollars": row[2], "oldest_at": row[3],
        }
        name = d.get("name") or ""
        if not name:
            continue
        oldest_at = d.get("oldest_at") or ""
        oldest_age_days = None
        try:
            if oldest_at:
                oldest_dt = datetime.fromisoformat(
                    oldest_at.replace("Z", "+00:00").split("+")[0]
                )
                oldest_age_days = (now - oldest_dt).days
        except Exception as _e:
            log.debug("suppressed: %s", _e)
        out.append({
            "institution": name,
            "stuck_count": int(d.get("stuck_count") or 0),
            "stuck_dollars": round(float(d.get("stuck_dollars") or 0), 2),
            "oldest_at": oldest_at,
            "oldest_age_days": oldest_age_days,
        })
    return out


def _compute_capture_rate_summary(conn, days: int = 365):
    """Return a one-line capture-rate summary for the dashboard header.

    capture_rate = (won + lost) / (won + lost + sent + pending)

    Below 60% means the win-rate bias fix isn't fully effective —
    outreach rankings still have noise. Above 80% means the data is
    clean enough to trust the curve-fitting and scalar V5 values
    simultaneously.
    """
    from datetime import datetime, timedelta
    try:
        since = (datetime.now() - timedelta(days=days)).isoformat()
        row = conn.execute(
            """SELECT
                 SUM(CASE WHEN status IN ('won','lost') THEN 1 ELSE 0 END) AS captured,
                 SUM(CASE WHEN status IN ('won','lost','sent','pending') THEN 1 ELSE 0 END) AS total
               FROM quotes
              WHERE is_test = 0
                AND created_at >= ?""",
            (since,),
        ).fetchone()
    except Exception as e:
        log.debug("_compute_capture_rate_summary failed: %s", e)
        return {"captured": 0, "total": 0, "rate": 0.0}

    d = dict(row) if hasattr(row, "keys") else {
        "captured": row[0], "total": row[1],
    }
    captured = int(d.get("captured") or 0)
    total = int(d.get("total") or 0)
    return {
        "captured": captured,
        "total": total,
        "rate": round(captured / total, 3) if total else 0.0,
        "days": days,
    }


@bp.route("/buyer-intelligence")
@auth_required
def buyer_intelligence_page():
    """Growth Phase B — buyer intelligence UI. Renders server-side with
    the top-N buyer summary already embedded, then fetches the selected
    buyer's full curve over XHR when the user clicks a row.
    """
    from src.core.pricing_oracle_v2 import optimal_markup_for_expected_profit
    from datetime import datetime, timedelta

    try:
        days = int(request.args.get("days", "365"))
    except (TypeError, ValueError):
        days = 365
    days = max(30, min(1095, days))

    # Outreach-trigger window is independent of the curve window. A
    # buyer who has been silent for 60d might still have enough 365d
    # history to fit a curve — both are useful.
    try:
        silence_days = int(request.args.get("silence_days", "60"))
    except (TypeError, ValueError):
        silence_days = 60
    silence_days = max(14, min(365, silence_days))

    since = (datetime.now() - timedelta(days=days)).isoformat()
    buyers = []
    outreach = []
    totals = {"institutions": 0, "quotes": 0, "won": 0, "sufficient": 0}

    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT COALESCE(NULLIF(institution, ''), agency) AS name,
                          COUNT(*) AS total,
                          SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS won,
                          SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS lost,
                          MAX(created_at) AS last_quote_at
                     FROM quotes
                    WHERE created_at >= ?
                      AND is_test = 0
                      AND status IN ('won', 'lost')
                      AND COALESCE(NULLIF(institution, ''), agency) IS NOT NULL
                      AND COALESCE(NULLIF(institution, ''), agency) != ''
                    GROUP BY name
                    ORDER BY total DESC
                    LIMIT 40""",
                (since,),
            ).fetchall()

            for row in rows:
                d = dict(row) if hasattr(row, "keys") else {
                    "name": row[0], "total": row[1],
                    "won": row[2], "lost": row[3],
                    "last_quote_at": row[4],
                }
                name = d.get("name") or ""
                if not name:
                    continue
                total = int(d.get("total") or 0)
                won = int(d.get("won") or 0)
                lost = int(d.get("lost") or 0)
                opt = optimal_markup_for_expected_profit(name, conn)
                buyers.append({
                    "institution": name,
                    "total_quotes": total,
                    "won": won,
                    "lost": lost,
                    "win_rate": round(won / total, 3) if total else 0,
                    "last_quote_at": d.get("last_quote_at") or "",
                    "sufficient": bool(opt.get("sufficient")),
                    "optimal_markup_pct": opt.get("markup_pct"),
                    "expected_value": opt.get("expected_value"),
                    "win_probability": opt.get("win_probability"),
                    "curve": opt.get("curve") or {},
                })
                totals["quotes"] += total
                totals["won"] += won
                if opt.get("sufficient"):
                    totals["sufficient"] += 1
            totals["institutions"] = len(buyers)
            # CRM outreach triggers + capture-gap widgets — computed
            # from the same DB handle so we don't reopen the cursor
            outreach = _compute_outreach_triggers(conn, silence_days=silence_days)
            capture_gap = _compute_capture_gap(conn)
            capture_summary = _compute_capture_rate_summary(conn, days=days)
    except Exception as e:
        log.warning("buyer-intelligence page query failed: %s", e)
        capture_gap = []
        capture_summary = {"captured": 0, "total": 0, "rate": 0.0}

    return render_page(
        "buyer_intelligence.html",
        active_page="Buyer Intel",
        days=days,
        silence_days=silence_days,
        buyers=buyers,
        outreach=outreach,
        capture_gap=capture_gap,
        capture_summary=capture_summary,
        totals=totals,
    )


@bp.route("/api/oracle/outreach-triggers")
@auth_required
@safe_route
def api_outreach_triggers():
    """JSON feed of buyers worth reaching out to. Used by scripts,
    external monitors, and the CRM outreach scheduler if we ever wire
    it to an email sender.

    Query params:
        silence_days — minimum days since last quote (default 60)
        min_quotes   — minimum historical quote count (default 5)
        limit        — max results (default 20, capped at 100)
    """
    try:
        silence_days = int(request.args.get("silence_days", "60"))
    except (TypeError, ValueError):
        silence_days = 60
    silence_days = max(14, min(365, silence_days))

    try:
        min_quotes = int(request.args.get("min_quotes", "5"))
    except (TypeError, ValueError):
        min_quotes = 5
    min_quotes = max(1, min(100, min_quotes))

    try:
        limit = int(request.args.get("limit", "20"))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(100, limit))

    try:
        with get_db() as conn:
            triggers = _compute_outreach_triggers(
                conn, silence_days=silence_days,
                min_quotes=min_quotes, limit=limit,
            )
    except Exception as e:
        log.warning("api_outreach_triggers failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "silence_days": silence_days,
        "min_quotes": min_quotes,
        "count": len(triggers),
        "triggers": triggers,
    })


@bp.route("/api/oracle/capture-gap")
@auth_required
@safe_route
def api_capture_gap():
    """Return quotes stuck in 'sent' status past min_age_days grouped
    by institution, plus an overall capture-rate summary. These are
    the rows that need a mark-won/lost decision before the outreach
    and buyer-curve systems can produce trustworthy rankings.

    Query params:
        min_age_days — minimum days since sent (default 30)
        days         — window for capture-rate summary (default 365)
    """
    try:
        min_age_days = int(request.args.get("min_age_days", "30"))
    except (TypeError, ValueError):
        min_age_days = 30
    min_age_days = max(7, min(365, min_age_days))

    try:
        days = int(request.args.get("days", "365"))
    except (TypeError, ValueError):
        days = 365
    days = max(30, min(1095, days))

    try:
        with get_db() as conn:
            gap = _compute_capture_gap(conn, min_age_days=min_age_days)
            summary = _compute_capture_rate_summary(conn, days=days)
    except Exception as e:
        log.warning("api_capture_gap failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "min_age_days": min_age_days,
        "days": days,
        "summary": summary,
        "count": len(gap),
        "gap": gap,
    })

