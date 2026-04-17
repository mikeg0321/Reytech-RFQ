# RFQ + Quote Routes
# 9 routes, 484 lines
# Loaded by dashboard.py via load_module()

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route
from src.core.security import rate_limit
from flask import redirect, flash
from src.core.paths import DATA_DIR, UPLOAD_DIR, OUTPUT_DIR
from src.core.db import get_db
from src.api.render import render_page
from datetime import datetime, timezone, timedelta
import re as _re_mod


def _validate_rid(rid: str):
    """Validate rfq_id to prevent path traversal. Returns None if valid,
    or a (response, status_code) tuple if invalid."""
    if not rid or ".." in rid or "/" in rid or "\\" in rid:
        return jsonify({"ok": False, "error": "Invalid RFQ ID"}), 400
    return None


# ═══════════════════════════════════════════════════════════════════════
# Pricing Intelligence — catalog + price history integration
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# F11: Margin Guardrails — configurable pricing rules
# ═══════════════════════════════════════════════════════════════════════

MARGIN_RULES = {
    "min_margin_pct": 15,       # Warn if margin below this
    "critical_margin_pct": 5,   # Fail if margin below this
    "max_over_scprs_pct": 10,   # Warn if bid > SCPRS by this much
    "max_under_scprs_pct": 15,  # Warn if bid < SCPRS by this much (leaving money)
    "require_cost_source": True, # Warn if cost has no backing URL/SCPRS
}


def _check_guardrails(items):
    """F11: Check margin guardrails on all items. Returns list of warnings."""
    warnings = []
    for i, item in enumerate(items):
        bid = item.get("price_per_unit") or 0
        cost = item.get("supplier_cost") or 0
        scprs = item.get("scprs_last_price") or 0
        desc = (item.get("description", "") or "")[:40]
        if not bid or bid <= 0:
            continue

        # Margin check — use LANDED cost (includes shipping + tax) if available
        _eff_cost = cost
        _supplier = item.get("item_supplier", "")
        if cost > 0 and _supplier:
            try:
                from src.core.db import calc_landed_cost
                _lc = calc_landed_cost(cost, item.get("qty", 1) or 1, _supplier)
                _eff_cost = _lc["landed_cost"]
            except Exception as _e:
                log.debug('suppressed in _check_guardrails: %s', _e)
        if _eff_cost > 0:
            margin = (bid - _eff_cost) / bid * 100
            _margin_note = "" if _eff_cost == cost else f" (landed: ${_eff_cost:.2f})"
            if margin < MARGIN_RULES["critical_margin_pct"]:
                warnings.append({
                    "idx": i, "desc": desc, "level": "critical",
                    "msg": f"Margin {margin:.1f}% is below {MARGIN_RULES['critical_margin_pct']}% minimum{_margin_note}"
                })
            elif margin < MARGIN_RULES["min_margin_pct"]:
                warnings.append({
                    "idx": i, "desc": desc, "level": "warn",
                    "msg": f"Margin {margin:.1f}% is below {MARGIN_RULES['min_margin_pct']}% target{_margin_note}"
                })

        # SCPRS comparison
        if scprs > 0 and bid > 0:
            diff_pct = (bid - scprs) / scprs * 100
            if diff_pct > MARGIN_RULES["max_over_scprs_pct"]:
                warnings.append({
                    "idx": i, "desc": desc, "level": "warn",
                    "msg": f"Bid is {diff_pct:.0f}% above SCPRS — may lose"
                })

        # Cost without source
        if cost > 0 and MARGIN_RULES["require_cost_source"]:
            if not item.get("item_link") and not scprs:
                warnings.append({
                    "idx": i, "desc": desc, "level": "info",
                    "msg": "Cost has no backing source (no URL or SCPRS)"
                })

        # Loss intelligence — check if we've lost on similar items before
        if bid > 0 and desc and len(desc) > 5:
            try:
                from src.agents.pricing_feedback import get_pricing_recommendation
                agency = items[0].get("agency", "") if items else ""
                loss_rec = get_pricing_recommendation(desc, agency, cost)
                if loss_rec.get("margin_warning"):
                    warnings.append({
                        "idx": i, "desc": desc, "level": "warn",
                        "msg": f"Loss intel: {loss_rec['margin_warning'][:120]}"
                    })
                comp_floor = loss_rec.get("competitor_floor")
                if comp_floor and bid > comp_floor * 1.10:
                    warnings.append({
                        "idx": i, "desc": desc, "level": "warn",
                        "msg": f"Bid ${bid:.2f} is >{10}% above competitor floor "
                               f"${comp_floor:.2f} (from {loss_rec.get('sources_used',0)} losses)"
                    })
            except Exception as _e:
                log.debug('suppressed in _check_guardrails: %s', _e)

    return warnings


def _recommend_price(item):
    """Lightweight price recommendation with loss intelligence.
    Returns {recommended, aggressive, safe, loss_intel} tiers or None."""
    scprs = item.get("scprs_last_price") or 0
    amazon = item.get("amazon_price") or 0
    cost = item.get("supplier_cost") or 0
    desc = (item.get("description") or "")[:60]
    agency = item.get("agency", "")

    base = 0
    reason = ""

    if scprs > 0:
        base = scprs
        reason = f"SCPRS ${scprs:.2f}"
    elif amazon > 0:
        base = amazon * 1.15
        reason = f"Amazon ${amazon:.2f}+15%"
    elif cost > 0:
        base = cost * 1.25
        reason = f"Cost ${cost:.2f}+25%"
    else:
        return None

    # Ensure minimum margin over cost
    if cost > 0 and base < cost * 1.10:
        base = cost * 1.10

    result = {
        "recommended": round(base * 0.98, 2),
        "aggressive": round(base * 0.93, 2),
        "safe": round(base * 1.05, 2),
        "reason": reason,
    }

    # Loss intelligence: adjust recommendations if we have competitor data
    if desc and len(desc) > 5:
        try:
            from src.agents.pricing_feedback import get_pricing_recommendation
            loss_rec = get_pricing_recommendation(desc, agency, float(cost) if cost else 0)
            if loss_rec.get("sources_used", 0) > 0:
                comp_floor = loss_rec.get("competitor_floor")
                suggested = loss_rec.get("suggested_range")
                result["loss_intel"] = {
                    "competitor_floor": comp_floor,
                    "suggested_range": suggested,
                    "margin_warning": loss_rec.get("margin_warning"),
                    "loss_count": loss_rec.get("sources_used", 0),
                    "confidence": loss_rec.get("confidence", 0),
                }
                # If competitor floor is known and lower than our recommended,
                # adjust aggressive tier to beat it
                if comp_floor and comp_floor > 0:
                    beat_floor = round(comp_floor * 0.98, 2)  # 2% under competitor
                    if cost > 0 and beat_floor > cost * 1.05:  # Must keep 5% min margin
                        result["competitive"] = beat_floor
                        result["reason"] += f" | Loss Intel: floor ${comp_floor:.2f}"
        except Exception as _e:
            log.debug('suppressed in _recommend_price: %s', _e)

    return result


def _enrich_items_with_intel(items, rfq_number="", agency=""):
    """Enrich line items with catalog matches and price history.
    Called on RFQ detail load to surface pricing intelligence."""
    for item in items:
        desc = item.get("description", "")
        pn = item.get("item_number", "") or ""
        if not desc and not pn:
            continue

        # 1. Catalog match
        if not item.get("catalog_match"):
            try:
                from src.core.catalog import search_catalog
                matches = search_catalog(pn or desc[:40], limit=1)
                if matches:
                    m = matches[0]
                    item["catalog_match"] = {
                        "sku": m.get("sku", ""),
                        "name": m.get("name", ""),
                        "typical_cost": m.get("typical_cost", 0),
                        "list_price": m.get("list_price", 0),
                        "category": m.get("category", ""),
                    }
            except Exception as _e:
                log.debug('suppressed in _enrich_items_with_intel: %s', _e)

        # 2. Price history (last 5 observations)
        if not item.get("price_intel"):
            try:
                from src.core.db import get_price_history_db
                history = get_price_history_db(
                    description=desc[:60] if not pn else "",
                    part_number=pn,
                    limit=5
                )
                if history:
                    prices = [h["unit_price"] for h in history if h.get("unit_price")]
                    item["price_intel"] = {
                        "history_count": len(history),
                        "avg_price": round(sum(prices) / len(prices), 2) if prices else 0,
                        "min_price": round(min(prices), 2) if prices else 0,
                        "max_price": round(max(prices), 2) if prices else 0,
                        "last_price": prices[0] if prices else 0,
                        "last_source": history[0].get("source", "") if history else "",
                        "last_date": history[0].get("found_at", "")[:10] if history else "",
                        "last_quote": history[0].get("quote_number", "") if history else "",
                    }
            except Exception as _e:
                log.debug('suppressed in _enrich_items_with_intel: %s', _e)


def _record_rfq_prices(rfq_data, source="rfq_save"):
    """Record all priced items to price_history + auto-ingest to catalog."""
    sol = rfq_data.get("solicitation_number", "")
    agency = rfq_data.get("agency", "")
    for item in rfq_data.get("line_items", []):
        desc = item.get("description", "")
        pn = item.get("item_number", "") or ""
        if not desc:
            continue

        # Record supplier cost
        cost = item.get("supplier_cost") or 0
        if cost and cost > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=cost, source=source,
                    part_number=pn, agency=agency, quote_number=sol,
                    source_url=item.get("item_link", ""),
                    notes=f"Supplier cost from RFQ {sol}"
                )
            except Exception as _e:
                log.debug('suppressed in _record_rfq_prices: %s', _e)

        # Record bid price
        bid = item.get("price_per_unit") or 0
        if bid and bid > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=bid, source=f"{source}_bid",
                    part_number=pn, agency=agency, quote_number=sol,
                    notes=f"Bid price from RFQ {sol}"
                )
            except Exception as _e:
                log.debug('suppressed in _record_rfq_prices: %s', _e)

        # Record SCPRS price
        scprs = item.get("scprs_last_price") or 0
        if scprs and scprs > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=scprs, source="scprs",
                    part_number=pn, agency=agency, quote_number=sol,
                )
            except Exception as _e:
                log.debug('suppressed in _record_rfq_prices: %s', _e)

        # Record Amazon price
        amz = item.get("amazon_price") or 0
        if amz and amz > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=amz, source="amazon",
                    part_number=pn, source_url=item.get("item_link", ""),
                )
            except Exception as _e:
                log.debug('suppressed in _record_rfq_prices: %s', _e)

        # Auto-ingest to product_catalog (same table PC uses + auto-price reads)
        if cost > 0 or bid > 0:
            try:
                from src.agents.product_catalog import add_to_catalog, init_catalog_db
                init_catalog_db()
                add_to_catalog(
                    description=desc,
                    part_number=pn,
                    cost=float(cost) if cost else 0,
                    sell_price=float(bid) if bid else 0,
                    source=f"rfq_{sol}",
                    supplier_name=item.get("item_supplier", ""),
                    supplier_url=item.get("item_link", ""),
                )
            except Exception as _e:
                log.debug('suppressed in _record_rfq_prices: %s', _e)

@bp.route("/health")
def health_check():
    """Health check endpoint for Railway/load balancers. No auth required."""
    checks = {"status": "ok", "timestamp": datetime.now().isoformat()}
    # Check SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("SELECT 1")
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"
        checks["status"] = "degraded"
    # Check data dir writable
    try:
        test_path = os.path.join(DATA_DIR, ".health_check")
        with open(test_path, "w") as f: f.write("ok")
        os.remove(test_path)
        checks["disk"] = "ok"
    except Exception as e:
        checks["disk"] = f"error: {e}"
        checks["status"] = "degraded"
    # Check validation module loads
    try:
        from src.core.validation import validate_price
        v, err = validate_price("12.50")
        checks["validation"] = "ok" if v == 12.5 and err is None else "fail"
    except Exception as e:
        checks["validation"] = f"error: {e}"
    # Active RFQ/PC counts (non-critical)
    try:
        active_rfqs = {k: v for k, v in load_rfqs().items() if v.get("status") not in ("dismissed", "sent", "duplicate")}
        checks["active_rfqs"] = len(active_rfqs)
    except Exception:
        checks["active_rfqs"] = -1
    try:
        from src.api.dashboard import load_price_checks
        pcs = load_price_checks()
        active_pcs = {k: v for k, v in pcs.items() if v.get("status") not in ("dismissed", "duplicate", "archived")}
        checks["active_pcs"] = len(active_pcs)
    except Exception:
        checks["active_pcs"] = -1
    code = 200 if checks["status"] == "ok" else 503
    return jsonify(checks), code

@bp.route("/")
@auth_required
@safe_page
def home():
    import time as _ht
    _t0 = _ht.time()
    log.info("HOME: request started")
    try:
        all_pcs = _load_price_checks()
    except Exception:
        all_pcs = {}
    log.info("HOME: PCs loaded (%d) in %.0fms", len(all_pcs), (_ht.time()-_t0)*1000)
    # Recovery runs at boot (dashboard.py), not on every request
    from src.api.dashboard import _is_user_facing_pc
    user_pcs = {k: v for k, v in all_pcs.items() if _is_user_facing_pc(v)}
    # Hide test fixtures from the human queue. The /api/test/create-pc route
    # is hit by the smoke suite on every promote (smoke_test.py feature_321)
    # and by the "Create Test RFQ" button on the agents page. Both flag the
    # record is_test=True — respect that flag here so those records don't
    # crowd out real work. They remain accessible via direct URL for any
    # admin diagnostic that wants them.
    user_pcs = {k: v for k, v in user_pcs.items() if not v.get("is_test")}
    # Keep PCs with zero items IF they came from a real buyer email —
    # those are parse failures that need human attention, not noise.
    # Before 2026-04-12 the filter was `len(items) > 0 OR status in (...)`,
    # which meant every Valencia / Jensen / CDCR email where the parser
    # missed the item table got silently hidden from the home queue. The
    # user had 12 such empty-item PCs on prod with no way to see them.
    # The _parse_failed flag is added below so the row can show a badge.
    def _keep_pc(v):
        items = v.get("items") or []
        if len(items) > 0:
            return True
        # Zero items but came from email with a sender — surface it so
        # the user can re-parse manually.
        if v.get("email_subject") or v.get("sender_email") or v.get("original_sender"):
            v["_parse_failed"] = True
            return True
        # Zero items but user explicitly moved it along — keep.
        if v.get("status") in ("sent", "won", "lost", "generated", "ready", "priced"):
            return True
        # Zero items and has a real PC/solicitation number — keep.
        if (v.get("solicitation_number") or v.get("pc_number", "")) not in ("", "unknown", "RFQ"):
            return True
        return False
    user_pcs = {k: v for k, v in user_pcs.items() if _keep_pc(v)}
    # Split: active queue vs sent/completed
    _pc_actionable = {"new", "draft", "parsed", "parse_error", "priced", "ready", "auto_drafted", "quoted", "generated", "enriching", "enriched"}
    active_pcs = {k: v for k, v in user_pcs.items() if v.get("status", "") in _pc_actionable}
    sent_pcs = {k: v for k, v in user_pcs.items() if v.get("status", "") in ("sent", "pending_award", "won", "lost")}
    sent_pcs = dict(sorted(sent_pcs.items(), key=lambda x: x[1].get("sent_at") or x[1].get("updated_at") or "", reverse=True))
    # Pacific "today" for California-based due date comparisons (PST/PDT aware)
    from zoneinfo import ZoneInfo as _ZI
    _today = datetime.now(_ZI("America/Los_Angeles")).replace(tzinfo=None)
    # Sort by URGENCY: overdue first, then soonest due date, then newest
    def _pc_sort_key(item):
        pc = item[1]
        due = pc.get("due_date", "") or ""
        status = pc.get("status", "")
        # Terminal statuses go to bottom
        if status in ("won", "lost", "dismissed", "archived", "expired"):
            return (3, "9999-99-99", "")
        # Parse due date and compute urgency
        urgency = 1  # default: normal
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days_left = (d - _today).days
                    if days_left < 0:
                        urgency = 0  # OVERDUE — top of queue
                    elif days_left <= 2:
                        urgency = 0  # Due within 48h — also top
                    due_sort = d.strftime("%Y-%m-%d")
                    return (urgency, due_sort, pc.get("created_at", ""))
                except ValueError:
                    continue
        except Exception as _e:
            log.debug('suppressed in _pc_sort_key: %s', _e)
        # No parseable due date — sort by creation
        return (2, "", pc.get("created_at", ""))
    sorted_pcs = dict(sorted(active_pcs.items(), key=_pc_sort_key))
    
    # Also compute urgency metadata for template
    for pid, pc in sorted_pcs.items():
        due = pc.get("due_date", "") or ""
        pc["_days_left"] = None
        pc["_urgency"] = "normal"
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days = (d - _today).days
                    pc["_days_left"] = days
                    if days < 0: pc["_urgency"] = "overdue"
                    elif days <= 1: pc["_urgency"] = "critical"
                    elif days <= 3: pc["_urgency"] = "soon"
                    break
                except ValueError:
                    continue
        except Exception as _e:
            log.debug('suppressed in _pc_sort_key: %s', _e)

    # Same for RFQs — split active from sent/completed
    _actionable_rfq = {"new", "draft", "ready", "generated", "parsed", "priced"}
    all_rfqs = load_rfqs()
    active_rfqs = {k: v for k, v in all_rfqs.items() if v.get("status", "") in _actionable_rfq}
    # Filter ghost RFQs: 0 items + no real solicitation
    active_rfqs = {k: v for k, v in active_rfqs.items()
                   if len(v.get("line_items", v.get("items", []))) > 0
                   or (v.get("solicitation_number") or v.get("rfq_number", "")) not in ("", "unknown", "RFQ")}
    sent_rfqs = {k: v for k, v in all_rfqs.items() if v.get("status", "") in ("sent", "won", "lost")}
    sent_rfqs = dict(sorted(sent_rfqs.items(), key=lambda x: x[1].get("sent_at") or x[1].get("updated_at") or "", reverse=True))
    for rid, r in active_rfqs.items():
        due = r.get("due_date", "") or ""
        r["_days_left"] = None
        r["_urgency"] = "normal"
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days = (d - _today).days
                    r["_days_left"] = days
                    if days < 0: r["_urgency"] = "overdue"
                    elif days <= 1: r["_urgency"] = "critical"
                    elif days <= 3: r["_urgency"] = "soon"
                    break
                except ValueError:
                    continue
        except Exception as _e:
            log.debug('suppressed in _pc_sort_key: %s', _e)
    # Sort RFQs by urgency too
    active_rfqs = dict(sorted(active_rfqs.items(), key=lambda x: (
        3 if x[1].get("status") in ("sent","generated") else 0 if x[1].get("_urgency") in ("overdue","critical") else 1,
        x[1].get("due_date", "9999"),
    )))
    # P0.5: Compute readiness scoring for each RFQ (mirrors PC readiness)
    for _rfq_rid, _rfq_r in active_rfqs.items():
        _rfq_items = _rfq_r.get("line_items", _rfq_r.get("items", []))
        if isinstance(_rfq_items, str):
            try:
                import json as _json_rfq
                _rfq_items = _json_rfq.loads(_rfq_items)
            except Exception:
                _rfq_items = []
        _rfq_total = len(_rfq_items)
        _rfq_priced = sum(1 for it in _rfq_items
                          if (it.get("price_per_unit") or it.get("supplier_cost")
                              or it.get("amazon_price") or it.get("scprs_last_price") or 0) > 0)
        _rfq_r["_readiness"] = {
            "total": _rfq_total,
            "priced": _rfq_priced,
            "pct": round(_rfq_priced / _rfq_total * 100) if _rfq_total > 0 else 0,
        }

    # ── Build action items panel ──
    action_items = []
    # Overdue PCs
    for pid, pc in sorted_pcs.items():
        if pc.get("_urgency") == "overdue":
            action_items.append({"type": "overdue", "icon": "🔴", "text": f"PC #{pc.get('pc_number', pid[:8])} OVERDUE — {pc.get('institution', 'Unknown')}", "url": f"/pricecheck/{pid}", "priority": 0})
        elif pc.get("_urgency") == "critical":
            action_items.append({"type": "critical", "icon": "🟡", "text": f"PC #{pc.get('pc_number', pid[:8])} due TOMORROW — {pc.get('institution', 'Unknown')}", "url": f"/pricecheck/{pid}", "priority": 1})
    # PCs priced >48h ago without conversion (suggest RFQ)
    for pid, pc in all_pcs.items():
        if pc.get("status") == "priced" and pc.get("auto_priced_at"):
            try:
                priced_at = datetime.fromisoformat(pc["auto_priced_at"])
                hours_old = (_today - priced_at.replace(tzinfo=None)).total_seconds() / 3600
                if hours_old > 48:
                    action_items.append({"type": "convert", "icon": "📤", "text": f"PC #{pc.get('pc_number', pid[:8])} priced {int(hours_old)}h ago — ready to convert to RFQ?", "url": f"/pricecheck/{pid}", "priority": 2})
            except Exception as _e:
                log.debug('suppressed in _pc_sort_key: %s', _e)
    # Trend alerts from enriched PCs
    for pid, pc in all_pcs.items():
        for alert in (pc.get("trend_alerts") or [])[:2]:
            action_items.append({"type": "trend", "icon": "📉", "text": alert, "url": f"/pricecheck/{pid}", "priority": 3})
    # Expiring quotes (sent >25 days ago)
    try:
        from src.core.db import get_db as _hdb
        with _hdb() as _hconn:
            _exp_rows = _hconn.execute(
                "SELECT quote_number, agency, total, contact_email, JULIANDAY('now') - JULIANDAY(sent_at) as days "
                "FROM quotes WHERE is_test=0 AND status='sent' AND sent_at IS NOT NULL "
                "AND JULIANDAY('now') - JULIANDAY(sent_at) BETWEEN 25 AND 30 "
                "ORDER BY sent_at LIMIT 5"
            ).fetchall()
            for r in _exp_rows:
                action_items.append({"type": "expiring", "icon": "⏰", "text": f"Quote {r[0]} ({r[1]}) expires in {30 - int(r[4] or 0)}d — ${float(r[2] or 0):,.0f}", "url": f"/quotes", "priority": 2})
    except Exception as _e:
        log.debug('suppressed in _pc_sort_key: %s', _e)
    # Circuit breaker alerts
    try:
        from src.core.circuit_breaker import all_status as _cb_status
        for cb in _cb_status():
            if cb["state"] == "open":
                action_items.append({"type": "circuit", "icon": "⚡", "text": f"{cb['name']} API circuit OPEN — {cb['failure_count']} failures", "url": "/api/system/circuits", "priority": 1})
    except Exception as _e:
        log.debug('suppressed in _pc_sort_key: %s', _e)
    action_items.sort(key=lambda x: x.get("priority", 9))

    # P0.4: Compute readiness scoring for each PC
    for _rid, _rpc in sorted_pcs.items():
        _ritems = _rpc.get("items", [])
        _ractive = [it for it in _ritems if not it.get("no_bid")]
        _rtotal = len(_ractive)
        _rwith_cost = sum(1 for it in _ractive
                         if (it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0) > 0)
        _rwith_price = sum(1 for it in _ractive
                          if (it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0) > 0)
        _rpc["_readiness"] = {
            "total": _rtotal,
            "costed": _rwith_cost,
            "priced": _rwith_price,
            "pct": round(_rwith_price / _rtotal * 100) if _rtotal > 0 else 0,
        }

    # ── Normalize queue data for unified table macro ──
    from src.core.queue_helpers import normalize_queue_item, normalize_sent_item
    norm_pcs = {pid: normalize_queue_item(pc, "pc", pid) for pid, pc in sorted_pcs.items()}
    norm_sent_pcs = {pid: normalize_sent_item(pc, "pc", pid) for pid, pc in sent_pcs.items()}
    norm_rfqs = {rid: normalize_queue_item(r, "rfq", rid) for rid, r in active_rfqs.items()}
    norm_sent_rfqs = {rid: normalize_sent_item(r, "rfq", rid) for rid, r in sent_rfqs.items()}

    pc_bulk_actions = [
        {"action": "dismiss", "label": "Dismiss Selected", "css": "btn btn-sm btn-s"},
        {"action": "generate", "label": "Generate All", "icon": "\U0001f4c4", "css": "btn btn-sm", "style": "background:rgba(52,211,153,.15);color:var(--gn)", "handler": "bulkGenerate"},
    ]
    rfq_bulk_actions = [
        {"action": "dismiss", "label": "Dismiss Selected", "css": "btn btn-sm btn-s"},
        {"action": "archive", "label": "Archive", "css": "btn btn-sm btn-s"},
        {"action": "markup", "label": "+20% Markup", "css": "btn btn-sm", "style": "background:rgba(251,191,36,.15);color:var(--yl)"},
    ]

    log.info("HOME: rendering template, %d PCs + %d RFQs + %d actions, total %.0fms",
             len(sorted_pcs), len(active_rfqs), len(action_items), (_ht.time()-_t0)*1000)
    # Feature flags for intelligence layer UI
    _nl_q = False
    _doc_intake = False
    try:
        from src.core.feature_flags import get_flag
        _nl_q = get_flag("nl_query_enabled", default=False)
        _doc_intake = get_flag("docling_intake", default=False)
    except Exception as _e:
        log.debug('suppressed in _pc_sort_key: %s', _e)

    return render_page("home.html", active_page="Home",
                       rfqs=active_rfqs, price_checks=sorted_pcs,
                       sent_rfqs=sent_rfqs, sent_pcs=sent_pcs,
                       norm_pcs=norm_pcs, norm_sent_pcs=norm_sent_pcs,
                       norm_rfqs=norm_rfqs, norm_sent_rfqs=norm_sent_rfqs,
                       pc_bulk_actions=pc_bulk_actions, rfq_bulk_actions=rfq_bulk_actions,
                       action_items=action_items,
                       nl_query_enabled=_nl_q, docling_intake=_doc_intake)

@bp.route("/growth")
@auth_required
@safe_route
def growth_redirect():
    """Growth page — redirects to /growth-intel (the live module).

    Was previously pointing at /pipeline, which made the home dashboard's
    'Growth Engine' / 'Quick Wins' / nav buttons all dead-end on the wrong
    page. /growth-intel is the actual reachable Growth Engine route.
    """
    return redirect("/growth-intel")


@bp.route("/awards")
@auth_required
@safe_page
def awards_page():
    """Pending PO Award Review page."""
    from src.api.dashboard import _load_pending_pos
    pending = _load_pending_pos()
    if callable(pending):
        pending = pending()  # Guard: if we got the function instead of its result
    if not isinstance(pending, list):
        pending = []
    return render_page("awards.html", active_page="Awards", pending=pending)


@bp.route("/api/award/<int:idx>/approve", methods=["POST"])
@auth_required
@safe_route
def api_award_approve(idx):
    """Approve a pending PO — creates order and marks RFQ/quote as won."""
    from src.api.dashboard import _load_pending_pos, _save_pending_pos, _pending_po_reviews, _create_order_from_po_email
    pending = _load_pending_pos()
    if idx < 0 or idx >= len(pending):
        return jsonify({"ok": False, "error": "Invalid index"})

    po = pending[idx]
    if po.get("review_status") != "pending":
        return jsonify({"ok": False, "error": "Already processed"})

    # Create the order
    try:
        order = _create_order_from_po_email(po)
        po["review_status"] = "approved"
        po["approved_at"] = datetime.now().isoformat()
        po["order_id"] = order.get("id", "") or order.get("order_id", "")
        _save_pending_pos()

        # Also update RFQ status to won
        sol = po.get("sol_number", "")
        if sol:
            rfqs = load_rfqs()
            for rid, r in rfqs.items():
                if r.get("solicitation_number") == sol or r.get("rfq_number") == sol:
                    r["status"] = "won"
                    r["outcome"] = "won"
                    r["outcome_date"] = datetime.now().isoformat()
                    r["po_number"] = po.get("po_number", "")
                    from src.api.dashboard import _save_single_rfq
                    _save_single_rfq(rid, r)
                    break

        return jsonify({
            "ok": True,
            "order_id": po["order_id"],
            "redirect": f"/order/{po['order_id']}",
        })
    except Exception as e:
        log.error("Award approve failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/award/<int:idx>/dismiss", methods=["POST"])
@auth_required
@safe_route
def api_award_dismiss(idx):
    """Dismiss a pending PO (not a real award)."""
    from src.api.dashboard import _load_pending_pos, _save_pending_pos, _pending_po_reviews
    pending = _load_pending_pos()
    if idx < 0 or idx >= len(pending):
        return jsonify({"ok": False, "error": "Invalid index"})

    data = request.get_json(force=True, silent=True) or {}
    pending[idx]["review_status"] = "dismissed"
    pending[idx]["dismiss_reason"] = data.get("reason", "")
    _save_pending_pos()
    return jsonify({"ok": True})


@bp.route("/api/awards/pending")
@auth_required
@safe_route
def api_awards_pending():
    """Get count of pending PO reviews (for home page banner).

    Deduplicates by po_number on the way out so historical duplicates
    (200-entry cap previously accepted the same PO on every redetection)
    don't bloat the banner or crash the home layout with an 80-item
    wall of text.
    """
    from src.api.dashboard import _load_pending_pos, _dedupe_pending_pos
    raw = [p for p in _load_pending_pos() if p.get("review_status") == "pending"]
    pending = _dedupe_pending_pos(raw)
    return jsonify({"ok": True, "count": len(pending), "pending": pending})


@bp.route("/api/awards/purge-dupes", methods=["POST"])
@auth_required
@safe_route
def api_awards_purge_dupes():
    """Admin one-shot: rewrite pending_po_reviews.json with a deduped copy.

    Useful after an incident that left many duplicate entries in the queue
    (2026-04-12 incident: 80 entries collapsed to 6 uniques). Returns
    the before/after counts so the caller can verify.
    """
    from src.api.dashboard import _load_pending_pos, _dedupe_pending_pos, _save_pending_pos
    import src.api.dashboard as _dash
    before = _load_pending_pos()
    before_count = len(before)
    deduped = _dedupe_pending_pos(before)
    _dash._pending_po_reviews[:] = deduped
    _save_pending_pos()
    return jsonify({
        "ok": True,
        "before": before_count,
        "after": len(deduped),
        "removed": before_count - len(deduped),
    })


@bp.route("/api/rfq/<rid>/mark-won", methods=["POST"])
@auth_required
@safe_route
def api_rfq_mark_won(rid):
    """Mark an RFQ as won with PO number."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404
    data = request.get_json(force=True, silent=True) or {}
    po_number = data.get("po_number", "")
    now = datetime.now().isoformat()
    r["status"] = "won"
    r["outcome"] = "won"
    r["outcome_date"] = now
    r["po_number"] = po_number
    r["closed_at"] = now
    r["closed_reason"] = f"Manually marked won — PO {po_number}" if po_number else "Manually marked won"
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    # Log activity
    try:
        from src.api.dashboard import _log_crm_activity
        _log_crm_activity(
            r.get("reytech_quote_number", rid), "quote_won",
            f"RFQ marked WON — PO {po_number}. Total: ${r.get('total', 0):,.2f}",
            actor="user", metadata={"po_number": po_number, "rfq_id": rid})
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_won: %s', _e)
    # Log revenue
    try:
        from src.core.db import log_revenue
        _total = 0
        for it in r.get("line_items", r.get("items", [])):
            _p = float(it.get("price_per_unit") or it.get("unit_price") or it.get("bid_price") or 0)
            _q = float(it.get("quantity") or it.get("qty") or 1)
            _total += _p * _q
        if _total > 0:
            log_revenue(amount=_total, source="rfq_won", quote_number=r.get("reytech_quote_number", ""),
                        po_number=po_number, agency=r.get("agency", ""), date=now[:10])
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_won: %s', _e)
    # Notify
    try:
        from src.agents.notify_agent import send_alert
        send_alert(event_type="quote_won",
                   title=f"RFQ Won — PO {po_number or 'manual'}",
                   body=f"RFQ {r.get('solicitation_number', rid)} marked won. PO: {po_number}",
                   urgency="deal", context={"rfq_id": rid, "po_number": po_number})
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_won: %s', _e)
    # Create Drive folder: Year/Quarter/PO-XXXXX/{RFQ,Supplier,Delivery,Invoice,Misc}
    try:
        from src.core.gdrive import is_configured, enqueue
        if is_configured():
            enqueue({
                "action": "create_po_folder",
                "po_number": po_number or f"RFQ-{rid[:8]}",
                "solicitation_number": r.get("solicitation_number", ""),
                "year": str(datetime.now().year),
                "quarter": f"Q{(datetime.now().month - 1) // 3 + 1}",
            })
            log.info("Drive: PO folder queued for %s (Q%d %d)",
                     po_number, (datetime.now().month - 1) // 3 + 1, datetime.now().year)
    except Exception as _de:
        log.debug("Drive folder creation: %s", _de)

    # Update award tracker — stop SCPRS checking
    try:
        import sqlite3 as _sql
        from src.core.paths import DATA_DIR as _DD
        _aconn = _sql.connect(os.path.join(_DD, "reytech.db"), timeout=10)
        _aconn.execute("""
            INSERT INTO award_tracker_log
            (checked_at, quote_number, scprs_searched, matches_found, outcome, notes)
            VALUES (?,?,0,1,?,?)
        """, (now, r.get("reytech_quote_number", rid),
              "won_manual", f"PO {po_number} — manually marked won"))
        _aconn.commit()
        _aconn.close()
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_won: %s', _e)

    # QA correlation — snapshot QA state at outcome time
    try:
        from src.core.dal import get_lifecycle_events as _gle_won, log_lifecycle_event as _lle_won
        _all_ev = _gle_won("rfq", rid, limit=200)
        _qa_ev = [e for e in _all_ev if e.get("event_type") == "form_qa_completed"]
        _gen_ev = [e for e in _all_ev if e.get("event_type") in ("form_qa_completed",)]
        _pc_ev = [e for e in _all_ev if e.get("event_type") == "pc_qa_completed"]
        _last_qa = _qa_ev[0].get("detail", {}) if _qa_ev else {}
        _last_pc = _pc_ev[0].get("detail", {}) if _pc_ev else {}
        _cats = set()
        for _qe in _qa_ev:
            for _c, _n in _qe.get("detail", {}).get("categories", {}).items():
                if _n > 0:
                    _cats.add(_c)
        _lle_won("rfq", rid, "outcome_qa_correlation",
                 f"Won — QA {'clean' if _last_qa.get('passed', True) else 'dirty'}, "
                 f"{len(_qa_ev)} QA runs",
                 actor="system", detail={
                     "outcome": "won",
                     "last_qa_passed": _last_qa.get("passed", True),
                     "generation_count": len(_qa_ev),
                     "qa_run_count": len(_qa_ev),
                     "last_pc_qa_score": _last_pc.get("score"),
                     "had_blockers": _last_qa.get("critical_count", 0) > 0,
                     "total_critical_issues": sum(e.get("detail", {}).get("critical_count", 0) for e in _qa_ev),
                     "total_warnings": sum(e.get("detail", {}).get("warning_count", 0) for e in _qa_ev),
                     "categories_flagged": list(_cats),
                     "quote_total": float(r.get("total", 0) or 0),
                     "po_number": po_number,
                 })
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_won: %s', _e)

    # V5: Calibrate Oracle from win outcome
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        calibrate_from_outcome(
            r.get("items", r.get("line_items", [])), "won",
            agency=r.get("institution") or r.get("agency", ""),
        )
    except Exception as e:
        log.warning("RFQ mark-won calibration: %s", e)

    log.info("RFQ %s marked WON (PO: %s)", rid, po_number)
    return jsonify({"ok": True, "status": "won", "po_number": po_number})


@bp.route("/api/rfq/<rid>/mark-lost", methods=["POST"])
@auth_required
@safe_route
def api_rfq_mark_lost(rid):
    """Mark an RFQ as lost."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404
    data = request.get_json(force=True, silent=True) or {}
    now = datetime.now().isoformat()
    r["status"] = "lost"
    r["outcome"] = "lost"
    r["outcome_date"] = now
    r["competitor_name"] = data.get("competitor", "")
    r["competitor_price"] = data.get("competitor_price", "")
    r["closed_at"] = now
    r["closed_reason"] = data.get("reason", "Lost to competitor")
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    # CRM Activity
    try:
        from src.api.dashboard import _log_crm_activity
        _log_crm_activity(
            r.get("reytech_quote_number", rid), "quote_lost",
            f"RFQ lost to {data.get('competitor', 'unknown')}. "
            f"Their price: {data.get('competitor_price', 'unknown')}",
            actor="user", metadata={"rfq_id": rid, "competitor": data.get("competitor", "")})
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_lost: %s', _e)

    # Competitor intel — feed into loss intelligence
    try:
        from src.core.db import get_db
        _our_total = sum(
            float(it.get("price_per_unit") or it.get("unit_price") or 0)
            * float(it.get("quantity") or it.get("qty") or 1)
            for it in r.get("line_items", r.get("items", []))
        )
        _comp_price = float(data.get("competitor_price", 0) or 0)
        _delta = _our_total - _comp_price if _comp_price > 0 else 0
        _delta_pct = (_delta / _comp_price * 100) if _comp_price > 0 else 0
        with get_db() as conn:
            conn.execute("""
                INSERT INTO competitor_intel
                (found_at, quote_number, our_price, competitor_name, competitor_price,
                 price_delta, price_delta_pct, agency, institution, outcome, notes,
                 loss_reason_class)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now, r.get("reytech_quote_number", rid), _our_total,
                  data.get("competitor", "Unknown"), _comp_price,
                  round(_delta, 2), round(_delta_pct, 1),
                  r.get("agency", ""), r.get("institution", ""), "lost",
                  f"Manually marked lost: {data.get('reason', '')}",
                  "price_higher" if _delta > 0 else "relationship_incumbent"))
    except Exception as _ce:
        log.debug("Competitor intel: %s", _ce)

    # Notification
    try:
        from src.agents.notify_agent import send_alert
        send_alert(event_type="quote_lost_signal",
                   title=f"RFQ Lost — {data.get('competitor', 'unknown')}",
                   body=f"RFQ {r.get('solicitation_number', rid)} lost to {data.get('competitor', 'unknown')}",
                   urgency="warning", context={"rfq_id": rid})
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_lost: %s', _e)

    # Award tracker — stop SCPRS checking
    try:
        import sqlite3 as _sql
        from src.core.paths import DATA_DIR as _DD
        _aconn = _sql.connect(os.path.join(_DD, "reytech.db"), timeout=10)
        _aconn.execute("""
            INSERT INTO award_tracker_log
            (checked_at, quote_number, scprs_searched, matches_found, outcome, notes)
            VALUES (?,?,0,1,?,?)
        """, (now, r.get("reytech_quote_number", rid),
              "lost_manual", f"Lost to {data.get('competitor', 'unknown')}"))
        _aconn.commit()
        _aconn.close()
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_lost: %s', _e)

    # QA correlation — snapshot QA state at outcome time
    try:
        from src.core.dal import get_lifecycle_events as _gle_lost, log_lifecycle_event as _lle_lost
        _all_ev = _gle_lost("rfq", rid, limit=200)
        _qa_ev = [e for e in _all_ev if e.get("event_type") == "form_qa_completed"]
        _pc_ev = [e for e in _all_ev if e.get("event_type") == "pc_qa_completed"]
        _last_qa = _qa_ev[0].get("detail", {}) if _qa_ev else {}
        _last_pc = _pc_ev[0].get("detail", {}) if _pc_ev else {}
        _cats = set()
        for _qe in _qa_ev:
            for _c, _n in _qe.get("detail", {}).get("categories", {}).items():
                if _n > 0:
                    _cats.add(_c)
        _our_tot = locals().get("_our_total", 0) or 0
        _comp_pr = locals().get("_comp_price", 0) or 0
        _lle_lost("rfq", rid, "outcome_qa_correlation",
                  f"Lost — QA {'clean' if _last_qa.get('passed', True) else 'dirty'}, "
                  f"{len(_qa_ev)} QA runs",
                  actor="system", detail={
                      "outcome": "lost",
                      "last_qa_passed": _last_qa.get("passed", True),
                      "generation_count": len(_qa_ev),
                      "qa_run_count": len(_qa_ev),
                      "last_pc_qa_score": _last_pc.get("score"),
                      "had_blockers": _last_qa.get("critical_count", 0) > 0,
                      "total_critical_issues": sum(e.get("detail", {}).get("critical_count", 0) for e in _qa_ev),
                      "total_warnings": sum(e.get("detail", {}).get("warning_count", 0) for e in _qa_ev),
                      "categories_flagged": list(_cats),
                      "quote_total": float(_our_tot),
                      "competitor_name": data.get("competitor", ""),
                      "competitor_price": float(_comp_pr),
                  })
    except Exception as _e:
        log.debug('suppressed in api_rfq_mark_lost: %s', _e)

    # V5: Calibrate Oracle from loss outcome
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        items = r.get("items", r.get("line_items", []))
        loss_type = "price" if float(data.get("competitor_price", 0) or 0) > 0 else "other"
        calibrate_from_outcome(
            items, "lost",
            agency=r.get("institution") or r.get("agency", ""),
            loss_reason=loss_type,
        )
    except Exception as e:
        log.warning("RFQ mark-lost calibration: %s", e)

    log.info("RFQ %s marked LOST (reason: %s)", rid, r["closed_reason"][:50])
    return jsonify({"ok": True, "status": "lost"})


@bp.route("/api/rfq/create-manual", methods=["POST"])
@auth_required
@safe_route
def api_rfq_create_manual():
    """Create an RFQ manually from the dashboard."""
    data = request.get_json(force=True, silent=True) or {}
    sol = data.get("solicitation_number", "").strip()
    if not sol:
        return jsonify({"ok": False, "error": "solicitation_number required"})

    import uuid
    rid = uuid.uuid4().hex[:8]
    agency_key = data.get("agency", "")
    agency_name = ""
    try:
        from src.core.agency_config import AGENCY_CONFIGS
        if agency_key in AGENCY_CONFIGS:
            agency_name = AGENCY_CONFIGS[agency_key].get("name", agency_key)
    except Exception:
        agency_name = agency_key

    rfq = {
        "id": rid,
        "solicitation_number": sol,
        "rfq_number": sol,
        "agency": agency_key,
        "agency_name": agency_name,
        "requestor_name": data.get("requestor_name", ""),
        "requestor_email": data.get("requestor_email", ""),
        "due_date": data.get("due_date", ""),
        "delivery_location": data.get("delivery_location", ""),
        "status": "new",
        "source": "manual",
        "created_at": datetime.now().isoformat(),
        "received_at": datetime.now().isoformat(),
        "line_items": [],
        "notes": data.get("notes", ""),
    }

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, rfq)

    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("rfq", rid, "manual_create",
            f"RFQ #{sol} created manually — {agency_name} / {data.get('requestor_name', '')}",
            actor="user")
    except Exception as _e:
        log.debug('suppressed in api_rfq_create_manual: %s', _e)

    return jsonify({"ok": True, "rfq_id": rid, "sol": sol})


@bp.route("/api/rfq/<rid>/upload-parse-doc", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_rfq_upload_parse_doc(rid):
    """Upload any document (PDF, image, screenshot) -> parse items -> populate RFQ.

    Tries parsers in order:
    1. AMS 704 (if PDF looks like a 704)
    2. Generic RFQ parser (XFA + text extraction)
    3. Vision parser (Claude vision for scanned/image docs)
    """
    # Telemetry: every manual upload recorded
    try:
        from src.core.utilization import record_feature_use
        record_feature_use("rfq.upload_parse_doc", context={"rfq_id": rid})
    except Exception as _e:
        log.debug('suppressed in api_rfq_upload_parse_doc: %s', _e)

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file uploaded"})

    # Sanitize filename — prevent path traversal and OS issues
    import re as _re_fn
    safe_filename = _re_fn.sub(r'[^a-zA-Z0-9._-]', '_', f.filename or 'upload.pdf')
    filename_lower = safe_filename.lower()
    is_pdf = filename_lower.endswith(".pdf")
    is_image = any(filename_lower.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff"])
    from src.forms.doc_converter import is_office_doc, OFFICE_EXTS
    is_office = is_office_doc(filename_lower)

    if not is_pdf and not is_image and not is_office:
        return jsonify({"ok": False, "error": "Upload a PDF, image, or office document (XLS, XLSX, DOC, DOCX)"})

    upload_dir = os.path.join(DATA_DIR, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    save_path = os.path.join(upload_dir, f"doc_{rid}_{safe_filename}")

    try:
        f.save(save_path)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not save file: {e}"})

    # ── Phase 3: unified ingest shortcut ──
    # When the classifier_v2 feature flag is on, route through the new
    # pipeline instead of the parallel parser chain. Fixes the RFQ
    # 6655f190 class of bugs (wrong parser picked, wrong PC linked)
    # at the root. Gated by flag so it can be disabled in 30 seconds
    # if anything regresses.
    try:
        from src.core.request_classifier import classify_enabled
        if classify_enabled():
            from src.core.ingest_pipeline import process_buyer_request
            log.info("upload-parse-doc: routing through classifier_v2 pipeline")
            result = process_buyer_request(
                files=[save_path],
                email_body=r.get("body_text", ""),
                email_subject=r.get("email_subject", ""),
                email_sender=r.get("requestor_email", ""),
                existing_record_id=rid,
                existing_record_type="rfq",
            )
            return jsonify({
                "ok": result.ok,
                "items": result.items_parsed,
                "parser_used": (result.classification or {}).get("shape", "classifier_v2"),
                "classification": result.classification,
                "linked_pc_id": result.linked_pc_id,
                "link_reason": result.link_reason,
                "link_confidence": result.link_confidence,
                "errors": result.errors,
                "warnings": result.warnings,
                "reasons": result.reasons,
            })
    except Exception as _cv2_e:
        log.warning("classifier_v2 fallthrough: %s", _cv2_e)

    items = []
    parser_used = ""
    header = {}
    vision_error = None

    try:
        if is_pdf:
            # Try 1: AMS 704
            try:
                from src.forms.price_check import parse_ams704
                parsed = parse_ams704(save_path)
                if not parsed.get("error") and parsed.get("line_items"):
                    items = parsed["line_items"]
                    header = parsed.get("header", {})
                    parser_used = "AMS 704"
            except Exception as e:
                log.debug("704 parse failed: %s", e)

            # Try 2: Generic RFQ parser
            if not items:
                try:
                    from src.forms.generic_rfq_parser import parse_generic_rfq
                    parsed = parse_generic_rfq([save_path],
                        subject=r.get("email_subject", ""),
                        sender_email=r.get("requestor_email", ""),
                        body=r.get("body_text", ""))
                    if parsed.get("items"):
                        items = parsed["items"]
                        header = parsed.get("header", {})
                        parser_used = "Generic RFQ"
                except Exception as e:
                    log.debug("Generic parse failed: %s", e)

            # Try 3: Vision parser
            if not items:
                try:
                    from src.forms.vision_parser import parse_with_vision, is_available
                    if not is_available():
                        vision_error = "Vision AI unavailable — check ANTHROPIC_API_KEY"
                    else:
                        parsed = parse_with_vision(save_path)
                        _vitems = (parsed.get("line_items") or parsed.get("items")) if parsed else None
                        if _vitems:
                            items = _vitems
                            header = parsed.get("header", {})
                            parser_used = "Vision AI"
                        else:
                            vision_error = "Vision AI returned no items"
                except Exception as e:
                    vision_error = f"Vision AI error: {e}"

        elif is_image:
            try:
                from src.forms.vision_parser import parse_with_vision, is_available
                if not is_available():
                    vision_error = "Vision AI unavailable (API key not set)"
                else:
                    parsed = parse_with_vision(save_path)
                    _vitems = (parsed.get("line_items") or parsed.get("items")) if parsed else None
                    if _vitems:
                        items = _vitems
                        header = parsed.get("header", {})
                        parser_used = "Vision AI"
                    else:
                        vision_error = "Vision AI returned no items from image"
            except Exception as e:
                vision_error = f"Vision AI error: {e}"

            if not items:
                try:
                    import subprocess
                    ocr_result = subprocess.run(["tesseract", save_path, "stdout"],
                        capture_output=True, text=True, timeout=30)
                    if ocr_result.returncode == 0 and ocr_result.stdout.strip():
                        from src.forms.generic_rfq_parser import parse_line_items_from_text
                        items = parse_line_items_from_text(ocr_result.stdout)
                        if items:
                            parser_used = "OCR + Text"
                except Exception as e:
                    log.debug("OCR parse failed: %s", e)

        elif is_office:
            # Office documents (XLS, XLSX, DOC, DOCX) — extract text, send to Claude API
            try:
                from src.forms.doc_converter import extract_text as _extract_office_text
                from src.forms.vision_parser import parse_from_text, is_available
                doc_text = _extract_office_text(save_path)
                log.info("Office doc extracted %d chars from %s", len(doc_text), safe_filename)
                # Try AI extraction first
                if is_available():
                    parsed = parse_from_text(doc_text, source_path=save_path)
                    _oitems = (parsed.get("line_items") or parsed.get("items")) if parsed else None
                    if _oitems:
                        items = _oitems
                        header = parsed.get("header", {})
                        parser_used = "Office Doc AI"
                if not items:
                    # Regex fallback for simple item lists
                    from src.forms.doc_converter import parse_items_from_text
                    fallback = parse_items_from_text(doc_text)
                    if fallback:
                        items = fallback
                        parser_used = "Office Doc Regex"
                    elif not is_available():
                        vision_error = "AI unavailable and regex found no items"
                    else:
                        vision_error = "Could not extract items from office document"
            except ValueError as ve:
                vision_error = str(ve)
            except Exception as e:
                vision_error = f"Office doc parse error: {e}"

        if not items:
            err_detail = vision_error or "No items could be extracted"
            _tried = (["AMS 704", "Generic RFQ", "Vision AI"] if is_pdf
                      else ["Office Doc AI"] if is_office
                      else ["Vision AI", "OCR"])
            return jsonify({"ok": False, "error": f"Could not extract items. {err_detail}",
                            "parser_tried": _tried})

        # Merge header into RFQ — use ship_to (not ship_to_address)
        if header:
            if header.get("institution") and not r.get("agency_name"):
                r["agency_name"] = header["institution"]
            if header.get("ship_to") and not r.get("delivery_location"):
                r["delivery_location"] = header["ship_to"]
            if header.get("price_check_number") and not r.get("linked_pc_number"):
                r["linked_pc_number"] = header["price_check_number"]
            if header.get("zip_code") and not r.get("tax_validated"):
                try:
                    from src.core.tax_rates import lookup_tax_rate
                    _tr = lookup_tax_rate(zip_code=header["zip_code"])
                    if _tr.get("rate"):
                        r["tax_rate"] = round(_tr["rate"] * 100, 3)
                        r["tax_source"] = _tr.get("source", "fallback")
                        r["tax_validated"] = True
                        r["tax_jurisdiction"] = _tr.get("jurisdiction", "")
                except Exception as _e:
                    log.debug('suppressed in api_rfq_upload_parse_doc: %s', _e)

        # Overwrite: uploaded doc replaces existing items (user intent = replace)
        existing_items = []
        added = 0

        for it in items:
            desc = (it.get("description") or it.get("name") or "").strip()
            if not desc or len(desc) < 3:
                continue
            try:
                qty = int(float(it.get("qty") or it.get("quantity") or 1))
            except (ValueError, TypeError):
                qty = 1
            uom = (it.get("uom") or it.get("unit_of_measure") or "EA").strip()
            part = (it.get("mfg_number") or it.get("part_number") or "").strip()
            if not part:
                raw = str(it.get("item_number", "")).strip()
                if raw and not raw.isdigit():
                    part = raw
            try:
                cost = float(it.get("price") or it.get("unit_price") or it.get("cost") or 0)
            except (ValueError, TypeError):
                cost = 0.0

            new_item = {"description": desc, "qty": qty, "uom": uom,
                        "line_number": added + 1,
                        "part_number": part, "item_number": part,
                        "supplier_cost": cost if cost > 0 else 0,
                        "price_per_unit": 0, "markup_pct": 0}
            if cost > 0:
                new_item["cost_source"] = f"Uploaded ({parser_used})"
            existing_items.append(new_item)
            added += 1

        # Only update the RFQ after all items built successfully
        r["line_items"] = existing_items
        try:
            from src.api.dashboard import _save_single_rfq
            _save_single_rfq(rid, r)
        except Exception as save_err:
            log.error("upload-parse _save_single_rfq failed for %s: %s", rid, save_err, exc_info=True)
            return jsonify({"ok": False, "error": f"Items parsed but could not save: {save_err}"})

        try:
            from src.core.dal import log_lifecycle_event
            log_lifecycle_event("rfq", rid, "doc_uploaded",
                f"Uploaded {f.filename}: {added} items via {parser_used}", actor="user")
        except Exception as _e:
            log.debug('suppressed in api_rfq_upload_parse_doc: %s', _e)

        return jsonify({"ok": True, "items_found": len(items), "items_added": added,
                        "parser": parser_used, "header": header})

    except Exception as e:
        log.error("upload-parse-doc unexpected error for %s: %s", rid, e, exc_info=True)
        return jsonify({"ok": False, "error": f"Unexpected error: {e}"})
    finally:
        try:
            os.remove(save_path)
        except OSError as _e:
            log.debug("suppressed: %s", _e)


@bp.route("/upload", methods=["POST"])
@auth_required
@safe_page
@rate_limit("heavy")
def upload():
    files = request.files.getlist("files")
    if not files:
        flash("No files uploaded", "error"); return redirect("/")
    
    rfq_id = str(uuid.uuid4())[:8]
    rfq_dir = os.path.join(UPLOAD_DIR, rfq_id)
    os.makedirs(rfq_dir, exist_ok=True)
    
    saved = []
    office_files = []
    from src.forms.doc_converter import is_office_doc, ALL_UPLOAD_EXTS
    for f in files:
        safe_fn = _safe_filename(f.filename)
        if not safe_fn:
            continue
        ext = os.path.splitext(safe_fn)[1].lower()
        if ext == ".pdf":
            p = os.path.join(rfq_dir, safe_fn)
            f.save(p); saved.append(p)
        elif is_office_doc(safe_fn):
            p = os.path.join(rfq_dir, safe_fn)
            f.save(p); office_files.append(p)

    if not saved and not office_files:
        flash("No supported files found (PDF, XLS, XLSX, DOC, DOCX)", "error"); return redirect("/")

    # Office docs: extract text → Claude AI → create RFQ directly
    if office_files and not saved:
        try:
            from src.forms.doc_converter import extract_text as _extract_office_text
            from src.forms.vision_parser import parse_from_text, is_available as _vis_avail
            if not _vis_avail():
                flash("AI parsing unavailable — upload a PDF instead", "error"); return redirect("/")
            combined_text = ""
            for of in office_files:
                combined_text += _extract_office_text(of) + "\n\n"
            parsed = None
            if _vis_avail():
                parsed = parse_from_text(combined_text, source_path=office_files[0])
            if not parsed or not parsed.get("line_items"):
                # Regex fallback
                from src.forms.doc_converter import parse_items_from_text
                fallback = parse_items_from_text(combined_text)
                if fallback:
                    parsed = {"line_items": fallback, "header": {},
                              "parse_method": "regex_fallback", "source_pdf": office_files[0]}
                else:
                    flash("Could not extract items from office document", "error"); return redirect("/")
            # Check if it looks like a Price Check
            header = parsed.get("header", {})
            pc_num = header.get("price_check_number", "")
            if PRICE_CHECK_AVAILABLE and pc_num:
                # Treat as a Price Check
                return _handle_office_pc_upload(parsed, office_files[0], rfq_id)
            # Build RFQ from parsed data
            from src.forms.price_check import _filter_junk_items
            parsed["line_items"] = _filter_junk_items(parsed.get("line_items", []))
            rfq = {
                "id": rfq_id,
                "source": "upload",
                "solicitation_number": header.get("price_check_number", ""),
                "agency": header.get("institution", ""),
                "due_date": header.get("due_date", ""),
                "line_items": parsed["line_items"],
                "delivery_location": header.get("delivery_zip", ""),
            }
            rfq["line_items"] = bulk_lookup(rfq.get("line_items", []))
            for _item in rfq.get("line_items", []):
                _sp = _item.get("scprs_last_price") or 0
                _ap = _item.get("amazon_price") or 0
                _best_cost = _sp or _ap
                if _best_cost and not _item.get("supplier_cost"):
                    try:
                        _item["supplier_cost"] = float(_best_cost)
                        _item["cost_source"] = "SCPRS" if _sp else "Amazon"
                    except (ValueError, TypeError) as _e:
                        log.debug("suppressed: %s", _e)
            items = rfq.get("line_items", [])
            priced_count = sum(1 for i in items if i.get("price_per_unit") or i.get("scprs_last_price"))
            rfq["auto_lookup_results"] = {
                "scprs_found": sum(1 for i in items if i.get("scprs_last_price")),
                "amazon_found": sum(1 for i in items if i.get("amazon_price")),
                "catalog_found": 0, "priced": priced_count, "total": len(items),
                "ran_at": datetime.now().isoformat(),
            }
            if priced_count > 0:
                _transition_status(rfq, "priced", actor="system", notes=f"Parsed office doc + {priced_count}/{len(items)} priced")
            else:
                _transition_status(rfq, "draft", actor="system", notes="Parsed from office doc upload")
            from src.api.dashboard import _save_single_rfq
            _save_single_rfq(rfq_id, rfq)
            flash(f"Parsed {len(items)} items from office document", "success")
            return redirect(f"/rfq/{rfq_id}")
        except ValueError as ve:
            flash(str(ve), "error"); return redirect("/")
        except Exception as e:
            log.error("Office doc upload failed: %s", e, exc_info=True)
            flash(f"Office doc parse failed: {e}", "error"); return redirect("/")

    log.info("Upload: %d PDFs saved to %s", len(saved), rfq_id)

    # Check if this is a Price Check (AMS 704) instead of an RFQ
    if PRICE_CHECK_AVAILABLE and len(saved) == 1:
        if _is_price_check(saved[0]):
            return _handle_price_check_upload(saved[0], rfq_id)

    templates = identify_attachments(saved)
    if "704b" not in templates:
        flash("Could not identify 704B", "error"); return redirect("/")
    
    rfq = parse_rfq_attachments(templates)
    rfq["id"] = rfq_id
    rfq["source"] = "upload"

    # Filter out junk items (legal text, instructions, boilerplate)
    from src.forms.price_check import _filter_junk_items
    rfq["line_items"] = _filter_junk_items(rfq.get("line_items", []))

    # Auto SCPRS lookup
    rfq["line_items"] = bulk_lookup(rfq.get("line_items", []))

    # ── Dedup check: reject if same solicitation + agency + due_date exists ──
    sol = rfq.get("solicitation_number", "")
    agency = rfq.get("agency", "")
    due = rfq.get("due_date", "")
    if sol and agency:
        existing = load_rfqs()
        for eid, er in existing.items():
            if (er.get("solicitation_number") == sol
                    and er.get("agency") == agency
                    and er.get("due_date") == due):
                log.warning("Duplicate RFQ upload blocked: %s for %s (existing ID: %s)", sol, agency, eid)
                flash(f"Duplicate RFQ: {sol} for {agency} already exists (ID: {eid})", "error")
                return redirect(f"/rfq/{eid}")

    # Carry SCPRS/Amazon cost to supplier_cost so YOUR COST column displays it
    for _item in rfq.get("line_items", []):
        _sp = _item.get("scprs_last_price") or 0
        _ap = _item.get("amazon_price") or 0
        _best_cost = _sp or _ap
        if _best_cost and not _item.get("supplier_cost"):
            try:
                _item["supplier_cost"] = float(_best_cost)
                _item["cost_source"] = "SCPRS" if _sp else "Amazon"
            except (ValueError, TypeError) as _e:
                log.debug("suppressed: %s", _e)

    # Store lookup results summary
    items = rfq.get("line_items", [])
    priced_count = sum(1 for i in items if i.get("price_per_unit") or i.get("scprs_last_price"))
    rfq["auto_lookup_results"] = {
        "scprs_found": sum(1 for i in items if i.get("scprs_last_price")),
        "amazon_found": sum(1 for i in items if i.get("amazon_price")),
        "catalog_found": 0,
        "priced": priced_count,
        "total": len(items),
        "ran_at": datetime.now().isoformat(),
    }
    
    # Set status based on whether prices were actually found
    if priced_count > 0:
        _transition_status(rfq, "priced", actor="system", notes=f"Parsed + {priced_count}/{len(items)} items priced")
    else:
        _transition_status(rfq, "draft", actor="system", notes="Parsed from upload — no prices found yet")
    
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rfq_id, rfq)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rfq_id, rfq.get("status", "draft"))
    except Exception as _e:
        log.debug('suppressed in upload: %s', _e)

    scprs_found = sum(1 for i in rfq["line_items"] if i.get("scprs_last_price"))
    msg = f"RFQ #{rfq['solicitation_number']} parsed — {len(rfq['line_items'])} items"
    if scprs_found:
        msg += f", {scprs_found} SCPRS prices found"
    flash(msg, "success")
    return redirect(f"/rfq/{rfq_id}")


def _is_price_check(pdf_path):
    """Detect if a PDF is an AMS 704 Price Check (NOT 704B quote worksheet).
    
    Uses filename first (fast, reliable), falls back to PDF content parsing.
    """
    basename = os.path.basename(pdf_path).lower()
    
    # ── Filename-based detection (fast path) ──
    # Exclude 704B / 703B / bid package by filename
    if any(x in basename for x in ["704b", "703b", "bid package", "bid_package", "quote worksheet"]):
        return False
    
    # Positive filename match: "AMS 704" or "ams704" in filename (but NOT 704B)
    if "704" in basename and "ams" in basename:
        return True
    # Also match "Quote - [Name] - [Date]" pattern (Valentina's format)
    # These always carry a single AMS 704 attachment
    if basename.startswith("quote") and basename.endswith(".pdf") and "704b" not in basename:
        # Only if filename looks like a price check attachment, not a generated quote
        if any(x in basename for x in ["ams", "704", "price"]):
            return True
    
    # ── PDF content fallback ──
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        text = reader.pages[0].extract_text() or ""
        text_lower = text.lower()
        
        # Exclude 704B forms
        if any(marker in text_lower for marker in ["704b", "quote worksheet", "acquisition quote"]):
            return False
        
        if "price check" in text_lower and ("ams 704" in text_lower or "worksheet" in text_lower):
            return True
        # Check form fields for AMS 704 patterns
        fields = reader.get_fields()
        if fields:
            field_names = set(fields.keys())
            ams704_markers = {"COMPANY NAME", "Requestor", "PRICE PER UNITRow1", "EXTENSIONRow1"}
            if len(ams704_markers & field_names) >= 3:
                return True
    except Exception as e:
        log.debug("PDF parse fallback failed for %s: %s", basename, e)
    return False


# ═══════════════════════════════════════════════════════════════════════
# Status Lifecycle — tracks every transition for PCs and RFQs
# ═══════════════════════════════════════════════════════════════════════

# PC lifecycle: parsed → priced → completed → won/lost/expired
# RFQ lifecycle: new → pending → ready → generated → sent → won/lost
PC_LIFECYCLE = ["parsed", "priced", "completed", "won", "lost", "expired"]
RFQ_LIFECYCLE = ["new", "pending", "ready", "generated", "sent", "won", "lost"]


def _transition_status(record, new_status, actor="system", notes=""):
    """Record a status transition with full history.

    Mutates record in place. Returns the record for chaining.
    """
    old_status = record.get("status", "")

    # Validate transition
    try:
        from src.core.quote_validator import validate_transition
        check = validate_transition(old_status, new_status)
        if not check["ok"]:
            log.warning("BLOCKED transition: %s -> %s (%s)",
                       old_status, new_status, check["error"])
            try:
                from flask import flash as _flash
                _flash(f"Unusual status change: {old_status} -> {new_status}", "warning")
            except Exception as _e:
                log.debug('suppressed in _transition_status: %s', _e)
    except Exception as _e:
        log.debug('suppressed in _transition_status: %s', _e)

    record["status"] = new_status
    now = datetime.now().isoformat()
    record["status_updated"] = now

    # Build status_history (create if missing for legacy records)
    history = record.get("status_history", [])
    entry = {"from": old_status, "to": new_status, "timestamp": now, "actor": actor}
    if notes:
        entry["notes"] = notes
    history.append(entry)
    record["status_history"] = history

    # Speed clock tracking
    try:
        from src.core.pricing_oracle_v2 import record_speed_event
        record_id = record.get("id") or record.get("pc_id") or ""
        record_type = "pc" if "pc_data" in record or "pc_number" in record else "quote"
        speed_map = {"parsed": "received", "draft": "received", "new": "received",
                     "priced": "priced", "ready": "priced", "generated": "generated",
                     "sent": "sent", "submitted": "sent"}
        event = speed_map.get(new_status)
        if event and record_id:
            record_speed_event(record_type, record_id, event)
    except Exception as _e:
        log.debug('suppressed in _transition_status: %s', _e)

    # On win: confirm item mappings + lock costs
    if new_status in ("won", "awarded"):
        try:
            from src.core.pricing_oracle_v2 import confirm_item_mapping, lock_cost
            items = record.get("line_items", record.get("items", []))
            if isinstance(items, str):
                import json as _json
                items = _json.loads(items)
            for item in (items or []):
                desc = item.get("description", "")
                cost = item.get("supplier_cost") or item.get("unit_cost") or item.get("cost")
                sell = item.get("unit_price") or item.get("sell_price") or item.get("price")
                if desc and sell:
                    confirm_item_mapping(
                        original_description=desc, canonical_description=desc,
                        item_number=item.get("item_number", ""),
                        supplier=item.get("item_supplier", ""),
                        cost=float(str(cost or 0).replace("$", "").replace(",", "")) if cost else None,
                    )
                if desc and cost:
                    try:
                        lock_cost(desc, float(str(cost).replace("$", "").replace(",", "")),
                                  supplier=item.get("item_supplier", ""),
                                  source="won_quote", expires_days=60,
                                  item_number=item.get("item_number", ""))
                    except Exception as _e:
                        log.debug('suppressed in _transition_status: %s', _e)
        except Exception as _e:
            log.debug('suppressed in _transition_status: %s', _e)

    # Post-send pipeline: schedule follow-ups and tracking
    if new_status in ("sent", "submitted"):
        try:
            from src.agents.post_send_pipeline import on_quote_sent
            record_type = "pc" if "pc_data" in record or "pc_number" in record else "rfq"
            on_quote_sent(record_type,
                         record.get("id", record.get("pc_id", "")),
                         record)
        except Exception as _e:
            log.warning("Post-send pipeline: %s", _e)

    return record


def _handle_office_pc_upload(parsed, source_path, pc_id):
    """Create a Price Check from office-doc-parsed data (same flow as PDF upload)."""
    items = parsed.get("line_items", [])
    header = parsed.get("header", {})
    pc_num = header.get("price_check_number", "PC")
    institution = header.get("institution", "")
    due_date = header.get("due_date", "")
    now = datetime.now().isoformat()

    # Dedup check
    pcs = _load_price_checks()
    for existing_id, existing_pc in pcs.items():
        if (existing_pc.get("pc_number", "").strip() == pc_num.strip()
                and existing_pc.get("institution", "").strip().lower() == institution.strip().lower()
                and existing_pc.get("due_date", "").strip() == due_date.strip()
                and pc_num != "unknown"):
            return redirect(f"/pricecheck/{existing_id}")

    # Save PC record
    pcs[pc_id] = {
        "id": pc_id,
        "pc_number": pc_num,
        "institution": institution,
        "due_date": due_date,
        "requestor": header.get("requestor", ""),
        "ship_to": parsed.get("ship_to", ""),
        "phone": header.get("phone", ""),
        "agency": institution,
        "items": items,
        "source_pdf": source_path,
        "status": "new",
        "status_history": [
            {"from": "", "to": "parsed", "timestamp": now, "actor": "system",
             "notes": f"Parsed {len(items)} items from office doc"},
            {"from": "parsed", "to": "new", "timestamp": now, "actor": "system",
             "notes": "Source: office_doc_upload"},
        ],
        "created_at": now,
        "source": "manual_upload",
        "parsed": parsed,
        "reytech_quote_number": "",
        "linked_quote_number": "",
    }
    _save_price_checks(pcs)
    log.info("PC #%s created from office doc — %d items from %s", pc_num, len(items), institution)
    flash(f"Price Check #{pc_num} — {len(items)} items from {institution}. Due {due_date}", "success")
    return redirect(f"/pricecheck/{pc_id}")


def _handle_price_check_upload(pdf_path, pc_id, from_email=False):
    """Process an uploaded Price Check PDF.
    
    Full pipeline:
    1. Parse PDF → extract header + line items
    2. Dedup check
    3. Catalog matching → pull costs, MFG#, UOM from known products
    4. Save with status 'new' (ready for work in queue)
    5. Return/redirect to PC detail page
    
    Args:
        from_email: If True, returns dict instead of redirect (email pipeline call)
    """
    # Save to data dir for persistence
    pc_file = os.path.join(DATA_DIR, f"pc_upload_{os.path.basename(pdf_path)}")
    shutil.copy2(pdf_path, pc_file)

    # Parse
    parsed = parse_ams704(pc_file)
    parse_error = parsed.get("error")
    now = datetime.now().isoformat()
    source = "email_auto" if from_email else "manual_upload"
    
    if parse_error:
        if from_email:
            # Still create a minimal PC so the email isn't lost
            log.warning("PC parse failed for %s: %s — creating minimal PC with PDF attached",
                        os.path.basename(pdf_path), parse_error)
            pcs = _load_price_checks()
            pcs[pc_id] = {
                "id": pc_id,
                "pc_number": os.path.basename(pdf_path).replace(".pdf", "").replace("pc_upload_", "")[:40],
                "institution": "",
                "due_date": "",
                "requestor": "",
                "ship_to": "",
                "items": [],
                "source_pdf": pc_file,
                "status": "parse_error",
                "status_history": [{"from": "", "to": "parse_error", "timestamp": now, "actor": "system"}],
                "created_at": now,
                "source": source,
                "parsed": {"error": parse_error},
                "parse_error": parse_error,
                "reytech_quote_number": "",
                "linked_quote_number": "",
            }
            _save_price_checks(pcs)
            return {"ok": True, "pc_id": pc_id, "parse_error": parse_error, "items": 0}
        flash(f"Price Check parse error: {parse_error}", "error")
        return redirect("/")

    items = parsed.get("line_items", [])
    header = parsed.get("header", {})
    pc_num = header.get("price_check_number", "PC")
    institution = header.get("institution", "")
    due_date = header.get("due_date", "")

    # ── DEDUP CHECK: same PC number + institution + due date = true duplicate ──
    pcs = _load_price_checks()
    for existing_id, existing_pc in pcs.items():
        if (existing_pc.get("pc_number", "").strip() == pc_num.strip()
                and existing_pc.get("institution", "").strip().lower() == institution.strip().lower()
                and existing_pc.get("due_date", "").strip() == due_date.strip()
                and pc_num != "unknown"):
            log.info("Dedup: PC #%s from %s (due %s) already exists as %s — skipping",
                     pc_num, institution, due_date, existing_id)
            if from_email:
                return {"dedup": True, "existing_id": existing_id}
            return redirect(f"/pricecheck/{existing_id}")

    # ── CATALOG MATCHING: enrich items with known costs, MFG#, UOM ──
    try:
        from src.agents.product_catalog import match_item as _cat_match, init_catalog_db as _cat_init
        _cat_init()
        for item in items:
            desc = (item.get("description") or "").strip()
            mfg = (item.get("mfg_number") or "").strip()
            if not desc and not mfg:
                continue
            matches = _cat_match(desc, mfg, top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.50:
                best = matches[0]
                # Initialize pricing dict
                pricing = item.get("pricing", {})
                if not pricing:
                    item["pricing"] = pricing
                # Pull catalog data into the item
                if best.get("cost") and not pricing.get("unit_cost"):
                    pricing["unit_cost"] = best["cost"]
                    pricing["price_source"] = "catalog"
                if best.get("sell_price") and not pricing.get("recommended_price"):
                    pricing["recommended_price"] = best["sell_price"]
                if best.get("mfg_number") and not item.get("mfg_number"):
                    item["mfg_number"] = best["mfg_number"]
                if best.get("uom"):
                    item["uom"] = best["uom"]
                if best.get("manufacturer"):
                    pricing["manufacturer"] = best["manufacturer"]
                pricing["catalog_match"] = best.get("name", "")[:50]
                pricing["catalog_id"] = best.get("id")
                pricing["catalog_confidence"] = best.get("match_confidence", 0)
                log.info("  catalog match for '%s': %s (%.0f%%) cost=$%.2f",
                         desc[:30], best.get("name", "")[:30],
                         best.get("match_confidence", 0) * 100,
                         best.get("cost", 0))
    except Exception as e:
        log.debug("Catalog matching on upload failed (non-fatal): %s", e)

    # ── Save PC Record ──
    pcs = _load_price_checks()
    pcs[pc_id] = {
        "id": pc_id,
        "pc_number": pc_num,
        "institution": institution,
        "due_date": due_date,
        "requestor": header.get("requestor", ""),
        "ship_to": parsed.get("ship_to", ""),
        "phone": header.get("phone", ""),
        "agency": institution,
        "items": items,
        "source_pdf": pc_file,
        "status": "new",
        "status_history": [
            {"from": "", "to": "parsed", "timestamp": now, "actor": "system", "notes": f"Parsed {len(items)} items"},
            {"from": "parsed", "to": "new", "timestamp": now, "actor": "system", "notes": f"Source: {source}"},
        ],
        "created_at": now,
        "source": source,
        "parsed": parsed,
        "reytech_quote_number": "",
        "linked_quote_number": "",
    }
    _save_price_checks(pcs)

    log.info("PC #%s created (%s) — %d items from %s, due %s, status=new",
             pc_num, source, len(items), institution, due_date)
    
    if from_email:
        return {"ok": True, "pc_id": pc_id, "pc_number": pc_num, "items": len(items)}
    
    flash(f"Price Check #{pc_num} — {len(items)} items from {institution}. Due {due_date}", "success")
    return redirect(f"/pricecheck/{pc_id}")


# _load_price_checks and _save_price_checks are inherited from dashboard.py globals
# via exec() module loading. Do NOT redefine them here — it overwrites the originals
# and creates circular imports (routes_rfq wrapper → dashboard import → gets wrapper back).


@bp.route("/rfq/<rid>")
@auth_required
@safe_route
def detail(rid):
    _bad = _validate_rid(rid)
    if _bad: return _bad
    # WARNING: GET handler — must NEVER call save_rfqs() or modify data.
    # Data loss incident 2026-03-16: save_rfqs in GET handler corrupted items.
    # Check if this is actually a price check
    pcs = _load_price_checks()
    if rid in pcs:
        return redirect(f"/pricecheck/{rid}")
    rfqs = load_rfqs()
    _r_orig = rfqs.get(rid)
    # Fallback: search by solicitation_number if direct ID lookup fails
    if not _r_orig:
        for _frid, _fr in rfqs.items():
            if (_fr.get("solicitation_number") == rid or _fr.get("rfq_number") == rid
                    or _fr.get("id") == rid):
                _r_orig = _fr
                rid = _frid  # use the actual dict key
                break
    if not _r_orig: flash("Not found", "error"); return redirect("/")

    # Quote Model V2 adapter: when flag is on, round-trip through pydantic
    # for validation + computed fields. Falls back to raw dict if flag is off.
    try:
        from src.core.quote_adapter import adapt_rfq
        _r_orig = adapt_rfq(_r_orig, rid)
    except Exception as _adapt_e:
        log.debug("Quote adapter skipped: %s", _adapt_e)

    # CRITICAL: deep copy for rendering — never mutate cached objects.
    # load_rfqs() has in-memory cache. Mutating r here (item mapping, intelligence
    # trimming) would persist in cache and corrupt data on next save.
    import copy as _copy
    r = _copy.deepcopy(_r_orig)

    # Ensure r is a plain dict (not a Jinja2-aware object)
    if not isinstance(r, dict):
        r = dict(r) if hasattr(r, 'items') else {}

    # ── Restore template paths from DB if files missing from disk (post-redeploy) ──
    tmpl = r.get("templates", {})
    db_files = list_rfq_files(rid, category="template")
    restored = False
    for db_f in db_files:
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
            full_f = get_rfq_file(db_f["id"])
            if full_f and full_f.get("data"):
                restore_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, db_f["filename"])
                with open(restore_path, "wb") as _fw:
                    _fw.write(full_f["data"])
                tmpl[ttype] = restore_path
                restored = True
    if restored:
        r["templates"] = tmpl
        rfqs[rid] = r
        r["_needs_save"] = True  # Deferred to POST /rfq/{rid}/save-restore
    
    # ── Restore output_files from DB if empty (post-redeploy) ──
    if not r.get("output_files") and r.get("status") in ("generated", "sent", "won", "lost"):
        db_gen_files = list_rfq_files(rid, category="generated")
        if db_gen_files:
            r["output_files"] = [f["filename"] for f in db_gen_files]
            # Also restore files to disk for download
            for db_f in db_gen_files:
                fname = db_f.get("filename", "")
                sol = r.get("solicitation_number", rid)
                restore_dir = os.path.join(OUTPUT_DIR, sol)
                restore_path = os.path.join(restore_dir, fname)
                if not os.path.exists(restore_path):
                    full_f = get_rfq_file(db_f["id"])
                    if full_f and full_f.get("data"):
                        os.makedirs(restore_dir, exist_ok=True)
                        with open(restore_path, "wb") as _fw:
                            _fw.write(full_f["data"])
            rfqs[rid] = r
            r["_needs_save"] = True  # Deferred to POST /rfq/{rid}/save-restore

    # ── Auto-fill delivery_location via full priority chain ──
    # PO history → CRM contact → institution resolver. Updated
    # 2026-04-14 from the old institution-only path so buyers with
    # established PO history resolve to their real delivery address.
    if not r.get("delivery_location"):
        try:
            from src.core.ship_to_resolver import lookup_buyer_ship_to
            _inst_name = r.get("agency_name", "")
            _buyer_name = r.get("requestor_name") or r.get("buyer_name", "")
            _buyer_email = r.get("requestor_email") or r.get("buyer_email", "")
            _resolved = lookup_buyer_ship_to(
                name=_buyer_name, email=_buyer_email, institution=_inst_name)
            if _resolved.get("ship_to"):
                r["delivery_location"] = _resolved["ship_to"]
                log.info("RFQ delivery_location auto-filled (%s) for %s: %s",
                         _resolved.get("source", "?"),
                         _buyer_name or _inst_name,
                         _resolved["ship_to"][:50])
        except Exception as _sta:
            log.debug("RFQ delivery_location auto-fill: %s", _sta)

    # ── Enrichment: catalog matches + price history on detail load ──
    try:
        _enrich_items = r.get("line_items") or r.get("items", [])
        if isinstance(_enrich_items, str):
            import json as _json
            _enrich_items = _json.loads(_enrich_items)
        if isinstance(_enrich_items, list) and _enrich_items:
            _enrich_items_with_intel(
                _enrich_items,
                rfq_number=r.get("solicitation_number", r.get("rfq_number", "")),
                agency=r.get("agency", ""),
            )
    except Exception as _enrich_err:
        log.warning("RFQ enrichment error (non-fatal): %s", _enrich_err)

    # Map items → line_items (SQLite column is "items", template expects "line_items")
    # Also handle: items might be a JSON string, a list, or missing
    if "line_items" not in r or not r["line_items"]:
        items_data = r.get("items", [])
        if isinstance(items_data, str):
            try:
                import json as _json
                items_data = _json.loads(items_data)
            except Exception:
                items_data = []
        if isinstance(items_data, list) and items_data:
            r["line_items"] = items_data

    if not isinstance(r.get("line_items"), list):
        r["line_items"] = []

    # Also map solicitation_number from rfq_number (SQLite vs JSON field names)
    if not r.get("solicitation_number") and r.get("rfq_number"):
        r["solicitation_number"] = r["rfq_number"]

    # Show link suggestion if unlinked (read-only — no save_rfqs here)
    if not r.get("linked_pc_id"):
        try:
            from src.core.pc_rfq_linker import find_matching_pc, expand_to_bundle
            from src.api.dashboard import _load_price_checks as _dash_load_pcs
            pcs = _dash_load_pcs()
            pc_id, pc_data, reason = find_matching_pc(r, pcs)
            if pc_id:
                r["_suggested_pc"] = pc_id
                r["_suggested_pc_reason"] = reason
                pc_inner = pc_data.get("pc_data", pc_data)
                if isinstance(pc_inner, str):
                    try:
                        import json as _json
                        pc_inner = _json.loads(pc_inner)
                    except Exception:
                        pc_inner = {}
                r["_suggested_pc_number"] = pc_inner.get("pc_number", pc_data.get("pc_number", ""))
                r["_suggested_pc_items"] = len(pc_inner.get("items", pc_data.get("items", [])))
                # Bundle context: if suggested PC is bundled, show bundle info
                bid = pc_inner.get("bundle_id") or pc_data.get("bundle_id", "")
                if bid:
                    siblings = expand_to_bundle(pc_id, pcs)
                    r["_suggested_bundle_id"] = bid
                    r["_suggested_bundle_pcs"] = len(siblings)
        except Exception as _e:
            log.debug('suppressed in detail: %s', _e)

    # ── Sibling RFQ/PC discovery for bundle-linked RFQs ──
    _sibling_rfqs = []
    _sibling_pcs_unconverted = []
    if r.get("bundle_id"):
        try:
            _all_rfqs = load_rfqs()
            for _sid, _sr in _all_rfqs.items():
                if _sr.get("bundle_id") == r["bundle_id"] and _sid != rid:
                    _sibling_rfqs.append({
                        "id": _sid,
                        "sol": _sr.get("solicitation_number", ""),
                        "institution": _sr.get("institution", _sr.get("department", "")),
                        "status": _sr.get("status", ""),
                    })
            from src.api.dashboard import _load_price_checks as _dash_load_pcs2
            _bpcs = _dash_load_pcs2()
            for _pcid, _bpc in _bpcs.items():
                if _bpc.get("bundle_id") == r["bundle_id"] and not _bpc.get("converted_to_rfq"):
                    _sibling_pcs_unconverted.append({
                        "id": _pcid, "pc_number": _bpc.get("pc_number", ""),
                    })
        except Exception as _e:
            log.debug('suppressed in detail: %s', _e)

    # Trim intelligence blobs to prevent page crash / slow render
    import json as _json_trim
    _items_list = r.get("line_items", r.get("items", []))
    try:
        _items_size = len(_json_trim.dumps(_items_list, default=str))
        if _items_size > 100000:  # >100KB of item JSON
            for _itm in _items_list:
                _itm.pop("intelligence", None)
                _itm.pop("oracle", None)
            log.warning("DETAIL %s: stripped intelligence blobs (%.0fKB)", rid, _items_size / 1024)
        else:
            for _itm in _items_list:
                _intel = _itm.get("intelligence", {})
                if isinstance(_intel, dict):
                    _cms = _intel.get("catalog_matches", [])
                    if len(_cms) > 3:
                        _intel["catalog_matches"] = _cms[:3]
                    for _cm in _intel.get("catalog_matches", []):
                        for _fld in ("description", "enriched_description"):
                            _v = _cm.get(_fld, "")
                            if len(_v) > 100:
                                _cm[_fld] = _v[:100] + "..."
    except Exception as _e:
        log.debug('suppressed in detail: %s', _e)

    log.info("RFQ detail render: rid=%s, line_items=%d", rid, len(_items_list))

    # Pass agency required_forms so checkboxes default correctly
    _agency_req = set()
    _agency_key = "other"
    _agency_name = "Unknown"
    _agency_matched_by = ""
    try:
        from src.core.agency_config import match_agency
        _ak, _ac = match_agency(r)
        _agency_key = _ak
        _agency_name = _ac.get("name", _ak)
        _agency_matched_by = _ac.get("matched_by", "")
        _agency_req = set(_ac.get("required_forms", []))
        # Store matched agency on RFQ for persistence
        if _ak != "other" and r.get("agency_key") != _ak:
            r["agency_key"] = _ak
            r["agency_name_resolved"] = _agency_name
    except Exception as _e:
        log.debug('suppressed in detail: %s', _e)

    # ── Normalize due_date to ISO for <input type="date"> ──
    _due_date_iso = ""
    _raw_due = r.get("due_date", "")
    if _raw_due:
        from datetime import datetime as _ddt
        for _dfmt in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%Y-%m-%d"):
            try:
                _due_date_iso = _ddt.strptime(str(_raw_due).strip(), _dfmt).strftime("%Y-%m-%d")
                break
            except (ValueError, TypeError):
                continue

    # ── Compute landed cost summary for margin truth ──
    _landed_summary = {"raw_cost": 0, "landed_cost": 0, "items": 0}
    try:
        from src.core.db import calc_landed_cost
        _li = r.get("line_items", [])
        for _it in _li:
            _sc = _it.get("supplier_cost") or 0
            _sup = _it.get("item_supplier") or ""
            _q = _it.get("qty", 1) or 1
            if _sc and _sc > 0:
                try:
                    _sc = float(_sc)
                except (ValueError, TypeError):
                    continue
                _lc = calc_landed_cost(_sc, _q, _sup)
                _landed_summary["raw_cost"] += _sc * _q
                _landed_summary["landed_cost"] += _lc["landed_cost"] * _q
                _landed_summary["items"] += 1
    except Exception as _e:
        log.debug('suppressed in detail: %s', _e)

    # Parse extracted requirements for display
    _requirements = {}
    try:
        import json as _rj
        _req_raw = r.get("requirements_json", "{}")
        _requirements = _rj.loads(_req_raw) if _req_raw and _req_raw != "{}" else {}
    except Exception as _e:
        log.debug('suppressed in detail: %s', _e)

    return render_page("rfq_detail.html", active_page="Home", r=r, rid=rid,
                       agency_required_forms=_agency_req,
                       agency_key=_agency_key,
                       agency_name=_agency_name,
                       agency_matched_by=_agency_matched_by,
                       sibling_rfqs=_sibling_rfqs,
                       sibling_pcs_unconverted=_sibling_pcs_unconverted,
                       due_date_iso=_due_date_iso,
                       landed_summary=_landed_summary,
                       requirements=_requirements)


@bp.route("/rfq/<rid>/review-package")
@auth_required
@safe_route
def review_package(rid):
    """Package review screen — guided form-by-form verification before sending."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")

    from src.core.dal import get_latest_manifest, get_buyer_preferences, get_lifecycle_events
    manifest = get_latest_manifest(rid)
    if not manifest:
        # Try to create manifest from existing output_files
        output_files = r.get("output_files", [])
        if output_files:
            try:
                from src.core.dal import create_package_manifest
                from src.core.agency_config import match_agency
                _ak, _ac = match_agency(r)
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
                    elif "std204" in _of_lower or "payee" in _of_lower: _fid = "std204"
                    elif "std1000" in _of_lower: _fid = "std1000"
                    elif "seller" in _of_lower or "permit" in _of_lower: _fid = "sellers_permit"
                    _gen_forms.append({"form_id": _fid, "filename": _of})

                _mid = create_package_manifest(
                    rfq_id=rid, agency_key=_ak, agency_name=_ac.get("name", ""),
                    required_forms=_ac.get("required_forms", []),
                    generated_forms=_gen_forms,
                    quote_number=r.get("reytech_quote_number", ""),
                    quote_total=sum(float(i.get("price_per_unit",0))*int(float(i.get("qty",1))) for i in r.get("line_items", r.get("items", [])) if i.get("price_per_unit")),
                    item_count=len(r.get("line_items", r.get("items", []))),
                    created_by="recovery"
                )
                if _mid:
                    manifest = get_latest_manifest(rid)
                    log.info("Created recovery manifest %s for RFQ %s from %d output_files", _mid, rid, len(output_files))
            except Exception as _rm_e:
                log.error("Recovery manifest failed: %s", _rm_e)

    if not manifest:
        flash("No package generated yet — generate first, then review.", "error")
        return redirect(f"/rfq/{rid}")

    buyer_email = r.get("requestor_email", "")
    buyer_prefs = get_buyer_preferences(buyer_email) if buyer_email else []
    timeline = get_lifecycle_events("rfq", rid, limit=20)
    sol = r.get("solicitation_number", "") or r.get("rfq_number", "") or "RFQ"

    # Get previous manifest for version diff
    prev_manifest = None
    if manifest and manifest.get("version", 1) > 1:
        try:
            from src.core.db import get_db
            with get_db() as conn:
                prev_row = conn.execute(
                    "SELECT id FROM package_manifest WHERE rfq_id = ? AND version = ?",
                    (rid, manifest["version"] - 1)).fetchone()
                if prev_row:
                    prev_manifest = get_package_manifest(prev_row[0])
        except Exception as _e:
            log.debug('suppressed in review_package: %s', _e)

    # Determine which forms are hidden (inside bid package, not standalone)
    _has_bidpkg = any(
        f.get("form_id") == "bidpkg" or "bidpackage" in (f.get("filename") or "").lower()
        for f in (manifest.get("generated_forms") or [])
    )
    _bidpkg_internal = set()
    if _has_bidpkg:
        try:
            from src.forms.form_qa import BID_PACKAGE_INTERNAL_FORMS
            _bidpkg_internal = BID_PACKAGE_INTERNAL_FORMS
        except ImportError:
            _bidpkg_internal = {"dvbe843", "sellers_permit", "calrecycle74", "darfur_act",
                                "bidder_decl", "std21", "genai_708"}

    _cm_enabled = False
    try:
        from src.core.feature_flags import get_flag
        _cm_enabled = get_flag("compliance_matrix", default=False)
    except Exception as _e:
        log.debug('suppressed in review_package: %s', _e)

    return render_page("rfq_review.html",
        r=r, rid=rid, sol=sol,
        manifest=manifest,
        prev_manifest=prev_manifest,
        buyer_prefs=buyer_prefs,
        timeline=timeline,
        bidpkg_internal=_bidpkg_internal,
        compliance_matrix_enabled=_cm_enabled,
        active_page="Home")


@bp.route("/rfq/<rid>/support")
@auth_required
@safe_route
def rfq_support_view(rid):
    """Support timeline — full RFQ lifecycle for customer support."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    from src.core.dal import get_lifecycle_events, get_latest_manifest, get_buyer_preferences
    timeline = get_lifecycle_events("rfq", rid, limit=200)
    manifest = get_latest_manifest(rid)
    buyer_email = r.get("requestor_email", "")
    buyer_prefs = get_buyer_preferences(buyer_email) if buyer_email else []

    all_manifests = []
    deliveries = []
    emails = []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, version, created_at, overall_status, total_forms, quote_number, quote_total, package_filename "
                "FROM package_manifest WHERE rfq_id = ? ORDER BY version DESC", (rid,)).fetchall()
            all_manifests = [dict(row) for row in rows]
            rows = conn.execute(
                "SELECT * FROM package_delivery WHERE rfq_id = ? ORDER BY delivered_at DESC", (rid,)).fetchall()
            deliveries = [dict(row) for row in rows]
    except Exception as _e:
        log.debug('suppressed in rfq_support_view: %s', _e)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, logged_at, direction, sender, recipient, subject, status "
                "FROM email_log WHERE rfq_id = ? ORDER BY logged_at DESC LIMIT 20", (rid,)).fetchall()
            emails = [dict(row) for row in rows]
    except Exception as _e:
        log.debug('suppressed in rfq_support_view: %s', _e)

    sol = r.get("solicitation_number", "") or r.get("rfq_number", "") or "RFQ"
    return render_page("rfq_support.html", r=r, rid=rid, sol=sol,
        timeline=timeline, manifest=manifest, all_manifests=all_manifests,
        deliveries=deliveries, emails=emails, buyer_prefs=buyer_prefs, active_page="Home")


@bp.route("/api/rfq/<rid>/add-buyer-pref", methods=["POST"])
@auth_required
@safe_route
def api_add_buyer_pref(rid):
    """Add a buyer preference from the support view."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    buyer_email = r.get("requestor_email", "")
    if not buyer_email:
        return jsonify({"ok": False, "error": "No buyer email on this RFQ"})
    data = request.get_json(force=True, silent=True) or {}
    key = data.get("preference_key", "")
    value = data.get("preference_value", "")
    notes = data.get("notes", "")
    if not key or not (value or notes):
        return jsonify({"ok": False, "error": "preference_key and notes required"})
    from src.core.dal import save_buyer_preference, log_lifecycle_event
    ok = save_buyer_preference(buyer_email, key, value or notes[:200],
        buyer_name=r.get("requestor_name", ""), agency_key=r.get("agency", ""),
        source=data.get("source", "manual"), notes=notes)
    if ok:
        log_lifecycle_event("rfq", rid, "buyer_preference_added",
            f"Preference added: {key} for {buyer_email}", actor="user",
            detail={"key": key, "notes": notes[:200]})
    return jsonify({"ok": ok})


@bp.route("/api/rfq/<rid>/lookup-tax-rate", methods=["POST"])
@auth_required
@safe_route
def api_lookup_tax_rate(rid):
    """Look up CA sales tax rate from delivery address via CDTFA API."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    data = request.get_json(force=True, silent=True) or {}
    address = data.get("address") or r.get("delivery_location") or r.get("ship_to") or ""
    if not address or len(address.strip()) < 5:
        return jsonify({"ok": False, "error": "No delivery address to look up"})
    try:
        from src.agents.tax_agent import get_tax_rate
        import re as _re_tax
        # ZIP: use LAST 5-digit sequence — avoids matching street numbers like "11500"
        _zip_matches = _re_tax.findall(r'\b(\d{5})\b', address)
        _d_zip = _zip_matches[-1] if _zip_matches else ""
        # City: handle "City, CA", "City, Ca.", and "City Ca. 90049" formats
        _city_match = (_re_tax.search(r',\s*([A-Za-z\s]+),?\s*[A-Z][A-Za-z]\.?\s*\d{5}', address) or
                       _re_tax.search(r',\s*([A-Za-z][A-Za-z\s]+?)\s*,\s*[A-Z]{2}', address))
        _d_city = _city_match.group(1).strip() if _city_match else ""
        # Street: everything before first comma
        _street_match = _re_tax.search(r'^(\d+\s+[^,\n]+?)(?:,|\s{2,}[A-Z][a-z]|$)', address)
        _d_street = _street_match.group(1).strip() if _street_match else ""
        # Last resort: extract city anchored to zip
        if not _d_city and _d_zip:
            _city_from_zip = _re_tax.search(
                r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,?\s*[Cc][Aa]\.?\s*' + _d_zip, address)
            if _city_from_zip:
                _d_city = _city_from_zip.group(1).strip()
        log.info("Tax lookup: raw='%s' → street='%s' city='%s' zip='%s'",
                 address[:60], _d_street, _d_city, _d_zip)
        if _d_street and _d_city and _d_zip:
            result = get_tax_rate(street=_d_street, city=_d_city, zip_code=_d_zip)
        else:
            from src.agents.tax_agent import parse_ship_to
            _parts = [p.strip() for p in address.split(",")]
            parsed = parse_ship_to("", _parts)
            result = get_tax_rate(
                street=parsed.get("street", ""),
                city=parsed.get("city", ""),
                zip_code=parsed.get("zip", "")
            )
        if result and result.get("rate"):
            rate_pct = round(result["rate"] * 100, 3)
            r["tax_rate"] = rate_pct
            r["tax_validated"] = True
            r["tax_source"] = result.get("source", "cdtfa_api")
            r["tax_jurisdiction"] = result.get("jurisdiction", "")
            from src.api.dashboard import _save_single_rfq
            _save_single_rfq(rid, r)
            return jsonify({"ok": True, "rate": rate_pct,
                "jurisdiction": result.get("jurisdiction", ""),
                "city": result.get("city", ""),
                "county": result.get("county", ""),
                "confidence": result.get("confidence", ""),
                "source": result.get("source", "")})
        else:
            return jsonify({"ok": False, "error": result.get("error", "Lookup failed")})
    except Exception as e:
        log.error("Tax rate lookup for RFQ %s: %s", rid, e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/rfq/<rid>/resend-package", methods=["POST"])
@auth_required
@safe_route
def api_resend_package(rid):
    """Resend the latest approved package to a recipient."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    data = request.get_json(force=True, silent=True) or {}
    to_email = data.get("to", "")
    subject = data.get("subject", "")
    body_text = data.get("body", "")
    if not to_email:
        return jsonify({"ok": False, "error": "Recipient email required"})
    sol = r.get("solicitation_number", "") or "RFQ"

    from src.core.dal import get_latest_manifest, log_lifecycle_event, record_package_delivery
    manifest = get_latest_manifest(rid)
    if not manifest:
        return jsonify({"ok": False, "error": "No package generated"})
    pkg_filename = manifest.get("package_filename") or f"RFQ_Package_{sol}_ReytechInc.pdf"

    # Find package data (disk or DB)
    pkg_data = None
    pkg_path = os.path.join(OUTPUT_DIR, sol, pkg_filename)
    if os.path.exists(pkg_path):
        with open(pkg_path, "rb") as _f:
            pkg_data = _f.read()
    else:
        try:
            files = list_rfq_files(rid, category="generated")
            for dbf in files:
                if "Package" in (dbf.get("filename") or "") or dbf.get("filename") == pkg_filename:
                    full = get_rfq_file(dbf["id"])
                    if full and full.get("data"):
                        pkg_data = full["data"]; break
        except Exception as _e:
            log.debug('suppressed in api_resend_package: %s', _e)
    if not pkg_data:
        return jsonify({"ok": False, "error": f"Package file not found: {pkg_filename}"})

    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.application import MIMEApplication
        smtp_user = os.environ.get("GMAIL_ADDRESS", "")
        smtp_pass = os.environ.get("GMAIL_PASSWORD", "")
        if not smtp_user or not smtp_pass:
            return jsonify({"ok": False, "error": "Email not configured"})
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = to_email
        msg["Subject"] = subject or f"Reytech Inc. — RFQ Response #{sol}"
        msg.attach(MIMEText(body_text or "Please find attached our RFQ response package.", "plain"))
        att = MIMEApplication(pkg_data, _subtype="pdf")
        att.add_header("Content-Disposition", "attachment", filename=pkg_filename)
        msg.attach(att)
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [to_email], msg.as_string())
        server.quit()

        log_lifecycle_event("rfq", rid, "package_sent",
            f"Resent to {to_email} (support view)", actor="user",
            detail={"recipient": to_email, "subject": subject, "resend": True})
        if manifest.get("id"):
            import hashlib
            record_package_delivery(manifest["id"], rid, to_email,
                recipient_name=r.get("requestor_name", ""), email_subject=subject,
                package_hash=hashlib.sha256(pkg_data).hexdigest())
        return jsonify({"ok": True, "sent_to": to_email, "size": len(pkg_data)})
    except Exception as e:
        log.error("Resend RFQ %s: %s", rid, e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/support/search-packages")
@auth_required
@safe_route
def api_search_packages():
    """Search package manifests by form_id, agency, status, solicitation, or buyer."""
    form_id = request.args.get("form_id", "")
    agency = request.args.get("agency", "")
    status = request.args.get("status", "")
    sol = request.args.get("sol", "")
    buyer = request.args.get("buyer", "")
    limit = min(int(request.args.get("limit", 50)), 200)
    try:
        from src.core.db import get_db
        import json as _jsearch
        with get_db() as conn:
            q = "SELECT pm.* FROM package_manifest pm WHERE 1=1"
            p = []
            if form_id:
                q += " AND pm.generated_forms LIKE ?"; p.append(f"%{form_id}%")
            if agency:
                q += " AND pm.agency_key LIKE ?"; p.append(f"%{agency}%")
            if status:
                q += " AND pm.overall_status = ?"; p.append(status)
            if sol:
                q += " AND pm.rfq_id LIKE ?"; p.append(f"%{sol}%")
            q += " ORDER BY pm.created_at DESC LIMIT ?"; p.append(limit)
            rows = conn.execute(q, p).fetchall()
            results = []
            for row in rows:
                d = dict(row)
                for f in ("generated_forms", "missing_forms", "required_forms"):
                    if d.get(f):
                        try: d[f] = _jsearch.loads(d[f])
                        except (ValueError, TypeError) as e: log.debug("packet_search json field %s: %s", f, e)
                results.append(d)
            return jsonify({"ok": True, "results": results, "count": len(results)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/rfq/<rid>/save-restore", methods=["POST"])
@auth_required
@safe_route
def rfq_save_restore(rid):
    """Save template/file restorations. Called via POST from rfq_detail.html, not GET."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False})
    if r.pop("_needs_save", False):
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        return jsonify({"ok": True, "saved": True})
    return jsonify({"ok": True, "saved": False})


@bp.route("/rfq/<rid>/update", methods=["GET"])
@auth_required
@safe_route
def update_get_redirect(rid):
    """Stray GETs (browser back-button on POST form, stale bookmarks, copied
    URL from email) used to 405. Redirect them to the RFQ detail page so
    the user lands somewhere useful instead of an error."""
    return redirect(f"/rfq/{rid}", code=303)


@bp.route("/rfq/<rid>/update", methods=["POST"])
@auth_required
@safe_route
def update(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    from src.core.validation import validate_price, validate_cost, validate_markup, validate_qty, validate_text, validate_short_text, validate_url
    for i, item in enumerate(r["line_items"]):
        for field, key, vfn in [("cost", "supplier_cost", validate_cost), ("scprs", "scprs_last_price", validate_price), ("price", "price_per_unit", validate_price), ("markup", "markup_pct", validate_markup)]:
            v = request.form.get(f"{field}_{i}")
            if v:
                val, err = vfn(v)
                if err: log.warning("RFQ update item[%d] %s: %s", i, key, err)
                item[key] = val
        # Save qty and uom from separate inputs
        qty_val = request.form.get(f"qty_{i}")
        if qty_val:
            val, err = validate_qty(qty_val)
            if err: log.warning("RFQ update item[%d] qty: %s", i, err)
            item["qty"] = val
        uom_val = request.form.get(f"uom_{i}")
        if uom_val is not None:
            val, _ = validate_short_text(uom_val, max_len=20, default="EA")
            item["uom"] = val.upper()
        # Save edited description
        desc_val = request.form.get(f"desc_{i}")
        if desc_val is not None:
            val, _ = validate_text(desc_val, max_len=5000)
            item["description"] = val
        # Save part number
        part_val = request.form.get(f"part_{i}")
        if part_val is not None:
            val, _ = validate_short_text(part_val, max_len=100)
            item["item_number"] = val
        # Save item link and auto-detect supplier
        link_raw = request.form.get(f"link_{i}", "")
        link_val, _ = validate_url(link_raw)
        item["item_link"] = link_val
        if link_val:
            try:
                from src.agents.item_link_lookup import detect_supplier
                item["item_supplier"] = detect_supplier(link_val)
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    
    # Save quote-level notes
    quote_notes_val, _ = validate_text(request.form.get("quote_notes", ""), max_len=2000)
    r["quote_notes"] = quote_notes_val

    _transition_status(r, "ready", actor="user", notes="Pricing updated")
    # raise_on_error=True: user-facing pricing save. Silent persistence failure
    # on this path was the 2026-04-16 PC incident; keep RFQ symmetric.
    from src.api.dashboard import _save_single_rfq
    try:
        _save_single_rfq(rid, r, raise_on_error=True)
    except Exception as _save_e:
        log.error("RFQ update persistence failed for %s: %s", rid, _save_e)
        flash(f"Save failed — your pricing edits did NOT persist: {_save_e}", "error")
        return redirect(f"/rfq/{rid}")
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception as _e:
        log.debug('suppressed in update: %s', _e)

    # Save SCPRS prices for future lookups
    save_prices_from_rfq(r)
    
    # Record ALL prices to history + auto-ingest to catalog
    try:
        _record_rfq_prices(r, source="rfq_finalize")
    except Exception as _e:
        log.debug("Price recording: %s", _e)
    
    # Sync all priced items to product catalog
    cat_added, cat_updated = 0, 0
    try:
        from src.agents.product_catalog import match_item, add_to_catalog, add_supplier_price, init_catalog_db
        init_catalog_db()
        for item in r.get("line_items", []):
            desc = item.get("description", "")
            pn = item.get("item_number", "") or ""
            cost = item.get("supplier_cost") or 0
            bid = item.get("price_per_unit") or 0
            supplier = item.get("item_supplier", "")
            uom = item.get("uom", "EA")
            url = item.get("item_link", "")
            if not desc or (not cost and not bid):
                continue
            cat_matches = match_item(desc, pn, top_n=1)
            if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                pid = cat_matches[0]["id"]
                if cost > 0 and supplier:
                    add_supplier_price(pid, supplier, cost, url=url)
                if url:
                    try:
                        from src.agents.product_catalog import _get_conn
                        conn = _get_conn()
                        conn.execute(
                            "UPDATE product_catalog SET photo_url=COALESCE(NULLIF(photo_url,''),?) WHERE id=?",
                            (url, pid))
                        conn.commit(); conn.close()
                    except Exception as _e:
                        log.debug('suppressed in update: %s', _e)
                cat_updated += 1
            else:
                pid = add_to_catalog(
                    description=desc, part_number=pn,
                    cost=cost if cost > 0 else 0,
                    sell_price=bid if bid > 0 else 0,
                    supplier_name=supplier, uom=uom,
                    supplier_url=url,
                    source=f"rfq_finalize_{r.get('solicitation_number', '')}",
                )
                if pid and cost > 0 and supplier:
                    add_supplier_price(pid, supplier, cost, url=url)
                    cat_added += 1
        if cat_added or cat_updated:
            log.info("Finalize catalog sync: +%d new, ~%d updated", cat_added, cat_updated)
    except Exception as _ce:
        log.warning("Finalize catalog sync failed: %s", _ce)

    # Auto-learn item mappings + lock costs from user pricing
    try:
        from src.core.pricing_oracle_v2 import auto_learn_mapping, lock_cost
        for _item in r.get("line_items", []):
            _desc = _item.get("description", "")
            _cost = _item.get("supplier_cost") or _item.get("unit_price")
            if _desc and _cost:
                try:
                    _cv = float(str(_cost).replace("$", "").replace(",", ""))
                except (ValueError, TypeError):
                    _cv = 0
                if _cv > 0:
                    auto_learn_mapping(_desc, _item.get("catalog_match", {}).get("name", _desc),
                                       item_number=_item.get("item_number", ""), confidence=0.7)
                    lock_cost(_desc, _cv, supplier=_item.get("item_supplier", ""),
                              source="user_pricing", expires_days=30,
                              item_number=_item.get("item_number", ""))
    except Exception as _e:
        log.debug('suppressed in update: %s', _e)

    _log_rfq_activity(rid, "pricing_finalized",
        f"Pricing finalized for #{r.get('solicitation_number','?')} ({len(r.get('line_items',[]))} items, catalog +{cat_added}/~{cat_updated})",
        actor="user")
    
    flash("Pricing finalized — saved to catalog", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/api/rfq/<rid>/update-field", methods=["POST"])
@auth_required
@safe_route
def rfq_update_field(rid):
    """Update individual header fields (solicitation, requestor, due date, etc.)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "Not found"})
    data = request.get_json(force=True, silent=True) or {}
    changed = []
    allowed = ["solicitation_number", "requestor_name", "requestor_email",
               "due_date", "ship_to", "delivery_location", "institution",
               "agency_name", "notes"]
    from src.core.validation import validate_header_field
    for field in allowed:
        if field in data:
            old = r.get(field, "")
            val, err = validate_header_field(field, data[field])
            if err:
                log.warning("RFQ %s update-field %s: %s", rid, field, err)
            r[field] = val
            if old != data[field]:
                changed.append(f"{field}: '{old}' -> '{data[field]}'")
                # Log parse gap when user fills an empty field
                if not old and data[field]:
                    try:
                        from src.core.db import get_db
                        with get_db() as _conn:
                            _conn.execute("""
                                INSERT INTO parse_gaps
                                (rfq_id, field_name, user_filled_value,
                                 source_type, email_subject, requestor_email, agency)
                                VALUES (?,?,?,?,?,?,?)
                            """, (rid, field, data[field],
                                  r.get("source", ""),
                                  r.get("email_subject", ""),
                                  r.get("requestor_email", ""),
                                  r.get("agency", "")))
                    except Exception as _e:
                        log.debug('suppressed in rfq_update_field: %s', _e)
                    try:
                        from src.forms.template_learning import record_buyer_feedback
                        record_buyer_feedback(
                            pc_id=rid,
                            feedback_type="parse_gap",
                            detail=f"{field}={data[field][:100]}",
                        )
                    except Exception as _e:
                        log.debug('suppressed in rfq_update_field: %s', _e)
    if changed:
        from src.api.dashboard import _save_single_rfq
        try:
            _save_single_rfq(rid, r, raise_on_error=True)
        except Exception as _save_e:
            log.error("RFQ update-field persistence failed for %s: %s", rid, _save_e)
            return jsonify({
                "ok": False,
                "error": f"Could not save field changes: {_save_e}",
                "attempted": changed,
            }), 500
        _log_rfq_activity(rid, "field_updated",
            "; ".join(changed), actor="user")

    # Re-attempt PC linking on any field update if not already linked
    link_result = None
    if not r.get("linked_pc_id") and any(f in data for f in ["solicitation_number", "requestor_email", "requestor_name"]):
        try:
            from src.api.dashboard import _link_rfq_to_pc
            _link_trace = []
            if _link_rfq_to_pc(r, _link_trace):
                from src.api.dashboard import _save_single_rfq
                _save_single_rfq(rid, r)
                link_result = {"linked": True, "trace": _link_trace,
                               "pc_id": r.get("linked_pc_id", ""),
                               "pc_number": r.get("linked_pc_number", "")}
                log.info("Re-linked RFQ %s: %s", rid, _link_trace)
            else:
                link_result = {"linked": False, "trace": _link_trace}
        except Exception as _le:
            link_result = {"linked": False, "error": str(_le)}
            log.warning("Re-link: %s", _le)

    # Smart validation against buyer history
    suggestions = {}
    if "delivery_location" in data or "ship_to" in data or "institution" in data:
        try:
            from src.core.db import get_db
            with get_db() as _vconn:
                email = r.get("requestor_email", "")
                if email:
                    history = _vconn.execute("""
                        SELECT ship_to_address, dept_name, COUNT(*) as cnt
                        FROM scprs_po_master
                        WHERE buyer_email = ?
                        AND ship_to_address != ''
                        GROUP BY ship_to_address
                        ORDER BY cnt DESC LIMIT 5
                    """, (email,)).fetchall()

                    if history:
                        new_val = data.get("delivery_location", data.get("ship_to", data.get("institution", "")))
                        top_location = history[0][0]
                        top_count = history[0][2]
                        total = sum(h[2] for h in history)

                        from difflib import SequenceMatcher
                        best_match = max(
                            [(h[0], h[2], SequenceMatcher(None, new_val.lower(), h[0].lower()).ratio())
                             for h in history],
                            key=lambda x: x[2]
                        )

                        suggestions["buyer_history"] = {
                            "most_common": top_location,
                            "frequency": f"{top_count}/{total} POs",
                            "confidence": round(top_count / total * 100),
                            "all_locations": [
                                {"location": h[0], "department": h[1], "count": h[2]}
                                for h in history
                            ],
                        }

                        if best_match[2] < 0.5 and top_count >= 3:
                            suggestions["warning"] = (
                                f"This buyer usually ships to '{top_location}' "
                                f"({top_count} of {total} POs). "
                                f"You entered '{new_val}'. Confirm?"
                            )
                            suggestions["needs_confirm"] = True
        except Exception as _e:
            log.debug('suppressed in rfq_update_field: %s', _e)

    return jsonify({"ok": True, "updated": changed, "suggestions": suggestions,
                    "link_result": link_result})


@bp.route("/api/rfq/<rid>/lookup-tax-rate", methods=["POST"])
@auth_required
@safe_route
def api_rfq_lookup_tax_rate(rid):
    """Look up CA sales tax rate from delivery address for an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    data = request.get_json(force=True, silent=True) or {}
    address = data.get("address") or r.get("delivery_location") or r.get("ship_to") or ""
    if not address:
        return jsonify({"ok": False, "error": "No address — enter Delivery Location first"})
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
        _force = bool(data.get("force_live"))
        result = get_tax_rate(city=_d_city, zip_code=_d_zip, force_live=_force)
        if result and result.get("rate"):
            rate_pct = round(result["rate"] * 100, 3)
            r["tax_rate"] = rate_pct
            r["tax_validated"] = True
            r["tax_source"] = result.get("source", "")
            from src.api.dashboard import _save_single_rfq
            _save_single_rfq(rid, r)
            return jsonify({"ok": True, "rate": rate_pct,
                "jurisdiction": result.get("jurisdiction", ""),
                "source": result.get("source", "")})
        return jsonify({"ok": False, "error": "Tax lookup returned no rate"})
    except Exception as e:
        log.error("RFQ tax lookup %s: %s", rid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/rfq/<rid>/auto-price", methods=["POST"])
@auth_required
@safe_route
def api_rfq_auto_price(rid):
    """Auto-price all items: catalog match → scrape catalog URLs → Amazon fallback."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404
    items = r.get("line_items", [])
    if not items:
        return jsonify({"ok": False, "error": "No line items"})

    results = []
    priced = 0
    default_markup = r.get("default_markup") or 25

    # Step 1: Catalog batch match — get URLs and pricing from previous quotes
    catalog_urls = {}  # idx → {url, cost, supplier, mfg, confidence}
    try:
        from src.agents.product_catalog import match_items_batch, init_catalog_db
        init_catalog_db()
        batch_input = [
            {"idx": i, "description": it.get("description", ""),
             "part_number": it.get("item_number", "") or it.get("mfg_number", "")}
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
        log.warning("Auto-price catalog match: %s", e)

    # Step 2: For each item — try catalog URL scrape, then Amazon search
    for i, item in enumerate(items):
        desc = item.get("description", "")
        if not desc or len(desc) < 5:
            results.append({"line": i + 1, "status": "skipped", "note": "No description"})
            continue

        # Skip items that already have a cost set by the user
        existing_cost = item.get("supplier_cost") or item.get("vendor_cost") or 0
        try:
            existing_cost = float(existing_cost)
        except (ValueError, TypeError):
            existing_cost = 0

        cat = catalog_urls.get(i)
        price = 0
        source = ""
        supplier = ""
        mfg = ""
        url = ""

        # 2a: If catalog match has a URL, scrape it for FRESH pricing
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
                log.debug("Auto-price catalog URL scrape line %d: %s", i + 1, e)

        # 2b: If catalog has cost but URL scrape failed, use catalog cost
        if price <= 0 and cat and cat.get("cost") and float(cat.get("cost", 0)) > 0:
            price = float(cat["cost"])
            source = "catalog"
            supplier = cat.get("supplier", "")
            mfg = cat.get("mfg", "")
            url = cat.get("url", "")

        # 2c: Amazon search fallback
        if price <= 0:
            try:
                from src.agents.product_research import search_amazon
                _q = desc[:120]
                amz = search_amazon(_q, max_results=1)
                if amz and amz[0].get("price", 0) > 0:
                    price = float(amz[0]["price"])
                    source = "amazon"
                    supplier = "Amazon"
                    url = amz[0].get("url", "")
                    mfg = amz[0].get("mfg_number", "") or amz[0].get("item_number", "")
                    log.info("Auto-price Amazon line %d: %s → $%.2f", i + 1, _q[:40], price)
            except Exception as e:
                log.debug("Auto-price Amazon search line %d: %s", i + 1, e)

        # 2d: Apply results to item
        if price > 0:
            item["supplier_cost"] = price
            item["cost_source"] = source
            if url:
                item["item_link"] = url
            if supplier:
                item["item_supplier"] = supplier
                item["cost_supplier_name"] = supplier
            if mfg:
                item["item_number"] = mfg
            markup = item.get("markup_pct") or default_markup
            try:
                markup = float(markup)
            except (ValueError, TypeError):
                markup = 25
            item["markup_pct"] = markup
            item["price_per_unit"] = round(price * (1 + markup / 100), 2)
            priced += 1
            results.append({
                "line": i + 1, "status": "ok", "source": source,
                "price": price, "supplier": supplier, "mfg": mfg,
                "url": url[:60] if url else "",
                "catalog_confidence": cat.get("confidence", 0) if cat else 0,
            })
        elif cat:
            # Catalog matched but no price found — still link the item
            if cat.get("url") and not item.get("item_link"):
                item["item_link"] = cat["url"]
            if cat.get("supplier") and not item.get("item_supplier"):
                item["item_supplier"] = cat["supplier"]
            if cat.get("mfg") and not item.get("item_number"):
                item["item_number"] = cat["mfg"]
            results.append({
                "line": i + 1, "status": "linked", "source": "catalog",
                "note": "Catalog matched, no live price",
                "catalog_confidence": cat.get("confidence", 0),
            })
        else:
            results.append({"line": i + 1, "status": "no_match", "note": "No catalog match or Amazon result"})

        # Rate limiting for external calls
        if source in ("catalog_url", "amazon"):
            import time
            time.sleep(0.5)

    # Save and sync to catalog
    if priced > 0:
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        try:
            from src.agents.product_catalog import save_pc_items_to_catalog
            save_pc_items_to_catalog({"items": items})
        except Exception as _e:
            log.debug('suppressed in api_rfq_auto_price: %s', _e)

    return jsonify({"ok": True, "results": results, "priced": priced, "total": len(items),
                    "catalog_matched": len(catalog_urls)})


@bp.route("/api/rfq/<rid>/bulk-scrape-urls", methods=["POST"])
@auth_required
@safe_route
def api_rfq_bulk_scrape_urls(rid):
    """Bulk paste URLs → scrape each → apply cost + supplier to items by index."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404
    data = request.get_json(force=True, silent=True) or {}
    urls = data.get("urls", [])
    if not urls:
        return jsonify({"ok": False, "error": "No URLs provided"})
    items = r.get("line_items", [])
    results = []
    applied = 0
    for i, raw_line in enumerate(urls):
        raw_line = (raw_line or "").strip()
        # Strip numbered prefixes like "1. " or "19. "
        raw_line = _re_mod.sub(r'^\d+[\.\)]\s*', '', raw_line)
        if not raw_line:
            results.append({"line": i + 1, "url": "", "status": "skipped"})
            continue
        if i >= len(items):
            results.append({"line": i + 1, "url": raw_line[:60], "status": "skipped"})
            continue

        # ── Smart extraction: pull URL from mixed-content lines ──
        # Handles LLM output like: "1,24,SKU 343586,Each,Description,https://..."
        _url_match = _re_mod.search(r'(https?://\S+)', raw_line)
        if _url_match:
            url = _url_match.group(1).rstrip('.,;)')
            _pre = raw_line[:_url_match.start()].strip().rstrip(',')
        else:
            url = raw_line
            _pre = ""

        # Parse CSV fields from prefix if present
        _parsed_desc = ""
        _parsed_mfg = ""
        _parsed_qty = 0
        if _pre and ',' in _pre:
            _parts = [p.strip() for p in _pre.split(',')]
            for _part in _parts:
                if not _part:
                    continue
                if _re_mod.match(r'^(SKU|MFG|PN|Item)\s*#?\s*\d', _part, _re_mod.IGNORECASE):
                    _parsed_mfg = _re_mod.sub(r'^(SKU|MFG|PN|Item)\s*#?\s*', '', _part, flags=_re_mod.IGNORECASE).strip()
                elif _re_mod.match(r'^\d{1,4}$', _part) and not _parsed_qty:
                    _v = int(_part)
                    if 0 < _v < 10000:
                        _parsed_qty = _v
                elif len(_part) > 10 and not _re_mod.match(r'^(Each|EA|CS|BX|PK|Case|Box|Pack|DZ|CT)$', _part, _re_mod.IGNORECASE):
                    _parsed_desc = _part
        elif _pre and len(_pre) > 5:
            _parsed_desc = _pre

        try:
            from src.agents.item_link_lookup import lookup_from_url, detect_supplier
            res = lookup_from_url(url)
            item = items[i]
            # Always apply URL, supplier, MFG#, description — even without price
            item["item_link"] = res.get("url", url)
            item["item_supplier"] = detect_supplier(url)
            _pn = res.get("mfg_number") or res.get("part_number") or _parsed_mfg or ""
            if _pn:
                item["item_number"] = _pn
            _desc = res.get("title") or res.get("description") or _parsed_desc or ""
            if _desc:
                _desc = _re_mod.sub(r'\s*https?://\S+', '', _desc).strip()
            if _desc and (not item.get("description") or len(item.get("description", "")) < 10):
                item["description"] = _desc
            # Persist photo_url and manufacturer from scrape
            if res.get("photo_url"):
                item["photo_url"] = res["photo_url"]
            if res.get("manufacturer"):
                item["manufacturer"] = res["manufacturer"]
            if _parsed_qty > 0 and (not item.get("qty") or item.get("qty") == 1):
                item["qty"] = _parsed_qty
            # Apply pricing if found
            price = res.get("price") or res.get("list_price") or res.get("cost")
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
                _search_q = _desc or item.get("description", "")
                if _search_q and len(_search_q) >= 8:
                    try:
                        from src.agents.product_research import search_amazon
                        amz = search_amazon(_search_q, max_results=1)
                        if amz and amz[0].get("price", 0) > 0:
                            price = float(amz[0]["price"])
                            amazon_fallback = True
                            _amz_asin = amz[0].get("asin", "")
                            if _amz_asin:
                                # Switch entire item to Amazon source
                                item["item_link"] = amz[0].get("url", "") or f"https://www.amazon.com/dp/{_amz_asin}"
                                item["item_supplier"] = "Amazon"
                                url = item["item_link"]
                            log.info("Amazon fallback for RFQ line %d: %s → $%.2f", i + 1, _search_q[:40], price)
                    except Exception as e:
                        log.debug("Amazon fallback error line %d: %s", i + 1, e)
            if price > 0:
                item["supplier_cost"] = price
                item["cost_source"] = "Amazon" if amazon_fallback else "Supplier URL"
                item["cost_supplier_name"] = item.get("item_supplier", "")
                markup = item.get("markup_pct") or r.get("default_markup") or 25
                try:
                    markup = float(markup)
                except (ValueError, TypeError):
                    markup = 25
                item["markup_pct"] = markup
                item["price_per_unit"] = round(price * (1 + markup / 100), 2)
                _status = "ok" if not amazon_fallback else "ok_amazon"
                results.append({"line": i + 1, "url": url[:60], "status": _status,
                               "price": price, "supplier": item["item_supplier"],
                               "note": "Price from Amazon" if amazon_fallback else ""})
            else:
                results.append({"line": i + 1, "url": url[:60], "status": "linked",
                               "supplier": item.get("item_supplier", ""), "note": "URL linked, no price found"})
            applied += 1
        except Exception as e:
            log.error("Bulk scrape URL error line %d: %s", i + 1, e, exc_info=True)
            results.append({"line": i + 1, "url": url[:60], "status": "error", "error": str(e)[:80]})
    if applied > 0:
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        # Auto-confirm scraped items to catalog
        try:
            from src.agents.product_catalog import save_pc_items_to_catalog
            cat_result = save_pc_items_to_catalog({"items": r.get("line_items", [])})
            log.info("RFQ bulk-scrape catalog sync: added=%d existing=%d",
                     cat_result.get("added", 0), cat_result.get("existing", 0))
        except Exception as e:
            log.error("RFQ bulk-scrape catalog sync error: %s", e, exc_info=True)
    return jsonify({"ok": True, "results": results, "applied": applied, "total": len(urls)})


@bp.route("/api/rfq/<rid>/bulk-paste-data", methods=["POST"])
@auth_required
@safe_route
def api_rfq_bulk_paste_data(rid):
    """Bulk paste multi-column data (description, MFG#, URL, cost, markup) into line items."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404
    data = request.get_json(force=True, silent=True) or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"ok": False, "error": "No data provided"})
    items = r.get("line_items", [])
    results = []
    applied = 0
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            results.append({"line": i + 1, "status": "skipped"})
            continue
        if i >= len(items):
            results.append({"line": i + 1, "status": "skipped"})
            continue
        # Check if row has any non-empty values
        has_data = any((row.get(k) or "").strip() for k in
                       ("description", "item_number", "item_link", "supplier_cost", "markup_pct"))
        if not has_data:
            results.append({"line": i + 1, "status": "skipped"})
            continue
        try:
            item = items[i]
            fields_set = 0
            # Description
            desc = (row.get("description") or "").strip()
            if desc:
                item["description"] = desc
                fields_set += 1
            # MFG# / Item Number
            mfg = (row.get("item_number") or "").strip()
            if mfg:
                item["item_number"] = mfg
                fields_set += 1
            # Item Link / URL
            link = (row.get("item_link") or "").strip()
            if link:
                if not link.startswith("http") and ("." in link):
                    link = "https://" + link
                item["item_link"] = link
                try:
                    from src.agents.item_link_lookup import detect_supplier
                    item["item_supplier"] = detect_supplier(link)
                except Exception as _e:
                    log.debug('suppressed in api_rfq_bulk_paste_data: %s', _e)
                fields_set += 1
            # Cost
            cost_str = (row.get("supplier_cost") or "").strip().replace("$", "").replace(",", "")
            if cost_str:
                try:
                    cost = float(cost_str)
                    if cost > 0:
                        item["supplier_cost"] = cost
                        item["cost_source"] = "Bulk Paste"
                        fields_set += 1
                        # Recalculate bid price with markup
                        markup_str = (row.get("markup_pct") or "").strip().replace("%", "")
                        if markup_str:
                            try:
                                markup = float(markup_str)
                                item["markup_pct"] = markup
                            except (ValueError, TypeError):
                                markup = item.get("markup_pct") or r.get("default_markup") or 25
                        else:
                            markup = item.get("markup_pct") or r.get("default_markup") or 25
                        try:
                            markup = float(markup)
                        except (ValueError, TypeError):
                            markup = 25
                        item["markup_pct"] = markup
                        item["price_per_unit"] = round(cost * (1 + markup / 100), 2)
                except (ValueError, TypeError) as _e:
                    log.debug("suppressed: %s", _e)
            elif (row.get("markup_pct") or "").strip():
                # Markup without cost — update markup only if item already has cost
                markup_str = (row.get("markup_pct") or "").strip().replace("%", "")
                try:
                    markup = float(markup_str)
                    item["markup_pct"] = markup
                    if item.get("supplier_cost") and float(item["supplier_cost"]) > 0:
                        item["price_per_unit"] = round(float(item["supplier_cost"]) * (1 + markup / 100), 2)
                    fields_set += 1
                except (ValueError, TypeError) as _e:
                    log.debug("suppressed: %s", _e)
            if fields_set > 0:
                res_obj = {"line": i + 1, "status": "ok", "fields": fields_set}
                if item.get("supplier_cost"):
                    res_obj["price"] = item["supplier_cost"]
                if item.get("item_supplier"):
                    res_obj["supplier"] = item["item_supplier"]
                results.append(res_obj)
                applied += 1
            else:
                results.append({"line": i + 1, "status": "skipped"})
        except Exception as e:
            log.error("Bulk paste data error line %d: %s", i + 1, e, exc_info=True)
            results.append({"line": i + 1, "status": "error", "error": str(e)[:80]})
    if applied > 0:
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
    return jsonify({"ok": True, "results": results, "applied": applied, "total": len(rows)})


