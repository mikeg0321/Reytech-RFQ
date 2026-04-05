# routes_growth_intel.py — Catalog Growth, Price Alerts, Win/Loss, Expansion Outreach
# Features #8, #10, #11, #13 from enhancement roadmap
#
# NOTE: This module is exec'd in dashboard.py globals — bp, load_rfqs, save_rfqs,
# _load_price_checks, _save_price_checks, render_page, auth_required, CONFIG,
# DATA_DIR, jsonify, request, redirect, flash, datetime, os, json, log are all available.

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect, flash
from src.core.paths import DATA_DIR
from src.core.db import get_db
from src.api.render import render_page

import sqlite3 as _sqlite3
import re as _re
from collections import defaultdict as _defaultdict
from datetime import timedelta as _timedelta

# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE #8: CATALOG AUTO-GROWTH
# Rebuild catalog from all historical data (quotes, PCs, RFQs, orders)
# ═══════════════════════════════════════════════════════════════════════════════

def _catalog_rebuild_from_history():
    """Process ALL historical quotes + PCs + orders into the product catalog.
    
    This is the 'catch-up' function — makes the catalog learn from everything
    that happened before the learning hooks were working.
    """
    try:
        from src.agents.product_catalog import (
            match_item, add_to_catalog, add_supplier_price,
            record_catalog_quote, init_catalog_db, update_product_pricing,
        )
        init_catalog_db()
    except Exception as e:
        log.error("Catalog rebuild: import failed: %s", e)
        return {"ok": False, "error": str(e)}

    stats = {"quotes_processed": 0, "items_added": 0, "items_updated": 0,
             "pcs_processed": 0, "orders_processed": 0, "errors": 0}

    # ── 1. Process all quotes ──
    try:
        quotes_path = os.path.join(DATA_DIR, "quotes_log.json")
        if os.path.exists(quotes_path):
            with open(quotes_path) as f:
                quotes = json.load(f)
            for qt in quotes:
                qn = qt.get("quote_number", "")
                agency = qt.get("agency", "")
                institution = qt.get("institution", "")
                for it in qt.get("items_detail", []):
                    try:
                        _catalog_learn_item(
                            desc=it.get("description", ""),
                            part_number=it.get("part_number", ""),
                            sell_price=it.get("unit_price", 0),
                            cost=it.get("cost", 0),
                            supplier=it.get("supplier", ""),
                            supplier_url=it.get("supplier_url", ""),
                            asin=it.get("asin", ""),
                            agency=agency,
                            institution=institution,
                            source=f"quote_{qn}",
                            qty=it.get("qty", 1),
                            # Pass the catalog functions
                            _match=match_item, _add=add_to_catalog,
                            _sup=add_supplier_price, _record=record_catalog_quote,
                            _update=update_product_pricing,
                            stats=stats,
                        )
                    except Exception as e:
                        stats["errors"] += 1
                        log.debug("Catalog rebuild quote item error: %s", e)
                stats["quotes_processed"] += 1
    except Exception as e:
        log.error("Catalog rebuild quotes error: %s", e)

    # ── 2. Process all price checks ──
    try:
        pcs = _load_price_checks()
        for pc in pcs:
            pcid = pc.get("id", "")
            agency = pc.get("agency", "") or pc.get("institution", "")
            institution = pc.get("institution", "")
            for it in pc.get("items", []):
                if it.get("no_bid"):
                    continue
                try:
                    _catalog_learn_item(
                        desc=it.get("description", ""),
                        part_number=it.get("mfg_number", "") or it.get("item_number", ""),
                        sell_price=it.get("unit_price", 0) or it.get("pricing", {}).get("recommended_price", 0),
                        cost=it.get("vendor_cost", 0) or it.get("pricing", {}).get("unit_cost", 0),
                        supplier=it.get("item_supplier", ""),
                        supplier_url=it.get("item_link", ""),
                        asin="",
                        agency=agency,
                        institution=institution,
                        source=f"pc_{pcid}",
                        qty=it.get("qty", 1),
                        _match=match_item, _add=add_to_catalog,
                        _sup=add_supplier_price, _record=record_catalog_quote,
                        _update=update_product_pricing,
                        stats=stats,
                    )
                except Exception as e:
                    stats["errors"] += 1
            stats["pcs_processed"] += 1
    except Exception as e:
        log.error("Catalog rebuild PCs error: %s", e)

    # ── 3. Process all orders ──
    try:
        orders = _load_orders()
        if orders:
            order_list = orders.values() if isinstance(orders, dict) else orders
            for order in order_list:
                agency = order.get("agency", "")
                institution = order.get("institution", "")
                for it in order.get("line_items", []):
                    try:
                        _catalog_learn_item(
                            desc=it.get("description", ""),
                            part_number=it.get("part_number", ""),
                            sell_price=it.get("unit_price", 0),
                            cost=it.get("cost", 0),
                            supplier=it.get("supplier", ""),
                            supplier_url=it.get("supplier_url", ""),
                            asin=it.get("asin", ""),
                            agency=agency,
                            institution=institution,
                            source=f"order_{order.get('order_id', '')}",
                            qty=it.get("qty", 1),
                            _match=match_item, _add=add_to_catalog,
                            _sup=add_supplier_price, _record=record_catalog_quote,
                            _update=update_product_pricing,
                            stats=stats,
                        )
                    except Exception as e:
                        stats["errors"] += 1
                stats["orders_processed"] += 1
    except Exception as e:
        log.error("Catalog rebuild orders error: %s", e)

    # ── 4. Process RFQ line items ──
    try:
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            agency = r.get("agency_name", "") or r.get("agency", "")
            for it in r.get("line_items", []):
                try:
                    _catalog_learn_item(
                        desc=it.get("description", ""),
                        part_number=it.get("item_number", ""),
                        sell_price=it.get("price_per_unit", 0),
                        cost=it.get("supplier_cost", 0),
                        supplier=it.get("item_supplier", ""),
                        supplier_url=it.get("item_link", "") or it.get("supplier_url", ""),
                        asin=it.get("asin", ""),
                        agency=agency,
                        institution="",
                        source=f"rfq_{rid}",
                        qty=it.get("qty", 1),
                        _match=match_item, _add=add_to_catalog,
                        _sup=add_supplier_price, _record=record_catalog_quote,
                        _update=update_product_pricing,
                        stats=stats,
                    )
                except Exception as e:
                    stats["errors"] += 1
    except Exception as e:
        log.error("Catalog rebuild RFQs error: %s", e)

    log.info("Catalog rebuild complete: %s", stats)
    return {"ok": True, **stats}


def _catalog_learn_item(desc, part_number, sell_price, cost, supplier,
                        supplier_url, asin, agency, institution, source,
                        qty, _match, _add, _sup, _record, _update, stats):
    """Learn a single item into the catalog. Used by rebuild and live hooks."""
    desc = (desc or "").strip()
    part_number = (part_number or "").strip()
    if not desc and not part_number:
        return

    # Skip test/placeholder items
    if desc.lower() in ("test", "test item", "x", ""):
        return
    if len(desc) < 5 and not part_number:
        return

    # Match against existing catalog
    matches = _match(desc, part_number, top_n=1)

    if matches and matches[0].get("match_confidence", 0) >= 0.45:
        # Update existing
        pid = matches[0]["id"]
        updates = {"times_quoted": (matches[0].get("times_quoted") or 0) + 1}
        if sell_price and sell_price > 0:
            updates["last_sold_price"] = float(sell_price)
            updates["last_sold_date"] = datetime.now().isoformat()
        if cost and cost > 0:
            updates["cost"] = float(cost)
        if sell_price and sell_price > 0:
            updates["sell_price"] = float(sell_price)
        _update(pid, **updates)
        stats["items_updated"] += 1
    else:
        # Add new
        pid = _add(
            description=desc,
            part_number=part_number,
            mfg_number=part_number,
            cost=float(cost) if cost else 0,
            sell_price=float(sell_price) if sell_price else 0,
            uom="EA",
            category="",
        )
        stats["items_added"] += 1

    if not pid:
        return

    # Record supplier
    if supplier and supplier_url:
        try:
            _sup(pid, supplier, float(cost) if cost else 0, url=supplier_url)
        except Exception:
            pass
    elif asin:
        try:
            _sup(pid, "Amazon", float(cost) if cost else 0,
                 url=f"https://www.amazon.com/dp/{asin}")
        except Exception:
            pass

    # Record price history
    if sell_price and sell_price > 0:
        try:
            _record(pid, "quoted", float(sell_price), quantity=float(qty or 1),
                    source=source, agency=agency, institution=institution)
        except Exception:
            pass
    if cost and cost > 0:
        try:
            _record(pid, "cost", float(cost), quantity=float(qty or 1),
                    source=source, agency=agency, institution=institution)
        except Exception:
            pass


@bp.route("/api/catalog/rebuild-from-history", methods=["POST"])
@auth_required
@safe_route
def api_catalog_rebuild():
    """Rebuild catalog from all historical quotes, PCs, orders, RFQs."""
    result = _catalog_rebuild_from_history()
    return jsonify(result)


# (api_catalog_stats already defined in routes_crm.py — use that instead)


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE #10: PRICE TREND ALERTS
# Detect significant price changes and alert on dashboard
# ═══════════════════════════════════════════════════════════════════════════════

def _get_price_alerts(threshold_pct=10.0, limit=20):
    """Scan catalog for items with significant price changes.
    
    Returns alerts for:
    - SCPRS price dropped (opportunity to undercut)
    - SCPRS price rose (margin risk)
    - Amazon/web cost changed significantly
    - Item quoted at very different prices over time
    """
    alerts = []
    try:
        from src.agents.product_catalog import init_catalog_db
        init_catalog_db()
        
        db_path = os.path.join(DATA_DIR, "reytech.db")
        from src.core.db import get_db
        with get_db() as conn:

            # Get items with price history
            products = conn.execute("""
                SELECT p.id, p.name, p.description, p.sku, p.mfg_number,
                       p.sell_price, p.cost, p.scprs_last_price, p.scprs_last_date,
                       p.web_lowest_price, p.web_lowest_source, p.best_cost, p.best_supplier,
                       p.last_sold_price, p.times_quoted
                FROM product_catalog p
                WHERE p.sell_price > 0 OR p.cost > 0 OR p.scprs_last_price > 0
                ORDER BY p.times_quoted DESC
                LIMIT 200
            """).fetchall()

            for prod in products:
                pid = prod["id"]
                name = prod["name"] or prod["description"] or ""
            
                # Get price history for this item
                history = conn.execute("""
                    SELECT price_type, price, source, agency, recorded_at
                    FROM catalog_price_history
                    WHERE product_id = ?
                    ORDER BY recorded_at DESC
                    LIMIT 20
                """, (pid,)).fetchall()

                if len(history) < 2:
                    continue

                # Group by type
                costs = [h for h in history if h["price_type"] == "cost"]
                quoted = [h for h in history if h["price_type"] == "quoted"]
                scprs = [h for h in history if h["price_type"] == "scprs"]

                # Alert: Cost increased significantly
                if len(costs) >= 2:
                    latest = costs[0]["price"]
                    prev = costs[1]["price"]
                    if prev > 0 and latest > 0:
                        pct = ((latest - prev) / prev) * 100
                        if abs(pct) >= threshold_pct:
                            alerts.append({
                                "type": "cost_increase" if pct > 0 else "cost_decrease",
                                "severity": "high" if abs(pct) > 25 else "medium",
                                "product_id": pid,
                                "product_name": name[:60],
                                "message": f"Cost {'↑' if pct > 0 else '↓'} {abs(pct):.0f}%: ${prev:.2f} → ${latest:.2f}",
                                "old_price": prev,
                                "new_price": latest,
                                "pct_change": round(pct, 1),
                                "source": costs[0]["source"],
                                "date": costs[0]["recorded_at"],
                            })

                # Alert: Quoted price variance (same item quoted at very different prices)
                if len(quoted) >= 2:
                    prices = [q["price"] for q in quoted if q["price"] > 0]
                    if prices:
                        avg = sum(prices) / len(prices)
                        latest = prices[0]
                        if avg > 0:
                            spread = ((max(prices) - min(prices)) / avg) * 100
                            if spread > 20:
                                alerts.append({
                                    "type": "price_variance",
                                    "severity": "low",
                                    "product_id": pid,
                                    "product_name": name[:60],
                                    "message": f"Quote spread {spread:.0f}%: ${min(prices):.2f}–${max(prices):.2f} (avg ${avg:.2f})",
                                    "avg_price": round(avg, 2),
                                    "min_price": min(prices),
                                    "max_price": max(prices),
                                    "pct_change": round(spread, 1),
                                    "date": quoted[0]["recorded_at"],
                                })

                # Alert: SCPRS undercut opportunity
                sell = prod["sell_price"] or prod["last_sold_price"] or 0
                scprs_price = prod["scprs_last_price"] or 0
                if sell > 0 and scprs_price > 0:
                    if sell > scprs_price * 1.15:
                        alerts.append({
                            "type": "scprs_undercut",
                            "severity": "high",
                            "product_id": pid,
                            "product_name": name[:60],
                            "message": f"Your price ${sell:.2f} is {((sell/scprs_price - 1)*100):.0f}% above SCPRS ${scprs_price:.2f}",
                            "your_price": sell,
                            "scprs_price": scprs_price,
                            "pct_change": round((sell/scprs_price - 1) * 100, 1),
                            "date": prod["scprs_last_date"] or "",
                        })

                # Alert: Margin erosion (cost > 80% of sell price)
                cost = prod["cost"] or prod["best_cost"] or 0
                if sell > 0 and cost > 0:
                    margin = (sell - cost) / sell * 100
                    if margin < 10 and margin > 0:
                        alerts.append({
                            "type": "margin_warning",
                            "severity": "high",
                            "product_id": pid,
                            "product_name": name[:60],
                            "message": f"Margin only {margin:.1f}%: sell ${sell:.2f}, cost ${cost:.2f}",
                            "sell_price": sell,
                            "cost": cost,
                            "pct_change": round(margin, 1),
                            "date": datetime.now().isoformat(),
                        })

    except Exception as e:
        log.error("Price alerts error: %s", e, exc_info=True)

    # Sort by severity
    severity_order = {"high": 0, "medium": 1, "low": 2}
    alerts.sort(key=lambda a: severity_order.get(a.get("severity", "low"), 3))
    return alerts[:limit]


@bp.route("/api/price-alerts")
@auth_required
@safe_route
def api_price_alerts():
    """Get price trend alerts for dashboard."""
    try:
        threshold = max(0.0, min(float(request.args.get("threshold", 10)), 100.0))
    except (ValueError, TypeError, OverflowError):
        threshold = 10.0
    try:
        limit = max(1, min(int(request.args.get("limit", 20)), 200))
    except (ValueError, TypeError, OverflowError):
        limit = 20
    alerts = _get_price_alerts(threshold_pct=threshold, limit=limit)
    return jsonify({"ok": True, "alerts": alerts, "count": len(alerts)})


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE #11: WIN/LOSS ANALYSIS
# Track why quotes are won or lost, feed back into pricing intelligence
# ═══════════════════════════════════════════════════════════════════════════════

_WIN_LOSS_REASONS = {
    "won": [
        "competitive_price", "relationship", "fast_response",
        "product_match", "sole_source", "incumbent", "other"
    ],
    "lost": [
        "too_expensive", "competitor_won", "no_response", "project_cancelled",
        "budget_cut", "wrong_product", "slow_response", "other"
    ],
    "expired": [
        "no_follow_up", "buyer_unresponsive", "project_delayed", "other"
    ],
}


def _load_win_loss_log():
    """Load win/loss tracking data."""
    path = os.path.join(DATA_DIR, "win_loss_log.json")
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_win_loss_log(records):
    """Save win/loss tracking data."""
    path = os.path.join(DATA_DIR, "win_loss_log.json")
    with open(path, "w") as f:
        json.dump(records, f, indent=2)


@bp.route("/api/rfq/<rid>/outcome", methods=["POST"])
@auth_required
@safe_route
def api_rfq_outcome(rid):
    """Record win/loss outcome for an RFQ/quote with reason."""
    data = request.get_json(force=True)
    outcome = data.get("outcome", "")  # won, lost, expired, no_response
    reason = data.get("reason", "")
    notes = data.get("notes", "")
    competitor = data.get("competitor", "")
    competitor_price = data.get("competitor_price", 0)

    if outcome not in ("won", "lost", "expired", "no_response"):
        return jsonify({"ok": False, "error": "Invalid outcome"}), 400

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    # Update RFQ status
    r["status"] = outcome if outcome != "no_response" else "expired"
    r["outcome"] = outcome
    r["outcome_reason"] = reason
    r["outcome_notes"] = notes
    r["outcome_date"] = datetime.now().isoformat()
    r["competitor"] = competitor
    r["competitor_price"] = competitor_price

    # Add to status history
    history = r.get("status_history", [])
    history.append({
        "from": r.get("status", ""),
        "to": outcome,
        "timestamp": datetime.now().isoformat(),
        "actor": "user",
        "reason": reason,
        "notes": notes,
    })
    r["status_history"] = history
    save_rfqs(rfqs)

    # Log to win/loss journal
    wl_log = _load_win_loss_log()
    record = {
        "id": f"WL-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "rfq_id": rid,
        "solicitation": r.get("solicitation_number", ""),
        "agency": r.get("agency_name", "") or r.get("agency", ""),
        "institution": r.get("delivery_location", ""),
        "requestor": r.get("requestor_name", ""),
        "items_count": len(r.get("line_items", [])),
        "total_quoted": sum(
            (it.get("price_per_unit", 0) or 0) * (it.get("qty", 1) or 1)
            for it in r.get("line_items", [])
        ),
        "outcome": outcome,
        "reason": reason,
        "notes": notes,
        "competitor": competitor,
        "competitor_price": competitor_price,
        "recorded_at": datetime.now().isoformat(),
        "days_to_decision": 0,
    }

    # Calculate days to decision
    created = r.get("parsed_at", "") or r.get("received_at", "")
    if created:
        try:
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00").split("+")[0])
            record["days_to_decision"] = (datetime.now() - created_dt).days
        except (ValueError, TypeError):
            pass

    wl_log.append(record)
    _save_win_loss_log(wl_log)

    # Feed outcome to catalog
    if outcome == "won":
        try:
            from src.agents.product_catalog import record_won_price, init_catalog_db
            init_catalog_db()
            for it in r.get("line_items", []):
                price = it.get("price_per_unit", 0)
                if price and price > 0:
                    record_won_price(
                        product_name=it.get("description", ""),
                        price=float(price),
                        agency=record["agency"],
                        institution=record["institution"],
                    )
        except Exception as e:
            log.debug("Win catalog update: %s", e)

    log.info("Outcome recorded: %s → %s (%s) for RFQ %s", rid, outcome, reason, r.get("solicitation_number", ""))
    return jsonify({"ok": True, "outcome": outcome, "reason": reason})


@bp.route("/api/win-loss-analytics")
@auth_required
@safe_route
def api_win_loss_analytics():
    """Win/loss analysis dashboard data."""
    wl_log = _load_win_loss_log()

    # Also scan RFQs for any with outcome set
    rfqs = load_rfqs()
    rfq_outcomes = []
    for rid, r in rfqs.items():
        if r.get("outcome"):
            rfq_outcomes.append({
                "rfq_id": rid,
                "outcome": r["outcome"],
                "reason": r.get("outcome_reason", ""),
                "agency": r.get("agency_name", "") or r.get("agency", ""),
                "total": sum((it.get("price_per_unit", 0) or 0) * (it.get("qty", 1) or 1) for it in r.get("line_items", [])),
            })

    # Also check quotes log for status
    try:
        with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
            quotes = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        quotes = []

    quote_statuses = _defaultdict(int)
    for q in quotes:
        quote_statuses[q.get("status", "unknown")] += 1

    total_quoted = sum(q.get("total", 0) for q in quotes)
    won_value = sum(q.get("total", 0) for q in quotes if q.get("status") == "won")
    lost_value = sum(q.get("total", 0) for q in quotes if q.get("status") == "lost")

    # Reason breakdown
    reason_counts = _defaultdict(int)
    for r in wl_log:
        key = f"{r.get('outcome', '?')}:{r.get('reason', 'unknown')}"
        reason_counts[key] += 1

    # Agency win rate
    agency_stats = _defaultdict(lambda: {"won": 0, "lost": 0, "expired": 0, "total_value": 0})
    for r in wl_log:
        ag = r.get("agency", "Unknown")
        agency_stats[ag][r.get("outcome", "unknown")] += 1
        if r.get("outcome") == "won":
            agency_stats[ag]["total_value"] += r.get("total_quoted", 0)

    # Days to decision
    decision_days = [r.get("days_to_decision", 0) for r in wl_log if r.get("days_to_decision", 0) > 0]
    avg_decision_days = round(sum(decision_days) / len(decision_days), 1) if decision_days else 0

    return jsonify({
        "ok": True,
        "summary": {
            "total_quotes": len(quotes),
            "total_quoted_value": round(total_quoted, 2),
            "won": quote_statuses.get("won", 0),
            "won_value": round(won_value, 2),
            "lost": quote_statuses.get("lost", 0),
            "lost_value": round(lost_value, 2),
            "pending": quote_statuses.get("pending", 0) + quote_statuses.get("sent", 0),
            "win_rate": round(quote_statuses.get("won", 0) / max(quote_statuses.get("won", 0) + quote_statuses.get("lost", 0), 1) * 100, 1),
            "avg_decision_days": avg_decision_days,
        },
        "quote_statuses": dict(quote_statuses),
        "reason_breakdown": dict(reason_counts),
        "agency_stats": dict(agency_stats),
        "recent_outcomes": wl_log[-20:][::-1],
        "win_loss_log_count": len(wl_log),
    })


@bp.route("/api/win-loss-reasons")
@auth_required
@safe_route
def api_win_loss_reasons():
    """Get valid reasons for each outcome type."""
    return jsonify({"ok": True, "reasons": _WIN_LOSS_REASONS})


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE #13: FACILITY EXPANSION OUTREACH
# Cold email campaigns to untouched facilities from territory intelligence
# ═══════════════════════════════════════════════════════════════════════════════

_OUTREACH_TEMPLATES = {
    "intro": {
        "subject": "Reytech Inc. — Your New Source for {category} Supplies",
        "body": """Dear {buyer_name},

I'm reaching out from Reytech Inc. regarding supply needs at {facility}.

We are an authorized California state vendor (DGS-approved, DVBE certified) specializing in {category} supplies for state institutions. We currently serve several {agency} facilities and would welcome the opportunity to support {facility} as well.

Our advantages:
• Competitive SCPRS pricing with fast turnaround
• Direct Amazon Business and distributor sourcing
• Automated quoting — we typically respond to 704s within hours
• Dedicated support for CA state procurement

I'd be happy to provide pricing on any upcoming needs. Feel free to send over a 704 form or requisition anytime.

Best regards,
Mike Gorzell
Reytech Inc.
mike@reytechinc.com
(916) 548-9484"""
    },
    "follow_up": {
        "subject": "Following Up — Reytech Inc. for {facility}",
        "body": """Hi {buyer_name},

Just following up on my earlier message. Reytech Inc. is ready to support {facility} with competitive pricing on {category} supplies.

We recently fulfilled orders for {reference_facility} in the same {agency} network and would love to extend that service to your location.

If you have any upcoming needs or 704 forms to price, please don't hesitate to reach out.

Best,
Mike Gorzell
Reytech Inc."""
    },
    "capability": {
        "subject": "Capability Statement — Reytech Inc.",
        "body": """Dear {buyer_name},

Please find below a brief overview of Reytech Inc.'s capabilities for {agency} procurement:

Company: Reytech Inc.
CAGE Code: Available upon request
Certifications: DGS-Approved, DVBE Certified
Specialties: {category}
Coverage: All California state facilities
Response Time: Same-day quoting on most items

We maintain competitive pricing through direct manufacturer and distributor relationships, and our automated quoting system ensures rapid responses to your procurement needs.

I welcome the opportunity to discuss how we can support {facility}.

Regards,
Mike Gorzell
Reytech Inc.
mike@reytechinc.com"""
    },
}


def _load_outreach_queue():
    """Load outreach queue."""
    path = os.path.join(DATA_DIR, "outreach_queue.json")
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_outreach_queue(queue):
    """Save outreach queue."""
    path = os.path.join(DATA_DIR, "outreach_queue.json")
    with open(path, "w") as f:
        json.dump(queue, f, indent=2)


def _build_outreach_targets():
    """Build list of outreach targets from CRM contacts and territory intelligence.
    
    Targets: contacts with email addresses that haven't been emailed yet.
    Prioritizes: contacts at facilities with purchase history at other facilities in same agency.
    """
    targets = []

    # Load CRM contacts
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            contacts = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        contacts = {}

    contact_list = contacts.values() if isinstance(contacts, dict) else contacts

    # Load growth prospects
    prospects_path = os.path.join(DATA_DIR, "growth_prospects.json")
    try:
        with open(prospects_path) as f:
            pdata = json.load(f)
        prospects = pdata.get("prospects", []) if isinstance(pdata, dict) else pdata
    except (FileNotFoundError, json.JSONDecodeError):
        prospects = []

    # Load existing outreach to skip already-emailed
    queue = _load_outreach_queue()
    emailed_emails = {q.get("email", "").lower() for q in queue if q.get("status") in ("sent", "follow_up_sent")}

    # Get agencies we've done business with (for reference)
    try:
        with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
            quotes = json.load(f)
        active_agencies = {q.get("agency", "") for q in quotes if q.get("status") in ("sent", "won")}
        active_institutions = {q.get("institution", "") for q in quotes if q.get("status") in ("sent", "won")}
    except (FileNotFoundError, json.JSONDecodeError):
        active_agencies = set()
        active_institutions = set()

    # Process CRM contacts
    for c in contact_list:
        email = (c.get("email") or "").strip().lower()
        if not email or "@" not in email or email in emailed_emails:
            continue

        agency = c.get("agency", "")
        facility = c.get("facility", "") or c.get("institution", "") or ""
        name = c.get("name", "") or c.get("buyer_name", "") or email.split("@")[0]
        status = c.get("outreach_status", "new")

        # Score: higher if we already serve this agency
        score = 50
        if agency in active_agencies:
            score += 30  # We know the agency
        if facility and facility not in active_institutions:
            score += 10  # Untouched facility in known agency
        if c.get("title", "").lower() in ("buyer", "procurement", "purchasing"):
            score += 10

        targets.append({
            "email": email,
            "name": name,
            "agency": agency,
            "facility": facility,
            "title": c.get("title", ""),
            "phone": c.get("phone", ""),
            "score": score,
            "source": "crm",
            "contact_id": c.get("id", ""),
            "status": status,
            "categories": c.get("categories_matched", []),
        })

    # Process growth prospects
    for p in prospects:
        email = (p.get("buyer_email") or "").strip().lower()
        if not email or "@" not in email or email in emailed_emails:
            continue
        # Avoid duplicates from CRM
        if any(t["email"] == email for t in targets):
            continue

        targets.append({
            "email": email,
            "name": p.get("buyer_name", ""),
            "agency": p.get("agency", ""),
            "facility": "",
            "title": "",
            "phone": p.get("buyer_phone", ""),
            "score": p.get("opportunity_score", 50),
            "source": "growth_prospect",
            "contact_id": p.get("id", ""),
            "status": p.get("outreach_status", "new"),
            "categories": p.get("categories_matched", []),
        })

    targets.sort(key=lambda t: t["score"], reverse=True)
    return targets


@bp.route("/api/outreach/targets")
@auth_required
@safe_route
def api_outreach_targets():
    """Get prioritized list of outreach targets."""
    targets = _build_outreach_targets()
    return jsonify({"ok": True, "targets": targets, "count": len(targets)})


@bp.route("/api/outreach/draft", methods=["POST"])
@auth_required
@safe_route
def api_outreach_draft():
    """Draft an outreach email for a target contact."""
    data = request.get_json(force=True)
    email = data.get("email", "")
    template_key = data.get("template", "intro")
    custom_category = data.get("category", "general procurement")

    if not email:
        return jsonify({"ok": False, "error": "Email required"}), 400

    # Find target info
    targets = _build_outreach_targets()
    target = next((t for t in targets if t["email"].lower() == email.lower()), None)

    if not target:
        target = {
            "name": data.get("name", email.split("@")[0]),
            "agency": data.get("agency", ""),
            "facility": data.get("facility", ""),
        }

    template = _OUTREACH_TEMPLATES.get(template_key, _OUTREACH_TEMPLATES["intro"])

    # Find a reference facility (one we've served in the same agency)
    reference = ""
    try:
        with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
            for q in json.load(f):
                if q.get("agency", "") == target.get("agency", "") and q.get("status") in ("sent", "won"):
                    reference = q.get("institution", "") or q.get("ship_to_name", "")
                    if reference:
                        break
    except Exception:
        pass

    # Categories from their purchase history
    categories = target.get("categories", [])
    cat_str = ", ".join(categories[:3]) if categories else custom_category

    # Fill template
    subject = template["subject"].format(
        buyer_name=target.get("name", ""),
        agency=target.get("agency", ""),
        facility=target.get("facility", "") or target.get("agency", ""),
        category=cat_str,
    )
    body = template["body"].format(
        buyer_name=target.get("name", "Procurement Team"),
        agency=target.get("agency", "your agency"),
        facility=target.get("facility", "") or target.get("agency", "your facility"),
        category=cat_str,
        reference_facility=reference or "other facilities",
    )

    return jsonify({
        "ok": True,
        "draft": {
            "to": email,
            "subject": subject,
            "body": body,
            "template": template_key,
            "target": target,
        }
    })


@bp.route("/api/outreach/send", methods=["POST"])
@auth_required
@safe_route
def api_outreach_send():
    """Send an outreach email and log it."""
    data = request.get_json(force=True)
    to_email = data.get("to", "")
    subject = data.get("subject", "")
    body = data.get("body", "")

    if not to_email or not subject or not body:
        return jsonify({"ok": False, "error": "to, subject, body required"}), 400

    # Send via existing SMTP
    try:
        from src.agents.email_poller import EmailSender
        smtp_config = {
            "smtp_host": os.environ.get("SMTP_HOST", "smtp.gmail.com"),
            "smtp_port": int(os.environ.get("SMTP_PORT", 587)),
            "email_addr": os.environ.get("GMAIL_ADDRESS", ""),
            "email_pwd": os.environ.get("GMAIL_APP_PASSWORD", ""),
            "from_name": os.environ.get("EMAIL_FROM_NAME", "Reytech Inc."),
        }
        sender = EmailSender(smtp_config)
        draft = {"to": to_email, "subject": subject, "body": body}
        sender.send(draft)
        sent = True
    except Exception as e:
        log.error("Outreach send failed: %s", e)
        return jsonify({"ok": False, "error": f"Send failed: {str(e)[:80]}"})

    # Log to outreach queue
    queue = _load_outreach_queue()
    record = {
        "id": f"OUT-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "email": to_email,
        "name": data.get("name", ""),
        "agency": data.get("agency", ""),
        "facility": data.get("facility", ""),
        "subject": subject,
        "template": data.get("template", "custom"),
        "status": "sent",
        "sent_at": datetime.now().isoformat(),
        "follow_up_date": (datetime.now() + _timedelta(days=7)).isoformat(),
        "opened": False,
        "replied": False,
        "notes": "",
    }
    queue.append(record)
    _save_outreach_queue(queue)

    # Update CRM contact status
    try:
        crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
        with open(crm_path) as f:
            contacts = json.load(f)
        for cid, c in contacts.items():
            if (c.get("email") or "").lower() == to_email.lower():
                c["outreach_status"] = "emailed"
                c["last_outreach"] = datetime.now().isoformat()
                break
        with open(crm_path, "w") as f:
            json.dump(contacts, f, indent=2)
    except Exception:
        pass

    log.info("Outreach sent: %s → %s", to_email, subject)
    return jsonify({"ok": True, "record": record})


@bp.route("/api/outreach/queue")
@auth_required
@safe_route
def api_outreach_queue():
    """Get outreach queue with status."""
    queue = _load_outreach_queue()
    # Check for follow-ups due
    now = datetime.now().isoformat()
    for item in queue:
        if item.get("status") == "sent" and item.get("follow_up_date", "") <= now:
            item["follow_up_due"] = True
    return jsonify({"ok": True, "queue": queue, "count": len(queue),
                    "follow_ups_due": sum(1 for q in queue if q.get("follow_up_due"))})


@bp.route("/api/outreach/update/<oid>", methods=["POST"])
@auth_required
@safe_route
def api_outreach_update(oid):
    """Update outreach record (mark replied, add notes, etc)."""
    data = request.get_json(force=True)
    queue = _load_outreach_queue()
    for item in queue:
        if item.get("id") == oid:
            for key in ("status", "notes", "replied", "opened"):
                if key in data:
                    item[key] = data[key]
            item["updated_at"] = datetime.now().isoformat()
            _save_outreach_queue(queue)
            return jsonify({"ok": True, "record": item})
    return jsonify({"ok": False, "error": "Not found"}), 404


# ═══════════════════════════════════════════════════════════════════════════════
# COMBINED DASHBOARD: Growth Intelligence Page
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/growth-intel")
@auth_required
@safe_page
def growth_intel_page():
    """Growth Intelligence dashboard combining all 4 features."""

    # Catalog stats
    try:
        from src.agents.product_catalog import get_catalog_stats, init_catalog_db
        init_catalog_db()
        catalog_stats = get_catalog_stats()
    except Exception:
        catalog_stats = {"total_products": 0}

    # Price alerts
    alerts = _get_price_alerts(limit=10)

    # Win/loss summary
    wl_log = _load_win_loss_log()
    try:
        with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
            quotes = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        quotes = []

    won_count = sum(1 for q in quotes if q.get("status") == "won")
    lost_count = sum(1 for q in quotes if q.get("status") == "lost")
    pending_count = sum(1 for q in quotes if q.get("status") in ("pending", "sent"))

    # Outreach stats
    outreach_queue = _load_outreach_queue()
    outreach_targets = _build_outreach_targets()
    follow_ups_due = sum(1 for q in outreach_queue
                         if q.get("status") == "sent"
                         and q.get("follow_up_date", "9999") <= datetime.now().isoformat())

    return render_page("growth_intelligence.html",
        catalog_stats=catalog_stats,
        alerts=alerts,
        won_count=won_count,
        lost_count=lost_count,
        pending_count=pending_count,
        total_quotes=len(quotes),
        wl_log=wl_log[-10:][::-1],
        outreach_sent=len(outreach_queue),
        outreach_targets_count=len(outreach_targets),
        follow_ups_due=follow_ups_due,
        outreach_targets=outreach_targets[:20],
        outreach_queue=outreach_queue[-10:][::-1],
    )
