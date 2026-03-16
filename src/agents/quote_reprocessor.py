"""
Quote Reprocessor
Re-enriches existing price checks with FI$Cal intelligence.
Runs after each data pull to keep pricing current.
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.quote_reprocessor")


def reprocess_all_quotes():
    """Re-enrich pending PCs + validate sent PCs with fresh market data."""
    log.info("=" * 50)
    log.info("QUOTE REPROCESSOR — STARTING")
    log.info("=" * 50)

    pending = _enrich_pending_pcs()
    validated = _validate_sent_pcs()

    log.info("QUOTE REPROCESSOR COMPLETE")
    log.info("  Pending enriched: %d", pending)
    log.info("  Sent validated: %d", validated)
    log.info("=" * 50)

    return {"pending_enriched": pending, "sent_validated": validated}


def _enrich_pending_pcs():
    """Enrich pending PCs and quotes with pricing intelligence."""
    import json
    import sqlite3
    from src.core.db import DB_PATH

    log.info("Enriching pending PCs and quotes...")
    enriched_count = 0

    try:
        db = sqlite3.connect(DB_PATH, timeout=30)
        from src.agents.quote_intelligence import enrich_extracted_items

        # --- Price Checks (pc_data JSON blob) ---
        try:
            pcs = db.execute("""
                SELECT id, pc_data, items FROM price_checks
                WHERE LOWER(status) IN ('parsed', 'new', 'pending', 'review', 'priced')
            """).fetchall()
            log.info("Found %d pending PCs", len(pcs))

            for pc in pcs:
                pc_id = pc[0]
                try:
                    pc_data = json.loads(pc[1] or "{}") if pc[1] else {}
                    items_json = json.loads(pc[2] or "[]") if pc[2] else []

                    items = pc_data.get("items", pc_data.get("line_items", items_json))
                    if isinstance(items, str):
                        items = json.loads(items)
                    if not items:
                        continue

                    # Skip if recently enriched
                    last = pc_data.get("intelligence_enriched_at", "")
                    if last:
                        try:
                            if (datetime.now() - datetime.fromisoformat(last)).total_seconds() < 43200:
                                continue
                        except Exception:
                            pass

                    items_for_enrichment = [{
                        "description": it.get("description", it.get("desc", "")),
                        "quantity": it.get("quantity", it.get("qty", 1)),
                        "unit_price": it.get("unit_price", it.get("price")),
                        "cost": it.get("cost", it.get("unit_cost", it.get("unit_price"))),
                    } for it in items]

                    enriched = enrich_extracted_items(items_for_enrichment)
                    count = 0
                    for i, e in enumerate(enriched):
                        if i < len(items) and e.get("intelligence"):
                            items[i]["intelligence"] = e["intelligence"]
                            count += 1

                    if count > 0:
                        pc_data["items"] = items
                        pc_data["intelligence_enriched_at"] = datetime.now().isoformat()
                        db.execute("UPDATE price_checks SET pc_data = ? WHERE id = ?",
                                   (json.dumps(pc_data, default=str), pc_id))
                        enriched_count += 1
                        log.info("Enriched PC %s: %d items", pc_id, count)
                except Exception as e:
                    log.warning("PC %s: %s", pc_id, str(e)[:60])
        except Exception as e:
            log.warning("PC enrichment: %s", e)

        # --- Quotes (items_detail JSON blob) ---
        try:
            quotes = db.execute("""
                SELECT id, items_detail, line_items, status FROM quotes
                WHERE LOWER(status) IN ('pending', 'draft', 'new', 'review')
            """).fetchall()
            log.info("Found %d pending quotes", len(quotes))

            for q in quotes:
                q_id = q[0]
                try:
                    items_raw = q[1] or q[2] or "[]"
                    items = json.loads(items_raw) if isinstance(items_raw, str) else items_raw
                    if not items:
                        continue

                    items_for_enrichment = [{
                        "description": it.get("description", ""),
                        "quantity": it.get("quantity", it.get("qty", 1)),
                        "unit_price": it.get("unit_price", it.get("price")),
                        "cost": it.get("cost", it.get("unit_cost")),
                    } for it in items]

                    enriched = enrich_extracted_items(items_for_enrichment)
                    count = 0
                    for i, e in enumerate(enriched):
                        if i < len(items) and e.get("intelligence"):
                            items[i]["intelligence"] = e["intelligence"]
                            count += 1

                    if count > 0:
                        db.execute("UPDATE quotes SET items_detail = ? WHERE id = ?",
                                   (json.dumps(items, default=str), q_id))
                        enriched_count += 1
                        log.info("Enriched quote %s: %d items", q_id, count)
                except Exception as e:
                    log.warning("Quote %s: %s", q_id, str(e)[:60])
        except Exception as e:
            log.warning("Quote enrichment: %s", e)

        db.commit()
        db.close()
    except Exception as e:
        log.error("Enrichment failed: %s", e)

    return enriched_count


def _validate_sent_pcs():
    """Validate sent quotes/PCs against current market data."""
    import json
    import sqlite3
    from src.core.db import DB_PATH

    log.info("Validating sent PCs and quotes...")
    validated_count = 0
    underpriced = 0
    alerts = []

    try:
        db = sqlite3.connect(DB_PATH, timeout=30)
        from src.agents.quote_intelligence import get_competitor_prices, _parse_price_str

        # --- Sent Price Checks ---
        try:
            pcs = db.execute("""
                SELECT id, pc_data, items, status FROM price_checks
                WHERE LOWER(status) IN ('sent', 'submitted', 'won', 'completed', 'pending_award')
            """).fetchall()
            log.info("Found %d sent PCs to validate", len(pcs))

            for pc in pcs:
                pc_id = pc[0]
                try:
                    pc_data = json.loads(pc[1] or "{}") if pc[1] else {}
                    items = pc_data.get("items", pc_data.get("line_items", []))
                    if isinstance(items, str):
                        items = json.loads(items)
                    if not items:
                        items_raw = pc[2]
                        items = json.loads(items_raw or "[]") if items_raw else []

                    institution = pc_data.get("institution", "")
                    pc_number = pc_data.get("pc_number", pc_id)
                    underpriced_items = []

                    for item in items:
                        desc = item.get("description", item.get("desc", ""))
                        qp = item.get("unit_price", item.get("price"))
                        if not qp:
                            qp = (item.get("pricing") or {}).get("recommended_price")
                        qty = item.get("quantity", item.get("qty", 1)) or 1
                        if not desc or not qp:
                            continue
                        try:
                            qp = float(str(qp).replace("$", "").replace(",", ""))
                        except (ValueError, TypeError):
                            continue

                        comps = get_competitor_prices(desc, limit=10)
                        comp_prices = [_parse_price_str(cp.get("unit_price"))
                                       for cp in comps if not cp.get("is_reytech")]
                        comp_prices = [p for p in comp_prices if p and p > 0]

                        if comp_prices:
                            comp_avg = sum(comp_prices) / len(comp_prices)
                            gap = comp_avg - qp
                            gap_pct = (gap / comp_avg) * 100 if comp_avg > 0 else 0

                            validation = {
                                "quoted_price": qp, "market_avg": round(comp_avg, 2),
                                "gap_per_unit": round(gap, 2), "gap_pct": round(gap_pct, 1),
                                "gap_total": round(gap * float(qty), 2),
                                "validated_at": datetime.now().isoformat(),
                                "flag": ("SIGNIFICANTLY_UNDERPRICED" if gap_pct > 15
                                         else ("SLIGHTLY_UNDER" if gap_pct > 5
                                               else ("ABOVE_MARKET" if gap_pct < -10
                                                     else "WELL_PRICED"))),
                            }
                            item["market_validation"] = validation
                            validated_count += 1
                            if gap_pct > 15:
                                underpriced += 1
                                underpriced_items.append({
                                    "description": desc[:80], "our_price": qp,
                                    "market_avg": round(comp_avg, 2),
                                    "gap_per_unit": round(gap, 2),
                                    "money_left_on_table": round(gap * float(qty), 2),
                                    "qty": qty,
                                })

                    if underpriced_items:
                        alerts.append({
                            "pc_id": pc_id, "pc_number": pc_number,
                            "institution": institution,
                            "underpriced_items": underpriced_items,
                            "total_money_left": round(sum(u["money_left_on_table"] for u in underpriced_items), 2),
                        })

                    # Save back
                    pc_data["items"] = items
                    pc_data["market_validated_at"] = datetime.now().isoformat()
                    db.execute("UPDATE price_checks SET pc_data = ? WHERE id = ?",
                               (json.dumps(pc_data, default=str), pc_id))
                except Exception as e:
                    log.warning("Validate PC %s: %s", pc_id, str(e)[:60])
        except Exception as e:
            log.warning("PC validation: %s", e)

        # --- Sent Quotes ---
        try:
            quotes = db.execute("""
                SELECT id, items_detail, line_items, status FROM quotes
                WHERE LOWER(status) IN ('sent', 'submitted', 'won')
            """).fetchall()
            log.info("Found %d sent quotes to validate", len(quotes))

            for q in quotes:
                q_id = q[0]
                try:
                    items = json.loads(q[1] or "[]") if q[1] else []
                    if not items:
                        items = json.loads(q[2] or "[]") if q[2] else []
                    if isinstance(items, str):
                        items = json.loads(items)

                    for item in items:
                        desc = item.get("description", item.get("desc", ""))
                        qp = item.get("unit_price", item.get("price"))
                        qty = item.get("quantity", item.get("qty", 1)) or 1
                        if not desc or not qp:
                            continue
                        try:
                            qp = float(str(qp).replace("$", "").replace(",", ""))
                        except (ValueError, TypeError):
                            continue

                        comps = get_competitor_prices(desc, limit=10)
                        comp_prices = [_parse_price_str(cp.get("unit_price"))
                                       for cp in comps if not cp.get("is_reytech")]
                        comp_prices = [p for p in comp_prices if p and p > 0]

                        if comp_prices:
                            comp_avg = sum(comp_prices) / len(comp_prices)
                            gap = comp_avg - qp
                            gap_pct = (gap / comp_avg) * 100 if comp_avg > 0 else 0
                            item["market_validation"] = {
                                "quoted_price": qp, "market_avg": round(comp_avg, 2),
                                "gap_per_unit": round(gap, 2), "gap_pct": round(gap_pct, 1),
                                "gap_total": round(gap * float(qty), 2),
                                "flag": ("SIGNIFICANTLY_UNDERPRICED" if gap_pct > 15
                                         else ("SLIGHTLY_UNDER" if gap_pct > 5
                                               else ("ABOVE_MARKET" if gap_pct < -10 else "WELL_PRICED"))),
                                "validated_at": datetime.now().isoformat(),
                            }
                            validated_count += 1
                            if gap_pct > 15:
                                underpriced += 1

                    db.execute("UPDATE quotes SET items_detail = ? WHERE id = ?",
                               (json.dumps(items, default=str), q_id))
                except Exception as e:
                    log.warning("Validate quote %s: %s", q_id, str(e)[:60])
        except Exception as e:
            log.warning("Quote validation: %s", e)

        db.commit()
        db.close()

        if alerts:
            _write_price_alerts(alerts)

    except Exception as e:
        log.error("Validation failed: %s", e)

    if underpriced:
        log.warning("UNDERPRICING ALERT: %d items below market", underpriced)

    return validated_count


def _write_price_alerts(alerts):
    """Write underpricing alerts to /data/price_alerts.json."""
    import json
    import os
    os.makedirs("/data", exist_ok=True)
    alert_data = {
        "generated_at": datetime.now().isoformat(),
        "total_alerts": len(alerts),
        "total_money_left": round(sum(a.get("total_money_left", 0) for a in alerts), 2),
        "alerts": alerts,
    }
    with open("/data/price_alerts.json", "w") as f:
        json.dump(alert_data, f, indent=2, default=str)
    log.info("Price alerts: %d PCs, $%.2f opportunity", len(alerts), alert_data["total_money_left"])
    try:
        from src.agents.notify_agent import send_alert
        send_alert("bell", f"Price validation: {len(alerts)} underpriced by ${alert_data['total_money_left']:,.2f}",
                   {"type": "price_validation"})
    except Exception:
        pass


def get_underpriced_report():
    """Get sent quotes/PCs that were underpriced vs market."""
    import json
    import sqlite3
    from src.core.db import DB_PATH
    results = []

    try:
        db = sqlite3.connect(DB_PATH, timeout=10)

        # Check price_checks
        try:
            rows = db.execute("""
                SELECT id, pc_data, status FROM price_checks
                WHERE LOWER(status) IN ('sent', 'submitted', 'won', 'completed')
            """).fetchall()
            for row in rows:
                try:
                    pc_data = json.loads(row[1] or "{}")
                    for item in pc_data.get("items", []):
                        v = item.get("market_validation", {})
                        if v.get("flag") in ("SIGNIFICANTLY_UNDERPRICED", "SLIGHTLY_UNDER"):
                            results.append({
                                "source": "price_check", "id": row[0], "status": row[2],
                                "description": item.get("description", item.get("desc", ""))[:80],
                                "quantity": item.get("quantity", item.get("qty")),
                                **v,
                            })
                except Exception:
                    pass
        except Exception:
            pass

        # Check quotes
        try:
            rows = db.execute("""
                SELECT id, items_detail, status FROM quotes
                WHERE LOWER(status) IN ('sent', 'submitted', 'won')
            """).fetchall()
            for row in rows:
                try:
                    items = json.loads(row[1] or "[]")
                    for item in items:
                        v = item.get("market_validation", {})
                        if v.get("flag") in ("SIGNIFICANTLY_UNDERPRICED", "SLIGHTLY_UNDER"):
                            results.append({
                                "source": "quote", "id": row[0], "status": row[2],
                                "description": item.get("description", item.get("desc", ""))[:80],
                                "quantity": item.get("quantity", item.get("qty")),
                                **v,
                            })
                except Exception:
                    pass
        except Exception:
            pass

        db.close()
    except Exception as e:
        results.append({"error": str(e)})

    return results
