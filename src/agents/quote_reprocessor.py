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
    """Find all unpriced/pending PC items and attach intelligence."""
    log.info("Enriching pending PCs...")
    enriched_count = 0

    try:
        # Import dashboard's PC loader (exec'd module, access via globals)
        import importlib
        import sys

        # Load PCs from DB
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.row_factory = sqlite3.Row

        rows = db.execute("""
            SELECT id, status, pc_data FROM price_checks
            WHERE LOWER(status) IN ('parsed', 'new', 'draft', 'priced', 'review')
        """).fetchall()

        log.info("Found %d pending PCs to enrich", len(rows))

        from src.agents.quote_intelligence import enrich_extracted_items
        import json

        for row in rows:
            pc_id = row["id"]
            try:
                pc_data = json.loads(row["pc_data"]) if row["pc_data"] else {}
            except (json.JSONDecodeError, TypeError):
                continue

            items = pc_data.get("items", [])
            if not items:
                continue

            # Check if already enriched recently
            last_enriched = pc_data.get("intelligence_enriched_at", "")
            if last_enriched:
                try:
                    last_dt = datetime.fromisoformat(last_enriched)
                    hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                    if hours_ago < 12:
                        continue  # Skip if enriched less than 12 hours ago
                except Exception:
                    pass

            # Enrich items
            try:
                enriched_items = enrich_extracted_items(items)
                items_with_intel = 0
                for i, enriched in enumerate(enriched_items):
                    if i < len(items) and enriched.get("intelligence"):
                        items[i]["intelligence"] = enriched["intelligence"]
                        items_with_intel += 1

                if items_with_intel > 0:
                    pc_data["items"] = items
                    pc_data["intelligence_enriched_at"] = datetime.now().isoformat()
                    pc_data["intelligence_items_count"] = items_with_intel

                    db.execute(
                        "UPDATE price_checks SET pc_data = ? WHERE id = ?",
                        (json.dumps(pc_data, default=str), pc_id)
                    )
                    enriched_count += 1
                    log.info("Enriched PC %s: %d/%d items with intelligence",
                             pc_id, items_with_intel, len(items))
            except Exception as e:
                log.warning("Enrich PC %s failed: %s", pc_id, str(e)[:60])

        db.commit()
        db.close()

    except Exception as e:
        log.error("Pending PC enrichment failed: %s", e)

    return enriched_count


def _validate_sent_pcs():
    """Check sent quotes against fresh market data. Flag underpriced items."""
    log.info("Validating sent PCs...")
    validated_count = 0
    alerts = []

    try:
        import sqlite3
        import json
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.row_factory = sqlite3.Row

        rows = db.execute("""
            SELECT id, status, pc_data FROM price_checks
            WHERE LOWER(status) IN ('sent', 'pending_award')
        """).fetchall()

        log.info("Found %d sent PCs to validate", len(rows))

        from src.agents.quote_intelligence import (
            search_catalog, get_competitor_prices, _parse_price_str, _normalize_unit_price
        )

        for row in rows:
            pc_id = row["id"]
            try:
                pc_data = json.loads(row["pc_data"]) if row["pc_data"] else {}
            except (json.JSONDecodeError, TypeError):
                continue

            items = pc_data.get("items", [])
            institution = pc_data.get("institution", "")
            pc_number = pc_data.get("pc_number", pc_id)
            underpriced_items = []

            for item in items:
                desc = item.get("description", "")
                our_price = item.get("pricing", {}).get("recommended_price")
                if not desc or not our_price:
                    continue

                try:
                    our_price = float(our_price)
                except (ValueError, TypeError):
                    continue

                # Get current market data
                competitors = get_competitor_prices(desc, limit=10)
                if not competitors:
                    continue

                # Calculate market avg (non-Reytech)
                comp_prices = []
                for cp in competitors:
                    if "REYTECH" in (cp.get("supplier", "") or "").upper():
                        continue
                    norm = _normalize_unit_price(
                        _parse_price_str(cp.get("unit_price")),
                        cp.get("description", "")
                    )
                    if norm and norm["normalized_price"] > 0:
                        comp_prices.append(norm["normalized_price"])

                if not comp_prices:
                    continue

                market_avg = sum(comp_prices) / len(comp_prices)

                # Flag if our price is significantly below market
                if our_price < market_avg * 0.85:
                    gap = market_avg - our_price
                    qty = item.get("qty", 1) or 1
                    money_left = gap * float(qty)
                    underpriced_items.append({
                        "description": desc[:80],
                        "our_price": our_price,
                        "market_avg": round(market_avg, 2),
                        "gap_per_unit": round(gap, 2),
                        "money_left_on_table": round(money_left, 2),
                        "qty": qty,
                    })

            if underpriced_items:
                alert = {
                    "pc_id": pc_id,
                    "pc_number": pc_number,
                    "institution": institution,
                    "underpriced_items": underpriced_items,
                    "total_money_left": round(sum(u["money_left_on_table"] for u in underpriced_items), 2),
                }
                alerts.append(alert)

                # Store validation result on PC
                pc_data["price_validation"] = {
                    "validated_at": datetime.now().isoformat(),
                    "underpriced_count": len(underpriced_items),
                    "total_money_left": alert["total_money_left"],
                    "details": underpriced_items,
                }
                db.execute(
                    "UPDATE price_checks SET pc_data = ? WHERE id = ?",
                    (json.dumps(pc_data, default=str), pc_id)
                )
                validated_count += 1

                log.warning("PC %s UNDERPRICED: %d items, $%.2f left on table",
                            pc_number, len(underpriced_items), alert["total_money_left"])

        db.commit()
        db.close()

        # Write alerts summary
        if alerts:
            _write_price_alerts(alerts)

    except Exception as e:
        log.error("Sent PC validation failed: %s", e)

    return validated_count


def _write_price_alerts(alerts):
    """Write underpricing alerts to /data/price_alerts.json."""
    import json
    import os
    os.makedirs("/data", exist_ok=True)

    alert_data = {
        "generated_at": datetime.now().isoformat(),
        "total_alerts": len(alerts),
        "total_money_left": round(sum(a["total_money_left"] for a in alerts), 2),
        "alerts": alerts,
    }

    with open("/data/price_alerts.json", "w") as f:
        json.dump(alert_data, f, indent=2, default=str)

    log.info("Price alerts written: %d PCs, $%.2f total opportunity",
             len(alerts), alert_data["total_money_left"])

    # Send notification
    try:
        from src.agents.notify_agent import send_alert
        msg = (f"Price validation: {len(alerts)} sent quotes underpriced by "
               f"${alert_data['total_money_left']:,.2f} total")
        send_alert("bell", msg, {"type": "price_validation"})
    except Exception:
        pass


def get_underpriced_report():
    """Get summary of sent quotes that were underpriced vs market."""
    import json
    import os
    results = []

    # First check the alerts file
    try:
        path = "/data/price_alerts.json"
        if os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
            for alert in data.get("alerts", []):
                for item in alert.get("underpriced_items", []):
                    results.append({
                        "pc_number": alert.get("pc_number", ""),
                        "institution": alert.get("institution", ""),
                        **item,
                    })
            return results
    except Exception:
        pass

    # Fallback: scan DB for intelligence with underpriced flags
    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)

        tables = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()

        for tbl in [t[0] for t in tables]:
            try:
                cols = [d[1] for d in db.execute(f"PRAGMA table_info([{tbl}])").fetchall()]
                if "intelligence" not in cols:
                    continue
                rows = db.execute(f"""
                    SELECT * FROM [{tbl}]
                    WHERE intelligence LIKE '%UNDERPRICED%'
                       OR intelligence LIKE '%SLIGHTLY_UNDER%'
                """).fetchall()
                intel_idx = cols.index("intelligence")
                for row in rows:
                    try:
                        intel = json.loads(row[intel_idx] or "{}")
                        validation = intel.get("market_validation", {})
                        if validation:
                            results.append(validation)
                    except Exception:
                        pass
            except Exception:
                pass

        db.close()
    except Exception as e:
        results.append({"error": str(e)})

    return results
