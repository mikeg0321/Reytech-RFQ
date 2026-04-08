"""
quote_lifecycle_shared.py — Unified win/loss/status logic for PC and RFQ.

Consolidates the duplicate mark-won/mark-lost implementations from
routes_pricecheck.py and routes_rfq.py into a single shared module.
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.lifecycle")


def mark_won(record, record_type, record_id, po_number="", notes=""):
    """Mark a PC or RFQ as won. Handles all side effects.

    Args:
        record: the PC or RFQ dict
        record_type: "pc" or "rfq"
        record_id: pcid or rfq_id
        po_number: optional PO number
        notes: optional notes

    Returns: dict with results
    """
    now = datetime.now().isoformat()
    result = {"ok": True, "record_type": record_type, "record_id": record_id}

    # Update status
    record["status"] = "won"
    record["outcome"] = "won"
    record["outcome_date"] = now
    record["closed_at"] = now
    record["closed_reason"] = f"Won — PO {po_number}" if po_number else "Won"
    if po_number:
        record["po_number"] = po_number

    # Log revenue
    items = record.get("items", record.get("line_items", []))
    try:
        from src.core.db import get_db
        total = 0
        for it in items:
            if it.get("no_bid"):
                continue
            price = it.get("unit_price") or it.get("price_per_unit") or it.get("pricing", {}).get("recommended_price") or 0
            qty = it.get("qty", 1) or 1
            total += float(price) * float(qty)

        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO revenue_log
                (logged_at, source, amount, category, description, po_number, agency)
                VALUES (?, ?, ?, 'quote_won', ?, ?, ?)
            """, (now, f"{record_type}_{record_id}", total,
                  f"Won {record_type.upper()} {record_id}", po_number,
                  record.get("institution") or record.get("agency", "")))
        result["revenue_logged"] = total
    except Exception as e:
        log.debug("mark_won revenue: %s", e)

    # Record to catalog
    try:
        from src.knowledge.pricing_intel import record_winning_prices
        # Build line_items with fields record_winning_prices expects
        line_items = []
        for it in items:
            if it.get("no_bid"):
                continue
            price = (it.get("unit_price") or it.get("price_per_unit")
                     or (it.get("pricing") or {}).get("recommended_price") or 0)
            cost = (it.get("vendor_cost") or it.get("cost")
                    or (it.get("pricing") or {}).get("unit_cost") or 0)
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
        record_winning_prices({
            "quote_number": record.get("reytech_quote_number", record_id),
            "po_number": po_number,
            "agency": record.get("institution") or record.get("agency", ""),
            "institution": record.get("institution", ""),
            "line_items": line_items,
        })
    except Exception as e:
        log.debug("mark_won catalog: %s", e)

    # V3: Calibrate Oracle from win outcome
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        calibrate_from_outcome(
            items, "won",
            agency=record.get("institution") or record.get("agency", "")
        )
    except Exception as e:
        log.debug("mark_won V3 calibration: %s", e)

    # CRM activity
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO activity_log (contact_id, event_type, event_detail, logged_at, metadata)
                VALUES (?, 'quote_won', ?, ?, ?)
            """, (record.get("requestor_email", ""), f"Won — PO {po_number}",
                  now, f'{{"record_type":"{record_type}","record_id":"{record_id}"}}'))
    except Exception as e:
        log.debug("mark_won CRM activity: %s", e)

    # Update recommendation_audit
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                UPDATE recommendation_audit SET outcome='won', updated_at=datetime('now')
                WHERE (pc_id=? OR quote_number=?) AND outcome='pending'
            """, (record_id, record.get("reytech_quote_number", "")))
    except Exception as e:
        log.debug("mark_won recommendation_audit: %s", e)

    # Stop award tracker monitoring
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO award_tracker_log
                (quote_number, checked_at, outcome, notes)
                VALUES (?, datetime('now'), 'won_manual', ?)
            """, (record.get("reytech_quote_number", record_id), notes or f"PO {po_number}"))
    except Exception as e:
        log.debug("mark_won award_tracker_log: %s", e)

    log.info("MARK_WON: %s %s — PO %s", record_type, record_id, po_number)
    return result


def mark_lost(record, record_type, record_id, competitor="", competitor_price=0, reason="", po_number=""):
    """Mark a PC or RFQ as lost. Handles all side effects."""
    now = datetime.now().isoformat()
    result = {"ok": True, "record_type": record_type, "record_id": record_id}

    record["status"] = "lost"
    record["outcome"] = "lost"
    record["outcome_date"] = now
    record["closed_at"] = now
    record["closed_reason"] = f"Lost to {competitor}" if competitor else reason or "Lost"

    # Log competitor intel
    if competitor:
        try:
            from src.core.db import get_db
            our_total = 0
            items = record.get("items", record.get("line_items", []))
            for it in items:
                if it.get("no_bid"):
                    continue
                price = it.get("unit_price") or it.get("price_per_unit") or 0
                qty = it.get("qty", 1) or 1
                our_total += float(price) * float(qty)

            delta = our_total - float(competitor_price) if competitor_price else 0
            delta_pct = round(delta / float(competitor_price) * 100, 1) if competitor_price else 0

            with get_db() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO competitor_intel
                    (found_at, quote_number, our_price, competitor_name, competitor_price,
                     price_delta, price_delta_pct, agency, institution, outcome, notes,
                     loss_reason_class)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (now, record.get("reytech_quote_number", record_id),
                      our_total, competitor, float(competitor_price) if competitor_price else 0,
                      delta, delta_pct,
                      record.get("institution") or record.get("agency", ""),
                      record.get("institution", ""),
                      "lost", reason or f"Lost to {competitor}",
                      "price_higher"))
            result["competitor_logged"] = True
        except Exception as e:
            log.debug("mark_lost competitor: %s", e)

    # V3: Calibrate Oracle from loss outcome
    try:
        from src.core.pricing_oracle_v2 import calibrate_from_outcome
        items = record.get("items", record.get("line_items", []))
        loss_type = "price" if (competitor_price and float(competitor_price) > 0) else "other"
        calibrate_from_outcome(
            items, "lost",
            agency=record.get("institution") or record.get("agency", ""),
            loss_reason=loss_type,
        )
    except Exception as e:
        log.debug("mark_lost V3 calibration: %s", e)

    # Update recommendation_audit
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                UPDATE recommendation_audit SET outcome='lost',
                    outcome_price=?, updated_at=datetime('now')
                WHERE (pc_id=? OR quote_number=?) AND outcome='pending'
            """, (float(competitor_price) if competitor_price else 0,
                  record_id, record.get("reytech_quote_number", "")))
    except Exception:
        pass

    # Generate action items from loss
    try:
        from src.agents.pricing_feedback import generate_action_items
        generate_action_items(
            {"loss_reason_class": "price_higher", "line_comparison": [], "margin_too_high_items": []},
            quote_number=record.get("reytech_quote_number", record_id),
            agency=record.get("institution") or record.get("agency", ""),
            institution=record.get("institution", ""),
        )
    except Exception:
        pass

    log.info("MARK_LOST: %s %s — competitor=%s price=%s", record_type, record_id, competitor, competitor_price)
    return result
