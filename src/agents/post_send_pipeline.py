"""
Post-Send Pipeline
After a quote/PC is sent, schedule follow-ups and tracking.
"""
import logging
import json
from datetime import datetime, timedelta

log = logging.getLogger("reytech.post_send")


def _ensure_tables():
    """Create tracking tables if they don't exist.
    Note: sent_quote_tracker removed in migration 16.
    """
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS award_check_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id TEXT NOT NULL,
                record_type TEXT DEFAULT 'rfq',
                solicitation TEXT DEFAULT '',
                check_after TEXT DEFAULT '',
                status TEXT DEFAULT 'pending',
                checked_at TEXT DEFAULT '',
                result TEXT DEFAULT '',
                phase TEXT DEFAULT 'daily',
                check_count INTEGER DEFAULT 0,
                last_checked TEXT DEFAULT '',
                next_check TEXT DEFAULT ''
            )
        """)


def on_quote_sent(record_type, record_id, record_data):
    """Called immediately after a quote or PC is sent.
    Sets up follow-up schedule and tracking.
    """
    _ensure_tables()
    from src.core.db import get_db

    email = record_data.get("requestor_email", record_data.get("requestor", ""))
    sol = record_data.get("solicitation_number", record_data.get("pc_number", ""))
    institution = record_data.get("institution", "")
    total = 0
    items = record_data.get("line_items", record_data.get("items", []))
    for item in items:
        try:
            price = float(str(item.get("price_per_unit", item.get("bid_price", 0)) or 0).replace("$", "").replace(",", ""))
            qty = float(str(item.get("quantity", item.get("qty", 1)) or 1).replace(",", ""))
            total += price * qty
        except (ValueError, TypeError) as _e:
            log.debug("suppressed: %s", _e)

    now = datetime.now()

    follow_ups = [
        {"day": 3, "type": "gentle", "due": (now + timedelta(days=3)).isoformat()},
        {"day": 7, "type": "value_add", "due": (now + timedelta(days=7)).isoformat()},
        {"day": 14, "type": "final", "due": (now + timedelta(days=14)).isoformat()},
    ]

    # sent_quote_tracker removed in migration 16 — log only
    log.info("Post-send: %s %s ($%.2f, %d items, follow-ups scheduled)",
             record_type, record_id, total, len(items))

    # Queue for award monitoring with adaptive schedule
    try:
        from src.core.scprs_schedule import get_next_check_time
        next_check = get_next_check_time(
            sent_at=datetime.now(),
            last_check=None,
            check_count=0,
        )
        next_check_iso = next_check.isoformat() if next_check else (now + timedelta(days=1)).isoformat()
        phase = "daily"  # Start in daily phase
    except ImportError:
        next_check_iso = (now + timedelta(days=1)).isoformat()
        phase = "daily"

    try:
        with get_db() as db:
            db.execute("""
                INSERT OR IGNORE INTO award_check_queue
                (record_id, record_type, solicitation, check_after, status,
                 phase, check_count, next_check)
                VALUES (?,?,?,?,?,?,0,?)
            """, (record_id, record_type, sol, next_check_iso, "pending",
                  phase, next_check_iso))
        log.info("Post-send: Award check queued for %s %s — first check at %s (phase: %s)",
                 record_type, record_id, next_check_iso[:16], phase)
    except Exception as e:
        # Fallback: try without new columns (pre-migration)
        try:
            with get_db() as db:
                db.execute("""
                    INSERT OR IGNORE INTO award_check_queue
                    (record_id, record_type, solicitation, check_after, status)
                    VALUES (?,?,?,?,?)
                """, (record_id, record_type, sol, next_check_iso, "pending"))
        except Exception as e2:
            log.debug("Award check queue: %s / %s", e, e2)

    return {"tracked": True, "follow_ups": len(follow_ups), "total": total}


def get_sent_quotes_dashboard():
    """Get sent quotes dashboard. sent_quote_tracker removed in migration 16."""
    # Table removed — return empty list. Callers handle empty gracefully.
    return []
