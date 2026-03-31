"""
Post-Send Pipeline
After a quote/PC is sent, schedule follow-ups and tracking.
"""
import logging
import json
from datetime import datetime, timedelta

log = logging.getLogger("reytech.post_send")


def _ensure_tables():
    """Create tracking tables if they don't exist."""
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_quote_tracker (
                id TEXT PRIMARY KEY,
                record_type TEXT DEFAULT 'rfq',
                solicitation TEXT DEFAULT '',
                institution TEXT DEFAULT '',
                requestor_email TEXT DEFAULT '',
                sent_at TEXT DEFAULT '',
                total_value REAL DEFAULT 0,
                item_count INTEGER DEFAULT 0,
                follow_up_schedule TEXT DEFAULT '[]',
                follow_up_status TEXT DEFAULT 'scheduled',
                status TEXT DEFAULT 'sent',
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
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
        except (ValueError, TypeError):
            pass

    now = datetime.now()

    follow_ups = [
        {"day": 3, "type": "gentle", "due": (now + timedelta(days=3)).isoformat()},
        {"day": 7, "type": "value_add", "due": (now + timedelta(days=7)).isoformat()},
        {"day": 14, "type": "final", "due": (now + timedelta(days=14)).isoformat()},
    ]

    try:
        with get_db() as db:
            db.execute("""
                INSERT OR REPLACE INTO sent_quote_tracker
                (id, record_type, solicitation, institution, requestor_email,
                 sent_at, total_value, item_count, follow_up_schedule,
                 follow_up_status, status)
                VALUES (?,?,?,?,?,datetime('now'),?,?,?,?,?)
            """, (record_id, record_type, sol, institution, email,
                  round(total, 2), len(items),
                  json.dumps(follow_ups, default=str),
                  "scheduled", "sent"))
        log.info("Post-send: %s %s tracked ($%.2f, %d items, follow-ups scheduled)",
                record_type, record_id, total, len(items))
    except Exception as e:
        log.warning("Post-send tracking: %s", e)

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
    """Get all sent quotes with follow-up status."""
    _ensure_tables()
    from src.core.db import get_db

    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT id, record_type, solicitation, institution,
                       requestor_email, sent_at, total_value, item_count,
                       follow_up_schedule, follow_up_status, status
                FROM sent_quote_tracker
                ORDER BY sent_at DESC LIMIT 50
            """).fetchall()

        results = []
        now = datetime.now()
        for r in rows:
            schedule = json.loads(r[8] or "[]")
            try:
                sent_dt = datetime.fromisoformat(r[5]) if r[5] else now
            except (ValueError, TypeError):
                sent_dt = now
            days_waiting = (now - sent_dt).days

            next_followup = None
            for fu in schedule:
                if fu.get("sent"):
                    continue
                next_followup = fu
                break

            urgency = "waiting"
            if days_waiting > 14:
                urgency = "overdue"
            elif next_followup:
                try:
                    fu_due = datetime.fromisoformat(next_followup["due"])
                    if fu_due <= now:
                        urgency = "follow_up_due"
                except (ValueError, TypeError, KeyError):
                    pass

            results.append({
                "id": r[0],
                "type": r[1],
                "solicitation": r[2],
                "institution": r[3],
                "email": r[4],
                "sent_at": r[5],
                "total_value": r[6],
                "item_count": r[7],
                "days_waiting": days_waiting,
                "status": r[10],
                "next_followup": next_followup,
                "urgency": urgency,
            })

        return results
    except Exception as e:
        log.warning("Sent quotes dashboard: %s", e)
        return []
