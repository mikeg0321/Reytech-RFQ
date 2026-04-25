"""
Post-Send Pipeline
After a quote/PC is sent, log the send and return a follow-up summary.

History: this module previously also wrote `award_check_queue` rows for an
adaptive award-checking consumer that was never built — `award_tracker.py`
shipped a different design that iterates `quotes`/`rfqs` directly and applies
`scprs_schedule.should_check_record` per-row at read-time. The queue write was
removed as part of the connectivity audit (DATA_ARCHITECTURE_MAP §7 silo S7);
the table is dropped in migration 29. A sibling `sent_quote_tracker` table
was already removed in migration 16, and its dead `get_sent_quotes_dashboard`
helper + the route that called it were removed in the same audit.
"""
import logging
from datetime import datetime, timedelta

log = logging.getLogger("reytech.post_send")


def on_quote_sent(record_type, record_id, record_data):
    """Called immediately after a quote or PC is sent. Logs the send and
    returns a summary used by callers for telemetry. Award-checking is
    handled separately by `agents/award_tracker.py`."""
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

    log.info("Post-send: %s %s ($%.2f, %d items, follow-ups scheduled)",
             record_type, record_id, total, len(items))

    return {"tracked": True, "follow_ups": len(follow_ups), "total": total}
