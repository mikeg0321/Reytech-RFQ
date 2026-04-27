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
    handled separately by `agents/award_tracker.py`.

    Phase 1.6 PR3h: also captures a labeled training pair (incoming
    blanks + outgoing fill + contract) for the per-buyer auto-trainer
    (PR3j, future) AND registers each attached PDF as a buyer-template
    candidate (PR3c). Both side-effects are best-effort — failure
    never blocks the send pipeline.
    """
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

    # Phase 1.6 PR3h: capture training pair + register attachments.
    # Belt + suspenders — helper has its own try/except, plus this outer
    # guard so even an unexpected raise can NEVER block the send return.
    try:
        capture_summary = _capture_training_artifacts(record_type, record_id)
    except Exception as _e:
        log.debug("training capture outer guard suppressed: %s", _e)
        capture_summary = {"pair_status": "error",
                            "candidates_registered": 0,
                            "candidates_matched_profile": 0}

    return {
        "tracked": True,
        "follow_ups": len(follow_ups),
        "total": total,
        "training_capture": capture_summary,
    }


def _capture_training_artifacts(record_type: str, record_id: str) -> dict:
    """Side-effect: build training pair + register buyer-template candidates.

    Lazy-imports modules so this file works even if PR3c/PR3g aren't yet
    deployed. Returns a small summary dict for telemetry.
    """
    summary = {
        "pair_status": "skipped",
        "candidates_registered": 0,
        "candidates_matched_profile": 0,
    }
    qt = (record_type or "").lower()
    if qt not in ("pc", "rfq"):
        return summary

    # 1) Build the training pair (PR3g)
    try:
        from src.agents.training_corpus import build_training_pair
        r = build_training_pair(record_id, qt)
        summary["pair_status"] = r.get("status", "error")
    except ImportError:
        log.debug("training_corpus not available — skipping pair capture")
    except Exception as e:
        log.debug("training pair capture suppressed: %s", e)

    # 2) Register attachments (PR3c)
    try:
        from src.agents.buyer_template_capture import register_attachment
        from src.agents.fill_plan_builder import (
            _list_attachments, _resolve_agency, _load_quote,
        )
        qd = _load_quote(record_id, qt)
        if qd:
            agency_key, _ = _resolve_agency(qd)
            for att in (_list_attachments(record_id, qt) or []):
                try:
                    r = register_attachment(record_id, qt, att,
                                            agency_key=agency_key)
                    if r.get("status") == "new_candidate" or \
                       r.get("status") == "existing_candidate":
                        summary["candidates_registered"] += 1
                    elif r.get("status") == "matched_profile":
                        summary["candidates_matched_profile"] += 1
                except Exception as _e:
                    log.debug("register_attachment suppressed: %s", _e)
    except ImportError:
        log.debug("buyer_template_capture not available — skipping reg")
    except Exception as e:
        log.debug("attachment registration suppressed: %s", e)

    return summary
