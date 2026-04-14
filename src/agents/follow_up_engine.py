"""
Follow-Up Automation Engine
============================
Auto-creates follow-up email drafts when outreach goes unanswered.

Scan sources:
  1. email_outbox.json — sent emails without replies
  2. growth_outreach.json — campaign outreach without response
  3. crm_activity.json — quote sends without PO

Schedule: Every hour, checks all sources. Creates draft follow-ups at:
  - Day 3: Gentle check-in
  - Day 7: Value-add follow-up
  - Day 14: Final attempt

Drafts go to email_outbox.json with status="follow_up_draft" for review.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta

log = logging.getLogger("follow_up")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

OUTBOX_FILE = os.path.join(DATA_DIR, "email_outbox.json")
GROWTH_FILE = os.path.join(DATA_DIR, "growth_outreach.json")
ACTIVITY_FILE = os.path.join(DATA_DIR, "crm_activity.json")
FOLLOWUP_STATE_FILE = os.path.join(DATA_DIR, "follow_up_state.json")

# Follow-up intervals (business days → calendar days approx)
FOLLOW_UP_SCHEDULE = [
    {"day": 3, "type": "gentle", "subject_prefix": "Following up: "},
    {"day": 7, "type": "value_add", "subject_prefix": "Quick update: "},
    {"day": 14, "type": "final", "subject_prefix": "Final check-in: "},
]

TEMPLATES = {
    "gentle": (
        "Hi {name},\n\n"
        "I wanted to follow up on the pricing I sent for {pc_number} on {sent_date}. "
        "The quote covers {item_count} items totaling ${total:.2f}.\n\n"
        "Please let me know if you have any questions or need any adjustments to the pricing.\n\n"
        "Best regards,\n"
        "Michael Guadan\n"
        "Reytech Inc.\n"
        "(619) 985-8610"
    ),
    "value_add": (
        "Hi {name},\n\n"
        "Just checking in on our quote {pc_number} from {sent_date}. "
        "I know procurement timelines can be tight.\n\n"
        "If any of the items need different specifications or quantities, "
        "I'm happy to revise the quote. We also carry related products that "
        "may be useful for your facility.\n\n"
        "Let me know how I can help.\n\n"
        "Best regards,\n"
        "Michael Guadan\n"
        "Reytech Inc.\n"
        "(619) 985-8610"
    ),
    "final": (
        "Hi {name},\n\n"
        "I wanted to reach out one last time regarding quote {pc_number}. "
        "Our pricing is valid for 45 days from {sent_date}.\n\n"
        "If the timing isn't right, no worries — I'll keep your requirements on file "
        "and can provide updated pricing whenever you're ready.\n\n"
        "Thank you for considering Reytech.\n\n"
        "Best regards,\n"
        "Michael Guadan\n"
        "Reytech Inc.\n"
        "(619) 985-8610"
    ),
}


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {} if "state" in path or "activity" in path else []


def _save_json(path, data):
    from src.core.data_guard import atomic_json_save
    atomic_json_save(path, data)


def _load_state():
    """Track which follow-ups we've already created to avoid duplicates."""
    return _load_json(FOLLOWUP_STATE_FILE)


def _save_state(state):
    _save_json(FOLLOWUP_STATE_FILE, state)


def _parse_name_from_email(email):
    """Extract likely first name from email address."""
    if not email:
        return "there"
    local = email.split("@")[0]
    parts = local.replace(".", " ").replace("_", " ").replace("-", " ").split()
    if parts:
        return parts[0].capitalize()
    return "there"


def scan_outbox_for_follow_ups():
    """Scan email_outbox.json for sent emails needing follow-up."""
    outbox = _load_json(OUTBOX_FILE)
    if not isinstance(outbox, list):
        outbox = outbox.get("emails", []) if isinstance(outbox, dict) else []

    needs_follow_up = []
    now = datetime.now()

    for email in outbox:
        # Only check sent emails (not drafts, not already follow-ups)
        status = email.get("status", "")
        if status not in ("sent", "approved"):
            continue
        if email.get("is_follow_up"):
            continue

        sent_date_str = email.get("sent_at") or email.get("created") or ""
        try:
            sent_date = datetime.fromisoformat(sent_date_str.replace("Z", "+00:00").replace("+00:00", ""))
        except (ValueError, AttributeError):
            continue

        days_since = (now - sent_date).days
        has_response = email.get("response_received", False)

        if not has_response and days_since >= 3:
            # ── Engagement-aware prioritization ──────────────────────
            opened = email.get("open_count", 0) > 0
            clicked = email.get("click_count", 0) > 0
            # Opened but no reply = hot lead, follow up sooner
            # Not opened after 7d = might need different approach
            if clicked:
                urgency = "hot"       # clicked a link — very engaged
            elif opened and days_since >= 3:
                urgency = "warm"      # opened but didn't reply
            elif not opened and days_since >= 7:
                urgency = "cold"      # never opened — try different subject
            else:
                urgency = "normal"
            # ── End engagement check ─────────────────────────────────

            needs_follow_up.append({
                "source": "outbox",
                "original_id": email.get("id", ""),
                "to_email": email.get("to", ""),
                "to_name": email.get("to_name", "") or _parse_name_from_email(email.get("to", "")),
                "facility": email.get("facility", "") or email.get("subject", ""),
                "original_subject": email.get("subject", ""),
                "email_message_id": email.get("message_id", "") or email.get("email_message_id", ""),
                "sent_date": sent_date,
                "days_since": days_since,
                "urgency": urgency,
                "opened": opened,
                "clicked": clicked,
                "open_count": email.get("open_count", 0),
                "click_count": email.get("click_count", 0),
            })

    return needs_follow_up


def scan_growth_for_follow_ups():
    """Scan growth_outreach.json for campaigns needing follow-up."""
    data = _load_json(GROWTH_FILE)
    if not isinstance(data, dict):
        return []

    needs_follow_up = []
    now = datetime.now()

    for campaign in data.get("campaigns", []):
        if campaign.get("dry_run"):
            continue
        for outreach in campaign.get("outreach", []):
            if not outreach.get("email_sent"):
                continue
            if outreach.get("response_received") or outreach.get("bounced"):
                continue

            sent_str = outreach.get("email_sent_at", "")
            try:
                sent_date = datetime.fromisoformat(sent_str.replace("Z", "+00:00").replace("+00:00", ""))
            except (ValueError, AttributeError):
                continue

            days_since = (now - sent_date).days
            if days_since >= 3:
                needs_follow_up.append({
                    "source": "growth",
                    "original_id": outreach.get("prospect_id", ""),
                    "to_email": outreach.get("email", ""),
                    "to_name": outreach.get("buyer_name", "") or _parse_name_from_email(outreach.get("email", "")),
                    "facility": outreach.get("agency", ""),
                    "original_subject": f"Reytech Inc — Medical Supply Partner for {outreach.get('agency', '')}",
                    "sent_date": sent_date,
                    "days_since": days_since,
                })

    return needs_follow_up


def scan_quotes_for_follow_ups():
    """Scan for sent quotes without PO response."""
    activity = _load_json(ACTIVITY_FILE)
    if not isinstance(activity, list):
        activity = activity.get("activity", []) if isinstance(activity, dict) else []

    needs_follow_up = []
    now = datetime.now()

    for act in activity:
        if act.get("type") not in ("quote_sent", "email_sent"):
            continue
        if act.get("response_received") or act.get("po_received"):
            continue

        sent_str = act.get("date", "") or act.get("created", "")
        try:
            sent_date = datetime.fromisoformat(sent_str.replace("Z", "+00:00").replace("+00:00", ""))
        except (ValueError, AttributeError):
            continue

        days_since = (now - sent_date).days
        if days_since >= 3 and act.get("contact_email"):
            needs_follow_up.append({
                "source": "quote",
                "original_id": act.get("quote_number", act.get("id", "")),
                "to_email": act.get("contact_email", ""),
                "to_name": act.get("contact_name", "") or _parse_name_from_email(act.get("contact_email", "")),
                "facility": act.get("customer", ""),
                "original_subject": f"Quote {act.get('quote_number', '')} for {act.get('customer', '')}",
                "sent_date": sent_date,
                "days_since": days_since,
            })

    return needs_follow_up


def _get_pc_details(pc_id):
    """Load PC details for follow-up template variables."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT pc_number, sent_at, email_message_id, requestor_name, total "
                "FROM price_checks WHERE id = ?", (pc_id,)
            ).fetchone()
            if row:
                pc = dict(row)
                # Count items
                items = conn.execute(
                    "SELECT COUNT(*) FROM pc_items WHERE pc_id = ?", (pc_id,)
                ).fetchone()[0]
                pc["item_count"] = items
                return pc
    except Exception as e:
        log.debug("_get_pc_details(%s): %s", pc_id, e)
    return {}


def create_follow_up_draft(item, follow_up_type):
    """Create a follow-up email draft in the outbox."""
    template = TEMPLATES.get(follow_up_type, TEMPLATES["gentle"])
    schedule = next((s for s in FOLLOW_UP_SCHEDULE if s["type"] == follow_up_type), FOLLOW_UP_SCHEDULE[0])

    # Gather PC-specific details for improved templates
    pc_details = {}
    if item.get("source") in ("outbox", "quote"):
        pc_details = _get_pc_details(item.get("original_id", ""))

    pc_number = pc_details.get("pc_number") or item.get("original_id", "")
    sent_date_obj = item.get("sent_date")
    sent_date_str = sent_date_obj.strftime("%B %d") if isinstance(sent_date_obj, datetime) else str(sent_date_obj)[:10]
    item_count = pc_details.get("item_count", 0)
    total = pc_details.get("total") or 0.0

    body = template.format(
        name=item["to_name"],
        facility=item["facility"] or "your facility",
        pc_number=pc_number,
        sent_date=sent_date_str,
        item_count=item_count,
        total=float(total),
    )

    draft = {
        "id": f"fu_{item['source']}_{item['original_id']}_{follow_up_type}_{datetime.now().strftime('%Y%m%d')}",
        "to": item["to_email"],
        "to_name": item["to_name"],
        "facility": item["facility"],
        "subject": schedule["subject_prefix"] + item["original_subject"],
        "body": body,
        "status": "follow_up_draft",
        "is_follow_up": True,
        "follow_up_type": follow_up_type,
        "follow_up_day": schedule["day"],
        "original_id": item["original_id"],
        "original_source": item["source"],
        "created": datetime.now().isoformat(),
        "days_since_original": item["days_since"],
    }

    # In-Reply-To threading — keeps follow-ups in same email thread
    email_message_id = pc_details.get("email_message_id") or item.get("email_message_id")
    if email_message_id:
        draft["in_reply_to"] = email_message_id
        draft["references"] = email_message_id

    return draft


def run_follow_up_scan():
    """Main scan: find all items needing follow-up, create drafts where needed."""
    state = _load_state()
    if not isinstance(state, dict):
        state = {"created_follow_ups": {}}
    created = state.get("created_follow_ups", {})

    # Gather all items needing follow-up
    all_items = []
    all_items.extend(scan_outbox_for_follow_ups())
    all_items.extend(scan_growth_for_follow_ups())
    all_items.extend(scan_quotes_for_follow_ups())

    new_drafts = []
    for item in all_items:
        key = f"{item['source']}_{item['original_id']}"

        # Determine which follow-up stage to create
        for schedule in FOLLOW_UP_SCHEDULE:
            if item["days_since"] >= schedule["day"]:
                fu_key = f"{key}_{schedule['type']}"
                if fu_key not in created:
                    draft = create_follow_up_draft(item, schedule["type"])
                    new_drafts.append(draft)
                    created[fu_key] = {
                        "created_at": datetime.now().isoformat(),
                        "type": schedule["type"],
                        "day": schedule["day"],
                    }

    # Save new drafts to outbox
    if new_drafts:
        outbox = _load_json(OUTBOX_FILE)
        if isinstance(outbox, dict):
            outbox = outbox.get("emails", [])
        if not isinstance(outbox, list):
            outbox = []

        outbox.extend(new_drafts)
        _save_json(OUTBOX_FILE, outbox)
        log.info("Follow-up engine: created %d new drafts", len(new_drafts))

    # Save state
    state["created_follow_ups"] = created
    state["last_scan"] = datetime.now().isoformat()
    state["last_scan_items"] = len(all_items)
    state["last_scan_new_drafts"] = len(new_drafts)
    _save_state(state)

    return {
        "ok": True,
        "scanned": len(all_items),
        "new_drafts": len(new_drafts),
        "total_tracked": len(created),
    }


def get_follow_up_summary():
    """Get summary for daily brief."""
    state = _load_state()
    all_items = []
    all_items.extend(scan_outbox_for_follow_ups())
    all_items.extend(scan_growth_for_follow_ups())
    all_items.extend(scan_quotes_for_follow_ups())

    created = state.get("created_follow_ups", {}) if isinstance(state, dict) else {}

    # Count pending (have draft) vs overdue (no draft yet)
    pending = []
    overdue = []
    for item in all_items:
        key = f"{item['source']}_{item['original_id']}"
        has_any_follow_up = any(k.startswith(key) for k in created)
        if has_any_follow_up:
            pending.append(item)
        elif item["days_since"] >= 7:
            overdue.append(item)

    return {
        "total_awaiting_response": len(all_items),
        "follow_ups_sent": len([k for k, v in created.items() if v]),
        "overdue": len(overdue),
        "overdue_items": overdue[:5],  # Top 5 for brief
        "pending_items": pending[:5],
        "last_scan": state.get("last_scan", "never") if isinstance(state, dict) else "never",
    }


# ── Background Scheduler ──────────────────────────────────────────────────────

_scheduler_started = False


def start_follow_up_scheduler(interval_seconds=3600):
    """Start background thread that scans for follow-ups every hour."""
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    def _loop():
        from src.core.scheduler import _shutdown_event
        _shutdown_event.wait(30)  # Wait for app boot
        if _shutdown_event.is_set():
            log.info("Shutdown requested — follow-up engine exiting before first cycle")
            return
        while not _shutdown_event.is_set():
            try:
                result = run_follow_up_scan()
                if result["new_drafts"] > 0:
                    log.info("Follow-up scan: %d new drafts created", result["new_drafts"])
                try:
                    from src.core.scheduler import heartbeat
                    heartbeat("follow-up-engine", success=True)
                except Exception as _e:
                    log.debug('suppressed in _loop: %s', _e)
            except Exception as e:
                log.error("Follow-up scan error: %s", e)
                try:
                    from src.core.scheduler import heartbeat
                    heartbeat("follow-up-engine", success=False, error=str(e)[:200])
                except Exception as _e:
                    log.debug('suppressed in _loop: %s', _e)
            _shutdown_event.wait(interval_seconds)  # Wakes immediately on shutdown
        log.info("Shutdown requested — follow-up engine exiting")

    t = threading.Thread(target=_loop, daemon=True, name="follow-up-engine")
    t.start()
    log.info("Follow-up engine started (scans every %ds)", interval_seconds)
