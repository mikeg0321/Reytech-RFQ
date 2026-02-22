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
        "I wanted to follow up on my previous email regarding medical supplies for {facility}. "
        "I know procurement timelines can be busy — just checking if you had a chance to review.\n\n"
        "Happy to answer any questions or send over a quote for specific items.\n\n"
        "Best regards,\nMichael Guadan\nReytech Inc.\n(949) 872-8676"
    ),
    "value_add": (
        "Hi {name},\n\n"
        "I wanted to share a quick update — we recently expanded our catalog with competitive pricing "
        "on several high-volume items that facilities like {facility} use regularly.\n\n"
        "Would it be helpful if I sent over a price comparison on your most-ordered items? "
        "Many of our state agency partners save 10-15% through our SB/DVBE pricing.\n\n"
        "Best regards,\nMichael Guadan\nReytech Inc.\n(949) 872-8676"
    ),
    "final": (
        "Hi {name},\n\n"
        "I'll keep this brief — I've reached out a couple of times about medical supply pricing "
        "for {facility}. I understand if the timing isn't right.\n\n"
        "If your needs change in the future, feel free to reach out anytime. "
        "I'll keep your facility on file for any new contract opportunities.\n\n"
        "Best regards,\nMichael Guadan\nReytech Inc.\n(949) 872-8676"
    ),
}


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {} if "state" in path or "activity" in path else []


def _save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, path)


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
            needs_follow_up.append({
                "source": "outbox",
                "original_id": email.get("id", ""),
                "to_email": email.get("to", ""),
                "to_name": email.get("to_name", "") or _parse_name_from_email(email.get("to", "")),
                "facility": email.get("facility", "") or email.get("subject", ""),
                "original_subject": email.get("subject", ""),
                "sent_date": sent_date,
                "days_since": days_since,
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


def create_follow_up_draft(item, follow_up_type):
    """Create a follow-up email draft in the outbox."""
    template = TEMPLATES.get(follow_up_type, TEMPLATES["gentle"])
    schedule = next((s for s in FOLLOW_UP_SCHEDULE if s["type"] == follow_up_type), FOLLOW_UP_SCHEDULE[0])

    body = template.format(
        name=item["to_name"],
        facility=item["facility"] or "your facility",
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
        time.sleep(30)  # Wait for app boot
        while True:
            try:
                result = run_follow_up_scan()
                if result["new_drafts"] > 0:
                    log.info("Follow-up scan: %d new drafts created", result["new_drafts"])
            except Exception as e:
                log.error("Follow-up scan error: %s", e)
            time.sleep(interval_seconds)

    t = threading.Thread(target=_loop, daemon=True, name="follow-up-engine")
    t.start()
    log.info("Follow-up engine started (scans every %ds)", interval_seconds)
