"""
Due Date Reminder Agent
Sends text reminders when PCs/RFQs are due within 24 hours.
Checks every hour. Only sends one reminder per item.
"""
import logging
import threading
import time
import json
import os
from datetime import datetime, timedelta

from src.core.paths import DATA_DIR

log = logging.getLogger("reytech.reminders")

REMINDED_FILE = os.path.join(DATA_DIR, "reminded_ids.json")


def _load_reminded():
    try:
        with open(REMINDED_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def _save_reminded(ids):
    try:
        os.makedirs(os.path.dirname(REMINDED_FILE), exist_ok=True)
        with open(REMINDED_FILE, "w") as f:
            json.dump(list(ids), f)
    except Exception:
        pass


def _parse_date(s):
    """Try to parse a date string."""
    if not s:
        return None
    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y",
                "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"]:
        try:
            return datetime.strptime(str(s).strip(), fmt)
        except (ValueError, TypeError):
            continue
    return None


def check_due_dates():
    """Check all PCs and RFQs for upcoming due dates."""
    reminded = _load_reminded()
    alerts = []

    now = datetime.now()
    tomorrow = now + timedelta(hours=24)

    # Check price_checks
    try:
        from src.core.db import get_db
        with get_db() as db:
            rows = db.execute("""
            SELECT id, pc_data, status, due_date FROM price_checks
            WHERE status NOT IN ('sent', 'won', 'lost', 'dismissed', 'expired')
            AND due_date IS NOT NULL AND due_date != ''
        """).fetchall()

        for row in rows:
            pc_id = str(row[0])
            if pc_id in reminded:
                continue

            due_str = row[3] or ""
            pc_data = json.loads(row[1] or "{}") if row[1] else {}
            if not due_str:
                due_str = pc_data.get("due_date", "")

            due_dt = _parse_date(due_str)
            if due_dt and now < due_dt <= tomorrow:
                hours_left = int((due_dt - now).total_seconds() / 3600)
                institution = pc_data.get("institution", "")
                pc_num = pc_data.get("pc_number", pc_id)

                alerts.append({
                    "type": "pc",
                    "id": pc_id,
                    "number": str(pc_num),
                    "institution": institution,
                    "due": due_str,
                    "hours_left": hours_left,
                    "message": f"PC #{pc_num} ({institution}) due in {hours_left}h - {due_str}",
                })
                reminded.add(pc_id)
    except Exception as e:
        log.warning("PC due date check: %s", e)

    # Check RFQs
    try:
        from src.api.dashboard import load_rfqs
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            if rid in reminded:
                continue
            if r.get("status") in ("sent", "won", "lost", "dismissed"):
                continue

            due_str = r.get("due_date", "")
            due_dt = _parse_date(due_str)
            if due_dt and now < due_dt <= tomorrow:
                hours_left = int((due_dt - now).total_seconds() / 3600)
                sol = r.get("solicitation_number", rid)

                alerts.append({
                    "type": "rfq",
                    "id": rid,
                    "number": str(sol),
                    "institution": r.get("institution", ""),
                    "due": due_str,
                    "hours_left": hours_left,
                    "message": f"RFQ #{sol} due in {hours_left}h - {due_str}",
                })
                reminded.add(rid)
    except Exception as e:
        log.warning("RFQ due date check: %s", e)

    # Send alerts
    for alert in alerts:
        # Bell notification
        try:
            from src.agents.notify_agent import send_alert
            send_alert("due_date_reminder", alert["message"],
                       f"{alert['type'].upper()} #{alert['number']} due {alert['due']}",
                       urgency="urgent",
                       context={"type": "due_date", "item_type": alert["type"],
                                "item_id": alert["id"]})
        except Exception:
            pass

        # SMS via Twilio (if configured)
        try:
            _send_sms_reminder(alert["message"])
        except Exception:
            pass

        log.info("DUE DATE ALERT: %s", alert["message"])

    _save_reminded(reminded)
    return alerts


def _send_sms_reminder(message):
    """Send SMS via Twilio if configured."""
    account_sid = os.environ.get("TWILIO_SID", "")
    auth_token = os.environ.get("TWILIO_TOKEN", "")
    from_number = os.environ.get("TWILIO_FROM", "")
    to_number = os.environ.get("TWILIO_TO", os.environ.get("OWNER_PHONE", ""))

    if not all([account_sid, auth_token, from_number, to_number]):
        log.debug("SMS not configured - skipping")
        return

    try:
        from twilio.rest import Client
        client = Client(account_sid, auth_token)
        sms = client.messages.create(
            body=f"Reytech RFQ: {message}",
            from_=from_number,
            to=to_number,
        )
        log.info("SMS sent: %s -> %s", sms.sid, to_number)
    except ImportError:
        log.debug("Twilio not installed")
    except Exception as e:
        log.warning("SMS failed: %s", e)


def start_reminder_scheduler():
    """Start hourly due date check."""
    def _loop():
        from src.core.scheduler import _shutdown_event
        _shutdown_event.wait(60)  # Wait 1 min after boot
        if _shutdown_event.is_set():
            log.info("Shutdown requested — due date reminders exiting before first cycle")
            return
        while not _shutdown_event.is_set():
            try:
                alerts = check_due_dates()
                if alerts:
                    log.info("Due date check: %d alerts sent", len(alerts))
            except Exception as e:
                log.warning("Reminder loop: %s", e)
            _shutdown_event.wait(3600)  # Wakes immediately on shutdown
        log.info("Shutdown requested — due date reminders exiting")

    t = threading.Thread(target=_loop, daemon=True, name="due-date-reminders")
    t.start()
    log.info("Due date reminder scheduler started (checks every 1h)")
