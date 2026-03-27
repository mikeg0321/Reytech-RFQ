"""
src/agents/email_lifecycle.py — Email Outbox Lifecycle (PRD-28 WI-2)

Manages the outbox beyond draft/send:
  1. Bulk approve/send/delete
  2. Failed email retry with exponential backoff
  3. Open/click tracking via pixel + link wrapping
  4. Engagement-driven follow-up triggers
  
Background scheduler retries failed emails every 15 minutes.
"""

import json
import logging
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone

log = logging.getLogger("email_lifecycle")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    from contextlib import contextmanager
    @contextmanager
    def get_db():
        conn = sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

# ── Config ────────────────────────────────────────────────────────────────────
MAX_RETRIES = 3
RETRY_INTERVALS = [900, 3600, 14400]  # 15min, 1hr, 4hr
RETRY_CHECK_INTERVAL = 900            # Check every 15 min
OPEN_FOLLOW_UP_DAYS = 3               # Draft follow-up if opened but no reply after 3 days
_scheduler_running = False


def _load_outbox_json() -> list:
    """Load outbox — DB first, JSON fallback."""
    try:
        from src.core.dal import get_outbox
        return get_outbox()
    except Exception:
        try:
            with open(os.path.join(DATA_DIR, "email_outbox.json")) as f:
                return json.load(f)
        except Exception:
            return []


def _save_outbox_json(outbox: list):
    """Save outbox to DB."""
    try:
        from src.core.dal import upsert_outbox_email
        for email in outbox:
            if email.get("id"):
                upsert_outbox_email(email)
    except Exception:
        with open(os.path.join(DATA_DIR, "email_outbox.json"), "w") as f:
            json.dump(outbox, f, indent=2, default=str)


# ── Bulk Actions ──────────────────────────────────────────────────────────────

def bulk_approve(email_ids: list = None) -> dict:
    """Approve multiple drafts at once. If no IDs, approve ALL drafts."""
    now = datetime.now(timezone.utc).isoformat()
    approved = 0

    # JSON file approach (current storage)
    outbox = _load_outbox_json()

    for email in outbox:
        if email.get("status") != "draft":
            continue
        if email_ids and email.get("id") not in email_ids:
            continue
        email["status"] = "approved"
        email["approved_at"] = now
        approved += 1

    _save_outbox_json(outbox)

    # Also update DB
    try:
        with get_db() as conn:
            if email_ids:
                placeholders = ",".join("?" for _ in email_ids)
                conn.execute(f"""
                    UPDATE email_outbox SET status = 'approved', approved_at = ?
                    WHERE id IN ({placeholders}) AND status = 'draft'
                """, [now] + list(email_ids))
            else:
                conn.execute("""
                    UPDATE email_outbox SET status = 'approved', approved_at = ?
                    WHERE status = 'draft'
                """, (now,))
    except Exception as e:
        log.warning("Bulk approve DB sync failed: %s", e)

    log.info("Bulk approved %d emails", approved)
    return {"ok": True, "approved": approved}


def bulk_delete(email_ids: list = None, status_filter: str = "draft") -> dict:
    """Delete multiple emails from outbox."""
    deleted = 0
    outbox = _load_outbox_json()

    original_count = len(outbox)
    if email_ids:
        outbox = [e for e in outbox if e.get("id") not in email_ids]
    else:
        outbox = [e for e in outbox if e.get("status") != status_filter]
    deleted = original_count - len(outbox)

    _save_outbox_json(outbox)

    try:
        with get_db() as conn:
            if email_ids:
                placeholders = ",".join("?" for _ in email_ids)
                conn.execute("DELETE FROM email_outbox WHERE id IN (" + placeholders + ")", list(email_ids))
            else:
                conn.execute("DELETE FROM email_outbox WHERE status = ?", (status_filter,))
    except Exception:
        pass

    log.info("Bulk deleted %d emails (filter=%s)", deleted, status_filter)
    return {"ok": True, "deleted": deleted}


# ── Failed Email Retry ────────────────────────────────────────────────────────

def mark_failed(email_id: str, error: str) -> dict:
    """Mark an email as failed and schedule retry."""
    now = datetime.now(timezone.utc)
    outbox = _load_outbox_json()

    for email in outbox:
        if email.get("id") == email_id:
            retry_count = email.get("retry_count", 0)
            email["last_error"] = error

            if retry_count >= MAX_RETRIES:
                email["status"] = "permanently_failed"
                log.warning("Email %s permanently failed after %d retries: %s", email_id, retry_count, error)
                _notify_permanent_failure(email)
            else:
                email["status"] = "failed"
                email["retry_count"] = retry_count + 1
                interval = RETRY_INTERVALS[min(retry_count, len(RETRY_INTERVALS) - 1)]
                email["retry_at"] = (now + timedelta(seconds=interval)).isoformat()
                log.info("Email %s retry %d scheduled in %ds", email_id, retry_count + 1, interval)
            break

    _save_outbox_json(outbox)

    return {"ok": True}


def retry_failed_emails() -> dict:
    """Check for failed emails ready to retry and re-queue them."""
    now = datetime.now(timezone.utc).isoformat()
    retried = 0

    outbox = _load_outbox_json()
    if not outbox and False:
        return {"ok": False, "error": "cannot load outbox"}

    for email in outbox:
        if email.get("status") != "failed":
            continue
        retry_at = email.get("retry_at", "")
        if retry_at and retry_at <= now:
            email["status"] = "approved"  # Re-queue for sending
            email.pop("retry_at", None)
            retried += 1
            log.info("Re-queued email %s for retry (attempt %d)", email.get("id"), email.get("retry_count", 0))

    if retried:
        _save_outbox_json(outbox)

    return {"ok": True, "retried": retried}


# ── Engagement Tracking ───────────────────────────────────────────────────────

def generate_tracking_id() -> str:
    """Generate a unique tracking ID for an email."""
    return f"trk-{uuid.uuid4().hex[:12]}"


def get_tracking_pixel_url(tracking_id: str) -> str:
    """Return the tracking pixel URL to embed in emails."""
    return f"/api/email/track/{tracking_id}/open"


def get_tracked_link(tracking_id: str, original_url: str) -> str:
    """Wrap a link for click tracking."""
    import urllib.parse
    encoded = urllib.parse.quote(original_url, safe="")
    return f"/api/email/track/{tracking_id}/click?url={encoded}"


def record_engagement(tracking_id: str, event_type: str,
                      ip_address: str = "", user_agent: str = "",
                      link_url: str = "") -> dict:
    """Record an open or click event."""
    now = datetime.now(timezone.utc).isoformat()

    # Find email by tracking_id
    email_id = None
    try:
        outbox = _load_outbox_json()
        for email in outbox:
            if email.get("tracking_id") == tracking_id:
                email_id = email.get("id")
                if event_type == "open":
                    email["open_count"] = email.get("open_count", 0) + 1
                    email["last_opened"] = now
                elif event_type == "click":
                    email["click_count"] = email.get("click_count", 0) + 1
                    email["last_clicked"] = now
                break
        _save_outbox_json(outbox)
    except Exception:
        pass

    # Log to DB
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO email_engagement (email_id, event_type, event_at, ip_address, user_agent, link_url)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (email_id or tracking_id, event_type, now, ip_address, user_agent, link_url))
    except Exception as e:
        log.warning("record_engagement DB: %s", e)

    log.info("Email %s: %s event from %s", tracking_id, event_type, ip_address)
    return {"ok": True}


def get_engagement_stats() -> dict:
    """Get overall email engagement statistics."""
    outbox = _load_outbox_json()

    sent = [e for e in outbox if e.get("status") in ("sent", "delivered")]
    opened = [e for e in sent if e.get("open_count", 0) > 0]
    clicked = [e for e in sent if e.get("click_count", 0) > 0]

    total_sent = len(sent) or 1  # avoid div/0

    return {
        "total_sent": len(sent),
        "total_opened": len(opened),
        "total_clicked": len(clicked),
        "open_rate": round(len(opened) / total_sent * 100, 1),
        "click_rate": round(len(clicked) / total_sent * 100, 1),
    }


def get_outbox_summary() -> dict:
    """Dashboard summary card data."""
    outbox = _load_outbox_json()

    from collections import Counter
    statuses = Counter(e.get("status", "?") for e in outbox)

    failed_retrying = sum(1 for e in outbox
                          if e.get("status") == "failed" and e.get("retry_at"))
    permanently_failed = statuses.get("permanently_failed", 0)

    engagement = get_engagement_stats()

    return {
        "drafts": statuses.get("draft", 0),
        "approved": statuses.get("approved", 0),
        "sent": statuses.get("sent", 0),
        "failed": statuses.get("failed", 0),
        "failed_retrying": failed_retrying,
        "permanently_failed": permanently_failed,
        "total": len(outbox),
        "open_rate": engagement.get("open_rate", 0),
        "click_rate": engagement.get("click_rate", 0),
    }


# ── Background Scheduler ─────────────────────────────────────────────────────

def _run_retry_check():
    """Periodic retry check."""
    global _scheduler_running
    if not _scheduler_running:
        return
    try:
        result = retry_failed_emails()
        if result.get("retried", 0) > 0:
            log.info("Retry check: re-queued %d emails", result["retried"])
    except Exception as e:
        log.error("Retry scheduler: %s", e)
    finally:
        if _scheduler_running:
            threading.Timer(RETRY_CHECK_INTERVAL, _run_retry_check).start()


def start_retry_scheduler():
    """Start background retry scheduler."""
    global _scheduler_running
    if _scheduler_running:
        return
    _scheduler_running = True
    threading.Timer(120, _run_retry_check).start()
    log.info("Email retry scheduler started (checks every %ds)", RETRY_CHECK_INTERVAL)


def _notify_permanent_failure(email: dict):
    """Notify Mike about permanently failed email."""
    try:
        from src.agents.notify_agent import send_alert
        send_alert(
            event_type="email_permanent_failure",
            title=f"Email failed permanently: {email.get('subject', '?')[:50]}",
            body=f"To: {email.get('to_address', '?')} — Error: {email.get('last_error', '?')[:100]}",
            urgency="warning",
            deep_link="/outbox"
        )
    except Exception:
        pass


# ── Agent Status ──────────────────────────────────────────────────────────────

def get_agent_status() -> dict:
    summary = get_outbox_summary()
    return {
        "name": "email_lifecycle",
        "status": "ok",
        "scheduler_running": _scheduler_running,
        **summary,
    }
