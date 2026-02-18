"""
notify_agent.py — Proactive Alert & Notification System for Reytech
Phase 28 | Version: 1.0.0

CHANNELS (in priority order):
  1. SMS — Twilio to NOTIFY_PHONE (works with Google Voice numbers too)
  2. Email — Gmail SMTP to NOTIFY_EMAIL (separate from sales@reytechinc.com)
  3. Dashboard bell — Persistent SQLite notifications table

SETUP (Railway env vars):
  NOTIFY_PHONE  = +16195551234   ← Your personal cell or Google Voice number
  NOTIFY_EMAIL  = you@gmail.com  ← Personal email (different from GMAIL_ADDRESS)
  NOTIFY_SMS    = true           ← Enable SMS (default: true if NOTIFY_PHONE set)
  NOTIFY_EMAIL_ALERTS = true     ← Enable email alerts (default: true)
  ALERT_COOLDOWN_MIN = 15        ← Minutes between duplicate alerts (default: 15)

TRIGGER MAP (what events fire what channels):
  ┌─────────────────────────────┬─────┬───────┬──────────────┐
  │ Event                       │ SMS │ Email │ Bell         │
  ├─────────────────────────────┼─────┼───────┼──────────────┤
  │ cs_draft_ready              │  ✅  │  ✅   │  ✅ urgent   │
  │ rfq_arrived                 │  ✅  │  ✅   │  ✅ urgent   │
  │ quote_won                   │  ✅  │  ✅   │  ✅ deal     │
  │ auto_draft_ready            │  ✅  │  ✅   │  ✅ draft    │
  │ outbox_stale                │  —   │  ✅   │  ✅ warning  │
  │ scprs_pull_done             │  —   │  —    │  ✅ info     │
  │ voice_call_placed           │  —   │  —    │  ✅ info     │
  │ quote_lost_signal           │  —   │  ✅   │  ✅ warning  │
  │ cs_call_placed              │  —   │  —    │  ✅ info     │
  └─────────────────────────────┴─────┴───────┴──────────────┘

EMAIL COMMUNICATION LOG:
  Every email sent/received is logged to SQLite email_log table.
  CS agent queries this for dispute resolution:
    "We sent you a quote on Feb 15 at 2:43pm (Subject: Re: RFQ #704B)"
  Captured: direction, sender, recipient, subject, body_preview, 
            associated entities (quote_number, po_number, rfq_id)
"""

import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("notify")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# ── Configuration from Railway env ─────────────────────────────────────────
NOTIFY_PHONE   = os.environ.get("NOTIFY_PHONE", "")      # +16195551234 or GV number
NOTIFY_EMAIL   = os.environ.get("NOTIFY_EMAIL", "")      # your personal email
GMAIL_ADDRESS  = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_PASSWORD", "")
TWILIO_SID     = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN   = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM    = os.environ.get("TWILIO_PHONE_NUMBER", "")

SMS_ENABLED    = os.environ.get("NOTIFY_SMS", "true").lower() not in ("false","0","off")
EMAIL_ENABLED  = os.environ.get("NOTIFY_EMAIL_ALERTS", "true").lower() not in ("false","0","off")
COOLDOWN_MIN   = int(os.environ.get("ALERT_COOLDOWN_MIN", "15"))

# ── Cooldown tracker (in-memory, per event+entity key) ─────────────────────
_cooldown: dict[str, float] = {}
_cooldown_lock = threading.Lock()

def _is_cooled_down(key: str) -> bool:
    """Return True if this alert key is past its cooldown period."""
    with _cooldown_lock:
        last = _cooldown.get(key, 0)
        now = time.time()
        if now - last >= COOLDOWN_MIN * 60:
            _cooldown[key] = now
            return True
        return False


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def send_alert(
    event_type: str,
    title: str,
    body: str,
    urgency: str = "info",          # urgent | warning | deal | draft | info
    context: dict = None,           # {quote_number, po_number, rfq_id, contact, entity_id}
    channels: list = None,          # override: ["sms","email","bell"] — None = auto from map
    cooldown_key: str = None,       # key for dedup — defaults to event_type
    run_async: bool = True,         # fire in background thread (default True)
) -> dict:
    """
    Central notification dispatcher. Call this from anywhere in the app.

    Example:
        send_alert(
            event_type="cs_draft_ready",
            title="📬 CS Draft Ready",
            body="Customer asked about PO #12345 — draft reply waiting for review",
            urgency="urgent",
            context={"po_number": "12345", "contact": "John Smith <purchasing@cdcr.ca.gov>"},
        )
    """
    context = context or {}
    dedup_key = cooldown_key or f"{event_type}:{context.get('entity_id','')}"

    if not _is_cooled_down(dedup_key):
        log.debug("Alert suppressed (cooldown): %s", dedup_key)
        return {"ok": False, "reason": "cooldown"}

    if run_async:
        t = threading.Thread(
            target=_dispatch_alert,
            args=(event_type, title, body, urgency, context, channels),
            daemon=True,
            name=f"alert-{event_type[:16]}",
        )
        t.start()
        return {"ok": True, "async": True}

    return _dispatch_alert(event_type, title, body, urgency, context, channels)


def _log_alert(event_type, title, body, urgency, context, channels, results):
    """Persist alert firing to SQLite for audit trail."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                UPDATE notifications SET
                    sms_sent=?,
                    email_sent=?
                WHERE event_type=? AND is_read=0
                ORDER BY created_at DESC LIMIT 1
            """, (
                1 if results.get("sms",{}).get("ok") else 0,
                1 if results.get("email",{}).get("ok") else 0,
                event_type,
            ))
    except Exception:
        pass


def _dispatch_alert(event_type, title, body, urgency, context, channels_override):
    """Actually send the alert across all appropriate channels."""
    # Determine channels
    CHANNEL_MAP = {
        "cs_draft_ready":   ["sms", "email", "bell"],
        "rfq_arrived":      ["sms", "email", "bell"],
        "quote_won":        ["sms", "email", "bell"],
        "auto_draft_ready": ["sms", "email", "bell"],
        "outbox_stale":     ["email", "bell"],
        "scprs_pull_done":  ["bell"],
        "voice_call_placed":["bell"],
        "quote_lost_signal":["email", "bell"],
        "cs_call_placed":   ["bell"],
        "invoice_unpaid":   ["email", "bell"],
        "delivery_confirmed":["bell"],
    }
    channels = channels_override or CHANNEL_MAP.get(event_type, ["bell"])

    results = {}

    if "sms" in channels and SMS_ENABLED and NOTIFY_PHONE:
        results["sms"] = _send_sms(title, body, context)
    
    if "email" in channels and EMAIL_ENABLED and NOTIFY_EMAIL:
        results["email"] = _send_alert_email(event_type, title, body, context)

    if "bell" in channels:
        results["bell"] = _push_bell(event_type, title, body, urgency, context)

    # Log to DB
    _log_alert(event_type, title, body, urgency, context, channels, results)

    log.info("Alert dispatched: %s | channels=%s | results=%s",
             event_type, channels, {k: v.get("ok") for k, v in results.items()})
    return {"ok": True, "results": results}


# ══════════════════════════════════════════════════════════════════════════════
# SMS (Twilio — works with Google Voice numbers as destination)
# ══════════════════════════════════════════════════════════════════════════════

def _send_sms(title: str, body: str, context: dict) -> dict:
    """Send SMS via Twilio to NOTIFY_PHONE (Google Voice compatible)."""
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, NOTIFY_PHONE]):
        return {"ok": False, "reason": "Twilio not configured — set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, NOTIFY_PHONE in Railway"}
    
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        
        # Keep SMS tight — 160 char limit ideal
        sms_body = f"🔔 REYTECH: {title}\n{body[:140]}"
        if context.get("quote_number"):
            sms_body += f"\nQuote: {context['quote_number']}"
        if context.get("po_number"):
            sms_body += f"\nPO: {context['po_number']}"
        
        msg = client.messages.create(
            body=sms_body[:1600],
            from_=TWILIO_FROM,
            to=NOTIFY_PHONE,
        )
        log.info("SMS sent: %s → %s (SID: %s)", title[:40], NOTIFY_PHONE, msg.sid)
        return {"ok": True, "sid": msg.sid}
    except Exception as e:
        log.warning("SMS failed: %s", e)
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL ALERT (Gmail SMTP → NOTIFY_EMAIL personal address)
# ══════════════════════════════════════════════════════════════════════════════

def _send_alert_email(event_type: str, title: str, body: str, context: dict) -> dict:
    """Send alert email via Gmail to Mike's personal address."""
    if not all([GMAIL_ADDRESS, GMAIL_PASSWORD, NOTIFY_EMAIL]):
        return {"ok": False, "reason": "Gmail or NOTIFY_EMAIL not configured"}
    
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        URGENCY_SUBJECT_PREFIX = {
            "cs_draft_ready":    "📬 [ACTION] CS Draft Ready",
            "rfq_arrived":       "🚨 [URGENT] New RFQ Arrived",
            "quote_won":         "💰 [WIN] Quote Won",
            "auto_draft_ready":  "📋 [REVIEW] Draft Ready",
            "outbox_stale":      "⏰ [REMINDER] Drafts Waiting",
            "quote_lost_signal": "📉 [FYI] Quote Lost Signal",
            "invoice_unpaid":    "💸 [FOLLOW-UP] Invoice Unpaid",
        }
        subject = URGENCY_SUBJECT_PREFIX.get(event_type, f"🔔 Reytech: {title}")

        # Build HTML email
        ctx_lines = ""
        if context.get("quote_number"): ctx_lines += f"<tr><td>Quote</td><td><b>{context['quote_number']}</b></td></tr>"
        if context.get("po_number"):    ctx_lines += f"<tr><td>PO</td><td><b>{context['po_number']}</b></td></tr>"
        if context.get("contact"):      ctx_lines += f"<tr><td>Contact</td><td>{context['contact']}</td></tr>"
        if context.get("agency"):       ctx_lines += f"<tr><td>Agency</td><td>{context['agency']}</td></tr>"
        if context.get("amount"):       ctx_lines += f"<tr><td>Amount</td><td><b>${context['amount']:,.2f}</b></td></tr>"
        if context.get("intent"):       ctx_lines += f"<tr><td>Intent</td><td>{context['intent']}</td></tr>"
        
        action_url = "https://reytechdash.railway.app"  # production dashboard
        action_label = "Open Dashboard →"
        if event_type in ("cs_draft_ready", "auto_draft_ready"):
            action_url += "/outbox"
            action_label = "Review Drafts →"
        elif event_type == "quote_won":
            action_url += "/quotes"
            action_label = "View Quote →"
        elif event_type == "rfq_arrived":
            action_label = "View RFQ →"

        html = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;background:#0f1117;color:#e4e6ed;padding:24px">
<div style="max-width:520px;margin:0 auto">
  <div style="background:#1a1d27;border:1px solid #2e3345;border-radius:10px;padding:24px">
    <h2 style="color:#4f8cff;margin:0 0 8px">{title}</h2>
    <p style="color:#8b90a0;font-size:12px;margin:0 0 16px">{datetime.now().strftime('%b %d, %Y at %I:%M %p PST')}</p>
    <p style="color:#e4e6ed;margin:0 0 20px">{body}</p>
    {f'<table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:20px">{ctx_lines}</table>' if ctx_lines else ''}
    <a href="{action_url}" style="display:inline-block;background:#4f8cff;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600">{action_label}</a>
    <p style="color:#3b6fd4;font-size:11px;margin:20px 0 0">Reytech RFQ Dashboard — Automated Alert</p>
  </div>
</div></body></html>"""

        msg = MIMEMultipart("alternative")
        msg["From"] = f"Reytech Dashboard <{GMAIL_ADDRESS}>"
        msg["To"] = NOTIFY_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_PASSWORD)
            server.send_message(msg)

        log.info("Alert email sent: %s → %s", subject[:50], NOTIFY_EMAIL)
        return {"ok": True, "to": NOTIFY_EMAIL}
    except Exception as e:
        log.warning("Alert email failed: %s", e)
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD BELL (persistent SQLite + in-memory for existing API)
# ══════════════════════════════════════════════════════════════════════════════

def _push_bell(event_type: str, title: str, body: str, urgency: str, context: dict) -> dict:
    """Push to both SQLite notifications table and in-memory deque (for /api/notifications)."""
    notif = {
        "type": event_type,
        "urgency": urgency,
        "title": title,
        "body": body,
        "context": context,
        "ts": datetime.now().isoformat(),
        "read": False,
        "deep_link": _get_deep_link(event_type, context),
    }

    # Push to in-memory (existing API compatibility)
    try:
        from src.api.dashboard import _push_notification
        _push_notification(notif)
    except Exception:
        pass

    # Persist to SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO notifications (event_type, urgency, title, body, context_json, deep_link, created_at, is_read)
                VALUES (?,?,?,?,?,?,?,0)
            """, (event_type, urgency, title, body, json.dumps(context), notif["deep_link"], notif["ts"]))
    except Exception as e:
        log.debug("Bell persist failed: %s", e)

    return {"ok": True}


def _get_deep_link(event_type: str, context: dict) -> str:
    """Return the most relevant dashboard URL for this notification."""
    links = {
        "cs_draft_ready":    "/outbox",
        "rfq_arrived":       "/",
        "quote_won":         "/quotes",
        "auto_draft_ready":  "/outbox",
        "outbox_stale":      "/outbox",
        "scprs_pull_done":   "/intelligence",
        "voice_call_placed": "/campaigns",
        "quote_lost_signal": "/quotes",
        "invoice_unpaid":    "/quotes",
        "delivery_confirmed":"/orders",
    }
    base = links.get(event_type, "/")
    if context.get("quote_number"):
        return f"/quotes?q={context['quote_number']}"
    return base


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL COMMUNICATION LOG (for CS dispute resolution)
# ══════════════════════════════════════════════════════════════════════════════

def log_email_event(
    direction: str,         # "sent" | "received"
    sender: str,
    recipient: str,
    subject: str,
    body_preview: str = "", # first 500 chars
    full_body: str = "",
    attachments: list = None,
    quote_number: str = "",
    po_number: str = "",
    rfq_id: str = "",
    contact_id: str = "",
    intent: str = "",       # rfq|cs_reply|cs_request|quote|alert|general
    status: str = "sent",   # draft|sent|delivered|failed|received
    message_id: str = "",   # email Message-ID header
    thread_id: str = "",    # email thread ID
) -> dict:
    """
    Log every email to SQLite email_log table for full audit trail.
    
    This powers CS dispute resolution:
      "We sent you Quote #R26Q4 on Feb 15 at 2:43pm."
      "Your email arrived on Feb 18 at 9:12am and we responded within 2 hours."
    
    Also updates activity_log for the associated CRM contact.
    """
    now = datetime.now().isoformat()
    preview = (body_preview or full_body or "")[:500]

    result = {}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO email_log (
                    logged_at, direction, sender, recipient, subject,
                    body_preview, full_body, attachments_json,
                    quote_number, po_number, rfq_id, contact_id,
                    intent, status, message_id, thread_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                now, direction, sender, recipient, subject,
                preview, full_body[:8000] if full_body else "",
                json.dumps(attachments or []),
                quote_number, po_number, rfq_id, contact_id,
                intent, status, message_id, thread_id,
            ))
        result = {"ok": True, "logged_at": now}
    except Exception as e:
        log.debug("email_log insert failed: %s", e)
        result = {"ok": False, "error": str(e)}

    # Also log to activity_log for CRM timeline
    if contact_id:
        try:
            from src.core.db import log_activity
            log_activity(
                contact_id=contact_id,
                event_type=f"email_{direction}",
                subject=subject,
                body=preview,
                outcome=status,
                actor="system" if direction == "received" else "mike",
                metadata={
                    "direction": direction,
                    "quote_number": quote_number,
                    "po_number": po_number,
                    "intent": intent,
                },
            )
        except Exception:
            pass

    return result


def get_email_thread(
    contact_email: str = "",
    quote_number: str = "",
    po_number: str = "",
    limit: int = 50,
) -> list:
    """
    Get full email thread for a contact/quote/PO — powers CS dispute resolution.

    Returns emails in chronological order with direction, date, subject, preview.
    CS agent uses this to say: "According to our records, we sent you a quote
    on Feb 15 and you replied on Feb 18."
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            clauses = []
            params = []
            if contact_email:
                clauses.append("(lower(sender) LIKE ? OR lower(recipient) LIKE ?)")
                params += [f"%{contact_email.lower()}%", f"%{contact_email.lower()}%"]
            if quote_number:
                clauses.append("quote_number=?")
                params.append(quote_number)
            if po_number:
                clauses.append("po_number=?")
                params.append(po_number)

            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            rows = conn.execute(
                f"SELECT * FROM email_log {where} ORDER BY logged_at ASC LIMIT ?",
                params + [limit]
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.debug("get_email_thread: %s", e)
        return []


def build_cs_communication_summary(contact_email: str = "", quote_number: str = "", po_number: str = "") -> str:
    """
    Build a human-readable communication history summary for CS calls.
    
    Used by CS agent and voice agent to resolve disputes:
    "Looking at our records: We sent you Quote R26Q4 on Feb 15 at 2:43pm.
     You replied on Feb 18 at 9:12am with a question about delivery timeline.
     We responded same day at 10:31am."
    """
    thread = get_email_thread(contact_email, quote_number, po_number, limit=20)
    if not thread:
        return ""

    lines = ["EMAIL COMMUNICATION HISTORY:"]
    for email in thread:
        ts = (email.get("logged_at") or "")[:16].replace("T", " ")
        direction = "→ SENT" if email.get("direction") == "sent" else "← RECEIVED"
        subj = email.get("subject", "")[:60]
        status = email.get("status", "")
        lines.append(f"  {ts} {direction}: {subj} [{status}]")
        if email.get("quote_number"):
            lines[-1] += f" | Quote: {email['quote_number']}"
        if email.get("po_number"):
            lines[-1] += f" | PO: {email['po_number']}"

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# STALE OUTBOX WATCHER (background thread)
# ══════════════════════════════════════════════════════════════════════════════

_stale_watcher_started = False

def start_stale_watcher():
    """Start background thread that alerts when drafts sit unreviewed for 4+ hours."""
    global _stale_watcher_started
    if _stale_watcher_started:
        return
    _stale_watcher_started = True

    def _watch():
        while True:
            time.sleep(3600)  # check every hour
            try:
                outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
                with open(outbox_path) as f:
                    outbox = json.load(f)
                
                cutoff = (datetime.now() - timedelta(hours=4)).isoformat()
                stale = [
                    e for e in outbox
                    if e.get("status") in ("draft", "cs_draft")
                    and e.get("created_at", "9999") < cutoff
                ]
                
                if stale:
                    sales_drafts = [e for e in stale if e.get("status") == "draft"]
                    cs_drafts = [e for e in stale if e.get("status") == "cs_draft"]
                    parts = []
                    if sales_drafts: parts.append(f"{len(sales_drafts)} sales draft(s)")
                    if cs_drafts: parts.append(f"{len(cs_drafts)} CS reply draft(s)")
                    
                    send_alert(
                        event_type="outbox_stale",
                        title=f"⏰ {len(stale)} Drafts Waiting for Review",
                        body=f"You have {', '.join(parts)} sitting unsent for 4+ hours. Tap to review.",
                        urgency="warning",
                        context={"count": len(stale), "entity_id": f"stale_{len(stale)}"},
                        cooldown_key="outbox_stale",
                        run_async=False,
                    )
            except Exception as e:
                log.debug("Stale watcher error: %s", e)

    t = threading.Thread(target=_watch, daemon=True, name="stale-watcher")
    t.start()
    log.info("Stale outbox watcher started (checks hourly, alerts at 4h threshold)")


# ══════════════════════════════════════════════════════════════════════════════
# PERSISTENT BELL — get notifications from SQLite
# ══════════════════════════════════════════════════════════════════════════════

def get_notifications(limit: int = 30, unread_only: bool = False) -> list:
    """Get persistent notifications from SQLite (survives deploys)."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            where = "WHERE is_read=0" if unread_only else ""
            rows = conn.execute(
                f"SELECT * FROM notifications {where} ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            results = []
            for r in rows:
                n = dict(r)
                try:
                    n["context"] = json.loads(n.get("context_json") or "{}")
                except Exception:
                    n["context"] = {}
                results.append(n)
            return results
    except Exception:
        return []


def mark_notifications_read(notification_ids: list = None) -> dict:
    """Mark notifications as read. If no IDs, marks all."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            if notification_ids:
                placeholders = ",".join("?" * len(notification_ids))
                conn.execute(f"UPDATE notifications SET is_read=1 WHERE id IN ({placeholders})", notification_ids)
            else:
                conn.execute("UPDATE notifications SET is_read=1")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_unread_count() -> int:
    """Fast unread count for bell badge — called every 30s by nav polling."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM notifications WHERE is_read=0").fetchone()
            return row["cnt"] if row else 0
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# AGENT STATUS
# ══════════════════════════════════════════════════════════════════════════════

def get_agent_status() -> dict:
    sms_ok = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM and NOTIFY_PHONE and SMS_ENABLED)
    email_ok = bool(GMAIL_ADDRESS and GMAIL_PASSWORD and NOTIFY_EMAIL and EMAIL_ENABLED)
    return {
        "agent": "notify_agent",
        "version": "1.0.0",
        "sms": {
            "enabled": sms_ok,
            "to": NOTIFY_PHONE[:6] + "****" if NOTIFY_PHONE else "(not set)",
            "from": TWILIO_FROM[:6] + "****" if TWILIO_FROM else "(not set)",
            "note": "Set NOTIFY_PHONE to your Google Voice or cell number in Railway",
        },
        "email_alerts": {
            "enabled": email_ok,
            "to": NOTIFY_EMAIL[:4] + "****@****" if NOTIFY_EMAIL else "(not set)",
            "from": GMAIL_ADDRESS,
            "note": "Set NOTIFY_EMAIL to your personal email (different from GMAIL_ADDRESS)",
        },
        "cooldown_minutes": COOLDOWN_MIN,
        "unread_bell": get_unread_count(),
        "stale_watcher": _stale_watcher_started,
        "setup_needed": [
            *([] if NOTIFY_PHONE else ["Set NOTIFY_PHONE in Railway env"]),
            *([] if NOTIFY_EMAIL else ["Set NOTIFY_EMAIL in Railway env"]),
            *([] if TWILIO_SID else ["Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER"]),
        ],
    }
