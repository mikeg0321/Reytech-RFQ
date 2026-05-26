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

try:
    from src.core.db import upsert_outbox_email as _db_outbox_save
    _HAS_DB_OUTBOX = True
except ImportError:
    _HAS_DB_OUTBOX = False
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
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
TWILIO_FROM    = os.environ.get("TWILIO_FROM_NUMBER", "")

# Telegram is the "reports" channel for non-actionable digests
# (oracle_weekly, order_digest, scprs_pull_done, award_tracker_idle).
# Operator-actionable events stay on sms/email/bell.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED   = (
    os.environ.get("TELEGRAM_ENABLED", "true").lower() not in ("false","0","off","no","disabled")
    and bool(TELEGRAM_BOT_TOKEN) and bool(TELEGRAM_CHAT_ID)
)

SMS_ENABLED    = os.environ.get("NOTIFY_SMS", "false").lower() not in ("false","0","off","no","disabled")
EMAIL_ENABLED  = os.environ.get("NOTIFY_EMAIL_ALERTS", "true").lower() not in ("false","0","off")
COOLDOWN_MIN   = int(os.environ.get("ALERT_COOLDOWN_MIN", "15"))

# ── Cooldown tracker (in-memory, per event+entity key) ─────────────────────
_cooldown: dict[str, float] = {}
_cooldown_lock = threading.Lock()

def _is_cooled_down(key: str, ttl_seconds: int = None, _now_fn=time.time) -> bool:
    """Return True if this alert key is past its cooldown period.

    ttl_seconds overrides the default COOLDOWN_MIN*60 window, e.g. pass
    86400 to dedup an alert to "once per 24h". Negative cooldown values
    (set by snooze_alert) are honored — an alert snoozed past `now` will
    keep returning False until the snooze expires.

    `_now_fn` is injectable so unit tests can drive a fake clock.
    """
    ttl = int(ttl_seconds) if ttl_seconds is not None else COOLDOWN_MIN * 60
    with _cooldown_lock:
        last = _cooldown.get(key, 0)
        now = _now_fn()
        if last < 0:
            # Snooze marker: -abs(snooze_until). If we're past it, clear and fire.
            snooze_until = -last
            if now >= snooze_until:
                _cooldown[key] = now
                return True
            return False
        if now - last >= ttl:
            _cooldown[key] = now
            return True
        return False


def snooze_alert(key: str, hours: float = 24.0, _now_fn=time.time) -> dict:
    """Snooze a notification key for N hours. The next call to
    `_is_cooled_down(key, ...)` (and therefore the next send_alert with the
    same dedup key) will be suppressed until `now + hours*3600`.

    Returns the snooze metadata so callers / tests can verify.
    """
    snooze_until = _now_fn() + max(0.0, float(hours)) * 3600.0
    with _cooldown_lock:
        # Encoded as a negative timestamp so _is_cooled_down can distinguish
        # snooze markers from normal "last fired" values.
        _cooldown[key] = -snooze_until
    return {"key": key, "snoozed_until": snooze_until, "hours": hours}


def _reset_cooldowns_for_test():
    """Test-only: wipe the in-memory cooldown table."""
    with _cooldown_lock:
        _cooldown.clear()


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
    cooldown_seconds: int = None,   # override the default cooldown window for this call
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

    # Per-day bucket for long-cooldown alerts. When cooldown_seconds is at
    # least a full day, dedup by (key, day_bucket) so the alert can fire
    # once per calendar day per title even if the watcher hits send_alert
    # repeatedly within the same day.
    if cooldown_seconds is not None and cooldown_seconds >= 86400:
        day_bucket = datetime.now().strftime("%Y%m%d")
        dedup_key = f"{dedup_key}:{day_bucket}"

    if not _is_cooled_down(dedup_key, ttl_seconds=cooldown_seconds):
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
    except Exception as _e:
        log.debug("suppressed: %s", _e)


# Boot time — used for deploy-window suppression. Captured at module load.
# Any non-CATASTROPHIC alert in the first DEPLOY_WINDOW_S seconds after
# boot is suppressed (still bell-archived, but no Telegram/email/SMS).
# Rationale: Railway re-deploys cause transient false-positives on every
# health check; the deploy IS the cause and pinging Mike during the
# window is pure noise.
_BOOT_TIME = time.time()
DEPLOY_WINDOW_S = int(os.environ.get("NOTIFY_DEPLOY_WINDOW_S", "600"))  # 10 min


def _in_deploy_window() -> bool:
    """True if the app has been up for less than DEPLOY_WINDOW_S seconds.
    During this window, only CATASTROPHIC events (urgency='urgent') fire
    their normal channels; everything else falls back to bell-only."""
    return (time.time() - _BOOT_TIME) < DEPLOY_WINDOW_S


def _dispatch_alert(event_type, title, body, urgency, context, channels_override):
    """Actually send the alert across all appropriate channels.

    Mike's notification philosophy (2026-05-25):
      "I see everything; extra email is clutter. Status I don't need
       unless app is catastrophic failure. Worthy alerts are something
       not connected, or SCPRS update failed. Everything else is noise."

    Routing tiers — note SILENT IS THE DEFAULT:

      - SILENT (bell only): the long tail of operator-visible events
        that already surface in the dashboard (`/home`, queue, PO list).
        Bell is the audit log; nothing pings the operator. Volume is
        too low to need email/SMS redundancy — Mike checks the console.

      - WORTHY (single channel, Telegram): persistent external-
        dependency breaks + a few high-signal intelligence digests.
        Examples: external_service_disconnected, scprs_pull_failed_
        persistent, oracle_weekly, award_tracker_idle, loss_pattern_
        detected. ONE channel — no email duplicate.

      - CATASTROPHIC (Telegram + SMS): app is down / can't ingest /
        DB locked > 10 min. Operator must act now. SMS is reserved
        for "wake Mike up" — set urgency='urgent' to bypass the
        deploy-window suppression.

    Deploy-window suppression: in the first DEPLOY_WINDOW_S seconds
    after boot, only urgency='urgent' fires its normal channels;
    everything else degrades to bell-only. The 2026-05-25 deploy-
    health email storm — "1 check failed" arriving on every Railway
    boot — was the canonical noise this suppresses.
    """
    CHANNEL_MAP = {
        # ── CATASTROPHIC (Telegram + SMS) ──────────────────────────
        # App is down or pipeline is broken. Always-on; bypasses
        # deploy-window suppression because urgency='urgent' on these.
        "app_down":                ["telegram", "sms", "bell"],
        "ingest_broken":           ["telegram", "sms", "bell"],
        "db_locked_persistent":    ["telegram", "sms", "bell"],

        # ── WORTHY (Telegram only) ─────────────────────────────────
        # Persistent external-dependency breaks + Mike-ratified
        # intelligence digests (2026-05-25 explicit list).
        "oracle_weekly":                 ["telegram", "bell"],
        "award_tracker_idle":            ["telegram", "bell"],
        "loss_pattern_detected":         ["telegram", "bell"],
        "external_service_disconnected": ["telegram", "bell"],
        "scprs_pull_failed_persistent":  ["telegram", "bell"],
        "gmail_oauth_expired":           ["telegram", "bell"],
        "twilio_unreachable":            ["telegram", "bell"],
        "oracle_weekly_failed":          ["telegram", "bell"],
        "oracle_weekly_never_sent":      ["telegram", "bell"],
        "oracle_weekly_overdue":         ["telegram", "bell"],
        "oracle_weekly_crash":           ["telegram", "bell"],

        # ── Deadline missed (WORTHY — operator-visible bid deadline) ──
        # Mr. Wolf audit 2026-05-26: deadline_critical was UNROUTED in
        # CHANNEL_MAP, falling through to bell-only default. The
        # `_scan_deadlines` emitter at line ~970 fires with
        # urgency="urgent" + per-bid cooldown_key=f"deadline_critical:{doc_id}"
        # + cooldown_seconds=3600 already, but with no CHANNEL_MAP entry
        # Mike got NO Telegram on his 3 active Job #1 inbound RFQs going
        # 9.8h overdue. Promote to WORTHY tier so money-on-the-line
        # deadlines surface immediately.
        #
        # NOT in _SUPERSEDING_EVENT_TYPES on purpose: deadline_critical
        # fires once per bid (different doc_id ⇒ different cooldown
        # key); supersede on event_type alone would conflate distinct
        # overdue bids into one visible card. Operator must see ALL
        # overdue cards, not just the latest one. The existing 1h
        # per-bid cooldown already prevents same-bid re-spam.
        "deadline_critical":             ["telegram", "bell"],

        # ── SILENT (bell only — the long tail) ─────────────────────
        # 2026-05-25 directive: "I see everything in the operator
        # console, extra email is clutter." Every event below was on
        # sms+email+bell pre-fix; bell-only is the new floor. Add new
        # event types here by default; explicit Telegram promotion
        # requires a justified PR.
        "cs_draft_ready":            ["bell"],
        "rfq_arrived":               ["bell"],
        "quote_won":                 ["bell"],
        "po_received":               ["bell"],
        "buyer_replied":             ["bell"],
        "email_permanent_failure":   ["bell"],
        "order_delivered":           ["bell"],
        "all_delivered":             ["bell"],
        "line_shipped":              ["bell"],
        "line_delivered":            ["bell"],
        "auto_draft_ready":          ["bell"],
        "outbox_stale":              ["bell"],
        "voice_call_placed":         ["bell"],
        "quote_lost_signal":         ["bell"],
        "cs_call_placed":            ["bell"],
        "invoice_unpaid":            ["bell"],
        "delivery_confirmed":        ["bell"],
        "order_digest":              ["bell"],
        "cross_sell_weekly":         ["bell"],
        "scprs_pull_done":           ["bell"],
        "award_loss_detected":       ["bell"],
        "award_loss_margin_too_high":["bell"],
        "server_error":              ["bell"],   # transient — see deploy_health
        "deploy_health_failed":      ["bell"],   # transient deploy noise
    }
    channels = channels_override or CHANNEL_MAP.get(event_type, ["bell"])

    # ── Deploy-window suppression — SCOPED to deploy-caused events ──
    # In the first DEPLOY_WINDOW_S seconds after boot, suppress alerts
    # whose ROOT CAUSE is the deploy itself: deploy_health_failed (a
    # check transiently failing during startup), server_error (some
    # init code raising before the app warms up). These would re-fire
    # on every Railway restart if not suppressed.
    #
    # Real-data alerts (external_service_disconnected, scprs_pull_*,
    # gmail_oauth_expired, oracle_weekly, award_tracker_idle) describe
    # state of the OUTPUT — they don't become noise because we
    # redeployed. A "Gmail silent 4 days" alert is just as true the
    # second after a deploy as the minute before.
    #
    # 2026-05-25 v2: tightened from "all non-urgent" to "deploy-caused
    # only" after the liveness sweep (PR #1081) ran inside the window
    # and got incorrectly degraded to bell-only.
    _DEPLOY_NOISE_EVENTS = {"deploy_health_failed", "server_error"}
    if (_in_deploy_window()
            and urgency != "urgent"
            and event_type in _DEPLOY_NOISE_EVENTS):
        if any(ch in channels for ch in ("telegram", "email", "sms")):
            log.info(
                "Alert suppressed (deploy window, %ds remaining): %s | "
                "would-be channels=%s — bell archive only",
                int(DEPLOY_WINDOW_S - (time.time() - _BOOT_TIME)),
                event_type, channels,
            )
            channels = [ch for ch in channels if ch == "bell"] or ["bell"]

    results = {}

    if "sms" in channels and SMS_ENABLED and NOTIFY_PHONE:
        results["sms"] = _send_sms(title, body, context)

    if "email" in channels and EMAIL_ENABLED and NOTIFY_EMAIL:
        results["email"] = _send_alert_email(event_type, title, body, context)

    if "telegram" in channels and TELEGRAM_ENABLED:
        results["telegram"] = _send_telegram(event_type, title, body, urgency, context)

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
    """Send SMS via canonical Twilio helper (Tier 2e, audit 2026-05-07).
    Body is built here; canonical helper handles the actual send +
    retry + dual-env-var resolution."""
    from src.core.twilio_client import send_sms as _canonical_send, \
        is_configured as _twilio_configured
    if not _twilio_configured() or not NOTIFY_PHONE:
        return {"ok": False, "reason": (
            "Twilio not configured — set TWILIO_ACCOUNT_SID, "
            "TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER, NOTIFY_PHONE "
            "in Railway")}

    # Keep SMS tight — 160 char limit ideal
    sms_body = f"🔔 REYTECH: {title}\n{body[:140]}"
    if context.get("quote_number"):
        sms_body += f"\nQuote: {context['quote_number']}"
    if context.get("po_number"):
        sms_body += f"\nPO: {context['po_number']}"

    result = _canonical_send(NOTIFY_PHONE, sms_body)
    if result.get("ok"):
        log.info("SMS sent: %s → %s (SID: %s)",
                 title[:40], NOTIFY_PHONE, result.get("sid", ""))
    else:
        log.warning("SMS failed: %s", result.get("error"))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL ALERT (Gmail SMTP → NOTIFY_EMAIL personal address)
# ══════════════════════════════════════════════════════════════════════════════

def _send_alert_email(event_type: str, title: str, body: str, context: dict) -> dict:
    """Send alert email via Gmail API to Mike's personal address."""
    if not NOTIFY_EMAIL:
        return {"ok": False, "reason": "NOTIFY_EMAIL not configured"}

    from src.core import gmail_api
    if not gmail_api.is_configured():
        return {"ok": False, "reason": "Gmail API not configured"}

    try:
        URGENCY_SUBJECT_PREFIX = {
            "cs_draft_ready":    "📬 [ACTION] CS Draft Ready",
            "rfq_arrived":       "🚨 [URGENT] New RFQ Arrived",
            "quote_won":         "💰 [WIN] Quote Won",
            "auto_draft_ready":  "📋 [REVIEW] Draft Ready",
            "outbox_stale":      "⏰ [REMINDER] Drafts Waiting",
            "quote_lost_signal": "📉 [FYI] Quote Lost Signal",
            "invoice_unpaid":    "💸 [FOLLOW-UP] Invoice Unpaid",
            "oracle_weekly":     "📊 [ORACLE] Weekly Intelligence",
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
    <p style="color:#8b90a0;font-size:14px;margin:0 0 16px">{datetime.now().strftime('%b %d, %Y at %I:%M %p PST')}</p>
    <p style="color:#e4e6ed;margin:0 0 20px">{body}</p>
    {f'<table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:20px">{ctx_lines}</table>' if ctx_lines else ''}
    <a href="{action_url}" style="display:inline-block;background:#4f8cff;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600">{action_label}</a>
    <p style="color:#3b6fd4;font-size:14px;margin:20px 0 0">Reytech RFQ Dashboard — Automated Alert</p>
  </div>
</div></body></html>"""

        # Allow callers to provide custom HTML (e.g. Oracle weekly report)
        if context.get("html_body"):
            html = context["html_body"]

        service = gmail_api.get_send_service()
        gmail_api.send_message(
            service,
            to=NOTIFY_EMAIL,
            subject=subject,
            body_plain=body,
            body_html=html,
        )

        log.info("Alert email sent: %s → %s", subject[:50], NOTIFY_EMAIL)
        return {"ok": True, "to": NOTIFY_EMAIL}
    except Exception as e:
        log.warning("Alert email failed: %s", e)
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM (reports / non-actionable status — read-when-you-want)
# ══════════════════════════════════════════════════════════════════════════════

# Telegram MarkdownV2 reserves these characters — escape with leading backslash.
_TELEGRAM_MD_RESERVED = r"_*[]()~`>#+-=|{}.!"


def _escape_markdown_v2(text: str) -> str:
    """Escape Telegram MarkdownV2 reserved chars in body text.

    Bold/italic markers in the title are emitted unescaped by the caller;
    everything else (body, context values) flows through here so a stray
    underscore in a quote number doesn't blow up the message.
    """
    if not text:
        return ""
    out = []
    for ch in text:
        if ch in _TELEGRAM_MD_RESERVED:
            out.append("\\")
        out.append(ch)
    return "".join(out)


# Telegram caps a sendMessage at 4096 chars. Reserve headroom for the title,
# context block, and our trailing footer.
_TELEGRAM_BODY_LIMIT = 3500


def _send_telegram(event_type: str, title: str, body: str, urgency: str,
                   context: dict) -> dict:
    """POST one Telegram message via the Bot API to TELEGRAM_CHAT_ID.

    Returns {"ok": True/False, ...}. Never raises — Telegram failure
    must not break the alert pipeline.
    """
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return {"ok": False, "reason": "Telegram not configured"}

    # ── Pre-formatted body short-circuit ─────────────────────────────
    # Callers that build their own MarkdownV2 layout (oracle weekly,
    # award_tracker_idle, etc.) pass context["telegram_body"] with the
    # complete escaped+formatted message. We send it AS-IS so emoji
    # headers, monospace pre-blocks, and table alignment all survive.
    # This mirrors how context["html_body"] short-circuits the email
    # template in _send_alert_email.
    if context and context.get("telegram_body"):
        pre_built = str(context["telegram_body"])[:_TELEGRAM_BODY_LIMIT]
        return _telegram_post(pre_built, event_type, title)

    _URGENCY_EMOJI = {
        "urgent": "🚨",
        "warning": "⚠️",
        "deal": "💰",
        "draft": "📝",
        "info": "📊",
    }
    emoji = _URGENCY_EMOJI.get(urgency, "🔔")

    # Title is bold, free-form. Body and context values are escaped to
    # survive MarkdownV2 parsing.
    safe_title = _escape_markdown_v2(title)
    truncated = (body or "")[:_TELEGRAM_BODY_LIMIT]
    safe_body = _escape_markdown_v2(truncated)
    text_parts = [f"*{emoji} {safe_title}*", safe_body]

    ctx_lines = []
    for key, label in (
        ("quote_number", "Quote"),
        ("po_number",    "PO"),
        ("rfq_id",       "RFQ"),
        ("agency",       "Agency"),
        ("contact",      "Contact"),
    ):
        val = context.get(key) if context else None
        if val:
            ctx_lines.append(f"{label}: `{_escape_markdown_v2(str(val))}`")
    if context and "amount" in context:
        try:
            amount_str = "${:,.2f}".format(float(context["amount"]))
            ctx_lines.append("Amount: `" + _escape_markdown_v2(amount_str) + "`")
        except (TypeError, ValueError):
            pass

    if ctx_lines:
        text_parts.append("\n" + "\n".join(ctx_lines))

    text = "\n\n".join(p for p in text_parts if p)
    return _telegram_post(text, event_type, title)


def _ack_keyboard(state: str = "unread") -> dict:
    """Inline keyboard JSON for the [✓ Got it] / [↩️ Keep it] toggle.

    state="unread" → "[✓ Got it]" with callback_data "tg_ack"
    state="acked"  → "[↩️ Keep it]" with callback_data "tg_unack"

    Webhook handler reads callback_data prefix, toggles DB state, edits
    the message's reply_markup to flip the button label.
    """
    if state == "acked":
        return {
            "inline_keyboard": [[
                {"text": "↩️ Keep it", "callback_data": "tg_unack"}
            ]]
        }
    return {
        "inline_keyboard": [[
            {"text": "✓ Got it", "callback_data": "tg_ack"}
        ]]
    }


def _record_telegram_send(message_id: int, event_type: str, title: str) -> None:
    """Write a row to telegram_messages so the cleanup cron + webhook
    handler can find this message later. Best-effort — a write failure
    must not undo the Telegram send."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO telegram_messages "
                "(message_id, chat_id, event_type, title, sent_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (int(message_id), str(TELEGRAM_CHAT_ID),
                 event_type or "", (title or "")[:200],
                 datetime.now(timezone.utc).isoformat()),
            )
    except Exception as e:
        log.warning("telegram_messages insert failed: %s", e)


# Events that supersede their prior card when fired again. These are the
# WORTHY-tier liveness/freshness alerts whose CONDITION persists until
# resolved — the same "Gmail OAuth expired" alarm firing again tomorrow
# is the SAME alarm, not a new one, and Mike doesn't want his chat
# accreting daily duplicates. Same shape as camplock's
# telegram_ledger.SUPERSEDING_CATEGORIES (proven primitive Mike already
# runs in azure cron */2). Ported here per back-window audit 2026-05-26
# Item 6 / PR-A.
#
# NOT in this set (deliberately):
#   - "oracle_weekly" — each weekly digest is distinct content, not a
#     re-fire of the same condition. Supersede would lose history.
#   - "loss_pattern_detected" — each detected pattern is potentially
#     distinct; supersede would conflate unrelated losses.
#   - Catastrophic-tier events (app_down etc.) — those need every
#     occurrence visible.
_SUPERSEDING_EVENT_TYPES = frozenset({
    "award_tracker_idle",
    "external_service_disconnected",
    "scprs_pull_failed_persistent",
    "gmail_oauth_expired",
    "twilio_unreachable",
    "oracle_weekly_failed",
    "oracle_weekly_never_sent",
    "oracle_weekly_overdue",
    "oracle_weekly_crash",
})


def _telegram_delete_message(message_id: int) -> dict:
    """POST deleteMessage to Telegram. Returns {ok, error?}. Telegram's
    48h delete hard wall means very old messages return 'message to
    delete not found' — caller treats that as success (already gone).
    """
    try:
        import urllib.request
        import urllib.parse
        import json as _json
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "message_id": int(message_id),
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
            data=data,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = _json.loads(resp.read().decode("utf-8") or "{}")
        if payload.get("ok"):
            return {"ok": True}
        return {"ok": False, "error": payload.get("description", "unknown")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _supersede_prior_telegrams(event_type: str, current_message_id: int) -> int:
    """Find prior un-deleted telegram_messages with the same event_type
    and delete them via the Telegram API.

    Returns count of cards superseded. Only runs when event_type is in
    `_SUPERSEDING_EVENT_TYPES` — non-superseding events are no-op.

    Substrate-singleness fix (back-window audit 2026-05-26 Item 6 /
    PR-A): without this, the daily liveness sweep accreted stale
    duplicate cards in Mike's Telegram chat (e.g. five "Gmail inbound
    silent 96h" cards spanning a week). After this PR: ONE card per
    underlying condition; new card supersedes old via the Telegram
    deleteMessage API; row stays in telegram_messages with
    deleted_at stamped so the cleanup cron's audit trail is intact.

    Failure handling:
      - "message to delete not found" (past Telegram's 48h hard wall):
        treat as success — message is already gone from chat, just
        mark deleted_at in DB so we stop retrying.
      - Other errors: store in delete_error column; row stays
        un-deleted_at so the next supersede attempt retries.
    """
    if event_type not in _SUPERSEDING_EVENT_TYPES:
        return 0
    superseded = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT message_id FROM telegram_messages "
                "WHERE chat_id = ? AND event_type = ? "
                "AND deleted_at IS NULL AND message_id != ?",
                (str(TELEGRAM_CHAT_ID), event_type, int(current_message_id)),
            ).fetchall()
            for row in rows:
                prior_id = row[0] if not hasattr(row, "keys") else row["message_id"]
                result = _telegram_delete_message(prior_id)
                now_iso = datetime.now(timezone.utc).isoformat()
                if result.get("ok"):
                    conn.execute(
                        "UPDATE telegram_messages SET deleted_at = ?, "
                        "delete_error = NULL "
                        "WHERE message_id = ? AND chat_id = ?",
                        (now_iso, int(prior_id), str(TELEGRAM_CHAT_ID)),
                    )
                    superseded += 1
                    log.info(
                        "Telegram superseded: msg_id=%s event=%s",
                        prior_id, event_type,
                    )
                else:
                    err = (result.get("error") or "")[:200]
                    # "message to delete not found" = past Telegram's
                    # 48h delete wall; chat-side already gone — stamp
                    # deleted_at so we stop retrying.
                    if "not found" in err.lower():
                        conn.execute(
                            "UPDATE telegram_messages SET deleted_at = ?, "
                            "delete_error = ? "
                            "WHERE message_id = ? AND chat_id = ?",
                            (now_iso, err, int(prior_id),
                             str(TELEGRAM_CHAT_ID)),
                        )
                    else:
                        conn.execute(
                            "UPDATE telegram_messages SET delete_error = ? "
                            "WHERE message_id = ? AND chat_id = ?",
                            (err, int(prior_id), str(TELEGRAM_CHAT_ID)),
                        )
                        log.warning(
                            "Telegram supersede delete failed: "
                            "msg_id=%s err=%s", prior_id, err,
                        )
    except Exception as e:
        log.warning("supersede_prior_telegrams failed: %s", e)
    return superseded


def _telegram_post(text: str, event_type: str, title: str) -> dict:
    """POST a pre-built MarkdownV2 payload to the Telegram Bot API with
    the [✓ Got it] inline keyboard attached. Records the resulting
    message_id in telegram_messages so the cleanup cron can find it.

    Used by both the standard escape-and-send path and the pre-formatted
    short-circuit so error handling stays in one place.
    """
    try:
        import urllib.request
        import urllib.parse
        import json as _json
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": "true",
            "reply_markup": _json.dumps(_ack_keyboard("unread")),
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=data,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = _json.loads(resp.read().decode("utf-8") or "{}")
        if payload.get("ok"):
            message_id = payload.get("result", {}).get("message_id")
            log.info("Telegram sent: %s [%s] msg_id=%s",
                     title[:60], event_type, message_id)
            if message_id:
                _record_telegram_send(message_id, event_type, title)
                # PR-A 2026-05-26: WORTHY-tier alerts whose CONDITION
                # persists supersede their prior card. Order matters:
                # record_send FIRST (so the new row exists and the
                # supersede query's `message_id != ?` filter targets
                # only OLDER cards), then supersede.
                _supersede_prior_telegrams(
                    event_type, current_message_id=message_id,
                )
            return {"ok": True, "message_id": message_id}
        log.warning("Telegram API rejected: %s", payload)
        return {"ok": False, "error": payload.get("description", "unknown")}
    except Exception as e:
        log.warning("Telegram send failed: %s", e)
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
    except Exception as _e:
        log.debug("suppressed: %s", _e)

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
        except Exception as _e:
            log.debug("suppressed: %s", _e)

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
    """Start background thread that alerts when drafts sit unreviewed for 4+ hours.

    Gated by `notify.stale_drafts_email_enabled` flag (default OFF — Mike found
    the count was inflated 268+ stale-draft emails because the outbox includes
    auto-generated CS replies that should never auto-send. Flip the flag back
    ON via /admin/flags once the underlying outbox query is fixed to filter
    auto-generated drafts).
    """
    global _stale_watcher_started
    if _stale_watcher_started:
        return
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("notify.stale_drafts_email_enabled", default=False):
            log.info("Stale outbox watcher disabled by flag "
                     "notify.stale_drafts_email_enabled (default off)")
            return
    except Exception as e:
        log.debug("flag check failed, defaulting to off: %s", e)
        return
    _stale_watcher_started = True

    def _watch():
        from src.core.scheduler import _shutdown_event, heartbeat
        _shutdown_event.wait(3600)  # initial delay + check every hour
        while not _shutdown_event.is_set():
            try:
                outbox_path = os.path.join(DATA_DIR, "email_outbox.json")  # fallback
                try:
                    from src.core.dal import get_outbox
                    outbox = get_outbox()
                except Exception:
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
                        # Dedup by title (not entity_id) so the same banner doesn't
                        # post 12× in a day each time the count ticks up by one.
                        # 24h cooldown + day-bucket key = once per calendar day.
                        cooldown_key="outbox_stale_drafts_waiting",
                        cooldown_seconds=86400,
                        run_async=False,
                    )
                heartbeat("stale-watcher", success=True)
            except Exception as e:
                log.debug("Stale watcher error: %s", e)
                heartbeat("stale-watcher", success=False, error=str(e)[:200])
            _shutdown_event.wait(3600)
        log.info("Stale watcher shutting down")

    t = threading.Thread(target=_watch, daemon=True, name="stale-watcher")
    t.start()
    log.info("Stale outbox watcher started (checks hourly, alerts at 4h threshold)")


# ══════════════════════════════════════════════════════════════════════════════
# DEADLINE ESCALATION WATCHER (background thread — GRILL-Q3)
# ══════════════════════════════════════════════════════════════════════════════

_deadline_watcher_started = False

def start_deadline_watcher():
    """Alert hourly when an active PC/RFQ is <4h out or overdue.

    Complements the UI hard-alert modal (base.html) for when the operator
    is away from the dashboard — SMS + email fire via send_alert() with
    a per-bid cooldown so a single overdue item doesn't spam every hour.
    """
    global _deadline_watcher_started
    if _deadline_watcher_started:
        return
    _deadline_watcher_started = True

    def _watch():
        from src.core.scheduler import _shutdown_event, heartbeat
        _shutdown_event.wait(3600)
        while not _shutdown_event.is_set():
            try:
                from src.api.modules.routes_deadlines import _scan_deadlines
                critical = _scan_deadlines(urgencies={"overdue", "critical"})
                for it in critical:
                    is_overdue = it["urgency"] == "overdue"
                    icon = "🚨" if is_overdue else "⏰"
                    label = "OVERDUE" if is_overdue else "DUE SOON"
                    pc_num = it.get("pc_number") or it["doc_id"][:8]
                    inst = it.get("institution") or "—"
                    send_alert(
                        event_type="deadline_critical",
                        title=f"{icon} {label}: {pc_num} ({inst})",
                        body=f"{it['countdown_text']} — {it.get('item_count', 0)} item(s). Open: {it['url']}",
                        urgency="urgent",
                        context={"doc_id": it["doc_id"], "doc_type": it["doc_type"],
                                 "entity_id": it["doc_id"], "urgency": it["urgency"]},
                        # Per-bid key + 1h cooldown = max one alert per bid per hour.
                        cooldown_key=f"deadline_critical:{it['doc_id']}",
                        cooldown_seconds=3600,
                        run_async=False,
                    )
                heartbeat("deadline-watcher", success=True)
            except Exception as e:
                log.debug("Deadline watcher error: %s", e)
                heartbeat("deadline-watcher", success=False, error=str(e)[:200])
            _shutdown_event.wait(3600)
        log.info("Deadline watcher shutting down")

    t = threading.Thread(target=_watch, daemon=True, name="deadline-watcher")
    t.start()
    log.info("Deadline escalation watcher started (checks hourly, alerts on <4h/overdue bids)")


# ══════════════════════════════════════════════════════════════════════════════
# DAILY DEADLINE DIGEST (morning summary email)
# ══════════════════════════════════════════════════════════════════════════════

_daily_digest_started = False
DIGEST_HOUR_PST = int(os.environ.get("DIGEST_HOUR_PST", "7"))   # 7am default
DIGEST_MIN_PST = int(os.environ.get("DIGEST_MIN_PST", "30"))   # :30
_PST_UTC_OFFSET_HOURS = 8  # PST; accept one-hour drift during DST


def _format_digest_line(it: dict) -> str:
    """One line per bid for the digest email: 'RFQ 10843276 CIW due today at 2:00 PM'."""
    doc_type = (it.get("doc_type") or "").upper() or "BID"
    pc_num = it.get("pc_number") or it.get("doc_id", "")[:8]
    inst = it.get("institution") or ""
    hours_left = it.get("hours_left", 0)
    urgency = it.get("urgency", "")
    time_str = (it.get("due_time") or "").strip()

    if urgency == "overdue":
        when = f"OVERDUE ({it.get('countdown_text', '')})"
    elif hours_left < 24:
        when = f"due today at {time_str}" if time_str else f"due today ({it.get('countdown_text','')})"
    elif hours_left < 48:
        when = f"due tomorrow at {time_str}" if time_str else f"due tomorrow ({it.get('countdown_text','')})"
    else:
        when = f"due {it.get('due_date','')} at {time_str}" if time_str else f"due {it.get('due_date','')}"

    parts = [f"{doc_type} {pc_num}"]
    if inst:
        parts.append(inst)
    parts.append(when)
    return " ".join(parts)


def _build_digest_body(items: list) -> str:
    if not items:
        return "No PCs or RFQs due in the next 48 hours. You're clear."

    overdue = [i for i in items if i["urgency"] == "overdue"]
    today = [i for i in items if i["urgency"] != "overdue" and i.get("hours_left", 999) < 24]
    tomorrow = [i for i in items if i["urgency"] != "overdue" and 24 <= i.get("hours_left", 999) < 48]

    lines = []
    if overdue:
        lines.append(f"🚨 OVERDUE ({len(overdue)}):")
        for it in overdue:
            lines.append(f"  • {_format_digest_line(it)}")
        lines.append("")
    if today:
        lines.append(f"⏰ DUE TODAY ({len(today)}):")
        for it in today:
            lines.append(f"  • {_format_digest_line(it)}")
        lines.append("")
    if tomorrow:
        lines.append(f"📅 DUE TOMORROW ({len(tomorrow)}):")
        for it in tomorrow:
            lines.append(f"  • {_format_digest_line(it)}")
        lines.append("")

    lines.append("— Reytech deadline digest")
    return "\n".join(lines)


def send_daily_digest() -> dict:
    """Send the deadline digest email. Callable directly for ad-hoc runs or tests."""
    try:
        from src.api.modules.routes_deadlines import _scan_deadlines
        all_items = _scan_deadlines()
    except Exception as e:
        log.warning("digest: scan failed: %s", e)
        return {"ok": False, "error": f"scan: {e}"}

    # Keep only overdue + next 48 hours so the email is actionable.
    items = [
        i for i in all_items
        if i.get("urgency") == "overdue" or i.get("hours_left", 999) < 48
    ]
    items.sort(key=lambda d: d.get("hours_left", 999))

    body = _build_digest_body(items)
    subj = f"Reytech deadlines — {len(items)} due next 48h"
    if any(i["urgency"] == "overdue" for i in items):
        subj = f"🚨 {subj}"

    # 2026-05-25 substrate: route via send_alert. `order_digest` is
    # bell-only in CHANNEL_MAP per Mike's silent-default directive
    # (this digest function is gated OFF by default anyway and is on
    # deletion track — kept compatible with the rest of the substrate
    # in case it's ever re-enabled).
    return send_alert(
        event_type="order_digest",
        title=subj,
        body=body,
        urgency="info",
        cooldown_key="deadline_digest_daily",
        cooldown_seconds=82800,  # 23h — daily-bucketed
        run_async=False,
    )


def start_daily_digest():
    """Daemon thread that fires send_daily_digest once per day at DIGEST_HOUR:DIGEST_MIN PST.

    Gated by `notify.deadline_digest_email_enabled` flag (default OFF — Mike found
    the count was 61 stale "overdue" PCs that included items already converted to
    RFQ or sent. Counts are frozen at 7:30am scan time and don't refresh before
    the email arrives. Flip back ON via /admin/flags once the digest re-scans at
    send time and excludes converted/sent PCs).
    """
    global _daily_digest_started
    if _daily_digest_started:
        return
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("notify.deadline_digest_email_enabled", default=False):
            log.info("Daily deadline digest disabled by flag "
                     "notify.deadline_digest_email_enabled (default off)")
            return
    except Exception as e:
        log.debug("flag check failed, defaulting to off: %s", e)
        return
    _daily_digest_started = True

    def _watch():
        from src.core.scheduler import _shutdown_event, heartbeat
        last_fire_date = None
        # First check 5 minutes after startup to absorb any reboot right at the window.
        _shutdown_event.wait(300)
        while not _shutdown_event.is_set():
            try:
                # Approx PST = UTC - 8. DST drift ± 1h acceptable for a daily digest.
                now_pst = datetime.utcnow() - timedelta(hours=_PST_UTC_OFFSET_HOURS)
                today = now_pst.date()
                in_window = (
                    now_pst.hour == DIGEST_HOUR_PST
                    and now_pst.minute >= DIGEST_MIN_PST
                )
                if in_window and last_fire_date != today:
                    r = send_daily_digest()
                    if r.get("ok"):
                        last_fire_date = today
                        heartbeat("daily-digest", success=True)
                    else:
                        heartbeat("daily-digest", success=False, error=str(r.get("error"))[:200])
                else:
                    heartbeat("daily-digest", success=True)
            except Exception as e:
                log.debug("Daily digest loop error: %s", e)
                heartbeat("daily-digest", success=False, error=str(e)[:200])
            # Poll every 15 minutes — cheap, and ensures we catch the window
            # even after a reboot that lands mid-morning.
            _shutdown_event.wait(900)
        log.info("Daily digest shutting down")

    t = threading.Thread(target=_watch, daemon=True, name="daily-digest")
    t.start()
    log.info("Daily deadline digest started (fires %02d:%02d PST)", DIGEST_HOUR_PST, DIGEST_MIN_PST)


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
                conn.execute("UPDATE notifications SET is_read=1 WHERE id IN (" + placeholders + ")", notification_ids)
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
    try:
        from src.core import gmail_api
        _gmail_ready = gmail_api.is_configured()
    except Exception:
        _gmail_ready = False
    email_ok = bool(_gmail_ready and NOTIFY_EMAIL and EMAIL_ENABLED)
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


def notify_new_rfq_sms(rfq_data: dict) -> None:
    """Send SMS alert for a new RFQ via canonical Twilio helper
    (Tier 2e, audit 2026-05-07). Fire-and-forget: a Twilio failure
    doesn't break ingest."""
    from src.core.twilio_client import send_sms as _canonical_send, \
        is_configured as _twilio_configured
    if not _twilio_configured() or not NOTIFY_PHONE:
        log.info("SMS skip (Twilio not configured): new RFQ %s",
                 rfq_data.get("id", "?"))
        return
    if not SMS_ENABLED:
        return
    sol = rfq_data.get("solicitation_number", "?")
    agency = rfq_data.get("agency", "?")
    items = rfq_data.get("line_items", rfq_data.get("items", []))
    item_count = len(items) if isinstance(items, list) else 0
    due = rfq_data.get("due_date", "TBD")
    rfq_id = rfq_data.get("id", "?")
    base_url = os.environ.get("BASE_URL",
        os.environ.get("RAILWAY_PUBLIC_DOMAIN",
                       "https://web-production-dcee9.up.railway.app"))
    if not base_url.startswith("http"):
        base_url = f"https://{base_url}"
    msg = (f"New RFQ: {sol} | {agency} | {item_count} items | "
           f"Due {due} | {base_url}/rfq/{rfq_id}")
    result = _canonical_send(NOTIFY_PHONE, msg)
    if result.get("ok"):
        log.info("SMS sent for new RFQ %s to %s", sol, NOTIFY_PHONE)
    else:
        log.warning("SMS for new RFQ failed (non-blocking): %s",
                    result.get("error"))


def notify_package_ready(rfq, result=None):
    """Internal notification when RFP package is generated and ready to send."""
    sol = rfq.get("solicitation_number", "") or "RFQ"
    qn = rfq.get("reytech_quote_number", "")
    agency = rfq.get("agency_name", "") or rfq.get("institution", "")
    total = result.get("total", 0) if result else 0
    items = len(rfq.get("line_items", []))
    rid = rfq.get("id", "")

    msg = f"📦 Package {qn} ready — {agency} #{sol} — ${total:,.2f} ({items} items)"
    log.info("NOTIFY: %s", msg)

    # Log to activity feed
    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("rfq", rid, "package_ready", msg, actor="system")
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    # Webhook (Slack/Teams/custom)
    try:
        from src.core.webhooks import fire_webhook
        fire_webhook("package.ready", {
            "quote_number": qn,
            "solicitation": sol,
            "agency": agency,
            "total": total,
            "items": items,
            "url": f"/rfq/{rid}/review-package",
        })
    except Exception as _e:
        log.debug("suppressed: %s", _e)
