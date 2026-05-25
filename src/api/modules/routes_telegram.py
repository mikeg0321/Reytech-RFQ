"""Telegram bot webhook + ack/auto-delete substrate.

Routes:
  - POST /telegram/webhook
      Telegram POSTs callback_query updates here when the user taps the
      [✓ Got it] / [↩️ Keep it] inline button. We toggle acked_at in the
      telegram_messages table and edit the message's keyboard.

Auth: Telegram includes the configured secret_token as
      `X-Telegram-Bot-Api-Secret-Token` header. Any POST without the
      correct header is rejected with 401 (spoof protection — bots'
      webhook URLs are guessable, the secret is not).

The 24h auto-delete cron lives in `src/core/ops_monitor.py`'s hourly
loop; it reads telegram_messages.expires_at and POSTs deleteMessage to
Telegram for any row past its expiry.
"""
import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from flask import request, jsonify

log = logging.getLogger("reytech.telegram_webhook")


# Telegram's hard limit on bot-initiated message deletion in private chats.
# `deleteMessage` only works on messages the bot sent within this window;
# past it, the API returns 400 "message can't be deleted for everyone".
# We use 47h (not 48h) so the cleanup cron has a 1-hour buffer for any
# clock skew between Railway, Telegram, and SQLite.
TELEGRAM_DELETE_HARD_LIMIT_H = 47
ACK_DELETE_DELAY_H = 24


def _telegram_api(method: str, payload: dict) -> dict:
    """POST to the Telegram Bot API. Returns parsed JSON. Never raises."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN not set"}
    try:
        data = urllib.parse.urlencode({
            k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
            for k, v in payload.items()
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/{method}",
            data=data, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8") or "{}")
    except Exception as e:
        log.warning("Telegram API %s failed: %s", method, e)
        return {"ok": False, "error": str(e)}


def _compute_expires_at(sent_at_iso: str, acked_at_dt: datetime) -> str:
    """Return ISO timestamp for when this message should be auto-deleted.

    expires_at = min(acked_at + 24h, sent_at + 47h)

    The 47h cap honors Telegram's 48h deleteMessage window with a 1h
    safety buffer. If the message was acked >47h after send, expires_at
    will be in the past — the cleanup cron will try once, fail (past
    Telegram's window), mark deleted_at, and stop retrying. Graceful
    degrade per Mike's "trim to 24hr, i don't want fluff" directive.
    """
    natural = acked_at_dt + timedelta(hours=ACK_DELETE_DELAY_H)
    try:
        sent_dt = datetime.fromisoformat(sent_at_iso)
        if sent_dt.tzinfo is None:
            sent_dt = sent_dt.replace(tzinfo=timezone.utc)
        hard_cap = sent_dt + timedelta(hours=TELEGRAM_DELETE_HARD_LIMIT_H)
        return min(natural, hard_cap).isoformat()
    except Exception:
        return natural.isoformat()


def _handle_callback(callback_query: dict) -> dict:
    """Process one Telegram callback_query.
    Returns {"ok": True} on success; logs and returns ok=False on errors.
    Never raises — webhook handler must always 200 to Telegram."""
    cb_id = callback_query.get("id")
    data = (callback_query.get("data") or "").strip()
    msg = callback_query.get("message") or {}
    message_id = msg.get("message_id")
    chat = msg.get("chat") or {}
    chat_id = str(chat.get("id", ""))
    from_user = callback_query.get("from") or {}
    from_id = str(from_user.get("id", ""))

    # Auth: only the configured operator chat may toggle messages. This
    # is belt-and-suspenders on top of the webhook secret_token header —
    # if a spoofed callback ever slipped through, this still blocks it.
    allowed_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
    if allowed_chat and from_id != allowed_chat and chat_id != allowed_chat:
        log.warning("Rejected Telegram callback from foreign user %s/%s",
                    from_id, chat_id)
        if cb_id:
            _telegram_api("answerCallbackQuery", {"callback_query_id": cb_id})
        return {"ok": False, "error": "foreign caller"}

    if not message_id:
        return {"ok": False, "error": "no message_id in callback"}

    if data not in ("tg_ack", "tg_unack"):
        log.debug("Unknown callback_data: %r", data)
        if cb_id:
            _telegram_api("answerCallbackQuery", {"callback_query_id": cb_id})
        return {"ok": False, "error": f"unknown data: {data}"}

    from src.core.db import get_db
    now = datetime.now(timezone.utc)

    if data == "tg_ack":
        # Toggle to ACKED — set acked_at + expires_at (trimmed).
        try:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT sent_at FROM telegram_messages "
                    "WHERE message_id=? AND chat_id=?",
                    (int(message_id), chat_id),
                ).fetchone()
                sent_at_iso = row[0] if row else now.isoformat()
                expires_at = _compute_expires_at(sent_at_iso, now)
                conn.execute(
                    "UPDATE telegram_messages "
                    "SET acked_at=?, expires_at=? "
                    "WHERE message_id=? AND chat_id=?",
                    (now.isoformat(), expires_at,
                     int(message_id), chat_id),
                )
        except Exception as e:
            log.warning("ack DB update failed: %s", e)
        # Flip the keyboard to [↩️ Keep it]
        from src.agents.notify_agent import _ack_keyboard
        _telegram_api("editMessageReplyMarkup", {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": _ack_keyboard("acked"),
        })
        toast = "Marked read — auto-delete in 24h"

    else:  # tg_unack
        try:
            with get_db() as conn:
                conn.execute(
                    "UPDATE telegram_messages "
                    "SET acked_at=NULL, expires_at=NULL "
                    "WHERE message_id=? AND chat_id=?",
                    (int(message_id), chat_id),
                )
        except Exception as e:
            log.warning("unack DB update failed: %s", e)
        from src.agents.notify_agent import _ack_keyboard
        _telegram_api("editMessageReplyMarkup", {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": _ack_keyboard("unread"),
        })
        toast = "Kept — won't auto-delete"

    if cb_id:
        _telegram_api("answerCallbackQuery", {
            "callback_query_id": cb_id, "text": toast,
        })
    return {"ok": True}


def register_webhook_routes(bp):
    """Called by dashboard.py at module load — attaches the routes.
    The Flask blueprint pattern in this repo uses a `bp` global; the
    route-module loader injects it before exec()."""

    @bp.route("/telegram/webhook", methods=["POST"])
    def telegram_webhook():
        # Auth — Telegram sends our configured secret in this header.
        expected = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
        header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not expected or header != expected:
            log.warning("Rejected Telegram webhook — bad/missing secret token")
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        try:
            update = request.get_json(force=True, silent=True) or {}
        except Exception as e:
            log.warning("webhook payload parse failed: %s", e)
            return jsonify({"ok": False, "error": "bad payload"}), 200

        # We only handle callback_query (button taps) — incoming messages,
        # edits, etc. are ignored. Always 200 so Telegram doesn't retry.
        cb = update.get("callback_query")
        if cb:
            result = _handle_callback(cb)
            return jsonify(result), 200

        return jsonify({"ok": True, "skipped": "no callback_query"}), 200

    return bp


# Route registration runs at module-exec time (the dashboard loader
# imports + executes this file in the dashboard namespace, where `bp`
# is already defined). Calling register_webhook_routes here attaches
# the route immediately.
try:
    register_webhook_routes(bp)  # noqa: F821 — `bp` lives in dashboard scope
except NameError:
    # Module imported directly (e.g. by tests) — caller must call
    # register_webhook_routes(bp) themselves.
    pass


# ── Cleanup cron — called from ops_monitor's hourly loop ──────────────────


def run_telegram_cleanup_sweep() -> dict:
    """Delete acked Telegram messages whose expires_at is in the past.
    Idempotent — already-deleted rows are skipped.

    Returns: {"checked": N, "deleted": M, "failures": K, "ran_at": iso}.
    """
    from src.core.db import get_db
    now = datetime.now(timezone.utc)
    summary = {"checked": 0, "deleted": 0, "failures": 0,
               "ran_at": now.isoformat()}

    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT message_id, chat_id FROM telegram_messages "
                "WHERE expires_at IS NOT NULL "
                "  AND expires_at <= ? "
                "  AND deleted_at IS NULL",
                (now.isoformat(),)
            ).fetchall()
    except Exception as e:
        log.warning("telegram cleanup query failed: %s", e)
        return summary

    for row in rows:
        message_id, chat_id = row[0], row[1]
        summary["checked"] += 1
        result = _telegram_api("deleteMessage", {
            "chat_id": chat_id, "message_id": message_id,
        })
        try:
            with get_db() as conn:
                if result.get("ok"):
                    conn.execute(
                        "UPDATE telegram_messages "
                        "SET deleted_at=? "
                        "WHERE message_id=? AND chat_id=?",
                        (now.isoformat(), int(message_id), str(chat_id)),
                    )
                    summary["deleted"] += 1
                else:
                    # Past Telegram's 48h window OR message already gone —
                    # mark as "tried, failed" so we stop retrying. The
                    # delete_error column preserves the reason for audit.
                    err = (result.get("description")
                           or result.get("error") or "unknown")[:200]
                    conn.execute(
                        "UPDATE telegram_messages "
                        "SET deleted_at=?, delete_error=? "
                        "WHERE message_id=? AND chat_id=?",
                        (now.isoformat(), err,
                         int(message_id), str(chat_id)),
                    )
                    summary["failures"] += 1
                    log.info(
                        "Telegram deleteMessage failed for %s/%s: %s "
                        "(marked as final attempt)",
                        chat_id, message_id, err,
                    )
        except Exception as e:
            log.warning("telegram cleanup DB update failed: %s", e)
            summary["failures"] += 1

    if summary["deleted"] or summary["failures"]:
        log.info("Telegram cleanup: %s", summary)
    return summary


# ── Webhook registration (one-time at app boot) ───────────────────────────


def ensure_telegram_webhook_registered() -> dict:
    """Register the webhook URL + secret with Telegram. Idempotent —
    Telegram returns ok=True even when overwriting the same URL.

    Called once at app boot via dashboard.py's startup hook. Reads:
      - TELEGRAM_BOT_TOKEN, TELEGRAM_WEBHOOK_SECRET (required)
      - TELEGRAM_WEBHOOK_URL (optional override; defaults to
        PUBLIC_URL + /telegram/webhook)

    Returns the Telegram API response or {"ok": False, "skipped": "..."}
    when prereqs are missing.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    secret = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
    if not token:
        return {"ok": False, "skipped": "no TELEGRAM_BOT_TOKEN"}
    if not secret:
        return {"ok": False, "skipped": "no TELEGRAM_WEBHOOK_SECRET"}

    url = os.environ.get("TELEGRAM_WEBHOOK_URL", "")
    if not url:
        public = os.environ.get("PUBLIC_URL", "") or \
                 os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
        if public:
            base = public if public.startswith("http") else f"https://{public}"
            url = base.rstrip("/") + "/telegram/webhook"
    if not url:
        return {"ok": False, "skipped": "no TELEGRAM_WEBHOOK_URL or PUBLIC_URL"}

    result = _telegram_api("setWebhook", {
        "url": url,
        "secret_token": secret,
        # Limit to callback_query so we don't get spammed with every
        # inbound message / edit / etc.
        "allowed_updates": ["callback_query"],
    })
    if result.get("ok"):
        log.info("Telegram webhook registered: %s", url)
    else:
        log.warning("Telegram setWebhook returned: %s", result)
    return result
