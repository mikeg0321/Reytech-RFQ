"""gmail_sent_watcher.py — Background watcher: scan Gmail SENT folder,
fire the same post-send pipeline as the in-app Mark-Sent button for
every matched outbound message we haven't already processed.

THE PROBLEM IT CLOSES (PR #9 — substrate-wave handoff PR-3):

Mike has been sending quotes via manual Gmail compose since ~2026-05-15,
bypassing the in-app Mark-Sent button. That button fires a chain of
side-effects (status flip, propagate_sent_to_quote_row Spine sync,
log_quote_sent → operator_quote_sent insert, drive_triggers.on_quote_sent,
post_send_pipeline.on_quote_sent, prior_submissions auto-capture).

When the operator skips the button, NONE of those fire. Cascade of
broken downstream behavior:
  - Drive forms-archive silent since 2026-05-15
  - operator_quote_sent table = 0 rows (KPI dashboard empty)
  - Catalog cascade can't surface prior-sent prices on rebids
  - award_monitor never gets the "we sent this" signal
  - Spine quote_row stays at 'generated' / 'pending' instead of 'sent'

THIS WATCHER:
  1. Periodically calls `observed_send.detect_observed_sends()` to find
     outbound Gmail messages matching existing PC/RFQ records.
  2. For each NEW match (already_attached=False), pushes a Flask app
     context and calls the SAME `_api_*_mark_sent_manually_locked`
     function the button calls. All downstream actions fire identically
     — zero duplicate logic.
  3. Per-match try/except so one failed match doesn't kill the cycle.
     Per-action wrapping already exists inside the locked function.

IDEMPOTENCY:
  `already_attached` is True iff `gmail_message_id` is in the record's
  `gmail_message_ids` list. The locked function appends the gmail_id
  to that list on success (via the in-app mark-sent flow), so re-polls
  naturally skip processed messages.

OUT OF SCOPE (separate tickets):
  - INBOUND PO/award detection (lives in `email_poller`'s INBOX path)
  - Gmail label management (idempotency already covered by
    `already_attached`)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

log = logging.getLogger("reytech.gmail_sent_watcher")

# Defaults — overridable via env for tuning without redeploy.
_DEFAULT_SINCE_DAYS = int(os.environ.get("GMAIL_SENT_WATCHER_SINCE_DAYS", "14"))
_DEFAULT_MAX_MESSAGES = int(os.environ.get("GMAIL_SENT_WATCHER_MAX_MESSAGES", "200"))
_DEFAULT_INTERVAL_SEC = int(os.environ.get("GMAIL_SENT_WATCHER_INTERVAL_SEC", "600"))
_SCHEDULER_NAME = "gmail-sent-watcher"

# Module-level guard for the watchdog restart contract (see
# src.core.scheduler.register_restartable).
_scheduler_started = False
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_app = None  # captured at start_scheduler time


# ─── Core: one poll cycle ────────────────────────────────────────────


def run_watcher_once(
    *,
    since_days: int = _DEFAULT_SINCE_DAYS,
    max_messages: int = _DEFAULT_MAX_MESSAGES,
    app=None,
    detect_fn=None,
    fire_fn=None,
) -> Dict[str, Any]:
    """One poll: scan SENT, fire pipeline for new matches.

    Best-effort: always returns a summary dict; never raises into the
    scheduler. Test-overridable via `detect_fn` / `fire_fn`.
    """
    result: Dict[str, Any] = {
        "ok": True,
        "scanned": 0,
        "matched": 0,
        "already_attached": 0,
        "new_matches": 0,
        "fired": 0,
        "fire_failures": 0,
        "errors": [],
    }

    _detect = detect_fn or _default_detect
    _fire = fire_fn or _fire_mark_sent_for_match

    try:
        detection = _detect(since_days=since_days, max_messages=max_messages)
    except Exception as e:
        log.exception("gmail_sent_watcher: detect failed")
        result["ok"] = False
        result["errors"].append(f"detect failed: {e}")
        return result

    if not detection.get("ok"):
        result["ok"] = False
        result["errors"].append(detection.get("error", "detect returned ok=False"))
        return result

    result["scanned"] = int(detection.get("scanned", 0) or 0)
    matches = detection.get("matches", []) or []
    result["matched"] = len(matches)

    for match in matches:
        if match.get("already_attached"):
            result["already_attached"] += 1
            continue
        result["new_matches"] += 1
        try:
            _fire(match, app=app)
            result["fired"] += 1
        except Exception as e:
            log.exception(
                "gmail_sent_watcher: fire failed for gmail_id=%s record=%s",
                match.get("gmail_message_id"),
                match.get("matched_record_id"),
            )
            result["fire_failures"] += 1
            result["errors"].append({
                "gmail_id": match.get("gmail_message_id"),
                "matched_record_id": match.get("matched_record_id"),
                "matched_record_kind": match.get("matched_record_kind"),
                "error": str(e),
            })

    return result


def _default_detect(*, since_days: int, max_messages: int) -> Dict[str, Any]:
    """Default detector — lazy import so tests can stub at the call site."""
    from src.agents.observed_send import detect_observed_sends
    return detect_observed_sends(
        since_days=since_days, max_messages=max_messages,
    )


def _fire_mark_sent_for_match(match: Dict[str, Any], *, app=None) -> None:
    """Call the same locked function the in-app Mark-Sent button calls.

    Pushes a Flask app context if one isn't already active (background
    threads don't get one for free). The locked function returns a Flask
    Response which we discard — the side-effects (status flip, Drive
    archive, operator_quote_sent insert, propagate_sent_to_quote_row,
    training capture, prior_submissions) are what we want.
    """
    rid = match["matched_record_id"]
    kind = match["matched_record_kind"]
    gmail_id = match.get("gmail_message_id", "") or ""
    to_email = (match.get("to") or "").strip()
    sent_at = (match.get("date") or "").strip()

    payload = {
        "sent_to": to_email,
        "sent_at": sent_at,
        "notes": f"Auto-marked by gmail_sent_watcher (gmail_id={gmail_id})",
    }

    target_app = app or _scheduler_app
    if target_app is None:
        try:
            from flask import current_app
            target_app = current_app._get_current_object()
        except Exception:
            # Last resort: import the global. If even this fails, raise —
            # the per-match try/except in run_watcher_once captures it.
            from src.api.dashboard import app as _flask_app  # type: ignore
            target_app = _flask_app

    with target_app.app_context():
        if kind == "rfq":
            from src.api.data_layer import _save_rfqs_lock
            from src.api.modules.routes_rfq_admin import (
                _api_rfq_mark_sent_manually_locked,
            )
            with _save_rfqs_lock:
                _api_rfq_mark_sent_manually_locked(
                    rid, payload=payload, uploaded=None,
                )
        elif kind == "pc":
            from src.api.data_layer import _save_pcs_lock
            from src.api.modules.routes_pricecheck_pricing import (
                _api_pricecheck_mark_sent_manually_locked,
            )
            with _save_pcs_lock:
                _api_pricecheck_mark_sent_manually_locked(
                    rid, payload=payload, uploaded=None,
                )
        else:
            raise ValueError(f"unknown matched_record_kind: {kind!r}")


# ─── Scheduler — periodic poll loop ──────────────────────────────────


def _scheduler_loop(interval_sec: int):
    """Background poll loop. Never returns. Per-cycle try/except so a
    single bad poll cannot kill the loop."""
    log.info(
        "gmail_sent_watcher loop started (interval=%ds, since_days=%d, max=%d)",
        interval_sec, _DEFAULT_SINCE_DAYS, _DEFAULT_MAX_MESSAGES,
    )
    while True:
        try:
            time.sleep(interval_sec)
            try:
                r = run_watcher_once()
                ok = bool(r.get("ok"))
                try:
                    from src.core.scheduler import heartbeat
                    heartbeat(_SCHEDULER_NAME, success=ok,
                              error=(None if ok else "; ".join(
                                  str(e) for e in r.get("errors", []))[:200]))
                except Exception as _hbe:
                    log.debug("heartbeat suppressed: %s", _hbe)
                if r.get("fired") or r.get("fire_failures") or r.get("new_matches"):
                    log.info(
                        "gmail_sent_watcher tick: scanned=%d matched=%d "
                        "new=%d fired=%d failed=%d",
                        r.get("scanned", 0), r.get("matched", 0),
                        r.get("new_matches", 0), r.get("fired", 0),
                        r.get("fire_failures", 0),
                    )
            except Exception as e:
                log.exception("gmail_sent_watcher tick crashed: %s", e)
                try:
                    from src.core.scheduler import heartbeat
                    heartbeat(_SCHEDULER_NAME, success=False, error=str(e)[:200])
                except Exception:
                    pass
        except Exception:
            # Outer guard — never let the loop die.
            log.exception("gmail_sent_watcher outer loop crashed")
            try:
                time.sleep(60)
            except Exception:
                return


def start_scheduler(app=None, interval_sec: int = _DEFAULT_INTERVAL_SEC):
    """Start the periodic watcher. Idempotent. Pass `app` so the
    background thread can push a Flask app context per fire."""
    global _scheduler_started, _scheduler_thread, _scheduler_app
    if _scheduler_started:
        return
    if app is not None:
        _scheduler_app = app
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(interval_sec,),
        daemon=True,
        name=_SCHEDULER_NAME,
    )
    _scheduler_thread.start()
    _scheduler_started = True
    log.info("gmail_sent_watcher thread started (interval=%ds)", interval_sec)
