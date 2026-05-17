"""
gmail_auth_watchdog.py — Proactive Gmail OAuth liveness substrate.

Tonight's silent OAuth break (2026-05-16, GMAIL_OAUTH_CLIENT_ID truncated
in Railway → `invalid_client` for hours, prod app silently blind to
inbound RFQs) proved that reactive circuit-breaker state isn't enough.
The poller failing on every cycle wasn't surfaced anywhere a human
checks — only buried in Railway logs.

This substrate closes that class:

  1. `check_inbox(inbox_name)` calls `users.getProfile()` — no-cost
     read, returns ok/error_class/checked_at dict.
  2. `run_watchdog_loop(interval=300)` runs every 5 min in a daemon
     thread; persists per-inbox health to gmail_health.json on the
     data volume.
  3. State transitions ok→broken emit:
       a. structured log line `GMAIL_AUTH_ALERT_BROKEN inbox=X reason=Y`
          (Railway log monitor can wire to PagerDuty/email).
       b. Twilio SMS to OPERATOR_PHONE (if Twilio configured).
     Re-warn every 30 min while broken (debounce prevents spam).
  4. State transitions broken→ok emit:
       a. structured log `GMAIL_AUTH_RECOVERED inbox=X`
       b. Twilio SMS "Gmail OAuth restored for X"
  5. `GET /api/admin/gmail/health` (in routes_gmail_health.py) returns
     the persisted state for dashboard / external monitoring.

The architecture mirrors the IngestRejection substrate pattern (every
event durably recorded, single writer, append-only history) but lives
one layer upstream — at the OAuth connection boundary, not the parser.
A failure here means no email is even visible to the parser.

DESIGN NOTES (Mike 2026-05-17):
- Healthcheck is a CORE component. "Should never not be working."
- Polls BOTH inboxes (sales@ + mike@). Per
  [[two-inboxes-mike-is-canonical-for-orders]] mike@ is the supplier-
  acknowledgment canonical inbox; losing it loses order ops too.
- Failure isolation: a Gmail API hiccup must NOT crash the watchdog
  thread. Wrap every check in try/except; log + persist + continue.
- Alert debounce: one SMS per state transition, not per poll iteration.
  Re-warn cadence 30 min capped at 6 messages per outage (3-hour
  ceiling) so a multi-hour outage doesn't drain the Twilio credit.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("reytech.gmail_auth_watchdog")

# Tunable cadences — environment-overridable for tests.
DEFAULT_WATCHDOG_INTERVAL_SEC = int(
    os.environ.get("GMAIL_WATCHDOG_INTERVAL_SEC", "300")
)
DEFAULT_REWARN_INTERVAL_SEC = int(
    os.environ.get("GMAIL_WATCHDOG_REWARN_SEC", "1800")
)
DEFAULT_REWARN_MAX_COUNT = int(
    os.environ.get("GMAIL_WATCHDOG_REWARN_MAX", "6")
)

# Inboxes we monitor. Per [[two-inboxes-mike-is-canonical-for-orders]]
# both are first-class; mike@ is the order-tracking canonical.
INBOXES = ("sales", "mike")


# ── Persistence ──────────────────────────────────────────────────────


def _state_path() -> Path:
    """Per-Railway-volume JSON state file. Defaults to data/."""
    try:
        from src.core.paths import DATA_DIR
        return Path(DATA_DIR) / "gmail_health.json"
    except Exception:
        return Path("data") / "gmail_health.json"


def load_state() -> dict:
    """Read persisted state. Returns empty dict if file missing/corrupt.

    Schema:
      {
        "sales": {
          "ok": bool,
          "error_class": str,
          "profile_email": str,
          "checked_at": iso,
          "state_changed_at": iso,
          "consecutive_failures": int,
          "rewarn_count": int,
          "last_alert_at": iso | null,
        },
        "mike": {...},
      }
    """
    path = _state_path()
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("gmail_health state read failed (will reset): %s", e)
        return {}


def _save_state(state: dict) -> None:
    """Atomic write via temp file + rename."""
    path = _state_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True),
                       encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        log.error("gmail_health state write failed: %s", e)


# ── Single inbox check ───────────────────────────────────────────────


def _classify_error(err_text: str) -> str:
    """Map raw error text to a stable category for alerting + dashboards."""
    s = (err_text or "").lower()
    if "invalid_client" in s:
        return "invalid_client"
    if "invalid_grant" in s:
        return "invalid_grant"
    if "invalid_scope" in s:
        return "invalid_scope"
    if "quota" in s or "rate" in s:
        return "rate_limited"
    if "timeout" in s or "timed out" in s:
        return "timeout"
    if "connection" in s:
        return "connection_error"
    return "other"


def check_inbox(inbox_name: str) -> dict:
    """One read-only liveness probe against the Gmail API.

    Uses users.getProfile() — Google's canonical zero-cost healthcheck.
    Returns a dict with ok/error_class/profile_email/checked_at fields.
    NEVER raises — every failure mode is captured + classified for the
    dashboard surface.
    """
    checked_at = datetime.now(timezone.utc).isoformat()
    base = {
        "checked_at": checked_at,
        "ok": False,
        "error_class": "",
        "profile_email": "",
    }
    try:
        from src.core.gmail_api import is_configured, get_service
    except Exception as e:
        log.error("gmail_auth_watchdog: gmail_api import failed: %s", e)
        return {**base, "error_class": "import_failed"}

    if not is_configured():
        return {**base, "error_class": "not_configured"}

    try:
        svc = get_service(inbox_name)
        result = svc.users().getProfile(userId="me").execute()
        return {
            **base,
            "ok": True,
            "profile_email": result.get("emailAddress", "") or "",
        }
    except Exception as e:
        cls = _classify_error(str(e))
        log.debug("gmail healthcheck %s failed: %s (class=%s)",
                  inbox_name, e, cls)
        return {**base, "error_class": cls}


def check_all_inboxes() -> dict:
    """Probe every inbox; return {inbox: probe_result, ...}."""
    return {inbox: check_inbox(inbox) for inbox in INBOXES}


# ── State transition + alerts ────────────────────────────────────────


def _send_sms_alert(body: str) -> None:
    """Twilio SMS to OPERATOR_PHONE. No-op if not configured.

    Never raises — alert failure must not crash the watchdog.
    """
    to = os.environ.get("OPERATOR_PHONE", "")
    if not to.strip():
        log.debug("gmail_auth_watchdog: OPERATOR_PHONE unset; skipping SMS")
        return
    try:
        from src.core.twilio_client import send_sms, is_configured
        if not is_configured():
            log.debug("gmail_auth_watchdog: Twilio not configured; skipping SMS")
            return
        result = send_sms(to, body[:1600])
        if not result.get("ok"):
            log.warning("gmail_auth_watchdog: SMS send failed: %s",
                        result.get("error"))
    except Exception as e:
        log.warning("gmail_auth_watchdog: SMS hook crashed: %s", e)


def _time_since_iso(iso_str: Optional[str]) -> Optional[timedelta]:
    if not iso_str:
        return None
    try:
        ts = datetime.fromisoformat(iso_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - ts
    except Exception:
        return None


def reconcile_and_alert(
    new_probes: dict,
    prior_state: dict,
    *,
    rewarn_interval_sec: int = DEFAULT_REWARN_INTERVAL_SEC,
    rewarn_max_count: int = DEFAULT_REWARN_MAX_COUNT,
    sms_sender=_send_sms_alert,
) -> dict:
    """Merge new probes with prior persisted state and emit alerts.

    Returns the new persisted state. Pure logic except for the SMS hook
    (which the caller can override for tests).

    Alert rules:
      - prior.ok=True  + new.ok=False → BROKEN alert (immediate)
      - prior.ok=False + new.ok=True  → RECOVERED alert (immediate)
      - prior.ok=False + new.ok=False
            AND time since last_alert_at >= rewarn_interval_sec
            AND rewarn_count < rewarn_max_count
                                       → REWARN (capped)
      - Otherwise: persist, no alert.
    """
    now = datetime.now(timezone.utc).isoformat()
    out: dict = {}
    for inbox, probe in new_probes.items():
        prior = (prior_state or {}).get(inbox, {})
        prior_ok = bool(prior.get("ok", True))
        new_ok = bool(probe.get("ok", False))

        entry = {
            **probe,
            "consecutive_failures": (
                0 if new_ok else int(prior.get("consecutive_failures", 0)) + 1
            ),
            "state_changed_at": (
                now if prior_ok != new_ok
                else prior.get("state_changed_at", now)
            ),
            "rewarn_count": int(prior.get("rewarn_count", 0)),
            "last_alert_at": prior.get("last_alert_at"),
        }

        if prior_ok and not new_ok:
            # BROKEN — immediate alert
            log.error(
                "GMAIL_AUTH_ALERT_BROKEN inbox=%s error_class=%s "
                "consecutive=%d", inbox, probe.get("error_class"),
                entry["consecutive_failures"],
            )
            sms_sender(
                f"Gmail OAuth BROKEN for {inbox}@reytechinc.com: "
                f"{probe.get('error_class','unknown')}. "
                f"Prod app is blind to inbound."
            )
            entry["last_alert_at"] = now
            entry["rewarn_count"] = 0

        elif not prior_ok and new_ok:
            # RECOVERED — immediate alert + reset counters
            log.info(
                "GMAIL_AUTH_RECOVERED inbox=%s after %d failures",
                inbox, int(prior.get("consecutive_failures", 0)),
            )
            sms_sender(
                f"Gmail OAuth RESTORED for {inbox}@reytechinc.com. "
                f"Prod app is reading inbound again."
            )
            entry["last_alert_at"] = now
            entry["rewarn_count"] = 0

        elif not new_ok:
            # Still broken — re-warn cadence
            since_last = _time_since_iso(entry["last_alert_at"])
            should_rewarn = (
                since_last is not None
                and since_last.total_seconds() >= rewarn_interval_sec
                and entry["rewarn_count"] < rewarn_max_count
            )
            if should_rewarn:
                log.error(
                    "GMAIL_AUTH_ALERT_STILL_BROKEN inbox=%s error_class=%s "
                    "consecutive=%d rewarn=%d",
                    inbox, probe.get("error_class"),
                    entry["consecutive_failures"],
                    entry["rewarn_count"] + 1,
                )
                sms_sender(
                    f"Gmail OAuth STILL BROKEN for {inbox}@reytechinc.com: "
                    f"{probe.get('error_class','unknown')}. "
                    f"Down for {int(since_last.total_seconds()/60)} min."
                )
                entry["last_alert_at"] = now
                entry["rewarn_count"] += 1

        out[inbox] = entry
    return out


# ── Daemon loop ──────────────────────────────────────────────────────


_WATCHDOG_THREAD: Optional[threading.Thread] = None
_WATCHDOG_STOP = threading.Event()


def run_watchdog_loop(
    interval_sec: int = DEFAULT_WATCHDOG_INTERVAL_SEC,
    *,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """Forever-loop that probes inboxes + persists state.

    Caller spawns this in a daemon thread. The loop NEVER raises out
    — every check + persist is try/except wrapped. A crash inside one
    iteration logs the error and sleeps to the next.
    """
    stop = stop_event or _WATCHDOG_STOP
    log.info(
        "gmail_auth_watchdog: starting (interval=%ds, inboxes=%s)",
        interval_sec, INBOXES,
    )
    while not stop.is_set():
        try:
            probes = check_all_inboxes()
            prior = load_state()
            new_state = reconcile_and_alert(probes, prior)
            _save_state(new_state)
        except Exception as e:
            log.error("gmail_auth_watchdog loop iteration crashed: %s",
                      e, exc_info=True)
        # Wake every second to check stop flag; sleep total = interval.
        for _ in range(interval_sec):
            if stop.is_set():
                break
            time.sleep(1)


def start_watchdog_thread(
    interval_sec: int = DEFAULT_WATCHDOG_INTERVAL_SEC,
) -> Optional[threading.Thread]:
    """Spawn the daemon thread once. Idempotent — second call is a no-op.

    Returns the thread handle, or None if startup was skipped.
    """
    global _WATCHDOG_THREAD
    if _WATCHDOG_THREAD is not None and _WATCHDOG_THREAD.is_alive():
        log.debug("gmail_auth_watchdog: thread already running")
        return _WATCHDOG_THREAD
    if os.environ.get("GMAIL_WATCHDOG_DISABLED", "").lower() in (
        "1", "true", "yes",
    ):
        log.info("gmail_auth_watchdog: disabled by env var")
        return None
    _WATCHDOG_STOP.clear()
    t = threading.Thread(
        target=run_watchdog_loop,
        kwargs={"interval_sec": interval_sec},
        name="gmail-auth-watchdog",
        daemon=True,
    )
    t.start()
    _WATCHDOG_THREAD = t
    log.info("gmail_auth_watchdog: thread started (id=%s)", t.ident)
    return t


def stop_watchdog_thread() -> None:
    """Signal the daemon to exit. For tests + clean shutdown."""
    global _WATCHDOG_THREAD
    _WATCHDOG_STOP.set()
    if _WATCHDOG_THREAD is not None:
        _WATCHDOG_THREAD.join(timeout=5)
    _WATCHDOG_THREAD = None
