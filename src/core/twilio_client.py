"""Canonical Twilio SMS helper.

Tier 2e (audit 2026-05-07). Five Twilio implementations across the
codebase did the same work with subtly different shapes:

  src/api/modules/routes_crm.py:_send_sms              (urllib REST)
  src/agents/due_date_reminder.py:_send_sms_reminder   (Twilio SDK)
  src/agents/notify_agent.py:_send_sms                  (Twilio SDK)
  src/agents/notify_agent.py:notify_new_rfq_sms        (Twilio SDK)
  src/agents/growth_agent.py:send_sms_outreach         (Twilio SDK,
                                                        short env vars)

Two env-var conventions in active use:
  * Twilio-official: TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN /
    TWILIO_FROM_NUMBER (also TWILIO_PHONE_NUMBER seen in some setups)
  * Short:           TWILIO_SID / TWILIO_TOKEN / TWILIO_FROM

If the operator sets one set, the codepaths reading the other set
silently no-op. This module reads BOTH, prefers official, and logs a
one-time deprecation when only short names resolve.

Adds transient-error retry via `src.core.external_call.with_retry()`
(PR #833) so a 5xx blip from Twilio's edge doesn't lose an alert.
Predicate: 5xx + 429 + ConnectionError/Timeout transient. 4xx-other
= operator/data error, fast-fail.
"""
from __future__ import annotations

import logging
import os
from typing import Dict

log = logging.getLogger(__name__)

# One-time deprecation log gate (module-level state). Cheap.
_DEPRECATION_LOGGED = False


def _read_creds() -> Dict[str, str]:
    """Read Twilio creds. Prefers Twilio-official env vars; falls back
    to short names with a one-time deprecation log.

    Returns: {sid, token, from_number}
    """
    sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    # Two official "from" names seen in the wild — TWILIO_FROM_NUMBER
    # is what Mike's deployment uses; TWILIO_PHONE_NUMBER is what
    # notify_agent.py historically referenced in error messages.
    from_number = (
        os.environ.get("TWILIO_FROM_NUMBER", "")
        or os.environ.get("TWILIO_PHONE_NUMBER", "")
    )

    short_sid = os.environ.get("TWILIO_SID", "")
    short_token = os.environ.get("TWILIO_TOKEN", "")
    short_from = os.environ.get("TWILIO_FROM", "")

    used_short = False
    if not sid and short_sid:
        sid = short_sid
        used_short = True
    if not token and short_token:
        token = short_token
        used_short = True
    if not from_number and short_from:
        from_number = short_from
        used_short = True

    if used_short:
        global _DEPRECATION_LOGGED
        if not _DEPRECATION_LOGGED:
            log.warning(
                "Twilio creds resolved from short env vars "
                "(TWILIO_SID/TOKEN/FROM). Migrate to "
                "TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN/TWILIO_FROM_NUMBER "
                "— pre-Tier-2e callers couldn't read these."
            )
            _DEPRECATION_LOGGED = True

    return {"sid": sid, "token": token, "from_number": from_number}


def is_configured() -> bool:
    """All three Twilio creds present (under either env-var convention)."""
    c = _read_creds()
    return bool(c["sid"] and c["token"] and c["from_number"])


def _is_transient_twilio_error(err: BaseException) -> bool:
    """5xx + 429 from Twilio = transient (gateway/rate-limit). 4xx-other
    = operator/data error, fast-fail. Network-layer Connection reset /
    Timeout = transient."""
    # TwilioRestException carries `.status` (int). httpx/requests
    # variants might use `.status_code` on the inner response.
    try:
        status = getattr(err, "status", None) or getattr(err, "code", None)
        if isinstance(status, int):
            return status in {429, 500, 502, 503, 504}
        resp = getattr(err, "response", None)
        if resp is not None:
            sc = getattr(resp, "status_code", None)
            if isinstance(sc, int):
                return sc in {429, 500, 502, 503, 504}
    except Exception:
        pass
    msg = str(err)
    return ("Read timed out" in msg or "TimeoutError" in msg
            or "Connection reset" in msg or "Connection aborted" in msg
            or "EOF occurred" in msg)


def send_sms(to: str, body: str) -> Dict:
    """Send an SMS via Twilio with retry on transient errors.

    Args:
        to:   recipient phone number, E.164 format (`+15551234567`)
        body: message body. Truncated to 1600 chars (Twilio max).

    Returns:
        {"ok": True,  "sid": "<msg_sid>"}             on success
        {"ok": False, "error": "<reason>"}            on failure
        {"ok": False, "error": "Twilio not configured"}
                                                       when creds missing
        {"ok": False, "error": "twilio SDK not installed"}
                                                       when SDK absent

    Retry: 3 attempts, linear 1.0s/2.0s backoff. Predicate matches
    5xx + 429 + ConnectionError/Timeout (substrate from PR #833).
    """
    if not (to or "").strip():
        return {"ok": False, "error": "to: empty"}
    if not (body or "").strip():
        return {"ok": False, "error": "body: empty"}

    c = _read_creds()
    if not (c["sid"] and c["token"] and c["from_number"]):
        return {
            "ok": False,
            "error": (
                "Twilio not configured — set TWILIO_ACCOUNT_SID, "
                "TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER"
            ),
        }

    try:
        from twilio.rest import Client
    except ImportError:
        return {"ok": False, "error": "twilio SDK not installed"}

    truncated = body[:1600]

    def _do_send():
        client = Client(c["sid"], c["token"])
        return client.messages.create(
            body=truncated,
            from_=c["from_number"],
            to=to,
        )

    from src.core.external_call import with_retry
    try:
        msg = with_retry(
            _do_send,
            op="Twilio send",
            attempts=3,
            base_delay=1.0,
            backoff="linear",
            is_transient=_is_transient_twilio_error,
            logger=log,
        )
        return {"ok": True, "sid": getattr(msg, "sid", "")}
    except Exception as e:
        log.warning("Twilio send failed: %s: %s", type(e).__name__, e)
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
