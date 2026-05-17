# routes_gmail_health.py — Admin surface for Gmail OAuth liveness.
#
# Companion to src/agents/gmail_auth_watchdog.py. The watchdog persists
# per-inbox health to data/gmail_health.json every 5 min; this route
# exposes that state for operator dashboards + external monitors.
#
# Per Mike 2026-05-17: Gmail auth is a CORE component, "should never
# not be working." This endpoint is the substrate visibility surface.

from flask import request, jsonify
from src.api.shared import bp, auth_required
import logging
import os

log = logging.getLogger("reytech")


@bp.route("/api/admin/gmail/health", methods=["GET"])
@auth_required
def admin_gmail_health():
    """Return Gmail OAuth liveness state for both inboxes.

    Reads `data/gmail_health.json` (written by gmail_auth_watchdog).
    Falls back to a one-shot live probe if the file doesn't exist yet
    (substrate must still be useful on first-boot before the watchdog
    has run an iteration).

    Query params:
      live=1 — bypass the persisted state and force a fresh live probe.
               Useful for "is it healthy RIGHT NOW" checks (paging
               escalation, manual sanity check after OAuth fix).

    Response shape:
      {
        "ok": True,
        "any_broken": bool,    # quick rollup for monitors
        "inboxes": {
          "sales": { ok, error_class, profile_email, checked_at,
                     state_changed_at, consecutive_failures,
                     rewarn_count, last_alert_at },
          "mike":  { ... },
        },
        "watchdog": {
          "interval_sec": int,
          "state_file": str,
          "source": "persisted" | "live_probe" | "not_yet_run",
        }
      }
    """
    try:
        from src.agents.gmail_auth_watchdog import (
            INBOXES, DEFAULT_WATCHDOG_INTERVAL_SEC,
            load_state, check_all_inboxes, _state_path,
        )
    except Exception as e:
        log.error("gmail_health: watchdog module unavailable: %s", e,
                  exc_info=True)
        return jsonify({
            "ok": False, "error": "gmail_auth_watchdog unavailable",
        }), 500

    live_mode = request.args.get("live", "").strip() in ("1", "true", "yes")

    if live_mode:
        inboxes = check_all_inboxes()
        source = "live_probe"
    else:
        persisted = load_state()
        if not persisted:
            # Watchdog hasn't run yet — do a one-shot live probe so the
            # operator gets a meaningful answer instead of empty state.
            inboxes = check_all_inboxes()
            source = "not_yet_run"
        else:
            inboxes = persisted
            source = "persisted"

    # Substrate guarantee: every monitored inbox appears in the
    # response, even if the persisted state is missing some entries.
    for inbox in INBOXES:
        if inbox not in inboxes:
            inboxes[inbox] = {
                "ok": False,
                "error_class": "no_data_yet",
                "checked_at": None,
            }

    any_broken = any(not (v or {}).get("ok", False) for v in inboxes.values())

    return jsonify({
        "ok": True,
        "any_broken": any_broken,
        "inboxes": inboxes,
        "watchdog": {
            "interval_sec": DEFAULT_WATCHDOG_INTERVAL_SEC,
            "state_file": str(_state_path()),
            "source": source,
            "disabled": os.environ.get(
                "GMAIL_WATCHDOG_DISABLED", ""
            ).lower() in ("1", "true", "yes"),
        },
    })
