"""Silent-by-default notification substrate — 2026-05-25 redesign.

Mike's directive after seeing residual deploy-health emails: "I see
everything in the operator console; extra email is clutter. Status I
don't need unless app is catastrophic failure. Worthy alerts are
something not connected, or SCPRS update failed. Everything else is
noise, especially during a deploy."

This file pins three substrate behaviours:

  1. CHANNEL_MAP — silent-by-default tiering:
     - WORTHY: oracle_weekly, award_tracker_idle, loss_pattern_detected,
       external_service_disconnected, scprs_pull_failed_persistent,
       gmail_oauth_expired, twilio_unreachable, oracle_weekly_*. All
       single-channel Telegram (+ bell archive).
     - CATASTROPHIC: app_down, ingest_broken, db_locked_persistent.
       Telegram + SMS, urgency='urgent' bypasses deploy-window gate.
     - SILENT (long tail): every actionable event Mike sees in the
       console — cs_draft_ready, rfq_arrived, quote_won, po_received,
       buyer_replied, email_permanent_failure, etc. Bell-only.

  2. Deploy-window suppression: in the first DEPLOY_WINDOW_S seconds
     after boot, any non-urgent alert that would otherwise hit
     Telegram/email/SMS gets degraded to bell-only. CATASTROPHIC
     (urgency='urgent') is exempt.

  3. CI guard — status-class files must not directly call
     gmail_api.send_message. Operator-outbound (vendor/buyer/agency
     emails) stays allowlisted at Layer 2; status emails must go
     through notify_agent.send_alert so CHANNEL_MAP routing applies.
"""
from pathlib import Path
import re
from unittest.mock import patch

import pytest


_REPO = Path(__file__).resolve().parent.parent


# ── CHANNEL_MAP silent-default routing ────────────────────────────────────


# Actionable events that Mike said "kill entirely — I see everything"
_MUST_BE_BELL_ONLY = [
    "cs_draft_ready", "rfq_arrived", "quote_won", "po_received",
    "buyer_replied", "email_permanent_failure", "order_delivered",
    "all_delivered", "line_shipped", "line_delivered",
    "auto_draft_ready", "outbox_stale", "voice_call_placed",
    "cs_call_placed", "invoice_unpaid", "delivery_confirmed",
    "order_digest", "cross_sell_weekly", "scprs_pull_done",
    "award_loss_detected", "award_loss_margin_too_high",
    "server_error", "deploy_health_failed", "quote_lost_signal",
]


# Events Mike explicitly ratified for Telegram
_MUST_INCLUDE_TELEGRAM = [
    "oracle_weekly", "award_tracker_idle", "loss_pattern_detected",
    "external_service_disconnected", "scprs_pull_failed_persistent",
    "gmail_oauth_expired", "twilio_unreachable",
    "oracle_weekly_failed", "oracle_weekly_never_sent",
    "oracle_weekly_overdue", "oracle_weekly_crash",
]


# Catastrophic events that should hit Telegram + SMS
_MUST_INCLUDE_SMS = ["app_down", "ingest_broken", "db_locked_persistent"]


@pytest.mark.parametrize("event_type", _MUST_BE_BELL_ONLY)
def test_silent_default_events_route_bell_only(event_type, monkeypatch):
    """Per Mike's 2026-05-25 directive: the long tail must default to
    bell-only. A regression that re-adds telegram/email/sms to any of
    these would re-flood the inbox/chat."""
    # Disable env-gated channels so we test what _dispatch_alert WOULD
    # route, not what survives the env-config gates.
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("NOTIFY_SMS", "true")
    monkeypatch.setenv("NOTIFY_PHONE", "+15555555555")
    monkeypatch.setenv("NOTIFY_EMAIL", "ops@example.com")
    monkeypatch.setenv("NOTIFY_EMAIL_ALERTS", "true")
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "0")  # disable deploy gate

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    fired = {"telegram": 0, "email": 0, "sms": 0, "bell": 0}

    with patch.object(na, "_send_telegram",
                      side_effect=lambda *a, **kw: fired.__setitem__("telegram", fired["telegram"] + 1) or {"ok": True}), \
         patch.object(na, "_send_alert_email",
                      side_effect=lambda *a, **kw: fired.__setitem__("email", fired["email"] + 1) or {"ok": True}), \
         patch.object(na, "_send_sms",
                      side_effect=lambda *a, **kw: fired.__setitem__("sms", fired["sms"] + 1) or {"ok": True}), \
         patch.object(na, "_push_bell",
                      side_effect=lambda *a, **kw: fired.__setitem__("bell", fired["bell"] + 1) or {"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type=event_type,
            title="t", body="b", urgency="info",
            context={}, channels_override=None,
        )

    assert fired["telegram"] == 0, (
        f"{event_type}: telegram fired — regression on silent-default. "
        f"This event was on Mike's 'kill entirely, I see everything' list."
    )
    assert fired["email"] == 0, f"{event_type}: email fired — regression"
    assert fired["sms"] == 0, f"{event_type}: SMS fired — regression"
    assert fired["bell"] == 1, f"{event_type}: bell didn't fire — audit log gap"


@pytest.mark.parametrize("event_type", _MUST_INCLUDE_TELEGRAM)
def test_worthy_events_route_to_telegram(event_type, monkeypatch):
    """Mike ratified these for Telegram. A regression that drops one to
    bell-only would silently break the worthy-alert pipeline."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "0")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    fired = {"telegram": False}
    with patch.object(na, "_send_telegram",
                      side_effect=lambda *a, **kw: fired.__setitem__("telegram", True) or {"ok": True}), \
         patch.object(na, "_send_alert_email", return_value={"ok": True}), \
         patch.object(na, "_send_sms", return_value={"ok": True}), \
         patch.object(na, "_push_bell", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type=event_type,
            title="t", body="b", urgency="warning",
            context={}, channels_override=None,
        )

    assert fired["telegram"], (
        f"{event_type}: Telegram did not fire — regression on a worthy "
        f"event tier. Either CHANNEL_MAP dropped this event or routing "
        f"silently degraded."
    )


@pytest.mark.parametrize("event_type", _MUST_INCLUDE_SMS)
def test_catastrophic_events_include_sms(event_type, monkeypatch):
    """CATASTROPHIC tier must hit SMS — wakes Mike up regardless of
    deploy window. Dropping SMS for these is a silent regression."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("NOTIFY_SMS", "true")
    monkeypatch.setenv("NOTIFY_PHONE", "+15555555555")
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "0")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    fired = {"sms": False, "telegram": False}
    with patch.object(na, "_send_sms",
                      side_effect=lambda *a, **kw: fired.__setitem__("sms", True) or {"ok": True}), \
         patch.object(na, "_send_telegram",
                      side_effect=lambda *a, **kw: fired.__setitem__("telegram", True) or {"ok": True}), \
         patch.object(na, "_send_alert_email", return_value={"ok": True}), \
         patch.object(na, "_push_bell", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type=event_type,
            title="t", body="b", urgency="urgent",
            context={}, channels_override=None,
        )

    assert fired["sms"], f"{event_type}: catastrophic event didn't fire SMS"
    assert fired["telegram"], f"{event_type}: catastrophic event didn't fire Telegram"


# ── Deploy-window suppression ──────────────────────────────────────────────


def test_deploy_window_degrades_deploy_health_to_bell(monkeypatch):
    """`deploy_health_failed` firing in the deploy window MUST degrade
    to bell-only — that's the canonical noise pattern: a health check
    transiently failing on every boot until the underlying bug is fixed.
    """
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "9999")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    fired = {"telegram": 0, "bell": 0}
    with patch.object(na, "_send_telegram",
                      side_effect=lambda *a, **kw: fired.__setitem__("telegram", fired["telegram"] + 1) or {"ok": True}), \
         patch.object(na, "_push_bell",
                      side_effect=lambda *a, **kw: fired.__setitem__("bell", fired["bell"] + 1) or {"ok": True}), \
         patch.object(na, "_send_alert_email", return_value={"ok": True}), \
         patch.object(na, "_send_sms", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type="deploy_health_failed",
            title="t", body="b", urgency="warning",
            context={}, channels_override=None,
        )

    # deploy_health_failed is bell-only by default in CHANNEL_MAP, but
    # this test exercises the suppression path — confirm Telegram stays
    # silent even if a future PR routed deploy_health_failed to Telegram.
    assert fired["telegram"] == 0, (
        "telegram fired during deploy window for deploy_health_failed"
    )
    assert fired["bell"] == 1, "bell archive must fire"


def test_deploy_window_does_NOT_suppress_liveness_alerts(monkeypatch):
    """Real-data alerts (external_service_disconnected, scprs_pull_*,
    gmail_oauth_expired, oracle_weekly, award_tracker_idle) describe
    OUTPUT state — they don't become noise because we redeployed.
    A 'Gmail silent 4 days' alert is just as true the second after a
    deploy as the minute before.

    2026-05-25 v2: this regression-test pins the scoping fix after the
    initial liveness sweep got incorrectly suppressed inside the deploy
    window.
    """
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "9999")  # always in window

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    real_data_events = [
        "external_service_disconnected",
        "scprs_pull_failed_persistent",
        "gmail_oauth_expired",
        "oracle_weekly",
        "award_tracker_idle",
        "loss_pattern_detected",
    ]
    for ev in real_data_events:
        fired = {"telegram": False}
        with patch.object(na, "_send_telegram",
                          side_effect=lambda *a, **kw: fired.__setitem__("telegram", True) or {"ok": True}), \
             patch.object(na, "_push_bell", return_value={"ok": True}), \
             patch.object(na, "_send_alert_email", return_value={"ok": True}), \
             patch.object(na, "_send_sms", return_value={"ok": True}), \
             patch.object(na, "_log_alert", return_value=None):
            na._dispatch_alert(
                event_type=ev,
                title="t", body="b", urgency="warning",
                context={}, channels_override=None,
            )
        assert fired["telegram"], (
            f"{ev}: deploy-window incorrectly suppressed a real-data alert. "
            f"Deploy doesn't make stale output suddenly fresh."
        )


def test_deploy_window_does_not_suppress_urgent(monkeypatch):
    """CATASTROPHIC events (urgency='urgent') must bypass the
    deploy-window gate — Mike needs to be paged regardless."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("NOTIFY_SMS", "true")
    monkeypatch.setenv("NOTIFY_PHONE", "+15555555555")
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "9999")  # always in window

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    fired = {"telegram": False, "sms": False}
    with patch.object(na, "_send_telegram",
                      side_effect=lambda *a, **kw: fired.__setitem__("telegram", True) or {"ok": True}), \
         patch.object(na, "_send_sms",
                      side_effect=lambda *a, **kw: fired.__setitem__("sms", True) or {"ok": True}), \
         patch.object(na, "_send_alert_email", return_value={"ok": True}), \
         patch.object(na, "_push_bell", return_value={"ok": True}), \
         patch.object(na, "_log_alert", return_value=None):
        na._dispatch_alert(
            event_type="app_down",
            title="t", body="b", urgency="urgent",
            context={}, channels_override=None,
        )

    assert fired["telegram"], "urgent event suppressed by deploy window — bug"
    assert fired["sms"], "urgent event missed SMS during deploy window"


def test_in_deploy_window_helper(monkeypatch):
    """The helper itself must respect the NOTIFY_DEPLOY_WINDOW_S env var."""
    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "0")
    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)
    assert na._in_deploy_window() is False, "0-second window must immediately exit"

    monkeypatch.setenv("NOTIFY_DEPLOY_WINDOW_S", "9999")
    importlib.reload(na)
    assert na._in_deploy_window() is True, "9999-second window must report active"


# ── CI guard: status emitters must use notify_agent ────────────────────────


# Files that LEGITIMATELY call gmail_api.send_message directly:
#  - notify_agent.py implements the email channel (the seam itself)
#  - gmail_api.py is the library
#  - Operator outbound: vendor / buyer / agency emails (real recipients)
#
# Adding a file to this allowlist is a substrate-tier decision. Justify
# the addition in the PR — every new entry is a place we lose Telegram
# routing.
_ALLOWED_DIRECT_GMAIL_CALLERS = {
    "src/agents/notify_agent.py",
    "src/core/gmail_api.py",
    "src/agents/email_poller.py",
    "src/agents/vendor_ordering_agent.py",
    "src/api/modules/routes_rfq.py",
    "src/api/modules/routes_pricecheck_admin.py",
    "src/api/modules/routes_pricecheck_gen.py",
    "src/api/modules/routes_analytics.py",
}


def test_status_emitters_use_notify_agent_not_direct_gmail():
    """CI guard. Files outside the allowlist must not call
    gmail_api.send_message directly — they must go through
    notify_agent.send_alert so CHANNEL_MAP routing applies.

    Failure means a status emitter is leaking past the routing layer —
    add it to the migration list, route via notify_agent, or (if it's a
    real operator-outbound flow) add it to _ALLOWED_DIRECT_GMAIL_CALLERS
    with a one-line justification in the PR.
    """
    src_dir = _REPO / "src"
    violations = []
    pat = re.compile(r"gmail_api\.send_message\s*\(")
    for path in src_dir.rglob("*.py"):
        rel = str(path.relative_to(_REPO)).replace("\\", "/")
        if rel in _ALLOWED_DIRECT_GMAIL_CALLERS:
            continue
        try:
            body = path.read_text(encoding="utf-8")
        except Exception:
            continue
        if pat.search(body):
            violations.append(rel)

    assert not violations, (
        "status emitters calling gmail_api.send_message directly — "
        "they bypass CHANNEL_MAP and re-flood the inbox. Route via "
        "notify_agent.send_alert(event_type=..., ...) instead, OR add "
        "to _ALLOWED_DIRECT_GMAIL_CALLERS with justification. "
        f"Violations: {violations}"
    )


def test_startup_checks_no_longer_calls_gmail_api_directly():
    """Lock the specific 2026-05-25 deploy-health email leak. The two
    `Deploy Health: 1 check(s) failed` emails in Mike's inbox came
    from this exact callsite — removed in the silent-default substrate
    PR. A regression putting it back would re-create the problem."""
    body = (_REPO / "src" / "core" / "startup_checks.py").read_text(encoding="utf-8")
    assert "gmail_api.send_message" not in body, (
        "regression: startup_checks.py reintroduced direct gmail_api "
        "send — deploy-health emails will flood the inbox again. The "
        "fix is to go through notify_agent.send_alert with "
        "event_type='deploy_health_failed' (bell-only by default + "
        "deploy-window suppressed)."
    )
    assert "deploy_health_failed" in body, (
        "regression: startup_checks.py stopped emitting "
        "deploy_health_failed events entirely — bell archive lost the "
        "diagnostic signal"
    )
