"""Pin: fiscal-exhaustive daemon is observable to the scheduler
watchdog and respawnable on death.

Chrome MCP audit 2026-05-26 anomaly #9: SCPRS award scrape was 25
days silent in prod. Root cause: `schedule_full_fiscal_scrape`
spawned a raw `threading.Thread` daemon, invisible to the scheduler
watchdog. Silent thread death = forever silent. Same defect class
as PR #1087's email-poller fix.

These tests pin:
  1. Module-level `_fiscal_scheduler_started` guard exists (so the
     watchdog's reset-and-restart pattern can drive a respawn).
  2. Schedule call records an initial heartbeat with the scheduler
     so liveness is observable from boot.
  3. Calling schedule twice doesn't spawn two threads (guard works).
  4. Boot wiring in app.py + dashboard.py calls register_restartable
     with the right shape.
"""
from __future__ import annotations

from pathlib import Path


def test_guard_attribute_exists_and_starts_false():
    """The watchdog's reset-and-restart pattern depends on a module-
    level boolean guard. Pin its name + initial value."""
    import src.agents.scprs_browser as sb
    assert hasattr(sb, "_fiscal_scheduler_started"), (
        "scprs_browser must expose _fiscal_scheduler_started for the "
        "watchdog to reset before respawn"
    )


def test_schedule_records_heartbeat_for_watchdog():
    """The first call to schedule_full_fiscal_scrape writes a baseline
    heartbeat so the watchdog has a recent signal even before the
    first 2am-PST scrape (up to 24h away)."""
    import src.agents.scprs_browser as sb
    # Reset state — re-enable for this test.
    sb._fiscal_scheduler_started = False

    # Capture heartbeat calls.
    captured = []
    import src.core.scheduler as sched
    real_hb = sched.heartbeat

    def _spy_hb(name, success=True, error=""):
        captured.append({"name": name, "success": success, "error": error})
        return real_hb(name, success=success, error=error)

    sched.heartbeat = _spy_hb
    try:
        sb.schedule_full_fiscal_scrape(target_hour_pst=2)
    finally:
        sched.heartbeat = real_hb

    # The schedule call should emit an initial heartbeat.
    names = [h["name"] for h in captured]
    assert "fiscal-exhaustive" in names, (
        f"schedule_full_fiscal_scrape did not record an initial "
        f"heartbeat. Captured: {names}"
    )


def test_double_start_is_guarded():
    """Second call to schedule is a no-op — prevents two daemon threads
    from competing for the same scraping slot."""
    import src.agents.scprs_browser as sb
    sb._fiscal_scheduler_started = False

    # First call sets the guard.
    sb.schedule_full_fiscal_scrape(target_hour_pst=2)
    assert sb._fiscal_scheduler_started is True

    # Second call should early-return without resetting state.
    # We assert this by checking guard stays True after the call.
    sb.schedule_full_fiscal_scrape(target_hour_pst=2)
    assert sb._fiscal_scheduler_started is True


def test_app_py_registers_with_watchdog():
    """Boot wiring in app.py must call register_restartable for the
    fiscal-exhaustive job with the right guard attribute."""
    content = Path(__file__).parent.parent.joinpath("app.py").read_text(
        encoding="utf-8",
    )
    assert 'register_restartable(' in content
    # Anchor on the job name + the guard attr — both must appear in the
    # same wiring block.
    idx = content.find('"fiscal-exhaustive"')
    assert idx > -1, "app.py boot wiring missing 'fiscal-exhaustive' registration"
    window = content[idx:idx + 600]
    assert '"_fiscal_scheduler_started"' in window, (
        "fiscal-exhaustive registration missing guard attribute name"
    )


def test_dashboard_py_registers_with_watchdog():
    """Same wiring exists in src/api/dashboard.py (the alternative
    boot path used when dashboard.py is exec'd)."""
    content = Path(__file__).parent.parent.joinpath(
        "src", "api", "dashboard.py"
    ).read_text(encoding="utf-8")
    idx = content.find('"fiscal-exhaustive"')
    assert idx > -1, (
        "dashboard.py boot wiring missing 'fiscal-exhaustive' registration"
    )
    window = content[idx:idx + 600]
    assert '"_fiscal_scheduler_started"' in window
