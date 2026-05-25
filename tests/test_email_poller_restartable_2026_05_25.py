"""Email poller silent-death substrate fix — 2026-05-25.

The email-poller thread is started in routes_crm.start_polling() as a
daemon `threading.Thread`. It calls scheduler.heartbeat() each cycle but
was NOT registered with `register_restartable()`, so when the thread
died (uncaught exception past the try/except, OS thread death, OAuth
refresh deadlock), the watchdog had no `restart_func` to call. The
2026-05-25 liveness sweep caught the symptom: 4 days silent inbound
with 3 unread CDCR RFQs sitting in the sales inbox.

Compare: 4 other daemons in dashboard.py use this pattern — follow-up-
engine, award-tracker, oracle-weekly-report, cross-sell-weekly-digest.
The email poller was the outlier.

These tests lock the fix:
  - The source code of start_polling MUST call register_restartable with
    name="email-poller" and guard_attr="_poll_started".
  - The scheduler primitive itself (register_restartable + the watchdog
    death-detection path) revives a dead thread by resetting the guard
    and re-invoking start_func.
"""

import sys
import threading
import time
import types
from datetime import datetime, timezone, timedelta
from pathlib import Path


def test_start_polling_source_registers_email_poller():
    """Substrate-shape regression: a future PR must not silently delete
    the register_restartable call from start_polling. Read the source —
    routes_crm.py is exec'd into dashboard.py's namespace, so we can't
    cleanly import the function in isolation."""
    src = Path("src/api/modules/routes_crm.py").read_text(encoding="utf-8")

    # The block must contain both the call and the right arguments.
    assert "register_restartable" in src, (
        "register_restartable import/call missing from routes_crm.py — "
        "the email-poller restart substrate has regressed."
    )
    assert '"email-poller"' in src and '"_poll_started"' in src, (
        'register_restartable call must use name="email-poller" and '
        'guard_attr="_poll_started" to match start_polling\'s guard.'
    )

    # And the call site must live inside start_polling (not some
    # unrelated helper that doesn't actually run at boot).
    sp_start = src.index("def start_polling(")
    sp_end = src.index("\ndef ", sp_start + 1)
    sp_body = src[sp_start:sp_end]
    assert "register_restartable" in sp_body, (
        "register_restartable call is outside start_polling — the "
        "registration won't actually fire at app boot."
    )


def test_register_restartable_revives_a_dead_thread():
    """Walk the scheduler substrate end-to-end with a stub module:
    register a job, mark a dead thread, run restart_dead_jobs, assert
    the guard was reset and start_func re-invoked."""
    # Fresh scheduler state — the in-process registry is module-global,
    # so we wipe it inside the test to avoid bleed-through.
    from src.core import scheduler
    with scheduler._lock:
        scheduler._jobs.clear()

    # Stub module that mimics routes_crm: an _poll_started flag + a
    # start_func that resets a side-effect counter.
    stub = types.ModuleType("stub_poller_module")
    stub._poll_started = True
    stub.restart_count = 0

    def fake_start():
        stub.restart_count += 1
        stub._poll_started = True

    # The job has a thread that has never been .start()'d → is_alive=False.
    dead_thread = threading.Thread(target=lambda: None)
    # Don't start it — we want is_alive() to return False.

    # Register first (this sets restart_func + max_restarts on the JobInfo).
    scheduler.register_restartable("test-poller", interval_sec=10,
                                   module=stub, guard_attr="_poll_started",
                                   start_func=fake_start)
    # Then seed JobInfo with an old heartbeat + the dead thread.
    with scheduler._lock:
        job = scheduler._jobs["test-poller"]
        job.thread = dead_thread
        job.status = "running"
        job.started_at = datetime.now(timezone.utc).isoformat()
        # last_run "60s ago" — well past 3x interval (30s).
        job.last_run = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()

    restarted, exhausted = scheduler.restart_dead_jobs()

    assert "test-poller" in restarted, (
        f"watchdog didn't restart the dead poller: restarted={restarted}, "
        f"exhausted={exhausted}"
    )
    assert stub.restart_count == 1, (
        f"start_func was not re-invoked exactly once "
        f"(got {stub.restart_count} calls)"
    )
    # Guard was reset to False, then start_func set it back to True.
    assert stub._poll_started is True, "guard flag not in expected post-restart state"


def test_register_restartable_respects_max_restarts():
    """If a job keeps dying, the watchdog stops after max_restarts and
    moves it to exhausted — prevents infinite restart spam."""
    from src.core import scheduler
    with scheduler._lock:
        scheduler._jobs.clear()

    stub = types.ModuleType("stub_dying_module")
    stub._guard = True
    stub.attempts = 0

    def fake_start():
        stub.attempts += 1
        stub._guard = True  # restart sets guard back to True

    scheduler.register_restartable("dying-job", interval_sec=10,
                                   module=stub, guard_attr="_guard",
                                   start_func=fake_start, max_restarts=2)

    # Run 3 restart cycles. Each time we re-seed the JobInfo as dead.
    for cycle in range(3):
        with scheduler._lock:
            job = scheduler._jobs["dying-job"]
            job.thread = threading.Thread(target=lambda: None)  # never started → dead
            job.status = "running"
            job.started_at = datetime.now(timezone.utc).isoformat()
            job.last_run = (datetime.now(timezone.utc) -
                            timedelta(seconds=60)).isoformat()
        restarted, exhausted = scheduler.restart_dead_jobs()
        # Cycles 0 + 1 should restart; cycle 2 should exhaust.
        if cycle < 2:
            assert "dying-job" in restarted, (
                f"cycle {cycle}: expected restart but got "
                f"restarted={restarted}, exhausted={exhausted}"
            )
        else:
            assert "dying-job" in exhausted, (
                f"cycle {cycle}: expected exhaustion at max_restarts=2 but "
                f"got restarted={restarted}, exhausted={exhausted}"
            )

    assert stub.attempts == 2, (
        f"start_func called {stub.attempts} times — should max out at 2 "
        f"and stop. Without the cap, a dead-on-arrival daemon would spam."
    )
