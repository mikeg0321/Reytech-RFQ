"""Pin: notification cooldown survives process restart via JSON persistence.

Chrome MCP audit 2026-05-26 anomaly #3: `deadline_critical` fired 3,596
events / 30d for one bid despite explicit `cooldown_seconds=3600`. Root
cause: `_cooldown` was an in-memory dict; every Railway redeploy reset
it, allowing each fresh boot to re-fire every overdue bid's alert.

These tests pin:
  1. Hydration: a cooldown stamped before a "restart" (clear in-memory
     + flip _cooldown_hydrated False) is still active after restart.
  2. Persistence: each `_is_cooled_down` write persists to disk so a
     restart can find it.
  3. Snooze persists across "restart" with the negative-timestamp
     semantics intact.
  4. Disk failure is non-fatal — in-memory still works for the run.
  5. Hydration is idempotent — second call is a no-op.
"""
from __future__ import annotations

import json
import os
import time


def _isolate_cooldown_file(monkeypatch, tmp_path):
    """Point notify_agent at a tmp cooldown file + reset state."""
    cd_file = str(tmp_path / "notification_cooldowns.json")
    monkeypatch.setattr(
        "src.agents.notify_agent._cooldown_file_path",
        lambda: cd_file,
    )
    import src.agents.notify_agent as na
    na._reset_cooldowns_for_test()
    na._cooldown_hydrated = False  # force re-hydration on next call
    return cd_file, na


# ─── Hydration / persistence round-trip ──────────────────────────────


def test_cooldown_persists_across_simulated_restart(monkeypatch, tmp_path):
    """Stamp a key, simulate a restart, verify the cooldown still
    blocks the alert from re-firing within the TTL."""
    cd_file, na = _isolate_cooldown_file(monkeypatch, tmp_path)

    # First call — should fire (cools down).
    assert na._is_cooled_down("deadline_critical:doc_X", ttl_seconds=3600) is True

    # File should now exist with the key.
    assert os.path.exists(cd_file)
    with open(cd_file) as fh:
        on_disk = json.load(fh)
    assert "deadline_critical:doc_X" in on_disk

    # Simulate restart: nuke in-memory state but leave the file.
    na._reset_cooldowns_for_test()
    na._cooldown_hydrated = False

    # Pre-fix behavior would let this fire again immediately. Post-fix
    # the file hydrates and the cooldown still blocks.
    assert na._is_cooled_down("deadline_critical:doc_X", ttl_seconds=3600) is False, (
        "cooldown did not survive restart — in-memory dict was reset and "
        "the persisted file was not hydrated. This is the prod bug."
    )


def test_cooldown_expires_normally_after_ttl(monkeypatch, tmp_path):
    """After the TTL, the persisted cooldown lets the alert fire again
    — the persistence doesn't make the cooldown permanent."""
    cd_file, na = _isolate_cooldown_file(monkeypatch, tmp_path)

    # Stamp at t=0
    fake_t = [1_000_000.0]
    def _now(): return fake_t[0]
    assert na._is_cooled_down("k", ttl_seconds=3600, _now_fn=_now) is True

    # Within TTL — still blocked.
    fake_t[0] = 1_000_000.0 + 1000  # 1000s later
    assert na._is_cooled_down("k", ttl_seconds=3600, _now_fn=_now) is False

    # Past TTL — fires again.
    fake_t[0] = 1_000_000.0 + 3700  # 3700s later
    assert na._is_cooled_down("k", ttl_seconds=3600, _now_fn=_now) is True


def test_snooze_persists_across_restart(monkeypatch, tmp_path):
    """A snooze marker (negative timestamp) hydrates correctly and
    continues to suppress alerts after restart."""
    cd_file, na = _isolate_cooldown_file(monkeypatch, tmp_path)

    fake_t = [2_000_000.0]
    def _now(): return fake_t[0]

    # Snooze for 24h
    result = na.snooze_alert("scprs_pull_failed_persistent", hours=24, _now_fn=_now)
    assert result["snoozed_until"] == 2_000_000.0 + 24 * 3600

    # Simulate restart
    na._reset_cooldowns_for_test()
    na._cooldown_hydrated = False

    # 1h later — still snoozed (post-restart hydration).
    fake_t[0] = 2_000_000.0 + 3600
    assert na._is_cooled_down(
        "scprs_pull_failed_persistent", ttl_seconds=3600, _now_fn=_now,
    ) is False, "snooze marker did not survive restart"

    # 25h later — snooze expired, fires.
    fake_t[0] = 2_000_000.0 + 25 * 3600
    assert na._is_cooled_down(
        "scprs_pull_failed_persistent", ttl_seconds=3600, _now_fn=_now,
    ) is True


def test_hydration_is_idempotent(monkeypatch, tmp_path):
    """Hydration runs at most once per process — second call is a no-op,
    doesn't re-overwrite in-memory writes that happened since."""
    cd_file, na = _isolate_cooldown_file(monkeypatch, tmp_path)

    # Pre-seed the file with a key.
    with open(cd_file, "w") as fh:
        json.dump({"pre-existing": 999.0}, fh)

    # First call hydrates it in.
    na._hydrate_cooldowns_once()
    assert na._cooldown.get("pre-existing") == 999.0

    # Mutate in-memory after hydration.
    na._cooldown["pre-existing"] = 1234.0

    # Second hydrate should NOT overwrite the in-memory value.
    na._hydrate_cooldowns_once()
    assert na._cooldown.get("pre-existing") == 1234.0


def test_disk_failure_is_non_fatal(monkeypatch, tmp_path):
    """If persistence raises (e.g., disk full, permissions), the alert
    path continues to work — degrades to in-memory-only for this run."""
    _isolate_cooldown_file(monkeypatch, tmp_path)
    import src.agents.notify_agent as na

    # Make persist explode.
    def _boom():
        raise RuntimeError("disk on fire")
    monkeypatch.setattr(na, "_persist_cooldowns_locked", _boom)

    # Should not raise — the alert path swallows persistence failures.
    # First call without persistence still acts as cooldown-stamp in-memory.
    try:
        # Manually drive the cooldown path; persistence is monkeypatched
        # to raise, but the cooldown semantics still hold.
        with na._cooldown_lock:
            na._cooldown["k"] = time.time()
        # In-memory dedup still works.
        assert na._is_cooled_down("k", ttl_seconds=3600) is False
    except Exception as e:
        # The persist call inside _is_cooled_down should be wrapped — if
        # it propagates, the test catches the regression.
        raise AssertionError(
            f"persistence failure leaked from alert path: {e}"
        )


def test_corrupt_file_does_not_crash_hydration(monkeypatch, tmp_path):
    """A garbage cooldown file (truncated JSON, bad types) hydrates to
    empty without crashing — same robustness as the other data/*.json
    callers in the dashboard."""
    cd_file, na = _isolate_cooldown_file(monkeypatch, tmp_path)
    with open(cd_file, "w") as fh:
        fh.write("{not valid json")

    # Should not raise; should mark hydrated; in-memory stays empty.
    na._hydrate_cooldowns_once()
    assert na._cooldown_hydrated is True
    assert na._cooldown == {}
