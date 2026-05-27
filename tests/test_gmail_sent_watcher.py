"""Tests for gmail_sent_watcher — the periodic poller that fires
the same post-send pipeline as the in-app Mark-Sent button for
outbound Gmail messages matching existing PC/RFQ records.

Test surface (no real Gmail / no real DB writes):
  - One match → fired exactly once
  - Already-attached match → skipped (idempotency)
  - Mixed batch → fired count and skipped count both correct
  - Per-match failure → other matches still fire (no cascade kill)
  - detect_observed_sends ok=False → returns ok=False, no fire calls
  - Unknown kind → counted as failure, no exception escapes
  - The refactored mark-sent helpers accept the new payload/uploaded kwargs
"""
from __future__ import annotations

import pytest

from src.agents import gmail_sent_watcher as gsw


# ─── _fire_mark_sent_for_match — verify it's wired correctly ──────────


class _FakeApp:
    """Minimal Flask-app stand-in for app_context()."""

    def app_context(self):
        return _FakeAppCtx()


class _FakeAppCtx:
    def __enter__(self): return self
    def __exit__(self, *a): return False


def test_fire_calls_rfq_locked_for_rfq_kind(monkeypatch):
    """An RFQ match calls _api_rfq_mark_sent_manually_locked with the
    refactored payload kwargs."""
    called = {}

    def fake_rfq_locked(rid, *, payload=None, uploaded=None):
        called["rid"] = rid
        called["payload"] = payload
        called["uploaded"] = uploaded
        return ({"ok": True}, 200)

    class _NullLock:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    # Patch the module-paths the watcher imports lazily.
    import src.api.modules.routes_rfq_admin as ram
    import src.api.data_layer as dl
    monkeypatch.setattr(
        ram, "_api_rfq_mark_sent_manually_locked", fake_rfq_locked
    )
    monkeypatch.setattr(dl, "_save_rfqs_lock", _NullLock())

    match = {
        "matched_record_id": "rfq_abc",
        "matched_record_kind": "rfq",
        "gmail_message_id": "gm_1",
        "to": "buyer@cchcs.ca.gov",
        "date": "2026-05-26T10:00:00Z",
    }
    gsw._fire_mark_sent_for_match(match, app=_FakeApp())

    assert called["rid"] == "rfq_abc"
    assert called["payload"]["sent_to"] == "buyer@cchcs.ca.gov"
    assert called["payload"]["sent_at"] == "2026-05-26T10:00:00Z"
    assert "gm_1" in called["payload"]["notes"]
    assert called["uploaded"] is None


def test_fire_calls_pc_locked_for_pc_kind(monkeypatch):
    called = {}

    def fake_pc_locked(pcid, *, payload=None, uploaded=None):
        called["pcid"] = pcid
        called["payload"] = payload
        return ({"ok": True}, 200)

    class _NullLock:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    import src.api.modules.routes_pricecheck_pricing as rpp
    import src.api.data_layer as dl
    monkeypatch.setattr(
        rpp, "_api_pricecheck_mark_sent_manually_locked", fake_pc_locked
    )
    monkeypatch.setattr(dl, "_save_pcs_lock", _NullLock())

    match = {
        "matched_record_id": "pc_xyz",
        "matched_record_kind": "pc",
        "gmail_message_id": "gm_2",
        "to": "buyer@cchcs.ca.gov",
        "date": "2026-05-26T11:00:00Z",
    }
    gsw._fire_mark_sent_for_match(match, app=_FakeApp())

    assert called["pcid"] == "pc_xyz"
    assert called["payload"]["sent_to"] == "buyer@cchcs.ca.gov"


def test_fire_raises_on_unknown_kind():
    match = {
        "matched_record_id": "?_999",
        "matched_record_kind": "bogus",
        "gmail_message_id": "gm_3",
    }
    with pytest.raises(ValueError):
        gsw._fire_mark_sent_for_match(match, app=_FakeApp())


# ─── run_watcher_once — the orchestrator ─────────────────────────────


def _stub_detect(matches=None, ok=True, error=None, scanned=None):
    """Return a detect_fn that produces a fixed response."""
    matches = list(matches or [])

    def _fn(*, since_days, max_messages):
        if not ok:
            return {"ok": False, "error": error or "stub", "scanned": 0,
                    "matches": [], "unmatched": [], "skipped_non_quote": 0}
        return {
            "ok": True,
            "scanned": scanned if scanned is not None else len(matches),
            "matches": matches,
            "unmatched": [],
            "skipped_non_quote": 0,
        }
    return _fn


def test_run_watcher_no_matches():
    r = gsw.run_watcher_once(detect_fn=_stub_detect(matches=[]))
    assert r["ok"] is True
    assert r["scanned"] == 0
    assert r["matched"] == 0
    assert r["new_matches"] == 0
    assert r["fired"] == 0
    assert r["fire_failures"] == 0


def test_run_watcher_one_new_match_fires_once():
    fired = []

    def fake_fire(match, *, app=None):
        fired.append(match.get("matched_record_id"))

    r = gsw.run_watcher_once(
        detect_fn=_stub_detect(matches=[{
            "matched_record_id": "rfq_a",
            "matched_record_kind": "rfq",
            "already_attached": False,
            "gmail_message_id": "g1",
        }]),
        fire_fn=fake_fire,
    )
    assert r["new_matches"] == 1
    assert r["fired"] == 1
    assert r["fire_failures"] == 0
    assert fired == ["rfq_a"]


def test_run_watcher_already_attached_skipped():
    fired = []

    def fake_fire(match, *, app=None):
        fired.append(match.get("matched_record_id"))

    r = gsw.run_watcher_once(
        detect_fn=_stub_detect(matches=[{
            "matched_record_id": "rfq_a",
            "matched_record_kind": "rfq",
            "already_attached": True,
            "gmail_message_id": "g1",
        }]),
        fire_fn=fake_fire,
    )
    assert r["matched"] == 1
    assert r["already_attached"] == 1
    assert r["new_matches"] == 0
    assert r["fired"] == 0
    assert fired == []  # nothing fired


def test_run_watcher_per_match_failure_doesnt_block_others():
    """One bad match must not kill the loop — the other matches still fire."""
    fired = []
    fail_for = "rfq_bad"

    def fake_fire(match, *, app=None):
        if match.get("matched_record_id") == fail_for:
            raise RuntimeError("boom")
        fired.append(match.get("matched_record_id"))

    r = gsw.run_watcher_once(
        detect_fn=_stub_detect(matches=[
            {"matched_record_id": "rfq_a", "matched_record_kind": "rfq",
             "already_attached": False, "gmail_message_id": "g1"},
            {"matched_record_id": "rfq_bad", "matched_record_kind": "rfq",
             "already_attached": False, "gmail_message_id": "g2"},
            {"matched_record_id": "rfq_c", "matched_record_kind": "rfq",
             "already_attached": False, "gmail_message_id": "g3"},
        ]),
        fire_fn=fake_fire,
    )
    assert r["new_matches"] == 3
    assert r["fired"] == 2
    assert r["fire_failures"] == 1
    assert set(fired) == {"rfq_a", "rfq_c"}
    assert len(r["errors"]) == 1
    assert r["errors"][0]["matched_record_id"] == "rfq_bad"


def test_run_watcher_propagates_detect_failure():
    r = gsw.run_watcher_once(detect_fn=_stub_detect(ok=False, error="gmail down"))
    assert r["ok"] is False
    assert "gmail down" in str(r["errors"])
    assert r["fired"] == 0


def test_run_watcher_handles_mixed_batch():
    """Already-attached + new + failure all in one cycle."""
    def fake_fire(match, *, app=None):
        if match.get("matched_record_id") == "rfq_fail":
            raise RuntimeError("nope")

    r = gsw.run_watcher_once(
        detect_fn=_stub_detect(matches=[
            {"matched_record_id": "rfq_skip", "matched_record_kind": "rfq",
             "already_attached": True, "gmail_message_id": "g1"},
            {"matched_record_id": "rfq_ok", "matched_record_kind": "rfq",
             "already_attached": False, "gmail_message_id": "g2"},
            {"matched_record_id": "rfq_fail", "matched_record_kind": "rfq",
             "already_attached": False, "gmail_message_id": "g3"},
        ], scanned=42),
        fire_fn=fake_fire,
    )
    assert r["ok"] is True
    assert r["scanned"] == 42
    assert r["matched"] == 3
    assert r["already_attached"] == 1
    assert r["new_matches"] == 2
    assert r["fired"] == 1
    assert r["fire_failures"] == 1


# ─── Refactor regression: the mark-sent helpers accept the new kwargs ──


def test_rfq_mark_sent_helper_accepts_payload_kwarg(monkeypatch):
    """PR #9 refactor: _api_rfq_mark_sent_manually_locked now takes
    `payload` and `uploaded` kwargs instead of reading request.* itself.
    This is the seam the watcher uses — pin it so a future refactor
    can't break the watcher silently."""
    import inspect
    from src.api.modules.routes_rfq_admin import (
        _api_rfq_mark_sent_manually_locked,
    )
    sig = inspect.signature(_api_rfq_mark_sent_manually_locked)
    params = sig.parameters
    assert "payload" in params, (
        "_api_rfq_mark_sent_manually_locked must accept `payload` kwarg "
        "(PR #9 refactor — gmail_sent_watcher relies on it)"
    )
    assert "uploaded" in params, (
        "_api_rfq_mark_sent_manually_locked must accept `uploaded` kwarg"
    )
    assert params["payload"].kind == inspect.Parameter.KEYWORD_ONLY
    assert params["uploaded"].kind == inspect.Parameter.KEYWORD_ONLY


def test_pc_mark_sent_helper_accepts_payload_kwarg():
    import inspect
    from src.api.modules.routes_pricecheck_pricing import (
        _api_pricecheck_mark_sent_manually_locked,
    )
    sig = inspect.signature(_api_pricecheck_mark_sent_manually_locked)
    params = sig.parameters
    assert "payload" in params
    assert "uploaded" in params
    assert params["payload"].kind == inspect.Parameter.KEYWORD_ONLY
    assert params["uploaded"].kind == inspect.Parameter.KEYWORD_ONLY


# ─── start_scheduler — idempotent registration ────────────────────────


def test_start_scheduler_is_idempotent(monkeypatch):
    """Calling start_scheduler twice shouldn't spawn two threads."""
    # Reset module state for clean test isolation.
    monkeypatch.setattr(gsw, "_scheduler_started", False)
    monkeypatch.setattr(gsw, "_scheduler_thread", None)

    started_threads = []

    real_thread = __import__("threading").Thread

    def fake_thread(*a, **kw):
        t = real_thread(*a, **kw)
        # Don't actually start the loop — replace target with no-op.
        t._target = lambda: None
        started_threads.append(t)
        return t

    monkeypatch.setattr("threading.Thread", fake_thread)

    gsw.start_scheduler(app=_FakeApp(), interval_sec=999)
    gsw.start_scheduler(app=_FakeApp(), interval_sec=999)  # second call

    assert len(started_threads) == 1, "start_scheduler must be idempotent"


def test_start_scheduler_callable_with_no_args(monkeypatch):
    """start_scheduler() must be callable from a scope where `app` isn't
    bound — that's how dashboard.py spawns it at module-level, BEFORE
    `app = create_app()` runs in app.py. Pre-fix prod log:
    `Gmail-SENT watcher failed to start: name 'app' is not defined`.
    The watcher must lazy-resolve the app at first fire instead."""
    monkeypatch.setattr(gsw, "_scheduler_started", False)
    monkeypatch.setattr(gsw, "_scheduler_thread", None)
    monkeypatch.setattr(gsw, "_scheduler_app", None)

    real_thread = __import__("threading").Thread

    def fake_thread(*a, **kw):
        t = real_thread(*a, **kw)
        t._target = lambda: None  # don't actually start the loop
        return t

    monkeypatch.setattr("threading.Thread", fake_thread)

    # No `app=` kwarg — this is what dashboard.py does post-fix.
    # Must not raise NameError, must not raise TypeError.
    gsw.start_scheduler()

    assert gsw._scheduler_started is True
    # _scheduler_app stays None when no app is passed; the fire path
    # lazy-resolves via `from app import app as _flask_app`.
    assert gsw._scheduler_app is None


def test_fire_lazy_resolves_app_from_app_py(monkeypatch):
    """When no app was captured at start_scheduler time AND no Flask
    request context is active, _fire_mark_sent_for_match must lazy-import
    the global Flask app from `app.py` (where `app = create_app()` lives
    at module-level). Pre-fix the watcher tried `from src.api.dashboard
    import app` — but dashboard.py has no module-level `app`, so the
    fallback raised ImportError on every fire."""
    import sys
    import types

    # Inject a fake `app` module that exposes `app` (mirrors app.py:738).
    fake_app_module = types.ModuleType("app")
    fake_app_module.app = _FakeApp()
    monkeypatch.setitem(sys.modules, "app", fake_app_module)

    # Force the fallback chain: no captured app, no Flask context.
    monkeypatch.setattr(gsw, "_scheduler_app", None)

    # Stub the RFQ-locked side-effect so the test doesn't hit the DB.
    import src.api.modules.routes_rfq_admin as ram
    import src.api.data_layer as dl
    monkeypatch.setattr(
        ram, "_api_rfq_mark_sent_manually_locked",
        lambda rid, *, payload=None, uploaded=None: ({"ok": True}, 200),
    )

    class _NullLock:
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(dl, "_save_rfqs_lock", _NullLock())

    # Should not raise — proves the fallback chain reaches `app.py`.
    gsw._fire_mark_sent_for_match(
        {
            "matched_record_id": "rfq_test",
            "matched_record_kind": "rfq",
            "gmail_message_id": "gid_test",
            "to": "buyer@example.com",
            "date": "2026-05-27T00:00:00Z",
        },
        app=None,
    )
