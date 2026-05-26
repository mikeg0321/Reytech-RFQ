"""Pin: deadline_critical routes to Telegram (WORTHY tier), not bell-only.

Mr. Wolf 2026-05-26 audit found Mike's 3 active Job #1 inbound RFQs
(Duffey×2, Coleman) went 9.8h OVERDUE without a Telegram alert. Root
cause: deadline_critical event_type had NO entry in CHANNEL_MAP, so
the dispatcher fell through to the default `["bell"]`.

The watcher at notify_agent.py:~970 fires deadline_critical with
urgency="urgent" + per-bid cooldown_key=f"deadline_critical:{doc_id}"
+ 1h cooldown. That part was correct. CHANNEL_MAP was the gap.

Tests pin:
  1. deadline_critical is in CHANNEL_MAP with ["telegram", "bell"]
  2. deadline_critical is NOT in _SUPERSEDING_EVENT_TYPES (deliberate
     — different bids share the event_type, supersede would conflate
     distinct overdue cards into one)
  3. send_alert(event_type="deadline_critical") dispatches to telegram
"""
from __future__ import annotations

import inspect
import re

import pytest


def test_deadline_critical_in_channel_map_telegram_bell():
    """The CHANNEL_MAP entry for deadline_critical must route to both
    telegram and bell. urgency="urgent" alone isn't enough — without
    the map entry the dispatcher falls through to bell-only default.

    CHANNEL_MAP lives inside _dispatch_alert, not send_alert."""
    from src.agents import notify_agent
    src = inspect.getsource(notify_agent._dispatch_alert)
    pat = re.compile(
        r'"deadline_critical"\s*:\s*\[\s*"telegram"\s*,\s*"bell"\s*\]'
    )
    assert pat.search(src), (
        "deadline_critical must be in CHANNEL_MAP with "
        '["telegram", "bell"]. See test_deadline_critical_routing.py '
        "for rationale (Mr. Wolf audit 2026-05-26)."
    )


def test_deadline_critical_not_in_superseding_set():
    """Per-bid deadline cards must NOT supersede each other. Different
    bids share event_type='deadline_critical'; supersede on event_type
    alone would collapse 3 overdue cards into 1, hiding 2 from Mike."""
    from src.agents.notify_agent import _SUPERSEDING_EVENT_TYPES
    assert "deadline_critical" not in _SUPERSEDING_EVENT_TYPES, (
        "deadline_critical is per-bid; supersede on event_type would "
        "conflate distinct overdue bids. Per-bid 1h cooldown handles "
        "same-bid re-spam without supersede."
    )


def test_send_alert_routes_deadline_critical_to_telegram(monkeypatch):
    """End-to-end: send_alert(event_type='deadline_critical', urgency='urgent')
    must call _send_telegram, not just _push_bell."""
    from src.agents import notify_agent as na

    telegram_calls = []
    bell_calls = []

    monkeypatch.setattr(
        na, "_send_telegram",
        lambda et, t, b, u, c: telegram_calls.append(et) or {"ok": True, "message_id": 1},
    )
    monkeypatch.setattr(
        na, "_push_bell",
        lambda et, t, b, u, c: bell_calls.append(et) or {"ok": True},
    )
    # Stub out SMS + email so we don't need credentials in tests
    monkeypatch.setattr(na, "_send_sms",
                        lambda *a, **kw: {"ok": True, "stubbed": True})
    monkeypatch.setattr(na, "_send_alert_email",
                        lambda *a, **kw: {"ok": True, "stubbed": True})
    # Force the telegram channel gate open; the live module reads
    # TELEGRAM_ENABLED at import time, so monkeypatching the module
    # attribute is the right surface.
    monkeypatch.setattr(na, "TELEGRAM_ENABLED", True)

    # run_async=False so the dispatch fires inline + test can assert
    na.send_alert(
        event_type="deadline_critical",
        title="🚨 OVERDUE: 10847187",
        body="9.8h overdue",
        urgency="urgent",
        context={"doc_id": "rfq_89bb9a3e"},
        cooldown_key="deadline_critical:rfq_89bb9a3e:test",
        run_async=False,
    )
    assert telegram_calls == ["deadline_critical"], (
        "deadline_critical must dispatch to telegram. If this fails, "
        "check CHANNEL_MAP has deadline_critical → ['telegram', 'bell']."
    )
    assert bell_calls == ["deadline_critical"], (
        "deadline_critical must also dispatch to bell (archive)."
    )
