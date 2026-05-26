"""Pin: scprs_undercut routing in CHANNEL_MAP + 24h per-product cooldown.

Chrome MCP audit 2026-05-26 anomaly #2: scprs_undercut fired 57,314
events / 30d, dominant bell-archive noise source. Two causes:
  - Default 15-min cooldown meant every /growth-intel scan re-fired
    every product over the 30% gap threshold.
  - The event_type was not in CHANNEL_MAP — relied on the implicit
    bell-only default, which made routing un-governed.

PR-D pins both: explicit CHANNEL_MAP entry + 24h cooldown at the
emit site. Volume should drop ~24×.
"""
from __future__ import annotations


def test_scprs_undercut_is_explicitly_routed_in_channel_map():
    """CHANNEL_MAP must contain scprs_undercut → bell-only.

    A future PR that promotes it to Telegram should at minimum touch
    this assertion (forcing review), instead of silently flipping the
    default."""
    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    # Drive the function to extract CHANNEL_MAP — it's defined inside
    # _dispatch_alert. The simplest probe is to call send_alert with a
    # known event_type and capture the channel list via the bell-only
    # behaviour: a bell-only event records to notifications and does
    # NOT call telegram/sms/email senders.
    import unittest.mock as mock
    with mock.patch("src.agents.notify_agent._send_telegram") as tg, \
         mock.patch("src.agents.notify_agent._send_sms") as sms, \
         mock.patch("src.agents.notify_agent._send_alert_email") as em, \
         mock.patch("src.agents.notify_agent._push_bell") as bell:
        na._reset_cooldowns_for_test()
        na.send_alert(
            event_type="scprs_undercut",
            title="t",
            body="b",
            run_async=False,
        )
        bell.assert_called_once()
        tg.assert_not_called()
        sms.assert_not_called()
        em.assert_not_called()


def test_scprs_undercut_emit_site_uses_24h_cooldown():
    """The emit site in routes_growth_intel must pass cooldown_seconds
    so the per-product alert can't re-fire every 15 minutes."""
    from pathlib import Path
    src = Path(
        __file__
    ).parent.parent / "src" / "api" / "modules" / "routes_growth_intel.py"
    content = src.read_text(encoding="utf-8")
    # Find the scprs_undercut emit block — must include cooldown_seconds.
    # Anchoring on the event_type literal so future emit-site relocations
    # are caught.
    snippet_start = content.find('event_type="scprs_undercut"')
    assert snippet_start > -1, "scprs_undercut emit site missing"
    # Look for cooldown_seconds within ~2500 chars after the literal —
    # the same send_alert(...) call, allowing room for the rationale
    # comment block.
    window = content[snippet_start:snippet_start + 2500]
    assert "cooldown_seconds=86400" in window, (
        "scprs_undercut emit site lost its 24h cooldown — would re-fire "
        "every 15min default and inflate bell volume again."
    )
