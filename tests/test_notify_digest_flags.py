"""Verify the three digest schedulers are gated behind feature flags
(default OFF) so Mike doesn't get spammed with stale data.

Bug found 2026-04-27: 61 stale "overdue" PCs, 268 inflated "drafts
waiting", and 5-action-item Order Digest dated 2 days stale. All three
froze counts at scan-time (hours before email send) without re-scanning.
Default-off until queries are fixed.
"""
import importlib
import unittest.mock as mock


def test_stale_watcher_off_by_default(monkeypatch):
    import src.agents.notify_agent as na
    importlib.reload(na)
    # Reset the started flag so we can re-test
    na._stale_watcher_started = False
    with mock.patch("threading.Thread") as mock_thread:
        na.start_stale_watcher()
        mock_thread.assert_not_called()


def test_stale_watcher_on_when_flag_set(monkeypatch):
    import src.agents.notify_agent as na
    importlib.reload(na)
    na._stale_watcher_started = False
    with mock.patch("src.core.feature_flags.get_flag", return_value=True), \
         mock.patch("threading.Thread") as mock_thread:
        na.start_stale_watcher()
        mock_thread.assert_called_once()


def test_daily_digest_off_by_default(monkeypatch):
    import src.agents.notify_agent as na
    importlib.reload(na)
    na._daily_digest_started = False
    with mock.patch("threading.Thread") as mock_thread:
        na.start_daily_digest()
        mock_thread.assert_not_called()


def test_daily_digest_on_when_flag_set(monkeypatch):
    import src.agents.notify_agent as na
    importlib.reload(na)
    na._daily_digest_started = False
    with mock.patch("src.core.feature_flags.get_flag", return_value=True), \
         mock.patch("threading.Thread") as mock_thread:
        na.start_daily_digest()
        mock_thread.assert_called_once()


def test_order_digest_off_by_default(monkeypatch):
    import src.agents.order_digest as od
    importlib.reload(od)
    od._digest_started = False
    with mock.patch("threading.Thread") as mock_thread:
        od.start_order_digest_scheduler()
        mock_thread.assert_not_called()


def test_order_digest_on_when_flag_set(monkeypatch):
    import src.agents.order_digest as od
    importlib.reload(od)
    od._digest_started = False
    with mock.patch("src.core.feature_flags.get_flag", return_value=True), \
         mock.patch("threading.Thread") as mock_thread:
        od.start_order_digest_scheduler()
        mock_thread.assert_called_once()
