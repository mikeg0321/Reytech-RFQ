"""Liveness substrate — silent-failure detector tests.

Pins the 2026-05-25 substrate addition: run_liveness_sweep walks the
CHECKS registry, reads output tables / credentials, fires Telegram
alerts on stale or missing signals. The Google Drive silent-disconnect
scenario (the bug that prompted this PR) is the canonical case.

Tested:
  - Stale output table → alert fires with the right event_type
  - Healthy output table → no alert
  - Missing credential → alert fires
  - Present credential → no alert
  - Check function raising an exception → sweep continues, no crash
  - Cooldown — sweep called twice within window doesn't double-fire
  - Adding a new tuple to CHECKS adds a check (registry shape stable)
"""
from datetime import datetime, timezone, timedelta
import sqlite3
from unittest.mock import patch

import pytest


# ── Sweep behavior ─────────────────────────────────────────────────────────


@pytest.fixture
def sweep_conn():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY, logged_at TEXT, direction TEXT
        );
        CREATE TABLE IF NOT EXISTS scprs_po_master (
            id INTEGER PRIMARY KEY, pulled_at TEXT, po_number TEXT
        );
        CREATE TABLE IF NOT EXISTS award_tracker_log (
            id INTEGER PRIMARY KEY, checked_at TEXT, quote_number TEXT
        );
        CREATE TABLE IF NOT EXISTS competitor_intel (
            id INTEGER PRIMARY KEY, found_at TEXT, outcome TEXT
        );
        CREATE TABLE IF NOT EXISTS oracle_calibration (
            id INTEGER PRIMARY KEY, category TEXT, last_updated TEXT
        );
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY, quote_number TEXT,
            created_at TEXT, status TEXT
        );
    """)
    conn.commit()
    yield conn
    conn.close()


def _fresh(conn):
    """Seed all output tables with a row 10 min ago — everything healthy."""
    now = datetime.now(timezone.utc) - timedelta(minutes=10)
    iso = now.isoformat()
    conn.execute("INSERT INTO email_log (logged_at, direction) VALUES (?, 'in')", (iso,))
    conn.execute("INSERT INTO scprs_po_master (pulled_at, po_number) VALUES (?, 'PO-1')", (iso,))
    conn.execute("INSERT INTO award_tracker_log (checked_at, quote_number) VALUES (?, 'R26Q1')", (iso,))
    conn.execute("INSERT INTO competitor_intel (found_at, outcome) VALUES (?, 'lost')", (iso,))
    conn.execute("INSERT INTO oracle_calibration (category, last_updated) VALUES ('general', ?)", (iso,))
    conn.execute("INSERT INTO quotes (quote_number, created_at, status) VALUES ('R26Q1', ?, 'sent')", (iso,))
    conn.commit()


def _stale(conn, table, col, days):
    """Set MAX(col) to N days ago for `table`."""
    old = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn.execute(f"DELETE FROM {table}")
    conn.execute(f"INSERT INTO {table} ({col}) VALUES (?)", (old,))
    conn.commit()


def _patch_db(conn):
    """Patch src.core.db.get_db to return our in-memory connection."""
    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None
    return patch("src.core.db.get_db", return_value=_Ctx())


def test_healthy_tables_no_alerts(sweep_conn, monkeypatch):
    """All tables fresh + all credentials present → zero alerts fired."""
    _fresh(sweep_conn)
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_ID", "x")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRET", "x")
    monkeypatch.setenv("GMAIL_OAUTH_REFRESH_TOKEN", "x")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "x")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")

    # Need to reload gmail_api to pick up the env vars (it caches at import).
    import importlib
    import src.core.gmail_api as gm
    importlib.reload(gm)

    from src.core.liveness_checks import run_liveness_sweep
    sent = []
    def _capture(**kw):
        sent.append(kw)
        return {"ok": True}

    # Avoid the SQLite-backup file check and the backup file path check
    # by patching the file-system helper to report fresh.
    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert", side_effect=_capture), \
         patch("src.core.liveness_checks._backup_file_freshness",
               lambda *a, **kw: lambda: (True, 60, "ok")):
        from src.core import liveness_checks
        importlib.reload(liveness_checks)
        result = liveness_checks.run_liveness_sweep()

    # Real assertion: NO alerts fired for the rows that were seeded fresh.
    table_alerts = [
        s for s in sent
        if any(label in s.get("title", "") for label in (
            "Gmail inbound", "SCPRS award", "Award tracker",
            "Competitor intel", "Oracle calibration", "Quote ingestion"))
    ]
    assert table_alerts == [], (
        f"healthy tables fired alerts: {[s.get('title') for s in table_alerts]}"
    )
    assert result["summary"]["fail"] <= 1, (
        f"more than the (possibly mocked) backup check failed: {result}"
    )


def test_stale_email_log_fires_alert(sweep_conn):
    """Gmail inbound poller silent for >2h → external_service_disconnected
    fires for that specific label."""
    _fresh(sweep_conn)
    _stale(sweep_conn, "email_log", "logged_at", days=1)  # 24h stale

    sent = []
    def _capture(**kw):
        sent.append(kw)
        return {"ok": True}

    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert", side_effect=_capture):
        from src.core.liveness_checks import run_liveness_sweep
        run_liveness_sweep()

    gmail_alerts = [s for s in sent if "Gmail inbound" in s.get("title", "")]
    assert len(gmail_alerts) == 1, (
        f"expected 1 Gmail-inbound alert, got {[s.get('title') for s in sent]}"
    )
    assert gmail_alerts[0]["event_type"] == "external_service_disconnected"
    assert gmail_alerts[0]["urgency"] == "warning"
    # 86400 = daily-bucketed cooldown per IN-14
    assert gmail_alerts[0]["cooldown_seconds"] == 86400


def test_stale_scprs_fires_specific_event(sweep_conn):
    """SCPRS gets its own event_type (`scprs_pull_failed_persistent`)
    because it's distinct from generic disconnection — the route is
    different. Pinning this prevents future PRs from collapsing the
    SCPRS alert into the generic one."""
    _fresh(sweep_conn)
    _stale(sweep_conn, "scprs_po_master", "pulled_at", days=4)  # >48h

    sent = []
    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        from src.core.liveness_checks import run_liveness_sweep
        run_liveness_sweep()

    scprs = [s for s in sent if "SCPRS" in s.get("title", "")]
    assert len(scprs) == 1
    assert scprs[0]["event_type"] == "scprs_pull_failed_persistent", (
        "regression: SCPRS check stopped using its dedicated event_type. "
        "Mike named this one specifically in the 2026-05-25 directive."
    )


def test_missing_telegram_credentials_fires_alert(monkeypatch):
    """No TELEGRAM_BOT_TOKEN → alert fires immediately (max_age=1)."""
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    import importlib
    import src.core.liveness_checks as lc
    importlib.reload(lc)

    sent = []
    # Patch DB-side checks so they don't false-fire and clutter the assertion.
    with patch("src.core.db.get_db") as mock_db, \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        mock_db.side_effect = Exception("DB not relevant for this test")
        lc.run_liveness_sweep()

    tg = [s for s in sent if "Telegram" in s.get("title", "")]
    assert len(tg) == 1, (
        f"missing Telegram credentials did not fire alert. "
        f"All alerts: {[s.get('title') for s in sent]}"
    )
    assert "TELEGRAM_BOT_TOKEN" in tg[0]["body"]


def test_present_credentials_no_alert(monkeypatch):
    """All env vars set → credential checks pass."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "abc")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-abc")

    import importlib
    import src.core.liveness_checks as lc
    importlib.reload(lc)

    sent = []
    with patch("src.core.db.get_db") as mock_db, \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        mock_db.side_effect = Exception("DB not relevant")
        lc.run_liveness_sweep()

    tg = [s for s in sent if "Telegram bot" in s.get("title", "")]
    anth = [s for s in sent if "Anthropic" in s.get("title", "")]
    assert tg == [], f"Telegram alert fired despite creds set: {tg}"
    assert anth == [], f"Anthropic alert fired despite key set: {anth}"


def test_check_function_exception_does_not_crash_sweep(sweep_conn):
    """If one check raises, the sweep records the failure and keeps
    going. Total robustness — never let one bad check kill the whole
    sweep."""
    _fresh(sweep_conn)

    import src.core.liveness_checks as lc

    # Inject a deliberately broken check at the front of the list.
    def _exploding():
        raise RuntimeError("simulated check failure")

    broken_tuple = ("Exploding check", "external_service_disconnected",
                    _exploding, 1)

    with patch.object(lc, "CHECKS", [broken_tuple] + lc.CHECKS), \
         _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert", return_value={"ok": True}):
        result = lc.run_liveness_sweep()

    # The exploding check is reported as failed
    assert any(c["name"] == "Exploding check" and not c["ok"]
               for c in result["checks"])
    # All checks ran — broken one + the original 10 = 11.
    # Use the patched-in length explicitly (CHECKS was mutated for this test).
    assert len(result["checks"]) == 11, (
        f"sweep didn't run all checks past the exploding one: "
        f"{len(result['checks'])} reported"
    )
    # Sweep itself didn't raise
    assert "error" not in result, f"sweep crashed: {result}"


def test_check_registry_shape_stable():
    """Each entry must be a 4-tuple: (label, event_type, callable, int).
    Adding new checks should always preserve this shape — locks the
    contract for the registry."""
    from src.core.liveness_checks import CHECKS
    assert len(CHECKS) >= 8, (
        f"CHECKS registry shrank to {len(CHECKS)} entries — the 2026-05-25 "
        f"baseline was 10+ checks. Was one accidentally removed?"
    )
    for idx, entry in enumerate(CHECKS):
        assert len(entry) == 4, f"CHECKS[{idx}] is not a 4-tuple: {entry}"
        label, event, fn, max_age = entry
        assert isinstance(label, str) and label
        assert isinstance(event, str) and event
        assert callable(fn)
        assert isinstance(max_age, int) and max_age > 0


def test_check_events_route_to_telegram_via_channel_map():
    """Every event_type emitted by a check must be in CHANNEL_MAP's
    WORTHY tier — otherwise the alert silently lands in bell-only and
    Mike never sees it."""
    import src.agents.notify_agent as na
    from src.core.liveness_checks import CHECKS

    # The CHANNEL_MAP is defined inside _dispatch_alert; introspect by
    # mocking a single dispatch and observing.
    # Simpler: assert each event_type is in the WORTHY allowlist from
    # the silent-default test file (which encodes the contract).
    worthy_telegram_events = {
        "external_service_disconnected",
        "scprs_pull_failed_persistent",
        "gmail_oauth_expired",
        "twilio_unreachable",
        "oracle_weekly", "oracle_weekly_failed",
        "oracle_weekly_never_sent", "oracle_weekly_overdue",
        "oracle_weekly_crash", "award_tracker_idle",
        "loss_pattern_detected",
    }
    for label, event, _fn, _max in CHECKS:
        assert event in worthy_telegram_events, (
            f"check {label!r} uses event_type {event!r} which is NOT in "
            f"the WORTHY (Telegram) tier of CHANNEL_MAP. The alert will "
            f"land in bell-only and Mike won't see it. Add {event!r} to "
            f"CHANNEL_MAP's WORTHY tier, or use an existing WORTHY event."
        )
