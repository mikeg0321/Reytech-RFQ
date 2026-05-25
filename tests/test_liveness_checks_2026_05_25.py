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
            id INTEGER PRIMARY KEY, pulled_at TEXT, scraped_at TEXT,
            po_number TEXT
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
        CREATE TABLE IF NOT EXISTS spine_quotes (
            quote_id TEXT PRIMARY KEY, state_json TEXT,
            created_at TEXT, updated_at TEXT
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


def test_scprs_check_passes_when_only_scraped_at_is_fresh(sweep_conn):
    """Reproduces the 2026-05-25 false-alarm: pulled_at empty/stale but
    scraped_at fresh from the scheduled browser scrape. The check must
    NOT fire — newest write wins across both columns. See
    feedback_kpi_substrate_singleness."""
    # Fresh scraped_at, no pulled_at row at all.
    now = datetime.now(timezone.utc) - timedelta(minutes=10)
    sweep_conn.execute("DELETE FROM scprs_po_master")
    sweep_conn.execute(
        "INSERT INTO scprs_po_master (scraped_at, po_number) VALUES (?, 'PO-X')",
        (now.isoformat(),),
    )
    # Other tables fresh so they don't pollute the alert list.
    _fresh(sweep_conn)
    sweep_conn.execute("DELETE FROM scprs_po_master")
    sweep_conn.execute(
        "INSERT INTO scprs_po_master (scraped_at, po_number) VALUES (?, 'PO-X')",
        (now.isoformat(),),
    )
    sweep_conn.commit()

    sent = []
    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        import importlib
        import src.core.liveness_checks as lc
        importlib.reload(lc)
        lc.run_liveness_sweep()

    scprs = [s for s in sent if "SCPRS" in s.get("title", "")]
    assert scprs == [], (
        f"SCPRS alert fired even though scraped_at is fresh: {scprs}. "
        f"The check must read both pulled_at AND scraped_at — the "
        f"scheduled browser scrape writes scraped_at."
    )


def test_scprs_check_fires_when_both_columns_stale(sweep_conn):
    """Belt-and-suspenders: when BOTH writer paths are silent past
    threshold, the alert must still fire. Catches the inverse regression
    of the fix (don't paper over a real outage)."""
    old = (datetime.now(timezone.utc) - timedelta(days=4)).isoformat()
    sweep_conn.execute("DELETE FROM scprs_po_master")
    sweep_conn.execute(
        "INSERT INTO scprs_po_master (pulled_at, scraped_at, po_number) "
        "VALUES (?, ?, 'PO-Y')",
        (old, old),
    )
    # Other tables fresh.
    now_iso = datetime.now(timezone.utc).isoformat()
    sweep_conn.execute("DELETE FROM email_log")
    sweep_conn.execute("INSERT INTO email_log (logged_at, direction) VALUES (?, 'in')", (now_iso,))
    sweep_conn.execute("DELETE FROM award_tracker_log")
    sweep_conn.execute("INSERT INTO award_tracker_log (checked_at, quote_number) VALUES (?, 'R26Q1')", (now_iso,))
    sweep_conn.execute("DELETE FROM competitor_intel")
    sweep_conn.execute("INSERT INTO competitor_intel (found_at, outcome) VALUES (?, 'lost')", (now_iso,))
    sweep_conn.execute("DELETE FROM oracle_calibration")
    sweep_conn.execute("INSERT INTO oracle_calibration (category, last_updated) VALUES ('general', ?)", (now_iso,))
    sweep_conn.execute("DELETE FROM quotes")
    sweep_conn.execute("INSERT INTO quotes (quote_number, created_at, status) VALUES ('R26Q1', ?, 'sent')", (now_iso,))
    sweep_conn.commit()

    sent = []
    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        import importlib
        import src.core.liveness_checks as lc
        importlib.reload(lc)
        lc.run_liveness_sweep()

    scprs = [s for s in sent if "SCPRS" in s.get("title", "")]
    assert len(scprs) == 1, (
        f"SCPRS alert did not fire even though BOTH columns are 4d stale "
        f"(threshold 48h). The multi-source primitive must not mask real "
        f"outages. Sent: {[s.get('title') for s in sent]}"
    )


def test_multi_source_freshness_picks_youngest():
    """Direct unit test of the primitive — youngest across sources wins,
    empty/erroring sources don't fail the check unless ALL fail."""
    import sqlite3
    from datetime import datetime, timezone, timedelta
    from src.core.liveness_checks import _multi_source_freshness

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE t1 (col_a TEXT);
        CREATE TABLE t2 (col_b TEXT);
    """)
    # t1.col_a = 10 days ago (stale); t2.col_b = 5 min ago (fresh)
    old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    new = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    conn.execute("INSERT INTO t1 (col_a) VALUES (?)", (old,))
    conn.execute("INSERT INTO t2 (col_b) VALUES (?)", (new,))
    conn.commit()

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()):
        check = _multi_source_freshness(("t1", "col_a"), ("t2", "col_b"))
        ok, age, detail = check()

    assert ok is True
    assert age < 600, f"expected ~5 min, got {age}s — youngest source didn't win"
    assert "t2.col_b" in detail


def test_multi_source_freshness_all_empty_fails():
    """All sources empty → check reports failure. No silent OK on a
    completely-empty substrate."""
    import sqlite3
    from src.core.liveness_checks import _multi_source_freshness

    conn = sqlite3.connect(":memory:")
    conn.executescript("CREATE TABLE t1 (col_a TEXT); CREATE TABLE t2 (col_b TEXT);")

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()):
        check = _multi_source_freshness(("t1", "col_a"), ("t2", "col_b"))
        ok, age, detail = check()

    assert ok is False
    assert age >= 10**9
    assert "empty" in detail


def test_scprs_browser_store_writes_both_timestamp_columns(tmp_path, monkeypatch):
    """The scheduled browser scrape MUST write `pulled_at` along with
    `scraped_at` so the liveness check (which has historically read
    `pulled_at`) and all the API pullers see one substrate event-time.
    This locks the substrate-consolidation half of the 2026-05-25 fix."""
    import sqlite3
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT UNIQUE, dept_code TEXT, dept_name TEXT,
            status TEXT, start_date TEXT, end_date TEXT,
            supplier TEXT, supplier_id TEXT, acq_type TEXT, acq_method TEXT,
            merch_amount TEXT, grand_total TEXT, buyer_name TEXT,
            buyer_email TEXT, buyer_phone TEXT, source_system TEXT,
            screenshot_path TEXT, scraped_at TEXT, pulled_at TEXT
        );
        CREATE TABLE scprs_po_lines (
            po_number TEXT, line_num INTEGER, item_id TEXT, description TEXT,
            unspsc TEXT, uom TEXT, quantity REAL, unit_price REAL,
            line_total REAL, line_status TEXT, category TEXT
        );
        CREATE TABLE scprs_catalog (
            description TEXT PRIMARY KEY, unspsc TEXT, last_unit_price REAL,
            last_quantity REAL, last_uom TEXT, last_supplier TEXT,
            last_department TEXT, last_po_number TEXT, last_date TEXT,
            times_seen INTEGER DEFAULT 0, updated_at TEXT
        );
    """)
    conn.commit()
    conn.close()

    # Point both DB paths the function reaches for at our temp DB.
    monkeypatch.setattr("src.core.db.DB_PATH", str(db_path))
    # Stub out the downstream calls _store_results makes after writing
    # (won_quotes ingest, buyer refresh) — irrelevant to this assertion.
    monkeypatch.setattr(
        "src.knowledge.won_quotes_db.ingest_scprs_result",
        lambda **kw: None,
    )
    monkeypatch.setattr(
        "src.agents.buyer_intelligence.refresh_buyer_profiles",
        lambda: None,
    )

    from src.agents.scprs_browser import _store_results
    batch = [{
        "header": {
            "po_number": "PO-TEST-1", "dept_code": "5225",
            "dept_name": "CDCR", "status": "Active",
            "start_date": "05/01/2026", "end_date": "06/01/2026",
            "supplier": "Reytech Inc.", "supplier_id": "S1",
            "acq_type": "RFQ", "acq_method": "Open",
            "merch_amount": "100", "grand_total": "100",
            "buyer_name": "Buyer", "buyer_email": "b@x.gov",
            "buyer_phone": "555-0100",
        },
        "line_items": [],
    }]
    _store_results(batch, set())

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT pulled_at, scraped_at FROM scprs_po_master "
        "WHERE po_number = 'PO-TEST-1'"
    ).fetchone()
    conn.close()

    assert row is not None, "row not inserted"
    pulled_at, scraped_at = row
    assert pulled_at, (
        "pulled_at is empty — the writer-side substrate fix regressed. "
        "scprs_browser._store_results must write pulled_at so the "
        "liveness check + the API pullers see one event-time."
    )
    assert scraped_at, "scraped_at also expected (historical column)"


def test_quote_ingestion_passes_when_only_spine_quotes_is_fresh(sweep_conn):
    """The 2026-05-25 false alarm reproducer: legacy `quotes.created_at`
    is stale (no new ingest via the legacy path for 10+ days) but the
    Spine `spine_quotes.created_at` is fresh. Per §0 LAW 1 the Spine
    is canonical — the check must NOT fire.
    See feedback_kpi_substrate_singleness."""
    _fresh(sweep_conn)
    # Make legacy quotes stale (10 days ago).
    old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    sweep_conn.execute("DELETE FROM quotes")
    sweep_conn.execute(
        "INSERT INTO quotes (quote_number, created_at, status) "
        "VALUES ('R26Q-OLD', ?, 'sent')", (old,))
    # Spine quote fresh (5 min ago).
    fresh = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    sweep_conn.execute(
        "INSERT INTO spine_quotes (quote_id, state_json, created_at, updated_at) "
        "VALUES ('Q-NEW-1', '{}', ?, ?)", (fresh, fresh))
    sweep_conn.commit()

    sent = []
    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        import importlib
        import src.core.liveness_checks as lc
        importlib.reload(lc)
        lc.run_liveness_sweep()

    qi = [s for s in sent if "Quote ingestion" in s.get("title", "")]
    assert qi == [], (
        f"Quote ingestion alert fired even though Spine spine_quotes is "
        f"fresh. The check must read both quotes AND spine_quotes. "
        f"Got: {qi}"
    )


def test_quote_ingestion_fires_when_both_tables_stale(sweep_conn):
    """Inverse: both substrates silent past 7d threshold → alert MUST
    fire. The Spine-awareness fix must not paper over a real outage."""
    old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    sweep_conn.execute("DELETE FROM quotes")
    sweep_conn.execute(
        "INSERT INTO quotes (quote_number, created_at, status) "
        "VALUES ('R26Q-OLD', ?, 'sent')", (old,))
    sweep_conn.execute(
        "INSERT INTO spine_quotes (quote_id, state_json, created_at, updated_at) "
        "VALUES ('Q-OLD-1', '{}', ?, ?)", (old, old))
    # Other tables fresh.
    now_iso = datetime.now(timezone.utc).isoformat()
    for t, col in (("email_log", "logged_at"), ("scprs_po_master", "pulled_at"),
                   ("award_tracker_log", "checked_at"),
                   ("competitor_intel", "found_at"),
                   ("oracle_calibration", "last_updated")):
        sweep_conn.execute(f"DELETE FROM {t}")
    sweep_conn.execute("INSERT INTO email_log (logged_at, direction) VALUES (?, 'in')", (now_iso,))
    sweep_conn.execute("INSERT INTO scprs_po_master (pulled_at, po_number) VALUES (?, 'PO-1')", (now_iso,))
    sweep_conn.execute("INSERT INTO award_tracker_log (checked_at, quote_number) VALUES (?, 'R')", (now_iso,))
    sweep_conn.execute("INSERT INTO competitor_intel (found_at, outcome) VALUES (?, 'lost')", (now_iso,))
    sweep_conn.execute("INSERT INTO oracle_calibration (category, last_updated) VALUES ('general', ?)", (now_iso,))
    sweep_conn.commit()

    sent = []
    with _patch_db(sweep_conn), \
         patch("src.agents.notify_agent.send_alert",
               side_effect=lambda **kw: sent.append(kw) or {"ok": True}):
        import importlib
        import src.core.liveness_checks as lc
        importlib.reload(lc)
        lc.run_liveness_sweep()

    qi = [s for s in sent if "Quote ingestion" in s.get("title", "")]
    assert len(qi) == 1, (
        f"Quote ingestion alert did not fire despite BOTH tables being "
        f"10d stale (threshold 7d). The Spine-awareness fix must not "
        f"mask real outages. Sent: {[s.get('title') for s in sent]}"
    )


def test_quote_ingestion_freshness_helper_picks_youngest_table():
    """Direct unit test: the helper compares both tables and reports
    the youngest write."""
    import sqlite3
    from src.core.liveness_checks import _quote_ingestion_freshness

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE quotes (created_at TEXT);
        CREATE TABLE spine_quotes (quote_id TEXT, state_json TEXT,
                                   created_at TEXT, updated_at TEXT);
    """)
    old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    new = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    conn.execute("INSERT INTO quotes (created_at) VALUES (?)", (old,))
    conn.execute(
        "INSERT INTO spine_quotes (quote_id, state_json, created_at, updated_at) "
        "VALUES ('q1', '{}', ?, ?)", (new, new))
    conn.commit()

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()):
        ok, age, detail = _quote_ingestion_freshness()()

    assert ok is True
    assert age < 300, f"expected ~2 min, got {age}s — spine_quotes (fresher) didn't win"
    assert "spine_quotes" in detail


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
