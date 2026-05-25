"""Oracle substrate fixes — 2026-05-25 PR.

Pins three substrate behaviours added in the oracle-substrate PR:

  1. generate_weekly_report() exposes a `calibration_samples_total`
     KPI sourced from `SUM(oracle_calibration.sample_size)` — NOT from
     `winning_prices.COUNT(*)`. The 2026-05-18→05-25 weekly email
     displayed "0 Data Points" while the calibration table below it
     showed 300+ samples — two substrate tables on one card.

  2. format_report_email() renders the KPI from
     `calibration_samples_total` and labels the card "Calibration
     Samples". Falls back to `winning_prices_total` only when the
     primary field is absent (back-compat for upstream callers that
     pre-date the rename).

  3. award_tracker.run_award_check() fires `award_tracker_idle` when
     0 eligible quotes are found but recent activity exists in the
     `quotes` / `rfqs` tables — the silent break that hid the
     Mark-Sent regression for a week.
"""
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from unittest.mock import patch

import pytest


# ── KPI #1: source field present ──────────────────────────────────────────


def _seed_calibration(conn, *, sample_size=120, win_count=30, avg_margin=27.5):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_number TEXT, status TEXT, institution TEXT,
            total REAL DEFAULT 0, po_number TEXT, created_at TEXT,
            is_test INTEGER DEFAULT 0, items_detail TEXT,
            agency TEXT
        );
        CREATE TABLE IF NOT EXISTS competitor_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            outcome TEXT, found_at TEXT, quote_number TEXT,
            competitor_name TEXT, competitor_price REAL,
            our_price REAL, price_delta_pct REAL,
            agency TEXT, loss_reason_class TEXT
        );
        CREATE TABLE IF NOT EXISTS competitor_intel_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor_intel_id INTEGER,
            line_num INTEGER, scprs_description TEXT,
            scprs_unit_price REAL, scprs_quantity REAL,
            scprs_mfg TEXT, our_item_idx INTEGER,
            our_unit_price REAL, price_delta_pct REAL,
            matched_by TEXT
        );
        CREATE TABLE IF NOT EXISTS oracle_calibration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT, agency TEXT,
            sample_size INTEGER, win_count INTEGER,
            loss_on_price INTEGER DEFAULT 0,
            loss_on_other INTEGER DEFAULT 0,
            avg_winning_margin REAL,
            recommended_max_markup REAL DEFAULT 30,
            last_updated TEXT
        );
        CREATE TABLE IF NOT EXISTS winning_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT, margin_pct REAL, recorded_at TEXT
        );
        CREATE TABLE IF NOT EXISTS action_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_type TEXT, description TEXT, priority TEXT,
            source_quote TEXT, created_at TEXT, status TEXT
        );
    """)
    # 4 categories, sample_size each, totaling 4 * sample_size.
    for cat in ("General", "Office", "Arts_crafts", "Medical"):
        conn.execute(
            "INSERT INTO oracle_calibration (category, agency, sample_size, "
            "win_count, avg_winning_margin, last_updated) VALUES (?,?,?,?,?,?)",
            (cat, "", sample_size, win_count, avg_margin,
             datetime.now().isoformat()),
        )
    # winning_prices stays EMPTY — that's the bug we are pinning.
    conn.commit()


def test_generate_weekly_report_exposes_calibration_samples():
    """The new KPI source field must aggregate oracle_calibration.sample_size,
    not winning_prices.COUNT(*)."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_calibration(conn, sample_size=120, win_count=30, avg_margin=27.5)

    from src.agents import oracle_weekly_report as owr

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()):
        report = owr.generate_weekly_report()

    # Field is present and correctly aggregated.
    assert "calibration_samples_total" in report, (
        "regression: KPI source field renamed away from "
        "calibration_samples_total — the 2026-05-25 fix relied on this "
        "exact key in format_report_email"
    )
    assert report["calibration_samples_total"] == 4 * 120
    assert report["calibration_wins_total"] == 4 * 30
    # winning_prices is empty but the KPI still shows the substrate truth.
    assert report["winning_prices_total"] == 0


def test_format_report_email_renders_calibration_samples_kpi():
    """The card displays calibration_samples_total under the label
    'Calibration Samples' — NOT 'Data Points' off winning_prices."""
    from src.agents.oracle_weekly_report import format_report_email
    report = {
        "period_start": "2026-05-18", "period_end": "2026-05-25",
        "generated_at": "2026-05-25T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [], "loss_count": 0,
        "calibrations": [],
        "calibration_samples_total": 487,
        "calibration_wins_total": 102,
        "avg_margin_all_time": 28.4,
        "winning_prices_total": 0, "winning_prices_unique": 0,
        "supplier_leads": [], "pending_actions": [],
    }
    html = format_report_email(report)
    assert "Calibration Samples" in html, (
        "regression: KPI card label rolled back to 'Data Points'"
    )
    assert ">487<" in html, (
        f"regression: calibration_samples_total not surfaced in KPI card "
        f"(html slice: ...{html[html.find('Calibration'):html.find('Calibration')+200]}...)"
    )
    # And the bug condition (empty winning_prices) does NOT make the card show 0.
    assert ">0<" not in html.split("Calibration Samples")[0][-200:], (
        "regression: card shows 0 despite calibration_samples_total=487 — "
        "this is the exact 2026-05-25 inbox screenshot bug"
    )


def test_format_report_email_falls_back_to_winning_prices_for_back_compat():
    """Old report dicts (no calibration_samples_total key) still render
    using winning_prices_total — keeps the function back-compat with
    callers that haven't yet rebuilt the report."""
    from src.agents.oracle_weekly_report import format_report_email
    report = {
        "period_start": "2026-05-18", "period_end": "2026-05-25",
        "generated_at": "2026-05-25T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [], "loss_count": 0,
        "calibrations": [],
        # NO calibration_samples_total field — old shape.
        "winning_prices_total": 99, "winning_prices_unique": 47,
        "avg_margin_all_time": 22.0,
        "supplier_leads": [], "pending_actions": [],
    }
    html = format_report_email(report)
    assert ">99<" in html  # back-compat KPI value


# ── Idle-scanner alarm ─────────────────────────────────────────────────────


def _seed_idle_scenario(conn, *, quotes_in_last_30d=5,
                       quotes_in_status_sent=0):
    """Seed the scenario that triggers award_tracker_idle: recent quotes
    exist but none are in status='sent'."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_number TEXT, agency TEXT, institution TEXT,
            total REAL DEFAULT 0, line_items TEXT, items_text TEXT,
            sent_at TEXT, created_at TEXT, contact_email TEXT,
            contact_name TEXT, source_pc_id TEXT, status TEXT,
            is_test INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS rfqs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_number TEXT, agency TEXT, institution TEXT,
            total REAL DEFAULT 0, items TEXT, sent_at TEXT,
            received_at TEXT, requestor_email TEXT,
            requestor_name TEXT, status TEXT
        );
        CREATE TABLE IF NOT EXISTS award_tracker_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT, quote_number TEXT,
            scprs_searched INTEGER, matches_found INTEGER,
            outcome TEXT, notes TEXT
        );
    """)
    now = datetime.now(timezone.utc)
    yest = (now - timedelta(days=2)).isoformat()
    for i in range(quotes_in_last_30d):
        conn.execute(
            "INSERT INTO quotes (quote_number, agency, total, status, "
            "created_at, is_test) VALUES (?,?,?,?,?,0)",
            (f"R26Q{i:03d}", "CCHCS", 1000.0, "draft", yest),
        )
    for i in range(quotes_in_status_sent):
        conn.execute(
            "INSERT INTO quotes (quote_number, agency, total, status, "
            "sent_at, created_at, is_test) VALUES (?,?,?,?,?,?,0)",
            (f"R26S{i:03d}", "CCHCS", 1000.0, "sent",
             (now - timedelta(days=5)).isoformat(),
             (now - timedelta(days=5)).isoformat()),
        )
    conn.commit()


def test_idle_alarm_fires_when_recent_activity_but_no_sent_quotes(monkeypatch):
    """Stage A diagnostic: scanner runs, sees 0 eligible quotes despite
    recent quotes — must fire award_tracker_idle."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_idle_scenario(conn, quotes_in_last_30d=5, quotes_in_status_sent=0)

    from src.agents import award_tracker as at

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    sent = []
    def _capture(**kwargs):
        sent.append(kwargs)
        return {"ok": True}

    # award_tracker uses _db() (sqlite3.connect directly), so patch that
    # instead of get_db.
    with patch.object(at, "_db", return_value=conn), \
         patch.object(at, "_ensure_tables", return_value=None), \
         patch("src.agents.notify_agent.send_alert", side_effect=_capture):
        result = at.run_award_check()

    assert result["eligible"] == 0
    assert any(s.get("event_type") == "award_tracker_idle" for s in sent), (
        f"award_tracker_idle did not fire despite 5 recent quotes + 0 sent. "
        f"send_alert calls: {[s.get('event_type') for s in sent]}"
    )
    # Cooldown is daily-bucketed
    idle = next(s for s in sent if s["event_type"] == "award_tracker_idle")
    assert idle["cooldown_seconds"] == 86400


def test_idle_alarm_silent_when_db_truly_empty(monkeypatch):
    """Fresh install / staging — no quotes at all. Idle alarm must NOT
    fire (would be noise during the very first deploy)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_idle_scenario(conn, quotes_in_last_30d=0, quotes_in_status_sent=0)

    from src.agents import award_tracker as at

    sent = []
    def _capture(**kwargs):
        sent.append(kwargs)
        return {"ok": True}

    with patch.object(at, "_db", return_value=conn), \
         patch.object(at, "_ensure_tables", return_value=None), \
         patch("src.agents.notify_agent.send_alert", side_effect=_capture):
        result = at.run_award_check()

    assert result["eligible"] == 0
    assert not any(s.get("event_type") == "award_tracker_idle" for s in sent), (
        "award_tracker_idle fired on a genuinely empty DB — that's "
        "deploy-time noise the threshold was designed to suppress"
    )


def test_idle_alarm_skipped_when_sent_quotes_exist(monkeypatch):
    """The idle branch is reached ONLY when sent_quotes is empty. With
    3 quotes in status='sent' the scanner falls through to the matching
    pipeline (which is fine to fail past this point — we just need to
    prove the idle branch was NOT taken)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_idle_scenario(conn, quotes_in_last_30d=10, quotes_in_status_sent=3)

    from src.agents import award_tracker as at

    sent = []
    def _capture(**kwargs):
        sent.append(kwargs)
        return {"ok": True}

    with patch.object(at, "_db", return_value=conn), \
         patch.object(at, "_ensure_tables", return_value=None), \
         patch("src.agents.notify_agent.send_alert", side_effect=_capture):
        try:
            at.run_award_check()
        except Exception:
            # Anything past the idle branch may fail in this minimal seed —
            # we only care that the idle alarm did NOT fire.
            pass

    assert not any(s.get("event_type") == "award_tracker_idle" for s in sent), (
        "idle alarm fired despite 3 quotes in status='sent' — the "
        "eligibility branch is mis-gated"
    )
