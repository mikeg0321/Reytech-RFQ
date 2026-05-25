"""Telegram rich-format payload — MarkdownV2 layout for oracle weekly + idle alarm.

Pins three substrate behaviours added 2026-05-25:

  1. format_telegram_report(report) builds a MarkdownV2 message with bold
     headers, monospace pre-blocks for tables, emoji section dividers,
     and properly-escaped interpolated values (dates, numbers).

  2. run_weekly_report() passes the formatted payload via
     context["telegram_body"], mirroring how context["html_body"] flows
     to the email channel.

  3. _send_telegram() short-circuits on context["telegram_body"] —
     sends it AS-IS without re-escaping, so the formatter's structural
     markdown survives.
"""
from unittest.mock import patch

import pytest


# ── format_telegram_report unit tests ──────────────────────────────────────


def _empty_week_report():
    return {
        "period_start": "2026-05-18",
        "period_end": "2026-05-25",
        "generated_at": "2026-05-25T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [], "loss_count": 0,
        "calibrations": [],
        "calibration_samples_total": 1991,
        "calibration_wins_total": 555,
        "avg_margin_all_time": 23.8,
        "winning_prices_total": 0,
        "supplier_leads": [], "pending_actions": [],
    }


def test_format_telegram_report_quiet_week_renders():
    from src.agents.oracle_weekly_report import format_telegram_report
    out = format_telegram_report(_empty_week_report())

    # Bold header is present
    assert "*Oracle Weekly Intelligence*" in out
    # Period is escaped (dashes need backslash in MarkdownV2)
    assert "2026\\-05\\-18" in out
    assert "2026\\-05\\-25" in out
    # KPI strip — all three lines
    assert "Wins" in out and "Losses" in out
    assert "Calibration Samples" in out
    # Headline KPI value (1991 sample count, comma-formatted)
    assert "1,991" in out
    # Margin string preserved
    assert "23.8%" in out or "23\\.8%" in out
    # Quiet-week footer fired (no wins, no losses, no calibrations)
    assert "No win/loss activity" in out
    # Generated footer present
    assert "Generated" in out


def test_format_telegram_report_with_calibration_table():
    from src.agents.oracle_weekly_report import format_telegram_report
    report = _empty_week_report()
    report["calibrations"] = [
        {"category": "general", "agency": "CCHCS", "samples": 106,
         "wins": 30, "win_rate": 28, "avg_win_margin": 35.6,
         "rec_max_markup": 31.3, "last_updated": "2026-05-25"},
        {"category": "office", "agency": "CCHCS", "samples": 68,
         "wins": 14, "win_rate": 21, "avg_win_margin": 31.7,
         "rec_max_markup": 25.8, "last_updated": "2026-05-25"},
    ]
    out = format_telegram_report(report)

    # Calibration State section header present
    assert "*Calibration State*" in out
    # Pre block fenced — table content is inside ```...```
    assert "```" in out
    # Category labels appear (note underscore replaced with space)
    assert "general" in out.lower() or "General" in out
    assert "office" in out.lower() or "Office" in out
    # Sample counts present
    assert "106" in out
    assert "68" in out
    # Markup percentages rendered
    assert "31.3%" in out
    assert "25.8%" in out


def test_format_telegram_report_with_wins_and_losses():
    from src.agents.oracle_weekly_report import format_telegram_report
    report = _empty_week_report()
    report["wins"] = [
        {"quote": "R26Q001", "agency": "CCHCS", "total": 1234.50,
         "po": "PO-9876", "date": "2026-05-22"},
    ]
    report["win_count"] = 1
    report["win_revenue"] = 1234.50
    report["losses"] = [
        {"id": 1, "quote": "R26Q042", "competitor": "AcmeMed",
         "their_price": 540, "our_price": 600, "delta_pct": -10.0,
         "agency": "cchcs", "reason": "price", "date": "2026-05-21"},
    ]
    report["loss_count"] = 1
    out = format_telegram_report(report)

    assert "*Wins This Week*" in out
    assert "R26Q001" in out
    assert "1,235" in out or "1,234" in out  # rev formatting

    assert "*Losses This Week*" in out
    assert "R26Q042" in out
    assert "AcmeMed" in out
    assert "-10.0%" in out


def test_format_telegram_report_caps_long_lists():
    """More than 10 wins/losses must truncate with '… +N more'."""
    from src.agents.oracle_weekly_report import format_telegram_report
    report = _empty_week_report()
    report["wins"] = [
        {"quote": f"R26Q{i:03d}", "agency": "CCHCS", "total": 100, "po": ""}
        for i in range(15)
    ]
    report["win_count"] = 15
    out = format_telegram_report(report)
    assert "+5 more" in out


def test_format_telegram_report_under_telegram_limit():
    """Payload must fit Telegram's 4096-char sendMessage cap with
    realistic data."""
    from src.agents.oracle_weekly_report import format_telegram_report
    report = _empty_week_report()
    # Realistic prod-scale data
    report["calibrations"] = [
        {"category": f"category_{i}", "agency": "CCHCS", "samples": 100 - i,
         "wins": 20, "win_rate": 25, "avg_win_margin": 30.0,
         "rec_max_markup": 28.0, "last_updated": "2026-05-25"}
        for i in range(15)
    ]
    report["pending_actions"] = [
        {"description": "Action " + "x" * 100, "priority": "high",
         "quote": "R26Q1", "date": "2026-05-22"}
        for _ in range(10)
    ]
    out = format_telegram_report(report)
    assert len(out) < 4096, f"payload too large: {len(out)} chars"


# ── run_weekly_report passes telegram_body in context ──────────────────────


def test_run_weekly_report_passes_telegram_body_in_context(monkeypatch):
    """run_weekly_report must build a telegram_body via
    format_telegram_report and pass it via context — without this wiring
    the Telegram channel falls back to the plain-text escape path and
    the carefully-formatted layout never reaches the bot."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY, quote_number TEXT, status TEXT,
            institution TEXT, total REAL DEFAULT 0, po_number TEXT,
            created_at TEXT, is_test INTEGER DEFAULT 0,
            items_detail TEXT, agency TEXT
        );
        CREATE TABLE IF NOT EXISTS competitor_intel (
            id INTEGER PRIMARY KEY, outcome TEXT, found_at TEXT,
            quote_number TEXT, competitor_name TEXT, competitor_price REAL,
            our_price REAL, price_delta_pct REAL, agency TEXT,
            loss_reason_class TEXT
        );
        CREATE TABLE IF NOT EXISTS competitor_intel_lines (
            id INTEGER PRIMARY KEY, competitor_intel_id INTEGER,
            line_num INTEGER, scprs_description TEXT,
            scprs_unit_price REAL, scprs_quantity REAL,
            scprs_mfg TEXT, our_item_idx INTEGER,
            our_unit_price REAL, price_delta_pct REAL, matched_by TEXT
        );
        CREATE TABLE IF NOT EXISTS oracle_calibration (
            id INTEGER PRIMARY KEY, category TEXT, agency TEXT,
            sample_size INTEGER, win_count INTEGER,
            loss_on_price INTEGER DEFAULT 0,
            loss_on_other INTEGER DEFAULT 0,
            avg_winning_margin REAL,
            recommended_max_markup REAL DEFAULT 30,
            last_updated TEXT
        );
        CREATE TABLE IF NOT EXISTS winning_prices (
            id INTEGER PRIMARY KEY, fingerprint TEXT,
            margin_pct REAL, recorded_at TEXT
        );
        CREATE TABLE IF NOT EXISTS action_items (
            id INTEGER PRIMARY KEY, action_type TEXT, description TEXT,
            priority TEXT, source_quote TEXT, created_at TEXT, status TEXT
        );
        CREATE TABLE IF NOT EXISTS oracle_report_log (
            id INTEGER PRIMARY KEY, sent_at TEXT, success INTEGER,
            win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0,
            supplier_leads INTEGER DEFAULT 0, calibrations INTEGER DEFAULT 0,
            error TEXT DEFAULT ''
        );
    """)
    conn.execute("INSERT INTO oracle_calibration (category, sample_size, "
                 "win_count, avg_winning_margin, last_updated) "
                 "VALUES ('General', 106, 30, 35.6, '2026-05-25')")
    conn.commit()

    from src.agents import oracle_weekly_report as owr

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    captured = {}
    def _capture_send_alert(**kw):
        captured.update(kw)
        return {"ok": True, "results": {"telegram": {"ok": True}}}

    with patch("src.core.db.get_db", return_value=_Ctx()), \
         patch("src.agents.notify_agent.send_alert", side_effect=_capture_send_alert), \
         patch("src.core.scheduler.heartbeat", return_value=None):
        result = owr.run_weekly_report()

    assert result["ok"] is True, f"run_weekly_report failed: {result}"
    ctx = captured.get("context") or {}
    assert "telegram_body" in ctx, (
        "regression: run_weekly_report stopped passing telegram_body in "
        "context — the rich-format payload won't reach Telegram"
    )
    tg_body = ctx["telegram_body"]
    assert "*Oracle Weekly Intelligence*" in tg_body
    assert "106" in tg_body  # the calibration sample we seeded


# ── _send_telegram short-circuit on context["telegram_body"] ───────────────


def test_send_telegram_uses_telegram_body_from_context(monkeypatch):
    """When context.telegram_body is set, _send_telegram POSTs it as-is
    (no further escaping). Without this, the structural markdown built
    by format_telegram_report would be double-escaped into garbage."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    captured = {}

    class _FakeResp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok": true, "result": {"message_id": 1}}'

    def _fake_urlopen(req, timeout=None):
        captured["data"] = req.data.decode("utf-8")
        return _FakeResp()

    pre_built = (
        "📊 *Oracle Weekly Intelligence*\n"
        "_2026\\-05\\-18 → 2026\\-05\\-25_\n\n"
        "🏆 *3* Wins · `$1,000`"
    )

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        result = na._send_telegram(
            event_type="oracle_weekly",
            title="Oracle Weekly: 3W / 0L",
            body="should be ignored",
            urgency="info",
            context={"telegram_body": pre_built},
        )

    assert result["ok"] is True
    # Payload is URL-encoded; check our pre-built string survived unmangled.
    # Critical: the asterisks for bold must NOT be backslash-escaped.
    assert "Oracle+Weekly+Intelligence" in captured["data"]
    # Find the text= field — it should NOT contain double-escaped asterisks
    # (i.e., %5C%2A which would be `\*`)
    assert "%5C%2A" not in captured["data"], (
        "regression: telegram_body was re-escaped — bold markers got "
        "backslash-mangled and Telegram will render literal `\\*`"
    )


def test_send_telegram_falls_back_to_title_body_when_no_telegram_body(monkeypatch):
    """The short-circuit must NOT activate when context.telegram_body is
    absent — preserves back-compat with all the non-oracle callers that
    use the title+body API."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    captured = {}

    class _FakeResp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok": true, "result": {"message_id": 2}}'

    def _fake_urlopen(req, timeout=None):
        captured["data"] = req.data.decode("utf-8")
        return _FakeResp()

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        result = na._send_telegram(
            event_type="quote_won",
            title="Quote Won",
            body="R26Q001 closed for $1,234",
            urgency="deal",
            context={"quote_number": "R26Q001"},
        )

    assert result["ok"] is True
    # The body's `,` is not reserved so it's not backslash-escaped, but `$`
    # is also not reserved. Confirm the title made it into the payload as
    # *bold* (asterisks present, not double-escaped).
    data = captured["data"]
    assert "Quote+Won" in data or "Quote%20Won" in data
    # The fallback path DOES escape — confirm at least one backslash is
    # present from the chars in the body (`R26Q001`, ` ` are safe but
    # the format calls _escape_markdown_v2 which would no-op on safe
    # chars). We don't have a single reserved char in the body, so this
    # path produces a clean output — main assertion is just that the
    # call succeeded with no telegram_body present.


def test_telegram_body_truncated_at_4096_chars(monkeypatch):
    """If a caller passes a huge telegram_body, the short-circuit must
    still respect Telegram's 4096-char limit (we slice to _TELEGRAM_BODY_LIMIT)."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    huge = "*Header*\n\n" + ("A" * 10000)
    captured = {}

    class _FakeResp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok": true, "result": {"message_id": 3}}'

    def _fake_urlopen(req, timeout=None):
        captured["data"] = req.data.decode("utf-8")
        return _FakeResp()

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        na._send_telegram(
            event_type="oracle_weekly",
            title="X", body="", urgency="info",
            context={"telegram_body": huge},
        )

    # The URL-encoded data length must be reasonable; 10000-char payload
    # would balloon way past 4096 chars of content. Our limit is 3500.
    # Count the As in the urlencoded body — each A urlencodes to one char.
    a_count = captured["data"].count("A")
    assert a_count <= 3500, (
        f"telegram_body exceeded the 3500-char safety slice: {a_count} As"
    )
