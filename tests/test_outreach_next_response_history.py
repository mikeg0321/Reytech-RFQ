"""
Tests for /outreach/next response-history signal ("What will land").

The helper classifies per-prospect email engagement history into an
action hint so the operator sees which outreach angle will actually
work before clicking Draft:

    none      — first outreach, default to price hook
    cold      — emails sent but no opens, try different angle
    warm      — opens but no replies, try specific ask
    engaged   — clicks/multi-opens, make it a call
    fatigued  — 3+ sent / 0 opened, switch channel (phone if avail)

This closes the last piece of Mike's ask: "what outreach will stick
and land" — surfaces past engagement per prospect inline on the card.
"""
import sqlite3
import pytest


def _call_signal(history, has_phone):
    """Import the helper live so test tracks the module."""
    from importlib import import_module
    # The route module is exec'd into dashboard — direct import works
    # because its path matches src.api.modules. For the isolated pure
    # function, we can spec-load it directly.
    import importlib.util, sys, os
    path = os.path.abspath("src/api/modules/routes_outreach_next.py")
    if "routes_outreach_next_test" not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            "routes_outreach_next_test", path)
        mod = importlib.util.module_from_spec(spec)
        # Stub the flask imports so we don't drag the whole app in.
        class _Bp:
            def route(self, *a, **k):
                return lambda f: f
        import types
        shared_stub = types.ModuleType("src.api.shared")
        shared_stub.bp = _Bp()
        shared_stub.auth_required = lambda f: f
        sys.modules.setdefault("src.api.shared", shared_stub)
        render_stub = types.ModuleType("src.api.render")
        render_stub.render_page = lambda *a, **k: ""
        sys.modules.setdefault("src.api.render", render_stub)
        try:
            spec.loader.exec_module(mod)
        except Exception:
            # Fall back: import via the proper runtime path.
            mod = import_module(
                "src.api.modules.routes_outreach_next"
            )
        sys.modules["routes_outreach_next_test"] = mod
    mod = sys.modules["routes_outreach_next_test"]
    return mod._response_signal(history, has_phone)


def test_signal_none_on_no_prior_contact():
    sig = _call_signal({}, has_phone=False)
    assert sig["level"] == "none"
    assert "First outreach" in sig["hint"] or "No prior" in sig["label"]


def test_signal_fatigued_when_3_sent_0_opened_with_phone():
    sig = _call_signal({"sent": 3, "opened": 0, "clicked": 0}, has_phone=True)
    assert sig["level"] == "fatigued"
    assert "phone" in sig["hint"].lower()


def test_signal_fatigued_without_phone_asks_for_phone_number():
    sig = _call_signal({"sent": 4, "opened": 0, "clicked": 0}, has_phone=False)
    assert sig["level"] == "fatigued"
    assert "phone number" in sig["hint"].lower()


def test_signal_cold_on_single_send_no_opens():
    sig = _call_signal({"sent": 1, "opened": 0, "clicked": 0}, has_phone=False)
    assert sig["level"] == "cold"
    assert "strategy B" in sig["hint"] or "subject" in sig["hint"].lower()


def test_signal_warm_when_opening_but_not_clicking():
    sig = _call_signal({"sent": 2, "opened": 1, "clicked": 0}, has_phone=False)
    assert sig["level"] == "warm"
    assert "specific ask" in sig["hint"].lower() or "reply" in sig["hint"].lower()


def test_signal_engaged_on_click():
    sig = _call_signal({"sent": 2, "opened": 1, "clicked": 1}, has_phone=False)
    assert sig["level"] == "engaged"
    assert "call" in sig["hint"].lower() or "quote" in sig["hint"].lower()


def test_signal_engaged_on_repeat_opens():
    sig = _call_signal({"sent": 2, "opened": 2, "clicked": 0}, has_phone=False)
    assert sig["level"] == "engaged"


def test_response_history_query_is_schema_tolerant_when_tables_missing(tmp_path):
    """No email_outbox / email_engagement tables → empty dict, no crash."""
    db_path = str(tmp_path / "empty.db")
    conn = sqlite3.connect(db_path)

    import sys
    if "routes_outreach_next_test" not in sys.modules:
        _call_signal({}, False)  # triggers the import shim above
    mod = sys.modules["routes_outreach_next_test"]

    out = mod._response_history_for_emails(conn, ["test@cchcs.ca.gov"])
    assert out == {}
    conn.close()


def test_response_history_query_uses_outbox_send_log(tmp_path):
    """With email_outbox rows but no engagement table, returns sent
    counts correctly (graceful degrade)."""
    db_path = str(tmp_path / "outbox_only.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE email_outbox ("
        "id TEXT PRIMARY KEY, to_address TEXT, status TEXT, sent_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO email_outbox (id, to_address, status, sent_at) "
        "VALUES (?, ?, 'sent', ?)",
        [
            ("m1", "jane.buyer@cchcs.ca.gov", "2026-03-01"),
            ("m2", "jane.buyer@cchcs.ca.gov", "2026-03-15"),
            ("m3", "bob.burton@cdcr.ca.gov", "2026-03-20"),
        ],
    )
    # drafts (not sent) should NOT count
    conn.execute(
        "INSERT INTO email_outbox (id, to_address, status, sent_at) "
        "VALUES ('m4', 'jane.buyer@cchcs.ca.gov', 'draft', '')"
    )
    conn.commit()

    import sys
    if "routes_outreach_next_test" not in sys.modules:
        _call_signal({}, False)
    mod = sys.modules["routes_outreach_next_test"]

    out = mod._response_history_for_emails(
        conn, ["jane.buyer@cchcs.ca.gov", "bob.burton@cdcr.ca.gov", "missing@x.com"]
    )
    assert out["jane.buyer@cchcs.ca.gov"]["sent"] == 2
    assert out["bob.burton@cdcr.ca.gov"]["sent"] == 1
    assert "missing@x.com" not in out
    conn.close()


def test_response_history_joins_engagement_for_open_counts(tmp_path):
    """Full query path with both outbox + engagement returns open/click
    counts per prospect."""
    db_path = str(tmp_path / "full.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE email_outbox ("
        "id TEXT PRIMARY KEY, to_address TEXT, status TEXT, sent_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE email_engagement ("
        "id INTEGER PRIMARY KEY, email_id TEXT, event_type TEXT, event_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO email_outbox (id, to_address, status, sent_at) VALUES (?,?,'sent',?)",
        [("m1","jane@x.com","2026-03-01"), ("m2","jane@x.com","2026-03-15"),
         ("m3","bob@x.com","2026-03-20")],
    )
    conn.executemany(
        "INSERT INTO email_engagement (email_id, event_type, event_at) VALUES (?,?,?)",
        [("m1","open","2026-03-02"), ("m1","open","2026-03-03"),
         ("m2","open","2026-03-16"), ("m2","click","2026-03-17")],
    )
    conn.commit()

    import sys
    if "routes_outreach_next_test" not in sys.modules:
        _call_signal({}, False)
    mod = sys.modules["routes_outreach_next_test"]

    out = mod._response_history_for_emails(conn, ["jane@x.com", "bob@x.com"])
    assert out["jane@x.com"]["sent"] == 2
    assert out["jane@x.com"]["opened"] == 3
    assert out["jane@x.com"]["clicked"] == 1
    assert out["bob@x.com"]["sent"] == 1
    assert out["bob@x.com"]["opened"] == 0
    conn.close()
