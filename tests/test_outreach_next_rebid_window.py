"""
Tests for V2-PR-1 rebid-window surveillance on /outreach/next.

Procurement-lens framing (per 2026-04-24 reframe): the signal that
matters is WHEN the incumbent's contract expires, because 60-90 days
before expiry is when the successor RFQ posts. Missing that window =
missing the whole procurement cycle.

This module tests:
  1. _parse_end_date — defensive parsing of TEXT end_date column
  2. _expiring_contracts_by_dept — batched SQL, is_test filter, dedup,
     future + award-gap window, Reytech-vs-competitor tagging
  3. _rebid_urgency — score boost curve (61-90d peak per product-eng review)
  4. _rebid_summary — level + label + hint per state
  5. Card ranking — counterfactual: rebid-urgency beats higher raw score
  6. E2E page render via auth_client with all four states on three cards
"""
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta

import pytest


def _load_rmod():
    """Import the route module via a file loader (exec'd into dashboard
    at runtime; not a normal importable module) using the test shim from
    test_outreach_next_response_history."""
    import sys
    import importlib.util
    import types
    key = "routes_outreach_next_test"
    if key in sys.modules:
        return sys.modules[key]
    path = os.path.abspath("src/api/modules/routes_outreach_next.py")
    spec = importlib.util.spec_from_file_location(key, path)
    mod = importlib.util.module_from_spec(spec)

    class _Bp:
        def route(self, *a, **k):
            return lambda f: f
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
        from importlib import import_module
        mod = import_module("src.api.modules.routes_outreach_next")
    sys.modules[key] = mod
    return mod


# ── _parse_end_date ───────────────────────────────────────────────────────────

def test_parse_end_date_handles_yyyy_mm_dd():
    mod = _load_rmod()
    r = mod._parse_end_date("2026-07-15")
    assert r == date(2026, 7, 15)


def test_parse_end_date_handles_datetime_prefix():
    mod = _load_rmod()
    r = mod._parse_end_date("2026-07-15T00:00:00")
    assert r == date(2026, 7, 15)


def test_parse_end_date_returns_none_on_garbage():
    mod = _load_rmod()
    assert mod._parse_end_date("") is None
    assert mod._parse_end_date(None) is None
    assert mod._parse_end_date("not-a-date") is None
    assert mod._parse_end_date(12345) is None
    assert mod._parse_end_date("0000-00-00") is None


# ── _rebid_urgency classifier ──────────────────────────────────────────────────

def _fake_contract(days, supplier="Medline", is_reytech=False, is_award_gap=None):
    return {
        "days_until_expiry": days,
        "supplier": supplier,
        "is_reytech": is_reytech,
        "is_award_gap": is_award_gap if is_award_gap is not None else days < 0,
        "description": "x", "end_date": "2026-01-01",
        "grand_total": 0, "line_total": 0, "opportunity_flag": "WIN_BACK",
    }


def test_urgency_peaks_in_61_to_90_day_window():
    """Per 2026-04-24 product-eng review: 61-90d is highest leverage
    (register NOW before RFQ posts), NOT 0-30d (often too late)."""
    mod = _load_rmod()
    assert mod._rebid_urgency([_fake_contract(75)]) == 30


def test_urgency_is_25_in_rebid_memo_window_31_to_60():
    mod = _load_rmod()
    assert mod._rebid_urgency([_fake_contract(45)]) == 25


def test_urgency_is_15_in_early_awareness_91_to_120():
    mod = _load_rmod()
    assert mod._rebid_urgency([_fake_contract(105)]) == 15


def test_urgency_is_10_in_last_30_days_when_too_late_to_onboard():
    mod = _load_rmod()
    assert mod._rebid_urgency([_fake_contract(20)]) == 10


def test_urgency_ignores_reytech_incumbent_contracts():
    """Our own expiring contract is a RENEWAL signal, not a rebid
    urgency — different action, don't boost for it."""
    mod = _load_rmod()
    assert mod._rebid_urgency([_fake_contract(75, is_reytech=True)]) == 0


def test_urgency_ignores_award_gap_contracts():
    mod = _load_rmod()
    assert mod._rebid_urgency([_fake_contract(-10)]) == 0


def test_urgency_takes_max_across_multiple_contracts():
    mod = _load_rmod()
    contracts = [_fake_contract(20), _fake_contract(75), _fake_contract(300)]
    assert mod._rebid_urgency(contracts) == 30  # from the 75d contract


def test_urgency_zero_on_empty_or_outside_window():
    mod = _load_rmod()
    assert mod._rebid_urgency([]) == 0
    assert mod._rebid_urgency([_fake_contract(500)]) == 0


# ── _rebid_summary classifier ─────────────────────────────────────────────────

def test_summary_red_on_competitor_expiry_within_60d():
    mod = _load_rmod()
    r = mod._rebid_summary([_fake_contract(45)])
    assert r["level"] == "red"
    assert "45d" in r["label"]
    assert "rebid memo" in r["hint"].lower()


def test_summary_amber_on_competitor_61_to_120d():
    mod = _load_rmod()
    r = mod._rebid_summary([_fake_contract(85)])
    assert r["level"] == "amber"
    assert "register" in r["hint"].lower()


def test_summary_renewal_for_reytech_incumbent():
    mod = _load_rmod()
    r = mod._rebid_summary([_fake_contract(60, is_reytech=True)])
    assert r["level"] == "renewal"
    assert "incumbent" in r["label"].lower()
    assert "defend" in r["hint"].lower()


def test_summary_award_gap_for_expired_contract():
    mod = _load_rmod()
    r = mod._rebid_summary([_fake_contract(-15)])
    assert r["level"] == "award_gap"
    assert "15d ago" in r["label"]


def test_summary_red_takes_precedence_over_amber_and_renewal():
    """When multiple contracts overlap, the most actionable wins."""
    mod = _load_rmod()
    contracts = [
        _fake_contract(100),  # amber
        _fake_contract(45),   # red
        _fake_contract(60, is_reytech=True),  # renewal
    ]
    r = mod._rebid_summary(contracts)
    assert r["level"] == "red"


def test_summary_none_when_no_contracts_in_window():
    mod = _load_rmod()
    r = mod._rebid_summary([])
    assert r["level"] == "none"


# ── _expiring_contracts_by_dept (SQL) ─────────────────────────────────────────

@pytest.fixture
def seeded_conn(tmp_path):
    """SCPRS tables with mixed-expiry contracts across two agencies."""
    db_path = str(tmp_path / "rebid.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Emulate _ensure_schema columns we depend on.
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT UNIQUE, dept_code TEXT, supplier TEXT,
            grand_total REAL, end_date TEXT, is_test INTEGER DEFAULT 0
        );
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER, po_number TEXT, line_num INTEGER,
            description TEXT, line_total REAL,
            opportunity_flag TEXT, is_test INTEGER DEFAULT 0
        );
    """)
    today = date.today()

    def seed(po_num, dept, supplier, end_delta, total, line_desc, line_total,
             is_test=0, opp="WIN_BACK"):
        end = (today + timedelta(days=end_delta)).isoformat() if end_delta is not None else ""
        cur = conn.execute(
            "INSERT INTO scprs_po_master (po_number, dept_code, supplier, "
            "grand_total, end_date, is_test) VALUES (?,?,?,?,?,?)",
            (po_num, dept, supplier, total, end, is_test),
        )
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
            "description, line_total, opportunity_flag, is_test) "
            "VALUES (?,?,?,?,?,?,?)",
            (cur.lastrowid, po_num, 0, line_desc, line_total, opp, is_test),
        )

    # CCHCS: Medline contract expires in 45d (red), Cardinal in 85d (amber),
    # past-expired 15d ago (award-gap), far-future 400d (outside window)
    seed("PO-A", "4700", "Medline", 45, 12000.0, "nitrile gloves M", 4000.0)
    seed("PO-B", "4700", "Cardinal Health", 85, 9000.0, "disinfectant wipes", 2000.0)
    seed("PO-C", "4700", "Henry Schein", -15, 6000.0, "abd pads", 1500.0)
    seed("PO-D", "4700", "FarFuture Inc", 400, 5000.0, "someday item", 500.0)

    # CCHCS Reytech-as-incumbent (renewal)
    seed("PO-R", "4700", "Reytech Inc.", 60, 8000.0, "safety glasses", 2000.0)

    # CCHCS is_test=1 (must be filtered out even though within window)
    seed("PO-T", "4700", "TestCo", 30, 99999.0, "ghost item", 9999.0, is_test=1)

    # CDCR: one competitor contract outside window (to prove the scoping)
    seed("PO-E", "5225", "Medline", 200, 4000.0, "gloves L", 500.0)

    conn.commit()
    return conn


def test_expiring_returns_contracts_in_window_sorted_by_soonest(seeded_conn):
    mod = _load_rmod()
    result = mod._expiring_contracts_by_dept(seeded_conn, ["4700", "5225"])
    cchcs = result["4700"]
    # 4 contracts should match: 45d (red), 85d (amber), -15d (award-gap),
    # 60d (renewal). 400d is outside, is_test is filtered, CDCR not in dept.
    assert len(cchcs) == 4
    # Sorted soonest first by end_date ASC; award-gap (-15) is earliest.
    assert cchcs[0]["days_until_expiry"] == -15
    assert cchcs[1]["days_until_expiry"] == 45


def test_expiring_flags_reytech_incumbent(seeded_conn):
    mod = _load_rmod()
    result = mod._expiring_contracts_by_dept(seeded_conn, ["4700"])
    reytech = [c for c in result["4700"] if c["is_reytech"]]
    assert len(reytech) == 1
    assert reytech[0]["supplier"] == "Reytech Inc."
    assert reytech[0]["days_until_expiry"] == 60


def test_expiring_filters_is_test_rows(seeded_conn):
    """Proof-specific: the seeded ghost PO at 30d and $99,999 must NOT
    appear, matching the §3e is_test discipline across SCPRS reads."""
    mod = _load_rmod()
    result = mod._expiring_contracts_by_dept(seeded_conn, ["4700"])
    suppliers = {c["supplier"] for c in result["4700"]}
    assert "TestCo" not in suppliers


def test_expiring_excludes_contracts_beyond_window(seeded_conn):
    mod = _load_rmod()
    result = mod._expiring_contracts_by_dept(
        seeded_conn, ["4700"], window_days=120)
    # FarFuture (400d) must not appear.
    suppliers = {c["supplier"] for c in result["4700"]}
    assert "FarFuture Inc" not in suppliers


def test_expiring_scopes_by_dept(seeded_conn):
    """Ensure CDCR contracts don't leak into CCHCS results."""
    mod = _load_rmod()
    result = mod._expiring_contracts_by_dept(seeded_conn, ["4700", "5225"])
    # CDCR seeded contract is at 200d — outside 120d window anyway.
    assert result.get("5225") == []


def test_expiring_empty_dept_list_returns_empty(seeded_conn):
    mod = _load_rmod()
    assert mod._expiring_contracts_by_dept(seeded_conn, []) == {}


# ── E2E: counterfactual ranking (the headline behavior change) ────────────────

def test_card_ranking_rebid_urgency_beats_higher_raw_score(
    auth_client, tmp_path, monkeypatch
):
    """The full-page behavior that V2-PR-1 is built for:
    AGENCY-LOW has raw score 60 + a 45-day expiry (urgency +25).
    AGENCY-HIGH has raw score 80 + no expiring contracts.
    → AGENCY-LOW (60+25=85) should rank ABOVE AGENCY-HIGH (80)."""
    data_dir = tmp_path / "rebid_rank"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "status TEXT, source TEXT, created_at TEXT, is_test INTEGER DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS email_outbox ("
        "id TEXT PRIMARY KEY, to_address TEXT, status TEXT, sent_at TEXT)"
    )
    today = date.today()

    def seed_po(po, dept, dname, supplier, total, end_delta, desc, qty, price, sells, opp, buyer_name=None, buyer_email=None):
        end = (today + timedelta(days=end_delta)).isoformat() if end_delta else ""
        cur = conn.execute(
            "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
            "agency_code, supplier, grand_total, start_date, end_date, "
            "buyer_name, buyer_email) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (po, dept, dname, dept, supplier, total,
             today.isoformat(), end, buyer_name, buyer_email),
        )
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
            "description, quantity, unit_price, line_total, reytech_sells, "
            "opportunity_flag) VALUES (?,?,?,?,?,?,?,?,?)",
            (cur.lastrowid, po, 0, desc, qty, price, qty * price, sells, opp),
        )

    # AGENCY-HIGH: modestly higher raw-score drivers, no expiring contracts.
    # Sized so the +25 urgency boost on AGENCY-LOW is the DECIDING factor,
    # not an arithmetic whitewash. In prod, cards with similar-tier spend
    # are the realistic sort-order flip zone — not 2x-spend differences.
    seed_po("PO-HI-1", "9999", "AGENCY-HIGH", "Cardinal", 15000.0, 500,
            "gauze", 600, 25.0, 1, "WIN_BACK",
            buyer_name="Alice", buyer_email="alice@high.gov")

    # AGENCY-LOW: slightly lower spend, more gap/match mix, plus the
    # 45-day expiring competitor contract → +25 urgency.
    seed_po("PO-LO-1", "8888", "AGENCY-LOW", "Medline", 9000.0, 45,
            "nitrile gloves", 500, 8.0, 1, "WIN_BACK",
            buyer_name="Bob", buyer_email="bob@low.gov")
    seed_po("PO-LO-2", "8888", "AGENCY-LOW", "Medline", 4000.0, 45,
            "abd pads", 300, 10.0, 0, "GAP_ITEM",
            buyer_name="Bob", buyer_email="bob@low.gov")

    conn.commit()
    conn.close()

    @contextmanager
    def _seeded():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()

    monkeypatch.setattr("src.core.db.get_db", _seeded)
    import src.agents.prospect_scorer as scorer
    monkeypatch.setattr(scorer, "_get_db", _seeded)

    resp = auth_client.get("/outreach/next")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")

    # Both agencies appear.
    assert "AGENCY-HIGH" in body
    assert "AGENCY-LOW" in body

    # AGENCY-LOW (urgency-boosted) must appear BEFORE AGENCY-HIGH.
    pos_low = body.index("AGENCY-LOW")
    pos_high = body.index("AGENCY-HIGH")
    assert pos_low < pos_high, (
        "AGENCY-LOW (+rebid urgency) must rank above AGENCY-HIGH "
        "(higher raw score but no expiring contract). V2-PR-1 is the "
        "build that makes this true."
    )

    # The red REBID WINDOW badge renders for AGENCY-LOW.
    assert "REBID WINDOW" in body
    # The urgency-suffix "+25" (from the 45d band) renders in the score pill.
    assert "+25" in body
