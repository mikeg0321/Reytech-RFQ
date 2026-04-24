"""
Tests for V2-PR-6 bid_memory on /outreach/next.

Operator-curated record of every RFQ Reytech received, our bid (if
we bid), the outcome (won/lost/no-bid), the winner's price, and
when their contract ends. Surfaces inline as:
  "🔴 LOST CDCR nitrile gloves — we bid $8.40 vs Medline at $8.12
   (2026-01-15) · their contract ends 2026-07-30"

Tests cover:
  1. Migration 27 schema
  2. _bid_memory_for_depts batched lookup, is_test filter, sort,
     schema-tolerant
  3. _bid_memory_summary label generation per outcome
  4. _bid_memory_urgency: lost + contract ending soon → +10,
     lost recent → +5, won/pending → 0
  5. POST /api/outreach/next/bid validation + upsert
  6. E2E: card renders bid memory + urgency suffix when applicable
  7. /health/quoting bid_memory_health counter
"""
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta

import pytest


def _load_rmod():
    import sys, importlib.util, types
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


# ── Migration 27 ─────────────────────────────────────────────────────────────

def test_migration_27_creates_bid_memory(tmp_path):
    db_path = str(tmp_path / "mig27.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    mig = next(m for m in MIGRATIONS if m[0] == 27)
    conn.executescript(mig[2])
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(bid_memory)").fetchall()}
    for required in {"rfq_id", "received_at", "dept_code", "category",
                     "summary_description", "our_status", "our_bid_amount",
                     "our_bid_per_unit", "outcome", "winning_supplier",
                     "winning_price", "award_date", "contract_end_date",
                     "notes", "source", "updated_by", "is_test",
                     "created_at", "updated_at"}:
        assert required in cols, f"missing: {required}"
    # Idempotent.
    conn.executescript(mig[2])
    conn.close()


# ── _bid_memory_for_depts ────────────────────────────────────────────────────

@pytest.fixture
def bid_conn(tmp_path):
    db_path = str(tmp_path / "bm.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 27)[2])
    return conn


def test_bid_memory_lookup_is_batched_and_per_dept(bid_conn):
    mod = _load_rmod()
    bid_conn.executemany(
        "INSERT INTO bid_memory (rfq_id, dept_code, dept_name, "
        "received_at, outcome, winning_supplier, winning_price, "
        "our_bid_per_unit, summary_description) VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("RFQ-A1", "4700", "CCHCS", "2026-03-15", "lost", "Medline",
             8.12, 8.40, "nitrile gloves"),
            ("RFQ-A2", "4700", "CCHCS", "2026-02-01", "won", "Reytech",
             0, 7.50, "first aid kits"),
            ("RFQ-B1", "5225", "CDCR", "2026-03-01", "pending", "", 0, 0,
             "wipes"),
        ],
    )
    bid_conn.commit()
    result = mod._bid_memory_for_depts(bid_conn, ["4700", "5225"])
    assert set(result.keys()) == {"4700", "5225"}
    assert len(result["4700"]) == 2
    # Most recent first (RFQ-A1 = 2026-03-15 > RFQ-A2 = 2026-02-01).
    assert result["4700"][0]["rfq_id"] == "RFQ-A1"


def test_bid_memory_filters_is_test(bid_conn):
    mod = _load_rmod()
    bid_conn.executemany(
        "INSERT INTO bid_memory (rfq_id, dept_code, received_at, outcome, "
        "is_test) VALUES (?,?,?,?,?)",
        [("REAL", "4700", "2026-03-01", "won", 0),
         ("TEST", "4700", "2026-03-15", "lost", 1)],
    )
    bid_conn.commit()
    result = mod._bid_memory_for_depts(bid_conn, ["4700"])
    rfq_ids = {m["rfq_id"] for m in result["4700"]}
    assert "TEST" not in rfq_ids
    assert "REAL" in rfq_ids


def test_bid_memory_lookup_schema_tolerant(tmp_path):
    """Fresh DB without table → empty dict, no crash."""
    mod = _load_rmod()
    conn = sqlite3.connect(str(tmp_path / "no_bm.db"))
    conn.row_factory = sqlite3.Row
    assert mod._bid_memory_for_depts(conn, ["4700"]) == {}


def test_bid_memory_empty_input(bid_conn):
    mod = _load_rmod()
    assert mod._bid_memory_for_depts(bid_conn, []) == {}


# ── _bid_memory_summary ──────────────────────────────────────────────────────

def test_summary_lost_includes_our_bid_and_winner_price():
    mod = _load_rmod()
    out = mod._bid_memory_summary([{
        "outcome": "lost", "summary_description": "nitrile gloves",
        "our_bid_per_unit": 8.40, "winning_supplier": "Medline",
        "winning_price": 8.12, "award_date": "2026-01-15",
    }])
    label = out[0]["label"]
    assert "🔴 LOST" in label
    assert "$8.40" in label
    assert "Medline" in label
    assert "$8.12" in label
    assert "2026-01-15" in label


def test_summary_won_includes_our_bid_and_award_date():
    mod = _load_rmod()
    out = mod._bid_memory_summary([{
        "outcome": "won", "summary_description": "first aid kits",
        "our_bid_per_unit": 58.0, "award_date": "2026-02-12",
    }])
    label = out[0]["label"]
    assert "✅ WON" in label
    assert "$58.00/unit" in label
    assert "2026-02-12" in label


def test_summary_pending_marks_in_progress():
    mod = _load_rmod()
    out = mod._bid_memory_summary([{
        "outcome": "pending", "summary_description": "PPE",
        "received_at": "2026-04-01",
    }])
    assert "PENDING" in out[0]["label"]


def test_summary_empty_input_returns_empty_list():
    mod = _load_rmod()
    assert mod._bid_memory_summary([]) == []


# ── _bid_memory_urgency ──────────────────────────────────────────────────────

def test_urgency_lost_with_contract_ending_in_window_is_10():
    """LOST + contract ends within rebid window (-30 to +120d) =
    +10 — we know exactly what to beat AND when."""
    mod = _load_rmod()
    today = date.today()
    end = (today + timedelta(days=60)).isoformat()
    boost = mod._bid_memory_urgency([{
        "outcome": "lost", "contract_end_date": end,
    }])
    assert boost == 10


def test_urgency_lost_with_contract_far_future_is_5():
    mod = _load_rmod()
    today = date.today()
    end = (today + timedelta(days=400)).isoformat()
    boost = mod._bid_memory_urgency([{
        "outcome": "lost", "contract_end_date": end,
    }])
    assert boost == 5


def test_urgency_lost_recent_no_contract_end_is_5():
    mod = _load_rmod()
    today = date.today()
    received = (today - timedelta(days=120)).isoformat()
    boost = mod._bid_memory_urgency([{
        "outcome": "lost", "received_at": received,
    }])
    assert boost == 5


def test_urgency_lost_old_no_contract_end_is_zero():
    """Lost a year ago, no contract intel → no actionable boost."""
    mod = _load_rmod()
    today = date.today()
    received = (today - timedelta(days=400)).isoformat()
    boost = mod._bid_memory_urgency([{
        "outcome": "lost", "received_at": received,
    }])
    assert boost == 0


def test_urgency_won_returns_zero():
    mod = _load_rmod()
    boost = mod._bid_memory_urgency([{"outcome": "won"}])
    assert boost == 0


def test_urgency_pending_returns_zero():
    mod = _load_rmod()
    boost = mod._bid_memory_urgency([{"outcome": "pending"}])
    assert boost == 0


def test_urgency_empty_returns_zero():
    mod = _load_rmod()
    assert mod._bid_memory_urgency([]) == 0
    assert mod._bid_memory_urgency(None) == 0


def test_urgency_takes_max_across_multiple_memories():
    mod = _load_rmod()
    today = date.today()
    far = (today + timedelta(days=400)).isoformat()
    near = (today + timedelta(days=60)).isoformat()
    boost = mod._bid_memory_urgency([
        {"outcome": "lost", "contract_end_date": far},   # +5
        {"outcome": "lost", "contract_end_date": near},  # +10
        {"outcome": "won"},                               # 0
    ])
    assert boost == 10


# ── POST /api/outreach/next/bid ─────────────────────────────────────────────

def test_api_bid_rejects_missing_rfq_id(auth_client):
    r = auth_client.post("/api/outreach/next/bid", json={"dept_code": "4700"})
    assert r.status_code == 400


def test_api_bid_rejects_missing_dept_code(auth_client):
    r = auth_client.post("/api/outreach/next/bid", json={"rfq_id": "X"})
    assert r.status_code == 400


def test_api_bid_rejects_invalid_outcome(auth_client):
    r = auth_client.post("/api/outreach/next/bid", json={
        "rfq_id": "X", "dept_code": "4700", "outcome": "banana",
    })
    assert r.status_code == 400


def test_api_bid_rejects_invalid_our_status(auth_client):
    r = auth_client.post("/api/outreach/next/bid", json={
        "rfq_id": "X", "dept_code": "4700", "our_status": "banana",
    })
    assert r.status_code == 400


def test_api_bid_rejects_malformed_dates(auth_client):
    r = auth_client.post("/api/outreach/next/bid", json={
        "rfq_id": "X", "dept_code": "4700", "received_at": "junk",
    })
    assert r.status_code == 400


def test_api_bid_upserts_and_returns_record(auth_client, tmp_path, monkeypatch):
    db_path = str(tmp_path / "upsert_bid.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 27)[2])
    conn.close()

    @contextmanager
    def _seeded():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()
    monkeypatch.setattr("src.core.db.get_db", _seeded)

    # INSERT
    r = auth_client.post("/api/outreach/next/bid", json={
        "rfq_id": "RFQ-NEW", "dept_code": "4700", "dept_name": "CCHCS",
        "outcome": "lost", "winning_supplier": "Medline",
        "winning_price": 8.12, "our_bid_per_unit": 8.40,
        "received_at": "2026-01-15",
        "contract_end_date": (date.today() + timedelta(days=60)).isoformat(),
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["record"]["outcome"] == "lost"
    assert data["record"]["winning_price"] == 8.12

    # UPDATE
    r2 = auth_client.post("/api/outreach/next/bid", json={
        "rfq_id": "RFQ-NEW", "dept_code": "4700", "outcome": "won",
    })
    assert r2.status_code == 200
    assert r2.get_json()["record"]["outcome"] == "won"
    # Single row remains.
    with _seeded() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM bid_memory WHERE rfq_id = 'RFQ-NEW'"
        ).fetchone()[0]
    assert n == 1


# ── E2E: card renders bid memory + urgency ──────────────────────────────────

def test_e2e_card_with_bid_memory_renders_block_and_urgency(
    auth_client, tmp_path, monkeypatch
):
    data_dir = tmp_path / "bm_e2e"
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
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 27)[2])
    today = date.today()
    cur = conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "agency_code, supplier, grand_total, start_date, buyer_name, "
        "buyer_email) VALUES (?,?,?,?,?,?,?,?,?)",
        ("PO-CCHCS-1", "4700", "CCHCS", "4700", "Medline", 12000.0,
         today.isoformat(), "Jane", "jane@cchcs.gov"),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, category, quantity, unit_price, line_total, "
        "reytech_sells, opportunity_flag) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (cur.lastrowid, "PO-CCHCS-1", 0, "nitrile gloves M",
         "exam_gloves", 500, 8.0, 4000.0, 1, "WIN_BACK"),
    )
    end = (today + timedelta(days=60)).isoformat()
    conn.execute(
        "INSERT INTO bid_memory (rfq_id, dept_code, dept_name, "
        "summary_description, outcome, winning_supplier, winning_price, "
        "our_bid_per_unit, received_at, contract_end_date) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        ("RFQ-PRIOR-1", "4700", "CCHCS", "nitrile gloves M",
         "lost", "Medline", 8.12, 8.40, "2026-01-15", end),
    )
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
    # Block header + lost line + urgency suffix all visible.
    assert "Bid history at this agency" in body
    assert "LOST" in body
    assert "Medline" in body
    assert "$8.40" in body
    assert "$8.12" in body
    # +10 urgency should be visible (lost + contract ends in 60d → +10).
    assert "+10" in body


# ── /health/quoting bid_memory_health counter ────────────────────────────────

def test_health_quoting_exposes_bid_memory_counter(
    auth_client, tmp_path, monkeypatch
):
    db_path = str(tmp_path / "bm_health.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 27)[2])
    conn.executemany(
        "INSERT INTO bid_memory (rfq_id, dept_code, outcome) VALUES (?,?,?)",
        [("R1", "4700", "won"), ("R2", "4700", "won"),
         ("R3", "5225", "lost"), ("R4", "4700", "pending")],
    )
    conn.commit()
    conn.close()

    @contextmanager
    def _seeded():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()
    monkeypatch.setattr("src.core.db.get_db", _seeded)

    r = auth_client.get("/api/health/quoting")
    assert r.status_code == 200
    data = r.get_json()
    assert "bid_memory_health" in data
    bh = data["bid_memory_health"]
    assert bh["ok"] is True
    assert bh["total"] == 4
    assert bh["by_outcome"]["won"] == 2
    assert bh["by_outcome"]["lost"] == 1
    assert bh["by_outcome"]["pending"] == 1
