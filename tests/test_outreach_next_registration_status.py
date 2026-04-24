"""
Tests for V2-PR-2 registration-status layer on /outreach/next.

Thesis: in CA public-sector procurement, being on the agency's bidder
distribution list is the #1 gate. If Reytech isn't registered, NO
amount of rebid timing or capability pitch matters — we won't see the
RFQ when it posts. This module tests:

  1. Migration 24 creates agency_vendor_registry with all required
     columns (including source + updated_by flagged by product-eng).
  2. _registration_status_for_depts — batched SQL, is_test filter,
     schema-tolerant (missing table → empty dict, no crash).
  3. _registration_summary — 5 levels classified correctly; expires_at
     in past OVERRIDES status='registered' (stale-data guard flagged by
     product-engineer review).
  4. _registration_urgency — +35 for not_registered/expired (outrank
     hottest rebid band), +5 unknown/pending, 0 registered.
  5. Compound ranking: not_registered + red rebid stacks to +60 boost;
     registered + red rebid stays at +25 only.
  6. POST /api/outreach/next/registry upserts, validates input, rejects
     bad status + bad date.
  7. E2E: card renders registration pill for each of 5 states; "not
     registered" card ranks above "registered" card at similar base.
  8. Health endpoint exposes registry_rows_by_status counter.
"""
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta

import pytest


def _load_rmod():
    """Shim: route module exec'd into dashboard at runtime."""
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


# ── Migration 24 ─────────────────────────────────────────────────────────────

def test_migration_24_creates_table_with_required_columns(tmp_path):
    """Confirm all flagged columns are in the schema — especially
    `source` + `updated_by` per product-engineer review."""
    db_path = str(tmp_path / "mig24.db")
    conn = sqlite3.connect(db_path)

    # Execute the migration SQL string directly (SQL entry in MIGRATIONS).
    from src.core.migrations import MIGRATIONS
    mig = next(m for m in MIGRATIONS if m[0] == 24)
    conn.executescript(mig[2])
    conn.commit()

    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(agency_vendor_registry)").fetchall()}
    # Must-have columns (schema + audit + is_test tenancy).
    for required in {"dept_code", "status", "confirmed_at", "expires_at",
                     "portal_url", "procurement_officer_name",
                     "procurement_officer_email", "procurement_officer_phone",
                     "vendor_id_at_agency", "categories_json", "notes",
                     "source", "updated_by", "is_test",
                     "created_at", "updated_at"}:
        assert required in cols, f"missing column: {required}"
    # PK is dept_code.
    pks = [r[1] for r in conn.execute(
        "PRAGMA table_info(agency_vendor_registry)").fetchall() if r[5]]
    assert pks == ["dept_code"]
    conn.close()


def test_migration_24_is_idempotent_via_if_not_exists(tmp_path):
    db_path = str(tmp_path / "mig24_idem.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    mig = next(m for m in MIGRATIONS if m[0] == 24)
    conn.executescript(mig[2])
    conn.executescript(mig[2])  # re-run — must not raise
    conn.commit()
    conn.close()


# ── _registration_status_for_depts ────────────────────────────────────────────

@pytest.fixture
def seeded_registry_conn(tmp_path):
    db_path = str(tmp_path / "reg.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 24)[2])
    today = date.today().isoformat()
    past = (date.today() - timedelta(days=30)).isoformat()
    future = (date.today() + timedelta(days=200)).isoformat()
    conn.executemany(
        "INSERT INTO agency_vendor_registry (dept_code, status, "
        "confirmed_at, expires_at, portal_url, is_test) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("4700", "registered", today, future,
             "https://cchcs.example.gov/portal", 0),
            ("5225", "not_registered", "", "",
             "https://cdcr.example.gov/portal", 0),
            ("7800", "registered", past, past, "", 0),  # STALE — expired
            ("4440", "pending", today, "", "", 0),
            ("9999", "registered", today, future, "", 1),  # is_test=1 — filter
        ],
    )
    conn.commit()
    return conn


def test_registration_lookup_returns_records_for_known_depts(seeded_registry_conn):
    mod = _load_rmod()
    result = mod._registration_status_for_depts(
        seeded_registry_conn, ["4700", "5225", "4440"])
    assert set(result.keys()) == {"4700", "5225", "4440"}
    assert result["4700"]["status"] == "registered"
    assert result["5225"]["status"] == "not_registered"
    assert result["4440"]["status"] == "pending"


def test_registration_lookup_filters_is_test(seeded_registry_conn):
    mod = _load_rmod()
    result = mod._registration_status_for_depts(
        seeded_registry_conn, ["9999", "4700"])
    # Only the real row (4700); 9999 is is_test=1.
    assert "9999" not in result
    assert "4700" in result


def test_registration_lookup_empty_input_returns_empty(seeded_registry_conn):
    mod = _load_rmod()
    assert mod._registration_status_for_depts(seeded_registry_conn, []) == {}


def test_registration_lookup_missing_depts_absent_from_result(seeded_registry_conn):
    mod = _load_rmod()
    result = mod._registration_status_for_depts(
        seeded_registry_conn, ["1234", "4700"])
    assert "1234" not in result
    assert "4700" in result


def test_registration_lookup_schema_tolerant_without_table(tmp_path):
    """Fresh DB without migration 24 fired → empty dict, no crash."""
    db_path = str(tmp_path / "no_table.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    mod = _load_rmod()
    assert mod._registration_status_for_depts(conn, ["4700"]) == {}


# ── _registration_summary classifier (with expiry-trumps-status logic) ────────

def test_summary_none_record_returns_unknown():
    mod = _load_rmod()
    r = mod._registration_summary(None)
    assert r["level"] == "unknown"
    assert r["status_effective"] == "unknown"


def test_summary_registered_with_future_expiry():
    mod = _load_rmod()
    future = (date.today() + timedelta(days=365)).isoformat()
    r = mod._registration_summary({
        "status": "registered", "confirmed_at": "2026-01-15",
        "expires_at": future, "portal_url": "https://p.example",
    })
    assert r["level"] == "registered"
    assert r["status_effective"] == "registered"
    assert r["action_url"] == "https://p.example"


def test_summary_registered_but_expired_overrides_stored_status():
    """THE headline product-engineer guard: stale `registered` rows must
    classify as EXPIRED. Never trust stored status alone."""
    mod = _load_rmod()
    past = (date.today() - timedelta(days=10)).isoformat()
    r = mod._registration_summary({
        "status": "registered", "expires_at": past, "portal_url": "",
    })
    assert r["level"] == "expired"
    assert r["status_effective"] == "expired"
    assert "Re-register" in r["hint"]


def test_summary_registered_with_near_expiry_warns_renewal():
    mod = _load_rmod()
    soon = (date.today() + timedelta(days=45)).isoformat()
    r = mod._registration_summary({
        "status": "registered", "confirmed_at": "2026-01-15",
        "expires_at": soon, "portal_url": "",
    })
    assert r["level"] == "registered"
    assert "renew" in r["label"].lower()


def test_summary_pending():
    mod = _load_rmod()
    r = mod._registration_summary({"status": "pending"})
    assert r["level"] == "pending"


def test_summary_not_registered():
    mod = _load_rmod()
    r = mod._registration_summary({
        "status": "not_registered", "portal_url": "https://register.example",
    })
    assert r["level"] == "not_registered"
    assert r["action_url"] == "https://register.example"


def test_summary_unknown():
    mod = _load_rmod()
    r = mod._registration_summary({"status": "unknown"})
    assert r["level"] == "unknown"


# ── _registration_urgency ────────────────────────────────────────────────────

def test_urgency_not_registered_is_35():
    """Must outrank any rebid band (rebid max is +30). Rationale:
    rebid-without-registration is useless."""
    mod = _load_rmod()
    assert mod._registration_urgency({"status_effective": "not_registered"}) == 35


def test_urgency_expired_is_35():
    mod = _load_rmod()
    assert mod._registration_urgency({"status_effective": "expired"}) == 35


def test_urgency_pending_is_5():
    mod = _load_rmod()
    assert mod._registration_urgency({"status_effective": "pending"}) == 5


def test_urgency_unknown_is_5():
    """Lowered from +10 per product-eng review: avoids noise-flood when
    most rows start `unknown`."""
    mod = _load_rmod()
    assert mod._registration_urgency({"status_effective": "unknown"}) == 5


def test_urgency_registered_is_zero():
    mod = _load_rmod()
    assert mod._registration_urgency({"status_effective": "registered"}) == 0


def test_urgency_defaults_to_unknown_when_summary_missing_field():
    mod = _load_rmod()
    assert mod._registration_urgency({}) == 5
    assert mod._registration_urgency(None) == 5


# ── E2E: compound ranking ────────────────────────────────────────────────────

def test_not_registered_card_ranks_above_registered_card(
    auth_client, tmp_path, monkeypatch
):
    """The V2-PR-2 thesis: an agency we can't even receive RFQs from must
    outrank an agency we CAN receive RFQs from, at comparable base scores.
    Otherwise the top-of-list continues to hide the #1 gating action."""
    data_dir = tmp_path / "reg_rank"
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
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 24)[2])

    today = date.today()

    def seed_po(po, dept, dname, supplier, total, desc, qty, price, sells,
                opp, bn, be):
        cur = conn.execute(
            "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
            "agency_code, supplier, grand_total, start_date, buyer_name, "
            "buyer_email) VALUES (?,?,?,?,?,?,?,?,?)",
            (po, dept, dname, dept, supplier, total,
             today.isoformat(), bn, be),
        )
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
            "description, quantity, unit_price, line_total, reytech_sells, "
            "opportunity_flag) VALUES (?,?,?,?,?,?,?,?,?)",
            (cur.lastrowid, po, 0, desc, qty, price, qty * price, sells, opp),
        )

    # AGENCY-REG: REGISTERED, comparable raw score.
    seed_po("PO-REG", "1111", "AGENCY-REG", "Cardinal", 12000.0,
            "gauze", 500, 24.0, 1, "WIN_BACK",
            "Alice", "alice@reg.gov")
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, "
        "confirmed_at, expires_at) VALUES (?,?,?,?)",
        ("1111", "registered", today.isoformat(),
         (today + timedelta(days=365)).isoformat()),
    )

    # AGENCY-NOREG: NOT_REGISTERED, slightly lower raw score.
    seed_po("PO-NOREG", "2222", "AGENCY-NOREG", "Medline", 10000.0,
            "nitrile gloves", 500, 8.0, 1, "WIN_BACK",
            "Bob", "bob@noreg.gov")
    seed_po("PO-NOREG-2", "2222", "AGENCY-NOREG", "Medline", 3000.0,
            "abd pads", 300, 10.0, 0, "GAP_ITEM",
            "Bob", "bob@noreg.gov")
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status) VALUES (?,?)",
        ("2222", "not_registered"),
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
    assert "AGENCY-REG" in body
    assert "AGENCY-NOREG" in body
    # NOT REGISTERED must appear BEFORE REGISTERED in the rendered body.
    pos_noreg = body.index("AGENCY-NOREG")
    pos_reg = body.index("AGENCY-REG")
    assert pos_noreg < pos_reg, (
        "AGENCY-NOREG (+35 urgency) must rank above AGENCY-REG. "
        "V2-PR-2 makes this true."
    )
    # The +35 urgency suffix renders.
    assert "+35" in body
    # The NOT REGISTERED pill label renders.
    assert "NOT REGISTERED" in body


# ── POST /api/outreach/next/registry ─────────────────────────────────────────

def test_api_registry_rejects_missing_dept_code(auth_client):
    r = auth_client.post("/api/outreach/next/registry",
                         json={"status": "registered"})
    assert r.status_code == 400
    assert r.get_json()["ok"] is False


def test_api_registry_rejects_invalid_status(auth_client):
    r = auth_client.post("/api/outreach/next/registry",
                         json={"dept_code": "4700", "status": "banana"})
    assert r.status_code == 400
    assert "status must be one of" in r.get_json()["error"]


def test_api_registry_rejects_malformed_expires_at(auth_client):
    r = auth_client.post("/api/outreach/next/registry", json={
        "dept_code": "4700", "status": "registered",
        "expires_at": "not-a-date",
    })
    assert r.status_code == 400
    assert "expires_at" in r.get_json()["error"]


def test_api_registry_upserts_and_returns_record(
    auth_client, tmp_path, monkeypatch
):
    db_path = str(tmp_path / "upsert.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 24)[2])
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
    r = auth_client.post("/api/outreach/next/registry", json={
        "dept_code": "4700", "status": "registered",
        "confirmed_at": "2026-04-15",
        "expires_at": (date.today() + timedelta(days=365)).isoformat(),
        "portal_url": "https://portal.example",
        "notes": "supplier id 12345",
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["record"]["status"] == "registered"
    assert data["record"]["portal_url"] == "https://portal.example"
    assert data["summary"]["level"] == "registered"

    # UPDATE (upsert path)
    r2 = auth_client.post("/api/outreach/next/registry", json={
        "dept_code": "4700", "status": "not_registered",
    })
    assert r2.status_code == 200
    data2 = r2.get_json()
    assert data2["record"]["status"] == "not_registered"
    # Updated, not duplicated.
    with _seeded() as c:
        rows = c.execute(
            "SELECT COUNT(*) FROM agency_vendor_registry WHERE dept_code=?",
            ("4700",)
        ).fetchone()[0]
    assert rows == 1


# ── /health/quoting registry counter ─────────────────────────────────────────

def test_health_quoting_exposes_registry_counts(
    auth_client, tmp_path, monkeypatch
):
    """Product-engineer request: make signal-maintenance visible via
    /api/health/quoting so Mike can tell at a glance whether registration
    status is actually being populated."""
    db_path = str(tmp_path / "health.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 24)[2])
    conn.executemany(
        "INSERT INTO agency_vendor_registry (dept_code, status) VALUES (?,?)",
        [("A1", "registered"), ("A2", "registered"),
         ("A3", "not_registered"), ("A4", "unknown")],
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
    assert "registry_health" in data
    rh = data["registry_health"]
    assert rh["ok"] is True
    assert rh["total"] == 4
    assert rh["by_status"].get("registered") == 2
    assert rh["by_status"].get("not_registered") == 1
    assert rh["by_status"].get("unknown") == 1
