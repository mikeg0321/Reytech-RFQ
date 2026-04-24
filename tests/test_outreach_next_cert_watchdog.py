"""
Tests for V2-PR-4 Reytech certification expiry watchdog.

Procurement-lens framing: SB / MB / DVBE / OSDS certifications are
the legal mechanism that gives Reytech preference on set-aside RFQs.
Silent cert expiry = invisible loss of every set-aside bid downstream.

This module tests:
  1. Migration 26 creates reytech_certifications with required columns.
  2. _cert_status_summary classifies aggregate state into 4 levels:
     critical (any expired) > warn (any ≤60d) > ok (all >60d) > none
     (no certs on file).
  3. POST /api/outreach/next/cert validates cert_type + ISO dates,
     upserts by cert_type.
  4. Page route renders cert banner above cards in each state.
  5. /health/quoting cert_health counter reflects state.
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


# ── Migration 26 ─────────────────────────────────────────────────────────────

def test_migration_26_creates_table_with_required_columns(tmp_path):
    db_path = str(tmp_path / "mig26.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    mig = next(m for m in MIGRATIONS if m[0] == 26)
    conn.executescript(mig[2])
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(reytech_certifications)").fetchall()}
    for required in {"cert_type", "cert_number", "issue_date",
                     "expires_at", "renewal_url", "notes",
                     "is_active", "is_test", "created_at", "updated_at"}:
        assert required in cols, f"missing: {required}"
    # cert_type is UNIQUE.
    idxs = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' "
        "AND tbl_name='reytech_certifications'"
    ).fetchall()
    assert any("cert_type" in (i[0] or "").lower() or "sqlite_autoindex"
               in (i[0] or "") for i in idxs)
    # Idempotent.
    conn.executescript(mig[2])
    conn.close()


# ── _cert_status_summary classifier ──────────────────────────────────────────

@pytest.fixture
def cert_conn(tmp_path):
    db_path = str(tmp_path / "certs.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 26)[2])
    return conn


def _seed_cert(conn, cert_type, expires_delta_days, is_active=1, is_test=0):
    today = date.today()
    expires = (today + timedelta(days=expires_delta_days)).isoformat() if expires_delta_days is not None else ""
    conn.execute(
        "INSERT INTO reytech_certifications (cert_type, cert_number, "
        "expires_at, is_active, is_test) VALUES (?, ?, ?, ?, ?)",
        (cert_type, f"#{cert_type}-001", expires, is_active, is_test),
    )
    conn.commit()


def test_cert_summary_none_when_no_certs(cert_conn):
    mod = _load_rmod()
    s = mod._cert_status_summary(cert_conn)
    assert s["summary"]["level"] == "none"
    assert s["certs"] == []
    assert "Add SB" in s["summary"]["hint"]


def test_cert_summary_ok_when_all_far_future(cert_conn):
    mod = _load_rmod()
    _seed_cert(cert_conn, "SB", 365)
    _seed_cert(cert_conn, "DVBE", 200)
    s = mod._cert_status_summary(cert_conn)
    assert s["summary"]["level"] == "ok"
    assert s["summary"]["expired"] == 0
    assert s["summary"]["expiring_soon"] == 0


def test_cert_summary_warn_on_60d_expiry(cert_conn):
    mod = _load_rmod()
    _seed_cert(cert_conn, "SB", 365)
    _seed_cert(cert_conn, "DVBE", 45)  # within 60d → warn
    s = mod._cert_status_summary(cert_conn)
    assert s["summary"]["level"] == "warn"
    assert s["summary"]["expiring_soon"] == 1
    assert "DVBE" in s["summary"]["label"]


def test_cert_summary_critical_on_any_expired(cert_conn):
    mod = _load_rmod()
    _seed_cert(cert_conn, "SB", 365)       # ok
    _seed_cert(cert_conn, "DVBE", -10)     # EXPIRED
    s = mod._cert_status_summary(cert_conn)
    assert s["summary"]["level"] == "critical"
    assert s["summary"]["expired"] == 1
    assert "EXPIRED" in s["summary"]["label"]


def test_cert_summary_critical_takes_precedence_over_warn(cert_conn):
    mod = _load_rmod()
    _seed_cert(cert_conn, "SB", -5)        # expired
    _seed_cert(cert_conn, "DVBE", 30)      # warn
    s = mod._cert_status_summary(cert_conn)
    assert s["summary"]["level"] == "critical"


def test_cert_summary_filters_inactive_certs(cert_conn):
    mod = _load_rmod()
    _seed_cert(cert_conn, "SB", -10, is_active=0)  # inactive — ignored
    _seed_cert(cert_conn, "DVBE", 365)
    s = mod._cert_status_summary(cert_conn)
    # Only the active DVBE counts → ok.
    assert s["summary"]["level"] == "ok"
    assert s["summary"]["total"] == 1


def test_cert_summary_filters_is_test(cert_conn):
    mod = _load_rmod()
    _seed_cert(cert_conn, "SB", -10, is_test=1)   # test — ignored
    _seed_cert(cert_conn, "DVBE", 365)
    s = mod._cert_status_summary(cert_conn)
    assert s["summary"]["level"] == "ok"
    assert s["summary"]["total"] == 1


def test_cert_summary_schema_tolerant_without_table(tmp_path):
    """Fresh DB pre-migration → none-state, no crash."""
    mod = _load_rmod()
    conn = sqlite3.connect(str(tmp_path / "no_cert_table.db"))
    conn.row_factory = sqlite3.Row
    s = mod._cert_status_summary(conn)
    assert s["summary"]["level"] == "none"


# ── POST /api/outreach/next/cert ─────────────────────────────────────────────

def test_api_cert_rejects_missing_cert_type(auth_client):
    r = auth_client.post("/api/outreach/next/cert",
                         json={"cert_number": "#123"})
    assert r.status_code == 400
    assert "cert_type required" in r.get_json()["error"]


def test_api_cert_rejects_malformed_expires_at(auth_client):
    r = auth_client.post("/api/outreach/next/cert",
                         json={"cert_type": "SB", "expires_at": "junk"})
    assert r.status_code == 400
    assert "expires_at" in r.get_json()["error"]


def test_api_cert_upserts_and_returns_record(auth_client, tmp_path, monkeypatch):
    db_path = str(tmp_path / "upsert_cert.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 26)[2])
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

    # INSERT.
    r = auth_client.post("/api/outreach/next/cert", json={
        "cert_type": "DVBE", "cert_number": "DVBE-9999",
        "issue_date": "2026-01-15",
        "expires_at": (date.today() + timedelta(days=200)).isoformat(),
        "renewal_url": "https://osds.example.gov/renew",
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["record"]["cert_type"] == "DVBE"
    assert data["cert_status"]["summary"]["level"] == "ok"

    # UPDATE — change expires_at to soon → level flips to warn.
    r2 = auth_client.post("/api/outreach/next/cert", json={
        "cert_type": "DVBE",
        "expires_at": (date.today() + timedelta(days=30)).isoformat(),
    })
    assert r2.status_code == 200
    assert r2.get_json()["cert_status"]["summary"]["level"] == "warn"
    # Single row remains.
    with _seeded() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM reytech_certifications "
            "WHERE cert_type = 'DVBE'"
        ).fetchone()[0]
    assert n == 1


def test_api_cert_normalizes_cert_type_to_upper(auth_client, tmp_path, monkeypatch):
    db_path = str(tmp_path / "upper_cert.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 26)[2])
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

    r = auth_client.post("/api/outreach/next/cert",
                         json={"cert_type": "dvbe"})
    assert r.status_code == 200
    assert r.get_json()["record"]["cert_type"] == "DVBE"


# ── E2E: page renders cert banner ────────────────────────────────────────────

def test_e2e_cert_banner_critical_renders_above_cards(
    auth_client, tmp_path, monkeypatch
):
    """Expired SB cert + scprs prospects → critical banner appears
    above the card list."""
    data_dir = tmp_path / "cert_e2e"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db)
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
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 26)[2])
    today = date.today()
    conn.execute(
        "INSERT INTO reytech_certifications (cert_type, expires_at, is_active) "
        "VALUES ('SB', ?, 1)",
        ((today - timedelta(days=10)).isoformat(),),
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
    assert "Reytech certifications" in body
    assert "SB" in body
    assert "EXPIRED" in body or "expired" in body.lower()


def test_e2e_cert_banner_hidden_when_no_certs(
    auth_client, tmp_path, monkeypatch
):
    data_dir = tmp_path / "cert_none"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db)
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
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 26)[2])
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
    body = resp.data.decode("utf-8", errors="replace")
    # No certs in DB → banner should not render.
    assert "Reytech certifications:" not in body


# ── /health/quoting cert counter ─────────────────────────────────────────────

def test_health_quoting_exposes_cert_health(auth_client, tmp_path, monkeypatch):
    db_path = str(tmp_path / "cert_health.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 26)[2])
    today = date.today()
    conn.executemany(
        "INSERT INTO reytech_certifications (cert_type, expires_at, is_active) "
        "VALUES (?,?,1)",
        [
            ("SB", (today + timedelta(days=200)).isoformat()),     # ok
            ("DVBE", (today + timedelta(days=30)).isoformat()),    # expiring_soon
            ("OSDS", (today - timedelta(days=5)).isoformat()),     # expired
        ],
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
    assert "cert_health" in data
    ch = data["cert_health"]
    assert ch["ok"] is True
    assert ch["total"] == 3
    assert ch["expired"] == 1
    assert ch["expiring_soon"] == 1
    assert ch["by_type"]["SB"] == "ok"
    assert ch["by_type"]["DVBE"] == "expiring_soon"
    assert ch["by_type"]["OSDS"] == "expired"
