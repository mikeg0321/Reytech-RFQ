"""
Tests for V2-PR-7 registration_gap_detector + Gmail bulk-seed agent.

Per 2026-04-25 product-engineer pre-build review, the must-do edits are:
  1. Auto-promote at high-confidence + ≥3 RFQs (real automation)
  2. evidence_message_ids JSON column for audit
  3. Reuse src/core/gmail_api (don't fork a new module)
  Plus: skip operator/imported rows, dedupe by thread_id, queue
  unmapped domains, skip ambiguous bare @ca.gov, cap message scan.

This module tests every must-do plus the API surface.
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest


def _seed_schema(conn):
    """Apply migrations 24 + 27 + 28 (programmatic 28 logic inline)."""
    from src.core.migrations import MIGRATIONS, _run_migration_28
    # agency_vendor_registry from 24
    for v in (24, 27):
        conn.executescript(next(m for m in MIGRATIONS if m[0] == v)[2])
    # 28 is programmatic
    _run_migration_28(conn)
    # scprs tables for the detector
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT UNIQUE, dept_code TEXT, dept_name TEXT,
            grand_total REAL, start_date TEXT, is_test INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER, po_number TEXT, line_num INTEGER,
            description TEXT, line_total REAL, is_test INTEGER DEFAULT 0
        );
    """)


# ── Migration 28 ─────────────────────────────────────────────────────────────

def test_migration_28_creates_alias_tables_and_seeds_known_domains(tmp_path):
    db_path = str(tmp_path / "mig28.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    # Tables exist.
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "agency_domain_aliases" in tables
    assert "agency_pending_aliases" in tables
    # Pre-seeded with known CA agency domains.
    aliases = {r["domain"]: dict(r) for r in conn.execute(
        "SELECT * FROM agency_domain_aliases").fetchall()}
    assert "cdcr.ca.gov" in aliases
    assert aliases["cdcr.ca.gov"]["dept_code"] == "5225"
    assert aliases["cchcs.ca.gov"]["dept_code"] == "4700"
    assert aliases["cdcr.ca.gov"]["confidence"] == "high"


def test_migration_28_adds_columns_to_agency_vendor_registry(tmp_path):
    db_path = str(tmp_path / "mig28_cols.db")
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(agency_vendor_registry)").fetchall()}
    assert "is_provisional" in cols
    assert "evidence_message_ids" in cols


def test_migration_28_idempotent(tmp_path):
    db_path = str(tmp_path / "mig28_idem.db")
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    from src.core.migrations import _run_migration_28
    _run_migration_28(conn)  # re-run — must not raise
    _run_migration_28(conn)  # third time — must not raise


# ── detect_registration_gaps ─────────────────────────────────────────────────

@pytest.fixture
def gap_conn(tmp_path, monkeypatch):
    db_path = str(tmp_path / "gap.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    # Seed top agencies with varying registry status.
    conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "grand_total, start_date) VALUES (?,?,?,?,date('now','-30 days'))",
        ("PO-A", "4700", "CCHCS", 12000.0),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, line_total) VALUES (?,?,?,?,?)",
        (1, "PO-A", 0, "gloves", 12000.0),
    )
    conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "grand_total, start_date) VALUES (?,?,?,?,date('now','-60 days'))",
        ("PO-B", "5225", "CDCR", 8000.0),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, line_total) VALUES (?,?,?,?,?)",
        (2, "PO-B", 0, "wipes", 8000.0),
    )
    # CCHCS = registered (operator) — NOT a gap
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source) "
        "VALUES ('4700', 'registered', 'operator')"
    )
    # CDCR has no registry row — gap
    conn.commit()

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
    return conn


def test_detect_registration_gaps_returns_no_record_agencies(gap_conn):
    from src.agents.registration_gap_detector import detect_registration_gaps
    result = detect_registration_gaps(top_n=10)
    assert result["ok"] is True
    gap_codes = {g["dept_code"] for g in result["gaps"]}
    # CDCR (5225) has no registry row → gap.
    assert "5225" in gap_codes
    # CCHCS (4700) has registered operator status → NOT a gap.
    assert "4700" not in gap_codes
    # The CDCR gap entry has the right metadata.
    cdcr_gap = next(g for g in result["gaps"] if g["dept_code"] == "5225")
    assert cdcr_gap["gap_status"] == "no_record"
    assert cdcr_gap["total_spend"] == 8000.0


def test_detect_registration_gaps_flags_unknown_status(tmp_path, monkeypatch):
    db_path = str(tmp_path / "unknown_gap.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "grand_total, start_date) VALUES (?,?,?,?,date('now','-30 days'))",
        ("PO-X", "9999", "UnknownAgency", 5000.0),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, line_total) VALUES (?,?,?,?,?)",
        (1, "PO-X", 0, "x", 5000.0),
    )
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source) "
        "VALUES ('9999', 'unknown', 'operator')"
    )
    conn.commit()

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
    from src.agents.registration_gap_detector import detect_registration_gaps
    result = detect_registration_gaps(top_n=10)
    gap = next((g for g in result["gaps"] if g["dept_code"] == "9999"), None)
    assert gap is not None
    assert gap["gap_status"] == "unknown"


# ── Helpers ──────────────────────────────────────────────────────────────────

def test_extract_domain_handles_angle_brackets():
    from src.agents.registration_gap_detector import _extract_domain
    assert _extract_domain("Jane Buyer <jane@cchcs.ca.gov>") == "cchcs.ca.gov"


def test_extract_domain_handles_bare_email():
    from src.agents.registration_gap_detector import _extract_domain
    assert _extract_domain("jane@cdcr.ca.gov") == "cdcr.ca.gov"


def test_extract_domain_lowers_case():
    from src.agents.registration_gap_detector import _extract_domain
    assert _extract_domain("Jane <JANE@CCHCS.CA.GOV>") == "cchcs.ca.gov"


def test_extract_domain_returns_none_on_garbage():
    from src.agents.registration_gap_detector import _extract_domain
    assert _extract_domain("") is None
    assert _extract_domain("not an email") is None


def test_is_rfq_subject_matches_common_patterns():
    from src.agents.registration_gap_detector import _is_rfq_subject
    assert _is_rfq_subject("RFQ #2026-001 — Nitrile Gloves")
    assert _is_rfq_subject("Request for Quote: Wound care")
    assert _is_rfq_subject("Solicitation 12345")
    assert _is_rfq_subject("RFP for medical supplies")
    assert _is_rfq_subject("IFB notice")
    assert _is_rfq_subject("Bid invitation closing 5/1")


def test_is_rfq_subject_skips_non_rfq():
    from src.agents.registration_gap_detector import _is_rfq_subject
    assert not _is_rfq_subject("Order confirmation")
    assert not _is_rfq_subject("Out of office")
    assert not _is_rfq_subject("Payment received")
    assert not _is_rfq_subject("")


# ── gmail_bulk_seed_registrations ────────────────────────────────────────────

def _stub_gmail(messages):
    """Build a fake gmail_api module surface that yields the given list."""
    fake = MagicMock()
    fake.is_configured.return_value = True
    fake.get_service.return_value = MagicMock()
    fake.list_message_ids.return_value = [m["id"] for m in messages]
    fake.get_message_metadata.side_effect = lambda svc, mid: next(
        (m for m in messages if m["id"] == mid), {})
    return fake


def test_bulk_seed_dry_run_proposes_without_writing(tmp_path, monkeypatch):
    db_path = str(tmp_path / "bs_dry.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.commit()

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

    # 4 RFQ messages from CCHCS across 4 distinct threads.
    messages = [
        {"id": "m1", "thread_id": "t1", "subject": "RFQ 2026-001",
         "from": "jane@cchcs.ca.gov"},
        {"id": "m2", "thread_id": "t2", "subject": "RFQ 2026-002",
         "from": "jane@cchcs.ca.gov"},
        {"id": "m3", "thread_id": "t3", "subject": "RFQ 2026-003",
         "from": "jane@cchcs.ca.gov"},
        {"id": "m4", "thread_id": "t4", "subject": "Solicitation 99",
         "from": "bob@cchcs.ca.gov"},
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=True, limit=200)
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["matched_messages"] == 4
    assert result["domains_seen"] == 1
    # 1 dept proposed (4700 = CCHCS).
    assert len(result["proposed"]) == 1
    assert result["proposed"][0]["dept_code"] == "4700"
    assert result["proposed"][0]["thread_count"] == 4
    # Auto-promoted: high confidence + ≥3 threads → is_provisional=0.
    assert result["proposed"][0]["is_provisional"] == 0
    # Dry-run did NOT write.
    with _seeded() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM agency_vendor_registry"
        ).fetchone()[0]
    assert n == 0


def test_bulk_seed_writes_and_auto_promotes_high_confidence(tmp_path, monkeypatch):
    db_path = str(tmp_path / "bs_live.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.commit()

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

    messages = [
        {"id": f"m{i}", "thread_id": f"t{i}",
         "subject": f"RFQ 2026-{i:03d}",
         "from": "ops@cdcr.ca.gov"}
        for i in range(1, 5)  # 4 distinct CDCR RFQs
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=False, limit=200)
    assert result["ok"] is True
    assert result["rows_upserted"] == 1

    with _seeded() as c:
        row = c.execute(
            "SELECT * FROM agency_vendor_registry WHERE dept_code='5225'"
        ).fetchone()
    assert row is not None
    assert row["status"] == "registered"
    assert row["source"] == "agent"
    # Auto-promoted: high-confidence + ≥3 threads → is_provisional=0.
    assert row["is_provisional"] == 0
    # Evidence persisted as JSON.
    evidence = json.loads(row["evidence_message_ids"])
    assert len(evidence) == 4


def test_bulk_seed_marks_provisional_below_three_threads(tmp_path, monkeypatch):
    db_path = str(tmp_path / "bs_prov.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.commit()

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

    # Only 2 RFQ threads from CalVet — below ≥3 auto-promote threshold.
    messages = [
        {"id": "m1", "thread_id": "t1", "subject": "RFQ 1",
         "from": "vet@calvet.ca.gov"},
        {"id": "m2", "thread_id": "t2", "subject": "RFQ 2",
         "from": "vet@calvet.ca.gov"},
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=False, limit=200)
    assert result["ok"] is True
    with _seeded() as c:
        row = c.execute(
            "SELECT is_provisional, source FROM agency_vendor_registry "
            "WHERE dept_code='7800'"
        ).fetchone()
    assert row is not None
    assert row["source"] == "agent"
    assert row["is_provisional"] == 1  # NOT auto-promoted


def test_bulk_seed_skips_operator_owned_rows(tmp_path, monkeypatch):
    """Operator truth is sacred — agent must NOT overwrite."""
    db_path = str(tmp_path / "bs_skip.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    # Operator already marked CCHCS 'not_registered' deliberately.
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source) "
        "VALUES ('4700', 'not_registered', 'operator')"
    )
    conn.commit()

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

    messages = [
        {"id": f"m{i}", "thread_id": f"t{i}", "subject": "RFQ",
         "from": "x@cchcs.ca.gov"}
        for i in range(1, 5)
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=False, limit=200)
    assert result["rows_skipped"] == 1
    # Row is unchanged.
    with _seeded() as c:
        row = c.execute(
            "SELECT status, source FROM agency_vendor_registry "
            "WHERE dept_code='4700'"
        ).fetchone()
    assert row["source"] == "operator"
    assert row["status"] == "not_registered"


def test_bulk_seed_queues_unmapped_domains(tmp_path, monkeypatch):
    """Domain we have no alias for → seen ≥2 threads → queued for
    operator review in agency_pending_aliases."""
    db_path = str(tmp_path / "bs_pending.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.commit()

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

    messages = [
        {"id": "m1", "thread_id": "t1", "subject": "RFQ",
         "from": "x@unknown-agency.ca.gov"},
        {"id": "m2", "thread_id": "t2", "subject": "RFQ",
         "from": "x@unknown-agency.ca.gov"},
        {"id": "m3", "thread_id": "t3", "subject": "RFQ",
         "from": "x@unknown-agency.ca.gov"},
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=False, limit=200)
    assert result["pending_aliases_queued"] >= 1
    with _seeded() as c:
        row = c.execute(
            "SELECT * FROM agency_pending_aliases "
            "WHERE domain='unknown-agency.ca.gov'"
        ).fetchone()
    assert row is not None


def test_bulk_seed_skips_ambiguous_bare_ca_gov(tmp_path, monkeypatch):
    """@ca.gov bare is too generic to seed even at medium confidence."""
    db_path = str(tmp_path / "bs_ambig.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.commit()

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

    messages = [
        {"id": f"m{i}", "thread_id": f"t{i}", "subject": "RFQ",
         "from": "x@ca.gov"} for i in range(1, 5)
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=False, limit=200)
    assert result["matched_messages"] == 0  # ambiguous → skipped pre-bucket
    assert result["domains_seen"] == 0


def test_bulk_seed_dedupes_by_thread_id(tmp_path, monkeypatch):
    """3 messages all in the same thread = ONE RFQ, NOT three."""
    db_path = str(tmp_path / "bs_thread.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _seed_schema(conn)
    conn.commit()

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

    messages = [
        {"id": "m1", "thread_id": "T-SAME", "subject": "RFQ 001",
         "from": "x@cchcs.ca.gov"},
        {"id": "m2", "thread_id": "T-SAME", "subject": "Re: RFQ 001",
         "from": "x@cchcs.ca.gov"},
        {"id": "m3", "thread_id": "T-SAME", "subject": "Fwd: RFQ 001",
         "from": "x@cchcs.ca.gov"},
    ]
    fake_gmail = _stub_gmail(messages)
    monkeypatch.setattr("src.core.gmail_api", fake_gmail)

    from src.agents.registration_gap_detector import gmail_bulk_seed_registrations
    result = gmail_bulk_seed_registrations(dry_run=True, limit=200)
    # 3 messages but 1 thread → thread_count=1 → NOT auto-promoted
    proposed = result["proposed"][0]
    assert proposed["thread_count"] == 1
    assert proposed["is_provisional"] == 1


# ── confirm / reject ─────────────────────────────────────────────────────────

def test_confirm_agent_registration_graduates_source(tmp_path, monkeypatch):
    db_path = str(tmp_path / "confirm.db")
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source, "
        "is_provisional) VALUES ('4700', 'registered', 'agent', 1)"
    )
    conn.commit()

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

    from src.agents.registration_gap_detector import confirm_agent_registration
    r = confirm_agent_registration("4700", updated_by="mike")
    assert r["ok"] is True and r["graduated"] is True
    with _seeded() as c:
        row = c.execute(
            "SELECT source, is_provisional, updated_by "
            "FROM agency_vendor_registry WHERE dept_code='4700'"
        ).fetchone()
    assert row["source"] == "operator"
    assert row["is_provisional"] == 0


def test_confirm_refuses_to_overwrite_operator_row(tmp_path, monkeypatch):
    db_path = str(tmp_path / "confirm_op.db")
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source) "
        "VALUES ('4700', 'registered', 'operator')"
    )
    conn.commit()

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

    from src.agents.registration_gap_detector import confirm_agent_registration
    r = confirm_agent_registration("4700")
    assert r["ok"] is False
    assert "not agent" in r["error"]


def test_reject_agent_registration_marks_not_registered(tmp_path, monkeypatch):
    db_path = str(tmp_path / "reject.db")
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source, "
        "is_provisional) VALUES ('4700', 'registered', 'agent', 1)"
    )
    conn.commit()

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

    from src.agents.registration_gap_detector import reject_agent_registration
    r = reject_agent_registration("4700")
    assert r["ok"] is True
    with _seeded() as c:
        row = c.execute(
            "SELECT status, source, is_provisional "
            "FROM agency_vendor_registry WHERE dept_code='4700'"
        ).fetchone()
    assert row["status"] == "not_registered"
    assert row["source"] == "operator"
    assert row["is_provisional"] == 0


# ── API endpoints ────────────────────────────────────────────────────────────

def test_api_detect_returns_gap_list(auth_client, tmp_path, monkeypatch):
    db_path = str(tmp_path / "api_detect.db")
    conn = sqlite3.connect(db_path)
    _seed_schema(conn)
    conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "grand_total, start_date) VALUES (?,?,?,?,date('now','-30 days'))",
        ("PO-N", "4700", "CCHCS", 5000.0),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, line_total) VALUES (?,?,?,?,?)",
        (1, "PO-N", 0, "x", 5000.0),
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

    r = auth_client.post("/api/admin/registration-gaps/detect", json={"top_n": 5})
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert "gaps" in data


def test_api_confirm_validates_dept_code(auth_client):
    r = auth_client.post("/api/admin/registration-gaps/confirm", json={})
    assert r.status_code == 400


def test_api_reject_validates_dept_code(auth_client):
    r = auth_client.post("/api/admin/registration-gaps/reject", json={})
    assert r.status_code == 400
