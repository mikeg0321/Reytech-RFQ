"""Contract tests for scripts/backfill_sent_status.py.

Bundle-5 PR-5a — closes audit item A (sent-status hygiene). The script's
job is to flip records that were emailed out-of-band (manual Gmail, merge
scripts, historical pre-fix records) from their stuck status ("generated",
"quoted", "priced") to "sent" so the active queue stops surfacing them.

Contracts locked here:
  * Missing DB → exit 2 (same as db_bloat_diagnostic.py / backfill_wins).
  * Empty DB → exit 0, zero flips.
  * Dry-run is the default and never writes.
  * Apply mode writes status=sent + sent_at + backfilled_sent_at +
    backfill_prior_status to data_json.
  * Records already in terminal statuses (sent/won/lost/etc.) are
    untouched — backfill is never destructive.
  * Records missing a reytech_quote_number are skipped (no evidence of a
    generated package, conservative heuristic).
  * Restricting to `--only pc` or `--only rfq` leaves the other table alone.
"""
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys

import pytest


@pytest.fixture(scope="module")
def mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "backfill_sent_status" in sys.modules:
        del sys.modules["backfill_sent_status"]
    return importlib.import_module("backfill_sent_status")


def _seed_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY, created_at TEXT, requestor TEXT,
            agency TEXT, institution TEXT, items TEXT, source_file TEXT,
            quote_number TEXT, pc_number TEXT, total_items INTEGER,
            status TEXT, email_uid TEXT, email_subject TEXT,
            due_date TEXT, pc_data TEXT, ship_to TEXT, data_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY, received_at TEXT, agency TEXT,
            institution TEXT, requestor_name TEXT, requestor_email TEXT,
            rfq_number TEXT, items TEXT, status TEXT, source TEXT,
            email_uid TEXT, notes TEXT, updated_at TEXT, data_json TEXT
        );
    """)
    conn.commit()
    conn.close()


def _insert_pc(db_path, pid, status, blob):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO price_checks (id, created_at, status, pc_number, "
        "institution, data_json) VALUES (?, ?, ?, ?, ?, ?)",
        (pid, blob.get("created_at", "2026-03-01T00:00:00"), status,
         blob.get("pc_number", ""), blob.get("institution", ""),
         json.dumps(blob))
    )
    conn.commit()
    conn.close()


def _insert_rfq(db_path, rid, status, blob):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO rfqs (id, received_at, status, rfq_number, "
        "institution, data_json) VALUES (?, ?, ?, ?, ?, ?)",
        (rid, blob.get("received_at", "2026-03-01T00:00:00"), status,
         blob.get("rfq_number", ""), blob.get("institution", ""),
         json.dumps(blob))
    )
    conn.commit()
    conn.close()


def _get_pc(db_path, pid):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT status, data_json FROM price_checks WHERE id=?", (pid,)
        ).fetchone()
    finally:
        conn.close()
    return row


def _get_rfq(db_path, rid):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT status, data_json FROM rfqs WHERE id=?", (rid,)
        ).fetchone()
    finally:
        conn.close()
    return row


def test_missing_db_returns_exit_2(mod):
    rc = mod.run("/nonexistent/path/to/reytech.db", apply=False)
    assert rc == 2


def test_empty_db_zero_flips(mod, tmp_path, capsys):
    db = str(tmp_path / "empty.db")
    _seed_db(db)
    rc = mod.run(db, apply=True)
    assert rc == 0
    out = capsys.readouterr().out
    assert "flipped 0 record" in out


def test_dry_run_writes_nothing(mod, tmp_path):
    db = str(tmp_path / "dry.db")
    _seed_db(db)
    _insert_pc(db, "pc_a", "generated", {
        "reytech_quote_number": "R26Q0100",
        "reytech_quote_pdf": "/data/output/x.pdf",
        "pc_number": "12345", "institution": "CCHCS",
    })
    rc = mod.run(db, apply=False)
    assert rc == 0
    row = _get_pc(db, "pc_a")
    assert row["status"] == "generated", "dry-run must NOT modify status"


def test_apply_flips_generated_pc_with_quote_and_pdf(mod, tmp_path):
    db = str(tmp_path / "apply.db")
    _seed_db(db)
    _insert_pc(db, "pc_a", "generated", {
        "reytech_quote_number": "R26Q0100",
        "reytech_quote_pdf": "/data/output/x.pdf",
        "pc_number": "12345", "institution": "CCHCS",
        "generated_at": "2026-03-10T12:00:00",
    })
    rc = mod.run(db, apply=True)
    assert rc == 0
    row = _get_pc(db, "pc_a")
    assert row["status"] == "sent"
    blob = json.loads(row["data_json"])
    assert blob["status"] == "sent"
    assert blob["sent_at"] == "2026-03-10T12:00:00"
    assert blob["backfill_prior_status"] == "generated"
    assert blob["backfilled_sent_at"]  # stamped


def test_skip_pc_without_quote_number(mod, tmp_path):
    db = str(tmp_path / "no_qn.db")
    _seed_db(db)
    _insert_pc(db, "pc_b", "generated", {
        "pc_number": "99999", "institution": "CDCR",
        "reytech_quote_pdf": "/data/output/y.pdf",
    })
    mod.run(db, apply=True)
    row = _get_pc(db, "pc_b")
    assert row["status"] == "generated", (
        "PC with a PDF but no quote_number is ambiguous — leave it alone"
    )


def test_skip_terminal_statuses(mod, tmp_path):
    db = str(tmp_path / "terminal.db")
    _seed_db(db)
    for term in ("sent", "won", "lost", "dismissed", "archived"):
        _insert_pc(db, f"pc_{term}", term, {
            "reytech_quote_number": "R26Q0200",
            "reytech_quote_pdf": "/data/output/z.pdf",
        })
    mod.run(db, apply=True)
    for term in ("sent", "won", "lost", "dismissed", "archived"):
        assert _get_pc(db, f"pc_{term}")["status"] == term


def test_flip_priced_pc_only_if_generated_status(mod, tmp_path):
    """A 'priced' PC without a PDF is still mid-workflow — don't flip."""
    db = str(tmp_path / "priced.db")
    _seed_db(db)
    _insert_pc(db, "pc_priced_no_pdf", "priced", {
        "reytech_quote_number": "R26Q0300",
    })
    _insert_pc(db, "pc_priced_with_pdf", "priced", {
        "reytech_quote_number": "R26Q0301",
        "reytech_quote_pdf": "/data/output/q.pdf",
    })
    mod.run(db, apply=True)
    assert _get_pc(db, "pc_priced_no_pdf")["status"] == "priced"
    assert _get_pc(db, "pc_priced_with_pdf")["status"] == "sent"


def test_flip_rfq_with_quote_and_pdf(mod, tmp_path):
    db = str(tmp_path / "rfq.db")
    _seed_db(db)
    _insert_rfq(db, "rfq_a", "generated", {
        "reytech_quote_number": "R26Q0400",
        "reytech_quote_pdf": "/data/output/r.pdf",
        "rfq_number": "10840000",
    })
    mod.run(db, apply=True)
    row = _get_rfq(db, "rfq_a")
    assert row["status"] == "sent"


def test_only_pc_leaves_rfqs_alone(mod, tmp_path):
    db = str(tmp_path / "only_pc.db")
    _seed_db(db)
    _insert_pc(db, "pc_a", "generated", {
        "reytech_quote_number": "R26Q0500",
        "reytech_quote_pdf": "/data/output/a.pdf",
    })
    _insert_rfq(db, "rfq_a", "generated", {
        "reytech_quote_number": "R26Q0501",
        "reytech_quote_pdf": "/data/output/b.pdf",
    })
    mod.run(db, apply=True, only="pc")
    assert _get_pc(db, "pc_a")["status"] == "sent"
    assert _get_rfq(db, "rfq_a")["status"] == "generated"


def test_only_rejects_invalid_arg(mod, tmp_path):
    db = str(tmp_path / "bad.db")
    _seed_db(db)
    rc = mod.run(db, apply=True, only="bogus")
    assert rc == 1


def test_main_dry_run_exit_0(mod, tmp_path, monkeypatch):
    db = str(tmp_path / "main.db")
    _seed_db(db)
    rc = mod.main(["--db", db])
    assert rc == 0


def test_main_apply_flag(mod, tmp_path):
    db = str(tmp_path / "main_apply.db")
    _seed_db(db)
    _insert_pc(db, "pc_m", "generated", {
        "reytech_quote_number": "R26Q0600",
        "reytech_quote_pdf": "/data/output/m.pdf",
    })
    rc = mod.main(["--db", db, "--apply"])
    assert rc == 0
    assert _get_pc(db, "pc_m")["status"] == "sent"
