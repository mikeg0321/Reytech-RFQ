"""Tests for the email_thread_id backfill — refactored run() + admin endpoint.

The script's run() now returns a structured dict instead of an int, so the
admin endpoint can wrap it without re-implementing parsing. The CLI path
(`python scripts/backfill_email_thread_id.py`) still works because main()
calls _print_report() on the result.

PR-B1-followup (admin-backfill-thread-ids).
"""
from __future__ import annotations

import json
import os
import sqlite3
from unittest.mock import patch

import pytest

from scripts.backfill_email_thread_id import run, _print_report


def _make_db(tmp_path, *, rfqs=(), pcs=()):
    """Build a minimal SQLite DB with the columns the backfill expects."""
    db = tmp_path / "reytech.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE rfqs (
            id TEXT PRIMARY KEY,
            data_json TEXT,
            email_thread_id TEXT DEFAULT '',
            updated_at TEXT
        );
        CREATE TABLE price_checks (
            id TEXT PRIMARY KEY,
            data_json TEXT,
            email_thread_id TEXT DEFAULT '',
            updated_at TEXT
        );
    """)
    for rid, blob in rfqs:
        conn.execute("INSERT INTO rfqs (id, data_json) VALUES (?, ?)",
                     (rid, json.dumps(blob)))
    for pid, blob in pcs:
        conn.execute("INSERT INTO price_checks (id, data_json) VALUES (?, ?)",
                     (pid, json.dumps(blob)))
    conn.commit()
    conn.close()
    return str(db)


class TestRunStructure:
    """run() now returns a dict — validate the contract."""

    def test_returns_error_when_db_missing(self, tmp_path):
        result = run(db_path=str(tmp_path / "no.db"), apply=False)
        assert result["ok"] is False
        assert "DB not found" in result["error"]
        assert result["records"] == []

    def test_returns_error_when_only_invalid(self, tmp_path):
        db = _make_db(tmp_path)
        result = run(db_path=db, only="weather")
        assert result["ok"] is False
        assert "only must be" in result["error"]

    def test_returns_error_when_gmail_unconfigured(self, tmp_path):
        db = _make_db(tmp_path)
        with patch("src.core.gmail_api.is_configured", return_value=False):
            result = run(db_path=db)
        assert result["ok"] is False
        assert "Gmail not configured" in result["error"]

    def test_dry_run_reports_findings_without_writing(self, tmp_path):
        db = _make_db(tmp_path, rfqs=[
            ("rfq_1", {"email_uid": "g_abc", "email_thread_id": ""}),
        ])
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=object()), \
             patch("scripts.backfill_email_thread_id._fetch_thread_id",
                   return_value="thr_xyz"):
            result = run(db_path=db, apply=False)
        assert result["ok"] is True
        assert result["mode"] == "dry-run"
        assert result["flipped"] == 1
        assert result["not_found"] == 0
        assert result["records"][0]["thread_id"] == "thr_xyz"
        assert result["records"][0]["applied"] is False
        # On disk, nothing changed
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT email_thread_id FROM rfqs").fetchone()
        conn.close()
        assert row[0] == ""

    def test_apply_writes_thread_id(self, tmp_path):
        db = _make_db(tmp_path, rfqs=[
            ("rfq_1", {"email_uid": "g_abc"}),
            ("rfq_2", {"email_uid": "g_def"}),
        ])
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=object()), \
             patch("scripts.backfill_email_thread_id._fetch_thread_id",
                   side_effect=["thr_1", "thr_2"]):
            result = run(db_path=db, apply=True)
        assert result["ok"] is True
        assert result["mode"] == "apply"
        assert result["flipped"] == 2
        assert all(r["applied"] for r in result["records"])
        # On disk, both rows updated
        conn = sqlite3.connect(db)
        rows = dict(conn.execute(
            "SELECT id, email_thread_id FROM rfqs").fetchall())
        conn.close()
        assert rows["rfq_1"] == "thr_1"
        assert rows["rfq_2"] == "thr_2"

    def test_caps_at_max(self, tmp_path):
        db = _make_db(tmp_path, rfqs=[
            (f"rfq_{i}", {"email_uid": f"g_{i}"}) for i in range(5)
        ])
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=object()), \
             patch("scripts.backfill_email_thread_id._fetch_thread_id",
                   return_value="thr"):
            result = run(db_path=db, apply=False, max_records=2)
        assert result["total_found"] == 5
        assert result["capped_at"] == 2
        assert len(result["records"]) == 2

    def test_skips_already_bound(self, tmp_path):
        # Records with email_thread_id already set must NOT appear
        db = _make_db(tmp_path, rfqs=[
            ("already_bound", {"email_uid": "g_1", "email_thread_id": "thr_pre"}),
            ("needs_backfill", {"email_uid": "g_2", "email_thread_id": ""}),
        ])
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=object()), \
             patch("scripts.backfill_email_thread_id._fetch_thread_id",
                   return_value="thr_new"):
            result = run(db_path=db)
        ids = [r["id"] for r in result["records"]]
        assert "already_bound" not in ids
        assert "needs_backfill" in ids


class TestPrintReport:
    """_print_report turns a result dict back into the CLI text format."""

    def test_returns_2_for_db_missing(self, capsys):
        rc = _print_report({"ok": False, "error": "DB not found: /x"})
        assert rc == 2

    def test_returns_1_for_other_error(self, capsys):
        rc = _print_report({"ok": False, "error": "Gmail not configured"})
        assert rc == 1

    def test_returns_0_on_success(self, capsys):
        rc = _print_report({
            "ok": True, "mode": "dry-run", "db_path": "/x.db",
            "total_found": 1, "flipped": 1, "not_found": 0,
            "capped_at": None,
            "records": [{"kind": "rfq", "id": "rfq_1", "source": "gmail_id",
                         "value": "g_x", "thread_id": "thr_1", "applied": False}],
        })
        out = capsys.readouterr().out
        assert rc == 0
        assert "RFQ" in out
        assert "thr_1" in out


class TestAdminEndpoint:
    """Admin route: POST /api/admin/backfill-email-thread-ids."""

    def test_dry_run_default(self, auth_client):
        with patch("scripts.backfill_email_thread_id.run") as mock_run:
            mock_run.return_value = {
                "ok": True, "mode": "dry-run", "db_path": "/data/reytech.db",
                "total_found": 3, "flipped": 2, "not_found": 1,
                "capped_at": None, "records": [],
            }
            r = auth_client.post(
                "/api/admin/backfill-email-thread-ids",
                data=json.dumps({}),
                content_type="application/json",
            )
        assert r.status_code == 200
        body = r.get_json()
        assert body["mode"] == "dry-run"
        assert body["flipped"] == 2
        # Default max=200, apply=False
        kwargs = mock_run.call_args.kwargs
        assert kwargs["apply"] is False
        assert kwargs["max_records"] == 200

    def test_apply_passed_through(self, auth_client):
        with patch("scripts.backfill_email_thread_id.run") as mock_run:
            mock_run.return_value = {"ok": True, "mode": "apply", "records": [],
                                     "total_found": 0, "flipped": 0,
                                     "not_found": 0, "capped_at": None,
                                     "db_path": "/data/reytech.db"}
            r = auth_client.post(
                "/api/admin/backfill-email-thread-ids",
                data=json.dumps({"apply": True, "only": "rfq", "max": 50}),
                content_type="application/json",
            )
        assert r.status_code == 200
        kwargs = mock_run.call_args.kwargs
        assert kwargs["apply"] is True
        assert kwargs["only"] == "rfq"
        assert kwargs["max_records"] == 50

    def test_rejects_invalid_only(self, auth_client):
        r = auth_client.post(
            "/api/admin/backfill-email-thread-ids",
            data=json.dumps({"only": "weather"}),
            content_type="application/json",
        )
        assert r.status_code == 400

    def test_rejects_max_out_of_range(self, auth_client):
        r = auth_client.post(
            "/api/admin/backfill-email-thread-ids",
            data=json.dumps({"max": 0}),
            content_type="application/json",
        )
        assert r.status_code == 400
        r = auth_client.post(
            "/api/admin/backfill-email-thread-ids",
            data=json.dumps({"max": 99999}),
            content_type="application/json",
        )
        assert r.status_code == 400

    def test_returns_500_when_run_fails(self, auth_client):
        with patch("scripts.backfill_email_thread_id.run") as mock_run:
            mock_run.return_value = {
                "ok": False, "error": "Gmail not configured",
                "records": [],
            }
            r = auth_client.post(
                "/api/admin/backfill-email-thread-ids",
                data=json.dumps({}),
                content_type="application/json",
            )
        assert r.status_code == 500
        assert "Gmail not configured" in r.get_json()["error"]
