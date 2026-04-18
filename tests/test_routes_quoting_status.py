"""Tests for /quoting/status dashboard routes.

Uses the existing `auth_client` fixture (Basic-Auth pre-wired) and seeds
the audit log via a real DB connection so we exercise the same query path
operators will hit in prod.
"""
from __future__ import annotations

import json

import pytest


def _seed_audit(conn, rows: list[tuple]) -> None:
    """rows: [(doc_id, doc_type, agency, stage_from, stage_to, outcome, reasons, actor, at)]"""
    for r in rows:
        conn.execute(
            """INSERT INTO quote_audit_log
               (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                outcome, reasons_json, actor, at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            r,
        )


@pytest.fixture
def seeded_audit():
    """Seed 3 quotes across advanced/blocked outcomes.

    We install migration 21's `quote_audit_log` schema directly instead of
    calling `run_migrations()` — migration 19 has a pre-existing failure on
    fresh test DBs (no such column: task_type) that aborts the loop before
    reaching 21. The point of these tests is the dashboard, not the
    migration runner.
    """
    from src.core.migrations import MIGRATIONS
    sql_21 = next(sql for v, _n, sql in MIGRATIONS if v == 21)
    from src.core.db import get_db
    with get_db() as conn:
        conn.executescript(sql_21)
        conn.execute("DELETE FROM quote_audit_log")
        _seed_audit(conn, [
            ("pc_aaa", "pc", "cchcs", "draft", "parsed", "advanced", "[]", "system", "2026-04-18T10:00:00"),
            ("pc_aaa", "pc", "cchcs", "parsed", "priced", "advanced", "[]", "system", "2026-04-18T10:01:00"),
            ("pc_aaa", "pc", "cchcs", "priced", "qa_pass", "blocked",
             json.dumps(["missing 703b"]), "system", "2026-04-18T10:02:00"),
            ("pc_bbb", "rfq", "calvet", "draft", "parsed", "advanced", "[]", "system", "2026-04-18T11:00:00"),
            ("pc_ccc", "pc", "dsh", "draft", "parsed", "error",
             json.dumps(["parser crashed"]), "system", "2026-04-18T12:00:00"),
        ])
    yield
    # clean up so this fixture doesn't pollute other tests
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM quote_audit_log")
    except Exception:
        pass


class TestApiStatusSummary:
    def test_api_lists_latest_row_per_quote(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        # Three distinct quotes → three summary rows.
        doc_ids = {r["doc_id"] for r in body["rows"]}
        assert doc_ids == {"pc_aaa", "pc_bbb", "pc_ccc"}
        # For pc_aaa, the latest outcome is "blocked" (qa_pass attempt).
        pc_aaa = next(r for r in body["rows"] if r["doc_id"] == "pc_aaa")
        assert pc_aaa["outcome"] == "blocked"
        assert pc_aaa["stage_to"] == "qa_pass"
        assert "missing 703b" in pc_aaa["reasons"]

    def test_api_reports_blocked_now(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status")
        body = resp.get_json()
        blocked_ids = {r["doc_id"] for r in body["blocked_now"]}
        assert blocked_ids == {"pc_aaa", "pc_ccc"}


class TestApiStatusDetail:
    def test_trail_is_chronological(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/pc_aaa")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert [t["stage_to"] for t in body["trail"]] == ["parsed", "priced", "qa_pass"]
        assert body["latest_outcome"] == "blocked"

    def test_unknown_doc_id_404s(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/pc_does_not_exist")
        assert resp.status_code == 404


class TestOverride:
    def test_override_requires_reason(self, auth_client, seeded_audit):
        resp = auth_client.post("/api/quoting/override/pc_aaa", json={})
        assert resp.status_code == 400

    def test_override_writes_new_audit_row(self, auth_client, seeded_audit):
        resp = auth_client.post(
            "/api/quoting/override/pc_aaa",
            json={"reason": "buyer confirmed 703b not required for this solicitation",
                  "actor": "mike"},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)

        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT outcome, actor, reasons_json FROM quote_audit_log "
                "WHERE quote_doc_id='pc_aaa' ORDER BY id DESC LIMIT 1"
            ).fetchall()
        assert rows, "expected a new audit row"
        assert rows[0][0] == "override"
        assert rows[0][1] == "mike"
        assert "buyer confirmed" in rows[0][2]

    def test_override_404s_for_unknown_doc(self, auth_client, seeded_audit):
        resp = auth_client.post(
            "/api/quoting/override/pc_does_not_exist",
            json={"reason": "irrelevant"},
        )
        assert resp.status_code == 404


class TestBackfill:
    """POST /api/quoting/backfill — drive existing PCs through orchestrator."""

    def test_backfill_requires_auth(self, anon_client):
        resp = anon_client.post("/api/quoting/backfill", json={})
        assert resp.status_code in (401, 403)

    def test_backfill_mode_ids_requires_ids(self, auth_client):
        resp = auth_client.post("/api/quoting/backfill", json={"mode": "ids"})
        assert resp.status_code == 400
        assert "ids list required" in resp.get_json()["error"]

    def test_backfill_with_no_pcs_returns_empty(self, auth_client, seeded_audit):
        # seeded_audit installs the audit log schema; no PCs seeded here.
        resp = auth_client.post("/api/quoting/backfill", json={"limit": 5})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["processed"] == 0
        assert body["results"] == []

    def test_backfill_runs_orchestrator_and_writes_audit(
        self, auth_client, seeded_audit, seed_db_price_check
    ):
        # seeded_audit already has rows for pc_aaa/pc_bbb/pc_ccc — so new PCs
        # with different IDs will not be skipped by skip_filled.
        seed_db_price_check(
            "pc_backfill_1",
            agency="CDCR",
            items=[{"description": "Gauze 4x4", "qty": 10, "uom": "BX"}],
        )

        resp = auth_client.post(
            "/api/quoting/backfill",
            json={"mode": "all", "limit": 10, "target_stage": "priced"},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["ok"] is True
        assert body["target_stage"] == "priced"
        # The seeded PC should have been processed.
        processed_ids = {r["pc_id"] for r in body["results"]}
        assert "pc_backfill_1" in processed_ids


class TestHtmlPages:
    def test_status_page_renders(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        assert "pc_aaa" in html
        assert "Quoting Status" in html

    def test_detail_page_renders(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status/pc_aaa")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        assert "Stage Timeline" in html
        # Override UI only shows when latest outcome is blocked/error
        assert "Operator Override" in html

    def test_detail_page_hides_override_when_advanced(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status/pc_bbb")
        html = resp.get_data(as_text=True)
        assert "Operator Override" not in html
