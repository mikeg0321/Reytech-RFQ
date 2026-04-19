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
        # Action UI (override + retry modal) only shows when latest outcome is blocked/error
        assert "Override or Retry" in html
        assert "action-modal-backdrop" in html

    def test_detail_page_hides_action_when_advanced(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status/pc_bbb")
        html = resp.get_data(as_text=True)
        assert "Override or Retry" not in html
        assert "action-modal-backdrop" not in html


class TestRetry:
    """POST /api/quoting/retry/<doc_id> — re-run orchestrator on a single doc."""

    def test_retry_requires_auth(self, anon_client):
        resp = anon_client.post("/api/quoting/retry/pc_aaa", json={"reason": "x"})
        assert resp.status_code in (401, 403)

    def test_retry_rejects_invalid_target_stage(self, auth_client, seeded_audit):
        resp = auth_client.post(
            "/api/quoting/retry/pc_aaa",
            json={"reason": "fixed", "target_stage": "sent"},  # not in allowed set
        )
        assert resp.status_code == 400
        assert "target_stage" in resp.get_json()["error"]

    def test_retry_404s_for_unknown_doc(self, auth_client, seeded_audit):
        resp = auth_client.post(
            "/api/quoting/retry/pc_does_not_exist",
            json={"reason": "fixed", "target_stage": "priced"},
        )
        assert resp.status_code == 404

    def test_retry_runs_orchestrator_for_seeded_pc(
        self, auth_client, seeded_audit, seed_db_price_check
    ):
        seed_db_price_check(
            "pc_retry_1",
            agency="CDCR",
            items=[{"description": "Bandage roll 2in", "qty": 5, "uom": "BX"}],
        )
        resp = auth_client.post(
            "/api/quoting/retry/pc_retry_1",
            json={"reason": "[price_error] re-priced after catalog refresh",
                  "target_stage": "priced"},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["doc_id"] == "pc_retry_1"
        assert body["target_stage"] == "priced"
        # Orchestrator may or may not advance depending on seed quality; what we
        # care about is that it ran without crashing and recorded the reason note.
        from src.core.db import get_db
        with get_db() as conn:
            override_rows = conn.execute(
                "SELECT reasons_json FROM quote_audit_log "
                "WHERE quote_doc_id='pc_retry_1' AND outcome='override' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchall()
        assert override_rows, "expected retry-note row recorded as override outcome"
        assert "retry-note" in override_rows[0][0]
        assert "price_error" in override_rows[0][0]


class TestSummaryCsvExport:
    """GET /api/quoting/status/export.csv — operator dumps the dashboard for triage.

    The point: an operator looking at a wall of blocked rows wants to grab the
    list, paste it into a notebook, and work through it offline. CSV is the
    universal "I need this in a spreadsheet" format. Anything fancier (xlsx,
    JSON-to-pivot) is yak-shaving for a feature operators will use once a week.
    """

    def test_export_returns_csv_mime_and_disposition(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/export.csv")
        assert resp.status_code == 200
        assert resp.mimetype == "text/csv"
        assert "attachment" in resp.headers.get("Content-Disposition", "")
        assert "quoting_status.csv" in resp.headers.get("Content-Disposition", "")

    def test_export_header_and_one_row_per_doc(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/export.csv")
        text = resp.get_data(as_text=True)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        # Header + 3 distinct doc_ids (pc_aaa latest, pc_bbb, pc_ccc).
        assert lines[0].startswith("doc_id,doc_type,agency_key,stage_from,stage_to,outcome,reasons,actor,at")
        assert len(lines) == 1 + 3

    def test_export_outcome_filter_subsets_rows(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/export.csv?outcome=blocked,error")
        text = resp.get_data(as_text=True)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        # Header + pc_aaa (blocked) + pc_ccc (error). pc_bbb (advanced) excluded.
        assert len(lines) == 1 + 2
        assert "pc_aaa" in text
        assert "pc_ccc" in text
        assert "pc_bbb" not in text

    def test_export_unknown_outcome_returns_only_header(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/export.csv?outcome=nonexistent")
        text = resp.get_data(as_text=True)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        assert len(lines) == 1  # header only

    def test_export_reasons_pipe_joined(self, auth_client, seeded_audit):
        # pc_aaa's blocked row carries reason "missing 703b" — must appear verbatim.
        resp = auth_client.get("/api/quoting/status/export.csv?outcome=blocked")
        text = resp.get_data(as_text=True)
        assert "missing 703b" in text


class TestTrailCsvExport:
    """GET /api/quoting/status/<doc_id>/export.csv — full chronological trail."""

    def test_trail_csv_returns_full_chronological_trail(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/pc_aaa/export.csv")
        assert resp.status_code == 200
        assert resp.mimetype == "text/csv"
        text = resp.get_data(as_text=True)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        # Header + 3 transitions for pc_aaa.
        assert lines[0].startswith("stage_from,stage_to,outcome,reasons,actor,at")
        assert len(lines) == 1 + 3
        # Chronology preserved: parsed → priced → qa_pass.
        assert "draft,parsed,advanced" in text
        assert "parsed,priced,advanced" in text
        assert "priced,qa_pass,blocked" in text

    def test_trail_csv_filename_uses_safe_doc_id(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/pc_aaa/export.csv")
        cd = resp.headers.get("Content-Disposition", "")
        assert "quoting_trail_pc_aaa.csv" in cd

    def test_trail_csv_404s_for_unknown_doc(self, auth_client, seeded_audit):
        resp = auth_client.get("/api/quoting/status/pc_does_not_exist/export.csv")
        assert resp.status_code == 404


class TestFilterChipsRendered:
    """The chip UI is JS-driven; the test verifies SSR includes the buttons +
    the export link, so an operator with JS disabled still sees affordances and
    a deep-link to the CSV. This guards against the chips being accidentally
    removed in a future template refactor."""

    def test_filter_chips_present_in_rendered_html(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status")
        html = resp.get_data(as_text=True)
        # Each chip carries data-filter="<key>" — assert by stable hook, not label.
        for key in ["all", "advanced", "blocked", "error", "skipped", "override"]:
            assert f'data-filter="{key}"' in html, f"chip {key!r} missing"

    def test_export_csv_link_present(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status")
        html = resp.get_data(as_text=True)
        assert 'href="/api/quoting/status/export.csv"' in html

    def test_detail_page_has_export_trail_link(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status/pc_aaa")
        html = resp.get_data(as_text=True)
        assert 'href="/api/quoting/status/pc_aaa/export.csv"' in html
        assert "Export trail CSV" in html

    def test_detail_page_without_trail_omits_export_link(self, auth_client, seeded_audit):
        resp = auth_client.get("/quoting/status/pc_unknown")
        html = resp.get_data(as_text=True)
        # No trail → no export affordance (avoids generating an empty CSV that
        # the operator then has to wonder about).
        assert "Export trail CSV" not in html
