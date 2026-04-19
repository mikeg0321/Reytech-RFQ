"""Tests for the operator approval queue (D1 batch).

Approval queue surfaces docs that finished orchestration cleanly and are now
sitting at qa_pass or priced — i.e., waiting for a human decision.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
APPROVAL_TPL = ROOT / "src" / "templates" / "approval_queue.html"
QSTATUS_ROUTES = ROOT / "src" / "api" / "modules" / "routes_quoting_status.py"
BASE_TPL = ROOT / "src" / "templates" / "base.html"


# ── Static surface area ────────────────────────────────────────────────────

class TestRouteRegistration:
    def setup_method(self):
        self.src = QSTATUS_ROUTES.read_text(encoding="utf-8")

    def test_html_route_registered(self):
        assert '@bp.route("/quoting/approval-queue")' in self.src

    def test_json_api_registered(self):
        assert '@bp.route("/api/quoting/approval-queue")' in self.src

    def test_only_qa_pass_and_priced_qualify(self):
        # The queue must exclude docs still in early stages or in error/blocked.
        # If someone widens this set without thinking, they'll flood the queue.
        assert '_APPROVAL_STAGES = {"qa_pass", "priced"}' in self.src

    def test_filter_requires_advanced_outcome(self):
        # Only "advanced" outcomes — blocked/error stay on /quoting/status.
        assert '"advanced"' in self.src


class TestTemplateMarkup:
    def setup_method(self):
        self.html = APPROVAL_TPL.read_text(encoding="utf-8")

    def test_has_kpi_tiles(self):
        assert "Ready to Generate" in self.html
        assert "Ready for QA" in self.html

    def test_links_back_to_quoting_status(self):
        # Every queue row links to the per-doc audit trail for context.
        assert "/quoting/status/" in self.html

    def test_open_action_routes_by_doc_type(self):
        # PCs go to /pricecheck/<id>, RFQs to /rfq/<id>.
        assert "'/pricecheck/' ~ r.doc_id" in self.html
        assert "'/rfq/' ~ r.doc_id" in self.html

    def test_empty_state_present(self):
        # Don't show a bare table when nothing's pending — operator wants reassurance.
        assert "Queue clear" in self.html


class TestPaletteEntry:
    def test_palette_includes_approval_queue(self):
        html = BASE_TPL.read_text(encoding="utf-8")
        assert "/quoting/approval-queue" in html
        assert "Approval Queue" in html


# ── Live behavior through Flask client ─────────────────────────────────────

@pytest.mark.usefixtures("auth_client")
class TestApprovalQueueLive:
    def test_page_renders_empty(self, auth_client):
        # No quotes seeded → queue clear path.
        resp = auth_client.get("/quoting/approval-queue")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Approval Queue" in body
        assert "Queue clear" in body

    def test_json_endpoint_returns_ok(self, auth_client):
        resp = auth_client.get("/api/quoting/approval-queue")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "rows" in data
        assert "by_stage" in data

    def test_queue_picks_up_qa_pass_advanced_row(self, auth_client):
        """Seed a qa_pass+advanced audit row and confirm it surfaces."""
        from src.core.db import get_db

        doc_id = "test_approval_queue_seed"
        at = datetime.now(timezone.utc).isoformat()
        with get_db() as conn:
            conn.execute(
                """INSERT INTO quote_audit_log
                   (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                    outcome, reasons_json, actor, at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (doc_id, "pc", "CCHCS", "priced", "qa_pass",
                 "advanced", json.dumps([]), "test", at),
            )

        resp = auth_client.get("/api/quoting/approval-queue")
        data = resp.get_json()
        ids = [r["doc_id"] for r in data["rows"]]
        assert doc_id in ids
        assert data["by_stage"].get("qa_pass", 0) >= 1

    def test_blocked_row_does_not_appear(self, auth_client):
        """Blocked rows belong on /quoting/status, NOT in the approval queue."""
        from src.core.db import get_db

        doc_id = "test_approval_blocked_seed"
        at = datetime.now(timezone.utc).isoformat()
        with get_db() as conn:
            conn.execute(
                """INSERT INTO quote_audit_log
                   (quote_doc_id, doc_type, agency_key, stage_from, stage_to,
                    outcome, reasons_json, actor, at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (doc_id, "pc", "CCHCS", "priced", "qa_pass",
                 "blocked", json.dumps(["missing form"]), "test", at),
            )

        resp = auth_client.get("/api/quoting/approval-queue")
        data = resp.get_json()
        ids = [r["doc_id"] for r in data["rows"]]
        assert doc_id not in ids
