"""Regression guards for the OB-2/OB-11/OB-13/OB-16/OB-18 cleanup sweep.

Audit 2026-04-21 found:
  OB-2:  `/outbox` swallowed every load error into an empty render -> operator
         saw "no drafts" even when cs_agent.get_cs_drafts() crashed on schema.
  OB-11: empty-`to` drafts surfaced in the queue and blew up at SMTP on Approve.
  OB-13: `approveCS` JS ignored the `warning` field from the OB-1 invariant
         (ok=true + warning = SMTP sent but DB persist failed).
  OB-16: `routes_rfq_admin` hardcoded "Michael Guadan - Reytech Inc." instead
         of sourcing from the canonical identity constants.
  OB-18: `email_outreach.approve_email` never logged the status transition,
         so duplicate-send incidents had no audit trail.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


class TestOB2PageOutboxSurfacesLoadErrors:
    """OB-2: bare-except on /outbox is gone; errors must be logged + surfaced."""

    def test_page_outbox_has_no_bare_except(self):
        src = (ROOT / "src" / "api" / "modules" / "routes_crm.py").read_text(encoding="utf-8")
        # Scope to the page_outbox function body
        m = re.search(r"def page_outbox\([^)]*\).*?(?=\ndef |\nclass )", src, re.DOTALL)
        assert m, "page_outbox not found in routes_crm.py"
        body = m.group(0)
        # The fix replaced the single bare-except with per-loader try/except
        # that each log.error or log.debug. There should be AT LEAST one log call
        # and the `return []` empty-list pattern on bare except must be gone.
        assert "log.error(" in body or "log.warning(" in body, (
            "OB-2 REGRESSION: page_outbox no longer logs load failures."
        )
        assert "load_error" in body, (
            "OB-2 REGRESSION: page_outbox no longer threads `load_error` to the template."
        )

    def test_outbox_template_renders_load_error_banner(self):
        tpl = (ROOT / "src" / "templates" / "outbox.html").read_text(encoding="utf-8")
        assert "{% if load_error %}" in tpl, (
            "OB-2 REGRESSION: outbox.html no longer renders the load_error banner."
        )
        assert "Outbox partially loaded" in tpl, (
            "OB-2 REGRESSION: load_error banner copy was changed or removed."
        )


class TestOB11EmptyToDraftsFiltered:
    """OB-11: empty-To drafts must be dropped from the UI + logged."""

    def test_page_outbox_filters_empty_to(self):
        src = (ROOT / "src" / "api" / "modules" / "routes_crm.py").read_text(encoding="utf-8")
        m = re.search(r"def page_outbox\([^)]*\).*?(?=\ndef |\nclass )", src, re.DOTALL)
        assert m
        body = m.group(0)
        # Filter comprehension should reference `.get("to")` on each draft.
        assert re.search(r"d\.get\(['\"]to['\"]\)", body), (
            "OB-11 REGRESSION: page_outbox no longer filters by draft['to']."
        )
        assert "dropped" in body and "empty 'to'" in body, (
            "OB-11 REGRESSION: page_outbox no longer logs upstream empty-to bugs."
        )


class TestOB13ApproveCSSurfacesWarning:
    """OB-13: approveCS JS must display the OB-1 invariant `warning` field."""

    def test_outbox_js_reads_warning(self):
        tpl = (ROOT / "src" / "templates" / "outbox.html").read_text(encoding="utf-8")
        # Locate the approveCS function body
        m = re.search(r"function approveCS\(.*?^\}", tpl, re.DOTALL | re.MULTILINE)
        assert m, "approveCS() JS function not found in outbox.html"
        fn = m.group(0)
        assert "d.warning" in fn, (
            "OB-13 REGRESSION: approveCS ignores the `warning` field again. "
            "Operators won't see when SMTP succeeded but DB persist failed."
        )
        assert "duplicate" in fn.lower(), (
            "OB-13 REGRESSION: warning alert no longer warns about duplicate-send risk."
        )


class TestOB16FromNameCanonical:
    """OB-16: from_name default must come from canonical identity constants."""

    def test_routes_rfq_admin_uses_canonical_name(self):
        src = (ROOT / "src" / "api" / "modules" / "routes_rfq_admin.py").read_text(encoding="utf-8")
        # The hardcoded string "Michael Guadan - Reytech Inc." must be gone as a
        # literal default. It may still appear in a comment or docstring.
        hits = [
            line for line in src.splitlines()
            if '"Michael Guadan - Reytech Inc."' in line
            and not line.lstrip().startswith("#")
        ]
        assert not hits, (
            "OB-16 REGRESSION: `Michael Guadan - Reytech Inc.` is hardcoded again "
            f"in routes_rfq_admin.py. Offending lines:\n" + "\n".join(hits)
        )
        # Positive: the new pattern must import the canonical constants.
        assert "from src.core.email_signature import NAME" in src, (
            "OB-16 REGRESSION: canonical NAME/COMPANY import is gone."
        )


class TestOB18ApproveEmailLogsTransition:
    """OB-18: approve_email must log the status transition for audit."""

    def test_approve_email_logs_transition(self, caplog, monkeypatch):
        # Bypass the DAL + JSON plumbing and drive approve_email with a
        # stubbed in-memory outbox. We only care about the log line.
        from src.agents import email_outreach

        draft = {
            "id": "test_draft_1",
            "to": "buyer@example.com",
            "subject": "Test",
            "body": "Test body",
            "status": "draft",
            "created_at": "2026-04-21T10:00:00",
        }
        store = [dict(draft)]
        monkeypatch.setattr(email_outreach, "_load_outbox", lambda: store)
        monkeypatch.setattr(email_outreach, "_save_outbox", lambda _: None)

        with caplog.at_level(logging.INFO, logger="outreach"):
            result = email_outreach.approve_email("test_draft_1")

        assert result["ok"] is True, f"approve_email failed: {result}"
        matching = [
            rec for rec in caplog.records
            if "approve_email" in rec.getMessage()
            and "draft -> approved" in rec.getMessage()
        ]
        assert matching, (
            "OB-18 REGRESSION: approve_email no longer logs the draft->approved "
            f"status transition. All log records: {[r.getMessage() for r in caplog.records]}"
        )
