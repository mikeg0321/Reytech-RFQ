"""Tests for /quotes PDF preview modal + audit-trail export endpoints.

C2 batch: confirm the modal scaffolding and event-delegation script are
present in the rendered template, and that the trail-export CSV endpoints
return well-formed responses.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
QUOTES_TPL = ROOT / "src" / "templates" / "quotes.html"
INTEL_ROUTES = ROOT / "src" / "api" / "modules" / "routes_intel.py"
QSTATUS_ROUTES = ROOT / "src" / "api" / "modules" / "routes_quoting_status.py"


# ── Static template / route assertions ─────────────────────────────────────

class TestPdfPreviewModalMarkup:
    def setup_method(self):
        self.html = QUOTES_TPL.read_text(encoding="utf-8")

    def test_modal_backdrop_present(self):
        assert 'id="pdf-preview-backdrop"' in self.html
        assert 'role="dialog"' in self.html
        assert 'aria-modal="true"' in self.html

    def test_modal_iframe_starts_blank(self):
        # Don't preload a real PDF in the iframe — wastes bandwidth & may auth-fail.
        assert 'id="pdf-preview-iframe"' in self.html
        assert 'src="about:blank"' in self.html

    def test_modal_has_close_and_open_in_tab(self):
        assert 'id="pdf-preview-close"' in self.html
        assert 'id="pdf-preview-open"' in self.html
        assert 'target="_blank"' in self.html  # open-in-tab safety
        assert 'rel="noopener"' in self.html

    def test_modal_js_uses_event_delegation(self):
        # Delegation matters because rows_html is server-rendered into innerHTML
        # via {{ rows_html|safe }} — direct addEventListener on rows would miss them.
        assert "document.addEventListener('click'" in self.html
        assert "closest('.quote-preview-btn')" in self.html

    def test_modal_closes_on_escape(self):
        assert "e.key === 'Escape'" in self.html


class TestPreviewButtonRendering:
    """The row-renderer in routes_intel must emit .quote-preview-btn alongside download."""

    def setup_method(self):
        self.src = INTEL_ROUTES.read_text(encoding="utf-8")

    def test_renderer_emits_preview_button(self):
        assert 'class="quote-preview-btn"' in self.src

    def test_renderer_uses_view_pdf_endpoint(self):
        # view-pdf serves inline (Content-Disposition: inline); download forces attachment.
        assert "/api/pricecheck/view-pdf/" in self.src

    def test_renderer_keeps_download_link(self):
        # Preview adds, doesn't replace, the download.
        assert "/api/pricecheck/download/" in self.src


# ── Audit trail export endpoints (already shipped — guard against regression) ─

class TestAuditTrailExportEndpoints:
    def setup_method(self):
        self.src = QSTATUS_ROUTES.read_text(encoding="utf-8")

    def test_summary_csv_endpoint_registered(self):
        assert '@bp.route("/api/quoting/status/export.csv")' in self.src

    def test_per_doc_trail_csv_endpoint_registered(self):
        assert '@bp.route("/api/quoting/status/<doc_id>/export.csv")' in self.src

    def test_summary_csv_supports_outcome_filter(self):
        # ?outcome=blocked,error must filter rows before CSV emit.
        assert '_parse_outcome_filter' in self.src
        assert "request.args.get(\"outcome\"" in self.src

    def test_csv_has_attachment_disposition(self):
        # Browsers must download the CSV, not render it inline as text/csv preview.
        assert 'attachment; filename=' in self.src


# ── Live render test through Flask client (catches Jinja errors early) ─────

@pytest.mark.usefixtures("auth_client")
class TestQuotesPageRendersWithModal:
    def test_quotes_page_includes_modal_scaffold(self, auth_client):
        resp = auth_client.get("/quotes")
        # Even with no quotes, the modal scaffold must render — it's a page-level
        # element, not row-level.
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert 'id="pdf-preview-backdrop"' in body
        assert 'id="pdf-preview-iframe"' in body
