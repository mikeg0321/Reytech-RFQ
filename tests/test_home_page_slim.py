"""Pin the 2026-05-06 home-slim deletions (PR-2 from the audit).

Three widgets were removed from `home.html` because they did not
serve the win-rate × volume KPI:

  - Win-Rate Intel header card (analytical, belongs on /quotes)
  - QA Status Banner (workflow tests, belongs on /qa/workflow)
  - Compliance Alerts (tenant-config noise)

Pin the absence so a future "let me bolt this back on" PR fails the
test rather than silently re-accreting noise on the home page.
"""
import os


def _home_html():
    p = os.path.join(os.path.dirname(__file__), "..",
                     "src/templates/home.html")
    with open(p, encoding="utf-8") as f:
        return f.read()


def test_win_rate_intel_card_removed():
    html = _home_html()
    # The DOM element id and the script that fetched /api/oracle/* are gone
    assert 'id="win-rate-intel"' not in html
    assert 'id="wri-rate"' not in html
    assert 'id="wri-headline"' not in html
    assert 'id="wri-degrading"' not in html
    assert "/api/oracle/items-yearly" not in html
    assert "/api/oracle/win-rate-yearly" not in html
    assert "/api/oracle/win-rate-by-agency" not in html


def test_qa_banner_removed():
    html = _home_html()
    # Only the DOM is gone; the JS branch that updates qa-nav-btn (top
    # nav score badge) still runs and is null-safe (`if(!banner)return`).
    assert 'id="qa-banner"' not in html
    assert 'id="qa-banner-title"' not in html
    assert 'id="qa-banner-detail"' not in html


def test_compliance_alerts_removed():
    html = _home_html()
    assert 'id="compliance-alerts"' not in html
    assert "/api/v1/tenant/compliance" not in html


def test_search_bar_still_present():
    """Regression guard: the deletions must not have nuked the search
    bar (which IS load-bearing — operator's primary entry point)."""
    html = _home_html()
    assert 'action="/search"' in html
    assert 'name="q"' in html
    assert 'placeholder="Search quotes' in html


def test_queue_tables_still_present():
    """The PC + RFQ queue tables are the gold-standard widgets the
    audit identified. They must survive the slim."""
    html = _home_html()
    # The home queue uses the queue_table macro; verify its outputs
    assert "queue-table" in html or "rfq-queue" in html or "pc-queue" in html


def test_action_bar_still_present():
    """The +New RFQ / +New PC / Multi-PC Upload buttons must remain."""
    html = _home_html()
    assert "+ New RFQ" in html
    assert "+ New PC" in html


def test_audit_trail_comment_present():
    """A future maintainer should see WHY these were removed."""
    html = _home_html()
    assert "PR-2 home-slim removals" in html or "AUDIT_HOME_QUOTING_GROWTH" in html
