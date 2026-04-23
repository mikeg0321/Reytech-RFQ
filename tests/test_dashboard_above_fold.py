"""Bundle-3 PR-3c: dashboard queue above the fold.

Source audit items absorbed:
- **U** — Queue buried under Expiring / QA / PO Award / Parse Error / Health
  banners. Operator's first viewport should show the ranked queue first.
  The banners are self-hiding (display:none until their fetch lands), so
  putting the queue first in DOM order gives it the top-of-page position
  whenever it has content, while alerts still render above any subsequent
  content when they fire.
- **V** — "FITS / TIGHT / DOESN'T FIT" badges on queue rows have no label
  explaining the threshold. A `title=` tooltip on each variant now spells
  out the comparison (time remaining vs. LOE × 1.25 / × 2).

These tests pin the DOM order + tooltip wording so a future dashboard
tidy doesn't silently undo either.
"""
from __future__ import annotations

import pytest


class TestQueueIsAboveTheFoldInDomOrder:
    """The ranked-queue widget (`#triage-widget`) must appear BEFORE
    any of the self-hiding alert banners in HTML source order. Rendering
    is `display:none` for empty state — the DOM-order move is what makes
    it the first visible block when it has content."""

    @pytest.fixture
    def dashboard_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        return resp.get_data(as_text=True)

    def test_triage_widget_present(self, dashboard_html):
        # The relocated widget carries a data-testid so tooling can
        # target it without the internal #triage-widget id (which is
        # also used by the legacy comment-only stub).
        assert 'data-testid="dash-triage-widget"' in dashboard_html

    def test_triage_widget_before_expiring_quotes(self, dashboard_html):
        t = dashboard_html.index('data-testid="dash-triage-widget"')
        e = dashboard_html.index('id="expiry-alerts"')
        assert t < e, (
            "triage widget must sit above the Expiring Quotes banner in DOM "
            "order — otherwise the queue is pushed off the first viewport"
        )

    def test_triage_widget_before_po_review_banner(self, dashboard_html):
        t = dashboard_html.index('data-testid="dash-triage-widget"')
        po = dashboard_html.index('id="po-review-banner"')
        assert t < po

    def test_triage_widget_before_parse_error_banner(self, dashboard_html):
        t = dashboard_html.index('data-testid="dash-triage-widget"')
        pe = dashboard_html.index('id="parse-error-banner"')
        assert t < pe

    def test_triage_widget_before_health_banner(self, dashboard_html):
        t = dashboard_html.index('data-testid="dash-triage-widget"')
        h = dashboard_html.index('id="health-banner"')
        assert t < h

    def test_triage_widget_not_duplicated(self, dashboard_html):
        """The move was a cut-and-paste. Old location must not still
        render a second copy (double-rendering would hide the moved
        version and waste vertical space)."""
        n = dashboard_html.count('data-testid="dash-triage-widget"')
        assert n == 1, (
            f"expected 1 triage widget, found {n} — duplicate left by bad move"
        )
        # And the inner id must also appear exactly once.
        inner = dashboard_html.count('id="triage-widget"')
        assert inner == 1, (
            f"#triage-widget appears {inner} times — duplicate DOM block"
        )

    def test_triage_script_still_present(self, dashboard_html):
        """The <script> that fetches /api/triage and fills the widget
        by id lives at its old position. Moving the markup must not
        have dislodged the script — otherwise the widget stays empty."""
        assert "triage-next-up-title" in dashboard_html
        assert "triage-queue-rows" in dashboard_html
        # fitBadge() still present (audit V fix adds tooltips, doesn't
        # remove it)
        assert "function fitBadge(" in dashboard_html


class TestFitBadgeTooltips:
    """Audit V: each of the three FITS badge variants must carry a
    descriptive `title=` tooltip so an operator hovering the badge
    learns what the threshold is measuring."""

    @pytest.fixture
    def dashboard_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        return resp.get_data(as_text=True)

    def test_doesnt_fit_has_tooltip(self, dashboard_html):
        # The badge is rendered by JS, so we match on the function body
        # (which the client ships) rather than live output.
        assert "DOESN'T FIT" in dashboard_html or "DOESN\\'T FIT" in dashboard_html
        # Tooltip explains the 1.25× LOE threshold
        assert "1.25× the estimated LOE" in dashboard_html

    def test_tight_has_tooltip(self, dashboard_html):
        assert "TIGHT" in dashboard_html
        assert "between 1.25× and 2×" in dashboard_html

    def test_fits_has_tooltip(self, dashboard_html):
        assert "FITS" in dashboard_html
        assert "2× the estimated LOE" in dashboard_html
        assert "comfortable fit" in dashboard_html

    def test_tooltips_use_title_attribute(self, dashboard_html):
        """Belt-and-suspenders: the tooltips must be served as `title=`
        attributes (native hover), not only inside a comment or
        console.log. Hover-to-learn is the whole point."""
        # We target the specific substring that opens the FITS branch
        start = dashboard_html.index("mins < loeMin * 2")
        chunk = dashboard_html[start:start + 900]
        assert 'title="' in chunk, (
            "fitBadge(TIGHT) must render with a title attribute on its span"
        )
