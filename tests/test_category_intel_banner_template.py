"""Phase 4.6.3: lock in the category-intel banner JS in base.html.

Pure string-presence tests — the banner JS is inline in the
window.openItemHistory() function. We can't run the JS from pytest
(no JS engine), but we can guarantee the markers are still in the
template so a refactor doesn't silently drop them.

Visual verification of the rendered banner is the operator's job
in Chrome (per CLAUDE.md UI-Chrome-MCP rule). These tests guard
against accidental deletion of the wiring.
"""

from pathlib import Path

BASE_HTML = Path(__file__).resolve().parent.parent / "src" / "templates" / "base.html"


class TestCategoryIntelBannerWiring:
    def setup_method(self):
        self.content = BASE_HTML.read_text(encoding="utf-8")

    def test_category_intel_field_is_read_from_response(self):
        # The handler reads d.category_intel into a local var
        assert "var ci = d.category_intel || null;" in self.content

    def test_loss_bucket_branch_renders_red(self):
        # Red color is applied when ci.danger is true
        assert "isLoss = !!ci.danger" in self.content

    def test_win_bucket_branch_renders_green(self):
        # Green icon when WIN BUCKET fires
        assert "var icon = isLoss ? '⚠' : '✓';" in self.content

    def test_warning_text_is_escaped(self):
        # User-visible warning_text must go through escapeHtml
        assert "escapeHtml(ci.warning_text)" in self.content

    def test_neutral_bucket_branch_present(self):
        # Known but not loud → small grey row, not full banner
        assert "ci.category !== 'uncategorized'" in self.content

    def test_banner_renders_above_stat_strip(self):
        # Concatenation order must put banner first
        idx_banner = self.content.find("body.innerHTML = categoryIntelHtml +")
        assert idx_banner > 0, "banner not concatenated into body.innerHTML"
        # And the old order (without banner first) is gone
        assert "body.innerHTML = stripHtml + oracleHtml" not in self.content
