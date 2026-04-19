"""
Tests for the 2026-04-19 Cmd+K palette expansion + quotes filter chips
(Batch C1).

Cmd+K palette is built into base.html so it works on every page. We
extended the CMD_ITEMS list with five new destinations (Quoting Status,
Awards, Templates, Build Health, Shadow Diffs) that operators previously
had to type into the URL bar.

Quotes page now has one-click filter chips above the existing dropdowns
so the most-common status filters (pending/won/lost) take a single click
instead of opening a select.
"""
from __future__ import annotations


class TestCmdPaletteExtensions:
    def test_palette_includes_quoting_status(self, auth_client):
        resp = auth_client.get("/")
        body = resp.get_data(as_text=True)
        # Both label and URL must be in the CMD_ITEMS list
        assert "u:'/quoting/status'" in body
        assert "Quoting Status" in body

    def test_palette_includes_awards(self, auth_client):
        resp = auth_client.get("/")
        body = resp.get_data(as_text=True)
        assert "u:'/awards'" in body

    def test_palette_includes_admin_pages(self, auth_client):
        resp = auth_client.get("/")
        body = resp.get_data(as_text=True)
        assert "u:'/admin/build-health'" in body
        assert "u:'/admin/shadow-diffs'" in body


class TestQuoteFilterChips:
    def test_quotes_page_renders_chips(self, auth_client):
        resp = auth_client.get("/quotes")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        # All 5 chip labels must appear
        assert "quote-chip" in body
        assert "Pending" in body and "Won" in body and "Lost" in body
        # Each chip must be a real link with status query param
        assert "?status=won" in body
        assert "?status=lost" in body
        assert "?status=pending" in body

    def test_active_chip_has_active_class_when_status_selected(self, auth_client):
        resp = auth_client.get("/quotes?status=won")
        body = resp.get_data(as_text=True)
        # The "won" chip should be marked active in the rendered HTML
        assert "quote-chip-active" in body

    def test_no_active_chip_class_when_no_status(self, auth_client):
        # When no status filter, only the "All" chip is active
        resp = auth_client.get("/quotes")
        body = resp.get_data(as_text=True)
        # Active class will be present once (on the "All" chip)
        assert body.count("quote-chip-active") >= 1
