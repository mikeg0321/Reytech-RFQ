"""Pin that voice_campaigns feature is fully deleted.

Per Mike 2026-05-10: "app isn't ready or stable enough to be building
agentic voice... quoting and data accuracy still an issue." The whole
feature surface was removed (not paused, not feature-flagged off —
deleted from the codebase) so no future session accidentally revives
a half-built path on shaky data.

This test pins the deletion so a revert/merge can't quietly bring it
back without an explicit decision.
"""
from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return pathlib.Path(rel).read_text(encoding="utf-8")


class TestVoiceCampaignsModuleStillNotImported:
    """src.agents.voice_campaigns was never built; nothing must import
    from it. Whole try/except block in routes_intel_ops was removed."""

    def test_routes_intel_ops_has_no_voice_campaigns_import(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "from src.agents.voice_campaigns" not in src, \
            "routes_intel_ops regressed: still imports phantom voice_campaigns"
        assert "CAMPAIGNS_AVAILABLE" not in src, \
            "routes_intel_ops regressed: still sets/uses CAMPAIGNS_AVAILABLE flag"

    def test_routes_voice_contacts_has_no_campaign_routes(self):
        src = _read("src/api/modules/routes_voice_contacts.py")
        # No campaign route paths
        assert '@bp.route("/campaigns")' not in src
        assert '@bp.route("/campaign/<' not in src
        assert '@bp.route("/api/campaigns' not in src
        # No campaign-function calls (entire section removed)
        assert "CAMPAIGNS_AVAILABLE" not in src, \
            "routes_voice_contacts still references CAMPAIGNS_AVAILABLE"
        assert "get_campaigns(" not in src
        assert "create_campaign(" not in src
        assert "execute_campaign_call(" not in src
        assert "get_campaign_stats(" not in src

    def test_config_has_no_campaigns_flag(self):
        src = _read("src/api/config.py")
        assert "CAMPAIGNS_AVAILABLE" not in src, \
            "config.py regressed: CAMPAIGNS_AVAILABLE flag should be deleted"

    def test_dashboard_import_excludes_campaigns_flag(self):
        src = _read("src/api/dashboard.py")
        # The cross-module flag import must not include CAMPAIGNS_AVAILABLE
        # anywhere (would crash at import-time since config.py no longer defines it)
        assert "CAMPAIGNS_AVAILABLE" not in src, \
            "dashboard.py regressed: still imports/uses CAMPAIGNS_AVAILABLE"


class TestVoiceCampaignsTemplatesDeleted:
    def test_voice_campaigns_html_deleted(self):
        assert not pathlib.Path("src/templates/voice_campaigns.html").exists(), \
            "voice_campaigns.html template must remain deleted"

    def test_campaign_detail_html_deleted(self):
        assert not pathlib.Path("src/templates/campaign_detail.html").exists(), \
            "campaign_detail.html template must remain deleted"


class TestNoCampaignRoutesRespond:
    """All campaign URL paths must return 404 (route deleted) rather
    than 200 / 500 (route still registered but broken)."""

    PATHS = ["/campaigns", "/campaign/abc", "/api/campaigns",
             "/api/campaigns/abc", "/api/campaigns/abc/call",
             "/api/campaigns/abc/outcome", "/api/campaigns/stats"]

    def test_all_campaign_paths_404(self, client):
        for path in self.PATHS:
            resp = client.get(path)
            assert resp.status_code == 404, \
                f"campaign route still registered: {path} → {resp.status_code}"
