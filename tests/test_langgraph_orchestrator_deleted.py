"""Pin that the LangGraph orchestrator feature is fully deleted.

Per Mike 2026-05-10: same playbook as voice_campaigns. The Feb 2026
LangGraph orchestrator (src/agents/orchestrator.py) was half-built
scaffolding that never worked end-to-end — the functions it called
(bulk_research in product_research, scan_for_leads in lead_gen_agent,
WorkflowOrchestrator class anywhere) never existed.

The REAL production orchestrator is src.core.quote_orchestrator —
that's the one routes_quoting_status uses and stays intact. This
test pins that the LangGraph one stays deleted.
"""
from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return pathlib.Path(rel).read_text(encoding="utf-8")


class TestLangGraphOrchestratorModuleDeleted:
    def test_orchestrator_module_file_gone(self):
        assert not pathlib.Path("src/agents/orchestrator.py").exists(), \
            "src/agents/orchestrator.py must remain deleted"

    def test_test_orchestrator_file_gone(self):
        assert not pathlib.Path("tests/test_orchestrator.py").exists(), \
            "tests/test_orchestrator.py must remain deleted"

    def test_module_does_not_import(self):
        try:
            import importlib
            importlib.import_module("src.agents.orchestrator")
            raise AssertionError("src.agents.orchestrator should not be importable")
        except ImportError:
            pass


class TestRoutesIntelOpsNoOrchestratorRefs:
    def test_no_orchestrator_import_block(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "from src.agents.orchestrator import" not in src, \
            "routes_intel_ops regressed: still imports the LangGraph orchestrator"

    def test_no_orchestrator_available_flag(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert "ORCHESTRATOR_AVAILABLE" not in src, \
            "ORCHESTRATOR_AVAILABLE flag must remain deleted"

    def test_no_workflow_routes(self):
        src = _read("src/api/modules/routes_intel_ops.py")
        assert '@bp.route("/api/workflow/run"' not in src
        assert '@bp.route("/api/workflow/status"' not in src
        assert '@bp.route("/api/workflow/graph/' not in src

    def test_status_response_has_no_orchestrator_field(self):
        """The status-response dict at api_agents_status must not list
        the orchestrator any more."""
        src = _read("src/api/modules/routes_intel_ops.py")
        # The status field signature `"orchestrator": get_workflow_status()`
        # must not appear (manager/qa next to each other instead).
        assert '"orchestrator": get_workflow_status()' not in src


class TestConfigAndDashboardCleanup:
    def test_config_has_no_orchestrator_flag(self):
        src = _read("src/api/config.py")
        assert "ORCHESTRATOR_AVAILABLE" not in src, \
            "config.py regressed: ORCHESTRATOR_AVAILABLE flag should be deleted"

    def test_dashboard_import_excludes_orchestrator_flag(self):
        src = _read("src/api/dashboard.py")
        assert "ORCHESTRATOR_AVAILABLE" not in src, \
            "dashboard.py regressed: still imports ORCHESTRATOR_AVAILABLE"


class TestQaAgentNoOrchestratorCheck:
    def test_no_check_orchestrator_function(self):
        src = _read("src/agents/qa_agent.py")
        assert "def _check_orchestrator" not in src, \
            "qa_agent regressed: _check_orchestrator function should be deleted"

    def test_no_orchestrator_check_registration(self):
        src = _read("src/agents/qa_agent.py")
        # The registration line `"orchestrator": _check_orchestrator,` must
        # not be present in the _CHECKS dict
        assert '"orchestrator": _check_orchestrator' not in src


class TestNoWorkflowRoutesRespond:
    """Hit each removed /api/workflow path — must 404, not 200/500."""

    PATHS = ["/api/workflow/status", "/api/workflow/graph/pc", "/api/workflow/run"]

    def test_all_workflow_paths_404(self, client):
        for path in self.PATHS:
            resp = client.get(path)
            assert resp.status_code == 404, \
                f"workflow route still registered: {path} → {resp.status_code}"


class TestRealQuoteOrchestratorStillWorks:
    """src.core.quote_orchestrator is the REAL production orchestrator
    (used by routes_quoting_status). It must NOT be affected by this
    deletion of the LangGraph one."""

    def test_quote_orchestrator_still_resolves(self):
        from src.core.quote_orchestrator import QuoteOrchestrator
        assert QuoteOrchestrator is not None
