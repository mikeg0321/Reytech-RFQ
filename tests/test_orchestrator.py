"""Tests for LangGraph Orchestrator — workflow graph builds, state typing, runner."""

import pytest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestGraphBuilds:
    """Verify all workflow graphs compile without error."""

    def test_pc_pipeline_compiles(self):
        from src.agents.orchestrator import build_pc_pipeline
        graph = build_pc_pipeline()
        compiled = graph.compile()
        assert compiled is not None

    def test_lead_pipeline_compiles(self):
        from src.agents.orchestrator import build_lead_pipeline
        graph = build_lead_pipeline()
        compiled = graph.compile()
        assert compiled is not None

    def test_quote_pipeline_compiles(self):
        from src.agents.orchestrator import build_quote_pipeline
        graph = build_quote_pipeline()
        compiled = graph.compile()
        assert compiled is not None


class TestPCPipelineNodes:
    """Test individual PC pipeline nodes."""

    def test_load_nonexistent_pc(self):
        from src.agents.orchestrator import _pc_load_node
        state = {"pc_id": "nonexistent_xyz"}
        result = _pc_load_node(state)
        assert result.get("error")
        assert "not found" in result["error"]
        assert len(result.get("steps_completed", [])) == 1
        assert "failed" in result["steps_completed"][0]["step"]

    def test_pricing_node_applies_markup(self):
        from src.agents.orchestrator import _pc_pricing_node
        state = {
            "pc_id": "test",
            "items": [
                {"description": "Test item", "pricing": {"unit_cost": 100, "markup_pct": 25}},
            ],
            "steps_completed": [],
        }
        result = _pc_pricing_node(state)
        assert result.get("pricing_applied") is True
        assert result["items"][0]["pricing"]["recommended_price"] == 125.0

    def test_pricing_node_preserves_existing_price(self):
        from src.agents.orchestrator import _pc_pricing_node
        state = {
            "pc_id": "test",
            "items": [
                {"description": "Test", "pricing": {"unit_cost": 100, "recommended_price": 150}},
            ],
            "steps_completed": [],
        }
        result = _pc_pricing_node(state)
        # Should NOT overwrite the existing recommended_price
        assert result["items"][0]["pricing"]["recommended_price"] == 150

    def test_scprs_node_handles_missing_module(self):
        """SCPRS node should gracefully skip if module isn't available."""
        from src.agents.orchestrator import _pc_scprs_node
        state = {"items": [], "steps_completed": []}
        result = _pc_scprs_node(state)
        assert "steps_completed" in result
        # Should complete (possibly skipped) but not error
        assert not result.get("error")

    def test_error_short_circuits(self):
        """If state has error, subsequent nodes should be no-ops."""
        from src.agents.orchestrator import _pc_scprs_node, _pc_amazon_node
        state = {"error": "test error", "items": [], "steps_completed": []}
        result = _pc_scprs_node(state)
        assert result.get("error") == "test error"
        # Shouldn't have added a step
        assert len(result.get("steps_completed", [])) == 0


class TestLeadPipelineNodes:

    def test_scan_node_runs(self):
        from src.agents.orchestrator import _lead_scan_node
        state = {}
        result = _lead_scan_node(state)
        assert "started_at" in result
        assert isinstance(result.get("leads_found"), list)
        assert len(result.get("steps_completed", [])) >= 1

    def test_score_node_runs(self):
        from src.agents.orchestrator import _lead_score_node
        state = {"leads_found": [], "steps_completed": []}
        result = _lead_score_node(state)
        assert isinstance(result.get("scored_leads"), list)

    def test_review_auto_approves_high_score(self):
        from src.agents.orchestrator import _lead_review_node
        state = {
            "email_drafted": True,
            "top_lead": {"institution": "Test", "score": 0.9},
            "steps_completed": [],
        }
        result = _lead_review_node(state)
        assert result.get("manager_approved") is True

    def test_review_flags_low_score(self):
        from src.agents.orchestrator import _lead_review_node
        state = {
            "email_drafted": True,
            "top_lead": {"institution": "Test", "score": 0.5},
            "steps_completed": [],
        }
        result = _lead_review_node(state)
        assert result.get("manager_approved") is False


class TestQuotePipelineNodes:

    def test_review_always_holds_for_approval(self):
        """Quotes always need human approval — never auto-send."""
        from src.agents.orchestrator import _quote_review_node
        state = {
            "email_drafted": True,
            "quote_number": "R26Q99",
            "recipient": "test@test.com",
            "steps_completed": [],
        }
        result = _quote_review_node(state)
        assert result.get("email_approved") is False
        assert "awaiting_approval" in result["steps_completed"][-1]["step"]


class TestWorkflowRunner:

    def test_run_unknown_workflow(self):
        from src.agents.orchestrator import run_workflow
        result = run_workflow("nonexistent_workflow", {})
        assert result.get("error")

    def test_run_pc_pipeline_with_missing_pc(self):
        from src.agents.orchestrator import run_workflow
        result = run_workflow("pc_pipeline", {"pc_id": "doesnt_exist_999"})
        # Should complete with error, not crash
        assert result.get("workflow") == "pc_pipeline"
        assert "duration_ms" in result

    def test_run_lead_pipeline(self):
        from src.agents.orchestrator import run_workflow
        result = run_workflow("lead_pipeline", {})
        assert result.get("workflow") == "lead_pipeline"
        assert "duration_ms" in result
        # Should complete without crashing (may have no leads to process)
        assert not result.get("error"), f"Unexpected error: {result.get('error')}"


class TestWorkflowStatus:

    def test_status_structure(self):
        from src.agents.orchestrator import get_workflow_status
        status = get_workflow_status()
        assert status["agent"] == "orchestrator"
        assert status["version"] == "1.0.0"
        assert "workflows" in status
        assert "pc_pipeline" in status["workflows"]
        assert "lead_pipeline" in status["workflows"]
        assert "quote_pipeline" in status["workflows"]

    def test_graph_viz(self):
        from src.agents.orchestrator import get_workflow_graph_viz
        viz = get_workflow_graph_viz("pc_pipeline")
        assert "nodes" in viz
        assert "edges" in viz
        assert "description" in viz
        assert len(viz["nodes"]) == 5
        assert "scprs_lookup" in viz["nodes"]
        assert "pricing" in viz["nodes"]

    def test_graph_viz_lead(self):
        from src.agents.orchestrator import get_workflow_graph_viz
        viz = get_workflow_graph_viz("lead_pipeline")
        assert "scan" in viz["nodes"]
        assert "manager_review" in viz["nodes"]

    def test_graph_viz_unknown(self):
        from src.agents.orchestrator import get_workflow_graph_viz
        viz = get_workflow_graph_viz("nonexistent")
        assert "error" in viz


class TestStepTracking:

    def test_steps_have_timestamps(self):
        from src.agents.orchestrator import _step
        state = {"steps_completed": []}
        result = _step(state, "test_step")
        assert len(result["steps_completed"]) == 1
        assert result["steps_completed"][0]["step"] == "test_step"
        assert "timestamp" in result["steps_completed"][0]

    def test_multiple_steps_accumulate(self):
        from src.agents.orchestrator import _step
        state = {"steps_completed": []}
        state = _step(state, "step1")
        state = _step(state, "step2")
        state = _step(state, "step3")
        assert len(state["steps_completed"]) == 3
        names = [s["step"] for s in state["steps_completed"]]
        assert names == ["step1", "step2", "step3"]
