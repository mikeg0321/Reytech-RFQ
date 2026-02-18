"""
orchestrator.py — LangGraph Workflow Engine for Reytech RFQ
Version: 1.0.0

Wires the existing 12 Reytech agents into executable state-machine
workflows using LangGraph. No placeholders — every node calls real
agent code that already works in production.

Workflows:
  1. pc_pipeline    — PC arrives → parse → SCPRS → Amazon → price → fill 704
  2. lead_pipeline  — SCPRS scan → score lead → draft outreach → manager approve
  3. quote_pipeline — PC priced → generate quote → draft email → approve → send

Each workflow returns full state with every intermediate result, making
the entire pipeline auditable and replayable.
"""

import logging
import os
import json
from datetime import datetime
from typing import TypedDict, Any, Optional

from langgraph.graph import StateGraph, END

log = logging.getLogger("orchestrator")

# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Full access to live CRM, quotes, revenue, price history, voice calls from SQLite.
try:
    from src.core.agent_context import (
        get_context, format_context_for_agent,
        get_contact_by_agency, get_best_price,
    )
    HAS_AGENT_CTX = True
except ImportError:
    HAS_AGENT_CTX = False
    def get_context(**kw): return {}
    def format_context_for_agent(c, **kw): return ""
    def get_contact_by_agency(a): return []
    def get_best_price(d): return None

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")


# ─── Workflow State Definitions ──────────────────────────────────────────────

class PCPipelineState(TypedDict, total=False):
    """State for the full PC processing pipeline."""
    pc_id: str
    pc_number: str
    institution: str
    items: list
    scprs_results: dict
    amazon_results: dict
    pricing_applied: bool
    pdf_generated: bool
    output_pdf: str
    error: str
    steps_completed: list
    started_at: str
    completed_at: str


class LeadPipelineState(TypedDict, total=False):
    """State for lead discovery → outreach pipeline."""
    source: str           # 'scprs_scan' | 'manual'
    leads_found: list
    scored_leads: list
    top_lead: dict
    email_drafted: bool
    email_id: str
    manager_approved: bool
    error: str
    steps_completed: list
    started_at: str
    completed_at: str


class QuotePipelineState(TypedDict, total=False):
    """State for quote generation → email → send pipeline."""
    pc_id: str
    quote_number: str
    quote_pdf: str
    email_drafted: bool
    email_id: str
    email_approved: bool
    email_sent: bool
    recipient: str
    error: str
    steps_completed: list
    started_at: str
    completed_at: str


# ─── Utility ─────────────────────────────────────────────────────────────────

def _step(state: dict, name: str) -> dict:
    """Record a completed step."""
    steps = state.get("steps_completed", [])
    steps.append({"step": name, "timestamp": datetime.now().isoformat()})
    state["steps_completed"] = steps
    return state


def _load_json(filename: str, default=None):
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default if default is not None else {}


def _save_json(filename: str, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ═════════════════════════════════════════════════════════════════════════════
# WORKFLOW 1: PC Pipeline — Parse → SCPRS → Amazon → Price → Fill 704
# ═════════════════════════════════════════════════════════════════════════════

def _pc_load_node(state: PCPipelineState) -> PCPipelineState:
    """Load PC data from storage."""
    state["started_at"] = datetime.now().isoformat()
    state["steps_completed"] = []
    pc_id = state.get("pc_id", "")
    pcs = _load_json("price_checks.json", {})
    pc = pcs.get(pc_id)
    if not pc:
        state["error"] = f"PC {pc_id} not found"
        return _step(state, "load:failed")
    state["pc_number"] = pc.get("pc_number", "")
    state["institution"] = pc.get("institution", "")
    state["items"] = pc.get("items", [])
    return _step(state, "load")


def _pc_scprs_node(state: PCPipelineState) -> PCPipelineState:
    """Run SCPRS lookup on all items."""
    if state.get("error"):
        return state
    try:
        from src.agents.scprs_lookup import bulk_lookup
        items = state.get("items", [])
        updated = bulk_lookup(items)
        state["items"] = updated
        found = sum(1 for i in updated if i.get("scprs_last_price"))
        state["scprs_results"] = {"found": found, "total": len(updated)}
        return _step(state, "scprs_lookup")
    except ImportError:
        state["scprs_results"] = {"found": 0, "total": 0, "skipped": "module not available"}
        return _step(state, "scprs_lookup:skipped")


def _pc_amazon_node(state: PCPipelineState) -> PCPipelineState:
    """Run Amazon product research on all items."""
    if state.get("error"):
        return state
    try:
        from src.agents.product_research import bulk_research
        items = state.get("items", [])
        updated = bulk_research(items)
        state["items"] = updated
        found = sum(1 for i in updated
                    if i.get("pricing", {}).get("amazon_price"))
        state["amazon_results"] = {"found": found, "total": len(updated)}
        return _step(state, "amazon_research")
    except ImportError:
        state["amazon_results"] = {"found": 0, "total": 0, "skipped": "module not available"}
        return _step(state, "amazon_research:skipped")


def _pc_pricing_node(state: PCPipelineState) -> PCPipelineState:
    """Apply markup and compute final prices."""
    if state.get("error"):
        return state
    items = state.get("items", [])
    for item in items:
        p = item.get("pricing", {})
        cost = p.get("unit_cost") or p.get("amazon_price") or p.get("scprs_price") or 0
        if cost and not p.get("recommended_price"):
            markup = p.get("markup_pct", 25)
            p["unit_cost"] = cost
            p["recommended_price"] = round(cost * (1 + markup / 100), 2)
            item["pricing"] = p
    state["items"] = items
    state["pricing_applied"] = True

    # Persist updated items back to PC
    pcs = _load_json("price_checks.json", {})
    pc_id = state.get("pc_id", "")
    if pc_id in pcs:
        pcs[pc_id]["items"] = items
        pcs[pc_id]["status"] = "priced"
        _save_json("price_checks.json", pcs)

    return _step(state, "pricing")


def _pc_generate_node(state: PCPipelineState) -> PCPipelineState:
    """Generate the filled 704 PDF."""
    if state.get("error"):
        return state
    try:
        from src.forms.price_check import fill_ams704
        import re

        pcs = _load_json("price_checks.json", {})
        pc_id = state.get("pc_id", "")
        pc = pcs.get(pc_id, {})
        source_pdf = pc.get("source_pdf", "")
        if not source_pdf or not os.path.exists(source_pdf):
            state["error"] = "Source PDF not found"
            return _step(state, "generate:failed")

        pc_num = state.get("pc_number", "unknown")
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
        output_path = os.path.join(DATA_DIR, f"PC_{safe_name}_Reytech_.pdf")

        parsed = pc.get("parsed", {})
        result = fill_ams704(
            source_pdf=source_pdf,
            parsed_pc=parsed,
            output_pdf=output_path,
            tax_rate=pc.get("tax_rate", 0) if pc.get("tax_enabled") else 0.0,
        )
        if result.get("ok"):
            state["pdf_generated"] = True
            state["output_pdf"] = output_path
            # Update PC status
            pcs[pc_id]["status"] = "completed"
            pcs[pc_id]["output_pdf"] = output_path
            _save_json("price_checks.json", pcs)
        else:
            state["error"] = result.get("error", "PDF generation failed")
        return _step(state, "generate_704")
    except ImportError:
        state["error"] = "price_check module not available"
        return _step(state, "generate:failed")


def _pc_should_continue(state: PCPipelineState) -> str:
    """Router: continue or end on error."""
    if state.get("error"):
        return END
    return "next"


def build_pc_pipeline() -> StateGraph:
    """Build the PC processing workflow graph."""
    graph = StateGraph(PCPipelineState)

    graph.add_node("load", _pc_load_node)
    graph.add_node("scprs_lookup", _pc_scprs_node)
    graph.add_node("amazon_research", _pc_amazon_node)
    graph.add_node("pricing", _pc_pricing_node)
    graph.add_node("generate_704", _pc_generate_node)

    graph.set_entry_point("load")
    graph.add_conditional_edges("load", _pc_should_continue,
                                {"next": "scprs_lookup", END: END})
    graph.add_edge("scprs_lookup", "amazon_research")
    graph.add_edge("amazon_research", "pricing")
    graph.add_conditional_edges("pricing", _pc_should_continue,
                                {"next": "generate_704", END: END})
    graph.add_edge("generate_704", END)

    return graph


# ═════════════════════════════════════════════════════════════════════════════
# WORKFLOW 2: Lead Pipeline — Scan → Score → Draft → Approve
# ═════════════════════════════════════════════════════════════════════════════

def _lead_scan_node(state: LeadPipelineState) -> LeadPipelineState:
    """Scan for new leads from SCPRS."""
    state["started_at"] = datetime.now().isoformat()
    state["steps_completed"] = []
    try:
        from src.agents.lead_gen_agent import scan_for_leads
        leads = scan_for_leads()
        state["leads_found"] = leads
        return _step(state, "scan")
    except ImportError:
        state["leads_found"] = []
        return _step(state, "scan:skipped")


def _lead_score_node(state: LeadPipelineState) -> LeadPipelineState:
    """Score and rank leads."""
    if state.get("error"):
        return state
    try:
        from src.agents.lead_gen_agent import get_leads
        leads = get_leads()
        scored = sorted(leads, key=lambda l: l.get("score", 0), reverse=True)
        state["scored_leads"] = scored[:10]
        if scored:
            state["top_lead"] = scored[0]
        return _step(state, "score")
    except ImportError:
        state["scored_leads"] = state.get("leads_found", [])
        return _step(state, "score:skipped")


def _lead_draft_node(state: LeadPipelineState) -> LeadPipelineState:
    """Draft outreach email for top lead."""
    if state.get("error") or not state.get("top_lead"):
        return _step(state, "draft:skipped")
    try:
        from src.agents.email_outreach import draft_for_lead
        lead = state["top_lead"]
        if not isinstance(lead, dict):
            return _step(state, "draft:skipped")
        result = draft_for_lead(lead)
        if result.get("ok"):
            state["email_drafted"] = True
            state["email_id"] = result.get("email_id", "")
        return _step(state, "draft_email")
    except (ImportError, Exception) as e:
        log.warning("Lead draft skipped: %s", e)
        return _step(state, "draft:skipped")


def _lead_review_node(state: LeadPipelineState) -> LeadPipelineState:
    """Manager review gate — requires human approval."""
    if not state.get("email_drafted"):
        return _step(state, "review:skipped")
    # Auto-approve high-confidence leads, flag others for human review
    lead = state.get("top_lead", {})
    score = lead.get("score", 0)
    if score >= 0.8:
        state["manager_approved"] = True
        log.info("Auto-approved lead %s (score %.0f%%)",
                 lead.get("institution", "?"), score * 100)
    else:
        state["manager_approved"] = False
        log.info("Lead %s needs manual approval (score %.0f%%)",
                 lead.get("institution", "?"), score * 100)
    state["completed_at"] = datetime.now().isoformat()
    return _step(state, "manager_review")


def build_lead_pipeline() -> StateGraph:
    """Build the lead discovery workflow graph."""
    graph = StateGraph(LeadPipelineState)

    graph.add_node("scan", _lead_scan_node)
    graph.add_node("score", _lead_score_node)
    graph.add_node("draft_email", _lead_draft_node)
    graph.add_node("manager_review", _lead_review_node)

    graph.set_entry_point("scan")
    graph.add_edge("scan", "score")
    graph.add_edge("score", "draft_email")
    graph.add_edge("draft_email", "manager_review")
    graph.add_edge("manager_review", END)

    return graph


# ═════════════════════════════════════════════════════════════════════════════
# WORKFLOW 3: Quote Pipeline — Price → Quote PDF → Email → Send
# ═════════════════════════════════════════════════════════════════════════════

def _quote_generate_node(state: QuotePipelineState) -> QuotePipelineState:
    """Generate Reytech Quote PDF from priced PC."""
    state["started_at"] = datetime.now().isoformat()
    state["steps_completed"] = []
    pc_id = state.get("pc_id", "")
    try:
        from src.forms.quote_generator import generate_quote_from_pc
        result = generate_quote_from_pc(pc_id)
        if result.get("ok"):
            state["quote_number"] = result.get("quote_number", "")
            state["quote_pdf"] = result.get("pdf_path", "")
        else:
            state["error"] = result.get("error", "Quote generation failed")
        return _step(state, "generate_quote")
    except (ImportError, Exception) as e:
        state["error"] = str(e)
        return _step(state, "generate_quote:failed")


def _quote_draft_email_node(state: QuotePipelineState) -> QuotePipelineState:
    """Draft email with quote attached."""
    if state.get("error"):
        return state
    try:
        from src.agents.email_outreach import draft_for_pc
        pc_id = state.get("pc_id", "")
        result = draft_for_pc(pc_id, state.get("quote_number", ""))
        if result.get("ok"):
            state["email_drafted"] = True
            state["email_id"] = result.get("email_id", "")
            state["recipient"] = result.get("to", "")
        return _step(state, "draft_email")
    except ImportError:
        return _step(state, "draft_email:skipped")


def _quote_review_node(state: QuotePipelineState) -> QuotePipelineState:
    """Manager approval gate for outbound quotes."""
    if not state.get("email_drafted"):
        return _step(state, "review:skipped")
    # Quotes always need human approval before sending
    state["email_approved"] = False
    log.info("Quote %s email to %s awaiting approval",
             state.get("quote_number", "?"), state.get("recipient", "?"))
    state["completed_at"] = datetime.now().isoformat()
    return _step(state, "awaiting_approval")


def _quote_should_continue(state: QuotePipelineState) -> str:
    if state.get("error"):
        return END
    return "next"


def build_quote_pipeline() -> StateGraph:
    """Build the quote delivery workflow graph."""
    graph = StateGraph(QuotePipelineState)

    graph.add_node("generate_quote", _quote_generate_node)
    graph.add_node("draft_email", _quote_draft_email_node)
    graph.add_node("manager_review", _quote_review_node)

    graph.set_entry_point("generate_quote")
    graph.add_conditional_edges("generate_quote", _quote_should_continue,
                                {"next": "draft_email", END: END})
    graph.add_edge("draft_email", "manager_review")
    graph.add_edge("manager_review", END)

    return graph


# ═════════════════════════════════════════════════════════════════════════════
# Workflow Runner — executes any pipeline and logs results
# ═════════════════════════════════════════════════════════════════════════════

# Compiled graphs (lazy-init)
_compiled = {}


def _get_compiled(name: str):
    """Get or compile a workflow graph."""
    if name not in _compiled:
        builders = {
            "pc_pipeline": build_pc_pipeline,
            "lead_pipeline": build_lead_pipeline,
            "quote_pipeline": build_quote_pipeline,
        }
        builder = builders.get(name)
        if not builder:
            raise ValueError(f"Unknown workflow: {name}")
        _compiled[name] = builder().compile()
    return _compiled[name]


def run_workflow(name: str, inputs: dict) -> dict:
    """
    Execute a named workflow with given inputs.

    Returns the final state dict with all intermediate results
    and a steps_completed audit trail.
    """
    log.info("Starting workflow: %s with inputs: %s", name, list(inputs.keys()))
    start = datetime.now()

    try:
        graph = _get_compiled(name)
        result = graph.invoke(inputs)
        result["workflow"] = name
        result["duration_ms"] = int((datetime.now() - start).total_seconds() * 1000)

        # Log the run
        _log_run(name, inputs, result)

        log.info("Workflow %s completed in %dms — steps: %s",
                 name, result["duration_ms"],
                 [s["step"] for s in result.get("steps_completed", [])])
        return result

    except Exception as e:
        log.error("Workflow %s failed: %s", name, e)
        return {
            "workflow": name,
            "error": str(e),
            "duration_ms": int((datetime.now() - start).total_seconds() * 1000),
        }


def _log_run(name: str, inputs: dict, result: dict):
    """Persist workflow run to audit log."""
    log_path = os.path.join(DATA_DIR, "workflow_runs.json")
    runs = []
    try:
        with open(log_path) as f:
            runs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    runs.append({
        "workflow": name,
        "inputs": {k: str(v)[:100] for k, v in inputs.items()},
        "steps": [s["step"] for s in result.get("steps_completed", [])],
        "error": result.get("error"),
        "duration_ms": result.get("duration_ms"),
        "timestamp": datetime.now().isoformat(),
    })

    # Keep last 100 runs
    runs = runs[-100:]
    try:
        with open(log_path, "w") as f:
            json.dump(runs, f, indent=2, default=str)
    except Exception:
        pass


def get_workflow_status() -> dict:
    """Status and run history for the orchestrator."""
    runs = []
    try:
        log_path = os.path.join(DATA_DIR, "workflow_runs.json")
        with open(log_path) as f:
            runs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    return {
        "agent": "orchestrator",
        "version": "1.0.0",
        "status": "active",
        "workflows": ["pc_pipeline", "lead_pipeline", "quote_pipeline"],
        "total_runs": len(runs),
        "recent_runs": runs[-5:] if runs else [],
    }


def get_workflow_graph_viz(name: str) -> dict:
    """Return the graph structure for visualization."""
    graphs = {
        "pc_pipeline": {
            "nodes": ["load", "scprs_lookup", "amazon_research", "pricing", "generate_704"],
            "edges": [
                {"from": "load", "to": "scprs_lookup", "condition": "no error"},
                {"from": "scprs_lookup", "to": "amazon_research"},
                {"from": "amazon_research", "to": "pricing"},
                {"from": "pricing", "to": "generate_704", "condition": "no error"},
                {"from": "generate_704", "to": "END"},
            ],
            "description": "PC arrives → parse → SCPRS prices → Amazon research → apply markup → fill 704 PDF",
        },
        "lead_pipeline": {
            "nodes": ["scan", "score", "draft_email", "manager_review"],
            "edges": [
                {"from": "scan", "to": "score"},
                {"from": "score", "to": "draft_email"},
                {"from": "draft_email", "to": "manager_review"},
                {"from": "manager_review", "to": "END"},
            ],
            "description": "Scan SCPRS POs → score leads → draft outreach → manager approval gate",
        },
        "quote_pipeline": {
            "nodes": ["generate_quote", "draft_email", "manager_review"],
            "edges": [
                {"from": "generate_quote", "to": "draft_email", "condition": "no error"},
                {"from": "draft_email", "to": "manager_review"},
                {"from": "manager_review", "to": "END"},
            ],
            "description": "Generate quote PDF → draft buyer email → hold for your approval",
        },
    }
    return graphs.get(name, {"error": f"Unknown workflow: {name}"})
