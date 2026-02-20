"""
trace.py — Workflow Tracing for Reytech RFQ System

Lightweight, thread-safe tracing for every workflow and email.
Traces persist in memory with auto-rotation (last 200 traces kept).

Usage:
    from src.api.trace import Trace, get_traces, get_trace

    # Start a trace
    t = Trace("email_pipeline", subject="Quote - Med OS")
    t.step("IMAP connected")
    t.step("PDF detected", filename="AMS 704 - Med OS.pdf", is_pc=True)
    t.step("PC created", pc_id="pc_abc123", items=5)
    t.ok("Pipeline complete")    # marks success
    # or
    t.fail("parse_ams704 failed", error=str(e))  # marks failure

    # Query traces
    traces = get_traces(workflow="email_pipeline", limit=20)
    trace = get_trace(trace_id)

Endpoints:
    GET /api/admin/traces                — all recent traces
    GET /api/admin/traces?workflow=X     — filter by workflow
    GET /api/admin/traces/<id>           — single trace detail
    DELETE /api/admin/traces             — clear all traces
"""

import threading
import time
import uuid
import logging
from datetime import datetime
from collections import deque

log = logging.getLogger("trace")

# ═══════════════════════════════════════════════════════════════════════
# Thread-safe trace storage
# ═══════════════════════════════════════════════════════════════════════

_lock = threading.Lock()
_traces = deque(maxlen=200)  # Auto-rotates: keeps last 200 traces
_traces_by_id = {}           # Quick lookup by trace_id


class Trace:
    """Records the journey of a single workflow execution."""
    
    def __init__(self, workflow: str, **context):
        self.id = f"tr_{uuid.uuid4().hex[:8]}"
        self.workflow = workflow
        self.context = context  # e.g. subject, email_uid, pc_id, rfq_id
        self.steps = []
        self.status = "running"  # running | ok | fail | warn
        self.started_at = datetime.now().isoformat()
        self.finished_at = None
        self.duration_ms = None
        self._t0 = time.time()
        
        # Auto-register
        with _lock:
            _traces.append(self)
            _traces_by_id[self.id] = self
            # Prune lookup dict to match deque
            if len(_traces_by_id) > 250:
                active_ids = {t.id for t in _traces}
                for tid in list(_traces_by_id.keys()):
                    if tid not in active_ids:
                        del _traces_by_id[tid]
    
    def step(self, message: str, **data):
        """Record a step in the workflow."""
        entry = {
            "t": round((time.time() - self._t0) * 1000),  # ms since start
            "msg": message,
        }
        if data:
            entry["data"] = data
        self.steps.append(entry)
        return self
    
    def ok(self, message: str = "Complete", **data):
        """Mark trace as successful."""
        self.step(message, **data)
        self.status = "ok"
        self._finish()
        return self
    
    def fail(self, message: str, **data):
        """Mark trace as failed."""
        self.step(f"FAIL: {message}", **data)
        self.status = "fail"
        self._finish()
        log.warning("[trace:%s] %s FAILED: %s", self.workflow, self.id, message)
        return self
    
    def warn(self, message: str, **data):
        """Record a warning (doesn't change status to fail)."""
        self.step(f"WARN: {message}", **data)
        if self.status == "running":
            self.status = "warn"
        return self
    
    def _finish(self):
        self.finished_at = datetime.now().isoformat()
        self.duration_ms = round((time.time() - self._t0) * 1000)
    
    def to_dict(self):
        return {
            "id": self.id,
            "workflow": self.workflow,
            "status": self.status,
            "context": self.context,
            "steps": self.steps,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "summary": self._summary(),
        }
    
    def _summary(self):
        """One-line summary for list views."""
        ctx_parts = []
        for k in ["subject", "pc_id", "rfq_id", "sol", "pc_number", "filename"]:
            if k in self.context:
                ctx_parts.append(f"{k}={self.context[k]}")
        ctx_str = ", ".join(ctx_parts[:3])
        last_msg = self.steps[-1]["msg"] if self.steps else "no steps"
        icon = {"ok": "✅", "fail": "❌", "warn": "⚠️", "running": "⏳"}.get(self.status, "?")
        return f"{icon} [{self.workflow}] {ctx_str} → {last_msg}"


# ═══════════════════════════════════════════════════════════════════════
# Query API
# ═══════════════════════════════════════════════════════════════════════

def get_traces(workflow=None, status=None, limit=50):
    """Get recent traces, optionally filtered."""
    with _lock:
        results = list(_traces)
    
    # Filter
    if workflow:
        results = [t for t in results if t.workflow == workflow]
    if status:
        results = [t for t in results if t.status == status]
    
    # Most recent first
    results = list(reversed(results))[:limit]
    return [t.to_dict() for t in results]


def get_trace(trace_id: str):
    """Get a single trace by ID."""
    with _lock:
        t = _traces_by_id.get(trace_id)
    return t.to_dict() if t else None


def clear_traces():
    """Clear all traces."""
    with _lock:
        _traces.clear()
        _traces_by_id.clear()


def get_summary():
    """Dashboard-level summary of recent trace health."""
    with _lock:
        all_traces = list(_traces)
    
    if not all_traces:
        return {"total": 0, "workflows": {}}
    
    workflows = {}
    for t in all_traces:
        wf = t.workflow
        if wf not in workflows:
            workflows[wf] = {"total": 0, "ok": 0, "fail": 0, "warn": 0, "running": 0}
        workflows[wf]["total"] += 1
        workflows[wf][t.status] = workflows[wf].get(t.status, 0) + 1
    
    # Last 10 failures
    recent_fails = [t.to_dict() for t in reversed(all_traces) if t.status == "fail"][:10]
    
    return {
        "total": len(all_traces),
        "ok": sum(1 for t in all_traces if t.status == "ok"),
        "fail": sum(1 for t in all_traces if t.status == "fail"),
        "warn": sum(1 for t in all_traces if t.status == "warn"),
        "running": sum(1 for t in all_traces if t.status == "running"),
        "workflows": workflows,
        "recent_failures": recent_fails,
    }
