"""
workflow_tracker.py — Persist background task status to SQLite.

Replaces in-memory status dicts with durable DB-backed tracking.
Agents write status updates; poll endpoints read from DB.
In-memory dicts remain as fast cache; DB provides durability across restarts.

Usage:
    from src.core.workflow_tracker import tracker

    # Starting a task:
    tracker.start("research_pc123", "product_research", items_total=10)

    # Updating progress:
    tracker.update("research_pc123", phase="searching", progress="Item 3/10", items_done=3)

    # Recording error:
    tracker.error("research_pc123", "API timeout on item 4")

    # Completing:
    tracker.finish("research_pc123", results_count=8)

    # Reading status:
    status = tracker.get_status("research_pc123")

    # List active tasks:
    active = tracker.get_active(task_type="product_research")
"""

import json
import logging
from datetime import datetime

log = logging.getLogger("reytech.workflow")


class WorkflowTracker:
    """Durable background task status tracking via SQLite."""

    @staticmethod
    def start(task_id, task_type, items_total=0):
        """Mark a workflow as started. Creates or resets the row."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO workflow_runs
                    (id, task_type, status, phase, progress, running,
                     items_done, items_total, results_count, errors_json,
                     started_at, finished_at, last_updated)
                    VALUES (?, ?, 'running', '', '', 1, 0, ?, 0, '[]',
                            datetime('now'), '', datetime('now'))
                """, (task_id, task_type, items_total))
        except Exception as e:
            log.debug("workflow_tracker.start failed (non-fatal): %s", e)

    @staticmethod
    def update(task_id, phase=None, progress=None, items_done=None,
               results_count=None):
        """Update progress fields on a running workflow."""
        try:
            from src.core.db import get_db
            sets = ["last_updated = datetime('now')"]
            params = []
            if phase is not None:
                sets.append("phase = ?")
                params.append(phase)
            if progress is not None:
                sets.append("progress = ?")
                params.append(progress)
            if items_done is not None:
                sets.append("items_done = ?")
                params.append(items_done)
            if results_count is not None:
                sets.append("results_count = ?")
                params.append(results_count)
            if not params:
                return
            params.append(task_id)
            with get_db() as conn:
                conn.execute(
                    f"UPDATE workflow_runs SET {', '.join(sets)} WHERE id = ?",
                    params
                )
        except Exception as e:
            log.debug("workflow_tracker.update failed (non-fatal): %s", e)

    @staticmethod
    def error(task_id, error_msg):
        """Append an error message to the workflow's error list."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT errors_json FROM workflow_runs WHERE id = ?",
                    (task_id,)
                ).fetchone()
                if row:
                    errors = json.loads(row["errors_json"] or "[]")
                    errors.append(str(error_msg))
                    conn.execute(
                        "UPDATE workflow_runs SET errors_json = ?, last_updated = datetime('now') WHERE id = ?",
                        (json.dumps(errors[-50:]), task_id)  # Cap at 50 errors
                    )
        except Exception as e:
            log.debug("workflow_tracker.error failed (non-fatal): %s", e)

    @staticmethod
    def finish(task_id, results_count=None, status="completed"):
        """Mark workflow as completed or failed."""
        try:
            from src.core.db import get_db
            sets = [
                "running = 0",
                "status = ?",
                "finished_at = datetime('now')",
                "last_updated = datetime('now')",
            ]
            params = [status]
            if results_count is not None:
                sets.append("results_count = ?")
                params.append(results_count)
            params.append(task_id)
            with get_db() as conn:
                conn.execute(
                    f"UPDATE workflow_runs SET {', '.join(sets)} WHERE id = ?",
                    params
                )
        except Exception as e:
            log.debug("workflow_tracker.finish failed (non-fatal): %s", e)

    @staticmethod
    def get_status(task_id):
        """Get status dict for a workflow. Returns None if not found."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT * FROM workflow_runs WHERE id = ?",
                    (task_id,)
                ).fetchone()
                if not row:
                    return None
                d = dict(row)
                d["errors"] = json.loads(d.pop("errors_json", "[]"))
                d["running"] = bool(d.get("running", 0))
                return d
        except Exception as e:
            log.debug("workflow_tracker.get_status failed: %s", e)
            return None

    @staticmethod
    def get_active(task_type=None):
        """Get all currently running workflows, optionally filtered by type."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                if task_type:
                    rows = conn.execute(
                        "SELECT * FROM workflow_runs WHERE running = 1 AND task_type = ? ORDER BY started_at DESC",
                        (task_type,)
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM workflow_runs WHERE running = 1 ORDER BY started_at DESC"
                    ).fetchall()
                result = []
                for row in rows:
                    d = dict(row)
                    d["errors"] = json.loads(d.pop("errors_json", "[]"))
                    d["running"] = bool(d.get("running", 0))
                    result.append(d)
                return result
        except Exception as e:
            log.debug("workflow_tracker.get_active failed: %s", e)
            return []

    @staticmethod
    def get_recent(limit=20):
        """Get recent workflows (running + completed) for dashboard."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute(
                    "SELECT * FROM workflow_runs ORDER BY last_updated DESC LIMIT ?",
                    (limit,)
                ).fetchall()
                result = []
                for row in rows:
                    d = dict(row)
                    d["errors"] = json.loads(d.pop("errors_json", "[]"))
                    d["running"] = bool(d.get("running", 0))
                    result.append(d)
                return result
        except Exception as e:
            log.debug("workflow_tracker.get_recent failed: %s", e)
            return []

    @staticmethod
    def cleanup(max_age_hours=24):
        """Remove completed workflows older than max_age_hours."""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute(
                    "DELETE FROM workflow_runs WHERE running = 0 AND finished_at < datetime('now', ? || ' hours')",
                    (f"-{max_age_hours}",)
                )
        except Exception as e:
            log.debug("workflow_tracker.cleanup failed: %s", e)


# Module-level singleton
tracker = WorkflowTracker()
