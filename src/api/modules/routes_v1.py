"""
routes_v1.py — MCP-ready /api/v1/ endpoints for external AI agents.
Stable contract: these routes will not change shape without version bump.
"""
from flask import request, jsonify
from src.api.shared import bp, auth_required, api_response
import logging

log = logging.getLogger("reytech")


@bp.route("/api/v1/rfq/<rfq_id>")
@auth_required
def api_v1_get_rfq(rfq_id):
    """Get a single RFQ with line items.
    Returns: api_response({...rfq fields...}) or 404.
    Auth: X-API-Key or Basic Auth.
    """
    try:
        from src.core.dal import get_rfq, get_line_items
        rfq = get_rfq(rfq_id)
        if not rfq:
            return api_response(error="RFQ not found", status=404)
        rfq["line_items"] = get_line_items(rfq_id, "rfq")
        return api_response(rfq)
    except Exception as e:
        log.error("v1/rfq/%s error: %s", rfq_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/rfq/<rfq_id>/price", methods=["POST"])
@auth_required
def api_v1_price_rfq(rfq_id):
    """Trigger pricing on an RFQ. Enqueues as async task.
    Accepts: {"force": bool} (optional)
    Returns: api_response({"status": "queued", "rfq_id": id})
    Auth: X-API-Key or Basic Auth.
    """
    try:
        from src.core.dal import get_rfq
        rfq = get_rfq(rfq_id)
        if not rfq:
            return api_response(error="RFQ not found", status=404)
        data = request.get_json(silent=True) or {}
        force = data.get("force", False)
        try:
            from src.core.task_queue import enqueue
            task_id = enqueue("price_rfq", {"rfq_id": rfq_id, "force": force},
                              actor="api_v1")
            return api_response({"status": "queued", "rfq_id": rfq_id, "task_id": task_id})
        except Exception:
            # Fallback: task_queue not initialized yet, return accepted
            return api_response({"status": "accepted", "rfq_id": rfq_id})
    except Exception as e:
        log.error("v1/rfq/%s/price error: %s", rfq_id, e, exc_info=True)
        return api_response(error=str(e), status=500)


@bp.route("/api/v1/pipeline")
@auth_required
def api_v1_pipeline():
    """Current queue depths and agent status for external orchestrators.
    Returns: api_response({rfqs: {...}, pcs: {...}, orders: {...}, agents: {...}})
    Auth: X-API-Key or Basic Auth.
    """
    try:
        from src.core.dal import list_rfqs, list_pcs, list_orders
        from collections import Counter

        # RFQ status counts
        rfqs = list_rfqs(limit=10000)
        rfq_counts = dict(Counter(r.get("status", "unknown") for r in rfqs))

        # PC status counts
        pcs = list_pcs(limit=10000)
        pc_counts = dict(Counter(p.get("status", "unknown") for p in pcs))

        # Order status counts
        orders = list_orders(limit=10000)
        order_counts = dict(Counter(o.get("status", "unknown") for o in orders))

        # Agent status from scheduler
        agents = {}
        try:
            from src.core.scheduler import get_all_jobs
            for job in get_all_jobs():
                agents[job["name"]] = {
                    "status": job["status"],
                    "last_run": job.get("last_run"),
                    "error_count": job.get("error_count", 0),
                }
        except Exception:
            pass

        return api_response({
            "rfqs": {"total": len(rfqs), "by_status": rfq_counts},
            "pcs": {"total": len(pcs), "by_status": pc_counts},
            "orders": {"total": len(orders), "by_status": order_counts},
            "agents": agents,
        })
    except Exception as e:
        log.error("v1/pipeline error: %s", e, exc_info=True)
        return api_response(error=str(e), status=500)
