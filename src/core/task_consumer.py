"""
task_consumer.py — Background consumer for the SQLite task queue.

Dequeues tasks from task_queue, dispatches to handler functions,
marks complete or failed with retry. Runs as a scheduler-registered
daemon thread inside the web process.

Task types handled:
    submit_rfq, trigger_price_check, update_order_status,
    mark_order_shipped, price_rfq, run_connector
"""
import json
import logging
import threading
import time

log = logging.getLogger("reytech.task_consumer")


# ── Handler functions ─────────────────────────────────────────────────────────

def _handle_submit_rfq(payload: dict) -> dict:
    """Create a new RFQ from webhook payload."""
    from src.core.dal import save_rfq
    rfq_id = payload.get("rfq_id", payload.get("id", ""))
    save_rfq(payload, actor="task_queue")
    return {"rfq_id": rfq_id}


def _handle_trigger_price_check(payload: dict) -> dict:
    """Trigger enrichment pipeline for a price check."""
    pc_id = payload.get("pc_id")
    if not pc_id:
        raise ValueError("trigger_price_check requires pc_id in payload")
    from src.agents.pc_enrichment_pipeline import enrich_pc
    enrich_pc(pc_id, force=payload.get("force", False))
    return {"pc_id": pc_id, "status": "enrichment_started"}


def _handle_update_order_status(payload: dict) -> dict:
    """Update order status via DAL."""
    order_id = payload.get("order_id")
    status = payload.get("status")
    if not order_id or not status:
        raise ValueError("update_order_status requires order_id and status")
    from src.core.dal import update_order_status
    update_order_status(order_id, status, actor=payload.get("actor", "task_queue"))
    return {"order_id": order_id, "status": status}


def _handle_mark_order_shipped(payload: dict) -> dict:
    """Mark order as shipped."""
    order_id = payload.get("order_id")
    if not order_id:
        raise ValueError("mark_order_shipped requires order_id")
    from src.core.dal import update_order_status
    update_order_status(order_id, "shipped", actor="task_queue")
    return {"order_id": order_id, "status": "shipped"}


def _handle_price_rfq(payload: dict) -> dict:
    """Run pricing/enrichment pipeline for an RFQ."""
    rfq_id = payload.get("rfq_id")
    if not rfq_id:
        raise ValueError("price_rfq requires rfq_id")
    from src.agents.pc_enrichment_pipeline import enrich_pc
    enrich_pc(rfq_id, force=payload.get("force", False))
    return {"rfq_id": rfq_id, "status": "pricing_started"}


def _handle_run_connector(payload: dict) -> dict:
    """Run a data connector pull (SCPRS, supplier, etc.)."""
    connector_id = payload.get("connector_id")
    if not connector_id:
        raise ValueError("run_connector requires connector_id")
    from src.core.pull_orchestrator import PullOrchestrator
    result = PullOrchestrator().run_connector(connector_id)
    return result


TASK_HANDLERS = {
    "submit_rfq": _handle_submit_rfq,
    "trigger_price_check": _handle_trigger_price_check,
    "update_order_status": _handle_update_order_status,
    "mark_order_shipped": _handle_mark_order_shipped,
    "price_rfq": _handle_price_rfq,
    "run_connector": _handle_run_connector,
}


# ── Dispatch ──────────────────────────────────────────────────────────────────

def _dispatch(task: dict) -> dict:
    """Route a dequeued task to its handler. Returns result dict."""
    task_type = task.get("task_type", "")
    handler = TASK_HANDLERS.get(task_type)
    if not handler:
        raise ValueError(f"Unknown task type: {task_type}")

    payload_raw = task.get("payload", "{}")
    if isinstance(payload_raw, str):
        payload = json.loads(payload_raw)
    else:
        payload = payload_raw or {}

    return handler(payload)


# ── Consumer loop ─────────────────────────────────────────────────────────────

def _consumer_loop(poll_interval: int):
    """Main consumer loop. Runs until shutdown requested."""
    from src.core.scheduler import should_run, heartbeat
    from src.core.task_queue import dequeue, complete, fail

    log.info("Task consumer started (poll every %ds)", poll_interval)

    while should_run():
        try:
            task = dequeue()
            if task is None:
                heartbeat("task-consumer", success=True)
                time.sleep(poll_interval)
                continue

            task_id = task["id"]
            task_type = task.get("task_type", "?")

            try:
                from src.core.structured_log import log_event
            except ImportError:
                log_event = None

            if log_event:
                log_event(log, "info", "task_dequeued",
                          task_id=task_id, task_type=task_type)
            else:
                log.info("Task dequeued: id=%d type=%s", task_id, task_type)

            try:
                result = _dispatch(task)
                complete(task_id, result)
                if log_event:
                    log_event(log, "info", "task_completed",
                              task_id=task_id, task_type=task_type)
                else:
                    log.info("Task completed: id=%d type=%s", task_id, task_type)
            except Exception as e:
                log.error("Task failed: id=%d type=%s error=%s",
                          task_id, task_type, str(e)[:200], exc_info=True)
                fail(task_id, f"{type(e).__name__}: {str(e)[:200]}")
                if log_event:
                    log_event(log, "error", "task_failed",
                              task_id=task_id, task_type=task_type,
                              error=str(e)[:200])

            heartbeat("task-consumer", success=True)

        except Exception as e:
            log.error("Consumer loop error: %s", e, exc_info=True)
            heartbeat("task-consumer", success=False, error=str(e)[:200])
            time.sleep(poll_interval)

    log.info("Task consumer stopped (shutdown requested)")


def start_task_consumer(poll_interval: int = 10):
    """Start the task consumer as a daemon thread, registered with scheduler."""
    from src.core.scheduler import register_job, mark_started

    register_job("task-consumer", interval_sec=poll_interval)

    t = threading.Thread(
        target=_consumer_loop,
        args=(poll_interval,),
        name="task-consumer",
        daemon=True,
    )
    t.start()
    mark_started("task-consumer", t)
    log.info("Task consumer thread started")
