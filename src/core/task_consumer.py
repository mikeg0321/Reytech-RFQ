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


# Reference to the Flask app instance, set by start_task_consumer().
# Used by handlers (e.g. _handle_generate_package) that need to build a
# test_request_context to re-invoke a Flask route in the worker thread.
# Set explicitly at consumer-start time so handlers don't need to import
# from `app.py` (which would either deadlock during create_app() or
# instantiate a second Flask app if the import happens later).
_FLASK_APP = None


# ── Handler functions ─────────────────────────────────────────────────────────

def _handle_submit_rfq(payload: dict) -> dict:
    """Create a new RFQ from webhook payload."""
    rfq_id = payload.get("rfq_id", payload.get("id", ""))
    if not rfq_id:
        raise ValueError("submit_rfq payload missing rfq_id/id")
    # Canonical writer: full 22-col shape + cache sync. The legacy
    # core.dal.save_rfq stub was deleted 2026-04-30 (V1 DAL audit drift #1).
    from src.api.data_layer import _save_single_rfq
    payload["id"] = rfq_id
    _save_single_rfq(rfq_id, payload)
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


def _handle_generate_package(payload: dict) -> dict:
    """Run the heavy RFQ package generation work in the background.

    Re-invokes ``generate_rfq_package`` via a Flask ``test_request_context``
    so the existing 1800-LOC route handler runs unchanged. The
    ``X-Async-Worker: 1`` header on the synthetic request prevents the
    route's Accept-header async branch from re-enqueueing (infinite-loop
    guard). Decorators are peeled because the enqueuer already
    authenticated when accepting the original POST.

    Closes the long-running-post gap for the 105s
    ``POST /rfq/<id>/generate-package`` route (Coleman 10842771
    2026-05-28 incident — browser saw ERR_CONNECTION_RESET at Railway
    edge proxy ~100s while server kept generating).

    Returns:
        Dict with ``redirect`` (where the operator should go), ``messages``
        (list of ``{category, text}`` flashes captured from the route),
        and ``rfq_id``. The frontend polls ``/api/jobs/<id>`` until status
        is ``completed`` then navigates to ``redirect``.
    """
    rid = payload.get("rfq_id")
    if not rid:
        raise ValueError("generate_package payload missing rfq_id")
    form_data = payload.get("form_data", {})
    force = bool(payload.get("force", False))

    # Use the Flask app set by start_task_consumer(). Falls back to
    # `flask.current_app` if a request context happens to be active
    # (e.g. test cases that enqueue + dispatch within the same request).
    # PR #1182 originally imported `from src.api.dashboard import app` —
    # that symbol doesn't exist in dashboard.py (the Flask app lives at
    # top-level `app.py` after `create_app()`). The import failed in
    # prod the moment any async caller hit `/api/jobs/<id>` polling.
    app = _FLASK_APP
    if app is None:
        try:
            from flask import current_app
            app = current_app._get_current_object()
        except RuntimeError:
            raise RuntimeError(
                "task_consumer Flask app not initialized — call "
                "start_task_consumer(app=...) at startup or invoke this "
                "handler from within a Flask request context."
            )
    from src.api.modules.routes_rfq_gen import generate_rfq_package

    # Peel @auth_required + @safe_route so we bypass auth (already
    # authenticated upstream) AND surface exceptions to task_queue's
    # retry/fail logic instead of being swallowed by safe_route's
    # JSON-error wrapper.
    inner = generate_rfq_package
    while hasattr(inner, "__wrapped__"):
        inner = inner.__wrapped__

    query_string = "force=1" if force else ""
    with app.test_request_context(
        path=f"/rfq/{rid}/generate-package",
        method="POST",
        data=form_data,
        query_string=query_string,
        headers={"X-Async-Worker": "1", "Accept": "text/html"},
    ):
        # Make the actor available for downstream audit/trace.
        from flask import g
        g.user = payload.get("actor", "task_queue_worker")

        response = inner(rid)

        # Capture flashes that the inner route emitted so the operator
        # can see "Package ready" / "Unpriced rows" / etc.
        from flask import get_flashed_messages
        messages = list(get_flashed_messages(with_categories=True))

        # Extract the redirect URL — the inner route returns a 302
        # Response on success (review-package) or on the unpriced-gate
        # bounce (back to the RFQ page).
        redirect_url = None
        if hasattr(response, "location") and response.location:
            redirect_url = response.location

    return {
        "redirect": redirect_url,
        "messages": [{"category": cat, "text": txt} for cat, txt in messages],
        "rfq_id": rid,
    }


TASK_HANDLERS = {
    "submit_rfq": _handle_submit_rfq,
    "trigger_price_check": _handle_trigger_price_check,
    "update_order_status": _handle_update_order_status,
    "mark_order_shipped": _handle_mark_order_shipped,
    "price_rfq": _handle_price_rfq,
    "run_connector": _handle_run_connector,
    "generate_package": _handle_generate_package,
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


def start_task_consumer(poll_interval: int = 10, app=None):
    """Start the task consumer as a daemon thread, registered with scheduler.

    Args:
        poll_interval: seconds between queue polls.
        app: optional Flask app instance. Stored at module level for
            handlers (e.g. ``_handle_generate_package``) that need to
            build a ``test_request_context`` to re-invoke a route in the
            worker thread. Pass the live ``create_app()`` instance here.
    """
    if app is not None:
        global _FLASK_APP
        _FLASK_APP = app
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
