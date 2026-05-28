"""Async generate-package backend pins.

The 105s ``POST /rfq/<id>/generate-package`` route exceeds Railway's
edge proxy timeout (~100s), causing operators to see ERR_CONNECTION_RESET
even when the server kept generating successfully (Coleman 10842771
2026-05-28 incident — see ``[[long-running-post-must-be-background]]``).

This PR adds an Accept-header-driven async dispatch:

  * Browser POSTs (Accept: text/html) → run sync, return 302 redirect
    to /review-package.  **Backwards-compatible — the UI still works
    without changes.**
  * AJAX POSTs (Accept: application/json) → enqueue via task_queue,
    return 202 + {job_id, status_url}.
  * GET /api/jobs/<id> → return current task state for polling.
  * The X-Async-Worker:1 header (set by the worker on its synthetic
    test_request_context call) prevents the handler from re-entering
    its own async branch — infinite-loop guard.

These tests pin the wiring (dispatch + status endpoint + handler
registration). They do NOT exercise the 1800-LOC heavy work — that's
covered by the existing route tests + prod regenerate verification.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.core import task_queue


# ── Test fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def init_queue():
    """Ensure the task_queue table exists in the test DB."""
    task_queue.init_task_queue()
    yield


# ── Dispatch + accept-header pins ─────────────────────────────────────


def test_browser_post_does_not_enqueue(auth_client, seed_rfq, init_queue):
    """Sync path floor — POSTs without Accept: application/json must NOT
    return 202. Backwards-compat with current operator workflow.
    """
    rfq_id = seed_rfq
    stats_before = task_queue.get_queue_stats()
    pending_before = stats_before.get("pending", 0)

    resp = auth_client.post(
        f"/rfq/{rfq_id}/generate-package?force=1",
        data={},
        # No Accept header set → Flask defaults to */* which prefers HTML
    )
    # Browser POST hits sync path. The heavy work may fail in the test
    # env (no real templates, no Gmail mock here, etc.) so we don't
    # assert success — we just assert "not 202" (= didn't enqueue).
    assert resp.status_code != 202, (
        f"Browser POST must NOT enqueue (was 202 with body {resp.data[:200]!r}). "
        f"Sync path is the backwards-compatible default."
    )

    # And the queue size did not grow from a generate_package row.
    stats_after = task_queue.get_queue_stats()
    pending_after = stats_after.get("pending", 0)
    assert pending_after == pending_before, (
        f"Browser POST grew pending queue from {pending_before} to "
        f"{pending_after} — sync path must not enqueue."
    )


def test_json_accept_enqueues_and_returns_202(auth_client, seed_rfq, init_queue):
    """Async path floor — POST with ``Accept: application/json`` enqueues
    a ``generate_package`` task and returns 202 with a poll URL.
    """
    rfq_id = seed_rfq
    resp = auth_client.post(
        f"/rfq/{rfq_id}/generate-package?force=1",
        data={},
        headers={"Accept": "application/json"},
    )
    assert resp.status_code == 202, (
        f"Expected 202 Accepted, got {resp.status_code}: {resp.data[:300]!r}"
    )
    body = resp.get_json()
    assert body is not None and body.get("ok") is True
    assert isinstance(body.get("job_id"), int) and body["job_id"] > 0
    assert body["status_url"] == f"/api/jobs/{body['job_id']}", (
        f"status_url shape mismatch: {body['status_url']!r}"
    )

    # And the task is actually in the queue with the right type + rfq_id.
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT task_type, status, payload FROM task_queue WHERE id = ?",
        (body["job_id"],),
    ).fetchone()
    conn.close()
    assert row is not None
    assert row["task_type"] == "generate_package"
    assert row["status"] in ("pending", "running")
    import json as _json
    payload = _json.loads(row["payload"])
    assert payload["rfq_id"] == rfq_id
    assert payload["force"] is True


def test_x_async_worker_header_breaks_recursion(auth_client, seed_rfq, init_queue):
    """Re-entry guard — if the worker re-invokes the route with
    ``X-Async-Worker: 1`` (which the handler shim sets), the async
    branch must NOT fire. Otherwise the worker enqueues itself in an
    infinite loop.
    """
    rfq_id = seed_rfq
    stats_before = task_queue.get_queue_stats()
    pending_before = stats_before.get("pending", 0)

    resp = auth_client.post(
        f"/rfq/{rfq_id}/generate-package?force=1",
        data={},
        headers={"Accept": "application/json", "X-Async-Worker": "1"},
    )
    # MUST NOT be 202 — the worker header bypasses the async branch.
    # Falls through to the sync heavy work (which may 500 in test env,
    # that's OK — we only care that it didn't enqueue).
    assert resp.status_code != 202, (
        f"X-Async-Worker header MUST prevent enqueue, got 202: "
        f"{resp.data[:200]!r}. Infinite-loop guard regressed."
    )

    stats_after = task_queue.get_queue_stats()
    pending_after = stats_after.get("pending", 0)
    assert pending_after == pending_before, (
        f"Worker-header POST enqueued anyway (pending {pending_before} "
        f"→ {pending_after}). The handler shim would loop forever."
    )


def test_unpriced_gate_still_fires_before_async_dispatch(auth_client, sample_rfq, temp_data_dir, init_queue):
    """The unpriced-rows gate (cheaper UX feedback than running the
    80-second generation) must fire BEFORE the async dispatch.
    Otherwise async callers could enqueue a job that the gate would
    have refused, paying the round-trip latency for nothing.
    """
    import os
    import json
    # Strip prices off the sample RFQ so the gate fires.
    rfq = dict(sample_rfq)
    items = []
    for item in rfq.get("line_items", []):
        unpriced = dict(item)
        unpriced["price_per_unit"] = 0
        unpriced["unit_price"] = 0
        items.append(unpriced)
    rfq["line_items"] = items
    with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
        json.dump({rfq["id"]: rfq}, f)

    stats_before = task_queue.get_queue_stats()
    pending_before = stats_before.get("pending", 0)

    resp = auth_client.post(
        f"/rfq/{rfq['id']}/generate-package",  # no force=1 → gate active
        data={},
        headers={"Accept": "application/json"},
    )
    # Gate redirects to /rfq/<id> with a flash. Should be 302, NOT 202.
    assert resp.status_code != 202, (
        f"Unpriced gate must run before async dispatch — got 202: "
        f"{resp.data[:200]!r}"
    )

    stats_after = task_queue.get_queue_stats()
    pending_after = stats_after.get("pending", 0)
    assert pending_after == pending_before, (
        f"Unpriced gate refused but a generate_package row was still "
        f"enqueued ({pending_before} → {pending_after})."
    )


# ── Job-status endpoint pins ──────────────────────────────────────────


def test_job_status_404_for_unknown_id(auth_client, init_queue):
    """GET /api/jobs/<id> on a nonexistent task returns 404."""
    resp = auth_client.get("/api/jobs/999999999")
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["ok"] is False
    assert "not found" in body["error"].lower()


def test_job_status_returns_pending(auth_client, init_queue):
    """GET /api/jobs/<id> on a freshly enqueued task returns status=pending
    + the full envelope shape the UI polls.
    """
    task_id = task_queue.enqueue(
        "generate_package",
        {"rfq_id": "test-rfq-001", "form_data": {}, "force": True},
        actor="test",
    )
    resp = auth_client.get(f"/api/jobs/{task_id}")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["task_id"] == task_id
    assert body["task_type"] == "generate_package"
    assert body["status"] == "pending"
    assert body["started_at"] is None
    assert body["finished_at"] is None
    assert body["result"] is None
    assert body["error"] is None


def test_job_status_returns_completed_with_result(auth_client, init_queue):
    """When the worker completes a task, /api/jobs/<id> surfaces the
    handler's result payload — what the UI uses to redirect operators
    to the review-package page.
    """
    task_id = task_queue.enqueue(
        "generate_package",
        {"rfq_id": "test-rfq-001", "form_data": {}, "force": True},
        actor="test",
    )
    # Simulate the worker dequeueing + completing
    claimed = task_queue.dequeue()
    assert claimed is not None and claimed["id"] == task_id
    task_queue.complete(task_id, {
        "redirect": "/rfq/test-rfq-001/review-package",
        "messages": [{"category": "info", "text": "Package ready"}],
        "rfq_id": "test-rfq-001",
    })

    resp = auth_client.get(f"/api/jobs/{task_id}")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "completed"
    assert body["finished_at"] is not None
    assert body["result"]["redirect"] == "/rfq/test-rfq-001/review-package"
    assert body["result"]["messages"][0]["text"] == "Package ready"


def test_job_status_returns_failed_with_error(auth_client, init_queue):
    """When a worker fails a task past max_retries, /api/jobs/<id>
    surfaces the error so the UI can show it to the operator.
    """
    task_id = task_queue.enqueue(
        "generate_package",
        {"rfq_id": "test-rfq-001", "form_data": {}, "force": True},
        actor="test",
        max_retries=0,  # fail immediately
    )
    task_queue.dequeue()
    task_queue.fail(task_id, "ValueError: synthetic failure for test")

    resp = auth_client.get(f"/api/jobs/{task_id}")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "failed"
    assert "synthetic failure" in body["error"]


# ── Handler registration pin ──────────────────────────────────────────


def test_generate_package_handler_registered():
    """``generate_package`` must be in TASK_HANDLERS — otherwise the
    consumer raises ``Unknown task type`` and the task fails forever.
    Pinning this catches a class of bug where a route enqueues a
    task_type that has no handler (silent prod break).
    """
    from src.core.task_consumer import TASK_HANDLERS
    assert "generate_package" in TASK_HANDLERS, (
        f"'generate_package' missing from TASK_HANDLERS — async route "
        f"will enqueue tasks that no consumer can dispatch. "
        f"Got types: {sorted(TASK_HANDLERS.keys())}"
    )
    handler = TASK_HANDLERS["generate_package"]
    assert callable(handler)


def test_generate_package_handler_validates_rfq_id():
    """The handler must reject payloads missing rfq_id (otherwise the
    test_request_context call below would build a URL with 'None' in
    the path and the route would 404 noisily — better to fail upfront).
    """
    from src.core.task_consumer import _handle_generate_package
    with pytest.raises(ValueError, match="rfq_id"):
        _handle_generate_package({"form_data": {}, "force": False})


def test_generate_package_handler_actually_resolves_flask_app(app, auth_client, seed_rfq, init_queue):
    """End-to-end-shape pin — the handler must successfully resolve the
    Flask app at dispatch time and build a test_request_context.

    PR #1182 originally imported ``from src.api.dashboard import app``
    which crashed in prod with ``ImportError: cannot import name 'app'
    from 'src.api.dashboard'`` the first time an async caller arrived
    — but the earlier unit tests only verified handler REGISTRATION,
    not handler EXECUTION. This test invokes the handler directly so
    the next time someone breaks the app-resolution path, CI catches
    it before deploy.

    Uses the test client's auth_client to seed the Flask app context;
    then registers the test app instance with the task_consumer
    (mirroring what app.py:start_task_consumer(..., app=app) does in
    prod) and invokes the handler with a minimal valid payload.
    """
    from src.core import task_consumer

    # Mirror the prod startup contract — register the live Flask app.
    # The `app` fixture is the Flask app instance the test client was
    # built against; equivalent to what app.py passes via
    # start_task_consumer(app=app) in prod.
    task_consumer._FLASK_APP = app
    try:
        # Minimal payload; force=1 to bypass the unpriced-rows gate.
        # The handler will call into generate_rfq_package which is
        # heavy — but for THIS test we only care that:
        #   * the handler can resolve the Flask app
        #   * it can build a test_request_context without ImportError
        #   * it returns a dict shaped {redirect, messages, rfq_id}
        # We accept any non-exception return as a pass. The Coleman
        # E2E pin still runs in prod against the real package output.
        result = task_consumer._handle_generate_package({
            "rfq_id": seed_rfq,
            "form_data": {},
            "force": True,
            "actor": "test",
        })
        assert isinstance(result, dict)
        assert result.get("rfq_id") == seed_rfq
        # `redirect` and `messages` keys must exist (UI poll contract).
        assert "redirect" in result
        assert "messages" in result
    finally:
        task_consumer._FLASK_APP = None


# ── Frontend wiring pins (rfq_detail.html) ────────────────────────────
#
# The backend async path (above) shipped in PR #1182/#1186, but the UI
# still POSTed the form synchronously — so operators kept hitting the
# 105s ERR_CONNECTION_RESET on large packages. This block pins the
# frontend half: both generate triggers must go through the async
# fetch/poll helper, and the old synchronous `form.submit()` to
# generate-package must be gone. A revert to sync would re-open the
# Coleman 10842771 incident, so these guard it loudly.


def _rfq_detail_src() -> str:
    import os
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(here, "src", "templates", "rfq_detail.html")
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_frontend_generate_triggers_are_async():
    """Both generate entry points (_doGenerate + regeneratePackage) must
    route through the async helper, not submit the form synchronously."""
    src = _rfq_detail_src()
    # The async helper exists and both triggers call it.
    assert "function _generatePackageAsync(" in src
    assert src.count("_generatePackageAsync()") >= 2, (
        "Both _doGenerate() and regeneratePackage() must call "
        "_generatePackageAsync()."
    )
    # The poll/complete/overlay helpers are present.
    for fn in ("_pollGenJob", "_onGenComplete", "_showGenOverlay",
               "_setGenOverlayHint", "_genOverlayError"):
        assert f"function {fn}(" in src, f"missing helper {fn}"


def test_frontend_no_sync_generate_package_submit():
    """The old synchronous redirect-after-POST to generate-package is the
    bug. It must not reappear: no code path may set form.action to
    generate-package and call .submit()."""
    src = _rfq_detail_src()
    assert "f.action='/rfq/'+RID+'/generate-package'" not in src, (
        "Sync _doGenerate path regressed — re-opens the 105s "
        "ERR_CONNECTION_RESET incident (Coleman 10842771)."
    )
    assert "form.action='/rfq/'+RID+'/generate-package'" not in src, (
        "Sync regeneratePackage path regressed — re-opens the 105s "
        "ERR_CONNECTION_RESET incident (Coleman 10842771)."
    )


def test_frontend_async_request_matches_backend_contract():
    """The fetch must send Accept: application/json (the header the route
    keys its 202 dispatch on) and poll the status_url the route returns."""
    src = _rfq_detail_src()
    assert "'Accept':'application/json'" in src, (
        "Async POST must send Accept: application/json — the route's "
        "async branch keys on best_match(...) == 'application/json'."
    )
    # Polls the status endpoint the 202 hands back.
    assert "/api/jobs/" in src
    assert "_pollGenJob(d.status_url" in src
