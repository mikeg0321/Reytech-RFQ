"""
Tests for task queue consumer — Phase 2 of architecture gap fixes.

Tests dispatch routing, unknown task handling, reset_stale_running,
and end-to-end enqueue→dequeue→complete/fail flow.
"""
import json
import pytest
from unittest.mock import patch, MagicMock


class TestDispatch:
    """Test _dispatch routes to correct handlers."""

    def test_dispatch_known_type(self, temp_data_dir):
        """Known task type routes to handler."""
        from src.core.task_consumer import _dispatch, TASK_HANDLERS

        mock_handler = MagicMock(return_value={"ok": True})
        original = TASK_HANDLERS["update_order_status"]
        TASK_HANDLERS["update_order_status"] = mock_handler
        try:
            task = {
                "task_type": "update_order_status",
                "payload": json.dumps({"order_id": "ord-001", "status": "shipped"}),
            }
            result = _dispatch(task)
            mock_handler.assert_called_once_with({"order_id": "ord-001", "status": "shipped"})
            assert result == {"ok": True}
        finally:
            TASK_HANDLERS["update_order_status"] = original

    def test_dispatch_unknown_type_raises(self, temp_data_dir):
        """Unknown task type raises ValueError."""
        from src.core.task_consumer import _dispatch
        task = {"task_type": "nonexistent_task", "payload": "{}"}
        with pytest.raises(ValueError, match="Unknown task type"):
            _dispatch(task)

    def test_dispatch_all_known_types(self, temp_data_dir):
        """Every registered task type has a handler."""
        from src.core.task_consumer import TASK_HANDLERS
        expected = {
            "submit_rfq", "trigger_price_check", "update_order_status",
            "mark_order_shipped", "price_rfq", "run_connector",
        }
        assert set(TASK_HANDLERS.keys()) == expected

    def test_dispatch_string_payload(self, temp_data_dir):
        """String payload is JSON-parsed."""
        from src.core.task_consumer import _dispatch, TASK_HANDLERS

        mock_handler = MagicMock(return_value={"ok": True})
        original = TASK_HANDLERS["mark_order_shipped"]
        TASK_HANDLERS["mark_order_shipped"] = mock_handler
        try:
            task = {
                "task_type": "mark_order_shipped",
                "payload": '{"order_id": "ord-002"}',
            }
            _dispatch(task)
            mock_handler.assert_called_once_with({"order_id": "ord-002"})
        finally:
            TASK_HANDLERS["mark_order_shipped"] = original

    def test_dispatch_dict_payload(self, temp_data_dir):
        """Dict payload is passed through."""
        from src.core.task_consumer import _dispatch, TASK_HANDLERS

        mock_handler = MagicMock(return_value={"ok": True})
        original = TASK_HANDLERS["mark_order_shipped"]
        TASK_HANDLERS["mark_order_shipped"] = mock_handler
        try:
            task = {
                "task_type": "mark_order_shipped",
                "payload": {"order_id": "ord-003"},
            }
            _dispatch(task)
            mock_handler.assert_called_once_with({"order_id": "ord-003"})
        finally:
            TASK_HANDLERS["mark_order_shipped"] = original


class TestResetStaleRunning:
    """Test reset_stale_running() recovers crashed tasks."""

    def test_resets_running_to_pending(self, temp_data_dir):
        """Tasks stuck in 'running' are reset to 'pending'."""
        from src.core.task_queue import init_task_queue, enqueue, reset_stale_running, _get_db

        init_task_queue()
        task_id = enqueue("test_task", {"foo": "bar"})

        # Manually set to running with old started_at
        conn = _get_db()
        conn.execute(
            "UPDATE task_queue SET status='running', started_at=datetime('now', '-60 minutes') "
            "WHERE id=?", (task_id,))
        conn.commit()
        conn.close()

        count = reset_stale_running(max_age_minutes=30)
        assert count == 1

        conn = _get_db()
        row = conn.execute("SELECT status, error FROM task_queue WHERE id=?",
                           (task_id,)).fetchone()
        conn.close()
        assert row["status"] == "pending"
        assert row["error"] == "reset on boot"

    def test_leaves_recent_running_alone(self, temp_data_dir):
        """Tasks that just started running should not be reset."""
        from src.core.task_queue import init_task_queue, enqueue, reset_stale_running, _get_db

        init_task_queue()
        task_id = enqueue("test_task", {})

        # Set to running just now
        conn = _get_db()
        conn.execute(
            "UPDATE task_queue SET status='running', started_at=datetime('now') WHERE id=?",
            (task_id,))
        conn.commit()
        conn.close()

        count = reset_stale_running(max_age_minutes=30)
        assert count == 0


class TestEndToEnd:
    """Test full enqueue → dequeue → complete/fail cycle."""

    def test_enqueue_dequeue_complete(self, temp_data_dir):
        """Task flows through queue lifecycle."""
        from src.core.task_queue import init_task_queue, enqueue, dequeue, complete, _get_db

        init_task_queue()
        task_id = enqueue("test_task", {"key": "value"}, actor="test")

        task = dequeue()
        assert task is not None
        assert task["id"] == task_id
        assert task["task_type"] == "test_task"
        # dequeue() returns the row as fetched (before UPDATE), verify DB state
        conn = _get_db()
        row = conn.execute("SELECT status FROM task_queue WHERE id=?", (task_id,)).fetchone()
        conn.close()
        assert row["status"] == "running"

        complete(task_id, {"result": "done"})

        conn = _get_db()
        row = conn.execute("SELECT status, result FROM task_queue WHERE id=?",
                           (task_id,)).fetchone()
        conn.close()
        assert row["status"] == "completed"
        assert json.loads(row["result"]) == {"result": "done"}

    def test_fail_retries_then_fails(self, temp_data_dir):
        """Task retries up to max_retries, then stays failed."""
        from src.core.task_queue import init_task_queue, enqueue, dequeue, fail, _get_db

        init_task_queue()
        task_id = enqueue("test_task", {}, max_retries=2)

        # First failure — should go back to pending
        dequeue()
        fail(task_id, "attempt 1")
        conn = _get_db()
        row = conn.execute("SELECT status, retries FROM task_queue WHERE id=?",
                           (task_id,)).fetchone()
        conn.close()
        assert row["status"] == "pending"
        assert row["retries"] == 1

        # Second failure — should go back to pending
        dequeue()
        fail(task_id, "attempt 2")
        conn = _get_db()
        row = conn.execute("SELECT status, retries FROM task_queue WHERE id=?",
                           (task_id,)).fetchone()
        conn.close()
        assert row["status"] == "pending"
        assert row["retries"] == 2

        # Third failure — should stay failed (max_retries=2, retries=2)
        dequeue()
        fail(task_id, "attempt 3")
        conn = _get_db()
        row = conn.execute("SELECT status FROM task_queue WHERE id=?",
                           (task_id,)).fetchone()
        conn.close()
        assert row["status"] == "failed"

    def test_empty_queue_returns_none(self, temp_data_dir):
        """Dequeue on empty queue returns None."""
        from src.core.task_queue import init_task_queue, dequeue

        init_task_queue()
        assert dequeue() is None

    def test_queue_stats(self, temp_data_dir):
        """get_queue_stats returns correct counts."""
        from src.core.task_queue import init_task_queue, enqueue, dequeue, complete, get_queue_stats

        init_task_queue()
        enqueue("a", {})
        enqueue("b", {})
        enqueue("c", {})

        task = dequeue()
        complete(task["id"], {})

        stats = get_queue_stats()
        assert stats["pending"] == 2
        assert stats["completed"] == 1
        assert stats["total"] == 3
