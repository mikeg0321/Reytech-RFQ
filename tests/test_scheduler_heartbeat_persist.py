"""scheduler.heartbeat() must durably mirror into scheduler_heartbeats.

Root cause (prod, 2026-05-28): qa_heartbeat's check_scheduler_heartbeats
read the scheduler_heartbeats table and reported "fail: 0 jobs
heartbeating (need >=1)" — keeping the qa-heartbeat agent perpetually
"error" — even though ~20 jobs were alive and reporting to the
IN-MEMORY scheduler registry (/api/v1/health showed them all ok). The
table had NO writer: heartbeat() only updated the in-memory JobInfo,
which is per-process and lost on restart. award_tracker's staleness
cross-check read the same empty table.

Fix: heartbeat() now upserts a durable row per job_name. These tests
pin that the write happens, carries the right status, and that the QA
check stops failing once jobs heartbeat.

The scheduler_heartbeats table lives in the numbered-migration system
(migration 5), not db.py SCHEMA, so the test creates it explicitly on
the isolated test DB — mirroring what run_migrations() does on prod.
"""
import importlib

import pytest

# migration 5 DDL — keep in sync with src/core/migrations.py migration (5,)
_SCHEDULER_HEARTBEATS_DDL = """
    CREATE TABLE IF NOT EXISTS scheduler_heartbeats (
        job_name TEXT PRIMARY KEY,
        last_heartbeat TEXT,
        interval_sec INTEGER,
        status TEXT DEFAULT 'ok'
    );
"""


@pytest.fixture
def hb_db(temp_data_dir):
    """Ensure the scheduler_heartbeats table exists on the isolated test DB."""
    from src.core.db import get_db
    with get_db() as conn:
        conn.executescript(_SCHEDULER_HEARTBEATS_DDL)
    return temp_data_dir


def _row(job_name):
    from src.core.db import get_db
    with get_db() as conn:
        return conn.execute(
            "SELECT job_name, last_heartbeat, interval_sec, status "
            "FROM scheduler_heartbeats WHERE job_name=?",
            (job_name,),
        ).fetchone()


def test_heartbeat_persists_row(hb_db):
    sched = importlib.import_module("src.core.scheduler")
    sched.register_job("hb-test-ok", interval_sec=900)
    sched.heartbeat("hb-test-ok", success=True)

    row = _row("hb-test-ok")
    assert row is not None, "heartbeat() did not persist a scheduler_heartbeats row"
    assert row[0] == "hb-test-ok"
    assert row[1], "last_heartbeat not set"
    assert row[2] == 900, "interval_sec not mirrored"
    assert row[3] == "ok"


def test_heartbeat_failure_status(hb_db):
    sched = importlib.import_module("src.core.scheduler")
    sched.heartbeat("hb-test-fail", success=False, error="boom")
    row = _row("hb-test-fail")
    assert row is not None
    assert row[3] == "error"


def test_heartbeat_upserts_not_duplicates(hb_db):
    sched = importlib.import_module("src.core.scheduler")
    sched.heartbeat("hb-test-upsert", success=True)
    sched.heartbeat("hb-test-upsert", success=True)
    sched.heartbeat("hb-test-upsert", success=True)
    from src.core.db import get_db
    with get_db() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM scheduler_heartbeats WHERE job_name=?",
            ("hb-test-upsert",),
        ).fetchone()[0]
    assert n == 1, f"expected one upserted row, got {n}"


def test_qa_check_passes_once_jobs_heartbeat(hb_db):
    """The check that was failing on prod: with >=3 jobs heartbeating in
    the last 6h it must no longer report 'fail'."""
    sched = importlib.import_module("src.core.scheduler")
    for j in ("hb-job-a", "hb-job-b", "hb-job-c"):
        sched.register_job(j, interval_sec=900)
        sched.heartbeat(j, success=True)

    from src.core.qa_heartbeat import check_scheduler_heartbeats
    from src.core.db import get_db
    with get_db() as conn:
        result = check_scheduler_heartbeats(conn)
    assert result["status"] != "fail", (
        f"qa check still failing after 3 jobs heartbeat: {result.get('message')}"
    )
    assert result["value"]["jobs_in_last_6h"] >= 3
