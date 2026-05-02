"""PR-5: sent_at integrity sweep + health card.

The bug Mike caught 2026-05-02: every Sent-table row stamped "today"
because some writer was filling sent_at with created_at. PR-3's
canonical `is_real_sent` filters them at read time. PR-5 cleans them
up at the data layer + surfaces a count card so the operator sees
the cleanup volume.

Two layers of coverage:

  1. Sweep script: dry-run reports counts without mutation, --apply
     clears sent_at (sets to '') on bug-shape rows + rewrites the
     data_json blob so JSON read paths agree.

  2. Health card: /health/quoting renders a `sent_at_integrity` card
     with status 'healthy' (zero rows) or 'warn' (≥1 row).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


# ─── Helpers ─────────────────────────────────────────────────────────────


def _conn():
    from src.core.db import get_db
    return get_db()


def _seed_rfq(c, rid, status, created_at, sent_at, is_test=0):
    """Insert a minimal rfqs row.

    rfqs has no column-level `sent_at` or `created_at` — both live
    in data_json. The creation field is `received_at` at the column
    level. Mirror prod shape: write the column for received_at and
    everything sent_at-related into the blob.
    """
    blob = json.dumps({
        "id": rid, "status": status,
        "created_at": created_at,
        "received_at": created_at,
        "sent_at": sent_at,
        "rfq_number": rid,
    })
    c.execute(
        "INSERT INTO rfqs (id, status, received_at, data_json) "
        "VALUES (?,?,?,?)",
        (rid, status, created_at, blob),
    )


def _seed_pc(c, pid, status, created_at, sent_at, is_test=0):
    blob = json.dumps({
        "id": pid, "status": status,
        "created_at": created_at, "sent_at": sent_at,
        "pc_number": pid,
    })
    c.execute(
        "INSERT INTO price_checks (id, status, created_at, sent_at, "
        "data_json) VALUES (?,?,?,?,?)",
        (pid, status, created_at, sent_at, blob),
    )


def _seed_quote(c, qn, status, created_at, sent_at):
    """Quotes table doesn't have data_json."""
    c.execute(
        "INSERT INTO quotes (quote_number, status, created_at, sent_at, "
        "agency, total) VALUES (?,?,?,?,?,?)",
        (qn, status, created_at, sent_at, "CDCR", 100.0),
    )


def _wipe(c):
    for tbl in ("rfqs", "price_checks", "quotes"):
        try:
            c.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    c.commit()


# ─── Sweep script: dry-run reports without mutating ──────────────────────


def test_sweep_dry_run_reports_counts_no_mutation(temp_data_dir):
    """Default invocation must not mutate the DB."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    with _conn() as c:
        _wipe(c)
        # bug-shape rows
        _seed_rfq(c, "rfq-pr5-1", "sent",
                  "2026-04-01T10:00:00", "2026-04-01T10:00:00")
        _seed_pc(c, "pc-pr5-1", "sent",
                 "2026-04-02T10:00:00", "2026-04-02T10:00:00")
        # healthy row (sent_at after created_at)
        _seed_rfq(c, "rfq-pr5-ok", "sent",
                  "2026-04-01T10:00:00", "2026-04-01T11:00:00")
        c.commit()

    script = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "scripts",
        "sweep_sent_at_integrity.py")
    proc = subprocess.run(
        [sys.executable, script, "--db", db_path],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "rfqs: 1 bad rows" in proc.stdout
    assert "price_checks: 1 bad rows" in proc.stdout
    assert "total bad rows: 2" in proc.stdout

    # Verify nothing was actually changed (rfqs is blob-only).
    with _conn() as c:
        row = c.execute(
            "SELECT data_json FROM rfqs WHERE id='rfq-pr5-1'"
        ).fetchone()
        blob = json.loads(row["data_json"])
        assert blob.get("sent_at") == "2026-04-01T10:00:00", (
            "dry-run mutated the DB"
        )


def test_sweep_apply_clears_sent_at_on_bug_shape(temp_data_dir):
    """--apply must clear sent_at on bug-shape rows AND rewrite the
    data_json blob so JSON-read paths agree with the column."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-pr5-2", "sent",
                  "2026-04-15T08:00:00", "2026-04-15T08:00:00")
        _seed_rfq(c, "rfq-pr5-keep", "sent",
                  "2026-04-15T08:00:00", "2026-04-16T09:00:00")
        c.commit()

    script = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "scripts",
        "sweep_sent_at_integrity.py")
    proc = subprocess.run(
        [sys.executable, script, "--db", db_path, "--apply",
         "--actor", "test_actor"],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "cleared sent_at on 1 rows" in proc.stdout

    with _conn() as c:
        # rfqs: no column-level sent_at — verify the blob got cleared.
        bad = c.execute(
            "SELECT data_json FROM rfqs WHERE id='rfq-pr5-2'"
        ).fetchone()
        blob = json.loads(bad["data_json"])
        assert blob.get("sent_at") == "", "blob sent_at not cleared"
        assert blob.get("sent_at_swept_by") == "test_actor", (
            "audit stamp missing on swept row"
        )

        good = c.execute(
            "SELECT data_json FROM rfqs WHERE id='rfq-pr5-keep'"
        ).fetchone()
        good_blob = json.loads(good["data_json"])
        assert good_blob.get("sent_at") == "2026-04-16T09:00:00", (
            "healthy row was incorrectly cleared"
        )


def test_sweep_only_targets_status_sent(temp_data_dir):
    """Bug-shape only matters for status='sent' rows. Drafts where
    sent_at == created_at (probably both empty or initialized to
    creation) are out of scope — could be intentional fallback."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-pr5-draft", "draft",
                  "2026-04-01T10:00:00", "2026-04-01T10:00:00")
        c.commit()

    script = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "scripts",
        "sweep_sent_at_integrity.py")
    proc = subprocess.run(
        [sys.executable, script, "--db", db_path, "--apply"],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0
    assert "rfqs: 0 bad rows" in proc.stdout


def test_sweep_handles_missing_db_path():
    """Bad path → exit 2, no crash."""
    script = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "scripts",
        "sweep_sent_at_integrity.py")
    proc = subprocess.run(
        [sys.executable, script, "--db", "/tmp/no-such-file.db"],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 2


# ─── Health card: surfaces count + samples ───────────────────────────────


def test_health_card_returns_healthy_on_clean_db(temp_data_dir):
    """Empty DB → status='healthy', total_bad=0."""
    from src.api.modules.routes_health import _build_sent_at_integrity_card
    with _conn() as c:
        _wipe(c)
    card = _build_sent_at_integrity_card()
    assert card["status"] == "healthy"
    assert card["total_bad"] == 0
    assert card["samples"] == []


def test_health_card_returns_warn_when_bug_rows_exist(temp_data_dir):
    """Seed 1 bad rfq + 2 bad pcs → status='warn', counts split."""
    from src.api.modules.routes_health import _build_sent_at_integrity_card
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-card-1", "sent",
                  "2026-04-15T08:00:00", "2026-04-15T08:00:00")
        _seed_pc(c, "pc-card-1", "sent",
                 "2026-04-15T08:00:00", "2026-04-15T08:00:00")
        _seed_pc(c, "pc-card-2", "sent",
                 "2026-04-16T09:00:00", "2026-04-16T09:00:00")
        c.commit()
    card = _build_sent_at_integrity_card()
    assert card["status"] == "warn"
    assert card["total_bad"] == 3
    assert card["rfqs_bad"] == 1
    assert card["price_checks_bad"] == 2
    assert card["quotes_bad"] == 0
    # Samples populated (capped at 5 across tables)
    assert 1 <= len(card["samples"]) <= 5
    sample = card["samples"][0]
    assert sample["created_at"] == sample["sent_at"], (
        "sample must be a bug-shape row"
    )


def test_health_card_excludes_healthy_rows(temp_data_dir):
    """Healthy rows (sent_at != created_at) must NOT count."""
    from src.api.modules.routes_health import _build_sent_at_integrity_card
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-good", "sent",
                  "2026-04-15T08:00:00", "2026-04-16T09:00:00")
        c.commit()
    card = _build_sent_at_integrity_card()
    assert card["status"] == "healthy"
    assert card["total_bad"] == 0


def test_health_card_excludes_non_sent_rows(temp_data_dir):
    """Bug-shape only matters when status='sent'. A draft row with
    matching timestamps is not a sent-table pollution."""
    from src.api.modules.routes_health import _build_sent_at_integrity_card
    with _conn() as c:
        _wipe(c)
        _seed_rfq(c, "rfq-draft", "draft",
                  "2026-04-15T08:00:00", "2026-04-15T08:00:00")
        c.commit()
    card = _build_sent_at_integrity_card()
    assert card["status"] == "healthy"
    assert card["total_bad"] == 0
