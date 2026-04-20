"""Contract tests for scripts/backfill_wins_from_orders.py.

The script exists because prod oracle_calibration had 0 wins recorded
vs. 47 losses — the `orders` table carries real shipped POs that never
reached Oracle. Locking the script's contract so a future refactor
doesn't break the backfill:

  - Reads shipped/delivered/invoiced rows from `orders`
  - Skips rows it already processed (ledger table prevents double-count)
  - Calls calibrate_from_outcome(items, 'won', agency=...) per order
  - Tolerates items without cost fields (win_count still increments)
  - Dry-run mode must never write
  - Missing DB returns exit 2 (same contract as db_bloat_diagnostic.py)
"""
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def backfill_mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "backfill_wins_from_orders" in sys.modules:
        del sys.modules["backfill_wins_from_orders"]
    return importlib.import_module("backfill_wins_from_orders")


def _seed_orders_table(db_path, rows):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE orders (
            id TEXT PRIMARY KEY, quote_number TEXT, agency TEXT,
            institution TEXT, po_number TEXT, status TEXT,
            total REAL, items TEXT, created_at TEXT
        )
    """)
    for r in rows:
        conn.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (r["id"], r.get("quote_number", ""), r.get("agency", ""),
             r.get("institution", ""), r.get("po_number", ""),
             r["status"], r.get("total", 0.0),
             json.dumps(r.get("items", [])), r.get("created_at", "2026-04-01"))
        )
    conn.commit()
    conn.close()


def test_missing_db_returns_exit_2(backfill_mod, capsys):
    rc = backfill_mod.run("/nonexistent/path/to/reytech.db", dry_run=True)
    assert rc == 2


def test_dry_run_does_not_touch_calibrate(backfill_mod, tmp_path):
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [{
        "id": "ORD-1", "agency": "CDCR", "po_number": "PO1",
        "status": "shipped", "total": 100.0,
        "items": [{"description": "x", "qty": 1, "unit_price": 50.0}],
    }])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc = backfill_mod.run(str(db), dry_run=True)
    assert rc == 0
    assert m.call_count == 0, (
        "Dry run must never call calibrate_from_outcome — that would "
        "pollute oracle_calibration on a prod read-only check"
    )


def test_shipped_order_fires_calibrate_with_won(backfill_mod, tmp_path):
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [{
        "id": "ORD-1", "agency": "CDCR", "po_number": "PO1",
        "status": "shipped", "total": 6408.24,
        "items": [
            {"description": "Thing A", "qty": 10, "unit_price": 13.92},
            {"description": "Thing B", "qty": 6, "unit_price": 13.92},
        ],
    }])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc = backfill_mod.run(str(db))
    assert rc == 0
    assert m.call_count == 1
    args, kwargs = m.call_args
    items_arg = args[0] if args else kwargs.get("items")
    outcome_arg = args[1] if len(args) > 1 else kwargs.get("outcome")
    agency_kw = kwargs.get("agency", "")
    assert outcome_arg == "won"
    assert agency_kw == "CDCR"
    assert len(items_arg) == 2


def test_items_without_cost_still_processed(backfill_mod, tmp_path):
    """The one real shipped order on prod has no per-line cost. Script
    must still register it as a win (win_count++) — calibrate_from_outcome
    already skips margin EMA for cost-less lines; that's fine."""
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [{
        "id": "ORD-costless", "agency": "CDCR", "po_number": "PO1",
        "status": "shipped", "total": 6408.24,
        "items": [{"description": "no cost", "qty": 1, "unit_price": 50.0}],
    }])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc = backfill_mod.run(str(db))
    assert rc == 0
    assert m.call_count == 1, (
        "Cost-less order must still be backfilled — otherwise the single "
        "real win on prod stays invisible to Oracle"
    )


def test_already_processed_orders_are_skipped(backfill_mod, tmp_path):
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [{
        "id": "ORD-dup", "agency": "CDCR", "po_number": "PO1",
        "status": "shipped", "total": 100.0,
        "items": [{"description": "x", "qty": 1, "unit_price": 50.0}],
    }])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc1 = backfill_mod.run(str(db))
        rc2 = backfill_mod.run(str(db))  # second run
    assert rc1 == 0 and rc2 == 0
    assert m.call_count == 1, (
        f"Ledger must prevent double-counting; got {m.call_count} calls"
    )


def test_non_win_status_orders_are_ignored(backfill_mod, tmp_path):
    """'new' and 'draft' statuses aren't realized wins — leave them alone."""
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [
        {"id": "O-new", "status": "new",
         "items": [{"description": "x", "qty": 1, "unit_price": 1.0}]},
        {"id": "O-draft", "status": "draft",
         "items": [{"description": "x", "qty": 1, "unit_price": 1.0}]},
    ])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc = backfill_mod.run(str(db))
    assert rc == 0
    assert m.call_count == 0


def test_empty_items_array_orders_are_skipped(backfill_mod, tmp_path):
    """3 of the 4 prod orders have items=[] — no item signal to register."""
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [
        {"id": "O-empty", "status": "shipped", "items": []},
    ])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome") as m:
        rc = backfill_mod.run(str(db))
    assert rc == 0
    assert m.call_count == 0


def test_script_bootstraps_sys_path_when_invoked_directly(tmp_path):
    """Regression: 2026-04-20 prod incident. Running
    `railway ssh python scripts/backfill_wins_from_orders.py` without
    PYTHONPATH=/app set hit ModuleNotFoundError('src') at the live
    calibrate import site. Dry-run didn't catch it because the import
    is inside _apply_one(dry_run=False).

    This test executes the script in a fresh subprocess with a sterile
    PYTHONPATH (repo root NOT on sys.path) so the bootstrap in the
    script itself is the only way the `from src.core.pricing_oracle_v2`
    import can succeed. Remove the bootstrap → this test fails.
    """
    import subprocess

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.join(repo_root, "scripts", "backfill_wins_from_orders.py")
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [{
        "id": "ORD-bootstrap", "agency": "CDCR", "po_number": "PO-BS",
        "status": "shipped", "total": 100.0,
        "items": [{"description": "thing", "qty": 1, "unit_price": 50.0}],
    }])

    # Empty PYTHONPATH — bootstrap must handle it alone.
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    # Run from tmp_path so cwd is NOT the repo — eliminates the implicit
    # "repo root is cwd, so src is importable" fallback.
    result = subprocess.run(
        [sys.executable, script, "--db", str(db), "--json"],
        env=env, cwd=str(tmp_path),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, (
        f"Script failed without PYTHONPATH — bootstrap missing?\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    parsed = json.loads(result.stdout.strip())
    assert parsed["candidates"] == 1
    assert parsed["applied"][0]["applied"] is True


def test_json_mode_emits_valid_json(backfill_mod, tmp_path, capsys):
    db = tmp_path / "reytech.db"
    _seed_orders_table(str(db), [{
        "id": "ORD-1", "agency": "CDCR", "po_number": "PO1",
        "status": "shipped", "total": 100.0,
        "items": [{"description": "x", "qty": 1, "unit_price": 50.0}],
    }])
    with patch("src.core.pricing_oracle_v2.calibrate_from_outcome"):
        rc = backfill_mod.run(str(db), as_json=True)
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out.strip())
    assert parsed["candidates"] == 1
    assert parsed["applied"][0]["applied"] is True
