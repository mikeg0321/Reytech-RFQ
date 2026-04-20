"""Read-only contract tests for scripts/db_bloat_diagnostic.py.

The script is the thing we want to run against a 513MB prod DB to
figure out what's bloating. It must be:
  - Read-only (opens DB with mode=ro — a write attempt would error)
  - Safe on an empty DB (no divide-by-zero, no KeyError)
  - Stable JSON output (so we can diff prod runs over time)
  - Surfaces the one-shot hint about free pages / huge log tables
"""
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys

import pytest


@pytest.fixture(scope="module")
def diag_mod():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "db_bloat_diagnostic" in sys.modules:
        del sys.modules["db_bloat_diagnostic"]
    return importlib.import_module("db_bloat_diagnostic")


def _write_sample_db(path, *, add_log_rows=0):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT)")
    cur.execute("CREATE INDEX idx_products_name ON products(name)")
    cur.executemany("INSERT INTO products(name) VALUES(?)",
                    [(f"p{i}",) for i in range(50)])

    cur.execute("CREATE TABLE audit_log(id INTEGER PRIMARY KEY, msg TEXT)")
    if add_log_rows:
        cur.executemany("INSERT INTO audit_log(msg) VALUES(?)",
                        [(f"event {i}",) for i in range(add_log_rows)])
    conn.commit()
    conn.close()


def test_run_on_empty_db_does_not_crash(diag_mod, tmp_path, capsys):
    db = tmp_path / "empty.db"
    sqlite3.connect(str(db)).close()
    rc = diag_mod.run(str(db))
    assert rc == 0
    out = capsys.readouterr().out
    assert "reytech.db bloat diagnostic" in out
    assert "File size" in out


def test_missing_db_returns_exit_2(diag_mod, capsys):
    rc = diag_mod.run("/nonexistent/path/to/reytech.db")
    assert rc == 2


def test_reports_row_counts(diag_mod, tmp_path, capsys):
    db = tmp_path / "sample.db"
    _write_sample_db(str(db), add_log_rows=3)
    rc = diag_mod.run(str(db))
    assert rc == 0
    out = capsys.readouterr().out
    # Table list + row counts surfaced
    assert "products" in out
    assert "audit_log" in out
    # 50 products inserted
    assert "50" in out


def test_json_mode_emits_valid_json(diag_mod, tmp_path, capsys):
    db = tmp_path / "sample.db"
    _write_sample_db(str(db))
    rc = diag_mod.run(str(db), as_json=True)
    assert rc == 0
    out = capsys.readouterr().out.strip()
    parsed = json.loads(out)
    assert parsed["file"]["file_size_bytes"] > 0
    assert parsed["rows_per_table"]["products"] == 50
    assert parsed["table_count"] >= 2
    assert parsed["index_count"] >= 1


def test_script_opens_read_only(diag_mod, tmp_path):
    """A write attempt through the script's helper must error — proves
    we open in ro mode and not rwc. This is the key safety guarantee."""
    db = tmp_path / "sample.db"
    _write_sample_db(str(db))
    conn = diag_mod._open_ro(str(db))
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO products(name) VALUES('x')")
    conn.close()


def test_flags_large_log_table(diag_mod):
    # Synthetic inputs that bypass the DB — just hit the hint logic.
    file_stats = {"file_size_bytes": 100_000_000, "freelist_count": 0,
                  "free_bytes": 0, "free_pct": 0.0,
                  "page_size": 4096, "page_count": 24414,
                  "computed_size_bytes": 100_000_000}
    row_counts = {"audit_log": 500_000, "api_calls": 200_000,
                  "products": 42}
    hints = diag_mod._suggestions(file_stats, row_counts, sizes=None)
    blob = " ".join(hints)
    assert "audit_log" in blob
    assert "api_calls" in blob
    # But normal-sized tables are NOT flagged
    assert "products" not in blob


def test_flags_vacuum_win_when_free_pages_high(diag_mod):
    file_stats = {"file_size_bytes": 100_000_000, "freelist_count": 6104,
                  "free_bytes": 25_000_000, "free_pct": 25.0,
                  "page_size": 4096, "page_count": 24414,
                  "computed_size_bytes": 100_000_000}
    hints = diag_mod._suggestions(file_stats, row_counts={}, sizes=None)
    blob = " ".join(hints)
    assert "VACUUM" in blob
    assert "25.0%" in blob


def test_no_vacuum_hint_when_free_pages_low(diag_mod):
    file_stats = {"file_size_bytes": 100_000_000, "freelist_count": 10,
                  "free_bytes": 40_960, "free_pct": 0.04,
                  "page_size": 4096, "page_count": 24414,
                  "computed_size_bytes": 100_000_000}
    hints = diag_mod._suggestions(file_stats, row_counts={}, sizes=None)
    blob = " ".join(hints)
    assert "VACUUM" not in blob


def test_fmt_bytes_scales(diag_mod):
    f = diag_mod._fmt_bytes
    assert "B" in f(512)
    assert "KB" in f(2048)
    assert "MB" in f(5_000_000)
    assert "GB" in f(5_000_000_000)
